# app/payments/routes.py

from __future__ import annotations

from typing import List
from datetime import datetime, UTC

from flask import jsonify, request, current_app

from . import payments_bp
from .models import payment_store
from .photonpay_client import PhotonPayClient


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


@payments_bp.route("/payments", methods=["POST"])
def create_payment():
    """
    POST /api/payments

    Body:
    {
      "customer_id": "1234567890",
      "amount": 1000,
      "currency": "USD"  # optional, default comes from YAML
    }
    """
    data = request.get_json(silent=True) or {}
    customer_id = str(data.get("customer_id", "")).strip()
    amount_raw = data.get("amount")

    cfg = current_app.config.get("PHOTONPAY_CONFIG", {})
    payments_cfg = cfg.get("payments", {})
    default_currency = payments_cfg.get("currency_default", "USD")

    currency = str(data.get("currency", default_currency)).strip().upper()

    errors: List[str] = []

    if not customer_id or not customer_id.isdigit():
        errors.append("Valid numeric customer_id is required.")

    try:
        amount = float(amount_raw)
        if amount <= 0:
            errors.append("amount must be greater than 0.")
    except (TypeError, ValueError):
        errors.append("amount must be a valid number.")

    if errors:
        return jsonify({"success": False, "errors": errors}), 400

    success_path = payments_cfg.get("success_path", "/payment-success")
    return_url = request.host_url.rstrip("/") + success_path

    client = PhotonPayClient()
    payment_resp = client.create_payment(
        customer_id=customer_id,
        amount=amount,
        currency=currency,
        return_url=return_url,
    )

    record = payment_store.create_payment(
        photonpay_id=payment_resp["id"],
        customer_id=customer_id,
        amount=amount,
        currency=currency,
    )

    return jsonify(
        {
            "success": True,
            "payment_id": payment_resp["id"],
            "authorization_url": payment_resp["checkout_url"],
            "amount": amount,
            "currency": currency,
            "record": record,
            "timestamp": _now_iso(),
        }
    ), 201


@payments_bp.route("/payments/<payment_id>/status", methods=["GET"])
def get_payment_status(payment_id: str):
    """
    GET /api/payments/<payment_id>/status
    """
    record = payment_store.get_payment(payment_id)
    if not record:
        return jsonify({"success": False, "errors": ["Payment not found."]}), 404

    return jsonify(
        {
            "success": True,
            "payment_id": payment_id,
            "status": record["status"],
            "amount": record["amount"],
            "currency": record["currency"],
            "customer_id": record["customer_id"],
            "created_at": record["created_at"],
            "updated_at": record["updated_at"],
        }
    ), 200


@payments_bp.route("/webhooks/photonpay", methods=["POST"])
def photonpay_webhook():
    """
    POST /api/webhooks/photonpay

    For now: log + update local state.
    Later: add signature verification using PhotonPayClient.verify_webhook_signature.
    """
    payload = request.get_json(silent=True) or {}
    event = payload.get("event")
    data = payload.get("data") or {}

    photonpay_id = data.get("id")
    status = data.get("status")

    print(f"[PHOTONPAY WEBHOOK] event={event}, id={photonpay_id}, status={status}")

    if photonpay_id and status:
        payment_store.update_status(photonpay_id, status)

    return jsonify({"success": True}), 200
