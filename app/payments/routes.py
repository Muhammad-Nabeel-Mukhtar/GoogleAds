# app/payments/routes.py

from __future__ import annotations

from typing import List
from datetime import datetime, timezone

from flask import jsonify, request, current_app

from . import payments_bp
from .leptage_client import LeptageClient
from .leptage_signing import get_webhook_verifier
from .models import Payment


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@payments_bp.route("/payments", methods=["POST"])
def create_payment():
    """
    POST /api/payments

    Body:
    {
      "amount": 100.0,
      "ccy": "USDT",          # optional, default from config
      "chain": "ETHEREUM"     # optional, default ETHEREUM
    }

    This does NOT create a Leptage checkout session.
    It just:
      - Gets your deposit address from Leptage (for ccy/chain)
      - Creates a local Payment record in Mongo
      - Returns payment_id + address to the frontend
    """
    data = request.get_json(silent=True) or {}
    amount_raw = data.get("amount")

    cfg = current_app.config.get("LEPTAGE_CONFIG", {})
    payments_cfg = cfg.get("payments", {})
    default_currency = payments_cfg.get("currency_default", "USDT")

    ccy = str(data.get("ccy", default_currency)).strip().upper()
    chain = str(data.get("chain", "ETHEREUM")).strip().upper()

    errors: List[str] = []

    try:
        amount = float(amount_raw)
        if amount <= 0:
            errors.append("amount must be greater than 0.")
    except (TypeError, ValueError):
        errors.append("amount must be a valid number.")

    if errors:
        return jsonify({"success": False, "errors": errors}), 400

    # 1) Get deposit address from Leptage
    client = LeptageClient()
    try:
        addr_resp = client.get_deposit_addresses(ccy=ccy, chain=chain)
    except Exception as e:
        current_app.logger.exception("Error calling get_deposit_addresses")
        return jsonify({"success": False, "errors": [f"Leptage error: {e}"]}), 502

    addresses = addr_resp.get("data") or []
    if not addresses:
        return jsonify(
            {"success": False, "errors": ["No deposit address available."]}
        ), 500

    address = addresses[0]["address"]

    # 2) Create local Payment record (no customer/campaign linkage for now)
    payment = Payment.create(
        campaign_id="generic_deposit",
        amount=amount,
        ccy=ccy,
        chain=chain,
    )

    return jsonify(
        {
            "success": True,
            "payment_id": payment.id,
            "amount": amount,
            "ccy": ccy,
            "chain": chain,
            "address": address,
            "status": payment.status,
            "timestamp": _now_iso(),
        }
    ), 201


@payments_bp.route("/payments/<payment_id>/status", methods=["GET"])
def get_payment_status(payment_id: str):
    """
    GET /api/payments/<payment_id>/status

    Frontend polls this to see if the deposit is confirmed.
    """
    payment = Payment.get_by_id(payment_id)
    if not payment:
        return jsonify(
            {"success": False, "errors": ["Payment not found."]}
        ), 404

    return jsonify(
        {
            "success": True,
            "payment_id": payment.id,
            "status": payment.status,
            "amount": payment.amount,
            "ccy": payment.ccy,
            "chain": payment.chain,
            "leptage_txn_id": payment.leptage_txn_id,
            "customer_wallet": payment.customer_wallet,
            "created_at": payment.created_at.isoformat(),
            "updated_at": payment.updated_at.isoformat(),
        }
    ), 200


@payments_bp.route("/webhooks/leptage", methods=["POST"])
def leptage_webhook():
    """
    POST /api/webhooks/leptage

    Verifies Leptage webhook using HMAC-SHA256 as per docs,
    then updates local Payment status.

    Expecting payload similar to /v1/txns/deposit response schema:
    {
      "code": "0000",
      "msg": "succeed",
      "data": {
        "txnId": "...",
        "ccy": "USDT",
        "amount": "30000.000000",
        "status": "SUCCEEDED",
        "createdAt": 1735892447000,
        "type": "FUNDS_IN",
        "chainInfo": {...},
        "payer": {...},
        "accountId": "..."
      }
    }
    or just the inner "data" object.
    """
    raw_body = request.get_data()
    headers = request.headers

    verifier = get_webhook_verifier()
    if not verifier.verify_webhook(headers, raw_body):
        print("[LEPTAGE WEBHOOK] Invalid signature")
        return jsonify({"success": False, "error": "Invalid signature"}), 401

    payload = request.get_json(silent=True) or {}

    # Some implementations wrap the object in { code, msg, data }, some send data directly.
    data = payload.get("data") or payload

    txn_id = data.get("txnId")
    ccy = data.get("ccy")
    amount_str = data.get("amount")
    status = data.get("status")
    chain_info = data.get("chainInfo") or {}
    payer = data.get("payer") or {}

    print(
        f"[LEPTAGE WEBHOOK] txn_id={txn_id}, ccy={ccy}, amount={amount_str}, status={status}"
    )

    try:
        amount = float(amount_str) if amount_str is not None else None
    except (TypeError, ValueError):
        amount = None

    # Simple strategy for now:
    # - You only care about "deposit to our account".
    # - Match by latest PENDING payment for this currency.
    payment = None
    if ccy:
        payment = Payment.get_latest_pending_for_ccy(ccy)

    if not payment:
        print("[LEPTAGE WEBHOOK] No matching local payment found; ignoring.")
        return jsonify({"success": True}), 200

    status_upper = str(status).upper() if status else ""

    if status_upper == "SUCCEEDED":
        source_addr = payer.get("sourceAddress") or chain_info.get("sourceAddress")
        payment.update_status(
            "CONFIRMED",
            leptage_txn_id=txn_id,
            customer_wallet=source_addr,
        )
        print(f"[LEPTAGE WEBHOOK] Payment {payment.id} confirmed.")
    elif status_upper == "FAILED":
        payment.update_status("FAILED", leptage_txn_id=txn_id)
        print(f"[LEPTAGE WEBHOOK] Payment {payment.id} failed.")
    else:
        print(f"[LEPTAGE WEBHOOK] Status {status} not handled explicitly.")

    return jsonify({"success": True}), 200
