# app/payments/routes.py

from __future__ import annotations

import os
import hmac
import hashlib
from typing import List
from datetime import datetime, timezone

from flask import jsonify, request, current_app

from . import payments_bp
from .leptage_client import LeptageClient
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

    Verifies Leptage webhook using HMAC-SHA256 as per their documentation Sheet 3:
    - Signature string: X-HOOK-NONCE + webhook_url + request_body
    - Algorithm: HMAC-SHA256
    - Headers: X-HOOK-NONCE, X-HOOK-SIGNATURE
    - Secret: Provided by Leptage in format "sbox:xxxxx"

    Then updates local Payment status.

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
    # Get headers
    nonce = request.headers.get("X-HOOK-NONCE")
    received_signature = request.headers.get("X-HOOK-SIGNATURE")

    if not nonce or not received_signature:
        current_app.logger.error("[LEPTAGE WEBHOOK] Missing signature headers")
        return jsonify({"success": False, "error": "Missing signature headers"}), 400

    # Get raw body as string (important: must match exactly what was sent)
    raw_body = request.get_data(as_text=True)

    # Construct signature string as per Leptage spec
    # Format: nonce + webhook_url + request_body
    webhook_url = "https://googleads-ex2w.onrender.com/api/webhooks/leptage"
    sign_str = nonce + webhook_url + raw_body

    # Get secret key from environment (format: "sbox:xxxxx" as per Leptage)
    secret_key = os.getenv("LEPTAGE_WEBHOOK_SECRET")
    
    if not secret_key:
        current_app.logger.error("[LEPTAGE WEBHOOK] LEPTAGE_WEBHOOK_SECRET not configured")
        return jsonify({"success": False, "error": "Server configuration error"}), 500

    # Compute HMAC-SHA256 signature
    computed_signature = hmac.new(
        secret_key.encode('utf-8'),
        sign_str.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    # Verify signature
    if not hmac.compare_digest(computed_signature, received_signature):
        current_app.logger.error(
            f"[LEPTAGE WEBHOOK] Invalid signature. "
            f"Computed: {computed_signature[:20]}..., Received: {received_signature[:20]}..."
        )
        return jsonify({"success": False, "error": "Invalid signature"}), 401

    current_app.logger.info("[LEPTAGE WEBHOOK] Signature verified successfully")

    # Parse JSON payload
    payload = request.get_json(silent=True) or {}

    # Some implementations wrap the object in { code, msg, data }, some send data directly.
    data = payload.get("data") or payload

    txn_id = data.get("txnId")
    ccy = data.get("ccy")
    amount_str = data.get("amount")
    status = data.get("status")
    chain_info = data.get("chainInfo") or {}
    payer = data.get("payer") or {}

    current_app.logger.info(
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
        current_app.logger.warning(
            f"[LEPTAGE WEBHOOK] No matching local payment found for ccy={ccy}; acknowledging anyway."
        )
        return jsonify({"success": True}), 200

    status_upper = str(status).upper() if status else ""

    if status_upper == "SUCCEEDED":
        source_addr = payer.get("sourceAddress") or chain_info.get("sourceAddress")
        payment.update_status(
            "CONFIRMED",
            leptage_txn_id=txn_id,
            customer_wallet=source_addr,
        )
        current_app.logger.info(f"[LEPTAGE WEBHOOK] Payment {payment.id} confirmed.")
    elif status_upper == "FAILED":
        payment.update_status("FAILED", leptage_txn_id=txn_id)
        current_app.logger.info(f"[LEPTAGE WEBHOOK] Payment {payment.id} failed.")
    else:
        current_app.logger.info(
            f"[LEPTAGE WEBHOOK] Status {status} not handled explicitly; no update."
        )

    return jsonify({"success": True}), 200


# Test endpoint for local development only
@payments_bp.route("/webhooks/leptage/test", methods=["POST"])
def leptage_webhook_test():
    """
    Local testing endpoint - bypasses signature verification
    Remove or disable in production
    """
    payload = request.get_json() or {}
    data = payload.get("data", {})

    txn_id = data.get("txnId")
    ccy = data.get("ccy")
    amount = float(data.get("amount", "0") or 0)
    chain_info = data.get("chainInfo") or {}
    src_address = chain_info.get("sourceAddress")
    status = data.get("status")

    # Find latest pending payment for this currency and amount
    payment = Payment.get_latest_pending_for_ccy(ccy)
    if not payment or payment.amount != amount:
        return jsonify({"success": False, "error": "No matching pending payment"}), 404

    if status == "SUCCEEDED":
        payment.update_status(
            status="CONFIRMED",
            leptage_txn_id=txn_id,
            customer_wallet=src_address,
        )

    return jsonify({"success": True}), 200
