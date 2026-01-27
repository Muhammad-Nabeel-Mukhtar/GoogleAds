# app/payments/leptage_client.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
import os

from flask import current_app
import requests

from .leptage_signing import get_signed_headers, get_webhook_verifier


@dataclass
class LeptageSettings:
    base_url: str
    api_key: str
    api_secret: str
    webhook_secret: Optional[str] = None


class LeptageClient:
    """
    Leptage client wrapper with EC secp256r1 request signing.

    Current behavior:
      - Reads non-secret config from app.config["LEPTAGE_CONFIG"] (YAML)
      - Reads secrets (API key/secret, webhook_secret) from environment (.env)
      - Automatically signs all API requests with ECDSA secp256r1 + SHA256
      - create_payment uses a local stub until real API is wired
    """

    def __init__(self) -> None:
        cfg = current_app.config.get("LEPTAGE_CONFIG", {})
        leptage_cfg = cfg.get("leptage", {})

        env_name = leptage_cfg.get("env", "uat")
        base_urls = leptage_cfg.get("base_urls", {})
        base_url = str(base_urls.get(env_name, "")).rstrip("/")
        if not base_url:
            raise RuntimeError(
                f"[LEPTAGE] Unknown environment or missing base_url for env={env_name}"
            )

        # Secrets from environment (.env)
        api_key = os.getenv("LEPTAGE_API_KEY", "").strip()
        api_secret = os.getenv("LEPTAGE_API_SECRET", "").strip()
        webhook_secret = os.getenv("LEPTAGE_WEBHOOK_SECRET", "").strip() or None

        self.settings = LeptageSettings(
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
            webhook_secret=webhook_secret,
        )

    def is_configured(self) -> bool:
        """Check if all required credentials are present."""
        s = self.settings
        return bool(s.base_url and s.api_key and s.api_secret)

    def create_payment(
        self,
        customer_id: str,
        amount: float,
        currency: str,
        return_url: str,
    ) -> Dict[str, Any]:
        """
        Create a payment / topup with Leptage.

        For now:
          - if credentials missing -> stub
          - if credentials present -> still stub until real API endpoint confirmed
        """
        if not self.is_configured():
            return self._create_payment_stub(
                customer_id, amount, currency, return_url
            )

        # TODO: Replace with real Leptage HTTP call (after API docs confirm endpoint)
        #
        # Example (adjust based on actual Leptage API spec):
        # payload = {
        #     "amount": str(amount),
        #     "currency": currency,
        #     "customerId": customer_id,
        #     "returnUrl": return_url,
        # }
        # headers = get_signed_headers("POST", "/v1/address/deposit", payload)
        # resp = requests.post(
        #     f"{self.settings.base_url}/v1/address/deposit",
        #     json=payload,
        #     headers=headers,
        #     timeout=15,
        # )
        # resp.raise_for_status()
        # return resp.json()
        #
        return self._create_payment_stub(
            customer_id, amount, currency, return_url
        )

    def _create_payment_stub(
        self,
        customer_id: str,
        amount: float,
        currency: str,
        return_url: str,
    ) -> Dict[str, Any]:
        from datetime import datetime, UTC

        fake_payment_id = f"leptage-stub-{customer_id}-{int(datetime.now(UTC).timestamp())}"
        fake_checkout_url = f"{return_url}?payment_id={fake_payment_id}"

        return {
            "id": fake_payment_id,
            "status": "PENDING",
            "checkout_url": fake_checkout_url,
            "amount": amount,
            "currency": currency,
        }

    def verify_webhook_signature(self, payload: bytes, signature: str) -> bool:
        """Verify a Leptage webhook signature."""
        verifier = get_webhook_verifier()
        return verifier.verify_webhook(payload, signature)
