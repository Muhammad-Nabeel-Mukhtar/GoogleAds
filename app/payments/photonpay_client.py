# app/payments/photonpay_client.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from flask import current_app
import requests


@dataclass
class PhotonPaySettings:
    base_url: str
    merchant_id: str
    api_key: str
    api_secret: str
    webhook_secret: Optional[str] = None


class PhotonPayClient:
    """
    PhotonPay client wrapper.

    For now, create_payment uses a local stub if credentials are empty.
    When credentials arrive, just fill photonpay.yaml; no code change required.
    """

    def __init__(self) -> None:
        cfg = current_app.config.get("PHOTONPAY_CONFIG", {})
        photon_cfg = cfg.get("photonpay", {})
        env_name = photon_cfg.get("env", "sandbox")

        base_urls = photon_cfg.get("base_urls", {})
        base_url = base_urls.get(env_name)
        if not base_url:
            raise RuntimeError(f"Unknown PhotonPay environment: {env_name}")

        self.settings = PhotonPaySettings(
            base_url=base_url,
            merchant_id=str(photon_cfg.get("merchant_id", "")).strip(),
            api_key=str(photon_cfg.get("api_key", "")).strip(),
            api_secret=str(photon_cfg.get("api_secret", "")).strip(),
            webhook_secret=photon_cfg.get("webhook_secret") or None,
        )

    def is_configured(self) -> bool:
        s = self.settings
        return bool(s.merchant_id and s.api_key and s.api_secret)

    def create_payment(
        self,
        customer_id: str,
        amount: float,
        currency: str,
        return_url: str,
    ) -> Dict[str, Any]:
        """
        Real implementation should call PhotonPay's API.
        Right now:
          - if credentials missing -> stub
          - if credentials present  -> still stub (you'll replace HTTP later)
        """
        if not self.is_configured():
            return self._create_payment_stub(customer_id, amount, currency, return_url)

        # TODO: replace with real PhotonPay HTTP call when docs/creds are provided.
        # Example outline:
        #
        # payload = {...}
        # headers = {...}
        # resp = requests.post(
        #     f"{self.settings.base_url}/payments",
        #     json=payload,
        #     headers=headers,
        #     timeout=10,
        # )
        # resp.raise_for_status()
        # return resp.json()
        #
        return self._create_payment_stub(customer_id, amount, currency, return_url)

    def _create_payment_stub(
        self,
        customer_id: str,
        amount: float,
        currency: str,
        return_url: str,
    ) -> Dict[str, Any]:
        from datetime import datetime, UTC

        fake_payment_id = f"stub-{customer_id}-{int(datetime.now(UTC).timestamp())}"
        fake_checkout_url = f"{return_url}?payment_id={fake_payment_id}"

        return {
            "id": fake_payment_id,
            "status": "PENDING",
            "checkout_url": fake_checkout_url,
            "amount": amount,
            "currency": currency,
        }

    def verify_webhook_signature(self, payload: bytes, signature: str) -> bool:
        """
        Placeholder: once PhotonPay gives webhook signing docs, implement HMAC here.
        """
        if not self.settings.webhook_secret:
            return True
        # TODO: real HMAC verification
        return True
