# app/payments/leptage_client.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
import os

from flask import current_app
import requests

from .leptage_signing import get_signed_headers_v2, get_webhook_verifier


@dataclass
class LeptageSettings:
    base_url: str
    api_key: str
    api_secret: str
    webhook_secret: Optional[str] = None


class LeptageClient:
    """
    Leptage client wrapper with EC secp256r1 request signing.

    Behavior:
      - Reads non-secret config from app.config["LEPTAGE_CONFIG"] (YAML)
      - Reads secrets (API key/secret, webhook_secret) from environment (.env / Render)
      - Automatically signs all API requests as per Leptage Java demo
      - create_payment currently uses a local stub until the real endpoint is finalized
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
        """
        Check if all required credentials are present.
        """
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
          - if credentials present -> still stub until real API endpoint is wired
        """
        if not self.is_configured():
            return self._create_payment_stub(
                customer_id, amount, currency, return_url
            )

        # TODO: Replace with real Leptage HTTP call (e.g. /openapi/v1/address/deposit)
        return self._create_payment_stub(
            customer_id, amount, currency, return_url
        )

    def list_deposits(
        self,
        page_index: int = 1,
        page_size: int = 10,
    ) -> Dict[str, Any]:
        """
        Call /openapi/v1/txns/deposit using the exact Java demo signing:

        Java:
          respJson = LeptageApiHttpUtils.postJson("/openapi/v1/txns/deposit", reqJson);
        """
        if not self.is_configured():
            return {"success": False, "error": "Leptage not configured"}

        path = "/openapi/v1/txns/deposit"
        payload = {
            "pageIndex": page_index,
            "pageSize": page_size,
        }

        headers = get_signed_headers_v2("POST", path, payload)

        base_no_openapi = self.settings.base_url.replace("/openapi", "")
        full_url = base_no_openapi + path

        resp = requests.post(
            full_url,
            json=payload,
            headers=headers,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    def _create_payment_stub(
        self,
        customer_id: str,
        amount: float,
        currency: str,
        return_url: str,
    ) -> Dict[str, Any]:
        from datetime import datetime, timezone

        fake_payment_id = (
            f"leptage-stub-{customer_id}-"
            f"{int(datetime.now(timezone.utc).timestamp())}"
        )
        fake_checkout_url = f"{return_url}?payment_id={fake_payment_id}"

        return {
            "id": fake_payment_id,
            "status": "PENDING",
            "checkout_url": fake_checkout_url,
            "amount": amount,
            "currency": currency,
        }
        
    

    def verify_webhook_signature(self, headers, payload: bytes) -> bool:
        """
        Verify a Leptage webhook signature.
        """
        verifier = get_webhook_verifier()
        return verifier.verify_webhook(headers, payload)
    
    
    
    def get_deposit_addresses(
    self,
    ccy: Optional[str] = None,
    chain: Optional[str] = None,
) -> Dict[str, Any]:
     path = "/v1/address/deposit"          # WITHOUT /openapi
     url_for_signing = "/openapi" + path   # /openapi/v1/address/deposit

     params = {}
     if ccy:
        params["ccy"] = ccy
     if chain:
        params["chain"] = chain

    # For signing we pass the full URL including /openapi
     headers = get_signed_headers_v2("GET", url_for_signing, params if params else None)

    # For actual HTTP call, we append path to base_url
     url = f"{self.settings.base_url}{path}"
     if params:
        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{query_string}"

     print(f"[DEBUG] Calling: {url}")
     print(f"[DEBUG] Headers: {headers}")

     resp = requests.get(url, headers=headers, timeout=15)
     if resp.status_code >= 400:
        print(f"[ERROR] Status: {resp.status_code}")
        print(f"[ERROR] Body: {resp.text}")
     resp.raise_for_status()
     return resp.json()


