# app/payments/leptage_simulation.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from flask import current_app
import requests

from .leptage_signing import get_signed_headers_v2


@dataclass
class LeptageSimulationSettings:
    base_url: str  # e.g. https://api1.uat.planckage.cc/openapi


class LeptageSimulator:
    """
    Call Leptage mock endpoints for UAT testing.

    Uses the same base_url from config/leptage.yaml as LeptageClient.
    Mock endpoints require API Authentication (ECDSA signing),
    same as business endpoints, following the Java demo.
    """

    def __init__(self) -> None:
        cfg = current_app.config.get("LEPTAGE_CONFIG", {})
        leptage_cfg = cfg.get("leptage", {})

        env_name = leptage_cfg.get("env", "uat")
        base_urls = leptage_cfg.get("base_urls", {})
        base_url = str(base_urls.get(env_name, "")).rstrip("/")
        if not base_url:
            raise RuntimeError(
                f"[LEPTAGE MOCK] Unknown environment or missing base_url for env={env_name}"
            )

        self.settings = LeptageSimulationSettings(base_url=base_url)

    def simulate_deposit(
        self,
        chain: str,
        address: str,
        ccy: str,
        amount: str,
        succeed: bool = True,
    ) -> Dict[str, Any]:
        """
        POST /openapi/v1/mock/deposit/crypto

        Full URL (UAT):
          https://api1.uat.planckage.cc/openapi/v1/mock/deposit/crypto

        Body:
        {
            "chain": "ETHEREUM" | "TRON",
            "address": "0x...",
            "ccy": "USDT" | "USDC" | "USD",
            "amount": "10000.000000",
            "succeed": true | false
        }
        """
        payload = {
            "chain": chain,
            "address": address,
            "ccy": ccy,
            "amount": amount,
            "succeed": succeed,
        }

        # Java demo style: url argument includes /openapi
        path = "/openapi/v1/mock/deposit/crypto"

        # Build signed headers exactly like their postJson: METHOD + url + nonce + jsonString
        headers = get_signed_headers_v2("POST", path, payload)

        # Your base_url is https://api1.uat.planckage.cc/openapi
        # Java demo uses urlPre = https://api1.uat.planckage.cc and passes /openapi/... as path.
        base_no_openapi = self.settings.base_url.replace("/openapi", "")

        full_url = base_no_openapi + path

        print(f"[LEPTAGE MOCK] Calling: {full_url}")
        print(f"[LEPTAGE MOCK] Payload: {payload}")
        print(f"[LEPTAGE MOCK] Headers: {headers}")

        resp = requests.post(
            full_url,
            json=payload,
            headers=headers,
            timeout=15,
        )

        if resp.status_code >= 400:
            print(f"[ERROR] Status: {resp.status_code}")
            print(f"[ERROR] Response body: {resp.text}")
        else:
            print(f"[SUCCESS] Status: {resp.status_code}")
            print(f"[SUCCESS] Response body: {resp.text}")

        resp.raise_for_status()
        return resp.json()


def get_leptage_simulator() -> LeptageSimulator:
    return LeptageSimulator()
