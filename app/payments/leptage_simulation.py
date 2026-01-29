# app/payments/leptage_simulation.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from flask import current_app
import requests


@dataclass
class LeptageSimulationSettings:
    base_url: str


class LeptageSimulator:
    """
    Call Leptage mock endpoints for UAT testing.

    Uses the same base_url from config/leptage.yaml as LeptageClient,
    but does NOT do API Authentication because mock endpoints do not
    require signing (docs show only Content-Type header).
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
        POST /v1/mock/deposit/crypto

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

        resp = requests.post(
            f"{self.settings.base_url}/v1/mock/deposit/crypto",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()


def get_leptage_simulator() -> LeptageSimulator:
    return LeptageSimulator()
