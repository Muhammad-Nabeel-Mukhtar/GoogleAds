# app/config_loader.py

from pathlib import Path
from typing import Any, Dict

import yaml
from flask import Flask


def load_photonpay_config(app: Flask) -> None:
    """
    Load config/photonpay.yaml into app.config["PHOTONPAY_CONFIG"].
    """
    root = Path(__file__).resolve().parents[1]  # project root
    config_path = root / "config" / "photonpay.yaml"

    if not config_path.exists():
        raise FileNotFoundError(f"PhotonPay config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        data: Dict[str, Any] = yaml.safe_load(f) or {}

    app.config["PHOTONPAY_CONFIG"] = data
