# app/mongo_client.py

import os
from typing import Any

from flask import current_app
from pymongo import MongoClient
from pymongo.collection import Collection

_client: MongoClient | None = None


def get_mongo_client() -> MongoClient:
    global _client
    if _client is not None:
        return _client

    # 1) Prefer environment variables (.env locally, Render in prod)
    uri = os.getenv("MONGO_URI")

    # 2) Fallback to YAML config if env not set (optional)
    if not uri:
        cfg = current_app.config.get("PHOTONPAY_CONFIG", {})
        mongo_cfg: dict[str, Any] = cfg.get("mongo", {})
        uri = mongo_cfg.get("uri", "mongodb://localhost:27017")

    _client = MongoClient(uri)
    return _client


def get_payments_collection() -> Collection:
    client = get_mongo_client()

    db_name = os.getenv("MONGO_DB_NAME") or "google_ads_backend"
    coll_name = os.getenv("MONGO_PAYMENTS_COLL") or "payments"

    db = client[db_name]
    coll = db[coll_name]

    coll.create_index("photonpay_id", unique=True)
    coll.create_index("customer_id")
    return coll
