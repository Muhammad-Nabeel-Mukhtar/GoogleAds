# app/mongo_client.py

import os
from typing import Any

from flask import current_app
from pymongo import MongoClient
from pymongo.collection import Collection

_client: MongoClient | None = None
_db_name: str | None = None
_payments_coll_name: str | None = None


def get_mongo_client() -> MongoClient:
    global _client
    if _client is not None:
        return _client

    # 1) Prefer environment variables (.env locally, Render in prod)
    uri = os.getenv("MONGO_URI")

    # 2) Fallback to YAML/Flask config if env not set (optional)
    if not uri:
        cfg = current_app.config.get("PHOTONPAY_CONFIG", {})
        mongo_cfg: dict[str, Any] = cfg.get("mongo", {})
        uri = mongo_cfg.get("uri", "mongodb://localhost:27017")

    _client = MongoClient(uri)
    return _client


def get_mongo_db():
    """
    Return the main MongoDB database handle.
    This is what Payment.collection() will call.
    """
    global _db_name
    client = get_mongo_client()

    if _db_name is None:
        _db_name = os.getenv("MONGO_DB_NAME") or "google_ads_backend"

    return client[_db_name]


def get_payments_collection() -> Collection:
    """
    Convenience helper for code that still wants the concrete payments collection.
    """
    global _payments_coll_name
    db = get_mongo_db()

    if _payments_coll_name is None:
        _payments_coll_name = os.getenv("MONGO_PAYMENTS_COLL") or "payments"

    coll = db[_payments_coll_name]

    coll.create_index("photonpay_id", unique=True)
    coll.create_index("customer_id")
    return coll
