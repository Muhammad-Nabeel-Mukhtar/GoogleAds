# app/payments/models.py

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Dict, List, Optional

from bson import ObjectId  # ensure pymongo is installed
from app.mongo_client import get_payments_collection


@dataclass
class Payment:
    # Renamed from photonpay_id so we can reuse for Leptage or any gateway
    gateway_id: str
    customer_id: str
    amount: float
    currency: str
    status: str
    created_at: str
    updated_at: str
    _id: Optional[str] = None


class MongoPaymentStore:
    @staticmethod
    def _now_iso() -> str:
        return datetime.now(UTC).isoformat().replace("+00:00", "Z")

    def _coll(self):
        return get_payments_collection()

    def create_payment(
        self,
        gateway_id: str,
        customer_id: str,
        amount: float,
        currency: str,
    ) -> Dict:
        """
        Create a new payment record.

        gateway_id will store the Leptage transaction / internal id used to
        identify this payment in callbacks/webhooks.
        """
        now = self._now_iso()
        doc = {
            "gateway_id": gateway_id,
            "customer_id": customer_id,
            "amount": float(amount),
            "currency": currency,
            "status": "PENDING",
            "created_at": now,
            "updated_at": now,
        }
        coll = self._coll()
        result = coll.insert_one(doc)
        doc["_id"] = str(result.inserted_id)
        return doc

    def update_status(self, gateway_id: str, status: str) -> Optional[Dict]:
        """
        Update status by gateway_id (used by Leptage webhook).
        """
        coll = self._coll()
        now = self._now_iso()

        updated = coll.find_one_and_update(
            {"gateway_id": gateway_id},
            {"$set": {"status": status, "updated_at": now}},
            return_document=True,
        )
        if not updated:
            return None
        updated["_id"] = str(updated["_id"])
        return updated

    def get_payment(self, gateway_id: str) -> Optional[Dict]:
        """
        Fetch a payment by gateway_id.
        """
        coll = self._coll()
        doc = coll.find_one({"gateway_id": gateway_id})
        if not doc:
            return None
        doc["_id"] = str(doc["_id"])
        return doc

    def list_payments_for_customer(self, customer_id: str) -> List[Dict]:
        coll = self._coll()
        docs: List[Dict] = []
        for d in coll.find({"customer_id": customer_id}).sort("created_at", -1):
            d["_id"] = str(d["_id"])
            docs.append(d)
        return docs


payment_store = MongoPaymentStore()
