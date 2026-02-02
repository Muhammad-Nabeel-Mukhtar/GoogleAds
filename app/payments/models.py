# app/payments/models.py

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from app.mongo_client import get_mongo_db


@dataclass
class Payment:
    id: str               # Mongo _id as string
    # For now you don't care about campaign/customer, so we keep a generic field
    campaign_id: str
    leptage_txn_id: Optional[str]
    ccy: str              # "USDT"
    chain: Optional[str]  # "ETHEREUM" or "TRON"
    amount: float
    status: str           # "PENDING" | "CONFIRMED" | "FAILED"
    created_at: datetime
    updated_at: datetime
    customer_wallet: Optional[str] = None

    @staticmethod
    def collection():
        db = get_mongo_db()
        return db["payments"]

    @classmethod
    def create(
        cls,
        campaign_id: str,
        amount: float,
        ccy: str = "USDT",
        chain: Optional[str] = None,
    ) -> "Payment":
        now = datetime.utcnow()
        doc = {
            "campaign_id": campaign_id,
            "leptage_txn_id": None,
            "ccy": ccy,
            "chain": chain,
            "amount": float(amount),
            "status": "PENDING",
            "created_at": now,
            "updated_at": now,
            "customer_wallet": None,
        }
        coll = cls.collection()
        result = coll.insert_one(doc)
        doc["_id"] = result.inserted_id
        return cls.from_mongo(doc)

    @classmethod
    def from_mongo(cls, doc) -> "Payment":
        return cls(
            id=str(doc["_id"]),
            campaign_id=doc["campaign_id"],
            leptage_txn_id=doc.get("leptage_txn_id"),
            ccy=doc["ccy"],
            chain=doc.get("chain"),
            amount=float(doc["amount"]),
            status=doc["status"],
            created_at=doc["created_at"],
            updated_at=doc["updated_at"],
            customer_wallet=doc.get("customer_wallet"),
        )

    @classmethod
    def get_by_id(cls, payment_id: str) -> Optional["Payment"]:
        from bson import ObjectId

        coll = cls.collection()
        try:
            oid = ObjectId(payment_id)
        except Exception:
            return None

        doc = coll.find_one({"_id": oid})
        if not doc:
            return None
        return cls.from_mongo(doc)

    @classmethod
    def get_latest_pending_for_ccy(cls, ccy: str) -> Optional["Payment"]:
        coll = cls.collection()
        cursor = (
            coll.find({"status": "PENDING", "ccy": ccy})
            .sort("created_at", -1)
            .limit(1)
        )
        docs = list(cursor)
        if not docs:
            return None
        return cls.from_mongo(docs[0])

    def update_status(
        self,
        status: str,
        leptage_txn_id: Optional[str] = None,
        customer_wallet: Optional[str] = None,
    ) -> None:
        from bson import ObjectId

        coll = self.collection()
        update = {
            "status": status,
            "updated_at": datetime.utcnow(),
        }
        if leptage_txn_id is not None:
            update["leptage_txn_id"] = leptage_txn_id
        if customer_wallet is not None:
            update["customer_wallet"] = customer_wallet

        coll.update_one({"_id": ObjectId(self.id)}, {"$set": update})

        # Update in-memory
        self.status = status
        if leptage_txn_id is not None:
            self.leptage_txn_id = leptage_txn_id
        if customer_wallet is not None:
            self.customer_wallet = customer_wallet
        self.updated_at = datetime.utcnow()
