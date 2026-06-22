"""Snapshot + rollback for reversible (agentic) writes.

When an agent is about to modify documents, it can snapshot their current state
first and obtain a *token*. If the change turns out to be wrong, ``rollback(token)``
restores the exact prior state (re-creating deleted docs, removing newly created
ones). This is the "correct mistakes" foundation the feedback loop (Module 5)
builds on.

Works against any Firestore-like ``db`` handle exposing::

    db.collection(name).document(id).get()  -> snapshot with .exists / .to_dict()
    db.collection(name).document(id).set(data)
    db.collection(name).document(id).delete()

so it is exercised in tests with an in-memory fake and runs unchanged against a
real ``google.cloud.firestore.Client`` (or the emulator).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SnapshotStore:
    def __init__(self, db: Any, snapshot_collection: str = "snapshots") -> None:
        self.db = db
        self.snapshot_collection = snapshot_collection

    def snapshot(
        self,
        collection: str,
        doc_ids: List[str],
        *,
        token: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> str:
        """Capture the current state of ``doc_ids`` in ``collection``.

        Returns a token usable with :meth:`rollback`.
        """
        token = token or f"snap-{uuid.uuid4().hex[:12]}"
        captured: List[Dict[str, Any]] = []
        for doc_id in doc_ids:
            snap = self.db.collection(collection).document(doc_id).get()
            existed = bool(getattr(snap, "exists", False))
            captured.append(
                {
                    "id": doc_id,
                    "existed": existed,
                    "data": snap.to_dict() if existed else None,
                }
            )

        self.db.collection(self.snapshot_collection).document(token).set(
            {
                "token": token,
                "created_at": _now_iso(),
                "run_id": run_id,
                "collection": collection,
                "docs": captured,
            }
        )
        return token

    def rollback(self, token: str) -> int:
        """Restore documents to the state captured under ``token``.

        Returns the number of documents restored. Raises ``KeyError`` if the
        token is unknown.
        """
        snap = self.db.collection(self.snapshot_collection).document(token).get()
        if not getattr(snap, "exists", False):
            raise KeyError(f"Unknown snapshot token: {token!r}")
        record = snap.to_dict()
        collection = record["collection"]

        restored = 0
        for entry in record.get("docs", []):
            ref = self.db.collection(collection).document(entry["id"])
            if entry["existed"]:
                ref.set(entry["data"])  # full overwrite back to prior state
            else:
                ref.delete()            # it did not exist before — remove it
            restored += 1
        return restored
