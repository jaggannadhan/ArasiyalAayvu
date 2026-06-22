"""Tests for Module 2 — provenance + runs collection.

Layers:
  1. RunContext lifecycle + provenance stamping (offline, InMemoryRunStore).
  2. Snapshot/rollback against an in-memory Firestore fake.
  3. Loader integration: an upload inside a RunContext stamps _provenance on the
     written docs and increments the run's row counters (in-memory fake client).
  4. The run record validates against the schemas 'runs' contract (Module 1).
"""

from __future__ import annotations

import os

import pytest

from agentic import (
    InMemoryRunStore,
    RunContext,
    SnapshotStore,
    current_run,
    current_run_id,
    set_default_store,
    stamp_provenance,
)


@pytest.fixture(autouse=True)
def _fresh_store():
    store = InMemoryRunStore()
    set_default_store(store)
    yield store
    set_default_store(None)


# ---------------------------------------------------------------------------
# In-memory Firestore fake (only the surface our code uses)
# ---------------------------------------------------------------------------


class _Snap:
    def __init__(self, data):
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return dict(self._data) if self._data is not None else None


class _DocRef:
    def __init__(self, store, coll, doc_id):
        self.store, self.coll, self.doc_id = store, coll, doc_id

    def get(self):
        return _Snap(self.store.get(self.coll, {}).get(self.doc_id))

    def set(self, data, merge=False):
        bucket = self.store.setdefault(self.coll, {})
        if merge and self.doc_id in bucket:
            bucket[self.doc_id] = {**bucket[self.doc_id], **data}
        else:
            bucket[self.doc_id] = dict(data)

    def delete(self):
        self.store.get(self.coll, {}).pop(self.doc_id, None)


class _CollRef:
    def __init__(self, store, coll):
        self.store, self.coll = store, coll

    def document(self, doc_id):
        return _DocRef(self.store, self.coll, doc_id)


class _Batch:
    def __init__(self):
        self.ops = []

    def set(self, ref, data, merge=False):
        self.ops.append((ref, dict(data), merge))

    def commit(self):
        for ref, data, merge in self.ops:
            ref.set(data, merge=merge)
        self.ops = []


class FakeFirestore:
    def __init__(self):
        self.store = {}

    def collection(self, name):
        return _CollRef(self.store, name)

    def batch(self):
        return _Batch()


# ---------------------------------------------------------------------------
# 1. RunContext lifecycle
# ---------------------------------------------------------------------------


def test_successful_run_records_status_and_rows(_fresh_store):
    with RunContext(tool="t", trigger="test") as run:
        run.record_write("manifesto_promises", 10)
        run.record_write("manifesto_promises", 5)
        run.record_write("feedback", 2)
    rec = _fresh_store.get_run(run.run_id)
    assert rec["status"] == "success"
    assert rec["rows_written"] == 17
    writes = {w["collection"]: w["count"] for w in rec["writes"]}
    assert writes == {"manifesto_promises": 15, "feedback": 2}
    assert rec["started_at"] and rec["finished_at"]
    assert rec["duration_s"] is not None


def test_failed_run_records_error_and_reraises(_fresh_store):
    with pytest.raises(ValueError):
        with RunContext(tool="t", trigger="test") as run:
            raise ValueError("boom")
    rec = _fresh_store.get_run(run.run_id)
    assert rec["status"] == "failed"
    assert any("boom" in e for e in rec["errors"])


def test_partial_status_when_errors_without_exception(_fresh_store):
    with RunContext(tool="t", trigger="test") as run:
        run.add_error("a recoverable problem")
    assert _fresh_store.get_run(run.run_id)["status"] == "partial"


def test_record_write_ignores_nonpositive(_fresh_store):
    with RunContext(tool="t", trigger="test") as run:
        run.record_write("x", 0)
        run.record_write("x", -3)
    assert _fresh_store.get_run(run.run_id)["rows_written"] == 0


def test_contextvar_set_and_restored(_fresh_store):
    assert current_run() is None
    with RunContext(tool="outer", trigger="test") as outer:
        assert current_run_id() == outer.run_id
        with RunContext(tool="inner", trigger="test") as inner:
            assert current_run_id() == inner.run_id
            assert inner.parent_run_id == outer.run_id
        assert current_run_id() == outer.run_id
    assert current_run() is None


# ---------------------------------------------------------------------------
# 2. provenance stamping
# ---------------------------------------------------------------------------


def test_stamp_noop_without_run():
    doc = {"a": 1}
    stamp_provenance(doc)
    assert "_provenance" not in doc


def test_stamp_adds_run_metadata(_fresh_store):
    with RunContext(tool="my_tool", trigger="test") as run:
        doc = {"a": 1}
        stamp_provenance(doc)
        assert doc["_provenance"]["run_id"] == run.run_id
        assert doc["_provenance"]["tool"] == "my_tool"
        assert "written_at" in doc["_provenance"]


# ---------------------------------------------------------------------------
# 3. snapshot / rollback
# ---------------------------------------------------------------------------


def test_rollback_restores_modified_and_removes_created():
    db = FakeFirestore()
    db.collection("manifesto_promises").document("p1").set({"text": "original"})
    snaps = SnapshotStore(db)

    token = snaps.snapshot("manifesto_promises", ["p1", "p2_new"])

    # mutate p1, create p2_new
    db.collection("manifesto_promises").document("p1").set({"text": "edited"})
    db.collection("manifesto_promises").document("p2_new").set({"text": "created"})

    restored = snaps.rollback(token)
    assert restored == 2
    assert db.collection("manifesto_promises").document("p1").get().to_dict() == {"text": "original"}
    assert db.collection("manifesto_promises").document("p2_new").get().exists is False


def test_rollback_unknown_token_raises():
    db = FakeFirestore()
    with pytest.raises(KeyError):
        SnapshotStore(db).rollback("nope")


# ---------------------------------------------------------------------------
# 4. loader integration (in-memory fake client)
# ---------------------------------------------------------------------------


def test_loader_stamps_provenance_and_counts_rows(_fresh_store, monkeypatch):
    # Allow firestore.Client() construction offline, then swap in the fake.
    monkeypatch.setenv("FIRESTORE_EMULATOR_HOST", "localhost:9999")
    monkeypatch.setenv("AAYVU_SCHEMA_VALIDATION", "warn")
    from loaders import firestore_loader as L

    fake = FakeFirestore()
    monkeypatch.setattr(L, "db", fake)

    winners = [
        {"doc_id": "2021_alpha", "constituency": "ALPHA", "party": "DMK"},
        {"doc_id": "2021_beta", "constituency": "BETA", "party": "AIADMK"},
    ]
    with RunContext(tool="accountability_transformer", trigger="test") as run:
        L.upload_mla_winners(winners)

    stored = fake.store["candidate_accountability"]
    assert set(stored) == {"2021_alpha", "2021_beta"}
    for doc in stored.values():
        assert doc["_provenance"]["run_id"] == run.run_id
        assert doc["_uploaded_at"]  # existing behaviour preserved

    rec = _fresh_store.get_run(run.run_id)
    assert rec["rows_written"] == 2
    assert rec["writes"] == [{"collection": "candidate_accountability", "count": 2}]


# ---------------------------------------------------------------------------
# 5. run record satisfies the Module 1 schema contract
# ---------------------------------------------------------------------------


def test_run_record_validates_against_schema(_fresh_store):
    from schemas import validate_doc

    with RunContext(tool="t", trigger="test") as run:
        run.record_write("feedback", 1)
    rec = _fresh_store.get_run(run.run_id)
    result = validate_doc("runs", rec)
    assert result.ok, [(e.loc, e.message) for e in result.errors]
