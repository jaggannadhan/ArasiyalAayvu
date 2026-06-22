"""Run provenance — the audit log + training signal for the control plane.

Usage
-----
Wrap any pipeline run in a :class:`RunContext`. While it is active, every
``loaders.firestore_loader`` upload automatically (a) stamps the originating
run id onto each document and (b) increments the run's row counters::

    from agentic import RunContext

    with RunContext(tool="manifesto_ocr_gemini", trigger="cli", args={"party": "dmk"}):
        ...                       # call scrapers/transformers/loaders as normal
    # on exit: the run record (status, duration, rows_written, errors) is saved.

Design principles (carried over from Module 1's validation hook):
* **Never break the pipeline.** All store I/O is best-effort; a logging failure
  must not abort a real ingestion run.
* **Pluggable storage.** ``FirestoreRunStore`` for production, ``InMemoryRunStore``
  for tests. The active store is discoverable via :func:`get_default_store`.
* **Discoverable via contextvars** so the loader can find the active run without
  any change to scraper call signatures.
"""

from __future__ import annotations

import contextvars
import os
import socket
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Active-run discovery (contextvars so it is async/thread-local safe)
# ---------------------------------------------------------------------------

_current_run: contextvars.ContextVar[Optional["RunContext"]] = contextvars.ContextVar(
    "aayvu_current_run", default=None
)


def current_run() -> Optional["RunContext"]:
    """Return the RunContext active on this execution context, if any."""
    return _current_run.get()


def current_run_id() -> Optional[str]:
    run = current_run()
    return run.run_id if run else None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stamp_provenance(doc: Dict[str, Any], run: Optional["RunContext"] = None) -> Dict[str, Any]:
    """Stamp ``_provenance`` onto a document in place (and return it).

    No-op when there is no active run, so existing non-instrumented uploads are
    unaffected.
    """
    run = run or current_run()
    if run is None:
        return doc
    doc["_provenance"] = {
        "run_id": run.run_id,
        "tool": run.tool,
        "written_at": _now_iso(),
    }
    return doc


# ---------------------------------------------------------------------------
# Run stores
# ---------------------------------------------------------------------------


class RunStore(ABC):
    """Persistence backend for run records (the ``runs`` collection)."""

    @abstractmethod
    def save_run(self, record: Dict[str, Any]) -> None: ...

    @abstractmethod
    def update_run(self, run_id: str, changes: Dict[str, Any]) -> None: ...

    @abstractmethod
    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]: ...

    @abstractmethod
    def list_recent(self, limit: int = 20) -> List[Dict[str, Any]]: ...


class InMemoryRunStore(RunStore):
    """Test/dev store. Keeps run records in a dict."""

    def __init__(self) -> None:
        self.runs: Dict[str, Dict[str, Any]] = {}

    def save_run(self, record: Dict[str, Any]) -> None:
        self.runs[record["run_id"]] = dict(record)

    def update_run(self, run_id: str, changes: Dict[str, Any]) -> None:
        self.runs.setdefault(run_id, {"run_id": run_id}).update(changes)

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        rec = self.runs.get(run_id)
        return dict(rec) if rec else None

    def list_recent(self, limit: int = 20) -> List[Dict[str, Any]]:
        ordered = sorted(
            self.runs.values(), key=lambda r: r.get("started_at", ""), reverse=True
        )
        return [dict(r) for r in ordered[:limit]]


class FirestoreRunStore(RunStore):
    """Production store — writes to the ``runs`` Firestore collection.

    The client is created lazily (and can be injected, e.g. to reuse the
    loader's client or point at the emulator) so importing this module never
    requires GCP credentials.
    """

    COLLECTION = "runs"

    def __init__(self, client: Any = None, project: Optional[str] = None) -> None:
        self._client = client
        self._project = project or os.environ.get("GOOGLE_CLOUD_PROJECT", "naatunadappu")

    @property
    def client(self) -> Any:
        if self._client is None:
            from google.cloud import firestore  # lazy

            self._client = firestore.Client(project=self._project)
        return self._client

    def save_run(self, record: Dict[str, Any]) -> None:
        self.client.collection(self.COLLECTION).document(record["run_id"]).set(
            record, merge=True
        )

    def update_run(self, run_id: str, changes: Dict[str, Any]) -> None:
        self.client.collection(self.COLLECTION).document(run_id).set(changes, merge=True)

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        snap = self.client.collection(self.COLLECTION).document(run_id).get()
        return snap.to_dict() if snap.exists else None

    def list_recent(self, limit: int = 20) -> List[Dict[str, Any]]:
        from google.cloud import firestore  # lazy, for Query.DESCENDING

        q = (
            self.client.collection(self.COLLECTION)
            .order_by("started_at", direction=firestore.Query.DESCENDING)
            .limit(limit)
        )
        return [d.to_dict() for d in q.stream()]


# Default store — pluggable. Defaults to Firestore but can be swapped in tests.
_default_store: Optional[RunStore] = None


def set_default_store(store: Optional[RunStore]) -> None:
    global _default_store
    _default_store = store


def get_default_store() -> RunStore:
    global _default_store
    if _default_store is None:
        _default_store = FirestoreRunStore()
    return _default_store


# ---------------------------------------------------------------------------
# RunContext
# ---------------------------------------------------------------------------


class RunContext:
    """Context manager that records one pipeline run.

    All store interactions are wrapped so a provenance failure never aborts the
    underlying work.
    """

    def __init__(
        self,
        tool: str,
        *,
        trigger: str = "cli",
        args: Optional[Dict[str, Any]] = None,
        store: Optional[RunStore] = None,
        parent_run_id: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> None:
        self.tool = tool
        self.trigger = trigger
        self.args = args or {}
        self.store = store  # resolved lazily on __enter__
        self.parent_run_id = parent_run_id or current_run_id()
        self.run_id = run_id or f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"

        self.status = "running"
        self.started_at: Optional[str] = None
        self.finished_at: Optional[str] = None
        self.rows_written = 0
        self.writes: Dict[str, int] = {}
        self.errors: List[str] = []
        self._token: Optional[contextvars.Token] = None

    # -- record-building -----------------------------------------------------

    def record_write(self, collection: str, count: int) -> None:
        """Called by the loader for each uploaded chunk."""
        if count <= 0:
            return
        self.writes[collection] = self.writes.get(collection, 0) + count
        self.rows_written += count

    def add_error(self, message: str) -> None:
        self.errors.append(str(message)[:1000])

    def to_dict(self) -> Dict[str, Any]:
        duration = None
        if self.started_at and self.finished_at:
            try:
                duration = round(
                    (
                        datetime.fromisoformat(self.finished_at)
                        - datetime.fromisoformat(self.started_at)
                    ).total_seconds(),
                    3,
                )
            except ValueError:
                duration = None
        return {
            "run_id": self.run_id,
            "tool": self.tool,
            "trigger": self.trigger,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_s": duration,
            "args": self.args,
            "rows_written": self.rows_written,
            "writes": [{"collection": c, "count": n} for c, n in sorted(self.writes.items())],
            "errors": self.errors,
            "host": socket.gethostname(),
            "parent_run_id": self.parent_run_id,
        }

    # -- safe store I/O ------------------------------------------------------

    def _safe(self, fn) -> None:
        try:
            fn()
        except Exception:  # pragma: no cover - provenance must never break a run
            pass

    # -- context manager -----------------------------------------------------

    def __enter__(self) -> "RunContext":
        if self.store is None:
            self.store = get_default_store()
        self.started_at = _now_iso()
        self.status = "running"
        self._token = _current_run.set(self)
        self._safe(lambda: self.store.save_run(self.to_dict()))
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.finished_at = _now_iso()
        if exc_type is not None:
            self.status = "failed"
            self.add_error(f"{exc_type.__name__}: {exc}")
        elif self.errors:
            self.status = "partial"
        else:
            self.status = "success"
        self._safe(lambda: self.store.save_run(self.to_dict()))
        if self._token is not None:
            _current_run.reset(self._token)
            self._token = None
        return False  # never suppress exceptions
