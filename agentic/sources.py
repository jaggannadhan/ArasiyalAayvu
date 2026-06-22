"""Source Watcher — decide *when* a source has new data.

Generalises ``scrapers/jobs/sdg_check.py`` (a single "is there a new CSV?" check)
into a config-driven detector across many sources, using pluggable strategies:

    file_present  — a new file matching a glob appeared in a directory
    http_hash     — the (optionally regex-extracted) body of a page changed
    http_header   — a resource's ETag / Last-Modified changed
    json_field    — a field in a JSON endpoint changed

Each source's last-seen fingerprint is persisted (``source_state``) so a check
is a comparison against history. A poll is wrapped in a Module-2 ``RunContext``
(so every sweep is audited) and can optionally trigger a Module-3 registry tool
when change is detected — but defaults to *suggest* mode (detect + report only),
matching the "observe / suggest" phases of the roadmap.

Network access is injected (``fetch``) so the detectors are fully unit-testable
offline.
"""

from __future__ import annotations

import hashlib
import json
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .provenance import RunContext, RunStore

Fetch = Callable[..., "tuple[int, Dict[str, str], str]"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_fetch(url: str, method: str = "GET", timeout: int = 20):
    """Lazy default fetcher (keeps `requests` out of import time)."""
    import requests  # lazy

    r = requests.request(method, url, timeout=timeout)
    return r.status_code, dict(r.headers), (r.text if method != "HEAD" else "")


# ---------------------------------------------------------------------------
# Specs / results
# ---------------------------------------------------------------------------


@dataclass
class SourceSpec:
    name: str
    detector: str
    params: Dict[str, Any]
    description: str = ""
    on_change_tool: Optional[str] = None
    on_change_args: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)


@dataclass
class DetectOutcome:
    changed: bool
    reason: str
    state: Dict[str, Any]
    detail: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


@dataclass
class ChangeResult:
    source: str
    changed: bool
    reason: str
    detail: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    triggered: Optional[Dict[str, Any]] = None


@dataclass
class WatchReport:
    results: List[ChangeResult] = field(default_factory=list)
    run_id: Optional[str] = None

    @property
    def changed(self) -> List[ChangeResult]:
        return [r for r in self.results if r.changed]

    @property
    def errored(self) -> List[ChangeResult]:
        return [r for r in self.results if r.error]

    def summary(self) -> str:
        return (
            f"{len(self.changed)}/{len(self.results)} sources changed · "
            f"{len(self.errored)} errors"
        )


# ---------------------------------------------------------------------------
# State stores
# ---------------------------------------------------------------------------


class SourceStateStore(ABC):
    @abstractmethod
    def get_state(self, source: str) -> Optional[Dict[str, Any]]: ...

    @abstractmethod
    def set_state(self, source: str, state: Dict[str, Any]) -> None: ...


class InMemorySourceStateStore(SourceStateStore):
    def __init__(self) -> None:
        self._s: Dict[str, Dict[str, Any]] = {}

    def get_state(self, source):
        v = self._s.get(source)
        return dict(v) if v else None

    def set_state(self, source, state):
        self._s[source] = dict(state)


class FirestoreSourceStateStore(SourceStateStore):
    COLLECTION = "source_state"

    def __init__(self, client: Any = None, project: Optional[str] = None) -> None:
        self._client = client
        self._project = project

    @property
    def client(self):
        if self._client is None:
            import os

            from google.cloud import firestore  # lazy

            self._client = firestore.Client(
                project=self._project or os.environ.get("GOOGLE_CLOUD_PROJECT", "naatunadappu")
            )
        return self._client

    def get_state(self, source):
        snap = self.client.collection(self.COLLECTION).document(source).get()
        return snap.to_dict() if snap.exists else None

    def set_state(self, source, state):
        self.client.collection(self.COLLECTION).document(source).set(state, merge=True)


# ---------------------------------------------------------------------------
# Detectors
# ---------------------------------------------------------------------------


class Detector(ABC):
    @abstractmethod
    def detect(self, spec: SourceSpec, previous: Optional[Dict[str, Any]], fetch: Fetch) -> DetectOutcome: ...


class FilePresentDetector(Detector):
    """New file matching a glob in a directory (generalises sdg_check)."""

    def detect(self, spec, previous, fetch):
        base = Path(spec.params["dir"])
        glob = spec.params.get("glob", "*")
        known = set(spec.params.get("known", [])) | set((previous or {}).get("known", []))
        if not base.exists():
            return DetectOutcome(False, f"directory missing: {base}", {"known": sorted(known)})
        files = sorted(p.name for p in base.glob(glob))
        new_files = [f for f in files if f not in known]
        state = {"known": sorted(set(files) | known)}
        if new_files:
            return DetectOutcome(True, f"{len(new_files)} new file(s)", state, {"new_files": new_files})
        return DetectOutcome(False, "no new files", state, {"file_count": len(files)})


class HttpHashDetector(Detector):
    """Body of a page changed (optionally narrowed by an extract_regex)."""

    def detect(self, spec, previous, fetch):
        url = spec.params["url"]
        status, _headers, text = fetch(url)
        if status >= 400:
            return DetectOutcome(False, f"http {status}", previous or {}, {"status": status}, error=f"http {status}")
        rx = spec.params.get("extract_regex")
        content = text
        if rx:
            m = re.search(rx, text, re.S)
            content = m.group(0) if m else text
        h = hashlib.sha256(content.encode("utf-8", "ignore")).hexdigest()
        state = {"hash": h, "fetched_at": _now_iso()}
        prev = (previous or {}).get("hash")
        if prev is None:
            return DetectOutcome(False, "baseline established", state, {"hash": h[:12]})
        if h != prev:
            return DetectOutcome(True, "content changed", state, {"old": prev[:12], "new": h[:12]})
        return DetectOutcome(False, "unchanged", state, {"hash": h[:12]})


class HttpHeaderDetector(Detector):
    """ETag / Last-Modified changed (cheap HEAD request)."""

    def detect(self, spec, previous, fetch):
        url = spec.params["url"]
        status, headers, _ = fetch(url, method="HEAD")
        if status >= 400:
            return DetectOutcome(False, f"http {status}", previous or {}, {"status": status}, error=f"http {status}")
        norm = {k.lower(): v for k, v in (headers or {}).items()}
        etag, lm = norm.get("etag"), norm.get("last-modified")
        state = {"etag": etag, "last_modified": lm}
        prev = previous or {}
        if not prev.get("etag") and not prev.get("last_modified"):
            return DetectOutcome(False, "baseline established", state, {"etag": etag, "last_modified": lm})
        if etag != prev.get("etag") or lm != prev.get("last_modified"):
            return DetectOutcome(True, "header changed", state, {"etag": etag, "last_modified": lm})
        return DetectOutcome(False, "unchanged", state)


def _dotted_get(data: Any, path: str) -> Any:
    cur = data
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


class JsonFieldDetector(Detector):
    """A field in a JSON endpoint changed (e.g. a 'last_updated' value)."""

    def detect(self, spec, previous, fetch):
        url = spec.params["url"]
        field_path = spec.params["field"]
        status, _headers, text = fetch(url)
        if status >= 400:
            return DetectOutcome(False, f"http {status}", previous or {}, {"status": status}, error=f"http {status}")
        try:
            value = _dotted_get(json.loads(text), field_path)
        except json.JSONDecodeError as exc:
            return DetectOutcome(False, "invalid json", previous or {}, {}, error=str(exc))
        state = {"value": value}
        prev = previous or {}
        if "value" not in prev:
            return DetectOutcome(False, "baseline established", state, {"value": value})
        if value != prev.get("value"):
            return DetectOutcome(True, "field changed", state, {"old": prev.get("value"), "new": value})
        return DetectOutcome(False, "unchanged", state, {"value": value})


# ---------------------------------------------------------------------------
# Watcher
# ---------------------------------------------------------------------------


class SourceWatcher:
    def __init__(
        self,
        state_store: Optional[SourceStateStore] = None,
        registry: Any = None,
        fetch: Optional[Fetch] = None,
    ) -> None:
        self.state_store = state_store or InMemorySourceStateStore()
        self.registry = registry
        self.fetch = fetch or _default_fetch
        self.detectors: Dict[str, Detector] = {
            "file_present": FilePresentDetector(),
            "http_hash": HttpHashDetector(),
            "http_header": HttpHeaderDetector(),
            "json_field": JsonFieldDetector(),
        }

    def _run_detector(self, spec: SourceSpec, prev: Optional[Dict[str, Any]]) -> DetectOutcome:
        det = self.detectors.get(spec.detector)
        if det is None:
            return DetectOutcome(False, f"unknown detector: {spec.detector}", prev or {}, error="unknown detector")
        try:
            return det.detect(spec, prev, self.fetch)
        except Exception as exc:  # detector/network failure isolated per source
            return DetectOutcome(False, "detector error", prev or {}, error=f"{type(exc).__name__}: {exc}")

    def check(self, spec: SourceSpec) -> ChangeResult:
        """Single source, no persistence, no side effects (pure read)."""
        outcome = self._run_detector(spec, self.state_store.get_state(spec.name))
        return ChangeResult(spec.name, outcome.changed, outcome.reason, outcome.detail, outcome.error)

    def poll(
        self,
        specs: List[SourceSpec],
        *,
        persist: bool = True,
        act: bool = False,
        store: Optional[RunStore] = None,
    ) -> WatchReport:
        """Check every source, persist new fingerprints, optionally trigger tools.

        The whole sweep is one ``RunContext`` run; any triggered tool invocations
        nest under it (parent_run_id linkage via contextvars).
        """
        report = WatchReport()
        with RunContext(
            tool="source_watcher.poll",
            trigger="watcher",
            args={"sources": len(specs), "act": act},
            store=store,
        ) as run:
            for spec in specs:
                prev = self.state_store.get_state(spec.name)
                outcome = self._run_detector(spec, prev)
                cr = ChangeResult(spec.name, outcome.changed, outcome.reason, outcome.detail, outcome.error)

                if persist and outcome.error is None:
                    self.state_store.set_state(
                        spec.name,
                        {**outcome.state, "last_checked": _now_iso(), "last_changed": outcome.changed},
                    )

                if outcome.changed and act and spec.on_change_tool and self.registry is not None:
                    res = self.registry.invoke(
                        spec.on_change_tool, spec.on_change_args, trigger="watcher", store=store
                    )
                    cr.triggered = {"tool": spec.on_change_tool, "run_id": res.run_id, "status": res.status}

                if outcome.error:
                    run.add_error(f"{spec.name}: {outcome.error}")
                report.results.append(cr)

            report.run_id = run.run_id
        return report
