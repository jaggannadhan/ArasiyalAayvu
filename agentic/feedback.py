"""Feedback Triage — start consuming the `feedback` collection.

Today the backend's ``POST /api/feedback`` writes items to Firestore and nothing
reads them. This module is the first step of the "learn from feedback" loop: it
reads new feedback, classifies and prioritises each item, resolves what entity /
data area it concerns (from ``entity_context`` or the ``page_url``), de-duplicates
near-identical reports, and routes each to a review queue (by transitioning its
``status`` and attaching a ``triage`` record).

It defaults to **suggest mode** — triage + queue only, never auto-editing data
(corrections must be human-verified). With ``act=True`` and a configured
``route_tools`` mapping it can trigger a Module-3 registry tool. Every triage
sweep is wrapped in a Module-2 ``RunContext``.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .provenance import RunContext, RunStore


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Classification vocabulary
# ---------------------------------------------------------------------------

# Words that push a correction / bug to high priority (factual or severe).
_HIGH_SIGNAL = {
    "wrong", "incorrect", "false", "fake", "lie", "lies", "error", "errors",
    "crash", "crashes", "broken", "500", "illegal", "defamatory", "criminal",
    "asset", "assets", "fraud", "urgent", "lawsuit", "wrongly", "misleading",
}

# page_url path prefix -> (data area, collection it concerns)
_PATH_MAP = [
    ("/constituency/", ("constituency", "candidate_accountability")),
    ("/politicians", ("politician", "politician_profile")),
    ("/manifesto-tracker", ("manifesto", "manifesto_promises")),
    ("/state-report/", ("state", "state_finances")),
    ("/sdg-tracker", ("sdg", "socio_economics")),
    ("/news", ("news", "news_articles")),
    ("/spending", ("finance", "departmental_spending")),
    ("/party-history", ("party", "party_accountability")),
    ("/2026_results", ("results", "election_results_2026")),
]

# The routing queue (who handles it) is decided by category — independent of
# which page the feedback was filed from.
_CATEGORY_ROUTE = {
    "correction": "data",
    "missing_data": "data",
    "bug_report": "engineering",
    "suggestion": "product",
    "other": "product",
}


@dataclass
class TriageDecision:
    feedback_id: str
    category: str
    priority: str                 # high | medium | low
    route: str                    # data | engineering | product (who handles it)
    domain: Optional[str] = None  # constituency | manifesto | ... (what it's about)
    target: Optional[str] = None             # slug / doc id
    target_collection: Optional[str] = None
    recommended_action: str = "manual_review"
    recommended_tool: Optional[str] = None
    confidence: float = 0.0
    duplicate_of: Optional[str] = None
    signals: List[str] = field(default_factory=list)

    def as_record(self) -> Dict[str, Any]:
        return {
            "priority": self.priority,
            "route": self.route,
            "domain": self.domain,
            "target": self.target,
            "target_collection": self.target_collection,
            "recommended_action": self.recommended_action,
            "recommended_tool": self.recommended_tool,
            "confidence": self.confidence,
            "duplicate_of": self.duplicate_of,
            "signals": self.signals,
            "triaged_at": _now_iso(),
        }


@dataclass
class TriageReport:
    decisions: List[TriageDecision] = field(default_factory=list)
    run_id: Optional[str] = None

    @property
    def duplicates(self) -> List[TriageDecision]:
        return [d for d in self.decisions if d.duplicate_of]

    def by_priority(self, priority: str) -> List[TriageDecision]:
        return [d for d in self.decisions if d.priority == priority and not d.duplicate_of]

    @property
    def triggered(self) -> List[TriageDecision]:
        return [d for d in self.decisions if d.recommended_tool and d.duplicate_of is None]

    def summary(self) -> str:
        uniq = [d for d in self.decisions if not d.duplicate_of]
        hi = sum(1 for d in uniq if d.priority == "high")
        return (
            f"{len(self.decisions)} feedback · {len(uniq)} unique · "
            f"{len(self.duplicates)} duplicates · {hi} high-priority"
        )


# ---------------------------------------------------------------------------
# Stores
# ---------------------------------------------------------------------------


class FeedbackStore(ABC):
    @abstractmethod
    def list_new(self, limit: int = 100) -> List[Dict[str, Any]]: ...

    @abstractmethod
    def update(self, feedback_id: str, changes: Dict[str, Any]) -> None: ...


class InMemoryFeedbackStore(FeedbackStore):
    def __init__(self, items: Optional[Dict[str, Dict[str, Any]]] = None) -> None:
        self.items: Dict[str, Dict[str, Any]] = dict(items or {})

    def add(self, feedback_id: str, doc: Dict[str, Any]) -> None:
        self.items[feedback_id] = dict(doc)

    def list_new(self, limit: int = 100) -> List[Dict[str, Any]]:
        out = []
        for fid, doc in self.items.items():
            if doc.get("status", "new") == "new":
                out.append({**doc, "id": fid})
        return out[:limit]

    def update(self, feedback_id: str, changes: Dict[str, Any]) -> None:
        self.items.setdefault(feedback_id, {}).update(changes)


class FirestoreFeedbackStore(FeedbackStore):
    COLLECTION = "feedback"

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

    def list_new(self, limit: int = 100) -> List[Dict[str, Any]]:
        from google.cloud.firestore_v1.base_query import FieldFilter

        q = (
            self.client.collection(self.COLLECTION)
            .where(filter=FieldFilter("status", "==", "new"))
            .limit(limit)
        )
        return [{**d.to_dict(), "id": d.id} for d in q.stream()]

    def update(self, feedback_id: str, changes: Dict[str, Any]) -> None:
        self.client.collection(self.COLLECTION).document(feedback_id).set(changes, merge=True)


# ---------------------------------------------------------------------------
# Triager
# ---------------------------------------------------------------------------


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _resolve_target(item: Dict[str, Any]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Return (area, target, collection) from entity_context or page_url."""
    ctx = item.get("entity_context") or {}
    if isinstance(ctx, dict) and (ctx.get("slug") or ctx.get("doc_id")):
        target = ctx.get("slug") or ctx.get("doc_id")
        collection = ctx.get("collection")
        area = ctx.get("area")
        if not (area and collection):
            # fall back to URL mapping for area/collection if context is partial
            a, c = _area_from_url(item.get("page_url", ""))
            area = area or a
            collection = collection or c
        return area, target, collection
    area, collection = _area_from_url(item.get("page_url", ""))
    target = _slug_from_url(item.get("page_url", ""))
    return area, target, collection


def _area_from_url(url: str) -> tuple[Optional[str], Optional[str]]:
    path = re.sub(r"^https?://[^/]+", "", url or "")
    for prefix, (area, collection) in _PATH_MAP:
        if path.startswith(prefix):
            return area, collection
    return None, None


def _slug_from_url(url: str) -> Optional[str]:
    path = re.sub(r"^https?://[^/]+", "", url or "").split("?")[0]
    for prefix, _ in _PATH_MAP:
        if path.startswith(prefix) and prefix.endswith("/"):
            rest = path[len(prefix):].strip("/")
            if rest:
                return rest.split("/")[0]
    return None


class FeedbackTriager:
    def __init__(self, route_tools: Optional[Dict[str, str]] = None) -> None:
        # domain -> registry tool name to invoke when act=True (default: none)
        self.route_tools = route_tools or {}

    def classify(self, item: Dict[str, Any]) -> TriageDecision:
        fid = str(item.get("id"))
        category = (item.get("category") or "other").lower()
        message = _normalize(item.get("message", ""))
        tokens = set(re.findall(r"[a-z0-9]+", message))
        signals: List[str] = []

        route = _CATEGORY_ROUTE.get(category, "product")  # who handles it
        domain, target, collection = _resolve_target(item)  # what it's about

        # priority
        high_hits = tokens & _HIGH_SIGNAL
        priority = "low"
        if category in ("correction", "bug_report", "missing_data"):
            priority = "medium"
        if high_hits:
            priority = "high"
            signals.append("high_signal_terms:" + ",".join(sorted(high_hits)))
        if category in ("suggestion", "other") and not high_hits:
            priority = "low"

        # recommended action
        if category == "correction":
            action = "verify_and_correct"
        elif category == "missing_data":
            action = "schedule_ingest"
        elif category == "bug_report":
            action = "investigate_bug"
        else:
            action = "product_review"

        recommended_tool = self.route_tools.get(domain) if domain else None

        # confidence: stronger when we resolved a concrete target + clear category
        confidence = 0.4
        if target:
            confidence += 0.3
            signals.append(f"target:{target}")
        if collection:
            confidence += 0.1
        if len(message) >= 20:
            confidence += 0.1
        if high_hits:
            confidence += 0.1
        confidence = round(min(confidence, 1.0), 2)

        return TriageDecision(
            feedback_id=fid,
            category=category,
            priority=priority,
            route=route,
            domain=domain,
            target=target,
            target_collection=collection,
            recommended_action=action,
            recommended_tool=recommended_tool,
            confidence=confidence,
            signals=signals,
        )

    def triage_batch(self, items: List[Dict[str, Any]]) -> List[TriageDecision]:
        decisions = [self.classify(it) for it in items]
        # de-dupe: same category + target + similar message head
        seen: Dict[str, str] = {}
        for d, item in zip(decisions, items):
            head = _normalize(item.get("message", ""))[:60]
            sig = f"{d.category}|{d.domain}|{d.target}|{head}"
            if sig in seen:
                d.duplicate_of = seen[sig]
                d.recommended_action = "duplicate"
                d.priority = "low"
            else:
                seen[sig] = d.feedback_id
        return decisions

    def run(
        self,
        store: FeedbackStore,
        *,
        limit: int = 100,
        persist: bool = True,
        act: bool = False,
        registry: Any = None,
        run_store: Optional[RunStore] = None,
    ) -> TriageReport:
        report = TriageReport()
        with RunContext(
            tool="feedback_triage.run",
            trigger="agent",
            args={"limit": limit, "act": act},
            store=run_store,
        ) as run:
            items = store.list_new(limit)
            decisions = self.triage_batch(items)
            report.decisions = decisions

            for d in decisions:
                if persist:
                    new_status = "triaged"
                    store.update(
                        d.feedback_id,
                        {"status": new_status, "triage": d.as_record()},
                    )
                if (
                    act
                    and d.duplicate_of is None
                    and d.recommended_tool
                    and registry is not None
                ):
                    res = registry.invoke(d.recommended_tool, {}, trigger="feedback", store=run_store)
                    d.signals.append(f"triggered:{d.recommended_tool}:{res.status}")

            report.run_id = run.run_id
        return report
