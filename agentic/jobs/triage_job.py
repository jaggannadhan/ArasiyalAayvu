"""Cloud Run Job: triage new feedback (agentic Module 5).

    python -m agentic.jobs.triage_job             # triage + queue (status -> triaged)
    python -m agentic.jobs.triage_job --act       # also trigger configured tools
    python -m agentic.jobs.triage_job --dry-run   # read-only preview (no status changes)

Default is suggest mode: classify, de-dupe and route to the review queue, but
never auto-apply corrections.
"""

from __future__ import annotations

import argparse
from typing import Any, Dict, Optional


def run_triage(
    *,
    store: Any = None,
    triager: Any = None,
    run_store: Any = None,
    act: bool = False,
    persist: bool = True,
    limit: int = 200,
    route_tools: Optional[Dict[str, str]] = None,
):
    """Testable core. Production deps are built lazily when not injected."""
    if store is None:
        from agentic.feedback import FirestoreFeedbackStore

        store = FirestoreFeedbackStore()
    if triager is None:
        from agentic.feedback import FeedbackTriager

        triager = FeedbackTriager(route_tools=route_tools or {})
    if run_store is None:
        from agentic.provenance import FirestoreRunStore

        run_store = FirestoreRunStore()
    registry = None
    if act:
        from agentic.tools import get_registry

        registry = get_registry()

    return triager.run(
        store, limit=limit, persist=persist, act=act, registry=registry, run_store=run_store
    )


def _print(report) -> None:
    print(report.summary())
    for d in report.by_priority("high") + report.by_priority("medium"):
        print(f"  {d.priority:6} {d.category:13} route={d.route:11} domain={d.domain} target={d.target} -> {d.recommended_action}")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Triage new feedback")
    ap.add_argument("--act", action="store_true", help="trigger configured tools for high-confidence items")
    ap.add_argument("--dry-run", action="store_true", help="read-only preview (no status changes)")
    ap.add_argument("--limit", type=int, default=200)
    a = ap.parse_args(argv)
    report = run_triage(act=a.act and not a.dry_run, persist=not a.dry_run, limit=a.limit)
    _print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
