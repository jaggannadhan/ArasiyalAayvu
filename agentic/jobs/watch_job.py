"""Cloud Run Job: poll watched sources for new data (agentic Module 4).

    python -m agentic.jobs.watch_job              # suggest mode (detect + persist)
    python -m agentic.jobs.watch_job --act        # also trigger configured tools
    python -m agentic.jobs.watch_job --dry-run    # read-only preview (no writes)

Default is suggest mode: detect changes and persist fingerprints, but never
trigger a tool. ``--act`` opts into triggering. The whole sweep is one run
record (Module 2).
"""

from __future__ import annotations

import argparse
from typing import Any, List, Optional


def run_watch(
    *,
    watcher: Any = None,
    sources: Optional[List[Any]] = None,
    run_store: Any = None,
    act: bool = False,
    persist: bool = True,
):
    """Testable core. Production deps are built lazily when not injected."""
    if watcher is None:
        from agentic.sources import FirestoreSourceStateStore, SourceWatcher
        from agentic.tools import get_registry

        watcher = SourceWatcher(
            state_store=FirestoreSourceStateStore(),
            registry=get_registry() if act else None,
        )
    if sources is None:
        from agentic.sources_config import get_sources

        sources = get_sources()
    if run_store is None:
        from agentic.provenance import FirestoreRunStore

        run_store = FirestoreRunStore()

    return watcher.poll(sources, persist=persist, act=act, store=run_store)


def _print(report) -> None:
    print(report.summary())
    for r in report.results:
        flag = "CHANGED" if r.changed else ("ERROR" if r.error else "ok")
        line = f"  {flag:8} {r.source}: {r.reason}{(' — ' + r.error) if r.error else ''}"
        if r.triggered:
            line += f"  -> triggered {r.triggered['tool']} ({r.triggered['status']})"
        print(line)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Poll watched sources for new data")
    ap.add_argument("--act", action="store_true", help="trigger configured tools on change")
    ap.add_argument("--dry-run", action="store_true", help="read-only preview (no writes, no triggers)")
    a = ap.parse_args(argv)
    report = run_watch(act=a.act and not a.dry_run, persist=not a.dry_run)
    _print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
