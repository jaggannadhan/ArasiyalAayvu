"""Agentic CLI.

    python -m agentic recent [N]    # list the N most recent runs (needs Firestore)
    python -m agentic demo          # offline: run a fake RunContext, print the record
    python -m agentic tools         # list registered pipeline tools
    python -m agentic sources       # list watched sources
    python -m agentic watch         # poll sources for changes (suggest mode)
"""

from __future__ import annotations

import json
import sys
from typing import List, Optional

from .provenance import InMemoryRunStore, RunContext, set_default_store


def _cmd_recent(n: int) -> int:
    from .provenance import get_default_store

    try:
        runs = get_default_store().list_recent(n)
    except Exception as exc:  # no creds / no emulator
        print(f"Could not read runs: {exc}")
        return 1
    for r in runs:
        print(
            f"{r.get('started_at','?')}  {r.get('status','?'):8}  "
            f"{r.get('tool','?'):28}  rows={r.get('rows_written',0)}  {r.get('run_id','')}"
        )
    if not runs:
        print("(no runs recorded yet)")
    return 0


def _cmd_demo() -> int:
    store = InMemoryRunStore()
    set_default_store(store)
    with RunContext(tool="demo_tool", trigger="cli", args={"example": True}) as run:
        run.record_write("manifesto_promises", 42)
        run.record_write("manifesto_promises", 8)
        run.record_write("candidate_accountability", 5)
    print(json.dumps(store.get_run(run.run_id), indent=2))
    return 0


def _cmd_tools() -> int:
    from .tools import get_registry

    for spec in get_registry().list():
        se = ("[" + ",".join(spec.side_effects) + "]") if spec.side_effects else ""
        writes = ("-> " + ",".join(spec.writes)) if spec.writes else ""
        print(f"{spec.name:32} {spec.category:9} {writes} {se}")
    return 0


def _cmd_sources() -> int:
    from .sources_config import get_sources

    for s in get_sources():
        print(f"{s.name:20} {s.detector:13} {s.description}")
    return 0


def _cmd_watch() -> int:
    from .sources import SourceWatcher
    from .sources_config import get_sources

    report = SourceWatcher().poll(get_sources(), persist=False, act=False)
    print(report.summary())
    for r in report.results:
        flag = "CHANGED" if r.changed else ("ERROR" if r.error else "ok")
        print(f"  {flag:8} {r.source:20} {r.reason}{(' — ' + r.error) if r.error else ''}")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in {"-h", "--help"}:
        print(__doc__)
        return 0
    if argv[0] == "recent":
        n = int(argv[1]) if len(argv) > 1 else 20
        return _cmd_recent(n)
    if argv[0] == "demo":
        return _cmd_demo()
    if argv[0] == "tools":
        return _cmd_tools()
    if argv[0] == "sources":
        return _cmd_sources()
    if argv[0] == "watch":
        return _cmd_watch()
    print(f"unknown command: {argv[0]}")
    return 2


raise SystemExit(main())
