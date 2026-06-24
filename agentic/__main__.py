"""Agentic CLI.

    python -m agentic recent [N]    # list the N most recent runs (needs Firestore)
    python -m agentic demo          # offline: run a fake RunContext, print the record
    python -m agentic tools         # list registered pipeline tools
    python -m agentic sources       # list watched sources
    python -m agentic watch         # poll sources for changes (suggest mode)
    python -m agentic triage        # triage new feedback (needs Firestore)
    python -m agentic build-index   # build the GraphRAG vector index (offline)
    python -m agentic ask "..."     # ask a cited question over the KG + manifestos
    python -m agentic verify <collection> <path.json>   # data-quality check a file
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


def _cmd_triage() -> int:
    from .feedback import FeedbackTriager, FirestoreFeedbackStore

    try:
        report = FeedbackTriager().run(FirestoreFeedbackStore(), persist=False)
    except Exception as exc:
        print(f"Could not read feedback: {exc}")
        return 1
    print(report.summary())
    for d in report.by_priority("high") + report.by_priority("medium"):
        print(f"  {d.priority:6} {d.category:13} route={d.route:11} domain={d.domain} target={d.target} -> {d.recommended_action}")
    return 0


def _cmd_build_index() -> int:
    from .graphrag import DEFAULT_INDEX_PATH, build_local_index

    rag = build_local_index()
    print(f"Built index: {len(rag.index.records)} records, dim={rag.index.dim} "
          f"({rag.embedder.name}) -> {DEFAULT_INDEX_PATH}")
    return 0


def _cmd_ask(question: str) -> int:
    from .graphrag import DEFAULT_INDEX_PATH, GraphRAG, build_local_index

    import os

    if os.path.exists(DEFAULT_INDEX_PATH):
        rag = GraphRAG.load(DEFAULT_INDEX_PATH)
    else:
        print("(no prebuilt index — building one in memory...)")
        rag = build_local_index(out_path=None)
    print(rag.answer(question, k=5).answer)
    return 0


def _cmd_verify(collection: str, path: str) -> int:
    import json

    from .quality import verify_docs

    data = json.loads(open(path, encoding="utf-8").read())
    if isinstance(data, list):
        docs = [d for d in data if isinstance(d, dict)]
    elif isinstance(data, dict):
        vals = list(data.values())
        docs = vals if vals and all(isinstance(v, dict) for v in vals) else [data]
    else:
        docs = []
    report = verify_docs(collection, docs)
    print(report.summary())
    for f in report.findings[:60]:
        print(f"  {f.severity:7} {f.check:16} [{f.doc_id}] {f.field}: {f.message}")
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
    if argv[0] == "triage":
        return _cmd_triage()
    if argv[0] == "build-index":
        return _cmd_build_index()
    if argv[0] == "ask":
        if len(argv) < 2:
            print('usage: python -m agentic ask "your question"')
            return 2
        return _cmd_ask(" ".join(argv[1:]))
    if argv[0] == "verify":
        if len(argv) < 3:
            print("usage: python -m agentic verify <collection> <path.json>")
            return 2
        return _cmd_verify(argv[1], argv[2])
    print(f"unknown command: {argv[0]}")
    return 2


raise SystemExit(main())
