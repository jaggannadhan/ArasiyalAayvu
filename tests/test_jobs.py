"""Tests for Module 7 — agentic job entrypoints (offline, injected deps)."""

from __future__ import annotations

import os

from agentic import (
    FeedbackTriager,
    InMemoryFeedbackStore,
    InMemorySourceStateStore,
    SourceSpec,
    SourceWatcher,
)
from agentic.graphrag import HashingEmbedder
from agentic.jobs import build_index_job, triage_job, watch_job
from agentic.provenance import InMemoryRunStore


def test_watch_job_core_records_run(tmp_path):
    (tmp_path / "new.csv").write_text("x")
    spec = SourceSpec(name="s", detector="file_present", params={"dir": str(tmp_path), "glob": "*.csv"})
    run_store = InMemoryRunStore()
    report = watch_job.run_watch(
        watcher=SourceWatcher(state_store=InMemorySourceStateStore()),
        sources=[spec],
        run_store=run_store,
        act=False,
        persist=False,
    )
    assert report.changed
    assert run_store.get_run(report.run_id)["tool"] == "source_watcher.poll"


def test_triage_job_core_transitions_status():
    store = InMemoryFeedbackStore()
    store.add("a", {"category": "correction", "message": "wrong assets",
                    "status": "new", "page_url": "https://s/constituency/x"})
    run_store = InMemoryRunStore()
    report = triage_job.run_triage(
        store=store, triager=FeedbackTriager(), run_store=run_store, act=False, persist=True
    )
    assert len(report.decisions) == 1
    assert store.items["a"]["status"] == "triaged"
    assert run_store.get_run(report.run_id)["tool"] == "feedback_triage.run"


def test_build_index_job_core_builds_and_uploads():
    captured = {}

    def load_sources():
        nodes = [{"id": "party:dmk", "type": "party", "label": "DMK"}]
        edges = []
        promises = [{"doc_id": "d1", "party_id": "dmk", "target_year": 2026,
                     "category": "Transport", "promise_text_en": "free bus travel"}]
        return nodes, edges, promises

    def upload(path):
        captured["path"] = path
        captured["exists"] = os.path.exists(path)

    run_store = InMemoryRunStore()
    rag = build_index_job.build_and_upload(
        load_sources=load_sources, upload=upload,
        embedder=HashingEmbedder(dim=64), run_store=run_store,
    )
    assert len(rag.index.records) == 2  # 1 node + 1 promise
    assert captured["exists"] is True
    rec = run_store.get_run(rag and run_store.list_recent(1)[0]["run_id"])
    assert rec["tool"] == "build_index_job"
    assert rec["rows_written"] == 2


def test_job_mains_importable():
    # entrypoints expose a main() for `python -m agentic.jobs.*`
    assert callable(watch_job.main)
    assert callable(triage_job.main)
    assert callable(build_index_job.main)
