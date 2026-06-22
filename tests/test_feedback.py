"""Tests for Module 5 — Feedback Triage (offline)."""

from __future__ import annotations

from agentic import (
    FeedbackTriager,
    InMemoryFeedbackStore,
    ToolRegistry,
)
from agentic.provenance import InMemoryRunStore


def fb(**kw):
    base = {"category": "other", "message": "", "status": "new"}
    base.update(kw)
    base.setdefault("id", "f1")
    return base


# ---------------------------------------------------------------------------
# classification
# ---------------------------------------------------------------------------


def test_correction_with_entity_context():
    t = FeedbackTriager()
    d = t.classify(
        fb(
            id="x",
            category="correction",
            message="His declared assets figure looks off here.",
            entity_context={"slug": "kolathur", "collection": "candidate_accountability", "area": "constituency"},
        )
    )
    assert d.route == "data"
    assert d.domain == "constituency"
    assert d.target == "kolathur"
    assert d.target_collection == "candidate_accountability"
    assert d.recommended_action == "verify_and_correct"
    assert d.priority == "high"  # "assets" + "off" -> high signal ("assets")
    assert d.confidence >= 0.7


def test_high_signal_terms_escalate():
    t = FeedbackTriager()
    d = t.classify(fb(category="correction", message="This is completely wrong and incorrect."))
    assert d.priority == "high"
    assert any("high_signal_terms" in s for s in d.signals)


def test_bug_report_routes_to_engineering_regardless_of_page():
    t = FeedbackTriager()
    # filed from a data page, but a bug still routes to engineering
    d = t.classify(fb(category="bug_report", message="The page crashes on load",
                      page_url="https://site/manifesto-tracker"))
    assert d.route == "engineering"
    assert d.domain == "manifesto"            # content domain still captured
    assert d.recommended_action == "investigate_bug"
    assert d.priority == "high"  # "crash"


def test_suggestion_is_low_product():
    t = FeedbackTriager()
    d = t.classify(fb(category="suggestion", message="Could you add a dark mode please"))
    assert d.route == "product" and d.priority == "low"


def test_missing_data_schedule_ingest():
    t = FeedbackTriager()
    d = t.classify(fb(category="missing_data", message="No data for this constituency"))
    assert d.recommended_action == "schedule_ingest"
    assert d.priority == "medium"


# ---------------------------------------------------------------------------
# URL-based target resolution
# ---------------------------------------------------------------------------


def test_target_from_page_url_constituency():
    t = FeedbackTriager()
    d = t.classify(fb(category="correction", message="x", page_url="https://site/constituency/kolathur"))
    assert d.domain == "constituency"
    assert d.target == "kolathur"
    assert d.target_collection == "candidate_accountability"


def test_target_from_page_url_politicians():
    t = FeedbackTriager()
    d = t.classify(fb(category="correction", message="x", page_url="https://site/politicians"))
    assert d.domain == "politician"
    assert d.target_collection == "politician_profile"


def test_confidence_higher_with_target():
    t = FeedbackTriager()
    with_target = t.classify(fb(category="correction", message="something is off here", page_url="https://s/constituency/x"))
    without = t.classify(fb(category="correction", message="something is off here"))
    assert with_target.confidence > without.confidence


# ---------------------------------------------------------------------------
# de-duplication
# ---------------------------------------------------------------------------


def test_duplicate_detection():
    t = FeedbackTriager()
    items = [
        fb(id="a", category="correction", message="Asset value is wrong", page_url="https://s/constituency/kolathur"),
        fb(id="b", category="correction", message="Asset value is wrong", page_url="https://s/constituency/kolathur"),
        fb(id="c", category="correction", message="Asset value is wrong", page_url="https://s/constituency/other"),
    ]
    decisions = t.triage_batch(items)
    by_id = {d.feedback_id: d for d in decisions}
    assert by_id["b"].duplicate_of == "a"
    assert by_id["b"].recommended_action == "duplicate"
    assert by_id["c"].duplicate_of is None  # different target


# ---------------------------------------------------------------------------
# run(): status transitions + run recording
# ---------------------------------------------------------------------------


def test_run_transitions_status_and_records_run():
    store = InMemoryFeedbackStore()
    store.add("a", fb(id="a", category="correction", message="wrong assets", page_url="https://s/constituency/x"))
    store.add("b", fb(id="b", category="suggestion", message="nice site"))
    store.add("done", fb(id="done", category="other", message="old", status="triaged"))  # ignored

    run_store = InMemoryRunStore()
    report = FeedbackTriager().run(store, run_store=run_store)

    assert len(report.decisions) == 2  # the already-triaged item is skipped
    assert store.items["a"]["status"] == "triaged"
    assert store.items["a"]["triage"]["priority"] == "high"
    assert store.items["done"]["status"] == "triaged"  # unchanged
    # run recorded
    assert run_store.get_run(report.run_id)["tool"] == "feedback_triage.run"


def test_suggest_mode_does_not_invoke_tools():
    store = InMemoryFeedbackStore()
    store.add("a", fb(id="a", category="missing_data", message="missing", page_url="https://s/constituency/x"))
    calls = []
    registry = ToolRegistry()
    registry.register_callable("ingest.demo", lambda: calls.append(1), category="load")
    FeedbackTriager(route_tools={"constituency": "ingest.demo"}).run(
        store, act=False, registry=registry, run_store=InMemoryRunStore()
    )
    assert calls == []


def test_act_triggers_tool_nested_under_run():
    store = InMemoryFeedbackStore()
    store.add("a", fb(id="a", category="missing_data", message="missing data here", page_url="https://s/constituency/x"))
    registry = ToolRegistry()
    registry.register_callable("ingest.demo", lambda: [], category="load")
    run_store = InMemoryRunStore()

    report = FeedbackTriager(route_tools={"constituency": "ingest.demo"}).run(
        store, act=True, registry=registry, run_store=run_store
    )

    d = report.decisions[0]
    assert d.recommended_tool == "ingest.demo"
    assert any("triggered:ingest.demo" in s for s in d.signals)
    # the triggered tool run is nested under the triage run
    child = [r for r in run_store.runs.values() if r["tool"] == "ingest.demo"][0]
    assert child["parent_run_id"] == report.run_id
