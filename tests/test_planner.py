"""Tests for Module 11 — plan-only Orchestrator planner (offline)."""

from __future__ import annotations

import pytest

from agentic import InMemoryPlanStore, Planner, ToolRegistry
from agentic.planner import PlanCycleError, PlanError, UnknownGoalTool


# ---------------------------------------------------------------------------
# dependency derivation from real catalogue tools
# ---------------------------------------------------------------------------


def test_derives_edge_from_reads_writes():
    # transform.party_accountability READS candidate_accountability;
    # refresh.accountability WRITES it -> a dependency edge + two stages.
    plan = Planner().plan("acc refresh", ["refresh.accountability", "transform.party_accountability"])
    by_tool = {t.tool: t for t in plan.tasks}
    tp = by_tool["transform.party_accountability"]
    assert by_tool["refresh.accountability"].task_id in tp.depends_on
    assert by_tool["refresh.accountability"].stage == 0
    assert tp.stage == 1


def test_independent_tools_share_stage_zero():
    plan = Planner().plan("two refreshes", ["refresh.finance", "refresh.socio"])
    assert all(t.stage == 0 and not t.depends_on for t in plan.tasks)
    assert len(plan.stages()) == 1


# ---------------------------------------------------------------------------
# risk + approval gating
# ---------------------------------------------------------------------------


def test_high_risk_forces_approval_even_in_act_mode():
    plan = Planner().plan("acc", ["refresh.accountability"], mode="act")
    assert plan.tasks[0].risk == "high"          # writes candidate_accountability
    assert plan.requires_approval and plan.status == "awaiting_approval"


def test_low_risk_act_mode_is_planned():
    plan = Planner().plan("debt", ["transform.debt_history"], mode="act")
    assert plan.tasks[0].risk == "low"           # no side effects, not high-risk coll
    assert not plan.requires_approval and plan.status == "planned"


def test_suggest_mode_always_awaits_approval():
    plan = Planner().plan("debt", ["transform.debt_history"], mode="suggest")
    assert plan.requires_approval and plan.status == "awaiting_approval"


def test_side_effect_tool_is_medium_risk():
    plan = Planner().plan("fin", ["refresh.finance"], mode="act")
    assert plan.tasks[0].risk == "medium"        # network+firestore, not high-risk coll


# ---------------------------------------------------------------------------
# cycle detection (dummy registry)
# ---------------------------------------------------------------------------


def test_cycle_is_rejected():
    reg = ToolRegistry()
    reg.register_callable("a", lambda: None, category="util", reads=["X"], writes=["Y"])
    reg.register_callable("b", lambda: None, category="util", reads=["Y"], writes=["X"])
    with pytest.raises(PlanCycleError):
        Planner(registry=reg).plan("cycle", ["a", "b"])


def test_toposort_order_respects_dependencies():
    from agentic.planner import _toposort

    plan = Planner().plan("chain", ["refresh.accountability", "transform.party_accountability"])
    order = [t.task_id for t in _toposort(plan.tasks)]
    ref = next(t for t in plan.tasks if t.tool == "refresh.accountability").task_id
    tp = next(t for t in plan.tasks if t.tool == "transform.party_accountability").task_id
    assert order.index(ref) < order.index(tp)


# ---------------------------------------------------------------------------
# guards
# ---------------------------------------------------------------------------


def test_unknown_tool_raises():
    with pytest.raises(UnknownGoalTool):
        Planner().plan("x", ["not.a.tool"])


def test_empty_tools_raises():
    with pytest.raises(PlanError):
        Planner().plan("x", [])


# ---------------------------------------------------------------------------
# persistence + schema conformance
# ---------------------------------------------------------------------------


def test_plan_persists_and_validates_against_schema():
    from schemas import validate_doc

    store = InMemoryPlanStore()
    plan = Planner(store=store).plan(
        "acc", ["refresh.accountability", "transform.party_accountability"]
    )
    assert plan.plan_id in store.plans
    assert len(store.tasks) == 2

    assert validate_doc("plans", plan.to_doc()).ok
    for t in plan.tasks:
        res = validate_doc("plan_tasks", t.to_doc(plan.plan_id))
        assert res.ok, [(e.loc, e.message) for e in res.errors]
