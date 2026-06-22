"""Tests for Module 3 — Tool Registry.

Covers discovery, lazy resolution, invocation wrapped in a RunContext, output
validation against Module-1 schemas, error capture, and arg summarisation.
"""

from __future__ import annotations

import pytest

from agentic import ToolRegistry, ToolSpec, UnknownTool, get_registry
from agentic.provenance import InMemoryRunStore


# ---------------------------------------------------------------------------
# Catalogue discovery (uses the process-wide registry)
# ---------------------------------------------------------------------------


def test_catalog_registers_transform_and_load_tools():
    reg = get_registry()
    names = reg.names()
    assert "transform.debt_history" in names
    assert "load.mla_winners" in names
    # transform tools have no side effects; load tools write to Firestore
    assert reg.get("transform.debt_history").side_effects == []
    assert "firestore" in reg.get("load.mla_winners").side_effects


def test_list_filters_by_category_and_tag():
    reg = get_registry()
    assert all(s.category == "load" for s in reg.list(category="load"))
    assert all("finance" in s.tags for s in reg.list(tag="finance"))
    assert reg.list(category="transform")  # non-empty


def test_describe_all_is_serialisable():
    rows = get_registry().describe_all()
    assert rows and all("name" in r and "category" in r for r in rows)


# ---------------------------------------------------------------------------
# Real transformer invocation (pure, no external deps)
# ---------------------------------------------------------------------------


def test_invoke_real_transformer_validates_output():
    reg = get_registry()
    store = InMemoryRunStore()
    # build_debt_history_series([]) returns the 7-year curated series.
    result = reg.invoke("transform.debt_history", {"raw_docs": []}, store=store)

    assert result.ok
    assert result.status == "success"
    assert isinstance(result.output, list) and len(result.output) == 7
    assert result.output_validation["ok"] is True
    assert result.output_validation["collection"] == "debt_history"
    # a run record was written for the invocation
    assert store.get_run(result.run_id)["tool"] == "transform.debt_history"


# ---------------------------------------------------------------------------
# Error handling + output validation surfacing (in-process dummy tools)
# ---------------------------------------------------------------------------


def test_invoke_captures_errors_as_result_not_raise():
    reg = ToolRegistry()

    def boom():
        raise RuntimeError("nope")

    reg.register_callable("dummy.boom", boom, category="util")
    store = InMemoryRunStore()
    result = reg.invoke("dummy.boom", store=store)

    assert not result.ok
    assert result.status == "failed"
    assert "RuntimeError: nope" in result.error
    # the run was recorded as failed too
    assert store.get_run(result.run_id)["status"] == "failed"


def test_invoke_flags_invalid_output_docs():
    reg = ToolRegistry()

    def make_bad():
        return [{"not": "a valid debt_history doc"}]  # missing fiscal_year

    reg.register_callable(
        "dummy.bad_debt", make_bad, category="transform", output_collection="debt_history"
    )
    result = reg.invoke("dummy.bad_debt", store=InMemoryRunStore())

    assert result.status == "success"  # the function itself ran fine
    assert result.output_validation["ok"] is False
    assert result.output_validation["errors"]


def test_args_are_summarised_in_run_record():
    reg = ToolRegistry()

    def passthrough(items):
        return items

    reg.register_callable("dummy.pass", passthrough, category="util")
    store = InMemoryRunStore()
    big = [{"i": i} for i in range(1234)]
    result = reg.invoke("dummy.pass", {"items": big}, store=store)

    rec = store.get_run(result.run_id)
    assert rec["args"] == {"items": "list[1234]"}  # shape, not the payload


# ---------------------------------------------------------------------------
# Registry guards
# ---------------------------------------------------------------------------


def test_unknown_tool_raises():
    with pytest.raises(UnknownTool):
        get_registry().get("nope.nope")


def test_duplicate_registration_raises():
    reg = ToolRegistry()
    reg.register(ToolSpec(name="x", summary="", category="util", func=lambda: 1))
    with pytest.raises(ValueError):
        reg.register(ToolSpec(name="x", summary="", category="util", func=lambda: 2))


def test_resolve_real_path():
    spec = get_registry().get("transform.departmental_spending")
    fn = spec.resolve()
    assert callable(fn)
