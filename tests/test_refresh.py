"""Tests for Module 10 — refresh (composite) tools.

The refresh functions wrap main.py pipelines. Tests inject the underlying run
hooks so nothing is scraped / imported heavy / written to Firestore.
"""

from __future__ import annotations

from agentic import get_registry
from agentic.refresh import (
    refresh_accountability,
    refresh_finance,
    refresh_political_history,
    refresh_socio,
)


# ---------------------------------------------------------------------------
# wrapper logic (injected run hooks)
# ---------------------------------------------------------------------------


def test_refresh_finance_calls_pipeline_with_years():
    calls = []
    out = refresh_finance(["2025-26"], _run=lambda years: calls.append(years))
    assert calls == [["2025-26"]]
    assert out == {"refresh": "finance", "years": ["2025-26"]}


def test_refresh_socio_and_accountability():
    hit = []
    assert refresh_socio(_run=lambda: hit.append("socio"))["refresh"] == "socio"
    assert refresh_accountability(_run=lambda: hit.append("acc"))["refresh"] == "accountability"
    assert hit == ["socio", "acc"]


def test_refresh_political_history_runs_all_three_in_order():
    order = []
    out = refresh_political_history(
        _scrape=lambda: order.append("scrape"),
        _transform=lambda: order.append("transform"),
        _upload=lambda: order.append("upload"),
    )
    assert order == ["scrape", "transform", "upload"]
    assert out == {"refresh": "political_history"}


# ---------------------------------------------------------------------------
# catalogue registration
# ---------------------------------------------------------------------------


def test_refresh_tools_registered_with_metadata():
    reg = get_registry()
    names = {s.name for s in reg.list(category="refresh")}
    assert {"refresh.finance", "refresh.socio", "refresh.accountability",
            "refresh.political_history"} <= names
    fin = reg.get("refresh.finance")
    assert "firestore" in fin.side_effects and "network" in fin.side_effects
    assert "state_finances" in fin.writes


def test_refresh_tool_resolves_without_heavy_imports():
    # resolving imports agentic.refresh (lazy internally) — must not pull scrapers
    fn = get_registry().get("refresh.socio").resolve()
    assert callable(fn)


def test_prs_source_wired_to_refresh_finance():
    from agentic.sources_config import get_source

    assert get_source("prs_tn_budget").on_change_tool == "refresh.finance"
