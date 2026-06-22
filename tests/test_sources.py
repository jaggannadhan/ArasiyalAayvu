"""Tests for Module 4 — Source Watcher (all offline via injected fetcher)."""

from __future__ import annotations

import json

import pytest

from agentic import (
    InMemorySourceStateStore,
    SourceSpec,
    SourceWatcher,
    ToolRegistry,
)
from agentic.provenance import InMemoryRunStore


class FakeFetch:
    """Injectable fetcher. Map url -> (status, headers, text); mutate between polls."""

    def __init__(self):
        self.responses = {}
        self.fail = set()

    def set(self, url, status=200, headers=None, text=""):
        self.responses[url] = (status, headers or {}, text)

    def set_fail(self, url):
        self.fail.add(url)

    def __call__(self, url, method="GET", timeout=20):
        if url in self.fail:
            raise ConnectionError("network down")
        status, headers, text = self.responses[url]
        return status, headers, (text if method != "HEAD" else "")


# ---------------------------------------------------------------------------
# file_present
# ---------------------------------------------------------------------------


def test_file_present_detects_new_then_not(tmp_path):
    (tmp_path / "sdg_2018.csv").write_text("x")
    (tmp_path / "sdg_2025_26.csv").write_text("y")  # the "new" one
    spec = SourceSpec(
        name="sdg",
        detector="file_present",
        params={"dir": str(tmp_path), "glob": "sdg_*.csv", "known": ["sdg_2018.csv"]},
    )
    w = SourceWatcher(state_store=InMemorySourceStateStore())

    report = w.poll([spec], store=InMemoryRunStore())
    assert report.changed
    assert report.results[0].detail["new_files"] == ["sdg_2025_26.csv"]

    # after persistence the new file is remembered → no longer "new"
    report2 = w.poll([spec], store=InMemoryRunStore())
    assert not report2.changed


def test_file_present_missing_dir_is_not_change(tmp_path):
    spec = SourceSpec(name="x", detector="file_present", params={"dir": str(tmp_path / "nope")})
    res = SourceWatcher().check(spec)
    assert not res.changed
    assert "missing" in res.reason


# ---------------------------------------------------------------------------
# http_hash
# ---------------------------------------------------------------------------


def test_http_hash_baseline_then_change():
    url = "https://example.test/page"
    fetch = FakeFetch()
    fetch.set(url, text="version A")
    w = SourceWatcher(state_store=InMemorySourceStateStore(), fetch=fetch)
    spec = SourceSpec(name="p", detector="http_hash", params={"url": url})

    assert not w.poll([spec], store=InMemoryRunStore()).changed   # baseline
    assert not w.poll([spec], store=InMemoryRunStore()).changed   # unchanged

    fetch.set(url, text="version B")
    assert w.poll([spec], store=InMemoryRunStore()).changed       # changed


def test_http_hash_extract_regex_ignores_noise():
    url = "https://example.test/p"
    fetch = FakeFetch()
    fetch.set(url, text="<table>DATA v1</table><footer>ts=1</footer>")
    spec = SourceSpec(
        name="p", detector="http_hash",
        params={"url": url, "extract_regex": r"<table>.*?</table>"},
    )
    w = SourceWatcher(state_store=InMemorySourceStateStore(), fetch=fetch)
    w.poll([spec], store=InMemoryRunStore())                       # baseline

    # change only the footer (outside the regex) -> no change
    fetch.set(url, text="<table>DATA v1</table><footer>ts=2</footer>")
    assert not w.poll([spec], store=InMemoryRunStore()).changed

    # change inside the table -> change
    fetch.set(url, text="<table>DATA v2</table><footer>ts=2</footer>")
    assert w.poll([spec], store=InMemoryRunStore()).changed


# ---------------------------------------------------------------------------
# http_header / json_field
# ---------------------------------------------------------------------------


def test_http_header_etag_change():
    url = "https://example.test/file"
    fetch = FakeFetch()
    fetch.set(url, headers={"ETag": "abc"})
    w = SourceWatcher(state_store=InMemorySourceStateStore(), fetch=fetch)
    spec = SourceSpec(name="h", detector="http_header", params={"url": url})

    assert not w.poll([spec], store=InMemoryRunStore()).changed   # baseline
    fetch.set(url, headers={"ETag": "def"})
    assert w.poll([spec], store=InMemoryRunStore()).changed


def test_json_field_change():
    url = "https://example.test/api"
    fetch = FakeFetch()
    fetch.set(url, text=json.dumps({"meta": {"last_updated": "2025-01"}}))
    w = SourceWatcher(state_store=InMemorySourceStateStore(), fetch=fetch)
    spec = SourceSpec(name="j", detector="json_field", params={"url": url, "field": "meta.last_updated"})

    assert not w.poll([spec], store=InMemoryRunStore()).changed
    fetch.set(url, text=json.dumps({"meta": {"last_updated": "2025-07"}}))
    assert w.poll([spec], store=InMemoryRunStore()).changed


# ---------------------------------------------------------------------------
# error isolation + run recording
# ---------------------------------------------------------------------------


def test_fetch_error_isolated_and_recorded():
    url = "https://example.test/down"
    fetch = FakeFetch()
    fetch.set_fail(url)
    store = InMemoryRunStore()
    w = SourceWatcher(state_store=InMemorySourceStateStore(), fetch=fetch)
    spec = SourceSpec(name="d", detector="http_hash", params={"url": url})

    report = w.poll([spec], store=store)
    assert report.errored and not report.changed
    assert "network down" in report.results[0].error
    # the poll run is recorded and marked partial (had a non-fatal error)
    assert store.get_run(report.run_id)["status"] == "partial"


def test_unknown_detector():
    res = SourceWatcher().check(SourceSpec(name="x", detector="bogus", params={}))
    assert not res.changed and res.error == "unknown detector"


# ---------------------------------------------------------------------------
# act=True triggers a registry tool, nested under the poll run
# ---------------------------------------------------------------------------


def test_act_triggers_tool_with_parent_linkage(tmp_path):
    (tmp_path / "new.csv").write_text("x")
    registry = ToolRegistry()
    registry.register_callable("ingest.demo", lambda: [], category="load")

    store = InMemoryRunStore()
    w = SourceWatcher(state_store=InMemorySourceStateStore(), registry=registry, fetch=FakeFetch())
    spec = SourceSpec(
        name="s", detector="file_present",
        params={"dir": str(tmp_path), "glob": "*.csv"},
        on_change_tool="ingest.demo",
    )

    report = w.poll([spec], act=True, store=store)
    cr = report.results[0]
    assert cr.changed and cr.triggered["tool"] == "ingest.demo"

    child = store.get_run(cr.triggered["run_id"])
    assert child["parent_run_id"] == report.run_id   # nested under the poll


def test_suggest_mode_does_not_trigger(tmp_path):
    (tmp_path / "new.csv").write_text("x")
    registry = ToolRegistry()
    calls = []
    registry.register_callable("ingest.demo", lambda: calls.append(1), category="load")
    w = SourceWatcher(state_store=InMemorySourceStateStore(), registry=registry, fetch=FakeFetch())
    spec = SourceSpec(name="s", detector="file_present", params={"dir": str(tmp_path), "glob": "*.csv"}, on_change_tool="ingest.demo")

    w.poll([spec], act=False, store=InMemoryRunStore())   # default suggest mode
    assert calls == []  # tool not invoked


# ---------------------------------------------------------------------------
# config catalogue
# ---------------------------------------------------------------------------


def test_config_catalog_loads():
    from agentic.sources_config import get_sources

    names = {s.name for s in get_sources()}
    assert "niti_sdg_csv" in names
    assert all(s.detector in {"file_present", "http_hash", "http_header", "json_field"} for s in get_sources())
