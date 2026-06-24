"""Tests for Module 9 — Critic/Verifier data-quality gate (offline)."""

from __future__ import annotations

from pathlib import Path

import pytest

from agentic.quality import (
    ManifestoHallucinationCheck,
    OutlierCheck,
    RangeCheck,
    Verifier,
    default_verifier,
    verify_docs,
)
from agentic.jobs import verify_job
from agentic.provenance import InMemoryRunStore

REPO = Path(__file__).resolve().parent.parent
PROCESSED = REPO / "data" / "processed"


# ---------------------------------------------------------------------------
# individual checks
# ---------------------------------------------------------------------------


def test_range_check_flags_out_of_range():
    chk = RangeCheck({"party_accountability": {"crorepati_pct": (0, 100)}})
    docs = [{"doc_id": "ok", "crorepati_pct": 86}, {"doc_id": "bad", "crorepati_pct": 350}]
    findings = chk.run("party_accountability", docs, "doc_id")
    assert len(findings) == 1
    assert findings[0].doc_id == "bad" and findings[0].severity == "error"


def test_range_check_ignores_bools_and_missing():
    chk = RangeCheck({"c": {"x": (0, 10)}})
    assert chk.run("c", [{"x": True}, {"y": 5}], None) == []


def test_outlier_check_flags_extreme():
    chk = OutlierCheck({"c": ["v"]})
    docs = [{"id": str(i), "v": 1.0} for i in range(20)] + [{"id": "huge", "v": 100000.0}]
    findings = chk.run("c", docs, "id")
    assert any(f.doc_id == "huge" for f in findings)


def test_consistency_net_assets_and_crorepati_and_severity():
    docs = [{
        "doc_id": "x", "assets_cr": 5.0, "liabilities_cr": 1.0,
        "net_assets_cr": 9.9,           # should be 4.0 -> mismatch
        "is_crorepati": False,          # 5.0 >= 1.0 -> should be True
        "criminal_cases_total": 0, "criminal_severity": "SERIOUS",  # 0 -> CLEAN
    }]
    report = default_verifier().run("candidate_accountability", docs)
    fields = {f.field for f in report.findings}
    assert {"net_assets_cr", "is_crorepati", "criminal_severity"} <= fields
    assert all(f.severity == "warning" for f in report.findings if f.check == "consistency")


def test_socio_percent_out_of_range_is_error():
    docs = [{"metric_id": "m", "unit": "percent", "value": 140}]
    report = default_verifier().run("socio_economics", docs)
    assert any(f.severity == "error" and f.field == "value" for f in report.findings)


def test_manifesto_hallucination_signals():
    chk = ManifestoHallucinationCheck()
    docs = [
        {"doc_id": "short", "promise_text_en": "hi"},
        {"doc_id": "contradiction", "promise_text_en": "Provide free buses to all women in the state.",
         "amount_mentioned": "Rs 500 cr", "fiscal_cost_note": "data unavailable — cannot calculate."},
    ]
    findings = chk.run("manifesto_promises", docs, "doc_id")
    assert any(f.doc_id == "short" for f in findings)
    assert any(f.doc_id == "contradiction" for f in findings)


def test_manifesto_check_scoped_to_collection():
    assert ManifestoHallucinationCheck().run("debt_history", [{"x": 1}], None) == []


# ---------------------------------------------------------------------------
# verifier aggregation + report
# ---------------------------------------------------------------------------


def test_clean_docs_report_ok():
    docs = [{"doc_id": "2021_x", "constituency": "X", "party": "DMK",
             "assets_cr": 5.0, "liabilities_cr": 1.0, "net_assets_cr": 4.0,
             "is_crorepati": True, "criminal_cases_total": 0, "criminal_severity": "CLEAN"}]
    report = verify_docs("candidate_accountability", docs)
    assert report.ok and report.docs_checked == 1


# ---------------------------------------------------------------------------
# integration against the real processed data
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("collection,filename", [
    ("candidate_accountability", "mla_winners.json"),
    ("party_accountability", "party_accountability.json"),
    ("debt_history", "debt_history.json"),
    ("manifesto_promises", "manifesto_promises_2026_dmk.json"),
])
def test_runs_on_real_data(collection, filename):
    import json

    path = PROCESSED / filename
    if not path.exists():
        pytest.skip(f"{filename} missing")
    docs = json.loads(path.read_text(encoding="utf-8"))
    docs = docs if isinstance(docs, list) else list(docs.values())
    report = verify_docs(collection, docs)
    assert report.docs_checked > 0  # runs without crashing; findings allowed


# ---------------------------------------------------------------------------
# verify job core
# ---------------------------------------------------------------------------


def test_verify_job_core_records_run_and_flags_errors():
    written = {}

    def load_docs(collection):
        return {"party_accountability": [{"doc_id": "p", "crorepati_pct": 350}]}.get(collection, [])

    def write_findings(run_id, report):
        written[report.collection] = len(report.findings)

    run_store = InMemoryRunStore()
    reports = verify_job.run_verify(
        collections=["party_accountability"],
        load_docs=load_docs,
        run_store=run_store,
        write_findings=write_findings,
    )
    assert reports[0].errors  # 350% flagged
    assert written["party_accountability"] >= 1
    rec = run_store.list_recent(1)[0]
    assert rec["tool"] == "verify_job"
    assert rec["status"] == "partial"  # errors -> partial run
