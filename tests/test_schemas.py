"""Tests for the schemas package (Module 1 — agentic Phase 0).

Two layers of coverage:
  1. Unit tests on the validation engine's behaviour (errors vs. warnings).
  2. Integration tests that validate the *real* processed data files in
     data/processed/ — proving the contracts match what the pipeline produces.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from schemas import (
    UnknownCollection,
    list_collections,
    validate_doc,
    validate_docs,
    validate_json_file,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
PROCESSED = REPO_ROOT / "data" / "processed"


# ---------------------------------------------------------------------------
# Unit: engine behaviour
# ---------------------------------------------------------------------------


def _good_promise() -> dict:
    return {
        "doc_id": "dmk_2026_infra_001",
        "party_id": "dmk",
        "category": "Infrastructure",
        "promise_text_en": "Build roads.",
        "promise_text_ta": "சாலைகள் அமைப்போம்.",
        "target_year": 2026,
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": "2026-01-01T00:00:00+00:00",  # loader metadata, extra field
        "_schema_version": "1.0",
    }


def test_valid_doc_passes():
    res = validate_doc("manifesto_promises", _good_promise())
    assert res.ok
    assert res.errors == []
    assert res.doc_id == "dmk_2026_infra_001"


def test_extra_fields_allowed():
    doc = _good_promise()
    doc["some_brand_new_field"] = {"nested": True}
    res = validate_doc("manifesto_promises", doc)
    assert res.ok


def test_missing_required_field_is_error():
    doc = _good_promise()
    del doc["promise_text_en"]
    res = validate_doc("manifesto_promises", doc)
    assert not res.ok
    assert any("promise_text_en" in e.loc for e in res.errors)


def test_wrong_type_is_error():
    doc = _good_promise()
    doc["target_year"] = "not-a-year"
    res = validate_doc("manifesto_promises", doc)
    assert not res.ok
    assert any(e.loc == "target_year" for e in res.errors)


def test_bad_confidence_literal_is_error():
    doc = _good_promise()
    doc["ground_truth_confidence"] = "PROBABLY"
    res = validate_doc("manifesto_promises", doc)
    assert not res.ok


def test_enum_drift_is_warning_not_error():
    doc = _good_promise()
    doc["implementation_risk"] = "catastrophic"  # outside observed domain
    res = validate_doc("manifesto_promises", doc)
    assert res.ok  # still valid — soft signal only
    assert any(w.loc == "implementation_risk" for w in res.warnings)


def test_known_enum_value_no_warning():
    doc = _good_promise()
    doc["implementation_risk"] = "high"
    res = validate_doc("manifesto_promises", doc)
    assert res.ok
    assert res.warnings == []


def test_unknown_collection_raises():
    with pytest.raises(UnknownCollection):
        validate_doc("not_a_real_collection", {})


def test_batch_report_counts():
    docs = [_good_promise(), {**_good_promise(), "target_year": "bad"}]
    report = validate_docs("manifesto_promises", docs)
    assert report.total == 2
    assert report.valid == 1
    assert len(report.errored) == 1


def test_alliance_uploaded_shape_valid():
    # Mirrors the doc that loaders.upload_alliances constructs from ALLIANCE_DATA.
    doc = {
        "doc_id": "2021_dmk_alliance",
        "year": 2021,
        "alliance_name": "Secular Progressive Alliance",
        "anchor_party": "dmk",
        "member_parties": ["dmk", "inc", "vck"],
        "national_front_alignment": "UPA",
        "outcome": "Won",
        "source_url": "https://www.assembly.tn.gov.in (curated)",
        "ground_truth_confidence": "HIGH",
    }
    assert validate_doc("alliances", doc).ok


def test_candidate_accountability_severity_literal():
    doc = {
        "doc_id": "2021_alangudi",
        "constituency": "ALANGUDI",
        "party": "DMK",
        "criminal_severity": "NUCLEAR",  # invalid literal
    }
    res = validate_doc("candidate_accountability", doc)
    assert not res.ok


# ---------------------------------------------------------------------------
# Integration: validate real processed data files
# ---------------------------------------------------------------------------

# (collection, filename) pairs that exist in data/processed/
REAL_FILES = [
    ("debt_history", "debt_history.json"),
    ("departmental_spending", "departmental_spending.json"),
    ("party_accountability", "party_accountability.json"),
    ("candidate_accountability", "mla_winners.json"),
    ("assembly_elections", "elections.json"),
    # NB: alliances.json on disk is the pre-upload nested {year: [...]} shape;
    # the uploaded doc shape is exercised in test_alliance_uploaded_shape_valid.
    ("manifesto_promises", "manifesto_promises_2026_dmk.json"),
    ("manifesto_promises", "manifesto_promises_2026_aiadmk.json"),
    ("manifesto_promises", "manifesto_promises_2026_ntk.json"),
    ("manifesto_promises", "manifesto_promises_seed.json"),
]


@pytest.mark.parametrize("collection,filename", REAL_FILES)
def test_real_data_files_have_no_schema_errors(collection, filename):
    path = PROCESSED / filename
    if not path.exists():
        pytest.skip(f"{filename} not present")
    report = validate_json_file(collection, path)
    assert report.total > 0
    # Warnings are acceptable (drift signal); hard errors are not.
    assert report.ok, "\n".join(
        f"[{r.doc_id}] {e.loc}: {e.message}"
        for r in report.errored
        for e in r.errors
    )


def test_all_registered_collections_have_id_field_entry():
    from schemas.registry import ID_FIELDS

    for coll in list_collections():
        assert coll in ID_FIELDS, f"{coll} missing from ID_FIELDS"
