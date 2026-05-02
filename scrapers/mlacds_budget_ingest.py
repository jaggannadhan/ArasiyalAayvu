"""
MLACDS Budget Ingest — ArasiyalAayvu
======================================
Loads verified MLACDS (Member of Legislative Assembly Constituency Development
Scheme) budget data into Firestore `mlacds_budget` collection.

Data covers fiscal years 2011-12 through 2025-26 (three assembly terms:
2011-2016 AIADMK, 2016-2021 AIADMK, 2021-2026 DMK).

Every record includes exact source citations — government policy notes,
official TNRD pages, or verified news reports.

Firestore schema
----------------
  Collection: mlacds_budget
  Doc ID:     {fiscal_year}   e.g. "2021-22"

Usage
-----
  # Dry run — preview records
  .venv/bin/python scrapers/mlacds_budget_ingest.py --dry-run

  # Upload to Firestore
  .venv/bin/python scrapers/mlacds_budget_ingest.py --upload

  # Validate totals only
  .venv/bin/python scrapers/mlacds_budget_ingest.py --validate
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "data" / "processed"
PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "naatunadappu")

NUM_CONSTITUENCIES = 234


# ---------------------------------------------------------------------------
# Source URLs — authoritative references for each data point
# ---------------------------------------------------------------------------

SOURCES = {
    "tnrd_mlacds_page": {
        "title": "TNRD MLACDS Scheme Page",
        "url": "https://tnrd.tn.gov.in/schemes/st_mlacds.php",
        "type": "government",
    },
    "tnrd_reports_page": {
        "title": "TNRD MLACDS Reports — Physical & Financial Performance",
        "url": "https://tnrd.tn.gov.in/rdweb_newsite/project/reports/Public/public_page_table_content_details_view.php?tabular_content_id=NQ%3D%3D&page_id=Ng%3D%3D",
        "type": "government",
    },
    "policy_note_2024_25": {
        "title": "TNRD Policy Note 2024-25, Demand No. 42, Section 3.4 (pp. 76-77)",
        "url": "https://tnrd.tn.gov.in/policynotes/rd1_e_pn_2024_25.pdf",
        "type": "government",
    },
    "policy_note_2021_22": {
        "title": "TNRD Policy Note 2021-22, Demand No. 42, Section 2.2 (p. 11)",
        "url": "https://tnrd.tn.gov.in/policynotes/42-RD-POLICY%20NOTE-ENGLSIH_2021_2022.pdf",
        "type": "government",
    },
    "policy_note_2019_20": {
        "title": "TNRD Policy Note 2019-20, Demand No. 42",
        "url": "https://tnrd.tn.gov.in/policynotes/42%20-%20RDPR%20-%20POLICY%20NOTE%20-%202019%20-%20ENGLISH.pdf",
        "type": "government",
    },
    "policy_note_2018_19": {
        "title": "TNRD Policy Note 2018-19, Demand No. 42",
        "url": "https://tnrd.tn.gov.in/policynotes/rd_e_pn_2018_19.pdf",
        "type": "government",
    },
    "policy_note_2017_18": {
        "title": "TNRD Policy Note 2017-18, Demand No. 42",
        "url": "https://tnrd.tn.gov.in/policynotes/Policy%20Note%20-%202017-18%20in%20English.pdf",
        "type": "government",
    },
    "policy_note_2016_17": {
        "title": "TNRD Policy Note 2016-17, Demand No. 42",
        "url": "https://tnrd.tn.gov.in/policynotes/Policy%20Note%20-%202016-17%20in%20English.pdf",
        "type": "government",
    },
    "policy_note_2015_16": {
        "title": "TNRD Policy Note 2015-16, Demand No. 42",
        "url": "https://tnrd.tn.gov.in/policynotes/ENGLISH%20POLICY%20NOTE%202015-16.pdf",
        "type": "government",
    },
    "policy_note_2014_15": {
        "title": "TNRD Policy Note 2014-15, Demand No. 42",
        "url": "https://tnrd.tn.gov.in/policynotes/ENGLISH%20POLICY%20NOTE%202014-15.pdf",
        "type": "government",
    },
    "krishnagiri_nic": {
        "title": "Krishnagiri District NIC — MLACDS Scheme Page",
        "url": "https://krishnagiri.nic.in/scheme/member-of-legislative-assembly-constituency-development-scheme/",
        "type": "government",
    },
    "dtnext_352cr_2021": {
        "title": "DTNext — TN Rs 352 cr MLA fund released, guidelines issued",
        "url": "https://www.dtnext.in/news/tamilnadu/tamil-nadu-rs-352-cr-mla-fund-released-guidelines-issued-for-works",
        "type": "news",
    },
    "dtnext_3cr_discretionary_2024": {
        "title": "DTNext — MLAs allowed to spend Rs 3 crore on work of their choice",
        "url": "https://www.dtnext.in/news/tamilnadu/mlas-would-be-allowed-to-spend-rs-3-crore-on-work-of-their-choice-in-their-constituency-minister-duraimurugan-792031",
        "type": "news",
    },
}


def _src(key: str) -> dict:
    """Return a source dict for embedding in a record."""
    return SOURCES[key]


# ---------------------------------------------------------------------------
# MLACDS Budget Records — verified from official sources
# ---------------------------------------------------------------------------

def _build_records() -> list[dict]:
    """
    Build all MLACDS budget records from 2011-12 to 2025-26.

    Each record is manually curated from official government policy notes,
    TNRD reports, and verified news sources. No scraped/estimated data.
    """
    records: list[dict] = []

    # ── Term 1: 2011-2016 (AIADMK — Jayalalithaa) ─────────────────────────
    # Allocation: ₹2.00 crore per constituency (increased from ₹1.75 crore)
    # Source: TNRD MLACDS page confirms "increased from Rs.1.75 crore to
    #         Rs.2.00 crore per Constituency per annum from 2011-2012 onwards"
    # Tied: ₹1.10 crore; Untied: ₹0.90 crore

    for fy in ["2011-12", "2012-13", "2013-14", "2014-15", "2015-16"]:
        rec = _base_record(fy)
        rec["assembly_term"] = "2011-2016"
        rec["ruling_party"] = "AIADMK"
        rec["per_constituency_allocation_cr"] = 2.00
        rec["state_total_allocation_cr"] = NUM_CONSTITUENCIES * 2.00  # 468
        rec["tied_fund_cr"] = 1.10
        rec["untied_fund_cr"] = 0.90
        rec["tied_components"] = {
            "school_infrastructure_cr": 0.25,
            "anganwadi_noon_meal_cr": 0.20,
            "water_supply_cr": 0.15,
            "other_permissible_cr": 0.50,
        }
        rec["sc_st_earmark_pct"] = 22.0
        rec["sources"] = [
            _src("tnrd_mlacds_page"),
            _src("policy_note_2014_15"),
            _src("policy_note_2015_16"),
        ]
        rec["source_notes"] = (
            "TNRD MLACDS page confirms ₹2.00 Cr allocation from 2011-12; "
            "tied ₹1.10 Cr / untied ₹0.90 Cr split from same page. "
            "TNRD page shows total allocation as ₹470 Cr (vs computed 234×2=₹468 Cr; "
            "₹2 Cr difference likely administrative overhead)."
        )

        # Add performance data where available
        if fy == "2015-16":
            rec["performance"] = {
                "total_allocation_cr": 470.00,
                "works_initiated": 14684,
                "total_expenditure_cr": 469.40,
                "utilization_pct": round(469.40 / 470.00 * 100, 1),
                "category_wise": {
                    "buildings": {"works": 3463, "expenditure_cr": 190.20},
                    "roads": {"works": 2908, "expenditure_cr": 110.74},
                    "water_supply": {"works": 2850, "expenditure_cr": 70.00},
                    "procurement_supply": {"works": 3431, "expenditure_cr": 50.69},
                    "cd_irrigation": {"works": 1155, "expenditure_cr": 40.01},
                },
            }
            rec["sources"].append(_src("tnrd_mlacds_page"))
            rec["performance_source"] = (
                "TNRD MLACDS page — year-wise performance data 2015-16"
            )

        records.append(rec)

    # 2016-17 — transition year (AIADMK still in power, election in May 2016)
    rec = _base_record("2016-17")
    rec["assembly_term"] = "2016-2021"
    rec["ruling_party"] = "AIADMK"
    rec["per_constituency_allocation_cr"] = 2.00
    rec["state_total_allocation_cr"] = NUM_CONSTITUENCIES * 2.00  # 468
    rec["tied_fund_cr"] = 1.10
    rec["untied_fund_cr"] = 0.90
    rec["tied_components"] = {
        "school_infrastructure_cr": 0.25,
        "anganwadi_noon_meal_cr": 0.20,
        "water_supply_cr": 0.15,
        "other_permissible_cr": 0.50,
    }
    rec["sc_st_earmark_pct"] = 22.0
    rec["sources"] = [
        _src("tnrd_mlacds_page"),
        _src("policy_note_2016_17"),
    ]
    rec["source_notes"] = (
        "TNRD MLACDS page confirms ₹2.00 Cr allocation for 2016-17; "
        "total allocation ₹470 Cr. Last year at ₹2 Cr before hike to ₹2.5 Cr."
    )
    rec["performance"] = {
        "total_allocation_cr": 470.00,
    }
    records.append(rec)

    # 2017-18 — allocation increased to ₹2.50 crore
    rec = _base_record("2017-18")
    rec["assembly_term"] = "2016-2021"
    rec["ruling_party"] = "AIADMK"
    rec["per_constituency_allocation_cr"] = 2.50
    rec["state_total_allocation_cr"] = NUM_CONSTITUENCIES * 2.50  # 585
    rec["tied_fund_cr"] = 1.50
    rec["untied_fund_cr"] = 1.00
    rec["tied_components"] = {
        "school_infrastructure_cr": 0.20,
        "other_priority_works_cr": 1.30,
    }
    rec["sc_st_earmark_pct"] = 22.0
    rec["sources"] = [
        _src("krishnagiri_nic"),
        _src("policy_note_2017_18"),
    ]
    rec["source_notes"] = (
        "Krishnagiri NIC district page confirms allocation increased from "
        "₹2.00 Cr to ₹2.50 Cr for 2017-18. Tied ₹1.50 Cr / Untied ₹1.00 Cr."
    )
    records.append(rec)

    # 2018-19 — same ₹2.50 crore
    rec = _base_record("2018-19")
    rec["assembly_term"] = "2016-2021"
    rec["ruling_party"] = "AIADMK"
    rec["per_constituency_allocation_cr"] = 2.50
    rec["state_total_allocation_cr"] = NUM_CONSTITUENCIES * 2.50  # 585
    rec["tied_fund_cr"] = 1.50
    rec["untied_fund_cr"] = 1.00
    rec["tied_components"] = {
        "school_infrastructure_cr": 0.20,
        "other_priority_works_cr": 1.30,
    }
    rec["sc_st_earmark_pct"] = 22.0
    rec["sources"] = [
        _src("krishnagiri_nic"),
        _src("policy_note_2018_19"),
    ]
    rec["source_notes"] = (
        "Continuation of ₹2.50 Cr allocation. Krishnagiri NIC and "
        "Policy Note 2018-19 confirm same structure as 2017-18."
    )
    records.append(rec)

    # 2019-20 — increased to ₹3.00 crore
    rec = _base_record("2019-20")
    rec["assembly_term"] = "2016-2021"
    rec["ruling_party"] = "AIADMK"
    rec["per_constituency_allocation_cr"] = 3.00
    rec["state_total_allocation_cr"] = NUM_CONSTITUENCIES * 3.00  # 702
    rec["tied_fund_cr"] = 1.50
    rec["untied_fund_cr"] = 1.50
    rec["tied_components"] = {
        "school_infrastructure_cr": 0.25,
        "anganwadi_noon_meal_cr": 0.25,
        "water_supply_cr": 0.15,
        "differently_abled_cr": 0.05,
        "other_priority_works_cr": 0.80,
    }
    rec["sc_st_earmark_pct"] = 22.0
    rec["sources"] = [
        _src("dtnext_352cr_2021"),
        _src("policy_note_2019_20"),
    ]
    rec["source_notes"] = (
        "DTNext confirms fund enhanced to ₹3 Cr in 2019. "
        "Tied ₹1.50 Cr / Untied ₹1.50 Cr. "
        "22% earmarked for SC/ST areas, 30% for priority works."
    )
    records.append(rec)

    # 2020-21 — reduced to ₹2.00 crore due to COVID-19
    rec = _base_record("2020-21")
    rec["assembly_term"] = "2016-2021"
    rec["ruling_party"] = "AIADMK"
    rec["per_constituency_allocation_cr"] = 2.00
    rec["state_total_allocation_cr"] = NUM_CONSTITUENCIES * 2.00  # 468
    rec["tied_fund_cr"] = None  # Guidelines modified during COVID
    rec["untied_fund_cr"] = None
    rec["tied_components"] = None
    rec["sc_st_earmark_pct"] = 22.0
    rec["covid_impact"] = True
    rec["sources"] = [
        _src("dtnext_352cr_2021"),
    ]
    rec["source_notes"] = (
        "DTNext article confirms: 'The fund was enhanced to Rs 3 crore in 2019, "
        "was scaled down to Rs 2 crore in 2020 owing to the spread of COVID-19.' "
        "Tied/untied split was modified during COVID — guidelines allowed MLACDS "
        "funds to be used for COVID prevention and containment."
    )
    records.append(rec)

    # ── Term 3: 2021-2026 (DMK — M.K. Stalin) ─────────────────────────────
    # Allocation restored to ₹3.00 crore

    # 2021-22
    rec = _base_record("2021-22")
    rec["assembly_term"] = "2021-2026"
    rec["ruling_party"] = "DMK"
    rec["per_constituency_allocation_cr"] = 3.00
    rec["state_total_allocation_cr"] = NUM_CONSTITUENCIES * 3.00  # 702
    rec["tied_fund_cr"] = 1.50
    rec["untied_fund_cr"] = 1.50
    rec["tied_components"] = {
        "school_infrastructure_cr": 0.25,
        "anganwadi_noon_meal_cr": 0.25,
        "water_supply_cr": 0.15,
        "differently_abled_cr": 0.05,
        "other_priority_works_cr": 0.80,
    }
    rec["sc_st_earmark_pct"] = 22.0
    rec["sources"] = [
        _src("policy_note_2021_22"),
        _src("dtnext_352cr_2021"),
        _src("tnrd_reports_page"),
    ]
    rec["source_notes"] = (
        "Policy Note 2021-22 Section 2.2 (p.11): '₹3.00 crore per constituency "
        "per annum. Tied ₹1.50 Cr for priority works, Untied ₹1.50 Cr.' "
        "DTNext confirms initial release of ₹352.5 Cr (₹1.5 Cr × 234 + ₹1.5 Cr). "
        "Total ₹702 Cr for the year. 30% priority works, 22% SC/ST."
    )
    rec["performance"] = {
        "total_released_cr": 702.00,
        "works_initiated": 10123,
        "total_cost_cr": 694.09,
        "works_completed": 9446,
        "works_ongoing": 677,
        "utilization_pct": round(694.09 / 702.00 * 100, 1),
    }
    rec["performance_source"] = (
        "TNRD Reports Page — MLACDS Physical & Financial Performance 2021-22"
    )
    records.append(rec)

    # 2022-23
    rec = _base_record("2022-23")
    rec["assembly_term"] = "2021-2026"
    rec["ruling_party"] = "DMK"
    rec["per_constituency_allocation_cr"] = 3.00
    rec["state_total_allocation_cr"] = NUM_CONSTITUENCIES * 3.00  # 702
    rec["tied_fund_cr"] = 1.50
    rec["untied_fund_cr"] = 1.50
    rec["tied_components"] = {
        "school_infrastructure_cr": 0.25,
        "anganwadi_noon_meal_cr": 0.25,
        "water_supply_cr": 0.15,
        "differently_abled_cr": 0.05,
        "other_priority_works_cr": 0.80,
    }
    rec["sc_st_earmark_pct"] = 22.0
    rec["sources"] = [
        _src("policy_note_2024_25"),
        _src("tnrd_reports_page"),
    ]
    rec["source_notes"] = (
        "Policy Note 2024-25 (p.76): 'During the Year 2022-23, the Government "
        "released new guidelines for the scheme.' "
        "TNRD Reports: ₹702 Cr released, 9,536 works initiated."
    )
    rec["performance"] = {
        "total_released_cr": 702.00,
        "works_initiated": 9536,
        "total_cost_cr": 683.55,
        "works_completed": 6703,
        "works_ongoing": 2833,
        "utilization_pct": round(683.55 / 702.00 * 100, 1),
    }
    rec["performance_source"] = (
        "TNRD Reports Page — MLACDS Physical & Financial Performance 2022-23"
    )
    records.append(rec)

    # 2023-24
    rec = _base_record("2023-24")
    rec["assembly_term"] = "2021-2026"
    rec["ruling_party"] = "DMK"
    rec["per_constituency_allocation_cr"] = 3.00
    rec["state_total_allocation_cr"] = NUM_CONSTITUENCIES * 3.00  # 702
    rec["tied_fund_cr"] = 1.50
    rec["untied_fund_cr"] = 1.50
    rec["tied_components"] = {
        "school_infrastructure_cr": 0.25,
        "anganwadi_noon_meal_cr": 0.25,
        "water_supply_cr": 0.15,
        "differently_abled_cr": 0.05,
        "other_priority_works_cr": 0.80,
    }
    rec["sc_st_earmark_pct"] = 22.0
    rec["sources"] = [
        _src("tnrd_reports_page"),
    ]
    rec["source_notes"] = (
        "TNRD Reports: ₹702 Cr sanctioned for 2023-24. "
        "9,012 works initiated at ₹604.86 Cr cost. "
        "Only 1,384 completed as of report date (7,628 ongoing — "
        "lower completion likely due to reporting midway through the year)."
    )
    rec["performance"] = {
        "total_sanctioned_cr": 702.00,
        "works_initiated": 9012,
        "total_cost_cr": 604.86,
        "works_completed": 1384,
        "works_ongoing": 7628,
        "utilization_pct": round(604.86 / 702.00 * 100, 1),
    }
    rec["performance_source"] = (
        "TNRD Reports Page — MLACDS Physical & Financial Performance 2023-24"
    )
    records.append(rec)

    # 2024-25
    rec = _base_record("2024-25")
    rec["assembly_term"] = "2021-2026"
    rec["ruling_party"] = "DMK"
    rec["per_constituency_allocation_cr"] = 3.00
    rec["state_total_allocation_cr"] = NUM_CONSTITUENCIES * 3.00  # 702
    # From June 2024: CM allowed full ₹3 Cr at MLA discretion
    rec["tied_fund_cr"] = None  # Tied/untied distinction removed
    rec["untied_fund_cr"] = 3.00
    rec["tied_components"] = None
    rec["sc_st_earmark_pct"] = 22.0
    rec["policy_change"] = (
        "CM approved modification allowing MLAs to utilize the entire ₹3 Cr "
        "for works of their choice, removing the tied component restriction. "
        "GST deductions on MLACDS works also eliminated."
    )
    rec["sources"] = [
        _src("policy_note_2024_25"),
        _src("dtnext_3cr_discretionary_2024"),
    ]
    rec["source_notes"] = (
        "Policy Note 2024-25 (p.77): 'For the year 2024-25, the Government have "
        "accorded administrative sanction for Rs.702 crore.' "
        "DTNext (June 2024): CM accepted MLAs' demands — entire ₹3 Cr now "
        "discretionary. GST deductions (~₹54 lakh) eliminated."
    )
    rec["performance"] = {
        "total_sanctioned_cr": 702.00,
    }
    records.append(rec)

    # 2025-26
    rec = _base_record("2025-26")
    rec["assembly_term"] = "2021-2026"
    rec["ruling_party"] = "DMK"
    rec["per_constituency_allocation_cr"] = 3.00
    rec["state_total_allocation_cr"] = NUM_CONSTITUENCIES * 3.00  # 702
    rec["tied_fund_cr"] = None
    rec["untied_fund_cr"] = 3.00
    rec["tied_components"] = None
    rec["sc_st_earmark_pct"] = 22.0
    rec["sources"] = [
        _src("policy_note_2024_25"),
    ]
    rec["source_notes"] = (
        "Continuation of ₹3 Cr allocation per Policy Note 2024-25 guidelines. "
        "This is the final year of the 2021-2026 DMK term. "
        "Performance data not yet available (fiscal year in progress)."
    )
    records.append(rec)

    return records


def _base_record(fiscal_year: str) -> dict[str, Any]:
    """Create a base record with common fields."""
    start_year = int(fiscal_year.split("-")[0])
    end_suffix = fiscal_year.split("-")[1]
    end_year = start_year + 1

    return {
        "doc_id": fiscal_year,
        "fiscal_year": fiscal_year,
        "fiscal_year_label": f"April {start_year} – March {end_year}",
        "state": "Tamil Nadu",
        "state_code": "TN",
        "scheme_name": "Member of Legislative Assembly Constituency Development Scheme",
        "scheme_short": "MLACDS",
        "num_constituencies": NUM_CONSTITUENCIES,
        "ground_truth_confidence": "HIGH",
    }


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_records(records: list[dict]) -> bool:
    """Validate that state totals match 234 × per-constituency allocation."""
    all_ok = True
    print("\n── Validation ──────────────────────────────────────────────────")
    for rec in records:
        fy = rec["fiscal_year"]
        per_const = rec["per_constituency_allocation_cr"]
        state_total = rec["state_total_allocation_cr"]
        computed = NUM_CONSTITUENCIES * per_const

        match = abs(state_total - computed) < 0.01
        status = "✓" if match else "✗"
        if not match:
            all_ok = False

        # Check performance total if available
        perf = rec.get("performance", {})
        perf_total = (
            perf.get("total_released_cr")
            or perf.get("total_sanctioned_cr")
            or perf.get("total_allocation_cr")
        )
        perf_note = ""
        if perf_total is not None:
            perf_match = abs(perf_total - computed) < 3.0  # allow ₹2-3 Cr admin overhead
            perf_status = "✓" if perf_match else "⚠"
            perf_note = f" | Official total: ₹{perf_total:,.0f} Cr {perf_status}"

        print(
            f"  {status} {fy}: ₹{per_const} Cr × {NUM_CONSTITUENCIES} = "
            f"₹{computed:,.0f} Cr (stored: ₹{state_total:,.0f} Cr){perf_note}"
        )

        # Validate source citations exist
        if not rec.get("sources"):
            print(f"    ⚠ WARNING: No sources cited for {fy}")
            all_ok = False

    print(f"\n  {'All validations passed ✓' if all_ok else 'VALIDATION FAILURES DETECTED ✗'}")
    return all_ok


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

def upload_records(records: list[dict], dry_run: bool) -> None:
    """Upload to Firestore mlacds_budget collection."""
    if dry_run:
        print(f"\n[DRY RUN] Would upload {len(records)} docs to `mlacds_budget`")
        for rec in records:
            fy = rec["fiscal_year"]
            alloc = rec["per_constituency_allocation_cr"]
            party = rec.get("ruling_party", "?")
            term = rec.get("assembly_term", "?")
            perf = rec.get("performance", {})
            works = perf.get("works_initiated", "—")
            src_count = len(rec.get("sources", []))
            print(
                f"  {fy} | ₹{alloc} Cr × 234 = ₹{alloc * 234:,.0f} Cr | "
                f"{party} ({term}) | Works: {works} | Sources: {src_count}"
            )
        return

    try:
        from google.cloud import firestore
    except ImportError:
        print("ERROR: google-cloud-firestore not installed")
        sys.exit(1)

    db = firestore.Client(project=PROJECT)
    collection = db.collection("mlacds_budget")
    now = datetime.now(timezone.utc).isoformat()

    for rec in records:
        rec["_uploaded_at"] = now
        rec["_schema_version"] = "1.0"
        doc_id = rec["doc_id"]
        collection.document(doc_id).set(rec, merge=True)
        print(f"  [uploaded] mlacds_budget/{doc_id}")

    print(f"\n✓ Uploaded {len(records)} records to Firestore `mlacds_budget`")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Load MLACDS budget data into Firestore"
    )
    ap.add_argument("--upload", action="store_true",
                    help="Upload to Firestore (default: dry run)")
    ap.add_argument("--dry-run", action="store_true", default=True,
                    help="Preview only, no Firestore writes (default)")
    ap.add_argument("--validate", action="store_true",
                    help="Validate totals only, don't upload")
    args = ap.parse_args()

    dry_run = not args.upload
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Building MLACDS budget records...")
    records = _build_records()
    print(f"  {len(records)} records built (FY {records[0]['fiscal_year']} to {records[-1]['fiscal_year']})")

    # Always validate
    valid = validate_records(records)

    if args.validate:
        return

    if not valid and not dry_run:
        print("\n⚠ Validation failed — aborting upload. Use --dry-run to preview.")
        sys.exit(1)

    # Save to JSON
    out_path = OUT_DIR / "mlacds_budget.json"
    with open(out_path, "w") as f:
        json.dump(records, f, indent=2, default=str, ensure_ascii=False)
    print(f"\n✓ Saved {len(records)} records → {out_path}")

    # Upload
    upload_records(records, dry_run)


if __name__ == "__main__":
    main()
