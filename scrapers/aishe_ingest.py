"""
AISHE (All India Survey on Higher Education) 2021-22 — ingestor
Source: AISHE Final Report 2021-22, Ministry of Education / Department of Higher Education
PDF: cdnbbsr.s3waas.gov.in/s392049debbe566ca5782a3045cf300a3c/uploads/2024/02/20240214825688998.pdf
Also: aishe.gov.in/aishe-final-report/

Note: 2022-23 final report not yet released as of Apr 2026. 2021-22 is the latest final report.

Indicators extracted:
  - GER (Gross Enrolment Ratio) 18-23 years: 2021-22 point estimates (male/female/total)
  - GER 5-year trend 2017-18 to 2021-22 (all categories)
  - Gender Parity Index (GPI) 2021-22
  - Number of Universities by state (Table 1)
  - Number of Colleges by state (Table 5): govt / private aided / private unaided / total
  - Enrollment by level (Table 6): PhD, MPhil, PG, UG, PG Diploma, Diploma

Outputs: data/processed/aishe_2021_22.json

Run:
    python scrapers/aishe_ingest.py
    python scrapers/aishe_ingest.py --upload   # also writes to Firestore
"""

import json
import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent

sys.path.insert(0, str(Path(__file__).parent))
from ts_utils import load_timeseries, upsert_snapshot, save_timeseries, upload_snapshot_to_firestore

FOCUS_STATES = ["Andhra Pradesh", "Karnataka", "Kerala", "Tamil Nadu", "Telangana"]

# ---------------------------------------------------------------------------
# GER — Table 19: Gross Enrolment Ratio (18-23 years), 2021-22
# Based on 2011 population projections. All categories.
# ---------------------------------------------------------------------------
GER_2021_22 = {
    "Andhra Pradesh": {"male": 37.7, "female": 35.2, "total": 36.5, "gpi": 0.93},
    "Karnataka":      {"male": 36.1, "female": 36.3, "total": 36.2, "gpi": 1.01},
    "Kerala":         {"male": 34.1, "female": 49.0, "total": 41.3, "gpi": 1.44},
    "Tamil Nadu":     {"male": 46.8, "female": 47.3, "total": 47.0, "gpi": 1.01},
    "Telangana":      {"male": 38.5, "female": 41.6, "total": 40.0, "gpi": 1.08},
    "All India":      {"male": 28.3, "female": 28.5, "total": 28.4, "gpi": 1.01},
}

# ---------------------------------------------------------------------------
# GER Trend — Table 47: GER during last 5 years (all categories, total persons)
# ---------------------------------------------------------------------------
GER_TREND_YEARS = ["2017-18", "2018-19", "2019-20", "2020-21", "2021-22"]

GER_TREND = {
    # Values: [male, female, total] for each year in GER_TREND_YEARS order
    "Andhra Pradesh": {
        "2017-18": {"male": 33.3, "female": 28.1, "total": 30.8},
        "2018-19": {"male": 34.3, "female": 30.1, "total": 32.3},
        "2019-20": {"male": 36.6, "female": 33.5, "total": 35.1},
        "2020-21": {"male": 38.3, "female": 36.0, "total": 37.2},
        "2021-22": {"male": 37.7, "female": 35.2, "total": 36.5},
    },
    "Karnataka": {
        "2017-18": {"male": 26.9, "female": 29.0, "total": 27.9},
        "2018-19": {"male": 27.8, "female": 29.9, "total": 28.8},
        "2019-20": {"male": 30.7, "female": 33.3, "total": 32.0},
        "2020-21": {"male": 34.8, "female": 37.2, "total": 36.0},
        "2021-22": {"male": 36.1, "female": 36.3, "total": 36.2},
    },
    "Kerala": {
        "2017-18": {"male": 29.6, "female": 38.7, "total": 34.0},
        "2018-19": {"male": 28.2, "female": 41.1, "total": 34.5},
        "2019-20": {"male": 29.8, "female": 42.3, "total": 35.9},
        "2020-21": {"male": 34.5, "female": 52.3, "total": 43.2},
        "2021-22": {"male": 34.1, "female": 49.0, "total": 41.3},
    },
    "Tamil Nadu": {
        "2017-18": {"male": 46.2, "female": 47.7, "total": 46.9},
        "2018-19": {"male": 46.6, "female": 47.5, "total": 47.0},
        "2019-20": {"male": 48.1, "female": 49.9, "total": 49.0},
        "2020-21": {"male": 45.4, "female": 48.6, "total": 46.9},
        "2021-22": {"male": 46.8, "female": 47.3, "total": 47.0},
    },
    "Telangana": {
        "2017-18": {"male": 34.7, "female": 34.1, "total": 34.4},
        "2018-19": {"male": 33.4, "female": 36.5, "total": 34.9},
        "2019-20": {"male": 32.2, "female": 36.4, "total": 34.3},
        "2020-21": {"male": 37.4, "female": 40.9, "total": 39.1},
        "2021-22": {"male": 38.5, "female": 41.6, "total": 40.0},
    },
    "All India": {
        "2017-18": {"male": 24.5, "female": 24.6, "total": 24.6},
        "2018-19": {"male": 24.4, "female": 25.5, "total": 24.9},
        "2019-20": {"male": 24.8, "female": 26.4, "total": 25.6},
        "2020-21": {"male": 26.7, "female": 27.9, "total": 27.3},
        "2021-22": {"male": 28.3, "female": 28.5, "total": 28.4},
    },
}

# ---------------------------------------------------------------------------
# Number of Universities — Table 1, 2021-22
# Types: Central / Central Open / Institute of National Importance /
#        State Public / State Open / Institute under State / State Private / Others
# ---------------------------------------------------------------------------
UNIVERSITIES = {
    "Andhra Pradesh": {"total": 47, "central": 3, "state_public": 24, "state_private": 5, "other": 4},
    "Karnataka":      {"total": 75, "central": 1, "state_public": 33, "state_private": 20, "other": 12},
    "Kerala":         {"total": 25, "central": 1, "state_public": 14, "state_private": 0,  "other": 2},
    "Tamil Nadu":     {"total": 62, "central": 2, "state_public": 21, "state_private": 3,  "other": 26},
    "Telangana":      {"total": 31, "central": 3, "state_public": 15, "state_private": 4,  "other": 2},
    "All India":      {"total": 1168},
}

# ---------------------------------------------------------------------------
# Number of Colleges — Table 5, 2021-22 (actual response, not estimated)
# ---------------------------------------------------------------------------
COLLEGES = {
    "Andhra Pradesh": {
        "private_unaided": 2130, "private_aided": 136, "private_total": 2266,
        "government": 316, "total": 2582,
    },
    "Karnataka": {
        "private_unaided": 3087, "private_aided": 492, "private_total": 3579,
        "government": 704, "total": 4283,
    },
    "Kerala": {
        "private_unaided": 830,  "private_aided": 222, "private_total": 1052,
        "government": 280, "total": 1332,
    },
    "Tamil Nadu": {
        "private_unaided": 2128, "private_aided": 265, "private_total": 2393,
        "government": 414, "total": 2807,
    },
    "Telangana": {
        "private_unaided": 1548, "private_aided": 109, "private_total": 1657,
        "government": 283, "total": 1940,
    },
    "All India": {
        "private_unaided": 27956, "private_aided": 5676, "private_total": 33632,
        "government": 9193, "total": 42825,
    },
}

# ---------------------------------------------------------------------------
# Enrollment by Level — Table 6, 2021-22 (total persons, all modes)
# ---------------------------------------------------------------------------
ENROLLMENT = {
    "Andhra Pradesh": {
        "phd": 5583,     "mphil": 23,    "pg": 184942,  "ug": 1569050,
        "pg_diploma": 1571, "diploma": 156963,
        "total_approx": 5583 + 23 + 184942 + 1569050 + 1571 + 156963,  # 1,918,132
    },
    "Karnataka": {
        "phd": 11193,    "mphil": 227,   "pg": 260616,  "ug": 1906300,
        "pg_diploma": 8526, "diploma": 229913,
        "total_approx": 11193 + 227 + 260616 + 1906300 + 8526 + 229913,  # 2,416,775
    },
    "Kerala": {
        "phd": 9134,     "mphil": 356,   "pg": 172576,  "ug": 1016386,
        "pg_diploma": 3120, "diploma": 89137,
        "total_approx": 9134 + 356 + 172576 + 1016386 + 3120 + 89137,   # 1,290,709
    },
    "Tamil Nadu": {
        "phd": 28867,    "mphil": 4041,  "pg": 464086,  "ug": 2491822,
        "pg_diploma": 14729, "diploma": 262714,
        "total_approx": 28867 + 4041 + 464086 + 2491822 + 14729 + 262714,  # 3,266,259
    },
    "Telangana": {
        "phd": 6967,     "mphil": 100,   "pg": 207082,  "ug": 1272323,
        "pg_diploma": 7915, "diploma": 91552,
        "total_approx": 6967 + 100 + 207082 + 1272323 + 7915 + 91552,   # 1,585,939
    },
    "All India": {
        "phd": 212568,   "mphil": 9520,  "pg": 5217753, "ug": 34139233,
        "pg_diploma": 234783, "diploma": 2916445,
        "total_approx": 212568 + 9520 + 5217753 + 34139233 + 234783 + 2916445,  # 42,730,302
    },
}


def build_by_state() -> dict:
    by_state = {}
    for state in FOCUS_STATES:
        ai_ger = GER_2021_22["All India"]
        st_ger = GER_2021_22[state]
        by_state[state] = {
            "ger_2021_22": st_ger,
            "ger_vs_all_india": round(st_ger["total"] - ai_ger["total"], 1),
            "ger_trend_2017_22": GER_TREND[state],
            "ger_trend_years": GER_TREND_YEARS,
            "universities_2021_22": UNIVERSITIES[state],
            "colleges_2021_22": COLLEGES[state],
            "enrollment_2021_22": ENROLLMENT[state],
        }
    return by_state


def build_output() -> dict:
    by_state = build_by_state()
    return {
        "meta": {
            "source": "AISHE Final Report 2021-22, Ministry of Education, Dept. of Higher Education",
            "url": "https://cdnbbsr.s3waas.gov.in/s392049debbe566ca5782a3045cf300a3c/uploads/2024/02/20240214825688998.pdf",
            "portal": "https://aishe.gov.in/aishe-final-report/",
            "survey_year": "2021-22",
            "note": (
                "2022-23 final report not yet released as of Apr 2026. "
                "GER based on 2011 population projections. "
                "Enrollment totals are approximate (sum of PhD+MPhil+PG+UG+PG-Diploma+Diploma; "
                "excludes Certificate courses). "
                "GPI > 1 means more females enrolled than males."
            ),
            "focus_states": FOCUS_STATES,
            "ger_trend_years": GER_TREND_YEARS,
        },
        "all_india": {
            "ger_2021_22": GER_2021_22["All India"],
            "ger_trend": GER_TREND["All India"],
            "colleges": COLLEGES["All India"],
            "universities": UNIVERSITIES["All India"],
            "enrollment": ENROLLMENT["All India"],
        },
        "by_state": by_state,
    }


def upload_to_firestore(ts: dict) -> None:
    """Upload all AISHE snapshots to Firestore sub-collection: aishe/{entity_id}/snapshots/{data_period}."""
    import firebase_admin
    from firebase_admin import credentials, firestore as fs

    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not firebase_admin._apps:
        cred = credentials.Certificate(cred_path) if cred_path else credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred)
    db = fs.client()

    count = 0
    for display_name, entity in ts["entities"].items():
        for data_period, snapshot in entity["snapshots"].items():
            upload_snapshot_to_firestore(db, "aishe", display_name, data_period, snapshot)
            count += 1
    print(f"  Uploaded {count} AISHE snapshots to Firestore.")


def main():
    upload = "--upload" in sys.argv
    out_path = BASE_DIR / "data" / "processed" / "aishe_ts.json"

    ts = load_timeseries(out_path)
    meta = {
        "dataset": "aishe",
        "source": "AISHE Final Report 2021-22, Ministry of Education, Dept. of Higher Education",
        "url": "https://cdnbbsr.s3waas.gov.in/s392049debbe566ca5782a3045cf300a3c/uploads/2024/02/20240214825688998.pdf",
        "note": (
            "2022-23 final report not yet released as of Apr 2026. "
            "GER based on 2011 population projections. "
            "GPI > 1 means more females enrolled than males."
        ),
    }

    ai_ger = GER_2021_22["All India"]

    for i, state in enumerate(FOCUS_STATES):
        st_ger = GER_2021_22[state]
        snapshot = {
            "ger": st_ger,
            "ger_vs_all_india": round(st_ger["total"] - ai_ger["total"], 1),
            "ger_trend": GER_TREND[state],
            "ger_trend_years": GER_TREND_YEARS,
            "universities": UNIVERSITIES[state],
            "colleges": COLLEGES[state],
            "enrollment": ENROLLMENT[state],
        }
        upsert_snapshot(ts, state, "2021-22", snapshot, meta=meta if i == 0 else None)

    upsert_snapshot(ts, "All India", "2021-22", {
        "ger": ai_ger,
        "ger_trend": GER_TREND["All India"],
        "ger_trend_years": GER_TREND_YEARS,
        "universities": UNIVERSITIES["All India"],
        "colleges": COLLEGES["All India"],
        "enrollment": ENROLLMENT["All India"],
    })

    save_timeseries(ts, out_path)
    print(f"Wrote {out_path}  ({out_path.stat().st_size // 1024} KB)")

    print("\n── GER 2021-22 (18-23 yrs, All categories) ──────────────────────────")
    print(f"{'State':<20}  {'Male':>6}  {'Female':>8}  {'Total':>7}  {'GPI':>6}  {'vs India':>9}")
    print("─" * 62)
    for state in FOCUS_STATES:
        s = GER_2021_22[state]
        gap = round(s["total"] - ai_ger["total"], 1)
        sign = "+" if gap >= 0 else ""
        print(f"{state:<20}  {s['male']:>6.1f}  {s['female']:>8.1f}  {s['total']:>7.1f}  {s['gpi']:>6.2f}  {sign+str(gap):>9}")
    print(f"{'All India':<20}  {ai_ger['male']:>6.1f}  {ai_ger['female']:>8.1f}  {ai_ger['total']:>7.1f}  {ai_ger['gpi']:>6.2f}  {'—':>9}")

    print("\n── Institutions 2021-22 ─────────────────────────────────────────────")
    print(f"{'State':<20}  {'Univs':>7}  {'Colleges':>10}  {'Govt%':>7}  {'Enrolment (approx)':>20}")
    print("─" * 72)
    for state in FOCUS_STATES:
        u = UNIVERSITIES[state]["total"]
        c = COLLEGES[state]["total"]
        g = COLLEGES[state]["government"]
        govt_pct = round(g / c * 100, 1)
        enr = ENROLLMENT[state]["total_approx"]
        print(f"{state:<20}  {u:>7}  {c:>10,}  {govt_pct:>6.1f}%  {enr:>20,}")

    print("\n── GER trend (total persons) ────────────────────────────────────────")
    print(f"{'State':<20}", "  ".join(y[-5:] for y in GER_TREND_YEARS))
    print("─" * 60)
    for state in FOCUS_STATES:
        vals = "  ".join(f"{GER_TREND[state][y]['total']:>5.1f}" for y in GER_TREND_YEARS)
        print(f"{state:<20} {vals}")

    if upload:
        print("\nUploading to Firestore...")
        upload_to_firestore(ts)

    return ts


if __name__ == "__main__":
    main()
