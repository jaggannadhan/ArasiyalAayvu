"""
SRS (Sample Registration System) — ingestor
Sources:
  - SRS Statistical Report 2023 (Census India, Sep 2025 release)
    URL: censusindia.gov.in/nada/index.php/catalog/46172/download/50420/SRS_STAT_2023.pdf
    Data: CBR, CDR, IMR, TFR (2023 point estimates + trend 2018-23)
  - SRS Special Bulletin on Maternal Mortality 2018-20
    URL: censusindia.gov.in/nada/index.php/catalog/44379/download/48052/SRS_MMR_Bulletin_2018_2020.pdf
    Data: MMR (per 1 lakh live births) for bigger states

Extraction method:
  curl → /tmp/*.pdf; pdftotext -layout; Python row parsing by state-name prefix.
  Data hardcoded (one-time extraction).

Outputs: data/processed/srs_2023.json

Run:
    python scrapers/srs_ingest.py
    python scrapers/srs_ingest.py --upload   # also writes to Firestore
"""

import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent

sys.path.insert(0, str(Path(__file__).parent))
from ts_utils import load_timeseries, upsert_snapshot, save_timeseries, upload_snapshot_to_firestore, get_firestore_client

FOCUS_STATES = ["Andhra Pradesh", "Karnataka", "Kerala", "Tamil Nadu", "Telangana"]

# ---------------------------------------------------------------------------
# Point estimates — SRS Statistical Report 2023
# ---------------------------------------------------------------------------

# Crude Birth Rate (CBR) — per 1000 population
# Statement 14 (Chapter 3). Total / Rural / Urban.
CBR_2023 = {
    "India":           {"total": 18.4, "rural": 20.3, "urban": 14.9},
    "Andhra Pradesh":  {"total": 15.0, "rural": 15.6, "urban": 13.9},
    "Karnataka":       {"total": 15.2, "rural": 16.1, "urban": 13.9},
    "Kerala":          {"total": 12.3, "rural": 12.2, "urban": 12.4},
    "Tamil Nadu":      {"total": 12.0, "rural": 12.5, "urban": 11.5},
    "Telangana":       {"total": 15.8, "rural": 16.2, "urban": 15.2},
}

# Crude Death Rate (CDR) — per 1000 population
# Statement 40 (Chapter 4). Total / Rural / Urban.
CDR_2023 = {
    "India":           {"total": 6.4, "rural": 6.8, "urban": 5.7},
    "Andhra Pradesh":  {"total": 6.9, "rural": 7.6, "urban": 5.6},
    "Karnataka":       {"total": 6.8, "rural": 7.8, "urban": 5.6},
    "Kerala":          {"total": 7.2, "rural": 7.3, "urban": 7.2},
    "Tamil Nadu":      {"total": 6.9, "rural": 8.0, "urban": 6.1},
    "Telangana":       {"total": 6.3, "rural": 7.1, "urban": 5.3},
}

# Total Fertility Rate (TFR) — children per woman
# Statement (Chapter 3). Total / Rural / Urban.
TFR_2023 = {
    "India":           {"total": 1.9, "rural": 2.1, "urban": 1.5},
    "Andhra Pradesh":  {"total": 1.5, "rural": 1.6, "urban": 1.3},
    "Karnataka":       {"total": 1.5, "rural": 1.7, "urban": 1.4},
    "Kerala":          {"total": 1.5, "rural": 1.5, "urban": 1.5},
    "Tamil Nadu":      {"total": 1.3, "rural": 1.3, "urban": 1.3},
    "Telangana":       {"total": 1.5, "rural": 1.6, "urban": 1.5},
}

# Infant Mortality Rate (IMR) — per 1000 live births
# Statement 46 (Chapter 4). Total / Rural / Urban, by sex.
IMR_2023 = {
    "India": {
        "total": {"total": 25, "male": 26, "female": 25},
        "rural": {"total": 28, "male": 29, "female": 28},
        "urban": {"total": 18, "male": 18, "female": 18},
    },
    "Andhra Pradesh": {
        "total": {"total": 19, "male": 20, "female": 18},
        "rural": {"total": 21, "male": 22, "female": 20},
        "urban": {"total": 14, "male": 16, "female": 12},
    },
    "Karnataka": {
        "total": {"total": 14, "male": 13, "female": 15},
        "rural": {"total": 16, "male": 16, "female": 16},
        "urban": {"total": 11, "male": 9,  "female": 14},
    },
    "Kerala": {
        "total": {"total": 5,  "male": 9,  "female": 2},
        "rural": {"total": 5,  "male": 7,  "female": 3},
        "urban": {"total": 5,  "male": 10, "female": 0},
    },
    "Tamil Nadu": {
        "total": {"total": 12, "male": 10, "female": 13},
        "rural": {"total": 13, "male": 13, "female": 13},
        "urban": {"total": 11, "male": 8,  "female": 13},
    },
    "Telangana": {
        "total": {"total": 18, "male": 19, "female": 17},
        "rural": {"total": 20, "male": 21, "female": 19},
        "urban": {"total": 15, "male": 15, "female": 14},
    },
}

# Maternal Mortality Ratio (MMR) — per 1 lakh live births
# Source: SRS Special Bulletin on Maternal Mortality 2018-20 (latest available)
# Note: MMR report lags the SRS annual report by ~3 years
MMR_2018_20 = {
    "India":           {"mmr": 97,  "ci_low": 88,  "ci_high": 106},
    "Andhra Pradesh":  {"mmr": 45,  "ci_low": 13,  "ci_high": 78},
    "Telangana":       {"mmr": 43,  "ci_low": 4,   "ci_high": 83},
    "Karnataka":       {"mmr": 69,  "ci_low": 35,  "ci_high": 103},
    "Kerala":          {"mmr": 19,  "ci_low": 0,   "ci_high": 42},
    "Tamil Nadu":      {"mmr": 54,  "ci_low": 24,  "ci_high": 85},
}

# ---------------------------------------------------------------------------
# Trend data 2018-2023 — Tables 12 / 13 / 14 / 15 (Detailed Tables section)
# ---------------------------------------------------------------------------

# CBR trend: Total only
CBR_TREND = {
    "India": {
        "total": [20.0, 19.7, 19.5, 19.3, 19.1, 18.4],
    },
    "Andhra Pradesh": {
        "total": [16.0, 15.9, 15.7, 15.4, 15.7, 15.0],
    },
    "Karnataka": {
        "total": [17.2, 16.9, 16.5, 16.2, 15.8, 15.2],
    },
    "Kerala": {
        "total": [13.9, 13.5, 13.2, 12.9, 12.4, 12.3],
    },
    "Tamil Nadu": {
        "total": [14.7, 14.2, 13.8, 13.4, 12.1, 12.0],
    },
    "Telangana": {
        "total": [16.9, 16.7, 16.4, 16.1, 16.5, 15.8],
    },
}

# CDR trend: Total only (2021 spike = COVID)
CDR_TREND = {
    "India":          {"total": [6.2, 6.0, 6.0, 7.5, 6.8, 6.4]},
    "Andhra Pradesh": {"total": [6.7, 6.4, 6.3, 8.0, 7.5, 6.9]},
    "Karnataka":      {"total": [6.3, 6.2, 6.2, 8.5, 7.4, 6.8]},
    "Kerala":         {"total": [6.9, 7.1, 7.0, 9.0, 7.6, 7.2]},
    "Tamil Nadu":     {"total": [6.5, 6.1, 6.1, 8.3, 7.3, 6.9]},
    "Telangana":      {"total": [6.3, 6.1, 6.0, 6.5, 6.6, 6.3]},
}

# IMR trend: Total only
IMR_TREND = {
    "India":          {"total": [32, 30, 28, 27, 26, 25]},
    "Andhra Pradesh": {"total": [29, 25, 24, 22, 20, 19]},
    "Karnataka":      {"total": [23, 21, 19, 17, 15, 14]},
    "Kerala":         {"total": [7,  6,  6,  6,  7,  5]},
    "Tamil Nadu":     {"total": [15, 15, 13, 12, 11, 12]},
    "Telangana":      {"total": [27, 23, 21, 20, 18, 18]},
}

# TFR trend: Total only
TFR_TREND = {
    "India":          {"total": [2.2, 2.1, 2.0, 2.0, 2.0, 1.9]},
    "Andhra Pradesh": {"total": [1.6, 1.5, 1.5, 1.5, 1.6, 1.5]},
    "Karnataka":      {"total": [1.7, 1.7, 1.6, 1.6, 1.6, 1.5]},
    "Kerala":         {"total": [1.7, 1.6, 1.5, 1.5, 1.5, 1.5]},
    "Tamil Nadu":     {"total": [1.6, 1.5, 1.4, 1.5, 1.3, 1.3]},
    "Telangana":      {"total": [1.6, 1.6, 1.5, 1.6, 1.6, 1.5]},
}

TREND_YEARS = [2018, 2019, 2020, 2021, 2022, 2023]


def build_by_state() -> dict:
    by_state = {}
    for state in FOCUS_STATES:
        by_state[state] = {
            "cbr_2023": CBR_2023[state],
            "cdr_2023": CDR_2023[state],
            "tfr_2023": TFR_2023[state],
            "imr_2023": IMR_2023[state],
            "mmr_2018_20": MMR_2018_20[state],
            "trend": {
                "years": TREND_YEARS,
                "cbr_total": CBR_TREND[state]["total"],
                "cdr_total": CDR_TREND[state]["total"],
                "imr_total": IMR_TREND[state]["total"],
                "tfr_total": TFR_TREND[state]["total"],
            },
        }
    return by_state


def build_output() -> dict:
    by_state = build_by_state()

    # Quick TN vs India comparison
    tn = by_state["Tamil Nadu"]
    india_cbr = CBR_2023["India"]["total"]
    india_cdr = CDR_2023["India"]["total"]
    india_imr = IMR_2023["India"]["total"]["total"]
    india_tfr = TFR_2023["India"]["total"]

    highlights = {}
    for state in FOCUS_STATES:
        s = by_state[state]
        highlights[state] = {
            "cbr_vs_india": round(s["cbr_2023"]["total"] - india_cbr, 1),
            "cdr_vs_india": round(s["cdr_2023"]["total"] - india_cdr, 1),
            "imr_vs_india": round(s["imr_2023"]["total"]["total"] - india_imr, 1),
            "tfr_vs_india": round(s["tfr_2023"]["total"] - india_tfr, 1),
            "mmr_2018_20":  s["mmr_2018_20"]["mmr"],
        }

    return {
        "meta": {
            "sources": [
                {
                    "name": "SRS Statistical Report 2023",
                    "url": "https://censusindia.gov.in/nada/index.php/catalog/46172/download/50420/SRS_STAT_2023.pdf",
                    "published": "September 2025",
                    "indicators": ["CBR", "CDR", "TFR", "IMR"],
                    "reference_year": 2023,
                },
                {
                    "name": "SRS Special Bulletin on Maternal Mortality 2018-20",
                    "url": "https://censusindia.gov.in/nada/index.php/catalog/44379/download/48052/SRS_MMR_Bulletin_2018_2020.pdf",
                    "indicators": ["MMR"],
                    "reference_period": "2018-2020",
                    "note": "MMR bulletin lags SRS annual report by ~3 years; 2021-23 not yet released",
                },
            ],
            "indicator_notes": {
                "CBR": "Crude Birth Rate — live births per 1000 population",
                "CDR": "Crude Death Rate — deaths per 1000 population",
                "TFR": "Total Fertility Rate — children per woman (replacement level = 2.1)",
                "IMR": "Infant Mortality Rate — deaths under age 1 per 1000 live births",
                "MMR": "Maternal Mortality Ratio — maternal deaths per 1 lakh live births",
            },
            "focus_states": FOCUS_STATES,
            "trend_years": TREND_YEARS,
            "notes": [
                "2021 CDR spike in all states reflects excess COVID mortality",
                "Kerala CDR (7.2) > TN (6.9) despite better healthcare — Kerala has older age structure",
                "TFR below 2.1 (replacement) in all 5 focus states; TN lowest at 1.3",
                "Kerala IMR = 5 — lowest in country, best in focus states",
            ],
        },
        "all_india": {
            "cbr_2023": CBR_2023["India"],
            "cdr_2023": CDR_2023["India"],
            "tfr_2023": TFR_2023["India"],
            "imr_2023": IMR_2023["India"],
            "mmr_2018_20": MMR_2018_20["India"],
            "trend": {
                "years": TREND_YEARS,
                "cbr_total": CBR_TREND["India"]["total"],
                "cdr_total": CDR_TREND["India"]["total"],
                "imr_total": IMR_TREND["India"]["total"],
                "tfr_total": TFR_TREND["India"]["total"],
            },
        },
        "by_state": by_state,
        "gaps_vs_all_india_2023": highlights,
    }


def upload_to_firestore(ts: dict) -> None:
    """Upload all SRS snapshots to Firestore sub-collection: srs/{entity_id}/snapshots/{data_period}."""
    db = get_firestore_client()

    count = 0
    for display_name, entity in ts["entities"].items():
        for data_period, snapshot in entity["snapshots"].items():
            upload_snapshot_to_firestore(db, "srs", display_name, data_period, snapshot)
            count += 1
    print(f"  Uploaded {count} SRS snapshots to Firestore.")


def main():
    upload = "--upload" in sys.argv
    out_path = BASE_DIR / "data" / "processed" / "srs_ts.json"

    ts = load_timeseries(out_path)
    meta = {
        "dataset": "srs",
        "sources": [
            {
                "name": "SRS Statistical Report 2023",
                "url": "https://censusindia.gov.in/nada/index.php/catalog/46172/download/50420/SRS_STAT_2023.pdf",
                "indicators": ["CBR", "CDR", "TFR", "IMR"],
            },
            {
                "name": "SRS Special Bulletin on Maternal Mortality 2018-20",
                "url": "https://censusindia.gov.in/nada/index.php/catalog/44379/download/48052/SRS_MMR_Bulletin_2018_2020.pdf",
                "indicators": ["MMR"],
                "note": "Lags SRS annual by ~3 years; 2021-23 not yet released",
            },
        ],
        "notes": [
            "2021 CDR spike in all states reflects excess COVID mortality",
            "Kerala CDR (7.2) > TN (6.9) despite better healthcare — Kerala has older age structure",
            "TFR below 2.1 (replacement) in all 5 focus states; TN lowest at 1.3",
        ],
    }

    by_state = build_by_state()

    for i, state in enumerate(FOCUS_STATES):
        s = by_state[state]
        snapshot = {
            "cbr": s["cbr_2023"],
            "cdr": s["cdr_2023"],
            "tfr": s["tfr_2023"],
            "imr": s["imr_2023"],
            "mmr_2018_20": s["mmr_2018_20"],
            "trend": s["trend"],
        }
        upsert_snapshot(ts, state, "2023", snapshot, meta=meta if i == 0 else None)

    upsert_snapshot(ts, "India", "2023", {
        "cbr": CBR_2023["India"],
        "cdr": CDR_2023["India"],
        "tfr": TFR_2023["India"],
        "imr": IMR_2023["India"],
        "mmr_2018_20": MMR_2018_20["India"],
        "trend": {
            "years": TREND_YEARS,
            "cbr_total": CBR_TREND["India"]["total"],
            "cdr_total": CDR_TREND["India"]["total"],
            "imr_total": IMR_TREND["India"]["total"],
            "tfr_total": TFR_TREND["India"]["total"],
        },
    })

    save_timeseries(ts, out_path)
    print(f"Wrote {out_path}  ({out_path.stat().st_size // 1024} KB)")

    # Summary table
    print("\n── Demographic indicators 2023, focus states vs All-India ────────────")
    print(f"{'State':<20} {'CBR':>6} {'CDR':>6} {'TFR':>6} {'IMR':>6} {'MMR(18-20)':>12}")
    print("─" * 58)
    for state in FOCUS_STATES:
        s = by_state[state]
        print(f"{state:<20}"
              f"  {s['cbr_2023']['total']:>4.1f}"
              f"  {s['cdr_2023']['total']:>4.1f}"
              f"  {s['tfr_2023']['total']:>4.1f}"
              f"  {s['imr_2023']['total']['total']:>4}"
              f"  {s['mmr_2018_20']['mmr']:>10}")
    print(f"{'All India':<20}"
          f"  {CBR_2023['India']['total']:>4.1f}"
          f"  {CDR_2023['India']['total']:>4.1f}"
          f"  {TFR_2023['India']['total']:>4.1f}"
          f"  {IMR_2023['India']['total']['total']:>4}"
          f"  {MMR_2018_20['India']['mmr']:>10}")

    print("\n── TFR trend (total), focus states ────────────────────────────────────")
    print(f"{'State':<20}", "  ".join(str(y) for y in TREND_YEARS))
    print("─" * 65)
    for state in FOCUS_STATES:
        vals = "  ".join(f"{v:.1f}" for v in by_state[state]["trend"]["tfr_total"])
        print(f"{state:<20} {vals}")

    if upload:
        print("\nUploading to Firestore...")
        upload_to_firestore(ts)

    return ts


if __name__ == "__main__":
    main()
