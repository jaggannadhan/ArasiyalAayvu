"""
HCES (Household Consumption Expenditure Survey) 2023-24 — ingestor
Source: MoSPI Factsheet on HCES 2023-24
PDF: mospi.gov.in/sites/default/files/publication_reports/HCES%20FactSheet%202023-24.pdf
Also: microdata.gov.in/NADA/index.php/catalog/237

Indicators extracted:
  - MPCE (Monthly Per Capita Consumption Expenditure) by state: rural / urban
    - Without imputed free items (Statement 7) — basic estimate
    - With imputed free items from social welfare (Statement 15) — includes PDS, govt schemes
  - All-India Gini coefficient of consumption: 2011-12, 2022-23, 2023-24
  - All-India food vs non-food share of MPCE

Outputs: data/processed/hces_2023_24.json

Run:
    python scrapers/hces_ingest.py
    python scrapers/hces_ingest.py --upload   # also writes to Firestore
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
# Statement 7: Average MPCE (Rs.) per month, 2023-24
# WITHOUT imputed value of free items received through government welfare schemes.
# Reflects out-of-pocket consumption spending.
# ---------------------------------------------------------------------------
MPCE_WITHOUT_FREE = {
    "Andhra Pradesh":   {"rural": 5327,  "urban": 7182},
    "Arunachal Pradesh":{"rural": 5995,  "urban": 9832},
    "Assam":            {"rural": 3793,  "urban": 6794},
    "Bihar":            {"rural": 3670,  "urban": 5080},
    "Chhattisgarh":     {"rural": 2739,  "urban": 4927},
    "Delhi":            {"rural": 7400,  "urban": 8534},
    "Goa":              {"rural": 8048,  "urban": 9726},
    "Gujarat":          {"rural": 4116,  "urban": 7175},
    "Haryana":          {"rural": 5377,  "urban": 8428},
    "Himachal Pradesh": {"rural": 5825,  "urban": 9223},
    "Jharkhand":        {"rural": 2946,  "urban": 5393},
    "Karnataka":        {"rural": 4903,  "urban": 8076},
    "Kerala":           {"rural": 6611,  "urban": 7783},
    "Madhya Pradesh":   {"rural": 3441,  "urban": 5538},
    "Maharashtra":      {"rural": 4145,  "urban": 7363},
    "Manipur":          {"rural": 4531,  "urban": 5945},
    "Meghalaya":        {"rural": 3852,  "urban": 7839},
    "Mizoram":          {"rural": 5963,  "urban": 8709},
    "Nagaland":         {"rural": 5155,  "urban": 8022},
    "Odisha":           {"rural": 3357,  "urban": 5825},
    "Punjab":           {"rural": 5817,  "urban": 7359},
    "Rajasthan":        {"rural": 4510,  "urban": 6574},
    "Sikkim":           {"rural": 9377,  "urban": 13927},
    "Tamil Nadu":       {"rural": 5701,  "urban": 8165},
    "Telangana":        {"rural": 5435,  "urban": 8978},
    "Tripura":          {"rural": 6259,  "urban": 8034},
    "Uttar Pradesh":    {"rural": 3481,  "urban": 5395},
    "Uttarakhand":      {"rural": 5003,  "urban": 7486},
    "West Bengal":      {"rural": 3620,  "urban": 5775},
    "All-India":        {"rural": 4122,  "urban": 6996},
}

# ---------------------------------------------------------------------------
# Statement 15: Average MPCE (Rs.) per month, 2023-24
# WITH imputed value of free items received through government welfare schemes
# (PDS rice/wheat, free medicines, etc.). Higher than Statement 7; reflects true
# consumption level including in-kind transfers.
# ---------------------------------------------------------------------------
MPCE_WITH_FREE = {
    "Andhra Pradesh":   {"rural": 5539,  "urban": 7341},
    "Arunachal Pradesh":{"rural": 6107,  "urban": 9877},
    "Assam":            {"rural": 3961,  "urban": 6913},
    "Bihar":            {"rural": 3788,  "urban": 5165},
    "Chhattisgarh":     {"rural": 2927,  "urban": 5114},
    "Delhi":            {"rural": 7415,  "urban": 8548},
    "Goa":              {"rural": 8178,  "urban": 9782},
    "Gujarat":          {"rural": 4190,  "urban": 7198},
    "Haryana":          {"rural": 5449,  "urban": 8462},
    "Himachal Pradesh": {"rural": 5833,  "urban": 9230},
    "Jharkhand":        {"rural": 3056,  "urban": 5455},
    "Karnataka":        {"rural": 5068,  "urban": 8169},
    "Kerala":           {"rural": 6673,  "urban": 7834},
    "Madhya Pradesh":   {"rural": 3522,  "urban": 5589},
    "Maharashtra":      {"rural": 4249,  "urban": 7415},
    "Manipur":          {"rural": 4592,  "urban": 6005},
    "Meghalaya":        {"rural": 3900,  "urban": 7857},
    "Mizoram":          {"rural": 5963,  "urban": 8709},
    "Nagaland":         {"rural": 5282,  "urban": 8136},
    "Odisha":           {"rural": 3509,  "urban": 5925},
    "Punjab":           {"rural": 5874,  "urban": 7383},
    "Rajasthan":        {"rural": 4626,  "urban": 6640},
    "Sikkim":           {"rural": 9474,  "urban": 13965},
    "Tamil Nadu":       {"rural": 5872,  "urban": 8325},
    "Telangana":        {"rural": 5675,  "urban": 9131},
    "Tripura":          {"rural": 6368,  "urban": 8118},
    "Uttar Pradesh":    {"rural": 3578,  "urban": 5474},
    "Uttarakhand":      {"rural": 5123,  "urban": 7547},
    "West Bengal":      {"rural": 3815,  "urban": 5903},
    "All-India":        {"rural": 4247,  "urban": 7078},
}

# ---------------------------------------------------------------------------
# Gini coefficient of total consumption expenditure — All-India
# Statement 8. Consumption inequality trending DOWN.
# ---------------------------------------------------------------------------
GINI_ALL_INDIA = {
    "2011-12":  {"rural": 0.283, "urban": 0.363},
    "2022-23":  {"rural": 0.266, "urban": 0.314},
    "2023-24":  {"rural": 0.237, "urban": 0.284},
}

# ---------------------------------------------------------------------------
# Food vs non-food share of MPCE — All-India 2023-24 (with free items)
# Statement 9.
# ---------------------------------------------------------------------------
FOOD_SHARE_ALL_INDIA = {
    "rural": {
        "food_avg_mpce":     2057,
        "food_share_pct":   48.43,
        "nonfood_avg_mpce":  2190,
        "nonfood_share_pct": 51.57,
        "total_avg_mpce":    4247,
    },
    "urban": {
        "food_avg_mpce":     2853,
        "food_share_pct":   40.31,
        "nonfood_avg_mpce":  4225,
        "nonfood_share_pct": 59.69,
        "total_avg_mpce":    7078,
    },
}

# ---------------------------------------------------------------------------
# Sample size reference — Statement 16
# ---------------------------------------------------------------------------
SAMPLE_SIZE = {
    "Andhra Pradesh": {"sample_rural": 6306,  "sample_urban": 4159,  "est_rural_00": 94687,  "est_urban_00": 45156},
    "Karnataka":      {"sample_rural": 6728,  "sample_urban": 5805,  "est_rural_00": 93006,  "est_urban_00": 68807},
    "Kerala":         {"sample_rural": 3883,  "sample_urban": 3517,  "est_rural_00": 48864,  "est_urban_00": 49251},
    "Tamil Nadu":     {"sample_rural": 7484,  "sample_urban": 7008,  "est_rural_00": 111770, "est_urban_00": 106862},
    "Telangana":      {"sample_rural": 3571,  "sample_urban": 3215,  "est_rural_00": 56897,  "est_urban_00": 54075},
}


def compute_welfare_uplift(state: str) -> dict:
    """Compute MPCE uplift from government welfare in-kind transfers."""
    without = MPCE_WITHOUT_FREE[state]
    with_ = MPCE_WITH_FREE[state]
    return {
        "rural_uplift_rs": with_["rural"] - without["rural"],
        "urban_uplift_rs": with_["urban"] - without["urban"],
        "rural_uplift_pct": round((with_["rural"] - without["rural"]) / without["rural"] * 100, 1),
        "urban_uplift_pct": round((with_["urban"] - without["urban"]) / without["urban"] * 100, 1),
    }


def rank_states_by_mpce(measure: str = "with_free", area: str = "rural") -> list:
    """Rank all states by MPCE. measure: 'without_free' | 'with_free'. area: 'rural'|'urban'."""
    data = MPCE_WITH_FREE if measure == "with_free" else MPCE_WITHOUT_FREE
    states = [(s, v[area]) for s, v in data.items() if s != "All-India"]
    return sorted(states, key=lambda x: x[1], reverse=True)


def build_output() -> dict:
    by_state = {}
    for state in FOCUS_STATES:
        wof = MPCE_WITHOUT_FREE[state]
        wf  = MPCE_WITH_FREE[state]
        ai_wf = MPCE_WITH_FREE["All-India"]
        by_state[state] = {
            "mpce_without_free_items": wof,
            "mpce_with_free_items": wf,
            "welfare_uplift": compute_welfare_uplift(state),
            "gap_vs_all_india_with_free": {
                "rural": wf["rural"] - ai_wf["rural"],
                "urban": wf["urban"] - ai_wf["urban"],
            },
            "sample_size": SAMPLE_SIZE.get(state),
        }

    # Rank focus states for quick reference
    rural_ranks = {s: i+1 for i, (s, _) in enumerate(rank_states_by_mpce("with_free", "rural")) if s in FOCUS_STATES}
    urban_ranks = {s: i+1 for i, (s, _) in enumerate(rank_states_by_mpce("with_free", "urban")) if s in FOCUS_STATES}

    return {
        "meta": {
            "source": "MoSPI Factsheet on HCES 2023-24",
            "url": "https://www.mospi.gov.in/sites/default/files/publication_reports/HCES%20FactSheet%202023-24.pdf",
            "microdata_catalog": "https://microdata.gov.in/NADA/index.php/catalog/237",
            "survey_period": "August 2023 – July 2024",
            "note": (
                "MPCE = Monthly Per Capita Consumption Expenditure. "
                "'Without free items' = out-of-pocket spending only. "
                "'With free items' = includes imputed value of PDS, govt welfare in-kind transfers. "
                "Gini trending down — rural 0.237, urban 0.284 in 2023-24."
            ),
            "focus_states": FOCUS_STATES,
        },
        "all_india": {
            "mpce_without_free_items": MPCE_WITHOUT_FREE["All-India"],
            "mpce_with_free_items": MPCE_WITH_FREE["All-India"],
            "welfare_uplift": compute_welfare_uplift("All-India"),
            "gini_trend": GINI_ALL_INDIA,
            "food_share": FOOD_SHARE_ALL_INDIA,
        },
        "all_states_mpce_with_free": {
            s: MPCE_WITH_FREE[s]
            for s in MPCE_WITH_FREE if s != "All-India"
        },
        "by_state": by_state,
        "highlights": {
            state: {
                "mpce_rural_with_free": MPCE_WITH_FREE[state]["rural"],
                "mpce_urban_with_free": MPCE_WITH_FREE[state]["urban"],
                "welfare_uplift_rural_pct": compute_welfare_uplift(state)["rural_uplift_pct"],
                "welfare_uplift_urban_pct": compute_welfare_uplift(state)["urban_uplift_pct"],
            }
            for state in FOCUS_STATES
        },
    }


def upload_to_firestore(ts: dict) -> None:
    """Upload all HCES snapshots to Firestore sub-collection: hces/{entity_id}/snapshots/{data_period}."""
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
            upload_snapshot_to_firestore(db, "hces", display_name, data_period, snapshot)
            count += 1
    print(f"  Uploaded {count} HCES snapshots to Firestore.")


def main():
    upload = "--upload" in sys.argv
    out_path = BASE_DIR / "data" / "processed" / "hces_ts.json"

    ts = load_timeseries(out_path)
    meta = {
        "dataset": "hces",
        "source": "MoSPI Factsheet on HCES 2023-24",
        "url": "https://www.mospi.gov.in/sites/default/files/publication_reports/HCES%20FactSheet%202023-24.pdf",
        "survey_period": "August 2023 – July 2024",
        "note": (
            "MPCE = Monthly Per Capita Consumption Expenditure. "
            "'Without free items' = out-of-pocket spending only. "
            "'With free items' = includes imputed value of PDS, govt welfare in-kind transfers."
        ),
    }

    ai_wf = MPCE_WITH_FREE["All-India"]

    for i, state in enumerate(FOCUS_STATES):
        wof = MPCE_WITHOUT_FREE[state]
        wf  = MPCE_WITH_FREE[state]
        snapshot = {
            "mpce_without_free_items": wof,
            "mpce_with_free_items": wf,
            "welfare_uplift": compute_welfare_uplift(state),
            "gap_vs_all_india_with_free": {
                "rural": wf["rural"] - ai_wf["rural"],
                "urban": wf["urban"] - ai_wf["urban"],
            },
            "sample_size": SAMPLE_SIZE.get(state),
        }
        upsert_snapshot(ts, state, "2023-24", snapshot, meta=meta if i == 0 else None)

    upsert_snapshot(ts, "All India", "2023-24", {
        "mpce_without_free_items": MPCE_WITHOUT_FREE["All-India"],
        "mpce_with_free_items": MPCE_WITH_FREE["All-India"],
        "welfare_uplift": compute_welfare_uplift("All-India"),
        "gini_trend": GINI_ALL_INDIA,
        "food_share": FOOD_SHARE_ALL_INDIA,
    })

    save_timeseries(ts, out_path)
    print(f"Wrote {out_path}  ({out_path.stat().st_size // 1024} KB)")

    ai_uplift = compute_welfare_uplift("All-India")
    print("\n── MPCE (with free items) 2023-24 — focus states ────────────────────")
    print(f"{'State':<20}  {'Rural':>8}  {'Urban':>8}  {'R_uplift%':>10}  {'U_uplift%':>10}")
    print("─" * 65)
    for state in FOCUS_STATES:
        wf = MPCE_WITH_FREE[state]
        uplift = compute_welfare_uplift(state)
        print(f"{state:<20}  {wf['rural']:>8,}  {wf['urban']:>8,}  {uplift['rural_uplift_pct']:>9.1f}%  {uplift['urban_uplift_pct']:>9.1f}%")
    print(f"{'All India':<20}  {ai_wf['rural']:>8,}  {ai_wf['urban']:>8,}  "
          f"{ai_uplift['rural_uplift_pct']:>9.1f}%  {ai_uplift['urban_uplift_pct']:>9.1f}%")

    print("\n── All-India Gini trend ─────────────────────────────────────────────")
    for yr, g in GINI_ALL_INDIA.items():
        print(f"  {yr}: rural={g['rural']}  urban={g['urban']}")

    if upload:
        print("\nUploading to Firestore...")
        upload_to_firestore(ts)

    return ts


if __name__ == "__main__":
    main()
