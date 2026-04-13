"""
NITI Aayog SDG India Index — ingestor
Source: NITI Aayog SDG India Index reports (2018, 2020-21, 2023-24)
Raw CSVs: data/raw/niti_sdg/sdg_<year>.csv

Outputs: data/processed/sdg_india_index.json

Run:
    python scrapers/niti_sdg_ingest.py
    python scrapers/niti_sdg_ingest.py --upload   # also writes to Firestore
"""

import csv
import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent

sys.path.insert(0, str(Path(__file__).parent))
from ts_utils import load_timeseries, upsert_snapshot, save_timeseries, upload_snapshot_to_firestore, get_firestore_client

SDG_GOALS = {
    "1":  "No Poverty",
    "2":  "Zero Hunger",
    "3":  "Good Health & Well-being",
    "4":  "Quality Education",
    "5":  "Gender Equality",
    "6":  "Clean Water & Sanitation",
    "7":  "Affordable & Clean Energy",
    "8":  "Decent Work & Economic Growth",
    "9":  "Industry, Innovation & Infrastructure",
    "10": "Reduced Inequalities",
    "11": "Sustainable Cities & Communities",
    "12": "Responsible Consumption & Production",
    "13": "Climate Action",
    "14": "Life Below Water",
    "15": "Life on Land",
    "16": "Peace, Justice & Strong Institutions",
}

# States of primary interest for TN knowledge graph
FOCUS_STATES = {
    "Tamil Nadu",
    "Kerala",
    "Karnataka",
    "Andhra Pradesh",
    "Telangana",
}

# Year → CSV file mapping
YEAR_FILES = {
    "2018":    "sdg_2018.csv",
    "2020-21": "sdg_2020_21.csv",
    "2023-24": "sdg_2023_24.csv",
}


def parse_csv(path: Path, year: str) -> list[dict]:
    """Return list of state records from one year's CSV."""
    records = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            state = row["States"].strip()
            goals: dict[str, int | None] = {}
            for g in SDG_GOALS:
                raw = row.get(f"Goal {g}", "").strip()
                goals[g] = int(raw) if raw else None
            composite_raw = row.get("Composite", "").strip()
            composite = int(composite_raw) if composite_raw else None
            rank_raw = row.get("SNo", "").strip()
            rank = int(rank_raw) if rank_raw else None
            records.append({
                "state": state,
                "year": year,
                "rank": rank,
                "composite": composite,
                "goals": goals,
                "is_focus_state": state in FOCUS_STATES,
            })
    return records


def build_output(all_records: list[dict]) -> dict:
    """Build structured output with by-state lookup + flat records."""
    # by_state[state][year] → {composite, rank, goals}
    by_state: dict[str, dict] = {}
    for rec in all_records:
        state = rec["state"]
        year = rec["year"]
        by_state.setdefault(state, {})
        by_state[state][year] = {
            "rank": rec["rank"],
            "composite": rec["composite"],
            "goals": rec["goals"],
        }

    # Focus state summary — TN vs neighbours, latest year
    latest_year = "2023-24"
    focus_summary = {}
    for state in sorted(FOCUS_STATES):
        if state in by_state and latest_year in by_state[state]:
            data = by_state[state][latest_year]
            focus_summary[state] = {
                "composite": data["composite"],
                "rank": data["rank"],
                "goals": data["goals"],
            }

    return {
        "meta": {
            "source": "NITI Aayog SDG India Index",
            "url": "https://sdgindiaindex.niti.gov.in",
            "years": list(YEAR_FILES.keys()),
            "goals": SDG_GOALS,
            "focus_states": sorted(FOCUS_STATES),
            "note": "Score 0-100; blank = data not reported for that state/goal",
        },
        "focus_summary": focus_summary,
        "by_state": by_state,
        "records": all_records,
    }


def compute_gaps(by_state: dict) -> dict:
    """
    For each focus state × goal × year: compute gap vs national best.
    Returns {state: {year: {goal: gap_from_best}}}
    """
    gaps: dict[str, dict] = {}
    years = list(YEAR_FILES.keys())

    for year in years:
        # Find best score per goal across ALL states this year
        best: dict[str, int] = {}
        for state_data in by_state.values():
            if year not in state_data:
                continue
            for g, score in state_data[year]["goals"].items():
                if score is not None:
                    best[g] = max(best.get(g, 0), score)

        for state in FOCUS_STATES:
            if state not in by_state or year not in by_state[state]:
                continue
            gaps.setdefault(state, {})
            gaps[state][year] = {}
            for g, score in by_state[state][year]["goals"].items():
                if score is not None and g in best:
                    gaps[state][year][g] = score - best[g]   # negative = behind best
                else:
                    gaps[state][year][g] = None

    return gaps


def upload_to_firestore(ts: dict) -> None:
    """Upload all SDG snapshots to Firestore sub-collection: sdg_index/{entity_id}/snapshots/{data_period}."""
    db = get_firestore_client()

    count = 0
    for display_name, entity in ts["entities"].items():
        for data_period, snapshot in entity["snapshots"].items():
            upload_snapshot_to_firestore(db, "sdg_index", display_name, data_period, snapshot)
            count += 1
    print(f"  Uploaded {count} SDG snapshots to Firestore.")


def main():
    upload = "--upload" in sys.argv
    raw_dir = BASE_DIR / "data" / "raw" / "niti_sdg"
    out_path = BASE_DIR / "data" / "processed" / "sdg_ts.json"

    all_records = []
    for year, filename in YEAR_FILES.items():
        path = raw_dir / filename
        if not path.exists():
            print(f"  MISSING: {path} — skipping {year}")
            continue
        recs = parse_csv(path, year)
        print(f"  Parsed {len(recs):2d} states for {year}")
        all_records.extend(recs)

    output = build_output(all_records)
    gaps = compute_gaps(output["by_state"])

    ts = load_timeseries(out_path)
    meta = {
        "dataset": "sdg_index",
        "source": "NITI Aayog SDG India Index",
        "url": "https://sdgindiaindex.niti.gov.in",
        "goals": SDG_GOALS,
        "note": "Score 0-100; blank = data not reported for that state/goal",
    }

    first = True
    for state, years_data in output["by_state"].items():
        for year, year_data in years_data.items():
            snapshot = {
                "composite": year_data["composite"],
                "rank": year_data["rank"],
                "goals": year_data["goals"],
                "gaps_from_national_best": gaps.get(state, {}).get(year, {}),
            }
            upsert_snapshot(ts, state, year, snapshot, meta=meta if first else None)
            first = False

    # Compute All India average per year (mean of all state composites)
    for year in YEAR_FILES:
        composites = [
            d[year]["composite"]
            for d in output["by_state"].values()
            if year in d and d[year].get("composite") is not None
        ]
        if composites:
            avg_composite = round(sum(composites) / len(composites), 1)
            goal_avgs: dict[str, int | None] = {}
            for g in SDG_GOALS:
                scores = [
                    d[year]["goals"].get(g)
                    for d in output["by_state"].values()
                    if year in d and d[year]["goals"].get(g) is not None
                ]
                goal_avgs[g] = round(sum(scores) / len(scores)) if scores else None
            upsert_snapshot(ts, "All India", year, {
                "composite": avg_composite,
                "rank": None,
                "goals": goal_avgs,
                "note": f"National average across {len(composites)} states/UTs",
            })
            print(f"  All India {year}: composite={avg_composite} (avg of {len(composites)} states)")

    save_timeseries(ts, out_path)
    print(f"\nWrote {out_path}  ({out_path.stat().st_size // 1024} KB)")

    # Print focus-state composite trend
    print("\n── Focus state composite scores ──────────────────────")
    print(f"{'State':<30} {'2018':>6} {'2020-21':>8} {'2023-24':>8}")
    print("─" * 56)
    for state in sorted(FOCUS_STATES):
        row = output["by_state"].get(state, {})
        vals = [str(row.get(y, {}).get("composite", "–")).rjust(6) for y in ["2018", "2020-21", "2023-24"]]
        print(f"{state:<30} {vals[0]} {vals[1]} {vals[2]}")

    # TN weakest goals in 2023-24
    tn_2024 = output["by_state"].get("Tamil Nadu", {}).get("2023-24", {})
    if tn_2024:
        goals_ranked = sorted(
            [(g, s) for g, s in tn_2024["goals"].items() if s is not None],
            key=lambda x: x[1]
        )
        print("\n── TN weakest goals (2023-24) ────────────────────────")
        for g, score in goals_ranked[:5]:
            print(f"  Goal {g:>2} — {SDG_GOALS[g]:<40} {score}")

    if upload:
        print("\nUploading to Firestore...")
        upload_to_firestore(ts)

    return ts


if __name__ == "__main__":
    main()
