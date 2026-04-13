"""
NCRB Crime in India — state-level ingestor
Source: NCRB Crime in India annual reports (NCRB / data.gov.in / OGD platform)

Raw CSVs expected at: data/raw/ncrb/ncrb_state_{year}.csv
  One file per year (e.g. ncrb_state_2022.csv, ncrb_state_2023.csv)

  How to download:
    1. Go to https://ncrb.gov.in/crime-in-india  (or data.gov.in)
    2. Open the latest Crime in India report
    3. Navigate to Chapter 1 → Table 1 (State/UT-wise Total Cognizable Crimes under IPC)
    4. Download/export as CSV
    OR: Search data.gov.in for "State/UT-wise Number of Indian Penal Code (IPC) Crimes"

  Expected column names (NCRB Table 1 format, may vary slightly by year):
    State/UT | Total IPC Crimes | Crime Rate Per Lakh | Murder | Rape |
    Kidnapping & Abduction | Dacoity | Robbery | Burglary | Theft | Riots |
    Crimes Against Women | Crimes Against Children | Crimes Against SC | Crimes Against ST

Outputs: data/processed/ncrb_ts.json

Run:
    python scrapers/ncrb_ingest.py               # parse all CSVs, write local JSON
    python scrapers/ncrb_ingest.py --upload      # also upload to Firestore
    python scrapers/ncrb_ingest.py --probe       # print CSV columns and exit
"""

from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(Path(__file__).parent))
from ts_utils import load_timeseries, upsert_snapshot, save_timeseries, upload_snapshot_to_firestore, get_firestore_client

RAW_DIR  = BASE_DIR / "data" / "raw" / "ncrb"
OUT_PATH = BASE_DIR / "data" / "processed" / "ncrb_ts.json"

FOCUS_STATES = {
    "Tamil Nadu", "Kerala", "Karnataka", "Andhra Pradesh", "Telangana",
}

# Aliases: lowercase partial match → canonical column key
# Handles year-to-year column name variations in NCRB reports
COLUMN_ALIASES: dict[str, list[str]] = {
    "total_ipc":         ["total ipc crimes", "total cognizable crimes", "total ipc", "ipc crimes"],
    "crime_rate":        ["crime rate per lakh", "rate per lakh", "crime rate"],
    "murder":            ["murder"],
    "rape":              ["rape"],
    "kidnapping":        ["kidnapping", "abduction"],
    "dacoity":           ["dacoity"],
    "robbery":           ["robbery"],
    "burglary":          ["burglary"],
    "theft":             ["theft"],
    "riots":             ["riots"],
    "crimes_women":      ["crimes against women", "crime against women"],
    "crimes_children":   ["crimes against children", "crime against children"],
    "crimes_sc":         ["crimes against sc", "crime against sc", "atrocities on sc"],
    "crimes_st":         ["crimes against st", "crime against st", "atrocities on st"],
}

STATE_NAME_ALIASES: dict[str, str] = {
    "andhra pradesh (new)": "Andhra Pradesh",
    "andhra pradesh*":      "Andhra Pradesh",
    "a&n islands":          None,  # skip
    "d&n haveli":           None,
    "daman & diu":          None,
}


def _match_col(header: str, aliases: list[str]) -> bool:
    h = header.lower().strip()
    return any(a in h for a in aliases)


def _parse_num(val: str) -> float | None:
    val = val.strip().replace(",", "").replace("N.A.", "").replace("-", "")
    if not val:
        return None
    try:
        return float(val)
    except ValueError:
        return None


def _find_csv_col(headers: list[str], aliases: list[str]) -> int | None:
    """Return column index whose header matches any alias, or None."""
    for i, h in enumerate(headers):
        if _match_col(h, aliases):
            return i
    return None


def parse_state_csv(path: Path, year: str) -> dict[str, dict]:
    """
    Parse one year's NCRB state-level CSV.
    Returns {state_name: {metric: value}} for focus states only.
    """
    results: dict[str, dict] = {}

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        raise ValueError(f"Empty CSV: {path}")

    # Find header row: first row where the first cell looks like "State" / "UT"
    header_idx = 0
    for i, row in enumerate(rows):
        if row and any(k in row[0].lower() for k in ("state", "uts", "union")):
            header_idx = i
            break

    headers = rows[header_idx]

    # Build column index map
    col_map: dict[str, int | None] = {}
    for key, aliases in COLUMN_ALIASES.items():
        col_map[key] = _find_csv_col(headers, aliases)

    if col_map["total_ipc"] is None and col_map["crime_rate"] is None:
        raise ValueError(
            f"Could not identify required columns in {path.name}.\n"
            f"Headers found: {headers[:10]}"
        )

    for row in rows[header_idx + 1:]:
        if not row or not row[0].strip():
            continue

        raw_state = row[0].strip().strip("*").rstrip("1234567890").strip()
        # Canonical alias lookup
        canonical = STATE_NAME_ALIASES.get(raw_state.lower())
        if canonical is None and raw_state.lower() in STATE_NAME_ALIASES:
            continue  # explicitly skipped
        if canonical is None:
            canonical = raw_state  # use as-is

        if canonical not in FOCUS_STATES:
            continue

        def get(key: str) -> float | None:
            idx = col_map.get(key)
            if idx is None or idx >= len(row):
                return None
            return _parse_num(row[idx])

        results[canonical] = {
            "total_ipc_crimes":    get("total_ipc"),
            "crime_rate_per_lakh": get("crime_rate"),
            "murder":              get("murder"),
            "rape":                get("rape"),
            "kidnapping":          get("kidnapping"),
            "dacoity":             get("dacoity"),
            "robbery":             get("robbery"),
            "burglary":            get("burglary"),
            "theft":               get("theft"),
            "riots":               get("riots"),
            "crimes_against_women":    get("crimes_women"),
            "crimes_against_children": get("crimes_children"),
            "crimes_against_sc":       get("crimes_sc"),
            "crimes_against_st":       get("crimes_st"),
        }

    return results


def find_state_csvs() -> list[tuple[str, Path]]:
    """Return sorted [(year, path)] for all ncrb_state_{year}.csv files found."""
    pairs = []
    for p in RAW_DIR.glob("ncrb_state_*.csv"):
        m = re.search(r"ncrb_state_(\d{4})\.csv", p.name)
        if m:
            pairs.append((m.group(1), p))
    return sorted(pairs, key=lambda x: x[0])


def print_reminder():
    print()
    print("━" * 65)
    print("  NCRB — NO DATA FILES FOUND")
    print("━" * 65)
    print(f"  Expected: {RAW_DIR}/ncrb_state_{{year}}.csv")
    print()
    print("  To download:")
    print("  1. Visit: https://ncrb.gov.in/crime-in-india")
    print("     OR:    https://www.data.gov.in/catalog/crime-india-2023")
    print("  2. Download Chapter 1 → Table 1 (State/UT-wise Total IPC Crimes)")
    print("  3. Export as CSV, save as:  data/raw/ncrb/ncrb_state_2023.csv")
    print("  4. Repeat for each year (2021, 2022, 2023)")
    print()
    print("  Data.gov.in direct URL (2020-2022 combined CSV):")
    print("    https://www.data.gov.in/resource/stateut-wise-number-indian-penal-code-ipc-crimes-2020-2022")
    print("━" * 65)


def main():
    upload = "--upload" in sys.argv
    probe  = "--probe"  in sys.argv

    if probe:
        csvs = find_state_csvs()
        if not csvs:
            print("No ncrb_state_*.csv files found in", RAW_DIR)
            return
        for year, path in csvs:
            print(f"\n── {path.name} ────────────────────────────────")
            with open(path, newline="", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                rows = list(reader)
            for i, row in enumerate(rows[:5]):
                print(f"  row {i}: {row[:8]}")
        return

    csvs = find_state_csvs()
    if not csvs:
        print_reminder()
        sys.exit(0)

    ts = load_timeseries(OUT_PATH)
    meta = {
        "dataset": "ncrb",
        "source":  "NCRB Crime in India annual reports",
        "url":     "https://ncrb.gov.in/crime-in-india",
        "note":    "State/UT-wise total cognizable crimes under IPC. Focus states only.",
    }

    total_snapshots = 0
    first = True
    for year, path in csvs:
        print(f"Parsing {path.name} …")
        try:
            state_data = parse_state_csv(path, year)
        except ValueError as e:
            print(f"  ERROR: {e}")
            continue

        for state, metrics in sorted(state_data.items()):
            upsert_snapshot(ts, state, year, metrics, meta=meta if first else None)
            first = False
            total_snapshots += 1
            print(f"  {state:<25} crime_rate={metrics.get('crime_rate_per_lakh')} "
                  f"total={metrics.get('total_ipc_crimes')}")

    save_timeseries(ts, OUT_PATH)
    print(f"\nWrote {OUT_PATH}  ({OUT_PATH.stat().st_size // 1024} KB)")
    print(f"Total snapshots: {total_snapshots}")

    if upload:
        print("\nUploading to Firestore …")
        db = get_firestore_client()
        count = 0
        for display_name, entity in ts["entities"].items():
            for data_period, snapshot in entity["snapshots"].items():
                upload_snapshot_to_firestore(db, "ncrb", display_name, data_period, snapshot)
                count += 1
        print(f"  Uploaded {count} NCRB snapshots to Firestore.")


if __name__ == "__main__":
    main()
