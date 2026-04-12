"""
Job: Annual SDG India Index check and ingestion
Schedule: 1st July every year, 09:00 IST

NITI Aayog releases the SDG India Index report annually, typically
in June/July. The website is a JS SPA — no automated download is
possible. This job:

  1. Checks which periods are already ingested (from sdg_ts.json)
  2. Checks if a new CSV has been manually placed in data/raw/niti_sdg/
  3. If new CSV found: runs the ingestor and uploads to Firestore
  4. If no new CSV: prints a reminder with exact steps to follow

Expected CSV naming: sdg_{period}.csv
  e.g., sdg_2025_26.csv  for the 2025-26 edition

Known release pattern:
  2018      → released 2018
  2020-21   → released 2021
  2023-24   → released July 2024
  2025-26   → expected June/July 2026 (next one after this job runs)

Run:
    .venv/bin/python3 scrapers/jobs/sdg_check.py
"""

from __future__ import annotations

import subprocess
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT / "scrapers"))
from ts_utils import load_timeseries

TS_PATH  = ROOT / "data" / "processed" / "sdg_ts.json"
RAW_DIR  = ROOT / "data" / "raw" / "niti_sdg"
NITI_URL = "https://sdgindiaindex.niti.gov.in"

# All currently known period → filename mappings (kept in sync with niti_sdg_ingest.py)
KNOWN_PERIODS = {
    "2018":    "sdg_2018.csv",
    "2020-21": "sdg_2020_21.csv",
    "2023-24": "sdg_2023_24.csv",
}


def already_ingested_periods() -> set[str]:
    ts = load_timeseries(TS_PATH)
    tn = ts.get("entities", {}).get("Tamil Nadu", {}).get("snapshots", {})
    return set(tn.keys())


def find_new_csvs() -> list[Path]:
    """Return CSVs in raw/niti_sdg/ that are NOT in KNOWN_PERIODS."""
    known_files = set(KNOWN_PERIODS.values())
    return [
        p for p in sorted(RAW_DIR.glob("sdg_*.csv"))
        if p.name not in known_files
    ]


def run_ingestor():
    result = subprocess.run(
        [sys.executable, str(ROOT / "scrapers" / "niti_sdg_ingest.py"), "--upload"],
        capture_output=False,
    )
    return result.returncode == 0


def print_reminder(ingested: set[str]):
    latest = sorted(ingested)[-1] if ingested else "none"
    today  = date.today()
    print()
    print("━" * 65)
    print("  SDG INDEX — NO NEW REPORT FOUND")
    print("━" * 65)
    print(f"  Latest period ingested : {latest}")
    print(f"  Checked on             : {today.isoformat()}")
    print()
    print("  To ingest the next report when released:")
    print()
    print("  1. Open your browser and go to:")
    print(f"       {NITI_URL}")
    print()
    print("  2. Download the state-wise score CSV for the new edition.")
    print("     The file is usually in the 'Download' or 'Data' section.")
    print()
    print("  3. Save it as:")
    print(f"       data/raw/niti_sdg/sdg_<period>.csv")
    print("     e.g., for 2025-26 edition → sdg_2025_26.csv")
    print()
    print("  4. Add one line to scrapers/niti_sdg_ingest.py YEAR_FILES dict:")
    print('       "2025-26": "sdg_2025_26.csv",')
    print()
    print("  5. Re-run this job:")
    print("       .venv/bin/python3 scrapers/jobs/sdg_check.py")
    print()
    next_year = today.year + 1
    print(f"  Expected next release: June / July {next_year}")
    print("━" * 65)


def main():
    print(f"SDG Index check — {date.today().isoformat()}")

    ingested = already_ingested_periods()
    print(f"  Already ingested: {sorted(ingested)}")

    new_csvs = find_new_csvs()

    if not new_csvs:
        print("  No new CSVs found in data/raw/niti_sdg/")
        print_reminder(ingested)
        sys.exit(0)

    print(f"  Found {len(new_csvs)} new CSV(s): {[p.name for p in new_csvs]}")
    print()
    print("  !! New CSV detected — add the period to YEAR_FILES in")
    print("     scrapers/niti_sdg_ingest.py if not already done, then")
    print("     this job will re-run the ingestor automatically.")
    print()

    # Check if YEAR_FILES has been updated to include the new file
    ingestor_src = (ROOT / "scrapers" / "niti_sdg_ingest.py").read_text()
    missing_from_ingestor = [p for p in new_csvs if p.name not in ingestor_src]
    if missing_from_ingestor:
        print("  BLOCKED — the following CSVs are not yet in YEAR_FILES:")
        for p in missing_from_ingestor:
            print(f"    {p.name}")
        print()
        print("  Add them to YEAR_FILES in scrapers/niti_sdg_ingest.py")
        print("  then re-run this job.")
        sys.exit(1)

    print("  YEAR_FILES is up to date. Running ingestor with --upload...")
    success = run_ingestor()
    if success:
        new_ingested = already_ingested_periods()
        added = new_ingested - ingested
        print(f"\n  Done. New periods ingested: {sorted(added)}")
    else:
        print("\n  Ingestor failed — check output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
