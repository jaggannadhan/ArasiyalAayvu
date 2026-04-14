"""
RBI Handbook — State-wise ASI (Industrial Statistics) time-series ingestor
Source: RBI Handbook of Statistics on Indian States, Tables 116-128
        https://rbi.org.in/scripts/PublicationsView.aspx?Id={id}

Scrapes 13 state-wise tables from RBI HTML pages (20 years: 2004-2024).
No manual download needed.

Metrics per state per year:
  factories          — Number of Factories
  fixed_capital      — Fixed Capital (₹ Lakh)
  working_capital    — Working Capital (₹ Lakh)
  workers            — Number of Workers
  persons_engaged    — Total Persons Engaged
  emoluments         — Total Emoluments (₹ Lakh)
  total_input        — Total Input (₹ Lakh)
  gross_output       — Value of Gross Output (₹ Lakh)
  nva                — Net Value Added (₹ Lakh)
  gva                — Gross Value Added (₹ Lakh)

Outputs: data/processed/rbi_asi_ts.json

Run:
    python scrapers/rbi_asi_ingest.py               # scrape + write JSON
    python scrapers/rbi_asi_ingest.py --upload      # also upload to Firestore
    python scrapers/rbi_asi_ingest.py --probe       # print parsed data and exit
"""

from __future__ import annotations

import re
import sys
import warnings
from pathlib import Path

import requests
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore")

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(Path(__file__).parent))
from ts_utils import (
    load_timeseries, upsert_snapshot, save_timeseries,
    upload_snapshot_to_firestore, get_firestore_client,
)

OUT_PATH = BASE_DIR / "data" / "processed" / "rbi_asi_ts.json"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

FOCUS_STATES = {
    "Tamil Nadu", "Kerala", "Karnataka", "Andhra Pradesh", "Telangana",
}

# Table ID → metric key. Each page has 4 sub-tables (A-D, D-K, K-P, R-Z+AllIndia)
TABLES: list[dict] = [
    {"id": 23565, "metric": "factories",       "unit": "count"},
    {"id": 23566, "metric": "fixed_capital",    "unit": "lakh"},
    {"id": 23567, "metric": "working_capital",  "unit": "lakh"},
    # 23568 = Physical Working Capital — skip (less useful)
    # 23569 = Productive Capital — skip
    # 23570 = Invested Capital — skip
    {"id": 23571, "metric": "workers",          "unit": "count"},
    {"id": 23572, "metric": "persons_engaged",  "unit": "count"},
    {"id": 23573, "metric": "emoluments",       "unit": "lakh"},
    {"id": 23574, "metric": "total_input",      "unit": "lakh"},
    {"id": 23575, "metric": "gross_output",     "unit": "lakh"},
    {"id": 23576, "metric": "nva",              "unit": "lakh"},
    {"id": 23577, "metric": "gva",              "unit": "lakh"},
]


def _parse_num(s: str) -> float | None:
    s = s.strip().replace(",", "").replace("–", "").replace("-", "").replace(".", "", s.count(".") - 1 if s.count(".") > 1 else 0)
    if not s or s == "." or s == "..":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _find_state_col(header: list[str], state_name: str) -> int | None:
    """Find column index for a state in the header row."""
    for i, h in enumerate(header):
        if state_name in h:
            return i
    return None


def scrape_table(page_id: int, metric: str) -> dict[str, dict[str, float | None]]:
    """
    Scrape one ASI table page. Returns {state: {year: value}}.
    Each page has ~4 HTML tables covering different state groups.
    """
    url = f"https://rbi.org.in/scripts/PublicationsView.aspx?Id={page_id}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=30, verify=False)
        r.raise_for_status()
    except Exception as e:
        print(f"    ERROR fetching {metric}: {e}")
        return {}

    soup = BeautifulSoup(r.text, "html.parser")
    tables = soup.find_all("table")

    results: dict[str, dict[str, float | None]] = {}
    targets = list(FOCUS_STATES) + ["All-India"]

    for t in tables:
        rows = t.find_all("tr")
        if len(rows) < 3:
            continue

        # Find header row with state names
        header: list[str] = []
        header_row_idx = -1
        for ri, row in enumerate(rows[:4]):
            cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            if any("Year" in c for c in cells) and len(cells) > 3:
                header = cells
                header_row_idx = ri
                break

        if not header:
            continue

        # Find columns for our target states
        state_cols: dict[str, int] = {}
        for target in targets:
            col = _find_state_col(header, target)
            if col is not None:
                state_cols[target] = col

        if not state_cols:
            continue

        # Extract data rows
        for row in rows[header_row_idx + 1:]:
            cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            if not cells or not re.match(r"^\d{4}", cells[0]):
                continue

            year = cells[0].strip()

            for state, col_idx in state_cols.items():
                if col_idx < len(cells):
                    val = _parse_num(cells[col_idx])
                    results.setdefault(state, {})
                    results[state][year] = val

    return results


def main():
    upload = "--upload" in sys.argv
    probe = "--probe" in sys.argv

    print("RBI Handbook — State-wise ASI time-series...")

    # {state: {year: {metric: value}}}
    all_data: dict[str, dict[str, dict[str, float | None]]] = {}

    for tbl in TABLES:
        metric = tbl["metric"]
        print(f"  Fetching Table {tbl['id']} ({metric})...")
        state_data = scrape_table(tbl["id"], metric)
        for state, years in state_data.items():
            for year, val in years.items():
                all_data.setdefault(state, {}).setdefault(year, {})
                all_data[state][year][metric] = val
        print(f"    {len(state_data)} states")

    if probe:
        for state in sorted(FOCUS_STATES) + ["All-India"]:
            if state not in all_data:
                continue
            print(f"\n── {state} ──")
            for year in sorted(all_data[state].keys()):
                d = all_data[state][year]
                parts = []
                if d.get("factories") is not None:
                    parts.append(f"fac={d['factories']:,.0f}")
                if d.get("gva") is not None:
                    parts.append(f"GVA={d['gva']:,.0f}L")
                if d.get("workers") is not None:
                    parts.append(f"wrk={d['workers']:,.0f}")
                if parts:
                    print(f"  {year:12} {' | '.join(parts)}")
        return

    ts = load_timeseries(OUT_PATH)
    meta = {
        "dataset": "rbi_asi",
        "source": "RBI Handbook of Statistics on Indian States (Tables 116-128)",
        "url": "https://rbi.org.in/scripts/AnnualPublications.aspx?head=Handbook+of+Statistics+on+Indian+States",
        "note": "Annual Survey of Industries — state-wise time-series. Monetary values in ₹ lakh.",
    }

    total = 0
    first = True
    for state in sorted(FOCUS_STATES) + ["All-India"]:
        if state not in all_data:
            continue
        for year, snapshot in sorted(all_data[state].items()):
            if not any(v is not None for v in snapshot.values()):
                continue
            upsert_snapshot(ts, state if state != "All-India" else "All India",
                           year, snapshot, meta=meta if first else None)
            first = False
            total += 1

    save_timeseries(ts, OUT_PATH)
    print(f"\nWrote {OUT_PATH}  ({OUT_PATH.stat().st_size // 1024} KB)")
    print(f"Total snapshots: {total}")

    if upload:
        print("\nUploading to Firestore …")
        db = get_firestore_client()
        count = 0
        for display_name, entity in ts["entities"].items():
            for data_period, snapshot in entity["snapshots"].items():
                upload_snapshot_to_firestore(db, "rbi_asi", display_name, data_period, snapshot)
                count += 1
        print(f"  Uploaded {count} RBI ASI snapshots to Firestore.")


if __name__ == "__main__":
    main()
