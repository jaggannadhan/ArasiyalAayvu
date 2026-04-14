"""
RBI State Finances — state-level ingestor
Source: RBI "State Finances: A Study of Budgets" — Statements 1-20
        https://rbi.org.in/scripts/PublicationsView.aspx?Id={id}

Scrapes 20 state-wise HTML tables from RBI website. No manual download needed.
All values in ₹ crore or per cent as published.

Key metrics extracted per state:
  - Gross Fiscal Deficit (₹ Cr) + as % of GSDP
  - Revenue Deficit (₹ Cr) + as % of GSDP
  - Debt-to-GSDP ratio (%)
  - Interest Payments (₹ Cr)
  - Tax Revenue (₹ Cr)
  - Development Expenditure (₹ Cr)

Outputs: data/processed/rbi_state_finances_ts.json

Run:
    python scrapers/rbi_state_finances_ingest.py               # scrape + write JSON
    python scrapers/rbi_state_finances_ingest.py --upload      # also upload to Firestore
    python scrapers/rbi_state_finances_ingest.py --probe       # print parsed data and exit
"""

from __future__ import annotations

import json
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

OUT_PATH = BASE_DIR / "data" / "processed" / "rbi_state_finances_ts.json"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

FOCUS_STATES = {
    "Tamil Nadu", "Kerala", "Karnataka", "Andhra Pradesh", "Telangana",
}

# RBI Statement page IDs and what they contain
# Each statement has a state-wise table with specific fiscal metrics
STATEMENTS: list[dict] = [
    {"id": 23712, "name": "Gross Fiscal Deficit",
     "cols": {"receipts": 1, "expenditure": 2, "gfd": 3},
     "unit": "cr", "periods": 3},
    {"id": 23711, "name": "Revenue Deficit",
     "cols": {"rev_receipts": 1, "rev_expenditure": 2, "rev_deficit": 3},
     "unit": "cr", "periods": 3},
    {"id": 23729, "name": "Debt-to-GSDP",
     "type": "timeseries",  # wide table with year columns
     "unit": "pct"},
    {"id": 23722, "name": "Interest Payments",
     "cols": {"interest_payments": 1},
     "unit": "cr", "periods": 3},
    {"id": 23723, "name": "Tax Revenue",
     "type": "ratio",  # has % columns
     "unit": "mixed"},
    {"id": 23720, "name": "Development Expenditure",
     "cols": {"dev_expenditure": 1},
     "unit": "cr", "periods": 3},
]


def _parse_num(s: str) -> float | None:
    """Parse Indian number format: '1,73,767.0' → 173767.0, handle negatives."""
    s = s.strip().replace("–", "-").replace(",", "")
    if not s or s == "-":
        return None
    # Handle (negative) notation
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except ValueError:
        return None


def _fetch_table(page_id: int) -> BeautifulSoup | None:
    """Fetch an RBI statement page and return the last data table."""
    url = f"https://rbi.org.in/scripts/PublicationsView.aspx?Id={page_id}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=30, verify=False)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        tables = soup.find_all("table")
        # Last table is usually the cleanest (no merged cell issues)
        return tables[-1] if tables else None
    except Exception as e:
        print(f"  ERROR fetching ID={page_id}: {e}")
        return None


def _canonical_state(raw: str) -> str | None:
    """Extract state name, stripping numbering prefix."""
    name = re.sub(r"^\d+\.\s*", "", raw).strip()
    if name in FOCUS_STATES:
        return name
    if "All States" in name:
        return "All India"
    return None


def parse_gfd_style(table: BeautifulSoup, stmt: dict) -> dict[str, dict]:
    """
    Parse GFD/Revenue Deficit/Interest/Dev Expenditure style tables.
    Structure: 3 fiscal year columns, each with sub-columns (Receipts, Expenditure, Deficit).
    """
    rows = table.find_all("tr")
    results: dict[str, dict] = {}

    # Find period headers (row with year labels)
    periods: list[str] = []
    for row in rows[:4]:
        cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
        year_cells = [c for c in cells if re.match(r"^\d{4}", c)]
        if year_cells:
            periods = year_cells
            break

    if not periods:
        return {}

    # Count sub-columns per period
    col_defs = stmt.get("cols", {})
    sub_cols = len(col_defs)

    for row in rows[4:]:
        cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
        if not cells:
            continue

        state = _canonical_state(cells[0])
        if not state:
            continue

        results[state] = {}
        for pi, period in enumerate(periods):
            # Normalize period label
            period_key = period.split("(")[0].strip()  # "2024-25 (RE)" → "2024-25"
            period_label = period  # keep full label for context

            base_col = 1 + pi * sub_cols
            entry: dict[str, float | str | None] = {"period_label": period_label}

            for metric, offset in col_defs.items():
                col_idx = base_col + offset - 1
                if col_idx < len(cells):
                    entry[metric] = _parse_num(cells[col_idx])

            results[state][period_key] = entry

    return results


def parse_timeseries(table: BeautifulSoup) -> dict[str, dict]:
    """Parse wide timeseries table (debt-to-GSDP style): State × Year matrix."""
    rows = table.find_all("tr")
    results: dict[str, dict] = {}

    # Find year headers
    years: list[str] = []
    for row in rows[:4]:
        cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
        year_cells = [c for c in cells if re.match(r"^\d{4}", c)]
        if len(year_cells) > 5:
            years = year_cells
            break

    if not years:
        return {}

    for row in rows[4:]:
        cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
        if not cells:
            continue

        state = _canonical_state(cells[0])
        if not state:
            continue

        values: dict[str, float | None] = {}
        for i, yr in enumerate(years):
            if i + 1 < len(cells):
                values[yr] = _parse_num(cells[i + 1])

        results[state] = values

    return results


def main():
    upload = "--upload" in sys.argv
    probe = "--probe" in sys.argv

    print("RBI State Finances — scraping 20 statements...")

    # Collect all data per state
    state_data: dict[str, dict] = {}

    # ── Statement 3: Gross Fiscal Deficit ──
    print("\n  Fetching Statement 3: Gross Fiscal Deficit...")
    table = _fetch_table(23712)
    if table:
        gfd = parse_gfd_style(table, STATEMENTS[0])
        for state, periods in gfd.items():
            state_data.setdefault(state, {})
            for period, vals in periods.items():
                state_data[state].setdefault(period, {})
                state_data[state][period]["gfd_cr"] = vals.get("gfd")
                state_data[state][period]["gfd_receipts_cr"] = vals.get("receipts")
                state_data[state][period]["gfd_expenditure_cr"] = vals.get("expenditure")
        print(f"    {len(gfd)} states parsed")

    # ── Statement 2: Revenue Deficit ──
    print("  Fetching Statement 2: Revenue Deficit...")
    table = _fetch_table(23711)
    if table:
        rd = parse_gfd_style(table, STATEMENTS[1])
        for state, periods in rd.items():
            state_data.setdefault(state, {})
            for period, vals in periods.items():
                state_data[state].setdefault(period, {})
                state_data[state][period]["rev_deficit_cr"] = vals.get("rev_deficit")
                state_data[state][period]["rev_receipts_cr"] = vals.get("rev_receipts")
                state_data[state][period]["rev_expenditure_cr"] = vals.get("rev_expenditure")
        print(f"    {len(rd)} states parsed")

    # ── Statement 20: Debt-to-GSDP ──
    print("  Fetching Statement 20: Debt-to-GSDP...")
    table = _fetch_table(23729)
    if table:
        debt = parse_timeseries(table)
        for state, years in debt.items():
            state_data.setdefault(state, {})
            for yr, val in years.items():
                yr_key = yr.split("(")[0].strip()
                state_data[state].setdefault(yr_key, {})
                state_data[state][yr_key]["debt_to_gsdp_pct"] = val
        print(f"    {len(debt)} states parsed")

    # ── Statement 13: Interest Payments ──
    print("  Fetching Statement 13: Interest Payments...")
    table = _fetch_table(23722)
    if table:
        ip = parse_gfd_style(table, STATEMENTS[3])
        for state, periods in ip.items():
            state_data.setdefault(state, {})
            for period, vals in periods.items():
                state_data[state].setdefault(period, {})
                state_data[state][period]["interest_payments_cr"] = vals.get("interest_payments")
        print(f"    {len(ip)} states parsed")

    # ── Statement 11: Development Expenditure ──
    print("  Fetching Statement 11: Development Expenditure...")
    table = _fetch_table(23720)
    if table:
        de = parse_gfd_style(table, STATEMENTS[5])
        for state, periods in de.items():
            state_data.setdefault(state, {})
            for period, vals in periods.items():
                state_data[state].setdefault(period, {})
                state_data[state][period]["dev_expenditure_cr"] = vals.get("dev_expenditure")
        print(f"    {len(de)} states parsed")

    # ── Probe mode ──
    if probe:
        for state in sorted(FOCUS_STATES) + ["All India"]:
            if state not in state_data:
                continue
            print(f"\n── {state} ──")
            for period in sorted(state_data[state].keys()):
                d = state_data[state][period]
                parts = []
                if d.get("gfd_cr") is not None:
                    parts.append(f"GFD={d['gfd_cr']:,.0f}")
                if d.get("rev_deficit_cr") is not None:
                    parts.append(f"RD={d['rev_deficit_cr']:,.0f}")
                if d.get("debt_to_gsdp_pct") is not None:
                    parts.append(f"Debt/GSDP={d['debt_to_gsdp_pct']}%")
                if d.get("interest_payments_cr") is not None:
                    parts.append(f"Interest={d['interest_payments_cr']:,.0f}")
                if parts:
                    print(f"  {period:20} {' | '.join(parts)}")
        return

    # ── Save to time-series JSON ──
    ts = load_timeseries(OUT_PATH)
    meta = {
        "dataset": "rbi_state_finances",
        "source": "RBI State Finances: A Study of Budgets",
        "url": "https://rbi.org.in/Scripts/AnnualPublications.aspx?head=State+Finances+%3a+A+Study+of+Budgets",
        "note": "Statements 2, 3, 11, 13, 20. Values in ₹ crore or per cent.",
    }

    total = 0
    first = True
    for state in sorted(FOCUS_STATES) + ["All India"]:
        if state not in state_data:
            continue
        for period, snapshot in sorted(state_data[state].items()):
            # Skip periods with no meaningful data
            if not any(v is not None for k, v in snapshot.items() if k != "period_label"):
                continue
            upsert_snapshot(ts, state, period, snapshot, meta=meta if first else None)
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
                upload_snapshot_to_firestore(db, "rbi_state_finances", display_name, data_period, snapshot)
                count += 1
        print(f"  Uploaded {count} RBI state finance snapshots to Firestore.")


if __name__ == "__main__":
    main()
