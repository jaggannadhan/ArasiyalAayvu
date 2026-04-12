"""
Job: 6-monthly Cost-of-Living refresh (Tamil Nadu non-fuel)
Schedule: 1st April and 1st October, 07:00 IST

Scrapes what's automatable:
  - Aavin dairy prices (LiveChennai)

Flags what requires manual verification:
  - Electricity slabs (TNERC quarterly orders — check tnebbillcalculator.info)
  - PDS ration prices (government policy, changes rarely)
  - Transport fares (TNSTC/MTC/Metro, changes by government order)
  - Healthcare (private hospital packages, changes annually)

Creates a new snapshot in:
  cost_of_living/cost_of_living_tamil_nadu/snapshots/{YYYY-MM}

Run:
    .venv/bin/python3 scrapers/jobs/col_refresh.py
    .venv/bin/python3 scrapers/jobs/col_refresh.py --dry-run
    .venv/bin/python3 scrapers/jobs/col_refresh.py --skip-manual-check
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT / "scrapers"))
from ts_utils import load_timeseries, upsert_snapshot, save_timeseries, upload_snapshot_to_firestore, get_firestore_client
import col_ingest  # pull the current hardcoded data blocks

TS_PATH = ROOT / "data" / "processed" / "col_ts.json"
ENTITY  = "Cost_of_Living_Tamil_Nadu"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
TIMEOUT = 20

MANUAL_CHECKS = [
    {
        "field": "electricity.slabs",
        "source": "https://tnebbillcalculator.info",
        "check": "Verify slab rates match latest TNERC order (revised quarterly). "
                 "Current order: TNERC SMT.No.6 of 2025, effective 01.07.2025.",
    },
    {
        "field": "ration_pds.commodities",
        "source": "https://rcs.tn.gov.in/pds_cooperative.php",
        "check": "Rice/wheat are free, sugar ₹25/kg, toor dal ₹30/kg, palmolein ₹25/litre. "
                 "Confirm no new GO has changed allocations or prices.",
    },
    {
        "field": "transport.tnstc_bus / mtc / chennai_metro",
        "source": "https://arasubus.tn.gov.in/fare.php",
        "check": "Fare revisions require government order. Verify no new fare hike since last snapshot.",
    },
    {
        "field": "healthcare.private_hospital_chennai",
        "source": "PristynCare / StarHealth / HexaHealth",
        "check": "Private delivery package prices shift annually. Re-check normal delivery avg and C-section range.",
    },
]


def _parse_inr(text: str) -> float | None:
    cleaned = re.sub(r"[₹,\s]", "", text)
    match = re.search(r"\d+\.?\d*", cleaned)
    return float(match.group()) if match else None


def fetch_aavin_prices() -> dict:
    """Scrape Aavin milk prices from LiveChennai."""
    url = "https://www.livechennai.com/aavin_milk_price_in_chennai.asp"
    r = httpx.get(url, headers=HEADERS, follow_redirects=True, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # Second table: Aavin Milk Products | Old Price | New Price | Retail Price
    # Rows: Blue (toned), Green (standardised), Orange (full cream), Magenta
    tables = soup.find_all("table")
    if len(tables) < 2:
        raise RuntimeError("Expected at least 2 tables on LiveChennai Aavin page")

    colour_to_type = {
        "blue":    "toned",
        "green":   "standardised",
        "orange":  "full_cream",
        "magenta": "full_cream_cardholder",  # Aavin card variant
    }

    prices: dict[str, float] = {}
    for row in tables[1].find_all("tr")[1:]:  # skip header
        cells = [c.get_text(strip=True) for c in row.find_all("td")]
        if len(cells) < 3:
            continue
        colour = cells[0].lower()
        key = colour_to_type.get(colour)
        if key:
            # columns: colour | old_price | new_price | retail_price
            # use retail_price (last column) as the definitive price
            price = _parse_inr(cells[-1])
            if price:
                prices[key] = price

    if not prices:
        raise RuntimeError("Could not parse any Aavin prices from LiveChennai")
    return prices


def build_snapshot(aavin: dict, prev_snapshot: dict | None) -> dict:
    """
    Build the new TN snapshot. For non-scraped sections, carry forward
    the previous snapshot's values (so history is preserved, not blanked).
    """
    period = date.today().strftime("%Y-%m")
    prev = prev_snapshot or {}

    food_dairy = {
        "milk_aavin_toned": {
            "price": aavin.get("toned"),
            "unit": "per litre",
            "brand": "Aavin (state dairy)",
            "type": "retail",
            "source": "LiveChennai",
            "as_of": period,
        },
        "milk_aavin_standardised": {
            "price": aavin.get("standardised"),
            "unit": "per litre",
            "brand": "Aavin (state dairy)",
            "type": "retail",
            "source": "LiveChennai",
            "as_of": period,
        },
        "milk_aavin_full_cream": {
            "price": aavin.get("full_cream"),
            "unit": "per litre",
            "brand": "Aavin (state dairy)",
            "type": "retail",
            "source": "LiveChennai",
            "as_of": period,
            "notes": f"Aavin cardholders: ₹{aavin.get('full_cream_cardholder', 'N/A')}/litre",
        },
        "milk_private_toned": prev.get("food_dairy", {}).get("milk_private_toned",
            col_ingest.COL_DATA["food_dairy"]["milk_private_toned"]),
        "milk_private_full_cream": prev.get("food_dairy", {}).get("milk_private_full_cream",
            col_ingest.COL_DATA["food_dairy"]["milk_private_full_cream"]),
    }

    return {
        "electricity":        prev.get("electricity",        col_ingest.COL_DATA["electricity"]),
        "food_dairy":         food_dairy,
        "pds_infrastructure": prev.get("pds_infrastructure", col_ingest.COL_DATA["pds_infrastructure"]),
        "ration_pds":         prev.get("ration_pds",         col_ingest.COL_DATA["ration_pds"]),
        "transport":          prev.get("transport",          col_ingest.COL_DATA["transport"]),
        "healthcare":         prev.get("healthcare",         col_ingest.COL_DATA["healthcare"]),
        "_refresh_note": {
            "auto_refreshed": ["food_dairy.milk_aavin_*"],
            "carried_forward": ["electricity", "pds_infrastructure", "ration_pds", "transport", "healthcare"],
            "manual_checks_pending": [c["field"] for c in MANUAL_CHECKS],
        },
    }


def print_manual_checklist():
    print("\n" + "━" * 60)
    print("  MANUAL VERIFICATION REQUIRED")
    print("  The following sections were carried forward from the")
    print("  previous snapshot. Verify before marking as reviewed.")
    print("━" * 60)
    for i, check in enumerate(MANUAL_CHECKS, 1):
        print(f"\n  {i}. {check['field']}")
        print(f"     Source : {check['source']}")
        print(f"     Check  : {check['check']}")
    print("\n  After verifying, update col_ingest.py and re-run:")
    print("  .venv/bin/python3 scrapers/col_ingest.py --upload")
    print("━" * 60)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-manual-check", action="store_true",
                        help="Suppress manual checklist output (use in CI)")
    args = parser.parse_args()

    period = date.today().strftime("%Y-%m")
    print(f"CoL (non-fuel) refresh — period: {period}")

    # Get previous TN snapshot to carry forward non-scraped sections
    ts = load_timeseries(TS_PATH)
    entity_snaps = ts.get("entities", {}).get(ENTITY, {}).get("snapshots", {})
    prev_period = sorted(entity_snaps.keys())[-1] if entity_snaps else None
    prev_snapshot = entity_snaps.get(prev_period) if prev_period else None
    if prev_period:
        print(f"  Previous snapshot: {prev_period} — carrying forward non-scraped sections")

    print("  Fetching Aavin milk prices from LiveChennai...")
    aavin = fetch_aavin_prices()
    print(f"    Toned           ₹{aavin.get('toned')}/litre")
    print(f"    Standardised    ₹{aavin.get('standardised')}/litre")
    print(f"    Full cream      ₹{aavin.get('full_cream')}/litre")
    print(f"    Full cream card ₹{aavin.get('full_cream_cardholder')}/litre")

    snapshot = build_snapshot(aavin, prev_snapshot)

    if not args.skip_manual_check:
        print_manual_checklist()

    if args.dry_run:
        print("\n[dry-run] Snapshot (not written):")
        print(json.dumps({"food_dairy": snapshot["food_dairy"], "_refresh_note": snapshot["_refresh_note"]}, indent=2, ensure_ascii=False))
        return

    upsert_snapshot(ts, ENTITY, period, snapshot)
    save_timeseries(ts, TS_PATH)
    print(f"\n  Saved snapshot {ENTITY}/{period} to {TS_PATH}")

    db = get_firestore_client()
    upload_snapshot_to_firestore(db, "cost_of_living", ENTITY, period, snapshot)
    print(f"  Uploaded to Firestore: cost_of_living/{ENTITY.lower().replace(' ','_')}/snapshots/{period}")


if __name__ == "__main__":
    main()
