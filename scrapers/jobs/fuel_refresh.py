"""
Job: Monthly fuel price refresh (Chennai)
Schedule: 1st of every month, 06:30 IST

Scrapes LPG, petrol, diesel prices from Goodreturns and creates a new
snapshot in cost_of_living/cost_of_living_india/snapshots/{YYYY-MM}.

Sources (confirmed working, IOCL direct URLs always redirect):
  LPG   — goodreturns.in/lpg-price-in-chennai.html
  Petrol — goodreturns.in/petrol-price-in-chennai.html
  Diesel — goodreturns.in/diesel-price-in-chennai.html

Run:
    .venv/bin/python3 scrapers/jobs/fuel_refresh.py
    .venv/bin/python3 scrapers/jobs/fuel_refresh.py --dry-run
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import date
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT / "scrapers"))
from ts_utils import load_timeseries, upsert_snapshot, save_timeseries, upload_snapshot_to_firestore, get_firestore_client

TS_PATH   = ROOT / "data" / "processed" / "col_ts.json"
ENTITY    = "Cost_of_Living_India"
HEADERS   = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
TIMEOUT   = 20

PMUY_SUBSIDY = -300.00   # ₹300/cylinder, up to 12/year — policy-driven, not scraped


def _parse_inr(text: str) -> float:
    """Extract first float from a string like '₹928.50' or '₹1,23,456'."""
    cleaned = re.sub(r"[₹,\s]", "", text)
    match = re.search(r"\d+\.?\d*", cleaned)
    if not match:
        raise ValueError(f"No price found in: {text!r}")
    return float(match.group())


def fetch_lpg() -> dict:
    """Scrape LPG 14.2 kg domestic + 5 kg + 19 kg commercial from Goodreturns."""
    url = "https://www.goodreturns.in/lpg-price-in-chennai.html"
    r = httpx.get(url, headers=HEADERS, follow_redirects=True, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    tables = soup.find_all("table")
    if not tables:
        raise RuntimeError("No tables found on LPG page")

    # First table: Type | Price | Price Change
    prices: dict[str, float] = {}
    for row in tables[0].find_all("tr"):
        cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
        if len(cells) < 2:
            continue
        label = cells[0].lower()
        if "14.2" in label:
            prices["lpg_14kg_domestic"] = _parse_inr(cells[1])
        elif re.search(r'\b5\s*kg\b', label):
            prices["lpg_5kg_domestic"] = _parse_inr(cells[1])
        elif "19 kg" in label or "19kg" in label:
            prices["lpg_19kg_commercial"] = _parse_inr(cells[1])

    if "lpg_14kg_domestic" not in prices:
        raise RuntimeError(f"Could not parse LPG 14.2 kg price. Rows: {[r.get_text() for r in tables[0].find_all('tr')[:5]]}")

    return prices


def fetch_petrol_diesel() -> dict:
    """Scrape today's petrol + diesel price in Chennai from Goodreturns."""
    results = {}
    for fuel, path in [("petrol", "petrol-price-in-chennai"), ("diesel", "diesel-price-in-chennai")]:
        url = f"https://www.goodreturns.in/{path}.html"
        r = httpx.get(url, headers=HEADERS, follow_redirects=True, timeout=TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        tables = soup.find_all("table")
        if not tables:
            raise RuntimeError(f"No tables on {fuel} page")

        # First table: Date | Price | Price Change — first data row = today
        rows = tables[0].find_all("tr")
        for row in rows[1:]:  # skip header
            cells = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cells) >= 2:
                results[fuel] = _parse_inr(cells[1])
                break

        if fuel not in results:
            raise RuntimeError(f"Could not parse {fuel} price")

    return results


def build_snapshot(lpg: dict, petdsl: dict) -> dict:
    period = date.today().strftime("%Y-%m")
    return {
        "fuel": {
            "lpg_14kg_domestic": {
                "price": lpg["lpg_14kg_domestic"],
                "unit": "per 14.2 kg cylinder",
                "type": "market",
                "source": "Goodreturns / IOCL",
                "as_of": period,
            },
            "lpg_14kg_ujjwala_subsidy": {
                "price": PMUY_SUBSIDY,
                "unit": "subsidy per cylinder (up to 12/year)",
                "type": "subsidy",
                "source": "PMUY / IOCL",
                "as_of": period,
                "notes": "₹300 credited to beneficiary bank account; effective cost = lpg_14kg_domestic + subsidy",
            },
            "lpg_5kg_domestic": {
                "price": lpg.get("lpg_5kg_domestic"),
                "unit": "per 5 kg cylinder",
                "type": "market",
                "source": "Goodreturns / IOCL",
                "as_of": period,
            },
            "lpg_19kg_commercial": {
                "price": lpg.get("lpg_19kg_commercial"),
                "unit": "per 19 kg cylinder",
                "type": "market",
                "source": "Goodreturns / IOCL",
                "as_of": period,
            },
            "petrol": {
                "price": petdsl["petrol"],
                "unit": "per litre",
                "type": "market",
                "source": "Goodreturns / IOCL",
                "as_of": period,
            },
            "diesel": {
                "price": petdsl["diesel"],
                "unit": "per litre",
                "type": "market",
                "source": "Goodreturns / IOCL",
                "as_of": period,
            },
        }
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Fetch and print without writing")
    args = parser.parse_args()

    period = date.today().strftime("%Y-%m")
    print(f"Fuel price refresh — period: {period}")

    print("  Fetching LPG prices from Goodreturns...")
    lpg = fetch_lpg()
    print(f"    LPG 14.2 kg  ₹{lpg['lpg_14kg_domestic']}")
    print(f"    LPG 5 kg     ₹{lpg.get('lpg_5kg_domestic', 'N/A')}")
    print(f"    LPG 19 kg    ₹{lpg.get('lpg_19kg_commercial', 'N/A')}")

    print("  Fetching petrol/diesel prices from Goodreturns...")
    petdsl = fetch_petrol_diesel()
    print(f"    Petrol       ₹{petdsl['petrol']}/litre")
    print(f"    Diesel       ₹{petdsl['diesel']}/litre")

    snapshot = build_snapshot(lpg, petdsl)

    if args.dry_run:
        import json
        print("\n[dry-run] Snapshot (not written):")
        print(json.dumps(snapshot, indent=2, ensure_ascii=False))
        return

    ts = load_timeseries(TS_PATH)
    upsert_snapshot(ts, ENTITY, period, snapshot, meta={
        "dataset": "cost_of_living",
        "note": "Cost_of_Living_India = national fuel prices (Chennai reference). Auto-refreshed monthly.",
    })
    save_timeseries(ts, TS_PATH)
    print(f"\n  Saved snapshot {ENTITY}/{period} to {TS_PATH}")

    db = get_firestore_client()
    upload_snapshot_to_firestore(db, "cost_of_living", ENTITY, period, snapshot["fuel"] | {"data_period": period})
    print(f"  Uploaded to Firestore: cost_of_living/{ENTITY.lower().replace(' ', '_')}/snapshots/{period}")


if __name__ == "__main__":
    main()
