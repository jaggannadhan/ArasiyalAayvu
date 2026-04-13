"""
Job: Monthly fuel price refresh — all 5 focus states
Schedule: 1st of every month, 06:30 IST

Scrapes LPG, petrol, diesel prices from Goodreturns for each state capital
and creates/updates snapshots in:
  cost_of_living/cost_of_living_{state_slug}/snapshots/{YYYY-MM}

Cities scraped:
  Tamil Nadu     → Chennai
  Kerala         → Kochi
  Karnataka      → Bengaluru
  Andhra Pradesh → Vijayawada
  Telangana      → Hyderabad

Sources (confirmed working):
  LPG    — goodreturns.in/lpg-price-in-{city}.html
  Petrol — goodreturns.in/petrol-price-in-{city}.html
  Diesel — goodreturns.in/diesel-price-in-{city}.html

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
from ts_utils import (
    load_timeseries, upsert_snapshot, save_timeseries,
    upload_snapshot_to_firestore, get_firestore_client,
)

TS_PATH = ROOT / "data" / "processed" / "col_ts.json"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
TIMEOUT = 20

PMUY_SUBSIDY = -300.00  # ₹300/cylinder, up to 12/year — policy-driven

# (state_display_name, entity_slug, goodreturns_city_slug, lpg_state_name)
# lpg_state_name: row label in Goodreturns state-wise LPG table (Table 1)
# Some cities (Chennai, Hyderabad, Vijayawada) have city-specific LPG pages
# with a direct Type|Price table. Others (Kochi, Bengaluru) only have a
# state-wise table where we look up by state name.
STATES: list[tuple[str, str, str, str]] = [
    ("Tamil Nadu",      "cost_of_living_tamil_nadu",      "chennai",    "Tamil Nadu"),
    ("Kerala",          "cost_of_living_kerala",          "kochi",      "Kerala"),
    ("Karnataka",       "cost_of_living_karnataka",       "bengaluru",  "Karnataka"),
    ("Andhra Pradesh",  "cost_of_living_andhra_pradesh",  "vijayawada", "Andhra Pradesh"),
    ("Telangana",       "cost_of_living_telangana",       "hyderabad",  "Telangana"),
]


def _parse_inr(text: str) -> float:
    """Extract first float from a string like '₹928.50' or '₹1,23,456'."""
    cleaned = re.sub(r"[₹,\s]", "", text)
    match = re.search(r"\d+\.?\d*", cleaned)
    if not match:
        raise ValueError(f"No price found in: {text!r}")
    return float(match.group())


def fetch_lpg(city: str, state_name: str) -> dict:
    """Scrape LPG 14.2 kg domestic + 5 kg + 19 kg commercial from Goodreturns.

    Goodreturns has two page layouts:
      - City-specific (Chennai, Hyderabad): Table 0 = Type|Price|Change
      - Generic (Kochi, Bengaluru): Table 0 = metro cities, Table 1 = state-wise
    We try the Type|Price table first, fall back to state-wise lookup.
    """
    url = f"https://www.goodreturns.in/lpg-price-in-{city}.html"
    r = httpx.get(url, headers=HEADERS, follow_redirects=True, timeout=TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    tables = soup.find_all("table")
    if not tables:
        raise RuntimeError(f"No tables found on LPG page for {city}")

    prices: dict[str, float] = {}

    # Strategy 1: city-specific page — Table 0 has Type|Price rows (td only, skip th)
    for row in tables[0].find_all("tr"):
        cells = [c.get_text(strip=True) for c in row.find_all("td")]
        if len(cells) < 2:
            continue
        label = cells[0].lower()
        try:
            if "domestic" in label and "14.2" in label:
                prices["lpg_14kg_domestic"] = _parse_inr(cells[1])
            elif re.search(r"\b5\s*kg\b", label):
                prices["lpg_5kg_domestic"] = _parse_inr(cells[1])
            elif "commercial" in label and "19" in label:
                prices["lpg_19kg_commercial"] = _parse_inr(cells[1])
        except ValueError:
            continue

    if "lpg_14kg_domestic" in prices:
        return prices

    # Strategy 2: state-wise table (Table 1) — find row matching state_name
    if len(tables) >= 2:
        for row in tables[1].find_all("tr"):
            cells = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cells) >= 2 and cells[0].strip().lower() == state_name.lower():
                try:
                    prices["lpg_14kg_domestic"] = _parse_inr(cells[1])
                except ValueError:
                    pass
                if len(cells) >= 3:
                    try:
                        prices["lpg_19kg_commercial"] = _parse_inr(cells[2])
                    except ValueError:
                        pass
                break

    if "lpg_14kg_domestic" not in prices:
        raise RuntimeError(f"Could not parse LPG price for {city} / {state_name}")

    return prices


def fetch_petrol_diesel(city: str) -> dict:
    """Scrape today's petrol + diesel price from Goodreturns for a city."""
    results = {}
    for fuel, slug in [("petrol", f"petrol-price-in-{city}"), ("diesel", f"diesel-price-in-{city}")]:
        url = f"https://www.goodreturns.in/{slug}.html"
        r = httpx.get(url, headers=HEADERS, follow_redirects=True, timeout=TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        tables = soup.find_all("table")
        if not tables:
            raise RuntimeError(f"No tables on {fuel} page for {city}")

        for row in tables[0].find_all("tr")[1:]:
            cells = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cells) >= 2:
                results[fuel] = _parse_inr(cells[1])
                break

        if fuel not in results:
            raise RuntimeError(f"Could not parse {fuel} price for {city}")

    return results


def build_fuel_snapshot(lpg: dict, petdsl: dict, city: str, period: str) -> dict:
    return {
        "lpg_14kg_domestic": {
            "price": lpg["lpg_14kg_domestic"],
            "unit": "per 14.2 kg cylinder",
            "type": "market",
            "source": f"Goodreturns / IOCL ({city})",
            "as_of": period,
        },
        "lpg_14kg_ujjwala_subsidy": {
            "price": PMUY_SUBSIDY,
            "unit": "subsidy per cylinder (up to 12/year)",
            "type": "subsidy",
            "source": "PMUY / IOCL",
            "as_of": period,
        },
        "lpg_5kg_domestic": {
            "price": lpg.get("lpg_5kg_domestic"),
            "unit": "per 5 kg cylinder",
            "type": "market",
            "source": f"Goodreturns / IOCL ({city})",
            "as_of": period,
        },
        "lpg_19kg_commercial": {
            "price": lpg.get("lpg_19kg_commercial"),
            "unit": "per 19 kg cylinder",
            "type": "market",
            "source": f"Goodreturns / IOCL ({city})",
            "as_of": period,
        },
        "petrol": {
            "price": petdsl["petrol"],
            "unit": "per litre",
            "type": "market",
            "source": f"Goodreturns / IOCL ({city})",
            "as_of": period,
        },
        "diesel": {
            "price": petdsl["diesel"],
            "unit": "per litre",
            "type": "market",
            "source": f"Goodreturns / IOCL ({city})",
            "as_of": period,
        },
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Fetch and print without writing")
    args = parser.parse_args()

    period = date.today().strftime("%Y-%m")
    print(f"Fuel price refresh — period: {period}")

    ts = load_timeseries(TS_PATH)
    db = None if args.dry_run else get_firestore_client()
    total = 0

    for state_name, entity_slug, city, lpg_state in STATES:
        print(f"\n── {state_name} ({city}) ──")
        try:
            lpg = fetch_lpg(city, lpg_state)
            print(f"  LPG 14.2 kg  ₹{lpg['lpg_14kg_domestic']}")

            petdsl = fetch_petrol_diesel(city)
            print(f"  Petrol       ₹{petdsl['petrol']}/litre")
            print(f"  Diesel       ₹{petdsl['diesel']}/litre")

            fuel_snapshot = build_fuel_snapshot(lpg, petdsl, city, period)

            if args.dry_run:
                continue

            # Merge fuel into existing snapshot (preserve non-fuel fields like dairy, electricity)
            existing_snaps = ts.get("entities", {}).get(entity_slug, {}).get("snapshots", {})
            prev = existing_snaps.get(period, {})
            snapshot = {**prev, "fuel": fuel_snapshot}

            upsert_snapshot(ts, entity_slug, period, snapshot, meta={
                "dataset": "cost_of_living",
                "note": f"Cost of living for {state_name}. Fuel prices auto-refreshed monthly.",
            } if total == 0 else None)

            upload_snapshot_to_firestore(
                db, "cost_of_living", entity_slug, period,
                snapshot,
            )
            print(f"  Uploaded: cost_of_living/{entity_slug}/snapshots/{period}")
            total += 1

        except Exception as e:
            print(f"  ERROR: {e}")

    if not args.dry_run:
        save_timeseries(ts, TS_PATH)
        print(f"\nSaved {TS_PATH}  ({total} state snapshots updated)")


if __name__ == "__main__":
    main()
