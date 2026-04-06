"""
Pincode Scraper — Tamil Nadu
=============================
Fetches all TN pincodes from India Post API, maps district → constituency,
and uploads to Firestore `pincode_mapping` collection.

Strategy
--------
- Scans TN pincode ranges (600001–643999) in steps
- India Post API returns: district, post office names, block
- Unambiguous: pincode's post offices all fall in one constituency → direct map
- Ambiguous: pincode spans multiple constituencies → is_ambiguous=True, user picks

Constituency matching
---------------------
1. Block/taluk name match against constituency names (most specific)
2. Post office name match
3. District fallback → ambiguous with all constituencies in that district

Usage
-----
  # Discover all valid TN pincodes (no Firestore write, saves to JSON)
  .venv/bin/python3.14 scrapers/pincode_scraper.py --discover --output data/processed/tn_pincodes_raw.json

  # Build mapping from discovered pincodes
  .venv/bin/python3.14 scrapers/pincode_scraper.py --input data/processed/tn_pincodes_raw.json --dry-run

  # Full run
  .venv/bin/python3.14 scrapers/pincode_scraper.py --input data/processed/tn_pincodes_raw.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT_DIR = Path(__file__).resolve().parents[1]
MAP_PATH = ROOT_DIR / "web" / "src" / "lib" / "constituency-map.json"
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "naatunadappu")

INDIA_POST_URL = "https://api.postalpincode.in/pincode/{}"

# TN pincode ranges: 600xxx–643xxx
TN_RANGES = list(range(600001, 600120)) + \
            list(range(601001, 601302)) + \
            list(range(602001, 602106)) + \
            list(range(603001, 603320)) + \
            list(range(604001, 604410)) + \
            list(range(605001, 605115)) + \
            list(range(606001, 606213)) + \
            list(range(607001, 607807)) + \
            list(range(608001, 608703)) + \
            list(range(609001, 609811)) + \
            list(range(610001, 610213)) + \
            list(range(611001, 611111)) + \
            list(range(612001, 612806)) + \
            list(range(613001, 613703)) + \
            list(range(614001, 614806)) + \
            list(range(615001, 615702)) + \
            list(range(616001, 616302)) + \
            list(range(617001, 617702)) + \
            list(range(618001, 618706)) + \
            list(range(619001, 619702)) + \
            list(range(620001, 620025)) + \
            list(range(621001, 621730)) + \
            list(range(622001, 622515)) + \
            list(range(623001, 623807)) + \
            list(range(624001, 624710)) + \
            list(range(625001, 625708)) + \
            list(range(626001, 626210)) + \
            list(range(627001, 627860)) + \
            list(range(628001, 628953)) + \
            list(range(629001, 629810)) + \
            list(range(630001, 630812)) + \
            list(range(631001, 631702)) + \
            list(range(632001, 632602)) + \
            list(range(633001, 633460)) + \
            list(range(634001, 634403)) + \
            list(range(635001, 635852)) + \
            list(range(636001, 636902)) + \
            list(range(637001, 637505)) + \
            list(range(638001, 638812)) + \
            list(range(639001, 639210)) + \
            list(range(640001, 640025)) + \
            list(range(641001, 641120)) + \
            list(range(642001, 642210)) + \
            list(range(643001, 643253))

# India Post district name → our district key (uppercase, matches constituency-map.json)
DISTRICT_ALIASES: dict[str, str] = {
    "TIRUVALLUR":        "THIRUVALLUR",
    "TIRUVALUR":         "THIRUVARUR",
    "THIRUVALUR":        "THIRUVARUR",
    "KANCHEEPURAM":      "KANCHEEPURAM",
    "KANCHIPURAM":       "KANCHEEPURAM",
    "VILLUPURAM":        "VILLUPPURAM",
    "NAGAPATNAM":        "NAGAPATTINAM",
    "NAGAPATTINAM":      "NAGAPATTINAM",
    "TIRUCHIRAPALLI":    "TIRUCHIRAPPALLI",
    "TIRUCHCHIRAPPALLI": "TIRUCHIRAPPALLI",
    "TIRUCHIRAPPALLI":   "TIRUCHIRAPPALLI",
    "TRICHY":            "TIRUCHIRAPPALLI",
    "NILGIRIS":          "THE NILGIRIS",
    "THE NILGIRIS":      "THE NILGIRIS",
    "TIRUNELVELI KATTABO": "TIRUNELVELI",
    "TUTICORIN":         "THOOTHUKUDI",
    "THOOTHUKUDI":       "THOOTHUKUDI",
    "KANNIYAKUMARI":     "KANNIYAKUMARI",
    "KANYAKUMARI":       "KANNIYAKUMARI",
    "SIVAGANGAI":        "SIVAGANGA",
    "TIRUPUR":           "TIRUPPUR",
    "TIRUPPUR":          "TIRUPPUR",
    "DINDIGUL ANNA":     "DINDIGUL",
    "RAMANATHAPURAM":    "RAMANATHAPURAM",
    "PUDUKKOTTAI":       "PUDUKKOTTAI",
    "PUDUKOTTAI":        "PUDUKKOTTAI",
    "KRISHNAGIRI":       "KRISHNAGIRI",
    "DHARMAPURI":        "DHARMAPURI",
    "TIRUVANNAMALAI":    "TIRUVANNAMALAI",
    "THIRUVANNAMALAI":   "TIRUVANNAMALAI",
    "VELLORE":           "VELLORE",
    "NAMAKKAL":          "NAMAKKAL",
    "ARIYALUR":          "ARIYALUR",
    "PERAMBALUR":        "PERAMBALUR",
    "KARUR":             "KARUR",
    "ERODE":             "ERODE",
    "COIMBATORE":        "COIMBATORE",
    "SALEM":             "SALEM",
    "MADURAI":           "MADURAI",
    "THENI":             "THENI",
    "DINDIGUL":          "DINDIGUL",
    "VIRUDHUNAGAR":      "VIRUDHUNAGAR",
    "CUDDALORE":         "CUDDALORE",
    "THANJAVUR":         "THANJAVUR",
    "TIRUVARUR":         "THIRUVARUR",
    "CHENGALPATTU":      "CHENGALPATTU",
    "RANIPET":           "RANIPET",
    "TENKASI":           "TENKASI",
    "TIRUPATHUR":        "TIRUPATHUR",
    "KALLAKURICHI":      "KALLAKURICHI",
    "CHENNAI":           "CHENNAI",
}


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.upper().strip())


def fetch_pincode(pin: int) -> dict | None:
    """Query India Post API. Returns None if not a TN pincode or not found."""
    try:
        url = INDIA_POST_URL.format(pin)
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "ArasiyalAayvuResearchBot/1.0", "Connection": "close"},
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        if not data or data[0].get("Status") != "Success":
            return None
        offices = data[0].get("PostOffice", [])
        if not offices:
            return None
        # Filter to TN only
        tn_offices = [o for o in offices if o.get("State", "").upper() == "TAMIL NADU"]
        if not tn_offices:
            return None
        return {
            "pincode": str(pin),
            "district": tn_offices[0].get("District", ""),
            "division": tn_offices[0].get("Division", ""),
            "offices": [
                {
                    "name": o.get("Name", ""),
                    "block": o.get("Block", ""),
                    "branch_type": o.get("BranchType", ""),
                    "delivery": o.get("DeliveryStatus", ""),
                }
                for o in tn_offices
            ],
        }
    except Exception:
        return None


def discover_pincodes(output_path: Path, max_workers: int = 10) -> list[dict]:
    """Scan all TN pincode ranges and save raw data (concurrent)."""
    total = len(TN_RANGES)
    print(f"Scanning {total} potential TN pincodes with {max_workers} workers…")

    results_map: dict[int, dict] = {}
    found = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_pin = {executor.submit(fetch_pincode, pin): pin for pin in TN_RANGES}
        completed = 0
        for future in as_completed(future_to_pin):
            pin = future_to_pin[future]
            completed += 1
            try:
                result = future.result()
            except Exception:
                result = None
            if result:
                results_map[pin] = result
                found += 1
                print(f"  [{completed}/{total}] {pin} → {result['district']} ({found} found)")
            elif completed % 500 == 0:
                print(f"  [{completed}/{total}] scanned… ({found} found so far)")

    # Sort by pincode order
    results = [results_map[pin] for pin in TN_RANGES if pin in results_map]
    print(f"\nFound {len(results)} valid TN pincodes")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"Saved → {output_path}")
    return results


def build_mapping(raw_pincodes: list[dict], constituency_map: dict) -> list[dict]:
    """Convert raw pincode data into Firestore-ready pincode_mapping records."""

    # Build district → list of constituency dicts
    district_to_slugs: dict[str, list[dict]] = {}
    for slug, meta in constituency_map.items():
        dist = norm(meta.get("district", ""))
        if dist not in district_to_slugs:
            district_to_slugs[dist] = []
        district_to_slugs[dist].append({
            "slug": slug,
            "name": meta.get("name", ""),
            "name_ta": meta.get("name_ta", ""),
        })

    # Build exact name → constituency dict (strip SC/ST markers for matching)
    def clean_name(s: str) -> str:
        return re.sub(r"\s*\(S[CT]\)$", "", norm(s)).strip()

    name_to_constituency: dict[str, dict] = {}
    for slug, meta in constituency_map.items():
        key = clean_name(meta.get("name", ""))
        name_to_constituency[key] = {"slug": slug, "name": meta["name"], "name_ta": meta.get("name_ta", "")}

    records = []
    for raw in raw_pincodes:
        pin = raw["pincode"]
        raw_district = norm(raw.get("district", ""))
        district = DISTRICT_ALIASES.get(raw_district, raw_district)

        constituencies_in_district = district_to_slugs.get(district, [])
        if not constituencies_in_district:
            print(f"  WARN: no district match for {pin} → '{raw_district}'")
            continue

        # Only try to match if the district has >1 constituency (otherwise trivially unambiguous)
        if len(constituencies_in_district) == 1:
            records.append({
                "pincode": pin,
                "district": raw.get("district", "").title(),
                "constituencies": constituencies_in_district,
                "is_ambiguous": False,
            })
            continue

        # Gather candidate constituency names from delivery post offices only
        # (Delivery offices are most location-accurate; non-delivery offices use admin blocks)
        delivery_offices = [o for o in raw.get("offices", []) if o.get("delivery", "").lower() == "delivery"]
        all_offices = delivery_offices or raw.get("offices", [])

        # Collect unique non-NA blocks from delivery offices
        delivery_blocks = list(dict.fromkeys(
            clean_name(o.get("block", "")) for o in delivery_offices
            if o.get("block", "") not in ("NA", "")
        ))

        matched: list[dict] = []

        # Trust a block match only when ALL offices (delivery + non-delivery) that have
        # a non-NA block unanimously agree on the same block name.
        # Rationale: 641001 has 5 offices all saying "Coimbatore South" → safe.
        #            600057 has offices split between "Ambattur" and "Ponneri" → unsafe.
        all_blocks = list(dict.fromkeys(
            clean_name(o.get("block", "")) for o in raw.get("offices", [])
            if o.get("block", "") not in ("NA", "")
        ))
        if len(all_blocks) == 1:
            block = all_blocks[0]
            if block in name_to_constituency:
                c = name_to_constituency[block]
                if any(x["slug"] == c["slug"] for x in constituencies_in_district):
                    matched.append(c)

        # Decision: unanimous block match → unambiguous;
        # anything else → district-level ambiguous (user picks from list)
        if len(matched) == 1:
            constituencies = matched
            is_ambiguous = False
        else:
            constituencies = constituencies_in_district
            is_ambiguous = True

        records.append({
            "pincode": pin,
            "district": raw.get("district", "").title(),
            "constituencies": [
                {"slug": c["slug"], "name": c["name"], "name_ta": c.get("name_ta", "")}
                for c in constituencies
            ],
            "is_ambiguous": is_ambiguous,
        })

    unambig = sum(1 for r in records if not r["is_ambiguous"])
    print(f"Built {len(records)} records ({unambig} unambiguous, {len(records)-unambig} ambiguous)")
    return records


def upload(records: list[dict], project_id: str, dry_run: bool) -> None:
    if dry_run:
        print(f"\n[DRY RUN] Would upload {len(records)} docs to `pincode_mapping`")
        sample = records[:5]
        for r in sample:
            print(f"  {r['pincode']} → {r['district']} | ambiguous={r['is_ambiguous']} | {[c['name'] for c in r['constituencies']]}")
        return

    from google.cloud import firestore
    db = firestore.Client(project=project_id)
    col = db.collection("pincode_mapping")
    batch = db.batch()
    written = 0
    for i, rec in enumerate(records):
        batch.set(col.document(rec["pincode"]), rec, merge=True)
        written += 1
        if (i + 1) % 400 == 0:
            batch.commit()
            batch = db.batch()
            print(f"  Committed {written} docs…")
    batch.commit()
    print(f"Uploaded {written} docs to `pincode_mapping`")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--discover", action="store_true", help="Scan India Post API for all TN pincodes")
    ap.add_argument("--input", default=None, help="Path to raw pincodes JSON (from --discover)")
    ap.add_argument("--output", default="data/processed/tn_pincodes_raw.json", help="Output path for --discover")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    constituency_map = json.loads(MAP_PATH.read_text(encoding="utf-8"))

    if args.discover:
        discover_pincodes(Path(args.output))
        return

    if not args.input:
        ap.error("Provide --discover to fetch pincodes, or --input <path> to build from cached data")

    raw = json.loads(Path(args.input).read_text(encoding="utf-8"))
    print(f"Loaded {len(raw)} raw pincodes from {args.input}")

    records = build_mapping(raw, constituency_map)

    # Save mapped output
    out = Path(args.output.replace("_raw.json", "_mapped.json")) if "_raw" in (args.output or "") else Path("data/processed/tn_pincodes_mapped.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"Saved mapped → {out}")

    upload(records, PROJECT_ID, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
