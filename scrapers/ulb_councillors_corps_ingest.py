"""
ULB Councillors Ingest — Tamil Nadu Municipal Corporations (2022)
=================================================================
Scrapes ward-level election results from OneIndia for all 21 TN municipal
corporations and writes docs to `ulb_councillors` Firestore collection.

Ward → constituency mapping uses the LGD CSV (same source as ward_mapping_ingest.py).

GCC (Chennai) is included so its data is refreshed in the same format; existing
GCC docs from ulb_councillors_gcc_ingest.py will be overwritten via set(merge=True).

Usage
-----
  .venv/bin/python scrapers/ulb_councillors_corps_ingest.py --dry-run
  .venv/bin/python scrapers/ulb_councillors_corps_ingest.py --dry-run --corp coimbatore
  .venv/bin/python scrapers/ulb_councillors_corps_ingest.py
  .venv/bin/python scrapers/ulb_councillors_corps_ingest.py --corp coimbatore
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

CSV_PATH    = Path("/tmp/cmu_extracted/constituencies_mapping_urban.02Apr2026.csv")
MAP_PATH    = ROOT / "web" / "src" / "lib" / "constituency-map.json"
PROJECT_ID  = os.getenv("GOOGLE_CLOUD_PROJECT", "naatunadappu")
TN_STATE    = "33"
NOW_ISO     = datetime.now(timezone.utc).isoformat()
SLEEP_SEC   = 1.5
HEADERS     = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36"}

# OneIndia page URL slug → LGD "Urban Localbody Name"
CORP_CONFIG: list[dict[str, str]] = [
    {"oneindia": "avadi-corporation-elections-593",          "lgd_name": "Avadi",                       "mayor": ""},
    # GCC skipped: already fully ingested via ulb_councillors_gcc_ingest.py (OpenCity CSV)
    # {"oneindia": "chennai-corporation-elections-26",      "lgd_name": "Greater Chennai Corporation", "mayor": ""},
    {"oneindia": "coimbatore-corporation-elections-27",      "lgd_name": "Coimbatore",                  "mayor": ""},
    {"oneindia": "cuddalore-corporation-elections-68",       "lgd_name": "Cuddalore",                   "mayor": ""},
    {"oneindia": "dindigul-corporation-elections-100",       "lgd_name": "Dindigul",                    "mayor": ""},
    {"oneindia": "erode-corporation-elections-127",          "lgd_name": "Erode",                       "mayor": ""},
    {"oneindia": "hosur-corporation-elections-260",          "lgd_name": "Hosur",                       "mayor": ""},
    {"oneindia": "kancheepuram-corporation-elections-182",   "lgd_name": "Kancheepuram",                "mayor": ""},
    {"oneindia": "karur-corporation-elections-247",          "lgd_name": "Karur",                       "mayor": ""},
    {"oneindia": "kumbakonam-corporation-elections-449",     "lgd_name": "Kumbakonam",                  "mayor": ""},
    {"oneindia": "madurai-corporation-elections-268",        "lgd_name": "Madurai",                     "mayor": ""},
    {"oneindia": "nagercoil-corporation-elections-188",      "lgd_name": "Nagercoil",                   "mayor": ""},
    {"oneindia": "salem-corporation-elections-372",          "lgd_name": "Salem",                       "mayor": ""},
    {"oneindia": "sivakasi-corporation-elections-671",       "lgd_name": "Sivakasi",                    "mayor": ""},
    {"oneindia": "tambaram-corporation-elections-5",         "lgd_name": "Tambaram",                    "mayor": ""},
    {"oneindia": "thanjavur-corporation-elections-448",      "lgd_name": "Thanjavur",                   "mayor": ""},
    {"oneindia": "thoothukudi-corporation-elections-501",    "lgd_name": "Thoothukudi",                 "mayor": ""},
    {"oneindia": "tiruchirappalli-corporation-elections-523","lgd_name": "Tiruchirappalli",              "mayor": ""},
    {"oneindia": "tirunelveli-corporation-elections-543",    "lgd_name": "Tirunelveli",                 "mayor": ""},
    {"oneindia": "tiruppur-corporation-elections-571",       "lgd_name": "Tiruppur",                    "mayor": ""},
    {"oneindia": "vellore-corporation-elections-633",        "lgd_name": "Vellore",                     "mayor": ""},
]

# Normalise party abbreviations from OneIndia to our PARTY_COLORS keys
PARTY_MAP: dict[str, str] = {
    "cpi(m)":     "CPI (M)",
    "cpi (m)":    "CPI (M)",
    "cpim":       "CPI (M)",
    "aiadmk":     "AIADMK",
    "admk":       "AIADMK",
    "dmk":        "DMK",
    "inc":        "INC",
    "congress":   "INC",
    "bjp":        "BJP",
    "cpi":        "CPI",
    "pmk":        "PMK",
    "vck":        "VCK",
    "others":     "OTH",
    "ind":        "IND",
    "independent":"IND",
}


def _slugify(s: str) -> str:
    return re.sub(r"_+", "_",
                  re.sub(r"[^a-z0-9]", "_", s.lower())).strip("_")


def _norm_party(raw: str) -> str:
    k = raw.strip().lower()
    return PARTY_MAP.get(k, raw.strip())


def _ward_num_from_lgd(ward_name: str) -> int | None:
    m = re.search(r"Ward\s+No[-.](\d+)", ward_name, re.IGNORECASE)
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# Build LGD mapping:  lgd_name.lower() → {ward_number → constituency_slug}
# ---------------------------------------------------------------------------

def build_lgd_mapping() -> dict[str, dict[int, str]]:
    """Returns { lb_name_lower: { ward_number: constituency_slug } }."""
    # Import aliases from ward_mapping_ingest to handle LGD/map name mismatches
    from scrapers.ward_mapping_ingest import LGD_NAME_ALIASES, ECI_OVERRIDES as _ECI_OV

    # constituency name → slug
    with open(MAP_PATH, encoding="utf-8") as f:
        cmap = json.load(f)

    def norm(s: str) -> str:
        s = s.upper().strip()
        s = re.sub(r"\s*\(SC\)|\s*\(ST\)", "", s)
        return re.sub(r"\s+", " ", s).strip()

    name_to_slug: dict[str, str] = {norm(v["name"]): slug for slug, v in cmap.items()}
    # Overlay LGD aliases (LGD spelling → slug)
    name_to_slug.update(LGD_NAME_ALIASES)

    # ECI code overrides (identical constituency names)
    ECI_OVERRIDES = _ECI_OV

    lb_map: dict[str, dict[int, str]] = {}
    with open(CSV_PATH, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row["State Code"].strip() != TN_STATE:
                continue
            eci_raw = row["Assembly Constituency ECI Code"].strip()
            if not eci_raw or eci_raw == "0":
                continue
            eci = int(eci_raw)
            lb = row["Urban Localbody Name"].strip().lower()
            wn = _ward_num_from_lgd(row["Ward Name"])
            if wn is None:
                continue
            ac_name = norm(row["Assembly Constituency Name"].strip())
            slug = ECI_OVERRIDES.get(eci) or name_to_slug.get(ac_name)
            if not slug:
                continue
            if lb not in lb_map:
                lb_map[lb] = {}
            lb_map[lb][wn] = slug
    return lb_map


# ---------------------------------------------------------------------------
# Scrape OneIndia
# ---------------------------------------------------------------------------

def scrape_corp(oneindia_slug: str) -> list[dict[str, Any]]:
    url = f"https://www.oneindia.com/{oneindia_slug}/"
    print(f"  Fetching {url}")
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    ward_rows: list[dict[str, Any]] = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if len(cells) < 4:
                continue
            # Skip header rows
            first = cells[0].lower().strip()
            if first in ("ward no", "sl. no", "#", "s.no", "ward", "no"):
                continue
            # Must start with a ward number
            if not cells[0].strip().isdigit():
                continue
            ward_num = int(cells[0].strip())
            # columns: Ward No | Ward Name | Zone Name | Winners | Party
            # (some pages omit Ward Name, making it: Ward No | Zone Name | Winners | Party)
            if len(cells) >= 5:
                zone   = cells[2].strip()
                winner = cells[3].strip()
                party  = cells[4].strip()
                # Skip Mayor-history rows: cells[3] is a 4-digit year ("2022")
                if re.match(r"^\d{4}$", winner) or winner.lower() == "incumbent":
                    continue
            else:
                zone   = cells[1].strip()
                winner = cells[2].strip()
                party  = cells[3].strip()
                if re.match(r"^\d{4}$", winner) or winner.lower() == "incumbent":
                    continue

            ward_rows.append({
                "ward_number": ward_num,
                "zone_name":   zone,
                "councillor_name": winner,
                "party":       _norm_party(party),
            })

    # Deduplicate by ward_number (keep first occurrence)
    seen: set[int] = set()
    deduped = []
    for r in ward_rows:
        if r["ward_number"] not in seen:
            seen.add(r["ward_number"])
            deduped.append(r)
    return sorted(deduped, key=lambda x: x["ward_number"])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(dry_run: bool, only_corp: str | None) -> None:
    print("Building LGD ward → constituency mapping…")
    lgd_map = build_lgd_mapping()

    corps = CORP_CONFIG
    if only_corp:
        key = only_corp.lower()
        corps = [c for c in CORP_CONFIG if key in c["lgd_name"].lower() or key in c["oneindia"].lower()]
        if not corps:
            print(f"ERROR: no corporation matching '{only_corp}'")
            sys.exit(1)

    db = None
    if not dry_run:
        from google.cloud import firestore  # type: ignore
        db = firestore.Client(project=PROJECT_ID)

    total_written = 0
    total_skipped = 0

    for cfg in corps:
        lgd_name  = cfg["lgd_name"]
        lb_key    = lgd_name.lower()
        lb_slug   = _slugify(lgd_name)
        ward_slug_prefix = lb_slug  # e.g. "coimbatore_ward022"

        print(f"\n{'='*60}")
        print(f"  Corporation: {lgd_name} (slug: {lb_slug})")

        ward_to_slug = lgd_map.get(lb_key, {})
        if not ward_to_slug:
            print(f"  [WARN] No LGD ward mapping found for '{lb_key}'")

        try:
            wards = scrape_corp(cfg["oneindia"])
        except Exception as exc:
            print(f"  [SKIP] Fetch failed: {exc}")
            continue
        print(f"  Scraped {len(wards)} wards")
        time.sleep(SLEEP_SEC)

        if dry_run:
            unmapped = [w["ward_number"] for w in wards if w["ward_number"] not in ward_to_slug]
            print(f"  Unmapped wards: {len(unmapped)} — {unmapped[:10]}")
            for w in wards[:5]:
                ac = ward_to_slug.get(w["ward_number"], "UNKNOWN")
                print(f"    Ward {w['ward_number']:3d} | {w['zone_name']:<20} | {w['councillor_name']:<30} | {w['party']:<10} | AC: {ac}")
            continue

        assert db is not None
        col = db.collection("ulb_councillors")
        written = skipped = 0

        for w in wards:
            ac_slug = ward_to_slug.get(w["ward_number"])
            if not ac_slug:
                print(f"  [skip] Ward {w['ward_number']} — no constituency mapping")
                skipped += 1
                continue

            doc_id = f"{ward_slug_prefix}_ward{w['ward_number']:03d}"
            doc = {
                "local_body_slug":           lb_slug,
                "local_body_name":           lgd_name,
                "local_body_type":           "Municipal Corporation",
                "assembly_constituency_slug": ac_slug,
                "ward_number":               w["ward_number"],
                "zone_name":                 w["zone_name"],
                "councillor_name":           w["councillor_name"],
                "party":                     w["party"],
                "party_full":                "",
                "election_year":             2022,
                "source":                    f"https://www.oneindia.com/{cfg['oneindia']}/",
                "ingested_at":               NOW_ISO,
            }
            try:
                col.document(doc_id).set(doc, merge=True)
                written += 1
            except Exception as e:
                print(f"  [error] {doc_id}: {e}")
                skipped += 1

        print(f"  Done — written={written} skipped={skipped}")
        total_written += written
        total_skipped += skipped

    print(f"\n{'='*60}")
    if dry_run:
        print(f"[DRY RUN] Processed {len(corps)} corporations.")
    else:
        print(f"TOTAL — written={total_written} skipped={total_skipped}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest TN municipal corporation ward results (2022)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--corp",    type=str, default=None,
                    help="Limit to one corporation (substring match on name or URL slug)")
    args = ap.parse_args()
    run(dry_run=args.dry_run, only_corp=args.corp)


if __name__ == "__main__":
    main()
