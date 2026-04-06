"""
MLA Profiles Ingest — Tamil Nadu 16th Assembly (2021–2026)
==========================================================
Scrapes assembly.tn.gov.in/16thassembly/members.php for:
  - MLA name + alias names
  - Party + constituency (with tenure from candidate_accountability)
  - Photo URL (assembly.tn.gov.in hosted)
  - Contact info (phone, email)

Writes to Firestore `mla_profiles` collection.
Doc ID: slugified canonical name (e.g. "mk_mohan")

Usage
-----
  # Dry run (print records, no upload)
  .venv/bin/python scrapers/mla_profiles_ingest.py --dry-run

  # Full upload
  .venv/bin/python scrapers/mla_profiles_ingest.py

  # Resume (skip docs that already exist)
  .venv/bin/python scrapers/mla_profiles_ingest.py --resume
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from google.cloud import firestore
except ImportError:
    firestore = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
COLLECTION   = "mla_profiles"
BASE_URL     = "https://assembly.tn.gov.in/16thassembly"
MEMBERS_URL  = f"{BASE_URL}/members.php"
PHOTO_BASE   = f"{BASE_URL}/members"
HEADERS      = {"User-Agent": "Mozilla/5.0 (compatible; ArasiyalAayvuResearchBot/2.0)"}
NOW_ISO      = datetime.now(timezone.utc).isoformat()

MAP_PATH     = ROOT / "web" / "src" / "lib" / "constituency-map.json"

# Constituency number → slug (built from map + known numbering)
# assembly.tn.gov.in photo filenames use constituency numbers (NNN.jpg)
# We derive slug from constituency name in the page itself.

# ---------------------------------------------------------------------------

def _slugify(value: str) -> str:
    return re.sub(r"_+", "_",
        re.sub(r"[^a-z0-9]", "_", value.lower())
    ).strip("_")


def _name_slug(name: str) -> str:
    """Canonical doc ID from MLA name."""
    # Strip honorifics / initials noise for doc ID
    return _slugify(name)


def _parse_name_party_constituency(cell_text: str) -> tuple[str, str, str]:
    """
    Parse cell like: 'Aassan Maulaana, JMH. (INC) (Velachery)'
    Returns (name, party, constituency)
    """
    # Extract party and constituency from parentheses (last two)
    matches = re.findall(r'\(([^)]+)\)', cell_text)
    party        = matches[-2].strip() if len(matches) >= 2 else ""
    constituency = matches[-1].strip() if len(matches) >= 1 else ""
    # Name is everything before the first '('
    name = re.sub(r'\s*\(.*', '', cell_text).strip().rstrip(',').strip()
    return name, party, constituency


def _parse_phones(raw: str) -> list[str]:
    """Extract phone numbers — split on whitespace/comma, keep digit groups."""
    raw = raw.strip()
    if not raw:
        return []
    # Normalise separators
    tokens = re.split(r'[\s,/]+', raw)
    phones = []
    buf = ""
    for tok in tokens:
        tok = tok.strip()
        if not tok:
            continue
        digits = re.sub(r'\D', '', tok)
        if not digits:
            continue
        # Assembly site formats: "044 - 47455502" or "98403 84523"
        # Join parts that form one number
        buf += digits
        if len(buf) >= 10:
            phones.append(buf[:11])  # cap at 11 digits (mobile with 0 prefix)
            buf = buf[11:]
    if buf and len(buf) >= 6:
        phones.append(buf)
    return phones


def _name_variants(name: str) -> list[str]:
    """
    Generate alias name variants:
      'Mohan, M.K.' → ['Mohan, M.K.', 'M.K. Mohan', 'MK Mohan']
    """
    variants: list[str] = [name]
    # If name contains a comma → "Surname, Initials" → also "Initials Surname"
    if ',' in name:
        parts = [p.strip() for p in name.split(',', 1)]
        if len(parts) == 2:
            flipped = f"{parts[1]} {parts[0]}"
            if flipped not in variants:
                variants.append(flipped)
            # Also without dots in initials
            no_dots = re.sub(r'\.', '', flipped)
            if no_dots != flipped and no_dots not in variants:
                variants.append(no_dots)
    return variants


def _constituency_slug_from_map(constituency_name: str) -> str | None:
    """Look up constituency slug from constituency-map.json by name."""
    try:
        cmap = json.loads(MAP_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    key = re.sub(r"\s+", " ", constituency_name.upper().strip())
    for slug, meta in cmap.items():
        map_key = re.sub(r"\s+", " ", meta.get("name", "").upper().strip())
        if map_key == key:
            return slug
    # Fuzzy: strip SC/ST suffix and try again
    base_key = re.sub(r"\s*\([Ss][CTct]\)\s*$", "", key).strip()
    for slug, meta in cmap.items():
        map_key = re.sub(r"\s+", " ", meta.get("name", "").upper().strip())
        if map_key == base_key or map_key == key:
            return slug
    return None


def scrape_members() -> list[dict[str, Any]]:
    print(f"Fetching {MEMBERS_URL} …")
    resp = requests.get(MEMBERS_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    rows = soup.select("table tr")
    print(f"  {len(rows) - 1} rows found")

    members = []
    for row in rows[1:]:
        cells = row.find_all("td")
        if len(cells) < 5:
            continue

        name_cell = cells[1].get_text(" ", strip=True)
        if not name_cell or "Vacant" in name_cell:
            continue

        name, party, constituency = _parse_name_party_constituency(name_cell)
        if not name:
            continue

        address  = cells[2].get_text(" ", strip=True)
        phone_raw = cells[3].get_text(" ", strip=True)
        email    = cells[4].get_text(" ", strip=True).lower().strip()

        # Photo URL from img src
        img_tag  = cells[5].find("img") if len(cells) > 5 else None
        photo_url = f"{BASE_URL}/{img_tag['src']}" if img_tag else None

        phones   = _parse_phones(phone_raw)
        aliases  = _name_variants(name)
        con_slug = _constituency_slug_from_map(constituency)

        doc_id   = _name_slug(name)

        members.append({
            "doc_id":          doc_id,
            "person_name":     name,
            "alias_names":     aliases,
            "parties": [
                {
                    "party":       party,
                    "constituency": constituency,
                    "constituency_slug": con_slug,
                    "tenure":      "2021–2026",
                    "election_year": 2021,
                }
            ],
            "photo_url":       photo_url,
            "contact_info": {
                "emails":      [email] if email else [],
                "ph_numbers":  phones,
                "address":     address,
            },
            "_source":         "assembly.tn.gov.in/16thassembly/members.php",
            "_ingested_at":    NOW_ISO,
        })

    return members


def upload(records: list[dict], project_id: str, dry_run: bool, resume: bool) -> None:
    if dry_run:
        print(f"\n[DRY RUN] {len(records)} records — sample output:\n")
        for r in records[:5]:
            print(f"  {r['doc_id']}")
            print(f"    name:    {r['person_name']}")
            print(f"    aliases: {r['alias_names']}")
            print(f"    party:   {r['parties'][0]['party']} · {r['parties'][0]['constituency']}")
            print(f"    photo:   {r['photo_url']}")
            print(f"    email:   {r['contact_info']['emails']}")
            print(f"    phones:  {r['contact_info']['ph_numbers']}")
            print()
        return

    if firestore is None:
        sys.exit("ERROR: google-cloud-firestore not installed")

    db = firestore.Client(project=project_id)
    col = db.collection(COLLECTION)

    existing: set[str] = set()
    if resume:
        existing = {d.id for d in col.stream()}
        print(f"[resume] {len(existing)} docs already exist — skipping")

    written = skipped = 0
    for rec in records:
        if rec["doc_id"] in existing:
            skipped += 1
            continue
        col.document(rec["doc_id"]).set(rec, merge=True)
        print(f"  ✓ {rec['doc_id']}  ({rec['parties'][0]['constituency']})")
        written += 1
        time.sleep(0.05)

    print(f"\nDone — written={written} skipped={skipped}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest TN MLA profiles from assembly.tn.gov.in")
    ap.add_argument("--dry-run",  action="store_true")
    ap.add_argument("--resume",   action="store_true", help="Skip docs that already exist")
    ap.add_argument("--project",  default="naatunadappu")
    args = ap.parse_args()

    records = scrape_members()
    print(f"Parsed {len(records)} active MLA records")
    upload(records, args.project, dry_run=args.dry_run, resume=args.resume)


if __name__ == "__main__":
    main()
