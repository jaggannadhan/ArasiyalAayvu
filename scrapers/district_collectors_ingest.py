"""
District Collectors Ingest — Tamil Nadu (1947–Present)
======================================================
Scrapes NIC district websites for historical lists of District Collectors.
Each district site maintains a "List of Collectors" / "Roll of Honour" page
with name + from/to tenure dates.

Writes to Firestore `district_collectors_profile` collection.
Doc ID: {district_slug}_{name_slug}_{from_year}

Data sources:
  - https://{district}.nic.in/list-of-collectors/ (and variants)
  - https://{district}.nic.in/profile-of-collector/ (current collector photo)

Usage
-----
  # Dry run (print records, no upload)
  .venv/bin/python scrapers/district_collectors_ingest.py --dry-run

  # Full upload
  .venv/bin/python scrapers/district_collectors_ingest.py

  # Resume (skip docs that already exist)
  .venv/bin/python scrapers/district_collectors_ingest.py --resume

  # Single district
  .venv/bin/python scrapers/district_collectors_ingest.py --district chennai

  # Limit to N records
  .venv/bin/python scrapers/district_collectors_ingest.py --limit 10
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
# Constants
# ---------------------------------------------------------------------------

COLLECTION = "district_collectors_profile"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ArasiyalAayvuResearchBot/2.0)"}
NOW_ISO = datetime.now(timezone.utc).isoformat()
BATCH_SIZE = 400
REQUEST_TIMEOUT = 20
DELAY_BETWEEN_DISTRICTS = 1.5  # seconds, be polite to NIC servers

# Tamil Nadu Assembly Election terms for mapping
ELECTION_TERMS = [
    {"term": "17th Assembly", "from": 2026, "to": None, "label": "2026–present"},
    {"term": "16th Assembly", "from": 2021, "to": 2026, "label": "2021–2026"},
    {"term": "15th Assembly", "from": 2016, "to": 2021, "label": "2016–2021"},
    {"term": "14th Assembly", "from": 2011, "to": 2016, "label": "2011–2016"},
    {"term": "13th Assembly", "from": 2006, "to": 2011, "label": "2006–2011"},
    {"term": "12th Assembly", "from": 2001, "to": 2006, "label": "2001–2006"},
    {"term": "11th Assembly", "from": 1996, "to": 2001, "label": "1996–2001"},
    {"term": "10th Assembly", "from": 1991, "to": 1996, "label": "1991–1996"},
    {"term": "9th Assembly", "from": 1989, "to": 1991, "label": "1989–1991"},
    {"term": "8th Assembly", "from": 1984, "to": 1989, "label": "1984–1989"},
    {"term": "7th Assembly", "from": 1980, "to": 1984, "label": "1980–1984"},
    {"term": "6th Assembly", "from": 1977, "to": 1980, "label": "1977–1980"},
    {"term": "5th Assembly", "from": 1971, "to": 1977, "label": "1971–1977"},
    {"term": "4th Assembly", "from": 1967, "to": 1971, "label": "1967–1971"},
    {"term": "3rd Assembly", "from": 1962, "to": 1967, "label": "1962–1967"},
    {"term": "2nd Assembly", "from": 1957, "to": 1962, "label": "1957–1962"},
    {"term": "1st Assembly", "from": 1952, "to": 1957, "label": "1952–1957"},
    {"term": "Pre-Assembly", "from": 1947, "to": 1952, "label": "1947–1952"},
]

# ---------------------------------------------------------------------------
# District URL Map — discovered via systematic crawling
# ---------------------------------------------------------------------------

DISTRICT_CONFIG = {
    "ariyalur": {
        "name": "Ariyalur",
        "path": "/about-district/administrative-setup/district-administration/",
        "notes": "merged_column_format",  # "Period: From To" in one column
    },
    "chengalpattu": {
        "name": "Chengalpattu",
        "path": "/list-of-collectors/",
    },
    "chennai": {
        "name": "Chennai",
        "path": "/list-of-collectors/",
    },
    "coimbatore": {
        "name": "Coimbatore",
        "path": None,  # No historical list page available
        "notes": "no_list_page",
    },
    "cuddalore": {
        "name": "Cuddalore",
        "path": None,
        "notes": "no_list_page",
    },
    "dharmapuri": {
        "name": "Dharmapuri",
        "path": "/roll-of-honour/",
    },
    "dindigul": {
        "name": "Dindigul",
        "path": "/list-of-collectors/",
    },
    "erode": {
        "name": "Erode",
        "path": "/roll-of-honour/",
    },
    "kallakurichi": {
        "name": "Kallakurichi",
        "path": "/about-district/administrative-setup/collectorate/",
    },
    "kancheepuram": {
        "name": "Kancheepuram",
        "path": "/list-of-collectors/",
    },
    "kanniyakumari": {
        "name": "Kanniyakumari",
        "path": "/listofcollectors/",
    },
    "karur": {
        "name": "Karur",
        "path": "/list-of-collectors/",
    },
    "krishnagiri": {
        "name": "Krishnagiri",
        "path": "/roll-of-honour/",
    },
    "madurai": {
        "name": "Madurai",
        "path": None,
        "notes": "no_list_page",
    },
    "mayiladuthurai": {
        "name": "Mayiladuthurai",
        "path": "/roll-of-honour/",
    },
    "nagapattinam": {
        "name": "Nagapattinam",
        "path": "/roll-of-honour/",
    },
    "namakkal": {
        "name": "Namakkal",
        "path": "/roll-of-honour/",
    },
    "nilgiris": {
        "name": "The Nilgiris",
        "path": "/collectors/",
        "subdomain": "nilgiris",
    },
    "perambalur": {
        "name": "Perambalur",
        "path": "/collectors/",
    },
    "pudukkottai": {
        "name": "Pudukkottai",
        "path": "/list-of-collectors/",
    },
    "ramanathapuram": {
        "name": "Ramanathapuram",
        "path": None,
        "notes": "no_list_page",
    },
    "ranipet": {
        "name": "Ranipet",
        "path": "/roll-of-honour/",
    },
    "salem": {
        "name": "Salem",
        "path": "/218523-2/",
    },
    "sivaganga": {
        "name": "Sivaganga",
        "path": "/roll-of-honour/",
    },
    "tenkasi": {
        "name": "Tenkasi",
        "path": "/list-of-collectors/",
    },
    "thanjavur": {
        "name": "Thanjavur",
        "path": "/district-collectors/",
    },
    "theni": {
        "name": "Theni",
        "path": "/roll_of_honour/",
    },
    "thoothukudi": {
        "name": "Thoothukudi",
        "path": "/list-of-collectors/",
    },
    "tiruchirappalli": {
        "name": "Tiruchirappalli",
        "path": "/roll-of-honour/",
    },
    "tirunelveli": {
        "name": "Tirunelveli",
        "path": "/list-of-collectors/",
    },
    "tirupathur": {
        "name": "Tirupathur",
        "path": None,
        "notes": "no_list_page",
    },
    "tiruppur": {
        "name": "Tiruppur",
        "path": "/list-of-collectors/",
    },
    "tiruvallur": {
        "name": "Tiruvallur",
        "path": "/about-district/tlrcollectors-list/",
    },
    "tiruvannamalai": {
        "name": "Tiruvannamalai",
        "path": "/list-of-collectors/",
    },
    "tiruvarur": {
        "name": "Tiruvarur",
        "path": "/list-of-collectors/",
    },
    "vellore": {
        "name": "Vellore",
        "path": "/list-of-collectors/",
    },
    "viluppuram": {
        "name": "Viluppuram",
        "path": "/collectrate/",
    },
    "virudhunagar": {
        "name": "Virudhunagar",
        "path": "/roll-of-honour/",
    },
}


# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------


def _slugify(value: str) -> str:
    """Convert a string to a URL-friendly slug."""
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]", "_", value.lower())).strip("_")


def _parse_date(date_str: str) -> str | None:
    """Parse various date formats into ISO format (YYYY-MM-DD).

    Handles:
      - DD/MM/YYYY, DD-MM-YYYY, DD.MM.YYYY
      - YYYY-MM-DD (already ISO)
      - "MON DD YYYY" (e.g. "FEB 08 2025")
      - Partial dates like "1947" or "Sep 1817"
    """
    if not date_str:
        return None

    date_str = date_str.strip()

    # Remove FN/AN suffixes (forenoon/afternoon) used in some TN sites
    date_str = re.sub(r"\s+(FN|AN|fn|an)\s+", " ", date_str).strip()
    date_str = re.sub(r"\s+(FN|AN|fn|an)$", "", date_str).strip()

    # Already ISO
    if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        return date_str

    # DD/MM/YYYY or DD-MM-YYYY or DD.MM.YYYY
    m = re.match(r"(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{4})", date_str)
    if m:
        day, month, year = m.groups()
        try:
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
        except ValueError:
            return None

    # MON DD YYYY or DD MON YYYY
    months_map = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
        "january": 1, "february": 2, "march": 3, "april": 4,
        "june": 6, "july": 7, "august": 8, "september": 9,
        "october": 10, "november": 11, "december": 12,
    }

    # "Feb 2025" or "February 2025"
    m = re.match(r"([a-zA-Z]+)\s+(\d{4})", date_str)
    if m:
        mon_str, year = m.groups()
        mon = months_map.get(mon_str.lower())
        if mon:
            return f"{int(year):04d}-{mon:02d}-01"

    # "12 Feb 2025" or "Feb 12 2025"
    m = re.match(r"(\d{1,2})\s+([a-zA-Z]+)\s+(\d{4})", date_str)
    if m:
        day, mon_str, year = m.groups()
        mon = months_map.get(mon_str.lower())
        if mon:
            return f"{int(year):04d}-{mon:02d}-{int(day):02d}"

    m = re.match(r"([a-zA-Z]+)\s+(\d{1,2})\s+(\d{4})", date_str)
    if m:
        mon_str, day, year = m.groups()
        mon = months_map.get(mon_str.lower())
        if mon:
            return f"{int(year):04d}-{mon:02d}-{int(day):02d}"

    # "Dec 1799" -> approximate
    m = re.match(r"([a-zA-Z]+)\s+(\d{4})", date_str)
    if m:
        mon_str, year = m.groups()
        mon = months_map.get(mon_str.lower())
        if mon:
            return f"{int(year):04d}-{mon:02d}-01"

    # Just a year "1947"
    m = re.match(r"^(\d{4})$", date_str)
    if m:
        return f"{m.group(1)}-01-01"

    return None


def _extract_year(date_iso: str | None) -> int | None:
    """Extract year from an ISO date string."""
    if not date_iso:
        return None
    try:
        return int(date_iso[:4])
    except (ValueError, IndexError):
        return None


def _clean_name(raw_name: str) -> str:
    """Clean collector name: remove honorifics, trim designations."""
    name = raw_name.strip()
    # Remove common prefixes
    name = re.sub(r"^(Thiru\.?|Tmt\.?|Dr\.?|Selvi\.?|Selvan\.?|Sri\.?|Smt\.?|Mr\.?|Mrs\.?|Ms\.?)\s*", "", name, flags=re.IGNORECASE)
    # Remove trailing IAS/ICS designations
    name = re.sub(r",?\s*(I\.?A\.?S\.?|I\.?C\.?S\.?|I\.?F\.?S\.?|I\.?P\.?S\.?).*$", "", name, flags=re.IGNORECASE)
    # Remove degree suffixes (require comma or space boundary before degree)
    name = re.sub(r",\s*(B\.?E\.?|M\.?B\.?A\.?|B\.?V\.?Sc\.?|M\.?Sc\.?|B\.?Com\.?|M\.?A\.?|Ph\.?D\.?).*$", "", name, flags=re.IGNORECASE)
    # Remove (I/C) or (IC) or (In-Charge)
    name = re.sub(r"\s*\(?(I/?C|In-?Charge|Acting)\)?", "", name, flags=re.IGNORECASE)
    # Remove DRO prefix
    name = re.sub(r",?\s*DRO\s*(\(I/?C\))?", "", name, flags=re.IGNORECASE)
    # Clean up whitespace
    name = re.sub(r"\s+", " ", name).strip()
    # Remove trailing punctuation
    name = name.rstrip(",. ")
    return name


def _extract_designation(raw_name: str) -> str:
    """Extract service designation (IAS/ICS/IFS) from raw name string."""
    m = re.search(r"(I\.?A\.?S\.?|I\.?C\.?S\.?|I\.?F\.?S\.?|I\.?P\.?S\.?)", raw_name, re.IGNORECASE)
    if m:
        return m.group(1).upper().replace(".", "")
    return "IAS"  # default assumption for post-independence collectors


def _is_current(to_date_raw: str) -> bool:
    """Check if the collector is currently serving."""
    if not to_date_raw:
        return True
    lower = to_date_raw.strip().lower()
    # Check for "still serving" indicators
    if lower in ["", "…", "...", "–", "-"]:
        return True
    return any(kw in lower for kw in ["present", "till date", "till now", "current", "continuing", "date"])


def _get_election_terms(from_date: str | None, to_date: str | None) -> list[str]:
    """Map a collector's tenure to overlapping election terms."""
    from_year = _extract_year(from_date)
    to_year = _extract_year(to_date)

    if not from_year:
        return []

    # If still serving, use current year
    if not to_year:
        to_year = 2026

    terms = []
    for term in ELECTION_TERMS:
        term_start = term["from"]
        term_end = term["to"] or 2031  # future-proof

        # Check overlap: collector served during this term (inclusive boundaries)
        if from_year <= term_end and to_year >= term_start:
            terms.append(term["label"])

    return terms


# ---------------------------------------------------------------------------
# Scraping Functions
# ---------------------------------------------------------------------------


def _fetch_page(url: str) -> BeautifulSoup | None:
    """Fetch a URL and return parsed BeautifulSoup, or None on failure."""
    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT, headers=HEADERS, verify=False)
        if r.status_code == 200:
            return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"  [WARN] Failed to fetch {url}: {e}")
    return None


def _parse_standard_table(table, district_slug: str, district_name: str) -> list[dict]:
    """Parse a standard table with columns: S.No, Name, From, To."""
    records = []
    rows = table.find_all("tr")

    if not rows:
        return records

    # Find the actual header row (might not be the first row due to notes/titles)
    # Also detect multi-row headers (e.g., row with "Sl.No, Name, Tenure" followed by "From, To")
    header_idx = 0
    data_start_idx = 1
    for i, row in enumerate(rows[:8]):
        cells = [c.get_text(strip=True).lower() for c in row.find_all(["th", "td"])]
        if len(cells) >= 3 and any("name" in c or "collector" in c for c in cells):
            header_idx = i
            # Check if next row is a sub-header (e.g., "From", "To")
            if i + 1 < len(rows):
                next_cells = [c.get_text(strip=True).lower() for c in rows[i + 1].find_all(["th", "td"])]
                if any("from" in c for c in next_cells):
                    data_start_idx = i + 2
                else:
                    data_start_idx = i + 1
            else:
                data_start_idx = i + 1
            break

    # Detect data format from actual data rows (not header)
    # Some tables have 4-col data despite 3-col header (e.g., "Tenure" splits into From/To)
    sample_row = None
    for row in rows[data_start_idx:data_start_idx + 3]:
        cells = row.find_all(["th", "td"])
        if len(cells) >= 3:
            sample_row = cells
            break

    headers_row = rows[header_idx]
    header_cells = [c.get_text(strip=True).lower() for c in headers_row.find_all(["th", "td"])]

    # If header says 3 cols (Sl, Name, Tenure) but data has 4 cols, infer From/To split
    sample_col_count = len(sample_row) if sample_row else len(header_cells)
    if len(header_cells) == 3 and sample_col_count == 4:
        # Data is: S.No, Name, From, To (header "Tenure" spans From+To)
        header_cells = ["sl", "name", "from", "to"]

    # Detect column indices
    name_col = None
    from_col = None
    to_col = None
    period_col = None  # For merged "Period: From To" format

    for i, h in enumerate(header_cells):
        if any(kw in h for kw in ["name", "collector"]):
            name_col = i
        elif ("from" in h) and ("to" not in h):
            from_col = i
        elif h.startswith("to") or h == "to date" or h == "tenue to" or (h.startswith("to") and "from" not in h):
            to_col = i
        elif "period" in h or "tenure" in h or ("from" in h and "to" in h):
            period_col = i
        elif "year" in h and name_col is not None and from_col is None:
            period_col = i

    # Fallback: if we have 4 columns, assume S.No, Name, From, To
    if name_col is None and len(header_cells) >= 3:
        if len(header_cells) == 4:
            name_col, from_col, to_col = 1, 2, 3
        elif len(header_cells) == 3:
            name_col, period_col = 1, 2
        elif len(header_cells) >= 5:
            # Some have extra columns (e.g., Photo, Designation)
            # Try to find from/to by checking if columns have date-like content
            name_col = 1
            for idx in range(2, len(header_cells)):
                h = header_cells[idx]
                if "from" in h:
                    from_col = idx
                elif "to" in h:
                    to_col = idx
            # If still not found, guess based on column count
            if from_col is None:
                if len(header_cells) == 6:
                    # Likely: SL, NAME, DESIGNATION, FROM, TO, PHOTO
                    from_col, to_col = 3, 4
                else:
                    from_col, to_col = 2, 3

    if name_col is None:
        return records

    for row in rows[data_start_idx:]:
        cells = row.find_all(["th", "td"])
        if len(cells) <= name_col:
            continue

        raw_name = cells[name_col].get_text(strip=True)
        if not raw_name or "merged" in raw_name.lower() or "district" in raw_name.lower() and "collector" not in raw_name.lower():
            continue

        from_raw = ""
        to_raw = ""

        if from_col is not None and from_col < len(cells):
            from_raw = cells[from_col].get_text(strip=True)
        if to_col is not None and to_col < len(cells):
            to_raw = cells[to_col].get_text(strip=True)

        # Handle merged period column (e.g., "01.01.2001 to 19.04.2002")
        if period_col is not None and period_col < len(cells):
            period_text = cells[period_col].get_text(strip=True)
            # Try splitting on " to " (case-insensitive) or " - " or "–"
            parts = re.split(r"\s+to\s+|\s*[-–]\s*", period_text, flags=re.IGNORECASE)
            if len(parts) >= 2:
                from_raw = parts[0].strip()
                to_raw = parts[1].strip()
            elif len(parts) == 1:
                from_raw = parts[0].strip()

        # Parse dates
        from_date = _parse_date(from_raw)
        is_current = _is_current(to_raw)
        to_date = None if is_current else _parse_date(to_raw)

        # Clean name
        clean = _clean_name(raw_name)
        if not clean or len(clean) < 3:
            continue

        # Skip rows where "name" looks like a date or non-person entry
        if re.match(r"^\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4}$", clean):
            continue
        if any(kw in clean.lower() for kw in ["section", "department", "meeting", "review"]):
            continue

        designation = _extract_designation(raw_name)
        name_slug = _slugify(clean)
        from_year = _extract_year(from_date) or 0

        doc_id = f"{district_slug}_{name_slug}_{from_year}"

        record = {
            "doc_id": doc_id,
            "name": clean,
            "name_raw": raw_name.strip(),
            "district": district_name,
            "district_slug": district_slug,
            "designation": designation,
            "from_date": from_date,
            "to_date": to_date,
            "is_current": is_current,
            "election_terms": _get_election_terms(from_date, to_date if not is_current else None),
            "ias_batch_year": None,
            "educational_qualification": None,
            "contact_email": f"collr{district_slug[:3]}@nic.in",
            "contact_phone": None,
            "photo_url": None,
            "source_url": None,  # filled by caller
            "ground_truth_confidence": "HIGH",
        }

        records.append(record)

    return records


def _scrape_district(district_slug: str, config: dict) -> list[dict]:
    """Scrape collector list for a single district."""
    district_name = config["name"]
    path = config.get("path")

    if not path:
        print(f"  [{district_name}] No list page available, skipping scrape")
        return []

    subdomain = config.get("subdomain", district_slug)
    url = f"https://{subdomain}.nic.in{path}"
    print(f"  [{district_name}] Fetching {url}")

    soup = _fetch_page(url)
    if not soup:
        return []

    # Find the relevant table
    tables = soup.find_all("table")
    if not tables:
        print(f"  [{district_name}] No tables found")
        return []

    # Pick the best table: prefer tables with collector-relevant headers
    def _table_score(t):
        """Score a table for likelihood of being a collectors list."""
        rows = t.find_all("tr")
        if not rows:
            return -1
        # Check all rows for header-like content
        text = " ".join(c.get_text(strip=True).lower() for r in rows[:5] for c in r.find_all(["th", "td"]))
        score = len(rows)
        # Boost tables with collector-relevant headers
        if any(kw in text for kw in ["collector", "from", "tenure", "i.a.s", "ias"]):
            score += 1000
        # Penalize tables with non-collector content
        if any(kw in text for kw in ["section head", "subject", "department", "charge officer"]):
            score -= 2000
        return score

    best_table = max(tables, key=_table_score)
    rows_count = len(best_table.find_all("tr"))

    if rows_count < 2:
        print(f"  [{district_name}] Table too small ({rows_count} rows)")
        return []

    records = _parse_standard_table(best_table, district_slug, district_name)

    # Set source URL
    for r in records:
        r["source_url"] = url

    print(f"  [{district_name}] Parsed {len(records)} collectors")
    return records


def _scrape_current_collector_photo(district_slug: str) -> str | None:
    """Try to fetch the current collector's photo from profile page."""
    subdomain = DISTRICT_CONFIG.get(district_slug, {}).get("subdomain", district_slug)

    profile_paths = [
        "/profile-of-collector/",
        "/collector-profile/",
        "/district-collector/",
    ]

    for path in profile_paths:
        url = f"https://{subdomain}.nic.in{path}"
        soup = _fetch_page(url)
        if not soup:
            continue

        # Look for images in the content
        content = soup.find("div", class_="entry-content") or soup.find("article") or soup
        images = content.find_all("img")
        for img in images:
            src = img.get("src", "")
            # Skip logos, icons, banners
            if any(kw in src.lower() for kw in ["logo", "icon", "banner", "flag", "emblem", "header"]):
                continue
            # Check if it looks like a portrait
            if src and ("collector" in src.lower() or "upload" in src.lower() or "cdn" in src.lower()):
                return src

    return None


# ---------------------------------------------------------------------------
# Firestore Upload
# ---------------------------------------------------------------------------


def _upload_to_firestore(records: list[dict], resume: bool = False) -> None:
    """Upload records to Firestore in batches."""
    if firestore is None:
        print("[ERROR] google-cloud-firestore not installed")
        sys.exit(1)

    db = firestore.Client(project="naatunadappu")
    total = len(records)
    uploaded = 0
    skipped = 0

    batch = db.batch()
    batch_count = 0

    for i, doc in enumerate(records, 1):
        doc_id = doc["doc_id"]
        ref = db.collection(COLLECTION).document(doc_id)

        if resume:
            existing = ref.get()
            if existing.exists:
                skipped += 1
                continue

        doc["_uploaded_at"] = NOW_ISO
        doc["_schema_version"] = "1.0"

        batch.set(ref, doc, merge=True)
        batch_count += 1

        if batch_count >= BATCH_SIZE:
            batch.commit()
            uploaded += batch_count
            print(f"  [{COLLECTION}] {uploaded}/{total} docs uploaded ({skipped} skipped)")
            batch = db.batch()
            batch_count = 0

    # Final batch
    if batch_count > 0:
        batch.commit()
        uploaded += batch_count

    print(f"  [{COLLECTION}] Done: {uploaded} uploaded, {skipped} skipped (total: {total})")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Scrape TN District Collectors data")
    parser.add_argument("--dry-run", action="store_true", help="Print records, no upload")
    parser.add_argument("--resume", action="store_true", help="Skip existing docs")
    parser.add_argument("--district", type=str, help="Scrape a single district (slug)")
    parser.add_argument("--limit", type=int, help="Limit total records")
    parser.add_argument("--with-photos", action="store_true", help="Also scrape current collector photos")
    parser.add_argument("--output", type=str, help="Save to JSON file instead of Firestore")
    args = parser.parse_args()

    print(f"=== District Collectors Ingest ===")
    print(f"Collection: {COLLECTION}")
    print(f"Mode: {'dry-run' if args.dry_run else 'upload'}")
    print()

    # Determine which districts to scrape
    if args.district:
        if args.district not in DISTRICT_CONFIG:
            print(f"[ERROR] Unknown district: {args.district}")
            print(f"Available: {', '.join(sorted(DISTRICT_CONFIG.keys()))}")
            sys.exit(1)
        districts_to_scrape = {args.district: DISTRICT_CONFIG[args.district]}
    else:
        districts_to_scrape = DISTRICT_CONFIG

    all_records: list[dict] = []

    for district_slug, config in districts_to_scrape.items():
        records = _scrape_district(district_slug, config)
        all_records.extend(records)

        if args.limit and len(all_records) >= args.limit:
            all_records = all_records[: args.limit]
            break

        # Be polite to the servers
        time.sleep(DELAY_BETWEEN_DISTRICTS)

    print(f"\nTotal records scraped: {len(all_records)}")

    # Optionally scrape photos for current collectors
    if args.with_photos:
        print("\n--- Scraping current collector photos ---")
        current_collectors = [r for r in all_records if r["is_current"]]
        for r in current_collectors:
            photo = _scrape_current_collector_photo(r["district_slug"])
            if photo:
                r["photo_url"] = photo
                print(f"  [{r['district']}] Photo found: {photo[:60]}...")
            time.sleep(1)

    # Output
    if args.output:
        output_path = Path(args.output)
        output_path.write_text(json.dumps(all_records, indent=2, ensure_ascii=False))
        print(f"\nSaved to {output_path} ({len(all_records)} records)")
    elif args.dry_run:
        print("\n--- Sample Records (first 5) ---")
        for r in all_records[:5]:
            print(json.dumps(r, indent=2, ensure_ascii=False))
        print(f"\n... and {len(all_records) - 5} more records")
    else:
        print("\n--- Uploading to Firestore ---")
        _upload_to_firestore(all_records, resume=args.resume)

    print("\nDone!")


if __name__ == "__main__":
    # Suppress InsecureRequestWarning for verify=False
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    main()
