"""
CEO TN MLA Ingest — Tamil Nadu Assembly Elections (2011, 2016, 2021)
=====================================================================
Uses the official Chief Electoral Officer (CEO) Tamil Nadu website as the
authoritative source for elected MLA names and party affiliations.

Source: https://elections.tn.gov.in/Archives.aspx
  - TNLA2021_MLA.aspx  (234 constituencies)
  - TNLA2016_MLA.aspx  (234 constituencies)
  - TNLA2011_MLA.aspx  (234 constituencies)

For each constituency:
1. Fetches name + party from CEO TN (authoritative)
2. Optionally enriches with criminal cases, education, assets from MyNeta
   constituency page (--with-myneta flag) — uses the MLA name from CEO TN
   to identify the correct row even when MyNeta hasn't flagged a winner
3. Writes/patches `candidate_accountability` Firestore docs for missing entries
   (skips docs that already exist unless --force)

Usage
-----
  # Dry-run 2021 (see what would be written)
  .venv/bin/python scrapers/ceo_tn_mla_ingest.py --year 2021 --dry-run

  # Fill missing 2021 docs with CEO TN data + MyNeta stats
  .venv/bin/python scrapers/ceo_tn_mla_ingest.py --year 2021 --with-myneta

  # Skip existing docs (default safe mode — only writes missing)
  .venv/bin/python scrapers/ceo_tn_mla_ingest.py --year 2016

  # Force-overwrite ALL docs (use carefully)
  .venv/bin/python scrapers/ceo_tn_mla_ingest.py --year 2016 --force
"""

from __future__ import annotations

import argparse
import re
import ssl
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from google.cloud import firestore
except ImportError:
    firestore = None  # type: ignore[assignment]

# ─────────────────────────────────────────────────────────────────────────────
COLLECTION   = "candidate_accountability"
PROJECT_ID   = "naatunadappu"
NOW_ISO      = datetime.now(timezone.utc).isoformat()

MAP_PATH     = ROOT / "web" / "src" / "lib" / "constituency-map.json"
CEO_TN_BASE  = "https://elections.tn.gov.in"
CEO_TN_PAGES = {
    2011: "TNLA2011_MLA.aspx",
    2016: "TNLA2016_MLA.aspx",
    2021: "TNLA2021_MLA.aspx",
}

MYNETA_BASE  = {
    2011: "https://myneta.info/tamilnadu2011",
    2016: "https://myneta.info/tamilnadu2016",
    2021: "https://myneta.info/tamilnadu2021",
}

CEO_HEADERS  = {"User-Agent": "Mozilla/5.0 (compatible; NaatuNadappuResearchBot/1.0)"}
MN_HEADERS   = {"User-Agent": "Mozilla/5.0 (compatible; NaatuNadappuResearchBot/2.0)"}
SLEEP        = 1.0

# Party full-name → short abbreviation (normalise CEO TN names to match MyNeta style)
PARTY_ABBREV: Dict[str, str] = {
    "Dravida Munnetra Kazhagam":                                "DMK",
    "All India Anna Dravida Munnetra Kazhagam":                 "AIADMK",
    "Indian National Congress":                                 "INC",
    "Bharatiya Janata Party":                                   "BJP",
    "Communist Party of India":                                 "CPI",
    "Communist Party of India (Marxist)":                       "CPI(M)",
    "Pattali Makkal Katchi":                                    "PMK",
    "Viduthalai Chiruthaigal Katchi":                           "VCK",
    "Naam Tamilar Katchi":                                      "NTK",
    "Marumalarchi Dravida Munnetra Kazhagam":                   "MDMK",
    "Desiya Murpokku Dravida Kazhagam":                         "DMDK",
    "Tamil Maanila Congress (Moopanar)":                        "TMC(M)",
    "Indian Union Muslim League":                               "IUML",
    "All India Forward Bloc":                                   "AIFB",
    "Revolutionary Socialist Party":                            "RSP",
    "Kongu Nadu Munnetra Kazhagam":                             "KNMK",
    "Independent":                                              "IND",
}
# ─────────────────────────────────────────────────────────────────────────────


def _load_slug_lookup() -> Dict[str, str]:
    """
    Build reverse lookup: normalised_constituency_name → map_slug.
    Handles '(SC)'/'(ST)' variations: both 'Ponneri' and 'Ponneri (SC)' map to 'ponneri_sc'.
    """
    import json as _json
    cmap = _json.loads(MAP_PATH.read_text(encoding="utf-8"))
    lookup: Dict[str, str] = {}
    for slug, meta in cmap.items():
        name = meta.get("name", "")
        if not name:
            continue
        # Normalise: uppercase, collapse spaces
        key = re.sub(r"\s+", " ", name.upper().strip())
        lookup[key] = slug
        # Also add version without SC/ST suffix (CEO TN 2021 sometimes omits it)
        base_key = re.sub(r"\s*\([Ss][CTct]\)\s*$", "", key).strip()
        if base_key != key:
            lookup.setdefault(base_key, slug)
    return lookup


# Global slug lookup (populated on first use)
_SLUG_LOOKUP: Dict[str, str] = {}


def _map_slug(raw_name: str) -> Optional[str]:
    """
    Return the canonical constituency-map.json slug for a CEO TN constituency name.
    Falls back to slugify() if not found.
    """
    global _SLUG_LOOKUP
    if not _SLUG_LOOKUP:
        _SLUG_LOOKUP = _load_slug_lookup()
    key = re.sub(r"\s+", " ", raw_name.upper().strip())
    # Try exact match first, then with (SC) stripped
    result = _SLUG_LOOKUP.get(key)
    if result:
        return result
    base_key = re.sub(r"\s*\([Ss][CTct]\)\s*$", "", key).strip()
    return _SLUG_LOOKUP.get(base_key)


def _make_session() -> requests.Session:
    session = requests.Session()
    session.verify = False
    return session


_SESSION = _make_session()


def _get(url: str, headers: Dict[str, str], retries: int = 3) -> requests.Response:
    for attempt in range(retries):
        try:
            resp = _SESSION.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            if attempt == retries - 1:
                raise
            print(f"  [retry {attempt+1}] {exc}")
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed after {retries} attempts: {url}")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _slugify(value: str) -> str:
    return re.sub(r"_+", "_",
        re.sub(r"[^a-z0-9]", "_", value.lower())
    ).strip("_")


def _constituency_slug(raw_name: str) -> str:
    """
    Convert a CEO TN constituency name to our slug.
    Handles '(SC)' and '(ST)' suffixes.
    E.g. 'Ponneri (SC)' → 'ponneri_sc'
         'Thiruvidaimarudur (SC)' → 'thiruvidaimarudur_sc'
    """
    sc_st = re.search(r"\(([Ss][CTct])\)\s*$", raw_name)
    clean = re.sub(r"\s*\([Ss][CTct]\)\s*$", "", raw_name).strip()
    slug = _slugify(clean)
    if sc_st:
        slug = slug + "_" + sc_st.group(1).lower()
    return slug


def _normalise_party(full_name: str) -> str:
    return PARTY_ABBREV.get(full_name.strip(), full_name.strip())


def _severity(cases: Optional[int]) -> str:
    if cases is None: return "UNKNOWN"
    if cases == 0:    return "CLEAN"
    if cases <= 2:    return "MINOR"
    if cases <= 5:    return "MODERATE"
    return "SERIOUS"


def _education_tier(raw: str) -> str:
    edu = (raw or "").lower()
    if any(t in edu for t in ["phd", "doctorate", "d.litt"]):           return "Doctorate"
    if any(t in edu for t in ["post graduate", "pg", "mba", "m.a", "m.sc",
                               "m.com", "m.e", "m.tech", "llm"]):        return "Postgraduate"
    if any(t in edu for t in ["graduate", "b.a", "b.sc", "b.com", "b.e",
                               "b.tech", "mbbs", "llb", "b.ed",
                               "diploma", "professional"]):               return "Graduate"
    if any(t in edu for t in ["12th", "hsc", "intermediate"]):           return "Class XII"
    if any(t in edu for t in ["10th", "sslc", "matriculat"]):            return "Class X"
    if any(t in edu for t in ["8th", "primary", "5th",
                               "illiterate", "literate"]):                return "Below Class X"
    return "Not Disclosed"


def _parse_cr(raw: str) -> Optional[float]:
    if not raw:
        return None
    text = re.sub(r"\s+", " ", raw).strip().lower()
    if any(k in text for k in ["nil", "n/a", "none", "not given"]):
        return 0.0
    rs = re.search(r"rs\.?\s*([\d,]+(?:\.\d+)?)", text, re.IGNORECASE)
    if rs:
        try:
            return round(float(rs.group(1).replace(",", "")) / 10_000_000, 4)
        except ValueError:
            pass
    compact = text.replace(",", "")
    unit = re.search(
        r"([\d]+(?:\.\d+)?)\s*(crore|crores|cr|lakh|lakhs|lac|lacs)",
        compact, re.IGNORECASE
    )
    if unit:
        n = float(unit.group(1))
        u = unit.group(2).lower()
        return round(n if u in {"crore", "crores", "cr"} else n / 100, 4)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# CEO TN scraper
# ─────────────────────────────────────────────────────────────────────────────

def scrape_ceo_tn(year: int) -> List[Dict[str, Any]]:
    """Fetch the official elected MLA list from CEO Tamil Nadu."""
    path = CEO_TN_PAGES[year]
    url  = f"{CEO_TN_BASE}/{path}"
    print(f"[CEO TN] {year}: {url}")
    soup = BeautifulSoup(_get(url, CEO_HEADERS).text, "lxml")

    records: List[Dict[str, Any]] = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 10:
            continue
        hdrs = [td.get_text(strip=True).lower() for td in rows[0].find_all(["th", "td"])]
        if not any("name" in h or "constituency" in h for h in hdrs):
            continue

        for row in rows[1:]:
            cells = [td.get_text(" ", strip=True) for td in row.find_all("td")]
            if not cells or not any(c.strip() for c in cells):
                continue

            raw_full  = re.sub(r"\s+", " ", cells[0]).strip()
            raw_const = re.sub(r"^\d+\.\s*", "", raw_full).strip()
            mla_name  = re.sub(r"\s+", " ", cells[1]).strip() if len(cells) > 1 else ""
            party_raw = re.sub(r"\s+", " ", cells[2]).strip() if len(cells) > 2 else ""

            if not raw_const or not mla_name:
                continue

            # Use map-aware slug lookup; fall back to slugify if name not in map
            slug = _map_slug(raw_const) or _constituency_slug(raw_const)
            records.append({
                "constituency":      raw_const,
                "constituency_slug": slug,
                "mla_name":          mla_name,
                "party":             _normalise_party(party_raw),
                "party_full":        party_raw,
            })

        if records:
            break

    print(f"  → {len(records)} records")
    return records


# ─────────────────────────────────────────────────────────────────────────────
# MyNeta enrichment
# ─────────────────────────────────────────────────────────────────────────────

def _build_myneta_id_map(year: int) -> Dict[str, int]:
    """
    Scan MyNeta constituency IDs 1–239 for the given year and build
    a map: normalised_constituency_name → constituency_id.
    """
    base = MYNETA_BASE[year]
    id_map: Dict[str, int] = {}
    print(f"  [MyNeta] building constituency ID map for {year}…")
    for cid in range(1, 240):
        try:
            r = _get(
                f"{base}/index.php?action=show_candidates&constituency_id={cid}",
                MN_HEADERS
            )
        except Exception:
            continue
        soup = BeautifulSoup(r.text, "lxml")
        title = soup.find("title")
        if not title:
            continue
        t = title.get_text(strip=True)
        m = re.match(r"List of Candidates in (.+?) : (.+?) Tamil Nadu \d{4}", t)
        if not m:
            continue
        constituency_name = m.group(1).strip()
        district_part     = m.group(2).strip()
        if "BYE" in district_part.upper() or "TOBEDELETED" in constituency_name.upper():
            continue
        key = _slugify(constituency_name)
        id_map[key] = cid
        time.sleep(0.3)

    print(f"  [MyNeta] mapped {len(id_map)} constituencies")
    return id_map


def _enrich_from_myneta(
    year: int,
    constituency_slug: str,
    mla_name: str,
    myneta_id_map: Dict[str, int],
) -> Dict[str, Any]:
    """
    Scrape the MyNeta constituency page and find the candidate matching mla_name.
    Returns a dict with criminal_cases_total, education, assets_cr, liabilities_cr
    (all None if not found).
    """
    empty: Dict[str, Any] = {
        "criminal_cases_total": None,
        "education":            None,
        "assets_cr":            None,
        "liabilities_cr":       None,
    }

    # Try slug-based lookup; also try stripping _sc/_st
    base_slug = re.sub(r"_(sc|st)$", "", constituency_slug)
    cid = myneta_id_map.get(constituency_slug) or myneta_id_map.get(base_slug)
    if not cid:
        return empty

    base = MYNETA_BASE[year]
    try:
        r = _get(f"{base}/index.php?action=show_candidates&constituency_id={cid}", MN_HEADERS)
    except Exception as exc:
        print(f"    [warn] MyNeta fetch failed cid={cid}: {exc}")
        return empty

    soup = BeautifulSoup(r.text, "lxml")
    # Normalise mla_name for fuzzy matching
    name_norm = re.sub(r"[^a-z]", "", mla_name.lower())

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 3:
            continue
        header = " ".join(td.get_text(strip=True).lower() for td in rows[0].find_all(["th", "td"]))
        if "candidate" not in header or "party" not in header:
            continue

        best_row: Optional[Any] = None
        best_score = 0.0
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 5:
                continue
            cell_name = re.sub(r"\s*winner\s*", "", cells[1].get_text(" ", strip=True), flags=re.IGNORECASE)
            cell_norm = re.sub(r"[^a-z]", "", cell_name.lower())
            # Simple overlap score
            shorter, longer = sorted([name_norm, cell_norm], key=len)
            if not longer:
                continue
            score = sum(1 for c in shorter if c in longer) / len(longer)
            if score > best_score:
                best_score  = score
                best_row    = cells

        if best_row is not None and best_score >= 0.55:
            try:
                cases = int(best_row[3].get_text(strip=True))
            except (ValueError, IndexError):
                cases = 0
            edu       = best_row[4].get_text(strip=True) if len(best_row) > 4 else ""
            assets_r  = best_row[6].get_text(strip=True) if len(best_row) > 6 else ""
            liabs_r   = best_row[7].get_text(strip=True) if len(best_row) > 7 else ""
            return {
                "criminal_cases_total": cases,
                "education":            edu,
                "assets_cr":            _parse_cr(assets_r),
                "liabilities_cr":       _parse_cr(liabs_r),
            }
        break  # only care about the main candidates table

    return empty


# ─────────────────────────────────────────────────────────────────────────────
# Main run
# ─────────────────────────────────────────────────────────────────────────────

def run(
    year: int,
    dry_run: bool,
    force: bool,
    with_myneta: bool,
    limit: Optional[int],
) -> None:
    records = scrape_ceo_tn(year)
    if limit:
        records = records[:limit]

    db = None
    existing_ids: set = set()

    if not dry_run:
        if firestore is None:
            print("ERROR: google-cloud-firestore not installed")
            sys.exit(1)
        db = firestore.Client(project=PROJECT_ID)
        if not force:
            existing = list(
                db.collection(COLLECTION)
                  .where("election_year", "==", year)
                  .stream()
            )
            existing_ids = {d.id for d in existing}
            print(f"[resume] {len(existing_ids)} existing {year} docs in Firestore")

    # Build MyNeta ID map only if enrichment requested and we have missing docs
    myneta_id_map: Dict[str, int] = {}
    if with_myneta and not dry_run:
        # Count how many we'll actually need to enrich
        to_write = [r for r in records if f"{year}_{r['constituency_slug']}" not in existing_ids]
        if to_write:
            myneta_id_map = _build_myneta_id_map(year)

    written = skipped = 0
    source_url = f"{CEO_TN_BASE}/{CEO_TN_PAGES[year]}"

    for i, rec in enumerate(records):
        slug   = rec["constituency_slug"]
        doc_id = f"{year}_{slug}"

        # Skip if exact doc exists
        if doc_id in existing_ids:
            skipped += 1
            continue

        # Also skip if there's already a doc for the SAME constituency under a close slug
        # (handles SC/ST suffix differences and transliteration variants)
        if existing_ids and not force:
            base = re.sub(r"_(sc|st)$", "", slug)
            shadowed = any(
                eid == f"{year}_{base}" or eid == f"{year}_{base}_sc" or eid == f"{year}_{base}_st"
                for eid in existing_ids
            )
            if shadowed:
                skipped += 1
                continue

        print(f"[{i+1}/{len(records)}] {rec['constituency']} ({doc_id})")

        # Build base document from CEO TN
        cases    = None
        edu      = None
        assets   = None
        liabs    = None

        # Optionally enrich from MyNeta
        if with_myneta and myneta_id_map:
            enriched = _enrich_from_myneta(year, slug, rec["mla_name"], myneta_id_map)
            cases    = enriched["criminal_cases_total"]
            edu      = enriched["education"]
            assets   = enriched["assets_cr"]
            liabs    = enriched["liabilities_cr"]
            time.sleep(SLEEP)

        # Only derive severity/tier when we actually have the data
        criminal_known = cases is not None
        doc: Dict[str, Any] = {
            "doc_id":               doc_id,
            "election_year":        year,
            "constituency":         rec["constituency"],
            "constituency_slug":    slug,
            "mla_name":             rec["mla_name"],
            "party":                rec["party"],
            "criminal_cases_total": cases,          # None = data not available
            "education":            edu or "",
            "assets_cr":            assets,
            "liabilities_cr":       liabs,
            "net_assets_cr":        round(assets - (liabs or 0), 4) if assets is not None else None,
            "is_crorepati":         (assets or 0) >= 1.0,
            "criminal_severity":    _severity(cases) if criminal_known else None,
            "education_tier":       _education_tier(edu or ""),
            "ground_truth_confidence": "HIGH",
            "source_url":           source_url,
            "_data_source":         "ceo_tn",
            "_uploaded_at":         NOW_ISO,
            "_schema_version":      "1.0",
        }

        if dry_run:
            print(f"  mla={doc['mla_name']} party={doc['party']} "
                  f"cases={doc['criminal_cases_total']} assets={doc['assets_cr']} "
                  f"sev={doc['criminal_severity']}")
        else:
            db.collection(COLLECTION).document(doc_id).set(doc, merge=True)  # type: ignore[union-attr]
            written += 1

    if dry_run:
        missing = len([r for r in records if f"{year}_{r['constituency_slug']}" not in existing_ids])
        print(f"\n[dry-run] Would write {missing} docs (skipping {len(records)-missing} existing)")
    else:
        print(f"\nDone — written={written} skipped={skipped}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Ingest missing TN MLA winners from CEO TN official website"
    )
    ap.add_argument("--year",         type=int, required=True, choices=[2011, 2016, 2021])
    ap.add_argument("--dry-run",      action="store_true",
                    help="Print what would be written without touching Firestore")
    ap.add_argument("--force",        action="store_true",
                    help="Overwrite existing docs (default: skip)")
    ap.add_argument("--with-myneta",  action="store_true",
                    help="Enrich with criminal/financial data from MyNeta constituency pages")
    ap.add_argument("--limit",        type=int, default=None,
                    help="Process only first N constituencies (for testing)")
    args = ap.parse_args()
    run(
        year=args.year,
        dry_run=args.dry_run,
        force=args.force,
        with_myneta=args.with_myneta,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
