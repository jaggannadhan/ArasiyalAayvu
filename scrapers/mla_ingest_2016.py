"""
MLA Ingest — Tamil Nadu 2016 Assembly Election
===============================================
Scrapes MyNeta TN 2016 winners and writes `candidate_accountability` Firestore
docs with doc IDs `2016_{constituency_slug}` — same schema as the 2021 docs.

Fields scraped from winners list:
  mla_name, constituency, party, criminal_cases_total, education, assets_cr,
  liabilities_cr

Fields scraped from each winner's detail page:
  movable_assets_cr, immovable_assets_cr, institution_name, source_pdf

Computed fields (same logic as accountability_transformer.py):
  criminal_severity, is_crorepati, net_assets_cr, education_tier

Usage
-----
  # Dry-run: scrape + print, no Firestore write
  .venv/bin/python scrapers/mla_ingest_2016.py --dry-run

  # Dry-run with detail pages (first 5)
  .venv/bin/python scrapers/mla_ingest_2016.py --dry-run --limit 5 --with-detail

  # Full run (all 235 winners, ~8 min at 1 req/s per detail page)
  .venv/bin/python scrapers/mla_ingest_2016.py

  # Resume: skip docs that already exist in Firestore
  .venv/bin/python scrapers/mla_ingest_2016.py --resume
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from google.cloud import firestore
except ImportError:
    firestore = None  # type: ignore[assignment]

# ─────────────────────────────────────────────────────────────────────────────
PROJECT_ID    = "naatunadappu"
COLLECTION    = "candidate_accountability"
BASE_URL      = "https://myneta.info/tamilnadu2016"
WINNERS_PATH  = "/index.php?action=show_winners&sort=default"
HEADERS       = {"User-Agent": "Mozilla/5.0 (compatible; NaatuNadappuResearchBot/2.0)"}
SLEEP_BETWEEN = 1.2   # seconds between detail page fetches
ELECTION_YEAR = 2016
NOW_ISO       = datetime.now(timezone.utc).isoformat()
# ─────────────────────────────────────────────────────────────────────────────


def _get(url: str, retries: int = 3) -> requests.Response:
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            if attempt == retries - 1:
                raise
            print(f"  [retry {attempt+1}] {exc}")
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed after {retries} attempts: {url}")


# ─────────────────────────────────────────────────────────────────────────────
# Field parsers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_cr(raw: str) -> Optional[float]:
    """Convert 'Rs 1,70,97,31,016' or '170 Crore+' to crore float."""
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
    unit = re.search(r"([\d]+(?:\.\d+)?)\s*(crore|crores|cr|lakh|lakhs|lac|lacs)", compact, re.IGNORECASE)
    if unit:
        n = float(unit.group(1))
        u = unit.group(2).lower()
        return round(n if u in {"crore", "crores", "cr"} else n / 100, 4)
    return None


def _clean_cases(raw: str) -> int:
    try:
        return int(raw.strip())
    except (ValueError, AttributeError):
        return 0


def _slugify(value: str) -> str:
    return re.sub(r"_+", "_",
        re.sub(r"[^a-z0-9]", "_", value.lower())
    ).strip("_")


def _education_tier(raw: str) -> str:
    edu = (raw or "").lower()
    if any(t in edu for t in ["phd", "doctorate", "d.litt"]):
        return "Doctorate"
    if any(t in edu for t in ["post graduate", "pg", "mba", "m.a", "m.sc", "m.com", "m.e", "m.tech", "llm"]):
        return "Postgraduate"
    if any(t in edu for t in ["graduate", "b.a", "b.sc", "b.com", "b.e", "b.tech", "mbbs", "llb", "b.ed", "diploma"]):
        return "Graduate"
    if any(t in edu for t in ["12th", "hsc", "intermediate"]):
        return "Class XII"
    if any(t in edu for t in ["10th", "sslc", "matriculat"]):
        return "Class X"
    if any(t in edu for t in ["8th", "primary", "5th", "illiterate", "literate"]):
        return "Below Class X"
    return "Not Disclosed"


def _severity(cases: int) -> str:
    if cases == 0:
        return "CLEAN"
    if cases <= 2:
        return "MINOR"
    if cases <= 5:
        return "MODERATE"
    return "SERIOUS"


# ─────────────────────────────────────────────────────────────────────────────
# Scrapers
# ─────────────────────────────────────────────────────────────────────────────

def scrape_winners_list() -> List[Dict[str, Any]]:
    """Scrape winners table → list of base winner dicts."""
    url = BASE_URL + WINNERS_PATH
    print(f"Fetching winners list: {url}")
    soup = BeautifulSoup(_get(url).text, "lxml")

    winners = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 10:
            continue
        headers = [td.get_text(strip=True).lower() for td in rows[0].find_all(["th", "td"])]
        if "candidate" not in " ".join(headers) and "constituency" not in " ".join(headers):
            continue

        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 5:
                continue
            link = cells[1].find("a", href=True)
            raw_name         = re.sub(r"\s+", " ", cells[1].get_text(" ", strip=True))
            raw_constituency = re.sub(r"\s+", " ", cells[2].get_text(" ", strip=True))
            if not raw_name or not raw_constituency:
                continue

            detail_url = BASE_URL + "/" + (link["href"].lstrip("/") if link else "")
            slug       = _slugify(raw_constituency)
            doc_id     = f"{ELECTION_YEAR}_{slug}"

            try:
                winners.append({
                    "doc_id":              doc_id,
                    "election_year":       ELECTION_YEAR,
                    "mla_name":            raw_name,
                    "constituency":        raw_constituency,
                    "constituency_slug":   slug,
                    "party":               re.sub(r"\s+", " ", cells[3].get_text(strip=True)) if len(cells) > 3 else "",
                    "criminal_cases_total": _clean_cases(cells[4].get_text(strip=True)) if len(cells) > 4 else 0,
                    "education":           re.sub(r"\s+", " ", cells[5].get_text(strip=True)) if len(cells) > 5 else "",
                    "assets_cr":           _parse_cr(cells[6].get_text(strip=True)) if len(cells) > 6 else None,
                    "liabilities_cr":      _parse_cr(cells[7].get_text(strip=True)) if len(cells) > 7 else None,
                    "_detail_url":         detail_url,
                })
            except (IndexError, ValueError):
                continue

        if winners:
            break

    print(f"Found {len(winners)} winners")
    return winners


def scrape_detail(detail_url: str) -> Dict[str, Any]:
    """Scrape detail page → movable/immovable assets, institution, source_pdf."""
    result: Dict[str, Any] = {
        "movable_assets_cr":   None,
        "immovable_assets_cr": None,
        "institution_name":    None,
        "source_pdf":          None,
    }
    try:
        soup = BeautifulSoup(_get(detail_url).text, "lxml")
    except Exception as exc:
        print(f"  [warn] detail fetch failed {detail_url}: {exc}")
        return result

    for row in soup.select("table tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        label = cells[0].get_text(" ", strip=True).lower()
        value = cells[1].get_text(" ", strip=True)
        # 2021-style labels
        if "movable" in label and "immovable" not in label:
            result["movable_assets_cr"] = _parse_cr(value)
        elif "immovable" in label:
            result["immovable_assets_cr"] = _parse_cr(value)
        # 2016-style labels: movable = "gross total value", immovable = "total current market value of (i) to (v)"
        elif "gross total value" in label:
            result["movable_assets_cr"] = _parse_cr(value)
        elif "total current market value" in label:
            result["immovable_assets_cr"] = _parse_cr(value)
        elif "education" in label:
            inst = re.search(r"[-–—]\s*(.{5,})", value)
            if inst:
                result["institution_name"] = inst.group(1).strip()[:120]

    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True).lower()
        if "affidavit" in text or "form 26" in text or href.endswith(".pdf"):
            if href.startswith("http"):
                result["source_pdf"] = href
            elif href.startswith("/"):
                result["source_pdf"] = "https://myneta.info" + href
            else:
                result["source_pdf"] = BASE_URL + "/" + href.lstrip("/")
            break

    return result


def enrich(winner: Dict[str, Any]) -> Dict[str, Any]:
    """Add computed fields to a base winner record."""
    cases      = winner.get("criminal_cases_total", 0)
    assets     = winner.get("assets_cr")
    liabs      = winner.get("liabilities_cr")
    education  = winner.get("education", "")

    winner["criminal_severity"] = _severity(cases)
    winner["education_tier"]    = _education_tier(education)
    winner["is_crorepati"]      = (assets or 0) >= 1.0
    winner["net_assets_cr"]     = round(assets - (liabs or 0), 4) if assets is not None else None
    winner["ground_truth_confidence"] = "HIGH"
    winner["source_url"]        = BASE_URL + WINNERS_PATH
    return winner


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def run(dry_run: bool, limit: Optional[int], resume: bool, with_detail: bool) -> None:
    winners = scrape_winners_list()
    if limit:
        winners = winners[:limit]

    db = None
    already_done: set = set()
    if not dry_run:
        if firestore is None:
            print("ERROR: google-cloud-firestore not installed")
            sys.exit(1)
        db = firestore.Client(project=PROJECT_ID)
        if resume:
            existing = list(db.collection(COLLECTION)
                            .where("election_year", "==", ELECTION_YEAR)
                            .stream())
            already_done = {d.id for d in existing}
            print(f"[resume] {len(already_done)} 2016 docs already exist — skipping")

    written = skipped = 0

    for i, w in enumerate(winners):
        doc_id = w["doc_id"]
        if doc_id in already_done:
            skipped += 1
            continue

        print(f"[{i+1}/{len(winners)}] {w['constituency']} ({doc_id})")

        # Enrich with computed fields
        enrich(w)

        # Optionally scrape detail page
        if with_detail or not dry_run:
            detail = scrape_detail(w.pop("_detail_url"))
            w.update(detail)
            time.sleep(SLEEP_BETWEEN)
        else:
            w.pop("_detail_url", None)

        # Remove internal helper field
        w.pop("_detail_url", None)

        if dry_run:
            print(f"  party={w['party']} cases={w['criminal_cases_total']} "
                  f"assets={w['assets_cr']} sev={w['criminal_severity']} "
                  f"movable={w.get('movable_assets_cr')} pdf={w.get('source_pdf')}")
        else:
            doc = {**w, "_uploaded_at": NOW_ISO, "_schema_version": "1.0"}
            db.collection(COLLECTION).document(doc_id).set(doc, merge=True)  # type: ignore[union-attr]
            written += 1

    if dry_run:
        print(f"\n[dry-run] Would write {len(winners)} docs to {COLLECTION}")
    else:
        print(f"\nDone — written={written} skipped={skipped}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest TN 2016 MLA winners into candidate_accountability")
    ap.add_argument("--dry-run",     action="store_true", help="Print without writing to Firestore")
    ap.add_argument("--limit",       type=int, default=None, help="Only process first N winners")
    ap.add_argument("--resume",      action="store_true", help="Skip docs that already exist")
    ap.add_argument("--with-detail", action="store_true", help="Also scrape detail pages in dry-run mode")
    args = ap.parse_args()
    run(dry_run=args.dry_run, limit=args.limit, resume=args.resume, with_detail=args.with_detail)


if __name__ == "__main__":
    main()
