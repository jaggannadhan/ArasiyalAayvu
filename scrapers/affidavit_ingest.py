"""
B.5 Affidavit Enrichment — scrape MyNeta winner detail pages and patch
`candidate_accountability` documents with:

  movable_assets_cr     — movable assets in crore (float | null)
  immovable_assets_cr   — immovable assets in crore (float | null)
  institution_name      — educational institution listed in affidavit (str | null)
  source_pdf            — direct URL to ECI Form 26 PDF scan (str | null)

Existing fields (criminal_cases_total, assets_cr, liabilities_cr, education_tier)
are NOT overwritten — this script only adds the new fields via merge=True.

Usage
-----
  # Dry-run: scrape 5 winners and print, no Firestore write
  .venv/bin/python scrapers/affidavit_ingest.py --dry-run --limit 5

  # Full run (all 224 winners, ~8 min at 1 req/s)
  .venv/bin/python scrapers/affidavit_ingest.py

  # Resume: skip winners that already have source_pdf set in Firestore
  .venv/bin/python scrapers/affidavit_ingest.py --resume
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional, List

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from google.cloud import firestore
except ImportError:
    firestore = None  # type: ignore[assignment]

# ─────────────────────────────────────────────────────────────────────────────
PROJECT_ID     = "naatunadappu"
COLLECTION     = "candidate_accountability"
BASE_URL       = "https://myneta.info/tamilnadu2021"
WINNERS_PATH   = "/index.php?action=show_winners&sort=default"
HEADERS        = {"User-Agent": "Mozilla/5.0 (compatible; NaatuNadappuResearchBot/2.0)"}
SLEEP_BETWEEN  = 1.2   # seconds between detail page fetches
BATCH_SIZE     = 400
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
    raise RuntimeError(f"Failed to fetch {url}")


def _inr_to_cr(inr_str: str) -> Optional[float]:
    """Convert 'Rs 1,34,59,578' or '1 Crore+' to float crore value."""
    if not inr_str:
        return None
    text = re.sub(r"\s+", " ", inr_str).strip().lower()
    if any(k in text for k in ["nil", "n/a", "none", "not given", "0"]):
        return 0.0

    # Prefer explicit Rs figure
    rs_match = re.search(r"rs\.?\s*([\d,]+(?:\.\d+)?)", text, re.IGNORECASE)
    if rs_match:
        try:
            inr = float(rs_match.group(1).replace(",", ""))
            return round(inr / 10_000_000, 4)
        except ValueError:
            pass

    # Compact notation
    compact = text.replace(",", "")
    unit_match = re.search(
        r"([\d]+(?:\.\d+)?)\s*(crore|crores|cr|lakh|lakhs|lac|lacs)",
        compact,
        re.IGNORECASE,
    )
    if unit_match:
        number = float(unit_match.group(1))
        unit = unit_match.group(2).lower()
        if unit in {"crore", "crores", "cr"}:
            return round(number, 4)
        if unit in {"lakh", "lakhs", "lac", "lacs"}:
            return round(number / 100, 4)
    return None


def scrape_winner_urls() -> List[Dict[str, str]]:
    """
    Fetch the winners table and return [{constituency, name, detail_url, doc_id}].
    doc_id is `2021_{constituency_slug}` matching candidate_accountability docs.
    """
    url = BASE_URL + WINNERS_PATH
    print(f"Fetching winners table: {url}")
    soup = BeautifulSoup(_get(url).text, "lxml")

    winners = []
    for row in soup.select("table tr"):
        cells = row.find_all("td")
        if len(cells) < 5:
            continue

        name_td = cells[1]
        constituency_td = cells[2]
        link = name_td.find("a", href=True)
        if not link:
            continue

        raw_name        = re.sub(r"\s+", " ", name_td.get_text(" ", strip=True))
        raw_constituency = re.sub(r"\s+", " ", constituency_td.get_text(" ", strip=True))
        detail_url      = BASE_URL + "/" + link["href"].lstrip("/")

        if not raw_name or not raw_constituency:
            continue

        constituency_slug = re.sub(r"_+", "_",
            re.sub(r"[^a-z0-9]", "_", raw_constituency.lower())
        ).strip("_")
        doc_id = f"2021_{constituency_slug}"

        winners.append({
            "name":        raw_name,
            "constituency": raw_constituency,
            "doc_id":      doc_id,
            "detail_url":  detail_url,
        })

    print(f"Found {len(winners)} winners")
    return winners


def scrape_detail(detail_url: str) -> Dict[str, Any]:
    """
    Fetch a candidate detail page and extract:
      movable_assets_cr, immovable_assets_cr, institution_name, source_pdf
    """
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

    # ── Assets breakdown ──────────────────────────────────────────────────
    # MyNeta detail pages have a table with rows like:
    #   "Movable Assets"     | "Rs X,XX,XXX ~ Y Crore+"
    #   "Immovable Assets"   | "Rs X,XX,XXX ~ Y Crore+"
    for row in soup.select("table tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        label = cells[0].get_text(" ", strip=True).lower()
        value = cells[1].get_text(" ", strip=True)
        if "movable" in label and "immovable" not in label:
            result["movable_assets_cr"] = _inr_to_cr(value)
        elif "immovable" in label:
            result["immovable_assets_cr"] = _inr_to_cr(value)

    # ── Education / Institution ───────────────────────────────────────────
    # Look for a row labelled "Education" — value is often "Graduate (B.Sc) — ABC College"
    for row in soup.select("table tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        label = cells[0].get_text(" ", strip=True).lower()
        if "education" in label:
            raw = cells[1].get_text(" ", strip=True)
            # Try to extract institution from patterns like "Graduate - XYZ College"
            # or just set institution_name if it's a multi-word value after the degree
            inst_match = re.search(r"[-–—]\s*(.{5,})", raw)
            if inst_match:
                result["institution_name"] = inst_match.group(1).strip()[:120]
            break

    # ── Source PDF ────────────────────────────────────────────────────────
    # MyNeta links to the ECI Form 26 PDF via text like "View Affidavit" or "Affidavit"
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True).lower()
        if ("affidavit" in text or "form 26" in text or href.endswith(".pdf")):
            # Ensure absolute URL
            if href.startswith("http"):
                result["source_pdf"] = href
            elif href.startswith("/"):
                result["source_pdf"] = "https://myneta.info" + href
            else:
                result["source_pdf"] = BASE_URL + "/" + href.lstrip("/")
            break

    return result


def _already_enriched_slugs(db: Any) -> set:
    """Return set of doc_ids that already have source_pdf set (for --resume)."""
    from google.cloud.firestore_v1.base_query import FieldFilter  # type: ignore[import]
    col = db.collection(COLLECTION)
    docs = list(col.where(
        filter=FieldFilter("source_pdf", "!=", None)
    ).stream())
    return {d.id for d in docs}


def run(dry_run: bool, limit: Optional[int], resume: bool) -> None:
    winners = scrape_winner_urls()
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
            already_done = _already_enriched_slugs(db)
            print(f"[resume] {len(already_done)} docs already enriched — skipping")

    enriched: List[Dict[str, Any]] = []
    skipped = 0

    for i, w in enumerate(winners):
        doc_id = w["doc_id"]
        if doc_id in already_done:
            skipped += 1
            continue

        print(f"[{i+1}/{len(winners)}] {w['constituency']} ({doc_id})")
        detail = scrape_detail(w["detail_url"])

        if dry_run:
            print(f"  movable={detail['movable_assets_cr']} immovable={detail['immovable_assets_cr']}"
                  f" institution={detail['institution_name']} pdf={detail['source_pdf']}")
        else:
            db.collection(COLLECTION).document(doc_id).set(detail, merge=True)  # type: ignore[union-attr]
            enriched.append({"doc_id": doc_id, **detail})

        time.sleep(SLEEP_BETWEEN)

    if dry_run:
        print(f"\n[dry-run complete] Would enrich {len(winners)} docs.")
    else:
        print(f"\nDone — enriched {len(enriched)} docs, skipped {skipped}.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich candidate_accountability with affidavit fields")
    parser.add_argument("--dry-run", action="store_true", help="Print without writing to Firestore")
    parser.add_argument("--limit", type=int, default=None, help="Only process first N winners")
    parser.add_argument("--resume", action="store_true", help="Skip docs that already have source_pdf")
    args = parser.parse_args()
    run(dry_run=args.dry_run, limit=args.limit, resume=args.resume)


if __name__ == "__main__":
    main()
