"""
eci_candidates_enrich.py — Enrich candidates_2026 with detail-page fields.

Visits each candidate's show_profile_url on affidavit.eci.gov.in and extracts:
  - gender
  - nomination_number
  - nomination_date
  - affidavit_url   (link to Form 26 PDF)
  - eci_candidate_id

Fields are patched into the candidates[] array in each Firestore doc.
Already-enriched candidates (gender != null) are skipped.

Usage:
  .venv/bin/python3 scrapers/eci_candidates_enrich.py --dry-run --limit 2
  .venv/bin/python3 scrapers/eci_candidates_enrich.py --only poompuhar
  .venv/bin/python3 scrapers/eci_candidates_enrich.py
  .venv/bin/python3 scrapers/eci_candidates_enrich.py --resume   # skip already-done slugs
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright, Page

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from google.cloud import firestore
except ImportError:
    firestore = None  # type: ignore[assignment]

COLLECTION     = "candidates_2026"
PORTAL_HOME    = "https://affidavit.eci.gov.in/"
PROGRESS_FILE  = ROOT / "scrapers" / ".eci_enrich_progress.json"

# ---------------------------------------------------------------------------
# JS extractor — runs inside the show-profile detail page
# ---------------------------------------------------------------------------
_DETAIL_JS = """
() => {
    const allText = document.body.innerText;

    // Gender — "Gender:\\n\\nmale" on ECI show-profile page
    let gender = '';
    const genderMatch = allText.match(/Gender\\s*[:\\-]\\s*(Male|Female|Third\\s*Gender)/i);
    if (genderMatch) gender = genderMatch[1].replace(/\\s+/g, ' ').trim();

    // Age
    let age = null;
    const ageMatch = allText.match(/Age\\s*[:\\-]\\s*(\\d+)/i);
    if (ageMatch) age = parseInt(ageMatch[1]);

    // Application Uploaded date — closest to a nomination filing date on this page
    let nomination_date = '';
    const dateMatch = allText.match(/Application\\s*Uploaded\\s*[:\\-]?\\s*([\\d]+(?:st|nd|rd|th)?\\s+[A-Za-z]+,?\\s+\\d{4})/i);
    if (dateMatch) nomination_date = dateMatch[1].trim();

    // Affidavit download links — collect all onclick handlers that call download
    let affidavit_url = '';
    for (const el of document.querySelectorAll('[onclick]')) {
        const oc = el.getAttribute('onclick') || '';
        // e.g. downloadAffidavit('/path/to/file.pdf')
        const pdfMatch = oc.match(/['"](https?:\/\/[^'"]+\\.pdf[^'"]*)['"]/i)
                      || oc.match(/['"](\/[^'"]+\\.pdf[^'"]*)['"]/i);
        if (pdfMatch) { affidavit_url = pdfMatch[1]; break; }
    }
    // Fall back: any <a> with a real PDF href
    if (!affidavit_url) {
        for (const a of document.querySelectorAll('a[href]')) {
            const href = a.href || '';
            if (href.includes('.pdf') && !href.startsWith('javascript')) {
                affidavit_url = href;
                break;
            }
        }
    }

    // Current page URL segments — ECI embeds candidate token in URL
    const url = window.location.href;

    return { gender, age, nomination_date, affidavit_url, url };
}
"""


def _enrich_candidate(page: Page, candidate: dict, explore: bool) -> dict | None:
    """
    Visit show_profile_url and return enriched fields dict, or None on failure.
    Only visits if gender is not already set.
    """
    url = candidate.get("show_profile_url")
    if not url:
        return None
    if candidate.get("gender"):
        return None  # already enriched

    try:
        page.goto(url, wait_until="commit", timeout=25000)
        page.wait_for_timeout(3000)
        result: dict = page.evaluate(_DETAIL_JS)
        if explore:
            print(f"      detail → {result}")
        return result
    except Exception as exc:
        print(f"      ERROR fetching detail: {exc}")
        return None


def run(dry_run: bool, limit: int | None, resume: bool, only: str | None, explore: bool) -> None:
    db = col = None
    if not dry_run:
        if firestore is None:
            sys.exit("ERROR: google-cloud-firestore not installed")
        db  = firestore.Client(project="naatunadappu")
        col = db.collection(COLLECTION)

    # Load done slugs
    done: set[str] = set()
    if resume and PROGRESS_FILE.exists():
        done = set(json.loads(PROGRESS_FILE.read_text()))
        print(f"[resume] {len(done)} slugs already done")

    # Load all constituency docs
    if dry_run and only is None and limit is None:
        sys.exit("Use --dry-run with --only or --limit to test")

    fs_col = (col or firestore.Client(project="naatunadappu").collection(COLLECTION))  # type: ignore[union-attr]
    if only:
        slugs = [s.strip() for s in only.split(",")]
        docs = [fs_col.document(slug).get() for slug in slugs]
    else:
        docs = list(fs_col.stream())

    if limit:
        docs = docs[:limit]

    print(f"Processing {len(docs)} constituency docs…")

    updated = skipped = errors = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        page = ctx.new_page()

        # Warm up
        print("Warming up portal…")
        page.goto(PORTAL_HOME, wait_until="commit", timeout=20000)
        page.wait_for_timeout(2500)

        for doc in docs:
            slug = doc.id
            if slug in done:
                skipped += 1
                continue

            data = doc.to_dict() or {}
            candidates = data.get("candidates", [])

            # Check if any need enrichment
            needs_enrich = [c for c in candidates if not c.get("gender")]
            if not needs_enrich:
                done.add(slug)
                skipped += 1
                continue

            print(f"  {slug}: {len(needs_enrich)}/{len(candidates)} candidates need enrichment")
            changed = False

            for c in candidates:
                if c.get("gender"):
                    continue  # already done
                if explore:
                    print(f"    → {c['name']}")
                detail = _enrich_candidate(page, c, explore)
                if detail:
                    if detail.get("gender"):
                        c["gender"] = detail["gender"].capitalize()
                    if detail.get("age") is not None:
                        c["age"] = detail["age"]
                    if detail.get("nomination_date"):
                        c["nomination_date"] = detail["nomination_date"]
                    if detail.get("affidavit_url"):
                        c["affidavit_url"] = detail["affidavit_url"]
                    # Store show_profile_url as affidavit_url fallback so UI can link to it
                    if not c.get("affidavit_url") and c.get("show_profile_url"):
                        c["affidavit_url"] = c["show_profile_url"]
                    changed = True
                time.sleep(0.5)

            if dry_run:
                print(f"    [dry-run] would patch {slug}")
                done.add(slug)
                continue

            if changed:
                try:
                    fs_col.document(slug).update({"candidates": candidates})
                    updated += 1
                    print(f"    ✓ updated {slug}")
                except Exception as exc:
                    print(f"    ERROR writing {slug}: {exc}")
                    errors += 1
            else:
                print(f"    no changes for {slug}")

            done.add(slug)
            PROGRESS_FILE.write_text(json.dumps(sorted(done)))
            time.sleep(0.5)

        browser.close()

    PROGRESS_FILE.write_text(json.dumps(sorted(done)))
    print(f"\nDone — updated={updated} skipped={skipped} errors={errors}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Enrich candidates_2026 with show-profile detail fields")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--explore", action="store_true", help="Print detail page results")
    ap.add_argument("--limit",   type=int, default=None, help="Process first N constituency docs")
    ap.add_argument("--resume",  action="store_true", help="Skip slugs already in progress file")
    ap.add_argument("--only",    type=str, default=None, help="Comma-separated slugs to process")
    args = ap.parse_args()
    run(
        dry_run=args.dry_run,
        limit=args.limit,
        resume=args.resume,
        only=args.only,
        explore=args.explore,
    )


if __name__ == "__main__":
    main()
