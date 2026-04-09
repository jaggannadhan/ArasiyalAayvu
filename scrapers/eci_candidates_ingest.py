"""
ECI Candidates Ingest — TN 2026 Assembly Election
==================================================
Scrapes https://affidavit.eci.gov.in/CandidateCustomFilter for all 234 TN
constituencies and writes ACCEPTED candidates to Firestore `candidates_2026`.

Why Playwright: Akamai WAF blocks all non-browser requests (direct requests/
headless with networkidle both fail). Non-headless Chromium + commit wait works.

URL pattern:
  https://affidavit.eci.gov.in/CandidateCustomFilter
    ?electionType=32-AC-GENERAL-3-60&election=32-AC-GENERAL-3-60
    &states=S22&phase=2&constId={1-234}&page={1,2,3,...}

Usage:
  .venv/bin/python3 scrapers/eci_candidates_ingest.py --dry-run --limit 5
  .venv/bin/python3 scrapers/eci_candidates_ingest.py
  .venv/bin/python3 scrapers/eci_candidates_ingest.py --resume
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright, Page

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from google.cloud import firestore
except ImportError:
    firestore = None  # type: ignore[assignment]

COLLECTION    = "candidates_2026"
PORTAL_HOME   = "https://affidavit.eci.gov.in/"
FILTER_BASE   = (
    "https://affidavit.eci.gov.in/CandidateCustomFilter"
    "?electionType=32-AC-GENERAL-3-60&election=32-AC-GENERAL-3-60"
    "&states=S22&phase=2"
)
MAP_PATH      = ROOT / "web" / "src" / "lib" / "constituency-map.json"
PROGRESS_FILE = ROOT / "scrapers" / ".eci_candidates_progress.json"
NOW_ISO       = datetime.now(timezone.utc).isoformat()

PARTY_NORM: dict[str, str] = {
    "dravida munnetra kazhagam":                  "dmk",
    "all india anna dravida munnetra kazhagam":   "aiadmk",
    "bharatiya janata party":                     "bjp",
    "indian national congress":                   "inc",
    "pattali makkal katchi":                      "pmk",
    "communist party of india":                   "cpi",
    "communist party of india (marxist)":         "cpim",
    "communist party of india  (marxist)":        "cpim",
    "viduthalai chiruthaigal katchi":             "vck",
    "desiya murpokku dravida kazhagam":           "dmdk",
    "marumalarchi dravida munnetra kazhagam":     "mdmk",
    "naam tamilar katchi":                        "ntk",
    "tamilaga vettri kazhagam":                   "tvk",
    "bahujan samaj party":                        "bsp",
    "independent":                                "ind",
}

from name_utils import name_variants, canonical_name  # noqa: E402  (after sys.path insert)


def _party_id(raw: str) -> str:
    key = re.sub(r"\s+", " ", raw.strip().lower())
    return PARTY_NORM.get(key, re.sub(r"_+", "_", re.sub(r"[^a-z0-9]", "_", key)).strip("_")[:16])


# ---------------------------------------------------------------------------
# JS extractor — runs inside the browser page
# ---------------------------------------------------------------------------
_EXTRACT_JS = """
() => {
    // DOM structure (confirmed from HTML inspection):
    //   tr
    //     td.text-center > div.img-bx > a[href=show-profile] > img[src=photo]
    //     td > div.details-name
    //       h4           ← candidate name
    //       div.left-party  > p (Party: ...), p (Status: ...)
    //       div.right-party > p (State: ...), p (Constituency: ...), div.hover-lay > a[show-profile]
    //
    // Note: Gender, Nomination No/Date, Age, Education, Assets are NOT on the list page —
    // those are only available on the show-profile detail page.

    const results = [];
    for (const row of document.querySelectorAll('#data-tab tbody tr')) {

        // ── Name ─────────────────────────────────────────────────────────
        const h4 = row.querySelector('.details-name h4, h4');
        const name = h4 ? h4.textContent.trim() : '';
        if (!name) continue;

        // ── Photo ─────────────────────────────────────────────────────────
        // Prefer img inside .img-bx; fall back to any td img
        const imgEl = row.querySelector('.img-bx img') || row.querySelector('td img');
        const photo_url = (imgEl && imgEl.src && !imgEl.src.includes('logo')
                           && !imgEl.src.includes('ECI') && !imgEl.src.includes('flag'))
                          ? imgEl.src : '';

        // ── Party & Status from .left-party ──────────────────────────────
        let party = '', status = '';
        const leftParty = row.querySelector('.left-party');
        if (leftParty) {
            for (const p of leftParty.querySelectorAll('p')) {
                const t = p.innerText || '';
                if (/Party\\s*:/i.test(t)) party  = t.replace(/.*Party\\s*:\\s*/i, '').trim();
                if (/Status\\s*:/i.test(t)) status = t.replace(/.*Status\\s*:\\s*/i, '').trim();
            }
        }

        // ── Constituency from .right-party ───────────────────────────────
        let constituency = '';
        const rightParty = row.querySelector('.right-party');
        if (rightParty) {
            for (const p of rightParty.querySelectorAll('p')) {
                const t = p.innerText || '';
                if (/Constituency\\s*:/i.test(t)) {
                    constituency = t.replace(/.*Constituency\\s*:\\s*/i, '').trim();
                    break;
                }
            }
        }

        // ── Show-profile link → used as affidavit_url for now ────────────
        // The show-profile page has the Form 26 download button
        const link = row.querySelector('.img-bx a[href*="show-profile"], .hover-lay a, a[href*="show-profile"]');
        const show_profile_url = link ? link.href : '';

        results.push({ name, party, status, constituency, photo_url, show_profile_url });
    }
    return results;
}
"""

_RAW_ROW_JS = """
() => {
    const table = document.querySelector('#data-tab');
    if (!table) return 'NO #data-tab';
    const row = table.querySelector('tbody tr');
    if (!row) return 'NO ROWS';
    return row.outerHTML.slice(0, 3000);
}
"""

_TOTAL_JS = """
() => {
    // Look for "Showing X to Y of Z results" text
    const body = document.body.innerText;
    const m = body.match(/of\\s+(\\d+)\\s+results/i);
    return m ? parseInt(m[1]) : 0;
}
"""


def _scrape_constituency(page: Page, const_id: int, explore: bool = False) -> list[dict]:
    """Fetch all pages for a constituency, return only Accepted candidates (deduplicated)."""
    all_candidates: list[dict] = []
    seen: set[tuple[str, str]] = set()  # (normalised_name, party) dedup key
    pg = 1

    while True:
        url = f"{FILTER_BASE}&constId={const_id}&page={pg}"
        page.goto(url, wait_until="commit", timeout=25000)
        page.wait_for_timeout(3500)

        if explore and pg == 1:
            raw_html = page.evaluate(_RAW_ROW_JS)
            print(f"\n  --- Raw first row HTML ---\n{raw_html}\n  ---\n")

        rows: list[dict] = page.evaluate(_EXTRACT_JS)
        if not rows:
            if pg == 1 and explore:
                print(f"    No rows on page 1 for constId={const_id}")
            break

        total = page.evaluate(_TOTAL_JS)

        new_this_page = 0
        for r in rows:
            if r["status"].lower() != "accepted":
                continue
            key = (re.sub(r"\s+", " ", r["name"].strip().upper()), r["party"].strip().upper())
            if key in seen:
                continue
            seen.add(key)
            new_this_page += 1
            raw_name = r["name"]
            all_candidates.append({
                "name":             raw_name,
                "canonical_name":   canonical_name(raw_name),
                "alias_names":      name_variants(raw_name),
                "party":            r["party"],
                "party_id":         _party_id(r["party"]),
                "status":           r["status"],
                "constituency":     r.get("constituency") or "",
                "photo_url":        r.get("photo_url") or None,
                "show_profile_url": r.get("show_profile_url") or None,
            })

        fetched_so_far = (pg - 1) * 10 + len(rows)
        if explore:
            print(f"    Page {pg}: {len(rows)} rows ({new_this_page} new accepted), total={total}, fetched={fetched_so_far}")

        # Stop if: no new unique candidates this page (DOM is cumulative — all dupes), OR
        # last page (fewer than 10 rows), OR total reached
        if new_this_page == 0:
            break
        if len(rows) < 10:
            break
        if total > 0 and fetched_so_far >= total:
            break
        pg += 1
        time.sleep(0.3)

    return all_candidates


def _build_id_slug_map() -> dict[int, str]:
    cmap = json.loads(MAP_PATH.read_text(encoding="utf-8"))
    return {
        meta["constituency_id"]: slug
        for slug, meta in cmap.items()
        if isinstance(meta.get("constituency_id"), int)
    }


def run(dry_run: bool, limit: int | None, resume: bool, explore: bool, force: bool, only: str | None = None) -> None:
    id_slug = _build_id_slug_map()
    # --only: restrict to specific slugs regardless of progress
    if only:
        target_slugs = {s.strip() for s in only.split(",")}
        const_ids = [cid for cid, slug in sorted(id_slug.items()) if slug in target_slugs]
        force = True  # always re-process when --only is used
        print(f"[only] Targeting {len(const_ids)} constituencies: {sorted(target_slugs)}")
    else:
        const_ids = sorted(id_slug.keys())
        if limit:
            const_ids = const_ids[:limit]

    # Load progress
    done: set[str] = set()
    if force:
        if PROGRESS_FILE.exists():
            PROGRESS_FILE.unlink()
        print("[force] Ignoring progress file — re-processing all constituencies")
    elif resume and PROGRESS_FILE.exists():
        done = set(json.loads(PROGRESS_FILE.read_text()))
        print(f"[resume] {len(done)} already done")

    db = col = None
    if not dry_run:
        if firestore is None:
            sys.exit("ERROR: google-cloud-firestore not installed")
        db  = firestore.Client(project="naatunadappu")
        col = db.collection(COLLECTION)

    written = skipped = errors = 0

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

        # Warm up — visit home to get cookies
        print("Warming up portal…")
        page.goto(PORTAL_HOME, wait_until="commit", timeout=20000)
        page.wait_for_timeout(2500)
        print(f"  Portal: {page.title()!r}")

        for const_id in const_ids:
            slug = id_slug[const_id]
            if slug in done:
                skipped += 1
                continue

            print(f"  {const_id:>3} {slug}", end="  ", flush=True)
            try:
                candidates = _scrape_constituency(page, const_id, explore)
            except Exception as exc:
                print(f"ERROR: {exc}")
                errors += 1
                time.sleep(3)
                continue

            print(f"→ {len(candidates)} accepted")
            if explore:
                for c in candidates[:5]:
                    photo = c.get("photo_url") or "NO PHOTO"
                    profile = "profile-link" if c.get("show_profile_url") else "NO LINK"
                    print(f"      {c['name']} | {c['party']} | photo={photo[:60]} | {profile}")

            if dry_run:
                done.add(slug)
                continue

            doc = {
                "constituency_slug":  slug,
                "ac_number":          const_id,
                "candidates":         candidates,
                "total_candidates":   len(candidates),
                "election_year":      2026,
                "_source":            FILTER_BASE,
                "_scraped_at":        NOW_ISO,
            }
            col.document(slug).set(doc)  # type: ignore[union-attr]
            written += 1
            done.add(slug)

            # Save progress every 10
            if written % 10 == 0:
                PROGRESS_FILE.write_text(json.dumps(sorted(done)))

            time.sleep(0.5)

        browser.close()

    PROGRESS_FILE.write_text(json.dumps(sorted(done)))
    print(f"\nDone — written={written} skipped={skipped} errors={errors}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest TN 2026 candidates from ECI affidavit portal")
    ap.add_argument("--dry-run",  action="store_true", help="Parse but do not write to Firestore")
    ap.add_argument("--explore",  action="store_true", help="Print candidate details during run")
    ap.add_argument("--limit",    type=int, default=None, help="Only process first N constituencies")
    ap.add_argument("--resume",   action="store_true", help="Skip slugs already in progress file")
    ap.add_argument("--force",    action="store_true", help="Ignore progress file, re-process all")
    ap.add_argument("--only",     type=str, default=None, help="Comma-separated slugs to re-process (overrides --limit/--resume)")
    args = ap.parse_args()
    run(dry_run=args.dry_run, limit=args.limit, resume=args.resume, explore=args.explore, force=args.force, only=args.only)


if __name__ == "__main__":
    main()
