"""
ECI Affidavit Ingest — TN 2026 Assembly Election
=================================================
Scrapes https://affidavit.eci.gov.in/ using Playwright (run LOCALLY — Akamai
WAF blocks all server/cloud IPs regardless of cookies).

Portal flow:
  1. Open portal → select Tamil Nadu (state), Phase 1 → click Filter
     → constituency dropdown (#constId) populates with 234 TN ACs
  2. For each of our 234 Firestore constituencies:
       select it in #constId → click Filter → scrape the ~10-30 candidate cards
  3. For each matched candidate, visit their "View more" / show-profile page
     to extract the affidavit PDF link
  4. Write affidavit_url + photo_url back to Firestore

Strategy
--------
- 234 constituency filter operations  vs  731-page scrolling through all 7308
- Playwright keeps the browser open; all navigation is in-browser (bypasses WAF)
- Firestore match: ac_slug_map (ECI const_id → slug) + name_key fuzzy match

Prerequisites
-------------
  .venv/bin/python3 -m pip install playwright beautifulsoup4 lxml
  .venv/bin/python3 -m playwright install chromium

Usage
-----
  # Explore: print raw card data for first 2 pages; visit 3 detail pages
  .venv/bin/python3 scrapers/eci_affidavit_ingest.py --explore

  # Dry-run (no Firestore writes)
  .venv/bin/python3 scrapers/eci_affidavit_ingest.py --dry-run --limit 10

  # Full run
  .venv/bin/python3 scrapers/eci_affidavit_ingest.py

  # Resume (skip candidates already patched)
  .venv/bin/python3 scrapers/eci_affidavit_ingest.py --resume
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

from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from playwright.sync_api import sync_playwright, Page
except ImportError:
    sys.exit("ERROR: playwright not installed.\n"
             "Run: .venv/bin/python3 -m pip install playwright && "
             ".venv/bin/python3 -m playwright install chromium")

try:
    from google.cloud import firestore
except ImportError:
    firestore = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PORTAL_URL    = "https://affidavit.eci.gov.in/"
COLLECTION    = "candidates_2026"
MAP_PATH      = ROOT / "web" / "src" / "lib" / "constituency-map.json"
PROGRESS_FILE = ROOT / "scrapers" / ".eci_progress.json"
NOW_ISO       = datetime.now(timezone.utc).isoformat()

TN_STATE   = "S22"
PHASE_LABEL = "1"   # displayed label; value in dropdown may differ

BROWSER_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0.0.0 Safari/537.36")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _name_key(name: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", name.upper())

def _surname(name: str) -> str:
    """
    Extract the most unique token from a name for cross-source matching.
    CEO TN format: "VENKATESAN L"  → longest word = "VENKATESAN"
    ECI format:    "L.VENKATESAN"  → longest word = "VENKATESAN"
    Returns the longest word with 4+ chars, uppercased.
    """
    words = [w for w in re.sub(r"[^A-Z0-9]", " ", name.upper()).split() if len(w) >= 4]
    return max(words, key=len) if words else ""

def _slugify(v: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]", "_", v.lower())).strip("_")

def _load_slug_map() -> dict[str, str]:
    """
    Returns ac_slug_map: str(constituency_id) → slug.
    ECI const_id numbers correspond to our constituency_id values (official TN AC numbers).
    Note: ECI dropdown labels do NOT reliably match constituency names — use numeric ID only.
    """
    cmap = json.loads(MAP_PATH.read_text(encoding="utf-8"))
    return {
        str(meta["constituency_id"]): slug
        for slug, meta in cmap.items()
        if isinstance(meta.get("constituency_id"), int)
    }


# ---------------------------------------------------------------------------
# Step 1 — navigate to TN candidate list
# ---------------------------------------------------------------------------
def _navigate_to_tn_list(page: Page, explore: bool) -> bool:
    """
    Open the portal, select Tamil Nadu + Phase 1, click Filter, then click
    the "All" stats card to show all candidates.
    Returns True on success.
    """
    for attempt in range(3):
        try:
            page.goto(PORTAL_URL, wait_until="commit", timeout=30000)
            page.wait_for_load_state("domcontentloaded", timeout=15000)
            break
        except Exception as e:
            if attempt == 2:
                print(f"ERROR: Could not load portal after 3 attempts: {e}")
                return False
            print(f"  Attempt {attempt+1} failed ({e}), retrying…")
            time.sleep(2)

    title = page.title()
    if "access denied" in title.lower():
        print("ERROR: Access Denied — run from local machine (residential IP)")
        return False
    print(f"  Portal: {title!r}")

    # ── Select state ──────────────────────────────────────────────────────
    page.evaluate(f"""
        const s = document.querySelector('#states');
        if (s) {{ s.value = '{TN_STATE}'; s.dispatchEvent(new Event('change', {{bubbles:true}})); }}
    """)
    page.wait_for_timeout(2000)

    # ── Select Phase 1 ────────────────────────────────────────────────────
    # The phase dropdown label "1" may have value "2" — find by text
    phase_set = page.evaluate(f"""
        (() => {{
            const ph = document.querySelector('#phase');
            if (!ph) return false;
            for (const opt of ph.options) {{
                if (opt.textContent.trim() === '{PHASE_LABEL}') {{
                    ph.value = opt.value;
                    ph.dispatchEvent(new Event('change', {{bubbles:true}}));
                    return opt.value;
                }}
            }}
            // fallback: first non-empty option
            for (const opt of ph.options) {{
                if (opt.value && opt.value !== '0') {{
                    ph.value = opt.value;
                    ph.dispatchEvent(new Event('change', {{bubbles:true}}));
                    return opt.value;
                }}
            }}
            return false;
        }})()
    """)
    if explore:
        print(f"  Phase set to value: {phase_set!r}")
    page.wait_for_timeout(1500)

    # ── Click Filter button ───────────────────────────────────────────────
    filter_clicked = False
    for sel in ['button:has-text("Filter")', 'input[value="Filter"]',
                '.btn-primary', 'button[type="submit"]', 'input[type="submit"]']:
        try:
            page.click(sel, timeout=3000)
            filter_clicked = True
            if explore:
                print(f"  Clicked filter: {sel!r}")
            break
        except Exception:
            continue

    if not filter_clicked:
        # Try submitting the form directly
        page.evaluate("document.querySelector('form')?.submit()")
        if explore:
            print("  Submitted form via JS")

    page.wait_for_timeout(2000)

    # ── Click Filter to initialise the session ────────────────────────────
    # (No need to open the full list — we'll filter per constituency next)
    filter_clicked = False
    for sel in ['button:has-text("Filter")', 'input[value="Filter"]',
                'button[type="submit"]', 'input[type="submit"]']:
        try:
            page.click(sel, timeout=2000)
            filter_clicked = True
            break
        except Exception:
            continue
    if not filter_clicked:
        page.evaluate("document.querySelector('form')?.submit()")

    page.wait_for_timeout(2500)

    # Verify constituency dropdown is populated
    const_count = page.evaluate("""
        document.querySelectorAll('#constId option[value]:not([value=""])').length
    """)
    if explore:
        print(f"  Constituency dropdown: {const_count} options")

    return True


# ---------------------------------------------------------------------------
# Step 2 — extract candidate cards from the current page
# ---------------------------------------------------------------------------
def _extract_page_cards(page: Page, explore: bool) -> list[dict]:
    """
    Read all candidate cards on the current page using browser-side JS
    (more accurate than BeautifulSoup since we work on the live DOM).
    Returns list of dicts: {name, party, constituency, photo_url, view_more_href}
    """
    if explore:
        # Dump raw HTML around the first View-more link so we can see the DOM
        raw_snippet = page.evaluate("""
            (() => {
                const link = Array.from(document.querySelectorAll('a'))
                                  .find(a => /view\\s*more/i.test(a.textContent.trim()));
                if (!link) return 'NO VIEW-MORE LINK FOUND';
                // Walk up 6 levels and dump the outerHTML
                let el = link;
                for (let i = 0; i < 6; i++) el = el.parentElement || el;
                return el.outerHTML.slice(0, 2000);
            })()
        """)
        print(f"\n  --- Raw card HTML (first card, 6 levels up from View more) ---\n{raw_snippet}\n  ---\n")

    # DOM structure confirmed from HTML dump:
    #   div.details-name
    #     h4.bg-dark-blu  ← candidate name
    #     div.container-fluid
    #       div.row
    #         div.left-party  ← Party, Status
    #         div.right-party ← State, Constituency, View-more link
    # Photo is in a sibling <td> in the same <tr>.
    cards: list[dict] = page.evaluate("""
        (() => {
            const results = [];

            for (const card of document.querySelectorAll('.details-name')) {
                // Name
                const nameEl = card.querySelector('h4');
                const name = nameEl ? nameEl.textContent.trim() : '';

                // Party
                const leftParty = card.querySelector('.left-party');
                let party = '';
                if (leftParty) {
                    const partyP = leftParty.querySelector('p');
                    if (partyP) party = partyP.textContent.replace(/Party\\s*:/i, '').trim();
                }

                // Constituency
                const rightParty = card.querySelector('.right-party');
                let constituency = '';
                if (rightParty) {
                    for (const p of rightParty.querySelectorAll('p')) {
                        if (/Constituency/i.test(p.textContent)) {
                            constituency = p.textContent.replace(/Constituency\\s*:/i, '').trim().toUpperCase();
                            break;
                        }
                    }
                }

                // View-more href
                const link = card.querySelector('a[href*="show-profile"], a[href*="candidate"]');
                const href = link ? link.href : '';

                // Photo — look in sibling <td> within the same <tr>
                let photoUrl = '';
                const tr = card.closest('tr');
                if (tr) {
                    const img = tr.querySelector('img');
                    if (img && img.src && !img.src.includes('logo')) photoUrl = img.src;
                }

                if (name || constituency) {
                    results.push({ name, party, constituency, photo_url: photoUrl, view_more_href: href });
                }
            }

            return results;
        })()
    """)

    return cards or []


# ---------------------------------------------------------------------------
# Step 3 — open detail page in a NEW TAB and extract affidavit PDF URL
# ---------------------------------------------------------------------------
def _get_affidavit_url(context: Any, view_more_href: str, explore: bool) -> str:
    """
    Opens the candidate show-profile page in a new tab. Looks for a 'Download'
    button (href=javascript:void(0)), clicks it, and captures the resulting URL
    via popup (window.open) or network request interception.
    Returns the affidavit PDF/download URL, or empty string.
    """
    tab = context.new_page()
    try:
        tab.goto(view_more_href, wait_until="commit", timeout=30000)
        tab.wait_for_load_state("domcontentloaded", timeout=15000)
        tab.wait_for_timeout(2500)

        if explore:
            links = tab.evaluate("""
                Array.from(document.querySelectorAll('a, button'))
                     .map(el => ({text: el.textContent.trim(), href: el.href || el.getAttribute('href') || '(button)'}))
                     .filter(e => e.text || e.href)
            """)
            print(f"      All links on show-profile page ({len(links)}):")
            for lnk in links:
                print(f"        {lnk['text']!r:45} → {lnk['href'][:80]}")

        # Check for a Download button
        download_btn = tab.query_selector(
            'a:has-text("Download"), button:has-text("Download"), '
            'a[onclick*="download"], button[onclick*="download"]'
        )
        if not download_btn:
            if explore:
                print("      No Download button found")
            return ""

        # ── Intercept window.open() calls before clicking ─────────────────
        # Suppress the actual open so no stray tabs are created
        tab.evaluate("""
            window._eci_captured = [];
            window.open = function(url) {
                if (url) window._eci_captured.push(String(url));
                return null;
            };
        """)

        # Capture all network requests + response bodies in one click
        all_requests: list[dict] = []
        all_responses: list[dict] = []

        def on_req(req: Any) -> None:
            all_requests.append({"url": req.url, "method": req.method})

        def on_resp(resp: Any) -> None:
            try:
                body = resp.text()
                all_responses.append({"url": resp.url, "body": body[:500]})
            except Exception:
                pass

        tab.on("request", on_req)
        tab.on("response", on_resp)

        # Single click — wait for JS + network to settle
        download_btn.click()
        tab.wait_for_timeout(5000)

        # ── Check window.open capture (most reliable) ─────────────────────
        opened = tab.evaluate("window._eci_captured")
        if opened:
            url = opened[0]
            if explore:
                print(f"      window.open URL: {url!r}")
            return url

        # ── Check response bodies for PDF URL ─────────────────────────────
        for resp in all_responses:
            body = resp["body"]
            if explore:
                print(f"      Response {resp['url'][:60]}: {body[:150]}")
            # Look for a URL in the JSON response
            m = re.search(r'https?://[^\s"\']+\.pdf[^\s"\']*', body)
            if m:
                return m.group(0)
            m2 = re.search(r'"(https?://[^\s"\']{30,})"', body)
            if m2 and any(kw in m2.group(1).lower() for kw in ("pdf", "download", "affidavit")):
                return m2.group(1)

        # ── Check if any request URL is the affidavit download endpoint ────
        for req in all_requests:
            u = req["url"]
            if "affidavit.eci.gov.in" in u and (
                "pdf-download" in u.lower() or ".pdf" in u.lower()
            ):
                if explore:
                    print(f"      Affidavit URL: {u!r}")
                return u

        if explore:
            print(f"      Requests fired: {[r['url'] for r in all_requests]}")
            print("      No PDF URL captured")
        return ""

    except Exception as e:
        if explore:
            print(f"      Error: {e}")
        return ""
    finally:
        tab.close()


# ---------------------------------------------------------------------------
# Step 4 — patch Firestore
# ---------------------------------------------------------------------------
def _patch_candidate(
    candidate: dict,
    eci_data: dict,
    dry_run: bool,
    col: Any,
    slug: str,
    all_candidates: list[dict],
    idx: int,
) -> bool:
    changed = False
    c = dict(candidate)

    if eci_data.get("affidavit_url") and not c.get("affidavit_url"):
        c["affidavit_url"] = eci_data["affidavit_url"]
        changed = True
    if eci_data.get("photo_url") and not c.get("photo_url"):
        c["photo_url"] = eci_data["photo_url"]
        changed = True

    if not changed:
        return False

    if dry_run:
        print(f"      [dry-run] {c['name']}:")
        print(f"        affidavit={c.get('affidavit_url','')}")
        print(f"        photo={c.get('photo_url','')[:80]}")
    else:
        all_candidates[idx] = c
        col.document(slug).update({
            "candidates":         all_candidates,
            "_affidavit_patched": NOW_ISO,
        })

    return True


# ---------------------------------------------------------------------------
# Step 3b — filter portal by one constituency and return its candidate cards
# ---------------------------------------------------------------------------
def _cards_for_constituency(
    page: Page, const_id: str, explore: bool
) -> list[dict]:
    """
    Select a specific constituency in the filter bar, click Filter, and return
    the extracted candidate cards. Handles pagination within a constituency
    (rare, but some urban seats have 30+ candidates).
    """
    # Set constituency in dropdown and click Filter
    page.evaluate(f"""
        const c = document.querySelector('#constId');
        if (c) {{ c.value = '{const_id}'; c.dispatchEvent(new Event('change', {{bubbles:true}})); }}
    """)
    page.wait_for_timeout(800)

    clicked = False
    for sel in ['button:has-text("Filter")', 'input[value="Filter"]',
                'button[type="submit"]', 'input[type="submit"]']:
        try:
            page.click(sel, timeout=3000)
            clicked = True
            break
        except Exception:
            continue
    if not clicked:
        page.evaluate("document.querySelector('form')?.submit()")

    page.wait_for_timeout(4000)

    # Collect cards across pages (constituency rarely exceeds 1 page)
    all_cards: list[dict] = []
    while True:
        cards = _extract_page_cards(page, explore=False)
        all_cards.extend(cards)

        next_link = page.query_selector('a:has-text("Next")')
        if not next_link or len(cards) == 0:
            break
        next_link.click()
        page.wait_for_timeout(2000)

    return all_cards


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run(explore: bool, dry_run: bool, limit: int | None,
        resume: bool, project: str) -> None:

    ac_slug_map = _load_slug_map()

    db = col = None
    fs_docs: dict[str, dict] = {}

    if not explore:
        if firestore is None:
            sys.exit("ERROR: google-cloud-firestore not installed")
        db = firestore.Client(project=project)
        col = db.collection(COLLECTION)
        print("Loading Firestore docs…")
        fs_docs = {d.id: d.to_dict() for d in col.stream()}
        print(f"  {len(fs_docs)} docs loaded")

    print("Launching browser…")
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, slow_mo=80)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=BROWSER_UA,
        )
        page = context.new_page()

        # ── Navigate portal and populate constituency dropdown ───────────────
        if not _navigate_to_tn_list(page, explore):
            browser.close()
            return

        # Read the constituency dropdown options (populated after phase select)
        const_options: list[tuple[str, str]] = page.evaluate("""
            Array.from(document.querySelectorAll('#constId option'))
                 .filter(o => o.value && o.value.trim() !== '' && o.value !== '0')
                 .map(o => ({value: o.value.trim(), text: o.textContent.trim()}))
        """)
        # Convert to list of (eci_const_id, label)
        const_options = [(o["value"], o["text"]) for o in const_options]

        if not const_options:
            print("ERROR: constituency dropdown empty after navigation")
            browser.close()
            return

        print(f"  {len(const_options)} constituencies to process")

        # Load progress file to support mid-run resume
        done_ids: set[str] = set()
        if resume and not dry_run and PROGRESS_FILE.exists():
            try:
                done_ids = set(json.loads(PROGRESS_FILE.read_text()).get("done", []))
                print(f"  Resuming — {len(done_ids)} constituencies already done")
            except Exception:
                pass

        if limit:
            const_options = const_options[:limit]

        total_matched = total_patched = skipped = errors = 0
        consts_done = 0

        for const_id, label in const_options:
            if const_id in done_ids:
                skipped += 1
                continue
            # Map ECI const_id → slug via official TN AC numbers
            # (ECI's numeric IDs match our constituency_id values; their labels do not)
            slug = ac_slug_map.get(const_id)
            if not slug:
                skipped += 1
                continue

            fs_doc = fs_docs.get(slug)
            if fs_doc is None and not explore:
                skipped += 1
                continue

            # Resume: skip constituency if all its candidates already patched
            if resume and fs_doc:
                if all(c.get("affidavit_url") for c in fs_doc.get("candidates", [])):
                    skipped += 1
                    continue

            consts_done += 1
            print(f"  [{consts_done:>3}] {label:<35} (id={const_id})", end="  ", flush=True)

            try:
                cards = _cards_for_constituency(page, const_id, explore)
            except Exception as e:
                print(f"ERROR scraping: {e}")
                errors += 1
                time.sleep(2)
                continue

            print(f"{len(cards)} candidates found", flush=True)

            if explore and consts_done <= 2:
                for c in cards[:3]:
                    print(f"    {c['name']!r}  constituency={c['constituency']!r}  photo={'yes' if c['photo_url'] else 'no'}")
                # Force-visit first card's show-profile to reveal link structure
                if cards and cards[0].get("view_more_href"):
                    print(f"  Visiting show-profile for {cards[0]['name']!r}…")
                    _get_affidavit_url(context, cards[0]["view_more_href"], explore=True)

            fs_candidates: list[dict] = list((fs_doc or {}).get("candidates", []))
            # Build name→index maps; pop matched entries to prevent duplicate matches
            fs_name_map: dict[str, int] = {_name_key(c["name"]): i for i, c in enumerate(fs_candidates)}
            # Surname map for cross-format matching (CEO TN "VENKATESAN L" ↔ ECI "L.VENKATESAN")
            fs_surname_map: dict[str, int] = {}
            for i, c in enumerate(fs_candidates):
                s = _surname(c["name"])
                if s and s not in fs_surname_map:
                    fs_surname_map[s] = i

            const_patched = 0
            for card in cards:
                key = _name_key(card["name"])
                idx = fs_name_map.get(key)
                if idx is None:
                    # Surname match: longest word (4+ chars) in name
                    card_surname = _surname(card["name"])
                    if card_surname:
                        idx = fs_surname_map.get(card_surname)
                if idx is None:
                    continue

                # Remove from both maps so same Firestore candidate isn't matched twice
                fs_name_map     = {k: v for k, v in fs_name_map.items()     if v != idx}
                fs_surname_map  = {k: v for k, v in fs_surname_map.items()  if v != idx}

                total_matched += 1

                if resume and fs_candidates[idx].get("affidavit_url"):
                    continue

                # Get affidavit from detail page (new tab — keeps list page intact)
                affidavit_url = ""
                if card.get("view_more_href"):
                    affidavit_url = _get_affidavit_url(context, card["view_more_href"], explore)

                eci_data = {
                    "affidavit_url": affidavit_url,
                    "photo_url":     card.get("photo_url", ""),
                }

                patched = _patch_candidate(
                    fs_candidates[idx], eci_data, dry_run,
                    col, slug, fs_candidates, idx
                )
                if patched:
                    const_patched += 1
                    total_patched += 1

            if const_patched:
                print(f"    → {const_patched} patched in {slug}")

            # Mark constituency as done in progress file (for resume)
            if not dry_run and not explore:
                done_ids.add(const_id)
                try:
                    PROGRESS_FILE.write_text(json.dumps({"done": list(done_ids)}))
                except Exception:
                    pass

            if explore and consts_done >= 2:
                print("  (explore: stopping after 2 constituencies)")
                break

            time.sleep(0.5)

        browser.close()

    print(f"\nDone — constituencies={consts_done}  matched={total_matched}  "
          f"patched={total_patched}  skipped={skipped}  errors={errors}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Pull TN 2026 affidavit data from affidavit.eci.gov.in → Firestore"
    )
    ap.add_argument("--explore", action="store_true",
                    help="Log raw data; visit 2 pages + 3 detail pages; no Firestore writes")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print what would be patched; no Firestore writes")
    ap.add_argument("--limit",   type=int, default=None,
                    help="Stop after processing N matched candidates")
    ap.add_argument("--resume",  action="store_true",
                    help="Skip candidates already patched")
    ap.add_argument("--project", default="naatunadappu",
                    help="GCP project ID")
    args = ap.parse_args()
    run(explore=args.explore, dry_run=args.dry_run,
        limit=args.limit, resume=args.resume, project=args.project)


if __name__ == "__main__":
    main()
