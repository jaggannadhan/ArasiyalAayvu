"""
IndiVotes TN 2011 Assembly Election — Electorate & Turnout Ingest
=================================================================
Scrapes https://www.indiavotes.com/vidhan-sabha/2011/tamil-nadu/215/40
(JS-rendered; requires Playwright).

Writes constituency-electorate-2011.json with fields:
  total, total_votes, poll_pct, margin, source

Usage:
  .venv/bin/python3 scrapers/indiavotes_2011_ingest.py --explore
  .venv/bin/python3 scrapers/indiavotes_2011_ingest.py
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT       = Path(__file__).resolve().parent.parent
MAP_PATH   = ROOT / "web" / "src" / "lib" / "constituency-map.json"
OUT_PATH   = ROOT / "web" / "src" / "lib" / "constituency-electorate-2011.json"
BASE_URL   = "https://www.indiavotes.com/vidhan-sabha/2011/tamil-nadu/215/40"
SOURCE_STR = "IndiaVotes / ECI, Tamil Nadu Assembly Election 2011"

# ── slug normalization ──────────────────────────────────────────────────────
def _build_name_slug_map() -> dict[str, str]:
    """Return {normalised_upper_name: slug} from constituency-map.json."""
    cmap = json.loads(MAP_PATH.read_text(encoding="utf-8"))
    result: dict[str, str] = {}
    for slug, meta in cmap.items():
        name = meta.get("name", "").upper().strip()
        result[name] = slug
        # also add without SC/ST suffix variants
        for suffix in [" (SC)", " (ST)", " SC", " ST"]:
            if name.endswith(suffix):
                result[name[: -len(suffix)].strip()] = slug
    return result


def _normalise(name: str) -> str:
    return re.sub(r"\s+", " ", name.strip().upper())


# ── JS extractor ────────────────────────────────────────────────────────────
_EXTRACT_JS = """
() => {
    const rows = [];
    // Row format confirmed:
    // [AC_no, Name, ECI_no, Category, District, Winner, Party, Total_voters, Votes_cast, Turnout%, Margin, Margin%]
    for (const tr of document.querySelectorAll('table tbody tr')) {
        const cells = [...tr.querySelectorAll('td')].map(td => td.innerText.trim());
        if (cells.length < 10) continue;  // skip short alliance/party rows
        rows.push(cells);
    }
    return rows;
}
"""

_PAGE_SOURCE_JS = """
() => document.body.innerText.slice(0, 500)
"""

_TABLE_HTML_JS = """
() => {
    const t = document.querySelector('table');
    return t ? t.outerHTML.slice(0, 3000) : 'NO TABLE';
}
"""


def _parse_number(s: str) -> float | None:
    """Parse '1,23,456' or '78.5%' or '78.5' → float."""
    s = s.replace(",", "").replace("%", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def scrape(explore: bool) -> dict[str, dict]:
    name_slug = _build_name_slug_map()
    results: dict[str, dict] = {}
    unmatched: list[str] = []

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

        # Warm up with homepage
        print("Loading indiavotes homepage…")
        page.goto("https://www.indiavotes.com/", wait_until="commit", timeout=20000)
        page.wait_for_timeout(2000)

        print(f"Fetching: {BASE_URL}")
        page.goto(BASE_URL, wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(4000)

        if explore:
            snippet = page.evaluate(_PAGE_SOURCE_JS)
            print(f"\nPage body snippet:\n{snippet}\n")
            table_html = page.evaluate(_TABLE_HTML_JS)
            print(f"Table HTML:\n{table_html}\n")

        rows: list[list[str]] = page.evaluate(_EXTRACT_JS)
        print(f"Extracted {len(rows)} table rows")

        if explore and rows:
            print("First 5 rows:")
            for r in rows[:5]:
                print(" ", r)

        # Paginate if needed — check for "Next" button
        page_num = 1
        while True:
            next_btn = page.query_selector('a.paginate_button.next:not(.disabled), li.next:not(.disabled) a')
            if not next_btn:
                break
            page_num += 1
            print(f"  → clicking Next (page {page_num})")
            next_btn.click()
            page.wait_for_timeout(2500)
            new_rows: list[list[str]] = page.evaluate(_EXTRACT_JS)
            added = [r for r in new_rows if r not in rows]
            rows.extend(added)
            print(f"  → {len(added)} new rows (total {len(rows)})")
            if not added:
                break

        browser.close()

    # ── Parse rows ────────────────────────────────────────────────────────
    # Expected columns (inspect in explore mode):
    # AC No | AC Name | Total Voters | Votes Cast | Turnout% | Margin | Winner | Party
    # Column indices may vary — we detect by header
    if not rows:
        print("ERROR: No rows extracted. Run with --explore to debug.")
        return {}

    # All rows have ≥10 cells (filtered in JS), so skip any that start with a digit
    # (actual constituency rows start with a number in col 0)
    data_rows = [r for r in rows if r and r[0].isdigit()]

    print(f"Parsing {len(data_rows)} constituency rows…")

    # Name overrides: indiavotes spelling → our constituency-map name
    NAME_OVERRIDE: dict[str, str] = {
        "POONMALLAE":              "POONAMALLEE (SC)",
        "SHOLINGUR":               "SHOLINGHUR",
        "THALLI":                  "THALLY",
        "PALACODU":                "PALACODE",
        "PAPPIREDDIPPATTI":        "PAPPIREDDIPATTI",
        "SALEM (WEST)":            "SALEM WEST",
        "SALEM (NORTH)":           "SALEM NORTH",
        "SALEM (SOUTH)":           "SALEM SOUTH",
        "RASIPURAM":               "RASIPURAM(SC)",
        "SENTHAMANGALAM":          "SENTHAMANGALAM(ST)",
        "ERODE (EAST)":            "ERODE EAST",
        "ERODE (WEST)":            "ERODE WEST",
        "METTUPPALAYAM":           "METTUPALAYAM",
        "TIRUPPUR (NORTH)":        "TIRUPPUR NORTH",
        "TIRUPPUR (SOUTH)":        "TIRUPPUR SOUTH",
        "COIMBATORE (NORTH)":      "COIMBATORE NORTH",
        "COIMBATORE (SOUTH)":      "COIMBATORE SOUTH",
        "TIRUCHIRAPPALLI (WEST)":  "TIRUCHIRAPPALLI WEST",
        "TIRUCHIRAPPALLI (EAST)":  "TIRUCHIRAPPALLI EAST",
        "GANDHARVAKOTTAI":         "GANDARVAKKOTTAI (SC)",
        "TIRUPPATTUR":             "TIRUPPATHUR",   # AC 185, Sivaganga (not Tirupattur AC 50)
        "THOOTHUKKUDI":            "THOOTHUKUDI",
        "SANKARANKOUIL":           "SANKARANKOVIL (SC)",
    }

    for row in data_rows:
        if len(row) < 10:
            continue

        # Confirmed column layout:
        # 0=AC_no 1=Name 2=ECI_no 3=Category 4=District 5=Winner 6=Party
        # 7=Total_voters 8=Votes_cast 9=Turnout% 10=Margin 11=Margin%
        ac_name = _normalise(row[1])
        ac_name = NAME_OVERRIDE.get(ac_name, ac_name)

        total   = _parse_number(row[7])
        voted   = _parse_number(row[8])
        turnout = _parse_number(row[9])   # "82.8 %" → 82.8
        margin  = _parse_number(row[10]) if len(row) > 10 else None

        if not ac_name or total is None:
            continue

        if turnout is None and total and voted:
            turnout = round((voted / total) * 100, 1)
        elif turnout is not None:
            turnout = round(turnout, 1)

        slug = name_slug.get(ac_name)
        if not slug:
            for variant in [
                ac_name.replace(" (SC)", "").replace(" (ST)", "").strip(),
                ac_name + " (SC)",
                ac_name + " (ST)",
                ac_name.replace("(SC)", "").replace("(ST)", "").strip(),
            ]:
                slug = name_slug.get(variant)
                if slug:
                    break

        if not slug:
            unmatched.append(ac_name)
            if explore:
                print(f"  UNMATCHED: {ac_name!r}")
            continue

        entry: dict = {
            "total":  int(total),
            "source": SOURCE_STR,
        }
        if voted is not None:
            entry["total_votes"] = int(voted)
        if turnout is not None:
            entry["poll_pct"] = turnout
        if margin is not None:
            entry["margin"] = int(margin)

        results[slug] = entry

    print(f"\nMatched: {len(results)} / 234")
    if unmatched:
        print(f"Unmatched ({len(unmatched)}): {unmatched}")

    return results


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--explore", action="store_true", help="Print raw page/table for debugging")
    ap.add_argument("--dry-run", action="store_true", help="Print result but do not write file")
    args = ap.parse_args()

    data = scrape(explore=args.explore)
    if not data:
        sys.exit(1)

    if args.dry_run:
        print(json.dumps(dict(list(data.items())[:5]), indent=2))
        return

    OUT_PATH.write_text(
        json.dumps(data, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    print(f"\nWritten → {OUT_PATH}  ({len(data)} entries)")


if __name__ == "__main__":
    main()
