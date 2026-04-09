"""
IndiVotes TN 2006 Assembly Election — Pre-Delimitation Electorate Ingest
=========================================================================
Scrapes https://www.indiavotes.com/vidhan-sabha/2006/tamil-nadu/187/40
and writes TWO output files:

  web/src/lib/constituency-pre2008-map.json   — pre-delimitation constituency index
  web/src/lib/constituency-electorate-2006.json — turnout data keyed by pre-delim slug

Column layout (11 cols, no District):
  0: AC No   1: AC Name   2: ECI No   3: Category   4: Winner   5: Party
  6: Total voters   7: Votes cast   8: Turnout%   9: Margin   10: Margin%

Usage:
  .venv/bin/python3 scrapers/indiavotes_2006_ingest.py
"""
from __future__ import annotations

import json
import re
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT       = Path(__file__).resolve().parent.parent
MAP_OUT    = ROOT / "web" / "src" / "lib" / "constituency-pre2008-map.json"
ELEC_OUT   = ROOT / "web" / "src" / "lib" / "constituency-electorate-2006.json"
SOURCE_STR = "IndiaVotes / ECI, Tamil Nadu Assembly Election 2006"
BASE_URL   = "https://www.indiavotes.com/vidhan-sabha/2006/tamil-nadu/187/40"


def _slugify(name: str) -> str:
    """'Dr. Radhakrishnan Nagar' → 'dr_radhakrishnan_nagar'"""
    s = name.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def _parse_num(s: str) -> float | None:
    s = s.replace(",", "").replace("%", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def scrape() -> tuple[dict, dict]:
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

        print("Warming up homepage…")
        page.goto("https://www.indiavotes.com/", wait_until="commit", timeout=20000)
        page.wait_for_timeout(2000)

        print(f"Fetching {BASE_URL}")
        page.goto(BASE_URL, wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(5000)

        rows: list[list[str]] = page.evaluate(r"""
        () => {
            const rows = [];
            for (const tr of document.querySelectorAll('table tbody tr')) {
                const cells = [...tr.querySelectorAll('td')].map(td => td.innerText.trim());
                if (cells.length >= 10 && /^\d+$/.test(cells[0])) rows.push(cells);
            }
            return rows;
        }
        """)
        browser.close()

    print(f"  {len(rows)} data rows extracted")

    pre2008_map: dict[str, dict] = {}
    electorate: dict[str, dict]  = {}
    seen_slugs: set[str] = set()

    for row in rows:
        ac_no    = int(row[0])
        name     = row[1].strip()
        category = row[3].strip()   # GEN / SC / ST
        total    = _parse_num(row[6])
        voted    = _parse_num(row[7])
        turnout  = _parse_num(row[8])   # "69.0 %" → 69.0
        margin   = _parse_num(row[9])

        if total is None:
            continue

        slug = _slugify(name)

        # If slug collides AND it's the same AC number → true bye-election duplicate, skip
        # If same slug but different AC → genuinely different constituency; disambiguate
        if slug in seen_slugs:
            existing_ac = pre2008_map[slug]["ac_no"]
            if existing_ac == ac_no:
                print(f"  DUPE (bye-election): AC{ac_no} {name!r} — skipping")
                continue
            # Different AC, same name → disambiguate by appending AC number
            old_slug = slug
            pre2008_map[f"{old_slug}_{existing_ac}"] = pre2008_map.pop(old_slug)
            pre2008_map[f"{old_slug}_{existing_ac}"]["slug"] = f"{old_slug}_{existing_ac}"
            electorate[f"{old_slug}_{existing_ac}"] = electorate.pop(old_slug)
            seen_slugs.discard(old_slug)
            seen_slugs.add(f"{old_slug}_{existing_ac}")
            slug = f"{slug}_{ac_no}"
            print(f"  COLLISION: {name!r} at AC{existing_ac} and AC{ac_no} → disambiguated")

        seen_slugs.add(slug)

        if turnout is None and total and voted:
            turnout = round((voted / total) * 100, 1)
        elif turnout is not None:
            turnout = round(turnout, 1)

        pre2008_map[slug] = {
            "name":       name.upper(),
            "ac_no":      ac_no,
            "category":   category,
            "slug":       slug,
        }

        entry: dict = {
            "total":  int(total),
            "source": SOURCE_STR,
        }
        if voted   is not None: entry["total_votes"] = int(voted)
        if turnout is not None: entry["poll_pct"]    = turnout
        if margin  is not None: entry["margin"]      = int(margin)

        electorate[slug] = entry

    return pre2008_map, electorate


def main() -> None:
    pre2008_map, electorate = scrape()

    print(f"\n{len(pre2008_map)} pre-delimitation constituencies")

    MAP_OUT.write_text(
        json.dumps(pre2008_map, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Written → {MAP_OUT}")

    ELEC_OUT.write_text(
        json.dumps(electorate, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    print(f"Written → {ELEC_OUT}")


if __name__ == "__main__":
    main()
