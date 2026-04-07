"""
TN 2026 Candidates Ingest
=========================
Scrapes electionapps.tn.gov.in (CEO Tamil Nadu) for all candidates
contesting in each of the 234 assembly constituencies and writes to
Firestore `candidates_2026` collection.

Doc ID: constituency_slug (matches constituency-map.json)
Source: https://www.electionapps.tn.gov.in/NOM2026/pu_nom/affidavit.aspx

Usage
-----
  .venv/bin/python scrapers/candidates_2026_ingest.py --dry-run --limit 5
  .venv/bin/python scrapers/candidates_2026_ingest.py
  .venv/bin/python scrapers/candidates_2026_ingest.py --resume
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
COLLECTION   = "candidates_2026"
BASE_URL     = "https://www.electionapps.tn.gov.in/NOM2026/pu_nom/affidavit.aspx"
HEADERS      = {
    "User-Agent": "Mozilla/5.0 (compatible; ArasiyalAayvuResearchBot/2.0)",
    "Referer":    BASE_URL,
    "Origin":     "https://www.electionapps.tn.gov.in",
}
MAP_PATH     = ROOT / "web" / "src" / "lib" / "constituency-map.json"
NOW_ISO      = datetime.now(timezone.utc).isoformat()

# Normalise CEO TN full party names → our party_id codes
PARTY_NORMALISE: dict[str, str] = {
    "dravida munnetra kazhagam":                        "dmk",
    "all india anna dravida munnetra kazhagam":         "aiadmk",
    "bharatiya janata party":                           "bjp",
    "indian national congress":                         "inc",
    "pattali makkal katchi":                            "pmk",
    "communist party of india":                         "cpi",
    "communist party of india  (marxist)":              "cpim",
    "communist party of india (marxist)":               "cpim",
    "viduthalai chiruthaigal katchi":                   "vck",
    "desiya murpokku dravida kazhagam":                 "dmdk",
    "marumalarchi dravida munnetra kazhagam":           "mdmk",
    "naam tamilar katchi":                              "ntk",
    "tamilaga vettri kazhagam":                         "tvk",
    "bahujan samaj party":                              "bsp",
    "independent":                                      "ind",
    "none of the above":                                "nota",
}

def _party_id(raw: str) -> str:
    return PARTY_NORMALISE.get(raw.strip().lower(), _slugify(raw)[:12])

def _slugify(v: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]", "_", v.lower())).strip("_")

# ---------------------------------------------------------------------------
# Build AC number → constituency slug map from constituency-map.json
# ---------------------------------------------------------------------------
def _build_ac_slug_map() -> dict[int, str]:
    cmap = json.loads(MAP_PATH.read_text(encoding="utf-8"))
    return {
        meta["constituency_id"]: slug
        for slug, meta in cmap.items()
        if isinstance(meta.get("constituency_id"), int)
    }

# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------
def _fetch_viewstate(session: requests.Session) -> dict[str, str]:
    """Fetch the form page once and extract ASP.NET hidden fields."""
    resp = session.get(BASE_URL, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    fields = {}
    for name in ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"]:
        tag = soup.find("input", {"name": name})
        if tag:
            fields[name] = tag.get("value", "")
    return fields


def _fetch_candidates_for_ac(
    session: requests.Session,
    ac_num: int,
    viewstate: dict[str, str],
    date_str: str = "04-04-2026",
) -> list[dict[str, Any]]:
    data = {
        **viewstate,
        "DropDownList1":   str(ac_num),
        "DropDownList2":   date_str,
        "Button1":         "View",
        "__EVENTTARGET":   "",
        "__EVENTARGUMENT": "",
    }
    resp = session.post(BASE_URL, headers=HEADERS, data=data, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    candidates = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        header = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th","td"])]
        if "party" not in " ".join(header) and "candidate" not in " ".join(header):
            continue
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 4:
                continue
            texts = [c.get_text(" ", strip=True) for c in cells]
            # Columns vary slightly; detect by content
            name  = texts[1] if len(texts) > 1 else texts[0]
            gender = texts[2] if len(texts) > 2 else ""
            party  = texts[3] if len(texts) > 3 else ""
            nom_no = texts[4] if len(texts) > 4 else ""
            nom_dt = texts[5] if len(texts) > 5 else ""

            if not name or name.lower() in ("name of the candidate", "candidate", "sl.no"):
                continue

            candidates.append({
                "name":              re.sub(r"\s+", " ", name).strip(),
                "party":             re.sub(r"\s+", " ", party).strip(),
                "party_id":          _party_id(party),
                "gender":            gender.strip(),
                "nomination_number": nom_no.strip(),
                "nomination_date":   nom_dt.strip(),
            })
        if candidates:
            break

    return candidates


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run(dry_run: bool, limit: int | None, resume: bool, project: str) -> None:
    ac_slug_map = _build_ac_slug_map()
    ac_numbers  = sorted(ac_slug_map.keys())
    if limit:
        ac_numbers = ac_numbers[:limit]

    db = None
    col = None
    existing: set[str] = set()
    if not dry_run:
        if firestore is None:
            sys.exit("ERROR: google-cloud-firestore not installed")
        db  = firestore.Client(project=project)
        col = db.collection(COLLECTION)
        if resume:
            existing = {d.id for d in col.stream()}
            print(f"[resume] {len(existing)} docs already exist")

    session = requests.Session()
    print("Fetching ViewState tokens…")
    viewstate = _fetch_viewstate(session)
    print(f"  Got {len(viewstate)} tokens")

    written = skipped = errors = 0

    for ac_num in ac_numbers:
        slug = ac_slug_map[ac_num]
        if slug in existing:
            skipped += 1
            continue

        print(f"  AC {ac_num:>3}  {slug}", end="  ", flush=True)
        try:
            candidates = _fetch_candidates_for_ac(session, ac_num, viewstate)
        except Exception as exc:
            print(f"ERROR: {exc}")
            errors += 1
            time.sleep(2)
            continue

        print(f"→ {len(candidates)} candidates")

        if dry_run:
            for c in candidates[:3]:
                print(f"      {c['name']} | {c['party']} | {c['gender']}")
        else:
            doc = {
                "constituency_slug": slug,
                "ac_number":         ac_num,
                "candidates":        candidates,
                "total_candidates":  len(candidates),
                "election_year":     2026,
                "_source":           BASE_URL,
                "_scraped_at":       NOW_ISO,
            }
            col.document(slug).set(doc)  # type: ignore[union-attr]
            written += 1

        time.sleep(0.5)

    print(f"\nDone — written={written} skipped={skipped} errors={errors}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest TN 2026 candidates from CEO TN portal")
    ap.add_argument("--dry-run",  action="store_true")
    ap.add_argument("--limit",    type=int, default=None)
    ap.add_argument("--resume",   action="store_true")
    ap.add_argument("--project",  default="naatunadappu")
    args = ap.parse_args()
    run(dry_run=args.dry_run, limit=args.limit, resume=args.resume, project=args.project)


if __name__ == "__main__":
    main()
