"""
ADR Assets Ingest
=================
Parses movable and immovable asset values from the ADR Tamil Nadu 2021 sitting
MLAs PDF text and patches `movable_assets_cr` / `immovable_assets_cr` into the
`candidate_accountability` Firestore collection.

Source PDF (same as adr_criminal_ingest.py):
  https://adrindia.org/sites/default/files/Tamil_Nadu_Assembly_Election_2021_Sitting_MLAs_Report_Finalver_Eng.pdf

Usage:
  # Extract PDF text first (one-time, same file):
  pdftotext <pdf_path> /tmp/adr_tn2021.txt

  # Dry-run (print what would be written):
  .venv/bin/python scrapers/adr_assets_ingest.py --dry-run

  # Live ingest:
  .venv/bin/python scrapers/adr_assets_ingest.py

  # Save parsed JSON for inspection:
  .venv/bin/python scrapers/adr_assets_ingest.py --export-json /tmp/adr_assets.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR   = Path(__file__).resolve().parents[1]
MAP_PATH   = ROOT_DIR / "web" / "src" / "lib" / "constituency-map.json"
PDF_TEXT   = Path("/tmp/adr_tn2021.txt")

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "naatunadappu")
NOW_ISO    = datetime.now(timezone.utc).isoformat()

# ─────────────────────────────────────────────────────────────────────────────
# Regex helpers
# ─────────────────────────────────────────────────────────────────────────────
AMOUNT       = re.compile(r'^[\d,]+\*?$')
AMOUNT_ZERO  = re.compile(r'^([\d,]+\*?|0)$')
CRORE_LABEL  = re.compile(r'^\d+ (Crore|Lacs)\+$')
HEADER_LINE  = re.compile(
    r'^(Movable Assets|Total Assets|S\.No\.|Name$|District$|Constituency$|'
    r'Party Name$|Age$|\(Rs\)$|PAN$|Given$|Data in|analysis\.|Website|Y$|N$|'
    r'Page \d|Full Assets)',
    re.IGNORECASE,
)
STOP_RE = re.compile(
    r'^(S\.No\.|Movable Assets|PAN$|Given$|Page \d|Data in|Website)',
    re.IGNORECASE,
)


# ─────────────────────────────────────────────────────────────────────────────
# Text helpers
# ─────────────────────────────────────────────────────────────────────────────

def _skip_blanks(lines: list[str], i: int) -> int:
    j = i + 1
    while j < len(lines) and not lines[j].strip():
        j += 1
    return j


def _skip_blanks_and_star(lines: list[str], i: int) -> int:
    """Skip blank lines and standalone '*' annotation lines."""
    j = i + 1
    while j < len(lines):
        l = lines[j].strip()
        if not l or l == '*':
            j += 1
            continue
        break
    return j


def _parse_cr(s: str) -> float | None:
    """Convert Indian-format rupee string to crore float. Returns None on failure."""
    if not s:
        return None
    cleaned = s.replace(',', '').rstrip('*')
    if not cleaned.isdigit():
        return None
    return round(int(cleaned) / 1e7, 4)


# ─────────────────────────────────────────────────────────────────────────────
# Core parser
# ─────────────────────────────────────────────────────────────────────────────

def parse_assets(pdf_text_path: Path) -> list[dict]:
    """
    Parse all 218 sitting-MLA asset entries from the ADR text file.

    Returns a list of dicts:
      {sno, name, district, constituency, movable_cr, immovable_cr}
    """
    raw_lines = pdf_text_path.read_text(encoding="utf-8", errors="replace").splitlines()

    # Locate "Full Assets Details of sitting MLAs" section
    start = next(
        (i for i, l in enumerate(raw_lines) if l.strip() == "Full Assets Details of sitting MLAs"),
        None,
    )
    end = next(
        (i for i, l in enumerate(raw_lines) if "Table: Full assets Details of Sitting MLAs" in l),
        None,
    )
    if start is None or end is None:
        raise ValueError("Could not locate Full Assets section in PDF text.")

    lines = raw_lines[start:end]

    # ── Step 1: Extract name blocks + inline assets ───────────────────────────
    mla_entries: dict[int, dict] = {}

    i = 0
    while i < len(lines):
        l = lines[i].strip().lstrip('\x0c')
        if not re.match(r'^\d{1,3}$', l):
            i += 1
            continue

        sno = int(l)
        if not (1 <= sno <= 250):
            i += 1
            continue

        j = _skip_blanks(lines, i)
        name = lines[j].strip() if j < len(lines) else ""

        # Validate name (has letters, not a header or number)
        if not name or not re.search(r'[A-Za-z]', name):
            i += 1
            continue
        if HEADER_LINE.match(name) or AMOUNT.match(name):
            i += 1
            continue

        # Consume all name-continuation lines (lowercase or dot-letter suffix)
        # e.g. "Ebenezer. J.J. Alias John" followed by "Ebenezer.J" continuation
        jj = j
        while True:
            kk = _skip_blanks(lines, jj)
            candidate = lines[kk].strip() if kk < len(lines) else ""
            # A name continuation has lowercase letters (district names are ALL CAPS)
            if candidate and re.search(r'[a-z]', candidate) and not HEADER_LINE.match(candidate):
                jj = kk  # skip this continuation line
            else:
                break

        k = _skip_blanks(lines, jj)
        district = lines[k].strip() if k < len(lines) else ""
        m = _skip_blanks(lines, k)
        constituency = lines[m].strip() if m < len(lines) else ""
        # Strip bye-election annotations like ": BYE ELECTION ON"
        constituency = re.sub(r'\s*:.*$', '', constituency).strip()
        p = _skip_blanks(lines, m)

        # Find age (1–2 digit integer), skipping multi-line party name
        age_i = p + 1
        while age_i < len(lines):
            ql = lines[age_i].strip()
            if not ql:
                age_i += 1
                continue
            if re.match(r'^\d{1,2}$', ql):
                break
            if HEADER_LINE.match(ql) or AMOUNT.match(ql):
                age_i = p
                break
            age_i += 1

        # Check for inline assets (2 numbers after age, no crore label)
        a1 = _skip_blanks(lines, age_i)
        v1 = lines[a1].strip() if a1 < len(lines) else ""
        a2 = _skip_blanks(lines, a1)
        v2 = lines[a2].strip() if a2 < len(lines) else ""
        a3 = _skip_blanks(lines, a2)
        v3 = lines[a3].strip() if a3 < len(lines) else ""

        inline_movable = inline_immovable = None
        if AMOUNT.match(v1) and AMOUNT_ZERO.match(v2) and not CRORE_LABEL.match(v3):
            inline_movable = v1
            inline_immovable = v2

        if sno not in mla_entries:
            mla_entries[sno] = {
                "sno": sno,
                "name": name,
                "district": district,
                "constituency": constituency,
                "movable_s": inline_movable,
                "immovable_s": inline_immovable,
            }

        i = a2 + 1 if inline_movable else m + 1

    # ── Step 2: Extract two-column asset triplets ─────────────────────────────
    # After each "Movable Assets Immovable Assets" header, collect 4-line groups
    # (total / movable / immovable / crore-label) until a non-asset boundary.

    asset_headers = [
        idx for idx, l in enumerate(lines)
        if 'Movable Assets Immovable Assets' in l
    ]

    two_col_assets: list[tuple[str, str, str]] = []

    for h in asset_headers:
        j = h + 4  # skip header + 3 column-header lines
        while j < len(lines):
            l = lines[j].strip().lstrip('\x0c')
            if not l:
                j += 1
                continue
            if l in ('Y', 'N'):
                j += 1
                continue
            if STOP_RE.match(l) or CRORE_LABEL.match(l):
                if CRORE_LABEL.match(l):
                    j += 1
                    continue
                break
            if AMOUNT.match(l):
                # Collect triplet; skip standalone '*' between values
                j2 = _skip_blanks_and_star(lines, j)
                mov_s = lines[j2].strip() if j2 < len(lines) else ""
                j3 = _skip_blanks_and_star(lines, j2)
                imm_s = lines[j3].strip().rstrip('*') if j3 < len(lines) else ""
                j4 = _skip_blanks_and_star(lines, j3)
                lbl = lines[j4].strip() if j4 < len(lines) else ""

                if AMOUNT.match(mov_s) and AMOUNT_ZERO.match(imm_s) and CRORE_LABEL.match(lbl):
                    two_col_assets.append((l, mov_s, imm_s))
                    j = j4 + 1
                else:
                    j += 1
            else:
                j += 1

    # ── Step 3: Assign two-column assets to entries lacking inline ────────────
    no_inline = sorted(sno for sno, v in mla_entries.items() if v["movable_s"] is None)

    if len(two_col_assets) != len(no_inline):
        print(
            f"⚠  two-column count mismatch: {len(two_col_assets)} assets "
            f"for {len(no_inline)} entries without inline",
            file=sys.stderr,
        )

    for sno, (_, mov_s, imm_s) in zip(no_inline, two_col_assets):
        mla_entries[sno]["movable_s"] = mov_s
        mla_entries[sno]["immovable_s"] = imm_s

    # ── Step 4: Build final list with crore values ────────────────────────────
    result = []
    for sno in sorted(mla_entries.keys()):
        v = mla_entries[sno]
        movable_cr   = _parse_cr(v.get("movable_s") or "")
        immovable_cr = _parse_cr(v.get("immovable_s") or "")
        result.append({
            "sno": sno,
            "name": v["name"],
            "district": v["district"],
            "constituency": v["constituency"],
            "movable_cr": movable_cr,
            "immovable_cr": immovable_cr,
        })

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Constituency slug lookup
# ─────────────────────────────────────────────────────────────────────────────

def _build_slug_map(map_path: Path) -> dict[str, str]:
    """
    Returns uppercase-constituency-name → slug mapping.
    e.g. "ANNA NAGAR" → "anna_nagar"
    """
    with open(map_path, encoding="utf-8") as f:
        data = json.load(f)

    slug_map: dict[str, str] = {}
    for slug, meta in data.items():
        name_upper = meta.get("name", "").upper().strip()
        if name_upper:
            slug_map[name_upper] = slug

    return slug_map


# Manual overrides for spelling differences between ADR text and constituency map
CONSTITUENCY_ALIASES: dict[str, str] = {
    "VILLUPPURAM": "villupuram",       # ADR double-P vs map single-P
    "DR. RADHAKRISHNAN NAGAR": "dr_radhakrishnan_nagar",
    "DR.RADHAKRISHNAN NAGAR":  "dr_radhakrishnan_nagar",
}


def _normalize_constituency(raw: str) -> str:
    """Strip trailing reserved-category suffixes and normalise spacing."""
    # Remove "(SC)" "(ST)" parenthetical but preserve other suffixes like "EAST" "NORTH"
    return re.sub(r'\s*\((?:SC|ST)\)\s*$', '', raw.strip().upper())


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Patch movable/immovable assets into candidate_accountability")
    ap.add_argument("--dry-run", action="store_true", help="Print actions without writing to Firestore")
    ap.add_argument("--export-json", metavar="PATH", help="Write parsed data to a JSON file and exit")
    ap.add_argument("--pdf-text", metavar="PATH", default=str(PDF_TEXT),
                    help=f"Path to pdftotext output (default: {PDF_TEXT})")
    args = ap.parse_args()

    pdf_path = Path(args.pdf_text)
    if not pdf_path.exists():
        sys.exit(f"PDF text file not found: {pdf_path}\nRun: pdftotext <pdf> {pdf_path}")

    print(f"Parsing assets from {pdf_path} …")
    assets = parse_assets(pdf_path)
    print(f"Parsed {len(assets)} entries.")

    if args.export_json:
        out = Path(args.export_json)
        out.write_text(json.dumps(assets, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Exported to {out}")
        return

    # Build constituency → slug mapping
    slug_map = _build_slug_map(MAP_PATH)

    # Resolve slug for each entry
    unmatched = []
    matched = []
    for entry in assets:
        raw = entry["constituency"]
        raw_upper = raw.upper().strip()
        # 1. Manual alias override
        slug = CONSTITUENCY_ALIASES.get(raw_upper)
        # 2. Exact match against map
        if not slug:
            slug = slug_map.get(raw_upper)
        # 3. Normalised (strip SC/ST suffix)
        if not slug:
            slug = slug_map.get(_normalize_constituency(raw))
        if slug:
            entry["constituency_slug"] = slug
            matched.append(entry)
        else:
            entry["constituency_slug"] = None
            unmatched.append(entry)

    print(f"Matched:   {len(matched)}")
    print(f"Unmatched: {len(unmatched)}")
    if unmatched:
        print("Unmatched constituencies:")
        for e in unmatched:
            print(f"  {e['sno']}. {e['name']} | {e['constituency']}")

    if args.dry_run:
        print("\n[DRY RUN] Would write:")
        for e in matched:
            print(
                f"  {e['constituency_slug']:30s}  "
                f"movable={e['movable_cr']} Cr  immovable={e['immovable_cr']} Cr"
            )
        return

    # Live ingest via Firestore
    from google.cloud import firestore

    db = firestore.Client(project=PROJECT_ID)
    col = db.collection("candidate_accountability")

    written = skipped = errors = 0
    for entry in matched:
        slug = entry["constituency_slug"]
        doc_ref = col.document(f"2021_{slug}")
        snap = doc_ref.get()
        if not snap.exists:
            print(f"  ⚠  No Firestore doc for 2021_{slug} ({entry['name']})")
            skipped += 1
            continue
        mov = entry["movable_cr"] or 0.0
        imm = entry["immovable_cr"] or 0.0
        total = round(mov + imm, 4)
        patch = {
            "movable_assets_cr":   entry["movable_cr"],
            "immovable_assets_cr": entry["immovable_cr"],
            "assets_cr":           total,
            "assets_patched_at":   NOW_ISO,
            "assets_source":       "ADR TN 2021 Sitting MLAs Report",
        }
        try:
            doc_ref.set(patch, merge=True)
            print(f"  ✓  2021_{slug}: M={entry['movable_cr']} I={entry['immovable_cr']}")
            written += 1
        except Exception as exc:
            print(f"  ✗  {slug}: {exc}")
            errors += 1

    print(f"\nDone — written={written} skipped={skipped} errors={errors}")


if __name__ == "__main__":
    main()
