"""
myneta_2026_enrich.py — Enrich politician_profile 2026 timeline entries
with data from the MyNeta SQLite database (theGtheOAT/Politics repo).

Reads the SQLite DB at a given path, matches candidates to existing
politician_profile docs by constituency_slug + name, and patches the
2026 timeline entry with:
  - education, education_tier
  - assets_cr, movable_assets_cr, immovable_assets_cr
  - liabilities_cr, net_assets_cr, is_crorepati
  - criminal_cases[], criminal_cases_total, criminal_severity
  - source_url (myneta link)

Also updates top-level profile summary fields when 2026 is the latest year.

Usage:
  .venv/bin/python3 scrapers/myneta_2026_enrich.py --dry-run
  .venv/bin/python3 scrapers/myneta_2026_enrich.py --dry-run --only kolathur
  .venv/bin/python3 scrapers/myneta_2026_enrich.py
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scrapers"))

from google.cloud import firestore
from name_utils import canonical_name as make_canonical

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PROJECT    = "naatunadappu"
COLLECTION = "politician_profile"
DEFAULT_DB = "/tmp/Politics/output/Candidates_Final.db"

# 3 constituency slugs that don't match between MyNeta and our Firestore
SLUG_ALIASES = {
    "drradhakrishnan_nagar": "dr_radhakrishnan_nagar",
    "madhuravoyal": "maduravoyal",
    "thiruvarur": "thiruvaur",
}

# Serious IPC sections — mirrors adr_criminal_ingest.py
SERIOUS_IPC = {
    "121", "121A", "122", "123", "124A", "120B",
    "147", "148", "153A", "153B",
    "302", "303", "304", "304B", "306", "307", "308",
    "326", "326A", "326B",
    "354A", "354B", "354C", "354D",
    "363", "363A", "364", "364A", "365", "366", "366A", "366B",
    "370", "370A", "371", "372", "373",
    "375", "376", "376A", "376B", "376C", "376D", "376E", "377",
    "384", "385", "386", "387",
    "392", "393", "394", "395", "396", "397", "398", "399",
    "406", "409", "420",
    "436", "437", "438", "467", "468", "498A",
}

# BNS equivalents of serious IPC sections (2023 Bharatiya Nyaya Sanhita)
SERIOUS_BNS = {
    "101", "103", "104", "105", "108", "109", "110",  # murder/attempt
    "115", "117", "118", "119",  # hurt/grievous
    "63", "64", "65", "66",  # sexual offences
    "70", "71", "72", "73", "74", "75", "76", "77", "78",  # kidnapping/trafficking
    "87", "88", "89", "90",  # robbery/dacoity
    "191", "192",  # rioting
    "196", "197",  # promoting enmity
    "308", "309", "310", "311",  # forgery
    "316", "318",  # cheating
    "85", "86",  # cruelty to wife / dowry death
}


# ---------------------------------------------------------------------------
# Helpers — parse MyNeta raw strings into our schema types
# ---------------------------------------------------------------------------
def _parse_cr(raw: str) -> float | None:
    """Parse 'Rs 9,70,24,938 ~9 Crore+' → 9.7025 (float in crores)."""
    if not raw:
        return None
    text = re.sub(r"\s+", " ", raw).strip().lower()
    if any(k in text for k in ["nil", "n/a", "none", "not given"]):
        return 0.0
    rs = re.search(r"rs\.?\s*([\d,]+(?:\.\d+)?)", text, re.IGNORECASE)
    if rs:
        try:
            return round(float(rs.group(1).replace(",", "")) / 1_00_00_000, 4)
        except ValueError:
            pass
    return None


def _education_tier(raw: str) -> str:
    """Classify education string into tier. Mirrors ceo_tn_mla_ingest.py."""
    edu = (raw or "").lower()
    if any(t in edu for t in ["phd", "doctorate", "d.litt"]):
        return "Doctorate"
    if any(t in edu for t in ["post graduate", "pg", "mba", "m.a", "m.sc",
                               "m.com", "m.e", "m.tech", "llm", "m.phil"]):
        return "Postgraduate"
    if any(t in edu for t in ["graduate", "b.a", "b.sc", "b.com", "b.e",
                               "b.tech", "mbbs", "llb", "b.ed", "b.d.s",
                               "diploma", "professional"]):
        return "Graduate"
    if any(t in edu for t in ["12th", "hsc", "intermediate"]):
        return "Class XII"
    if any(t in edu for t in ["10th", "sslc", "matriculat"]):
        return "Class X"
    if any(t in edu for t in ["8th", "primary", "5th",
                               "illiterate", "literate"]):
        return "Below Class X"
    return "Others"


def _name_key(name: str) -> str:
    """Strip all non-alphanumeric chars and uppercase for matching."""
    return re.sub(r"[^A-Z0-9]", "", name.upper())


def _name_words_key(name: str) -> str:
    """Extract long words (>=3 chars), sorted, as matching key.
    Strips initials and titles so 'Stalin M.K.' and 'M. K. STALIN' both → 'STALIN'."""
    clean = re.sub(r"[^A-Za-z ]", " ", name).upper().split()
    words = sorted(w for w in clean if len(w) >= 3
                   and w not in ("DR", "MR", "MRS", "MS", "SMT", "THIRU"))
    return "".join(words)


def _extract_movable_total(assets: list[tuple]) -> float | None:
    """Find the movable 'Gross Total' row and parse the last column."""
    for _, atype, row_json in assets:
        if atype != "Movable":
            continue
        try:
            row = json.loads(row_json)
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(row, list) and row and "gross total" in str(row[0]).lower():
            # Last column has the combined total
            return _parse_cr(str(row[-1]))
    return None


def _extract_immovable_total(assets: list[tuple], total_assets: float | None,
                              movable: float | None) -> float | None:
    """Immovable = total - movable (no explicit total row for immovable)."""
    if total_assets is not None and movable is not None:
        imm = round(total_assets - movable, 4)
        return imm if imm >= 0 else None
    return None


def _parse_criminal_cases(cases: list[tuple], ipc_briefs: list[str]) -> list[dict]:
    """Parse criminal_cases rows into our CriminalCase schema."""
    parsed = []
    for _, case_type, row_json in cases:
        try:
            row = json.loads(row_json)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(row, list) or len(row) < 6:
            continue
        # Skip header rows and "No Cases" rows
        first = str(row[0]).strip().lower()
        if first in ("serial no.", "serial no", "") or "no cases" in first:
            continue

        law_type = str(row[4]).strip() if len(row) > 4 else ""   # "IPC" or "BNS"
        sections_raw = str(row[5]).strip() if len(row) > 5 else ""
        sections = [s.strip() for s in re.split(r"[,\s]+", sections_raw) if s.strip()]

        is_serious = False
        if law_type.upper() == "BNS":
            is_serious = any(s in SERIOUS_BNS for s in sections)
        else:
            is_serious = any(s in SERIOUS_IPC for s in sections)

        court = str(row[3]).strip() if len(row) > 3 else ""
        fir_no = str(row[1]).strip() if len(row) > 1 else ""
        case_no = str(row[2]).strip() if case_type == "Pending" and len(row) > 2 else ""

        parsed.append({
            "act": law_type or "IPC",
            "ipc_sections": sections,
            "description": "",  # filled below from brief_ipc
            "status": case_type,  # "Pending" or "Convicted"
            "court": court or None,
            "fir_no": fir_no or None,
            "case_no": case_no or None,
            "is_serious": is_serious,
        })

    # Attach brief_ipc descriptions to cases
    for brief in ipc_briefs:
        # briefs are like "2 charges related to Being member of unlawful assembly. (BNS Section-189(2))"
        for case in parsed:
            if not case["description"]:
                # Match by section number
                for sec in case["ipc_sections"]:
                    if sec in brief:
                        case["description"] = brief
                        break

    return parsed


def _compute_severity(cases: list[dict]) -> str:
    if not cases:
        return "CLEAN"
    if any(c["is_serious"] for c in cases):
        return "SERIOUS"
    if len(cases) >= 3:
        return "MODERATE"
    return "MINOR"


def myneta_constituency_to_slug(constituency: str) -> str:
    """Convert 'KOLATHUR (CHENNAI)' → 'kolathur', 'ARAKKONAM (SC) (RANIPET)' → 'arakkonam_sc'."""
    parts = re.findall(r"\(([^)]+)\)", constituency)
    base = constituency.split("(")[0].strip()
    qualifiers = [p.strip().lower() for p in parts[:-1]] if len(parts) >= 2 else []
    slug = base.lower().strip().replace(" ", "_").replace("-", "_").replace(".", "")
    for q in qualifiers:
        slug += "_" + q.replace(" ", "_")
    return SLUG_ALIASES.get(slug, slug)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB, help="Path to Candidates_Final.db")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", help="Comma-separated constituency slugs to process")
    args = ap.parse_args()

    # ── 1. Load MyNeta SQLite data ────────────────────────────────────────────
    print(f"Loading MyNeta DB from {args.db}...")
    conn = sqlite3.connect(args.db)

    candidates = conn.execute(
        "SELECT id, name, constituency, party, age, education, "
        "total_assets, total_liabilities, criminal_cases_count, url "
        "FROM candidates"
    ).fetchall()
    print(f"  {len(candidates)} candidates loaded")

    # Build lookup: slug → list of (name_key, words_key, candidate_data)
    myneta_by_slug: dict[str, list[tuple[str, str, dict]]] = {}
    for row in candidates:
        cid, name, constituency, party, age, edu, assets, liabs, crim_count, url = row
        slug = myneta_constituency_to_slug(constituency)

        # Fetch sub-table data
        asset_rows = conn.execute(
            "SELECT id, asset_type, row_json FROM assets WHERE candidate_id = ?", (cid,)
        ).fetchall()
        criminal_rows = conn.execute(
            "SELECT id, case_type, row_json FROM criminal_cases WHERE candidate_id = ?", (cid,)
        ).fetchall()
        ipc_rows = conn.execute(
            "SELECT details FROM brief_ipc WHERE candidate_id = ?", (cid,)
        ).fetchall()

        assets_cr = _parse_cr(assets)
        liabilities_cr = _parse_cr(liabs)
        movable_cr = _extract_movable_total(asset_rows)
        immovable_cr = _extract_immovable_total(asset_rows, assets_cr, movable_cr)
        net_cr = round(assets_cr - liabilities_cr, 4) if assets_cr is not None and liabilities_cr is not None else None

        ipc_briefs = [r[0] for r in ipc_rows]
        criminal_cases = _parse_criminal_cases(criminal_rows, ipc_briefs)

        data = {
            "name": name,
            "education": edu,
            "education_tier": _education_tier(edu),
            "assets_cr": assets_cr,
            "movable_assets_cr": movable_cr,
            "immovable_assets_cr": immovable_cr,
            "liabilities_cr": liabilities_cr,
            "net_assets_cr": net_cr,
            "is_crorepati": (assets_cr or 0) >= 1.0,
            "criminal_cases": criminal_cases,
            "criminal_cases_total": int(crim_count) if crim_count else 0,
            "criminal_severity": _compute_severity(criminal_cases),
            "source_url": url,
        }

        myneta_by_slug.setdefault(slug, []).append((_name_key(name), _name_words_key(name), data))

    print(f"  {len(myneta_by_slug)} constituencies indexed")

    # ── 2. Load politician_profile docs with 2026 timeline entries ────────────
    print("Loading politician_profile docs from Firestore...")
    db = firestore.Client(project=PROJECT)
    col = db.collection(COLLECTION)

    target_slugs = None
    if args.only:
        target_slugs = set(s.strip() for s in args.only.split(","))

    all_docs = list(col.stream())
    print(f"  {len(all_docs)} total docs")

    # ── 3. Match and enrich ───────────────────────────────────────────────────
    matched = 0
    unmatched = 0
    skipped = 0
    updated = 0
    batch = db.batch()
    batch_count = 0
    unmatched_names: list[str] = []

    for doc in all_docs:
        d = doc.to_dict()
        timeline = d.get("timeline", [])

        # Find the 2026 timeline entry
        tl_2026_idx = None
        for i, t in enumerate(timeline):
            if t.get("year") == 2026:
                tl_2026_idx = i
                break

        if tl_2026_idx is None:
            continue  # no 2026 entry

        tl = timeline[tl_2026_idx]
        slug = tl.get("constituency_slug", "")

        if target_slugs and slug not in target_slugs:
            skipped += 1
            continue

        # Already enriched?
        if tl.get("assets_cr") is not None:
            skipped += 1
            continue

        # Find matching MyNeta candidate
        myneta_list = myneta_by_slug.get(slug, [])
        if not myneta_list:
            unmatched += 1
            unmatched_names.append(f"{d.get('canonical_name')} ({slug}) — no MyNeta data for constituency")
            continue

        profile_name = d.get("canonical_name", "")
        profile_key = _name_key(profile_name)
        profile_words_key = _name_words_key(profile_name)

        match_data = None

        # Tier 1: exact alphanumeric key match
        for mk, wk, mdata in myneta_list:
            if mk == profile_key:
                match_data = mdata
                break

        # Tier 2: words-only key match (strips initials/titles)
        if not match_data and profile_words_key:
            words_matches = [mdata for _, wk, mdata in myneta_list if wk == profile_words_key]
            if len(words_matches) == 1:
                match_data = words_matches[0]

        # Tier 3: surname (longest word) match — only accept if unique in constituency
        if not match_data:
            profile_words = [w for w in re.sub(r"[^A-Z ]", "", profile_name.upper()).split() if len(w) >= 4]
            profile_surname = max(profile_words, key=len) if profile_words else ""
            if profile_surname:
                surname_matches = []
                for _, _, mdata in myneta_list:
                    myneta_words = [w for w in re.sub(r"[^A-Z ]", "", mdata["name"].upper()).split() if len(w) >= 4]
                    myneta_surname = max(myneta_words, key=len) if myneta_words else ""
                    if profile_surname == myneta_surname:
                        surname_matches.append(mdata)
                if len(surname_matches) == 1:
                    match_data = surname_matches[0]

        if not match_data:
            unmatched += 1
            unmatched_names.append(f"{profile_name} ({slug})")
            continue

        matched += 1

        # ── 4. Build the update ───────────────────────────────────────────────
        tl_update = {
            "education": match_data["education"],
            "education_tier": match_data["education_tier"],
            "assets_cr": match_data["assets_cr"],
            "movable_assets_cr": match_data["movable_assets_cr"],
            "immovable_assets_cr": match_data["immovable_assets_cr"],
            "liabilities_cr": match_data["liabilities_cr"],
            "net_assets_cr": match_data["net_assets_cr"],
            "is_crorepati": match_data["is_crorepati"],
            "criminal_cases": match_data["criminal_cases"],
            "criminal_cases_total": match_data["criminal_cases_total"],
            "criminal_severity": match_data["criminal_severity"],
            "source_url": match_data["source_url"],
        }

        # Apply to timeline entry
        timeline[tl_2026_idx].update(tl_update)

        # Update top-level fields if 2026 is latest year
        latest_year = max((t.get("year") or 0) for t in timeline)
        top_update: dict[str, Any] = {"timeline": timeline}
        if latest_year == 2026:
            top_update["education"] = match_data["education_tier"]
            top_update["total_assets_cr"] = match_data["assets_cr"]
            top_update["total_liabilities_cr"] = match_data["liabilities_cr"]
            top_update["net_assets_cr"] = match_data["net_assets_cr"]
            top_update["criminal_cases_total"] = match_data["criminal_cases_total"]
            top_update["criminal_severity"] = match_data["criminal_severity"]

        if args.dry_run:
            if matched <= 5:
                print(f"  [DRY] {profile_name} ({slug})")
                print(f"    matched → {match_data['name']}")
                print(f"    assets={match_data['assets_cr']} liab={match_data['liabilities_cr']} "
                      f"edu={match_data['education_tier']} criminal={match_data['criminal_cases_total']} "
                      f"severity={match_data['criminal_severity']}")
        else:
            batch.update(doc.reference, top_update)
            batch_count += 1
            if batch_count >= 400:
                batch.commit()
                batch = db.batch()
                updated += batch_count
                batch_count = 0
                print(f"  {updated} docs updated...")

    # Flush remaining batch
    if not args.dry_run and batch_count > 0:
        batch.commit()
        updated += batch_count

    # ── 5. Report ─────────────────────────────────────────────────────────────
    print(f"\n{'[DRY RUN] ' if args.dry_run else ''}Results:")
    print(f"  Matched & enriched: {matched}")
    print(f"  Unmatched:          {unmatched}")
    print(f"  Skipped (already enriched or filtered): {skipped}")
    if not args.dry_run:
        print(f"  Firestore docs updated: {updated}")

    if unmatched_names:
        print(f"\nUnmatched candidates ({len(unmatched_names)}):")
        for name in unmatched_names[:30]:
            print(f"  - {name}")
        if len(unmatched_names) > 30:
            print(f"  ... and {len(unmatched_names) - 30} more")

    conn.close()


if __name__ == "__main__":
    main()
