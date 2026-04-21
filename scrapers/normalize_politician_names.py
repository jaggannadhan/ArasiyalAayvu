"""Standardize politician names across politician_profile collection.

Convention:
    <Title> <Name Parts> <Initials with dots>
    e.g., Dr. Ajith Kumar A.J.

Rules:
    1. Initials (1-2 chars) move to the END, separated by dots, trailing dot.
    2. Name parts (3+ chars) are Title Cased.
    3. Title prefixes (DR., ADV., PROF., AGRI.) stay at the front.
    4. Remove commas, semicolons, extra dots/spaces.
    5. Names with '@' aliases → flagged for review.
    6. Ambiguous names → flagged (name_needs_review: true).

Usage:
    python scrapers/normalize_politician_names.py --dry-run
    python scrapers/normalize_politician_names.py --dry-run --show-flagged
    python scrapers/normalize_politician_names.py
"""
from __future__ import annotations

import argparse
import re
import sys
from typing import Any

from google.cloud import firestore

PROJECT = "naatunadappu"
COLLECTION = "politician_profile"

TITLE_PREFIXES = {"DR", "ADV", "PROF", "AGRI", "CAPT", "COL", "MAJOR", "GEN", "SMT"}


def normalize_name(raw: str) -> tuple[str, bool]:
    """Return (normalized_name, needs_review).

    needs_review is True when the name is ambiguous and requires manual check.
    """
    original = raw.strip()
    if not original:
        return original, True

    # Flag @ aliases immediately
    if "@" in original:
        # Still try to normalize the first part
        parts = re.split(r"\s*@\s*", original, maxsplit=1)
        normalized, _ = _do_normalize(parts[0])
        return normalized, True  # always flag @ names

    return _do_normalize(original)


def _do_normalize(raw: str) -> tuple[str, bool]:
    needs_review = False

    # Clean up: remove commas, semicolons, extra whitespace
    s = raw.strip()
    s = re.sub(r"[,;]", " ", s)
    # Split "S.Stalinkumar" → "S. Stalinkumar" (initial stuck to name)
    s = re.sub(r"\b([A-Za-z])\.([A-Za-z]{3,})", r"\1. \2", s)
    # Split "Palaniswami.K" → "Palaniswami K" (name stuck to initial)
    s = re.sub(r"([A-Za-z]{3,})\.([A-Za-z])\b", r"\1 \2", s)
    s = re.sub(r"\s+", " ", s).strip()

    # Tokenize — split on spaces, keeping dots attached to their token
    tokens = s.split()

    titles: list[str] = []
    initials: list[str] = []
    name_parts: list[str] = []

    for token in tokens:
        clean = token.strip(".").strip()
        if not clean:
            continue

        upper = clean.upper()

        # Check if it's a known title prefix
        if upper in TITLE_PREFIXES:
            titles.append(upper[0] + upper[1:].lower() + ".")
            continue

        # Check if it's a dotted initial cluster like "M.K" or "A.R.S"
        # Must have dots between letters to qualify (plain "ALI" is a name, not initials)
        if "." in token and re.fullmatch(r"([A-Za-z]\.)+[A-Za-z]?\.?", token.strip()):
            letters = re.findall(r"[A-Za-z]", token)
            for l in letters:
                initials.append(l.upper())
            continue

        # Single character — definitely an initial
        if len(clean) == 1 and clean.isalpha():
            initials.append(clean.upper())
            continue

        # 2 characters, all alpha — likely initials (e.g., "SS", "MK")
        if len(clean) == 2 and clean.isalpha() and clean.isupper():
            for ch in clean:
                initials.append(ch.upper())
            continue

        # 3+ characters — name part
        name_parts.append(clean)

    # Flag if no name parts found (all initials)
    if not name_parts:
        needs_review = True
        # Try to use the longest token as the name
        all_tokens = [t.strip(".").strip() for t in tokens if t.strip(".").strip()]
        if all_tokens:
            longest = max(all_tokens, key=len)
            name_parts = [longest]
            initials = [t.upper() for t in all_tokens if t != longest and len(t) <= 2]
        else:
            return raw.strip(), True

    # Flag if name has unusual patterns
    if len(name_parts) == 1 and len(name_parts[0]) <= 2:
        needs_review = True

    # Flag names where ALL tokens were short (hard to distinguish)
    all_short = all(len(t.strip(".")) <= 2 for t in tokens)
    if all_short and len(tokens) > 1:
        needs_review = True

    # Build the result: <Titles> <Name Parts title-cased> <Initials with dots>
    result_parts: list[str] = []

    # Titles
    result_parts.extend(titles)

    # Name parts — title case
    for part in name_parts:
        result_parts.append(part[0].upper() + part[1:].lower() if len(part) > 1 else part.upper())

    # Initials — joined with dots, trailing dot
    if initials:
        result_parts.append(".".join(initials) + ".")

    result = " ".join(result_parts)

    # Final cleanup
    result = re.sub(r"\s+", " ", result).strip()

    return result, needs_review


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--show-flagged", action="store_true", help="In dry-run, show only flagged names")
    ap.add_argument("--limit", type=int, default=0, help="Process only first N docs (0=all)")
    args = ap.parse_args()

    db = firestore.Client(project=PROJECT)
    col = db.collection(COLLECTION)
    docs = list(col.stream())
    print(f"Loaded {len(docs)} profiles")

    if args.limit:
        docs = docs[:args.limit]

    changes: list[tuple[str, str, str, bool]] = []  # (doc_id, old, new, flagged)
    flagged_count = 0
    changed_count = 0

    for d in docs:
        data = d.to_dict() or {}
        old_name = (data.get("canonical_name") or "").strip()
        if not old_name:
            continue

        new_name, needs_review = normalize_name(old_name)

        if new_name != old_name:
            changed_count += 1
        if needs_review:
            flagged_count += 1

        changes.append((d.id, old_name, new_name, needs_review))

    print(f"Changed: {changed_count}, Flagged: {flagged_count}, Unchanged: {len(changes) - changed_count}")

    if args.dry_run:
        print("\n── Samples ──")
        shown = 0
        for doc_id, old, new, flagged in changes:
            if args.show_flagged and not flagged:
                continue
            if old == new and not flagged:
                continue
            flag = " ⚠ REVIEW" if flagged else ""
            print(f"  {old:40s} → {new:40s}{flag}")
            shown += 1
            if shown >= 50:
                print("  ... (capped at 50)")
                break
        return

    # Apply to Firestore
    print("\nApplying to Firestore...")
    batch = db.batch()
    batch_count = 0
    for i, (doc_id, old_name, new_name, needs_review) in enumerate(changes, 1):
        update: dict[str, Any] = {}

        if new_name != old_name:
            update["canonical_name"] = new_name
            # Add old name to aliases if not already there
            doc_data = next((d.to_dict() for d in docs if d.id == doc_id), {}) or {}
            aliases = list(doc_data.get("aliases") or [])
            if old_name not in aliases:
                aliases.append(old_name)
            update["aliases"] = aliases

        if needs_review:
            update["name_needs_review"] = True
        else:
            update["name_needs_review"] = False

        if update:
            batch.update(col.document(doc_id), update)
            batch_count += 1

        if batch_count >= 400:
            batch.commit()
            batch = db.batch()
            batch_count = 0
            print(f"  {i}/{len(changes)} processed")

    if batch_count > 0:
        batch.commit()
    print(f"  ✓ {len(changes)} profiles updated")


if __name__ == "__main__":
    main()
