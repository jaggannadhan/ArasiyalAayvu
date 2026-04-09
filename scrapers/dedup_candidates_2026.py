"""
Deduplicate candidates_2026 Firestore collection in-place.
Removes duplicate candidates (same name+party) from each constituency doc.

Usage:
  .venv/bin/python3 scrapers/dedup_candidates_2026.py --dry-run
  .venv/bin/python3 scrapers/dedup_candidates_2026.py
"""
import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from google.cloud import firestore  # noqa: E402

COLLECTION = "candidates_2026"
PROJECT    = "naatunadappu"


def _key(c: dict) -> tuple[str, str]:
    name  = re.sub(r"\s+", " ", c.get("name", "").strip().upper())
    party = c.get("party", "").strip().upper()
    return (name, party)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    db  = firestore.Client(project=PROJECT)
    col = db.collection(COLLECTION)

    docs = list(col.stream())
    print(f"Loaded {len(docs)} constituency docs")

    total_removed = 0
    fixed = 0

    for doc in docs:
        data = doc.to_dict()
        candidates = data.get("candidates", [])
        seen: set[tuple[str, str]] = set()
        deduped: list[dict] = []
        dupes = 0
        for c in candidates:
            k = _key(c)
            if k in seen:
                dupes += 1
            else:
                seen.add(k)
                deduped.append(c)

        if dupes == 0:
            continue

        total_removed += dupes
        fixed += 1
        print(f"  {doc.id}: {len(candidates)} → {len(deduped)} ({dupes} dupes removed)")

        if not args.dry_run:
            col.document(doc.id).update({
                "candidates":       deduped,
                "total_candidates": len(deduped),
            })

    print(f"\n{'[DRY RUN] ' if args.dry_run else ''}Done — {fixed} docs fixed, {total_removed} duplicates removed")


if __name__ == "__main__":
    main()
