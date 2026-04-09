"""
Fix MLA profile tenure data in Firestore.

K.Arjunan (amman_k_arjunan) was scraped from assembly.tn.gov.in/16thassembly
and has election_year=2021 / tenure="2021–2026" for coimbatore_south — which is wrong.
He was the 2016–2021 MLA. Vanathi Srinivasan is the 2021–2026 MLA.

This script corrects his party entry in mla_profiles.

Usage:
    .venv/bin/python3 scrapers/fix_mla_profile_tenure.py [--dry-run]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from google.cloud import firestore

FIXES = [
    {
        "doc_id": "amman_k_arjunan",
        "constituency_slug": "coimbatore_south",
        "correct_election_year": 2016,
        "correct_tenure": "2016–2021",
    },
]


def run(dry_run: bool, project: str) -> None:
    db = firestore.Client(project=project)
    col = db.collection("mla_profiles")

    for fix in FIXES:
        doc_id = fix["doc_id"]
        ref = col.document(doc_id)
        snap = ref.get()
        if not snap.exists:
            print(f"  SKIP {doc_id} — doc not found")
            continue

        data = snap.to_dict() or {}
        parties = data.get("parties", [])
        changed = False

        for entry in parties:
            if entry.get("constituency_slug") == fix["constituency_slug"]:
                old_year = entry.get("election_year")
                old_tenure = entry.get("tenure")
                if old_year == fix["correct_election_year"] and old_tenure == fix["correct_tenure"]:
                    print(f"  OK   {doc_id} — already correct")
                    break
                print(f"  FIX  {doc_id}: election_year {old_year!r} → {fix['correct_election_year']}, "
                      f"tenure {old_tenure!r} → {fix['correct_tenure']!r}")
                entry["election_year"] = fix["correct_election_year"]
                entry["tenure"] = fix["correct_tenure"]
                changed = True
                break
        else:
            print(f"  SKIP {doc_id} — no party entry for {fix['constituency_slug']!r}")
            continue

        if changed and not dry_run:
            ref.update({"parties": parties})
            print(f"  ✓    {doc_id} updated in Firestore")
        elif dry_run:
            print(f"  DRY  {doc_id} — would update Firestore")


def main() -> None:
    ap = argparse.ArgumentParser(description="Fix MLA profile tenure data")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--project", default="naatunadappu")
    args = ap.parse_args()
    run(dry_run=args.dry_run, project=args.project)


if __name__ == "__main__":
    main()
