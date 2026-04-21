"""Migrate candidate_accountability + candidates_2026 → politician_profile.

Creates one doc per person-per-election (no auto-dedup). Deduplication is
handled manually via the Politician Profile admin page's "Merge" feature.

Schema: each profile has basic identity fields + a `timeline` array where
each entry holds the FULL tenure-specific data (assets breakdown, criminal
case details, source URLs, etc.) — not just summaries.

Usage:
    python scrapers/politician_profile_migrate.py --dry-run
    python scrapers/politician_profile_migrate.py
    python scrapers/politician_profile_migrate.py --wipe-first   # delete existing + re-migrate
"""
from __future__ import annotations

import argparse
import hashlib
import sys
from typing import Any

from google.cloud import firestore

PROJECT = "naatunadappu"
COLLECTION = "politician_profile"


def slugify_id(name: str, year: int | str, slug: str) -> str:
    clean = name.strip().lower().replace(" ", "_").replace(".", "")
    short_hash = hashlib.md5(f"{name}_{year}_{slug}".encode()).hexdigest()[:6]
    return f"{clean}_{year}_{slug}_{short_hash}"[:128]


def build_from_accountability(doc_id: str, d: dict[str, Any]) -> dict[str, Any]:
    name = (d.get("mla_name") or "").strip()
    year = d.get("election_year")
    slug = d.get("constituency_slug") or doc_id.split("_", 1)[-1]
    constituency = d.get("constituency") or slug.replace("_", " ").upper()

    timeline_entry = {
        "year": year,
        "constituency_slug": slug,
        "constituency": constituency,
        "party": d.get("party"),
        "won": True,
        # Full asset breakdown
        "assets_cr": d.get("assets_cr"),
        "movable_assets_cr": d.get("movable_assets_cr"),
        "immovable_assets_cr": d.get("immovable_assets_cr"),
        "liabilities_cr": d.get("liabilities_cr"),
        "net_assets_cr": d.get("net_assets_cr"),
        "is_crorepati": d.get("is_crorepati"),
        # Full criminal detail
        "criminal_cases": d.get("criminal_cases") or [],  # array of {act, status, fir_no, court, ipc_sections, ...}
        "criminal_cases_total": d.get("criminal_cases_total") or 0,
        "criminal_severity": d.get("criminal_severity") or "CLEAN",
        # Education at time of filing
        "education": d.get("education"),
        "education_tier": d.get("education_tier"),
        "institution_name": d.get("institution_name"),
        # Sources
        "source_url": d.get("source_url"),
        "source_pdf": d.get("source_pdf"),
        "ground_truth_confidence": d.get("ground_truth_confidence"),
        # Source doc reference
        "source_doc_id": doc_id,
        "source_collection": "candidate_accountability",
    }

    return {
        "canonical_name": name,
        "aliases": [],
        "photo_url": d.get("photo_url") or None,
        "gender": None,
        "dob": None,
        "age": None,
        "education": d.get("education"),
        "current_party": d.get("party"),
        "current_constituency": constituency,
        "current_constituency_slug": slug,
        "win_count": 1,
        "loss_count": 0,
        "total_contested": 1,
        "timeline": [timeline_entry],
    }


def build_from_2026(slug: str, c: dict[str, Any]) -> dict[str, Any]:
    name = (c.get("name") or "").strip()
    constituency = c.get("constituency") or slug.replace("_", " ").upper()

    timeline_entry = {
        "year": 2026,
        "constituency_slug": slug,
        "constituency": constituency,
        "party": c.get("party"),
        "won": None,
        "assets_cr": None,
        "movable_assets_cr": None,
        "immovable_assets_cr": None,
        "liabilities_cr": None,
        "net_assets_cr": None,
        "is_crorepati": None,
        "criminal_cases": [],
        "criminal_cases_total": 0,
        "criminal_severity": None,
        "education": None,
        "education_tier": None,
        "institution_name": None,
        "source_url": c.get("show_profile_url"),
        "source_pdf": None,
        "affidavit_url": c.get("affidavit_url"),
        "nomination_date": c.get("nomination_date"),
        "status": c.get("status"),
        "ground_truth_confidence": None,
        "source_doc_id": f"candidates_2026/{slug}",
        "source_collection": "candidates_2026",
    }

    return {
        "canonical_name": name,
        "aliases": [],
        "photo_url": c.get("photo_url") or None,
        "gender": c.get("gender") or None,
        "dob": None,
        "age": c.get("age") or None,
        "education": None,
        "current_party": c.get("party"),
        "current_constituency": constituency,
        "current_constituency_slug": slug,
        "win_count": 0,
        "loss_count": 0,
        "total_contested": 1,
        "timeline": [timeline_entry],
    }


def wipe_collection(db: firestore.Client) -> int:
    col = db.collection(COLLECTION)
    docs = list(col.stream())
    batch = db.batch()
    for i, d in enumerate(docs, 1):
        batch.delete(d.reference)
        if i % 400 == 0:
            batch.commit()
            batch = db.batch()
    batch.commit()
    return len(docs)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--wipe-first", action="store_true", help="Delete all existing profiles before migrating")
    args = ap.parse_args()

    db = firestore.Client(project=PROJECT)

    if args.wipe_first and not args.dry_run:
        print("Wiping existing politician_profile collection...")
        count = wipe_collection(db)
        print(f"  ✓ Deleted {count} docs")

    col = db.collection(COLLECTION)
    profiles: list[tuple[str, dict[str, Any]]] = []

    # 1. candidate_accountability (MLA winners 2006–2021)
    print("Reading candidate_accountability...")
    acc_docs = list(db.collection("candidate_accountability").stream())
    print(f"  {len(acc_docs)} docs")
    for doc in acc_docs:
        d = doc.to_dict() or {}
        name = (d.get("mla_name") or "").strip()
        if not name:
            continue
        doc_id = slugify_id(name, d.get("election_year", 0), d.get("constituency_slug", ""))
        profiles.append((doc_id, build_from_accountability(doc.id, d)))

    # 2. candidates_2026
    print("Reading candidates_2026...")
    c26_docs = list(db.collection("candidates_2026").stream())
    total_c26 = 0
    for doc in c26_docs:
        d = doc.to_dict() or {}
        for c in d.get("candidates", []):
            name = (c.get("name") or "").strip()
            if not name:
                continue
            doc_id = slugify_id(name, 2026, doc.id)
            profiles.append((doc_id, build_from_2026(doc.id, c)))
            total_c26 += 1
    print(f"  {total_c26} candidates across {len(c26_docs)} constituencies")

    print(f"\nTotal profiles to write: {len(profiles)}")

    if args.dry_run:
        print("\n[DRY RUN] Sample profiles:")
        for doc_id, p in profiles[:3]:
            tl = p["timeline"][0]
            print(f"  {doc_id}")
            print(f"    name={p['canonical_name']} | party={tl['party']} | {tl['constituency']} {tl['year']}")
            print(f"    assets={tl.get('assets_cr')} | criminal_cases={len(tl.get('criminal_cases', []))} | source={tl.get('source_collection')}")
        print("  ...")
        return

    print("Writing to Firestore...")
    batch = db.batch()
    for i, (doc_id, profile) in enumerate(profiles, 1):
        profile["created_at"] = firestore.SERVER_TIMESTAMP
        batch.set(col.document(doc_id), profile)
        if i % 400 == 0:
            batch.commit()
            batch = db.batch()
            print(f"  {i}/{len(profiles)} written")
    batch.commit()
    print(f"  ✓ {len(profiles)} profiles written to {COLLECTION}")


if __name__ == "__main__":
    main()
