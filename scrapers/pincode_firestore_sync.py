"""
Sync tn_pincodes_mapped.json → Firestore `pincode_mapping` collection.

Confidence tiers:
  exact     (1 constituency)  → upload, is_ambiguous=False
  ambiguous (2-3 constituencies) → upload, is_ambiguous=True
  skip      (4+ constituencies)  → delete from Firestore if present

Usage:
  .venv/bin/python scrapers/pincode_firestore_sync.py --dry-run
  .venv/bin/python scrapers/pincode_firestore_sync.py
"""

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

PROJECT_ID = "naatunadappu"
COLLECTION = "pincode_mapping"
MAPPED_JSON = ROOT / "data/processed/tn_pincodes_mapped.json"
CONSTITUENCY_MAP_JSON = ROOT / "web/src/lib/constituency-map.json"
MAX_CONSTITUENCIES = 3  # entries with more than this are low-confidence

BATCH_SIZE = 400


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    with open(MAPPED_JSON, encoding="utf-8") as f:
        all_records: list[dict] = json.load(f)

    with open(CONSTITUENCY_MAP_JSON, encoding="utf-8") as f:
        cmap: dict = json.load(f)

    # Partition records
    to_upload: list[dict] = []
    to_delete_pins: list[str] = []

    for rec in all_records:
        n = len(rec.get("constituencies", []))
        if n == 0 or n > MAX_CONSTITUENCIES:
            to_delete_pins.append(rec["pincode"])
        else:
            # Enrich constituency names from constituency map
            enriched = []
            for c in rec["constituencies"]:
                slug = c["slug"]
                meta = cmap.get(slug, {})
                enriched.append({
                    "slug": slug,
                    "name": meta.get("name", c.get("name", slug)),
                    "name_ta": c.get("name_ta", ""),
                })
            to_upload.append({
                "pincode": rec["pincode"],
                "district": rec.get("district", ""),
                "is_ambiguous": n > 1,
                "constituencies": enriched,
                "ground_truth_confidence": "HIGH" if n == 1 else "MEDIUM",
                "_schema_version": "2.0",
            })

    print(f"Records to upload:  {len(to_upload)}")
    print(f"Records to delete:  {len(to_delete_pins)}")

    if args.dry_run:
        print("\n[DRY RUN] Sample uploads:")
        for r in to_upload[:8]:
            print(f"  {r['pincode']} → {[c['slug'] for c in r['constituencies']]} "
                  f"({'ambiguous' if r['is_ambiguous'] else 'exact'})")
        print(f"\n[DRY RUN] Sample deletes: {to_delete_pins[:8]}")
        return

    from google.cloud import firestore  # noqa: E402

    db = firestore.Client(project=PROJECT_ID)
    col = db.collection(COLLECTION)

    # Upload
    written = 0
    batch = db.batch()
    for i, rec in enumerate(to_upload):
        batch.set(col.document(rec["pincode"]), rec)
        written += 1
        if written % BATCH_SIZE == 0:
            batch.commit()
            batch = db.batch()
            print(f"  Uploaded {written}/{len(to_upload)}…")
    batch.commit()
    print(f"Uploaded {written} documents")

    # Delete low-confidence entries
    deleted = 0
    batch = db.batch()
    for i, pin in enumerate(to_delete_pins):
        batch.delete(col.document(pin))
        deleted += 1
        if deleted % BATCH_SIZE == 0:
            batch.commit()
            batch = db.batch()
            print(f"  Deleted {deleted}/{len(to_delete_pins)}…")
    batch.commit()
    print(f"Deleted {deleted} low-confidence documents")
    print(f"\nDone — {written} uploaded, {deleted} deleted.")


if __name__ == "__main__":
    main()
