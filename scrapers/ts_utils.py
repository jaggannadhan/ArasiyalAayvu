"""
Time-series utilities shared by all knowledge-graph ingestors.

Schema (local JSON):
{
  "meta": {
    "dataset": "plfs",
    "last_updated": "2026-04-11",
    "description": "..."
  },
  "entities": {
    "Tamil Nadu": {
      "id": "tamil_nadu",
      "snapshots": {
        "2023-24": {
          "fetched_at": "2026-04-11",
          "data_period": "2023-24",
          "source_url": "...",
          ...data fields...
        }
      }
    }
  }
}

Firestore mirror:
  {collection}/{entity_id}/snapshots/{data_period}

Entity IDs are slugified display names:
  "Tamil Nadu"              → "tamil_nadu"
  "All India"               → "all_india"
  "Cost_of_Living_India"    → "cost_of_living_india"
  "Cost_of_Living_Tamil_Nadu" → "cost_of_living_tamil_nadu"
"""

import json
import re
from datetime import date
from pathlib import Path
from typing import Optional


def slugify(name: str) -> str:
    """'Tamil Nadu' → 'tamil_nadu'."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def today() -> str:
    return date.today().isoformat()


def load_timeseries(path: Path) -> dict:
    """Load existing time-series JSON, or return a fresh skeleton."""
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {"meta": {}, "entities": {}}


def upsert_snapshot(
    ts: dict,
    display_name: str,
    data_period: str,
    snapshot: dict,
    meta: Optional[dict] = None,
) -> None:
    """
    Add or replace a snapshot for one entity.

    ts          — the full time-series dict (mutated in place)
    display_name — e.g. "Tamil Nadu", "Cost_of_Living_India"
    data_period  — e.g. "2023-24", "2026-04", "2023"
    snapshot     — dict of data fields; fetched_at is injected automatically
    meta         — optional top-level meta fields to merge
    """
    if meta:
        ts["meta"].update(meta)

    ts["meta"]["last_updated"] = today()

    entity = ts["entities"].setdefault(display_name, {
        "id": slugify(display_name),
        "snapshots": {},
    })
    entity["snapshots"][data_period] = {
        "fetched_at": today(),
        "data_period": data_period,
        **snapshot,
    }


def save_timeseries(ts: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(ts, f, ensure_ascii=False, indent=2)


def upload_snapshot_to_firestore(
    db,
    collection: str,
    entity_display_name: str,
    data_period: str,
    snapshot: dict,
    entity_extra_fields: Optional[dict] = None,
) -> None:
    """
    Write one snapshot to Firestore sub-collection.

    Path: {collection}/{entity_id}/snapshots/{data_period}

    Also sets/merges static fields on the parent entity document
    (name, id) so the entity doc itself is queryable.
    """
    entity_id = slugify(entity_display_name)
    entity_ref = db.collection(collection).document(entity_id)

    # Ensure parent entity doc has basic fields
    entity_doc = {"id": entity_id, "name": entity_display_name}
    if entity_extra_fields:
        entity_doc.update(entity_extra_fields)
    entity_ref.set(entity_doc, merge=True)

    # Write snapshot as sub-document
    snap_ref = entity_ref.collection("snapshots").document(data_period)
    snap_ref.set({
        "fetched_at": today(),
        "data_period": data_period,
        **snapshot,
    })
