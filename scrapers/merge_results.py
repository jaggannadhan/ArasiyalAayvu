"""Merge re-scraped ACs 1-35 into the main tn_results_2026.json file.

Usage:
    python scrapers/merge_results.py --patch /path/to/tn_results_2026_ac1-35.json
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAIN_FILE = ROOT / "data" / "processed" / "tn_results_2026.json"


def merge(patch_file: Path) -> None:
    with open(MAIN_FILE, "r") as f:
        main = json.load(f)
    with open(patch_file, "r") as f:
        patch = json.load(f)

    existing = {r["ac_no"]: r for r in main["results"]}
    added, updated = 0, 0

    for r in patch["results"]:
        ac = r["ac_no"]
        if ac in existing:
            existing[ac] = r
            updated += 1
        else:
            existing[ac] = r
            added += 1

    main["results"] = sorted(existing.values(), key=lambda x: x["ac_no"])
    main["total_acs"] = len(main["results"])
    main["scraped_at"] = patch.get("scraped_at", main.get("scraped_at"))

    with open(MAIN_FILE, "w") as f:
        json.dump(main, f, indent=2, ensure_ascii=False)

    print(f"Merged: {added} added, {updated} updated. Total: {len(main['results'])} ACs.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--patch", required=True, help="Path to patch JSON (e.g. ac1-35)")
    args = parser.parse_args()
    merge(Path(args.patch))
