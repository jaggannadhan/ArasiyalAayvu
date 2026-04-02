"""
Ward Mapping Ingest
===================
Reads the pre-processed LGD constituencies_mapping_urban CSV and writes one
doc per constituency into the `ward_mapping` Firestore collection.

Source data:
  LGD (Local Government Directory) — Government of India
  Archive: https://github.com/ramSeraph/opendata/releases/download/lgd-latest-extra1/constituencies_mapping_urban.02Apr2026.csv.7z
  Original: https://lgdirectory.gov.in

Usage:
  .venv/bin/python scrapers/ward_mapping_ingest.py --dry-run
  .venv/bin/python scrapers/ward_mapping_ingest.py
"""

from __future__ import annotations
import argparse, csv, json, os, re, sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR   = Path(__file__).resolve().parents[1]
CSV_PATH   = Path("/tmp/cmu_extracted/constituencies_mapping_urban.02Apr2026.csv")
MAP_PATH   = ROOT_DIR / "web" / "src" / "lib" / "constituency-map.json"
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "naatunadappu")
NOW_ISO    = datetime.now(timezone.utc).isoformat()
TN_STATE_CODE = "33"

# ---------------------------------------------------------------------------
# Name normalisation: strip SC/ST markers, collapse whitespace, upper-case
# ---------------------------------------------------------------------------
def norm(s: str) -> str:
    s = s.upper().strip()
    s = re.sub(r'\s*\(SC\)|\s*\(ST\)', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


# ---------------------------------------------------------------------------
# LGD spelling → our slug  (where norm(lgd_name) ≠ norm(map_name))
# ---------------------------------------------------------------------------
LGD_NAME_ALIASES: dict[str, str] = {
    "MADURAVOYAL":              "madhuravoyal",
    "MADAVARAM":                "madhavaram",
    "TIRUVOTTIYUR":             "thiruvottiyur",
    "THIRU -VI -KA -NAGAR":    "thiru_vi_ka_nagar_sc",
    "SHOLINGUR":                "sholinghur",
    "THALLI":                   "thally",
    "PALACODU":                 "palacode",
    "PAPPIREDDIPPATTI":         "pappireddipatti",
    "VILUPPURAM":               "villupuram",
    "THIRUKOILUR":              "tirukkoyilur",
    "SALEM (NORTH)":            "salem_north",
    "SALEM (SOUTH)":            "salem_south",
    "PARAMATHI-VELUR":          "paramathivelur",
    "ERODE (WEST)":             "erode_west",
    "MODAKURICHI":              "modakkurichi",
    "METTUPPALAYAM":            "mettupalayam",
    "TIRUPPUR (NORTH)":         "tiruppur_north",
    "COIMBATORE (NORTH)":       "coimbatore_north",
    "COIMBATORE (SOUTH)":       "coimbatore_south",
    "TIRUCHIRAPPALLI (WEST)":   "tiruchirappalli_west",
    "TIRUCHIRAPPALLI (EAST)":   "tiruchirappalli_east",
    "VRIDDHACHALAM":            "vridhachalam",
    "VEDARANYAM":               "vedharanyam",
    "THIRUVARUR":               "thiruvaur",
    "ORATTANADU":               "orathanadu",
    "BODINAYACKANUR":           "bodinayakkanur",
    "ARUPPUKKOTTAI":            "aruppukottai",
    "MUDHUKULATHUR":            "mudukulathur",
    "THOOTHUKKUDI":             "thoothukudi",
    "COLACHEL":                 "colachal",
}

# LGD ECI code → slug for the two "Tiruppattur" constituencies
# (identical names; disambiguation by ECI code only)
ECI_OVERRIDES: dict[int, str] = {
    50:  "tirupattur",   # Tirupattur (Tirupathur district); localbody: Thirupathur
    185: "tiruppathur",  # Tiruppathur (Sivaganga district); localbody: Kottaiyur
}


def build_name_to_slug(map_path: Path) -> dict[str, str]:
    """norm(constituency name) → slug, from constituency-map.json."""
    with open(map_path, encoding="utf-8") as f:
        cmap = json.load(f)
    mapping: dict[str, str] = {}
    for slug, v in cmap.items():
        mapping[norm(v["name"])] = slug
    # Overlay aliases (LGD spellings that differ from map names)
    mapping.update(LGD_NAME_ALIASES)
    return mapping


def parse_csv(csv_path: Path, name_to_slug: dict[str, str]) -> list[dict]:
    ac_data: dict[int, dict] = defaultdict(lambda: {
        "name": None,
        "ward_codes": set(),
        "local_bodies": defaultdict(lambda: {"type": None, "ward_codes": set()}),
    })

    with open(csv_path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row["State Code"].strip() != TN_STATE_CODE:
                continue
            eci_raw = row["Assembly Constituency ECI Code"].strip()
            if not eci_raw or eci_raw == "0":
                continue
            eci = int(eci_raw)
            ac_data[eci]["name"] = row["Assembly Constituency Name"].strip()
            ac_data[eci]["ward_codes"].add(row["Ward Code"].strip())
            lb = row["Urban Localbody Name"].strip()
            ac_data[eci]["local_bodies"][lb]["type"] = row["Localbody Type"].strip()
            ac_data[eci]["local_bodies"][lb]["ward_codes"].add(row["Ward Code"].strip())

    results = []
    for eci, d in sorted(ac_data.items()):
        raw_name = d["name"]
        normed   = norm(raw_name)

        # ECI override wins (for identical-name constituencies like both Tiruppattur)
        slug = ECI_OVERRIDES.get(eci) or name_to_slug.get(normed)

        lbs = sorted([
            {"name": n, "type": v["type"], "ward_count": len(v["ward_codes"])}
            for n, v in d["local_bodies"].items()
        ], key=lambda x: -x["ward_count"])

        results.append({
            "eci_code":           eci,
            "constituency_slug":  slug,
            "constituency_name":  raw_name,
            "total_urban_wards":  len(d["ward_codes"]),
            "local_bodies":       lbs,
        })
    return results


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not CSV_PATH.exists():
        sys.exit(f"CSV not found: {CSV_PATH}\nRun the download + extraction step first.")

    print(f"Building name→slug map from {MAP_PATH.name} …")
    name_to_slug = build_name_to_slug(MAP_PATH)

    print(f"Parsing {CSV_PATH.name} …")
    records = parse_csv(CSV_PATH, name_to_slug)

    matched   = [r for r in records if r["constituency_slug"]]
    unmatched = [r for r in records if not r["constituency_slug"]]
    print(f"  ACs parsed:    {len(records)}")
    print(f"  Slug matched:  {len(matched)}")
    print(f"  No slug:       {len(unmatched)}")
    if unmatched:
        for r in unmatched:
            print(f"    ECI {r['eci_code']:3d}: {r['constituency_name']}")

    if args.dry_run:
        print("\n[DRY RUN] Sample output (top 5 by ward count):")
        for r in sorted(matched, key=lambda x: -x["total_urban_wards"])[:5]:
            lbs = "; ".join(
                f"{lb['name']} ({lb['type'][:4]}, {lb['ward_count']}w)"
                for lb in r["local_bodies"][:3]
            )
            print(f"  {r['constituency_slug']:<35} {r['total_urban_wards']:3d} wards  {lbs}")
        return

    from google.cloud import firestore
    db  = firestore.Client(project=PROJECT_ID)
    col = db.collection("ward_mapping")

    written = skipped = 0
    for r in matched:
        slug = r["constituency_slug"]
        doc = {
            "constituency_slug":  slug,
            "constituency_name":  r["constituency_name"],
            "eci_code":           r["eci_code"],
            "total_urban_wards":  r["total_urban_wards"],
            "local_bodies":       r["local_bodies"],
            "data_source":        "LGD (Local Government Directory) — GoI",
            "source_url":         "https://lgdirectory.gov.in",
            "archive_url":        "https://github.com/ramSeraph/opendata/releases/tag/lgd-latest-extra1",
            "data_date":          "2026-04-02",
            "ingested_at":        NOW_ISO,
        }
        try:
            col.document(slug).set(doc)
            print(f"  ✓ {slug}: {r['total_urban_wards']} wards")
            written += 1
        except Exception as e:
            print(f"  ✗ {slug}: {e}")
            skipped += 1

    print(f"\nDone — written={written} skipped={skipped}")


if __name__ == "__main__":
    main()
