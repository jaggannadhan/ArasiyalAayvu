"""
ULB Councillors Ingest — Greater Chennai Corporation (2022)
============================================================
Downloads the OpenCity GCC Elections 2022 CSV and writes one Firestore doc
per ward councillor into `ulb_councillors`, plus a local body head doc into
`ulb_heads`.

Firestore schema
----------------
ulb_councillors/{gcc_wardNNN}
  local_body_slug: "greater_chennai_corporation"
  local_body_name: "Greater Chennai Corporation"
  ward_number: int
  zone_number: int
  zone_name: str
  councillor_name: str
  party: str            # abbreviation e.g. "DMK"
  party_full: str       # full name
  sex: str
  age: int | None
  ward_reservation: str
  ingested_at: str

ulb_heads/greater_chennai_corporation
  local_body_slug: "greater_chennai_corporation"
  local_body_name: "Greater Chennai Corporation"
  local_body_type: "Municipal Corporation"
  head_title: "Mayor"
  head_name: "R. Priya"
  party: "DMK"
  party_full: "Dravida Munnetra Kazhagam"
  election_year: 2022
  notes: "49th Mayor; youngest and third woman to serve as Mayor of Chennai"
  source_url: "https://en.wikipedia.org/wiki/Greater_Chennai_Corporation"
  ingested_at: str

Usage
-----
  .venv/bin/python scrapers/ulb_councillors_gcc_ingest.py --dry-run
  .venv/bin/python scrapers/ulb_councillors_gcc_ingest.py
"""

from __future__ import annotations
import argparse, csv, io, json, os, re, sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import urllib.request as urlreq
except ImportError:
    urlreq = None  # type: ignore

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "naatunadappu")
NOW_ISO    = datetime.now(timezone.utc).isoformat()

ROOT_DIR  = Path(__file__).resolve().parents[1]
MAP_PATH  = ROOT_DIR / "web" / "src" / "lib" / "constituency-map.json"
LGD_CSV   = Path("/tmp/cmu_extracted/constituencies_mapping_urban.02Apr2026.csv")

# LGD AC name (lower) → our slug (for names that differ from constituency-map.json)
LGD_AC_OVERRIDES: dict[str, str] = {
    "anna nagar":             "anna_nagar",
    "madavaram":              "madhavaram",
    "thiru -vi -ka -nagar":  "thiru_vi_ka_nagar_sc",
    "egmore":                 "egmore_sc",
    "dr.radhakrishnan nagar": "dr_radhakrishnan_nagar",
    "chepauk-thiruvallikeni": "chepauk_thiruvallikeni",
    "thousand lights":        "thousand_lights",
    "tiruvottiyur":           "thiruvottiyur",
    "pallavaram":             "pallavaram",
    "ponneri":                "ponneri",
    "maduravoyal":            "madhuravoyal",
}


def build_ward_to_slug() -> dict[int, str]:
    """Build ward_number → constituency_slug using LGD CSV + constituency-map.json."""
    cmap = json.loads(MAP_PATH.read_text(encoding="utf-8"))
    lgd_name_to_slug: dict[str, str] = {
        v["name"].lower(): slug for slug, v in cmap.items()
    }

    ward_to_slug: dict[int, str] = {}
    if not LGD_CSV.exists():
        print(f"  ⚠ LGD CSV not found at {LGD_CSV} — ward→AC mapping unavailable")
        return ward_to_slug

    with open(LGD_CSV, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row["State Code"].strip() != "33":
                continue
            if "chennai" not in row["Urban Localbody Name"].lower():
                continue
            m = re.search(r"Ward No\.?\s*(\d+)", row["Ward Name"])
            if not m:
                continue
            ward_num = int(m.group(1))
            ac_lgd = row["Assembly Constituency Name"].strip().lower()
            slug = LGD_AC_OVERRIDES.get(ac_lgd) or lgd_name_to_slug.get(ac_lgd)
            if slug:
                ward_to_slug[ward_num] = slug
    return ward_to_slug


OPENCITY_CSV_URL = (
    "https://data.opencity.in/dataset/6f51175f-636f-4c61-abfa-cadaed53f6f8"
    "/resource/dd9d9957-81cc-4de8-873c-a216d749d68e"
    "/download/766741ac-a1ab-46d2-9e38-0f0d5f30456a.csv"
)

# 15 GCC zones (zone number 1-15 → name)
GCC_ZONE_NAMES: dict[int, str] = {
    1:  "Thiruvottiyur",
    2:  "Manali",
    3:  "Madhavaram",
    4:  "Tondiarpet",
    5:  "Royapuram",
    6:  "Thiru Vi Ka Nagar",
    7:  "Ambattur",
    8:  "Anna Nagar",
    9:  "Teynampet",
    10: "Kodambakkam",
    11: "Valasaravakkam",
    12: "Alandur",
    13: "Adyar",
    14: "Perungudi",
    15: "Sholinganallur",
}

GCC_MAYOR = {
    "local_body_slug": "greater_chennai_corporation",
    "local_body_name": "Greater Chennai Corporation",
    "local_body_type": "Municipal Corporation",
    "head_title": "Mayor",
    "head_name": "R. Priya",
    "party": "DMK",
    "party_full": "Dravida Munnetra Kazhagam",
    "election_year": 2022,
    "notes": "49th Mayor; youngest and third woman to serve as Mayor of Chennai",
    "source_url": "https://en.wikipedia.org/wiki/Greater_Chennai_Corporation",
}


def fetch_csv(url: str) -> list[dict]:
    print(f"Fetching {url} …")
    with urlreq.urlopen(url, timeout=30) as resp:  # type: ignore[union-attr]
        raw = resp.read().decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(raw))
    return list(reader)


def parse_councillors(rows: list[dict], ward_to_slug: dict[int, str]) -> list[dict]:
    docs = []
    for row in rows:
        try:
            zone_num = int(str(row.get("Zone", "")).strip())
            ward_num = int(str(row.get("Ward", "")).strip())
        except ValueError:
            print(f"  ⚠ Skipping unparseable row: {row}")
            continue

        age_raw = str(row.get("Age", "")).strip()
        try:
            age = int(age_raw)
        except ValueError:
            age = None

        party_abbr = str(row.get("Party (s)", "")).strip()
        party_full  = str(row.get("Party name", "")).strip()
        # Normalise "Independent" variants
        if party_abbr.lower() in ("independent", "ind"):
            party_abbr = "IND"
            party_full = "Independent"

        docs.append({
            "local_body_slug":           "greater_chennai_corporation",
            "local_body_name":           "Greater Chennai Corporation",
            "ward_number":               ward_num,
            "zone_number":               zone_num,
            "zone_name":                 GCC_ZONE_NAMES.get(zone_num, f"Zone {zone_num}"),
            "assembly_constituency_slug": ward_to_slug.get(ward_num),
            "councillor_name":           str(row.get("Name of candidate", "")).strip(),
            "party":                     party_abbr,
            "party_full":                party_full,
            "sex":             str(row.get("Sex", "")).strip(),
            "age":             age,
            "ward_reservation": str(row.get("Ward reservation", "")).strip(),
            "ingested_at":     NOW_ISO,
        })
    return docs


def doc_id(ward_num: int) -> str:
    return f"gcc_ward{ward_num:03d}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    print("Building ward→AC slug mapping from LGD CSV …")
    ward_to_slug = build_ward_to_slug()
    print(f"  Mapped {len(ward_to_slug)} wards to constituency slugs")

    rows = fetch_csv(OPENCITY_CSV_URL)
    print(f"  Downloaded {len(rows)} rows")

    councillors = parse_councillors(rows, ward_to_slug)
    print(f"  Parsed {len(councillors)} councillors")

    if args.dry_run:
        print("\n[DRY RUN] Sample (first 5 by ward number):")
        for c in sorted(councillors, key=lambda x: x["ward_number"])[:5]:
            print(f"  Ward {c['ward_number']:3d} | Zone {c['zone_number']:2d} {c['zone_name']:<20} | {c['councillor_name']:<30} | {c['party']}")
        print(f"\n[DRY RUN] Mayor doc:")
        for k, v in GCC_MAYOR.items():
            print(f"  {k}: {v}")
        return

    from google.cloud import firestore
    db = firestore.Client(project=PROJECT_ID)

    # Write councillors
    col = db.collection("ulb_councillors")
    written = skipped = 0
    for c in councillors:
        did = doc_id(c["ward_number"])
        try:
            col.document(did).set(c)
            print(f"  ✓ {did}: {c['councillor_name']} ({c['party']})")
            written += 1
        except Exception as e:
            print(f"  ✗ {did}: {e}")
            skipped += 1

    # Write Mayor
    head_doc = {**GCC_MAYOR, "ingested_at": NOW_ISO}
    try:
        db.collection("ulb_heads").document("greater_chennai_corporation").set(head_doc)
        print(f"  ✓ ulb_heads/greater_chennai_corporation: Mayor R. Priya (DMK)")
    except Exception as e:
        print(f"  ✗ ulb_heads/greater_chennai_corporation: {e}")

    print(f"\nDone — written={written} skipped={skipped}")


if __name__ == "__main__":
    main()
