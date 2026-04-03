"""
ULB Councillors Ingest — Greater Chennai Corporation (2011-2016 term)
=====================================================================
Downloads the OpenCity GCC 2011-2016 councillors CSV and writes one Firestore
doc per ward into `ulb_councillors` with election_year=2011, plus the Mayor
doc into `ulb_heads` as `greater_chennai_corporation_2011`.

The 2011 CSV only has: Ward, Corporator, Contact number, Email (no party/zone).
Ward→AC slug mapping is derived from the same LGD CSV used for the 2022 ingest.

Firestore schema
----------------
ulb_councillors/gcc_ward{NNN}_2011
  local_body_slug: "greater_chennai_corporation"
  local_body_name: "Greater Chennai Corporation"
  election_year: 2011
  ward_number: int
  assembly_constituency_slug: str | None
  councillor_name: str
  party: ""          # not available in source data
  party_full: ""

ulb_heads/greater_chennai_corporation_2011
  local_body_slug: "greater_chennai_corporation"
  local_body_name: "Greater Chennai Corporation"
  local_body_type: "Municipal Corporation"
  head_title: "Mayor"
  head_name: "Saidai S. Duraiswamy"
  party: "AIADMK"
  party_full: "All India Anna Dravida Munnetra Kazhagam"
  election_year: 2011
  notes: "Elected 2011; served until GCC went under administrator rule (2016)"

Usage
-----
  .venv/bin/python scrapers/ulb_councillors_gcc_2011_ingest.py --dry-run
  .venv/bin/python scrapers/ulb_councillors_gcc_2011_ingest.py
  .venv/bin/python scrapers/ulb_councillors_gcc_2011_ingest.py --resume
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

OPENCITY_2011_URL = (
    "https://data.opencity.in/dataset/73446067-4fc1-434a-90e6-6a918106fd3c"
    "/resource/e6da6bbc-abab-4ebe-8f02-6732c3818bf3"
    "/download/b17c183f-9eb1-40c4-b288-aad51282ad72.csv"
)

# Same LGD overrides as the 2022 ingest
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

GCC_MAYOR_2011 = {
    "local_body_slug":  "greater_chennai_corporation",
    "local_body_name":  "Greater Chennai Corporation",
    "local_body_type":  "Municipal Corporation",
    "head_title":       "Mayor",
    "head_name":        "Saidai S. Duraiswamy",
    "party":            "AIADMK",
    "party_full":       "All India Anna Dravida Munnetra Kazhagam",
    "election_year":    2011,
    "notes":            "Elected 2011; served until GCC went under administrator rule (2016)",
    "source_url":       "https://en.wikipedia.org/wiki/Greater_Chennai_Corporation",
}


def build_ward_to_slug() -> dict[int, str]:
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


def fetch_csv(url: str) -> list[dict]:
    print(f"Fetching {url} …")
    with urlreq.urlopen(url, timeout=30) as resp:  # type: ignore[union-attr]
        raw = resp.read().decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(raw))
    return list(reader)


def parse_councillors(rows: list[dict], ward_to_slug: dict[int, str]) -> list[dict]:
    docs = []
    for row in rows:
        ward_raw = str(row.get("Ward", "")).strip()
        name_raw = str(row.get("Corporator", "")).strip()
        if not ward_raw or not name_raw:
            continue
        try:
            ward_num = int(ward_raw)
        except ValueError:
            print(f"  ⚠ Skipping unparseable ward: {ward_raw!r}")
            continue
        docs.append({
            "local_body_slug":            "greater_chennai_corporation",
            "local_body_name":            "Greater Chennai Corporation",
            "election_year":              2011,
            "ward_number":                ward_num,
            "assembly_constituency_slug": ward_to_slug.get(ward_num),
            "councillor_name":            name_raw,
            "party":                      "",
            "party_full":                 "",
            "ingested_at":                NOW_ISO,
        })
    return docs


def doc_id(ward_num: int) -> str:
    return f"gcc_ward{ward_num:03d}_2011"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--resume", action="store_true", help="Skip docs that already exist")
    args = ap.parse_args()

    print("Building ward→AC slug mapping from LGD CSV …")
    ward_to_slug = build_ward_to_slug()
    print(f"  Mapped {len(ward_to_slug)} wards")

    rows = fetch_csv(OPENCITY_2011_URL)
    print(f"  Downloaded {len(rows)} rows")

    councillors = parse_councillors(rows, ward_to_slug)
    print(f"  Parsed {len(councillors)} councillors")

    if args.dry_run:
        print("\n[DRY RUN] Sample (first 10 by ward number):")
        for c in sorted(councillors, key=lambda x: x["ward_number"])[:10]:
            slug = c["assembly_constituency_slug"] or "??"
            print(f"  Ward {c['ward_number']:3d} | {slug:<30} | {c['councillor_name']}")
        unmapped = [c for c in councillors if not c["assembly_constituency_slug"]]
        print(f"\n  Unmapped wards: {len(unmapped)}")
        print(f"\n[DRY RUN] Mayor doc (ulb_heads/greater_chennai_corporation_2011):")
        for k, v in GCC_MAYOR_2011.items():
            print(f"  {k}: {v}")
        print(f"\n[DRY RUN] Would write {len(councillors)} councillor docs")
        return

    from google.cloud import firestore
    db = firestore.Client(project=PROJECT_ID)

    already_done: set[str] = set()
    if args.resume:
        existing = db.collection("ulb_councillors").where(
            filter=firestore.FieldFilter("election_year", "==", 2011)
        ).stream()
        already_done = {d.id for d in existing}
        print(f"[resume] {len(already_done)} 2011 docs already exist — skipping")

    col = db.collection("ulb_councillors")
    written = skipped = 0
    for c in councillors:
        did = doc_id(c["ward_number"])
        if did in already_done:
            skipped += 1
            continue
        try:
            col.document(did).set(c)
            print(f"  ✓ {did}: {c['councillor_name']}")
            written += 1
        except Exception as e:
            print(f"  ✗ {did}: {e}")
            skipped += 1

    # Write 2011 Mayor
    head_doc = {**GCC_MAYOR_2011, "ingested_at": NOW_ISO}
    try:
        db.collection("ulb_heads").document("greater_chennai_corporation_2011").set(head_doc)
        print(f"  ✓ ulb_heads/greater_chennai_corporation_2011: Mayor Saidai S. Duraiswamy (AIADMK)")
    except Exception as e:
        print(f"  ✗ ulb_heads/greater_chennai_corporation_2011: {e}")

    print(f"\nDone — written={written} skipped={skipped}")


if __name__ == "__main__":
    main()
