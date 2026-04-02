"""
NFHS-5 District-Level Ingest
=============================
Downloads district-level NFHS-5 data for Tamil Nadu (32 districts) from
SaiSiddhardhaKalla/NFHS GitHub repo (derived from official IIPS factsheets),
and writes district-scoped docs into the `socio_economics` Firestore collection.

Source:
  https://github.com/SaiSiddhardhaKalla/NFHS/blob/main/_states/TN.csv
  (Derived from official IIPS/MoHFW NFHS-5 district factsheets)
  Original: http://rchiips.org/nfhs/districtfactsheet_NFHS-5.shtml

Coverage:
  32 / 38 TN districts — 6 new districts (post-2011 Census) have no NFHS-5 factsheet.
  Missing 6: Chengalpattu, Kallakurichi, Ranipet, Tenkasi, Tirupathur, Mayiladuthurai.
  These are assigned their parent district's values (disclosed in metadata).

Usage:
  .venv/bin/python scrapers/nfhs5_district_ingest.py --dry-run
  .venv/bin/python scrapers/nfhs5_district_ingest.py
"""

from __future__ import annotations
import argparse, csv, io, os, sys
from datetime import datetime, timezone
from pathlib import Path

import requests

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "naatunadappu")
NOW_ISO    = datetime.now(timezone.utc).isoformat()

CSV_URL = "https://raw.githubusercontent.com/SaiSiddhardhaKalla/NFHS/main/_states/TN.csv"

# ---------------------------------------------------------------------------
# The 4 indicators we ingest (exact indicator strings from the CSV)
# ---------------------------------------------------------------------------
INDICATORS = {
    "nfhs5_anaemia_women": {
        "indicator_str":   "All women age 15-49 years who are anaemic (%)",
        "metric_name":     "Anaemia in Women (NFHS-5)",
        "tamil_name":      "பெண்களில் இரத்த சோகை (NFHS-5)",
        "category":        "Health",
        "unit":            "percent",
        "good_direction":  "low",
        "national_nfhs5":  57.0,   # India average from NFHS-5 national report
        "alert_above":     40,
        "context":         "Percentage of women aged 15-49 with haemoglobin < 12 g/dl. "
                           "High anaemia reflects iron deficiency, poor nutrition, and "
                           "limited access to healthcare — directly linked to maternal mortality risk.",
    },
    "nfhs5_stunting_under5": {
        "indicator_str":   "Children under 5 years who are stunted (height for age) (%)",
        "metric_name":     "Child Stunting Under 5 (NFHS-5)",
        "tamil_name":      "5 வயதுக்கு கீழான குழந்தைகளில் வளர்ச்சிக் குறைபாடு (NFHS-5)",
        "category":        "Health",
        "unit":            "percent",
        "good_direction":  "low",
        "national_nfhs5":  35.5,
        "alert_above":     30,
        "context":         "Percentage of under-5 children whose height is too low for their age. "
                           "Stunting is a marker of chronic malnutrition and predicts long-term "
                           "cognitive and economic outcomes.",
    },
    "nfhs5_institutional_deliveries": {
        "indicator_str":   "Births attended by skilled health personnel (%)",
        "metric_name":     "Skilled Birth Attendance (NFHS-5)",
        "tamil_name":      "திறமையான சுகாதார பணியாளரால் மேற்கொள்ளப்பட்ட பிரசவங்கள் (NFHS-5)",
        "category":        "Health",
        "unit":            "percent",
        "good_direction":  "high",
        "national_nfhs5":  88.6,
        "alert_below":     80,
        "context":         "Percentage of births attended by a doctor, nurse, or midwife. "
                           "A key maternal health indicator — low values predict higher "
                           "maternal and neonatal mortality.",
    },
    "nfhs5_women_literacy": {
        "indicator_str":   "Women who are literate (%)",
        "metric_name":     "Female Literacy Rate (NFHS-5)",
        "tamil_name":      "பெண்கள் கல்வியறிவு விகிதம் (NFHS-5)",
        "category":        "Education",
        "unit":            "percent",
        "good_direction":  "high",
        "national_nfhs5":  71.5,
        "alert_below":     70,
        "context":         "Percentage of women aged 15-49 who can read and write. "
                           "Female literacy is the strongest single predictor of child health, "
                           "nutrition outcomes, and inter-generational poverty reduction.",
    },
}

# ---------------------------------------------------------------------------
# NFHS-5 district name → our district_slug
# ---------------------------------------------------------------------------
NFHS_NAME_TO_SLUG: dict[str, str] = {
    "Ariyalur":         "ariyalur",
    "Chennai":          "chennai",
    "Coimbatore":       "coimbatore",
    "Cuddalore":        "cuddalore",
    "Dharmapuri":       "dharmapuri",
    "Dindigul":         "dindigul",
    "Erode":            "erode",
    "Kancheepuram":     "kancheepuram",
    "Kanniyakumari":    "kanniyakumari",
    "Karur":            "karur",
    "Krishnagiri":      "krishnagiri",
    "Madurai":          "madurai",
    "Nagapattinam":     "nagapattinam",
    "Namakkal":         "namakkal",
    "Perambalur":       "perambalur",
    "Pudukkottai":      "pudukkottai",
    "Ramanathapuram":   "ramanathapuram",
    "Salem":            "salem",
    "Sivaganga":        "sivaganga",
    "Thanjavur":        "thanjavur",
    "The Nilgiris":     "the_nilgiris",
    "Theni":            "theni",          # CSV has trailing space — stripped below
    "Thiruvallur":      "thiruvallur",
    "Thiruvarur":       "thiruvarur",
    "Thoothukkudi":     "thoothukudi",    # NFHS spelling → our slug
    "Tiruchirappalli":  "tiruchirappalli",
    "Tirunelveli":      "tirunelveli",
    "Tiruppur":         "tiruppur",
    "Tiruvannamalai":   "tiruvannamalai",
    "Vellore":          "vellore",
    "Viluppuram":       "villuppuram",    # NFHS one-l → our two-l slug
    "Virudhunagar":     "virudhunagar",
}

# 6 new districts (post-2011): map to parent NFHS-5 district slug
NEW_DISTRICT_PARENT: dict[str, str] = {
    "chengalpattu": "kancheepuram",
    "kallakurichi":  "villuppuram",
    "ranipet":       "vellore",
    "tenkasi":       "tirunelveli",
    "tirupathur":    "vellore",
}
# mayiladuthurai is in our system — parent is nagapattinam
# (check if it exists in our constituency map)


def fetch_csv() -> list[dict]:
    print(f"Fetching {CSV_URL} …")
    r = requests.get(CSV_URL, timeout=30)
    r.raise_for_status()
    reader = csv.DictReader(io.StringIO(r.text.lstrip("\ufeff")))
    return list(reader)


def extract_district_values(rows: list[dict]) -> dict[str, dict[str, float | None]]:
    """Returns {district_slug: {metric_id: value}}"""
    result: dict[str, dict[str, float | None]] = {}

    # Build target indicator set for fast lookup
    target_indicators = {cfg["indicator_str"]: mid for mid, cfg in INDICATORS.items()}

    for row in rows:
        raw_name = row["District Name"].strip()
        slug = NFHS_NAME_TO_SLUG.get(raw_name)
        if not slug:
            continue
        indicator = row["Indicator"].strip()
        if indicator not in target_indicators:
            continue
        metric_id = target_indicators[indicator]
        try:
            value = float(row["NFHS 5"])
        except (ValueError, TypeError):
            value = None
        if slug not in result:
            result[slug] = {}
        result[slug][metric_id] = value

    return result


def build_docs(district_values: dict[str, dict]) -> list[dict]:
    docs = []

    all_slugs = set(district_values.keys()) | set(NEW_DISTRICT_PARENT.keys())

    for slug in sorted(all_slugs):
        is_new_district = slug in NEW_DISTRICT_PARENT
        parent_slug = NEW_DISTRICT_PARENT.get(slug, slug)
        values = district_values.get(parent_slug, {})

        # District display name (title-case of slug)
        district_name = slug.replace("_", " ").title()

        for metric_id, cfg in INDICATORS.items():
            value = values.get(metric_id)
            if value is None:
                continue

            doc_id = f"{slug}_{metric_id}"
            doc = {
                "metric_id":               metric_id,
                "category":                cfg["category"],
                "metric_name":             cfg["metric_name"],
                "tamil_name":              cfg["tamil_name"],
                "value":                   value,
                "unit":                    cfg["unit"],
                "year":                    2021,
                "survey":                  "NFHS-5",
                "national_average":        cfg["national_nfhs5"],
                "context":                 cfg["context"],
                "district_slug":           slug,
                "district_name":           district_name,
                "metric_scope":            "district",
                "alert_level":             _alert_level(metric_id, cfg, value),
                "ground_truth_confidence": "HIGH" if not is_new_district else "MEDIUM",
                "source_url":              "http://rchiips.org/nfhs/nfhs-5.shtml",
                "source_title":            "NFHS-5 District Factsheets (2019-21)",
                "archive_url":             "https://github.com/SaiSiddhardhaKalla/NFHS",
                "ingested_at":             NOW_ISO,
                **({"parent_district_note": f"No NFHS-5 factsheet — using {parent_slug} district values"} if is_new_district else {}),
            }
            docs.append((doc_id, doc))

    return docs


def _alert_level(metric_id: str, cfg: dict, value: float) -> str | None:
    if "alert_above" in cfg and value > cfg["alert_above"]:
        return "HIGH"
    if "alert_below" in cfg and value < cfg["alert_below"]:
        return "HIGH"
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    rows = fetch_csv()
    print(f"  CSV rows: {len(rows)}")

    district_values = extract_district_values(rows)
    print(f"  Districts found: {len(district_values)}")

    docs = build_docs(district_values)
    print(f"  Docs to write: {len(docs)}")

    if args.dry_run:
        print("\n[DRY RUN] Sample (first 8 docs):")
        for doc_id, doc in docs[:8]:
            print(f"  {doc_id:<50s}  {doc['metric_id']} = {doc['value']} ({doc['district_slug']})")
        print()
        # Show cross-district spread for anaemia
        print("Anaemia spread across districts:")
        for doc_id, doc in docs:
            if doc["metric_id"] == "nfhs5_anaemia_women":
                flag = "  ⚠" if doc.get("alert_level") == "HIGH" else ""
                print(f"  {doc['district_slug']:<20s} {doc['value']:5.1f}%{flag}")
        return

    from google.cloud import firestore
    db  = firestore.Client(project=PROJECT_ID)
    col = db.collection("socio_economics")

    written = skipped = 0
    for doc_id, doc in docs:
        try:
            col.document(doc_id).set(doc)
            print(f"  ✓ {doc_id}")
            written += 1
        except Exception as e:
            print(f"  ✗ {doc_id}: {e}")
            skipped += 1

    print(f"\nDone — written={written} skipped={skipped}")


if __name__ == "__main__":
    main()
