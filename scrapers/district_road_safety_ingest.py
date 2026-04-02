"""
District Road Safety Ingest — Tamil Nadu 2021-2023
Populates the `district_road_safety` Firestore collection.

Source: TN Police / Highway Safety via OpenCity
Dataset:  https://data.opencity.in/dataset/tamil-nadu-road-accidents-reports
CSV:      https://data.opencity.in/dataset/2fc34e49-936f-4255-a4c3-1ef92f25adf3/resource/764579ad-22e0-4f27-a694-34d0d2f843ea/download/d23e7ba9-6a60-41db-b37b-62413cc72365.csv

Fields per district:
  accidents_2021 / deaths_2021 / death_rate_per_lakh_2021
  accidents_2022 / deaths_2022 / death_rate_per_lakh_2022
  accidents_2023 / deaths_2023 / death_rate_per_lakh_2023
  road_safety_level   — HIGH_RISK / MEDIUM_RISK / LOW_RISK  (based on 2021 death rate)
  trend_2021_2023     — IMPROVING / STABLE / WORSENING
  trend_pct_change    — percentage change in deaths 2021→2023

doc_id == district_slug so backend lookup works:
  _db.collection("district_road_safety").document(district_slug).get()

Notes:
  - Mayiladuthurai is excluded (new district, NA for 2021).
  - Population figures are mid-year 2021 projections (same source as crime index).
  - TN average death rate 2021: ~20.1 per lakh (15,384 deaths / 764.79L population).
  - Trend classification: IMPROVING < -10%, WORSENING > +15%, else STABLE.

Usage:
  .venv/bin/python scrapers/district_road_safety_ingest.py --dry-run
  .venv/bin/python scrapers/district_road_safety_ingest.py
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from google.cloud import firestore
except ImportError:
    firestore = None  # type: ignore[assignment]

PROJECT_ID = "naatunadappu"
COLLECTION = "district_road_safety"
NOW_ISO    = datetime.now(timezone.utc).isoformat()

SOURCE_DATASET = "https://data.opencity.in/dataset/tamil-nadu-road-accidents-reports"
SOURCE_CSV     = (
    "https://data.opencity.in/dataset/2fc34e49-936f-4255-a4c3-1ef92f25adf3"
    "/resource/764579ad-22e0-4f27-a694-34d0d2f843ea"
    "/download/d23e7ba9-6a60-41db-b37b-62413cc72365.csv"
)

TN_AVG_DEATH_RATE_2021 = 20.1   # deaths per lakh, 2021

# Population (mid-year 2021, lakhs) — same values as district_crime_index
POPULATION: dict[str, float] = {
    "ariyalur": 8.0, "chengalpattu": 24.2, "chennai": 72.3,
    "coimbatore": 36.6, "cuddalore": 27.6, "dharmapuri": 16.0,
    "dindigul": 22.9, "erode": 23.9, "kallakurichi": 14.8,
    "kancheepuram": 9.6, "kanniyakumari": 19.8, "karur": 11.3,
    "krishnagiri": 19.9, "madurai": 32.2, "nagapattinam": 17.1,
    "namakkal": 18.3, "perambalur": 6.0, "pudukkottai": 17.2,
    "ramanathapuram": 14.3, "ranipet": 12.7, "salem": 36.9,
    "sivaganga": 14.2, "tenkasi": 15.1, "thanjavur": 25.5,
    "the_nilgiris": 7.8, "theni": 13.2, "thiruvallur": 25.0,
    "thiruvarur": 13.4, "thoothukudi": 18.6, "tiruchirappalli": 28.9,
    "tirunelveli": 17.5, "tirupathur": 12.5, "tiruppur": 26.3,
    "tiruvannamalai": 26.1, "vellore": 16.5, "villuppuram": 21.9,
    "virudhunagar": 20.6,
}


def _rate(deaths: int, slug: str) -> float:
    pop = POPULATION.get(slug, 1.0)
    return round(deaths / pop, 1)


def _safety_level(rate_2021: float) -> str:
    if rate_2021 > 25.0:
        return "HIGH_RISK"
    if rate_2021 >= 15.0:
        return "MEDIUM_RISK"
    return "LOW_RISK"


def _trend(deaths_2021: int, deaths_2023: int) -> tuple[str, float]:
    pct = round((deaths_2023 - deaths_2021) / deaths_2021 * 100, 1)
    if pct <= -10.0:
        status = "IMPROVING"
    elif pct >= 15.0:
        status = "WORSENING"
    else:
        status = "STABLE"
    return status, pct


def _context(d: dict[str, Any]) -> str:
    rate = d["death_rate_per_lakh_2021"]
    trend = d["trend_2021_2023"]
    pct = d["trend_pct_change"]
    parts = []
    if rate > TN_AVG_DEATH_RATE_2021 * 1.4:
        parts.append(
            f"Road death rate ({rate}/lakh) is significantly above TN average ({TN_AVG_DEATH_RATE_2021})"
        )
    elif rate < TN_AVG_DEATH_RATE_2021 * 0.7:
        parts.append(
            f"Road death rate ({rate}/lakh) is well below TN average ({TN_AVG_DEATH_RATE_2021})"
        )
    else:
        parts.append(f"Road death rate: {rate} per lakh (TN avg: {TN_AVG_DEATH_RATE_2021})")

    if trend == "IMPROVING":
        parts.append(f"deaths declining ({pct:+.0f}% change 2021→2023)")
    elif trend == "WORSENING":
        parts.append(f"deaths rising sharply ({pct:+.0f}% change 2021→2023)")

    return "; ".join(parts) + "."


# ─────────────────────────────────────────────────────────────────────────────
# Raw data from CSV (district_name → accidents/deaths for 2021, 2022, 2023).
# Coimbatore and Madurai appear as single rows (district + city combined).
# Chennai is labelled "CHENNAI CITY" but is the only Chennai row.
# ─────────────────────────────────────────────────────────────────────────────
_RAW: list[tuple[str, str, int, int, int, int, int, int]] = [
    # (slug, name, acc21, dth21, acc22, dth22, acc23, dth23)
    ("ariyalur",       "Ariyalur",        545,   174,   562,   184,   584,   190),
    ("chengalpattu",   "Chengalpattu",   1614,   472,  3044,   937,  3402,   936),
    ("chennai",        "Chennai",        5035,   999,  3453,   507,  3654,   504),
    ("coimbatore",     "Coimbatore",     2792,   841,  3544,  1057,  3657,  1044),
    ("cuddalore",      "Cuddalore",      2927,   550,  3426,   478,  3121,   585),
    ("dharmapuri",     "Dharmapuri",     1240,   317,  1357,   389,  1420,   343),
    ("dindigul",       "Dindigul",       1747,   539,  2002,   680,  2296,   755),
    ("erode",          "Erode",          1852,   525,  2155,   661,  2396,   687),
    ("kallakurichi",   "Kallakurichi",   1070,   275,  1147,   284,  1166,   327),
    ("kancheepuram",   "Kancheepuram",    910,   240,  1011,   303,  1039,   297),
    ("kanniyakumari",  "Kanniyakumari",  1225,   321,  1381,   306,  1471,   350),
    ("karur",          "Karur",          1111,   413,  1101,   382,  1132,   362),
    ("krishnagiri",    "Krishnagiri",    1734,   589,  2029,   646,  2142,   656),
    ("madurai",        "Madurai",        2283,   711,  2489,   810,  2660,   890),
    ("nagapattinam",   "Nagapattinam",   1024,   225,   592,   134,   529,   134),
    ("namakkal",       "Namakkal",       1734,   422,  2021,   529,  2089,   584),
    ("perambalur",     "Perambalur",      511,   173,   550,   228,   529,   188),
    ("pudukkottai",    "Pudukkottai",    1369,   380,  1731,   497,  1681,   447),
    ("ramanathapuram", "Ramanathapuram", 1107,   335,  1211,   374,  1310,   426),
    ("ranipet",        "Ranipet",         833,   242,   858,   271,   911,   283),
    ("salem",          "Salem",          2606,   721,  3360,   868,  3199,   797),
    ("sivaganga",      "Sivagangai",      961,   337,  1143,   395,  1244,   392),
    ("tenkasi",        "Tenkasi",         979,   237,  1103,   283,  1117,   252),
    ("thanjavur",      "Thanjavur",      1849,   431,  2224,   551,  2327,   609),
    ("the_nilgiris",   "The Nilgiris",    210,    33,   236,    44,   213,    75),
    ("theni",          "Theni",           960,   239,  1121,   338,  1177,   327),
    ("thiruvallur",    "Thiruvallur",    1274,   331,  2595,   727,  2624,   730),
    ("thiruvarur",     "Thiruvarur",      621,   168,   727,   216,   893,   227),
    ("thoothukudi",    "Thoothukudi",    1253,   394,  1246,   457,  1440,   486),
    ("tiruchirappalli","Tiruchirappalli", 1953,   643,  2222,   714,  2426,   720),
    ("tirunelveli",    "Tirunelveli",    1280,   359,  1294,   408,  1531,   415),
    ("tirupathur",     "Tirupathur",      709,   226,   766,   217,   622,   217),
    ("tiruppur",       "Tiruppur",       2703,   798,  3145,   899,  3294,   875),
    ("tiruvannamalai", "Tiruvannamalai", 1435,   522,  1762,   565,  1913,   697),
    ("vellore",        "Vellore",         819,   246,   976,   294,   992,   328),
    ("villuppuram",    "Villupuram",     2131,   520,  2403,   583,  2591,   564),
    ("virudhunagar",   "Virudhunagar",   1276,   436,  1458,   515,  1650,   502),
]


def _build_records() -> list[dict[str, Any]]:
    records = []
    for slug, name, acc21, dth21, acc22, dth22, acc23, dth23 in _RAW:
        rate21 = _rate(dth21, slug)
        rate22 = _rate(dth22, slug)
        rate23 = _rate(dth23, slug)
        trend, pct = _trend(dth21, dth23)
        rec: dict[str, Any] = {
            "doc_id":              slug,
            "district_slug":       slug,
            "district_name":       name,
            "year_range":          "2021-2023",
            "population_lakhs":    POPULATION.get(slug, 0.0),

            "accidents_2021":      acc21,
            "deaths_2021":         dth21,
            "death_rate_per_lakh_2021": rate21,

            "accidents_2022":      acc22,
            "deaths_2022":         dth22,
            "death_rate_per_lakh_2022": rate22,

            "accidents_2023":      acc23,
            "deaths_2023":         dth23,
            "death_rate_per_lakh_2023": rate23,

            "road_safety_level":   _safety_level(rate21),
            "trend_2021_2023":     trend,
            "trend_pct_change":    pct,
        }
        rec["context"]  = _context(rec)
        rec["source_title"] = "TN Police Road Accidents Report via OpenCity"
        rec["source_url"]   = SOURCE_DATASET
        rec["source_csv"]   = SOURCE_CSV
        rec["ground_truth_confidence"] = "HIGH"
        rec["_uploaded_at"]    = NOW_ISO
        rec["_schema_version"] = "1.0"
        records.append(rec)
    return records


def _upload(records: list[dict[str, Any]], db: Any) -> None:
    col   = db.collection(COLLECTION)
    batch = db.batch()
    count = 0
    for record in records:
        r = dict(record)
        doc_id = r.pop("doc_id")
        batch.set(col.document(doc_id), r, merge=True)
        r["doc_id"] = doc_id
        count += 1
        if count % 400 == 0:
            batch.commit()
            batch = db.batch()
    batch.commit()
    print(f"  ✓ Wrote {count} docs to '{COLLECTION}'")


def run(dry_run: bool) -> None:
    records = _build_records()
    print(f"→ {COLLECTION} ({len(records)} districts)")
    if dry_run:
        print(f"  [dry-run] Would write {len(records)} docs")
        for r in records:
            print(
                f"    {r['doc_id']}: deaths/lakh {r['death_rate_per_lakh_2021']} "
                f"| {r['road_safety_level']} | {r['trend_2021_2023']} ({r['trend_pct_change']:+.0f}%)"
            )
        return

    if firestore is None:
        print("ERROR: google-cloud-firestore not installed")
        sys.exit(1)

    db = firestore.Client(project=PROJECT_ID)
    _upload(records, db)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest TN district road safety data into Firestore"
    )
    parser.add_argument("--dry-run", action="store_true", help="Print without writing")
    args = parser.parse_args()
    run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
