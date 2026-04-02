"""
District Crime Index Ingest — Tamil Nadu 2021
Populates the `district_crime_index` Firestore collection.

Source: Greater Chennai Police / SCRB via OpenCity
Dataset: https://data.opencity.in/dataset/tamil-nadu-crime-data

Data CSVs used:
  IPC Crimes 2019-2021    — total IPC cognizable crimes + crime rate per lakh
  Deaths due to Crime     — murder rate, road negligence deaths
  Sexual Harassment 2021  — rape, assault on women rates
  Theft and Robbery       — theft rate, robbery rate
  Suicides Data 2021      — total suicides by gender

Notes:
  - Districts with a separate city commissionerate (Coimbatore, Madurai, Salem,
    Tiruppur, Trichy, Tirunelveli) have their district + city rows combined.
  - Chennai has a single combined row in all CSVs.
  - Railway police units and CyberCell rows are excluded.
  - doc_id == district_slug so backend lookup works:
      _db.collection("district_crime_index").document(district_slug).get()

Usage:
  .venv/bin/python scrapers/district_crime_ingest.py --dry-run
  .venv/bin/python scrapers/district_crime_ingest.py
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

PROJECT_ID  = "naatunadappu"
COLLECTION  = "district_crime_index"
NOW_ISO     = datetime.now(timezone.utc).isoformat()

SOURCE_DATASET  = "https://data.opencity.in/dataset/tamil-nadu-crime-data"
SOURCE_IPC      = "https://data.opencity.in/dataset/1ae54d8f-6cbd-4cda-9021-d46314f2b17c/resource/2a6b9737-9ddb-4393-a3f5-db9a770014ff/download/8e47b896-53f4-4e9e-bda7-f23b7dbe23f2.csv"
SOURCE_DEATHS   = "https://data.opencity.in/dataset/1ae54d8f-6cbd-4cda-9021-d46314f2b17c/resource/4e55bc7c-7d4c-4241-a854-8df6458beda0/download/9e487b59-d50e-462f-b0c9-bfe7fde85ff3.csv"
SOURCE_SH       = "https://data.opencity.in/dataset/1ae54d8f-6cbd-4cda-9021-d46314f2b17c/resource/d33de71f-2a48-4a39-8de1-d54717515737/download/f3a241ff-63be-41e7-9110-7d4475214bb1.csv"
SOURCE_THEFT    = "https://data.opencity.in/dataset/1ae54d8f-6cbd-4cda-9021-d46314f2b17c/resource/d342a54b-9e23-4d32-b435-86bff65dbc2a/download/b770d6df-25a7-4053-8b70-3484e4b9c79a.csv"
SOURCE_SUICIDES = "https://data.opencity.in/dataset/1ae54d8f-6cbd-4cda-9021-d46314f2b17c/resource/1b00a8ed-808f-4cd5-97e7-f3683a6ccfe4/download/de15d061-13d2-4eea-9add-e5525b502329.csv"

# TN state averages (2021) — for context generation
TN_AVG_IPC_RATE    = 422.1
TN_AVG_MURDER_RATE = 2.2
TN_AVG_RAPE_RATE   = 1.1
TN_AVG_THEFT_RATE  = 18.7
TN_AVG_NEG_DEATH_RATE = 19.6


def _crime_level(rate: float) -> str:
    """Classify IPC crime rate relative to TN average."""
    if rate > 600:
        return "HIGH"
    if rate > 350:
        return "MEDIUM"
    return "LOW"


def _context(d: dict[str, Any]) -> str:
    """Generate a brief data-driven context string."""
    parts = []
    rate = d["ipc_crime_rate_per_lakh"]
    if rate > TN_AVG_IPC_RATE * 1.5:
        parts.append(f"IPC crime rate ({rate:.0f}/lakh) is significantly above TN average ({TN_AVG_IPC_RATE})")
    elif rate < TN_AVG_IPC_RATE * 0.6:
        parts.append(f"IPC crime rate ({rate:.0f}/lakh) is well below TN average ({TN_AVG_IPC_RATE})")
    else:
        parts.append(f"IPC crime rate: {rate:.0f} per lakh (TN avg: {TN_AVG_IPC_RATE})")

    mr = d["murder_rate_per_lakh"]
    if mr > TN_AVG_MURDER_RATE * 1.5:
        parts.append(f"elevated murder rate ({mr}/lakh)")

    wr = d["assault_on_women_rate_per_lakh"]
    if wr > 4.0:
        parts.append(f"high assault-on-women rate ({wr}/lakh)")

    nd = d["negligence_death_rate_per_lakh"]
    if nd > TN_AVG_NEG_DEATH_RATE * 1.4:
        parts.append(f"high road negligence death rate ({nd}/lakh)")

    return "; ".join(parts) + "."


# ─────────────────────────────────────────────────────────────────────────────
# Curated district data — all combined (district + city commissionerate).
# Fields:
#   ipc_crimes_total        — absolute 2021 count
#   ipc_crime_rate_per_lakh — per lakh population (2021 mid-year projected)
#   population_lakhs        — combined district + city population
#   murder_incidents / murder_rate_per_lakh
#   rape_incidents / rape_rate_per_lakh
#   assault_on_women_incidents / assault_on_women_rate_per_lakh
#   theft_incidents / theft_rate_per_lakh
#   robbery_incidents / robbery_rate_per_lakh
#   negligence_deaths / negligence_death_rate_per_lakh  (road + other negligence)
#   suicides_total / suicides_male / suicides_female
# ─────────────────────────────────────────────────────────────────────────────

DISTRICT_CRIME_INDEX: list[dict[str, Any]] = [
    # ── ariyalur ──────────────────────────────────────────────────────────────
    {
        "doc_id": "ariyalur", "district_slug": "ariyalur", "district_name": "Ariyalur",
        "population_lakhs": 8.0,
        "ipc_crimes_total": 4242, "ipc_crime_rate_per_lakh": 530.1,
        "murder_incidents": 18, "murder_rate_per_lakh": 2.3,
        "rape_incidents": 14, "rape_rate_per_lakh": 3.5,
        "assault_on_women_incidents": 8, "assault_on_women_rate_per_lakh": 2.0,
        "theft_incidents": 77, "theft_rate_per_lakh": 9.6,
        "robbery_incidents": 18, "robbery_rate_per_lakh": 2.3,
        "negligence_deaths": 174, "negligence_death_rate_per_lakh": 20.1,
        "suicides_total": 228, "suicides_male": 135, "suicides_female": 93,
    },
    # ── chengalpattu ──────────────────────────────────────────────────────────
    {
        "doc_id": "chengalpattu", "district_slug": "chengalpattu", "district_name": "Chengalpattu",
        "population_lakhs": 24.2,
        "ipc_crimes_total": 8076, "ipc_crime_rate_per_lakh": 334.2,
        "murder_incidents": 27, "murder_rate_per_lakh": 1.1,
        "rape_incidents": 8, "rape_rate_per_lakh": 0.7,
        "assault_on_women_incidents": 15, "assault_on_women_rate_per_lakh": 1.2,
        "theft_incidents": 155, "theft_rate_per_lakh": 6.4,
        "robbery_incidents": 53, "robbery_rate_per_lakh": 2.2,
        "negligence_deaths": 472, "negligence_death_rate_per_lakh": 18.7,
        "suicides_total": 338, "suicides_male": 238, "suicides_female": 100,
    },
    # ── chennai (combined — single commissionerate covers full district) ──────
    {
        "doc_id": "chennai", "district_slug": "chennai", "district_name": "Chennai",
        "population_lakhs": 72.3,
        "ipc_crimes_total": 46077, "ipc_crime_rate_per_lakh": 637.2,
        "murder_incidents": 161, "murder_rate_per_lakh": 2.2,
        "rape_incidents": 45, "rape_rate_per_lakh": 1.2,
        "assault_on_women_incidents": 146, "assault_on_women_rate_per_lakh": 4.0,
        "theft_incidents": 4054, "theft_rate_per_lakh": 56.1,
        "robbery_incidents": 609, "robbery_rate_per_lakh": 8.4,
        "negligence_deaths": 1121, "negligence_death_rate_per_lakh": 15.1,
        "suicides_total": 2699, "suicides_male": 2008, "suicides_female": 687,
    },
    # ── coimbatore (district 24.6L + city 12L = 36.6L combined) ──────────────
    {
        "doc_id": "coimbatore", "district_slug": "coimbatore", "district_name": "Coimbatore",
        "population_lakhs": 36.6,
        "ipc_crimes_total": 13070, "ipc_crime_rate_per_lakh": 357.1,
        "murder_incidents": 86, "murder_rate_per_lakh": 2.3,
        "rape_incidents": 20, "rape_rate_per_lakh": 0.5,
        "assault_on_women_incidents": 18, "assault_on_women_rate_per_lakh": 0.5,
        "theft_incidents": 762, "theft_rate_per_lakh": 20.8,
        "robbery_incidents": 150, "robbery_rate_per_lakh": 4.1,
        "negligence_deaths": 876, "negligence_death_rate_per_lakh": 23.9,
        "suicides_total": 1160, "suicides_male": 855, "suicides_female": 305,
    },
    # ── cuddalore ─────────────────────────────────────────────────────────────
    {
        "doc_id": "cuddalore", "district_slug": "cuddalore", "district_name": "Cuddalore",
        "population_lakhs": 27.6,
        "ipc_crimes_total": 16045, "ipc_crime_rate_per_lakh": 580.8,
        "murder_incidents": 46, "murder_rate_per_lakh": 1.7,
        "rape_incidents": 27, "rape_rate_per_lakh": 2.0,
        "assault_on_women_incidents": 23, "assault_on_women_rate_per_lakh": 1.7,
        "theft_incidents": 252, "theft_rate_per_lakh": 9.1,
        "robbery_incidents": 36, "robbery_rate_per_lakh": 1.3,
        "negligence_deaths": 550, "negligence_death_rate_per_lakh": 19.0,
        "suicides_total": 533, "suicides_male": 386, "suicides_female": 147,
    },
    # ── dharmapuri ────────────────────────────────────────────────────────────
    {
        "doc_id": "dharmapuri", "district_slug": "dharmapuri", "district_name": "Dharmapuri",
        "population_lakhs": 16.0,
        "ipc_crimes_total": 5523, "ipc_crime_rate_per_lakh": 345.8,
        "murder_incidents": 28, "murder_rate_per_lakh": 1.8,
        "rape_incidents": 6, "rape_rate_per_lakh": 0.8,
        "assault_on_women_incidents": 18, "assault_on_women_rate_per_lakh": 2.3,
        "theft_incidents": 110, "theft_rate_per_lakh": 6.9,
        "robbery_incidents": 14, "robbery_rate_per_lakh": 0.9,
        "negligence_deaths": 322, "negligence_death_rate_per_lakh": 19.6,
        "suicides_total": 327, "suicides_male": 197, "suicides_female": 130,
    },
    # ── dindigul ──────────────────────────────────────────────────────────────
    {
        "doc_id": "dindigul", "district_slug": "dindigul", "district_name": "Dindigul",
        "population_lakhs": 22.9,
        "ipc_crimes_total": 12981, "ipc_crime_rate_per_lakh": 567.0,
        "murder_incidents": 47, "murder_rate_per_lakh": 2.1,
        "rape_incidents": 5, "rape_rate_per_lakh": 0.4,
        "assault_on_women_incidents": 40, "assault_on_women_rate_per_lakh": 3.5,
        "theft_incidents": 260, "theft_rate_per_lakh": 11.4,
        "robbery_incidents": 39, "robbery_rate_per_lakh": 1.7,
        "negligence_deaths": 539, "negligence_death_rate_per_lakh": 22.2,
        "suicides_total": 388, "suicides_male": 260, "suicides_female": 128,
    },
    # ── erode ─────────────────────────────────────────────────────────────────
    {
        "doc_id": "erode", "district_slug": "erode", "district_name": "Erode",
        "population_lakhs": 23.9,
        "ipc_crimes_total": 3818, "ipc_crime_rate_per_lakh": 160.0,
        "murder_incidents": 44, "murder_rate_per_lakh": 1.8,
        "rape_incidents": 5, "rape_rate_per_lakh": 0.4,
        "assault_on_women_incidents": 12, "assault_on_women_rate_per_lakh": 1.0,
        "theft_incidents": 297, "theft_rate_per_lakh": 12.4,
        "robbery_incidents": 31, "robbery_rate_per_lakh": 1.3,
        "negligence_deaths": 525, "negligence_death_rate_per_lakh": 20.7,
        "suicides_total": 445, "suicides_male": 297, "suicides_female": 148,
    },
    # ── kallakurichi ──────────────────────────────────────────────────────────
    {
        "doc_id": "kallakurichi", "district_slug": "kallakurichi", "district_name": "Kallakurichi",
        "population_lakhs": 14.8,
        "ipc_crimes_total": 5166, "ipc_crime_rate_per_lakh": 349.8,
        "murder_incidents": 29, "murder_rate_per_lakh": 2.0,
        "rape_incidents": 15, "rape_rate_per_lakh": 2.0,
        "assault_on_women_incidents": 7, "assault_on_women_rate_per_lakh": 0.9,
        "theft_incidents": 114, "theft_rate_per_lakh": 7.7,
        "robbery_incidents": 31, "robbery_rate_per_lakh": 2.1,
        "negligence_deaths": 275, "negligence_death_rate_per_lakh": 17.2,
        "suicides_total": 278, "suicides_male": 184, "suicides_female": 94,
    },
    # ── kancheepuram ──────────────────────────────────────────────────────────
    {
        "doc_id": "kancheepuram", "district_slug": "kancheepuram", "district_name": "Kancheepuram",
        "population_lakhs": 9.6,
        "ipc_crimes_total": 3300, "ipc_crime_rate_per_lakh": 342.3,
        "murder_incidents": 30, "murder_rate_per_lakh": 3.1,
        "rape_incidents": 6, "rape_rate_per_lakh": 1.3,
        "assault_on_women_incidents": 4, "assault_on_women_rate_per_lakh": 0.8,
        "theft_incidents": 167, "theft_rate_per_lakh": 17.4,
        "robbery_incidents": 64, "robbery_rate_per_lakh": 6.7,
        "negligence_deaths": 240, "negligence_death_rate_per_lakh": 24.3,
        "suicides_total": 214, "suicides_male": 167, "suicides_female": 47,
    },
    # ── kanniyakumari ─────────────────────────────────────────────────────────
    {
        "doc_id": "kanniyakumari", "district_slug": "kanniyakumari", "district_name": "Kanniyakumari",
        "population_lakhs": 19.8,
        "ipc_crimes_total": 4291, "ipc_crime_rate_per_lakh": 216.4,
        "murder_incidents": 28, "murder_rate_per_lakh": 1.4,
        "rape_incidents": 9, "rape_rate_per_lakh": 0.9,
        "assault_on_women_incidents": 32, "assault_on_women_rate_per_lakh": 3.2,
        "theft_incidents": 421, "theft_rate_per_lakh": 21.3,
        "robbery_incidents": 39, "robbery_rate_per_lakh": 2.0,
        "negligence_deaths": 321, "negligence_death_rate_per_lakh": 15.5,
        "suicides_total": 646, "suicides_male": 538, "suicides_female": 108,
    },
    # ── karur ─────────────────────────────────────────────────────────────────
    {
        "doc_id": "karur", "district_slug": "karur", "district_name": "Karur",
        "population_lakhs": 11.3,
        "ipc_crimes_total": 6835, "ipc_crime_rate_per_lakh": 605.7,
        "murder_incidents": 22, "murder_rate_per_lakh": 1.9,
        "rape_incidents": 3, "rape_rate_per_lakh": 0.5,
        "assault_on_women_incidents": 7, "assault_on_women_rate_per_lakh": 1.3,
        "theft_incidents": 128, "theft_rate_per_lakh": 11.3,
        "robbery_incidents": 31, "robbery_rate_per_lakh": 2.7,
        "negligence_deaths": 420, "negligence_death_rate_per_lakh": 35.4,
        "suicides_total": 246, "suicides_male": 167, "suicides_female": 79,
    },
    # ── krishnagiri ───────────────────────────────────────────────────────────
    {
        "doc_id": "krishnagiri", "district_slug": "krishnagiri", "district_name": "Krishnagiri",
        "population_lakhs": 19.9,
        "ipc_crimes_total": 4301, "ipc_crime_rate_per_lakh": 215.8,
        "murder_incidents": 66, "murder_rate_per_lakh": 3.3,
        "rape_incidents": 7, "rape_rate_per_lakh": 0.7,
        "assault_on_women_incidents": 11, "assault_on_women_rate_per_lakh": 1.1,
        "theft_incidents": 169, "theft_rate_per_lakh": 8.5,
        "robbery_incidents": 34, "robbery_rate_per_lakh": 1.7,
        "negligence_deaths": 600, "negligence_death_rate_per_lakh": 27.8,
        "suicides_total": 502, "suicides_male": 331, "suicides_female": 171,
    },
    # ── madurai (district 20.6L + city 11.6L = 32.2L combined) ───────────────
    {
        "doc_id": "madurai", "district_slug": "madurai", "district_name": "Madurai",
        "population_lakhs": 32.2,
        "ipc_crimes_total": 13175, "ipc_crime_rate_per_lakh": 409.2,
        "murder_incidents": 102, "murder_rate_per_lakh": 3.2,
        "rape_incidents": 22, "rape_rate_per_lakh": 0.7,
        "assault_on_women_incidents": 52, "assault_on_women_rate_per_lakh": 1.6,
        "theft_incidents": 870, "theft_rate_per_lakh": 27.0,
        "robbery_incidents": 256, "robbery_rate_per_lakh": 8.0,
        "negligence_deaths": 718, "negligence_death_rate_per_lakh": 22.3,
        "suicides_total": 845, "suicides_male": 587, "suicides_female": 258,
    },
    # ── nagapattinam ──────────────────────────────────────────────────────────
    {
        "doc_id": "nagapattinam", "district_slug": "nagapattinam", "district_name": "Nagapattinam",
        "population_lakhs": 17.1,
        "ipc_crimes_total": 14082, "ipc_crime_rate_per_lakh": 821.8,
        "murder_incidents": 46, "murder_rate_per_lakh": 2.7,
        "rape_incidents": 16, "rape_rate_per_lakh": 1.9,
        "assault_on_women_incidents": 54, "assault_on_women_rate_per_lakh": 6.3,
        "theft_incidents": 145, "theft_rate_per_lakh": 8.5,
        "robbery_incidents": 28, "robbery_rate_per_lakh": 1.6,
        "negligence_deaths": 225, "negligence_death_rate_per_lakh": 12.9,
        "suicides_total": 264, "suicides_male": 144, "suicides_female": 120,
    },
    # ── namakkal ──────────────────────────────────────────────────────────────
    {
        "doc_id": "namakkal", "district_slug": "namakkal", "district_name": "Namakkal",
        "population_lakhs": 18.3,
        "ipc_crimes_total": 6089, "ipc_crime_rate_per_lakh": 332.7,
        "murder_incidents": 41, "murder_rate_per_lakh": 2.2,
        "rape_incidents": 4, "rape_rate_per_lakh": 0.4,
        "assault_on_women_incidents": 31, "assault_on_women_rate_per_lakh": 3.4,
        "theft_incidents": 118, "theft_rate_per_lakh": 6.4,
        "robbery_incidents": 26, "robbery_rate_per_lakh": 1.4,
        "negligence_deaths": 431, "negligence_death_rate_per_lakh": 23.1,
        "suicides_total": 396, "suicides_male": 261, "suicides_female": 135,
    },
    # ── perambalur ────────────────────────────────────────────────────────────
    {
        "doc_id": "perambalur", "district_slug": "perambalur", "district_name": "Perambalur",
        "population_lakhs": 6.0,
        "ipc_crimes_total": 3660, "ipc_crime_rate_per_lakh": 610.9,
        "murder_incidents": 20, "murder_rate_per_lakh": 3.3,
        "rape_incidents": 2, "rape_rate_per_lakh": 0.7,
        "assault_on_women_incidents": 19, "assault_on_women_rate_per_lakh": 6.3,
        "theft_incidents": 129, "theft_rate_per_lakh": 21.5,
        "robbery_incidents": 33, "robbery_rate_per_lakh": 5.5,
        "negligence_deaths": 173, "negligence_death_rate_per_lakh": 27.3,
        "suicides_total": 291, "suicides_male": 191, "suicides_female": 100,
    },
    # ── pudukkottai ───────────────────────────────────────────────────────────
    {
        "doc_id": "pudukkottai", "district_slug": "pudukkottai", "district_name": "Pudukkottai",
        "population_lakhs": 17.2,
        "ipc_crimes_total": 9474, "ipc_crime_rate_per_lakh": 552.3,
        "murder_incidents": 30, "murder_rate_per_lakh": 1.7,
        "rape_incidents": 8, "rape_rate_per_lakh": 0.9,
        "assault_on_women_incidents": 57, "assault_on_women_rate_per_lakh": 6.6,
        "theft_incidents": 282, "theft_rate_per_lakh": 16.4,
        "robbery_incidents": 39, "robbery_rate_per_lakh": 2.3,
        "negligence_deaths": 390, "negligence_death_rate_per_lakh": 21.5,
        "suicides_total": 286, "suicides_male": 182, "suicides_female": 104,
    },
    # ── ramanathapuram ────────────────────────────────────────────────────────
    {
        "doc_id": "ramanathapuram", "district_slug": "ramanathapuram", "district_name": "Ramanathapuram",
        "population_lakhs": 14.3,
        "ipc_crimes_total": 4436, "ipc_crime_rate_per_lakh": 309.2,
        "murder_incidents": 56, "murder_rate_per_lakh": 3.9,
        "rape_incidents": 8, "rape_rate_per_lakh": 1.1,
        "assault_on_women_incidents": 23, "assault_on_women_rate_per_lakh": 3.2,
        "theft_incidents": 227, "theft_rate_per_lakh": 15.9,
        "robbery_incidents": 54, "robbery_rate_per_lakh": 3.8,
        "negligence_deaths": 335, "negligence_death_rate_per_lakh": 22.2,
        "suicides_total": 298, "suicides_male": 216, "suicides_female": 82,
    },
    # ── ranipet ───────────────────────────────────────────────────────────────
    {
        "doc_id": "ranipet", "district_slug": "ranipet", "district_name": "Ranipet",
        "population_lakhs": 12.7,
        "ipc_crimes_total": 3822, "ipc_crime_rate_per_lakh": 300.6,
        "murder_incidents": 23, "murder_rate_per_lakh": 1.8,
        "rape_incidents": 1, "rape_rate_per_lakh": 0.2,
        "assault_on_women_incidents": 13, "assault_on_women_rate_per_lakh": 2.0,
        "theft_incidents": 149, "theft_rate_per_lakh": 11.7,
        "robbery_incidents": 34, "robbery_rate_per_lakh": 2.7,
        "negligence_deaths": 242, "negligence_death_rate_per_lakh": 18.8,
        "suicides_total": 228, "suicides_male": 166, "suicides_female": 61,
    },
    # ── salem (district 27.4L + city 9.5L = 36.9L combined) ──────────────────
    {
        "doc_id": "salem", "district_slug": "salem", "district_name": "Salem",
        "population_lakhs": 36.9,
        "ipc_crimes_total": 7952, "ipc_crime_rate_per_lakh": 215.5,
        "murder_incidents": 76, "murder_rate_per_lakh": 2.1,
        "rape_incidents": 11, "rape_rate_per_lakh": 0.3,
        "assault_on_women_incidents": 88, "assault_on_women_rate_per_lakh": 2.4,
        "theft_incidents": 462, "theft_rate_per_lakh": 12.5,
        "robbery_incidents": 100, "robbery_rate_per_lakh": 2.7,
        "negligence_deaths": 723, "negligence_death_rate_per_lakh": 19.6,
        "suicides_total": 595, "suicides_male": 364, "suicides_female": 231,
    },
    # ── sivaganga ─────────────────────────────────────────────────────────────
    {
        "doc_id": "sivaganga", "district_slug": "sivaganga", "district_name": "Sivagangai",
        "population_lakhs": 14.2,
        "ipc_crimes_total": 5672, "ipc_crime_rate_per_lakh": 399.6,
        "murder_incidents": 42, "murder_rate_per_lakh": 3.0,
        "rape_incidents": 13, "rape_rate_per_lakh": 1.8,
        "assault_on_women_incidents": 21, "assault_on_women_rate_per_lakh": 3.0,
        "theft_incidents": 185, "theft_rate_per_lakh": 13.0,
        "robbery_incidents": 57, "robbery_rate_per_lakh": 4.0,
        "negligence_deaths": 337, "negligence_death_rate_per_lakh": 22.5,
        "suicides_total": 380, "suicides_male": 269, "suicides_female": 110,
    },
    # ── tenkasi ───────────────────────────────────────────────────────────────
    {
        "doc_id": "tenkasi", "district_slug": "tenkasi", "district_name": "Tenkasi",
        "population_lakhs": 15.1,
        "ipc_crimes_total": 4616, "ipc_crime_rate_per_lakh": 305.4,
        "murder_incidents": 48, "murder_rate_per_lakh": 3.2,
        "rape_incidents": 3, "rape_rate_per_lakh": 0.4,
        "assault_on_women_incidents": 22, "assault_on_women_rate_per_lakh": 2.9,
        "theft_incidents": 186, "theft_rate_per_lakh": 12.3,
        "robbery_incidents": 13, "robbery_rate_per_lakh": 0.9,
        "negligence_deaths": 238, "negligence_death_rate_per_lakh": 15.0,
        "suicides_total": 431, "suicides_male": 303, "suicides_female": 128,
    },
    # ── thanjavur ─────────────────────────────────────────────────────────────
    {
        "doc_id": "thanjavur", "district_slug": "thanjavur", "district_name": "Thanjavur",
        "population_lakhs": 25.5,
        "ipc_crimes_total": 23102, "ipc_crime_rate_per_lakh": 905.8,
        "murder_incidents": 57, "murder_rate_per_lakh": 2.2,
        "rape_incidents": 13, "rape_rate_per_lakh": 1.0,
        "assault_on_women_incidents": 88, "assault_on_women_rate_per_lakh": 6.9,
        "theft_incidents": 289, "theft_rate_per_lakh": 11.3,
        "robbery_incidents": 102, "robbery_rate_per_lakh": 4.0,
        "negligence_deaths": 431, "negligence_death_rate_per_lakh": 16.3,
        "suicides_total": 670, "suicides_male": 461, "suicides_female": 209,
    },
    # ── theni ─────────────────────────────────────────────────────────────────
    {
        "doc_id": "theni", "district_slug": "theni", "district_name": "Theni",
        "population_lakhs": 13.2,
        "ipc_crimes_total": 9407, "ipc_crime_rate_per_lakh": 712.3,
        "murder_incidents": 36, "murder_rate_per_lakh": 2.7,
        "rape_incidents": 7, "rape_rate_per_lakh": 1.1,
        "assault_on_women_incidents": 27, "assault_on_women_rate_per_lakh": 4.1,
        "theft_incidents": 136, "theft_rate_per_lakh": 10.3,
        "robbery_incidents": 18, "robbery_rate_per_lakh": 1.4,
        "negligence_deaths": 239, "negligence_death_rate_per_lakh": 17.1,
        "suicides_total": 456, "suicides_male": 308, "suicides_female": 148,
    },
    # ── the_nilgiris ──────────────────────────────────────────────────────────
    {
        "doc_id": "the_nilgiris", "district_slug": "the_nilgiris", "district_name": "The Nilgiris",
        "population_lakhs": 7.8,
        "ipc_crimes_total": 1208, "ipc_crime_rate_per_lakh": 155.0,
        "murder_incidents": 15, "murder_rate_per_lakh": 1.9,
        "rape_incidents": 5, "rape_rate_per_lakh": 1.3,
        "assault_on_women_incidents": 9, "assault_on_women_rate_per_lakh": 2.3,
        "theft_incidents": 48, "theft_rate_per_lakh": 6.2,
        "robbery_incidents": 4, "robbery_rate_per_lakh": 0.5,
        "negligence_deaths": 33, "negligence_death_rate_per_lakh": 4.1,
        "suicides_total": 251, "suicides_male": 192, "suicides_female": 59,
    },
    # ── thiruvallur ───────────────────────────────────────────────────────────
    {
        "doc_id": "thiruvallur", "district_slug": "thiruvallur", "district_name": "Thiruvallur",
        "population_lakhs": 25.0,
        "ipc_crimes_total": 3751, "ipc_crime_rate_per_lakh": 149.8,
        "murder_incidents": 38, "murder_rate_per_lakh": 1.5,
        "rape_incidents": 15, "rape_rate_per_lakh": 1.2,
        "assault_on_women_incidents": 13, "assault_on_women_rate_per_lakh": 1.0,
        "theft_incidents": 148, "theft_rate_per_lakh": 5.9,
        "robbery_incidents": 56, "robbery_rate_per_lakh": 2.2,
        "negligence_deaths": 336, "negligence_death_rate_per_lakh": 12.9,
        "suicides_total": 393, "suicides_male": 272, "suicides_female": 121,
    },
    # ── thiruvarur ────────────────────────────────────────────────────────────
    {
        "doc_id": "thiruvarur", "district_slug": "thiruvarur", "district_name": "Thiruvarur",
        "population_lakhs": 13.4,
        "ipc_crimes_total": 3151, "ipc_crime_rate_per_lakh": 235.1,
        "murder_incidents": 28, "murder_rate_per_lakh": 2.1,
        "rape_incidents": 10, "rape_rate_per_lakh": 1.5,
        "assault_on_women_incidents": 22, "assault_on_women_rate_per_lakh": 3.3,
        "theft_incidents": 81, "theft_rate_per_lakh": 6.0,
        "robbery_incidents": 33, "robbery_rate_per_lakh": 2.5,
        "negligence_deaths": 168, "negligence_death_rate_per_lakh": 12.1,
        "suicides_total": 368, "suicides_male": 265, "suicides_female": 101,
    },
    # ── thoothukudi ───────────────────────────────────────────────────────────
    {
        "doc_id": "thoothukudi", "district_slug": "thoothukudi", "district_name": "Thoothukudi",
        "population_lakhs": 18.6,
        "ipc_crimes_total": 6158, "ipc_crime_rate_per_lakh": 331.9,
        "murder_incidents": 50, "murder_rate_per_lakh": 2.7,
        "rape_incidents": 15, "rape_rate_per_lakh": 1.6,
        "assault_on_women_incidents": 36, "assault_on_women_rate_per_lakh": 3.9,
        "theft_incidents": 315, "theft_rate_per_lakh": 16.9,
        "robbery_incidents": 46, "robbery_rate_per_lakh": 2.5,
        "negligence_deaths": 407, "negligence_death_rate_per_lakh": 20.6,
        "suicides_total": 681, "suicides_male": 470, "suicides_female": 210,
    },
    # ── tiruchirappalli (district 19.2L + city 9.7L = 28.9L combined) ─────────
    {
        "doc_id": "tiruchirappalli", "district_slug": "tiruchirappalli", "district_name": "Tiruchirappalli",
        "population_lakhs": 28.9,
        "ipc_crimes_total": 12289, "ipc_crime_rate_per_lakh": 425.2,
        "murder_incidents": 63, "murder_rate_per_lakh": 2.2,
        "rape_incidents": 20, "rape_rate_per_lakh": 0.7,
        "assault_on_women_incidents": 43, "assault_on_women_rate_per_lakh": 1.5,
        "theft_incidents": 585, "theft_rate_per_lakh": 20.2,
        "robbery_incidents": 97, "robbery_rate_per_lakh": 3.4,
        "negligence_deaths": 664, "negligence_death_rate_per_lakh": 23.0,
        "suicides_total": 544, "suicides_male": 347, "suicides_female": 197,
    },
    # ── tirunelveli (district 12.1L + city 5.4L = 17.5L combined) ────────────
    {
        "doc_id": "tirunelveli", "district_slug": "tirunelveli", "district_name": "Tirunelveli",
        "population_lakhs": 17.5,
        "ipc_crimes_total": 7856, "ipc_crime_rate_per_lakh": 449.5,
        "murder_incidents": 63, "murder_rate_per_lakh": 3.6,
        "rape_incidents": 8, "rape_rate_per_lakh": 0.5,
        "assault_on_women_incidents": 39, "assault_on_women_rate_per_lakh": 2.2,
        "theft_incidents": 369, "theft_rate_per_lakh": 21.1,
        "robbery_incidents": 48, "robbery_rate_per_lakh": 2.7,
        "negligence_deaths": 363, "negligence_death_rate_per_lakh": 20.7,
        "suicides_total": 541, "suicides_male": 385, "suicides_female": 156,
    },
    # ── tirupathur ────────────────────────────────────────────────────────────
    {
        "doc_id": "tirupathur", "district_slug": "tirupathur", "district_name": "Tirupathur",
        "population_lakhs": 12.5,
        "ipc_crimes_total": 3346, "ipc_crime_rate_per_lakh": 267.4,
        "murder_incidents": 23, "murder_rate_per_lakh": 1.8,
        "rape_incidents": 3, "rape_rate_per_lakh": 0.5,
        "assault_on_women_incidents": 11, "assault_on_women_rate_per_lakh": 1.7,
        "theft_incidents": 133, "theft_rate_per_lakh": 10.6,
        "robbery_incidents": 25, "robbery_rate_per_lakh": 2.0,
        "negligence_deaths": 226, "negligence_death_rate_per_lakh": 17.4,
        "suicides_total": 247, "suicides_male": 159, "suicides_female": 88,
    },
    # ── tiruppur (district 21.2L + city 5.1L = 26.3L combined) ──────────────
    {
        "doc_id": "tiruppur", "district_slug": "tiruppur", "district_name": "Tiruppur",
        "population_lakhs": 26.3,
        "ipc_crimes_total": 20285, "ipc_crime_rate_per_lakh": 771.4,
        "murder_incidents": 61, "murder_rate_per_lakh": 2.3,
        "rape_incidents": 7, "rape_rate_per_lakh": 0.3,
        "assault_on_women_incidents": 11, "assault_on_women_rate_per_lakh": 0.4,
        "theft_incidents": 409, "theft_rate_per_lakh": 15.6,
        "robbery_incidents": 101, "robbery_rate_per_lakh": 3.8,
        "negligence_deaths": 814, "negligence_death_rate_per_lakh": 31.0,
        "suicides_total": 905, "suicides_male": 593, "suicides_female": 311,
    },
    # ── tiruvannamalai ────────────────────────────────────────────────────────
    {
        "doc_id": "tiruvannamalai", "district_slug": "tiruvannamalai", "district_name": "Tiruvannamalai",
        "population_lakhs": 26.1,
        "ipc_crimes_total": 12931, "ipc_crime_rate_per_lakh": 494.9,
        "murder_incidents": 36, "murder_rate_per_lakh": 1.4,
        "rape_incidents": 21, "rape_rate_per_lakh": 1.6,
        "assault_on_women_incidents": 12, "assault_on_women_rate_per_lakh": 0.9,
        "theft_incidents": 179, "theft_rate_per_lakh": 6.9,
        "robbery_incidents": 54, "robbery_rate_per_lakh": 2.1,
        "negligence_deaths": 522, "negligence_death_rate_per_lakh": 18.8,
        "suicides_total": 506, "suicides_male": 320, "suicides_female": 186,
    },
    # ── vellore ───────────────────────────────────────────────────────────────
    {
        "doc_id": "vellore", "district_slug": "vellore", "district_name": "Vellore",
        "population_lakhs": 16.5,
        "ipc_crimes_total": 2050, "ipc_crime_rate_per_lakh": 124.3,
        "murder_incidents": 26, "murder_rate_per_lakh": 1.6,
        "rape_incidents": 2, "rape_rate_per_lakh": 0.2,
        "assault_on_women_incidents": 9, "assault_on_women_rate_per_lakh": 1.1,
        "theft_incidents": 157, "theft_rate_per_lakh": 9.5,
        "robbery_incidents": 38, "robbery_rate_per_lakh": 2.3,
        "negligence_deaths": 246, "negligence_death_rate_per_lakh": 14.4,
        "suicides_total": 281, "suicides_male": 179, "suicides_female": 102,
    },
    # ── villuppuram ───────────────────────────────────────────────────────────
    {
        "doc_id": "villuppuram", "district_slug": "villuppuram", "district_name": "Villupuram",
        "population_lakhs": 21.9,
        "ipc_crimes_total": 5587, "ipc_crime_rate_per_lakh": 255.2,
        "murder_incidents": 25, "murder_rate_per_lakh": 1.1,
        "rape_incidents": 19, "rape_rate_per_lakh": 1.7,
        "assault_on_women_incidents": 17, "assault_on_women_rate_per_lakh": 1.6,
        "theft_incidents": 242, "theft_rate_per_lakh": 11.1,
        "robbery_incidents": 56, "robbery_rate_per_lakh": 2.6,
        "negligence_deaths": 520, "negligence_death_rate_per_lakh": 23.0,
        "suicides_total": 388, "suicides_male": 277, "suicides_female": 111,
    },
    # ── virudhunagar ──────────────────────────────────────────────────────────
    {
        "doc_id": "virudhunagar", "district_slug": "virudhunagar", "district_name": "Virudhunagar",
        "population_lakhs": 20.6,
        "ipc_crimes_total": 3593, "ipc_crime_rate_per_lakh": 174.5,
        "murder_incidents": 46, "murder_rate_per_lakh": 2.2,
        "rape_incidents": 18, "rape_rate_per_lakh": 1.7,
        "assault_on_women_incidents": 15, "assault_on_women_rate_per_lakh": 1.5,
        "theft_incidents": 195, "theft_rate_per_lakh": 9.5,
        "robbery_incidents": 41, "robbery_rate_per_lakh": 2.0,
        "negligence_deaths": 436, "negligence_death_rate_per_lakh": 20.0,
        "suicides_total": 471, "suicides_male": 326, "suicides_female": 145,
    },
]


def _enrich(record: dict[str, Any]) -> dict[str, Any]:
    """Add computed fields, context, and metadata to each record."""
    r = dict(record)
    r["year"] = 2021
    r["crime_index_level"] = _crime_level(r["ipc_crime_rate_per_lakh"])
    r["context"] = _context(r)
    r["source_title"] = "TN Police / SCRB via OpenCity Tamil Nadu Crime Data"
    r["source_url"] = SOURCE_DATASET
    r["source_urls"] = {
        "ipc_crimes":        SOURCE_IPC,
        "deaths_negligence": SOURCE_DEATHS,
        "sexual_harassment": SOURCE_SH,
        "theft_robbery":     SOURCE_THEFT,
        "suicides":          SOURCE_SUICIDES,
    }
    r["ground_truth_confidence"] = "HIGH"
    r["_uploaded_at"] = NOW_ISO
    r["_schema_version"] = "1.0"
    return r


def _upload(records: list[dict[str, Any]], db: Any, dry_run: bool) -> None:
    col = db.collection(COLLECTION)
    batch = db.batch()
    count = 0

    for record in records:
        enriched = _enrich(record)
        doc_id = enriched.pop("doc_id")
        doc_ref = col.document(doc_id)
        batch.set(doc_ref, enriched, merge=True)
        enriched["doc_id"] = doc_id  # restore for logging
        count += 1
        if count % 400 == 0:
            batch.commit()
            batch = db.batch()

    batch.commit()
    print(f"  ✓ Wrote {count} docs to '{COLLECTION}'")


def run(dry_run: bool) -> None:
    records = DISTRICT_CRIME_INDEX
    print(f"→ {COLLECTION} ({len(records)} districts)")

    if dry_run:
        print(f"  [dry-run] Would write {len(records)} docs to '{COLLECTION}'")
        for r in records:
            enriched = _enrich(dict(r))
            print(f"    {r['doc_id']}: IPC rate {r['ipc_crime_rate_per_lakh']}/lakh "
                  f"| murder {r['murder_rate_per_lakh']} | {enriched['crime_index_level']}")
        return

    if firestore is None:
        print("ERROR: google-cloud-firestore not installed")
        sys.exit(1)

    db = firestore.Client(project=PROJECT_ID)
    _upload(records, db, dry_run)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest TN district crime index into Firestore")
    parser.add_argument("--dry-run", action="store_true", help="Print without writing")
    args = parser.parse_args()
    run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
