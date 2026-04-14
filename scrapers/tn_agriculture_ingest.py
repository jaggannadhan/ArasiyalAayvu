"""
TN Statistical Handbook — Agriculture chapter ingestor
Source: https://www.tn.gov.in/deptst/agriculture.pdf
        TN Department of Economics & Statistics — Statistical Handbook 2022-23

Extracts:
  1. State-level crop production (area, production, yield) — Table 4.3
  2. District-wise crop area — Table 4.4
  3. District-wise crop production — Table 4.6
  4. District-wise fertilizer consumption — Table 4.8
  5. Time series (area + production, 15 years) — Tables 4.12-4.13
  6. Land utilisation — Table 4.2

Outputs: data/processed/tn_agriculture.json

Run:
    python scrapers/tn_agriculture_ingest.py
    python scrapers/tn_agriculture_ingest.py --upload
    python scrapers/tn_agriculture_ingest.py --probe
"""

from __future__ import annotations

import json
import re
import sys
import warnings
from pathlib import Path

import pdfplumber
import requests

warnings.filterwarnings("ignore")

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(Path(__file__).parent))
from ts_utils import (
    load_timeseries, upsert_snapshot, save_timeseries,
    upload_snapshot_to_firestore, get_firestore_client,
)

URL = "https://www.tn.gov.in/deptst/agriculture.pdf"
RAW_PATH = BASE_DIR / "data" / "raw" / "tn_agriculture_2022_23.pdf"
OUT_PATH = BASE_DIR / "data" / "processed" / "tn_agriculture.json"
TS_PATH = BASE_DIR / "data" / "processed" / "tn_agriculture_ts.json"
HEADERS = {"User-Agent": "Mozilla/5.0"}
PERIOD = "2022-23"


def _parse_int(s: str) -> int | None:
    s = s.strip().replace(",", "").replace(" ", "")
    if not s or s == "-" or s == "..":
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _parse_float(s: str) -> float | None:
    s = s.strip().replace(",", "").replace(" ", "")
    if not s or s == "-" or s == "..":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def download_pdf() -> Path:
    if RAW_PATH.exists():
        print(f"  [cached] {RAW_PATH.name}")
        return RAW_PATH
    RAW_PATH.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(URL, headers=HEADERS, timeout=30, verify=False)
    r.raise_for_status()
    RAW_PATH.write_bytes(r.content)
    print(f"  [downloaded] {RAW_PATH.name} ({len(r.content)//1024} KB)")
    return RAW_PATH


def parse_state_crops(pdf: pdfplumber.PDF) -> dict:
    """Table 4.3 — State-level crop area/production/productivity."""
    text = pdf.pages[5].extract_text() or ""
    crops = {}
    for line in text.split("\n"):
        m = re.match(r"\d+\.\s*(.+?)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)", line)
        if m:
            crop = m.group(1).strip().rstrip("@#*")
            crops[crop] = {
                "area_hectares": _parse_int(m.group(2)),
                "production_tonnes": _parse_int(m.group(3)),
                "productivity_kg_per_hec": _parse_int(m.group(4)),
            }
    # Also catch total rows
    for line in text.split("\n"):
        if "Total Cereals" in line or "Total Pulses" in line:
            nums = re.findall(r"[\d,]+", line)
            if len(nums) >= 2:
                label = "Total Cereals" if "Cereals" in line else "Total Pulses"
                crops[label] = {
                    "area_hectares": _parse_int(nums[0]),
                    "production_tonnes": _parse_int(nums[1]),
                }
    return crops


def parse_district_data(pdf: pdfplumber.PDF) -> dict[str, dict]:
    """Tables 4.4 + 4.6 — District-wise crop area and production."""
    districts: dict[str, dict] = {}

    # Pages 7-10: Area under principal crops by districts (Table 4.4)
    # Pages 15-20: Production of principal crops by districts (Table 4.6)
    for page_idx in range(7, 21):
        if page_idx >= len(pdf.pages):
            break
        tables = pdf.pages[page_idx].extract_tables()
        for t in tables:
            if len(t) < 5:
                continue
            # Find header row with crop names
            header = None
            data_start = 0
            for ri, row in enumerate(t[:4]):
                cells = [str(c or "").strip() for c in row]
                if any("District" in c for c in cells):
                    header = cells
                    data_start = ri + 1
                    # Skip the numbering row
                    if data_start < len(t):
                        next_row = [str(c or "").strip() for c in t[data_start]]
                        if next_row and next_row[0].isdigit() and len(next_row[0]) <= 2:
                            data_start += 1
                    break

            if not header:
                continue

            # Extract crop names from header
            crop_cols: list[tuple[int, str]] = []
            for ci, h in enumerate(header):
                h_clean = h.replace("\n", " ").strip()
                if h_clean and h_clean != "Districts" and not h_clean.isdigit():
                    crop_cols.append((ci, h_clean))

            for row in t[data_start:]:
                cells = [str(c or "").strip() for c in row]
                if not cells:
                    continue
                # District name: first non-empty, non-number cell
                dist_name = ""
                for c in cells[:2]:
                    c_clean = re.sub(r"^\d+\.\s*", "", c).strip()
                    if c_clean and not c_clean.isdigit() and len(c_clean) > 2:
                        dist_name = c_clean
                        break

                if not dist_name or "Total" in dist_name or "State" in dist_name:
                    continue

                districts.setdefault(dist_name, {})
                for ci, crop_name in crop_cols:
                    if ci < len(cells):
                        val = _parse_float(cells[ci])
                        if val is not None:
                            districts[dist_name][crop_name] = val

    return districts


def parse_time_series(pdf: pdfplumber.PDF) -> dict[str, dict]:
    """Tables 4.12-4.13 — Time series of area and production."""
    ts_data: dict[str, dict] = {}

    for page_idx in [25, 26, 27, 28]:  # Pages 26-29
        if page_idx >= len(pdf.pages):
            break
        tables = pdf.pages[page_idx].extract_tables()
        for t in tables:
            if len(t) < 5:
                continue
            # Header with crop names
            header = [str(c or "").replace("\n", " ").strip() for c in t[0]]
            if "Year" not in header[0]:
                continue
            crop_names = [h for h in header[1:] if h and not h.isdigit()]

            for row in t[2:]:  # skip header + numbering row
                cells = [str(c or "").strip() for c in row]
                if not cells or not re.match(r"^\d{4}", cells[0]):
                    continue
                year = cells[0].strip()
                ts_data.setdefault(year, {})
                for i, crop in enumerate(crop_names):
                    if i + 1 < len(cells):
                        val = _parse_float(cells[i + 1])
                        if val is not None:
                            ts_data[year][crop] = val

    return ts_data


def parse_fertilizer(pdf: pdfplumber.PDF) -> dict[str, dict]:
    """Table 4.8 — District-wise fertilizer consumption."""
    fert: dict[str, dict] = {}
    page = pdf.pages[21]  # Page 22
    tables = page.extract_tables()
    for t in tables:
        if len(t) < 10:
            continue
        for row in t[2:]:
            cells = [str(c or "").strip() for c in row]
            if len(cells) < 5:
                continue
            dist = ""
            for c in cells[:2]:
                c_clean = re.sub(r"^\d+\.\s*", "", c).strip()
                if c_clean and len(c_clean) > 2 and not c_clean.isdigit():
                    dist = c_clean
                    break
            if not dist or "Total" in dist:
                continue
            # Cols: N, P, K, Total fertilizer, Pesticides
            fert[dist] = {}
            col_names = ["nitrogen_mt", "phosphorus_mt", "potash_mt", "total_fertilizer_mt"]
            for ci, name in enumerate(col_names):
                if ci + 2 < len(cells):
                    fert[dist][name] = _parse_float(cells[ci + 2])
    return fert


def main():
    upload = "--upload" in sys.argv
    probe = "--probe" in sys.argv

    print("TN Agriculture Statistical Handbook 2022-23...")
    path = download_pdf()

    with pdfplumber.open(str(path)) as pdf:
        crops = parse_state_crops(pdf)
        districts = parse_district_data(pdf)
        time_series = parse_time_series(pdf)
        fertilizer = parse_fertilizer(pdf)

    result = {
        "source": "TN Statistical Handbook 2022-23 — Agriculture",
        "url": URL,
        "period": PERIOD,
        "state_crops": crops,
        "district_crop_data": districts,
        "time_series_area_000hec": time_series,
        "district_fertilizer": fertilizer,
    }

    if probe:
        print(f"\nState crops: {len(crops)}")
        for c, v in sorted(crops.items()):
            print(f"  {c:25} area={v.get('area_hectares'):>12,} prod={v.get('production_tonnes') or 0:>12,}")
        print(f"\nDistricts with crop data: {len(districts)}")
        for d in sorted(districts.keys())[:5]:
            keys = list(districts[d].keys())[:4]
            print(f"  {d:20} {keys}")
        print(f"\nTime series years: {sorted(time_series.keys())}")
        print(f"Fertilizer districts: {len(fertilizer)}")
        return

    # Save full JSON
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(result, f, indent=2, default=str, ensure_ascii=False)
    print(f"\nWrote {OUT_PATH} ({OUT_PATH.stat().st_size // 1024} KB)")

    # Also save as time-series for Firestore
    ts = load_timeseries(TS_PATH)
    meta = {
        "dataset": "tn_agriculture",
        "source": "TN Statistical Handbook — Agriculture",
        "url": URL,
        "note": "District-wise crop area, production, fertilizer. State-level time series.",
    }

    snapshot = {
        "crops": crops,
        "districts_count": len(districts),
        "fertilizer_districts": len(fertilizer),
        "time_series_years": len(time_series),
    }
    upsert_snapshot(ts, "Tamil Nadu", PERIOD, snapshot, meta=meta)
    save_timeseries(ts, TS_PATH)
    print(f"Wrote {TS_PATH}")

    if upload:
        print("\nUploading to Firestore …")
        db = get_firestore_client()

        # Upload state-level summary
        upload_snapshot_to_firestore(db, "tn_agriculture", "tamil_nadu", PERIOD, snapshot)

        # Upload district-level data as sub-documents
        for dist, data in districts.items():
            dist_slug = dist.lower().replace(" ", "_")
            fert_data = fertilizer.get(dist, {})
            doc = {**data, **fert_data, "district": dist, "data_period": PERIOD}
            upload_snapshot_to_firestore(db, "tn_agriculture", f"district_{dist_slug}", PERIOD, doc)

        print(f"  Uploaded {1 + len(districts)} documents to Firestore.")


if __name__ == "__main__":
    main()
