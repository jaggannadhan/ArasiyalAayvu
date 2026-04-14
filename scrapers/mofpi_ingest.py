"""
MOFPI State Profile — Food Processing & Agriculture ingestor
Source: Ministry of Food Processing Industries (MOFPI)
        https://www.mofpi.gov.in/sites/default/files/KnowledgeCentre/State%20Profile/{state}.pdf

Extracts key production statistics and food processing metrics
from MOFPI state profile PDFs.

Metrics extracted:
  - Crop production: rice, pulses, coarse cereals, oilseeds ('000 MT)
  - Livestock: eggs (lakh nos), milk ('000 MT), meat ('000 MT), fish (lakh MT)
  - Vegetables: tapioca, tomato, onion ('000 MT)
  - Fruits: banana, mango ('000 MT)
  - Spices: turmeric, tamarind ('000 MT)
  - PMFME: micro enterprises, subsidy, SHG members
  - Agri infra: mandis, e-NAM mandis, FPOs

Outputs: data/processed/mofpi_ts.json

Run:
    python scrapers/mofpi_ingest.py               # download + parse, write JSON
    python scrapers/mofpi_ingest.py --upload      # also upload to Firestore
    python scrapers/mofpi_ingest.py --probe       # print parsed data and exit
"""

from __future__ import annotations

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

OUT_PATH = BASE_DIR / "data" / "processed" / "mofpi_ts.json"
RAW_DIR = BASE_DIR / "data" / "raw" / "mofpi"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

STATES = {
    "Tamil_Nadu":      "Tamil Nadu",
    "Kerala":          "Kerala",
    "Karnataka":       "Karnataka",
    "Andhra_Pradesh":  "Andhra Pradesh",
    "Telangana":       "Telangana",
}

BASE_URL = "https://www.mofpi.gov.in/sites/default/files/KnowledgeCentre/State%20Profile/{slug}.pdf"


def _parse_num(s: str) -> float | None:
    s = s.strip().replace(",", "").replace("–", "").replace("-", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _download(slug: str) -> Path | None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    dest = RAW_DIR / f"{slug}.pdf"
    if dest.exists():
        print(f"  [cached] {dest.name}")
        return dest

    url = BASE_URL.format(slug=slug)
    try:
        r = requests.get(url, headers=HEADERS, timeout=30, verify=False)
        if r.status_code != 200 or not r.content[:5] == b"%PDF-":
            print(f"  [skip] {slug}: HTTP {r.status_code}")
            return None
        dest.write_bytes(r.content)
        print(f"  [downloaded] {dest.name} ({len(r.content)//1024} KB)")
        return dest
    except Exception as e:
        print(f"  [error] {slug}: {e}")
        return None


def _extract_simple_table(tables: list, header_patterns: list[str]) -> dict[str, float | None]:
    """Extract {Particulars: Production} from simple 3-column tables."""
    results: dict[str, float | None] = {}
    for t in tables:
        if len(t) < 2:
            continue
        for row in t:
            if not row or len(row) < 2:
                continue
            # Column 1 or 2 has the label, last column has the value
            label = str(row[1] or row[0] or "").strip().lower()
            val_str = str(row[-1] or "").strip()

            for pat in header_patterns:
                if pat in label:
                    val = _parse_num(val_str)
                    if val is not None:
                        results[pat] = val
                    break
    return results


def parse_pdf(path: Path) -> dict:
    """Parse a MOFPI state profile PDF and extract key metrics."""
    snapshot: dict = {}

    with pdfplumber.open(str(path)) as pdf:
        all_tables: list = []
        all_text = ""
        for page in pdf.pages:
            tables = page.extract_tables()
            all_tables.extend(tables)
            all_text += (page.extract_text() or "") + "\n"

    # ── Production stats (page 8 tables) ──
    crop_keys = ["rice", "pulses", "coarse cereals", "oilseeds"]
    livestock_keys = ["eggs", "milk", "meat", "fish"]
    veg_keys = ["tapioca", "tomato", "onion"]
    fruit_keys = ["banana", "mango", "watermelon"]
    spice_keys = ["turmeric", "tamarind"]

    for t in all_tables:
        for row in t:
            if not row or len(row) < 3:
                continue
            label = str(row[1] or "").strip().lower()
            val_str = str(row[2] or row[-1] or "").strip()
            val = _parse_num(val_str)
            if val is None:
                continue

            # Crops
            for key in crop_keys:
                if key in label:
                    snapshot[f"crop_{key.replace(' ', '_')}_000mt"] = val
            # Livestock
            if "eggs" in label:
                snapshot["livestock_eggs_lakh"] = val
            elif "milk" in label and "000" in str(row[1] or ""):
                snapshot["livestock_milk_000mt"] = val
            elif "meat" in label:
                snapshot["livestock_meat_000mt"] = val
            elif "fish" in label:
                snapshot["livestock_fish_lakh_mt"] = val
            # Vegetables
            for key in veg_keys:
                if key in label:
                    snapshot[f"veg_{key}_000mt"] = val
            # Fruits
            for key in fruit_keys:
                if key in label:
                    snapshot[f"fruit_{key}_000mt"] = val
            # Spices
            for key in spice_keys:
                if key in label:
                    snapshot[f"spice_{key}_000mt"] = val

    # ── PMFME stats ──
    for t in all_tables:
        for row in t:
            if not row or len(row) < 2:
                continue
            label = str(row[0] or "").strip().lower()
            val_str = str(row[1] or "").strip()
            if "micro enterprises" in label:
                snapshot["pmfme_micro_enterprises"] = _parse_num(val_str)
            elif "approved subsidy" in label:
                snapshot["pmfme_subsidy_cr"] = _parse_num(val_str)
            elif "shg members" in label:
                snapshot["pmfme_shg_members"] = _parse_num(val_str)

    # ── Agri infra ──
    for t in all_tables:
        for row in t:
            if not row or len(row) < 3:
                continue
            label = str(row[1] or "").strip().lower()
            val_str = str(row[2] or "").strip()
            if "mandi" in label and "e-nam" not in label:
                snapshot["agri_mandis"] = _parse_num(val_str)
            elif "e-nam" in label:
                snapshot["agri_enam_mandis"] = _parse_num(val_str)
            elif "farmer producer" in label:
                snapshot["agri_fpos"] = _parse_num(val_str)

    # ── Infrastructure from overview table ──
    for t in all_tables:
        for row in t:
            if not row or len(row) < 2:
                continue
            label = str(row[0] or "").strip().lower()
            val_str = str(row[1] or "").strip()
            if "installed power" in label:
                m = re.search(r"[\d,.]+", val_str)
                if m:
                    snapshot["installed_power_mw"] = _parse_num(m.group())

    return snapshot


def main():
    upload = "--upload" in sys.argv
    probe = "--probe" in sys.argv

    print("MOFPI State Profiles — Food Processing & Agriculture...")

    ts = load_timeseries(OUT_PATH)
    meta = {
        "dataset": "mofpi",
        "source": "Ministry of Food Processing Industries — State Profiles",
        "url": "https://www.mofpi.gov.in/KnowledgeCentre/state-profile",
        "note": "Key agriculture production, livestock, PMFME metrics. Period: 2023-24.",
    }

    total = 0
    first = True
    period = "2023-24"

    for slug, state_name in STATES.items():
        print(f"\n── {state_name} ──")
        path = _download(slug)
        if not path:
            continue

        snapshot = parse_pdf(path)

        if probe:
            for k, v in sorted(snapshot.items()):
                print(f"  {k:35} {v}")
            continue

        if not snapshot:
            print("  No data extracted")
            continue

        upsert_snapshot(ts, state_name, period, snapshot, meta=meta if first else None)
        first = False
        total += 1

        crops = snapshot.get("crop_rice_000mt")
        milk = snapshot.get("livestock_milk_000mt")
        print(f"  rice={crops} milk={milk} keys={len(snapshot)}")

    if not probe:
        save_timeseries(ts, OUT_PATH)
        print(f"\nWrote {OUT_PATH}  ({OUT_PATH.stat().st_size // 1024} KB)")
        print(f"Total snapshots: {total}")

        if upload and total > 0:
            print("\nUploading to Firestore …")
            db = get_firestore_client()
            count = 0
            for display_name, entity in ts["entities"].items():
                for data_period, snapshot in entity["snapshots"].items():
                    upload_snapshot_to_firestore(db, "mofpi", display_name, data_period, snapshot)
                    count += 1
            print(f"  Uploaded {count} MOFPI snapshots to Firestore.")


if __name__ == "__main__":
    main()
