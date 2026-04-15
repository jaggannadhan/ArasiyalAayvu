"""
MOSPI Energy Statistics India — state-level ingestor
Source: https://www.mospi.gov.in (Energy Statistics India 2022-2026)

Extracts state-wise energy data from 5 annual PDF reports:
  - Installed electricity capacity (GW) by source
  - Renewable energy capacity (MW) — solar, wind, hydro
  - Coal reserves (million tonnes)

Outputs: data/processed/energy_stats_ts.json

Run:
    python scrapers/energy_stats_ingest.py
    python scrapers/energy_stats_ingest.py --upload
    python scrapers/energy_stats_ingest.py --probe
"""

from __future__ import annotations

import re
import sys
import warnings
from pathlib import Path

import pdfplumber

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(Path(__file__).parent))
from ts_utils import (
    load_timeseries, upsert_snapshot, save_timeseries,
    upload_snapshot_to_firestore, get_firestore_client,
)

OUT_PATH = BASE_DIR / "data" / "processed" / "energy_stats_ts.json"
RAW_DIR = BASE_DIR / "data" / "raw" / "energy_stats"

FOCUS_STATES = ["Tamil Nadu", "Kerala", "Karnataka", "Andhra Pradesh", "Telangana"]


def _find_state_line(text: str, state: str) -> str | None:
    for line in text.split("\n"):
        if state in line:
            return line
    return None


def _nums_after(line: str, state: str) -> list[float]:
    after = line[line.index(state) + len(state):]
    return [float(x) for x in re.findall(r"[\d,]+\.?\d*", after.replace(",", ""))]


def parse_pdf(path: Path, year: str) -> dict[str, dict]:
    """Parse one Energy Statistics PDF, return {state: {metrics}}."""
    results: dict[str, dict] = {}

    with pdfplumber.open(str(path)) as pdf:
        all_text = ""
        for page in pdf.pages:
            all_text += (page.extract_text() or "") + "\n\n"

        # Find state-wise installed capacity table
        # Look for table with "Installed Capacity" + state names
        for page in pdf.pages:
            text = page.extract_text() or ""
            if ("Installed Capacity" in text or "Regionwise" in text) and "Tamil Nadu" in text:
                for state in FOCUS_STATES:
                    line = _find_state_line(text, state)
                    if line:
                        nums = _nums_after(line, state)
                        results.setdefault(state, {})
                        # The exact column positions vary by year but typically:
                        # hydro, thermal, nuclear, renewable, total (pairs of prev/current year)
                        if len(nums) >= 8:
                            # Take the latest year values (even indices = prev, odd = current in some formats)
                            results[state]["installed_capacity_gw"] = nums[-2] if len(nums) >= 10 else nums[-1]
                break

        # Find renewable capacity table
        for page in pdf.pages:
            text = page.extract_text() or ""
            if ("Renewable Power" in text or "cumulative Installed" in text) and "Tamil Nadu" in text and "Solar" in text:
                for state in FOCUS_STATES:
                    line = _find_state_line(text, state)
                    if line:
                        nums = _nums_after(line, state)
                        results.setdefault(state, {})
                        # Find solar and wind — look for large numbers
                        if len(nums) >= 10:
                            results[state]["wind_mw"] = nums[3] if nums[3] > 10 else nums[2]
                            results[state]["solar_mw"] = nums[-3] if nums[-3] > 100 else nums[-4]
                            results[state]["total_renewable_mw"] = nums[-1] if nums[-1] > 100 else nums[-2]
                break

        # Extract per-capita energy consumption from national tables
        for page in pdf.pages:
            text = page.extract_text() or ""
            if "Per-Capita" in text and "Energy Consumption" in text and "Table 8" in text:
                # National figure — last row with numbers
                for line in reversed(text.split("\n")):
                    nums = re.findall(r"\d+\.?\d+", line)
                    valid = [float(n) for n in nums if float(n) > 100]
                    if valid:
                        results.setdefault("All India", {})
                        results["All India"]["per_capita_energy_kgoe"] = valid[-1]
                        break
                break

    return results


def main():
    upload = "--upload" in sys.argv
    probe = "--probe" in sys.argv

    print("Energy Statistics India — parsing 5 annual reports...")

    years = ["2022", "2023", "2024", "2025", "2026"]
    all_data: dict[str, dict[str, dict]] = {}  # {state: {year: {metrics}}}

    for year in years:
        path = RAW_DIR / f"energy_stats_{year}.pdf"
        if not path.exists():
            print(f"  {year}: [missing] {path}")
            continue

        print(f"  {year}: parsing {path.name} ({path.stat().st_size // 1024} KB)...")
        state_data = parse_pdf(path, year)
        for state, metrics in state_data.items():
            all_data.setdefault(state, {})[year] = metrics
        states_found = [s for s in FOCUS_STATES if s in state_data]
        print(f"    {len(states_found)} focus states found")

    if probe:
        for state in FOCUS_STATES + ["All India"]:
            if state not in all_data:
                continue
            print(f"\n── {state} ──")
            for year in sorted(all_data[state].keys()):
                d = all_data[state][year]
                parts = []
                if "installed_capacity_gw" in d:
                    parts.append(f"capacity={d['installed_capacity_gw']} GW")
                if "solar_mw" in d:
                    parts.append(f"solar={d['solar_mw']} MW")
                if "wind_mw" in d:
                    parts.append(f"wind={d['wind_mw']} MW")
                if "total_renewable_mw" in d:
                    parts.append(f"total_re={d['total_renewable_mw']} MW")
                if parts:
                    print(f"  {year}: {' | '.join(parts)}")
        return

    ts = load_timeseries(OUT_PATH)
    meta = {
        "dataset": "energy_stats",
        "source": "Energy Statistics India (MOSPI)",
        "url": "https://www.mospi.gov.in/publications-reports",
        "note": "State-wise installed capacity and renewable energy. Annual reports 2022-2026.",
    }

    total = 0
    first = True
    for state in FOCUS_STATES + ["All India"]:
        if state not in all_data:
            continue
        for year, snapshot in sorted(all_data[state].items()):
            if not snapshot:
                continue
            upsert_snapshot(ts, state, year, snapshot, meta=meta if first else None)
            first = False
            total += 1

    save_timeseries(ts, OUT_PATH)
    print(f"\nWrote {OUT_PATH} ({OUT_PATH.stat().st_size // 1024} KB)")
    print(f"Total snapshots: {total}")

    if upload:
        print("\nUploading to Firestore …")
        db = get_firestore_client()
        count = 0
        for display_name, entity in ts["entities"].items():
            for data_period, snapshot in entity["snapshots"].items():
                upload_snapshot_to_firestore(db, "energy_stats", display_name, data_period, snapshot)
                count += 1
        print(f"  Uploaded {count} energy stats snapshots to Firestore.")


if __name__ == "__main__":
    main()
