"""
ASI (Annual Survey of Industries) — state-level ingestor
Source: MOSPI publications — https://www.mospi.gov.in/publications-reports
        Also: https://www.mospi.gov.in/asi-summary-results

Raw PDF expected at: data/raw/asi/  (any file matching *ASI*Vol*I*.pdf)
  e.g. "ASI Vol I 2023-24C-2.pdf"

  How to download:
    1. Visit: https://www.mospi.gov.in/asi-summary-results
    2. Find the latest ASI edition (e.g. "ASI 2023-24")
    3. Download "Volume I — Summary Results of Factory Sector"
    4. Save the PDF to: data/raw/asi/

  The PDF contains Table 2 — per-state industry breakdowns.
  Each state's first page has an "All" column with aggregated values.
  This script extracts those aggregate metrics for focus states.

  Metrics extracted (values in ₹ thousands → converted to ₹ crore):
    factories          — Number of Factories
    fixed_capital_cr   — Fixed Capital
    total_output_cr    — Total Output
    total_input_cr     — Total Inputs
    gva_cr             — Gross Value Added
    nva_cr             — Net Value Added

Outputs: data/processed/asi_ts.json

Run:
    python scrapers/asi_ingest.py               # parse PDF, write JSON
    python scrapers/asi_ingest.py --upload      # also upload to Firestore
    python scrapers/asi_ingest.py --probe       # print extracted data and exit
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Optional

import pdfplumber

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(Path(__file__).parent))
from ts_utils import (
    load_timeseries, upsert_snapshot, save_timeseries,
    upload_snapshot_to_firestore, get_firestore_client,
)

RAW_DIR  = BASE_DIR / "data" / "raw" / "asi"
OUT_PATH = BASE_DIR / "data" / "processed" / "asi_ts.json"

FOCUS_STATES = {
    "Tamil Nadu", "Kerala", "Karnataka", "Andhra Pradesh", "Telangana",
}

# Line prefix in Table 2 → metric key
# "All" column value = first number after the label text
METRIC_MAP: dict[str, str] = {
    "1. Number of Factories":   "factories",
    "3. Fixed Capital":         "fixed_capital",
    "15. Total Output":         "total_output",
    "18. Total Inputs":         "total_input",
    "19. Gross Value Added":    "gva",
    "21. Net Value Added":      "nva",
}


def _thousands_to_crore(v: Optional[int]) -> Optional[float]:
    """Convert ₹ thousands to ₹ crore (÷ 10,00,000 i.e. ÷ 1e5)."""
    return round(v / 1e5, 2) if v is not None else None


def _find_pdf() -> Optional[Path]:
    """Find the ASI Volume I PDF in the raw directory."""
    for p in RAW_DIR.glob("*.pdf"):
        nl = p.name.lower()
        if "asi" in nl or "vol" in nl or "annual" in nl:
            return p
    # Fall back to any PDF
    pdfs = list(RAW_DIR.glob("*.pdf"))
    return pdfs[0] if pdfs else None


def _detect_period(pdf_path: Path, pdf: pdfplumber.PDF) -> str:
    """Extract the period (e.g. '2023-24') from the PDF title page."""
    for page in pdf.pages[:5]:
        text = page.extract_text() or ""
        m = re.search(r"20\d{2}\s*[-–]\s*\d{2,4}", text)
        if m:
            raw = m.group().replace("–", "-").replace(" ", "")
            parts = raw.split("-")
            if len(parts) == 2:
                return f"{parts[0]}-{parts[1][-2:]}"
    # Fallback: try filename
    m = re.search(r"(\d{4})[-_](\d{2,4})", pdf_path.name)
    if m:
        return f"{m.group(1)}-{m.group(2)[-2:]}"
    return "unknown"


def parse_pdf(path: Path, probe: bool) -> tuple[str, dict[str, dict]]:
    """
    Parse ASI Volume I PDF.
    Returns (period, {state: {metric: value}}).
    """
    results: dict[str, dict] = {}

    with pdfplumber.open(path) as pdf:
        period = _detect_period(path, pdf)
        print(f"  Period detected: {period}")
        print(f"  Pages: {len(pdf.pages)}")

        current_state: Optional[str] = None

        for page in pdf.pages[10:]:  # skip intro pages
            text = page.extract_text() or ""

            # Detect state from Table 2 header
            if "Table 2:" in text:
                for s in FOCUS_STATES | {"All-India"}:
                    if s in text and s != current_state:
                        current_state = s
                        if s not in results:
                            results[s] = {}
                        break

            if current_state not in (FOCUS_STATES | {"All-India"}):
                continue

            for line in text.split("\n"):
                for prefix, key in METRIC_MAP.items():
                    if line.startswith(prefix):
                        after_label = line[len(prefix):]
                        nums = re.findall(r"-?[\d,]+", after_label)
                        if nums and key not in results[current_state]:
                            results[current_state][key] = int(
                                nums[0].replace(",", "")
                            )
                        break

    # Convert monetary values to ₹ crore
    for state, raw in results.items():
        results[state] = {
            "factories":        raw.get("factories"),
            "fixed_capital_cr": _thousands_to_crore(raw.get("fixed_capital")),
            "total_output_cr":  _thousands_to_crore(raw.get("total_output")),
            "total_input_cr":   _thousands_to_crore(raw.get("total_input")),
            "gva_cr":           _thousands_to_crore(raw.get("gva")),
            "nva_cr":           _thousands_to_crore(raw.get("nva")),
        }

    return period, results


def print_reminder():
    print()
    print("━" * 65)
    print("  ASI — NO PDF FOUND")
    print("━" * 65)
    print(f"  Expected: any PDF in {RAW_DIR}/")
    print("  e.g.:     data/raw/asi/ASI Vol I 2023-24.pdf")
    print()
    print("  To download:")
    print("  1. Visit: https://www.mospi.gov.in/asi-summary-results")
    print("  2. Download 'ASI 2023-24 — Volume I Summary Results'")
    print("  3. Save to: data/raw/asi/")
    print("━" * 65)


def main():
    upload = "--upload" in sys.argv
    probe  = "--probe"  in sys.argv

    pdf_path = _find_pdf()
    if not pdf_path:
        print_reminder()
        sys.exit(0)

    print(f"\nParsing {pdf_path.name} …")
    period, state_data = parse_pdf(pdf_path, probe)

    if not state_data:
        print("  ERROR: No state data extracted.")
        sys.exit(1)

    if probe:
        hdr = f"{'State':25} {'Factories':>10} {'Output ₹Cr':>12} {'Input ₹Cr':>12} {'GVA ₹Cr':>12} {'NVA ₹Cr':>12}"
        print(f"\n{hdr}")
        print("-" * len(hdr))
        for state in sorted(state_data):
            d = state_data[state]
            print(f"{state:25} {d.get('factories') or '':>10} {d.get('total_output_cr') or '':>12} "
                  f"{d.get('total_input_cr') or '':>12} {d.get('gva_cr') or '':>12} {d.get('nva_cr') or '':>12}")
        return

    ts = load_timeseries(OUT_PATH)
    meta = {
        "dataset": "asi",
        "source":  "Annual Survey of Industries (MOSPI)",
        "url":     "https://www.mospi.gov.in/asi-summary-results",
        "note":    "Factory sector. Monetary values in ₹ crore. Focus states + All India.",
    }

    total_snapshots = 0
    first = True
    for state, snapshot in sorted(state_data.items()):
        upsert_snapshot(ts, state, period, snapshot, meta=meta if first else None)
        first = False
        total_snapshots += 1
        print(f"  {state:<28} factories={snapshot.get('factories')} "
              f"gva_cr={snapshot.get('gva_cr')}")

    save_timeseries(ts, OUT_PATH)
    print(f"\nWrote {OUT_PATH}  ({OUT_PATH.stat().st_size // 1024} KB)")
    print(f"Total snapshots: {total_snapshots}")

    if upload and total_snapshots > 0:
        print("\nUploading to Firestore …")
        db = get_firestore_client()
        count = 0
        for display_name, entity in ts["entities"].items():
            for data_period, snapshot in entity["snapshots"].items():
                upload_snapshot_to_firestore(db, "asi", display_name, data_period, snapshot)
                count += 1
        print(f"  Uploaded {count} ASI snapshots to Firestore.")


if __name__ == "__main__":
    main()
