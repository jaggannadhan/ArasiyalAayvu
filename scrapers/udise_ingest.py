"""
UDISE+ School Education — ingestor
Source: GitHub community CSV export (gsidhu/udise-csv-data)
        Mirrors official UDISE+ reports published by MoE

Auto-downloads ZIP from GitHub on first run; caches in data/raw/udise/.
Parses state-level data for 5 focus states + All India from:
  Table 6.1  — Gross Enrolment Ratio (GER) by Gender and Level
  Table 6.13 — Dropout Rate by level of education and gender
  Table 4.11 — Pupil Teacher Ratio (PTR) by level
  Table 3.1  — Number of schools by level
  Table 7.1  — % schools with electricity (continued = percentage sheet)

Header structure (4 rows for GER/Dropout, 3 rows for PTR):
  Row 0: Title | section header ...
  Row 1: "" | Level names (Primary, Upper Primary, ...)
  Row 2: "" | Boys | Girls | Total  (repeating per level)
  Row 3: (1) | (2) | (3) | ...  (column numbers — skip)
  Row 4+: State data

Outputs: data/processed/udise_ts.json

Run:
    python scrapers/udise_ingest.py               # download + parse + write JSON
    python scrapers/udise_ingest.py --upload      # also upload to Firestore
    python scrapers/udise_ingest.py --probe       # print table names for each ZIP
    python scrapers/udise_ingest.py --no-download # use cached ZIPs only
"""

from __future__ import annotations

import csv
import io
import re
import sys
import zipfile
from pathlib import Path
from typing import Optional

import httpx

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(Path(__file__).parent))
from ts_utils import load_timeseries, upsert_snapshot, save_timeseries, upload_snapshot_to_firestore, get_firestore_client

RAW_DIR  = BASE_DIR / "data" / "raw" / "udise"
OUT_PATH = BASE_DIR / "data" / "processed" / "udise_ts.json"

FOCUS_STATES = {
    "Tamil Nadu", "Kerala", "Karnataka", "Andhra Pradesh", "Telangana",
}

GITHUB_BASE = "https://github.com/gsidhu/udise-csv-data/raw/main"
TARGET_YEARS = ["2021-22", "2022-23", "2023-24"]
YEAR_ZIP = {
    "2021-22": "UDISE%202021-22.zip",
    "2022-23": "UDISE%202022-23.zip",
    "2023-24": "UDISE%202023-24.zip",
}

# State name normalisation
STATE_NORM = {
    "india": "All India",
    "a & n islands": None,
    "andaman and nicobar islands": None,
    "lakshadweep": None,
    "dnhdd": None,
    "d&nh and dd": None,
}


def _norm_state(raw: str) -> Optional[str]:
    s = raw.strip().strip("*").replace("\n", " ").strip()
    low = s.lower()
    if low in STATE_NORM:
        return STATE_NORM[low]
    return s


def _read_csv(zf: zipfile.ZipFile, member: str) -> list[list[str]]:
    with zf.open(member) as f:
        text = io.TextIOWrapper(f, encoding="utf-8-sig").read()
    return list(csv.reader(io.StringIO(text)))


def _find(zf: zipfile.ZipFile, *patterns: str) -> Optional[str]:
    """Return first ZIP member matching ALL patterns (case-insensitive). Skip '(continued)'."""
    compiled = [re.compile(p, re.IGNORECASE) for p in patterns]
    # Prefer non-continued version first
    candidates = []
    for name in zf.namelist():
        if not name.endswith(".csv"):
            continue
        if all(c.search(name) for c in compiled):
            candidates.append(name)
    # Non-continued first
    for c in candidates:
        if "(continued)" not in c:
            return c
    return candidates[0] if candidates else None


def _find_pct(zf: zipfile.ZipFile, *patterns: str) -> Optional[str]:
    """Like _find but prefers the 'continued' / 'Percentage' variant."""
    compiled = [re.compile(p, re.IGNORECASE) for p in patterns]
    for name in zf.namelist():
        if not name.endswith(".csv"):
            continue
        if all(c.search(name) for c in compiled):
            if "continued" in name.lower() or "ercentage" in name:
                return name
    return _find(zf, *patterns)


# ── GER parser ────────────────────────────────────────────────────────────────
# Table 6.1: 4-row header; Total columns at 3, 6, 9, 12, 15 (0-indexed)
# Levels: Primary, Upper Primary, Elementary, Secondary, Higher Secondary

GER_LEVELS = ["primary", "upper_primary", "elementary", "secondary", "higher_secondary"]
GER_TOTAL_COLS = [3, 6, 9, 12, 15]  # 0-indexed


def parse_ger(zf: zipfile.ZipFile) -> dict[str, dict]:
    member = _find(zf, r"6\.1\b", r"[Gg]ross\s+[Ee]nrolment\s+Ratio")
    if not member:
        return {}
    rows = _read_csv(zf, member)
    data_start = 4  # skip title, levels, gender, column-numbers rows
    results: dict[str, dict] = {}
    for row in rows[data_start:]:
        if not row or not row[0].strip() or row[0].strip().startswith("("):
            continue
        state = _norm_state(row[0])
        if state is None:
            continue
        if state not in FOCUS_STATES and state != "All India":
            continue
        ger: dict[str, Optional[float]] = {}
        for level, col in zip(GER_LEVELS, GER_TOTAL_COLS):
            if col < len(row):
                try:
                    ger[level] = float(row[col].strip().replace(",", ""))
                except ValueError:
                    ger[level] = None
        results[state] = ger
    return results


# ── Dropout parser ────────────────────────────────────────────────────────────
# Table 6.13: 4-row header; levels = Primary, Upper Primary, Secondary
# Total columns at 3, 6, 9 (0-indexed)

DROPOUT_LEVELS    = ["primary", "upper_primary", "secondary"]
DROPOUT_TOTAL_COLS = [3, 6, 9]


def parse_dropout(zf: zipfile.ZipFile) -> dict[str, dict]:
    member = _find(zf, r"6\.13\b", r"[Dd]ropout\s+[Rr]ate")
    if not member:
        return {}
    rows = _read_csv(zf, member)
    results: dict[str, dict] = {}
    for row in rows[4:]:
        if not row or not row[0].strip() or row[0].strip().startswith("("):
            continue
        state = _norm_state(row[0])
        if state is None:
            continue
        if state not in FOCUS_STATES and state != "All India":
            continue
        drop: dict[str, Optional[float]] = {}
        for level, col in zip(DROPOUT_LEVELS, DROPOUT_TOTAL_COLS):
            if col < len(row):
                try:
                    drop[level] = float(row[col].strip().replace(",", ""))
                except ValueError:
                    drop[level] = None
        results[state] = drop
    return results


# ── PTR parser ────────────────────────────────────────────────────────────────
# Table 4.11: 3-row header; cols 1-4 = Primary, Upper Primary, Secondary, Higher Secondary

PTR_LEVELS = ["primary", "upper_primary", "secondary", "higher_secondary"]


def parse_ptr(zf: zipfile.ZipFile) -> dict[str, Optional[float]]:
    """Returns {state: ptr_secondary} — secondary PTR as the headline metric."""
    member = _find(zf, r"4\.11\b", r"[Pp]upil\s+[Tt]eacher\s+[Rr]atio")
    if not member:
        return {}
    rows = _read_csv(zf, member)
    results: dict[str, Optional[float]] = {}
    for row in rows[3:]:  # 3-row header
        if not row or not row[0].strip() or row[0].strip().startswith("("):
            continue
        state = _norm_state(row[0])
        if state is None:
            continue
        if state not in FOCUS_STATES and state != "All India":
            continue
        ptr_map: dict[str, Optional[float]] = {}
        for i, level in enumerate(PTR_LEVELS, start=1):
            if i < len(row):
                try:
                    ptr_map[level] = float(row[i].strip().replace(",", ""))
                except ValueError:
                    ptr_map[level] = None
        results[state] = ptr_map
    return results


# ── Schools parser ─────────────────────────────────────────────────────────────
# Table 3.1: total schools by level; col 1 = total or we sum across levels

def parse_schools(zf: zipfile.ZipFile) -> dict[str, Optional[int]]:
    member = _find(zf, r"3\.1\b", r"[Nn]umber\s+of\s+schools\s+by\s+level")
    if not member:
        return {}
    rows = _read_csv(zf, member)
    # Find the "Total" column in header (last level = Total)
    header = rows[1] if len(rows) > 1 else []
    total_col: Optional[int] = None
    for j, h in enumerate(header):
        if h.strip().lower() in ("total", "total (1 to 12)"):
            total_col = j
            break
    if total_col is None and len(header) > 1:
        # Last non-empty col
        for j in range(len(header) - 1, 0, -1):
            if header[j].strip():
                total_col = j
                break

    results: dict[str, Optional[int]] = {}
    data_start = 3  # 3-row header for this table typically
    for row in rows[data_start:]:
        if not row or not row[0].strip() or row[0].strip().startswith("("):
            continue
        state = _norm_state(row[0])
        if state is None:
            continue
        if state not in FOCUS_STATES and state != "All India":
            continue
        val: Optional[int] = None
        if total_col and total_col < len(row):
            try:
                val = int(float(row[total_col].strip().replace(",", "")))
            except ValueError:
                pass
        results[state] = val
    return results


# ── Infrastructure parser ──────────────────────────────────────────────────────
# Table 7.1 (continued) — % schools with electricity
# Table 7.11 — % schools with internet (may be count; we detect)

def _pct_col(header: list[str], keywords: list[str]) -> Optional[int]:
    for j, h in enumerate(header):
        hl = h.lower()
        if all(k in hl for k in keywords):
            return j
    return None


def parse_infra(zf: zipfile.ZipFile) -> dict[str, dict]:
    results: dict[str, dict] = {}

    # Electricity % (Table 7.1 continued = Percentage sheet)
    elec_member = _find_pct(zf, r"7\.1\b", r"[Ee]lectricity")
    if elec_member:
        rows = _read_csv(zf, elec_member)
        # Find "functional" electricity % col
        header = rows[1] if len(rows) > 1 else []
        funct_col = _pct_col(header, ["functional", "electricity"]) or _pct_col(header, ["electricity"])
        data_start = 3
        for row in rows[data_start:]:
            if not row or not row[0].strip() or row[0].strip().startswith("("):
                continue
            state = _norm_state(row[0])
            if state is None or (state not in FOCUS_STATES and state != "All India"):
                continue
            pct: Optional[float] = None
            if funct_col and funct_col < len(row):
                try:
                    pct = float(row[funct_col].strip().replace(",", "").replace("%", ""))
                except ValueError:
                    pass
            results.setdefault(state, {})["schools_with_electricity_pct"] = pct

    # Internet (Table 7.11)
    inet_member = _find(zf, r"7\.11\b", r"[Ii]nternet")
    if inet_member:
        rows = _read_csv(zf, inet_member)
        header = rows[1] if len(rows) > 1 else []
        inet_col = _pct_col(header, ["internet"]) or _pct_col(header, ["functional"])
        data_start = 3
        for row in rows[data_start:]:
            if not row or not row[0].strip() or row[0].strip().startswith("("):
                continue
            state = _norm_state(row[0])
            if state is None or (state not in FOCUS_STATES and state != "All India"):
                continue
            pct = None
            if inet_col and inet_col < len(row):
                try:
                    pct = float(row[inet_col].strip().replace(",", "").replace("%", ""))
                except ValueError:
                    pass
            results.setdefault(state, {})["schools_with_internet_pct"] = pct

    # Toilet — Table 7.3 or 7.5 (girls toilet %)
    toilet_member = _find_pct(zf, r"7\.[35]\b", r"[Tt]oilet")
    if toilet_member:
        rows = _read_csv(zf, toilet_member)
        header = rows[1] if len(rows) > 1 else []
        toilet_col = _pct_col(header, ["toilet"]) or _pct_col(header, ["functional"])
        data_start = 3
        for row in rows[data_start:]:
            if not row or not row[0].strip() or row[0].strip().startswith("("):
                continue
            state = _norm_state(row[0])
            if state is None or (state not in FOCUS_STATES and state != "All India"):
                continue
            pct = None
            if toilet_col and toilet_col < len(row):
                try:
                    pct = float(row[toilet_col].strip().replace(",", "").replace("%", ""))
                except ValueError:
                    pass
            results.setdefault(state, {})["schools_with_toilet_pct"] = pct

    return results


# ── Download ───────────────────────────────────────────────────────────────────

def download_year(period: str, no_download: bool) -> Optional[Path]:
    zip_name = YEAR_ZIP.get(period)
    if not zip_name:
        return None
    local_path = RAW_DIR / zip_name.replace("%20", " ")
    if local_path.exists():
        print(f"  Cached: {local_path.name}")
        return local_path
    if no_download:
        print(f"  SKIP (--no-download): {local_path.name}")
        return None
    url = f"{GITHUB_BASE}/{zip_name}"
    print(f"  Downloading {period} … ", end="", flush=True)
    try:
        r = httpx.get(url, follow_redirects=True, timeout=120)
        r.raise_for_status()
        local_path.write_bytes(r.content)
        print(f"done ({len(r.content) // 1024} KB)")
        return local_path
    except Exception as e:
        print(f"FAILED: {e}")
        return None


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    upload      = "--upload"      in sys.argv
    probe       = "--probe"       in sys.argv
    no_download = "--no-download" in sys.argv

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    ts = load_timeseries(OUT_PATH)
    meta = {
        "dataset": "udise",
        "source":  "UDISE+ (MoE) via gsidhu/udise-csv-data",
        "url":     "https://udiseplus.gov.in",
        "note":    "GER, dropout rate, PTR, infrastructure. Focus states + All India.",
    }

    total_snapshots = 0
    first_meta = True

    for period in TARGET_YEARS:
        print(f"\n── UDISE {period} ───────────────────────────────────────────")
        zip_path = download_year(period, no_download)
        if not zip_path:
            continue

        with zipfile.ZipFile(zip_path) as zf:
            if probe:
                print("  Tables found:")
                for name in sorted(zf.namelist()):
                    if name.endswith(".csv"):
                        print(f"    {name}")
                continue

            ger     = parse_ger(zf)
            dropout = parse_dropout(zf)
            ptr     = parse_ptr(zf)
            schools = parse_schools(zf)
            infra   = parse_infra(zf)

        all_states = (FOCUS_STATES | {"All India"}) & (set(ger) | set(dropout))

        for state in sorted(all_states):
            snapshot: dict = {}
            if s := schools.get(state):
                snapshot["total_schools"] = s
            if g := ger.get(state):
                snapshot["ger"] = g
            if d := dropout.get(state):
                snapshot["dropout_rate"] = d
            if p := ptr.get(state):
                snapshot["ptr"] = p
            if inf := infra.get(state):
                snapshot.update(inf)

            upsert_snapshot(ts, state, period, snapshot, meta=meta if first_meta else None)
            first_meta = False
            total_snapshots += 1
            g_prim = (snapshot.get("ger") or {}).get("primary")
            dr_prim = (snapshot.get("dropout_rate") or {}).get("primary")
            print(f"  {state:<28} GER_primary={g_prim}  dropout_primary={dr_prim}")

    if not probe:
        save_timeseries(ts, OUT_PATH)
        print(f"\nWrote {OUT_PATH}  ({OUT_PATH.stat().st_size // 1024} KB)")
        print(f"Total snapshots: {total_snapshots}")

        if upload and total_snapshots > 0:
            print("\nUploading to Firestore …")
            db = get_firestore_client()
            count = 0
            for display_name, entity in ts["entities"].items():
                for data_period, snapshot in entity["snapshots"].items():
                    upload_snapshot_to_firestore(db, "udise", display_name, data_period, snapshot)
                    count += 1
            print(f"  Uploaded {count} UDISE snapshots to Firestore.")


if __name__ == "__main__":
    main()
