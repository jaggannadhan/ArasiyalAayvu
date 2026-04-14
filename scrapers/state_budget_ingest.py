"""
State Budget Ingest — ArasiyalAayvu
====================================
Scrapes state-level fiscal data from official government sources and uploads
to Firestore `state_budgets` collection.

Sources
-------
  CAG Finance Accounts Vol I  — primary, all-state, annual actuals
    https://cag.gov.in/en/state-accounts-report?defuat_state_id={state_id}

  PIB Tax Devolution PDFs     — central devolution per instalment (all states)
    https://static.pib.gov.in/WriteReadData/specificdocs/documents/...

  RBI State Finances          — manual-link only (rbidocs.rbi.org.in blocks automation)
    https://rbi.org.in/Scripts/AnnualPublications.aspx?head=State+Finances+...
    Use --rbi-xlsx /path/to/downloaded.xlsx to process a manually downloaded file.

Firestore schema
----------------
  Collection: state_budgets
  Doc ID:     {state_code}_{fiscal_year}   e.g. TN_2024-25

Usage
-----
  # Dry run for Tamil Nadu (all available years)
  .venv/bin/python scrapers/state_budget_ingest.py --state TN --dry-run

  # All states, latest year only
  .venv/bin/python scrapers/state_budget_ingest.py --latest-only

  # Specific states, upload
  .venv/bin/python scrapers/state_budget_ingest.py --state TN KL KA MH --upload

  # Process a PIB devolution PDF and merge into existing records
  .venv/bin/python scrapers/state_budget_ingest.py --pib-pdf data/raw/pib/devolution_oct2025.pdf --fiscal-year 2025-26 --dry-run

  # Process a manually downloaded RBI Excel (Appendix Table 1-5)
  .venv/bin/python scrapers/state_budget_ingest.py --rbi-xlsx /tmp/rbi_appendix1.xlsx --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings()

ROOT      = Path(__file__).resolve().parent.parent
OUT_DIR   = ROOT / "data" / "processed"
RAW_CAG   = ROOT / "data" / "raw" / "cag_state_finances"
RAW_PIB   = ROOT / "data" / "raw" / "pib"
PROJECT   = os.getenv("GOOGLE_CLOUD_PROJECT", "naatunadappu")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
}

# ---------------------------------------------------------------------------
# State catalogue — CAG state IDs + standard codes / names
# ---------------------------------------------------------------------------
STATE_CATALOGUE: list[dict] = [
    {"code": "AP",  "name": "Andhra Pradesh",    "cag_id": 64},
    {"code": "AR",  "name": "Arunachal Pradesh", "cag_id": 65},
    {"code": "AS",  "name": "Assam",             "cag_id": 66},
    {"code": "BR",  "name": "Bihar",             "cag_id": 67},
    {"code": "CG",  "name": "Chhattisgarh",      "cag_id": 68},
    {"code": "DL",  "name": "Delhi",             "cag_id": 69},
    {"code": "GA",  "name": "Goa",               "cag_id": 70},
    {"code": "GJ",  "name": "Gujarat",           "cag_id": 71},
    {"code": "HR",  "name": "Haryana",           "cag_id": 72},
    {"code": "HP",  "name": "Himachal Pradesh",  "cag_id": 73},
    {"code": "JK",  "name": "Jammu & Kashmir",   "cag_id": 74},
    {"code": "JH",  "name": "Jharkhand",         "cag_id": 75},
    {"code": "KA",  "name": "Karnataka",         "cag_id": 76},
    {"code": "KL",  "name": "Kerala",            "cag_id": 77},
    {"code": "MP",  "name": "Madhya Pradesh",    "cag_id": 78},
    {"code": "MH",  "name": "Maharashtra",       "cag_id": 79},
    {"code": "MN",  "name": "Manipur",           "cag_id": 80},
    {"code": "ML",  "name": "Meghalaya",         "cag_id": 81},
    {"code": "MZ",  "name": "Mizoram",           "cag_id": 82},
    {"code": "NL",  "name": "Nagaland",          "cag_id": 83},
    {"code": "OD",  "name": "Odisha",            "cag_id": 84},
    {"code": "PB",  "name": "Punjab",            "cag_id": 85},
    {"code": "RJ",  "name": "Rajasthan",         "cag_id": 86},
    {"code": "SK",  "name": "Sikkim",            "cag_id": 87},
    {"code": "TN",  "name": "Tamil Nadu",        "cag_id": 88},
    {"code": "TR",  "name": "Tripura",           "cag_id": 89},
    {"code": "UP",  "name": "Uttar Pradesh",     "cag_id": 90},
    {"code": "UK",  "name": "Uttarakhand",       "cag_id": 91},
    {"code": "WB",  "name": "West Bengal",       "cag_id": 92},
    {"code": "TS",  "name": "Telangana",         "cag_id": 93},
    {"code": "PY",  "name": "Puducherry",        "cag_id": 366},
]

STATE_BY_CODE = {s["code"]: s for s in STATE_CATALOGUE}
STATE_BY_CAG_ID = {s["cag_id"]: s for s in STATE_CATALOGUE}

# PIB state name → state code mapping for devolution table parsing
PIB_NAME_MAP: dict[str, str] = {
    "ANDHRA PRADESH": "AP", "ARUNACHAL PRADESH": "AR", "ASSAM": "AS",
    "BIHAR": "BR", "CHHATTISGARH": "CG", "GOA": "GA", "GUJARAT": "GJ",
    "HARYANA": "HR", "HIMACHAL PRADESH": "HP", "JHARKHAND": "JH",
    "KARNATAKA": "KA", "KERALA": "KL", "MADHYA PRADESH": "MP",
    "MAHARASHTRA": "MH", "MANIPUR": "MN", "MEGHALAYA": "ML",
    "MIZORAM": "MZ", "NAGALAND": "NL", "ODISHA": "OD", "PUNJAB": "PB",
    "RAJASTHAN": "RJ", "SIKKIM": "SK", "TAMIL NADU": "TN", "TELANGANA": "TS",
    "TRIPURA": "TR", "UTTAR PRADESH": "UP", "UTTARAKHAND": "UK",
    "WEST BENGAL": "WB", "DELHI": "DL", "PUDUCHERRY": "PY",
    "PONDICHERRY": "PY",
}


# ---------------------------------------------------------------------------
# Manual records — for scanned/inaccessible PDFs (hardcoded from official sources)
# ---------------------------------------------------------------------------
# Sources for each entry are cited inline. Values in ₹ crore.
# TN 2023-24: CAG "Accounts at a Glance 2024-25" (5-year trend tables)
#             + PRS "Tamil Nadu Budget Analysis 2025-26" (Table 3, 5, 7)
#             URL: https://cag.gov.in/uploads/state_accounts_report/account-report-Accounts-at-a-glance-Eng-2024-25-0699be10d0cf539-36906021.pdf
#             URL: https://prsindia.org/files/budget/budget_state/tamil-nadu/2025/TN_Budget_Analysis_2025-26.pdf
MANUAL_RECORDS: dict[str, dict] = {
    # TN 2022-23: CAG "Accounts at a Glance 2024-25" (5-year trend tables)
    #             + PRS "Tamil Nadu Budget Analysis 2024-25" (Tables 2, 3, 5)
    #             + PRS "Tamil Nadu Budget Analysis 2025-26" (Table 7 cross-check)
    "TN_2022-23": {
        "doc_id":            "TN_2022-23",
        "state_code":        "TN",
        "state_name":        "Tamil Nadu",
        "fiscal_year":       "2022-23",
        "fiscal_year_label": "April 2022 – March 2023",
        "data_type":         "Actuals",
        "revenue": {
            "own_tax_revenue_cr":        150223.0,   # 1,50,223 crore (PRS 2024-25 Table 5)
            "non_tax_revenue_cr":         17061.0,   # 17,061 crore (CAG trend / PRS 2024-25 Table 5)
            "central_devolution_cr":      38731.0,   # 38,731 crore
            "central_grants_cr":          37734.0,   # 37,734 crore
            "total_revenue_receipts_cr": 243749.0,   # 2,43,749 crore
        },
        "expenditure": {
            "revenue_exp_cr":  279964.0,   # 2,79,964 crore (PRS 2024-25 Table 2)
            "capital_exp_cr":   39530.0,   # 39,530 crore (PRS 2024-25 Table 2)
            "total_exp_cr":    319494.0,
        },
        "committed": {
            "salaries_cr":        68588.0,   # 68,588 crore (PRS 2024-25 Table 3)
            "pensions_cr":        32177.0,   # 32,177 crore
            "interest_cr":        46911.0,   # 46,911 crore
            "subsidies_cr":        None,
            "grants_in_aid_cr":    None,
            "total_committed_cr": 147676.0,  # 1,47,676 crore (PRS 2024-25 Table 3)
            "discretionary_cr":  132288.0,   # revenue_exp - total_committed
        },
        "fiscal": {
            "fiscal_deficit_cr":       81886.0,   # 81,886 crore (PRS 2024-25 Table 1)
            "closing_cash_balance_cr":   None,
        },
        "feasibility": {
            "total_revenue_cr":    243749.0,
            "total_rev_exp_cr":    279964.0,
            "committed_cr":        147676.0,
            "discretionary_cr":    132288.0,
            "cost_per_rupee_per_lakh_pop_cr": 0.12,
            "pct_discretionary_per_rupee_lakh": round(0.12 / 132288.0 * 100, 6),
        },
        "source": (
            "CAG Accounts at a Glance 2024-25 (5-year trend tables); "
            "PRS Tamil Nadu Budget Analysis 2024-25 (Tables 2, 3, 5). "
            "Original Finance Accounts 2022-23 PDF is scanned/image-only."
        ),
        "source_url": "https://prsindia.org/files/budget/budget_state/tamil-nadu/2024/Tamil_Nadu_Budget_Analysis_2024-25.pdf",
        "_manual_entry": True,
    },

    # TN 2021-22: PRS "Tamil Nadu Budget Analysis 2023-24" (Tables 2, 3, 5, 7)
    #             URL: https://prsindia.org/files/budget/budget_state/tamil-nadu/2023/TN_Budget_Analysis_2023-24.pdf
    "TN_2021-22": {
        "doc_id":            "TN_2021-22",
        "state_code":        "TN",
        "state_name":        "Tamil Nadu",
        "fiscal_year":       "2021-22",
        "fiscal_year_label": "April 2021 – March 2022",
        "data_type":         "Actuals",
        "revenue": {
            "own_tax_revenue_cr":        122866.0,   # 1,22,866 crore (PRS 2023-24 Table 5 / Table 7)
            "non_tax_revenue_cr":         12117.0,   # 12,117 crore
            "central_devolution_cr":      37459.0,   # 37,459 crore
            "central_grants_cr":          35051.0,   # 35,051 crore
            "total_revenue_receipts_cr": 207492.0,   # 2,07,492 crore
        },
        "expenditure": {
            "revenue_exp_cr":  254030.0,   # 2,54,030 crore (PRS 2023-24 Table 2)
            "capital_exp_cr":   37011.0,   # 37,011 crore
            "total_exp_cr":    291041.0,
        },
        "committed": {
            "salaries_cr":        60625.0,   # 60,625 crore (PRS 2023-24 Table 3)
            "pensions_cr":        26250.0,   # 26,250 crore
            "interest_cr":        41564.0,   # 41,564 crore
            "subsidies_cr":        None,
            "grants_in_aid_cr":    None,
            "total_committed_cr": 128439.0,  # 1,28,439 crore (PRS 2023-24 Table 3)
            "discretionary_cr":  125591.0,   # revenue_exp - total_committed
        },
        "fiscal": {
            "fiscal_deficit_cr":       73739.0,   # 73,739 crore (PRS 2023-24 Table 1)
            "closing_cash_balance_cr":   None,
        },
        "feasibility": {
            "total_revenue_cr":    207492.0,
            "total_rev_exp_cr":    254030.0,
            "committed_cr":        128439.0,
            "discretionary_cr":    125591.0,
            "cost_per_rupee_per_lakh_pop_cr": 0.12,
            "pct_discretionary_per_rupee_lakh": round(0.12 / 125591.0 * 100, 6),
        },
        "source": (
            "PRS Tamil Nadu Budget Analysis 2023-24 (Tables 2, 3, 5, 7 — 2021-22 Actuals column). "
            "Original Finance Accounts 2021-22 PDF is scanned/image-only."
        ),
        "source_url": "https://prsindia.org/files/budget/budget_state/tamil-nadu/2023/TN_Budget_Analysis_2023-24.pdf",
        "_manual_entry": True,
    },

    "TN_2023-24": {
        "doc_id":            "TN_2023-24",
        "state_code":        "TN",
        "state_name":        "Tamil Nadu",
        "fiscal_year":       "2023-24",
        "fiscal_year_label": "April 2023 – March 2024",
        "data_type":         "Actuals",
        "revenue": {
            # Source: PRS TN Budget Analysis 2025-26, Table 7 & Table 5 (2023-24 Actuals column)
            "own_tax_revenue_cr":        167278.0,   # 1,67,278 crore
            "non_tax_revenue_cr":         25904.0,   # 25,904 crore
            "central_devolution_cr":      46072.0,   # 46,072 crore (Share in central taxes)
            "central_grants_cr":          25342.0,   # 25,342 crore (Grants-in-aid from Centre)
            "total_revenue_receipts_cr": 264597.0,   # 2,64,597 crore
        },
        "expenditure": {
            # Source: PRS TN Budget Analysis 2025-26, Table 2 (2023-24 Actuals column)
            "revenue_exp_cr":  309718.0,   # 3,09,718 crore
            "capital_exp_cr":   40500.0,   # 40,500 crore (Capital Outlay, excl. loans)
            "total_exp_cr":    350218.0,   # revenue + capital
        },
        "committed": {
            # Source: PRS TN Budget Analysis 2025-26, Table 3 (2023-24 Actuals column)
            "salaries_cr":        75030.0,   # 75,030 crore
            "pensions_cr":        37697.0,   # 37,697 crore
            "interest_cr":        53566.0,   # 53,566 crore
            "subsidies_cr":        None,     # not separately available for 2023-24 actuals
            "grants_in_aid_cr":    None,
            "total_committed_cr": 166293.0,  # 1,66,293 crore (PRS Table 3 total)
            "discretionary_cr":  143425.0,   # revenue_exp - total_committed
        },
        "fiscal": {
            # Source: PRS TN Budget Analysis 2025-26, Table 1 (2023-24 Actuals column)
            "fiscal_deficit_cr":        90442.0,   # 90,442 crore
            "closing_cash_balance_cr":   None,
        },
        "feasibility": {
            "total_revenue_cr":    264597.0,
            "total_rev_exp_cr":    309718.0,
            "committed_cr":        166293.0,
            "discretionary_cr":    143425.0,
            "cost_per_rupee_per_lakh_pop_cr": 0.12,
            "pct_discretionary_per_rupee_lakh": round(0.12 / 143425.0 * 100, 6),
        },
        "source": (
            "CAG Accounts at a Glance 2024-25 (5-year trend tables); "
            "PRS Tamil Nadu Budget Analysis 2025-26 (Tables 3, 5, 7). "
            "Original Finance Accounts 2023-24 PDF is scanned/image-only."
        ),
        "source_url": "https://cag.gov.in/uploads/state_accounts_report/account-report-Accounts-at-a-glance-Eng-2024-25-0699be10d0cf539-36906021.pdf",
        "_manual_entry": True,
    },
}


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get(url: str, timeout: int = 60, stream: bool = False) -> requests.Response:
    """GET with browser headers and SSL verification disabled (govt sites)."""
    return requests.get(url, headers=HEADERS, timeout=timeout,
                        verify=False, stream=stream)


# ---------------------------------------------------------------------------
# CAG — scrape Finance Accounts Vol I PDF links for a state
# ---------------------------------------------------------------------------

def fetch_cag_pdf_links(cag_id: int) -> list[dict]:
    """
    Returns list of {fiscal_year, url, doc_type} for Finance Accounts Vol I PDFs
    available on the CAG state accounts page for the given state.
    """
    url = f"https://cag.gov.in/en/state-accounts-report?defuat_state_id={cag_id}"
    try:
        r = _get(url, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"  [CAG] Failed to fetch state page (id={cag_id}): {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    results = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)

        # Match Finance Accounts Vol I links only
        if not ("Finance" in text or "Finance" in href):
            continue
        # Exclude Vol II
        if "VOL-II" in href.upper() or "VOL II" in text.upper():
            continue
        # Only accept PDF links from uploads server
        if "cag.gov.in/uploads" not in href and not href.startswith("/uploads"):
            continue

        # Derive full URL
        full_url = href if href.startswith("http") else f"https://cag.gov.in{href}"

        # Extract fiscal year from URL (pattern: ...-2024-25-...)
        fy_match = re.search(r"(\d{4}-\d{2})[-_]", href)
        if not fy_match:
            continue
        fiscal_year = fy_match.group(1)

        # Determine type from text
        if "Accounts at a Glance" in text or "account-at" in href.lower():
            doc_type = "accounts_at_a_glance"
        elif "Finance Accounts" in text or "Finance-Accounts" in href:
            doc_type = "finance_accounts_vol1"
        else:
            continue

        results.append({
            "fiscal_year": fiscal_year,
            "url": full_url,
            "doc_type": doc_type,
        })

    # Deduplicate (same year may appear multiple times due to repeated link text)
    seen = set()
    unique = []
    for r in results:
        key = (r["fiscal_year"], r["doc_type"])
        if key not in seen:
            seen.add(key)
            unique.append(r)

    return sorted(unique, key=lambda x: x["fiscal_year"], reverse=True)


# ---------------------------------------------------------------------------
# CAG — download and parse Finance Accounts Vol I
# ---------------------------------------------------------------------------

def download_cag_pdf(url: str, dest: Path) -> bool:
    """Download a CAG PDF. Returns True on success."""
    try:
        r = _get(url, timeout=90, stream=True)
        r.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as f:
            for chunk in r.iter_content(65536):
                f.write(chunk)
        return True
    except Exception as e:
        print(f"  [download] Failed: {e}")
        return False


def _clean_num(val: str | None) -> float | None:
    """Parse an Indian-format number string like '1,80,225.40' → 180225.40."""
    if not val:
        return None
    cleaned = re.sub(r"[,\s]", "", str(val))
    # Remove (cid:XX) encoding artifacts
    cleaned = re.sub(r"\(cid:\d+\)", "", cleaned)
    cleaned = cleaned.replace("(", "").replace(")", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def _extract_number_after(text: str, pattern: str) -> float | None:
    """
    Find `pattern` in text (case-insensitive), then grab the first
    Indian-format number that follows it on the same or next line.
    Returns the value in crore.
    """
    num_re = r"([\d,]+\.\d{1,2})"
    m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    snippet = text[m.end(): m.end() + 300]
    n = re.search(num_re, snippet)
    if not n:
        return None
    return _clean_num(n.group(1))


def parse_finance_accounts_vol1(pdf_path: Path, fiscal_year: str, state_code: str) -> dict | None:
    """
    Parse CAG Finance Accounts Vol I PDF.
    Targets Statement 2 (Receipts & Disbursements) and its Cash Balance Annexure.
    Returns a structured dict of key fiscal metrics (all values in ₹ crore).
    """
    try:
        import pdfplumber
    except ImportError:
        print("  ERROR: pdfplumber not installed. Run: pip install pdfplumber")
        return None

    print(f"  [parse] {pdf_path.name} ({fiscal_year})")

    stmt2_pages: list[str] = []
    annexure_pages: list[str] = []

    # Indian number pattern: 1,80,225.40 or 47,107.95 etc.
    _NUM = r"[\d,]+\.\d{1,2}"

    with pdfplumber.open(str(pdf_path)) as pdf:
        total_pages = len(pdf.pages)
        print(f"  [parse] {total_pages} pages")

        for page in pdf.pages:
            text = page.extract_text() or ""
            text = text.replace("(cid:67)", "₹").replace("(cid:3)", " ")

            is_stmt2 = "STATEMENT OF RECEIPTS AND DISBURSEMENTS" in text.upper()
            is_annex = "CASH BALANCES AND INVESTMENTS" in text.upper()
            # Only include pages that actually have large financial numbers (not guide pages)
            has_data = bool(re.search(r"\d{2},\d{3}\.\d{2}", text))

            if is_annex and has_data:
                annexure_pages.append(text)
            elif is_stmt2 and has_data and not is_annex:
                stmt2_pages.append(text)

    if not stmt2_pages:
        print("  [parse] WARNING: Statement 2 not found in PDF")
        return None

    # Deduplicate near-identical pages (some CAG PDFs print each page twice)
    seen_keys: set[str] = set()
    unique_stmt2: list[str] = []
    for text in stmt2_pages:
        key = text[:300]
        if key not in seen_keys:
            seen_keys.add(key)
            unique_stmt2.append(text)
    seen_keys_a: set[str] = set()
    unique_annex: list[str] = []
    for text in annexure_pages:
        key = text[:300]
        if key not in seen_keys_a:
            seen_keys_a.add(key)
            unique_annex.append(text)

    full_stmt2 = "\n".join(unique_stmt2)
    full_annex = "\n".join(unique_annex)

    # ── Extraction helpers ────────────────────────────────────────────────────

    def _first_num(pattern: str, text: str) -> float | None:
        """Find pattern, return first Indian number in next 300 chars."""
        m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if not m:
            return None
        snippet = text[m.end(): m.end() + 300]
        n = re.search(_NUM, snippet)
        return _clean_num(n.group(0)) if n else None

    def _nth_match_first_num(pattern: str, text: str, n: int) -> float | None:
        """Return first number after the Nth (0-indexed) match of pattern."""
        matches = list(re.finditer(pattern, text, re.IGNORECASE | re.DOTALL))
        if len(matches) <= n:
            return None
        snippet = text[matches[n].end(): matches[n].end() + 300]
        nm = re.search(_NUM, snippet)
        return _clean_num(nm.group(0)) if nm else None

    def _num_before(pattern: str, text: str, lookback: int = 150) -> float | None:
        """Find last Indian number in the `lookback` chars BEFORE first match of pattern."""
        m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if not m:
            return None
        preceding = text[max(0, m.start() - lookback): m.start()]
        nums = re.findall(_NUM, preceding)
        return _clean_num(nums[-1]) if nums else None

    # ── Revenue Receipts ─────────────────────────────────────────────────────
    # "(Ref. Statement 3 & 14) 2,82,829.28 ..." — 1st occurrence
    revenue_receipts   = _first_num(r"\(Ref\.\s*Statement\s*3\s*&\s*14\)", full_stmt2)

    # Revenue Expenditure: "(Ref. Statement 4-A, 4-B" WITH space → 3,28,669.40
    revenue_exp        = _first_num(r"\(Ref\.\s*Statement\s*4-A,\s+4-B\b", full_stmt2)

    # Capital Expenditure: FIRST "(Ref. Statement 4-A,4-B" WITHOUT space → 47,107.95
    capital_exp        = _nth_match_first_num(r"\(Ref\.\s*Statement\s*4-A,4-B\b", full_stmt2, 0)

    # Own Tax Revenue: after "the State)" → 1,80,225.40
    own_tax_revenue    = _first_num(r"the\s+State\)", full_stmt2)

    # Salaries: after "(Ref. Statement 4-B &" → 46,425.82
    salaries           = _first_num(r"\(Ref\.\s*Statement\s*4-B\s*&", full_stmt2)

    # Non-tax Revenue: 3rd occurrence of "(Ref. Statement 3 & 14)" → 33,602.62
    # (2nd occurrence appears as part of the capital expenditure reference block)
    non_tax_revenue    = _nth_match_first_num(r"\(Ref\.\s*Statement\s*3\s*&\s*14\)", full_stmt2, 2)

    # Subsidies: "(Ref. Statement 4B" (no hyphen) → 52,603.28
    subsidies          = _first_num(r"\(Ref\.\s*Statement\s*4B\b", full_stmt2)

    # Grants-in-Aid: large number (>10,000) after "Grants-in-Aid" label
    # Line: "Grants-in-Aid 1,2 82,947.66" — "1,2" is a footnote marker, skip it
    grants_in_aid: float | None = None
    for gm in re.finditer(r"Grants.in.Aid", full_stmt2, re.IGNORECASE):
        snippet = full_stmt2[gm.end(): gm.end() + 200]
        for candidate in re.findall(_NUM, snippet):
            v = _clean_num(candidate)
            if v and v > 10000:
                grants_in_aid = v
                break
        if grants_in_aid:
            break

    # Central Devolution (Share of Union Taxes/Duties): after "Taxes/Duties" → 52,491.88
    central_devolution = _first_num(r"Taxes[/]?Duties", full_stmt2)

    # Central Grants: garbled "...Central 16,509.38..." — first realistic number after "Central"
    central_grants: float | None = None
    for cgm in re.finditer(r"\bCentral\b", full_stmt2, re.IGNORECASE):
        snippet = full_stmt2[cgm.end(): cgm.end() + 80]
        n = re.search(_NUM, snippet)
        if n:
            v = _clean_num(n.group(0))
            if v and 500 < v < 80000:
                central_grants = v
                break

    # Interest Payment: SECOND occurrence of "(Ref. Statement 4-A,4-B" (0-indexed: 1)
    # Order in PDF: [0]=Capital Exp, [1]=Interest Payment, [2]=Pension
    # Line: "(Ref. Statement 4-A,4-B 60,166.24 54,848.85 (Ref. Statement 3,7 & 18)"
    interest_payment   = _nth_match_first_num(r"\(Ref\.\s*Statement\s*4-A,4-B\b", full_stmt2, 1)

    # Pension: THIRD occurrence of "(Ref. Statement 4-A,4-B" (0-indexed: 2)
    # Order in PDF: [0]=Capital, [1]=Interest, [2]=Pension
    pension            = _nth_match_first_num(r"\(Ref\.\s*Statement\s*4-A,4-B\b", full_stmt2, 2)

    # ── Fiscal position ─────────────────────────────────────────────────────
    # Fiscal Deficit appears in various formats across states:
    #   "Fiscal Deficit (a) 48,922.06"   (TS)
    #   "Fiscal Deficit(*) 48,248.14"    (KL)
    #   "Fiscal Deficit 85,029.56(#)"    (KA)
    #   "Fiscal Deficit 81,071.17"       (AP)
    fiscal_deficit = None
    for fd_pat in [
        r"Fiscal\s+Deficit\s*\(a\)",             # TS format
        r"Fiscal\s+Deficit\s*\(\*\)",             # KL format
        r"Fiscal\s+Deficit(?:\s*\([^)]*\))*\s*",  # generic: skip any footnote markers
    ]:
        fiscal_deficit = _extract_number_after(full_stmt2, fd_pat)
        if fiscal_deficit is not None:
            break

    # Revenue Deficit — similar format variations
    revenue_deficit = None
    for rd_pat in [
        r"Revenue\s+Deficit\s*\(-?\)",
        r"Revenue\s+Deficit(?:\s*\([^)]*\))*\s*",
    ]:
        revenue_deficit = _extract_number_after(full_stmt2, rd_pat)
        if revenue_deficit is not None:
            break

    # Primary Deficit
    primary_deficit = _extract_number_after(full_stmt2, r"Primary\s+Deficit")

    # ── Fallback: extract from fiscal summary page ─────────────────────────
    # Some states have a compact summary page with lines like:
    #   "Revenue Receipts 2,58,152.52 Revenue Expenditure 2,78,986.97"
    #   "Total Receipts 3,61,836.42 Total Expenditure 3,68,419.19"
    # Try these as fallback for missing fields.
    if revenue_exp is None:
        revenue_exp = _extract_number_after(full_stmt2, r"Revenue\s+Expenditure\s+")
        # Avoid picking up "Revenue Expenditure" from guide/contents pages
        if revenue_exp is not None and revenue_exp < 1000:
            revenue_exp = None

    if capital_exp is None:
        capital_exp = _extract_number_after(full_stmt2, r"Capital\s+Expenditure\s+(?:Stt|Statement)")
        if capital_exp is None:
            # Try the summary page format: "Capital Receipts X Capital Expenditure Y"
            for ce_m in re.finditer(r"Capital\s+Expenditure\s+", full_stmt2):
                snippet = full_stmt2[ce_m.end():ce_m.end() + 80]
                n = re.search(_NUM, snippet)
                if n:
                    v = _clean_num(n.group(0))
                    if v and v > 5000:  # realistic capital expenditure
                        capital_exp = v
                        break

    # ── Cash balance (from Annexure) ─────────────────────────────────────────
    # Closing Cash Balance appears as "Closing Cash Balance (-)17.15 (-)93.22"
    cash_text = full_annex or full_stmt2
    cb_match = re.search(r"Closing Cash Balance\s*(\(?-?\)?[\d,]+\.\d{2})", cash_text)
    closing_cash_balance: float | None = None
    if cb_match:
        raw_cb = cb_match.group(1).replace("(", "-").replace(")", "")
        closing_cash_balance = _clean_num(raw_cb)

    # ── Derived metrics ──────────────────────────────────────────────────────
    total_committed: float | None = None
    if salaries is not None and pension is not None and interest_payment is not None:
        total_committed = salaries + pension + interest_payment

    discretionary: float | None = None
    if revenue_exp is not None and total_committed is not None:
        discretionary = revenue_exp - total_committed

    total_exp: float | None = None
    if revenue_exp is not None and capital_exp is not None:
        total_exp = revenue_exp + capital_exp
    # Fallback: extract "Total Expenditure" directly from summary page
    if total_exp is None:
        total_exp = _extract_number_after(full_stmt2, r"Total\s+Expenditure\s+(?:Consolidated\s+)?")
        if total_exp is not None and total_exp < 10000:
            total_exp = None  # avoid picking up small/irrelevant numbers

    # Fiscal year label  e.g. "2024-25" → "April 2024 – March 2025"
    fy_label = _fy_label(fiscal_year)

    record: dict[str, Any] = {
        "doc_id":             f"{state_code}_{fiscal_year}",
        "state_code":         state_code,
        "state_name":         STATE_BY_CODE.get(state_code, {}).get("name", state_code),
        "fiscal_year":        fiscal_year,
        "fiscal_year_label":  fy_label,
        "data_type":          "Actuals",

        "revenue": {
            "own_tax_revenue_cr":    own_tax_revenue,
            "non_tax_revenue_cr":    non_tax_revenue,
            "central_devolution_cr": central_devolution,
            "central_grants_cr":     central_grants,
            "total_revenue_receipts_cr": revenue_receipts,
        },
        "expenditure": {
            "revenue_expenditure_cr": revenue_exp,
            "capital_expenditure_cr": capital_exp,
            "total_exp_cr":           total_exp,
        },
        "committed": {
            "salaries_cr":        salaries,
            "pensions_cr":        pension,
            "interest_cr":        interest_payment,
            "subsidies_cr":       subsidies,
            "grants_in_aid_cr":   grants_in_aid,
            "total_committed_cr": total_committed,
            "discretionary_cr":   discretionary,
        },
        "fiscal": {
            "fiscal_deficit_cr":       fiscal_deficit,
            "revenue_deficit_cr":      revenue_deficit,
            "primary_deficit_cr":      primary_deficit,
            "closing_cash_balance_cr": closing_cash_balance,
        },

        # Promise feasibility helpers (pre-computed)
        "feasibility": _compute_feasibility(
            revenue_receipts, revenue_exp, total_committed, discretionary
        ),

        "source":     "CAG Finance Accounts Volume I",
        "source_url": f"https://cag.gov.in/en/state-accounts-report?defuat_state_id={STATE_BY_CODE.get(state_code, {}).get('cag_id', '')}",
        "_ingested_at": datetime.now(timezone.utc).isoformat(),
    }

    return record


def _fy_label(fy: str) -> str:
    """'2024-25' → 'April 2024 – March 2025'"""
    try:
        start = int(fy.split("-")[0])
        return f"April {start} – March {start + 1}"
    except Exception:
        return fy


def _compute_feasibility(
    revenue: float | None,
    rev_exp: float | None,
    committed: float | None,
    discretionary: float | None,
) -> dict:
    """
    Pre-compute promise feasibility anchor points.
    These help the frontend answer: "can the state afford promise X?"

    cost_1cr_rupees_per_lakh_pop: cost (₹ cr) per ₹1/month per lakh people
    → frontend can multiply: promise_amount × target_pop_lakhs × this factor
    """
    result: dict[str, Any] = {}
    if revenue:
        result["total_revenue_cr"] = revenue
    if rev_exp:
        result["total_rev_exp_cr"] = rev_exp
    if committed:
        result["committed_cr"] = committed
    if discretionary:
        result["discretionary_cr"] = discretionary
        # ₹1/month to 1 lakh people = 1 × 12 × 1,00,000 = ₹12,00,000 = ₹0.12 crore
        result["cost_per_rupee_per_lakh_pop_cr"] = 0.12
        # % of discretionary that ₹1/month per lakh people consumes
        result["pct_discretionary_per_rupee_lakh"] = round(0.12 / discretionary * 100, 6)
    return result


# ---------------------------------------------------------------------------
# PIB — parse devolution PDF
# ---------------------------------------------------------------------------

def parse_pib_devolution(pdf_path: Path, fiscal_year: str) -> list[dict]:
    """
    Parse a PIB tax devolution press release PDF.
    Returns list of {state_code, state_name, amount_cr, fiscal_year, release_date}.
    """
    try:
        import pdfplumber
    except ImportError:
        print("  ERROR: pdfplumber not installed")
        return []

    print(f"  [PIB] Parsing {pdf_path.name}")
    all_text = ""
    all_tables: list = []

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            all_text += "\n" + text
            tables = page.extract_tables()
            all_tables.extend(tables)

    # Extract release date from text
    date_match = re.search(r"(\d{1,2}(?:st|nd|rd|th)?\s+\w+\.?\s+\d{4})", all_text)
    release_date = date_match.group(1) if date_match else None

    # Extract total amount
    total_match = re.search(r"₹\s*([\d,]+)\s*crore", all_text)
    total_crore = _clean_num(total_match.group(1)) if total_match else None

    results: list[dict] = []

    for table in all_tables:
        if not table or len(table) < 2:
            continue
        # Look for table with "State" and "Amount" columns
        header = [str(h).strip().lower() if h else "" for h in table[0]]
        if not any("state" in h for h in header):
            continue
        state_col = next((i for i, h in enumerate(header) if "state" in h), None)
        amt_col = next((i for i, h in enumerate(header) if "amount" in h or "crore" in h), None)
        if state_col is None or amt_col is None:
            continue

        for row in table[1:]:
            if not row or len(row) <= max(state_col, amt_col):
                continue
            raw_state = str(row[state_col] or "").strip().upper()
            raw_amt   = str(row[amt_col]   or "").strip()
            if not raw_state or not raw_amt:
                continue
            state_code = PIB_NAME_MAP.get(raw_state)
            if not state_code:
                continue
            amount = _clean_num(raw_amt)
            if not amount:
                continue
            state_info = STATE_BY_CODE.get(state_code, {})
            results.append({
                "doc_id":       f"pib_devolution_{fiscal_year}_{state_code}",
                "state_code":   state_code,
                "state_name":   state_info.get("name", raw_state.title()),
                "fiscal_year":  fiscal_year,
                "instalment_cr": amount,
                "release_date": release_date,
                "total_release_cr": total_crore,
                "source":       "PIB Ministry of Finance",
                "source_url":   str(pdf_path),
                "_ingested_at": datetime.now(timezone.utc).isoformat(),
            })

    print(f"  [PIB] {len(results)} state devolution records extracted")
    return results


# ---------------------------------------------------------------------------
# RBI — manual-link Excel parser (Appendix Tables 1-5)
# ---------------------------------------------------------------------------

def parse_rbi_appendix(xlsx_path: Path) -> list[dict]:
    """
    Parse a manually downloaded RBI State Finances Appendix Table Excel file.
    Handles Appendix Tables 1-5 (deficit, devolution, development expenditure).
    Returns list of state-year records with whatever metrics are in the file.
    """
    try:
        import openpyxl
    except ImportError:
        print("  ERROR: openpyxl not installed. Run: pip install openpyxl")
        return []

    print(f"  [RBI] Parsing {xlsx_path.name}")
    wb = openpyxl.load_workbook(str(xlsx_path), data_only=True)
    results = []

    for sheet in wb.worksheets:
        rows = list(sheet.iter_rows(values_only=True))
        if not rows:
            continue
        # Find header row (contains "State" or "States")
        header_idx = None
        for i, row in enumerate(rows[:10]):
            row_str = " ".join(str(c or "") for c in row).lower()
            if "state" in row_str:
                header_idx = i
                break
        if header_idx is None:
            continue

        headers = [str(c or "").strip() for c in rows[header_idx]]
        print(f"  [RBI] Sheet '{sheet.title}' — headers: {headers[:8]}")

        for row in rows[header_idx + 1:]:
            if not row or not row[0]:
                continue
            state_name = str(row[0]).strip()
            if not state_name or state_name.lower() in ("", "total", "states", "all states"):
                continue
            record = {"state_raw": state_name}
            for j, h in enumerate(headers[1:], 1):
                if j < len(row) and row[j] is not None:
                    try:
                        record[h] = float(row[j])
                    except (ValueError, TypeError):
                        record[h] = str(row[j])
            results.append(record)

    print(f"  [RBI] {len(results)} rows extracted from {xlsx_path.name}")
    return results


# ---------------------------------------------------------------------------
# Firestore upload
# ---------------------------------------------------------------------------

def upload_records(
    records: list[dict],
    collection: str,
    project_id: str,
    dry_run: bool,
    id_field: str = "doc_id",
) -> None:
    if dry_run:
        print(f"\n[DRY RUN] Would upload {len(records)} docs to `{collection}`")
        for r in records[:3]:
            print(f"  {r.get(id_field, '?')}: {json.dumps({k: v for k, v in r.items() if k not in ('_ingested_at',)}, default=str)[:200]}")
        return

    try:
        from google.cloud import firestore as fs
    except ImportError:
        print("ERROR: google-cloud-firestore not installed")
        return

    db = fs.Client(project=project_id)
    col = db.collection(collection)
    for rec in records:
        doc_id = rec[id_field]
        col.document(doc_id).set(rec, merge=True)
    print(f"  Uploaded {len(records)} docs to Firestore `{collection}`")


# ---------------------------------------------------------------------------
# Core pipeline — CAG scrape for one state
# ---------------------------------------------------------------------------

def process_state_cag(
    state_code: str,
    latest_only: bool,
    dry_run: bool,
    project_id: str,
) -> list[dict]:
    state = STATE_BY_CODE.get(state_code)
    if not state:
        print(f"  Unknown state code: {state_code}")
        return []

    cag_id = state["cag_id"]
    print(f"\n── {state_code} ({state['name']}, CAG id={cag_id}) ──────────────")

    pdf_links = fetch_cag_pdf_links(cag_id)
    fa_links = [l for l in pdf_links if l["doc_type"] == "finance_accounts_vol1"]

    if not fa_links:
        print(f"  No Finance Accounts Vol I links found for {state_code}")
        return []

    print(f"  Found {len(fa_links)} Finance Accounts Vol I PDFs: "
          f"{[l['fiscal_year'] for l in fa_links]}")

    if latest_only:
        fa_links = fa_links[:1]

    records = []
    for link in fa_links:
        fy = link["fiscal_year"]
        url = link["url"]
        dest = RAW_CAG / f"{state_code}_finance_accounts_vol1_{fy}.pdf"

        # Use cached file if available
        if dest.exists():
            print(f"  [cache] {dest.name}")
        else:
            print(f"  [download] {fy} → {dest.name} ...", end=" ", flush=True)
            if not download_cag_pdf(url, dest):
                print("FAILED")
                continue
            print(f"{dest.stat().st_size // 1024} KB")
            time.sleep(1)  # polite delay

        # Use manual record if available (e.g. scanned PDF years)
        manual_key = f"{state_code}_{fy}"
        if manual_key in MANUAL_RECORDS:
            record = dict(MANUAL_RECORDS[manual_key])
            record["_ingested_at"] = datetime.now(timezone.utc).isoformat()
            print(f"  [manual] Using hardcoded record for {manual_key}")
            records.append(record)
            _print_summary(record)
            continue

        record = parse_finance_accounts_vol1(dest, fy, state_code)
        if record:
            records.append(record)
            _print_summary(record)
        else:
            print(f"  [parse] Failed to parse {dest.name}")

    # Inject any manual records for this state that weren't covered by CAG links
    covered_fys = {r["fiscal_year"] for r in records}
    for key, manual_rec in MANUAL_RECORDS.items():
        if manual_rec["state_code"] == state_code and manual_rec["fiscal_year"] not in covered_fys:
            if latest_only:
                continue  # skip historical manual records when --latest-only
            rec = dict(manual_rec)
            rec["_ingested_at"] = datetime.now(timezone.utc).isoformat()
            print(f"  [manual] Injecting {key} (not on CAG page)")
            records.append(rec)
            _print_summary(rec)

    if records:
        upload_records(records, "state_budgets", project_id, dry_run)

    return records


def _print_summary(r: dict) -> None:
    sc = r["state_code"]
    fy = r["fiscal_year"]
    rev = r["revenue"].get("total_revenue_receipts_cr")
    exp = r["expenditure"].get("total_exp_cr")
    disc = r["committed"].get("discretionary_cr")
    fd = r["fiscal"].get("fiscal_deficit_cr")
    print(f"  ✓ {sc} {fy}: Rev={_fmt(rev)} Exp={_fmt(exp)} "
          f"Discretionary={_fmt(disc)} Deficit={_fmt(fd)} (₹ crore)")


def _fmt(v: float | None) -> str:
    if v is None:
        return "?"
    return f"{v:,.0f}"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Scrape state budget data from CAG, PIB, and RBI into Firestore"
    )
    ap.add_argument("--state", nargs="+", metavar="CODE",
                    help="State codes to process (e.g. TN KL KA). Omit for all.")
    ap.add_argument("--latest-only", action="store_true",
                    help="Only process the most recent fiscal year per state")
    ap.add_argument("--upload", action="store_true",
                    help="Upload to Firestore (default: dry run)")
    ap.add_argument("--dry-run", action="store_true", default=True,
                    help="Preview only, no Firestore writes (default)")

    # PIB devolution mode
    ap.add_argument("--pib-pdf", metavar="PATH",
                    help="Path to a PIB devolution PDF to parse")
    ap.add_argument("--fiscal-year", metavar="FY",
                    help="Fiscal year for PIB/RBI file (e.g. 2025-26)")

    # RBI manual-link mode
    ap.add_argument("--rbi-xlsx", metavar="PATH",
                    help="Path to a manually downloaded RBI Appendix Table Excel")

    args = ap.parse_args()

    # --upload overrides --dry-run default
    dry_run = not args.upload

    project_id = PROJECT
    RAW_CAG.mkdir(parents=True, exist_ok=True)
    RAW_PIB.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # ── PIB mode ─────────────────────────────────────────────────────────────
    if args.pib_pdf:
        if not args.fiscal_year:
            ap.error("--fiscal-year is required with --pib-pdf")
        pib_path = Path(args.pib_pdf)
        if not pib_path.exists():
            ap.error(f"File not found: {pib_path}")
        records = parse_pib_devolution(pib_path, args.fiscal_year)
        if records:
            out = OUT_DIR / f"pib_devolution_{args.fiscal_year}.json"
            with open(out, "w") as f:
                json.dump(records, f, indent=2, default=str)
            print(f"  Saved → {out}")
            upload_records(records, "pib_devolution", project_id, dry_run)
        return

    # ── RBI manual-link mode ──────────────────────────────────────────────────
    if args.rbi_xlsx:
        xlsx_path = Path(args.rbi_xlsx)
        if not xlsx_path.exists():
            ap.error(f"File not found: {xlsx_path}")
        records = parse_rbi_appendix(xlsx_path)
        print(f"\n[RBI] Parsed {len(records)} rows — inspect output:")
        for r in records[:5]:
            print(f"  {r}")
        if args.fiscal_year and records:
            out = OUT_DIR / f"rbi_appendix_{args.fiscal_year}.json"
            with open(out, "w") as f:
                json.dump(records, f, indent=2, default=str)
            print(f"  Saved → {out}")
        return

    # ── CAG scrape mode ───────────────────────────────────────────────────────
    state_codes = args.state or [s["code"] for s in STATE_CATALOGUE]
    invalid = [c for c in state_codes if c not in STATE_BY_CODE]
    if invalid:
        ap.error(f"Unknown state codes: {invalid}. Valid: {list(STATE_BY_CODE.keys())}")

    print(f"Processing {len(state_codes)} state(s): {state_codes}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE UPLOAD'} | "
          f"{'Latest year only' if args.latest_only else 'All available years'}")

    all_records: list[dict] = []
    for code in state_codes:
        records = process_state_cag(code, args.latest_only, dry_run, project_id)
        all_records.extend(records)
        time.sleep(0.5)

    if all_records:
        out = OUT_DIR / "state_budgets.json"
        with open(out, "w") as f:
            json.dump(all_records, f, indent=2, default=str)
        print(f"\n✓ {len(all_records)} total records → {out}")

    print("\n── RBI Note ─────────────────────────────────────────────────────")
    print("  rbidocs.rbi.org.in blocks automated downloads (TLS fingerprinting).")
    print("  To use RBI Appendix Tables:")
    print("  1. Open https://rbi.org.in/Scripts/AnnualPublications.aspx?head=State+Finances+%3a+A+Study+of+Budgets")
    print("  2. Download Appendix Table 1-5 Excel files manually")
    print("  3. Run: .venv/bin/python scrapers/state_budget_ingest.py --rbi-xlsx /path/to/file.xlsx --fiscal-year 2025-26")


if __name__ == "__main__":
    main()
