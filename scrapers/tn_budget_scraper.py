"""
TN Finance Department — Budget Scraper + Manual-Link PDF Utility
Source: finance.tn.gov.in (Demands for Grants, Budget at a Glance)

Status: BLOCKED — finance.tn.gov.in has the same server-side TLS drop as
other TN govt domains (SSL_ERROR_SYSCALL mid-handshake). Even curl fails.

Manual-Link Utility: Since the .gov portal blocks automated access, this
module provides a CLI utility that accepts a local file path or a direct
PDF URL (e.g. from a browser download) and processes it into the
state_finances Firestore schema.

Usage (CLI):
    # Process a locally downloaded PDF
    python -m scrapers.tn_budget_scraper --pdf /path/to/budget_at_a_glance.pdf --year 2025-26

    # Process a direct PDF URL (bypasses the broken TN portal index)
    python -m scrapers.tn_budget_scraper --url "https://finance.tn.gov.in/..." --year 2025-26

    # Or call from main pipeline with --task manual-pdf
    python main.py --task manual-pdf --pdf /path/to/file.pdf --year 2025-26
"""

import argparse
import hashlib
import re
import sys
from pathlib import Path

import pdfplumber
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; NaatuNadappuResearchBot/1.0)"}

# Known TN Finance portal URL patterns — attempt these only after manual trigger.
# Do NOT auto-retry; the SSL failure is deterministic.
TN_FINANCE_BASE = "https://finance.tn.gov.in"
KNOWN_PDF_PATTERNS = [
    "/sites/default/files/budget_document/budget_at_a_glance_{year}.pdf",
    "/sites/default/files/budget_document/{year}/budget_at_a_glance.pdf",
    "/budget/{year}/budget_at_a_glance.pdf",
]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def download_from_url(url: str, dest: Path) -> tuple[Path, str]:
    """
    Download a PDF from a direct URL. Tries standard HTTPS first,
    then falls back to verify=False for govt sites.
    """
    print(f"  [fetch] {url}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=60, stream=True)
        r.raise_for_status()
    except Exception:
        # Fallback: skip SSL verification for known govt domains
        from requests.adapters import HTTPAdapter
        from urllib3.util.ssl_ import create_urllib3_context
        import ssl

        class _LegacySSLAdapter(HTTPAdapter):
            def init_poolmanager(self, *args, **kwargs):
                ctx = create_urllib3_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT
                ctx.set_ciphers("DEFAULT@SECLEVEL=1")
                kwargs["ssl_context"] = ctx
                super().init_poolmanager(*args, **kwargs)

        session = requests.Session()
        session.mount("https://", _LegacySSLAdapter())
        r = session.get(url, headers=HEADERS, timeout=60, stream=True, verify=False)
        r.raise_for_status()

    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)
    return dest, sha256_file(dest)


# ---------------------------------------------------------------------------
# GoTN Budget at a Glance — PDF parser
# TN Finance dept PDFs have a different layout than PRS — they contain
# the full Demands for Grants breakdowns which PRS analysis doesn't include.
# ---------------------------------------------------------------------------

def parse_tn_budget_pdf(pdf_path: Path, year: str) -> dict:
    """
    Parse a TN Finance dept PDF (Budget at a Glance or Demands for Grants).
    Extracts departmental allocations not available in PRS analysis.
    """
    print(f"  [parse] TN Budget PDF: {pdf_path.name}")
    raw_dir = Path("data/raw/tn_budget")
    raw_dir.mkdir(parents=True, exist_ok=True)

    with pdfplumber.open(str(pdf_path)) as pdf:
        full_text = "\n".join(p.extract_text() or "" for p in pdf.pages)
        all_tables = []
        for page in pdf.pages:
            tbls = page.extract_tables()
            all_tables.extend(tbls)

    dept_allocations = _extract_departmental_allocations(full_text, all_tables)
    demands_for_grants = _extract_demands_for_grants(all_tables)

    return {
        "fiscal_year": year,
        "source": "GoTN Finance Department — Budget at a Glance",
        "departmental_allocations": dept_allocations,
        "demands_for_grants": demands_for_grants,
        "pdf_checksum": sha256_file(pdf_path),
        "source_url": TN_FINANCE_BASE,
        "ground_truth_confidence": "HIGH",
    }


def _extract_departmental_allocations(text: str, tables: list) -> list[dict]:
    """
    Extract department-wise allocations from TN Budget at a Glance PDF.
    The TN Finance format lists departments with voted + charged amounts.
    """
    allocations = []

    # Department name → allocation pattern common in TN Budget PDFs
    dept_pattern = re.compile(
        r"([A-Z][A-Za-z &,\-()]+)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)"
    )

    for line in text.splitlines():
        m = dept_pattern.match(line.strip())
        if m and _clean_num(m.group(2)):
            allocations.append({
                "department": m.group(1).strip(),
                "voted_cr": _clean_num(m.group(2)),
                "charged_cr": _clean_num(m.group(3)),
                "total_cr": _clean_num(m.group(4)),
            })

    return allocations


def _extract_demands_for_grants(tables: list) -> list[dict]:
    """
    Parse tabular Demands for Grants data — scheme-level allocations.
    Returns [{demand_no, department, scheme, amount_cr}]
    """
    grants = []
    for table in tables:
        if not table or len(table) < 2:
            continue
        headers = [str(h).lower().strip() if h else "" for h in table[0]]
        if "demand" not in " ".join(headers) and "department" not in " ".join(headers):
            continue
        for row in table[1:]:
            if not row or not any(row):
                continue
            grant = {}
            for i, h in enumerate(headers):
                if i < len(row) and row[i]:
                    grant[h] = str(row[i]).strip()
            if grant:
                grants.append(grant)

    return grants


def _clean_num(val: str | None) -> float | None:
    if val is None:
        return None
    cleaned = re.sub(r"[,%\s]", "", str(val))
    try:
        return float(cleaned)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# CLI interface — Manual-Link Utility
# ---------------------------------------------------------------------------

def run_manual_link(pdf_path_str: str | None, url: str | None, year: str) -> dict | None:
    """
    Entry point for the manual-link utility.
    Accepts either a local file path or a direct PDF URL.
    """
    dest = Path(f"data/raw/tn_budget/tn_budget_{year}.pdf")

    if pdf_path_str:
        pdf_path = Path(pdf_path_str)
        if not pdf_path.exists():
            print(f"  [error] File not found: {pdf_path}")
            return None
        print(f"  [local] Using {pdf_path}")

    elif url:
        try:
            pdf_path, _ = download_from_url(url, dest)
        except Exception as e:
            print(f"  [error] Download failed: {e}")
            return None
    else:
        print("  [error] Provide either --pdf or --url")
        return None

    return parse_tn_budget_pdf(pdf_path, year)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Manual-Link PDF Utility — process a TN Budget PDF into Firestore schema"
    )
    parser.add_argument("--pdf",  help="Path to a locally downloaded TN Budget PDF")
    parser.add_argument("--url",  help="Direct URL to a TN Budget PDF")
    parser.add_argument("--year", required=True, help="Fiscal year (e.g. 2025-26)")
    parser.add_argument("--upload", action="store_true", help="Upload to Firestore after parsing")
    args = parser.parse_args()

    doc = run_manual_link(args.pdf, args.url, args.year)
    if doc:
        import json
        print(json.dumps(doc, indent=2, default=str))
        if args.upload:
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from loaders.firestore_loader import upload_finance_manual
            upload_finance_manual(doc)
            print(f"  [uploaded] state_finances/{args.year} → Firestore")
