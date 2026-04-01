"""
PRS Legislative Research — Tamil Nadu Budget Analysis Scraper
Sources: prsindia.org/budgets/states/tamil-nadu-budget-analysis-{year}

Status: WORKING — PRS India serves standard HTTPS, no SSL issues.
PDFs are publicly available and have consistent 7-page layouts.

PDF structure (confirmed on 2025-26 edition):
  Page 1 — Budget Highlights + GSDP growth chart
  Page 2 — Table 1: Key Budget Figures (expenditure / receipts / deficits)
  Page 3 — Table 3: Committed Expenditure (salaries / pension / interest)
            Table 4: Sector-wise Expenditure
  Page 4 — Table 5: Break-up of Receipts (SOTR / devolution / grants)
  Page 5 — Deficits, Debt, and FRBM Targets
  Page 6 — Annexure: Comparison with other states
  Page 7 — Sources / Notes
"""

import hashlib
import re
import time
from pathlib import Path

import pdfplumber
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

RAW_DIR = Path("data/raw/prs")
RAW_DIR.mkdir(parents=True, exist_ok=True)

PRS_BASE = "https://prsindia.org"
PRS_STATES_PATH = "/budgets/states"

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; NaatuNadappuResearchBot/1.0)"}

BUDGET_YEARS = ["2025-26", "2024-25", "2023-24", "2022-23", "2021-22", "2020-21"]

# PDF filename patterns vary by year — discovered from PRS site
PDF_PATH_MAP = {
    "2025-26": "/files/budget/budget_state/tamil-nadu/2025/TN_Budget_Analysis_2025-26.pdf",
    "2024-25": "/files/budget/budget_state/tamil-nadu/2024/Tamil_Nadu_Budget_Analysis_2024-25.pdf",
    "2023-24": "/files/budget/budget_state/tamil-nadu/2023/TN_Budget_Analysis_2023-24.pdf",
    "2022-23": "/files/budget/budget_state/tamil-nadu/2022/Tamil Nadu Budget Analysis 2022-23.pdf",
    "2021-22": "/files/budget/budget_state/tamil-nadu/2021/Tamil Nadu Budget Analysis 2021-22.pdf",
    "2020-21": "/files/budget/budget_state/tamil-nadu/2020/State Budget Analysis - TN 2020-21.pdf",
}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=8))
def _get(url: str, stream: bool = False) -> requests.Response:
    resp = requests.get(url, headers=HEADERS, timeout=60, stream=stream)
    resp.raise_for_status()
    return resp


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def discover_pdf_url(year: str) -> str | None:
    """
    Try the known PDF path first; fall back to scraping the PRS analysis page.
    """
    if year in PDF_PATH_MAP:
        raw = f"{PRS_BASE}{PDF_PATH_MAP[year]}"
        return raw.replace(" ", "%20")

    # Fallback: scrape the analysis page for a .pdf link
    from bs4 import BeautifulSoup
    page_url = f"{PRS_BASE}{PRS_STATES_PATH}/tamil-nadu-budget-analysis-{year}"
    try:
        resp = _get(page_url)
        soup = BeautifulSoup(resp.text, "lxml")
        for a in soup.find_all("a", href=True):
            if ".pdf" in a["href"].lower():
                href = a["href"]
                return href if href.startswith("http") else f"{PRS_BASE}{href}"
    except Exception as e:
        print(f"  [warn] PRS page scrape failed for {year}: {e}")
    return None


def download_pdf(year: str) -> tuple[Path, str] | tuple[None, None]:
    dest = RAW_DIR / f"TN_Budget_Analysis_{year}.pdf"

    if dest.exists():
        print(f"  [cache] {dest.name}")
        return dest, sha256_file(dest)

    url = discover_pdf_url(year)
    if not url:
        print(f"  [warn] No PDF URL found for {year}")
        return None, None

    print(f"  [fetch] {year} → {url}")
    resp = _get(url, stream=True)
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(8192):
            f.write(chunk)
    time.sleep(1.0)
    return dest, sha256_file(dest)


# ---------------------------------------------------------------------------
# PDF Parsers — each targets a specific table in the PRS format
# ---------------------------------------------------------------------------

def _clean_num(val: str | None) -> float | None:
    """Strip commas, %, whitespace from a cell value and convert to float."""
    if val is None:
        return None
    cleaned = re.sub(r"[,%\s]", "", str(val))
    try:
        return float(cleaned)
    except ValueError:
        return None


def _extract_text_all(pdf_path: Path) -> str:
    """Extract full text from all pages — used as regex fallback."""
    with pdfplumber.open(str(pdf_path)) as pdf:
        return "\n".join(p.extract_text() or "" for p in pdf.pages)


def parse_table1_key_figures(pdf_path: Path) -> dict:
    """
    Table 1 (Page 2): Budget at a Glance — key expenditure / receipts / deficit figures.
    Returns dict with keys matching state_finances summary schema.
    Row order (confirmed from 2025-26):
      Total Expenditure, (-) Repayment of debt, Net Expenditure,
      Total Receipts, (-) Borrowings, ..., Net Receipts,
      Fiscal Deficit, as % of GSDP, Revenue Deficit, as % of GSDP,
      Primary Deficit, as % of GSDP, GSDP
    """
    result = {}
    text = _extract_text_all(pdf_path)

    patterns = {
        "net_expenditure_cr":       r"Net Expenditure.*?\d[\d,]+\s+\d[\d,]+\s+\d[\d,]+\s+-?\d+%\s+([\d,]+)",
        "net_receipts_cr":          r"Net Receipts.*?\d[\d,]+\s+\d[\d,]+\s+\d[\d,]+\s+-?\d+%\s+([\d,]+)",
        "fiscal_deficit_cr":        r"Fiscal Deficit \(E-R\)\s+\d[\d,]+\s+\d[\d,]+\s+\d[\d,]+\s+-?\d+%\s+([\d,]+)",
        "fiscal_deficit_pct_gsdp":  r"Fiscal Deficit \(E-R\).*?as % of GSDP\s+[\d.]+%\s+[\d.]+%\s+[\d.]+%\s+([\d.]+)%",
        "revenue_deficit_cr":       r"Revenue Deficit\s+\d[\d,]+\s+\d[\d,]+\s+\d[\d,]+\s+-?\d+%\s+([\d,]+)",
        "revenue_deficit_pct_gsdp": r"Revenue Deficit.*?as % of GSDP\s+[\d.]+%\s+[\d.]+%\s+[\d.]+%\s+([\d.]+)%",
        "primary_deficit_cr":       r"Primary Deficit\s+\d[\d,]+\s+\d[\d,]+\s+\d[\d,]+\s+-?\d+%\s+([\d,]+)",
        "primary_deficit_pct_gsdp": r"Primary Deficit.*?as % of GSDP\s+[\d.]+%\s+[\d.]+%\s+[\d.]+%\s+([\d.]+)%",
        "total_borrowings_cr":      r"\(-\) Borrowings\s+\d[\d,]+\s+\d[\d,]+\s+\d[\d,]+\s+-?\d+%\s+([\d,]+)",
        "debt_repayment_cr":        r"\(-\) Repayment of debt\s+\d[\d,]+\s+\d[\d,]+\s+\d[\d,]+\s+-?\d+%\s+([\d,]+)",
    }

    for key, pattern in patterns.items():
        m = re.search(pattern, text, re.DOTALL)
        if m:
            result[key] = _clean_num(m.group(1))

    # GSDP — parse from highlights bullet (most reliable location)
    gsdp_m = re.search(
        r"GSDP.*?Rs\s+([\d,]+)\s+crore.*?growth of ([\d.]+)%",
        text, re.DOTALL
    )
    if gsdp_m:
        result["gsdp_projected_cr"] = _clean_num(gsdp_m.group(1))
        result["gsdp_growth_pct"] = _clean_num(gsdp_m.group(2))

    return result


def parse_table3_committed_expenditure(pdf_path: Path) -> dict:
    """
    Table 3 (Page 3): Committed Expenditure — salaries, pension, interest.
    Returns dict with BE 2025-26 column values.
    """
    result = {}
    text = _extract_text_all(pdf_path)

    patterns = {
        "salaries_cr":          r"Salaries\s+\d[\d,]+\s+\d[\d,]+\s+\d[\d,]+\s+-?\d+%\s+([\d,]+)",
        "pensions_cr":          r"Pension\s+\d[\d,]+\s+\d[\d,]+\s+\d[\d,]+\s+-?\d+%\s+([\d,]+)",
        "interest_payments_cr": r"Interest payment\s+\d[\d,]+\s+\d[\d,]+\s+\d[\d,]+\s+-?\d+%\s+([\d,]+)",
        "committed_total_cr":   r"Total\s+\d[\d,]+\s+\d[\d,]+\s+\d[\d,]+\s+-?\d+%\s+([\d,]+)\s+\d+%",
    }
    for key, pattern in patterns.items():
        m = re.search(pattern, text)
        if m:
            result[key] = _clean_num(m.group(1))

    # Committed expenditure % of revenue receipts — from narrative
    pct_m = re.search(r"([\d.]+)%\s+of its estimated revenue receipts", text)
    if pct_m:
        result["committed_pct_revenue_receipts"] = float(pct_m.group(1))

    # Interest % of revenue receipts — from narrative
    int_pct_m = re.search(r"interest payments \(([\d.]+)%\)", text)
    if int_pct_m:
        result["interest_pct_revenue_receipts"] = float(int_pct_m.group(1))

    return result


def parse_table4_sector_expenditure(pdf_path: Path) -> list[dict]:
    """
    Table 4 (Page 3): Sector-wise expenditure — BE 2025-26 values.
    Returns list of {sector, allocation_cr, pct_change_from_re}.
    """
    sectors = []
    text = _extract_text_all(pdf_path)

    # Sector rows follow a consistent pattern: Name ... actuals BE RE BE pct%
    sector_patterns = [
        ("Education, Sports, Arts, and Culture", "education"),
        ("Social Welfare and Nutrition",         "social_welfare"),
        ("Transport",                             "transport"),
        ("Health and Family Welfare",             "health"),
        ("Agriculture and Allied Activities",    "agriculture"),
        ("Rural Development",                     "rural_development"),
        ("Urban Development",                     "urban_development"),
        ("Energy",                                "energy"),
        ("Irrigation",                            "irrigation"),
        ("Police",                                "police"),
    ]

    for full_name, slug in sector_patterns:
        # Match the row: sector name followed by 4 numbers and a pct change
        pattern = re.escape(full_name) + r"\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)\s+(-?\d+)%"
        m = re.search(pattern, text, re.DOTALL)
        if m:
            sectors.append({
                "sector": full_name,
                "sector_slug": slug,
                "actuals_2023_24_cr": _clean_num(m.group(1)),
                "be_2024_25_cr": _clean_num(m.group(2)),
                "re_2024_25_cr": _clean_num(m.group(3)),
                "be_2025_26_cr": _clean_num(m.group(4)),
                "pct_change_re_to_be": float(m.group(5)),
            })

    return sectors


def parse_table5_receipts(pdf_path: Path) -> dict:
    """
    Table 5 (Page 4): Break-up of state government receipts.
    """
    result = {}
    text = _extract_text_all(pdf_path)

    patterns = {
        "sotr_cr":                  r"State's Own Tax\s+\d[\d,]+\s+\d[\d,]+\s+\d[\d,]+\s+-?\d+%\s+([\d,]+)",
        "own_non_tax_revenue_cr":   r"State's Own Non-Tax\s+\d[\d,]+\s+\d[\d,]+\s+\d[\d,]+\s+-?\d+%\s+([\d,]+)",
        "central_tax_devolution_cr":r"Share in Central Taxes\s+\d[\d,]+\s+\d[\d,]+\s+\d[\d,]+\s+-?\d+%\s+([\d,]+)",
        "grants_from_centre_cr":    r"Grants-in-aid from Centre\s+\d[\d,]+\s+\d[\d,]+\s+\d[\d,]+\s+-?\d+%\s+([\d,]+)",
        "revenue_receipts_cr":      r"Revenue Receipts\s+\d[\d,]+\s+\d[\d,]+\s+\d[\d,]+\s+-?\d+%\s+([\d,]+)",
    }
    for key, pattern in patterns.items():
        m = re.search(pattern, text)
        if m:
            result[key] = _clean_num(m.group(1))

    # Devolution as % of revenue receipts — from narrative
    dev_pct_m = re.search(r"share in central taxes\s+\(([\d]+)%\s+of revenue receipts\)", text)
    if dev_pct_m:
        result["devolution_pct_revenue"] = float(dev_pct_m.group(1))

    return result


def parse_debt_frbm(pdf_path: Path) -> dict:
    """
    Page 5: Debt and FRBM targets — outstanding debt, debt/GSDP.
    """
    result = {}
    text = _extract_text_all(pdf_path)

    # Outstanding debt
    debt_m = re.search(
        r"outstanding (?:liabilities|debt).*?Rs\s+([\d,]+)\s+crore",
        text, re.IGNORECASE | re.DOTALL
    )
    if debt_m:
        result["outstanding_debt_cr"] = _clean_num(debt_m.group(1))

    # Debt to GSDP
    debt_gsdp_m = re.search(
        r"outstanding (?:liabilities|debt).*?([\d.]+)%\s+of GSDP",
        text, re.IGNORECASE | re.DOTALL
    )
    if debt_gsdp_m:
        result["debt_to_gsdp_pct"] = float(debt_gsdp_m.group(1))

    # TANGEDCO grants — important context for debt narrative
    tangedco_m = re.search(
        r"Rs\s+([\d,]+)\s+crore as grants to\s+Tamil Nadu Electricity",
        text, re.DOTALL
    )
    if tangedco_m:
        result["tangedco_grants_total_cr"] = _clean_num(tangedco_m.group(1))

    return result


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def _is_valid_pdf(path: Path) -> bool:
    """Quick check that the file is a valid PDF before parsing."""
    try:
        with open(path, "rb") as f:
            header = f.read(5)
        return header == b"%PDF-"
    except OSError:
        return False


def parse_prs_pdf(pdf_path: Path, year: str, source_url: str, checksum: str) -> dict:
    """
    Full parse of a single PRS TN Budget Analysis PDF.
    Returns a complete state_finances document ready for Firestore.
    """
    if not _is_valid_pdf(pdf_path):
        print(f"  [error] {pdf_path.name} is not a valid PDF — skipping")
        return None

    print(f"  [parse] {pdf_path.name}")

    try:
        key_figures = parse_table1_key_figures(pdf_path)
        committed   = parse_table3_committed_expenditure(pdf_path)
        sectors     = parse_table4_sector_expenditure(pdf_path)
        receipts    = parse_table5_receipts(pdf_path)
        debt_frbm   = parse_debt_frbm(pdf_path)
    except Exception as e:
        print(f"  [error] Parse failed for {pdf_path.name}: {type(e).__name__}: {e}")
        return None

    # Merge all parsed values
    summary = {**key_figures, **receipts}
    debt_context = {**debt_frbm, **{
        k: committed[k]
        for k in ("interest_payments_cr", "interest_pct_revenue_receipts", "committed_pct_revenue_receipts")
        if k in committed
    }}

    doc = {
        "fiscal_year": year,
        "budget_type": "Interim" if "interim" in pdf_path.name.lower() else "Full",
        "summary": {
            "gsdp_projected_cr":        summary.get("gsdp_projected_cr"),
            "gsdp_growth_pct":          summary.get("gsdp_growth_pct"),
            "net_expenditure_cr":       summary.get("net_expenditure_cr"),
            "net_receipts_cr":          summary.get("net_receipts_cr"),
            "total_borrowings_cr":      summary.get("total_borrowings_cr"),
            "debt_repayment_cr":        summary.get("debt_repayment_cr"),
            "fiscal_deficit_cr":        summary.get("fiscal_deficit_cr"),
            "fiscal_deficit_pct_gsdp":  summary.get("fiscal_deficit_pct_gsdp"),
            "revenue_deficit_cr":       summary.get("revenue_deficit_cr"),
            "revenue_deficit_pct_gsdp": summary.get("revenue_deficit_pct_gsdp"),
            "primary_deficit_cr":       summary.get("primary_deficit_cr"),
            "primary_deficit_pct_gsdp": summary.get("primary_deficit_pct_gsdp"),
        },
        "receipts": {
            "revenue_receipts_cr":           receipts.get("revenue_receipts_cr"),
            "sotr_cr":                        receipts.get("sotr_cr"),
            "own_non_tax_revenue_cr":         receipts.get("own_non_tax_revenue_cr"),
            "central_tax_devolution_cr":      receipts.get("central_tax_devolution_cr"),
            "grants_from_centre_cr":          receipts.get("grants_from_centre_cr"),
            "devolution_pct_revenue":         receipts.get("devolution_pct_revenue"),
        },
        "committed_expenditure": {
            "salaries_cr":                    committed.get("salaries_cr"),
            "pensions_cr":                    committed.get("pensions_cr"),
            "interest_payments_cr":           committed.get("interest_payments_cr"),
            "committed_total_cr":             committed.get("committed_total_cr"),
            "committed_pct_revenue_receipts": committed.get("committed_pct_revenue_receipts"),
            "interest_pct_revenue_receipts":  committed.get("interest_pct_revenue_receipts"),
        },
        "debt_context": {
            "outstanding_debt_cr":      debt_context.get("outstanding_debt_cr"),
            "debt_to_gsdp_pct":         debt_context.get("debt_to_gsdp_pct"),
            "interest_payments_cr":     debt_context.get("interest_payments_cr"),
            "interest_as_pct_revenue":  debt_context.get("interest_pct_revenue_receipts"),
            "tangedco_grants_total_cr": debt_context.get("tangedco_grants_total_cr"),
        },
        "sector_expenditure": sectors,
        "sources": [
            {
                "title": f"PRS Legislative Research — TN Budget Analysis {year}",
                "url": source_url,
                "pdf_checksum": checksum,
                "accessed_date": "2026-03-31",
            }
        ],
        "ground_truth_confidence": "HIGH",
    }

    return doc


def run_prs_scraper(years: list[str] = None) -> list[dict]:
    """
    Full pipeline: discover → download → parse → return list of finance docs.
    """
    if years is None:
        years = BUDGET_YEARS

    all_docs = []
    for year in years:
        pdf_path, checksum = download_pdf(year)
        if pdf_path is None:
            print(f"  [skip] {year} — no PDF available")
            continue

        source_url = f"{PRS_BASE}{PDF_PATH_MAP.get(year, '')}"
        doc = parse_prs_pdf(pdf_path, year, source_url, checksum)
        if doc is None:
            continue

        parsed_count = sum(
            1 for v in [
                doc["summary"].get("fiscal_deficit_cr"),
                doc["receipts"].get("sotr_cr"),
                doc["committed_expenditure"].get("interest_payments_cr"),
            ] if v is not None
        )
        print(f"  [ok] {year}: {parsed_count}/3 core fields parsed, {len(doc['sector_expenditure'])} sectors")
        all_docs.append(doc)

    return all_docs
