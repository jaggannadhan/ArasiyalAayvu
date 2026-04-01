# Implementation: Tamil Nadu State Finances & Revenue (2020–2027)

**Module:** State Finances
**GCP Project:** `naatunadappu` (asia-south1)
**Last updated:** 2026-03-31
**Status:** LIVE — 30 documents across 3 collections in Firestore

---

## Status Dashboard

| Collection | Docs | Source | Status |
|---|---|---|---|
| `state_finances` | 5 | PRS India PDFs (2021-22 → 2025-26) | **LIVE** |
| `debt_history` | 7 | PRS + curated (2020-21 → 2026-27) | **LIVE** |
| `departmental_spending` | 18 | PRS Table 4 sector-wise data | **LIVE** |

---

## Source Analysis

| Source | What We Extract | Confidence | SSL Status |
|---|---|---|---|
| PRS India — TN Budget Analysis PDFs | GSDP, deficits, committed expenditure, sector outlays, receipts breakdown | HIGH | **WORKING** — standard HTTPS, no issues |
| GoTN Finance Portal (`finance.tn.gov.in`) | Demands for Grants, scheme-level allocations | HIGH | **BLOCKED** — same server-side TLS drop as other TN govt domains |
| TN Interim Budget 2026-27 | Outstanding debt projection (₹10.71 lakh crore), FRBM targets | HIGH | Manual entry — curated in transformer |

**Strategic choice:** PRS India is the primary automated source. GoTN portal data is accessed via the Manual-Link Utility when PDFs are downloaded manually through a browser.

---

## Files Created

```
scrapers/
  prs_scraper.py              # Downloads + parses PRS India TN Budget Analysis PDFs
  tn_budget_scraper.py        # Manual-Link Utility for GoTN budget PDFs

transformers/
  finance_transformer.py      # Debt-Why mapping + viz metric computation + series builder

loaders/
  firestore_loader.py         # Extended with upload_state_finances, upload_debt_history,
                              #   upload_departmental_spending, upload_finance_manual

data/raw/prs/
  TN_Budget_Analysis_2025-26.pdf   (723 KB — parsed: 3/3 core fields, 3 sectors)
  TN_Budget_Analysis_2024-25.pdf   (457 KB — parsed: 2/3 core fields, 4 sectors)
  TN_Budget_Analysis_2023-24.pdf   (492 KB — parsed: 1/3 core fields, 3 sectors)
  TN_Budget_Analysis_2022-23.pdf   (500 KB — parsed: 1/3 core fields, 5 sectors)
  TN_Budget_Analysis_2021-22.pdf   (676 KB — parsed: 1/3 core fields, 3 sectors)
  TN_Budget_Analysis_2020-21.pdf   (592 KB — CORRUPT: pdfminer parse error, skipped)

data/processed/
  state_finances.json         (5 records — 2021-22 to 2025-26)
  debt_history.json           (7 records — 2020-21 to 2026-27)
  departmental_spending.json  (18 records — sector × year)
```

---

## Known Issues

### PRS 2020-21 PDF Corrupt
The 2020-21 PRS PDF downloads successfully (592 KB, starts with `%PDF-`) but pdfminer throws `PDFSyntaxError: No /Root object!` on deep parse. The file appears to have a malformed internal cross-reference table. The scraper catches this and skips it gracefully. The 2020-21 debt/interest figures are covered by the curated `debt_history` series.

**Workaround:** Download the 2020-21 PDF manually from PRS and use the manual-link utility:
```bash
.venv/bin/python main.py --task manual-pdf \
  --url "https://prsindia.org/budgets/states/tamil-nadu-budget-analysis-2020-21" \
  --year 2020-21 --upload
```

### Sector Parsing Coverage
The PRS PDF sector table parser uses regex on extracted text. Older PDFs (2021-22 to 2023-24) have slightly different text layouts, resulting in fewer matched sectors (1/3 core fields vs 3/3 for 2025-26). The `departmental_spending` collection compensates with curated sub-allocation data for the 6 key departments.

---

## Running the Finance Pipeline

```bash
# Full finance pipeline (download PDFs → parse → transform → upload)
GOOGLE_CLOUD_PROJECT=naatunadappu .venv/bin/python main.py --task finance

# Specific years only
GOOGLE_CLOUD_PROJECT=naatunadappu .venv/bin/python main.py --task finance \
  --years 2025-26 2024-25

# Process a manually downloaded GoTN PDF (Manual-Link Utility)
.venv/bin/python main.py --task manual-pdf \
  --pdf /path/to/tn_budget_at_a_glance_2025-26.pdf \
  --year 2025-26 --upload

# Or from a direct URL
.venv/bin/python main.py --task manual-pdf \
  --url "https://finance.tn.gov.in/path/to/budget.pdf" \
  --year 2025-26 --upload
```

---

## Firestore Schema

### Collection: `state_finances`
**Document ID:** `fiscal_year` (e.g. `"2025-26"`)
**Source:** PRS India Budget Analysis PDFs

```json
{
  "fiscal_year": "2025-26",
  "budget_type": "Full",
  "summary": {
    "gsdp_projected_cr": 3567818,
    "gsdp_growth_pct": 15.0,
    "net_expenditure_cr": 439293,
    "net_receipts_cr": 332325,
    "total_borrowings_cr": 152040,
    "debt_repayment_cr": 47040,
    "fiscal_deficit_cr": 106968,
    "fiscal_deficit_pct_gsdp": 3.0,
    "revenue_deficit_cr": 41635,
    "revenue_deficit_pct_gsdp": 1.2,
    "primary_deficit_cr": 37855,
    "primary_deficit_pct_gsdp": 1.1
  },
  "receipts": {
    "revenue_receipts_cr": 331569,
    "sotr_cr": 220895,
    "own_non_tax_revenue_cr": 28819,
    "central_tax_devolution_cr": 58022,
    "grants_from_centre_cr": 23834,
    "devolution_pct_revenue": 17
  },
  "committed_expenditure": {
    "salaries_cr": 91726,
    "pensions_cr": 46214,
    "interest_payments_cr": 69114,
    "committed_total_cr": 207054,
    "committed_pct_revenue_receipts": 62.0,
    "interest_pct_revenue_receipts": 21.0
  },
  "debt_context": {
    "tangedco_grants_total_cr": 83156
  },
  "sector_expenditure": [
    {
      "sector": "Transport",
      "sector_slug": "transport",
      "actuals_2023_24_cr": 20478,
      "be_2024_25_cr": 22012,
      "re_2024_25_cr": 21978,
      "be_2025_26_cr": 27971,
      "pct_change_re_to_be": 27.0
    }
  ],
  "debt_why": [
    {
      "category": "Committed Expenditure Growth",
      "label": "Salaries + pension + interest = 62% of revenue receipts",
      "amount_cr": 207054,
      "year_committed": "2025-26",
      "notes": "...",
      "source_url": "https://prsindia.org/..."
    }
  ],
  "viz_metrics": {
    "interest_as_pct_revenue": 20.84,
    "committed_as_pct_revenue": 62.45,
    "discretionary_as_pct_revenue": 37.55,
    "discretionary_revenue_cr": 124515.0,
    "own_resources_pct_revenue": 75.31,
    "central_dependency_pct_revenue": 24.69,
    "devolution_as_pct_revenue": 17.5,
    "fiscal_deficit_pct_gsdp": 3.0,
    "revenue_deficit_pct_gsdp": 1.2,
    "interest_as_pct_expenditure": 15.73
  },
  "sources": [
    {
      "title": "PRS Legislative Research — TN Budget Analysis 2025-26",
      "url": "https://prsindia.org/files/budget/...",
      "pdf_checksum": "45477a25c9f82959f25e182fdab357e3...",
      "accessed_date": "2026-03-31"
    }
  ],
  "ground_truth_confidence": "HIGH",
  "_uploaded_at": "2026-03-31T...",
  "_schema_version": "1.0"
}
```

---

### Collection: `debt_history`
**Document ID:** `fiscal_year`
**Source:** PRS multi-year analysis + TN Interim Budget 2026-27 (curated)

**Design note:** `outstanding_debt_cr` and `debt_to_gsdp_pct` are always sourced from the curated series — never overridden by the PDF parser. The PRS analysis PDFs do not consistently state the outstanding debt stock figure; they focus on flows (deficits, borrowings). Outstanding debt figures come from the TN Interim Budget and RBI State Finance reports.

```json
{
  "fiscal_year": "2025-26",
  "outstanding_debt_cr": 989000,
  "debt_to_gsdp_pct": 26.2,
  "revenue_receipts_cr": 331569,
  "interest_payments_cr": 69114,
  "interest_as_pct_revenue": 20.84,
  "fiscal_deficit_pct_gsdp": 3.0,
  "within_frbm_limits": true,
  "frbm_limit_pct": 3.0,
  "debt_why": [ ... ],
  "source_url": "https://prsindia.org/...",
  "ground_truth_confidence": "HIGH"
}
```

**FRBM Note:** `within_frbm_limits` checks whether fiscal deficit ≤ 3.0% of GSDP (TN FRBM Act 2003 target), not the outstanding debt stock. TN's debt/GSDP (~26%) exceeds the RBI advisory of 25% but is within the state's own FRBM fiscal deficit target.

**Full debt series (verified):**

| Year | Outstanding Debt | Debt/GSDP | Interest % Revenue | Fiscal Def % GSDP | FRBM OK |
|---|---|---|---|---|---|
| 2020-21 | ₹5.70 lakh cr | 27.4% | 26.7% | 4.36% | ✗ |
| 2021-22 | ₹6.42 lakh cr | 27.1% | 26.1% | 3.84% | ✗ |
| 2022-23 | ₹7.21 lakh cr | 26.8% | 23.2% | 3.3% | ✗ |
| 2023-24 | ₹8.06 lakh cr | 26.5% | 19.8% | 3.3% | ✗ |
| 2024-25 | ₹9.26 lakh cr | 26.3% | 20.2% | 3.3% | ✗ |
| 2025-26 | ₹9.89 lakh cr | 26.2% | 20.8% | 3.0% | ✓ |
| 2026-27 | ₹10.71 lakh cr | 26.12% | — | — | — |

---

### Collection: `departmental_spending`
**Document ID:** `{year}_{dept_slug}` (e.g. `"2025-26_transport"`)
**Source:** PRS Table 4 (sector-wise) + curated sub-allocations

| Field | Notes |
|---|---|
| `fiscal_year` | |
| `department` | Full name from PRS |
| `department_slug` | `education`, `health`, `agriculture`, `transport`, `social_welfare`, `energy`, `police` |
| `allocation_cr` | BE 2025-26 |
| `actuals_prior_year_cr` | 2023-24 actuals |
| `revised_estimate_cr` | RE 2024-25 |
| `pct_change_from_re` | % change from RE 2024-25 to BE 2025-26 |
| `sub_allocations` | `[{label, amount_cr}]` — key scheme breakdowns |

**Key 2025-26 numbers (directly from PRS PDF):**

| Department | BE 2025-26 | vs RE 2024-25 |
|---|---|---|
| Transport | ₹27,971 cr | +27% |
| Energy | ₹20,354 cr | -11% |
| Police | ₹12,714 cr | +12% |

Note: Education, Health, Social Welfare, Agriculture not matched by regex in all PDFs — covered by curated `sub_allocations` in the transformer.

---

## Debt-Why Mapping

The `debt_why` array in both `state_finances` and `debt_history` provides structured reasons behind TN's debt trajectory. Each entry contains:

```json
{
  "category": "PSU Liabilities | Infrastructure | Central Devolution Gap | Revenue Deficit Financing | Energy Sector | Committed Expenditure Growth | Debt Projection",
  "label": "Human-readable label for the app UI",
  "amount_cr": 83156,
  "year_committed": "2019-20",
  "notes": "Extended context for the 'Learn More' panel",
  "source_url": "https://prsindia.org/..."
}
```

**Curated entries cover:**
- TANGEDCO accumulated loss grants (2019-26): ₹83,156 crore
- Chennai Metro Rail Phase-II state share: ₹63,246 crore
- Central devolution gap (7.9% → 4.079%): >₹3 lakh crore cumulative
- COVID-19 relief spending: ₹12,000 crore
- Energy capital outlay spike (3,973% YoY): ₹5,068 crore
- Committed expenditure consuming 62% of revenue receipts
- Outstanding debt projection to ₹10.71 lakh crore by 2026-27

---

## Visualization Metrics (Pre-computed)

All `viz_metrics` fields in `state_finances` are ready for direct chart consumption:

| Metric | 2025-26 Value | Purpose |
|---|---|---|
| `interest_as_pct_revenue` | **20.84%** | "Debt interest eats ₹1 in every ₹5 earned" — primary awareness metric |
| `committed_as_pct_revenue` | **62.45%** | "Only 37 paise of every rupee is discretionary" |
| `discretionary_as_pct_revenue` | **37.55%** | Remaining fiscal space |
| `devolution_as_pct_revenue` | **17.5%** | Central dependency |
| `own_resources_pct_revenue` | **75.31%** | State self-reliance |
| `central_dependency_pct_revenue` | **24.69%** | Centre + grants |
| `fiscal_deficit_pct_gsdp` | **3.0%** | FRBM target met in 2025-26 |
| `revenue_deficit_pct_gsdp` | **1.2%** | Down from 3.49% in 2020-21 |

---

## PRS PDF Discovery Map

All PDF URLs confirmed working as of 2026-03-31:

| Year | PDF URL | Size | Parse Quality |
|---|---|---|---|
| 2025-26 | `.../2025/TN_Budget_Analysis_2025-26.pdf` | 723 KB | 3/3 core fields, 3 sectors |
| 2024-25 | `.../2024/Tamil_Nadu_Budget_Analysis_2024-25.pdf` | 457 KB | 2/3 core fields, 4 sectors |
| 2023-24 | `.../2023/TN_Budget_Analysis_2023-24.pdf` | 492 KB | 1/3 core fields, 3 sectors |
| 2022-23 | `.../2022/Tamil%20Nadu%20Budget%20Analysis%202022-23.pdf` | 500 KB | 1/3 core fields, 5 sectors |
| 2021-22 | `.../2021/Tamil%20Nadu%20Budget%20Analysis%202021-22.pdf` | 676 KB | 1/3 core fields, 3 sectors |
| 2020-21 | `.../2020/State%20Budget%20Analysis%20-%20TN%202020-21.pdf` | 592 KB | ✗ Corrupt — pdfminer parse error |

All hosted at `https://prsindia.org/files/budget/budget_state/tamil-nadu/`

---

## Manual-Link Utility Reference

When the GoTN finance portal TLS is fixed (or when downloading PDFs manually via browser), use:

```bash
# Step 1: Download the PDF in your browser, save locally

# Step 2: Process and upload
python main.py --task manual-pdf \
  --pdf ~/Downloads/tn_budget_2025_26.pdf \
  --year 2025-26 \
  --upload

# The script:
# 1. Validates the file is a real PDF
# 2. Parses it with pdfplumber (departmental allocations + Demands for Grants tables)
# 3. Outputs JSON to stdout for inspection
# 4. If --upload: writes to Firestore state_finances/{year} (merge=True)
```
