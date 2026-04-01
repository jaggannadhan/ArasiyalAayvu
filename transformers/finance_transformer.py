"""
Finance Transformer
Cleans raw PRS scraper output, injects the curated debt_why mapping,
and computes all visualization metrics consumed by the web app.
"""

import json
from pathlib import Path

# ---------------------------------------------------------------------------
# Debt-Why Mapping — structured reasons behind TN's debt increases
# Sources: PRS India, TN White Paper (2021), TANGEDCO reports, GoTN press releases
# ---------------------------------------------------------------------------

DEBT_WHY_MAP: dict[str, list[dict]] = {
    "2021-22": [
        {
            "category": "PSU Liabilities",
            "label": "TANGEDCO accumulated losses absorption",
            "amount_cr": 47000,
            "year_committed": "2021-22",
            "notes": (
                "State absorbed TANGEDCO losses as UDAY grants. "
                "Per unit supply cost gap vs revenue: Rs 0.89/kWh vs state average Rs 0.47/kWh. "
                "TN White Paper (2021) called for full restructuring."
            ),
            "source_url": "https://prsindia.org/budgets/states/tamil-nadu-budget-analysis-2021-22",
        },
        {
            "category": "Central Devolution Gap",
            "label": "TN share of central taxes: 7.9% → 4.023% (14th FC)",
            "amount_cr": 50000,
            "year_committed": "2015-16",
            "notes": (
                "14th Finance Commission reduced TN's share of divisible pool from 4.969% to 4.023%. "
                "Loss compounded over 5 years (2015-20), forcing state to borrow to maintain expenditure."
            ),
            "source_url": "https://prsindia.org/budgets/states/tamil-nadu-budget-analysis-2021-22",
        },
        {
            "category": "COVID-19 Relief",
            "label": "COVID-19 relief and health expenditure",
            "amount_cr": 12000,
            "year_committed": "2020-21",
            "notes": "Emergency health and welfare spending during pandemic; no central compensation for full amount.",
            "source_url": "https://cms.tn.gov.in",
        },
    ],
    "2022-23": [
        {
            "category": "Infrastructure",
            "label": "Chennai Metro Rail Phase-II — state share",
            "amount_cr": 63246,
            "year_committed": "2022-23",
            "notes": (
                "CMRL Phase II approved at total project cost of ~Rs 63,246 crore. "
                "State provides equity and guarantees. Loan component from JICA and central govt."
            ),
            "source_url": "https://prsindia.org/budgets/states/tamil-nadu-budget-analysis-2022-23",
        },
        {
            "category": "PSU Liabilities",
            "label": "TANGEDCO ongoing grants",
            "amount_cr": 15000,
            "year_committed": "2022-23",
            "notes": "Annual grants to bridge TANGEDCO revenue gap. Cumulative grants 2019-26 estimated at Rs 83,156 crore.",
            "source_url": "https://prsindia.org/budgets/states/tamil-nadu-budget-analysis-2022-23",
        },
        {
            "category": "Revenue Deficit Financing",
            "label": "Revenue deficit borrowing (1.7% of GSDP)",
            "amount_cr": 45121,
            "year_committed": "2023-24",
            "notes": "State borrows to cover revenue expenditure that exceeds revenue receipts — structurally driven by committed expenditure growth.",
            "source_url": "https://prsindia.org/budgets/states/tamil-nadu-budget-analysis-2023-24",
        },
    ],
    "2023-24": [
        {
            "category": "Infrastructure",
            "label": "Roads and bridges capital outlay spike",
            "amount_cr": 18456,
            "year_committed": "2025-26",
            "notes": "Rs 18,456 crore allocated for capital outlay on roads in 2025-26 BE — part of multi-year highway programme.",
            "source_url": "https://prsindia.org/budgets/states/tamil-nadu-budget-analysis-2025-26",
        },
        {
            "category": "Energy Sector",
            "label": "Energy capital outlay — 3,973% increase",
            "amount_cr": 5068,
            "year_committed": "2025-26",
            "notes": (
                "Energy sector capital outlay jumped from Rs 124 crore (RE 2024-25) to "
                "Rs 5,068 crore (BE 2025-26) — a 3,973% increase. "
                "Represents state investment in power corporation infrastructure."
            ),
            "source_url": "https://prsindia.org/budgets/states/tamil-nadu-budget-analysis-2025-26",
        },
    ],
    "2024-25": [
        {
            "category": "Central Devolution Gap",
            "label": "TN share of central taxes: 4.079% (15th FC) vs historical 7.9%",
            "amount_cr": 300000,
            "year_committed": "2015-16",
            "notes": (
                "Cumulative loss estimated at over Rs 3 lakh crore (2015-2025). "
                "TN's share declined through 13th FC (4.969%), 14th FC (4.023%), 15th FC (4.079%). "
                "At original 7.9% share, TN would have received ~Rs 3 lakh crore more in central transfers."
            ),
            "source_url": "https://prsindia.org/budgets/states/tamil-nadu-budget-analysis-2024-25",
        },
        {
            "category": "PSU Liabilities",
            "label": "TANGEDCO cumulative grants 2019-2025",
            "amount_cr": 83156,
            "year_committed": "2019-20",
            "notes": "Total TANGEDCO grants from state between 2019-20 and 2025-26 estimated at Rs 83,156 crore.",
            "source_url": "https://prsindia.org/budgets/states/tamil-nadu-budget-analysis-2025-26",
        },
    ],
    "2025-26": [
        {
            "category": "Committed Expenditure Growth",
            "label": "Salaries + pension + interest = 62% of revenue receipts",
            "amount_cr": 207054,
            "year_committed": "2025-26",
            "notes": (
                "Committed expenditure (Rs 2,07,054 crore) consumes 62% of estimated revenue receipts "
                "(Rs 3,31,569 crore). Salaries: 28%, Pension: 14%, Interest: 21%. "
                "This structural rigidity limits capital investment flexibility."
            ),
            "source_url": "https://prsindia.org/budgets/states/tamil-nadu-budget-analysis-2025-26",
        },
        {
            "category": "Debt Projection",
            "label": "Outstanding debt projected at Rs 10.71 lakh crore by 2026-27",
            "amount_cr": 1071000,
            "year_committed": "2026-27",
            "notes": (
                "Outstanding liabilities projected to reach Rs 10.71 lakh crore (26.12% of GSDP) "
                "in 2026-27 as per TN Interim Budget. Remains within FRBM ceiling. "
                "Net borrowings for 2025-26: Rs 1,05,000 crore."
            ),
            "source_url": "https://finance.tn.gov.in",
        },
    ],
}


# ---------------------------------------------------------------------------
# Visualization Metric Calculator
# ---------------------------------------------------------------------------

def compute_viz_metrics(doc: dict) -> dict:
    """
    Derives all front-end chart data points from a state_finances document.
    Called after PRS parsing to enrich the doc before Firestore upload.
    """
    s = doc.get("summary", {})
    r = doc.get("receipts", {})
    c = doc.get("committed_expenditure", {})
    d = doc.get("debt_context", {})

    rev = r.get("revenue_receipts_cr") or 0
    interest = c.get("interest_payments_cr") or 0
    committed = c.get("committed_total_cr") or 0
    sotr = r.get("sotr_cr") or 0
    devolution = r.get("central_tax_devolution_cr") or 0
    grants = r.get("grants_from_centre_cr") or 0
    gsdp = s.get("gsdp_projected_cr") or 0
    debt = d.get("outstanding_debt_cr") or 0
    fiscal_def = s.get("fiscal_deficit_cr") or 0
    expenditure = s.get("net_expenditure_cr") or 0

    viz = {}

    if rev > 0:
        # Interest burden — PRIMARY AWARENESS METRIC
        viz["interest_as_pct_revenue"] = round((interest / rev) * 100, 2)
        # Committed expenditure squeeze
        viz["committed_as_pct_revenue"] = round((committed / rev) * 100, 2)
        # Revenue available for discretionary spend
        viz["discretionary_revenue_cr"] = round(rev - committed, 2)
        viz["discretionary_as_pct_revenue"] = round(((rev - committed) / rev) * 100, 2)
        # Own resources vs centre dependency
        viz["own_resources_pct_revenue"] = round(((sotr + r.get("own_non_tax_revenue_cr", 0)) / rev) * 100, 2)
        viz["central_dependency_pct_revenue"] = round(((devolution + grants) / rev) * 100, 2)
        viz["devolution_as_pct_revenue"] = round((devolution / rev) * 100, 2)

    if gsdp > 0:
        # debt_to_gsdp comes from debt_history curated series, not computed here
        viz["fiscal_deficit_pct_gsdp"] = s.get("fiscal_deficit_pct_gsdp")
        viz["revenue_deficit_pct_gsdp"] = s.get("revenue_deficit_pct_gsdp")

    if expenditure > 0:
        viz["interest_as_pct_expenditure"] = round((interest / expenditure) * 100, 2)

    # Devolution gap context (hardcoded known fact — for display on app)
    viz["devolution_gap_note"] = (
        "TN's share of central taxes declined from ~7.9% (pre-2010) to 4.079% (15th Finance Commission). "
        "Estimated cumulative loss: >Rs 3 lakh crore (2015-2025)."
    )

    return viz


# ---------------------------------------------------------------------------
# Main transform function
# ---------------------------------------------------------------------------

def transform_prs_docs(raw_docs: list[dict]) -> list[dict]:
    """
    Takes raw PRS scraper output, injects debt_why and viz_metrics,
    returns final list of state_finances documents ready for Firestore.
    """
    transformed = []
    for doc in raw_docs:
        year = doc["fiscal_year"]

        # Inject debt_why mapping for this year (fall back to empty list)
        doc["debt_why"] = DEBT_WHY_MAP.get(year, [])

        # Compute visualization metrics
        doc["viz_metrics"] = compute_viz_metrics(doc)

        transformed.append(doc)

    return transformed


def build_debt_history_series(raw_docs: list[dict]) -> list[dict]:
    """
    Builds the debt_history collection documents — one per fiscal year.
    Designed to show the multi-year trend for the "Debt Story" visualization.
    """
    # Curated trend data — from PRS India multi-year reports and TN Budget statements
    CURATED_DEBT_SERIES = [
        {"fiscal_year": "2020-21", "outstanding_debt_cr": 570000, "debt_to_gsdp_pct": 27.4,  "revenue_receipts_cr": 180541, "interest_payments_cr": 48263, "fiscal_deficit_pct_gsdp": 4.36},
        {"fiscal_year": "2021-22", "outstanding_debt_cr": 642000, "debt_to_gsdp_pct": 27.1,  "revenue_receipts_cr": 205813, "interest_payments_cr": 52940, "fiscal_deficit_pct_gsdp": 3.84},
        {"fiscal_year": "2022-23", "outstanding_debt_cr": 721000, "debt_to_gsdp_pct": 26.8,  "revenue_receipts_cr": 238416, "interest_payments_cr": 53566, "fiscal_deficit_pct_gsdp": 3.3},
        {"fiscal_year": "2023-24", "outstanding_debt_cr": 806000, "debt_to_gsdp_pct": 26.5,  "revenue_receipts_cr": 264597, "interest_payments_cr": 53566, "fiscal_deficit_pct_gsdp": 3.3},
        {"fiscal_year": "2024-25", "outstanding_debt_cr": 926000, "debt_to_gsdp_pct": 26.3,  "revenue_receipts_cr": 293906, "interest_payments_cr": 60357, "fiscal_deficit_pct_gsdp": 3.3},
        {"fiscal_year": "2025-26", "outstanding_debt_cr": 989000, "debt_to_gsdp_pct": 26.2,  "revenue_receipts_cr": 331569, "interest_payments_cr": 69114, "fiscal_deficit_pct_gsdp": 3.0},
        {"fiscal_year": "2026-27", "outstanding_debt_cr": 1071000, "debt_to_gsdp_pct": 26.12, "revenue_receipts_cr": None,   "interest_payments_cr": None,  "fiscal_deficit_pct_gsdp": None},
    ]

    # Merge any PRS-parsed values over the curated baseline
    parsed_by_year = {d["fiscal_year"]: d for d in raw_docs}

    result = []
    for row in CURATED_DEBT_SERIES:
        year = row["fiscal_year"]
        parsed = parsed_by_year.get(year, {})

        # Outstanding debt and debt/GSDP: use curated values ONLY.
        # PRS PDFs don't consistently state outstanding debt — the parser
        # can misidentify TANGEDCO grants or other large figures as debt.
        # Source: TN Interim Budget 2026-27 + PRS multi-year tables.
        debt = row["outstanding_debt_cr"]
        debt_gsdp = row["debt_to_gsdp_pct"]

        # Revenue receipts and interest: prefer parsed (directly extracted from Table 1/3)
        rev = parsed.get("receipts", {}).get("revenue_receipts_cr") or row["revenue_receipts_cr"]
        interest = parsed.get("committed_expenditure", {}).get("interest_payments_cr") or row["interest_payments_cr"]

        interest_pct = round((interest / rev) * 100, 2) if (interest and rev) else None
        fiscal_def_pct = parsed.get("summary", {}).get("fiscal_deficit_pct_gsdp") or row["fiscal_deficit_pct_gsdp"]
        # TN FRBM Act 2003: fiscal deficit must be ≤3% of GSDP (not debt stock)
        frbm_ok = (fiscal_def_pct <= 3.0) if fiscal_def_pct else None

        result.append({
            "fiscal_year": year,
            "outstanding_debt_cr": debt,
            "debt_to_gsdp_pct": debt_gsdp,
            "revenue_receipts_cr": rev,
            "interest_payments_cr": interest,
            "interest_as_pct_revenue": interest_pct,
            "fiscal_deficit_pct_gsdp": parsed.get("summary", {}).get("fiscal_deficit_pct_gsdp") or row["fiscal_deficit_pct_gsdp"],
            "within_frbm_limits": frbm_ok,
            "frbm_limit_pct": 25.0,
            "debt_why": DEBT_WHY_MAP.get(year, []),
            "source_url": "https://prsindia.org/budgets/states/tamil-nadu-budget-analysis-" + year,
            "ground_truth_confidence": "HIGH" if parsed else "MEDIUM",
        })

    return result


def build_departmental_spending(raw_docs: list[dict]) -> list[dict]:
    """
    Builds departmental_spending collection — one doc per (year, department).
    Primary source: PRS Table 4 sector-wise data. Enriched with sub-allocations.
    """
    records = []

    SUB_ALLOCATIONS = {
        "education": [
            {"label": "Government secondary schools", "amount_cr": 13974},
            {"label": "Government primary schools", "amount_cr": 14328},
            {"label": "Higher education (colleges + universities)", "amount_cr": 8200},
            {"label": "CM Breakfast scheme", "amount_cr": 600},
        ],
        "health": [
            {"label": "Primary Health Centres + CHCs", "amount_cr": 5200},
            {"label": "Government hospitals (tertiary)", "amount_cr": 7800},
            {"label": "National Health Mission (NHM)", "amount_cr": 3100},
            {"label": "Medical education", "amount_cr": 2900},
        ],
        "agriculture": [
            {"label": "Crop insurance (PMFBY)", "amount_cr": 900},
            {"label": "Agricultural input subsidies", "amount_cr": 2100},
            {"label": "Irrigation and water management", "amount_cr": 3400},
            {"label": "Horticulture and allied", "amount_cr": 1200},
        ],
        "transport": [
            {"label": "Capital outlay on roads and bridges", "amount_cr": 18456},
            {"label": "Chennai Metro Rail (state contribution)", "amount_cr": 3200},
            {"label": "TNSTC bus services", "amount_cr": 1800},
        ],
        "social_welfare": [
            {"label": "Magalir Urimai Thogai (women welfare scheme)", "amount_cr": 13807},
            {"label": "Scheduled Caste welfare", "amount_cr": 5200},
            {"label": "Scheduled Tribe welfare", "amount_cr": 1400},
            {"label": "Differently-abled welfare", "amount_cr": 900},
        ],
        "energy": [
            {"label": "TANGEDCO grants (loss subsidy)", "amount_cr": 3100},
            {"label": "Energy capital outlay (power infrastructure)", "amount_cr": 5068},
            {"label": "Solar energy promotion", "amount_cr": 400},
        ],
    }

    for doc in raw_docs:
        year = doc["fiscal_year"]
        sectors = doc.get("sector_expenditure", [])
        total_sector_spend = sum(s.get("be_2025_26_cr", 0) or 0 for s in sectors)

        for sector in sectors:
            slug = sector.get("sector_slug", "")
            alloc = sector.get("be_2025_26_cr")
            if alloc is None:
                continue

            pct_total = round((alloc / total_sector_spend) * 100, 2) if total_sector_spend > 0 else None
            prev_re = sector.get("re_2024_25_cr")
            pct_change = round(((alloc - prev_re) / prev_re) * 100, 2) if prev_re else None

            records.append({
                "doc_id": f"{year}_{slug}",
                "fiscal_year": year,
                "department": sector.get("sector"),
                "department_slug": slug,
                "allocation_cr": alloc,
                "actuals_prior_year_cr": sector.get("actuals_2023_24_cr"),
                "revised_estimate_cr": prev_re,
                "pct_of_sector_budget": pct_total,
                "pct_change_from_re": pct_change,
                "sub_allocations": SUB_ALLOCATIONS.get(slug, []),
                "source_url": f"https://prsindia.org/budgets/states/tamil-nadu-budget-analysis-{year}",
                "ground_truth_confidence": "HIGH",
            })

    return records


def save_processed(data, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  [saved] {path}")
