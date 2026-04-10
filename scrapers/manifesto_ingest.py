"""
Manifesto Ingest — Tamil Nadu Election Manifestos
==================================================
Downloads party manifesto PDFs (OpenCity / party sites), extracts structured
promises using Claude (OCR + LLM for Tamil PDFs), and writes to Firestore
`manifesto_promises` collection.

Sources
-------
  2026 TN — OpenCity / ECI  (DMK, AIADMK, NTK — Tamil PDFs)
  2024 National — OpenCity   (BJP, INC, CPI(M) — English PDFs, LOW confidence)

Requirements
------------
  pip install anthropic pdfplumber requests google-cloud-firestore python-dotenv

Env vars
--------
  ANTHROPIC_API_KEY   — required
  GOOGLE_CLOUD_PROJECT — required for --upload (default: naatunadappu)

Usage
-----
  python scrapers/manifesto_ingest.py --year 2026 --dry-run
  python scrapers/manifesto_ingest.py --year 2026 --party dmk --dry-run
  python scrapers/manifesto_ingest.py --year 2026 --party dmk
  python scrapers/manifesto_ingest.py --year 2026
  python scrapers/manifesto_ingest.py --year 2021 --party bjp
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
import time
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests

ROOT_DIR = Path(__file__).resolve().parents[1]
OUT_DIR  = ROOT_DIR / "data" / "processed"
NUMBEO_CACHE_DIR      = ROOT_DIR / "data" / "raw" / "numbeo"
NUMBEO_CACHE_TTL_DAYS = 90

# ---------------------------------------------------------------------------
# Source catalogue
# ---------------------------------------------------------------------------
# Each entry: (party_id, target_year) → config dict
SOURCE_CONFIG: dict[tuple[str, int], dict[str, Any]] = {
    # ── 2026 TN manifestos (OpenCity / ECI) ─────────────────────────────────
    ("dmk", 2026): {
        "party_name":  "DMK",
        "party_color": "bg-red-600",
        "pdf_url":     "https://data.opencity.in/dataset/ee83bf58-b563-4e90-86fb-0d8c1330adbd/resource/d56e2869-5d15-4760-b150-e5caaeab1422/download/dmk-manifesto-document.pdf",
        "lang":        "ta",
        "confidence":  "MEDIUM",
        "is_joint":    False,
        "track_fulfillment": True,
    },
    ("aiadmk", 2026): {
        "party_name":  "AIADMK",
        "party_color": "bg-green-700",
        "pdf_url":     "https://data.opencity.in/dataset/ee83bf58-b563-4e90-86fb-0d8c1330adbd/resource/5b4a3445-72f1-4e16-9047-5f39c57ee948/download/aiadmk-election-manifesto-assembly-general-election-2026.pdf",
        "lang":        "ta",
        "confidence":  "MEDIUM",
        "is_joint":    False,
        "track_fulfillment": True,
    },
    ("ntk", 2026): {
        "party_name":  "NTK",
        "party_color": "bg-yellow-600",
        "pdf_url":     "https://data.opencity.in/dataset/ee83bf58-b563-4e90-86fb-0d8c1330adbd/resource/2d5dd958-6932-40a0-8e11-453ea389cc58/download/ntk-2026-manifesto_compressed.pdf",
        "lang":        "ta",
        "confidence":  "MEDIUM",
        "is_joint":    False,
        "track_fulfillment": True,
    },
    # ── 2024 National manifestos (proxy for 2021 TN opposition reference) ───
    # These are national manifestos — not TN-specific.
    # Used only as opposition reference; fulfillment tracking disabled.
    ("bjp", 2024): {
        "party_name":  "BJP",
        "party_color": "bg-orange-500",
        "pdf_url":     "https://data.opencity.in/dataset/76e54184-f294-44e4-a40c-8594ccb410c8/resource/6210fb78-c1c3-4700-a61f-ed01daee9aff/download/7377fce3-f32d-4dba-8d1c-4969c25a3add.pdf",
        "lang":        "en",
        "confidence":  "LOW",
        "is_joint":    False,
        "track_fulfillment": False,
        "source_note": "National manifesto (Lok Sabha 2024) — not TN-specific",
    },
    ("inc", 2024): {
        "party_name":  "INC",
        "party_color": "bg-blue-600",
        "pdf_url":     "https://data.opencity.in/dataset/76e54184-f294-44e4-a40c-8594ccb410c8/resource/e2a62a20-74a6-472e-ab7e-79f610235893/download/8a16787a-0134-4ac4-9fde-d17506675642.pdf",
        "lang":        "en",
        "confidence":  "LOW",
        "is_joint":    False,
        "track_fulfillment": False,
        "source_note": "National manifesto (Lok Sabha 2024) — not TN-specific",
    },
    ("cpim", 2024): {
        "party_name":  "CPI(M)",
        "party_color": "bg-rose-700",
        "pdf_url":     "https://data.opencity.in/dataset/76e54184-f294-44e4-a40c-8594ccb410c8/resource/c04b71e0-5941-4684-8b7e-c58cd713081d/download/2bc55e52-18f0-42fa-b228-56e85ec55de5.pdf",
        "lang":        "en",
        "confidence":  "LOW",
        "is_joint":    False,
        "track_fulfillment": False,
        "source_note": "National manifesto (Lok Sabha 2024) — not TN-specific",
    },
}

# Category short codes for doc_id generation
_CAT_ABBREV: dict[str, str] = {
    "Agriculture":       "agri",
    "Education":         "edu",
    "TASMAC & Revenue":  "tasmac",
    "Women's Welfare":   "women",
    "Infrastructure":    "infra",
}

VALID_CATEGORIES = set(_CAT_ABBREV.keys())
VALID_STANCES = {
    "Welfare-centric", "Infrastructure-heavy", "Revenue-focused",
    "Populist", "Reform-oriented", "Women-focused", "Farmer-focused",
}

PAGES_PER_CHUNK = 5   # pages sent to Claude per API call

# ---------------------------------------------------------------------------
# PDF download + text extraction
# ---------------------------------------------------------------------------

def download_pdf(url: str, dest: Path) -> None:
    headers = {"User-Agent": "ArasiyalAayvuResearchBot/1.0"}
    r = requests.get(url, headers=headers, timeout=60, stream=True)
    r.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in r.iter_content(chunk_size=65536):
            f.write(chunk)
    print(f"  Downloaded {dest.stat().st_size // 1024} KB")


def extract_pages(pdf_path: Path) -> list[tuple[int, str]]:
    """Return list of (page_number, text) for all pages."""
    import pdfplumber

    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        print(f"  {total} pages in PDF")
        for i, page in enumerate(pdf.pages, start=1):
            text = (page.extract_text() or "").strip()
            if text:
                pages.append((i, text))
    return pages


# ---------------------------------------------------------------------------
# Cost-of-living data (Numbeo, 90-day TTL cache)
# ---------------------------------------------------------------------------

_NUMBEO_LABELS: dict[str, str] = {
    "meal_inexpensive_inr":       "Meal, Inexpensive Restaurant",
    "avg_monthly_net_salary_inr": "Average Monthly Net Salary",
    "basic_utilities_inr":        "Basic (Electricity, Heating",
    "rent_1br_city_center_inr":   "Apartment (1 bedroom) in City Centre",
    "transport_monthly_pass_inr": "Monthly Pass (Regular)",
    "tomatoes_1kg_inr":           "Tomatoes (1",
    "eggs_12_inr":                "Eggs (regular)",
    "milk_1liter_inr":            "Milk (regular)",
}


def _extract_numbeo_value(html: str, label_fragment: str) -> float | None:
    """Find a Numbeo price row by partial label text, return float INR or None."""
    idx = html.find(label_fragment)
    if idx < 0:
        return None
    snippet = html[idx : idx + 500]
    m = re.search(r"<span[^>]*>([\d,]+\.?\d*)</span>", snippet)
    if m:
        try:
            return float(m.group(1).replace(",", ""))
        except ValueError:
            return None
    return None


def _fetch_numbeo_all_cities() -> dict:
    """Fetch cost-of-living snapshots from Numbeo for key TN cities."""
    cities = ["Chennai", "Coimbatore", "Madurai"]
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }
    result: dict[str, Any] = {"fetched_on": str(date.today()), "cities": {}}
    for city in cities:
        url = f"https://www.numbeo.com/cost-of-living/in/{city}"
        try:
            r = requests.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            html = r.text
            city_data: dict[str, Any] = {}
            for key, label in _NUMBEO_LABELS.items():
                city_data[key] = _extract_numbeo_value(html, label)
            result["cities"][city] = city_data
            sal = city_data.get("avg_monthly_net_salary_inr")
            print(f"  [Numbeo] {city}: avg_salary=₹{sal:,.0f}" if sal else f"  [Numbeo] {city}: salary N/A")
        except Exception as e:
            print(f"  [Numbeo] WARNING: Could not fetch {city}: {e}")
            result["cities"][city] = {}
        time.sleep(1)  # polite crawl
    return result


def get_col_data() -> dict:
    """Return CoL data from 90-day TTL cache, refreshing from Numbeo when stale.

    Old snapshots are NEVER overwritten — they accumulate as dated files in
    NUMBEO_CACHE_DIR, enabling future inflation-trend comparisons.
    """
    NUMBEO_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_files = sorted(NUMBEO_CACHE_DIR.glob("col_tn_*.json"), reverse=True)
    if cache_files:
        latest = cache_files[0]
        try:
            cached_date = date.fromisoformat(latest.stem.split("_")[-1])
            age_days = (date.today() - cached_date).days
            if age_days < NUMBEO_CACHE_TTL_DAYS:
                print(f"  [CoL] Using cached snapshot from {cached_date} ({age_days}d old)")
                with open(latest) as f:
                    return json.load(f)
        except (ValueError, json.JSONDecodeError):
            pass  # corrupt cache — fall through to fresh fetch
    print("  [CoL] Fetching fresh Numbeo data…")
    data = _fetch_numbeo_all_cities()
    snapshot_path = NUMBEO_CACHE_DIR / f"col_tn_{date.today().isoformat()}.json"
    if not snapshot_path.exists():  # never overwrite — accumulate for inflation tracking
        with open(snapshot_path, "w") as f:
            json.dump(data, f, indent=2)
    return data


def _load_latest_tn_budget() -> dict | None:
    """Read the latest TN state budget record from processed JSON cache."""
    candidates = [
        OUT_DIR / "state_budgets_TN.json",
        OUT_DIR / "state_budgets.json",
    ]
    for path in candidates:
        if path.exists():
            try:
                with open(path) as f:
                    records = json.load(f)
                tn = [r for r in records if r.get("state_code") == "TN"]
                if tn:
                    tn.sort(key=lambda r: r.get("fiscal_year", ""), reverse=True)
                    return tn[0]
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
    return None


# ---------------------------------------------------------------------------
# Claude extraction
# ---------------------------------------------------------------------------

VALID_BENEFICIARY_COVERAGE = {"universal", "broad_majority", "targeted_poor", "specific_group"}
VALID_IMPACT_DEPTH         = {"transformative", "substantive", "supplemental", "symbolic"}
VALID_FISCAL_VIABILITY     = {"feasible", "stressed", "central_dependent", "uncosted"}
VALID_IMPLEMENTATION_RISK  = {"low", "medium", "high"}


def build_system_prompt() -> str:
    """Assemble the LLM system prompt with live CoL data and latest TN budget context."""

    # ── 1. Cost of living ────────────────────────────────────────────────────
    col = get_col_data()
    cities = col.get("cities", {})
    col_date = col.get("fetched_on", "unknown")

    def _fmt_city(city_name: str) -> str:
        d = cities.get(city_name, {})
        lines = []
        if d.get("avg_monthly_net_salary_inr"):
            lines.append(f"  Avg monthly net salary:                    ₹{d['avg_monthly_net_salary_inr']:>9,.0f}")
        if d.get("meal_inexpensive_inr"):
            lines.append(f"  Inexpensive restaurant meal:               ₹{d['meal_inexpensive_inr']:>9,.0f}")
        if d.get("basic_utilities_inr"):
            lines.append(f"  Monthly basic utilities (elec+water+GC):   ₹{d['basic_utilities_inr']:>9,.0f}")
        if d.get("rent_1br_city_center_inr"):
            lines.append(f"  1BR rent (city centre):                    ₹{d['rent_1br_city_center_inr']:>9,.0f}/mo")
        if d.get("transport_monthly_pass_inr"):
            lines.append(f"  Monthly transport pass:                    ₹{d['transport_monthly_pass_inr']:>9,.0f}")
        if d.get("eggs_12_inr"):
            lines.append(f"  Eggs (12):                                 ₹{d['eggs_12_inr']:>9,.0f}")
        if d.get("milk_1liter_inr"):
            lines.append(f"  Milk (1 litre):                            ₹{d['milk_1liter_inr']:>9,.0f}")
        return "\n".join(lines) if lines else "  (data unavailable)"

    # ── 2. TN state budget ────────────────────────────────────────────────────
    budget = _load_latest_tn_budget()
    if budget:
        fy   = budget.get("fiscal_year", "2024-25")
        rev  = budget.get("revenue", {})
        exp  = budget.get("expenditure", {})
        com  = budget.get("committed", {})
        fis  = budget.get("fiscal", {})
        rev_cr         = rev.get("total_revenue_receipts_cr", 0) or 0
        rev_exp_cr     = exp.get("revenue_exp_cr", 0) or 0
        committed_cr   = com.get("total_committed_cr", 0) or 0
        salary_cr      = com.get("salaries_cr", 0) or 0
        pension_cr     = com.get("pensions_cr", 0) or 0
        interest_cr    = com.get("interest_cr", 0) or 0
        disc_cr        = rev_exp_cr - committed_cr
        deficit_cr     = fis.get("fiscal_deficit_cr", 0) or 0
        committed_pct  = (committed_cr / rev_exp_cr * 100) if rev_exp_cr else 0
        budget_section = (
            f"TN State Budget {fy} (CAG actuals):\n"
            f"  Total revenue receipts : ₹{rev_cr:>10,.0f} cr\n"
            f"  Revenue expenditure    : ₹{rev_exp_cr:>10,.0f} cr\n"
            f"  Committed expenditure  : ₹{committed_cr:>10,.0f} cr  ({committed_pct:.0f}% of rev exp)\n"
            f"    → Salaries : ₹{salary_cr:>10,.0f} cr\n"
            f"    → Pensions : ₹{pension_cr:>10,.0f} cr\n"
            f"    → Interest : ₹{interest_cr:>10,.0f} cr\n"
            f"  Discretionary budget   : ₹{disc_cr:>10,.0f} cr\n"
            f"  Fiscal deficit         : ₹{deficit_cr:>10,.0f} cr"
        )
    else:
        budget_section = (
            "TN State Budget 2024-25 (approximate — local cache unavailable):\n"
            "  Use general knowledge of TN's fiscal position: ~55% revenue exp is committed\n"
            "  (salaries + pensions + interest); discretionary ~₹1.3 lakh cr; deficit ~₹78,000 cr."
        )

    # ── 3. Social baseline (NFHS-5 2019-21 / Census) ─────────────────────────
    social_baseline = (
        "TN Social Baseline (NFHS-5 2019-21 / Census):\n"
        "  Population (2026 est.)          : ~7.9 crore\n"
        "  Ration-card (PDS) households    : ~2.0 crore\n"
        "  BPL households (Tendulkar)      : ~1.2 crore\n"
        "  MGNREGS daily wage (TN 2024-25) : ₹294\n"
        "  Female LFPR                     : 33% urban, 51% rural\n"
        "  Child stunting (under-5)        : 25%\n"
        "  Poverty line (Rangarajan 2014)  : ₹1,407/mo urban, ₹972/mo rural"
    )

    return f"""\
You are an expert Tamil Nadu election manifesto analyst with deep knowledge of TN governance,
fiscal policy, and social conditions.
Your task: extract concrete, specific electoral PROMISES and assess each promise's welfare impact
with factual precision — neither embellishing nor tarnishing any party's record.

━━━ LIVE ECONOMIC CONTEXT (as of {col_date}) ━━━
{budget_section}

Cost of Living — Tamil Nadu cities (Numbeo, {col_date}):
Chennai:
{_fmt_city("Chennai")}
Coimbatore:
{_fmt_city("Coimbatore")}
Madurai:
{_fmt_city("Madurai")}

{social_baseline}

Use this data to calibrate beneficiary_coverage, impact_depth, fiscal_viability, and
standalone_sufficient — e.g. a ₹2,500/month transfer against a Chennai salary of ₹45,000
is supplemental, not transformative; a farm-loan waiver above ₹5 acres excludes 70%+ of TN farmers.

━━━ CATEGORIES (use EXACTLY these strings — nothing else) ━━━
  "Agriculture"       — farm loans, irrigation, crop insurance, MSP, farmers
  "Education"         — schools, laptops, scholarships, mid-day meals, skill dev
  "TASMAC & Revenue"  — liquor policy, TASMAC, alcohol, state revenue, taxation
  "Women's Welfare"   — SHGs, maternity, women's safety, reservations, Magalir
  "Infrastructure"    — roads, power, water supply, housing, transport, internet

━━━ STANCE_VIBE (pick one) ━━━
  "Welfare-centric" | "Infrastructure-heavy" | "Revenue-focused" |
  "Populist" | "Reform-oriented" | "Women-focused" | "Farmer-focused"

━━━ WELFARE ASSESSMENT FIELDS (factual — no political spin) ━━━

beneficiary_coverage — who actually receives this benefit:
  "universal"      → all TN residents
  "broad_majority" → large defined group (all women, all students, all farmers)
  "targeted_poor"  → explicitly BPL / low-income / marginalized
  "specific_group" → narrow occupational/demographic group (weavers, fishermen, ex-servicemen)

impact_depth — magnitude of change in beneficiary's material circumstances:
  "transformative" → eliminates debt or provides sustained income floor (structural change)
  "substantive"    → meaningful improvement — significant income supplement or essential cost covered
  "supplemental"   → modest benefit — small transfer, minor discount, one-time subsidy
  "symbolic"       → primarily signaling; no significant material impact

fiscal_viability — based on TN budget position above:
  "feasible"           → within TN's demonstrated discretionary capacity (≲₹5,000 cr/yr)
  "stressed"           → requires major reallocation; stretches TN budget
  "central_dependent"  → requires central government scheme or matching funds
  "uncosted"           → manifesto provides no cost estimate

standalone_sufficient — true only if this promise alone meaningfully addresses the stated problem;
  false if structural gaps (eligibility exclusions, implementation, linked services) limit real impact.

coverage_gap_note — one factual sentence (with numbers where available) on who is excluded or what
  structural gap limits reach. Set to null ONLY if the promise genuinely has no meaningful gap.

━━━ DEEP ANALYSIS FIELDS (2-3 level causal chain) ━━━

impact_mechanism — one sentence: HOW this promise creates tangible change for its target group.
  Name the mechanism explicitly:
  e.g., "Removes debt burden from farm loans, freeing capital for reinvestment and consumption."
  e.g., "Provides unconditional income floor, directly raising household purchasing power."

first_order_effect — the immediate, direct outcome if implemented as stated:
  e.g., "1.37 crore women heads of household receive ₹2,000/month direct bank transfer."
  e.g., "Outstanding farm loans up to ₹2 lakh cleared for ~10 lakh farmers."

second_order_effect — the downstream consequence that flows from the first-order effect:
  e.g., "Increased disposable income reduces daily loan dependency and improves food security."
  e.g., "Freed capital allows investment in next crop cycle, raising agricultural output."

third_order_effect — long-term systemic change enabled by the second-order effect; null if not applicable:
  e.g., "Breaks intergenerational poverty cycle by enabling education spending for children."
  e.g., "Sustained income independence reduces women's vulnerability to domestic exploitation."

implementation_risk — probability this promise is NOT delivered within the 5-year term:
  "low"    → simple administrative action, within TN capacity, no major blockers
  "medium" → requires coordination, new legislation, or central government alignment
  "high"   → significant financial, political, or implementation barriers; similar past promises failed

root_cause_addressed — true if this promise targets structural drivers of the problem (e.g., land tenure,
  income floor, debt trap); false if it addresses symptoms or provides temporary relief only.

━━━ EXTRACTION RULES ━━━
1. Extract only SPECIFIC, ACTIONABLE promises — not vague aspirations.
   Good: "Waive farm loans up to ₹2 lakh within 100 days"
   Bad:  "We will work for farmers' welfare"
2. If text is Tamil, translate promise_text_en to clear English; keep original in promise_text_ta.
3. If text is English, set promise_text_ta to null.
4. amount_mentioned: include only when a specific figure is stated (e.g., "₹1,000/month").
5. scheme_name: include only when the promise names a specific scheme.
6. approx_page: page number where this promise appears.

━━━ OUTPUT FORMAT ━━━
Respond with ONLY a valid JSON array (no prose, no markdown fences):
[
  {{
    "category": "<one of the 5 categories above>",
    "promise_text_en": "<English promise text>",
    "promise_text_ta": "<Tamil text or null>",
    "stance_vibe": "<one of 7 stances>",
    "amount_mentioned": "<string or null>",
    "scheme_name": "<string or null>",
    "approx_page": <integer>,
    "beneficiary_coverage": "<universal|broad_majority|targeted_poor|specific_group>",
    "impact_depth": "<transformative|substantive|supplemental|symbolic>",
    "fiscal_viability": "<feasible|stressed|central_dependent|uncosted>",
    "standalone_sufficient": <true|false>,
    "coverage_gap_note": "<factual string or null>",
    "impact_mechanism": "<one sentence on HOW this creates change, or null>",
    "first_order_effect": "<immediate direct result, or null>",
    "second_order_effect": "<downstream consequence, or null>",
    "third_order_effect": "<systemic/long-term effect, or null>",
    "implementation_risk": "<low|medium|high>",
    "root_cause_addressed": <true|false>
  }}
]
If no concrete promises found in this section, return [].
"""


def call_claude(client: Any, system_prompt: str, text_chunk: str, chunk_label: str, model: str) -> list[dict]:
    """Call Claude to extract promises from a text chunk. Returns list of raw promise dicts."""
    user_msg = f"Extract promises from the following manifesto section:\n\n{text_chunk}"
    try:
        response = client.messages.create(
            model=model,
            max_tokens=4096,
            system=system_prompt,
            messages=[{"role": "user", "content": user_msg}],
        )
        raw = response.content[0].text.strip()
        # Strip markdown fences if Claude added them anyway
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        parsed = json.loads(raw)
        if not isinstance(parsed, list):
            print(f"    [{chunk_label}] Unexpected response type: {type(parsed)}")
            return []
        return parsed
    except json.JSONDecodeError as e:
        print(f"    [{chunk_label}] JSON parse error: {e}")
        return []
    except Exception as e:
        print(f"    [{chunk_label}] API error: {e}")
        return []


def validate_promise(raw: dict) -> dict | None:
    """Validate and normalise a raw promise dict from Claude."""
    cat = raw.get("category", "")
    if cat not in VALID_CATEGORIES:
        return None
    stance = raw.get("stance_vibe", "Welfare-centric")
    if stance not in VALID_STANCES:
        stance = "Welfare-centric"
    en = (raw.get("promise_text_en") or "").strip()
    if not en:
        return None

    beneficiary_coverage = raw.get("beneficiary_coverage")
    if beneficiary_coverage not in VALID_BENEFICIARY_COVERAGE:
        beneficiary_coverage = None

    impact_depth = raw.get("impact_depth")
    if impact_depth not in VALID_IMPACT_DEPTH:
        impact_depth = None

    fiscal_viability = raw.get("fiscal_viability")
    if fiscal_viability not in VALID_FISCAL_VIABILITY:
        fiscal_viability = None

    standalone_sufficient = raw.get("standalone_sufficient")
    if not isinstance(standalone_sufficient, bool):
        standalone_sufficient = None

    coverage_gap_note = raw.get("coverage_gap_note")
    if isinstance(coverage_gap_note, str):
        coverage_gap_note = coverage_gap_note.strip() or None
    elif coverage_gap_note is not None:
        coverage_gap_note = None

    def _clean_str(key: str) -> str | None:
        v = raw.get(key)
        return v.strip() or None if isinstance(v, str) else None

    implementation_risk = raw.get("implementation_risk")
    if implementation_risk not in VALID_IMPLEMENTATION_RISK:
        implementation_risk = None

    root_cause_addressed = raw.get("root_cause_addressed")
    if not isinstance(root_cause_addressed, bool):
        root_cause_addressed = None

    return {
        "category":             cat,
        "promise_text_en":      en,
        "promise_text_ta":      (raw.get("promise_text_ta") or "").strip() or None,
        "stance_vibe":          stance,
        "amount_mentioned":     raw.get("amount_mentioned") or None,
        "scheme_name":          raw.get("scheme_name") or None,
        "approx_page":          int(raw.get("approx_page", 0)),
        # Welfare assessment — level 1
        "beneficiary_coverage":  beneficiary_coverage,
        "impact_depth":          impact_depth,
        "fiscal_viability":      fiscal_viability,
        "standalone_sufficient": standalone_sufficient,
        "coverage_gap_note":     coverage_gap_note,
        # Deep analysis — levels 2-3
        "impact_mechanism":      _clean_str("impact_mechanism"),
        "first_order_effect":    _clean_str("first_order_effect"),
        "second_order_effect":   _clean_str("second_order_effect"),
        "third_order_effect":    _clean_str("third_order_effect"),
        "implementation_risk":   implementation_risk,
        "root_cause_addressed":  root_cause_addressed,
    }


# ---------------------------------------------------------------------------
# Core extraction pipeline
# ---------------------------------------------------------------------------

def extract_promises(
    party_id: str,
    target_year: int,
    cfg: dict[str, Any],
    model: str,
    client: Any,
    keep_pdf: bool = False,
) -> list[dict]:
    print(f"\n── {party_id.upper()} {target_year} ─────────────────────────────")
    print(f"  PDF: {cfg['pdf_url']}")

    # 1. Download
    pdf_path = Path(tempfile.mkdtemp()) / f"{party_id}_{target_year}.pdf"
    try:
        download_pdf(cfg["pdf_url"], pdf_path)
    except Exception as e:
        print(f"  ERROR downloading PDF: {e}")
        return []

    # 2. Extract text
    try:
        pages = extract_pages(pdf_path)
    except Exception as e:
        print(f"  ERROR extracting text: {e}")
        return []

    if not pages:
        print("  WARNING: No text extracted (may be a scanned PDF requiring OCR)")
        return []

    print(f"  {len(pages)} pages with text extracted")

    # 3. Chunk pages and call Claude
    raw_promises: list[dict] = []
    chunks = [pages[i:i + PAGES_PER_CHUNK] for i in range(0, len(pages), PAGES_PER_CHUNK)]
    print(f"  {len(chunks)} chunks × {PAGES_PER_CHUNK} pages → Claude {model}")

    system_prompt = build_system_prompt()  # built once per party (CoL data cached)

    for ci, chunk in enumerate(chunks, start=1):
        page_nums = [p for p, _ in chunk]
        text = "\n\n---\n\n".join(f"[Page {p}]\n{t}" for p, t in chunk)
        label = f"chunk {ci}/{len(chunks)}, pages {page_nums[0]}–{page_nums[-1]}"
        print(f"  Processing {label} ({len(text)} chars)…", end=" ", flush=True)

        raw = call_claude(client, system_prompt, text, label, model)
        valid = [v for r in raw if (v := validate_promise(r)) is not None]
        print(f"{len(valid)} promises")
        raw_promises.extend(valid)

        # Polite rate limit (Haiku is generous but let's be safe)
        if ci < len(chunks):
            time.sleep(0.5)

    # 4. Assign stable doc_ids
    cat_counters: dict[str, int] = defaultdict(int)
    now = datetime.now(timezone.utc).isoformat()
    result = []
    for p in raw_promises:
        cat_abbrev = _CAT_ABBREV[p["category"]]
        cat_counters[cat_abbrev] += 1
        doc_id = f"{party_id}_{target_year}_{cat_abbrev}_{cat_counters[cat_abbrev]:03d}"
        source_note = cfg.get("source_note", f"LLM-extracted from OpenCity PDF · {cfg['pdf_url']}")

        record: dict[str, Any] = {
            "doc_id":               doc_id,
            "party_id":             party_id,
            "party_name":           cfg["party_name"],
            "party_color":          cfg["party_color"],
            "category":             p["category"],
            "promise_text_en":      p["promise_text_en"],
            "promise_text_ta":      p["promise_text_ta"] or "",
            "target_year":          target_year,
            "status":               "Proposed",
            "stance_vibe":          p["stance_vibe"],
            "amount_mentioned":     p["amount_mentioned"],
            "scheme_name":          p["scheme_name"],
            "manifesto_pdf_url":    cfg["pdf_url"],
            "manifesto_pdf_page":   p["approx_page"],
            "source_notes":         source_note,
            "ground_truth_confidence": cfg["confidence"],
            "is_joint_manifesto":   cfg["is_joint"],
            "track_fulfillment":    cfg["track_fulfillment"],
            # Welfare assessment — level 1
            "beneficiary_coverage":  p["beneficiary_coverage"],
            "impact_depth":          p["impact_depth"],
            "fiscal_viability":      p["fiscal_viability"],
            "standalone_sufficient": p["standalone_sufficient"],
            "coverage_gap_note":     p["coverage_gap_note"],
            # Deep analysis — levels 2-3
            "impact_mechanism":      p["impact_mechanism"],
            "first_order_effect":    p["first_order_effect"],
            "second_order_effect":   p["second_order_effect"],
            "third_order_effect":    p["third_order_effect"],
            "implementation_risk":   p["implementation_risk"],
            "root_cause_addressed":  p["root_cause_addressed"],
            "_extracted_at":        now,
        }
        result.append(record)

    print(f"  ✓ {len(result)} total promises extracted")

    if not keep_pdf:
        pdf_path.unlink(missing_ok=True)

    return result


# ---------------------------------------------------------------------------
# Firestore upload
# ---------------------------------------------------------------------------

def upload_to_firestore(records: list[dict], project_id: str, dry_run: bool) -> None:
    if dry_run:
        print(f"\n[DRY RUN] Would upload {len(records)} docs to `manifesto_promises`")
        return

    try:
        from google.cloud import firestore as fs
    except ImportError:
        print("ERROR: google-cloud-firestore not installed")
        return

    db = fs.Client(project=project_id)
    col = db.collection("manifesto_promises")
    written = 0
    for rec in records:
        doc_id = rec["doc_id"]
        col.document(doc_id).set(rec, merge=True)
        written += 1
    print(f"  Uploaded {written} docs to Firestore `manifesto_promises`")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Extract manifesto promises via Claude and load to Firestore")
    ap.add_argument("--year",    type=int, required=True,  help="Election year: 2026 or 2024")
    ap.add_argument("--party",   default=None,             help="Specific party_id (e.g. dmk). Omit for all.")
    ap.add_argument("--dry-run", action="store_true",      help="Extract only — don't upload to Firestore")
    ap.add_argument("--keep-pdf", action="store_true",     help="Keep downloaded PDFs in /tmp")
    ap.add_argument("--model",   default="claude-haiku-4-5-20251001",
                                                           help="Anthropic model to use")
    ap.add_argument("--output",  default=None,             help="Optional JSON output file path")
    args = ap.parse_args()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        sys.exit("ERROR: ANTHROPIC_API_KEY environment variable not set")

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
    except ImportError:
        sys.exit("ERROR: anthropic package not installed. Run: pip install anthropic")

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "naatunadappu")

    # Select sources for this run
    sources = {
        (pid, yr): cfg
        for (pid, yr), cfg in SOURCE_CONFIG.items()
        if yr == args.year and (args.party is None or pid == args.party)
    }

    if not sources:
        available = [f"{pid} {yr}" for (pid, yr) in SOURCE_CONFIG if yr == args.year]
        sys.exit(
            f"No sources match --year {args.year}"
            + (f" --party {args.party}" if args.party else "")
            + f"\nAvailable for {args.year}: {', '.join(available) if available else 'none'}"
        )

    print(f"Processing {len(sources)} source(s) for year {args.year}")
    print(f"Model: {args.model}")
    print(f"Mode:  {'DRY RUN' if args.dry_run else 'LIVE UPLOAD'}")

    all_records: list[dict] = []

    for (party_id, year), cfg in sources.items():
        records = extract_promises(
            party_id, year, cfg, args.model, client, keep_pdf=args.keep_pdf
        )
        all_records.extend(records)

        if records:
            # Save per-party JSON
            out_path = OUT_DIR / f"manifesto_promises_{year}_{party_id}.json"
            OUT_DIR.mkdir(parents=True, exist_ok=True)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            print(f"  Saved → {out_path.relative_to(ROOT_DIR)}")

    if not all_records:
        print("\nNo promises extracted — nothing to upload")
        return

    # Optional combined output
    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            json.dump(all_records, f, ensure_ascii=False, indent=2)
        print(f"\nCombined output → {args.output}")

    print(f"\nTotal extracted: {len(all_records)} promises across {len(sources)} parties")

    # Firestore
    upload_to_firestore(all_records, project_id, dry_run=args.dry_run)

    # Summary table
    from collections import Counter
    by_party = Counter(r["party_id"] for r in all_records)
    by_cat   = Counter(r["category"] for r in all_records)
    print("\n── Summary ──────────────────────────────")
    print("Party breakdown:")
    for pid, n in sorted(by_party.items()):
        print(f"  {pid:12s}  {n}")
    print("Category breakdown:")
    for cat, n in sorted(by_cat.items()):
        print(f"  {cat:25s}  {n}")


if __name__ == "__main__":
    main()
