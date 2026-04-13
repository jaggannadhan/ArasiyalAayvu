"""
Bridge Rules — maps SDG goals to socioeconomic indicators and
manifesto categories to SDG goals, with influence weights.

Three types of bridges:
  A. Policy-to-Outcome (manifesto → SDG → indicator): semantic/causal
  B. Performance-to-Accountability (MLA stats ↔ district outcomes): correlational
  C. Inter-indicator influences (education → employment): expert-defined causal

Weights:
  1.0 = direct, primary relationship
  0.7 = strong secondary relationship
  0.4 = indirect / partial relationship
  0.2 = weak contextual link

These are expert-defined (Type A — Semantic Link). Statistical validation
(Type B) requires 5+ years of time-series data — planned for Phase 2.
"""

from __future__ import annotations

# ─────────────────────────────────────────────────────────────────────────────
# SDG Goal → Indicator mappings
# Each SDG goal maps to one or more socioeconomic indicators with a weight
# representing how directly the indicator measures that goal's progress.
# ─────────────────────────────────────────────────────────────────────────────

SDG_TO_INDICATORS: dict[str, list[dict]] = {
    "1": [  # No Poverty
        {"indicator": "indicator_hces",  "field": "mpce_combined", "weight": 1.0, "reason": "Household expenditure is the primary poverty proxy"},
        {"indicator": "indicator_plfs",  "field": "unemployment_rate", "weight": 0.7, "reason": "Unemployment directly drives poverty"},
        {"indicator": "indicator_col",   "field": "petrol",       "weight": 0.4, "reason": "Fuel cost affects purchasing power of poor"},
    ],
    "2": [  # Zero Hunger
        {"indicator": "indicator_hces",  "field": "mpce_combined", "weight": 0.7, "reason": "Food is the largest share of poor household expenditure"},
        {"indicator": "indicator_col",   "field": "lpg_14kg",     "weight": 0.4, "reason": "Cooking fuel cost affects food preparation access"},
    ],
    "3": [  # Good Health & Well-being
        {"indicator": "indicator_srs",   "field": "imr",          "weight": 1.0, "reason": "Infant mortality is the primary health outcome indicator"},
        {"indicator": "indicator_srs",   "field": "mmr",          "weight": 1.0, "reason": "Maternal mortality measures healthcare system quality"},
        {"indicator": "indicator_srs",   "field": "cbr",          "weight": 0.4, "reason": "Birth rate reflects reproductive health access"},
        {"indicator": "indicator_srs",   "field": "cdr",          "weight": 0.7, "reason": "Death rate measures overall health system efficacy"},
    ],
    "4": [  # Quality Education
        {"indicator": "indicator_udise", "field": "ger_primary",      "weight": 1.0, "reason": "Gross enrolment ratio = primary education access"},
        {"indicator": "indicator_udise", "field": "dropout_primary",  "weight": 0.7, "reason": "Dropout rate measures retention"},
        {"indicator": "indicator_udise", "field": "ptr",              "weight": 0.7, "reason": "Pupil-teacher ratio = education quality proxy"},
        {"indicator": "indicator_aishe", "field": "ger",              "weight": 1.0, "reason": "Higher education GER = tertiary access"},
    ],
    "5": [  # Gender Equality
        {"indicator": "indicator_ncrb",  "field": "crimes_against_women", "weight": 1.0, "reason": "Violence against women is the primary gender safety metric"},
        {"indicator": "indicator_plfs",  "field": "female_lfpr",          "weight": 0.7, "reason": "Female labour participation = economic empowerment"},
    ],
    "7": [  # Affordable & Clean Energy
        {"indicator": "indicator_col",   "field": "petrol",       "weight": 0.7, "reason": "Fuel affordability"},
        {"indicator": "indicator_col",   "field": "lpg_14kg",     "weight": 1.0, "reason": "LPG access = clean cooking fuel"},
        {"indicator": "indicator_udise", "field": "electricity_pct", "weight": 0.7, "reason": "School electrification = infrastructure proxy"},
    ],
    "8": [  # Decent Work & Economic Growth
        {"indicator": "indicator_plfs",  "field": "unemployment_rate", "weight": 1.0, "reason": "Unemployment is the direct measure"},
        {"indicator": "indicator_plfs",  "field": "lfpr",              "weight": 0.7, "reason": "Labour force participation = economic engagement"},
        {"indicator": "indicator_asi",   "field": "factories",         "weight": 0.7, "reason": "Factory count = formal sector employment capacity"},
        {"indicator": "indicator_asi",   "field": "gva_cr",            "weight": 1.0, "reason": "Gross value added = economic output"},
    ],
    "9": [  # Industry, Innovation & Infrastructure
        {"indicator": "indicator_asi",   "field": "factories",         "weight": 1.0, "reason": "Factory count = industrial base"},
        {"indicator": "indicator_asi",   "field": "total_output_cr",   "weight": 1.0, "reason": "Industrial output"},
        {"indicator": "indicator_asi",   "field": "gva_cr",            "weight": 0.7, "reason": "Value addition in manufacturing"},
        {"indicator": "indicator_udise", "field": "internet_pct",      "weight": 0.4, "reason": "Digital infrastructure proxy"},
    ],
    "10": [  # Reduced Inequalities
        {"indicator": "indicator_ncrb",  "field": "crimes_against_sc", "weight": 1.0, "reason": "Caste-based violence = inequality metric"},
        {"indicator": "indicator_ncrb",  "field": "crimes_against_st", "weight": 1.0, "reason": "Tribal discrimination = inequality metric"},
        {"indicator": "indicator_hces",  "field": "mpce_combined",     "weight": 0.4, "reason": "Expenditure inequality (needs rural-urban gap)"},
    ],
    "11": [  # Sustainable Cities
        {"indicator": "indicator_col",   "field": "petrol",   "weight": 0.4, "reason": "Transport cost in cities"},
        {"indicator": "indicator_ncrb",  "field": "total_ipc_crimes", "weight": 0.4, "reason": "Urban safety"},
    ],
    "16": [  # Peace, Justice & Strong Institutions
        {"indicator": "indicator_ncrb",  "field": "total_ipc_crimes", "weight": 1.0, "reason": "Total crime = institutional effectiveness proxy"},
        {"indicator": "indicator_ncrb",  "field": "crimes_against_children", "weight": 0.7, "reason": "Child safety = institutional protection capacity"},
    ],
}

# ─────────────────────────────────────────────────────────────────────────────
# Manifesto Category → SDG Goal mappings
# Maps broad manifesto promise categories to their relevant SDG goals.
# ─────────────────────────────────────────────────────────────────────────────

MANIFESTO_CATEGORY_TO_SDG: dict[str, list[dict]] = {
    "education": [
        {"sdg": "4", "weight": 1.0},
    ],
    "healthcare": [
        {"sdg": "3", "weight": 1.0},
    ],
    "employment": [
        {"sdg": "8", "weight": 1.0},
        {"sdg": "1", "weight": 0.7},
    ],
    "agriculture": [
        {"sdg": "2", "weight": 1.0},
        {"sdg": "1", "weight": 0.4},
    ],
    "women_empowerment": [
        {"sdg": "5", "weight": 1.0},
    ],
    "infrastructure": [
        {"sdg": "9", "weight": 1.0},
        {"sdg": "11", "weight": 0.7},
    ],
    "energy": [
        {"sdg": "7", "weight": 1.0},
    ],
    "industry": [
        {"sdg": "9", "weight": 1.0},
        {"sdg": "8", "weight": 0.7},
    ],
    "social_justice": [
        {"sdg": "10", "weight": 1.0},
        {"sdg": "16", "weight": 0.7},
    ],
    "law_and_order": [
        {"sdg": "16", "weight": 1.0},
    ],
    "poverty_alleviation": [
        {"sdg": "1", "weight": 1.0},
        {"sdg": "2", "weight": 0.7},
    ],
    "housing": [
        {"sdg": "11", "weight": 1.0},
        {"sdg": "1", "weight": 0.4},
    ],
    "environment": [
        {"sdg": "13", "weight": 1.0},
        {"sdg": "15", "weight": 0.7},
    ],
    "water_sanitation": [
        {"sdg": "6", "weight": 1.0},
        {"sdg": "3", "weight": 0.4},
    ],
}

# ─────────────────────────────────────────────────────────────────────────────
# Inter-indicator causal influences (expert-defined)
# These represent "if X changes, Y is likely affected" relationships.
# Direction matters: source → target.
# ─────────────────────────────────────────────────────────────────────────────

INDICATOR_INFLUENCES: list[dict] = [
    {
        "source": "indicator_plfs",  "source_field": "unemployment_rate",
        "target": "indicator_ncrb",  "target_field": "total_ipc_crimes",
        "weight": 0.6, "direction": "positive",
        "reason": "Higher unemployment correlates with higher crime rates",
    },
    {
        "source": "indicator_udise", "source_field": "dropout_primary",
        "target": "indicator_plfs",  "target_field": "unemployment_rate",
        "weight": 0.5, "direction": "positive",
        "reason": "School dropouts enter unskilled labour market, increasing structural unemployment",
    },
    {
        "source": "indicator_asi",   "source_field": "factories",
        "target": "indicator_plfs",  "target_field": "lfpr",
        "weight": 0.7, "direction": "positive",
        "reason": "More factories → more formal employment → higher labour force participation",
    },
    {
        "source": "indicator_col",   "source_field": "petrol",
        "target": "indicator_hces",  "target_field": "mpce_combined",
        "weight": 0.5, "direction": "positive",
        "reason": "Fuel cost increase → higher household expenditure (cost-push)",
    },
    {
        "source": "indicator_srs",   "source_field": "cbr",
        "target": "indicator_udise", "target_field": "schools_total",
        "weight": 0.4, "direction": "positive",
        "reason": "Higher birth rates → increased school demand in 5-6 years",
    },
    {
        "source": "indicator_aishe", "source_field": "ger",
        "target": "indicator_plfs",  "target_field": "unemployment_rate",
        "weight": 0.4, "direction": "negative",
        "reason": "Higher education GER → better employability → lower unemployment (lagged)",
    },
    {
        "source": "indicator_ncrb",  "source_field": "crimes_against_women",
        "target": "indicator_plfs",  "target_field": "female_lfpr",
        "weight": 0.5, "direction": "negative",
        "reason": "Higher crime against women → reduced female labour participation (safety barrier)",
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# SDG Goal metadata (for node labels and display)
# ─────────────────────────────────────────────────────────────────────────────

SDG_GOAL_NAMES: dict[str, str] = {
    "1":  "No Poverty",
    "2":  "Zero Hunger",
    "3":  "Good Health & Well-being",
    "4":  "Quality Education",
    "5":  "Gender Equality",
    "6":  "Clean Water & Sanitation",
    "7":  "Affordable & Clean Energy",
    "8":  "Decent Work & Economic Growth",
    "9":  "Industry, Innovation & Infrastructure",
    "10": "Reduced Inequalities",
    "11": "Sustainable Cities & Communities",
    "12": "Responsible Consumption & Production",
    "13": "Climate Action",
    "14": "Life Below Water",
    "15": "Life on Land",
    "16": "Peace, Justice & Strong Institutions",
}
