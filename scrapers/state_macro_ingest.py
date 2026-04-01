"""
State Macro Ingest — ArasiyalAayvu
Loads curated state-level macro-economic, health, water-risk, and crop-economics
data into Firestore.  All data is sourced from official publications (cited inline).

Collections written:
  state_macro          — GSDP, sectoral breakdown, fiscal metrics
  district_health      — NCD prevalence by district (NFHS-5 / ICMR)
  district_water_risk  — Water stress levels (SDMA / WRI Aqueduct)
  crop_economics       — MSP, FRP, production cost per crop

Run:
  python scrapers/state_macro_ingest.py [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# ── Firestore ────────────────────────────────────────────────────────────────
try:
    from google.cloud import firestore
    from google.cloud.firestore_v1 import SERVER_TIMESTAMP
except ImportError:
    print("google-cloud-firestore not installed. Run: pip install google-cloud-firestore")
    sys.exit(1)

ROOT = Path(__file__).resolve().parent.parent
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "naatunadappu")

NOW_ISO = datetime.now(timezone.utc).isoformat()

# ─────────────────────────────────────────────────────────────────────────────
# 1. STATE MACRO — GSDP, Sectoral, Fiscal
# Source: Tamil Nadu Economic Survey 2023-24; MoSPI advance estimates
# ─────────────────────────────────────────────────────────────────────────────
STATE_MACRO: list[dict] = [
    {
        "doc_id": "gsdp_growth_fy24",
        "metric_id": "gsdp_growth_fy24",
        "metric_name": "GSDP Growth Rate FY2023-24",
        "metric_name_ta": "மாநில உள்நாட்டு உற்பத்தி வளர்ச்சி விகிதம் 2023-24",
        "category": "Economy",
        "subcategory": "Growth",
        "value": 11.19,
        "unit": "percent",
        "comparison_national": 8.2,
        "tn_vs_national": "+2.99 pp above national average",
        "year": 2024,
        "context": "Tamil Nadu's GSDP grew at 11.19% in FY2023-24, outpacing India's national GDP growth of 8.2%. "
                   "TN is the second-largest state economy by GSDP at approx. ₹25.9 lakh crore.",
        "source_title": "Tamil Nadu Economic Survey 2023-24",
        "source_url": "https://tn.gov.in/economic-survey",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "sectoral_services_share",
        "metric_id": "sectoral_services_share",
        "metric_name": "Services Sector Share of GSDP",
        "metric_name_ta": "சேவைத் துறையின் மாநில உற்பத்தில் பங்கு",
        "category": "Economy",
        "subcategory": "Sectoral",
        "value": 53.0,
        "unit": "percent",
        "year": 2024,
        "context": "Services (IT, finance, trade, tourism) account for 53% of TN's GSDP, led by Chennai's IT corridor "
                   "and banking hub. Software exports alone exceed ₹3.5 lakh crore.",
        "source_title": "TN Economic Survey 2023-24",
        "source_url": "https://tn.gov.in/economic-survey",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "sectoral_manufacturing_share",
        "metric_id": "sectoral_manufacturing_share",
        "metric_name": "Manufacturing Sector Share of GSDP",
        "metric_name_ta": "உற்பத்தித் துறையின் மாநில உற்பத்தில் பங்கு",
        "category": "Economy",
        "subcategory": "Sectoral",
        "value": 34.0,
        "unit": "percent",
        "year": 2024,
        "context": "Manufacturing contributes 34% of GSDP. Key industries: automobiles (Chennai = Detroit of India), "
                   "electronics (Foxconn/Samsung in Sriperumbudur), textiles (Tiruppur), leather (Vellore/Ranipet).",
        "leading_sectors": ["Automobiles & Auto-components", "Electronics & Semiconductors",
                             "Textiles & Garments", "Leather & Footwear"],
        "source_title": "TN Economic Survey 2023-24",
        "source_url": "https://tn.gov.in/economic-survey",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "sectoral_agriculture_share",
        "metric_id": "sectoral_agriculture_share",
        "metric_name": "Agriculture Sector Share of GSDP",
        "metric_name_ta": "விவசாயத் துறையின் மாநில உற்பத்தில் பங்கு",
        "category": "Economy",
        "subcategory": "Sectoral",
        "value": 13.0,
        "unit": "percent",
        "year": 2024,
        "context": "Agriculture contributes 13% of GSDP but employs ~36% of the workforce. "
                   "TN is a major producer of paddy, sugarcane, bananas, flowers, and spices. "
                   "The Cauvery delta remains TN's rice bowl.",
        "source_title": "TN Economic Survey 2023-24",
        "source_url": "https://tn.gov.in/economic-survey",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "fiscal_debt_to_gsdp_fy24",
        "metric_id": "fiscal_debt_to_gsdp_fy24",
        "metric_name": "State Debt-to-GSDP Ratio",
        "metric_name_ta": "மாநில கடன்-உற்பத்தி விகிதம்",
        "category": "Fiscal",
        "subcategory": "Debt",
        "value": 24.1,
        "unit": "percent",
        "frbm_target": 25.0,
        "year": 2024,
        "context": "TN's debt-to-GSDP ratio stands at 24.1% — within the FRBM ceiling of 25%. "
                   "Total outstanding liabilities: approx. ₹7.99 lakh crore. "
                   "Welfare spending pressure from schemes like Kalaignar Magalir Urimai Thogai drives borrowing.",
        "alert_level": "MEDIUM",
        "source_title": "TN Budget 2024-25 Overview",
        "source_url": "https://finance.tn.gov.in",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "tasmac_revenue_fy24",
        "metric_id": "tasmac_revenue_fy24",
        "metric_name": "TASMAC Revenue (Excise + Sales)",
        "metric_name_ta": "தாஸ்மாக் வருவாய் (கலால் + விற்பனை)",
        "category": "Fiscal",
        "subcategory": "Revenue",
        "value": 47800,
        "unit": "crore_inr",
        "share_of_own_tax_revenue": 20.5,
        "year": 2024,
        "context": "TASMAC (liquor) contributed approx. ₹47,800 crore to state revenue in FY2023-24, "
                   "representing ~20.5% of TN's own tax revenue. This is a key fiscal dependency and a "
                   "contested policy area — critics link TASMAC density to higher NCD burden in affected districts.",
        "policy_tension": "Revenue dependence vs. public health outcomes",
        "source_title": "TN Budget 2024-25; TASMAC Annual Report",
        "source_url": "https://finance.tn.gov.in",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# 2. DISTRICT HEALTH — NCD Prevalence
# Source: NFHS-5 (2019-21) state factsheet; ICMR-INDIAB Study 2023
# ─────────────────────────────────────────────────────────────────────────────
# District-level NCD data from ICMR-INDIAB Study and NFHS-5 sub-state analysis.
# Where district-level data is unavailable, state average is used (scope: "state").
DISTRICT_HEALTH: list[dict] = [
    # ── State-level benchmark records ────────────────────────────────────────
    {
        "doc_id": "tn_hypertension_state",
        "metric_id": "hypertension_prevalence",
        "district_slug": None,
        "district_name": None,
        "metric_scope": "state",
        "metric_name": "Hypertension Prevalence (Adults 18+)",
        "metric_name_ta": "உயர் இரத்த அழுத்தம் பரவல் (18+ வயது)",
        "value": 31.4,
        "unit": "percent",
        "national_average": 28.0,
        "tn_vs_national": "+3.4 pp above national average",
        "year": 2021,
        "category": "NCD",
        "subcategory": "Cardiovascular",
        "context": "31.4% of TN adults have hypertension — higher than national average. "
                   "Urban districts (Chennai, Coimbatore) show higher prevalence (35-38%) due to "
                   "sedentary lifestyles, high-salt diet, and TASMAC density.",
        "alert_level": "HIGH",
        "policy_gap": True,
        "source_title": "NFHS-5 Tamil Nadu State Factsheet (2019-21)",
        "source_url": "https://main.mohfw.gov.in/sites/default/files/NFHS-5_Phase-II_0.pdf",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "tn_diabetes_state",
        "metric_id": "diabetes_prevalence",
        "district_slug": None,
        "district_name": None,
        "metric_scope": "state",
        "metric_name": "Diabetes Prevalence (Adults 20+)",
        "metric_name_ta": "நீரிழிவு நோய் பரவல் (20+ வயது)",
        "value": 16.8,
        "unit": "percent",
        "national_average": 11.4,
        "tn_vs_national": "+5.4 pp above national average — one of highest in India",
        "year": 2023,
        "category": "NCD",
        "subcategory": "Metabolic",
        "context": "TN has one of India's highest diabetes rates at 16.8%. "
                   "ICMR-INDIAB 2023 estimates 1 in 6 TN adults is diabetic. "
                   "Urban districts show 20-22% prevalence. Economic cost: ~₹8,500 crore/year in treatment.",
        "alert_level": "HIGH",
        "policy_gap": True,
        "source_title": "ICMR-INDIAB Study Phase III (2023); Lancet Diabetes & Endocrinology",
        "source_url": "https://www.thelancet.com/journals/landia/article/PIIS2213-8587(23)00119-4",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "tn_overweight_bmi_state",
        "metric_id": "overweight_bmi_prevalence",
        "district_slug": None,
        "district_name": None,
        "metric_scope": "state",
        "metric_name": "Overweight/Obese Adults (BMI ≥25)",
        "metric_name_ta": "அதிக எடை / பருமன் (BMI ≥25)",
        "value": 40.0,
        "unit": "percent",
        "national_average": 24.0,
        "tn_vs_national": "+16 pp — significantly above national",
        "year": 2021,
        "category": "NCD",
        "subcategory": "Metabolic",
        "context": "40% of TN adults are overweight or obese (BMI ≥25), nearly double the national average. "
                   "Driven by diet transition (high refined carbs, fried foods) and urbanisation. "
                   "Strongly correlated with TN's high diabetes and hypertension rates.",
        "alert_level": "HIGH",
        "source_title": "NFHS-5 Tamil Nadu State Factsheet (2019-21)",
        "source_url": "https://main.mohfw.gov.in/sites/default/files/NFHS-5_Phase-II_0.pdf",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "tn_ncd_deaths_share",
        "metric_id": "ncd_share_of_deaths",
        "district_slug": None,
        "district_name": None,
        "metric_scope": "state",
        "metric_name": "NCDs as Share of Total Deaths",
        "metric_name_ta": "மொத்த இறப்புகளில் தொற்றா நோய்களின் பங்கு",
        "value": 75.0,
        "unit": "percent",
        "year": 2022,
        "category": "NCD",
        "subcategory": "Mortality",
        "context": "Non-communicable diseases (NCDs) — cardiovascular, diabetes, cancer, COPD — account for "
                   "75% of deaths in Tamil Nadu. TN has completed the epidemiological transition faster "
                   "than most Indian states. Cardiovascular disease alone: ~35% of deaths.",
        "alert_level": "HIGH",
        "leading_ncd_causes": ["Cardiovascular disease", "Diabetes complications", "Cancer", "COPD"],
        "source_title": "Global Burden of Disease Study 2022 — India State Estimates",
        "source_url": "https://www.healthdata.org/india",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    # ── District-level records (selected high-data-availability districts) ───
    {
        "doc_id": "chennai_hypertension",
        "metric_id": "hypertension_prevalence",
        "district_slug": "chennai",
        "district_name": "Chennai",
        "metric_scope": "district",
        "metric_name": "Hypertension Prevalence — Chennai",
        "metric_name_ta": "சென்னையில் உயர் இரத்த அழுத்தம்",
        "value": 37.5,
        "unit": "percent",
        "year": 2021,
        "category": "NCD",
        "subcategory": "Cardiovascular",
        "context": "Chennai's hypertension rate (37.5%) is significantly above the state average (31.4%), "
                   "driven by urban sedentary lifestyles, high-salt street food culture, and alcohol consumption.",
        "alert_level": "HIGH",
        "source_title": "NFHS-5 District Factsheets (2019-21)",
        "source_url": "https://main.mohfw.gov.in/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "coimbatore_diabetes",
        "metric_id": "diabetes_prevalence",
        "district_slug": "coimbatore",
        "district_name": "Coimbatore",
        "metric_scope": "district",
        "metric_name": "Diabetes Prevalence — Coimbatore",
        "metric_name_ta": "கோயம்புத்தூரில் நீரிழிவு",
        "value": 19.2,
        "unit": "percent",
        "year": 2023,
        "category": "NCD",
        "subcategory": "Metabolic",
        "context": "Coimbatore is known as TN's diabetes capital with 19.2% adult prevalence. "
                   "Dr. V. Mohan's Diabetes Specialities Centre tracks one of India's largest "
                   "urban diabetes cohorts here.",
        "alert_level": "HIGH",
        "source_title": "ICMR-INDIAB Phase III District Analysis (2023)",
        "source_url": "https://www.thelancet.com/journals/landia/article/PIIS2213-8587(23)00119-4",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# 3. DISTRICT WATER RISK
# Source: WRI Aqueduct 4.0 (2023); TN SDMA Water Security Framework
# ─────────────────────────────────────────────────────────────────────────────
DISTRICT_WATER_RISK: list[dict] = [
    {
        "doc_id": "pudukottai_water_risk",
        "district_slug": "pudukkottai",
        "district_name": "Pudukkottai",
        "risk_level": "EXTREMELY_HIGH",
        "risk_label_en": "Extremely High Water Stress",
        "risk_label_ta": "மிகவும் அதிக நீர் அழுத்தம்",
        "water_stress_score": 4.8,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Severe",
        "avg_annual_rainfall_mm": 852,
        "drought_frequency_years": 3,
        "primary_source": "Tanks (92% dependence)",
        "context": "Pudukkottai has the highest water stress in TN. 92% of irrigation depends on tanks, "
                   "many of which are silted or encroached. Groundwater over-extraction exceeds recharge by 40%.",
        "policy_implication": "Tank restoration and rainwater harvesting are critical for the district.",
        "source_title": "WRI Aqueduct 4.0 Water Risk Atlas (2023)",
        "source_url": "https://www.wri.org/applications/aqueduct/water-risk-atlas/",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "kancheepuram_water_risk",
        "district_slug": "kancheepuram",
        "district_name": "Kancheepuram",
        "risk_level": "EXTREMELY_HIGH",
        "risk_label_en": "Extremely High Water Stress",
        "risk_label_ta": "மிகவும் அதிக நீர் அழுத்தம்",
        "water_stress_score": 4.6,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Severe",
        "avg_annual_rainfall_mm": 1160,
        "drought_frequency_years": 4,
        "primary_source": "Palar River / Groundwater",
        "context": "Dense industrialisation (electronics, auto) around Sriperumbudur has severely depleted "
                   "groundwater. The Palar river is seasonal and heavily polluted by tanneries upstream.",
        "policy_implication": "Industrial water recycling mandates and treated sewage reuse are critical.",
        "source_title": "WRI Aqueduct 4.0; TN SDMA District Risk Atlas",
        "source_url": "https://www.wri.org/applications/aqueduct/water-risk-atlas/",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "vellore_water_risk",
        "district_slug": "vellore",
        "district_name": "Vellore",
        "risk_level": "EXTREMELY_HIGH",
        "risk_label_en": "Extremely High Water Stress",
        "risk_label_ta": "மிகவும் அதிக நீர் அழுத்தம்",
        "water_stress_score": 4.5,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Critical",
        "avg_annual_rainfall_mm": 920,
        "drought_frequency_years": 3,
        "primary_source": "Palar River / Tanks",
        "context": "Vellore's leather tannery industry (Ranipet cluster) has contaminated groundwater with "
                   "hexavalent chromium. CPCB has flagged multiple critically polluted zones. "
                   "Water scarcity affects ~60% of rural households.",
        "policy_implication": "Effluent treatment and ZLD mandates for tannery clusters needed.",
        "source_title": "CPCB Critically Polluted Areas Assessment; WRI Aqueduct 4.0",
        "source_url": "https://cpcb.nic.in/",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "cauvery_delta_flood_risk",
        "district_slug": "thanjavur",
        "district_name": "Thanjavur (Cauvery Delta)",
        "risk_level": "FLOOD_PRONE",
        "risk_label_en": "Flood-Prone (Delta Region)",
        "risk_label_ta": "வெள்ள அபாயம் (டெல்டா பகுதி)",
        "water_stress_score": 1.2,
        "water_stress_max": 5.0,
        "flood_risk_score": 4.2,
        "avg_annual_rainfall_mm": 1100,
        "primary_source": "Cauvery & Mettur Dam",
        "context": "The Cauvery delta (Thanjavur, Tiruvarur, Nagapattinam) faces the opposite problem — "
                   "cyclonic floods in October-December. Erratic Mettur Dam releases and climate-intensified "
                   "northeast monsoon cause crop losses exceeding ₹2,000 crore in major flood years.",
        "policy_implication": "Flood forecasting, crop insurance uptake, and mangrove restoration are priorities.",
        "source_title": "TN SDMA Cyclone & Flood Risk Assessment 2023",
        "source_url": "https://sdma.tn.gov.in/",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "chennai_water_risk",
        "district_slug": "chennai",
        "district_name": "Chennai",
        "risk_level": "HIGH",
        "risk_label_en": "High Water Stress (Urban)",
        "risk_label_ta": "அதிக நீர் அழுத்தம் (நகர்ப்புறம்)",
        "water_stress_score": 4.1,
        "water_stress_max": 5.0,
        "avg_annual_rainfall_mm": 1400,
        "primary_source": "Veeranam / Krishna Water / Desalination",
        "context": "Despite high annual rainfall, Chennai has chronic water stress due to poor groundwater "
                   "recharge, encroached lakes, and rapid population growth. Day Zero in 2019 dried all four "
                   "major reservoirs. The city now relies on desalination (100+ MLD capacity) and distant "
                   "sources (Krishna Water via CWSS).",
        "policy_implication": "Lake restoration, rooftop rainwater harvesting (RTRWH enforcement), and "
                              "desalination capacity expansion are key.",
        "source_title": "CMWSSB Urban Water Security Report 2023",
        "source_url": "https://www.chennaimetrowater.tn.gov.in/",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "ramanathapuram_water_risk",
        "district_slug": "ramanathapuram",
        "district_name": "Ramanathapuram",
        "risk_level": "EXTREMELY_HIGH",
        "risk_label_en": "Extremely High Water Stress",
        "risk_label_ta": "மிகவும் அதிக நீர் அழுத்தம்",
        "water_stress_score": 4.7,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Critical",
        "avg_annual_rainfall_mm": 750,
        "primary_source": "Tanks / Rainwater",
        "context": "Ramanathapuram is TN's driest district with just 750mm average annual rainfall. "
                   "99% of the district is classified as water-scarce. "
                   "Out-migration for water-intensive farming is common.",
        "policy_implication": "Desalination plants, micro-irrigation, and alternate livelihood support are essential.",
        "source_title": "WRI Aqueduct 4.0; TN SDMA",
        "source_url": "https://www.wri.org/applications/aqueduct/water-risk-atlas/",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# 4. CROP ECONOMICS — MSP, FRP, Cost of Production
# Source: CACP Price Policy Reports 2024-25; TNAU Economics division
# ─────────────────────────────────────────────────────────────────────────────
CROP_ECONOMICS: list[dict] = [
    {
        "doc_id": "paddy_kharif_2024",
        "crop_name": "Paddy (Common)",
        "crop_name_ta": "நெல் (பொதுவான)",
        "crop_type": "Cereal",
        "season": "Kharif",
        "marketing_year": "2024-25",
        "msp_per_quintal": 2300,
        "a2_fl_cost_per_quintal": 1629,
        "c2_cost_per_quintal": 2120,
        "profit_over_a2fl_pct": 41.2,
        "profit_over_c2_pct": 8.5,
        "frp_applicable": False,
        "primary_tn_districts": ["thanjavur", "thiruvarur", "nagapattinam", "tiruvarur"],
        "tn_production_lakh_mt": 98.5,
        "context": "MSP ₹2,300/qtl for Kharif 2024-25. Cost of production (A2+FL) is ₹1,629/qtl, "
                   "giving a 41.2% return — above the government's 50% over A2+FL target. "
                   "However, using C2 cost (includes land rent), profit margin shrinks to 8.5%.",
        "policy_tension": "Farmers argue C2-based MSP should be used; CACP uses A2+FL basis.",
        "source_title": "CACP Price Policy Report Kharif 2024-25",
        "source_url": "https://cacp.dacnet.nic.in/",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "paddy_rabi_2024",
        "crop_name": "Paddy (Grade A)",
        "crop_name_ta": "நெல் (தரம் A)",
        "crop_type": "Cereal",
        "season": "Rabi",
        "marketing_year": "2024-25",
        "msp_per_quintal": 2320,
        "a2_fl_cost_per_quintal": 1644,
        "c2_cost_per_quintal": 2135,
        "profit_over_a2fl_pct": 41.1,
        "profit_over_c2_pct": 8.7,
        "frp_applicable": False,
        "primary_tn_districts": ["thanjavur", "tiruvarur", "nagapattinam"],
        "tn_production_lakh_mt": 62.3,
        "context": "Grade A paddy fetches ₹20 premium. The Cauvery delta is India's highest-yield paddy "
                   "belt with 4.5–5 MT/ha productivity, but water disputes with Karnataka threaten samba crop.",
        "policy_tension": "Mettur Dam release timing is a perennial political flashpoint.",
        "source_title": "CACP Price Policy Report Rabi 2024-25",
        "source_url": "https://cacp.dacnet.nic.in/",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "sugarcane_2025",
        "crop_name": "Sugarcane",
        "crop_name_ta": "கரும்பு",
        "crop_type": "Cash Crop",
        "season": "Annual",
        "marketing_year": "2025-26",
        "msp_per_quintal": None,
        "frp_per_quintal": 340,
        "state_advised_price_per_quintal": 350,
        "a2_fl_cost_per_quintal": 238,
        "c2_cost_per_quintal": 298,
        "profit_over_a2fl_pct": 42.9,
        "profit_over_frp_over_c2_pct": 14.1,
        "frp_applicable": True,
        "primary_tn_districts": ["erode", "vellore", "cuddalore", "thanjavur"],
        "tn_production_lakh_mt": 480.0,
        "context": "FRP (Fair & Remunerative Price) for 2025-26 is ₹340/qtl (basic recovery 10.25%). "
                   "TN's state-advised price is ₹350/qtl. "
                   "TN has ~3.5 lakh ha under sugarcane. Mills often delay payment by 3-6 months — "
                   "a chronic complaint. The actual farmer margin over C2 cost is ~14%.",
        "policy_tension": "Mill payment delays; recovery-linked pricing disadvantages farmers with older varieties.",
        "source_title": "CACP FRP Notification 2025-26; TN Sugar Corporation",
        "source_url": "https://cacp.dacnet.nic.in/",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "groundnut_kharif_2024",
        "crop_name": "Groundnut",
        "crop_name_ta": "நிலக்கடலை",
        "crop_type": "Oilseed",
        "season": "Kharif",
        "marketing_year": "2024-25",
        "msp_per_quintal": 6783,
        "a2_fl_cost_per_quintal": 4518,
        "c2_cost_per_quintal": 5460,
        "profit_over_a2fl_pct": 50.1,
        "profit_over_c2_pct": 24.2,
        "frp_applicable": False,
        "primary_tn_districts": ["vellore", "tiruvannamalai", "krishnagiri", "dindigul"],
        "tn_production_lakh_mt": 7.8,
        "context": "Groundnut MSP ₹6,783/qtl gives 50.1% return over A2+FL — exactly meeting the government's "
                   "promise of 50% profit. However, market prices often fall below MSP during harvest flush "
                   "without adequate procurement infrastructure.",
        "policy_tension": "PSS (Price Support Scheme) activation is often delayed or capped by state.",
        "source_title": "CACP Price Policy Report Kharif 2024-25",
        "source_url": "https://cacp.dacnet.nic.in/",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "cotton_kharif_2024",
        "crop_name": "Cotton (Medium Staple)",
        "crop_name_ta": "பருத்தி (நடுத்தர நூல்)",
        "crop_type": "Fibre",
        "season": "Kharif",
        "marketing_year": "2024-25",
        "msp_per_quintal": 7121,
        "a2_fl_cost_per_quintal": 4747,
        "c2_cost_per_quintal": 5882,
        "profit_over_a2fl_pct": 50.0,
        "profit_over_c2_pct": 21.1,
        "frp_applicable": False,
        "primary_tn_districts": ["coimbatore", "tiruppur", "erode", "virudhunagar"],
        "tn_production_lakh_mt": 3.2,
        "context": "Cotton MSP ₹7,121/qtl. Tiruppur's textile cluster (₹70,000 crore turnover) depends on "
                   "cotton supply. TN cotton yield (450 kg/ha) is below national average (500 kg/ha) due to "
                   "water stress and pest pressure. Pink bollworm remains a critical threat.",
        "policy_tension": "MSP procurement often bypassed by market arrivals below MSP in lean years.",
        "source_title": "CACP Price Policy Report Kharif 2024-25",
        "source_url": "https://cacp.dacnet.nic.in/",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "banana_tn_2024",
        "crop_name": "Banana (Robusta/Cavendish)",
        "crop_name_ta": "வாழைப்பழம் (ரோபஸ்டா)",
        "crop_type": "Horticulture",
        "season": "Perennial",
        "marketing_year": "2024",
        "msp_per_quintal": None,
        "approximate_farm_gate_price_per_quintal": 1200,
        "a2_fl_cost_per_quintal": 800,
        "frp_applicable": False,
        "primary_tn_districts": ["theni", "dindigul", "erode", "coimbatore"],
        "tn_production_lakh_mt": 88.0,
        "context": "TN is India's largest banana producer. No MSP exists for bananas — prices are fully "
                   "market-driven and highly volatile (₹400–₹2,000/qtl). Theni district produces "
                   "the premium 'Poovan' variety. Export potential to the Middle East and EU is underutilised.",
        "policy_tension": "No price safety net; farmers vulnerable to glut crashes (as in 2019, 2021).",
        "source_title": "NHB Horticulture Statistics 2024; TNAU Crop Production Guide",
        "source_url": "https://nhb.gov.in/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# Loader
# ─────────────────────────────────────────────────────────────────────────────
def _upsert_batch(db: "firestore.Client", collection: str, records: list[dict],
                  dry_run: bool) -> None:
    if dry_run:
        print(f"  [dry-run] Would write {len(records)} docs to '{collection}'")
        for r in records:
            print(f"    {r['doc_id']}: {r.get('metric_name') or r.get('crop_name') or r.get('risk_label_en','')}")
        return

    col_ref = db.collection(collection)
    batch = db.batch()
    count = 0
    for record in records:
        doc_id = record.pop("doc_id")
        record["_schema_version"] = "1.0"
        doc_ref = col_ref.document(doc_id)
        batch.set(doc_ref, record, merge=True)
        record["doc_id"] = doc_id  # restore for logging
        count += 1
        if count % 400 == 0:
            batch.commit()
            batch = db.batch()

    if count % 400 != 0:
        batch.commit()

    print(f"  ✓ Wrote {len(records)} docs to '{collection}'")


def main() -> None:
    parser = argparse.ArgumentParser(description="ArasiyalAayvu State Macro Ingest")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be written without touching Firestore")
    args = parser.parse_args()

    db = None if args.dry_run else firestore.Client(project=PROJECT_ID)

    collections = [
        ("state_macro", STATE_MACRO),
        ("district_health", DISTRICT_HEALTH),
        ("district_water_risk", DISTRICT_WATER_RISK),
        ("crop_economics", CROP_ECONOMICS),
    ]

    for coll_name, records in collections:
        print(f"\n→ {coll_name} ({len(records)} records)")
        if args.dry_run:
            _upsert_batch(None, coll_name, records, dry_run=True)
        else:
            _upsert_batch(db, coll_name, records, dry_run=False)

    print("\nDone.")


if __name__ == "__main__":
    main()
