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
        "doc_id": "pudukkottai",
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
        "doc_id": "kancheepuram",
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
        "doc_id": "vellore",
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
        "doc_id": "thanjavur",
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
        "doc_id": "chennai",
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
        "doc_id": "ramanathapuram",
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
    # ── 31 additional districts (WRI Aqueduct 4.0 + IMD rainfall + CGWB groundwater) ──
    {
        "doc_id": "madurai",
        "district_slug": "madurai",
        "district_name": "Madurai",
        "risk_level": "EXTREMELY_HIGH",
        "risk_label_en": "Extremely High Water Stress",
        "risk_label_ta": "மிகவும் அதிக நீர் அழுத்தம்",
        "water_stress_score": 4.3,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Critical",
        "avg_annual_rainfall_mm": 870,
        "primary_source": "Vaigai River / Groundwater",
        "context": "Madurai depends almost entirely on the seasonal Vaigai river and rapidly depleting groundwater. "
                   "Over 70% of blocks are classified as semi-critical or critical by CGWB. Urban expansion "
                   "has encroached on natural tanks and recharge zones.",
        "policy_implication": "Vaigai rejuvenation, tank restoration, and groundwater regulation are urgent.",
        "source_title": "WRI Aqueduct 4.0; CGWB TN Groundwater Atlas",
        "source_url": "https://www.wri.org/applications/aqueduct/water-risk-atlas/",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "sivaganga",
        "district_slug": "sivaganga",
        "district_name": "Sivaganga",
        "risk_level": "EXTREMELY_HIGH",
        "risk_label_en": "Extremely High Water Stress",
        "risk_label_ta": "மிகவும் அதிக நீர் அழுத்தம்",
        "water_stress_score": 4.4,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Severe",
        "avg_annual_rainfall_mm": 790,
        "primary_source": "Tanks / Rainwater",
        "context": "Sivaganga lies in the rain shadow belt adjacent to Ramanathapuram. "
                   "Tank-fed agriculture is the primary livelihood but over 60% of tanks are silted. "
                   "Groundwater overexploitation is widespread in all blocks.",
        "policy_implication": "Tank desilting, check dam construction, and micro-irrigation incentives needed.",
        "source_title": "WRI Aqueduct 4.0; TN SDMA",
        "source_url": "https://www.wri.org/applications/aqueduct/water-risk-atlas/",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "tiruppur",
        "district_slug": "tiruppur",
        "district_name": "Tiruppur",
        "risk_level": "EXTREMELY_HIGH",
        "risk_label_en": "Extremely High Water Stress (Industrial Pollution)",
        "risk_label_ta": "மிகவும் அதிக நீர் அழுத்தம் (தொழில்துறை மாசு)",
        "water_stress_score": 4.4,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Critical",
        "avg_annual_rainfall_mm": 700,
        "primary_source": "Noyyal River / Bhavani Canal",
        "context": "Tiruppur's textile dyeing industry has severely polluted the Noyyal River — a CPCB "
                   "critically polluted zone. The river was declared biologically dead. Industrial effluents "
                   "have infiltrated shallow aquifers serving 500+ villages downstream.",
        "policy_implication": "ZLD enforcement, CETP upgrades, and alternative water sourcing from Bhavani are critical.",
        "source_title": "CPCB Critically Polluted Areas; WRI Aqueduct 4.0",
        "source_url": "https://cpcb.nic.in/",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "ranipet",
        "district_slug": "ranipet",
        "district_name": "Ranipet",
        "risk_level": "EXTREMELY_HIGH",
        "risk_label_en": "Extremely High Water Stress (Chromium Contamination)",
        "risk_label_ta": "மிகவும் அதிக நீர் அழுத்தம் (குரோமியம் மாசு)",
        "water_stress_score": 4.5,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Critical",
        "avg_annual_rainfall_mm": 880,
        "primary_source": "Palar River / Groundwater",
        "context": "Ranipet (formerly part of Vellore) hosts the largest tannery cluster in Asia. "
                   "CPCB data shows hexavalent chromium contamination in groundwater at 50-100× safe limits. "
                   "The Palar River through Ranipet has been classified as severely polluted since 2015.",
        "policy_implication": "ZLD norms for tanneries, groundwater remediation, and alternative piped water supply are essential.",
        "source_title": "CPCB Critically Polluted Areas Assessment; WRI Aqueduct 4.0",
        "source_url": "https://cpcb.nic.in/",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "thoothukudi",
        "district_slug": "thoothukudi",
        "district_name": "Thoothukudi",
        "risk_level": "HIGH",
        "risk_label_en": "High Water Stress (Coastal Arid)",
        "risk_label_ta": "அதிக நீர் அழுத்தம் (கடலோர வறண்ட பகுதி)",
        "water_stress_score": 4.0,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Severe",
        "avg_annual_rainfall_mm": 650,
        "primary_source": "Tamirabarani Canal / Groundwater",
        "context": "One of TN's driest coastal districts. Salt pans and industrial complexes (Sterlite/Vedanta) "
                   "have caused significant groundwater salinisation. Limited surface water — dependent on "
                   "Tamirabarani river canals from Tirunelveli.",
        "policy_implication": "Desalination plants for industrial water and piped drinking water extension to rural areas.",
        "source_title": "WRI Aqueduct 4.0; CGWB TN",
        "source_url": "https://www.wri.org/applications/aqueduct/water-risk-atlas/",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "dharmapuri",
        "district_slug": "dharmapuri",
        "district_name": "Dharmapuri",
        "risk_level": "HIGH",
        "risk_label_en": "High Water Stress",
        "risk_label_ta": "அதிக நீர் அழுத்தம்",
        "water_stress_score": 3.9,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Severe",
        "avg_annual_rainfall_mm": 840,
        "primary_source": "Cauvery River / Tanks",
        "context": "Dharmapuri is a drought-prone district in the rain shadow of the Western and Eastern Ghats. "
                   "Mango orchards dominate agriculture but depend on rapidly depleting borewell water. "
                   "CGWB classifies 8 of 9 blocks as over-exploited.",
        "policy_implication": "Drip irrigation mandates for horticulture and watershed programmes are critical.",
        "source_title": "CGWB TN Groundwater Year Book 2022; WRI Aqueduct 4.0",
        "source_url": "https://cgwb.gov.in/",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "krishnagiri",
        "district_slug": "krishnagiri",
        "district_name": "Krishnagiri",
        "risk_level": "HIGH",
        "risk_label_en": "High Water Stress",
        "risk_label_ta": "அதிக நீர் அழுத்தம்",
        "water_stress_score": 3.9,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Severe",
        "avg_annual_rainfall_mm": 850,
        "primary_source": "Thenpennai River / Tanks",
        "context": "Krishnagiri shares the semi-arid character of Dharmapuri. The Krishnagiri Reservoir supplies "
                   "irrigation but faces siltation. Over-extraction for mango and tomato cultivation has "
                   "depleted groundwater in most blocks.",
        "policy_implication": "Reservoir desilting, watershed development, and farmer awareness on drip irrigation needed.",
        "source_title": "CGWB TN Groundwater Year Book 2022; WRI Aqueduct 4.0",
        "source_url": "https://cgwb.gov.in/",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "virudhunagar",
        "district_slug": "virudhunagar",
        "district_name": "Virudhunagar",
        "risk_level": "HIGH",
        "risk_label_en": "High Water Stress",
        "risk_label_ta": "அதிக நீர் அழுத்தம்",
        "water_stress_score": 3.8,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Severe",
        "avg_annual_rainfall_mm": 780,
        "primary_source": "Tanks / Groundwater",
        "context": "Virudhunagar (fireworks industry hub) faces acute water scarcity — 85% of its area is "
                   "tank-dependent with most tanks functioning below 40% capacity. The district receives "
                   "low and unreliable rainfall from both monsoons.",
        "policy_implication": "Industrial water audit for fireworks units and tank restoration programme needed.",
        "source_title": "WRI Aqueduct 4.0; TN SDMA",
        "source_url": "https://www.wri.org/applications/aqueduct/water-risk-atlas/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "tiruvannamalai",
        "district_slug": "tiruvannamalai",
        "district_name": "Tiruvannamalai",
        "risk_level": "HIGH",
        "risk_label_en": "High Water Stress",
        "risk_label_ta": "அதிக நீர் அழுத்தம்",
        "water_stress_score": 3.9,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Critical",
        "avg_annual_rainfall_mm": 1000,
        "primary_source": "Arani River / Tanks",
        "context": "Despite moderate rainfall, Tiruvannamalai's rugged terrain causes high runoff and poor "
                   "recharge. CGWB classifies most blocks as over-exploited. The Arani river is seasonal "
                   "and heavily silted, reducing surface water availability.",
        "policy_implication": "Percolation pond construction in rocky terrain and groundwater extraction regulation needed.",
        "source_title": "CGWB TN Groundwater Year Book 2022; WRI Aqueduct 4.0",
        "source_url": "https://cgwb.gov.in/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "thiruvallur",
        "district_slug": "thiruvallur",
        "district_name": "Thiruvallur",
        "risk_level": "HIGH",
        "risk_label_en": "High Water Stress (Peri-Urban)",
        "risk_label_ta": "அதிக நீர் அழுத்தம் (நகர் ஓரப்பகுதி)",
        "water_stress_score": 3.8,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Severe",
        "avg_annual_rainfall_mm": 1100,
        "primary_source": "Kosasthalaiyar River / Chennai Metro Supply",
        "context": "Chennai's industrial expansion into Thiruvallur (Sricity, TIDEL, auto clusters) has rapidly "
                   "depleted groundwater. The Kosasthalaiyar river is polluted from industrial effluents. "
                   "Many panchayats rely on CMWSSB tanker supply.",
        "policy_implication": "Mandatory industrial effluent treatment and extension of piped metro water supply needed.",
        "source_title": "WRI Aqueduct 4.0; CGWB TN",
        "source_url": "https://www.wri.org/applications/aqueduct/water-risk-atlas/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "chengalpattu",
        "district_slug": "chengalpattu",
        "district_name": "Chengalpattu",
        "risk_level": "HIGH",
        "risk_label_en": "High Water Stress (Peri-Urban Industrial)",
        "risk_label_ta": "அதிக நீர் அழுத்தம் (நகர் ஓரப் தொழிலகப் பகுதி)",
        "water_stress_score": 3.8,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Severe",
        "avg_annual_rainfall_mm": 1200,
        "primary_source": "Palar River / Tanks / Chennai Supply",
        "context": "Chengalpattu (split from Kancheepuram in 2019) hosts auto and electronics industries "
                   "around Mahindra World City and TIDEL Park. Rapid urbanisation has encroached on "
                   "natural tank systems. Groundwater quality is compromised by industrial discharge.",
        "policy_implication": "Industrial water audits, effluent ZLD, and restoration of Oragadam tank network needed.",
        "source_title": "WRI Aqueduct 4.0; CGWB TN",
        "source_url": "https://www.wri.org/applications/aqueduct/water-risk-atlas/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "salem",
        "district_slug": "salem",
        "district_name": "Salem",
        "risk_level": "HIGH",
        "risk_label_en": "High Water Stress",
        "risk_label_ta": "அதிக நீர் அழுத்தம்",
        "water_stress_score": 3.8,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Severe",
        "avg_annual_rainfall_mm": 900,
        "primary_source": "Mettur Dam / Cauvery Canal",
        "context": "Despite proximity to Mettur Dam, Salem city and peri-urban areas face severe groundwater "
                   "depletion from steel and textile industries. The Thirumanimuthar river (tributary of "
                   "Cauvery) has reduced flow. 7 of 11 blocks classified as over-exploited by CGWB.",
        "policy_implication": "Industrial water recycling, groundwater regulation, and canal recharge structures needed.",
        "source_title": "CGWB TN Groundwater Year Book 2022; WRI Aqueduct 4.0",
        "source_url": "https://cgwb.gov.in/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "perambalur",
        "district_slug": "perambalur",
        "district_name": "Perambalur",
        "risk_level": "HIGH",
        "risk_label_en": "High Water Stress",
        "risk_label_ta": "அதிக நீர் அழுத்தம்",
        "water_stress_score": 3.7,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Severe",
        "avg_annual_rainfall_mm": 900,
        "primary_source": "Vembaru River / Tanks",
        "context": "Perambalur is a small, semi-arid district dependent on tank irrigation. All three blocks "
                   "are classified as over-exploited by CGWB. The limestone quarrying industry further "
                   "disrupts natural recharge pathways.",
        "policy_implication": "Tank restoration and quarry water management needed to prevent further depletion.",
        "source_title": "CGWB TN Groundwater Year Book 2022; WRI Aqueduct 4.0",
        "source_url": "https://cgwb.gov.in/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "dindigul",
        "district_slug": "dindigul",
        "district_name": "Dindigul",
        "risk_level": "HIGH",
        "risk_label_en": "High Water Stress",
        "risk_label_ta": "அதிக நீர் அழுத்தம்",
        "water_stress_score": 3.6,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Severe",
        "avg_annual_rainfall_mm": 820,
        "primary_source": "Vaigai River / Tanks",
        "context": "Dindigul district straddles the Palani Hills and the semi-arid plains. The upper areas "
                   "receive better rainfall but the plains blocks (Nilakottai, Vedasandur) face severe "
                   "groundwater depletion. Banana and cotton cultivation are major water users.",
        "policy_implication": "Crop diversification towards drought-tolerant varieties and drip irrigation needed.",
        "source_title": "CGWB TN Groundwater Year Book 2022; WRI Aqueduct 4.0",
        "source_url": "https://cgwb.gov.in/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "ariyalur",
        "district_slug": "ariyalur",
        "district_name": "Ariyalur",
        "risk_level": "HIGH",
        "risk_label_en": "High Water Stress",
        "risk_label_ta": "அதிக நீர் அழுத்தம்",
        "water_stress_score": 3.5,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Critical",
        "avg_annual_rainfall_mm": 900,
        "primary_source": "Cauvery Canal / Tanks",
        "context": "Ariyalur is in the Cauvery-Kollidam belt with access to canal irrigation, but cement "
                   "quarrying (one of TN's largest cement manufacturing hubs) disrupts groundwater "
                   "recharge. Semi-arid blocks in the west face water scarcity during summer months.",
        "policy_implication": "Quarry water harvesting and groundwater recharge structures near cement belts needed.",
        "source_title": "CGWB TN; WRI Aqueduct 4.0",
        "source_url": "https://cgwb.gov.in/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "kallakurichi",
        "district_slug": "kallakurichi",
        "district_name": "Kallakurichi",
        "risk_level": "HIGH",
        "risk_label_en": "High Water Stress",
        "risk_label_ta": "அதிக நீர் அழுத்தம்",
        "water_stress_score": 3.5,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Severe",
        "avg_annual_rainfall_mm": 980,
        "primary_source": "Gomuki River / Tanks",
        "context": "Kallakurichi (carved from Villuppuram in 2019) has tribal hill communities in the "
                   "Kalrayan Hills who lack access to safe drinking water. Lowland blocks suffer "
                   "from groundwater depletion linked to sugarcane cultivation.",
        "policy_implication": "Piped drinking water schemes for tribal habitations and sugarcane water audit needed.",
        "source_title": "CGWB TN; WRI Aqueduct 4.0",
        "source_url": "https://cgwb.gov.in/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "tirupathur",
        "district_slug": "tirupathur",
        "district_name": "Tirupathur",
        "risk_level": "HIGH",
        "risk_label_en": "High Water Stress",
        "risk_label_ta": "அதிக நீர் அழுத்தம்",
        "water_stress_score": 3.7,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Severe",
        "avg_annual_rainfall_mm": 950,
        "primary_source": "Palar River / Tanks",
        "context": "Tirupathur (split from Vellore in 2019) shares the semi-arid character of the Palar "
                   "basin. Mango, mulberry, and sericulture zones rely on borewell irrigation. "
                   "Most blocks are classified as over-exploited by CGWB.",
        "policy_implication": "Micro-watershed development and drip irrigation for horticulture crops needed.",
        "source_title": "CGWB TN; WRI Aqueduct 4.0",
        "source_url": "https://cgwb.gov.in/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "coimbatore",
        "district_slug": "coimbatore",
        "district_name": "Coimbatore",
        "risk_level": "MEDIUM",
        "risk_label_en": "Medium Water Stress",
        "risk_label_ta": "நடுத்தர நீர் அழுத்தம்",
        "water_stress_score": 3.5,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Critical",
        "avg_annual_rainfall_mm": 680,
        "primary_source": "Bhavani River / Pillur Dam",
        "context": "Coimbatore's textile and engineering industries are major water consumers. The eastern "
                   "blocks (Pollachi side) receive better rainfall from the Western Ghats, while the city "
                   "and industrial zones face water stress. The Noyyal river is highly polluted "
                   "downstream from bleaching and dyeing units.",
        "policy_implication": "Tertiary treatment and industrial water reuse, Noyyal clean-up programme.",
        "source_title": "CGWB TN; WRI Aqueduct 4.0",
        "source_url": "https://cgwb.gov.in/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "erode",
        "district_slug": "erode",
        "district_name": "Erode",
        "risk_level": "MEDIUM",
        "risk_label_en": "Medium Water Stress",
        "risk_label_ta": "நடுத்தர நீர் அழுத்தம்",
        "water_stress_score": 3.2,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Semi-Critical",
        "avg_annual_rainfall_mm": 750,
        "primary_source": "Cauvery / Bhavani Rivers",
        "context": "Erode benefits from the Cauvery and Bhavani rivers via Mettur Dam releases. "
                   "Turmeric and banana cultivation are water-intensive. The bleaching and dyeing "
                   "industry (Erode textile hub) pollutes the Bhavani river downstream.",
        "policy_implication": "CETP upgrades for textile units and irrigation scheduling from Mettur releases.",
        "source_title": "WRI Aqueduct 4.0; CGWB TN",
        "source_url": "https://www.wri.org/applications/aqueduct/water-risk-atlas/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "namakkal",
        "district_slug": "namakkal",
        "district_name": "Namakkal",
        "risk_level": "MEDIUM",
        "risk_label_en": "Medium Water Stress",
        "risk_label_ta": "நடுத்தர நீர் அழுத்தம்",
        "water_stress_score": 3.4,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Semi-Critical",
        "avg_annual_rainfall_mm": 850,
        "primary_source": "Cauvery Canal / Tanks",
        "context": "Namakkal district (poultry and lorry transport hub) has medium water stress. "
                   "The Salem-Erode Cauvery canal network provides some irrigation. "
                   "Poultry industry wastewater contamination affects groundwater in several blocks.",
        "policy_implication": "Poultry effluent regulation and groundwater monitoring needed.",
        "source_title": "WRI Aqueduct 4.0; CGWB TN",
        "source_url": "https://www.wri.org/applications/aqueduct/water-risk-atlas/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "karur",
        "district_slug": "karur",
        "district_name": "Karur",
        "risk_level": "MEDIUM",
        "risk_label_en": "Medium Water Stress",
        "risk_label_ta": "நடுத்தர நீர் அழுத்தம்",
        "water_stress_score": 3.3,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Semi-Critical",
        "avg_annual_rainfall_mm": 800,
        "primary_source": "Cauvery River / Grand Anicut Canal",
        "context": "Karur (textiles and bus body building hub) lies on the Cauvery and benefits from "
                   "Grand Anicut irrigation. Dyeing and bleaching units have caused Cauvery river "
                   "pollution. Groundwater quality issues from industrial effluents in Karur town.",
        "policy_implication": "Industrial effluent treatment and river buffer zone protection needed.",
        "source_title": "WRI Aqueduct 4.0; CGWB TN",
        "source_url": "https://www.wri.org/applications/aqueduct/water-risk-atlas/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "tiruchirappalli",
        "district_slug": "tiruchirappalli",
        "district_name": "Tiruchirappalli",
        "risk_level": "MEDIUM",
        "risk_label_en": "Medium Water Stress",
        "risk_label_ta": "நடுத்தர நீர் அழுத்தம்",
        "water_stress_score": 3.3,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Semi-Critical",
        "avg_annual_rainfall_mm": 900,
        "primary_source": "Cauvery River / Kollidam",
        "context": "Trichy sits at the bifurcation of Cauvery and Kollidam and has historically good "
                   "surface water. Urban growth and industrial expansion (BHEL, ordnance factories) "
                   "have increased groundwater demand. Southern dry blocks face moderate water stress.",
        "policy_implication": "Urban groundwater regulation and wastewater recycling for industries.",
        "source_title": "WRI Aqueduct 4.0; CGWB TN",
        "source_url": "https://www.wri.org/applications/aqueduct/water-risk-atlas/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "theni",
        "district_slug": "theni",
        "district_name": "Theni",
        "risk_level": "MEDIUM",
        "risk_label_en": "Medium Water Stress",
        "risk_label_ta": "நடுத்தர நீர் அழுத்தம்",
        "water_stress_score": 3.3,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Semi-Critical",
        "avg_annual_rainfall_mm": 950,
        "primary_source": "Vaigai Reservoir / Mullaperiyar",
        "context": "Theni district has the Vaigai reservoir headwaters (Mullaperiyar / Vaigai Dam) and "
                   "benefits from the Kerala water surplus via inter-state sharing. Grapes, banana, and "
                   "banana chip agro-industry are major water users. Urban Theni town has adequate supply.",
        "policy_implication": "Maintain equitable Mullaperiyar water sharing; expand micro-irrigation for horticulture.",
        "source_title": "WRI Aqueduct 4.0; CWC Inter-State River Data",
        "source_url": "https://www.wri.org/applications/aqueduct/water-risk-atlas/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "tirunelveli",
        "district_slug": "tirunelveli",
        "district_name": "Tirunelveli",
        "risk_level": "MEDIUM",
        "risk_label_en": "Medium Water Stress",
        "risk_label_ta": "நடுத்தர நீர் அழுத்தம்",
        "water_stress_score": 3.1,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Semi-Critical",
        "avg_annual_rainfall_mm": 760,
        "primary_source": "Tamirabarani River / Papanasam Dam",
        "context": "Tirunelveli is fortunate to have the perennial Tamirabarani river — one of TN's "
                   "few year-round flowing rivers, fed by Western Ghats. However, downstream districts "
                   "(Thoothukudi) face water scarcity from over-abstraction. Urban growth and "
                   "industrial demand are increasing pressure.",
        "policy_implication": "Tamirabarani flow maintenance, sand mining regulation, and equitable sharing with Thoothukudi.",
        "source_title": "WRI Aqueduct 4.0; CWC",
        "source_url": "https://www.wri.org/applications/aqueduct/water-risk-atlas/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "cuddalore",
        "district_slug": "cuddalore",
        "district_name": "Cuddalore",
        "risk_level": "MEDIUM",
        "risk_label_en": "Medium Water Stress (Industrial Pollution Risk)",
        "risk_label_ta": "நடுத்தர நீர் அழுத்தம் (தொழில்துறை மாசு அபாயம்)",
        "water_stress_score": 2.8,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Semi-Critical",
        "avg_annual_rainfall_mm": 1250,
        "primary_source": "Pennaiar River / Vadavar River",
        "context": "Cuddalore receives good northeast monsoon rainfall but hosts SIPCOT Cuddalore (one of "
                   "CPCB's critically polluted areas). Chemical and pharmaceutical industries have "
                   "contaminated groundwater near Cuddalore town. Coastal zones face saltwater intrusion.",
        "policy_implication": "SIPCOT industrial effluent treatment, coastal aquifer protection, and mangrove restoration.",
        "source_title": "CPCB Critically Polluted Areas; WRI Aqueduct 4.0",
        "source_url": "https://cpcb.nic.in/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "villuppuram",
        "district_slug": "villuppuram",
        "district_name": "Villuppuram",
        "risk_level": "MEDIUM",
        "risk_label_en": "Medium Water Stress",
        "risk_label_ta": "நடுத்தர நீர் அழுத்தம்",
        "water_stress_score": 3.2,
        "water_stress_max": 5.0,
        "groundwater_depletion": "Semi-Critical",
        "avg_annual_rainfall_mm": 1050,
        "primary_source": "Ponnaiyar River / Tanks",
        "context": "Villuppuram is TN's largest district by area with mixed terrain — coastal plains, "
                   "inland semi-arid zones, and Eastern Ghats foothills. Ponnaiyar river is seasonal "
                   "and dependent on northeast monsoon. Agricultural groundwater use is heavy in paddy zones.",
        "policy_implication": "Tank restoration and conjunctive use of surface and groundwater in dry blocks.",
        "source_title": "WRI Aqueduct 4.0; CGWB TN",
        "source_url": "https://www.wri.org/applications/aqueduct/water-risk-atlas/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "nagapattinam",
        "district_slug": "nagapattinam",
        "district_name": "Nagapattinam",
        "risk_level": "FLOOD_PRONE",
        "risk_label_en": "Flood-Prone (Cyclone & Delta Region)",
        "risk_label_ta": "வெள்ள அபாயம் (புயல் மற்றும் டெல்டா பகுதி)",
        "water_stress_score": 1.4,
        "water_stress_max": 5.0,
        "avg_annual_rainfall_mm": 1350,
        "primary_source": "Cauvery Delta / Kollidam",
        "context": "Nagapattinam is the most cyclone-exposed district in TN — it bore the brunt of the "
                   "2004 Tsunami and multiple cyclones (Gaja, Vardah). The Cauvery delta provides abundant "
                   "surface water. The district's risk is from flooding and storm surges, not drought.",
        "policy_implication": "Mangrove restoration, cyclone shelter upgrades, and early warning systems are priorities.",
        "source_title": "TN SDMA Cyclone Risk Atlas; WRI Aqueduct 4.0",
        "source_url": "https://sdma.tn.gov.in/",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "thiruvarur",
        "district_slug": "thiruvarur",
        "district_name": "Thiruvarur",
        "risk_level": "FLOOD_PRONE",
        "risk_label_en": "Flood-Prone (Cauvery Delta)",
        "risk_label_ta": "வெள்ள அபாயம் (காவிரி டெல்டா)",
        "water_stress_score": 1.3,
        "water_stress_max": 5.0,
        "avg_annual_rainfall_mm": 1050,
        "primary_source": "Cauvery / Grand Anicut",
        "context": "Thiruvarur lies at the heart of the Cauvery delta. Like Thanjavur and Nagapattinam, "
                   "it faces periodic flooding from erratic Mettur releases and intense northeast monsoon. "
                   "Paddy yields fluctuate sharply due to flood-drought cycles in successive seasons.",
        "policy_implication": "Flood modelling, delta-specific crop insurance, and drainage channel maintenance.",
        "source_title": "TN SDMA; WRI Aqueduct 4.0",
        "source_url": "https://sdma.tn.gov.in/",
        "ground_truth_confidence": "HIGH",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "tenkasi",
        "district_slug": "tenkasi",
        "district_name": "Tenkasi",
        "risk_level": "LOW",
        "risk_label_en": "Low Water Stress (Western Ghats)",
        "risk_label_ta": "குறைந்த நீர் அழுத்தம் (மேற்குத் தொடர்ச்சி மலை)",
        "water_stress_score": 2.0,
        "water_stress_max": 5.0,
        "avg_annual_rainfall_mm": 1250,
        "primary_source": "Chittar River / Western Ghats Streams",
        "context": "Tenkasi (carved from Tirunelveli in 2019) receives good rainfall from the Western "
                   "Ghats. The Chittar and Manimuthar rivers provide surface water. Water stress is "
                   "localised to eastern lowland blocks during summer.",
        "policy_implication": "Protect Western Ghats forest cover which sustains river flows; regulate sand mining.",
        "source_title": "WRI Aqueduct 4.0; IMD",
        "source_url": "https://www.wri.org/applications/aqueduct/water-risk-atlas/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "kanniyakumari",
        "district_slug": "kanniyakumari",
        "district_name": "Kanniyakumari",
        "risk_level": "LOW",
        "risk_label_en": "Low Water Stress (Tri-Sea Junction)",
        "risk_label_ta": "குறைந்த நீர் அழுத்தம் (முக்கடல் சங்கமம்)",
        "water_stress_score": 1.8,
        "water_stress_max": 5.0,
        "avg_annual_rainfall_mm": 1800,
        "primary_source": "Thamirabarani Headwaters / Kodayar Project",
        "context": "Kanniyakumari receives both the southwest and northeast monsoons and has high annual "
                   "rainfall. The Kodayar and Pechiparai reservoirs provide reliable irrigation. "
                   "The district is largely free from water stress — key concern is coastal salinisation.",
        "policy_implication": "Protect backwater and coastal wetlands from salinity intrusion due to sea-level rise.",
        "source_title": "WRI Aqueduct 4.0; IMD",
        "source_url": "https://www.wri.org/applications/aqueduct/water-risk-atlas/",
        "ground_truth_confidence": "MEDIUM",
        "_uploaded_at": NOW_ISO,
    },
    {
        "doc_id": "the_nilgiris",
        "district_slug": "the_nilgiris",
        "district_name": "The Nilgiris",
        "risk_level": "LOW",
        "risk_label_en": "Low Water Stress (High-Altitude Rainforest)",
        "risk_label_ta": "குறைந்த நீர் அழுத்தம் (மலை மழைக்காட்டு பகுதி)",
        "water_stress_score": 1.5,
        "water_stress_max": 5.0,
        "avg_annual_rainfall_mm": 2000,
        "primary_source": "Ooty Lake / Pykara River",
        "context": "The Nilgiris receives over 2000mm of rainfall and is a critical water catchment for "
                   "TN — headwaters of the Bhavani and Moyar rivers originate here. "
                   "Key risks are landslides and runoff water quality from tea estate pesticides.",
        "policy_implication": "Protect shola forests; regulate pesticide use in tea estates; manage eco-tourism water demand.",
        "source_title": "WRI Aqueduct 4.0; IMD",
        "source_url": "https://www.wri.org/applications/aqueduct/water-risk-atlas/",
        "ground_truth_confidence": "MEDIUM",
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
