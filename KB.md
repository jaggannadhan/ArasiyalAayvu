# ArasiyalAayvu — Data Knowledge Base

This file is the authoritative record of every dataset in the product.
It drives UI decisions, disclaimer labeling, and ingest prioritization.
Updated after every ingest run.

**Last updated:** 2026-04-02 (ward_mapping ingested — 221 docs)
**Maintained by:** Claude (CTO) · Reviewed by: JV (CEO)

---

## How to read this KB

- **Granularity** — the smallest unit at which data is reported: `state` / `district` / `constituency` / `ward`
- **Coverage** — what fraction of 234 TN Assembly Constituencies have this data
- **Confidence** — `HIGH` (official primary source), `MEDIUM` (secondary/derived), `LOW` (estimated/proxy)
- **Status** — `LIVE` (in Firestore, showing in UI) / `INGESTED` (in Firestore, not yet in UI) / `PENDING` (data found, not ingested) / `MISSING` (no source found)

---

## 1. MLA / Candidate Accountability

### 1.1 Base MLA Record
| Field | Value |
|---|---|
| **Collection** | `candidate_accountability` |
| **Doc ID pattern** | `2021_{constituency_slug}` |
| **Coverage** | 231 / 234 ACs (3 missing: alandur base doc absent, vikravandi, vilavancode not in map) |
| **Granularity** | Constituency |
| **Status** | LIVE |
| **Source** | MyNeta / ECI Form 26A affidavits |
| **Source URL** | https://www.myneta.info/tamil-nadu2021/ |
| **Ingested by** | `scrapers/candidate_transparency_ingest.py` |
| **Date ingested** | 2025 (initial) |
| **Confidence** | HIGH |
| **Fields** | `mla_name`, `party`, `party_id`, `constituency`, `constituency_slug`, `criminal_cases_total`, `criminal_severity`, `assets_cr`, `liabilities_cr`, `net_assets_cr`, `is_crorepati`, `education`, `education_tier`, `election_year` |

### 1.2 Asset Breakdown (Movable / Immovable)
| Field | Value |
|---|---|
| **Coverage** | 213 / 234 ACs |
| **Granularity** | Constituency |
| **Status** | LIVE |
| **Source** | ADR Tamil Nadu 2021 Sitting MLAs Report (PDF) |
| **Source URL** | https://adrindia.org/sites/default/files/Tamil_Nadu_Assembly_Election_2021_Sitting_MLAs_Report_Finalver_Eng.pdf |
| **Ingested by** | `scrapers/adr_assets_ingest.py` |
| **Date ingested** | 2026-04-02 |
| **Confidence** | HIGH |
| **Fields** | `movable_assets_cr`, `immovable_assets_cr`, `assets_cr` (recomputed), `is_crorepati` (recomputed), `net_assets_cr` (recomputed) |
| **Gaps** | 4 not in map (VIKRAVANDI, ERODE EAST, VILAVANCODE, KUMARAPALAYAM); 17 had no base doc |

### 1.3 Structured Criminal Cases
| Field | Value |
|---|---|
| **Coverage** | 130 MLAs with declared cases parsed; 4 ACs with cases but no match |
| **Granularity** | Constituency (case-level detail) |
| **Status** | LIVE |
| **Source** | ADR Tamil Nadu 2021 Sitting MLAs Report (PDF) |
| **Source URL** | https://adrindia.org/sites/default/files/Tamil_Nadu_Assembly_Election_2021_Sitting_MLAs_Report_Finalver_Eng.pdf |
| **Ingested by** | `scrapers/adr_criminal_ingest.py` |
| **Date ingested** | 2026-04-02 |
| **Confidence** | HIGH |
| **Fields** | `criminal_cases[]` (`ipc_sections`, `act`, `description`, `status`, `court`, `is_serious`), `criminal_severity` (recomputed from cases) |
| **Gaps** | 18 ACs: parsed count ≠ declared count (UI shows partial-data disclosure); 4 ACs: zero cases parsed |
| **Disclosure** | UI shows amber warning when `cases.length < criminal_cases_total` |

---

## 2. District-Level Socio Metrics

### 2.1 Health Metrics (NFHS-5)
| Field | Value |
|---|---|
| **Collection** | `socio_metrics` |
| **Coverage** | All districts → shown on all 234 constituency pages |
| **Granularity** | **DISTRICT** ⚠️ (shown at constituency level via district lookup) |
| **Status** | LIVE |
| **Source** | NFHS-5 (National Family Health Survey 2019–21) |
| **Source URL** | http://rchiips.org/nfhs/nfhs-5.shtml |
| **Ingested by** | `scrapers/aser_scraper.py` (merged) |
| **Confidence** | HIGH |
| **UI Disclaimer** | "Data reported at district level per State Planning Commission standards" |

### 2.2 Education Metrics (ASER 2024)
| Field | Value |
|---|---|
| **Collection** | `socio_metrics` |
| **Coverage** | All districts |
| **Granularity** | **DISTRICT** ⚠️ |
| **Status** | LIVE |
| **Source** | ASER 2024 (Annual Status of Education Report) |
| **Source URL** | https://asercentre.org/ |
| **Confidence** | HIGH |
| **UI Disclaimer** | "Data reported at district level" |

---

## 3. District Water Risk

| Field | Value |
|---|---|
| **Collection** | `district_water_risk` |
| **Doc ID pattern** | `{district_slug}` |
| **Coverage** | 37 / 37 TN districts |
| **Granularity** | **DISTRICT** ⚠️ |
| **Status** | LIVE |
| **Source** | WRI Aqueduct 4.0 Water Risk Atlas + TWAD Board data |
| **Source URL** | https://www.wri.org/aqueduct |
| **Ingested by** | `scrapers/state_macro_ingest.py` |
| **Date ingested** | 2026 |
| **Confidence** | MEDIUM (Aqueduct scores derived, not direct measurement) |
| **Fields** | `risk_level`, `risk_label_en/ta`, `water_stress_score`, `avg_annual_rainfall_mm`, `context`, `policy_implication` |

---

## 4. District Crime Index

| Field | Value |
|---|---|
| **Collection** | `district_crime_index` |
| **Doc ID pattern** | `{district_slug}` |
| **Coverage** | Partial (see ingest script) |
| **Granularity** | **DISTRICT** ⚠️ |
| **Status** | INGESTED (UI: TenurePulse shows Crime Index row) |
| **Source** | SCRB (State Crime Records Bureau) 2021 via OpenCity |
| **Source URL** | https://data.opencity.in/dataset/tamil-nadu-crime-data |
| **Ingested by** | `scrapers/district_crime_ingest.py` |
| **Confidence** | HIGH |
| **Fields** | `ipc_crime_rate_per_lakh`, `crime_index_level`, `murder_rate_per_lakh`, `rape_rate_per_lakh`, `theft_rate_per_lakh` etc. |

---

## 5. District Road Safety

| Field | Value |
|---|---|
| **Collection** | `district_road_safety` |
| **Doc ID pattern** | `{district_slug}` |
| **Coverage** | Partial (see ingest script) |
| **Granularity** | **DISTRICT** ⚠️ |
| **Status** | INGESTED (UI: TenurePulse shows Road Safety row) |
| **Source** | TN Police Road Accident Data 2021–2023 |
| **Source URL** | https://www.tnpolice.gov.in |
| **Ingested by** | `scrapers/district_road_safety_ingest.py` |
| **Confidence** | MEDIUM |
| **Fields** | `deaths_2023`, `death_rate_per_lakh_2023`, `road_safety_level`, `trend_2021_2023` |

---

## 6. Ward & Local Body Mapping

| Field | Value |
|---|---|
| **Collection** | `ward_mapping` |
| **Doc ID pattern** | `{constituency_slug}` |
| **Coverage** | 221 / 234 ACs (10 fully rural ACs have zero urban wards; 3 not in map: Kumarapalayam, Erode East, Vilavancode) |
| **Granularity** | **CONSTITUENCY** ✅ |
| **Status** | INGESTED (221 docs in Firestore; UI integration pending) |
| **Source** | LGD (Local Government Directory) — Government of India |
| **Source URL** | https://lgdirectory.gov.in |
| **Archive URL** | https://github.com/ramSeraph/opendata/releases/tag/lgd-latest-extra1 |
| **Data date** | 2026-04-02 (daily LGD snapshot) |
| **Ingested by** | `scrapers/ward_mapping_ingest.py` |
| **Date ingested** | 2026-04-02 |
| **Confidence** | HIGH (official GoI directory) |
| **Fields** | `total_urban_wards`, `local_bodies[]` (`name`, `type`, `ward_count`) |
| **10 rural-only ACs** | ECI codes 45, 54, 62, 71, 75, 88, 114, 153, 195, 226 — will show "No urban wards — rural constituency" |
| **3 not-in-map ACs** | Kumarapalayam, Erode East (both missing from constituency-map.json), Vilavancode |

---

## 7. Corporation Zone Counts

| Field | Value |
|---|---|
| **Coverage** | 21 Municipal Corporations (research complete) |
| **Granularity** | Corporation-level (not constituency-level) |
| **Status** | PENDING (data available; not yet ingested — deferred to P2) |
| **Source** | Official corporation websites + Wikipedia |
| **Planned use** | Enrich `local_bodies[]` in `ward_mapping` with zone count |
| **Key data** | Chennai: 15z/200w · Coimbatore: 5z/100w · Madurai: 5z/100w · Trichy: 4z/65w · Tirunelveli: 4z/55w · Salem: 4z/60w · Tiruppur: 4z/60w · Vellore: 4z/60w · Thoothukudi: 4z/60w · Tambaram: 5z/70w · Avadi: 4z/48w · Erode: 4z/60w · Hosur: 4z/45w · Thanjavur: ?z/51w · Dindigul: ?z/48w · Kancheepuram: ?z/51w · Nagercoil: ?z/52w · Cuddalore: ?z/45w · Sivakasi: ?z/33w · Tiruvannamalai: ?z/39w |
| **Note** | Ranipet is a Municipality (30w, no zones), not a Municipal Corporation |

---

## Granularity Summary

| Data | True Granularity | Shown As | Gap |
|---|---|---|---|
| MLA criminal record | Constituency | Constituency | None |
| MLA assets | Constituency | Constituency | None |
| Health (NFHS-5) | District | Constituency* | Weighted avg needed |
| Education (ASER) | District | Constituency* | Weighted avg needed |
| Water Risk | District | Constituency* | Weighted avg needed |
| Crime Index | District | Constituency* | Weighted avg needed |
| Road Safety | District | Constituency* | Weighted avg needed |
| Ward mapping | Constituency | Constituency | None (urban only) |

*Displayed with district-level disclaimer per State Planning Commission standards.
Upgrade path: GIS weighted average using AC population overlap ratios (DataMeet shapefiles + Delimitation Commission data).

---

## Tamil Nadu Government Data Portals

Primary one-stop repositories for official TN state data. All actively maintained as of 2026.

| Portal | URL | What it has |
|---|---|---|
| **State Planning Commission (SPC)** | https://spc.tn.gov.in/reports-2021-2025/ | Annual Economic Survey (most recent: Economic Survey of Tamil Nadu 2025-26, Feb 2026), SDG reports, socio-economic evaluations — best for comprehensive yearly assessments |
| **Dept of Economics & Statistics (DES)** | https://spc.tn.gov.in/ | Official Statistical Handbook, district-wise indicators, seasonal agricultural reports — authoritative source for district-level statistics |
| **TN Open Data Portal** | https://www.tn.gov.in/deptst/ecoindicator.htm | Central repository for departmental datasets under state data-driven governance mission |

**Key notes:**
- Economic Survey 2025-26 provides "nowcasting" of real-time economic indicators — use for current economic data
- SPC is the most reliable source for composite district-level socio-economic indicators
- Upgrade path for P5 (true AC-level socio metrics): SPC district data + GIS weighted average

---

## Upgrade Roadmap

| Priority | Feature | Data Source | Effort |
|---|---|---|---|
| P1 | Ward councillor count per AC | LGD (done above) | Low |
| P2 | Corporation zone count per local body | Corp websites | Low |
| P3 | Pincode → constituency lookup | `pincode_mapping` collection | Medium |
| P4 | ECI affidavit PDF links (source_pdf) | MyNeta scraper | Medium |
| P5 | True AC-level socio metrics | GIS weighted average | High |
| P6 | Rural panchayat ward counts | elections.tn.gov.in PDF parse (234 PDFs) | High |
| P7 | Ward councillor identity data | TNSEC election results | High |
