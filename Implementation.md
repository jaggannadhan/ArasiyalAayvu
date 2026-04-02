# Naatu Nadappu — Implementation Reference

**Project:** Tamil Nadu Election Awareness Web App (Mobile + Desktop)
**GCP Project:** `naatunadappu` (Project No: 301895032269)
**Firestore Region:** `asia-south1`
**Python Runtime:** 3.14.3 (macOS local dev) / 3.12 (Cloud Run target)
**Last updated:** 2026-04-01 (Session 7 — B.4 Pincode-to-Constituency Resolver)

---

## App Objective

A public-facing web app (mobile + desktop) that presents **verified, sourced information** about Tamil Nadu's state elections, governance, and socio-economic outcomes to the general public. All data stored in Firestore carries a `source_url`, `ground_truth_confidence`, and `_uploaded_at` audit field. No opinion, no unverified claim reaches the UI.

---

## Status Dashboard — All Collections

| # | Module | Collection | Status | Docs Live | Source |
|---|---|---|---|---|---|
| 1 | Political Parties | `political_parties` | **LIVE** | 12 | ECI (curated) |
| 2 | Leaders | `leaders` | **LIVE** | 7 | Curated |
| 3 | Chief Ministers | `chief_ministers` | **LIVE** | 20 | TN Assembly (curated) |
| 4 | Assembly Elections | `assembly_elections` | **LIVE** | 16 | CEO TN (curated) |
| 5 | Alliance Matrix | `alliances` | **LIVE** | 28 | Curated |
| 6 | Welfare Achievements | `achievements` | **LIVE** | 6 | Curated |
| 7 | State Finances | `state_finances` | **LIVE** | 5 | PRS India PDFs |
| 8 | Debt History | `debt_history` | **LIVE** | 7 | PRS India + curated series |
| 9 | Departmental Spending | `departmental_spending` | **LIVE** | varies | PRS India |
| 10 | Socio-Economic Metrics | `socio_economics` | **LIVE** | 15 | NFHS-5, SDG Index, ASER 2024, AISHE, DPIIT |
| 11 | MLA Accountability | `candidate_accountability` | **LIVE** | 225 | MyNeta/ADR TN 2021 (224 MLAs + 1 summary) |
| 12 | Party Accountability | `party_accountability` | **LIVE** | 8 | Derived from MyNeta |
| 13 | Manifesto Tracker | `manifesto_promises` | **LIVE** | 13 | DMK/AIADMK 2021 manifestos (curated seed) |
| 14 | Candidate Transparency | `candidate_transparency` | **LIVE** | 3,578 | MyNeta TN 2021 — all candidates (238 constituencies) |
| 15 | View Counters | `usage_counters` | **LIVE** | grows | Client-side page-view increment (Firestore `Increment`) |
| 16 | State Macro — Economy | `state_macro` | **SEEDED** (script ready) | 6 | GSDP growth, sectoral shares, debt ratio, TASMAC |
| 17 | District Health | `district_health` | **SEEDED** (script ready) | 6 | NCD prevalence: Hypertension 31.4%, Diabetes 16.8% |
| 18 | District Water Risk | `district_water_risk` | **SEEDED** (script ready) | 6 | Risk scores: Pudukottai 4.8, Ramanathapuram 4.7 |
| 19 | Crop Economics | `crop_economics` | **SEEDED** (script ready) | 6 | MSP data: Paddy ₹2,300/qtl, Sugarcane FRP ₹340/qtl |
| 20 | Pincode Mapping | `pincode_mapping` | **SEEDED** (script ready) | ~183 | 6-digit pincode → Assembly Constituency (ambiguity-aware) |

**Total: 20 collections defined. 14 fully live; 6 seeded via ingest scripts (run to populate).**

---

## Project Structure

```
NaatuNadappu/
├── scrapers/
│   ├── ceo_tn_scraper.py              # CEO TN — BLOCKED (govt TLS), falls back to curated
│   ├── assembly_scraper.py            # TN Assembly CM list — BLOCKED (govt TLS), falls back to curated
│   ├── eci_scraper.py                 # ECI party list — BLOCKED (govt TLS), falls back to curated
│   ├── prs_scraper.py                 # PRS India budget PDFs — WORKING
│   ├── tn_budget_scraper.py           # Manual-Link PDF utility — WORKING
│   ├── myneta_scraper.py              # MyNeta TN 2021 MLA winners — WORKING (two-stage)
│   ├── aser_scraper.py                # ASER 2024 India state PDF — WORKING
│   ├── candidate_transparency_ingest.py  # MyNeta all-candidates (Level 1+2) — STANDALONE
│   ├── state_macro_ingest.py          # State macro/health/water/crops seed — STANDALONE
│   └── pincode_ingest.py              # TN pincode→constituency mapping — STANDALONE (~183 pincodes)
├── transformers/
│   ├── election_transformer.py        # Election data + alliance matrix — LIVE
│   ├── finance_transformer.py         # Budget/debt/viz metrics — LIVE
│   ├── socio_transformer.py           # ASER merge + enrollment extras — LIVE
│   └── accountability_transformer.py  # MLA severity + party rollups — LIVE
├── loaders/
│   └── firestore_loader.py            # Batched Firestore upsert — LIVE
├── data/
│   ├── raw/
│   │   ├── socio/
│   │   │   └── ASER_2024_India_State_Report.pdf   # Cached (282 KB)
│   │   ├── ceo_tn_raw.json        # Empty (scraper blocked)
│   │   ├── chief_ministers.json   # Curated fallback — 20 records
│   │   └── eci_parties.json       # Curated fallback — 10 records
│   └── processed/
│       ├── parties.json                                # UPLOADED
│       ├── leaders.json                                # UPLOADED
│       ├── elections.json                              # UPLOADED
│       ├── alliances.json                              # UPLOADED
│       ├── achievements.json                           # UPLOADED
│       ├── state_finances.json                         # UPLOADED
│       ├── debt_history.json                           # UPLOADED
│       ├── departmental_spending.json                  # UPLOADED
│       ├── socio_economics.json                        # Curated base (13 metrics)
│       ├── socio_economics_final.json                  # Merged with ASER live data (15 metrics) — UPLOADED
│       ├── mla_winners.json                            # 224 enriched MLA records — UPLOADED
│       ├── party_accountability.json                   # 8 party rollups — UPLOADED
│       ├── assembly_accountability_summary.json        # Assembly-level summary — UPLOADED
│       ├── manifesto_promises_seed.json                # 13 atomic promise records — UPLOADED
│       └── candidate_transparency.json                 # Output of standalone scraper (not yet uploaded)
├── web/                               # Next.js 15 frontend
│   ├── src/
│   │   ├── app/
│   │   │   └── manifesto-tracker/
│   │   │       └── page.tsx           # Manifesto Tracker page — LIVE at /manifesto-tracker
│   │   ├── components/
│   │   │   └── manifesto/
│   │   │       ├── ComparisonMatrix.tsx
│   │   │       ├── PromiseCard.tsx
│   │   │       ├── PromiseSkeleton.tsx   # Loading skeletons (PromiseCardSkeleton, ComparisonSkeleton)
│   │   │       ├── VerificationPanel.tsx
│   │   │       ├── StanceLabel.tsx
│   │   │       ├── PillarTabs.tsx
│   │   │       └── PartySelector.tsx
│   │   ├── hooks/
│   │   │   └── useManifestos.ts       # Firestore onSnapshot hook with yearFilter
│   │   └── lib/
│   │       ├── firebase.ts            # Firebase singleton (Client SDK, env vars only)
│   │       ├── api-client.ts          # `apiGet<T>()` — fetch wrapper for FastAPI backend
│   │       ├── constituency-fetcher.ts # `fetchConstituencyDrillData()` — full drill response type
│   │       ├── constituency-map.json  # 234 assembly slugs → {name, district, constituency_id}
│   │       ├── ls-constituency-map.json  # 39 LS constituencies → assembly_slugs (98.7% coverage)
│   │       ├── types.ts               # All TypeScript types (ManifestoPromise, PARTIES, PILLARS, + new C.1 types)
│   │       └── manifesto-data.ts      # 30 curated promise records (static seed — kept as fallback reference)
│   ├── package.json                   # next@15.5.14, react@19, firebase@12, tailwindcss@4
│   ├── next.config.ts
│   └── CLAUDE.md → AGENTS.md
├── main.py                            # Pipeline entrypoint — all tasks wired
├── requirements.txt
├── Implementation.md                  # This file
└── Implementation_Finance.md          # Finance module deep-dive
```

---

## Environment Setup

```bash
# Prerequisites: Python 3.14, gcloud CLI authenticated
gcloud config set project naatunadappu
gcloud auth application-default login   # browser auth — sets quota project automatically

python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

**`requirements.txt`**
```
requests
beautifulsoup4
lxml
pydantic
google-cloud-firestore
google-cloud-storage
tqdm
python-dotenv
tenacity
tabula-py
pdfminer.six
pdfplumber
```

---

## Running the Pipeline

```bash
# Static/curated political data
.venv/bin/python main.py --task static

# Full political history pipeline (scrape → transform → upload)
.venv/bin/python main.py --task all

# Finance pipeline (PRS India PDFs → transform → upload)
.venv/bin/python main.py --task finance

# Finance — specific years only
.venv/bin/python main.py --task finance --years 2025-26 2024-25

# Manual-link utility (for GoTN Budget PDFs downloaded manually)
.venv/bin/python main.py --task manual-pdf --pdf /path/to/file.pdf --year 2025-26 --upload

# Socio-economics (ASER scrape + curated NFHS/SDG merge + upload)
.venv/bin/python main.py --task socio

# Candidate accountability (MyNeta winners scrape + transform + upload)
.venv/bin/python main.py --task accountability

# Both awareness pipelines together
.venv/bin/python main.py --task awareness

# Manifesto Tracker seed data upload
.venv/bin/python main.py --task manifesto
```

### Standalone: Candidate Transparency Ingestion

`candidate_transparency_ingest.py` runs independently (not wired into `main.py`):

```bash
# Scrape all 2021 TN candidates (Level 1 list + Level 2 detail) and upload
.venv/bin/python scrapers/candidate_transparency_ingest.py --year 2021 --upload

# Save to JSON only (no upload)
.venv/bin/python scrapers/candidate_transparency_ingest.py --year 2021 \
  --output data/processed/candidate_transparency.json

# Generate 2026 constituency placeholder docs from 2021 constituency list
.venv/bin/python scrapers/candidate_transparency_ingest.py --year 2026 \
  --placeholders-only --upload
```

### Frontend (web/)

```bash
cd web
npm install
npm run dev        # http://localhost:3000
# Routes: /manifesto-tracker
```

**Node version requirement:** Node 25.x (system). Next.js 15.5.14 (downgraded from 16 — see Known Issue 5).

---

## Known Issues

### Issue 1 — Government Website TLS Failures (BLOCKING for live scraping)

**Affected scrapers:** `ceo_tn_scraper.py`, `assembly_scraper.py`, `eci_scraper.py`

All Tamil Nadu and ECI government domains drop the TCP connection mid-TLS handshake:

```
ssl.SSLEOFError: [SSL: UNEXPECTED_EOF_WHILE_READING]
EOF occurred in violation of protocol
```

**Domains blocked:** `elections.tn.gov.in`, `assembly.tn.gov.in`, `eci.gov.in`, `finance.tn.gov.in`

**What was tried:** `verify=False`, custom `HTTPAdapter` with `OP_LEGACY_SERVER_CONNECT + SECLEVEL=1`, `curl --tlsv1.0 -k` — all failed. Server-side misconfiguration; no client-side fix possible.

**Resolution:** All three scrapers catch the exception and fall back to manually curated `data/raw/*.json` files marked `ground_truth_confidence: HIGH`.

**Finance portal workaround:** `tn_budget_scraper.py` provides a Manual-Link utility — download PDFs manually from `finance.tn.gov.in` and point the CLI at the local file.

---

### Issue 2 — PyMuPDF Not Available on Python 3.14

**Symptom:** `pip install PyMuPDF` fails — no wheel for Python 3.14.x.

**Resolution:** Using `pdfplumber` (primary for ASER), `tabula-py` (primary for PRS), `pdfminer.six` (fallback). All three are installed and working.

---

### Issue 3 — PRS 2020-21 PDF Corrupt

**Symptom:** `pdfminer.pdfparser.PDFSyntaxError: No /Root object!` on the 2020-21 PRS PDF.

**Resolution:** `_is_valid_pdf()` pre-check + try/except around `parse_prs_pdf()`. Year skipped gracefully; 2021-22 through 2025-26 fully parsed. 2020-21 data is available via curated `debt_history` series.

---

### Issue 4 — MyNeta Winners Table Missing Every 9th Row

**Root cause:** MyNeta's server injects an obfuscated ad `<script>` tag between every 9th table `<tr>`, causing 24 of 224 winner rows to be absent from the parsed HTML. Not a pagination issue — a server-side display quirk.

**Resolution (two-stage scraper in `myneta_scraper.py`):**
1. `scrape_winners()` — main winners table, gets ~200 records
2. Gap detection: if `len(winners) < summary_total`, triggers fallback
3. `scrape_missing_via_constituencies(known)` — iterates `constituency_id=1..234`, scrapes each page, identifies winner by the `"Winner"` suffix MyNeta appends to the name, extracts constituency from `<h3>` heading
4. Merges and deduplicates; all 224 winners recovered

---

### Issue 5 — Next.js 16 + Node 25 Turbopack Panic

**Symptom:** `TurbopackInternalError: Invalid distDirRoot: "". distDirRoot should not navigate out of the projectPath.`

**Cause:** Turbopack in Next.js 16.2.2 computes `distDirRoot` relative to `projectPath`; on Node 25.x this yields an empty string. All config workarounds (`distDir`, `turbopack.root`, ESM `import.meta.url`) failed.

**Resolution:** Downgraded to `next@15.5.14` + `eslint-config-next@15.5.14`. Next.js 15 uses webpack by default (no Turbopack). Dev server verified working: `GET /manifesto-tracker 200`.

---

## Firestore Schema — Module 1: Political History

### Collection: `political_parties`
**Document ID:** `party_id` (e.g. `dmk`, `aiadmk`)

| Field | Type | Example |
|---|---|---|
| `party_id` | string | `"dmk"` |
| `full_name` | string | `"Dravida Munnetra Kazhagam"` |
| `tamil_name` | string | `"திராவிட முன்னேற்றக் கழகம்"` |
| `abbreviation` | string | `"DMK"` |
| `founded_date` | string (ISO) | `"1949-09-17"` |
| `founded_by` | string[] | `["C. N. Annadurai"]` |
| `ideology` | string[] | `["Dravidian nationalism"]` |
| `parent_org` | string\|null | `"dk"` |
| `eci_recognized_status` | string | `"State Party"` |
| `eci_symbol` | string | `"Rising Sun"` |
| `current_president` | string | `"M. K. Stalin"` |
| `is_active` | bool | `true` |
| `source_url` | string | ECI URL |
| `ground_truth_confidence` | string | `"HIGH"` |

**Parties in Firestore:** `justice_party`, `dk`, `dmk`, `aiadmk`, `inc`, `bjp`, `pmk`, `vck`, `ntk`, `mdmk`, `cpi`, `cpim`

---

### Collection: `leaders`
**Document ID:** `leader_id` (slug, e.g. `m_karunanidhi`)

| Field | Type |
|---|---|
| `leader_id` | string |
| `full_name` | string |
| `tamil_name` | string |
| `born` | string (ISO) |
| `died` | string\|null |
| `party_affiliations` | object[] — `{party_id, from, to, role}` |
| `chief_minister_tenures` | object[] — `{from, to, term_number}` |
| `notable_achievements` | string[] |
| `source_urls` | string[] |
| `ground_truth_confidence` | string |

**Records:** Periyar, C.N. Annadurai, Kamaraj, Karunanidhi, MGR, Jayalalithaa, M.K. Stalin

---

### Collection: `chief_ministers`
**Document ID:** `cm_XX_<name_slug>` (e.g. `cm_01_ps_kumaraswamy_raja`)

| Field | Type |
|---|---|
| `name` | string |
| `party` | string |
| `tenure_from` | string (ISO) |
| `tenure_to` | string\|null |
| `source_url` | string |

**Records:** 20 CMs from P.S. Kumaraswamy Raja (1952) to M.K. Stalin (2021–present)

---

### Collection: `assembly_elections`
**Document ID:** `"<year>"` (e.g. `"2021"`)

| Field | Type |
|---|---|
| `year` | int |
| `total_seats` | int — 234 |
| `majority_mark` | int — 118 |
| `party_results` | object[] — `{party_id, seats_won, vote_share_pct}` |
| `winning_party` | string — party_id |
| `alliance_composition` | object[] |
| `source_url` | string |
| `pdf_checksum` | string\|null |

**Records:** 16 elections — 1952 through 2021

---

### Collection: `alliances`
**Document ID:** `<year>_<anchor_party>_alliance`

| Field | Type |
|---|---|
| `year` | int |
| `alliance_name` | string |
| `anchor_party` | string |
| `member_parties` | string[] |
| `outcome` | string — `"Won"` / `"Lost"` |

**Records:** 28 alliance records across all elections 1952–2021

---

### Collection: `achievements`
**Document ID:** `<scheme_slug>` (e.g. `midday_meal_1956`)

| Field | Type |
|---|---|
| `scheme_id` | string |
| `scheme_name` | string |
| `tamil_name` | string\|null |
| `category` | string |
| `introduced_by_party` | string |
| `year_launched` | int |
| `impact_metrics` | object[] |
| `source_url` | string |

**Records:** Mid-day Meal (1956), MGR Nutritious Meal (1982), Kalaignar Health Insurance (2009), Amma Canteen (2013), Anti-Hindi Agitation (1937), Naan Mudhalvan (2022)

---

## Firestore Schema — Module 2: State Finances

### Collection: `state_finances`
**Document ID:** `<fiscal_year>` (e.g. `"2025-26"`)
**Source:** PRS India TN Budget Analysis PDFs (WORKING)

| Field | Type | Notes |
|---|---|---|
| `fiscal_year` | string | |
| `gsdp_current_prices_cr` | number | |
| `gsdp_growth_rate_pct` | number | |
| `total_receipts_cr` | number | |
| `total_expenditure_cr` | number | |
| `revenue_receipts_cr` | number | |
| `revenue_expenditure_cr` | number | |
| `fiscal_deficit_cr` | number | |
| `fiscal_deficit_pct_gsdp` | number | |
| `within_frbm_limits` | bool | Checks `fiscal_deficit_pct_gsdp <= 3.0` (TN FRBM Act 2003) |
| `committed_expenditure_cr` | number | Salaries + pensions + interest |
| `committed_as_pct_revenue` | number | Key indicator |
| `interest_payments_cr` | number | |
| `interest_as_pct_revenue` | number | Key indicator — ~24% currently |
| `sotr_cr` | number | State's Own Tax Revenue |
| `central_tax_devolution_cr` | number | |
| `debt_why` | object[] | See sub-schema below |
| `viz_metrics` | object | Pre-computed front-end display values |
| `source_url` | string | PRS India PDF URL |
| `pdf_checksum` | string | SHA-256 |
| `ground_truth_confidence` | string | `"MEDIUM"` (PRS is secondary) |

**`debt_why` sub-schema:**
```json
[
  {
    "category": "PSU Liabilities",
    "label": "TANGEDCO restructuring",
    "amount_cr": 110000,
    "year_committed": "2021-22",
    "source_url": "https://prsindia.org/..."
  }
]
```

**Records live:** 2021-22 through 2025-26 (5 years). 2020-21 PRS PDF corrupt — skipped.

---

### Collection: `debt_history`
**Document ID:** `<fiscal_year>`
**Source:** PRS India + curated series (outstanding debt always from curated — never overridden by parser)

| Field | Type | Notes |
|---|---|---|
| `fiscal_year` | string | |
| `outstanding_debt_cr` | number | Always from curated CURATED_DEBT_SERIES |
| `debt_to_gsdp_pct` | number | Always from curated series |
| `new_borrowings_cr` | number | From PRS PDF |
| `interest_payments_cr` | number | From PRS PDF |
| `within_frbm_limits` | bool | Checks `fiscal_deficit_pct_gsdp <= 3.0` |

**Curated Debt Series (ground truth):**

| Year | Outstanding Debt | Debt/GSDP | Key Driver |
|---|---|---|---|
| 2020-21 | ₹5.70 lakh cr | 27.4% | COVID relief spending |
| 2021-22 | ₹6.42 lakh cr | 27.1% | TANGEDCO restructuring |
| 2022-23 | ₹7.21 lakh cr | 26.8% | Metro Rail Phase II |
| 2023-24 | ₹8.06 lakh cr | 26.5% | Infrastructure push |
| 2024-25 | ₹9.26 lakh cr | 26.3% | Revenue deficit financing |
| 2025-26 | ₹9.89 lakh cr | 26.2% | TN Interim Budget |
| 2026-27 | ₹10.71 lakh cr | 26.12% | TN Interim Budget (projected) |

---

### Collection: `departmental_spending`
**Document ID:** `<fiscal_year>_<dept_slug>` (e.g. `"2025-26_education"`)

| Field | Type |
|---|---|
| `fiscal_year` | string |
| `department` | string |
| `allocation_cr` | number |
| `pct_of_total_budget` | number |
| `pct_change_from_prev_year` | number |
| `sub_allocations` | object[] |
| `source_url` | string |

---

## Firestore Schema — Module 3: Socio-Economic Metrics

### Collection: `socio_economics`
**Document ID:** `metric_id` (e.g. `nfhs5_institutional_deliveries`)
**Source:** NFHS-5 (2019-21), NITI Aayog SDG Index 2023-24, ASER 2024, AISHE 2022-23, DPIIT, MoSPI

| Field | Type | Notes |
|---|---|---|
| `metric_id` | string | slug |
| `category` | string | `"Health"` / `"Education"` / `"Economy"` / `"Development"` / `"Infrastructure"` |
| `subcategory` | string | |
| `metric_name` | string | English label |
| `tamil_name` | string | Tamil label |
| `value` | number | Primary metric value |
| `unit` | string | `"percent"`, `"score_out_of_100"`, `"rupees_per_year"`, etc. |
| `year` | int | Survey year |
| `survey` | string | Source survey name |
| `national_average` | number\|null | For comparison |
| `tn_vs_national` | string\|null | Human-readable comparison |
| `trend` | object | Year-keyed historical values |
| `context` | string | Narrative for the UI |
| `alert_level` | string\|null | `"HIGH"` for policy gaps (anaemia, etc.) |
| `policy_gap` | bool\|null | `true` if this is a known failure area |
| `source_url` | string | |
| `ground_truth_confidence` | string | |

**15 metrics live:**

| metric_id | Value | Source |
|---|---|---|
| `nfhs5_institutional_deliveries` | 99.6% | NFHS-5 |
| `nfhs5_stunting_under5` | 25.0% | NFHS-5 |
| `nfhs5_anaemia_women` | 53.0% ⚠ HIGH alert | NFHS-5 |
| `nfhs5_anaemia_children` | 57.0% ⚠ HIGH alert | NFHS-5 |
| `nfhs5_sanitation_basic` | 82.0% | NFHS-5 |
| `sdg_index_2024_composite` | 78/100, Rank 3 | NITI Aayog |
| `sdg_goal1_no_poverty` | 91/100 (Achiever) | NITI Aayog |
| `higher_education_ger` | 47% (vs 28.4% national) | AISHE |
| `per_capita_income_2024` | ₹3.62 lakh (1.77x national) | MoSPI |
| `manufacturing_share_india` | 12.11% of India's mfg GDP | DPIIT |
| `industrial_corridors_district_coverage` | 100% of districts | DPIIT |
| `aser2024_std3_reading_recovery` | 12.0% (recovered from 4.8% in 2022) | ASER 2024 |
| `aser2024_std8_arithmetic` | 64.2% | ASER 2024 |
| `aser2024_out_of_school_rate` | 1.8% (near-universal enrollment) | ASER 2024 |
| `aser2024_govt_school_enrollment` | 68.7% | ASER 2024 |

**ASER scraper details:**
- PDF: `ASER_2024_India_State_Report.pdf` (282 KB) — cached at `data/raw/socio/`
- URL: `https://asercentre.org/wp-content/uploads/2022/12/India.pdf`
- TN data from Table 15 (page 5), 25-column row covering 2018/2022/2024
- `_curated_aser_tn()` fallback with verified values if PDF parse fails
- TN raw row confirmed: `67.4 | 75.7 | 68.7 | 2.3 | 1.9 | 1.8 | 10.2 | 4.8 | 12.0 | 26.0 | 11.2 | 27.7 | 40.7 | 25.2 | 35.6 | 25.4 | 14.9 | 20.8 | 73.2 | 63.0 | 64.2 | 50.2 | 44.4 | 40.0`

---

## Firestore Schema — Module 4: Candidate Accountability

### Collection: `candidate_accountability`
**Document ID:** `2021_<constituency_slug>` (e.g. `2021_chennai_central`) + `tn_assembly_2021_summary`
**Source:** MyNeta/ADR TN 2021 (myneta.info/tamilnadu2021) — WORKING

**Individual MLA document:**

| Field | Type | Notes |
|---|---|---|
| `doc_id` | string | `"2021_<constituency_slug>"` |
| `mla_name` | string | |
| `constituency` | string | |
| `party` | string | |
| `election_year` | int | 2021 |
| `criminal_cases_total` | int | |
| `criminal_severity` | string | `"CLEAN"` / `"MINOR"` / `"MODERATE"` / `"SERIOUS"` |
| `assets_cr` | float\|null | Total declared assets in crore |
| `liabilities_cr` | float\|null | |
| `net_assets_cr` | float\|null | `assets_cr - liabilities_cr` |
| `is_crorepati` | bool | `assets_cr >= 1.0` |
| `education` | string | Raw MyNeta value |
| `education_tier` | string | `"Graduate"` / `"Post Graduate"` / `"Doctorate"` / `"Class XII"` / `"Class X"` / `"Below Class X"` / `"Not Disclosed"` |
| `source_url` | string | MyNeta winners/constituency URL |
| `ground_truth_confidence` | string | `"HIGH"` |

**Criminal severity classification:**
```python
"CLEAN"    — 0 cases
"MINOR"    — 1–2 cases
"MODERATE" — 3–5 cases
"SERIOUS"  — 6+ cases
```

**Assembly summary document** (`tn_assembly_2021_summary`):
```json
{
  "doc_id": "tn_assembly_2021_summary",
  "election_year": 2021,
  "total_constituencies": 234,
  "winners_analyzed": 224,
  "criminal_accountability": {
    "with_criminal_cases": 134,
    "with_criminal_cases_pct": 60,
    "with_serious_cases": 58,
    "with_serious_cases_pct": 26
  },
  "financial_profile": {
    "crorepati_winners": 192,
    "crorepati_pct": 86,
    "avg_assets_cr": 12.52
  },
  "education_profile": {
    "graduate_or_above": 142,
    "graduate_or_above_pct": 63
  }
}
```

---

### Collection: `party_accountability`
**Document ID:** `2021_party_<party_slug>` (e.g. `2021_party_dmk`)
**Source:** Derived from `candidate_accountability` winners

| Field | Type |
|---|---|
| `doc_id` | string |
| `party` | string |
| `election_year` | int |
| `mla_count` | int |
| `criminal_cases_pct` | float |
| `serious_cases_pct` | float |
| `crorepati_pct` | float |
| `avg_assets_cr` | float |
| `graduate_pct` | float |
| `ground_truth_confidence` | string |

**8 party records:** DMK, AIADMK, INC, BJP, PMK, AIFB, CPI, CPM (parties with ≥1 winner)

---

## Firestore Schema — Module 5: Manifesto Tracker

### Collection: `manifesto_promises`
**Document ID:** `<party_id>_<pillar_slug>_<index>` (e.g. `dmk_agriculture_001`)
**Source:** DMK 2021 Manifesto + AIADMK 2021 Manifesto (curated seed at `data/processed/manifesto_promises_seed.json`)
**Pipeline task:** `python main.py --task manifesto`

| Field | Type | Notes |
|---|---|---|
| `doc_id` | string | |
| `party_id` | string | `"dmk"` / `"aiadmk"` / `"bjp"` / `"pmk"` |
| `party_name` | string | |
| `party_color` | string | Tailwind class — `"bg-red-600"` |
| `category` | string | Pillar: `"Agriculture"` / `"Education"` / `"TASMAC & Revenue"` / `"Women's Welfare"` / `"Infrastructure"` |
| `promise_text_en` | string | English text |
| `promise_text_ta` | string | Tamil text |
| `target_year` | int | |
| `status` | string | `"Proposed"` / `"Fulfilled"` / `"Partial"` / `"Abandoned"` / `"Historical"` |
| `stance_vibe` | string | `"Welfare-centric"` / `"Infrastructure-heavy"` / `"Revenue-focused"` / `"Populist"` / `"Reform-oriented"` / `"Women-focused"` / `"Farmer-focused"` |
| `amount_mentioned` | string\|null | `"₹2 lakh per farmer"` |
| `scheme_name` | string\|null | Official scheme name if launched |
| `manifesto_pdf_url` | string | Direct PDF link |
| `manifesto_pdf_page` | int\|null | Page number in PDF |
| `source_notes` | string\|null | Verification notes |
| `ground_truth_confidence` | string | |

**13 records live (seed):** 8 DMK promises + 5 AIADMK promises across all 5 pillars.

**30 records in static frontend seed** (`web/src/lib/manifesto-data.ts`): 19 DMK + 11 AIADMK — used by the frontend until Firestore connection is wired.

**Fulfilled examples (DMK):** Farm loan waiver (₹2L/farmer), free bus for women, Kalaignar Magalir Urimai (₹1000/month cash transfer), CM Breakfast Scheme (free school breakfast), Naan Mudhalvan (skill training), laptop scheme revival.

**Abandoned/Partial examples (DMK):** TASMAC relocation (Abandoned — ₹40,000cr revenue dependency), Metro Phase II (Partial), 5L housing units (Partial).

**Status classification:**

| Status | Meaning |
|---|---|
| `Fulfilled` | Scheme launched, benefits demonstrably delivered |
| `Partial` | In progress or incomplete scope |
| `Abandoned` | Promised but officially dropped or quietly shelved |
| `Proposed` | AIADMK opposition promises, never implemented |
| `Historical` | Predates 2021 — included for context |

---

## Firestore Schema — Module 6: Candidate Transparency

### Collection: `candidate_transparency`
**Document ID:** `{year}_{slugify(constituency)}_{slugify(candidate_name)}` (e.g. `2021_chennai_north_k_s_alagiri`)
**Source:** MyNeta/ADR TN 2021 — all declared candidates (not just winners)
**Pipeline:** `scrapers/candidate_transparency_ingest.py` (standalone — not in `main.py`)

### A.2 — Full Transparency Scrape (READY TO RUN)

| Feature | Implementation |
|---|---|
| **Resume / checkpoint** | On startup, loads `--output` JSON as checkpoint; builds set of already-scraped `doc_id`s; skips those rows entirely. No work is lost if interrupted. |
| **Checkpoint saves** | Writes JSON every `--checkpoint-every N` new records (default: 50). Final save at end. |
| **Rate limiting** | `sleep(1.0)` after each detail fetch; `sleep(5.0)` (total cooldown) after every 10th fetch. Average ~0.67 req/s. |
| **`--constituency NAME`** | Filters Level 1 rows to a single constituency by case-insensitive substring match. Combined with `--dry-run` for pre-flight validation. |
| **`--dry-run`** | Level 1 scrape only — no detail fetches, no file writes. Runs `validate_document()` on all rows and prints a sample doc. |
| **`validate_document()`** | Checks `total_assets_inr`/`liabilities_inr`/`net_worth_inr` are `int | None`; `doc_id` non-empty; `data_confidence_score` in [0,1]. Upload step shows warnings for any invalid docs. |
| **`ground_truth_confidence`** | Derived in `merge_row_and_detail`: ≥0.8 → HIGH, ≥0.5 → MEDIUM, else LOW. |

**Run sequence:**

```bash
# Step 1 — dry-run for Harur to verify schema before full run
.venv/bin/python scrapers/candidate_transparency_ingest.py \
  --constituency Harur --dry-run

# Step 2 — full 2021 scrape with checkpointing (~2–3 hours)
.venv/bin/python scrapers/candidate_transparency_ingest.py \
  --year 2021 \
  --output data/processed/candidate_transparency.json

# Step 3 — upload to Firestore (can re-run safely; all writes are merge=True)
.venv/bin/python scrapers/candidate_transparency_ingest.py \
  --year 2021 --upload

# Resume if interrupted: just re-run Step 2/3 — already-scraped records are skipped
```

This collection covers **all declared candidates** (target: ~3,859 for 2021), unlike `candidate_accountability` which covers only the 224 winners.

**Document schema:**

| Field | Type | Notes |
|---|---|---|
| `doc_id` | string | Deterministic ID — safe to re-run |
| `election_year` | int | 2021 or 2026 |
| `constituency` | string | |
| `candidate_name` | string | |
| `party` | string\|null | |
| `candidate_id` | string\|null | MyNeta internal ID |
| `education.raw` | string\|null | Raw string from MyNeta |
| `education.level` | string | `"Doctorate"` / `"Postgraduate"` / `"Graduate"` / `"School"` / `"Unknown"` |
| `criminal_cases.count` | int\|null | |
| `criminal_cases.ipc_sections` | string[] | e.g. `["302", "307", "420"]` |
| `financials.total_assets_inr` | int\|null | Normalized to rupees |
| `financials.liabilities_inr` | int\|null | |
| `financials.net_worth_inr` | int\|null | `assets - liabilities` |
| `source_url` | string | Detail page URL |
| `list_source_url` | string | List page URL |
| `last_scraped` | string (ISO) | |
| `data_confidence_score` | float | 0.0–1.0 (base 0.4, max 1.0) |
| `ground_truth_confidence` | string | Derived: ≥0.8→HIGH, ≥0.5→MEDIUM, else LOW |
| `_uploaded_at` | string (ISO) | Auto-set on write |
| `_schema_version` | string | `"1.0"` |

**Education normalization buckets:**

| Bucket | Matches |
|---|---|
| `Doctorate` | phd, doctorate, d.litt, md, dm |
| `Postgraduate` | postgraduate, master, m.a, m.sc, mba, llm, ca, cs, icwa |
| `Graduate` | graduate, b.a, b.sc, b.e, b.tech, mbbs, llb, diploma |
| `School` | 12th, hsc, 10th, sslc, 8th, primary, matric, literate, illiterate |
| `Unknown` | anything else or blank |

**`data_confidence_score` breakdown:**

| Component | Score |
|---|---|
| Base (row parsed) | +0.40 |
| candidate_name + constituency present | +0.20 |
| education_level ≠ Unknown | +0.10 |
| criminal_cases_count not null | +0.10 |
| total_assets_inr not null | +0.10 |
| liabilities_inr not null | +0.05 |
| ipc_sections non-empty | +0.05 |
| **Maximum** | **1.00** |

**2026 placeholder docs:** When run with `--year 2026 --placeholders-only`, creates one placeholder doc per constituency from the 2021 constituency list. `is_placeholder: true`, `data_confidence_score: 0.2`, `ground_truth_confidence: "LOW"`. Used to pre-populate Firestore ahead of candidate declarations.

---

## Frontend — Next.js Web App

**Stack:** Next.js 15.5.14, React 19, TypeScript 5.9, Tailwind CSS v4, Firebase SDK v12

**Dev server:** `cd web && npm run dev` → `http://localhost:3000`

### Routes

| Route | Status | Description |
|---|---|---|
| `/` | **LIVE** | Homepage — search + Frequently Browsed (dynamic, Firestore-backed) |
| `/constituency/[slug]` | **LIVE** | Per-constituency drill: MLA card, socio-economic metrics, manifesto promises |
| `/manifesto-tracker` | **LIVE** | Promise vs. Performance comparison interface |

### Manifesto Tracker Feature

**Components (`web/src/components/manifesto/`):**

| Component | Purpose |
|---|---|
| `ComparisonMatrix` | Side-by-side 2-column grid: Party A vs Party B promises for a given pillar |
| `PromiseCard` | Single promise card with toggle for VerificationPanel |
| `VerificationPanel` | Shows `ground_truth_confidence`, PDF link+page, scheme_name, source_notes |
| `StanceLabel` | Color-coded chip for `StanceVibe` (7 types) |
| `StatusBadge` | Color-coded badge for `PromiseStatus` (5 types) |
| `PillarTabs` | Pill buttons for 5 pillars with promise-count badges |
| `PartySelector` | Dropdown with auto-swap guard (prevents same party in both columns) |

**Key types (`web/src/lib/types.ts`):**
```typescript
type PromiseStatus = "Proposed" | "Fulfilled" | "Partial" | "Abandoned" | "Historical"
type Pillar = "Agriculture" | "Education" | "TASMAC & Revenue" | "Women's Welfare" | "Infrastructure"
type StanceVibe = "Welfare-centric" | "Infrastructure-heavy" | "Revenue-focused" | "Populist"
               | "Reform-oriented" | "Women-focused" | "Farmer-focused"
```

**Party colors:**

| Party | Color |
|---|---|
| DMK | `bg-red-600` |
| AIADMK | `bg-green-700` |
| BJP | `bg-orange-500` |
| PMK | `bg-yellow-500` |

**Data source:** Live Firestore via `useManifestos` hook (real-time `onSnapshot` listener). Falls back gracefully when Firestore is unreachable — error banner shown.

**Bilingual support:** EN/Tamil toggle on page. All promise text, pillar labels, stance vibes, and status labels have Tamil translations.

**Year filter:** "All" / "Historical Performance (2021)" / "Upcoming Promises (2026)" — issues a `where("target_year", "==", year)` Firestore query dynamically.

### Firebase Setup

`web/.env.local` required for Firestore reads — create this file with your Firebase project credentials:
```
NEXT_PUBLIC_FIREBASE_API_KEY=
NEXT_PUBLIC_FIREBASE_AUTH_DOMAIN=naatunadappu.firebaseapp.com
NEXT_PUBLIC_FIREBASE_PROJECT_ID=naatunadappu
NEXT_PUBLIC_FIREBASE_STORAGE_BUCKET=naatunadappu.appspot.com
NEXT_PUBLIC_FIREBASE_MESSAGING_SENDER_ID=
NEXT_PUBLIC_FIREBASE_APP_ID=
```

Firebase singleton is initialized in `web/src/lib/firebase.ts` (already exists). Only `NEXT_PUBLIC_` prefixed keys are used — no service account keys in the frontend.

### A.1 — Live Manifesto Integration (DONE)

| File | Change |
|---|---|
| `web/src/hooks/useManifestos.ts` | **NEW** — `onSnapshot` real-time listener; `yearFilter` param triggers `where("target_year", "==", year)` query |
| `web/src/components/manifesto/PromiseSkeleton.tsx` | **NEW** — `PromiseCardSkeleton` + `ComparisonSkeleton` (pulse-animated, matches real layout) |
| `web/src/components/manifesto/VerificationPanel.tsx` | Updated — now shows `_uploaded_at` Firestore timestamp ("Last verified: 1 Apr 2026") |
| `web/src/app/manifesto-tracker/page.tsx` | Updated — replaced static `SEED_PROMISES` with `useManifestos(yearFilter)`; added year filter pills; skeleton during load; live promise count in status banner; error banner on Firestore failure |
| `web/src/lib/types.ts` | Updated — added optional `_uploaded_at?: string` field to `ManifestoPromise` |

### Constituency Drill Feature (`/constituency/[slug]`)

**Data fetched:** `_fetch_constituency_drill(slug)` in `main.py` — assembles:
- MLA record from `candidate_accountability` (3-tier lookup: by `constituency_id`, by slug field, by doc ID)
- Socio-economic metrics from `socio_economics`
- Manifesto promises from `manifesto_promises` (filtered by party)
- `parent_ls` breadcrumb from `ASSEMBLY_TO_LS` reverse map

**SC/ST slug normalization (fixed Session 6):**
- 45 of 230 constituencies have names like "HARUR (SC)" → `accountability_transformer.py` previously generated `harur__sc_` (double underscore, trailing underscore)
- Fix 1 (retroactive): `_fetch_mla_by_constituency()` derives the transformer's dirty slug from `constituency_name` and tries it as a fallback doc ID lookup
- Fix 2 (future loads): `accountability_transformer.py` now uses `re.sub(r"_+", "_", ...).strip("_")` for clean slugs

**Breadcrumb:** `Tamil Nadu › Chennai South (LS) › Mylapore` (bilingual; LS name from `ls-constituency-map.json`)

**View counter:** `useEffect` on page load fires `POST /api/constituency/{slug}/view` (fire-and-forget, non-blocking). Atomically increments `usage_counters/{slug}.view_count` via Firestore `Increment(1)`.

### Homepage Frequently Browsed

- Initialises from `FEATURED_FALLBACK` (6 hardcoded slugs) so the grid renders immediately
- `useEffect` fetches `GET /api/frequently-browsed?limit=6`; merges API results with fallback to always show 6 cards
- Falls back silently on any API error

### TypeScript Types Added (Session 6)

```typescript
interface LsConstituencyMeta    { ls_slug, ls_name, ls_name_ta, ls_id, confidence }
interface FrequentlyBrowsedItem { slug, name, district, view_count }
interface StateMacroRecord      { metric_id, category, metric_name, tamil_name, value, unit, year, national_average, context, source_url, ground_truth_confidence }
interface DistrictHealthRecord  { metric_id, district, metric_name, tamil_name, value, unit, year, alert_level, source_url }
interface DistrictWaterRisk     { district, district_slug, risk_score, risk_level, primary_driver, secondary_driver, affected_taluks, source_url }
interface CropEconomicsRecord   { crop_id, crop_name, crop_name_ta, category, msp_inr_per_qtl, frp_inr_per_qtl, production_cost_inr_per_qtl, price_gap_pct, year, source_url }
interface StateVitalsData       { economy: StateMacroRecord[], health: DistrictHealthRecord[], water: DistrictWaterRisk[], crops: CropEconomicsRecord[] }
```

**`PARTIES` constant** updated — added `inc` entry (blue-600, INC/Congress support end-to-end).

---

### Known Issue — `next build` under `NODE_ENV=production`

**Symptom:** `Build error occurred [TypeError: generate is not a function]`

**Cause:** `NODE_ENV=production` in the shell causes `npm install` to skip devDependencies (`typescript`, `@types/react`, etc.), which breaks the Next.js build toolchain.

**Workaround:** Run `npm install --include=dev` once, then `npm run build`. The dev server (`npm run dev`) works correctly regardless.

---

## Backend API — FastAPI (`web/backend_api/main.py`)

**Server:** `make run-be` → `.venv/bin/uvicorn web.backend_api.main:app --reload --port 8000`

**CORS:** `allow_origins=["*"]`, `allow_methods=["GET", "POST"]`

**Startup:** Loads `CONSTITUENCY_MAP` from `constituency-map.json` (234 slugs) and `ASSEMBLY_TO_LS` from `ls-constituency-map.json` (227 assembly → LS mappings, optional).

### Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Service status + `ls_map_loaded` count |
| `GET` | `/api/constituency/{slug}` | Full drill data: MLA + metrics + promises + parent_ls |
| `GET` | `/api/socio-economics` | All `socio_economics` docs (sorted by METRIC_ORDER) |
| `GET` | `/api/manifesto-promises` | All `manifesto_promises` docs (sorted by PILLAR_ORDER) |
| `GET` | `/api/frequently-browsed?limit=N` | Top-N by `view_count DESC` from `usage_counters` |
| `POST` | `/api/constituency/{slug}/view` | Atomic `Increment(1)` on `usage_counters/{slug}` |
| `GET` | `/api/lookup-pincode?code=600023` | Resolve 6-digit pincode → constituency list from `pincode_mapping` |
| `GET` | `/api/state-vitals?category=all\|economy\|health\|water\|crops` | Reads `state_macro` / `district_health` / `district_water_risk` / `crop_economics` |

### MLA Lookup — 3-Tier Strategy

```python
def _fetch_mla_by_constituency(slug, constituency_id, constituency_name):
    # 1. by constituency_id (int field) — fastest, most reliable
    # 2. by constituency_slug field — handles rename cases
    # 3. direct doc ID (f"2021_{slug}") — standard case
    # 4. dirty-slug fallback: derive transformer slug from constituency_name
    #    catches SC/ST names like "HARUR (SC)" → "harur__sc_" stored in Firestore
```

### Firestore Query Syntax (FieldFilter — required from google-cloud-firestore ≥2.13)

All `.where()` calls use the new API to avoid deprecation warnings:
```python
from google.cloud.firestore_v1.base_query import FieldFilter
col.where(filter=FieldFilter("field", "==", value))
```

---

## Firestore Schema — Module 7: State Macro & Correlation Engine

### Collection: `usage_counters`
**Document ID:** `<constituency_slug>` (e.g. `harur_sc`)
**Written by:** `POST /api/constituency/{slug}/view` on every constituency page load

| Field | Type | Notes |
|---|---|---|
| `view_count` | number | Atomically incremented via `Increment(1)` |
| `constituency_name` | string | From `CONSTITUENCY_MAP` |
| `district` | string | From `CONSTITUENCY_MAP` |
| `district_slug` | string | |
| `last_viewed` | timestamp | `SERVER_TIMESTAMP` |

---

### Collection: `state_macro`
**Document ID:** `<metric_id>` (e.g. `gsdp_growth_2024`)
**Source:** Curated — MoSPI, PRS India, TN Budget 2025-26
**Ingest:** `scrapers/state_macro_ingest.py`

| Field | Type | Notes |
|---|---|---|
| `metric_id` | string | |
| `category` | string | `"economy"` |
| `metric_name` | string | English label |
| `tamil_name` | string | Tamil label |
| `value` | number | |
| `unit` | string | `"percent"`, `"crore_inr"`, etc. |
| `year` | int | |
| `national_average` | number\|null | For comparison |
| `context` | string | UI narrative |
| `source_url` | string | |
| `ground_truth_confidence` | string | |

**6 seed records:** GSDP growth 11.19% (vs 8.2% national), Services 53%, Manufacturing 34%, Agriculture 13%, Debt/GSDP 24.1%, TASMAC revenue ₹47,800cr.

---

### Collection: `district_health`
**Document ID:** `<metric_id>` (e.g. `tn_hypertension_prevalence`)
**Source:** NFHS-5, ICMR-INDIAB, State Health Department
**Ingest:** `scrapers/state_macro_ingest.py`

| Field | Type | Notes |
|---|---|---|
| `metric_id` | string | |
| `district` | string | `"Tamil Nadu"` for state-level |
| `metric_name` | string | |
| `tamil_name` | string | |
| `value` | number | |
| `unit` | string | |
| `year` | int | |
| `alert_level` | string\|null | `"HIGH"` for policy-gap metrics |
| `source_url` | string | |

**6 seed records:** Hypertension 31.4%, Diabetes 16.8%, High BMI 40%, NCD deaths 75%, Chennai hypertension 37.5%, Coimbatore diabetes 19.2%.

---

### Collection: `district_water_risk`
**Document ID:** `<district_slug>` (e.g. `pudukottai`)
**Source:** NITI Aayog Composite Water Management Index, IMD
**Ingest:** `scrapers/state_macro_ingest.py`

| Field | Type | Notes |
|---|---|---|
| `district` | string | |
| `district_slug` | string | |
| `risk_score` | number | 0–5 scale |
| `risk_level` | string | `"Extreme"` / `"High"` / `"Moderate"` |
| `primary_driver` | string | e.g. `"Groundwater depletion"` |
| `secondary_driver` | string | |
| `affected_taluks` | string[] | |
| `source_url` | string | |

**6 seed records:** Pudukottai 4.8 (Extreme), Ramanathapuram 4.7, Kancheepuram 4.6, Vellore 4.5, Chennai 4.1, Cauvery delta flood risk.

---

### Collection: `crop_economics`
**Document ID:** `<crop_id>` (e.g. `paddy_kharif_2024`)
**Source:** CACP (Commission for Agricultural Costs and Prices), FCI, Sugar Directorate
**Ingest:** `scrapers/state_macro_ingest.py`

| Field | Type | Notes |
|---|---|---|
| `crop_id` | string | |
| `crop_name` | string | |
| `crop_name_ta` | string | Tamil name |
| `category` | string | `"cereal"` / `"cash_crop"` / `"oilseed"` / `"horticulture"` |
| `msp_inr_per_qtl` | number\|null | Minimum Support Price; null if no MSP |
| `frp_inr_per_qtl` | number\|null | Fair and Remunerative Price (sugarcane only) |
| `production_cost_inr_per_qtl` | number\|null | C2 cost (full cost including land rent) |
| `price_gap_pct` | number\|null | `(MSP - cost) / cost × 100`; negative = farmer loss |
| `year` | int | |
| `source_url` | string | |

**6 seed records:** Paddy kharif ₹2,300 MSP, Paddy rabi ₹2,320, Sugarcane FRP ₹340/qtl, Groundnut ₹6,783, Cotton ₹7,121, Banana (no MSP — market-linked).

**Run ingest:**
```bash
# Dry-run first
.venv/bin/python scrapers/state_macro_ingest.py --dry-run

# Live upload
.venv/bin/python scrapers/state_macro_ingest.py
```

---

### Collection: `pincode_mapping`
**Document ID:** `<pincode>` (e.g. `"600023"`)
**Source:** Curated — India Post postal area boundaries + ECI Delimitation Order 2008
**Ingest:** `scrapers/pincode_ingest.py`

| Field | Type | Notes |
|---|---|---|
| `pincode` | string | 6-digit postal code |
| `district` | string | Revenue district |
| `is_ambiguous` | bool | `true` when pincode straddles ≥2 AC boundaries |
| `constituencies` | object[] | `{slug, name, name_ta}` — 1 entry if unambiguous, 2-3 if ambiguous |
| `ground_truth_confidence` | string | `"MEDIUM"` — postal areas ≠ ECI boundaries exactly |

**Seed coverage (~183 pincodes):**
- Chennai 600001–600119: ~90 pincodes, full range
- Coimbatore 641xxx: ~15 pincodes
- Madurai 625xxx: ~15 pincodes
- Trichy 620xxx: ~7 pincodes
- Salem 636xxx: ~6 pincodes
- District HQs (35 districts): ~35 pincodes

**Run ingest:**
```bash
.venv/bin/python scrapers/pincode_ingest.py --dry-run
.venv/bin/python scrapers/pincode_ingest.py
```

**API:** `GET /api/lookup-pincode?code=600023`
```json
{
  "pincode": "600023",
  "district": "Chennai",
  "is_ambiguous": false,
  "constituencies": [
    { "slug": "anna_nagar", "name": "Anna Nagar", "name_ta": "அண்ணா நகர்" }
  ]
}
```

### B.4 Pincode Resolver — Frontend

**Component:** `web/src/components/constituency/PincodeResolverModal.tsx`
**Trigger:** rendered inside `ConstituencySearch` — "Find by pincode" link below the search box

**State machine:**
| State | UI |
|---|---|
| `idle` | Input + "Find" button |
| `loading` | Spinner in button |
| `single` | Green confirmation flash → auto-redirect (600ms) |
| `ambiguous` | Constituency choice cards |
| `not_found` | Amber error — pincode not in DB |
| `error` | Red error — network/server failure |

**localStorage persistence:** key `aayvu_p2c_{pincode}` → selected slug. On re-lookup of the same pincode, skips API call and navigates directly.

**Geolocation:** "Use my location" button → `navigator.geolocation` → Nominatim reverse geocode (`nominatim.openstreetmap.org/reverse`) → extracts postcode → pre-fills input and triggers lookup automatically.

**Bilingual:** full EN/Tamil support via `lang` prop (labels, placeholders, status messages).

---

### LS Constituency Map (`web/src/lib/ls-constituency-map.json`)

Maps 39 Lok Sabha constituencies → arrays of assembly slugs.

**Coverage:** 227/230 assembly slugs (98.7%). Three slugs without LS parent: `rameswaram` (Ramanathapuram LS), `thovalai`, `vilavancode` (Kanniyakumari LS).

**Loaded at startup** into `ASSEMBLY_TO_LS` reverse map: `assembly_slug → { ls_slug, ls_name, ls_name_ta, ls_id, confidence }`.

**Schema per entry:**
```json
"madurai": {
  "name": "Madurai",
  "name_ta": "மதுரை",
  "ls_id": 32,
  "confidence": "HIGH",
  "assembly_slugs": ["madurai_north", "madurai_south", "madurai_central", ...]
}
```

**Confidence levels:** `HIGH` (official ECI boundary data), `MEDIUM` (derived from census/news).

---

## Scraper Architecture

### Working Scrapers

| Scraper | Target | Method | Notes |
|---|---|---|---|
| `prs_scraper.py` | prsindia.org | HTTPS + pdfplumber | 5 years parsed; 2020-21 corrupt/skipped |
| `tn_budget_scraper.py` | local file / direct URL | pdfplumber | Manual-link workaround for GoTN SSL block |
| `myneta_scraper.py` | myneta.info/tamilnadu2021 | requests + BeautifulSoup | Two-stage: main table + constituency fallback |
| `aser_scraper.py` | asercentre.org PDF | pdfplumber | Cached PDF; curated fallback if parse fails |

### Blocked Scrapers (curated fallback active)

| Scraper | Target | Error | Fallback |
|---|---|---|---|
| `ceo_tn_scraper.py` | elections.tn.gov.in | SSL EOF | `_curated_election_results()` |
| `assembly_scraper.py` | assembly.tn.gov.in | SSL EOF | `_curated_chief_ministers()` — 20 CMs |
| `eci_scraper.py` | eci.gov.in | SSL EOF | `_curated_eci_tn_parties()` — 10 parties |

---

## Transformer Summary

### `election_transformer.py`
- `PARTY_NAME_MAP` — 20-entry normalization dict (raw site name → party_id slug)
- `ALLIANCE_DATA` — curated 1952–2021 alliance matrix (can't be scraped)
- `build_alliance_matrix()` — returns curated dict

### `finance_transformer.py`
- `DEBT_WHY_MAP` — structured debt reasons per year (2021-22 to 2025-26)
- `CURATED_DEBT_SERIES` — authoritative debt stock values; parser output NEVER overrides these
- `compute_viz_metrics()` — 9 derived fields (interest_as_pct_revenue, committed_as_pct_revenue, etc.)
- `build_debt_history_series()` — 7-year series with curated debt/GSDP values
- `within_frbm_limits` checks `fiscal_deficit_pct_gsdp <= 3.0` (TN FRBM Act 2003 — not debt stock)

### `socio_transformer.py`
- `merge_aser_into_socio(aser_data, socio_docs)` — overrides curated ASER entries with live-scraped values
- `add_aser_enrollment_metrics(aser_data)` — creates 2 extra docs (out-of-school rate, govt school enrollment share)

### `accountability_transformer.py`
- `classify_criminal_severity(cases)` — CLEAN/MINOR/MODERATE/SERIOUS
- `enrich_winner(winner)` — adds `criminal_severity`, `is_crorepati`, `net_assets_cr`, `education_tier`, `doc_id`
- `build_party_rollups(winners)` — per-party scorecards (criminal%, crorepati%, avg_assets, graduate%)
- `build_assembly_summary(winners, stats)` — assembly-level summary doc

---

## Loader: `firestore_loader.py`

All uploads use `_batch_upload()` with `BATCH_SIZE=400` (Firestore max is 500) and `merge=True` (upsert).

Every document gets auto-stamped with `_uploaded_at` (ISO UTC) and `_schema_version: "1.0"`.

| Function | Collection |
|---|---|
| `upload_parties()` | `political_parties` |
| `upload_leaders()` | `leaders` |
| `upload_chief_ministers()` | `chief_ministers` |
| `upload_elections()` | `assembly_elections` |
| `upload_alliances()` | `alliances` |
| `upload_achievements()` | `achievements` |
| `upload_state_finances()` | `state_finances` |
| `upload_debt_history()` | `debt_history` |
| `upload_departmental_spending()` | `departmental_spending` |
| `upload_finance_manual()` | `state_finances` (single doc upsert) |
| `upload_socio_economics()` | `socio_economics` |
| `upload_mla_winners()` | `candidate_accountability` |
| `upload_party_rollups()` | `party_accountability` |
| `upload_assembly_summary()` | `candidate_accountability` (single doc upsert) |
| `upload_manifesto_promises()` | `manifesto_promises` |

Note: `candidate_transparency` collection is uploaded by `candidate_transparency_ingest.py` directly via its own `upload_firestore()` function — not via `firestore_loader.py`.

---

## Data Confidence Model

Every Firestore document carries:

| Field | Values | Meaning |
|---|---|---|
| `ground_truth_confidence` | `"HIGH"` | Directly from official source (ECI, TN Assembly, GoTN Budget, MyNeta/ADR, NFHS-5, ASER) |
| `ground_truth_confidence` | `"MEDIUM"` | Secondary source — cross-reference recommended (PRS India for finance) |
| `ground_truth_confidence` | `"LOW"` | Single secondary source — editorial review needed |
| `pdf_checksum` | SHA-256 hex | Set when sourced from a PDF; `null` for curated/manual entries |
| `_uploaded_at` | ISO timestamp | Auto-set on every write |
| `_schema_version` | `"1.0"` | For future migrations |

---

## GCP Deployment

### Firestore Security Rules
```
rules_version = '2';
service cloud.firestore {
  match /databases/{database}/documents {
    match /{document=**} {
      allow read: if true;     // public read for the awareness app
      allow write: if false;   // writes only via service account / pipeline
    }
  }
}
```

### Cloud Run Job (batch ingestion)
```bash
gcloud run jobs create naatu-nadappu-ingest \
  --image gcr.io/naatunadappu/naatu-nadappu:latest \
  --region asia-south1 \
  --set-secrets /secrets/sa-key.json=naatu-sa-key:latest \
  --args="--task=all"
```

### Dockerfile
```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV GOOGLE_APPLICATION_CREDENTIALS=/secrets/sa-key.json
ENTRYPOINT ["python", "main.py"]
CMD ["--task", "all"]
```

> Use Python **3.12** on Cloud Run — stable C-extension wheels for all dependencies. Local dev runs 3.14.

---

## Next Steps (Ordered by Priority)

1. **Run `pincode_ingest.py`** — Upload the 183-pincode seed to Firestore:
   ```bash
   .venv/bin/python scrapers/pincode_ingest.py --dry-run   # verify
   .venv/bin/python scrapers/pincode_ingest.py              # upload
   ```
   Expand coverage by adding entries to `_PINCODE_DATA` in the script.

2. **Run `state_macro_ingest.py`** — Script is created but not yet executed. Populates `state_macro`, `district_health`, `district_water_risk`, `crop_economics` collections:
   ```bash
   .venv/bin/python scrapers/state_macro_ingest.py --dry-run   # verify
   .venv/bin/python scrapers/state_macro_ingest.py              # upload
   ```

2. **Phase 3: Correlation Matrix UI** — "What's the Connection?" heatmap feature. X/Y axis toggles:
   - Industrial Growth vs NRI/Migration
   - TASMAC Revenue vs Health Ailments
   - MSP/Agriculture vs District Literacy
   Reads from `state_macro`, `district_health`, `crop_economics` already in Firestore.

3. **State Vitals page** — Wire `GET /api/state-vitals` to a `/state-vitals` frontend route. Currently shows "Coming soon" on homepage nav card.

4. **Add `web/.env.local`** — Firebase config keys needed to activate live Firestore reads for the manifesto tracker. Get values from Firebase Console → Project Settings → Your apps.

5. **`candidate_transparency` frontend** — 3,578 records uploaded. Build a `/candidates` route: searchable by name, party, constituency, criminal cases, or education level.

6. **Expand manifesto seed** — Current seed is 13 records (Firestore) / 30 records (frontend static). Target ~60–80 atomic promises across all 5 pillars × 4 parties. Re-run `python main.py --task manifesto` after updating `manifesto_promises_seed.json`.

7. **2026 Election data** — Add `election_2026` collection for candidate announcements. Run `candidate_transparency_ingest.py --year 2026 --placeholders-only` to pre-populate constituency stubs.

8. **Finance scraper improvement** — 2020-21 PRS PDF is corrupt. Use `tn_budget_scraper.py --task manual-pdf` to fill the gap if the PDF can be sourced manually.

9. **Alert dashboard** — Surface `alert_level: "HIGH"` metrics (anaemia_women 53%, anaemia_children 57%) in the UI.

10. **GCP Cloud Run job** — Containerise pipeline with Python 3.12 Dockerfile. Schedule monthly re-scrape for MyNeta and ASER updates.
