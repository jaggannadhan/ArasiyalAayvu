```
   _               _           _   _                      
  /_\  _ _ __ _ __(_)_  _ __ _| | /_\  __ _ _  ___ ___  _ 
 / _ \| '_/ _` (_-< | || / _` | |/ _ \/ _` | || \ V / || |
/_/ \_\_| \__,_/__/_|\_, \__,_|_/_/ \_\__,_|\_, |\_/ \_,_|
                     |__/                   |__/           
```

<p align="center">
  <strong>அரசியல்ஆய்வு</strong><br>
  <em>arasiyal (politics) + aayvu (research)</em>
</p>

<p align="center">
  <b>Open-source political transparency platform for Tamil Nadu.</b><br>
  Turning government data into citizen power — one constituency at a time.
</p>

<p align="center">
  <a href="https://arasiyal-aayvu.vercel.app">🌐 Live App</a> · <a href="#-how-it-works">How It Works</a> · <a href="#-core-features">Features</a> · <a href="#-system-architecture">Architecture</a> · <a href="#-tech-stack">Tech Stack</a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/politicians-5%2C000%2B-blue" alt="Politicians" />
  <img src="https://img.shields.io/badge/KG%20nodes-6%2C871-purple" alt="KG Nodes" />
  <img src="https://img.shields.io/badge/manifesto%20promises-1%2C889-orange" alt="Promises" />
  <img src="https://img.shields.io/badge/data%20sources-17-green" alt="Sources" />
  <img src="https://img.shields.io/badge/constituencies-234-red" alt="Constituencies" />
  <img src="https://img.shields.io/badge/prod%20memory-107%20MB-brightgreen" alt="Memory" />
</p>

---

## 🎯 The Problem

India's democratic data is **public by law but inaccessible by design**:

- **234 constituencies** — most voters can't name their MLA's declared assets or pending criminal cases
- **15+ government portals** with inconsistent formats — ECI, MyNeta, NCRB, MOSPI, NITI Aayog, UDISE+, PLFS, SRS — nobody cross-references them
- **Manifestos forgotten** the day after elections — no tracker maps promises to outcomes
- **Rising youth turnout** but zero accessible tools for evidence-based political research

## 💡 The Solution

ArasiyalAayvu ingests, normalizes, and presents **all public political data for Tamil Nadu** in one searchable, bilingual (English + தமிழ்) interface — from a phone, in under 3 seconds.

> **The goal:** an informed electorate that votes on evidence, not rhetoric.

---

## ⚙️ How It Works

### The Data Pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        GOVERNMENT DATA SOURCES                         │
│  ECI · MyNeta · MOSPI · NCRB · NITI Aayog · RBI · CAG · MOFPI · CEA  │
│  PLFS · SRS · HCES · AISHE · UDISE+ · NFHS-5 · PRS · ASER · MNRE    │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │   INGESTION LAYER     │
                    │   50+ Python scrapers │
                    │                       │
                    │ • BeautifulSoup (HTML) │
                    │ • Playwright (JS-rendered sites)
                    │ • pypdf (PDF extraction)│
                    │ • Gemini 2.5 Pro (OCR) │
                    │ • Custom Tamil font    │
                    │   decoders             │
                    └───────────┬───────────┘
                                │
          ┌─────────────────────▼─────────────────────┐
          │            GOOGLE CLOUD FIRESTORE          │
          │            25+ collections                 │
          │                                            │
          │  ┌──────────────┐  ┌───────────────────┐   │
          │  │ politician_  │  │ manifesto_        │   │
          │  │ profile      │  │ promises          │   │
          │  │ (5,000+)     │  │ (1,889)           │   │
          │  └──────┬───────┘  └────────┬──────────┘   │
          │         │                   │              │
          │  ┌──────▼───────┐  ┌───────▼──────────┐   │
          │  │ constituency │  │ knowledge_graph   │   │
          │  │ _mla_index   │  │ (6,871 nodes)     │   │
          │  │ (912 entries)│  │ (16,856 edges)    │   │
          │  └──────────────┘  └──────────────────┘   │
          │                                            │
          │  candidates_2026 · state_finances · plfs   │
          │  srs · hces · ncrb · udise · asi · energy  │
          │  mofpi · rbi_state_finances · feedback     │
          └─────────────────────┬─────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │   FastAPI BACKEND     │
                    │   Cloud Run           │
                    │   40+ endpoints       │
                    │                       │
                    │ • Graph Query Engine  │
                    │   (NetworkX runtime)  │
                    │ • SDG Alignment       │
                    │   (cached per party)  │
                    │ • Fiscal Feasibility  │
                    │   (KG traversal)      │
                    │ • Session Tracking    │
                    │   (live user count)   │
                    │ • Politician CRUD     │
                    │   (merge/dedup)       │
                    └───────────┬───────────┘
                                │
                    ┌───────────▼───────────┐
                    │   Next.js 15 FRONTEND │
                    │   Vercel Edge CDN     │
                    │                       │
                    │ • App-shell layout    │
                    │   (fixed header/footer│
                    │   + scrollable middle)│
                    │ • Client-side cache   │
                    │   (localStorage,      │
                    │   midnight TTL,       │
                    │   consent-gated)      │
                    │ • Idle prefetch       │
                    │   (12 endpoints)      │
                    │ • Bilingual (EN/தமிழ்) │
                    └───────────────────────┘
```

### The AI Layer — Manifesto OCR + Enrichment

Tamil political manifestos are published as PDFs using **legacy non-Unicode Tamil fonts** (Bamini/TSCII). Standard text extraction returns garbled bytes. Previous attempts with pdfplumber + Claude produced hallucinated content (e.g., NTK manifesto showed "200 new TASMAC outlets" — the exact opposite of their actual anti-liquor stance).

**Our pipeline:**

```
┌──────────────┐     ┌─────────────────────┐     ┌──────────────────┐
│  Tamil PDF   │     │  Gemini 2.5 Pro     │     │  Structured      │
│  (legacy     │────▶│  Multimodal         │────▶│  Promises JSON   │
│   fonts)     │     │                     │     │                  │
│              │     │  • Reads PDF as     │     │  • Tamil Unicode │
│  462 pages   │     │    images (bypasses  │     │  • English trans. │
│  NTK example │     │    font encoding)   │     │  • Category      │
│              │     │  • OCR → Unicode    │     │  • Stance vibe   │
│              │     │  • Translate Tamil  │     │  • Page ref      │
│              │     │  • JSON schema      │     │                  │
│              │     │    enforced output   │     │  1,065 promises  │
└──────────────┘     └─────────────────────┘     └────────┬─────────┘
                                                          │
                     ┌─────────────────────┐     ┌────────▼─────────┐
                     │  Enrichment Pass    │     │  Deep Analysis   │
                     │  (TN-grounded)      │◀────│  per promise     │
                     │                     │     │                  │
                     │  • TN Budget 2025-26│     │  • impact_mechanism
                     │    context injected  │     │  • fiscal_cost_note
                     │  • Reference data:  │     │  • sustainability │
                     │    population, BPL,  │     │    _verdict      │
                     │    farmer count,     │     │  • promise_      │
                     │    MGNREGS wage     │     │    components[]  │
                     │  • Arithmetic-      │     │  • implementation│
                     │    required rule    │     │    _risk         │
                     │  • "data unavailable│     │  • root_cause_   │
                     │    — cannot calc"   │     │    addressed     │
                     │    (no hallucination)│     │                  │
                     └─────────────────────┘     └──────────────────┘
```

**Results:** NTK (1,065 promises from 462 pages), DMK (525 from 98 pages), AIADMK (299 from 45 pages) — all with deep analysis, zero hallucinated content.

### The Knowledge Graph Engine

```
                    ┌─────────────────────┐
                    │   KNOWLEDGE GRAPH   │
                    │   6,871 nodes       │
                    │   16,856 edges      │
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
    ┌─────────▼──────┐ ┌──────▼───────┐ ┌──────▼──────────┐
    │  FOUNDATION    │ │  POLITICAL   │ │ SOCIOECONOMIC   │
    │                │ │              │ │                 │
    │ 5 states       │ │ 108 parties  │ │ 13 indicator    │
    │ 38 districts   │ │ 4,488 cands  │ │   types × 5     │
    │ 234 constits   │ │ 637 promises │ │   states        │
    └────────────────┘ └──────────────┘ └─────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │   BRIDGE LAYER      │
                    │   16 SDG Goals      │
                    │                     │
                    │ targets_goal (1,071) │
                    │ measured_by (194)    │
                    │ influences (59)      │
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
    ┌─────────▼──────┐ ┌──────▼───────┐ ┌──────▼──────────┐
    │  TRAVERSAL     │ │  FEASIBILITY │ │  SDG ALIGNMENT  │
    │  /api/graph/   │ │  /api/graph/ │ │  /api/manifesto │
    │  neighbors     │ │  feasibility │ │  /{party}/{year}│
    │  traverse      │ │  /{promise}  │ │  /sdg-alignment │
    │  path          │ │              │ │                 │
    │                │ │ Promise →    │ │ Cached per      │
    │ BFS with verb  │ │ SDG → Indic  │ │ party+year      │
    │ filters,       │ │ ator → State │ │ (indefinite     │
    │ max_depth,     │ │ Finances →   │ │  TTL, cleared   │
    │ max_nodes      │ │ Score 0-100  │ │  on data update)│
    └────────────────┘ └──────────────┘ └─────────────────┘
```

### The Politician Identity System

```
┌─────────────────────────────────────────────────────────────┐
│                   POLITICIAN PROFILE                        │
│                   (Single Source of Truth)                   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  Identity                                           │    │
│  │  canonical_name: "Stalin M.K."                      │    │
│  │  aliases: ["M.K. STALIN", "M.k.stalin"]             │    │
│  │  photo_url: https://storage.../profile.jpg          │    │
│  │  gender: Male | age: 73 | education: Graduate       │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  Timeline (per-election full detail)                 │    │
│  │                                                     │    │
│  │  2021 │ DMK │ KOLATHUR │ Won                        │    │
│  │       │ Assets: ₹4.67 Cr (movable ₹2.1, immov ₹2.5)│    │
│  │       │ Liabilities: ₹0.12 Cr                      │    │
│  │       │ Criminal: 4 cases SERIOUS                   │    │
│  │       │   └─ [{act: "IPC", status: "Pending"...}]  │    │
│  │       │ Education: Graduate                         │    │
│  │       │ Source: candidate_accountability/2021_kolathur│   │
│  │                                                     │    │
│  │  2016 │ DMK │ KOLATHUR │ Won                        │    │
│  │       │ Assets: ₹3.21 Cr                           │    │
│  │       │ Criminal: 2 cases MODERATE                  │    │
│  │                                                     │    │
│  │  2026 │ DMK │ KOLATHUR │ Contesting                 │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                             │
│  win_count: 2 │ loss_count: 0 │ total_contested: 3         │
│                                                             │
│  ┌──────────────────────────────────────────┐               │
│  │  Asset Growth                            │               │
│  │  📊 ₹3.21 Cr (2016) → ₹4.67 Cr (2021)  │               │
│  │     +45% over 5 years                    │               │
│  └──────────────────────────────────────────┘               │
└──────────────────────────┬──────────────────────────────────┘
                           │
            ┌──────────────┼──────────────┐
            │              │              │
  ┌─────────▼────┐  ┌─────▼─────┐  ┌─────▼──────────┐
  │ Constituency │  │ Politician│  │ Search Index   │
  │ Page         │  │ Profiles  │  │ (4,506 entries)│
  │              │  │ Page      │  │                │
  │ Reads via    │  │           │  │ Candidate name │
  │ constituency │  │ Table +   │  │ → constituency │
  │ _mla_index   │  │ Grid view │  │ → term 2026   │
  │ → profile    │  │ + modal   │  │                │
  └──────────────┘  └───────────┘  └────────────────┘
```

### Client-Side Performance Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    BROWSER                               │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │  data-cache.ts (URL-keyed, generic)             │    │
│  │                                                 │    │
│  │  ┌─────────────┐    ┌──────────────────────┐    │    │
│  │  │ In-Memory   │◄──▶│ localStorage         │    │    │
│  │  │ Map<url,    │    │ (consent-gated)      │    │    │
│  │  │   response> │    │                      │    │    │
│  │  │             │    │ TTL: midnight local  │    │    │
│  │  │ cachePeek() │    │ Schema version: 1    │    │    │
│  │  │ cacheFetch()│    │ Hydrates on module   │    │    │
│  │  │ cacheSet()  │    │   load (before React)│    │    │
│  │  └──────┬──────┘    └──────────────────────┘    │    │
│  │         │                                       │    │
│  │  ┌──────▼──────────────────────────────────┐    │    │
│  │  │  prefetchOnIdle(urls[])                 │    │    │
│  │  │  requestIdleCallback / setTimeout       │    │    │
│  │  │  12 endpoints warmed on home page load  │    │    │
│  │  └─────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────┘    │
│                                                         │
│  ┌────────────┐  ┌──────────────┐  ┌────────────────┐   │
│  │ Language   │  │ Cookie       │  │ Session        │   │
│  │ Context    │  │ Consent      │  │ Tracking       │   │
│  │            │  │              │  │                │   │
│  │ Persists   │  │ Performance  │  │ X-Session-ID   │   │
│  │ to local   │  │ cookies gate │  │ header on all  │   │
│  │ Storage    │  │ localStorage │  │ API calls      │   │
│  │ EN ↔ தமிழ் │  │ + Analytics  │  │ (piggyback,    │   │
│  │            │  │              │  │  zero extra    │   │
│  │ Shared via │  │ Banner re-   │  │  requests)     │   │
│  │ React      │  │ shows until  │  │                │   │
│  │ Context    │  │ accepted     │  │ Backend flushes│   │
│  │            │  │              │  │ to Firestore   │   │
│  │            │  │              │  │ 1 write/min    │   │
│  │            │  │              │  │ per instance   │   │
│  └────────────┘  └──────────────┘  └────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

---

## 🚀 Core Features

### 🗳️ Know Your Constituency
Search any of 234 assembly constituencies by **name, district, pincode, locality, or candidate name**. The search normalizes Tamil transliteration variants (`Thiruvallur` = `Tiruvallur`) and strips initials (`Stalin` matches `M.K. Stalin`).

Click any MLA card → full politician profile modal with career timeline, asset growth, criminal record history.

### 👤 Politician Profiles
Unified person-level records for **5,000+ politicians** across 5 election terms (2006–2026):

- **Standardized Indian naming convention:** `<Name> <Initials>` (e.g., `Stalin M.K.`, `Rehman A.R.S.`)
- **Career timeline** with per-term assets (movable/immovable breakdown), liabilities, criminal cases (full FIR detail), education
- **Asset growth tracking:** `₹0.50 Cr (2006) → ₹7.54 Cr (2021), +1408%`
- **Profile photos:** 93% coverage across all terms (5,000+ photos in GCS, immutable, 1-year CDN cache)
- **Table + Grid views** — click any row/card → profile modal with full detail
- **Entity resolution:** strict 3-rule dedup (same name+initials, same constituency, same gender)

Constituency page reads MLA data via `constituency_mla_index` → `politician_profile` — **single source of truth**.

### 📜 Manifesto Tracker
Three party manifestos fully processed:

| Party | Pages | Promises | Source |
|---|---|---|---|
| NTK | 462 | 1,065 | [makkalarasu.in](https://makkalarasu.in) (Tamil) |
| DMK | 98 | 525 | [dmk.in](https://dmksite.blob.core.windows.net) (Tamil) |
| AIADMK | 45 | 299 | [aiadmk.com](https://aiadmk.com) (English) |

Each promise includes: fiscal cost estimate, sustainability verdict (structural/symptomatic/optics), SDG alignment via KG edges, implementation risk, promise components breakdown.

### 📊 State Vitals — 12 Tabs

| Tab | Source | Key Metrics |
|---|---|---|
| Labour | PLFS | LFPR, WPR, Unemployment (15+ and 15–29 youth breakdown) |
| Health | SRS | IMR, MMR, TFR, CBR, CDR — rural vs urban |
| Spending | HCES | MPCE per person per month, welfare uplift from free goods |
| Higher Ed | AISHE | GER, GPI, college/university count |
| School Ed | UDISE+ | GER by level, dropout rates, pupil-teacher ratio |
| Crime | NCRB | IPC crimes, crimes against women/children/SC/ST |
| Industry | ASI | Factories, GVA, NVA, total output, fixed capital |
| Energy | CEA/MNRE | Installed capacity (GW), solar, wind, renewable (MW) |
| Agriculture | MOFPI | Crops, livestock, fruits, spices, FPOs, eNAM mandis, PM-FME |
| Fiscal | CAG + RBI | Revenue, expenditure, deficits, committed spend, debt-to-GSDP |
| SDG | NITI Aayog | 16-goal composite score, chain-break analysis |
| Cost of Living | Multiple | Fuel, Aavin dairy prices |

Every abbreviation has an **inline ⓘ tooltip** with full form + plain-English explanation. Designed for first-time voters, not policy wonks.

### 🕸️ Knowledge Graph
**6,871 nodes × 16,856 edges** — politicians, constituencies, parties, promises, SDG goals, socioeconomic indicators all connected. Powers runtime graph queries, fiscal feasibility scoring, and SDG alignment computation.

### 🗺️ Constituency Map
1,788 pincodes → 234 constituencies. Localities searchable (e.g., "Kilpauk" → Anna Nagar).

### 🌍 SDG Tracker
17 UN Sustainable Development Goals scored 0–100 for Tamil Nadu, benchmarked against peer states.

### 💬 Feedback System
5-category submission (Correction, Missing Data, Suggestion, Bug Report, Other) → Firestore with IP, user-agent, page URL context. Footer link on every page.

### 🟢 Live User Count
Zero-extra-request session tracking — `X-Session-ID` header piggybacks on existing API calls. Backend daemon flushes to Firestore once/min per Cloud Run instance. Scales to 100K+ users at ~5 Firestore writes/min total.

---

## 🏗️ System Architecture

```
scrapers/                              Data pipelines
  knowledge_graph/                     KG builder (NetworkX → GCS)
    graph_builder.py                   6,871 nodes, 16,856 edges
    bridge_rules.py                    SDG↔indicator↔promise mappings
    ontology.json                      21 node types, 11 edge verbs
  manifesto_ocr_gemini.py              Gemini 2.5 Pro multimodal OCR
  manifesto_enrich_gemini.py           Deep analysis (TN-budget-grounded)
  politician_profile_migrate.py        Master identity builder
  normalize_politician_names.py        Indian naming convention standardizer
  cache_candidate_photos.py            ECI photo cacher → GCS (5,000+)

web/
  src/app/                             Next.js 15 App Router (8 routes)
  src/components/
    politicians/ProfileModal.tsx       Shared profile modal (used everywhere)
    state/InfoTip.tsx                  40-term glossary with tooltips
    state/SkeletonCard.tsx             Shimmer loading placeholders
    feedback/FeedbackModal.tsx         5-category feedback form
    consent/CookieBanner.tsx           GDPR-style consent with toggle
    LiveCount.tsx                      Pulsing green "X live users" badge
  src/lib/
    data-cache.ts                      URL-keyed cache + localStorage persistence
    LanguageContext.tsx                Global EN↔தமிழ் with localStorage persist
    CookieConsentContext.tsx           Performance cookie gating
    api-client.ts                      apiGet with X-Session-ID header
  backend_api/
    main.py                            40+ endpoints, session tracking middleware
    graph_query.py                     NetworkX in-memory traversal engine
    sdg_alignment.py                   Pre-computed SDG coverage (cached)

Infrastructure:
  Firestore         25+ collections
  Cloud Run         Backend API (37+ revisions shipped)
  Cloud Build       Container CI/CD
  Cloud Scheduler   3 automated refresh jobs
  GCS               KG JSON + 5,000+ candidate photos
  Vercel            Frontend (edge CDN, auto-SSL)
```

---

## 🛠️ Tech Stack

| Layer | Technology | Why |
|---|---|---|
| **Frontend** | Next.js 15, React 19, Tailwind CSS 4, TypeScript | App-shell layout, SSR, mobile-first, 107MB prod memory |
| **Backend** | FastAPI, Python 3.14, Cloud Run | 40+ endpoints, auto-scaling, <200ms p95 |
| **Database** | Google Cloud Firestore | Serverless, 25+ collections, zero-ops |
| **Knowledge Graph** | NetworkX (in-memory) + GCS | Runtime traversal, 6.8K nodes, 1h TTL auto-refresh |
| **AI/ML** | Gemini 2.5 Pro (Vertex AI) | Multimodal PDF OCR, Tamil→English, structured extraction |
| **Caching** | localStorage (consent-gated, midnight TTL) + in-memory Maps | 0ms cache hits, idle prefetch, survives refresh |
| **Live Analytics** | Session piggyback → Firestore daemon flush | Zero extra requests, 1 write/min/instance, scales to 100K+ |
| **Photos** | GCS (immutable, 1yr cache) + Next.js Image optimizer | 5,000+ photos, CDN-served, 30-day optimizer cache |
| **Hosting** | Vercel + Cloud Run + GCS | Edge CDN, auto-SSL, zero-downtime deploys |

---

## ⚡ Performance

| Metric | Value |
|---|---|
| Production memory footprint | **107 MB** |
| Cache hit (state tab switch) | **0ms** (synchronous render from localStorage) |
| State Vitals API (warm) | ~50ms |
| SDG alignment (cached) | ~140ms |
| Politician profiles (5K records) | ~2s |
| Knowledge Graph load | ~3s (3.8MB → NetworkX) |
| Background prefetch | 12 endpoints on idle |
| Midnight cache expiry | Auto-fresh data every day |

---

## 📚 Data Sources

All data sourced from **publicly available government publications**:

| Source | Data |
|---|---|
| [ECI](https://eci.gov.in) | Candidate affidavits, results, photos |
| [MyNeta / ADR](https://myneta.info) | MLA assets, criminal records, education |
| [PLFS](https://mospi.gov.in) | Labour force surveys |
| [SRS](https://censusindia.gov.in) | Vital statistics |
| [HCES](https://mospi.gov.in) | Household consumption expenditure |
| [NCRB](https://ncrb.gov.in) | Crime statistics |
| [UDISE+](https://udiseplus.gov.in) | School education |
| [AISHE](https://aishe.gov.in) | Higher education |
| [ASI](https://mospi.gov.in) | Industrial statistics |
| [RBI](https://rbi.org.in) | State finances |
| [CAG](https://cag.gov.in) | Audited state accounts |
| [MOFPI](https://mofpi.gov.in) | Agriculture, food processing |
| [CEA / MNRE](https://cea.nic.in) | Power generation |
| [NITI Aayog](https://sdgindiaindex.niti.gov.in) | SDG India Index |
| [PRS](https://prsindia.org) | State budget analysis |
| [NFHS-5](https://rchiips.org/nfhs/) | Health & family welfare |
| [ASER](https://asercentre.org) | Education quality |

---

## 🖥️ Local Development

```bash
# Backend
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export GOOGLE_CLOUD_PROJECT=naatunadappu

# Frontend
cd web && npm install
make run-fe        # :3000
make run-be        # :8000
```

```bash
# web/.env.local
NEXT_PUBLIC_API_BASE_URL=http://localhost:8000
```

---

## 🤝 Contributing

Civic project. Non-partisan. If you care about transparent governance in Tamil Nadu:

- **Tamil translation** of UI text and tooltips
- **District-level datasets** (health, education, infrastructure)
- **Ward-level civic data** (ULB councillors, local body performance)
- **Fact-checking** manifesto promise statuses against government orders
- **Historical political context** (TN political history 1947→)

---

## ⚠️ Disclaimer

India is evolving — both in digital footprint and political transparency — but we're not there yet. **Some data here may be wrong.** Help us fix it.

**Be a citizen, not a spectator.**

இந்தியா வளர்ந்து வருகிறது — அதன் டிஜிட்டல் தடயத்திலும் சரி, அரசியல் வெளிப்படைத்தன்மையிலும் சரி — ஆயினும் நாம் இன்னும் முழு இலக்கை எட்டவில்லை. **இங்குள்ள சில தரவுகள் தவறாக இருக்கலாம்.** அவற்றைச் சரிசெய்ய எங்களுக்கு உதவுங்கள்.

**ஒரு பார்வையாளராக மட்டும் இராமல், பொறுப்புள்ள குடிமகனாகச் செயல்படுங்கள்.**

---

<p align="center">
  <strong>Built for the people of Tamil Nadu.</strong><br>
  Open source. Non-partisan. Evidence over rhetoric.<br><br>
  <a href="https://arasiyal-aayvu.vercel.app">Try it now →</a>
</p>
