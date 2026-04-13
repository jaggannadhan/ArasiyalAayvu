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
  Open-source political transparency platform for Tamil Nadu.<br>
  Turning government data into citizen power.
</p>

---

## What is this?

ArasiyalAayvu is a **non-partisan civic tech platform** that aggregates public data about Tamil Nadu's elected representatives, government performance, and socioeconomic indicators into a single, accessible interface.

Every MLA's assets, criminal record, education, and election promises — alongside state-level metrics on health, education, employment, crime, and industry — queryable by anyone with a phone.

**The goal:** an informed electorate that votes on evidence, not rhetoric.

## Why this exists

- **234 constituencies.** Most voters can't name their MLA's declared assets or pending criminal cases.
- **Government data is scattered** across 15+ portals (ECI, MyNeta, NCRB, MOSPI, NITI Aayog, UDISE+, PLFS, SRS...) in inconsistent formats. Nobody cross-references them.
- **Manifestos are forgotten** the day after elections. No public tracker maps promises to outcomes.
- **Youth turnout** in TN state elections is rising, but accessible tools for political research barely exist.

ArasiyalAayvu fixes this by ingesting, normalizing, and presenting all of it in one place.

## Core Features

### Know Your Constituency
Search any of 234 assembly constituencies. See the sitting MLA's profile — assets, criminal cases, education — alongside every candidate contesting in 2026, with ECI affidavit data and party affiliation.

### Manifesto Tracker
Party promises mapped against delivery status. DMK 2021 manifesto commitments tracked with evidence. 2026 contesting party manifestos compared side-by-side.

### State Vitals Dashboard
Real-time state-level indicators across 9 datasets:

| Domain | Source | Metrics |
|---|---|---|
| Labour | PLFS (MoSFA) | Unemployment rate, LFPR, worker distribution |
| Health | SRS (RGI) | Birth/death/infant mortality rates |
| Spending | HCES (MoSFA) | Monthly per-capita expenditure, rural/urban split |
| Higher Ed | AISHE (MoE) | GER, colleges, faculty count |
| School Ed | UDISE+ (MoE) | GER by level, dropout rates, PTR, infrastructure |
| Crime | NCRB (MHA) | IPC crimes, crimes against women/children/SC/ST |
| Industry | ASI (MOSPI) | Factory count, GVA, output, employment |
| SDG | NITI Aayog | 16-goal composite score, goal-wise breakdown |
| Cost of Living | Multiple | Fuel, dairy, electricity, PDS, transport |

Five south Indian states benchmarked: **TN, KL, KA, AP, TG** + All India reference.

### Constituency Map
Pincode-to-constituency lookup. Enter your PIN, get your MLA.

### SDG Tracker
Tamil Nadu's Sustainable Development Goals performance — 17 goals, state vs national, trend over time.

## Architecture

```
scrapers/          50+ Python ingestors & scrapers
  jobs/            Scheduled refresh jobs (Cloud Run Jobs + Cloud Scheduler)
  ts_utils.py      Shared time-series utilities (JSON + Firestore)

web/
  src/app/         Next.js 15 app router (6 page routes)
  backend_api/     FastAPI service (Firestore reads, search, KG API)

Firestore          20+ collections (candidates, MLAs, manifestos, KG datasets)
Cloud Run          Backend API (asia-south1)
Cloud Scheduler    3 automated data refresh jobs
Vercel             Frontend hosting
```

### Data Pipeline

```
Government portals    Scrapers (Python)    Firestore    API (FastAPI)    Frontend (Next.js)
     ECI           -->                  -->           -->              -->
     MyNeta        -->   50+ ingestors  -->  20+ collections  -->  /api/*  -->  React UI
     MOSPI         -->                  -->           -->              -->
     NCRB          -->                  -->           -->              -->
     ...           -->                  -->           -->              -->
```

### Scheduled Jobs

| Job | Cadence | What it refreshes |
|---|---|---|
| `fuel-refresh` | Monthly | LPG, petrol, diesel prices |
| `col-refresh` | 6-monthly | Dairy prices + manual verification flags |
| `sdg-check` | Annual | NITI Aayog SDG India Index |

## Tech Stack

| Layer | Tech |
|---|---|
| Frontend | Next.js 15, React 19, Tailwind CSS 4, TypeScript |
| Backend API | FastAPI, Python 3.12, Cloud Run |
| Database | Google Cloud Firestore |
| Data pipelines | Python (BeautifulSoup, pdfplumber, openpyxl, Playwright) |
| Infra | GCP (Cloud Run, Cloud Build, Cloud Scheduler, GCR) |
| Analytics | Vercel Analytics |

## Local Development

### Backend (Python pipelines)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Frontend

```bash
cd web
npm install
npm run dev
```

### Backend API (local)

```bash
cd web
uvicorn backend_api.main:app --reload --port 8080
```

## Data Sources

All data is sourced from **publicly available government publications and portals**:

- [Election Commission of India](https://www.eci.gov.in) — candidate affidavits, results
- [MyNeta / ADR](https://www.myneta.info) — MLA assets, criminal records
- [PLFS](https://www.mospi.gov.in) — labour force surveys
- [SRS](https://censusindia.gov.in/census.website/) — vital statistics
- [NCRB](https://www.ncrb.gov.in) — crime statistics
- [UDISE+](https://udiseplus.gov.in) — school education metrics
- [AISHE](https://aishe.gov.in) — higher education statistics
- [MOSPI / ASI](https://www.mospi.gov.in) — industrial statistics
- [NITI Aayog](https://sdgindiaindex.niti.gov.in) — SDG India Index
- [NFHS-5](https://rchiips.org/nfhs/) — health and family welfare
- [ASER](https://asercentre.org) — education quality surveys

## Contributing

This is a civic project. If you care about transparent governance in Tamil Nadu, you're welcome here.

**Areas where help is needed:**
- Tamil translation of the UI
- More district-level datasets
- Ward-level civic data (ULB councillors, local body performance)
- Fact-checking manifesto promise statuses

## License

Open source. Built for the people of Tamil Nadu.
