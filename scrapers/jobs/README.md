# Scheduled Jobs

| Job | Script | Cadence | What it does |
|---|---|---|---|
| Fuel prices | `fuel_refresh.py` | Monthly (1st, 01:00 IST) | Scrapes LPG/petrol/diesel from Goodreturns → new `cost_of_living_india` snapshot |
| CoL non-fuel | `col_refresh.py` | 6-monthly (1 Apr, 1 Oct, 07:00 IST) | Scrapes Aavin dairy; flags electricity/PDS/transport for manual check → new `cost_of_living_tamil_nadu` snapshot |
| SDG Index | `sdg_check.py` | Annual (1 Jul, 09:00 IST) | Checks for new NITI CSV; ingests if found; prints step-by-step reminder if not |

## Running locally

```bash
# dry-run (fetch + print, no write)
.venv/bin/python3 scrapers/jobs/fuel_refresh.py --dry-run
.venv/bin/python3 scrapers/jobs/col_refresh.py  --dry-run

# actual run
.venv/bin/python3 scrapers/jobs/fuel_refresh.py
.venv/bin/python3 scrapers/jobs/col_refresh.py
.venv/bin/python3 scrapers/jobs/sdg_check.py
```

## Cloud infrastructure

**GCP Project:** `naatunadappu`
**Region:** `asia-south1`
**Image:** `gcr.io/naatunadappu/kg-jobs`
**Service account:** `301895032269-compute@developer.gserviceaccount.com`

### Cloud Run Jobs

| Job name | Entrypoint |
|---|---|
| `fuel-refresh` | `python scrapers/jobs/fuel_refresh.py` |
| `col-refresh` | `python scrapers/jobs/col_refresh.py --skip-manual-check` |
| `sdg-check` | `python scrapers/jobs/sdg_check.py` |

### Cloud Scheduler triggers

| Scheduler name | Cron (IST) | Target |
|---|---|---|
| `fuel-refresh-monthly` | `0 1 1 * *` (1st of month, 01:00) | `fuel-refresh` |
| `col-refresh-biannual` | `0 7 1 4,10 *` (1 Apr, 1 Oct, 07:00) | `col-refresh` |
| `sdg-check-annual` | `0 9 1 7 *` (1 Jul, 09:00) | `sdg-check` |

### Rebuilding the jobs image

```bash
cd scrapers
gcloud builds submit --config ../cloudbuild-jobs.yaml .
```

Then update each Cloud Run Job to use the new image:

```bash
gcloud run jobs update fuel-refresh --image gcr.io/naatunadappu/kg-jobs --region asia-south1
gcloud run jobs update col-refresh  --image gcr.io/naatunadappu/kg-jobs --region asia-south1
gcloud run jobs update sdg-check    --image gcr.io/naatunadappu/kg-jobs --region asia-south1
```

### Manual execution

```bash
gcloud run jobs execute fuel-refresh --region asia-south1 --wait
gcloud run jobs execute col-refresh  --region asia-south1 --wait
gcloud run jobs execute sdg-check    --region asia-south1 --wait
```
