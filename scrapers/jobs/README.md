# Scheduled Jobs

| Job | Script | Cadence | What it does |
|---|---|---|---|
| Fuel prices | `fuel_refresh.py` | Monthly (1st) | Scrapes LPG/petrol/diesel from Goodreturns → new `cost_of_living_india` snapshot |
| CoL non-fuel | `col_refresh.py` | 6-monthly (1 Apr, 1 Oct) | Scrapes Aavin dairy; flags electricity/PDS/transport for manual check → new `cost_of_living_tamil_nadu` snapshot |
| SDG Index | `sdg_check.py` | Annual (1 Jul) | Checks for new NITI CSV; ingests if found; prints step-by-step reminder if not |

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

## Google Cloud Scheduler setup

The jobs write directly to Firestore via Application Default Credentials.
To run them on a schedule, wrap each in a Cloud Run Job and trigger via Cloud Scheduler.

### 1. Create a Cloud Run Job for each script

```bash
# Build and push a scraper image (one-time)
gcloud builds submit --tag gcr.io/naatunadappu/kg-jobs .

# Create jobs
gcloud run jobs create fuel-refresh \
  --image gcr.io/naatunadappu/kg-jobs \
  --command ".venv/bin/python3" \
  --args "scrapers/jobs/fuel_refresh.py" \
  --region us-central1

gcloud run jobs create col-refresh \
  --image gcr.io/naatunadappu/kg-jobs \
  --command ".venv/bin/python3" \
  --args "scrapers/jobs/col_refresh.py,--skip-manual-check" \
  --region us-central1

gcloud run jobs create sdg-check \
  --image gcr.io/naatunadappu/kg-jobs \
  --command ".venv/bin/python3" \
  --args "scrapers/jobs/sdg_check.py" \
  --region us-central1
```

### 2. Schedule via Cloud Scheduler

```bash
# Fuel — 1st of every month, 06:30 IST (01:00 UTC)
gcloud scheduler jobs create http fuel-refresh-monthly \
  --schedule "30 1 1 * *" \
  --uri "https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/naatunadappu/jobs/fuel-refresh:run" \
  --oauth-service-account-email <SA>@naatunadappu.iam.gserviceaccount.com \
  --location us-central1

# CoL non-fuel — 1 April and 1 October, 07:00 IST (01:30 UTC)
gcloud scheduler jobs create http col-refresh-biannual \
  --schedule "30 1 1 4,10 *" \
  --uri "https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/naatunadappu/jobs/col-refresh:run" \
  --oauth-service-account-email <SA>@naatunadappu.iam.gserviceaccount.com \
  --location us-central1

# SDG — 1 July every year, 09:00 IST (03:30 UTC)
gcloud scheduler jobs create http sdg-check-annual \
  --schedule "30 3 1 7 *" \
  --uri "https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/naatunadappu/jobs/sdg-check:run" \
  --oauth-service-account-email <SA>@naatunadappu.iam.gserviceaccount.com \
  --location us-central1
```

Replace `<SA>` with the actual service account name.

## Cron schedule summary

```
# fuel prices — monthly
30 1 1 * *     fuel_refresh.py

# CoL non-fuel — 1 Apr and 1 Oct
30 1 1 4,10 *  col_refresh.py

# SDG check — 1 Jul
30 3 1 7 *     sdg_check.py
```
