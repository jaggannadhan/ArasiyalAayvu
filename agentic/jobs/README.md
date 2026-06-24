# Agentic Jobs — activation & scheduling

Runs the agentic control plane (Modules 4–6) on a schedule, and publishes the
GraphRAG index the backend serves. Safe-by-default: every job is **suggest
mode** unless you pass `--act`.

| Job | Entrypoint | Suggested cadence | What it does |
|---|---|---|---|
| Source Watcher | `python -m agentic.jobs.watch_job` | Daily 05:00 IST | Detect new data across configured sources; persist fingerprints + a run record. `--act` triggers configured tools. |
| Feedback Triage | `python -m agentic.jobs.triage_job` | Daily 07:00 IST | Classify/route/de-dupe new `feedback`; move `new → triaged`. `--act` triggers tools for routed items. |
| GraphRAG Index | `python -m agentic.jobs.build_index_job --vertex` | Weekly / after data refresh | Rebuild the index from GCS KG + Firestore promises; upload `graphrag/latest.json`. |

## Local dry-run (no writes)

```bash
python -m agentic.jobs.watch_job  --dry-run
python -m agentic.jobs.triage_job --dry-run
```

## Cloud infrastructure

**Project:** `naatunadappu` · **Region:** `asia-south1` · **Image:**
`gcr.io/naatunadappu/agentic-jobs` · **SA:** `301895032269-compute@developer.gserviceaccount.com`

This is a **new, isolated image** (built from the repo root) — it does not touch
the existing `kg-jobs` or backend images.

### 1. Build the image

```bash
gcloud builds submit --config cloudbuild-agentic-jobs.yaml .
```

### 2. Create the Cloud Run Jobs

```bash
gcloud run jobs create agentic-watch \
  --image gcr.io/naatunadappu/agentic-jobs --region asia-south1 \
  --args agentic.jobs.watch_job \
  --service-account 301895032269-compute@developer.gserviceaccount.com

gcloud run jobs create agentic-triage \
  --image gcr.io/naatunadappu/agentic-jobs --region asia-south1 \
  --args agentic.jobs.triage_job \
  --service-account 301895032269-compute@developer.gserviceaccount.com

gcloud run jobs create graphrag-build-index \
  --image gcr.io/naatunadappu/agentic-jobs --region asia-south1 \
  --memory 1Gi --args agentic.jobs.build_index_job \
  --service-account 301895032269-compute@developer.gserviceaccount.com
```

> To enable acting (not just suggesting), append `,--act` to the `--args` of the
> watch / triage jobs once you trust them. For Vertex embeddings on the index
> job use `--args agentic.jobs.build_index_job,--vertex` (and uncomment
> `google-cloud-aiplatform` in `agentic/jobs/requirements.txt`).

### 3. Scheduler triggers (IST)

```bash
for J in "agentic-watch:0 5 * * *" "agentic-triage:0 7 * * *" "graphrag-build-index:0 4 * * 1"; do
  NAME="${J%%:*}"; CRON="${J##*:}"
  gcloud scheduler jobs create http "${NAME}-sched" \
    --location=asia-south1 --schedule="$CRON" --time-zone="Asia/Kolkata" \
    --uri="https://asia-south1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/naatunadappu/jobs/${NAME}:run" \
    --http-method=POST \
    --oauth-service-account-email="301895032269-compute@developer.gserviceaccount.com" \
    --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform"
done
```

### Manual run

```bash
gcloud run jobs execute agentic-watch --region asia-south1 --wait
```

---

## Activating the backend `/api/ask` endpoint

The endpoint (added in Module 6) is live-safe but returns 503 until two things
are in place:

**a) Publish the index** (one-off + whenever data changes):
```bash
gcloud run jobs execute graphrag-build-index --region asia-south1 --wait
# → writes gs://naatunadappu-media/graphrag/latest.json
```

**b) Make `agentic` available to the backend image.** The backend currently
builds from the `web/` context, which excludes the repo-root `agentic/` package.
Apply ONE of:

- *Preferred:* change the backend build context to the repo root —
  in `cloudbuild.yaml` drop `dir: 'web'`, set the context to `.`, and use
  `-f web/backend_api/Dockerfile`; then add to `web/backend_api/Dockerfile`:
  `COPY agentic/ ./agentic/  &&  COPY schemas/ ./schemas/` and add `numpy` to
  `web/backend_api/requirements.txt`.
- *Or:* vendor a copy of `agentic/graphrag.py` (+ its deps) under `web/backend_api/`.

> These backend-build changes touch a live deploy and could not be tested in the
> dev sandbox — verify locally first: run `make run-be` from the repo root and
> `curl "localhost:8000/api/ask?q=loan%20waiver%20for%20farmers"`.

---

## Provenance

`main.py` now wraps every ETL task in a `RunContext`, and these jobs record their
runs too — so `python -m agentic recent` (or the `runs` collection) shows the
full operating history. Set `AAYVU_SCHEMA_VALIDATION=warn` (or `strict` in CI)
to log schema drift at the write boundary.
