# ArasiyalAayvu Web + Backend API

This repository contains:
- `web` frontend (Next.js, mobile + desktop)
- `backend_api` Python API (FastAPI) that reads Firestore server-side

The frontend no longer reads Firestore directly from the browser. It calls Python APIs.

## Run Frontend

```bash
cd /Users/jv/Documents/MyProjects/ArasiyalAayvu/web
npm install
make run-fe
```

## Run Backend API

```bash
cd /Users/jv/Documents/MyProjects/ArasiyalAayvu/web
make install-be-deps
make run-be
```

Backend runs on `http://localhost:8000` by default.

## Frontend API base URL

Create `web/.env.local` if your API is not on localhost:

```bash
NEXT_PUBLIC_API_BASE_URL=http://localhost:8000
```

## API Endpoints

- `GET /health`
- `GET /api/manifesto-promises?year=all|2021|2026`
- `GET /api/constituency/{slug}`

## Firestore Credentials (server-side)

The backend API uses Google Cloud credentials (ADC/service account), not browser API keys.

Local options:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/service-account.json
export GOOGLE_CLOUD_PROJECT=naatunadappu
```

For deployment, attach a service account to the runtime (Cloud Run/GKE/VM), and keep Firestore access server-side only.
