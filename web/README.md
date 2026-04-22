# ArasiyalAayvu — Web + Backend API

The user-facing application: Next.js 15 frontend + FastAPI backend.

See the [root README](../README.md) for the full project overview.

## Quick Start

```bash
cd web
npm install
make run-fe        # Frontend on http://localhost:3000
make run-be        # Backend on http://localhost:8000
```

## Environment

Create `web/.env.local`:

```bash
NEXT_PUBLIC_API_BASE_URL=http://localhost:8000
VERCEL_API_KEY=<optional-vercel-token>
```

Backend requires Google Cloud credentials:

```bash
export GOOGLE_CLOUD_PROJECT=naatunadappu
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

## Structure

```
src/app/                    8 page routes (App Router)
src/components/             Shared components (ProfileModal, InfoTip, LiveCount, etc.)
src/lib/                    Client infra (data-cache, LanguageContext, CookieConsent, api-client)
src/hooks/                  Custom hooks (useManifestos)
backend_api/
  main.py                   40+ API endpoints
  graph_query.py            NetworkX runtime traversal
  sdg_alignment.py          Pre-computed SDG coverage
```

## Deployment

- **Frontend:** Vercel (auto-deploys from `main` branch)
- **Backend:** Cloud Run via `gcloud builds submit --config cloudbuild.yaml`
