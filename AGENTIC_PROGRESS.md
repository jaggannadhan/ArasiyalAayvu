# Agentic Transformation — Progress Tracker

Plan-of-record for evolving ArasiyalAayvu into a self-managing agentic platform,
per **Part II** of [`architecture.md`](./architecture.md). Each module is built on
its own `feat/*` branch, validated end-to-end, then handed off for push + deploy.

**Working agreement**
- One module at a time; modular and independently shippable.
- One feature branch per module (`feat/agentic-*`).
- Validated end-to-end before push (no live GCP creds in dev → Firestore emulator + mocks).
- Author commits locally; **owner reviews, pushes to GitHub, and deploys** (Vercel / Cloud Run).

---

## Status board

| # | Module | Branch | Status | Validation |
|---|---|---|---|---|
| 1 | Schemas — Pydantic contracts + validation engine | `feat/agentic-schemas` (merged to main) | ✅ **Done** | 21 unit/integration tests; real data files |
| 2 | Provenance + `runs` collection | `feat/agentic-provenance` (merged to main) | ✅ **Done** | 32 tests total; in-mem fake client + emulator recipe |
| 3 | Tool Registry | `feat/agentic-tool-registry` (merged to main) | ✅ **Done** | 42 tests total; 16 tools resolve |
| 4 | Source Watcher (generalize `sdg_check`) | `feat/agentic-source-watcher` (merged to main) | ✅ **Done** | 55 tests total; 4 detectors, real-data poll |
| 5 | Feedback Triage worker | `feat/agentic-feedback-triage` (merged to main) | ✅ **Done** | 67 tests total; classify/route/dedup |
| 6 | GraphRAG vector index + cited Q&A | `feat/agentic-graphrag` (merged to main) | ✅ **Done** | 78 tests total; real-data ask + /api/ask |
| 7 | Activation — Cloud Run Jobs + provenance wiring | `feat/agentic-activation` (merged + deployed) | ✅ **Done** | 83 tests; jobs scheduled, /api/ask live in prod |
| 8 | GraphRAG retrieval dedup | `feat/agentic-graphrag-dedup` (merged to main) | ✅ **Done** | 85 tests; collapses KG-node/promise-doc overlap |
| 9 | Critic / Verifier — data-quality gate | `feat/agentic-verifier` (merged to main) | ✅ **Done** | 98 tests; range/outlier/consistency/hallucination |
| 10 | Refresh (composite) tools | `feat/agentic-refresh-tools` (off main) | ✅ **Done (awaiting push)** | 104 tests; 4 argument-free domain pipelines |

Dependency order: **1 → 2 → 3 → (4, 5) → 6**. Modules 4–6 consume the schemas (1),
provenance/run-log (2), and tool registry (3).

---

## Module 1 — Schemas ✅

**Branch:** `feat/agentic-schemas`

**What it delivers**
- `schemas/` package: typed Pydantic v2 contracts for 11 core Firestore collections
  (`assembly_elections`, `alliances`, `candidate_accountability`,
  `party_accountability`, `manifesto_promises`, `politician_profile`,
  `state_finances`, `debt_history`, `departmental_spending`, `socio_economics`,
  `feedback`) plus knowledge-graph node/edge models.
- A validation engine (`schemas/validate.py`) that separates **hard errors**
  (missing required field, wrong type, bad `Literal`) from **soft warnings**
  (categorical value outside the observed vocabulary → drift signal).
- A `registry.py` mapping collections → models, id fields, and enum domains.
- A non-breaking, opt-in validation hook at the single write boundary
  (`loaders/firestore_loader._batch_upload`), controlled by
  `AAYVU_SCHEMA_VALIDATION = off | warn | strict` (default `warn`).
- CLI: `python -m schemas <collection> <path.json> [--strict]`.

**Design choices**
- `extra="allow"` on every model so new fields and loader metadata
  (`_uploaded_at`, `_schema_version`) never break validation — we enforce the
  *contract*, not the whole payload.
- Validation never blocks production writes unless `AAYVU_SCHEMA_VALIDATION=strict`.

**Validation evidence**
- `pytest tests/test_schemas.py` → **21 passed**.
- Real processed data validated with **zero errors**: 1,889 manifesto promises
  (NTK 1065 / DMK 525 / AIADMK 299), 224 MLA winner records, debt history,
  departmental spending, party rollups, elections.
- Enum domains harvested from real data so warnings are meaningful.

**Files**
```
schemas/{__init__,_base,political,finance,socio,civic,graph,registry,validate}.py
loaders/firestore_loader.py        (added opt-in _validate_chunk hook)
tests/{__init__,test_schemas}.py
requirements-dev.txt
```

**How to verify locally**
```bash
pip install -r requirements-dev.txt
python -m pytest tests/test_schemas.py -q
python -m schemas manifesto_promises data/processed/manifesto_promises_2026_dmk.json
```

**Push / deploy notes**
- Pure additive Python; no frontend/Cloud Run change required.
- Default mode is `warn`; set `AAYVU_SCHEMA_VALIDATION=strict` in a scraper/CI run
  to enforce. No behavioural change to existing uploads otherwise.

---

## Module 2 — Provenance + `runs` collection ✅

**Branch:** `feat/agentic-provenance` (stacked on `feat/agentic-schemas`)

**What it delivers**
- New `agentic/` package — the control-plane home.
  - `agentic/provenance.py` — `RunContext` context manager that records every
    pipeline run to the `runs` collection (`status`, `started/finished`,
    `duration_s`, `rows_written`, per-collection `writes`, `errors`,
    `parent_run_id`). Active run is discoverable via `contextvars`
    (`current_run()` / `current_run_id()`), so scrapers need no signature change.
  - `RunStore` interface with `FirestoreRunStore` (lazy client — no creds needed
    to import) and `InMemoryRunStore` (tests); pluggable via
    `get_default_store` / `set_default_store`.
  - `agentic/rollback.py` — `SnapshotStore.snapshot(...)` / `rollback(token)` for
    reversible writes (restores edited docs, removes newly-created ones).
- `schemas/runs.py` — `RunRecordDoc` contract; `runs` registered in the schema
  registry (Module 1 reused).
- Loader now stamps `_provenance = {run_id, tool, written_at}` on every written
  doc and auto-increments the active run's counters — best-effort, a no-op when
  no run is active (uncoordinated uploads behave exactly as before).
- CLI: `python -m agentic demo` (offline) and `python -m agentic recent [N]`.

**Design choices**
- Provenance/logging is best-effort everywhere: a store failure never aborts a
  real ingestion run (same principle as Module 1's validation hook).
- Importing `agentic.provenance` pulls in **no** GCP libraries — Firestore is
  imported lazily inside `FirestoreRunStore` only.

**Validation evidence**
- `pytest tests/` → **32 passed** (21 M1 + 11 M2).
- Loader integration verified with an in-memory Firestore fake mirroring the
  real client surface (`collection/document/get/set/delete/batch`): an upload
  inside a `RunContext` stamps `_provenance` and records `rows_written`.
- Snapshot/rollback verified (restore + delete paths); run records validate
  against the `runs` schema contract.

**Files**
```
agentic/{__init__,provenance,rollback,__main__}.py
schemas/runs.py                 (+ registry/__init__ wiring)
loaders/firestore_loader.py     (provenance stamping + auto row-count)
tests/test_provenance.py
```

**How to verify locally**
```bash
python -m pytest tests/ -q                 # 32 passed
python -m agentic demo                      # prints a sample run record
```

**Verify against the Firestore emulator (optional, matches dev preference)**
```bash
gcloud beta emulators firestore start --host-port=localhost:8085 &
export FIRESTORE_EMULATOR_HOST=localhost:8085
export GOOGLE_CLOUD_PROJECT=naatunadappu
python - <<'PY'
from agentic import RunContext
from agentic.provenance import FirestoreRunStore, set_default_store
set_default_store(FirestoreRunStore())          # writes to the emulator
with RunContext(tool="smoke", trigger="manual") as r:
    r.record_write("manifesto_promises", 3)
print("wrote run", r.run_id)
PY
python -m agentic recent 5                       # reads it back from the emulator
```

**Push / deploy notes**
- Additive; no behavioural change unless code runs inside a `RunContext`.
- The loader stamps `_provenance` only when a run is active — safe to merge
  before any scraper is wrapped.

---

## Module 3 — Tool Registry ✅

**Branch:** `feat/agentic-tool-registry` (stacked on `feat/agentic-provenance`)

**What it delivers**
- `agentic/tools.py` — `ToolSpec` / `ToolRegistry` / `ToolResult`.
  - Tools are declared by **dotted import path** and resolved lazily, so the
    registry imports nothing heavy (Playwright, Gemini, google.cloud) until a
    tool is actually invoked.
  - `ToolRegistry.invoke(name, args)` wraps the call in a Module-2 `RunContext`
    (automatic provenance + `runs` record), passes args as kwargs, **validates
    the returned docs against the Module-1 schema** of the tool's
    `output_collection`, and returns a structured `ToolResult` (status, output,
    output_validation, error, rows_written, run_id) instead of raising — so a
    planner can inspect and react.
  - Args are summarised (`list[1234]`) in the run record, never stored raw.
- `agentic/catalog.py` — 16 tools wrapping existing transformers (pure) and
  loaders (Firestore), tagged and annotated with reads/writes/side-effects.
- CLI: `python -m agentic tools` lists the catalogue.

**Why this shape**
- This is the surface the autonomous agents (Modules 4-6) plan over: discover
  tools by capability, invoke uniformly, get typed results — with every step
  automatically audited (M2) and schema-checked (M1). M1 + M2 + M3 now compose.

**Validation evidence**
- `pytest tests/` → **42 passed** (21 M1 + 11 M2 + 10 M3).
- All **16 catalogue tools resolve** to real callables (no path typos).
- Real transformer invoked end-to-end (`transform.debt_history`): output
  validated against the `debt_history` schema, run recorded.
- Error capture, invalid-output flagging, and arg summarisation verified.

**Files**
```
agentic/{tools,catalog}.py
agentic/{__init__,__main__}.py   (registry exports + `tools` command)
tests/test_tools.py
```

**How to verify locally**
```bash
python -m pytest tests/ -q        # 42 passed
python -m agentic tools           # list the 16 registered tools
```

**Push / deploy notes**
- Additive; imports nothing heavy. No runtime/behavioural change to the app.

---

## Module 4 — Source Watcher ✅

**Branch:** `feat/agentic-source-watcher` (off `main`, which now carries M1-M3)

> **Branching switched to merge-as-you-go (#2):** M1-M3 were fast-forwarded onto
> `main`; from M4 on, each module branches off `main` and is merged back after
> review — no more long stacks.

**What it delivers**
- `agentic/sources.py` — generalises `scrapers/jobs/sdg_check.py` into a
  config-driven watcher with four pluggable detectors:
  `file_present` (new file in a dir — the sdg_check case), `http_hash`
  (page body changed, optional `extract_regex` to cut noise), `http_header`
  (ETag / Last-Modified), `json_field` (a JSON field changed).
- Per-source fingerprints persist via a `SourceStateStore`
  (`InMemory` + `Firestore` (`source_state`)), so a check is a comparison
  against history.
- `SourceWatcher.poll(...)` wraps the whole sweep in a Module-2 `RunContext`;
  detector/network failures are isolated per source (one bad source never
  aborts the sweep). Defaults to **suggest mode** (detect + report only); with
  `act=True` it triggers the configured Module-3 registry tool, nested under the
  poll run (parent/child run linkage).
- `agentic/sources_config.py` — declarative catalogue (NITI SDG CSV + web, PRS
  TN budget, Chennai fuel) mirroring the real refresh jobs.
- Network is injected (`fetch`), so all detectors are unit-tested offline.
- CLI: `python -m agentic sources` / `python -m agentic watch`.

**Validation evidence**
- `pytest tests/` → **53 passed** (21 M1 + 11 M2 + 10 M3 + 11 M4).
- All four detectors verified (baseline → change), extract_regex noise-immunity,
  error isolation (poll marked `partial`), `act=True` tool trigger with parent
  linkage, and suggest-mode-does-not-trigger.
- Real-data poll of `niti_sdg_csv` against `data/raw/niti_sdg/` correctly
  reports "no new files" (the 3 known CSVs are recognised).

**Files**
```
agentic/{sources,sources_config}.py
agentic/{__init__,__main__}.py   (exports + sources/watch commands)
tests/test_sources.py
```

**How to verify locally**
```bash
python -m pytest tests/ -q        # 53 passed
python -m agentic sources         # list watched sources
python -m agentic watch           # poll (suggest mode; http sources hit network)
```

**Push / deploy notes**
- Additive; nothing runs it automatically yet. To schedule, a future Cloud Run
  Job can call `SourceWatcher(FirestoreSourceStateStore()).poll(get_sources())`
  — the natural replacement for the bespoke `jobs/*_check.py` scripts.

---

## Module 5 — Feedback Triage ✅

**Branch:** `feat/agentic-feedback-triage` (off `main`)

**What it delivers**
- `agentic/feedback.py` — finally consumes the `feedback` collection (nothing
  read it before). `FeedbackTriager`:
  - **classifies** each item: `priority` (high/medium/low, escalated by
    factual/severe terms), `route` (data / engineering / product — by category,
    so a bug always goes to engineering regardless of page), and `domain` +
    `target` + `target_collection` (what it's about, resolved from
    `entity_context` or by parsing `page_url`, e.g. `/constituency/kolathur`).
  - **de-duplicates** near-identical reports (category + domain + target +
    message head) — duplicates flagged, not re-queued.
  - **routes** by transitioning `status` `new → triaged` and attaching a
    `triage` record (the review queue is `feedback where status==triaged`).
  - **confidence** score reflecting how concretely the target was resolved.
- `FeedbackStore` (InMemory + Firestore `feedback`).
- Defaults to **suggest mode** (triage + queue only — corrections are never
  auto-applied; they need human verification). With `act=True` + a
  `route_tools` mapping it triggers a Module-3 tool, nested under the triage run.
- Each sweep wrapped in a Module-2 `RunContext`. CLI: `python -m agentic triage`.

**Design choice worth noting**
- **route vs domain** are kept separate: *route* is who handles it (stable, by
  category), *domain* is what it concerns (by page/entity). Conflating them sent
  bug reports to the wrong queue — splitting them fixed it.

**Validation evidence**
- `pytest tests/` → **67 passed** (21 M1 + 11 M2 + 10 M3 + 13 M4 + 12 M5).
- Classification, URL/entity target resolution, dedup, status transitions,
  suggest-mode-no-trigger, and `act=True` tool trigger with parent linkage all
  verified. End-to-end in-memory triage demo produces sensible queues.

**Files**
```
agentic/feedback.py
agentic/{__init__,__main__}.py   (exports + triage command)
tests/test_feedback.py
```

**How to verify locally**
```bash
python -m pytest tests/ -q        # 67 passed
python -m agentic triage          # triage new feedback (needs Firestore creds)
```

**Push / deploy notes**
- Additive; reads/updates `feedback` only when run. A future Cloud Run Job can
  call `FeedbackTriager().run(FirestoreFeedbackStore())` on a schedule. The
  backend could later surface `status==triaged` items in an admin view.

---

## Module 6 — GraphRAG vector index + cited Q&A ✅

**Branch:** `feat/agentic-graphrag` (off `main`)

**What it delivers**
- `agentic/graphrag.py` — makes the README's "GraphRAG" real.
  - **Embedder** (pluggable): `HashingEmbedder` — deterministic, offline,
    whole-word + char 3/4-gram hashing so morphological variants overlap
    (farmer~farmers, loan~loans); `VertexEmbedder` for production (lazy).
  - **VectorIndex** — in-memory numpy cosine search (sub-10ms at this scale),
    JSON save/load.
  - **GraphRAG** — builds records from KG nodes + manifesto promises, retrieves
    by similarity, then **expands KG hits through graph neighbours** for context;
    answers are **extractive + cited by default**, with an optional pluggable
    `GeminiSynthesizer` (grounded, cite-only prompt).
  - Persists index **+ adjacency** together; `build_local_index` / `answer_question`
    convenience for CLI and the API.
- `GET /api/ask?q=&k=&synth=` added to `web/backend_api/main.py` — loads the
  index from GCS (`graphrag/latest.json`) with a local fallback, cached 1h.
- CLI: `python -m agentic build-index` and `python -m agentic ask "..."`.

**Validation evidence**
- `pytest tests/` → **78 passed** (21 M1 + 11 M2 + 10 M3 + 13 M4 + 12 M5 + 11 M6).
- Real-data build: **8,760 records** (6,871 KG nodes + 1,889 promises) indexed
  offline in ~2s. `ask "loan waiver for farmers"` and `"monthly cash assistance
  for women"` return the right promises across parties, with citations and SDG
  graph-neighbour expansion.
- A modeling fix mid-build: pure exact-token hashing missed word variants and
  matched on stopwords — added stopword removal + char n-grams (caught by the
  ranking test before it shipped).

**Files**
```
agentic/graphrag.py
agentic/{__init__,__main__}.py        (exports + build-index/ask commands)
web/backend_api/main.py               (GET /api/ask endpoint)
tests/test_graphrag.py
.gitignore                            (ignore generated data/processed/graphrag_index.json)
```

**How to verify locally**
```bash
python -m pytest tests/ -q                       # 78 passed
python -m agentic build-index                     # 8,760 records -> data/processed/
python -m agentic ask "free bus travel for women"
# backend endpoint (dev, run from repo root so `agentic` is importable):
make run-be   # then: curl "localhost:8000/api/ask?q=loan%20waiver%20for%20farmers"
```

**⚠️ Deployment wiring required for /api/ask (not done here — needs your infra)**
The backend image is built from the `web/` context, so it does **not** contain
the repo-root `agentic/` package or the index. Before the endpoint works in prod:
1. `COPY agentic/ ./agentic/` (and `numpy`) into `web/backend_api/Dockerfile`,
   or pip-install the repo into the image.
2. Build the index and upload it:
   `python -m agentic build-index` →
   `gsutil cp data/processed/graphrag_index.json gs://naatunadappu-media/graphrag/latest.json`
   (use `VertexEmbedder` for production-quality embeddings; the endpoint
   reconstructs the matching embedder from the index's stored model name).
The endpoint is defensive: until then it returns a 503, never crashing the app.

---

## Module 7 — Activation (deploy + scheduling) ✅

**Branch:** `feat/agentic-activation` (off `main`)

**What it delivers** — turns the six tested libraries into something that runs.
- `agentic/jobs/` — three Cloud Run Job entrypoints with testable cores
  (injected deps) + thin `main()` wrappers:
  - `watch_job` — Source Watcher poll against `FirestoreSourceStateStore` +
    `FirestoreRunStore`.
  - `triage_job` — Feedback Triage against `FirestoreFeedbackStore`.
  - `build_index_job` — rebuild GraphRAG index from GCS KG + Firestore promises,
    upload `graphrag/latest.json` (HashingEmbedder default, `--vertex` for prod).
  - All **suggest-mode by default**; `--act` opts into triggering; `--dry-run`
    is read-only.
- A **new isolated image** `gcr.io/naatunadappu/agentic-jobs`
  (`agentic/jobs/Dockerfile` + `cloudbuild-agentic-jobs.yaml`, repo-root context)
  — does not touch the existing `kg-jobs` or backend images.
- `agentic/jobs/README.md` — build + Cloud Run Job + Scheduler commands, and the
  exact steps to activate `/api/ask` (publish the index; make `agentic`
  available to the backend image).
- `main.py` now wraps every ETL task in a `RunContext` (guarded — falls back to
  a nullcontext if the agentic layer is absent), activating provenance for the
  existing pipeline.

**Validation evidence**
- `pytest tests/` → **82 passed** (78 + 4 job-core tests). `main.py` and all job
  modules compile.
- Job cores tested offline with in-memory stores + injected sources/upload;
  each records its run.

**Files**
```
agentic/jobs/{__init__,watch_job,triage_job,build_index_job,Dockerfile,requirements.txt,README.md}
cloudbuild-agentic-jobs.yaml
main.py                              (RunContext wrap of task dispatch)
tests/test_jobs.py
```

**Deployed (confirmed live in `naatunadappu`):**
- `agentic-jobs` image built + pushed; Cloud Run Jobs `agentic-watch`,
  `agentic-triage`, `graphrag-build-index` created; Scheduler triggers ENABLED;
  `run.invoker` granted to the runtime SA.
- Manual executions succeeded — `python -m agentic recent` shows
  `build_index_job` (8,795 records), `source_watcher.poll`, and
  `feedback_triage.run` all recorded. The index is published to
  `gs://naatunadappu-media/graphrag/latest.json`.

**`/api/ask` wiring (done in code):** `agentic/graphrag.py` is vendored into
`web/backend_api/graphrag.py` (byte-identical, guarded by a sync test) and the
endpoint imports it; `numpy` added to backend requirements. **Remaining: redeploy
the backend** (`cloudbuild.yaml`) and verify with `make run-be`.

---

## Module 8 — GraphRAG retrieval dedup ✅

**Branch:** `feat/agentic-graphrag-dedup` (off `main`)

**What it delivers** — a query-time fix in `GraphRAG.retrieve()`: a manifesto
promise is indexed both as a KG node (`promise:<doc_id>`) and a promise doc
(`<doc_id>`), so results showed it twice. Now retrieve over-fetches a pool,
groups the two representations by `doc_id`, keeps the **promise doc** as the
representative (it carries the page citation), and **merges the KG node's SDG
neighbours** onto it. Non-promise nodes are untouched.

- No index rebuild needed (query-time only); the published index stays valid.
- Only the **backend** needs redeploy (vendored `graphrag.py` re-synced; guard
  test keeps the two copies identical). The agentic-jobs image doesn't use
  `retrieve()`, so it needs no rebuild.

**Validation evidence**
- `pytest tests/` → **85 passed** (+ dedup collapse test, + distinct-nodes test).
- Demo: result that was a duplicate KG-node copy is now a distinct promise doc
  with a page citation and inherited SDG neighbours.

**Files**
```
agentic/graphrag.py            (retrieve dedup + _dedup_key/_neighbors_for)
web/backend_api/graphrag.py    (re-synced vendored copy)
tests/test_graphrag.py         (dedup tests)
```

**To go live:** redeploy the backend (build + `gcloud run deploy`).

---

## Module 9 — Critic / Verifier (data-quality gate) ✅

**Branch:** `feat/agentic-verifier` (off `main`)

**What it delivers** — `agentic/quality.py`: report-only checks on data *values*
(M1 only checks structure). Four check types, config-driven and pluggable:
- **RangeCheck** — numeric bounds per (collection, field): percentages 0–100,
  non-negative assets/counts, debt-to-GSDP 0–100, plausible `target_year`, etc.
- **OutlierCheck** — IQR fences (with a σ fallback for degenerate spreads) flag
  statistical outliers as *info*.
- **ConsistencyCheck** — cross-field rules: `net_assets == assets − liabilities`,
  `is_crorepati == (assets ≥ 1 Cr)`, `criminal_severity` matches case count,
  socio `percent` values in 0–100.
- **ManifestoHallucinationCheck** — the class of bug that once produced
  "200 TASMAC outlets" for an anti-liquor party: empty/too-short promise text,
  or an amount stated while the cost note says "data unavailable".

`Verifier` aggregates into a `QualityReport` (info / warning / error). It never
modifies or blocks data — findings go to a `quality_findings` collection via the
job, for human review. Errors mark the run `partial` (visible in `agentic recent`).

- Job: `agentic/jobs/verify_job.py` (testable core + Firestore wiring).
- CLI: `python -m agentic verify <collection> <path.json>`.

**Validation evidence**
- `pytest tests/` → **98 passed** (+13 quality tests).
- On real data: 0 false errors; `candidate_accountability` surfaces 23 *info*
  outliers (genuinely high-asset MLAs); `party_accountability` / `debt_history`
  clean.

**Files**
```
agentic/quality.py
agentic/jobs/verify_job.py
agentic/{__init__,__main__}.py   (exports + verify command)
tests/test_quality.py
```

**How to verify locally**
```bash
python -m pytest tests/ -q
python -m agentic verify candidate_accountability data/processed/mla_winners.json
```

**Deploy (optional, same image):** the `agentic-jobs` image already contains it
once rebuilt — create a `data-verify` Cloud Run Job
(`--args agentic.jobs.verify_job`) + a daily Scheduler trigger (see
`agentic/jobs/README.md`).

---

## Module 10 — Refresh (composite) tools ✅

**Branch:** `feat/agentic-refresh-tools` (off `main`)

**What it delivers** — `agentic/refresh.py`: four **argument-free, end-to-end**
pipeline tools (the granularity a Source Watcher trigger / the future
Orchestrator actually wants, vs. the fine-grained transform/load steps):
`refresh.finance`, `refresh.socio`, `refresh.accountability`,
`refresh.political_history`. Each wraps the existing `main.py` orchestration —
no logic duplicated — with lazy imports (importing the module pulls in nothing
heavy) and injectable run hooks (so the wrapper logic is unit-tested without
scraping / Firestore).

- Registered in the catalogue as a new `refresh` category with
  `writes` + `side_effects=[network, firestore]` metadata.
- The `prs_tn_budget` source is now wired `on_change_tool="refresh.finance"` —
  **inert in suggest mode**, ready for when you enable `--act`.
- CLI: `python -m agentic refresh <domain>`.

**Validation evidence**
- `pytest tests/` → **104 passed** (+6 refresh tests): wrapper call/order,
  catalogue registration, lazy resolution, source wiring.

**Files**
```
agentic/refresh.py
agentic/catalog.py            (4 refresh tools)
agentic/sources_config.py     (prs_tn_budget on_change_tool)
agentic/__main__.py           (refresh command)
tests/test_refresh.py
```

**⚠️ Execution-environment note** — because refresh tools run the scrapers, they
must execute in an image containing `scrapers/` + `main.py` + the full
`requirements.txt` (Playwright, Gemini, pdfplumber, …). The current
`agentic-jobs` image does **not** include those. This doesn't matter yet
(triggering is deferred with `--act`), but before the watcher/orchestrator can
actually run a refresh you'll either extend the `agentic-jobs` image to include
`scrapers/` + `main.py` + root requirements, or run refreshes from the `kg-jobs`
image (which already has them).

---

## ✅ All ten modules complete

Phase 0-2 of the agentic roadmap (`architecture.md` Part II) are built (M1-M6)
and wired to run on a schedule (M7), each on its own branch, each validated
end-to-end. **82 tests** across the suite. The
deterministic data plane is now agent-ready: typed contracts (M1), provenance +
reversible writes (M2), a uniform tool surface (M3), change detection (M4), a
feedback learning loop (M5), and GraphRAG retrieval/Q&A (M6) — all composing,
all defaulting to safe "suggest" behaviour with opt-in `act`.

**Remaining (future phases, not in this batch):** scheduling the watcher/triage
as Cloud Run Jobs, wiring `/api/ask` into the frontend, the Self-Healing and
Bridge-Rule-Learner agents, and promoting proven workflows from suggest → auto.
