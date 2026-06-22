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
| 4 | Source Watcher (generalize `sdg_check`) | `feat/agentic-source-watcher` (off main) | ✅ **Done (awaiting push)** | 53 tests total; 4 detectors, real-data poll |
| 5 | Feedback Triage worker | `feat/agentic-feedback-triage` | ⬜ Not started | emulator |
| 6 | GraphRAG vector index + cited Q&A | `feat/agentic-graphrag` | ⬜ Not started | unit + sample queries |

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

## Next up — Module 5 (Feedback Triage)

**Goal:** finally consume the `feedback` collection — classify each item
(correction / missing-data / bug), de-dupe, and route to a review queue or a
Module-3 tool, recording each triage decision as a Module-2 run. The first step
of the "learn from feedback" loop.
