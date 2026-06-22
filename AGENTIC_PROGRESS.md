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
| 1 | Schemas — Pydantic contracts + validation engine | `feat/agentic-schemas` | ✅ **Done (awaiting push)** | 21 unit/integration tests; real data files |
| 2 | Provenance + `runs` collection | `feat/agentic-provenance` | ✅ **Done (awaiting push)** | 32 tests total; in-mem fake client + emulator recipe |
| 3 | Tool Registry | `feat/agentic-tool-registry` | ⬜ Not started | unit |
| 4 | Source Watcher (generalize `sdg_check`) | `feat/agentic-source-watcher` | ⬜ Not started | emulator + mocked HTTP |
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

## Next up — Module 3 (Tool Registry)

**Goal:** a single registry that wraps each scraper / transformer / loader as a
callable tool with a uniform `run(args)` interface and typed I/O (Module 1
schemas), so the planner/agents (Modules 4–6) can discover and invoke pipeline
steps — each invocation wrapped automatically in a Module 2 `RunContext`.
