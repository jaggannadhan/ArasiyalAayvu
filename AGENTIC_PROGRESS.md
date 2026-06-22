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
| 2 | Provenance + `runs` collection | `feat/agentic-provenance` | ⬜ Not started | Firestore emulator |
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

## Next up — Module 2 (Provenance + `runs` collection)

**Goal:** every ingestion/transform/load run writes a structured outcome record
(`runs` collection) and stamps provenance on the docs it produces — the audit log
and the training signal the agents (Modules 4–6) learn from.

**Planned scope**
- A thin run-context wrapper (`agentic/provenance.py`) that records
  `{tool, args, started_at, status, rows_written, errors, triggered_by}`.
- Extend the loader to stamp `_provenance` (source run id) alongside the existing
  `_uploaded_at` / `_schema_version`.
- Snapshot/rollback token hook for reversible agentic writes.
- Validate against the **Firestore emulator**.
