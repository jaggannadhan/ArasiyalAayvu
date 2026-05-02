import json
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import firestore

db = firestore.Client(project="naatunadappu")

BATCH_SIZE = 400


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _batch_upload(collection: str, documents: list[dict], id_field: str) -> None:
    total = len(documents)
    uploaded = 0

    for chunk_start in range(0, total, BATCH_SIZE):
        batch = db.batch()
        chunk = documents[chunk_start: chunk_start + BATCH_SIZE]

        for doc in chunk:
            doc_id = str(doc[id_field])
            doc["_uploaded_at"] = _now_iso()
            doc["_schema_version"] = "1.0"
            ref = db.collection(collection).document(doc_id)
            batch.set(ref, doc, merge=True)

        batch.commit()
        uploaded += len(chunk)
        print(f"  [{collection}] {uploaded}/{total} docs uploaded")


def upload_elections(elections: dict[int, dict]) -> None:
    docs = []
    for year, data in elections.items():
        doc = dict(data)
        doc["year_str"] = str(year)
        docs.append(doc)
    _batch_upload("assembly_elections", docs, id_field="year_str")


def upload_alliances(alliance_matrix: dict[int, list[dict]]) -> None:
    flat_docs = []
    for year, alliances in alliance_matrix.items():
        for i, alliance in enumerate(alliances):
            doc = dict(alliance)
            doc["doc_id"] = f"{year}_{alliance['anchor_party']}_alliance"
            doc["year"] = year
            doc.setdefault("source_url", "https://www.assembly.tn.gov.in (curated)")
            doc.setdefault("ground_truth_confidence", "HIGH")
            flat_docs.append(doc)
    _batch_upload("alliances", flat_docs, id_field="doc_id")


def upload_parties(parties: list[dict]) -> None:
    _batch_upload("political_parties", parties, id_field="party_id")


def upload_leaders(leaders: list[dict]) -> None:
    _batch_upload("leaders", leaders, id_field="leader_id")


def upload_chief_ministers(cms: list[dict]) -> None:
    for i, cm in enumerate(cms):
        cm["leader_id"] = f"cm_{str(i+1).zfill(2)}_{cm['name'].lower().replace(' ', '_').replace('.', '')}"
    _batch_upload("chief_ministers", cms, id_field="leader_id")


def upload_achievements(achievements: list[dict]) -> None:
    _batch_upload("achievements", achievements, id_field="scheme_id")


# ---------------------------------------------------------------------------
# Module 2 — State Finances
# ---------------------------------------------------------------------------

def upload_state_finances(docs: list[dict]) -> None:
    """Upload state_finances documents keyed by fiscal_year (e.g. '2025-26')."""
    _batch_upload("state_finances", docs, id_field="fiscal_year")


def upload_debt_history(docs: list[dict]) -> None:
    """Upload debt_history documents keyed by fiscal_year."""
    _batch_upload("debt_history", docs, id_field="fiscal_year")


def upload_departmental_spending(docs: list[dict]) -> None:
    """Upload departmental_spending documents keyed by '{year}_{dept_slug}'."""
    _batch_upload("departmental_spending", docs, id_field="doc_id")


def upload_finance_manual(doc: dict) -> None:
    """Single-document upsert — used by the manual-link PDF utility."""
    doc["_uploaded_at"] = datetime.now(timezone.utc).isoformat()
    doc["_schema_version"] = "1.0"
    year = doc.get("fiscal_year", "unknown")
    db.collection("state_finances").document(year).set(doc, merge=True)
    print(f"  [uploaded] state_finances/{year}")


# ---------------------------------------------------------------------------
# Module 3 — Citizen Awareness: Socio-Economics
# ---------------------------------------------------------------------------

def upload_socio_economics(docs: list[dict]) -> None:
    """Upload socio_economics documents keyed by metric_id."""
    _batch_upload("socio_economics", docs, id_field="metric_id")


# ---------------------------------------------------------------------------
# Module 4 — Citizen Awareness: Candidate Accountability
# ---------------------------------------------------------------------------

def upload_mla_winners(winners: list[dict]) -> None:
    """Upload individual MLA records to candidate_accountability collection."""
    _batch_upload("candidate_accountability", winners, id_field="doc_id")


def upload_party_rollups(rollups: list[dict]) -> None:
    """Upload party-level accountability rollups to party_accountability collection."""
    _batch_upload("party_accountability", rollups, id_field="doc_id")


def upload_assembly_summary(summary: dict) -> None:
    """Upload single assembly-level summary document."""
    summary["_uploaded_at"] = datetime.now(timezone.utc).isoformat()
    summary["_schema_version"] = "1.0"
    doc_id = summary.get("doc_id", "tn_assembly_2021_summary")
    db.collection("candidate_accountability").document(doc_id).set(summary, merge=True)
    print(f"  [uploaded] candidate_accountability/{doc_id}")


# ---------------------------------------------------------------------------
# Module 5 — Manifesto Tracker
# ---------------------------------------------------------------------------

def upload_manifesto_promises(promises: list[dict]) -> None:
    """Upload atomic manifesto promise documents keyed by doc_id."""
    _batch_upload("manifesto_promises", promises, id_field="doc_id")


# ---------------------------------------------------------------------------
# Module 6 — MLACDS Budget
# ---------------------------------------------------------------------------

def upload_mlacds_budget(docs: list[dict]) -> None:
    """Upload MLACDS budget documents keyed by fiscal_year (e.g. '2021-22')."""
    _batch_upload("mlacds_budget", docs, id_field="doc_id")
