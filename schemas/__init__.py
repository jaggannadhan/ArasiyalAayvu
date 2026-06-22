"""ArasiyalAayvu document schemas (Phase 0 — agent-ready data plane).

Typed Pydantic contracts for the core Firestore collections, plus a validation
engine that distinguishes hard contract violations (errors) from benign
vocabulary drift (warnings).

Quick start::

    from schemas import validate_docs
    report = validate_docs("manifesto_promises", list_of_promise_dicts)
    print(report.summary())
    if not report.ok:
        for r in report.errored:
            ...

CLI::

    python -m schemas.validate manifesto_promises data/processed/manifesto_promises_2026_dmk.json
    python -m schemas.validate --list
"""

from ._base import Confidence, CriminalSeverity, EducationTier, FirestoreDoc
from .civic import FeedbackDoc
from .finance import DebtHistoryDoc, DepartmentalSpendingDoc, StateFinanceDoc
from .graph import KGEdge, KGNode, KnowledgeGraphDoc
from .political import (
    AllianceDoc,
    CandidateAccountabilityDoc,
    ElectionDoc,
    ManifestoPromiseDoc,
    PartyAccountabilityDoc,
    PoliticianProfileDoc,
)
from .registry import (
    COLLECTION_MODELS,
    ENUM_DOMAINS,
    ID_FIELDS,
    get_id_field,
    get_model,
    list_collections,
)
from .socio import SocioEconomicDoc
from .validate import (
    BatchReport,
    DocResult,
    Issue,
    UnknownCollection,
    validate_doc,
    validate_docs,
    validate_json_file,
)

__all__ = [
    # base
    "FirestoreDoc",
    "Confidence",
    "CriminalSeverity",
    "EducationTier",
    # models
    "ElectionDoc",
    "AllianceDoc",
    "CandidateAccountabilityDoc",
    "PartyAccountabilityDoc",
    "ManifestoPromiseDoc",
    "PoliticianProfileDoc",
    "StateFinanceDoc",
    "DebtHistoryDoc",
    "DepartmentalSpendingDoc",
    "SocioEconomicDoc",
    "FeedbackDoc",
    "KGNode",
    "KGEdge",
    "KnowledgeGraphDoc",
    # registry
    "COLLECTION_MODELS",
    "ID_FIELDS",
    "ENUM_DOMAINS",
    "get_model",
    "get_id_field",
    "list_collections",
    # validation
    "validate_doc",
    "validate_docs",
    "validate_json_file",
    "DocResult",
    "BatchReport",
    "Issue",
    "UnknownCollection",
]
