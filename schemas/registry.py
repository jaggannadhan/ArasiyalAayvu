"""Central registry mapping Firestore collections to their schema models.

This is the lookup the validation layer (and, later, the Tool Registry in
Module 3) uses to find the right model for a collection.

Three things are registered per collection:

* ``COLLECTION_MODELS`` — collection name -> pydantic model (hard contract).
* ``ID_FIELDS``         — collection name -> the field used as the Firestore
  document id (mirrors ``loaders/firestore_loader``).
* ``ENUM_DOMAINS``      — (collection, field) -> set of values we have observed.
  Values outside the set are reported as *warnings*, not errors: they usually
  signal benign upstream vocabulary drift worth a human glance.
"""

from __future__ import annotations

from typing import Dict, Optional, Set, Tuple, Type

from ._base import FirestoreDoc
from .civic import FeedbackDoc
from .finance import DebtHistoryDoc, DepartmentalSpendingDoc, StateFinanceDoc
from .political import (
    AllianceDoc,
    CandidateAccountabilityDoc,
    ElectionDoc,
    ManifestoPromiseDoc,
    PartyAccountabilityDoc,
    PoliticianProfileDoc,
)
from .socio import SocioEconomicDoc

# Collection name -> model -------------------------------------------------
COLLECTION_MODELS: Dict[str, Type[FirestoreDoc]] = {
    "assembly_elections": ElectionDoc,
    "alliances": AllianceDoc,
    "candidate_accountability": CandidateAccountabilityDoc,
    "party_accountability": PartyAccountabilityDoc,
    "manifesto_promises": ManifestoPromiseDoc,
    "politician_profile": PoliticianProfileDoc,
    "state_finances": StateFinanceDoc,
    "debt_history": DebtHistoryDoc,
    "departmental_spending": DepartmentalSpendingDoc,
    "socio_economics": SocioEconomicDoc,
    "feedback": FeedbackDoc,
}

# Collection name -> document id field (mirrors firestore_loader) ----------
ID_FIELDS: Dict[str, str] = {
    "assembly_elections": "year_str",
    "alliances": "doc_id",
    "candidate_accountability": "doc_id",
    "party_accountability": "doc_id",
    "manifesto_promises": "doc_id",
    "politician_profile": "profile_id",
    "state_finances": "fiscal_year",
    "debt_history": "fiscal_year",
    "departmental_spending": "doc_id",
    "socio_economics": "metric_id",
    "feedback": None,  # Firestore auto-id
}

# (collection, field) -> observed value domain (soft warning if outside) ---
ENUM_DOMAINS: Dict[Tuple[str, str], Set[str]] = {
    ("manifesto_promises", "implementation_risk"): {"low", "medium", "high"},
    ("manifesto_promises", "sustainability_verdict"): {
        "structural",
        "symptomatic",
        "optics",
    },
    ("manifesto_promises", "impact_depth"): {
        "transformative",
        "substantive",
        "supplemental",
        "symbolic",
    },
    ("manifesto_promises", "beneficiary_coverage"): {
        "universal",
        "broad_majority",
        "targeted_poor",
        "specific_group",
    },
    ("manifesto_promises", "fiscal_viability"): {
        "feasible",
        "stressed",
        "uncosted",
        "central_dependent",
    },
}


def get_model(collection: str) -> Optional[Type[FirestoreDoc]]:
    """Return the schema model for a collection, or ``None`` if unregistered."""
    return COLLECTION_MODELS.get(collection)


def get_id_field(collection: str) -> Optional[str]:
    return ID_FIELDS.get(collection)


def list_collections() -> list[str]:
    return sorted(COLLECTION_MODELS)
