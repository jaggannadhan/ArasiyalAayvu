"""
Shared building blocks for ArasiyalAayvu Firestore document schemas.

Design goals (Phase 0 — "make the data plane agent-ready"):

* **Catch drift, don't break production.** Every model permits unknown extra
  fields (``extra="allow"``) so a newly added field never fails validation.
  Only structural invariants — the document id field, the handful of fields the
  app genuinely depends on, and known categorical vocabularies — are enforced.
* **Hard errors vs. soft warnings.** Wrong types / missing required fields are
  *errors*. A categorical field carrying a value we have never seen before is a
  *warning* (likely upstream drift worth a human glance, not a reason to drop
  the row). See :mod:`schemas.validate`.
* **Match what is actually written.** Field names mirror the documents produced
  by the transformers / scrapers and uploaded via ``loaders/firestore_loader``.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

# ---------------------------------------------------------------------------
# Common typed vocabularies
# ---------------------------------------------------------------------------

# Confidence stamped by virtually every scraper/transformer.
Confidence = Literal["HIGH", "MEDIUM", "LOW"]

# Criminal-severity buckets — accountability_transformer.classify_criminal_severity
CriminalSeverity = Literal["CLEAN", "MINOR", "MODERATE", "SERIOUS"]

# Education tiers — accountability_transformer.enrich_winner
EducationTier = Literal[
    "Doctorate",
    "Post Graduate",
    "Graduate",
    "Class XII",
    "Class X",
    "Below Class X",
    "Not Disclosed",
]


class FirestoreDoc(BaseModel):
    """Base for every Firestore document model.

    ``extra="allow"`` is deliberate: the loader stamps ``_uploaded_at`` and
    ``_schema_version`` onto every doc, scrapers add source-specific fields, and
    we never want those to trip validation. We validate the *contract*, not the
    whole payload.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)
