"""Schema for the ``runs`` provenance collection (Module 2).

Every ingestion / transform / load run writes one document here: the audit log
*and* the training signal the agentic control plane (Modules 4-6) learns from.
Written by :mod:`agentic.provenance`, not by the scrapers directly.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import Field

from ._base import FirestoreDoc

# Status is fully controlled by RunContext, so it is a hard contract.
RunStatus = Literal["running", "success", "failed", "partial"]


class CollectionWrite(FirestoreDoc):
    collection: str
    count: int = 0


class RunRecordDoc(FirestoreDoc):
    """One row per pipeline run (id: ``run_id``)."""

    run_id: str
    tool: str
    status: RunStatus = "running"
    trigger: Optional[str] = None        # cli | scheduler | agent | manual | test | backfill
    started_at: str
    finished_at: Optional[str] = None
    duration_s: Optional[float] = None
    args: Dict[str, Any] = Field(default_factory=dict)
    rows_written: int = 0
    writes: List[CollectionWrite] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    host: Optional[str] = None
    parent_run_id: Optional[str] = None  # set when one run spawns another
