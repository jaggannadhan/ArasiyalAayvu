"""Schemas for the Orchestrator's ``plans`` and ``plan_tasks`` collections (M11).

These are the *orchestration* layer — the DAG, per-task state, retries, decisions
— layered over the existing ``runs``/``snapshots`` (M2) which hold execution
truth. See ``docs`` / AGENTIC_PROGRESS for the ER + state-machine sketch.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import Field

from ._base import FirestoreDoc

PlanMode = Literal["suggest", "act"]
PlanStatus = Literal[
    "pending", "planning", "awaiting_approval", "planned",
    "running", "succeeded", "partial", "failed", "aborted", "rolled_back",
]
TaskState = Literal[
    "pending", "ready", "running", "succeeded", "failed", "skipped",
    "retrying", "rolled_back",
]
TaskRisk = Literal["low", "medium", "high"]


class PlanDoc(FirestoreDoc):
    """One document per plan (id: ``plan_id``)."""

    plan_id: str
    goal: str
    mode: PlanMode = "suggest"
    status: PlanStatus = "awaiting_approval"
    trigger: Dict[str, Any] = Field(default_factory=dict)
    created_at: str
    policy: Dict[str, Any] = Field(default_factory=dict)
    task_ids: List[str] = Field(default_factory=list)
    run_id: Optional[str] = None            # parent RunContext once executed
    rollback_token: Optional[str] = None
    requires_approval: bool = True


class PlanTaskDoc(FirestoreDoc):
    """One document per DAG node (id: ``task_id``)."""

    task_id: str
    plan_id: str
    tool: str                               # registry tool name (M3)
    args: Dict[str, Any] = Field(default_factory=dict)
    depends_on: List[str] = Field(default_factory=list)   # task_ids — the DAG edges
    state: TaskState = "pending"
    risk: TaskRisk = "low"
    stage: int = 0                          # topological level (parallelizable band)
    reads: List[str] = Field(default_factory=list)
    writes: List[str] = Field(default_factory=list)
    attempts: int = 0
    run_id: Optional[str] = None            # execution run (M2) once run
    rollback_token: Optional[str] = None
    last_error: Optional[str] = None
