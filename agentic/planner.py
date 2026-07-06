"""Plan-only Orchestrator planner (Module 11).

Turns a goal — expressed as the set of registry tools to run — into a validated
DAG of tasks, deriving dependency edges from the tools' ``reads``/``writes``
metadata (M3). It **plans only**: it produces and persists the plan for review /
approval and executes nothing. Execution (walking the DAG, running each task as a
Module-2 run, reacting to results) is a later module.

Design maps to the ER + state-machine sketch:
* ``Plan`` / ``PlanTask`` ← the two new collections
* ``depends_on`` (adjacency list) ← the DAG edges, derived from reads/writes
* risk + approval ← the suggest→act gate
* stages ← topological levels (parallelizable bands)
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Writing to these collections is high-risk (candidate criminal/asset claims,
# person identities, party manifestos) — such tasks always need approval.
HIGH_RISK_COLLECTIONS = {
    "candidate_accountability",
    "party_accountability",
    "politician_profile",
    "manifesto_promises",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class PlanError(ValueError):
    pass


class PlanCycleError(PlanError):
    """Raised when the derived dependency graph contains a cycle."""


class UnknownGoalTool(PlanError):
    pass


# ---------------------------------------------------------------------------
# In-memory plan representation
# ---------------------------------------------------------------------------


@dataclass
class PlanTask:
    task_id: str
    tool: str
    depends_on: List[str] = field(default_factory=list)
    args: Dict[str, Any] = field(default_factory=dict)
    reads: List[str] = field(default_factory=list)
    writes: List[str] = field(default_factory=list)
    risk: str = "low"
    state: str = "pending"
    stage: int = 0

    def to_doc(self, plan_id: str) -> Dict[str, Any]:
        return {
            "task_id": self.task_id, "plan_id": plan_id, "tool": self.tool,
            "args": self.args, "depends_on": self.depends_on, "state": self.state,
            "risk": self.risk, "stage": self.stage,
            "reads": self.reads, "writes": self.writes, "attempts": 0,
        }


@dataclass
class Plan:
    plan_id: str
    goal: str
    tasks: List[PlanTask]
    mode: str = "suggest"
    status: str = "awaiting_approval"
    trigger: Dict[str, Any] = field(default_factory=dict)
    policy: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_now_iso)
    requires_approval: bool = True

    def stages(self) -> List[List[PlanTask]]:
        by_stage: Dict[int, List[PlanTask]] = {}
        for t in self.tasks:
            by_stage.setdefault(t.stage, []).append(t)
        return [by_stage[k] for k in sorted(by_stage)]

    def to_doc(self) -> Dict[str, Any]:
        return {
            "plan_id": self.plan_id, "goal": self.goal, "mode": self.mode,
            "status": self.status, "trigger": self.trigger, "policy": self.policy,
            "created_at": self.created_at, "requires_approval": self.requires_approval,
            "task_ids": [t.task_id for t in self.tasks],
        }


# ---------------------------------------------------------------------------
# DAG utilities
# ---------------------------------------------------------------------------


def _toposort(tasks: List[PlanTask]) -> List[PlanTask]:
    """Kahn's algorithm. Raises PlanCycleError on a cycle. Also sets ``stage``."""
    by_id = {t.task_id: t for t in tasks}
    indeg = {t.task_id: 0 for t in tasks}
    children: Dict[str, List[str]] = {t.task_id: [] for t in tasks}
    for t in tasks:
        for dep in t.depends_on:
            if dep in by_id:
                indeg[t.task_id] += 1
                children[dep].append(t.task_id)

    # stage = longest dependency depth (parallelizable band)
    ready = [tid for tid, d in indeg.items() if d == 0]
    stage_of = {tid: 0 for tid in ready}
    order: List[PlanTask] = []
    queue = list(ready)
    while queue:
        tid = queue.pop(0)
        order.append(by_id[tid])
        for c in children[tid]:
            stage_of[c] = max(stage_of.get(c, 0), stage_of[tid] + 1)
            indeg[c] -= 1
            if indeg[c] == 0:
                queue.append(c)

    if len(order) != len(tasks):
        remaining = [t.task_id for t in tasks if t not in order]
        raise PlanCycleError(f"dependency cycle among: {sorted(remaining)}")

    for t in tasks:
        t.stage = stage_of.get(t.task_id, 0)
    return order


def _risk(writes: List[str], side_effects: List[str]) -> str:
    if set(writes) & HIGH_RISK_COLLECTIONS:
        return "high"
    if side_effects:
        return "medium"
    return "low"


# ---------------------------------------------------------------------------
# Plan stores
# ---------------------------------------------------------------------------


class PlanStore:
    def save_plan(self, plan: Plan) -> None:  # pragma: no cover - interface
        raise NotImplementedError


class InMemoryPlanStore(PlanStore):
    def __init__(self) -> None:
        self.plans: Dict[str, Dict[str, Any]] = {}
        self.tasks: Dict[str, Dict[str, Any]] = {}

    def save_plan(self, plan: Plan) -> None:
        self.plans[plan.plan_id] = plan.to_doc()
        for t in plan.tasks:
            self.tasks[t.task_id] = t.to_doc(plan.plan_id)


class FirestorePlanStore(PlanStore):
    def __init__(self, client: Any = None, project: Optional[str] = None) -> None:
        self._client = client
        self._project = project

    @property
    def client(self):
        if self._client is None:
            import os

            from google.cloud import firestore  # lazy

            self._client = firestore.Client(
                project=self._project or os.environ.get("GOOGLE_CLOUD_PROJECT", "naatunadappu")
            )
        return self._client

    def save_plan(self, plan: Plan) -> None:
        self.client.collection("plans").document(plan.plan_id).set(plan.to_doc(), merge=True)
        for t in plan.tasks:
            self.client.collection("plan_tasks").document(t.task_id).set(
                t.to_doc(plan.plan_id), merge=True
            )


# ---------------------------------------------------------------------------
# Planner
# ---------------------------------------------------------------------------


class Planner:
    def __init__(self, registry: Any = None, store: Optional[PlanStore] = None) -> None:
        self._registry = registry
        self.store = store

    @property
    def registry(self):
        if self._registry is None:
            from .tools import get_registry

            self._registry = get_registry()
        return self._registry

    def plan(
        self,
        goal: str,
        tools: List[str],
        *,
        mode: str = "suggest",
        trigger: Optional[Dict[str, Any]] = None,
        args: Optional[Dict[str, Dict[str, Any]]] = None,
        policy: Optional[Dict[str, Any]] = None,
    ) -> Plan:
        if not tools:
            raise PlanError("a plan needs at least one tool")

        from .tools import UnknownTool

        specs = {}
        for t in tools:
            try:
                specs[t] = self.registry.get(t)
            except UnknownTool as exc:
                raise UnknownGoalTool(str(exc)) from None

        plan_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
        args = args or {}
        tid = {t: f"{plan_id}:{t}" for t in tools}

        tasks: List[PlanTask] = []
        for t in tools:
            spec = specs[t]
            # depends_on: any selected tool that WRITES a collection this tool READS
            deps = [
                tid[other]
                for other in tools
                if other != t and (set(spec.reads) & set(specs[other].writes))
            ]
            tasks.append(PlanTask(
                task_id=tid[t], tool=t, depends_on=deps, args=args.get(t, {}),
                reads=list(spec.reads), writes=list(spec.writes),
                risk=_risk(spec.writes, spec.side_effects),
            ))

        _toposort(tasks)  # validates (no cycle) + assigns stages

        has_high = any(t.risk == "high" for t in tasks)
        requires_approval = (mode == "suggest") or has_high
        status = "awaiting_approval" if requires_approval else "planned"

        plan = Plan(
            plan_id=plan_id, goal=goal, tasks=tasks, mode=mode, status=status,
            trigger=trigger or {"type": "manual"}, policy=policy or {},
            requires_approval=requires_approval,
        )
        if self.store is not None:
            self.store.save_plan(plan)
        return plan
