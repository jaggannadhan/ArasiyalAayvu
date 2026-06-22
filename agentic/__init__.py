"""ArasiyalAayvu agentic control plane.

A thin layer that *observes, decides, and acts* over the deterministic data
plane (scrapers / transformers / loaders). It is built up module by module per
``architecture.md`` Part II and tracked in ``AGENTIC_PROGRESS.md``.

Module 2 (this commit) — provenance:
    Every pipeline run is wrapped in a :class:`RunContext`, which writes a
    structured record to the ``runs`` collection and lets the loader stamp the
    originating run id onto every document it produces. This is the audit log
    and the training signal later agents learn from.
"""

from .provenance import (
    InMemoryRunStore,
    RunContext,
    RunStore,
    current_run,
    current_run_id,
    get_default_store,
    set_default_store,
    stamp_provenance,
)
from .rollback import SnapshotStore
from .tools import (
    ToolRegistry,
    ToolResult,
    ToolSpec,
    UnknownTool,
    get_registry,
)

__all__ = [
    "RunContext",
    "RunStore",
    "InMemoryRunStore",
    "current_run",
    "current_run_id",
    "stamp_provenance",
    "get_default_store",
    "set_default_store",
    "SnapshotStore",
    "ToolRegistry",
    "ToolSpec",
    "ToolResult",
    "UnknownTool",
    "get_registry",
]
