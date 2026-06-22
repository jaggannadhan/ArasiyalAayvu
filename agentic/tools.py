"""Tool Registry — a uniform, discoverable interface over the pipeline.

Each scraper / transformer / loader is registered as a :class:`ToolSpec` with a
dotted import path (so the registry imports nothing heavy until a tool is
actually invoked) plus metadata: what it reads, what it writes, and what side
effects it has (network / LLM / Firestore).

Invoking a tool via :meth:`ToolRegistry.invoke`:
  * wraps the call in a Module-2 ``RunContext`` (automatic provenance + the
    ``runs`` record),
  * passes the args as keyword arguments to the underlying function,
  * optionally validates the returned documents against the Module-1 schema of
    the tool's ``output_collection``,
  * returns a structured :class:`ToolResult` instead of raising, so a planner
    can inspect the outcome and decide what to do next.

This is the surface the autonomous agents (Modules 4-6) plan over.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .provenance import RunContext, RunStore


def _summarize_args(args: Dict[str, Any]) -> Dict[str, Any]:
    """Compact, serialisable summary of call args for the run record.

    Avoids bloating ``runs`` documents with full payloads (lists of thousands of
    docs) — stores shapes instead.
    """
    out: Dict[str, Any] = {}
    for k, v in (args or {}).items():
        if isinstance(v, list):
            out[k] = f"list[{len(v)}]"
        elif isinstance(v, dict):
            out[k] = f"dict[{len(v)}]"
        elif isinstance(v, str) and len(v) > 80:
            out[k] = v[:77] + "..."
        else:
            out[k] = v
    return out


def _as_doc_list(output: Any) -> Optional[List[Dict[str, Any]]]:
    """Coerce a tool's return value to a list of docs for validation, or None."""
    if isinstance(output, list):
        return [d for d in output if isinstance(d, dict)]
    if isinstance(output, dict):
        vals = list(output.values())
        if vals and all(isinstance(v, dict) for v in vals):
            return vals  # type: ignore[return-value]
    return None


@dataclass
class ToolSpec:
    name: str
    summary: str
    category: str  # transform | load | scrape | pipeline | util
    path: Optional[str] = None          # "module:attr"
    func: Optional[Callable] = None     # direct callable (tests / in-process)
    output_collection: Optional[str] = None
    writes: List[str] = field(default_factory=list)
    reads: List[str] = field(default_factory=list)
    side_effects: List[str] = field(default_factory=list)  # network | llm | firestore
    tags: List[str] = field(default_factory=list)
    default_trigger: str = "agent"
    _resolved: Optional[Callable] = field(default=None, repr=False)

    def resolve(self) -> Callable:
        """Import and cache the underlying callable."""
        if self.func is not None:
            return self.func
        if self._resolved is not None:
            return self._resolved
        if not self.path or ":" not in self.path:
            raise ValueError(f"Tool {self.name!r} has no valid import path")
        module_name, attr = self.path.split(":", 1)
        module = importlib.import_module(module_name)
        self._resolved = getattr(module, attr)
        return self._resolved

    def describe(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "category": self.category,
            "summary": self.summary,
            "writes": self.writes,
            "reads": self.reads,
            "output_collection": self.output_collection,
            "side_effects": self.side_effects,
            "tags": self.tags,
        }


@dataclass
class ToolResult:
    tool: str
    status: str                      # success | partial | failed
    run_id: Optional[str] = None
    output: Any = None
    output_validation: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    rows_written: int = 0

    @property
    def ok(self) -> bool:
        return self.status != "failed"


class UnknownTool(KeyError):
    pass


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: Dict[str, ToolSpec] = {}

    # -- registration --------------------------------------------------------

    def register(self, spec: ToolSpec) -> ToolSpec:
        if spec.name in self._tools:
            raise ValueError(f"Tool already registered: {spec.name!r}")
        self._tools[spec.name] = spec
        return spec

    def register_callable(self, name: str, func: Callable, **kwargs: Any) -> ToolSpec:
        kwargs.setdefault("summary", func.__doc__ or name)
        kwargs.setdefault("category", "util")
        return self.register(ToolSpec(name=name, func=func, **kwargs))

    # -- discovery -----------------------------------------------------------

    def get(self, name: str) -> ToolSpec:
        try:
            return self._tools[name]
        except KeyError:
            raise UnknownTool(name) from None

    def names(self) -> List[str]:
        return sorted(self._tools)

    def list(self, *, category: Optional[str] = None, tag: Optional[str] = None) -> List[ToolSpec]:
        out = []
        for spec in self._tools.values():
            if category and spec.category != category:
                continue
            if tag and tag not in spec.tags:
                continue
            out.append(spec)
        return sorted(out, key=lambda s: s.name)

    def describe_all(self) -> List[Dict[str, Any]]:
        return [s.describe() for s in self.list()]

    # -- invocation ----------------------------------------------------------

    def invoke(
        self,
        name: str,
        args: Optional[Dict[str, Any]] = None,
        *,
        trigger: Optional[str] = None,
        store: Optional[RunStore] = None,
        validate_output: bool = True,
    ) -> ToolResult:
        spec = self.get(name)
        fn = spec.resolve()
        args = args or {}
        trigger = trigger or spec.default_trigger

        run: Optional[RunContext] = None
        output: Any = None
        error: Optional[str] = None
        try:
            with RunContext(
                tool=name, trigger=trigger, args=_summarize_args(args), store=store
            ) as run:
                output = fn(**args)
        except Exception as exc:  # capture so the planner can react
            error = f"{type(exc).__name__}: {exc}"

        output_validation = None
        if validate_output and error is None and spec.output_collection:
            docs = _as_doc_list(output)
            if docs is not None:
                from schemas import UnknownCollection, validate_docs

                try:
                    rep = validate_docs(spec.output_collection, docs)
                    output_validation = {
                        "collection": spec.output_collection,
                        "summary": rep.summary(),
                        "ok": rep.ok,
                        "errors": [
                            f"[{r.doc_id}] {e.loc}: {e.message}"
                            for r in rep.errored[:10]
                            for e in r.errors
                        ],
                    }
                except UnknownCollection:
                    pass

        return ToolResult(
            tool=name,
            status=(run.status if run is not None else "failed"),
            run_id=(run.run_id if run is not None else None),
            output=output,
            output_validation=output_validation,
            error=error,
            rows_written=(run.rows_written if run is not None else 0),
        )


# Process-wide default registry, populated by agentic.catalog.
_registry: Optional[ToolRegistry] = None


def get_registry() -> ToolRegistry:
    global _registry
    if _registry is None:
        _registry = ToolRegistry()
        from . import catalog  # noqa: F401  (registers tools as a side effect)

        catalog.register_all(_registry)
    return _registry
