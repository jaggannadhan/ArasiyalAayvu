"""Validation engine for ArasiyalAayvu documents.

Public API
----------
    validate_doc(collection, doc)        -> DocResult
    validate_docs(collection, docs)      -> BatchReport
    validate_json_file(collection, path) -> BatchReport

Errors vs. warnings
-------------------
* **Errors**  — pydantic ValidationError (missing required field, wrong type,
  value outside a *hard* ``Literal`` such as ``ground_truth_confidence``).
  These mean the document violates the contract the app relies on.
* **Warnings** — a categorical field carries a value outside the observed
  ``ENUM_DOMAINS`` set. Likely benign upstream drift; surfaced for a human.

CLI
---
    python -m schemas <collection> <path-to-json> [--strict]
    python -m schemas --list

The JSON file may contain a single object, a list of objects, or a dict keyed
by id (as ``elections.json`` / ``alliances.json`` are) — all are handled.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from pydantic import ValidationError

from .registry import ENUM_DOMAINS, get_id_field, get_model, list_collections


@dataclass
class Issue:
    kind: str          # "error" | "warning"
    loc: str           # field path, e.g. "implementation_risk"
    message: str


@dataclass
class DocResult:
    collection: str
    doc_id: Optional[str]
    errors: List[Issue] = field(default_factory=list)
    warnings: List[Issue] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


@dataclass
class BatchReport:
    collection: str
    total: int = 0
    valid: int = 0
    results: List[DocResult] = field(default_factory=list)

    @property
    def errored(self) -> List[DocResult]:
        return [r for r in self.results if r.errors]

    @property
    def warned(self) -> List[DocResult]:
        return [r for r in self.results if r.warnings]

    @property
    def ok(self) -> bool:
        return not self.errored

    def summary(self) -> str:
        n_err_docs = len(self.errored)
        n_warn_docs = len(self.warned)
        n_err = sum(len(r.errors) for r in self.results)
        n_warn = sum(len(r.warnings) for r in self.results)
        return (
            f"[{self.collection}] {self.valid}/{self.total} valid · "
            f"{n_err_docs} docs with errors ({n_err}) · "
            f"{n_warn_docs} docs with warnings ({n_warn})"
        )


class UnknownCollection(ValueError):
    """Raised when a collection has no registered schema."""


def _enum_warnings(collection: str, doc: Dict[str, Any]) -> List[Issue]:
    out: List[Issue] = []
    for (coll, field_name), allowed in ENUM_DOMAINS.items():
        if coll != collection:
            continue
        val = doc.get(field_name)
        if val is None:
            continue
        if str(val) not in allowed:
            out.append(
                Issue(
                    kind="warning",
                    loc=field_name,
                    message=(
                        f"value {val!r} not in known domain "
                        f"{sorted(allowed)} — possible upstream drift"
                    ),
                )
            )
    return out


def validate_doc(collection: str, doc: Dict[str, Any]) -> DocResult:
    """Validate a single document against its collection schema."""
    model = get_model(collection)
    if model is None:
        raise UnknownCollection(
            f"No schema registered for collection {collection!r}. "
            f"Known: {list_collections()}"
        )

    id_field = get_id_field(collection)
    doc_id = str(doc.get(id_field)) if id_field and doc.get(id_field) is not None else None
    result = DocResult(collection=collection, doc_id=doc_id)

    try:
        model.model_validate(doc)
    except ValidationError as exc:
        for err in exc.errors():
            loc = ".".join(str(p) for p in err.get("loc", ()))
            result.errors.append(Issue("error", loc, err.get("msg", "invalid")))

    result.warnings.extend(_enum_warnings(collection, doc))
    return result


def validate_docs(collection: str, docs: Iterable[Dict[str, Any]]) -> BatchReport:
    report = BatchReport(collection=collection)
    for doc in docs:
        res = validate_doc(collection, doc)
        report.total += 1
        if res.ok:
            report.valid += 1
        report.results.append(res)
    return report


def _coerce_to_doc_list(data: Any) -> List[Dict[str, Any]]:
    """Accept a single object, a list, or an id-keyed dict of objects."""
    if isinstance(data, list):
        return [d for d in data if isinstance(d, dict)]
    if isinstance(data, dict):
        # dict of objects (e.g. elections.json keyed by year) vs. a single doc
        values = list(data.values())
        if values and all(isinstance(v, dict) for v in values):
            return values  # type: ignore[return-value]
        return [data]
    return []


def validate_json_file(collection: str, path: str | Path) -> BatchReport:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return validate_docs(collection, _coerce_to_doc_list(data))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _print_report(report: BatchReport, *, verbose: bool = True) -> None:
    print(report.summary())
    if not verbose:
        return
    for r in report.errored[:50]:
        for e in r.errors:
            print(f"  ERROR  [{r.doc_id}] {e.loc}: {e.message}")
    for r in report.warned[:50]:
        for w in r.warnings:
            print(f"  WARN   [{r.doc_id}] {w.loc}: {w.message}")


def main(argv: Optional[List[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] in {"-h", "--help"}:
        print(__doc__)
        return 0
    if argv[0] == "--list":
        print("Registered collections:")
        for c in list_collections():
            print(f"  {c}")
        return 0

    strict = "--strict" in argv
    argv = [a for a in argv if a != "--strict"]
    if len(argv) < 2:
        print("usage: python -m schemas.validate <collection> <path.json> [--strict]")
        return 2

    collection, path = argv[0], argv[1]
    try:
        report = validate_json_file(collection, path)
    except UnknownCollection as exc:
        print(f"error: {exc}")
        return 2

    _print_report(report)
    return 1 if (strict and not report.ok) else 0


if __name__ == "__main__":
    raise SystemExit(main())
