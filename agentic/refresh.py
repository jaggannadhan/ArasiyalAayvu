"""Refresh tools — self-contained, end-to-end pipeline units.

The registry's other tools are fine-grained (a single transform or load). A
*refresh* tool runs a whole domain pipeline (scrape → transform → load) as one
named, argument-free unit — the granularity a Source Watcher trigger or the
future Orchestrator actually wants ("PRS budget changed → refresh.finance").

Each wraps the existing orchestration in ``main.py`` (no logic duplicated).
Imports are lazy, so importing this module pulls in nothing heavy; the scrapers
and their deps are only imported when a refresh actually runs.

Testability: every function takes injectable ``_run`` hooks, so the wrapper
logic is unit-tested without importing ``main`` / the scrapers / Firestore.

Runtime note: because these execute the scrapers, they must run in an image that
contains ``scrapers/`` + ``main.py`` + the full ``requirements.txt`` (the
``kg-jobs`` image, or an extended ``agentic-jobs`` image). See
``agentic/jobs/README.md``.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional


def _lazy(module: str, attr: str) -> Callable:
    import importlib

    return getattr(importlib.import_module(module), attr)


def refresh_finance(years: Optional[List[str]] = None, *, _run: Optional[Callable] = None) -> Dict[str, Any]:
    """Scrape PRS budget PDFs → transform → upload state finances."""
    (_run or _lazy("main", "run_finance"))(years)
    return {"refresh": "finance", "years": years}


def refresh_socio(*, _run: Optional[Callable] = None) -> Dict[str, Any]:
    """Scrape ASER → merge into curated socio_economics → upload."""
    (_run or _lazy("main", "run_socio"))()
    return {"refresh": "socio"}


def refresh_accountability(*, _run: Optional[Callable] = None) -> Dict[str, Any]:
    """Scrape MyNeta winners → transform → upload candidate/party accountability."""
    (_run or _lazy("main", "run_accountability"))()
    return {"refresh": "accountability"}


def refresh_political_history(
    *,
    _scrape: Optional[Callable] = None,
    _transform: Optional[Callable] = None,
    _upload: Optional[Callable] = None,
) -> Dict[str, Any]:
    """Full political-history pipeline: scrape → transform → upload."""
    (_scrape or _lazy("main", "run_scrape"))()
    (_transform or _lazy("main", "run_transform"))()
    (_upload or _lazy("main", "run_upload"))()
    return {"refresh": "political_history"}
