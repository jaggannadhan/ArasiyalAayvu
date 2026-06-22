"""Declarative catalogue of watched sources.

Mirrors the real refresh jobs in ``scrapers/jobs/`` plus a few high-value web
sources. Add a source by appending a ``SourceSpec`` — no code changes needed.

``known`` lists for ``file_present`` are kept in sync with the corresponding
ingestor (e.g. ``niti_sdg_ingest.py`` YEAR_FILES) so only genuinely new drops
trigger an alert.
"""

from __future__ import annotations

from typing import List

from .sources import SourceSpec

SOURCES: List[SourceSpec] = [
    # Generalises scrapers/jobs/sdg_check.py
    SourceSpec(
        name="niti_sdg_csv",
        detector="file_present",
        params={
            "dir": "data/raw/niti_sdg",
            "glob": "sdg_*.csv",
            "known": ["sdg_2018.csv", "sdg_2020_21.csv", "sdg_2023_24.csv"],
        },
        description="New NITI SDG India Index CSV dropped for ingestion.",
        tags=["sdg", "annual"],
    ),
    # NITI SDG SPA — header change can hint a new edition is live
    SourceSpec(
        name="niti_sdg_web",
        detector="http_header",
        params={"url": "https://sdgindiaindex.niti.gov.in"},
        description="NITI SDG index site resource changed (possible new edition).",
        tags=["sdg"],
    ),
    # PRS budget analysis listing for Tamil Nadu (finance refresh trigger)
    SourceSpec(
        name="prs_tn_budget",
        detector="http_hash",
        params={"url": "https://prsindia.org/budgets/states/tamil-nadu"},
        description="PRS Tamil Nadu budget-analysis page changed (new fiscal year?).",
        tags=["finance"],
    ),
    # Fuel prices — generalises scrapers/jobs/fuel_refresh.py
    SourceSpec(
        name="fuel_prices_tn",
        detector="http_hash",
        params={"url": "https://www.goodreturns.in/petrol-price-in-chennai.html"},
        description="Chennai fuel price page changed.",
        tags=["cost_of_living", "monthly"],
    ),
]


def get_sources() -> List[SourceSpec]:
    return list(SOURCES)


def get_source(name: str) -> SourceSpec:
    for s in SOURCES:
        if s.name == name:
            return s
    raise KeyError(name)
