"""Concrete tool catalogue.

Wraps existing scrapers / transformers / loaders as registry tools by dotted
import path. Nothing here imports those modules at definition time — paths are
resolved lazily on first invocation — so importing the catalogue is cheap and
free of heavy dependencies (Playwright, Gemini, google.cloud, ...).

Tools are intentionally fine-grained (transform vs. load) so the planner can
compose them and so each step gets its own provenance run record.
"""

from __future__ import annotations

from .tools import ToolRegistry, ToolSpec

# Each entry mirrors a function already in the codebase. ``output_collection`` is
# set only where the function returns docs in the *uploaded* shape, so output
# validation is meaningful (e.g. alliances/assembly-summary are pre-upload or
# heterogeneous, so they are left unset to avoid false errors).
_SPECS = [
    # ---- transformers (pure, no side effects) ----
    ToolSpec(
        name="transform.elections",
        summary="CEO-TN raw rows -> assembly_elections docs.",
        category="transform",
        path="transformers.election_transformer:transform_ceo_records",
        output_collection="assembly_elections",
        reads=["data/raw/ceo_tn"],
        writes=["assembly_elections"],
        tags=["election"],
    ),
    ToolSpec(
        name="transform.alliances",
        summary="Curated TN alliance matrix (1952-2021).",
        category="transform",
        path="transformers.election_transformer:build_alliance_matrix",
        writes=["alliances"],
        tags=["election"],
    ),
    ToolSpec(
        name="transform.state_finances",
        summary="PRS budget docs -> state_finances (debt_why + viz_metrics).",
        category="transform",
        path="transformers.finance_transformer:transform_prs_docs",
        output_collection="state_finances",
        writes=["state_finances"],
        tags=["finance"],
    ),
    ToolSpec(
        name="transform.debt_history",
        summary="Curated multi-year debt trend series.",
        category="transform",
        path="transformers.finance_transformer:build_debt_history_series",
        output_collection="debt_history",
        writes=["debt_history"],
        tags=["finance"],
    ),
    ToolSpec(
        name="transform.departmental_spending",
        summary="PRS sector data -> departmental_spending docs.",
        category="transform",
        path="transformers.finance_transformer:build_departmental_spending",
        output_collection="departmental_spending",
        writes=["departmental_spending"],
        tags=["finance"],
    ),
    ToolSpec(
        name="transform.party_accountability",
        summary="MyNeta winners -> per-party accountability rollups.",
        category="transform",
        path="transformers.accountability_transformer:build_party_rollups",
        output_collection="party_accountability",
        reads=["candidate_accountability"],
        writes=["party_accountability"],
        tags=["accountability"],
    ),
    ToolSpec(
        name="transform.socio_aser_extras",
        summary="ASER enrollment/dropout -> extra socio_economics docs.",
        category="transform",
        path="transformers.socio_transformer:add_aser_enrollment_metrics",
        output_collection="socio_economics",
        writes=["socio_economics"],
        tags=["socio"],
    ),
    # ---- loaders (write to Firestore) ----
    ToolSpec(
        name="load.mla_winners",
        summary="Upload enriched MLA winner records.",
        category="load",
        path="loaders.firestore_loader:upload_mla_winners",
        writes=["candidate_accountability"],
        side_effects=["firestore"],
        tags=["accountability"],
    ),
    ToolSpec(
        name="load.party_rollups",
        summary="Upload party accountability rollups.",
        category="load",
        path="loaders.firestore_loader:upload_party_rollups",
        writes=["party_accountability"],
        side_effects=["firestore"],
        tags=["accountability"],
    ),
    ToolSpec(
        name="load.manifesto_promises",
        summary="Upload manifesto promise docs.",
        category="load",
        path="loaders.firestore_loader:upload_manifesto_promises",
        writes=["manifesto_promises"],
        side_effects=["firestore"],
        tags=["manifesto"],
    ),
    ToolSpec(
        name="load.state_finances",
        summary="Upload state_finances docs.",
        category="load",
        path="loaders.firestore_loader:upload_state_finances",
        writes=["state_finances"],
        side_effects=["firestore"],
        tags=["finance"],
    ),
    ToolSpec(
        name="load.debt_history",
        summary="Upload debt_history docs.",
        category="load",
        path="loaders.firestore_loader:upload_debt_history",
        writes=["debt_history"],
        side_effects=["firestore"],
        tags=["finance"],
    ),
    ToolSpec(
        name="load.departmental_spending",
        summary="Upload departmental_spending docs.",
        category="load",
        path="loaders.firestore_loader:upload_departmental_spending",
        writes=["departmental_spending"],
        side_effects=["firestore"],
        tags=["finance"],
    ),
    ToolSpec(
        name="load.socio_economics",
        summary="Upload socio_economics docs.",
        category="load",
        path="loaders.firestore_loader:upload_socio_economics",
        writes=["socio_economics"],
        side_effects=["firestore"],
        tags=["socio"],
    ),
    ToolSpec(
        name="load.elections",
        summary="Upload assembly_elections docs.",
        category="load",
        path="loaders.firestore_loader:upload_elections",
        writes=["assembly_elections"],
        side_effects=["firestore"],
        tags=["election"],
    ),
    ToolSpec(
        name="load.alliances",
        summary="Upload alliance docs (flattens year-keyed matrix).",
        category="load",
        path="loaders.firestore_loader:upload_alliances",
        writes=["alliances"],
        side_effects=["firestore"],
        tags=["election"],
    ),
]


def register_all(registry: ToolRegistry) -> None:
    for spec in _SPECS:
        registry.register(spec)
