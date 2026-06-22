"""Schemas for political / electoral Firestore collections.

Collections covered:
    assembly_elections        (election_transformer.transform_ceo_records)
    alliances                 (election_transformer.ALLIANCE_DATA)
    candidate_accountability  (accountability_transformer.enrich_winner)
    party_accountability      (accountability_transformer.build_party_rollups)
    manifesto_promises        (manifesto_ocr_gemini + manifesto_enrich_gemini)
    politician_profile        (politician_profile_migrate — single source of truth)
"""

from __future__ import annotations

from typing import Any, List, Optional

from pydantic import Field

from ._base import (
    Confidence,
    CriminalSeverity,
    EducationTier,
    FirestoreDoc,
)

# ---------------------------------------------------------------------------
# assembly_elections
# ---------------------------------------------------------------------------


class PartyResult(FirestoreDoc):
    party_id: str
    party_name_raw: Optional[str] = None
    seats_contested: Optional[int] = None
    seats_won: Optional[int] = None
    votes: Optional[int] = None
    vote_share_pct: Optional[float] = None


class ElectionDoc(FirestoreDoc):
    """One document per election year in ``assembly_elections`` (id: year_str)."""

    year: int
    year_str: Optional[str] = None  # added by the loader
    total_seats: Optional[int] = None
    majority_mark: Optional[int] = None
    party_results: List[PartyResult] = Field(default_factory=list)
    winning_party: Optional[str] = None
    source_url: Optional[str] = None
    pdf_checksum: Optional[str] = None
    ground_truth_confidence: Optional[Confidence] = None


# ---------------------------------------------------------------------------
# alliances
# ---------------------------------------------------------------------------


class AllianceDoc(FirestoreDoc):
    """One row per alliance per year (id: ``{year}_{anchor_party}_alliance``)."""

    doc_id: str
    year: int
    alliance_name: str
    anchor_party: str
    member_parties: List[str] = Field(default_factory=list)
    national_front_alignment: Optional[str] = None
    outcome: Optional[str] = None
    source_url: Optional[str] = None
    ground_truth_confidence: Optional[Confidence] = None


# ---------------------------------------------------------------------------
# candidate_accountability
# ---------------------------------------------------------------------------


class CandidateAccountabilityDoc(FirestoreDoc):
    """Enriched MyNeta winner record (id: ``{year}_{constituency_slug}``)."""

    doc_id: str
    constituency: str
    party: str
    mla_name: Optional[str] = None
    election_year: Optional[int] = None
    criminal_cases_total: Optional[int] = None
    education: Optional[str] = None
    education_tier: Optional[EducationTier] = None
    criminal_severity: Optional[CriminalSeverity] = None
    assets_cr: Optional[float] = None
    liabilities_cr: Optional[float] = None
    net_assets_cr: Optional[float] = None
    is_crorepati: Optional[bool] = None
    source_url: Optional[str] = None
    ground_truth_confidence: Optional[Confidence] = None


class PartyAccountabilityDoc(FirestoreDoc):
    """Party-level rollup (id: ``{year}_party_{party_slug}``)."""

    doc_id: str
    party: str
    election_year: int
    mla_count: int
    criminal_cases_pct: Optional[float] = None
    serious_cases_pct: Optional[float] = None
    crorepati_pct: Optional[float] = None
    avg_assets_cr: Optional[float] = None
    graduate_pct: Optional[float] = None
    source_url: Optional[str] = None
    ground_truth_confidence: Optional[Confidence] = None


# ---------------------------------------------------------------------------
# manifesto_promises
# ---------------------------------------------------------------------------


class PromiseComponent(FirestoreDoc):
    component: Optional[str] = None
    analysis: Optional[str] = None


class ManifestoPromiseDoc(FirestoreDoc):
    """Atomic manifesto promise (id: ``{party}_{year}_{category}_{NNN}``).

    Core OCR fields are required; the deep-analysis enrichment fields are all
    optional because a freshly-OCR'd promise exists before enrichment runs.
    """

    doc_id: str
    party_id: str
    category: str
    promise_text_en: str
    promise_text_ta: str
    target_year: int

    # OCR-stage optional fields
    party_name: Optional[str] = None
    party_color: Optional[str] = None
    status: Optional[str] = None
    stance_vibe: Optional[str] = None  # free-form, not a fixed enum
    amount_mentioned: Optional[str] = None
    scheme_name: Optional[str] = None
    manifesto_pdf_url: Optional[str] = None
    manifesto_pdf_page: Optional[int] = None
    is_aspirational: Optional[bool] = None
    track_fulfillment: Optional[bool] = None
    ground_truth_confidence: Optional[Confidence] = None

    # Enrichment-stage optional fields (manifesto_enrich_gemini)
    impact_mechanism: Optional[str] = None
    promise_components: Optional[List[PromiseComponent]] = None
    fiscal_cost_note: Optional[str] = None
    implementation_risk: Optional[str] = None        # low | medium | high
    root_cause_addressed: Optional[bool] = None
    sustainability_verdict: Optional[str] = None      # structural | symptomatic | optics
    sustainability_reasoning: Optional[str] = None
    impact_depth: Optional[str] = None                # transformative | substantive | supplemental | symbolic
    beneficiary_coverage: Optional[str] = None        # universal | broad_majority | targeted_poor
    fiscal_viability: Optional[str] = None
    coverage_gap_note: Optional[str] = None


# ---------------------------------------------------------------------------
# politician_profile  (single source of truth)
# ---------------------------------------------------------------------------


class ProfileTimelineEntry(FirestoreDoc):
    year: Optional[int] = None
    constituency_slug: Optional[str] = None
    party: Optional[str] = None
    won: Optional[bool] = None


class PoliticianProfileDoc(FirestoreDoc):
    """Person-level identity record. Modelled leniently — the timeline is the
    important invariant; biography fields vary by source coverage."""

    canonical_name: str
    aliases: Optional[List[str]] = None
    photo_url: Optional[str] = None
    gender: Optional[str] = None
    age: Optional[int] = None
    education: Optional[str] = None
    timeline: Optional[List[ProfileTimelineEntry]] = None
    win_count: Optional[int] = None
    loss_count: Optional[int] = None
    total_contested: Optional[int] = None
    ground_truth_confidence: Optional[Confidence] = None
    # tolerate id field under any of these names
    profile_id: Optional[str] = None
    doc_id: Optional[str] = None
    extra_meta: Optional[dict[str, Any]] = None
