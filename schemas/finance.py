"""Schemas for finance Firestore collections.

Collections covered:
    state_finances           (finance_transformer.transform_prs_docs)
    debt_history             (finance_transformer.build_debt_history_series)
    departmental_spending    (finance_transformer.build_departmental_spending)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import Field

from ._base import Confidence, FirestoreDoc


class DebtWhyItem(FirestoreDoc):
    category: Optional[str] = None
    label: Optional[str] = None
    amount_cr: Optional[float] = None
    year_committed: Optional[str] = None
    notes: Optional[str] = None
    source_url: Optional[str] = None


class StateFinanceDoc(FirestoreDoc):
    """One document per fiscal year (id: ``fiscal_year`` e.g. ``2025-26``).

    The PRS payload is deeply nested and varies year to year, so the nested
    blocks are accepted as free-form dicts; only ``fiscal_year`` is enforced.
    """

    fiscal_year: str
    summary: Optional[Dict[str, Any]] = None
    receipts: Optional[Dict[str, Any]] = None
    committed_expenditure: Optional[Dict[str, Any]] = None
    debt_context: Optional[Dict[str, Any]] = None
    sector_expenditure: Optional[List[Dict[str, Any]]] = None
    debt_why: Optional[List[DebtWhyItem]] = None
    viz_metrics: Optional[Dict[str, Any]] = None
    ground_truth_confidence: Optional[Confidence] = None


class DebtHistoryDoc(FirestoreDoc):
    """One document per fiscal year in the curated debt trend series."""

    fiscal_year: str
    outstanding_debt_cr: Optional[float] = None
    debt_to_gsdp_pct: Optional[float] = None
    revenue_receipts_cr: Optional[float] = None
    interest_payments_cr: Optional[float] = None
    interest_as_pct_revenue: Optional[float] = None
    fiscal_deficit_pct_gsdp: Optional[float] = None
    within_frbm_limits: Optional[bool] = None
    frbm_limit_pct: Optional[float] = None
    debt_why: Optional[List[DebtWhyItem]] = Field(default_factory=list)
    source_url: Optional[str] = None
    ground_truth_confidence: Optional[Confidence] = None


class SubAllocation(FirestoreDoc):
    label: Optional[str] = None
    amount_cr: Optional[float] = None


class DepartmentalSpendingDoc(FirestoreDoc):
    """One document per (fiscal_year, department) (id: ``{year}_{dept_slug}``)."""

    doc_id: str
    fiscal_year: str
    department: Optional[str] = None
    department_slug: Optional[str] = None
    allocation_cr: Optional[float] = None
    actuals_prior_year_cr: Optional[float] = None
    revised_estimate_cr: Optional[float] = None
    pct_of_sector_budget: Optional[float] = None
    pct_change_from_re: Optional[float] = None
    sub_allocations: Optional[List[SubAllocation]] = Field(default_factory=list)
    source_url: Optional[str] = None
    ground_truth_confidence: Optional[Confidence] = None
