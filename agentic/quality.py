"""Critic / Verifier — a data-quality gate over *values* (report-only).

Module 1 validates document *structure* (types, required fields, enums). This
layer validates the *content*: numbers in plausible ranges, internal consistency
(e.g. net_assets == assets − liabilities), statistical outliers, and
manifesto-extraction hallucination signals (the class of bug that once produced
"200 new TASMAC outlets" for an anti-liquor party).

It is **report-only**: it never modifies or blocks data — it emits `Finding`s
(info / warning / error) for human review, written to a `quality_findings`
collection by the verify job. Each sweep is a Module-2 run.

All checks operate on plain lists of dicts, so the whole thing is testable
offline against the real `data/processed/*.json` files.
"""

from __future__ import annotations

import statistics
from abc import ABC, abstractmethod
from dataclasses import dataclass, field as dc_field  # 'field' is a Finding attribute name
from typing import Any, Callable, Dict, List, Optional, Tuple

Severity = str  # "info" | "warning" | "error"


@dataclass
class Finding:
    collection: str
    check: str
    severity: Severity
    message: str
    doc_id: Optional[str] = None
    field: Optional[str] = None
    detail: Dict[str, Any] = dc_field(default_factory=dict)


@dataclass
class QualityReport:
    collection: str
    docs_checked: int
    findings: List[Finding] = dc_field(default_factory=list)

    def by_severity(self, severity: Severity) -> List[Finding]:
        return [f for f in self.findings if f.severity == severity]

    @property
    def errors(self) -> List[Finding]:
        return self.by_severity("error")

    @property
    def ok(self) -> bool:
        return not self.errors

    def summary(self) -> str:
        e = len(self.by_severity("error"))
        w = len(self.by_severity("warning"))
        i = len(self.by_severity("info"))
        return f"[{self.collection}] {self.docs_checked} docs · {e} errors · {w} warnings · {i} info"


def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _doc_id(doc: Dict[str, Any], id_field: Optional[str]) -> Optional[str]:
    if id_field and doc.get(id_field) is not None:
        return str(doc[id_field])
    return None


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


class Check(ABC):
    name: str = "check"

    @abstractmethod
    def run(self, collection: str, docs: List[Dict[str, Any]], id_field: Optional[str]) -> List[Finding]: ...


class RangeCheck(Check):
    """Numeric fields must fall within configured [lo, hi] (None = unbounded)."""

    name = "range"

    def __init__(self, bounds: Dict[str, Dict[str, Tuple[Optional[float], Optional[float]]]]):
        self.bounds = bounds

    def run(self, collection, docs, id_field):
        spec = self.bounds.get(collection, {})
        out: List[Finding] = []
        for d in docs:
            for fld, (lo, hi) in spec.items():
                v = d.get(fld)
                if not _is_num(v):
                    continue
                if (lo is not None and v < lo) or (hi is not None and v > hi):
                    out.append(Finding(collection, self.name, "error",
                                       f"{fld}={v} outside [{lo}, {hi}]",
                                       _doc_id(d, id_field), fld, {"value": v}))
        return out


class OutlierCheck(Check):
    """Flags statistical outliers (beyond Q1/Q3 ± 3·IQR) within the batch."""

    name = "outlier"

    def __init__(self, fields: Dict[str, List[str]]):
        self.fields = fields

    def run(self, collection, docs, id_field):
        out: List[Finding] = []
        for fld in self.fields.get(collection, []):
            pairs = [(d, d.get(fld)) for d in docs if _is_num(d.get(fld))]
            nums = sorted(v for _, v in pairs)
            if len(nums) < 8:
                continue
            q1 = nums[len(nums) // 4]
            q3 = nums[(len(nums) * 3) // 4]
            iqr = q3 - q1
            if iqr > 0:
                lo, hi = q1 - 3 * iqr, q3 + 3 * iqr
            else:
                # Degenerate spread (e.g. a constant baseline + one spike):
                # fall back to a mean ± 4·σ fence so the spike is still caught.
                sd = statistics.pstdev(nums)
                if sd <= 0:
                    continue
                mean = statistics.fmean(nums)
                lo, hi = mean - 4 * sd, mean + 4 * sd
            for d, v in pairs:
                if v < lo or v > hi:
                    out.append(Finding(collection, self.name, "info",
                                       f"{fld}={v} is a statistical outlier (fence [{round(lo, 2)}, {round(hi, 2)}])",
                                       _doc_id(d, id_field), fld, {"value": v}))
        return out


# A consistency rule inspects one doc and optionally returns
# (severity, message, field, detail).
ConsistencyRule = Callable[[Dict[str, Any]], Optional[Tuple[str, str, Optional[str], Dict[str, Any]]]]


class ConsistencyCheck(Check):
    """Cross-field internal-consistency rules, per collection."""

    name = "consistency"

    def __init__(self, rules: Dict[str, List[ConsistencyRule]]):
        self.rules = rules

    def run(self, collection, docs, id_field):
        out: List[Finding] = []
        for d in docs:
            for rule in self.rules.get(collection, []):
                r = rule(d)
                if r:
                    sev, msg, fld, detail = r
                    out.append(Finding(collection, self.name, sev, msg, _doc_id(d, id_field), fld, detail or {}))
        return out


class ManifestoHallucinationCheck(Check):
    """Heuristic signals that a manifesto promise was mis-extracted."""

    name = "manifesto_signal"
    _UNAVAILABLE = ("data unavailable", "cannot calculate", "cannot be calculated", "not calculable")

    def run(self, collection, docs, id_field):
        if collection != "manifesto_promises":
            return []
        out: List[Finding] = []
        for d in docs:
            did = _doc_id(d, id_field)
            text = (d.get("promise_text_en") or "").strip()
            if len(text) < 10:
                out.append(Finding(collection, self.name, "warning",
                                   "promise_text_en empty or too short", did, "promise_text_en", {}))
            amount = (d.get("amount_mentioned") or "").strip()
            note = (d.get("fiscal_cost_note") or "").lower()
            if amount and any(u in note for u in self._UNAVAILABLE):
                out.append(Finding(collection, self.name, "info",
                                   "amount stated but fiscal_cost_note says data unavailable",
                                   did, "fiscal_cost_note", {"amount": amount}))
        return out


# ---------------------------------------------------------------------------
# Default consistency rules
# ---------------------------------------------------------------------------


def _net_assets_rule(d):
    a, l, n = d.get("assets_cr"), d.get("liabilities_cr"), d.get("net_assets_cr")
    if all(_is_num(x) for x in (a, l, n)) and abs((a - l) - n) > 0.01:
        return ("warning", f"net_assets_cr={n} ≠ assets−liabilities ({round(a - l, 4)})",
                "net_assets_cr", {"assets": a, "liabilities": l, "net": n})
    return None


def _crorepati_rule(d):
    a, flag = d.get("assets_cr"), d.get("is_crorepati")
    if _is_num(a) and isinstance(flag, bool) and flag != (a >= 1.0):
        return ("warning", f"is_crorepati={flag} but assets_cr={a}", "is_crorepati", {})
    return None


def _severity_rule(d):
    c, s = d.get("criminal_cases_total"), d.get("criminal_severity")
    if isinstance(c, int) and s:
        expected = "CLEAN" if c == 0 else "MINOR" if c <= 2 else "MODERATE" if c <= 5 else "SERIOUS"
        if s != expected:
            return ("warning", f"criminal_severity={s} but {c} cases (expected {expected})",
                    "criminal_severity", {})
    return None


def _socio_percent_rule(d):
    u, v = (d.get("unit") or "").lower(), d.get("value")
    if "percent" in u and _is_num(v) and (v < 0 or v > 100):
        return ("error", f"value={v} but unit is percent (expected 0–100)", "value", {})
    return None


# ---------------------------------------------------------------------------
# Default configuration + verifier
# ---------------------------------------------------------------------------

DEFAULT_RANGE_BOUNDS: Dict[str, Dict[str, Tuple[Optional[float], Optional[float]]]] = {
    "candidate_accountability": {"criminal_cases_total": (0, 200), "assets_cr": (0, None), "liabilities_cr": (0, None)},
    "party_accountability": {"criminal_cases_pct": (0, 100), "serious_cases_pct": (0, 100),
                              "crorepati_pct": (0, 100), "graduate_pct": (0, 100),
                              "avg_assets_cr": (0, None), "mla_count": (0, None)},
    "debt_history": {"debt_to_gsdp_pct": (0, 100), "interest_as_pct_revenue": (0, 100),
                      "fiscal_deficit_pct_gsdp": (-20, 20), "outstanding_debt_cr": (0, None)},
    "departmental_spending": {"allocation_cr": (0, None), "pct_of_sector_budget": (0, 100)},
    "manifesto_promises": {"target_year": (1990, 2035)},
}

DEFAULT_OUTLIER_FIELDS: Dict[str, List[str]] = {
    "candidate_accountability": ["assets_cr", "criminal_cases_total"],
    "party_accountability": ["avg_assets_cr"],
}

DEFAULT_CONSISTENCY_RULES: Dict[str, List[ConsistencyRule]] = {
    "candidate_accountability": [_net_assets_rule, _crorepati_rule, _severity_rule],
    "socio_economics": [_socio_percent_rule],
}


class Verifier:
    def __init__(self, checks: List[Check]):
        self.checks = checks

    def run(self, collection: str, docs: List[Dict[str, Any]], id_field: Any = "__auto__") -> QualityReport:
        if id_field == "__auto__":
            try:
                from schemas import get_id_field

                id_field = get_id_field(collection)
            except Exception:
                id_field = None
        findings: List[Finding] = []
        for check in self.checks:
            findings.extend(check.run(collection, docs, id_field))
        return QualityReport(collection=collection, docs_checked=len(docs), findings=findings)


def default_verifier() -> Verifier:
    return Verifier([
        RangeCheck(DEFAULT_RANGE_BOUNDS),
        ConsistencyCheck(DEFAULT_CONSISTENCY_RULES),
        OutlierCheck(DEFAULT_OUTLIER_FIELDS),
        ManifestoHallucinationCheck(),
    ])


def verify_docs(collection: str, docs: List[Dict[str, Any]]) -> QualityReport:
    return default_verifier().run(collection, docs)
