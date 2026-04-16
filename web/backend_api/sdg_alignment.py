"""SDG Alignment — precomputed per (party_id, election_year) with in-memory cache.

Source of truth for promise → SDG is the Knowledge Graph's `targets_goal` edges
(which carry weights: 1.0 primary, 0.7 secondary, 0.4 indirect).

Ported from `web/src/lib/sdg-mapping.ts`. The frontend used to compute this on
every render; now the backend computes it once per party and caches the result
(manifestos are fixed for an election cycle).
"""
from __future__ import annotations

import threading
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

import networkx as nx

# SDG dependency graph — "to deliver SDG X, you also need SDG Y".
# Identical to the TS constant in sdg-mapping.ts.
SDG_DEPENDENCIES: Dict[int, List[int]] = {
    1:  [8, 4],
    2:  [1, 8],
    3:  [2, 6],
    4:  [1, 3],
    5:  [1, 4, 8],
    6:  [9, 11],
    7:  [9],
    8:  [4, 9],
    9:  [7, 11],
    10: [1, 4, 8],
    11: [9, 7],
    13: [7, 15],
    14: [6, 13],
    15: [2, 13],
    16: [1, 4, 10],
}


def _impact_score(promise: Dict[str, Any], edge_weight: float = 1.0) -> float:
    """Ported from TS `impactScore` + multiplied by the KG edge weight so
    primary (1.0) SDG targets count fully, secondary (0.7) at 70%, indirect (0.4)
    at 40%."""
    depth_map = {"transformative": 3, "substantive": 2, "supplemental": 1, "symbolic": 0}
    depth = depth_map.get(promise.get("impact_depth") or "", 1)  # absent → 1

    cov_map = {"universal": 1.5, "broad_majority": 1.2, "targeted_poor": 1.1}
    mult = cov_map.get(promise.get("beneficiary_coverage") or "", 1.0)

    risk_map = {"low": 1.0, "medium": 0.85, "high": 0.65}
    risk = risk_map.get(promise.get("implementation_risk") or "", 1.0)

    root = promise.get("root_cause_addressed")
    root_factor = 1.0 if root is True else (0.8 if root is False else 1.0)

    return depth * mult * risk * root_factor * edge_weight


def _score_to_quality(score: float) -> str:
    if score >= 3.5:
        return "strong"
    if score >= 1.0:
        return "moderate"
    if score > 0:
        return "weak"
    return "none"


def _specificity(p: Dict[str, Any]) -> int:
    return (2 if p.get("amount_mentioned") else 0) + (1 if p.get("scheme_name") else 0)


def compute_party_alignment(
    g: nx.MultiDiGraph,
    promises: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Walk the KG for every promise; aggregate per SDG; apply impact scoring.

    Returns a list of 17 SDGCoverage dicts (one per SDG 1..17) in a shape
    compatible with the frontend SDGCoverage TS type.
    """
    # Step 1 — collect (promise, edge_weight) tuples per SDG via the KG.
    per_sdg: Dict[int, List[Tuple[Dict[str, Any], float]]] = defaultdict(list)
    for promise in promises:
        doc_id = promise.get("doc_id")
        if not doc_id:
            continue
        node_id = f"promise:{doc_id}"
        if node_id not in g:
            continue
        for _, target, data in g.out_edges(node_id, data=True):
            if data.get("verb") != "targets_goal":
                continue
            if not target.startswith("sdg:"):
                continue
            try:
                sdg_num = int(target.split(":", 1)[1])
            except ValueError:
                continue
            weight = float(data.get("weight") or 1.0)
            per_sdg[sdg_num].append((promise, weight))

    # Step 2 — per-SDG coverage object.
    coverage: Dict[int, Dict[str, Any]] = {}
    for sdg in range(1, 18):
        entries = per_sdg.get(sdg, [])
        deps = SDG_DEPENDENCIES.get(sdg, [])

        if not entries:
            coverage[sdg] = {
                "sdg_id": sdg,
                "covered": False,
                "coverage_quality": "none",
                "promise_count": 0,
                "effective_promise_count": 0,
                "contributing_pillars": [],
                "top_promises": [],
                "dependency_ids": deps,
                "chain_breaks": [],
                "top_gap_notes": [],
            }
            continue

        contributing = sorted({p.get("category") for p, _ in entries if p.get("category")})
        effective = sum(
            1 for p, _ in entries
            if p.get("impact_depth") in ("transformative", "substantive")
        )

        # Rank: weighted impact score desc, then specificity desc.
        ranked = sorted(
            entries,
            key=lambda pw: (-_impact_score(pw[0], pw[1]), -_specificity(pw[0])),
        )

        # Top 3 only — prevents inflation from many low-quality promises.
        total_score = sum(_impact_score(p, w) for p, w in ranked[:3])
        quality = _score_to_quality(total_score)

        top_gap_notes: List[str] = []
        for p, _ in ranked[:5]:
            note = p.get("coverage_gap_note")
            if isinstance(note, str) and note.strip() and note not in top_gap_notes:
                top_gap_notes.append(note)
            if len(top_gap_notes) >= 2:
                break

        coverage[sdg] = {
            "sdg_id": sdg,
            "covered": quality != "none",
            "coverage_quality": quality,
            "promise_count": len(entries),
            "effective_promise_count": effective,
            "contributing_pillars": contributing,
            # Return the full promise doc (including deep-analysis fields like
            # impact_mechanism, promise_components, sustainability_verdict) so
            # the frontend "How?" modal can render without a second fetch.
            "top_promises": [p for p, _ in ranked[:3]],
            "dependency_ids": deps,
            "chain_breaks": [],  # filled in pass 2
            "top_gap_notes": top_gap_notes,
        }

    # Step 3 — chain breaks (covered SDGs whose dependencies are uncovered).
    for cov in coverage.values():
        if not cov["covered"]:
            continue
        cov["chain_breaks"] = [
            d for d in cov["dependency_ids"]
            if not coverage.get(d, {}).get("covered", False)
        ]

    return [coverage[sdg] for sdg in range(1, 18)]


# ── Cache ────────────────────────────────────────────────────────────────────
# Manifestos are fixed for an election cycle, so we cache indefinitely.
# Keyed by (party_id, year). Explicit invalidation via clear_cache().

_cache: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
_lock = threading.Lock()


def get_cached(party_id: str, year: int) -> Optional[List[Dict[str, Any]]]:
    with _lock:
        return _cache.get((party_id, year))


def set_cached(party_id: str, year: int, result: List[Dict[str, Any]]) -> None:
    with _lock:
        _cache[(party_id, year)] = result


def clear_cache(party_id: Optional[str] = None, year: Optional[int] = None) -> int:
    """Evict entries. No args → wipe everything. Returns the count removed."""
    with _lock:
        if party_id is None and year is None:
            n = len(_cache)
            _cache.clear()
            return n
        keys = [
            k for k in _cache
            if (party_id is None or k[0] == party_id)
            and (year is None or k[1] == year)
        ]
        for k in keys:
            del _cache[k]
        return len(keys)


def cache_stats() -> Dict[str, Any]:
    with _lock:
        return {
            "size": len(_cache),
            "keys": [f"{p}:{y}" for (p, y) in _cache.keys()],
        }
