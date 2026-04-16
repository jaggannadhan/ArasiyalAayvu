"""Graph Query Engine — runtime traversal over the Knowledge Graph.

Loads the pre-built KG (nodes + edges as temporal quadruples) into a NetworkX
MultiDiGraph in memory, exposes traversal primitives, and implements the
promise-feasibility computation that walks:

    promise --targets_goal--> sdg --measured_by--> indicator --influences--> ...

and cross-joins against fiscal snapshots to produce a feasibility estimate.
"""
from __future__ import annotations

import json
import re
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import networkx as nx

ROOT_DIR = Path(__file__).resolve().parents[2]
LOCAL_KG_PATH = ROOT_DIR / "data" / "processed" / "knowledge_graph_slim.json"

# Cache the assembled graph in memory. TTL matches the GCS cache in main.py.
_GRAPH_CACHE: Dict[str, Any] = {"graph": None, "raw": None, "ts": 0.0}
_CACHE_TTL_SEC = 3600


def _load_raw_from_gcs(project_id: str) -> Optional[Dict[str, Any]]:
    try:
        from google.cloud import storage
        client = storage.Client(project=project_id)
        blob = client.bucket("naatunadappu-media").blob("knowledge_graph/latest.json")
        return json.loads(blob.download_as_text())
    except Exception:
        return None


def _load_raw_from_disk() -> Optional[Dict[str, Any]]:
    if LOCAL_KG_PATH.exists():
        with LOCAL_KG_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    return None


def load_graph(project_id: str = "naatunadappu", force: bool = False) -> Tuple[nx.MultiDiGraph, Dict[str, Any]]:
    """Return (graph, raw_payload). Prefers GCS, falls back to local disk."""
    now = time.time()
    if not force and _GRAPH_CACHE["graph"] is not None and now - _GRAPH_CACHE["ts"] < _CACHE_TTL_SEC:
        return _GRAPH_CACHE["graph"], _GRAPH_CACHE["raw"]
    # Reset before re-fetching so a partial load doesn't leak old state.
    _GRAPH_CACHE.update({"graph": None, "raw": None, "ts": 0.0})

    raw = _load_raw_from_gcs(project_id) or _load_raw_from_disk()
    if raw is None:
        raise RuntimeError("Knowledge graph not available in GCS or on disk")

    g = nx.MultiDiGraph()
    for node in raw.get("nodes", []):
        g.add_node(node["id"], **{k: v for k, v in node.items() if k != "id"})
    for edge in raw.get("edges", []):
        g.add_edge(
            edge["source"],
            edge["target"],
            key=edge.get("verb", "edge"),
            verb=edge.get("verb"),
            weight=edge.get("weight", 1.0),
            period=edge.get("period"),
        )

    _GRAPH_CACHE.update({"graph": g, "raw": raw, "ts": now})
    return g, raw


def clear_cache() -> Dict[str, Any]:
    """Evict the in-memory KG. Next load_graph() call will re-fetch from GCS."""
    had_graph = _GRAPH_CACHE["graph"] is not None
    _GRAPH_CACHE.update({"graph": None, "raw": None, "ts": 0.0})
    return {"cleared": had_graph}


# ── Traversal primitives ─────────────────────────────────────────────────────

def neighbors(
    g: nx.MultiDiGraph,
    node_id: str,
    verb: Optional[str] = None,
    direction: str = "out",
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """Return immediate neighbors filtered by verb + direction."""
    if node_id not in g:
        return []

    out: List[Dict[str, Any]] = []
    if direction in ("out", "both"):
        for _, tgt, data in g.out_edges(node_id, data=True):
            if verb is None or data.get("verb") == verb:
                out.append({
                    "source": node_id, "target": tgt, "direction": "out",
                    "verb": data.get("verb"), "weight": data.get("weight"),
                    "period": data.get("period"),
                    "target_node": {"id": tgt, **g.nodes[tgt]},
                })
    if direction in ("in", "both"):
        for src, _, data in g.in_edges(node_id, data=True):
            if verb is None or data.get("verb") == verb:
                out.append({
                    "source": src, "target": node_id, "direction": "in",
                    "verb": data.get("verb"), "weight": data.get("weight"),
                    "period": data.get("period"),
                    "source_node": {"id": src, **g.nodes[src]},
                })
    return out[:limit]


def traverse(
    g: nx.MultiDiGraph,
    start: str,
    allowed_verbs: Optional[Iterable[str]] = None,
    max_depth: int = 3,
    max_nodes: int = 500,
) -> Dict[str, Any]:
    """BFS from `start` following only `allowed_verbs`. Returns visited nodes + edges."""
    if start not in g:
        return {"nodes": [], "edges": [], "error": f"Unknown node: {start}"}

    allowed = set(allowed_verbs) if allowed_verbs else None
    visited: Set[str] = {start}
    edges_out: List[Dict[str, Any]] = []
    q: deque[Tuple[str, int]] = deque([(start, 0)])

    while q and len(visited) < max_nodes:
        node, depth = q.popleft()
        if depth >= max_depth:
            continue
        for _, tgt, data in g.out_edges(node, data=True):
            verb = data.get("verb")
            if allowed is not None and verb not in allowed:
                continue
            edges_out.append({
                "source": node, "target": tgt, "verb": verb,
                "weight": data.get("weight"), "period": data.get("period"),
            })
            if tgt not in visited:
                visited.add(tgt)
                q.append((tgt, depth + 1))

    return {
        "start": start,
        "nodes": [{"id": n, **g.nodes[n]} for n in visited],
        "edges": edges_out,
        "depth_limit": max_depth,
    }


def shortest_path(
    g: nx.MultiDiGraph,
    source: str,
    target: str,
    allowed_verbs: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    if source not in g or target not in g:
        return {"path": None, "error": "Unknown source or target"}

    if allowed_verbs:
        allowed = set(allowed_verbs)
        sub = nx.DiGraph()
        for u, v, d in g.edges(data=True):
            if d.get("verb") in allowed:
                sub.add_edge(u, v, verb=d.get("verb"), weight=d.get("weight", 1.0))
        graph_for_search: Any = sub
    else:
        graph_for_search = g

    try:
        path = nx.shortest_path(graph_for_search, source=source, target=target)
    except (nx.NetworkXNoPath, nx.NodeNotFound):
        return {"path": None}

    edges: List[Dict[str, Any]] = []
    for u, v in zip(path, path[1:]):
        # Pick any edge u→v (first matching allowed verb)
        for _, _, data in g.out_edges(u, data=True):
            if (not allowed_verbs or data.get("verb") in allowed_verbs):
                edges.append({"source": u, "target": v, "verb": data.get("verb"),
                              "weight": data.get("weight"), "period": data.get("period")})
                break

    return {
        "path": path,
        "nodes": [{"id": n, **g.nodes[n]} for n in path],
        "edges": edges,
        "length": len(path) - 1,
    }


# ── Feasibility computation ──────────────────────────────────────────────────

# Parses amounts like "₹1000 crore", "Rs. 5,000 cr", "10 lakh crore", "₹2.5 lakh cr".
_AMOUNT_RE = re.compile(
    r"(?:₹|rs\.?|inr)?\s*([\d,]+(?:\.\d+)?)\s*(lakh|thousand|k)?\s*(crore|cr|lakh|lakhs)?",
    re.IGNORECASE,
)


def parse_amount_to_cr(raw: Optional[str]) -> Optional[float]:
    """Best-effort parse of a free-text amount into ₹ crore."""
    if not raw or not isinstance(raw, str):
        return None
    s = raw.lower().replace(",", "").strip()
    if not s or s in {"none", "na", "n/a", "-"}:
        return None

    match = _AMOUNT_RE.search(s)
    if not match:
        return None
    number_s, prefix, suffix = match.groups()
    try:
        number = float(number_s)
    except ValueError:
        return None

    prefix = (prefix or "").strip()
    suffix = (suffix or "").strip()

    # "lakh crore" → *1e5 cr; "crore" → *1 cr; "lakh" alone → /100 cr (1 lakh = 0.01 cr)
    if prefix in {"lakh"} and suffix in {"crore", "cr"}:
        return number * 100_000
    if suffix in {"crore", "cr"}:
        return number
    if suffix in {"lakh", "lakhs"} or prefix in {"lakh"}:
        return number / 100.0  # 1 lakh = 0.01 crore
    # No recognizable unit — assume the author meant crore (most promises do)
    return number


@dataclass
class FeasibilityResult:
    promise_id: str
    promise_label: str
    amount_cr: Optional[float]
    amount_raw: Optional[str]
    sdg_goals: List[Dict[str, Any]]
    affected_indicators: List[Dict[str, Any]]
    causal_chain: List[Dict[str, Any]]
    fiscal_snapshot: Optional[Dict[str, Any]]
    metrics: Dict[str, Optional[float]]
    score: int
    score_band: str
    notes: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "promise_id": self.promise_id,
            "promise_label": self.promise_label,
            "amount_cr": self.amount_cr,
            "amount_raw": self.amount_raw,
            "sdg_goals": self.sdg_goals,
            "affected_indicators": self.affected_indicators,
            "causal_chain": self.causal_chain,
            "fiscal_snapshot": self.fiscal_snapshot,
            "metrics": self.metrics,
            "score": self.score,
            "score_band": self.score_band,
            "notes": self.notes,
        }


def _score_band(score: int) -> str:
    if score >= 75:
        return "High feasibility"
    if score >= 50:
        return "Moderate feasibility"
    if score >= 25:
        return "Stretched"
    return "Low feasibility"


def _compute_score(cost_pct_revenue: Optional[float], cost_pct_fiscal_deficit: Optional[float]) -> int:
    """Heuristic: 100 when cost is negligible vs. revenue, drops sharply as it approaches
    a meaningful share of annual revenue receipts or exceeds the year's fiscal deficit headroom."""
    if cost_pct_revenue is None:
        return 50  # unknown cost → neutral
    # 0% of revenue → 100, 5% → 50, 10%+ → near 0
    revenue_score = max(0.0, 100.0 - cost_pct_revenue * 10.0)
    if cost_pct_fiscal_deficit is not None:
        # If the promise alone exceeds this year's fiscal deficit, penalize
        deficit_penalty = min(50.0, max(0.0, (cost_pct_fiscal_deficit - 25.0)))
        revenue_score -= deficit_penalty
    return max(0, min(100, int(round(revenue_score))))


def compute_feasibility(
    g: nx.MultiDiGraph,
    promise_id: str,
    fetch_promise_doc,                 # callable: (doc_id) -> dict or None
    fetch_state_finances_latest,       # callable: () -> dict or None
    state_slug: str = "tamil_nadu",
    fetch_indicator_snapshot=None,     # callable: (collection, slug) -> dict or None
) -> FeasibilityResult:
    """Walk the KG from a promise node and score its fiscal feasibility.

    `fetch_promise_doc`, `fetch_state_finances_latest`, and `fetch_indicator_snapshot`
    are injected so this module stays Firestore-agnostic (easier to test).
    """
    if promise_id not in g:
        raise KeyError(f"Promise node not in graph: {promise_id}")

    node = g.nodes[promise_id]
    notes: List[str] = []

    # Promise doc in Firestore is keyed by the raw doc_id (sans "promise:" prefix).
    raw_doc_id = promise_id.split(":", 1)[1] if promise_id.startswith("promise:") else promise_id
    doc = fetch_promise_doc(raw_doc_id) or {}
    amount_raw = doc.get("amount_mentioned") or doc.get("amount") or None
    amount_cr = parse_amount_to_cr(amount_raw)
    if amount_raw and amount_cr is None:
        notes.append(f"Could not parse amount_mentioned: {amount_raw!r}")
    if not amount_raw:
        notes.append("Promise has no amount_mentioned — cost unknown")

    # Step 1: promise → targets_goal → sdg
    sdg_edges = [
        (t, d) for _, t, d in g.out_edges(promise_id, data=True)
        if d.get("verb") == "targets_goal"
    ]
    sdg_goals = [
        {
            "id": t,
            "label": g.nodes[t].get("label"),
            "weight": d.get("weight"),
        }
        for t, d in sdg_edges
    ]
    if not sdg_goals:
        notes.append("Promise is not wired to any SDG goal — category → SDG bridge is missing")

    # Step 2: sdg → measured_by → indicator (filter to this state)
    affected: List[Dict[str, Any]] = []
    indicator_ids: Set[str] = set()
    for sdg in sdg_goals:
        for _, ind_id, d in g.out_edges(sdg["id"], data=True):
            if d.get("verb") != "measured_by":
                continue
            ind = g.nodes[ind_id]
            ind_state = ind.get("state") or ind_id.partition(":")[2]
            if ind_state != state_slug:
                continue
            indicator_ids.add(ind_id)
            snapshot = None
            if fetch_indicator_snapshot is not None:
                # ind_id shape is "indicator_<kind>:<state>" — collection is the kind.
                prefix, _, slug = ind_id.partition(":")
                collection = prefix.replace("indicator_", "")
                snapshot = fetch_indicator_snapshot(collection, slug)
            affected.append({
                "sdg_id": sdg["id"],
                "sdg_weight": sdg["weight"],
                "indicator_id": ind_id,
                "indicator_type": ind.get("type"),
                "indicator_label": ind.get("label"),
                "period": d.get("period"),
                "snapshot": snapshot,
            })

    # Step 3: indicator → influences → indicator (causal chain, 1 hop)
    chain: List[Dict[str, Any]] = []
    for ind_id in indicator_ids:
        for _, downstream, d in g.out_edges(ind_id, data=True):
            if d.get("verb") != "influences":
                continue
            dnode = g.nodes[downstream]
            d_state = dnode.get("state") or downstream.partition(":")[2]
            if d_state != state_slug:
                continue
            chain.append({
                "from": ind_id,
                "to": downstream,
                "to_label": dnode.get("label"),
                "weight": d.get("weight"),
                "period": d.get("period"),
            })

    # Step 4: fiscal snapshot + cost ratios
    fiscal = fetch_state_finances_latest() or None
    cost_pct_revenue: Optional[float] = None
    cost_pct_fiscal_deficit: Optional[float] = None
    if fiscal and amount_cr:
        summary = fiscal.get("summary", {}) or {}
        receipts = fiscal.get("receipts", {}) or {}
        rev_receipts = receipts.get("revenue_receipts_cr") or summary.get("net_receipts_cr")
        fisc_deficit = summary.get("fiscal_deficit_cr")
        if rev_receipts:
            cost_pct_revenue = round(amount_cr / rev_receipts * 100, 2)
        if fisc_deficit:
            cost_pct_fiscal_deficit = round(amount_cr / fisc_deficit * 100, 2)

    score = _compute_score(cost_pct_revenue, cost_pct_fiscal_deficit)

    return FeasibilityResult(
        promise_id=promise_id,
        promise_label=node.get("label", ""),
        amount_cr=amount_cr,
        amount_raw=amount_raw,
        sdg_goals=sdg_goals,
        affected_indicators=affected,
        causal_chain=chain,
        fiscal_snapshot=({
            "fiscal_year": fiscal.get("fiscal_year"),
            "summary": fiscal.get("summary"),
            "receipts": fiscal.get("receipts"),
        } if fiscal else None),
        metrics={
            "cost_as_pct_revenue_receipts": cost_pct_revenue,
            "cost_as_pct_fiscal_deficit": cost_pct_fiscal_deficit,
        },
        score=score,
        score_band=_score_band(score),
        notes=notes,
    )
