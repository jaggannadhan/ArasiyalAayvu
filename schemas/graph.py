"""Schemas for the knowledge-graph payload (``knowledge_graph_slim.json``).

Unlike the other modules these do not map to a Firestore *collection* of docs —
the graph is a single GCS/Firestore JSON document of ``{nodes, edges, meta}``.
The node/edge models are provided so the graph file can be validated and so
Module 6 (GraphRAG) has a typed handle on graph elements.
"""

from __future__ import annotations

from typing import Any, List, Optional

from pydantic import Field

from ._base import FirestoreDoc


class KGNode(FirestoreDoc):
    id: str
    type: str
    label: Optional[str] = None
    layer: Optional[Any] = None  # int z-order or string layer label
    color: Optional[str] = None
    slug: Optional[str] = None
    party: Optional[str] = None
    constituency: Optional[str] = None
    category: Optional[str] = None
    fx: Optional[float] = None
    fy: Optional[float] = None


class KGEdge(FirestoreDoc):
    source: str
    target: str
    verb: str
    weight: Optional[float] = None
    period: Optional[Any] = None


class KnowledgeGraphDoc(FirestoreDoc):
    nodes: List[KGNode] = Field(default_factory=list)
    edges: List[KGEdge] = Field(default_factory=list)
    meta: Optional[dict[str, Any]] = None
