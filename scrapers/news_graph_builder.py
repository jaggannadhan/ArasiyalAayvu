"""News Knowledge Graph Builder.

Reads NER-enriched articles from Firestore (news_articles collection),
builds a NetworkX-compatible graph, and uploads to GCS as a separate
graph that can be linked to the Election KG at query time.

Usage
-----
    # Build + save locally:
    python scrapers/news_graph_builder.py

    # Build + upload to GCS + clear backend cache:
    python scrapers/news_graph_builder.py --upload

    # Print stats only:
    python scrapers/news_graph_builder.py --stats

Env
---
    GOOGLE_CLOUD_PROJECT  (default: naatunadappu)
    BACKEND_URL           (default: https://arasiyalaayvu-be-bo6oacabma-uc.a.run.app)
"""
from __future__ import annotations

import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "naatunadappu")
BACKEND_URL = os.environ.get(
    "BACKEND_URL", "https://arasiyalaayvu-be-bo6oacabma-uc.a.run.app"
)

OUT_DIR = ROOT / "data" / "processed"
OUT_PATH = OUT_DIR / "news_knowledge_graph.json"

# Node type → visual config (layer, color) for frontend rendering
NODE_TYPE_CONFIG: dict[str, dict[str, str]] = {
    "Person":      {"layer": "political",       "color": "#6366f1"},  # indigo
    "Party":       {"layer": "political",       "color": "#ef4444"},  # red
    "Institution": {"layer": "governance",      "color": "#f59e0b"},  # amber
    "Community":   {"layer": "social",          "color": "#10b981"},  # emerald
    "Place":       {"layer": "geographic",      "color": "#3b82f6"},  # blue
    "Policy":      {"layer": "governance",      "color": "#8b5cf6"},  # violet
    "Event":       {"layer": "temporal",        "color": "#f97316"},  # orange
    "Industry":    {"layer": "economic",        "color": "#14b8a6"},  # teal
    "SDG":         {"layer": "sdg",             "color": "#06b6d4"},  # cyan
    "Topic":       {"layer": "thematic",        "color": "#a855f7"},  # purple
    "Resource":    {"layer": "economic",        "color": "#84cc16"},  # lime
}


def get_firestore_client():
    from google.cloud import firestore
    return firestore.Client(project=PROJECT)


def fetch_news_articles(db) -> list[dict[str, Any]]:
    """Fetch all NER-enriched articles from Firestore."""
    docs = list(db.collection("news_articles").stream())
    articles = []
    for doc in docs:
        d = doc.to_dict()
        d["_doc_id"] = doc.id
        articles.append(d)
    return articles


class NewsGraphBuilder:
    """Builds a graph from NER-enriched news articles."""

    def __init__(self, articles: list[dict[str, Any]]):
        self.articles = articles
        self.nodes: dict[str, dict[str, Any]] = {}  # node_id → node data
        self.edges: list[dict[str, Any]] = []
        self.edge_set: set[tuple[str, str, str]] = set()  # dedup edges

    def _add_node(
        self,
        node_id: str,
        name: str,
        node_type: str,
        canonical_id: str | None = None,
        sdg_ids: list[str] | None = None,
        **kwargs,
    ) -> str:
        """Add or update a node. Returns the effective node_id."""
        # Use canonical_id as the node_id when available for merging
        effective_id = canonical_id if canonical_id and canonical_id != "null" else node_id
        effective_id = effective_id.lower().replace(" ", "_").replace(".", "")

        if effective_id in self.nodes:
            # Merge: increment mention count, update sdg_ids
            existing = self.nodes[effective_id]
            existing["mentions"] = existing.get("mentions", 1) + 1
            if sdg_ids:
                existing_sdgs = set(existing.get("sdg_ids", []))
                existing_sdgs.update(sdg_ids)
                existing["sdg_ids"] = sorted(existing_sdgs)
            # Keep article references
            article_ids = existing.get("article_ids", [])
            if kwargs.get("article_id") and kwargs["article_id"] not in article_ids:
                article_ids.append(kwargs["article_id"])
                existing["article_ids"] = article_ids
        else:
            config = NODE_TYPE_CONFIG.get(node_type, {"layer": "other", "color": "#9ca3af"})
            self.nodes[effective_id] = {
                "id": effective_id,
                "type": node_type,
                "label": name,
                "layer": config["layer"],
                "color": config["color"],
                "mentions": 1,
                "sdg_ids": sdg_ids or [],
                "article_ids": [kwargs["article_id"]] if kwargs.get("article_id") else [],
                **({"canonical_id": canonical_id} if canonical_id and canonical_id != "null" else {}),
            }

        return effective_id

    def _add_edge(
        self,
        source: str,
        target: str,
        verb: str,
        weight: float = 1.0,
        article_id: str | None = None,
        timestamp: str | None = None,
        confidence: float = 0.8,
    ) -> None:
        """Add an edge, deduplicating by (source, target, verb)."""
        source = source.lower().replace(" ", "_").replace(".", "")
        target = target.lower().replace(" ", "_").replace(".", "")

        if source not in self.nodes or target not in self.nodes:
            return

        key = (source, target, verb)
        if key in self.edge_set:
            # Find and update existing edge weight
            for e in self.edges:
                if e["source"] == source and e["target"] == target and e["verb"] == verb:
                    e["weight"] = e.get("weight", 1.0) + 0.5  # Reinforce repeated edges
                    break
            return

        self.edge_set.add(key)
        self.edges.append({
            "source": source,
            "target": target,
            "verb": verb,
            "weight": weight,
            **({"article_id": article_id} if article_id else {}),
            **({"timestamp": timestamp} if timestamp else {}),
            **({"confidence": confidence} if confidence < 1.0 else {}),
        })

    def build(self) -> dict[str, Any]:
        """Build the graph from all articles."""
        skipped = 0

        for article in self.articles:
            entities = article.get("entities", [])
            relations = article.get("relations", [])
            timestamp = article.get("published_at", "")
            article_id = article.get("ov_id", article.get("_doc_id", ""))

            if not entities:
                skipped += 1
                continue

            # Add all entity nodes
            id_map: dict[str, str] = {}  # original node_id → effective node_id
            for ent in entities:
                eid = self._add_node(
                    node_id=ent["node_id"],
                    name=ent["name"],
                    node_type=ent["type"],
                    canonical_id=ent.get("canonical_id"),
                    sdg_ids=ent.get("sdg_ids", []),
                    article_id=article_id,
                )
                id_map[ent["node_id"]] = eid

            # Add relation edges
            for rel in relations:
                src = id_map.get(rel["subject"], rel["subject"])
                tgt = id_map.get(rel["object"], rel["object"])
                self._add_edge(
                    source=src,
                    target=tgt,
                    verb=rel["predicate"],
                    article_id=article_id,
                    timestamp=timestamp,
                    confidence=rel.get("confidence", 0.8),
                )

            # Add SDG edges for entities with sdg_ids
            for ent in entities:
                for sdg_id in ent.get("sdg_ids", []):
                    sdg_node = self._add_node(
                        node_id=sdg_id.lower(),
                        name=sdg_id,
                        node_type="SDG",
                    )
                    ent_id = id_map.get(ent["node_id"], ent["node_id"])
                    self._add_edge(
                        source=ent_id,
                        target=sdg_node,
                        verb="relates_to",
                        weight=0.5,
                        timestamp=timestamp,
                    )

        print(f"  Processed {len(self.articles)} articles ({skipped} skipped — no entities)")

        # Build output
        node_list = list(self.nodes.values())

        # Sort nodes by mention count (most mentioned first)
        node_list.sort(key=lambda n: n.get("mentions", 0), reverse=True)

        meta = {
            "node_count": len(node_list),
            "edge_count": len(self.edges),
            "article_count": len(self.articles) - skipped,
            "layers": sorted(set(n["layer"] for n in node_list)),
            "node_types": dict(Counter(n["type"] for n in node_list).most_common()),
            "edge_verbs": dict(Counter(e["verb"] for e in self.edges).most_common()),
            "built_at": datetime.now(timezone.utc).isoformat(),
        }

        return {"nodes": node_list, "edges": self.edges, "meta": meta}


def precompute_layout(graph: dict) -> dict:
    """Run spring layout for stable node positions."""
    import networkx as nx

    print("  Pre-computing layout...")
    G = nx.Graph()
    for n in graph["nodes"]:
        G.add_node(n["id"])
    for e in graph["edges"]:
        G.add_edge(e["source"], e["target"], weight=e.get("weight", 1.0))

    if len(G.nodes) == 0:
        return graph

    pos = nx.spring_layout(G, k=2.5, iterations=100, seed=42, scale=800)
    pos_map = {nid: (float(xy[0]), float(xy[1])) for nid, xy in pos.items()}
    for node in graph["nodes"]:
        xy = pos_map.get(node["id"])
        if xy:
            node["fx"] = round(xy[0], 1)
            node["fy"] = round(xy[1], 1)

    print(f"    Positioned {len(pos_map)} nodes")
    return graph


def main():
    upload = "--upload" in sys.argv
    stats = "--stats" in sys.argv

    print("Building News Knowledge Graph...")
    db = get_firestore_client()

    print("  Fetching articles from Firestore...")
    articles = fetch_news_articles(db)
    print(f"  Found {len(articles)} articles")

    if not articles:
        print("  No articles found. Run news_ingestion.py first.")
        return

    builder = NewsGraphBuilder(articles)
    graph = builder.build()

    print(f"\n  Nodes: {graph['meta']['node_count']}")
    print(f"  Edges: {graph['meta']['edge_count']}")

    if stats:
        print("\n  Node types:")
        for t, c in sorted(graph["meta"]["node_types"].items(), key=lambda x: -x[1]):
            print(f"    {t:<20} {c:>5}")
        print("\n  Edge verbs:")
        for v, c in sorted(graph["meta"]["edge_verbs"].items(), key=lambda x: -x[1]):
            print(f"    {v:<20} {c:>5}")
        print(f"\n  Top 20 nodes by mentions:")
        for n in graph["nodes"][:20]:
            print(f"    {n['label']:<30} [{n['type']:<12}] mentions={n['mentions']}")
        return

    # Layout
    graph = precompute_layout(graph)

    # Save locally
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(graph, default=str, ensure_ascii=False))
    print(f"\n  Wrote {OUT_PATH}  ({OUT_PATH.stat().st_size // 1024} KB)")

    if upload:
        try:
            from google.cloud import storage
            client = storage.Client()
            bucket = client.bucket("naatunadappu-media")
            blob = bucket.blob("news_knowledge_graph/latest.json")
            blob.cache_control = "no-store, max-age=0"
            blob.upload_from_string(
                OUT_PATH.read_text(encoding="utf-8"),
                content_type="application/json",
            )
            print("  Uploaded to gs://naatunadappu-media/news_knowledge_graph/latest.json")

            # Clear backend cache
            import requests
            try:
                r = requests.post(f"{BACKEND_URL}/api/news-graph/cache/clear", timeout=15)
                if r.ok:
                    print(f"  Cleared backend news KG cache: {r.json()}")
            except Exception as exc:
                print(f"  (Skipped backend cache clear: {exc})")
        except Exception as exc:
            print(f"  GCS upload failed: {exc}")


if __name__ == "__main__":
    main()
