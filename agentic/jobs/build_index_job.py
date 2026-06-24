"""Cloud Run Job: rebuild the GraphRAG index and publish it to GCS (Module 6).

Pulls the knowledge graph from GCS and manifesto promises from Firestore, builds
the vector index, and uploads it to ``gs://<bucket>/graphrag/latest.json`` — the
artifact the backend's ``/api/ask`` endpoint loads.

    python -m agentic.jobs.build_index_job            # HashingEmbedder (no deps)
    python -m agentic.jobs.build_index_job --vertex   # Vertex embeddings (prod quality)
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from typing import Any, Callable, List, Optional, Tuple

GCS_BUCKET = "naatunadappu-media"
KG_BLOB = "knowledge_graph/latest.json"
INDEX_BLOB = "graphrag/latest.json"

Sources = Tuple[List[dict], List[dict], List[dict]]  # nodes, edges, promises


def build_and_upload(
    *,
    load_sources: Callable[[], Sources],
    upload: Callable[[str], None],
    embedder: Any = None,
    run_store: Any = None,
):
    """Testable core: build the index from injected sources and upload it."""
    from agentic.graphrag import GraphRAG, HashingEmbedder
    from agentic.provenance import RunContext

    embedder = embedder or HashingEmbedder()
    with RunContext(tool="build_index_job", trigger="scheduler",
                    args={"embedder": embedder.name}, store=run_store) as run:
        nodes, edges, promises = load_sources()
        rag = GraphRAG.build_from_sources(embedder, nodes=nodes, edges=edges, promises=promises)
        run.record_write("graphrag_index", len(rag.index.records))
        tmp = os.path.join(tempfile.gettempdir(), "graphrag_index.json")
        rag.save(tmp)
        upload(tmp)
    return rag


# --- production source/upload wiring (GCS + Firestore) ---------------------


def _cloud_load_sources(project: str) -> Sources:
    from google.cloud import firestore, storage

    blob = storage.Client(project=project).bucket(GCS_BUCKET).blob(KG_BLOB)
    kg = json.loads(blob.download_as_text())
    nodes, edges = kg.get("nodes", []), kg.get("edges", [])

    db = firestore.Client(project=project)
    promises = [d.to_dict() for d in db.collection("manifesto_promises").stream()]
    return nodes, edges, promises


def _cloud_upload(project: str) -> Callable[[str], None]:
    from google.cloud import storage

    def _upload(path: str) -> None:
        storage.Client(project=project).bucket(GCS_BUCKET).blob(INDEX_BLOB).upload_from_filename(path)
        print(f"  uploaded gs://{GCS_BUCKET}/{INDEX_BLOB}")

    return _upload


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Rebuild + publish the GraphRAG index")
    ap.add_argument("--vertex", action="store_true", help="use Vertex AI embeddings")
    a = ap.parse_args(argv)

    project = os.environ.get("GOOGLE_CLOUD_PROJECT", "naatunadappu")
    embedder = None
    if a.vertex:
        from agentic.graphrag import VertexEmbedder

        embedder = VertexEmbedder()

    rag = build_and_upload(
        load_sources=lambda: _cloud_load_sources(project),
        upload=_cloud_upload(project),
        embedder=embedder,
    )
    print(f"Built + published index: {len(rag.index.records)} records ({rag.embedder.name})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
