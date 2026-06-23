"""Tests for Module 6 — GraphRAG (offline, deterministic HashingEmbedder)."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from agentic.graphrag import (
    GraphRAG,
    HashingEmbedder,
    Synthesizer,
    VectorIndex,
    build_local_index,
    get_embedder,
)

REPO = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# embedder
# ---------------------------------------------------------------------------


def test_hashing_embedder_deterministic_and_normalised():
    e = HashingEmbedder(dim=128)
    a = e.embed_one("free bus travel for women")
    b = e.embed_one("free bus travel for women")
    assert np.allclose(a, b)                      # deterministic
    assert abs(np.linalg.norm(a) - 1.0) < 1e-5    # L2-normalised


def test_get_embedder_roundtrip():
    e = get_embedder("hashing-256")
    assert isinstance(e, HashingEmbedder) and e.dim == 256


# ---------------------------------------------------------------------------
# index
# ---------------------------------------------------------------------------


def test_index_search_ranks_by_similarity():
    e = HashingEmbedder(dim=256)
    recs = [
        {"id": "a", "kind": "promise", "text": "waive farmer crop loans agriculture", "citation": "A"},
        {"id": "b", "kind": "promise", "text": "free laptops for college students", "citation": "B"},
        {"id": "c", "kind": "promise", "text": "build metro rail transport infrastructure", "citation": "C"},
    ]
    idx = VectorIndex(e.name, 256)
    idx.add(e.embed([r["text"] for r in recs]), recs)
    top = idx.search(e.embed_one("loan waiver for farmers"), k=1)
    assert top[0][1]["id"] == "a"


def test_index_k_larger_than_corpus():
    e = HashingEmbedder(dim=64)
    recs = [{"id": "x", "kind": "promise", "text": "hello world", "citation": "X"}]
    idx = VectorIndex(e.name, 64)
    idx.add(e.embed(["hello world"]), recs)
    assert len(idx.search(e.embed_one("hello"), k=10)) == 1


# ---------------------------------------------------------------------------
# GraphRAG build / retrieve / answer
# ---------------------------------------------------------------------------


def _sample_rag():
    nodes = [
        {"id": "party:dmk", "type": "party", "label": "DMK", "slug": "dmk"},
        {"id": "constituency:kolathur", "type": "constituency", "label": "Kolathur", "slug": "kolathur"},
    ]
    edges = [{"source": "party:dmk", "target": "constituency:kolathur", "verb": "won"}]
    promises = [
        {"doc_id": "dmk_2026_agri_001", "party_id": "dmk", "target_year": 2026,
         "category": "Agriculture", "scheme_name": "Loan Waiver",
         "promise_text_en": "Waive all outstanding farm loans for farmers.", "manifesto_pdf_page": 12},
        {"doc_id": "dmk_2026_edu_001", "party_id": "dmk", "target_year": 2026,
         "category": "Education", "scheme_name": "Free Laptops",
         "promise_text_en": "Provide free laptops to college students.", "manifesto_pdf_page": 5},
    ]
    return GraphRAG.build_from_sources(HashingEmbedder(dim=256), nodes=nodes, edges=edges, promises=promises)


def test_retrieve_finds_relevant_promise():
    rag = _sample_rag()
    hits = rag.retrieve("farm loan waiver", k=1)
    assert hits[0].id == "dmk_2026_agri_001"
    assert hits[0].kind == "promise"


def test_kg_node_hit_has_neighbors():
    rag = _sample_rag()
    hits = rag.retrieve("DMK party", k=3)
    node_hits = [h for h in hits if h.id == "party:dmk"]
    assert node_hits and any("won" in n for n in node_hits[0].neighbors)


def test_extractive_answer_is_cited():
    rag = _sample_rag()
    res = rag.answer("farm loan waiver", k=2)
    assert "[1]" in res.answer
    assert res.citations and "doc dmk_2026_agri_001" in res.citations[0]
    assert res.hits[0].id == "dmk_2026_agri_001"


def test_answer_empty_index():
    rag = GraphRAG(HashingEmbedder(dim=64))
    res = rag.answer("anything", k=3)
    assert "No relevant information" in res.answer
    assert res.citations == []


def test_pluggable_synthesizer_used():
    class Dummy(Synthesizer):
        def synthesize(self, question, hits):
            return f"SYNTH[{len(hits)}]"

    rag = _sample_rag()
    res = rag.answer("farmers", k=2, synthesizer=Dummy())
    assert res.answer.startswith("SYNTH[")
    assert res.citations  # citations still attached independently of synthesis


# ---------------------------------------------------------------------------
# persistence (index + adjacency)
# ---------------------------------------------------------------------------


def test_graphrag_save_load_roundtrip(tmp_path):
    rag = _sample_rag()
    p = tmp_path / "idx.json"
    rag.save(p)
    loaded = GraphRAG.load(p)
    assert loaded.embedder.name == rag.embedder.name
    # same retrieval result after reload
    assert loaded.retrieve("farm loan waiver", k=1)[0].id == "dmk_2026_agri_001"
    # adjacency preserved
    assert loaded.adjacency.get("party:dmk")


# ---------------------------------------------------------------------------
# integration: build from the real local data
# ---------------------------------------------------------------------------


def test_build_from_real_local_data():
    kg = REPO / "data" / "processed" / "knowledge_graph_slim.json"
    if not kg.exists():
        pytest.skip("KG slim not present")
    rag = build_local_index(out_path=None)  # builds from KG + manifesto files
    assert len(rag.index.records) > 1000
    # a manifesto-style question should surface a promise with a citation
    res = rag.answer("free bus travel for women", k=5)
    assert res.hits
    assert any(h.kind == "promise" for h in res.hits)
