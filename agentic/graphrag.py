"""GraphRAG — vector retrieval over the knowledge graph + manifesto text.

Makes the README's "GraphRAG" name real: the deterministic KG (Modules built
earlier) gains a retrieval surface. KG nodes and manifesto promises are embedded
into an in-memory vector index; a question is embedded and matched by cosine
similarity, then KG hits are expanded through graph neighbours for context. The
answer is **extractive and cited by default** (no LLM, no hallucination); a
Gemini synthesizer is an optional, pluggable upgrade.

Everything is pluggable so the offline path is fully testable:

    Embedder      HashingEmbedder (deterministic, offline)  | VertexEmbedder (prod)
    Synthesizer   None -> extractive cited passages         | GeminiSynthesizer (prod)
    VectorIndex   in-memory numpy, save/load JSON            (swap for Vertex Vector
                                                              Search later if needed)

Scale note: ~6.9K nodes + ~1.9K promises ≈ 9K vectors — a numpy dot product is
sub-10ms, so no ANN infrastructure is warranted.
"""

from __future__ import annotations

import hashlib
import json
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

_TOKEN = re.compile(r"[a-z0-9]+")

# Small English stopword list — these create spurious overlaps in a bag-of-words
# index without carrying topical meaning.
_STOPWORDS = {
    "the", "a", "an", "of", "to", "for", "and", "or", "in", "on", "at", "by",
    "is", "are", "be", "will", "with", "as", "all", "this", "that", "from",
    "we", "our", "their", "his", "her", "it", "its", "they", "i", "you",
}


def _tokens(text: str) -> List[str]:
    return [t for t in _TOKEN.findall((text or "").lower()) if t not in _STOPWORDS]


def _char_ngrams(token: str, n: int) -> List[str]:
    s = f"#{token}#"
    return [s[i : i + n] for i in range(len(s) - n + 1)] if len(s) >= n else [s]


# ---------------------------------------------------------------------------
# Embedders
# ---------------------------------------------------------------------------


class Embedder(ABC):
    name: str = "embedder"

    @abstractmethod
    def embed(self, texts: List[str]) -> np.ndarray: ...

    def embed_one(self, text: str) -> np.ndarray:
        return self.embed([text])[0]


class HashingEmbedder(Embedder):
    """Deterministic bag-of-tokens hashing embedder — no deps, no network.

    Uses md5 (not Python's salted hash) so vectors are reproducible across
    processes/runs. Hashes whole tokens **and** character 3/4-grams so
    morphological variants overlap (farmer~farmers, loan~loans, waive~waiver) —
    enough for keyword-style retrieval and fully unit-testable offline.
    """

    def __init__(self, dim: int = 256) -> None:
        self.dim = dim
        self.name = f"hashing-{dim}"

    def _bump(self, row: np.ndarray, feature: str, weight: float) -> None:
        h = int(hashlib.md5(feature.encode()).hexdigest(), 16)
        sign = 1.0 if (h >> 7) & 1 else -1.0
        row[h % self.dim] += sign * weight

    def embed(self, texts: List[str]) -> np.ndarray:
        out = np.zeros((len(texts), self.dim), dtype=np.float32)
        for i, text in enumerate(texts):
            for tok in _tokens(text):
                self._bump(out[i], f"w:{tok}", 1.0)            # whole word
                for n in (3, 4):                                 # subword robustness
                    for g in _char_ngrams(tok, n):
                        self._bump(out[i], f"g:{g}", 0.5)
        # L2-normalise (so dot product == cosine similarity)
        norms = np.linalg.norm(out, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        return out / norms


class VertexEmbedder(Embedder):
    """Production embedder backed by Vertex AI text embeddings (lazy import)."""

    def __init__(self, model: str = "text-embedding-004") -> None:
        self.model = model
        self.name = f"vertex:{model}"
        self._client = None

    def _model(self):
        if self._client is None:
            from vertexai.language_models import TextEmbeddingModel  # lazy

            self._client = TextEmbeddingModel.from_pretrained(self.model)
        return self._client

    def embed(self, texts: List[str]) -> np.ndarray:
        vecs = [e.values for e in self._model().get_embeddings(texts)]
        arr = np.array(vecs, dtype=np.float32)
        norms = np.linalg.norm(arr, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        return arr / norms


def get_embedder(name: str) -> Embedder:
    """Reconstruct an embedder from its stored ``name`` (so query matches index)."""
    if name.startswith("hashing-"):
        return HashingEmbedder(dim=int(name.split("-", 1)[1]))
    if name.startswith("vertex:"):
        return VertexEmbedder(model=name.split(":", 1)[1])
    raise ValueError(f"Unknown embedder name: {name!r}")


# ---------------------------------------------------------------------------
# Vector index
# ---------------------------------------------------------------------------


class VectorIndex:
    def __init__(self, model_name: str, dim: int) -> None:
        self.model_name = model_name
        self.dim = dim
        self.vectors = np.zeros((0, dim), dtype=np.float32)
        self.records: List[Dict[str, Any]] = []

    def add(self, vectors: np.ndarray, records: List[Dict[str, Any]]) -> None:
        if len(records) != vectors.shape[0]:
            raise ValueError("records / vectors length mismatch")
        self.vectors = np.vstack([self.vectors, vectors.astype(np.float32)]) if len(self.records) else vectors.astype(np.float32)
        self.records.extend(records)

    def search(self, query_vec: np.ndarray, k: int = 5) -> List[Tuple[float, Dict[str, Any]]]:
        if not self.records:
            return []
        q = query_vec.astype(np.float32)
        n = np.linalg.norm(q)
        if n:
            q = q / n
        scores = self.vectors @ q
        k = min(k, len(self.records))
        top = np.argpartition(-scores, k - 1)[:k]
        top = top[np.argsort(-scores[top])]
        return [(float(scores[i]), self.records[i]) for i in top]

    # -- persistence ---------------------------------------------------------

    def save(self, path: str | Path) -> None:
        Path(path).write_text(
            json.dumps(
                {
                    "model_name": self.model_name,
                    "dim": self.dim,
                    "records": self.records,
                    "vectors": self.vectors.tolist(),
                }
            ),
            encoding="utf-8",
        )

    @classmethod
    def load(cls, path: str | Path) -> "VectorIndex":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        idx = cls(data["model_name"], data["dim"])
        idx.records = data["records"]
        idx.vectors = np.array(data["vectors"], dtype=np.float32).reshape(-1, data["dim"])
        return idx


# ---------------------------------------------------------------------------
# Records / results
# ---------------------------------------------------------------------------


@dataclass
class Hit:
    id: str
    kind: str             # kg_node | promise
    score: float
    text: str
    citation: str
    meta: Dict[str, Any] = field(default_factory=dict)
    neighbors: List[str] = field(default_factory=list)


@dataclass
class AnswerResult:
    question: str
    answer: str
    citations: List[str]
    hits: List[Hit]


# ---------------------------------------------------------------------------
# Record builders
# ---------------------------------------------------------------------------


def _promise_record(p: Dict[str, Any]) -> Dict[str, Any]:
    text = " ".join(
        s for s in [p.get("promise_text_en"), p.get("scheme_name"), p.get("category")] if s
    )
    party = (p.get("party_id") or "?").upper()
    year = p.get("target_year")
    page = p.get("manifesto_pdf_page")
    cite = f'{party} {year} manifesto — {p.get("scheme_name") or p.get("category") or "promise"} (doc {p.get("doc_id")}, p.{page})'
    return {
        "id": p.get("doc_id"),
        "kind": "promise",
        "text": text,
        "citation": cite,
        "meta": {
            "party_id": p.get("party_id"),
            "target_year": year,
            "category": p.get("category"),
            "doc_id": p.get("doc_id"),
        },
    }


def _node_record(n: Dict[str, Any]) -> Dict[str, Any]:
    label = n.get("label") or n.get("id")
    text = f"{label} {n.get('type', '')} {n.get('slug', '')}".strip()
    return {
        "id": n.get("id"),
        "kind": "kg_node",
        "text": text,
        "citation": f'KG node {n.get("id")} ({label})',
        "meta": {"type": n.get("type"), "slug": n.get("slug"), "label": label},
    }


# ---------------------------------------------------------------------------
# GraphRAG
# ---------------------------------------------------------------------------


class GraphRAG:
    def __init__(
        self,
        embedder: Embedder,
        index: Optional[VectorIndex] = None,
        adjacency: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        self.embedder = embedder
        self.index = index or VectorIndex(embedder.name, getattr(embedder, "dim", 0))
        self.adjacency = adjacency or {}

    # -- build ---------------------------------------------------------------

    def build(self, records: List[Dict[str, Any]]) -> "GraphRAG":
        if not records:
            return self
        vecs = self.embedder.embed([r["text"] for r in records])
        if self.index.dim == 0:
            self.index = VectorIndex(self.embedder.name, vecs.shape[1])
        self.index.add(vecs, records)
        return self

    @classmethod
    def build_from_sources(
        cls,
        embedder: Embedder,
        *,
        nodes: Optional[List[Dict[str, Any]]] = None,
        edges: Optional[List[Dict[str, Any]]] = None,
        promises: Optional[List[Dict[str, Any]]] = None,
    ) -> "GraphRAG":
        nodes = nodes or []
        promises = promises or []
        records = [_node_record(n) for n in nodes] + [_promise_record(p) for p in promises]

        adjacency: Dict[str, List[str]] = {}
        if edges:
            label = {n.get("id"): (n.get("label") or n.get("id")) for n in nodes}
            for e in edges:
                s, t, verb = e.get("source"), e.get("target"), e.get("verb")
                adjacency.setdefault(s, []).append(f"{verb} → {label.get(t, t)}")

        rag = cls(embedder, adjacency=adjacency)
        return rag.build(records)

    # -- query ---------------------------------------------------------------

    @staticmethod
    def _dedup_key(rec: Dict[str, Any]):
        """A manifesto promise is indexed twice — as a KG node (id ``promise:<doc_id>``)
        and as a promise doc (id ``<doc_id>``). Map both to the same key so they
        collapse; everything else stays unique."""
        rid = rec.get("id")
        if rec.get("kind") == "promise":
            return ("promise", rid)
        if isinstance(rid, str) and rid.startswith("promise:"):
            return ("promise", rid.split("promise:", 1)[1])
        return ("node", rid)

    def _neighbors_for(self, rec: Dict[str, Any]) -> List[str]:
        """Adjacency for a record, also checking the ``promise:<id>`` KG node so a
        promise doc inherits its SDG neighbours even if the node wasn't retrieved."""
        ids = [rec.get("id")]
        if rec.get("kind") == "promise":
            ids.append(f"promise:{rec.get('id')}")
        out: List[str] = []
        for i in ids:
            for n in self.adjacency.get(i, []):
                if n not in out:
                    out.append(n)
        return out

    def retrieve(self, question: str, k: int = 5, *, expand: bool = True,
                 pool: Optional[int] = None) -> List[Hit]:
        qv = self.embedder.embed_one(question)
        # Over-fetch so dedup doesn't shrink the result below k.
        pool = pool if pool is not None else max(k * 4, 20)

        groups: Dict[Any, Dict[str, Any]] = {}
        for score, rec in self.index.search(qv, pool):
            key = self._dedup_key(rec)
            nbrs = self._neighbors_for(rec) if expand else []
            g = groups.get(key)
            if g is None:
                groups[key] = {"rec": rec, "score": score, "neighbors": list(nbrs)}
            else:
                if score > g["score"]:
                    g["score"] = score
                # prefer the promise doc as representative (carries the page cite)
                if rec.get("kind") == "promise" and g["rec"].get("kind") != "promise":
                    g["rec"] = rec
                for n in nbrs:
                    if n not in g["neighbors"]:
                        g["neighbors"].append(n)

        ranked = sorted(groups.values(), key=lambda g: -g["score"])[:k]
        return [
            Hit(
                id=g["rec"]["id"],
                kind=g["rec"]["kind"],
                score=round(g["score"], 4),
                text=g["rec"]["text"],
                citation=g["rec"]["citation"],
                meta=g["rec"].get("meta", {}),
                neighbors=g["neighbors"][:5],
            )
            for g in ranked
        ]

    def answer(
        self,
        question: str,
        k: int = 5,
        *,
        synthesizer: "Optional[Synthesizer]" = None,
    ) -> AnswerResult:
        hits = self.retrieve(question, k)
        citations = [h.citation for h in hits]
        if synthesizer is not None:
            text = synthesizer.synthesize(question, hits)
        else:
            text = _extractive_answer(question, hits)
        return AnswerResult(question=question, answer=text, citations=citations, hits=hits)


    # -- persistence (index + adjacency) ------------------------------------

    def save(self, path: str | Path) -> None:
        Path(path).write_text(
            json.dumps(
                {
                    "model_name": self.index.model_name,
                    "dim": self.index.dim,
                    "records": self.index.records,
                    "vectors": self.index.vectors.tolist(),
                    "adjacency": self.adjacency,
                }
            ),
            encoding="utf-8",
        )

    @classmethod
    def load(cls, path: str | Path, embedder: Optional[Embedder] = None) -> "GraphRAG":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        emb = embedder or get_embedder(data["model_name"])
        idx = VectorIndex(data["model_name"], data["dim"])
        idx.records = data["records"]
        idx.vectors = np.array(data["vectors"], dtype=np.float32).reshape(-1, data["dim"])
        return cls(emb, index=idx, adjacency=data.get("adjacency") or {})


def _extractive_answer(question: str, hits: List[Hit]) -> str:
    if not hits:
        return "No relevant information found in the knowledge base."
    lines = [f"Top matches for: {question}", ""]
    for i, h in enumerate(hits, 1):
        snippet = h.text if len(h.text) <= 240 else h.text[:237] + "..."
        lines.append(f"[{i}] {snippet}  (source: {h.citation})")
        if h.neighbors:
            lines.append(f"     related: {'; '.join(h.neighbors)}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Synthesizer (optional, production)
# ---------------------------------------------------------------------------


class Synthesizer(ABC):
    @abstractmethod
    def synthesize(self, question: str, hits: List[Hit]) -> str: ...


class GeminiSynthesizer(Synthesizer):
    """Optional Vertex Gemini answer synthesis over retrieved, cited context.

    Kept strictly grounded: the prompt instructs the model to answer only from
    the provided passages and to cite them, mirroring the manifesto-OCR
    "no hallucination" discipline.
    """

    def __init__(self, model: str = "gemini-2.5-pro") -> None:
        self.model = model

    def synthesize(self, question: str, hits: List[Hit]) -> str:
        from vertexai.generative_models import GenerativeModel  # lazy

        context = "\n".join(f"[{i}] {h.text} (source: {h.citation})" for i, h in enumerate(hits, 1))
        prompt = (
            "Answer the question using ONLY the numbered context passages below. "
            "Cite passages as [n]. If the answer is not in the context, say so.\n\n"
            f"Question: {question}\n\nContext:\n{context}\n\nAnswer:"
        )
        resp = GenerativeModel(self.model).generate_content(prompt)
        return resp.text


# ---------------------------------------------------------------------------
# Local build / load convenience (used by the CLI and the /api/ask endpoint)
# ---------------------------------------------------------------------------

DEFAULT_KG_PATH = "data/processed/knowledge_graph_slim.json"
DEFAULT_MANIFESTO_GLOB = "data/processed/manifesto_promises_2026_*.json"
DEFAULT_INDEX_PATH = "data/processed/graphrag_index.json"


def _load_local_sources(kg_path: str, manifesto_glob: str):
    import glob

    kg = json.loads(Path(kg_path).read_text(encoding="utf-8")) if Path(kg_path).exists() else {}
    nodes, edges = kg.get("nodes", []), kg.get("edges", [])
    promises: List[Dict[str, Any]] = []
    for f in sorted(glob.glob(manifesto_glob)):
        promises.extend(json.loads(Path(f).read_text(encoding="utf-8")))
    return nodes, edges, promises


def build_local_index(
    embedder: Optional[Embedder] = None,
    *,
    kg_path: str = DEFAULT_KG_PATH,
    manifesto_glob: str = DEFAULT_MANIFESTO_GLOB,
    out_path: Optional[str] = DEFAULT_INDEX_PATH,
) -> GraphRAG:
    embedder = embedder or HashingEmbedder()
    nodes, edges, promises = _load_local_sources(kg_path, manifesto_glob)
    rag = GraphRAG.build_from_sources(embedder, nodes=nodes, edges=edges, promises=promises)
    if out_path:
        rag.save(out_path)
    return rag


def answer_question(
    question: str,
    k: int = 5,
    *,
    index_path: str = DEFAULT_INDEX_PATH,
    use_gemini: bool = False,
) -> AnswerResult:
    rag = GraphRAG.load(index_path)
    synthesizer = GeminiSynthesizer() if use_gemini else None
    return rag.answer(question, k, synthesizer=synthesizer)
