"""Drift guard: the backend's vendored graphrag.py must stay byte-identical to
the canonical agentic/graphrag.py (so query-time embeddings match the index the
build_index_job produced). If you edit one, re-copy to the other.
"""

from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def test_vendored_graphrag_matches_source():
    canonical = (REPO / "agentic" / "graphrag.py").read_text(encoding="utf-8")
    vendored = (REPO / "web" / "backend_api" / "graphrag.py").read_text(encoding="utf-8")
    assert vendored == canonical, (
        "web/backend_api/graphrag.py is out of sync with agentic/graphrag.py — "
        "re-run: cp agentic/graphrag.py web/backend_api/graphrag.py"
    )
