"""Cloud Run Job: data-quality verification (agentic Module 9). Report-only.

    python -m agentic.jobs.verify_job

Verifies each schema-registered collection's values, writes findings to
`quality_findings`, and records a run (marked `partial` if any range errors are
found). Never modifies the source data.
"""

from __future__ import annotations

import argparse
from typing import Any, Callable, List, Optional


def _firestore_doc_loader():
    from google.cloud import firestore  # lazy
    import os

    db = firestore.Client(project=os.environ.get("GOOGLE_CLOUD_PROJECT", "naatunadappu"))

    def load(collection: str):
        return [d.to_dict() for d in db.collection(collection).stream()]

    return load


def _firestore_findings_writer(max_write: int = 200):
    from google.cloud import firestore  # lazy
    import os

    db = firestore.Client(project=os.environ.get("GOOGLE_CLOUD_PROJECT", "naatunadappu"))

    def write(run_id: str, report) -> None:
        db.collection("quality_findings").document(f"{run_id}_{report.collection}").set({
            "run_id": run_id,
            "collection": report.collection,
            "docs_checked": report.docs_checked,
            "summary": report.summary(),
            "findings": [
                {"check": f.check, "severity": f.severity, "message": f.message,
                 "doc_id": f.doc_id, "field": f.field}
                for f in report.findings[:max_write]
            ],
        })

    return write


def run_verify(
    *,
    collections: Optional[List[str]] = None,
    load_docs: Optional[Callable[[str], List[dict]]] = None,
    verifier: Any = None,
    run_store: Any = None,
    write_findings: Any = None,
):
    """Testable core. Production deps built lazily when not injected."""
    from agentic.provenance import RunContext

    if verifier is None:
        from agentic.quality import default_verifier

        verifier = default_verifier()
    if collections is None:
        from schemas import list_collections

        collections = list_collections()
    if load_docs is None:
        load_docs = _firestore_doc_loader()
    if run_store is None:
        from agentic.provenance import FirestoreRunStore

        run_store = FirestoreRunStore()

    reports = []
    with RunContext(tool="verify_job", trigger="scheduler",
                    args={"collections": len(collections)}, store=run_store) as run:
        for coll in collections:
            docs = load_docs(coll)
            report = verifier.run(coll, docs)
            reports.append(report)
            if report.findings and write_findings is not None:
                write_findings(run.run_id, report)
            run.record_write("quality_findings", len(report.findings))
            if report.errors:
                run.add_error(f"{coll}: {len(report.errors)} value errors")
    return reports


def main(argv: Optional[List[str]] = None) -> int:
    argparse.ArgumentParser(description="Data-quality verification").parse_args(argv)
    reports = run_verify(write_findings=_firestore_findings_writer())
    for r in reports:
        print(r.summary())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
