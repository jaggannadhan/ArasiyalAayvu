"""Cloud Run Job entrypoints that activate the agentic layer on a schedule.

Each job wires production stores (Firestore / GCS) into the tested module cores
and is safe-by-default (suggest mode). The cores accept injected dependencies so
they can be unit-tested offline without GCP.
"""
