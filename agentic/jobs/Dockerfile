# Agentic-jobs image — built from the REPO ROOT context (so it includes the
# agentic package and the deterministic modules its tools resolve).
#
#   docker build -f agentic/jobs/Dockerfile -t gcr.io/naatunadappu/agentic-jobs .
#
FROM python:3.12-slim

WORKDIR /app

COPY agentic/jobs/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Control plane + the deterministic modules the tool registry resolves.
COPY agentic/ ./agentic/
COPY schemas/ ./schemas/
COPY loaders/ ./loaders/
COPY transformers/ ./transformers/

# Run as e.g.:  python -m agentic.jobs.watch_job [--act]
ENTRYPOINT ["python", "-m"]
