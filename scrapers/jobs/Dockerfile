FROM python:3.12-slim

WORKDIR /app

COPY jobs/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all scrapers (jobs + shared modules: ts_utils, col_ingest, niti_sdg_ingest)
COPY . ./scrapers/

# Create data dirs (jobs write local JSON before uploading)
RUN mkdir -p data/processed data/raw/niti_sdg

ENTRYPOINT ["python"]
