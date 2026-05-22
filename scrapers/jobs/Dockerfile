FROM python:3.12-slim

# ffmpeg is required by the news_reader_refresh job for audio post-processing
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY jobs/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all scrapers (jobs + shared modules)
COPY . ./scrapers/

# Create data dirs (jobs write local JSON/audio before uploading)
RUN mkdir -p data/processed data/raw/niti_sdg data/processed/news_reader

ENTRYPOINT ["python"]
