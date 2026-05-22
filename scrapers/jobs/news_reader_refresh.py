"""Job: AI News Reader (TTS, EN + TA)
Schedule: twice daily, 06:00 and 18:00 IST

Pulls top articles from Firestore, generates per-article TTS audio (edge-tts) in
both English and Tamil, and uploads:
  - clips/<lang>/intro.mp3
  - clips/<lang>/article_{NN}.mp3
  - clips/<lang>/outro.mp3
  - latest_meta_<lang>.json
to gs://naatunadappu-media/news-reader/.

Run:
    .venv/bin/python3 scrapers/jobs/news_reader_refresh.py
    .venv/bin/python3 scrapers/jobs/news_reader_refresh.py --lang ta
    .venv/bin/python3 scrapers/jobs/news_reader_refresh.py --test --no-upload
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scrapers.ai_news_reader import main  # noqa: E402

if __name__ == "__main__":
    asyncio.run(main())
