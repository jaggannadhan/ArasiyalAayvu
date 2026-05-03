"""Historical backfill — fetch all TN stories from OmnesVident (Jan 30 - present)
and ingest them through the same NER pipeline.

Usage:
    python scrapers/news_backfill.py --probe     # count without ingesting
    python scrapers/news_backfill.py              # full backfill
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "naatunadappu")
LOCATION = os.environ.get("VERTEX_LOCATION", "us-central1")
OV_API_BASE = "https://omnesvident-api-naqkmfs2qa-uc.a.run.app"


async def fetch_all_stories(api_key: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
    """Fetch all TN stories in 1-day windows to stay under the 200/request cap."""
    all_stories: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    window = timedelta(days=1)
    current = start

    async with httpx.AsyncClient(timeout=30) as client:
        while current < end:
            w_end = min(current + window, end)
            resp = await client.get(
                f"{OV_API_BASE}/v1/stories",
                params={
                    "region": "IN-TN",
                    "start_date": current.strftime("%Y-%m-%dT00:00:00Z"),
                    "end_date": w_end.strftime("%Y-%m-%dT23:59:59Z"),
                    "limit": 200,
                },
                headers={"x-api-key": api_key},
            )
            resp.raise_for_status()
            stories = resp.json().get("stories", [])
            for s in stories:
                sid = s["dedup_group_id"]
                if sid not in seen_ids:
                    seen_ids.add(sid)
                    all_stories.append(s)

            count = len(stories)
            if count > 0:
                print(f"  {current.strftime('%b %d')} - {w_end.strftime('%b %d')}: {count} stories")

            current = w_end + timedelta(days=1)

    print(f"\n  Total fetched: {len(all_stories)} unique stories")
    return all_stories


async def main():
    parser = argparse.ArgumentParser(description="News Historical Backfill")
    parser.add_argument("--probe", action="store_true", help="Count only, don't ingest")
    parser.add_argument("--start", default="2026-01-30", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default="2026-04-26", help="End date (YYYY-MM-DD)")
    parser.add_argument("--model", default="gemini-2.5-flash", help="Gemini model for NER")
    parser.add_argument("--batch-size", type=int, default=5, help="NER batch size")
    args = parser.parse_args()

    from dotenv import load_dotenv
    env_path = ROOT / "web" / ".env.local"
    if env_path.exists():
        load_dotenv(env_path)

    api_key = os.environ.get("OMNES_VIDENT_API_KEY", "")
    if not api_key:
        print("ERROR: OMNES_VIDENT_API_KEY not set")
        sys.exit(1)

    start_dt = datetime.strptime(args.start, "%Y-%m-%d")
    end_dt = datetime.strptime(args.end, "%Y-%m-%d")

    print(f"=== News Historical Backfill ===")
    print(f"  Range: {args.start} → {args.end}")
    print()

    # 1. Fetch all stories
    print("[1/5] Fetching stories from OmnesVident...")
    stories = await fetch_all_stories(api_key, start_dt, end_dt)
    if not stories:
        print("  No stories found.")
        return

    # 2. Dedup against Firestore
    from google.cloud import firestore
    db = firestore.Client(project=PROJECT)

    print("[2/5] Deduplicating against Firestore...")
    existing_ids: set[str] = set()
    for doc in db.collection("news_articles").select(["ov_id"]).stream():
        existing_ids.add(doc.to_dict().get("ov_id", ""))
    new_stories = [s for s in stories if s["dedup_group_id"] not in existing_ids]
    print(f"  {len(stories)} fetched, {len(existing_ids)} existing, {len(new_stories)} new")

    if not new_stories:
        print("  All stories already ingested.")
        return

    if args.probe:
        print(f"\n  [PROBE] Would ingest {len(new_stories)} new stories. Exiting.")
        from collections import Counter
        months = Counter(s["timestamp"][:7] for s in new_stories)
        for m in sorted(months):
            print(f"    {m}: {months[m]}")
        return

    # 3. Fetch full article texts
    print("[3/5] Fetching full article texts...")
    from scrapers.news_ingestion import fetch_article_texts
    full_texts = await fetch_article_texts(new_stories, concurrency=10)

    # 4. Run NER in small sequential batches
    print("[4/5] Running Gemini NER...")
    from google import genai
    from scrapers.news_ingestion import (
        _load_reference_data, _build_ner_system_prompt, run_ner_batch,
    )

    ref_data = _load_reference_data()
    system_prompt = _build_ner_system_prompt(ref_data)
    client = genai.Client(vertexai=True, project=PROJECT, location=LOCATION)

    ner_by_id: dict[str, dict[str, Any]] = {}
    succeeded = 0
    failed = 0

    for i in range(0, len(new_stories), args.batch_size):
        batch = new_stories[i : i + args.batch_size]
        batch_num = i // args.batch_size + 1
        total_batches = (len(new_stories) + args.batch_size - 1) // args.batch_size
        print(f"  Batch {batch_num}/{total_batches} ({len(batch)} articles)...", end=" ", flush=True)

        results = await run_ner_batch(client, batch, full_texts, system_prompt, args.model)
        for r in results:
            ner_by_id[r["article_id"]] = r

        ok = len(results)
        fail = len(batch) - ok
        succeeded += ok
        failed += fail
        print(f"{'✓' if ok == len(batch) else f'{ok}✓ {fail}✗'}")

        # Pause between batches
        if i + args.batch_size < len(new_stories):
            await asyncio.sleep(3)

    print(f"  NER complete: {succeeded} succeeded, {failed} failed")

    # 5. Store in Firestore
    print("[5/5] Storing in Firestore...")
    fb = db.batch()
    batch_count = 0
    stored = 0

    for story in new_stories:
        sid = story["dedup_group_id"]
        ner = ner_by_id.get(sid, {})
        doc_data = {
            "ov_id": sid,
            "title": story["title"],
            "snippet": story.get("snippet", ""),
            "full_text": full_texts.get(sid, ""),
            "source_url": story.get("source_url", ""),
            "source_name": story.get("source_name", ""),
            "region_code": story.get("region_code", ""),
            "ov_category": story.get("category", ""),
            "latitude": story.get("latitude"),
            "longitude": story.get("longitude"),
            "is_breaking": story.get("is_breaking", False),
            "heat_score": story.get("heat_score", 0),
            "published_at": story.get("timestamp", ""),
            "ov_processed_at": story.get("processed_at", ""),
            "entities": ner.get("entities", []),
            "relations": ner.get("relations", []),
            "topics": ner.get("topics", []),
            "sdg_alignment": ner.get("sdg_alignment", []),
            "sentiment": ner.get("sentiment", 0.0),
            "relevance_to_tn": ner.get("relevance_to_tn", 0.0),
            "one_line_summary": ner.get("one_line_summary", ""),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
        doc_id = hashlib.md5(sid.encode()).hexdigest()[:16]
        fb.set(db.collection("news_articles").document(doc_id), doc_data)
        batch_count += 1
        stored += 1
        if batch_count >= 400:
            fb.commit()
            fb = db.batch()
            batch_count = 0

    if batch_count > 0:
        fb.commit()

    print(f"\n=== Done: {stored} articles ingested ({succeeded} with NER) ===")


if __name__ == "__main__":
    asyncio.run(main())
