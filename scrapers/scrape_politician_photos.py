"""Scrape profile photos for politicians without images via Google Images.

Uses Playwright (headless Chrome) to search Google Images and download
the first relevant portrait-like result. Only saves high-confidence
results. Skips ambiguous cases.

Usage:
    python scrapers/scrape_politician_photos.py --dry-run --limit 5
    python scrapers/scrape_politician_photos.py --limit 50
    python scrapers/scrape_politician_photos.py
"""
from __future__ import annotations

import argparse
import io
import re
import sys
import time
from typing import Any

import requests
from google.cloud import firestore, storage

PROJECT = "naatunadappu"
COLLECTION = "politician_profile"
BUCKET_NAME = "naatunadappu-media"

MIN_WIDTH = 80
MIN_HEIGHT = 100


def build_search_query(p: dict[str, Any]) -> str:
    name = p.get("name") or ""
    party = p.get("party") or ""
    constituency = p.get("constituency") or ""
    year = p.get("year") or ""

    # Shorten long party names
    for full, short in [
        ("All India Anna Dravida Munnetra Kazhagam", "AIADMK"),
        ("Dravida Munnetra Kazhagam", "DMK"),
        ("Indian National Congress", "INC"),
        ("Bharatiya Janata Party", "BJP"),
        ("Communist Party of India (Marxist)", "CPM"),
        ("Communist Party of India", "CPI"),
        ("Desiya Murpokku Dravida Kazhagam", "DMDK"),
        ("Pattali Makkal Katchi", "PMK"),
    ]:
        if full.lower() in party.lower():
            party = short
            break

    return f"{name} {party} MLA {constituency} Tamil Nadu {year}"


def search_google_images(page: Any, query: str) -> str | None:
    """Use an open Playwright page to search Google Images and return
    the first usable image URL."""
    try:
        url = f"https://www.google.com/search?q={requests.utils.quote(query)}&tbm=isch"
        page.goto(url, timeout=20000)
        page.wait_for_load_state("domcontentloaded", timeout=10000)
        time.sleep(2)  # let images load

        # Google Images embeds image URLs in data attributes
        html = page.content()

        # Pattern 1: full-size image URLs in the page source
        # Google embeds them as ["url",width,height] arrays
        img_urls = re.findall(
            r'https?://[^"\'<>\s]+\.(?:jpg|jpeg|png|webp)',
            html
        )

        # Filter out Google's own assets, favicons, etc.
        skip_patterns = [
            "google.com", "gstatic.com", "googleapis.com/images",
            "favicon", "logo", "icon", "emoji", "svg",
            "encrypted-tbn", "data:image",
        ]

        for img_url in img_urls:
            if any(s in img_url.lower() for s in skip_patterns):
                continue
            if len(img_url) > 500:  # likely a data URI or tracking URL
                continue
            return img_url

        return None
    except Exception as e:
        print(f"search error: {e}")
        return None


def download_and_validate(url: str) -> bytes | None:
    try:
        r = requests.get(url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
        })
        if r.status_code != 200:
            return None
        data = r.content
        if len(data) < 2000 or len(data) > 5_000_000:
            return None

        try:
            from PIL import Image
            img = Image.open(io.BytesIO(data))
            w, h = img.size
            if w < MIN_WIDTH or h < MIN_HEIGHT:
                return None
            if w / h > 2.5:
                return None
        except ImportError:
            pass
        except Exception:
            return None

        return data
    except Exception:
        return None


def upload_to_gcs(bucket: Any, data: bytes, doc_id: str) -> str:
    content_type = "image/jpeg"
    ext = ".jpg"
    if data[:4] == b"\x89PNG":
        content_type = "image/png"
        ext = ".png"
    elif data[:4] == b"RIFF":
        content_type = "image/webp"
        ext = ".webp"

    blob = bucket.blob(f"candidate_photos/scraped/{doc_id}{ext}")
    blob.cache_control = "public, max-age=31536000, immutable"
    blob.upload_from_string(data, content_type=content_type)
    blob.make_public()
    return blob.public_url


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=5.0)
    args = ap.parse_args()

    db = firestore.Client(project=PROJECT)
    storage_client = storage.Client(project=PROJECT)
    bucket = storage_client.bucket(BUCKET_NAME)

    # Collect no-photo politicians
    print("Loading profiles...")
    docs = list(db.collection(COLLECTION).stream())
    no_photo: list[dict[str, Any]] = []

    for d in docs:
        data = d.to_dict() or {}
        if data.get("photo_url"):
            continue
        tl = data.get("timeline", [])
        if not tl:
            continue
        latest = sorted(tl, key=lambda e: e.get("year") or 0, reverse=True)[0]
        no_photo.append({
            "doc_id": d.id,
            "name": data.get("canonical_name") or "",
            "party": latest.get("party") or data.get("current_party") or "",
            "constituency": latest.get("constituency") or data.get("current_constituency") or "",
            "year": latest.get("year"),
        })

    print(f"Politicians without photo: {len(no_photo)}")
    if args.limit:
        no_photo = no_photo[:args.limit]

    if args.dry_run:
        for i, p in enumerate(no_photo, 1):
            print(f"  [{i}] {p['name']:30s} → {build_search_query(p)}")
        return

    # Launch browser once, reuse for all searches
    from playwright.sync_api import sync_playwright
    found = 0
    skipped = 0

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()

        for i, p in enumerate(no_photo, 1):
            query = build_search_query(p)
            print(f"  [{i}/{len(no_photo)}] {p['name']:30s} ... ", end="", flush=True)

            img_url = search_google_images(page, query)
            if not img_url:
                print("no results")
                skipped += 1
                time.sleep(args.sleep)
                continue

            data = download_and_validate(img_url)
            if not data:
                print(f"invalid image")
                skipped += 1
                time.sleep(args.sleep)
                continue

            photo_url = upload_to_gcs(bucket, data, p["doc_id"])
            db.collection(COLLECTION).document(p["doc_id"]).update({"photo_url": photo_url})
            print(f"✓")
            found += 1
            time.sleep(args.sleep)

        browser.close()

    print(f"\nDone: {found} photos found, {skipped} skipped")


if __name__ == "__main__":
    main()
