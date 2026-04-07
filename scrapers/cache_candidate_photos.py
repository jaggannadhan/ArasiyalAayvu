"""
Cache Candidate Photos to GCS
==============================
Downloads all candidate photo_url images from Firestore collections and
uploads them to a GCS bucket under a stable path. Updates Firestore with
the new permanent URL so external ECI servers are no longer a dependency.

GCS path pattern:  candidate_photos/{year}/{slug}/{filename}
Public URL:        https://storage.googleapis.com/BUCKET/{path}

Supports any collection that stores photo_url at the top level OR inside
a candidates[] array.

Usage
-----
  # Dry-run (no writes)
  .venv/bin/python3 scrapers/cache_candidate_photos.py --dry-run

  # Cache 2026 candidates
  .venv/bin/python3 scrapers/cache_candidate_photos.py --collection candidates_2026 --year 2026

  # Resume (skip already-cached photos)
  .venv/bin/python3 scrapers/cache_candidate_photos.py --collection candidates_2026 --year 2026 --resume
"""
from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from google.cloud import firestore, storage
except ImportError:
    sys.exit("ERROR: google-cloud-storage not installed.\n"
             "Run: .venv/bin/python3 -m pip install google-cloud-storage")

BUCKET_NAME = "naatunadappu-media"
GCS_BASE    = f"https://storage.googleapis.com/{BUCKET_NAME}"
NOW_ISO     = datetime.now(timezone.utc).isoformat()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ArasiyalAayvuBot/1.0)",
    "Referer":    "https://affidavit.eci.gov.in/",
}

# ---------------------------------------------------------------------------
# GCS helpers
# ---------------------------------------------------------------------------
def _get_or_create_bucket(gcs: storage.Client) -> storage.Bucket:
    try:
        bucket = gcs.get_bucket(BUCKET_NAME)
        print(f"  Using existing bucket: gs://{BUCKET_NAME}")
        return bucket
    except Exception:
        print(f"  Creating bucket: gs://{BUCKET_NAME}")
        bucket = gcs.create_bucket(BUCKET_NAME, location="asia-south1")
        # Make all objects publicly readable by default
        policy = bucket.get_iam_policy(requested_policy_version=3)
        policy.bindings.append({
            "role": "roles/storage.objectViewer",
            "members": {"allUsers"},
        })
        bucket.set_iam_policy(policy)
        print("  Bucket created with public read access")
        return bucket


def _gcs_path(year: int, slug: str, url: str) -> str:
    filename = urlparse(url).path.rstrip("/").split("/")[-1]
    if not filename:
        filename = "photo.jpg"
    return f"candidate_photos/{year}/{slug}/{filename}"


def _upload(bucket: storage.Bucket, gcs_path: str, image_bytes: bytes,
            content_type: str = "image/jpeg") -> str:
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(image_bytes, content_type=content_type)
    blob.make_public()
    return f"{GCS_BASE}/{gcs_path}"


# ---------------------------------------------------------------------------
# Download + upload one photo
# ---------------------------------------------------------------------------
def _process_photo(
    bucket: storage.Bucket,
    slug: str,
    year: int,
    url: str,
    resume: bool,
    dry_run: bool,
) -> str | None:
    """
    Downloads `url`, uploads to GCS, returns the new public URL.
    Returns None if skipped or failed.
    """
    gcs_p = _gcs_path(year, slug, url)
    blob  = bucket.blob(gcs_p)

    if resume and blob.exists():
        return f"{GCS_BASE}/{gcs_p}"

    if dry_run:
        return f"{GCS_BASE}/{gcs_p}"  # pretend

    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "image/jpeg").split(";")[0]
        return _upload(bucket, gcs_p, resp.content, content_type)
    except Exception as e:
        print(f"    WARN: download failed for {url[:60]}: {e}")
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run(collection: str, year: int, dry_run: bool, resume: bool,
        project: str, workers: int) -> None:

    db  = firestore.Client(project=project)
    gcs = storage.Client(project=project)
    col = db.collection(collection)

    print(f"Collection: {collection}  year={year}  dry_run={dry_run}  resume={resume}")
    bucket = _get_or_create_bucket(gcs)

    # Stream all docs
    docs = {d.id: d.to_dict() for d in col.stream()}
    print(f"  {len(docs)} docs loaded\n")

    total = cached = skipped = errors = 0

    for slug, data in docs.items():
        candidates = data.get("candidates", [])

        # Build list of (idx, url) pairs that need caching
        to_cache = []
        for i, c in enumerate(candidates):
            url = c.get("photo_url", "")
            if not url:
                continue
            # Already points to our GCS bucket → already cached
            if BUCKET_NAME in url:
                skipped += 1
                continue
            to_cache.append((i, url))

        if not to_cache:
            continue

        total += len(to_cache)

        def _job(args):
            idx, url = args
            new_url = _process_photo(bucket, slug, year, url, resume, dry_run)
            return idx, new_url

        updated = False
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_job, a): a for a in to_cache}
            for fut in as_completed(futures):
                idx, new_url = fut.result()
                if new_url:
                    if not dry_run:
                        candidates[idx]["photo_url"] = new_url
                    cached += 1
                    updated = True
                else:
                    errors += 1

        if updated and not dry_run:
            col.document(slug).update({
                "candidates":       candidates,
                "_photos_cached_at": NOW_ISO,
            })
            print(f"  {slug:<35} {len(to_cache)} photos cached")

    print(f"\nDone — total={total}  cached={cached}  "
          f"already_cached={skipped}  errors={errors}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Download candidate photos → GCS → update Firestore"
    )
    ap.add_argument("--collection", default="candidates_2026",
                    help="Firestore collection name")
    ap.add_argument("--year",       type=int, default=2026,
                    help="Election year (used in GCS path)")
    ap.add_argument("--dry-run",    action="store_true",
                    help="No downloads or Firestore writes")
    ap.add_argument("--resume",     action="store_true",
                    help="Skip photos already uploaded to GCS")
    ap.add_argument("--workers",    type=int, default=8,
                    help="Parallel download threads (default 8)")
    ap.add_argument("--project",    default="naatunadappu")
    args = ap.parse_args()
    run(collection=args.collection, year=args.year,
        dry_run=args.dry_run, resume=args.resume,
        project=args.project, workers=args.workers)


if __name__ == "__main__":
    main()
