"""
Cache Candidate Photos to GCS
==============================
Downloads all candidate photo_url images from Firestore collections and
uploads them to a GCS bucket under a stable path. Updates Firestore with
the new permanent URL so external servers are no longer a dependency.

For ECI photos (suvidha.eci.gov.in) — which sit behind Akamai WAF and
require a browser session — Playwright is used to download via the live
cookie jar.  All other hosts are downloaded with plain requests.

GCS path pattern:  candidate_photos/{year}/{slug}/{filename}
Public URL:        https://storage.googleapis.com/BUCKET/{path}

Supports two schemas:
  candidates[]  — docs with a candidates array (e.g. candidates_2026)
  top-level     — docs where photo_url is a direct field (e.g. mla_profiles)

Usage
-----
  # Dry-run
  .venv/bin/python3 scrapers/cache_candidate_photos.py --dry-run

  # Cache 2026 candidates (ECI photos, uses Playwright)
  .venv/bin/python3 scrapers/cache_candidate_photos.py \\
      --collection candidates_2026 --year 2026

  # Cache MLA profile photos (assembly.tn.gov.in, plain HTTP)
  .venv/bin/python3 scrapers/cache_candidate_photos.py \\
      --collection mla_profiles --year 2021 --top-level

  # Resume (skip already-cached photos)
  .venv/bin/python3 scrapers/cache_candidate_photos.py \\
      --collection candidates_2026 --year 2026 --resume
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
    sys.exit("google-cloud-storage not installed. Run: "
             ".venv/bin/python3 -m pip install google-cloud-storage")

try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

BUCKET_NAME = "naatunadappu-media"
GCS_BASE    = f"https://storage.googleapis.com/{BUCKET_NAME}"
NOW_ISO     = datetime.now(timezone.utc).isoformat()

ECI_HOST    = "suvidha.eci.gov.in"
ECI_HOME    = "https://affidavit.eci.gov.in/"

PLAIN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 Chrome/124.0.0 Safari/537.36",
}


# ── GCS helpers ─────────────────────────────────────────────────────────────

def _get_bucket(gcs: storage.Client) -> storage.Bucket:
    try:
        return gcs.get_bucket(BUCKET_NAME)
    except Exception:
        print(f"Creating bucket gs://{BUCKET_NAME} …")
        bucket = gcs.create_bucket(BUCKET_NAME, location="asia-south1")
        policy = bucket.get_iam_policy(requested_policy_version=3)
        policy.bindings.append({"role": "roles/storage.objectViewer",
                                 "members": {"allUsers"}})
        bucket.set_iam_policy(policy)
        return bucket


def _gcs_path(year: int, slug: str, url: str) -> str:
    filename = urlparse(url).path.rstrip("/").split("/")[-1] or "photo.jpg"
    return f"candidate_photos/{year}/{slug}/{filename}"


def _upload(bucket: storage.Bucket, gcs_path: str,
            image_bytes: bytes, content_type: str = "image/jpeg") -> str:
    blob = bucket.blob(gcs_path)
    blob.cache_control = "public, max-age=31536000, immutable"
    blob.upload_from_string(image_bytes, content_type=content_type)
    blob.make_public()
    return f"{GCS_BASE}/{gcs_path}"


# ── Download helpers ─────────────────────────────────────────────────────────

def _is_image_content_type(ct: str) -> bool:
    return ct.split(";")[0].strip().startswith("image/")


def _download_plain(url: str) -> tuple[bytes, str] | None:
    """Download via plain requests (non-WAF hosts)."""
    try:
        resp = requests.get(url, headers=PLAIN_HEADERS, timeout=20)
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "image/jpeg")
        if not _is_image_content_type(ct):
            print(f"    SKIP: non-image content-type {ct!r} for {url[:60]}")
            return None
        return resp.content, ct.split(";")[0].strip()
    except Exception as e:
        print(f"    WARN: plain download failed: {e}")
        return None


# ── Per-photo processor ──────────────────────────────────────────────────────

def _process_photo(
    bucket: storage.Bucket,
    slug: str,
    year: int,
    url: str,
    resume: bool,
    dry_run: bool,
    pw_page=None,            # Playwright page — used only for ECI photos
) -> str | None:
    """Download url, upload to GCS, return public GCS URL. None = skip/fail."""
    gcs_p = _gcs_path(year, slug, url)
    blob  = bucket.blob(gcs_p)

    if resume and blob.exists():
        return f"{GCS_BASE}/{gcs_p}"

    if dry_run:
        return f"{GCS_BASE}/{gcs_p}"

    host = urlparse(url).hostname or ""

    if ECI_HOST in host:
        # ECI photos require a live browser session (Akamai WAF)
        if pw_page is None:
            print(f"    SKIP: Playwright not available for ECI URL {url[:60]}")
            return None
        try:
            resp = pw_page.request.get(url, timeout=20000)
            ct   = resp.headers.get("content-type", "image/jpeg")
            if not _is_image_content_type(ct):
                print(f"    SKIP: ECI returned non-image ({ct[:40]}) for {url[:60]}")
                return None
            return _upload(bucket, gcs_p, resp.body(), ct.split(";")[0].strip())
        except Exception as e:
            print(f"    WARN: ECI Playwright download failed: {e}")
            return None
    else:
        result = _download_plain(url)
        if result is None:
            return None
        image_bytes, ct = result
        return _upload(bucket, gcs_p, image_bytes, ct)


# ── Main run ─────────────────────────────────────────────────────────────────

def run(collection: str, year: int, dry_run: bool, resume: bool,
        top_level: bool, workers: int, project: str) -> None:

    db  = firestore.Client(project=project)
    gcs = storage.Client(project=project)
    col = db.collection(collection)

    print(f"Collection : {collection}")
    print(f"Year       : {year}")
    print(f"Schema     : {'top-level photo_url' if top_level else 'candidates[] array'}")
    print(f"Dry-run    : {dry_run}  Resume: {resume}\n")

    bucket = _get_bucket(gcs)
    docs   = {d.id: d.to_dict() for d in col.stream()}
    print(f"{len(docs)} docs loaded\n")

    total = cached = skipped = errors = 0

    # Determine if we need Playwright (any ECI URLs present)
    def _needs_playwright(data: dict, is_top: bool) -> bool:
        urls = ([data.get("photo_url", "")] if is_top
                else [c.get("photo_url", "") for c in data.get("candidates", [])])
        return any(ECI_HOST in (u or "") for u in urls)

    need_pw = any(_needs_playwright(d, top_level) for d in docs.values())
    if need_pw and not HAS_PLAYWRIGHT:
        sys.exit("ERROR: Playwright required for ECI photos but not installed.")

    # Open Playwright context only if needed
    _pw_instance = sync_playwright().start() if need_pw else None
    pw_ctx = _pw_instance          # Playwright object (has .chromium, etc.)
    pw_browser = pw_page = None
    if pw_ctx:
        pw_browser = pw_ctx.chromium.launch(headless=False)
        ctx = pw_browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        )
        pw_page = ctx.new_page()
        print("Warming up ECI portal for session cookies…")
        pw_page.goto(ECI_HOME, wait_until="commit", timeout=20000)
        pw_page.wait_for_timeout(2500)
        print("  ECI session ready\n")

    try:
        for slug, data in docs.items():
            if top_level:
                # Schema: photo_url is a direct field on the doc
                url = data.get("photo_url", "")
                if not url:
                    continue
                if BUCKET_NAME in url:
                    skipped += 1
                    continue
                total += 1
                new_url = _process_photo(bucket, slug, year, url,
                                         resume, dry_run, pw_page)
                if new_url:
                    cached += 1
                    if not dry_run:
                        col.document(slug).update({
                            "photo_url": new_url,
                            "_photo_cached_at": NOW_ISO,
                        })
                    print(f"  {slug:<40} ✓")
                else:
                    errors += 1
            else:
                # Schema: candidates[].photo_url
                candidates = data.get("candidates", [])
                to_cache = [
                    (i, c["photo_url"])
                    for i, c in enumerate(candidates)
                    if c.get("photo_url") and BUCKET_NAME not in c["photo_url"]
                ]
                if not to_cache:
                    continue

                total += len(to_cache)

                # ECI photos must be downloaded serially (shared Playwright page)
                eci_jobs  = [(i, u) for i, u in to_cache if ECI_HOST in u]
                rest_jobs = [(i, u) for i, u in to_cache if ECI_HOST not in u]

                updated_indices: dict[int, str] = {}

                # ECI — serial (Playwright page not thread-safe)
                for idx, url in eci_jobs:
                    new_url = _process_photo(bucket, slug, year, url,
                                             resume, dry_run, pw_page)
                    if new_url:
                        updated_indices[idx] = new_url
                        cached += 1
                    else:
                        errors += 1

                # Non-ECI — parallel
                def _job(args):
                    i, u = args
                    return i, _process_photo(bucket, slug, year, u,
                                             resume, dry_run, None)

                with ThreadPoolExecutor(max_workers=workers) as pool:
                    for idx, new_url in pool.map(_job, rest_jobs):
                        if new_url:
                            updated_indices[idx] = new_url
                            cached += 1
                        else:
                            errors += 1

                if updated_indices and not dry_run:
                    for idx, new_url in updated_indices.items():
                        candidates[idx]["photo_url"] = new_url
                    col.document(slug).update({
                        "candidates":         candidates,
                        "_photos_cached_at":  NOW_ISO,
                    })
                    print(f"  {slug:<40} {len(updated_indices)} photos cached")
                elif updated_indices:
                    skipped += len(updated_indices)

    finally:
        if pw_browser:
            pw_browser.close()
        if _pw_instance:
            _pw_instance.stop()

    print(f"\nDone — total={total}  cached={cached}  "
          f"already_on_gcs={skipped}  errors={errors}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Download candidate/MLA photos → GCS → update Firestore"
    )
    ap.add_argument("--collection", default="candidates_2026")
    ap.add_argument("--year",       type=int, default=2026)
    ap.add_argument("--top-level",  action="store_true",
                    help="photo_url is a top-level field (not inside candidates[])")
    ap.add_argument("--dry-run",    action="store_true")
    ap.add_argument("--resume",     action="store_true",
                    help="Skip photos already on GCS")
    ap.add_argument("--workers",    type=int, default=8,
                    help="Parallel threads for non-ECI photos (default 8)")
    ap.add_argument("--project",    default="naatunadappu")
    args = ap.parse_args()
    run(collection=args.collection, year=args.year,
        dry_run=args.dry_run, resume=args.resume,
        top_level=args.top_level, workers=args.workers,
        project=args.project)


if __name__ == "__main__":
    main()
