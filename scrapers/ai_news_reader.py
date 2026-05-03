"""AI News Reader — per-article TTS + parallel Wav2Lip video generation.

Pipeline:
  1. Firestore articles → Gemini per-article scripts → edge-tts per-article audio
  2. Parallel Wav2Lip: one video clip per article (+ intro/outro)
  3. Upload individual clips + metadata to GCS

Each article gets its own video file — article sync is structurally guaranteed.

Usage:
    python scrapers/ai_news_reader.py              # full pipeline
    python scrapers/ai_news_reader.py --test        # sample articles (skip Gemini)
    python scrapers/ai_news_reader.py --no-upload   # skip GCS upload
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import concurrent.futures
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
WAV2LIP_DIR = ROOT.parent / "Wav2Lip"
ANCHOR_IMAGE = ROOT / "data" / "assets" / "news_anchor_female_cropped.png"
OUTPUT_DIR = ROOT / "data" / "processed" / "news_reader"

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "naatunadappu")
LOCATION = os.environ.get("VERTEX_LOCATION", "us-central1")
GCS_BUCKET = "naatunadappu-media"
GCS_PREFIX = "news-reader"

TAMIL_VOICE = "ta-IN-PallaviNeural"
ENGLISH_VOICE = "en-IN-NeerjaNeural"

INTRO_SCRIPT_TA = "வணக்கம்! அரசியல் ஆய்வு செய்திகளை வாசிப்பது உங்கள் தமிழ் செல்வி."
INTRO_SCRIPT_EN = "Good evening! This is TamilSelvi, reading the news for Arasiyal Aayvu."
OUTRO_SCRIPT_TA = "மீண்டும் சந்திப்போம், அரசியல் ஆய்வு செய்தி சேனலில் இருந்து தமிழ் செல்வி."
OUTRO_SCRIPT_EN = "See you again, this is TamilSelvi from Arasiyal Aayvu news channel."


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get_audio_duration(path: Path) -> float:
    result = subprocess.run(
        ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
         "-of", "csv=p=0", str(path)],
        capture_output=True, text=True,
    )
    return float(result.stdout.strip())


def _fade_out_audio(input_path: Path, output_path: Path, fade_ms: int = 300) -> Path:
    dur = _get_audio_duration(input_path)
    fade_sec = fade_ms / 1000.0
    fade_start = max(0, dur - fade_sec)
    subprocess.run(
        ["ffmpeg", "-y", "-i", str(input_path),
         "-af", f"afade=t=out:st={fade_start}:d={fade_sec}",
         "-c:a", "libmp3lame", "-b:a", "48k", str(output_path)],
        capture_output=True, text=True,
    )
    return output_path


# ── Step A: Generate per-article scripts via Gemini ────────────────────────────

async def generate_article_script(article: dict[str, Any], lang: str = "ta") -> str:
    from google import genai
    from google.genai import types

    client = genai.Client(vertexai=True, project=PROJECT, location=LOCATION)

    title = article.get("title", "")
    summary = article.get("one_line_summary", "") or article.get("snippet", "")

    if lang == "ta":
        prompt = f"""நீங்கள் ஒரு தமிழ் செய்தி வாசிப்பாளர். கீழே கொடுக்கப்பட்ட செய்தியை 1-2 வாக்கியங்களில் சுருக்கி, செய்தி வாசிப்பு பாணியில் எழுதவும்.

விதிகள்:
- முழுவதும் தமிழில் எழுதவும்
- இயற்கையாகவும் பேச்சு வழக்கிலும் எழுதவும்
- வாசிப்பு ஸ்கிரிப்ட் மட்டுமே — முன்னுரை அல்லது முடிவுரை வேண்டாம்

செய்தி:
தலைப்பு: {title}
சுருக்கம்: {summary}

ஸ்கிரிப்ட்:"""
    else:
        prompt = f"""You are a news anchor. Summarize this article in 1-2 sentences, as spoken language.

Rules:
- Write only the reading script — no intro or outro
- Write naturally, as spoken language

Article:
Title: {title}
Summary: {summary}

Script:"""

    config = types.GenerateContentConfig(temperature=0.7)
    resp = await client.aio.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
        config=config,
    )
    return resp.text.strip()


# ── Step B: Per-article TTS ────────────────────────────────────────────────────

async def generate_tts(text: str, output_path: Path, lang: str = "ta") -> Path:
    import edge_tts
    voice = TAMIL_VOICE if lang == "ta" else ENGLISH_VOICE
    raw_path = output_path.with_suffix(".raw.mp3")
    comm = edge_tts.Communicate(text, voice)
    await comm.save(str(raw_path))
    _fade_out_audio(raw_path, output_path, fade_ms=300)
    raw_path.unlink(missing_ok=True)
    return output_path


# ── Step C: Wav2Lip per clip ───────────────────────────────────────────────────

def _run_wav2lip(audio_path: Path, output_path: Path, label: str) -> Path:
    """Run Wav2Lip for a single audio clip.

    Each call gets its own temp directory to avoid collisions when running in parallel.
    Wav2Lip uses hardcoded 'temp/' for intermediate files — parallel runs clobber each other
    if they share the same cwd.
    """
    import shutil
    import tempfile

    # Create an isolated working directory with Wav2Lip code symlinked
    work_dir = Path(tempfile.mkdtemp(prefix=f"wav2lip_{label}_"))
    temp_dir = work_dir / "temp"
    temp_dir.mkdir()

    shim_dir = work_dir / "_shim"
    shim_dir.mkdir()
    (shim_dir / "sitecustomize.py").write_text(
        "import numpy as np\n"
        "for attr, val in [('float',float),('int',int),('complex',complex),('bool',bool),('object',object),('str',str)]:\n"
        "    if not hasattr(np, attr): setattr(np, attr, val)\n"
    )

    cmd = [
        sys.executable,
        str(WAV2LIP_DIR / "inference.py"),
        "--checkpoint_path", str(WAV2LIP_DIR / "checkpoints" / "wav2lip_gan.pth"),
        "--face", str(ANCHOR_IMAGE),
        "--audio", str(audio_path.resolve()),
        "--outfile", str(output_path.resolve()),
        "--pads", "0", "15", "5", "5",
    ]

    env = os.environ.copy()
    existing_pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(shim_dir) + os.pathsep + str(WAV2LIP_DIR) + (os.pathsep + existing_pp if existing_pp else "")

    t0 = __import__("time").time()
    result = subprocess.run(
        cmd, capture_output=True, text=True,
        cwd=str(work_dir),  # isolated cwd — each process gets its own temp/
        env=env, timeout=600,
    )
    elapsed = __import__("time").time() - t0

    # Cleanup isolated work dir
    shutil.rmtree(work_dir, ignore_errors=True)

    if result.returncode != 0:
        print(f"  [{label}] FAILED ({elapsed:.1f}s): {result.stderr[-300:]}")
        raise RuntimeError(f"Wav2Lip failed for {label}")

    size_kb = output_path.stat().st_size // 1024
    print(f"  [{label}] Done in {elapsed:.1f}s ({size_kb} KB)")
    return output_path


def run_wav2lip_parallel(tasks: list[tuple[Path, Path, str]], max_workers: int = 3) -> list[Path]:
    """Run multiple Wav2Lip jobs in parallel using a thread pool.

    Args:
        tasks: List of (audio_path, output_video_path, label) tuples
        max_workers: Max parallel Wav2Lip processes (3 is safe for most CPUs)

    Returns:
        List of output video paths in the same order as input tasks
    """
    print(f"  Launching {len(tasks)} Wav2Lip jobs (max {max_workers} parallel)...")

    results: dict[int, Path] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(_run_wav2lip, audio, video, label): idx
            for idx, (audio, video, label) in enumerate(tasks)
        }
        for future in concurrent.futures.as_completed(future_to_idx):
            idx = future_to_idx[future]
            results[idx] = future.result()

    return [results[i] for i in range(len(tasks))]


# ── Step D: Fetch articles from Firestore ──────────────────────────────────────

def fetch_top_articles(limit: int = 5) -> list[dict[str, Any]]:
    from google.cloud import firestore
    db = firestore.Client(project=PROJECT)

    docs = list(
        db.collection("news_articles")
        .order_by("published_at", direction=firestore.Query.DESCENDING)
        .limit(50)
        .stream()
    )

    articles = []
    for doc in docs:
        d = doc.to_dict()
        d["_doc_id"] = doc.id
        if d.get("relevance_to_tn", 0) >= 0.5 and d.get("entities"):
            articles.append(d)
        if len(articles) >= limit:
            break

    if len(articles) < limit:
        for doc in docs:
            d = doc.to_dict()
            if d not in articles:
                d["_doc_id"] = doc.id
                articles.append(d)
            if len(articles) >= limit:
                break

    return articles


# ── Step E: Upload to GCS ──────────────────────────────────────────────────────

def upload_to_gcs(local_path: Path, gcs_name: str) -> str:
    from google.cloud import storage
    client = storage.Client(project=PROJECT)
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(f"{GCS_PREFIX}/{gcs_name}")
    blob.cache_control = "no-cache, max-age=0"
    blob.upload_from_filename(str(local_path))
    blob.make_public()
    url = blob.public_url
    print(f"  Uploaded: {url}")
    return url


# ── Main ─────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="AI News Reader — Per-article Wav2Lip Pipeline")
    parser.add_argument("--test", action="store_true", help="Use sample scripts (skip Gemini)")
    parser.add_argument("--lang", default="ta", choices=["ta", "en"], help="Language")
    parser.add_argument("--no-upload", action="store_true", help="Skip GCS upload")
    parser.add_argument("--workers", type=int, default=3, help="Max parallel Wav2Lip jobs")
    args = parser.parse_args()

    from dotenv import load_dotenv
    env_path = ROOT / "web" / ".env.local"
    if env_path.exists():
        load_dotenv(env_path)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    run_dir = OUTPUT_DIR / f"run_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)

    print("=== AI News Reader Pipeline (Per-Article Wav2Lip) ===")
    print(f"  Language: {args.lang}")
    print(f"  Parallel workers: {args.workers}")
    print()

    # ── Step A: Get articles + generate scripts ──
    if args.test:
        print("[A] Using test articles...")
        articles = [
            {"title": "TN Assembly Election Results on May 4", "snippet": "Election Commission announces results date",
             "ov_category": "POLITICS", "sdg_alignment": ["SDG-16"], "source_name": "Test", "source_url": ""},
            {"title": "DMK Confident of Victory", "snippet": "MK Stalin expresses confidence",
             "ov_category": "POLITICS", "sdg_alignment": ["SDG-16"], "source_name": "Test", "source_url": ""},
            {"title": "TVK Vijay Asks Candidates to Stay Alert", "snippet": "Vijay urges vigilance at counting centres",
             "ov_category": "POLITICS", "sdg_alignment": ["SDG-16"], "source_name": "Test", "source_url": ""},
        ]
        scripts = [
            "தமிழ்நாடு சட்டமன்ற தேர்தல் முடிவுகள் மே 4ம் தேதி வெளியாகும் என தேர்தல் ஆணையம் அறிவித்துள்ளது." if args.lang == "ta" else "The Tamil Nadu Assembly election results will be announced on May 4th.",
            "திமுக தலைவர் முதலமைச்சர் மு.க.ஸ்டாலின் வெற்றி நம்பிக்கையுடன் இருப்பதாக தெரிவித்துள்ளார்." if args.lang == "ta" else "DMK leader Chief Minister MK Stalin has expressed confidence of victory.",
            "தமிழக வெற்றி கழகம் தலைவர் விஜய் வாக்கு எண்ணிக்கை மையங்களில் விழிப்புடன் இருக்குமாறு கேட்டுக்கொண்டுள்ளார்." if args.lang == "ta" else "TVK president Vijay has asked party candidates to stay alert at counting centres.",
        ]
    else:
        print("[A] Fetching top articles from Firestore...")
        articles = fetch_top_articles(5)
        print(f"  Found {len(articles)} articles")
        if not articles:
            print("  No articles found. Exiting.")
            return

        print("[A] Extracting per-article scripts...")
        scripts = []
        for i, a in enumerate(articles):
            # Use pre-computed consolidated script from ingestion if available
            script_key = "consolidated_script_ta" if args.lang == "ta" else "consolidated_script_en"
            pre_computed = a.get(script_key, "")
            if pre_computed:
                scripts.append(pre_computed)
                print(f"  Article {i+1} [consolidated]: {pre_computed[:80]}{'...' if len(pre_computed) > 80 else ''}")
            else:
                # Fallback: generate fresh via Gemini
                script = await generate_article_script(a, lang=args.lang)
                scripts.append(script)
                print(f"  Article {i+1} [generated]: {script[:80]}{'...' if len(script) > 80 else ''}")

    print()

    # ── Step B: Generate TTS audio for intro, articles, outro ──
    print("[B] Generating TTS audio...")

    intro_text = INTRO_SCRIPT_TA if args.lang == "ta" else INTRO_SCRIPT_EN
    outro_text = OUTRO_SCRIPT_TA if args.lang == "ta" else OUTRO_SCRIPT_EN

    intro_audio = run_dir / "intro.mp3"
    await generate_tts(intro_text, intro_audio, lang=args.lang)
    print(f"  Intro: {_get_audio_duration(intro_audio):.2f}s")

    article_audios: list[Path] = []
    for i, script in enumerate(scripts):
        audio_path = run_dir / f"article_{i:02d}.mp3"
        await generate_tts(script, audio_path, lang=args.lang)
        dur = _get_audio_duration(audio_path)
        print(f"  Article {i}: {dur:.2f}s — {articles[i].get('title', '')[:60]}")
        article_audios.append(audio_path)

    outro_audio = run_dir / "outro.mp3"
    await generate_tts(outro_text, outro_audio, lang=args.lang)
    print(f"  Outro: {_get_audio_duration(outro_audio):.2f}s")

    print()

    # ── Step C: Run Wav2Lip in parallel for all clips ──
    print("[C] Running Wav2Lip (parallel)...")

    wav2lip_tasks: list[tuple[Path, Path, str]] = []

    # Intro
    wav2lip_tasks.append((intro_audio, run_dir / "intro.mp4", "intro"))

    # Per-article
    for i, audio_path in enumerate(article_audios):
        wav2lip_tasks.append((audio_path, run_dir / f"article_{i:02d}.mp4", f"article_{i}"))

    # Outro
    wav2lip_tasks.append((outro_audio, run_dir / "outro.mp4", "outro"))

    import time
    t0 = time.time()
    video_paths = run_wav2lip_parallel(wav2lip_tasks, max_workers=args.workers)
    elapsed = time.time() - t0
    print(f"  All {len(video_paths)} clips done in {elapsed:.1f}s (wall-clock)")

    # video_paths order: [intro, article_0, article_1, ..., outro]
    intro_video = video_paths[0]
    article_videos = video_paths[1:-1]
    outro_video = video_paths[-1]

    print()

    # ── Step D: Upload to GCS ──
    if not args.no_upload:
        print("[D] Uploading video clips to GCS...")
        intro_url = upload_to_gcs(intro_video, "clips/intro.mp4")
        outro_url = upload_to_gcs(outro_video, "clips/outro.mp4")

        article_urls = []
        for i, vp in enumerate(article_videos):
            url = upload_to_gcs(vp, f"clips/article_{i:02d}.mp4")
            article_urls.append(url)
    else:
        print("[D] Skipping GCS upload (--no-upload)")
        intro_url = "/news-reader/clips/intro.mp4"
        outro_url = "/news-reader/clips/outro.mp4"
        article_urls = [f"/news-reader/clips/article_{i:02d}.mp4" for i in range(len(article_videos))]

    print()

    # ── Step E: Build and upload metadata ──
    print("[E] Building metadata...")
    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lang": args.lang,
        "intro": {
            "video_url": intro_url,
            "duration": round(_get_audio_duration(intro_audio), 3),
        },
        "outro": {
            "video_url": outro_url,
            "duration": round(_get_audio_duration(outro_audio), 3),
        },
        "articles": [],
    }

    for i, a in enumerate(articles):
        meta["articles"].append({
            "title": a.get("title", ""),
            "summary": a.get("one_line_summary", "") or a.get("snippet", ""),
            "category": a.get("ov_category", ""),
            "sdg_alignment": a.get("sdg_alignment", []),
            "source_name": a.get("source_name", ""),
            "source_url": a.get("source_url", ""),
            "video_url": article_urls[i],
            "duration": round(_get_audio_duration(article_audios[i]), 3),
        })

    meta_path = run_dir / "meta.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2))
    print(f"  Metadata saved: {meta_path}")

    if not args.no_upload:
        upload_to_gcs(meta_path, "latest_meta.json")

    total_dur = sum(_get_audio_duration(a) for a in [intro_audio] + article_audios + [outro_audio])
    print(f"\n=== Done! {len(articles)} articles, total: {total_dur:.1f}s, rendered in {elapsed:.1f}s ===")
    print(f"  Run directory: {run_dir}")


if __name__ == "__main__":
    asyncio.run(main())
