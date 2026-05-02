"""AI News Reader — generates a lip-synced news anchor video from today's top stories.

Pipeline: Firestore articles → Gemini per-article scripts → edge-tts segments → concat → Wav2Lip video
         + metadata JSON (articles + timestamps) uploaded alongside the video

Usage:
    # Generate from live Firestore articles:
    python scrapers/ai_news_reader.py

    # Test with a sample script (skip Gemini):
    python scrapers/ai_news_reader.py --test

    # Custom output path:
    python scrapers/ai_news_reader.py --output /tmp/news.mp4
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
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


# ── Step A: Generate per-article scripts via Gemini ────────────────────────────

async def generate_segments(articles: list[dict[str, Any]], lang: str = "ta") -> list[str]:
    """Generate individual news script segments — one per article + intro/outro."""
    from google import genai
    from google.genai import types

    client = genai.Client(vertexai=True, project=PROJECT, location=LOCATION)

    article_block = "\n\n".join(
        f"[{i+1}] {a.get('title', '')}\n{a.get('one_line_summary', '') or a.get('snippet', '')}"
        for i, a in enumerate(articles[:5])
    )

    if lang == "ta":
        prompt = f"""நீங்கள் ஒரு தமிழ் செய்தி வாசிப்பாளர். கீழே கொடுக்கப்பட்ட செய்திகளை செய்தி ஸ்கிரிப்டாக மாற்றுங்கள்.

விதிகள்:
- முழுவதும் தமிழில் எழுதவும்
- ஒவ்வொரு செய்தியையும் 1-2 வாக்கியங்களில் சுருக்கவும்
- இயற்கையாகவும் பேச்சு வழக்கிலும் எழுதவும்
- செய்திகளின் வரிசையை மாற்றக் கூடாது — கொடுக்கப்பட்ட வரிசையிலேயே எழுதவும்
- ஒவ்வொரு பகுதியையும் ||| என்ற குறியீட்டால் பிரிக்கவும்
- சரியாக {len(articles) + 2} பகுதிகள் வேண்டும்:
  பகுதி 1: அறிமுகம் — "வணக்கம்! அரசியல் ஆய்வு செய்திகளை வாசிப்பது உங்கள் தமிழ் செல்வி."
  பகுதி 2 முதல் {len(articles) + 1} வரை: ஒவ்வொரு செய்தியும் (வரிசையில்)
  கடைசி பகுதி: முடிவு — "மீண்டும் சந்திப்போம், அரசியல் ஆய்வு செய்தி சேனலில் இருந்து தமிழ் செல்வி."

செய்திகள்:
{article_block}

ஸ்கிரிப்ட் (||| பிரிப்பான்களுடன்):"""
    else:
        prompt = f"""You are a professional news anchor. Convert these news articles into a news script.

Rules:
- Summarize each story in 1-2 sentences
- Write naturally, as spoken language
- IMPORTANT: Keep articles in the exact order given — do NOT reorder them
- Separate each section with ||| delimiter
- There must be exactly {len(articles) + 2} sections:
  Section 1: Intro — "Good evening! This is TamilSelvi, reading the news for Arasiyal Aayvu."
  Sections 2 to {len(articles) + 1}: One section per article (in order)
  Last section: Outro — "See you again, this is TamilSelvi from Arasiyal Aayvu news channel."

Articles:
{article_block}

Script (with ||| separators):"""

    config = types.GenerateContentConfig(temperature=0.7)
    resp = await client.aio.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
        config=config,
    )

    raw = resp.text.strip()
    segments = [s.strip() for s in raw.split("|||") if s.strip()]
    return segments


# ── Step B: Per-segment TTS + concatenation with pacing gaps ───────────────────

INTRO_DURATION = 15.0   # intro padded to exactly 15 seconds
TRANSITION_GAP = 3.0    # silence between articles


def _get_audio_duration(path: Path) -> float:
    """Get audio duration in seconds via ffprobe."""
    result = subprocess.run(
        ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
         "-of", "csv=p=0", str(path)],
        capture_output=True, text=True,
    )
    return float(result.stdout.strip())


def _generate_silence(output_path: Path, duration: float) -> Path:
    """Generate a silent MP3 of the given duration."""
    subprocess.run(
        ["ffmpeg", "-y", "-f", "lavfi", "-i",
         f"anullsrc=r=24000:cl=mono", "-t", str(duration),
         "-c:a", "libmp3lame", "-b:a", "48k", str(output_path)],
        capture_output=True, text=True,
    )
    return output_path


def _pad_audio_to_duration(input_path: Path, target_duration: float, output_path: Path) -> Path:
    """Pad an audio file with silence to reach the target duration."""
    current = _get_audio_duration(input_path)
    if current >= target_duration:
        # Already long enough, just copy
        import shutil
        shutil.copy2(input_path, output_path)
        return output_path

    pad = target_duration - current
    # Use ffmpeg adelay/apad filter to append silence
    subprocess.run(
        ["ffmpeg", "-y", "-i", str(input_path),
         "-af", f"apad=pad_dur={pad}", "-c:a", "libmp3lame", "-b:a", "48k",
         str(output_path)],
        capture_output=True, text=True,
    )
    return output_path


async def text_to_speech_segments(
    segments: list[str], output_dir: Path, lang: str = "ta"
) -> tuple[Path, list[float]]:
    """TTS each segment, pad intro to 15s, insert 3s gaps between articles, concatenate.

    Audio layout:
        [intro padded to 15s] [3s gap] [article 1] [3s gap] [article 2] ... [3s gap] [outro]

    Returns:
        combined_path: Path to the concatenated MP3
        timestamps: Cumulative start times for each logical segment (including gaps)
    """
    import edge_tts

    voice = TAMIL_VOICE if lang == "ta" else ENGLISH_VOICE

    # Generate TTS for each segment
    raw_paths: list[Path] = []
    for i, text in enumerate(segments):
        seg_path = output_dir / f"_seg_{i:02d}.mp3"
        comm = edge_tts.Communicate(text, voice)
        await comm.save(str(seg_path))
        raw_paths.append(seg_path)
        print(f"  Segment {i}: {seg_path.name} ({seg_path.stat().st_size // 1024} KB)")

    raw_durations = [_get_audio_duration(p) for p in raw_paths]
    print(f"  Raw durations: {[round(d, 2) for d in raw_durations]}")

    # Pad intro (segment 0) to INTRO_DURATION
    padded_intro = output_dir / "_seg_00_padded.mp3"
    _pad_audio_to_duration(raw_paths[0], INTRO_DURATION, padded_intro)
    intro_actual = _get_audio_duration(padded_intro)
    print(f"  Intro padded: {raw_durations[0]:.2f}s → {intro_actual:.2f}s")

    # Generate silence file for gaps
    silence_path = output_dir / "_silence.mp3"
    _generate_silence(silence_path, TRANSITION_GAP)
    silence_dur = _get_audio_duration(silence_path)

    # Build concat list and compute timestamps
    # Layout: [intro_padded] [gap] [art1] [gap] [art2] ... [gap] [outro]
    concat_files: list[Path] = []
    timestamps: list[float] = []
    cumulative = 0.0

    # Intro
    timestamps.append(round(cumulative, 3))         # intro starts at 0
    concat_files.append(padded_intro)
    cumulative += intro_actual

    # Article segments (indices 1 to N-2) + outro (index N-1)
    for i in range(1, len(segments)):
        # Insert gap before each article and before outro
        concat_files.append(silence_path)
        cumulative += silence_dur

        timestamps.append(round(cumulative, 3))      # article/outro start
        concat_files.append(raw_paths[i])
        cumulative += raw_durations[i]

    print(f"  Total duration: {cumulative:.2f}s")
    print(f"  Timestamps: {timestamps}")

    # Concatenate all files
    concat_list = output_dir / "_concat_list.txt"
    concat_list.write_text(
        "\n".join(f"file '{p.name}'" for p in concat_files)
    )
    combined_path = output_dir / "_combined.mp3"
    subprocess.run(
        ["ffmpeg", "-y", "-f", "concat", "-safe", "0",
         "-i", str(concat_list), "-c", "copy", str(combined_path)],
        capture_output=True, text=True, cwd=str(output_dir),
    )

    print(f"  Combined audio: {combined_path} ({combined_path.stat().st_size // 1024} KB)")

    # Cleanup temp files
    for p in raw_paths:
        p.unlink(missing_ok=True)
    padded_intro.unlink(missing_ok=True)
    silence_path.unlink(missing_ok=True)
    concat_list.unlink(missing_ok=True)

    return combined_path, timestamps, raw_durations


async def text_to_speech(script: str, output_path: Path, lang: str = "ta") -> Path:
    """Convert script to audio using edge-tts (legacy single-script fallback)."""
    import edge_tts

    voice = TAMIL_VOICE if lang == "ta" else ENGLISH_VOICE
    comm = edge_tts.Communicate(script, voice)
    await comm.save(str(output_path))
    print(f"  Audio saved: {output_path} ({output_path.stat().st_size // 1024} KB)")
    return output_path


# ── Step C: Lip-sync animation via Wav2Lip ─────────────────────────────────────

def generate_video(
    source_image: Path,
    audio_path: Path,
    output_path: Path,
) -> Path:
    """Run Wav2Lip to generate lip-synced video.

    Wav2Lip is much faster than SadTalker on CPU (~45s vs ~105min for 30s audio).
    It only animates the lip region, keeping the rest of the face static.
    """
    # Numpy compat shim for Wav2Lip (written for numpy 1.x)
    shim_dir = output_path.parent / "_shim"
    shim_dir.mkdir(parents=True, exist_ok=True)
    (shim_dir / "sitecustomize.py").write_text(
        "import numpy as np\n"
        "for attr, val in [('float',float),('int',int),('complex',complex),('bool',bool),('object',object),('str',str)]:\n"
        "    if not hasattr(np, attr): setattr(np, attr, val)\n"
    )

    cmd = [
        sys.executable,
        str(WAV2LIP_DIR / "inference.py"),
        "--checkpoint_path", str(WAV2LIP_DIR / "checkpoints" / "wav2lip_gan.pth"),
        "--face", str(source_image),
        "--audio", str(audio_path),
        "--outfile", str(output_path),
        "--pads", "0", "15", "5", "5",
    ]

    print(f"  Running Wav2Lip...")

    env = os.environ.copy()
    existing_pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(shim_dir) + os.pathsep + str(WAV2LIP_DIR) + (os.pathsep + existing_pp if existing_pp else "")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(WAV2LIP_DIR),
        env=env,
        timeout=600,  # ~7 min with video input on CPU
    )

    if result.returncode != 0:
        print(f"  STDERR: {result.stderr[-500:]}")
        raise RuntimeError(f"Wav2Lip failed with exit code {result.returncode}")

    if not output_path.exists():
        raise RuntimeError(f"Output not found: {output_path}")

    print(f"  Video saved: {output_path} ({output_path.stat().st_size // 1024} KB)")
    return output_path


# ── Step D: Fetch articles from Firestore ────────────────────────────────────

def fetch_top_articles(limit: int = 5) -> list[dict[str, Any]]:
    """Fetch the most recent high-relevance articles from Firestore."""
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
        # Prefer articles with NER data and higher relevance
        if d.get("relevance_to_tn", 0) >= 0.5 and d.get("entities"):
            articles.append(d)
        if len(articles) >= limit:
            break

    # Fallback: if not enough high-relevance articles, take any
    if len(articles) < limit:
        for doc in docs:
            d = doc.to_dict()
            if d not in articles:
                d["_doc_id"] = doc.id
                articles.append(d)
            if len(articles) >= limit:
                break

    return articles


# ── Step E: Upload to GCS ────────────────────────────────────────────────────

def upload_to_gcs(local_path: Path, gcs_name: str) -> str:
    """Upload a file to GCS and return its public URL."""
    from google.cloud import storage
    client = storage.Client(project=PROJECT)
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(f"{GCS_PREFIX}/{gcs_name}")
    blob.upload_from_filename(str(local_path))
    blob.make_public()
    url = blob.public_url
    print(f"  Uploaded: {url}")
    return url


# ── Step F: Build metadata JSON ─────────────────────────────────────────────

def _match_segments_to_articles(
    articles: list[dict[str, Any]], segments: list[str],
) -> list[int]:
    """Match each article segment to its source article index using keyword overlap.

    Gemini may reorder articles in the script, so we can't assume segment i+1 = article i.
    Returns a list where result[seg_idx] = article index for that segment.
    """
    import re

    # Build keyword sets for each article from title + summary
    def _keywords(text: str) -> set[str]:
        # Extract words 3+ chars, lowercased — works for English; for Tamil, whole tokens
        return {w.lower() for w in re.findall(r'\w{3,}', text)}

    article_kws = []
    for a in articles:
        title = a.get("title", "")
        summary = a.get("one_line_summary", "") or a.get("snippet", "")
        article_kws.append(_keywords(f"{title} {summary}"))

    # Article segments are segments[1:-1] (skip intro and outro)
    art_segments = segments[1:-1]

    # Greedy matching: for each segment, find the best-matching unused article
    used: set[int] = set()
    seg_to_article: list[int] = []

    for seg_text in art_segments:
        seg_kw = _keywords(seg_text)
        best_idx = 0
        best_score = -1
        for ai, akw in enumerate(article_kws):
            if ai in used:
                continue
            score = len(seg_kw & akw)
            if score > best_score:
                best_score = score
                best_idx = ai
        seg_to_article.append(best_idx)
        used.add(best_idx)

    return seg_to_article


def build_metadata(
    articles: list[dict[str, Any]],
    segments: list[str],
    timestamps: list[float],
    raw_durations: list[float],
    lang: str,
) -> dict:
    """Build metadata JSON that maps each article to its video timestamp.

    Timestamps layout (with gaps baked in):
        [0]=intro_start, [1]=art0_start, [2]=art1_start, ..., [N]=outro_start

    Each article's end = start + raw_duration (excludes the gap that follows).
    The gap before each article is TRANSITION_GAP seconds.
    """
    seg_to_article = _match_segments_to_articles(articles, segments)

    print(f"  Segment→Article mapping: {seg_to_article}")

    article_meta = []
    for seg_i, art_i in enumerate(seg_to_article):
        a = articles[art_i]
        ts_idx = seg_i + 1  # +1 to skip intro timestamp
        start = timestamps[ts_idx] if ts_idx < len(timestamps) else timestamps[-1]
        # End = start + raw TTS duration of this article segment (index = seg_i + 1 in segments)
        raw_dur = raw_durations[seg_i + 1]  # +1 because raw_durations[0] is intro
        end = start + raw_dur
        article_meta.append({
            "title": a.get("title", ""),
            "summary": a.get("one_line_summary", "") or a.get("snippet", ""),
            "category": a.get("ov_category", ""),
            "sdg_alignment": a.get("sdg_alignment", []),
            "source_name": a.get("source_name", ""),
            "source_url": a.get("source_url", ""),
            "start": round(start, 3),
            "end": round(end, 3),
        })

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lang": lang,
        "segment_count": len(segments),
        "timestamps": timestamps,
        "intro_duration": INTRO_DURATION,
        "transition_gap": TRANSITION_GAP,
        "articles": article_meta,
    }


# ── Main ─────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="AI News Reader Video Generator")
    parser.add_argument("--test", action="store_true", help="Use sample script instead of Gemini")
    parser.add_argument("--lang", default="ta", choices=["ta", "en"], help="Language")
    parser.add_argument("--output", type=str, default=None, help="Output video path")
    parser.add_argument("--script-only", action="store_true", help="Generate script only, no video")
    parser.add_argument("--no-upload", action="store_true", help="Skip GCS upload")
    args = parser.parse_args()

    from dotenv import load_dotenv
    env_path = ROOT / "web" / ".env.local"
    if env_path.exists():
        load_dotenv(env_path)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    output_video = Path(args.output) if args.output else OUTPUT_DIR / f"news_{timestamp}.mp4"
    meta_path = OUTPUT_DIR / f"news_{timestamp}_meta.json"

    anchor_face = ANCHOR_IMAGE

    print("=== AI News Reader Pipeline ===")
    print(f"  Language: {args.lang}")
    print(f"  Anchor face: {anchor_face}")
    print()

    # Step A: Generate script segments
    if args.test:
        print("[A] Using test script segments...")
        if args.lang == "ta":
            segments = [
                "வணக்கம்! அரசியல் ஆய்வு செய்திகளை வாசிப்பது உங்கள் தமிழ் செல்வி.",
                "தமிழ்நாடு சட்டமன்ற தேர்தல் முடிவுகள் மே 4ம் தேதி வெளியாகும் என தேர்தல் ஆணையம் அறிவித்துள்ளது.",
                "திமுக தலைவர் முதலமைச்சர் மு.க.ஸ்டாலின் வெற்றி நம்பிக்கையுடன் இருப்பதாக தெரிவித்துள்ளார்.",
                "தமிழக வெற்றி கழகம் தலைவர் விஜய் வாக்கு எண்ணிக்கை மையங்களில் விழிப்புடன் இருக்குமாறு கேட்டுக்கொண்டுள்ளார்.",
                "மீண்டும் சந்திப்போம், அரசியல் ஆய்வு செய்தி சேனலில் இருந்து தமிழ் செல்வி.",
            ]
        else:
            segments = [
                "Good evening! This is TamilSelvi, reading the news for Arasiyal Aayvu.",
                "The Tamil Nadu Assembly election results will be announced on May 4th.",
                "DMK leader Chief Minister MK Stalin has expressed confidence of victory.",
                "TVK president Vijay has asked party candidates to stay alert at counting centres.",
                "See you again, this is TamilSelvi from Arasiyal Aayvu news channel.",
            ]
        # Build test articles to match segments
        articles = [
            {"title": "TN Assembly Election Results on May 4", "snippet": "Election Commission announces results date",
             "ov_category": "POLITICS", "sdg_alignment": ["SDG-16"], "source_name": "Test"},
            {"title": "DMK Confident of Victory", "snippet": "MK Stalin expresses confidence",
             "ov_category": "POLITICS", "sdg_alignment": ["SDG-16"], "source_name": "Test"},
            {"title": "TVK Vijay Asks Candidates to Stay Alert", "snippet": "Vijay urges vigilance at counting centres",
             "ov_category": "POLITICS", "sdg_alignment": ["SDG-16"], "source_name": "Test"},
        ]
    else:
        print("[A] Fetching top articles from Firestore...")
        articles = fetch_top_articles(5)
        print(f"  Found {len(articles)} articles")
        if not articles:
            print("  No articles found. Exiting.")
            return

        print("[A] Generating per-article script segments via Gemini...")
        segments = await generate_segments(articles, lang=args.lang)

    print(f"\n  Segments ({len(segments)}):")
    for i, seg in enumerate(segments):
        label = "INTRO" if i == 0 else ("OUTRO" if i == len(segments) - 1 else f"ART-{i}")
        print(f"    [{label}] {seg[:80]}{'...' if len(seg) > 80 else ''}")
    print()

    if args.script_only:
        print("  --script-only mode. Done.")
        return

    # Step B: Per-segment TTS + concatenation
    print("[B] Generating per-segment audio via edge-tts...")
    audio_path, timestamps, raw_durations = await text_to_speech_segments(segments, OUTPUT_DIR, lang=args.lang)

    # Step C: Build metadata
    print("[C] Building metadata...")
    metadata = build_metadata(articles, segments, timestamps, raw_durations, args.lang)
    meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2))
    print(f"  Metadata saved: {meta_path}")

    # Step D: Generate video
    print("[D] Generating lip-synced video via Wav2Lip...")
    generate_video(anchor_face, audio_path, output_video)

    # Cleanup combined audio
    audio_path.unlink(missing_ok=True)

    # Step E: Upload to GCS
    if not args.no_upload:
        print("[E] Uploading to GCS...")
        upload_to_gcs(output_video, "latest.mp4")
        upload_to_gcs(meta_path, "latest_meta.json")
    else:
        print("[E] Skipping GCS upload (--no-upload)")

    print(f"\n=== Done! Video: {output_video} ===")
    print(f"         Metadata: {meta_path}")


if __name__ == "__main__":
    asyncio.run(main())
