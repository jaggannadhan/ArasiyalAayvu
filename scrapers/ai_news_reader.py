"""AI News Reader — audio-only pipeline (EN + TA).

Pipeline (per language):
  1. Firestore articles → Gemini per-article scripts (or pre-computed) → edge-tts MP3
  2. Upload per-article audio + intro/outro to GCS
  3. Write latest_meta_<lang>.json with audio_urls

Both languages are generated in a single run by default.

Usage:
    python scrapers/ai_news_reader.py                # both languages, full pipeline
    python scrapers/ai_news_reader.py --lang ta      # tamil only
    python scrapers/ai_news_reader.py --test         # sample articles (skip Firestore + Gemini)
    python scrapers/ai_news_reader.py --no-upload    # skip GCS upload
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
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

TEST_ARTICLES: list[dict[str, Any]] = [
    {"title": "TN Assembly Election Results on May 4", "snippet": "Election Commission announces results date",
     "ov_category": "POLITICS", "sdg_alignment": ["SDG-16"], "source_name": "Test", "source_url": ""},
    {"title": "DMK Confident of Victory", "snippet": "MK Stalin expresses confidence",
     "ov_category": "POLITICS", "sdg_alignment": ["SDG-16"], "source_name": "Test", "source_url": ""},
    {"title": "TVK Vijay Asks Candidates to Stay Alert", "snippet": "Vijay urges vigilance at counting centres",
     "ov_category": "POLITICS", "sdg_alignment": ["SDG-16"], "source_name": "Test", "source_url": ""},
]

TEST_SCRIPTS: dict[str, list[str]] = {
    "ta": [
        "தமிழ்நாடு சட்டமன்ற தேர்தல் முடிவுகள் மே 4ம் தேதி வெளியாகும் என தேர்தல் ஆணையம் அறிவித்துள்ளது.",
        "திமுக தலைவர் முதலமைச்சர் மு.க.ஸ்டாலின் வெற்றி நம்பிக்கையுடன் இருப்பதாக தெரிவித்துள்ளார்.",
        "தமிழக வெற்றி கழகம் தலைவர் விஜய் வாக்கு எண்ணிக்கை மையங்களில் விழிப்புடன் இருக்குமாறு கேட்டுக்கொண்டுள்ளார்.",
    ],
    "en": [
        "The Tamil Nadu Assembly election results will be announced on May 4th.",
        "DMK leader Chief Minister MK Stalin has expressed confidence of victory.",
        "TVK president Vijay has asked party candidates to stay alert at counting centres.",
    ],
}


# ── Audio helpers ──────────────────────────────────────────────────────────────

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


# ── Script generation ─────────────────────────────────────────────────────────

async def generate_article_script(article: dict[str, Any], lang: str) -> str:
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


# ── TTS ───────────────────────────────────────────────────────────────────────

async def generate_tts(text: str, output_path: Path, lang: str) -> Path:
    import edge_tts
    voice = TAMIL_VOICE if lang == "ta" else ENGLISH_VOICE
    raw_path = output_path.with_suffix(".raw.mp3")
    comm = edge_tts.Communicate(text, voice)
    await comm.save(str(raw_path))
    _fade_out_audio(raw_path, output_path, fade_ms=300)
    raw_path.unlink(missing_ok=True)
    return output_path


# ── Firestore ─────────────────────────────────────────────────────────────────

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


# ── GCS ───────────────────────────────────────────────────────────────────────

def upload_to_gcs(local_path: Path, gcs_name: str) -> str:
    from google.cloud import storage
    client = storage.Client(project=PROJECT)
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(f"{GCS_PREFIX}/{gcs_name}")
    blob.cache_control = "no-cache, max-age=0"
    blob.upload_from_filename(str(local_path))
    blob.make_public()
    url = blob.public_url
    print(f"    uploaded: {url}")
    return url


# ── Per-language render ───────────────────────────────────────────────────────

async def render_language(
    lang: str,
    articles: list[dict[str, Any]],
    run_dir: Path,
    *,
    test_mode: bool,
    no_upload: bool,
) -> dict[str, Any]:
    print(f"\n=== Rendering [{lang}] ===")
    lang_dir = run_dir / lang
    lang_dir.mkdir(parents=True, exist_ok=True)

    # ── Scripts ──
    if test_mode:
        scripts = TEST_SCRIPTS[lang][: len(articles)]
        print(f"  [scripts] test mode — {len(scripts)} scripts")
    else:
        print(f"  [scripts] extracting per-article scripts...")
        scripts: list[str] = []
        script_key = "consolidated_script_ta" if lang == "ta" else "consolidated_script_en"
        for i, a in enumerate(articles):
            pre_computed = a.get(script_key, "")
            if pre_computed:
                scripts.append(pre_computed)
                preview = pre_computed[:80] + ("..." if len(pre_computed) > 80 else "")
                print(f"    {i + 1} [pre]: {preview}")
            else:
                script = await generate_article_script(a, lang=lang)
                scripts.append(script)
                preview = script[:80] + ("..." if len(script) > 80 else "")
                print(f"    {i + 1} [gen]: {preview}")

    # ── TTS ──
    print(f"  [tts] generating audio...")
    intro_text = INTRO_SCRIPT_TA if lang == "ta" else INTRO_SCRIPT_EN
    outro_text = OUTRO_SCRIPT_TA if lang == "ta" else OUTRO_SCRIPT_EN

    intro_audio = lang_dir / "intro.mp3"
    await generate_tts(intro_text, intro_audio, lang=lang)
    print(f"    intro: {_get_audio_duration(intro_audio):.2f}s")

    article_audios: list[Path] = []
    for i, script in enumerate(scripts):
        audio_path = lang_dir / f"article_{i:02d}.mp3"
        await generate_tts(script, audio_path, lang=lang)
        dur = _get_audio_duration(audio_path)
        title_preview = articles[i].get("title", "")[:60]
        print(f"    article {i}: {dur:.2f}s — {title_preview}")
        article_audios.append(audio_path)

    outro_audio = lang_dir / "outro.mp3"
    await generate_tts(outro_text, outro_audio, lang=lang)
    print(f"    outro: {_get_audio_duration(outro_audio):.2f}s")

    # ── Upload ──
    if not no_upload:
        print(f"  [upload] uploading audio clips...")
        intro_url = upload_to_gcs(intro_audio, f"clips/{lang}/intro.mp3")
        outro_url = upload_to_gcs(outro_audio, f"clips/{lang}/outro.mp3")
        article_urls = [
            upload_to_gcs(ap, f"clips/{lang}/article_{i:02d}.mp3")
            for i, ap in enumerate(article_audios)
        ]
    else:
        print(f"  [upload] skipped (--no-upload)")
        intro_url = f"/news-reader/clips/{lang}/intro.mp3"
        outro_url = f"/news-reader/clips/{lang}/outro.mp3"
        article_urls = [
            f"/news-reader/clips/{lang}/article_{i:02d}.mp3"
            for i in range(len(article_audios))
        ]

    # ── Metadata ──
    meta: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lang": lang,
        "intro": {
            "audio_url": intro_url,
            "duration": round(_get_audio_duration(intro_audio), 3),
        },
        "outro": {
            "audio_url": outro_url,
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
            "audio_url": article_urls[i],
            "duration": round(_get_audio_duration(article_audios[i]), 3),
        })

    meta_path = lang_dir / "meta.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2))

    if not no_upload:
        upload_to_gcs(meta_path, f"latest_meta_{lang}.json")
        print(f"  [meta] uploaded latest_meta_{lang}.json")
    else:
        print(f"  [meta] {meta_path}")

    total_dur = sum(_get_audio_duration(a) for a in [intro_audio] + article_audios + [outro_audio])
    print(f"  done — {len(articles)} articles, total {total_dur:.1f}s")
    return meta


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(description="AI News Reader — audio-only pipeline (EN + TA)")
    parser.add_argument("--test", action="store_true", help="Use sample articles (skip Firestore + Gemini)")
    parser.add_argument("--lang", choices=["ta", "en", "all"], default="all", help="Language (default: all)")
    parser.add_argument("--no-upload", action="store_true", help="Skip GCS upload")
    args = parser.parse_args()

    try:
        from dotenv import load_dotenv
        env_path = ROOT / "web" / ".env.local"
        if env_path.exists():
            load_dotenv(env_path)
    except ImportError:
        pass

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    run_dir = OUTPUT_DIR / f"run_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)

    print("=== AI News Reader Pipeline (audio only) ===")
    print(f"  Languages: {args.lang}")
    print(f"  Run dir:   {run_dir}")

    if args.test:
        articles = TEST_ARTICLES
        print(f"  Articles:  test mode, {len(articles)} sample articles")
    else:
        print("  Articles:  fetching from Firestore...")
        articles = fetch_top_articles(5)
        if not articles:
            print("  No articles found. Exiting.")
            return
        print(f"  Articles:  {len(articles)} fetched")

    langs = ["en", "ta"] if args.lang == "all" else [args.lang]
    for lang in langs:
        await render_language(
            lang, articles, run_dir,
            test_mode=args.test, no_upload=args.no_upload,
        )

    print(f"\n=== Done — {len(langs)} language(s), {len(articles)} articles ===")


if __name__ == "__main__":
    asyncio.run(main())
