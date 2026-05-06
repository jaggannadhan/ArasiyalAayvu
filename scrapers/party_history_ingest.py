#!/usr/bin/env python3
"""Party History Ingestion via Gemini with Google Search Grounding.

Uses Gemini's built-in Google Search grounding to research comprehensive
party histories for Tamil Nadu's major political parties. The grounding
ensures Gemini cites real, verifiable sources rather than hallucinating.

For each party, runs multiple Gemini calls:
  1. OVERVIEW  — founding, ideology, leadership, splits (compact)
  2. CHAPTERS  — era-by-era deep narrative (one call per batch of eras)
  3. MEDIA     — YouTube videos, images, articles per chapter

Output: data/processed/party_history_{party_id}.json

Usage
-----
    # Probe a single party (no save):
    python scrapers/party_history_ingest.py --party dmk --probe

    # Ingest one party:
    python scrapers/party_history_ingest.py --party dmk

    # Ingest all 5 default parties:
    python scrapers/party_history_ingest.py

    # Custom model:
    python scrapers/party_history_ingest.py --model gemini-2.5-pro

Env
---
    GEMINI_API_KEY or GEMINI_API_KEYS (comma-separated for rotation)
    Falls back to Vertex AI if no API key set.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Config ───────────────────────────────────────────────────────────────────

MODEL = "gemini-2.5-flash"
GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

# Parties to ingest (order: most TN-relevant first)
DEFAULT_PARTIES = [
    {
        "party_id": "dmk",
        "name": "Dravida Munnetra Kazhagam (DMK)",
        "name_tamil": "திராவிட முன்னேற்றக் கழகம்",
        "context": "Dravidian party founded in 1949 by C.N. Annadurai as a breakaway from Periyar's Dravidar Kazhagam. One of the two dominant parties in Tamil Nadu since 1967. Currently ruling party (2021, 2026 under M.K. Stalin).",
    },
    {
        "party_id": "aiadmk",
        "name": "All India Anna Dravida Munnetra Kazhagam (AIADMK)",
        "name_tamil": "அனைத்திந்திய அண்ணா திராவிட முன்னேற்றக் கழகம்",
        "context": "Founded in 1972 by M.G. Ramachandran (MGR) after splitting from DMK. Named after C.N. Annadurai (Anna). Dominated TN politics under MGR and later Jayalalithaa. Currently in opposition after 2026 loss.",
    },
    {
        "party_id": "tvk",
        "name": "Tamilaga Vettri Kazhagam (TVK)",
        "name_tamil": "தமிழக வெற்றி கழகம்",
        "context": "Very new party founded on 2 February 2024 by actor Vijay (Joseph Vijay Chandrasekhar). Contested its first election in 2026 TN Assembly. Emerged as a significant force winning seats in its debut.",
    },
    {
        "party_id": "bjp",
        "name": "Bharatiya Janata Party (BJP) — Tamil Nadu",
        "name_tamil": "பாரதிய ஜனதா கட்சி",
        "context": "National party, TN state unit. Historically marginal in Dravidian-dominated TN. Allied with AIADMK in several elections. Focus on BJP's Tamil Nadu journey specifically — national history only as context.",
    },
    {
        "party_id": "inc",
        "name": "Indian National Congress (INC) — Tamil Nadu",
        "name_tamil": "இந்திய தேசிய காங்கிரஸ்",
        "context": "Ruled TN from independence until 1967 when Dravidian parties took over. Now a junior alliance partner (usually with DMK). Focus on INC's Tamil Nadu story — role in freedom movement, post-independence governance, decline, and current alliance politics.",
    },
]

# ── Existing election data for grounding ─────────────────────────────────────

def _load_election_context() -> str:
    """Load elections.json and alliances.json to give Gemini factual grounding."""
    lines = []
    elections_path = OUT_DIR / "elections.json"
    alliances_path = OUT_DIR / "alliances.json"

    if elections_path.exists():
        elections = json.loads(elections_path.read_text(encoding="utf-8"))
        lines.append("KNOWN TN ASSEMBLY ELECTION YEARS AND OUTCOMES:")
        for year, data in sorted(elections.items()):
            alliances_in_year = data.get("alliance_composition", [])
            winners = [a["alliance_name"] for a in alliances_in_year if a.get("outcome") == "Won"]
            lines.append(f"  {year}: Winner(s) = {', '.join(winners) if winners else 'unknown'}")
        lines.append("")

    if alliances_path.exists():
        alliances = json.loads(alliances_path.read_text(encoding="utf-8"))
        lines.append("KNOWN ALLIANCE COMPOSITIONS BY YEAR:")
        for year, alliance_list in sorted(alliances.items()):
            for a in alliance_list:
                members = ", ".join(a.get("member_parties", []))
                outcome = a.get("outcome", "?")
                lines.append(f"  {year} | {a['alliance_name']}: [{members}] → {outcome}")
        lines.append("")

    return "\n".join(lines) if lines else ""


# ── Gemini REST API Call ─────────────────────────────────────────────────────

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "naatunadappu")
LOCATION = os.environ.get("VERTEX_LOCATION", "us-central1")

# Singleton Vertex AI client
_vertex_client = None

def _get_vertex_client():
    global _vertex_client
    if _vertex_client is None:
        from google import genai
        _vertex_client = genai.Client(vertexai=True, project=PROJECT, location=LOCATION)
    return _vertex_client


def call_gemini(
    system_prompt: str,
    user_prompt: str,
    api_key: str,  # kept for compat; Vertex AI uses gcloud auth
    model: str = MODEL,
    temperature: float = 0.3,
    max_tokens: int = 32768,
    use_grounding: bool = True,
) -> str:
    """Call Gemini via Vertex AI with Google Search grounding.

    Uses the google.genai SDK with Vertex AI backend (project quota, not API key quota).
    """
    from google.genai import types

    client = _get_vertex_client()

    tools = []
    if use_grounding:
        tools = [types.Tool(google_search=types.GoogleSearch())]

    config = types.GenerateContentConfig(
        system_instruction=system_prompt,
        temperature=temperature,
        max_output_tokens=max_tokens,
        tools=tools if tools else None,
    )

    for attempt in range(5):
        try:
            print(f"    [vertex/{model}|attempt {attempt+1}/5] calling...", end=" ", flush=True)
            resp = client.models.generate_content(
                model=model,
                contents=user_prompt,
                config=config,
            )
            # Extract text
            if resp.text:
                # Log grounding info
                grounding = getattr(resp.candidates[0], "grounding_metadata", None) if resp.candidates else None
                if grounding and getattr(grounding, "search_entry_point", None):
                    print("OK (grounded)")
                else:
                    print("OK")
                return resp.text

            # Fallback: manually check parts
            text_parts = []
            if resp.candidates:
                for part in resp.candidates[0].content.parts:
                    if hasattr(part, "text") and part.text:
                        text_parts.append(part.text)
            if text_parts:
                print("OK (multi-part)")
                return "\n".join(text_parts)

            print("empty response — retrying in 15s")
            time.sleep(15)

        except Exception as exc:
            wait = 10 * (attempt + 1)
            msg = str(exc)[:200]
            print(f"error: {msg}")
            if attempt < 4:
                print(f"      retrying in {wait}s...")
                time.sleep(wait)

    raise RuntimeError(f"All 5 attempts failed for {model}")


def parse_json_response(text: str) -> dict | list:
    """Parse JSON from Gemini response, stripping markdown fences and extra text."""
    cleaned = text.strip()
    # Strip ```json ... ``` fences
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        start = 1 if lines[0].startswith("```") else 0
        end = len(lines) - 1 if lines[-1].strip() == "```" else len(lines)
        cleaned = "\n".join(lines[start:end])

    # Find the outermost JSON structure (object or array)
    for start_char, end_char in [("{", "}"), ("[", "]")]:
        idx = cleaned.find(start_char)
        if idx >= 0:
            depth = 0
            for i in range(idx, len(cleaned)):
                if cleaned[i] == start_char:
                    depth += 1
                elif cleaned[i] == end_char:
                    depth -= 1
                    if depth == 0:
                        return json.loads(cleaned[idx:i+1])

    return json.loads(cleaned)


# ── Prompts ──────────────────────────────────────────────────────────────────

def build_overview_prompt(party: dict, election_context: str) -> tuple[str, str]:
    """Phase 1: Get party overview — founding, ideology, leaders, splits."""
    system = f"""\
You are a political historian specializing in Tamil Nadu politics.
Use Google Search to verify every claim. Be FACTUALLY ACCURATE.

REFERENCE DATA (verified election outcomes — use as ground truth):
{election_context}
"""

    user = f"""\
Research the overview of this political party:

PARTY: {party['name']}
TAMIL NAME: {party['name_tamil']}
CONTEXT: {party['context']}

Return a JSON object with:
{{
  "party_id": "{party['party_id']}",
  "party_name": "{party['name']}",
  "party_name_tamil": "{party['name_tamil']}",
  "founded_year": <integer>,
  "founded_date": "<exact date or null>",
  "founded_place": "<city>",
  "parent_organization": "<party it split from, or null>",
  "founders": [
    {{"name": "<full name>", "role": "<founder/co-founder/first president>", "brief": "<1-2 sentence bio>"}}
  ],
  "original_ideology": "<2-3 sentences>",
  "current_ideology": "<2-3 sentences>",
  "original_motto": "<motto in original language + English>",
  "current_motto": "<current motto if different>",
  "symbol": "<current election symbol>",
  "symbol_history": "<brief history of symbol changes>",
  "headquarters": "<current HQ>",
  "key_splits_and_mergers": [
    {{"year": <int>, "event": "<what>", "parties_involved": ["<names>"], "reason": "<why>"}}
  ],
  "notable_leaders_through_history": [
    {{"name": "<name>", "tenure": "<years>", "role": "<role>", "legacy": "<1-2 sentences>"}}
  ],
  "current_leadership": [
    {{"name": "<name>", "role": "<role>", "since": "<year>"}}
  ],
  "era_timeline": [
    "<start_year>-<end_year>: <one-line description of this era — MAX 7 YEARS per era>"
  ],
  "overall_sources": ["<URL>"]
}}

IMPORTANT:
- The "era_timeline" should list ALL major eras chronologically (8-15 for older parties, 2-4 for newer).
  CRITICAL: Each era MUST span at most 5-7 years. Align eras with election cycles
  (e.g., 1991-1996, 1996-2001, 2001-2006). NEVER combine multiple election cycles into one era.
  A party that existed for 50 years should have at least 10 eras.
- Return ONLY the JSON object. No markdown fences, no commentary.
"""
    return system, user


def build_chapters_prompt(party: dict, overview: dict, era_batch: list[str], election_context: str) -> tuple[str, str]:
    """Phase 2: Get detailed chapters for a batch of eras."""
    eras_text = "\n".join(f"  {i+1}. {era}" for i, era in enumerate(era_batch))
    founders = ", ".join(f["name"] for f in overview.get("founders", []))

    system = f"""\
You are a political historian specializing in Tamil Nadu politics.
Use Google Search to verify every claim. Write ENGAGING narrative that
tells a STORY — the drama, the turning points, the people. Not dry summaries.
The audience is young Tamil voters who want to understand their state's
political history. Be balanced and objective.

REFERENCE DATA (verified election outcomes — use as ground truth):
{election_context}
"""

    user = f"""\
Write detailed chapter narratives for these eras of {party['name']}:

Party founded: {overview.get('founded_year')} by {founders}
Ideology: {overview.get('original_ideology', '')}

ERAS TO COVER:
{eras_text}

For each era, return a JSON array of chapter objects:
[
  {{
    "era": "<start_year>-<end_year>",
    "title": "<compelling chapter title>",
    "narrative": "<250-400 word engaging narrative. Tell the STORY — drama, turning points, people. Not a dry summary.>",
    "key_events": [
      {{"year": <int>, "month": <int or null>, "event": "<what>", "significance": "<why it mattered>"}}
    ],
    "achievements": ["<specific achievement with context>"],
    "leadership": ["<name — role — tenure>"],
    "election_results": [
      {{
        "year": <int>,
        "type": "<assembly/lok_sabha>",
        "seats_contested": <int or null>,
        "seats_won": <int>,
        "total_seats": <int>,
        "vote_share_pct": <float or null>,
        "alliance": "<alliance name or standalone>",
        "outcome": "<won_government/opposition/coalition_partner/did_not_contest>"
      }}
    ],
    "controversies": ["<notable controversy with factual description>"],
    "sources": ["<URL>"]
  }}
]

IMPORTANT:
- One chapter object per era listed above.
- Each narrative should be 250-400 words of ENGAGING storytelling.
- Include ALL elections in each era (assembly + Lok Sabha if relevant).
- Return ONLY the JSON array. No markdown fences, no commentary.
"""
    return system, user


def build_media_prompt(party_id: str, party_name: str, chapters: list[dict]) -> tuple[str, str]:
    """Phase 3: Media enrichment — YouTube, images, articles per chapter."""
    chapter_summaries = []
    for i, ch in enumerate(chapters):
        events = "; ".join(e["event"] for e in ch.get("key_events", [])[:5])
        chapter_summaries.append(f"  Chapter {i}: \"{ch['title']}\" ({ch['era']}) — {events}")

    chapters_text = "\n".join(chapter_summaries)

    system = """\
You are a media researcher. For each chapter of a political party's history,
find real, existing, publicly accessible media. Use Google Search to verify.

RULES:
- Only suggest media that ACTUALLY EXISTS. Verify URLs via search.
- YouTube URLs must be real. If you cannot verify a URL, provide a search_query instead.
- Prefer Tamil-language content where available.
- For each media item, explain WHY it's relevant.
"""

    user = f"""\
Find media for each chapter of {party_name}'s history:

{chapters_text}

Return a JSON array (one object per chapter):
[
  {{
    "chapter_index": <integer>,
    "youtube_videos": [
      {{
        "title": "<video title>",
        "url": "<YouTube URL or null>",
        "search_query": "<YouTube search query>",
        "description": "<why relevant>",
        "language": "<tamil/english/hindi>"
      }}
    ],
    "image_suggestions": [
      {{
        "description": "<what the image shows>",
        "search_query": "<search query to find it>",
        "relevance": "<why it matters>"
      }}
    ],
    "articles": [
      {{
        "title": "<article title>",
        "url": "<URL>",
        "source": "<publication>",
        "year": <int>,
        "relevance": "<why it matters>"
      }}
    ]
  }}
]

Return ONLY the JSON array. No markdown fences.
"""
    return system, user


# ── Main Pipeline ────────────────────────────────────────────────────────────

def ingest_party(
    party: dict,
    api_key: str,
    model: str,
    election_context: str,
    probe: bool = False,
    skip_media: bool = False,
    resume: bool = False,
) -> dict:
    """Run the full ingestion pipeline for one party."""
    party_id = party["party_id"]
    print(f"\n{'='*60}")
    print(f"  RESEARCHING: {party['name']}")
    print(f"{'='*60}\n")

    existing_path = OUT_DIR / f"party_history_{party_id}.json"
    overview = None

    # ── Resume: load existing overview ───────────────────────────────────────
    if resume and existing_path.exists():
        overview = json.loads(existing_path.read_text(encoding="utf-8"))
        era_timeline = overview.get("era_timeline", [])
        existing_eras = {ch["era"] for ch in overview.get("chapters", [])}
        print(f"  Resuming: loaded existing overview ({len(era_timeline)} eras, {len(existing_eras)} chapters done)")

    # ── Phase 1: Overview ────────────────────────────────────────────────────
    if overview is None:
        print("  Phase 1: Party overview (founding, ideology, leaders, era timeline)...")
        sys_prompt, user_prompt = build_overview_prompt(party, election_context)
        raw = call_gemini(sys_prompt, user_prompt, api_key, model=model)

        try:
            overview = parse_json_response(raw)
        except (json.JSONDecodeError, ValueError) as e:
            print(f"  ✗ Failed to parse overview: {e}")
            debug_path = OUT_DIR / f"party_history_{party_id}_overview_raw.txt"
            debug_path.write_text(raw, encoding="utf-8")
            print(f"    Raw saved to {debug_path}")
            return {}

    era_timeline = overview.get("era_timeline", [])
    print(f"  ✓ Overview done — {len(era_timeline)} eras identified:")
    for era in era_timeline:
        print(f"      • {era}")

    if probe:
        print(f"\n  Founded: {overview.get('founded_year')} in {overview.get('founded_place')}")
        print(f"  Founders: {', '.join(f['name'] for f in overview.get('founders', []))}")
        print(f"  Ideology: {overview.get('original_ideology', '')[:200]}")
        print(f"  Splits: {len(overview.get('key_splits_and_mergers', []))}")
        print(f"  Leaders: {len(overview.get('notable_leaders_through_history', []))}")
        print(f"  Sources: {overview.get('overall_sources', [])[:5]}")
        print("\n  (PROBE: skipping chapter narratives and media)")
        return overview

    # ── Phase 2: Chapter narratives (one era per call) ─────────────────────
    existing_chapters = overview.get("chapters", [])
    existing_eras = {ch["era"] for ch in existing_chapters}
    remaining_eras = [e for e in era_timeline if e.split(":")[0].strip() not in existing_eras]

    if remaining_eras:
        print(f"\n  Phase 2: Chapter narratives ({len(remaining_eras)} remaining of {len(era_timeline)} eras)...")
        all_chapters = list(existing_chapters)  # keep existing

        for i, era in enumerate(remaining_eras):
            era_label = era.split(":")[0].strip()
            print(f"\n    [{i+1}/{len(remaining_eras)}] {era_label}")
            ch_sys, ch_user = build_chapters_prompt(party, overview, [era], election_context)
            ch_raw = call_gemini(ch_sys, ch_user, api_key, model=model)

            try:
                chapters = parse_json_response(ch_raw)
                if isinstance(chapters, dict):
                    chapters = chapters.get("chapters", [chapters])
                print(f"    ✓ Got {len(chapters)} chapters")
                all_chapters.extend(chapters)
            except (json.JSONDecodeError, ValueError) as e:
                print(f"    ⚠ Parse failed for {era_label}: {e}")
                debug_path = OUT_DIR / f"party_history_{party_id}_ch{i}_raw.txt"
                debug_path.write_text(ch_raw, encoding="utf-8")
                print(f"      Raw saved to {debug_path}")

            # Save after each chapter (resume-safe)
            overview["chapters"] = all_chapters
            out_path = OUT_DIR / f"party_history_{party_id}.json"
            out_path.write_text(json.dumps(overview, ensure_ascii=False, indent=2), encoding="utf-8")

            # Cool down between calls
            if i < len(remaining_eras) - 1:
                time.sleep(3)

        overview["chapters"] = all_chapters
    else:
        all_chapters = existing_chapters
        print(f"\n  Phase 2: All {len(era_timeline)} chapters already present — skipping")
    print(f"\n  ✓ Total chapters: {len(all_chapters)}")

    # ── Phase 3: Media enrichment ────────────────────────────────────────────
    if all_chapters and not skip_media:
        print(f"\n  Phase 3: Media enrichment...")
        media_sys, media_user = build_media_prompt(party_id, party["name"], all_chapters)
        media_raw = call_gemini(media_sys, media_user, api_key, model=model)

        try:
            media_list = parse_json_response(media_raw)
            if isinstance(media_list, dict):
                media_list = media_list.get("chapter_media", [media_list])
            print(f"  ✓ Got media for {len(media_list)} chapters")

            media_by_idx = {m["chapter_index"]: m for m in media_list}
            for i, ch in enumerate(all_chapters):
                if i in media_by_idx:
                    m = media_by_idx[i]
                    ch["youtube_videos"] = m.get("youtube_videos", [])
                    ch["image_suggestions"] = m.get("image_suggestions", [])
                    ch["articles"] = m.get("articles", [])
        except (json.JSONDecodeError, ValueError) as e:
            print(f"  ⚠ Media parse failed: {e} — skipping")

    # ── Save ─────────────────────────────────────────────────────────────────
    out_path = OUT_DIR / f"party_history_{party_id}.json"
    out_path.write_text(
        json.dumps(overview, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\n  ✓ Saved to {out_path}")

    # Summary
    print(f"\n  Summary for {party['name']}:")
    print(f"    Founded: {overview.get('founded_year')} in {overview.get('founded_place')}")
    print(f"    Founders: {', '.join(f.get('name', '?') for f in overview.get('founders', []))}")
    print(f"    Chapters: {len(all_chapters)}")
    total_events = sum(len(ch.get("key_events", [])) for ch in all_chapters)
    total_elections = sum(len(ch.get("election_results", [])) for ch in all_chapters)
    total_videos = sum(len(ch.get("youtube_videos", [])) for ch in all_chapters)
    total_articles = sum(len(ch.get("articles", [])) for ch in all_chapters)
    print(f"    Key events: {total_events}")
    print(f"    Election records: {total_elections}")
    print(f"    YouTube videos: {total_videos}")
    print(f"    Articles: {total_articles}")

    return overview


def main():
    parser = argparse.ArgumentParser(description="Party History Ingestion via Gemini + Google Search")
    parser.add_argument("--party", type=str, help="Single party_id to ingest (default: all 5)")
    parser.add_argument("--model", type=str, default=None, help="Gemini model (default: gemini-2.5-flash)")
    parser.add_argument("--probe", action="store_true", help="Probe mode: overview only, no chapters/media")
    parser.add_argument("--no-media", action="store_true", help="Skip media enrichment phase")
    parser.add_argument("--resume", action="store_true", help="Resume: reuse existing overview, only run missing chapters")
    args = parser.parse_args()

    # API key
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        # Try loading from web/.env.local
        env_path = ROOT / "web" / ".env.local"
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("GEMINI_API_KEY="):
                    api_key = line.split("=", 1)[1].strip()
                    break
    if not api_key:
        sys.exit("ERROR: set GEMINI_API_KEY or add it to web/.env.local")

    model = args.model or MODEL

    # Load election context
    election_context = _load_election_context()
    if election_context:
        print(f"Loaded election reference data ({len(election_context)} chars)")

    # Determine which parties to ingest
    if args.party:
        parties = [p for p in DEFAULT_PARTIES if p["party_id"] == args.party]
        if not parties:
            valid = ", ".join(p["party_id"] for p in DEFAULT_PARTIES)
            sys.exit(f"Unknown party '{args.party}'. Valid: {valid}")
    else:
        parties = DEFAULT_PARTIES

    print(f"\nWill ingest {len(parties)} parties: {', '.join(p['party_id'] for p in parties)}")
    print(f"Model: {model}")
    if args.probe:
        print("(PROBE mode — overview only, no chapters or media)\n")

    results = {}
    for party in parties:
        try:
            history = ingest_party(
                party, api_key, model, election_context,
                probe=args.probe, skip_media=args.no_media,
                resume=args.resume,
            )
            results[party["party_id"]] = history
            if not args.probe and len(parties) > 1:
                print("\n  Cooling down 10s before next party...")
                time.sleep(10)
        except Exception as e:
            print(f"\n  ✗ FAILED for {party['party_id']}: {e}")
            import traceback
            traceback.print_exc()
            continue

    print(f"\n{'='*60}")
    print(f"  DONE — {len(results)}/{len(parties)} parties ingested")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
