"""News Ingestion + NER Pipeline via OmnesVident API + Gemini 2.5 Pro.

Polls OmnesVident for Tamil Nadu news across all categories, fetches full
article text via trafilatura, runs Gemini NER + relation extraction, and
stores results in Firestore (news_articles collection).

Usage
-----
    # Dry run — fetch + extract, print results, don't write to Firestore:
    python scrapers/news_ingestion.py --probe

    # Full ingestion — fetch, extract, store in Firestore:
    python scrapers/news_ingestion.py

    # Custom time window (hours back from now):
    python scrapers/news_ingestion.py --hours 4

    # Limit categories:
    python scrapers/news_ingestion.py --categories POLITICS BUSINESS

Env
---
    OMNES_VIDENT_API_KEY  — API key for OmnesVident public API
    GOOGLE_CLOUD_PROJECT  (default: naatunadappu)
    VERTEX_LOCATION       (default: us-central1)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "naatunadappu")
LOCATION = os.environ.get("VERTEX_LOCATION", "us-central1")

OV_API_BASE = "https://omnesvident-api-naqkmfs2qa-uc.a.run.app"
OV_CATEGORIES = ["POLITICS", "BUSINESS", "HEALTH", "SCIENCE_TECH", "WORLD", "ENTERTAINMENT", "SPORTS"]

# Reference data paths
CANDIDATE_INDEX_PATH = ROOT / "web" / "src" / "lib" / "candidate-search-index.json"
PROCESSED_DIR = ROOT / "data" / "processed"

# Gemini NER batch size — keep small to avoid timeouts with large system prompt
NER_BATCH_SIZE = 10


# ── Reference Data (loaded once, passed to Gemini as context) ────────────────

def _load_reference_data() -> dict[str, Any]:
    """Load compact reference lists for Gemini entity resolution.

    Only include politicians from major parties to keep the context window
    manageable (~500 entries instead of 4000+). Minor-party and independent
    candidates are unlikely to appear in news headlines.
    """
    MAJOR_PARTIES = {
        "All India Anna Dravida Munnetra Kazhagam",
        "Dravida Munnetra Kazhagam",
        "Bharatiya Janata Party",
        "Indian National Congress",
        "Pattali Makkal Katchi",
        "Tamilaga Vettri Kazhagam",
        "Naam Tamilar Katchi",
        "Communist Party of India",
        "Communist Party of India  (Marxist)",
        "Viduthalai Chiruthaigal Katchi",
        "Desiya Murpokku Dravida Kazhagam",
        "Marumalarchi Dravida Munnetra Kazhagam",
        "Amma Makkal Munnettra Kazagam",
        "Bahujan Samaj Party",
    }
    politicians: list[dict[str, str]] = []
    if CANDIDATE_INDEX_PATH.exists():
        with open(CANDIDATE_INDEX_PATH, encoding="utf-8") as f:
            raw = json.load(f)
        seen: set[str] = set()
        for entry in raw:
            name = entry["n"]
            if name not in seen and entry["p"] in MAJOR_PARTIES:
                seen.add(name)
                politicians.append({"name": name, "party": entry["p"], "constituency": entry["s"]})

    # Major parties (canonical IDs)
    parties = [
        "dmk", "aiadmk", "bjp", "inc", "pmk", "tvk", "ntk",
        "cpi", "cpim", "vck", "dmdk", "mdmk", "bsp",
    ]

    # Districts
    districts = [
        "ariyalur", "chengalpattu", "chennai", "coimbatore", "cuddalore",
        "dharmapuri", "dindigul", "erode", "kallakurichi", "kancheepuram",
        "kanyakumari", "karur", "krishnagiri", "madurai", "mayiladuthurai",
        "nagapattinam", "namakkal", "nilgiris", "perambalur", "pudukkottai",
        "ramanathapuram", "ranipet", "salem", "sivaganga", "tenkasi",
        "thanjavur", "theni", "thoothukudi", "tiruchirappalli", "tirunelveli",
        "tirupattur", "tirupur", "tiruvallur", "tiruvannamalai", "tiruvarur",
        "vellore", "viluppuram", "virudhunagar",
    ]

    return {
        "politicians": politicians,
        "parties": parties,
        "districts": districts,
    }


# ── OmnesVident API Client ──────────────────────────────────────────────────

async def fetch_ov_stories(
    api_key: str,
    categories: list[str],
    hours_back: int = 2,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Fetch TN stories from OmnesVident in a single call, then filter locally.

    A single request avoids burning through the community-tier rate limit
    (5 req/min) when polling multiple categories.
    """
    start_date = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
    cat_set = {c.upper() for c in categories}

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(
                f"{OV_API_BASE}/v1/stories",
                params={
                    "region": "IN-TN",
                    "start_date": start_date,
                    "limit": limit,
                },
                headers={"x-api-key": api_key},
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            print(f"  [ERROR] Failed to fetch stories: {exc}")
            return []

    # Filter to requested categories (local filter — no extra API calls)
    stories = [
        s for s in data.get("stories", [])
        if s.get("category", "").upper() in cat_set
    ]

    total = data.get("total", 0)
    print(f"  Fetched {total} stories from OV, {len(stories)} match categories: {', '.join(sorted(cat_set))}")
    return stories


# ── Article Full-Text Extraction ─────────────────────────────────────────────

async def fetch_article_texts(
    stories: list[dict[str, Any]],
    concurrency: int = 10,
) -> dict[str, str]:
    """Fetch full article text for each story URL using trafilatura."""
    import trafilatura

    sem = asyncio.Semaphore(concurrency)
    results: dict[str, str] = {}

    async def fetch_one(story: dict[str, Any]) -> None:
        url = story.get("source_url", "")
        sid = story["dedup_group_id"]
        if not url:
            return
        async with sem:
            try:
                # trafilatura is sync — run in thread pool
                loop = asyncio.get_event_loop()
                downloaded = await loop.run_in_executor(
                    None, trafilatura.fetch_url, url
                )
                if downloaded:
                    text = await loop.run_in_executor(
                        None,
                        lambda: trafilatura.extract(
                            downloaded,
                            include_comments=False,
                            include_tables=False,
                            no_fallback=True,
                        ),
                    )
                    if text and len(text) > 50:
                        results[sid] = text[:3000]  # Cap at 3K chars
            except Exception:
                pass  # Fall back to title+snippet

    await asyncio.gather(*(fetch_one(s) for s in stories))
    print(f"  Extracted full text for {len(results)}/{len(stories)} articles")
    return results


# ── Dedup ─────────────────────────────────────────────────────────────────────

import re as _re
import unicodedata as _ud

def _normalize_title(title: str) -> str:
    """Lowercase, strip punctuation/whitespace, collapse spaces for comparison."""
    t = _ud.normalize("NFKD", title.lower())
    t = _re.sub(r"[^\w\s]", "", t)       # strip punctuation
    t = _re.sub(r"\s+", " ", t).strip()   # collapse whitespace
    return t


def _title_is_duplicate(title: str, existing_titles: set[str], threshold: float = 0.90) -> bool:
    """Check if a normalized title overlaps >threshold with any existing title."""
    norm = _normalize_title(title)
    if not norm:
        return False
    # Exact match first (fast)
    if norm in existing_titles:
        return True
    # Token-overlap ratio for near-matches
    norm_tokens = set(norm.split())
    if not norm_tokens:
        return False
    for existing in existing_titles:
        ex_tokens = set(existing.split())
        if not ex_tokens:
            continue
        intersection = norm_tokens & ex_tokens
        # Jaccard-like overlap: |intersection| / |smaller set|
        overlap = len(intersection) / min(len(norm_tokens), len(ex_tokens))
        if overlap >= threshold:
            return True
    return False


def get_existing_story_ids_and_titles(db: Any) -> tuple[set[str], set[str]]:
    """Return (set of ov_ids, set of normalized titles) from Firestore."""
    try:
        docs = db.collection("news_articles").select(["ov_id", "title"]).stream()
        ids: set[str] = set()
        titles: set[str] = set()
        for d in docs:
            data = d.to_dict()
            ids.add(data.get("ov_id", ""))
            t = data.get("title", "")
            if t:
                titles.add(_normalize_title(t))
        return ids, titles
    except Exception:
        return set(), set()


def dedup_stories(stories: list[dict[str, Any]], existing_titles: set[str]) -> list[dict[str, Any]]:
    """Remove duplicate stories — both against existing Firestore articles and within the batch."""
    result: list[dict[str, Any]] = []
    batch_titles: set[str] = set(existing_titles)  # copy to also check within-batch

    for story in stories:
        title = story.get("title", "")
        if _title_is_duplicate(title, batch_titles):
            continue
        norm = _normalize_title(title)
        if norm:
            batch_titles.add(norm)
        result.append(story)

    removed = len(stories) - len(result)
    if removed > 0:
        print(f"  Removed {removed} title-duplicate stories")
    return result


# ── Gemini NER + Relation Extraction ─────────────────────────────────────────

def _build_ner_system_prompt(ref_data: dict[str, Any]) -> str:
    """Build the system prompt with reference entity lists."""
    # Compact politician list: "Name | party | constituency"
    pol_lines = []
    for p in ref_data["politicians"]:
        pol_lines.append(f"{p['name']} | {p['party']} | {p['constituency']}")
    pol_block = "\n".join(pol_lines)

    parties_block = ", ".join(ref_data["parties"])
    districts_block = ", ".join(ref_data["districts"])

    return f"""You are an NER + Relation Extraction engine for Tamil Nadu news.

Your job: For each news article, extract a structured subgraph of entities and relationships.

## REFERENCE ENTITIES (use canonical_id when you match one)

### Politicians (Name | Party | Constituency Slug)
{pol_block}

### Major Party IDs
{parties_block}

### Districts
{districts_block}

### SDG Goals
SDG-1 No Poverty, SDG-2 Zero Hunger, SDG-3 Good Health, SDG-4 Quality Education,
SDG-5 Gender Equality, SDG-6 Clean Water, SDG-7 Affordable Energy, SDG-8 Decent Work,
SDG-9 Industry & Innovation, SDG-10 Reduced Inequalities, SDG-11 Sustainable Cities,
SDG-12 Responsible Consumption, SDG-13 Climate Action, SDG-14 Life Below Water,
SDG-15 Life on Land, SDG-16 Peace & Justice, SDG-17 Partnerships

## ENTITY TYPES
- Person: politicians, officials, activists, business leaders, judges
- Party: political parties (use party_id as canonical_id if matched)
- Institution: govt departments, courts, commissions, PSUs (TANGEDCO, TNEB, etc.)
- Community: social groups (fishermen, farmers, salt pan workers, students, etc.)
- Place: districts, constituencies, cities, villages (use slug as canonical_id if matched)
- Policy: government schemes, programs, laws, bills
- Event: protests, disasters, elections, court verdicts, inaugurations
- Industry: sectors (textile, IT, agriculture, fishing, mining, etc.)
- SDG: SDG goals (use SDG-N as canonical_id)
- Topic: broad themes (corruption, caste, water_crisis, inflation, climate, etc.)
- Resource: physical resources (water, electricity, land, fuel, food, etc.)

## EDGE PREDICATES (use these verbs)
announces, opposes, supports, funds, regulates, protests, visits, leads,
affects, employs, occurs_in, located_in, relates_to, concerns, driven_by,
participates, implements, criticizes, demands, inaugurates, arrests,
allocates, benefits, threatens, disrupts, investigates, rules_on

## OUTPUT RULES
1. Extract ALL meaningful entities — not just politicians. Every person, place, community, institution, event, policy, topic, and resource mentioned.
2. For each entity, set canonical_id ONLY if it matches the reference lists above. Otherwise set canonical_id to null.
3. Generate a snake_case normalized `node_id` for every entity (e.g. "salt_pan_workers", "heat_wave_2026", "tn_election_2026").
4. Extract relationships as (subject_node_id, predicate, object_node_id) triples.
5. Assign SDG IDs where relevant — most socio-economic news touches at least one SDG.
6. Rate relevance_to_tn from 0.0 to 1.0 — how directly relevant is this to Tamil Nadu governance/politics/society.
7. Keep the output tight — no explanations, just structured JSON."""


NER_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "articles": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "article_id": {"type": "string"},
                    "entities": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "node_id": {"type": "string"},
                                "name": {"type": "string"},
                                "type": {
                                    "type": "string",
                                    "enum": [
                                        "Person", "Party", "Institution", "Community",
                                        "Place", "Policy", "Event", "Industry",
                                        "SDG", "Topic", "Resource",
                                    ],
                                },
                                "canonical_id": {"type": "string"},
                                "sdg_ids": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                            },
                            "required": ["node_id", "name", "type"],
                        },
                    },
                    "relations": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "subject": {"type": "string"},
                                "predicate": {"type": "string"},
                                "object": {"type": "string"},
                                "confidence": {"type": "number"},
                            },
                            "required": ["subject", "predicate", "object"],
                        },
                    },
                    "topics": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "sdg_alignment": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "sentiment": {"type": "number"},
                    "relevance_to_tn": {"type": "number"},
                    "one_line_summary": {"type": "string"},
                },
                "required": [
                    "article_id", "entities", "relations", "topics",
                    "sdg_alignment", "sentiment", "relevance_to_tn", "one_line_summary",
                ],
            },
        }
    },
    "required": ["articles"],
}


async def run_ner_batch(
    client: Any,
    articles: list[dict[str, Any]],
    full_texts: dict[str, str],
    system_prompt: str,
    model: str = "gemini-2.5-flash",
) -> list[dict[str, Any]]:
    """Run Gemini NER on a batch of articles. Returns extracted subgraphs."""
    from google.genai import types

    # Build the input
    article_inputs = []
    for a in articles:
        sid = a["dedup_group_id"]
        body = full_texts.get(sid, a.get("snippet", ""))
        article_inputs.append({
            "id": sid,
            "title": a["title"],
            "body": body,
            "category": a.get("category", ""),
            "source": a.get("source_name", ""),
            "timestamp": a.get("timestamp", ""),
            "region": a.get("region_code", ""),
        })

    user_prompt = (
        "Extract entities and relations from these articles. "
        "Return one result per article in the same order.\n\n"
        + json.dumps(article_inputs, separators=(",", ":"))
    )

    config = types.GenerateContentConfig(
        system_instruction=system_prompt,
        response_mime_type="application/json",
        response_schema=NER_RESPONSE_SCHEMA,
        temperature=0.1,
    )

    for attempt in range(3):
        try:
            resp = await client.aio.models.generate_content(
                model=model,
                contents=user_prompt,
                config=config,
            )
            result = json.loads(resp.text)
            return result.get("articles", [])
        except Exception as exc:
            print(f"    [WARN] Gemini NER attempt {attempt + 1} failed: {exc}")
            if attempt < 2:
                await asyncio.sleep(10 * (attempt + 1))
    return []


async def run_all_ner(
    articles: list[dict[str, Any]],
    full_texts: dict[str, str],
    ref_data: dict[str, Any],
    model: str = "gemini-2.5-flash",
    concurrency: int = 2,
) -> list[dict[str, Any]]:
    """Run NER on all articles in batches."""
    from google import genai

    client = genai.Client(vertexai=True, project=PROJECT, location=LOCATION)
    system_prompt = _build_ner_system_prompt(ref_data)

    # Batch articles
    batches = [
        articles[i : i + NER_BATCH_SIZE]
        for i in range(0, len(articles), NER_BATCH_SIZE)
    ]
    print(f"  Running NER on {len(articles)} articles in {len(batches)} batches (concurrency={concurrency})")

    sem = asyncio.Semaphore(concurrency)
    all_results: list[dict[str, Any]] = []

    async def process_batch(batch_idx: int, batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
        async with sem:
            print(f"    Batch {batch_idx + 1}/{len(batches)} ({len(batch)} articles)...")
            results = await run_ner_batch(client, batch, full_texts, system_prompt, model)
            print(f"    Batch {batch_idx + 1} → {len(results)} results")
            return results

    tasks = [process_batch(i, b) for i, b in enumerate(batches)]
    batch_results = await asyncio.gather(*tasks)
    for br in batch_results:
        all_results.extend(br)

    return all_results


# ── News Consolidation (KG-enriched scripts) ────────────────────────────────

def _load_news_kg() -> Any:
    """Load the News Knowledge Graph as a NetworkX graph."""
    try:
        sys.path.insert(0, str(ROOT / "web" / "backend_api"))
        from graph_query import load_news_graph
        g, _ = load_news_graph(project_id=PROJECT, force=True)
        return g
    except Exception as e:
        print(f"  Warning: Could not load News KG: {e}")
        return None


def _get_entity_context(
    g: Any,
    entities: list[dict],
    db: Any,
    current_title: str,
) -> str:
    """Query the KG for all edges involving this article's entities,
    then fetch titles of related articles. Returns a text block for Gemini."""
    if g is None or not entities:
        return ""

    # Import neighbors from graph_query
    sys.path.insert(0, str(ROOT / "web" / "backend_api"))
    from graph_query import neighbors as gq_neighbors

    # Collect entity node_ids and canonical_ids
    node_ids = set()
    for e in entities:
        node_ids.add(e.get("node_id", ""))
        if e.get("canonical_id"):
            node_ids.add(e["canonical_id"])
    node_ids.discard("")

    # Get KG edges for each entity
    kg_facts: list[str] = []
    related_article_ids: set[str] = set()

    for nid in node_ids:
        if nid not in g:
            continue
        edges = gq_neighbors(g, nid, direction="both", limit=30)
        for edge in edges:
            verb = edge.get("verb", "")
            if edge["direction"] == "out":
                target_node = edge.get("target_node", {})
                target_label = target_node.get("label", edge.get("target", ""))
                source_label = g.nodes[nid].get("label", nid) if nid in g else nid
                kg_facts.append(f"- {source_label} {verb} {target_label}")
                # Collect article IDs from the target node
                for aid in target_node.get("article_ids", []):
                    related_article_ids.add(aid)
            else:
                source_node = edge.get("source_node", {})
                source_label = source_node.get("label", edge.get("source", ""))
                target_label = g.nodes[nid].get("label", nid) if nid in g else nid
                kg_facts.append(f"- {source_label} {verb} {target_label}")
                for aid in source_node.get("article_ids", []):
                    related_article_ids.add(aid)

    # Fetch titles of related articles from Firestore (up to 15)
    related_articles: list[str] = []
    for aid in list(related_article_ids)[:15]:
        try:
            doc = db.collection("news_articles").document(aid).get()
            if doc.exists:
                d = doc.to_dict()
                title = d.get("title", "")
                if title and title != current_title:
                    date = d.get("published_at", "")[:10]
                    summary = d.get("one_line_summary", "")
                    related_articles.append(f"- [{date}] {title}: {summary}" if summary else f"- [{date}] {title}")
        except Exception:
            continue

    # Deduplicate KG facts
    kg_facts = list(dict.fromkeys(kg_facts))[:30]

    context_parts = []
    if kg_facts:
        context_parts.append("KNOWLEDGE GRAPH FACTS:\n" + "\n".join(kg_facts))
    if related_articles:
        context_parts.append("RELATED PAST ARTICLES:\n" + "\n".join(related_articles[:10]))

    return "\n\n".join(context_parts)


async def consolidate_article(
    client: Any,
    article_title: str,
    article_summary: str,
    kg_context: str,
    lang: str = "ta",
    model: str = "gemini-2.5-flash",
) -> dict:
    """Use Gemini to generate a contextual news script + identify relevant history.

    Returns:
        {
            "consolidated_script_ta": "...",
            "consolidated_script_en": "...",
            "contextual_history": [{"ref_title": ..., "relation": ..., "confidence": ...}]
        }
    """
    from google.genai import types

    if lang == "ta":
        prompt = f"""நீங்கள் ஒரு தமிழ் செய்தி வாசிப்பாளர். இன்றைய செய்தியையும், தொடர்புடைய கடந்த கால தகவல்களையும் பயன்படுத்தி ஒரு முழுமையான செய்தி ஸ்கிரிப்ட் எழுதுங்கள்.

இன்றைய செய்தி:
தலைப்பு: {article_title}
சுருக்கம்: {article_summary}

{kg_context if kg_context else "(தொடர்புடைய கடந்த கால தகவல்கள் இல்லை)"}

விதிகள்:
- முக்கியம்: முதலில் இன்றைய செய்தியின் உள்ளடக்கத்தை சொல்லவும், பிறகு தொடர்புடைய பின்னணியை இணைக்கவும்
- கடந்த கால தகவல்களை செய்தியின் சூழலில் தொடர்புடையதாக இருந்தால் மட்டுமே பயன்படுத்தவும்
- ஒரு entity பெயர் பொருந்துவது மட்டும் போதாது — காரண-விளைவு உறவு அல்லது கதை தொடர்ச்சி இருக்க வேண்டும்
- தொடர்பு இல்லையென்றால் இன்றைய செய்தியை மட்டும் சுருக்கி எழுதவும்
- 2-4 வாக்கியங்களில் எழுதவும்
- இயற்கையாகவும் பேச்சு வழக்கிலும் எழுதவும்

JSON வெளியீடு (வேறு எதுவும் வேண்டாம்):
{{
  "script_ta": "தமிழ் ஸ்கிரிப்ட்",
  "script_en": "English translation of the same script",
  "used_context": [
    {{"ref_title": "past article title used", "relation": "consequence|continuation|background", "confidence": 0.0-1.0}}
  ]
}}

குறிப்பு: used_context-ல் நீங்கள் உண்மையில் ஸ்கிரிப்டில் பயன்படுத்திய கடந்த கால செய்திகளை மட்டுமே சேர்க்கவும். பயன்படுத்தவில்லை என்றால் [] என்று விடவும்."""
    else:
        prompt = f"""You are a news anchor. Write a contextual news script using today's article and any relevant past information.

TODAY'S ARTICLE:
Title: {article_title}
Summary: {article_summary}

{kg_context if kg_context else "(No related past information available)"}

RULES:
- IMPORTANT: Start with today's article content FIRST, then weave in relevant background
- Include past events ONLY if they explain WHY or HOW today's news happened
- Just matching an entity name is NOT enough — there must be a causal, consequential, or narrative link
- If nothing from history is contextually relevant, just summarize today's article as-is
- Write 2-4 sentences
- Write naturally, as spoken language

JSON output (nothing else):
{{
  "script_ta": "Tamil script",
  "script_en": "English script",
  "used_context": [
    {{"ref_title": "past article title used", "relation": "consequence|continuation|background", "confidence": 0.0-1.0}}
  ]
}}

Note: In used_context, include ONLY past articles you actually referenced in the script. If none were used, return []."""

    config = types.GenerateContentConfig(
        temperature=0.4,
        response_mime_type="application/json",
    )

    try:
        resp = await client.aio.models.generate_content(
            model=model,
            contents=prompt,
            config=config,
        )
        result = json.loads(resp.text.strip())
        return {
            "consolidated_script_ta": result.get("script_ta", ""),
            "consolidated_script_en": result.get("script_en", ""),
            "contextual_history": result.get("used_context", []),
        }
    except Exception as e:
        print(f"    Consolidation failed: {e}")
        return {
            "consolidated_script_ta": "",
            "consolidated_script_en": "",
            "contextual_history": [],
        }


async def run_consolidation(
    stories: list[dict[str, Any]],
    ner_results: list[dict[str, Any]],
    db: Any,
    model: str = "gemini-2.5-flash",
) -> dict[str, dict]:
    """Run KG-enriched consolidation for all articles.

    Returns: {article_id: {"consolidated_script_ta": ..., "consolidated_script_en": ..., "contextual_history": [...]}}
    """
    from google import genai

    print("  Loading News Knowledge Graph...")
    g = _load_news_kg()
    if g:
        print(f"  KG loaded: {g.number_of_nodes()} nodes, {g.number_of_edges()} edges")
    else:
        print("  KG not available — scripts will be standalone")

    client = genai.Client(vertexai=True, project=PROJECT, location=LOCATION)

    # Index NER by article_id
    ner_by_id = {nr["article_id"]: nr for nr in ner_results}

    results: dict[str, dict] = {}
    for i, story in enumerate(stories):
        sid = story["dedup_group_id"]
        ner = ner_by_id.get(sid, {})
        title = story.get("title", "")
        summary = ner.get("one_line_summary", "") or story.get("snippet", "")
        entities = ner.get("entities", [])

        # Get KG context for this article's entities
        kg_context = _get_entity_context(g, entities, db, title)

        # Run Gemini consolidation
        result = await consolidate_article(
            client, title, summary, kg_context, lang="ta", model=model,
        )
        results[sid] = result

        ctx_count = len(result.get("contextual_history", []))
        ctx_label = f"+ {ctx_count} context refs" if ctx_count else "standalone"
        print(f"  Article {i + 1}: {title[:60]}... [{ctx_label}]")

    return results


# ── Firestore Storage ────────────────────────────────────────────────────────

def store_articles(
    db: Any,
    stories: list[dict[str, Any]],
    ner_results: list[dict[str, Any]],
    full_texts: dict[str, str],
    consolidation: dict[str, dict] | None = None,
) -> int:
    """Store articles + NER results in Firestore news_articles collection."""
    # Index NER results by article_id
    ner_by_id: dict[str, dict[str, Any]] = {}
    for nr in ner_results:
        ner_by_id[nr["article_id"]] = nr

    stored = 0
    batch = db.batch()
    batch_count = 0

    for story in stories:
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
            # NER results
            "entities": ner.get("entities", []),
            "relations": ner.get("relations", []),
            "topics": ner.get("topics", []),
            "sdg_alignment": ner.get("sdg_alignment", []),
            "sentiment": ner.get("sentiment", 0.0),
            "relevance_to_tn": ner.get("relevance_to_tn", 0.0),
            "one_line_summary": ner.get("one_line_summary", ""),
            # Consolidation (KG-enriched scripts for news reader)
            **(consolidation.get(sid, {}) if consolidation else {}),
            # Metadata
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

        # Use title hash as doc ID — prevents exact-title duplicates across different ov_ids
        doc_id = hashlib.md5(_normalize_title(story["title"]).encode()).hexdigest()[:16]
        ref = db.collection("news_articles").document(doc_id)
        batch.set(ref, doc_data)
        batch_count += 1
        stored += 1

        # Firestore batch limit is 500
        if batch_count >= 400:
            batch.commit()
            batch = db.batch()
            batch_count = 0

    if batch_count > 0:
        batch.commit()

    print(f"  Stored {stored} articles in Firestore")
    return stored


# ── Retry empty NER ───────────────────────────────────────────���──────────────

async def _retry_empty_ner(model: str, batch_size: int) -> None:
    """Re-run NER on Firestore articles that have zero entities.

    Processes sequentially (1 batch at a time, concurrency=1) to avoid
    Vertex AI "Server disconnected" timeouts.
    """
    from google.cloud import firestore as fs
    from google import genai

    db = fs.Client(project=PROJECT)
    print("=== Retry NER on empty articles ===")

    # Fetch articles with no entities
    docs = list(db.collection("news_articles").stream())
    empty = []
    for doc in docs:
        d = doc.to_dict()
        if not d.get("entities"):
            empty.append((doc.id, d))

    print(f"  Found {len(empty)} articles with empty entities (of {len(docs)} total)")
    if not empty:
        print("  Nothing to retry.")
        return

    ref_data = _load_reference_data()
    system_prompt = _build_ner_system_prompt(ref_data)
    client = genai.Client(vertexai=True, project=PROJECT, location=LOCATION)

    succeeded = 0
    failed = 0

    # Process in small batches, sequentially
    for i in range(0, len(empty), batch_size):
        batch = empty[i : i + batch_size]
        print(f"\n  Batch {i // batch_size + 1}/{(len(empty) + batch_size - 1) // batch_size} ({len(batch)} articles)")

        # Build article dicts for NER
        articles = []
        full_texts: dict[str, str] = {}
        for doc_id, d in batch:
            ov_id = d.get("ov_id", doc_id)
            articles.append({
                "dedup_group_id": ov_id,
                "title": d.get("title", ""),
                "snippet": d.get("snippet", ""),
                "category": d.get("ov_category", ""),
                "source_name": d.get("source_name", ""),
                "timestamp": d.get("published_at", ""),
                "region_code": d.get("region_code", ""),
            })
            ft = d.get("full_text", "")
            if ft:
                full_texts[ov_id] = ft

        results = await run_ner_batch(client, articles, full_texts, system_prompt, model)

        # Update Firestore with results
        ner_by_id: dict[str, dict] = {r["article_id"]: r for r in results}
        for doc_id, d in batch:
            ov_id = d.get("ov_id", doc_id)
            ner = ner_by_id.get(ov_id)
            if ner and ner.get("entities"):
                db.collection("news_articles").document(doc_id).update({
                    "entities": ner["entities"],
                    "relations": ner.get("relations", []),
                    "topics": ner.get("topics", []),
                    "sdg_alignment": ner.get("sdg_alignment", []),
                    "sentiment": ner.get("sentiment", 0.0),
                    "relevance_to_tn": ner.get("relevance_to_tn", 0.0),
                    "one_line_summary": ner.get("one_line_summary", ""),
                })
                succeeded += 1
                print(f"    ✓ {d.get('title', '')[:60]} → {len(ner['entities'])} entities")
            else:
                failed += 1
                print(f"    ✗ {d.get('title', '')[:60]} — NER failed")

        # Small pause between batches to avoid rate limiting
        if i + batch_size < len(empty):
            await asyncio.sleep(5)

    print(f"\n=== Done: {succeeded} succeeded, {failed} failed ===")


# ── Backfill consolidation for existing articles ─────────────────────────────

async def _consolidate_existing(model: str) -> None:
    """Run KG consolidation on Firestore articles that don't have consolidated_script_ta yet."""
    from google.cloud import firestore as fs
    from google import genai

    db = fs.Client(project=PROJECT)
    print("=== Backfill: Consolidate existing articles ===")

    # Fetch all articles that have entities but no consolidated script
    docs = list(db.collection("news_articles").stream())
    pending = []
    for doc in docs:
        d = doc.to_dict()
        if d.get("entities") and not d.get("consolidated_script_ta"):
            pending.append((doc.id, d))

    print(f"  Found {len(pending)} articles needing consolidation (of {len(docs)} total)")
    if not pending:
        print("  Nothing to consolidate.")
        return

    # Load KG
    print("  Loading News Knowledge Graph...")
    g = _load_news_kg()
    if g:
        print(f"  KG loaded: {g.number_of_nodes()} nodes, {g.number_of_edges()} edges")
    else:
        print("  KG not available — scripts will be standalone")

    client = genai.Client(vertexai=True, project=PROJECT, location=LOCATION)
    succeeded = 0

    for i, (doc_id, d) in enumerate(pending):
        title = d.get("title", "")
        summary = d.get("one_line_summary", "") or d.get("snippet", "")
        entities = d.get("entities", [])

        kg_context = _get_entity_context(g, entities, db, title)

        result = await consolidate_article(
            client, title, summary, kg_context, lang="ta", model=model,
        )

        if result.get("consolidated_script_ta"):
            db.collection("news_articles").document(doc_id).update(result)
            succeeded += 1
            ctx_count = len(result.get("contextual_history", []))
            ctx_label = f"+ {ctx_count} refs" if ctx_count else "standalone"
            print(f"  [{i + 1}/{len(pending)}] {title[:60]}... [{ctx_label}]")
        else:
            print(f"  [{i + 1}/{len(pending)}] {title[:60]}... [FAILED]")

        # Pace to avoid Gemini rate limits
        if (i + 1) % 5 == 0:
            await asyncio.sleep(2)

    print(f"\n=== Done: {succeeded}/{len(pending)} articles consolidated ===")


# ── Main ─────────────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(description="News Ingestion + NER Pipeline")
    parser.add_argument("--probe", action="store_true", help="Dry run — don't write to Firestore")
    parser.add_argument("--hours", type=int, default=2, help="Hours back to fetch (default: 2)")
    parser.add_argument("--categories", nargs="+", default=OV_CATEGORIES, help="Categories to poll")
    parser.add_argument("--model", default="gemini-2.5-flash", help="Gemini model")
    parser.add_argument("--skip-ner", action="store_true", help="Skip NER, store raw articles only")
    parser.add_argument("--retry-empty", action="store_true", help="Re-run NER on articles with empty entities")
    parser.add_argument("--consolidate-existing", action="store_true", help="Backfill consolidation on existing articles")
    parser.add_argument("--batch-size", type=int, default=NER_BATCH_SIZE, help="NER batch size (default: 10)")
    args = parser.parse_args()

    # Load env
    from dotenv import load_dotenv
    env_path = ROOT / "web" / ".env.local"
    if env_path.exists():
        load_dotenv(env_path)

    api_key = os.environ.get("OMNES_VIDENT_API_KEY", "")

    # ── Retry-empty mode: re-process articles that have no NER results ────────
    if args.retry_empty:
        await _retry_empty_ner(args.model, args.batch_size)
        return

    # ── Consolidate-existing mode: backfill KG-enriched scripts ──────────────
    if args.consolidate_existing:
        await _consolidate_existing(args.model)
        return

    if not api_key:
        print("ERROR: OMNES_VIDENT_API_KEY not set")
        sys.exit(1)

    print(f"=== News Ingestion Pipeline ===")
    print(f"  Hours back: {args.hours}")
    print(f"  Categories: {', '.join(args.categories)}")
    print(f"  Probe mode: {args.probe}")
    print()

    # 1. Fetch stories from OmnesVident
    print("[1/5] Fetching stories from OmnesVident...")
    stories = await fetch_ov_stories(api_key, args.categories, args.hours)
    if not stories:
        print("  No new stories found. Exiting.")
        return

    # 2. Dedup against Firestore (by ov_id + title similarity)
    if not args.probe:
        print("[2/5] Deduplicating against Firestore...")
        from google.cloud import firestore
        db = firestore.Client(project=PROJECT)
        existing_ids, existing_titles = get_existing_story_ids_and_titles(db)
        new_stories = [s for s in stories if s["dedup_group_id"] not in existing_ids]
        print(f"  {len(stories)} fetched, {len(existing_ids)} existing, {len(new_stories)} new (by ID)")
        # Title-similarity dedup: remove near-duplicates within batch + against existing
        new_stories = dedup_stories(new_stories, existing_titles)
        stories = new_stories
        if not stories:
            print("  All stories already ingested. Exiting.")
            return
    else:
        print("[2/5] Skipping dedup (probe mode)")

    # 3. Fetch full article texts
    print("[3/5] Fetching full article texts...")
    full_texts = await fetch_article_texts(stories)

    # 4. Run NER
    if args.skip_ner:
        print("[4/5] Skipping NER (--skip-ner)")
        ner_results: list[dict[str, Any]] = []
    else:
        print("[4/5] Running Gemini NER + Relation Extraction...")
        ref_data = _load_reference_data()
        print(f"  Reference data: {len(ref_data['politicians'])} politicians, "
              f"{len(ref_data['parties'])} parties, {len(ref_data['districts'])} districts")
        ner_results = await run_all_ner(stories, full_texts, ref_data, model=args.model)

    # 5. Consolidate — KG-enriched contextual scripts
    consolidation: dict[str, dict] = {}
    if not args.skip_ner and ner_results and not args.probe:
        print("[5/6] Running KG-enriched consolidation...")
        consolidation = await run_consolidation(stories, ner_results, db, model=args.model)
    else:
        print("[5/6] Skipping consolidation")

    # 6. Store or print
    if args.probe:
        print("[6/6] Probe mode — printing results:")
        print()
        for i, story in enumerate(stories[:5]):
            sid = story["dedup_group_id"]
            ner = next((r for r in ner_results if r.get("article_id") == sid), {})
            print(f"── Article {i + 1}: {story['title'][:80]} ──")
            print(f"   Category: {story.get('category')} | Heat: {story.get('heat_score')}")
            print(f"   Full text: {'Yes' if sid in full_texts else 'No'} ({len(full_texts.get(sid, ''))} chars)")
            if ner:
                print(f"   Summary: {ner.get('one_line_summary', '-')}")
                print(f"   Entities: {len(ner.get('entities', []))}")
                for e in ner.get("entities", [])[:5]:
                    cid = e.get("canonical_id", "")
                    cid_str = f" → {cid}" if cid else ""
                    print(f"     [{e['type']}] {e['name']}{cid_str}")
                print(f"   Relations: {len(ner.get('relations', []))}")
                for r in ner.get("relations", [])[:3]:
                    print(f"     ({r['subject']}) —{r['predicate']}→ ({r['object']})")
                print(f"   Topics: {', '.join(ner.get('topics', []))}")
                print(f"   SDGs: {', '.join(ner.get('sdg_alignment', []))}")
                print(f"   Sentiment: {ner.get('sentiment', 0):.2f} | Relevance: {ner.get('relevance_to_tn', 0):.2f}")
            print()
    else:
        print("[6/6] Storing in Firestore...")
        stored = store_articles(db, stories, ner_results, full_texts, consolidation)
        print(f"\n=== Done: {stored} articles ingested ===")


if __name__ == "__main__":
    asyncio.run(main())
