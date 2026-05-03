"""News NER helpers for the cron ingestion endpoint.

Minimal extraction of _load_reference_data, _build_ner_system_prompt, and
NER_RESPONSE_SCHEMA from scrapers/news_ingestion.py so the backend Docker
image doesn't need the entire scrapers/ directory.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]  # web/
CANDIDATE_INDEX_PATH = ROOT_DIR / "src" / "lib" / "candidate-search-index.json"

# Fallback: the Docker image copies this to a known location
DOCKER_CANDIDATE_INDEX = Path("/app/data/candidate-search-index.json")

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

DISTRICTS = [
    "ariyalur", "chengalpattu", "chennai", "coimbatore", "cuddalore",
    "dharmapuri", "dindigul", "erode", "kallakurichi", "kancheepuram",
    "kanyakumari", "karur", "krishnagiri", "madurai", "mayiladuthurai",
    "nagapattinam", "namakkal", "nilgiris", "perambalur", "pudukkottai",
    "ramanathapuram", "ranipet", "salem", "sivaganga", "tenkasi",
    "thanjavur", "theni", "thoothukudi", "tiruchirappalli", "tirunelveli",
    "tirupattur", "tirupur", "tiruvallur", "tiruvannamalai", "tiruvarur",
    "vellore", "viluppuram", "virudhunagar",
]

PARTY_IDS = [
    "dmk", "aiadmk", "bjp", "inc", "pmk", "tvk", "ntk",
    "cpi", "cpim", "vck", "dmdk", "mdmk", "bsp",
]


def load_reference_data() -> dict[str, Any]:
    """Load compact reference lists for Gemini entity resolution."""
    politicians: list[dict[str, str]] = []
    index_path = CANDIDATE_INDEX_PATH if CANDIDATE_INDEX_PATH.exists() else DOCKER_CANDIDATE_INDEX
    if index_path.exists():
        with open(index_path, encoding="utf-8") as f:
            raw = json.load(f)
        seen: set[str] = set()
        for entry in raw:
            name = entry["n"]
            if name not in seen and entry["p"] in MAJOR_PARTIES:
                seen.add(name)
                politicians.append({"name": name, "party": entry["p"], "constituency": entry["s"]})

    return {"politicians": politicians, "parties": PARTY_IDS, "districts": DISTRICTS}


def build_ner_system_prompt(ref_data: dict[str, Any]) -> str:
    """Build the system prompt with reference entity lists."""
    pol_lines = [f"{p['name']} | {p['party']} | {p['constituency']}" for p in ref_data["politicians"]]
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
1. Extract ALL meaningful entities — not just politicians.
2. For each entity, set canonical_id ONLY if it matches the reference lists above. Otherwise set canonical_id to null.
3. Generate a snake_case normalized `node_id` for every entity.
4. Extract relationships as (subject_node_id, predicate, object_node_id) triples.
5. Assign SDG IDs where relevant.
6. Rate relevance_to_tn from 0.0 to 1.0.
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
                                "sdg_ids": {"type": "array", "items": {"type": "string"}},
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
                    "topics": {"type": "array", "items": {"type": "string"}},
                    "sdg_alignment": {"type": "array", "items": {"type": "string"}},
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
