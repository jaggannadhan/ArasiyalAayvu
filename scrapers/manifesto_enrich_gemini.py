"""Manifesto Deep Enrichment via Gemini 2.5 Pro (Vertex AI).

Mirrors `manifesto_deep_enrich.py` (Claude version) but uses the same
project-wide Vertex AI auth as `manifesto_ocr_gemini.py`. Reuses the
battle-tested system prompt from manifesto_deep_enrich (TN budget context,
reference data grounding, arithmetic-required rule).

Generates 7 analytical fields per promise:
    impact_mechanism, promise_components, fiscal_cost_note,
    implementation_risk, root_cause_addressed, sustainability_verdict,
    sustainability_reasoning

Usage
-----
    # Probe (first 10 promises, no save):
    python scrapers/manifesto_enrich_gemini.py --party ntk --year 2026 --probe

    # Full enrichment, write to local JSON only:
    python scrapers/manifesto_enrich_gemini.py --party ntk --year 2026

    # Full + Firestore merge + SDG cache clear:
    python scrapers/manifesto_enrich_gemini.py --party ntk --year 2026 \
        --upload --clear-sdg-cache

Env
---
    GOOGLE_CLOUD_PROJECT  (default: naatunadappu)
    VERTEX_LOCATION       (default: us-central1)
    BACKEND_URL           (default: production Cloud Run URL)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "processed"
sys.path.insert(0, str(ROOT / "scrapers"))
from manifesto_deep_enrich import build_enrich_system_prompt  # noqa: E402

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "naatunadappu")
LOCATION = os.environ.get("VERTEX_LOCATION", "us-central1")
BACKEND_URL = os.environ.get(
    "BACKEND_URL",
    "https://arasiyalaayvu-be-bo6oacabma-uc.a.run.app",
)

BATCH_SIZE = 10        # promises per Gemini call
CONCURRENCY = 5
MODEL = "gemini-2.5-pro"

# Schema enforced by Gemini structured-output mode.
RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "results": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "index":                 {"type": "integer"},
                    "impact_mechanism":      {"type": "string"},
                    "promise_components": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "component": {"type": "string"},
                                "analysis":  {"type": "string"},
                            },
                            "required": ["component", "analysis"],
                        },
                    },
                    "fiscal_cost_note":         {"type": "string"},
                    "implementation_risk":      {"type": "string", "enum": ["low", "medium", "high"]},
                    "root_cause_addressed":     {"type": "boolean"},
                    "sustainability_verdict":   {"type": "string", "enum": ["structural", "symptomatic", "optics"]},
                    "sustainability_reasoning": {"type": "string"},
                    # Welfare-assessment fields (optional) — derived directly from the analysis.
                    "impact_depth":          {"type": "string", "enum": ["transformative", "substantive", "supplemental", "symbolic"]},
                    "beneficiary_coverage":  {"type": "string", "enum": ["universal", "broad_majority", "targeted_poor", "specific_group"]},
                    "fiscal_viability":      {"type": "string", "enum": ["feasible", "stressed", "central_dependent", "uncosted"]},
                    "coverage_gap_note":     {"type": "string"},
                },
                "required": [
                    "index", "impact_mechanism", "promise_components", "fiscal_cost_note",
                    "implementation_risk", "root_cause_addressed",
                    "sustainability_verdict", "sustainability_reasoning",
                    "impact_depth", "beneficiary_coverage", "fiscal_viability",
                ],
            },
        }
    },
    "required": ["results"],
}

EXTENDED_HINT = """\
ALSO classify each promise:
  • impact_depth: transformative (root-cause structural change), substantive (meaningful but partial),
    supplemental (modest help), symbolic (signalling, no material effect).
  • beneficiary_coverage: universal (every person), broad_majority (>50% of TN), targeted_poor
    (BPL/marginal groups), specific_group (a niche / occupational / geographic slice).
  • fiscal_viability: feasible (fits comfortably in discretionary budget), stressed (consumes a large
    share of available room), central_dependent (needs Union govt funds), uncosted (no figure stated
    and impossible to estimate).
  • coverage_gap_note (optional): one short sentence on who is left out — null/empty if not applicable.

Set "index" to the 0-based position of the promise within THIS batch (first promise = 0).
"""


def build_user_prompt(batch: list[dict]) -> str:
    """Build the user message that lists this batch's promises for Gemini."""
    lines = [EXTENDED_HINT, "", "PROMISES TO ANALYSE:", ""]
    for i, p in enumerate(batch):
        meta_bits: list[str] = [f"category={p.get('category')}"]
        if p.get("amount_mentioned"):
            meta_bits.append(f"amount={p['amount_mentioned']}")
        if p.get("scheme_name"):
            meta_bits.append(f"scheme={p['scheme_name']}")
        meta = " | ".join(meta_bits)
        lines.append(f"--- Promise {i} ({meta}) ---")
        lines.append(p.get("promise_text_en", "").strip())
        lines.append("")
    return "\n".join(lines)


async def enrich_batch(
    client: Any,
    system_prompt: str,
    batch: list[dict],
    label: str,
) -> list[dict] | None:
    from google.genai import types

    user = build_user_prompt(batch)
    config = types.GenerateContentConfig(
        system_instruction=system_prompt,
        response_mime_type="application/json",
        response_schema=RESPONSE_SCHEMA,
        temperature=0.2,
    )
    for attempt in range(3):
        try:
            resp = await client.aio.models.generate_content(
                model=MODEL, contents=user, config=config,
            )
            parsed = json.loads(resp.text)
            return parsed.get("results", [])
        except Exception as exc:  # noqa: BLE001
            wait = 5 * (attempt + 1)
            msg = str(exc)[:140]
            print(f"  ⚠ {label} attempt {attempt+1}/3 failed: {msg}; retrying in {wait}s")
            await asyncio.sleep(wait)
    print(f"  ✗ {label} GAVE UP after 3 attempts")
    return None


async def enrich_all(
    client: Any,
    promises: list[dict],
    batch_size: int,
    concurrency: int,
) -> list[dict]:
    system_prompt = build_enrich_system_prompt()
    sem = asyncio.Semaphore(concurrency)
    batches = [promises[i:i + batch_size] for i in range(0, len(promises), batch_size)]
    enriched_full: list[dict] = list(promises)  # in-place mutation

    async def run_one(batch_idx: int, batch: list[dict]) -> None:
        async with sem:
            label = f"batch {batch_idx + 1}/{len(batches)}"
            results = await enrich_batch(client, system_prompt, batch, label)
            if not results:
                return
            for r in results:
                idx_in_batch = r.get("index")
                if not isinstance(idx_in_batch, int) or not 0 <= idx_in_batch < len(batch):
                    continue
                global_idx = batch_idx * batch_size + idx_in_batch
                tgt = enriched_full[global_idx]
                # Only set fields that came back (defensive).
                for f in (
                    "impact_mechanism", "promise_components", "fiscal_cost_note",
                    "implementation_risk", "root_cause_addressed",
                    "sustainability_verdict", "sustainability_reasoning",
                    "impact_depth", "beneficiary_coverage", "fiscal_viability",
                ):
                    if f in r:
                        tgt[f] = r[f]
                gap = r.get("coverage_gap_note")
                if isinstance(gap, str) and gap.strip():
                    tgt["coverage_gap_note"] = gap.strip()
            enriched_in_batch = sum(1 for x in batch if x.get("impact_mechanism"))
            print(f"  ✓ {label}  →  {enriched_in_batch}/{len(batch)} enriched")

    await asyncio.gather(*(run_one(i, b) for i, b in enumerate(batches)))
    return enriched_full


def upload_enrichment(promises: list[dict], party_id: str, year: int) -> None:
    """Merge enrichment fields into existing Firestore docs (no delete/recreate)."""
    from google.cloud import firestore

    db = firestore.Client(project=PROJECT)
    col = db.collection("manifesto_promises")
    ENRICH_FIELDS = (
        "impact_mechanism", "promise_components", "fiscal_cost_note",
        "implementation_risk", "root_cause_addressed",
        "sustainability_verdict", "sustainability_reasoning",
        "impact_depth", "beneficiary_coverage", "fiscal_viability",
        "coverage_gap_note",
    )

    batch = db.batch()
    n = 0
    for p in promises:
        if not p.get("impact_mechanism"):
            continue  # skip un-enriched
        patch = {f: p[f] for f in ENRICH_FIELDS if f in p}
        batch.set(col.document(p["doc_id"]), patch, merge=True)
        n += 1
        if n % 400 == 0:
            batch.commit()
            batch = db.batch()
    batch.commit()
    print(f"  ✓ Merged enrichment into {n} Firestore docs")


def clear_sdg_cache(party_id: str, year: int) -> None:
    url = f"{BACKEND_URL}/api/manifesto/sdg-alignment/cache/clear?party_id={party_id}&year={year}"
    r = requests.post(url, timeout=30)
    r.raise_for_status()
    print(f"  ✓ SDG cache cleared: {r.json()}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--party", required=True)
    ap.add_argument("--year",  type=int, required=True)
    ap.add_argument("--batch-size",  type=int, default=BATCH_SIZE)
    ap.add_argument("--concurrency", type=int, default=CONCURRENCY)
    ap.add_argument("--probe",  action="store_true", help="Only enrich first 10 promises (sanity check)")
    ap.add_argument("--upload", action="store_true", help="Merge into Firestore")
    ap.add_argument("--clear-sdg-cache", action="store_true")
    args = ap.parse_args()

    in_path = OUT_DIR / f"manifesto_promises_{args.year}_{args.party}.json"
    if not in_path.exists():
        sys.exit(f"Missing input: {in_path}. Run manifesto_ocr_gemini.py first.")
    promises = json.loads(in_path.read_text(encoding="utf-8"))
    print(f"Loaded {len(promises)} promises from {in_path}")

    target = promises[:10] if args.probe else promises
    print(f"Will enrich {len(target)} promises in batches of {args.batch_size} (concurrency={args.concurrency})\n")

    from google import genai
    client = genai.Client(vertexai=True, project=PROJECT, location=LOCATION)

    enriched_subset = asyncio.run(enrich_all(
        client, target, batch_size=args.batch_size, concurrency=args.concurrency,
    ))

    if args.probe:
        print("\n── PROBE: first 2 enriched records ──\n")
        for p in enriched_subset[:2]:
            keys = [
                "doc_id", "category", "promise_text_en",
                "impact_mechanism", "promise_components", "fiscal_cost_note",
                "implementation_risk", "root_cause_addressed",
                "sustainability_verdict", "sustainability_reasoning",
                "impact_depth", "beneficiary_coverage", "fiscal_viability",
            ]
            view = {k: p.get(k) for k in keys}
            print(json.dumps(view, ensure_ascii=False, indent=2)[:1500])
            print("\n---\n")
        print("(Probe mode — not writing local JSON, not uploading)")
        return

    # Patch the in-memory list back into the full promises list (it's the same objects).
    in_path.write_text(json.dumps(promises, ensure_ascii=False, indent=2), encoding="utf-8")
    enriched_n = sum(1 for p in promises if p.get("impact_mechanism"))
    print(f"\n  ✓ Wrote {enriched_n}/{len(promises)} enriched promises → {in_path}")

    if args.upload:
        print("\n── Merging into Firestore ──")
        upload_enrichment(promises, args.party, args.year)

    if args.clear_sdg_cache:
        print("\n── Clearing backend SDG cache ──")
        clear_sdg_cache(args.party, args.year)


if __name__ == "__main__":
    main()
