"""Manifesto OCR + Translation via Gemini 2.5 Pro (Vertex AI multimodal).

Replaces the pdfplumber+Claude pipeline in manifesto_ingest.py for Tamil
manifestos encoded with legacy (non-Unicode) fonts, where text extraction
yields garbage and the downstream LLM hallucinates. Gemini reads the PDF pages
as images, OCRs the Tamil into Unicode, translates, and returns structured
promises in one call per 10-page batch.

Usage
-----
    # Dry run (first 2 batches only, prints samples):
    python scrapers/manifesto_ocr_gemini.py --party ntk --year 2026 --probe

    # Full extraction, write to data/processed/:
    python scrapers/manifesto_ocr_gemini.py --party ntk --year 2026

    # Full extraction + overwrite Firestore + invalidate SDG cache:
    python scrapers/manifesto_ocr_gemini.py --party ntk --year 2026 \
        --upload --clear-sdg-cache

Env
---
    GOOGLE_CLOUD_PROJECT  (default: naatunadappu)
    VERTEX_LOCATION       (default: us-central1)
    BACKEND_URL           (default: https://arasiyalaayvu-be-bo6oacabma-uc.a.run.app)
"""
from __future__ import annotations

import argparse
import asyncio
import io
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from pypdf import PdfReader, PdfWriter

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data" / "processed"
RAW_DIR = ROOT / "data" / "raw" / "manifesto_pdfs"

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "naatunadappu")
LOCATION = os.environ.get("VERTEX_LOCATION", "us-central1")
BACKEND_URL = os.environ.get("BACKEND_URL", "https://arasiyalaayvu-be-bo6oacabma-uc.a.run.app")

# Parties we support. The PDF URL here is the *official* source; we ignore the
# OpenCity mirror because some of those are re-compressed copies with different
# fonts/pagination.
PARTY_CONFIG: dict[tuple[str, int], dict[str, Any]] = {
    ("ntk", 2026): {
        "party_name":  "NTK",
        "party_color": "bg-yellow-600",
        "pdf_url":     "https://makkalarasu.in/assets/NTK-2026-Manifesto.pdf",
        "lang":        "ta",
        "confidence":  "HIGH",  # Gemini multimodal — OCR + translation one-shot
    },
    ("dmk", 2026): {
        "party_name":  "DMK",
        "party_color": "bg-red-600",
        "pdf_url":     "https://dmksite.blob.core.windows.net/prod-dmk-strapi-replica/assets/DMK_Manifesto_2026_d601f15181",
        "lang":        "ta",
        "confidence":  "HIGH",
    },
    ("aiadmk", 2026): {
        "party_name":  "AIADMK",
        "party_color": "bg-green-700",
        "pdf_url":     "https://aiadmk.com/wp-content/uploads/2026/03/Aiadmk-Manifesto-2026-english.pdf",
        "lang":        "en",  # official English version; Gemini also machine-translates promise_text_ta
        "confidence":  "HIGH",
    },
}

CATEGORIES = ["Agriculture", "Education", "Infrastructure", "TASMAC & Revenue", "Women's Welfare"]
# Must match StanceVibe in web/src/lib/types.ts — frontend's VIBE_META keys
# are PascalCase and crash if a value isn't recognized.
STANCE_VIBES = [
    "Welfare-centric", "Infrastructure-heavy", "Revenue-focused",
    "Populist", "Reform-oriented", "Women-focused", "Farmer-focused",
]

# JSON schema the model must return. Gemini enforces it when
# response_mime_type="application/json" + response_schema are set.
RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "promises": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "category":           {"type": "string", "enum": CATEGORIES},
                    "promise_text_ta":    {"type": "string", "description": "Original Tamil text in Tamil Unicode (e.g. நாம் தமிழர்), NOT legacy font bytes"},
                    "promise_text_en":    {"type": "string", "description": "Natural English translation, not literal"},
                    "page_in_batch":      {"type": "integer", "description": "1-based page number within this 10-page batch"},
                    "amount_mentioned":   {"type": "string", "description": "Empty string if no monetary amount is stated"},
                    "scheme_name":        {"type": "string", "description": "Empty string if no specific scheme name is given"},
                    "stance_vibe":        {"type": "string", "enum": STANCE_VIBES},
                    "is_aspirational":    {"type": "boolean", "description": "true for vision statements, false for concrete actions"},
                },
                "required": ["category", "promise_text_ta", "promise_text_en", "page_in_batch", "stance_vibe", "is_aspirational"],
            },
        }
    },
    "required": ["promises"],
}

EXTRACTION_PROMPT = """\
You are reading pages {start}–{end} (of {total}) of a {src_lang_desc} political
manifesto for the 2026 Tamil Nadu Assembly elections. {lang_specific_note}

Extract every distinct POLICY PROMISE or COMMITMENT. A promise is a concrete
pledge the party is making to voters about what they will do if elected.

For each promise:
  - `promise_text_ta`: the Tamil version of the promise, in Tamil UNICODE
    (e.g. நாம் தமிழர், not font-encoded bytes). {ta_source_rule}
  - `promise_text_en`: {en_source_rule}
  - `category`: MUST be one of: {categories}. Choose the closest fit. For
    healthcare, law & order, energy, housing, water, environment, industry
    etc. that don't map cleanly, use "Infrastructure" as the catch-all.
  - `page_in_batch`: 1-based page number WITHIN THIS BATCH (1..10).
  - `amount_mentioned`: if a rupee amount is stated, include it with the unit
    (e.g. "₹10,000 crore", "₹5,000/month"). Empty string if none.
  - `scheme_name`: if the promise names a specific scheme/program/mission,
    include it. Empty string if none.
  - `stance_vibe`: one of {stances}. Pick the BEST fit:
      • Welfare-centric — direct cash/free goods/subsidies to households
      • Infrastructure-heavy — roads, water, power, buildings, tech systems
      • Revenue-focused — fiscal/taxes/state finances/centre-state tax sharing
      • Populist — symbolic, identity, mass-appeal, name-branded schemes
      • Reform-oriented — institutional / governance / legal reform
      • Women-focused — explicitly targets women or girls
      • Farmer-focused — explicitly targets farmers/agricultural workers
  - `is_aspirational`: true for vision statements ("a safer Tamil Nadu"),
    false for concrete actions ("establish 543 primary schools").

DO NOT extract:
  - Page numbers, section headers, table-of-contents entries.
  - Biographies, historical anecdotes, quotes from leaders.
  - Pure ideological preamble without a specific commitment.
  - Duplicates — if the same promise appears twice on adjacent pages, return it once.

If a page contains no promises, skip it. Return an empty `promises` array if
the entire batch has none.
"""


def ensure_dirs() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)


def download_pdf(url: str, dest: Path) -> Path:
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  ✓ PDF already cached: {dest} ({dest.stat().st_size // 1024} KB)")
        return dest
    print(f"  ↓ Downloading {url}")
    r = requests.get(url, timeout=120, stream=True)
    r.raise_for_status()
    dest.write_bytes(r.content)
    print(f"  ✓ Saved {dest.stat().st_size // 1024} KB to {dest}")
    return dest


def split_pdf(path: Path, batch_size: int) -> list[tuple[int, int, bytes]]:
    """Return [(start_page_1indexed, end_page_1indexed, pdf_bytes)]."""
    reader = PdfReader(str(path))
    total = len(reader.pages)
    batches: list[tuple[int, int, bytes]] = []
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        writer = PdfWriter()
        for i in range(start, end):
            writer.add_page(reader.pages[i])
        buf = io.BytesIO()
        writer.write(buf)
        batches.append((start + 1, end, buf.getvalue()))
    return batches


async def extract_batch(
    client: Any,
    pdf_bytes: bytes,
    start_page: int,
    end_page: int,
    total_pages: int,
    model: str,
    src_lang: str = "ta",
) -> list[dict[str, Any]]:
    from google.genai import types

    if src_lang == "ta":
        src_lang_desc = "Tamil"
        lang_specific_note = (
            "The PDF uses legacy Tamil fonts, so you must OCR the pages as "
            "images and output Tamil Unicode."
        )
        ta_source_rule = (
            "Keep it concise — one or two sentences, trimmed of headers, in "
            "Tamil Unicode (never legacy-font bytes)."
        )
        en_source_rule = (
            "Natural English translation (not word-for-word literal)."
        )
    else:  # English-source PDF
        src_lang_desc = "English"
        lang_specific_note = "The PDF is in English — extract the text directly."
        ta_source_rule = (
            "Since the source is English, machine-translate the promise into "
            "clear conversational Tamil Unicode. Keep it concise and faithful."
        )
        en_source_rule = (
            "The original English text from the PDF, trimmed to one or two "
            "sentences without headers or bullet markers."
        )

    prompt = EXTRACTION_PROMPT.format(
        start=start_page,
        end=end_page,
        total=total_pages,
        categories=", ".join(CATEGORIES),
        stances=", ".join(STANCE_VIBES),
        src_lang_desc=src_lang_desc,
        lang_specific_note=lang_specific_note,
        ta_source_rule=ta_source_rule,
        en_source_rule=en_source_rule,
    )
    contents = [
        types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
        prompt,
    ]
    config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=RESPONSE_SCHEMA,
        temperature=0.1,  # low — we want consistent extraction, not creative rewriting
    )
    resp = await client.aio.models.generate_content(
        model=model, contents=contents, config=config
    )
    parsed = json.loads(resp.text)
    promises = parsed.get("promises", [])
    # Rewrite page_in_batch → absolute manifesto_pdf_page.
    for p in promises:
        p["manifesto_pdf_page"] = start_page + int(p.pop("page_in_batch", 1)) - 1
    return promises


async def run_all(
    client: Any,
    batches: list[tuple[int, int, bytes]],
    total_pages: int,
    concurrency: int,
    model: str,
    probe: bool,
    src_lang: str = "ta",
) -> list[dict[str, Any]]:
    sem = asyncio.Semaphore(concurrency)
    results: list[list[dict[str, Any]]] = [[] for _ in batches]
    batches_to_run = batches[:2] if probe else batches

    async def run_one(idx: int, s: int, e: int, data: bytes) -> None:
        async with sem:
            for attempt in range(3):
                try:
                    out = await extract_batch(client, data, s, e, total_pages, model, src_lang=src_lang)
                    results[idx] = out
                    print(f"  ✓ pp {s:>3}-{e:<3}  →  {len(out)} promises")
                    return
                except Exception as exc:  # noqa: BLE001 — API errors are opaque; retry all
                    wait = 5 * (attempt + 1)
                    msg = str(exc)[:120]
                    print(f"  ⚠ pp {s:>3}-{e:<3}  attempt {attempt+1}/3 failed: {msg}; retrying in {wait}s")
                    await asyncio.sleep(wait)
            print(f"  ✗ pp {s:>3}-{e:<3}  GAVE UP after 3 attempts")

    await asyncio.gather(*(run_one(i, s, e, d) for i, (s, e, d) in enumerate(batches_to_run)))

    flat: list[dict[str, Any]] = []
    for r in results:
        flat.extend(r)
    return flat


def finalize_records(
    raw: list[dict[str, Any]],
    party_id: str,
    year: int,
    cfg: dict[str, Any],
    pdf_url: str,
) -> list[dict[str, Any]]:
    """Dedupe, assign doc_ids, attach party metadata."""
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # Dedupe by normalized English text. Page order is preserved since batches
    # are returned in-order.
    seen: set[str] = set()
    kept: list[dict[str, Any]] = []
    for p in raw:
        en = (p.get("promise_text_en") or "").strip()
        key = " ".join(en.lower().split())
        if not en or key in seen:
            continue
        seen.add(key)
        kept.append(p)

    # Per-category counter for doc_ids.
    category_slug = {
        "Agriculture":        "agri",
        "Education":          "edu",
        "Infrastructure":     "infra",
        "TASMAC & Revenue":   "tasmac",
        "Women's Welfare":    "women",
    }
    counters: dict[str, int] = {k: 0 for k in category_slug.values()}

    records: list[dict[str, Any]] = []
    for p in kept:
        cat = p.get("category") or "Infrastructure"
        if cat not in category_slug:
            cat = "Infrastructure"
        slug = category_slug[cat]
        counters[slug] += 1
        doc_id = f"{party_id}_{year}_{slug}_{counters[slug]:03d}"

        records.append({
            "doc_id":                  doc_id,
            "party_id":                party_id,
            "party_name":              cfg["party_name"],
            "party_color":             cfg["party_color"],
            "category":                cat,
            "promise_text_en":         p["promise_text_en"].strip(),
            "promise_text_ta":         p["promise_text_ta"].strip(),
            "target_year":             year,
            "status":                  "Proposed",
            "stance_vibe":             p.get("stance_vibe") or "pragmatic",
            "amount_mentioned":        (p.get("amount_mentioned") or "").strip() or None,
            "scheme_name":             (p.get("scheme_name") or "").strip() or None,
            "manifesto_pdf_url":       pdf_url,
            "manifesto_pdf_page":      p.get("manifesto_pdf_page"),
            "source_notes":            f"Gemini 2.5 Pro multimodal OCR + translation on pages {p.get('manifesto_pdf_page')}",
            "ground_truth_confidence": cfg["confidence"],
            "is_joint_manifesto":      False,
            "track_fulfillment":       True,
            "is_aspirational":         bool(p.get("is_aspirational", False)),
            "_extracted_at":           now,
            "_extractor":              "manifesto_ocr_gemini.py",
        })
    return records


def write_local(records: list[dict[str, Any]], party_id: str, year: int) -> Path:
    out = OUT_DIR / f"manifesto_promises_{year}_{party_id}.json"
    out.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  ✓ Wrote {len(records)} promises → {out}  ({out.stat().st_size // 1024} KB)")
    return out


def overwrite_firestore(records: list[dict[str, Any]], party_id: str, year: int) -> None:
    from google.cloud import firestore
    from google.cloud.firestore_v1.base_query import FieldFilter

    db = firestore.Client(project=PROJECT)
    col = db.collection("manifesto_promises")

    # Delete existing docs for this (party_id, year) — wrong data we're replacing.
    existing = list(
        col.where(filter=FieldFilter("party_id", "==", party_id))
           .where(filter=FieldFilter("target_year", "==", year))
           .stream()
    )
    print(f"  Found {len(existing)} existing {party_id} {year} docs to delete")
    batch = db.batch()
    for i, d in enumerate(existing, 1):
        batch.delete(d.reference)
        if i % 400 == 0:  # Firestore batch limit 500
            batch.commit()
            batch = db.batch()
    batch.commit()
    print(f"  ✓ Deleted {len(existing)} old docs")

    # Write new docs.
    batch = db.batch()
    for i, rec in enumerate(records, 1):
        batch.set(col.document(rec["doc_id"]), rec)
        if i % 400 == 0:
            batch.commit()
            batch = db.batch()
    batch.commit()
    print(f"  ✓ Uploaded {len(records)} new docs")


def clear_sdg_cache(party_id: str, year: int) -> None:
    url = f"{BACKEND_URL}/api/manifesto/sdg-alignment/cache/clear?party_id={party_id}&year={year}"
    r = requests.post(url, timeout=30)
    r.raise_for_status()
    print(f"  ✓ SDG cache cleared: {r.json()}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--party", required=True, help="party_id, e.g. ntk")
    ap.add_argument("--year",  type=int, required=True, help="target_year, e.g. 2026")
    ap.add_argument("--model", default="gemini-2.5-pro")
    ap.add_argument("--batch-size",  type=int, default=10, help="pages per Gemini call")
    ap.add_argument("--concurrency", type=int, default=5,  help="parallel Gemini calls")
    ap.add_argument("--probe",  action="store_true", help="Only run first 2 batches (quality check)")
    ap.add_argument("--upload", action="store_true", help="Overwrite Firestore manifesto_promises")
    ap.add_argument("--clear-sdg-cache", action="store_true", help="Invalidate backend SDG cache after upload")
    args = ap.parse_args()

    key = (args.party, args.year)
    if key not in PARTY_CONFIG:
        sys.exit(f"No config for {args.party} / {args.year}")
    cfg = PARTY_CONFIG[key]

    ensure_dirs()
    pdf_path = RAW_DIR / f"{args.party}-{args.year}-manifesto.pdf"
    download_pdf(cfg["pdf_url"], pdf_path)

    print(f"\n── Splitting PDF into {args.batch_size}-page batches ──")
    batches = split_pdf(pdf_path, args.batch_size)
    total = batches[-1][1]
    print(f"  Total pages: {total} · Batches: {len(batches)}")

    print(f"\n── Extracting with {args.model} (concurrency={args.concurrency}) ──")
    from google import genai
    client = genai.Client(vertexai=True, project=PROJECT, location=LOCATION)

    raw = asyncio.run(run_all(
        client, batches, total,
        concurrency=args.concurrency,
        model=args.model,
        probe=args.probe,
        src_lang=cfg.get("lang", "ta"),
    ))
    print(f"  Raw promises across batches: {len(raw)}")

    records = finalize_records(raw, args.party, args.year, cfg, cfg["pdf_url"])
    print(f"  After dedupe + normalization: {len(records)}")

    if args.probe:
        print("\n── PROBE: showing first 3 records ──")
        for r in records[:3]:
            print(json.dumps(r, ensure_ascii=False, indent=2)[:600])
            print("---")
        print("\n(Probe mode — not writing local JSON, not uploading)")
        return

    write_local(records, args.party, args.year)

    if args.upload:
        print("\n── Overwriting Firestore ──")
        overwrite_firestore(records, args.party, args.year)

    if args.clear_sdg_cache:
        print("\n── Clearing backend SDG alignment cache ──")
        clear_sdg_cache(args.party, args.year)


if __name__ == "__main__":
    main()
