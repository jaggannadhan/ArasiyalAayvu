"""
Constitution of India — Clause-Level Text Extraction
=====================================================
Extracts the full text of every article, clause by clause, from the official
Constitution PDF published by India Code (indiacode.nic.in).

Source PDF
----------
  "The Constitution of India" — Legislative Department, Ministry of Law & Justice
  https://www.indiacode.nic.in/bitstream/123456789/19151/1/constitution_of_india.pdf

Approach
--------
  The PDF is a proper text PDF (not scanned), so we extract text with pypdf
  and parse it with regex patterns to identify article boundaries and clause
  numbers. For ambiguous sections, Gemini is used to structure the text.

Output
------
  Enriches data/processed/indian_constitution.json with `clauses[]` on each
  article, and adds `full_text` field.

Usage
-----
  # Extract clauses from PDF (dry run):
  .venv/bin/python scrapers/constitution_clause_extract.py --dry-run

  # Extract and merge into existing constitution JSON:
  .venv/bin/python scrapers/constitution_clause_extract.py

  # Use Gemini for difficult sections:
  .venv/bin/python scrapers/constitution_clause_extract.py --use-gemini
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from pypdf import PdfReader

ROOT = Path(__file__).resolve().parents[1]
PDF_PATH = ROOT / "data" / "raw" / "constitution" / "constitution_of_india.pdf"
CONSTITUTION_JSON = ROOT / "data" / "processed" / "indian_constitution.json"
BACKEND_COPY = ROOT / "web" / "backend_api" / "indian_constitution.json"

# ---------------------------------------------------------------------------
# PDF Text Extraction
# ---------------------------------------------------------------------------


def extract_full_text(pdf_path: Path) -> str:
    """Extract all text from the Constitution PDF."""
    reader = PdfReader(str(pdf_path))
    pages = []
    for i, page in enumerate(reader.pages):
        text = page.extract_text() or ""
        pages.append(text)
    return "\n".join(pages)


# ---------------------------------------------------------------------------
# Article Parser
# ---------------------------------------------------------------------------

# Matches article start: "164." or "21A." or "243ZG." at the start of a line
# or after a newline, followed by clause text
ARTICLE_START = re.compile(
    r'(?:^|\n)'                          # start of text or newline
    r'(\d{1,3}[A-Z]{0,3})\.\s+'         # article number + dot + space
    r'(\(1\)\s+)?'                       # optional "(1)" for first clause
    r'(.+?)(?=\n\d{1,3}[A-Z]{0,3}\.\s|\Z)',  # text until next article
    re.DOTALL,
)

# Matches clause numbers within an article: "(1)", "(2)", "(1A)", etc.
CLAUSE_PATTERN = re.compile(
    r'\((\d{1,2}[A-Z]?)\)\s+'
)

# Matches sub-clause: "(a)", "(b)", "(i)", "(ii)" etc.
SUB_CLAUSE_PATTERN = re.compile(
    r'\(([a-z]{1,3}|[ivx]{1,4})\)\s+'
)

# Matches proviso patterns
PROVISO_PATTERN = re.compile(
    r'(?:Provided\s+that|Provided\s+further\s+that|Provided\s+also\s+that)',
    re.IGNORECASE
)

# Matches "Explanation" sections
EXPLANATION_PATTERN = re.compile(
    r'(?:^|\n)\s*Explanation\.?\s*[-—]?\s*',
    re.IGNORECASE
)


def clean_text(text: str) -> str:
    """Clean extracted text: normalize whitespace, remove footnote markers and page chrome."""
    # Remove footnote reference numbers like 1[, 2[, *[
    text = re.sub(r'\d+\[', '', text)
    text = re.sub(r'\*\[', '', text)
    # Remove unmatched closing brackets that were part of footnotes
    text = re.sub(r'\](?!\))', '', text)
    # Remove page headers/footers: "THE CONSTITUTION OF INDIA" lines
    text = re.sub(r'THE CONSTITUTION OF INDIA\s*', '', text)
    # Remove part/article references in footers: "(Part VI.—The States.—Arts. 163-164.)"
    text = re.sub(r'\(Part\s+[IVXLC]+[A-Z]*\..*?Arts?\.\s*\d+.*?\)', '', text)
    # Remove page numbers (standalone numbers on a line)
    text = re.sub(r'\n\d{1,3}\n', '\n', text)
    # Remove footnote text at bottom (starts with number + period pattern like "1Subs. by the...")
    text = re.sub(r'\n\d+(?:Subs|Ins|Added|Omitted|Rep|See)\..*?(?=\n|$)', '', text)
    # Remove marginal notes (short phrases like "Other provisions\nas to Ministers.")
    # These appear as short lines between clause text — harder to remove generically,
    # so we leave them for now (they're harmless in the output)
    # Remove * footnote date references like "*7-1-2004, vide S.O. 21(E)..."
    text = re.sub(r'\*\d{1,2}-\d{1,2}-\d{4},\s*vide.*?(?=\n|$)', '', text)
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text


# Pattern for clause numbers that appear at the START of a clause (not mid-text references)
# Key insight: real clause starts appear after a newline (or start of text),
# potentially preceded by footnote markers like "2[".
# Mid-text references look like "under clause (1)" or "in sub-clause (a)".
CLAUSE_START_RE = re.compile(
    r'(?:^|\n)\s*'                  # start of text or newline
    r'(?:\d+\[)?'                   # optional footnote marker like "2["
    r'\((\d{1,2}[A-Z]?)\)\s+'      # clause number: (1), (2), (1A), (1B), etc.
)


def parse_article_clauses(article_text: str) -> list[dict[str, str]]:
    """Parse an article's text into individual clauses."""
    clauses: list[dict[str, str]] = []

    # Find clause starts (must be at beginning of line, not mid-text)
    clause_matches = list(CLAUSE_START_RE.finditer(article_text))

    if not clause_matches:
        # Single-clause article (no sub-numbering)
        cleaned = clean_text(article_text)
        if cleaned:
            clauses.append({"sub": "", "text": cleaned})
        return clauses

    # Check for text before the first clause (preamble text of the article)
    first_start = clause_matches[0].start()
    if first_start > 5:  # more than just whitespace
        preamble = article_text[:first_start].strip()
        preamble_cleaned = clean_text(preamble)
        if preamble_cleaned and len(preamble_cleaned) > 20:
            clauses.append({"sub": "", "text": preamble_cleaned})

    # Track seen clause numbers to avoid duplicates from mid-text matches
    seen_clauses: set[str] = set()

    for i, match in enumerate(clause_matches):
        clause_num = match.group(1)

        # Skip if we've already seen this clause number (likely a mid-text reference
        # that slipped through)
        if clause_num in seen_clauses:
            continue
        seen_clauses.add(clause_num)

        start = match.end()

        # End is the start of the next clause match, or end of text
        if i + 1 < len(clause_matches):
            end = clause_matches[i + 1].start()
        else:
            end = len(article_text)

        clause_text = article_text[start:end].strip()
        cleaned = clean_text(clause_text)

        if cleaned:
            clauses.append({"sub": f"({clause_num})", "text": cleaned})

    return clauses


def extract_articles_from_text(full_text: str) -> dict[str, dict[str, Any]]:
    """
    Extract all articles with their clause text from the full Constitution text.
    Returns {article_number: {title_hint, clauses, full_text}}.
    """
    articles: dict[str, dict[str, Any]] = {}

    # Split text by article boundaries
    # We look for patterns like "\n164. " or "\n21A. " at line starts
    # The article number is followed by either a clause "(1)" or direct text

    # More robust: find all article starts
    article_positions: list[tuple[str, int]] = []

    # Pattern: newline + optional footnote marker (e.g. "2[") + article number + dot + space
    # Real examples from PDF: "\n164. ", "\n2[226. ", "\n1[370. ", "\n*[21A. "
    for m in re.finditer(r'\n(?:\d+\[|\*\[)?(\d{1,3}[A-Z]{0,3})\.\s', full_text):
        art_num = m.group(1)
        # Filter out things that aren't article numbers
        # Article numbers: 1-395, plus suffixed ones like 21A, 31A, 243ZG etc.
        base_num = re.match(r'(\d+)', art_num)
        if base_num and 1 <= int(base_num.group(1)) <= 395:
            article_positions.append((art_num, m.start() + 1))  # +1 to skip newline

    # Now extract text between consecutive article starts
    for i, (art_num, start) in enumerate(article_positions):
        # End position is start of next article, or end of text
        if i + 1 < len(article_positions):
            end = article_positions[i + 1][1]
        else:
            end = len(full_text)

        raw_text = full_text[start:end]

        # Remove the "164. " or "2[226. " prefix (with optional footnote marker)
        raw_text = re.sub(r'^(?:\d+\[|\*\[)?\d{1,3}[A-Z]{0,3}\.\s+', '', raw_text)

        # Parse into clauses
        clauses = parse_article_clauses(raw_text)

        # Build full_text (cleaned version of entire article)
        full_article_text = clean_text(raw_text)

        if art_num not in articles:
            articles[art_num] = {
                "clauses": clauses,
                "full_text": full_article_text[:3000],  # cap at 3000 chars
                "clause_count": len(clauses),
            }

    return articles


# ---------------------------------------------------------------------------
# Gemini enrichment (optional — for edge cases)
# ---------------------------------------------------------------------------

async def gemini_enrich_article(article_num: str, raw_text: str) -> list[dict[str, str]]:
    """Use Gemini to parse a difficult article into structured clauses."""
    from google import genai
    from google.genai import types

    client = genai.Client(
        project=os.environ.get("GOOGLE_CLOUD_PROJECT", "naatunadappu"),
        location=os.environ.get("VERTEX_LOCATION", "us-central1"),
    )

    prompt = f"""Parse Article {article_num} of the Constitution of India into structured clauses.

The raw text is:
---
{raw_text[:4000]}
---

Return a JSON array of clauses. Each clause must have:
- "sub": the clause number like "(1)", "(2)", "(1A)", or "" for single-clause articles
- "text": the full text of that clause, cleaned up (remove footnote markers like [1, *[, etc.)

Include Provisos as part of the clause they belong to.
Include Explanations as separate entries with sub like "Explanation" or "Explanation I".
"""

    schema = {
        "type": "object",
        "properties": {
            "clauses": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "sub": {"type": "string"},
                        "text": {"type": "string"},
                    },
                    "required": ["sub", "text"],
                },
            }
        },
        "required": ["clauses"],
    }

    config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=schema,
        temperature=0.0,
    )

    resp = await client.aio.models.generate_content(
        model="gemini-2.5-pro",
        contents=[prompt],
        config=config,
    )
    parsed = json.loads(resp.text)
    return parsed.get("clauses", [])


# ---------------------------------------------------------------------------
# Merge into existing constitution JSON
# ---------------------------------------------------------------------------

def merge_clauses(constitution_data: dict, extracted: dict[str, dict]) -> tuple[int, int]:
    """Merge extracted clauses into the constitution JSON. Returns (enriched, skipped)."""
    enriched = 0
    skipped = 0

    for part in constitution_data.get("parts", []):
        for article in part.get("articles", []):
            art_num = article["number"]

            if art_num in extracted:
                ext = extracted[art_num]
                article["clauses"] = ext["clauses"]
                article["full_text"] = ext["full_text"]
                article["clause_count"] = ext["clause_count"]
                enriched += 1
            else:
                skipped += 1

    return enriched, skipped


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Constitution clause-level extraction")
    parser.add_argument("--dry-run", action="store_true", help="Extract and print without writing")
    parser.add_argument("--use-gemini", action="store_true", help="Use Gemini for edge-case articles")
    parser.add_argument("--article", type=str, help="Extract a specific article only (e.g., '164')")
    args = parser.parse_args()

    if not PDF_PATH.exists():
        print(f"ERROR: Constitution PDF not found at {PDF_PATH}")
        print("Download it first:")
        print(f'  curl -L -o {PDF_PATH} "https://www.indiacode.nic.in/bitstream/123456789/19151/1/constitution_of_india.pdf"')
        sys.exit(1)

    print("Constitution of India — Clause-Level Extraction")
    print("=" * 55)
    print(f"Source: {PDF_PATH} ({PDF_PATH.stat().st_size // 1024} KB)")

    # Step 1: Extract full text from PDF
    print("\n--- Extracting text from PDF ---")
    full_text = extract_full_text(PDF_PATH)
    print(f"  Total text: {len(full_text):,} characters from {PdfReader(str(PDF_PATH)).pages.__len__()} pages")

    # Step 2: Parse articles
    print("\n--- Parsing articles ---")
    extracted = extract_articles_from_text(full_text)
    print(f"  Found {len(extracted)} articles in PDF")

    # Show specific article if requested
    if args.article:
        art = extracted.get(args.article)
        if art:
            print(f"\n  Article {args.article}:")
            print(f"  Clauses: {art['clause_count']}")
            for c in art["clauses"]:
                sub_label = c["sub"] if c["sub"] else "(single)"
                print(f"    {sub_label}: {c['text'][:150]}...")
        else:
            print(f"\n  Article {args.article} not found in extracted text")
            # Show nearby articles
            nearby = [k for k in sorted(extracted.keys(), key=lambda x: (len(x), x))
                      if k.startswith(args.article[:2])]
            print(f"  Nearby: {nearby[:10]}")
        return

    # Step 3: Load existing constitution JSON
    if not CONSTITUTION_JSON.exists():
        print(f"\nERROR: {CONSTITUTION_JSON} not found. Run constitution_ingest.py --seed first.")
        sys.exit(1)

    with CONSTITUTION_JSON.open("r", encoding="utf-8") as f:
        constitution_data = json.load(f)

    # Step 4: Merge
    print("\n--- Merging clauses into constitution data ---")
    enriched, skipped = merge_clauses(constitution_data, extracted)
    print(f"  Enriched: {enriched} articles with clause text")
    print(f"  Skipped: {skipped} articles (not found in PDF extraction)")

    # Show sample
    sample_articles = ["14", "19", "21", "32", "164", "226", "352", "368"]
    print("\n--- Sample enriched articles ---")
    for part in constitution_data["parts"]:
        for art in part["articles"]:
            if art["number"] in sample_articles and "clauses" in art:
                n_clauses = len(art["clauses"])
                first_clause = art["clauses"][0]["text"][:100] if art["clauses"] else "N/A"
                print(f"  Art {art['number']:5s} | {n_clauses} clauses | {first_clause}...")

    # Step 5: Update meta
    constitution_data["meta"]["clause_enrichment"] = {
        "status": "complete",
        "articles_enriched": enriched,
        "articles_skipped": skipped,
        "source_pdf": "indiacode.nic.in/bitstream/123456789/19151/1/constitution_of_india.pdf",
        "extraction_method": "pypdf text extraction + regex parsing",
    }

    # Step 6: Write
    if args.dry_run:
        print(f"\n[DRY RUN] Would write {enriched} enriched articles to {CONSTITUTION_JSON}")
    else:
        with CONSTITUTION_JSON.open("w", encoding="utf-8") as f:
            json.dump(constitution_data, f, ensure_ascii=False, indent=2)
        print(f"\nWrote {CONSTITUTION_JSON} ({CONSTITUTION_JSON.stat().st_size:,} bytes)")

        with BACKEND_COPY.open("w", encoding="utf-8") as f:
            json.dump(constitution_data, f, ensure_ascii=False, indent=2)
        print(f"Copied to {BACKEND_COPY}")

    print("\nDone!")


if __name__ == "__main__":
    main()
