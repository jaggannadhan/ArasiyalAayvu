"""
Manifesto Ingest — Tamil Nadu Election Manifestos
==================================================
Downloads party manifesto PDFs (OpenCity / party sites), extracts structured
promises using Claude (OCR + LLM for Tamil PDFs), and writes to Firestore
`manifesto_promises` collection.

Sources
-------
  2026 TN — OpenCity / ECI  (DMK, AIADMK, NTK — Tamil PDFs)
  2024 National — OpenCity   (BJP, INC, CPI(M) — English PDFs, LOW confidence)

Requirements
------------
  pip install anthropic pdfplumber requests google-cloud-firestore python-dotenv

Env vars
--------
  ANTHROPIC_API_KEY   — required
  GOOGLE_CLOUD_PROJECT — required for --upload (default: naatunadappu)

Usage
-----
  python scrapers/manifesto_ingest.py --year 2026 --dry-run
  python scrapers/manifesto_ingest.py --year 2026 --party dmk --dry-run
  python scrapers/manifesto_ingest.py --year 2026 --party dmk
  python scrapers/manifesto_ingest.py --year 2026
  python scrapers/manifesto_ingest.py --year 2021 --party bjp
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

ROOT_DIR = Path(__file__).resolve().parents[1]
OUT_DIR  = ROOT_DIR / "data" / "processed"

# ---------------------------------------------------------------------------
# Source catalogue
# ---------------------------------------------------------------------------
# Each entry: (party_id, target_year) → config dict
SOURCE_CONFIG: dict[tuple[str, int], dict[str, Any]] = {
    # ── 2026 TN manifestos (OpenCity / ECI) ─────────────────────────────────
    ("dmk", 2026): {
        "party_name":  "DMK",
        "party_color": "bg-red-600",
        "pdf_url":     "https://data.opencity.in/dataset/ee83bf58-b563-4e90-86fb-0d8c1330adbd/resource/d56e2869-5d15-4760-b150-e5caaeab1422/download/dmk-manifesto-document.pdf",
        "lang":        "ta",
        "confidence":  "MEDIUM",
        "is_joint":    False,
        "track_fulfillment": True,
    },
    ("aiadmk", 2026): {
        "party_name":  "AIADMK",
        "party_color": "bg-green-700",
        "pdf_url":     "https://data.opencity.in/dataset/ee83bf58-b563-4e90-86fb-0d8c1330adbd/resource/5b4a3445-72f1-4e16-9047-5f39c57ee948/download/aiadmk-election-manifesto-assembly-general-election-2026.pdf",
        "lang":        "ta",
        "confidence":  "MEDIUM",
        "is_joint":    False,
        "track_fulfillment": True,
    },
    ("ntk", 2026): {
        "party_name":  "NTK",
        "party_color": "bg-yellow-600",
        "pdf_url":     "https://data.opencity.in/dataset/ee83bf58-b563-4e90-86fb-0d8c1330adbd/resource/2d5dd958-6932-40a0-8e11-453ea389cc58/download/ntk-2026-manifesto_compressed.pdf",
        "lang":        "ta",
        "confidence":  "MEDIUM",
        "is_joint":    False,
        "track_fulfillment": True,
    },
    # ── 2024 National manifestos (proxy for 2021 TN opposition reference) ───
    # These are national manifestos — not TN-specific.
    # Used only as opposition reference; fulfillment tracking disabled.
    ("bjp", 2024): {
        "party_name":  "BJP",
        "party_color": "bg-orange-500",
        "pdf_url":     "https://data.opencity.in/dataset/76e54184-f294-44e4-a40c-8594ccb410c8/resource/6210fb78-c1c3-4700-a61f-ed01daee9aff/download/7377fce3-f32d-4dba-8d1c-4969c25a3add.pdf",
        "lang":        "en",
        "confidence":  "LOW",
        "is_joint":    False,
        "track_fulfillment": False,
        "source_note": "National manifesto (Lok Sabha 2024) — not TN-specific",
    },
    ("inc", 2024): {
        "party_name":  "INC",
        "party_color": "bg-blue-600",
        "pdf_url":     "https://data.opencity.in/dataset/76e54184-f294-44e4-a40c-8594ccb410c8/resource/e2a62a20-74a6-472e-ab7e-79f610235893/download/8a16787a-0134-4ac4-9fde-d17506675642.pdf",
        "lang":        "en",
        "confidence":  "LOW",
        "is_joint":    False,
        "track_fulfillment": False,
        "source_note": "National manifesto (Lok Sabha 2024) — not TN-specific",
    },
    ("cpim", 2024): {
        "party_name":  "CPI(M)",
        "party_color": "bg-rose-700",
        "pdf_url":     "https://data.opencity.in/dataset/76e54184-f294-44e4-a40c-8594ccb410c8/resource/c04b71e0-5941-4684-8b7e-c58cd713081d/download/2bc55e52-18f0-42fa-b228-56e85ec55de5.pdf",
        "lang":        "en",
        "confidence":  "LOW",
        "is_joint":    False,
        "track_fulfillment": False,
        "source_note": "National manifesto (Lok Sabha 2024) — not TN-specific",
    },
}

# Category short codes for doc_id generation
_CAT_ABBREV: dict[str, str] = {
    "Agriculture":       "agri",
    "Education":         "edu",
    "TASMAC & Revenue":  "tasmac",
    "Women's Welfare":   "women",
    "Infrastructure":    "infra",
}

VALID_CATEGORIES = set(_CAT_ABBREV.keys())
VALID_STANCES = {
    "Welfare-centric", "Infrastructure-heavy", "Revenue-focused",
    "Populist", "Reform-oriented", "Women-focused", "Farmer-focused",
}

PAGES_PER_CHUNK = 5   # pages sent to Claude per API call

# ---------------------------------------------------------------------------
# PDF download + text extraction
# ---------------------------------------------------------------------------

def download_pdf(url: str, dest: Path) -> None:
    headers = {"User-Agent": "ArasiyalAayvuResearchBot/1.0"}
    r = requests.get(url, headers=headers, timeout=60, stream=True)
    r.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in r.iter_content(chunk_size=65536):
            f.write(chunk)
    print(f"  Downloaded {dest.stat().st_size // 1024} KB")


def extract_pages(pdf_path: Path) -> list[tuple[int, str]]:
    """Return list of (page_number, text) for all pages."""
    import pdfplumber

    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        print(f"  {total} pages in PDF")
        for i, page in enumerate(pdf.pages, start=1):
            text = (page.extract_text() or "").strip()
            if text:
                pages.append((i, text))
    return pages


# ---------------------------------------------------------------------------
# Claude extraction
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are an expert Tamil Nadu election manifesto analyst.
Your task: extract concrete, specific electoral PROMISES from the given manifesto text.

CATEGORIES (use EXACTLY these strings — nothing else):
  "Agriculture"       — farm loans, irrigation, crop insurance, MSP, farmers
  "Education"         — schools, laptops, scholarships, mid-day meals, skill dev
  "TASMAC & Revenue"  — liquor policy, TASMAC, alcohol, state revenue, taxation
  "Women's Welfare"   — SHGs, maternity, women's safety, reservations, Magalir
  "Infrastructure"    — roads, power, water supply, housing, transport, internet

STANCE_VIBE (pick one):
  "Welfare-centric" | "Infrastructure-heavy" | "Revenue-focused" |
  "Populist" | "Reform-oriented" | "Women-focused" | "Farmer-focused"

RULES:
1. Extract only SPECIFIC, ACTIONABLE promises — not vague aspirations.
   Good: "Waive farm loans up to ₹2 lakh within 100 days"
   Bad:  "We will work for farmers' welfare"
2. If text is Tamil, translate promise_text_en to clear English; keep original Tamil in promise_text_ta.
3. If text is English, set promise_text_ta to null.
4. Include amount_mentioned only when a specific number/amount is stated (e.g., "₹1,000/month").
5. Include scheme_name only when the promise names a specific scheme.
6. approx_page: the page number(s) where this promise appears.

Output format — respond with ONLY a valid JSON array (no prose, no markdown fences):
[
  {
    "category": "<one of the 5 categories above>",
    "promise_text_en": "<English promise text>",
    "promise_text_ta": "<Tamil text or null>",
    "stance_vibe": "<one of 7 stances>",
    "amount_mentioned": "<string or null>",
    "scheme_name": "<string or null>",
    "approx_page": <integer>
  }
]
If no concrete promises found in this section, return [].
"""


def call_claude(client: Any, text_chunk: str, chunk_label: str, model: str) -> list[dict]:
    """Call Claude to extract promises from a text chunk. Returns list of raw promise dicts."""
    user_msg = f"Extract promises from the following manifesto section:\n\n{text_chunk}"
    try:
        response = client.messages.create(
            model=model,
            max_tokens=4096,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )
        raw = response.content[0].text.strip()
        # Strip markdown fences if Claude added them anyway
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        parsed = json.loads(raw)
        if not isinstance(parsed, list):
            print(f"    [{chunk_label}] Unexpected response type: {type(parsed)}")
            return []
        return parsed
    except json.JSONDecodeError as e:
        print(f"    [{chunk_label}] JSON parse error: {e}")
        return []
    except Exception as e:
        print(f"    [{chunk_label}] API error: {e}")
        return []


def validate_promise(raw: dict) -> dict | None:
    """Validate and normalise a raw promise dict from Claude."""
    cat = raw.get("category", "")
    if cat not in VALID_CATEGORIES:
        return None
    stance = raw.get("stance_vibe", "Welfare-centric")
    if stance not in VALID_STANCES:
        stance = "Welfare-centric"
    en = (raw.get("promise_text_en") or "").strip()
    if not en:
        return None
    return {
        "category":        cat,
        "promise_text_en": en,
        "promise_text_ta": (raw.get("promise_text_ta") or "").strip() or None,
        "stance_vibe":     stance,
        "amount_mentioned": raw.get("amount_mentioned") or None,
        "scheme_name":     raw.get("scheme_name") or None,
        "approx_page":     int(raw.get("approx_page", 0)),
    }


# ---------------------------------------------------------------------------
# Core extraction pipeline
# ---------------------------------------------------------------------------

def extract_promises(
    party_id: str,
    target_year: int,
    cfg: dict[str, Any],
    model: str,
    client: Any,
    keep_pdf: bool = False,
) -> list[dict]:
    print(f"\n── {party_id.upper()} {target_year} ─────────────────────────────")
    print(f"  PDF: {cfg['pdf_url']}")

    # 1. Download
    pdf_path = Path(tempfile.mkdtemp()) / f"{party_id}_{target_year}.pdf"
    try:
        download_pdf(cfg["pdf_url"], pdf_path)
    except Exception as e:
        print(f"  ERROR downloading PDF: {e}")
        return []

    # 2. Extract text
    try:
        pages = extract_pages(pdf_path)
    except Exception as e:
        print(f"  ERROR extracting text: {e}")
        return []

    if not pages:
        print("  WARNING: No text extracted (may be a scanned PDF requiring OCR)")
        return []

    print(f"  {len(pages)} pages with text extracted")

    # 3. Chunk pages and call Claude
    raw_promises: list[dict] = []
    chunks = [pages[i:i + PAGES_PER_CHUNK] for i in range(0, len(pages), PAGES_PER_CHUNK)]
    print(f"  {len(chunks)} chunks × {PAGES_PER_CHUNK} pages → Claude {model}")

    for ci, chunk in enumerate(chunks, start=1):
        page_nums = [p for p, _ in chunk]
        text = "\n\n---\n\n".join(f"[Page {p}]\n{t}" for p, t in chunk)
        label = f"chunk {ci}/{len(chunks)}, pages {page_nums[0]}–{page_nums[-1]}"
        print(f"  Processing {label} ({len(text)} chars)…", end=" ", flush=True)

        raw = call_claude(client, text, label, model)
        valid = [v for r in raw if (v := validate_promise(r)) is not None]
        print(f"{len(valid)} promises")
        raw_promises.extend(valid)

        # Polite rate limit (Haiku is generous but let's be safe)
        if ci < len(chunks):
            time.sleep(0.5)

    # 4. Assign stable doc_ids
    cat_counters: dict[str, int] = defaultdict(int)
    now = datetime.now(timezone.utc).isoformat()
    result = []
    for p in raw_promises:
        cat_abbrev = _CAT_ABBREV[p["category"]]
        cat_counters[cat_abbrev] += 1
        doc_id = f"{party_id}_{target_year}_{cat_abbrev}_{cat_counters[cat_abbrev]:03d}"
        source_note = cfg.get("source_note", f"LLM-extracted from OpenCity PDF · {cfg['pdf_url']}")

        record: dict[str, Any] = {
            "doc_id":               doc_id,
            "party_id":             party_id,
            "party_name":           cfg["party_name"],
            "party_color":          cfg["party_color"],
            "category":             p["category"],
            "promise_text_en":      p["promise_text_en"],
            "promise_text_ta":      p["promise_text_ta"] or "",
            "target_year":          target_year,
            "status":               "Proposed",
            "stance_vibe":          p["stance_vibe"],
            "amount_mentioned":     p["amount_mentioned"],
            "scheme_name":          p["scheme_name"],
            "manifesto_pdf_url":    cfg["pdf_url"],
            "manifesto_pdf_page":   p["approx_page"],
            "source_notes":         source_note,
            "ground_truth_confidence": cfg["confidence"],
            "is_joint_manifesto":   cfg["is_joint"],
            "track_fulfillment":    cfg["track_fulfillment"],
            "_extracted_at":        now,
        }
        result.append(record)

    print(f"  ✓ {len(result)} total promises extracted")

    if not keep_pdf:
        pdf_path.unlink(missing_ok=True)

    return result


# ---------------------------------------------------------------------------
# Firestore upload
# ---------------------------------------------------------------------------

def upload_to_firestore(records: list[dict], project_id: str, dry_run: bool) -> None:
    if dry_run:
        print(f"\n[DRY RUN] Would upload {len(records)} docs to `manifesto_promises`")
        return

    try:
        from google.cloud import firestore as fs
    except ImportError:
        print("ERROR: google-cloud-firestore not installed")
        return

    db = fs.Client(project=project_id)
    col = db.collection("manifesto_promises")
    written = 0
    for rec in records:
        doc_id = rec["doc_id"]
        col.document(doc_id).set(rec, merge=True)
        written += 1
    print(f"  Uploaded {written} docs to Firestore `manifesto_promises`")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Extract manifesto promises via Claude and load to Firestore")
    ap.add_argument("--year",    type=int, required=True,  help="Election year: 2026 or 2024")
    ap.add_argument("--party",   default=None,             help="Specific party_id (e.g. dmk). Omit for all.")
    ap.add_argument("--dry-run", action="store_true",      help="Extract only — don't upload to Firestore")
    ap.add_argument("--keep-pdf", action="store_true",     help="Keep downloaded PDFs in /tmp")
    ap.add_argument("--model",   default="claude-haiku-4-5-20251001",
                                                           help="Anthropic model to use")
    ap.add_argument("--output",  default=None,             help="Optional JSON output file path")
    args = ap.parse_args()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        sys.exit("ERROR: ANTHROPIC_API_KEY environment variable not set")

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
    except ImportError:
        sys.exit("ERROR: anthropic package not installed. Run: pip install anthropic")

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "naatunadappu")

    # Select sources for this run
    sources = {
        (pid, yr): cfg
        for (pid, yr), cfg in SOURCE_CONFIG.items()
        if yr == args.year and (args.party is None or pid == args.party)
    }

    if not sources:
        available = [f"{pid} {yr}" for (pid, yr) in SOURCE_CONFIG if yr == args.year]
        sys.exit(
            f"No sources match --year {args.year}"
            + (f" --party {args.party}" if args.party else "")
            + f"\nAvailable for {args.year}: {', '.join(available) if available else 'none'}"
        )

    print(f"Processing {len(sources)} source(s) for year {args.year}")
    print(f"Model: {args.model}")
    print(f"Mode:  {'DRY RUN' if args.dry_run else 'LIVE UPLOAD'}")

    all_records: list[dict] = []

    for (party_id, year), cfg in sources.items():
        records = extract_promises(
            party_id, year, cfg, args.model, client, keep_pdf=args.keep_pdf
        )
        all_records.extend(records)

        if records:
            # Save per-party JSON
            out_path = OUT_DIR / f"manifesto_promises_{year}_{party_id}.json"
            OUT_DIR.mkdir(parents=True, exist_ok=True)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            print(f"  Saved → {out_path.relative_to(ROOT_DIR)}")

    if not all_records:
        print("\nNo promises extracted — nothing to upload")
        return

    # Optional combined output
    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            json.dump(all_records, f, ensure_ascii=False, indent=2)
        print(f"\nCombined output → {args.output}")

    print(f"\nTotal extracted: {len(all_records)} promises across {len(sources)} parties")

    # Firestore
    upload_to_firestore(all_records, project_id, dry_run=args.dry_run)

    # Summary table
    from collections import Counter
    by_party = Counter(r["party_id"] for r in all_records)
    by_cat   = Counter(r["category"] for r in all_records)
    print("\n── Summary ──────────────────────────────")
    print("Party breakdown:")
    for pid, n in sorted(by_party.items()):
        print(f"  {pid:12s}  {n}")
    print("Category breakdown:")
    for cat, n in sorted(by_cat.items()):
        print(f"  {cat:25s}  {n}")


if __name__ == "__main__":
    main()
