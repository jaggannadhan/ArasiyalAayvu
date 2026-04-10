"""
Manifesto Deep Enrich — One-time causal chain analysis
=======================================================
Reads existing extracted manifesto JSON files, sends promises to Claude in
batches to fill in the 6 deep analysis fields, then saves back to JSON and
optionally uploads to Firestore with merge=True (updates only these fields).

Run ONCE per party. Skips already-enriched promises (idempotent).
Re-run only when explicitly asked — these fields are expensive to generate.

Usage
-----
  python scrapers/manifesto_deep_enrich.py --year 2026 --dry-run
  python scrapers/manifesto_deep_enrich.py --year 2026 --party dmk
  python scrapers/manifesto_deep_enrich.py --year 2026
  python scrapers/manifesto_deep_enrich.py --year 2026 --force   # re-enrich all

Deep analysis fields populated
-------------------------------
  impact_mechanism      — HOW the promise creates tangible change
  first_order_effect    — immediate direct result if implemented
  second_order_effect   — downstream consequence
  third_order_effect    — systemic / long-term effect (or null)
  implementation_risk   — "low" | "medium" | "high"
  root_cause_addressed  — true | false
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
OUT_DIR  = ROOT_DIR / "data" / "processed"
NUMBEO_CACHE_DIR = ROOT_DIR / "data" / "raw" / "numbeo"
PROGRESS_FILE = Path(__file__).parent / ".manifesto_enrich_progress.json"

ENRICH_BATCH_SIZE = 8   # promises per API call (keeps prompt manageable)
VALID_IMPLEMENTATION_RISK = {"low", "medium", "high"}

# ---------------------------------------------------------------------------
# Progress tracking — resume after interruption
# ---------------------------------------------------------------------------

def load_progress() -> set[str]:
    """Return set of doc_ids already successfully enriched."""
    if PROGRESS_FILE.exists():
        try:
            with open(PROGRESS_FILE) as f:
                return set(json.load(f))
        except (json.JSONDecodeError, TypeError):
            pass
    return set()


def save_progress(done: set[str]) -> None:
    with open(PROGRESS_FILE, "w") as f:
        json.dump(sorted(done), f)


# ---------------------------------------------------------------------------
# Context helpers (reuse CoL + budget from ingest module)
# ---------------------------------------------------------------------------

def _load_col_context() -> str:
    """Load the most recent Numbeo CoL snapshot for context (no network call)."""
    cache_files = sorted(NUMBEO_CACHE_DIR.glob("col_tn_*.json"), reverse=True)
    if not cache_files:
        return "(CoL data unavailable — no cache found)"
    with open(cache_files[0]) as f:
        col = json.load(f)
    cities = col.get("cities", {})
    col_date = col.get("fetched_on", "unknown")
    lines = [f"Cost of Living — TN cities (Numbeo, {col_date}):"]
    for city in ["Chennai", "Coimbatore", "Madurai"]:
        d = cities.get(city, {})
        sal = d.get("avg_monthly_net_salary_inr")
        lines.append(f"  {city}: avg salary ₹{sal:,.0f}/mo" if sal else f"  {city}: (no salary data)")
    return "\n".join(lines)


def _load_budget_context() -> str:
    for fname in ["state_budgets_TN.json", "state_budgets.json"]:
        p = OUT_DIR / fname
        if not p.exists():
            continue
        try:
            records = json.load(open(p))
            tn = sorted(
                [r for r in records if r.get("state_code") == "TN"],
                key=lambda r: r.get("fiscal_year", ""),
                reverse=True,
            )
            if tn:
                b = tn[0]
                fy = b.get("fiscal_year", "2024-25")
                exp = b.get("expenditure", {})
                com = b.get("committed", {})
                rev_exp = exp.get("revenue_exp_cr", 0) or 0
                committed = com.get("total_committed_cr", 0) or 0
                disc = rev_exp - committed
                deficit = b.get("fiscal", {}).get("fiscal_deficit_cr", 0) or 0
                return (
                    f"TN Budget {fy}: revenue exp ₹{rev_exp:,.0f} cr, "
                    f"committed ₹{committed:,.0f} cr ({committed/rev_exp*100:.0f}% of rev exp), "
                    f"discretionary ~₹{disc:,.0f} cr, fiscal deficit ₹{deficit:,.0f} cr."
                )
        except Exception:
            continue
    return "TN Budget 2024-25: ~55% revenue exp is committed (salaries+pensions+interest); discretionary ~₹1.3 lakh cr."


def build_enrich_system_prompt() -> str:
    col_ctx    = _load_col_context()
    budget_ctx = _load_budget_context()
    return f"""\
You are a Tamil Nadu governance and public policy expert assessing the real-world impact
of election promises. You will be given a batch of manifesto promises and must provide
a 2-3 level causal chain analysis for EACH promise.

━━━ ECONOMIC CONTEXT ━━━
{budget_ctx}

{col_ctx}

TN Social Baseline: Population ~7.9 crore, ration-card households ~2.0 crore,
BPL households ~1.2 crore, MGNREGS wage ₹294/day (2024-25), female LFPR 33% urban / 51% rural.

━━━ TASK ━━━
For each promise in the input JSON array, provide:

impact_mechanism — one sentence: HOW this promise creates tangible change for its target group.
  Name the mechanism explicitly (income transfer / debt relief / access to service / skill building /
  price subsidy / infrastructure provision / legal protection, etc.)

first_order_effect — the immediate, direct outcome if this promise is implemented as stated.
  Include scale where knowable (e.g. "~10 lakh farmers", "1.37 crore women HoH").

second_order_effect — the downstream consequence that flows from the first-order effect.
  This must be a CAUSAL consequence, not a restatement.

third_order_effect — long-term systemic change enabled by second-order effect.
  Set to null if the causal chain doesn't extend meaningfully this far.

implementation_risk — likelihood this promise is NOT fully delivered within a 5-year term:
  "low"    → simple administrative action, within TN capacity, no major blockers
  "medium" → requires inter-department coordination, new legislation, or central alignment
  "high"   → significant financial barrier, past similar promises failed, or politically contested

root_cause_addressed:
  true  — promise targets a structural driver of the problem (income floor, land tenure, debt trap,
          access to education / healthcare at point of need)
  false — addresses a symptom or provides temporary relief without changing underlying conditions

━━━ OUTPUT FORMAT ━━━
Return ONLY a valid JSON array with one object per input promise, in the SAME ORDER:
[
  {{
    "index": <integer matching input array index, 0-based>,
    "impact_mechanism": "<one sentence>",
    "first_order_effect": "<immediate direct result>",
    "second_order_effect": "<downstream consequence>",
    "third_order_effect": "<systemic effect or null>",
    "implementation_risk": "<low|medium|high>",
    "root_cause_addressed": <true|false>
  }}
]
No prose, no markdown fences. Return exactly {ENRICH_BATCH_SIZE} objects (or fewer if the batch is smaller).
"""


# ---------------------------------------------------------------------------
# Claude call
# ---------------------------------------------------------------------------

def enrich_batch(
    client: Any,
    system_prompt: str,
    batch: list[dict],
    model: str,
    batch_label: str,
) -> list[dict | None]:
    """Return list of enrichment dicts (indexed by batch position), None on failure."""
    # Minimal representation — just what Claude needs for context
    input_items = [
        {
            "index": i,
            "promise": p["promise_text_en"],
            "category": p["category"],
            "amount": p.get("amount_mentioned"),
            "beneficiary_coverage": p.get("beneficiary_coverage"),
            "impact_depth": p.get("impact_depth"),
        }
        for i, p in enumerate(batch)
    ]
    user_msg = f"Analyse these {len(batch)} manifesto promises:\n\n{json.dumps(input_items, ensure_ascii=False, indent=2)}"

    try:
        response = client.messages.create(
            model=model,
            max_tokens=4096,
            system=system_prompt,
            messages=[{"role": "user", "content": user_msg}],
        )
        raw = response.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        # Use raw_decode so trailing text after the JSON array is ignored
        decoder = json.JSONDecoder()
        # Find first '[' to handle any leading prose
        start = raw.find("[")
        if start < 0:
            print(f"    [{batch_label}] No JSON array found in response")
            return [None] * len(batch)
        parsed, _ = decoder.raw_decode(raw, start)
        if not isinstance(parsed, list):
            print(f"    [{batch_label}] Unexpected type: {type(parsed)}")
            return [None] * len(batch)

        # Map index → result
        by_index: dict[int, dict] = {item["index"]: item for item in parsed if isinstance(item, dict) and "index" in item}
        return [by_index.get(i) for i in range(len(batch))]

    except json.JSONDecodeError as e:
        print(f"    [{batch_label}] JSON parse error: {e}")
        return [None] * len(batch)
    except Exception as e:
        print(f"    [{batch_label}] API error: {e}")
        return [None] * len(batch)


def _validate_enrich(raw: dict | None) -> dict | None:
    if not isinstance(raw, dict):
        return None

    def _cs(k: str) -> str | None:
        v = raw.get(k)
        return v.strip() or None if isinstance(v, str) else None

    risk = raw.get("implementation_risk")
    if risk not in VALID_IMPLEMENTATION_RISK:
        risk = None

    root = raw.get("root_cause_addressed")
    if not isinstance(root, bool):
        root = None

    return {
        "impact_mechanism":   _cs("impact_mechanism"),
        "first_order_effect": _cs("first_order_effect"),
        "second_order_effect":_cs("second_order_effect"),
        "third_order_effect": _cs("third_order_effect"),
        "implementation_risk":risk,
        "root_cause_addressed": root,
    }


# ---------------------------------------------------------------------------
# Per-party enrichment
# ---------------------------------------------------------------------------

def enrich_party(
    party_id: str,
    year: int,
    client: Any,
    model: str,
    force: bool,
    dry_run: bool,
    done: set[str],
) -> list[dict]:
    json_path = OUT_DIR / f"manifesto_promises_{year}_{party_id}.json"
    if not json_path.exists():
        print(f"  [{party_id}] JSON not found: {json_path} — run manifesto_ingest.py first")
        return []

    with open(json_path, encoding="utf-8") as f:
        promises = json.load(f)

    # Determine which need enrichment
    to_enrich = [
        p for p in promises
        if force or not p.get("impact_mechanism") and p["doc_id"] not in done
    ]

    print(f"\n── {party_id.upper()} {year} ─────────────────────────────")
    print(f"  {len(promises)} total promises, {len(to_enrich)} need deep analysis")

    if not to_enrich:
        print("  ✓ Already fully enriched — skipping")
        return promises

    system_prompt = build_enrich_system_prompt()
    batches = [to_enrich[i:i + ENRICH_BATCH_SIZE] for i in range(0, len(to_enrich), ENRICH_BATCH_SIZE)]
    print(f"  {len(batches)} batches × {ENRICH_BATCH_SIZE} → Claude {model}")

    enriched_map: dict[str, dict] = {}  # doc_id → enrichment fields

    for bi, batch in enumerate(batches, start=1):
        label = f"batch {bi}/{len(batches)}"
        print(f"  Processing {label} ({len(batch)} promises)…", end=" ", flush=True)

        if dry_run:
            print("[DRY RUN — skipped]")
            continue

        results = enrich_batch(client, system_prompt, batch, model, label)
        success = 0
        for p, raw_result in zip(batch, results):
            validated = _validate_enrich(raw_result)
            if validated:
                enriched_map[p["doc_id"]] = validated
                done.add(p["doc_id"])
                success += 1
        print(f"{success}/{len(batch)} ok")
        save_progress(done)

        if bi < len(batches):
            time.sleep(0.5)

    if dry_run:
        print("  [DRY RUN] No changes written")
        return promises

    # Merge enrichment back into promise list
    for p in promises:
        if p["doc_id"] in enriched_map:
            p.update(enriched_map[p["doc_id"]])

    # Save enriched JSON
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(promises, f, ensure_ascii=False, indent=2)
    print(f"  Saved → {json_path.relative_to(ROOT_DIR)}")

    return promises


# ---------------------------------------------------------------------------
# Firestore upload — merge=True so only deep analysis fields are updated
# ---------------------------------------------------------------------------

def upload_enrichment_to_firestore(
    all_promises: list[dict],
    project_id: str,
    dry_run: bool,
) -> None:
    if dry_run:
        print(f"\n[DRY RUN] Would update {len(all_promises)} docs in `manifesto_promises`")
        return

    DEEP_FIELDS = [
        "impact_mechanism", "first_order_effect", "second_order_effect",
        "third_order_effect", "implementation_risk", "root_cause_addressed",
    ]

    try:
        from google.cloud import firestore as fs
    except ImportError:
        print("ERROR: google-cloud-firestore not installed")
        return

    db = fs.Client(project=project_id)
    col = db.collection("manifesto_promises")

    updated = skipped = 0
    for p in all_promises:
        patch = {k: p.get(k) for k in DEEP_FIELDS}
        # Only upload if at least one field was filled
        if any(v is not None for v in patch.values()):
            col.document(p["doc_id"]).set(patch, merge=True)
            updated += 1
        else:
            skipped += 1

    print(f"  Firestore: {updated} docs updated, {skipped} skipped (no enrichment data)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Deep-enrich manifesto promises with causal chain analysis (run once)")
    ap.add_argument("--year",    type=int, required=True, help="Election year (2026)")
    ap.add_argument("--party",   default=None,            help="Specific party_id (e.g. dmk). Omit for all.")
    ap.add_argument("--dry-run", action="store_true",     help="Analyse only — don't write files or upload")
    ap.add_argument("--force",   action="store_true",     help="Re-enrich already-enriched promises")
    ap.add_argument("--model",   default="claude-haiku-4-5-20251001",
                                                          help="Anthropic model to use")
    args = ap.parse_args()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        sys.exit("ERROR: ANTHROPIC_API_KEY not set")

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
    except ImportError:
        sys.exit("ERROR: pip install anthropic")

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "naatunadappu")

    # Determine which parties to process
    all_json = sorted(OUT_DIR.glob(f"manifesto_promises_{args.year}_*.json"))
    if not all_json:
        sys.exit(f"No manifesto JSON files found for year {args.year} in {OUT_DIR}")

    party_ids = [p.stem.split("_")[-1] for p in all_json]
    if args.party:
        if args.party not in party_ids:
            sys.exit(f"Party '{args.party}' not found. Available: {', '.join(party_ids)}")
        party_ids = [args.party]

    print(f"Deep enrich: {', '.join(party_ids)} ({args.year})")
    print(f"Model: {args.model}")
    print(f"Mode:  {'DRY RUN' if args.dry_run else 'LIVE'}{' [FORCE]' if args.force else ''}")

    if args.force:
        done: set[str] = set()
        if PROGRESS_FILE.exists():
            PROGRESS_FILE.unlink()
    else:
        done = load_progress()
        if done:
            print(f"Resuming: {len(done)} doc_ids already in progress cache")

    all_enriched: list[dict] = []
    for pid in party_ids:
        enriched = enrich_party(pid, args.year, client, args.model, args.force, args.dry_run, done)
        all_enriched.extend(enriched)

    if not args.dry_run and all_enriched:
        upload_enrichment_to_firestore(all_enriched, project_id, dry_run=False)

    # Summary
    has_deep = sum(1 for p in all_enriched if p.get("impact_mechanism"))
    print(f"\n── Done ──────────────────────────────────")
    print(f"Total promises: {len(all_enriched)}, enriched: {has_deep}")
    if args.dry_run:
        print("(DRY RUN — no files written, no Firestore upload)")


if __name__ == "__main__":
    main()
