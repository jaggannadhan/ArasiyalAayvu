"""TN 2026 Election Results Ingestion — ECI scraped data → Firestore.

Loads constituency-wise results from the browser-scraped JSON file
and stores them in Firestore collections:
  - election_results_2026/{constituency_slug} — per-constituency results
  - results_summary_2026/state — party-wise summary

Also matches winners to existing politician profiles from candidate-search-index.json
using constituency slug + fuzzy name matching.

Usage:
    python scrapers/results_ingest.py                          # ingest from default path
    python scrapers/results_ingest.py --file /path/to/data.json
    python scrapers/results_ingest.py --dry-run                # preview without writing
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "naatunadappu")

# Default paths
DEFAULT_RESULTS_FILE = ROOT / "data" / "processed" / "tn_results_2026.json"
PARTY_SUMMARY_FILE = ROOT / "data" / "processed" / "tn_results_2026_party_summary.json"

# Constituency map for name → slug matching
CONSTITUENCY_MAP_PATH = ROOT / "web" / "backend_api" / "constituency-map.json"

# Candidate search index for politician profile matching
CANDIDATE_INDEX_PATH = ROOT / "web" / "backend_api" / "candidate-search-index.json"


def _slugify(name: str) -> str:
    """Convert constituency name to URL-safe slug."""
    s = name.lower().strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s_]+", "_", s)
    return s


def _load_constituency_map() -> dict[str, str]:
    """Load constituency name → slug mapping from the existing map file."""
    if not CONSTITUENCY_MAP_PATH.exists():
        return {}
    with open(CONSTITUENCY_MAP_PATH, "r") as f:
        data = json.load(f)
    # Build a lookup: normalized name → slug
    lookup: dict[str, str] = {}
    for entry in data if isinstance(data, list) else data.values():
        if isinstance(entry, dict):
            name = entry.get("name", "") or entry.get("ac_name", "")
            slug = entry.get("slug", "") or _slugify(name)
            if name and slug:
                lookup[name.lower().strip()] = slug
                lookup[_slugify(name)] = slug
    return lookup


def _normalize_name(name: str) -> str:
    """Normalize a candidate name for comparison.

    ECI names like 'MUNIRATHINAM.J' → 'munirathinam j'
    Index names like 'Munirathinam J.' → 'munirathinam j'
    """
    s = name.lower().strip()
    # Remove dots, commas, extra spaces
    s = re.sub(r"[.,]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _name_similarity(a: str, b: str) -> float:
    """Fuzzy name similarity score (0-1)."""
    na, nb = _normalize_name(a), _normalize_name(b)
    if na == nb:
        return 1.0
    # Check if one contains the other (handles "K. Palaniswami" vs "Edappadi K. Palaniswami")
    if na in nb or nb in na:
        return 0.9
    # Check if last-name/first-name tokens overlap significantly
    tokens_a = set(na.split())
    tokens_b = set(nb.split())
    if tokens_a and tokens_b:
        overlap = len(tokens_a & tokens_b)
        # If the longer name's main tokens all appear in the other
        if overlap >= min(len(tokens_a), len(tokens_b)):
            return 0.85
    return SequenceMatcher(None, na, nb).ratio()


def _load_candidate_index() -> dict[str, list[dict]]:
    """Load candidate search index, grouped by constituency slug.

    Returns: {slug: [{n: name, p: party, s: slug}, ...]}
    """
    if not CANDIDATE_INDEX_PATH.exists():
        return {}
    with open(CANDIDATE_INDEX_PATH, "r") as f:
        entries = json.load(f)
    by_slug: dict[str, list[dict]] = {}
    for e in entries:
        slug = e.get("s", "")
        if slug:
            by_slug.setdefault(slug, []).append(e)
    return by_slug


def match_winner_to_profile(
    winner_name: str, winner_party: str, constituency_slug: str,
    candidate_index: dict[str, list[dict]],
) -> dict | None:
    """Match an election winner to an existing politician profile.

    Strategy:
      1. Filter candidates by constituency slug (exact match)
      2. Score each candidate by name similarity
      3. Return best match if score >= 0.7
    """
    candidates = candidate_index.get(constituency_slug, [])
    if not candidates:
        return None

    best_match = None
    best_score = 0.0

    for c in candidates:
        score = _name_similarity(winner_name, c["n"])
        # Boost score if party also matches
        if c.get("p", "").lower() == winner_party.lower():
            score = min(1.0, score + 0.05)
        if score > best_score:
            best_score = score
            best_match = c

    if best_match and best_score >= 0.7:
        return {"name": best_match["n"], "party": best_match["p"],
                "slug": best_match["s"], "score": round(best_score, 3)}
    return None


def ingest_results(results_file: Path, dry_run: bool = False) -> None:
    """Ingest constituency-wise results from scraped JSON into Firestore."""
    from google.cloud import firestore

    with open(results_file, "r") as f:
        data = json.load(f)

    results = data.get("results", [])
    print(f"=== TN 2026 Results Ingestion ===")
    print(f"  File: {results_file}")
    print(f"  Constituencies: {len(results)}")
    print(f"  Scraped at: {data.get('scraped_at', 'unknown')}")
    print()

    if not results:
        print("  No results found in file. Exiting.")
        return

    # Load constituency slug map
    slug_map = _load_constituency_map()

    # Load candidate index for profile matching
    candidate_index = _load_candidate_index()
    print(f"  Candidate index: {sum(len(v) for v in candidate_index.values())} entries across {len(candidate_index)} constituencies")

    db = firestore.Client(project=PROJECT) if not dry_run else None

    # Profile match stats
    matched_profiles = 0
    unmatched_profiles = []

    # Party-wise aggregation
    party_seats: dict[str, dict] = {}
    total_declared = 0
    total_votes_all = 0

    batch = db.batch() if db else None
    batch_count = 0

    for r in results:
        if r.get("error"):
            print(f"  ⚠ AC-{r['ac_no']}: {r['error']} — skipping")
            continue

        ac_no = r["ac_no"]
        ac_name = r.get("ac_name", f"AC-{ac_no}")
        winner = r.get("winner")
        runner_up = r.get("runner_up")
        candidates = r.get("candidates", [])
        margin = r.get("margin", 0)
        total_votes = r.get("total_votes", 0)

        # Find slug
        slug = slug_map.get(ac_name.lower().strip()) or slug_map.get(_slugify(ac_name)) or _slugify(ac_name)

        # Determine status
        status = "declared" if winner else "counting"
        if winner:
            total_declared += 1

        # Aggregate party seats
        if winner:
            party = winner.get("party", "IND")
            if party not in party_seats:
                party_seats[party] = {"won": 0, "votes": 0}
            party_seats[party]["won"] += 1
            party_seats[party]["votes"] += winner.get("votes", 0)

        total_votes_all += total_votes

        # Match winner to existing politician profile
        profile_match = None
        if winner:
            profile_match = match_winner_to_profile(
                winner["name"], winner.get("party", ""), slug, candidate_index
            )
            if profile_match:
                matched_profiles += 1
            else:
                unmatched_profiles.append(f"AC-{ac_no} {ac_name}: {winner['name']} ({winner.get('party', '')})")

        doc_data = {
            "ac_no": ac_no,
            "ac_name": ac_name,
            "slug": slug,
            "status": status,
            "winner": winner,
            "runner_up": runner_up,
            "margin": margin,
            "total_votes": total_votes,
            "candidates": candidates[:10],  # top 10 candidates
            "total_candidates": len(candidates),
            "profile_match": profile_match,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

        if dry_run:
            w = f"{winner['name']} ({winner['party']})" if winner else "?"
            pm = f" [✓ profile]" if profile_match else " [✗ no profile]" if winner else ""
            print(f"  AC-{ac_no:3d} {ac_name:30s} → {w:40s} margin: {margin:>6,}{pm}")
        else:
            ref = db.collection("election_results_2026").document(slug)
            batch.set(ref, doc_data)
            batch_count += 1

            if batch_count >= 400:
                batch.commit()
                batch = db.batch()
                batch_count = 0

    # Commit remaining
    if batch and batch_count > 0:
        batch.commit()

    print(f"\n  Declared: {total_declared}/234")
    print(f"  Total votes: {total_votes_all:,}")
    print(f"  Profile matches: {matched_profiles}/{total_declared}")
    if unmatched_profiles:
        print(f"\n  Unmatched winners ({len(unmatched_profiles)}):")
        for u in unmatched_profiles[:20]:
            print(f"    {u}")
        if len(unmatched_profiles) > 20:
            print(f"    ... and {len(unmatched_profiles) - 20} more")
    print()

    # Party-wise summary
    print("  Party-wise seats:")
    party_summary = []
    for party, data in sorted(party_seats.items(), key=lambda x: -x[1]["won"]):
        print(f"    {party:10s}: {data['won']:3d} seats | {data['votes']:>10,} votes")
        party_summary.append({
            "party": party,
            "won": data["won"],
            "votes": data["votes"],
        })

    # Also load the ECI party summary if available
    if PARTY_SUMMARY_FILE.exists():
        with open(PARTY_SUMMARY_FILE, "r") as f:
            eci_summary = json.load(f)
    else:
        eci_summary = {"parties": []}

    # Store summary
    summary_doc = {
        "state": "Tamil Nadu",
        "year": 2026,
        "total_seats": 234,
        "total_declared": total_declared,
        "total_votes": total_votes_all,
        "majority_mark": 118,
        "party_wise": party_summary,
        "eci_party_wise": eci_summary.get("parties", []),
        "scraped_at": data.get("scraped_at", ""),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }

    if dry_run:
        print(f"\n  [DRY RUN] Would write summary to results_summary_2026/state")
    else:
        db.collection("results_summary_2026").document("state").set(summary_doc)
        print(f"\n  ✅ Summary written to results_summary_2026/state")

    # Update candidate_accountability_2026 with winners
    if not dry_run:
        print("\n  Updating candidate_accountability_2026 with winners...")
        winners_updated = 0
        batch = db.batch()
        batch_count = 0

        for r in results:
            if r.get("error") or not r.get("winner"):
                continue

            winner = r["winner"]
            ac_name = r.get("ac_name", "")
            slug = slug_map.get(ac_name.lower().strip()) or slug_map.get(_slugify(ac_name)) or _slugify(ac_name)

            # Match winner to profile
            profile = match_winner_to_profile(
                winner["name"], winner.get("party", ""), slug, candidate_index
            )

            accountability_doc = {
                "name": winner.get("name", ""),
                "party": winner.get("party", ""),
                "constituency": ac_name,
                "constituency_slug": slug,
                "ac_no": r["ac_no"],
                "votes": winner.get("votes", 0),
                "margin": r.get("margin", 0),
                "total_votes": r.get("total_votes", 0),
                "photo_url": winner.get("photo_url", ""),
                "elected_on": "2026-05-04",
                "profile_match": profile,
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }

            ref = db.collection("candidate_accountability_2026").document(slug)
            batch.set(ref, accountability_doc)
            batch_count += 1
            winners_updated += 1

            if batch_count >= 400:
                batch.commit()
                batch = db.batch()
                batch_count = 0

        if batch_count > 0:
            batch.commit()

        print(f"  ✅ {winners_updated} winners written to candidate_accountability_2026")

    print(f"\n=== Done ===")


def main():
    parser = argparse.ArgumentParser(description="TN 2026 Election Results Ingestion")
    parser.add_argument("--file", type=str, default=None, help="Path to scraped results JSON")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing to Firestore")
    args = parser.parse_args()

    from dotenv import load_dotenv
    env_path = ROOT / "web" / ".env.local"
    if env_path.exists():
        load_dotenv(env_path)

    results_file = Path(args.file) if args.file else DEFAULT_RESULTS_FILE

    # Also check common download locations
    if not results_file.exists():
        alt_paths = [
            Path.home() / "Downloads" / "tn_results_2026.json",
            Path.home() / "Downloads" / "ArasiyalAavyuDB" / "tn_results_2026.json",
        ]
        for alt in alt_paths:
            if alt.exists():
                results_file = alt
                break

    if not results_file.exists():
        print(f"ERROR: Results file not found: {results_file}")
        print("  Run the browser console script on results.eci.gov.in first.")
        print("  Or specify --file /path/to/tn_results_2026.json")
        sys.exit(1)

    ingest_results(results_file, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
