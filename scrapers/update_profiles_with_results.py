"""Update politician_profile 2026 timeline entries with actual election results.

Reads the full results JSON (all candidates, not just top 10 in Firestore),
matches each profile's 2026 timeline entry by constituency_slug + name similarity,
and updates won=true/false, votes, vote_share_pct.

Usage:
    python scrapers/update_profiles_with_results.py              # update all
    python scrapers/update_profiles_with_results.py --dry-run    # preview
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from difflib import SequenceMatcher
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

RESULTS_FILE = ROOT / "data" / "processed" / "tn_results_2026.json"


def _normalize(name: str) -> str:
    s = name.lower().strip()
    s = re.sub(r"[.,]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _slugify(name: str) -> str:
    s = name.lower().strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s_]+", "_", s)
    return s


def _name_sim(a: str, b: str) -> float:
    na, nb = _normalize(a), _normalize(b)
    if na == nb:
        return 1.0
    if na in nb or nb in na:
        return 0.9
    tokens_a, tokens_b = set(na.split()), set(nb.split())
    if tokens_a and tokens_b:
        overlap = len(tokens_a & tokens_b)
        if overlap >= min(len(tokens_a), len(tokens_b)):
            return 0.85
    return SequenceMatcher(None, na, nb).ratio()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    from dotenv import load_dotenv
    env_path = ROOT / "web" / ".env.local"
    if env_path.exists():
        load_dotenv(env_path)

    # 1. Load full results from JSON (has ALL candidates, not just top 10)
    with open(RESULTS_FILE) as f:
        results_data = json.load(f)

    # Build lookup: constituency_slug -> {candidates, total_votes}
    # We need to compute slugs the same way as the ingestion script
    from google.cloud import firestore
    db = firestore.Client(project=os.environ.get("GOOGLE_CLOUD_PROJECT", "naatunadappu"))

    # Load constituency map for proper slug resolution
    cmap_path = ROOT / "web" / "backend_api" / "constituency-map.json"
    slug_map: dict[str, str] = {}
    if cmap_path.exists():
        with open(cmap_path) as f:
            cmap = json.load(f)
        for entry in cmap if isinstance(cmap, list) else cmap.values():
            if isinstance(entry, dict):
                name = entry.get("name", "") or entry.get("ac_name", "")
                slug = entry.get("slug", "") or _slugify(name)
                if name and slug:
                    slug_map[name.lower().strip()] = slug
                    slug_map[_slugify(name)] = slug

    candidates_by_slug: dict[str, list[dict]] = {}
    total_votes_by_slug: dict[str, int] = {}

    for r in results_data["results"]:
        if r.get("error"):
            continue
        ac_name = r.get("ac_name", "")
        slug = slug_map.get(ac_name.lower().strip()) or slug_map.get(_slugify(ac_name)) or _slugify(ac_name)
        candidates_by_slug[slug] = r.get("candidates", [])
        total_votes_by_slug[slug] = r.get("total_votes", 0)

    print(f"Loaded results for {len(candidates_by_slug)} constituencies (full candidate lists)")

    # 2. Scan all politician profiles
    print("Scanning politician profiles...")
    total_2026 = 0
    matched = 0
    updated = 0
    unmatched: list[str] = []

    batch = db.batch()
    batch_count = 0

    for doc in db.collection("politician_profile").stream():
        profile = doc.to_dict()
        timeline = profile.get("timeline", [])
        modified = False

        for i, entry in enumerate(timeline):
            if entry.get("year") != 2026:
                continue

            total_2026 += 1
            c_slug = entry.get("constituency_slug", "")
            profile_name = profile.get("canonical_name", "")

            cands = candidates_by_slug.get(c_slug, [])
            total_votes = total_votes_by_slug.get(c_slug, 0)
            # Try without _sc suffix (reserved constituencies)
            if not cands and c_slug.endswith("_sc"):
                alt_slug = c_slug[:-3]
                cands = candidates_by_slug.get(alt_slug, [])
                total_votes = total_votes_by_slug.get(alt_slug, 0)
            if not cands:
                unmatched.append(f"{profile_name} @ {c_slug} (no results for slug)")
                continue

            # Match by name
            best_match = None
            best_score = 0.0
            for rc in cands:
                score = _name_sim(profile_name, rc.get("name", ""))
                if score > best_score:
                    best_score = score
                    best_match = rc

            if best_match and best_score >= 0.60:
                matched += 1
                won = best_match.get("status") == "won"
                votes = best_match.get("votes", 0)

                timeline[i]["won"] = won
                timeline[i]["votes"] = votes
                if total_votes > 0:
                    timeline[i]["vote_share_pct"] = round((votes / total_votes) * 100, 1)
                # Add ECI photo if profile doesn't have one
                eci_photo = best_match.get("photo_url", "")
                if eci_photo:
                    timeline[i]["eci_photo_url"] = eci_photo
                modified = True
            else:
                unmatched.append(f"{profile_name} @ {c_slug} (best={best_score:.2f})")

        if modified:
            wins = sum(1 for t in timeline if t.get("won") is True)
            losses = sum(1 for t in timeline if t.get("won") is False)

            update_data = {
                "timeline": timeline,
                "win_count": wins,
                "loss_count": losses,
                "total_contested": len(timeline),
            }

            sorted_tl = sorted(timeline, key=lambda t: t.get("year") or 0, reverse=True)
            if sorted_tl:
                latest = sorted_tl[0]
                update_data["current_party"] = latest.get("party")
                update_data["current_constituency"] = latest.get("constituency")
                update_data["current_constituency_slug"] = latest.get("constituency_slug")

            if not args.dry_run:
                batch.update(db.collection("politician_profile").document(doc.id), update_data)
                batch_count += 1
                updated += 1

                if batch_count >= 400:
                    batch.commit()
                    batch = db.batch()
                    batch_count = 0

    if not args.dry_run and batch_count > 0:
        batch.commit()

    print(f"\n=== Results ===")
    print(f"  Profiles with 2026 entries: {total_2026}")
    print(f"  Matched to results: {matched}")
    print(f"  Updated in Firestore: {updated}")
    print(f"  Unmatched: {len(unmatched)}")
    if unmatched:
        print(f"\n  Sample unmatched ({min(15, len(unmatched))}):")
        for u in unmatched[:15]:
            print(f"    {u}")
    print("=== Done ===")


if __name__ == "__main__":
    main()
