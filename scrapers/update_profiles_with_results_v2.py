"""Update politician_profile 2026 timeline entries with election results — V2.

Improved matching strategy:
1. Build a lookup from constituency_slug → all result candidates
2. Handle _sc/_st suffix mismatches
3. For each profile with a 2026 entry, match by constituency + relaxed name matching
4. For winners still unmatched, create/update profiles

Usage:
    python scrapers/update_profiles_with_results_v2.py --dry-run
    python scrapers/update_profiles_with_results_v2.py
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
    s = re.sub(r"[.,;:()\[\]]", " ", s)
    s = re.sub(r"\b(dr|mr|mrs|ms|sri|smt|thiru|selvi)\b", "", s)
    return re.sub(r"\s+", " ", s).strip()


def _slugify(name: str) -> str:
    s = name.lower().strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s_]+", "_", s)
    return s


def _name_tokens(name: str) -> set[str]:
    """Extract meaningful name tokens (length > 1)."""
    return {t for t in _normalize(name).split() if len(t) > 1}


def _name_match(profile_name: str, eci_name: str) -> float:
    """Relaxed name matching — handles initial-heavy ECI names."""
    na, nb = _normalize(profile_name), _normalize(eci_name)
    if na == nb:
        return 1.0

    # Token overlap — main matching signal
    tokens_a = _name_tokens(profile_name)
    tokens_b = _name_tokens(eci_name)
    if tokens_a and tokens_b:
        overlap = len(tokens_a & tokens_b)
        shorter = min(len(tokens_a), len(tokens_b))
        if overlap >= shorter and shorter >= 1:
            return 0.90 if overlap >= 2 else 0.80

    # Subsequence check
    if na in nb or nb in na:
        return 0.85

    return SequenceMatcher(None, na, nb).ratio()


SLUG_ALIASES: dict[str, str] = {
    "aruppukottai": "aruppukkottai",
    "bodinayakkanur": "bodinayakanur",
    "madhavaram": "madavaram",
    "madhuravoyal": "maduravoyal",
    "mettupalayam": "mettuppalayam",
    "mudukulathur": "mudhukulathur",
    "palacode": "palacodu",
    "pappireddipatti": "pappireddippatti",
    "paramathivelur": "paramathi_velur",
    "poonamallee": "poonmallae",
    "sholinganallur": "shozhinganallur",
    "sholinghur": "sholingur",
    "thally": "thalli",
    "thiruvaur": "thiruvarur",
    "thoothukudi": "thoothukkudi",
    "tiruppathur": "tiruppathur",
    "vedharanyam": "vedaranyam",
    "vridhachalam": "vriddhachalam",
    "villupuram": "viluppuram",
    "gandarvakottai": "gandharvakottai",
    "dr_radhakrishnan_nagar": "drradhakrishnan_nagar",
    "chepauk_thiruvallikeni": "chepauk-thiruvallikeni",
    "paramathi_velur": "paramathi-velur",
    "thiru_vi_ka_nagar": "thiru-vi-ka-nagar",
}


def _slug_variants(slug: str) -> list[str]:
    """Generate slug variants: original, without _sc/_st, aliased, hyphen/underscore swaps."""
    variants = [slug]

    # Strip reservation suffixes
    for suffix in ("_sc", "_st", "_gen"):
        if slug.endswith(suffix):
            variants.append(slug[: -len(suffix)])

    # Add known aliases
    for base in list(variants):
        if base in SLUG_ALIASES:
            variants.append(SLUG_ALIASES[base])

    # Try hyphen ↔ underscore swap
    for v in list(variants):
        variants.append(v.replace("-", "_"))
        variants.append(v.replace("_", "-"))

    return list(dict.fromkeys(variants))  # dedupe preserving order


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    from dotenv import load_dotenv
    env_path = ROOT / "web" / ".env.local"
    if env_path.exists():
        load_dotenv(env_path)

    from google.cloud import firestore
    db = firestore.Client(project=os.environ.get("GOOGLE_CLOUD_PROJECT", "naatunadappu"))

    # 1. Load full results
    with open(RESULTS_FILE) as f:
        results_data = json.load(f)

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

    # Build: slug -> {candidates, total_votes, ac_name, winner_name}
    results_by_slug: dict[str, dict] = {}
    for r in results_data["results"]:
        if r.get("error"):
            continue
        ac_name = r.get("ac_name", "")
        slug = slug_map.get(ac_name.lower().strip()) or slug_map.get(_slugify(ac_name)) or _slugify(ac_name)
        results_by_slug[slug] = {
            "candidates": r.get("candidates", []),
            "total_votes": r.get("total_votes", 0),
            "ac_name": ac_name,
            "winner": r.get("winner"),
        }

    print(f"Loaded results for {len(results_by_slug)} constituencies")

    # 2. Scan profiles and update
    total_2026 = 0
    updated = 0
    still_null = 0

    batch = db.batch()
    batch_count = 0

    for doc in db.collection("politician_profile").stream():
        profile = doc.to_dict()
        timeline = profile.get("timeline", [])
        modified = False

        for i, entry in enumerate(timeline):
            if entry.get("year") != 2026:
                continue
            # Skip already-matched entries
            if entry.get("won") is not None:
                total_2026 += 1
                continue

            total_2026 += 1
            c_slug = entry.get("constituency_slug", "")
            profile_name = profile.get("canonical_name", "")

            # Try slug variants (with/without _sc, _st)
            cands = []
            total_votes = 0
            for variant in _slug_variants(c_slug):
                if variant in results_by_slug:
                    cands = results_by_slug[variant]["candidates"]
                    total_votes = results_by_slug[variant]["total_votes"]
                    break

            if not cands:
                still_null += 1
                continue

            # Match by name
            best_match = None
            best_score = 0.0
            for rc in cands:
                score = _name_match(profile_name, rc.get("name", ""))
                if score > best_score:
                    best_score = score
                    best_match = rc

            if best_match and best_score >= 0.55:
                won = best_match.get("status") == "won"
                votes = best_match.get("votes", 0)

                timeline[i]["won"] = won
                timeline[i]["votes"] = votes
                if total_votes > 0:
                    timeline[i]["vote_share_pct"] = round((votes / total_votes) * 100, 1)
                eci_photo = best_match.get("photo_url", "")
                if eci_photo:
                    timeline[i]["eci_photo_url"] = eci_photo
                modified = True
            else:
                still_null += 1

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

    print(f"\n=== Phase 1: Update existing profiles ===")
    print(f"  Profiles with 2026 entries: {total_2026}")
    print(f"  Newly matched & updated:    {updated}")
    print(f"  Still unmatched:            {still_null}")

    # 3. Check winners coverage — find winners NOT in any profile
    print(f"\n=== Phase 2: Ensure all winners have profiles ===")
    # Reload profiles to get current state
    profile_winners: set[str] = set()  # constituency slugs where a profile has won=true for 2026
    for doc in db.collection("politician_profile").stream():
        for t in doc.to_dict().get("timeline", []):
            if t.get("year") == 2026 and t.get("won") is True:
                cs = t.get("constituency_slug", "")
                for v in _slug_variants(cs):
                    profile_winners.add(v)

    winners_missing = []
    for slug, r in results_by_slug.items():
        if slug in profile_winners:
            continue
        winner = r.get("winner")
        if winner:
            winners_missing.append((slug, r["ac_name"], winner))

    print(f"  Winners in profiles: {len(results_by_slug) - len(winners_missing)}/234")
    print(f"  Winners MISSING:     {len(winners_missing)}")

    if winners_missing:
        print(f"\n  Missing winners:")
        for slug, ac_name, w in winners_missing[:30]:
            print(f"    {slug:30s} {w['name']:35s} ({w.get('party', '')})")
        if len(winners_missing) > 30:
            print(f"    ... and {len(winners_missing) - 30} more")

    print(f"\n=== Done ===")


if __name__ == "__main__":
    main()
