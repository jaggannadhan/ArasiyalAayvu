"""Fix the AC 1-35 scraped data where CSS selectors didn't match.

The raw scrape captured candidate info as jumbled text in the name field like:
  "won\n  94320 (+ 27945)\n  \n  S.VIJAYAKUMAR\n  Tamilaga Vettri Kazhagam"

This script parses that text to extract proper name, party, votes, status, and photo_url.
Then merges into the main tn_results_2026.json.
"""
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "data" / "processed" / "tn_results_2026_ac1-35_raw.json"
MAIN = ROOT / "data" / "processed" / "tn_results_2026.json"


def parse_candidate_entry(entry: dict) -> dict | None:
    """Parse a single candidate entry from the raw scraped data."""
    raw_name = entry.get("name", "")
    photo_url = entry.get("photo_url", "")

    # Skip entries that are just vote counts (the duplicate rows)
    # These look like "94320 (+ 27945)" with no newlines and no photo
    if not photo_url or photo_url == "":
        return None

    # Skip NOTA
    if "NOTA" in raw_name and "None of the Above" in raw_name:
        return None

    # Parse the jumbled text
    # Format: "won/lost\n  VOTES (+ MARGIN)\n  \n  NAME\n  PARTY"
    # or:     "VOTES (+ MARGIN)\n  \n  NAME\n  PARTY" (for NOTA-like entries)
    lines = [l.strip() for l in raw_name.split("\n") if l.strip()]

    status = ""
    votes = 0
    name = ""
    party = ""

    if not lines:
        return None

    idx = 0
    # First token might be "won" or "lost"
    if lines[idx] in ("won", "lost"):
        status = lines[idx]
        idx += 1

    # Next should be votes like "94320 (+ 27945)" or "66375 ( -27945)"
    if idx < len(lines):
        vote_match = re.match(r"(\d+)\s*\(", lines[idx])
        if vote_match:
            votes = int(vote_match.group(1))
            idx += 1

    # Remaining lines: name then party
    remaining = lines[idx:]
    if len(remaining) >= 2:
        name = remaining[0]
        party = remaining[1]
    elif len(remaining) == 1:
        name = remaining[0]

    if not name:
        return None

    return {
        "name": name,
        "party": party,
        "votes": votes,
        "status": status,
        "photo_url": photo_url,
    }


def fix_constituency(result: dict) -> dict:
    """Fix a single constituency result."""
    if result.get("error"):
        return result

    raw_candidates = result.get("candidates", [])
    candidates = []

    for entry in raw_candidates:
        parsed = parse_candidate_entry(entry)
        if parsed:
            candidates.append(parsed)

    # Sort by votes descending
    candidates.sort(key=lambda c: c["votes"], reverse=True)

    # Ensure winner is marked
    if candidates and not any(c["status"] == "won" for c in candidates):
        candidates[0]["status"] = "won"

    winner = next((c for c in candidates if c["status"] == "won"), candidates[0] if candidates else None)
    runner_up = next((c for c in candidates if c != winner), None) if candidates else None
    total_votes = sum(c["votes"] for c in candidates)
    margin = (winner["votes"] - runner_up["votes"]) if winner and runner_up else 0

    return {
        "ac_no": result["ac_no"],
        "ac_name": result.get("ac_name", f"AC-{result['ac_no']}"),
        "winner": {
            "name": winner["name"],
            "party": winner["party"],
            "votes": winner["votes"],
            "photo_url": winner["photo_url"],
        } if winner else None,
        "runner_up": {
            "name": runner_up["name"],
            "party": runner_up["party"],
            "votes": runner_up["votes"],
            "photo_url": runner_up["photo_url"],
        } if runner_up else None,
        "margin": margin,
        "total_votes": total_votes,
        "candidates": candidates,
        "total_candidates": len(candidates),
    }


def main():
    # Check for the raw file - user may have saved it with different name
    input_file = INPUT
    if not input_file.exists():
        # Try Downloads
        alt = Path.home() / "Downloads" / "tn_results_2026_ac1-35 (1).json"
        if alt.exists():
            input_file = alt
        else:
            alt2 = Path.home() / "Downloads" / "tn_results_2026_ac1-35.json"
            if alt2.exists():
                input_file = alt2

    if not input_file.exists():
        print(f"ERROR: Raw file not found. Save the scraped JSON as:\n  {INPUT}")
        print("  Or place it in ~/Downloads/")
        return

    with open(input_file) as f:
        raw = json.load(f)

    print(f"Fixing {len(raw['results'])} constituencies from {input_file.name}...")

    fixed_results = []
    for r in raw["results"]:
        if r.get("error"):
            print(f"  AC-{r['ac_no']}: ERROR ({r['error']}) — skipping")
            continue
        fixed = fix_constituency(r)
        w = fixed["winner"]
        if w:
            print(f"  AC-{fixed['ac_no']:3d} {fixed['ac_name']:30s} → {w['name']:30s} ({w['party']}) margin: {fixed['margin']:>6,}")
        else:
            print(f"  AC-{fixed['ac_no']:3d} {fixed['ac_name']:30s} → NO WINNER")
        fixed_results.append(fixed)

    print(f"\nFixed {len(fixed_results)} constituencies.")

    # Merge into main file
    if not MAIN.exists():
        print(f"ERROR: Main file not found: {MAIN}")
        return

    with open(MAIN) as f:
        main_data = json.load(f)

    existing = {r["ac_no"]: r for r in main_data["results"]}
    added, updated = 0, 0
    for r in fixed_results:
        ac = r["ac_no"]
        if ac in existing:
            updated += 1
        else:
            added += 1
        existing[ac] = r

    main_data["results"] = sorted(existing.values(), key=lambda x: x["ac_no"])
    main_data["total_acs"] = len(main_data["results"])

    with open(MAIN, "w") as f:
        json.dump(main_data, f, indent=2, ensure_ascii=False)

    print(f"\nMerged into {MAIN.name}: {added} added, {updated} updated. Total: {len(main_data['results'])} ACs.")


if __name__ == "__main__":
    main()
