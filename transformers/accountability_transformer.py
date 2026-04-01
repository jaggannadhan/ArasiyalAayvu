"""
Accountability Transformer
Processes MyNeta/ADR scraped MLA data into candidate_accountability Firestore schema.
Adds severity classification, party-level rollups, and constituency linkage.
"""

import json
import re
from pathlib import Path


SERIOUS_CHARGE_KEYWORDS = [
    "murder", "attempt to murder", "rape", "kidnapping", "dacoity",
    "robbery", "extortion", "cheating", "fraud", "corruption",
    "ipc 302", "ipc 307", "ipc 376", "ipc 363", "ipc 420",
    "pocso", "domestic violence", "electoral malpractice"
]


def classify_criminal_severity(cases: int, education: str) -> str:
    """Simple severity bucket for the awareness UI."""
    if cases == 0:
        return "CLEAN"
    if cases <= 2:
        return "MINOR"
    if cases <= 5:
        return "MODERATE"
    return "SERIOUS"


def enrich_winner(winner: dict) -> dict:
    """Add computed fields to a raw MyNeta winner record."""
    cases = winner.get("criminal_cases_total", 0)
    assets = winner.get("assets_cr")
    liabilities = winner.get("liabilities_cr")

    winner["criminal_severity"] = classify_criminal_severity(
        cases, winner.get("education", "")
    )
    winner["is_crorepati"] = (assets or 0) >= 1.0
    winner["net_assets_cr"] = (
        round(assets - (liabilities or 0), 4) if assets is not None else None
    )

    # Normalize education tier
    edu = (winner.get("education") or "").lower()
    if any(t in edu for t in ["doctorate", "phd"]):
        winner["education_tier"] = "Doctorate"
    elif any(t in edu for t in ["post graduate", "pg", "mba", "ma ", "msc", "mtech"]):
        winner["education_tier"] = "Post Graduate"
    elif any(t in edu for t in ["graduate", "be ", "btech", "bsc", "bca", "bcom", "ba "]):
        winner["education_tier"] = "Graduate"
    elif "12th" in edu or "hsc" in edu or "inter" in edu:
        winner["education_tier"] = "Class XII"
    elif "10th" in edu or "sslc" in edu or "matric" in edu:
        winner["education_tier"] = "Class X"
    elif "8th" in edu or "primary" in edu:
        winner["education_tier"] = "Below Class X"
    else:
        winner["education_tier"] = "Not Disclosed"

    # Firestore document ID — collapse multiple underscores and strip trailing
    # so "HARUR (SC)" → "harur_sc" instead of "harur__sc_"
    constituency_slug = re.sub(
        r"_+", "_", re.sub(r"[^a-z0-9]", "_", winner.get("constituency", "").lower())
    ).strip("_")
    winner["doc_id"] = f"2021_{constituency_slug}"

    return winner


def build_party_rollups(winners: list[dict]) -> list[dict]:
    """
    Aggregate MLA stats per party — for party accountability scorecards.
    """
    from collections import defaultdict

    party_data: dict[str, dict] = defaultdict(lambda: {
        "mla_count": 0,
        "total_criminal_cases": 0,
        "mlas_with_cases": 0,
        "mlas_with_serious_cases": 0,
        "crorepati_mlas": 0,
        "total_assets_cr": 0.0,
        "graduates_or_above": 0,
        "party": "",
    })

    for w in winners:
        party = w.get("party", "Unknown")
        p = party_data[party]
        p["party"] = party
        p["mla_count"] += 1
        cases = w.get("criminal_cases_total", 0)
        p["total_criminal_cases"] += cases
        if cases > 0:
            p["mlas_with_cases"] += 1
        if w.get("criminal_severity") in ("MODERATE", "SERIOUS"):
            p["mlas_with_serious_cases"] += 1
        if w.get("is_crorepati"):
            p["crorepati_mlas"] += 1
        if w.get("assets_cr"):
            p["total_assets_cr"] += w["assets_cr"]
        if w.get("education_tier") in ("Graduate", "Post Graduate", "Doctorate"):
            p["graduates_or_above"] += 1

    rollups = []
    for party, data in party_data.items():
        n = data["mla_count"]
        if n == 0:
            continue
        rollups.append({
            "doc_id": f"2021_party_{re.sub(r'[^a-z0-9]', '_', party.lower())}",
            "party": party,
            "election_year": 2021,
            "mla_count": n,
            "criminal_cases_pct": round((data["mlas_with_cases"] / n) * 100, 1),
            "serious_cases_pct": round((data["mlas_with_serious_cases"] / n) * 100, 1),
            "crorepati_pct": round((data["crorepati_mlas"] / n) * 100, 1),
            "avg_assets_cr": round(data["total_assets_cr"] / n, 2) if n > 0 else 0,
            "graduate_pct": round((data["graduates_or_above"] / n) * 100, 1),
            "source_url": "https://myneta.info/tamilnadu2021/",
            "ground_truth_confidence": "HIGH",
        })

    return sorted(rollups, key=lambda x: -x["mla_count"])


def build_assembly_summary(winners: list[dict], stats: dict) -> dict:
    """
    Single summary document for the 2021 TN Assembly — top-level accountability card.
    Uses both scraped stats and derived values.
    """
    assets = [w["assets_cr"] for w in winners if w.get("assets_cr") is not None]
    avg_assets = round(sum(assets) / len(assets), 2) if assets else 12.52

    return {
        "doc_id": "tn_assembly_2021_summary",
        "election_year": 2021,
        "total_constituencies": 234,
        "winners_analyzed": stats.get("total_winners_analyzed", 224),
        "criminal_accountability": {
            "with_criminal_cases": stats.get("winners_with_criminal_cases", 134),
            "with_criminal_cases_pct": stats.get("winners_with_criminal_cases_pct", 60),
            "with_serious_cases": stats.get("winners_with_serious_criminal_cases", 58),
            "with_serious_cases_pct": stats.get("winners_with_serious_criminal_cases_pct", 26),
            "clean_record": (stats.get("total_winners_analyzed", 224) or 224) - (stats.get("winners_with_criminal_cases", 134) or 134),
        },
        "financial_profile": {
            "crorepati_winners": stats.get("crorepati_winners", 192),
            "crorepati_pct": stats.get("crorepati_winners_pct", 86),
            "avg_assets_cr": stats.get("avg_assets_cr") or avg_assets,
            "max_assets_cr": stats.get("max_assets_cr"),
        },
        "education_profile": {
            "graduate_or_above": stats.get("graduate_or_above", 142),
            "graduate_or_above_pct": stats.get("graduate_or_above_pct", 63),
        },
        "source_url": "https://myneta.info/tamilnadu2021/index.php?action=summary",
        "source_title": "MyNeta — ADR Tamil Nadu 2021 Assembly Election Analysis",
        "ground_truth_confidence": "HIGH",
    }


def save_processed(data, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    print(f"  [saved] {path}")
