import json
from pathlib import Path

PARTY_NAME_MAP: dict[str, str] = {
    "Dravida Munnetra Kazhagam": "dmk",
    "DMK": "dmk",
    "All India Anna Dravida Munnetra Kazhagam": "aiadmk",
    "AIADMK": "aiadmk",
    "Anna DMK": "aiadmk",
    "ADMK": "aiadmk",
    "Indian National Congress": "inc",
    "INC": "inc",
    "Congress": "inc",
    "Bharatiya Janata Party": "bjp",
    "BJP": "bjp",
    "Pattali Makkal Katchi": "pmk",
    "PMK": "pmk",
    "Viduthalai Chiruthaigal Katchi": "vck",
    "VCK": "vck",
    "Naam Tamilar Katchi": "ntk",
    "NTK": "ntk",
    "Marumalarchi Dravida Munnetra Kazhagam": "mdmk",
    "MDMK": "mdmk",
    "Communist Party of India": "cpi",
    "CPI": "cpi",
    "Communist Party of India (Marxist)": "cpim",
    "CPI(M)": "cpim",
    "Independent": "ind",
    "Others": "others",
}


def normalize_party_name(raw_name: str) -> str:
    for key, val in PARTY_NAME_MAP.items():
        if key.lower() in raw_name.lower():
            return val
    return raw_name.lower().replace(" ", "_")


def transform_ceo_records(raw_records: list[dict]) -> dict[int, dict]:
    by_year: dict[int, list[dict]] = {}
    for rec in raw_records:
        year = rec["year"]
        by_year.setdefault(year, []).append(rec)

    elections: dict[int, dict] = {}
    for year, party_rows in by_year.items():
        party_results = []
        for row in party_rows:
            party_results.append({
                "party_id": normalize_party_name(row["party_name"]),
                "party_name_raw": row["party_name"],
                "seats_contested": row["seats_contested"],
                "seats_won": row["seats_won"],
                "votes": row["votes"],
                "vote_share_pct": row["vote_share_pct"],
            })

        winner = max(party_results, key=lambda x: x["seats_won"], default=None)

        elections[year] = {
            "year": year,
            "total_seats": 234,
            "majority_mark": 118,
            "party_results": party_results,
            "winning_party": winner["party_id"] if winner else "unknown",
            "source_url": party_rows[0]["source_url"],
            "pdf_checksum": party_rows[0]["pdf_checksum"],
            "ground_truth_confidence": "HIGH",
        }

    return elections


# fmt: off
ALLIANCE_DATA: dict[int, list[dict]] = {
    1952: [{"alliance_name": "INC (standalone)", "anchor_party": "inc", "member_parties": ["inc"], "national_front_alignment": None, "outcome": "Won"}],
    1957: [{"alliance_name": "INC (standalone)", "anchor_party": "inc", "member_parties": ["inc"], "national_front_alignment": None, "outcome": "Won"}],
    1962: [{"alliance_name": "INC (standalone)", "anchor_party": "inc", "member_parties": ["inc"], "national_front_alignment": None, "outcome": "Won"}],
    1967: [{"alliance_name": "DMK (standalone)", "anchor_party": "dmk", "member_parties": ["dmk"], "national_front_alignment": None, "outcome": "Won"},
           {"alliance_name": "INC (standalone)", "anchor_party": "inc", "member_parties": ["inc"], "national_front_alignment": None, "outcome": "Lost"}],
    1971: [{"alliance_name": "DMK (standalone)", "anchor_party": "dmk", "member_parties": ["dmk"], "national_front_alignment": None, "outcome": "Won"}],
    1977: [{"alliance_name": "AIADMK-INC front", "anchor_party": "aiadmk", "member_parties": ["aiadmk", "inc"], "national_front_alignment": None, "outcome": "Won"},
           {"alliance_name": "DMK (standalone)", "anchor_party": "dmk", "member_parties": ["dmk"], "national_front_alignment": None, "outcome": "Lost"}],
    1980: [{"alliance_name": "AIADMK (standalone)", "anchor_party": "aiadmk", "member_parties": ["aiadmk"], "national_front_alignment": None, "outcome": "Won"},
           {"alliance_name": "DMK-INC front", "anchor_party": "dmk", "member_parties": ["dmk", "inc"], "national_front_alignment": None, "outcome": "Lost"}],
    1984: [{"alliance_name": "AIADMK-INC front", "anchor_party": "aiadmk", "member_parties": ["aiadmk", "inc"], "national_front_alignment": "INC-led", "outcome": "Won"},
           {"alliance_name": "DMK (standalone)", "anchor_party": "dmk", "member_parties": ["dmk"], "national_front_alignment": None, "outcome": "Lost"}],
    1989: [{"alliance_name": "DMK-INC front", "anchor_party": "dmk", "member_parties": ["dmk", "inc"], "national_front_alignment": "INC-led", "outcome": "Won"},
           {"alliance_name": "AIADMK (standalone)", "anchor_party": "aiadmk", "member_parties": ["aiadmk"], "national_front_alignment": None, "outcome": "Lost"}],
    1991: [{"alliance_name": "AIADMK-BJP-PMK front", "anchor_party": "aiadmk", "member_parties": ["aiadmk", "bjp", "pmk"], "national_front_alignment": None, "outcome": "Won"},
           {"alliance_name": "DMK (standalone)", "anchor_party": "dmk", "member_parties": ["dmk"], "national_front_alignment": None, "outcome": "Lost"}],
    1996: [{"alliance_name": "DMK-TMC front", "anchor_party": "dmk", "member_parties": ["dmk", "tmc"], "national_front_alignment": "Third Front", "outcome": "Won"},
           {"alliance_name": "AIADMK-INC front", "anchor_party": "aiadmk", "member_parties": ["aiadmk", "inc"], "national_front_alignment": "INC-led", "outcome": "Lost"}],
    2001: [{"alliance_name": "AIADMK-INC-PMK-MDMK front", "anchor_party": "aiadmk", "member_parties": ["aiadmk", "inc", "pmk", "mdmk"], "national_front_alignment": "UPA", "outcome": "Won"},
           {"alliance_name": "DMK-BJP front", "anchor_party": "dmk", "member_parties": ["dmk", "bjp"], "national_front_alignment": "NDA", "outcome": "Lost"}],
    2006: [{"alliance_name": "DMK-INC-PMK-Left front", "anchor_party": "dmk", "member_parties": ["dmk", "inc", "pmk", "cpi", "cpim", "mdmk", "vck"], "national_front_alignment": "UPA", "outcome": "Won"},
           {"alliance_name": "AIADMK-DMDK front", "anchor_party": "aiadmk", "member_parties": ["aiadmk", "dmdk"], "national_front_alignment": None, "outcome": "Lost"}],
    2011: [{"alliance_name": "AIADMK-DMDK-PMK-Left front", "anchor_party": "aiadmk", "member_parties": ["aiadmk", "dmdk", "pmk", "mdmk", "vck", "cpi", "cpim"], "national_front_alignment": None, "outcome": "Won"},
           {"alliance_name": "DMK-INC-BJPfront", "anchor_party": "dmk", "member_parties": ["dmk", "inc", "bjp", "pmk"], "national_front_alignment": "UPA", "outcome": "Lost"}],
    2016: [{"alliance_name": "AIADMK (standalone)", "anchor_party": "aiadmk", "member_parties": ["aiadmk"], "national_front_alignment": None, "outcome": "Won"},
           {"alliance_name": "DMK-INC-Left front", "anchor_party": "dmk", "member_parties": ["dmk", "inc", "cpi", "cpim", "mdmk", "vck"], "national_front_alignment": "UPA", "outcome": "Lost"}],
    2021: [{"alliance_name": "Secular Progressive Alliance", "anchor_party": "dmk", "member_parties": ["dmk", "inc", "cpi", "cpim", "vck", "mdmk", "mmk", "iuml"], "national_front_alignment": "UPA", "outcome": "Won"},
           {"alliance_name": "AIADMK-BJP-PMK front", "anchor_party": "aiadmk", "member_parties": ["aiadmk", "bjp", "pmk", "dmdk"], "national_front_alignment": "NDA", "outcome": "Lost"}],
}
# fmt: on


def build_alliance_matrix(elections: dict[int, dict] = None) -> dict[int, list[dict]]:
    return ALLIANCE_DATA


def save_processed(data: dict, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  [saved] {path}")
