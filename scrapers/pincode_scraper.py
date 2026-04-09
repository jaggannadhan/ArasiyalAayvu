"""
Pincode Scraper — Tamil Nadu
=============================
Fetches all TN pincodes from India Post API, maps district → constituency,
and uploads to Firestore `pincode_mapping` collection.

Strategy
--------
- Scans TN pincode ranges (600001–643999) in steps
- India Post API returns: district, post office names, block
- Unambiguous: pincode's post offices all fall in one constituency → direct map
- Ambiguous: pincode spans multiple constituencies → is_ambiguous=True, user picks

Constituency matching
---------------------
1. Block/taluk name match against constituency names (most specific)
2. Post office name match
3. District fallback → ambiguous with all constituencies in that district

Usage
-----
  # Discover all valid TN pincodes (no Firestore write, saves to JSON)
  .venv/bin/python3.14 scrapers/pincode_scraper.py --discover --output data/processed/tn_pincodes_raw.json

  # Build mapping from discovered pincodes
  .venv/bin/python3.14 scrapers/pincode_scraper.py --input data/processed/tn_pincodes_raw.json --dry-run

  # Full run
  .venv/bin/python3.14 scrapers/pincode_scraper.py --input data/processed/tn_pincodes_raw.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT_DIR = Path(__file__).resolve().parents[1]
MAP_PATH = ROOT_DIR / "web" / "src" / "lib" / "constituency-map.json"
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "naatunadappu")

INDIA_POST_URL = "https://api.postalpincode.in/pincode/{}"

# TN pincode ranges: 600xxx–643xxx
TN_RANGES = list(range(600001, 600120)) + \
            list(range(601001, 601302)) + \
            list(range(602001, 602106)) + \
            list(range(603001, 603320)) + \
            list(range(604001, 604410)) + \
            list(range(605001, 605115)) + \
            list(range(606001, 606213)) + \
            list(range(607001, 607807)) + \
            list(range(608001, 608703)) + \
            list(range(609001, 609811)) + \
            list(range(610001, 610213)) + \
            list(range(611001, 611111)) + \
            list(range(612001, 612806)) + \
            list(range(613001, 613703)) + \
            list(range(614001, 614806)) + \
            list(range(615001, 615702)) + \
            list(range(616001, 616302)) + \
            list(range(617001, 617702)) + \
            list(range(618001, 618706)) + \
            list(range(619001, 619702)) + \
            list(range(620001, 620025)) + \
            list(range(621001, 621730)) + \
            list(range(622001, 622515)) + \
            list(range(623001, 623807)) + \
            list(range(624001, 624710)) + \
            list(range(625001, 625708)) + \
            list(range(626001, 626210)) + \
            list(range(627001, 627860)) + \
            list(range(628001, 628953)) + \
            list(range(629001, 629810)) + \
            list(range(630001, 630812)) + \
            list(range(631001, 631702)) + \
            list(range(632001, 632602)) + \
            list(range(633001, 633460)) + \
            list(range(634001, 634403)) + \
            list(range(635001, 635852)) + \
            list(range(636001, 636902)) + \
            list(range(637001, 637505)) + \
            list(range(638001, 638812)) + \
            list(range(639001, 639210)) + \
            list(range(640001, 640025)) + \
            list(range(641001, 641120)) + \
            list(range(642001, 642210)) + \
            list(range(643001, 643253))

# India Post district name → one or more of our district keys.
# A list means India Post hasn't split the district (pre-2019 bifurcations) —
# we merge all constituencies across the listed districts for matching.
DISTRICT_ALIASES: dict[str, str | list[str]] = {
    # Post-2019 bifurcations: India Post still uses old names
    "KANCHIPURAM":       ["KANCHEEPURAM", "CHENGALPATTU"],  # bifurcated 2019
    "KANCHEEPURAM":      ["KANCHEEPURAM", "CHENGALPATTU"],
    "TIRUVALLUR":        ["THIRUVALLUR", "CHENNAI"],         # Chennai metro straddles

    # Transliteration variants
    "TIRUVALUR":         "THIRUVARUR",
    "THIRUVALUR":        "THIRUVARUR",
    "VILLUPURAM":        "VILLUPPURAM",
    "NAGAPATNAM":        "NAGAPATTINAM",
    "NAGAPATTINAM":      "NAGAPATTINAM",
    "TIRUCHIRAPALLI":    "TIRUCHIRAPPALLI",
    "TIRUCHCHIRAPPALLI": "TIRUCHIRAPPALLI",
    "TIRUCHIRAPPALLI":   "TIRUCHIRAPPALLI",
    "TRICHY":            "TIRUCHIRAPPALLI",
    "NILGIRIS":          "THE NILGIRIS",
    "THE NILGIRIS":      "THE NILGIRIS",
    "TIRUNELVELI KATTABO": "TIRUNELVELI",
    "TUTICORIN":         "THOOTHUKUDI",
    "THOOTHUKUDI":       "THOOTHUKUDI",
    "KANNIYAKUMARI":     "KANNIYAKUMARI",
    "KANYAKUMARI":       "KANNIYAKUMARI",
    "SIVAGANGAI":        "SIVAGANGA",
    "TIRUPUR":           "TIRUPPUR",
    "TIRUPPUR":          "TIRUPPUR",
    "DINDIGUL ANNA":     "DINDIGUL",
    "RAMANATHAPURAM":    "RAMANATHAPURAM",
    "PUDUKKOTTAI":       "PUDUKKOTTAI",
    "PUDUKOTTAI":        "PUDUKKOTTAI",
    "KRISHNAGIRI":       "KRISHNAGIRI",
    "DHARMAPURI":        "DHARMAPURI",
    "TIRUVANNAMALAI":    "TIRUVANNAMALAI",
    "THIRUVANNAMALAI":   "TIRUVANNAMALAI",
    "VELLORE":           "VELLORE",
    "NAMAKKAL":          "NAMAKKAL",
    "ARIYALUR":          "ARIYALUR",
    "PERAMBALUR":        "PERAMBALUR",
    "KARUR":             "KARUR",
    "ERODE":             "ERODE",
    "COIMBATORE":        "COIMBATORE",
    "SALEM":             "SALEM",
    "MADURAI":           "MADURAI",
    "THENI":             "THENI",
    "DINDIGUL":          "DINDIGUL",
    "VIRUDHUNAGAR":      "VIRUDHUNAGAR",
    "CUDDALORE":         "CUDDALORE",
    "THANJAVUR":         "THANJAVUR",
    "TIRUVARUR":         "THIRUVARUR",
    "CHENGALPATTU":      "CHENGALPATTU",
    "RANIPET":           "RANIPET",
    "TENKASI":           "TENKASI",
    "TIRUPATHUR":        "TIRUPATHUR",
    "KALLAKURICHI":      "KALLAKURICHI",
    "CHENNAI":           "CHENNAI",
}


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.upper().strip())


def norm_match(s: str) -> str:
    """
    Normalize a name for fuzzy matching.
    Applies Tamil transliteration collapse (same rules as frontend search)
    and strips SC/ST category suffixes.
    """
    s = norm(s)
    s = re.sub(r"\s*\(S[CT]\)\s*$", "", s)     # strip (SC) / (ST)
    s = re.sub(r"\bNORTH\b|\bSOUTH\b|\bEAST\b|\bWEST\b|\bCENTRAL\b", "", s)
    s = re.sub(r"[^A-Z0-9 ]", "", s)
    s = s.replace("TH", "T").replace("DH", "D").replace("ZH", "L")
    s = re.sub(r"EE", "I", s)
    s = re.sub(r"OO", "U", s)
    # Collapse double consonants
    s = re.sub(r"([BCDFGHJKLMNPQRSTVWXYZ])\1", r"\1", s)
    return re.sub(r"\s+", " ", s).strip()


def _block_norm(s: str) -> str:
    """
    Normalize a block name for exact constituency lookup.
    Same as norm_match but KEEPS directional words (NORTH/SOUTH/EAST/WEST/CENTRAL)
    so that 'Coimbatore North' block correctly matches 'COIMBATORE NORTH' constituency
    instead of colliding with 'COIMBATORE SOUTH'.
    """
    s = norm(s)
    s = re.sub(r"\s*\(S[CT]\)\s*$", "", s)     # strip (SC) / (ST)
    s = re.sub(r"[^A-Z0-9 ]", "", s)            # remove punctuation but keep directionals
    s = s.replace("TH", "T").replace("DH", "D").replace("ZH", "L")
    s = re.sub(r"EE", "I", s)
    s = re.sub(r"OO", "U", s)
    s = re.sub(r"([BCDFGHJKLMNPQRSTVWXYZ])\1", r"\1", s)
    return re.sub(r"\s+", " ", s).strip()


# Blocks that span multiple constituencies and can't be resolved by token overlap alone.
# Keys are _block_norm(block_name); values are lists of constituency slugs.
# These may reference constituencies across district boundaries (e.g. Ambattur block
# shows up in Chennai-district pincodes but is in THIRUVALLUR constituency district).
BLOCK_OVERRIDES: dict[str, list[str]] = {
    # Chennai: "Perambur Purasawalkam" division covers THIRU-VI-KA-NAGAR (SC), EGMORE (SC), PERAMBUR
    "PERAMBUR PURASAWALKAM": ["thiru_vi_ka_nagar_sc", "egmore_sc", "perambur"],
    # "Egmore Nungambakkam" covers EGMORE (SC) and THOUSAND LIGHTS
    "EGMORE NUNGAMBAKAM": ["egmore_sc", "thousand_lights"],
    # "Ambattur" block appears on Chennai-district pincodes but the constituency
    # is in THIRUVALLUR district — India Post hasn't updated after bifurcation
    "AMBATUR": ["ambattur"],
}

# Pincode-level overrides for cases where block/PO names give no useful signal
# (e.g. "Chennai City Corporation" block for central Chennai pincodes).
# Web-verified against elections.tn.gov.in / GCC ward data.
PINCODE_OVERRIDES: dict[str, dict] = {
    "600024": {"constituencies": ["thousand_lights"], "is_ambiguous": False},   # Kodambakkam
    "600026": {"constituencies": ["thiyagarayanagar"],  "is_ambiguous": False}, # Vadapalani
    "600107": {"constituencies": ["virugampakkam"],     "is_ambiguous": False}, # Koyambedu
}


# Tokens we ignore when matching (too generic to be meaningful)
_STOP = {"SO", "BO", "HO", "POST", "OFFICE", "HEAD", "SUB", "BRANCH",
         "COLONY", "NAGAR", "STREET", "ROAD", "AREA", "TOWN", "CITY"}


def _token_norm(s: str) -> str:
    """
    Lighter normalization for token extraction — same transliteration as norm_match
    but WITHOUT double-consonant collapse (which would turn 'ANNA'→'ANA', too short).
    Also normalises trailing Y→I so 'VELACHERY' and 'VELACHERI' share the same token.
    """
    s = norm(s)
    s = re.sub(r"\s*\(S[CT]\)\s*$", "", s)
    s = re.sub(r"\bNORTH\b|\bSOUTH\b|\bEAST\b|\bWEST\b|\bCENTRAL\b", "", s)
    s = re.sub(r"[^A-Z0-9 ]", "", s)
    s = s.replace("TH", "T").replace("DH", "D").replace("ZH", "L")
    s = re.sub(r"EE", "I", s)
    s = re.sub(r"OO", "U", s)
    s = re.sub(r"Y\b", "I", s)   # VELACHERY → VELACHERI
    return re.sub(r"\s+", " ", s).strip()


def name_tokens(s: str) -> set[str]:
    """Return significant tokens (4+ chars, non-stop) from a name."""
    return {w for w in _token_norm(s).split() if len(w) >= 4 and w not in _STOP}


def fetch_pincode(pin: int) -> dict | None:
    """Query India Post API. Returns None if not a TN pincode or not found."""
    try:
        url = INDIA_POST_URL.format(pin)
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "ArasiyalAayvuResearchBot/1.0", "Connection": "close"},
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        if not data or data[0].get("Status") != "Success":
            return None
        offices = data[0].get("PostOffice", [])
        if not offices:
            return None
        # Filter to TN only
        tn_offices = [o for o in offices if o.get("State", "").upper() == "TAMIL NADU"]
        if not tn_offices:
            return None
        return {
            "pincode": str(pin),
            "district": tn_offices[0].get("District", ""),
            "division": tn_offices[0].get("Division", ""),
            "offices": [
                {
                    "name": o.get("Name", ""),
                    "block": o.get("Block", ""),
                    "branch_type": o.get("BranchType", ""),
                    "delivery": o.get("DeliveryStatus", ""),
                }
                for o in tn_offices
            ],
        }
    except Exception:
        return None


def discover_pincodes(output_path: Path, max_workers: int = 10) -> list[dict]:
    """Scan all TN pincode ranges and save raw data (concurrent)."""
    total = len(TN_RANGES)
    print(f"Scanning {total} potential TN pincodes with {max_workers} workers…")

    results_map: dict[int, dict] = {}
    found = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_pin = {executor.submit(fetch_pincode, pin): pin for pin in TN_RANGES}
        completed = 0
        for future in as_completed(future_to_pin):
            pin = future_to_pin[future]
            completed += 1
            try:
                result = future.result()
            except Exception:
                result = None
            if result:
                results_map[pin] = result
                found += 1
                print(f"  [{completed}/{total}] {pin} → {result['district']} ({found} found)")
            elif completed % 500 == 0:
                print(f"  [{completed}/{total}] scanned… ({found} found so far)")

    # Sort by pincode order
    results = [results_map[pin] for pin in TN_RANGES if pin in results_map]
    print(f"\nFound {len(results)} valid TN pincodes")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"Saved → {output_path}")
    return results


def build_mapping(raw_pincodes: list[dict], constituency_map: dict) -> list[dict]:
    """
    Convert raw pincode data into Firestore-ready pincode_mapping records.

    Matching strategy (in priority order):
      1. Unanimous block match  — all non-NA blocks agree AND match a constituency
      2. Token overlap match    — tokens from PO names / blocks overlap with constituency name tokens
      3. District fallback      — only constituencies whose name tokens appear in ANY PO name/block
                                  (never dumps all district constituencies blindly)
    """

    # district → [constituency dicts]
    district_to_slugs: dict[str, list[dict]] = {}
    for slug, meta in constituency_map.items():
        dist = norm(meta.get("district", ""))
        district_to_slugs.setdefault(dist, []).append({
            "slug": slug,
            "name": meta.get("name", ""),
            "name_ta": meta.get("name_ta", ""),
        })

    # Global slug → constituency dict — used for BLOCK_OVERRIDES / PINCODE_OVERRIDES
    # which may reference constituencies across district boundaries
    const_by_slug: dict[str, dict] = {
        slug: {"slug": slug, "name": meta["name"], "name_ta": meta.get("name_ta", "")}
        for slug, meta in constituency_map.items()
    }

    # block-normalised-name → constituency dict (keeps directionals so NORTH ≠ SOUTH)
    norm_to_const: dict[str, dict] = {}
    # tokens → constituency dict (for token-overlap lookup)
    const_tokens: list[tuple[set[str], dict]] = []
    for slug, meta in constituency_map.items():
        c = {"slug": slug, "name": meta["name"], "name_ta": meta.get("name_ta", "")}
        key = _block_norm(meta.get("name", ""))
        norm_to_const[key] = c
        tokens = name_tokens(meta.get("name", ""))
        if tokens:
            const_tokens.append((tokens, c))

    def _token_match(text: str, district_slugs: set[str]) -> list[dict]:
        """Find constituencies whose name tokens overlap with `text` tokens."""
        text_toks = name_tokens(text)
        if not text_toks:
            return []
        hits = []
        seen: set[str] = set()
        for ctoks, c in const_tokens:
            if c["slug"] not in district_slugs:
                continue
            if ctoks & text_toks and c["slug"] not in seen:
                hits.append(c)
                seen.add(c["slug"])
        return hits

    records = []
    for raw in raw_pincodes:
        pin = raw["pincode"]

        # ── Pincode-level override (web-verified) ──────────────────────────
        if pin in PINCODE_OVERRIDES:
            ov = PINCODE_OVERRIDES[pin]
            ov_consts = [const_by_slug[s] for s in ov["constituencies"] if s in const_by_slug]
            if ov_consts:
                records.append({
                    "pincode": pin,
                    "district": raw.get("district", "").title(),
                    "constituencies": ov_consts,
                    "is_ambiguous": ov.get("is_ambiguous", len(ov_consts) > 1),
                })
                continue

        raw_district = norm(raw.get("district", ""))
        alias = DISTRICT_ALIASES.get(raw_district, raw_district)

        # alias may be a single district key or a list (pre-bifurcation India Post names)
        district_keys = alias if isinstance(alias, list) else [alias]
        dist_consts: list[dict] = []
        for dk in district_keys:
            dist_consts.extend(district_to_slugs.get(dk, []))
        # Deduplicate while preserving order
        seen_slugs: set[str] = set()
        dist_consts_dedup = []
        for c in dist_consts:
            if c["slug"] not in seen_slugs:
                dist_consts_dedup.append(c)
                seen_slugs.add(c["slug"])
        dist_consts = dist_consts_dedup

        if not dist_consts:
            print(f"  WARN: no district match for {pin} → '{raw_district}'")
            continue

        district_slug_set = {c["slug"] for c in dist_consts}

        # Trivially unambiguous (single-constituency district e.g. Ariyalur)
        if len(dist_consts) == 1:
            records.append({
                "pincode": pin,
                "district": raw.get("district", "").title(),
                "constituencies": dist_consts,
                "is_ambiguous": False,
            })
            continue

        offices = raw.get("offices", [])
        delivery = [o for o in offices if o.get("delivery", "").lower() == "delivery"] or offices

        # ── Step 1: unanimous block match ──────────────────────────────────
        all_blocks = [
            _block_norm(o.get("block", ""))
            for o in offices
            if o.get("block", "") not in ("NA", "", "N.A.")
        ]
        unique_blocks = list(dict.fromkeys(all_blocks))

        matched: list[dict] = []
        if len(unique_blocks) == 1:
            blk_key = unique_blocks[0]
            # Check BLOCK_OVERRIDES first — known multi-constituency blocks.
            # Use const_by_slug (not dist_consts) so cross-district blocks work.
            if blk_key in BLOCK_OVERRIDES:
                override_consts = [
                    const_by_slug[s] for s in BLOCK_OVERRIDES[blk_key]
                    if s in const_by_slug
                ]
                if override_consts:
                    records.append({
                        "pincode": pin,
                        "district": raw.get("district", "").title(),
                        "constituencies": override_consts,
                        "is_ambiguous": len(override_consts) > 1,
                    })
                    continue
            c = norm_to_const.get(blk_key)
            if c and c["slug"] in district_slug_set:
                matched = [c]

        if len(matched) == 1:
            records.append({
                "pincode": pin,
                "district": raw.get("district", "").title(),
                "constituencies": matched,
                "is_ambiguous": False,
            })
            continue

        # ── Step 2: token-overlap on PO names + blocks ─────────────────────
        # Build a combined text from all delivery office names and blocks
        all_text_parts: list[str] = []
        for o in delivery:
            all_text_parts.append(o.get("name", ""))
            blk = o.get("block", "")
            if blk not in ("NA", "", "N.A."):
                all_text_parts.append(blk)

        # Collect all distinct constituency matches across all PO names
        token_hits: dict[str, dict] = {}
        for text in all_text_parts:
            for c in _token_match(text, district_slug_set):
                token_hits[c["slug"]] = c

        if len(token_hits) == 1:
            records.append({
                "pincode": pin,
                "district": raw.get("district", "").title(),
                "constituencies": list(token_hits.values()),
                "is_ambiguous": False,
            })
            continue

        if 2 <= len(token_hits) <= 5:
            # Narrowed ambiguous set — much better than all district constituencies
            records.append({
                "pincode": pin,
                "district": raw.get("district", "").title(),
                "constituencies": list(token_hits.values()),
                "is_ambiguous": True,
            })
            continue

        # ── Step 3: fallback — still show district list but warn ────────────
        # At least log it so we know which pincodes need manual review
        print(f"  FALLBACK {pin} ({raw_district}): {len(dist_consts)} constituencies "
              f"— PO names: {[o.get('name','') for o in delivery[:3]]}")
        records.append({
            "pincode": pin,
            "district": raw.get("district", "").title(),
            "constituencies": [
                {"slug": c["slug"], "name": c["name"], "name_ta": c.get("name_ta", "")}
                for c in dist_consts
            ],
            "is_ambiguous": True,
        })

    unambig = sum(1 for r in records if not r["is_ambiguous"])
    print(f"\nBuilt {len(records)} records: {unambig} unambiguous, "
          f"{len(records)-unambig} ambiguous")
    return records


def upload(records: list[dict], project_id: str, dry_run: bool) -> None:
    if dry_run:
        print(f"\n[DRY RUN] Would upload {len(records)} docs to `pincode_mapping`")
        for r in records[:10]:
            print(f"  {r['pincode']} → {r['district']} | ambiguous={r['is_ambiguous']} | "
                  f"{[c['name'] for c in r['constituencies']]}")
        return

    from google.cloud import firestore
    db = firestore.Client(project=project_id)
    col = db.collection("pincode_mapping")

    # Load existing hand-curated HIGH-confidence docs so we don't overwrite them
    existing_high = {
        d.id for d in col.stream()
        if d.to_dict().get("ground_truth_confidence") == "HIGH"
    }
    print(f"  Skipping {len(existing_high)} HIGH-confidence hand-curated entries")

    batch = db.batch()
    written = skipped = 0
    for i, rec in enumerate(records):
        if rec["pincode"] in existing_high:
            skipped += 1
            continue
        batch.set(col.document(rec["pincode"]), rec)
        written += 1
        if written % 400 == 0:
            batch.commit()
            batch = db.batch()
            print(f"  Committed {written} docs…")
    batch.commit()
    print(f"Uploaded {written} docs, skipped {skipped} HIGH-confidence entries")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--discover", action="store_true", help="Scan India Post API for all TN pincodes")
    ap.add_argument("--input", default=None, help="Path to raw pincodes JSON (from --discover)")
    ap.add_argument("--output", default="data/processed/tn_pincodes_raw.json", help="Output path for --discover")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    constituency_map = json.loads(MAP_PATH.read_text(encoding="utf-8"))

    if args.discover:
        discover_pincodes(Path(args.output))
        return

    if not args.input:
        ap.error("Provide --discover to fetch pincodes, or --input <path> to build from cached data")

    raw = json.loads(Path(args.input).read_text(encoding="utf-8"))
    print(f"Loaded {len(raw)} raw pincodes from {args.input}")

    records = build_mapping(raw, constituency_map)

    # Save mapped output
    out = Path(args.output.replace("_raw.json", "_mapped.json")) if "_raw" in (args.output or "") else Path("data/processed/tn_pincodes_mapped.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"Saved mapped → {out}")

    upload(records, PROJECT_ID, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
