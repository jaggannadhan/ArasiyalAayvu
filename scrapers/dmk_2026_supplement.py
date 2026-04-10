"""
DMK 2026 Manifesto — Supplement Upload
=======================================
Manually curated promises that were MISSING from the LLM-extracted run
(manifesto_ingest.py). Cross-referenced against the official English summary of:
  https://dmksite.blob.core.windows.net/prod-dmk-strapi-replica/assets/DMK_Manifesto_2026_d601f15181

Covers 12 missing promises across 4 pillars:
  Women's Welfare  — 4 promises (welfare transfers, elderly pension)
  Agriculture      — 3 promises (dairy, veterinary)
  Education        — 2 promises (breakfast scheme, Zero Dropout Act)
  Infrastructure   — 3 promises (dialysis, Global Cities, ports)

NOTE: SDG alignment is NOT stored here. Manifesto data is kept pristine.
      SDG mapping is computed at the client/API layer.

Usage
-----
  # Dry run — print records, no Firestore write
  python scrapers/dmk_2026_supplement.py --dry-run

  # Upload to Firestore
  python scrapers/dmk_2026_supplement.py

  # Override GCP project
  python scrapers/dmk_2026_supplement.py --project my-project
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from google.cloud import firestore
except ImportError:
    firestore = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
COLLECTION        = "manifesto_promises"
PARTY_ID          = "dmk"
PARTY_NAME        = "DMK"
PARTY_COLOR       = "bg-red-600"
TARGET_YEAR       = 2026
MANIFESTO_PDF_URL = (
    "https://dmksite.blob.core.windows.net/prod-dmk-strapi-replica/"
    "assets/DMK_Manifesto_2026_d601f15181"
)
SOURCE_NOTES = (
    "Manually curated — supplement to LLM-extracted run. "
    "Source: Official DMK 2026 manifesto (English summary). "
    "Promises verified against published manifesto highlights."
)
NOW_ISO = datetime.now(timezone.utc).isoformat()

# ---------------------------------------------------------------------------
# Missing promises
# ---------------------------------------------------------------------------
PROMISES: list[dict[str, Any]] = [

    # ── Women's Welfare ───────────────────────────────────────────────────────

    {
        "category":       "Women's Welfare",
        "promise_text_en": (
            "Increase the monthly assistance under Kalaignar Magalir Urimai Thittam "
            "from ₹1,000 to ₹2,000 per month, benefiting over 1.37 crore women "
            "heads of households."
        ),
        "promise_text_ta": (
            "கலைஞர் மகளிர் உரிமைத் திட்டத்தின் கீழ் மாதாந்திர உதவித்தொகையை "
            "₹1,000-லிருந்து ₹2,000 ஆக உயர்த்துவோம் — 1.37 கோடிக்கும் மேற்பட்ட "
            "பெண் குடும்பத் தலைவர்கள் பயனடைவர்."
        ),
        "stance_vibe":      "Women-focused",
        "amount_mentioned": "₹2,000/month (up from ₹1,000)",
        "scheme_name":      "Kalaignar Magalir Urimai Thittam",
    },
    {
        "category":       "Women's Welfare",
        "promise_text_en": (
            "Provide a one-time ₹8,000 coupon to all non-income-tax-paying homemakers "
            "to purchase an electronic home appliance of their choice from local stores."
        ),
        "promise_text_ta": (
            "வருமான வரி செலுத்தாத அனைத்து இல்லத்தரசிகளுக்கும் உள்ளூர் கடைகளில் "
            "விரும்பிய மின் சாதனம் வாங்க ஒருமுறை ₹8,000 கூப்பன் வழங்குவோம்."
        ),
        "stance_vibe":      "Populist",
        "amount_mentioned": "₹8,000 one-time coupon",
        "scheme_name":      None,
    },
    {
        "category":       "Women's Welfare",
        "promise_text_en": (
            "Increase the monthly social security pension for the elderly to ₹2,000 "
            "per month."
        ),
        "promise_text_ta": (
            "முதியோர்களுக்கான மாதாந்திர சமூகப் பாதுகாப்பு ஓய்வூதியத்தை "
            "₹2,000 ஆக உயர்த்துவோம்."
        ),
        "stance_vibe":      "Welfare-centric",
        "amount_mentioned": "₹2,000/month",
        "scheme_name":      None,
    },
    {
        "category":       "Women's Welfare",
        "promise_text_en": (
            "Provide a special one-time assistance of ₹10,000 to every family to "
            "mitigate the burden of rising prices and local tax increases."
        ),
        "promise_text_ta": (
            "விலைவாசி உயர்வு மற்றும் உள்ளாட்சி வரி சுமையை குறைக்க ஒவ்வொரு "
            "குடும்பத்திற்கும் ஒருமுறை ₹10,000 சிறப்பு உதவி வழங்குவோம்."
        ),
        "stance_vibe":      "Populist",
        "amount_mentioned": "₹10,000 one-time",
        "scheme_name":      None,
    },

    # ── Agriculture (Dairy & Veterinary) ─────────────────────────────────────

    {
        "category":       "Agriculture",
        "promise_text_en": (
            "Increase the milk procurement price by ₹5 per litre to support dairy "
            "producers across Tamil Nadu."
        ),
        "promise_text_ta": (
            "பால் உற்பத்தியாளர்களுக்கு ஆதரவளிக்க பால் கொள்முதல் விலையை "
            "லிட்டருக்கு ₹5 உயர்த்துவோம்."
        ),
        "stance_vibe":      "Farmer-focused",
        "amount_mentioned": "+₹5 per litre",
        "scheme_name":      None,
    },
    {
        "category":       "Agriculture",
        "promise_text_en": (
            "Distribute milking machines to dairy suppliers across 3,000 milk "
            "cooperative societies to improve productivity and reduce manual labour."
        ),
        "promise_text_ta": (
            "உற்பத்தித் திறனை மேம்படுத்தவும் கைத்தொழிலை குறைக்கவும் 3,000 பால் "
            "கூட்டுறவு சங்கங்களில் பால் கறக்கும் இயந்திரங்கள் வழங்குவோம்."
        ),
        "stance_vibe":      "Farmer-focused",
        "amount_mentioned": "3,000 cooperative societies",
        "scheme_name":      None,
    },
    {
        "category":       "Agriculture",
        "promise_text_en": (
            "Establish 24-hour veterinary clinics in all districts and increase cattle "
            "maintenance loans to ₹5,000 per animal."
        ),
        "promise_text_ta": (
            "அனைத்து மாவட்டங்களிலும் 24 மணி நேர கால்நடை மருத்துவமனைகள் "
            "அமைக்கப்படும்; கால்நடை பராமரிப்பு கடன் ₹5,000 ஆக உயர்த்தப்படும்."
        ),
        "stance_vibe":      "Farmer-focused",
        "amount_mentioned": "₹5,000 per animal (cattle loan)",
        "scheme_name":      None,
    },

    # ── Education ────────────────────────────────────────────────────────────

    {
        "category":       "Education",
        "promise_text_en": (
            "Introduce a special Act for the protection of children and launch a "
            "state mission to make Tamil Nadu a 'Zero School Dropout' state."
        ),
        "promise_text_ta": (
            "குழந்தைகளின் பாதுகாப்பிற்காக சிறப்புச் சட்டம் இயற்றப்படும்; "
            "தமிழ்நாட்டை 'பள்ளி இடைவிலக்கு இல்லாத மாநிலம்' ஆக்கும் "
            "இலக்கு திட்டம் தொடங்கப்படும்."
        ),
        "stance_vibe":      "Reform-oriented",
        "amount_mentioned": None,
        "scheme_name":      "Zero School Dropout Mission",
    },
    {
        "category":       "Education",
        "promise_text_en": (
            "Expand the Chief Minister's Breakfast Scheme from Classes 1–5 to cover "
            "all government school students up to Class 8 across all 38 districts."
        ),
        "promise_text_ta": (
            "முதலமைச்சர் காலை உணவுத் திட்டத்தை 1-5 வகுப்புகளில் இருந்து 8-ஆம் "
            "வகுப்பு வரை அனைத்து 38 மாவட்டங்களிலும் விரிவாக்குவோம்."
        ),
        "stance_vibe":      "Welfare-centric",
        "amount_mentioned": None,
        "scheme_name":      "Chief Minister's Breakfast Scheme (Expanded)",
    },

    # ── Infrastructure (Healthcare & Urban) ──────────────────────────────────

    {
        "category":       "Infrastructure",
        "promise_text_en": (
            "Double the number of dialysis machines at all government hospitals to "
            "improve access for renal patients across Tamil Nadu."
        ),
        "promise_text_ta": (
            "சிறுநீரக நோயாளிகளுக்கான அணுகலை மேம்படுத்த அனைத்து அரசு "
            "மருத்துவமனைகளிலும் டயாலிசிஸ் இயந்திரங்களின் எண்ணிக்கையை இரட்டிப்பு "
            "செய்வோம்."
        ),
        "stance_vibe":      "Welfare-centric",
        "amount_mentioned": "2× existing dialysis capacity",
        "scheme_name":      None,
    },
    {
        "category":       "Infrastructure",
        "promise_text_en": (
            "Develop new 'Global Cities' near Tiruchi, Madurai, Coimbatore, and Salem "
            "with world-class infrastructure to manage urban population growth and "
            "reduce pressure on Chennai."
        ),
        "promise_text_ta": (
            "நகர்ப்புற மக்கள்தொகை வளர்ச்சியை நிர்வகிக்கவும் சென்னையின் மீதான "
            "அழுத்தத்தை குறைக்கவும் திருச்சி, மதுரை, கோயம்புத்தூர் மற்றும் சேலம் "
            "அருகே புதிய 'உலக நகரங்கள்' உருவாக்கப்படும்."
        ),
        "stance_vibe":      "Infrastructure-heavy",
        "amount_mentioned": "4 new Global Cities",
        "scheme_name":      "Global Cities Initiative",
    },
    {
        "category":       "Infrastructure",
        "promise_text_en": (
            "Develop new minor ports and upgrade existing ports at Nagapattinam and "
            "Cuddalore to world-class standards to boost maritime trade and coastal "
            "livelihoods."
        ),
        "promise_text_ta": (
            "புதிய சிறு துறைமுகங்கள் அமைக்கப்படும்; நாகப்பட்டினம் மற்றும் "
            "கடலூர் துறைமுகங்கள் உலகத்தரம் வாய்ந்தவையாக மேம்படுத்தப்படும் — "
            "கடல்சார் வணிகம் மற்றும் கடற்கரை வாழ்வாதாரங்களை வலுப்படுத்த."
        ),
        "stance_vibe":      "Infrastructure-heavy",
        "amount_mentioned": None,
        "scheme_name":      None,
    },
]


# ---------------------------------------------------------------------------
# Build Firestore records
# ---------------------------------------------------------------------------

def build_records() -> list[dict[str, Any]]:
    cat_abbrev = {
        "Agriculture":      "agri",
        "Education":        "edu",
        "TASMAC & Revenue": "tasmac",
        "Women's Welfare":  "women",
        "Infrastructure":   "infra",
    }
    counters: dict[str, int] = defaultdict(int)
    records = []

    for p in PROMISES:
        abbrev = cat_abbrev[p["category"]]
        counters[abbrev] += 1
        doc_id = f"{PARTY_ID}_{TARGET_YEAR}_{abbrev}_supp_{counters[abbrev]:03d}"

        records.append({
            "doc_id":                    doc_id,
            "party_id":                  PARTY_ID,
            "party_name":                PARTY_NAME,
            "party_color":               PARTY_COLOR,
            "category":                  p["category"],
            "promise_text_en":           p["promise_text_en"],
            "promise_text_ta":           p.get("promise_text_ta", ""),
            "target_year":               TARGET_YEAR,
            "status":                    "Proposed",
            "stance_vibe":               p["stance_vibe"],
            "amount_mentioned":          p.get("amount_mentioned"),
            "scheme_name":               p.get("scheme_name"),
            "manifesto_pdf_url":         MANIFESTO_PDF_URL,
            "manifesto_pdf_page":        None,
            "source_notes":              SOURCE_NOTES,
            "ground_truth_confidence":   "HIGH",
            "is_joint_manifesto":        False,
            "track_fulfillment":         True,
            "_extracted_at":             NOW_ISO,
        })

    return records


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

def upload(records: list[dict], project_id: str, dry_run: bool) -> None:
    if dry_run:
        from collections import Counter
        print(f"\n[DRY RUN] {len(records)} records would be written to `{COLLECTION}`:\n")
        by_cat = Counter(r["category"] for r in records)
        for cat, n in sorted(by_cat.items()):
            print(f"  {cat:<25}  {n} promises")
        print()
        for r in records:
            en = r["promise_text_en"]
            preview = en[:90] + "…" if len(en) > 90 else en
            print(f"  [{r['doc_id']}]  {preview}")
        return

    if firestore is None:
        sys.exit("ERROR: google-cloud-firestore not installed. Run: pip install google-cloud-firestore")

    db = firestore.Client(project=project_id)
    col = db.collection(COLLECTION)
    written = 0
    for rec in records:
        col.document(rec["doc_id"]).set(rec, merge=True)
        written += 1
        print(f"  ✓ {rec['doc_id']}")
    print(f"\nDone — {written} docs written to `{COLLECTION}`")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Upload missing DMK 2026 manifesto promises to Firestore"
    )
    ap.add_argument("--dry-run", action="store_true",
                    help="Print records without uploading")
    ap.add_argument("--project", default="naatunadappu",
                    help="GCP project ID (default: naatunadappu)")
    args = ap.parse_args()

    records = build_records()
    print(f"Built {len(records)} DMK 2026 supplement records")

    from collections import Counter
    by_cat = Counter(r["category"] for r in records)
    for cat, n in sorted(by_cat.items()):
        print(f"  {cat:<25}  {n}")

    upload(records, args.project, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
