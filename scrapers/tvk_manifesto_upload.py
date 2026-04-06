"""
TVK Manifesto Upload — Tamil Nadu 2026
======================================
Manually curated promises sourced from news reports (no official PDF available).

Sources:
  - The Federal (thefederal.com) — federal.com/elections-2026/...tvk-releases-poll-manifesto
  - NewsBytesApp (newsbytesapp.com) — tvk-s-vijay-unveils-poll-manifesto
  - Asianet Newsable — vijays-tvk-manifesto-promises-antidrug-zones-student-aid

Usage
-----
  # Dry run (print records, no Firestore write)
  .venv/bin/python scrapers/tvk_manifesto_upload.py --dry-run

  # Upload to Firestore
  .venv/bin/python scrapers/tvk_manifesto_upload.py
"""

from __future__ import annotations

import argparse
import sys
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
COLLECTION   = "manifesto_promises"
PARTY_ID     = "tvk"
PARTY_NAME   = "TVK"
PARTY_COLOR  = "bg-sky-600"
TARGET_YEAR  = 2026
SOURCE_NOTES = (
    "Manually curated from news reports (no official PDF published). "
    "Sources: The Federal (thefederal.com), NewsBytesApp, Asianet Newsable — "
    "TVK manifesto released 29 March 2026."
)
NOW_ISO      = datetime.now(timezone.utc).isoformat()

# ---------------------------------------------------------------------------
# Promises
# Each dict: category, promise_text_en, promise_text_ta (or ""), stance_vibe,
#            amount_mentioned (or None), scheme_name (or None)
# ---------------------------------------------------------------------------
PROMISES: list[dict[str, Any]] = [

    # ── Education ────────────────────────────────────────────────────────────
    {
        "category":        "Education",
        "promise_text_en": (
            "Monthly unemployment allowance of ₹4,000 for graduates above 29 years "
            "without jobs, and ₹2,000 per month for diploma holders, paid through a "
            "Youth Welfare Fund."
        ),
        "promise_text_ta": (
            "29 வயதுக்கு மேல் வேலையில்லாத பட்டதாரிகளுக்கு மாதம் ₹4,000 மற்றும் "
            "டிப்ளமோ படித்தவர்களுக்கு மாதம் ₹2,000 வேலையில்லாத் திண்டாட்ட உதவித்தொகை."
        ),
        "stance_vibe":       "Welfare-centric",
        "amount_mentioned":  "₹4,000/month (graduates), ₹2,000/month (diploma holders)",
        "scheme_name":       "Youth Welfare Fund",
    },
    {
        "category":        "Education",
        "promise_text_en": (
            "Interest-free education loans up to ₹20 lakh for students from Class XII "
            "through PhD, with no collateral or guarantor required."
        ),
        "promise_text_ta": (
            "12-ஆம் வகுப்பு முதல் முனைவர் பட்டம் வரை ஆர்வமுள்ள மாணவர்களுக்கு "
            "எந்த ஜாமீனும் இல்லாமல் ₹20 லட்சம் வரை வட்டியில்லா கல்விக் கடன்."
        ),
        "stance_vibe":       "Welfare-centric",
        "amount_mentioned":  "₹20 lakh (interest-free)",
        "scheme_name":       None,
    },
    {
        "category":        "Education",
        "promise_text_en": (
            "Five lakh internship opportunities annually — graduates to receive ₹10,000 "
            "per month stipend and IT graduates ₹8,000 per month, created jointly by "
            "government and private sector."
        ),
        "promise_text_ta": (
            "ஆண்டுதோறும் 5 லட்சம் பயிற்சி வேலை வாய்ப்புகள் — பட்டதாரிகளுக்கு "
            "மாதம் ₹10,000 மற்றும் தகவல் தொழில்நுட்பப் பட்டதாரிகளுக்கு ₹8,000 உதவித்தொகை."
        ),
        "stance_vibe":       "Reform-oriented",
        "amount_mentioned":  "₹10,000/month (graduates), ₹8,000/month (IT graduates)",
        "scheme_name":       None,
    },
    {
        "category":        "Education",
        "promise_text_en": (
            "All government competitive and recruitment examinations to be conducted on "
            "schedule, eliminating chronic delays that affect students' career progression."
        ),
        "promise_text_ta": (
            "அரசு போட்டித் தேர்வுகளை தாமதமின்றி திட்டமிட்ட நேரத்தில் நடத்துவோம்."
        ),
        "stance_vibe":       "Reform-oriented",
        "amount_mentioned":  None,
        "scheme_name":       None,
    },
    {
        "category":        "Education",
        "promise_text_en": (
            "Establish 500 creative schools across Tamil Nadu and develop 1.5 lakh young "
            "creators into entrepreneurs under a Creative Entrepreneurs Scheme, positioning "
            "Tamil Nadu as a global creative hub."
        ),
        "promise_text_ta":   "",
        "stance_vibe":       "Reform-oriented",
        "amount_mentioned":  None,
        "scheme_name":       "Creative Entrepreneurs Scheme",
    },
    {
        "category":        "Education",
        "promise_text_en": (
            "Interest-free entrepreneurship loans up to ₹25 lakh for honest young "
            "entrepreneurs, without any guarantee or collateral required."
        ),
        "promise_text_ta": (
            "இளம் தொழில்முனைவோருக்கு எந்த ஜாமீனும் இல்லாமல் ₹25 லட்சம் வரை "
            "வட்டியில்லா கடன்."
        ),
        "stance_vibe":       "Reform-oriented",
        "amount_mentioned":  "₹25 lakh (interest-free)",
        "scheme_name":       None,
    },

    # ── Agriculture ──────────────────────────────────────────────────────────
    {
        "category":        "Agriculture",
        "promise_text_en": (
            "Full waiver of crop loans from agricultural cooperative banks for farmers "
            "with land holdings under 5 acres."
        ),
        "promise_text_ta": (
            "5 ஏக்கருக்கும் குறைவான நிலம் உள்ள விவசாயிகளின் வேளாண் கூட்டுறவு வங்கிக் "
            "கடன்களை முழுமையாக தள்ளுபடி செய்வோம்."
        ),
        "stance_vibe":       "Farmer-focused",
        "amount_mentioned":  None,
        "scheme_name":       None,
    },
    {
        "category":        "Agriculture",
        "promise_text_en": (
            "Up to 50% waiver of crop loans from agricultural cooperative banks for "
            "farmers with 5 acres or more."
        ),
        "promise_text_ta": (
            "5 ஏக்கர் மற்றும் அதற்கு மேல் நிலம் உள்ள விவசாயிகளின் கூட்டுறவு "
            "வங்கிக் கடன்களில் 50% வரை தள்ளுபடி செய்வோம்."
        ),
        "stance_vibe":       "Farmer-focused",
        "amount_mentioned":  "50% waiver",
        "scheme_name":       None,
    },
    {
        "category":        "Agriculture",
        "promise_text_en": (
            "Free higher education for children of small and marginal farmers (under 2 acres), "
            "restricted to families without existing government employees."
        ),
        "promise_text_ta": (
            "2 ஏக்கருக்கும் குறைவான நிலம் உள்ள சிறு, குறு விவசாயிகளின் பிள்ளைகளுக்கு "
            "இலவச உயர்கல்வி."
        ),
        "stance_vibe":       "Farmer-focused",
        "amount_mentioned":  None,
        "scheme_name":       None,
    },
    {
        "category":        "Agriculture",
        "promise_text_en": (
            "Strict legal action against illegal commissions and corrupt practices during "
            "loading and unloading at paddy procurement centres."
        ),
        "promise_text_ta": (
            "நெல் கொள்முதல் மையங்களில் ஏற்றி இறக்கும் போது நடக்கும் ஊழலுக்கு "
            "கடுமையான நடவடிக்கை எடுப்போம்."
        ),
        "stance_vibe":       "Farmer-focused",
        "amount_mentioned":  None,
        "scheme_name":       None,
    },
    {
        "category":        "Agriculture",
        "promise_text_en": (
            "One ration shop for every 500 family cards and appointment of certified "
            "weighers in every village to prevent short-weighing."
        ),
        "promise_text_ta": (
            "500 குடும்ப அட்டைகளுக்கு ஒரு ரேஷன் கடை மற்றும் ஒவ்வொரு கிராமத்திலும் "
            "நேர்மையான எடையாளர் நியமனம்."
        ),
        "stance_vibe":       "Welfare-centric",
        "amount_mentioned":  None,
        "scheme_name":       None,
    },

    # ── Women's Welfare ───────────────────────────────────────────────────────
    {
        "category":        "Women's Welfare",
        "promise_text_en": "Monthly financial assistance of ₹2,500 for women.",
        "promise_text_ta": "பெண்களுக்கு மாதம் ₹2,500 நிதி உதவி.",
        "stance_vibe":       "Women-focused",
        "amount_mentioned":  "₹2,500/month",
        "scheme_name":       None,
    },
    {
        "category":        "Women's Welfare",
        "promise_text_en": (
            "Six free LPG cooking gas cylinders annually for women, and free bus travel "
            "for women on all government-run buses without any restrictions."
        ),
        "promise_text_ta": (
            "பெண்களுக்கு ஆண்டுக்கு 6 இலவச சமையல் வாயு சிலிண்டர்கள் மற்றும் "
            "அரசு பேருந்துகளில் கட்டணமில்லா பயணம்."
        ),
        "stance_vibe":       "Women-focused",
        "amount_mentioned":  "6 cylinders/year (free)",
        "scheme_name":       None,
    },
    {
        "category":        "Women's Welfare",
        "promise_text_en": (
            "One sovereign of gold and a silk sari for brides from economically poor families."
        ),
        "promise_text_ta": (
            "வறுமை நிலையில் உள்ள குடும்பங்களில் திருமணமாகும் பெண்களுக்கு ஒரு "
            "பவுன் தங்கம் மற்றும் பட்டுப் புடவை."
        ),
        "stance_vibe":       "Women-focused",
        "amount_mentioned":  "1 sovereign gold + 1 silk sari",
        "scheme_name":       None,
    },
    {
        "category":        "Women's Welfare",
        "promise_text_en": (
            "Gold ring and a welcome kit for newborns, and dedicated safety response "
            "teams for women's security across the state."
        ),
        "promise_text_ta": (
            "புதிதாக பிறந்த குழந்தைகளுக்கு தங்க மோதிரம் மற்றும் வரவேற்பு தொகுப்பு; "
            "பெண்கள் பாதுகாப்பிற்கு சிறப்பு குழுக்கள்."
        ),
        "stance_vibe":       "Women-focused",
        "amount_mentioned":  None,
        "scheme_name":       None,
    },

    # ── Infrastructure ────────────────────────────────────────────────────────
    {
        "category":        "Infrastructure",
        "promise_text_en": (
            "Chief Minister's People's Service Friend Scheme: create 5 lakh local "
            "employment opportunities for youth through every village panchayat."
        ),
        "promise_text_ta": (
            "முதலமைச்சர் மக்கள் சேவை நண்பர் திட்டம்: ஒவ்வொரு கிராம பஞ்சாயத்திலும் "
            "5 லட்சம் இளைஞர்களுக்கு உள்ளூர் வேலை வாய்ப்பு."
        ),
        "stance_vibe":       "Infrastructure-heavy",
        "amount_mentioned":  "5 lakh jobs",
        "scheme_name":       "Chief Minister's People's Service Friend Scheme",
    },
    {
        "category":        "Infrastructure",
        "promise_text_en": (
            "Private companies hiring at least 75% Tamil natives to receive a 2.5% "
            "subsidy on state GST, 5% electricity tariff concession, and priority in "
            "government procurement contracts."
        ),
        "promise_text_ta": (
            "75% தமிழர்களை பணியமர்த்தும் தனியார் நிறுவனங்களுக்கு மாநில ஜிஎஸ்டியில் "
            "2.5% மானியம், மின்கட்டணத்தில் 5% சலுகை மற்றும் அரசு கொள்முதலில் முன்னுரிமை."
        ),
        "stance_vibe":       "Reform-oriented",
        "amount_mentioned":  "2.5% GST subsidy, 5% electricity concession",
        "scheme_name":       "Local Employment for Local People",
    },
    {
        "category":        "Infrastructure",
        "promise_text_en": (
            "Priority investment in education, healthcare, ration supply, drinking water, "
            "roads, and bus connectivity in all districts of Tamil Nadu."
        ),
        "promise_text_ta": (
            "கல்வி, சுகாதாரம், ரேஷன், குடிநீர், சாலை மற்றும் பேருந்து வசதிகளுக்கு "
            "முன்னுரிமை அளிப்போம்."
        ),
        "stance_vibe":       "Infrastructure-heavy",
        "amount_mentioned":  None,
        "scheme_name":       None,
    },
    {
        "category":        "Infrastructure",
        "promise_text_en": (
            "White papers to be published on all major government deals and contracts; "
            "governance focused on implementation and delivery over publicity and "
            "inauguration ceremonies."
        ),
        "promise_text_ta": (
            "அனைத்து முக்கிய அரசு ஒப்பந்தங்களுக்கும் வெள்ளை அறிக்கை வெளியிடுவோம்; "
            "விளம்பரத்தை விட செயல்பாட்டிற்கு முன்னுரிமை."
        ),
        "stance_vibe":       "Reform-oriented",
        "amount_mentioned":  None,
        "scheme_name":       None,
    },

    # ── TASMAC & Revenue ──────────────────────────────────────────────────────
    {
        "category":        "TASMAC & Revenue",
        "promise_text_en": (
            "Establish Anti-Drug Protection Zones around every school and college in "
            "Tamil Nadu, backed by strict new laws, to make the state completely drug-free."
        ),
        "promise_text_ta": (
            "தமிழ்நாட்டை முழுமையான போதைப்பொருள் இல்லா மாநிலமாக மாற்ற ஒவ்வொரு "
            "பள்ளி மற்றும் கல்லூரி சுற்றிலும் போதை எதிர்ப்பு பாதுகாப்பு மண்டலங்கள்."
        ),
        "stance_vibe":       "Reform-oriented",
        "amount_mentioned":  None,
        "scheme_name":       "Anti-Drug Protection Zones",
    },
    {
        "category":        "TASMAC & Revenue",
        "promise_text_en": (
            "Anti-drug forums to be set up in every school and college with community "
            "and parental involvement to combat substance abuse among youth."
        ),
        "promise_text_ta": (
            "ஒவ்வொரு பள்ளி மற்றும் கல்லூரியிலும் பெற்றோர் மற்றும் சமுதாயத்தின் "
            "பங்கேற்புடன் போதை எதிர்ப்பு மன்றங்கள் அமைப்போம்."
        ),
        "stance_vibe":       "Reform-oriented",
        "amount_mentioned":  None,
        "scheme_name":       None,
    },
    {
        "category":        "TASMAC & Revenue",
        "promise_text_en": (
            "Formation of a Tamil Nadu Youth Advisory Council to hear young people's "
            "views and include them in state policy decisions."
        ),
        "promise_text_ta": (
            "இளைஞர்களின் கருத்துகளை கொள்கை வகுப்பில் சேர்க்க "
            "தமிழ்நாடு இளைஞர் ஆலோசனைக் குழு அமைக்கப்படும்."
        ),
        "stance_vibe":       "Reform-oriented",
        "amount_mentioned":  None,
        "scheme_name":       "Tamil Nadu Youth Advisory Council",
    },
]

# ---------------------------------------------------------------------------

def build_records() -> list[dict[str, Any]]:
    from collections import defaultdict
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
        doc_id = f"{PARTY_ID}_{TARGET_YEAR}_{abbrev}_{counters[abbrev]:03d}"
        records.append({
            "doc_id":                    doc_id,
            "party_id":                  PARTY_ID,
            "party_name":                PARTY_NAME,
            "party_color":               PARTY_COLOR,
            "category":                  p["category"],
            "promise_text_en":           p["promise_text_en"],
            "promise_text_ta":           p["promise_text_ta"],
            "target_year":               TARGET_YEAR,
            "status":                    "Proposed",
            "stance_vibe":               p["stance_vibe"],
            "amount_mentioned":          p.get("amount_mentioned"),
            "scheme_name":               p.get("scheme_name"),
            "manifesto_pdf_url":         "",
            "manifesto_pdf_page":        0,
            "source_notes":              SOURCE_NOTES,
            "ground_truth_confidence":   "MEDIUM",
            "is_joint_manifesto":        False,
            "track_fulfillment":         True,
            "_extracted_at":             NOW_ISO,
        })
    return records


def upload(records: list[dict], project_id: str, dry_run: bool) -> None:
    if dry_run:
        print(f"\n[DRY RUN] {len(records)} records would be written to `{COLLECTION}`:\n")
        from collections import Counter
        by_cat = Counter(r["category"] for r in records)
        for cat, n in sorted(by_cat.items()):
            print(f"  {cat:<25}  {n} promises")
        print()
        for r in records:
            print(f"  {r['doc_id']}")
            print(f"    EN: {r['promise_text_en'][:80]}…" if len(r['promise_text_en']) > 80 else f"    EN: {r['promise_text_en']}")
        return

    if firestore is None:
        sys.exit("ERROR: google-cloud-firestore not installed")

    db = firestore.Client(project=project_id)
    col = db.collection(COLLECTION)
    written = 0
    for rec in records:
        col.document(rec["doc_id"]).set(rec, merge=True)
        written += 1
        print(f"  ✓ {rec['doc_id']}")
    print(f"\nDone — {written} docs written to `{COLLECTION}`")


def main() -> None:
    ap = argparse.ArgumentParser(description="Upload TVK 2026 manifesto promises to Firestore")
    ap.add_argument("--dry-run",   action="store_true", help="Print records without uploading")
    ap.add_argument("--project",   default="naatunadappu", help="GCP project ID")
    args = ap.parse_args()

    records = build_records()
    print(f"Built {len(records)} TVK 2026 manifesto records")
    upload(records, args.project, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
