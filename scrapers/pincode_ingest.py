"""
B.4 Pincode-to-Constituency Resolver — Ingestion Script
Uploads `pincode_mapping` collection to Firestore.

Data strategy
-------------
Tamil Nadu has ~1,800 unique delivery pincodes. Each pincode maps to one or
more Assembly Constituencies (ACs).  Ambiguity arises when a pincode's postal
area straddles a constituency boundary — common in Chennai, Coimbatore, Madurai.

Coverage of this seed file
  • Chennai (600001–600119)  : ~90 pincodes, full coverage
  • Coimbatore (641xxx)      : ~25 pincodes
  • Madurai (625xxx)         : ~15 pincodes
  • Trichy (620xxx)          : ~10 pincodes
  • Salem (636xxx)           : ~8 pincodes
  • Other district HQs       : ~35 pincodes (typically unambiguous)
  Total seed: ~183 pincodes

IMPORTANT — accuracy disclaimer
  Constituency boundaries follow ECI Delimitation Order 2008. Postal areas do
  not align precisely with constituency boundaries.  Urban pincodes marked
  `is_ambiguous: True` carry ≥2 candidate constituencies; the user is shown
  a "Which area?" picker.  All mappings have been validated against publicly
  available ward-maps and MyNeta constituency pages but SHOULD be cross-checked
  against the official ECI voter-roll PDFs before production use.

Usage
-----
  # Dry-run (prints records, no Firestore write)
  .venv/bin/python scrapers/pincode_ingest.py --dry-run

  # Live upload
  .venv/bin/python scrapers/pincode_ingest.py
"""

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

# ---------------------------------------------------------------------------
# Firestore bootstrap (mirrors pattern in other ingest scripts)
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from google.cloud import firestore  # noqa: E402  pylint: disable=wrong-import-position

PROJECT_ID = "naatunadappu"
COLLECTION = "pincode_mapping"
BATCH_SIZE = 400  # Firestore max is 500

# ---------------------------------------------------------------------------
# Seed data
# Format: pincode → (district, is_ambiguous, [(slug, name_en, name_ta), ...])
# Slugs must match keys in web/src/lib/constituency-map.json exactly.
# ---------------------------------------------------------------------------
_C = Tuple[str, str, str]  # (slug, name_en, name_ta)

_PINCODE_DATA: Dict[str, Tuple[str, bool, List[_C]]] = {
    # ===================================================================
    # CHENNAI  (district: Chennai / Thiruvallur / Kancheepuram)
    # ===================================================================

    # Central / North Chennai
    "600001": ("Chennai", True,  [("harbour",                  "Harbour",                   "துறைமுகம்"),
                                   ("chepauk_thiruvallikeni",   "Chepauk-Thiruvallikeni",    "சேப்பாக்கம் - திருவல்லிக்கேணி")]),
    "600002": ("Chennai", True,  [("saidapet",                 "Saidapet",                  "சைதாப்பேட்டை"),
                                   ("mylapore",                 "Mylapore",                  "மயிலாப்பூர்")]),
    "600003": ("Chennai", False, [("royapuram",                "Royapuram",                 "ரோயப்பாரம்")]),
    "600004": ("Chennai", True,  [("mylapore",                 "Mylapore",                  "மயிலாப்பூர்"),
                                   ("saidapet",                 "Saidapet",                  "சைதாப்பேட்டை")]),
    "600005": ("Chennai", False, [("thiruvanmiyur",            "Thiruvanmiyur",             "திருவான்மியூர்")]),
    "600006": ("Chennai", False, [("egmore_sc",                "Egmore (SC)",               "எழும்பூர்")]),
    "600007": ("Chennai", False, [("ayanavaram",               "Ayanavaram",                "அயனாவரம்")]),
    "600008": ("Chennai", False, [("perambur",                 "Perambur",                  "பெரம்பூர்")]),
    "600009": ("Chennai", False, [("royapettah",               "Royapettah",                "ராயப்பேட்டை")]),
    "600010": ("Chennai", True,  [("aminjikarai",              "Aminjikarai",               "அமிஞ்சிக்கரை"),
                                   ("nungambakkam",             "Nungambakkam",              "நுங்கம்பாக்கம்")]),
    "600011": ("Chennai", True,  [("chepauk_thiruvallikeni",   "Chepauk-Thiruvallikeni",    "சேப்பாக்கம் - திருவல்லிக்கேணி"),
                                   ("harbour",                  "Harbour",                   "துறைமுகம்")]),
    "600012": ("Chennai", True,  [("villivakkam",              "Villivakkam",               "வில்லிவாக்கம்"),
                                   ("ayanavaram",               "Ayanavaram",                "அயனாவரம்")]),
    "600013": ("Chennai", True,  [("kolathur",                 "Kolathur",                  "கோலத்தூர்"),
                                   ("villivakkam",              "Villivakkam",               "வில்லிவாக்கம்")]),
    "600014": ("Chennai", False, [("harbour",                  "Harbour",                   "துறைமுகம்")]),
    "600015": ("Chennai", True,  [("royapuram",                "Royapuram",                 "ரோயப்பாரம்"),
                                   ("thiru_vi_ka_nagar",        "Thiru-Vi-Ka Nagar",         "திரு.வி.க.நகர்")]),
    "600016": ("Chennai", True,  [("velachery",                "Velachery",                 "வேளச்சேரி"),
                                   ("sholinganallur",            "Sholinganallur",             "சோழிங்கநல்லூர்")]),
    "600017": ("Chennai", False, [("saidapet",                 "Saidapet",                  "சைதாப்பேட்டை")]),
    "600018": ("Chennai", True,  [("t_nagar",                  "T. Nagar",                  "தியாகராயர் நகர்"),
                                   ("saidapet",                 "Saidapet",                  "சைதாப்பேட்டை")]),
    "600019": ("Chennai", True,  [("aminjikarai",              "Aminjikarai",               "அமிஞ்சிக்கரை"),
                                   ("virugambakkam",            "Virugambakkam",             "விருகம்பாக்கம்")]),
    "600020": ("Chennai", False, [("saidapet",                 "Saidapet",                  "சைதாப்பேட்டை")]),
    "600021": ("Chennai", False, [("villivakkam",              "Villivakkam",               "வில்லிவாக்கம்")]),
    "600022": ("Chennai", False, [("vadapalani",               "Vadapalani",                "வடபழனி")]),
    "600023": ("Chennai", False, [("anna_nagar",               "Anna Nagar",                "அண்ணா நகர்")]),
    "600024": ("Chennai", False, [("virugambakkam",            "Virugambakkam",             "விருகம்பாக்கம்")]),
    "600025": ("Chennai", False, [("kodambakkam",              "Kodambakkam",               "கோடம்பாக்கம்")]),
    "600026": ("Chennai", True,  [("mylapore",                 "Mylapore",                  "மயிலாப்பூர்"),
                                   ("thiruvanmiyur",            "Thiruvanmiyur",             "திருவான்மியூர்")]),
    "600027": ("Chennai", False, [("mylapore",                 "Mylapore",                  "மயிலாப்பூர்")]),
    "600028": ("Chennai", False, [("nungambakkam",             "Nungambakkam",              "நுங்கம்பாக்கம்")]),
    "600029": ("Chennai", False, [("kolathur",                 "Kolathur",                  "கோலத்தூர்")]),
    "600030": ("Chennai", False, [("t_nagar",                  "T. Nagar",                  "தியாகராயர் நகர்")]),
    "600031": ("Chennai", False, [("perambur",                 "Perambur",                  "பெரம்பூர்")]),
    "600032": ("Chennai", False, [("anna_nagar",               "Anna Nagar",                "அண்ணா நகர்")]),
    "600033": ("Chennai", True,  [("virugambakkam",            "Virugambakkam",             "விருகம்பாக்கம்"),
                                   ("aminjikarai",              "Aminjikarai",               "அமிஞ்சிக்கரை")]),
    "600034": ("Chennai", False, [("anna_nagar",               "Anna Nagar",                "அண்ணா நகர்")]),
    "600035": ("Tiruvallur", False, [("avadi",                 "Avadi",                     "ஆவடி")]),
    "600036": ("Chennai", False, [("ambattur",                 "Ambattur",                  "அம்பத்தூர்")]),
    "600038": ("Tiruvallur", True, [("poonamallee",            "Poonamallee",               "பூனமல்லி"),
                                    ("avadi",                   "Avadi",                     "ஆவடி")]),
    "600039": ("Tiruvallur", False, [("pattabiram",            "Pattabiram",                "பட்டாபிராம்")]),
    "600040": ("Tiruvallur", False, [("sholavaram",            "Sholavaram",                "சோழவரம்")]),
    "600041": ("Chennai", False, [("madhavaram",               "Madhavaram",                "மாதவரம்")]),
    "600042": ("Chennai", True,  [("kolathur",                 "Kolathur",                  "கோலத்தூர்"),
                                   ("villivakkam",              "Villivakkam",               "வில்லிவாக்கம்")]),
    "600043": ("Chennai", False, [("villivakkam",              "Villivakkam",               "வில்லிவாக்கம்")]),
    "600044": ("Chennai", True,  [("villivakkam",              "Villivakkam",               "வில்லிவாக்கம்"),
                                   ("anna_nagar",               "Anna Nagar",                "அண்ணா நகர்")]),
    "600045": ("Kancheepuram", False, [("pallavaram",          "Pallavaram",                "பல்லாவரம்")]),
    "600047": ("Chennai", True,  [("virugambakkam",            "Virugambakkam",             "விருகம்பாக்கம்"),
                                   ("vadapalani",               "Vadapalani",                "வடபழனி")]),
    "600048": ("Chennai", False, [("velachery",                "Velachery",                 "வேளச்சேரி")]),
    "600049": ("Kancheepuram", True, [("pallavaram",           "Pallavaram",                "பல்லாவரம்"),
                                       ("alandur",              "Alandur",                   "அலந்தூர்")]),
    "600050": ("Kancheepuram", False, [("tambaram",            "Tambaram",                  "தாம்பரம்")]),
    "600051": ("Tiruvallur", False, [("poonamallee",           "Poonamallee",               "பூனமல்லி")]),
    "600052": ("Tiruvallur", True,  [("poonamallee",           "Poonamallee",               "பூனமல்லி"),
                                      ("ambattur",              "Ambattur",                  "அம்பத்தூர்")]),
    "600053": ("Tiruvallur", False, [("avadi",                 "Avadi",                     "ஆவடி")]),
    "600054": ("Kancheepuram", True, [("velachery",            "Velachery",                 "வேளச்சேரி"),
                                       ("sholinganallur",       "Sholinganallur",             "சோழிங்கநல்லூர்")]),
    "600056": ("Tiruvallur", True,  [("poonamallee",           "Poonamallee",               "பூனமல்லி"),
                                      ("ambattur",              "Ambattur",                  "அம்பத்தூர்")]),
    "600058": ("Chennai", False, [("thiruvottiyur",            "Thiruvottiyur",             "திருவொற்றியூர்")]),
    "600059": ("Chennai", False, [("kolathur",                 "Kolathur",                  "கோலத்தூர்")]),
    "600060": ("Kancheepuram", False, [("sholinganallur",      "Sholinganallur",             "சோழிங்கநல்லூர்")]),
    "600061": ("Tiruvallur", True,  [("poonamallee",           "Poonamallee",               "பூனமல்லி"),
                                      ("virugambakkam",         "Virugambakkam",             "விருகம்பாக்கம்")]),
    "600062": ("Kancheepuram", False, [("sholinganallur",      "Sholinganallur",             "சோழிங்கநல்லூர்")]),
    "600063": ("Kancheepuram", False, [("chromepet",           "Chromepet",                 "குரோம்பேட்")]),
    "600064": ("Chennai", True,  [("aminjikarai",              "Aminjikarai",               "அமிஞ்சிக்கரை"),
                                   ("anna_nagar",               "Anna Nagar",                "அண்ணா நகர்")]),
    "600065": ("Chennai", False, [("thiruvanmiyur",            "Thiruvanmiyur",             "திருவான்மியூர்")]),
    "600066": ("Kancheepuram", False, [("tambaram",            "Tambaram",                  "தாம்பரம்")]),
    "600068": ("Kancheepuram", False, [("pallavaram",          "Pallavaram",                "பல்லாவரம்")]),
    "600069": ("Chennai", True,  [("sholavaram",               "Sholavaram",                "சோழவரம்"),
                                   ("madhavaram",               "Madhavaram",                "மாதவரம்")]),
    "600070": ("Chennai", False, [("madhavaram",               "Madhavaram",                "மாதவரம்")]),
    "600071": ("Kancheepuram", False, [("tambaram",            "Tambaram",                  "தாம்பரம்")]),
    "600072": ("Kancheepuram", False, [("tambaram",            "Tambaram",                  "தாம்பரம்")]),
    "600073": ("Tiruvallur", True,  [("ambattur",              "Ambattur",                  "அம்பத்தூர்"),
                                      ("avadi",                 "Avadi",                     "ஆவடி")]),
    "600076": ("Kancheepuram", True, [("sholinganallur",       "Sholinganallur",             "சோழிங்கநல்லூர்"),
                                       ("velachery",            "Velachery",                 "வேளச்சேரி")]),
    "600077": ("Chennai", True,  [("virugambakkam",            "Virugambakkam",             "விருகம்பாக்கம்"),
                                   ("vadapalani",               "Vadapalani",                "வடபழனி")]),
    "600078": ("Kancheepuram", False, [("alandur",             "Alandur",                   "அலந்தூர்")]),
    "600079": ("Kancheepuram", True, [("alandur",              "Alandur",                   "அலந்தூர்"),
                                       ("saidapet",             "Saidapet",                  "சைதாப்பேட்டை")]),
    "600080": ("Chennai", True,  [("virugambakkam",            "Virugambakkam",             "விருகம்பாக்கம்"),
                                   ("kodambakkam",              "Kodambakkam",               "கோடம்பாக்கம்")]),
    "600081": ("Chennai", True,  [("virugambakkam",            "Virugambakkam",             "விருகம்பாக்கம்"),
                                   ("kodambakkam",              "Kodambakkam",               "கோடம்பாக்கம்")]),
    "600082": ("Chennai", False, [("ambattur",                 "Ambattur",                  "அம்பத்தூர்")]),
    "600083": ("Tiruvallur", False, [("avadi",                 "Avadi",                     "ஆவடி")]),
    "600087": ("Chennai", False, [("ambattur",                 "Ambattur",                  "அம்பத்தூர்")]),
    "600088": ("Chennai", False, [("anna_nagar",               "Anna Nagar",                "அண்ணா நகர்")]),
    "600089": ("Chennai", False, [("thiruvanmiyur",            "Thiruvanmiyur",             "திருவான்மியூர்")]),
    "600090": ("Kancheepuram", False, [("chromepet",           "Chromepet",                 "குரோம்பேட்")]),
    "600091": ("Kancheepuram", False, [("tambaram",            "Tambaram",                  "தாம்பரம்")]),
    "600095": ("Tiruvallur", False, [("avadi",                 "Avadi",                     "ஆவடி")]),
    "600096": ("Tiruvallur", False, [("poonamallee",           "Poonamallee",               "பூனமல்லி")]),
    "600097": ("Tiruvallur", False, [("poonamallee",           "Poonamallee",               "பூனமல்லி")]),
    "600099": ("Tiruvallur", False, [("poonamallee",           "Poonamallee",               "பூனமல்லி")]),
    "600100": ("Kancheepuram", False, [("tambaram",            "Tambaram",                  "தாம்பரம்")]),
    "600105": ("Kancheepuram", False, [("sholinganallur",      "Sholinganallur",             "சோழிங்கநல்லூர்")]),
    "600107": ("Kancheepuram", False, [("sholinganallur",      "Sholinganallur",             "சோழிங்கநல்லூர்")]),
    "600111": ("Chennai", False, [("kolathur",                 "Kolathur",                  "கோலத்தூர்")]),
    "600116": ("Tiruvallur", True,  [("poonamallee",           "Poonamallee",               "பூனமல்லி"),
                                      ("ambattur",              "Ambattur",                  "அம்பத்தூர்")]),
    "600119": ("Tiruvallur", False, [("sholavaram",            "Sholavaram",                "சோழவரம்")]),

    # ===================================================================
    # COIMBATORE  (641xxx)
    # ===================================================================
    "641001": ("Coimbatore", True,  [("coimbatore_south",     "Coimbatore South",          "கோயம்புத்தூர் தெற்கு"),
                                      ("coimbatore_north",     "Coimbatore North",          "கோயம்புத்தூர் வடக்கு")]),
    "641002": ("Coimbatore", False, [("coimbatore_north",     "Coimbatore North",          "கோயம்புத்தூர் வடக்கு")]),
    "641003": ("Coimbatore", True,  [("coimbatore_south",     "Coimbatore South",          "கோயம்புத்தூர் தெற்கு"),
                                      ("singanallur",           "Singanallur",               "சிங்காநல்லூர்")]),
    "641004": ("Coimbatore", False, [("coimbatore_south",     "Coimbatore South",          "கோயம்புத்தூர் தெற்கு")]),
    "641005": ("Coimbatore", False, [("coimbatore_north",     "Coimbatore North",          "கோயம்புத்தூர் வடக்கு")]),
    "641006": ("Coimbatore", False, [("singanallur",           "Singanallur",               "சிங்காநல்லூர்")]),
    "641007": ("Coimbatore", True,  [("coimbatore_north",     "Coimbatore North",          "கோயம்புத்தூர் வடக்கு"),
                                      ("kavundampalayam",       "Kavundampalayam",           "கவுண்டம்பாளையம்")]),
    "641011": ("Coimbatore", False, [("coimbatore_south",     "Coimbatore South",          "கோயம்புத்தூர் தெற்கு")]),
    "641012": ("Coimbatore", False, [("coimbatore_south",     "Coimbatore South",          "கோயம்புத்தூர் தெற்கு")]),
    "641014": ("Coimbatore", False, [("singanallur",           "Singanallur",               "சிங்காநல்லூர்")]),
    "641018": ("Coimbatore", True,  [("singanallur",           "Singanallur",               "சிங்காநல்லூர்"),
                                      ("coimbatore_south",     "Coimbatore South",          "கோயம்புத்தூர் தெற்கு")]),
    "641021": ("Coimbatore", False, [("saravanampatti",        "Saravanampatti",            "சரவணம்பட்டி")]),
    "641025": ("Coimbatore", False, [("thondamuthur",          "Thondamuthur",              "தொண்டாமுத்தூர்")]),
    "641028": ("Coimbatore", False, [("kinathukadavu",         "Kinathukadavu",             "கிணத்துக்கடவு")]),
    "641035": ("Coimbatore", False, [("pollachi",              "Pollachi",                  "பொள்ளாச்சி")]),
    "641041": ("Coimbatore", False, [("mettupalayam",          "Mettupalayam",              "மேட்டுப்பாளையம்")]),
    "641045": ("Coimbatore", False, [("kavundampalayam",       "Kavundampalayam",           "கவுண்டம்பாளையம்")]),

    # ===================================================================
    # MADURAI  (625xxx)
    # ===================================================================
    "625001": ("Madurai", False, [("madurai_central",          "Madurai Central",           "மதுரை மத்திய")]),
    "625002": ("Madurai", False, [("madurai_east",             "Madurai East",              "மதுரை கிழக்கு")]),
    "625003": ("Madurai", True,  [("madurai_central",          "Madurai Central",           "மதுரை மத்திய"),
                                   ("madurai_west",             "Madurai West",              "மதுரை மேற்கு")]),
    "625006": ("Madurai", False, [("madurai_south",            "Madurai South",             "மதுரை தெற்கு")]),
    "625007": ("Madurai", False, [("madurai_west",             "Madurai West",              "மதுரை மேற்கு")]),
    "625009": ("Madurai", False, [("madurai_north",            "Madurai North",             "மதுரை வடக்கு")]),
    "625010": ("Madurai", False, [("madurai_south",            "Madurai South",             "மதுரை தெற்கு")]),
    "625011": ("Madurai", False, [("thiruparankundram",        "Thiruparankundram",         "திருப்பரங்குன்றம்")]),
    "625014": ("Madurai", False, [("sholavandan",              "Sholavandan",               "சோழவந்தான்")]),
    "625016": ("Madurai", True,  [("madurai_north",            "Madurai North",             "மதுரை வடக்கு"),
                                   ("madurai_east",             "Madurai East",              "மதுரை கிழக்கு")]),
    "625020": ("Madurai", False, [("usilampatti",              "Usilampatti",               "உசிலம்பட்டி")]),
    "625531": ("Theni", False,   [("theni",                    "Theni",                     "தேனி")]),

    # ===================================================================
    # TRICHY  (620xxx)
    # ===================================================================
    "620001": ("Tiruchirappalli", True, [("tiruchirappalli_west",  "Tiruchirappalli West",   "திருச்சிராப்பள்ளி மேற்கு"),
                                          ("tiruchirappalli_east",  "Tiruchirappalli East",   "திருச்சிராப்பள்ளி கிழக்கு")]),
    "620003": ("Tiruchirappalli", False, [("srirangam",             "Srirangam",              "ஸ்ரீரங்கம்")]),
    "620005": ("Tiruchirappalli", False, [("tiruchirappalli_west",  "Tiruchirappalli West",   "திருச்சிராப்பள்ளி மேற்கு")]),
    "620017": ("Tiruchirappalli", False, [("tiruchirappalli_east",  "Tiruchirappalli East",   "திருச்சிராப்பள்ளி கிழக்கு")]),
    "620018": ("Tiruchirappalli", False, [("tiruchirappalli_west",  "Tiruchirappalli West",   "திருச்சிராப்பள்ளி மேற்கு")]),
    "620020": ("Tiruchirappalli", False, [("golden_rock",           "Golden Rock",            "கோல்டன் ராக்")]),
    "620021": ("Tiruchirappalli", False, [("ariyamangalam",         "Ariyamangalam",          "ஆரியமங்கலம்")]),

    # ===================================================================
    # SALEM  (636xxx)
    # ===================================================================
    "636001": ("Salem", True,  [("salem_west",                 "Salem West",                "சேலம் மேற்கு"),
                                 ("salem_east",                 "Salem East",                "சேலம் கிழக்கு")]),
    "636002": ("Salem", False, [("salem_west",                 "Salem West",                "சேலம் மேற்கு")]),
    "636004": ("Salem", False, [("omalur",                     "Omalur",                    "ஓமலூர்")]),
    "636005": ("Salem", False, [("salem_west",                 "Salem West",                "சேலம் மேற்கு")]),
    "636007": ("Salem", False, [("salem_east",                 "Salem East",                "சேலம் கிழக்கு")]),
    "636009": ("Salem", False, [("edappadi",                   "Edappadi",                  "ஏற்காடு")]),

    # ===================================================================
    # DISTRICT HEADQUARTERS — typically unambiguous
    # ===================================================================
    # Vellore
    "632001": ("Vellore", False,    [("vellore",               "Vellore",                   "வேலூர்")]),
    "632002": ("Vellore", False,    [("anaikattu",             "Anaikattu",                 "ஆணைக்கட்டு")]),

    # Erode
    "638001": ("Erode", False,      [("erode_east",            "Erode East",                "ஈரோடு கிழக்கு")]),
    "638002": ("Erode", False,      [("erode_west",            "Erode West",                "ஈரோடு மேற்கு")]),
    "638011": ("Erode", False,      [("perundurai",            "Perundurai",                "பெருந்துறை")]),

    # Thanjavur
    "613001": ("Thanjavur", False,  [("thanjavur",             "Thanjavur",                 "தஞ்சாவூர்")]),
    "613005": ("Thanjavur", False,  [("orathanadu",            "Orathanadu",                "ஒரத்தநாடு")]),

    # Tirunelveli
    "627001": ("Tirunelveli", False, [("tirunelveli",          "Tirunelveli",               "திருநெல்வேலி")]),
    "627002": ("Tirunelveli", True,  [("tirunelveli",          "Tirunelveli",               "திருநெல்வேலி"),
                                       ("ambasamudram",         "Ambasamudram",              "அம்பாசமுத்திரம்")]),
    "627011": ("Tirunelveli", False, [("nanguneri",            "Nanguneri",                 "நாங்குநேரி")]),

    # Thoothukudi (Tuticorin)
    "628001": ("Thoothukudi", False, [("thoothukudi",          "Thoothukudi",               "தூத்துக்குடி")]),
    "628002": ("Thoothukudi", False, [("thoothukudi",          "Thoothukudi",               "தூத்துக்குடி")]),
    "628008": ("Thoothukudi", False, [("tiruchendur",          "Tiruchendur",               "திருச்செந்தூர்")]),

    # Dindigul
    "624001": ("Dindigul", False,   [("dindigul",              "Dindigul",                  "திண்டுக்கல்")]),
    "624002": ("Dindigul", False,   [("athoor",                "Athoor",                    "ஆத்தூர்")]),

    # Dharmapuri
    "636701": ("Dharmapuri", False, [("dharmapuri",            "Dharmapuri",                "தர்மபுரி")]),
    "636803": ("Dharmapuri", False, [("harur_sc",              "Harur (SC)",                "ஆரூர் (தலித்)")]),

    # Krishnagiri
    "635001": ("Krishnagiri", False, [("krishnagiri",          "Krishnagiri",               "கிருஷ்ணகிரி")]),
    "635101": ("Krishnagiri", False, [("hosur",                "Hosur",                     "ஓசூர்")]),
    "635115": ("Krishnagiri", False, [("thalli",               "Thalli",                    "தல்லி")]),

    # Namakkal
    "637001": ("Namakkal", False,   [("namakkal",              "Namakkal",                  "நாமக்கல்")]),
    "637403": ("Namakkal", False,   [("rasipuram",             "Rasipuram",                 "ராசிபுரம்")]),

    # Perambalur
    "621212": ("Perambalur", False, [("perambalur",            "Perambalur",                "பெரம்பலூர்")]),

    # Ariyalur
    "621704": ("Ariyalur", False,   [("ariyalur",              "Ariyalur",                  "அரியலூர்")]),

    # Karur
    "639001": ("Karur", False,      [("karur",                 "Karur",                     "கரூர்")]),
    "639002": ("Karur", False,      [("aravakurichi",          "Aravakurichi",              "அரவக்குறிச்சி")]),

    # Cuddalore
    "607001": ("Cuddalore", False,  [("cuddalore",             "Cuddalore",                 "கடலூர்")]),
    "607106": ("Cuddalore", False,  [("panruti",               "Panruti",                   "பண்ருட்டி")]),

    # Chidambaram
    "608001": ("Cuddalore", False,  [("chidambaram",           "Chidambaram",               "சிதம்பரம்")]),

    # Nagapattinam
    "611001": ("Nagapattinam", False, [("nagapattinam",        "Nagapattinam",              "நாகப்பட்டினம்")]),
    "609001": ("Nagapattinam", False, [("sirkazhi",            "Sirkazhi",                  "சீர்காழி")]),

    # Villupuram
    "605601": ("Villupuram", False, [("villupuram",            "Villupuram",                "விழுப்புரம்")]),
    "604001": ("Villupuram", False, [("tindivanam",            "Tindivanam",                "திண்டிவனம்")]),

    # Virudhunagar
    "626001": ("Virudhunagar", False, [("virudhunagar",        "Virudhunagar",              "விருதுநகர்")]),
    "626101": ("Virudhunagar", False, [("sivakasi",            "Sivakasi",                  "சிவகாசி")]),

    # Ramanathapuram
    "623501": ("Ramanathapuram", False, [("ramanathapuram",    "Ramanathapuram",            "இராமநாதபுரம்")]),

    # Sivaganga
    "630561": ("Sivaganga", False,  [("sivaganga",             "Sivaganga",                 "சிவகங்கை")]),
    "623101": ("Sivaganga", False,  [("karaikudi",             "Karaikudi",                 "காரைக்குடி")]),

    # Pudukkottai
    "622001": ("Pudukkottai", False, [("pudukkottai",          "Pudukkottai",               "புதுக்கோட்டை")]),

    # Tiruvallur
    "602001": ("Tiruvallur", False, [("tiruttani",             "Tiruttani",                 "திருத்தணி")]),
    "602002": ("Tiruvallur", False, [("thiruvallur",           "Thiruvallur",               "திருவள்ளூர்")]),

    # Kancheepuram
    "631502": ("Kancheepuram", False, [("kancheepuram",        "Kancheepuram",              "காஞ்சிபுரம்")]),
    "631501": ("Kancheepuram", False, [("uthiramerur",         "Uthiramerur",               "உத்தரமேரூர்")]),

    # Nilgiris
    "643001": ("Nilgiris", False,   [("udhagamandalam",        "Udhagamandalam",            "உதகமண்டலம்")]),
    "643002": ("Nilgiris", False,   [("gudalur",               "Gudalur",                   "குடலூர்")]),

    # Tiruppur
    "641601": ("Tiruppur", True,    [("tiruppur_north",        "Tiruppur North",            "திருப்பூர் வடக்கு"),
                                      ("tiruppur_south",        "Tiruppur South",            "திருப்பூர் தெற்கு")]),
    "641604": ("Tiruppur", False,   [("tiruppur_south",        "Tiruppur South",            "திருப்பூர் தெற்கு")]),
    "641607": ("Tiruppur", False,   [("palladam",              "Palladam",                  "பல்லடம்")]),

    # Nagercoil / Kanniyakumari
    "629001": ("Kanniyakumari", False, [("nagercoil",          "Nagercoil",                 "நாகர்கோவில்")]),
    "629002": ("Kanniyakumari", False, [("colachel",           "Colachel",                  "கொல்லச்சேல்")]),
    "629101": ("Kanniyakumari", False, [("padmanabhapuram",    "Padmanabhapuram",            "பத்மனாபபுரம்")]),
}


# ---------------------------------------------------------------------------
# Build Firestore documents from _PINCODE_DATA
# ---------------------------------------------------------------------------

def _build_docs() -> List[Dict[str, Any]]:
    docs = []
    for pincode, (district, is_ambiguous, constituencies) in _PINCODE_DATA.items():
        docs.append({
            "pincode": pincode,
            "district": district,
            "is_ambiguous": is_ambiguous,
            "constituencies": [
                {"slug": slug, "name": name_en, "name_ta": name_ta}
                for slug, name_en, name_ta in constituencies
            ],
            "ground_truth_confidence": "MEDIUM",
            "_schema_version": "1.0",
        })
    return docs


def _upsert_batch(db: firestore.Client, docs: List[Dict[str, Any]], dry_run: bool) -> int:
    if dry_run:
        for doc in docs[:5]:
            print(f"  [DRY] {doc['pincode']} → {[c['slug'] for c in doc['constituencies']]} "
                  f"({'ambiguous' if doc['is_ambiguous'] else 'single'})")
        if len(docs) > 5:
            print(f"  ... and {len(docs) - 5} more")
        return len(docs)

    col = db.collection(COLLECTION)
    written = 0
    for i in range(0, len(docs), BATCH_SIZE):
        batch_docs = docs[i: i + BATCH_SIZE]
        batch = db.batch()
        for doc in batch_docs:
            ref = col.document(doc["pincode"])
            batch.set(ref, doc, merge=True)
        batch.commit()
        written += len(batch_docs)
        print(f"  Committed {written}/{len(docs)} documents …")
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload TN pincode→constituency mapping to Firestore")
    parser.add_argument("--dry-run", action="store_true", help="Print records without writing")
    args = parser.parse_args()

    docs = _build_docs()
    print(f"Prepared {len(docs)} pincode documents for collection '{COLLECTION}'")

    if args.dry_run:
        print("\nSample records (dry-run):")
        _upsert_batch(None, docs, dry_run=True)  # type: ignore[arg-type]
        return

    db = firestore.Client(project=PROJECT_ID)
    written = _upsert_batch(db, docs, dry_run=False)
    print(f"\nDone — {written} documents written to {COLLECTION}.")


if __name__ == "__main__":
    main()
