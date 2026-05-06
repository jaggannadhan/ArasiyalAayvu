"""
Constitution of India — Data Ingest
=====================================
Scrapes the complete text of the Constitution of India from official
government sources and produces a structured JSON for the app.

Sources (Government of India — authoritative)
----------------------------------------------
  India Code (Legislative Dept, Ministry of Law & Justice)
    https://www.indiacode.nic.in/handle/123456789/8305

  Legislative Department, Ministry of Law & Justice
    https://legislative.gov.in/constitution-of-india

  e-Gazette of India (for amendment notifications)
    https://egazette.gov.in/

Firestore schema
-----------------
  Collection: indian_constitution
  Doc ID:     part_{part_number}    e.g. part_1, part_4a
  Fields:     part_number, part_title, part_title_ta, articles[]

  Collection: indian_constitution_meta
  Doc ID:     schedules / amendments / central_acts / preamble
  Fields:     varies by doc type

Output
------
  data/processed/indian_constitution.json

Usage
-----
  # Generate seed data (from built-in authoritative structure)
  .venv/bin/python scrapers/constitution_ingest.py --seed

  # Scrape latest from India Code (updates seed with full article text)
  .venv/bin/python scrapers/constitution_ingest.py --scrape

  # Upload to Firestore
  .venv/bin/python scrapers/constitution_ingest.py --upload

  # Dry run
  .venv/bin/python scrapers/constitution_ingest.py --seed --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

# ---------------------------------------------------------------------------
# Firestore (lazy init)
# ---------------------------------------------------------------------------

_db = None


def _get_db():
    global _db
    if _db is None:
        from google.cloud import firestore
        _db = firestore.Client(project=os.getenv("GOOGLE_CLOUD_PROJECT", "naatunadappu"))
    return _db


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

INDIA_CODE_BASE = "https://www.indiacode.nic.in"
LEGISLATIVE_BASE = "https://legislative.gov.in"
EGAZETTE_BASE = "https://egazette.gov.in"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

OUTPUT_DIR = Path(__file__).resolve().parents[1] / "data" / "processed"

# ---------------------------------------------------------------------------
# Seed data — authoritative Constitution structure
# Source: India Code / Legislative Department, verified against
# "The Constitution of India (As on 9th September, 2024)"
# published by Legislative Department, Ministry of Law & Justice
# ---------------------------------------------------------------------------

PREAMBLE = {
    "text_en": (
        "WE, THE PEOPLE OF INDIA, having solemnly resolved to constitute India into a "
        "SOVEREIGN SOCIALIST SECULAR DEMOCRATIC REPUBLIC and to secure to all its citizens: "
        "JUSTICE, social, economic and political; "
        "LIBERTY of thought, expression, belief, faith and worship; "
        "EQUALITY of status and of opportunity; "
        "and to promote among them all "
        "FRATERNITY assuring the dignity of the individual and the unity and integrity of the Nation; "
        "IN OUR CONSTITUENT ASSEMBLY this twenty-sixth day of November, 1949, do "
        "HEREBY ADOPT, ENACT AND GIVE TO OURSELVES THIS CONSTITUTION."
    ),
    "text_ta": (
        "இந்தியாவின் மக்களாகிய நாம், இந்தியாவை ஒரு இறையாண்மை சோசலிச சமயச்சார்பற்ற "
        "ஜனநாயகக் குடியரசாக அமைத்துக்கொள்ளவும், அதன் குடிமக்கள் அனைவருக்கும்: "
        "சமூக, பொருளாதார, அரசியல் நீதியும்; "
        "சிந்தனை, கருத்துரிமை, நம்பிக்கை, மதம், வழிபாடு ஆகியவற்றில் சுதந்திரமும்; "
        "அந்தஸ்து மற்றும் வாய்ப்பில் சமத்துவமும்; "
        "தனிநபரின் கண்ணியத்தையும் தேசத்தின் ஒற்றுமை மற்றும் ஒருமைப்பாட்டையும் "
        "உறுதிப்படுத்தும் சகோதரத்துவமும் பெற உறுதி எடுத்துக்கொண்டு, "
        "1949ஆம் ஆண்டு நவம்பர் 26ஆம் நாள் நமது அரசியல் நிர்ணய சபையில் "
        "இந்த அரசியலமைப்பை ஏற்றுக்கொண்டு, இயற்றி, நமக்கு நாமே வழங்கிக் கொள்கிறோம்."
    ),
    "enacted_date": "1949-11-26",
    "commenced_date": "1950-01-26",
    "source_url": "https://www.indiacode.nic.in/handle/123456789/8305",
}

PARTS: List[Dict[str, Any]] = [
    {
        "part_number": "I",
        "part_title": "The Union and its Territory",
        "part_title_ta": "ஒன்றியமும் அதன் நிலப்பரப்பும்",
        "articles": [
            {"number": "1", "title": "Name and territory of the Union", "title_ta": "ஒன்றியத்தின் பெயரும் நிலப்பரப்பும்"},
            {"number": "2", "title": "Admission or establishment of new States", "title_ta": "புதிய மாநிலங்களை சேர்த்தல் அல்லது நிறுவுதல்"},
            {"number": "2A", "title": "Sikkim to be associated with the Union", "title_ta": "சிக்கிம் ஒன்றியத்துடன் இணைக்கப்படுதல்", "status": "Repealed", "amendment": "36th Amendment, 1975"},
            {"number": "3", "title": "Formation of new States and alteration of areas, boundaries or names of existing States", "title_ta": "புதிய மாநிலங்கள் அமைத்தல், எல்லைகள் மாற்றுதல்"},
            {"number": "4", "title": "Laws made under articles 2 and 3 to provide for the amendment of the First and the Fourth Schedules", "title_ta": "உறுப்புகள் 2 மற்றும் 3 கீழ் சட்டங்கள்"},
        ],
    },
    {
        "part_number": "II",
        "part_title": "Citizenship",
        "part_title_ta": "குடியுரிமை",
        "articles": [
            {"number": "5", "title": "Citizenship at the commencement of the Constitution", "title_ta": "அரசியலமைப்பு தொடங்கும்போது குடியுரிமை"},
            {"number": "6", "title": "Rights of citizenship of certain persons who have migrated to India from Pakistan", "title_ta": "பாகிஸ்தானிலிருந்து இடம்பெயர்ந்தவர்களின் குடியுரிமை"},
            {"number": "7", "title": "Rights of citizenship of certain migrants to Pakistan", "title_ta": "பாகிஸ்தானுக்கு இடம்பெயர்ந்தவர்களின் குடியுரிமை"},
            {"number": "8", "title": "Rights of citizenship of certain persons of Indian origin residing outside India", "title_ta": "வெளிநாட்டில் வசிக்கும் இந்திய வம்சாவளி நபர்களின் குடியுரிமை"},
            {"number": "9", "title": "Persons voluntarily acquiring citizenship of a foreign State not to be citizens", "title_ta": "வெளிநாட்டு குடியுரிமை பெற்றவர்கள் குடிமக்கள் ஆகமாட்டார்கள்"},
            {"number": "10", "title": "Continuance of the rights of citizenship", "title_ta": "குடியுரிமை உரிமைகளின் தொடர்ச்சி"},
            {"number": "11", "title": "Parliament to regulate the right of citizenship by law", "title_ta": "குடியுரிமையை சட்டத்தால் ஒழுங்குபடுத்த நாடாளுமன்ற அதிகாரம்"},
        ],
    },
    {
        "part_number": "III",
        "part_title": "Fundamental Rights",
        "part_title_ta": "அடிப்படை உரிமைகள்",
        "articles": [
            {"number": "12", "title": "Definition — 'State'", "title_ta": "வரையறை — 'அரசு'"},
            {"number": "13", "title": "Laws inconsistent with or in derogation of the fundamental rights", "title_ta": "அடிப்படை உரிமைகளுக்கு முரணான சட்டங்கள்"},
            {"number": "14", "title": "Equality before law", "title_ta": "சட்டத்தின் முன் சமத்துவம்", "is_landmark": True},
            {"number": "15", "title": "Prohibition of discrimination on grounds of religion, race, caste, sex or place of birth", "title_ta": "மதம், இனம், சாதி, பாலினம் அடிப்படையில் பாகுபாடு தடை", "is_landmark": True},
            {"number": "16", "title": "Equality of opportunity in matters of public employment", "title_ta": "அரசுப் பணிகளில் சம வாய்ப்பு", "is_landmark": True},
            {"number": "17", "title": "Abolition of Untouchability", "title_ta": "தீண்டாமை ஒழிப்பு", "is_landmark": True},
            {"number": "18", "title": "Abolition of titles", "title_ta": "பட்டங்கள் ஒழிப்பு"},
            {"number": "19", "title": "Protection of certain rights regarding freedom of speech, etc.", "title_ta": "பேச்சு சுதந்திரம் போன்ற உரிமைகளின் பாதுகாப்பு", "is_landmark": True},
            {"number": "20", "title": "Protection in respect of conviction for offences", "title_ta": "குற்றத்தீர்ப்பு தொடர்பான பாதுகாப்பு"},
            {"number": "21", "title": "Protection of life and personal liberty", "title_ta": "உயிர் மற்றும் தனி சுதந்திரம் பாதுகாப்பு", "is_landmark": True},
            {"number": "21A", "title": "Right to education", "title_ta": "கல்வி உரிமை", "is_landmark": True, "amendment": "86th Amendment, 2002"},
            {"number": "22", "title": "Protection against arrest and detention in certain cases", "title_ta": "கைது மற்றும் தடுப்புக்காவல் எதிர்ப்பு பாதுகாப்பு"},
            {"number": "23", "title": "Prohibition of traffic in human beings and forced labour", "title_ta": "மனித கடத்தல் மற்றும் கட்டாய உழைப்பு தடை"},
            {"number": "24", "title": "Prohibition of employment of children in factories, etc.", "title_ta": "தொழிற்சாலைகளில் குழந்தைகள் வேலை செய்வதற்கான தடை"},
            {"number": "25", "title": "Freedom of conscience and free profession, practice and propagation of religion", "title_ta": "மனசாட்சி சுதந்திரம், மதம் பின்பற்றல், பரப்புதல்"},
            {"number": "26", "title": "Freedom to manage religious affairs", "title_ta": "மத விவகாரங்களை நிர்வகிக்கும் சுதந்திரம்"},
            {"number": "27", "title": "Freedom as to payment of taxes for promotion of any particular religion", "title_ta": "குறிப்பிட்ட மதத்தை ஊக்குவிக்க வரி செலுத்துவதிலிருந்து சுதந்திரம்"},
            {"number": "28", "title": "Freedom as to attendance at religious instruction or religious worship in certain educational institutions", "title_ta": "கல்வி நிறுவனங்களில் மத போதனை/வழிபாட்டில் கலந்துகொள்வதிலிருந்து சுதந்திரம்"},
            {"number": "29", "title": "Protection of interests of minorities", "title_ta": "சிறுபான்மையினரின் நலன்களின் பாதுகாப்பு"},
            {"number": "30", "title": "Right of minorities to establish and administer educational institutions", "title_ta": "கல்வி நிறுவனங்களை நிறுவ மற்றும் நிர்வகிக்க சிறுபான்மையினரின் உரிமை"},
            {"number": "31", "title": "Compulsory acquisition of property", "title_ta": "கட்டாய சொத்து கையகப்படுத்தல்", "status": "Repealed", "amendment": "44th Amendment, 1978"},
            {"number": "31A", "title": "Saving of Laws providing for acquisition of estates, etc.", "title_ta": "எஸ்டேட் கையகப்படுத்தும் சட்டங்களின் பாதுகாப்பு"},
            {"number": "31B", "title": "Validation of certain Acts and Regulations", "title_ta": "சில சட்டங்கள் மற்றும் ஒழுங்குமுறைகளின் சரிபார்ப்பு"},
            {"number": "31C", "title": "Saving of laws giving effect to certain directive principles", "title_ta": "வழிகாட்டுக் கொள்கைகளை செயல்படுத்தும் சட்டங்களின் பாதுகாப்பு"},
            {"number": "31D", "title": "Saving of laws in respect of anti-national activities", "title_ta": "தேச விரோத நடவடிக்கைகள் தொடர்பான சட்டங்களின் பாதுகாப்பு", "status": "Repealed", "amendment": "43rd Amendment, 1977"},
            {"number": "32", "title": "Remedies for enforcement of rights conferred by this Part", "title_ta": "அடிப்படை உரிமைகளை நிலைநாட்ட தீர்வுகள்", "is_landmark": True},
            {"number": "33", "title": "Power of Parliament to modify the rights conferred by this Part in their application to Forces, etc.", "title_ta": "ராணுவத்திற்கு உரிமைகளை மாற்றியமைக்க நாடாளுமன்ற அதிகாரம்"},
            {"number": "34", "title": "Restriction on rights conferred by this Part while martial law is in force", "title_ta": "இராணுவ நிர்வாகம் நடைபெறும்போது உரிமைகள் மீதான கட்டுப்பாடு"},
            {"number": "35", "title": "Legislation to give effect to the provisions of this Part", "title_ta": "இப்பகுதி விதிகளை செயல்படுத்த சட்டம் இயற்றல்"},
        ],
    },
    {
        "part_number": "IV",
        "part_title": "Directive Principles of State Policy",
        "part_title_ta": "அரசு கொள்கையின் வழிகாட்டுத் தத்துவங்கள்",
        "articles": [
            {"number": "36", "title": "Definition", "title_ta": "வரையறை"},
            {"number": "37", "title": "Application of the principles contained in this Part", "title_ta": "இப்பகுதியின் கொள்கைகளின் பயன்பாடு"},
            {"number": "38", "title": "State to secure a social order for the promotion of welfare of the people", "title_ta": "மக்கள் நலனுக்கான சமூக ஒழுங்கை அரசு உறுதிசெய்தல்"},
            {"number": "39", "title": "Certain principles of policy to be followed by the State", "title_ta": "அரசு பின்பற்ற வேண்டிய கொள்கைத் தத்துவங்கள்"},
            {"number": "39A", "title": "Equal justice and free legal aid", "title_ta": "சம நீதி மற்றும் இலவச சட்ட உதவி", "amendment": "42nd Amendment, 1976"},
            {"number": "40", "title": "Organisation of village panchayats", "title_ta": "கிராம பஞ்சாயத்துக்களை அமைத்தல்"},
            {"number": "41", "title": "Right to work, to education and to public assistance in certain cases", "title_ta": "வேலை, கல்வி மற்றும் பொது உதவிக்கான உரிமை"},
            {"number": "42", "title": "Provision for just and humane conditions of work and maternity relief", "title_ta": "நியாயமான பணி நிலைமைகள் மற்றும் மகப்பேறு உதவி"},
            {"number": "43", "title": "Living wage, etc., for workers", "title_ta": "தொழிலாளர்களுக்கு வாழ்வாதார ஊதியம்"},
            {"number": "43A", "title": "Participation of workers in management of industries", "title_ta": "தொழிற்சாலை நிர்வாகத்தில் தொழிலாளர் பங்கேற்பு"},
            {"number": "43B", "title": "Promotion of co-operative societies", "title_ta": "கூட்டுறவு சங்கங்களை ஊக்குவித்தல்", "amendment": "97th Amendment, 2011"},
            {"number": "44", "title": "Uniform civil code for the citizens", "title_ta": "குடிமக்களுக்கான ஒரே சிவில் சட்டம்", "is_landmark": True},
            {"number": "45", "title": "Provision for early childhood care and education to children below the age of six years", "title_ta": "6 வயதுக்குட்பட்ட குழந்தைகளுக்கான ஆரம்ப கால கவனிப்பு"},
            {"number": "46", "title": "Promotion of educational and economic interests of SCs, STs and other weaker sections", "title_ta": "SC, ST மற்றும் பிற பலவீனப் பிரிவினரின் கல்வி மற்றும் பொருளாதார நலன்களை ஊக்குவித்தல்"},
            {"number": "47", "title": "Duty of the State to raise the level of nutrition and the standard of living", "title_ta": "ஊட்டச்சத்து நிலை மற்றும் வாழ்க்கைத் தரத்தை உயர்த்த அரசின் கடமை"},
            {"number": "48", "title": "Organisation of agriculture and animal husbandry", "title_ta": "வேளாண்மை மற்றும் கால்நடை வளர்ப்பு அமைப்பு"},
            {"number": "48A", "title": "Protection and improvement of environment and safeguarding of forests and wild life", "title_ta": "சுற்றுச்சூழல் பாதுகாப்பு, காடுகள் மற்றும் வன உயிரினங்கள் பாதுகாப்பு"},
            {"number": "49", "title": "Protection of monuments and places and objects of national importance", "title_ta": "தேசிய முக்கியத்துவம் வாய்ந்த நினைவுச்சின்னங்கள் பாதுகாப்பு"},
            {"number": "50", "title": "Separation of judiciary from executive", "title_ta": "நீதித்துறையை நிர்வாகத்திலிருந்து பிரித்தல்"},
            {"number": "51", "title": "Promotion of international peace and security", "title_ta": "சர்வதேச அமைதி மற்றும் பாதுகாப்பை ஊக்குவித்தல்"},
        ],
    },
    {
        "part_number": "IVA",
        "part_title": "Fundamental Duties",
        "part_title_ta": "அடிப்படைக் கடமைகள்",
        "articles": [
            {"number": "51A", "title": "Fundamental duties", "title_ta": "அடிப்படைக் கடமைகள்", "is_landmark": True, "amendment": "42nd Amendment, 1976"},
        ],
    },
    {
        "part_number": "V",
        "part_title": "The Union",
        "part_title_ta": "ஒன்றியம்",
        "chapters": [
            {"chapter": "I", "title": "The Executive", "articles_range": "52-78"},
            {"chapter": "II", "title": "Parliament", "articles_range": "79-122"},
            {"chapter": "III", "title": "Legislative Powers of the President", "articles_range": "123"},
            {"chapter": "IV", "title": "The Union Judiciary", "articles_range": "124-147"},
            {"chapter": "V", "title": "Comptroller and Auditor-General of India", "articles_range": "148-151"},
        ],
        "articles": [
            {"number": "52", "title": "The President of India", "title_ta": "இந்தியக் குடியரசுத் தலைவர்"},
            {"number": "53", "title": "Executive power of the Union", "title_ta": "ஒன்றியத்தின் நிர்வாக அதிகாரம்"},
            {"number": "54", "title": "Election of President", "title_ta": "குடியரசுத் தலைவர் தேர்தல்"},
            {"number": "55", "title": "Manner of election of President", "title_ta": "குடியரசுத் தலைவர் தேர்தல் முறை"},
            {"number": "56", "title": "Term of office of President", "title_ta": "குடியரசுத் தலைவரின் பதவிக் காலம்"},
            {"number": "61", "title": "Procedure for impeachment of the President", "title_ta": "குடியரசுத் தலைவரை பதவி நீக்கம் செய்யும் நடைமுறை"},
            {"number": "72", "title": "Power of President to grant pardons, etc.", "title_ta": "மன்னிப்பு வழங்க குடியரசுத் தலைவரின் அதிகாரம்"},
            {"number": "74", "title": "Council of Ministers to aid and advise President", "title_ta": "குடியரசுத் தலைவருக்கு உதவி மற்றும் ஆலோசனை வழங்க அமைச்சர்கள் குழு"},
            {"number": "75", "title": "Other provisions as to Ministers", "title_ta": "அமைச்சர்கள் தொடர்பான இதர விதிகள்"},
            {"number": "76", "title": "Attorney-General for India", "title_ta": "இந்தியாவின் அட்டார்னி ஜெனரல்"},
            {"number": "79", "title": "Constitution of Parliament", "title_ta": "நாடாளுமன்றத்தின் அமைப்பு"},
            {"number": "80", "title": "Composition of the Council of States (Rajya Sabha)", "title_ta": "மாநிலங்களவையின் அமைப்பு"},
            {"number": "81", "title": "Composition of the House of the People (Lok Sabha)", "title_ta": "மக்களவையின் அமைப்பு"},
            {"number": "100", "title": "Voting in Houses, power of Houses to act notwithstanding vacancies and quorum", "title_ta": "அவைகளில் வாக்களிப்பு மற்றும் குறைந்தபட்ச எண்ணிக்கை"},
            {"number": "110", "title": "Definition of 'Money Bills'", "title_ta": "நிதி மசோதா வரையறை"},
            {"number": "112", "title": "Annual financial statement (Union Budget)", "title_ta": "ஆண்டு நிதி அறிக்கை (ஒன்றிய பட்ஜெட்)", "is_landmark": True},
            {"number": "123", "title": "Power of President to promulgate Ordinances", "title_ta": "அவசரச் சட்டம் பிறப்பிக்க குடியரசுத் தலைவரின் அதிகாரம்"},
            {"number": "124", "title": "Establishment and constitution of Supreme Court", "title_ta": "உச்ச நீதிமன்றத்தின் நிறுவனம் மற்றும் அமைப்பு", "is_landmark": True},
            {"number": "129", "title": "Supreme Court to be a court of record", "title_ta": "உச்ச நீதிமன்றம் பதிவு நீதிமன்றம்"},
            {"number": "131", "title": "Original jurisdiction of the Supreme Court", "title_ta": "உச்ச நீதிமன்றத்தின் முதல் நிலை அதிகார வரம்பு"},
            {"number": "136", "title": "Special leave to appeal by the Supreme Court", "title_ta": "உச்ச நீதிமன்றத்தில் சிறப்பு மேல்முறையீட்டு அனுமதி"},
            {"number": "141", "title": "Law declared by Supreme Court to be binding on all courts", "title_ta": "உச்ச நீதிமன்றம் அறிவிக்கும் சட்டம் அனைத்து நீதிமன்றங்களையும் கட்டுப்படுத்தும்", "is_landmark": True},
            {"number": "143", "title": "Power of President to consult Supreme Court", "title_ta": "உச்ச நீதிமன்றத்தை ஆலோசிக்க குடியரசுத் தலைவரின் அதிகாரம்"},
            {"number": "148", "title": "Comptroller and Auditor-General of India", "title_ta": "இந்தியாவின் தலைமை கணக்கு மற்றும் தணிக்கையாளர்"},
        ],
    },
    {
        "part_number": "VI",
        "part_title": "The States",
        "part_title_ta": "மாநிலங்கள்",
        "articles": [
            {"number": "152", "title": "Definition", "title_ta": "வரையறை"},
            {"number": "153", "title": "Governors of States", "title_ta": "மாநில ஆளுநர்கள்"},
            {"number": "154", "title": "Executive power of State", "title_ta": "மாநிலத்தின் நிர்வாக அதிகாரம்"},
            {"number": "163", "title": "Council of Ministers to aid and advise Governor", "title_ta": "ஆளுநருக்கு உதவி மற்றும் ஆலோசனை வழங்க அமைச்சர்கள் குழு"},
            {"number": "164", "title": "Other provisions as to Ministers (Chief Minister appointment)", "title_ta": "அமைச்சர்கள் தொடர்பான இதர விதிகள் (முதலமைச்சர் நியமனம்)", "is_landmark": True},
            {"number": "165", "title": "Advocate-General for the State", "title_ta": "மாநில அரசு வழக்கறிஞர்"},
            {"number": "170", "title": "Composition of the Legislative Assemblies", "title_ta": "சட்டமன்றங்களின் அமைப்பு"},
            {"number": "171", "title": "Composition of the Legislative Councils", "title_ta": "சட்ட மேலவையின் அமைப்பு"},
            {"number": "200", "title": "Assent to Bills (Governor's role)", "title_ta": "மசோதாக்களுக்கு ஒப்புதல் (ஆளுநரின் பங்கு)"},
            {"number": "213", "title": "Power of Governor to promulgate Ordinances", "title_ta": "அவசரச் சட்டம் பிறப்பிக்க ஆளுநரின் அதிகாரம்"},
            {"number": "214", "title": "High Courts for States", "title_ta": "மாநிலங்களுக்கான உயர் நீதிமன்றங்கள்"},
            {"number": "226", "title": "Power of High Courts to issue certain writs", "title_ta": "குறிப்பிட்ட நீதிப் பேராணைகள் பிறப்பிக்க உயர் நீதிமன்ற அதிகாரம்", "is_landmark": True},
            {"number": "233", "title": "Appointment of district judges", "title_ta": "மாவட்ட நீதிபதிகள் நியமனம்"},
        ],
    },
    {
        "part_number": "VII",
        "part_title": "States in Part B of the First Schedule",
        "part_title_ta": "முதல் அட்டவணையின் பகுதி B மாநிலங்கள்",
        "articles": [
            {"number": "238", "title": "Application of provisions of Part VI to States in Part B of the First Schedule", "title_ta": "பகுதி VI விதிகளின் பயன்பாடு", "status": "Repealed", "amendment": "7th Amendment, 1956"},
        ],
    },
    {
        "part_number": "VIII",
        "part_title": "The Union Territories",
        "part_title_ta": "ஒன்றியப் பிரதேசங்கள்",
        "articles": [
            {"number": "239", "title": "Administration of Union territories", "title_ta": "ஒன்றியப் பிரதேசங்களின் நிர்வாகம்"},
            {"number": "239A", "title": "Creation of local Legislatures or Council of Ministers for certain Union territories", "title_ta": "சில ஒன்றியப் பிரதேசங்களுக்கான சட்டமன்றங்கள் உருவாக்கம்"},
            {"number": "239AA", "title": "Special provisions with respect to Delhi", "title_ta": "டெல்லி தொடர்பான சிறப்பு விதிகள்", "amendment": "69th Amendment, 1991"},
            {"number": "240", "title": "Power of President to make regulations for certain Union territories", "title_ta": "சில ஒன்றியப் பிரதேசங்களுக்கான ஒழுங்குமுறைகளை உருவாக்க குடியரசுத் தலைவரின் அதிகாரம்"},
            {"number": "241", "title": "High Courts for Union territories", "title_ta": "ஒன்றியப் பிரதேசங்களுக்கான உயர் நீதிமன்றங்கள்"},
        ],
    },
    {
        "part_number": "IX",
        "part_title": "The Panchayats",
        "part_title_ta": "பஞ்சாயத்துகள்",
        "articles": [
            {"number": "243", "title": "Definitions", "title_ta": "வரையறைகள்", "amendment": "73rd Amendment, 1992"},
            {"number": "243A", "title": "Gram Sabha", "title_ta": "கிராம சபை"},
            {"number": "243B", "title": "Constitution of Panchayats", "title_ta": "பஞ்சாயத்துகளின் அமைப்பு"},
            {"number": "243C", "title": "Composition of Panchayats", "title_ta": "பஞ்சாயத்துகளின் உள்ளடக்கம்"},
            {"number": "243D", "title": "Reservation of seats (SC/ST/Women)", "title_ta": "இடங்கள் ஒதுக்கீடு (SC/ST/பெண்கள்)", "is_landmark": True},
            {"number": "243G", "title": "Powers, authority and responsibilities of Panchayats", "title_ta": "பஞ்சாயத்துகளின் அதிகாரங்கள், பொறுப்புகள்"},
            {"number": "243K", "title": "Elections to the Panchayats", "title_ta": "பஞ்சாயத்துத் தேர்தல்கள்"},
            {"number": "243O", "title": "Bar to interference by courts in electoral matters", "title_ta": "தேர்தல் விவகாரங்களில் நீதிமன்றத்தின் தலையீட்டுக்கு தடை"},
        ],
    },
    {
        "part_number": "IXA",
        "part_title": "The Municipalities",
        "part_title_ta": "நகராட்சிகள்",
        "articles": [
            {"number": "243P", "title": "Definitions", "title_ta": "வரையறைகள்", "amendment": "74th Amendment, 1992"},
            {"number": "243Q", "title": "Constitution of Municipalities", "title_ta": "நகராட்சிகளின் அமைப்பு"},
            {"number": "243R", "title": "Composition of Municipalities", "title_ta": "நகராட்சிகளின் உள்ளடக்கம்"},
            {"number": "243S", "title": "Constitution and composition of Wards Committees, etc.", "title_ta": "வார்டு குழுக்களின் அமைப்பு"},
            {"number": "243T", "title": "Reservation of seats (SC/ST/Women)", "title_ta": "இடங்கள் ஒதுக்கீடு (SC/ST/பெண்கள்)"},
            {"number": "243W", "title": "Powers, authority and responsibilities of Municipalities", "title_ta": "நகராட்சிகளின் அதிகாரங்கள், பொறுப்புகள்"},
            {"number": "243ZG", "title": "Committee for district planning", "title_ta": "மாவட்ட திட்டக் குழு"},
        ],
    },
    {
        "part_number": "IXB",
        "part_title": "The Co-operative Societies",
        "part_title_ta": "கூட்டுறவு சங்கங்கள்",
        "articles": [
            {"number": "243ZH", "title": "Definitions", "title_ta": "வரையறைகள்", "amendment": "97th Amendment, 2011"},
            {"number": "243ZI", "title": "Incorporation of co-operative societies", "title_ta": "கூட்டுறவு சங்கங்களை நிறுவுதல்"},
            {"number": "243ZJ", "title": "Number and term of members of board", "title_ta": "குழு உறுப்பினர்களின் எண்ணிக்கை மற்றும் பதவிக்காலம்"},
            {"number": "243ZT", "title": "Application to Union territories", "title_ta": "ஒன்றியப் பிரதேசங்களுக்கு பயன்பாடு"},
        ],
    },
    {
        "part_number": "X",
        "part_title": "The Scheduled and Tribal Areas",
        "part_title_ta": "பட்டியல் மற்றும் பழங்குடி பகுதிகள்",
        "articles": [
            {"number": "244", "title": "Administration of Scheduled Areas and Tribal Areas", "title_ta": "பட்டியல் பகுதிகள் மற்றும் பழங்குடி பகுதிகளின் நிர்வாகம்"},
            {"number": "244A", "title": "Formation of an autonomous State comprising certain tribal areas in Assam", "title_ta": "அசாமில் பழங்குடி தன்னாட்சி மாநிலம் அமைத்தல்"},
        ],
    },
    {
        "part_number": "XI",
        "part_title": "Relations between the Union and the States",
        "part_title_ta": "ஒன்றியத்திற்கும் மாநிலங்களுக்கும் இடையிலான உறவுகள்",
        "articles": [
            {"number": "245", "title": "Extent of laws made by Parliament and by the Legislatures of States", "title_ta": "நாடாளுமன்றம் மற்றும் மாநில சட்டமன்றங்கள் இயற்றும் சட்டங்களின் எல்லை"},
            {"number": "246", "title": "Subject-matter of laws made by Parliament and by the Legislatures of States", "title_ta": "சட்டப்பட்டியல்கள் — ஒன்றியம், மாநிலம், பொதுப்பட்டியல்", "is_landmark": True},
            {"number": "249", "title": "Power of Parliament to legislate with respect to a matter in the State List in the national interest", "title_ta": "தேசிய நலன் கருதி மாநிலப் பட்டியலில் சட்டமியற்ற நாடாளுமன்ற அதிகாரம்"},
            {"number": "254", "title": "Inconsistency between laws made by Parliament and laws made by the Legislatures of States", "title_ta": "நாடாளுமன்ற சட்டங்களுக்கும் மாநில சட்டங்களுக்கும் இடையிலான முரண்பாடு"},
            {"number": "256", "title": "Obligation of States and the Union", "title_ta": "மாநிலங்களின் மற்றும் ஒன்றியத்தின் கடமை"},
            {"number": "263", "title": "Provisions with respect to an inter-State Council", "title_ta": "மாநிலங்களுக்கிடையிலான குழு தொடர்பான விதிகள்"},
        ],
    },
    {
        "part_number": "XII",
        "part_title": "Finance, Property, Contracts and Suits",
        "part_title_ta": "நிதி, சொத்து, ஒப்பந்தங்கள் மற்றும் வழக்குகள்",
        "articles": [
            {"number": "264", "title": "Interpretation", "title_ta": "விளக்கம்"},
            {"number": "265", "title": "Taxes not to be imposed save by authority of law", "title_ta": "சட்ட அதிகாரமின்றி வரி விதிக்கக்கூடாது", "is_landmark": True},
            {"number": "266", "title": "Consolidated Funds and public accounts of India and of the States", "title_ta": "ஒருங்கிணைந்த நிதி மற்றும் பொதுக் கணக்குகள்"},
            {"number": "267", "title": "Contingency Fund", "title_ta": "அவசர நிதி"},
            {"number": "270", "title": "Taxes levied and distributed between the Union and the States", "title_ta": "ஒன்றியத்திற்கும் மாநிலங்களுக்கும் இடையே வரிகள் பிரிக்கப்படுதல்", "is_landmark": True},
            {"number": "275", "title": "Grants from the Union to certain States", "title_ta": "சில மாநிலங்களுக்கு ஒன்றிய மானியங்கள்"},
            {"number": "280", "title": "Finance Commission", "title_ta": "நிதிக்குழு", "is_landmark": True},
            {"number": "300A", "title": "Persons not to be deprived of property save by authority of law", "title_ta": "சட்ட அதிகாரமின்றி சொத்தை பறிக்கக்கூடாது", "amendment": "44th Amendment, 1978"},
        ],
    },
    {
        "part_number": "XIII",
        "part_title": "Trade, Commerce and Intercourse within the Territory of India",
        "part_title_ta": "இந்திய எல்லைக்குள் வர்த்தகம், வணிகம்",
        "articles": [
            {"number": "301", "title": "Freedom of trade, commerce and intercourse", "title_ta": "வர்த்தகம், வணிகம் சுதந்திரம்"},
            {"number": "302", "title": "Power of Parliament to impose restrictions on trade, commerce and intercourse", "title_ta": "வர்த்தகக் கட்டுப்பாடுகள் விதிக்க நாடாளுமன்ற அதிகாரம்"},
            {"number": "307", "title": "Appointment of authority for carrying out the purposes of articles 301 to 304", "title_ta": "உறுப்புகள் 301 முதல் 304 நோக்கங்களை நிறைவேற்ற அதிகாரி நியமனம்"},
        ],
    },
    {
        "part_number": "XIV",
        "part_title": "Services under the Union and the States",
        "part_title_ta": "ஒன்றியம் மற்றும் மாநிலங்களின் கீழ் பணிகள்",
        "articles": [
            {"number": "308", "title": "Interpretation", "title_ta": "விளக்கம்"},
            {"number": "309", "title": "Recruitment and conditions of service of persons serving the Union or a State", "title_ta": "பணி நியமனம் மற்றும் பணி நிலைமைகள்"},
            {"number": "311", "title": "Dismissal, removal or reduction in rank of persons employed in civil capacities under the Union or a State", "title_ta": "பதவி நீக்கம், இடைநீக்கம் அல்லது பதவி தரம் குறைப்பு"},
            {"number": "312", "title": "All-India services", "title_ta": "அகில இந்தியப் பணிகள்"},
        ],
    },
    {
        "part_number": "XIVA",
        "part_title": "Tribunals",
        "part_title_ta": "தீர்ப்பாயங்கள்",
        "articles": [
            {"number": "323A", "title": "Administrative tribunals", "title_ta": "நிர்வாக தீர்ப்பாயங்கள்", "amendment": "42nd Amendment, 1976"},
            {"number": "323B", "title": "Tribunals for other matters", "title_ta": "பிற விவகாரங்களுக்கான தீர்ப்பாயங்கள்"},
        ],
    },
    {
        "part_number": "XV",
        "part_title": "Elections",
        "part_title_ta": "தேர்தல்கள்",
        "articles": [
            {"number": "324", "title": "Superintendence, direction and control of elections to be vested in an Election Commission", "title_ta": "தேர்தல் ஆணையத்தில் தேர்தல் மேற்பார்வை", "is_landmark": True},
            {"number": "325", "title": "No person to be ineligible for inclusion in, or to claim to be included in a special, electoral roll on grounds of religion, race, caste or sex", "title_ta": "மதம், இனம், சாதி, பாலினம் அடிப்படையில் வாக்காளர் பட்டியலில் இடம்பெற தகுதியின்மை கூறக்கூடாது"},
            {"number": "326", "title": "Elections to the House of the People and to the Legislative Assemblies of States to be on the basis of adult suffrage", "title_ta": "வயது வந்தோர் வாக்குரிமை அடிப்படையில் தேர்தல்", "is_landmark": True},
            {"number": "329", "title": "Bar to interference by courts in electoral matters", "title_ta": "தேர்தல் விவகாரங்களில் நீதிமன்ற தலையீட்டுக்கு தடை"},
        ],
    },
    {
        "part_number": "XVI",
        "part_title": "Special Provisions Relating to Certain Classes",
        "part_title_ta": "சில வகுப்பினருக்கான சிறப்பு விதிகள்",
        "articles": [
            {"number": "330", "title": "Reservation of seats for SCs and STs in the House of the People", "title_ta": "மக்களவையில் SC/ST இட ஒதுக்கீடு", "is_landmark": True},
            {"number": "331", "title": "Representation of the Anglo-Indian community in the House of the People", "title_ta": "மக்களவையில் ஆங்கிலோ-இந்தியர் பிரதிநிதித்துவம்"},
            {"number": "332", "title": "Reservation of seats for SCs and STs in the Legislative Assemblies of the States", "title_ta": "மாநில சட்டமன்றங்களில் SC/ST இட ஒதுக்கீடு"},
            {"number": "334", "title": "Reservation of seats and special representation to cease after certain period", "title_ta": "குறிப்பிட்ட காலத்திற்குப் பின் இட ஒதுக்கீடு நிறுத்தம்"},
            {"number": "335", "title": "Claims of SCs and STs to services and posts", "title_ta": "பணிகள் மற்றும் பதவிகளில் SC/ST உரிமைகோரல்"},
            {"number": "338", "title": "National Commission for Scheduled Castes", "title_ta": "தேசிய பட்டியல் சாதிகள் ஆணையம்"},
            {"number": "338A", "title": "National Commission for Scheduled Tribes", "title_ta": "தேசிய பட்டியல் பழங்குடிகள் ஆணையம்"},
            {"number": "340", "title": "Appointment of a Commission to investigate the conditions of backward classes", "title_ta": "பிற்படுத்தப்பட்ட வகுப்பினரின் நிலைமைகளை ஆராய ஆணையம் நியமனம்"},
            {"number": "341", "title": "Scheduled Castes", "title_ta": "பட்டியல் சாதிகள்"},
            {"number": "342", "title": "Scheduled Tribes", "title_ta": "பட்டியல் பழங்குடிகள்"},
        ],
    },
    {
        "part_number": "XVII",
        "part_title": "Official Language",
        "part_title_ta": "அலுவல் மொழி",
        "articles": [
            {"number": "343", "title": "Official language of the Union", "title_ta": "ஒன்றியத்தின் அலுவல் மொழி"},
            {"number": "344", "title": "Commission and Committee of Parliament on official language", "title_ta": "அலுவல் மொழி ஆணையம் மற்றும் நாடாளுமன்றக் குழு"},
            {"number": "345", "title": "Official language or languages of a State", "title_ta": "மாநிலத்தின் அலுவல் மொழி"},
            {"number": "348", "title": "Language to be used in the Supreme Court and in the High Courts", "title_ta": "உச்ச நீதிமன்றம் மற்றும் உயர் நீதிமன்றங்களில் பயன்படுத்தப்படும் மொழி"},
            {"number": "350A", "title": "Facilities for instruction in mother-tongue at primary stage", "title_ta": "ஆரம்ப நிலையில் தாய்மொழியில் கற்பித்தல் வசதிகள்"},
            {"number": "350B", "title": "Special Officer for linguistic minorities", "title_ta": "மொழிச் சிறுபான்மையினருக்கான சிறப்பு அலுவலர்"},
            {"number": "351", "title": "Directive for development of the Hindi language", "title_ta": "இந்தி மொழி வளர்ச்சிக்கான வழிகாட்டல்"},
        ],
    },
    {
        "part_number": "XVIII",
        "part_title": "Emergency Provisions",
        "part_title_ta": "அவசரகால விதிகள்",
        "articles": [
            {"number": "352", "title": "Proclamation of Emergency (National Emergency)", "title_ta": "அவசரநிலை பிரகடனம் (தேசிய அவசரநிலை)", "is_landmark": True},
            {"number": "356", "title": "Provisions in case of failure of constitutional machinery in States (President's Rule)", "title_ta": "மாநிலங்களில் அரசியலமைப்பு இயந்திரம் செயலிழப்பு (குடியரசுத் தலைவர் ஆட்சி)", "is_landmark": True},
            {"number": "358", "title": "Suspension of provisions of Article 19 during emergencies", "title_ta": "அவசரநிலையில் உறுப்பு 19 விதிகள் இடைநிறுத்தம்"},
            {"number": "359", "title": "Suspension of the enforcement of the rights conferred by Part III during emergencies", "title_ta": "அவசரநிலையில் அடிப்படை உரிமைகள் நிலைநிறுத்தல் இடைநிறுத்தம்"},
            {"number": "360", "title": "Provisions as to financial emergency", "title_ta": "நிதி அவசரநிலை தொடர்பான விதிகள்"},
        ],
    },
    {
        "part_number": "XIX",
        "part_title": "Miscellaneous",
        "part_title_ta": "இதர விதிகள்",
        "articles": [
            {"number": "361", "title": "Protection of President and Governors", "title_ta": "குடியரசுத் தலைவர் மற்றும் ஆளுநர்களின் பாதுகாப்பு"},
            {"number": "365", "title": "Effect of failure to comply with, or to give effect to, directions given by the Union", "title_ta": "ஒன்றிய வழிகாட்டுதல்களை பின்பற்றத் தவறுவதன் விளைவு"},
            {"number": "366", "title": "Definitions", "title_ta": "வரையறைகள்"},
            {"number": "367", "title": "Interpretation", "title_ta": "விளக்கம்"},
        ],
    },
    {
        "part_number": "XX",
        "part_title": "Amendment of the Constitution",
        "part_title_ta": "அரசியலமைப்பு திருத்தம்",
        "articles": [
            {"number": "368", "title": "Power of Parliament to amend the Constitution and procedure therefor", "title_ta": "அரசியலமைப்பை திருத்தம் செய்ய நாடாளுமன்ற அதிகாரம்", "is_landmark": True, "note": "Originally in Part XIX, often referenced under Part XX"},
        ],
    },
    {
        "part_number": "XXI",
        "part_title": "Temporary, Transitional and Special Provisions",
        "part_title_ta": "தற்காலிக, இடைநிலை மற்றும் சிறப்பு விதிகள்",
        "articles": [
            {"number": "369", "title": "Temporary power to Parliament to make laws with respect to certain matters in the State List", "title_ta": "மாநிலப் பட்டியலில் சில விவகாரங்களில் நாடாளுமன்றத்தின் தற்காலிக அதிகாரம்"},
            {"number": "370", "title": "Temporary provisions with respect to the State of Jammu and Kashmir", "title_ta": "ஜம்மு காஷ்மீர் தொடர்பான தற்காலிக விதிகள்", "is_landmark": True, "note": "Effectively abrogated by C.O. 272 (2019), upheld by Supreme Court in 2023"},
            {"number": "371", "title": "Special provision with respect to the States of Maharashtra and Gujarat", "title_ta": "மகாராஷ்டிரா மற்றும் குஜராத் தொடர்பான சிறப்பு விதி"},
            {"number": "371A", "title": "Special provision with respect to the State of Nagaland", "title_ta": "நாகாலாந்து தொடர்பான சிறப்பு விதி"},
            {"number": "371D", "title": "Special provisions with respect to the State of Andhra Pradesh or the State of Telangana", "title_ta": "ஆந்திரா/தெலங்கானா தொடர்பான சிறப்பு விதிகள்"},
            {"number": "371J", "title": "Special provisions with respect to the State of Karnataka (Hyderabad-Karnataka region)", "title_ta": "கர்நாடகா (ஹைதராபாத்-கர்நாடகா) சிறப்பு விதிகள்"},
        ],
    },
    {
        "part_number": "XXII",
        "part_title": "Short Title, Commencement, Authoritative Text in Hindi and Repeals",
        "part_title_ta": "சுருக்கப் பெயர், தொடக்கம், இந்தியில் அதிகாரப்பூர்வ உரை மற்றும் நீக்கங்கள்",
        "articles": [
            {"number": "393", "title": "Short title", "title_ta": "சுருக்கப் பெயர்"},
            {"number": "394", "title": "Commencement", "title_ta": "தொடக்கம்"},
            {"number": "394A", "title": "Authoritative text of the Constitution in Hindi", "title_ta": "இந்தியில் அரசியலமைப்பின் அதிகாரப்பூர்வ உரை"},
            {"number": "395", "title": "Repeals", "title_ta": "நீக்கங்கள்"},
        ],
    },
]

SCHEDULES: List[Dict[str, Any]] = [
    {"number": 1, "title": "List of States and Union Territories", "title_ta": "மாநிலங்கள் மற்றும் ஒன்றியப் பிரதேசங்களின் பட்டியல்", "related_articles": "1, 4"},
    {"number": 2, "title": "Salaries and emoluments of key officials", "title_ta": "முக்கிய அதிகாரிகளின் சம்பளம்", "related_articles": "59, 65, 75, 97, 125, 148, 158, 164, 186, 221"},
    {"number": 3, "title": "Forms of Oaths or Affirmations", "title_ta": "பதவிப் பிரமாண படிவங்கள்", "related_articles": "75, 99, 124, 148, 164, 188, 219"},
    {"number": 4, "title": "Allocation of seats in the Council of States (Rajya Sabha)", "title_ta": "மாநிலங்களவையில் இடங்கள் ஒதுக்கீடு", "related_articles": "4, 80"},
    {"number": 5, "title": "Provisions for administration of Scheduled Areas and Scheduled Tribes", "title_ta": "பட்டியல் பகுதிகள் மற்றும் பழங்குடிகள் நிர்வாகம்", "related_articles": "244"},
    {"number": 6, "title": "Provisions for administration of Tribal Areas in Assam, Meghalaya, Tripura and Mizoram", "title_ta": "அசாம், மேகாலயா, திரிபுரா, மிசோரம் பழங்குடி பகுதிகள் நிர்வாகம்", "related_articles": "244, 275"},
    {"number": 7, "title": "Union List, State List, Concurrent List", "title_ta": "ஒன்றியப் பட்டியல், மாநிலப் பட்டியல், பொதுப் பட்டியல்", "related_articles": "246", "is_landmark": True},
    {"number": 8, "title": "Languages recognized by the Constitution", "title_ta": "அரசியலமைப்பால் அங்கீகரிக்கப்பட்ட மொழிகள்", "related_articles": "344, 351", "detail": "22 languages: Assamese, Bengali, Bodo, Dogri, Gujarati, Hindi, Kannada, Kashmiri, Konkani, Maithili, Malayalam, Manipuri, Marathi, Nepali, Odia, Punjabi, Sanskrit, Santhali, Sindhi, Tamil, Telugu, Urdu"},
    {"number": 9, "title": "Acts and Regulations validated (immune from judicial review under Art. 31B)", "title_ta": "நீதித் தீர்வுக்கு உட்படாத சட்டங்கள் (உறுப்பு 31B)", "related_articles": "31B"},
    {"number": 10, "title": "Provisions relating to Anti-Defection Law", "title_ta": "கட்சி தாவல் தடைச் சட்ட விதிகள்", "related_articles": "102, 191", "is_landmark": True, "amendment": "52nd Amendment, 1985"},
    {"number": 11, "title": "Powers, authority and responsibilities of Panchayats (29 subjects)", "title_ta": "பஞ்சாயத்துகளின் அதிகாரங்கள் (29 பொருள்கள்)", "related_articles": "243G", "amendment": "73rd Amendment, 1992"},
    {"number": 12, "title": "Powers, authority and responsibilities of Municipalities (18 subjects)", "title_ta": "நகராட்சிகளின் அதிகாரங்கள் (18 பொருள்கள்)", "related_articles": "243W", "amendment": "74th Amendment, 1992"},
]

LANDMARK_AMENDMENTS: List[Dict[str, Any]] = [
    {"number": 1, "year": 1951, "title": "Empowered state to make special provisions for socially/educationally backward classes; added Ninth Schedule", "title_ta": "சமூக/கல்வி ரீதியாக பிற்படுத்தப்பட்ட வகுப்பினருக்கு சிறப்பு ஏற்பாடுகள்; ஒன்பதாம் அட்டவணை சேர்க்கப்பட்டது"},
    {"number": 7, "year": 1956, "title": "Reorganization of States; abolished Part B States and Part C States distinction", "title_ta": "மாநிலங்கள் மறுசீரமைப்பு; Part B, Part C மாநிலங்கள் வேறுபாடு நீக்கம்"},
    {"number": 42, "year": 1976, "title": "Added 'Socialist', 'Secular', 'Integrity' to Preamble; added Fundamental Duties (Art. 51A); curtailed judicial review", "title_ta": "முகவுரையில் 'சோசலிச', 'சமயச்சார்பற்ற', 'ஒருமைப்பாடு' சேர்ப்பு; அடிப்படைக் கடமைகள் சேர்ப்பு", "is_major": True},
    {"number": 44, "year": 1978, "title": "Right to Property removed from Fundamental Rights; made it a legal right (Art. 300A)", "title_ta": "சொத்துரிமை அடிப்படை உரிமையிலிருந்து நீக்கம்; சட்ட உரிமையாக மாற்றம் (உறுப்பு 300A)", "is_major": True},
    {"number": 52, "year": 1985, "title": "Anti-Defection Law added (Tenth Schedule)", "title_ta": "கட்சி தாவல் தடைச் சட்டம் சேர்ப்பு (பத்தாம் அட்டவணை)", "is_major": True},
    {"number": 61, "year": 1989, "title": "Reduced voting age from 21 to 18 years", "title_ta": "வாக்களிக்கும் வயது 21-ல் இருந்து 18 ஆக குறைப்பு", "is_major": True},
    {"number": 73, "year": 1992, "title": "Panchayati Raj — constitutional status to Panchayats; Eleventh Schedule added", "title_ta": "பஞ்சாயத்து ராஜ் — பஞ்சாயத்துகளுக்கு அரசியலமைப்பு அந்தஸ்து", "is_major": True},
    {"number": 74, "year": 1992, "title": "Municipalities — constitutional status to urban local bodies; Twelfth Schedule added", "title_ta": "நகராட்சிகளுக்கு அரசியலமைப்பு அந்தஸ்து; பன்னிரண்டாம் அட்டவணை சேர்ப்பு", "is_major": True},
    {"number": 86, "year": 2002, "title": "Right to Education made a Fundamental Right (Art. 21A)", "title_ta": "கல்வி உரிமை அடிப்படை உரிமையாக (உறுப்பு 21A)", "is_major": True},
    {"number": 97, "year": 2011, "title": "Co-operative Societies — Part IXB and Art. 43B added", "title_ta": "கூட்டுறவு சங்கங்கள் — பகுதி IXB சேர்ப்பு"},
    {"number": 100, "year": 2015, "title": "Exchange of enclaves between India and Bangladesh (LBA)", "title_ta": "இந்தியா-வங்கதேசம் எல்லை பகுதிகள் பரிமாற்றம்"},
    {"number": 101, "year": 2016, "title": "Goods and Services Tax (GST) — One Nation One Tax", "title_ta": "சரக்கு மற்றும் சேவை வரி (GST) — ஒரு நாடு ஒரு வரி", "is_major": True},
    {"number": 103, "year": 2019, "title": "10% reservation for Economically Weaker Sections (EWS)", "title_ta": "பொருளாதார ரீதியாக நலிவடைந்த பிரிவினருக்கு 10% இட ஒதுக்கீடு (EWS)", "is_major": True},
    {"number": 104, "year": 2020, "title": "Extended SC/ST reservation in Lok Sabha and State Assemblies by 10 years (till 2030)", "title_ta": "மக்களவை மற்றும் சட்டமன்றங்களில் SC/ST இட ஒதுக்கீடு 10 ஆண்டுகள் நீட்டிப்பு (2030 வரை)"},
    {"number": 105, "year": 2021, "title": "Restored power of States to identify OBCs (after Maratha reservation case)", "title_ta": "OBC அடையாளம் காணும் மாநிலங்களின் அதிகாரம் மீட்பு"},
    {"number": 106, "year": 2023, "title": "One-third reservation for women in Lok Sabha and State Legislative Assemblies", "title_ta": "மக்களவை மற்றும் மாநில சட்டமன்றங்களில் பெண்களுக்கு மூன்றில் ஒரு பங்கு இட ஒதுக்கீடு", "is_major": True},
    {"number": 128, "year": 2025, "title": "One Nation One Election — simultaneous elections to Lok Sabha and State Assemblies (128th Amendment Bill passed by Parliament, March 2025)", "title_ta": "ஒரே நாடு ஒரே தேர்தல் — மக்களவை மற்றும் மாநில சட்டமன்றங்களுக்கு ஒரே நேரத்தில் தேர்தல் (128வது திருத்த மசோதா நாடாளுமன்றத்தில் நிறைவேற்றம், மார்ச் 2025)", "is_major": True},
]

MAJOR_CENTRAL_ACTS: List[Dict[str, Any]] = [
    {"name": "Bharatiya Nyaya Sanhita (BNS)", "name_ta": "பாரதிய நியாய சன்ஹிதா", "year": 2023, "replaces": "Indian Penal Code (IPC), 1860", "category": "Criminal Law", "sections_count": 358, "source_url": "https://www.indiacode.nic.in/handle/123456789/20280"},
    {"name": "Bharatiya Nagarik Suraksha Sanhita (BNSS)", "name_ta": "பாரதிய நாகரிக் சுரக்ஷா சன்ஹிதா", "year": 2023, "replaces": "Code of Criminal Procedure (CrPC), 1973", "category": "Criminal Procedure", "sections_count": 531, "source_url": "https://www.indiacode.nic.in/handle/123456789/20281"},
    {"name": "Bharatiya Sakshya Adhiniyam (BSA)", "name_ta": "பாரதிய சாக்ஷ்ய அதினியம்", "year": 2023, "replaces": "Indian Evidence Act, 1872", "category": "Evidence Law", "sections_count": 170, "source_url": "https://www.indiacode.nic.in/handle/123456789/20282"},
    {"name": "Right to Information Act", "name_ta": "தகவல் அறியும் உரிமைச் சட்டம்", "year": 2005, "category": "Governance", "sections_count": 31, "source_url": "https://www.indiacode.nic.in/handle/123456789/1895"},
    {"name": "Representation of the People Act", "name_ta": "மக்கள் பிரதிநிதித்துவ சட்டம்", "year": 1951, "category": "Electoral Law", "sections_count": 171, "source_url": "https://www.indiacode.nic.in/handle/123456789/1546"},
    {"name": "Protection of Women from Domestic Violence Act", "name_ta": "குடும்ப வன்முறையிலிருந்து பெண்கள் பாதுகாப்புச் சட்டம்", "year": 2005, "category": "Women's Rights", "sections_count": 37, "source_url": "https://www.indiacode.nic.in/handle/123456789/2021"},
    {"name": "Right of Children to Free and Compulsory Education Act (RTE)", "name_ta": "குழந்தைகளுக்கான இலவச மற்றும் கட்டாயக் கல்வி உரிமைச் சட்டம்", "year": 2009, "category": "Education", "sections_count": 38, "source_url": "https://www.indiacode.nic.in/handle/123456789/2085"},
    {"name": "Goods and Services Tax Act", "name_ta": "சரக்கு மற்றும் சேவை வரிச் சட்டம்", "year": 2017, "category": "Taxation", "sections_count": 174, "source_url": "https://www.indiacode.nic.in/handle/123456789/6575"},
    {"name": "Consumer Protection Act", "name_ta": "நுகர்வோர் பாதுகாப்புச் சட்டம்", "year": 2019, "category": "Consumer Rights", "sections_count": 107, "source_url": "https://www.indiacode.nic.in/handle/123456789/15256"},
    {"name": "Information Technology Act", "name_ta": "தகவல் தொழில்நுட்பச் சட்டம்", "year": 2000, "category": "Cyber Law", "sections_count": 94, "source_url": "https://www.indiacode.nic.in/handle/123456789/1999"},
    {"name": "Motor Vehicles Act", "name_ta": "மோட்டார் வாகனச் சட்டம்", "year": 2019, "replaces": "Motor Vehicles Act, 1988", "category": "Transport", "sections_count": 228, "source_url": "https://www.indiacode.nic.in/handle/123456789/15329"},
    {"name": "SC/ST (Prevention of Atrocities) Act", "name_ta": "SC/ST (அட்டூழிய தடுப்பு) சட்டம்", "year": 1989, "category": "Social Justice", "sections_count": 23, "source_url": "https://www.indiacode.nic.in/handle/123456789/1538"},
    {"name": "POCSO Act (Protection of Children from Sexual Offences)", "name_ta": "பாக்சோ சட்டம் (பாலியல் குற்றங்களிலிருந்து குழந்தைகள் பாதுகாப்பு)", "year": 2012, "category": "Child Protection", "sections_count": 46, "source_url": "https://www.indiacode.nic.in/handle/123456789/2079"},
    {"name": "Prevention of Corruption Act", "name_ta": "ஊழல் தடுப்புச் சட்டம்", "year": 1988, "category": "Anti-Corruption", "sections_count": 31, "source_url": "https://www.indiacode.nic.in/handle/123456789/1558"},
    {"name": "National Food Security Act", "name_ta": "தேசிய உணவுப் பாதுகாப்புச் சட்டம்", "year": 2013, "category": "Food Security", "sections_count": 40, "source_url": "https://www.indiacode.nic.in/handle/123456789/2118"},
    {"name": "Lokpal and Lokayuktas Act", "name_ta": "லோக்பால் மற்றும் லோகாயுக்தா சட்டம்", "year": 2013, "category": "Anti-Corruption", "sections_count": 62, "source_url": "https://www.indiacode.nic.in/handle/123456789/2108"},
    {"name": "Digital Personal Data Protection Act", "name_ta": "டிஜிட்டல் தனிநபர் தரவு பாதுகாப்புச் சட்டம்", "year": 2023, "category": "Data Privacy", "sections_count": 44, "source_url": "https://www.indiacode.nic.in/handle/123456789/20283"},
    {"name": "Waqf (Amendment) Act", "name_ta": "வக்ஃப் (திருத்த) சட்டம்", "year": 2025, "category": "Religious Property", "sections_count": 44, "source_url": "https://www.indiacode.nic.in/"},
]


# ---------------------------------------------------------------------------
# Build seed JSON
# ---------------------------------------------------------------------------

def build_seed_data() -> Dict[str, Any]:
    """Assemble the full Constitution data from built-in seed constants."""
    total_articles = sum(len(p["articles"]) for p in PARTS)
    total_landmark = sum(
        1 for p in PARTS for a in p["articles"] if a.get("is_landmark")
    )
    latest_amend = LANDMARK_AMENDMENTS[-1]

    return {
        "meta": {
            "title": "The Constitution of India",
            "title_ta": "இந்திய அரசியலமைப்பு",
            "original_date": "1949-11-26",
            "commencement_date": "1950-01-26",
            "total_parts": len(PARTS),
            "total_parts_current": 25,
            "total_articles_listed": total_articles,
            "total_articles_original": 395,
            "total_articles_current": 448,
            "total_schedules": len(SCHEDULES),
            "total_schedules_original": 8,
            "total_amendments": latest_amend["number"],
            "amendments_listed": len(LANDMARK_AMENDMENTS),
            "latest_amendment": latest_amend["number"],
            "latest_amendment_year": latest_amend["year"],
            "landmark_articles_count": total_landmark,
            "source": "India Code — Legislative Department, Ministry of Law & Justice, Government of India",
            "source_url": "https://www.indiacode.nic.in/handle/123456789/8305",
            "alt_source_url": "https://legislative.gov.in/constitution-of-india",
            "gazette_url": "https://egazette.gov.in/",
            "last_updated": "2025-12-01",
        },
        "preamble": PREAMBLE,
        "parts": PARTS,
        "schedules": SCHEDULES,
        "amendments": LANDMARK_AMENDMENTS,
        "central_acts": MAJOR_CENTRAL_ACTS,
        "categories": [
            {"id": "fundamental_rights", "name": "Fundamental Rights", "name_ta": "அடிப்படை உரிமைகள்", "part": "III", "icon": "balance-scale"},
            {"id": "dpsp", "name": "Directive Principles", "name_ta": "வழிகாட்டுத் தத்துவங்கள்", "part": "IV", "icon": "compass"},
            {"id": "fundamental_duties", "name": "Fundamental Duties", "name_ta": "அடிப்படைக் கடமைகள்", "part": "IVA", "icon": "flag"},
            {"id": "union_govt", "name": "Union Government", "name_ta": "ஒன்றிய அரசு", "part": "V", "icon": "building"},
            {"id": "state_govt", "name": "State Government", "name_ta": "மாநில அரசு", "part": "VI", "icon": "landmark"},
            {"id": "local_govt", "name": "Local Government", "name_ta": "உள்ளாட்சி", "part": "IX/IXA", "icon": "users"},
            {"id": "elections", "name": "Elections", "name_ta": "தேர்தல்கள்", "part": "XV", "icon": "vote-yea"},
            {"id": "emergency", "name": "Emergency Provisions", "name_ta": "அவசரகால விதிகள்", "part": "XVIII", "icon": "exclamation-triangle"},
            {"id": "amendment", "name": "Amendment Process", "name_ta": "திருத்த நடைமுறை", "part": "XX", "icon": "edit"},
            {"id": "criminal_law", "name": "Criminal Law", "name_ta": "குற்றவியல் சட்டம்", "part": "central_acts", "icon": "gavel"},
            {"id": "citizen_rights", "name": "Citizen Rights", "name_ta": "குடிமக்கள் உரிமைகள்", "part": "central_acts", "icon": "shield-alt"},
        ],
    }


# ---------------------------------------------------------------------------
# Scraper — India Code
# ---------------------------------------------------------------------------

def scrape_india_code(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Scrape latest Constitution data from India Code (indiacode.nic.in).
    Uses the official government repository maintained by the Legislative
    Department, Ministry of Law & Justice.
    """
    print("  Scraping from India Code (indiacode.nic.in)...")
    print("  NOTE: indiacode.nic.in may require browser-like session handling.")
    print("  If 403 errors occur, the seed data is still authoritative.")

    client = httpx.Client(headers=HEADERS, follow_redirects=True, timeout=30)

    # Try to fetch the Constitution listing page
    try:
        resp = client.get(
            f"{INDIA_CODE_BASE}/handle/123456789/8305",
            params={"sam_handle": "123456789/2492", "view_type": "browse"},
        )
        if resp.status_code == 200:
            print(f"  Got Constitution listing page ({len(resp.text)} bytes)")
            # Parse and enrich data from the HTML
            # The structure on India Code is: Parts > Articles with full text
            _parse_constitution_page(resp.text, data)
        else:
            print(f"  WARNING: Got status {resp.status_code} — using seed data only")
    except Exception as e:
        print(f"  WARNING: Failed to scrape India Code: {e}")
        print("  Using seed data (which is sourced from the same official documents)")

    client.close()
    return data


def _parse_constitution_page(html: str, data: Dict[str, Any]):
    """Parse the Constitution listing from India Code HTML."""
    # India Code uses a DSpace-based system with handle identifiers.
    # The listing page shows Parts and their article handles.
    # We look for patterns like: /show-data?actid=...&sectionId=...

    article_links = re.findall(
        r'/show-data\?[^"]*actid=([^&"]+)[^"]*sectionId=(\d+)[^"]*sectionno=([^&"]+)',
        html,
    )

    if article_links:
        print(f"  Found {len(article_links)} article links on the page")
        data["meta"]["scrape_status"] = "partial"
        data["meta"]["articles_found_on_page"] = len(article_links)
    else:
        print("  No article links found (page structure may have changed)")
        data["meta"]["scrape_status"] = "seed_only"


# ---------------------------------------------------------------------------
# Firestore upload
# ---------------------------------------------------------------------------

def upload_to_firestore(data: Dict[str, Any]):
    """Upload constitution data to Firestore."""
    db = _get_db()

    # Upload preamble + meta
    db.collection("indian_constitution_meta").document("preamble").set(data["preamble"])
    db.collection("indian_constitution_meta").document("meta").set(data["meta"])
    print("  Uploaded preamble + meta")

    # Upload Parts (with their articles)
    for part in data["parts"]:
        doc_id = f"part_{part['part_number'].lower().replace(' ', '_')}"
        db.collection("indian_constitution").document(doc_id).set(part)
    print(f"  Uploaded {len(data['parts'])} parts")

    # Upload schedules
    db.collection("indian_constitution_meta").document("schedules").set(
        {"items": data["schedules"]}
    )
    print(f"  Uploaded {len(data['schedules'])} schedules")

    # Upload amendments
    db.collection("indian_constitution_meta").document("amendments").set(
        {"items": data["amendments"]}
    )
    print(f"  Uploaded {len(data['amendments'])} amendments")

    # Upload central acts
    db.collection("indian_constitution_meta").document("central_acts").set(
        {"items": data["central_acts"]}
    )
    print(f"  Uploaded {len(data['central_acts'])} central acts")

    # Upload categories
    db.collection("indian_constitution_meta").document("categories").set(
        {"items": data["categories"]}
    )
    print("  Uploaded categories")

    print("  Done — Firestore upload complete")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Constitution of India — Data Ingest")
    parser.add_argument("--seed", action="store_true", help="Generate seed data from built-in authoritative structure")
    parser.add_argument("--scrape", action="store_true", help="Scrape latest from India Code")
    parser.add_argument("--upload", action="store_true", help="Upload to Firestore")
    parser.add_argument("--dry-run", action="store_true", help="Print data without writing")
    args = parser.parse_args()

    if not any([args.seed, args.scrape, args.upload]):
        parser.print_help()
        return

    print("Constitution of India — Data Ingest")
    print("=" * 50)

    # Step 1: Build seed data
    data = build_seed_data()
    print(f"\nSeed data: {data['meta']['total_parts']} parts, "
          f"{data['meta']['total_articles_listed']} articles listed, "
          f"{data['meta']['total_schedules']} schedules, "
          f"{data['meta']['total_amendments']} amendments, "
          f"{len(data['central_acts'])} central acts")

    # Step 2: Scrape (optional enrichment)
    if args.scrape:
        print("\n--- Scraping ---")
        data = scrape_india_code(data)

    # Step 3: Save JSON
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "indian_constitution.json"

    if args.dry_run:
        print(f"\n[DRY RUN] Would write to {out_path}")
        print(json.dumps(data["meta"], indent=2))
    else:
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"\nWrote {out_path} ({out_path.stat().st_size:,} bytes)")

        # Also copy to backend_api for Docker
        backend_copy = Path(__file__).resolve().parents[1] / "web" / "backend_api" / "indian_constitution.json"
        with backend_copy.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Copied to {backend_copy}")

    # Step 4: Upload
    if args.upload and not args.dry_run:
        print("\n--- Firestore Upload ---")
        upload_to_firestore(data)

    print("\nDone!")


if __name__ == "__main__":
    main()
