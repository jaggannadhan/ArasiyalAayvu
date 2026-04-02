"""
ADR Criminal Case Ingest
========================
Parses the ADR Tamil Nadu Assembly 2021 Sitting MLAs PDF (text extracted via
pdftotext) and patches structured criminal case data into the
`candidate_accountability` Firestore collection.

Source PDF:
  https://adrindia.org/sites/default/files/Tamil_Nadu_Assembly_Election_2021_Sitting_MLAs_Report_Finalver_Eng.pdf

Usage:
  # Extract PDF text first (one-time):
  pdftotext <pdf_path> /tmp/adr_tn2021.txt

  # Dry-run (print what would be written):
  .venv/bin/python scrapers/adr_criminal_ingest.py --dry-run

  # Live ingest:
  .venv/bin/python scrapers/adr_criminal_ingest.py

  # Save parsed JSON for inspection:
  .venv/bin/python scrapers/adr_criminal_ingest.py --export-json /tmp/adr_cases.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────
ROOT_DIR   = Path(__file__).resolve().parents[1]
MAP_PATH   = ROOT_DIR / "web" / "src" / "lib" / "constituency-map.json"
PDF_TEXT   = Path("/tmp/adr_tn2021.txt")

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "naatunadappu")
NOW_ISO    = datetime.now(timezone.utc).isoformat()

# ─────────────────────────────────────────────────────────────────────────────
# ADR "Serious IPC" section set  (max punishment ≥ 5 yrs or heinous offence)
# Mirrors web/src/lib/formatters.ts → isSeriousCrime()
# ─────────────────────────────────────────────────────────────────────────────
SERIOUS_IPC = {
    "121", "121A", "122", "123", "124A",
    "120B",
    "147", "148",
    "153A", "153B",
    "302", "303", "304", "304B", "306", "307", "308",
    "326", "326A", "326B",
    "354A", "354B", "354C", "354D",
    "363", "363A", "364", "364A", "365", "366", "366A", "366B",
    "370", "370A", "371", "372", "373",
    "375", "376", "376A", "376B", "376C", "376D", "376E", "377",
    "384", "385", "386", "387",
    "392", "393", "394", "395", "396", "397", "398", "399",
    "406", "409",
    "420",
    "436", "437", "438",
    "467", "468",
    "498A",
}


def is_serious(sections: list[str]) -> bool:
    return any(s.strip().upper() in SERIOUS_IPC for s in sections)


# ─────────────────────────────────────────────────────────────────────────────
# IPC section → short description  (top ~80 most common)
# ─────────────────────────────────────────────────────────────────────────────
IPC_DESCRIPTIONS: dict[str, str] = {
    "101": "Right of private defence extending to causing harm",
    "107": "Abetment of a thing",
    "109": "Abetment — punishment when act is committed",
    "120B": "Criminal conspiracy",
    "143": "Unlawful assembly",
    "144": "Unlawful assembly with deadly weapon",
    "145": "Joining unlawful assembly after command to disperse",
    "147": "Rioting",
    "148": "Rioting with deadly weapon",
    "149": "Every member of unlawful assembly guilty of offence",
    "151": "Knowingly continuing in unlawful assembly",
    "153A": "Promoting enmity between groups",
    "153B": "Imputations prejudicial to national integration",
    "186": "Obstructing public servant in discharge of functions",
    "188": "Disobedience to order of public servant",
    "201": "Causing disappearance of evidence of offence",
    "241": "Delivering counterfeit coin",
    "269": "Negligent act likely to spread infection",
    "270": "Malignant act likely to spread infection",
    "283": "Danger or obstruction in public way",
    "285": "Negligent conduct with fire or combustible matter",
    "294B": "Obscene acts / songs",
    "302": "Murder",
    "303": "Murder by life-convict",
    "304": "Culpable homicide not amounting to murder",
    "304A": "Causing death by negligence",
    "304B": "Dowry death",
    "306": "Abetment of suicide",
    "307": "Attempt to murder",
    "308": "Attempt to commit culpable homicide",
    "323": "Voluntarily causing hurt",
    "324": "Causing hurt by dangerous weapons",
    "325": "Grievous hurt",
    "326": "Grievous hurt by dangerous weapons",
    "326A": "Voluntarily causing grievous hurt by use of acid",
    "341": "Wrongful restraint",
    "342": "Wrongful confinement",
    "354": "Assault or criminal force to woman",
    "354A": "Sexual harassment",
    "354B": "Assault with intent to disrobe",
    "354C": "Voyeurism",
    "354D": "Stalking",
    "363": "Kidnapping",
    "364": "Kidnapping to murder",
    "364A": "Kidnapping for ransom",
    "365": "Kidnapping or abducting to secretly confine",
    "366": "Kidnapping to compel marriage",
    "370": "Trafficking of persons",
    "376": "Rape",
    "376A": "Rape — causing death or persistent vegetative state",
    "376B": "Sexual intercourse by husband upon his wife during separation",
    "376C": "Sexual intercourse by person in authority",
    "376D": "Gang rape",
    "377": "Unnatural offences",
    "379": "Punishment for theft",
    "380": "Theft in dwelling house",
    "382": "Theft after preparation for hurt",
    "384": "Extortion",
    "385": "Putting person in fear for extortion",
    "386": "Extortion by putting person in fear of death",
    "387": "Extortion by threat of grievous hurt or death",
    "392": "Robbery",
    "393": "Attempt to commit robbery",
    "394": "Robbery with hurt",
    "395": "Dacoity",
    "397": "Robbery or dacoity with attempt to cause death",
    "399": "Making preparation to commit dacoity",
    "406": "Criminal breach of trust",
    "409": "Criminal breach of trust by public servant or banker",
    "419": "Cheating by personation",
    "420": "Cheating and dishonestly inducing delivery of property",
    "436": "Mischief by fire or explosive substance",
    "447": "Criminal trespass",
    "448": "House trespass",
    "452": "House trespass with preparation for hurt",
    "465": "Forgery",
    "466": "Forgery of record of court or public register",
    "467": "Forgery of valuable security or will",
    "468": "Forgery for purpose of cheating",
    "471": "Using as genuine a forged document",
    "472": "Making or possessing counterfeit seal",
    "477": "Fraudulent cancellation or destruction of will",
    "498A": "Husband or relatives subjecting wife to cruelty",
    "504": "Intentional insult to provoke breach of peace",
    "505": "Statements conducing to public mischief",
    "506": "Criminal intimidation",
    "507": "Criminal intimidation by anonymous communication",
    "509": "Word or gesture intending to insult modesty of woman",
}

OTHER_ACT_DESCRIPTIONS: dict[str, str] = {
    "POCSO": "Protection of Children from Sexual Offences Act",
    "PC ACT": "Prevention of Corruption Act",
    "PREVENTION OF CORRUPTION": "Prevention of Corruption Act",
    "SC/ST": "SC/ST (Prevention of Atrocities) Act",
    "ATROCITIES": "SC/ST (Prevention of Atrocities) Act",
    "ARMS ACT": "Arms Act",
    "NDPS": "Narcotics Drugs and Psychotropic Substances Act",
    "GOONDAS": "TN Goondas Act",
    "PPDL": "TN Property Destruction Prevention Law",
    "EPIDEMIC": "Epidemic Diseases Act",
    "DISASTER": "Disaster Management Act",
}


def sections_to_description(sections: list[str], other_details: str) -> str:
    """Generate a human-readable description from IPC sections + other details."""
    parts: list[str] = []
    for s in sections:
        sec = s.strip().upper()
        if sec in IPC_DESCRIPTIONS:
            parts.append(IPC_DESCRIPTIONS[sec])

    # Check other_details for known act names
    od_upper = other_details.upper()
    for key, label in OTHER_ACT_DESCRIPTIONS.items():
        if key in od_upper and label not in parts:
            parts.append(label)

    if parts:
        return "; ".join(parts[:3]) + ("…" if len(parts) > 3 else "")
    # Fallback: first section number
    return f"IPC {', '.join(sections[:3])}" if sections else "Criminal case"


# ─────────────────────────────────────────────────────────────────────────────
# Constituency name → slug mapping
# ─────────────────────────────────────────────────────────────────────────────
def build_name_to_slug(map_path: Path) -> dict[str, str]:
    data: dict[str, dict] = json.loads(map_path.read_text(encoding="utf-8"))
    result: dict[str, str] = {}
    for slug, entry in data.items():
        name = entry.get("name", "").strip().upper()
        if name:
            result[name] = slug
    return result


def normalise_name(name: str) -> str:
    """Strip common parenthetical suffixes for fuzzy matching."""
    return re.sub(r"\s*\([^)]*\)\s*$", "", name.strip().upper())


def match_constituency(raw: str, name_to_slug: dict[str, str]) -> str | None:
    key = raw.strip().upper()
    if key in name_to_slug:
        return name_to_slug[key]
    # Try without parenthetical suffix
    normed = normalise_name(key)
    if normed in name_to_slug:
        return name_to_slug[normed]
    # Partial match
    for name, slug in name_to_slug.items():
        if normed in name or name in normed:
            return slug
    return None


# ─────────────────────────────────────────────────────────────────────────────
# PDF parser
# ─────────────────────────────────────────────────────────────────────────────

# Matches start of individual case line:  "1.", "10.", "25." etc.
CASE_LINE_RE = re.compile(r"^\s*(\d+)\.\s+(IPC Sections\s+-\s+)(.*)", re.DOTALL)

# Boilerplate injected by pdftotext at every page break — strip before parsing
PAGE_NOISE_RE = re.compile(
    r"Data in this Kit is presented[^\n]*\n"
    r"analysis\. Website:[^\n]*\n"
    r"\n?"
    r"Page \d+ of \d+\s*\n"
    r"(?:S\.No\.\s*\n\s*MLA Information\s*\n\s*Brief Details of IPCs\s*\n)?",
    re.IGNORECASE,
)

# Known header field keywords — used as stop-anchors in field extraction
_FIELD_STOP = (
    r"(?=\s*(?:District|Constituency|Party|Total Cases|Serious IPC|Other IPC"
    r"|Cases \(Pending\)|Cases \(Convicted\))\b)"
)


def _strip_block_noise(block: str) -> str:
    """Remove pdftotext page-break boilerplate from an MLA block."""
    return PAGE_NOISE_RE.sub("", block)


def _field(key: str, text: str) -> str:
    """Extract the value after 'KEY:' up to the next known field keyword."""
    m = re.search(
        rf"(?:^|\s){re.escape(key)}:?\s*(.+?){_FIELD_STOP}",
        text,
        re.DOTALL | re.IGNORECASE,
    )
    if not m:
        return ""
    return re.sub(r"\s+", " ", m.group(1)).strip(" ,\n")


def parse_mla_header(block: str) -> dict[str, Any] | None:
    """
    Robustly extract MLA header fields from a block.

    Handles three pdftotext layout variants that break a rigid sequential regex:
      A) All fields on one line:  "Name:X District:Y Constituency:Z Party:P Total Cases:N"
      B) Page break mid-header:  Name+District on page N, remaining fields on page N+1
      C) Missing Serious/Other IPC before the charge-summary spills onto the next page
    """
    clean = _strip_block_noise(block)

    name         = _field("Name",        clean)
    district     = _field("District",    clean)
    constituency = _field("Constituency", clean)
    party        = _field("Party",       clean)
    total_str    = _field("Total Cases", clean)

    if not name or not district or not constituency:
        return None
    if not total_str or not re.match(r"^\d+$", total_str):
        # Fallback: count IPC section charge-summary lines as proxy for total
        total_str = str(len(re.findall(r"IPC Section-", clean)))
        if total_str == "0":
            return None

    return {
        "name":         name,
        "district":     district,
        "constituency": constituency,
        "party":        party,
        "total_cases":  int(total_str),
    }


def parse_case_line(line: str) -> dict[str, Any] | None:
    """
    Parse a single case entry such as:
      "IPC Sections - 120B, 302, Other Details - ..., Case No. - ..., Court - ..., FIR No. - ...,
       Charges Framed - Yes, Date Charges Framed - 20 Nov 2019, Appeal Filed - No"
    Returns a dict or None if the line is not a recognisable case entry.
    """
    # The CASE_LINE_RE stripped the leading number+period. What remains starts
    # after "IPC Sections - ".
    rest = line.strip()

    def extract(key: str, text: str) -> str:
        """Pull the value after 'KEY - ' up to the next known key."""
        pattern = rf"{re.escape(key)}\s*-\s*(.*?)(?=,?\s*(?:IPC Sections|Other Details|Case No\.|Court|FIR No\.|Charges Framed|Date Charges Framed|Appeal Filed|$))"
        m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        return m.group(1).strip(" ,\n") if m else ""

    # IPC sections (comma-separated, before "Other Details")
    ipc_raw = extract("IPC Sections", rest)
    sections: list[str] = []
    if ipc_raw:
        sections = [s.strip() for s in re.split(r"[,;]\s*", ipc_raw) if s.strip()]

    other_details  = extract("Other Details", rest)
    case_no        = extract("Case No\\.", rest)
    court_raw      = extract("Court", rest)
    fir_raw        = extract("FIR No\\.", rest)

    charges_framed_m = re.search(r"Charges Framed\s*-\s*(Yes|No)", rest, re.IGNORECASE)
    charges_framed = (charges_framed_m.group(1).strip() if charges_framed_m else "").lower() == "yes"

    # Build a clean court string (drop trailing page markers etc.)
    court = re.sub(r"\s+", " ", court_raw).strip()
    court = re.sub(r"\s*Page \d+ of \d+\s*", "", court).strip(" ,")

    return {
        "ipc_sections": sections,
        "act": "IPC" if sections else (other_details[:60].strip() or "IPC"),
        "other_details": other_details,
        "case_no": case_no,
        "court": court,
        "fir_no": fir_raw,
        "charges_framed": charges_framed,
        "is_serious": is_serious(sections),
    }


NOISE_LINES = re.compile(
    r"^(S\.No\.\s*|MLA Information\s*|Brief Details of IPCs\s*"
    r"|Data in this Kit.*|Page \d+ of \d+\s*|MyNeta App.*|Donate.*"
    r"|Association for Democratic.*|Website:.*|ADR Speaks.*|Listen.*"
    r"|Other platforms.*|Contact Details.*|Tamil Nadu Election Watch.*"
    r"|Prof\..*|Maj\.*|Dr\.\s*Ajit.*|\s*)$",
    re.IGNORECASE,
)

# Matches charge-summary lines: "3 charges related to Murder (IPC Section-302)"
CHARGE_SUMMARY_RE = re.compile(
    r"(\d+)\s+charges?\s+related\s+to\s+(.+?)\s*\(IPC\s+Section[-\s]+([\w()\[\]/]+)\)",
    re.IGNORECASE,
)


def clean_lines(raw: str) -> list[str]:
    """Remove noise lines from a block of text."""
    lines = []
    for ln in raw.split("\n"):
        if not NOISE_LINES.match(ln):
            lines.append(ln)
    return lines


def parse_summary_cases(block: str, status_context: str) -> list[dict[str, Any]]:
    """
    Fallback: when no 'IPC Sections - ...' case lines are found, build one
    synthetic case per unique IPC section from the charge-summary lines.
    E.g. "3 charges related to Attempt to murder (IPC Section-307)" → one case entry.
    """
    cases: list[dict[str, Any]] = []
    seen: set[str] = set()
    for m in CHARGE_SUMMARY_RE.finditer(block):
        sec = m.group(3).strip().upper()
        if sec in seen:
            continue
        seen.add(sec)
        desc = m.group(2).strip()
        serious = is_serious([sec])
        cases.append({
            "ipc_sections": [sec],
            "act": "IPC",
            "description": desc,
            "status": status_context if status_context == "Convicted" else "Pending",
            "court": None,
            "fir_no": None,
            "case_no": None,
            "is_serious": serious,
        })
    return cases


def parse_mla_block(raw_block: str, status_context: str) -> list[dict[str, Any]]:
    """
    Parse all case lines within a Pending or Convicted block.
    Returns a list of case dicts.
    """
    if "-----No Cases---" in raw_block:
        return []

    cases: list[dict[str, Any]] = []
    lines = clean_lines(raw_block)
    text = "\n".join(lines)

    # Split on numbered case markers "N." at the start of a token
    # We look for lines starting with a digit followed by period then space + "IPC"
    entries = re.split(r"\n\s*(?=\d+\.\s+IPC Sections)", text)
    for entry in entries:
        entry = entry.strip()
        if not entry:
            continue
        # Strip leading case number
        entry = re.sub(r"^\d+\.\s*", "", entry)
        if not entry.startswith("IPC Sections"):
            # Some entries have "IPC Sections" on the next line after the number
            entry = re.sub(r"^.*?\n", "", entry, count=1)
        if not entry.strip():
            continue

        parsed = parse_case_line("IPC Sections - " + entry.lstrip("IPC Sections -").strip()
                                  if not entry.startswith("IPC Sections")
                                  else entry)
        if parsed is None:
            continue

        # Determine status from context
        # charges_framed=True means trial is underway — still Pending outcome
        if status_context == "Convicted":
            status = "Convicted"
        else:
            status = "Pending"

        cases.append({
            "ipc_sections": parsed["ipc_sections"],
            "act": parsed["act"],
            "description": sections_to_description(
                parsed["ipc_sections"], parsed.get("other_details", "")
            ),
            "status": status,
            "court": parsed["court"] or None,
            "fir_no": parsed["fir_no"] or None,
            "case_no": parsed["case_no"] or None,
            "is_serious": parsed["is_serious"],
        })

    # Fallback: if no structured IPC Sections lines were found, try charge-summary
    if not cases:
        cases = parse_summary_cases(raw_block, status_context)

    return cases


def parse_all_mla_records(text: str) -> list[dict[str, Any]]:
    """
    Split the full PDF text on MLA header blocks, parse each one,
    and return a list of structured MLA records.
    """
    # Find the criminal cases section
    start_idx = text.find("Details of Sitting MLAs with declared Criminal Cases")
    if start_idx == -1:
        raise ValueError("Criminal cases section not found in text")
    section = text[start_idx:]

    # Each MLA block starts with "Name:" and ends just before the next "Name:"
    # or at end of section
    mla_positions = [m.start() for m in re.finditer(r"^Name:", section, re.MULTILINE)]
    records: list[dict[str, Any]] = []

    for i, pos in enumerate(mla_positions):
        end = mla_positions[i + 1] if i + 1 < len(mla_positions) else len(section)
        block = section[pos:end]

        # Parse header — robust to page-splits and single-line layouts
        hdr = parse_mla_header(block)
        if not hdr:
            continue

        mla_name     = hdr["name"]
        district     = hdr["district"]
        constituency = hdr["constituency"]
        party        = hdr["party"]
        total_cases  = hdr["total_cases"]

        # Split block into Pending / Convicted sections
        pending_match   = re.search(r"Cases \(Pending\)", block)
        convicted_match = re.search(r"Cases \(Convicted\)", block)

        pending_cases: list[dict] = []
        convicted_cases: list[dict] = []

        if pending_match and convicted_match:
            pending_text   = block[pending_match.end():convicted_match.start()]
            convicted_text = block[convicted_match.end():]
            pending_cases   = parse_mla_block(pending_text,   "Pending")
            convicted_cases = parse_mla_block(convicted_text, "Convicted")
        elif pending_match:
            pending_text = block[pending_match.end():]
            pending_cases = parse_mla_block(pending_text, "Pending")
        elif convicted_match:
            convicted_text = block[convicted_match.end():]
            convicted_cases = parse_mla_block(convicted_text, "Convicted")

        all_cases = pending_cases + convicted_cases

        records.append({
            "mla_name": mla_name,
            "district": district,
            "constituency": constituency,
            "party": party,
            "total_cases_declared": total_cases,
            "cases": all_cases,
        })

    return records


# ─────────────────────────────────────────────────────────────────────────────
# Firestore doc_id resolution
# ─────────────────────────────────────────────────────────────────────────────

def resolve_doc_id(constituency: str, name_to_slug: dict[str, str]) -> str | None:
    slug = match_constituency(constituency, name_to_slug)
    if slug:
        return f"2021_{slug}"
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="ADR Criminal Case Ingest")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be written; don't touch Firestore")
    parser.add_argument("--export-json", metavar="PATH",
                        help="Write parsed JSON to this file instead of (or in addition to) Firestore")
    parser.add_argument("--pdf-text", default=str(PDF_TEXT),
                        help=f"Path to pdftotext output (default: {PDF_TEXT})")
    args = parser.parse_args()

    txt_path = Path(args.pdf_text)
    if not txt_path.exists():
        print(f"ERROR: PDF text file not found: {txt_path}")
        print("Run: pdftotext <path_to_pdf> /tmp/adr_tn2021.txt")
        sys.exit(1)

    print(f"Reading {txt_path} …")
    text = txt_path.read_text(encoding="utf-8", errors="replace")

    print("Parsing MLA records …")
    records = parse_all_mla_records(text)
    print(f"  Found {len(records)} MLA records with criminal cases")

    # Enrich with constituency slugs
    name_to_slug = build_name_to_slug(MAP_PATH)
    unmatched: list[str] = []

    enriched: list[dict] = []
    for rec in records:
        doc_id = resolve_doc_id(rec["constituency"], name_to_slug)
        if doc_id is None:
            unmatched.append(f"{rec['constituency']} ({rec['mla_name']})")
        enriched.append({**rec, "doc_id": doc_id})

    if unmatched:
        print(f"\nWARN: {len(unmatched)} constituencies could not be matched to a slug:")
        for u in unmatched:
            print(f"  ✗ {u}")

    matched = [r for r in enriched if r["doc_id"] is not None]
    print(f"\n  Matchable: {len(matched)} / {len(enriched)}")
    for r in matched:
        print(f"  {r['doc_id']:<40} {r['mla_name']:<30} cases={r['total_cases_declared']} parsed={len(r['cases'])}")

    # Export JSON if requested
    if args.export_json:
        out = Path(args.export_json)
        out.write_text(json.dumps(enriched, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\nJSON written to {out}")

    if args.dry_run:
        print("\n[dry-run] — Firestore not modified.")
        return

    # Firestore ingest
    from google.cloud import firestore
    db = firestore.Client(project=PROJECT_ID)
    col = db.collection("candidate_accountability")

    written = 0
    skipped = 0
    for rec in matched:
        doc_ref = col.document(rec["doc_id"])
        snap = doc_ref.get()
        if not snap.exists:
            print(f"  SKIP {rec['doc_id']} — doc not found in Firestore")
            skipped += 1
            continue

        cases = rec["cases"]
        # Recompute severity from actual parsed cases (overrides the MyNeta-scraped value)
        if not cases:
            severity = "CLEAN"
        elif any(c["is_serious"] for c in cases):
            severity = "SERIOUS"
        elif len(cases) >= 3:
            severity = "MODERATE"
        else:
            severity = "MINOR"

        doc_ref.set(
            {
                "criminal_cases": cases,
                "criminal_severity": severity,
                "criminal_cases_parsed_at": NOW_ISO,
                "criminal_cases_source": "ADR TN 2021 Sitting MLAs Report",
            },
            merge=True,
        )
        written += 1
        print(f"  ✓ {rec['doc_id']} ({len(rec['cases'])} cases patched)")

    print(f"\nDone. Written: {written}, Skipped: {skipped}")


if __name__ == "__main__":
    main()
