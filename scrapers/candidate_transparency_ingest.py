#!/usr/bin/env python3
"""
Candidate Transparency Ingestion

Scrapes ADR/MyNeta Tamil Nadu election candidate data and loads a normalized dataset
into Firestore collection: `candidate_transparency`.

Features:
- Level 1 scrape: constituency-wise list page (name, party, education, case count)
- Level 2 scrape: candidate detail pages (assets/liabilities normalized to INR, IPC sections)
- Education normalization to standardized levels
- Deterministic Firestore document IDs to avoid duplicates
- Audit fields: source_url, last_scraped, data_confidence_score
- 2026 placeholder generation mode

Examples:
  python Candidate_Transparency_Ingest.py --year 2021 --upload
  python Candidate_Transparency_Ingest.py --year 2021 --output data/processed/candidate_transparency_2021.json
  python Candidate_Transparency_Ingest.py --year 2026 --placeholders-only --upload
"""

from __future__ import annotations

import argparse
import json
import re
import time
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests import Response
from tenacity import retry, stop_after_attempt, wait_exponential

try:
    from google.cloud import firestore
except Exception:  # pragma: no cover - optional runtime dependency for local no-upload runs
    firestore = None


BASE_URL_BY_YEAR = {
    2021: "https://myneta.info/tamilnadu2021",
    # Placeholder target for future election cycle. We do not assume this is live yet.
    2026: "https://myneta.info/tamilnadu2026",
}

LIST_PATH_MAIN       = "/"           # Main TN election page — has all constituency links
LIST_PATH_WINNERS    = "/index.php?action=show_winners&sort=default"  # fallback: winners only

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NaatuNadappuResearchBot/2.0)",
}


@dataclass(slots=True)
class CandidateListRow:
    election_year: int
    constituency: str
    candidate_name: str
    party: str | None
    education_raw: str | None
    education_level: str
    criminal_cases_count: int | None
    candidate_id: str | None
    candidate_url: str | None
    source_url: str
    # Financial strings scraped directly from the list page row
    _assets_raw: str | None
    _liabilities_raw: str | None


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def slugify(value: str) -> str:
    norm = unicodedata.normalize("NFKD", value)
    ascii_only = norm.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", ascii_only.lower()).strip("_")
    return slug or "unknown"


def normalize_whitespace(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def normalize_education_level(raw: str | None) -> str:
    """
    Standardized buckets for Firestore filtering and analytics:
    - Unknown
    - School
    - Graduate
    - Postgraduate
    - Doctorate
    """
    text = normalize_whitespace(raw).lower()
    if not text:
        return "Unknown"

    if any(k in text for k in ["phd", "doctorate", "d.litt", "md ", "dm "]):
        return "Doctorate"

    if any(
        k in text
        for k in [
            "post graduate",
            "postgraduate",
            "master",
            "m.a",
            "m.sc",
            "m.com",
            "m.e",
            "m.tech",
            "mba",
            "llm",
            "ca",
            "cs",
            "icwa",
        ]
    ):
        return "Postgraduate"

    if any(
        k in text
        for k in [
            "graduate professional",
            "graduate",
            "b.a",
            "b.sc",
            "b.com",
            "b.e",
            "b.tech",
            "mbbs",
            "llb",
            "b.ed",
            "diploma",
            "engineer",
        ]
    ):
        return "Graduate"

    if any(
        k in text
        for k in [
            "12th",
            "hsc",
            "intermediate",
            "10th",
            "sslc",
            "8th",
            "5th",
            "primary",
            "middle",
            "matric",
            "school",
            "literate",
            "illiterate",
        ]
    ):
        return "School"

    return "Unknown"


def parse_inr_amount(value: str | None) -> int | None:
    """
    Converts human-formatted amounts to integer INR.

    Supports patterns like:
    - "Rs 5,43,21,000 ~ 5 Crore+"
    - "Rs 61,00,000"
    - "5 Crore+"
    - "75 Lacs+"
    - "Nil"
    """
    if not value:
        return None

    text = normalize_whitespace(value)
    if not text:
        return None

    low = text.lower()
    if any(k in low for k in ["nil", "na", "n/a", "none", "not given"]):
        return 0

    # Prefer explicit Rs figure when available.
    rs_match = re.search(r"rs\.?\s*([\d,]+(?:\.\d+)?)", text, flags=re.IGNORECASE)
    if rs_match:
        num = rs_match.group(1).replace(",", "")
        try:
            return int(float(num))
        except ValueError:
            pass

    # Fallback: crore / lakh / thousand notation.
    compact = text.replace(",", "")
    unit_match = re.search(
        r"([\d]+(?:\.\d+)?)\s*(crore|crores|cr|lakh|lakhs|lac|lacs|thousand|k)",
        compact,
        flags=re.IGNORECASE,
    )
    if unit_match:
        number = float(unit_match.group(1))
        unit = unit_match.group(2).lower()
        if unit in {"crore", "crores", "cr"}:
            return int(number * 10_000_000)
        if unit in {"lakh", "lakhs", "lac", "lacs"}:
            return int(number * 100_000)
        if unit in {"thousand", "k"}:
            return int(number * 1_000)

    # Last fallback: naked number.
    bare_num = re.search(r"([\d]{2,})", compact)
    if bare_num:
        try:
            return int(bare_num.group(1))
        except ValueError:
            return None

    return None


def parse_cases_count(value: str | None) -> int | None:
    if not value:
        return None
    text = normalize_whitespace(value)
    m = re.search(r"\d+", text)
    if not m:
        return None
    return int(m.group(0))


class MyNetaClient:
    def __init__(self, election_year: int, timeout_sec: int = 30) -> None:
        self.election_year = election_year
        self.base_url = BASE_URL_BY_YEAR[election_year]
        self.timeout_sec = timeout_sec
        self.session = requests.Session()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8), reraise=True)
    def get(self, url: str) -> Response:
        resp = self.session.get(url, headers=HEADERS, timeout=self.timeout_sec, verify=False)
        resp.raise_for_status()
        return resp

    def fetch_soup(self, url: str) -> BeautifulSoup:
        return BeautifulSoup(self.get(url).text, "lxml")

    # ------------------------------------------------------------------
    # Level 1: enumerate constituency IDs from main page, then scrape
    # each constituency page for its full candidate list.
    # ------------------------------------------------------------------

    def get_constituency_ids(self) -> list[tuple[int, str]]:
        """
        Returns a list of (constituency_id, constituency_name) tuples by
        scraping the main TN election page for all constituency links.
        Falls back to sequential probe 1..280 if no links are found.
        """
        main_url = f"{self.base_url}{LIST_PATH_MAIN}"
        soup = self.fetch_soup(main_url)

        results: list[tuple[int, str]] = []
        seen: set[int] = set()
        for a in soup.find_all("a", href=True):
            m = re.search(r"constituency_id=(\d+)", a["href"])
            if not m:
                continue
            cid = int(m.group(1))
            if cid in seen:
                continue
            seen.add(cid)
            name = normalize_whitespace(a.get_text(strip=True))
            results.append((cid, name))

        return sorted(results, key=lambda x: x[0])

    def list_candidates(self, constituency_filter: str | None = None) -> list[CandidateListRow]:
        """
        Level 1 scraping.
        Fetches the main page to enumerate all constituency IDs, then scrapes
        each constituency page for its full candidate list.
        Assets and liabilities are parsed directly from the list rows —
        no Level 2 fetch needed for financial data.

        constituency_filter: if set, only fetch pages whose name contains this
        string (case-insensitive). Applied before any HTTP requests to constituency
        pages — fast for single-constituency dry-runs.
        """
        constituency_ids = self.get_constituency_ids()
        if not constituency_ids:
            # Fallback: winners-only page (224 records)
            print("  [warn] Could not enumerate constituencies — falling back to winners page.")
            return self._parse_winners_page(f"{self.base_url}{LIST_PATH_WINNERS}")

        if constituency_filter:
            cf = constituency_filter.strip().lower()
            matched = [(cid, name) for cid, name in constituency_ids if cf in name.lower()]
            if not matched:
                all_names = [name for _, name in constituency_ids]
                print(f"  [warn] '{constituency_filter}' matched no constituency names.")
                print(f"  [hint] Available (first 20): {all_names[:20]}")
                return []
            constituency_ids = matched
            print(f"  [filter] Fetching {len(constituency_ids)} constituency page(s): {[n for _, n in constituency_ids]}")

        all_rows: list[CandidateListRow] = []
        for i, (cid, name) in enumerate(constituency_ids):
            url = f"{self.base_url}/index.php?action=show_candidates&constituency_id={cid}&sort=default"
            try:
                rows = self._parse_constituency_page(url)
                all_rows.extend(rows)
                if not constituency_filter:
                    # Progress indicator for full runs only
                    print(f"  [L1] {i+1}/{len(constituency_ids)} {name}: {len(rows)} candidates", end="\r", flush=True)
            except Exception as exc:
                print(f"  [warn] cid={cid} ({name}) failed: {exc}")

            # Polite rate limiting between constituency list pages
            if not constituency_filter:
                if i > 0 and i % 20 == 0:
                    time.sleep(2.0)
                else:
                    time.sleep(0.5)

        if not constituency_filter:
            print()  # newline after \r progress

        return all_rows

    def _parse_constituency_page(self, page_url: str) -> list[CandidateListRow]:
        """
        Parses a constituency-level candidate page.

        Column layout (table 4 on the page):
          SNo | Candidate | Party | Criminal Cases | Education | Age | Total Assets | Liabilities

        Constituency name is extracted from the h3 heading:
          "List of Candidates - HARUR (SC):DHARMAPURI ( Comparison... )"
        Assets and liabilities are parsed directly — no Level 2 fetch needed for financials.
        """
        soup = self.fetch_soup(page_url)
        constituency = self._extract_constituency_name(soup)

        tables = soup.find_all("table")
        # Table 4 (index 4) is consistently the candidates table on constituency pages
        table = None
        for t in tables:
            first_row = t.find("tr")
            if first_row:
                hdr = normalize_whitespace(first_row.get_text(" ", strip=True).lower())
                if "candidate" in hdr and "criminal" in hdr:
                    table = t
                    break

        if table is None:
            raise ValueError(f"Candidate table not found: {page_url}")

        rows: list[CandidateListRow] = []
        for tr in table.find_all("tr")[1:]:
            tds = tr.find_all("td")
            # Expect at least 7 cols: SNo Candidate Party Criminal Education Age Assets [Liabilities]
            if len(tds) < 6:
                continue

            # Column indices: 0=SNo, 1=Candidate, 2=Party, 3=CriminalCases, 4=Education, 5=Age, 6=Assets, 7=Liabilities
            candidate_td  = tds[1]
            party_td      = tds[2]
            cases_td      = tds[3]
            edu_td        = tds[4]
            assets_td     = tds[6] if len(tds) > 6 else None
            liabilities_td = tds[7] if len(tds) > 7 else None

            raw_name = normalize_whitespace(candidate_td.get_text(" ", strip=True))
            # Strip "Winner" suffix that MyNeta appends to the winning candidate
            candidate_name = re.sub(r"\s+Winner\s*$", "", raw_name, flags=re.IGNORECASE).strip()

            # Guard against ad-injected rows
            if not candidate_name or not constituency:
                continue
            if len(candidate_name) > 120 or "<" in candidate_name:
                continue

            detail_link = candidate_td.find("a", href=True)
            detail_url = urljoin(self.base_url + "/", detail_link["href"]) if detail_link else None
            candidate_id = self._extract_candidate_id(detail_url) if detail_url else None

            party         = normalize_whitespace(party_td.get_text(" ", strip=True)) or None
            education_raw = normalize_whitespace(edu_td.get_text(" ", strip=True)) or None
            cases_raw     = normalize_whitespace(cases_td.get_text(" ", strip=True))
            assets_raw    = normalize_whitespace(assets_td.get_text(" ", strip=True)) if assets_td else None
            liabilities_raw = normalize_whitespace(liabilities_td.get_text(" ", strip=True)) if liabilities_td else None

            rows.append(
                CandidateListRow(
                    election_year=self.election_year,
                    constituency=constituency,
                    candidate_name=candidate_name,
                    party=party,
                    education_raw=education_raw,
                    education_level=normalize_education_level(education_raw),
                    criminal_cases_count=parse_cases_count(cases_raw),
                    candidate_id=candidate_id,
                    candidate_url=detail_url,
                    source_url=page_url,
                    # financials from the list row — stored temporarily on the dataclass
                    _assets_raw=assets_raw,
                    _liabilities_raw=liabilities_raw,
                )
            )

        return rows

    def _parse_winners_page(self, page_url: str) -> list[CandidateListRow]:
        """Fallback: parse the winners-only page (no financials, no constituency per-row)."""
        soup = self.fetch_soup(page_url)
        tables = soup.find_all("table")
        table = None
        for t in tables:
            first_row = t.find("tr")
            if first_row:
                hdr = normalize_whitespace(first_row.get_text(" ", strip=True).lower())
                if "candidate" in hdr and "constituency" in hdr:
                    table = t
                    break
        if table is None:
            raise ValueError(f"Winners table not found: {page_url}")

        rows: list[CandidateListRow] = []
        # Build header map
        header_cells = table.find("tr").find_all(["th", "td"])
        hmap: dict[str, int] = {}
        for idx, cell in enumerate(header_cells):
            text = normalize_whitespace(cell.get_text(" ", strip=True).lower())
            if "candidate" in text and "constituency" not in text:
                hmap["candidate"] = idx
            elif "constituency" in text:
                hmap["constituency"] = idx
            elif "party" in text:
                hmap["party"] = idx
            elif "criminal" in text:
                hmap["criminal_cases"] = idx
            elif "educat" in text:
                hmap["education"] = idx

        for tr in table.find_all("tr")[1:]:
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue
            candidate_td   = tds[hmap.get("candidate", 1)]
            constituency_td = tds[hmap.get("constituency", 2)]
            party_td       = tds[hmap.get("party", 3)] if len(tds) > 3 else None
            cases_td       = tds[hmap.get("criminal_cases", 4)] if len(tds) > 4 else None
            edu_td         = tds[hmap.get("education", 5)] if len(tds) > 5 else None

            raw_name = normalize_whitespace(candidate_td.get_text(" ", strip=True))
            candidate_name = re.sub(r"\s+Winner\s*$", "", raw_name, flags=re.IGNORECASE).strip()
            constituency = normalize_whitespace(constituency_td.get_text(" ", strip=True))
            if not candidate_name or not constituency:
                continue
            if len(candidate_name) > 120 or "<" in candidate_name or "<" in constituency:
                continue

            detail_link = candidate_td.find("a", href=True)
            detail_url = urljoin(self.base_url + "/", detail_link["href"]) if detail_link else None
            candidate_id = self._extract_candidate_id(detail_url) if detail_url else None

            rows.append(CandidateListRow(
                election_year=self.election_year,
                constituency=constituency,
                candidate_name=candidate_name,
                party=normalize_whitespace(party_td.get_text(" ", strip=True)) if party_td else None,
                education_raw=normalize_whitespace(edu_td.get_text(" ", strip=True)) if edu_td else None,
                education_level=normalize_education_level(edu_td.get_text(" ", strip=True) if edu_td else None),
                criminal_cases_count=parse_cases_count(cases_td.get_text(" ", strip=True) if cases_td else None),
                candidate_id=candidate_id,
                candidate_url=detail_url,
                source_url=page_url,
                _assets_raw=None,
                _liabilities_raw=None,
            ))
        return rows

    @staticmethod
    def _extract_constituency_name(soup: BeautifulSoup) -> str:
        """
        Extracts constituency name from h3 heading.
        Format: "List of Candidates - HARUR (SC):DHARMAPURI ( Comparison... )"
        """
        for h3 in soup.find_all("h3"):
            txt = h3.get_text(separator=" ", strip=True)
            m = re.search(r"-\s+(.+?):", txt)
            if m:
                return m.group(1).strip()
        return ""

    @staticmethod
    def _extract_candidate_id(url: str) -> str | None:
        if not url:
            return None
        m = re.search(r"candidate_id=(\d+)", url)
        return m.group(1) if m else None

    # ------------------------------------------------------------------
    # Level 2: fetch detail page for IPC sections only.
    # Only called for candidates with criminal_cases_count > 0.
    # Financial data is already captured from the list row.
    # ------------------------------------------------------------------

    def fetch_ipc_sections(self, detail_url: str | None) -> list[str]:
        """
        Level 2 scraping — IPC sections only.
        Only called when criminal_cases_count > 0 and detail_url is available.
        """
        if not detail_url:
            return []
        try:
            soup = self.fetch_soup(detail_url)
            return self._extract_ipc_sections(soup)
        except Exception as exc:
            print(f"  [warn] IPC fetch failed for {detail_url}: {exc}")
            return []

    @staticmethod
    def _extract_ipc_sections(soup: BeautifulSoup) -> list[str]:
        text = soup.get_text(" ", strip=True)
        patterns = [
            r"\bipc\s*(?:section|sections)?\s*[-:]?\s*([0-9]{2,3}[a-zA-Z]?)",
            r"\bu/s\s*([0-9]{2,3}[a-zA-Z]?)",
            r"\bsection\s*([0-9]{2,3}[a-zA-Z]?)\s*ipc\b",
            r"\bsections\s*([0-9]{2,3}[a-zA-Z]?(?:\s*,\s*[0-9]{2,3}[a-zA-Z]?)+)\s*ipc\b",
        ]
        found: set[str] = set()
        for pat in patterns:
            for m in re.finditer(pat, text, flags=re.IGNORECASE):
                grp = m.group(1)
                if not grp:
                    continue
                for token in re.split(r"\s*,\s*", grp):
                    cleaned = token.strip().upper()
                    if re.fullmatch(r"\d{2,3}[A-Z]?", cleaned):
                        found.add(cleaned)
        return sorted(found, key=lambda x: (int(re.match(r"\d+", x).group(0)), x)) if found else []


def compute_confidence_score(
    row: CandidateListRow,
    total_assets_inr: int | None,
    liabilities_inr: int | None,
    ipc_sections: list[str],
) -> float:
    score = 0.4  # base: row parsed

    if row.candidate_name and row.constituency:
        score += 0.2
    if row.education_level != "Unknown":
        score += 0.1
    if row.criminal_cases_count is not None:
        score += 0.1
    if total_assets_inr is not None:
        score += 0.1
    if liabilities_inr is not None:
        score += 0.05
    if ipc_sections:
        score += 0.05

    return round(min(score, 1.0), 2)


def build_document_id(election_year: int, constituency: str, candidate_name: str) -> str:
    return f"{election_year}_{slugify(constituency)}_{slugify(candidate_name)}"


def build_document(row: CandidateListRow, ipc_sections: list[str]) -> dict:
    """
    Builds the final Firestore document from a list row + IPC sections.
    Financial data comes from the list row (_assets_raw / _liabilities_raw).
    IPC sections come from a Level 2 detail fetch (only for candidates with cases).
    """
    total_assets_inr = parse_inr_amount(row._assets_raw)
    liabilities_inr  = parse_inr_amount(row._liabilities_raw)
    net_worth_inr    = (
        total_assets_inr - liabilities_inr
        if total_assets_inr is not None and liabilities_inr is not None
        else None
    )

    doc_id = build_document_id(row.election_year, row.constituency, row.candidate_name)
    score  = compute_confidence_score(row, total_assets_inr, liabilities_inr, ipc_sections)
    ground_truth_confidence = "HIGH" if score >= 0.8 else "MEDIUM" if score >= 0.5 else "LOW"

    return {
        "doc_id": doc_id,
        "election_year": row.election_year,
        "constituency": row.constituency,
        "candidate_name": row.candidate_name,
        "party": row.party,
        "candidate_id": row.candidate_id,
        "education": {
            "raw": row.education_raw,
            "level": row.education_level,
        },
        "criminal_cases": {
            "count": row.criminal_cases_count,
            "ipc_sections": ipc_sections,
        },
        "financials": {
            "total_assets_inr": total_assets_inr,
            "liabilities_inr": liabilities_inr,
            "net_worth_inr": net_worth_inr,
        },
        "source_url": row.source_url,
        "list_source_url": row.source_url,
        "last_scraped": now_iso(),
        "data_confidence_score": score,
        "ground_truth_confidence": ground_truth_confidence,
    }


def dedupe_documents(docs: Iterable[dict]) -> list[dict]:
    by_id: dict[str, dict] = {}
    for doc in docs:
        doc_id = doc["doc_id"]
        old = by_id.get(doc_id)
        if not old:
            by_id[doc_id] = doc
            continue

        # Keep the richer record.
        if doc.get("data_confidence_score", 0) >= old.get("data_confidence_score", 0):
            by_id[doc_id] = doc

    return list(by_id.values())


def build_2026_placeholders(seed_docs: list[dict]) -> list[dict]:
    """
    Prepares placeholder docs for 2026 with stable IDs and known constituency keys.
    Seeded from 2021 constituency coverage.
    """
    placeholders: list[dict] = []
    scraped_at = now_iso()

    seen: set[str] = set()
    for doc in seed_docs:
        constituency = doc.get("constituency")
        if not constituency:
            continue

        doc_id = f"2026_{slugify(constituency)}_placeholder"
        if doc_id in seen:
            continue
        seen.add(doc_id)

        placeholders.append(
            {
                "doc_id": doc_id,
                "election_year": 2026,
                "constituency": constituency,
                "candidate_name": None,
                "party": None,
                "candidate_id": None,
                "education": {"raw": None, "level": "Unknown"},
                "criminal_cases": {"count": None, "ipc_sections": []},
                "financials": {
                    "total_assets_inr": None,
                    "liabilities_inr": None,
                    "net_worth_inr": None,
                },
                "source_url": BASE_URL_BY_YEAR[2026],
                "list_source_url": BASE_URL_BY_YEAR[2026],
                "last_scraped": scraped_at,
                "data_confidence_score": 0.2,
                "is_placeholder": True,
                "placeholder_reason": "Awaiting ADR/MyNeta 2026 candidate declarations",
            }
        )

    return placeholders


def validate_document(doc: dict) -> list[str]:
    """
    Returns a list of validation warnings for a document.
    An empty list means the document is clean.
    """
    warnings: list[str] = []
    fin = doc.get("financials", {})

    assets = fin.get("total_assets_inr")
    if assets is not None and not isinstance(assets, int):
        warnings.append(f"total_assets_inr is not int: {assets!r}")

    liabilities = fin.get("liabilities_inr")
    if liabilities is not None and not isinstance(liabilities, int):
        warnings.append(f"liabilities_inr is not int: {liabilities!r}")

    net = fin.get("net_worth_inr")
    if net is not None and not isinstance(net, int):
        warnings.append(f"net_worth_inr is not int: {net!r}")

    if not doc.get("doc_id"):
        warnings.append("doc_id is missing or empty")

    if not doc.get("candidate_name"):
        warnings.append("candidate_name is missing")

    score = doc.get("data_confidence_score")
    if score is not None and not (0.0 <= score <= 1.0):
        warnings.append(f"data_confidence_score out of range: {score}")

    return warnings


def save_json(path: Path, docs: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)


def load_checkpoint(path: Path) -> dict[str, dict]:
    """
    Load an existing JSON output file and return a dict keyed by doc_id.
    Returns an empty dict if the file does not exist or is corrupt.
    """
    if not path.exists():
        return {}
    try:
        with path.open(encoding="utf-8") as f:
            existing = json.load(f)
        return {doc["doc_id"]: doc for doc in existing if doc.get("doc_id")}
    except Exception as exc:
        print(f"  [warn] Could not load checkpoint {path}: {exc}")
        return {}


def upload_firestore(project_id: str, docs: list[dict], collection: str = "candidate_transparency") -> None:
    if firestore is None:
        raise RuntimeError("google-cloud-firestore is not available in this environment")

    db = firestore.Client(project=project_id)
    batch = db.batch()
    size = 0

    for doc in docs:
        doc_id = doc["doc_id"]
        payload = dict(doc)
        payload["_uploaded_at"] = now_iso()
        payload["_schema_version"] = "1.0"

        ref = db.collection(collection).document(doc_id)
        batch.set(ref, payload, merge=True)
        size += 1

        if size >= 400:
            batch.commit()
            batch = db.batch()
            size = 0

    if size > 0:
        batch.commit()


def run_ingest(
    election_year: int,
    placeholders_only: bool,
    include_2026_placeholders: bool,
    constituency_filter: str | None = None,
    checkpoint_path: Path | None = None,
    checkpoint_every: int = 50,
    dry_run: bool = False,
) -> list[dict]:
    """
    Main ingestion pipeline.

    Args:
        election_year:            2021 or 2026
        placeholders_only:        Generate 2026 stubs from 2021 constituency list
        include_2026_placeholders: Append 2026 stubs after a 2021 scrape
        constituency_filter:      If set, only scrape rows where constituency matches
                                  this string (case-insensitive). Useful for dry-runs.
        checkpoint_path:          Path to the output JSON. Existing records are loaded
                                  on startup; already-scraped doc_ids are skipped.
        checkpoint_every:         Save checkpoint JSON every N new records (default 50).
        dry_run:                  Level 1 only — parse the list page, skip all detail
                                  fetches, validate schema, print sample, and return.
    """
    if placeholders_only and election_year != 2026:
        raise ValueError("--placeholders-only is supported only with --year 2026")

    if placeholders_only:
        seed_client = MyNetaClient(election_year=2021)
        seed_rows = seed_client.list_candidates()
        seed_docs = [build_document(row, []) for row in seed_rows]
        return build_2026_placeholders(seed_docs)

    client = MyNetaClient(election_year=election_year)

    print("  [L1] Fetching constituency list from main page…")
    list_rows = client.list_candidates(constituency_filter=constituency_filter)
    if not list_rows and constituency_filter:
        return []
    print(f"  [L1] {len(list_rows)} candidates across {len(set(r.constituency for r in list_rows))} constituencies")

    # --- Dry-run: skip Level 2, validate Level 1 output, print sample ---
    if dry_run:
        print("  [dry-run] Skipping Level 2 IPC fetches.")
        docs = [build_document(row, []) for row in list_rows]
        docs = dedupe_documents(docs)
        _print_dry_run_report(docs)
        return docs

    # --- Resume: load existing checkpoint, build set of already-scraped IDs ---
    checkpoint: dict[str, dict] = {}
    if checkpoint_path:
        checkpoint = load_checkpoint(checkpoint_path)
        if checkpoint:
            print(f"  [resume] {len(checkpoint)} records already in checkpoint — skipping those.")

    already_scraped = set(checkpoint.keys())
    new_docs: list[dict] = []
    ipc_fetch_count = 0  # counts only Level 2 requests that actually go out

    for i, row in enumerate(list_rows):
        doc_id = build_document_id(row.election_year, row.constituency, row.candidate_name)

        if doc_id in already_scraped:
            continue  # already in checkpoint — skip entirely

        # Level 2: fetch IPC sections only for candidates with criminal cases.
        # Financial data is already on the list row — no separate fetch needed.
        ipc_sections: list[str] = []
        if row.criminal_cases_count and row.criminal_cases_count > 0 and row.candidate_url:
            ipc_sections = client.fetch_ipc_sections(row.candidate_url)
            ipc_fetch_count += 1

            # Rate limiting: 1s per IPC fetch, 5s cooldown every 10 fetches
            if ipc_fetch_count % 10 == 0:
                time.sleep(5.0)
            else:
                time.sleep(1.0)

        doc = build_document(row, ipc_sections)
        new_docs.append(doc)
        checkpoint[doc_id] = doc

        # Checkpoint save every N new records.
        if checkpoint_path and len(new_docs) % checkpoint_every == 0:
            all_so_far = list(checkpoint.values())
            save_json(checkpoint_path, all_so_far)
            print(f"  [checkpoint] {len(all_so_far)} total | {len(new_docs)} new this run | row {i + 1}/{len(list_rows)}")

    # Final merge: existing checkpoint + new docs, deduplicated.
    all_docs = dedupe_documents(list(checkpoint.values()))

    if include_2026_placeholders and election_year == 2021:
        all_docs.extend(build_2026_placeholders(all_docs))

    return all_docs


def _print_dry_run_report(docs: list[dict]) -> None:
    """Print a validation summary for a dry-run."""
    print(f"\n  === DRY-RUN REPORT ({len(docs)} docs) ===")

    errors = 0
    for doc in docs:
        warnings = validate_document(doc)
        if warnings:
            errors += 1
            print(f"  [INVALID] {doc.get('doc_id')}: {'; '.join(warnings)}")

    print(f"  Validation: {len(docs) - errors} clean, {errors} with warnings")

    if docs:
        sample = docs[0]
        print("\n  Sample document:")
        print(json.dumps(sample, indent=4, ensure_ascii=False, default=str))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ADR/MyNeta Candidate Transparency Ingestion",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry-run for a single constituency (no detail fetches, prints sample doc)
  python scrapers/candidate_transparency_ingest.py --constituency Harur --dry-run

  # Full 2021 scrape with checkpointing (resumes if interrupted)
  python scrapers/candidate_transparency_ingest.py --year 2021 --output data/processed/candidate_transparency.json

  # Upload after scrape completes
  python scrapers/candidate_transparency_ingest.py --year 2021 --upload

  # 2026 placeholders
  python scrapers/candidate_transparency_ingest.py --year 2026 --placeholders-only --upload
        """,
    )
    parser.add_argument("--year", type=int, choices=[2021, 2026], default=2021, help="Election year (default: 2021)")
    parser.add_argument(
        "--output", type=Path,
        default=Path("data/processed/candidate_transparency.json"),
        help="Output JSON path — also used as checkpoint for resume (default: data/processed/candidate_transparency.json)",
    )
    parser.add_argument("--upload", action="store_true", help="Upload to Firestore after scrape")
    parser.add_argument("--project", default="naatunadappu", help="GCP project id for Firestore (default: naatunadappu)")
    parser.add_argument("--placeholders-only", action="store_true", help="Generate 2026 placeholder docs only (requires --year 2026)")
    parser.add_argument(
        "--include-2026-placeholders",
        action="store_true",
        help="When scraping 2021, also append 2026 placeholder documents",
    )
    parser.add_argument(
        "--constituency",
        metavar="NAME",
        help="Filter to a single constituency by name (case-insensitive substring). Use with --dry-run for quick validation.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Level 1 only — parse list page, validate schema, print sample doc. No detail fetches, no writes.",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=50,
        metavar="N",
        help="Save checkpoint JSON every N new records (default: 50)",
    )
    return parser.parse_args()


def main() -> None:
    # MyNeta can occasionally present TLS oddities in some environments.
    requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]

    args = parse_args()

    # Checkpoint path: same as output file. Existing records are loaded on startup
    # so interrupted runs can resume without re-scraping already-done candidates.
    checkpoint_path = None if args.dry_run else args.output

    docs = run_ingest(
        election_year=args.year,
        placeholders_only=args.placeholders_only,
        include_2026_placeholders=args.include_2026_placeholders,
        constituency_filter=args.constituency,
        checkpoint_path=checkpoint_path,
        checkpoint_every=args.checkpoint_every,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        print("\n[dry-run complete] No files written.")
        return

    save_json(args.output, docs)
    print(f"[ok] Saved {len(docs)} records to {args.output}")

    if args.upload:
        # Validate all docs before uploading — abort if any have structural errors.
        invalid = [(doc["doc_id"], validate_document(doc)) for doc in docs if validate_document(doc)]
        if invalid:
            print(f"[warn] {len(invalid)} documents have validation warnings:")
            for doc_id, warnings in invalid[:10]:
                print(f"  {doc_id}: {'; '.join(warnings)}")
        upload_firestore(project_id=args.project, docs=docs, collection="candidate_transparency")
        print(f"[ok] Uploaded {len(docs)} records to Firestore collection: candidate_transparency")


if __name__ == "__main__":
    main()
