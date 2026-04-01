import hashlib
import ssl
import time
from pathlib import Path

import requests
import urllib3
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from tenacity import retry, stop_after_attempt, wait_exponential
from urllib3.util.ssl_ import create_urllib3_context

RAW_DIR = Path("data/raw/ceo_tn")
RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://elections.tn.gov.in"
STAT_REPORTS_PATH = "/statistical-reports"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; NaatuNadappuResearchBot/1.0)"
    )
}

ELECTION_YEARS = [
    1952, 1957, 1962, 1967, 1971, 1977, 1980, 1984,
    1989, 1991, 1996, 2001, 2006, 2011, 2016, 2021
]


class _LegacySSLAdapter(HTTPAdapter):
    """
    Adapter that allows TLS 1.0/1.1 and weak ciphers.
    Required for Indian government sites (elections.tn.gov.in) that use
    non-standard TLS configurations and drop connections on strict handshakes.
    """
    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        kwargs["ssl_context"] = ctx
        super().init_poolmanager(*args, **kwargs)


def _make_session() -> requests.Session:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    session = requests.Session()
    session.mount("https://", _LegacySSLAdapter())
    return session


_SESSION = _make_session()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
def _get(url: str) -> requests.Response:
    resp = _SESSION.get(url, headers=HEADERS, timeout=30, verify=False)
    resp.raise_for_status()
    return resp


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch_pdf_links() -> dict[int, str]:
    """
    Scrape the CEO TN statistical reports page and return
    a mapping of {election_year: pdf_url}.
    """
    resp = _get(f"{BASE_URL}{STAT_REPORTS_PATH}")
    soup = BeautifulSoup(resp.text, "lxml")

    year_to_url: dict[int, str] = {}

    for link in soup.find_all("a", href=True):
        href = link["href"]
        text = link.get_text(strip=True)

        for year in ELECTION_YEARS:
            if str(year) in text or str(year) in href:
                pdf_url = href if href.startswith("http") else f"{BASE_URL}/{href.lstrip('/')}"
                year_to_url.setdefault(year, pdf_url)
                break

    return year_to_url


def download_pdf(year: int, url: str) -> tuple[Path, str]:
    """Download PDF and return (local_path, sha256_checksum)."""
    dest = RAW_DIR / f"tn_election_{year}.pdf"

    if dest.exists():
        print(f"  [cache] {dest.name}")
        return dest, sha256_file(dest)

    print(f"  [fetch] {year} → {url}")
    resp = _get(url)

    with open(dest, "wb") as f:
        f.write(resp.content)

    checksum = sha256_file(dest)
    time.sleep(1.5)
    return dest, checksum


def extract_party_results_from_pdf(pdf_path: Path, year: int) -> list[dict]:
    """
    Parse CEO TN statistical PDF to extract party-wise results.
    Uses tabula-py for table extraction; falls back to text parsing.
    """
    try:
        import tabula
        tables = tabula.read_pdf(str(pdf_path), pages="all", multiple_tables=True, silent=True)
        for df in tables:
            cols = [str(c).lower() for c in df.columns]
            if any("party" in c for c in cols) and any("won" in c for c in cols):
                return _parse_tabula_df(df, year)
    except Exception:
        pass

    # Text-based fallback via pdfminer if available
    try:
        from pdfminer.high_level import extract_text
        text = extract_text(str(pdf_path))
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        return _parse_party_table_lines(lines, year)
    except Exception:
        pass

    return []


def _parse_tabula_df(df, year: int) -> list[dict]:
    import pandas as pd
    results = []
    col_map = {}
    for col in df.columns:
        low = str(col).lower()
        if "party" in low or "name" in low:
            col_map["party"] = col
        elif "won" in low:
            col_map["won"] = col
        elif "contest" in low:
            col_map["contested"] = col
        elif "vote" in low and "share" not in low and "%" not in low:
            col_map["votes"] = col
        elif "share" in low or "%" in low:
            col_map["share"] = col

    if "party" not in col_map or "won" not in col_map:
        return results

    for _, row in df.iterrows():
        try:
            results.append({
                "party_name": str(row[col_map["party"]]).strip(),
                "seats_contested": int(str(row.get(col_map.get("contested", ""), 0)).replace(",", "") or 0),
                "seats_won": int(str(row[col_map["won"]]).replace(",", "") or 0),
                "votes": int(str(row.get(col_map.get("votes", ""), 0)).replace(",", "") or 0),
                "vote_share_pct": float(str(row.get(col_map.get("share", ""), 0.0)).replace("%", "") or 0.0),
                "year": year,
            })
        except (ValueError, TypeError):
            continue
    return results


def _parse_party_table_lines(lines: list[str], year: int) -> list[dict]:
    results = []
    in_table = False

    for line in lines:
        if ("Party Name" in line or "Name of Party" in line) and (
            "Seats Won" in line or "Won" in line
        ):
            in_table = True
            continue

        if not in_table:
            continue

        if any(marker in line for marker in ["Total", "Grand Total", "Page", "Note"]):
            continue

        parts = line.split()
        if len(parts) >= 4:
            try:
                vote_share = float(parts[-1].replace("%", ""))
                votes = int(parts[-2].replace(",", ""))
                seats_won = int(parts[-3])
                seats_contested = int(parts[-4])
                party_name = " ".join(parts[:-4])

                if party_name and seats_contested > 0:
                    results.append({
                        "party_name": party_name,
                        "seats_contested": seats_contested,
                        "seats_won": seats_won,
                        "votes": votes,
                        "vote_share_pct": vote_share,
                        "year": year,
                    })
            except (ValueError, IndexError):
                continue

    return results


def run_ceo_tn_scraper() -> list[dict]:
    print("[CEO TN] Fetching PDF link index...")
    try:
        year_to_url = fetch_pdf_links()
    except Exception as e:
        print(f"  [warn] CEO TN site unreachable (server-side TLS issue): {e}")
        print("  [info] elections.tn.gov.in has a broken TLS config — falling back to curated data.")
        return []
    print(f"  Found links for years: {sorted(year_to_url.keys())}")

    all_records: list[dict] = []

    for year in ELECTION_YEARS:
        url = year_to_url.get(year)
        if not url:
            print(f"  [warn] No PDF found for {year}, skipping.")
            continue

        pdf_path, checksum = download_pdf(year, url)
        party_results = extract_party_results_from_pdf(pdf_path, year)

        for record in party_results:
            record["source_url"] = url
            record["pdf_checksum"] = checksum

        print(f"  [ok] {year}: {len(party_results)} party records")
        all_records.extend(party_results)

    return all_records
