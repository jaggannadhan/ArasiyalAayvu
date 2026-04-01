"""
MyNeta / ADR — Tamil Nadu 2021 Assembly Election MLA Data
Source: myneta.info/tamilnadu2021/ (Association for Democratic Reforms)

Status: WORKING — standard HTTPS, full table data accessible.

Data available per winner:
  - Name, Constituency, Party
  - Criminal cases (count)
  - Education level
  - Total assets (Rs)
  - Liabilities (Rs)

Summary stats confirmed from site:
  - 224 winners analyzed
  - 134 (60%) with declared criminal cases
  - 58 (26%) with declared serious criminal cases
  - 192 (86%) Crorepati winners
  - 142 (63%) graduates or above
  - Average assets: Rs 12.52 crore per winner
"""

import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

BASE_URL = "https://myneta.info/tamilnadu2021"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; NaatuNadappuResearchBot/1.0)"}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=8))
def _get(url: str) -> requests.Response:
    resp = requests.get(url, headers=HEADERS, timeout=20, verify=False)
    resp.raise_for_status()
    return resp


def _parse_amount(raw: str) -> float | None:
    """Convert MyNeta asset string like 'Rs 1,34,59,578~ 1 Crore+' to float (in crore)."""
    if not raw:
        return None
    # Try to extract the numeric value directly
    # Format: "Rs 1,34,59,578~ 1 Crore+" or "Rs 6,10,000~ 6 Lacs+"
    m = re.search(r"Rs\s*([\d,]+)", raw)
    if m:
        val = float(m.group(1).replace(",", ""))
        return round(val / 1e7, 4)  # convert to crore
    return None


def _parse_liabilities(raw: str) -> float | None:
    if not raw:
        return None
    m = re.search(r"Rs\s*([\d,]+)", raw)
    if m:
        val = float(m.group(1).replace(",", ""))
        return round(val / 1e7, 4)
    return None


def _clean_cases(raw: str) -> int:
    """Extract integer criminal case count."""
    if not raw:
        return 0
    try:
        return int(raw.strip())
    except ValueError:
        return 0


def scrape_summary_stats() -> dict:
    """Scrape the summary highlights from MyNeta TN 2021."""
    resp = _get(f"{BASE_URL}/index.php?action=summary")
    soup = BeautifulSoup(resp.text, "lxml")

    stats = {
        "total_winners_analyzed": None,
        "winners_with_criminal_cases": None,
        "winners_with_criminal_cases_pct": None,
        "winners_with_serious_criminal_cases": None,
        "winners_with_serious_criminal_cases_pct": None,
        "crorepati_winners": None,
        "crorepati_winners_pct": None,
        "graduate_or_above": None,
        "graduate_or_above_pct": None,
    }

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows:
            cols = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if len(cols) >= 2:
                label, value = cols[0].lower(), cols[1]
                if "total winners" in label:
                    stats["total_winners_analyzed"] = int(re.sub(r"\D", "", value) or 0)
                elif "winners with declared criminal cases" in label and "serious" not in label:
                    m = re.search(r"(\d+)\s*\((\d+)%\)", value)
                    if m:
                        stats["winners_with_criminal_cases"] = int(m.group(1))
                        stats["winners_with_criminal_cases_pct"] = int(m.group(2))
                elif "serious criminal" in label:
                    m = re.search(r"(\d+)\s*\((\d+)%\)", value)
                    if m:
                        stats["winners_with_serious_criminal_cases"] = int(m.group(1))
                        stats["winners_with_serious_criminal_cases_pct"] = int(m.group(2))
                elif "crorepati" in label:
                    m = re.search(r"(\d+)\s*\((\d+)%\)", value)
                    if m:
                        stats["crorepati_winners"] = int(m.group(1))
                        stats["crorepati_winners_pct"] = int(m.group(2))
                elif "graduate" in label:
                    m = re.search(r"(\d+)\s*\((\d+)%\)", value)
                    if m:
                        stats["graduate_or_above"] = int(m.group(1))
                        stats["graduate_or_above_pct"] = int(m.group(2))

    return stats


def scrape_winners() -> list[dict]:
    """
    Scrape all 224 winners from MyNeta TN 2021.
    Returns list of MLA dicts with criminal, financial, and education data.
    """
    resp = _get(f"{BASE_URL}/index.php?action=show_winners&sort=default")
    soup = BeautifulSoup(resp.text, "lxml")

    winners = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 10:
            continue

        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]
        if "candidate" not in " ".join(headers) and "constituency" not in " ".join(headers):
            continue

        for row in rows[1:]:
            cols = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cols) < 5:
                continue

            # Column order: Sno | Candidate | Constituency | Party | Criminal Case | Education | Total Assets | Liabilities
            try:
                winner = {
                    "mla_name": cols[1] if len(cols) > 1 else "",
                    "constituency": cols[2] if len(cols) > 2 else "",
                    "party": cols[3] if len(cols) > 3 else "",
                    "criminal_cases_total": _clean_cases(cols[4]) if len(cols) > 4 else 0,
                    "education": cols[5] if len(cols) > 5 else "",
                    "assets_cr": _parse_amount(cols[6]) if len(cols) > 6 else None,
                    "liabilities_cr": _parse_liabilities(cols[7]) if len(cols) > 7 else None,
                    "source_url": f"{BASE_URL}/index.php?action=show_winners",
                    "election_year": 2021,
                    "ground_truth_confidence": "HIGH",
                }
                if winner["mla_name"] and winner["constituency"]:
                    winners.append(winner)
            except (IndexError, ValueError):
                continue

        if winners:
            break  # found the right table

    return winners


def _extract_constituency_name(soup) -> str:
    """Extract constituency name from an individual constituency page <h3> heading.
    Format: 'List of Candidates - CONSTITUENCY_NAME:DISTRICT ( Comparison... )'
    """
    for h3 in soup.find_all("h3"):
        txt = h3.get_text(separator=" ", strip=True)
        # Extract the part between "- " and ":" — that is the constituency name
        m = re.search(r"-\s+(.+?):", txt)
        if m:
            return m.group(1).strip()
    return ""


def scrape_constituency_winner(constituency_id: int) -> dict | None:
    """
    Scrape a single constituency page and return the winner's data dict,
    or None if no winner row is found.
    """
    url = f"{BASE_URL}/index.php?action=show_candidates&constituency_id={constituency_id}&sort=default"
    try:
        resp = _get(url)
    except Exception:
        return None

    soup = BeautifulSoup(resp.text, "lxml")
    constituency_name = _extract_constituency_name(soup)

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 3:
            continue
        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]
        if "candidate" not in " ".join(headers):
            continue

        for row in rows[1:]:
            cols = [td.get_text(separator=" ", strip=True) for td in row.find_all("td")]
            if len(cols) < 5:
                continue
            # Winner rows have "Winner" appended to the candidate name by MyNeta
            if "Winner" not in cols[1]:
                continue

            name = cols[1].replace("Winner", "").strip()
            return {
                "mla_name": name,
                "constituency": constituency_name,
                "party": cols[2] if len(cols) > 2 else "",
                "criminal_cases_total": _clean_cases(cols[3]) if len(cols) > 3 else 0,
                "education": cols[4] if len(cols) > 4 else "",
                "assets_cr": _parse_amount(cols[6]) if len(cols) > 6 else None,
                "liabilities_cr": _parse_liabilities(cols[7]) if len(cols) > 7 else None,
                "source_url": url,
                "election_year": 2021,
                "ground_truth_confidence": "HIGH",
            }
    return None


def scrape_missing_via_constituencies(
    known_constituencies: set[str],
    total_constituencies: int = 234,
) -> list[dict]:
    """
    Iterate constituency_id=1..total_constituencies, scrape winner for any
    constituency not already in known_constituencies.
    Returns list of newly found winner dicts.
    """
    found = []
    for cid in range(1, total_constituencies + 1):
        winner = scrape_constituency_winner(cid)
        if winner is None:
            continue
        c_name = winner["constituency"].upper().strip()
        if c_name and c_name not in known_constituencies:
            found.append(winner)
            known_constituencies.add(c_name)
        time.sleep(0.5)
    return found


def run_myneta_scraper() -> tuple[list[dict], dict]:
    """
    Full pipeline: scrape summary stats + all winners.
    Falls back to per-constituency scraping to recover rows the main
    winners table omits (MyNeta skips every 9th row due to ad injection).
    Returns (winners_list, summary_stats_dict).
    """
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    print("[MyNeta] Scraping TN 2021 summary stats...")
    stats = scrape_summary_stats()
    print(f"  [ok] Summary: {stats.get('total_winners_analyzed')} winners, "
          f"{stats.get('winners_with_criminal_cases_pct')}% criminal cases, "
          f"{stats.get('crorepati_winners_pct')}% crorepatis")

    print("[MyNeta] Scraping all winners (main table)...")
    winners = scrape_winners()
    print(f"  [ok] {len(winners)} winner records from main table")

    # Detect gap and fill via per-constituency pages
    expected = stats.get("total_winners_analyzed") or 0
    if len(winners) < expected:
        gap = expected - len(winners)
        print(f"  [warn] {gap} winners missing from main table — scraping constituency pages...")
        known = {w["constituency"].upper().strip() for w in winners}
        extra = scrape_missing_via_constituencies(known)
        winners.extend(extra)
        print(f"  [ok] {len(extra)} additional winners recovered via constituency pages")

    print(f"  [total] {len(winners)} winner records")

    # Compute derived stats
    if winners:
        assets = [w["assets_cr"] for w in winners if w["assets_cr"] is not None]
        stats["avg_assets_cr"] = round(sum(assets) / len(assets), 2) if assets else None
        stats["max_assets_cr"] = round(max(assets), 2) if assets else None
        stats["zero_criminal_cases"] = sum(1 for w in winners if w["criminal_cases_total"] == 0)
        stats["source_url"] = f"{BASE_URL}/index.php?action=summary"
        stats["election_year"] = 2021
        stats["ground_truth_confidence"] = "HIGH"

    time.sleep(1.0)
    return winners, stats
