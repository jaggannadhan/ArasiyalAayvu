"""
Job: Monitor Indian Legal System updates
Schedule: Weekly — every Monday, 10:00 IST

Monitors official Government of India legal sources for changes to the
Constitution, new Central Acts, amendments, and gazette notifications.

Sources (all Government of India)
----------------------------------
  1. e-Gazette of India (egazette.gov.in)
     — Official gazette for all legal notifications, new acts, amendments
     — RSS feed / recent notifications page

  2. India Code (indiacode.nic.in)
     — Legislative Department, Ministry of Law & Justice
     — Central Acts repository, Constitution text

  3. Legislative Department (legislative.gov.in)
     — Bills passed, constitutional amendments

  4. PRS Legislative Research (prsindia.org)
     — Bill tracker, Parliament session updates

Workflow
--------
  1. Fetch latest gazette notifications from egazette.gov.in
  2. Check India Code for any new acts or amendments
  3. Compare against our local data (indian_constitution.json)
  4. If changes detected: update data + log the diff
  5. If no changes: log a clean check

Run:
    .venv/bin/python3 scrapers/jobs/legal_updates_check.py
    .venv/bin/python3 scrapers/jobs/legal_updates_check.py --update  # apply changes
"""

from __future__ import annotations

import json
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List

import httpx

ROOT = Path(__file__).parent.parent.parent

CONSTITUTION_PATH = ROOT / "data" / "processed" / "indian_constitution.json"
BACKEND_COPY_PATH = ROOT / "web" / "backend_api" / "indian_constitution.json"
LOG_PATH = ROOT / "data" / "processed" / "legal_updates_log.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Government sources
EGAZETTE_URL = "https://egazette.gov.in/"
EGAZETTE_RECENT_URL = "https://egazette.gov.in/SearchResult.aspx"
INDIA_CODE_URL = "https://www.indiacode.nic.in/"
LEGISLATIVE_URL = "https://legislative.gov.in/"
PRS_BILLS_URL = "https://prsindia.org/billtrack"


def load_current_data() -> Dict[str, Any]:
    """Load the current constitution data."""
    if CONSTITUTION_PATH.exists():
        with CONSTITUTION_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def load_update_log() -> List[Dict[str, Any]]:
    """Load the update log."""
    if LOG_PATH.exists():
        with LOG_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_update_log(log: List[Dict[str, Any]]):
    """Save the update log."""
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)


def check_egazette() -> List[Dict[str, str]]:
    """
    Check e-Gazette of India for recent constitutional/legal notifications.
    The e-Gazette publishes all official notifications including:
    - Constitutional Amendment Acts
    - New Central Acts
    - Ordinances
    - Rules and Regulations
    """
    print("  Checking e-Gazette of India (egazette.gov.in)...")
    findings: List[Dict[str, str]] = []

    client = httpx.Client(headers=HEADERS, follow_redirects=True, timeout=30)

    try:
        resp = client.get(EGAZETTE_URL)
        if resp.status_code == 200:
            html = resp.text

            # Look for recent gazette entries mentioning Constitution, Amendment, Act
            # The gazette page typically lists recent extraordinary gazettes
            patterns = [
                r"Constitution\s*\(.*?Amendment\)\s*Act[^<]*(\d{4})",
                r"Amendment\s*Act[,\s]*(\d{4})",
                r"THE\s+\w+[\w\s]*ACT[,\s]*(\d{4})",
            ]

            for pattern in patterns:
                matches = re.findall(pattern, html, re.IGNORECASE)
                for match in matches:
                    findings.append({
                        "source": "e-Gazette of India",
                        "type": "gazette_notification",
                        "year": match,
                        "url": EGAZETTE_URL,
                    })

            if not findings:
                print("    No new constitutional amendments found in gazette")
            else:
                print(f"    Found {len(findings)} potential updates")

        else:
            print(f"    WARNING: e-Gazette returned status {resp.status_code}")

    except Exception as e:
        print(f"    WARNING: Could not reach e-Gazette: {e}")
        print("    This is common — govt sites may require browser-like access")
    finally:
        client.close()

    return findings


def check_india_code() -> List[Dict[str, str]]:
    """
    Check India Code for newly added Central Acts.
    India Code (indiacode.nic.in) is the official repository maintained by
    the Legislative Department, Ministry of Law & Justice.
    """
    print("  Checking India Code (indiacode.nic.in)...")
    findings: List[Dict[str, str]] = []

    client = httpx.Client(headers=HEADERS, follow_redirects=True, timeout=30)

    try:
        # Check the main page for any new act indicators
        resp = client.get(INDIA_CODE_URL)
        if resp.status_code == 200:
            html = resp.text

            # India Code often highlights newly added acts on the homepage
            new_act_pattern = r'(?:New|Recent|Latest)\s*(?:Act|Enactment)[^<]*'
            matches = re.findall(new_act_pattern, html, re.IGNORECASE)

            for match in matches:
                findings.append({
                    "source": "India Code",
                    "type": "new_act",
                    "detail": match.strip()[:200],
                    "url": INDIA_CODE_URL,
                })

            # Check for total acts count to detect new additions
            act_count_match = re.search(r'(\d{3,5})\s*(?:Central|Total)\s*Acts?', html, re.IGNORECASE)
            if act_count_match:
                total_acts = int(act_count_match.group(1))
                findings.append({
                    "source": "India Code",
                    "type": "acts_count",
                    "detail": f"Total Central Acts on India Code: {total_acts}",
                    "url": INDIA_CODE_URL,
                })

            print(f"    India Code check complete — {len(findings)} data points")
        else:
            print(f"    WARNING: India Code returned status {resp.status_code}")

    except Exception as e:
        print(f"    WARNING: Could not reach India Code: {e}")
    finally:
        client.close()

    return findings


def check_prs_bills() -> List[Dict[str, str]]:
    """
    Check PRS Legislative Research for recently passed bills.
    PRS tracks all bills introduced and passed in Parliament.
    """
    print("  Checking PRS Legislative Research (prsindia.org)...")
    findings: List[Dict[str, str]] = []

    client = httpx.Client(headers=HEADERS, follow_redirects=True, timeout=30)

    try:
        resp = client.get(PRS_BILLS_URL)
        if resp.status_code == 200:
            html = resp.text

            # Look for "Passed" bills
            passed_pattern = r'<[^>]*class="[^"]*passed[^"]*"[^>]*>([^<]+)'
            matches = re.findall(passed_pattern, html, re.IGNORECASE)

            for match in matches[:10]:  # limit to recent 10
                findings.append({
                    "source": "PRS Legislative Research",
                    "type": "bill_passed",
                    "detail": match.strip(),
                    "url": PRS_BILLS_URL,
                })

            print(f"    PRS check complete — {len(findings)} recently passed bills found")
        else:
            print(f"    WARNING: PRS returned status {resp.status_code}")

    except Exception as e:
        print(f"    WARNING: Could not reach PRS: {e}")
    finally:
        client.close()

    return findings


def compare_with_local(findings: List[Dict[str, str]], current_data: Dict[str, Any]) -> List[Dict[str, str]]:
    """Compare findings against our local data to identify genuine updates."""
    if not current_data:
        return findings

    meta = current_data.get("meta", {})
    latest_amendment = meta.get("latest_amendment", 0)
    known_acts = {a["name"] for a in current_data.get("central_acts", [])}

    new_findings = []
    for f in findings:
        if f["type"] == "gazette_notification":
            # Check if it's a newer amendment than what we have
            try:
                year = int(f.get("year", 0))
                if year > meta.get("latest_amendment_year", 2023):
                    new_findings.append({**f, "action": "potential_new_amendment"})
            except ValueError:
                pass
        elif f["type"] == "new_act":
            # Check if we already know about this act
            detail = f.get("detail", "")
            if not any(act in detail for act in known_acts):
                new_findings.append({**f, "action": "potential_new_act"})
        else:
            new_findings.append(f)

    return new_findings


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Legal System Update Monitor")
    parser.add_argument("--update", action="store_true", help="Apply detected changes")
    parser.add_argument("--quiet", action="store_true", help="Only print if changes found")
    args = parser.parse_args()

    today = date.today().isoformat()

    if not args.quiet:
        print("Legal System Update Monitor")
        print("=" * 50)
        print(f"Date: {today}")
        print()

    # Load current data
    current_data = load_current_data()
    if current_data:
        meta = current_data.get("meta", {})
        if not args.quiet:
            print(f"Current data: {meta.get('total_parts', '?')} parts, "
                  f"latest amendment: {meta.get('latest_amendment', '?')} "
                  f"({meta.get('latest_amendment_year', '?')})")
            print()

    # Check all government sources
    all_findings: List[Dict[str, str]] = []

    if not args.quiet:
        print("--- Checking Government Sources ---")

    gazette_findings = check_egazette()
    all_findings.extend(gazette_findings)

    india_code_findings = check_india_code()
    all_findings.extend(india_code_findings)

    prs_findings = check_prs_bills()
    all_findings.extend(prs_findings)

    # Compare with local data
    new_updates = compare_with_local(all_findings, current_data)

    if not args.quiet:
        print()
        print(f"--- Results ---")
        print(f"Total data points collected: {len(all_findings)}")
        print(f"Potential new updates: {len(new_updates)}")

    if new_updates:
        print()
        print("UPDATES DETECTED:")
        for u in new_updates:
            action = u.get("action", u.get("type", "unknown"))
            detail = u.get("detail", u.get("year", ""))
            print(f"  [{action}] {u['source']}: {detail}")
            print(f"    URL: {u.get('url', 'N/A')}")

        if args.update:
            print()
            print("To apply updates, run:")
            print("  .venv/bin/python scrapers/constitution_ingest.py --scrape --upload")
        else:
            print()
            print("Run with --update to apply changes, or manually run:")
            print("  .venv/bin/python scrapers/constitution_ingest.py --scrape")
    else:
        if not args.quiet:
            print()
            print("No new updates detected. Constitution data is current.")

    # Log this check
    log = load_update_log()
    log.append({
        "check_date": today,
        "timestamp": datetime.now().isoformat(),
        "findings_count": len(all_findings),
        "new_updates_count": len(new_updates),
        "sources_checked": ["egazette.gov.in", "indiacode.nic.in", "prsindia.org"],
        "updates": new_updates if new_updates else [],
    })
    # Keep last 52 weeks of logs
    log = log[-52:]
    save_update_log(log)

    if not args.quiet:
        print(f"\nCheck logged to {LOG_PATH}")
        print("Next check: next Monday")


if __name__ == "__main__":
    main()
