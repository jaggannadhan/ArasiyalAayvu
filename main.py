"""
Naatu Nadappu — Tamil Nadu Election Awareness Data Pipeline
Target: Firestore project 'naatunadappu' (asia-south1)

Tasks:
    all            — full political history pipeline (scrape + transform + upload)
    scrape         — attempt live scraping (falls back to curated on TN govt SSL failures)
    transform      — process raw JSON into Firestore-ready docs
    upload         — push political history data to Firestore
    static         — upload only curated static data (parties, leaders, achievements)
    finance        — full finance pipeline: download PRS PDFs + parse + upload
    manual-pdf     — process a locally downloaded TN Budget PDF into Firestore
    socio          — scrape ASER 2024, merge into curated socio_economics, upload
    accountability — scrape MyNeta TN 2021, transform, upload candidate_accountability
    awareness      — run both socio + accountability pipelines
    manifesto      — upload curated manifesto_promises seed data to Firestore
"""

import argparse
import json
import sys
from pathlib import Path

from scrapers.assembly_scraper import scrape_chief_ministers, _curated_chief_ministers
from scrapers.ceo_tn_scraper import run_ceo_tn_scraper
from scrapers.eci_scraper import scrape_eci_party_recognition
from transformers.election_transformer import (
    build_alliance_matrix,
    save_processed,
    transform_ceo_records,
)
from loaders.firestore_loader import (
    upload_achievements,
    upload_alliances,
    upload_chief_ministers,
    upload_elections,
    upload_leaders,
    upload_parties,
    upload_state_finances,
    upload_debt_history,
    upload_departmental_spending,
    upload_finance_manual,
    upload_socio_economics,
    upload_mla_winners,
    upload_party_rollups,
    upload_assembly_summary,
    upload_manifesto_promises,
)


# ---------------------------------------------------------------------------
# Political History Pipeline
# ---------------------------------------------------------------------------

def run_scrape() -> None:
    print("\n=== PHASE 1: SCRAPING (Political History) ===")
    Path("data/raw").mkdir(parents=True, exist_ok=True)

    raw_ceo = run_ceo_tn_scraper()
    with open("data/raw/ceo_tn_raw.json", "w") as f:
        json.dump(raw_ceo, f, indent=2)
    print(f"  [ok] {len(raw_ceo)} CEO TN party-year records")

    cms = scrape_chief_ministers()
    with open("data/raw/chief_ministers.json", "w") as f:
        json.dump(cms, f, indent=2)
    print(f"  [ok] {len(cms)} Chief Minister records")

    eci_parties = scrape_eci_party_recognition()
    with open("data/raw/eci_parties.json", "w") as f:
        json.dump(eci_parties, f, indent=2)
    print(f"  [ok] {len(eci_parties)} ECI party recognition records")


def run_transform() -> None:
    print("\n=== PHASE 2: TRANSFORMING (Political History) ===")

    ceo_raw_path = Path("data/raw/ceo_tn_raw.json")
    if not ceo_raw_path.exists():
        print("  [warn] No CEO TN raw data. Run --task scrape first.")
        elections = {}
    else:
        with open(ceo_raw_path) as f:
            raw_ceo = json.load(f)
        elections = transform_ceo_records(raw_ceo)
        print(f"  [ok] Transformed {len(elections)} election years from PDF data")

    alliance_matrix = build_alliance_matrix(elections)

    for year, alliances in alliance_matrix.items():
        if year in elections:
            elections[year]["alliance_composition"] = alliances
        else:
            elections[year] = {
                "year": year,
                "total_seats": 234,
                "majority_mark": 118,
                "party_results": [],
                "alliance_composition": alliances,
                "source_url": "https://www.assembly.tn.gov.in (curated)",
                "pdf_checksum": None,
                "ground_truth_confidence": "HIGH",
            }

    save_processed(elections, "data/processed/elections.json")
    save_processed(
        {str(k): v for k, v in alliance_matrix.items()},
        "data/processed/alliances.json",
    )
    print(f"  [ok] {len(elections)} election records | {sum(len(v) for v in alliance_matrix.values())} alliance records")


def run_upload() -> None:
    print("\n=== PHASE 3: UPLOADING (Political History) ===")

    for path, loader, key_transform in [
        ("data/processed/elections.json",  upload_elections,  lambda d: {int(k): v for k, v in d.items()}),
        ("data/processed/alliances.json",  upload_alliances,  lambda d: {int(k): v for k, v in d.items()}),
    ]:
        p = Path(path)
        if p.exists():
            with open(p) as f:
                data = key_transform(json.load(f))
            loader(data)
        else:
            print(f"  [skip] {path} not found")

    run_static_upload()


def run_static_upload() -> None:
    print("\n=== STATIC DATA UPLOAD ===")

    with open("data/processed/parties.json") as f:
        upload_parties(json.load(f))

    with open("data/processed/leaders.json") as f:
        upload_leaders(json.load(f))

    with open("data/processed/achievements.json") as f:
        upload_achievements(json.load(f))

    cms_path = Path("data/raw/chief_ministers.json")
    cms = json.load(open(cms_path)) if cms_path.exists() else _curated_chief_ministers()
    upload_chief_ministers(cms)

    print("\n[done] Political history uploaded to Firestore.")
    print("       Project: naatunadappu | DB: (default) | Region: asia-south1")


# ---------------------------------------------------------------------------
# Finance Pipeline
# ---------------------------------------------------------------------------

def run_finance(years: list[str] = None) -> None:
    print("\n=== FINANCE PIPELINE ===")

    from scrapers.prs_scraper import run_prs_scraper
    from transformers.finance_transformer import (
        transform_prs_docs,
        build_debt_history_series,
        build_departmental_spending,
        save_processed as save_fin,
    )

    print("\n-- Step 1: Download + parse PRS India PDFs --")
    raw_docs = run_prs_scraper(years)
    print(f"  [ok] {len(raw_docs)} PRS budget documents parsed")

    print("\n-- Step 2: Transform (inject debt_why + viz_metrics) --")
    finance_docs = transform_prs_docs(raw_docs)
    debt_docs    = build_debt_history_series(raw_docs)
    dept_docs    = build_departmental_spending(raw_docs)

    save_fin(finance_docs, "data/processed/state_finances.json")
    save_fin(debt_docs,    "data/processed/debt_history.json")
    save_fin(dept_docs,    "data/processed/departmental_spending.json")

    print(f"  [ok] {len(finance_docs)} finance summaries")
    print(f"  [ok] {len(debt_docs)} debt history records")
    print(f"  [ok] {len(dept_docs)} departmental spending records")

    print("\n-- Step 3: Upload to Firestore --")
    upload_state_finances(finance_docs)
    upload_debt_history(debt_docs)
    upload_departmental_spending(dept_docs)

    print("\n[done] Finance data uploaded to Firestore.")
    print("       Collections: state_finances, debt_history, departmental_spending")


# ---------------------------------------------------------------------------
# Socio-Economics Pipeline
# ---------------------------------------------------------------------------

def run_socio() -> None:
    print("\n=== SOCIO-ECONOMICS PIPELINE ===")

    from scrapers.aser_scraper import run_aser_scraper
    from transformers.socio_transformer import (
        merge_aser_into_socio,
        add_aser_enrollment_metrics,
        save_processed as save_socio,
    )

    print("\n-- Step 1: Fetch ASER 2024 TN data --")
    aser_data = run_aser_scraper()
    print(f"  [ok] {len(aser_data)} ASER fields extracted")

    print("\n-- Step 2: Load curated socio_economics base --")
    socio_path = Path("data/processed/socio_economics.json")
    with open(socio_path) as f:
        socio_docs = json.load(f)
    print(f"  [ok] {len(socio_docs)} curated metrics loaded")

    print("\n-- Step 3: Merge ASER scraped values into curated docs --")
    socio_docs = merge_aser_into_socio(aser_data, socio_docs)
    extra_docs = add_aser_enrollment_metrics(aser_data)
    socio_docs.extend(extra_docs)
    print(f"  [ok] {len(extra_docs)} extra ASER enrollment docs added")
    print(f"  [ok] Total: {len(socio_docs)} socio_economics documents")

    save_socio(socio_docs, "data/processed/socio_economics_final.json")

    print("\n-- Step 4: Upload to Firestore --")
    upload_socio_economics(socio_docs)

    print("\n[done] Socio-economics uploaded to Firestore.")
    print("       Collection: socio_economics")


# ---------------------------------------------------------------------------
# Accountability Pipeline
# ---------------------------------------------------------------------------

def run_accountability() -> None:
    print("\n=== CANDIDATE ACCOUNTABILITY PIPELINE ===")

    from scrapers.myneta_scraper import run_myneta_scraper
    from transformers.accountability_transformer import (
        enrich_winner,
        build_party_rollups,
        build_assembly_summary,
        save_processed as save_acc,
    )

    print("\n-- Step 1: Scrape MyNeta TN 2021 winners --")
    winners, stats = run_myneta_scraper()
    print(f"  [ok] {len(winners)} MLA winner records, summary stats fetched")

    print("\n-- Step 2: Enrich each winner record --")
    enriched = [enrich_winner(w) for w in winners]
    print(f"  [ok] {len(enriched)} winners enriched (severity, assets, education tier)")

    print("\n-- Step 3: Build party rollups --")
    party_rollups = build_party_rollups(enriched)
    print(f"  [ok] {len(party_rollups)} party accountability scorecards")

    print("\n-- Step 4: Build assembly summary --")
    assembly_summary = build_assembly_summary(enriched, stats)

    save_acc(enriched,        "data/processed/mla_winners.json")
    save_acc(party_rollups,   "data/processed/party_accountability.json")
    save_acc(assembly_summary,"data/processed/assembly_accountability_summary.json")

    print("\n-- Step 5: Upload to Firestore --")
    upload_mla_winners(enriched)
    upload_party_rollups(party_rollups)
    upload_assembly_summary(assembly_summary)

    print("\n[done] Accountability data uploaded to Firestore.")
    print("       Collections: candidate_accountability, party_accountability")


def run_manifesto() -> None:
    print("\n=== MANIFESTO TRACKER PIPELINE ===")
    seed_path = Path("data/processed/manifesto_promises_seed.json")
    with open(seed_path) as f:
        promises = json.load(f)
    print(f"  [ok] {len(promises)} atomic promise records loaded from seed")
    upload_manifesto_promises(promises)
    print(f"\n[done] Manifesto promises uploaded to Firestore.")
    print("       Collection: manifesto_promises")


def run_manual_pdf(pdf_path: str, url: str, year: str, upload: bool) -> None:
    print(f"\n=== MANUAL-PDF UTILITY (year={year}) ===")
    from scrapers.tn_budget_scraper import run_manual_link

    doc = run_manual_link(pdf_path, url, year)
    if doc:
        print(json.dumps(doc, indent=2, default=str))
        if upload:
            upload_finance_manual(doc)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Naatu Nadappu — Tamil Nadu Election Awareness Data Pipeline"
    )
    parser.add_argument(
        "--task",
        choices=[
            "all", "scrape", "transform", "upload", "static",
            "finance", "manual-pdf",
            "socio", "accountability", "awareness",
            "manifesto",
        ],
        default="all",
        help="Pipeline task to run (default: all)",
    )
    parser.add_argument("--years",  nargs="+", help="Finance years to process (e.g. 2025-26 2024-25)")
    parser.add_argument("--pdf",    help="[manual-pdf] Local path to a TN Budget PDF")
    parser.add_argument("--url",    help="[manual-pdf] Direct URL to a TN Budget PDF")
    parser.add_argument("--year",   help="[manual-pdf] Fiscal year label (e.g. 2025-26)")
    parser.add_argument("--upload", action="store_true", help="[manual-pdf] Upload to Firestore after parsing")
    args = parser.parse_args()

    if args.task == "static":
        run_static_upload()
    elif args.task == "finance":
        run_finance(args.years)
    elif args.task == "manual-pdf":
        if not args.year:
            parser.error("--year is required for manual-pdf task")
        run_manual_pdf(args.pdf, args.url, args.year, args.upload)
    elif args.task == "socio":
        run_socio()
    elif args.task == "accountability":
        run_accountability()
    elif args.task == "awareness":
        run_socio()
        run_accountability()
    elif args.task == "manifesto":
        run_manifesto()
    else:

        if args.task in ("all", "scrape"):
            run_scrape()
        if args.task in ("all", "transform"):
            run_transform()
        if args.task in ("all", "upload"):
            run_upload()


if __name__ == "__main__":
    main()
