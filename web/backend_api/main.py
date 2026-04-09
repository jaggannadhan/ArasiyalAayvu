from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple  # noqa: UP035
from urllib.parse import quote_plus, urlparse, parse_qs, unquote

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from google.api_core.exceptions import GoogleAPICallError, RetryError
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from google.cloud.firestore_v1 import Increment
from pydantic import BaseModel

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "naatunadappu")
API_TITLE = "ArasiyalAayvu Backend API"

ROOT_DIR = Path(__file__).resolve().parents[1]
MAP_PATH = ROOT_DIR / "src" / "lib" / "constituency-map.json"
LS_MAP_PATH = ROOT_DIR / "src" / "lib" / "ls-constituency-map.json"

# True = higher value is better for that metric (used for percentile direction)
METRIC_HIGHER_IS_BETTER: Dict[str, bool] = {
    "nfhs5_women_literacy":                  True,
    "nfhs5_institutional_deliveries":        True,
    "aser2024_std3_reading_recovery":        True,
    "industrial_corridors_district_coverage": True,
    "nfhs5_anaemia_women":                   False,
    "nfhs5_stunting_under5":                 False,
}

# Slug alias map: (election_year, map_slug) -> orphan_doc_slug_in_firestore
# Handles transliteration variants between MyNeta spelling and our constituency-map.json
MLA_SLUG_ALIASES: Dict[Tuple[int, str], str] = {
    (2011, "aruppukottai"):     "aruppukkottai",
    (2011, "bodinayakkanur"):   "bodinayakanur",
    (2011, "gandarvakottai_sc"):"gandharvakottai",
    (2011, "madhavaram"):       "madavaram",
    (2011, "madhuravoyal"):     "maduravoyal",
    (2011, "mettupalayam"):     "mettuppalayam",
    (2011, "palacode"):         "palacodu",
    (2011, "pappireddipatti"):  "pappireddippatti",
    (2011, "paramathivelur"):   "paramathi_velur",
    (2011, "poonamallee_sc"):   "poonmallae",
    (2011, "sholinganallur"):   "shozhinganallur",
    (2011, "sholinghur"):       "sholingur",
    (2011, "thally"):           "thalli",
    (2011, "thiruvaur"):        "thiruvarur",
    (2011, "thoothukudi"):      "thoothukkudi",
    (2011, "tiruppathur"):      "tiruppattur",
    (2011, "vedharanyam"):      "vedaranyam",
    (2011, "vridhachalam"):     "vriddhachalam",
    (2016, "aruppukottai"):     "aruppukkottai",
    (2016, "bodinayakkanur"):   "bodinayakanur",
    (2016, "gandarvakottai_sc"):"gandharvakottai",
    (2016, "madhavaram"):       "madavaram",
    (2016, "madhuravoyal"):     "maduravoyal",
    (2016, "mettupalayam"):     "mettuppalayam",
    (2016, "mudukulathur"):     "mudhukulathur",
    (2016, "palacode"):         "palacodu",
    (2016, "pappireddipatti"):  "pappireddippatti",
    (2016, "paramathivelur"):   "paramathi_velur",
    (2016, "poonamallee_sc"):   "poonmallae",
    (2016, "sholinganallur"):   "shozhinganallur",
    (2016, "sholinghur"):       "sholingur",
    (2016, "thally"):           "thalli",
    (2016, "thiruvaur"):        "thiruvarur",
    (2016, "thoothukudi"):      "thoothukkudi",
    (2016, "vedharanyam"):      "vedaranyam",
    (2016, "vridhachalam"):     "vriddhachalam",
}

# Slug aliases for 2026 candidates collection
# Maps canonical map slug → alternate Firestore doc slug (if scraper used a different spelling)
CANDIDATE_2026_SLUG_ALIASES: Dict[str, str] = {
    "madhuravoyal": "maduravoyal",  # historical typo; canonical map slug is maduravoyal
}

KEY_METRIC_IDS = {
    "aser2024_std3_reading_recovery",
    "nfhs5_women_literacy",
    "nfhs5_stunting_under5",
    "nfhs5_anaemia_women",
    "nfhs5_institutional_deliveries",
    "industrial_corridors_district_coverage",
}
METRIC_ORDER = {
    "aser2024_std3_reading_recovery": 0,
    "nfhs5_women_literacy": 1,
    "nfhs5_anaemia_women": 2,
    "nfhs5_stunting_under5": 3,
    "nfhs5_institutional_deliveries": 4,
    "industrial_corridors_district_coverage": 5,
}
PILLAR_ORDER = {
    "Agriculture": 0,
    "Education": 1,
    "TASMAC & Revenue": 2,
    "Women's Welfare": 3,
    "Infrastructure": 4,
}

app = FastAPI(title=API_TITLE, version="1.0.0")

# CORS: allow_origins defaults to * for local dev.
# Set ALLOWED_ORIGINS env var (comma-separated) in Cloud Run to lock down to Vercel domain.
_raw_origins = os.getenv("ALLOWED_ORIGINS", "*")
_allow_origins: list[str] = (
    ["*"] if _raw_origins.strip() == "*"
    else [o.strip() for o in _raw_origins.split(",") if o.strip()]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allow_origins,
    allow_origin_regex=r"https://.*\.vercel\.app",
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

with MAP_PATH.open("r", encoding="utf-8") as f:
    CONSTITUENCY_MAP: Dict[str, Dict[str, Any]] = json.load(f)

# LS reverse map: assembly_slug → { ls_slug, ls_name, ls_name_ta, ls_id, confidence }
ASSEMBLY_TO_LS: Dict[str, Dict[str, Any]] = {}
try:
    with LS_MAP_PATH.open("r", encoding="utf-8") as f:
        _ls_raw: Dict[str, Dict[str, Any]] = json.load(f)
    for ls_slug, ls_data in _ls_raw.items():
        ls_meta = {
            "ls_slug": ls_slug,
            "ls_name": ls_data["name"],
            "ls_name_ta": ls_data["name_ta"],
            "ls_id": ls_data["ls_id"],
            "confidence": ls_data.get("confidence", "MEDIUM"),
        }
        for a_slug in ls_data.get("assembly_slugs", []):
            ASSEMBLY_TO_LS[a_slug] = ls_meta
except FileNotFoundError:
    pass  # LS map optional — breadcrumb gracefully absent

_db = firestore.Client(project=PROJECT_ID)


def _party_id_from_name(party_name: str) -> str:
    name = (party_name or "").strip().upper()
    if "DMK" in name and "AIADMK" not in name and "ADMK" not in name:
        return "dmk"
    if "AIADMK" in name or "ADMK" in name:
        return "aiadmk"
    if "BJP" in name or "BHARATIYA JANATA" in name:
        return "bjp"
    if "PMK" in name:
        return "pmk"
    if "CONGRESS" in name or "INC" in name:
        return "inc"
    return "".join(c.lower() if c.isalnum() else "_" for c in name).strip("_")


def _sort_metrics(metrics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        metrics,
        key=lambda m: (
            METRIC_ORDER.get(str(m.get("metric_id", "")), 999),
            str(m.get("metric_id", "")),
        ),
    )


def _sort_promises(promises: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        promises,
        key=lambda p: (
            PILLAR_ORDER.get(str(p.get("category", "")), 99),
            str(p.get("doc_id", "")),
        ),
    )


def _compute_tn_percentile(
    value: float, all_values: List[float], higher_is_better: bool
) -> int:
    """Returns % of TN districts this value is 'better than' (0-100)."""
    if not all_values:
        return 0
    n_worse = sum(1 for v in all_values if (v < value if higher_is_better else v > value))
    return round(n_worse / len(all_values) * 100)


def _doc_to_dict(doc: firestore.DocumentSnapshot) -> Dict[str, Any]:
    data = doc.to_dict() or {}
    data.setdefault("doc_id", doc.id)
    return data


def _fetch_mla_by_constituency(
    constituency_slug: str, constituency_id: Optional[int],
    constituency_name: str = "", election_year: int = 2021
) -> Optional[Dict[str, Any]]:
    col = _db.collection("candidate_accountability")

    # Direct doc ID lookup — primary path for all years
    mla_doc_id = f"{election_year}_{constituency_slug}"
    direct = col.document(mla_doc_id).get()
    if direct.exists:
        return _doc_to_dict(direct)

    # Cross-year fallback: SC/ST reserved seats are stored without the _sc/_st suffix
    # when the source (e.g. MyNeta 2016) omits the reservation category from the name.
    # e.g. our slug map has "harur_sc" but the doc is stored as "2016_harur".
    base_slug = re.sub(r"_(sc|st)$", "", constituency_slug, flags=re.IGNORECASE)
    if base_slug != constituency_slug:
        base_fallback = col.document(f"{election_year}_{base_slug}").get()
        if base_fallback.exists:
            return _doc_to_dict(base_fallback)

    # Transliteration alias fallback: MyNeta spells some names differently from our map.
    # e.g. map="aruppukottai" but doc stored as "2016_aruppukkottai".
    alias_slug = MLA_SLUG_ALIASES.get((election_year, constituency_slug))
    if alias_slug:
        alias_doc = col.document(f"{election_year}_{alias_slug}").get()
        if alias_doc.exists:
            return _doc_to_dict(alias_doc)

    # 2021 fallback: SC/ST constituencies were loaded with a "dirty" slug
    # (e.g. "HARUR (SC)" → "harur__sc_") which differs from the clean map slug.
    if election_year == 2021 and constituency_name:
        dirty_slug = re.sub(r"[^a-z0-9]", "_", constituency_name.lower())
        if dirty_slug != constituency_slug:
            fallback = col.document(f"2021_{dirty_slug}").get()
            if fallback.exists:
                return _doc_to_dict(fallback)

    # 2021 fallback: query by constituency_id (older docs before slug-keyed re-ingest)
    if election_year == 2021 and isinstance(constituency_id, int):
        docs = list(col.where(filter=FieldFilter("constituency_id", "==", constituency_id)).limit(1).stream())
        if docs:
            return _doc_to_dict(docs[0])

    return None


def _filter_metrics(docs: List[firestore.DocumentSnapshot]) -> List[Dict[str, Any]]:
    metrics: List[Dict[str, Any]] = []
    for doc in docs:
        data = _doc_to_dict(doc)
        if str(data.get("metric_id", "")) in KEY_METRIC_IDS:
            metrics.append(data)
    return metrics


def _fetch_socio_metrics_for_district(
    district_slug: Optional[str],
) -> Tuple[List[Dict[str, Any]], str]:
    col = _db.collection("socio_economics")
    all_docs = list(col.stream())

    # State-level docs have no district_slug (ASER and similar state-only metrics)
    state_docs = [d for d in all_docs if not d.to_dict().get("district_slug")]
    state_metrics = _filter_metrics(state_docs)

    if district_slug:
        district_docs = [d for d in all_docs if d.to_dict().get("district_slug") == district_slug]
        district_metrics = _filter_metrics(district_docs)
        if district_metrics:
            # Build per-metric list of all district values for TN percentile
            metric_all_values: Dict[str, List[float]] = {}
            for doc in all_docs:
                d = doc.to_dict() or {}
                mid = str(d.get("metric_id", ""))
                ds  = d.get("district_slug")
                val = d.get("value")
                if ds and mid in KEY_METRIC_IDS and val is not None:
                    try:
                        metric_all_values.setdefault(mid, []).append(float(val))
                    except (TypeError, ValueError):
                        pass

            # Annotate each district metric with its TN percentile
            for m in district_metrics:
                mid  = str(m.get("metric_id", ""))
                val  = m.get("value")
                vals = metric_all_values.get(mid, [])
                if vals and val is not None:
                    higher = METRIC_HIGHER_IS_BETTER.get(mid, True)
                    m["tn_percentile"] = _compute_tn_percentile(float(val), vals, higher)

            # Merge: district wins for shared metric_ids; keep state-only for the rest
            district_ids = {m["metric_id"] for m in district_metrics}
            merged = district_metrics + [m for m in state_metrics if m["metric_id"] not in district_ids]
            return _sort_metrics(merged), "district"

    return _sort_metrics(state_metrics), "state_fallback"


def _fetch_promises_for_party(party_id: str) -> List[Dict[str, Any]]:
    if not party_id:
        return []

    col = _db.collection("manifesto_promises")
    docs = list(col.where(filter=FieldFilter("party_id", "==", party_id)).stream())
    promises = [_doc_to_dict(d) for d in docs]
    return _sort_promises(promises)


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "service": API_TITLE,
        "project_id": PROJECT_ID,
        "ls_map_loaded": len(ASSEMBLY_TO_LS),
    }


@app.get("/api/manifesto-promises")
def manifesto_promises(year: str = Query("all")) -> List[Dict[str, Any]]:
    try:
        col = _db.collection("manifesto_promises")

        if year == "all":
            docs = list(col.stream())
        else:
            try:
                year_int = int(year)
            except ValueError as exc:
                raise HTTPException(
                    status_code=400,
                    detail="year must be 'all' or a number (e.g. 2021, 2026)",
                ) from exc
            docs = list(col.where(filter=FieldFilter("target_year", "==", year_int)).stream())

        payload = [_doc_to_dict(d) for d in docs]
        return jsonable_encoder(_sort_promises(payload))
    except (GoogleAPICallError, RetryError) as exc:
        raise HTTPException(status_code=503, detail="Firestore unavailable") from exc


@app.get("/api/constituency/{slug}")
def constituency_drill(slug: str, term: int = Query(default=2021, ge=2001, le=2031)) -> Dict[str, Any]:
    try:
        map_entry = CONSTITUENCY_MAP.get(slug)
        district_meta = (
            {
                "constituency_name": map_entry.get("name"),
                "constituency_id": map_entry.get("constituency_id"),
                "district_name": map_entry.get("district"),
                "district_slug": map_entry.get("district_slug"),
            }
            if map_entry
            else None
        )

        constituency_id = district_meta.get("constituency_id") if district_meta else None
        district_slug = district_meta.get("district_slug") if district_meta else None

        constituency_name = district_meta.get("constituency_name", "") if district_meta else ""
        mla = _fetch_mla_by_constituency(slug, constituency_id, constituency_name, election_year=term)

        # Enrich MLA with photo_url from mla_profiles (2011 + 2016 + 2021)
        if mla and term in {2011, 2016, 2021}:
            for pdoc in _db.collection("mla_profiles").stream():
                pdata = pdoc.to_dict() or {}
                for party_entry in pdata.get("parties", []):
                    if party_entry.get("constituency_slug") != slug:
                        continue
                    # Match tenure start year: "2016–2021" → 2016 must equal term
                    tenure = party_entry.get("tenure", "")
                    if "–" in tenure or "-" in tenure:
                        sep = "–" if "–" in tenure else "-"
                        try:
                            tenure_start = int(tenure.split(sep)[0].strip())
                        except ValueError:
                            tenure_start = None
                        if tenure_start and tenure_start != term:
                            continue
                    photo_url = pdata.get("photo_url")
                    if photo_url:
                        mla["photo_url"] = photo_url
                    break
                if mla.get("photo_url"):
                    break

        metrics, metrics_scope = _fetch_socio_metrics_for_district(district_slug)

        promises: List[Dict[str, Any]] = []
        if mla:
            party_id = str(mla.get("party_id") or _party_id_from_name(str(mla.get("party", ""))))
            promises = _fetch_promises_for_party(party_id)

        # Parent Lok Sabha constituency lookup
        parent_ls = ASSEMBLY_TO_LS.get(slug)

        # District water risk — with TN percentile across all districts
        district_water_risk = None
        if district_slug:
            all_wr = list(_db.collection("district_water_risk").stream())
            wr_doc = next((d for d in all_wr if d.id == district_slug), None)
            if wr_doc and wr_doc.exists:
                district_water_risk = _doc_to_dict(wr_doc)
                score = district_water_risk.get("water_stress_score")
                if score is not None:
                    all_scores = [float(d.to_dict()["water_stress_score"]) for d in all_wr
                                  if d.to_dict().get("water_stress_score") is not None]
                    district_water_risk["tn_percentile"] = _compute_tn_percentile(
                        float(score), all_scores, higher_is_better=False
                    )

        # District crime index — with TN percentile
        district_crime_index = None
        if district_slug:
            all_ci = list(_db.collection("district_crime_index").stream())
            ci_doc = next((d for d in all_ci if d.id == district_slug), None)
            if ci_doc and ci_doc.exists:
                district_crime_index = _doc_to_dict(ci_doc)
                rate = district_crime_index.get("ipc_crime_rate_per_lakh")
                if rate is not None:
                    all_rates = [float(d.to_dict()["ipc_crime_rate_per_lakh"]) for d in all_ci
                                 if d.to_dict().get("ipc_crime_rate_per_lakh") is not None]
                    district_crime_index["tn_percentile"] = _compute_tn_percentile(
                        float(rate), all_rates, higher_is_better=False
                    )

        # District road safety — with TN percentile
        district_road_safety = None
        if district_slug:
            all_rs = list(_db.collection("district_road_safety").stream())
            rs_doc = next((d for d in all_rs if d.id == district_slug), None)
            if rs_doc and rs_doc.exists:
                district_road_safety = _doc_to_dict(rs_doc)
                dr = district_road_safety.get("death_rate_per_lakh_2023")
                if dr is not None:
                    all_dr = [float(d.to_dict()["death_rate_per_lakh_2023"]) for d in all_rs
                              if d.to_dict().get("death_rate_per_lakh_2023") is not None]
                    district_road_safety["tn_percentile"] = _compute_tn_percentile(
                        float(dr), all_dr, higher_is_better=False
                    )

        # Ward mapping — single doc keyed by constituency_slug
        ward_mapping = None
        wm_doc = _db.collection("ward_mapping").document(slug).get()
        if wm_doc.exists:
            ward_mapping = _doc_to_dict(wm_doc)

        # Map state assembly term → local body election year (GCC only for now)
        # term=2021 → 2022 GCC election
        # term=2011 → 2011 GCC election (elected Oct 2011, served through 2016)
        # term=2016 → None: Chennai Corp under administrator rule 2016–2022
        # term=2006 → None: no GCC data for pre-2011 elections
        TERM_TO_COUNCIL_YEAR: Dict[int, int] = {
            2021: 2022,
            2011: 2011,
        }
        council_year = TERM_TO_COUNCIL_YEAR.get(term)

        # ULB councillors — filter by assembly_constituency_slug (exact match)
        # ULB heads — look up for each local body in the ward mapping
        ulb_councillors: List[Dict[str, Any]] = []
        ulb_heads: List[Dict[str, Any]] = []

        # Councillors: fetch all for this AC slug, then filter by council_year
        all_c_docs = list(
            _db.collection("ulb_councillors")
            .where(filter=FieldFilter("assembly_constituency_slug", "==", slug))
            .stream()
        )
        for cdoc in all_c_docs:
            d = _doc_to_dict(cdoc)
            doc_year = d.get("election_year")
            if council_year == 2022:
                # 2022 docs may lack the election_year field (legacy); include them too
                if doc_year in (2022, None):
                    ulb_councillors.append(d)
            elif council_year is not None:
                if doc_year == council_year:
                    ulb_councillors.append(d)
            # If council_year is None (e.g. term=2011 with no data), return empty

        # Heads: one per unique local body, keyed by council_year suffix
        if ward_mapping:
            seen_local_body_slugs: set[str] = set()
            for lb in ward_mapping.get("local_bodies", []):
                lb_slug = (
                    lb.get("name", "")
                    .lower()
                    .replace(" ", "_")
                    .replace(".", "")
                    .replace("(", "")
                    .replace(")", "")
                    .replace("-", "_")
                )
                if lb_slug in seen_local_body_slugs:
                    continue
                seen_local_body_slugs.add(lb_slug)
                # Only look up heads when we have a council year for this term
                if council_year is None:
                    continue
                # Try year-specific doc first (e.g. greater_chennai_corporation_2011),
                # fall back to plain slug (e.g. greater_chennai_corporation for 2022)
                head_doc = None
                if council_year != 2022:
                    head_doc = _db.collection("ulb_heads").document(f"{lb_slug}_{council_year}").get()
                    if not head_doc.exists:
                        head_doc = None
                if head_doc is None:
                    head_doc = _db.collection("ulb_heads").document(lb_slug).get()
                if head_doc and head_doc.exists:
                    ulb_heads.append(_doc_to_dict(head_doc))

        payload = {
            "mla": mla,
            "metrics": metrics,
            "metrics_scope": metrics_scope,
            "district_meta": district_meta,
            "promises": promises,
            "parent_ls": parent_ls,
            "district_water_risk": district_water_risk,
            "district_crime_index": district_crime_index,
            "district_road_safety": district_road_safety,
            "ward_mapping": ward_mapping,
            "ulb_councillors": ulb_councillors,
            "ulb_heads": ulb_heads,
        }
        return jsonable_encoder(payload)
    except (GoogleAPICallError, RetryError) as exc:
        raise HTTPException(status_code=503, detail="Firestore unavailable") from exc


@app.get("/api/candidates/2026/{slug}")
def candidates_2026(slug: str) -> Dict[str, Any]:
    """Return 2026 election candidates for a constituency (deduplicated by name+party)."""
    try:
        # Resolve alias: if slug has a known alternate Firestore key, try that too
        lookup = CANDIDATE_2026_SLUG_ALIASES.get(slug, slug)
        doc = _db.collection("candidates_2026").document(lookup).get()
        if not doc.exists and lookup != slug:
            doc = _db.collection("candidates_2026").document(slug).get()
        if not doc.exists:
            raise HTTPException(status_code=404, detail=f"No 2026 candidate data for: {slug}")
        data = _doc_to_dict(doc)
        seen: set[tuple[str, str]] = set()
        unique: list = []
        for c in data.get("candidates", []):
            key = (c.get("name", "").strip().upper(), c.get("party", "").strip().upper())
            if key not in seen:
                seen.add(key)
                unique.append(c)
        data["candidates"] = unique
        data["total_candidates"] = len(unique)
        return jsonable_encoder(data)
    except (GoogleAPICallError, RetryError) as exc:
        raise HTTPException(status_code=503, detail="Firestore unavailable") from exc


@app.post("/api/constituency/{slug}/view")
def record_view(slug: str) -> Dict[str, Any]:
    """Atomically increment the view counter for a constituency.
    Called client-side on every page load via useEffect (not SSR).
    """
    if slug not in CONSTITUENCY_MAP:
        raise HTTPException(status_code=404, detail=f"Unknown constituency slug: {slug}")
    try:
        map_entry = CONSTITUENCY_MAP[slug]
        doc_ref = _db.collection("usage_counters").document(slug)
        doc_ref.set(
            {
                "view_count": Increment(1),
                "constituency_name": map_entry.get("name", slug),
                "district": map_entry.get("district", ""),
                "district_slug": map_entry.get("district_slug", ""),
                "last_viewed": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )
        return {"ok": True, "slug": slug}
    except (GoogleAPICallError, RetryError) as exc:
        # Non-fatal: view counter failure should not break the page
        raise HTTPException(status_code=503, detail="Firestore unavailable") from exc


@app.get("/api/frequently-browsed")
def frequently_browsed(limit: int = Query(6, ge=1, le=20)) -> List[Dict[str, Any]]:
    """Return top-N constituencies ordered by view_count descending."""
    try:
        col = _db.collection("usage_counters")
        docs = list(
            col.order_by("view_count", direction=firestore.Query.DESCENDING)
               .limit(limit)
               .stream()
        )
        result = []
        for doc in docs:
            data = doc.to_dict() or {}
            result.append({
                "slug": doc.id,
                "name": data.get("constituency_name", doc.id.replace("_", " ").upper()),
                "district": data.get("district", ""),
                "view_count": data.get("view_count", 0),
            })
        return jsonable_encoder(result)
    except (GoogleAPICallError, RetryError) as exc:
        raise HTTPException(status_code=503, detail="Firestore unavailable") from exc


# ─────────────────────────────────────────────────────────────────────────────
# Pincode resolve helpers
# ─────────────────────────────────────────────────────────────────────────────

# Domains we trust as govt or mainstream news sources
_TRUSTED_DOMAINS = {
    "tn.gov.in", "elections.tn.gov.in", "ceotn.in", "indiapost.gov.in",
    "pib.gov.in", "eci.gov.in", "myneta.info", "lgdirectory.gov.in",
    "wikipedia.org", "thehindu.com", "indianexpress.com",
    "timesofindia.com", "ndtv.com", "newindianexpress.com",
    "deccanherald.com", "scroll.in", "thewire.in", "hindustantimes.com",
}

# India Post API sometimes uses alternate spellings; normalize to our map names
_DISTRICT_ALIASES: Dict[str, str] = {
    "KANCHIPURAM": "KANCHEEPURAM",
    "TUTICORIN": "THOOTHUKUDI",
    "TIRUVALLUR": "THIRUVALLUR",
    "NILGIRIS": "THE NILGIRIS",
    "PUDUKKOTTAI": "PUDUKKOTTAI",
    "PUDUKOTTAI": "PUDUKKOTTAI",
    "TIRUPUR": "TIRUPPUR",
}


def _normalize_district(raw: str) -> str:
    u = raw.upper().strip()
    return _DISTRICT_ALIASES.get(u, u)


def _is_trusted(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower().lstrip("www.")
        return any(host == d or host.endswith("." + d) for d in _TRUSTED_DOMAINS)
    except Exception:
        return False


async def _ddg_search(query: str) -> List[Dict[str, Any]]:
    """Fetch DuckDuckGo Lite results; return list of {title, url, snippet, is_trusted}."""
    url = f"https://lite.duckduckgo.com/lite/?q={quote_plus(query)}"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; ArasiyalAayvu/1.0)"}
    try:
        async with httpx.AsyncClient(headers=headers, timeout=10, follow_redirects=True) as client:
            resp = await client.get(url)
            html = resp.text
    except Exception:
        return []

    # DDG Lite: links are anchors with href like /l/?uddg=<encodedURL>
    link_re = re.compile(r'href="(/l/\?[^"]+)"[^>]*>\s*([^<]+?)\s*</a>', re.IGNORECASE)
    snippet_re = re.compile(r'class="result-snippet"[^>]*>(.*?)</td>', re.DOTALL | re.IGNORECASE)

    raw_links = link_re.findall(html)
    raw_snippets = snippet_re.findall(html)

    results: List[Dict[str, Any]] = []
    for i, (href, title) in enumerate(raw_links[:20]):
        try:
            qs = parse_qs(urlparse("https://lite.duckduckgo.com" + href).query)
            actual_url = unquote(qs.get("uddg", [href])[0])
        except Exception:
            actual_url = href

        snippet = re.sub(r"<[^>]+>", " ", raw_snippets[i] if i < len(raw_snippets) else "").strip()
        results.append({
            "title": title.strip(),
            "url": actual_url,
            "snippet": snippet,
            "is_trusted": _is_trusted(actual_url),
        })

    trusted = [r for r in results if r["is_trusted"]]
    others = [r for r in results if not r["is_trusted"]]
    return trusted + others[:3]


def _match_constituencies(
    postal: Optional[Dict[str, Any]],
    search_results: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    suggestions: Dict[str, Dict[str, Any]] = {}
    conf_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}

    def upsert(slug: str, confidence: str, reason: str) -> None:
        meta = CONSTITUENCY_MAP.get(slug)
        if not meta:
            return
        if slug not in suggestions:
            suggestions[slug] = {
                "slug": slug,
                "name": meta["name"],
                "district": meta.get("district", ""),
                "confidence": confidence,
                "reasons": [reason],
            }
        else:
            suggestions[slug]["reasons"].append(reason)
            if conf_order.get(confidence, 9) < conf_order.get(suggestions[slug]["confidence"], 9):
                suggestions[slug]["confidence"] = confidence

    if postal:
        ip_district = _normalize_district(postal.get("district", ""))
        ip_taluk = postal.get("taluk", "").upper().strip()

        for slug, meta in CONSTITUENCY_MAP.items():
            con_district = meta.get("district", "").upper()
            con_name = re.sub(r"\s*\([^)]+\)", "", meta.get("name", "")).upper().strip()

            if not ip_district or con_district != ip_district:
                continue

            # Taluk matches constituency name → HIGH confidence
            taluk_clean = re.sub(r"\s+", " ", ip_taluk)
            if taluk_clean and (taluk_clean in con_name or con_name in taluk_clean):
                upsert(slug, "HIGH", f"District+Taluk: {ip_district}/{ip_taluk}")
            else:
                upsert(slug, "MEDIUM", f"District: {ip_district}")

    # Scan trusted search snippets for known constituency names
    trusted_text = " ".join(
        r.get("title", "") + " " + r.get("snippet", "")
        for r in search_results if r.get("is_trusted")
    ).upper()

    if trusted_text.strip():
        for slug, meta in CONSTITUENCY_MAP.items():
            name = re.sub(r"\s*\([^)]+\)", "", meta.get("name", "")).upper().strip()
            if name and len(name) > 4 and name in trusted_text:
                upsert(slug, "MEDIUM", f"Mentioned in trusted search results")

    result = sorted(
        suggestions.values(),
        key=lambda s: (conf_order.get(s["confidence"], 9), s["name"]),
    )[:10]

    # Enrich each suggestion with its parent Lok Sabha constituency
    for s in result:
        ls = ASSEMBLY_TO_LS.get(s["slug"])
        if ls:
            s["ls_name"] = ls["ls_name"]
            s["ls_slug"] = ls["ls_slug"]

    return result


class PincodePatch(BaseModel):
    slug: str
    is_ambiguous: bool = False
    ls_slug: Optional[str] = None


@app.get("/api/pincode/{code}/resolve")
async def resolve_pincode(code: str) -> Dict[str, Any]:
    """Search govt + news sources and suggest constituency mapping for a pincode."""
    if not code.isdigit() or len(code) != 6:
        raise HTTPException(status_code=400, detail="Pincode must be 6 digits")

    postal: Optional[Dict[str, Any]] = None
    search_results: List[Dict[str, Any]] = []

    # 1. India Post unofficial API (authoritative for district/taluk)
    try:
        async with httpx.AsyncClient(
            timeout=12,
            headers={"User-Agent": "Mozilla/5.0 (compatible; ArasiyalAayvu/1.0)"},
            follow_redirects=True,
        ) as client:
            resp = await client.get(f"https://api.postalpincode.in/{code}")
            if resp.status_code == 200:
                data = resp.json()
                if data and data[0].get("Status") == "Success":
                    offices = data[0].get("PostOffice", [])
                    if offices:
                        first = offices[0]
                        # India Post API returns taluk as "Block" (e.g. "Purasawalkam Taluk")
                        block = first.get("Block", "")
                        taluk = re.sub(r"\s+[Tt]aluk$", "", block).strip()
                        postal = {
                            "district": first.get("District", ""),
                            "taluk": taluk,
                            "division": first.get("Division", ""),
                            "state": first.get("State", ""),
                            "post_offices": [o.get("Name", "") for o in offices[:8]],
                        }
    except Exception:
        pass

    # 2. DuckDuckGo search filtered to govt/news sources
    query = (
        f"For this pincode {code} give me the following: "
        "State Assembly Constituency Name, "
        "Lok Sabha Constituency Name (If available), "
        "District Name, "
        "Taluk "
        "Key localities. "
        "Give me the results from only legit sources."
    )
    try:
        search_results = await _ddg_search(query)
    except Exception:
        pass

    suggestions = _match_constituencies(postal, search_results)

    # Build a slug→ls_info lookup for all known assembly→LS mappings (for frontend enrichment)
    assembly_ls_map = {
        slug: {"ls_slug": info["ls_slug"], "ls_name": info["ls_name"]}
        for slug, info in ASSEMBLY_TO_LS.items()
    }

    return jsonable_encoder({
        "pincode": code,
        "postal": postal,
        "search_results": [r for r in search_results if r["is_trusted"]][:8],
        "assembly_ls_map": assembly_ls_map,
        "suggestions": suggestions,
        "google_url": f"https://www.google.com/search?q={quote_plus(query)}",
    })


@app.post("/api/pincode/{code}/patch")
def patch_pincode(code: str, body: PincodePatch) -> Dict[str, Any]:
    """Apply a resolved constituency mapping directly to Firestore."""
    if not code.isdigit() or len(code) != 6:
        raise HTTPException(status_code=400, detail="Pincode must be 6 digits")

    meta = CONSTITUENCY_MAP.get(body.slug)
    if not meta:
        raise HTTPException(status_code=400, detail=f"Unknown constituency: {body.slug}")

    doc: Dict[str, Any] = {
        "pincode": code,
        "district": meta.get("district", ""),
        "is_ambiguous": body.is_ambiguous,
        "constituencies": [{
            "slug": body.slug,
            "name": meta.get("name", body.slug),
            "name_ta": meta.get("tamil_name", ""),
            "type": "assembly",
        }],
        "ground_truth_confidence": "HIGH",
        "_schema_version": "2.0",
        "_manually_resolved": True,
    }

    # Also store parent Lok Sabha constituency if provided
    ls_slug = body.ls_slug or (ASSEMBLY_TO_LS.get(body.slug) or {}).get("ls_slug")
    if ls_slug:
        ls_meta = next(
            (v for v in ASSEMBLY_TO_LS.values() if v.get("ls_slug") == ls_slug), None
        )
        if ls_meta:
            doc["ls_constituency"] = {
                "slug": ls_slug,
                "name": ls_meta.get("ls_name", ls_slug),
                "type": "lok_sabha",
            }

    try:
        _db.collection("pincode_mapping").document(code).set(doc)
        return {"ok": True, "pincode": code, "slug": body.slug}
    except (GoogleAPICallError, RetryError) as exc:
        raise HTTPException(status_code=503, detail="Firestore unavailable") from exc


@app.get("/api/lookup-pincode")
def lookup_pincode(code: str = Query(..., min_length=6, max_length=6)) -> Dict[str, Any]:
    """Resolve a 6-digit pincode to one or more Assembly Constituencies.

    Returns:
        { pincode, district, constituencies: [{slug, name, name_ta}], is_ambiguous }

    Raises 400 if the code is not 6 digits, 404 if not in the mapping.
    """
    if not code.isdigit():
        raise HTTPException(status_code=400, detail="Pincode must be 6 digits")
    try:
        doc = _db.collection("pincode_mapping").document(code).get()
        if not doc.exists:
            raise HTTPException(status_code=404, detail=f"Pincode {code} not found in mapping")
        data = doc.to_dict()
        # Drop low-confidence entries (>3 constituencies = district-level fallback)
        if len(data.get("constituencies", [])) > 3:
            raise HTTPException(status_code=404, detail=f"Pincode {code} not found in mapping")
        # Enrich each constituency with its district from the constituency map
        for c in data.get("constituencies", []):
            meta = CONSTITUENCY_MAP.get(c.get("slug", ""), {})
            if meta.get("district"):
                c["district"] = meta["district"].title()
        return jsonable_encoder(data)
    except (GoogleAPICallError, RetryError) as exc:
        raise HTTPException(status_code=503, detail="Firestore unavailable") from exc


@app.get("/api/state-vitals")
def state_vitals(category: str = Query("all")) -> Dict[str, List[Dict[str, Any]]]:
    """Return state macro, health, water risk, and crop economics data.
    category: 'all' | 'economy' | 'health' | 'water' | 'crops'
    """
    try:
        result: Dict[str, List[Dict[str, Any]]] = {}

        fetch_map = {
            "economy": ("state_macro", None),
            "health": ("district_health", None),
            "water": ("district_water_risk", None),
            "crops": ("crop_economics", None),
        }

        categories_to_fetch = list(fetch_map.keys()) if category == "all" else [category]

        for cat in categories_to_fetch:
            if cat not in fetch_map:
                continue
            coll_name, _ = fetch_map[cat]
            docs = list(_db.collection(coll_name).stream())
            records = []
            for doc in docs:
                data = doc.to_dict() or {}
                data.setdefault("doc_id", doc.id)
                records.append(data)
            result[cat] = records

        return jsonable_encoder(result)
    except (GoogleAPICallError, RetryError) as exc:
        raise HTTPException(status_code=503, detail="Firestore unavailable") from exc
