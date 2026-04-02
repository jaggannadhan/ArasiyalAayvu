from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from google.api_core.exceptions import GoogleAPICallError, RetryError
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from google.cloud.firestore_v1 import Increment

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "naatunadappu")
API_TITLE = "ArasiyalAayvu Backend API"

ROOT_DIR = Path(__file__).resolve().parents[1]
MAP_PATH = ROOT_DIR / "src" / "lib" / "constituency-map.json"
LS_MAP_PATH = ROOT_DIR / "src" / "lib" / "ls-constituency-map.json"

KEY_METRIC_IDS = {
    "aser2024_std3_reading_recovery",
    "nfhs5_stunting_under5",
    "nfhs5_anaemia_women",
    "industrial_corridors_district_coverage",
}
METRIC_ORDER = {
    "aser2024_std3_reading_recovery": 0,
    "nfhs5_stunting_under5": 1,
    "nfhs5_anaemia_women": 2,
    "industrial_corridors_district_coverage": 3,
}
PILLAR_ORDER = {
    "Agriculture": 0,
    "Education": 1,
    "TASMAC & Revenue": 2,
    "Women's Welfare": 3,
    "Infrastructure": 4,
}

app = FastAPI(title=API_TITLE, version="1.0.0")

# Public-read API — POST allowed for view-counter increments.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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


def _doc_to_dict(doc: firestore.DocumentSnapshot) -> Dict[str, Any]:
    data = doc.to_dict() or {}
    data.setdefault("doc_id", doc.id)
    return data


def _fetch_mla_by_constituency(
    constituency_slug: str, constituency_id: Optional[int], constituency_name: str = ""
) -> Optional[Dict[str, Any]]:
    col = _db.collection("candidate_accountability")

    if isinstance(constituency_id, int):
        docs = list(col.where(filter=FieldFilter("constituency_id", "==", constituency_id)).limit(1).stream())
        if docs:
            return _doc_to_dict(docs[0])

    docs = list(col.where(filter=FieldFilter("constituency_slug", "==", constituency_slug)).limit(1).stream())
    if docs:
        return _doc_to_dict(docs[0])

    # Try direct lookup using the clean map slug
    mla_doc_id = f"2021_{constituency_slug}"
    direct = col.document(mla_doc_id).get()
    if direct.exists:
        return _doc_to_dict(direct)

    # Fallback: SC/ST constituencies were loaded with a "dirty" slug derived from
    # the raw constituency name (e.g. "HARUR (SC)" → "harur__sc_"), which differs
    # from the clean map slug ("harur_sc"). Try that variant too.
    if constituency_name:
        dirty_slug = re.sub(r"[^a-z0-9]", "_", constituency_name.lower())
        if dirty_slug != constituency_slug:
            fallback = col.document(f"2021_{dirty_slug}").get()
            if fallback.exists:
                return _doc_to_dict(fallback)

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

    if district_slug:
        district_docs = list(col.where(filter=FieldFilter("district_slug", "==", district_slug)).stream())
        district_metrics = _filter_metrics(district_docs)
        if district_metrics:
            return _sort_metrics(district_metrics), "district"

    state_docs = list(col.stream())
    state_metrics = _filter_metrics(state_docs)
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
def constituency_drill(slug: str) -> Dict[str, Any]:
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
        mla = _fetch_mla_by_constituency(slug, constituency_id, constituency_name)
        metrics, metrics_scope = _fetch_socio_metrics_for_district(district_slug)

        promises: List[Dict[str, Any]] = []
        if mla:
            party_id = str(mla.get("party_id") or _party_id_from_name(str(mla.get("party", ""))))
            promises = _fetch_promises_for_party(party_id)

        # Parent Lok Sabha constituency lookup
        parent_ls = ASSEMBLY_TO_LS.get(slug)

        payload = {
            "mla": mla,
            "metrics": metrics,
            "metrics_scope": metrics_scope,
            "district_meta": district_meta,
            "promises": promises,
            "parent_ls": parent_ls,  # { ls_slug, ls_name, ls_name_ta, ls_id, confidence } | null
        }
        return jsonable_encoder(payload)
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
        return jsonable_encoder(doc.to_dict())
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
