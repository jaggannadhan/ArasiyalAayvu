from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple  # noqa: UP035
from urllib.parse import quote_plus, urlparse, parse_qs, unquote

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
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

import threading
import time as _time

app = FastAPI(title=API_TITLE, version="1.0.0")


# ─────────────────────────────────────────────────────────────────────────────
# Live user count — piggybacks on existing API traffic (zero extra client
# requests). Each request carrying an X-Session-ID header is logged into an
# in-memory set. A background thread flushes distinct-session counts to a
# single Firestore doc once per minute. GET /api/live-count reads that doc
# (cached for 30s) to serve the badge number.
# ─────────────────────────────────────────────────────────────────────────────

_PRESENCE_WINDOW_SEC = 300      # "active in last 5 min"
_FLUSH_INTERVAL_SEC  = 60       # flush to Firestore every 60s
_COUNT_CACHE_SEC     = 30       # cache the count read for 30s

# In-memory: {session_id: last_seen_epoch}
_sessions: Dict[str, float] = {}
_sessions_lock = threading.Lock()

# Cached count result
_live_count_cache: Dict[str, Any] = {"count": 0, "ts": 0.0}


def _record_session(session_id: str) -> None:
    with _sessions_lock:
        _sessions[session_id] = _time.time()


def _flush_presence() -> None:
    """Flush active-session count from this instance to Firestore. Runs in a
    background thread every _FLUSH_INTERVAL_SEC seconds."""
    while True:
        _time.sleep(_FLUSH_INTERVAL_SEC)
        try:
            now = _time.time()
            with _sessions_lock:
                # Prune stale sessions and count active ones.
                stale = [sid for sid, ts in _sessions.items() if now - ts > _PRESENCE_WINDOW_SEC]
                for sid in stale:
                    del _sessions[sid]
                active = len(_sessions)

            # Write this instance's count + a server timestamp so the reader
            # can sum across instances and ignore stale ones.
            import socket
            instance_id = os.getenv("K_REVISION", socket.gethostname())[:32]
            _db.collection("meta").document("presence").collection("instances").document(instance_id).set({
                "active": active,
                "updated_at": firestore.SERVER_TIMESTAMP,
            })
        except Exception:
            pass  # best-effort; never crash the background thread


# Start the flusher thread (daemon so it dies with the process).
_flusher = threading.Thread(target=_flush_presence, daemon=True)
_flusher.start()

# CORS: allow_origins defaults to * for local dev.
# Set ALLOWED_ORIGINS env var (comma-separated) in Cloud Run to lock down to Vercel domain.
_raw_origins = os.getenv("ALLOWED_ORIGINS", "*")
_allow_origins: list[str] = (
    ["*"] if _raw_origins.strip() == "*"
    else [o.strip() for o in _raw_origins.split(",") if o.strip()]
)

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest
from starlette.responses import Response as StarletteResponse


class SessionTrackingMiddleware(BaseHTTPMiddleware):
    """Record X-Session-ID from every inbound request. No extra client calls
    needed — the header piggybacks on normal data fetches."""
    async def dispatch(self, request: StarletteRequest, call_next) -> StarletteResponse:  # type: ignore[override]
        sid = request.headers.get("x-session-id")
        if sid:
            _record_session(sid)
        return await call_next(request)


# Middleware order matters: last-added = outermost. CORSMiddleware must be
# outermost so it adds Access-Control-* headers even when inner middleware
# or the route handler raises an error (404, 500, etc.).
app.add_middleware(SessionTrackingMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allow_origins,
    allow_origin_regex=r"https://.*\.vercel\.app",
    allow_credentials=False,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
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
    # 2026 winners are in a separate per-year collection with slug-only doc IDs
    if election_year == 2026:
        doc_2026 = _db.collection("candidate_accountability_2026").document(constituency_slug).get()
        if doc_2026.exists:
            return _doc_to_dict(doc_2026)
        # Try without _sc/_st suffix
        base = re.sub(r"_(sc|st)$", "", constituency_slug, flags=re.IGNORECASE)
        if base != constituency_slug:
            fallback = _db.collection("candidate_accountability_2026").document(base).get()
            if fallback.exists:
                return _doc_to_dict(fallback)
        return None

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


def _fetch_mla_from_profile(
    constituency_slug: str, election_year: int
) -> Optional[Dict[str, Any]]:
    """Try to fetch MLA data from politician_profile via the reverse index.
    Returns a dict shaped like candidate_accountability for backward compat,
    or None if not found."""
    slug_clean = constituency_slug.rstrip("_")
    index_key = f"{election_year}_{slug_clean}"

    # Also try without SC/ST suffix
    base_slug = re.sub(r"_(sc|st)$", "", slug_clean, flags=re.IGNORECASE)
    keys_to_try = [index_key]
    if base_slug != slug_clean:
        keys_to_try.append(f"{election_year}_{base_slug}")

    profile_id = None
    for key in keys_to_try:
        idx_doc = _db.collection("constituency_mla_index").document(key).get()
        if idx_doc.exists:
            profile_id = (idx_doc.to_dict() or {}).get("profile_id")
            break

    if not profile_id:
        return None

    prof_doc = _db.collection("politician_profile").document(profile_id).get()
    if not prof_doc.exists:
        return None

    prof = prof_doc.to_dict() or {}
    timeline = prof.get("timeline") or []

    # Find the timeline entry matching this year + constituency
    entry = None
    for t in timeline:
        t_slug = (t.get("constituency_slug") or "").rstrip("_")
        if t.get("year") == election_year and (t_slug == slug_clean or t_slug == base_slug):
            entry = t
            break

    if not entry:
        return None

    # Shape the response to match candidate_accountability format so the
    # frontend doesn't need changes. Profile-level fields override where richer.
    return {
        "doc_id": prof_doc.id,
        "mla_name": prof.get("canonical_name") or entry.get("mla_name", ""),
        "party": entry.get("party"),
        "photo_url": prof.get("photo_url"),
        "election_year": election_year,
        "constituency": entry.get("constituency"),
        "constituency_slug": entry.get("constituency_slug"),
        "assets_cr": entry.get("assets_cr"),
        "movable_assets_cr": entry.get("movable_assets_cr"),
        "immovable_assets_cr": entry.get("immovable_assets_cr"),
        "liabilities_cr": entry.get("liabilities_cr"),
        "net_assets_cr": entry.get("net_assets_cr"),
        "is_crorepati": entry.get("is_crorepati"),
        "criminal_cases": entry.get("criminal_cases") or [],
        "criminal_cases_total": entry.get("criminal_cases_total") or 0,
        "criminal_severity": entry.get("criminal_severity") or "CLEAN",
        "education": entry.get("education"),
        "education_tier": entry.get("education_tier"),
        "source_url": entry.get("source_url"),
        "ground_truth_confidence": entry.get("ground_truth_confidence"),
        # Profile-level extras
        "gender": prof.get("gender"),
        "age": prof.get("age"),
        "aliases": prof.get("aliases") or [],
        "_source": "politician_profile",
    }


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


@app.get("/api/live-count")
def live_count() -> Dict[str, Any]:
    """Return the approximate number of users active in the last 5 minutes.

    Reads from Firestore `meta/presence/instances/*` — each Cloud Run instance
    writes its local session count every 60s. We sum across instances whose
    `updated_at` is within the last 2 minutes (stale instances are ignored).
    Result is cached in-memory for 30s so high traffic doesn't hammer Firestore.
    """
    now = _time.time()
    if now - _live_count_cache["ts"] < _COUNT_CACHE_SEC:
        return {"count": _live_count_cache["count"], "cached": True}

    try:
        from datetime import datetime, timezone, timedelta
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=_FLUSH_INTERVAL_SEC * 2)
        docs = list(
            _db.collection("meta")
            .document("presence")
            .collection("instances")
            .stream()
        )
        total = 0
        for d in docs:
            data = d.to_dict() or {}
            updated = data.get("updated_at")
            if updated and updated.replace(tzinfo=timezone.utc) >= cutoff:
                total += data.get("active", 0)

        _live_count_cache["count"] = total
        _live_count_cache["ts"] = now
        return {"count": total, "cached": False}
    except Exception:
        return {"count": _live_count_cache.get("count", 0), "cached": True}


# ─────────────────────────────────────────────────────────────────────────────
# Feedback — user-submitted corrections, suggestions, bug reports, etc.
# ─────────────────────────────────────────────────────────────────────────────

_FEEDBACK_CATEGORIES = {"correction", "missing_data", "suggestion", "bug_report", "other"}


class FeedbackSubmission(BaseModel):
    category: str
    message: str
    page_url: Optional[str] = None     # client-captured, e.g. window.location.href
    entity_context: Optional[Dict[str, Any]] = None  # optional: { slug, doc_id, … }


@app.post("/api/feedback")
def submit_feedback(payload: FeedbackSubmission, request: Request) -> Dict[str, Any]:
    category = (payload.category or "").strip().lower()
    if category not in _FEEDBACK_CATEGORIES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown category. Expected one of: {sorted(_FEEDBACK_CATEGORIES)}",
        )

    message = (payload.message or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="Message is required.")
    if len(message) > 5000:
        raise HTTPException(status_code=400, detail="Message too long (max 5000 characters).")

    # Client-provided context — trim but accept free-form.
    page_url = (payload.page_url or "").strip()[:500] or None
    entity_context = payload.entity_context or None

    # Capture server-side signals useful for moderation/debugging.
    user_agent = request.headers.get("user-agent", "")[:500]
    # Respect the Cloud-Run/Vercel proxy chain when reading client IP.
    fwd = request.headers.get("x-forwarded-for", "")
    client_ip = fwd.split(",")[0].strip() if fwd else (request.client.host if request.client else "")

    doc = {
        "category":       category,
        "message":        message,
        "page_url":       page_url,
        "entity_context": entity_context,
        "user_agent":     user_agent,
        "client_ip":      client_ip,
        "status":         "new",  # for future moderation workflow
        "created_at":     firestore.SERVER_TIMESTAMP,
    }

    try:
        ref = _db.collection("feedback").document()
        ref.set(doc)
    except (GoogleAPICallError, RetryError) as exc:
        raise HTTPException(status_code=503, detail="Firestore unavailable") from exc

    return {"ok": True, "id": ref.id}


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

        # Try politician_profile first (single source of truth with merged
        # timelines, standardized names, and curated photos). Fall back to
        # candidate_accountability if not found in the index.
        mla = _fetch_mla_from_profile(slug, election_year=term)
        if not mla:
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

        # Fallback: if still no photo, check candidates_2026 for same constituency
        # (candidate is contesting again so ECI has a recent photo)
        if mla and not mla.get("photo_url"):
            c26_doc = _db.collection("candidates_2026").document(slug).get()
            if c26_doc.exists:
                c26_data = c26_doc.to_dict() or {}
                import re as _re
                def _name_tokens(s: str) -> frozenset:
                    # Split into alpha tokens only, lowercase — order/punctuation agnostic
                    return frozenset(t for t in _re.findall(r"[a-z]+", s.lower()) if len(t) > 1)
                mla_tokens = _name_tokens(str(mla.get("mla_name", "")))
                for cand in c26_data.get("candidates", []):
                    if cand.get("photo_url") and _name_tokens(str(cand.get("name", ""))) == mla_tokens:
                        mla["photo_url"] = cand["photo_url"]
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

        # 2026 election result for this constituency
        election_result_2026 = None
        result_doc = _db.collection("election_results_2026").document(slug).get()
        if result_doc.exists:
            rd = result_doc.to_dict()
            election_result_2026 = {
                "winner": rd.get("winner"),
                "runner_up": rd.get("runner_up"),
                "margin": rd.get("margin"),
                "total_votes": rd.get("total_votes"),
                "status": rd.get("status"),
            }

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
            "election_result_2026": election_result_2026,
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
# Knowledge Graph API
# Collections: plfs, srs, hces, aishe, sdg_index, cost_of_living,
#              udise, ncrb, asi
# Firestore structure: {collection}/{entity_slug}/snapshots/{period}
# ─────────────────────────────────────────────────────────────────────────────

_KG_COLLECTIONS = frozenset({
    "plfs", "srs", "hces", "aishe", "sdg_index", "cost_of_living",
    "udise", "ncrb", "asi", "rbi_state_finances",
    "energy_stats", "mofpi",
})

# States we have KG data for → their Firestore entity slug (matches ts_utils.slugify)
_KG_STATE_SLUGS = {
    "tamil_nadu", "kerala", "karnataka", "andhra_pradesh", "telangana",
}


def _kg_latest_snapshot(collection: str, entity_slug: str) -> Optional[Dict[str, Any]]:
    snaps = list(
        _db.collection(collection).document(entity_slug).collection("snapshots").stream()
    )
    if not snaps:
        return None
    latest = max(snaps, key=lambda d: d.id)
    return {"period": latest.id, **(latest.to_dict() or {})}


@app.get("/api/kg/{collection}")
def kg_list(collection: str) -> List[Dict[str, Any]]:
    """List all entities in a KG collection with their latest snapshot."""
    if collection not in _KG_COLLECTIONS:
        raise HTTPException(status_code=404, detail=f"Unknown KG collection: {collection}")
    try:
        entity_docs = list(_db.collection(collection).stream())
        result = []
        for doc in entity_docs:
            meta = doc.to_dict() or {}
            snap = _kg_latest_snapshot(collection, doc.id)
            entry: Dict[str, Any] = {**meta}
            if snap:
                entry["latest_period"] = snap.pop("period")
                entry["snapshot"] = snap
            result.append(entry)
        return jsonable_encoder(result)
    except (GoogleAPICallError, RetryError) as exc:
        raise HTTPException(status_code=503, detail="Firestore unavailable") from exc


@app.get("/api/kg/{collection}/{entity_slug}")
def kg_entity(collection: str, entity_slug: str) -> Dict[str, Any]:
    """Get all snapshots for a KG entity (time-series)."""
    if collection not in _KG_COLLECTIONS:
        raise HTTPException(status_code=404, detail=f"Unknown KG collection: {collection}")
    try:
        entity_ref = _db.collection(collection).document(entity_slug)
        entity_doc = entity_ref.get()
        if not entity_doc.exists:
            raise HTTPException(
                status_code=404,
                detail=f"No entity '{entity_slug}' in {collection}",
            )
        meta = entity_doc.to_dict() or {}
        snaps = list(entity_ref.collection("snapshots").stream())
        snapshots = {d.id: d.to_dict() for d in snaps}
        return jsonable_encoder({**meta, "snapshots": snapshots})
    except (GoogleAPICallError, RetryError) as exc:
        raise HTTPException(status_code=503, detail="Firestore unavailable") from exc


@app.get("/api/kg/{collection}/{entity_slug}/{period}")
def kg_snapshot(collection: str, entity_slug: str, period: str) -> Dict[str, Any]:
    """Get a single KG snapshot for an entity + period."""
    if collection not in _KG_COLLECTIONS:
        raise HTTPException(status_code=404, detail=f"Unknown KG collection: {collection}")
    try:
        snap_doc = (
            _db.collection(collection)
            .document(entity_slug)
            .collection("snapshots")
            .document(period)
            .get()
        )
        if not snap_doc.exists:
            raise HTTPException(
                status_code=404,
                detail=f"No snapshot '{entity_slug}/{period}' in {collection}",
            )
        return jsonable_encoder(snap_doc.to_dict() or {})
    except (GoogleAPICallError, RetryError) as exc:
        raise HTTPException(status_code=503, detail="Firestore unavailable") from exc


@app.get("/api/state-report/{state_slug}")
def state_report(state_slug: str) -> Dict[str, Any]:
    """
    Aggregate all KG datasets for a state into one response.
    Returns the latest snapshot from: plfs, srs, hces, aishe, sdg_index,
    udise, ncrb, asi.
    For Tamil Nadu also includes cost_of_living_tamil_nadu.
    Always includes cost_of_living_india (fuel prices, national).
    Includes all_india reference snapshots for plfs, srs, hces, udise, asi.
    """
    if state_slug not in _KG_STATE_SLUGS:
        raise HTTPException(status_code=404, detail=f"No state report for: {state_slug}")
    try:
        report: Dict[str, Any] = {"state": state_slug}

        for col in (
            "plfs", "srs", "hces", "aishe", "sdg_index", "udise", "ncrb", "asi",
            "rbi_state_finances", "energy_stats", "mofpi",
        ):
            report[col] = _kg_latest_snapshot(col, state_slug)

        report["cost_of_living"] = _kg_latest_snapshot(
            "cost_of_living", f"cost_of_living_{state_slug}"
        )

        # State budgets (CAG Finance Accounts) — doc ID = {code}_{fy}
        _SLUG_TO_CAG_CODE = {
            "tamil_nadu": "TN", "kerala": "KL", "karnataka": "KA",
            "andhra_pradesh": "AP", "telangana": "TS",
        }
        cag_code = _SLUG_TO_CAG_CODE.get(state_slug)
        report["state_budget"] = None
        if cag_code:
            # Find latest by scanning all docs for this state
            budget_docs = [
                d for d in _db.collection("state_budgets").stream()
                if d.id.startswith(f"{cag_code}_")
            ]
            if budget_docs:
                latest = max(budget_docs, key=lambda d: d.id)
                report["state_budget"] = latest.to_dict()

        report["all_india"] = {
            col: _kg_latest_snapshot(col, "all_india")
            for col in ("plfs", "srs", "hces", "udise", "asi", "sdg_index", "ncrb", "aishe", "energy_stats")
        }

        return jsonable_encoder(report)
    except (GoogleAPICallError, RetryError) as exc:
        raise HTTPException(status_code=503, detail="Firestore unavailable") from exc


# ─────────────────────────────────────────────────────────────────────────────
# Knowledge Graph
# ─────────────────────────────────────────────────────────────────────────────

_kg_graph_cache: Dict[str, Any] = {"data": None, "ts": 0}

@app.get("/api/knowledge-graph")
def knowledge_graph():
    """Serve the pre-built knowledge graph JSON (cached for 1 hour)."""
    import time
    now = time.time()
    # Cache for 1 hour
    if _kg_graph_cache["data"] and now - _kg_graph_cache["ts"] < 3600:
        return _kg_graph_cache["data"]

    try:
        from google.cloud import storage
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket("naatunadappu-media")
        blob = bucket.blob("knowledge_graph/latest.json")
        raw = blob.download_as_text()
        data = json.loads(raw)
        _kg_graph_cache["data"] = data
        _kg_graph_cache["ts"] = now
        return data
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Could not load knowledge graph: {exc}") from exc


# ─────────────────────────────────────────────────────────────────────────────
# Graph Query API — runtime traversal over the KG
# ─────────────────────────────────────────────────────────────────────────────

from . import graph_query as _gq  # noqa: E402
from . import sdg_alignment as _sdg  # noqa: E402


def _graph() -> "_gq.nx.MultiDiGraph":
    g, _raw = _gq.load_graph(project_id=PROJECT_ID)
    return g


def _fetch_promise_doc(raw_doc_id: str) -> Optional[Dict[str, Any]]:
    doc = _db.collection("manifesto_promises").document(raw_doc_id).get()
    return doc.to_dict() if doc.exists else None


def _fetch_state_finances_latest(state_slug: str = "tamil_nadu") -> Optional[Dict[str, Any]]:
    # state_finances collection is keyed by fiscal_year (e.g., "2025-26"), TN-only for now
    if state_slug != "tamil_nadu":
        return None
    docs = list(_db.collection("state_finances").stream())
    if not docs:
        return None
    latest = max(docs, key=lambda d: d.id)
    return {"fiscal_year": latest.id, **(latest.to_dict() or {})}


def _fetch_indicator_snapshot(collection: str, entity_slug: str) -> Optional[Dict[str, Any]]:
    if collection not in _KG_COLLECTIONS:
        return None
    return _kg_latest_snapshot(collection, entity_slug)


@app.post("/api/graph/cache/clear")
def graph_cache_clear() -> Dict[str, Any]:
    """Evict the in-memory KG; next request re-fetches from GCS. Use after a
    `graph_builder.py --upload` so the new graph is served immediately."""
    return _gq.clear_cache()


@app.get("/api/graph/neighbors/{node_id:path}")
def graph_neighbors(
    node_id: str,
    verb: Optional[str] = Query(None, description="Filter to a single relation verb"),
    direction: str = Query("out", pattern="^(in|out|both)$"),
    limit: int = Query(200, ge=1, le=1000),
) -> Dict[str, Any]:
    """Immediate 1-hop neighbors of a KG node, filterable by edge verb + direction."""
    g = _graph()
    if node_id not in g:
        raise HTTPException(status_code=404, detail=f"Unknown node: {node_id}")
    node = {"id": node_id, **g.nodes[node_id]}
    results = _gq.neighbors(g, node_id, verb=verb, direction=direction, limit=limit)
    return {"node": node, "neighbors": results, "count": len(results)}


@app.get("/api/graph/traverse/{node_id:path}")
def graph_traverse(
    node_id: str,
    verbs: Optional[str] = Query(None, description="Comma-separated verbs to follow"),
    max_depth: int = Query(3, ge=1, le=6),
    max_nodes: int = Query(500, ge=1, le=2000),
) -> Dict[str, Any]:
    """BFS traversal from a node, optionally restricted to specific edge verbs."""
    g = _graph()
    allowed = [v.strip() for v in verbs.split(",")] if verbs else None
    return _gq.traverse(g, node_id, allowed_verbs=allowed, max_depth=max_depth, max_nodes=max_nodes)


@app.get("/api/graph/path")
def graph_path(
    source: str = Query(...),
    target: str = Query(...),
    verbs: Optional[str] = Query(None, description="Comma-separated verbs to allow"),
) -> Dict[str, Any]:
    """Shortest path between two nodes, optionally restricted to specific verbs."""
    g = _graph()
    allowed = [v.strip() for v in verbs.split(",")] if verbs else None
    return _gq.shortest_path(g, source, target, allowed_verbs=allowed)


from fastapi import Response  # noqa: E402

# Manifesto/enrichment data changes when we re-OCR or re-enrich a party. Browsers
# applying heuristic caching (no Cache-Control header) can serve a stale response
# for ~10 min, which masks server-side cache invalidations. Force always-revalidate.
_NO_BROWSER_CACHE = {"Cache-Control": "no-store, max-age=0"}


@app.get("/api/manifesto/{party_id}/{year}/sdg-alignment")
def manifesto_sdg_alignment(
    party_id: str,
    year: int,
    response: Response,
    refresh: bool = Query(False, description="Force recompute and overwrite cache"),
) -> Dict[str, Any]:
    """Pre-computed SDG coverage for a single party's manifesto in a given year.

    Sourced from the Knowledge Graph's `targets_goal` edges (weighted). Cached
    server-side indefinitely since manifestos are fixed for the cycle; pass
    `refresh=true` to recompute after a manifesto edit.
    """
    response.headers.update(_NO_BROWSER_CACHE)

    if not refresh:
        cached = _sdg.get_cached(party_id, year)
        if cached is not None:
            return {"party_id": party_id, "year": year, "cached": True, "coverage": cached}

    try:
        docs = list(
            _db.collection("manifesto_promises")
            .where(filter=FieldFilter("party_id", "==", party_id))
            .where(filter=FieldFilter("target_year", "==", year))
            .stream()
        )
    except (GoogleAPICallError, RetryError) as exc:
        raise HTTPException(status_code=503, detail="Firestore unavailable") from exc

    promises = [_doc_to_dict(d) for d in docs]
    if not promises:
        raise HTTPException(
            status_code=404,
            detail=f"No promises found for party={party_id}, year={year}",
        )

    g = _graph()
    coverage = _sdg.compute_party_alignment(g, promises)
    _sdg.set_cached(party_id, year, coverage)

    return {
        "party_id": party_id,
        "year": year,
        "cached": False,
        "promise_count": len(promises),
        "coverage": coverage,
    }


@app.get("/api/manifesto/sdg-alignment/cache")
def sdg_alignment_cache_stats() -> Dict[str, Any]:
    return _sdg.cache_stats()


@app.post("/api/manifesto/sdg-alignment/cache/clear")
def sdg_alignment_cache_clear(
    party_id: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
) -> Dict[str, Any]:
    removed = _sdg.clear_cache(party_id=party_id, year=year)
    return {"removed": removed, "stats": _sdg.cache_stats()}


@app.get("/api/graph/feasibility/{promise_id:path}")
def graph_feasibility(
    promise_id: str,
    state: str = Query("tamil_nadu"),
) -> Dict[str, Any]:
    """Walk a promise node through SDG → indicators → fiscal data and score feasibility."""
    g = _graph()
    # Accept either the bare doc_id (e.g., "dmk_2026_women_001") or the graph id.
    node_id = promise_id if promise_id.startswith("promise:") else f"promise:{promise_id}"
    if node_id not in g:
        raise HTTPException(status_code=404, detail=f"Unknown promise: {promise_id}")

    try:
        result = _gq.compute_feasibility(
            g,
            node_id,
            fetch_promise_doc=_fetch_promise_doc,
            fetch_state_finances_latest=lambda: _fetch_state_finances_latest(state),
            state_slug=state,
            fetch_indicator_snapshot=_fetch_indicator_snapshot,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (GoogleAPICallError, RetryError) as exc:
        raise HTTPException(status_code=503, detail="Firestore unavailable") from exc

    return jsonable_encoder(result.to_dict())


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


# ─────────────────────────────────────────────────────────────────────────────
# Politician Profiles — admin CRUD for the master person-identity table.
# ─────────────────────────────────────────────────────────────────────────────

_POLITICIAN_COL = "politician_profile"

# In-memory cache for politician profiles — avoids re-streaming the entire
# Firestore collection on every search request.  TTL = 5 minutes.
_politician_cache: Dict[str, Any] = {"docs": [], "ts": 0.0}
_POLITICIAN_CACHE_TTL = 300  # seconds


def _get_politician_docs() -> list[Dict[str, Any]]:
    """Return cached list of politician dicts, refreshing if stale."""
    now = _time.time()
    if _politician_cache["docs"] and now - _politician_cache["ts"] < _POLITICIAN_CACHE_TTL:
        return _politician_cache["docs"]
    col = _db.collection(_POLITICIAN_COL)
    docs = []
    for d in col.stream():
        data = d.to_dict() or {}
        data["doc_id"] = d.id
        docs.append(data)
    _politician_cache["docs"] = docs
    _politician_cache["ts"] = now
    return docs


def _extract_name_and_initials(name: str) -> tuple[str, str]:
    """Split a standardized name like 'Ajith Kumar A.J.' into:
       name_part = 'ajith kumar' (lowercased, for grouping)
       initials  = 'a.j.' (lowercased, for matching — empty string if no initials)
    """
    import re as _re
    s = _re.sub(r"[,;@]", " ", name.strip())
    s = " ".join(s.split())

    # Separate trailing initials (e.g., "A.J." at the end) from name parts.
    # Also handle title prefixes at the front (Dr., Adv., etc.) — keep in name.
    tokens = s.split()
    name_parts: list[str] = []
    init_parts: list[str] = []

    for t in tokens:
        clean = t.strip(".").strip()
        # Single letter or dotted cluster (A., M.K., A.R.S.)
        if _re.fullmatch(r"([A-Za-z]\.?)+", t.strip()) and len(clean.replace(".", "")) <= 3 and all(len(seg) <= 1 for seg in clean.split(".")):
            for ch in _re.findall(r"[A-Za-z]", t):
                init_parts.append(ch.lower())
        else:
            name_parts.append(clean.lower())

    return " ".join(name_parts), ".".join(init_parts) + ("." if init_parts else "")


@app.get("/api/politicians")
def list_politicians(
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=500),
    q: Optional[str] = Query(None, description="Search by name"),
    sort: str = Query("canonical_name"),
    order: str = Query("asc", pattern="^(asc|desc)$"),
    duplicates_only: bool = Query(False, description="Show only records with duplicate names"),
    needs_review: bool = Query(False, description="Show only records flagged for name review"),
    no_photo: bool = Query(False, description="Show only records without a profile picture"),
) -> Dict[str, Any]:
    """Paginated politician profiles with optional name search."""
    try:
        all_docs = _get_politician_docs()
        docs_list = []
        needle = (q or "").strip().lower()

        for data in all_docs:
            data = dict(data)  # shallow copy so we don't mutate cache
            if needle:
                name = (data.get("canonical_name") or "").lower()
                aliases = [a.lower() for a in (data.get("aliases") or [])]
                party = (data.get("current_party") or "").lower()
                constituency = (data.get("current_constituency") or "").lower()
                if not (needle in name or any(needle in a for a in aliases) or needle in party or needle in constituency):
                    continue
            # Compute latest_year for sorting
            tl = data.get("timeline") or data.get("elections") or []
            years = [e.get("year") for e in tl if e.get("year")]
            data["latest_year"] = max(years) if years else 0
            docs_list.append(data)

        # Needs-review filter
        if needs_review:
            docs_list = [d for d in docs_list if d.get("name_needs_review") is True]

        # No-photo filter
        if no_photo:
            docs_list = [d for d in docs_list if not d.get("photo_url")]

        # Strict duplicate filter — three rules must ALL be satisfied:
        #   1. Same name spelling + initials (no-initials matches any)
        #   2. Share at least one constituency_slug across timelines
        #   3. Same gender (null matches any)
        if duplicates_only:
            from collections import defaultdict as _defaultdict

            # Pre-compute per-doc: name_part, initials, constituency set, gender
            for d in docs_list:
                np, ini = _extract_name_and_initials(d.get("canonical_name") or "")
                d["_name_part"] = np
                d["_initials"] = ini
                tl = d.get("timeline") or d.get("elections") or []
                d["_constituencies"] = {e.get("constituency_slug") for e in tl if e.get("constituency_slug")}

            # Group by name_part (bare name without initials)
            groups: dict[str, list[Dict[str, Any]]] = _defaultdict(list)
            for d in docs_list:
                groups[d["_name_part"]].append(d)

            # Within each group, find records that form duplicate pairs
            dup_ids: set[str] = set()
            for name_key, members in groups.items():
                if len(members) < 2:
                    continue
                for i, a in enumerate(members):
                    for j, b in enumerate(members):
                        if i >= j:
                            continue
                        # Rule 1: initials must match, or one/both have no initials
                        a_ini = a["_initials"]
                        b_ini = b["_initials"]
                        if a_ini and b_ini and a_ini != b_ini:
                            continue
                        # Rule 2: share at least one constituency
                        if not (a["_constituencies"] & b["_constituencies"]):
                            continue
                        # Rule 3: same gender (null matches any)
                        a_gen = a.get("gender")
                        b_gen = b.get("gender")
                        if a_gen and b_gen and a_gen != b_gen:
                            continue
                        # All three rules satisfied — both are duplicates
                        dup_ids.add(a["doc_id"])
                        dup_ids.add(b["doc_id"])

            docs_list = [d for d in docs_list if d["doc_id"] in dup_ids]
            # Attach grouping key for sorted output
            for d in docs_list:
                d["_dedup_key"] = d["_name_part"]

            # Clean up temp fields
            for d in docs_list:
                d.pop("_name_part", None)
                d.pop("_initials", None)
                d.pop("_constituencies", None)

        # Sort — when showing duplicates, group by normalized name first so
        # matching entries sit together; user's chosen sort applies within groups.
        reverse = order == "desc"
        def sort_key(item: Dict[str, Any]) -> Any:
            val = item.get(sort)
            if val is None:
                return "" if not reverse else "zzz"
            if isinstance(val, (int, float)):
                return val
            return str(val).lower()

        if duplicates_only:
            docs_list.sort(key=lambda d: (d.get("_dedup_key", ""), sort_key(d)), reverse=reverse)
            for d in docs_list:
                d.pop("_dedup_key", None)
        else:
            docs_list.sort(key=sort_key, reverse=reverse)

        total = len(docs_list)
        start = (page - 1) * limit
        page_items = docs_list[start : start + limit]

        return {
            "total": total,
            "page": page,
            "limit": limit,
            "total_pages": (total + limit - 1) // limit,
            "items": jsonable_encoder(page_items),
        }
    except (GoogleAPICallError, RetryError) as exc:
        raise HTTPException(status_code=503, detail="Firestore unavailable") from exc


@app.get("/api/politicians/{doc_id}")
def get_politician(doc_id: str) -> Dict[str, Any]:
    """Get a single politician profile by doc_id."""
    try:
        ref = _db.collection(_POLITICIAN_COL).document(doc_id)
        doc = ref.get()
        if not doc.exists:
            raise HTTPException(status_code=404, detail=f"Profile not found: {doc_id}")
        data = doc.to_dict() or {}
        data["doc_id"] = doc.id
        return jsonable_encoder(data)
    except (GoogleAPICallError, RetryError) as exc:
        raise HTTPException(status_code=503, detail="Firestore unavailable") from exc


@app.get("/api/politicians/by-constituency/{slug}")
def get_politician_by_constituency(
    slug: str,
    year: int = Query(2026, ge=2006, le=2031),
    name: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """Find politician profiles that contested in a constituency for a given year.

    Returns a list of matches. If `name` is provided, returns the single best match.
    """
    try:
        all_docs = _get_politician_docs()
        matches = []

        # Normalize slug variants (handle _sc, hyphens, etc.)
        slug_variants = {slug, slug.replace("-", "_"), slug.replace("_", "-")}
        for sfx in ("_sc", "_st", "_gen"):
            if slug.endswith(sfx):
                slug_variants.add(slug[: -len(sfx)])

        for data in all_docs:
            for t in data.get("timeline", []):
                if t.get("year") != year:
                    continue
                t_slug = t.get("constituency_slug", "")
                t_variants = {t_slug, t_slug.replace("-", "_"), t_slug.replace("_", "-")}
                for sfx in ("_sc", "_st", "_gen"):
                    if t_slug.endswith(sfx):
                        t_variants.add(t_slug[: -len(sfx)])
                if not slug_variants & t_variants:
                    continue
                matches.append({
                    "doc_id": data.get("doc_id"),
                    "canonical_name": data.get("canonical_name"),
                    "photo_url": data.get("photo_url"),
                    "current_party": data.get("current_party"),
                    "constituency_slug": t_slug,
                    "won": t.get("won"),
                    "votes": t.get("votes"),
                })
                break

        if name:
            # Find best match by name
            from difflib import SequenceMatcher
            needle = re.sub(r"[.,]", " ", name.lower()).strip()
            best, best_score = None, 0.0
            for m in matches:
                cand = re.sub(r"[.,]", " ", (m["canonical_name"] or "").lower()).strip()
                score = SequenceMatcher(None, needle, cand).ratio()
                # Token overlap boost
                nt, ct = set(needle.split()), set(cand.split())
                if nt and ct and len(nt & ct) >= min(len(nt), len(ct)):
                    score = max(score, 0.85)
                if score > best_score:
                    best_score = score
                    best = m
            if best and best_score >= 0.5:
                return {"match": best, "score": round(best_score, 3)}
            raise HTTPException(status_code=404, detail=f"No matching profile for '{name}' in {slug}")

        return {"matches": matches, "count": len(matches)}
    except (GoogleAPICallError, RetryError) as exc:
        raise HTTPException(status_code=503, detail="Firestore unavailable") from exc


@app.delete("/api/politicians/{doc_id}")
def delete_politician(doc_id: str) -> Dict[str, Any]:
    """Delete a single politician profile."""
    try:
        ref = _db.collection(_POLITICIAN_COL).document(doc_id)
        doc = ref.get()
        if not doc.exists:
            raise HTTPException(status_code=404, detail=f"Profile not found: {doc_id}")
        ref.delete()
        _politician_cache["ts"] = 0.0  # invalidate cache
        return {"ok": True, "deleted": doc_id}
    except (GoogleAPICallError, RetryError) as exc:
        raise HTTPException(status_code=503, detail="Firestore unavailable") from exc


class MergeRequest(BaseModel):
    source_id: str
    target_id: str
    education_override: Optional[str] = None  # user-resolved when source/target disagree


@app.post("/api/politicians/merge")
def merge_politicians(payload: MergeRequest) -> Dict[str, Any]:
    """Merge source into target: copy elections, add name as alias, delete source."""
    try:
        src_ref = _db.collection(_POLITICIAN_COL).document(payload.source_id)
        tgt_ref = _db.collection(_POLITICIAN_COL).document(payload.target_id)
        src_doc = src_ref.get()
        tgt_doc = tgt_ref.get()
        if not src_doc.exists:
            raise HTTPException(status_code=404, detail=f"Source not found: {payload.source_id}")
        if not tgt_doc.exists:
            raise HTTPException(status_code=404, detail=f"Target not found: {payload.target_id}")

        src = src_doc.to_dict() or {}
        tgt = tgt_doc.to_dict() or {}

        # Step 1 — fill every gap in target with source's data. This ensures
        # no field is lost: if target has null/empty for any key and source
        # has a value, it carries over.
        SKIP_KEYS = {"doc_id", "created_at"}
        merged = dict(tgt)
        for key, val in src.items():
            if key in SKIP_KEYS:
                continue
            existing = merged.get(key)
            if existing is None or existing == "" or existing == []:
                merged[key] = val

        # Step 2 — merge timeline (union, avoid duplicating same year+constituency)
        tgt_timeline = list(tgt.get("timeline") or tgt.get("elections") or [])
        existing_keys = {(e.get("year"), e.get("constituency_slug")) for e in tgt_timeline}
        for e in (src.get("timeline") or src.get("elections") or []):
            if (e.get("year"), e.get("constituency_slug")) not in existing_keys:
                tgt_timeline.append(e)
        merged["timeline"] = tgt_timeline
        merged.pop("elections", None)  # drop legacy key if present

        # Step 3 — merge aliases (union + add source's canonical name)
        aliases = list(tgt.get("aliases") or [])
        src_name = (src.get("canonical_name") or "").strip()
        if src_name and src_name not in aliases and src_name != tgt.get("canonical_name"):
            aliases.append(src_name)
        for a in (src.get("aliases") or []):
            if a not in aliases and a != tgt.get("canonical_name"):
                aliases.append(a)
        merged["aliases"] = aliases

        # Step 4 — prefer non-null photo
        merged["photo_url"] = tgt.get("photo_url") or src.get("photo_url")

        # Step 5 — recompute aggregate fields from the full timeline
        wins = sum(1 for e in tgt_timeline if e.get("won") is True)
        losses = sum(1 for e in tgt_timeline if e.get("won") is False)
        sorted_el = sorted(tgt_timeline, key=lambda e: e.get("year") or 0, reverse=True)
        latest = sorted_el[0] if sorted_el else {}

        merged["win_count"] = wins
        merged["loss_count"] = losses
        merged["total_contested"] = len(tgt_timeline)
        merged["current_party"] = latest.get("party") or tgt.get("current_party") or src.get("current_party")
        merged["current_constituency"] = latest.get("constituency") or tgt.get("current_constituency") or src.get("current_constituency")
        merged["current_constituency_slug"] = latest.get("constituency_slug") or tgt.get("current_constituency_slug") or src.get("current_constituency_slug")

        # Assets & criminal: always from most recent election (these can go up or down)
        merged["total_assets_cr"] = latest.get("assets_cr") or tgt.get("total_assets_cr") or src.get("total_assets_cr")
        merged["total_liabilities_cr"] = latest.get("liabilities_cr") or tgt.get("total_liabilities_cr") or src.get("total_liabilities_cr")
        merged["net_assets_cr"] = tgt.get("net_assets_cr") or src.get("net_assets_cr")
        merged["criminal_cases_total"] = latest.get("criminal_cases_total") if latest.get("criminal_cases_total") is not None else (tgt.get("criminal_cases_total") or src.get("criminal_cases_total"))
        merged["criminal_severity"] = latest.get("criminal_severity") or tgt.get("criminal_severity") or src.get("criminal_severity")

        merged["gender"] = tgt.get("gender") or src.get("gender")

        # Age: take the highest (politician was younger in earlier elections)
        ages = [a for a in [tgt.get("age"), src.get("age")] if isinstance(a, (int, float))]
        merged["age"] = max(ages) if ages else None

        # Education: user-resolved override wins; otherwise fall back to most recent
        if payload.education_override:
            merged["education"] = payload.education_override
        else:
            merged["education"] = latest.get("education") or tgt.get("education") or src.get("education")

        # Write the fully merged doc (set, not update, so no field is left behind)
        merged.pop("doc_id", None)
        tgt_ref.set(merged)
        src_ref.delete()
        _politician_cache["ts"] = 0.0  # invalidate cache

        # Return the full merged target doc so the frontend can update in-place
        # without refetching the entire list.
        merged["doc_id"] = payload.target_id
        return jsonable_encoder({
            "ok": True,
            "merged": payload.source_id,
            "into": payload.target_id,
            "target": merged,
        })
    except (GoogleAPICallError, RetryError) as exc:
        raise HTTPException(status_code=503, detail="Firestore unavailable") from exc


# ── News API ─────────────────────────────────────────────────────────────────

@app.get("/api/news")
def get_news(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    category: Optional[str] = Query(None, description="OV category filter (POLITICS, BUSINESS, etc.)"),
    topic: Optional[str] = Query(None, description="NER topic filter"),
    sdg: Optional[str] = Query(None, description="SDG filter (e.g. SDG-4)"),
    entity: Optional[str] = Query(None, description="Filter to articles mentioning this entity node_id"),
    min_relevance: float = Query(0.0, ge=0.0, le=1.0),
) -> Dict[str, Any]:
    """Paginated news articles with NER-enriched metadata."""
    try:
        query = _db.collection("news_articles").order_by(
            "published_at", direction=firestore.Query.DESCENDING
        ).limit(500)
        docs = list(query.stream())

        articles = []
        for doc in docs:
            d = doc.to_dict()
            d["doc_id"] = doc.id

            if category and d.get("ov_category", "").upper() != category.upper():
                continue
            if min_relevance > 0 and d.get("relevance_to_tn", 0) < min_relevance:
                continue
            if topic:
                topics_lower = [t.lower() for t in d.get("topics", [])]
                if topic.lower() not in topics_lower:
                    continue
            if sdg:
                if sdg.upper() not in [s.upper() for s in d.get("sdg_alignment", [])]:
                    continue
            if entity:
                entity_ids = [e.get("node_id", "") for e in d.get("entities", [])]
                canonical_ids = [e.get("canonical_id", "") for e in d.get("entities", [])]
                if entity not in entity_ids and entity not in canonical_ids:
                    continue

            articles.append({
                "doc_id": d["doc_id"],
                "title": d.get("title", ""),
                "snippet": d.get("snippet", ""),
                "summary": d.get("one_line_summary", ""),
                "source_url": d.get("source_url", ""),
                "source_name": d.get("source_name", ""),
                "category": d.get("ov_category", ""),
                "topics": d.get("topics", []),
                "sdg_alignment": d.get("sdg_alignment", []),
                "sentiment": d.get("sentiment", 0),
                "relevance_to_tn": d.get("relevance_to_tn", 0),
                "is_breaking": d.get("is_breaking", False),
                "heat_score": d.get("heat_score", 0),
                "latitude": d.get("latitude"),
                "longitude": d.get("longitude"),
                "published_at": d.get("published_at", ""),
                "entity_count": len(d.get("entities", [])),
                "relation_count": len(d.get("relations", [])),
            })

        paginated = articles[offset : offset + limit]
        return {"total": len(articles), "offset": offset, "limit": limit, "articles": paginated}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/news/threads")
def get_news_threads(
    top_n: int = Query(10, ge=1, le=20, description="Number of anchor stories to thread"),
) -> Dict[str, Any]:
    """Build narrative threads starting from today's top stories.

    For each of the top N most recent articles (by heat_score, then recency),
    find all other articles that share at least one NER entity.
    Returns threads sorted by the anchor article's rank.
    """
    try:
        # Fetch all articles with entities
        all_docs = list(
            _db.collection("news_articles")
            .order_by("published_at", direction=firestore.Query.DESCENDING)
            .limit(500)
            .stream()
        )
        all_articles: List[Dict[str, Any]] = []
        for doc in all_docs:
            d = doc.to_dict()
            d["doc_id"] = doc.id
            all_articles.append(d)

        # Build entity → article index
        # Skip overly generic entities that connect everything
        STOP_ENTITIES = {
            "tamil_nadu", "india", "chennai", "tamil_nadu_assembly_elections",
            "state_government", "chief_minister", "assembly_elections",
            "tn_assembly_elections_2026", "election", "elections",
            "voters", "candidates", "political_parties",
            # Generic community/topic terms that link unrelated articles
            "public", "residents", "media", "journalists", "people",
            "government", "officials", "police", "youth", "women",
            "students", "citizens", "opposition", "ruling_party",
            "exit_polls", "political_reactions", "ground_reality",
            "tamil_nadu_assembly_election", "tamil_nadu_assembly",
        }

        # Only thread on high-signal entity types.
        # Place/Topic/Community/Resource are too generic — district names
        # appear in weather bulletins, election results, and power reports
        # as mere geographic tags, creating false threads.
        THREAD_ENTITY_TYPES = {"Person", "Party", "Policy", "Event", "Institution"}

        entity_to_articles: Dict[str, List[int]] = {}  # entity node_id → [article indices]
        for idx, a in enumerate(all_articles):
            for ent in a.get("entities", []):
                if ent.get("type", "") not in THREAD_ENTITY_TYPES:
                    continue
                nid = ent.get("node_id", "")
                cid = ent.get("canonical_id")
                key = cid if cid and cid != "null" else nid
                if key and key.lower().replace(" ", "_") not in STOP_ENTITIES:
                    entity_to_articles.setdefault(key, []).append(idx)

        # Also filter entities that appear in >60% of articles (too generic)
        max_frequency = len(all_articles) * 0.6
        entity_to_articles = {
            k: v for k, v in entity_to_articles.items()
            if len(v) <= max_frequency
        }

        # Pick anchor stories: most recent, weighted by heat_score
        # Sort by: heat_score desc, then published_at desc
        anchors = sorted(
            range(len(all_articles)),
            key=lambda i: (
                all_articles[i].get("heat_score", 0),
                all_articles[i].get("published_at", ""),
            ),
            reverse=True,
        )[:top_n]

        used_article_ids: set[str] = set()  # track which articles are already anchored
        used_thread_entities: set[str] = set()  # avoid duplicate thread labels
        threads: List[Dict[str, Any]] = []

        for anchor_idx in anchors:
            anchor = all_articles[anchor_idx]
            anchor_id = anchor["doc_id"]

            if anchor_id in used_article_ids:
                continue

            # Find all articles connected via shared high-signal entities
            anchor_entities = set()
            for ent in anchor.get("entities", []):
                if ent.get("type", "") not in THREAD_ENTITY_TYPES:
                    continue
                cid = ent.get("canonical_id")
                key = cid if cid and cid != "null" else ent.get("node_id", "")
                if key and key.lower().replace(" ", "_") not in STOP_ENTITIES:
                    anchor_entities.add(key)

            if not anchor_entities:
                continue

            # Collect connected articles — require 2+ shared entities
            # (not just 1) to ensure content relevance, not coincidence
            article_shared_count: Dict[int, int] = {}  # article_idx → number of shared entities
            shared_entities_map: Dict[str, int] = {}  # entity → count of articles sharing it
            for ent_key in anchor_entities:
                article_indices = entity_to_articles.get(ent_key, [])
                shared_entities_map[ent_key] = len(article_indices)
                for ai in article_indices:
                    article_shared_count[ai] = article_shared_count.get(ai, 0) + 1

            # Only include articles sharing 2+ entities with the anchor
            MIN_SHARED = 2
            connected_indices: set[int] = {anchor_idx}  # always include anchor
            for ai, count in article_shared_count.items():
                if count >= MIN_SHARED:
                    connected_indices.add(ai)

            # Build thread: anchor + connected, sorted chronologically
            thread_articles = []
            for idx in connected_indices:
                a = all_articles[idx]
                aid = a["doc_id"]
                is_anchor = (aid == anchor_id)
                thread_articles.append({
                    "doc_id": aid,
                    "title": a.get("title", ""),
                    "summary": a.get("one_line_summary", ""),
                    "snippet": a.get("snippet", ""),
                    "source_url": a.get("source_url", ""),
                    "source_name": a.get("source_name", ""),
                    "category": a.get("ov_category", ""),
                    "published_at": a.get("published_at", ""),
                    "heat_score": a.get("heat_score", 0),
                    "is_breaking": a.get("is_breaking", False),
                    "is_anchor": is_anchor,
                    "sentiment": a.get("sentiment", 0),
                    "sdg_alignment": a.get("sdg_alignment", []),
                })

            # Sort by published_at ascending (timeline left→right)
            thread_articles.sort(key=lambda x: x.get("published_at", ""))

            # Thread title: use the anchor article's title as the thread label
            # when it's a standalone (1 article) or small thread.
            # For larger threads, use the most prominent Person/Party entity.
            # This ensures the label always conveys the essence of the thread.

            if len(connected_indices) <= 2:
                # Small/standalone thread — the anchor title IS the thread
                thread_label = anchor.get("title", "")
                # Trim to a concise headline: take first clause
                for sep in [": ", " — ", " - ", " | ", "; "]:
                    if sep in thread_label:
                        thread_label = thread_label.split(sep)[0]
                        break
                # Cap at a word boundary
                if len(thread_label) > 45:
                    cut = thread_label[:45].rfind(" ")
                    if cut > 20:
                        thread_label = thread_label[:cut] + "..."
                    else:
                        thread_label = thread_label[:42] + "..."
                thread_entity = thread_label.lower().replace(" ", "_")
            else:
                # Larger thread — find the most prominent entity
                ENTITY_TYPE_RANK = {"Person": 0, "Party": 1, "Event": 2, "Institution": 3, "Policy": 4}
                best_entity_key = ""
                best_entity_label = ""
                best_score = (-1, -1)

                for ent in anchor.get("entities", []):
                    cid = ent.get("canonical_id")
                    key = cid if cid and cid != "null" else ent.get("node_id", "")
                    if key not in anchor_entities or key not in shared_entities_map:
                        continue
                    etype = ent.get("type", "")
                    type_rank = ENTITY_TYPE_RANK.get(etype, 9)
                    shared_count = shared_entities_map.get(key, 0)
                    score = (-type_rank, shared_count)
                    if score > best_score:
                        best_score = score
                        best_entity_key = key
                        best_entity_label = ent.get("name", key)

                thread_entity = best_entity_key or (max(shared_entities_map, key=shared_entities_map.get) if shared_entities_map else "")
                thread_label = best_entity_label or thread_entity

            # Clean up label: remove underscores, title-case
            thread_label = thread_label.replace("_", " ").strip()
            if thread_label == thread_label.lower():
                thread_label = thread_label.title()

            # Skip if this thread's label was already used (normalized)
            label_key = thread_label.lower().strip()
            if label_key in used_thread_entities:
                continue
            used_article_ids.add(anchor_id)
            used_thread_entities.add(label_key)

            threads.append({
                "anchor_title": anchor.get("title", ""),
                "thread_entity": thread_entity,
                "thread_label": thread_label,
                "article_count": len(thread_articles),
                "shared_entities": list(anchor_entities)[:10],
                "articles": thread_articles,
            })

        return {"threads": threads, "count": len(threads)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/news/{doc_id}")
def get_news_article(doc_id: str) -> Dict[str, Any]:
    """Single news article with full NER detail (entities + relations)."""
    try:
        doc = _db.collection("news_articles").document(doc_id).get()
        if not doc.exists:
            raise HTTPException(status_code=404, detail="Article not found")
        d = doc.to_dict()
        d["doc_id"] = doc.id
        return d
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/news/by-entity/{entity_id}")
def news_by_entity(
    entity_id: str,
    limit: int = Query(20, ge=1, le=100),
) -> Dict[str, Any]:
    """Find news articles mentioning a specific entity (politician, place, party, etc.)."""
    try:
        docs = list(
            _db.collection("news_articles")
            .order_by("published_at", direction=firestore.Query.DESCENDING)
            .limit(200)
            .stream()
        )
        matching = []
        for doc in docs:
            d = doc.to_dict()
            entities = d.get("entities", [])
            matched = any(
                e.get("node_id") == entity_id or e.get("canonical_id") == entity_id
                for e in entities
            )
            if matched:
                matching.append({
                    "doc_id": doc.id,
                    "title": d.get("title", ""),
                    "summary": d.get("one_line_summary", ""),
                    "source_url": d.get("source_url", ""),
                    "category": d.get("ov_category", ""),
                    "topics": d.get("topics", []),
                    "published_at": d.get("published_at", ""),
                    "sentiment": d.get("sentiment", 0),
                })
            if len(matching) >= limit:
                break
        return {"entity_id": entity_id, "total": len(matching), "articles": matching}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── News Knowledge Graph ─────────────────────────────────────────────────────

@app.get("/api/news-graph")
def get_news_graph() -> Dict[str, Any]:
    """Return the raw News KG payload (nodes + edges + meta)."""
    try:
        _, raw = _gq.load_news_graph(project_id=PROJECT_ID)
        return raw
    except RuntimeError:
        raise HTTPException(status_code=404, detail="News knowledge graph not available yet")


@app.post("/api/news-graph/cache/clear")
def news_graph_cache_clear() -> Dict[str, Any]:
    """Evict the in-memory News KG cache."""
    return _gq.clear_news_cache()


@app.get("/api/news-graph/neighbors/{node_id:path}")
def news_graph_neighbors(
    node_id: str,
    verb: Optional[str] = Query(None),
    direction: str = Query("out", pattern="^(in|out|both)$"),
    limit: int = Query(200, ge=1, le=1000),
) -> Dict[str, Any]:
    """1-hop neighbors in the News KG."""
    try:
        g, _ = _gq.load_news_graph(project_id=PROJECT_ID)
    except RuntimeError:
        raise HTTPException(status_code=404, detail="News KG not available")
    if node_id not in g:
        raise HTTPException(status_code=404, detail=f"Unknown node: {node_id}")
    node = {"id": node_id, **g.nodes[node_id]}
    results = _gq.neighbors(g, node_id, verb=verb, direction=direction, limit=limit)
    return {"node": node, "neighbors": results, "count": len(results)}


# ── District Collectors ───────────────────────────────────────────────────────

@app.get("/api/district-collectors/{district_slug}")
def district_collectors(district_slug: str) -> Dict[str, Any]:
    """Current + historical collectors for a district."""
    try:
        docs = list(
            _db.collection("district_collectors_profile")
            .where(filter=FieldFilter("district_slug", "==", district_slug))
            .stream()
        )
        if not docs:
            return {"current": None, "history": [], "count": 0}

        all_collectors = sorted(
            [{**d.to_dict(), "doc_id": d.id} for d in docs],
            key=lambda x: x.get("from_date") or "",
            reverse=True,
        )
        current = next((c for c in all_collectors if c.get("is_current")), None)
        return {"current": current, "history": all_collectors, "count": len(all_collectors)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── MLACDS Budget ─────────────────────────────────────────────────────────────

@app.get("/api/mlacds-budget")
def mlacds_budget() -> Dict[str, Any]:
    """MLA Constituency Development Scheme — all years (state-level, uniform allocation)."""
    try:
        docs = list(_db.collection("mlacds_budget").stream())
        budgets = sorted(
            [{**d.to_dict(), "doc_id": d.id} for d in docs],
            key=lambda x: x.get("doc_id", ""),
        )
        latest = budgets[-1] if budgets else None
        return {"budgets": budgets, "count": len(budgets), "latest": latest}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── State Budget Time-Series ──────────────────────────────────────────────────

@app.get("/api/state-budgets/{state_slug}/timeseries")
def state_budgets_timeseries(state_slug: str) -> Dict[str, Any]:
    """All available budget years for a state (up to 14 years for TN)."""
    # Map slug → state code prefix used in doc IDs
    slug_to_prefix: Dict[str, str] = {
        "tamil_nadu": "TN", "kerala": "KL", "karnataka": "KA",
        "andhra_pradesh": "AP", "telangana": "TS",
    }
    prefix = slug_to_prefix.get(state_slug)
    if not prefix:
        raise HTTPException(status_code=404, detail=f"No budget data for {state_slug}")

    try:
        docs = [
            d for d in _db.collection("state_budgets").stream()
            if d.id.startswith(f"{prefix}_")
        ]
        budgets = sorted(
            [{**d.to_dict(), "doc_id": d.id} for d in docs],
            key=lambda x: x.get("fiscal_year", x["doc_id"]),
        )
        return {"state": state_slug, "budgets": budgets, "count": len(budgets)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── News Ingestion Cron ──────────────────────────────────────────────────────

# ── Election Results 2026 ─────────────────────────────────────────────


@app.get("/api/results/summary")
def results_summary() -> Dict[str, Any]:
    """State-level election results summary: party-wise seats, total votes, etc."""
    doc = _db.collection("results_summary_2026").document("state").get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="Results summary not found")
    return doc.to_dict()


@app.get("/api/results/constituencies")
def results_constituencies(
    party: Optional[str] = Query(None),
    limit: int = Query(234, ge=1, le=234),
) -> Dict[str, Any]:
    """All constituency results, optionally filtered by winning party."""
    query = _db.collection("election_results_2026").order_by("ac_no")
    if party:
        query = query.where(filter=FieldFilter("winner.party", "==", party))
    docs = query.limit(limit).stream()
    results = []
    for doc in docs:
        d = doc.to_dict()
        # Slim down for list view — omit full candidate list
        results.append({
            "ac_no": d.get("ac_no"),
            "ac_name": d.get("ac_name"),
            "slug": d.get("slug"),
            "status": d.get("status"),
            "winner": d.get("winner"),
            "runner_up": d.get("runner_up"),
            "margin": d.get("margin"),
            "total_votes": d.get("total_votes"),
            "profile_match": d.get("profile_match"),
        })
    return {"constituencies": results, "count": len(results)}


@app.get("/api/results/{slug}")
def results_constituency(slug: str) -> Dict[str, Any]:
    """Full constituency result with all candidates."""
    doc = _db.collection("election_results_2026").document(slug).get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail=f"No results for constituency: {slug}")
    return doc.to_dict()


from fastapi import Header  # noqa: E402

_NEWS_INGEST_SECRET = os.getenv("NEWS_INGEST_SECRET", "")


@app.post("/tasks/news-ingest")
async def task_news_ingest(
    x_ingest_token: Optional[str] = Header(None, alias="X-Ingest-Token"),
    hours: int = Query(4, ge=1, le=48),
) -> Dict[str, Any]:
    """Cron-triggered news ingestion: fetch from OmnesVident, run NER, store.

    Protected by X-Ingest-Token header. Called by Cloud Scheduler every 3h.
    """
    if not _NEWS_INGEST_SECRET:
        raise HTTPException(status_code=503, detail="NEWS_INGEST_SECRET not configured")
    if x_ingest_token != _NEWS_INGEST_SECRET:
        raise HTTPException(status_code=401, detail="Invalid ingest token")

    import hashlib
    from datetime import datetime, timedelta, timezone

    ov_key = os.getenv("OMNES_VIDENT_API_KEY", "")
    if not ov_key:
        raise HTTPException(status_code=503, detail="OMNES_VIDENT_API_KEY not configured")

    OV_API_BASE = "https://omnesvident-api-naqkmfs2qa-uc.a.run.app"
    OV_CATEGORIES = {"POLITICS", "BUSINESS", "HEALTH", "SCIENCE_TECH", "WORLD", "ENTERTAINMENT", "SPORTS"}
    start_date = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    # 1. Fetch stories from OmnesVident (single call)
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            f"{OV_API_BASE}/v1/stories",
            params={"region": "IN-TN", "start_date": start_date, "limit": 200},
            headers={"x-api-key": ov_key},
        )
        resp.raise_for_status()
        all_stories = [
            s for s in resp.json().get("stories", [])
            if s.get("category", "").upper() in OV_CATEGORIES
        ]

    # 2. Dedup against existing (by ID + title similarity)
    import unicodedata as _ud
    def _norm_title(t: str) -> str:
        t = _ud.normalize("NFKD", t.lower())
        t = re.sub(r"[^\w\s]", "", t)
        return re.sub(r"\s+", " ", t).strip()

    existing_ids: set[str] = set()
    existing_titles: set[str] = set()
    for doc in _db.collection("news_articles").select(["ov_id", "title"]).stream():
        d = doc.to_dict()
        existing_ids.add(d.get("ov_id", ""))
        t = d.get("title", "")
        if t:
            existing_titles.add(_norm_title(t))

    new_stories = [s for s in all_stories if s["dedup_group_id"] not in existing_ids]

    # Title-similarity dedup within batch + against existing
    deduped: list = []
    batch_titles: set[str] = set(existing_titles)
    for s in new_stories:
        norm = _norm_title(s.get("title", ""))
        if not norm:
            deduped.append(s)
            continue
        if norm in batch_titles:
            continue
        # Token overlap check
        tokens = set(norm.split())
        is_dup = False
        for et in batch_titles:
            et_tokens = set(et.split())
            if et_tokens and tokens:
                overlap = len(tokens & et_tokens) / min(len(tokens), len(et_tokens))
                if overlap >= 0.90:
                    is_dup = True
                    break
        if not is_dup:
            batch_titles.add(norm)
            deduped.append(s)
    title_dupes_removed = len(new_stories) - len(deduped)
    new_stories = deduped

    if not new_stories:
        return {"status": "ok", "fetched": len(all_stories), "new": 0, "stored": 0, "title_dupes_removed": title_dupes_removed}

    # 3. Fetch full article texts via trafilatura
    full_texts: Dict[str, str] = {}
    try:
        import trafilatura
        import concurrent.futures

        def _extract(url: str) -> Optional[str]:
            try:
                dl = trafilatura.fetch_url(url)
                if dl:
                    text = trafilatura.extract(dl, include_comments=False, include_tables=False, no_fallback=True)
                    return text[:3000] if text and len(text) > 50 else None
            except Exception:
                pass
            return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            futures = {pool.submit(_extract, s.get("source_url", "")): s["dedup_group_id"] for s in new_stories}
            for fut in concurrent.futures.as_completed(futures):
                sid = futures[fut]
                result = fut.result()
                if result:
                    full_texts[sid] = result
    except ImportError:
        pass  # trafilatura not installed — use snippet only

    # 4. Run NER via Gemini
    ner_results: list[Dict[str, Any]] = []
    try:
        from google import genai
        from google.genai import types as genai_types

        from backend_api.news_ner import load_reference_data, build_ner_system_prompt, NER_RESPONSE_SCHEMA

        ref_data = load_reference_data()
        system_prompt = build_ner_system_prompt(ref_data)
        gemini_client = genai.Client(vertexai=True, project=PROJECT_ID, location="us-central1")

        for i in range(0, len(new_stories), 10):
            batch = new_stories[i : i + 10]
            article_inputs = [{
                "id": a["dedup_group_id"], "title": a["title"],
                "body": full_texts.get(a["dedup_group_id"], a.get("snippet", "")),
                "category": a.get("category", ""), "source": a.get("source_name", ""),
                "timestamp": a.get("timestamp", ""), "region": a.get("region_code", ""),
            } for a in batch]

            config = genai_types.GenerateContentConfig(
                system_instruction=system_prompt,
                response_mime_type="application/json",
                response_schema=NER_RESPONSE_SCHEMA,
                temperature=0.1,
            )
            try:
                gemini_resp = await gemini_client.aio.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=(
                        "Extract entities and relations from these articles. "
                        "Return one result per article in the same order.\n\n"
                        + json.dumps(article_inputs, separators=(",", ":"))
                    ),
                    config=config,
                )
                ner_results.extend(json.loads(gemini_resp.text).get("articles", []))
            except Exception:
                pass  # Store without NER; retry-empty fixes later
    except Exception:
        pass

    # 5. Store in Firestore
    ner_by_id = {r["article_id"]: r for r in ner_results}
    fb = _db.batch()
    batch_count = 0
    stored = 0

    for story in new_stories:
        sid = story["dedup_group_id"]
        ner = ner_by_id.get(sid, {})
        doc_data = {
            "ov_id": sid,
            "title": story["title"],
            "snippet": story.get("snippet", ""),
            "full_text": full_texts.get(sid, ""),
            "source_url": story.get("source_url", ""),
            "source_name": story.get("source_name", ""),
            "region_code": story.get("region_code", ""),
            "ov_category": story.get("category", ""),
            "latitude": story.get("latitude"),
            "longitude": story.get("longitude"),
            "is_breaking": story.get("is_breaking", False),
            "heat_score": story.get("heat_score", 0),
            "published_at": story.get("timestamp", ""),
            "ov_processed_at": story.get("processed_at", ""),
            "entities": ner.get("entities", []),
            "relations": ner.get("relations", []),
            "topics": ner.get("topics", []),
            "sdg_alignment": ner.get("sdg_alignment", []),
            "sentiment": ner.get("sentiment", 0.0),
            "relevance_to_tn": ner.get("relevance_to_tn", 0.0),
            "one_line_summary": ner.get("one_line_summary", ""),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
        # Use title hash as doc ID to prevent exact-title duplicates across different ov_ids
        title_hash = hashlib.md5(_norm_title(story.get("title", "")).encode()).hexdigest()[:16]
        fb.set(_db.collection("news_articles").document(title_hash), doc_data)
        batch_count += 1
        stored += 1
        if batch_count >= 400:
            fb.commit()
            fb = _db.batch()
            batch_count = 0

    if batch_count > 0:
        fb.commit()

    # 6. Run KG-enriched consolidation for newly stored articles
    consolidated = 0
    try:
        from google import genai as _genai
        from google.genai import types as _genai_types

        _g_client = _genai.Client(vertexai=True, project=PROJECT_ID, location="us-central1")

        # Load KG for context
        try:
            _kg, _ = _gq.load_news_graph(PROJECT_ID)
        except Exception:
            _kg = None

        for story in new_stories:
            sid = story["dedup_group_id"]
            ner = ner_by_id.get(sid, {})
            title = story.get("title", "")
            summary = ner.get("one_line_summary", "") or story.get("snippet", "")
            entities = ner.get("entities", [])

            if not entities:
                continue

            # Get KG context
            kg_context_parts = []
            if _kg:
                for e in entities[:8]:
                    nid = e.get("canonical_id") or e.get("node_id", "")
                    if nid and nid in _kg:
                        for _, tgt, data in list(_kg.out_edges(nid, data=True))[:10]:
                            tgt_label = _kg.nodes[tgt].get("label", tgt) if tgt in _kg else tgt
                            src_label = _kg.nodes[nid].get("label", nid)
                            kg_context_parts.append(f"- {src_label} {data.get('verb', '')} {tgt_label}")
                        for src, _, data in list(_kg.in_edges(nid, data=True))[:10]:
                            src_label = _kg.nodes[src].get("label", src) if src in _kg else src
                            tgt_label = _kg.nodes[nid].get("label", nid)
                            kg_context_parts.append(f"- {src_label} {data.get('verb', '')} {tgt_label}")

            kg_context = "KNOWLEDGE GRAPH FACTS:\n" + "\n".join(list(dict.fromkeys(kg_context_parts))[:20]) if kg_context_parts else ""

            prompt = f"""You are a Tamil news anchor. Write a contextual news script using today's article and any relevant past information.

TODAY'S ARTICLE:
Title: {title}
Summary: {summary}

{kg_context if kg_context else "(No related past information available)"}

RULES:
- IMPORTANT: Start with today's article content FIRST, then weave in relevant background
- Include past events ONLY if they explain WHY or HOW today's news happened
- Just matching an entity name is NOT enough — there must be a causal or narrative link
- If nothing from history is contextually relevant, just summarize today's article as-is
- Write 2-4 sentences, naturally as spoken language

JSON output (nothing else):
{{"script_ta": "Tamil script", "script_en": "English script", "used_context": [{{"ref_title": "past article title", "relation": "consequence|continuation|background", "confidence": 0.0-1.0}}]}}

Note: In used_context, include ONLY past articles you actually referenced. If none, return []."""

            try:
                _cons_resp = await _g_client.aio.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt,
                    config=_genai_types.GenerateContentConfig(temperature=0.4, response_mime_type="application/json"),
                )
                _cons_result = json.loads(_cons_resp.text.strip())
                doc_id = hashlib.md5(sid.encode()).hexdigest()[:16]
                _db.collection("news_articles").document(doc_id).update({
                    "consolidated_script_ta": _cons_result.get("script_ta", ""),
                    "consolidated_script_en": _cons_result.get("script_en", ""),
                    "contextual_history": _cons_result.get("used_context", []),
                })
                consolidated += 1
            except Exception:
                pass
    except Exception:
        pass

    return {
        "status": "ok",
        "fetched": len(all_stories),
        "new": len(new_stories),
        "stored": stored,
        "ner_extracted": len(ner_results),
        "full_text_fetched": len(full_texts),
        "consolidated": consolidated,
    }
