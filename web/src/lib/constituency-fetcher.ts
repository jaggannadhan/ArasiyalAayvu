import { apiGet } from "@/lib/api-client";
import type { MlaRecord, SocioMetric, ManifestoPromise, LsConstituencyMeta, DistrictWaterRisk, DistrictCrimeIndex, DistrictRoadSafety, WardMapping, UlbCouncillor, UlbHead } from "@/lib/types";

type MetricsScope = "district" | "state_fallback";

interface DistrictMeta {
  constituency_name: string;
  constituency_id: number | null;
  district_name: string;
  district_slug: string;
}

export interface ConstituencyDrillData {
  mla: MlaRecord | null;
  metrics: SocioMetric[];
  metrics_scope: MetricsScope;
  district_meta: DistrictMeta | null;
  promises: ManifestoPromise[];
  parent_ls: LsConstituencyMeta | null;
  district_water_risk: DistrictWaterRisk | null;
  district_crime_index: DistrictCrimeIndex | null;
  district_road_safety: DistrictRoadSafety | null;
  ward_mapping: WardMapping | null;
  ulb_councillors: UlbCouncillor[];
  ulb_heads: UlbHead[];
}

// In-memory cache for the browser session.
// Keyed by "slug:termYear". In-flight map prevents duplicate concurrent fetches.
const _cache = new Map<string, ConstituencyDrillData>();
const _inFlight = new Map<string, Promise<ConstituencyDrillData>>();

export async function fetchConstituencyData(
  constituencySlug: string,
  termYear: number = 2021
): Promise<ConstituencyDrillData> {
  if (!constituencySlug) {
    throw new Error("Missing constituency slug");
  }

  const key = `${constituencySlug}:${termYear}`;

  const cached = _cache.get(key);
  if (cached) return cached;

  const inFlight = _inFlight.get(key);
  if (inFlight) return inFlight;

  const promise = apiGet<ConstituencyDrillData>(
    `/api/constituency/${encodeURIComponent(constituencySlug)}?term=${termYear}`
  ).then((data) => {
    _cache.set(key, data);
    _inFlight.delete(key);
    return data;
  }).catch((err) => {
    _inFlight.delete(key);
    throw err;
  });

  _inFlight.set(key, promise);
  return promise;
}
