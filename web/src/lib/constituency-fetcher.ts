import { cacheFetch, cachePeek } from "@/lib/data-cache";
import type { MlaRecord, SocioMetric, ManifestoPromise, LsConstituencyMeta, DistrictWaterRisk, DistrictCrimeIndex, DistrictRoadSafety, WardMapping, UlbCouncillor, UlbHead } from "@/lib/types";

type MetricsScope = "district" | "state_fallback";

interface DistrictMeta {
  constituency_name: string;
  constituency_id: number | null;
  district_name: string;
  district_slug: string;
}

export interface ElectionResult2026 {
  winner: { name: string; party: string; votes: number; photo_url?: string } | null;
  runner_up: { name: string; party: string; votes: number; photo_url?: string } | null;
  margin: number;
  total_votes: number;
  status: string;
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
  election_result_2026: ElectionResult2026 | null;
}

/** Canonical API path — also the cache key. */
export function constituencyUrl(slug: string, term: number = 2021): string {
  return `/api/constituency/${encodeURIComponent(slug)}?term=${term}`;
}

/** Synchronous read from the shared cache — useful during render so components
 *  can show data immediately if it's already warm (via prior visit, prefetch,
 *  or localStorage hydration with performance-cookie consent). */
export function peekConstituencyData(
  slug: string,
  term: number = 2021,
): ConstituencyDrillData | undefined {
  if (!slug) return undefined;
  return cachePeek<ConstituencyDrillData>(constituencyUrl(slug, term));
}

export async function fetchConstituencyData(
  constituencySlug: string,
  termYear: number = 2021,
): Promise<ConstituencyDrillData> {
  if (!constituencySlug) {
    throw new Error("Missing constituency slug");
  }
  return cacheFetch<ConstituencyDrillData>(constituencyUrl(constituencySlug, termYear));
}
