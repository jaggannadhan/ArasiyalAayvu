import { apiGet } from "@/lib/api-client";
import type { MlaRecord, SocioMetric, ManifestoPromise, LsConstituencyMeta } from "@/lib/types";

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
}

export async function fetchConstituencyData(
  constituencySlug: string
): Promise<ConstituencyDrillData> {
  if (!constituencySlug) {
    throw new Error("Missing constituency slug");
  }

  return apiGet<ConstituencyDrillData>(
    `/api/constituency/${encodeURIComponent(constituencySlug)}`
  );
}
