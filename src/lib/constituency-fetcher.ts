/**
 * constituency-fetcher.ts
 *
 * Fires three parallel Firestore queries for the constituency drill-down page:
 *   1. MLA winner record from candidate_accountability
 *   2. District-level socio-economic metrics from socio_economics (falls back to state-level)
 *   3. Manifesto promises for the incumbent's party from manifesto_promises
 *
 * Queries #1 and #2 run in parallel via Promise.all.
 */

import {
  collection,
  documentId,
  doc,
  getDoc,
  getDocs,
  limit,
  query,
  where,
} from "firebase/firestore";
import { db } from "@/lib/firebase";
import type { MlaRecord, SocioMetric, ManifestoPromise } from "@/lib/types";
import constituencyMap from "@/lib/constituency-map.json";

type MapEntry = {
  name: string;
  tamil_name?: string;
  district: string;
  district_slug: string;
  constituency_id: number;
};

const MAP = constituencyMap as Record<string, MapEntry>;

// Metrics shown on the constituency drill-down (quick gauges)
const KEY_METRIC_IDS = [
  "aser2024_std3_reading_recovery",
  "nfhs5_stunting_under5",
  "nfhs5_anaemia_women",
  "industrial_corridors_district_coverage",
];

const METRIC_ORDER = KEY_METRIC_IDS.reduce<Record<string, number>>((acc, id, idx) => {
  acc[id] = idx;
  return acc;
}, {});

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
}

/**
 * Derives the party_id slug from the raw party name stored in candidate_accountability.
 * Covers the parties that have manifesto_promises in Firestore.
 */
function partyIdFromName(partyName: string): string {
  const name = partyName.trim().toUpperCase();
  if (name.includes("DMK") && !name.includes("AIADMK") && !name.includes("ADMK")) return "dmk";
  if (name.includes("AIADMK") || name.includes("ADMK")) return "aiadmk";
  if (name.includes("BJP") || name.includes("BHARATIYA JANATA")) return "bjp";
  if (name.includes("PMK")) return "pmk";
  if (name.includes("CONGRESS") || name.includes("INC")) return "inc";
  // Unknown party — return lowercase slug so the promises query simply returns []
  return name.toLowerCase().replace(/[^a-z0-9]/g, "_");
}

function sortMetrics(metrics: SocioMetric[]): SocioMetric[] {
  return [...metrics].sort((a, b) => {
    const ai = METRIC_ORDER[a.metric_id] ?? 999;
    const bi = METRIC_ORDER[b.metric_id] ?? 999;
    if (ai !== bi) return ai - bi;
    return a.metric_id.localeCompare(b.metric_id);
  });
}

function sortPromises(promises: ManifestoPromise[]): ManifestoPromise[] {
  const pillarOrder = {
    Agriculture: 0,
    Education: 1,
    "TASMAC & Revenue": 2,
    "Women's Welfare": 3,
    Infrastructure: 4,
  } as const;

  return [...promises].sort((a, b) => {
    const ap = pillarOrder[a.category] ?? 99;
    const bp = pillarOrder[b.category] ?? 99;
    if (ap !== bp) return ap - bp;
    return a.doc_id.localeCompare(b.doc_id);
  });
}

async function fetchMlaByConstituency(
  constituencySlug: string,
  constituencyId: number | null
): Promise<MlaRecord | null> {
  const col = collection(db, "candidate_accountability");

  // Preferred pivot 1: constituency_id (future-proof once docs carry this field)
  if (typeof constituencyId === "number") {
    const byId = await getDocs(
      query(col, where("constituency_id", "==", constituencyId), limit(1))
    );
    if (!byId.empty) return byId.docs[0].data() as MlaRecord;
  }

  // Preferred pivot 2: constituency_slug (future-proof once docs carry this field)
  const bySlug = await getDocs(
    query(col, where("constituency_slug", "==", constituencySlug), limit(1))
  );
  if (!bySlug.empty) return bySlug.docs[0].data() as MlaRecord;

  // Current canonical fallback: deterministic doc id (2021_<slug>)
  const mlaDocId = `2021_${constituencySlug}`;
  const byDocIdQuery = await getDocs(
    query(col, where(documentId(), "==", mlaDocId), limit(1))
  );
  if (!byDocIdQuery.empty) return byDocIdQuery.docs[0].data() as MlaRecord;

  // Last fallback: direct doc read
  const byDocId = await getDoc(doc(db, "candidate_accountability", mlaDocId));
  return byDocId.exists() ? (byDocId.data() as MlaRecord) : null;
}

async function fetchSocioMetricsForDistrict(
  districtSlug: string | null
): Promise<{ metrics: SocioMetric[]; metrics_scope: MetricsScope }> {
  const col = collection(db, "socio_economics");

  // Attempt district-level metrics first.
  if (districtSlug) {
    const districtSnap = await getDocs(
      query(col, where("district_slug", "==", districtSlug))
    );
    const districtMetrics = districtSnap.docs
      .map((d) => d.data() as SocioMetric)
      .filter((m) => KEY_METRIC_IDS.includes(m.metric_id));

    if (districtMetrics.length > 0) {
      return {
        metrics: sortMetrics(districtMetrics),
        metrics_scope: "district",
      };
    }
  }

  // Fallback: current state-level socio collection.
  const stateSnap = await getDocs(query(col));
  const stateMetrics = stateSnap.docs
    .map((d) => d.data() as SocioMetric)
    .filter((m) => KEY_METRIC_IDS.includes(m.metric_id));

  return {
    metrics: sortMetrics(stateMetrics),
    metrics_scope: "state_fallback",
  };
}

/**
 * Fetch all three data streams in parallel.
 *
 * @param constituencySlug  URL slug e.g. "harur_sc" (matches 2021_<slug> doc_id pattern)
 */
export async function fetchConstituencyData(
  constituencySlug: string
): Promise<ConstituencyDrillData> {
  const mapEntry = MAP[constituencySlug];
  const district_meta: DistrictMeta | null = mapEntry
    ? {
        constituency_name: mapEntry.name,
        constituency_id: mapEntry.constituency_id,
        district_name: mapEntry.district,
        district_slug: mapEntry.district_slug,
      }
    : null;

  const [mla, socio] = await Promise.all([
    fetchMlaByConstituency(constituencySlug, district_meta?.constituency_id ?? null),
    fetchSocioMetricsForDistrict(district_meta?.district_slug ?? null),
  ]);

  const metrics = socio.metrics;

  // Only fetch promises if we know the party
  let promises: ManifestoPromise[] = [];
  if (mla) {
    const partyId = mla.party_id ?? partyIdFromName(mla.party);
    const promisesSnap = await getDocs(
      query(collection(db, "manifesto_promises"), where("party_id", "==", partyId))
    );
    promises = sortPromises(
      promisesSnap.docs.map((d) => d.data() as ManifestoPromise)
    );
  }

  return {
    mla,
    metrics,
    metrics_scope: socio.metrics_scope,
    district_meta,
    promises,
  };
}
