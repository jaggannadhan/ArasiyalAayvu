"use client";

import { useEffect, useState } from "react";
import { apiGet } from "@/lib/api-client";
import type { DistrictCollector } from "@/lib/types";

interface CollectorResponse {
  current: DistrictCollector | null;
  history: DistrictCollector[];
  count: number;
}

function tenureDuration(from: string, to: string | null): string {
  const start = new Date(from);
  const end = to ? new Date(to) : new Date();
  const months = Math.round((end.getTime() - start.getTime()) / (30 * 24 * 60 * 60 * 1000));
  if (months < 12) return `${months}m`;
  const years = Math.floor(months / 12);
  const rem = months % 12;
  return rem > 0 ? `${years}y ${rem}m` : `${years}y`;
}

function titleCase(s: string): string {
  return s.toLowerCase().replace(/(^|\s)\S/g, (c) => c.toUpperCase());
}

interface Props {
  districtSlug: string;
  lang?: "en" | "ta";
}

export function CollectorCard({ districtSlug, lang = "en" }: Props) {
  const isTA = lang === "ta";
  const [data, setData] = useState<CollectorResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [showHistory, setShowHistory] = useState(false);

  useEffect(() => {
    if (!districtSlug) return;
    setLoading(true);
    apiGet<CollectorResponse>(`/api/district-collectors/${encodeURIComponent(districtSlug)}`)
      .then(setData)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [districtSlug]);

  if (loading) {
    return (
      <div className="space-y-2">
        <div className="h-4 w-40 bg-gray-100 rounded animate-pulse" />
        <div className="h-16 bg-gray-50 rounded-xl animate-pulse" />
      </div>
    );
  }

  if (!data?.current) return null;

  const c = data.current;
  const pastCollectors = data.history.filter((h) => !h.is_current).slice(0, 10);

  return (
    <div className="space-y-3">
      <p className="text-xs font-bold text-gray-500 uppercase tracking-wide">
        {isTA ? "மாவட்ட நிர்வாகம்" : "District Administration"}
      </p>

      {/* Current collector */}
      <div className="bg-gray-50 rounded-xl p-4 border border-gray-100">
        <div className="flex items-start gap-3">
          {/* Photo or avatar */}
          {c.photo_url ? (
            // eslint-disable-next-line @next/next/no-img-element
            <img
              src={c.photo_url}
              alt={c.name}
              className="w-14 h-16 object-cover rounded-lg shadow-sm shrink-0"
            />
          ) : (
            <div className="w-14 h-16 bg-gray-200 rounded-lg flex items-center justify-center text-gray-400 text-lg shrink-0">
              🏛️
            </div>
          )}

          <div className="flex-1 min-w-0">
            <p className="text-[10px] font-bold text-blue-600 uppercase tracking-wider">
              {isTA ? "மாவட்ட ஆட்சியர்" : "District Collector"} · IAS
            </p>
            <h3 className="text-sm font-black text-gray-900 truncate mt-0.5">
              {titleCase(c.name)}
            </h3>
            <p className="text-xs text-gray-500 mt-0.5">
              {isTA ? "பதவியில்:" : "Since:"} {new Date(c.from_date).toLocaleDateString("en-IN", { month: "short", year: "numeric" })}
              {" · "}{tenureDuration(c.from_date, null)}
            </p>

            {/* Details row */}
            <div className="flex flex-wrap gap-x-3 gap-y-1 mt-2">
              {c.ias_batch_year && (
                <span className="text-[10px] text-gray-500">
                  <span className="font-semibold">Batch:</span> {c.ias_batch_year}
                </span>
              )}
              {c.educational_qualification && (
                <span className="text-[10px] text-gray-500 truncate max-w-[200px]">
                  <span className="font-semibold">{isTA ? "கல்வி:" : "Edu:"}</span> {c.educational_qualification}
                </span>
              )}
            </div>

            {c.contact_email && (
              <p className="text-[10px] text-blue-500 mt-1 truncate">
                📧 {c.contact_email}
              </p>
            )}
          </div>
        </div>
      </div>

      {/* Previous collectors (expandable) */}
      {pastCollectors.length > 0 && (
        <div>
          <button
            onClick={() => setShowHistory(!showHistory)}
            className="text-[10px] font-semibold text-gray-400 hover:text-gray-600 transition-colors"
          >
            {showHistory ? "▼" : "▶"} {isTA ? "முந்தைய ஆட்சியர்கள்" : "Previous Collectors"} ({pastCollectors.length})
          </button>
          {showHistory && (
            <div className="mt-2 space-y-1 max-h-40 overflow-y-auto">
              {pastCollectors.map((h) => (
                <div key={h.doc_id} className="flex items-center justify-between py-1.5 px-3 bg-gray-50 rounded-lg text-xs">
                  <span className="font-medium text-gray-700 truncate flex-1">{titleCase(h.name)}</span>
                  <span className="text-[10px] text-gray-400 shrink-0 ml-2">
                    {h.from_date?.slice(0, 4)}–{h.to_date?.slice(0, 4) ?? "present"}
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
