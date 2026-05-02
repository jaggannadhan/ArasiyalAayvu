"use client";

import { useEffect, useState } from "react";
import { apiGet } from "@/lib/api-client";
import type { MLACDSBudget } from "@/lib/types";

interface MLACDSResponse {
  budgets: MLACDSBudget[];
  count: number;
  latest: MLACDSBudget | null;
}

const PARTY_COLOR: Record<string, string> = {
  DMK: "border-l-red-600",
  AIADMK: "border-l-green-600",
  "DMK+": "border-l-red-600",
  "AIADMK+": "border-l-green-600",
};

function fCr(v: number | null | undefined): string {
  if (v == null) return "—";
  return "₹" + v.toLocaleString("en-IN", { maximumFractionDigits: 2 }) + " Cr";
}

interface Props {
  lang?: "en" | "ta";
}

export function MLACDSCard({ lang = "en" }: Props) {
  const isTA = lang === "ta";
  const [data, setData] = useState<MLACDSResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState(false);

  useEffect(() => {
    apiGet<MLACDSResponse>("/api/mlacds-budget")
      .then(setData)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-5 space-y-3">
        <div className="h-4 w-48 bg-gray-100 rounded animate-pulse" />
        <div className="h-16 bg-gray-50 rounded-xl animate-pulse" />
      </div>
    );
  }

  if (!data || !data.latest) return null;

  const latest = data.latest;
  const budgets = data.budgets;
  const partyColor = PARTY_COLOR[latest.ruling_party ?? ""] ?? "border-l-gray-400";

  // Build sparkline data
  const maxAlloc = Math.max(...budgets.map((b) => b.per_constituency_allocation_cr));

  return (
    <div className={`bg-white rounded-2xl border border-gray-200 shadow-sm overflow-hidden border-l-4 ${partyColor}`}>
      {/* Header */}
      <div className="px-5 py-4 border-b border-gray-100">
        <p className="text-[10px] font-bold text-gray-400 uppercase tracking-wider">
          {isTA ? "சட்டமன்ற உறுப்பினர் தொகுதி வளர்ச்சி நிதி" : "MLA Constituency Development Scheme"}
        </p>
        <div className="flex items-baseline gap-2 mt-1">
          <p className="text-2xl font-black text-gray-900">
            {fCr(latest.per_constituency_allocation_cr)}
          </p>
          <p className="text-xs text-gray-500">
            {isTA ? "ஒரு தொகுதிக்கு" : "per constituency"} · {latest.doc_id ?? latest.fiscal_year}
          </p>
        </div>
      </div>

      {/* Key stats */}
      <div className="px-5 py-3 grid grid-cols-3 gap-3 text-center border-b border-gray-100">
        <div>
          <p className="text-[10px] text-gray-500 font-semibold">
            {isTA ? "ஒதுக்கீடு (மொத்தம்)" : "State Total"}
          </p>
          <p className="text-sm font-black text-gray-900">{fCr(latest.state_total_allocation_cr)}</p>
          <p className="text-[9px] text-gray-400">234 {isTA ? "தொகுதிகள்" : "constituencies"}</p>
        </div>
        <div>
          <p className="text-[10px] text-gray-500 font-semibold">
            {isTA ? "கட்டுப்பாடு நிதி" : "Tied Fund"}
          </p>
          <p className="text-sm font-black text-gray-900">{fCr(latest.tied_fund_cr)}</p>
          <p className="text-[9px] text-gray-400">{isTA ? "கட்டாய பயன்" : "Mandatory use"}</p>
        </div>
        <div>
          <p className="text-[10px] text-gray-500 font-semibold">
            {isTA ? "விலக்கு நிதி" : "Untied Fund"}
          </p>
          <p className="text-sm font-black text-gray-900">{fCr(latest.untied_fund_cr)}</p>
          <p className="text-[9px] text-gray-400">{isTA ? "சட்டமன்ற உறுப்பினர் தீர்மானம்" : "MLA's discretion"}</p>
        </div>
      </div>

      {/* SC/ST earmark */}
      {latest.sc_st_earmark_pct != null && (
        <div className="px-5 py-2 bg-amber-50 border-b border-gray-100">
          <p className="text-[10px] font-semibold text-amber-700">
            {latest.sc_st_earmark_pct}% {isTA ? "SC/ST பகுதிகளுக்கு ஒதுக்கீடு" : "earmarked for SC/ST areas"}
          </p>
        </div>
      )}

      {/* Mini sparkline — 15-year allocation trend */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full px-5 py-3 hover:bg-gray-50 transition-colors text-left"
      >
        <div className="flex items-center justify-between mb-2">
          <p className="text-[10px] font-bold text-gray-500 uppercase tracking-wider">
            {isTA ? "ஒதுக்கீடு போக்கு" : "Allocation Trend"} ({budgets.length} {isTA ? "ஆண்டுகள்" : "years"})
          </p>
          <span className="text-[10px] text-gray-400">{expanded ? "▲" : "▼"}</span>
        </div>
        <div className="flex items-end gap-[3px] h-10">
          {budgets.map((b, i) => {
            const pct = maxAlloc > 0 ? (b.per_constituency_allocation_cr / maxAlloc) * 100 : 0;
            const isCurrent = i === budgets.length - 1;
            return (
              <div key={i} className="flex-1 flex flex-col items-center gap-0.5">
                <div
                  className={`w-full rounded-sm ${isCurrent ? "bg-blue-500" : "bg-gray-300"}`}
                  style={{ height: `${Math.max(pct, 8)}%` }}
                />
              </div>
            );
          })}
        </div>
        <div className="flex justify-between mt-1">
          <span className="text-[8px] text-gray-400">{budgets[0]?.doc_id?.slice(0, 4)}</span>
          <span className="text-[8px] text-gray-400">{budgets[budgets.length - 1]?.doc_id?.slice(0, 4)}</span>
        </div>
      </button>

      {/* Expanded: tied components + performance */}
      {expanded && (
        <div className="px-5 pb-4 space-y-3 border-t border-gray-100 pt-3">
          {/* Tied components */}
          {latest.tied_components && Object.keys(latest.tied_components).length > 0 && (
            <div>
              <p className="text-[10px] font-bold text-gray-500 uppercase mb-2">
                {isTA ? "கட்டுப்பாடு நிதி பிரிவுகள்" : "Tied Fund Categories"}
              </p>
              <div className="grid grid-cols-2 gap-2">
                {Object.entries(latest.tied_components).map(([k, v]) => (
                  <div key={k} className="bg-gray-50 rounded-lg px-3 py-1.5">
                    <p className="text-[10px] text-gray-500 capitalize">{k.replace(/_/g, " ")}</p>
                    <p className="text-xs font-bold text-gray-900">{fCr(v as number)}</p>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Year-by-year table */}
          <div>
            <p className="text-[10px] font-bold text-gray-500 uppercase mb-2">
              {isTA ? "ஆண்டு வாரியாக" : "Year-by-Year"}
            </p>
            <div className="space-y-1 max-h-48 overflow-y-auto">
              {[...budgets].reverse().map((b) => (
                <div key={b.doc_id} className="flex items-center justify-between py-1 text-xs">
                  <span className="text-gray-600 font-medium">{b.doc_id}</span>
                  <span className="font-bold text-gray-900">{fCr(b.per_constituency_allocation_cr)}</span>
                  <span className="text-[10px] text-gray-400">{b.ruling_party ?? ""}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Footer */}
      <div className="px-4 py-2 bg-gray-50 border-t border-gray-100">
        <p className="text-[9px] text-gray-400 text-center">
          {isTA
            ? "ஒதுக்கீடு அனைத்து 234 தொகுதிகளுக்கும் சமமாக உள்ளது · ஆதாரம்: TNRD"
            : "Allocation is uniform across all 234 constituencies · Source: TNRD Policy Notes"}
        </p>
      </div>
    </div>
  );
}
