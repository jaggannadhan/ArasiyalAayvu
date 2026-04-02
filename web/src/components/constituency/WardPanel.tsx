"use client";

import { useState } from "react";
import type { WardMapping, UlbCouncillor, UlbHead } from "@/lib/types";

interface WardPanelProps {
  wardMapping: WardMapping | null;
  ulbHeads?: UlbHead[];
  ulbCouncillors?: UlbCouncillor[];
  lang?: "en" | "ta";
}

const PARTY_COLORS: Record<string, string> = {
  DMK:       "bg-red-100 text-red-800",
  ADMK:      "bg-green-100 text-green-800",
  AIADMK:    "bg-green-100 text-green-800",
  BJP:       "bg-orange-100 text-orange-800",
  INC:       "bg-blue-100 text-blue-800",
  "CPI (M)": "bg-rose-100 text-rose-800",
  CPI:       "bg-rose-100 text-rose-800",
  PMK:       "bg-yellow-100 text-yellow-800",
  VCK:       "bg-purple-100 text-purple-800",
  IND:       "bg-gray-100 text-gray-700",
};

function partyChip(party: string) {
  const cls = PARTY_COLORS[party] ?? "bg-gray-100 text-gray-700";
  return (
    <span className={`inline-block text-[10px] font-bold px-1.5 py-0.5 rounded ${cls}`}>
      {party}
    </span>
  );
}

function typeShort(type: string): string {
  const t = type.toLowerCase();
  if (t.includes("corporation")) return "Mpl. Corp.";
  if (t.includes("municipality")) return "Municipality";
  if (t.includes("town panchayat") || t.includes("town")) return "Town Panchayat";
  return type;
}

const T = {
  en: {
    title: "Local Governance",
    noUrban: "Rural constituency — no urban wards in LGD data",
    wardCount: (n: number) => `${n}w`,
    sourceNote: "Source: LGD — GoI",
    dataDate: (d: string) => `Data as of ${d}`,
    urbanOnly: "Urban wards only · Rural panchayat wards not included",
    totalWards: "wards",
    wardCouncillors: "Ward Councillors",
    councilNote: "2022 election results · OpenCity / LGD",
    ward: "Ward",
    showCouncillors: (n: number) => `Show ${n} ward councillors`,
    hideCouncillors: "Hide councillors",
  },
  ta: {
    title: "உள்ளாட்சி அமைப்பு",
    noUrban: "கிராமப்புற தொகுதி — LGD தரவில் நகர்ப்புற வார்டுகள் இல்லை",
    wardCount: (n: number) => `${n}வ`,
    sourceNote: "ஆதாரம்: LGD — GoI",
    dataDate: (d: string) => `${d} நிலவரம்`,
    urbanOnly: "நகர்ப்புற வார்டுகள் மட்டும் · கிராம பஞ்சாயத்து வார்டுகள் இல்லை",
    totalWards: "வார்டுகள்",
    wardCouncillors: "வார்டு உறுப்பினர்கள்",
    councilNote: "2022 தேர்தல் முடிவுகள் · OpenCity / LGD",
    ward: "வார்டு",
    showCouncillors: (n: number) => `${n} வார்டு உறுப்பினர்களை காட்டு`,
    hideCouncillors: "மறை",
  },
} as const;

export function WardPanel({
  wardMapping,
  ulbHeads = [],
  ulbCouncillors = [],
  lang = "en",
}: WardPanelProps) {
  const t = T[lang];
  const [expanded, setExpanded] = useState(false);

  if (!wardMapping) return null;

  const isRural = wardMapping.total_urban_wards === 0;
  const sortedCouncillors = [...ulbCouncillors].sort((a, b) => a.ward_number - b.ward_number);

  return (
    <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-5 space-y-4">
      {/* ── Header ── */}
      <div className="flex items-start justify-between gap-2">
        <div>
          <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
            🏙 {t.title}
          </h3>
          {!isRural && (
            <p className="text-xs text-gray-400 mt-0.5">
              {wardMapping.total_urban_wards} {t.totalWards} ·{" "}
              {wardMapping.local_bodies.length === 1
                ? wardMapping.local_bodies[0].name
                : `${wardMapping.local_bodies.length} local bodies`}
            </p>
          )}
          {isRural && <p className="text-xs text-gray-400 mt-0.5">{t.noUrban}</p>}
        </div>
        {!isRural && (
          <div className="shrink-0 text-right">
            <p className="text-2xl font-black text-gray-900 leading-none">
              {wardMapping.total_urban_wards}
            </p>
            <p className="text-[10px] text-gray-400 uppercase tracking-wide">
              {lang === "ta" ? "வார்டுகள்" : "wards"}
            </p>
          </div>
        )}
      </div>

      {/* ── Local Body Head (Mayor / Chairman) ── */}
      {ulbHeads.length > 0 && (
        <div className="space-y-2">
          {ulbHeads.map((head) => (
            <div
              key={head.local_body_slug}
              className="flex items-center justify-between gap-3 px-3 py-2.5 rounded-xl bg-gray-50 border border-gray-100"
            >
              <div className="min-w-0">
                <p className="text-[10px] font-bold text-gray-400 uppercase tracking-wider">
                  {head.head_title} · {typeShort(head.local_body_type)}
                </p>
                <p className="text-sm font-black text-gray-900 leading-tight mt-0.5">
                  {head.head_name}
                </p>
                {head.notes && (
                  <p className="text-[10px] text-gray-400 mt-0.5 leading-tight">{head.notes}</p>
                )}
              </div>
              <div className="shrink-0">{partyChip(head.party)}</div>
            </div>
          ))}
        </div>
      )}

      {/* ── Local Bodies list (shown only when no head data) ── */}
      {!isRural && ulbHeads.length === 0 && wardMapping.local_bodies.length > 0 && (
        <div className="space-y-1.5">
          {wardMapping.local_bodies.map((lb) => (
            <div
              key={lb.name}
              className="flex items-center justify-between gap-2 px-3 py-2 rounded-lg bg-gray-50 border border-gray-100"
            >
              <div className="min-w-0">
                <p className="text-xs font-semibold text-gray-800 truncate">{lb.name}</p>
                <p className="text-[10px] text-gray-400">{typeShort(lb.type)}</p>
              </div>
              <span className="shrink-0 text-xs font-bold text-gray-600 bg-gray-200 px-2 py-0.5 rounded-full">
                {t.wardCount(lb.ward_count)}
              </span>
            </div>
          ))}
        </div>
      )}

      {/* ── Ward Councillors ── */}
      {sortedCouncillors.length > 0 && (
        <div>
          <button
            onClick={() => setExpanded((v) => !v)}
            className="w-full flex items-center justify-between px-3 py-2 rounded-xl border border-gray-200 hover:bg-gray-50 transition-colors text-left"
          >
            <span className="text-xs font-semibold text-gray-700">
              👥 {t.wardCouncillors} ({sortedCouncillors.length})
            </span>
            <span className="text-gray-400 text-xs select-none">{expanded ? "▲" : "▼"}</span>
          </button>

          {expanded && (
            <div className="mt-2">
              <p className="text-[10px] text-gray-400 italic px-1 mb-2">{t.councilNote}</p>
              <div className="rounded-xl border border-gray-100 overflow-hidden">
                {sortedCouncillors.map((c, i) => (
                  <div
                    key={c.ward_number}
                    className={`flex items-center justify-between px-3 py-2.5 ${
                      i < sortedCouncillors.length - 1 ? "border-b border-gray-100" : ""
                    }`}
                  >
                    <div className="flex items-center gap-2.5 min-w-0">
                      <span className="shrink-0 text-[10px] font-bold text-gray-400 w-6 text-right">
                        {c.ward_number}
                      </span>
                      <div className="min-w-0">
                        <p className="text-xs font-semibold text-gray-800 truncate">
                          {c.councillor_name}
                        </p>
                        {c.ward_reservation && c.ward_reservation !== "General" && (
                          <p className="text-[10px] text-gray-400">{c.ward_reservation}</p>
                        )}
                      </div>
                    </div>
                    {partyChip(c.party)}
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── Footer ── */}
      <p className="text-[10px] text-gray-400 border-t border-gray-100 pt-3">
        {t.sourceNote} · {wardMapping.data_date ? t.dataDate(wardMapping.data_date) : ""} · {t.urbanOnly}
      </p>
    </div>
  );
}
