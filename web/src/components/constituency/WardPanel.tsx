"use client";

import { useState } from "react";
import type { WardMapping, UlbCouncillor, UlbHead } from "@/lib/types";

interface WardPanelProps {
  wardMapping: WardMapping | null;
  ulbHeads?: UlbHead[];
  ulbCouncillors?: UlbCouncillor[];
  selectedTerm?: number;
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
  if (!party) return null;
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

// Map state assembly term → the local body election year shown
const TERM_TO_COUNCIL_YEAR: Record<number, number> = {
  2021: 2022,
  2011: 2011,  // GCC elected Oct 2011, served through the 2011–2016 assembly term
};

const COUNCIL_NOTE: Record<number, { en: string; ta: string }> = {
  2022: {
    en: "2022 election results · OpenCity / LGD",
    ta: "2022 தேர்தல் முடிவுகள் · OpenCity / LGD",
  },
  2011: {
    en: "2011 GCC election · OpenCity / LGD · Names only (party data not in source)",
    ta: "2011 GCC தேர்தல் · OpenCity / LGD · பெயர்கள் மட்டும் (கட்சி தரவு இல்லை)",
  },
};

// Per-term message shown when there is no elected local body (administrator rule, etc.)
const NO_COUNCIL_MSG: Record<number, { en: string; ta: string }> = {
  2016: {
    en: "Chennai Corporation was under administrator rule from 2016–2022. No ward councillors were elected during the 2016–2021 state assembly term.",
    ta: "2016–2022 காலத்தில் சென்னை மாநகராட்சி நிர்வாகி ஆட்சியில் இருந்தது. 2016–2021 சட்டமன்றக் காலத்தில் வார்டு உறுப்பினர்கள் யாரும் தேர்ந்தெடுக்கப்படவில்லை.",
  },
  2006: {
    en: "Local body election data for the 2006–2011 period is not available in public datasets.",
    ta: "2006–2011 காலத்திற்கான உள்ளாட்சித் தேர்தல் தரவு கிடைக்கவில்லை.",
  },
};

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
    ward: "Ward",
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
    ward: "வார்டு",
  },
} as const;

export function WardPanel({
  wardMapping,
  ulbHeads = [],
  ulbCouncillors = [],
  selectedTerm = 2021,
  lang = "en",
}: WardPanelProps) {
  const t = T[lang];
  const [expanded, setExpanded] = useState(false);
  const councilYear = TERM_TO_COUNCIL_YEAR[selectedTerm];
  const councilNote = councilYear ? COUNCIL_NOTE[councilYear]?.[lang] : undefined;
  const noCouncilMsg = !councilYear ? NO_COUNCIL_MSG[selectedTerm]?.[lang] : undefined;
  const hasCouncilData = ulbCouncillors.length > 0 || ulbHeads.length > 0;

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

      {/* ── Local Bodies list (shown only when no head data available) ── */}
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

      {/* ── No elected body for this term (e.g. administrator rule) ── */}
      {!isRural && noCouncilMsg && (
        <p className="text-[11px] text-gray-500 leading-relaxed">
          {noCouncilMsg}
        </p>
      )}

      {/* ── Ward Councillors (with full data) ── */}
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
              {councilNote && (
                <p className="text-[10px] text-gray-400 italic px-1 mb-2">{councilNote}</p>
              )}
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

      {/* ── Ward Numbers only (no councillor data available) ── */}
      {!isRural && sortedCouncillors.length === 0 && councilYear && !noCouncilMsg &&
        wardMapping.local_bodies.some((lb) => (lb.ward_numbers?.length ?? 0) > 0) && (
        <div>
          <button
            onClick={() => setExpanded((v) => !v)}
            className="w-full flex items-center justify-between px-3 py-2 rounded-xl border border-gray-200 hover:bg-gray-50 transition-colors text-left"
          >
            <span className="text-xs font-semibold text-gray-700">
              🗺 {lang === "ta" ? "வார்டு எண்கள்" : "Ward Numbers"} ({wardMapping.total_urban_wards})
            </span>
            <span className="text-gray-400 text-xs select-none">{expanded ? "▲" : "▼"}</span>
          </button>

          {expanded && (
            <div className="mt-2 space-y-3">
              <p className="text-[10px] text-gray-400 italic px-1">
                {lang === "ta"
                  ? "வார்டு எண்கள் LGD தரவிலிருந்து · தேர்தல் முடிவுகள் கிடைக்கவில்லை"
                  : "Ward numbers from LGD · Election results not available"}
              </p>
              {wardMapping.local_bodies.map((lb) => {
                const nums = lb.ward_numbers ?? [];
                if (nums.length === 0) return null;
                return (
                  <div key={lb.name}>
                    {wardMapping.local_bodies.length > 1 && (
                      <p className="text-[10px] font-bold text-gray-400 uppercase tracking-wide px-1 mb-1">
                        {lb.name}
                      </p>
                    )}
                    <div className="flex flex-wrap gap-1.5">
                      {nums.map((n) => (
                        <span
                          key={n}
                          className="text-[11px] font-semibold text-gray-600 bg-gray-100 px-2 py-0.5 rounded-md"
                        >
                          {lang === "ta" ? `வ. ${n}` : `W${n}`}
                        </span>
                      ))}
                    </div>
                  </div>
                );
              })}
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
