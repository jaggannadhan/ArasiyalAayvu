"use client";

import Image from "next/image";
import type { MlaRecord } from "@/lib/types";
import { PARTIES } from "@/lib/types";

interface MlaCardProps {
  mla: MlaRecord;
  district: string;
  lang?: "en" | "ta";
}

const SEVERITY_META = {
  CLEAN:    { label_en: "Clean Record",   label_ta: "தூய்மையான பதிவு", color: "bg-emerald-100 text-emerald-800 border-emerald-200" },
  MINOR:    { label_en: "Minor Cases",    label_ta: "சிறிய வழக்குகள்",  color: "bg-yellow-100 text-yellow-800 border-yellow-200" },
  MODERATE: { label_en: "Moderate Cases", label_ta: "நடுத்தர வழக்குகள்", color: "bg-orange-100 text-orange-800 border-orange-200" },
  SERIOUS:  { label_en: "Serious Cases",  label_ta: "தீவிர வழக்குகள்",  color: "bg-red-100 text-red-800 border-red-200" },
};

function partyIdFromName(name: string): string {
  const n = name.trim().toUpperCase();
  if (n.includes("DMK") && !n.includes("AIADMK") && !n.includes("ADMK")) return "dmk";
  if (n.includes("AIADMK") || n.includes("ADMK")) return "aiadmk";
  if (n.includes("BJP")) return "bjp";
  if (n.includes("PMK")) return "pmk";
  if (n.includes("CONGRESS") || n === "INC") return "inc";
  return n.toLowerCase().replace(/[^a-z0-9]/g, "_");
}

export function MlaCard({ mla, district, lang = "en" }: MlaCardProps) {
  const isTA = lang === "ta";
  const partyId = mla.party_id ?? partyIdFromName(mla.party);
  const partyMeta = PARTIES[partyId];
  const severity = SEVERITY_META[mla.criminal_severity];

  // Initials avatar from MLA name
  const initials = mla.mla_name
    .split(/[\s.]+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((w) => w[0].toUpperCase())
    .join("");

  return (
    <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-5">
      {/* Title row */}
      <div className="flex items-start gap-4">
        {/* Avatar */}
        {mla.photo_url ? (
          <Image
            src={mla.photo_url}
            alt={`${mla.mla_name} profile`}
            width={56}
            height={56}
            unoptimized
            sizes="56px"
            className="shrink-0 w-14 h-14 rounded-full object-cover border border-gray-200"
          />
        ) : (
          <div className={`shrink-0 w-14 h-14 rounded-full flex items-center justify-center text-white text-lg font-black ${partyMeta?.color ?? "bg-gray-400"}`}>
            {initials}
          </div>
        )}

        <div className="flex-1 min-w-0">
          <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-0.5">
            {isTA ? "தேர்ந்தெடுக்கப்பட்ட சட்டமன்ற உறுப்பினர் · 2021" : "Elected MLA · 2021"}
          </p>
          <h2 className="text-lg font-black text-gray-900 leading-tight truncate">{mla.mla_name}</h2>
          <div className="flex flex-wrap items-center gap-2 mt-1">
            <span className={`text-xs font-bold px-2.5 py-0.5 rounded-full text-white ${partyMeta?.color ?? "bg-gray-500"}`}>
              {isTA ? (partyMeta?.tamil_name ?? mla.party) : mla.party}
            </span>
            <span className="text-xs text-gray-500">{district} {isTA ? "மாவட்டம்" : "District"}</span>
          </div>
        </div>
      </div>

      {mla.criminal_cases_total > 0 && (
        <div className="mt-3 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700 font-medium">
          {isTA
            ? `⚠ ${mla.criminal_cases_total} குற்ற வழக்குகள் பதிவாகியுள்ளன`
            : `⚠ High Alert: ${mla.criminal_cases_total} criminal case(s) declared`}
        </div>
      )}

      {/* Stats row */}
      <div className="mt-4 grid grid-cols-3 gap-3">
        {/* Criminal record */}
        <div className="flex flex-col gap-1">
          <p className="text-xs text-gray-500">{isTA ? "வழக்குகள்" : "Cases"}</p>
          <span className={`text-xs font-semibold px-2 py-1 rounded-lg border text-center ${severity.color}`}>
            {mla.criminal_cases_total === 0
              ? (isTA ? severity.label_ta : severity.label_en)
              : `${mla.criminal_cases_total} ${isTA ? severity.label_ta : severity.label_en}`}
          </span>
        </div>

        {/* Net assets */}
        <div className="flex flex-col gap-1">
          <p className="text-xs text-gray-500">{isTA ? "நிகர சொத்து" : "Net Assets"}</p>
          <div>
            <p className="text-sm font-bold text-gray-900">
              {mla.net_assets_cr != null
                ? `₹${mla.net_assets_cr.toFixed(2)} Cr`
                : "—"}
            </p>
            {mla.is_crorepati && (
              <p className="text-xs text-amber-700">{isTA ? "கோடீஸ்வரர்" : "Crorepati"}</p>
            )}
          </div>
        </div>

        {/* Education */}
        <div className="flex flex-col gap-1">
          <p className="text-xs text-gray-500">{isTA ? "கல்வி" : "Education"}</p>
          <p className="text-xs font-medium text-gray-800 leading-tight">{mla.education_tier}</p>
        </div>
      </div>

      {/* Assets bar */}
      {mla.assets_cr != null && mla.assets_cr > 0 && (
        <div className="mt-4">
          <div className="flex justify-between text-xs text-gray-500 mb-1">
            <span>{isTA ? "மொத்த சொத்துக்கள்" : "Total Assets"}</span>
            <span>₹{mla.assets_cr.toFixed(2)} Cr</span>
          </div>
          <div className="w-full h-2 bg-gray-100 rounded-full overflow-hidden">
            {/* Bar scaled relative to 50 Cr as max for visual comparison */}
            <div
              className={`h-full rounded-full ${partyMeta?.color ?? "bg-gray-400"}`}
              style={{ width: `${Math.min((mla.assets_cr / 50) * 100, 100)}%` }}
            />
          </div>
          {mla.liabilities_cr != null && mla.liabilities_cr > 0 && (
            <p className="text-xs text-gray-400 mt-1">
              {isTA ? "கடன்கள்" : "Liabilities"}: ₹{mla.liabilities_cr.toFixed(2)} Cr
            </p>
          )}
        </div>
      )}
    </div>
  );
}
