"use client";

import { useState } from "react";
import Image from "next/image";
import type { MlaRecord } from "@/lib/types";
import { CandidateCriminalModal } from "./CandidateCriminalModal";
import { normalizeName } from "@/lib/formatters";

interface MlaCardProps {
  mla: MlaRecord;
  district: string;
  lang?: "en" | "ta";
  winnerVotes?: number;
  winnerPct?: number;
}

const PARTY_FLAG_EXT: Record<string, string> = {
  dmk: "svg", aiadmk: "svg", bjp: "svg", inc: "svg", pmk: "svg",
  cpi: "svg", cpim: "png", vck: "png", dmdk: "png", mdmk: "svg",
  ntk: "gif", tvk: "jpeg",
};

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

export function MlaCard({ mla, district, lang = "en", winnerVotes, winnerPct }: MlaCardProps) {
  const isTA = lang === "ta";
  const partyId = mla.party_id ?? partyIdFromName(mla.party);
  const severity = SEVERITY_META[mla.criminal_severity as keyof typeof SEVERITY_META] ?? SEVERITY_META.CLEAN;
  const [modalOpen, setModalOpen] = useState(false);
  const [assetsExpanded, setAssetsExpanded] = useState(false);
  const hasAssetBreakdown = mla.movable_assets_cr != null || mla.immovable_assets_cr != null;
  // Use parsed case count when available — single source of truth for what's displayed
  const caseCount = mla.criminal_cases && mla.criminal_cases.length > 0
    ? mla.criminal_cases.length
    : mla.criminal_cases_total;

  return (
    <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-5">
      {/* Title row: [photo] [name + constituency] [party flag] */}
      <div className="flex items-center gap-4">
        {/* MLA photo */}
        {/* eslint-disable-next-line @next/next/no-img-element */}
        <img
          src={mla.photo_url ?? "/default-mla.svg"}
          alt={mla.photo_url ? `${mla.mla_name} profile` : "MLA placeholder"}
          className="shrink-0 w-16 h-20 rounded-xl object-cover border border-gray-200 shadow-sm bg-gray-50 flex-none"
        />

        {/* Name + constituency */}
        <div className="flex-1 min-w-0">
          <p className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-0.5">
            {isTA ? "சட்டமன்ற உறுப்பினர்" : "Elected MLA"}
          </p>
          <h2 className="text-lg font-black text-gray-900 leading-tight">{normalizeName(mla.mla_name)}</h2>
          <p className="text-xs text-gray-500 mt-0.5">{district} {isTA ? "மாவட்டம்" : "District"}</p>
          {winnerVotes != null && (
            <p className="text-xs text-indigo-600 font-semibold mt-1">
              {isTA ? "பெற்ற வாக்குகள்:" : "Secured"}{" "}
              {winnerVotes.toLocaleString("en-IN")} {isTA ? "" : "votes"}
              {winnerPct != null && <span className="text-gray-400 font-normal"> · {winnerPct}%</span>}
            </p>
          )}
        </div>

        {/* Party flag — far right */}
        {PARTY_FLAG_EXT[partyId] && (
          <div className="shrink-0 flex flex-col items-center gap-1">
            {/* eslint-disable-next-line @next/next/no-img-element */}
            <img
              src={`/party-flags/${partyId}.${PARTY_FLAG_EXT[partyId]}`}
              alt={mla.party}
              className="w-12 h-8 object-contain rounded-sm"
            />
            <span className="text-[9px] font-bold text-gray-500 uppercase tracking-wide">{mla.party}</span>
          </div>
        )}
      </div>

      {/* Stats row */}
      <div className="mt-4 grid grid-cols-3 gap-3">
        {/* Criminal record — clickable badge opens detail modal */}
        <div className="flex flex-col gap-1">
          <p className="text-xs text-gray-500">{isTA ? "வழக்குகள்" : "Cases"}</p>
          <button
            onClick={() => setModalOpen(true)}
            className={`text-xs font-semibold px-2 py-1 rounded-lg border text-center transition-opacity hover:opacity-80 ${severity.color}`}
          >
            {!caseCount
              ? (isTA ? severity.label_ta : severity.label_en)
              : `${caseCount} ${isTA ? severity.label_ta : severity.label_en}`}
          </button>
        </div>

        {/* Total assets — clickable if movable/immovable breakdown exists */}
        <div className="flex flex-col gap-1">
          <p className="text-xs text-gray-500">{isTA ? "மொத்த சொத்து" : "Total Assets"}</p>
          {hasAssetBreakdown ? (
            <button
              onClick={() => setAssetsExpanded((v) => !v)}
              className="text-left group"
            >
              <p className="text-sm font-bold text-gray-900 group-hover:underline decoration-dotted underline-offset-2">
                {mla.assets_cr != null ? `₹${mla.assets_cr.toFixed(2)} Cr` : "—"}
                <span className="ml-1 text-gray-400 text-xs">{assetsExpanded ? "▲" : "▼"}</span>
              </p>
              {mla.is_crorepati && (
                <p className="text-xs text-amber-700">{isTA ? "கோடீஸ்வரர்" : "Crorepati"}</p>
              )}
            </button>
          ) : (
            <div>
              <p className="text-sm font-bold text-gray-900">
                {mla.assets_cr != null ? `₹${mla.assets_cr.toFixed(2)} Cr` : "—"}
              </p>
              {mla.is_crorepati && (
                <p className="text-xs text-amber-700">{isTA ? "கோடீஸ்வரர்" : "Crorepati"}</p>
              )}
            </div>
          )}
        </div>

        {/* Education */}
        <div className="flex flex-col gap-1">
          <p className="text-xs text-gray-500">{isTA ? "கல்வி" : "Education"}</p>
          <p className="text-xs font-medium text-gray-800 leading-tight">{mla.education_tier}</p>
        </div>
      </div>

      {/* Asset breakdown — expands when Total Assets is tapped */}
      {assetsExpanded && hasAssetBreakdown && (
        <div className="mt-3 grid grid-cols-2 gap-2 rounded-xl bg-gray-50 border border-gray-100 px-4 py-3">
          <div>
            <p className="text-xs text-gray-500">{isTA ? "நடமாடும் சொத்து" : "Movable"}</p>
            <p className="text-sm font-bold text-gray-900">
              {mla.movable_assets_cr != null ? `₹${mla.movable_assets_cr.toFixed(2)} Cr` : "—"}
            </p>
          </div>
          <div>
            <p className="text-xs text-gray-500">{isTA ? "நிலையான சொத்து" : "Immovable"}</p>
            <p className="text-sm font-bold text-gray-900">
              {mla.immovable_assets_cr != null ? `₹${mla.immovable_assets_cr.toFixed(2)} Cr` : "—"}
            </p>
          </div>
          {mla.liabilities_cr != null && mla.liabilities_cr > 0 && (
            <div className="col-span-2 border-t border-gray-200 pt-2 mt-1">
              <p className="text-xs text-gray-500">{isTA ? "கடன்கள்" : "Liabilities"}</p>
              <p className="text-sm font-bold text-red-600">
                − ₹{mla.liabilities_cr.toFixed(2)} Cr
              </p>
            </div>
          )}
        </div>
      )}

      {/* Institution name (below education_tier) */}
      {mla.institution_name && (
        <p className="mt-2 text-xs text-gray-500">
          🏫 {mla.institution_name}
        </p>
      )}

      {/* View ECI Affidavit link */}
      {mla.source_pdf && (
        <div className="mt-4 border-t border-gray-100 pt-4">
          <a
            href={mla.source_pdf}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1.5 text-xs font-semibold px-3 py-1.5 rounded-lg border border-gray-300 text-gray-700 hover:bg-gray-50 transition-colors"
          >
            📄 {isTA ? "தேர்தல் உறுதிமொழி ஆவணம்" : "View ECI Affidavit"}
          </a>
        </div>
      )}

      {/* Criminal detail modal — portal-rendered, scroll-locked */}
      <CandidateCriminalModal
        mla={mla}
        isOpen={modalOpen}
        onClose={() => setModalOpen(false)}
        lang={lang}
      />
    </div>
  );
}
