"use client";

import { useState } from "react";
import type { DistrictWaterRisk, DistrictCrimeIndex, DistrictRoadSafety } from "@/lib/types";

interface TenurePulseProps {
  districtName: string;
  waterRisk: DistrictWaterRisk | null;
  crimeIndex: DistrictCrimeIndex | null;
  roadSafety: DistrictRoadSafety | null;
  lang?: "en" | "ta";
}

const WATER_RISK_INDICATOR: Record<string, { dot: string; label_en: string; label_ta: string }> = {
  EXTREMELY_HIGH: { dot: "🔴", label_en: "Extremely High", label_ta: "மிக அதிக ஆபத்து" },
  HIGH:           { dot: "🔴", label_en: "High",           label_ta: "அதிக ஆபத்து" },
  MEDIUM:         { dot: "🟡", label_en: "Medium",          label_ta: "நடுத்தர ஆபத்து" },
  LOW:            { dot: "🟢", label_en: "Low",             label_ta: "குறைந்த ஆபத்து" },
  FLOOD_PRONE:    { dot: "🔵", label_en: "Flood-Prone",     label_ta: "வெள்ளம் வாய்ப்பு" },
};

const CRIME_LEVEL_META: Record<string, { dot: string; label_en: string; label_ta: string }> = {
  HIGH:   { dot: "🔴", label_en: "High",   label_ta: "அதிக குற்ற விகிதம்" },
  MEDIUM: { dot: "🟡", label_en: "Medium", label_ta: "நடுத்தர குற்ற விகிதம்" },
  LOW:    { dot: "🟢", label_en: "Low",    label_ta: "குறைந்த குற்ற விகிதம்" },
};

const ROAD_SAFETY_META: Record<string, { dot: string; label_en: string; label_ta: string }> = {
  HIGH_RISK:   { dot: "🔴", label_en: "High Risk",   label_ta: "அதிக சாலை அபாயம்" },
  MEDIUM_RISK: { dot: "🟡", label_en: "Medium Risk", label_ta: "நடுத்தர சாலை அபாயம்" },
  LOW_RISK:    { dot: "🟢", label_en: "Low Risk",    label_ta: "குறைந்த சாலை அபாயம்" },
};

const TREND_META: Record<string, { icon: string; label_en: string; label_ta: string }> = {
  IMPROVING: { icon: "↓", label_en: "improving", label_ta: "குறைகிறது" },
  STABLE:    { icon: "→", label_en: "stable",     label_ta: "நிலையானது" },
  WORSENING: { icon: "↑", label_en: "worsening",  label_ta: "அதிகரிக்கிறது" },
};

// ---------------------------------------------------------------------------
// Crime category breakdown helpers
// ---------------------------------------------------------------------------

interface CrimeCategory {
  icon: string;
  label_en: string;
  label_ta: string;
  incidents: number;
  rate: number;
  alert: boolean; // flag row in red if rate is high
}

function buildCrimeCategories(c: DistrictCrimeIndex): CrimeCategory[] {
  const pop = c.population_lakhs;

  const violentIncidents = c.murder_incidents;
  const violentRate = pop > 0 ? violentIncidents / pop : 0;

  const propertyIncidents = c.theft_incidents + c.robbery_incidents;
  const propertyRate = pop > 0 ? propertyIncidents / pop : 0;

  const womenIncidents = c.rape_incidents + c.assault_on_women_incidents;
  const womenRate = pop > 0 ? womenIncidents / pop : 0;

  const negIncidents = c.negligence_deaths;
  const negRate = c.negligence_death_rate_per_lakh;

  // Thresholds: flag if rate > 150% of illustrative TN averages
  // TN approx averages (SCRB 2021): murder ~1.5, property ~80, women ~25, negligence ~5 per lakh
  return [
    {
      icon: "🔪",
      label_en: "Violent Crime (Murder)",
      label_ta: "வன்முறை குற்றம் (கொலை)",
      incidents: violentIncidents,
      rate: parseFloat(violentRate.toFixed(2)),
      alert: violentRate > 2.5,
    },
    {
      icon: "🏚",
      label_en: "Property Crime (Theft & Robbery)",
      label_ta: "சொத்து குற்றம் (திருட்டு & கொள்ளை)",
      incidents: propertyIncidents,
      rate: parseFloat(propertyRate.toFixed(1)),
      alert: propertyRate > 120,
    },
    {
      icon: "👩",
      label_en: "Crimes Against Women",
      label_ta: "பெண்களுக்கு எதிரான குற்றங்கள்",
      incidents: womenIncidents,
      rate: parseFloat(womenRate.toFixed(1)),
      alert: womenRate > 38,
    },
    {
      icon: "⚠",
      label_en: "Negligence Deaths",
      label_ta: "அலட்சிய மரணங்கள்",
      incidents: negIncidents,
      rate: parseFloat(negRate.toFixed(1)),
      alert: negRate > 8,
    },
  ];
}

export function TenurePulse({ districtName, waterRisk, crimeIndex, roadSafety, lang = "en" }: TenurePulseProps) {
  const isTA = lang === "ta";
  const [crimeExpanded, setCrimeExpanded] = useState(false);

  const riskInfo   = waterRisk   ? WATER_RISK_INDICATOR[waterRisk.risk_level]     : null;
  const crimeMeta  = crimeIndex  ? CRIME_LEVEL_META[crimeIndex.crime_index_level]  : null;
  const roadMeta   = roadSafety  ? ROAD_SAFETY_META[roadSafety.road_safety_level]  : null;
  const trendMeta  = roadSafety  ? TREND_META[roadSafety.trend_2021_2023]          : null;

  const crimeCategories = crimeIndex ? buildCrimeCategories(crimeIndex) : [];

  return (
    <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-5">
      <h3 className="text-sm font-black text-gray-900 mb-3">
        {isTA ? "பதவிக்காலப் புள்ளிவிவரங்கள் (2021–2026)" : "Tenure Pulse (2021–2026)"}
      </h3>

      <div className="divide-y divide-gray-100">
        {/* ── Water Risk row ── */}
        <div className="py-3 flex items-start justify-between gap-4">
          <div className="flex-1 min-w-0">
            <p className="text-xs font-semibold text-gray-700">
              {isTA ? "நீர் ஆபத்து நிலை" : "Water Risk Level"}
            </p>
            <p className="text-xs text-gray-400 mt-0.5">
              {isTA ? "மூல: மாவட்ட நீர் வள அறிக்கை" : "Source: District Water Risk Report"}
            </p>
          </div>
          <div className="shrink-0 text-right">
            {waterRisk && riskInfo ? (
              <p className="text-sm font-bold text-gray-900">
                {riskInfo.dot}{" "}
                {isTA ? (waterRisk.risk_label_ta || riskInfo.label_ta) : (waterRisk.risk_label_en || riskInfo.label_en)}
              </p>
            ) : (
              <p className="text-xs text-gray-400 italic">
                {isTA ? "மாவட்ட தரவு இல்லை" : "District data unavailable"}
              </p>
            )}
          </div>
        </div>

        {/* ── Crime Index row (expandable) ── */}
        <div className="py-3">
          <button
            onClick={() => crimeIndex && setCrimeExpanded((v) => !v)}
            className={`w-full flex items-start justify-between gap-4 ${crimeIndex ? "cursor-pointer" : "cursor-default"}`}
            disabled={!crimeIndex}
          >
            <div className="flex-1 min-w-0 text-left">
              <p className="text-xs font-semibold text-gray-700">
                {isTA ? "குற்ற குறியீடு" : "Crime Index"}
              </p>
              <p className="text-xs text-gray-400 mt-0.5">
                {isTA ? "ஆதாரம்: SCRB / OpenCity 2021" : "Source: SCRB / OpenCity 2021"}
              </p>
            </div>
            <div className="shrink-0 text-right flex items-start gap-2">
              {crimeIndex && crimeMeta ? (
                <div>
                  <p className="text-sm font-bold text-gray-900">
                    {crimeMeta.dot} {isTA ? crimeMeta.label_ta : crimeMeta.label_en}
                  </p>
                  <p className="text-xs text-gray-400 mt-0.5">
                    {crimeIndex.ipc_crime_rate_per_lakh.toFixed(0)}{isTA ? "/லட்சம்" : "/lakh pop."}
                  </p>
                </div>
              ) : (
                <p className="text-xs text-gray-400 italic">
                  {isTA ? "மாவட்ட தரவு இல்லை" : "District data unavailable"}
                </p>
              )}
              {crimeIndex && (
                <span className="text-gray-400 text-sm mt-0.5 select-none">
                  {crimeExpanded ? "▲" : "▼"}
                </span>
              )}
            </div>
          </button>

          {/* Expanded crime category breakdown */}
          {crimeExpanded && crimeIndex && (
            <div className="mt-3 rounded-xl border border-gray-100 overflow-hidden">
              {/* Category rows */}
              {crimeCategories.map((cat) => (
                <div
                  key={cat.label_en}
                  className={`flex items-center justify-between px-3 py-2.5 border-b border-gray-100 last:border-0 ${
                    cat.alert ? "bg-red-50/60" : "bg-gray-50/40"
                  }`}
                >
                  <div className="flex items-center gap-2 min-w-0">
                    <span className="text-base leading-none">{cat.icon}</span>
                    <div className="min-w-0">
                      <p className={`text-xs font-medium leading-snug ${cat.alert ? "text-red-800" : "text-gray-700"}`}>
                        {isTA ? cat.label_ta : cat.label_en}
                      </p>
                      <p className="text-[11px] text-gray-400 mt-0.5">
                        {cat.incidents.toLocaleString()} {isTA ? "வழக்குகள்" : "cases"}{" · "}
                        {cat.rate}{isTA ? "/லட்சம்" : "/lakh"}
                        {cat.alert && (
                          <span className="ml-1 text-red-600 font-semibold">
                            {isTA ? " · உயர் விகிதம்" : " · above avg"}
                          </span>
                        )}
                      </p>
                    </div>
                  </div>
                </div>
              ))}
              {/* Baseline note */}
              <div className="px-3 py-2 bg-white border-t border-gray-100">
                <p className="text-[11px] text-gray-400">
                  {isTA
                    ? "📊 SCRB 2021 தரவு · 2026 SCRB அறிக்கை வெளியீட்டிற்கு பின் போக்கு கணக்கீடு புதுப்பிக்கப்படும்"
                    : "📊 SCRB 2021 baseline · Trend comparison will update after the 2026 SCRB report"}
                </p>
              </div>
            </div>
          )}
        </div>

        {/* ── Road Safety row ── */}
        <div className="py-3 flex items-start justify-between gap-4">
          <div className="flex-1 min-w-0">
            <p className="text-xs font-semibold text-gray-700">
              {isTA ? "சாலை பாதுகாப்பு" : "Road Safety"}
            </p>
            <p className="text-xs text-gray-400 mt-0.5">
              {isTA ? "ஆதாரம்: TN போலீஸ் விபத்து அறிக்கை 2021-23" : "Source: TN Police Road Accidents 2021-23"}
            </p>
          </div>
          <div className="shrink-0 text-right">
            {roadSafety && roadMeta && trendMeta ? (
              <div>
                <p className="text-sm font-bold text-gray-900">
                  {roadMeta.dot} {isTA ? roadMeta.label_ta : roadMeta.label_en}
                </p>
                <p className="text-xs text-gray-400 mt-0.5">
                  {roadSafety.death_rate_per_lakh_2021.toFixed(1)}{isTA ? "/லட்சம்" : "/lakh"}{" "}
                  · {trendMeta.icon} {isTA ? trendMeta.label_ta : trendMeta.label_en}
                </p>
              </div>
            ) : (
              <p className="text-xs text-gray-400 italic">
                {isTA ? "மாவட்ட தரவு இல்லை" : "District data unavailable"}
              </p>
            )}
          </div>
        </div>
      </div>

      <p className="mt-3 text-xs text-gray-400">
        {isTA
          ? "குறிப்பு: குற்றம், நீர், சுகாதாரம் தொடர்பான சமூக-பொருளாதார தரவு மாவட்ட அளவில் அரசு வழிகாட்டுதலின்படி வழங்கப்படுகிறது."
          : `Note: Socio-economic data (Crime, Water, Health) is reported at the District level as per State Planning Commission standards.`}
      </p>
    </div>
  );
}
