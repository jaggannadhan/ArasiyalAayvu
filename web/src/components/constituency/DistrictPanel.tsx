"use client";

import { useState } from "react";
import type {
  SocioMetric,
  DistrictWaterRisk,
  DistrictCrimeIndex,
  DistrictRoadSafety,
} from "@/lib/types";

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------
interface DistrictPanelProps {
  metrics: SocioMetric[];
  waterRisk: DistrictWaterRisk | null;
  crimeIndex: DistrictCrimeIndex | null;
  roadSafety: DistrictRoadSafety | null;
  districtName: string;
  metricsScope: "district" | "state_fallback";
  lang?: "en" | "ta";
}

// ---------------------------------------------------------------------------
// Metric display config (raw view)
// ---------------------------------------------------------------------------
type MetricGroup = "literacy" | "health" | "economy";

const METRIC_CFG: Record<string, {
  label_en: string; label_ta: string; icon: string;
  // "X in 100 [subject] [verb]" — filled with the rounded value at render time
  per100_en: (n: number) => string;
  per100_ta: (n: number) => string;
  // One-line source/scope note
  source_note_en: string; source_note_ta: string;
  group: MetricGroup; good_direction: "high" | "low";
  warn_below?: number; warn_above?: number;
}> = {
  aser2024_std3_reading_recovery: {
    label_en: "Grade 3 Reading Level",
    label_ta: "3ஆம் வகுப்பு வாசிப்பு நிலை",
    icon: "📖",
    per100_en: (n) => `${n} in 100 Grade 3 children can read a Grade 2-level text`,
    per100_ta: (n) => `100ல் ${n} மூன்றாம் வகுப்பு மாணவர்கள் இரண்டாம் வகுப்பு பாடம் படிக்க முடியும்`,
    source_note_en: "ASER 2024 · TN state avg · higher = better",
    source_note_ta: "ASER 2024 · TN மாநில சராசரி · அதிகம் = சிறந்தது",
    group: "literacy", good_direction: "high", warn_below: 30,
  },
  nfhs5_women_literacy: {
    label_en: "Female Literacy Rate",
    label_ta: "பெண்கள் கல்வியறிவு விகிதம்",
    icon: "🎓",
    per100_en: (n) => `${n} in 100 women (age 15–49) can read and write`,
    per100_ta: (n) => `100ல் ${n} பெண்கள் (15-49 வயது) படிக்கவும் எழுதவும் முடியும்`,
    source_note_en: "NFHS-5 (2019-21) · district data · higher = better",
    source_note_ta: "NFHS-5 (2019-21) · மாவட்ட தரவு · அதிகம் = சிறந்தது",
    group: "literacy", good_direction: "high", warn_below: 70,
  },
  nfhs5_anaemia_women: {
    label_en: "Anaemia in Women",
    label_ta: "பெண்களில் இரத்த சோகை",
    icon: "💉",
    per100_en: (n) => `${n} in 100 women (age 15–49) have anaemia — low blood haemoglobin`,
    per100_ta: (n) => `100ல் ${n} பெண்களுக்கு இரத்த சோகை உள்ளது`,
    source_note_en: "NFHS-5 (2019-21) · district data · lower = better",
    source_note_ta: "NFHS-5 (2019-21) · மாவட்ட தரவு · குறைவு = சிறந்தது",
    group: "health", good_direction: "low", warn_above: 40,
  },
  nfhs5_stunting_under5: {
    label_en: "Child Stunting (Under 5)",
    label_ta: "5 வயதுக்கு கீழ் குழந்தை வளர்ச்சிக் குறைபாடு",
    icon: "🌱",
    per100_en: (n) => `${n} in 100 children under 5 are stunted — too short for their age`,
    per100_ta: (n) => `100ல் ${n} குழந்தைகள் (5 வயதுக்கு கீழ்) வளர்ச்சிக் குறைபாட்டால் பாதிக்கப்பட்டுள்ளனர்`,
    source_note_en: "NFHS-5 (2019-21) · district data · lower = better",
    source_note_ta: "NFHS-5 (2019-21) · மாவட்ட தரவு · குறைவு = சிறந்தது",
    group: "health", good_direction: "low", warn_above: 30,
  },
  nfhs5_institutional_deliveries: {
    label_en: "Skilled Birth Attendance",
    label_ta: "திறமையான மருத்துவப் பிரசவம்",
    icon: "🏥",
    per100_en: (n) => `${n} in 100 births were attended by a doctor or trained health worker`,
    per100_ta: (n) => `100ல் ${n} பிரசவங்கள் மருத்துவர் அல்லது பயிற்சி பெற்ற உதவியாளர் கண்காணிப்பில் நடந்தன`,
    source_note_en: "NFHS-5 (2019-21) · district data · higher = better",
    source_note_ta: "NFHS-5 (2019-21) · மாவட்ட தரவு · அதிகம் = சிறந்தது",
    group: "health", good_direction: "high", warn_below: 80,
  },
  industrial_corridors_district_coverage: {
    label_en: "Industrial Corridor Coverage",
    label_ta: "தொழில் வளாகம் உள்ளது",
    icon: "🏭",
    per100_en: (n) => n >= 100 ? "District is part of a national industrial corridor plan" : "District is not covered by a national industrial corridor plan",
    per100_ta: (n) => n >= 100 ? "மாவட்டம் தேசிய தொழில் வளாக திட்டத்தில் உள்ளது" : "மாவட்டம் தேசிய தொழில் வளாக திட்டத்தில் இல்லை",
    source_note_en: "National Industrial Corridor Programme · higher = better",
    source_note_ta: "தேசிய தொழில் வளாக திட்டம் · அதிகம் = சிறந்தது",
    group: "economy", good_direction: "high", warn_below: 60,
  },
};

// ---------------------------------------------------------------------------
// Water risk display
// ---------------------------------------------------------------------------
const WATER_RISK_META: Record<string, { dot: string; label_en: string; label_ta: string }> = {
  EXTREMELY_HIGH: { dot: "🔴", label_en: "Extremely High", label_ta: "மிக அதிக ஆபத்து" },
  HIGH:           { dot: "🔴", label_en: "High",           label_ta: "அதிக ஆபத்து" },
  MEDIUM:         { dot: "🟡", label_en: "Medium",         label_ta: "நடுத்தர ஆபத்து" },
  LOW:            { dot: "🟢", label_en: "Low",            label_ta: "குறைந்த ஆபத்து" },
  FLOOD_PRONE:    { dot: "🔵", label_en: "Flood-Prone",    label_ta: "வெள்ளம் வாய்ப்பு" },
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
  STABLE:    { icon: "→", label_en: "stable",    label_ta: "நிலையானது" },
  WORSENING: { icon: "↑", label_en: "worsening", label_ta: "அதிகரிக்கிறது" },
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function GaugeBar({ value, goodDirection, warn_below, warn_above }: {
  value: number; goodDirection: "high" | "low"; warn_below?: number; warn_above?: number;
}) {
  const isWarning =
    (warn_below != null && value < warn_below) ||
    (warn_above != null && value > warn_above);
  const pct = Math.min(value, 100);
  const barColor = isWarning
    ? "bg-red-400"
    : goodDirection === "high"
    ? "bg-emerald-500"
    : value < (warn_above ?? 20) ? "bg-emerald-500" : "bg-amber-400";
  return (
    <div className="w-full h-1.5 bg-gray-100 rounded-full overflow-hidden">
      <div className={`h-full rounded-full ${barColor}`} style={{ width: `${pct}%` }} />
    </div>
  );
}

function TnBadge({ percentile, lang }: { percentile: number; lang: "en" | "ta" }) {
  const color =
    percentile >= 70 ? "bg-emerald-100 text-emerald-800 border-emerald-200" :
    percentile >= 40 ? "bg-amber-100 text-amber-800 border-amber-200" :
                       "bg-red-100 text-red-800 border-red-200";
  const label = lang === "ta"
    ? `TN மாவட்டங்களில் ${percentile}% ஐ விட சிறந்தது`
    : `Better than ${percentile}% of TN`;
  return (
    <span className={`inline-block text-[10px] font-bold px-2 py-0.5 rounded-full border ${color}`}>
      {label}
    </span>
  );
}

function VsIndia({ value, nationalAvg, goodDirection, lang }: {
  value: number; nationalAvg: number; goodDirection: "high" | "low"; lang: "en" | "ta";
}) {
  const diff = parseFloat(Math.abs(value - nationalAvg).toFixed(1));
  const isBetter = goodDirection === "high" ? value > nationalAvg : value < nationalAvg;
  const color = isBetter ? "text-emerald-700" : "text-red-600";
  const label = lang === "ta"
    ? `${diff}pp ${isBetter ? "இந்தியாவை விட சிறந்தது" : "இந்தியாவை விட மோசம்"}`
    : `${diff}pp ${isBetter ? "better than India" : "worse than India"}`;
  return <span className={`text-[10px] font-semibold ${color}`}>{label}</span>;
}

// ---------------------------------------------------------------------------
// Section header
// ---------------------------------------------------------------------------
function SectionHeader({ icon, label }: { icon: string; label: string }) {
  return (
    <p className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-2.5">
      {icon} {label}
    </p>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------
export function DistrictPanel({
  metrics, waterRisk, crimeIndex, roadSafety,
  districtName, metricsScope, lang = "en",
}: DistrictPanelProps) {
  const isTA = lang === "ta";
  const [view, setView] = useState<"raw" | "context">("raw");
  const [crimeExpanded, setCrimeExpanded] = useState(false);

  // Build ordered metric list from config
  const orderedMetrics = Object.entries(METRIC_CFG)
    .map(([id, cfg]) => ({ metric: metrics.find((m) => m.metric_id === id), cfg }))
    .filter((x): x is { metric: SocioMetric; cfg: typeof METRIC_CFG[string] } => x.metric != null);

  const byGroup = (g: MetricGroup) => orderedMetrics.filter(({ cfg }) => cfg.group === g);

  const riskInfo  = waterRisk  ? WATER_RISK_META[waterRisk.risk_level]            : null;
  const crimeMeta = crimeIndex ? CRIME_LEVEL_META[crimeIndex.crime_index_level]   : null;
  const roadMeta  = roadSafety ? ROAD_SAFETY_META[roadSafety.road_safety_level]   : null;
  const trendMeta = roadSafety ? TREND_META[roadSafety.trend_2021_2023]           : null;

  // ---------------------------------------------------------------------------
  // Render a single SocioMetric row (works for literacy/health/economy)
  // ---------------------------------------------------------------------------
  function MetricRow({ metric, cfg }: { metric: SocioMetric; cfg: typeof METRIC_CFG[string] }) {
    const isWarning =
      (cfg.warn_below != null && metric.value < cfg.warn_below) ||
      (cfg.warn_above != null && metric.value > cfg.warn_above);
    // Treat all these metrics as percentages regardless of the stored unit string
    const displayValue = `${metric.value}%`;

    if (view === "context") {
      return (
        <div className="space-y-1">
          <div className="flex items-center justify-between gap-2">
            <div className="min-w-0">
              <div className="flex items-center gap-1.5">
                <span className="text-sm">{cfg.icon}</span>
                <span className="text-xs font-medium text-gray-700 truncate">
                  {isTA ? cfg.label_ta : cfg.label_en}
                </span>
              </div>
              <p className="text-[10px] text-gray-700 mt-0.5 pl-6 leading-tight font-medium">
                {isTA ? cfg.per100_ta(Math.round(metric.value)) : cfg.per100_en(Math.round(metric.value))}
              </p>
              <p className="text-[10px] text-gray-400 mt-0.5 pl-6">
                {isTA ? cfg.source_note_ta : cfg.source_note_en}
              </p>
            </div>
            <span className="text-sm font-bold text-gray-900 shrink-0">{displayValue}</span>
          </div>
          <div className="flex flex-wrap items-center gap-1.5 pl-6">
            {metric.tn_percentile != null && (
              <TnBadge percentile={metric.tn_percentile} lang={lang} />
            )}
            {metric.national_average != null && (
              <VsIndia
                value={metric.value}
                nationalAvg={metric.national_average}
                goodDirection={cfg.good_direction}
                lang={lang}
              />
            )}
          </div>
        </div>
      );
    }

    // raw view
    return (
      <div className="space-y-1.5">
        <div className="flex items-center justify-between gap-2">
          <div className="min-w-0">
            <div className="flex items-center gap-1.5">
              <span className="text-sm">{cfg.icon}</span>
              <span className="text-xs font-medium text-gray-700 truncate">
                {isTA ? cfg.label_ta : cfg.label_en}
              </span>
              {isWarning && <span className="shrink-0 text-xs text-red-600 font-bold">⚠</span>}
            </div>
            <p className="text-[10px] text-gray-700 mt-0.5 pl-6 leading-tight font-medium">
              {isTA ? cfg.per100_ta(Math.round(metric.value)) : cfg.per100_en(Math.round(metric.value))}
            </p>
            <p className="text-[10px] text-gray-400 mt-0.5 pl-6">
              {isTA ? cfg.source_note_ta : cfg.source_note_en}
            </p>
          </div>
          <span className="text-sm font-bold text-gray-900 shrink-0">{displayValue}</span>
        </div>
        <GaugeBar
          value={metric.value}
          goodDirection={cfg.good_direction}
          warn_below={cfg.warn_below}
          warn_above={cfg.warn_above}
        />
        {metric.national_average != null && (
          <p className="text-[10px] text-gray-400">
            {isTA ? "தேசிய சராசரி" : "National avg"}: {metric.national_average}%
          </p>
        )}
      </div>
    );
  }

  // ---------------------------------------------------------------------------
  // Render a risk/safety row (water / crime / road)
  // ---------------------------------------------------------------------------
  function RiskRow({
    icon, label_en, label_ta, rawValue, rawUnit,
    levelDot, levelLabel, secondaryLine, tnPercentile, unavailable,
    expandable, expanded, onExpand, expandContent,
  }: {
    icon: string; label_en: string; label_ta: string;
    rawValue?: string; rawUnit?: string;
    levelDot?: string; levelLabel?: string;
    secondaryLine?: string;
    tnPercentile?: number;
    unavailable?: boolean;
    expandable?: boolean; expanded?: boolean; onExpand?: () => void;
    expandContent?: React.ReactNode;
  }) {
    const label = isTA ? label_ta : label_en;

    return (
      <div>
        <button
          onClick={expandable && !unavailable ? onExpand : undefined}
          className={`w-full flex items-start justify-between gap-4 ${expandable && !unavailable ? "cursor-pointer" : "cursor-default"}`}
        >
          <div className="flex-1 min-w-0 text-left">
            <p className="text-xs font-semibold text-gray-700">{icon} {label}</p>
            {view === "context" && tnPercentile != null && !unavailable && (
              <div className="mt-1">
                <TnBadge percentile={tnPercentile} lang={lang} />
              </div>
            )}
          </div>
          <div className="shrink-0 text-right flex items-start gap-1.5">
            {unavailable ? (
              <p className="text-xs text-gray-400 italic">
                {isTA ? "தரவு இல்லை" : "Data unavailable"}
              </p>
            ) : view === "context" ? (
              <p className="text-xs font-bold text-gray-700">
                {rawValue}{rawUnit ? <span className="text-gray-400 font-normal"> {rawUnit}</span> : null}
              </p>
            ) : (
              <div>
                {levelDot && levelLabel && (
                  <p className="text-xs font-bold text-gray-900">{levelDot} {levelLabel}</p>
                )}
                {secondaryLine && (
                  <p className="text-[11px] text-gray-400 mt-0.5">{secondaryLine}</p>
                )}
              </div>
            )}
            {expandable && !unavailable && (
              <span className="text-gray-400 text-xs mt-0.5 select-none">
                {expanded ? "▲" : "▼"}
              </span>
            )}
          </div>
        </button>
        {expandable && expanded && expandContent}
      </div>
    );
  }

  // Crime breakdown (raw view expandable)
  const crimeBreakdown = crimeIndex ? (
    <div className="mt-3 rounded-xl border border-gray-100 overflow-hidden">
      {[
        { icon: "🔪", label_en: "Violent (Murder)", label_ta: "வன்முறை (கொலை)",
          incidents: crimeIndex.murder_incidents, rate: crimeIndex.murder_rate_per_lakh, alert: crimeIndex.murder_rate_per_lakh > 2.5 },
        { icon: "🏚", label_en: "Property Crime", label_ta: "சொத்து குற்றம்",
          incidents: crimeIndex.theft_incidents + crimeIndex.robbery_incidents,
          rate: parseFloat(((crimeIndex.theft_incidents + crimeIndex.robbery_incidents) / crimeIndex.population_lakhs).toFixed(1)),
          alert: (crimeIndex.theft_incidents + crimeIndex.robbery_incidents) / crimeIndex.population_lakhs > 120 },
        { icon: "👩", label_en: "Crimes Against Women", label_ta: "பெண்களுக்கு எதிரான குற்றம்",
          incidents: crimeIndex.rape_incidents + crimeIndex.assault_on_women_incidents,
          rate: parseFloat(((crimeIndex.rape_incidents + crimeIndex.assault_on_women_incidents) / crimeIndex.population_lakhs).toFixed(1)),
          alert: (crimeIndex.rape_incidents + crimeIndex.assault_on_women_incidents) / crimeIndex.population_lakhs > 38 },
      ].map((cat) => (
        <div key={cat.label_en}
          className={`flex items-center justify-between px-3 py-2.5 border-b border-gray-100 last:border-0 ${cat.alert ? "bg-red-50/60" : "bg-gray-50/40"}`}
        >
          <div className="flex items-center gap-2 min-w-0">
            <span className="text-sm leading-none">{cat.icon}</span>
            <div>
              <p className={`text-xs font-medium ${cat.alert ? "text-red-800" : "text-gray-700"}`}>
                {isTA ? cat.label_ta : cat.label_en}
              </p>
              <p className="text-[11px] text-gray-400 mt-0.5">
                {cat.incidents.toLocaleString()} {isTA ? "வழக்குகள்" : "cases"}{" · "}
                {cat.rate}{isTA ? "/லட்சம்" : "/lakh"}
                {cat.alert && <span className="ml-1 text-red-600 font-semibold">{isTA ? " · உயர்" : " · above avg"}</span>}
              </p>
            </div>
          </div>
        </div>
      ))}
      <div className="px-3 py-2 bg-white">
        <p className="text-[11px] text-gray-400">
          {"📊 "}
          <a href={crimeIndex?.source_url ?? "https://scrb.gov.in"} target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">
            {isTA ? "SCRB 2021 தரவு" : "SCRB 2021 baseline"}
          </a>
        </p>
      </div>
    </div>
  ) : null;

  return (
    <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-5 space-y-5">

      {/* ── Header ── */}
      <div className="flex items-start justify-between gap-2">
        <div>
          <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
            {isTA ? "மாவட்ட குறிகாட்டிகள்" : "District Indicators"}
          </p>
          <p className="text-xs text-gray-400 mt-0.5">
            {metricsScope === "district"
              ? isTA ? `${districtName} மாவட்டம்` : `${districtName} District`
              : isTA ? "மாநில சராசரி (மாவட்ட தரவு இல்லை)" : "State averages (district data unavailable)"}
          </p>
        </div>
        {/* Robinhood-style toggle */}
        <button
          onClick={() => setView(view === "raw" ? "context" : "raw")}
          className={`shrink-0 text-xs font-bold px-3 py-1.5 rounded-full border transition-colors ${
            view === "context"
              ? "bg-gray-900 text-white border-gray-900"
              : "bg-white text-gray-600 border-gray-300 hover:bg-gray-50"
          }`}
        >
          {view === "context" ? "# Raw" : "% Context"}
        </button>
      </div>

      {/* ── Literacy ── */}
      {byGroup("literacy").length > 0 && (
        <div>
          <SectionHeader icon="📚" label={isTA ? "கல்வியறிவு" : "Literacy"} />
          <div className="space-y-3.5">
            {byGroup("literacy").map(({ metric, cfg }) => (
              <MetricRow key={metric.metric_id} metric={metric} cfg={cfg} />
            ))}
          </div>
        </div>
      )}

      {/* ── Health ── */}
      {byGroup("health").length > 0 && (
        <div>
          <SectionHeader icon="🏥" label={isTA ? "சுகாதாரம்" : "Health"} />
          <div className="space-y-3.5">
            {byGroup("health").map(({ metric, cfg }) => (
              <MetricRow key={metric.metric_id} metric={metric} cfg={cfg} />
            ))}
          </div>
        </div>
      )}

      {/* ── Environment ── */}
      <div>
        <SectionHeader icon="🌊" label={isTA ? "சுற்றுச்சூழல்" : "Environment"} />
        <RiskRow
          icon="💧"
          label_en="Water Stress"
          label_ta="நீர் அழுத்தம்"
          rawValue={waterRisk ? `${((waterRisk.water_stress_score / 5) * 100).toFixed(0)}%` : undefined}
          rawUnit={waterRisk ? "of max" : undefined}
          levelDot={riskInfo?.dot}
          levelLabel={waterRisk && riskInfo ? (isTA ? (waterRisk.risk_label_ta || riskInfo.label_ta) : (waterRisk.risk_label_en || riskInfo.label_en)) : undefined}
          secondaryLine={waterRisk ? `Score ${waterRisk.water_stress_score.toFixed(1)} / 5` : undefined}
          tnPercentile={waterRisk?.tn_percentile}
          unavailable={!waterRisk}
        />
      </div>

      {/* ── Safety ── */}
      <div>
        <SectionHeader icon="🔒" label={isTA ? "பாதுகாப்பு" : "Safety"} />
        <div className="space-y-3">
          <RiskRow
            icon="🚨"
            label_en="Crime Rate"
            label_ta="குற்ற விகிதம்"
            rawValue={crimeIndex ? crimeIndex.ipc_crime_rate_per_lakh.toFixed(0) : undefined}
            rawUnit={isTA ? "/லட்சம்" : "/lakh"}
            levelDot={crimeMeta?.dot}
            levelLabel={crimeMeta ? (isTA ? crimeMeta.label_ta : crimeMeta.label_en) : undefined}
            secondaryLine={crimeIndex ? `${crimeIndex.ipc_crime_rate_per_lakh.toFixed(0)} IPC cases${isTA ? "/லட்சம்" : "/lakh"}` : undefined}
            tnPercentile={crimeIndex?.tn_percentile}
            unavailable={!crimeIndex}
            expandable={!!crimeIndex && view === "raw"}
            expanded={crimeExpanded}
            onExpand={() => setCrimeExpanded((v) => !v)}
            expandContent={crimeBreakdown}
          />
          <RiskRow
            icon="🛣"
            label_en="Road Safety"
            label_ta="சாலை பாதுகாப்பு"
            rawValue={roadSafety ? roadSafety.death_rate_per_lakh_2023.toFixed(1) : undefined}
            rawUnit={isTA ? "மரணம்/லட்சம்" : "deaths/lakh"}
            levelDot={roadMeta?.dot}
            levelLabel={roadMeta ? (isTA ? roadMeta.label_ta : roadMeta.label_en) : undefined}
            secondaryLine={roadSafety && trendMeta ? `${trendMeta.icon} ${isTA ? trendMeta.label_ta : trendMeta.label_en} 2021–23` : undefined}
            tnPercentile={roadSafety?.tn_percentile}
            unavailable={!roadSafety}
          />
        </div>
      </div>

      {/* ── Economy ── */}
      {byGroup("economy").length > 0 && (
        <div>
          <SectionHeader icon="🏭" label={isTA ? "பொருளாதாரம்" : "Economy"} />
          <div className="space-y-3.5">
            {byGroup("economy").map(({ metric, cfg }) => (
              <MetricRow key={metric.metric_id} metric={metric} cfg={cfg} />
            ))}
          </div>
        </div>
      )}

      {/* ── Footer ── */}
      <p className="text-[10px] text-gray-400 border-t border-gray-100 pt-3 leading-relaxed">
        {isTA ? "ஆதாரம்: " : "Sources: "}
        <a href="https://rchiips.org/nfhs/NFHS-5Reports/TN.pdf" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">NFHS-5 (2019-21)</a>
        {", "}
        <a href="https://asercentre.org/aser-2024/" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">ASER 2024</a>
        {", "}
        <a href={crimeIndex?.source_url ?? "https://scrb.gov.in"} target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">SCRB 2021</a>
        {", "}
        <a href={roadSafety?.source_url ?? "https://morth.nic.in/road-accident-in-india"} target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">{isTA ? "சாலை விபத்து அறிக்கை" : "Road Accident Report"}</a>
        {", "}
        <a href="https://www.wri.org/aqueduct" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">WRI Aqueduct</a>
      </p>
    </div>
  );
}
