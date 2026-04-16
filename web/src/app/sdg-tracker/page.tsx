"use client";

import { useState } from "react";
import Link from "next/link";
import { useLanguage } from "@/lib/LanguageContext";
import {
  SDG_GOALS,
  TN_SDG_SUMMARY,
  TOP_STATES_AHEAD,
  PERFORMANCE_META,
  type SDGGoal,
  type SDGPerformance,
  type CompetitorState,
} from "@/lib/sdg-data";

// ---------------------------------------------------------------------------
// Trend arrow helper
// ---------------------------------------------------------------------------
function TrendArrow({ trend, good_direction }: { trend: "up" | "down" | "stable"; good_direction: "up" | "down" }) {
  if (trend === "stable") return <span className="text-gray-400 text-xs">→</span>;
  const isGood = trend === good_direction;
  if (trend === "up") return <span className={`text-xs font-bold ${isGood ? "text-emerald-600" : "text-red-500"}`}>↑</span>;
  return <span className={`text-xs font-bold ${isGood ? "text-emerald-600" : "text-red-500"}`}>↓</span>;
}

// ---------------------------------------------------------------------------
// Score bar
// ---------------------------------------------------------------------------
function ScoreBar({ score, color_bg }: { score: number; color_bg: string }) {
  return (
    <div className="w-full bg-gray-100 rounded-full h-1.5 mt-2">
      <div
        className={`h-1.5 rounded-full ${color_bg} opacity-70`}
        style={{ width: `${score}%` }}
      />
    </div>
  );
}

// ---------------------------------------------------------------------------
// Individual SDG card — collapsible
// ---------------------------------------------------------------------------
function SDGCard({ goal, lang }: { goal: SDGGoal; lang: "en" | "ta" }) {
  const [open, setOpen] = useState(false);
  const perf = PERFORMANCE_META[goal.tn_performance];
  const isTA = lang === "ta";

  return (
    <div className={`bg-white rounded-2xl border border-gray-200 overflow-hidden transition-shadow hover:shadow-md`}>
      {/* Color header strip */}
      <div className={`${goal.color_bg} px-4 py-3 flex items-center gap-3`}>
        <span className="text-2xl">{goal.icon}</span>
        <div className="flex-1 min-w-0">
          <p className={`text-[10px] font-bold uppercase tracking-widest ${goal.color_text} opacity-80`}>
            SDG {goal.id}
          </p>
          <p className={`text-sm font-black ${goal.color_text} leading-tight`}>
            {isTA ? goal.name_ta : goal.name}
          </p>
        </div>
        <div className="flex-shrink-0 text-right">
          {goal.tn_score !== undefined && (
            <p className={`text-lg font-black ${goal.color_text}`}>{goal.tn_score}</p>
          )}
          {goal.india_score !== undefined && (
            <p className={`text-[10px] ${goal.color_text} opacity-70`}>IN: {goal.india_score}</p>
          )}
        </div>
      </div>

      {/* Body */}
      <div className="px-4 py-3 space-y-3">
        {/* Description */}
        <p className="text-xs text-gray-600 leading-relaxed">
          {isTA ? goal.description_ta : goal.description_en}
        </p>

        {/* Score bar */}
        {goal.tn_score !== undefined && (
          <ScoreBar score={goal.tn_score} color_bg={goal.color_bg} />
        )}

        {/* Performance badge */}
        <div className="flex items-center justify-between">
          <span className={`text-[10px] font-bold px-2 py-0.5 rounded-full border ${perf.bg} ${perf.text} ${perf.border}`}>
            {isTA ? perf.label_ta : perf.label}
          </span>
          <button
            onClick={() => setOpen(!open)}
            className="text-[11px] font-semibold text-gray-500 hover:text-gray-800 transition-colors"
          >
            {open
              ? (isTA ? "மூடு ↑" : "Hide metrics ↑")
              : (isTA ? "குறிகாட்டிகள் ↓" : "Key metrics ↓")}
          </button>
        </div>

        {/* Expandable metrics */}
        {open && (
          <div className="space-y-2 pt-1 border-t border-gray-100">
            {goal.metrics.map((m, i) => (
              <div key={i} className="flex items-start justify-between gap-2">
                <div className="flex-1 min-w-0">
                  <p className="text-[11px] font-semibold text-gray-700 leading-tight">
                    {isTA ? m.label_ta : m.label}
                  </p>
                  {m.context && (
                    <p className="text-[10px] text-gray-400 leading-tight mt-0.5">{m.context}</p>
                  )}
                </div>
                <div className="flex items-center gap-1 flex-shrink-0">
                  <span className="text-[11px] font-bold text-gray-900">{m.value}</span>
                  <TrendArrow trend={m.trend} good_direction={m.good_direction} />
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Benchmark card — one competitor state
// ---------------------------------------------------------------------------
function BenchmarkCard({ state, lang }: { state: CompetitorState; lang: "en" | "ta" }) {
  const [open, setOpen] = useState(false);
  const isTA = lang === "ta";
  const overallGap = state.overall_score - TN_SDG_SUMMARY.overall_score;

  return (
    <div className="bg-white rounded-2xl border border-gray-200 overflow-hidden">
      {/* Header */}
      <div className="flex items-center gap-3 px-4 py-3 bg-gray-50 border-b border-gray-100">
        <span className="text-2xl">{state.emoji}</span>
        <div className="flex-1 min-w-0">
          <p className="text-sm font-black text-gray-900">
            {isTA ? state.name_ta : state.name}
          </p>
          <p className="text-[10px] text-gray-500 font-semibold">
            {isTA ? state.rank_label_ta : state.rank_label_en} · {isTA ? "NITI Aayog SDG குறியீடு" : "NITI Aayog SDG Index"}
          </p>
        </div>
        <div className="text-right flex-shrink-0">
          <p className="text-xl font-black text-gray-900">{state.overall_score}</p>
          <p className="text-[10px] font-bold text-red-500">
            TN {overallGap > 0 ? `−${overallGap}` : `+${Math.abs(overallGap)}`} {isTA ? "புள்ளிகள்" : "pts"}
          </p>
        </div>
      </div>

      {/* Score comparison bar */}
      <div className="px-4 pt-3 pb-1 space-y-1.5">
        <div>
          <div className="flex justify-between text-[10px] mb-0.5">
            <span className="font-semibold text-gray-500">{isTA ? state.name_ta : state.name}</span>
            <span className="font-bold text-gray-700">{state.overall_score}</span>
          </div>
          <div className="h-2 bg-gray-100 rounded-full">
            <div className="h-2 rounded-full bg-blue-500" style={{ width: `${state.overall_score}%` }} />
          </div>
        </div>
        <div>
          <div className="flex justify-between text-[10px] mb-0.5">
            <span className="font-semibold text-gray-500">{isTA ? "தமிழ்நாடு" : "Tamil Nadu"}</span>
            <span className="font-bold text-gray-700">{TN_SDG_SUMMARY.overall_score}</span>
          </div>
          <div className="h-2 bg-gray-100 rounded-full">
            <div className="h-2 rounded-full bg-yellow-400" style={{ width: `${TN_SDG_SUMMARY.overall_score}%` }} />
          </div>
        </div>
      </div>

      {/* Lag areas toggle */}
      <div className="px-4 pb-3 pt-2">
        <button
          onClick={() => setOpen(!open)}
          className="w-full text-left text-[11px] font-bold text-gray-500 hover:text-gray-800 transition-colors"
        >
          {open
            ? (isTA ? "மூடு ↑" : "Hide lag areas ↑")
            : (isTA
                ? `தமிழ்நாடு பின்தங்கிய ${state.lag_areas.length} பகுதிகள் ↓`
                : `${state.lag_areas.length} areas where TN lags ↓`)}
        </button>

        {open && (
          <div className="mt-2 space-y-3 pt-2 border-t border-gray-100">
            {state.lag_areas.map((area) => {
              const goal = SDG_GOALS.find((g) => g.id === area.sdg_id);
              if (!goal) return null;
              return (
                <div key={area.sdg_id} className="space-y-1.5">
                  {/* Goal label */}
                  <div className="flex items-center gap-2">
                    <span className={`text-[10px] font-black px-2 py-0.5 rounded-full ${goal.color_bg} ${goal.color_text}`}>
                      SDG {goal.id}
                    </span>
                    <span className="text-xs font-bold text-gray-700">
                      {isTA ? goal.name_ta : goal.name}
                    </span>
                  </div>

                  {/* Score bars side by side */}
                  <div className="grid grid-cols-2 gap-2">
                    <div>
                      <p className="text-[9px] text-gray-400 mb-0.5">{isTA ? state.name_ta : state.name}</p>
                      <div className="flex items-center gap-1.5">
                        <div className="flex-1 h-1.5 bg-gray-100 rounded-full">
                          <div className="h-1.5 rounded-full bg-blue-500" style={{ width: `${area.their_score}%` }} />
                        </div>
                        <span className="text-[10px] font-black text-blue-600 w-6 text-right">{area.their_score}</span>
                      </div>
                    </div>
                    <div>
                      <p className="text-[9px] text-gray-400 mb-0.5">{isTA ? "தமிழ்நாடு" : "Tamil Nadu"}</p>
                      <div className="flex items-center gap-1.5">
                        <div className="flex-1 h-1.5 bg-gray-100 rounded-full">
                          <div className="h-1.5 rounded-full bg-yellow-400" style={{ width: `${area.tn_score}%` }} />
                        </div>
                        <span className="text-[10px] font-black text-yellow-600 w-6 text-right">{area.tn_score}</span>
                      </div>
                    </div>
                  </div>

                  {/* Gap badge + key metric */}
                  <div className="flex items-start gap-2">
                    <span className="flex-shrink-0 text-[9px] font-black bg-red-100 text-red-600 px-1.5 py-0.5 rounded-full">
                      −{area.gap} {isTA ? "புள்" : "pts"}
                    </span>
                    <p className="text-[10px] text-gray-500 leading-snug">
                      {isTA ? area.key_gap_ta : area.key_gap_en}
                    </p>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Filter pill
// ---------------------------------------------------------------------------
function FilterPill({
  label,
  active,
  onClick,
}: {
  label: string;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      className={`text-xs font-semibold px-3 py-1.5 rounded-full border transition-all ${
        active
          ? "bg-gray-900 text-white border-gray-900"
          : "bg-white text-gray-600 border-gray-300 hover:border-gray-500"
      }`}
    >
      {label}
    </button>
  );
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------
type FilterKey = "all" | SDGPerformance;

export default function SDGTrackerPage() {
  const { lang, setLang } = useLanguage();
  const [filter, setFilter] = useState<FilterKey>("all");
  const isTA = lang === "ta";

  const filtered = filter === "all"
    ? SDG_GOALS
    : SDG_GOALS.filter((g) => g.tn_performance === filter);

  const counts: Record<FilterKey, number> = {
    all: SDG_GOALS.length,
    strength: SDG_GOALS.filter((g) => g.tn_performance === "strength").length,
    moderate: SDG_GOALS.filter((g) => g.tn_performance === "moderate").length,
    needs_attention: SDG_GOALS.filter((g) => g.tn_performance === "needs_attention").length,
  };

  return (
    <main className="min-h-full bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 shadow-sm sticky top-0 z-10">
        <div className="max-w-2xl mx-auto px-4 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Link href="/" className="text-gray-400 hover:text-gray-600 text-sm">
              ← {isTA ? "முகப்பு" : "Home"}
            </Link>
            <span className="text-gray-200">|</span>
            <div>
              <h1 className="text-sm font-black text-gray-900">
                {isTA ? "நிலையான வளர்ச்சி இலக்குகள்" : "SDG Tracker"}
              </h1>
              <p className="text-[10px] text-gray-500">
                {isTA ? "தமிழ்நாடு நிலை" : "Tamil Nadu Performance"}
              </p>
            </div>
          </div>
          <button
            onClick={() => setLang(lang === "en" ? "ta" : "en")}
            className="text-xs font-bold px-3 py-1.5 rounded-full border border-gray-300 hover:bg-gray-100 transition-colors text-gray-900"
          >
            {lang === "en" ? "தமிழ்" : "English"}
          </button>
        </div>
      </header>

      <div className="max-w-2xl mx-auto px-4 py-6 space-y-6">
        {/* Hero banner — TN overall score */}
        <div className="bg-gradient-to-br from-blue-900 to-blue-700 rounded-2xl p-5 text-white">
          <p className="text-xs font-bold uppercase tracking-widest opacity-70 mb-1">
            {isTA ? "தமிழ்நாடு · NITI Aayog SDG India Index" : "Tamil Nadu · NITI Aayog SDG India Index"}
          </p>
          <div className="flex items-end gap-4 mt-2">
            <div>
              <p className="text-5xl font-black">{TN_SDG_SUMMARY.overall_score}</p>
              <p className="text-sm opacity-80">{isTA ? "ஒட்டுமொத்த மதிப்பெண்" : "Overall Score"}</p>
            </div>
            <div className="pb-1">
              <p className="text-2xl font-black text-yellow-300">#{TN_SDG_SUMMARY.national_rank}</p>
              <p className="text-xs opacity-80">{isTA ? "இந்தியாவில் தரவரிசை" : "National Rank"}</p>
            </div>
            <div className="pb-1">
              <p className="text-sm font-bold text-emerald-300">{TN_SDG_SUMMARY.performance_category}</p>
              <p className="text-xs opacity-80">{isTA ? "வகை" : "Category"}</p>
            </div>
          </div>
          <div className="mt-3 pt-3 border-t border-white/20 flex items-center gap-3">
            <div className="flex-1">
              <div className="flex justify-between text-xs opacity-70 mb-1">
                <span>{isTA ? "தமிழ்நாடு" : "Tamil Nadu"}</span>
                <span>78</span>
              </div>
              <div className="bg-white/20 rounded-full h-2">
                <div className="bg-yellow-300 h-2 rounded-full" style={{ width: "78%" }} />
              </div>
            </div>
            <div className="flex-1">
              <div className="flex justify-between text-xs opacity-70 mb-1">
                <span>{isTA ? "இந்தியா" : "India Avg"}</span>
                <span>71</span>
              </div>
              <div className="bg-white/20 rounded-full h-2">
                <div className="bg-white/60 h-2 rounded-full" style={{ width: "71%" }} />
              </div>
            </div>
          </div>
          <p className="text-[10px] opacity-60 mt-2">
            {isTA ? "ஆதாரம்: " : "Source: "}
            <a
              href={TN_SDG_SUMMARY.source_url}
              target="_blank"
              rel="noopener noreferrer"
              className="underline underline-offset-2 hover:opacity-100"
            >
              {TN_SDG_SUMMARY.source}
            </a>
            {" · "}
            {TN_SDG_SUMMARY.year}
          </p>
        </div>

        {/* Who We're Behind */}
        <div className="space-y-3">
          <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
            {isTA ? "யார் முன்னிலையில் உள்ளனர்?" : "Who's ahead of Tamil Nadu"}
          </p>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            {TOP_STATES_AHEAD.map((state) => (
              <BenchmarkCard key={state.name} state={state} lang={lang} />
            ))}
          </div>
        </div>

        {/* What are SDGs? */}
        <div className="bg-white rounded-2xl border border-gray-200 p-4">
          <p className="text-sm font-black text-gray-900 mb-1">
            {isTA ? "SDG என்றால் என்ன?" : "What are the SDGs?"}
          </p>
          <p className="text-xs text-gray-600 leading-relaxed">
            {isTA
              ? "நிலையான வளர்ச்சி இலக்குகள் (SDG) என்பது 2030-ஆம் ஆண்டுக்குள் வறுமை, சமத்துவமின்மை மற்றும் காலநிலை மாற்றத்தை எதிர்கொள்ள 193 ஐக்கிய நாடுகள் உறுப்பினர்களால் ஏற்றுக்கொள்ளப்பட்ட 17 உலகளாவிய இலக்குகளின் தொகுப்பாகும்."
              : "The 17 Sustainable Development Goals (SDGs) are a UN-adopted blueprint of interconnected objectives to end poverty, protect the planet, and ensure prosperity for all by 2030. India's NITI Aayog tracks all states against 113 indicators."
            }
          </p>
          <div className="mt-3 flex flex-wrap gap-1.5">
            {[
              { icon: "👥", label: isTA ? "மக்கள்" : "People" },
              { icon: "🌍", label: isTA ? "கிரகம்" : "Planet" },
              { icon: "💰", label: isTA ? "செழுமை" : "Prosperity" },
              { icon: "☮️", label: isTA ? "அமைதி" : "Peace" },
              { icon: "🤝", label: isTA ? "கூட்டாண்மை" : "Partnership" },
            ].map((p) => (
              <span key={p.label} className="text-[11px] bg-gray-100 text-gray-700 px-2 py-1 rounded-full font-semibold">
                {p.icon} {p.label}
              </span>
            ))}
          </div>
        </div>

        {/* Filter pills */}
        <div className="space-y-2">
          <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
            {isTA ? "வடிகட்டு" : "Filter by performance"}
          </p>
          <div className="flex flex-wrap gap-2">
            <FilterPill label={isTA ? `அனைத்தும் (${counts.all})` : `All (${counts.all})`} active={filter === "all"} onClick={() => setFilter("all")} />
            <FilterPill label={isTA ? `பலம் (${counts.strength})` : `Strengths (${counts.strength})`} active={filter === "strength"} onClick={() => setFilter("strength")} />
            <FilterPill label={isTA ? `வழியில் (${counts.moderate})` : `On Track (${counts.moderate})`} active={filter === "moderate"} onClick={() => setFilter("moderate")} />
            <FilterPill label={isTA ? `கவனம் தேவை (${counts.needs_attention})` : `Needs Focus (${counts.needs_attention})`} active={filter === "needs_attention"} onClick={() => setFilter("needs_attention")} />
          </div>
        </div>

        {/* SDG Grid */}
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
          {filtered.map((goal) => (
            <SDGCard key={goal.id} goal={goal} lang={lang} />
          ))}
        </div>

        {/* Manifesto link */}
        <div className="bg-blue-50 border border-blue-200 rounded-2xl p-4">
          <p className="text-sm font-bold text-blue-900 mb-1">
            {isTA ? "வாக்குறுதிகள் SDG-ஐ நிவர்த்தி செய்கிறதா?" : "Do party manifestos address these goals?"}
          </p>
          <p className="text-xs text-blue-700 mb-3 leading-relaxed">
            {isTA
              ? "கட்சிகளின் தேர்தல் வாக்குறுதிகள் இந்த SDG இலக்குகளை எவ்வாறு நிவர்த்தி செய்கின்றன என்பதை Manifesto Tracker-இல் பாருங்கள்."
              : "Check the Manifesto Tracker to see how party election promises align with these SDG targets — and whether they were fulfilled."}
          </p>
          <Link
            href="/manifesto-tracker"
            className="inline-block text-xs font-bold bg-blue-700 text-white px-4 py-2 rounded-full hover:bg-blue-800 transition-colors"
          >
            {isTA ? "→ அறிக்கை ஒப்பீடு" : "→ Open Manifesto Tracker"}
          </Link>
        </div>

        {/* Attribution — every source is a live link to the authoritative dataset. */}
        <p className="text-center text-[10px] text-gray-400 pb-4 flex flex-wrap justify-center items-center gap-x-1.5 gap-y-1">
          <span>{isTA ? "ஆதாரம்:" : "Source:"}</span>
          {(
            [
              { label: "NITI Aayog SDG India Index 2023-24", url: "https://sdgindiaindex.niti.gov.in/#/ranking" },
              { label: "NFHS-5",         url: "https://rchiips.org/nfhs/NFHS-5Reports/TN.pdf" },
              { label: "UDISE+",         url: "https://udiseplus.gov.in/#/page/publications" },
              { label: "PLFS 2022-23",   url: "https://www.mospi.gov.in/publication/plfs-annual-report-2022-23" },
              { label: "FSI 2023",       url: "https://fsi.nic.in/forest-report-2023" },
              { label: "NCRB 2022",      url: "https://www.ncrb.gov.in/crime-in-india-additional-table?year=2022&category=States/UTs" },
            ] as const
          ).map((s, i, arr) => (
            <span key={s.label} className="inline-flex items-center">
              <a
                href={s.url}
                target="_blank"
                rel="noopener noreferrer"
                className="underline underline-offset-2 hover:text-gray-600 transition-colors"
              >
                {s.label}
              </a>
              {i < arr.length - 1 && <span className="ml-1.5 text-gray-300">·</span>}
            </span>
          ))}
        </p>
      </div>
    </main>
  );
}
