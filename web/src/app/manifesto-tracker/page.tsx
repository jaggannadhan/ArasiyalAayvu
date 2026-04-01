"use client";

import { useState, useMemo } from "react";
import { ComparisonMatrix } from "@/components/manifesto/ComparisonMatrix";
import { PillarTabs } from "@/components/manifesto/PillarTabs";
import { PartySelector } from "@/components/manifesto/PartySelector";
import { ComparisonSkeleton } from "@/components/manifesto/PromiseSkeleton";
import { useManifestos, type ManifestoYearFilter } from "@/hooks/useManifestos";
import type { Pillar } from "@/lib/types";
import { PILLARS } from "@/lib/types";

export default function ManifestoTrackerPage() {
  const [lang, setLang] = useState<"en" | "ta">("en");
  const [partyA, setPartyA] = useState("dmk");
  const [partyB, setPartyB] = useState("aiadmk");
  const [activePillar, setActivePillar] = useState<Pillar>("Agriculture");
  const [yearFilter, setYearFilter] = useState<ManifestoYearFilter>("all");

  const { promises, loading, error } = useManifestos(yearFilter);

  // Swap parties when the same one is selected on both sides
  const handlePartyAChange = (id: string) => {
    if (id === partyB) setPartyB(partyA);
    setPartyA(id);
  };
  const handlePartyBChange = (id: string) => {
    if (id === partyA) setPartyA(partyB);
    setPartyB(id);
  };

  const promiseCounts = useMemo(() => {
    const counts: Partial<Record<Pillar, number>> = {};
    for (const pillar of PILLARS) {
      counts[pillar] = promises.filter(
        (p) => p.category === pillar && (p.party_id === partyA || p.party_id === partyB)
      ).length;
    }
    return counts;
  }, [promises, partyA, partyB]);

  const isTA = lang === "ta";

  return (
    <main className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="sticky top-0 z-20 bg-white border-b border-gray-200 shadow-sm">
        <div className="max-w-5xl mx-auto px-4 py-3 flex items-center justify-between gap-4">
          <div>
            <h1 className="text-base font-black text-gray-900 leading-tight">
              {isTA ? "தேர்தல் அறிக்கை ஒப்பீடு" : "Manifesto Tracker"}
            </h1>
            <p className="text-xs text-gray-500">
              {isTA ? "வாக்குறுதி vs செயல்திறன்" : "Promise vs. Performance · Tamil Nadu 2026"}
            </p>
          </div>
          {/* Language toggle */}
          <button
            onClick={() => setLang(lang === "en" ? "ta" : "en")}
            className="shrink-0 text-xs font-bold px-3 py-1.5 rounded-full border border-gray-300 hover:bg-gray-100 transition-colors"
          >
            {lang === "en" ? "தமிழ்" : "English"}
          </button>
        </div>
      </header>

      <div className="max-w-5xl mx-auto px-4 py-5 space-y-5">
        {/* Year filter toggle */}
        <section className="flex items-center gap-2">
          <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide mr-1">
            {isTA ? "காலம்" : "View"}
          </span>
          {(
            [
              { value: "all",  en: "All",                   ta: "அனைத்தும்" },
              { value: 2021,   en: "Historical Performance", ta: "2021 செயல்திறன்" },
              { value: 2026,   en: "Upcoming Promises",      ta: "2026 வாக்குறுதிகள்" },
            ] as { value: ManifestoYearFilter; en: string; ta: string }[]
          ).map(({ value, en, ta }) => (
            <button
              key={String(value)}
              onClick={() => setYearFilter(value)}
              className={`text-xs px-3 py-1.5 rounded-full border transition-colors font-medium ${
                yearFilter === value
                  ? "bg-gray-900 text-white border-gray-900"
                  : "bg-white text-gray-600 border-gray-300 hover:bg-gray-50"
              }`}
            >
              {isTA ? ta : en}
            </button>
          ))}
        </section>

        {/* Party selector */}
        <section className="bg-white rounded-2xl border border-gray-200 shadow-sm p-4 space-y-3">
          <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
            {isTA ? "ஒப்பிட இரண்டு கட்சிகளை தேர்வு செய்க" : "Select two parties to compare"}
          </p>
          <div className="flex gap-3 items-end">
            <PartySelector
              label={isTA ? "கட்சி A" : "Party A"}
              value={partyA}
              onChange={handlePartyAChange}
              excludeId={partyB}
              lang={lang}
            />
            <div className="pb-3 text-gray-400 font-bold text-sm shrink-0">vs</div>
            <PartySelector
              label={isTA ? "கட்சி B" : "Party B"}
              value={partyB}
              onChange={handlePartyBChange}
              excludeId={partyA}
              lang={lang}
            />
          </div>
        </section>

        {/* Pillar filter */}
        <section>
          <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2 px-1">
            {isTA ? "தூண் வாயிலாக தேடவும்" : "Search by Pillar"}
          </p>
          <PillarTabs
            selected={activePillar}
            onChange={setActivePillar}
            lang={lang}
            promiseCounts={promiseCounts}
          />
        </section>

        {/* Status banner */}
        {error ? (
          <div className="flex items-center gap-2 text-xs text-red-700 bg-red-50 border border-red-200 rounded-xl px-4 py-2.5">
            <span>⚠️</span>
            <span>
              {isTA
                ? "தரவு ஏற்றுவதில் பிழை. தயவுசெய்து பின்னர் முயற்சிக்கவும்."
                : "Failed to load data from backend API. Please try again later."}
            </span>
          </div>
        ) : (
          <div className="flex items-center gap-2 text-xs text-gray-500 bg-amber-50 border border-amber-200 rounded-xl px-4 py-2.5">
            <span>{loading ? "⏳" : "✓"}</span>
            <span>
              {loading
                ? (isTA ? "தரவு ஏற்றுகிறது…" : "Loading from backend API…")
                : isTA
                ? `${promises.length} வாக்குறுதிகள் ஏற்றப்பட்டன · ஒவ்வொரு அட்டையிலும் நம்பகத்தன்மை குறிப்பிடப்பட்டுள்ளது`
                : `${promises.length} promises loaded · Confidence level shown on every card · Click "Show source" to view PDF`}
            </span>
          </div>
        )}

        {/* Comparison grid or skeleton */}
        {loading ? (
          <ComparisonSkeleton rows={3} />
        ) : (
          <ComparisonMatrix
            promises={promises}
            partyA={partyA}
            partyB={partyB}
            pillar={activePillar}
            lang={lang}
          />
        )}

        {/* Legend */}
        <section className="bg-white rounded-2xl border border-gray-200 p-4 space-y-3">
          <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
            {isTA ? "நிலை விளக்கம்" : "Status Legend"}
          </p>
          <div className="flex flex-wrap gap-2 text-xs">
            {[
              { status: "Fulfilled",  en: "Implemented",          ta: "நிறைவேற்றப்பட்டது",   color: "bg-emerald-100 text-emerald-800" },
              { status: "Partial",    en: "Partially done",        ta: "பகுதி நிறைவு",         color: "bg-yellow-100 text-yellow-800" },
              { status: "Proposed",   en: "2026 Promise",          ta: "2026 வாக்குறுதி",      color: "bg-sky-100 text-sky-800" },
              { status: "Abandoned",  en: "Not implemented",       ta: "கைவிடப்பட்டது",        color: "bg-red-100 text-red-800" },
              { status: "Historical", en: "Historical reference",  ta: "வரலாற்று குறிப்பு",   color: "bg-gray-100 text-gray-700" },
            ].map(({ en, ta, color }) => (
              <span key={en} className={`px-2.5 py-1 rounded-full font-medium ${color}`}>
                {isTA ? ta : en}
              </span>
            ))}
          </div>
        </section>

        <footer className="text-center text-xs text-gray-400 pb-8">
          {isTA
            ? "அரசியல்ஆய்வு · தமிழ்நாடு தேர்தல் விழிப்புணர்வு · தரவு: கட்சி வலைத்தளங்கள் மற்றும் அதிகாரப்பூர்வ அறிக்கைகள்"
            : "ArasiyalAayvu · Tamil Nadu Election Awareness · Data: Official party manifestos & government records"}
        </footer>
      </div>
    </main>
  );
}
