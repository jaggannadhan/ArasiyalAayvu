"use client";

import { useState, useMemo } from "react";
import Link from "next/link";
import { useLanguage } from "@/lib/LanguageContext";
import { LiveCount } from "@/components/LiveCount";
import { TenureNavigator, TERMS } from "@/components/constituency/TenureNavigator";
import { PillarTabs } from "@/components/manifesto/PillarTabs";
import { PromiseCard } from "@/components/manifesto/PromiseCard";
import { ComparisonSkeleton } from "@/components/manifesto/PromiseSkeleton";
import { SDGAlignment, type SdgJumpTarget } from "@/components/manifesto/SDGAlignment";
import { useManifestos } from "@/hooks/useManifestos";
import {
  PILLARS,
  PILLAR_META,
  STATUS_META,
  TERM_COALITIONS,
  type Pillar,
  type PromiseStatus,
  type CoalitionParty,
} from "@/lib/types";

const HAS_DATA_FOR = new Set([2021, 2026]);
const STATUS_ORDER: PromiseStatus[] = ["Fulfilled", "Partial", "Proposed", "Abandoned", "Historical"];
const FILTER_STATUSES = ["All", "Fulfilled", "Partial", "Abandoned"] as const;
type StatusFilter = (typeof FILTER_STATUSES)[number];

// ---------------------------------------------------------------------------
// Party chip — clickable, active/inactive visual states
// ---------------------------------------------------------------------------
const PARTY_FLAG_EXT: Record<string, string> = {
  dmk: "svg", aiadmk: "svg", bjp: "svg", inc: "svg", pmk: "svg",
  cpi: "svg", cpim: "png", vck: "png", dmdk: "png", mdmk: "svg",
  ntk: "gif", tvk: "jpeg",
};

const PARTY_FULL_NAME: Record<string, { en: string; ta: string }> = {
  dmk:    { en: "Dravida Munnetra Kazhagam",          ta: "திராவிட முன்னேற்றக் கழகம்" },
  aiadmk: { en: "All India Anna Dravida Munnetra Kazhagam", ta: "அனைத்திந்திய அண்ணா திராவிட முன்னேற்றக் கழகம்" },
  bjp:    { en: "Bharatiya Janata Party",              ta: "பாரதிய ஜனதா கட்சி" },
  inc:    { en: "Indian National Congress",            ta: "இந்திய தேசிய காங்கிரஸ்" },
  pmk:    { en: "Pattali Makkal Katchi",               ta: "பட்டாளி மக்கள் கட்சி" },
  cpi:    { en: "Communist Party of India",            ta: "இந்திய கம்யூனிஸ்ட் கட்சி" },
  cpim:   { en: "Communist Party of India (Marxist)",  ta: "இந்திய கம்யூனிஸ்ட் கட்சி (மார்க்சிஸ்ட்)" },
  vck:    { en: "Viduthalai Chiruthaigal Katchi",      ta: "விடுதலைச் சிறுத்தைகள் கட்சி" },
  dmdk:   { en: "Desiya Murpokku Dravida Kazhagam",    ta: "தேசிய முற்போக்கு திராவிட கழகம்" },
  mdmk:   { en: "Marumalarchi Dravida Munnetra Kazhagam", ta: "மறுமலர்ச்சி திராவிட முன்னேற்றக் கழகம்" },
  ntk:    { en: "Naam Tamilar Katchi",                 ta: "நாம் தமிழர் கட்சி" },
  tvk:    { en: "Tamilaga Vettri Kazhagam",            ta: "தமிழக வெற்றி கழகம்" },
};

// ---------------------------------------------------------------------------
function PartyChip({
  p, lang, isActive, onClick,
}: {
  p: CoalitionParty;
  lang: "en" | "ta";
  isActive: boolean;
  onClick: () => void;
}) {
  const fullName = PARTY_FULL_NAME[p.id];
  const tooltip = fullName ? (lang === "ta" ? fullName.ta : fullName.en) : p.name;
  const ext = PARTY_FLAG_EXT[p.id];

  return (
    <button
      onClick={onClick}
      title={tooltip}
      style={ext ? {
        backgroundImage: `url(/party-flags/${p.id}.${ext})`,
        backgroundSize: "contain",
        backgroundPosition: "center",
        backgroundRepeat: "no-repeat",
      } : undefined}
      className={`relative overflow-hidden inline-flex items-center justify-center text-[11px] font-black px-3 py-1.5 rounded-lg transition-all cursor-pointer min-w-[52px] ${
        isActive
          ? "shadow-md ring-2 ring-offset-1 ring-gray-600"
          : "hover:scale-105"
      }`}
    >
      <span className={`absolute inset-0 transition-colors ${isActive ? "bg-black/20" : "bg-white/40"}`} />
      <span className={`relative z-10 tracking-wide ${isActive ? "text-white drop-shadow" : "text-gray-900"}`}>
        {lang === "ta" ? p.name_ta : p.name}
      </span>
    </button>
  );
}

// ---------------------------------------------------------------------------
// One row of the parties block
// ---------------------------------------------------------------------------
function CoalitionRow({
  label, parties, lang, selectedId, onSelect,
}: {
  label: string;
  parties: CoalitionParty[];
  lang: "en" | "ta";
  selectedId: string;
  onSelect: (id: string) => void;
}) {
  if (parties.length === 0) return null;
  return (
    <div>
      <p className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-1.5">{label}</p>
      <div className="flex flex-wrap gap-1.5">
        {parties.map((p) => (
          <PartyChip
            key={p.id}
            p={p}
            lang={lang}
            isActive={selectedId === p.id}
            onClick={() => onSelect(p.id)}
          />
        ))}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------
export default function ManifestoTrackerPage() {
  const { lang, setLang } = useLanguage();
  const [selectedTerm, setSelectedTerm] = useState(TERMS[TERMS.length - 1].electionYear);
  const [activePillar, setActivePillar] = useState<Pillar | "All">("All");
  const [activeStatus, setActiveStatus] = useState<StatusFilter>("All");
  const [view, setView] = useState<"promises" | "sdg">("promises");
  const [sdgFilter, setSdgFilter] = useState<SdgJumpTarget | null>(null);

  // selectedPartyId and activeStatus reset when term changes
  const [partyOverride, setPartyOverride] = useState<string | null>(null);
  const [lastTerm, setLastTerm] = useState(selectedTerm);
  if (lastTerm !== selectedTerm) {
    setLastTerm(selectedTerm);
    setPartyOverride(null);
    setActiveStatus("All");
    setActivePillar("All");
    setSdgFilter(null);
    setView("promises");
  }

  const isTA = lang === "ta";
  const hasData = HAS_DATA_FOR.has(selectedTerm);
  const coalition = TERM_COALITIONS[selectedTerm];
  const termMeta = TERMS.find((t) => t.electionYear === selectedTerm)!;

  const isUpcoming = coalition?.is_upcoming ?? false;

  const defaultPartyId = isUpcoming
    ? (coalition?.contesting?.[0]?.id ?? "")
    : (coalition?.ruling[0]?.id ?? "");
  const selectedPartyId = partyOverride ?? defaultPartyId;

  // All parties (flat list regardless of mode — for lookup)
  const allParties: CoalitionParty[] = useMemo(() => {
    if (!coalition) return [];
    if (isUpcoming) return coalition.contesting ?? [];
    return [...coalition.ruling, ...coalition.opposition, ...(coalition.others ?? [])];
  }, [coalition, isUpcoming]);

  const selectedParty = allParties.find((p) => p.id === selectedPartyId);

  const { promises, loading, error } = useManifestos(hasData ? selectedTerm as 2021 | 2026 : "all");

  // Party has data in Firestore?
  const partyHasData = useMemo(
    () => promises.some((p) => p.party_id === selectedPartyId),
    [promises, selectedPartyId]
  );

  // Pillar counts for selected party
  const promiseCounts = useMemo(() => {
    const counts: Partial<Record<Pillar, number>> = {};
    for (const pillar of PILLARS) {
      counts[pillar] = promises.filter(
        (p) => p.category === pillar && p.party_id === selectedPartyId
      ).length;
    }
    return counts;
  }, [promises, selectedPartyId]);

  // Status counts for selected party
  const statusCounts = useMemo(() => {
    const counts: Partial<Record<PromiseStatus, number>> = {};
    for (const p of promises.filter((p) => p.party_id === selectedPartyId)) {
      counts[p.status] = (counts[p.status] ?? 0) + 1;
    }
    return counts;
  }, [promises, selectedPartyId]);

  // Visible promise list
  const visiblePromises = useMemo(() => {
    return promises
      .filter((p) =>
        p.party_id === selectedPartyId &&
        (sdgFilter
          ? sdgFilter.highlighted_doc_ids.includes(p.doc_id)
          : activePillar === "All" || p.category === activePillar) &&
        (activeStatus === "All" || p.status === activeStatus)
      )
      .sort((a, b) => STATUS_ORDER.indexOf(a.status) - STATUS_ORDER.indexOf(b.status));
  }, [promises, selectedPartyId, activePillar, activeStatus, sdgFilter]);

  // All promises for the selected party — used for SDG alignment (no pillar/status filter)
  const partyPromises = useMemo(
    () => promises.filter((p) => p.party_id === selectedPartyId),
    [promises, selectedPartyId]
  );

  function handlePartySelect(id: string) {
    setPartyOverride(id);
    setActiveStatus("All");
    setActivePillar("All");
    setSdgFilter(null);
    setView("promises");
  }

  function handleTermChange(year: number) {
    setSelectedTerm(year);
    setActivePillar("All");
  }

  return (
    <main className="min-h-full bg-gray-50">
      {/* Header */}
      <header className="sticky top-0 z-20 bg-white border-b border-gray-200 shadow-sm">
        <div className="max-w-2xl mx-auto px-4 py-3 flex items-center justify-between gap-3">
          <div className="flex items-center gap-2 min-w-0">
            <Link href="/" className="text-gray-400 hover:text-gray-700 shrink-0 text-lg">←</Link>
            <div className="min-w-0">
              <h1 className="text-sm font-black text-gray-900 leading-tight">
                {isTA ? "தேர்தல் அறிக்கை கண்காணிப்பு" : "Manifesto Tracker"}
              </h1>
              <p className="text-xs text-gray-500 truncate">
                {isTA ? "வாக்குறுதி vs செயல்திறன் · தமிழ்நாடு" : "Promise vs. Performance · Tamil Nadu"}
              </p>
            </div>
          </div>
          <div className="flex items-center gap-3 shrink-0">
            <LiveCount />
            <button
              onClick={() => setLang(lang === "en" ? "ta" : "en")}
              className="text-xs font-bold px-3 py-1.5 rounded-full border border-gray-300 hover:bg-gray-100 transition-colors text-gray-900"
            >
              {lang === "en" ? "தமிழ்" : "English"}
            </button>
          </div>
        </div>
      </header>

      <div className="max-w-2xl mx-auto px-4 py-5 space-y-4">

        {/* Tenure navigator */}
        <div className="bg-white rounded-2xl border border-gray-200 shadow-sm px-4 py-3">
          <TenureNavigator selectedYear={selectedTerm} onChange={handleTermChange} lang={lang} />
        </div>

        {/* Parties in play */}
        {coalition && (
          <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-4 space-y-3">
            <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
              {isTA ? "கட்சிகள்" : "Parties"} · {termMeta.label}
            </h3>

            {isUpcoming ? (
              /* Upcoming election — flat contesting parties, no ruling/opposition */
              <div>
                <p className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-1.5">
                  {isTA ? "போட்டியிடும் கட்சிகள்" : "Contesting Parties"}
                </p>
                <div className="flex flex-wrap gap-1.5">
                  {(coalition.contesting ?? []).map((p) => (
                    <PartyChip
                      key={p.id}
                      p={p}
                      lang={lang}
                      isActive={selectedPartyId === p.id}
                      onClick={() => handlePartySelect(p.id)}
                    />
                  ))}
                </div>
              </div>
            ) : coalition.ruling.length > 0 ? (
              <>
                <CoalitionRow
                  label={isTA ? `🟢 ஆளும் கூட்டணி · ${coalition.ruling_label_ta}` : `🟢 Ruling · ${coalition.ruling_label_en}`}
                  parties={coalition.ruling}
                  lang={lang}
                  selectedId={selectedPartyId}
                  onSelect={handlePartySelect}
                />
                <CoalitionRow
                  label={isTA ? "🔵 எதிர்க்கட்சி" : "🔵 Opposition"}
                  parties={coalition.opposition}
                  lang={lang}
                  selectedId={selectedPartyId}
                  onSelect={handlePartySelect}
                />
                <CoalitionRow
                  label={isTA ? "○ மற்ற கட்சிகள்" : "○ Other Parties"}
                  parties={coalition.others ?? []}
                  lang={lang}
                  selectedId={selectedPartyId}
                  onSelect={handlePartySelect}
                />
              </>
            ) : (
              <p className="text-xs text-gray-400 italic">
                {isTA ? coalition.ruling_label_ta : coalition.ruling_label_en}
              </p>
            )}
          </div>
        )}

        {/* Selected party indicator */}
        {coalition && selectedParty && (() => {
          const ext = PARTY_FLAG_EXT[selectedParty.id];
          return (
            <div className="overflow-hidden rounded-2xl border border-gray-200 shadow-sm bg-white flex items-stretch min-h-40">
              {ext && (
                <img
                  src={`/party-flags/${selectedParty.id}.${ext}`}
                  alt=""
                  aria-hidden
                  className="flex-shrink-0 w-[200px] object-cover self-stretch pointer-events-none select-none"
                />
              )}
              <div className="flex-1 flex flex-col items-center justify-center text-center px-6 py-5">
                <p className="text-[10px] font-semibold text-gray-600 uppercase tracking-widest mb-1">
                  {isTA ? "காண்பிக்கப்படுவது" : "Viewing Manifesto"}
                </p>
                <p className="text-xl font-black text-gray-900 leading-tight">
                  {PARTY_FULL_NAME[selectedParty.id]
                    ? (isTA ? PARTY_FULL_NAME[selectedParty.id].ta : PARTY_FULL_NAME[selectedParty.id].en)
                    : (isTA ? selectedParty.name_ta : selectedParty.name)}
                  <span className="text-base font-bold text-gray-500 ml-1.5">
                    ({selectedParty.name})
                  </span>
                </p>
              </div>
            </div>
          );
        })()}

        {/* View toggle — only for 2026 when party has data */}
        {hasData && isUpcoming && partyHasData && !loading && !error && (
          <div className="flex rounded-xl overflow-hidden border border-gray-200 bg-white shadow-sm">
            <button
              onClick={() => setView("promises")}
              className={`flex-1 py-2.5 text-xs font-bold transition-colors cursor-pointer ${
                view === "promises"
                  ? "bg-gray-900 text-white"
                  : "text-gray-500 hover:text-gray-800 hover:bg-gray-50"
              }`}
            >
              📜 {isTA ? "வாக்குறுதிகள்" : "Promises"}
            </button>
            <button
              onClick={() => setView("sdg")}
              className={`flex-1 py-2.5 text-xs font-bold transition-colors cursor-pointer border-l border-gray-200 ${
                view === "sdg"
                  ? "bg-gray-900 text-white"
                  : "text-gray-500 hover:text-gray-800 hover:bg-gray-50"
              }`}
            >
              🌍 {isTA ? "SDG ஒப்படைவு" : "SDG Alignment"}
            </button>
          </div>
        )}

        {/* SDG Alignment view */}
        {hasData && isUpcoming && view === "sdg" && partyHasData && selectedParty && (
          <SDGAlignment
            promises={partyPromises}
            partyName={
              PARTY_FULL_NAME[selectedParty.id]?.en ?? selectedParty.name
            }
            partyNameTa={
              PARTY_FULL_NAME[selectedParty.id]?.ta ?? selectedParty.name_ta
            }
            lang={lang}
            onJumpToPromises={(target) => {
              setSdgFilter(target);
              setActivePillar("All");
              setView("promises");
            }}
          />
        )}

        {/* No data for this term */}
        {!hasData && (
          <div className="rounded-2xl border border-gray-200 bg-white px-5 py-10 text-center space-y-1">
            <p className="text-2xl">📋</p>
            <p className="text-sm font-semibold text-gray-700">
              {isTA
                ? `${termMeta.label} காலத்திற்கான அறிக்கை தரவு இல்லை`
                : `Manifesto data for ${termMeta.label} not yet available`}
            </p>
            <p className="text-xs text-gray-400">
              {isTA ? "2021–2026 காலத்திற்கு திரும்பவும்" : "Switch to 2021–2026 to view tracked promises"}
            </p>
          </div>
        )}

        {/* Data section — hidden when SDG view is active */}
        {hasData && view === "promises" && (
          <>
            {/* No manifesto data for selected party */}
            {!loading && !error && !partyHasData && (
              <div className="rounded-2xl border border-gray-200 bg-white px-5 py-8 text-center space-y-1">
                <p className="text-xl">📄</p>
                <p className="text-sm font-semibold text-gray-700">
                  {isTA
                    ? `${selectedParty ? (isTA ? selectedParty.name_ta : selectedParty.name) : selectedPartyId} அறிக்கை தரவு இல்லை`
                    : `No manifesto data available for ${selectedParty?.name ?? selectedPartyId}`}
                </p>
                <p className="text-xs text-gray-400">
                  {isTA ? "தற்போது DMK மற்றும் AIADMK தரவு மட்டுமே உள்ளது" : "Currently only DMK and AIADMK data is available"}
                </p>
              </div>
            )}

            {/* Filter by card */}
            {!loading && !error && partyHasData && (
              <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-4 space-y-4">
                <h3 className="text-base font-black text-gray-900 leading-tight">
                  {isTA ? "வடிகட்டு" : "Filter by"}
                </h3>

                {/* Status — hidden for upcoming elections (all promises are Proposed) */}
                {!isUpcoming && (
                  <div className="space-y-2">
                    <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
                      {isTA ? "நிலை" : "Status"}
                    </p>
                    <div className="flex flex-wrap gap-2">
                      <button
                        onClick={() => setActiveStatus("All")}
                        className={`flex items-center gap-1.5 px-3 py-1.5 rounded-xl cursor-pointer transition-all ${
                          activeStatus === "All"
                            ? "bg-gray-900 text-white"
                            : "bg-gray-100 text-gray-600 hover:bg-gray-200"
                        }`}
                      >
                        <span className="text-lg font-black leading-none">
                          {Object.values(statusCounts).reduce((a, b) => (a ?? 0) + (b ?? 0), 0)}
                        </span>
                        <span className="text-xs font-semibold">
                          {isTA ? "அனைத்தும்" : "All"}
                        </span>
                      </button>
                      {(["Fulfilled", "Partial", "Abandoned"] as PromiseStatus[]).map((s) => {
                        const meta = STATUS_META[s];
                        const count = statusCounts[s] ?? 0;
                        const isActive = activeStatus === s;
                        return (
                          <button
                            key={s}
                            onClick={() => setActiveStatus(s as StatusFilter)}
                            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-xl cursor-pointer transition-all ${
                              isActive ? "bg-gray-900 text-white" : `${meta.bg} hover:opacity-80`
                            }`}
                          >
                            <span className={`text-lg font-black leading-none ${isActive ? "text-white" : meta.text}`}>
                              {count}
                            </span>
                            <span className={`text-xs font-semibold ${isActive ? "text-white" : meta.text}`}>
                              {isTA ? meta.label_ta : meta.label_en}
                            </span>
                          </button>
                        );
                      })}
                    </div>
                  </div>
                )}

                {/* Categories */}
                <div className="space-y-2">
                  <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
                    {isTA ? "வகைகள்" : "Categories"}
                  </p>
                  <div className="flex flex-wrap gap-2">
                    {/* All categories button */}
                    <button
                      onClick={() => { setSdgFilter(null); setActivePillar("All"); }}
                      className={`flex items-center gap-1.5 px-3 py-1.5 rounded-xl text-xs font-semibold cursor-pointer transition-all border ${
                        !sdgFilter && activePillar === "All"
                          ? "bg-gray-900 text-white border-gray-900"
                          : "bg-white text-gray-600 border-gray-200 hover:border-gray-400 hover:text-gray-900"
                      }`}
                    >
                      <span>{isTA ? "அனைத்தும்" : "All"}</span>
                      <span className={`text-[10px] font-bold px-1 rounded-full ${!sdgFilter && activePillar === "All" ? "bg-white/20 text-white" : "bg-gray-100 text-gray-500"}`}>
                        {Object.values(promiseCounts).reduce((a, b) => (a ?? 0) + (b ?? 0), 0)}
                      </span>
                    </button>
                    {PILLARS.map((pillar) => {
                      const meta = PILLAR_META[pillar];
                      const count = promiseCounts[pillar] ?? 0;
                      const isActive = sdgFilter
                        ? sdgFilter.pillars.includes(pillar)
                        : activePillar === pillar;
                      return (
                        <button
                          key={pillar}
                          onClick={() => { setSdgFilter(null); setActivePillar(pillar); }}
                          className={`flex items-center gap-1.5 px-3 py-1.5 rounded-xl text-xs font-semibold cursor-pointer transition-all border ${
                            isActive
                              ? "bg-gray-900 text-white border-gray-900"
                              : "bg-white text-gray-600 border-gray-200 hover:border-gray-400 hover:text-gray-900"
                          }`}
                        >
                          <span>{meta.icon}</span>
                          <span>{isTA ? meta.tamil : pillar}</span>
                          <span className={`text-[10px] font-bold px-1 rounded-full ${isActive ? "bg-white/20 text-white" : "bg-gray-100 text-gray-500"}`}>
                            {count}
                          </span>
                        </button>
                      );
                    })}
                  </div>

                  {/* SDG filter chip — shown when arriving from SDG tab */}
                  {sdgFilter && (
                    <div className="flex items-center gap-2 pt-1">
                      <span className="text-[10px] font-semibold text-gray-400 uppercase tracking-wide">
                        {isTA ? "வடிகட்டி" : "Filter"}:
                      </span>
                      <span className="inline-flex items-center gap-1.5 text-[11px] font-semibold bg-indigo-50 border border-indigo-200 text-indigo-800 px-2.5 py-1 rounded-full">
                        SDG {sdgFilter.sdg_id} · {isTA ? sdgFilter.sdg_name_ta : sdgFilter.sdg_name}
                        <button
                          onClick={() => { setSdgFilter(null); setActivePillar("All"); }}
                          aria-label="Clear SDG filter"
                          className="ml-0.5 text-indigo-400 hover:text-indigo-700 font-black leading-none transition-colors"
                        >
                          ✕
                        </button>
                      </span>
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Error */}
            {error && (
              <div className="rounded-2xl border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700">
                {isTA ? "தரவு ஏற்றுவதில் பிழை." : `Failed to load: ${error}`}
              </div>
            )}

            {/* Promise list */}
            {loading ? (
              <ComparisonSkeleton rows={4} />
            ) : partyHasData ? (
              <div className="space-y-3">
                {visiblePromises.length === 0 ? (
                  <div className="text-center py-8 text-sm text-gray-400">
                    {isTA ? "இந்த தூணில் வாக்குறுதிகள் இல்லை" : "No promises in this pillar"}
                  </div>
                ) : sdgFilter ? (
                  // SDG mode — group by each contributing pillar
                  sdgFilter.pillars.map((pillar) => {
                    const group = visiblePromises.filter((p) => p.category === pillar);
                    if (group.length === 0) return null;
                    const meta = PILLAR_META[pillar];
                    return (
                      <div key={pillar} className="space-y-2">
                        <div className="flex items-center gap-2 px-1 pt-1">
                          <span className="text-sm">{meta.icon}</span>
                          <p className="text-xs font-bold text-gray-700 uppercase tracking-wide">
                            {isTA ? meta.tamil : pillar}
                          </p>
                          <span className="text-[10px] font-semibold text-gray-400 bg-gray-100 px-1.5 py-0.5 rounded-full">
                            {group.length}
                          </span>
                        </div>
                        {group.map((p) => (
                          <PromiseCard
                            key={p.doc_id}
                            promise={p}
                            lang={lang}
                            highlighted={sdgFilter.highlighted_doc_ids.includes(p.doc_id)}
                          />
                        ))}
                      </div>
                    );
                  })
                ) : (
                  visiblePromises.map((p) => (
                    <PromiseCard key={p.doc_id} promise={p} lang={lang} />
                  ))
                )}
              </div>
            ) : null}
          </>
        )}

        <footer className="text-center text-xs text-gray-400 pb-8">
          {isTA
            ? "அரசியல்ஆய்வு · தரவு: கட்சி வலைத்தளங்கள் மற்றும் அதிகாரப்பூர்வ அரசாணைகள்"
            : "ArasiyalAayvu · Data: Official party manifestos & government orders"}
        </footer>
      </div>
    </main>
  );
}
