"use client";

import { useMemo, useState } from "react";
import type { ManifestoPromise, Pillar } from "@/lib/types";
import {
  computeSDGCoverage,
  coveredSDGs,
  uncoveredSDGs,
  blockedBy,
  type SDGCoverage,
  type SDGCoverageMap,
  type CoverageQuality,
} from "@/lib/sdg-mapping";
import { SDG_GOALS } from "@/lib/sdg-data";

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

export interface SdgJumpTarget {
  sdg_id: number;
  sdg_name: string;
  sdg_name_ta: string;
  pillars: Pillar[];
  /** doc_ids of the top-scoring promises for this SDG (ranked by impact score) */
  highlighted_doc_ids: string[];
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function goalById(id: number) {
  return SDG_GOALS.find((g) => g.id === id);
}

const QUALITY_STYLE: Record<CoverageQuality, {
  dot: string; badge_bg: string; badge_text: string;
  label_en: string; label_ta: string;
}> = {
  strong:   { dot: "bg-emerald-500 text-white", badge_bg: "bg-emerald-100", badge_text: "text-emerald-700", label_en: "Strong",   label_ta: "வலுவான" },
  moderate: { dot: "bg-blue-400 text-white",    badge_bg: "bg-blue-100",    badge_text: "text-blue-700",    label_en: "Moderate",  label_ta: "மிதமான" },
  weak:     { dot: "bg-amber-400 text-white",   badge_bg: "bg-amber-100",   badge_text: "text-amber-700",   label_en: "Weak",      label_ta: "பலவீன" },
  none:     { dot: "bg-gray-200 text-gray-400", badge_bg: "bg-gray-100",    badge_text: "text-gray-500",    label_en: "Not addressed", label_ta: "உரையாற்றப்படவில்லை" },
};

// ---------------------------------------------------------------------------
// 17-dot coverage bar
// ---------------------------------------------------------------------------
function CoverageBar({ map, lang }: { map: SDGCoverageMap; lang: "en" | "ta" }) {
  const covered = coveredSDGs(map).length;
  const total = 17;
  const isTA = lang === "ta";
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between text-xs">
        <span className="font-semibold text-gray-600">
          {isTA ? `${total} இல் ${covered} SDG உரையாற்றப்பட்டது` : `${covered} of ${total} SDGs addressed`}
        </span>
        <span className="text-gray-400">{total - covered} {isTA ? "இடைவெளிகள்" : "gaps"}</span>
      </div>
      <div className="flex gap-1 flex-wrap">
        {Array.from({ length: total }, (_, i) => i + 1).map((id) => {
          const cov = map.get(id);
          const goal = goalById(id);
          const quality = cov?.coverage_quality ?? "none";
          const hasBreak = (cov?.chain_breaks.length ?? 0) > 0;
          return (
            <div
              key={id}
              title={goal ? (isTA ? goal.name_ta : goal.name) : `SDG ${id}`}
              className={`relative h-4 w-4 rounded-sm flex items-center justify-center text-[8px] font-black transition-all ${QUALITY_STYLE[quality].dot}`}
            >
              {id}
              {hasBreak && (
                <span className="absolute -top-0.5 -right-0.5 w-1.5 h-1.5 bg-orange-400 rounded-full" />
              )}
            </div>
          );
        })}
      </div>
      <div className="flex flex-wrap gap-3 text-[10px] text-gray-500">
        <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-sm bg-emerald-500 inline-block" />{isTA ? "வலுவான" : "Strong"}</span>
        <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-sm bg-blue-400 inline-block" />{isTA ? "மிதமான" : "Moderate"}</span>
        <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-sm bg-amber-400 inline-block" />{isTA ? "பலவீன" : "Weak"}</span>
        <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-sm bg-gray-200 inline-block" />{isTA ? "இல்லை" : "Not addressed"}</span>
        <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-orange-400 inline-block" />{isTA ? "சங்கிலி உடைப்பு" : "Chain break"}</span>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Single covered SDG card
// ---------------------------------------------------------------------------
const DEPTH_BADGE: Record<string, { bg: string; text: string; label_en: string; label_ta: string }> = {
  transformative: { bg: "bg-emerald-50",  text: "text-emerald-700", label_en: "Transformative", label_ta: "உருமாற்றம்" },
  substantive:    { bg: "bg-blue-50",     text: "text-blue-700",    label_en: "Substantive",    label_ta: "குறிப்பிடத்தக்க" },
  supplemental:   { bg: "bg-yellow-50",   text: "text-yellow-700",  label_en: "Supplemental",   label_ta: "துணை" },
  symbolic:       { bg: "bg-gray-50",     text: "text-gray-500",    label_en: "Symbolic",       label_ta: "குறியீட்டு" },
};

function CoveredSDGCard({
  cov, lang, onJumpToPromises, onHowClick,
}: {
  cov: SDGCoverage;
  lang: "en" | "ta";
  onJumpToPromises?: (target: SdgJumpTarget) => void;
  onHowClick?: () => void;
}) {
  const goal = goalById(cov.sdg_id);
  if (!goal) return null;

  const isTA = lang === "ta";
  const qs = QUALITY_STYLE[cov.coverage_quality];

  return (
    <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
      {/* Header strip */}
      <div className={`${goal.color_bg} px-3 py-2 flex items-center gap-2`}>
        <span className="text-base">{goal.icon}</span>
        <div className="flex-1 min-w-0">
          <p className={`text-[9px] font-bold uppercase tracking-widest ${goal.color_text} opacity-70`}>SDG {goal.id}</p>
          <p className={`text-xs font-black ${goal.color_text} leading-tight`}>
            {isTA ? goal.name_ta : goal.name}
          </p>
        </div>
        <div className="flex-shrink-0 flex flex-col items-end gap-1">
          {/* Quality tier badge */}
          <span className={`text-[9px] font-black px-2 py-0.5 rounded-full ${qs.badge_bg} ${qs.badge_text}`}>
            {isTA ? qs.label_ta : qs.label_en}
          </span>
          {/* Promise count — clickable → jump to Promises tab */}
          <button
            onClick={onJumpToPromises ? () => onJumpToPromises({
              sdg_id: cov.sdg_id,
              sdg_name: goal.name,
              sdg_name_ta: goal.name_ta,
              pillars: cov.contributing_pillars,
              highlighted_doc_ids: cov.top_promises.map((p) => p.doc_id),
            }) : undefined}
            title={onJumpToPromises ? (isTA ? "வாக்குறுதிகளுக்கு செல்" : "View these promises") : undefined}
            className={`text-[9px] font-semibold px-1.5 py-0.5 rounded-full bg-white/30 ${goal.color_text} ${
              onJumpToPromises ? "cursor-pointer hover:bg-white/50 active:scale-95 transition-all" : "cursor-default"
            }`}
          >
            {cov.top_promises.length} {isTA ? "சிறந்த வாக்குறுதி" : cov.top_promises.length === 1 ? "top promise" : "top promises"}
            {onJumpToPromises && <span className="ml-1 opacity-60">↗</span>}
          </button>
        </div>
      </div>

      <div className="px-3 py-2.5 space-y-2">
        {/* Pillars */}
        <div className="flex flex-wrap gap-1">
          {cov.contributing_pillars.map((p) => (
            <span key={p} className="text-[10px] bg-gray-100 text-gray-600 px-2 py-0.5 rounded-full font-semibold">
              {p}
            </span>
          ))}
        </div>

        {/* Coverage gap notes (from welfare assessment) */}
        {cov.top_gap_notes.length > 0 && (
          <div className="bg-yellow-50 border border-yellow-200 rounded-lg px-2.5 py-2 space-y-1">
            <p className="text-[10px] font-bold text-yellow-700 uppercase tracking-wide">
              ⚠ {isTA ? "கவரேஜ் இடைவெளி" : "Coverage gap"}
            </p>
            {cov.top_gap_notes.map((note, i) => (
              <p key={i} className="text-[10px] text-yellow-700 leading-snug">{note}</p>
            ))}
          </div>
        )}

        {/* Chain break warning */}
        {cov.chain_breaks.length > 0 && (
          <div className="bg-orange-50 border border-orange-200 rounded-lg px-2.5 py-2 space-y-1">
            <p className="text-[10px] font-bold text-orange-700 uppercase tracking-wide">
              ⚠ {isTA ? "சங்கிலி உடைப்பு" : "Chain Break"}
            </p>
            {cov.chain_breaks.map((depId) => {
              const dep = goalById(depId);
              return (
                <p key={depId} className="text-[10px] text-orange-600 leading-snug">
                  {isTA
                    ? `SDG ${depId} (${dep?.name_ta ?? depId}) — இந்த அறிக்கையில் உரையாற்றப்படவில்லை`
                    : `SDG ${depId} (${dep?.name ?? depId}) — not addressed in this manifesto`}
                </p>
              );
            })}
            <p className="text-[10px] text-orange-500 italic leading-snug">
              {isTA
                ? "இந்த SDG வாக்குறுதிகள் மேலே உள்ள இடைவெளிகளால் கட்டமைப்பு ரீதியாக முழுமையடையவில்லை."
                : "These promises are structurally incomplete without the gaps above being addressed."}
            </p>
          </div>
        )}

        {/* Dependencies fully covered — positive signal */}
        {cov.chain_breaks.length === 0 && cov.dependency_ids.length > 0 && (
          <p className="text-[10px] text-emerald-600 font-semibold">
            ✓ {isTA ? "சங்கிலி முழுமையானது" : "Chain complete"} —{" "}
            {isTA ? "அனைத்து சார்பு SDG-களும் உரையாற்றப்பட்டன" : "all dependency SDGs are addressed"}
          </p>
        )}

        {/* How? button — opens deep-analysis modal */}
        {onHowClick && cov.top_promises.length > 0 && (
          <button
            onClick={onHowClick}
            className="text-[11px] font-semibold text-indigo-600 hover:text-indigo-800 transition-colors w-full text-left"
          >
            {isTA ? "எப்படி? →" : "How? →"}
          </button>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Single uncovered SDG card
// ---------------------------------------------------------------------------
function UncoveredSDGCard({ sdgId, map, lang }: { sdgId: number; map: SDGCoverageMap; lang: "en" | "ta" }) {
  const goal = goalById(sdgId);
  if (!goal) return null;
  const isTA = lang === "ta";

  // Which covered SDGs does this gap break?
  const blockedGoals = blockedBy(sdgId, map);

  return (
    <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
      {/* Muted header */}
      <div className="bg-gray-100 px-3 py-2 flex items-center gap-2">
        <span className="text-base grayscale opacity-60">{goal.icon}</span>
        <div className="flex-1 min-w-0">
          <p className="text-[9px] font-bold uppercase tracking-widest text-gray-400">SDG {goal.id}</p>
          <p className="text-xs font-black text-gray-500 leading-tight">
            {isTA ? goal.name_ta : goal.name}
          </p>
        </div>
        <span className="text-[10px] font-bold text-gray-400 bg-gray-200 px-2 py-0.5 rounded-full">
          {isTA ? "உரையாற்றப்படவில்லை" : "Not addressed"}
        </span>
      </div>

      <div className="px-3 py-2.5 space-y-2">
        {/* TN current score */}
        {goal.tn_score !== undefined && (
          <div className="flex items-center gap-2">
            <div className="flex-1">
              <div className="flex justify-between text-[10px] mb-0.5">
                <span className="text-gray-500">{isTA ? "தமிழ்நாடு தற்போதைய மதிப்பெண்" : "TN current score"}</span>
                <span className="font-bold text-gray-700">{goal.tn_score}/100</span>
              </div>
              <div className="h-1.5 bg-gray-100 rounded-full">
                <div
                  className={`h-1.5 rounded-full ${goal.color_bg} opacity-60`}
                  style={{ width: `${goal.tn_score}%` }}
                />
              </div>
            </div>
            {goal.india_score !== undefined && (
              <span className="text-[9px] text-gray-400 flex-shrink-0">
                IN avg: {goal.india_score}
              </span>
            )}
          </div>
        )}

        {/* Key metric from sdg-data */}
        {goal.metrics[0] && (
          <div className="bg-gray-50 rounded-lg px-2.5 py-2">
            <p className="text-[10px] font-semibold text-gray-600">
              {isTA ? goal.metrics[0].label_ta : goal.metrics[0].label}
            </p>
            <p className="text-sm font-black text-gray-800">{goal.metrics[0].value}</p>
            {goal.metrics[0].context && (
              <p className="text-[9px] text-gray-400 mt-0.5">{goal.metrics[0].context}</p>
            )}
          </div>
        )}

        {/* Impact — which covered SDGs this breaks */}
        {blockedGoals.length > 0 && (
          <div className="bg-red-50 border border-red-100 rounded-lg px-2.5 py-2">
            <p className="text-[10px] font-bold text-red-600 mb-1">
              {isTA ? "இந்த இடைவெளியால் பாதிக்கப்படும் SDG-கள்:" : "This gap creates chain breaks in:"}
            </p>
            {blockedGoals.map((bid) => {
              const bg = goalById(bid);
              return (
                <p key={bid} className="text-[10px] text-red-500">
                  SDG {bid} — {isTA ? (bg?.name_ta ?? bid) : (bg?.name ?? bid)}
                </p>
              );
            })}
          </div>
        )}

        {/* Description */}
        <p className="text-[10px] text-gray-400 leading-relaxed italic">
          {isTA ? goal.description_ta : goal.description_en}
        </p>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------
interface SDGAlignmentProps {
  promises: ManifestoPromise[];
  partyName: string;
  partyNameTa: string;
  lang: "en" | "ta";
  onJumpToPromises?: (target: SdgJumpTarget) => void;
}

export function SDGAlignment({ promises, partyName, partyNameTa, lang, onJumpToPromises }: SDGAlignmentProps) {
  const isTA = lang === "ta";
  const [howModalSdgId, setHowModalSdgId] = useState<number | null>(null);

  const coverageMap = useMemo(() => computeSDGCoverage(promises), [promises]);
  const covered = useMemo(() => coveredSDGs(coverageMap), [coverageMap]);
  const uncovered = useMemo(() => uncoveredSDGs(coverageMap), [coverageMap]);

  const chainBreakCount = covered.filter(
    (id) => (coverageMap.get(id)?.chain_breaks.length ?? 0) > 0
  ).length;

  // Sort uncovered: SDGs that create chain breaks first (most impactful gaps)
  const sortedUncovered = useMemo(() => {
    return [...uncovered].sort((a, b) => {
      const aBlocks = blockedBy(a, coverageMap).length;
      const bBlocks = blockedBy(b, coverageMap).length;
      return bBlocks - aBlocks;
    });
  }, [uncovered, coverageMap]);

  if (promises.length === 0) {
    return (
      <div className="bg-white rounded-2xl border border-gray-200 px-5 py-10 text-center">
        <p className="text-2xl mb-2">📄</p>
        <p className="text-sm font-semibold text-gray-700">
          {isTA ? "இந்தக் கட்சிக்கான அறிக்கை தரவு இல்லை" : "No manifesto data available for this party"}
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-4">

      {/* Summary banner */}
      <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-4 space-y-4">
        <div>
          <p className="text-[10px] font-bold uppercase tracking-widest text-gray-400 mb-0.5">
            {isTA ? "SDG ஒப்படைவு · 2026 தேர்தல் அறிக்கை" : "SDG Alignment · 2026 Election Manifesto"}
          </p>
          <p className="text-base font-black text-gray-900">
            {isTA ? partyNameTa : partyName}
          </p>
        </div>

        {/* Coverage bar */}
        <CoverageBar map={coverageMap} lang={lang} />

        {/* Chain break summary */}
        {chainBreakCount > 0 && (
          <div className="bg-orange-50 border border-orange-200 rounded-xl px-3 py-2.5">
            <p className="text-xs font-bold text-orange-700">
              ⚠ {chainBreakCount} {isTA ? "சங்கிலி உடைப்புகள் கண்டறியப்பட்டன" : "chain breaks detected"}
            </p>
            <p className="text-[10px] text-orange-600 mt-0.5 leading-snug">
              {isTA
                ? "சில SDG-கள் உரையாற்றப்பட்டாலும், அவற்றின் சார்பு இலக்குகள் இந்த அறிக்கையில் இல்லாத காரணத்தால் கட்டமைப்பு ரீதியாக முழுமையடையவில்லை."
                : "Some SDGs are addressed, but their structural prerequisites are missing from this manifesto — limiting real-world impact."}
            </p>
          </div>
        )}
      </div>

      {/* Covered SDGs */}
      {covered.length > 0 && (
        <div className="space-y-2">
          <p className="text-xs font-bold text-gray-500 uppercase tracking-wide px-1">
            {isTA ? `✓ உரையாற்றப்பட்ட SDG-கள் (${covered.length})` : `✓ Addressed SDGs (${covered.length})`}
          </p>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
            {covered.map((id) => {
              const cov = coverageMap.get(id);
              if (!cov) return null;
              return (
                <CoveredSDGCard
                  key={id}
                  cov={cov}
                  lang={lang}
                  onJumpToPromises={onJumpToPromises}
                  onHowClick={() => setHowModalSdgId(id)}
                />
              );
            })}
          </div>
        </div>
      )}

      {/* Uncovered SDGs */}
      {uncovered.length > 0 && (
        <div className="space-y-2">
          <p className="text-xs font-bold text-gray-500 uppercase tracking-wide px-1">
            {isTA ? `✗ உரையாற்றப்படாத SDG-கள் (${uncovered.length})` : `✗ Not Addressed (${uncovered.length})`}
          </p>
          <p className="text-[10px] text-gray-400 px-1 -mt-1">
            {isTA
              ? "தமிழ்நாட்டின் தற்போதைய மதிப்பெண்கள் மற்றும் இந்த இடைவெளிகளின் தாக்கம்"
              : "TN's current scores and the impact of these policy gaps"}
          </p>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
            {sortedUncovered.map((id) => (
              <UncoveredSDGCard key={id} sdgId={id} map={coverageMap} lang={lang} />
            ))}
          </div>
        </div>
      )}

      {/* Footer note */}
      <p className="text-[10px] text-gray-400 text-center px-2 pb-2 leading-relaxed">
        {isTA
          ? "SDG தரம் = வாக்குறுதியின் தாக்க ஆழம் × பயனாளி பரப்பு × செயல்படுத்தல் ஆபத்து. ஆதாரம்: NITI Aayog SDG India Index 2023-24."
          : "SDG quality = impact depth × beneficiary breadth × delivery risk. Source: NITI Aayog SDG India Index 2023-24."}
      </p>

      {/* "How?" deep-analysis modal */}
      {howModalSdgId !== null && (() => {
        const cov = coverageMap.get(howModalSdgId);
        const goal = goalById(howModalSdgId);
        if (!cov || !goal) return null;
        return (
          <div
            className="fixed inset-0 z-50 flex items-end sm:items-center justify-center p-4 bg-black/50"
            onClick={() => setHowModalSdgId(null)}
          >
            <div
              className="bg-white rounded-2xl w-full max-w-lg max-h-[85vh] overflow-y-auto shadow-2xl"
              onClick={(e) => e.stopPropagation()}
            >
              {/* Modal header */}
              <div className={`${goal.color_bg} px-4 py-3 flex items-center gap-2 sticky top-0 rounded-t-2xl`}>
                <span className="text-lg">{goal.icon}</span>
                <div className="flex-1 min-w-0">
                  <p className={`text-[9px] font-bold uppercase tracking-widest ${goal.color_text} opacity-70`}>
                    {isTA ? "எப்படி உதவுகிறது?" : "How does this address the SDG?"}
                  </p>
                  <p className={`text-sm font-black ${goal.color_text} leading-tight`}>
                    SDG {goal.id} · {isTA ? goal.name_ta : goal.name}
                  </p>
                </div>
                <button
                  onClick={() => setHowModalSdgId(null)}
                  className={`${goal.color_text} opacity-70 hover:opacity-100 font-black text-xl leading-none transition-opacity`}
                >
                  ✕
                </button>
              </div>

              {/* Modal body — one block per top promise */}
              <div className="p-4 space-y-4">
                {cov.top_promises.map((p, idx) => {
                  const db = p.impact_depth ? DEPTH_BADGE[p.impact_depth] : null;
                  return (
                    <div key={p.doc_id} className="border border-gray-200 rounded-xl overflow-hidden">
                      {/* Promise header */}
                      <div className="bg-gray-50 px-3 py-2 border-b border-gray-200">
                        <p className="text-[10px] font-bold text-gray-400 uppercase tracking-wide mb-0.5">
                          {isTA ? `வாக்குறுதி ${idx + 1}` : `Promise ${idx + 1}`}
                        </p>
                        <p className="text-[12px] font-semibold text-gray-800 leading-snug">
                          {isTA && p.promise_text_ta ? p.promise_text_ta : p.promise_text_en}
                        </p>
                        <div className="flex flex-wrap gap-1 mt-1.5">
                          {db && (
                            <span className={`text-[9px] px-1.5 py-0.5 rounded-full font-bold border ${db.bg} ${db.text}`}>
                              {isTA ? db.label_ta : db.label_en}
                            </span>
                          )}
                          {p.implementation_risk && (
                            <span className={`text-[9px] px-1.5 py-0.5 rounded-full font-bold border ${
                              p.implementation_risk === "low"    ? "bg-emerald-50 text-emerald-700 border-emerald-200" :
                              p.implementation_risk === "medium" ? "bg-yellow-50 text-yellow-700 border-yellow-200" :
                                                                   "bg-red-50 text-red-700 border-red-200"
                            }`}>
                              {isTA ? "செயல்படுத்தல் ஆபத்து" : "risk"}: {p.implementation_risk}
                            </span>
                          )}
                          {p.root_cause_addressed != null && (
                            <span className={`text-[9px] px-1.5 py-0.5 rounded-full font-bold border ${
                              p.root_cause_addressed
                                ? "bg-emerald-50 text-emerald-700 border-emerald-200"
                                : "bg-orange-50 text-orange-700 border-orange-200"
                            }`}>
                              {p.root_cause_addressed
                                ? (isTA ? "வேர் காரணம் தீர்க்கப்படுகிறது" : "Root cause addressed")
                                : (isTA ? "அறிகுறி நிவாரணம்" : "Symptom-level fix")}
                            </span>
                          )}
                        </div>
                      </div>

                      {/* Analysis body */}
                      <div className="px-3 py-2.5 space-y-2.5">

                        {/* Impact mechanism summary */}
                        {p.impact_mechanism && (
                          <div>
                            <p className="text-[10px] font-bold text-indigo-700 uppercase tracking-wide mb-0.5">
                              {isTA ? "தாக்க வழிமுறை" : "How it works"}
                            </p>
                            <p className="text-[11px] text-gray-700 leading-snug">{p.impact_mechanism}</p>
                          </div>
                        )}

                        {/* Promise breakdown — grounded component analysis */}
                        {p.promise_components && p.promise_components.length > 0 && (
                          <div>
                            <p className="text-[10px] font-bold text-gray-500 uppercase tracking-wide mb-1">
                              {isTA ? "வாக்குறுதி பகுப்பாய்வு" : "Promise breakdown"}
                            </p>
                            <div className="space-y-1.5">
                              {p.promise_components.map((comp, ci) => (
                                <div key={ci} className="bg-gray-50 border border-gray-200 rounded-lg px-2.5 py-2">
                                  <p className="text-[10px] font-bold text-gray-800 mb-0.5">
                                    {ci + 1}. {comp.component}
                                  </p>
                                  <p className="text-[10px] text-gray-600 leading-snug font-mono whitespace-pre-wrap">
                                    {comp.analysis}
                                  </p>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}

                        {/* Fiscal cost */}
                        {p.fiscal_cost_note && (
                          <div className="bg-blue-50 border border-blue-200 rounded-lg px-2.5 py-2">
                            <p className="text-[10px] font-bold text-blue-700 mb-0.5">
                              💰 {isTA ? "நிதி செலவு மதிப்பீடு" : "Fiscal cost estimate"}
                            </p>
                            <p className="text-[10px] text-blue-800 leading-snug font-mono whitespace-pre-wrap">
                              {p.fiscal_cost_note}
                            </p>
                          </div>
                        )}

                        {/* Sustainability verdict */}
                        {p.sustainability_verdict && (
                          <div className={`rounded-lg px-2.5 py-2 border ${
                            p.sustainability_verdict === "structural"  ? "bg-emerald-50 border-emerald-200" :
                            p.sustainability_verdict === "symptomatic" ? "bg-amber-50 border-amber-200" :
                                                                         "bg-rose-50 border-rose-200"
                          }`}>
                            <div className="flex items-center gap-1.5 mb-0.5">
                              <span className={`text-[9px] font-black px-1.5 py-0.5 rounded-full border ${
                                p.sustainability_verdict === "structural"  ? "bg-emerald-100 text-emerald-700 border-emerald-300" :
                                p.sustainability_verdict === "symptomatic" ? "bg-amber-100 text-amber-700 border-amber-300" :
                                                                             "bg-rose-100 text-rose-700 border-rose-300"
                              }`}>
                                {p.sustainability_verdict === "structural"  ? (isTA ? "⬆ கட்டமைப்பு மாற்றம்" : "⬆ Structural change") :
                                 p.sustainability_verdict === "symptomatic" ? (isTA ? "⚡ அறிகுறி நிவாரணம்" : "⚡ Symptomatic relief") :
                                                                              (isTA ? "👁 அரசியல் காட்சி"   : "👁 Political optics")}
                              </span>
                            </div>
                            {p.sustainability_reasoning && (
                              <p className={`text-[10px] leading-snug ${
                                p.sustainability_verdict === "structural"  ? "text-emerald-800" :
                                p.sustainability_verdict === "symptomatic" ? "text-amber-800" :
                                                                             "text-rose-800"
                              }`}>{p.sustainability_reasoning}</p>
                            )}
                          </div>
                        )}

                        {/* Coverage gap */}
                        {p.coverage_gap_note && (
                          <div className="bg-yellow-50 border border-yellow-200 rounded-lg px-2.5 py-2">
                            <p className="text-[10px] font-bold text-yellow-700 mb-0.5">
                              ⚠ {isTA ? "கவரேஜ் இடைவெளி" : "Coverage gap"}
                            </p>
                            <p className="text-[10px] text-yellow-700 leading-snug">{p.coverage_gap_note}</p>
                          </div>
                        )}

                        {/* Fallback for seed / pre-enrichment data */}
                        {!p.impact_mechanism && !p.promise_components && (
                          <p className="text-[10px] text-gray-400 italic">
                            {isTA
                              ? "ஆழமான பகுப்பாய்வு தரவு இன்னும் கிடைக்கவில்லை."
                              : "Deep analysis not yet available for this promise."}
                          </p>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        );
      })()}
    </div>
  );
}
