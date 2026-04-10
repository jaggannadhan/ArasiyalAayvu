"use client";

import { useMemo, useState } from "react";
import type { ManifestoPromise } from "@/lib/types";
import {
  computeSDGCoverage,
  coveredSDGs,
  uncoveredSDGs,
  blockedBy,
  type SDGCoverage,
  type SDGCoverageMap,
} from "@/lib/sdg-mapping";
import { SDG_GOALS } from "@/lib/sdg-data";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function goalById(id: number) {
  return SDG_GOALS.find((g) => g.id === id);
}

function coverageTier(count: number): "strong" | "partial" | "none" {
  if (count === 0) return "none";
  if (count <= 2) return "partial";
  return "strong";
}

// ---------------------------------------------------------------------------
// 17-dot coverage bar
// ---------------------------------------------------------------------------
function CoverageBar({ map, lang }: { map: SDGCoverageMap; lang: "en" | "ta" }) {
  const covered = coveredSDGs(map).length;
  const total = 17;
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between text-xs">
        <span className="font-semibold text-gray-600">
          {lang === "ta" ? `${total} இல் ${covered} SDG உரையாற்றப்பட்டது` : `${covered} of ${total} SDGs addressed`}
        </span>
        <span className="text-gray-400">{total - covered} {lang === "ta" ? "இடைவெளிகள்" : "gaps"}</span>
      </div>
      <div className="flex gap-1 flex-wrap">
        {Array.from({ length: total }, (_, i) => i + 1).map((id) => {
          const cov = map.get(id);
          const goal = goalById(id);
          const tier = coverageTier(cov?.promise_count ?? 0);
          const hasBreak = (cov?.chain_breaks.length ?? 0) > 0;
          return (
            <div
              key={id}
              title={goal ? (lang === "ta" ? goal.name_ta : goal.name) : `SDG ${id}`}
              className={`relative h-4 w-4 rounded-sm flex items-center justify-center text-[8px] font-black transition-all
                ${tier === "strong"  ? "bg-emerald-500 text-white" : ""}
                ${tier === "partial" ? "bg-amber-400 text-white" : ""}
                ${tier === "none"    ? "bg-gray-200 text-gray-400" : ""}
              `}
            >
              {id}
              {hasBreak && (
                <span className="absolute -top-0.5 -right-0.5 w-1.5 h-1.5 bg-orange-400 rounded-full" />
              )}
            </div>
          );
        })}
      </div>
      <div className="flex gap-3 text-[10px] text-gray-500">
        <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-sm bg-emerald-500 inline-block" />{lang === "ta" ? "உரையாற்றப்பட்டது" : "Addressed"}</span>
        <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-sm bg-amber-400 inline-block" />{lang === "ta" ? "சிறிதளவு" : "Lightly"}</span>
        <span className="flex items-center gap-1"><span className="w-2.5 h-2.5 rounded-sm bg-gray-200 inline-block" />{lang === "ta" ? "இல்லை" : "Not addressed"}</span>
        <span className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-orange-400 inline-block" />{lang === "ta" ? "சங்கிலி உடைப்பு" : "Chain break"}</span>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Single covered SDG card
// ---------------------------------------------------------------------------
function CoveredSDGCard({ cov, lang }: { cov: SDGCoverage; lang: "en" | "ta" }) {
  const [open, setOpen] = useState(false);
  const goal = goalById(cov.sdg_id);
  if (!goal) return null;

  const tier = coverageTier(cov.promise_count);
  const isTA = lang === "ta";

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
        <div className="flex-shrink-0 flex items-center gap-2">
          {/* Promise count badge */}
          <span className={`text-[10px] font-black px-2 py-0.5 rounded-full bg-white/30 ${goal.color_text}`}>
            {cov.promise_count} {isTA ? "வாக்குறுதி" : cov.promise_count === 1 ? "promise" : "promises"}
          </span>
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

        {/* Top promises toggle */}
        <button
          onClick={() => setOpen(!open)}
          className="text-[11px] font-semibold text-gray-500 hover:text-gray-800 transition-colors w-full text-left"
        >
          {open
            ? (isTA ? "மூடு ↑" : "Hide top promises ↑")
            : (isTA ? `முக்கிய வாக்குறுதிகள் ↓` : `Top promises ↓`)}
        </button>

        {open && (
          <div className="space-y-2 pt-1 border-t border-gray-100">
            {cov.top_promises.length === 0 ? (
              <p className="text-[10px] text-gray-400 italic">{isTA ? "வாக்குறுதிகள் இல்லை" : "No promises available"}</p>
            ) : (
              cov.top_promises.map((p) => (
                <div key={p.doc_id} className="space-y-0.5">
                  <p className="text-[11px] text-gray-700 leading-snug">
                    {isTA && p.promise_text_ta ? p.promise_text_ta : p.promise_text_en}
                  </p>
                  <div className="flex flex-wrap gap-1">
                    {p.amount_mentioned && (
                      <span className="text-[9px] bg-emerald-50 text-emerald-700 border border-emerald-200 px-1.5 py-0.5 rounded-full font-bold">
                        {p.amount_mentioned}
                      </span>
                    )}
                    {p.scheme_name && (
                      <span className="text-[9px] bg-blue-50 text-blue-700 border border-blue-200 px-1.5 py-0.5 rounded-full font-semibold">
                        {p.scheme_name}
                      </span>
                    )}
                  </div>
                </div>
              ))
            )}
            {cov.promise_count > 3 && (
              <p className="text-[10px] text-gray-400 italic">
                {isTA
                  ? `+ ${cov.promise_count - 3} கூடுதல் வாக்குறுதிகள் (வாக்குறுதி காட்சியில் காண்க)`
                  : `+ ${cov.promise_count - 3} more (see Promises view)`}
              </p>
            )}
          </div>
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
}

export function SDGAlignment({ promises, partyName, partyNameTa, lang }: SDGAlignmentProps) {
  const isTA = lang === "ta";

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
              return <CoveredSDGCard key={id} cov={cov} lang={lang} />;
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
          ? "SDG வரைபடல் அறிக்கை தூண்களின் அடிப்படையில் கணக்கிடப்படுகிறது — தனிப்பட்ட வாக்குறுதி நிலை அல்ல. ஆதாரம்: NITI Aayog SDG India Index 2023-24."
          : "SDG mapping is computed from manifesto pillars — not individual promise text. Source: NITI Aayog SDG India Index 2023-24."}
      </p>
    </div>
  );
}
