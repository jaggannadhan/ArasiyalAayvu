"use client";

import { useMemo, useState } from "react";
import type { ManifestoPromise, Pillar } from "@/lib/types";
import { PILLARS, PILLAR_META, STATUS_META } from "@/lib/types";

interface PromiseMatrixProps {
  promises: ManifestoPromise[];
  partyName: string;
  lang?: "en" | "ta";
}

export function PromiseMatrix({ promises, partyName, lang = "en" }: PromiseMatrixProps) {
  const isTA = lang === "ta";
  const [activePillar, setActivePillar] = useState<Pillar | "all">("all");

  const pillarOrder = useMemo(
    () =>
      Object.fromEntries(
        PILLARS.map((pillar, idx) => [pillar, idx])
      ) as Record<Pillar, number>,
    []
  );

  const sortedPromises = useMemo(() => {
    return [...promises].sort((a, b) => {
      const ap = pillarOrder[a.category];
      const bp = pillarOrder[b.category];
      if (ap !== bp) return ap - bp;
      return a.doc_id.localeCompare(b.doc_id);
    });
  }, [promises, pillarOrder]);

  const filtered = activePillar === "all"
    ? sortedPromises
    : sortedPromises.filter((p) => p.category === activePillar);

  // Count per pillar for badges
  const counts = Object.fromEntries(
    PILLARS.map((p) => [p, promises.filter((pr) => pr.category === p).length])
  ) as Record<Pillar, number>;

  if (promises.length === 0) {
    return (
      <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-5">
        <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3">
          {isTA ? "தேர்தல் வாக்குறுதிகள்" : "Manifesto Promises"}
        </p>
        <p className="text-sm text-gray-400 text-center py-8">
          {isTA
            ? `${partyName} க்கான வாக்குறுதிகள் தரவுத்தளத்தில் இல்லை`
            : `No promises found for ${partyName} in the database`}
        </p>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-5 space-y-4">
      <div className="flex items-center justify-between gap-2 flex-wrap">
        <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
          {isTA ? `${partyName} வாக்குறுதிகள்` : `${partyName} Promises`}
        </p>
        <span className="text-xs text-gray-400">{promises.length} {isTA ? "மொத்தம்" : "total"}</span>
      </div>

      {/* Pillar filter pills */}
      <div className="flex gap-2 flex-wrap">
        <button
          onClick={() => setActivePillar("all")}
          className={`text-xs px-3 py-1 rounded-full border font-medium transition-colors ${
            activePillar === "all"
              ? "bg-gray-900 text-white border-gray-900"
              : "bg-white text-gray-600 border-gray-300 hover:bg-gray-50"
          }`}
        >
          {isTA ? "அனைத்தும்" : "All"} ({promises.length})
        </button>
        {PILLARS.map((pillar) =>
          counts[pillar] > 0 ? (
            <button
              key={pillar}
              onClick={() => setActivePillar(pillar)}
              className={`text-xs px-3 py-1 rounded-full border font-medium transition-colors ${
                activePillar === pillar
                  ? "bg-gray-900 text-white border-gray-900"
                  : "bg-white text-gray-600 border-gray-300 hover:bg-gray-50"
              }`}
            >
              {PILLAR_META[pillar].icon}{" "}
              {isTA ? PILLAR_META[pillar].tamil : pillar} ({counts[pillar]})
            </button>
          ) : null
        )}
      </div>

      {/* Promise list */}
      <div className="space-y-2.5">
        {filtered.map((promise) => {
          const status = STATUS_META[promise.status];
          return (
            <div
              key={promise.doc_id}
              className="flex items-start gap-3 rounded-xl border border-gray-100 bg-gray-50 p-3"
            >
              <span className="text-base shrink-0 mt-0.5">
                {PILLAR_META[promise.category].icon}
              </span>
              <div className="flex-1 min-w-0 space-y-1">
                <p className="text-sm text-gray-800 leading-snug">
                  {isTA ? promise.promise_text_ta : promise.promise_text_en}
                </p>
                <div className="flex flex-wrap items-center gap-1.5">
                  <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${status.bg} ${status.text}`}>
                    {isTA ? status.label_ta : status.label_en}
                  </span>
                  {promise.amount_mentioned && (
                    <span className="text-xs font-semibold text-gray-600 bg-white border border-gray-200 px-2 py-0.5 rounded-full">
                      {promise.amount_mentioned}
                    </span>
                  )}
                  {promise.scheme_name && (
                    <span className="text-xs text-gray-500 italic">{promise.scheme_name}</span>
                  )}
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
