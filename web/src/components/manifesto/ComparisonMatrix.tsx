"use client";

import { useMemo } from "react";
import { PromiseCard } from "./PromiseCard";
import type { ManifestoPromise, Pillar } from "@/lib/types";
import { PARTIES, PILLAR_META } from "@/lib/types";

interface ComparisonMatrixProps {
  promises: ManifestoPromise[];
  partyA: string;
  partyB: string;
  pillar: Pillar;
  lang?: "en" | "ta";
}

const LABELS = {
  en: {
    noPromises: "No promises found for this pillar.",
    pillarContext: "What this covers",
    vsSeparator: "vs",
  },
  ta: {
    noPromises: "இந்த தூணில் வாக்குறுதிகள் எதுவும் இல்லை.",
    pillarContext: "இது என்னை உள்ளடக்கியது",
    vsSeparator: "எதிராக",
  },
};

export function ComparisonMatrix({ promises, partyA, partyB, pillar, lang = "en" }: ComparisonMatrixProps) {
  const L = LABELS[lang];
  const pillarMeta = PILLAR_META[pillar];
  const partyAMeta = PARTIES[partyA];
  const partyBMeta = PARTIES[partyB];

  const { promisesA, promisesB } = useMemo(() => ({
    promisesA: promises.filter((p) => p.party_id === partyA && p.category === pillar),
    promisesB: promises.filter((p) => p.party_id === partyB && p.category === pillar),
  }), [promises, partyA, partyB, pillar]);

  const maxRows = Math.max(promisesA.length, promisesB.length, 1);

  return (
    <div className="space-y-4">
      {/* Pillar context banner */}
      <div className="flex items-center gap-3 rounded-xl bg-gray-50 border border-gray-200 px-4 py-3">
        <span className="text-2xl">{pillarMeta.icon}</span>
        <div>
          <p className="text-xs text-gray-500">{L.pillarContext}</p>
          <p className="text-sm text-gray-700">{pillarMeta.description}</p>
        </div>
      </div>

      {/* Column headers */}
      <div className="grid grid-cols-2 gap-3">
        <div className={`rounded-xl border-2 ${partyAMeta?.border_color ?? "border-gray-300"} px-4 py-2.5 text-center`}>
          <p className={`text-lg font-black ${partyAMeta?.text_color ?? "text-gray-800"}`}>
            {lang === "ta" ? partyAMeta?.tamil_name : partyAMeta?.party_name}
          </p>
          <p className="text-xs text-gray-500">{promisesA.length} {lang === "ta" ? "வாக்குறுதிகள்" : "promises"}</p>
        </div>
        <div className={`rounded-xl border-2 ${partyBMeta?.border_color ?? "border-gray-300"} px-4 py-2.5 text-center`}>
          <p className={`text-lg font-black ${partyBMeta?.text_color ?? "text-gray-800"}`}>
            {lang === "ta" ? partyBMeta?.tamil_name : partyBMeta?.party_name}
          </p>
          <p className="text-xs text-gray-500">{promisesB.length} {lang === "ta" ? "வாக்குறுதிகள்" : "promises"}</p>
        </div>
      </div>

      {/* Side-by-side rows */}
      {maxRows > 0 && (promisesA.length > 0 || promisesB.length > 0) ? (
        <div className="grid grid-cols-2 gap-3">
          {/* Column A */}
          <div className="space-y-3">
            {promisesA.length > 0 ? (
              promisesA.map((p) => <PromiseCard key={p.doc_id} promise={p} lang={lang} />)
            ) : (
              <EmptyColumn partyName={lang === "ta" ? partyAMeta?.tamil_name : partyAMeta?.party_name} lang={lang} />
            )}
          </div>
          {/* Column B */}
          <div className="space-y-3">
            {promisesB.length > 0 ? (
              promisesB.map((p) => <PromiseCard key={p.doc_id} promise={p} lang={lang} />)
            ) : (
              <EmptyColumn partyName={lang === "ta" ? partyBMeta?.tamil_name : partyBMeta?.party_name} lang={lang} />
            )}
          </div>
        </div>
      ) : (
        <div className="rounded-xl border border-dashed border-gray-300 py-12 text-center text-gray-400 text-sm">
          {L.noPromises}
        </div>
      )}
    </div>
  );
}

function EmptyColumn({ partyName, lang }: { partyName?: string; lang: "en" | "ta" }) {
  return (
    <div className="rounded-xl border border-dashed border-gray-200 py-10 text-center text-sm text-gray-400">
      {lang === "ta"
        ? `${partyName} இந்த தூணில் வாக்குறுதிகள் எதுவும் இல்லை`
        : `No promises from ${partyName} in this pillar`}
    </div>
  );
}
