"use client";

import { useState } from "react";
import { StanceLabel, StatusBadge } from "./StanceLabel";
import { VerificationPanel } from "./VerificationPanel";
import type { ManifestoPromise } from "@/lib/types";
import { PARTIES } from "@/lib/types";

interface PromiseCardProps {
  promise: ManifestoPromise;
  lang?: "en" | "ta";
  highlighted?: boolean;
}

export function PromiseCard({ promise, lang = "en", highlighted = false }: PromiseCardProps) {
  const [showVerification, setShowVerification] = useState(false);
  const party = PARTIES[promise.party_id];
  const text = lang === "ta" ? promise.promise_text_ta : promise.promise_text_en;

  return (
    <div className={`rounded-xl border bg-white shadow-sm hover:shadow-md transition-shadow p-4 flex flex-col gap-3 ${
      highlighted ? "border-indigo-300 ring-1 ring-indigo-100" : "border-gray-200"
    }`}>
      {/* Party tag + status */}
      <div className="flex items-center justify-between gap-2 flex-wrap">
        <span className={`inline-flex items-center gap-1.5 text-xs font-bold px-2.5 py-1 rounded-full text-white ${party?.color ?? "bg-gray-500"}`}>
          {lang === "ta" ? party?.tamil_name : promise.party_name}
        </span>
        <div className="flex items-center gap-1.5">
          {highlighted && (
            <span className="text-[9px] font-bold text-indigo-600 bg-indigo-50 border border-indigo-200 px-1.5 py-0.5 rounded-full">
              ★ {lang === "ta" ? "சிறந்த பொருத்தம்" : "Top match"}
            </span>
          )}
          <StatusBadge status={promise.status} lang={lang} />
        </div>
      </div>

      {/* Promise text */}
      <p className={`text-sm text-gray-800 leading-relaxed ${lang === "ta" ? "font-tamil" : ""}`}>
        {text}
      </p>

      {/* Stance vibe chip */}
      <div className="flex items-center gap-2 flex-wrap">
        <StanceLabel vibe={promise.stance_vibe} lang={lang} />
        {promise.amount_mentioned && (
          <span className="text-xs font-semibold text-gray-600 bg-gray-100 px-2 py-0.5 rounded-full">
            {promise.amount_mentioned}
          </span>
        )}
      </div>

      {/* Verification toggle */}
      <button
        onClick={() => setShowVerification(!showVerification)}
        className="text-xs text-blue-600 hover:text-blue-800 underline underline-offset-2 self-start mt-1 transition-colors"
      >
        {showVerification
          ? (lang === "ta" ? "சரிபார்ப்பை மறை" : "Hide verification")
          : (lang === "ta" ? "ஆதாரம் காட்டு ↓" : "Show source & verification ↓")}
      </button>

      {showVerification && <VerificationPanel promise={promise} lang={lang} />}
    </div>
  );
}
