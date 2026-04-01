"use client";

import type { ManifestoPromise } from "@/lib/types";

interface VerificationPanelProps {
  promise: ManifestoPromise;
  lang?: "en" | "ta";
}

const LABELS = {
  en: {
    source: "Source",
    manifesto: "Official Manifesto PDF",
    page: "Page",
    confidence: "Ground Truth",
    notes: "Verification Notes",
    open: "Open Manifesto PDF ↗",
    uploaded: "Last verified",
  },
  ta: {
    source: "ஆதாரம்",
    manifesto: "அதிகாரப்பூர்வ தேர்தல் அறிக்கை PDF",
    page: "பக்கம்",
    confidence: "நம்பகத்தன்மை",
    notes: "சரிபார்ப்பு குறிப்புகள்",
    open: "தேர்தல் அறிக்கை PDF திறக்கவும் ↗",
    uploaded: "கடைசியாக சரிபார்க்கப்பட்டது",
  },
};

const CONFIDENCE_META = {
  HIGH:   { label_en: "HIGH",   label_ta: "உயர்",   color: "text-emerald-700 bg-emerald-50" },
  MEDIUM: { label_en: "MEDIUM", label_ta: "நடுத்தர", color: "text-amber-700 bg-amber-50" },
  LOW:    { label_en: "LOW",    label_ta: "குறைந்த", color: "text-red-700 bg-red-50" },
};

export function VerificationPanel({ promise, lang = "en" }: VerificationPanelProps) {
  const L = LABELS[lang];
  const conf = CONFIDENCE_META[promise.ground_truth_confidence];

  return (
    <div className="mt-3 rounded-lg border border-gray-200 bg-gray-50 p-3 text-xs space-y-2">
      {/* Confidence badge */}
      <div className="flex items-center gap-2">
        <span className="text-gray-500">{L.confidence}:</span>
        <span className={`font-semibold rounded px-1.5 py-0.5 ${conf.color}`}>
          {lang === "ta" ? conf.label_ta : conf.label_en}
        </span>
      </div>

      {/* Manifesto PDF link */}
      <div className="flex items-start gap-2">
        <span className="text-gray-500 shrink-0">{L.manifesto}:</span>
        <a
          href={promise.manifesto_pdf_url}
          target="_blank"
          rel="noopener noreferrer"
          className="text-blue-600 hover:underline break-all font-medium"
        >
          {L.open}
          {promise.manifesto_pdf_page && (
            <span className="ml-1 text-gray-400 font-normal">
              ({L.page} {promise.manifesto_pdf_page})
            </span>
          )}
        </a>
      </div>

      {/* Scheme name */}
      {promise.scheme_name && (
        <div className="flex items-center gap-2">
          <span className="text-gray-500">Scheme:</span>
          <span className="font-medium text-gray-800">{promise.scheme_name}</span>
        </div>
      )}

      {/* Amount */}
      {promise.amount_mentioned && (
        <div className="flex items-center gap-2">
          <span className="text-gray-500">Amount:</span>
          <span className="font-semibold text-gray-800">{promise.amount_mentioned}</span>
        </div>
      )}

      {/* Source notes */}
      {promise.source_notes && (
        <div className="border-t border-gray-200 pt-2">
          <span className="text-gray-500">{L.notes}: </span>
          <span className="text-gray-700">{promise.source_notes}</span>
        </div>
      )}

      {/* Last verified timestamp (from Firestore _uploaded_at) */}
      {promise._uploaded_at && (
        <div className="flex items-center gap-2 border-t border-gray-200 pt-2">
          <span className="text-gray-400">{L.uploaded}:</span>
          <span className="text-gray-500 font-mono">
            {new Date(promise._uploaded_at).toLocaleDateString("en-IN", {
              day: "numeric",
              month: "short",
              year: "numeric",
            })}
          </span>
        </div>
      )}
    </div>
  );
}
