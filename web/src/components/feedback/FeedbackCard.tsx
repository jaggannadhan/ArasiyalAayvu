"use client";

import { useState } from "react";
import { FeedbackModal } from "./FeedbackModal";

interface FeedbackCardProps {
  lang?: "en" | "ta";
}

/** Full-width feedback card for the home-page grid. */
export function FeedbackCard({ lang = "en" }: FeedbackCardProps) {
  const isTA = lang === "ta";
  const [open, setOpen] = useState(false);

  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(true)}
        className="bg-white rounded-2xl border border-gray-200 p-4 hover:shadow-sm hover:border-gray-400 transition-all col-span-2 text-left cursor-pointer"
      >
        <div className="flex items-start justify-between gap-2">
          <div>
            <p className="text-lg mb-1">💬</p>
            <p className="text-sm font-bold text-gray-900">
              {isTA ? "கருத்து / திருத்தம் அனுப்பு" : "Send Feedback"}
            </p>
            <p className="text-xs text-gray-500">
              {isTA
                ? "திருத்தம் · விடுபட்ட தரவு · பரிந்துரை · பிழை அறிக்கை"
                : "Correction · Missing data · Suggestion · Bug report"}
            </p>
          </div>
          <span className="flex-shrink-0 text-gray-300 text-xl">→</span>
        </div>
      </button>

      <FeedbackModal open={open} onClose={() => setOpen(false)} lang={lang} />
    </>
  );
}
