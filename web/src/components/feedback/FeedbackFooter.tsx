"use client";

import { useState } from "react";
import { FeedbackModal } from "./FeedbackModal";
import { useLanguage } from "@/lib/LanguageContext";

/** Page-wide footer rendered at the bottom of every page via layout.tsx.
 *  Stacks the civic disclaimer above a tight, centered row containing the
 *  "Buy me a coffee" link and the "Send feedback" trigger. Text swaps between
 *  English and Tamil based on the shared LanguageContext (toggled via the
 *  Translate button in each page's header). */
export function FeedbackFooter() {
  const [open, setOpen] = useState(false);
  const { lang } = useLanguage();
  const isTA = lang === "ta";

  return (
    <>
      <footer className="border-t border-gray-200 bg-white/70 backdrop-blur-sm py-4 sm:py-5 px-4 max-w-full overflow-hidden select-none">
        <div className="max-w-2xl w-full mx-auto flex flex-col items-center gap-3 text-xs">
          {/* Civic disclaimer — one language at a time, driven by the header toggle. */}
          {isTA ? (
            <p
              lang="ta"
              className="text-[11px] text-gray-500 text-center leading-relaxed max-w-lg break-words hyphens-auto"
            >
              இந்தியா வளர்ந்து வருகிறது — அதன் டிஜிட்டல் தடயத்திலும் சரி, அரசியல் வெளிப்படைத்தன்மையிலும் சரி — ஆயினும் நாம் இன்னும் முழு இலக்கை எட்டவில்லை.{" "}
              <span className="font-semibold text-gray-600">⚠ இங்குள்ள சில தரவுகள் தவறாக இருக்கலாம்.</span>
              {" "}அவற்றைச் சரிசெய்ய எங்களுக்கு உதவுங்கள்.{" "}
              <span className="font-semibold text-gray-600">ஒரு பார்வையாளராக மட்டும் இராமல், பொறுப்புள்ள குடிமகனாகச் செயல்படுங்கள்.</span>
            </p>
          ) : (
            <p className="text-[11px] text-gray-500 text-center leading-relaxed max-w-lg break-words hyphens-auto">
              India is evolving — both in digital footprint and political transparency — but we&apos;re not there yet.{" "}
              <span className="font-semibold text-gray-600">⚠ Some data here may be wrong.</span>
              {" "}Help us fix it.{" "}
              <span className="font-semibold text-gray-600">Be a citizen, not a spectator.</span>
            </p>
          )}

          {/* Coffee + Feedback, centered with a tight gap. */}
          <div className="flex items-center justify-center gap-6">
            <a
              href="https://buymeacoffee.com/jaggannadhan"
              target="_blank"
              rel="noopener noreferrer"
              className="font-semibold text-gray-700 hover:text-gray-950 inline-flex items-center gap-1.5 transition-colors"
            >
              <span>☕</span>
              <span lang={isTA ? "ta" : undefined}>
                {isTA ? "நிதி உதவி" : "Buy me a coffee"}
              </span>
            </a>
            <span className="text-gray-300">·</span>
            <button
              type="button"
              onClick={() => setOpen(true)}
              className="font-semibold text-gray-700 hover:text-gray-950 inline-flex items-center gap-1.5 cursor-pointer transition-colors"
            >
              <span>💬</span>
              <span lang={isTA ? "ta" : undefined}>
                {isTA ? "கருத்து தெரிவிக்கவும்" : "Send feedback"}
              </span>
            </button>
          </div>
        </div>
      </footer>

      <FeedbackModal open={open} onClose={() => setOpen(false)} lang={lang} />
    </>
  );
}
