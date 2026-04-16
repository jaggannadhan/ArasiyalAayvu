"use client";

import { useEffect, useState } from "react";
import { useCookieConsent } from "@/lib/CookieConsentContext";
import { useLanguage } from "@/lib/LanguageContext";

/**
 * Bottom-anchored cookie consent banner. Shown on first visit and re-shown
 * when the user clicks "Cookie settings" in the footer. Bilingual copy.
 */
export function CookieBanner() {
  const { bannerOpen, setPerformance, closeBanner } = useCookieConsent();
  const { lang } = useLanguage();
  const isTA = lang === "ta";

  // Toggle always defaults ON — mirrors the app's recommended choice. The
  // user explicitly opts out by flipping it before Confirm. Resets to ON
  // every time the banner reopens, even after a previous rejection, so the
  // "try again" nudge stays consistent.
  const [allow, setAllow] = useState<boolean>(true);

  useEffect(() => {
    if (bannerOpen) setAllow(true);
  }, [bannerOpen]);

  if (!bannerOpen) return null;

  const copy = isTA
    ? {
        title: "உலாவி சேமிப்பு அனுமதி",
        body:
          "செயல்திறனை மேம்படுத்தவும், உங்கள் மொழித் தேர்வை நினைவில் வைத்துக் கொள்ளவும் தளம் உங்கள் உலாவியில் சில குறிப்புகளைச் சேமிக்கிறது. விளம்பரக் குக்கீகள் இல்லை.",
        toggleLabel: "செயல்திறன் குக்கீகள்",
        toggleHint: "அநாமதேயப் பயன்பாட்டுப் புள்ளி விவரங்கள் + மொழி/பின்கோடு தேர்வுகள்",
        confirm: "எனது தேர்வை உறுதிசெய்",
      }
    : {
        title: "Browser storage consent",
        body:
          "This site keeps small notes in your browser so the app is fast and remembers your language. No advertising cookies, no cross-site tracking.",
        toggleLabel: "Performance cookies",
        toggleHint: "Anonymous usage analytics + language & pincode preferences",
        confirm: "Confirm my choices",
      };

  return (
    <div
      role="dialog"
      aria-modal="false"
      aria-label={copy.title}
      className="fixed inset-x-0 bottom-0 z-50 p-3 sm:p-4 flex justify-center pointer-events-none"
    >
      <div className="pointer-events-auto w-full max-w-2xl bg-white rounded-2xl shadow-2xl border border-gray-200 p-4 sm:p-5 flex flex-col gap-3 text-sm">
        <div className="flex items-start justify-between gap-3">
          <p className="font-black text-gray-900 text-base">🍪 {copy.title}</p>
          <button
            type="button"
            onClick={closeBanner}
            aria-label="Close"
            className="text-gray-400 hover:text-gray-700 text-xl leading-none -mt-1 -mr-1 transition-colors"
          >
            ×
          </button>
        </div>

        <p className="text-xs text-gray-600 leading-relaxed">{copy.body}</p>

        {/* Toggle row */}
        <div className="flex items-center justify-between gap-3 bg-gray-50 border border-gray-200 rounded-xl px-3 py-2.5">
          <div className="min-w-0">
            <p className="text-xs font-bold text-gray-800">{copy.toggleLabel}</p>
            <p className="text-[10px] text-gray-500 leading-snug">{copy.toggleHint}</p>
          </div>
          <Switch
            checked={allow}
            onChange={setAllow}
            ariaLabel={copy.toggleLabel}
          />
        </div>

        {/* Single confirm button — saves whatever the toggle is currently set to. */}
        <div className="flex justify-end pt-1">
          <button
            type="button"
            onClick={() => setPerformance(allow)}
            className="px-4 py-2 rounded-lg bg-gray-900 text-white text-xs font-bold hover:bg-gray-700 transition-colors"
          >
            {copy.confirm}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Tiny toggle switch (no extra deps) ───────────────────────────────────────

interface SwitchProps {
  checked: boolean;
  onChange: (next: boolean) => void;
  ariaLabel: string;
}

function Switch({ checked, onChange, ariaLabel }: SwitchProps) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={checked}
      aria-label={ariaLabel}
      onClick={() => onChange(!checked)}
      className={`relative inline-flex flex-shrink-0 h-5 w-9 items-center rounded-full border transition-colors cursor-pointer ${
        checked
          ? "bg-emerald-500 border-emerald-600"
          : "bg-gray-300 border-gray-400"
      }`}
    >
      <span
        className={`inline-block h-4 w-4 transform rounded-full bg-white shadow transition-transform ${
          checked ? "translate-x-[1.125rem]" : "translate-x-0.5"
        }`}
      />
    </button>
  );
}
