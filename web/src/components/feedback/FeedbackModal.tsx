"use client";

import { useEffect, useState } from "react";
import { useFocusTrap } from "@/hooks/useFocusTrap";

const API_BASE_URL = (process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000").replace(/\/$/, "");

export type FeedbackCategory =
  | "correction"
  | "missing_data"
  | "suggestion"
  | "bug_report"
  | "other";

const CATEGORY_OPTIONS: { value: FeedbackCategory; label_en: string; label_ta: string }[] = [
  { value: "correction",   label_en: "Correction (fact / data is wrong)",    label_ta: "திருத்தம் (தரவு தவறாக உள்ளது)" },
  { value: "missing_data", label_en: "Missing data (something should be added)", label_ta: "விடுபட்ட தரவு (சேர்க்கப்பட வேண்டியது)" },
  { value: "suggestion",   label_en: "Suggestion (feature / improvement)",   label_ta: "பரிந்துரை (வசதி / மேம்பாடு)" },
  { value: "bug_report",   label_en: "Bug report (something broke)",         label_ta: "பிழை அறிக்கை (கோளாறு)" },
  { value: "other",        label_en: "Other",                                 label_ta: "மற்றவை" },
];

interface FeedbackModalProps {
  open: boolean;
  onClose: () => void;
  lang?: "en" | "ta";
  /** Seed the dropdown, e.g. "correction" when a user clicks a contextual "Report this" link. */
  defaultCategory?: FeedbackCategory;
}

export function FeedbackModal({ open, onClose, lang = "en", defaultCategory }: FeedbackModalProps) {
  const isTA = lang === "ta";
  const [category, setCategory] = useState<FeedbackCategory>(defaultCategory ?? "correction");
  const [message, setMessage]   = useState("");
  const [status, setStatus]     = useState<"idle" | "submitting" | "success" | "error">("idle");
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  // Reset when opened/closed so a stale success state isn't shown on re-open.
  useEffect(() => {
    if (open) {
      setCategory(defaultCategory ?? "correction");
      setMessage("");
      setStatus("idle");
      setErrorMsg(null);
    }
  }, [open, defaultCategory]);

  // Esc to close
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  const focusTrapRef = useFocusTrap(open);

  if (!open) return null;

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = message.trim();
    if (!trimmed) {
      setErrorMsg(isTA ? "கருத்தை உள்ளிடவும்." : "Please enter your message.");
      return;
    }
    setStatus("submitting");
    setErrorMsg(null);
    try {
      const res = await fetch(`${API_BASE_URL}/api/feedback`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          category,
          message: trimmed,
          page_url: typeof window !== "undefined" ? window.location.href : null,
        }),
      });
      if (!res.ok) {
        const body = (await res.json().catch(() => ({}))) as { detail?: string };
        throw new Error(body.detail || `HTTP ${res.status}`);
      }
      setStatus("success");
    } catch (err: unknown) {
      setStatus("error");
      setErrorMsg((err as Error).message || "Failed to submit.");
    }
  }

  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-label={isTA ? "கருத்து சமர்ப்பிக்கவும்" : "Send feedback"}
      className="fixed inset-0 z-50 flex items-end sm:items-center justify-center bg-black/50 backdrop-blur-sm p-0 sm:p-4"
      onClick={onClose}
    >
      <div
        ref={focusTrapRef}
        onClick={(e) => e.stopPropagation()}
        className="w-full sm:max-w-md bg-white rounded-t-2xl sm:rounded-2xl shadow-2xl border border-gray-200 p-5 flex flex-col gap-4"
      >
        <div className="flex items-start justify-between gap-3">
          <div>
            <h2 className="text-base font-black text-gray-900">
              {isTA ? "கருத்து சமர்ப்பிக்கவும்" : "Send feedback"}
            </h2>
            <p className="text-[11px] text-gray-500 leading-snug mt-0.5">
              {isTA
                ? "உங்கள் கருத்து தரவுத் தரத்தை மேம்படுத்த உதவுகிறது."
                : "Your feedback helps us keep the data accurate and the app useful."}
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close"
            className="text-gray-400 hover:text-gray-700 text-xl leading-none p-1 -m-1 transition-colors"
          >
            ×
          </button>
        </div>

        {status === "success" ? (
          <div className="py-6 text-center space-y-2">
            <p className="text-3xl">✅</p>
            <p className="text-sm font-bold text-gray-900">
              {isTA ? "நன்றி!" : "Thank you!"}
            </p>
            <p className="text-xs text-gray-500">
              {isTA
                ? "உங்கள் கருத்து சமர்ப்பிக்கப்பட்டது. விரைவில் மதிப்பாய்வு செய்யப்படும்."
                : "Your feedback has been recorded. We'll review it soon."}
            </p>
            <button
              type="button"
              onClick={onClose}
              className="mt-3 inline-flex items-center px-4 py-2 rounded-lg bg-gray-900 text-white text-xs font-semibold hover:bg-gray-700 transition-colors"
            >
              {isTA ? "மூடு" : "Close"}
            </button>
          </div>
        ) : (
          <form onSubmit={handleSubmit} className="flex flex-col gap-3">
            <label className="flex flex-col gap-1">
              <span className="text-[11px] font-bold text-gray-600 uppercase tracking-wide">
                {isTA ? "வகை" : "Category"}
              </span>
              <select
                value={category}
                onChange={(e) => setCategory(e.target.value as FeedbackCategory)}
                disabled={status === "submitting"}
                className="border border-gray-300 rounded-lg px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-gray-900 cursor-pointer"
              >
                {CATEGORY_OPTIONS.map((o) => (
                  <option key={o.value} value={o.value}>
                    {isTA ? o.label_ta : o.label_en}
                  </option>
                ))}
              </select>
            </label>

            <label className="flex flex-col gap-1">
              <span className="text-[11px] font-bold text-gray-600 uppercase tracking-wide">
                {isTA ? "விவரம்" : "Details"}
              </span>
              <textarea
                value={message}
                onChange={(e) => setMessage(e.target.value)}
                disabled={status === "submitting"}
                rows={5}
                maxLength={5000}
                placeholder={
                  isTA
                    ? "எது தவறு / எது சேர்க்கப்பட வேண்டும் / எந்தப் பிழை ஏற்பட்டது என்று எழுதவும்…"
                    : "Tell us what's wrong, what's missing, or what you'd like to see…"
                }
                className="border border-gray-300 rounded-lg px-3 py-2 text-sm resize-none focus:outline-none focus:ring-2 focus:ring-gray-900 placeholder:text-gray-400"
              />
              <div className="flex justify-between items-center">
                <p className="text-[10px] text-gray-400">
                  {isTA
                    ? `${message.length} / 5000 எழுத்துகள்`
                    : `${message.length} / 5000 characters`}
                </p>
                {typeof window !== "undefined" && (
                  <p className="text-[10px] text-gray-400 truncate max-w-[60%]">
                    📍 {window.location.pathname}
                  </p>
                )}
              </div>
            </label>

            {errorMsg && (
              <p className="text-xs text-rose-600 bg-rose-50 border border-rose-200 rounded-lg px-3 py-2">
                {errorMsg}
              </p>
            )}

            <div className="flex gap-2 justify-end">
              <button
                type="button"
                onClick={onClose}
                disabled={status === "submitting"}
                className="px-4 py-2 text-sm font-semibold text-gray-600 hover:text-gray-900 transition-colors"
              >
                {isTA ? "ரத்து" : "Cancel"}
              </button>
              <button
                type="submit"
                disabled={status === "submitting" || !message.trim()}
                className="px-4 py-2 rounded-lg bg-gray-900 text-white text-sm font-semibold hover:bg-gray-700 disabled:bg-gray-300 disabled:cursor-not-allowed transition-colors inline-flex items-center gap-2"
              >
                {status === "submitting" && (
                  <span className="inline-block w-3.5 h-3.5 border-2 border-white border-t-transparent rounded-full animate-spin" />
                )}
                {status === "submitting"
                  ? (isTA ? "சமர்ப்பிக்கப்படுகிறது…" : "Submitting…")
                  : (isTA ? "சமர்ப்பி" : "Submit")}
              </button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
}
