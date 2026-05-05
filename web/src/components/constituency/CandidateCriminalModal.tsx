"use client";

import { useEffect, useState, useRef } from "react";
import { useFocusTrap } from "@/hooks/useFocusTrap";
import type { MlaRecord, CriminalCase, CriminalCaseStatus } from "@/lib/types";
import { groupCasesByStatus } from "@/lib/formatters";

interface CandidateCriminalModalProps {
  mla: MlaRecord;
  isOpen: boolean;
  onClose: () => void;
  lang?: "en" | "ta";
}

// ---------------------------------------------------------------------------
// Translation dictionary
// ---------------------------------------------------------------------------
const T = {
  en: {
    title: "Criminal Record",
    subtitle: (n: number) =>
      n === 0 ? "No cases declared" : `${n} case${n > 1 ? "s" : ""} declared in ECI affidavit`,
    filterAll: "All",
    filterPending: "Pending",
    filterDismissed: "Dismissed",
    filterConvicted: "Convicted",
    colCase: "Case",
    colSections: "IPC / Act Sections",
    colDescription: "Description",
    colStatus: "Status",
    seriousBadge: "Serious",
    noDetailTitle: "Case details not yet available",
    noDetailBody:
      "Structured case data is scraped from ECI Form 26A affidavits. This will be populated after the next ingest run.",
    viewAffidavit: "View ECI Affidavit (Form 26A)",
    dataNote: "Source: ECI Form 26A · MyNeta / ADR",
    partialNote: (shown: number, declared: number) =>
      `Showing ${shown} of ${declared} declared cases — remaining cases could not be extracted from affidavit text. Tap "View ECI Affidavit" below to see all.`,
    close: "Close",
    actLabel: (act: string) => act,
    emptyFilter: "No cases with this status.",
    severityLabel: {
      CLEAN: "Clean Record",
      MINOR: "Minor Cases",
      MODERATE: "Moderate Cases",
      SERIOUS: "Serious Cases",
    },
  },
  ta: {
    title: "குற்ற வரலாறு",
    subtitle: (n: number) =>
      n === 0
        ? "எந்த வழக்கும் பதிவாகவில்லை"
        : `ECI உறுதிமொழியில் ${n} வழக்கு${n > 1 ? "கள்" : ""} அறிவிக்கப்பட்டது`,
    filterAll: "அனைத்தும்",
    filterPending: "நிலுவையில்",
    filterDismissed: "தள்ளுபடி",
    filterConvicted: "தண்டிக்கப்பட்டது",
    colCase: "வழக்கு",
    colSections: "சட்டப் பிரிவுகள்",
    colDescription: "குற்றம்",
    colStatus: "நிலை",
    seriousBadge: "தீவிரம்",
    noDetailTitle: "விரிவான வழக்கு தரவு இல்லை",
    noDetailBody:
      "வழக்கு விவரங்கள் ECI படிவம் 26A இல் இருந்து சேகரிக்கப்படும். அடுத்த இறக்குமதி நடக்கும்போது நிரப்பப்படும்.",
    viewAffidavit: "ECI உறுதிமொழி ஆவணம் காண்க (படிவம் 26A)",
    dataNote: "ஆதாரம்: ECI படிவம் 26A · MyNeta / ADR",
    partialNote: (shown: number, declared: number) =>
      `${declared} வழக்குகளில் ${shown} மட்டும் காட்டப்படுகிறது — மீதமுள்ளவை உறுதிமொழி ஆவணத்திலிருந்து பிரித்தெடுக்க இயலவில்லை. கீழே உள்ள "ECI உறுதிமொழி ஆவணம்" பொத்தானை அழுத்தவும்.`,
    close: "மூடு",
    actLabel: (act: string) => act,
    emptyFilter: "இந்த நிலையில் வழக்குகள் இல்லை.",
    severityLabel: {
      CLEAN: "தூய்மையான பதிவு",
      MINOR: "சிறிய வழக்குகள்",
      MODERATE: "நடுத்தர வழக்குகள்",
      SERIOUS: "தீவிர வழக்குகள்",
    },
  },
} as const;

// ---------------------------------------------------------------------------
// Status badge styles
// ---------------------------------------------------------------------------
const STATUS_STYLE: Record<CriminalCaseStatus, string> = {
  Pending:   "bg-yellow-100 text-yellow-800",
  Dismissed: "bg-emerald-100 text-emerald-800",
  Convicted: "bg-red-100 text-red-800",
};

const STATUS_FILTER_KEYS: Array<"All" | CriminalCaseStatus> = [
  "All", "Pending", "Dismissed", "Convicted",
];

const SEVERITY_STYLE = {
  CLEAN:    "bg-emerald-100 text-emerald-800 border-emerald-200",
  MINOR:    "bg-yellow-100 text-yellow-800 border-yellow-200",
  MODERATE: "bg-orange-100 text-orange-800 border-orange-200",
  SERIOUS:  "bg-red-100 text-red-800 border-red-200",
};

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------
export function CandidateCriminalModal({
  mla,
  isOpen,
  onClose,
  lang = "en",
}: CandidateCriminalModalProps) {
  const t = T[lang];
  const [activeFilter, setActiveFilter] = useState<"All" | CriminalCaseStatus>("All");
  const backdropRef = useRef<HTMLDivElement>(null);
  const focusTrapRef = useFocusTrap(isOpen);

  // Lock body scroll while open
  useEffect(() => {
    if (isOpen) {
      document.body.style.overflow = "hidden";
    } else {
      document.body.style.overflow = "";
    }
    return () => { document.body.style.overflow = ""; };
  }, [isOpen]);

  // Reset filter when modal opens
  useEffect(() => {
    if (isOpen) setActiveFilter("All");
  }, [isOpen]);

  // Close on Escape
  useEffect(() => {
    if (!isOpen) return;
    const handler = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  const cases = mla.criminal_cases ?? [];
  const hasCaseDetail = cases.length > 0;
  const grouped = groupCasesByStatus(cases);

  const filteredCases: CriminalCase[] =
    activeFilter === "All" ? cases : grouped[activeFilter];

  const counts = {
    All:       cases.length,
    Pending:   grouped.Pending.length,
    Dismissed: grouped.Dismissed.length,
    Convicted: grouped.Convicted.length,
  };

  // Use parsed case count as authoritative display count.
  // criminal_cases_total is what the candidate declared; cases.length is what
  // we could parse from the ECI affidavit text. Show whichever is available.
  const displayTotal = hasCaseDetail ? cases.length : (mla.criminal_cases_total ?? 0);
  const severity = (mla.criminal_severity ?? "CLEAN") as keyof typeof SEVERITY_STYLE;

  return (
    <div
      ref={backdropRef}
      className="fixed inset-0 z-50 flex items-end sm:items-center justify-center bg-black/50 backdrop-blur-sm p-0 sm:p-4"
      onClick={(e) => { if (e.target === backdropRef.current) onClose(); }}
    >
      <div ref={focusTrapRef} className="relative w-full sm:max-w-2xl max-h-[92dvh] sm:max-h-[85dvh] bg-white sm:rounded-2xl rounded-t-2xl shadow-2xl flex flex-col overflow-hidden">
        {/* ── Header ── */}
        <div className="flex items-start justify-between gap-3 px-5 pt-5 pb-4 border-b border-gray-100">
          <div className="min-w-0">
            <h2 className="text-base font-black text-gray-900 leading-tight">
              {t.title} — {mla.mla_name}
            </h2>
            <p className="text-xs text-gray-500 mt-0.5">
              {t.subtitle(displayTotal)}
            </p>
          </div>
          {/* Severity badge */}
          <div className="flex items-start gap-2 shrink-0">
            <span className={`text-xs font-semibold px-2 py-1 rounded-lg border ${SEVERITY_STYLE[severity]}`}>
              {t.severityLabel[severity]}
            </span>
            <button
              onClick={onClose}
              aria-label={t.close}
              className="text-gray-400 hover:text-gray-700 text-xl leading-none p-1"
            >
              ×
            </button>
          </div>
        </div>

        {/* ── Body ── */}
        <div className="flex-1 overflow-y-auto">
          {hasCaseDetail ? (
            <>
              {/* Status filter tabs */}
              <div className="flex gap-2 px-5 py-3 border-b border-gray-100 overflow-x-auto">
                {STATUS_FILTER_KEYS.map((key) => {
                  const label =
                    key === "All"       ? t.filterAll :
                    key === "Pending"   ? t.filterPending :
                    key === "Dismissed" ? t.filterDismissed :
                                         t.filterConvicted;
                  const count = counts[key];
                  const isActive = activeFilter === key;
                  return (
                    <button
                      key={key}
                      onClick={() => setActiveFilter(key)}
                      className={`shrink-0 text-xs font-semibold px-3 py-1.5 rounded-full border transition-colors ${
                        isActive
                          ? "bg-gray-900 text-white border-gray-900"
                          : "bg-white text-gray-600 border-gray-300 hover:bg-gray-50"
                      }`}
                    >
                      {label}
                      {count > 0 && (
                        <span className={`ml-1.5 px-1.5 py-0.5 rounded-full text-[10px] font-bold ${
                          isActive ? "bg-white/20 text-white" : "bg-gray-100 text-gray-700"
                        }`}>
                          {count}
                        </span>
                      )}
                    </button>
                  );
                })}
              </div>

              {/* Case table */}
              {filteredCases.length === 0 ? (
                <p className="px-5 py-8 text-center text-sm text-gray-400 italic">
                  {t.emptyFilter}
                </p>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-xs border-collapse min-w-[520px]">
                    <thead>
                      <tr className="bg-gray-50 text-gray-500 text-left uppercase tracking-wide">
                        <th className="px-4 py-2.5 font-semibold w-10">#</th>
                        <th className="px-4 py-2.5 font-semibold">{t.colSections}</th>
                        <th className="px-4 py-2.5 font-semibold">{t.colDescription}</th>
                        <th className="px-4 py-2.5 font-semibold whitespace-nowrap">{t.colStatus}</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                      {filteredCases.map((c, i) => (
                        <tr
                          key={c.case_id ?? i}
                          className={c.is_serious ? "bg-red-50/60" : "bg-white"}
                        >
                          <td className="px-4 py-3 text-gray-400 font-medium">{i + 1}</td>
                          <td className="px-4 py-3">
                            <div className="flex flex-wrap gap-1">
                              {c.ipc_sections.map((s) => (
                                <span
                                  key={s}
                                  className={`px-1.5 py-0.5 rounded text-[11px] font-bold border ${
                                    c.is_serious
                                      ? "bg-red-100 text-red-800 border-red-200"
                                      : "bg-gray-100 text-gray-700 border-gray-200"
                                  }`}
                                >
                                  {s}
                                </span>
                              ))}
                              <span className="text-[11px] text-gray-400 self-center">
                                {c.act}
                              </span>
                            </div>
                            {c.is_serious && (
                              <span className="mt-1 inline-flex items-center gap-1 text-[10px] font-bold text-red-700">
                                ⚠ {t.seriousBadge}
                              </span>
                            )}
                          </td>
                          <td className="px-4 py-3 text-gray-800 leading-snug">
                            {c.description}
                            {c.court && (
                              <p className="text-gray-400 mt-0.5">{c.court}</p>
                            )}
                          </td>
                          <td className="px-4 py-3">
                            <span className={`px-2 py-1 rounded-full text-[11px] font-semibold ${STATUS_STYLE[c.status]}`}>
                              {c.status}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              {/* Partial-parse disclosure — shown when parsed count < declared count */}
              {cases.length < mla.criminal_cases_total && (
                <p className="px-5 py-3 text-[11px] text-amber-700 bg-amber-50 border-t border-amber-100">
                  ⚠ {t.partialNote(cases.length, mla.criminal_cases_total)}
                </p>
              )}
            </>
          ) : (
            /* Empty state — case details not yet ingested */
            <div className="px-5 py-8 space-y-4">
              {/* Aggregate summary pills */}
              {(mla.criminal_cases_total ?? 0) > 0 && (
                <div className="flex items-center gap-3 p-4 rounded-xl bg-amber-50 border border-amber-200">
                  <span className="text-2xl">⚠</span>
                  <div>
                    <p className="text-sm font-bold text-amber-900">
                      {mla.criminal_cases_total} {lang === "ta" ? "வழக்குகள் அறிவிக்கப்பட்டன" : "cases declared in ECI affidavit"}
                    </p>
                    <p className="text-xs text-amber-700 mt-0.5">
                      {lang === "ta"
                        ? `தீவிரம்: ${T.ta.severityLabel[severity]}`
                        : `Severity classification: ${T.en.severityLabel[severity]}`}
                    </p>
                  </div>
                </div>
              )}
              {(mla.criminal_cases_total ?? 0) === 0 && (
                <div className="flex items-center gap-3 p-4 rounded-xl bg-emerald-50 border border-emerald-200">
                  <span className="text-2xl">✓</span>
                  <p className="text-sm font-bold text-emerald-900">
                    {lang === "ta" ? "எந்த குற்ற வழக்கும் அறிவிக்கப்படவில்லை." : "No criminal cases declared in ECI affidavit."}
                  </p>
                </div>
              )}
              <div className="rounded-xl border border-gray-200 bg-gray-50 p-4 text-center">
                <p className="text-sm font-semibold text-gray-700">{t.noDetailTitle}</p>
                <p className="text-xs text-gray-400 mt-1 leading-relaxed">{t.noDetailBody}</p>
              </div>
            </div>
          )}
        </div>

        {/* ── Footer ── */}
        <div className="px-5 py-3 border-t border-gray-100 flex items-center justify-between gap-3 bg-gray-50/60">
          <p className="text-[11px] text-gray-400">
            {lang === "ta" ? "ஆதாரம்: ECI படிவம் 26A · " : "Source: ECI Form 26A · "}
            <a
              href={mla.source_url}
              target="_blank"
              rel="noopener noreferrer"
              className="underline underline-offset-2 hover:text-gray-600 transition-colors"
            >
              MyNeta
            </a>
            {" / "}
            <a
              href="https://adrindia.org"
              target="_blank"
              rel="noopener noreferrer"
              className="underline underline-offset-2 hover:text-gray-600 transition-colors"
            >
              ADR
            </a>
          </p>
          {mla.source_pdf && (
            <a
              href={mla.source_pdf}
              target="_blank"
              rel="noopener noreferrer"
              className="shrink-0 inline-flex items-center gap-1.5 text-xs font-semibold px-3 py-1.5 rounded-lg border border-gray-300 text-gray-700 hover:bg-white transition-colors"
            >
              📄 {t.viewAffidavit}
            </a>
          )}
        </div>
      </div>
    </div>
  );
}
