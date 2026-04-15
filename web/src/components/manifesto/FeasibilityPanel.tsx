"use client";

import { useEffect, useState } from "react";
import { apiGet } from "@/lib/api-client";

interface FeasibilityResult {
  promise_id: string;
  promise_label: string;
  amount_cr: number | null;
  amount_raw: string | null;
  sdg_goals: Array<{ id: string; label: string; weight: number }>;
  affected_indicators: Array<{
    sdg_id: string;
    sdg_weight: number;
    indicator_id: string;
    indicator_label: string;
    indicator_type: string;
    period: string | null;
  }>;
  causal_chain: Array<{
    from: string;
    to: string;
    to_label: string;
    weight: number;
    period: string | null;
  }>;
  fiscal_snapshot: {
    fiscal_year: string;
    summary?: Record<string, number | null>;
    receipts?: Record<string, number | null>;
  } | null;
  metrics: {
    cost_as_pct_revenue_receipts: number | null;
    cost_as_pct_fiscal_deficit: number | null;
  };
  score: number;
  score_band: string;
  notes: string[];
}

const SCORE_BAND_STYLES: Record<string, string> = {
  "High feasibility": "bg-emerald-100 text-emerald-800 border-emerald-200",
  "Moderate feasibility": "bg-amber-100 text-amber-800 border-amber-200",
  "Stretched": "bg-orange-100 text-orange-800 border-orange-200",
  "Low feasibility": "bg-rose-100 text-rose-800 border-rose-200",
};

interface Props {
  docId: string;
  lang?: "en" | "ta";
}

export function FeasibilityPanel({ docId, lang = "en" }: Props) {
  const [data, setData] = useState<FeasibilityResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    apiGet<FeasibilityResult>(`/api/graph/feasibility/${encodeURIComponent(docId)}`)
      .then((r) => {
        if (!cancelled) setData(r);
      })
      .catch((e: Error) => {
        if (!cancelled) setError(e.message);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [docId]);

  if (loading) {
    return (
      <div className="mt-2 rounded-lg border border-gray-200 bg-gray-50 p-3 text-xs text-gray-600">
        {lang === "ta" ? "பகுப்பாய்வு…" : "Computing feasibility…"}
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="mt-2 rounded-lg border border-rose-200 bg-rose-50 p-3 text-xs text-rose-700">
        {lang === "ta" ? "பகுப்பாய்வு கிடைக்கவில்லை: " : "Could not compute feasibility: "}
        {error ?? "unknown error"}
      </div>
    );
  }

  const bandStyle = SCORE_BAND_STYLES[data.score_band] ?? "bg-gray-100 text-gray-800 border-gray-200";
  const fy = data.fiscal_snapshot?.fiscal_year;

  return (
    <div className="mt-2 rounded-lg border border-indigo-200 bg-indigo-50/40 p-4 flex flex-col gap-3 text-xs">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div className="flex items-center gap-2">
          <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full font-bold border ${bandStyle}`}>
            {data.score}/100 · {data.score_band}
          </span>
          {fy && (
            <span className="text-gray-500">
              {lang === "ta" ? "நிதி ஆண்டு" : "FY"} {fy}
            </span>
          )}
        </div>
        {data.amount_cr !== null && (
          <span className="font-semibold text-gray-700">
            ₹{data.amount_cr.toLocaleString("en-IN")} cr
            {data.amount_raw && data.amount_raw !== `₹${data.amount_cr} crore` && (
              <span className="ml-1 text-gray-400 font-normal">({data.amount_raw})</span>
            )}
          </span>
        )}
      </div>

      {(data.metrics.cost_as_pct_revenue_receipts !== null ||
        data.metrics.cost_as_pct_fiscal_deficit !== null) && (
        <div className="grid grid-cols-2 gap-2 text-[11px]">
          {data.metrics.cost_as_pct_revenue_receipts !== null && (
            <div className="rounded bg-white border border-gray-200 p-2">
              <div className="text-gray-500">
                {lang === "ta" ? "வருவாய் வரவின் %" : "% of revenue receipts"}
              </div>
              <div className="font-bold text-gray-900">
                {data.metrics.cost_as_pct_revenue_receipts}%
              </div>
            </div>
          )}
          {data.metrics.cost_as_pct_fiscal_deficit !== null && (
            <div className="rounded bg-white border border-gray-200 p-2">
              <div className="text-gray-500">
                {lang === "ta" ? "நிதி பற்றாக்குறையின் %" : "% of fiscal deficit"}
              </div>
              <div className="font-bold text-gray-900">
                {data.metrics.cost_as_pct_fiscal_deficit}%
              </div>
            </div>
          )}
        </div>
      )}

      {data.sdg_goals.length > 0 && (
        <div>
          <div className="text-gray-500 mb-1">{lang === "ta" ? "இலக்கு SDG" : "Targets SDG"}</div>
          <div className="flex flex-wrap gap-1.5">
            {data.sdg_goals.map((s) => (
              <span
                key={s.id}
                className="inline-flex items-center gap-1 px-2 py-0.5 rounded bg-sky-100 text-sky-800 font-medium"
              >
                {s.id.replace("sdg:", "SDG ")}
                {s.weight < 1 && <span className="text-sky-500">×{s.weight}</span>}
              </span>
            ))}
          </div>
        </div>
      )}

      {data.affected_indicators.length > 0 && (
        <div>
          <div className="text-gray-500 mb-1">
            {lang === "ta" ? "தாக்கும் குறிகாட்டிகள்" : "Measured by"}
          </div>
          <ul className="flex flex-col gap-1">
            {data.affected_indicators.map((ind, i) => (
              <li key={`${ind.indicator_id}-${ind.sdg_id}-${i}`} className="flex items-center gap-2">
                <span className="font-medium text-gray-800">{ind.indicator_label}</span>
                {ind.period && <span className="text-gray-400">· {ind.period}</span>}
              </li>
            ))}
          </ul>
        </div>
      )}

      {data.causal_chain.length > 0 && (
        <div>
          <div className="text-gray-500 mb-1">
            {lang === "ta" ? "காரணிய தொடர்" : "Causal chain"}
          </div>
          <ul className="flex flex-col gap-1">
            {data.causal_chain.map((c, i) => (
              <li key={i} className="text-gray-700">
                {prettyIndicatorId(c.from)} <span className="text-gray-400">→</span>{" "}
                <span className="font-medium">{c.to_label ?? prettyIndicatorId(c.to)}</span>
                <span className="text-gray-400"> (w={c.weight})</span>
              </li>
            ))}
          </ul>
        </div>
      )}

      {data.notes.length > 0 && (
        <div className="rounded bg-amber-50 border border-amber-200 p-2 text-amber-800">
          <ul className="list-disc pl-4 flex flex-col gap-0.5">
            {data.notes.map((n, i) => (
              <li key={i}>{n}</li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

function prettyIndicatorId(id: string): string {
  const [prefix, slug] = id.split(":");
  const kind = prefix?.replace("indicator_", "") ?? prefix;
  return `${kind}:${slug}`;
}
