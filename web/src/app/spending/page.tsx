"use client";

import { useState, useEffect, useMemo } from "react";
import Link from "next/link";
import { apiGet } from "@/lib/api-client";
import { useLanguage } from "@/lib/LanguageContext";

// ── Types ────────────────────────────────────────────────────────────────────

interface TaxBreakdownItem {
  head: string;
  amount_cr: number;
  pct: number;
  ta: string;
  note?: string;
}

interface DeptSector {
  sector: string;
  amount_cr: number;
  pct: number;
  ta: string;
  national_avg_pct: number | null;
}

interface FCFormulaCriterion {
  weight_pct: number;
  description: string;
  tn_impact?: string;
  tn_note?: string;
  change?: string;
}

interface FCStateShare {
  state: string;
  share_pct: number;
  highlight?: boolean;
}

interface FCShareTrend {
  commission: string;
  period: string;
  share_pct: number;
}

interface BudgetYear {
  fiscal_year: string;
  revenue?: { total_revenue_receipts_cr?: number; own_tax_revenue_cr?: number; central_devolution_cr?: number; central_grants_cr?: number };
  expenditure?: { total_exp_cr?: number; revenue_exp_cr?: number; revenue_expenditure_cr?: number; capital_exp_cr?: number; capital_expenditure_cr?: number };
  committed?: { salaries_cr?: number; pensions_cr?: number; interest_cr?: number; subsidies_cr?: number; total_committed_cr?: number; discretionary_cr?: number };
  fiscal?: { fiscal_deficit_cr?: number; revenue_deficit_cr?: number };
}

interface MLACDSYear {
  fiscal_year: string;
  per_constituency_allocation_cr: number;
  state_total_allocation_cr: number;
  performance?: { utilization_pct?: number; works_initiated?: number; works_completed?: number };
}

interface SpendingData {
  population_cr: number;
  gsdp_lakh_cr: number;
  per_capita_income_lakh: number;
  national_avg_per_capita_lakh: number;
  gdp_contribution_pct: number;
  population_share_pct: number;
  finance_commission: {
    "15th": { tn_share_pct: number; formula: Record<string, FCFormulaCriterion>; state_shares: FCStateShare[] };
    "16th": { tn_share_pct: number; formula: Record<string, FCFormulaCriterion> };
    donor_state: { headline: string; fc_share_trend: FCShareTrend[] };
  };
  revenue_breakdown_2024_25: {
    total_revenue_cr: number;
    own_tax: { total_cr: number; pct_of_revenue: number; breakdown: TaxBreakdownItem[] };
    non_tax_revenue: { total_cr: number; pct_of_revenue: number };
    central_devolution: { total_cr: number; pct_of_revenue: number };
    central_grants: { total_cr: number; pct_of_revenue: number };
  };
  department_spending_2024_25: { total_expenditure_cr: number; sectors: DeptSector[] };
  theory: Record<string, { title_en: string; title_ta: string; description_en: string; description_ta: string }>;
  budgets: BudgetYear[];
  mlacds: MLACDSYear[];
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function fCr(v: number): string {
  if (v >= 100000) return `${(v / 100000).toFixed(2)}L Cr`;
  return `${v.toLocaleString("en-IN")} Cr`;
}

function fPct(v: number): string {
  return `${v.toFixed(1)}%`;
}

const IMPACT_COLOR: Record<string, string> = {
  positive: "text-green-600 bg-green-50",
  negative: "text-red-600 bg-red-50",
  moderate: "text-amber-600 bg-amber-50",
};

const IMPACT_LABEL: Record<string, Record<string, string>> = {
  positive: { en: "Benefits TN", ta: "TN-க்கு சாதகம்" },
  negative: { en: "Hurts TN", ta: "TN-க்கு பாதகம்" },
  moderate: { en: "Moderate", ta: "மிதமான தாக்கம்" },
};

// ── Page ─────────────────────────────────────────────────────────────────────

export default function SpendingPage() {
  const { lang, setLang } = useLanguage();
  const isTA = lang === "ta";
  const [data, setData] = useState<SpendingData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    apiGet<SpendingData>("/api/spending")
      .then(setData)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  // Sorted budgets (chronological)
  const budgets = useMemo(
    () => [...(data?.budgets || [])].sort((a, b) => a.fiscal_year.localeCompare(b.fiscal_year)),
    [data]
  );

  const latestBudget = budgets.length > 0 ? budgets[budgets.length - 1] : null;

  if (loading) {
    return (
      <main className="min-h-full bg-gray-50">
        <header className="sticky top-0 z-30 bg-white border-b border-gray-200 shadow-sm">
          <div className="max-w-3xl mx-auto px-4 py-3 flex items-center gap-3">
            <Link href="/" className="text-gray-400 hover:text-gray-600"><svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" /></svg></Link>
            <h1 className="text-lg font-black text-gray-900">{isTA ? "செலவு & நிதி" : "Spending & Finance"}</h1>
          </div>
        </header>
        <div className="max-w-3xl mx-auto px-4 py-8 space-y-6">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="h-48 bg-gray-100 rounded-2xl animate-pulse" />
          ))}
        </div>
      </main>
    );
  }

  if (!data) return null;

  const rev = data.revenue_breakdown_2024_25;
  const dept = data.department_spending_2024_25;
  const fc15 = data.finance_commission["15th"];
  const fc16 = data.finance_commission["16th"];
  const donor = data.finance_commission.donor_state;

  // Per-person math
  const totalExpCr = latestBudget?.expenditure?.total_exp_cr || dept.total_expenditure_cr;
  const popLakh = data.population_cr * 100;
  const perPersonPerYear = (totalExpCr * 10000000) / (data.population_cr * 10000000); // in rupees
  const perPersonPerDay = perPersonPerYear / 365;
  const committedCr = latestBudget?.committed?.total_committed_cr || 0;
  const discretionaryCr = latestBudget?.committed?.discretionary_cr || 0;
  const committedPerDay = committedCr > 0 ? (committedCr / totalExpCr) * perPersonPerDay : 0;
  const discretionaryPerDay = discretionaryCr > 0 ? (discretionaryCr / totalExpCr) * perPersonPerDay : 0;

  return (
    <main className="min-h-full bg-gray-50">
      {/* Header */}
      <header className="sticky top-0 z-30 bg-white border-b border-gray-200 shadow-sm">
        <div className="max-w-3xl mx-auto px-4 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Link href="/" className="text-gray-400 hover:text-gray-600 transition-colors">
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
              </svg>
            </Link>
            <div>
              <h1 className="text-lg font-black text-gray-900">
                {isTA ? "செலவு" : "Spending"}
              </h1>
              <p className="text-[10px] text-gray-500 font-semibold uppercase tracking-wider">
                {isTA ? "தமிழ்நாடு நிதி கையேடு" : "Tamil Nadu Fiscal Guide"}
              </p>
            </div>
          </div>
          <button
            onClick={() => setLang(lang === "en" ? "ta" : "en")}
            className="text-xs font-bold px-3 py-1.5 rounded-full border border-gray-300 hover:bg-gray-100 transition-colors text-gray-900"
          >
            {lang === "en" ? "தமிழ்" : "English"}
          </button>
        </div>
      </header>

      <div className="max-w-3xl mx-auto px-4 py-6 space-y-10">

        {/* ══════════════════════════════════════════════════════════════════
            SECTION 1: Where does TN get its money?
           ══════════════════════════════════════════════════════════════════ */}
        <section className="space-y-4">
          <div>
            <p className="text-[10px] font-bold text-blue-600 uppercase tracking-wider mb-1">
              {isTA ? "பகுதி 1" : "SECTION 1"}
            </p>
            <h2 className="text-xl font-black text-gray-900">
              {isTA ? "தமிழ்நாடுக்கு பணம் எங்கிருந்து வருகிறது?" : "Where does Tamil Nadu get its money?"}
            </h2>
          </div>

          {/* Theory card */}
          <div className="bg-blue-50 border border-blue-200 rounded-2xl p-4 text-sm text-blue-900 leading-relaxed">
            {isTA ? data.theory.how_states_earn.description_ta : data.theory.how_states_earn.description_en}
          </div>

          {/* Three buckets */}
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            {[
              { label: isTA ? "சொந்த வரி" : "Own Tax Revenue", amount: rev.own_tax.total_cr, pct: rev.own_tax.pct_of_revenue, color: "bg-blue-500", bg: "bg-blue-50" },
              { label: isTA ? "மத்திய பகிர்வு" : "Central Devolution", amount: rev.central_devolution.total_cr, pct: rev.central_devolution.pct_of_revenue, color: "bg-orange-500", bg: "bg-orange-50" },
              { label: isTA ? "மத்திய மானியம்" : "Central Grants", amount: rev.central_grants.total_cr, pct: rev.central_grants.pct_of_revenue, color: "bg-green-500", bg: "bg-green-50" },
            ].map((b) => (
              <div key={b.label} className={`${b.bg} rounded-xl p-4 text-center`}>
                <div className={`w-3 h-3 rounded-full ${b.color} mx-auto mb-2`} />
                <p className="text-lg font-black text-gray-900">{fCr(b.amount)}</p>
                <p className="text-xs font-semibold text-gray-600">{b.label}</p>
                <p className="text-[10px] text-gray-500">{fPct(b.pct)} {isTA ? "வருவாயில்" : "of revenue"}</p>
              </div>
            ))}
          </div>

          {/* Revenue bar */}
          <div>
            <p className="text-xs font-semibold text-gray-500 mb-1">
              {isTA ? "மொத்த வருவாய்" : "Total Revenue"}: <span className="text-gray-900 font-black">{fCr(rev.total_revenue_cr)}</span> (2024-25)
            </p>
            <div className="h-4 rounded-full overflow-hidden flex">
              <div className="bg-blue-500" style={{ width: `${rev.own_tax.pct_of_revenue}%` }} title={`Own Tax: ${fPct(rev.own_tax.pct_of_revenue)}`} />
              <div className="bg-blue-300" style={{ width: `${rev.non_tax_revenue.pct_of_revenue}%` }} title={`Non-Tax: ${fPct(rev.non_tax_revenue.pct_of_revenue)}`} />
              <div className="bg-orange-400" style={{ width: `${rev.central_devolution.pct_of_revenue}%` }} title={`Central Share: ${fPct(rev.central_devolution.pct_of_revenue)}`} />
              <div className="bg-green-400" style={{ width: `${rev.central_grants.pct_of_revenue}%` }} title={`Grants: ${fPct(rev.central_grants.pct_of_revenue)}`} />
            </div>
            <div className="flex justify-between mt-1 text-[9px] text-gray-400">
              <span>{isTA ? "சொந்தம் (75.6%)" : "Own (75.6%)"}</span>
              <span>{isTA ? "மத்தியம் (24.4%)" : "Centre (24.4%)"}</span>
            </div>
          </div>

          {/* Own tax breakdown */}
          <div className="bg-white rounded-2xl border border-gray-200 p-4 space-y-3">
            <p className="text-xs font-bold text-gray-500 uppercase tracking-wide">
              {isTA ? "சொந்த வரி வருவாய் பிரிவு" : "Own Tax Revenue Breakdown"}
            </p>
            {rev.own_tax.breakdown.map((t) => (
              <div key={t.head} className="flex items-center gap-3">
                <div className="flex-1 min-w-0">
                  <div className="flex items-center justify-between mb-0.5">
                    <p className="text-xs font-semibold text-gray-800 truncate">
                      {isTA ? t.ta : t.head}
                    </p>
                    <p className="text-xs font-bold text-gray-900">{fCr(t.amount_cr)}</p>
                  </div>
                  <div className="h-2 rounded-full bg-gray-100 overflow-hidden">
                    <div className="h-full bg-blue-400 rounded-full" style={{ width: `${t.pct}%` }} />
                  </div>
                </div>
                <span className="text-[10px] text-gray-400 w-10 text-right">{fPct(t.pct)}</span>
              </div>
            ))}
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════════════════
            SECTION 2: Finance Commission
           ══════════════════════════════════════════════════════════════════ */}
        <section className="space-y-4">
          <div>
            <p className="text-[10px] font-bold text-orange-600 uppercase tracking-wider mb-1">
              {isTA ? "பகுதி 2" : "SECTION 2"}
            </p>
            <h2 className="text-xl font-black text-gray-900">
              {isTA ? "நிதிக்குழு — பணம் எப்படி பிரிக்கப்படுகிறது?" : "The Finance Commission — How is the pie divided?"}
            </h2>
          </div>

          {/* Theory */}
          <div className="bg-orange-50 border border-orange-200 rounded-2xl p-4 text-sm text-orange-900 leading-relaxed">
            {isTA ? data.theory.what_is_finance_commission.description_ta : data.theory.what_is_finance_commission.description_en}
          </div>

          {/* Donor state callout */}
          <div className="bg-red-50 border-2 border-red-200 rounded-2xl p-4 space-y-3">
            <p className="text-sm font-bold text-red-900 leading-snug">
              {isTA
                ? `தமிழ்நாடு இந்தியாவின் GDP-யில் ${donor.fc_share_trend[donor.fc_share_trend.length - 1]?.share_pct}% பங்களிக்கிறது, ஆனால் மக்கள்தொகையில் ${data.population_share_pct}% மட்டுமே — ஆனாலும் மத்திய வரிப் பகிர்வில் ${data.finance_commission["16th"].tn_share_pct}% மட்டுமே பெறுகிறது.`
                : donor.headline}
            </p>
            <div className="grid grid-cols-3 gap-2 text-center">
              <div className="bg-white rounded-lg p-2">
                <p className="text-lg font-black text-gray-900">{data.gdp_contribution_pct}%</p>
                <p className="text-[9px] text-gray-500">{isTA ? "GDP பங்கு" : "GDP Contribution"}</p>
              </div>
              <div className="bg-white rounded-lg p-2">
                <p className="text-lg font-black text-gray-900">{data.population_share_pct}%</p>
                <p className="text-[9px] text-gray-500">{isTA ? "மக்கள்தொகை" : "Population"}</p>
              </div>
              <div className="bg-white rounded-lg p-2">
                <p className="text-lg font-black text-red-600">{fc16.tn_share_pct}%</p>
                <p className="text-[9px] text-gray-500">{isTA ? "பகிர்வு" : "Gets Back"}</p>
              </div>
            </div>
          </div>

          {/* FC share trend */}
          <div className="bg-white rounded-2xl border border-gray-200 p-4 space-y-3">
            <p className="text-xs font-bold text-gray-500 uppercase tracking-wide">
              {isTA ? "TN-ன் நிதிக்குழு பங்கு — வீழ்ச்சிப் போக்கு" : "TN's Finance Commission Share — Declining Trend"}
            </p>
            <div className="space-y-1.5">
              {donor.fc_share_trend.map((t) => (
                <div key={t.commission} className="flex items-center gap-2">
                  <span className="text-[10px] text-gray-400 w-16">{t.period}</span>
                  <div className="flex-1 h-4 bg-gray-50 rounded-full overflow-hidden">
                    <div
                      className={`h-full rounded-full ${t.share_pct > 5 ? "bg-orange-400" : t.share_pct > 4 ? "bg-orange-300" : "bg-red-400"}`}
                      style={{ width: `${(t.share_pct / 10) * 100}%` }}
                    />
                  </div>
                  <span className="text-xs font-bold text-gray-900 w-12 text-right">{t.share_pct}%</span>
                </div>
              ))}
            </div>
            <p className="text-[10px] text-gray-400">
              {isTA
                ? "9வது நிதிக்குழுவில் 7.93% இருந்தது, 16வது நிதிக்குழுவில் 4.10% ஆக குறைந்துள்ளது"
                : "Dropped from 7.93% (9th FC) to 4.10% (16th FC) — nearly halved over 35 years"}
            </p>
          </div>

          {/* Formula comparison */}
          <div className="bg-white rounded-2xl border border-gray-200 p-4 space-y-3">
            <p className="text-xs font-bold text-gray-500 uppercase tracking-wide">
              {isTA ? "16வது நிதிக்குழு சூத்திரம் (2026-31)" : "16th FC Devolution Formula (2026-31)"}
            </p>
            {Object.entries(fc16.formula).map(([key, c]) => (
              <div key={key} className="flex items-start gap-3 py-1">
                <div className="text-right w-10 flex-shrink-0">
                  <p className="text-sm font-black text-gray-900">{c.weight_pct}%</p>
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-xs font-semibold text-gray-800">
                    {key.replace(/_/g, " ").replace(/\b\w/g, (l) => l.toUpperCase())}
                  </p>
                  <p className="text-[10px] text-gray-500">{c.description}</p>
                  {c.change && (
                    <p className="text-[9px] text-blue-600 font-semibold mt-0.5">{c.change}</p>
                  )}
                </div>
                {fc15.formula[key]?.tn_impact && (
                  <span className={`text-[8px] font-bold px-1.5 py-0.5 rounded-full flex-shrink-0 ${IMPACT_COLOR[fc15.formula[key].tn_impact!]}`}>
                    {IMPACT_LABEL[fc15.formula[key].tn_impact!]?.[lang] || fc15.formula[key].tn_impact}
                  </span>
                )}
              </div>
            ))}
          </div>

          {/* State comparison */}
          <div className="bg-white rounded-2xl border border-gray-200 p-4 space-y-2">
            <p className="text-xs font-bold text-gray-500 uppercase tracking-wide">
              {isTA ? "மாநிலவாரி பகிர்வு (15வது நிதிக்குழு)" : "State-wise Share (15th FC)"}
            </p>
            {fc15.state_shares.slice(0, 10).map((s) => (
              <div key={s.state} className={`flex items-center gap-2 ${s.highlight ? "bg-orange-50 -mx-2 px-2 py-1 rounded-lg" : ""}`}>
                <span className={`text-xs w-28 truncate ${s.highlight ? "font-black text-orange-700" : "text-gray-700"}`}>
                  {s.state}
                </span>
                <div className="flex-1 h-3 bg-gray-100 rounded-full overflow-hidden">
                  <div
                    className={`h-full rounded-full ${s.highlight ? "bg-orange-500" : "bg-gray-300"}`}
                    style={{ width: `${(s.share_pct / 20) * 100}%` }}
                  />
                </div>
                <span className={`text-xs font-bold w-12 text-right ${s.highlight ? "text-orange-700" : "text-gray-600"}`}>
                  {s.share_pct}%
                </span>
              </div>
            ))}
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════════════════
            SECTION 3: Where does it all go?
           ══════════════════════════════════════════════════════════════════ */}
        <section className="space-y-4">
          <div>
            <p className="text-[10px] font-bold text-purple-600 uppercase tracking-wider mb-1">
              {isTA ? "பகுதி 3" : "SECTION 3"}
            </p>
            <h2 className="text-xl font-black text-gray-900">
              {isTA ? "பணம் எங்கே செல்கிறது?" : "Where does all the money go?"}
            </h2>
          </div>

          {/* Committed vs Discretionary theory */}
          <div className="bg-purple-50 border border-purple-200 rounded-2xl p-4 text-sm text-purple-900 leading-relaxed">
            {isTA ? data.theory.committed_vs_discretionary.description_ta : data.theory.committed_vs_discretionary.description_en}
          </div>

          {/* Spending waterfall */}
          {latestBudget?.committed && (() => {
            const c = latestBudget.committed!;
            const totalRev = latestBudget.revenue?.total_revenue_receipts_cr || rev.total_revenue_cr;
            const items = [
              { label: isTA ? "மொத்த வருவாய்" : "Total Revenue", amount: totalRev, remaining: totalRev, color: "bg-blue-500" },
              { label: isTA ? "சம்பளம்" : "Salaries", amount: c.salaries_cr || 0, remaining: totalRev - (c.salaries_cr || 0), color: "bg-red-400" },
              { label: isTA ? "ஓய்வூதியம்" : "Pensions", amount: c.pensions_cr || 0, remaining: totalRev - (c.salaries_cr || 0) - (c.pensions_cr || 0), color: "bg-orange-400" },
              { label: isTA ? "கடன் வட்டி" : "Interest", amount: c.interest_cr || 0, remaining: totalRev - (c.salaries_cr || 0) - (c.pensions_cr || 0) - (c.interest_cr || 0), color: "bg-yellow-500" },
            ];
            if (c.subsidies_cr) {
              const prevRemaining = items[items.length - 1].remaining;
              items.push({ label: isTA ? "மானியங்கள்" : "Subsidies", amount: c.subsidies_cr, remaining: prevRemaining - c.subsidies_cr, color: "bg-pink-400" });
            }
            const discretionary = items[items.length - 1].remaining;

            return (
              <div className="bg-white rounded-2xl border border-gray-200 p-4 space-y-3">
                <p className="text-xs font-bold text-gray-500 uppercase tracking-wide">
                  {isTA ? "செலவு நீர்வீழ்ச்சி — எவ்வளவு மிஞ்சுகிறது?" : "Spending Waterfall — What's Left?"}
                  <span className="text-gray-400 normal-case font-normal ml-1">({latestBudget.fiscal_year})</span>
                </p>
                {items.map((item, i) => (
                  <div key={item.label}>
                    {i > 0 && (
                      <div className="flex items-center gap-2 py-1">
                        <span className="text-[10px] text-gray-400">{isTA ? "கழி" : "minus"}</span>
                        <span className="text-xs font-semibold text-gray-700">{item.label}</span>
                        <span className="text-xs font-bold text-red-600">-{fCr(item.amount)}</span>
                      </div>
                    )}
                    <div className="h-5 bg-gray-100 rounded-full overflow-hidden">
                      <div
                        className={`h-full rounded-full transition-all ${i === 0 ? "bg-blue-500" : "bg-emerald-400"}`}
                        style={{ width: `${(item.remaining / totalRev) * 100}%` }}
                      />
                    </div>
                    <p className="text-[10px] text-gray-500 mt-0.5">
                      {i === 0
                        ? `${isTA ? "தொடக்கம்" : "Start"}: ${fCr(totalRev)}`
                        : `${isTA ? "மீதம்" : "Remaining"}: ${fCr(item.remaining)} (${fPct((item.remaining / totalRev) * 100)})`}
                    </p>
                  </div>
                ))}
                <div className="bg-emerald-50 border border-emerald-200 rounded-xl p-3 text-center">
                  <p className="text-[10px] text-emerald-600 font-semibold mb-0.5">
                    {isTA ? "விருப்பச் செலவு — அரசின் உண்மையான தேர்வு" : "Discretionary Spending — The Government's Actual Choice"}
                  </p>
                  <p className="text-2xl font-black text-emerald-700">{fCr(discretionary)}</p>
                  <p className="text-xs text-emerald-600">{fPct((discretionary / totalRev) * 100)} {isTA ? "வருவாயில்" : "of revenue"}</p>
                </div>
              </div>
            );
          })()}

          {/* Department-wise spending */}
          <div className="bg-white rounded-2xl border border-gray-200 p-4 space-y-3">
            <p className="text-xs font-bold text-gray-500 uppercase tracking-wide">
              {isTA ? "துறைவாரி செலவு (2024-25)" : "Department-wise Spending (2024-25)"}
            </p>
            {dept.sectors.filter((s) => s.pct >= 1.5).map((s) => (
              <div key={s.sector} className="space-y-0.5">
                <div className="flex items-center justify-between">
                  <p className="text-xs font-semibold text-gray-800 truncate flex-1">{isTA ? s.ta : s.sector}</p>
                  <div className="flex items-center gap-2 flex-shrink-0">
                    <span className="text-xs font-bold text-gray-900">{fCr(s.amount_cr)}</span>
                    {s.national_avg_pct != null && (
                      <span className={`text-[8px] font-bold px-1 py-0.5 rounded ${s.pct >= s.national_avg_pct ? "bg-green-50 text-green-600" : "bg-red-50 text-red-600"}`}>
                        {s.pct >= s.national_avg_pct ? ">" : "<"} {isTA ? "தேசிய சராசரி" : "avg"}
                      </span>
                    )}
                  </div>
                </div>
                <div className="h-2 bg-gray-100 rounded-full overflow-hidden">
                  <div className="h-full bg-purple-400 rounded-full" style={{ width: `${(s.pct / 15) * 100}%` }} />
                </div>
              </div>
            ))}
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════════════════
            SECTION 4: 14-year trend
           ══════════════════════════════════════════════════════════════════ */}
        <section className="space-y-4">
          <div>
            <p className="text-[10px] font-bold text-emerald-600 uppercase tracking-wider mb-1">
              {isTA ? "பகுதி 4" : "SECTION 4"}
            </p>
            <h2 className="text-xl font-black text-gray-900">
              {isTA ? "14 ஆண்டு போக்கு" : "14-Year Trend"}
            </h2>
          </div>

          {/* Fiscal deficit theory */}
          <div className="bg-emerald-50 border border-emerald-200 rounded-2xl p-4 text-sm text-emerald-900 leading-relaxed">
            {isTA ? data.theory.fiscal_deficit_explained.description_ta : data.theory.fiscal_deficit_explained.description_en}
          </div>

          {/* Revenue vs Expenditure trend */}
          <div className="bg-white rounded-2xl border border-gray-200 p-4 space-y-3">
            <p className="text-xs font-bold text-gray-500 uppercase tracking-wide">
              {isTA ? "வருவாய் vs செலவு (₹ கோடி)" : "Revenue vs Expenditure (₹ Crore)"}
            </p>
            <div className="space-y-1">
              {budgets.map((b) => {
                const revCr = b.revenue?.total_revenue_receipts_cr || 0;
                const expCr = b.expenditure?.total_exp_cr || 0;
                const maxVal = Math.max(...budgets.map((x) => x.expenditure?.total_exp_cr || 0));
                return (
                  <div key={b.fiscal_year} className="flex items-center gap-2">
                    <span className="text-[9px] text-gray-400 w-12">{b.fiscal_year.slice(0, 4)}</span>
                    <div className="flex-1 relative h-4">
                      <div className="absolute inset-y-0 left-0 bg-blue-200 rounded-full" style={{ width: `${(revCr / maxVal) * 100}%` }} />
                      <div className="absolute inset-y-0 left-0 bg-red-300 rounded-full opacity-60" style={{ width: `${(expCr / maxVal) * 100}%`, height: "40%", top: "30%" }} />
                    </div>
                    <div className="text-[8px] text-gray-500 w-24 text-right">
                      <span className="text-blue-600">{(revCr / 100000).toFixed(1)}L</span>
                      {" / "}
                      <span className="text-red-500">{(expCr / 100000).toFixed(1)}L</span>
                    </div>
                  </div>
                );
              })}
            </div>
            <div className="flex items-center gap-4 text-[9px] text-gray-400">
              <span className="flex items-center gap-1"><span className="w-2 h-2 bg-blue-200 rounded" /> {isTA ? "வருவாய்" : "Revenue"}</span>
              <span className="flex items-center gap-1"><span className="w-2 h-2 bg-red-300 rounded" /> {isTA ? "செலவு" : "Expenditure"}</span>
            </div>
          </div>

          {/* Fiscal deficit trend */}
          <div className="bg-white rounded-2xl border border-gray-200 p-4 space-y-3">
            <p className="text-xs font-bold text-gray-500 uppercase tracking-wide">
              {isTA ? "நிதிப் பற்றாக்குறை போக்கு" : "Fiscal Deficit Trend"}
            </p>
            <div className="space-y-1.5">
              {budgets.filter((b) => b.fiscal?.fiscal_deficit_cr).map((b) => {
                const def = b.fiscal!.fiscal_deficit_cr!;
                const maxDef = Math.max(...budgets.filter((x) => x.fiscal?.fiscal_deficit_cr).map((x) => x.fiscal!.fiscal_deficit_cr!));
                return (
                  <div key={b.fiscal_year} className="flex items-center gap-2">
                    <span className="text-[9px] text-gray-400 w-12">{b.fiscal_year.slice(0, 4)}</span>
                    <div className="flex-1 h-3 bg-gray-50 rounded-full overflow-hidden">
                      <div className="h-full bg-red-400 rounded-full" style={{ width: `${(def / maxDef) * 100}%` }} />
                    </div>
                    <span className="text-[10px] font-bold text-gray-700 w-16 text-right">{fCr(def)}</span>
                  </div>
                );
              })}
            </div>
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════════════════
            SECTION 5: Your MLA's Fund
           ══════════════════════════════════════════════════════════════════ */}
        <section className="space-y-4">
          <div>
            <p className="text-[10px] font-bold text-rose-600 uppercase tracking-wider mb-1">
              {isTA ? "பகுதி 5" : "SECTION 5"}
            </p>
            <h2 className="text-xl font-black text-gray-900">
              {isTA ? "உங்கள் சட்டமன்ற உறுப்பினரின் நிதி" : "Your MLA's Fund"}
            </h2>
            <p className="text-sm text-gray-500 mt-1">
              {isTA
                ? "ஒவ்வொரு சட்டமன்ற உறுப்பினருக்கும் ஆண்டுதோறும் அவர்களின் தொகுதியில் செலவிட ₹3 கோடி ஒதுக்கப்படுகிறது."
                : "Every MLA gets ₹3 crore per year to spend in their constituency under the MLACDS scheme."}
            </p>
          </div>

          {data.mlacds.length > 0 && (
            <div className="bg-white rounded-2xl border border-gray-200 p-4 space-y-3">
              <div className="grid grid-cols-3 gap-3 text-center">
                <div className="bg-rose-50 rounded-xl p-3">
                  <p className="text-lg font-black text-gray-900">{fCr(data.mlacds[data.mlacds.length - 1]?.per_constituency_allocation_cr || 3)}</p>
                  <p className="text-[9px] text-gray-500">{isTA ? "தொகுதிக்கு" : "Per Constituency"}</p>
                </div>
                <div className="bg-rose-50 rounded-xl p-3">
                  <p className="text-lg font-black text-gray-900">234</p>
                  <p className="text-[9px] text-gray-500">{isTA ? "தொகுதிகள்" : "Constituencies"}</p>
                </div>
                <div className="bg-rose-50 rounded-xl p-3">
                  <p className="text-lg font-black text-gray-900">{fCr(data.mlacds[data.mlacds.length - 1]?.state_total_allocation_cr || 702)}</p>
                  <p className="text-[9px] text-gray-500">{isTA ? "மொத்தம்" : "State Total"}</p>
                </div>
              </div>

              {/* Allocation trend */}
              <p className="text-xs font-bold text-gray-500 uppercase tracking-wide">
                {isTA ? "ஒதுக்கீடு போக்கு (15 ஆண்டுகள்)" : "Allocation Trend (15 Years)"}
              </p>
              <div className="space-y-1">
                {data.mlacds.map((m) => (
                  <div key={m.fiscal_year} className="flex items-center gap-2">
                    <span className="text-[9px] text-gray-400 w-14">{m.fiscal_year}</span>
                    <div className="flex-1 h-3 bg-gray-50 rounded-full overflow-hidden">
                      <div className="h-full bg-rose-400 rounded-full" style={{ width: `${(m.per_constituency_allocation_cr / 3.5) * 100}%` }} />
                    </div>
                    <span className="text-[10px] font-bold text-gray-700 w-12 text-right">{fCr(m.per_constituency_allocation_cr)}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </section>

        {/* ══════════════════════════════════════════════════════════════════
            SECTION 6: Per Person Math
           ══════════════════════════════════════════════════════════════════ */}
        <section className="space-y-4">
          <div>
            <p className="text-[10px] font-bold text-indigo-600 uppercase tracking-wider mb-1">
              {isTA ? "பகுதி 6" : "SECTION 6"}
            </p>
            <h2 className="text-xl font-black text-gray-900">
              {isTA ? "நபர் ஒன்றுக்கான கணக்கு" : "The Per-Person Math"}
            </h2>
          </div>

          <div className="bg-indigo-50 border border-indigo-200 rounded-2xl p-5 space-y-4 text-center">
            <p className="text-sm text-indigo-900">
              {isTA
                ? `தமிழ்நாடு அரசு ${data.population_cr} கோடி மக்களுக்காக ஆண்டுக்கு ${fCr(totalExpCr)} செலவிடுகிறது.`
                : `The Tamil Nadu government spends ${fCr(totalExpCr)} per year for ${data.population_cr} crore people.`}
            </p>

            <div>
              <p className="text-4xl font-black text-indigo-700">
                {isTA ? "₹" : "₹"}{Math.round(perPersonPerDay)}
              </p>
              <p className="text-xs text-indigo-600 font-semibold">
                {isTA ? "நபர் ஒன்றுக்கு, நாள் ஒன்றுக்கு" : "per person, per day"}
              </p>
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div className="bg-white rounded-xl p-3">
                <p className="text-lg font-black text-red-600">{isTA ? "₹" : "₹"}{Math.round(committedPerDay)}</p>
                <p className="text-[9px] text-gray-500">{isTA ? "கட்டாயச் செலவு" : "Committed (locked)"}</p>
                <p className="text-[8px] text-gray-400">{isTA ? "சம்பளம் + ஓய்வூதியம் + வட்டி" : "Salary + Pension + Interest"}</p>
              </div>
              <div className="bg-white rounded-xl p-3">
                <p className="text-lg font-black text-emerald-600">{isTA ? "₹" : "₹"}{Math.round(discretionaryPerDay)}</p>
                <p className="text-[9px] text-gray-500">{isTA ? "விருப்பச் செலவு" : "Discretionary (flexible)"}</p>
                <p className="text-[8px] text-gray-400">{isTA ? "திட்டங்கள் + உள்கட்டமைப்பு" : "Schemes + Infrastructure"}</p>
              </div>
            </div>
          </div>

          {/* GSDP context */}
          <div className="bg-white rounded-2xl border border-gray-200 p-4">
            <div className="grid grid-cols-2 gap-4 text-center">
              <div>
                <p className="text-xl font-black text-gray-900">{isTA ? "₹" : "₹"}{data.per_capita_income_lakh}L</p>
                <p className="text-[10px] text-gray-500">{isTA ? "TN தனிநபர் வருமானம்" : "TN Per Capita Income"}</p>
              </div>
              <div>
                <p className="text-xl font-black text-gray-400">{isTA ? "₹" : "₹"}{data.national_avg_per_capita_lakh}L</p>
                <p className="text-[10px] text-gray-500">{isTA ? "தேசிய சராசரி" : "National Average"}</p>
              </div>
            </div>
            <p className="text-[10px] text-center text-gray-400 mt-2">
              {isTA
                ? `TN-ன் தனிநபர் வருமானம் தேசிய சராசரியை விட ${(data.per_capita_income_lakh / data.national_avg_per_capita_lakh).toFixed(1)}x அதிகம்`
                : `TN's per capita income is ${(data.per_capita_income_lakh / data.national_avg_per_capita_lakh).toFixed(1)}x the national average`}
            </p>
          </div>
        </section>

        {/* Source */}
        <p className="text-center text-[10px] text-gray-400 pb-4 pt-2">
          {isTA ? "ஆதாரங்கள்: " : "Sources: "}
          <a href="https://prsindia.org/budgets/states/tamil-nadu-budget-analysis-2024-25" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">PRS India</a>
          {" · "}
          <a href="https://cag.gov.in" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">CAG</a>
          {" · "}
          <a href="https://fincomindia.nic.in" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">Finance Commission of India</a>
        </p>
      </div>
    </main>
  );
}
