"use client";

import { useState, useEffect } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { apiGet } from "@/lib/api-client";
import { InfoTip, LabelWithTip } from "@/components/state/InfoTip";

// ─── Types ────────────────────────────────────────────────────────────────────

interface GenderCell {
  male?: number | null;
  female?: number | null;
  person?: number | null;
}

interface AreaCell {
  rural?: GenderCell;
  urban?: GenderCell;
  total?: GenderCell;
}

interface PLFSSnapshot {
  period: string;
  lfpr?: Record<string, AreaCell>;
  wpr?: Record<string, AreaCell>;
  ur?: Record<string, AreaCell>;
  gaps_vs_all_india?: Record<string, AreaCell>;
}

interface RuralUrbanTotal {
  rural?: number | null;
  urban?: number | null;
  total?: number | null;
}

interface IMRCell {
  male?: number | null;
  female?: number | null;
  total?: number | null;
}

interface SRSSnapshot {
  period: string;
  cbr?: RuralUrbanTotal;
  cdr?: RuralUrbanTotal;
  tfr?: RuralUrbanTotal;
  imr?: { rural?: IMRCell; urban?: IMRCell; total?: IMRCell };
  mmr_2018_20?: { mmr?: number; ci_low?: number; ci_high?: number };
  trend?: {
    years?: number[];
    cbr_total?: number[];
    cdr_total?: number[];
    imr_total?: number[];
    tfr_total?: number[];
  };
}

interface HCESSnapshot {
  period: string;
  mpce_with_free_items?: { rural?: number; urban?: number };
  mpce_without_free_items?: { rural?: number; urban?: number };
  welfare_uplift?: { rural_uplift_pct?: number; urban_uplift_pct?: number; rural_uplift_rs?: number; urban_uplift_rs?: number };
  gap_vs_all_india_with_free?: { rural?: number; urban?: number };
}

interface EnrollmentCell {
  male?: number | null;
  female?: number | null;
  total?: number | null;
}

interface AISHESnapshot {
  period: string;
  ger?: { male?: number; female?: number; total?: number; gpi?: number };
  universities?: { total?: number; [k: string]: number | undefined };
  colleges?: { total?: number; government?: number; private_total?: number; [k: string]: number | undefined };
  enrollment?: { total_approx?: number; ug?: number; pg?: number; phd?: number; [k: string]: number | undefined };
  ger_vs_all_india?: number;
}

interface SDGSnapshot {
  period: string;
  composite?: number;
  rank?: number;
  goals?: Record<string, number | null>;
  gaps_from_national_best?: Record<string, number | null>;
}

interface FuelItem {
  price?: number | null;
  unit?: string;
}

interface CoLIndiaSnapshot {
  period: string;
  fuel?: Record<string, FuelItem>;
}

interface DairyItem {
  price?: number | null;
  unit?: string;
}

interface CoLTNSnapshot {
  period: string;
  food_dairy?: Record<string, DairyItem>;
}

interface UDISESnapshot {
  period: string;
  ger?: { primary?: number; upper_primary?: number; elementary?: number; secondary?: number; higher_secondary?: number };
  dropout_rate?: { primary?: number; upper_primary?: number; secondary?: number };
  ptr?: { primary?: number; upper_primary?: number; secondary?: number; higher_secondary?: number };
  schools_total?: number;
  schools_with_electricity_pct?: number | null;
  schools_with_internet_pct?: number | null;
}

interface NCRBSnapshot {
  period: string;
  total_ipc_crimes?: number;
  crimes_against_women?: number;
  crimes_against_children?: number;
  crimes_against_sc?: number;
  crimes_against_st?: number;
}

interface ASISnapshot {
  period: string;
  factories?: number;
  fixed_capital_cr?: number;
  total_output_cr?: number;
  total_input_cr?: number;
  gva_cr?: number;
  nva_cr?: number;
}

interface CoLSnapshot {
  period: string;
  fuel?: Record<string, FuelItem>;
  food_dairy?: Record<string, DairyItem>;
}

interface StateBudget {
  fiscal_year: string;
  state_code: string;
  state_name: string;
  source?: string;
  source_url?: string;
  revenue: {
    own_tax_revenue_cr?: number | null;
    central_devolution_cr?: number | null;
    grants_in_aid_cr?: number | null;
    total_revenue_receipts_cr?: number | null;
  };
  expenditure: {
    revenue_expenditure_cr?: number | null;
    capital_expenditure_cr?: number | null;
    total_exp_cr?: number | null;
  };
  fiscal: {
    fiscal_deficit_cr?: number | null;
    revenue_deficit_cr?: number | null;
    primary_deficit_cr?: number | null;
  };
  committed: {
    salaries_cr?: number | null;
    pensions_cr?: number | null;
    interest_cr?: number | null;
    subsidies_cr?: number | null;
    discretionary_cr?: number | null;
  };
}

interface RBISnapshot {
  period: string;
  gfd_cr?: number | null;
  rev_deficit_cr?: number | null;
  debt_to_gsdp_pct?: number | null;
  interest_payments_cr?: number | null;
  dev_expenditure_cr?: number | null;
}

interface AllIndiaRef {
  plfs?: PLFSSnapshot | null;
  srs?: SRSSnapshot | null;
  hces?: HCESSnapshot | null;
  udise?: UDISESnapshot | null;
  asi?: ASISnapshot | null;
  sdg_index?: SDGSnapshot | null;
  ncrb?: NCRBSnapshot | null;
  aishe?: AISHESnapshot | null;
}

interface StateReport {
  state: string;
  plfs?: PLFSSnapshot | null;
  srs?: SRSSnapshot | null;
  hces?: HCESSnapshot | null;
  aishe?: AISHESnapshot | null;
  sdg_index?: SDGSnapshot | null;
  udise?: UDISESnapshot | null;
  ncrb?: NCRBSnapshot | null;
  asi?: ASISnapshot | null;
  cost_of_living?: CoLSnapshot | null;
  state_budget?: StateBudget | null;
  rbi_state_finances?: RBISnapshot | null;
  all_india?: AllIndiaRef;
}

// ─── State config ─────────────────────────────────────────────────────────────

const STATES = [
  { slug: "tamil_nadu",     name: "Tamil Nadu",     emoji: "🏛️" },
  { slug: "kerala",         name: "Kerala",          emoji: "🌴" },
  { slug: "karnataka",      name: "Karnataka",       emoji: "🦁" },
  { slug: "andhra_pradesh", name: "Andhra Pradesh",  emoji: "🌾" },
  { slug: "telangana",      name: "Telangana",       emoji: "🔵" },
];

const SDG_GOAL_NAMES: Record<string, string> = {
  "1": "No Poverty",       "2": "Zero Hunger",       "3": "Good Health",
  "4": "Quality Education","5": "Gender Equality",   "6": "Clean Water",
  "7": "Clean Energy",     "8": "Decent Work",       "9": "Innovation",
  "10": "Reduced Inequality","11": "Sustainable Cities","12": "Responsible Consumption",
  "13": "Climate Action",  "14": "Life Below Water", "15": "Life on Land",
  "16": "Peace & Justice",
};

// ─── Shared helpers ───────────────────────────────────────────────────────────

function f1(v: number | null | undefined): string {
  if (v == null) return "—";
  if (typeof v !== "number") return "—";
  return v.toFixed(1);
}

function fInr(v: number | null | undefined): string {
  if (v == null) return "—";
  return "₹" + v.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function SourceLink({ name, url, period }: { name: string; url: string; period?: string }) {
  return (
    <p className="text-[10px] text-gray-400 mb-3">
      {period && <>As of {period} · </>}
      Source:{" "}
      <a href={url} target="_blank" rel="noopener noreferrer"
         className="underline underline-offset-2 hover:text-gray-600">
        {name}
      </a>
    </p>
  );
}

function MiniBar({
  value,
  max = 100,
  color = "bg-blue-500",
}: {
  value?: number | null;
  max?: number;
  color?: string;
}) {
  if (value == null) return null;
  return (
    <div className="w-full bg-gray-100 rounded-full h-1.5 mt-1">
      <div
        className={`h-1.5 rounded-full ${color}`}
        style={{ width: `${Math.min(100, (value / max) * 100)}%` }}
      />
    </div>
  );
}

function EmptySection({ msg }: { msg: string }) {
  return (
    <div className="bg-gray-50 rounded-2xl border border-dashed border-gray-300 p-8 text-center">
      <p className="text-sm text-gray-400">{msg}</p>
    </div>
  );
}

// ─── Labour (PLFS) ────────────────────────────────────────────────────────────

interface PLFSHistory {
  snapshots?: Record<string, PLFSSnapshot>;
}

function LabourSection({
  plfs,
  aiPlfs,
  slug,
}: {
  plfs?: PLFSSnapshot | null;
  aiPlfs?: PLFSSnapshot | null;
  slug: string;
}) {
  // Fetch full time-series so we can fall back to a snapshot that actually has
  // the 15-29 youth breakdown. The latest snapshot is often the MoSPI
  // Quarterly Bulletin (15+ headline only); the Annual Report (with youth
  // detail) lags by 9-12 months.
  const [youth, setYouth] = useState<{ period: string; ur?: Record<string, AreaCell> } | null>(null);
  const [youthAI, setYouthAI] = useState<{ period: string; ur?: Record<string, AreaCell> } | null>(null);

  useEffect(() => {
    if (!slug) return;
    let cancelled = false;
    const findLatestWithYouth = (history: PLFSHistory | null) => {
      const snaps = history?.snapshots ?? {};
      const periods = Object.keys(snaps).sort().reverse();
      for (const p of periods) {
        if (snaps[p]?.ur?.["15-29"]) return { period: p, ur: snaps[p].ur };
      }
      return null;
    };
    Promise.all([
      apiGet<PLFSHistory>(`/api/kg/plfs/${slug}`).catch(() => null),
      apiGet<PLFSHistory>(`/api/kg/plfs/all_india`).catch(() => null),
    ]).then(([stateHist, aiHist]) => {
      if (cancelled) return;
      setYouth(findLatestWithYouth(stateHist));
      setYouthAI(findLatestWithYouth(aiHist));
    });
    return () => {
      cancelled = true;
    };
  }, [slug]);

  if (!plfs) return <EmptySection msg="No PLFS data available" />;

  const areas = ["rural", "urban", "total"] as const;
  const isQuarterly = /^\d{4}$/.test(plfs.period); // 4-digit = quarterly bulletin; "YYYY-YY" = annual report

  return (
    <div className="space-y-4">
      {/* 15+ headline */}
      <div className="bg-white rounded-2xl border border-gray-200 p-4">
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-1">
          Labour Force — Age 15+ · {plfs.period}
        </p>
        <SourceLink name="Periodic Labour Force Survey (MoSFA)" url="https://www.mospi.gov.in/publication/plfs-annual-report-2023-24" period={plfs.period} />
        <div className="grid grid-cols-3 gap-2 text-center mb-4">
          {(["lfpr", "wpr", "ur"] as const).map((metric) => {
            const val = plfs[metric]?.["15+"]?.total?.person;
            const ai = aiPlfs?.[metric]?.["15+"]?.total?.person;
            const labels = { lfpr: "LFPR", wpr: "WPR", ur: "Unemployment" } as const;
            const terms  = { lfpr: "LFPR", wpr: "WPR", ur: "Unemployment" } as const;
            return (
              <div key={metric} className="bg-gray-50 rounded-xl p-2">
                <p className="text-[10px] text-gray-500 font-semibold inline-flex items-center justify-center w-full">
                  {labels[metric]}
                  <InfoTip term={terms[metric]} />
                </p>
                <p className="text-xl font-black text-gray-900">
                  {f1(val)}<span className="text-xs font-normal text-gray-400">%</span>
                </p>
                {ai != null && (
                  <p className="text-[9px] text-gray-400">India: {f1(ai)}%</p>
                )}
              </div>
            );
          })}
        </div>

        {/* LFPR by area + gender */}
        <div className="space-y-3">
          {areas.map((area) => {
            const male   = plfs.lfpr?.["15+"]?.[area]?.male;
            const female = plfs.lfpr?.["15+"]?.[area]?.female;
            const total  = plfs.lfpr?.["15+"]?.[area]?.person;
            return (
              <div key={area}>
                <p className="text-[10px] font-bold text-gray-400 uppercase mb-1.5 inline-flex items-center">
                  {area} <LabelWithTip label="LFPR" term="LFPR" className="ml-1" /> (15+)
                </p>
                <div className="grid grid-cols-3 gap-2">
                  {[
                    { label: "Male",   val: male,   color: "text-blue-700",  bg: "bg-blue-50" },
                    { label: "Female", val: female, color: "text-rose-700",  bg: "bg-rose-50" },
                    { label: "Total",  val: total,  color: "text-gray-900",  bg: "bg-gray-50" },
                  ].map(({ label, val: v, color, bg }) => (
                    <div key={label} className={`${bg} rounded-lg px-2 py-1.5 text-center`}>
                      <p className="text-[9px] text-gray-500 font-semibold">{label}</p>
                      <p className={`text-sm font-black ${color}`}>{f1(v)}%</p>
                    </div>
                  ))}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Youth 15-29 unemployment */}
      <div className="bg-white rounded-2xl border border-gray-200 p-4">
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-1 inline-flex items-center">
          Youth Unemployment Rate — Age 15–29
          <InfoTip term="Youth Unemployment" />
        </p>

        {/* Prefer headline period if it has youth data; otherwise fall back to the
            latest Annual Report snapshot that does. */}
        {(() => {
          const hasHeadlineYouth = plfs.ur?.["15-29"] != null;
          const sourceUr  = hasHeadlineYouth ? plfs.ur?.["15-29"] : youth?.ur?.["15-29"];
          const sourceAI  = hasHeadlineYouth
            ? aiPlfs?.ur?.["15-29"]
            : youthAI?.ur?.["15-29"];
          const sourcePeriod = hasHeadlineYouth ? plfs.period : youth?.period;

          if (!sourceUr) {
            return (
              <div className="bg-amber-50 border border-amber-200 rounded-xl p-3 text-[11px] text-amber-800 leading-snug">
                Youth (15–29) breakdown isn&apos;t published yet for any period we have on file.
                MoSPI releases this in the PLFS <span className="font-semibold">Annual Report</span>,
                which typically lags the Quarterly Bulletin by 9–12 months.
              </div>
            );
          }

          return (
            <>
              <p className="text-[10px] text-gray-500 mb-3 leading-snug">
                {isQuarterly && !hasHeadlineYouth ? (
                  <>
                    Source: <span className="font-semibold">PLFS Annual Report {sourcePeriod}</span>.
                    The {plfs.period} Quarterly Bulletin only publishes the 15+ headline — MoSPI releases
                    the youth breakdown separately, 9–12 months later.
                  </>
                ) : (
                  <>As of {sourcePeriod}.</>
                )}
              </p>
              <div className="grid grid-cols-3 gap-2">
                {areas.map((area) => {
                  const val = sourceUr?.[area]?.person;
                  const ai  = sourceAI?.[area]?.person;
                  const gap = val != null && ai != null ? val - ai : null;
                  return (
                    <div key={area} className="bg-gray-50 rounded-xl p-3 text-center">
                      <p className="text-[10px] text-gray-500 font-semibold capitalize mb-1">{area}</p>
                      <p className="text-lg font-black text-gray-900">
                        {f1(val)}<span className="text-xs font-normal text-gray-400">%</span>
                      </p>
                      {ai != null && (
                        <p className="text-[9px] text-gray-400">India: {f1(ai)}%</p>
                      )}
                      {gap != null && (
                        <p className={`text-[10px] font-bold ${gap > 0 ? "text-red-500" : "text-emerald-600"}`}>
                          {gap > 0 ? "+" : ""}{gap.toFixed(1)} pts vs India
                        </p>
                      )}
                    </div>
                  );
                })}
              </div>
              <p className="text-[10px] text-gray-400 mt-3">
                Lower is better. <span className="text-emerald-600 font-semibold">Green</span> = TN is below the national rate;{" "}
                <span className="text-red-500 font-semibold">red</span> = worse than India average.
              </p>
            </>
          );
        })()}
      </div>
    </div>
  );
}

// ─── Health (SRS) ─────────────────────────────────────────────────────────────

function HealthSection({
  srs,
  aiSrs,
}: {
  srs?: SRSSnapshot | null;
  aiSrs?: SRSSnapshot | null;
}) {
  if (!srs) return <EmptySection msg="No SRS data available" />;

  const rows: {
    label: string;
    term: string;
    val?: number | null;
    ai?: number | null;
    unit: string;
    lowerBetter: boolean;
  }[] = [
    { label: "Infant Mortality Rate (IMR)",    term: "IMR", val: srs.imr?.total?.total, ai: aiSrs?.imr?.total?.total, unit: "per 1,000 live births",    lowerBetter: true  },
    { label: "Maternal Mortality Ratio (MMR)", term: "MMR", val: srs.mmr_2018_20?.mmr,  ai: aiSrs?.mmr_2018_20?.mmr,  unit: "per 100,000 live births",  lowerBetter: true  },
    { label: "Total Fertility Rate (TFR)",     term: "TFR", val: srs.tfr?.total,        ai: aiSrs?.tfr?.total,        unit: "children per woman",       lowerBetter: true  },
    { label: "Crude Birth Rate (CBR)",         term: "CBR", val: srs.cbr?.total,        ai: aiSrs?.cbr?.total,        unit: "per 1,000 population",     lowerBetter: false },
    { label: "Crude Death Rate (CDR)",         term: "CDR", val: srs.cdr?.total,        ai: aiSrs?.cdr?.total,        unit: "per 1,000 population",     lowerBetter: true  },
  ];

  return (
    <div className="space-y-4">
      <div className="bg-white rounded-2xl border border-gray-200 p-4">
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-1 inline-flex items-center">
          Vital Statistics · <LabelWithTip label="SRS" term="SRS" className="mx-1" /> {srs.period}
        </p>
        <SourceLink name="SRS Statistical Report 2023 (RGI)" url="https://censusindia.gov.in/nada/index.php/catalog/46172/download/50420/SRS_STAT_2023.pdf" />
        <div className="space-y-0">
          {rows.map(({ label, term, val, ai, unit, lowerBetter }) => {
            const gap = val != null && ai != null ? val - ai : null;
            const gapGood = gap != null && (lowerBetter ? gap < 0 : gap > 0);
            return (
              <div
                key={label}
                className="flex items-center justify-between py-2.5 border-b border-gray-50 last:border-0 gap-2"
              >
                <span className="text-xs text-gray-700 inline-flex items-center">
                  {label}
                  <InfoTip term={term} />
                </span>
                <div className="flex items-center gap-2 text-right flex-shrink-0">
                  {gap != null && (
                    <span className={`text-[10px] font-bold ${gapGood ? "text-emerald-600" : "text-red-500"}`}>
                      {gap > 0 ? "+" : ""}{gap.toFixed(1)}
                    </span>
                  )}
                  <span className="text-xs font-black text-gray-900 whitespace-nowrap">
                    {f1(val)}<span className="text-[10px] text-gray-400 font-normal">{unit && ` ${unit}`}</span>
                  </span>
                  {ai != null && (
                    <span className="text-[10px] text-gray-400 whitespace-nowrap">India: {f1(ai)}</span>
                  )}
                </div>
              </div>
            );
          })}
        </div>
        <p className="text-[10px] text-gray-400 mt-2 leading-snug">
          The leftmost number is the <span className="font-semibold">gap vs India average</span>
          {" "}(green = better for this metric; context-aware). Middle = Tamil Nadu&apos;s value. Right = India&apos;s value.
          Tap the <span className="inline-block w-3.5 h-3.5 rounded-full border border-gray-300 text-[8px] text-gray-500 font-bold leading-none text-center align-middle">i</span> for a definition.
        </p>
      </div>

      {/* Rural vs Urban */}
      <div className="bg-white rounded-2xl border border-gray-200 p-4">
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-3">
          Rural vs Urban
        </p>
        <div className="grid grid-cols-2 gap-3">
          {[
            { label: "IMR",         term: "IMR", rural: srs.imr?.rural?.total,  urban: srs.imr?.urban?.total },
            { label: "TFR",         term: "TFR", rural: srs.tfr?.rural,         urban: srs.tfr?.urban },
            { label: "Birth Rate",  term: "CBR", rural: srs.cbr?.rural,         urban: srs.cbr?.urban },
            { label: "Death Rate",  term: "CDR", rural: srs.cdr?.rural,         urban: srs.cdr?.urban },
          ].map(({ label, term, rural, urban }) => (
            <div key={label} className="bg-gray-50 rounded-xl p-3">
              <p className="text-[10px] font-bold text-gray-500 mb-2 inline-flex items-center">
                {label}<InfoTip term={term} />
              </p>
              <div className="flex justify-between">
                <div>
                  <p className="text-[9px] text-green-600 font-semibold">Rural</p>
                  <p className="text-sm font-black text-gray-900">{f1(rural)}</p>
                </div>
                <div className="text-right">
                  <p className="text-[9px] text-blue-600 font-semibold">Urban</p>
                  <p className="text-sm font-black text-gray-900">{f1(urban)}</p>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

// ─── Spending (HCES) ──────────────────────────────────────────────────────────

function SpendingSection({
  hces,
  aiHces,
}: {
  hces?: HCESSnapshot | null;
  aiHces?: HCESSnapshot | null;
}) {
  if (!hces) return <EmptySection msg="No HCES data available" />;

  return (
    <div className="space-y-4">
      <div className="bg-white rounded-2xl border border-gray-200 p-4">
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-1 inline-flex items-center flex-wrap">
          <LabelWithTip label="MPCE" term="MPCE" />
          <span className="ml-1">— Monthly Per Capita Consumption · <LabelWithTip label="HCES" term="HCES" className="ml-0.5" /> {hces.period}</span>
        </p>
        <SourceLink name="Household Consumption Expenditure Survey (MoSFA)" url="https://www.mospi.gov.in/publication/household-consumption-expenditure-survey-2023-24" period={hces.period} />
        <p className="text-[10px] text-gray-500 mb-1 leading-snug">
          Average <span className="font-semibold">₹ spent per person, per month</span> — calculated across every individual in sampled households (adults, children, elderly) and averaged state-wide.
        </p>
        <p className="text-[10px] text-gray-400 mb-4">
          &ldquo;With free goods&rdquo; includes govt-supplied items (rice, pulses, laptops, etc.) valued at market price.
        </p>

        {/* MPCE grid */}
        <div className="grid grid-cols-2 gap-3 mb-4">
          {[
            { label: "Rural\n(with free goods)",    val: hces.mpce_with_free_items?.rural,   ai: aiHces?.mpce_with_free_items?.rural },
            { label: "Urban\n(with free goods)",    val: hces.mpce_with_free_items?.urban,   ai: aiHces?.mpce_with_free_items?.urban },
            { label: "Rural\n(without free goods)", val: hces.mpce_without_free_items?.rural, ai: aiHces?.mpce_without_free_items?.rural },
            { label: "Urban\n(without free goods)", val: hces.mpce_without_free_items?.urban, ai: aiHces?.mpce_without_free_items?.urban },
          ].map(({ label, val, ai }) => {
            const gap = val != null && ai != null ? val - ai : null;
            return (
              <div key={label} className="bg-gray-50 rounded-xl p-3">
                <p className="text-[10px] text-gray-500 font-semibold leading-tight whitespace-pre-line mb-1">
                  {label}
                </p>
                <p className="text-sm font-black text-gray-900">{fInr(val)}</p>
                {ai != null && (
                  <p className="text-[9px] text-gray-400">India: {fInr(ai)}</p>
                )}
                {gap != null && (
                  <p className={`text-[10px] font-bold mt-0.5 ${gap > 0 ? "text-emerald-600" : "text-red-500"}`}>
                    {gap > 0 ? "+" : ""}{fInr(gap)} vs India
                  </p>
                )}
              </div>
            );
          })}
        </div>

        {/* Welfare uplift */}
        {(hces.welfare_uplift?.rural_uplift_pct != null || hces.welfare_uplift?.urban_uplift_pct != null) && (
          <div className="border-t border-gray-100 pt-3">
            <p className="text-[10px] font-bold text-gray-500 mb-2 inline-flex items-center">
              Welfare Uplift from Free Goods
              <InfoTip term="Welfare Uplift" />
            </p>
            <div className="flex gap-6">
              <div>
                <p className="text-[9px] text-green-600 font-semibold">Rural</p>
                <p className="text-base font-black text-gray-900">
                  +{f1(hces.welfare_uplift?.rural_uplift_pct)}%
                </p>
              </div>
              <div>
                <p className="text-[9px] text-blue-600 font-semibold">Urban</p>
                <p className="text-base font-black text-gray-900">
                  +{f1(hces.welfare_uplift?.urban_uplift_pct)}%
                </p>
              </div>
            </div>
            <p className="text-[9px] text-gray-400 mt-1">
              % increase in MPCE when free goods are included.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}

// ─── Education (AISHE) ────────────────────────────────────────────────────────

function EducationSection({ aishe, aiAishe }: { aishe?: AISHESnapshot | null; aiAishe?: AISHESnapshot | null }) {
  if (!aishe) return <EmptySection msg="No AISHE data available" />;

  return (
    <div className="space-y-4">
      <div className="bg-white rounded-2xl border border-gray-200 p-4">
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-1 inline-flex items-center">
          Higher Education — <LabelWithTip label="AISHE" term="AISHE" className="mx-1" /> {aishe.period}
        </p>
        <SourceLink name="AISHE 2021-22 Report (MoE)" url="https://aishe.gov.in/aishe/viewDocument.action?documentId=352" period={aishe.period} />

        {/* GER */}
        {aishe.ger && (
          <div className="mb-4">
            <p className="text-[10px] text-gray-500 font-semibold mb-2 inline-flex items-center">
              Gross Enrolment Ratio (<LabelWithTip label="GER" term="GER" />) — 18–23 age group
            </p>
            <div className="grid grid-cols-3 gap-2 text-center">
              {(["male", "female", "total"] as const).map((g) => {
                const ai = aiAishe?.ger?.[g];
                return (
                  <div key={g} className="bg-gray-50 rounded-xl p-2">
                    <p className="text-[9px] text-gray-500 capitalize font-semibold">
                      {g === "total" ? "Overall" : g}
                    </p>
                    <p className="text-xl font-black text-gray-900">
                      {f1(aishe.ger?.[g])}
                      <span className="text-xs font-normal text-gray-400">%</span>
                    </p>
                    {ai != null && <p className="text-[9px] text-gray-400">India: {f1(ai)}%</p>}
                    <MiniBar
                      value={aishe.ger?.[g]}
                      max={60}
                      color={g === "male" ? "bg-blue-400" : g === "female" ? "bg-rose-400" : "bg-gray-400"}
                    />
                  </div>
                );
              })}
            </div>
            {aishe.ger?.gpi != null && (
              <p className="text-[10px] text-gray-500 mt-2 text-center inline-flex items-center justify-center w-full">
                <LabelWithTip label="Gender Parity Index" term="GPI" className="mr-1" />:{" "}
                <span className={`font-bold ${aishe.ger.gpi >= 1 ? "text-emerald-600" : "text-amber-600"}`}>
                  {aishe.ger.gpi.toFixed(2)}
                </span>
                <span className="text-gray-400"> — {aishe.ger.gpi >= 1 ? "more women than men enrolled" : "below parity"}</span>
              </p>
            )}
          </div>
        )}

        {/* Institutions */}
        <div className="grid grid-cols-3 gap-2 text-center mb-4">
          {[
            { label: "Universities",  val: aishe.universities?.total },
            { label: "Colleges",      val: aishe.colleges?.total },
            { label: "PhD Students",  val: aishe.enrollment?.phd },
          ].map(({ label, val }) => (
            <div key={label} className="bg-gray-50 rounded-xl p-2">
              <p className="text-[10px] text-gray-500 font-semibold">{label}</p>
              <p className="text-sm font-black text-gray-900">
                {val != null ? val.toLocaleString("en-IN") : "—"}
              </p>
            </div>
          ))}
        </div>

        {/* Enrollment */}
        {aishe.enrollment && (
          <div className="border-t border-gray-100 pt-3">
            <p className="text-[10px] font-bold text-gray-500 mb-2">Enrollment Breakdown</p>
            <div className="grid grid-cols-4 gap-2 text-center">
              {[
                { label: "Total", term: null,  val: aishe.enrollment.total_approx },
                { label: "UG",    term: "UG",  val: aishe.enrollment.ug },
                { label: "PG",    term: "PG",  val: aishe.enrollment.pg },
                { label: "PhD",   term: "PhD", val: aishe.enrollment.phd },
              ].map(({ label, term, val }) => (
                <div key={label}>
                  <p className="text-[9px] text-gray-400 font-semibold inline-flex items-center justify-center w-full">
                    {label}{term && <InfoTip term={term} />}
                  </p>
                  <p className="text-sm font-black text-gray-900">
                    {val != null ? (val >= 100000 ? (val / 100000).toFixed(1) + "L" : val.toLocaleString("en-IN")) : "—"}
                  </p>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// ─── SDG ──────────────────────────────────────────────────────────────────────

function SDGSection({ sdg }: { sdg?: SDGSnapshot | null }) {
  if (!sdg) return <EmptySection msg="No SDG data available" />;

  const goalEntries = Object.entries(sdg.goals ?? {})
    .filter(([, v]) => v != null)
    .sort((a, b) => Number(a[0]) - Number(b[0]));

  const weakest = [...goalEntries]
    .sort((a, b) => (a[1] ?? 100) - (b[1] ?? 100))
    .slice(0, 5);

  return (
    <div className="space-y-4">
      {/* Hero */}
      <div className="bg-gradient-to-br from-blue-900 to-blue-700 rounded-2xl p-5 text-white">
        <p className="text-[10px] font-bold uppercase tracking-widest opacity-70 inline-flex items-center flex-wrap">
          <a href="https://sdgindiaindex.niti.gov.in/#/ranking" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:opacity-100">
            NITI Aayog <LabelWithTip label="SDG" term="SDG" /> India Index 2023-24
          </a>{" "}· {sdg.period}
        </p>
        <div className="flex items-end gap-6 mt-2">
          <div>
            <p className="text-5xl font-black">{sdg.composite ?? "—"}</p>
            <p className="text-xs opacity-80 inline-flex items-center">
              <LabelWithTip label="Composite Score" term="Composite Score" />
              <span className="ml-1">(0–100)</span>
            </p>
          </div>
          <div className="pb-1">
            <p className="text-2xl font-black text-yellow-300">#{sdg.rank ?? "—"}</p>
            <p className="text-xs opacity-80">Rank among Indian states/UTs</p>
          </div>
        </div>
        {sdg.composite != null && (
          <div className="mt-3 bg-white/10 rounded-full h-2">
            <div
              className="bg-yellow-300 h-2 rounded-full"
              style={{ width: `${sdg.composite}%` }}
            />
          </div>
        )}
      </div>

      {/* Weakest goals */}
      <div className="bg-white rounded-2xl border border-gray-200 p-4">
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-3">
          5 Weakest Goals
        </p>
        <div className="space-y-3">
          {weakest.map(([g, score]) => {
            const gap = sdg.gaps_from_national_best?.[g];
            return (
              <div key={g}>
                <div className="flex items-center justify-between mb-1">
                  <span className="text-[11px] text-gray-700 font-semibold">
                    Goal {g} — {SDG_GOAL_NAMES[g] ?? ""}
                  </span>
                  <div className="flex items-center gap-2">
                    {gap != null && gap < 0 && (
                      <span className="text-[10px] font-bold text-red-500">{gap} pts</span>
                    )}
                    <span className="text-xs font-black text-gray-900">{score}</span>
                  </div>
                </div>
                <MiniBar value={score} max={100} color="bg-amber-400" />
              </div>
            );
          })}
        </div>
      </div>

      {/* All goals */}
      <div className="bg-white rounded-2xl border border-gray-200 p-4">
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-3">
          All Goal Scores
        </p>
        <div className="space-y-2">
          {goalEntries.map(([g, score]) => (
            <div key={g}>
              <div className="flex items-center justify-between mb-0.5">
                <span className="text-[10px] text-gray-600">
                  SDG {g} — {SDG_GOAL_NAMES[g] ?? ""}
                </span>
                <span className="text-[11px] font-black text-gray-900">{score}</span>
              </div>
              <MiniBar
                value={score}
                max={100}
                color={
                  (score ?? 0) >= 75
                    ? "bg-emerald-500"
                    : (score ?? 0) >= 50
                    ? "bg-yellow-400"
                    : "bg-red-400"
                }
              />
            </div>
          ))}
        </div>
        <p className="text-[9px] text-gray-400 mt-2">
          Green ≥ 75 · Yellow 50–74 · Red &lt; 50 · Gap vs national best in brackets
        </p>
      </div>
    </div>
  );
}

// ─── Cost of Living ───────────────────────────────────────────────────────────

function CostSection({
  colIndia,
  colTN,
}: {
  colIndia?: CoLSnapshot | null;
  colTN?: CoLSnapshot | null;
}) {
  return (
    <div className="space-y-4">
      {/* Fuel */}
      {colIndia?.fuel && (
        <div className="bg-white rounded-2xl border border-gray-200 p-4">
          <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-1">
            Fuel Prices — Chennai Reference
          </p>
          <SourceLink name="Goodreturns / IOCL" url="https://www.goodreturns.in/petrol-price.html" period={colIndia.period} />
          <div className="space-y-0">
            {[
              { label: "Petrol",                     key: "petrol" },
              { label: "Diesel",                     key: "diesel" },
              { label: "LPG 14.2 kg (domestic)",    key: "lpg_14kg_domestic" },
              { label: "LPG 5 kg",                   key: "lpg_5kg_domestic" },
              { label: "LPG 19 kg (commercial)",     key: "lpg_19kg_commercial" },
            ].map(({ label, key }) => {
              const item = colIndia.fuel?.[key];
              if (!item || item.price == null) return null;
              return (
                <div
                  key={key}
                  className="flex items-center justify-between py-2.5 border-b border-gray-50 last:border-0"
                >
                  <span className="text-xs text-gray-700">{label}</span>
                  <div className="text-right">
                    <span className="text-xs font-black text-gray-900">{fInr(item.price)}</span>
                    <span className="text-[10px] text-gray-400 ml-1">{item.unit}</span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Aavin dairy */}
      {colTN?.food_dairy && (
        <div className="bg-white rounded-2xl border border-gray-200 p-4">
          <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-1">
            Aavin Dairy Prices — Tamil Nadu
          </p>
          <SourceLink name="LiveChennai / Aavin" url="https://www.livechennai.com/aavin_milk_price_in_chennai.asp" period={colTN.period} />
          <div className="space-y-0">
            {Object.entries(colTN.food_dairy)
              .filter(([, v]) => v.price != null)
              .map(([key, v]) => (
                <div
                  key={key}
                  className="flex items-center justify-between py-2.5 border-b border-gray-50 last:border-0"
                >
                  <span className="text-xs text-gray-700 capitalize">
                    {key
                      .replace(/_/g, " ")
                      .replace("milk aavin ", "Aavin ")
                      .replace("milk private ", "Private (branded) ")}
                  </span>
                  <div className="text-right">
                    <span className="text-xs font-black text-gray-900">{fInr(v.price)}</span>
                    <span className="text-[10px] text-gray-400 ml-1">{v.unit}</span>
                  </div>
                </div>
              ))}
          </div>
        </div>
      )}

      {!colIndia && !colTN && <EmptySection msg="No cost of living data available" />}
    </div>
  );
}

// ─── School Education (UDISE+) ───────────────────────────────────────────────

function SchoolSection({ udise, aiUdise }: { udise?: UDISESnapshot | null; aiUdise?: UDISESnapshot | null }) {
  if (!udise) return <EmptySection msg="No UDISE+ data available" />;
  const ger = udise.ger;
  const aiGer = aiUdise?.ger;
  const dropout = udise.dropout_rate;
  const aiDropout = aiUdise?.dropout_rate;
  const ptr = udise.ptr;

  return (
    <div className="space-y-4">
      {/* GER */}
      <div className="bg-white rounded-2xl border border-gray-200 p-4">
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-1 inline-flex items-center flex-wrap">
          <LabelWithTip label="GER" term="GER" />
          <span className="ml-1">— Gross Enrolment Ratio · <LabelWithTip label="UDISE+" term="UDISE+" className="ml-0.5" /> {udise.period}</span>
        </p>
        <SourceLink name="UDISE+ Flash Statistics (MoE)" url="https://udiseplus.gov.in/#/page/publications" period={udise.period} />
        <div className="grid grid-cols-5 gap-2 text-center">
          {(["primary", "upper_primary", "elementary", "secondary", "higher_secondary"] as const).map((level) => {
            const val = ger?.[level];
            const ai = aiGer?.[level];
            const labels: Record<string, string> = { primary: "Primary", upper_primary: "Upper Pri.", elementary: "Elem.", secondary: "Secondary", higher_secondary: "Hr. Sec." };
            return (
              <div key={level} className="bg-gray-50 rounded-xl p-2">
                <p className="text-[9px] text-gray-500 font-semibold">{labels[level]}</p>
                <p className="text-lg font-black text-gray-900">{f1(val)}</p>
                {ai != null && <p className="text-[9px] text-gray-400">India: {f1(ai)}</p>}
                <MiniBar value={val} max={120} color="bg-blue-500" />
              </div>
            );
          })}
        </div>
        <p className="text-[10px] text-gray-400 mt-2 leading-snug">
          GER can exceed 100% because it counts over-age and under-age students, not just the eligible age group.
        </p>
      </div>

      {/* Dropout */}
      {dropout && (
        <div className="bg-white rounded-2xl border border-gray-200 p-4">
          <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-3 inline-flex items-center">
            Dropout Rate (%)
            <InfoTip term="Dropout Rate" />
          </p>
          <div className="grid grid-cols-3 gap-3 text-center">
            {(["primary", "upper_primary", "secondary"] as const).map((level) => {
              const val = dropout?.[level];
              const ai = aiDropout?.[level];
              const labels: Record<string, string> = { primary: "Primary", upper_primary: "Upper Primary", secondary: "Secondary" };
              const isGood = val != null && val < 3;
              return (
                <div key={level} className="bg-gray-50 rounded-xl p-3">
                  <p className="text-[10px] text-gray-500 font-semibold">{labels[level]}</p>
                  <p className={`text-xl font-black ${isGood ? "text-green-600" : val != null && val > 10 ? "text-red-600" : "text-gray-900"}`}>
                    {f1(val)}%
                  </p>
                  {ai != null && <p className="text-[9px] text-gray-400">India: {f1(ai)}%</p>}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* PTR + Schools */}
      <div className="bg-white rounded-2xl border border-gray-200 p-4">
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-3 inline-flex items-center">
          <LabelWithTip label="PTR" term="PTR" />
          <span className="ml-1">— Pupil-Teacher Ratio & Infrastructure</span>
        </p>
        <div className="grid grid-cols-2 gap-3">
          {ptr && (["primary", "secondary"] as const).map((level) => (
            <div key={level} className="bg-gray-50 rounded-xl p-3 text-center">
              <p className="text-[10px] text-gray-500 font-semibold inline-flex items-center justify-center w-full">
                PTR ({level})<InfoTip term="PTR" />
              </p>
              <p className="text-xl font-black text-gray-900">{f1(ptr[level])}</p>
              <p className="text-[9px] text-gray-400">students per teacher</p>
            </div>
          ))}
          {udise.schools_total && (
            <div className="bg-gray-50 rounded-xl p-3 text-center">
              <p className="text-[10px] text-gray-500 font-semibold">Total Schools</p>
              <p className="text-xl font-black text-gray-900">{udise.schools_total.toLocaleString("en-IN")}</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ─── Crime (NCRB) ────────────────────────────────────────────────────────────

function CrimeSection({ ncrb, aiNcrb }: { ncrb?: NCRBSnapshot | null; aiNcrb?: NCRBSnapshot | null }) {
  if (!ncrb) return <EmptySection msg="No NCRB data available" />;

  const metrics: { key: keyof NCRBSnapshot; label: string; term: string | null; color: string }[] = [
    { key: "total_ipc_crimes",       label: "Total IPC Crimes",       term: "IPC", color: "bg-red-500" },
    { key: "crimes_against_women",   label: "Against Women",          term: null,  color: "bg-pink-500" },
    { key: "crimes_against_children",label: "Against Children",       term: null,  color: "bg-orange-500" },
    { key: "crimes_against_sc",      label: "Against SC",             term: "SC",  color: "bg-amber-500" },
    { key: "crimes_against_st",      label: "Against ST",             term: "ST",  color: "bg-yellow-500" },
  ];

  return (
    <div className="space-y-4">
      <div className="bg-white rounded-2xl border border-gray-200 p-4">
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-1 inline-flex items-center flex-wrap">
          Cognizable Crimes — <LabelWithTip label="IPC" term="IPC" className="mx-1" /> · <LabelWithTip label="NCRB" term="NCRB" className="mx-1" /> {ncrb.period}
        </p>
        <SourceLink name="NCRB Crime in India — Additional Tables (States/UTs)" url="https://www.ncrb.gov.in/crime-in-india-additional-table?year=2022&category=States/UTs" period={ncrb.period} />
        <p className="text-[10px] text-gray-500 mb-3 leading-snug">
          Absolute number of registered cognizable offences in {ncrb.period}. Higher reported numbers can reflect either more crime or better reporting/registration — interpret with care.
        </p>

        <div className="space-y-3">
          {metrics.map(({ key, label, term, color }) => {
            const val = ncrb[key] as number | undefined;
            const ai = aiNcrb?.[key] as number | undefined;
            if (val == null) return null;
            return (
              <div key={key} className="flex items-center gap-3">
                <div className="w-32 flex-shrink-0">
                  <p className="text-xs font-semibold text-gray-700 inline-flex items-center">
                    {label}{term && <InfoTip term={term} />}
                  </p>
                </div>
                <div className="flex-1">
                  <div className="flex items-baseline gap-2">
                    <p className="text-lg font-black text-gray-900">{val.toLocaleString("en-IN")}</p>
                    {ai != null && (
                      <p className="text-[10px] text-gray-400">India: {ai.toLocaleString("en-IN")}</p>
                    )}
                  </div>
                  <MiniBar value={val} max={ai || val * 1.2} color={color} />
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

// ─── Industry (ASI) ──────────────────────────────────────────────────────────

function IndustrySection({ asi, aiAsi }: { asi?: ASISnapshot | null; aiAsi?: ASISnapshot | null }) {
  if (!asi) return <EmptySection msg="No ASI data available" />;

  return (
    <div className="space-y-4">
      <div className="bg-white rounded-2xl border border-gray-200 p-4">
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-1 inline-flex items-center">
          Factory Sector · <LabelWithTip label="ASI" term="ASI" className="mx-1" /> {asi.period}
        </p>
        <SourceLink name="ASI 2023-24 Vol I (MOSPI)" url="https://www.mospi.gov.in/asi-summary-results" period={asi.period} />

        <div className="grid grid-cols-2 gap-3">
          {[
            { label: "Factories",     term: null,             val: asi.factories,        ai: aiAsi?.factories,        fmt: (v: number) => v.toLocaleString("en-IN"), unit: "" },
            { label: "GVA",           term: "GVA",            val: asi.gva_cr,           ai: aiAsi?.gva_cr,           fmt: (v: number) => `₹${v.toFixed(0)}`, unit: " Cr" },
            { label: "Total Output",  term: "Total Output",   val: asi.total_output_cr,  ai: aiAsi?.total_output_cr,  fmt: (v: number) => `₹${v.toFixed(0)}`, unit: " Cr" },
            { label: "Total Input",   term: "Total Input",    val: asi.total_input_cr,   ai: aiAsi?.total_input_cr,   fmt: (v: number) => `₹${v.toFixed(0)}`, unit: " Cr" },
            { label: "NVA",           term: "NVA",            val: asi.nva_cr,           ai: aiAsi?.nva_cr,           fmt: (v: number) => `₹${v.toFixed(0)}`, unit: " Cr" },
            { label: "Fixed Capital", term: "Fixed Capital",  val: asi.fixed_capital_cr, ai: aiAsi?.fixed_capital_cr, fmt: (v: number) => `₹${v.toFixed(0)}`, unit: " Cr" },
          ].map(({ label, term, val, ai, fmt, unit }) => {
            if (val == null) return null;
            return (
              <div key={label} className="bg-gray-50 rounded-xl p-3 text-center">
                <p className="text-[10px] text-gray-500 font-semibold inline-flex items-center justify-center w-full">
                  {label}{term && <InfoTip term={term} />}
                </p>
                <p className="text-lg font-black text-gray-900">
                  {fmt(val)}<span className="text-xs font-normal text-gray-400">{unit}</span>
                </p>
                {ai != null && (
                  <p className="text-[9px] text-gray-400">India: {fmt(ai)}{unit}</p>
                )}
              </div>
            );
          })}
        </div>
        <p className="text-[10px] text-gray-400 mt-3 leading-snug">
          Covers registered factories (10+ workers with power, or 20+ without). &ldquo;Cr&rdquo; = crore (1 Cr = ₹10 million = ₹1,00,00,000).
        </p>
      </div>
    </div>
  );
}

// ─── Fiscal (State Budget / CAG) ─────────────────────────────────────────────

function fCr(v: number | null | undefined): string {
  if (v == null) return "—";
  if (typeof v !== "number") return "—";
  return "₹" + v.toLocaleString("en-IN", { maximumFractionDigits: 0 }) + " Cr";
}

function FiscalSection({ budget, rbi }: { budget?: StateBudget | null; rbi?: RBISnapshot | null }) {
  if (!budget && !rbi) return <EmptySection msg="No state budget data available" />;
  // These are only used inside {budget && (...)} so always defined there
  const rev = budget?.revenue ?? {} as StateBudget["revenue"];
  const exp = budget?.expenditure ?? {} as StateBudget["expenditure"];
  const fis = budget?.fiscal ?? {} as StateBudget["fiscal"];
  const com = budget?.committed ?? {} as StateBudget["committed"];

  return (
    <div className="space-y-4">
      {/* RBI Debt-to-GSDP headline */}
      {rbi?.debt_to_gsdp_pct != null && (
        <div className="bg-gradient-to-br from-red-900 to-red-700 rounded-2xl p-5 text-white">
          <p className="text-[10px] font-bold uppercase tracking-widest opacity-70">
            <a href="https://rbi.org.in/scripts/PublicationsView.aspx?Id=23729" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:opacity-100">
              RBI State Finances
            </a>{" "}· {rbi.period}
          </p>
          <div className="flex items-end gap-6 mt-2">
            <div>
              <p className="text-4xl font-black">{rbi.debt_to_gsdp_pct}%</p>
              <p className="text-[11px] opacity-70 inline-flex items-center">
                <LabelWithTip label="Debt-to-GSDP ratio" term="Debt-to-GSDP" />
              </p>
            </div>
            <div className="flex gap-4 text-sm opacity-80">
              {rbi.gfd_cr != null && (
                <div>
                  <p className="font-bold">{fCr(rbi.gfd_cr)}</p>
                  <p className="text-[10px] opacity-70 inline-flex items-center">
                    <LabelWithTip label="Gross Fiscal Deficit" term="GFD" />
                  </p>
                </div>
              )}
              {rbi.interest_payments_cr != null && (
                <div>
                  <p className="font-bold">{fCr(rbi.interest_payments_cr)}</p>
                  <p className="text-[10px] opacity-70">Interest Payments</p>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* CAG detailed breakdown */}
      {budget && (
      <div className="bg-white rounded-2xl border border-gray-200 p-4">
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-1 inline-flex items-center">
          State Finances — <LabelWithTip label="CAG" term="CAG" className="mx-1" /> Actuals · {budget.fiscal_year}
        </p>
        <p className="text-[10px] text-gray-400 mb-3">
          Source:{" "}
          {budget.source_url ? (
            <a href={budget.source_url} target="_blank" rel="noopener noreferrer"
               className="underline underline-offset-2 hover:text-gray-600">
              {budget.source || "CAG Finance Accounts Vol I"}
            </a>
          ) : (
            budget.source || "CAG Finance Accounts Vol I"
          )}
        </p>

        {/* Revenue */}
        <div className="mb-4">
          <p className="text-[10px] font-bold text-gray-500 mb-2 inline-flex items-center">
            <LabelWithTip label="Revenue Receipts" term="Revenue Receipts" />
          </p>
          <div className="grid grid-cols-2 gap-2">
            {[
              { label: "Total Revenue",      term: "Revenue Receipts",  val: rev.total_revenue_receipts_cr },
              { label: "Own Tax Revenue",    term: "Own Tax Revenue",   val: rev.own_tax_revenue_cr },
              { label: "Central Devolution", term: "Central Devolution",val: rev.central_devolution_cr },
              { label: "Grants-in-Aid",      term: "Grants-in-Aid",     val: rev.grants_in_aid_cr },
            ].filter(({ val }) => val != null).map(({ label, term, val }) => (
              <div key={label} className="bg-gray-50 rounded-xl p-3">
                <p className="text-[10px] text-gray-500 font-semibold inline-flex items-center">
                  {label}<InfoTip term={term} />
                </p>
                <p className="text-sm font-black text-gray-900">{fCr(val)}</p>
              </div>
            ))}
          </div>
        </div>

        {/* Expenditure */}
        {(exp.total_exp_cr != null || exp.revenue_expenditure_cr != null) && (
          <div className="mb-4">
            <p className="text-[10px] font-bold text-gray-500 mb-2">Expenditure</p>
            <div className="grid grid-cols-2 gap-2">
              {[
                { label: "Total Expenditure",   term: null,                    val: exp.total_exp_cr },
                { label: "Revenue Expenditure", term: "Revenue Expenditure",   val: exp.revenue_expenditure_cr },
                { label: "Capital Expenditure", term: "Capital Expenditure",   val: exp.capital_expenditure_cr },
              ].filter(({ val }) => val != null).map(({ label, term, val }) => (
                <div key={label} className="bg-gray-50 rounded-xl p-3">
                  <p className="text-[10px] text-gray-500 font-semibold inline-flex items-center">
                    {label}{term && <InfoTip term={term} />}
                  </p>
                  <p className="text-sm font-black text-gray-900">{fCr(val)}</p>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Fiscal deficits */}
        {(fis.fiscal_deficit_cr != null || fis.revenue_deficit_cr != null) && (
          <div className="mb-4">
            <p className="text-[10px] font-bold text-gray-500 mb-2">Deficit Indicators</p>
            <div className="grid grid-cols-2 gap-2">
              {[
                { label: "Fiscal Deficit",  term: "Fiscal Deficit",  val: fis.fiscal_deficit_cr,  warn: true },
                { label: "Revenue Deficit", term: "Revenue Deficit", val: fis.revenue_deficit_cr, warn: true },
                { label: "Primary Deficit", term: "Primary Deficit", val: fis.primary_deficit_cr, warn: true },
              ].filter(({ val }) => val != null).map(({ label, term, val, warn }) => (
                <div key={label} className="bg-gray-50 rounded-xl p-3">
                  <p className="text-[10px] text-gray-500 font-semibold inline-flex items-center">
                    {label}<InfoTip term={term} />
                  </p>
                  <p className={`text-sm font-black ${warn && val != null && val > 0 ? "text-red-600" : "text-gray-900"}`}>
                    {fCr(val)}
                  </p>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Committed expenditure */}
        {(com.salaries_cr != null || com.interest_cr != null || com.pensions_cr != null) && (
          <div>
            <p className="text-[10px] font-bold text-gray-500 mb-2 inline-flex items-center">
              <LabelWithTip label="Committed Expenditure" term="Committed Expenditure" />
            </p>
            <div className="grid grid-cols-2 gap-2">
              {[
                { label: "Salaries",            term: null,                    val: com.salaries_cr },
                { label: "Pensions",            term: null,                    val: com.pensions_cr },
                { label: "Interest Payments",   term: null,                    val: com.interest_cr },
                { label: "Subsidies",           term: null,                    val: com.subsidies_cr },
                { label: "Discretionary Space", term: "Discretionary Space",   val: com.discretionary_cr },
              ].filter(({ val }) => val != null).map(({ label, term, val }) => (
                <div key={label} className="bg-gray-50 rounded-xl p-3">
                  <p className="text-[10px] text-gray-500 font-semibold inline-flex items-center">
                    {label}{term && <InfoTip term={term} />}
                  </p>
                  <p className="text-sm font-black text-gray-900">{fCr(val)}</p>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
      )}
    </div>
  );
}

// ─── Page ─────────────────────────────────────────────────────────────────────

type SectionKey = "labour" | "health" | "spending" | "education" | "school" | "crime" | "industry" | "fiscal" | "sdg" | "cost";

const SECTIONS: { key: SectionKey; label: string }[] = [
  { key: "labour",    label: "Labour" },
  { key: "health",    label: "Health" },
  { key: "spending",  label: "Spending" },
  { key: "education", label: "Higher Ed" },
  { key: "school",    label: "School Ed" },
  { key: "crime",     label: "Crime" },
  { key: "industry",  label: "Industry" },
  { key: "fiscal",    label: "Fiscal" },
  { key: "sdg",       label: "SDG" },
  { key: "cost",      label: "Cost of Living" },
];

export default function StateReportPage() {
  const params = useParams();
  const slug = typeof params.slug === "string" ? params.slug : "";

  const [activeSection, setActiveSection] = useState<SectionKey>("labour");
  const [report, setReport]   = useState<StateReport | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError]     = useState<string | null>(null);

  const currentState = STATES.find((s) => s.slug === slug) ?? STATES[0];
  const isTN = slug === "tamil_nadu";

  useEffect(() => {
    if (!slug) return;
    setLoading(true);
    setReport(null);
    setError(null);
    apiGet<StateReport>(`/api/state-report/${slug}`)
      .then(setReport)
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false));
  }, [slug]);

  const aiPlfs  = report?.all_india?.plfs;
  const aiSrs   = report?.all_india?.srs;
  const aiHces  = report?.all_india?.hces;
  const aiUdise = report?.all_india?.udise;
  const aiAsi   = report?.all_india?.asi;
  const aiNcrb  = report?.all_india?.ncrb;
  const aiAishe = report?.all_india?.aishe;

  const visibleSections = SECTIONS;

  return (
    <main className="min-h-screen bg-gray-50">
      {/* Sticky header */}
      <header className="bg-white border-b border-gray-200 shadow-sm sticky top-0 z-10">
        <div className="max-w-2xl mx-auto px-4 py-3 flex items-center gap-3">
          <Link href="/" className="text-gray-400 hover:text-gray-600 text-sm flex-shrink-0">
            ← Home
          </Link>
          <span className="text-gray-200">|</span>
          <div className="min-w-0">
            <h1 className="text-sm font-black text-gray-900 truncate">
              {currentState.emoji} {currentState.name} — State Vitals
            </h1>
            <p className="text-[10px] text-gray-500">
              PLFS · SRS · HCES · AISHE · SDG Index{isTN ? " · Cost of Living" : ""}
            </p>
          </div>
        </div>

        {/* State selector */}
        <div className="max-w-2xl mx-auto px-4 pb-2 flex gap-2 overflow-x-auto no-scrollbar">
          {STATES.map((s) => (
            <Link
              key={s.slug}
              href={`/state-report/${s.slug}`}
              className={`text-xs font-semibold px-3 py-1 rounded-full border whitespace-nowrap transition-all flex-shrink-0 ${
                s.slug === slug
                  ? "bg-gray-900 text-white border-gray-900"
                  : "bg-white text-gray-600 border-gray-300 hover:border-gray-500"
              }`}
            >
              {s.emoji} {s.name}
            </Link>
          ))}
        </div>

        {/* Section tabs */}
        <div className="max-w-2xl mx-auto px-4 pb-2 pt-1 flex gap-2 overflow-x-auto no-scrollbar border-t border-gray-100">
          {visibleSections.map((s) => (
            <button
              key={s.key}
              onClick={() => setActiveSection(s.key)}
              className={`text-xs font-semibold px-3 py-1 rounded-full border whitespace-nowrap transition-all flex-shrink-0 ${
                s.key === activeSection
                  ? "bg-gray-900 text-white border-gray-900"
                  : "bg-white text-gray-600 border-gray-200 hover:border-gray-400"
              }`}
            >
              {s.label}
            </button>
          ))}
        </div>
      </header>

      <div className="max-w-2xl mx-auto px-4 py-6">
        {/* Loading skeleton */}
        {loading && (
          <div className="space-y-3">
            {[1, 2, 3].map((i) => (
              <div
                key={i}
                className="bg-white rounded-2xl border border-gray-200 p-4 h-32 animate-pulse"
              />
            ))}
          </div>
        )}

        {/* Error */}
        {!loading && error && (
          <div className="bg-red-50 border border-red-200 rounded-2xl p-4 text-sm text-red-600">
            Failed to load report: {error}
          </div>
        )}

        {/* Content */}
        {!loading && !error && report && (
          <div className="space-y-4">
            {activeSection === "labour"    && <LabourSection    plfs={report.plfs}       aiPlfs={aiPlfs} slug={slug} />}
            {activeSection === "health"    && <HealthSection    srs={report.srs}         aiSrs={aiSrs} />}
            {activeSection === "spending"  && <SpendingSection  hces={report.hces}       aiHces={aiHces} />}
            {activeSection === "education" && <EducationSection aishe={report.aishe} aiAishe={aiAishe} />}
            {activeSection === "school"    && <SchoolSection    udise={report.udise}     aiUdise={aiUdise} />}
            {activeSection === "crime"     && <CrimeSection     ncrb={report.ncrb}       aiNcrb={aiNcrb} />}
            {activeSection === "industry"  && <IndustrySection  asi={report.asi}         aiAsi={aiAsi} />}
            {activeSection === "fiscal"    && <FiscalSection    budget={report.state_budget} rbi={report.rbi_state_finances} />}
            {activeSection === "sdg"       && <SDGSection       sdg={report.sdg_index} />}
            {activeSection === "cost"      && (
              <CostSection
                colIndia={report.cost_of_living}
                colTN={isTN ? report.cost_of_living : undefined}
              />
            )}
          </div>
        )}

        {/* Attribution */}
        <p className="text-center text-[10px] text-gray-400 mt-10 pb-4">
          Sources: PLFS · SRS · HCES · AISHE · UDISE+ · NCRB · ASI (MOSPI) · NITI Aayog SDG Index · Goodreturns
        </p>
      </div>
    </main>
  );
}
