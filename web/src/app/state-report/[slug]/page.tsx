"use client";

import { useState, useEffect } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { apiGet } from "@/lib/api-client";

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

interface SRSSnapshot {
  period: string;
  birth_rate?: RuralUrbanTotal;
  death_rate?: RuralUrbanTotal;
  natural_growth_rate?: RuralUrbanTotal;
  imr?: RuralUrbanTotal;
  u5mr?: RuralUrbanTotal;
  tfr?: RuralUrbanTotal;
  birth_rate_trend?: number[];
  death_rate_trend?: number[];
  imr_trend?: number[];
  tfr_trend?: number[];
  birth_rate_trend_years?: number[];
}

interface HCESSnapshot {
  period: string;
  mpce_with_free?: { rural?: number; urban?: number };
  mpce_without_free?: { rural?: number; urban?: number };
  welfare_uplift_pct?: { rural?: number; urban?: number };
}

interface EnrollmentCell {
  male?: number | null;
  female?: number | null;
  total?: number | null;
}

interface AISHESnapshot {
  period: string;
  ger?: EnrollmentCell;
  universities?: number;
  colleges?: number;
  enrollment?: EnrollmentCell;
  phd_enrollment?: number;
  gender_parity_index?: number;
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

interface AllIndiaRef {
  plfs?: PLFSSnapshot | null;
  srs?: SRSSnapshot | null;
  hces?: HCESSnapshot | null;
  udise?: UDISESnapshot | null;
  asi?: ASISnapshot | null;
  sdg_index?: SDGSnapshot | null;
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
  return v == null ? "—" : v.toFixed(1);
}

function fInr(v: number | null | undefined): string {
  if (v == null) return "—";
  return "₹" + v.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
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

function LabourSection({
  plfs,
  aiPlfs,
}: {
  plfs?: PLFSSnapshot | null;
  aiPlfs?: PLFSSnapshot | null;
}) {
  if (!plfs) return <EmptySection msg="No PLFS data available" />;

  const areas = ["rural", "urban", "total"] as const;

  return (
    <div className="space-y-4">
      {/* 15+ headline */}
      <div className="bg-white rounded-2xl border border-gray-200 p-4">
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-3">
          Labour Force — Age 15+ · {plfs.period}
        </p>
        <div className="grid grid-cols-3 gap-2 text-center mb-4">
          {(["lfpr", "wpr", "ur"] as const).map((metric) => {
            const val = plfs[metric]?.["15+"]?.total?.person;
            const ai = aiPlfs?.[metric]?.["15+"]?.total?.person;
            const labels = { lfpr: "LFPR", wpr: "WPR", ur: "Unemployment" };
            return (
              <div key={metric} className="bg-gray-50 rounded-xl p-2">
                <p className="text-[10px] text-gray-500 font-semibold">{labels[metric]}</p>
                <p className="text-xl font-black text-gray-900">
                  {f1(val)}<span className="text-xs font-normal text-gray-400">%</span>
                </p>
                {ai != null && (
                  <p className="text-[9px] text-gray-400">IN: {f1(ai)}%</p>
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
                <p className="text-[10px] font-bold text-gray-400 uppercase mb-1.5">
                  {area} LFPR (15+)
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
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-3">
          Youth Unemployment Rate — Age 15–29
        </p>
        <div className="grid grid-cols-3 gap-2">
          {areas.map((area) => {
            const val  = plfs.ur?.["15-29"]?.[area]?.person;
            const ai   = aiPlfs?.ur?.["15-29"]?.[area]?.person;
            const gap  = val != null && ai != null ? val - ai : null;
            return (
              <div key={area} className="bg-gray-50 rounded-xl p-3 text-center">
                <p className="text-[10px] text-gray-500 font-semibold capitalize mb-1">{area}</p>
                <p className="text-lg font-black text-gray-900">
                  {f1(val)}<span className="text-xs font-normal text-gray-400">%</span>
                </p>
                {gap != null && (
                  <p className={`text-[10px] font-bold ${gap > 0 ? "text-red-500" : "text-emerald-600"}`}>
                    {gap > 0 ? "+" : ""}{gap.toFixed(1)} vs IN
                  </p>
                )}
              </div>
            );
          })}
        </div>
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
    val?: number | null;
    ai?: number | null;
    unit: string;
    lowerBetter: boolean;
  }[] = [
    { label: "Infant Mortality Rate",      val: srs.imr?.total,              ai: aiSrs?.imr?.total,              unit: "/1000 LB", lowerBetter: true  },
    { label: "Under-5 Mortality",          val: srs.u5mr?.total,             ai: aiSrs?.u5mr?.total,             unit: "/1000 LB", lowerBetter: true  },
    { label: "Total Fertility Rate",       val: srs.tfr?.total,              ai: aiSrs?.tfr?.total,              unit: "",         lowerBetter: true  },
    { label: "Birth Rate",                 val: srs.birth_rate?.total,       ai: aiSrs?.birth_rate?.total,       unit: "/1000",    lowerBetter: false },
    { label: "Death Rate",                 val: srs.death_rate?.total,       ai: aiSrs?.death_rate?.total,       unit: "/1000",    lowerBetter: true  },
    { label: "Natural Growth Rate",        val: srs.natural_growth_rate?.total, ai: aiSrs?.natural_growth_rate?.total, unit: "/1000", lowerBetter: false },
  ];

  return (
    <div className="space-y-4">
      <div className="bg-white rounded-2xl border border-gray-200 p-4">
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-3">
          Vital Statistics · SRS {srs.period}
        </p>
        <div className="space-y-0">
          {rows.map(({ label, val, ai, unit, lowerBetter }) => {
            const gap = val != null && ai != null ? val - ai : null;
            const gapGood = gap != null && (lowerBetter ? gap < 0 : gap > 0);
            return (
              <div
                key={label}
                className="flex items-center justify-between py-2.5 border-b border-gray-50 last:border-0"
              >
                <span className="text-xs text-gray-700">{label}</span>
                <div className="flex items-center gap-2 text-right flex-shrink-0">
                  {gap != null && (
                    <span className={`text-[10px] font-bold ${gapGood ? "text-emerald-600" : "text-red-500"}`}>
                      {gap > 0 ? "+" : ""}{gap.toFixed(1)}
                    </span>
                  )}
                  <span className="text-xs font-black text-gray-900">
                    {f1(val)}<span className="text-[10px] text-gray-400">{unit && ` ${unit}`}</span>
                  </span>
                  {ai != null && (
                    <span className="text-[10px] text-gray-400 w-12">IN: {f1(ai)}</span>
                  )}
                </div>
              </div>
            );
          })}
        </div>
        <p className="text-[9px] text-gray-400 mt-2">
          Gap shown vs All India average. Green = better than average.
        </p>
      </div>

      {/* Rural vs Urban */}
      <div className="bg-white rounded-2xl border border-gray-200 p-4">
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-3">
          Rural vs Urban
        </p>
        <div className="grid grid-cols-2 gap-3">
          {[
            { label: "IMR",         rural: srs.imr?.rural,         urban: srs.imr?.urban },
            { label: "TFR",         rural: srs.tfr?.rural,         urban: srs.tfr?.urban },
            { label: "Birth Rate",  rural: srs.birth_rate?.rural,  urban: srs.birth_rate?.urban },
            { label: "Death Rate",  rural: srs.death_rate?.rural,  urban: srs.death_rate?.urban },
          ].map(({ label, rural, urban }) => (
            <div key={label} className="bg-gray-50 rounded-xl p-3">
              <p className="text-[10px] font-bold text-gray-500 mb-2">{label}</p>
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
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-1">
          Monthly Per Capita Consumption — HCES {hces.period}
        </p>
        <p className="text-[10px] text-gray-400 mb-4">
          &ldquo;With free goods&rdquo; counts govt-supplied items at imputed value.
        </p>

        {/* MPCE grid */}
        <div className="grid grid-cols-2 gap-3 mb-4">
          {[
            { label: "Rural\n(with free goods)",    val: hces.mpce_with_free?.rural,   ai: aiHces?.mpce_with_free?.rural },
            { label: "Urban\n(with free goods)",    val: hces.mpce_with_free?.urban,   ai: aiHces?.mpce_with_free?.urban },
            { label: "Rural\n(without free goods)", val: hces.mpce_without_free?.rural, ai: aiHces?.mpce_without_free?.rural },
            { label: "Urban\n(without free goods)", val: hces.mpce_without_free?.urban, ai: aiHces?.mpce_without_free?.urban },
          ].map(({ label, val, ai }) => {
            const gap = val != null && ai != null ? val - ai : null;
            return (
              <div key={label} className="bg-gray-50 rounded-xl p-3">
                <p className="text-[10px] text-gray-500 font-semibold leading-tight whitespace-pre-line mb-1">
                  {label}
                </p>
                <p className="text-sm font-black text-gray-900">{fInr(val)}</p>
                {ai != null && (
                  <p className="text-[9px] text-gray-400">IN: {fInr(ai)}</p>
                )}
                {gap != null && (
                  <p className={`text-[10px] font-bold mt-0.5 ${gap > 0 ? "text-emerald-600" : "text-red-500"}`}>
                    {gap > 0 ? "+" : ""}{fInr(gap)} vs IN
                  </p>
                )}
              </div>
            );
          })}
        </div>

        {/* Welfare uplift */}
        {(hces.welfare_uplift_pct?.rural != null || hces.welfare_uplift_pct?.urban != null) && (
          <div className="border-t border-gray-100 pt-3">
            <p className="text-[10px] font-bold text-gray-500 mb-2">
              Welfare Uplift from Free Goods
            </p>
            <div className="flex gap-6">
              <div>
                <p className="text-[9px] text-green-600 font-semibold">Rural</p>
                <p className="text-base font-black text-gray-900">
                  +{f1(hces.welfare_uplift_pct?.rural)}%
                </p>
              </div>
              <div>
                <p className="text-[9px] text-blue-600 font-semibold">Urban</p>
                <p className="text-base font-black text-gray-900">
                  +{f1(hces.welfare_uplift_pct?.urban)}%
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

function EducationSection({ aishe }: { aishe?: AISHESnapshot | null }) {
  if (!aishe) return <EmptySection msg="No AISHE data available" />;

  return (
    <div className="space-y-4">
      <div className="bg-white rounded-2xl border border-gray-200 p-4">
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-3">
          Higher Education — AISHE {aishe.period}
        </p>

        {/* GER */}
        {aishe.ger && (
          <div className="mb-4">
            <p className="text-[10px] text-gray-500 font-semibold mb-2">
              Gross Enrolment Ratio (GER) — 18–23 age group
            </p>
            <div className="grid grid-cols-3 gap-2 text-center">
              {(["male", "female", "total"] as const).map((g) => (
                <div key={g} className="bg-gray-50 rounded-xl p-2">
                  <p className="text-[9px] text-gray-500 capitalize font-semibold">
                    {g === "total" ? "Overall" : g}
                  </p>
                  <p className="text-xl font-black text-gray-900">
                    {f1(aishe.ger?.[g])}
                    <span className="text-xs font-normal text-gray-400">%</span>
                  </p>
                  <MiniBar
                    value={aishe.ger?.[g]}
                    max={60}
                    color={g === "male" ? "bg-blue-400" : g === "female" ? "bg-rose-400" : "bg-gray-400"}
                  />
                </div>
              ))}
            </div>
            {aishe.gender_parity_index != null && (
              <p className="text-[10px] text-gray-500 mt-2 text-center">
                Gender Parity Index:{" "}
                <span className={`font-bold ${aishe.gender_parity_index >= 1 ? "text-emerald-600" : "text-amber-600"}`}>
                  {aishe.gender_parity_index.toFixed(2)}
                </span>
                <span className="text-gray-400"> — {aishe.gender_parity_index >= 1 ? "more women than men enrolled" : "below parity"}</span>
              </p>
            )}
          </div>
        )}

        {/* Institutions */}
        <div className="grid grid-cols-3 gap-2 text-center mb-4">
          {[
            { label: "Universities",  val: aishe.universities },
            { label: "Colleges",      val: aishe.colleges },
            { label: "PhD Students",  val: aishe.phd_enrollment },
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
            <p className="text-[10px] font-bold text-gray-500 mb-2">Total Enrollment</p>
            <div className="grid grid-cols-3 gap-2 text-center">
              {(["male", "female", "total"] as const).map((g) => {
                const v = aishe.enrollment?.[g];
                return (
                  <div key={g}>
                    <p className="text-[9px] text-gray-400 capitalize font-semibold">
                      {g === "total" ? "Total" : g}
                    </p>
                    <p className="text-sm font-black text-gray-900">
                      {v != null ? (v / 100000).toFixed(1) + "L" : "—"}
                    </p>
                  </div>
                );
              })}
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
        <p className="text-[10px] font-bold uppercase tracking-widest opacity-70">
          NITI Aayog SDG India Index · {sdg.period}
        </p>
        <div className="flex items-end gap-6 mt-2">
          <div>
            <p className="text-5xl font-black">{sdg.composite ?? "—"}</p>
            <p className="text-xs opacity-80">Overall Score (0–100)</p>
          </div>
          <div className="pb-1">
            <p className="text-2xl font-black text-yellow-300">#{sdg.rank ?? "—"}</p>
            <p className="text-xs opacity-80">National Rank</p>
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
          <p className="text-[10px] text-gray-400 mb-3">As of {colIndia.period} · Source: Goodreturns / IOCL</p>
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
          <p className="text-[10px] text-gray-400 mb-3">As of {colTN.period} · Source: LiveChennai</p>
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
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-1">
          Gross Enrolment Ratio · {udise.period}
        </p>
        <p className="text-[10px] text-gray-400 mb-3">Source: UDISE+</p>
        <div className="grid grid-cols-5 gap-2 text-center">
          {(["primary", "upper_primary", "elementary", "secondary", "higher_secondary"] as const).map((level) => {
            const val = ger?.[level];
            const ai = aiGer?.[level];
            const labels: Record<string, string> = { primary: "Primary", upper_primary: "Upper Pri.", elementary: "Elem.", secondary: "Secondary", higher_secondary: "Hr. Sec." };
            return (
              <div key={level} className="bg-gray-50 rounded-xl p-2">
                <p className="text-[9px] text-gray-500 font-semibold">{labels[level]}</p>
                <p className="text-lg font-black text-gray-900">{f1(val)}</p>
                {ai != null && <p className="text-[9px] text-gray-400">IN: {f1(ai)}</p>}
                <MiniBar value={val} max={120} color="bg-blue-500" />
              </div>
            );
          })}
        </div>
      </div>

      {/* Dropout */}
      {dropout && (
        <div className="bg-white rounded-2xl border border-gray-200 p-4">
          <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-3">
            Dropout Rate (%)
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
                  {ai != null && <p className="text-[9px] text-gray-400">IN: {f1(ai)}%</p>}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* PTR + Schools */}
      <div className="bg-white rounded-2xl border border-gray-200 p-4">
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-3">
          Pupil-Teacher Ratio & Infrastructure
        </p>
        <div className="grid grid-cols-2 gap-3">
          {ptr && (["primary", "secondary"] as const).map((level) => (
            <div key={level} className="bg-gray-50 rounded-xl p-3 text-center">
              <p className="text-[10px] text-gray-500 font-semibold">PTR ({level})</p>
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

  const metrics: { key: keyof NCRBSnapshot; label: string; color: string }[] = [
    { key: "total_ipc_crimes",       label: "Total IPC Crimes",       color: "bg-red-500" },
    { key: "crimes_against_women",   label: "Against Women",          color: "bg-pink-500" },
    { key: "crimes_against_children",label: "Against Children",       color: "bg-orange-500" },
    { key: "crimes_against_sc",      label: "Against SC",             color: "bg-amber-500" },
    { key: "crimes_against_st",      label: "Against ST",             color: "bg-yellow-500" },
  ];

  return (
    <div className="space-y-4">
      <div className="bg-white rounded-2xl border border-gray-200 p-4">
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-1">
          Cognizable Crimes — IPC · {ncrb.period}
        </p>
        <p className="text-[10px] text-gray-400 mb-3">Source: NCRB Crime in India</p>

        <div className="space-y-3">
          {metrics.map(({ key, label, color }) => {
            const val = ncrb[key] as number | undefined;
            const ai = aiNcrb?.[key] as number | undefined;
            if (val == null) return null;
            return (
              <div key={key} className="flex items-center gap-3">
                <div className="w-32 flex-shrink-0">
                  <p className="text-xs font-semibold text-gray-700">{label}</p>
                </div>
                <div className="flex-1">
                  <div className="flex items-baseline gap-2">
                    <p className="text-lg font-black text-gray-900">{val.toLocaleString("en-IN")}</p>
                    {ai != null && (
                      <p className="text-[10px] text-gray-400">IN: {ai.toLocaleString("en-IN")}</p>
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
        <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-1">
          Factory Sector · {asi.period}
        </p>
        <p className="text-[10px] text-gray-400 mb-3">Source: Annual Survey of Industries (MOSPI)</p>

        <div className="grid grid-cols-2 gap-3">
          {[
            { label: "Factories", val: asi.factories, ai: aiAsi?.factories, fmt: (v: number) => v.toLocaleString("en-IN"), unit: "" },
            { label: "GVA", val: asi.gva_cr, ai: aiAsi?.gva_cr, fmt: (v: number) => `₹${v.toFixed(0)}`, unit: " Cr" },
            { label: "Total Output", val: asi.total_output_cr, ai: aiAsi?.total_output_cr, fmt: (v: number) => `₹${v.toFixed(0)}`, unit: " Cr" },
            { label: "Total Input", val: asi.total_input_cr, ai: aiAsi?.total_input_cr, fmt: (v: number) => `₹${v.toFixed(0)}`, unit: " Cr" },
            { label: "NVA", val: asi.nva_cr, ai: aiAsi?.nva_cr, fmt: (v: number) => `₹${v.toFixed(0)}`, unit: " Cr" },
            { label: "Fixed Capital", val: asi.fixed_capital_cr, ai: aiAsi?.fixed_capital_cr, fmt: (v: number) => `₹${v.toFixed(0)}`, unit: " Cr" },
          ].map(({ label, val, ai, fmt, unit }) => {
            if (val == null) return null;
            return (
              <div key={label} className="bg-gray-50 rounded-xl p-3 text-center">
                <p className="text-[10px] text-gray-500 font-semibold">{label}</p>
                <p className="text-lg font-black text-gray-900">
                  {fmt(val)}<span className="text-xs font-normal text-gray-400">{unit}</span>
                </p>
                {ai != null && (
                  <p className="text-[9px] text-gray-400">IN: {fmt(ai)}{unit}</p>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

// ─── Page ─────────────────────────────────────────────────────────────────────

type SectionKey = "labour" | "health" | "spending" | "education" | "school" | "crime" | "industry" | "sdg" | "cost";

const SECTIONS: { key: SectionKey; label: string }[] = [
  { key: "labour",    label: "Labour" },
  { key: "health",    label: "Health" },
  { key: "spending",  label: "Spending" },
  { key: "education", label: "Higher Ed" },
  { key: "school",    label: "School Ed" },
  { key: "crime",     label: "Crime" },
  { key: "industry",  label: "Industry" },
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
            {activeSection === "labour"    && <LabourSection    plfs={report.plfs}       aiPlfs={aiPlfs} />}
            {activeSection === "health"    && <HealthSection    srs={report.srs}         aiSrs={aiSrs} />}
            {activeSection === "spending"  && <SpendingSection  hces={report.hces}       aiHces={aiHces} />}
            {activeSection === "education" && <EducationSection aishe={report.aishe} />}
            {activeSection === "school"    && <SchoolSection    udise={report.udise}     aiUdise={aiUdise} />}
            {activeSection === "crime"     && <CrimeSection     ncrb={report.ncrb}       aiNcrb={undefined} />}
            {activeSection === "industry"  && <IndustrySection  asi={report.asi}         aiAsi={aiAsi} />}
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
