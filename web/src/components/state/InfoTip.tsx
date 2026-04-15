"use client";

import { useState, useRef, useEffect } from "react";

export interface GlossaryEntry {
  full: string;        // Full form, e.g. "Labour Force Participation Rate"
  what: string;        // What it actually means, layman-friendly
  unit?: string;       // Unit explanation (e.g., "% of working-age population")
}

// Central glossary for every abbreviation + metric used on the State Vitals page.
// Keep entries short, concrete, and free of jargon.
export const GLOSSARY: Record<string, GlossaryEntry> = {
  // ── Labour (PLFS) ──────────────────────────────────────────────────────────
  LFPR: {
    full: "Labour Force Participation Rate",
    what: "Share of people aged 15+ who are either working or actively looking for work. Higher = more economic engagement.",
    unit: "% of working-age population",
  },
  WPR: {
    full: "Worker Population Ratio",
    what: "Share of people aged 15+ who are actually working (employed). Higher = more people with jobs.",
    unit: "% of working-age population",
  },
  Unemployment: {
    full: "Unemployment Rate",
    what: "Share of the labour force (people wanting to work) who can't find work. Lower is better.",
    unit: "% of labour force",
  },
  "Youth Unemployment": {
    full: "Youth Unemployment Rate (15–29)",
    what: "Share of young people (aged 15–29) in the labour force who are jobless. A leading signal of how hard it is for young adults to start careers.",
    unit: "% of 15–29 labour force",
  },
  PLFS: {
    full: "Periodic Labour Force Survey",
    what: "MoSPI's nationwide employment survey — the official source for jobs data in India.",
  },

  // ── Health (SRS) ───────────────────────────────────────────────────────────
  IMR: {
    full: "Infant Mortality Rate",
    what: "How many babies die before their first birthday, for every 1,000 babies born alive. Lower is better.",
    unit: "per 1,000 live births",
  },
  MMR: {
    full: "Maternal Mortality Ratio",
    what: "How many mothers die from pregnancy-related causes, for every 100,000 babies born alive. Lower is better.",
    unit: "per 100,000 live births",
  },
  TFR: {
    full: "Total Fertility Rate",
    what: "Average number of children a woman is expected to have over her lifetime. Replacement level is ~2.1.",
    unit: "children per woman",
  },
  CBR: {
    full: "Crude Birth Rate",
    what: "Number of live births in a year for every 1,000 people in the population.",
    unit: "births per 1,000 population",
  },
  CDR: {
    full: "Crude Death Rate",
    what: "Number of deaths in a year for every 1,000 people in the population.",
    unit: "deaths per 1,000 population",
  },
  SRS: {
    full: "Sample Registration System",
    what: "Registrar General of India's annual survey — the official source for India's vital statistics (births, deaths, fertility).",
  },
  LB: {
    full: "Live Births",
    what: "Babies born alive. Used as the denominator for infant and maternal mortality rates.",
  },

  // ── Spending (HCES) ────────────────────────────────────────────────────────
  MPCE: {
    full: "Monthly Per Capita Consumption Expenditure",
    what: "Average rupees spent per person, per month — across every individual in a household (including children and elderly), state-wide average. Higher = more purchasing power.",
    unit: "₹ per person per month",
  },
  HCES: {
    full: "Household Consumption Expenditure Survey",
    what: "MoSPI's survey of what households actually spend on food, rent, fuel, and other items — the official source for living-standard data.",
  },
  "Welfare Uplift": {
    full: "Welfare Uplift from Free Goods",
    what: "How much higher the average monthly spend looks once government-supplied free items (rice, laptops, etc.) are valued and added in. The gap between what people would have paid vs. what they actually got free.",
    unit: "% increase in MPCE",
  },

  // ── Higher Education (AISHE) ───────────────────────────────────────────────
  GER: {
    full: "Gross Enrolment Ratio",
    what: "Number of students enrolled at a given level, as a percentage of the age group that should be at that level. Can exceed 100% (late entrants, repeaters). Higher = more people in formal education.",
    unit: "% of eligible age group",
  },
  GPI: {
    full: "Gender Parity Index",
    what: "Female GER ÷ Male GER. 1.0 means equal enrolment; above 1.0 means more women than men; below 1.0 means women are behind.",
    unit: "ratio",
  },
  UG: { full: "Undergraduate", what: "Bachelor's degree students (BA, BSc, BCom, BE, BTech, etc.)." },
  PG: { full: "Postgraduate",  what: "Master's degree students (MA, MSc, MCom, MBA, MTech, etc.)." },
  PhD: { full: "Doctor of Philosophy", what: "Doctoral research students." },
  AISHE: {
    full: "All India Survey on Higher Education",
    what: "Ministry of Education's annual census of universities and colleges — the official source for higher-education data.",
  },

  // ── School Education (UDISE+) ──────────────────────────────────────────────
  PTR: {
    full: "Pupil-Teacher Ratio",
    what: "Average number of students per teacher. Lower generally means more attention per student.",
    unit: "students per teacher",
  },
  "UDISE+": {
    full: "Unified District Information System for Education Plus",
    what: "Ministry of Education's annual school-level database — the official source for school enrolment, dropouts, and infrastructure.",
  },
  "Dropout Rate": {
    full: "Dropout Rate",
    what: "Share of students enrolled at a given level who leave school before completing it. Lower is better.",
    unit: "% of enrolled students",
  },

  // ── Crime (NCRB) ───────────────────────────────────────────────────────────
  IPC: {
    full: "Indian Penal Code",
    what: "The body of criminal law covering offences like theft, assault, murder, etc. Cognizable IPC crimes are those where police can act without a warrant.",
  },
  NCRB: {
    full: "National Crime Records Bureau",
    what: "Ministry of Home Affairs' agency — the official source for crime statistics in India.",
  },
  SC: { full: "Scheduled Castes",  what: "Communities historically subject to untouchability, listed in the Constitution for affirmative protections." },
  ST: { full: "Scheduled Tribes",  what: "Indigenous tribal communities listed in the Constitution for affirmative protections." },

  // ── Industry (ASI) ─────────────────────────────────────────────────────────
  ASI: {
    full: "Annual Survey of Industries",
    what: "MoSPI's yearly census of factories — the official source for industrial-output and employment data.",
  },
  GVA: {
    full: "Gross Value Added",
    what: "The extra value factories created this year: output minus the cost of materials and services they used. GDP at the industry level.",
    unit: "₹ crore",
  },
  NVA: {
    full: "Net Value Added",
    what: "GVA minus depreciation (wear-and-tear on machinery). A cleaner measure of how much new value was actually produced.",
    unit: "₹ crore",
  },
  "Total Output": {
    full: "Total Output",
    what: "Gross value of everything factories produced this year, before subtracting input costs.",
    unit: "₹ crore",
  },
  "Total Input": {
    full: "Total Input",
    what: "Value of raw materials, power, and services factories bought this year to make their products.",
    unit: "₹ crore",
  },
  "Fixed Capital": {
    full: "Fixed Capital",
    what: "Value of land, buildings, and machinery factories own. A measure of industrial base.",
    unit: "₹ crore",
  },

  // ── Fiscal ─────────────────────────────────────────────────────────────────
  GSDP: {
    full: "Gross State Domestic Product",
    what: "Total value of all goods and services produced in the state in a year — the state's economy size.",
    unit: "₹ crore",
  },
  "Debt-to-GSDP": {
    full: "Debt-to-GSDP Ratio",
    what: "State's outstanding debt as a percentage of the state's annual economic output. Lower means the debt burden is more manageable.",
    unit: "% of GSDP",
  },
  GFD: {
    full: "Gross Fiscal Deficit",
    what: "How much more the state spent than it earned this year — the gap the state had to borrow to cover. Higher = more borrowing pressure.",
    unit: "₹ crore",
  },
  "Fiscal Deficit": {
    full: "Fiscal Deficit",
    what: "Total borrowing needed: all spending minus all non-debt income (taxes, grants, asset sales). FRBM Act caps this at 3% of GSDP for states.",
    unit: "₹ crore",
  },
  "Revenue Deficit": {
    full: "Revenue Deficit",
    what: "Shortfall when day-to-day income (taxes, grants) falls short of day-to-day spending (salaries, pensions, interest). A red flag — means the state is borrowing to pay its running costs, not to invest.",
    unit: "₹ crore",
  },
  "Primary Deficit": {
    full: "Primary Deficit",
    what: "Fiscal deficit minus interest payments on existing debt. Shows whether, ignoring old debt, the state's budget is balanced.",
    unit: "₹ crore",
  },
  "Revenue Receipts": {
    full: "Revenue Receipts",
    what: "All the money the state earns in a year that doesn't create new debt — tax collections, central-government transfers, and grants.",
    unit: "₹ crore",
  },
  "Own Tax Revenue": {
    full: "Own Tax Revenue (SOTR)",
    what: "Taxes the state itself collects (GST state share, stamp duty, vehicle tax, excise). Higher = less dependence on Delhi.",
    unit: "₹ crore",
  },
  "Central Devolution": {
    full: "Central Tax Devolution",
    what: "The state's share of central taxes — set by the Finance Commission every 5 years. Currently Tamil Nadu gets ~4% of the national pool.",
    unit: "₹ crore",
  },
  "Grants-in-Aid": {
    full: "Grants-in-Aid from Centre",
    what: "Purpose-tied money from the central government for specific schemes (MGNREGA, PMAY, etc.).",
    unit: "₹ crore",
  },
  "Capital Expenditure": {
    full: "Capital Expenditure",
    what: "Money spent on long-lasting assets: roads, metro, hospitals, schools, dams. This is what builds the state's future.",
    unit: "₹ crore",
  },
  "Revenue Expenditure": {
    full: "Revenue Expenditure",
    what: "Money spent on day-to-day running: salaries, pensions, subsidies, interest on debt. Keeps the lights on.",
    unit: "₹ crore",
  },
  "Committed Expenditure": {
    full: "Committed Expenditure",
    what: "Spending the state has no choice over: salaries of existing staff, pensions of retirees, interest on existing debt. The higher this is as a share of revenue, the less room the CM has to launch new schemes.",
    unit: "₹ crore",
  },
  "Discretionary Space": {
    full: "Discretionary Space",
    what: "What's left after committed expenditure — the room the government has to fund new schemes, welfare, or investment.",
    unit: "₹ crore",
  },
  CAG: {
    full: "Comptroller & Auditor General of India",
    what: "Constitutional body that audits government accounts. 'CAG Actuals' are the audited, final numbers (vs. budget estimates which are projections).",
  },

  // ── SDG ────────────────────────────────────────────────────────────────────
  SDG: {
    full: "Sustainable Development Goals",
    what: "17 UN goals covering poverty, hunger, health, education, equality, climate, etc. NITI Aayog scores every Indian state 0–100 against them.",
  },
  "Composite Score": {
    full: "SDG Composite Score",
    what: "Weighted average across all 16 measured goals. 100 = target achieved; 0 = furthest from target.",
    unit: "0–100",
  },

  // ── Cost of Living ─────────────────────────────────────────────────────────
  LPG: { full: "Liquefied Petroleum Gas", what: "Cooking gas — sold in 5 kg, 14.2 kg (domestic) and 19 kg (commercial) cylinders." },

  // ── General ────────────────────────────────────────────────────────────────
  "vs IN": {
    full: "vs India average",
    what: "Difference between this state's value and the all-India average. Green = better than national average (context-aware: lower is better for mortality/deficit, higher is better for LFPR/GER/etc.).",
  },
  "IN:": {
    full: "India average",
    what: "The all-India figure for the same metric, for comparison.",
  },
};

// ── InfoTip component ────────────────────────────────────────────────────────

interface InfoTipProps {
  term: keyof typeof GLOSSARY | string;
  className?: string;
  iconClassName?: string;
}

/** A tiny "ⓘ" next to a metric label. Hover or tap to see full form + explanation. */
export function InfoTip({ term, className = "", iconClassName = "" }: InfoTipProps) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLSpanElement>(null);
  const entry = GLOSSARY[term as string];

  useEffect(() => {
    if (!open) return;
    const onClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onClick);
    return () => document.removeEventListener("mousedown", onClick);
  }, [open]);

  if (!entry) return null;

  return (
    <span ref={ref} className={`relative inline-flex items-center ${className}`}>
      <button
        type="button"
        aria-label={`What is ${entry.full}?`}
        onClick={(e) => {
          e.stopPropagation();
          setOpen((v) => !v);
        }}
        onMouseEnter={() => setOpen(true)}
        onMouseLeave={() => setOpen(false)}
        className={`ml-1 w-3.5 h-3.5 inline-flex items-center justify-center rounded-full border border-gray-300 text-[9px] font-bold text-gray-400 hover:text-gray-700 hover:border-gray-500 transition-colors ${iconClassName}`}
      >
        i
      </button>
      {open && (
        <span
          role="tooltip"
          className="absolute z-50 left-1/2 -translate-x-1/2 top-full mt-1.5 w-60 max-w-[80vw] bg-gray-900 text-white text-[11px] font-normal rounded-lg shadow-xl p-3 leading-snug pointer-events-none"
        >
          <span className="block font-bold text-white mb-0.5">{entry.full}</span>
          <span className="block text-gray-200">{entry.what}</span>
          {entry.unit && (
            <span className="block mt-1 text-gray-400 italic">Unit: {entry.unit}</span>
          )}
        </span>
      )}
    </span>
  );
}

/** Label + InfoTip pair. Use when space is tight inline. */
export function LabelWithTip({
  label,
  term,
  className = "",
}: {
  label: string;
  term: keyof typeof GLOSSARY | string;
  className?: string;
}) {
  return (
    <span className={`inline-flex items-center ${className}`}>
      {label}
      <InfoTip term={term} />
    </span>
  );
}
