// ---------------------------------------------------------------------------
// Lok Sabha parent constituency
// ---------------------------------------------------------------------------

export interface LsConstituencyMeta {
  ls_slug: string;
  ls_name: string;       // "Chennai South"
  ls_name_ta: string;    // "சென்னை தெற்கு"
  ls_id: number;
  confidence: "HIGH" | "MEDIUM";
}

// ---------------------------------------------------------------------------
// B.4 Pincode-to-Constituency Resolver
// ---------------------------------------------------------------------------

export interface PincodeConstituency {
  slug: string;
  name: string;       // "Mylapore"
  name_ta: string;    // "மயிலாப்பூர்"
}

export interface PincodeResult {
  pincode: string;
  district: string;
  constituencies: PincodeConstituency[];
  is_ambiguous: boolean;
}

// ---------------------------------------------------------------------------
// Usage counter / Frequently Browsed
// ---------------------------------------------------------------------------

export interface FrequentlyBrowsedItem {
  slug: string;
  name: string;
  district: string;
  view_count: number;
}

// ---------------------------------------------------------------------------
// State Vitals — Macro, Health, Water, Crops
// ---------------------------------------------------------------------------

export interface StateMacroRecord {
  doc_id: string;
  metric_id: string;
  metric_name: string;
  metric_name_ta: string;
  category: string;
  subcategory?: string;
  value: number;
  unit: string;
  comparison_national?: number;
  tn_vs_national?: string;
  year: number;
  context: string;
  alert_level?: "HIGH" | "MEDIUM" | "LOW";
  source_title: string;
  source_url: string;
  ground_truth_confidence: "HIGH" | "MEDIUM" | "LOW";
}

export interface DistrictHealthRecord {
  doc_id: string;
  metric_id: string;
  metric_name: string;
  metric_name_ta: string;
  district_slug: string | null;
  district_name: string | null;
  metric_scope: "state" | "district";
  value: number;
  unit: string;
  national_average?: number;
  tn_vs_national?: string;
  year: number;
  category: string;
  alert_level?: "HIGH" | "MEDIUM" | "LOW";
  policy_gap?: boolean;
  context: string;
  source_title: string;
  source_url: string;
  ground_truth_confidence: "HIGH" | "MEDIUM" | "LOW";
}

export interface DistrictRoadSafety {
  tn_percentile?: number;
  doc_id: string;
  district_slug: string;
  district_name: string;
  year_range: string;
  population_lakhs: number;
  accidents_2021: number;
  deaths_2021: number;
  death_rate_per_lakh_2021: number;
  accidents_2022: number;
  deaths_2022: number;
  death_rate_per_lakh_2022: number;
  accidents_2023: number;
  deaths_2023: number;
  death_rate_per_lakh_2023: number;
  road_safety_level: "HIGH_RISK" | "MEDIUM_RISK" | "LOW_RISK";
  trend_2021_2023: "IMPROVING" | "STABLE" | "WORSENING";
  trend_pct_change: number;
  context: string;
  source_url: string;
  ground_truth_confidence: "HIGH" | "MEDIUM" | "LOW";
}

export interface DistrictCrimeIndex {
  tn_percentile?: number;
  doc_id: string;
  district_slug: string;
  district_name: string;
  year: number;
  population_lakhs: number;
  ipc_crimes_total: number;
  ipc_crime_rate_per_lakh: number;
  crime_index_level: "HIGH" | "MEDIUM" | "LOW";
  murder_incidents: number;
  murder_rate_per_lakh: number;
  rape_incidents: number;
  rape_rate_per_lakh: number;
  assault_on_women_incidents: number;
  assault_on_women_rate_per_lakh: number;
  theft_incidents: number;
  theft_rate_per_lakh: number;
  robbery_incidents: number;
  robbery_rate_per_lakh: number;
  negligence_deaths: number;
  negligence_death_rate_per_lakh: number;
  suicides_total: number;
  suicides_male: number;
  suicides_female: number;
  context: string;
  source_url: string;
  ground_truth_confidence: "HIGH" | "MEDIUM" | "LOW";
}

// ---------------------------------------------------------------------------
// Ward & Local Body Mapping (LGD data — urban wards only)
// ---------------------------------------------------------------------------

export interface WardLocalBody {
  name: string;        // "Greater Chennai Corporation"
  type: string;        // "Municipal Corporation" | "Municipality" | "Town Panchayat"
  ward_count: number;
  ward_numbers?: number[];  // sorted ward numbers from LGD (non-GCC bodies only)
}

export interface WardMapping {
  constituency_slug: string;
  constituency_name: string;
  eci_code: number;
  total_urban_wards: number;
  local_bodies: WardLocalBody[];
  data_date: string;   // "2026-04-02"
}

export interface UlbCouncillor {
  local_body_slug: string;
  local_body_name: string;
  ward_number: number;
  zone_number: number;
  zone_name: string;
  councillor_name: string;
  party: string;
  party_full: string;
  sex: string;
  age: number | null;
  ward_reservation: string;
}

export interface UlbHead {
  local_body_slug: string;
  local_body_name: string;
  local_body_type: string;
  head_title: string;   // "Mayor" | "Chairman" | "Chairperson"
  head_name: string;
  party: string;
  party_full: string;
  election_year: number;
  notes?: string;
}

export interface DistrictWaterRisk {
  tn_percentile?: number;
  doc_id: string;
  district_slug: string;
  district_name: string;
  risk_level: "EXTREMELY_HIGH" | "HIGH" | "MEDIUM" | "LOW" | "FLOOD_PRONE";
  risk_label_en: string;
  risk_label_ta: string;
  water_stress_score: number;   // 0–5
  avg_annual_rainfall_mm: number;
  context: string;
  policy_implication: string;
  ground_truth_confidence: "HIGH" | "MEDIUM" | "LOW";
}

export interface CropEconomicsRecord {
  doc_id: string;
  crop_name: string;
  crop_name_ta: string;
  crop_type: string;
  season: string;
  marketing_year: string;
  msp_per_quintal: number | null;
  frp_per_quintal?: number | null;
  a2_fl_cost_per_quintal: number;
  c2_cost_per_quintal: number;
  profit_over_a2fl_pct: number;
  frp_applicable: boolean;
  primary_tn_districts: string[];
  context: string;
  policy_tension?: string;
  source_title: string;
  source_url: string;
  ground_truth_confidence: "HIGH" | "MEDIUM" | "LOW";
}

export interface StateVitalsData {
  economy?: StateMacroRecord[];
  health?: DistrictHealthRecord[];
  water?: DistrictWaterRisk[];
  crops?: CropEconomicsRecord[];
}

// ---------------------------------------------------------------------------
// Constituency Drill-Down types
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// B.6 Criminal case detail (populated by affidavit_ingest.py)
// ---------------------------------------------------------------------------

export type CriminalCaseStatus = "Pending" | "Dismissed" | "Convicted";

export interface CriminalCase {
  case_id?: string;
  ipc_sections: string[];          // ["302", "307"] — raw section numbers
  act: string;                     // "IPC" | "POCSO" | "Prevention of Corruption Act"
  description: string;             // Human-readable crime description
  status: CriminalCaseStatus;
  court?: string;                  // "Sessions Court, Chennai"
  year_filed?: number;
  is_serious: boolean;             // Pre-computed by ingest using isSeriousCrime()
}

export interface MlaRecord {
  doc_id: string;
  mla_name: string;
  constituency: string;
  constituency_id?: number;
  constituency_slug?: string;
  party: string;                // raw party name e.g. "DMK"
  party_id?: string;            // slug e.g. "dmk" — may be absent in older docs
  photo_url?: string | null;    // optional image URL; UI falls back to initials
  criminal_cases_total: number;
  criminal_severity: "CLEAN" | "MINOR" | "MODERATE" | "SERIOUS";
  assets_cr: number | null;
  liabilities_cr: number | null;
  net_assets_cr: number | null;
  is_crorepati: boolean;
  education: string;
  education_tier: string;
  election_year: number;
  source_url: string;
  ground_truth_confidence: "HIGH" | "MEDIUM" | "LOW";
  // B.5 affidavit enrichment (optional — populated after affidavit_ingest.py run)
  movable_assets_cr?: number | null;
  immovable_assets_cr?: number | null;
  institution_name?: string | null;
  source_pdf?: string | null;
  // B.6 structured criminal case details (optional — populated by affidavit_ingest.py)
  criminal_cases?: CriminalCase[];
}

export interface SocioMetric {
  metric_id: string;
  category: string;
  metric_name: string;
  tamil_name: string;
  value: number;
  unit: string;
  year: number;
  survey: string;
  national_average: number | null;
  tn_vs_national: string | null;
  context: string;
  district_id?: string | null;
  district_slug?: string | null;
  district_name?: string | null;
  metric_scope?: "district" | "state";
  alert_level?: string | null;
  ground_truth_confidence: "HIGH" | "MEDIUM" | "LOW";
  tn_percentile?: number;   // % of TN districts this value is better than
}

export interface ConstituencyMeta {
  name: string;           // "HARUR (SC)"
  district: string;       // "DHARMAPURI"
  district_slug: string;  // "dharmapuri"
  constituency_id: number;
}

// ---------------------------------------------------------------------------
// Manifesto types
// ---------------------------------------------------------------------------

export type PromiseStatus = "Proposed" | "Fulfilled" | "Partial" | "Abandoned" | "Historical";

export type Pillar =
  | "Agriculture"
  | "Education"
  | "TASMAC & Revenue"
  | "Women's Welfare"
  | "Infrastructure";

export type StanceVibe =
  | "Welfare-centric"
  | "Infrastructure-heavy"
  | "Revenue-focused"
  | "Populist"
  | "Reform-oriented"
  | "Women-focused"
  | "Farmer-focused";

export interface ManifestoPromise {
  doc_id: string;
  party_id: string;           // "dmk" | "aiadmk" | "bjp" | "pmk" etc.
  party_name: string;
  party_color: string;        // Tailwind bg color class
  category: Pillar;
  promise_text_en: string;
  promise_text_ta: string;
  target_year: number;        // 2026 for upcoming; 2021 for current mandate
  status: PromiseStatus;
  stance_vibe: StanceVibe;
  amount_mentioned?: string;  // e.g. "₹1,000 crore", "₹500/month"
  scheme_name?: string;       // Official scheme name if named
  manifesto_pdf_url: string;  // Link to official party manifesto PDF
  manifesto_pdf_page?: number;
  source_notes?: string;
  ground_truth_confidence: "HIGH" | "MEDIUM" | "LOW";
  _uploaded_at?: string;  // ISO timestamp — set by Firestore loader; absent in seed data
}

export interface PartyMeta {
  party_id: string;
  party_name: string;
  tamil_name: string;
  color: string;              // Tailwind bg class
  text_color: string;         // Tailwind text class
  border_color: string;       // Tailwind border class
  manifesto_pdf_url: string;
  manifesto_year: number;
}

export const PARTIES: Record<string, PartyMeta> = {
  dmk: {
    party_id: "dmk",
    party_name: "DMK",
    tamil_name: "திமுக",
    color: "bg-red-600",
    text_color: "text-red-600",
    border_color: "border-red-600",
    manifesto_pdf_url: "https://www.dmk.in/manifesto2021.pdf",
    manifesto_year: 2021,
  },
  aiadmk: {
    party_id: "aiadmk",
    party_name: "AIADMK",
    tamil_name: "அதிமுக",
    color: "bg-green-700",
    text_color: "text-green-700",
    border_color: "border-green-700",
    manifesto_pdf_url: "https://www.aiadmk.com/manifesto2021.pdf",
    manifesto_year: 2021,
  },
  bjp: {
    party_id: "bjp",
    party_name: "BJP",
    tamil_name: "பாஜக",
    color: "bg-orange-500",
    text_color: "text-orange-500",
    border_color: "border-orange-500",
    manifesto_pdf_url: "https://www.bjp4india.com/manifesto/tamil-nadu-2026",
    manifesto_year: 2026,
  },
  pmk: {
    party_id: "pmk",
    party_name: "PMK",
    tamil_name: "பாமக",
    color: "bg-yellow-500",
    text_color: "text-yellow-600",
    border_color: "border-yellow-500",
    manifesto_pdf_url: "https://www.pmk.in/manifesto2026.pdf",
    manifesto_year: 2026,
  },
  inc: {
    party_id: "inc",
    party_name: "INC",
    tamil_name: "காங்கிரஸ்",
    color: "bg-blue-600",
    text_color: "text-blue-600",
    border_color: "border-blue-600",
    manifesto_pdf_url: "https://www.inc.in/manifesto",
    manifesto_year: 2021,
  },
};

export const PILLARS: Pillar[] = [
  "Agriculture",
  "Education",
  "TASMAC & Revenue",
  "Women's Welfare",
  "Infrastructure",
];

export const PILLAR_META: Record<Pillar, { icon: string; tamil: string; description: string }> = {
  Agriculture: {
    icon: "🌾",
    tamil: "விவசாயம்",
    description: "Farmer welfare, crop insurance, MSP, irrigation",
  },
  Education: {
    icon: "📚",
    tamil: "கல்வி",
    description: "Schools, colleges, midday meals, scholarships",
  },
  "TASMAC & Revenue": {
    icon: "🏛️",
    tamil: "தாஸ்மாக் & வருவாய்",
    description: "Liquor policy, state revenue, taxation",
  },
  "Women's Welfare": {
    icon: "👩",
    tamil: "பெண்கள் நலன்",
    description: "SHGs, safety, reservations, maternity benefits",
  },
  Infrastructure: {
    icon: "🏗️",
    tamil: "உள்கட்டமைப்பு",
    description: "Roads, metro, power, water, housing",
  },
};

export const VIBE_META: Record<StanceVibe, { label: string; bg: string; text: string }> = {
  "Welfare-centric":     { label: "நலன்புரி",         bg: "bg-blue-100",   text: "text-blue-800" },
  "Infrastructure-heavy":{ label: "உள்கட்டமைப்பு",  bg: "bg-slate-100",  text: "text-slate-800" },
  "Revenue-focused":     { label: "வருவாய் கவனம்",   bg: "bg-amber-100",  text: "text-amber-800" },
  "Populist":            { label: "மக்கள் சார்பு",   bg: "bg-pink-100",   text: "text-pink-800" },
  "Reform-oriented":     { label: "சீர்திருத்தம்",   bg: "bg-purple-100", text: "text-purple-800" },
  "Women-focused":       { label: "பெண்கள் கவனம்",   bg: "bg-rose-100",   text: "text-rose-800" },
  "Farmer-focused":      { label: "விவசாயி கவனம்",   bg: "bg-green-100",  text: "text-green-800" },
};

// ---------------------------------------------------------------------------
// Term coalition config — ruling / opposition / also contested per term
// ---------------------------------------------------------------------------

export interface CoalitionParty {
  id: string;
  name: string;
  name_ta: string;
  bg: string;        // unselected bg (pastel)
  text: string;      // unselected text
  bg_active: string; // selected bg (solid, white text assumed)
}

export interface TermCoalition {
  ruling_label_en: string;
  ruling_label_ta: string;
  ruling: CoalitionParty[];      // first = main ruling party (default selection)
  opposition: CoalitionParty[];  // first = main opposition party
  others?: CoalitionParty[];     // other parties that contested
  // For upcoming elections where no winner is known yet — use flat party list instead
  is_upcoming?: boolean;
  contesting?: CoalitionParty[]; // all parties on equal footing; first = default selection
}

const _cp = (
  id: string, name: string, name_ta: string,
  bg: string, text: string, bg_active: string
): CoalitionParty => ({ id, name, name_ta, bg, text, bg_active });

const _DMK  = _cp("dmk",    "DMK",    "திமுக",                    "bg-red-100",    "text-red-800",    "bg-red-600");
const _ADMK = _cp("aiadmk", "AIADMK", "அதிமுக",                   "bg-green-100",  "text-green-800",  "bg-green-700");
const _INC  = _cp("inc",    "INC",    "காங்கிரஸ்",                "bg-blue-100",   "text-blue-800",   "bg-blue-600");
const _BJP  = _cp("bjp",    "BJP",    "பாஜக",                     "bg-orange-100", "text-orange-800", "bg-orange-500");
const _PMK  = _cp("pmk",    "PMK",    "பாமக",                     "bg-yellow-100", "text-yellow-800", "bg-yellow-500");
const _CPI  = _cp("cpi",    "CPI",    "CPI",                      "bg-rose-100",   "text-rose-800",   "bg-rose-600");
const _CPIM = _cp("cpim",   "CPI(M)", "CPI(M)",                   "bg-rose-100",   "text-rose-800",   "bg-rose-700");
const _VCK  = _cp("vck",    "VCK",    "விடுதலைச் சிறுத்தைகள்",  "bg-purple-100", "text-purple-800", "bg-purple-600");
const _DMDK = _cp("dmdk",   "DMDK",   "DMDK",                     "bg-slate-100",  "text-slate-700",  "bg-slate-500");
const _MDMK = _cp("mdmk",   "MDMK",   "MDMK",                     "bg-teal-100",   "text-teal-700",   "bg-teal-600");
const _NTK  = _cp("ntk",    "NTK",    "நாம் தமிழர்",             "bg-yellow-100", "text-yellow-800", "bg-yellow-600");
const _TVK  = _cp("tvk",    "TVK",    "தமிழக வெற்றி கழகம்",     "bg-sky-100",    "text-sky-800",    "bg-sky-600");

export const TERM_COALITIONS: Record<number, TermCoalition> = {
  2006: {
    ruling_label_en: "Secular Progressive Alliance (DMK-led)",
    ruling_label_ta: "மதச்சார்பற்ற முற்போக்கு கூட்டணி (திமுக தலைமை)",
    ruling:     [_DMK, _INC, _CPI, _CPIM, _PMK, _VCK],
    opposition: [_ADMK],
    others:     [_BJP],
  },
  2011: {
    ruling_label_en: "AIADMK-led Alliance",
    ruling_label_ta: "அதிமுக தலைமை கூட்டணி",
    ruling:     [_ADMK, _DMDK],
    opposition: [_DMK, _INC, _VCK, _PMK, _CPI, _CPIM],
    others:     [_BJP],
  },
  2016: {
    ruling_label_en: "AIADMK (contested alone)",
    ruling_label_ta: "அதிமுக (தனியாக போட்டியிட்டது)",
    ruling:     [_ADMK],
    opposition: [_DMK, _INC, _VCK, _CPI, _CPIM, _MDMK],
    others:     [_BJP, _PMK, _DMDK],
  },
  2021: {
    ruling_label_en: "Secular Progressive Alliance (DMK-led)",
    ruling_label_ta: "மதச்சார்பற்ற முற்போக்கு கூட்டணி (திமுக தலைமை)",
    ruling:     [_DMK, _INC, _CPI, _CPIM, _VCK, _MDMK],
    opposition: [_ADMK, _BJP, _PMK, _DMDK],
  },
  2026: {
    ruling_label_en: "Tamil Nadu Assembly Elections 2026",
    ruling_label_ta: "தமிழ்நாடு சட்டமன்றத் தேர்தல் 2026",
    ruling:     [],
    opposition: [],
    is_upcoming: true,
    contesting: [_DMK, _ADMK, _TVK, _NTK, _BJP, _PMK, _INC, _CPI, _CPIM, _VCK, _MDMK],
  },
};

export const STATUS_META: Record<PromiseStatus, { label_en: string; label_ta: string; bg: string; text: string }> = {
  Proposed:    { label_en: "Proposed",    label_ta: "முன்மொழிவு",   bg: "bg-sky-100",    text: "text-sky-800" },
  Fulfilled:   { label_en: "Fulfilled",   label_ta: "நிறைவேற்றப்பட்டது", bg: "bg-emerald-100", text: "text-emerald-800" },
  Partial:     { label_en: "Partial",     label_ta: "பகுதி நிறைவு",  bg: "bg-yellow-100", text: "text-yellow-800" },
  Abandoned:   { label_en: "Abandoned",   label_ta: "கைவிடப்பட்டது", bg: "bg-red-100",    text: "text-red-800" },
  Historical:  { label_en: "Historical",  label_ta: "வரலாற்று வாக்குறுதி", bg: "bg-gray-100",   text: "text-gray-700" },
};
