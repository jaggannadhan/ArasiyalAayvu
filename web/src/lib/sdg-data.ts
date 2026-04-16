// ---------------------------------------------------------------------------
// SDG India Index 2023-24 — Tamil Nadu Focus
// Source: NITI Aayog SDG India Index 2023-24 (July 2024)
// TN Overall Score: 78/100 | National Rank: 3rd
// ---------------------------------------------------------------------------

export type SDGPerformance = "strength" | "moderate" | "needs_attention";

export interface SDGMetric {
  label: string;
  label_ta: string;
  value: string;
  context?: string;
  trend: "up" | "down" | "stable";
  // Is an upward trend good for this metric? (e.g. "up" is bad for poverty rate)
  good_direction: "up" | "down";
}

export interface SDGGoal {
  id: number;
  name: string;
  name_ta: string;
  description_en: string;
  description_ta: string;
  icon: string;
  // Tailwind bg color approximating UN SDG official palette
  color_bg: string;
  color_text: string;
  tn_performance: SDGPerformance;
  tn_score?: number;       // TN's goal-level score (0-100) from SDG Index
  india_score?: number;    // National average for comparison
  metrics: SDGMetric[];
}

export const SDG_GOALS: SDGGoal[] = [
  {
    id: 1,
    name: "No Poverty",
    name_ta: "வறுமையின்மை",
    description_en: "End poverty in all its forms everywhere",
    description_ta: "எல்லா வடிவங்களிலும் வறுமையை ஒழிக்கவும்",
    icon: "🏠",
    color_bg: "bg-red-600",
    color_text: "text-white",
    tn_performance: "strength",
    tn_score: 80,
    india_score: 72,
    metrics: [
      { label: "Multidimensional Poverty", label_ta: "பன்முக வறுமை விகிதம்", value: "10.8%", trend: "down", good_direction: "down", context: "Below national average of 14.96%" },
      { label: "PMAY Housing Beneficiaries", label_ta: "வீட்டுவசதி பயனாளிகள்", value: "12.6 lakh", trend: "up", good_direction: "up", context: "Rural housing scheme coverage" },
      { label: "Below Poverty Line Households", label_ta: "வறுமை இரேகைக்கு கீழுள்ள குடும்பங்கள்", value: "11.3%", trend: "down", good_direction: "down" },
    ],
  },
  {
    id: 2,
    name: "Zero Hunger",
    name_ta: "பசியின்மை",
    description_en: "End hunger, achieve food security and improved nutrition",
    description_ta: "பசியை ஒழிக்கவும், உணவுப் பாதுகாப்பு மற்றும் ஊட்டச்சத்தை மேம்படுத்தவும்",
    icon: "🌾",
    color_bg: "bg-amber-500",
    color_text: "text-white",
    tn_performance: "needs_attention",
    tn_score: 60,
    india_score: 54,
    metrics: [
      { label: "Child Stunting (under 5)", label_ta: "குழந்தை வளர்ச்சி குறைபாடு", value: "25.5%", trend: "down", good_direction: "down", context: "NFHS-5; improvement from 27.1% (NFHS-4)" },
      { label: "Child Wasting", label_ta: "குழந்தை மெலிவு", value: "19.7%", trend: "up", good_direction: "down", context: "NFHS-5; slight worsening needs attention" },
      { label: "Anaemia in Women (15-49)", label_ta: "பெண்களுக்கு இரத்த சோகை", value: "46.9%", trend: "up", good_direction: "down", context: "NFHS-5; rising trend" },
    ],
  },
  {
    id: 3,
    name: "Good Health & Well-Being",
    name_ta: "நல்ல ஆரோக்கியம்",
    description_en: "Ensure healthy lives and promote well-being for all",
    description_ta: "அனைவருக்கும் ஆரோக்கியமான வாழ்க்கை மற்றும் நல்வாழ்வை உறுதிப்படுத்தவும்",
    icon: "🏥",
    color_bg: "bg-green-600",
    color_text: "text-white",
    tn_performance: "strength",
    tn_score: 82,
    india_score: 68,
    metrics: [
      { label: "Neonatal Mortality Rate", label_ta: "நவஜாத குழந்தை இறப்பு விகிதம்", value: "10 per 1,000", trend: "down", good_direction: "down", context: "Below national rate of 20; NFHS-5" },
      { label: "Maternal Mortality Ratio", label_ta: "தாய் மரண விகிதம்", value: "54 per 1L births", trend: "down", good_direction: "down", context: "Well below national MMR of 97" },
      { label: "Institutional Deliveries", label_ta: "மருத்துவமனை பிரசவங்கள்", value: "98.7%", trend: "up", good_direction: "up" },
    ],
  },
  {
    id: 4,
    name: "Quality Education",
    name_ta: "தரமான கல்வி",
    description_en: "Ensure inclusive and equitable quality education for all",
    description_ta: "அனைவருக்கும் தரமான, சமத்துவமான கல்வியை உறுதிப்படுத்தவும்",
    icon: "📚",
    color_bg: "bg-red-700",
    color_text: "text-white",
    tn_performance: "strength",
    tn_score: 84,
    india_score: 73,
    metrics: [
      { label: "Net Enrolment Ratio (elementary)", label_ta: "ஆரம்பக் கல்வி சேர்வு விகிதம்", value: "97.9%", trend: "up", good_direction: "up", context: "UDISE+ 2022-23" },
      { label: "Literacy Rate", label_ta: "எழுத்தறிவு விகிதம்", value: "82.9%", trend: "up", good_direction: "up", context: "Census 2011; expected higher by 2026" },
      { label: "Learning Outcomes (Gr.8 Maths)", label_ta: "கற்றல் தர மதிப்பீடு (8ம் வகுப்பு)", value: "54%", trend: "up", good_direction: "up", context: "ASER 2024 — students doing division" },
    ],
  },
  {
    id: 5,
    name: "Gender Equality",
    name_ta: "பாலின சமத்துவம்",
    description_en: "Achieve gender equality and empower all women and girls",
    description_ta: "பாலின சமத்துவத்தை அடையவும், பெண்கள் மற்றும் சிறுமியரை மேம்படுத்தவும்",
    icon: "♀️",
    color_bg: "bg-orange-600",
    color_text: "text-white",
    tn_performance: "needs_attention",
    tn_score: 65,
    india_score: 56,
    metrics: [
      { label: "Female Labour Force Participation", label_ta: "பெண் தொழிலாளர் பங்கேற்பு", value: "32.7%", trend: "up", good_direction: "up", context: "Rising but still below potential; PLFS 2023" },
      { label: "Sex Ratio at Birth", label_ta: "பிறப்பில் பாலின விகிதம்", value: "978 per 1,000 males", trend: "up", good_direction: "up", context: "NFHS-5; improving" },
      { label: "Women in State Assembly", label_ta: "சட்டமன்றத்தில் பெண்கள்", value: "10.7%", trend: "up", good_direction: "up", context: "2021 election — 26 of 234 seats" },
    ],
  },
  {
    id: 6,
    name: "Clean Water & Sanitation",
    name_ta: "சுத்தமான நீர் & சுகாதாரம்",
    description_en: "Ensure availability of water and sanitation for all",
    description_ta: "அனைவருக்கும் தண்ணீர் மற்றும் சுகாதாரம் உறுதிப்படுத்தவும்",
    icon: "💧",
    color_bg: "bg-cyan-500",
    color_text: "text-white",
    tn_performance: "moderate",
    tn_score: 71,
    india_score: 67,
    metrics: [
      { label: "Households with Piped Water", label_ta: "குழாய் நீர் வசதி உள்ள குடும்பங்கள்", value: "85.4%", trend: "up", good_direction: "up", context: "Jal Jeevan Mission progress" },
      { label: "Open Defecation Free Villages", label_ta: "திறந்தவெளி மலஜலம் இல்லாத கிராமங்கள்", value: "99.2%", trend: "up", good_direction: "up", context: "Swachh Bharat verification" },
      { label: "Water-Stressed Districts", label_ta: "நீர் வறட்சி மாவட்டங்கள்", value: "18 of 38", trend: "stable", good_direction: "down", context: "Annual water risk assessment" },
    ],
  },
  {
    id: 7,
    name: "Affordable & Clean Energy",
    name_ta: "மலிவான & சுத்தமான ஆற்றல்",
    description_en: "Ensure access to affordable, reliable, and clean energy",
    description_ta: "அனைவருக்கும் மலிவான, நம்பகமான சுத்தமான ஆற்றலை உறுதிப்படுத்தவும்",
    icon: "⚡",
    color_bg: "bg-yellow-500",
    color_text: "text-gray-900",
    tn_performance: "strength",
    tn_score: 91,
    india_score: 96,
    metrics: [
      { label: "Household Electrification", label_ta: "மின்சார இணைப்பு உள்ள குடும்பங்கள்", value: "99.6%", trend: "up", good_direction: "up", context: "Near-universal coverage" },
      { label: "Renewable Energy Capacity", label_ta: "புதுப்பிக்கத்தக்க ஆற்றல் திறன்", value: "25,000+ MW", trend: "up", good_direction: "up", context: "Solar + Wind installed; TN leads nationally" },
      { label: "LPG Coverage", label_ta: "எல்பிஜி இணைப்பு", value: "94.2%", trend: "up", good_direction: "up", context: "Ujjwala + state scheme beneficiaries" },
    ],
  },
  {
    id: 8,
    name: "Decent Work & Economic Growth",
    name_ta: "நல்ல வேலை & பொருளாதார வளர்ச்சி",
    description_en: "Promote inclusive and sustainable economic growth",
    description_ta: "உள்ளடக்கிய நிலையான பொருளாதார வளர்ச்சியை மேம்படுத்தவும்",
    icon: "💼",
    color_bg: "bg-rose-800",
    color_text: "text-white",
    tn_performance: "moderate",
    tn_score: 72,
    india_score: 69,
    metrics: [
      { label: "GSDP Growth Rate", label_ta: "மாநில மொத்த உள்நாட்டு உற்பத்தி வளர்ச்சி", value: "8.2%", trend: "up", good_direction: "up", context: "FY2023-24 at constant prices" },
      { label: "Unemployment Rate (PLFS)", label_ta: "வேலையின்மை விகிதம்", value: "5.3%", trend: "down", good_direction: "down", context: "Usual Status; PLFS 2022-23" },
      { label: "MSME Establishments", label_ta: "சிறு, நுண் நிறுவனங்கள்", value: "96 lakh+", trend: "up", good_direction: "up", context: "4th highest in India by count" },
    ],
  },
  {
    id: 9,
    name: "Industry, Innovation & Infrastructure",
    name_ta: "தொழில், கண்டுபிடிப்பு & உள்கட்டமைப்பு",
    description_en: "Build resilient infrastructure and foster innovation",
    description_ta: "வலுவான உள்கட்டமைப்பை உருவாக்கவும், கண்டுபிடிப்பை ஊக்கப்படுத்தவும்",
    icon: "🏭",
    color_bg: "bg-orange-500",
    color_text: "text-white",
    tn_performance: "strength",
    tn_score: 79,
    india_score: 65,
    metrics: [
      { label: "National Highways Length", label_ta: "தேசிய நெடுஞ்சாலை நீளம்", value: "5,006 km", trend: "up", good_direction: "up", context: "Among top states for NH network" },
      { label: "Gross Industrial Output", label_ta: "மொத்த தொழில்துறை உற்பத்தி", value: "₹16.8L Cr", trend: "up", good_direction: "up", context: "TN = 2nd largest manufacturing state" },
      { label: "Patent Applications Filed", label_ta: "காப்புரிமை விண்ணப்பங்கள்", value: "4,800+", trend: "up", good_direction: "up", context: "Annual; strong engineering college ecosystem" },
    ],
  },
  {
    id: 10,
    name: "Reduced Inequalities",
    name_ta: "சமத்துவமின்மை குறைப்பு",
    description_en: "Reduce inequality within and among countries",
    description_ta: "நாடுகளுக்கு உள்ளும் இடையேயும் சமத்துவமின்மையை குறைக்கவும்",
    icon: "⚖️",
    color_bg: "bg-pink-700",
    color_text: "text-white",
    tn_performance: "moderate",
    tn_score: 68,
    india_score: 61,
    metrics: [
      { label: "Rural-Urban Consumption Gap", label_ta: "கிராம-நகர நுகர்வு இடைவெளி", value: "1.4x", trend: "down", good_direction: "down", context: "Lower than national average of 1.7x" },
      { label: "SC/ST Poverty Headcount", label_ta: "தாழ்த்தப்பட்டோர் வறுமை", value: "18.4%", trend: "down", good_direction: "down", context: "NFHS-5 MPI by social group" },
      { label: "Gini Coefficient (consumption)", label_ta: "ஜினி குணகம்", value: "0.34", trend: "stable", good_direction: "down", context: "Moderate inequality; HCES 2022-23" },
    ],
  },
  {
    id: 11,
    name: "Sustainable Cities & Communities",
    name_ta: "நிலையான நகரங்கள் & சமூகங்கள்",
    description_en: "Make cities inclusive, safe, resilient and sustainable",
    description_ta: "நகரங்களை உள்ளடக்கிய, பாதுகாப்பான, தன்னிலையான இடங்களாக மாற்றவும்",
    icon: "🏙️",
    color_bg: "bg-amber-600",
    color_text: "text-white",
    tn_performance: "moderate",
    tn_score: 70,
    india_score: 63,
    metrics: [
      { label: "Urban Population Share", label_ta: "நகர்ப்புற மக்கள் தொகை விகிதம்", value: "48.4%", trend: "up", good_direction: "up", context: "Census 2011; likely ~52% by 2026" },
      { label: "Households with Pucca Dwelling", label_ta: "நிரந்தர வீடுகளில் குடும்பங்கள்", value: "79.5%", trend: "up", good_direction: "up" },
      { label: "Road Accident Deaths (per lakh)", label_ta: "சாலை விபத்து மரணங்கள்", value: "9.1", trend: "down", good_direction: "down", context: "2023; improving but requires attention" },
    ],
  },
  {
    id: 12,
    name: "Responsible Consumption & Production",
    name_ta: "பொறுப்பான நுகர்வு & உற்பத்தி",
    description_en: "Ensure sustainable consumption and production patterns",
    description_ta: "நிலையான நுகர்வு மற்றும் உற்பத்தி முறைகளை உறுதிப்படுத்தவும்",
    icon: "♻️",
    color_bg: "bg-yellow-700",
    color_text: "text-white",
    tn_performance: "strength",
    tn_score: 83,
    india_score: 89,
    metrics: [
      { label: "Municipal Solid Waste Processed", label_ta: "நகர திட கழிவுகள் செயலாக்கம்", value: "72%", trend: "up", good_direction: "up", context: "SBM-Urban 2.0 progress" },
      { label: "E-Waste Registered Recyclers", label_ta: "மின்னணு கழிவு மறுசுழற்சி", value: "312+", trend: "up", good_direction: "up" },
      { label: "Single-Use Plastic Ban Coverage", label_ta: "ஒற்றை-பயன்பாட்டு பிளாஸ்டிக் தடை", value: "Active since 2022", trend: "stable", good_direction: "up" },
    ],
  },
  {
    id: 13,
    name: "Climate Action",
    name_ta: "காலநிலை நடவடிக்கை",
    description_en: "Take urgent action to combat climate change and its impacts",
    description_ta: "காலநிலை மாற்றத்தை எதிர்கொள்ள அவசர நடவடிக்கை எடுக்கவும்",
    icon: "🌍",
    color_bg: "bg-green-800",
    color_text: "text-white",
    tn_performance: "moderate",
    tn_score: 67,
    india_score: 63,
    metrics: [
      { label: "Renewable Energy Share", label_ta: "புதுப்பிக்கத்தக்க ஆற்றல் பங்கு", value: "47% of capacity", trend: "up", good_direction: "up", context: "Solar + Wind; among top 3 states" },
      { label: "Forest & Tree Cover", label_ta: "காடு மற்றும் மரங்கள் மூடல்", value: "20.5% of area", trend: "stable", good_direction: "up", context: "FSI 2023; below 33% national target" },
      { label: "Cyclone-Prone Coastline", label_ta: "சுழல்காற்று அபாய கடற்கரை", value: "1,076 km", trend: "stable", good_direction: "down", context: "Requires robust disaster preparedness" },
    ],
  },
  {
    id: 14,
    name: "Life Below Water",
    name_ta: "கடலினுள் உயிரினங்கள்",
    description_en: "Conserve and sustainably use oceans, seas and marine resources",
    description_ta: "கடல்கள் மற்றும் கடல் வளங்களை பாதுகாக்கவும்",
    icon: "🐟",
    color_bg: "bg-blue-600",
    color_text: "text-white",
    tn_performance: "moderate",
    tn_score: 63,
    india_score: 58,
    metrics: [
      { label: "Marine Fishing Communities", label_ta: "கடல் மீனவ சமூகங்கள்", value: "9 lakh+ fishers", trend: "stable", good_direction: "up", context: "Highest in India by marine fishers" },
      { label: "Marine Protected Areas", label_ta: "கடல் பாதுகாப்பு பகுதிகள்", value: "4 MPAs", trend: "stable", good_direction: "up", context: "Gulf of Mannar, Palk Bay ecosystem" },
      { label: "Coastal Erosion Risk Length", label_ta: "கடற்கரை அரிப்பு அபாயம்", value: "41% of coastline", trend: "stable", good_direction: "down" },
    ],
  },
  {
    id: 15,
    name: "Life on Land",
    name_ta: "நிலத்தில் உயிரினங்கள்",
    description_en: "Protect, restore and promote sustainable use of terrestrial ecosystems",
    description_ta: "நிலம் மற்றும் காட்டு சூழலியல்களை பாதுகாக்கவும்",
    icon: "🌳",
    color_bg: "bg-lime-600",
    color_text: "text-white",
    tn_performance: "moderate",
    tn_score: 66,
    india_score: 62,
    metrics: [
      { label: "Forest Cover", label_ta: "வனப் பகுதி", value: "17.06% of TN area", trend: "stable", good_direction: "up", context: "FSI 2023; moderate coverage" },
      { label: "Wildlife Sanctuaries", label_ta: "வனவிலங்கு சரணாலயங்கள்", value: "31 sanctuaries", trend: "stable", good_direction: "up" },
      { label: "Degraded Land (Est.)", label_ta: "சீரழிந்த நிலம்", value: "8.1 lakh ha", trend: "stable", good_direction: "down", context: "Wastelands atlas; needs afforestation" },
    ],
  },
  {
    id: 16,
    name: "Peace, Justice & Strong Institutions",
    name_ta: "அமைதி, நீதி & வலுவான நிறுவனங்கள்",
    description_en: "Promote peaceful, inclusive societies and strong institutions",
    description_ta: "அமைதியான, உள்ளடக்கிய சமுதாயங்களையும் வலுவான நிறுவனங்களையும் மேம்படுத்தவும்",
    icon: "⚖️",
    color_bg: "bg-blue-800",
    color_text: "text-white",
    tn_performance: "moderate",
    tn_score: 69,
    india_score: 65,
    metrics: [
      { label: "IPC Crime Rate (per lakh pop.)", label_ta: "IPC குற்ற விகிதம்", value: "194", trend: "stable", good_direction: "down", context: "NCRB 2022; moderate among southern states" },
      { label: "Case Disposal Rate (High Court)", label_ta: "வழக்கு தீர்வு விகிதம்", value: "98.4%", trend: "up", good_direction: "up", context: "Madras HC case disposal 2023" },
      { label: "RTI Applications Filed", label_ta: "தகவல் உரிமை விண்ணப்பங்கள்", value: "82,000+/year", trend: "up", good_direction: "up", context: "Active civic engagement" },
    ],
  },
  {
    id: 17,
    name: "Partnerships for the Goals",
    name_ta: "இலக்குகளுக்கான கூட்டாண்மை",
    description_en: "Strengthen the means of implementation and global partnership",
    description_ta: "செயல்படுத்தும் வழிமுறைகளையும் உலகளாவிய கூட்டாண்மையையும் வலுப்படுத்தவும்",
    icon: "🤝",
    color_bg: "bg-blue-900",
    color_text: "text-white",
    tn_performance: "moderate",
    tn_score: 70,
    india_score: 67,
    metrics: [
      { label: "Tax-GSDP Ratio", label_ta: "வரி-GSDP விகிதம்", value: "8.6%", trend: "up", good_direction: "up", context: "Own tax revenue as % of GSDP" },
      { label: "FDI Inflows", label_ta: "வெளிநாட்டு நேரடி முதலீடு", value: "$5.6B (2023)", trend: "up", good_direction: "up", context: "TN among top FDI destinations" },
      { label: "Central Transfers to State", label_ta: "மத்திய அரசு பகிர்வு", value: "₹74,000 Cr", trend: "up", good_direction: "up", context: "Devolution + grants FY2023-24" },
    ],
  },
];

export const TN_SDG_SUMMARY = {
  overall_score: 78,
  india_score: 71,
  national_rank: 3,
  performance_category: "Front Runner" as const,
  year: "2023-24",
  source: "NITI Aayog SDG India Index 2023-24",
  source_url: "https://sdgindiaindex.niti.gov.in/#/ranking",
  strengths: [1, 3, 4, 7, 9, 12] as number[],  // SDG IDs where TN is a strength
  needs_attention: [2, 5] as number[],           // SDG IDs that need improvement
};

export const PERFORMANCE_META: Record<SDGPerformance, { label: string; label_ta: string; bg: string; text: string; border: string }> = {
  strength:         { label: "Strength",      label_ta: "பலம்",           bg: "bg-emerald-100", text: "text-emerald-800", border: "border-emerald-300" },
  moderate:         { label: "On Track",       label_ta: "வழியில் உள்ளது", bg: "bg-amber-100",   text: "text-amber-800",   border: "border-amber-300" },
  needs_attention:  { label: "Needs Focus",    label_ta: "கவனம் தேவை",     bg: "bg-red-100",     text: "text-red-700",     border: "border-red-300" },
};

// ---------------------------------------------------------------------------
// States ranked above Tamil Nadu — SDG India Index 2023-24
// Per-goal scores are from NITI Aayog SDG India Index 2023-24 state dashboards.
// TN per-goal scores must match the tn_score values in SDG_GOALS above.
// ---------------------------------------------------------------------------

export interface CompetitorLagArea {
  sdg_id: number;
  their_score: number;       // competitor state's score for this goal
  tn_score: number;          // TN's score for the same goal (matches SDG_GOALS)
  gap: number;               // their_score − tn_score (always positive)
  key_gap_en: string;        // specific metric where they lead TN
  key_gap_ta: string;
}

export interface CompetitorState {
  name: string;
  name_ta: string;
  overall_score: number;
  national_rank: number;
  rank_label_en: string;
  rank_label_ta: string;
  emoji: string;
  lag_areas: CompetitorLagArea[];
}

export const TOP_STATES_AHEAD: CompetitorState[] = [
  {
    name: "Kerala",
    name_ta: "கேரளா",
    overall_score: 79,
    national_rank: 1,
    rank_label_en: "Joint 1st",
    rank_label_ta: "கூட்டு 1வது",
    emoji: "🌴",
    lag_areas: [
      {
        sdg_id: 3,
        their_score: 92,
        tn_score: 82,
        gap: 10,
        key_gap_en: "MMR 19 vs TN's 54 per 1L births; IMR 6 vs TN's 10 per 1,000 live births",
        key_gap_ta: "தாய் மரண விகிதம் 19 vs தமிழ்நாடு 54; குழந்தை மரண விகிதம் 6 vs தமிழ்நாடு 10",
      },
      {
        sdg_id: 5,
        their_score: 73,
        tn_score: 65,
        gap: 8,
        key_gap_en: "Female LFPR 41% vs TN's 32.7%; better sex ratio at birth (1,047 vs 978)",
        key_gap_ta: "பெண் தொழிலாளர் பங்கேற்பு 41% vs தமிழ்நாடு 32.7%",
      },
      {
        sdg_id: 6,
        their_score: 79,
        tn_score: 71,
        gap: 8,
        key_gap_en: "99% HH piped water access vs TN's 85.4%; zero open defecation districts",
        key_gap_ta: "குழாய் நீர் வசதி 99% vs தமிழ்நாடு 85.4%; திறந்தவெளி மல ஜலம் பூஜ்யம்",
      },
    ],
  },
  {
    name: "Uttarakhand",
    name_ta: "உத்தரகண்ட்",
    overall_score: 79,
    national_rank: 1,
    rank_label_en: "Joint 1st",
    rank_label_ta: "கூட்டு 1வது",
    emoji: "🏔️",
    lag_areas: [
      {
        sdg_id: 2,
        their_score: 72,
        tn_score: 60,
        gap: 12,
        key_gap_en: "Child stunting 19.5% vs TN's 25.5%; child wasting 13.4% vs TN's 19.7%",
        key_gap_ta: "குழந்தை வளர்ச்சி குறைபாடு 19.5% vs தமிழ்நாடு 25.5%",
      },
      {
        sdg_id: 6,
        their_score: 84,
        tn_score: 71,
        gap: 13,
        key_gap_en: "93% piped water coverage; only 8 of 13 districts face water stress vs TN's 18 of 38",
        key_gap_ta: "குழாய் நீர் வசதி 93%; நீர் வறட்சி மாவட்டங்கள் குறைவு",
      },
      {
        sdg_id: 15,
        their_score: 82,
        tn_score: 66,
        gap: 16,
        key_gap_en: "Forest cover 71% of state area vs TN's 17.06%; richer biodiversity and ecosystem services",
        key_gap_ta: "வன் பகுதி 71% vs தமிழ்நாடு 17%; உயிரி வேற்றுமை செழுமை",
      },
    ],
  },
];
