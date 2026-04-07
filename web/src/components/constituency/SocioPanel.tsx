"use client";

import type { SocioMetric } from "@/lib/types";

interface SocioPanelProps {
  metrics: SocioMetric[];
  district: string;
  metricsScope?: "district" | "state_fallback";
  lang?: "en" | "ta";
}

// Which metrics to show and how to render them
const DISPLAY_CONFIG: Record<string, {
  label_en: string;
  label_ta: string;
  icon: string;
  group: "literacy" | "health" | "economy";
  good_direction: "high" | "low";
  warn_below?: number;
  warn_above?: number;
}> = {
  aser2024_std3_reading_recovery: {
    label_en: "Std 3 Reading (ASER 2024)",
    label_ta: "வகுப்பு 3 வாசிப்பு",
    icon: "📖",
    group: "literacy",
    good_direction: "high",
    warn_below: 30,
  },
  nfhs5_women_literacy: {
    label_en: "Female Literacy (NFHS-5)",
    label_ta: "பெண்கள் கல்வியறிவு",
    icon: "🎓",
    group: "literacy",
    good_direction: "high",
    warn_below: 70,
  },
  nfhs5_anaemia_women: {
    label_en: "Anaemia in Women",
    label_ta: "பெண்களில் இரத்த சோகை",
    icon: "💉",
    group: "health",
    good_direction: "low",
    warn_above: 40,
  },
  nfhs5_stunting_under5: {
    label_en: "Child Stunting (Under 5)",
    label_ta: "குழந்தை வளர்ச்சிக் குறைபாடு",
    icon: "🌱",
    group: "health",
    good_direction: "low",
    warn_above: 30,
  },
  nfhs5_institutional_deliveries: {
    label_en: "Skilled Birth Attendance",
    label_ta: "திறமையான மருத்துவப் பிரசவம்",
    icon: "🏥",
    group: "health",
    good_direction: "high",
    warn_below: 80,
  },
  industrial_corridors_district_coverage: {
    label_en: "Industrial Presence",
    label_ta: "தொழில் இருப்பு",
    icon: "🏭",
    group: "economy",
    good_direction: "high",
    warn_below: 60,
  },
};

function GaugeBar({ value, goodDirection, warn_below, warn_above }: {
  value: number;
  goodDirection: "high" | "low";
  warn_below?: number;
  warn_above?: number;
}) {
  const isWarning =
    (warn_below != null && value < warn_below) ||
    (warn_above != null && value > warn_above);

  const pct = Math.min(value, 100);
  const barColor = isWarning
    ? "bg-red-400"
    : goodDirection === "high"
    ? "bg-emerald-500"
    : value < (warn_above ?? 20)
    ? "bg-emerald-500"
    : "bg-amber-400";

  return (
    <div className="w-full h-2 bg-gray-100 rounded-full overflow-hidden">
      <div className={`h-full rounded-full ${barColor}`} style={{ width: `${pct}%` }} />
    </div>
  );
}

const GROUP_META = {
  literacy: { label_en: "Literacy",  label_ta: "கல்வியறிவு",     icon: "📚" },
  health:   { label_en: "Health",    label_ta: "சுகாதாரம்",       icon: "🏥" },
  economy:  { label_en: "Economy",   label_ta: "பொருளாதாரம்",     icon: "🏭" },
} as const;

type Group = keyof typeof GROUP_META;

export function SocioPanel({
  metrics,
  district,
  metricsScope = "state_fallback",
  lang = "en",
}: SocioPanelProps) {
  const isTA = lang === "ta";

  // Build ordered list from DISPLAY_CONFIG key order, matched to fetched metrics
  const ordered = Object.entries(DISPLAY_CONFIG)
    .map(([id, cfg]) => {
      const metric = metrics.find((m) => m.metric_id === id);
      return metric ? { metric, cfg } : null;
    })
    .filter((x): x is { metric: SocioMetric; cfg: typeof DISPLAY_CONFIG[string] } => x != null);

  // Group by theme
  const groups = (["literacy", "health", "economy"] as Group[]).map((g) => ({
    group: g,
    items: ordered.filter(({ cfg }) => cfg.group === g),
  })).filter(({ items }) => items.length > 0);

  return (
    <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-5 space-y-5">
      {/* Header */}
      <div>
        <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
          {isTA ? "சமூக-பொருளாதார குறிகாட்டிகள்" : "Socio-Economic Indicators"}
        </p>
        <p className="text-xs text-gray-400 mt-0.5">
          {metricsScope === "district"
            ? isTA
              ? `${district} மாவட்ட அளவிலான தரவு`
              : `District-level data · ${district}`
            : isTA
              ? `மாநில-அளவிலான சராசரி (மாவட்ட தரவு கிடைக்கவில்லை)`
              : `State-level averages (district data unavailable)`}
        </p>
      </div>

      {/* Grouped metrics */}
      {groups.map(({ group, items }) => {
        const gm = GROUP_META[group];
        return (
          <div key={group}>
            <p className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-2.5">
              {gm.icon} {isTA ? gm.label_ta : gm.label_en}
            </p>
            <div className="space-y-3.5">
              {items.map(({ metric, cfg }) => {
                const isWarning =
                  (cfg.warn_below != null && metric.value < cfg.warn_below) ||
                  (cfg.warn_above != null && metric.value > cfg.warn_above);
                const isPercent = metric.unit === "percent" || metric.unit === "percent_of_districts";

                return (
                  <div key={metric.metric_id} className="space-y-1.5">
                    <div className="flex items-center justify-between gap-2">
                      <div className="flex items-center gap-1.5 min-w-0">
                        <span className="text-sm">{cfg.icon}</span>
                        <span className="text-xs font-medium text-gray-700 truncate">
                          {isTA ? cfg.label_ta : cfg.label_en}
                        </span>
                        {isWarning && (
                          <span className="shrink-0 text-xs text-red-600 font-bold">⚠</span>
                        )}
                      </div>
                      <span className="text-sm font-bold text-gray-900 shrink-0">
                        {metric.value}{isPercent ? "%" : ""}
                      </span>
                    </div>
                    <GaugeBar
                      value={metric.value}
                      goodDirection={cfg.good_direction}
                      warn_below={cfg.warn_below}
                      warn_above={cfg.warn_above}
                    />
                    {metric.national_average != null && (
                      <p className="text-[10px] text-gray-400">
                        {isTA ? "தேசிய சராசரி" : "National avg"}: {metric.national_average}%
                      </p>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        );
      })}

      {/* Footer */}
      <p className="text-[10px] text-gray-400 border-t border-gray-100 pt-3 leading-relaxed">
        {isTA
          ? "தரவு மாவட்ட அளவில் மாநில திட்டமிடல் ஆணையம் வழிகாட்டுதலின்படி வழங்கப்படுகிறது. ஆதாரம்: "
          : "Data reported at District level per State Planning Commission standards. Sources: "}
        <a href="https://rchiips.org/nfhs/NFHS-5Reports/TN.pdf" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">NFHS-5 (2019-21)</a>
        {", "}
        <a href="https://asercentre.org/aser-2024/" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">ASER 2024</a>
      </p>
    </div>
  );
}
