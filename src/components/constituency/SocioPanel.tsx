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
  good_direction: "high" | "low";  // is a higher value good or bad?
  warn_below?: number;
  warn_above?: number;
}> = {
  aser2024_std3_reading_recovery: {
    label_en: "Std 3 Reading (2024)",
    label_ta: "வகுப்பு 3 வாசிப்பு",
    icon: "📖",
    good_direction: "high",
    warn_below: 30,
  },
  nfhs5_stunting_under5: {
    label_en: "Child Stunting (Under 5)",
    label_ta: "குழந்தை வளர்ச்சிக் குறைபாடு",
    icon: "🌱",
    good_direction: "low",
    warn_above: 30,
  },
  nfhs5_anaemia_women: {
    label_en: "Anaemia in Women",
    label_ta: "பெண்களில் இரத்த சோகை",
    icon: "💉",
    good_direction: "low",
    warn_above: 40,
  },
  industrial_corridors_district_coverage: {
    label_en: "Industrial Presence",
    label_ta: "தொழில் இருப்பு",
    icon: "🏭",
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

export function SocioPanel({
  metrics,
  district,
  metricsScope = "state_fallback",
  lang = "en",
}: SocioPanelProps) {
  const isTA = lang === "ta";

  // Order metrics by DISPLAY_CONFIG key order
  const ordered = Object.keys(DISPLAY_CONFIG)
    .map((id) => metrics.find((m) => m.metric_id === id))
    .filter((m): m is SocioMetric => m != null);

  return (
    <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-5 space-y-4">
      <div>
        <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
          {isTA ? "உள்ளூர் சமூக நலன் குறிகாட்டிகள்" : "Local Socio-Economic Indicators"}
        </p>
        <p className="text-xs text-gray-400 mt-0.5">
          {metricsScope === "district"
            ? isTA
              ? `${district} மாவட்ட அளவிலான தரவு`
              : `District-level data for ${district}`
            : isTA
              ? `${district} மாவட்டத்திற்கான மாநில-அளவிலான (fallback) தரவு`
              : `State-level fallback benchmarks for ${district}`}
        </p>
      </div>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        {ordered.map((metric) => {
          const cfg = DISPLAY_CONFIG[metric.metric_id];
          if (!cfg) return null;
          const isWarning =
            (cfg.warn_below != null && metric.value < cfg.warn_below) ||
            (cfg.warn_above != null && metric.value > cfg.warn_above);

          return (
            <div key={metric.metric_id} className="space-y-1.5">
              <div className="flex items-center justify-between gap-2">
                <div className="flex items-center gap-1.5">
                  <span>{cfg.icon}</span>
                  <span className="text-xs font-medium text-gray-700">
                    {isTA ? cfg.label_ta : cfg.label_en}
                  </span>
                  {isWarning && (
                    <span className="text-xs text-red-600 font-bold">⚠</span>
                  )}
                </div>
                <span className="text-sm font-bold text-gray-900 shrink-0">
                  {metric.value}
                  {(metric.unit === "percent" || metric.unit === "percent_of_districts") ? "%" : ""}
                </span>
              </div>
              <GaugeBar
                value={metric.value}
                goodDirection={cfg.good_direction}
                warn_below={cfg.warn_below}
                warn_above={cfg.warn_above}
              />
              {metric.national_average != null && (
                <p className="text-xs text-gray-400">
                  {isTA ? "தேசிய சராசரி" : "National avg"}: {metric.national_average}%
                </p>
              )}
            </div>
          );
        })}
      </div>

      <p className="text-xs text-gray-400 border-t border-gray-100 pt-3">
        {isTA
          ? "ஆதாரம்: NFHS-5 (2019-21), ASER 2024"
          : "Source: NFHS-5 (2019-21), ASER 2024"}
      </p>
    </div>
  );
}
