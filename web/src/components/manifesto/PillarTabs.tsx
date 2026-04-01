"use client";

import { PILLARS, PILLAR_META } from "@/lib/types";
import type { Pillar } from "@/lib/types";

interface PillarTabsProps {
  selected: Pillar;
  onChange: (pillar: Pillar) => void;
  lang?: "en" | "ta";
  promiseCounts?: Partial<Record<Pillar, number>>;
}

export function PillarTabs({ selected, onChange, lang = "en", promiseCounts = {} }: PillarTabsProps) {
  return (
    <div className="flex gap-2 overflow-x-auto pb-1 scrollbar-hide" role="tablist">
      {PILLARS.map((pillar) => {
        const meta = PILLAR_META[pillar];
        const isActive = pillar === selected;
        const count = promiseCounts[pillar];
        return (
          <button
            key={pillar}
            role="tab"
            aria-selected={isActive}
            onClick={() => onChange(pillar)}
            className={`
              shrink-0 flex items-center gap-1.5 px-4 py-2 rounded-full text-sm font-medium
              border transition-all duration-150 whitespace-nowrap
              ${isActive
                ? "bg-gray-900 text-white border-gray-900 shadow"
                : "bg-white text-gray-600 border-gray-200 hover:border-gray-400 hover:text-gray-900"
              }
            `}
          >
            <span>{meta.icon}</span>
            <span>{lang === "ta" ? meta.tamil : pillar}</span>
            {count !== undefined && (
              <span className={`text-xs rounded-full px-1.5 py-0.5 font-bold ${isActive ? "bg-white/20 text-white" : "bg-gray-100 text-gray-500"}`}>
                {count}
              </span>
            )}
          </button>
        );
      })}
    </div>
  );
}
