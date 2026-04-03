"use client";

interface Term {
  electionYear: number;   // 2021
  label: string;          // "2021–2026"
  assembly: string;       // "16th Assembly"
  hasDrillData: boolean;
}

const TERMS: Term[] = [
  { electionYear: 2006, label: "2006–2011", assembly: "12th Assembly", hasDrillData: true  },
  { electionYear: 2011, label: "2011–2016", assembly: "13th Assembly", hasDrillData: true  },
  { electionYear: 2016, label: "2016–2021", assembly: "14th Assembly", hasDrillData: true  },
  { electionYear: 2021, label: "2021–2026", assembly: "15th Assembly", hasDrillData: true  },
  { electionYear: 2026, label: "2026–2031", assembly: "16th Assembly", hasDrillData: false },
];

interface TenureNavigatorProps {
  selectedYear: number;
  onChange: (year: number) => void;
  lang?: "en" | "ta";
}

export function TenureNavigator({ selectedYear, onChange, lang = "en" }: TenureNavigatorProps) {
  const isTA = lang === "ta";
  const idx = TERMS.findIndex((t) => t.electionYear === selectedYear);
  const current = TERMS[idx] ?? TERMS.find((t) => t.electionYear === 2021)!;
  const prev = idx > 0 ? TERMS[idx - 1] : null;
  const next = idx < TERMS.length - 1 ? TERMS[idx + 1] : null;

  return (
    <div className="flex items-center justify-between gap-2 px-1">
      {/* Prev */}
      <button
        onClick={() => prev && onChange(prev.electionYear)}
        disabled={!prev}
        className="flex items-center gap-1 text-xs font-bold text-gray-400 hover:text-gray-700 disabled:opacity-20 disabled:cursor-not-allowed transition-colors px-2 py-1 rounded-lg hover:bg-gray-100 disabled:hover:bg-transparent"
        aria-label={prev ? `Go to ${prev.label}` : "No earlier term"}
      >
        ‹ <span className="hidden sm:inline">{prev?.label}</span>
      </button>

      {/* Current term */}
      <div className="text-center">
        <p className="text-sm font-black text-gray-900 leading-none">{current.label}</p>
        <p className="text-[10px] text-gray-400 mt-0.5">
          {isTA ? "சட்டமன்றக் காலம்" : current.assembly}
        </p>
      </div>

      {/* Next */}
      <button
        onClick={() => next && onChange(next.electionYear)}
        disabled={!next}
        className="flex items-center gap-1 text-xs font-bold text-gray-400 hover:text-gray-700 disabled:opacity-20 disabled:cursor-not-allowed transition-colors px-2 py-1 rounded-lg hover:bg-gray-100 disabled:hover:bg-transparent"
        aria-label={next ? `Go to ${next.label}` : "No later term"}
      >
        <span className="hidden sm:inline">{next?.label}</span> ›
      </button>
    </div>
  );
}

export { TERMS };
export type { Term };
