"use client";

import { PARTIES } from "@/lib/types";
import type { PartyMeta } from "@/lib/types";

interface PartySelectorProps {
  label: string;
  value: string;
  onChange: (partyId: string) => void;
  excludeId?: string;
  lang?: "en" | "ta";
}

export function PartySelector({ label, value, onChange, excludeId, lang = "en" }: PartySelectorProps) {
  const available = Object.values(PARTIES).filter((p) => p.party_id !== excludeId);
  const selected = PARTIES[value];

  return (
    <div className="flex flex-col gap-1.5 flex-1 min-w-0">
      <label className="text-xs font-semibold text-gray-500 uppercase tracking-wide">{label}</label>
      <div className="relative">
        <select
          value={value}
          onChange={(e) => onChange(e.target.value)}
          className={`
            w-full appearance-none rounded-xl border-2 px-4 py-3 pr-10
            text-sm font-bold cursor-pointer focus:outline-none focus:ring-2 focus:ring-offset-1
            ${selected?.border_color ?? "border-gray-300"}
            ${selected?.text_color ?? "text-gray-800"}
            bg-white
          `}
        >
          {available.map((party: PartyMeta) => (
            <option key={party.party_id} value={party.party_id}>
              {lang === "ta" ? `${party.tamil_name} (${party.party_name})` : party.party_name}
            </option>
          ))}
        </select>
        {/* Custom dropdown arrow */}
        <div className="pointer-events-none absolute inset-y-0 right-3 flex items-center">
          <svg className="h-4 w-4 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          </svg>
        </div>
      </div>
    </div>
  );
}
