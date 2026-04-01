"use client";

import { useState, useRef, useEffect } from "react";
import { useRouter } from "next/navigation";
import constituencyMap from "@/lib/constituency-map.json";

interface ConstituencySearchProps {
  lang?: "en" | "ta";
  currentSlug?: string;
}

interface ConstituencyOption {
  slug: string;
  name: string;
  district: string;
}

// Build a sorted list from the map at module load time
const ALL_CONSTITUENCIES: ConstituencyOption[] = Object.entries(
  constituencyMap as Record<string, { name: string; district: string }>
)
  .map(([slug, meta]) => ({ slug, name: meta.name, district: meta.district }))
  .sort((a, b) => a.name.localeCompare(b.name));

export function ConstituencySearch({ lang = "en", currentSlug }: ConstituencySearchProps) {
  const isTA = lang === "ta";
  const router = useRouter();
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  const results = query.trim().length < 1
    ? []
    : ALL_CONSTITUENCIES.filter(
        (c) =>
          c.name.toLowerCase().includes(query.toLowerCase()) ||
          c.district.toLowerCase().includes(query.toLowerCase())
      ).slice(0, 8);

  // Close dropdown on outside click
  useEffect(() => {
    function handler(e: MouseEvent) {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  function navigate(slug: string) {
    setQuery("");
    setOpen(false);
    router.push(`/constituency/${slug}`);
  }

  return (
    <div ref={containerRef} className="relative w-full max-w-sm">
      <div className="relative">
        <span className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400 text-sm">🔍</span>
        <input
          type="text"
          value={query}
          onChange={(e) => { setQuery(e.target.value); setOpen(true); }}
          onFocus={() => setOpen(true)}
          placeholder={isTA ? "தொகுதி தேடுக… (e.g. Harur)" : "Search constituency… (e.g. Harur)"}
          className="w-full pl-9 pr-4 py-2.5 text-sm rounded-xl border border-gray-300 focus:outline-none focus:ring-2 focus:ring-gray-900 bg-white"
        />
      </div>

      {open && results.length > 0 && (
        <div className="absolute z-30 top-full mt-1 w-full bg-white rounded-xl border border-gray-200 shadow-lg overflow-hidden">
          {results.map((c) => (
            <button
              key={c.slug}
              onClick={() => navigate(c.slug)}
              className={`w-full text-left px-4 py-2.5 hover:bg-gray-50 flex items-center justify-between gap-3 text-sm transition-colors ${
                c.slug === currentSlug ? "bg-gray-100 font-semibold" : ""
              }`}
            >
              <span className="font-medium text-gray-900 truncate">{c.name}</span>
              <span className="text-xs text-gray-400 shrink-0">{c.district}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
