"use client";

import { useState, useRef, useEffect } from "react";
import { useRouter } from "next/navigation";
import constituencyMap from "@/lib/constituency-map.json";
import { apiGet } from "@/lib/api-client";
import type { PincodeResult } from "@/lib/types";

interface ConstituencySearchProps {
  lang?: "en" | "ta";
  currentSlug?: string;
}

interface ConstituencyOption {
  slug: string;
  name: string;
  district: string;
}

const ALL_CONSTITUENCIES: ConstituencyOption[] = Object.entries(
  constituencyMap as Record<string, { name: string; district: string }>
)
  .map(([slug, meta]) => ({ slug, name: meta.name, district: meta.district }))
  .sort((a, b) => a.name.localeCompare(b.name));

const IS_PINCODE = (v: string) => /^\d+$/.test(v);
const STORAGE_KEY = (pin: string) => `aayvu_p2c_${pin}`;

type PincodeStatus = "idle" | "loading" | "ambiguous" | "not_found" | "error";

export function ConstituencySearch({ lang = "en", currentSlug }: ConstituencySearchProps) {
  const isTA = lang === "ta";
  const router = useRouter();
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const [pincodeStatus, setPincodeStatus] = useState<PincodeStatus>("idle");
  const [pincodeResult, setPincodeResult] = useState<PincodeResult | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const isPinMode = IS_PINCODE(query);

  // Name-search results (only when not in pin mode)
  const nameResults = isPinMode || query.trim().length < 1
    ? []
    : ALL_CONSTITUENCIES.filter(
        (c) =>
          c.name.toLowerCase().includes(query.toLowerCase()) ||
          c.district.toLowerCase().includes(query.toLowerCase())
      ).slice(0, 8);

  const showDropdown = open && nameResults.length > 0;

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

  async function handlePincodeLookup(pin: string) {
    if (pin.length !== 6) return;

    const saved = localStorage.getItem(STORAGE_KEY(pin));
    if (saved) { navigate(saved); return; }

    setPincodeStatus("loading");
    setPincodeResult(null);
    try {
      const data = await apiGet<PincodeResult>(`/api/lookup-pincode?code=${pin}`);
      if (!data.is_ambiguous && data.constituencies.length === 1) {
        navigate(data.constituencies[0].slug);
      } else {
        setPincodeResult(data);
        setPincodeStatus("ambiguous");
      }
    } catch (err: unknown) {
      setPincodeStatus(err instanceof Error && err.message.includes("404") ? "not_found" : "error");
    }
  }

  function handlePincodeSelect(slug: string) {
    localStorage.setItem(STORAGE_KEY(query), slug);
    navigate(slug);
  }

  function handleChange(e: React.ChangeEvent<HTMLInputElement>) {
    const v = e.target.value;
    // If currently in pin mode, strip non-digits and cap at 6
    const next = IS_PINCODE(v) || v === "" ? v.replace(/\D/g, "").slice(0, 6) : v;
    setQuery(next);
    setOpen(true);
    if (pincodeStatus !== "idle") {
      setPincodeStatus("idle");
      setPincodeResult(null);
    }
  }

  function handleKeyDown(e: React.KeyboardEvent) {
    if (e.key === "Enter" && isPinMode) handlePincodeLookup(query);
  }

  const icon = isPinMode ? "📮" : "🔍";
  const placeholder = isTA
    ? "தொகுதி பெயர் அல்லது பின்கோடு (எ.கா. Harur அல்லது 600023)"
    : "Constituency name or pincode (e.g. Harur or 600023)";

  return (
    <div ref={containerRef} className="relative w-full max-w-sm">
      {/* Single unified input */}
      <div className="relative">
        <span className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400 text-sm select-none">
          {icon}
        </span>
        <input
          type="text"
          inputMode={isPinMode ? "numeric" : "text"}
          value={query}
          onChange={handleChange}
          onFocus={() => setOpen(true)}
          onKeyDown={handleKeyDown}
          placeholder={placeholder}
          className="w-full pl-9 pr-4 py-2.5 text-sm text-gray-900 rounded-xl border border-gray-300 focus:outline-none focus:ring-2 focus:ring-gray-900 bg-white placeholder:text-gray-400"
        />
        {/* Spinner for pincode loading state */}
        {pincodeStatus === "loading" && (
          <span className="absolute right-3 top-1/2 -translate-y-1/2">
            <span className="inline-block w-4 h-4 border-2 border-gray-400 border-t-transparent rounded-full animate-spin" />
          </span>
        )}
      </div>

      {/* Name-search dropdown */}
      {showDropdown && (
        <div className="absolute z-30 top-full mt-1 w-full bg-white rounded-xl border border-gray-200 shadow-lg overflow-hidden">
          {nameResults.map((c) => (
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

      {/* Pincode: ambiguous picker (inline, below input) */}
      {pincodeStatus === "ambiguous" && pincodeResult && (
        <div className="mt-2 space-y-1.5">
          <p className="text-xs text-gray-500 text-center">
            {isTA ? "உங்கள் பகுதி எது?" : "Which area is yours?"}
          </p>
          {pincodeResult.constituencies.map((c) => (
            <button
              key={c.slug}
              onClick={() => handlePincodeSelect(c.slug)}
              className="w-full text-left px-4 py-2.5 rounded-xl border border-gray-200 bg-white hover:border-gray-400 hover:bg-gray-50 flex items-center justify-between gap-3 transition-all"
            >
              <span className="text-sm font-semibold text-gray-900">
                {isTA ? c.name_ta : c.name}
              </span>
              <span className="text-xs text-gray-400">
                {isTA ? c.name : c.name_ta}
              </span>
            </button>
          ))}
          <p className="text-xs text-gray-400 text-center">
            {isTA ? "உங்கள் தேர்வு நினைவில் வைக்கப்படும்" : "Your choice will be remembered"}
          </p>
        </div>
      )}

      {/* Pincode: inline status messages */}
      {pincodeStatus === "not_found" && (
        <p className="mt-2 text-xs text-amber-600 text-center">
          {isTA
            ? `${query} பின்கோடு தரவுத்தளத்தில் இல்லை.`
            : `Pincode ${query} isn't in our database yet.`}
        </p>
      )}
      {pincodeStatus === "error" && (
        <p className="mt-2 text-xs text-red-500 text-center">
          {isTA ? "பிழை ஏற்பட்டது. மீண்டும் முயற்சிக்கவும்." : "Something went wrong — please try again."}
        </p>
      )}
    </div>
  );
}
