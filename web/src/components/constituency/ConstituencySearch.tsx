"use client";

import { useState, useRef, useEffect } from "react";
import { useRouter } from "next/navigation";
import constituencyMap from "@/lib/constituency-map.json";
import constituencyPincodes from "@/lib/constituency-pincodes.json";
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

// ---------------------------------------------------------------------------
// Search helpers
// ---------------------------------------------------------------------------

/** Collapse common Tamil transliteration variants so alternate spellings match.
 *  Applied to both the query and the stored names before comparison.
 *  Examples:
 *    "Thiruvallur" → "tiruvallur"  (th→t)
 *    "Tiruttani"   → "tirutani"    (tt→t)
 *    "Tiruthani"   → "tirutani"    (th→t, then nothing else to collapse)
 *    "Thoothukudi" → "tutukudi"    (th→t, oo→u)
 */
function normalize(s: string): string {
  return s
    .toLowerCase()
    .replace(/th/g, "t")
    .replace(/dh/g, "d")
    .replace(/zh/g, "l")
    .replace(/ee/g, "i")
    .replace(/oo/g, "u")
    .replace(/([bcdfghjklmnpqrstvwxyz])\1/g, "$1"); // collapse double consonants
}

/** English common names that normalization alone can't bridge. */
const ALIASES: Record<string, string> = {
  madras:    "chennai",
  trichy:    "tiruchirappalli",
  tiruchy:   "tiruchirappalli",
  tuticorin: "thoothukudi",
  tuticorn:  "thoothukudi",
  tanjore:   "thanjavur",
  ooty:      "nilgiris",
  nellai:    "tirunelveli",
  kovai:     "coimbatore",
};

/** Normalize a user query, substituting known aliases at word boundaries. */
function normalizeQuery(q: string): string {
  let s = q.toLowerCase().trim();
  for (const [alias, canonical] of Object.entries(ALIASES)) {
    s = s.replace(new RegExp(`\\b${alias}\\b`, "g"), canonical);
  }
  return normalize(s);
}

interface ConstituencyOptionNorm extends ConstituencyOption {
  normName: string;
  normDistrict: string;
}

const ALL_CONSTITUENCIES: ConstituencyOptionNorm[] = Object.entries(
  constituencyMap as Record<string, { name: string; district: string }>
)
  .map(([slug, meta]) => ({
    slug,
    name:         meta.name,
    district:     meta.district,
    normName:     normalize(meta.name),
    normDistrict: normalize(meta.district),
  }))
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
  const normQ = normalizeQuery(query);
  const nameResults = isPinMode || query.trim().length < 1
    ? []
    : ALL_CONSTITUENCIES.filter(
        (c) => c.normName.includes(normQ) || c.normDistrict.includes(normQ)
      ).slice(0, 8);

  const showDropdown =
    open &&
    (nameResults.length > 0 ||
      (pincodeStatus === "ambiguous" && !!pincodeResult));

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
    if (saved) {
      // Show cached result in dropdown rather than auto-navigating
      const match = ALL_CONSTITUENCIES.find((c) => c.slug === saved);
      if (match) {
        setPincodeResult({ pincode: pin, district: match.district, is_ambiguous: false, constituencies: [{ slug: match.slug, name: match.name, name_ta: match.name }] });
        setPincodeStatus("ambiguous");
        return;
      }
    }

    setPincodeStatus("loading");
    setPincodeResult(null);
    try {
      const data = await apiGet<PincodeResult>(`/api/lookup-pincode?code=${pin}`);
      // Always show dropdown — never auto-navigate
      setPincodeResult(data);
      setPincodeStatus("ambiguous");
    } catch (err: unknown) {
      const status = (err as { status?: number }).status;
      setPincodeStatus(status === 404 ? "not_found" : "error");
    }
  }

  function handlePincodeSelect(slug: string) {
    localStorage.setItem(STORAGE_KEY(query), slug);
    navigate(slug);
  }

  function handleChange(e: React.ChangeEvent<HTMLInputElement>) {
    const v = e.target.value;
    const next = IS_PINCODE(v) || v === "" ? v.replace(/\D/g, "").slice(0, 6) : v;
    setQuery(next);
    setOpen(true);
    if (pincodeStatus !== "idle") {
      setPincodeStatus("idle");
      setPincodeResult(null);
    }
    // Auto-trigger lookup when exactly 6 digits entered
    if (IS_PINCODE(next) && next.length === 6) {
      handlePincodeLookup(next);
    }
  }

  function handleKeyDown(e: React.KeyboardEvent) {
    if (e.key === "Enter" && isPinMode) handlePincodeLookup(query);
  }

  const icon = isPinMode ? "📮" : "🔍";
  const placeholder = isTA
    ? "தொகுதி பெயர் அல்லது பின்கோடு (எ.கா. Harur அல்லது 600023)"
    : "Constituency, District, PinCode, Locality";

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

      {/* Unified dropdown: name-search results OR pincode ambiguous picker */}
      {showDropdown && (
        <div className="absolute z-30 top-full mt-1 w-full bg-white rounded-xl border border-gray-200 shadow-lg overflow-y-auto max-h-64">
          {/* Name-search results */}
          {nameResults.map((c) => {
            const pcData = (constituencyPincodes as Record<string, { pincodes: string[]; ambiguous_pincodes: string[] }>)[c.slug];
            const pinCount = pcData ? pcData.pincodes.length + pcData.ambiguous_pincodes.length : 0;
            return (
              <button
                key={c.slug}
                onClick={() => navigate(c.slug)}
                className={`w-full text-left px-4 py-2.5 hover:bg-gray-50 flex items-center justify-between gap-3 text-sm transition-colors cursor-pointer ${
                  c.slug === currentSlug ? "bg-gray-100 font-semibold" : ""
                }`}
              >
                <span className="font-medium text-gray-900 truncate">{c.name}</span>
                <span className="text-xs text-gray-400 shrink-0 flex items-center gap-1.5">
                  <span>{c.district}</span>
                  {pinCount > 0 && (
                    <span className="text-gray-300">·</span>
                  )}
                  {pinCount > 0 && (
                    <span>{pinCount} PINs</span>
                  )}
                </span>
              </button>
            );
          })}

          {/* Pincode ambiguous picker */}
          {pincodeStatus === "ambiguous" && pincodeResult && (
            <>
              <div className="px-4 py-1.5 border-b border-gray-100">
                <p className="text-xs text-gray-400">
                  {isTA ? "உங்கள் பகுதி எது?" : "Which area is yours?"}
                </p>
              </div>
              {pincodeResult.constituencies.map((c) => {
                const district = c.district ?? pincodeResult.district;
                return (
                  <button
                    key={c.slug}
                    onClick={() => handlePincodeSelect(c.slug)}
                    className="w-full text-left px-4 py-2.5 hover:bg-gray-50 flex items-center justify-between gap-3 text-sm transition-colors cursor-pointer"
                  >
                    <span className="font-medium text-gray-900 truncate">
                      {isTA && c.name_ta ? c.name_ta : c.name}
                    </span>
                    <span className="text-xs text-gray-400 shrink-0">
                      {district ? district.charAt(0) + district.slice(1).toLowerCase() : ""}
                    </span>
                  </button>
                );
              })}
            </>
          )}
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
