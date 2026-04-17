"use client";

import { useState, useRef, useEffect } from "react";
import { useRouter } from "next/navigation";
import constituencyMap from "@/lib/constituency-map.json";
import constituencyPincodes from "@/lib/constituency-pincodes.json";
import constituencyLocalities from "@/lib/constituency-localities.json";
import pincodeFlat from "@/lib/pincode-flat.json";
import { apiGet } from "@/lib/api-client";
import type { PincodeResult } from "@/lib/types";
import candidateIndex from "@/lib/candidate-search-index.json";

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

interface Locality {
  display: string;  // original casing, e.g. "Kilpauk"
  norm:    string;  // normalized for matching, e.g. "kilpauk"
}

interface ConstituencyOptionNorm extends ConstituencyOption {
  normName: string;
  normDistrict: string;
  localities: Locality[];
}

// Merge localities from two sources so we don't lose any: the curated
// per-constituency list AND the per-pincode list (pincode-flat carries
// localities like "Ashok Nagar" that aren't always mirrored into
// constituency-localities.json).
type PincodeRow = { assembly_slugs?: string[]; localities?: string[] };
const MERGED_LOCALITIES: Record<string, string[]> = (() => {
  const out: Record<string, string[]> = {};
  const seenPerSlug: Record<string, Set<string>> = {};

  const push = (slug: string, items: string[] | undefined) => {
    if (!items || items.length === 0) return;
    const arr  = (out[slug] ??= []);
    const seen = (seenPerSlug[slug] ??= new Set<string>());
    for (const raw of items) {
      const item = raw.trim();
      if (!item) continue;
      // Drop junk like a year string ("2026") that slipped into the data.
      if (/^\d{4}$/.test(item)) continue;
      const key = item.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      arr.push(item);
    }
  };

  for (const [slug, locs] of Object.entries(
    constituencyLocalities as Record<string, string[]>,
  )) {
    push(slug, locs);
  }
  for (const row of pincodeFlat as PincodeRow[]) {
    for (const slug of row.assembly_slugs ?? []) {
      push(slug, row.localities);
    }
  }
  return out;
})();

const ALL_CONSTITUENCIES: ConstituencyOptionNorm[] = Object.entries(
  constituencyMap as Record<string, { name: string; district: string }>
)
  .map(([slug, meta]) => ({
    slug,
    name:         meta.name,
    district:     meta.district,
    normName:     normalize(meta.name),
    normDistrict: normalize(meta.district),
    localities: (MERGED_LOCALITIES[slug] ?? []).map((l) => ({
      display: l,
      norm:    normalize(l),
    })),
  }))
  .sort((a, b) => a.name.localeCompare(b.name));

// ── Candidate search index ────────────────────────────────────────────────────

interface CandidateEntry {
  n: string;   // full name (ALL CAPS from scraper)
  p: string;   // party
  s: string;   // constituency slug
}

/** Strip single-letter initials ("M.K.", "E.P.S.", "K.") so "Stalin" matches
 *  "M.K. STALIN". Also applies the same transliteration normalize(). */
function normalizeCandidateName(raw: string): string {
  // Remove patterns like "J.", "M.K.", "E.P.S."
  const stripped = raw.replace(/\b[A-Z]\.?\s*/gi, "").trim();
  return normalize(stripped || raw);
}

function titleCase(s: string): string {
  return s
    .toLowerCase()
    .replace(/(^|\s|\.)\S/g, (c) => c.toUpperCase());
}

const CANDIDATE_MAP = constituencyMap as Record<string, { name: string; district: string }>;

interface CandidateMatch {
  name: string;          // title-cased display name
  party: string;
  slug: string;
  constituencyName: string;
}

const CANDIDATES_NORM = (candidateIndex as CandidateEntry[]).map((c) => ({
  ...c,
  norm: normalizeCandidateName(c.n),
  normFull: normalize(c.n),  // also match full name with initials
}));

const IS_PINCODE = (v: string) => /^\d+$/.test(v);
const STORAGE_KEY = (pin: string) => `aayvu_p2c_${pin}`;
const TERM_STORAGE_KEY = "aayvu_selected_term";

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

  // Name/district/locality search results (only when not in pin mode).
  // Localities extend the match surface — e.g. "kilpauk" matches Anna Nagar.
  const normQ = normalizeQuery(query);
  type MatchedResult = ConstituencyOptionNorm & { matchedLocality: string | null };
  const nameResults: MatchedResult[] = isPinMode || query.trim().length < 1
    ? []
    : ALL_CONSTITUENCIES.reduce<MatchedResult[]>((acc, c) => {
        const nameHit     = c.normName.includes(normQ);
        const districtHit = c.normDistrict.includes(normQ);
        const localityHit = !nameHit && !districtHit
          ? c.localities.find((l) => l.norm.includes(normQ)) ?? null
          : null;
        if (nameHit || districtHit || localityHit) {
          acc.push({ ...c, matchedLocality: localityHit ? localityHit.display : null });
        }
        return acc;
      }, []).slice(0, 8);

  // Candidate name search — runs alongside constituency/locality search.
  // Prioritize exact (case-insensitive) substring matches over transliteration-
  // normalized ones so "seeman" shows SEEMAN first, not "Narasimman" (which
  // matches only after ee→i + mm→m collapse).
  const candidateResults: CandidateMatch[] = (() => {
    if (isPinMode || query.trim().length < 2) return [];
    const rawQ = query.trim().toLowerCase();
    const exact: CandidateMatch[] = [];
    const fuzzy: CandidateMatch[] = [];
    for (const c of CANDIDATES_NORM) {
      if (exact.length + fuzzy.length >= 5) break;
      const meta = CANDIDATE_MAP[c.s];
      const entry: CandidateMatch = {
        name: titleCase(c.n),
        party: c.p,
        slug: c.s,
        constituencyName: meta?.name ?? c.s.replace(/_/g, " ").toUpperCase(),
      };
      // Exact lowercase match (before normalization) gets priority.
      if (c.n.toLowerCase().includes(rawQ)) {
        exact.push(entry);
      } else if (c.norm.includes(normQ) || c.normFull.includes(normQ)) {
        fuzzy.push(entry);
      }
    }
    return [...exact, ...fuzzy].slice(0, 5);
  })();

  const showDropdown =
    open &&
    (nameResults.length > 0 ||
      candidateResults.length > 0 ||
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

  function navigate(slug: string, term?: number) {
    setQuery("");
    setOpen(false);
    if (term) {
      try { localStorage.setItem(TERM_STORAGE_KEY, String(term)); } catch { /* ignore */ }
    }
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
    : "Constituency, District, PinCode, Candidate";

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
          {/* Name/district/locality-search results */}
          {nameResults.map((c) => {
            const pcData = (constituencyPincodes as Record<string, { pincodes: string[]; ambiguous_pincodes: string[] }>)[c.slug];
            const pinCount = pcData ? pcData.pincodes.length + pcData.ambiguous_pincodes.length : 0;
            return (
              <button
                key={c.slug}
                onClick={() => navigate(c.slug)}
                className={`w-full text-left px-4 py-2.5 hover:bg-gray-50 flex flex-col gap-0.5 text-sm transition-colors cursor-pointer ${
                  c.slug === currentSlug ? "bg-gray-100 font-semibold" : ""
                }`}
              >
                <div className="flex items-center justify-between gap-3">
                  <span className="font-medium text-gray-900 truncate">{c.name}</span>
                  <span className="text-xs text-gray-400 shrink-0 flex items-center gap-1.5">
                    <span>{c.district}</span>
                    {pinCount > 0 && (
                      <>
                        <span className="text-gray-300">·</span>
                        <span>{pinCount} PINs</span>
                      </>
                    )}
                  </span>
                </div>
                {c.matchedLocality && (
                  <span className="text-[11px] text-indigo-600">
                    📍 {c.matchedLocality}
                  </span>
                )}
              </button>
            );
          })}

          {/* Candidate name results */}
          {candidateResults.length > 0 && nameResults.length > 0 && (
            <div className="px-4 py-1.5 border-t border-gray-100">
              <p className="text-[10px] font-semibold text-gray-400 uppercase tracking-wide">
                {isTA ? "வேட்பாளர்கள் (2026)" : "Candidates (2026)"}
              </p>
            </div>
          )}
          {candidateResults.map((c, i) => (
            <button
              key={`${c.slug}-${i}`}
              onClick={() => navigate(c.slug, 2026)}
              className="w-full text-left px-4 py-2.5 hover:bg-gray-50 flex flex-col gap-0.5 text-sm transition-colors cursor-pointer"
            >
              <div className="flex items-center justify-between gap-3">
                <span className="font-medium text-gray-900 truncate">
                  {c.name}
                </span>
                <span className="text-xs text-gray-400 shrink-0">
                  {c.party}
                </span>
              </div>
              <span className="text-[11px] text-emerald-600">
                🏛 {c.constituencyName}
              </span>
            </button>
          ))}

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
