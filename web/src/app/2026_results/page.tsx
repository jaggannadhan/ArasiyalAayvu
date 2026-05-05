"use client";

import { useState, useEffect } from "react";
import Link from "next/link";
import { apiGet } from "@/lib/api-client";
import { useLanguage } from "@/lib/LanguageContext";

interface PartyResult {
  party: string;
  abbr?: string;
  won: number;
  leading?: number;
  total?: number;
  votes?: number;
}

interface ResultsSummary {
  total_seats: number;
  total_declared: number;
  total_votes: number;
  majority_mark: number;
  party_wise: PartyResult[];
  eci_party_wise: PartyResult[];
}

interface ConstituencyResult {
  ac_no: number;
  ac_name: string;
  slug: string;
  winner: { name: string; party: string; votes: number; photo_url?: string } | null;
  runner_up: { name: string; party: string; votes: number; photo_url?: string } | null;
  margin: number;
  total_votes: number;
}

const PARTY_COLORS: Record<string, string> = {
  TVK: "bg-yellow-400",
  DMK: "bg-red-600",
  ADMK: "bg-green-600",
  INC: "bg-blue-500",
  BJP: "bg-orange-500",
  PMK: "bg-yellow-600",
  "CPI": "bg-red-500",
  "CPI(M)": "bg-red-700",
  VCK: "bg-purple-600",
  IUML: "bg-emerald-600",
  DMDK: "bg-sky-500",
  AMMK: "bg-teal-500",
};

// Map party abbreviation to flag filename (without extension)
const PARTY_FLAG: Record<string, { file: string; ext: string }> = {
  TVK: { file: "tvk", ext: "jpeg" },
  DMK: { file: "dmk", ext: "svg" },
  ADMK: { file: "aiadmk", ext: "svg" },
  INC: { file: "inc", ext: "svg" },
  BJP: { file: "bjp", ext: "svg" },
  PMK: { file: "pmk", ext: "svg" },
  CPI: { file: "cpi", ext: "svg" },
  "CPI(M)": { file: "cpim", ext: "png" },
  VCK: { file: "vck", ext: "png" },
  IUML: { file: "inc", ext: "svg" }, // fallback
  DMDK: { file: "dmdk", ext: "png" },
  AMMK: { file: "aiadmk", ext: "svg" }, // fallback
};

const PARTY_ABBR: Record<string, string> = {
  "Tamilaga Vettri Kazhagam": "TVK",
  "Dravida Munnetra Kazhagam": "DMK",
  "All India Anna Dravida Munnetra Kazhagam": "ADMK",
  "Indian National Congress": "INC",
  "Bharatiya Janata Party": "BJP",
  "Pattali Makkal Katchi": "PMK",
  "Communist Party of India": "CPI",
  "Communist Party of India (Marxist)": "CPI(M)",
  "Viduthalai Chiruthaigal Katchi": "VCK",
  "Indian Union Muslim League": "IUML",
  "Desiya Murpokku Dravida Kazhagam": "DMDK",
  "Amma Makkal Munnettra Kazagam": "AMMK",
};

function getAbbr(party: string): string {
  return PARTY_ABBR[party] || party.slice(0, 4).toUpperCase();
}

function getColor(party: string): string {
  const abbr = getAbbr(party);
  return PARTY_COLORS[abbr] || "bg-gray-400";
}

export default function ResultsPage() {
  const { lang, setLang } = useLanguage();
  const isTA = lang === "ta";
  const [summary, setSummary] = useState<ResultsSummary | null>(null);
  const [constituencies, setConstituencies] = useState<ConstituencyResult[]>([]);
  const [loading, setLoading] = useState(true);
  const [filterParty, setFilterParty] = useState<string>("");
  const [search, setSearch] = useState("");

  useEffect(() => {
    Promise.all([
      apiGet<ResultsSummary>("/api/results/summary"),
      apiGet<{ constituencies: ConstituencyResult[] }>("/api/results/constituencies"),
    ])
      .then(([sum, con]) => {
        setSummary(sum);
        setConstituencies(con.constituencies || []);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  // Merge: use party_wise (final counts from constituency results) enriched with
  // abbreviations from eci_party_wise. party_wise has correct final seat counts,
  // eci_party_wise may have stale won/leading split from mid-counting.
  const eciLookup: Record<string, string> = {};
  for (const p of summary?.eci_party_wise || []) {
    if (p.party && p.abbr) eciLookup[p.party] = p.abbr;
  }
  const partyData: PartyResult[] = (summary?.party_wise || []).map((p) => ({
    ...p,
    abbr: eciLookup[p.party] || undefined,
  }));

  // Top 3 for the scoreboard
  const top3 = partyData.slice(0, 3);

  // Filtered constituencies
  const filtered = constituencies.filter((c) => {
    if (filterParty && c.winner?.party !== filterParty) return false;
    if (search) {
      const q = search.toLowerCase();
      return (
        c.ac_name.toLowerCase().includes(q) ||
        (c.winner?.name || "").toLowerCase().includes(q) ||
        String(c.ac_no).includes(q)
      );
    }
    return true;
  });

  return (
    <main className="min-h-full bg-gray-50">
      {/* Header */}
      <header className="sticky top-0 z-30 bg-white border-b border-gray-200 shadow-sm">
        <div className="max-w-3xl mx-auto px-4 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Link href="/" className="text-gray-400 hover:text-gray-600 transition-colors">
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
              </svg>
            </Link>
            <div>
              <h1 className="text-lg font-black text-gray-900">
                {isTA ? "தேர்தல் முடிவுகள் 2026" : "Election Results 2026"}
              </h1>
              <p className="text-[10px] text-gray-500 font-semibold uppercase tracking-wider">
                {isTA ? "தமிழ்நாடு சட்டமன்றம்" : "Tamil Nadu Legislative Assembly"}
              </p>
            </div>
          </div>
          <button
            onClick={() => setLang(lang === "en" ? "ta" : "en")}
            className="text-xs font-bold px-3 py-1.5 rounded-full border border-gray-300 hover:bg-gray-100 transition-colors text-gray-900"
          >
            {lang === "en" ? "தமிழ்" : "English"}
          </button>
        </div>
      </header>

      <div className="max-w-3xl mx-auto px-4 py-6 space-y-6">
        {/* Loading */}
        {loading && (
          <div className="space-y-4">
            <div className="grid grid-cols-3 gap-3">
              {[1, 2, 3].map((i) => (
                <div key={i} className="h-24 bg-gray-100 rounded-2xl animate-pulse" />
              ))}
            </div>
            <div className="h-8 w-48 bg-gray-100 rounded animate-pulse" />
            {[1, 2, 3, 4, 5].map((i) => (
              <div key={i} className="h-16 bg-gray-100 rounded-xl animate-pulse" />
            ))}
          </div>
        )}

        {!loading && summary && (
          <>
            {/* Scoreboard - Top 3 */}
            <div className="grid grid-cols-3 gap-3">
              {top3.map((p, i) => {
                const abbr = p.abbr || getAbbr(p.party);
                const color = PARTY_COLORS[abbr] || "bg-gray-400";
                const seats = p.won || 0;
                const flag = PARTY_FLAG[abbr];
                const flagUrl = flag
                  ? `/party-flags/${flag.file}.${flag.ext}`
                  : undefined;
                return (
                  <button
                    key={abbr}
                    onClick={() => setFilterParty(filterParty === p.party ? "" : p.party)}
                    className={`relative rounded-2xl border-2 p-3 text-center transition-all overflow-hidden ${
                      filterParty === p.party
                        ? "border-gray-900 shadow-md"
                        : "border-gray-200 hover:border-gray-300"
                    }`}
                  >
                    {/* Blurred flag background */}
                    {flagUrl && (
                      <div
                        className="absolute inset-0"
                        style={{
                          backgroundImage: `url(${flagUrl})`,
                          backgroundSize: "cover",
                          backgroundPosition: "center",
                          filter: "blur(1px)",
                          opacity: 0.55,
                          transform: "scale(1.05)",
                        }}
                      />
                    )}
                    {/* Frosted glass overlay */}
                    <div className="absolute inset-0 bg-white/35" />

                    {/* Content on top */}
                    <div className="relative z-10">
                      <p className="text-2xl font-black text-gray-900">{seats}</p>
                      <p className="text-xs font-bold text-gray-600">{abbr}</p>
                    </div>
                    {i === 0 && seats >= summary.majority_mark && (
                      <span className="absolute -top-2 -right-2 z-20 text-[9px] font-bold px-1.5 py-0.5 rounded-full bg-green-600 text-white">
                        {isTA ? "ஆட்சி" : "MAJORITY"}
                      </span>
                    )}
                  </button>
                );
              })}
            </div>

            {/* Stats bar */}
            <div className="flex items-center gap-4 text-[11px] text-gray-500">
              <span>
                <span className="font-bold text-gray-900">{summary.total_declared}</span>/234{" "}
                {isTA ? "அறிவிக்கப்பட்டன" : "declared"}
              </span>
              <span className="text-gray-300">|</span>
              <span>
                {isTA ? "மொத்த வாக்குகள்" : "Total votes"}:{" "}
                <span className="font-bold text-gray-900">
                  {(summary.total_votes / 10000000).toFixed(1)}Cr
                </span>
              </span>
              <span className="text-gray-300">|</span>
              <span>
                {isTA ? "தேவையான பெரும்பான்மை" : "Required Majority"}: {summary.majority_mark}
              </span>
            </div>

            {/* Seat share bar */}
            <div className="h-3 rounded-full overflow-hidden flex bg-gray-100">
              {partyData.map((p) => {
                const abbr = p.abbr || getAbbr(p.party);
                const color = PARTY_COLORS[abbr] || "bg-gray-400";
                const pct = ((p.won || 0) / 234) * 100;
                if (pct < 0.5) return null;
                return (
                  <div
                    key={abbr}
                    className={`${color} transition-all`}
                    style={{ width: `${pct}%` }}
                    title={`${abbr}: ${p.won}`}
                  />
                );
              })}
            </div>

            {/* All parties */}
            <div className="flex flex-wrap gap-2">
              {partyData.map((p) => {
                const abbr = p.abbr || getAbbr(p.party);
                const color = PARTY_COLORS[abbr] || "bg-gray-400";
                const isActive = filterParty === p.party;
                return (
                  <button
                    key={abbr}
                    onClick={() => setFilterParty(isActive ? "" : p.party)}
                    className={`flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[11px] font-semibold transition-all ${
                      isActive
                        ? "bg-gray-900 text-white"
                        : "bg-white border border-gray-200 text-gray-700 hover:border-gray-400"
                    }`}
                  >
                    <div className={`w-2 h-2 rounded-full ${color}`} />
                    {abbr} · {p.won}
                  </button>
                );
              })}
            </div>

            {/* Search */}
            <input
              type="text"
              placeholder={isTA ? "தொகுதி அல்லது வேட்பாளர் தேடு..." : "Search constituency or candidate..."}
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="w-full px-4 py-2.5 rounded-xl border border-gray-200 text-sm focus:outline-none focus:ring-2 focus:ring-gray-300 bg-white text-gray-900 placeholder-gray-400"
            />

            {/* Constituency results table */}
            <div className="space-y-2">
              <p className="text-xs font-semibold text-gray-500">
                {filtered.length} {isTA ? "தொகுதிகள்" : "constituencies"}
                {filterParty && ` · ${getAbbr(filterParty)}`}
              </p>
              {filtered.map((c) => {
                const abbr = c.winner ? getAbbr(c.winner.party) : "?";
                const color = c.winner ? getColor(c.winner.party) : "bg-gray-300";
                return (
                  <Link
                    key={c.ac_no}
                    href={`/constituency/${c.slug}`}
                    className="flex items-center gap-3 bg-white rounded-xl border border-gray-200 px-3 py-2.5 hover:shadow-sm transition-all"
                  >
                    {/* AC number */}
                    <span className="text-[10px] font-mono text-gray-400 w-6 text-right flex-shrink-0">
                      {c.ac_no}
                    </span>

                    {/* Party color dot */}
                    <div className={`w-2.5 h-2.5 rounded-full ${color} flex-shrink-0`} />

                    {/* Name and winner */}
                    <div className="flex-1 min-w-0">
                      <p className="text-xs font-bold text-gray-900 truncate">{c.ac_name}</p>
                      <p className="text-[10px] text-gray-500 truncate">
                        {c.winner ? `${c.winner.name} · ${abbr}` : "—"}
                      </p>
                    </div>

                    {/* Margin */}
                    <div className="text-right flex-shrink-0">
                      <p className="text-xs font-bold text-gray-900">
                        +{c.margin.toLocaleString()}
                      </p>
                      <p className="text-[9px] text-gray-400">
                        {c.total_votes.toLocaleString()} {isTA ? "வாக்குகள்" : "votes"}
                      </p>
                    </div>
                  </Link>
                );
              })}
            </div>

            {/* Source */}
            <p className="text-center text-[10px] text-gray-400 pb-4 pt-2">
              {isTA ? "ஆதாரம்: " : "Source: "}
              <a
                href="https://results.eci.gov.in"
                target="_blank"
                rel="noopener noreferrer"
                className="underline underline-offset-2 hover:text-gray-600"
              >
                Election Commission of India
              </a>
            </p>
          </>
        )}
      </div>
    </main>
  );
}
