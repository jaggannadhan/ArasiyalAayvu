"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { useLanguage } from "@/lib/LanguageContext";
import { LiveCount } from "@/components/LiveCount";
import { ProfileModal, type PoliticianProfile } from "@/components/politicians/ProfileModal";
import { apiGet as apiGetRaw } from "@/lib/api-client";
import { MlaCard } from "@/components/constituency/MlaCard";
import { CandidatesPanel } from "@/components/constituency/CandidatesPanel";
import { WardPanel } from "@/components/constituency/WardPanel";
import { DistrictPanel } from "@/components/constituency/DistrictPanel";
import { InTheNews } from "@/components/news/InTheNews";
import { MLACDSCard } from "@/components/constituency/MLACDSCard";
import { CollectorCard } from "@/components/constituency/CollectorCard";
import { ConstituencySearch } from "@/components/constituency/ConstituencySearch";
import { ConstituencySkeleton } from "@/components/constituency/ConstituencySkeleton";
import { TenureNavigator, TERMS } from "@/components/constituency/TenureNavigator";
import { ElectorateCard } from "@/components/constituency/ElectorateCard";
import {
  fetchConstituencyData,
  peekConstituencyData,
  constituencyUrl,
  type ConstituencyDrillData,
  type ElectionResult2026,
} from "@/lib/constituency-fetcher";
import { apiGet } from "@/lib/api-client";
import { cacheHas } from "@/lib/data-cache";
import constituencyMap from "@/lib/constituency-map.json";
import electorateData2021 from "@/lib/constituency-electorate-2021.json";

type MapEntry = {
  name: string;
  tamil_name?: string;
  district: string;
  district_slug: string;
  constituency_id: number;
  // For constituencies whose district changed mid-history (e.g. Mayiladuthurai split Oct 2020)
  district_pre_2021?: string;
  district_slug_pre_2021?: string;
};

type ErrorState = {
  kind: "offline" | "generic";
  message: string;
};

const MAP = constituencyMap as Record<string, MapEntry>;

function getErrorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  if (typeof err === "string") return err;
  return "Unknown error";
}

function isOfflineError(err: unknown): boolean {
  if (typeof navigator !== "undefined" && navigator.onLine === false) {
    return true;
  }

  if (!err || typeof err !== "object") {
    return false;
  }

  const maybeFirebase = err as { code?: string; message?: string };
  const code = (maybeFirebase.code ?? "").toLowerCase();
  const message = (maybeFirebase.message ?? "").toLowerCase();

  return (
    code.includes("unavailable") ||
    message.includes("client is offline") ||
    message.includes("network request failed") ||
    message.includes("failed to fetch")
  );
}

interface ResultCandidate {
  name: string;
  party: string;
  votes: number;
  status: string;
  photo_url?: string;
}

function ResultCard2026({
  slug,
  result,
  lang,
  onCandidateClick,
}: {
  slug: string;
  result: ElectionResult2026;
  lang: string;
  onCandidateClick?: (candidateName: string) => void;
}) {
  const isTA = lang === "ta";
  const [candidates, setCandidates] = useState<ResultCandidate[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    apiGet<{ candidates: ResultCandidate[] }>(`/api/results/${encodeURIComponent(slug)}`)
      .then((data) => {
        const sorted = (data.candidates || []).sort((a, b) => b.votes - a.votes);
        setCandidates(sorted);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [slug]);

  const winner = result.winner;
  if (!winner) return null;

  const votePct = (votes: number) =>
    result.total_votes > 0 ? ((votes / result.total_votes) * 100).toFixed(1) : "0";

  return (
    <div className="rounded-2xl border border-gray-200 bg-white overflow-hidden">
      {/* Header */}
      <div className="px-4 pt-4 pb-3 space-y-1">
        <div className="flex items-center justify-between">
          <p className="text-[10px] font-bold text-blue-600 uppercase tracking-wider">
            {isTA ? "தேர்தல் 2026 · முடிவுகள்" : "ELECTION 2026 · RESULTS"}
          </p>
          <span className="text-[9px] font-bold px-2 py-0.5 rounded-full bg-green-600 text-white">
            {isTA ? "அறிவிக்கப்பட்டது" : "DECLARED"}
          </span>
        </div>
        <div className="flex items-baseline gap-2">
          <p className="text-2xl font-black text-gray-900">
            {loading ? "—" : candidates.length}
          </p>
          <p className="text-sm text-gray-500 font-medium">
            {isTA ? "போட்டியிட்டனர்" : "contested"}
          </p>
        </div>
      </div>

      {/* Winner highlight */}
      <div
        className={`mx-4 mb-3 p-3 rounded-xl bg-gradient-to-r from-green-50 to-emerald-50 border border-green-200 ${onCandidateClick ? "cursor-pointer hover:shadow-md transition-shadow" : ""}`}
        onClick={() => onCandidateClick?.(winner.name)}
      >
        <div className="flex items-center gap-3">
          {winner.photo_url && (
            <img
              src={winner.photo_url}
              alt={winner.name}
              className="w-12 h-12 rounded-full object-cover border-2 border-green-400"
            />
          )}
          <div className="flex-1 min-w-0">
            <p className="text-sm font-black text-gray-900 truncate">{winner.name}</p>
            <p className="text-xs text-gray-600">{winner.party}</p>
          </div>
          <div className="text-right flex-shrink-0">
            <p className="text-sm font-black text-green-700">+{result.margin.toLocaleString()}</p>
            <p className="text-[9px] text-gray-500">{isTA ? "வெற்றி இடைவெளி" : "margin"}</p>
          </div>
        </div>
      </div>

      {/* Candidate list */}
      {loading && (
        <div className="px-4 pb-3 space-y-2">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="h-12 bg-gray-50 rounded-lg animate-pulse" />
          ))}
        </div>
      )}

      {!loading && candidates.length > 0 && (
        <div className="divide-y divide-gray-100">
          {candidates.map((c, i) => {
            const barWidth = result.total_votes > 0 ? (c.votes / result.total_votes) * 100 : 0;
            const isWinner = c.status === "won";
            return (
              <div
                key={`${c.name}-${i}`}
                className={`relative px-4 py-2.5 ${onCandidateClick ? "cursor-pointer hover:bg-gray-100/50 transition-colors" : ""}`}
                onClick={() => onCandidateClick?.(c.name)}
              >
                {/* Vote share bar */}
                <div
                  className={`absolute inset-y-0 left-0 ${isWinner ? "bg-green-50" : "bg-gray-50"}`}
                  style={{ width: `${barWidth}%` }}
                />
                <div className="relative flex items-center gap-3">
                  <span className="text-[10px] font-mono text-gray-400 w-5 text-right flex-shrink-0">
                    {i + 1}
                  </span>
                  {c.photo_url ? (
                    <img
                      src={c.photo_url}
                      alt={c.name}
                      className={`w-8 h-8 rounded-full object-cover flex-shrink-0 ${
                        isWinner ? "border-2 border-green-400" : "border border-gray-200"
                      }`}
                    />
                  ) : (
                    <div className="w-8 h-8 rounded-full bg-gray-200 flex-shrink-0" />
                  )}
                  <div className="flex-1 min-w-0">
                    <p className={`text-xs truncate ${isWinner ? "font-bold text-gray-900" : "font-medium text-gray-700"}`}>
                      {c.name}
                      {isWinner && (
                        <span className="ml-1.5 text-[8px] font-bold px-1.5 py-0.5 rounded bg-green-600 text-white">
                          WON
                        </span>
                      )}
                    </p>
                    <p className="text-[10px] text-gray-400 truncate">{c.party}</p>
                  </div>
                  <div className="text-right flex-shrink-0">
                    <p className={`text-xs ${isWinner ? "font-bold text-gray-900" : "font-medium text-gray-600"}`}>
                      {c.votes.toLocaleString()}
                    </p>
                    <p className="text-[9px] text-gray-400">{votePct(c.votes)}%</p>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Total votes footer */}
      {!loading && candidates.length > 0 && (
        <div className="px-4 py-2.5 bg-gray-50 border-t border-gray-100 flex items-center justify-between">
          <span className="text-[10px] font-semibold text-gray-500">
            {isTA ? "மொத்த வாக்குகள்" : "Total Votes Polled"}
          </span>
          <span className="text-xs font-bold text-gray-700">
            {result.total_votes.toLocaleString()}
          </span>
        </div>
      )}
    </div>
  );
}

// Resolve slug to canonical form — handles missing _sc/_st suffixes
function resolveSlug(rawSlug: string): string {
  if (rawSlug in (constituencyMap as Record<string, unknown>)) return rawSlug;
  for (const suffix of ["_sc", "_st"]) {
    const candidate = rawSlug + suffix;
    if (candidate in (constituencyMap as Record<string, unknown>)) return candidate;
  }
  return rawSlug;
}

export default function ConstituencyPage() {
  const params = useParams();
  const rawSlug = typeof params.slug === "string" ? params.slug : "";
  const slug = resolveSlug(rawSlug);

  // Redirect to canonical slug URL to avoid hydration mismatches
  useEffect(() => {
    if (rawSlug && slug !== rawSlug) {
      window.history.replaceState(null, "", `/constituency/${slug}`);
    }
  }, [rawSlug, slug]);

  const { lang, setLang } = useLanguage();

  // Always show the latest term (2026) when opening a constituency page.
  const [selectedTerm, setSelectedTerm] = useState(2026);
  // undefined = closed, null = open + loading, PoliticianProfile = open + loaded
  const [profileModal, setProfileModal] = useState<PoliticianProfile | null | undefined>(undefined);

  function handleMlaClick(mlaDocId: string | undefined) {
    if (!mlaDocId) return;
    setProfileModal(null); // open immediately with skeleton
    apiGetRaw<PoliticianProfile>(`/api/politicians/${encodeURIComponent(mlaDocId)}`)
      .then(setProfileModal)
      .catch(() => setProfileModal(undefined)); // close on error
  }

  function handleMlaClickByName(name: string) {
    if (!name || !slug) return;
    setProfileModal(null); // open immediately with skeleton
    apiGetRaw<{ match: { doc_id: string } }>(
      `/api/politicians/by-constituency/${encodeURIComponent(slug)}?year=${selectedTerm}&name=${encodeURIComponent(name)}`
    )
      .then((res) => {
        if (res.match?.doc_id) {
          return apiGetRaw<PoliticianProfile>(`/api/politicians/${encodeURIComponent(res.match.doc_id)}`);
        }
        setProfileModal(undefined); // no match — close
        return null;
      })
      .then((profile) => {
        if (profile) setProfileModal(profile);
      })
      .catch(() => setProfileModal(undefined)); // close on error
  }

  // `bumpFetchTick` forces a re-read from cache when a fetch completes.
  const [, bumpFetchTick] = useState(0);
  // Track whether we've mounted — avoids hydration mismatch from reading
  // browser-only cache during SSR (server always has empty cache → loading=true,
  // but client may have a warm cache → loading=false → different HTML).
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<ErrorState | null>(null);

  // Only read from cache after mount to keep server/client initial render identical.
  const data: ConstituencyDrillData | null = mounted && slug
    ? peekConstituencyData(slug, selectedTerm) ?? null
    : null;

  const currentTermMeta = TERMS.find((t) => t.electionYear === selectedTerm) ?? TERMS.find((t) => t.electionYear === 2021)!;

  const meta = MAP[slug] as MapEntry | undefined;
  const isTA = lang === "ta";
  const constituencyName = meta?.name ?? slug.replace(/_/g, " ").toUpperCase();
  const constituencyBilingual = meta?.tamil_name
    ? `${constituencyName} / ${meta.tamil_name}`
    : constituencyName;

  // For constituencies whose district was bifurcated mid-history,
  // show the district that was in effect for the selected term.
  // e.g. Mayiladuthurai was carved from Nagapattinam in Oct 2020;
  // 2021 elections (Apr 2021) were already in the new district.
  const effectiveDistrict =
    selectedTerm < 2021 && meta?.district_pre_2021
      ? meta.district_pre_2021
      : (meta?.district ?? "");
  const effectiveDistrictSlug =
    selectedTerm < 2021 && meta?.district_slug_pre_2021
      ? meta.district_slug_pre_2021
      : (meta?.district_slug ?? "");

  // Fetch constituency data (re-fetches when slug or term changes, unless
  // the same slug+term is already in cache — in which case `data` is already
  // populated via the render-time cache read above and this is a no-op).
  useEffect(() => {
    if (!slug || !currentTermMeta.hasDrillData) {
      setLoading(false);
      return;
    }
    if (cacheHas(constituencyUrl(slug, selectedTerm))) {
      setLoading(false);
      setError(null);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setError(null);

    fetchConstituencyData(slug, selectedTerm)
      .then(() => {
        if (!cancelled) bumpFetchTick((n) => n + 1);
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        if (isOfflineError(err)) {
          setError({ kind: "offline", message: "offline" });
          return;
        }
        console.error("[ConstituencyPage] fetch error:", err);
        setError({ kind: "generic", message: getErrorMessage(err) });
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [slug, selectedTerm, currentTermMeta.hasDrillData]);

  // Fire-and-forget view counter increment (client-side only, non-blocking)
  useEffect(() => {
    if (!slug) return;
    apiGet<{ ok: boolean }>(`/api/constituency/${encodeURIComponent(slug)}/view`, {
      method: "POST" as RequestInit["method"],
    }).catch(() => { /* non-fatal */ });
  }, [slug]);

  return (
    <main className="min-h-full bg-gray-50">
      {/* Header */}
      <header className="sticky top-0 z-20 bg-white border-b border-gray-200 shadow-sm">
        <div className="max-w-2xl mx-auto px-4 py-3 flex items-center justify-between gap-3">
          <div className="flex items-center gap-2 min-w-0">
            <Link
              href="/"
              className="text-gray-400 hover:text-gray-700 shrink-0 text-lg"
            >
              ←
            </Link>
            <div className="min-w-0">
              <div className="flex items-center gap-1.5 leading-tight">
                <h1 className="text-sm font-black text-gray-900 truncate">
                  {constituencyBilingual}
                </h1>
                {meta?.constituency_id != null && (
                  <span className="shrink-0 text-[10px] font-bold text-gray-400 bg-gray-100 rounded px-1 py-0.5 tracking-widest font-mono">
                    {String(meta.constituency_id).padStart(3, "0")}
                  </span>
                )}
              </div>
              {/* Breadcrumb: Tamil Nadu > LS Constituency > Assembly */}
              <p className="text-xs text-gray-500 truncate">
                {data?.parent_ls
                  ? isTA
                    ? `தமிழ்நாடு › ${data.parent_ls.ls_name_ta} (ம.தொ.) › ${constituencyName}`
                    : `Tamil Nadu › ${data.parent_ls.ls_name} (LS) › ${constituencyName}`
                  : meta
                  ? `${effectiveDistrict} ${isTA ? "மாவட்டம்" : "District"}`
                  : "Tamil Nadu"}
              </p>
            </div>
          </div>
          <div className="flex items-center gap-3 shrink-0">
            <LiveCount />
            <button
              onClick={() => setLang(lang === "en" ? "ta" : "en")}
              className="text-xs font-bold px-3 py-1.5 rounded-full border border-gray-300 hover:bg-gray-100 transition-colors text-gray-900"
            >
              {lang === "en" ? "தமிழ்" : "English"}
            </button>
          </div>
        </div>
      </header>

      <div className="max-w-2xl mx-auto px-4 py-5 space-y-4">
        {/* Search bar */}
        <ConstituencySearch lang={lang} currentSlug={slug} />

        {/* Not found */}
        {!meta && !loading && (
          <div className="rounded-2xl border border-amber-200 bg-amber-50 px-5 py-4 text-sm text-amber-800">
            {isTA
              ? `"${slug}" என்ற தொகுதி காணப்படவில்லை. மேலே தேடுங்கள்.`
              : `Constituency "${slug}" not found. Try searching above.`}
          </div>
        )}

        {/* Tenure navigator — always visible once constituency is known */}
        {meta && (
          <TenureNavigator
            selectedYear={selectedTerm}
            onChange={setSelectedTerm}
            lang={lang}
          />
        )}

        {/* Electorate stats — 2016, 2021 and 2026 terms */}
        {meta && (selectedTerm === 2006 || selectedTerm === 2011 || selectedTerm === 2016 || selectedTerm === 2021 || selectedTerm === 2026) && (
          <ElectorateCard slug={slug} year={selectedTerm as 2006 | 2011 | 2016 | 2021 | 2026} lang={lang} />
        )}

        {/* Loading skeleton */}
        {loading && <ConstituencySkeleton />}

        {/* Error */}
        {error && !loading && (
          <div
            className={
              error.kind === "offline"
                ? "rounded-2xl border border-amber-200 bg-amber-50 px-5 py-4 text-sm text-amber-800"
                : "rounded-2xl border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700"
            }
          >
            {error.kind === "offline"
              ? isTA
                ? "இணைய இணைப்பு இல்லை அல்லது backend API தற்காலிகமாக அணுக முடியவில்லை. சிறிது நேரத்தில் மீண்டும் முயற்சிக்கவும்."
                : "You appear to be offline or the backend API is temporarily unreachable. Please check your connection and retry."
              : isTA
                ? "தரவு ஏற்றுவதில் பிழை. தயவுசெய்து பின்னர் முயற்சிக்கவும்."
                : `Failed to load data: ${error.message}`}
          </div>
        )}

        {/* 2026 Election Result + full candidate list (replaces old CandidatesPanel) */}
        {!loading && data?.election_result_2026?.winner && (
          <ResultCard2026
            slug={slug}
            result={data.election_result_2026}
            lang={lang}
            onCandidateClick={(candidateName) => {
              apiGetRaw<{ match: { doc_id: string } }>(
                `/api/politicians/by-constituency/${encodeURIComponent(slug)}?year=2026&name=${encodeURIComponent(candidateName)}`
              )
                .then((res) => {
                  if (res.match?.doc_id) {
                    handleMlaClick(res.match.doc_id);
                  }
                })
                .catch(() => { /* profile not found — silent */ });
            }}
          />
        )}

        {/* Term not yet available (non-2026 terms without drill data) */}
        {!loading && !currentTermMeta.hasDrillData && selectedTerm !== 2026 && (
          <div className="rounded-2xl border border-gray-200 bg-white px-5 py-10 text-center space-y-1">
            <p className="text-2xl">🗓</p>
            <p className="text-sm font-semibold text-gray-700">
              {isTA
                ? `${currentTermMeta.label} காலத்திற்கான தரவு இல்லை`
                : `Data for ${currentTermMeta.label} not yet available`}
            </p>
            <p className="text-xs text-gray-400">
              {isTA
                ? "2021–2026 காலத்திற்கு திரும்பவும்"
                : "Switch back to 2021–2026 to view current data"}
            </p>
          </div>
        )}

        {/* Main content — current term only */}
        {!loading && !error && data && currentTermMeta.hasDrillData && (
          <>
            {/* MLA not found */}
            {!data.mla && (
              <div className="rounded-2xl border border-gray-200 bg-white px-5 py-8 text-center text-sm text-gray-400">
                {isTA
                  ? "இந்த தொகுதிக்கான சட்டமன்ற உறுப்பினர் தகவல் கிடைக்கவில்லை."
                  : "MLA record not found for this constituency."}
              </div>
            )}

            {/* MLA card — clickable to show full politician profile */}
            {data.mla && (() => {
              const e2021 = (electorateData2021 as Record<string, { winner_votes?: number; winner_pct?: number }>)[slug];
              const winnerVotes = selectedTerm === 2021 ? e2021?.winner_votes : undefined;
              const winnerPct   = selectedTerm === 2021 ? e2021?.winner_pct   : undefined;
              return (
                <div
                  className="cursor-pointer"
                  onClick={() => {
                    if (selectedTerm === 2026) {
                      handleMlaClickByName(data.mla?.mla_name || "");
                    } else {
                      handleMlaClick(data.mla?.doc_id);
                    }
                  }}
                >
                  <MlaCard
                    mla={data.mla!}
                    district={effectiveDistrict}
                    lang={lang}
                    winnerVotes={winnerVotes}
                    winnerPct={winnerPct}
                  />
                </div>
              );
            })()}

            {/* MLACDS — MLA development fund allocation */}
            <MLACDSCard lang={lang} />

            {/* In the News — articles mentioning this constituency */}
            <InTheNews entityId={slug} lang={lang} />

            {/* District Collector */}
            {effectiveDistrictSlug && (
              <CollectorCard districtSlug={effectiveDistrictSlug} lang={lang} />
            )}

            {/* Ward & Local Body mapping + councillors */}
            <WardPanel
              wardMapping={data.ward_mapping}
              ulbHeads={data.ulb_heads}
              ulbCouncillors={data.ulb_councillors}
              selectedTerm={selectedTerm}
              lang={lang}
            />

            {/* District Indicators — combines socio metrics + risk data with toggle */}
            <DistrictPanel
              metrics={data.metrics}
              waterRisk={data.district_water_risk}
              crimeIndex={data.district_crime_index}
              roadSafety={data.district_road_safety}
              districtName={effectiveDistrict}
              metricsScope={data.metrics_scope}
              lang={lang}
            />

            {/* Data attribution */}
            <p className="text-xs text-center text-gray-400 pb-8">
              {isTA ? "ஆதாரம்: " : "Sources: "}
              <a href={data.mla?.source_url ?? "https://www.myneta.info/TamilNadu2021/"} target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">MyNeta</a>
              {" / "}
              <a href="https://adrindia.org" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">ADR 2021</a>
              {", "}
              <a href="https://rchiips.org/nfhs/NFHS-5Reports/TN.pdf" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">NFHS-5</a>
              {", "}
              <a href="https://asercentre.org/aser-2024/" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">ASER 2024</a>
              {", "}
              <a href="https://lgdirectory.gov.in" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">LGD GoI</a>
              {", "}
              <a href="https://elections.tn.gov.in/Form20.aspx" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">ECI TN 2016</a>
              {" · "}
              {isTA ? "அரசியல்ஆய்வு" : "ArasiyalAayvu"}
            </p>
          </>
        )}
      </div>

      {profileModal !== undefined && (
        <ProfileModal profile={profileModal} onClose={() => setProfileModal(undefined)} />
      )}
    </main>
  );
}
