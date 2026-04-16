"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { useLanguage } from "@/lib/LanguageContext";
import { MlaCard } from "@/components/constituency/MlaCard";
import { CandidatesPanel } from "@/components/constituency/CandidatesPanel";
import { WardPanel } from "@/components/constituency/WardPanel";
import { DistrictPanel } from "@/components/constituency/DistrictPanel";
import { ConstituencySearch } from "@/components/constituency/ConstituencySearch";
import { ConstituencySkeleton } from "@/components/constituency/ConstituencySkeleton";
import { TenureNavigator, TERMS } from "@/components/constituency/TenureNavigator";
import { ElectorateCard } from "@/components/constituency/ElectorateCard";
import {
  fetchConstituencyData,
  type ConstituencyDrillData,
} from "@/lib/constituency-fetcher";
import { apiGet } from "@/lib/api-client";
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

export default function ConstituencyPage() {
  const params = useParams();
  const slug = typeof params.slug === "string" ? params.slug : "";

  const { lang, setLang } = useLanguage();
  const [selectedTerm, setSelectedTerm] = useState(2026);
  const [data, setData] = useState<ConstituencyDrillData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<ErrorState | null>(null);

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

  // Fetch constituency data (re-fetches when slug or term changes)
  useEffect(() => {
    if (!slug || !currentTermMeta.hasDrillData) { setLoading(false); return; }
    setLoading(true);
    setError(null);
    setData(null);

    fetchConstituencyData(slug, selectedTerm)
      .then(setData)
      .catch((err: unknown) => {
        if (isOfflineError(err)) {
          setError({ kind: "offline", message: "offline" });
          return;
        }

        console.error("[ConstituencyPage] fetch error:", err);
        setError({ kind: "generic", message: getErrorMessage(err) });
      })
      .finally(() => setLoading(false));
  }, [slug, selectedTerm]);

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
          <button
            onClick={() => setLang(lang === "en" ? "ta" : "en")}
            className="shrink-0 text-xs font-bold px-3 py-1.5 rounded-full border border-gray-300 hover:bg-gray-100 transition-colors text-gray-900"
          >
            {lang === "en" ? "தமிழ்" : "English"}
          </button>
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

        {/* 2026 upcoming election — show candidates panel */}
        {!loading && selectedTerm === 2026 && meta && (
          <CandidatesPanel slug={slug} lang={lang} />
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

            {/* MLA card */}
            {data.mla && (() => {
              const e2021 = (electorateData2021 as Record<string, { winner_votes?: number; winner_pct?: number }>)[slug];
              const winnerVotes = selectedTerm === 2021 ? e2021?.winner_votes : undefined;
              const winnerPct   = selectedTerm === 2021 ? e2021?.winner_pct   : undefined;
              return (
                <MlaCard
                  mla={data.mla!}
                  district={effectiveDistrict}
                  lang={lang}
                  winnerVotes={winnerVotes}
                  winnerPct={winnerPct}
                />
              );
            })()}

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
    </main>
  );
}
