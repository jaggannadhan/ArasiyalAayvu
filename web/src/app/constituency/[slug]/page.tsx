"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { MlaCard } from "@/components/constituency/MlaCard";
import { SocioPanel } from "@/components/constituency/SocioPanel";
import { PromiseMatrix } from "@/components/constituency/PromiseMatrix";
import { ConstituencySearch } from "@/components/constituency/ConstituencySearch";
import { ConstituencySkeleton } from "@/components/constituency/ConstituencySkeleton";
import {
  fetchConstituencyData,
  type ConstituencyDrillData,
} from "@/lib/constituency-fetcher";
import { apiGet } from "@/lib/api-client";
import constituencyMap from "@/lib/constituency-map.json";

type MapEntry = {
  name: string;
  tamil_name?: string;
  district: string;
  district_slug: string;
  constituency_id: number;
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

  const [lang, setLang] = useState<"en" | "ta">("en");
  const [data, setData] = useState<ConstituencyDrillData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<ErrorState | null>(null);

  const meta = MAP[slug] as MapEntry | undefined;
  const isTA = lang === "ta";
  const constituencyName = meta?.name ?? slug.replace(/_/g, " ").toUpperCase();
  const constituencyBilingual = meta?.tamil_name
    ? `${constituencyName} / ${meta.tamil_name}`
    : constituencyName;

  // Fetch constituency data
  useEffect(() => {
    if (!slug) return;
    setLoading(true);
    setError(null);
    setData(null);

    fetchConstituencyData(slug)
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
  }, [slug]);

  // Fire-and-forget view counter increment (client-side only, non-blocking)
  useEffect(() => {
    if (!slug) return;
    apiGet<{ ok: boolean }>(`/api/constituency/${encodeURIComponent(slug)}/view`, {
      method: "POST" as RequestInit["method"],
    }).catch(() => { /* non-fatal */ });
  }, [slug]);

  return (
    <main className="min-h-screen bg-gray-50">
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
              <h1 className="text-sm font-black text-gray-900 leading-tight truncate">
                {constituencyBilingual}
              </h1>
              {/* Breadcrumb: Tamil Nadu > LS Constituency > Assembly */}
              <p className="text-xs text-gray-500 truncate">
                {data?.parent_ls
                  ? isTA
                    ? `தமிழ்நாடு › ${data.parent_ls.ls_name_ta} (ம.தொ.) › ${constituencyName}`
                    : `Tamil Nadu › ${data.parent_ls.ls_name} (LS) › ${constituencyName}`
                  : meta
                  ? `${meta.district} ${isTA ? "மாவட்டம்" : "District"}`
                  : "Tamil Nadu"}
              </p>
            </div>
          </div>
          <button
            onClick={() => setLang(lang === "en" ? "ta" : "en")}
            className="shrink-0 text-xs font-bold px-3 py-1.5 rounded-full border border-gray-300 hover:bg-gray-100 transition-colors"
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

        {/* Main content */}
        {!loading && !error && data && (
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
            {data.mla && (
              <MlaCard mla={data.mla} district={meta?.district ?? ""} lang={lang} />
            )}

            {/* Socio panel */}
            {data.metrics.length > 0 && (
              <SocioPanel
                metrics={data.metrics}
                district={meta?.district ?? "Tamil Nadu"}
                metricsScope={data.metrics_scope}
                lang={lang}
              />
            )}

            {/* Promise matrix */}
            {data.mla && (
              <PromiseMatrix
                promises={data.promises}
                partyName={data.mla.party}
                lang={lang}
              />
            )}

            {/* Data attribution */}
            <p className="text-xs text-center text-gray-400 pb-8">
              {isTA
                ? "ஆதாரம்: MyNeta/ADR 2021, NFHS-5, ASER 2024 · அரசியல்ஆய்வு"
                : "Sources: MyNeta/ADR 2021, NFHS-5, ASER 2024 · ArasiyalAayvu"}
            </p>
          </>
        )}
      </div>
    </main>
  );
}
