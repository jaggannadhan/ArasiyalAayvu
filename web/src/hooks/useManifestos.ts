"use client";

import { useEffect, useState } from "react";
import { cacheFetch, cacheHas, cachePeek } from "@/lib/data-cache";
import type { ManifestoPromise } from "@/lib/types";

export type ManifestoYearFilter = "all" | 2021 | 2026;

interface UseManifestosResult {
  promises: ManifestoPromise[];
  loading: boolean;
  error: string | null;
}

function urlForYear(yearFilter: ManifestoYearFilter): string {
  const yearParam = yearFilter === "all" ? "all" : String(yearFilter);
  return `/api/manifesto-promises?year=${encodeURIComponent(yearParam)}`;
}

export function useManifestos(
  yearFilter: ManifestoYearFilter = "all",
): UseManifestosResult {
  const url = urlForYear(yearFilter);
  const cached = cachePeek<ManifestoPromise[]>(url);

  const [promises, setPromises] = useState<ManifestoPromise[]>(cached ?? []);
  const [loading, setLoading]   = useState(!cached);
  const [error, setError]       = useState<string | null>(null);

  useEffect(() => {
    const nextUrl = urlForYear(yearFilter);

    // Cached — short-circuit, no network.
    if (cacheHas(nextUrl)) {
      setPromises(cachePeek<ManifestoPromise[]>(nextUrl)!);
      setLoading(false);
      setError(null);
      return;
    }

    setLoading(true);
    setError(null);

    let cancelled = false;
    cacheFetch<ManifestoPromise[]>(nextUrl)
      .then((docs) => {
        if (cancelled) return;
        setPromises(docs);
        setLoading(false);
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        console.error("[useManifestos] API error:", err);
        setError(err instanceof Error ? err.message : "Unknown error");
        setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [yearFilter]);

  return { promises, loading, error };
}
