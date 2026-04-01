"use client";

import { useEffect, useState } from "react";
import { apiGet } from "@/lib/api-client";
import type { ManifestoPromise } from "@/lib/types";

export type ManifestoYearFilter = "all" | 2021 | 2026;

interface UseManifestosResult {
  promises: ManifestoPromise[];
  loading: boolean;
  error: string | null;
}

export function useManifestos(
  yearFilter: ManifestoYearFilter = "all"
): UseManifestosResult {
  const [promises, setPromises] = useState<ManifestoPromise[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(null);

    const yearParam = yearFilter === "all" ? "all" : String(yearFilter);

    apiGet<ManifestoPromise[]>(
      `/api/manifesto-promises?year=${encodeURIComponent(yearParam)}`,
      { signal: controller.signal }
    )
      .then((docs) => setPromises(docs))
      .catch((err) => {
        if ((err as { name?: string })?.name === "AbortError") return;
        console.error("[useManifestos] API error:", err);
        setError(err instanceof Error ? err.message : "Unknown error");
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      });

    return () => controller.abort();
  }, [yearFilter]);

  return { promises, loading, error };
}
