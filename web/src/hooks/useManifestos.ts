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

// Module-level cache — persists for the browser session.
// Keyed by year filter string. In-flight map prevents duplicate concurrent fetches.
const _cache = new Map<string, ManifestoPromise[]>();
const _inFlight = new Map<string, Promise<ManifestoPromise[]>>();

function fetchManifestos(yearFilter: ManifestoYearFilter): Promise<ManifestoPromise[]> {
  const key = String(yearFilter);

  const cached = _cache.get(key);
  if (cached) return Promise.resolve(cached);

  const inFlight = _inFlight.get(key);
  if (inFlight) return inFlight;

  const yearParam = yearFilter === "all" ? "all" : String(yearFilter);
  const promise = apiGet<ManifestoPromise[]>(
    `/api/manifesto-promises?year=${encodeURIComponent(yearParam)}`
  ).then((docs) => {
    _cache.set(key, docs);
    _inFlight.delete(key);
    return docs;
  }).catch((err) => {
    _inFlight.delete(key);
    throw err;
  });

  _inFlight.set(key, promise);
  return promise;
}

export function useManifestos(
  yearFilter: ManifestoYearFilter = "all"
): UseManifestosResult {
  const cached = _cache.get(String(yearFilter));
  const [promises, setPromises] = useState<ManifestoPromise[]>(cached ?? []);
  const [loading, setLoading] = useState(!cached);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const key = String(yearFilter);

    // Already cached — no fetch needed
    if (_cache.has(key)) {
      setPromises(_cache.get(key)!);
      setLoading(false);
      setError(null);
      return;
    }

    setLoading(true);
    setError(null);

    let cancelled = false;
    fetchManifestos(yearFilter)
      .then((docs) => {
        if (!cancelled) {
          setPromises(docs);
          setLoading(false);
        }
      })
      .catch((err) => {
        if (cancelled) return;
        console.error("[useManifestos] API error:", err);
        setError(err instanceof Error ? err.message : "Unknown error");
        setLoading(false);
      });

    return () => { cancelled = true; };
  }, [yearFilter]);

  return { promises, loading, error };
}
