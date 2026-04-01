"use client";

import { useEffect, useRef, useState } from "react";
import {
  collection,
  onSnapshot,
  query,
  where,
  type Unsubscribe,
} from "firebase/firestore";
import { db } from "@/lib/firebase";
import type { ManifestoPromise } from "@/lib/types";

export type ManifestoYearFilter = "all" | 2021 | 2026;

interface UseManifestosResult {
  promises: ManifestoPromise[];
  loading: boolean;
  error: string | null;
}

/**
 * Real-time listener for the manifesto_promises Firestore collection.
 *
 * yearFilter:
 *   "all"  — no year constraint (both historical and upcoming promises)
 *   2021   — DMK/AIADMK 2021 mandate record (Historical Performance view)
 *   2026   — Upcoming promises (Upcoming Promises view)
 */
export function useManifestos(yearFilter: ManifestoYearFilter = "all"): UseManifestosResult {
  const [promises, setPromises] = useState<ManifestoPromise[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Keep a stable ref to the unsubscribe fn so we can clean up on filter change.
  const unsubRef = useRef<Unsubscribe | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);

    const col = collection(db, "manifesto_promises");
    const q =
      yearFilter === "all"
        ? query(col)
        : query(col, where("target_year", "==", yearFilter));

    const unsub = onSnapshot(
      q,
      (snap) => {
        const docs = snap.docs.map((d) => d.data() as ManifestoPromise);
        setPromises(docs);
        setLoading(false);
      },
      (err) => {
        console.error("[useManifestos] Firestore error:", err);
        setError(err.message);
        setLoading(false);
      }
    );

    unsubRef.current = unsub;
    return () => unsub();
  }, [yearFilter]);

  return { promises, loading, error };
}
