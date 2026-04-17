"use client";

import { useEffect, useState } from "react";
import { useLanguage } from "@/lib/LanguageContext";

const API_BASE_URL = (process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000").replace(/\/$/, "");
const POLL_INTERVAL_MS = 5 * 60 * 1000; // refresh every 5 minutes

/**
 * Small "🟢 X Live" badge for page headers. Fetches from /api/live-count
 * which aggregates session activity across all Cloud Run instances.
 * Re-polls every 5 minutes and on tab-visibility-change (so returning
 * users see a fresh count).
 */
export function LiveCount() {
  const { lang } = useLanguage();
  const isTA = lang === "ta";
  const [count, setCount] = useState<number | null>(null);

  useEffect(() => {
    const fetchCount = () => {
      fetch(`${API_BASE_URL}/api/live-count`)
        .then((r) => (r.ok ? r.json() : null))
        .then((d: { count?: number } | null) => {
          if (d && typeof d.count === "number") setCount(d.count);
        })
        .catch(() => { /* silent */ });
    };

    fetchCount();
    const timer = setInterval(fetchCount, POLL_INTERVAL_MS);

    const onVisibility = () => {
      if (!document.hidden) fetchCount();
    };
    document.addEventListener("visibilitychange", onVisibility);

    return () => {
      clearInterval(timer);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, []);

  if (count === null || count === 0) return null;

  return (
    <span className="inline-flex items-center gap-1.5 text-[11px] text-gray-500 font-semibold tabular-nums">
      <span className="relative flex h-2 w-2">
        <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-emerald-400 opacity-75" />
        <span className="relative inline-flex h-2 w-2 rounded-full bg-emerald-500" />
      </span>
      {count} {isTA ? "நேரடி பயனர்கள்" : "live users"}
    </span>
  );
}
