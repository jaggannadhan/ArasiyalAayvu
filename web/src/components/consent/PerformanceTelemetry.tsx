"use client";

import { Analytics } from "@vercel/analytics/next";
import { SpeedInsights } from "@vercel/speed-insights/next";
import { useCookieConsent } from "@/lib/CookieConsentContext";

/**
 * Mounts Vercel Analytics + Speed Insights only when the user has granted
 * performance-cookie consent. Rendered unconditionally; the internal check
 * keeps the gating in one place (`useCookieConsent`) rather than scattered
 * across `layout.tsx`.
 */
export function PerformanceTelemetry() {
  const { performance } = useCookieConsent();
  if (performance !== true) return null;
  return (
    <>
      <Analytics />
      <SpeedInsights />
    </>
  );
}
