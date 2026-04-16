"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import { clearPersistedCache, persistInMemoryCache } from "./data-cache";

/**
 * Cookie / local-storage consent state for the app.
 *
 * We don't set tracking/ad cookies. "Performance" covers:
 *   - Vercel Analytics + Speed Insights (first-party, anonymised)
 *   - localStorage preferences we already write (aayvu_lang, pincode→constituency
 *     resolutions). These are strictly convenience, not identifiers, but we
 *     still let the user gate them to be honest about client-side storage.
 *
 * `null`  → user hasn't made a choice yet; banner should show.
 * `false` → user explicitly rejected performance cookies.
 * `true`  → user explicitly granted performance cookies.
 */
export type ConsentStatus = null | false | true;

const STORAGE_KEY = "aayvu_cookie_consent";

interface StoredConsent {
  performance: boolean;
  decided_at: string;   // ISO timestamp
  version: 1;
}

interface CookieConsentValue {
  /** Whether performance cookies are allowed; null = undecided. */
  performance: ConsentStatus;
  /** True only on the very first visit (before any choice has been saved). */
  undecided: boolean;
  /** Set the preference and persist it. */
  setPerformance: (allow: boolean) => void;
  /** Shortcuts — same as setPerformance(true/false). */
  acceptAll: () => void;
  rejectAll: () => void;
  /** Force the banner to show again (wire to a "Cookie settings" link). */
  reopen: () => void;
  /** Whether the banner should currently be rendered. */
  bannerOpen: boolean;
  closeBanner: () => void;
}

const CookieConsentContext = createContext<CookieConsentValue | null>(null);

export function CookieConsentProvider({ children }: { children: React.ReactNode }) {
  const [performance, setPerformanceState] = useState<ConsentStatus>(null);
  const [hydrated, setHydrated] = useState(false);
  // `bannerOpen` is separate from undecided so the user can reopen the banner
  // later via the footer link without losing their existing preference.
  const [bannerOpen, setBannerOpen] = useState(false);

  // Hydrate from localStorage on mount. Banner opens unless the user has
  // previously *granted* performance cookies — anything else (no decision, or
  // an explicit reject) re-prompts on every page load.
  useEffect(() => {
    let granted = false;
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (raw) {
        const parsed = JSON.parse(raw) as Partial<StoredConsent>;
        if (typeof parsed?.performance === "boolean") {
          setPerformanceState(parsed.performance);
          granted = parsed.performance === true;
        }
      }
    } catch {
      /* localStorage disabled — treat as no prior decision */
    }
    if (!granted) setBannerOpen(true);
    setHydrated(true);
  }, []);

  const setPerformance = useCallback((allow: boolean) => {
    setPerformanceState(allow);
    setBannerOpen(false);
    try {
      const doc: StoredConsent = {
        performance: allow,
        decided_at: new Date().toISOString(),
        version: 1,
      };
      localStorage.setItem(STORAGE_KEY, JSON.stringify(doc));
    } catch {
      /* localStorage unavailable — remembered for this session only */
    }

    // Keep the data cache in sync with the user's choice:
    //   allow  → flush anything already fetched this session into storage so
    //            the next reload picks it up.
    //   reject → purge any previously-persisted cache entries.
    if (allow) {
      persistInMemoryCache();
    } else {
      clearPersistedCache();
    }
  }, []);

  const value = useMemo<CookieConsentValue>(
    () => ({
      performance,
      undecided: hydrated && performance === null,
      setPerformance,
      acceptAll: () => setPerformance(true),
      rejectAll: () => setPerformance(false),
      reopen: () => setBannerOpen(true),
      bannerOpen,
      closeBanner: () => setBannerOpen(false),
    }),
    [performance, hydrated, setPerformance, bannerOpen],
  );

  return (
    <CookieConsentContext.Provider value={value}>
      {children}
    </CookieConsentContext.Provider>
  );
}

export function useCookieConsent(): CookieConsentValue {
  const ctx = useContext(CookieConsentContext);
  if (!ctx) {
    // Fail-soft fallback — component rendered outside the provider.
    return {
      performance: null,
      undecided: false,
      setPerformance: () => { /* noop */ },
      acceptAll: () => { /* noop */ },
      rejectAll: () => { /* noop */ },
      reopen: () => { /* noop */ },
      bannerOpen: false,
      closeBanner: () => { /* noop */ },
    };
  }
  return ctx;
}
