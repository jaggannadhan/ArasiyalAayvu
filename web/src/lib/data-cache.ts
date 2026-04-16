"use client";

import { apiGet } from "./api-client";

// Generic URL-keyed response cache + in-flight dedupe. Any component or
// background prefetcher can share this — the key is the API path, so
// consumers don't need to coordinate on a bespoke key scheme.
//
// Persistence: when the user has granted "performance cookies" consent (see
// CookieConsentContext), successful fetches are also written to localStorage
// so a full page reload still renders instantly. Entries have a 12-hour TTL
// so periodic ingestion (nightly) is picked up on the next day's first visit.

const _cache    = new Map<string, unknown>();
const _inflight = new Map<string, Promise<unknown>>();

// ── Persistence layer ────────────────────────────────────────────────────────

const STORAGE_PREFIX = "aayvu_cache:";
const CONSENT_KEY    = "aayvu_cookie_consent";
const SCHEMA_VERSION = 1;

interface PersistedEntry {
  data: unknown;
  stored_at: number;  // epoch ms
  version: number;
}

/** An entry is stale if it was stored before today's midnight in the user's
 *  local timezone. Every user gets a clean slate at their own 00:00. */
function isStale(storedAt: number): boolean {
  const now = new Date();
  const todayMidnight = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
  return storedAt < todayMidnight;
}

function hasPerformanceConsent(): boolean {
  if (typeof window === "undefined") return false;
  try {
    const raw = localStorage.getItem(CONSENT_KEY);
    if (!raw) return false;
    const parsed = JSON.parse(raw) as { performance?: boolean };
    return parsed?.performance === true;
  } catch {
    return false;
  }
}

function persist(url: string, data: unknown): void {
  if (typeof window === "undefined" || !hasPerformanceConsent()) return;
  try {
    const entry: PersistedEntry = {
      data,
      stored_at: Date.now(),
      version: SCHEMA_VERSION,
    };
    localStorage.setItem(STORAGE_PREFIX + url, JSON.stringify(entry));
  } catch {
    /* quota exceeded / storage disabled — drop silently */
  }
}

function hydrate(): void {
  if (typeof window === "undefined" || !hasPerformanceConsent()) return;
  try {
    const toRemove: string[] = [];
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (!key || !key.startsWith(STORAGE_PREFIX)) continue;
      try {
        const raw = localStorage.getItem(key);
        if (!raw) continue;
        const entry = JSON.parse(raw) as PersistedEntry;
        if (entry.version !== SCHEMA_VERSION || isStale(entry.stored_at)) {
          toRemove.push(key);
          continue;
        }
        _cache.set(key.slice(STORAGE_PREFIX.length), entry.data);
      } catch {
        toRemove.push(key);
      }
    }
    for (const k of toRemove) localStorage.removeItem(k);
  } catch {
    /* ignore */
  }
}

// Called from CookieConsentContext when consent flips to false.
export function clearPersistedCache(): void {
  if (typeof window === "undefined") return;
  try {
    const toRemove: string[] = [];
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key && key.startsWith(STORAGE_PREFIX)) toRemove.push(key);
    }
    for (const k of toRemove) localStorage.removeItem(k);
  } catch {
    /* ignore */
  }
}

// Called from CookieConsentContext when consent flips to true — flush any
// fetches that already landed in memory during the session so far.
export function persistInMemoryCache(): void {
  if (typeof window === "undefined" || !hasPerformanceConsent()) return;
  for (const [url, data] of _cache.entries()) persist(url, data);
}

// Client-only: hydrate once when this module is imported. Runs before any
// React render, so consumers reading via cachePeek() during render already
// see persisted data.
if (typeof window !== "undefined") hydrate();

/** Synchronous read. Returns undefined if not yet fetched. */
export function cachePeek<T>(url: string): T | undefined {
  return _cache.get(url) as T | undefined;
}

/** Cache-aware fetch. Returns cached immediately if present; dedupes
 *  concurrent callers so a single in-flight request serves everyone. */
export function cacheFetch<T>(url: string): Promise<T> {
  const cached = _cache.get(url);
  if (cached !== undefined) return Promise.resolve(cached as T);

  const pending = _inflight.get(url);
  if (pending) return pending as Promise<T>;

  const p = apiGet<T>(url)
    .then((data) => {
      _cache.set(url, data);
      persist(url, data);
      _inflight.delete(url);
      return data;
    })
    .catch((err) => {
      _inflight.delete(url);
      throw err;
    });

  _inflight.set(url, p as Promise<unknown>);
  return p;
}

/** Write directly into the cache (e.g., when prefetch already resolved via
 *  another path or after a manual invalidation). */
export function cacheSet<T>(url: string, data: T): void {
  _cache.set(url, data);
  persist(url, data);
}

export function cacheHas(url: string): boolean {
  return _cache.has(url);
}

interface IdleWindow {
  requestIdleCallback?: (cb: () => void, opts?: { timeout: number }) => number;
}

/** Kick off a list of prefetch URLs during browser idle time. Silent failures
 *  (background prefetch shouldn't disrupt the user). Falls back to a short
 *  setTimeout on Safari, which still lacks requestIdleCallback. */
export function prefetchOnIdle(urls: string[]): void {
  if (typeof window === "undefined" || urls.length === 0) return;
  const run = () => {
    for (const url of urls) {
      if (cacheHas(url)) continue;
      cacheFetch(url).catch(() => {
        /* silent prefetch — consumer will see the error on real fetch */
      });
    }
  };
  const w = window as IdleWindow;
  if (typeof w.requestIdleCallback === "function") {
    w.requestIdleCallback(run, { timeout: 5000 });
  } else {
    setTimeout(run, 500);
  }
}

/** Clear everything — in-memory AND persisted. Useful for a "hard refresh"
 *  button or tests. */
export function cacheClear(): void {
  _cache.clear();
  _inflight.clear();
  clearPersistedCache();
}
