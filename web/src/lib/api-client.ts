const API_BASE_URL = (process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000").replace(/\/$/, "");

function buildApiUrl(path: string): string {
  if (/^https?:\/\//i.test(path)) return path;
  return `${API_BASE_URL}${path.startsWith("/") ? path : `/${path}`}`;
}

// Stable per-tab session ID — generated once and kept in sessionStorage so it
// survives soft navigations but not tab close. Used by the backend's session-
// tracking middleware to count "live users" without extra heartbeat requests.
function getSessionId(): string {
  if (typeof window === "undefined") return "";
  const KEY = "aayvu_session_id";
  let id = sessionStorage.getItem(KEY);
  if (!id) {
    id = crypto.randomUUID?.() ?? `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    sessionStorage.setItem(KEY, id);
  }
  return id;
}

async function parseError(response: Response): Promise<string> {
  try {
    const body = (await response.json()) as { detail?: string };
    if (body?.detail) return body.detail;
  } catch {
    // ignore JSON parse errors for non-JSON responses
  }
  return `HTTP ${response.status}`;
}

export async function apiGet<T>(
  path: string,
  init?: RequestInit
): Promise<T> {
  const sessionId = getSessionId();
  const response = await fetch(buildApiUrl(path), {
    method: "GET",
    ...init,
    headers: {
      "Accept": "application/json",
      ...(sessionId ? { "X-Session-ID": sessionId } : {}),
      ...(init?.headers ?? {}),
    },
  });

  if (!response.ok) {
    const detail = await parseError(response);
    const err = new Error(detail) as Error & { status: number };
    err.status = response.status;
    throw err;
  }

  return (await response.json()) as T;
}
