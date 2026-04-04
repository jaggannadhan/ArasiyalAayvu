const API_BASE_URL = (process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000").replace(/\/$/, "");

function buildApiUrl(path: string): string {
  if (/^https?:\/\//i.test(path)) return path;
  return `${API_BASE_URL}${path.startsWith("/") ? path : `/${path}`}`;
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
  const response = await fetch(buildApiUrl(path), {
    method: "GET",
    ...init,
    headers: {
      "Accept": "application/json",
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
