"use client";

import { useEffect, useState, useCallback } from "react";
import Link from "next/link";
import Image from "next/image";
import { apiGet } from "@/lib/api-client";
import { useLanguage } from "@/lib/LanguageContext";
import { LiveCount } from "@/components/LiveCount";

// ── Types ────────────────────────────────────────────────────────────────────

interface TimelineEntry {
  year: number | null;
  constituency_slug?: string;
  constituency?: string;
  party?: string;
  won?: boolean | null;
  assets_cr?: number | null;
  movable_assets_cr?: number | null;
  immovable_assets_cr?: number | null;
  liabilities_cr?: number | null;
  net_assets_cr?: number | null;
  is_crorepati?: boolean | null;
  criminal_cases?: Array<Record<string, unknown>>;
  criminal_cases_total?: number | null;
  criminal_severity?: string | null;
  education?: string | null;
  education_tier?: string | null;
  source_url?: string | null;
  source_doc_id?: string | null;
  source_collection?: string | null;
  affidavit_url?: string | null;
}

interface PoliticianProfile {
  doc_id: string;
  canonical_name: string;
  aliases: string[];
  photo_url?: string | null;
  gender?: string | null;
  dob?: string | null;
  education?: string | null;
  age?: number | null;
  timeline: TimelineEntry[];
  current_party?: string | null;
  current_constituency?: string | null;
  current_constituency_slug?: string | null;
  win_count?: number | null;
  loss_count?: number | null;
  total_contested?: number | null;
}

interface ListResponse {
  total: number;
  page: number;
  limit: number;
  total_pages: number;
  items: PoliticianProfile[];
}

// ── Helpers ──────────────────────────────────────────────────────────────────

const API = (process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000").replace(/\/$/, "");

function titleCase(s: string): string {
  return s.toLowerCase().replace(/(^|\s|\.)\S/g, (c) => c.toUpperCase());
}

function fCr(v: number | null | undefined): string {
  if (v == null) return "—";
  return "₹" + v.toLocaleString("en-IN", { maximumFractionDigits: 2 }) + " Cr";
}

const SEVERITY_COLOR: Record<string, string> = {
  CLEAN: "text-emerald-600",
  MINOR: "text-amber-600",
  MODERATE: "text-orange-600",
  SERIOUS: "text-rose-600",
};

type SortField = "canonical_name" | "current_party" | "current_constituency" | "total_assets_cr" | "criminal_cases_total" | "win_count" | "age" | "latest_year";

// ── Page ─────────────────────────────────────────────────────────────────────

export default function PoliticiansPage() {
  const { lang, setLang } = useLanguage();
  const isTA = lang === "ta";

  const [data, setData] = useState<ListResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [query, setQuery] = useState("");
  const [searchInput, setSearchInput] = useState("");
  const [sortField, setSortField] = useState<SortField>("canonical_name");
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("asc");
  const [duplicatesOnly, setDuplicatesOnly] = useState(false);
  const [needsReview, setNeedsReview] = useState(false);
  const [noPhoto, setNoPhoto] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<PoliticianProfile | null>(null);
  const [mergeSource, setMergeSource] = useState<string | null>(null);
  const [mergeTargetInput, setMergeTargetInput] = useState("");
  const [mergeConfirm, setMergeConfirm] = useState<{
    sourceId: string;
    targetId: string;
    sourceEdu: string | null;
    targetEdu: string | null;
    educationConflict: boolean;
  } | null>(null);
  const [mergeEducationChoice, setMergeEducationChoice] = useState<string>("");
  const [actionLoading, setActionLoading] = useState(false);
  const [profileModal, setProfileModal] = useState<PoliticianProfile | null>(null);
  const [viewMode, setViewMode] = useState<"table" | "grid">("table");
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [bulkMergeConfirm, setBulkMergeConfirm] = useState(false);

  function toggleSelect(docId: string) {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(docId)) next.delete(docId); else next.add(docId);
      return next;
    });
  }

  function toggleSelectAll() {
    if (!data) return;
    const allOnPage = data.items.map((p) => p.doc_id);
    setSelected((prev) => {
      const allSelected = allOnPage.every((id) => prev.has(id));
      const next = new Set(prev);
      if (allSelected) {
        allOnPage.forEach((id) => next.delete(id));
      } else {
        allOnPage.forEach((id) => next.add(id));
      }
      return next;
    });
  }

  async function handleBulkMerge() {
    if (selected.size < 2) { alert("Select at least 2 rows to merge"); return; }
    setActionLoading(true);
    try {
      const ids = Array.from(selected);
      const targetId = ids[0];
      for (let i = 1; i < ids.length; i++) {
        const res = await fetch(`${API}/api/politicians/merge`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ source_id: ids[i], target_id: targetId }),
        });
        if (!res.ok) {
          const errBody = await res.json().catch(() => ({}));
          alert(`Merge failed for ${ids[i]}: ${(errBody as { detail?: string }).detail || res.status}`);
          break;
        }
        const result = await res.json() as { target?: PoliticianProfile };
        if (result.target) {
          applyMergeLocally(ids[i], result.target);
        }
      }
      setSelected(new Set());
      setBulkMergeConfirm(false);
    } catch {
      alert("Bulk merge failed");
    } finally {
      setActionLoading(false);
    }
  }

  const LIMIT = 100;

  const fetchData = useCallback(() => {
    setLoading(true);
    setError(null);
    const params = new URLSearchParams({
      page: String(page),
      limit: String(LIMIT),
      sort: sortField,
      order: sortOrder,
    });
    if (query) params.set("q", query);
    if (duplicatesOnly) params.set("duplicates_only", "true");
    if (needsReview) params.set("needs_review", "true");
    if (noPhoto) params.set("no_photo", "true");
    apiGet<ListResponse>(`/api/politicians?${params}`)
      .then(setData)
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false));
  }, [page, query, sortField, sortOrder, duplicatesOnly, needsReview, noPhoto]);

  useEffect(() => { fetchData(); }, [fetchData]);

  function handleSearch(e: React.FormEvent) {
    e.preventDefault();
    setPage(1);
    setQuery(searchInput.trim());
  }

  function toggleSort(field: SortField) {
    if (sortField === field) {
      setSortOrder((o) => (o === "asc" ? "desc" : "asc"));
    } else {
      setSortField(field);
      setSortOrder("asc");
    }
    setPage(1);
  }

  function sortArrow(field: SortField): string {
    if (sortField !== field) return " ↕";
    return sortOrder === "asc" ? " ↑" : " ↓";
  }

  async function handleDelete(docId: string) {
    setActionLoading(true);
    try {
      const res = await fetch(`${API}/api/politicians/${encodeURIComponent(docId)}`, { method: "DELETE" });
      if (!res.ok) {
        const errBody = await res.json().catch(() => ({}));
        alert(`Delete failed: ${(errBody as { detail?: string }).detail || res.status}`);
        return;
      }
      setDeleteTarget(null);
      setData((prev) => {
        if (!prev) return prev;
        return { ...prev, items: prev.items.filter((p) => p.doc_id !== docId), total: prev.total - 1 };
      });
      setSelected((prev) => { const next = new Set(prev); next.delete(docId); return next; });
    } catch {
      alert("Delete failed");
    } finally {
      setActionLoading(false);
    }
  }

  function applyMergeLocally(sourceId: string, updatedTarget: PoliticianProfile) {
    setData((prev) => {
      if (!prev) return prev;
      const items = prev.items
        .filter((p) => p.doc_id !== sourceId)
        .map((p) => (p.doc_id === updatedTarget.doc_id ? updatedTarget : p));
      return { ...prev, items, total: prev.total - 1 };
    });
    setSelected((prev) => {
      const next = new Set(prev);
      next.delete(sourceId);
      return next;
    });
  }

  async function handleMerge(sourceId: string, targetId: string, educationOverride?: string) {
    setActionLoading(true);
    try {
      const reqBody: Record<string, string> = { source_id: sourceId, target_id: targetId };
      if (educationOverride) reqBody.education_override = educationOverride;
      const res = await fetch(`${API}/api/politicians/merge`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(reqBody),
      });
      if (!res.ok) {
        const errBody = await res.json().catch(() => ({}));
        alert(`Merge failed: ${(errBody as { detail?: string }).detail || res.status}`);
        return;
      }
      const result = await res.json() as { target?: PoliticianProfile };
      if (result.target) {
        applyMergeLocally(sourceId, result.target);
      }
      setMergeSource(null);
      setMergeTargetInput("");
    } catch {
      alert("Merge failed");
    } finally {
      setActionLoading(false);
    }
  }

  return (
    <main className="min-h-full bg-gray-50">
      <header className="sticky top-0 z-20 bg-white border-b border-gray-200 shadow-sm">
        <div className="max-w-7xl mx-auto px-4 py-3 flex items-center justify-between gap-3">
          <div className="flex items-center gap-3 min-w-0">
            <Link href="/" className="text-gray-400 hover:text-gray-600 text-sm shrink-0">← Home</Link>
            <div className="min-w-0">
              <h1 className="text-sm font-black text-gray-900 truncate">
                {isTA ? "அரசியல்வாதிகள் சுயவிவரம்" : "Politician Profiles"}
              </h1>
              <p className="text-[10px] text-gray-500">{data ? `${data.total} records` : "Loading..."}</p>
            </div>
          </div>
          <div className="flex items-center gap-3 shrink-0">
            <LiveCount />
            <button
              onClick={() => setLang(lang === "en" ? "ta" : "en")}
              className="text-xs font-bold px-3 py-1.5 rounded-full border border-gray-300 hover:bg-gray-100 transition-colors text-gray-900"
            >
              {lang === "en" ? "தமிழ்" : "English"}
            </button>
          </div>
        </div>
      </header>

      <div className="max-w-7xl mx-auto px-4 py-4">
        {/* Search */}
        <form onSubmit={handleSearch} className="flex gap-2 mb-4">
          <input
            type="text"
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            placeholder={isTA ? "பெயர், கட்சி, தொகுதி தேடுங்கள்…" : "Search by name, party, constituency…"}
            className="flex-1 px-3 py-2 text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-gray-900"
          />
          <button type="submit" className="px-4 py-2 bg-gray-900 text-white text-sm font-bold rounded-lg hover:bg-gray-700 transition-colors">
            {isTA ? "தேடு" : "Search"}
          </button>
          {query && (
            <button type="button" onClick={() => { setSearchInput(""); setQuery(""); setPage(1); }}
              className="px-3 py-2 text-sm text-gray-500 hover:text-gray-900 transition-colors cursor-pointer">
              Clear
            </button>
          )}
          {/* View toggle */}
          <div className="flex border border-gray-300 rounded-lg overflow-hidden shrink-0">
            <button
              type="button"
              onClick={() => setViewMode("table")}
              title="Table view"
              className={`px-3 py-2 transition-colors cursor-pointer ${
                viewMode === "table" ? "bg-gray-900 text-white" : "bg-white text-gray-500 hover:bg-gray-50"
              }`}
            >
              <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
                <rect x="1" y="1" width="14" height="14" rx="1" />
                <line x1="1" y1="5.5" x2="15" y2="5.5" />
                <line x1="1" y1="10" x2="15" y2="10" />
                <line x1="6" y1="1" x2="6" y2="15" />
              </svg>
            </button>
            <button
              type="button"
              onClick={() => setViewMode("grid")}
              title="Grid view"
              className={`px-3 py-2 transition-colors cursor-pointer ${
                viewMode === "grid" ? "bg-gray-900 text-white" : "bg-white text-gray-500 hover:bg-gray-50"
              }`}
            >
              <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
                <rect x="0.5" y="0.5" width="6" height="6" rx="1" />
                <rect x="9" y="0.5" width="6" height="6" rx="1" />
                <rect x="0.5" y="9" width="6" height="6" rx="1" />
                <rect x="9" y="9" width="6" height="6" rx="1" />
              </svg>
            </button>
          </div>
        </form>

        {error && <p className="text-sm text-rose-600 mb-4">{error}</p>}

        {loading && (
          <div className="py-8 text-center text-gray-400 text-sm">Loading…</div>
        )}

        {!loading && data && data.items.length === 0 && (
          <div className="py-8 text-center text-gray-400 text-sm">No results</div>
        )}

        {/* Table View */}
        {!loading && viewMode === "table" && data && data.items.length > 0 && (
          <div className="overflow-x-auto rounded-xl border border-gray-200 bg-white">
            <table className="w-full text-xs">
              <thead className="bg-gray-50 border-b border-gray-200 text-left">
                <tr>
                  <th className="pl-5 pr-3 py-2 w-12">Photo</th>
                  <th className="px-3 py-2 cursor-pointer hover:text-gray-900" onClick={() => toggleSort("canonical_name")}>
                    Name{sortArrow("canonical_name")}
                  </th>
                  <th className="px-3 py-2 w-16">Gender</th>
                  <th className="px-3 py-2 cursor-pointer hover:text-gray-900" onClick={() => toggleSort("age")}>
                    Age{sortArrow("age")}
                  </th>
                  <th className="px-3 py-2">Education</th>
                  <th className="px-3 py-2 cursor-pointer hover:text-gray-900" onClick={() => toggleSort("current_party")}>
                    Current Party{sortArrow("current_party")}
                  </th>
                  <th className="pl-3 pr-5 py-2 cursor-pointer hover:text-gray-900" onClick={() => toggleSort("win_count")}>
                    W/L{sortArrow("win_count")}
                  </th>
                </tr>
              </thead>
              <tbody>
                {data.items.map((p) => (
                  <tr key={p.doc_id} className="border-b border-gray-100 hover:bg-gray-50 align-top cursor-pointer" onClick={() => setProfileModal(p)}>
                    <td className="pl-5 pr-3 py-2">
                      {p.photo_url ? (
                        // eslint-disable-next-line @next/next/no-img-element
                        <img src={p.photo_url} alt="" className="w-16 h-20 object-cover rounded" />
                      ) : (
                        <div className="w-16 h-20 bg-gray-200 rounded flex items-center justify-center text-gray-400 text-xs">—</div>
                      )}
                    </td>
                    <td className="px-3 py-2 font-semibold text-gray-900">{titleCase(p.canonical_name)}</td>
                    <td className="px-3 py-2 text-gray-600">{p.gender ?? "—"}</td>
                    <td className="px-3 py-2 text-gray-600">{p.age ?? "—"}</td>
                    <td className="px-3 py-2 text-gray-600">{p.education ?? "—"}</td>
                    <td className="px-3 py-2">
                      <span className="font-semibold text-gray-900">{p.current_party ?? "—"}</span>
                      {p.current_constituency && (
                        <div className="text-[10px] text-gray-500">{p.current_constituency}</div>
                      )}
                    </td>
                    <td className="pl-3 pr-5 py-2 text-gray-700">
                      {(p.win_count ?? 0)}W / {(p.loss_count ?? 0)}L
                      <div className="text-[9px] text-gray-400">{p.total_contested ?? 0} contested</div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Grid View */}
        {!loading && viewMode === "grid" && data && data.items.length > 0 && (
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-3">
            {data.items.map((p) => (
              <div
                key={p.doc_id}
                onClick={() => setProfileModal(p)}
                className="bg-white rounded-xl border border-gray-200 hover:shadow-md hover:border-gray-300 transition-all cursor-pointer overflow-hidden"
              >
                {/* Photo */}
                <div className="h-40 bg-gray-100 relative overflow-hidden">
                  {p.photo_url ? (
                    // eslint-disable-next-line @next/next/no-img-element
                    <img src={p.photo_url} alt="" className="w-full h-full object-cover" />
                  ) : (
                    <div className="w-full h-full flex items-center justify-center text-gray-300 text-3xl">👤</div>
                  )}
                  {/* Win badge */}
                  {(p.win_count ?? 0) > 0 && (
                    <span className="absolute top-2 right-2 text-[9px] font-bold bg-emerald-500 text-white px-1.5 py-0.5 rounded-full">
                      {p.win_count}W
                    </span>
                  )}
                </div>
                {/* Info */}
                <div className="p-3">
                  <p className="text-xs font-bold text-gray-900 truncate">{titleCase(p.canonical_name)}</p>
                  <p className="text-[10px] text-gray-600 truncate">{p.current_party ?? "—"}</p>
                  {p.current_constituency && (
                    <p className="text-[10px] text-gray-400 truncate">{p.current_constituency}</p>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Pagination */}
        {data && data.total_pages > 1 && (
          <div className="flex items-center justify-between mt-4 text-xs text-gray-600">
            <p>
              Page {data.page} of {data.total_pages} ({data.total} records)
            </p>
            <div className="flex gap-2">
              <button
                disabled={page <= 1}
                onClick={() => setPage((p) => p - 1)}
                className="px-3 py-1.5 rounded-lg border border-gray-300 hover:bg-gray-100 disabled:opacity-40 disabled:cursor-not-allowed"
              >
                ← Prev
              </button>
              <button
                disabled={page >= (data.total_pages)}
                onClick={() => setPage((p) => p + 1)}
                className="px-3 py-1.5 rounded-lg border border-gray-300 hover:bg-gray-100 disabled:opacity-40 disabled:cursor-not-allowed"
              >
                Next →
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Delete confirmation modal */}
      {deleteTarget && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4"
             onClick={() => setDeleteTarget(null)}>
          <div className="bg-white rounded-2xl shadow-2xl p-5 max-w-sm w-full space-y-4"
               onClick={(e) => e.stopPropagation()}>
            <h3 className="font-black text-gray-900">Delete this profile?</h3>
            <div className="text-sm text-gray-700">
              <p className="font-semibold">{titleCase(deleteTarget.canonical_name)}</p>
              <p className="text-gray-500">{deleteTarget.current_party} · {deleteTarget.current_constituency}</p>
              <p className="text-[10px] text-gray-400 mt-1 font-mono">{deleteTarget.doc_id}</p>
            </div>
            <p className="text-xs text-rose-600">This action cannot be undone.</p>
            <div className="flex justify-end gap-2">
              <button onClick={() => setDeleteTarget(null)}
                className="px-4 py-2 text-sm text-gray-600 hover:text-gray-900 transition-colors">
                Cancel
              </button>
              <button
                onClick={() => handleDelete(deleteTarget.doc_id)}
                disabled={actionLoading}
                className="px-4 py-2 rounded-lg bg-rose-600 text-white text-sm font-bold hover:bg-rose-700 disabled:bg-rose-300 transition-colors"
              >
                {actionLoading ? "Deleting…" : "Delete"}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Merge confirmation modal */}
      {mergeConfirm && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4"
             onClick={() => !actionLoading && setMergeConfirm(null)}>
          <div className="bg-white rounded-2xl shadow-2xl p-5 max-w-md w-full space-y-4"
               onClick={(e) => e.stopPropagation()}>
            <h3 className="font-black text-gray-900">Merge these profiles?</h3>
            <div className="text-sm space-y-2">
              <div className="bg-rose-50 border border-rose-200 rounded-lg p-3">
                <p className="text-[9px] font-bold text-rose-500 uppercase tracking-wide mb-1">Will be deleted</p>
                <p className="font-mono text-[10px] text-gray-500 break-all">{mergeConfirm.sourceId}</p>
              </div>
              <div className="text-center text-gray-400 text-lg">↓ merged into ↓</div>
              <div className="bg-emerald-50 border border-emerald-200 rounded-lg p-3">
                <p className="text-[9px] font-bold text-emerald-600 uppercase tracking-wide mb-1">Will survive (target)</p>
                <p className="font-mono text-[10px] text-gray-500 break-all">{mergeConfirm.targetId}</p>
              </div>
            </div>
            <p className="text-xs text-gray-500">
              Age: highest will be kept. Assets &amp; criminal cases: most recent election. Photo: target&apos;s if available, else source&apos;s.
            </p>

            {/* Education conflict — user must pick */}
            {mergeConfirm.educationConflict && (
              <div className="bg-amber-50 border border-amber-200 rounded-lg p-3 space-y-2">
                <p className="text-[10px] font-bold text-amber-700 uppercase tracking-wide">⚠ Education differs — pick one:</p>
                <label className="flex items-center gap-2 text-sm cursor-pointer">
                  <input
                    type="radio"
                    name="edu-pick"
                    checked={mergeEducationChoice === mergeConfirm.targetEdu}
                    onChange={() => setMergeEducationChoice(mergeConfirm.targetEdu || "")}
                    className="accent-indigo-600"
                  />
                  <span className="text-gray-800">
                    <span className="font-semibold">{mergeConfirm.targetEdu}</span>
                    <span className="text-[10px] text-gray-400 ml-1">(target)</span>
                  </span>
                </label>
                <label className="flex items-center gap-2 text-sm cursor-pointer">
                  <input
                    type="radio"
                    name="edu-pick"
                    checked={mergeEducationChoice === mergeConfirm.sourceEdu}
                    onChange={() => setMergeEducationChoice(mergeConfirm.sourceEdu || "")}
                    className="accent-indigo-600"
                  />
                  <span className="text-gray-800">
                    <span className="font-semibold">{mergeConfirm.sourceEdu}</span>
                    <span className="text-[10px] text-gray-400 ml-1">(source)</span>
                  </span>
                </label>
              </div>
            )}

            <div className="flex justify-end gap-2">
              <button onClick={() => setMergeConfirm(null)} disabled={actionLoading}
                className="px-4 py-2 text-sm text-gray-600 hover:text-gray-900 transition-colors cursor-pointer">
                Cancel
              </button>
              <button
                onClick={async () => {
                  await handleMerge(
                    mergeConfirm.sourceId,
                    mergeConfirm.targetId,
                    mergeConfirm.educationConflict ? mergeEducationChoice : undefined,
                  );
                  setMergeConfirm(null);
                }}
                disabled={actionLoading}
                className="px-4 py-2 rounded-lg bg-indigo-600 text-white text-sm font-bold hover:bg-indigo-700 disabled:bg-indigo-300 transition-colors cursor-pointer inline-flex items-center gap-2"
              >
                {actionLoading && (
                  <span className="inline-block w-3.5 h-3.5 border-2 border-white border-t-transparent rounded-full animate-spin" />
                )}
                {actionLoading ? "Merging…" : "Confirm merge"}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Bulk selection action bar */}
      {selected.size >= 2 && (
        <div className="fixed bottom-20 left-1/2 -translate-x-1/2 z-40 bg-indigo-600 text-white rounded-full shadow-2xl px-6 py-3 flex items-center gap-4">
          <span className="text-sm font-bold">{selected.size} selected</span>
          <button
            onClick={() => setBulkMergeConfirm(true)}
            className="bg-white text-indigo-700 text-sm font-bold px-4 py-1.5 rounded-full hover:bg-indigo-50 transition-colors cursor-pointer"
          >
            Merge all selected
          </button>
          <button
            onClick={() => setSelected(new Set())}
            className="text-indigo-200 hover:text-white text-sm transition-colors cursor-pointer"
          >
            Clear
          </button>
        </div>
      )}

      {/* Bulk merge confirmation */}
      {bulkMergeConfirm && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4"
             onClick={() => !actionLoading && setBulkMergeConfirm(false)}>
          <div className="bg-white rounded-2xl shadow-2xl p-5 max-w-md w-full space-y-4"
               onClick={(e) => e.stopPropagation()}>
            <h3 className="font-black text-gray-900">Merge {selected.size} profiles?</h3>
            <div className="text-sm space-y-2">
              <div className="bg-emerald-50 border border-emerald-200 rounded-lg p-3">
                <p className="text-[9px] font-bold text-emerald-600 uppercase tracking-wide mb-1">Target (will survive)</p>
                <p className="font-mono text-[10px] text-gray-600 break-all">{Array.from(selected)[0]}</p>
                {(() => {
                  const tgt = data?.items.find((p) => p.doc_id === Array.from(selected)[0]);
                  return tgt ? <p className="text-xs font-semibold text-gray-800 mt-1">{titleCase(tgt.canonical_name)}</p> : null;
                })()}
              </div>
              <div className="bg-rose-50 border border-rose-200 rounded-lg p-3">
                <p className="text-[9px] font-bold text-rose-500 uppercase tracking-wide mb-1">Will be merged &amp; deleted ({selected.size - 1})</p>
                {Array.from(selected).slice(1).map((id) => {
                  const p = data?.items.find((x) => x.doc_id === id);
                  return (
                    <p key={id} className="text-[10px] text-gray-600">
                      {p ? titleCase(p.canonical_name) : id}
                      <span className="text-gray-400 ml-1 font-mono">({id.slice(0, 20)}…)</span>
                    </p>
                  );
                })}
              </div>
            </div>
            <p className="text-xs text-gray-500">
              The first selected row becomes the target. All others merge into it (timeline, aliases, photo) and are deleted.
            </p>
            <div className="flex justify-end gap-2">
              <button onClick={() => setBulkMergeConfirm(false)} disabled={actionLoading}
                className="px-4 py-2 text-sm text-gray-600 hover:text-gray-900 transition-colors cursor-pointer">
                Cancel
              </button>
              <button
                onClick={handleBulkMerge}
                disabled={actionLoading}
                className="px-4 py-2 rounded-lg bg-indigo-600 text-white text-sm font-bold hover:bg-indigo-700 disabled:bg-indigo-300 transition-colors cursor-pointer inline-flex items-center gap-2"
              >
                {actionLoading && (
                  <span className="inline-block w-3.5 h-3.5 border-2 border-white border-t-transparent rounded-full animate-spin" />
                )}
                {actionLoading ? "Merging…" : `Merge ${selected.size} profiles`}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Profile detail modal */}
      {profileModal && (
        <div
          className="fixed inset-0 z-50 flex items-end sm:items-center justify-center bg-black/50 backdrop-blur-sm p-0 sm:p-4"
          onClick={() => setProfileModal(null)}
        >
          <div
            className="w-full sm:max-w-lg bg-white rounded-t-2xl sm:rounded-2xl shadow-2xl border border-gray-200 max-h-[90vh] overflow-y-auto"
            onClick={(e) => e.stopPropagation()}
          >
            {/* Header with photo + name */}
            <div className="bg-gray-50 p-5 flex items-center gap-4 border-b border-gray-200 rounded-t-2xl">
              {profileModal.photo_url ? (
                // eslint-disable-next-line @next/next/no-img-element
                <img src={profileModal.photo_url} alt="" className="w-16 h-20 object-cover rounded-lg shadow-sm" />
              ) : (
                <div className="w-16 h-20 bg-gray-200 rounded-lg flex items-center justify-center text-gray-400 text-xl">👤</div>
              )}
              <div className="min-w-0 flex-1">
                <h2 className="text-lg font-black text-gray-900 truncate">{titleCase(profileModal.canonical_name)}</h2>
                <p className="text-sm text-gray-600">{profileModal.current_party ?? "—"}</p>
                {profileModal.current_constituency && (
                  <p className="text-xs text-gray-400">{profileModal.current_constituency}</p>
                )}
              </div>
              <button
                type="button"
                onClick={() => setProfileModal(null)}
                className="text-gray-400 hover:text-gray-700 text-2xl leading-none p-1 -m-1 transition-colors self-start"
              >
                ×
              </button>
            </div>

            <div className="p-5 space-y-5">
              {/* Quick stats */}
              <div className="grid grid-cols-3 gap-3 text-center">
                <div className="bg-gray-50 rounded-xl p-3">
                  <p className="text-[10px] text-gray-500 font-semibold">Gender</p>
                  <p className="text-sm font-black text-gray-900">{profileModal.gender ?? "—"}</p>
                </div>
                <div className="bg-gray-50 rounded-xl p-3">
                  <p className="text-[10px] text-gray-500 font-semibold">Age</p>
                  <p className="text-sm font-black text-gray-900">{profileModal.age ?? "—"}</p>
                </div>
                <div className="bg-gray-50 rounded-xl p-3">
                  <p className="text-[10px] text-gray-500 font-semibold">Education</p>
                  <p className="text-sm font-black text-gray-900">{profileModal.education ?? "—"}</p>
                </div>
              </div>

              {/* Win/Loss record */}
              <div className="flex items-center gap-4">
                <div className="flex items-center gap-2">
                  <span className="text-emerald-600 font-black text-lg">{profileModal.win_count ?? 0}</span>
                  <span className="text-xs text-gray-500">Wins</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-rose-500 font-black text-lg">{profileModal.loss_count ?? 0}</span>
                  <span className="text-xs text-gray-500">Losses</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-gray-700 font-black text-lg">{profileModal.total_contested ?? 0}</span>
                  <span className="text-xs text-gray-500">Contested</span>
                </div>
              </div>

              {/* Asset Information */}
              {(() => {
                const withAssets = [...profileModal.timeline]
                  .filter((t) => t.assets_cr != null)
                  .sort((a, b) => (b.year ?? 0) - (a.year ?? 0));
                if (withAssets.length === 0) return null;
                const latest = withAssets[0];
                const oldest = withAssets[withAssets.length - 1];
                const hasGrowth = withAssets.length >= 2 && oldest.assets_cr != null && latest.assets_cr != null && oldest.assets_cr > 0;
                const growthPct = hasGrowth ? ((latest.assets_cr! - oldest.assets_cr!) / oldest.assets_cr! * 100) : null;

                return (
                  <div>
                    <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-3">Asset Information</p>
                    <div className="grid grid-cols-2 gap-3">
                      <div className="bg-gray-50 rounded-xl p-3">
                        <p className="text-[10px] text-gray-500 font-semibold">Total Assets</p>
                        <p className="text-sm font-black text-gray-900">{fCr(latest.assets_cr)}</p>
                        <p className="text-[9px] text-gray-400">as of {latest.year}</p>
                      </div>
                      <div className="bg-gray-50 rounded-xl p-3">
                        <p className="text-[10px] text-gray-500 font-semibold">Liabilities</p>
                        <p className="text-sm font-black text-gray-900">{fCr(latest.liabilities_cr)}</p>
                        <p className="text-[9px] text-gray-400">as of {latest.year}</p>
                      </div>
                      {latest.movable_assets_cr != null && (
                        <div className="bg-gray-50 rounded-xl p-3">
                          <p className="text-[10px] text-gray-500 font-semibold">Movable Assets</p>
                          <p className="text-sm font-black text-gray-900">{fCr(latest.movable_assets_cr)}</p>
                        </div>
                      )}
                      {latest.immovable_assets_cr != null && (
                        <div className="bg-gray-50 rounded-xl p-3">
                          <p className="text-[10px] text-gray-500 font-semibold">Immovable Assets</p>
                          <p className="text-sm font-black text-gray-900">{fCr(latest.immovable_assets_cr)}</p>
                        </div>
                      )}
                    </div>
                    {/* Asset growth trend */}
                    {hasGrowth && growthPct != null && (
                      <div className={`mt-3 rounded-lg px-3 py-2 text-[11px] font-semibold ${
                        growthPct >= 0
                          ? "bg-amber-50 border border-amber-200 text-amber-800"
                          : "bg-emerald-50 border border-emerald-200 text-emerald-800"
                      }`}>
                        📊 Asset growth: {fCr(oldest.assets_cr)} ({oldest.year}) → {fCr(latest.assets_cr)} ({latest.year})
                        {" · "}
                        <span className="font-black">{growthPct >= 0 ? "+" : ""}{growthPct.toFixed(0)}%</span>
                        {" "}over {(latest.year ?? 0) - (oldest.year ?? 0)} years
                      </div>
                    )}
                  </div>
                );
              })()}

              {/* Timeline */}
              {profileModal.timeline.length > 0 && (
                <div>
                  <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-3">Political Timeline</p>
                  <div className="space-y-3">
                    {[...profileModal.timeline].sort((a, b) => (b.year ?? 0) - (a.year ?? 0)).map((t, i) => (
                      <div key={i} className="bg-gray-50 rounded-xl p-3 border border-gray-100">
                        <div className="flex items-center justify-between gap-2 mb-2">
                          <div className="flex items-center gap-2">
                            <span className="text-sm font-black text-gray-900">{t.year}</span>
                            <span className="text-xs font-semibold text-gray-600">{t.party}</span>
                          </div>
                          {t.won === true && (
                            <span className="text-[10px] font-bold text-emerald-700 bg-emerald-50 px-2 py-0.5 rounded-full">✓ Won</span>
                          )}
                          {t.won === false && (
                            <span className="text-[10px] font-bold text-rose-700 bg-rose-50 px-2 py-0.5 rounded-full">✗ Lost</span>
                          )}
                          {t.won === null && (
                            <span className="text-[10px] font-bold text-gray-500 bg-gray-100 px-2 py-0.5 rounded-full">Contesting</span>
                          )}
                        </div>
                        <p className="text-xs text-gray-600 mb-2">📍 {t.constituency}</p>
                        <div className="grid grid-cols-2 gap-2 text-[10px]">
                          {t.assets_cr != null && (
                            <div>
                              <span className="text-gray-500">Assets: </span>
                              <span className="font-semibold text-gray-800">{fCr(t.assets_cr)}</span>
                            </div>
                          )}
                          {t.liabilities_cr != null && (
                            <div>
                              <span className="text-gray-500">Liabilities: </span>
                              <span className="font-semibold text-gray-800">{fCr(t.liabilities_cr)}</span>
                            </div>
                          )}
                          {(t.criminal_cases_total ?? 0) > 0 && (
                            <div>
                              <span className="text-gray-500">Criminal: </span>
                              <span className={`font-semibold ${SEVERITY_COLOR[t.criminal_severity ?? ""] ?? "text-gray-800"}`}>
                                {t.criminal_cases_total} cases ({t.criminal_severity})
                              </span>
                            </div>
                          )}
                          {(t.criminal_cases_total ?? 0) === 0 && t.criminal_severity === "CLEAN" && (
                            <div>
                              <span className="text-gray-500">Criminal: </span>
                              <span className="font-semibold text-emerald-600">Clean</span>
                            </div>
                          )}
                          {t.education && (
                            <div>
                              <span className="text-gray-500">Education: </span>
                              <span className="font-semibold text-gray-800">{t.education}</span>
                            </div>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </main>
  );
}
