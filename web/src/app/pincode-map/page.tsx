"use client";

import { useState, useMemo } from "react";
import Link from "next/link";
import hierarchyRaw from "@/lib/pincode-hierarchy.json";

type PincodeRow = {
  pincode: string;
  taluk: string;
  localities: string[];
};

type Constituency = {
  slug: string;
  name: string;
  lok_sabha: string;
  pincodes: PincodeRow[];
};

type District = {
  district: string;
  constituencies: Constituency[];
  pincode_count: number;
};

const HIERARCHY = hierarchyRaw as District[];
const ALL_DISTRICTS = HIERARCHY.map((d) => d.district);

function getTaluks(c: Constituency): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const p of c.pincodes) {
    const t = p.taluk.trim();
    if (t && !seen.has(t)) { seen.add(t); result.push(t); }
  }
  return result;
}

export default function PincodeMapPage() {
  const [search, setSearch] = useState("");
  const [filterDistrict, setFilterDistrict] = useState("all");
  const [expandedDistricts, setExpandedDistricts] = useState<Set<string>>(new Set());

  function toggleDistrict(dist: string) {
    setExpandedDistricts((prev) => {
      const next = new Set(prev);
      next.has(dist) ? next.delete(dist) : next.add(dist);
      return next;
    });
  }

  const q = search.trim().toLowerCase();
  const isFiltering = q !== "" || filterDistrict !== "all";

  const filteredHierarchy = useMemo(() => {
    return HIERARCHY
      .filter((d) => filterDistrict === "all" || d.district === filterDistrict)
      .map((d) => ({
        ...d,
        constituencies: d.constituencies.filter((c) => {
          if (!q) return true;
          if (d.district.toLowerCase().includes(q)) return true;
          if (c.name.toLowerCase().includes(q)) return true;
          if (c.lok_sabha.toLowerCase().includes(q)) return true;
          return c.pincodes.some(
            (p) =>
              p.pincode.includes(q) ||
              p.taluk.toLowerCase().includes(q) ||
              p.localities.some((l) => l.toLowerCase().includes(q))
          );
        }),
      }))
      .filter((d) => d.constituencies.length > 0);
  }, [q, filterDistrict]);

  const isDistrictOpen = (dist: string) =>
    isFiltering || expandedDistricts.has(dist);

  const totalConst = filteredHierarchy.reduce((s, d) => s + d.constituencies.length, 0);

  const thClass =
    "px-4 py-3 text-left text-xs font-bold text-gray-500 uppercase tracking-wide whitespace-nowrap";

  return (
    <main className="min-h-screen bg-gray-50">
      <header className="bg-white border-b border-gray-200 shadow-sm sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-4 py-3 flex items-center gap-4">
          <Link href="/" className="text-sm text-gray-500 hover:text-gray-900">
            ← Back
          </Link>
          <div>
            <h1 className="text-base font-black text-gray-900">
              Pincode → Constituency Map
            </h1>
            <p className="text-xs text-gray-400">
              {ALL_DISTRICTS.length} districts · 234 constituencies
            </p>
          </div>
        </div>
      </header>

      <div className="max-w-7xl mx-auto px-4 py-6 space-y-4">
        {/* Filters */}
        <div className="flex flex-col sm:flex-row gap-3">
          <input
            type="text"
            placeholder="Search pincode, district, constituency, taluk, locality…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="flex-1 px-4 py-2 text-sm rounded-xl border border-gray-300 focus:outline-none focus:ring-2 focus:ring-gray-900 bg-white"
          />
          <select
            value={filterDistrict}
            onChange={(e) => setFilterDistrict(e.target.value)}
            className="px-4 py-2 text-sm rounded-xl border border-gray-300 bg-white focus:outline-none focus:ring-2 focus:ring-gray-900"
          >
            <option value="all">All Districts</option>
            {ALL_DISTRICTS.map((d) => (
              <option key={d} value={d}>{d}</option>
            ))}
          </select>
        </div>

        {isFiltering && (
          <p className="text-xs text-gray-500">
            {totalConst} {totalConst === 1 ? "constituency" : "constituencies"} matched
            {q && <> for <span className="font-medium">&ldquo;{q}&rdquo;</span></>}
          </p>
        )}

        {/* Table */}
        <div className="bg-white rounded-2xl border border-gray-200 overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-sm border-collapse">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200">
                  <th className={thClass}>Constituency</th>
                  <th className={`${thClass} text-indigo-500`}>Lok Sabha</th>
                  <th className={thClass}>Taluk(s)</th>
                  <th className={thClass}>Pincode</th>
                  <th className={thClass}>Key Localities</th>
                </tr>
              </thead>
              <tbody>
                {filteredHierarchy.map((d) => {
                  const distOpen = isDistrictOpen(d.district);

                  return (
                    <>
                      {/* ── District header row ── */}
                      <tr
                        key={`dist-${d.district}`}
                        onClick={() => toggleDistrict(d.district)}
                        className="bg-gray-900 border-b border-gray-700 select-none cursor-pointer hover:bg-gray-800 transition-colors"
                      >
                        <td colSpan={5} className="px-4 py-2.5">
                          <div className="flex items-center justify-between">
                            <div className="flex items-center gap-3">
                              <span className="text-sm font-black text-white">
                                {d.district} District
                              </span>
                              <span className="text-xs text-gray-400">
                                {d.constituencies.length} constituencies
                              </span>
                            </div>
                            <span className="text-gray-400 text-xs">
                              {distOpen ? "▲" : "▼"}
                            </span>
                          </div>
                        </td>
                      </tr>

                      {/* ── Constituency groups ── */}
                      {distOpen &&
                        d.constituencies.map((c, cIdx) => {
                          const taluks = getTaluks(c);
                          const rows = c.pincodes.length > 0 ? c.pincodes : [{ pincode: "", taluk: "", localities: [] }];
                          const isLastConst = cIdx === d.constituencies.length - 1;

                          return rows.map((p, pIdx) => {
                            const isFirstRow = pIdx === 0;
                            const isLastRow = pIdx === rows.length - 1;
                            const rowSpan = rows.length;

                            // Bottom border: heavy on last constituency, light between pincodes
                            const borderClass = isLastRow
                              ? isLastConst
                                ? "border-b-2 border-gray-300"
                                : "border-b border-gray-200"
                              : "border-b border-gray-100";

                            return (
                              <tr
                                key={`${c.slug}-${p.pincode || pIdx}`}
                                className={`align-top ${borderClass}`}
                              >
                                {/* Constituency — only on first pincode row, spans all */}
                                {isFirstRow && (
                                  <td
                                    rowSpan={rowSpan}
                                    className="px-5 py-2.5 align-top border-r border-gray-100"
                                  >
                                    <Link
                                      href={`/constituency/${c.slug}`}
                                      className="text-xs font-semibold text-gray-900 hover:text-blue-600 hover:underline"
                                    >
                                      {c.name}
                                    </Link>
                                  </td>
                                )}

                                {/* Lok Sabha — only on first pincode row, spans all */}
                                {isFirstRow && (
                                  <td
                                    rowSpan={rowSpan}
                                    className="px-4 py-2.5 align-top text-xs text-indigo-700 whitespace-nowrap border-r border-gray-100"
                                  >
                                    {c.lok_sabha || <span className="text-gray-300">—</span>}
                                  </td>
                                )}

                                {/* Taluk(s) — only on first pincode row, spans all */}
                                {isFirstRow && (
                                  <td
                                    rowSpan={rowSpan}
                                    className="px-4 py-2.5 align-top border-r border-gray-100"
                                  >
                                    {taluks.length > 0 ? (
                                      <div className="flex flex-col gap-1">
                                        {taluks.map((t) => (
                                          <span
                                            key={t}
                                            className="inline-block bg-gray-100 text-gray-700 rounded px-1.5 py-0.5 text-[11px] whitespace-nowrap"
                                          >
                                            {t}
                                          </span>
                                        ))}
                                      </div>
                                    ) : (
                                      <span className="text-gray-300 text-xs">—</span>
                                    )}
                                  </td>
                                )}

                                {/* Pincode */}
                                <td className="px-4 py-2 text-xs font-mono text-gray-700 whitespace-nowrap">
                                  {p.pincode || <span className="text-gray-300">—</span>}
                                </td>

                                {/* Key Localities */}
                                <td className="px-4 py-2 text-xs text-gray-500">
                                  {p.localities.length > 0
                                    ? p.localities.join(", ")
                                    : <span className="text-gray-300">—</span>}
                                </td>
                              </tr>
                            );
                          });
                        })}
                    </>
                  );
                })}
              </tbody>
            </table>
          </div>

          {filteredHierarchy.length === 0 && (
            <div className="px-4 py-10 text-center text-sm text-gray-400">
              No results found.
            </div>
          )}
        </div>
      </div>
    </main>
  );
}
