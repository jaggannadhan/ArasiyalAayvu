"use client";

import { useState, useEffect, useMemo } from "react";
import Link from "next/link";
import { apiGet } from "@/lib/api-client";
import { useLanguage } from "@/lib/LanguageContext";

// ── Types ────────────────────────────────────────────────────────────────────

interface Clause {
  sub: string;   // "(1)", "(2)", "(1A)", or "" for single-clause articles
  text: string;  // Full text of the clause
}

interface Article {
  number: string;
  title: string;
  title_ta: string;
  is_landmark?: boolean;
  status?: string;
  amendment?: string;
  note?: string;
  clauses?: Clause[];
  full_text?: string;
  clause_count?: number;
}

interface Part {
  part_number: string;
  part_title: string;
  part_title_ta: string;
  articles: Article[];
  chapters?: { chapter: string; title: string; articles_range: string }[];
}

interface Schedule {
  number: number;
  title: string;
  title_ta: string;
  related_articles: string;
  is_landmark?: boolean;
  amendment?: string;
  detail?: string;
}

interface Amendment {
  number: number;
  year: number;
  title: string;
  title_ta: string;
  is_major?: boolean;
}

interface CentralAct {
  name: string;
  name_ta: string;
  year: number;
  replaces?: string;
  category: string;
  sections_count: number;
  source_url: string;
  note?: string;
}

interface Category {
  id: string;
  name: string;
  name_ta: string;
  part: string;
  icon: string;
}

interface ConstitutionMeta {
  title: string;
  title_ta: string;
  original_date: string;
  commencement_date: string;
  total_parts: number;
  total_parts_current: number;
  total_articles_listed: number;
  total_articles_original: number;
  total_articles_current: number;
  total_schedules: number;
  total_schedules_original: number;
  total_amendments: number;
  latest_amendment: number;
  latest_amendment_year: number;
  landmark_articles_count: number;
  source: string;
  source_url: string;
  alt_source_url: string;
  gazette_url: string;
  last_updated: string;
}

interface Preamble {
  text_en: string;
  text_ta: string;
  enacted_date: string;
  commenced_date: string;
}

interface ConstitutionData {
  meta: ConstitutionMeta;
  preamble: Preamble;
  parts: Part[];
  schedules: Schedule[];
  amendments: Amendment[];
  central_acts: CentralAct[];
  categories: Category[];
}

// ── Helpers ──────────────────────────────────────────────────────────────────

const CATEGORY_ICONS: Record<string, string> = {
  "balance-scale": "\u2696\uFE0F",
  compass: "\uD83E\uDDED",
  flag: "\uD83C\uDFF3\uFE0F",
  building: "\uD83C\uDFDB\uFE0F",
  landmark: "\uD83C\uDFE4",
  users: "\uD83D\uDC65",
  "vote-yea": "\uD83D\uDDF3\uFE0F",
  "exclamation-triangle": "\u26A0\uFE0F",
  edit: "\u270F\uFE0F",
  gavel: "\u2696\uFE0F",
  "shield-alt": "\uD83D\uDEE1\uFE0F",
};

const ACT_CATEGORY_COLORS: Record<string, { bg: string; text: string }> = {
  "Criminal Law": { bg: "bg-red-50", text: "text-red-700" },
  "Criminal Procedure": { bg: "bg-red-50", text: "text-red-600" },
  "Evidence Law": { bg: "bg-orange-50", text: "text-orange-700" },
  Governance: { bg: "bg-blue-50", text: "text-blue-700" },
  "Electoral Law": { bg: "bg-purple-50", text: "text-purple-700" },
  "Electoral Reform": { bg: "bg-purple-50", text: "text-purple-700" },
  "Women's Rights": { bg: "bg-pink-50", text: "text-pink-700" },
  Education: { bg: "bg-teal-50", text: "text-teal-700" },
  Taxation: { bg: "bg-amber-50", text: "text-amber-700" },
  "Consumer Rights": { bg: "bg-emerald-50", text: "text-emerald-700" },
  "Cyber Law": { bg: "bg-indigo-50", text: "text-indigo-700" },
  Transport: { bg: "bg-sky-50", text: "text-sky-700" },
  "Social Justice": { bg: "bg-violet-50", text: "text-violet-700" },
  "Child Protection": { bg: "bg-rose-50", text: "text-rose-700" },
  "Anti-Corruption": { bg: "bg-yellow-50", text: "text-yellow-700" },
  "Food Security": { bg: "bg-lime-50", text: "text-lime-700" },
  "Data Privacy": { bg: "bg-cyan-50", text: "text-cyan-700" },
  "Religious Property": { bg: "bg-orange-50", text: "text-orange-700" },
};

type Tab = "overview" | "parts" | "schedules" | "amendments" | "acts";

// ── Page ─────────────────────────────────────────────────────────────────────

export default function ConstitutionPage() {
  const { lang, setLang } = useLanguage();
  const isTA = lang === "ta";
  const [data, setData] = useState<ConstitutionData | null>(null);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState<Tab>("overview");
  const [expandedPart, setExpandedPart] = useState<string | null>(null);
  const [expandedArticle, setExpandedArticle] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");

  useEffect(() => {
    apiGet<ConstitutionData>("/api/constitution")
      .then(setData)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  // Filter articles by search
  const filteredParts = useMemo(() => {
    if (!data || !searchQuery.trim()) return data?.parts || [];
    const q = searchQuery.toLowerCase();
    return data.parts
      .map((part) => ({
        ...part,
        articles: part.articles.filter(
          (a) =>
            a.number.toLowerCase().includes(q) ||
            a.title.toLowerCase().includes(q) ||
            a.title_ta.includes(q) ||
            (a.amendment && a.amendment.toLowerCase().includes(q))
        ),
      }))
      .filter((part) =>
        part.articles.length > 0 ||
        part.part_title.toLowerCase().includes(q) ||
        part.part_title_ta.includes(q)
      );
  }, [data, searchQuery]);

  const filteredActs = useMemo(() => {
    if (!data || !searchQuery.trim()) return data?.central_acts || [];
    const q = searchQuery.toLowerCase();
    return data.central_acts.filter(
      (a) =>
        a.name.toLowerCase().includes(q) ||
        a.name_ta.includes(q) ||
        a.category.toLowerCase().includes(q) ||
        (a.replaces && a.replaces.toLowerCase().includes(q))
    );
  }, [data, searchQuery]);

  // ── Loading skeleton ──
  if (loading) {
    return (
      <main className="min-h-full bg-gray-50">
        <header className="sticky top-0 z-30 bg-white border-b border-gray-200 shadow-sm">
          <div className="max-w-3xl mx-auto px-4 py-3 flex items-center gap-3">
            <Link href="/" className="text-gray-400 hover:text-gray-600">
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
              </svg>
            </Link>
            <h1 className="text-lg font-black text-gray-900">
              {isTA ? "இந்திய அரசியலமைப்பு" : "The Indian Constitution"}
            </h1>
          </div>
        </header>
        <div className="max-w-3xl mx-auto px-4 py-8 space-y-6">
          {[1, 2, 3, 4, 5].map((i) => (
            <div key={i} className="h-32 bg-gray-100 rounded-2xl animate-pulse" />
          ))}
        </div>
      </main>
    );
  }

  if (!data) return null;

  const TABS: { id: Tab; label: string; label_ta: string }[] = [
    { id: "overview", label: "Overview", label_ta: "கண்ணோட்டம்" },
    { id: "parts", label: "Parts & Articles", label_ta: "பகுதிகள் & உறுப்புகள்" },
    { id: "schedules", label: "Schedules", label_ta: "அட்டவணைகள்" },
    { id: "amendments", label: "Amendments", label_ta: "திருத்தங்கள்" },
    { id: "acts", label: "Central Acts", label_ta: "மத்திய சட்டங்கள்" },
  ];

  return (
    <main className="min-h-full bg-gray-50">
      {/* Header */}
      <header className="sticky top-0 z-30 bg-white border-b border-gray-200 shadow-sm">
        <div className="max-w-3xl mx-auto px-4 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Link href="/" className="text-gray-400 hover:text-gray-600 transition-colors">
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
              </svg>
            </Link>
            <div>
              <h1 className="text-lg font-black text-gray-900">
                {isTA ? "இந்திய அரசியலமைப்பு" : "Law — The Indian Constitution"}
              </h1>
              <p className="text-[10px] text-gray-500 font-semibold uppercase tracking-wider">
                {isTA ? "சட்ட அமைப்பு வழிகாட்டி" : "Indian Legal System Guide"}
              </p>
            </div>
          </div>
          <button
            onClick={() => setLang(lang === "en" ? "ta" : "en")}
            className="text-xs font-bold px-3 py-1.5 rounded-full border border-gray-300 hover:bg-gray-100 transition-colors text-gray-900"
          >
            {lang === "en" ? "\u0BA4\u0BAE\u0BBF\u0BB4\u0BCD" : "English"}
          </button>
        </div>
      </header>

      <div className="max-w-3xl mx-auto px-4 py-6 space-y-6">
        {/* Preamble Banner */}
        <div className="bg-gradient-to-br from-amber-50 to-orange-50 border-2 border-amber-200 rounded-2xl p-5 space-y-3">
          <div className="flex items-center gap-2 mb-2">
            <span className="text-2xl">{"\u2696\uFE0F"}</span>
            <span className="text-xs font-black uppercase tracking-wider text-amber-800">
              {isTA ? "முகவுரை" : "Preamble"}
            </span>
          </div>
          <p className="text-sm text-amber-900 leading-relaxed italic">
            &ldquo;{isTA ? data.preamble.text_ta : data.preamble.text_en}&rdquo;
          </p>
          <div className="flex items-center gap-4 text-[10px] text-amber-700">
            <span>{isTA ? "இயற்றப்பட்ட நாள்" : "Enacted"}: {data.preamble.enacted_date}</span>
            <span>{isTA ? "நடைமுறைக்கு வந்த நாள்" : "Commenced"}: {data.preamble.commenced_date}</span>
          </div>
        </div>

        {/* Stats Grid */}
        <div className="grid grid-cols-4 gap-2">
          {[
            { value: data.meta.total_parts_current, label: isTA ? "பகுதிகள்" : "Parts", color: "text-blue-700" },
            { value: data.meta.total_articles_current, label: isTA ? "உறுப்புகள்" : "Articles", color: "text-emerald-700" },
            { value: data.meta.total_schedules, label: isTA ? "அட்டவணைகள்" : "Schedules", color: "text-purple-700" },
            { value: data.meta.latest_amendment, label: isTA ? "திருத்தங்கள்" : "Amendments", color: "text-orange-700" },
          ].map((s) => (
            <div key={s.label} className="bg-white rounded-xl border border-gray-200 p-3 text-center">
              <p className={`text-xl font-black ${s.color}`}>{s.value}</p>
              <p className="text-[9px] text-gray-500 font-semibold">{s.label}</p>
            </div>
          ))}
        </div>

        {/* Original vs Current */}
        <div className="bg-blue-50 border border-blue-200 rounded-xl p-3 flex items-center justify-between text-[10px]">
          <span className="text-blue-800 font-semibold">
            {isTA ? "1950 அசல்: 395 உறுப்புகள், 22 பகுதிகள், 8 அட்டவணைகள்" : "Original (1950): 395 Articles, 22 Parts, 8 Schedules"}
          </span>
          <span className="text-blue-600 font-bold">
            {isTA ? "இன்று: 448 உறுப்புகள்" : `Current: ${data.meta.total_articles_current} Articles`}
          </span>
        </div>

        {/* Tab Bar */}
        <div className="flex gap-1 overflow-x-auto no-scrollbar pb-1">
          {TABS.map((tab) => (
            <button
              key={tab.id}
              onClick={() => { setActiveTab(tab.id); setSearchQuery(""); }}
              className={`text-xs font-bold px-3 py-2 rounded-full whitespace-nowrap transition-all ${
                activeTab === tab.id
                  ? "bg-gray-900 text-white"
                  : "bg-white text-gray-600 border border-gray-200 hover:bg-gray-100"
              }`}
            >
              {isTA ? tab.label_ta : tab.label}
            </button>
          ))}
        </div>

        {/* Search (for Parts & Acts tabs) */}
        {(activeTab === "parts" || activeTab === "acts") && (
          <div className="relative">
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder={
                isTA
                  ? (activeTab === "parts" ? "உறுப்பு எண் அல்லது தலைப்பு தேடுங்கள்..." : "சட்டப் பெயர் தேடுங்கள்...")
                  : (activeTab === "parts" ? "Search article number or title..." : "Search act name or category...")
              }
              className="w-full bg-white border border-gray-200 rounded-xl px-4 py-2.5 text-sm text-gray-900 placeholder-gray-400 focus:outline-none focus:border-gray-400"
            />
            {searchQuery && (
              <button
                onClick={() => setSearchQuery("")}
                className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
              >
                {"\u2715"}
              </button>
            )}
          </div>
        )}

        {/* ═══ OVERVIEW TAB ═══ */}
        {activeTab === "overview" && (
          <div className="space-y-6">
            {/* Quick Categories */}
            <section className="space-y-3">
              <p className="text-xs font-bold text-gray-500 uppercase tracking-wide">
                {isTA ? "வகைகள்" : "Browse by Category"}
              </p>
              <div className="grid grid-cols-2 gap-2">
                {data.categories.filter((c) => c.part !== "central_acts").map((cat) => (
                  <button
                    key={cat.id}
                    onClick={() => {
                      setActiveTab("parts");
                      setExpandedPart(cat.part.split("/")[0]);
                    }}
                    className="bg-white rounded-xl border border-gray-200 p-3 text-left hover:border-gray-400 hover:shadow-sm transition-all"
                  >
                    <span className="text-lg">{CATEGORY_ICONS[cat.icon] || "\uD83D\uDCC4"}</span>
                    <p className="text-xs font-bold text-gray-900 mt-1">
                      {isTA ? cat.name_ta : cat.name}
                    </p>
                    <p className="text-[9px] text-gray-400">
                      {isTA ? `\u0BAA\u0B95\u0BC1\u0BA4\u0BBF ${cat.part}` : `Part ${cat.part}`}
                    </p>
                  </button>
                ))}
              </div>
            </section>

            {/* Landmark Articles */}
            <section className="space-y-3">
              <p className="text-xs font-bold text-gray-500 uppercase tracking-wide">
                {isTA ? "முக்கிய உறுப்புகள்" : "Landmark Articles"}
              </p>
              <div className="space-y-2">
                {data.parts.flatMap((p) =>
                  p.articles
                    .filter((a) => a.is_landmark)
                    .map((a) => ({ ...a, partNumber: p.part_number, partTitle: p.part_title }))
                ).map((a) => (
                  <div
                    key={a.number}
                    className="bg-white rounded-xl border border-gray-200 p-3 flex items-start gap-3"
                  >
                    <div className="bg-amber-100 text-amber-800 text-xs font-black px-2 py-1 rounded-lg flex-shrink-0">
                      Art. {a.number}
                    </div>
                    <div className="min-w-0">
                      <p className="text-xs font-semibold text-gray-900">
                        {isTA ? a.title_ta : a.title}
                      </p>
                      <p className="text-[9px] text-gray-400 mt-0.5">
                        Part {a.partNumber} — {a.partTitle}
                      </p>
                    </div>
                  </div>
                ))}
              </div>
            </section>

            {/* Major Amendments Timeline */}
            <section className="space-y-3">
              <p className="text-xs font-bold text-gray-500 uppercase tracking-wide">
                {isTA ? "முக்கிய திருத்தங்கள்" : "Major Constitutional Amendments"}
              </p>
              <div className="space-y-2">
                {data.amendments
                  .filter((a) => a.is_major)
                  .map((a) => (
                    <div
                      key={a.number}
                      className="bg-white rounded-xl border border-gray-200 p-3 flex items-start gap-3"
                    >
                      <div className="text-center flex-shrink-0 w-14">
                        <p className="text-sm font-black text-gray-900">{a.year}</p>
                        <p className="text-[8px] text-gray-400 font-bold">
                          {isTA ? `${a.number}வது` : `${a.number}${a.number === 1 ? "st" : a.number === 2 ? "nd" : a.number === 3 ? "rd" : "th"}`}
                        </p>
                      </div>
                      <p className="text-xs text-gray-700 leading-relaxed">
                        {isTA ? a.title_ta : a.title}
                      </p>
                    </div>
                  ))}
              </div>
            </section>

            {/* Central Acts Quick View */}
            <section className="space-y-3">
              <div className="flex items-center justify-between">
                <p className="text-xs font-bold text-gray-500 uppercase tracking-wide">
                  {isTA ? "முக்கிய மத்திய சட்டங்கள்" : "Key Central Acts"}
                </p>
                <button
                  onClick={() => setActiveTab("acts")}
                  className="text-[10px] font-bold text-blue-600 hover:text-blue-800"
                >
                  {isTA ? "அனைத்தும் காண" : "View All"} {"\u2192"}
                </button>
              </div>
              <div className="grid grid-cols-1 gap-2">
                {data.central_acts.slice(0, 5).map((act) => {
                  const colors = ACT_CATEGORY_COLORS[act.category] || { bg: "bg-gray-50", text: "text-gray-700" };
                  return (
                    <a
                      key={act.name}
                      href={act.source_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="bg-white rounded-xl border border-gray-200 p-3 hover:border-gray-400 hover:shadow-sm transition-all"
                    >
                      <div className="flex items-start justify-between gap-2">
                        <div className="min-w-0">
                          <p className="text-xs font-bold text-gray-900">
                            {isTA ? act.name_ta : act.name}
                          </p>
                          {act.replaces && (
                            <p className="text-[9px] text-gray-400 mt-0.5">
                              {isTA ? "\u0BAA\u0BA4\u0BBF\u0BB2\u0BBE\u0B95: " : "Replaces: "}{act.replaces}
                            </p>
                          )}
                        </div>
                        <div className="flex items-center gap-1.5 flex-shrink-0">
                          <span className={`text-[8px] font-bold px-1.5 py-0.5 rounded-full ${colors.bg} ${colors.text}`}>
                            {act.category}
                          </span>
                          <span className="text-[10px] font-bold text-gray-500">{act.year}</span>
                        </div>
                      </div>
                    </a>
                  );
                })}
              </div>
            </section>
          </div>
        )}

        {/* ═══ PARTS & ARTICLES TAB ═══ */}
        {activeTab === "parts" && (
          <div className="space-y-2">
            {filteredParts.map((part) => {
              const isExpanded = expandedPart === part.part_number;
              return (
                <div key={part.part_number} className="bg-white rounded-xl border border-gray-200 overflow-hidden">
                  <button
                    onClick={() => setExpandedPart(isExpanded ? null : part.part_number)}
                    className="w-full px-4 py-3 flex items-center justify-between text-left hover:bg-gray-50 transition-colors"
                  >
                    <div className="flex items-center gap-3 min-w-0">
                      <span className="text-[10px] font-black text-blue-600 bg-blue-50 px-2 py-0.5 rounded-md flex-shrink-0">
                        {part.part_number}
                      </span>
                      <div className="min-w-0">
                        <p className="text-xs font-bold text-gray-900 truncate">
                          {isTA ? part.part_title_ta : part.part_title}
                        </p>
                        <p className="text-[9px] text-gray-400">
                          {part.articles.length} {isTA ? "உறுப்புகள்" : "articles"}
                        </p>
                      </div>
                    </div>
                    <svg
                      className={`w-4 h-4 text-gray-400 transition-transform ${isExpanded ? "rotate-180" : ""}`}
                      fill="none" stroke="currentColor" viewBox="0 0 24 24"
                    >
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                    </svg>
                  </button>

                  {isExpanded && (
                    <div className="px-4 pb-3 space-y-1.5 border-t border-gray-100 pt-2">
                      {part.chapters && part.chapters.length > 0 && (
                        <div className="flex flex-wrap gap-1.5 mb-2">
                          {part.chapters.map((ch) => (
                            <span
                              key={ch.chapter}
                              className="text-[8px] font-semibold px-2 py-0.5 rounded-full bg-gray-100 text-gray-600"
                            >
                              Ch. {ch.chapter}: {ch.title} (Art. {ch.articles_range})
                            </span>
                          ))}
                        </div>
                      )}
                      {part.articles.map((article) => {
                        const isArticleExpanded = expandedArticle === `${part.part_number}_${article.number}`;
                        const hasClauses = article.clauses && article.clauses.length > 0;
                        return (
                          <div key={article.number} className="space-y-0">
                            <button
                              onClick={() => {
                                if (hasClauses) {
                                  setExpandedArticle(isArticleExpanded ? null : `${part.part_number}_${article.number}`);
                                }
                              }}
                              className={`w-full flex items-start gap-2 py-1.5 px-2 rounded-lg text-left transition-colors ${
                                article.is_landmark ? "bg-amber-50" : article.status === "Repealed" ? "bg-gray-50" : ""
                              } ${hasClauses ? "hover:bg-gray-50 cursor-pointer" : "cursor-default"}`}
                            >
                              <span
                                className={`text-[9px] font-bold px-1.5 py-0.5 rounded flex-shrink-0 ${
                                  article.is_landmark
                                    ? "bg-amber-200 text-amber-800"
                                    : article.status === "Repealed"
                                      ? "bg-gray-200 text-gray-500 line-through"
                                      : "bg-gray-100 text-gray-600"
                                }`}
                              >
                                {article.number}
                              </span>
                              <div className="min-w-0 flex-1">
                                <p className={`text-[11px] leading-snug ${
                                  article.status === "Repealed" ? "text-gray-400 line-through" : "text-gray-800"
                                }`}>
                                  {isTA ? article.title_ta : article.title}
                                </p>
                                {(article.amendment || article.note) && (
                                  <p className="text-[8px] text-blue-500 mt-0.5">
                                    {article.amendment || article.note}
                                  </p>
                                )}
                                {article.status === "Repealed" && (
                                  <p className="text-[8px] text-red-400 mt-0.5">
                                    {isTA ? "\u0BA8\u0BC0\u0B95\u0BCD\u0B95\u0BAA\u0BCD\u0BAA\u0B9F\u0BCD\u0B9F\u0BA4\u0BC1" : "Repealed"}
                                  </p>
                                )}
                              </div>
                              <div className="flex items-center gap-1 flex-shrink-0">
                                {article.is_landmark && (
                                  <span className="text-[7px] font-bold px-1 py-0.5 rounded bg-amber-100 text-amber-700">
                                    {isTA ? "\u0BAE\u0BC1\u0B95\u0BCD\u0B95\u0BBF\u0BAF\u0BAE\u0BCD" : "KEY"}
                                  </span>
                                )}
                                {hasClauses && (
                                  <svg
                                    className={`w-3 h-3 text-gray-400 transition-transform ${isArticleExpanded ? "rotate-180" : ""}`}
                                    fill="none" stroke="currentColor" viewBox="0 0 24 24"
                                  >
                                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                                  </svg>
                                )}
                              </div>
                            </button>

                            {/* Clause expansion */}
                            {isArticleExpanded && hasClauses && (
                              <div className="ml-8 mr-2 mb-2 mt-1 bg-white border border-gray-200 rounded-lg p-3 space-y-2.5">
                                {article.clauses!.map((clause, ci) => (
                                  <div key={ci} className="flex gap-2">
                                    {clause.sub && (
                                      <span className="text-[9px] font-bold text-blue-600 bg-blue-50 px-1.5 py-0.5 rounded flex-shrink-0 h-fit">
                                        {clause.sub}
                                      </span>
                                    )}
                                    <p className="text-[11px] text-gray-700 leading-relaxed">
                                      {clause.text}
                                    </p>
                                  </div>
                                ))}
                                <p className="text-[8px] text-gray-400 pt-1 border-t border-gray-100">
                                  {isTA ? "\u0B86\u0BA4\u0BBE\u0BB0\u0BAE\u0BCD" : "Source"}: India Code (indiacode.nic.in) {"\u2014"} {isTA ? "\u0B9A\u0B9F\u0BCD\u0B9F\u0BAE\u0BCD \u0BAE\u0BB1\u0BCD\u0BB1\u0BC1\u0BAE\u0BCD \u0BA8\u0BC0\u0BA4\u0BBF \u0B85\u0BAE\u0BC8\u0B9A\u0BCD\u0B9A\u0B95\u0BAE\u0BCD" : "Legislative Dept., Ministry of Law & Justice"}
                                </p>
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              );
            })}
            {filteredParts.length === 0 && searchQuery && (
              <div className="text-center py-8">
                <p className="text-sm text-gray-400">
                  {isTA ? "முடிவுகள் இல்லை" : "No results found"}
                </p>
              </div>
            )}
          </div>
        )}

        {/* ═══ SCHEDULES TAB ═══ */}
        {activeTab === "schedules" && (
          <div className="space-y-2">
            {data.schedules.map((schedule) => (
              <div
                key={schedule.number}
                className={`bg-white rounded-xl border p-4 ${
                  schedule.is_landmark ? "border-amber-300 bg-amber-50/30" : "border-gray-200"
                }`}
              >
                <div className="flex items-start gap-3">
                  <div className="bg-purple-100 text-purple-800 text-xs font-black px-2.5 py-1 rounded-lg flex-shrink-0">
                    {schedule.number}
                  </div>
                  <div className="min-w-0 space-y-1">
                    <p className="text-xs font-bold text-gray-900">
                      {isTA ? schedule.title_ta : schedule.title}
                    </p>
                    <p className="text-[9px] text-gray-500">
                      {isTA ? "தொடர்புடைய உறுப்புகள்" : "Related Articles"}: {schedule.related_articles}
                    </p>
                    {schedule.detail && (
                      <p className="text-[9px] text-gray-400 leading-relaxed">
                        {schedule.detail}
                      </p>
                    )}
                    {schedule.amendment && (
                      <p className="text-[8px] text-blue-500">{schedule.amendment}</p>
                    )}
                  </div>
                  {schedule.is_landmark && (
                    <span className="text-[7px] font-bold px-1 py-0.5 rounded bg-amber-100 text-amber-700 flex-shrink-0">
                      {isTA ? "முக்கியம்" : "KEY"}
                    </span>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* ═══ AMENDMENTS TAB ═══ */}
        {activeTab === "amendments" && (
          <div className="space-y-2">
            {data.amendments.map((amendment) => (
              <div
                key={amendment.number}
                className={`bg-white rounded-xl border p-4 ${
                  amendment.is_major ? "border-orange-300 bg-orange-50/30" : "border-gray-200"
                }`}
              >
                <div className="flex items-start gap-3">
                  <div className="text-center flex-shrink-0 w-12">
                    <p className="text-sm font-black text-gray-900">{amendment.number}</p>
                    <p className="text-[9px] text-gray-400">{amendment.year}</p>
                  </div>
                  <p className="text-xs text-gray-700 leading-relaxed flex-1">
                    {isTA ? amendment.title_ta : amendment.title}
                  </p>
                  {amendment.is_major && (
                    <span className="text-[7px] font-bold px-1.5 py-0.5 rounded-full bg-orange-100 text-orange-700 flex-shrink-0">
                      {isTA ? "பெரிய" : "MAJOR"}
                    </span>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* ═══ CENTRAL ACTS TAB ═══ */}
        {activeTab === "acts" && (
          <div className="space-y-2">
            {filteredActs.map((act) => {
              const colors = ACT_CATEGORY_COLORS[act.category] || { bg: "bg-gray-50", text: "text-gray-700" };
              return (
                <a
                  key={act.name}
                  href={act.source_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="block bg-white rounded-xl border border-gray-200 p-4 hover:border-gray-400 hover:shadow-sm transition-all"
                >
                  <div className="flex items-start justify-between gap-2 mb-1.5">
                    <p className="text-sm font-bold text-gray-900">
                      {isTA ? act.name_ta : act.name}
                    </p>
                    <span className="text-sm font-black text-gray-400 flex-shrink-0">{act.year}</span>
                  </div>
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className={`text-[9px] font-bold px-2 py-0.5 rounded-full ${colors.bg} ${colors.text}`}>
                      {act.category}
                    </span>
                    {act.sections_count > 0 && (
                      <span className="text-[9px] text-gray-400">
                        {act.sections_count} {isTA ? "பிரிவுகள்" : "sections"}
                      </span>
                    )}
                    {act.replaces && (
                      <span className="text-[9px] text-red-400">
                        {isTA ? "\u0BAA\u0BA4\u0BBF\u0BB2\u0BBE\u0B95" : "Replaces"}: {act.replaces}
                      </span>
                    )}
                  </div>
                  {act.note && (
                    <p className="text-[9px] text-blue-500 mt-1">{act.note}</p>
                  )}
                </a>
              );
            })}
            {filteredActs.length === 0 && searchQuery && (
              <div className="text-center py-8">
                <p className="text-sm text-gray-400">
                  {isTA ? "முடிவுகள் இல்லை" : "No results found"}
                </p>
              </div>
            )}
          </div>
        )}

        {/* Source */}
        <p className="text-center text-[10px] text-gray-400 pb-4 pt-2">
          {isTA ? "ஆதாரங்கள்: " : "Sources: "}
          <a href="https://www.indiacode.nic.in/handle/123456789/8305" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">
            India Code (Ministry of Law & Justice)
          </a>
          {" \u00B7 "}
          <a href="https://legislative.gov.in/constitution-of-india" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">
            Legislative Dept.
          </a>
          {" \u00B7 "}
          <a href="https://egazette.gov.in/" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">
            e-Gazette of India
          </a>
        </p>
      </div>
    </main>
  );
}
