"use client";

import { useState, useEffect } from "react";
import Link from "next/link";
import { apiGet } from "@/lib/api-client";
import { useLanguage } from "@/lib/LanguageContext";
import { NewsReaderPlayer } from "@/components/news/NewsReaderPlayer";

interface ThreadArticle {
  doc_id: string;
  title: string;
  summary: string;
  snippet: string;
  source_url: string;
  source_name: string;
  category: string;
  published_at: string;
  heat_score: number;
  is_breaking: boolean;
  is_anchor: boolean;
  sentiment: number;
  sdg_alignment: string[];
}

interface NewsThread {
  anchor_title: string;
  thread_entity: string;
  thread_label: string;
  article_count: number;
  shared_entities: string[];
  articles: ThreadArticle[];
}

interface ThreadsResponse {
  threads: NewsThread[];
  count: number;
}

const CAT_COLOR: Record<string, string> = {
  POLITICS: "bg-red-500",
  BUSINESS: "bg-blue-500",
  HEALTH: "bg-green-500",
  SCIENCE_TECH: "bg-purple-500",
  WORLD: "bg-gray-500",
  ENTERTAINMENT: "bg-pink-500",
  SPORTS: "bg-orange-500",
};

function timeAgo(dateStr: string): string {
  const diff = Date.now() - new Date(dateStr).getTime();
  const min = Math.floor(diff / 60000);
  if (min < 60) return `${min}m`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h`;
  const days = Math.floor(hr / 24);
  return `${days}d`;
}

function formatDate(dateStr: string): string {
  const d = new Date(dateStr);
  return d.toLocaleDateString("en-IN", { month: "short", day: "numeric" });
}

function ThreadCard({ article, fullWidth }: { article: ThreadArticle; fullWidth?: boolean }) {
  const catColor = CAT_COLOR[article.category] ?? "bg-gray-400";

  return (
    <a
      href={article.source_url}
      target="_blank"
      rel="noopener noreferrer"
      className={`${fullWidth ? "w-full" : "shrink-0 w-56"} bg-white rounded-xl border shadow-sm hover:shadow-md transition-all overflow-hidden flex flex-col ${
        article.is_anchor
          ? "border-amber-300 ring-2 ring-amber-200"
          : "border-gray-200"
      }`}
    >
      {/* Top edge — category color bar */}
      <div className={`h-1 ${catColor}`} />

      <div className="p-3 flex-1 flex flex-col">
        {/* Date + breaking badge */}
        <div className="flex items-center justify-between mb-1.5">
          <span className="text-[9px] font-semibold text-gray-400">
            {formatDate(article.published_at)}
          </span>
          {article.is_breaking && (
            <span className="text-[8px] font-bold px-1.5 py-0.5 rounded-full bg-red-600 text-white">
              LIVE
            </span>
          )}
          {article.is_anchor && !article.is_breaking && (
            <span className="text-[8px] font-bold px-1.5 py-0.5 rounded-full bg-amber-100 text-amber-700">
              TODAY
            </span>
          )}
        </div>

        {/* Title — 3 lines max */}
        <h4 className="text-xs font-bold text-gray-900 leading-snug line-clamp-3 flex-1">
          {article.title}
        </h4>

        {/* Summary — 2 lines */}
        {article.summary && (
          <p className="text-[10px] text-gray-500 leading-relaxed line-clamp-2 mt-1.5">
            {article.summary}
          </p>
        )}

        {/* Footer */}
        <div className="flex items-center justify-between mt-2 pt-1.5 border-t border-gray-100">
          <span className="text-[9px] text-gray-400 truncate max-w-[100px]">
            {article.source_name}
          </span>
          <span className="text-[9px] text-gray-400">
            {timeAgo(article.published_at)}
          </span>
        </div>
      </div>
    </a>
  );
}

function ThreadRow({ thread, isTA }: { thread: NewsThread; isTA: boolean }) {
  const isSingle = thread.article_count === 1;

  return (
    <div className="space-y-2">
      {/* Thread header — the "string" label */}
      <div className="flex items-center gap-2 px-1">
        <div className="w-2 h-2 rounded-full bg-amber-400 shadow-sm shadow-amber-200" />
        <h3 className="text-sm font-black text-gray-900 truncate">
          {thread.thread_label}
        </h3>
        {!isSingle && (
          <span className="text-[10px] text-gray-400 shrink-0">
            {thread.article_count} {isTA ? "செய்திகள்" : "articles"}
          </span>
        )}
        <div className="flex-1 border-t border-dashed border-gray-200" />
        <div className="w-2 h-2 rounded-full bg-amber-400 shadow-sm shadow-amber-200" />
      </div>

      {/* The "string of lights" — horizontally scrollable cards */}
      <div className="relative">
        {/* The wire/string line behind the cards */}
        <div className="absolute top-0 left-3 right-3 h-px bg-gradient-to-r from-amber-200 via-amber-300 to-amber-200" />

        {/* Cards hanging from the string */}
        <div className={`flex gap-3 pb-2 pt-3 px-1 ${isSingle ? "" : "overflow-x-auto no-scrollbar snap-x snap-mandatory"}`}>
          {thread.articles.map((article) => (
            <div key={article.doc_id} className={`snap-start flex flex-col items-center ${isSingle ? "w-full" : ""}`}>
              {/* "Light" bulb connector */}
              <div className="w-1.5 h-3 bg-gradient-to-b from-amber-300 to-amber-100 rounded-b-full mb-1" />
              <ThreadCard article={article} fullWidth={isSingle} />
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export default function NewsPage() {
  const { lang, setLang } = useLanguage();
  const isTA = lang === "ta";
  const [threads, setThreads] = useState<NewsThread[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    apiGet<ThreadsResponse>("/api/news/threads?top_n=10")
      .then((data) => setThreads(data.threads))
      .catch(() => setThreads([]))
      .finally(() => setLoading(false));
  }, []);

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
                {isTA ? "செய்திகள் & பகுப்பாய்வு" : "News & Analysis"}
              </h1>
              <p className="text-[10px] text-gray-500 font-semibold uppercase tracking-wider">
                {isTA ? "தமிழ்நாடு · AI-NER · கதை நூல்கள்" : "Tamil Nadu · AI-NER · Story Threads"}
              </p>
            </div>
          </div>
          <button
            onClick={() => setLang(lang === "en" ? "ta" : "en")}
            className="text-xs font-bold px-3 py-1.5 rounded-full border border-gray-300 hover:bg-gray-100 transition-colors text-gray-900"
          >
            {lang === "en" ? "தமிழ்" : "English"}
          </button>
        </div>
      </header>

      <div className="max-w-3xl mx-auto px-4 py-6 space-y-8">
        {/* AI News Reader — self-contained: fetches its own metadata from GCS */}
        <NewsReaderPlayer lang={lang} />

        {/* Loading */}
        {loading && (
          <div className="space-y-8">
            {[1, 2, 3].map((i) => (
              <div key={i} className="space-y-2">
                <div className="h-4 w-32 bg-gray-100 rounded animate-pulse" />
                <div className="flex gap-3 overflow-hidden">
                  {[1, 2, 3, 4].map((j) => (
                    <div key={j} className="w-56 h-36 shrink-0 bg-gray-100 rounded-xl animate-pulse" />
                  ))}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Empty state */}
        {!loading && threads.length === 0 && (
          <div className="text-center py-16">
            <p className="text-3xl mb-3">📰</p>
            <p className="text-sm font-semibold text-gray-600">
              {isTA ? "இன்று செய்தி நூல்கள் எதுவும் இல்லை" : "No story threads yet"}
            </p>
            <p className="text-xs text-gray-400 mt-1">
              {isTA ? "செய்திகள் விரைவில் வரும்" : "News threads will appear as articles are ingested"}
            </p>
          </div>
        )}

        {/* Thread rows */}
        {!loading && threads.map((thread, i) => (
          <ThreadRow key={`${thread.thread_entity}-${i}`} thread={thread} isTA={isTA} />
        ))}

        {/* Footer */}
        <p className="text-center text-[10px] text-gray-400 pb-4 pt-2">
          {isTA ? "ஆதாரம்: " : "Source: "}
          <a
            href="https://frontendportal-nine.vercel.app"
            target="_blank"
            rel="noopener noreferrer"
            className="underline underline-offset-2 hover:text-gray-600"
          >
            OmnesVident
          </a>
          {" · "}
          {isTA ? "NER: Gemini 2.5 Flash · நிறுவன இணைப்பு மூலம் நூல்கள்" : "NER: Gemini 2.5 Flash · Threads via entity overlap"}
        </p>
      </div>
    </main>
  );
}
