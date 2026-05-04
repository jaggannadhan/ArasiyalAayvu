"use client";

import { useState, useEffect } from "react";
import Link from "next/link";
import { apiGet } from "@/lib/api-client";
import { useLanguage } from "@/lib/LanguageContext";
import { NewsReaderPlayer } from "@/components/news/NewsReaderPlayer";

interface NewsArticle {
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
  sdg_alignment: string[];
}

const CAT_META: Record<string, { color: string; label: string; labelTA: string }> = {
  POLITICS: { color: "bg-red-500", label: "Politics", labelTA: "அரசியல்" },
  BUSINESS: { color: "bg-blue-500", label: "Business", labelTA: "வணிகம்" },
  HEALTH: { color: "bg-green-500", label: "Health", labelTA: "சுகாதாரம்" },
  SCIENCE_TECH: { color: "bg-purple-500", label: "Science & Tech", labelTA: "அறிவியல்" },
  WORLD: { color: "bg-gray-500", label: "World", labelTA: "உலகம்" },
  ENTERTAINMENT: { color: "bg-pink-500", label: "Entertainment", labelTA: "பொழுதுபோக்கு" },
  SPORTS: { color: "bg-orange-500", label: "Sports", labelTA: "விளையாட்டு" },
};

function timeAgo(dateStr: string): string {
  const diff = Date.now() - new Date(dateStr).getTime();
  const min = Math.floor(diff / 60000);
  if (min < 60) return `${min}m`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h`;
  return `${Math.floor(hr / 24)}d`;
}

export default function NewsPage() {
  const { lang, setLang } = useLanguage();
  const isTA = lang === "ta";
  const [articles, setArticles] = useState<NewsArticle[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    apiGet<{ articles: NewsArticle[] }>("/api/news?limit=100")
      .then((data) => {
        // Filter to today's articles only
        const today = new Date().toISOString().slice(0, 10);
        const todayArticles = (data.articles || []).filter(
          (a) => a.published_at?.slice(0, 10) === today
        );
        setArticles(todayArticles);
      })
      .catch(() => setArticles([]))
      .finally(() => setLoading(false));
  }, []);

  // Group by category
  const grouped: Record<string, NewsArticle[]> = {};
  for (const a of articles) {
    const cat = a.category || "OTHER";
    if (!grouped[cat]) grouped[cat] = [];
    grouped[cat].push(a);
  }

  // Sort categories by article count (most articles first)
  const sortedCategories = Object.keys(grouped).sort(
    (a, b) => grouped[b].length - grouped[a].length
  );

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
                {isTA ? "தமிழ்நாடு · இன்றைய செய்திகள்" : "Tamil Nadu · Today's News"}
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
        {/* News Recordings */}
        <NewsReaderPlayer lang={lang} />

        {/* Today's News heading */}
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-black text-gray-900">
            {isTA ? "இன்றைய செய்திகள்" : "Today's News"}
          </h2>
          {!loading && (
            <span className="text-[10px] text-gray-400 font-semibold">
              {articles.length} {isTA ? "செய்திகள்" : "articles"}
            </span>
          )}
        </div>

        {/* Loading */}
        {loading && (
          <div className="space-y-6">
            {[1, 2, 3].map((i) => (
              <div key={i} className="space-y-2">
                <div className="h-4 w-24 bg-gray-200 rounded animate-pulse" />
                <div className="grid grid-cols-2 gap-3">
                  {[1, 2].map((j) => (
                    <div key={j} className="h-28 bg-gray-100 rounded-xl animate-pulse" />
                  ))}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Empty state */}
        {!loading && articles.length === 0 && (
          <div className="text-center py-12">
            <p className="text-sm font-semibold text-gray-600">
              {isTA ? "இன்று செய்திகள் எதுவும் இல்லை" : "No news articles today"}
            </p>
            <p className="text-xs text-gray-400 mt-1">
              {isTA ? "செய்திகள் விரைவில் வரும்" : "Articles will appear as they are ingested"}
            </p>
          </div>
        )}

        {/* Articles grouped by category */}
        {!loading && sortedCategories.map((cat) => {
          const catMeta = CAT_META[cat] || { color: "bg-gray-400", label: cat, labelTA: cat };
          const catArticles = grouped[cat];

          return (
            <div key={cat} className="space-y-3">
              {/* Category header */}
              <div className="flex items-center gap-2">
                <div className={`w-2 h-2 rounded-full ${catMeta.color}`} />
                <h3 className="text-xs font-black text-gray-800 uppercase tracking-wide">
                  {isTA ? catMeta.labelTA : catMeta.label}
                </h3>
                <span className="text-[9px] text-gray-400">{catArticles.length}</span>
                <div className="flex-1 border-t border-gray-200" />
              </div>

              {/* Article cards */}
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                {catArticles.map((article) => (
                  <a
                    key={article.doc_id}
                    href={article.source_url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="bg-white rounded-xl border border-gray-200 hover:shadow-md transition-all overflow-hidden flex flex-col"
                  >
                    <div className={`h-1 ${catMeta.color}`} />
                    <div className="p-3 flex-1 flex flex-col">
                      {/* Title */}
                      <h4 className="text-xs font-bold text-gray-900 leading-snug line-clamp-2 flex-1">
                        {article.title}
                      </h4>

                      {/* Summary */}
                      {(article.summary || article.snippet) && (
                        <p className="text-[10px] text-gray-500 leading-relaxed line-clamp-2 mt-1.5">
                          {article.summary || article.snippet}
                        </p>
                      )}

                      {/* Footer */}
                      <div className="flex items-center justify-between mt-2 pt-1.5 border-t border-gray-100">
                        <span className="text-[9px] text-gray-400 truncate max-w-[120px]">
                          {article.source_name}
                        </span>
                        <div className="flex items-center gap-2">
                          {article.is_breaking && (
                            <span className="text-[8px] font-bold px-1.5 py-0.5 rounded-full bg-red-600 text-white">
                              LIVE
                            </span>
                          )}
                          <span className="text-[9px] text-gray-400">
                            {timeAgo(article.published_at)}
                          </span>
                        </div>
                      </div>
                    </div>
                  </a>
                ))}
              </div>
            </div>
          );
        })}

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
          {isTA ? "NER: Gemini 2.5 Flash" : "NER: Gemini 2.5 Flash"}
        </p>
      </div>
    </main>
  );
}
