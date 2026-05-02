"use client";

import { useEffect, useState } from "react";
import { apiGet } from "@/lib/api-client";

interface NewsItem {
  doc_id: string;
  title: string;
  summary: string;
  source_url: string;
  category: string;
  topics: string[];
  published_at: string;
  sentiment: number;
}

interface Props {
  entityId: string;
  lang?: "en" | "ta";
  limit?: number;
}

const CAT_ICON: Record<string, string> = {
  POLITICS: "🏛️", BUSINESS: "💼", HEALTH: "🏥", SCIENCE_TECH: "🔬",
  WORLD: "🌍", ENTERTAINMENT: "🎬", SPORTS: "⚽",
};

function timeAgo(dateStr: string): string {
  const diff = Date.now() - new Date(dateStr).getTime();
  const min = Math.floor(diff / 60000);
  if (min < 60) return `${min}m`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h`;
  return `${Math.floor(hr / 24)}d`;
}

export function InTheNews({ entityId, lang = "en", limit = 5 }: Props) {
  const isTA = lang === "ta";
  const [items, setItems] = useState<NewsItem[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    apiGet<{ articles: NewsItem[] }>(
      `/api/news/by-entity/${encodeURIComponent(entityId)}?limit=${limit}`
    )
      .then((res) => setItems(res.articles))
      .catch(() => setItems([]))
      .finally(() => setLoading(false));
  }, [entityId, limit]);

  if (loading) {
    return (
      <div className="space-y-2">
        {[1, 2].map((i) => (
          <div key={i} className="h-12 bg-gray-50 rounded-lg animate-pulse" />
        ))}
      </div>
    );
  }

  if (items.length === 0) return null;

  return (
    <div className="bg-white rounded-2xl border border-gray-200 shadow-sm overflow-hidden">
      <div className="px-4 py-3 border-b border-gray-100">
        <p className="text-[10px] font-bold text-gray-400 uppercase tracking-wider">
          {isTA ? "📰 செய்திகளில்" : "📰 In the News"}
        </p>
      </div>
      <div className="divide-y divide-gray-100">
        {items.map((item) => (
          <a
            key={item.doc_id}
            href={item.source_url}
            target="_blank"
            rel="noopener noreferrer"
            className="block px-4 py-3 hover:bg-gray-50 transition-colors"
          >
            <div className="flex items-start gap-2">
              <span className="text-xs mt-0.5">{CAT_ICON[item.category] ?? "📰"}</span>
              <div className="flex-1 min-w-0">
                <p className="text-xs font-semibold text-gray-900 leading-snug line-clamp-2">
                  {item.title}
                </p>
                {item.summary && (
                  <p className="text-[10px] text-gray-500 mt-0.5 line-clamp-1">{item.summary}</p>
                )}
              </div>
              <span className="text-[9px] text-gray-400 shrink-0">{timeAgo(item.published_at)}</span>
            </div>
          </a>
        ))}
      </div>
    </div>
  );
}
