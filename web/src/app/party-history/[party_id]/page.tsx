"use client";

import { useParams } from "next/navigation";
import { useState, useEffect, useRef } from "react";
import Link from "next/link";
import Image from "next/image";
import { useLanguage } from "@/lib/LanguageContext";

// ── Types ────────────────────────────────────────────────────────────────────

interface Founder {
  name: string;
  role: string;
  brief: string;
}

interface KeyEvent {
  year: number;
  month: number | null;
  event: string;
  significance: string;
}

interface ElectionResult {
  year: number;
  type: string;
  seats_contested: number | null;
  seats_won: number;
  total_seats: number;
  vote_share_pct: number | null;
  alliance: string;
  outcome: string;
}

interface Chapter {
  era: string;
  title: string;
  narrative: string;
  key_events: KeyEvent[];
  achievements: string[];
  leadership: string[];
  election_results: ElectionResult[];
  controversies: string[];
  youtube_videos?: { title: string; url: string | null; search_query: string }[];
  articles?: { title: string; url: string; source: string; year: number }[];
}

interface PartyHistory {
  party_id: string;
  party_name: string;
  party_name_tamil: string;
  founded_year: number;
  founded_date: string | null;
  founded_place: string;
  parent_organization: string | null;
  founders: Founder[];
  original_ideology: string;
  current_ideology: string;
  original_motto: string;
  current_motto: string;
  symbol: string;
  symbol_history: string;
  headquarters: string;
  chapters: Chapter[];
  notable_leaders_through_history: { name: string; tenure: string; role: string; legacy: string }[];
  current_leadership: { name: string; role: string; since: string }[];
}

// ── Party meta (colors, flags) ───────────────────────────────────────────────

// ── Leader photo mapping (name as it appears in JSON → filename in /leaders/) ─

const LEADER_PHOTOS: Record<string, string> = {
  "C.N. Annadurai":              "/leaders/C_N_Annadurai.jpeg",
  "E.V.K. Sampath":              "/leaders/E_V_K_Sampath.jpeg",
  "N.V. Natarajan":              "/leaders/N_V_Natarajan.jpeg",
  "Dr. P.T. Rajan":              "/leaders/P_T_Rajan.jpg",
  "M.G. Ramachandran":           "/leaders/M_G_Ramachandran.jpg",
  "Atal Bihari Vajpayee":        "/leaders/Atal_Bihari_Vajpayee.jpeg",
  "Lal Krishna Advani":          "/leaders/Lal_Krishna_Advani.jpg",
  "Shyama Prasad Mukherjee":     "/leaders/Shyama_Prasad_Mukherjee.jpeg",
  "Joseph Vijay Chandrasekhar":  "https://results.eci.gov.in/uploads2/candprofile/E32/2026/AC/s22/CJOS-2026-20260330045107.jpg",
  "Allan Octavian Hume":         "/leaders/Allan_Octavian_Hume.jpeg",
  "Womesh Chunder Bonnerjee":    "/leaders/Womesh_Chunder_Bonnerjee.jpg",
  "Dadabhai Naoroji":            "/leaders/Dadabhai_Naoroji.jpeg",
};

function LeaderAvatar({ name }: { name: string }) {
  const src = LEADER_PHOTOS[name];
  if (!src) {
    // Fallback: initials circle
    const initials = name.split(" ").filter(w => w.length > 1 && w[0] !== "(").map(w => w[0]).slice(0, 2).join("");
    return (
      <div className="w-12 h-12 rounded-full bg-gray-200 flex items-center justify-center flex-shrink-0">
        <span className="text-xs font-bold text-gray-500">{initials}</span>
      </div>
    );
  }
  return (
    <div className="w-12 h-12 rounded-full overflow-hidden bg-gray-100 flex-shrink-0 border border-gray-200">
      {/* eslint-disable-next-line @next/next/no-img-element */}
      <img src={src} alt={name} className="w-full h-full object-cover" />
    </div>
  );
}

const PARTY_META: Record<string, { color: string; bg: string; text: string; accent: string; flag: string }> = {
  dmk:    { color: "border-red-600",    bg: "bg-red-600",    text: "text-red-700",    accent: "bg-red-50",    flag: "/party-flags/dmk.svg" },
  aiadmk: { color: "border-green-700",  bg: "bg-green-700",  text: "text-green-700",  accent: "bg-green-50",  flag: "/party-flags/aiadmk.svg" },
  tvk:    { color: "border-sky-600",    bg: "bg-sky-600",    text: "text-sky-700",    accent: "bg-sky-50",    flag: "/party-flags/tvk.jpeg" },
  bjp:    { color: "border-orange-500", bg: "bg-orange-500", text: "text-orange-700", accent: "bg-orange-50", flag: "/party-flags/bjp.svg" },
  inc:    { color: "border-blue-600",   bg: "bg-blue-600",   text: "text-blue-700",   accent: "bg-blue-50",   flag: "/party-flags/inc.svg" },
};

// ── Components ───────────────────────────────────────────────────────────────

function FoundingHero({ data, meta }: { data: PartyHistory; meta: typeof PARTY_META.dmk }) {
  return (
    <section className={`relative ${meta.accent} rounded-3xl p-6 mb-8 border-2 ${meta.color}`}>
      <div className="flex items-start gap-4 mb-4">
        <div className="w-16 h-16 rounded-xl overflow-hidden bg-white border border-gray-200 flex-shrink-0">
          <Image src={meta.flag} alt={data.party_id} width={64} height={64} className="w-full h-full object-contain p-1" />
        </div>
        <div>
          <h1 className={`text-2xl font-black ${meta.text}`}>{data.party_name_tamil}</h1>
          <p className="text-sm text-gray-600">{data.party_name}</p>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3 mb-5">
        <div className="bg-white/70 rounded-xl p-3">
          <p className="text-[10px] uppercase tracking-wider text-gray-500 font-semibold">Founded</p>
          <p className="text-lg font-black text-gray-900">{data.founded_year}</p>
          <p className="text-xs text-gray-500">{data.founded_place}</p>
        </div>
        <div className="bg-white/70 rounded-xl p-3">
          <p className="text-[10px] uppercase tracking-wider text-gray-500 font-semibold">Symbol</p>
          <p className="text-sm font-bold text-gray-900">{data.symbol}</p>
        </div>
      </div>

      {/* Motto */}
      <blockquote className={`border-l-4 ${meta.color} pl-3 mb-5`}>
        <p className="text-sm font-semibold text-gray-800 italic">&ldquo;{data.original_motto}&rdquo;</p>
      </blockquote>

      {/* Founders */}
      <div className="mb-4">
        <p className="text-xs uppercase tracking-wider text-gray-500 font-semibold mb-2">Founders</p>
        <div className="space-y-2">
          {data.founders.slice(0, 4).map((f) => (
            <div key={f.name} className="bg-white/70 rounded-lg p-3 flex items-start gap-3">
              <LeaderAvatar name={f.name} />
              <div className="flex-1 min-w-0">
                <p className="text-sm font-bold text-gray-900">{f.name}</p>
                <p className="text-[11px] text-gray-500">{f.role}</p>
                <p className="text-xs text-gray-600 mt-0.5">{f.brief}</p>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Ideology */}
      <div>
        <p className="text-xs uppercase tracking-wider text-gray-500 font-semibold mb-1">Ideology</p>
        <p className="text-sm text-gray-700 leading-relaxed">{data.original_ideology}</p>
      </div>
    </section>
  );
}

function TimelineSpine({ chapters, activeIdx, onSelect }: { chapters: Chapter[]; activeIdx: number; onSelect: (i: number) => void }) {
  return (
    <div className="sticky top-16 z-10 bg-gray-50/95 backdrop-blur-sm py-3 -mx-4 px-4 mb-4 overflow-x-auto">
      <div className="flex gap-1 min-w-max">
        {chapters.map((ch, i) => {
          const startYear = ch.era.split("-")[0];
          return (
            <button
              key={ch.era}
              onClick={() => onSelect(i)}
              className={`px-2.5 py-1.5 rounded-full text-[11px] font-semibold transition-all whitespace-nowrap ${
                i === activeIdx
                  ? "bg-gray-900 text-white"
                  : "bg-white text-gray-500 hover:bg-gray-100 border border-gray-200"
              }`}
            >
              {startYear}
            </button>
          );
        })}
      </div>
    </div>
  );
}

function ChapterCard({ chapter, index, meta }: { chapter: Chapter; index: number; meta: typeof PARTY_META.dmk }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <article className="relative pl-6 pb-8 border-l-2 border-gray-200 last:border-l-0">
      {/* Timeline dot */}
      <div className={`absolute -left-[9px] top-0 w-4 h-4 rounded-full ${meta.bg} border-2 border-white shadow-sm`} />

      {/* Era label */}
      <div className="flex items-center gap-2 mb-2">
        <span className={`text-xs font-bold ${meta.text}`}>{chapter.era}</span>
      </div>

      {/* Chapter title */}
      <h3 className="text-base font-black text-gray-900 mb-3 leading-snug">{chapter.title}</h3>

      {/* Narrative */}
      <p className="text-sm text-gray-700 leading-relaxed mb-4">{chapter.narrative}</p>

      {/* Election results (always visible) */}
      {chapter.election_results.length > 0 && (
        <div className="mb-4">
          <div className="flex flex-wrap gap-2">
            {chapter.election_results.map((er) => (
              <div key={`${er.year}-${er.type}`} className="bg-white border border-gray-200 rounded-lg px-3 py-2 text-center min-w-[80px]">
                <p className="text-[10px] text-gray-400 uppercase">{er.type === "assembly" ? "MLA" : "MP"} {er.year}</p>
                <p className="text-lg font-black text-gray-900">{er.seats_won}</p>
                <p className="text-[10px] text-gray-500">/ {er.total_seats}</p>
                {er.vote_share_pct && (
                  <p className="text-[10px] text-gray-400">{er.vote_share_pct}%</p>
                )}
                <p className={`text-[9px] mt-0.5 font-semibold ${
                  er.outcome === "won_government" ? "text-green-600" :
                  er.outcome === "opposition" ? "text-red-500" :
                  "text-gray-500"
                }`}>
                  {er.outcome === "won_government" ? "WON" :
                   er.outcome === "opposition" ? "OPP" :
                   er.outcome === "coalition_partner" ? "ALLY" :
                   er.outcome.toUpperCase()}
                </p>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Expandable details */}
      {(chapter.key_events.length > 0 || chapter.achievements.length > 0 || chapter.controversies.length > 0) && (
        <button
          onClick={() => setExpanded(!expanded)}
          className="text-xs font-semibold text-gray-500 hover:text-gray-700 flex items-center gap-1 mb-3"
        >
          <svg className={`w-3.5 h-3.5 transition-transform ${expanded ? "rotate-90" : ""}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
          </svg>
          {expanded ? "Hide details" : `${chapter.key_events.length} events, ${chapter.achievements.length} achievements`}
        </button>
      )}

      {expanded && (
        <div className="space-y-4 mt-3 animate-in fade-in duration-200">
          {/* Key events */}
          {chapter.key_events.length > 0 && (
            <div>
              <p className="text-[10px] uppercase tracking-wider text-gray-400 font-semibold mb-2">Key Events</p>
              <div className="space-y-2">
                {chapter.key_events.map((e, i) => (
                  <div key={i} className="flex gap-2">
                    <span className="text-xs font-bold text-gray-400 w-10 flex-shrink-0">{e.year}</span>
                    <div>
                      <p className="text-xs font-semibold text-gray-800">{e.event}</p>
                      <p className="text-[11px] text-gray-500">{e.significance}</p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Achievements */}
          {chapter.achievements.length > 0 && (
            <div>
              <p className="text-[10px] uppercase tracking-wider text-gray-400 font-semibold mb-2">Achievements</p>
              <ul className="space-y-1">
                {chapter.achievements.map((a, i) => (
                  <li key={i} className="text-xs text-gray-700 flex gap-1.5">
                    <span className="text-green-500 flex-shrink-0">+</span>
                    <span>{a}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}

          {/* Controversies */}
          {chapter.controversies.length > 0 && (
            <div>
              <p className="text-[10px] uppercase tracking-wider text-gray-400 font-semibold mb-2">Controversies</p>
              <ul className="space-y-1">
                {chapter.controversies.map((c, i) => (
                  <li key={i} className="text-xs text-gray-600 flex gap-1.5">
                    <span className="text-amber-500 flex-shrink-0">!</span>
                    <span>{c}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}
    </article>
  );
}

function LeadershipSection({ data }: { data: PartyHistory }) {
  return (
    <section className="mt-8 mb-6">
      <h2 className="text-sm font-black text-gray-900 uppercase tracking-wider mb-3">Current Leadership</h2>
      <div className="grid grid-cols-1 gap-2">
        {data.current_leadership.map((l) => (
          <div key={l.name} className="bg-white rounded-xl border border-gray-200 p-3 flex items-center gap-3">
            <LeaderAvatar name={l.name} />
            <div className="flex-1 min-w-0">
              <p className="text-sm font-bold text-gray-900">{l.name}</p>
              <p className="text-xs text-gray-500">{l.role}</p>
            </div>
            <span className="text-[10px] text-gray-400 font-semibold flex-shrink-0">since {l.since}</span>
          </div>
        ))}
      </div>
    </section>
  );
}

// ── Main Page ────────────────────────────────────────────────────────────────

export default function PartyHistoryDetail() {
  const params = useParams();
  const partyId = params.party_id as string;
  const { lang } = useLanguage();
  const isTA = lang === "ta";

  const [data, setData] = useState<PartyHistory | null>(null);
  const [activeChapter, setActiveChapter] = useState(0);
  const chapterRefs = useRef<(HTMLElement | null)[]>([]);

  const meta = PARTY_META[partyId] || PARTY_META.dmk;

  useEffect(() => {
    fetch(`/data/party_history_${partyId}.json`)
      .then((res) => {
        if (!res.ok) throw new Error("Not found");
        return res.json();
      })
      .then((d) => setData(d))
      .catch(() => setData(null));
  }, [partyId]);

  const scrollToChapter = (idx: number) => {
    setActiveChapter(idx);
    chapterRefs.current[idx]?.scrollIntoView({ behavior: "smooth", block: "start" });
  };

  // Track scroll position to update active chapter
  useEffect(() => {
    if (!data) return;
    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            const idx = Number(entry.target.getAttribute("data-chapter-idx"));
            if (!isNaN(idx)) setActiveChapter(idx);
          }
        }
      },
      { rootMargin: "-20% 0px -60% 0px" }
    );

    chapterRefs.current.forEach((ref) => {
      if (ref) observer.observe(ref);
    });
    return () => observer.disconnect();
  }, [data]);

  if (!data) {
    return (
      <main className="min-h-full bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="w-8 h-8 border-2 border-gray-300 border-t-gray-900 rounded-full animate-spin mx-auto mb-3" />
          <p className="text-sm text-gray-500">Loading party history...</p>
        </div>
      </main>
    );
  }

  return (
    <main className="min-h-full bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 shadow-sm sticky top-0 z-20">
        <div className="max-w-2xl mx-auto px-4 py-3 flex items-center gap-3">
          <Link href="/party-history" className="text-gray-400 hover:text-gray-600">
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
            </svg>
          </Link>
          <div className="flex items-center gap-2">
            <div className="w-7 h-7 rounded-lg overflow-hidden bg-gray-100 border border-gray-200">
              <Image src={meta.flag} alt={partyId} width={28} height={28} className="w-full h-full object-contain" />
            </div>
            <h1 className={`text-base font-black ${meta.text}`}>
              {isTA ? data.party_name_tamil : data.party_name.split("(")[0].trim()}
            </h1>
          </div>
        </div>
      </header>

      <div className="max-w-2xl mx-auto px-4 py-6">
        {/* Founding Hero Section */}
        <FoundingHero data={data} meta={meta} />

        {/* Timeline Navigation */}
        <TimelineSpine chapters={data.chapters} activeIdx={activeChapter} onSelect={scrollToChapter} />

        {/* Chapters */}
        <div className="mt-6">
          {data.chapters.map((chapter, idx) => (
            <div
              key={chapter.era}
              ref={(el) => { chapterRefs.current[idx] = el; }}
              data-chapter-idx={idx}
            >
              <ChapterCard chapter={chapter} index={idx} meta={meta} />
            </div>
          ))}
        </div>

        {/* Current Leadership */}
        <LeadershipSection data={data} />

        {/* Notable Leaders */}
        {data.notable_leaders_through_history.length > 0 && (
          <section className="mb-8">
            <h2 className="text-sm font-black text-gray-900 uppercase tracking-wider mb-3">Leaders Through History</h2>
            <div className="space-y-2">
              {data.notable_leaders_through_history.map((l) => (
                <div key={l.name} className="bg-white rounded-xl border border-gray-200 p-3 flex items-start gap-3">
                  <LeaderAvatar name={l.name} />
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-bold text-gray-900">{l.name}</p>
                    <p className="text-xs text-gray-500">{l.role} ({l.tenure})</p>
                    <p className="text-xs text-gray-600 mt-1">{l.legacy}</p>
                  </div>
                </div>
              ))}
            </div>
          </section>
        )}

        {/* Footer */}
        <div className="text-center py-6 border-t border-gray-200">
          <Link
            href="/party-history"
            className="text-xs font-semibold text-gray-500 hover:text-gray-700"
          >
            {isTA ? "அனைத்து கட்சிகளும்" : "All Parties"} &rarr;
          </Link>
        </div>
      </div>
    </main>
  );
}
