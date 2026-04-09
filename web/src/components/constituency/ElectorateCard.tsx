"use client";

import electorateData2006 from "@/lib/constituency-electorate-2006.json";
import electorateData2011 from "@/lib/constituency-electorate-2011.json";
import electorateData2016 from "@/lib/constituency-electorate.json";
import electorateData2021 from "@/lib/constituency-electorate-2021.json";
import electorateData2026 from "@/lib/constituency-electorate-2026.json";
import postToPre2008 from "@/lib/constituency-post-to-pre2008.json";

type EntryWithGender = {
  male: number;
  female: number;
  third_gender: number;
  total: number;
  source: string;
  source_url?: string;
  total_votes?: number;
  poll_pct?: number;
};

type EntryTurnoverOnly = {
  total: number;
  total_votes: number;
  poll_pct: number;
  margin?: number;
  source: string;
  source_url?: string;
  winner_votes?: number;
  winner_pct?: number;
};

type ElectorateEntry = EntryWithGender | EntryTurnoverOnly;

const ELECTORATE: Record<number, Record<string, ElectorateEntry>> = {
  2006: electorateData2006 as Record<string, EntryTurnoverOnly>,
  2011: electorateData2011 as Record<string, EntryTurnoverOnly>,
  2016: electorateData2016 as Record<string, EntryWithGender>,
  2021: electorateData2021 as Record<string, EntryTurnoverOnly>,
  2026: electorateData2026 as Record<string, EntryWithGender>,
};

const POST_TO_PRE2008 = postToPre2008 as Record<string, string>;

interface Props {
  slug: string;
  year: 2006 | 2011 | 2016 | 2021 | 2026;
  lang?: "en" | "ta";
}

function fmt(n: number): string {
  return n.toLocaleString("en-IN");
}

function pct(n: number, total: number): string {
  return ((n / total) * 100).toFixed(1);
}

function hasGender(e: ElectorateEntry): e is EntryWithGender {
  return "male" in e;
}

export function ElectorateCard({ slug, year, lang = "en" }: Props) {
  // 2006 uses pre-delimitation constituency boundaries; resolve via mapping
  const lookupSlug = year === 2006 ? (POST_TO_PRE2008[slug] ?? slug) : slug;
  const data = ELECTORATE[year]?.[lookupSlug];
  if (!data) return null;

  const isTA = lang === "ta";

  return (
    <div className="bg-white rounded-2xl border border-gray-200 px-5 py-4 space-y-3">
      <div className="flex items-center justify-between">
        <h3 className="text-xs font-bold text-gray-500 uppercase tracking-wide">
          {isTA ? "வாக்காளர் பட்டியல்" : "Registered Voters"}
        </h3>
        <span className="text-xs text-gray-400">{year}</span>
      </div>

      {hasGender(data) ? (
        <>
          {/* Total + gender breakdown (2016, 2026) */}
          <div className="flex items-end gap-4">
            <div>
              <p className="text-2xl font-black text-gray-900">{fmt(data.total)}</p>
              <p className="text-xs text-gray-400 mt-0.5">
                {isTA ? "மொத்த வாக்காளர்கள்" : "Total voters"}
              </p>
            </div>
            <div className="flex gap-4 pb-0.5 ml-auto text-right">
              <div>
                <p className="text-sm font-bold text-blue-600">{fmt(data.male)}</p>
                <p className="text-xs text-gray-400">{isTA ? "ஆண்" : "Male"} · {pct(data.male, data.total)}%</p>
              </div>
              <div>
                <p className="text-sm font-bold text-rose-500">{fmt(data.female)}</p>
                <p className="text-xs text-gray-400">{isTA ? "பெண்" : "Female"} · {pct(data.female, data.total)}%</p>
              </div>
              {data.third_gender > 0 && (
                <div>
                  <p className="text-sm font-bold text-purple-500">{fmt(data.third_gender)}</p>
                  <p className="text-xs text-gray-400">{isTA ? "மூன்றாம் பாலினம்" : "Other"}</p>
                </div>
              )}
            </div>
          </div>
          <div className="h-2 rounded-full overflow-hidden bg-gray-100 flex">
            <div className="bg-blue-400 h-full transition-all" style={{ width: `${(data.male / data.total) * 100}%` }} />
            <div className="bg-rose-400 h-full transition-all" style={{ width: `${(data.female / data.total) * 100}%` }} />
            <div className="bg-purple-300 h-full flex-1" />
          </div>
          {data.total_votes != null && data.poll_pct != null && (
            <>
              <div className="flex items-end gap-4 pt-1">
                <div className="flex gap-4 ml-auto text-right">
                  <div>
                    <p className="text-sm font-bold text-emerald-600">{fmt(data.total_votes)}</p>
                    <p className="text-xs text-gray-400">{isTA ? "வாக்களித்தவர்கள்" : "Votes cast"}</p>
                  </div>
                  <div>
                    <p className="text-sm font-bold text-gray-700">{data.poll_pct}%</p>
                    <p className="text-xs text-gray-400">{isTA ? "வாக்குச்சதவீதம்" : "Turnout"}</p>
                  </div>
                </div>
              </div>
              <div className="h-2 rounded-full overflow-hidden bg-gray-100">
                <div className="bg-emerald-400 h-full transition-all" style={{ width: `${data.poll_pct}%` }} />
              </div>
            </>
          )}
        </>
      ) : (
        <>
          {/* Total + turnout (2021 — no gender split available) */}
          <div className="flex items-end gap-4">
            <div>
              <p className="text-2xl font-black text-gray-900">{fmt(data.total)}</p>
              <p className="text-xs text-gray-400 mt-0.5">
                {isTA ? "மொத்த வாக்காளர்கள்" : "Total voters"}
              </p>
            </div>
            <div className="flex gap-4 pb-0.5 ml-auto text-right">
              <div>
                <p className="text-sm font-bold text-emerald-600">{fmt(data.total_votes)}</p>
                <p className="text-xs text-gray-400">{isTA ? "வாக்களித்தவர்கள்" : "Votes cast"}</p>
              </div>
              <div>
                <p className="text-sm font-bold text-gray-700">{data.poll_pct}%</p>
                <p className="text-xs text-gray-400">{isTA ? "வாக்குச்சதவீதம்" : "Turnout"}</p>
              </div>
            </div>
          </div>
          <div className="h-2 rounded-full overflow-hidden bg-gray-100">
            <div
              className="bg-emerald-400 h-full transition-all"
              style={{ width: `${data.poll_pct}%` }}
            />
          </div>
        </>
      )}

      <p className="text-[10px] text-gray-300 leading-relaxed">
        {isTA ? "ஆதாரம்: " : "Source: "}
        {"source_url" in data && data.source_url ? (
          <a
            href={data.source_url}
            target="_blank"
            rel="noopener noreferrer"
            className="underline underline-offset-2 hover:text-gray-500"
          >
            {data.source}
          </a>
        ) : data.source}
      </p>
    </div>
  );
}
