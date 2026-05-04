"use client";

import { useEffect, useState } from "react";
import Image from "next/image";
import { apiGet } from "@/lib/api-client";
import { normalizeName } from "@/lib/formatters";
import { type Candidate2026 } from "./Candidate2026ProfileModal";
import { ProfileModal, type PoliticianProfile } from "@/components/politicians/ProfileModal";

type Candidate = Candidate2026;

interface CandidatesData {
  constituency_slug: string;
  ac_number: number;
  candidates: Candidate[];
  total_candidates: number;
  election_year: number;
  _scraped_at: string;
}

const PARTY_FLAG_EXT: Record<string, string> = {
  dmk: "svg", aiadmk: "svg", bjp: "svg", inc: "svg", pmk: "svg",
  cpi: "svg", cpim: "png", vck: "png", dmdk: "png", mdmk: "svg",
  ntk: "gif", tvk: "jpeg",
  bsp: "svg", tamizhaga_vaazhv: "png", naam_indiar_part: "svg", veerath_thiyagi_: "svg",
};

const PARTY_ABBR: Record<string, string> = {
  ind: "IND", naam_indiar_part: "NIP", veerath_thiyagi_: "VTVTK",
  all_india_puratc: "AIPTMMK",
};

const GENDER_ICON: Record<string, string> = {
  Male: "♂", Female: "♀", "Third Gender": "⚧",
};

function CandidatePhoto({ url, gender, name, className }: {
  url: string | null | undefined; gender: string; name: string; className: string;
}) {
  const def = gender === "Female" ? "/default-mla-female.svg" : "/default-mla.svg";
  // ECI URLs are blocked by Next.js optimizer (WAF) — render as plain <img> so browser fetches directly
  if (url?.includes("suvidha.eci.gov.in")) {
    // eslint-disable-next-line @next/next/no-img-element
    return <img src={url} alt={name} className={className} />;
  }
  return <Image src={url ?? def} alt={name} width={64} height={80} sizes="64px" className={className} />;
}

interface Props {
  slug: string;
  lang?: "en" | "ta";
}

export function CandidatesPanel({ slug, lang = "en" }: Props) {
  const isTA = lang === "ta";
  const [data, setData] = useState<CandidatesData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const [profileModal, setProfileModal] = useState<PoliticianProfile | null>(null);

  function handleCandidateClick(c: Candidate) {
    // Build a minimal PoliticianProfile from candidate data so the standard
    // ProfileModal appears instantly — no waiting for the API.
    const stubProfile: PoliticianProfile = {
      doc_id: "",
      canonical_name: c.name,
      aliases: [],
      photo_url: c.photo_url,
      gender: c.gender,
      age: c.age ?? null,
      current_party: c.party,
      current_constituency: slug.replace(/_/g, " ").replace(/\b\w/g, (l) => l.toUpperCase()),
      current_constituency_slug: slug,
      timeline: [{
        year: 2026,
        constituency_slug: slug,
        constituency: slug.replace(/_/g, " ").replace(/\b\w/g, (l) => l.toUpperCase()),
        party: c.party,
        won: null,
      }],
    };
    setProfileModal(stubProfile);

    // In parallel, fetch the full profile and upgrade the modal seamlessly
    const searchName = encodeURIComponent(c.name);
    apiGet<{ total: number; items: PoliticianProfile[] }>(
      `/api/politicians?q=${searchName}&limit=5`
    )
      .then((res) => {
        const match = res.items.find((p) =>
          p.timeline.some(
            (t) => t.year === 2026 && t.constituency_slug === slug
          )
        );
        if (match) {
          setProfileModal(match);
        }
      })
      .catch(() => {
        // Keep the stub modal that's already showing
      });
  }

  useEffect(() => {
    setLoading(true);
    setError(false);
    setData(null);
    apiGet<CandidatesData>(`/api/candidates/2026/${encodeURIComponent(slug)}`)
      .then(setData)
      .catch(() => setError(true))
      .finally(() => setLoading(false));
  }, [slug]);

  if (loading) {
    return (
      <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-5 space-y-3">
        <div className="h-4 w-40 bg-gray-100 rounded animate-pulse" />
        {[1, 2, 3].map((i) => (
          <div key={i} className="h-12 bg-gray-50 rounded-xl animate-pulse" />
        ))}
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="bg-white rounded-2xl border border-gray-200 shadow-sm px-5 py-10 text-center">
        <p className="text-2xl mb-2">🗳</p>
        <p className="text-sm font-semibold text-gray-600">
          {isTA ? "2026 தேர்தல் வேட்பாளர் தரவு இல்லை" : "No 2026 candidate data yet"}
        </p>
        <p className="text-xs text-gray-400 mt-1">
          {isTA ? "தரவு விரைவில் வரும்" : "Data will appear after nomination filing"}
        </p>
      </div>
    );
  }

  // Sort: known parties first, then by party name
  const sorted = [...data.candidates].sort((a, b) => {
    const aKnown = PARTY_FLAG_EXT[a.party_id] ? 0 : 1;
    const bKnown = PARTY_FLAG_EXT[b.party_id] ? 0 : 1;
    if (aKnown !== bKnown) return aKnown - bKnown;
    return a.party.localeCompare(b.party);
  });

  // Count gender breakdown
  const femaleCount = data.candidates.filter(c => c.gender === "Female").length;
  const thirdGenderCount = data.candidates.filter(c => c.gender === "Third Gender").length;

  return (
    <div className="bg-white rounded-2xl border border-gray-200 shadow-sm overflow-hidden">
      {/* Header */}
      <div className="px-5 py-4 border-b border-gray-100">
        <div className="flex items-center justify-between">
          <div>
            <p className="text-[10px] font-bold text-gray-400 uppercase tracking-wider">
              {isTA ? "தேர்தல் 2026 · வேட்பாளர்கள்" : "Election 2026 · Candidates"}
            </p>
            <p className="text-lg font-black text-gray-900 mt-0.5">
              {data.total_candidates}{" "}
              <span className="text-sm font-semibold text-gray-500">
                {isTA ? "வேட்பாளர்கள்" : "contesting"}
              </span>
            </p>
          </div>
          <div className="text-right text-xs text-gray-400 space-y-0.5">
            {femaleCount > 0 && (
              <p>♀ {femaleCount} {isTA ? "பெண்" : "women"}</p>
            )}
            {thirdGenderCount > 0 && (
              <p>⚧ {thirdGenderCount}</p>
            )}
          </div>
        </div>
      </div>

      {/* Candidate cards */}
      <div className="divide-y divide-gray-100">
        {sorted.map((c, i) => {
          const flagExt = PARTY_FLAG_EXT[c.party_id];
          return (
            <button
              key={i}
              onClick={() => handleCandidateClick(c)}
              className="w-full flex items-center gap-4 px-5 py-4 text-left hover:bg-gray-50 transition-colors cursor-pointer"
            >
              {/* Photo */}
              <CandidatePhoto
                url={c.photo_url}
                gender={c.gender}
                name={c.name}
                className="shrink-0 w-16 h-20 flex-none rounded-xl object-cover border border-gray-200 shadow-sm bg-gray-50"
              />

              {/* Name + party */}
              <div className="flex-1 min-w-0">
                <p className="text-[10px] font-bold text-gray-400 uppercase tracking-wider mb-0.5">
                  {GENDER_ICON[c.gender] ?? ""} {c.gender}
                </p>
                <h3 className="text-base font-black text-gray-900 leading-tight">{normalizeName(c.name)}</h3>
                <p className="text-xs text-gray-500 mt-0.5">{c.party}</p>
              </div>

              {/* Party flag */}
              <div className="shrink-0 flex flex-col items-center gap-1">
                {flagExt ? (
                  // eslint-disable-next-line @next/next/no-img-element
                  <img
                    src={`/party-flags/${c.party_id}.${flagExt}`}
                    alt={c.party}
                    className="w-12 h-8 object-contain rounded-sm"
                  />
                ) : (
                  <div className="w-12 h-8 rounded-sm bg-gray-100 flex items-center justify-center px-1">
                    <span className="text-[9px] font-bold text-gray-500 uppercase text-center leading-tight">
                      {c.party.split(" ").map((w: string) => w[0]).join("").slice(0, 5)}
                    </span>
                  </div>
                )}
                <span className="text-[9px] font-bold text-gray-500 uppercase tracking-wide">
                  {PARTY_ABBR[c.party_id] ?? c.party_id.toUpperCase()}
                </span>
              </div>
            </button>
          );
        })}
      </div>

      {/* Footer */}
      <div className="px-4 py-2.5 bg-gray-50 border-t border-gray-100">
        <p className="text-[10px] text-gray-400 text-center">
          {isTA ? "ஆதாரம்: தமிழ்நாடு தேர்தல் ஆணையர் அலுவலகம் · " : "Source: CEO Tamil Nadu · "}
          <a
            href="https://electionapps.tn.gov.in/NOM2026/pu_nom/affidavit.aspx"
            target="_blank"
            rel="noopener noreferrer"
            className="underline underline-offset-2 hover:text-gray-600"
          >
            electionapps.tn.gov.in
          </a>
        </p>
      </div>

      {/* Politician profile modal */}
      {profileModal && (
        <ProfileModal profile={profileModal} onClose={() => setProfileModal(null)} />
      )}
    </div>
  );
}
