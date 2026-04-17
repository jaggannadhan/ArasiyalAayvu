"use client";

import { useEffect, useRef } from "react";
import Image from "next/image";
import { normalizeName } from "@/lib/formatters";

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
  if (url?.includes("suvidha.eci.gov.in")) {
    // eslint-disable-next-line @next/next/no-img-element
    return <img src={url} alt={name} className={className} />;
  }
  return <Image src={url ?? def} alt={name} width={80} height={100} sizes="80px" className={className} />;
}

export interface Candidate2026 {
  name: string;
  party: string;
  party_id: string;
  gender: string;
  age?: number | null;
  nomination_date?: string | null;
  photo_url?: string | null;
  affidavit_url?: string | null;
  show_profile_url?: string | null;
}

interface Props {
  candidate: Candidate2026 | null;
  onClose: () => void;
  lang?: "en" | "ta";
}

export function Candidate2026ProfileModal({ candidate, onClose, lang = "en" }: Props) {
  const isTA = lang === "ta";
  const backdropRef = useRef<HTMLDivElement>(null);
  const isOpen = candidate !== null;

  // Lock body scroll while open
  useEffect(() => {
    document.body.style.overflow = isOpen ? "hidden" : "";
    return () => { document.body.style.overflow = ""; };
  }, [isOpen]);

  // Close on Escape
  useEffect(() => {
    if (!isOpen) return;
    const handler = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [isOpen, onClose]);

  if (!candidate) return null;

  const flagExt = PARTY_FLAG_EXT[candidate.party_id];

  return (
    <div
      ref={backdropRef}
      className="fixed inset-0 z-50 flex items-end sm:items-center justify-center bg-black/50 backdrop-blur-sm p-0 sm:p-4"
      onClick={(e) => { if (e.target === backdropRef.current) onClose(); }}
    >
      <div className="relative w-full sm:max-w-md max-h-[92dvh] sm:max-h-[85dvh] bg-white sm:rounded-2xl rounded-t-2xl shadow-2xl flex flex-col overflow-hidden">

        {/* Header */}
        <div className="flex items-start justify-between gap-3 px-5 pt-5 pb-4 border-b border-gray-100">
          <div className="min-w-0">
            <p className="text-[10px] font-bold text-gray-400 uppercase tracking-wider">
              {isTA ? "2026 தேர்தல் · வேட்பாளர்" : "Election 2026 · Candidate"}
            </p>
            <h2 className="text-base font-black text-gray-900 leading-tight mt-0.5">
              {normalizeName(candidate.name)}
            </h2>
          </div>
          <button
            onClick={onClose}
            aria-label={isTA ? "மூடு" : "Close"}
            className="text-gray-400 hover:text-gray-700 text-xl leading-none p-1 shrink-0 cursor-pointer"
          >
            ×
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto px-5 py-5 space-y-5">

          {/* Photo + identity */}
          <div className="flex items-center gap-4">
            <CandidatePhoto
              url={candidate.photo_url}
              gender={candidate.gender}
              name={candidate.name}
              className="shrink-0 w-20 h-[100px] rounded-xl object-cover border border-gray-200 shadow-sm bg-gray-50 flex-none"
            />
            <div className="flex-1 min-w-0 space-y-1">
              <p className="text-xs text-gray-500">
                {GENDER_ICON[candidate.gender] ?? ""}{" "}
                {candidate.gender}
              </p>
              <div className="flex items-center gap-2 flex-wrap">
                {flagExt ? (
                  // eslint-disable-next-line @next/next/no-img-element
                  <img
                    src={`/party-flags/${candidate.party_id}.${flagExt}`}
                    alt={candidate.party}
                    className="w-12 h-8 object-contain rounded-sm"
                  />
                ) : (
                  <div className="w-12 h-8 rounded-sm bg-gray-100 flex items-center justify-center px-1">
                    <span className="text-[9px] font-bold text-gray-500 uppercase text-center leading-tight">
                      {candidate.party.split(" ").map((w) => w[0]).join("").slice(0, 5)}
                    </span>
                  </div>
                )}
                <div>
                  <p className="text-[10px] font-bold text-gray-500 uppercase tracking-wide">
                    {PARTY_ABBR[candidate.party_id] ?? candidate.party_id.toUpperCase()}
                  </p>
                  <p className="text-xs text-gray-700 font-medium leading-tight">{candidate.party}</p>
                </div>
              </div>
            </div>
          </div>

          {/* Nomination details */}
          <div className="rounded-xl bg-gray-50 border border-gray-100 px-4 py-3 space-y-2">
            <p className="text-[10px] font-bold text-gray-400 uppercase tracking-wider">
              {isTA ? "வேட்பாளர் விவரங்கள்" : "Candidate Details"}
            </p>
            <div className="grid grid-cols-2 gap-3">
              <div>
                <p className="text-xs text-gray-500">{isTA ? "வயது" : "Age"}</p>
                <p className="text-sm font-bold text-gray-900">{candidate.age ?? "—"}</p>
              </div>
              <div>
                <p className="text-xs text-gray-500">{isTA ? "தாக்கல் தேதி" : "Filed On"}</p>
                <p className="text-sm font-bold text-gray-900">{candidate.nomination_date || "—"}</p>
              </div>
            </div>
          </div>

          {/* Affidavit — link to show_profile_url or affidavit_url */}
          {(candidate.affidavit_url || candidate.show_profile_url) ? (
            <a
              href={(candidate.affidavit_url || candidate.show_profile_url)!}
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-2 rounded-xl bg-gray-50 border border-gray-200 px-4 py-3 hover:bg-gray-100 transition-colors"
            >
              <span className="text-lg">📄</span>
              <div className="min-w-0">
                <p className="text-xs font-semibold text-gray-800">
                  {isTA ? "ECI உறுதிமொழி ஆவணம் (Form 26)" : "ECI Affidavit (Form 26)"}
                </p>
                <p className="text-[10px] text-gray-500 mt-0.5">
                  {isTA ? "ஆதாரம்: affidavit.eci.gov.in — திறக்க தட்டவும்" : "Source: affidavit.eci.gov.in — tap to open"}
                </p>
              </div>
              <span className="ml-auto text-gray-400 text-sm shrink-0">↗</span>
            </a>
          ) : (
            <div className="rounded-xl bg-amber-50 border border-amber-200 px-4 py-3">
              <p className="text-xs font-semibold text-amber-800">
                📄 {isTA ? "வேட்பாளர் உறுதிமொழி ஆவணம்" : "Candidate Affidavit"}
              </p>
              <p className="text-xs text-amber-700 mt-1 leading-relaxed">
                {isTA
                  ? <>ECI Form 26 ஆவணம் (சொத்து, வழக்குகள்) <a href="https://affidavit.eci.gov.in/" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2">affidavit.eci.gov.in</a> இல் இருந்து இழுக்கப்படுகிறது.</>
                  : <>ECI Form 26 affidavit (assets, criminal cases) is being pulled from <a href="https://affidavit.eci.gov.in/" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2">affidavit.eci.gov.in</a>.</>}
              </p>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-5 py-3 border-t border-gray-100 bg-gray-50/60">
          <p className="text-[11px] text-gray-400 text-center">
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
      </div>
    </div>
  );
}
