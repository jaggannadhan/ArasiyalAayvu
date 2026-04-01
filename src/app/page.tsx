"use client";

import { useState } from "react";
import { ConstituencySearch } from "@/components/constituency/ConstituencySearch";
import Link from "next/link";

const FEATURED = [
  { slug: "anna_nagar",           name: "Anna Nagar",           district: "Chennai" },
  { slug: "harur_sc",             name: "Harur (SC)",           district: "Dharmapuri" },
  { slug: "coimbatore_south",     name: "Coimbatore South",     district: "Coimbatore" },
  { slug: "madurai_central",      name: "Madurai Central",      district: "Madurai" },
  { slug: "chepauk_thiruvallikeni", name: "Chepauk-Thiruvallikeni", district: "Chennai" },
  { slug: "tirunelveli",          name: "Tirunelveli",          district: "Tirunelveli" },
];

export default function Home() {
  const [lang, setLang] = useState<"en" | "ta">("en");
  const isTA = lang === "ta";

  return (
    <main className="min-h-screen bg-gray-50">
      <header className="bg-white border-b border-gray-200 shadow-sm">
        <div className="max-w-2xl mx-auto px-4 py-4 flex items-center justify-between">
          <div>
            <h1 className="text-lg font-black text-gray-900">
              {isTA ? "அரசியல்ஆய்வு" : "ArasiyalAayvu"}
            </h1>
            <p className="text-xs text-gray-500">
              {isTA ? "தமிழ்நாடு தேர்தல் விழிப்புணர்வு" : "Tamil Nadu Election Awareness"}
            </p>
          </div>
          <button
            onClick={() => setLang(lang === "en" ? "ta" : "en")}
            className="text-xs font-bold px-3 py-1.5 rounded-full border border-gray-300 hover:bg-gray-100 transition-colors"
          >
            {lang === "en" ? "தமிழ்" : "English"}
          </button>
        </div>
      </header>

      <div className="max-w-2xl mx-auto px-4 py-8 space-y-8">
        {/* Hero */}
        <div className="text-center space-y-3">
          <p className="text-4xl">🗳️</p>
          <h2 className="text-xl font-black text-gray-900">
            {isTA ? "உங்கள் தொகுதியை அறிந்து கொள்ளுங்கள்" : "Know Your Constituency"}
          </h2>
          <p className="text-sm text-gray-600 max-w-sm mx-auto">
            {isTA
              ? "உங்கள் சட்டமன்ற உறுப்பினரின் சொத்து, வழக்குகள், கல்வி மற்றும் தேர்தல் வாக்குறுதிகளை பார்க்கவும்"
              : "See your MLA's assets, criminal record, education, and the promises made to your constituency"}
          </p>
        </div>

        {/* Search */}
        <div className="flex justify-center">
          <ConstituencySearch lang={lang} />
        </div>

        {/* Featured constituencies */}
        <div className="space-y-3">
          <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
            {isTA ? "சில தொகுதிகள்" : "Browse Constituencies"}
          </p>
          <div className="grid grid-cols-2 gap-2 sm:grid-cols-3">
            {FEATURED.map((c) => (
              <Link
                key={c.slug}
                href={`/constituency/${c.slug}`}
                className="bg-white rounded-xl border border-gray-200 px-4 py-3 hover:border-gray-400 hover:shadow-sm transition-all text-left"
              >
                <p className="text-sm font-semibold text-gray-900 truncate">{c.name}</p>
                <p className="text-xs text-gray-400">{c.district}</p>
              </Link>
            ))}
          </div>
        </div>

        {/* Nav links */}
        <div className="grid grid-cols-2 gap-3">
          <Link
            href="/manifesto-tracker"
            className="bg-white rounded-2xl border border-gray-200 p-4 hover:shadow-sm transition-all"
          >
            <p className="text-lg mb-1">📜</p>
            <p className="text-sm font-bold text-gray-900">
              {isTA ? "அறிக்கை ஒப்பீடு" : "Manifesto Tracker"}
            </p>
            <p className="text-xs text-gray-500">
              {isTA ? "வாக்குறுதி vs செயல்திறன்" : "Promise vs. Performance"}
            </p>
          </Link>
          <div className="bg-gray-100 rounded-2xl border border-gray-200 p-4 opacity-60">
            <p className="text-lg mb-1">📊</p>
            <p className="text-sm font-bold text-gray-700">
              {isTA ? "மாநில நலன் குறிகாட்டிகள்" : "State Vitals"}
            </p>
            <p className="text-xs text-gray-400">
              {isTA ? "விரைவில் வருகிறது" : "Coming soon"}
            </p>
          </div>
        </div>

        <p className="text-center text-xs text-gray-400 pb-4">
          {isTA
            ? "தரவு: MyNeta/ADR, NFHS-5, ASER 2024, கட்சி தேர்தல் அறிக்கைகள்"
            : "Data: MyNeta/ADR, NFHS-5, ASER 2024, Official party manifestos"}
        </p>
      </div>
    </main>
  );
}
