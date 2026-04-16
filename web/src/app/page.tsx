"use client";

import { useState, useEffect } from "react";
import { ConstituencySearch } from "@/components/constituency/ConstituencySearch";
import { useLanguage } from "@/lib/LanguageContext";
import Link from "next/link";
import { apiGet } from "@/lib/api-client";
import type { FrequentlyBrowsedItem } from "@/lib/types";

const FEATURED_FALLBACK: FrequentlyBrowsedItem[] = [
  { slug: "anna_nagar",             name: "Anna Nagar",             district: "Chennai",     view_count: 0 },
  { slug: "harur_sc",               name: "Harur (SC)",             district: "Dharmapuri",  view_count: 0 },
  { slug: "coimbatore_south",       name: "Coimbatore South",       district: "Coimbatore",  view_count: 0 },
  { slug: "madurai_central",        name: "Madurai Central",        district: "Madurai",     view_count: 0 },
  { slug: "chepauk_thiruvallikeni", name: "Chepauk-Thiruvallikeni", district: "Chennai",     view_count: 0 },
  { slug: "tirunelveli",            name: "Tirunelveli",            district: "Tirunelveli", view_count: 0 },
];

export default function Home() {
  const { lang, setLang } = useLanguage();
  const [featured, setFeatured] = useState<FrequentlyBrowsedItem[]>(FEATURED_FALLBACK);
  const isTA = lang === "ta";

  useEffect(() => {
    apiGet<FrequentlyBrowsedItem[]>("/api/frequently-browsed?limit=6")
      .then((items) => {
        if (!Array.isArray(items) || items.length === 0) {
          return;
        }

        const merged = [...items];
        for (const fallback of FEATURED_FALLBACK) {
          if (merged.length >= 6) break;
          if (!merged.some((item) => item.slug === fallback.slug)) {
            merged.push(fallback);
          }
        }

        setFeatured(merged.slice(0, 6));
      })
      .catch(() => {
        // use fallback silently
      });
  }, []);

  return (
    <main className="min-h-full bg-gray-50">
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
            className="text-xs font-bold px-3 py-1.5 rounded-full border border-gray-300 hover:bg-gray-100 transition-colors text-gray-900"
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
            {isTA ? "அடிக்கடி பார்க்கப்படுகின்றன" : "Frequently Browsed"}
          </p>
          <div className="grid grid-cols-2 gap-2 sm:grid-cols-3">
            {featured.map((c) => (
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
          <Link
            href="/state-report/tamil_nadu"
            className="bg-white rounded-2xl border border-gray-200 p-4 hover:shadow-sm transition-all"
          >
            <p className="text-lg mb-1">📊</p>
            <p className="text-sm font-bold text-gray-900">
              {isTA ? "மாநில நலன் குறிகாட்டிகள்" : "State Vitals"}
            </p>
            <p className="text-xs text-gray-500">
              {isTA ? "தொழிலாளர் · சுகாதாரம் · கல்வி · செலவு" : "Labour · Health · Education · Spending"}
            </p>
          </Link>
          <Link
            href="/sdg-tracker"
            className="bg-white rounded-2xl border border-gray-200 p-4 hover:shadow-sm transition-all col-span-2"
          >
            <div className="flex items-start justify-between gap-2">
              <div>
                <p className="text-lg mb-1">🌍</p>
                <p className="text-sm font-bold text-gray-900">
                  {isTA ? "நிலையான வளர்ச்சி இலக்குகள் (SDG)" : "Sustainable Development Goals"}
                </p>
                <p className="text-xs text-gray-500">
                  {isTA
                    ? "தமிழ்நாடு SDG மதிப்பெண் · இந்தியாவில் 3வது இடம்"
                    : "TN's SDG Score vs. National Average · 17 Goals · Key Metrics"}
                </p>
              </div>
              <div className="flex-shrink-0 text-right">
                <p className="text-2xl font-black text-blue-800">78</p>
                <p className="text-[10px] text-gray-400 font-semibold">#3 in India</p>
              </div>
            </div>
          </Link>
          <Link
            href="/knowledge-graph"
            className="bg-gray-950 rounded-2xl border border-gray-700 p-4 hover:shadow-sm hover:border-gray-500 transition-all col-span-2"
          >
            <div className="flex items-start justify-between gap-2">
              <div>
                <p className="text-lg mb-1">🕸️</p>
                <p className="text-sm font-bold text-white">
                  {isTA ? "அறிவு வரைபடம்" : "Knowledge Graph"}
                </p>
                <p className="text-xs text-gray-400">
                  {isTA
                    ? "அரசியல் · பொருளாதாரம் · SDG · உறவுகள்"
                    : "Politics · Socioeconomics · SDG · Relationships"}
                </p>
              </div>
              <div className="flex-shrink-0 text-right">
                <p className="text-lg font-black text-blue-400">5.5K</p>
                <p className="text-[10px] text-gray-500 font-semibold">nodes</p>
              </div>
            </div>
          </Link>
          <Link
            href="/pincode-map"
            className="bg-white rounded-2xl border border-gray-200 p-4 hover:shadow-sm transition-all col-span-2"
          >
            <p className="text-lg mb-1">🗺️</p>
            <p className="text-sm font-bold text-gray-900">
              {isTA ? "தொகுதி வரைபடம்" : "Constituency Map"}
            </p>
            <p className="text-xs text-gray-500">
              {isTA ? "பின்கோடு → தொகுதி → மாவட்டம்" : "Pincode → Constituency → District"}
            </p>
          </Link>
        </div>

        <p className="text-center text-xs text-gray-400 pb-4">
          {isTA ? "தரவு: " : "Data: "}
          <a href="https://www.myneta.info/TamilNadu2021/" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">MyNeta</a>
          {" / "}
          <a href="https://adrindia.org" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">ADR</a>
          {", "}
          <a href="https://rchiips.org/nfhs/NFHS-5Reports/TN.pdf" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">NFHS-5</a>
          {", "}
          <a href="https://asercentre.org/aser-2024/" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">ASER 2024</a>
          {", "}
          {isTA ? "கட்சி தேர்தல் அறிக்கைகள்" : "Official party manifestos"}
        </p>
      </div>
    </main>
  );
}
