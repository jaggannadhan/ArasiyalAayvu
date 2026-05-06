"use client";

import Link from "next/link";
import Image from "next/image";
import { useLanguage } from "@/lib/LanguageContext";

const PARTIES = [
  {
    id: "dmk",
    name: "DMK",
    tamil: "திமுக",
    full: "Dravida Munnetra Kazhagam",
    full_ta: "திராவிட முன்னேற்றக் கழகம்",
    founded: 1949,
    color: "border-red-600",
    bg: "bg-red-50",
    text: "text-red-700",
    flag: "/party-flags/dmk.svg",
  },
  {
    id: "aiadmk",
    name: "AIADMK",
    tamil: "அதிமுக",
    full: "All India Anna Dravida Munnetra Kazhagam",
    full_ta: "அனைத்திந்திய அண்ணா திராவிட முன்னேற்றக் கழகம்",
    founded: 1972,
    color: "border-green-700",
    bg: "bg-green-50",
    text: "text-green-700",
    flag: "/party-flags/aiadmk.svg",
  },
  {
    id: "tvk",
    name: "TVK",
    tamil: "தவெக",
    full: "Tamilaga Vettri Kazhagam",
    full_ta: "தமிழக வெற்றி கழகம்",
    founded: 2024,
    color: "border-sky-600",
    bg: "bg-sky-50",
    text: "text-sky-700",
    flag: "/party-flags/tvk.jpeg",
  },
  {
    id: "bjp",
    name: "BJP",
    tamil: "பாஜக",
    full: "Bharatiya Janata Party",
    full_ta: "பாரதிய ஜனதா கட்சி",
    founded: 1980,
    color: "border-orange-500",
    bg: "bg-orange-50",
    text: "text-orange-700",
    flag: "/party-flags/bjp.svg",
  },
  {
    id: "inc",
    name: "INC",
    tamil: "காங்கிரஸ்",
    full: "Indian National Congress",
    full_ta: "இந்திய தேசிய காங்கிரஸ்",
    founded: 1885,
    color: "border-blue-600",
    bg: "bg-blue-50",
    text: "text-blue-700",
    flag: "/party-flags/inc.svg",
  },
];

export default function PartyHistoryPage() {
  const { lang } = useLanguage();
  const isTA = lang === "ta";

  return (
    <main className="min-h-full bg-gray-50">
      <header className="bg-white border-b border-gray-200 shadow-sm sticky top-0 z-10">
        <div className="max-w-2xl mx-auto px-4 py-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Link href="/" className="text-gray-400 hover:text-gray-600">
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
              </svg>
            </Link>
            <div>
              <h1 className="text-lg font-black text-gray-900">
                {isTA ? "கட்சி வரலாறு" : "Party History"}
              </h1>
              <p className="text-xs text-gray-500">
                {isTA ? "தமிழ்நாட்டின் அரசியல் கட்சிகளின் கதை" : "The story of Tamil Nadu's political parties"}
              </p>
            </div>
          </div>
        </div>
      </header>

      <div className="max-w-2xl mx-auto px-4 py-6 space-y-4">
        {PARTIES.map((party) => (
          <Link
            key={party.id}
            href={`/party-history/${party.id}`}
            className={`block bg-white rounded-2xl border-2 ${party.color} p-5 hover:shadow-lg transition-all group`}
          >
            <div className="flex items-center gap-4">
              <div className="w-14 h-14 rounded-xl overflow-hidden bg-gray-100 flex-shrink-0 border border-gray-200">
                <Image
                  src={party.flag}
                  alt={party.name}
                  width={56}
                  height={56}
                  className="w-full h-full object-contain p-1"
                />
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-baseline gap-2">
                  <h2 className={`text-lg font-black ${party.text}`}>
                    {party.name}
                  </h2>
                  <span className="text-xs text-gray-400 font-semibold">
                    est. {party.founded}
                  </span>
                </div>
                <p className="text-sm text-gray-700 truncate">
                  {isTA ? party.full_ta : party.full}
                </p>
              </div>
              <svg className="w-5 h-5 text-gray-300 group-hover:text-gray-500 transition-colors flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
              </svg>
            </div>
          </Link>
        ))}
      </div>
    </main>
  );
}
