"use client";

import Link from "next/link";
import { useLanguage } from "@/lib/LanguageContext";

export default function HungAssemblyPage() {
  const { lang, setLang } = useLanguage();
  const isTA = lang === "ta";

  return (
    <main className="min-h-full bg-gray-50">
      {/* Header */}
      <header className="sticky top-0 z-30 bg-white border-b border-gray-200 shadow-sm">
        <div className="max-w-3xl mx-auto px-4 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Link href="/2026_results" className="text-gray-400 hover:text-gray-600 transition-colors">
              <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
              </svg>
            </Link>
            <div>
              <h1 className="text-lg font-black text-gray-900">
                {isTA ? "தொங்கு சட்டமன்றம்" : "Hung Assembly"}
              </h1>
              <p className="text-[10px] text-gray-500 font-semibold uppercase tracking-wider">
                {isTA ? "அரசியலமைப்பு விளக்கம்" : "Constitutional Explainer"}
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

        {/* Definition */}
        <section>
          <h2 className="text-xl font-black text-gray-900 mb-3">
            {isTA ? "தொங்கு சட்டமன்றம் என்றால் என்ன?" : "What is a Hung Assembly?"}
          </h2>
          <div className="bg-amber-50 border border-amber-200 rounded-2xl p-4 text-sm text-amber-900 leading-relaxed">
            {isTA
              ? "ஒரு தேர்தலுக்குப் பிறகு, எந்த அரசியல் கட்சியும் அல்லது கூட்டணியும் சட்டமன்றத்தில் முழுமையான பெரும்பான்மை இடங்களைப் (234 இடங்களில் 118) பெறாத அரசியல் நிலையே தொங்கு சட்டமன்றம் ஆகும். இது அரசு அமைப்பதில் பேச்சுவார்த்தைகள் மற்றும் அரசியல் சூழ்ச்சிகளுக்கு வழிவகுக்கிறது."
              : "A Hung Assembly is a political scenario where no single party or pre-poll alliance secures an absolute majority of seats (118 out of 234) in the legislature following an election. This leads to negotiations, coalition-building, and constitutional procedures to resolve the deadlock."}
          </div>
        </section>

        {/* What happens — 3 paths explained */}
        <section>
          <h2 className="text-lg font-black text-gray-900 mb-3">
            {isTA ? "என்ன நடக்கும்?" : "What Happens Next?"}
          </h2>
          <p className="text-xs text-gray-500 mb-3">
            {isTA
              ? "ஆளுநர் பின்வரும் மூன்று வழிகளில் ஒன்றை தேர்வு செய்கிறார் (சர்க்காரியா கமிஷன் பரிந்துரையின்படி, உச்ச நீதிமன்றத்தால் உறுதிசெய்யப்பட்டது):"
              : "The Governor chooses one of three paths (per Sarkaria Commission recommendation, affirmed by Supreme Court in Rameshwar Prasad vs. Union of India, 2005):"}
          </p>

          <div className="space-y-3">
            {/* 1. Single largest party */}
            <div className="border border-blue-200 bg-blue-50 rounded-xl p-4">
              <div className="flex items-start gap-3">
                <span className="text-lg font-black text-blue-300">1</span>
                <div>
                  <p className="text-sm font-bold text-blue-900">
                    {isTA ? "மிகப்பெரிய கட்சி — வெளி ஆதரவுடன்" : "Single Largest Party — With Outside Support"}
                  </p>
                  <p className="text-xs text-blue-800 mt-1 leading-relaxed">
                    {isTA
                      ? "அதிக இடங்களை வென்ற கட்சி தனியாக அரசு அமைக்கிறது. சுயேச்சை உறுப்பினர்கள் மற்றும் சிறிய கட்சிகள் நம்பிக்கை வாக்கெடுப்பின் போது ஆதரவு வாக்குகளை அளிக்க ஒப்புக்கொள்கின்றன, ஆனால் அவர்கள் அமைச்சரவையில் சேர மாட்டார்கள் — எந்த அமைச்சர் பதவியும் கிடையாது."
                      : "The party with the most seats governs alone. Independents and smaller parties agree to vote in favour during confidence motions, but they do NOT join the cabinet — no minister posts for them."}
                  </p>
                </div>
              </div>
            </div>

            {/* 2. Coalition government */}
            <div className="border border-emerald-200 bg-emerald-50 rounded-xl p-4">
              <div className="flex items-start gap-3">
                <span className="text-lg font-black text-emerald-300">2</span>
                <div>
                  <p className="text-sm font-bold text-emerald-900">
                    {isTA ? "கூட்டணி அரசு — அனைவரும் அரசில் இணைதல்" : "Coalition Government — All Partners Join"}
                  </p>
                  <p className="text-xs text-emerald-800 mt-1 leading-relaxed">
                    {isTA
                      ? "பல கட்சிகள் ஒன்றிணைந்து கூட்டணி அரசு அமைக்கின்றன. ஒவ்வொரு கூட்டணி கட்சியும் அமைச்சர் பதவிகள் பெறுகிறது — துணை முதலமைச்சர், குறிப்பிட்ட துறை அமைச்சகங்கள் போன்றவை. அதிகாரம் பகிர்ந்தளிக்கப்படுகிறது."
                      : "Multiple parties join together to form a coalition government. Every coalition partner gets minister posts — Deputy CM, specific ministry portfolios, etc. Power is shared across parties proportional to their seat strength."}
                  </p>
                </div>
              </div>
            </div>

            {/* 3. President's Rule */}
            <div className="border border-red-200 bg-red-50 rounded-xl p-4">
              <div className="flex items-start gap-3">
                <span className="text-lg font-black text-red-300">3</span>
                <div>
                  <p className="text-sm font-bold text-red-900">
                    {isTA ? "குடியரசுத் தலைவர் ஆட்சி — கலைப்பு & புதிய தேர்தல்" : "President's Rule — Dissolution & Fresh Elections"}
                  </p>
                  <p className="text-xs text-red-800 mt-1 leading-relaxed">
                    {isTA
                      ? "அரசு அமைக்கும் அனைத்து முயற்சிகளும் தோல்வியடைந்தால், ஆளுநர் சட்டமன்றத்தை நிறுத்தி வைக்கிறார் அல்லது கலைக்கிறார். அரசியலமைப்பு பிரிவு 356-ன் கீழ் குடியரசுத் தலைவர் ஆட்சி விதிக்கப்பட்டு, புதிய தேர்தல் நடத்தப்படும் வரை மத்திய அரசு நேரடியாக நிர்வகிக்கிறது."
                      : "If all attempts to form a government fail, the Governor suspends or dissolves the assembly. President's Rule is imposed under Article 356, and the central government directly administers the state until fresh elections can be held."}
                  </p>
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* Flow diagram */}
        <section>
          <h2 className="text-lg font-black text-gray-900 mb-4">
            {isTA ? "முடிவெடுக்கும் வழிமுறை" : "Decision Flowchart"}
          </h2>
          <div className="bg-white border border-gray-200 rounded-2xl p-5 overflow-x-auto">
            {/* SVG-based flow diagram — fixed coordinates, no layout breakage */}
            <svg viewBox="0 0 480 340" className="w-full max-w-lg mx-auto" fill="none" xmlns="http://www.w3.org/2000/svg">
              {/* ── Lines ── */}
              {/* Vertical from top box to T-junction */}
              <line x1="240" y1="40" x2="240" y2="70" stroke="#d1d5db" strokeWidth="1.5" />
              {/* Horizontal T-bar */}
              <line x1="80" y1="70" x2="400" y2="70" stroke="#d1d5db" strokeWidth="1.5" />
              {/* Three verticals down to option boxes */}
              <line x1="80" y1="70" x2="80" y2="100" stroke="#d1d5db" strokeWidth="1.5" />
              <line x1="240" y1="70" x2="240" y2="100" stroke="#d1d5db" strokeWidth="1.5" />
              <line x1="400" y1="70" x2="400" y2="100" stroke="#d1d5db" strokeWidth="1.5" />
              {/* Arrows */}
              <polygon points="80,98 76,88 84,88" fill="#9ca3af" />
              <polygon points="240,98 236,88 244,88" fill="#9ca3af" />
              <polygon points="400,98 396,88 404,88" fill="#9ca3af" />
              {/* Verticals from option boxes to description boxes */}
              <line x1="80" y1="140" x2="80" y2="155" stroke="#d1d5db" strokeWidth="1.5" />
              <line x1="240" y1="140" x2="240" y2="155" stroke="#d1d5db" strokeWidth="1.5" />
              <line x1="400" y1="140" x2="400" y2="155" stroke="#d1d5db" strokeWidth="1.5" />
              {/* Verticals from desc boxes 1 & 2 down to merge */}
              <line x1="80" y1="210" x2="80" y2="250" stroke="#d1d5db" strokeWidth="1.5" />
              <line x1="240" y1="210" x2="240" y2="250" stroke="#d1d5db" strokeWidth="1.5" />
              {/* Horizontal merge */}
              <line x1="80" y1="250" x2="240" y2="250" stroke="#d1d5db" strokeWidth="1.5" />
              {/* Vertical from merge midpoint to floor test */}
              <line x1="160" y1="250" x2="160" y2="275" stroke="#d1d5db" strokeWidth="1.5" />
              <polygon points="160,273 156,263 164,263" fill="#9ca3af" />

              {/* ── Boxes ── */}
              {/* Top: HUNG ASSEMBLY */}
              <rect x="140" y="8" width="200" height="32" rx="8" fill="#fef3c7" stroke="#f59e0b" strokeWidth="2" />
              <text x="240" y="28" textAnchor="middle" className="text-[11px] font-black fill-amber-800" style={{ fontSize: 11, fontWeight: 900 }}>
                {isTA ? "தொங்கு சட்டமன்றம்" : "HUNG ASSEMBLY DECLARED"}
              </text>

              {/* Option 1: Largest party */}
              <rect x="10" y="100" width="140" height="40" rx="8" fill="#eff6ff" stroke="#93c5fd" strokeWidth="2" />
              <text x="80" y="116" textAnchor="middle" className="fill-blue-800" style={{ fontSize: 9, fontWeight: 900 }}>
                {isTA ? "மிகப்பெரிய கட்சி" : "LARGEST PARTY"}
              </text>
              <text x="80" y="130" textAnchor="middle" className="fill-blue-800" style={{ fontSize: 9, fontWeight: 900 }}>
                {isTA ? "+ வெளி ஆதரவு" : "+ OUTSIDE SUPPORT"}
              </text>

              {/* Option 2: Coalition */}
              <rect x="170" y="100" width="140" height="40" rx="8" fill="#ecfdf5" stroke="#6ee7b7" strokeWidth="2" />
              <text x="240" y="116" textAnchor="middle" className="fill-emerald-800" style={{ fontSize: 9, fontWeight: 900 }}>
                {isTA ? "கூட்டணி" : "COALITION"}
              </text>
              <text x="240" y="130" textAnchor="middle" className="fill-emerald-800" style={{ fontSize: 9, fontWeight: 900 }}>
                {isTA ? "அரசு" : "GOVERNMENT"}
              </text>

              {/* Option 3: President's Rule */}
              <rect x="330" y="100" width="140" height="40" rx="8" fill="#fef2f2" stroke="#fca5a5" strokeWidth="2" />
              <text x="400" y="116" textAnchor="middle" className="fill-red-800" style={{ fontSize: 9, fontWeight: 900 }}>
                {isTA ? "குடியரசுத் தலைவர்" : "PRESIDENT'S"}
              </text>
              <text x="400" y="130" textAnchor="middle" className="fill-red-800" style={{ fontSize: 9, fontWeight: 900 }}>
                {isTA ? "ஆட்சி" : "RULE"}
              </text>

              {/* Desc 1 */}
              <rect x="10" y="158" width="140" height="48" rx="6" fill="none" stroke="#93c5fd" strokeWidth="1" strokeDasharray="4 2" />
              <text x="80" y="172" textAnchor="middle" className="fill-gray-500" style={{ fontSize: 8 }}>
                {isTA ? "ஒரே கட்சி ஆட்சி;" : "Single party governs;"}
              </text>
              <text x="80" y="184" textAnchor="middle" className="fill-gray-500" style={{ fontSize: 8 }}>
                {isTA ? "மற்றவர்கள் வாக்கு மட்டும்" : "others only pledge votes,"}
              </text>
              <text x="80" y="196" textAnchor="middle" className="fill-gray-500" style={{ fontSize: 8 }}>
                {isTA ? "அளிப்பர், பதவி இல்லை" : "no cabinet posts"}
              </text>

              {/* Desc 2 */}
              <rect x="170" y="158" width="140" height="48" rx="6" fill="none" stroke="#6ee7b7" strokeWidth="1" strokeDasharray="4 2" />
              <text x="240" y="172" textAnchor="middle" className="fill-gray-500" style={{ fontSize: 8 }}>
                {isTA ? "பல கட்சிகள் இணைந்து" : "Multiple parties share"}
              </text>
              <text x="240" y="184" textAnchor="middle" className="fill-gray-500" style={{ fontSize: 8 }}>
                {isTA ? "அமைச்சர் பதவிகளை" : "power; all get"}
              </text>
              <text x="240" y="196" textAnchor="middle" className="fill-gray-500" style={{ fontSize: 8 }}>
                {isTA ? "பகிர்ந்து ஆட்சி" : "minister posts"}
              </text>

              {/* Desc 3 */}
              <rect x="330" y="158" width="140" height="48" rx="6" fill="none" stroke="#fca5a5" strokeWidth="1" strokeDasharray="4 2" />
              <text x="400" y="176" textAnchor="middle" className="fill-gray-500" style={{ fontSize: 8 }}>
                {isTA ? "சட்டமன்றம் கலைப்பு;" : "Assembly dissolved;"}
              </text>
              <text x="400" y="188" textAnchor="middle" className="fill-gray-500" style={{ fontSize: 8 }}>
                {isTA ? "புதிய தேர்தல்" : "fresh elections"}
              </text>

              {/* Floor Test box */}
              <rect x="90" y="278" width="140" height="40" rx="8" fill="#eef2ff" stroke="#a5b4fc" strokeWidth="2" />
              <text x="160" y="296" textAnchor="middle" className="fill-indigo-800" style={{ fontSize: 10, fontWeight: 900 }}>
                {isTA ? "நம்பிக்கை வாக்கெடுப்பு" : "FLOOR TEST"}
              </text>
              <text x="160" y="310" textAnchor="middle" className="fill-indigo-500" style={{ fontSize: 8 }}>
                {isTA ? "10-14 நாட்களுக்குள்" : "Within 10-14 days"}
              </text>
            </svg>

            {/* Sarkaria attribution */}
            <p className="text-[9px] text-gray-400 text-center mt-3">
              {isTA
                ? "ஆதாரம்: சர்க்காரியா கமிஷன் (1988) · உச்ச நீதிமன்றம் — Rameshwar Prasad vs Union of India (2005)"
                : "Source: Sarkaria Commission (1988) · Supreme Court — Rameshwar Prasad vs. Union of India (2005)"}
            </p>
          </div>
        </section>

        {/* Floor test */}
        <section>
          <h2 className="text-lg font-black text-gray-900 mb-3">
            {isTA ? "நம்பிக்கை வாக்கெடுப்பு (Floor Test)" : "The Floor Test"}
          </h2>
          <div className="bg-indigo-50 border border-indigo-200 rounded-2xl p-4 text-sm text-indigo-900 leading-relaxed">
            {isTA
              ? "அரசு அமைக்க அழைக்கப்பட்ட தலைவர் 10-14 நாட்களுக்குள் சட்டமன்றத்தில் பெரும்பான்மையை நிரூபிக்க வேண்டும். உச்ச நீதிமன்றத்தின் 1994 S.R. Bommai vs Union of India வழக்கு — சட்டமன்றத் தளமே பெரும்பான்மையை சோதிக்கும் ஒரே செல்லுபடியான இடம் என்று நிறுவியது. ஆளுநர் தனது சொந்த தீர்ப்பின் அடிப்படையில் பெரும்பான்மையை தீர்மானிக்க முடியாது."
              : "The invited leader must prove their majority through a floor test in the assembly within 10-14 days. The Supreme Court's landmark '1994 S.R. Bommai vs. Union of India' case established that the floor of the legislature is the only valid place to test a government's majority. The Governor cannot determine majority based on their own assessment."}
          </div>
        </section>

        {/* Constitutional references */}
        <section>
          <h2 className="text-lg font-black text-gray-900 mb-3">
            {isTA ? "அரசியலமைப்பு குறிப்புகள்" : "Constitutional References"}
          </h2>
          <div className="bg-white border border-gray-200 rounded-2xl divide-y divide-gray-100">
            {[
              {
                article: "Article 164",
                url: "https://indiankanoon.org/doc/578636/",
                en: "Appointment of state ministers — The Chief Minister shall be appointed by the Governor, and other Ministers shall be appointed by the Governor on the advice of the Chief Minister.",
                ta: "மாநில அமைச்சர்கள் நியமனம் — முதலமைச்சரை ஆளுநர் நியமிக்கிறார், மற்ற அமைச்சர்களை முதலமைச்சரின் ஆலோசனையின் பேரில் ஆளுநர் நியமிக்கிறார்."
              },
              {
                article: "Article 75",
                url: "https://indiankanoon.org/doc/345627/",
                en: "Appointment of union ministers — The Prime Minister shall be appointed by the President, and other Ministers shall be appointed by the President on the advice of the Prime Minister.",
                ta: "மத்திய அமைச்சர்கள் நியமனம் — பிரதமரை குடியரசுத் தலைவர் நியமிக்கிறார், மற்ற அமைச்சர்களை பிரதமரின் ஆலோசனையின் பேரில் குடியரசுத் தலைவர் நியமிக்கிறார்."
              },
              {
                article: "Article 356",
                url: "https://indiankanoon.org/doc/522079/",
                en: "Failure of constitutional machinery in a state — If the President is satisfied that the government of a State cannot be carried on in accordance with the Constitution, they may assume all functions of the State Government.",
                ta: "மாநிலத்தில் அரசியலமைப்பு இயந்திரம் தோல்வி — ஒரு மாநில அரசு அரசியலமைப்பின்படி நடத்த முடியாது என்று குடியரசுத் தலைவர் திருப்தியடைந்தால், மாநில அரசின் அனைத்து செயல்பாடுகளையும் ஏற்கலாம்."
              },
            ].map((ref) => (
              <div key={ref.article} className="p-4">
                <a
                  href={ref.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-sm font-bold text-blue-700 underline underline-offset-2 hover:text-blue-500"
                >
                  {ref.article}
                </a>
                <p className="text-xs text-gray-600 mt-1 leading-relaxed">
                  {isTA ? ref.ta : ref.en}
                </p>
              </div>
            ))}
          </div>
        </section>

        {/* Sources */}
        <p className="text-center text-[10px] text-gray-400 pb-4">
          {isTA ? "ஆதாரங்கள்: " : "Sources: "}
          <a href="https://indiankanoon.org" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">Indian Kanoon</a>
          {" · "}
          <a href="https://www.barandbench.com/columns/hung-assembly-governor-call" target="_blank" rel="noopener noreferrer" className="underline underline-offset-2 hover:text-gray-600">Bar & Bench</a>
          {" · "}
          Sarkaria Commission Report (1988)
          {" · "}
          S.R. Bommai vs. Union of India (1994)
        </p>
      </div>
    </main>
  );
}
