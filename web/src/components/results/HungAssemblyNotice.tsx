"use client";

import Link from "next/link";

interface HungAssemblyNoticeProps {
  majorityMark: number;
  lang: string;
}

/**
 * Reusable Hung Assembly notice banner.
 * Links to the /hung-assembly explainer page.
 * Use on any election results page when no party reaches majority.
 */
export function HungAssemblyNotice({ majorityMark, lang }: HungAssemblyNoticeProps) {
  const isTA = lang === "ta";

  return (
    <div className="bg-amber-50 border border-amber-200 rounded-xl px-4 py-2.5 text-[11px] text-amber-900 leading-relaxed">
      {isTA
        ? <>எந்தக் கட்சியும் {majorityMark} என்ற மாய எண்ணை எட்டவில்லை — <Link href="/hung-assembly" className="font-bold underline underline-offset-2 hover:text-amber-700">தொங்கு சட்டமன்றம்</Link> ஏற்பட்டுள்ளது.</>
        : <>No party crossed the magic number of {majorityMark} seats — a <Link href="/hung-assembly" className="font-bold underline underline-offset-2 hover:text-amber-700">Hung Assembly</Link> has occurred.</>}
    </div>
  );
}
