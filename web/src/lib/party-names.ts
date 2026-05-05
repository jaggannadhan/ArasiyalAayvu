/** Tamil names and abbreviations for political parties. */

interface PartyInfo {
  en: string;
  ta: string;
  abbrEn: string;
  abbrTa: string;
}

const PARTIES: PartyInfo[] = [
  { en: "Dravida Munnetra Kazhagam",                          ta: "திராவிட முன்னேற்றக் கழகம்",                              abbrEn: "DMK",     abbrTa: "தி.மு.க" },
  { en: "All India Anna Dravida Munnetra Kazhagam",           ta: "அனைத்திந்திய அண்ணா திராவிட முன்னேற்றக் கழகம்",           abbrEn: "ADMK",    abbrTa: "அ.தி.மு.க" },
  { en: "Tamilaga Vettri Kazhagam",                           ta: "தமிழக வெற்றி கழகம்",                                     abbrEn: "TVK",     abbrTa: "த.வெ.க" },
  { en: "Naam Tamilar Katchi",                                ta: "நாம் தமிழர் கட்சி",                                      abbrEn: "NTK",     abbrTa: "நா.த.க" },
  { en: "Indian National Congress",                           ta: "இந்திய தேசிய காங்கிரஸ்",                                 abbrEn: "INC",     abbrTa: "இ.தே.கா" },
  { en: "Pattali Makkal Katchi",                              ta: "பாட்டாளி மக்கள் கட்சி",                                  abbrEn: "PMK",     abbrTa: "பா.ம.க" },
  { en: "Viduthalai Chiruthaigal Katchi",                     ta: "விடுதலைச் சிறுத்தைகள் கட்சி",                             abbrEn: "VCK",     abbrTa: "வி.சி.க" },
  { en: "Bharatiya Janata Party",                             ta: "பாரதிய ஜனதா கட்சி",                                     abbrEn: "BJP",     abbrTa: "பா.ஜ.க" },
  { en: "Communist Party of India (Marxist)",                 ta: "இந்திய கம்யூனிஸ்ட் கட்சி (மார்க்சிஸ்ட்)",               abbrEn: "CPI(M)",  abbrTa: "இ.க.க (மா)" },
  { en: "Communist Party of India",                           ta: "இந்திய கம்யூனிஸ்ட் கட்சி",                               abbrEn: "CPI",     abbrTa: "இ.க.க" },
  { en: "Marumalarchi Dravida Munnetra Kazhagam",             ta: "மறுமலர்ச்சி திராவிட முன்னேற்றக் கழகம்",                   abbrEn: "MDMK",    abbrTa: "ம.தி.மு.க" },
  { en: "Desiya Murpokku Dravida Kazhagam",                   ta: "தேசிய முற்போக்கு திராவிட கழகம்",                           abbrEn: "DMDK",    abbrTa: "தே.மு.தி.க" },
  { en: "Indian Union Muslim League",                         ta: "இந்திய யூனியன் முஸ்லிம் லீக்",                            abbrEn: "IUML",    abbrTa: "இ.யூ.மு.லீ" },
  { en: "Amma Makkal Munnettra Kazagam",                      ta: "அம்மா மக்கள் முன்னேற்றக் கழகம்",                          abbrEn: "AMMK",    abbrTa: "அ.ம.மு.க" },
  { en: "Independent",                                        ta: "சுயேச்சை",                                                abbrEn: "IND",     abbrTa: "சுயேச்சை" },
];

// Lookup maps built once
const _byName: Record<string, PartyInfo> = {};
const _byAbbr: Record<string, PartyInfo> = {};
for (const p of PARTIES) {
  _byName[p.en.toLowerCase()] = p;
  _byAbbr[p.abbrEn.toLowerCase()] = p;
}

/** Get the Tamil abbreviation for a party (by full English name or English abbreviation). */
export function partyAbbrTa(nameOrAbbr: string): string {
  const key = nameOrAbbr.toLowerCase();
  const info = _byName[key] || _byAbbr[key];
  return info?.abbrTa ?? nameOrAbbr;
}

/** Get the English abbreviation for a party (by full English name). */
export function partyAbbrEn(fullName: string): string {
  const info = _byName[fullName.toLowerCase()];
  return info?.abbrEn ?? fullName.slice(0, 4).toUpperCase();
}

/** Get the full Tamil name for a party (by full English name). */
export function partyNameTa(fullName: string): string {
  const info = _byName[fullName.toLowerCase()];
  return info?.ta ?? fullName;
}

/** Get abbreviation in the current language. */
export function partyAbbr(nameOrAbbr: string, lang: string): string {
  if (lang === "ta") return partyAbbrTa(nameOrAbbr);
  const key = nameOrAbbr.toLowerCase();
  const info = _byName[key] || _byAbbr[key];
  return info?.abbrEn ?? nameOrAbbr;
}

/** Get full party name in the current language. */
export function partyName(fullNameEn: string, lang: string): string {
  if (lang !== "ta") return fullNameEn;
  return partyNameTa(fullNameEn);
}
