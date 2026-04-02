import type { CriminalCase, CriminalCaseStatus } from "./types";

// ---------------------------------------------------------------------------
// ADR-defined "Serious IPC" sections (max punishment ≥ 5 years or
// explicitly flagged by ADR/MHA as heinous offences).
// Source: ADR Background of Candidates Report methodology
// ---------------------------------------------------------------------------
const SERIOUS_IPC_SECTIONS = new Set([
  // Offences against the state / public tranquility
  "121", "121A", "122", "123", "124A",
  "120B",               // Criminal conspiracy (serious charge)
  "147", "148",         // Rioting with deadly weapons
  "153A", "153B",       // Promoting enmity between groups
  // Murder & grievous hurt
  "302", "303",         // Murder / culpable homicide
  "304",                // Culpable homicide not amounting to murder
  "304B",               // Dowry death
  "307", "308",         // Attempt to murder / culpable homicide
  "326", "326A", "326B", // Grievous hurt by corrosive / acid attack
  // Kidnapping & trafficking
  "363", "363A", "364", "364A", "365", "366", "366A", "366B",
  "370", "370A", "371", "372", "373",
  // Sexual offences
  "354A", "354B", "354C", "354D", // Outraging modesty / stalking
  "375", "376", "376A", "376B", "376C", "376D", "376E",
  "377",
  // Robbery, dacoity, extortion
  "384", "385", "386", "387",   // Extortion
  "392", "393", "394", "395", "396", "397", "398", "399",
  // Arson
  "436", "437", "438",
  // Cheating & forgery (severe)
  "420",                // Cheating (7 yr max)
  "467", "468",         // Forgery of valuable security / for fraud
  // Dowry / domestic violence
  "498A",               // Cruelty by husband/relatives (3 yr — ADR flags)
  "306",                // Abetment of suicide
  // POCSO — referenced by section number from POCSO Act 2012
  "4", "6", "8", "10", "12",  // Only overlap if act == "POCSO"
]);

/**
 * Normalise a raw section string to just digits + optional letter suffix.
 * "Sec. 302 IPC" → "302", "376A" → "376A", "420 r/w 34" → "420"
 */
function normSection(raw: string): string {
  return raw.replace(/^[^0-9]*/, "").split(/[\s,/]/)[0].trim();
}

/**
 * Returns true if ANY of the provided IPC/POCSO sections is classified as
 * "Serious" per ADR methodology (max punishment ≥ 5 yrs or heinous offence).
 */
export function isSeriousCrime(sections: string[]): boolean {
  return sections.some((s) => SERIOUS_IPC_SECTIONS.has(normSection(s)));
}

/**
 * Group an array of CriminalCase objects by their status.
 * Returns object keyed by status with arrays of matching cases.
 */
export function groupCasesByStatus(
  cases: CriminalCase[]
): Record<CriminalCaseStatus, CriminalCase[]> {
  const groups: Record<CriminalCaseStatus, CriminalCase[]> = {
    Pending:   [],
    Dismissed: [],
    Convicted: [],
  };
  for (const c of cases) {
    const key = c.status in groups ? c.status : "Pending";
    groups[key].push(c);
  }
  return groups;
}

/**
 * Count cases per status in one pass.
 */
export function countByStatus(
  cases: CriminalCase[]
): Record<CriminalCaseStatus, number> {
  const g = groupCasesByStatus(cases);
  return {
    Pending:   g.Pending.length,
    Dismissed: g.Dismissed.length,
    Convicted: g.Convicted.length,
  };
}
