/**
 * SDG Alignment — Client-side mapping layer.
 *
 * Manifesto data stays pristine in Firestore.
 * This module computes SDG coverage from promise pillars at runtime.
 *
 * Architecture:
 *   ManifestoPromise[] → computeSDGCoverage() → Map<sdgId, SDGCoverage>
 */

import type { ManifestoPromise, Pillar } from "./types";

// ---------------------------------------------------------------------------
// Pillar → SDG direct mapping
// Which UN SDGs does each manifesto pillar address?
// ---------------------------------------------------------------------------
export const PILLAR_SDG_MAP: Record<Pillar, number[]> = {
  "Agriculture":      [1, 2, 8, 12, 13, 14, 15],
  // SDG 1  — farm loan waivers, guaranteed minimum price, direct farm income support → reduce rural poverty
  // SDG 2  — crop production, dairy, food security directly
  // SDG 8  — farm income = decent economic return for rural workers
  // SDG 12 — circular economy in farming, agricultural residue / waste management
  // SDG 13 — organic farming, pesticide bans, forest agroforestry, biogas/solar for farming → climate action
  // SDG 14 — fisheries, aquaculture, marine fishing communities, fishing harbor & equipment support
  // SDG 15 — land use, soil health, sustainable farming, forest conservation

  "Education":        [2, 4, 8, 10],
  // SDG 2  — midday meal schemes, free school meals, nutritional support → directly fight hunger
  // SDG 4  — schooling, literacy, dropout prevention directly
  // SDG 8  — educated / skilled workforce → productive employment
  // SDG 10 — education access for marginalised groups (SC/ST, girls, disabled) = reduced inequality

  "TASMAC & Revenue": [3, 16],
  // SDG 3  — alcohol policy has direct public health impact
  // SDG 16 — revenue governance, anti-corruption, rule of law

  "Women's Welfare":  [1, 3, 5, 10],
  // SDG 1  — direct income transfers (Magalir Urimai, Amma scheme) reduce household poverty
  // SDG 3  — maternal health, elderly welfare, healthcare access
  // SDG 5  — women's empowerment, economic independence, safety measures
  // SDG 10 — targeted welfare reduces consumption inequality

  "Infrastructure":   [3, 6, 7, 9, 11, 12, 16],
  // SDG 3  — hospitals, PHCs, cancer centres, IVF, healthcare delivery infrastructure
  // SDG 6  — piped water schemes, tank restoration, water conservation policy, sanitation / SWM
  // SDG 7  — power / energy projects, free electricity connections
  // SDG 9  — roads, metro rail, ports, industrial estates = innovation & infrastructure
  // SDG 11 — urban planning, housing, sustainable cities
  // SDG 12 — solid waste management (Green Waste Policy), SWM plants, construction waste
  // SDG 16 — judicial court upgrades, RTI implementation, Social Audit Act, local body elections
};

// ---------------------------------------------------------------------------
// SDG Dependency graph — 1 level
// To fully deliver SDG X, you also need SDG Y to be addressed.
// Source: UN 2030 Agenda interconnections framework.
// ---------------------------------------------------------------------------
export const SDG_DEPENDENCIES: Record<number, number[]> = {
  1:  [8, 4],      // No Poverty        → needs Decent Work + Education
  2:  [1, 8],      // Zero Hunger       → needs No Poverty (affordability) + Decent Work (income)
  3:  [2, 6],      // Good Health       → needs Zero Hunger (nutrition) + Clean Water
  4:  [1, 3],      // Quality Education → needs No Poverty (can attend) + Good Health (can learn)
  5:  [1, 4, 8],   // Gender Equality   → needs No Poverty + Education + Decent Work (equal pay)
  6:  [9, 11],     // Clean Water       → needs Infrastructure (water systems) + Sustainable Cities (sanitation planning)
  7:  [9],         // Clean Energy      → needs Infrastructure capacity
  8:  [4, 9],      // Decent Work       → needs Education (skilled workers) + Infrastructure
  9:  [7, 11],     // Infrastructure    → needs Clean Energy + Sustainable Cities (urban planning)
  10: [1, 4, 8],   // Reduced Inequal.  → needs No Poverty + Education + Decent Work
  11: [9, 7],      // Sustainable Cities→ needs Infrastructure + Clean Energy
  13: [7, 15],     // Climate Action    → needs Clean Energy (emissions) + Life on Land (forests/sinks)
  14: [6, 13],     // Life Below Water  → needs Clean Water (river/coastal health) + Climate Action (ocean warming)
  15: [2, 13],     // Life on Land      → needs Zero Hunger (sustainable farming) + Climate Action
  16: [1, 4, 10],  // Peace & Justice   → needs No Poverty + Education + Reduced Inequalities
};

// ---------------------------------------------------------------------------
// Impact scoring — weights each promise by depth × coverage breadth
// ---------------------------------------------------------------------------

/**
 * Numeric impact score for a single promise.
 *
 * Depth:
 *   transformative → 3   (eliminates debt / provides sustained income floor)
 *   substantive    → 2   (meaningful income supplement / essential cost covered)
 *   supplemental   → 1   (modest transfer, discount, one-time subsidy)
 *   symbolic       → 0   (no material effect)
 *   absent         → 1   (conservative default: treat as supplemental)
 *
 * Coverage multiplier:
 *   universal      → ×1.5
 *   broad_majority → ×1.2
 *   targeted_poor  → ×1.1
 *   specific_group → ×1.0
 *   absent         → ×1.0
 */
/**
 * Impact score for a single promise. Combines depth × coverage breadth ×
 * delivery risk × whether it addresses root causes.
 *
 * Depth:
 *   transformative → 3   substantive → 2   supplemental → 1
 *   symbolic       → 0   absent      → 1   (conservative default)
 *
 * Coverage multiplier:
 *   universal → ×1.5   broad_majority → ×1.2   targeted_poor → ×1.1   else → ×1.0
 *
 * Implementation risk discount (when field is present):
 *   low → ×1.0   medium → ×0.85   high → ×0.65
 *
 * Root-cause discount (when field is present):
 *   true → ×1.0   false (symptom-level fix) → ×0.8
 */
function impactScore(p: ManifestoPromise): number {
  const depth =
    p.impact_depth === "transformative" ? 3 :
    p.impact_depth === "substantive"    ? 2 :
    p.impact_depth === "supplemental"   ? 1 :
    p.impact_depth === "symbolic"       ? 0 : 1;

  const mult =
    p.beneficiary_coverage === "universal"      ? 1.5 :
    p.beneficiary_coverage === "broad_majority" ? 1.2 :
    p.beneficiary_coverage === "targeted_poor"  ? 1.1 : 1.0;

  const riskFactor =
    p.implementation_risk === "low"    ? 1.00 :
    p.implementation_risk === "medium" ? 0.85 :
    p.implementation_risk === "high"   ? 0.65 : 1.0; // absent → no penalty

  const rootFactor =
    p.root_cause_addressed === true  ? 1.0 :
    p.root_cause_addressed === false ? 0.8 : 1.0; // absent → no penalty

  return depth * mult * riskFactor * rootFactor;
}

/**
 * Quality tier based on sum of top-3 impact scores only.
 * Scoring only the top 3 prevents inflation from many low-quality promises.
 *
 * Calibration (examples with extracted welfare fields):
 *   strong   ≥ 3.5 — needs ≥1 transformative×broad or ≥2 substantive×broad
 *   moderate ≥ 1.0 — at least 1 substantive promise (score 2.0) or 2+ supplemental
 *   weak     > 0   — has some coverage but no meaningful impact
 *   none     = 0   — no promises in contributing pillars
 *
 * Seed data (no welfare fields → all default to supplemental×specific = 1.0 each):
 *   top-3 sum = 3.0 → moderate (correctly shows as unassessed, not Strong)
 */
export type CoverageQuality = "strong" | "moderate" | "weak" | "none";

function scoreToCoverageQuality(score: number): CoverageQuality {
  if (score >= 3.5) return "strong";
  if (score >= 1.0) return "moderate";
  if (score > 0)    return "weak";
  return "none";
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
export interface SDGCoverage {
  sdg_id: number;
  /** true when coverage_quality !== "none" */
  covered: boolean;
  /** Quality of coverage, weighted by impact_depth × beneficiary_coverage */
  coverage_quality: CoverageQuality;
  promise_count: number;
  /** Promises with impact_depth = transformative | substantive */
  effective_promise_count: number;
  contributing_pillars: Pillar[];
  /** Top 3 promises ranked by impact score, then specificity */
  top_promises: ManifestoPromise[];
  dependency_ids: number[];
  chain_breaks: number[];
  /** Up to 2 coverage_gap_note strings from the top-scoring promises */
  top_gap_notes: string[];
}

export type SDGCoverageMap = Map<number, SDGCoverage>;

// ---------------------------------------------------------------------------
// Core computation — pure function, no side effects
// ---------------------------------------------------------------------------
export function computeSDGCoverage(promises: ManifestoPromise[]): SDGCoverageMap {
  // Group promises by pillar
  const byPillar = new Map<Pillar, ManifestoPromise[]>();
  for (const p of promises) {
    const pillar = p.category as Pillar;
    const arr = byPillar.get(pillar) ?? [];
    arr.push(p);
    byPillar.set(pillar, arr);
  }

  // Pass 1 — quality-weighted coverage per SDG (1–17)
  const coverage: SDGCoverageMap = new Map();

  for (let sdg = 1; sdg <= 17; sdg++) {
    const contributingPillars: Pillar[] = [];
    const allPromises: ManifestoPromise[] = [];

    for (const [pillar, sdgIds] of Object.entries(PILLAR_SDG_MAP) as [Pillar, number[]][]) {
      if (sdgIds.includes(sdg)) {
        const pillarPromises = byPillar.get(pillar) ?? [];
        if (pillarPromises.length > 0) {
          contributingPillars.push(pillar);
          allPromises.push(...pillarPromises);
        }
      }
    }

    const effectiveCount = allPromises.filter(
      (p) => p.impact_depth === "transformative" || p.impact_depth === "substantive"
    ).length;

    // Rank: impact score desc, then amount > scheme > plain
    const ranked = [...allPromises].sort((a, b) => {
      const diff = impactScore(b) - impactScore(a);
      if (diff !== 0) return diff;
      const specA = (a.amount_mentioned ? 2 : 0) + (a.scheme_name ? 1 : 0);
      const specB = (b.amount_mentioned ? 2 : 0) + (b.scheme_name ? 1 : 0);
      return specB - specA;
    });

    // Score top 3 only — prevents inflation from many low-quality pillar promises
    const totalScore = ranked.slice(0, 3).reduce((sum, p) => sum + impactScore(p), 0);
    const quality = scoreToCoverageQuality(totalScore);

    // Collect up to 2 distinct gap notes from top promises
    const topGapNotes = ranked
      .slice(0, 5)
      .map((p) => p.coverage_gap_note)
      .filter((n): n is string => typeof n === "string" && n.trim().length > 0)
      .slice(0, 2);

    coverage.set(sdg, {
      sdg_id: sdg,
      covered: quality !== "none",
      coverage_quality: quality,
      promise_count: allPromises.length,
      effective_promise_count: effectiveCount,
      contributing_pillars: contributingPillars,
      top_promises: ranked.slice(0, 3),
      dependency_ids: SDG_DEPENDENCIES[sdg] ?? [],
      chain_breaks: [], // filled in pass 2
      top_gap_notes: topGapNotes,
    });
  }

  // Pass 2 — chain break analysis
  for (const cov of coverage.values()) {
    if (!cov.covered) continue;
    cov.chain_breaks = cov.dependency_ids.filter((depId) => {
      const dep = coverage.get(depId);
      return dep ? !dep.covered : false;
    });
  }

  return coverage;
}

// ---------------------------------------------------------------------------
// Derived helpers
// ---------------------------------------------------------------------------

/** SDG IDs that are covered by at least one promise. */
export function coveredSDGs(m: SDGCoverageMap): number[] {
  return [...m.entries()].filter(([, v]) => v.covered).map(([k]) => k).sort((a, b) => a - b);
}

/** SDG IDs with no promises at all. */
export function uncoveredSDGs(m: SDGCoverageMap): number[] {
  return [...m.entries()].filter(([, v]) => !v.covered).map(([k]) => k).sort((a, b) => a - b);
}

/** Covered SDGs that have at least one chain break (dependency gap). */
export function brokenChainSDGs(m: SDGCoverageMap): number[] {
  return [...m.entries()]
    .filter(([, v]) => v.covered && v.chain_breaks.length > 0)
    .map(([k]) => k)
    .sort((a, b) => a - b);
}

/**
 * Reverse dependency: which covered SDGs does an uncovered SDG block?
 * e.g. if SDG 6 is uncovered, which covered SDGs depend on it?
 */
export function blockedBy(uncoveredId: number, m: SDGCoverageMap): number[] {
  const blocked: number[] = [];
  for (const cov of m.values()) {
    if (cov.covered && cov.chain_breaks.includes(uncoveredId)) {
      blocked.push(cov.sdg_id);
    }
  }
  return blocked.sort((a, b) => a - b);
}
