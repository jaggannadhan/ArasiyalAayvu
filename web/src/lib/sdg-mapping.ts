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
// Types
// ---------------------------------------------------------------------------
export interface SDGCoverage {
  sdg_id: number;
  covered: boolean;
  promise_count: number;
  contributing_pillars: Pillar[];
  // Top 3 most specific promises (sorted: has amount > has scheme > plain)
  top_promises: ManifestoPromise[];
  // SDG IDs this goal depends on (1-level)
  dependency_ids: number[];
  // Subset of dependency_ids that are NOT covered — the chain breaks
  chain_breaks: number[];
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

  // Pass 1 — direct coverage per SDG (1–17)
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

    // Rank by specificity: amount > scheme > plain text
    const specificity = (p: ManifestoPromise) =>
      (p.amount_mentioned ? 2 : 0) + (p.scheme_name ? 1 : 0);
    const topPromises = [...allPromises]
      .sort((a, b) => specificity(b) - specificity(a))
      .slice(0, 3);

    coverage.set(sdg, {
      sdg_id: sdg,
      covered: allPromises.length > 0,
      promise_count: allPromises.length,
      contributing_pillars: contributingPillars,
      top_promises: topPromises,
      dependency_ids: SDG_DEPENDENCIES[sdg] ?? [],
      chain_breaks: [], // filled in pass 2
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
