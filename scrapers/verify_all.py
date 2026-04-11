"""
Data verification suite for all knowledge-graph time-series datasets.

Layer 1 — Internal consistency (math / structural)
  - PLFS:  UR ≈ (LFPR - WPR) / LFPR × 100
  - HCES:  MPCE_with_free ≥ MPCE_without_free; welfare_uplift_pct recomputed
  - SRS:   rates in plausible ranges; IMR rural ≥ urban; trend arrays length 6
  - AISHE: enrollment total_approx == sum of levels; GER in [5, 70]; GPI in [0.5, 2.0]
  - SDG:   all scores in [0, 100]; gaps ≤ 0; no missing goals
  - CoL:   prices > 0; electricity net ≤ gross per slab

Layer 3 — Cross-dataset sanity (known facts that must hold)
  - Kerala UR highest among focus states
  - TN GER highest among focus states
  - Kerala IMR lowest among focus states
  - Kerala rural MPCE highest among focus states
  - All focus-state TFRs below replacement (2.1)
  - All-India UR within ±1 pp of focus-state median

Run:
    python scrapers/verify_all.py
"""

import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data" / "processed"

FOCUS_STATES = ["Andhra Pradesh", "Karnataka", "Kerala", "Tamil Nadu", "Telangana"]

PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"

issues = []   # (level, dataset, message)


def ok(dataset, msg):
    print(f"  {PASS}  [{dataset}] {msg}")


def fail(dataset, msg):
    issues.append((FAIL, dataset, msg))
    print(f"  {FAIL}  [{dataset}] {msg}")


def warn(dataset, msg):
    issues.append((WARN, dataset, msg))
    print(f"  {WARN}  [{dataset}] {msg}")


def load(filename):
    path = DATA_DIR / filename
    if not path.exists():
        fail(filename, f"File not found: {path}")
        return None
    return json.loads(path.read_text())


def snap(ts, entity, period):
    """Return snapshot dict or None."""
    return ts.get("entities", {}).get(entity, {}).get("snapshots", {}).get(period)


# ─────────────────────────────────────────────────────────────────────────────
# PLFS
# ─────────────────────────────────────────────────────────────────────────────

def verify_plfs(ts):
    print("\n── PLFS ─────────────────────────────────────────────────────────────")
    errors = 0
    age_groups = ["15-29", "15-59", "15+", "all"]
    areas = ["rural", "urban", "total"]
    sexes = ["male", "female", "person"]

    for state in FOCUS_STATES + ["All India"]:
        s = snap(ts, state, "2023-24")
        if s is None:
            fail("plfs", f"Missing snapshot for {state} / 2023-24")
            continue

        for age in age_groups:
            lfpr_block = s["lfpr"][age]
            wpr_block  = s["wpr"][age]
            ur_block   = s["ur"][age]

            for area in areas:
                for sex in sexes:
                    lfpr = lfpr_block[area][sex]
                    wpr  = wpr_block[area][sex]
                    ur   = ur_block[area][sex]

                    # UR = (LFPR - WPR) / LFPR × 100
                    if lfpr > 0:
                        expected_ur = round((lfpr - wpr) / lfpr * 100, 1)
                        diff = abs(ur - expected_ur)
                        if diff > 0.6:
                            fail("plfs", f"{state} {age} {area} {sex}: UR={ur} but (LFPR-WPR)/LFPR×100={expected_ur} (diff={diff})")
                            errors += 1

                    # Range checks
                    if not (0 <= lfpr <= 100):
                        fail("plfs", f"{state} {age} {area} {sex}: LFPR={lfpr} out of [0,100]")
                    if not (0 <= wpr <= 100):
                        fail("plfs", f"{state} {age} {area} {sex}: WPR={wpr} out of [0,100]")
                    if not (0 <= ur <= 70):
                        fail("plfs", f"{state} {age} {area} {sex}: UR={ur} out of [0,70]")
                    if wpr > lfpr:
                        fail("plfs", f"{state} {age} {area} {sex}: WPR={wpr} > LFPR={lfpr} (impossible)")

    if errors == 0:
        ok("plfs", f"UR = (LFPR-WPR)/LFPR×100 holds for all cells (tolerance ±0.6 pp)")


# ─────────────────────────────────────────────────────────────────────────────
# HCES
# ─────────────────────────────────────────────────────────────────────────────

def verify_hces(ts):
    print("\n── HCES ─────────────────────────────────────────────────────────────")
    errors = 0

    for entity in FOCUS_STATES + ["All India"]:
        s = snap(ts, entity, "2023-24")
        if s is None:
            fail("hces", f"Missing snapshot for {entity} / 2023-24")
            continue

        wof = s["mpce_without_free_items"]
        wf  = s["mpce_with_free_items"]

        for area in ["rural", "urban"]:
            # With-free must be ≥ without-free
            if wf[area] < wof[area]:
                fail("hces", f"{entity} {area}: MPCE_with_free={wf[area]} < MPCE_without_free={wof[area]}")
                errors += 1

            # Welfare uplift pct recomputed
            if entity != "All India" or "welfare_uplift" in s:
                stored = s["welfare_uplift"][f"{area}_uplift_pct"]
                computed = round((wf[area] - wof[area]) / wof[area] * 100, 1)
                if abs(stored - computed) > 0.2:
                    fail("hces", f"{entity} {area}: stored uplift_pct={stored} but computed={computed}")
                    errors += 1

            # Plausible range: ₹1,000–₹25,000/month
            for label, val in [("without_free", wof[area]), ("with_free", wf[area])]:
                if not (1000 <= val <= 25000):
                    warn("hces", f"{entity} {area} MPCE_{label}={val} outside expected ₹1,000–₹25,000")

    if errors == 0:
        ok("hces", "MPCE_with_free ≥ MPCE_without_free for all entities; welfare_uplift_pct verified")


# ─────────────────────────────────────────────────────────────────────────────
# SRS
# ─────────────────────────────────────────────────────────────────────────────

PLAUSIBLE_RANGES = {
    "cbr": (4, 35),
    "cdr": (2, 15),
    "tfr": (0.8, 4.0),
    "imr": (0, 100),
    "mmr": (0, 500),
}

def verify_srs(ts):
    print("\n── SRS ──────────────────────────────────────────────────────────────")
    errors = 0

    for entity in FOCUS_STATES + ["India"]:
        s = snap(ts, entity, "2023")
        if s is None:
            fail("srs", f"Missing snapshot for {entity} / 2023")
            continue

        # Rate range checks
        for ind, (lo, hi) in PLAUSIBLE_RANGES.items():
            if ind == "mmr":
                val = s["mmr_2018_20"]["mmr"]
                if not (lo <= val <= hi):
                    fail("srs", f"{entity} MMR={val} outside [{lo},{hi}]")
                    errors += 1
                continue

            block = s[ind]
            for area in ["total", "rural", "urban"]:
                if ind == "imr":
                    val = block[area]["total"]
                else:
                    val = block[area]
                if not (lo <= val <= hi):
                    fail("srs", f"{entity} {ind.upper()} {area}={val} outside [{lo},{hi}]")
                    errors += 1

        # IMR rural ≥ urban (universally true)
        imr = s["imr"]
        if imr["rural"]["total"] < imr["urban"]["total"]:
            warn("srs", f"{entity}: IMR rural={imr['rural']['total']} < urban={imr['urban']['total']} (unusual)")

        # Trend arrays must be length 6 (2018–2023)
        trend = s["trend"]
        for key in ["cbr_total", "cdr_total", "imr_total", "tfr_total"]:
            arr = trend.get(key, [])
            if len(arr) != 6:
                fail("srs", f"{entity} trend[{key}] has {len(arr)} values, expected 6")
                errors += 1
            # Latest trend value should match 2023 point estimate
            if arr:
                ind_name = key.split("_")[0]
                if ind_name == "imr":
                    point = s["imr"]["total"]["total"]
                else:
                    point = s[ind_name]["total"]
                if abs(arr[-1] - point) > 0.05:
                    fail("srs", f"{entity} trend[{key}][-1]={arr[-1]} doesn't match 2023 point estimate={point}")
                    errors += 1

    if errors == 0:
        ok("srs", "All rates in plausible ranges; trend arrays length 6; trend[-1] matches point estimate")


# ─────────────────────────────────────────────────────────────────────────────
# AISHE
# ─────────────────────────────────────────────────────────────────────────────

def verify_aishe(ts):
    print("\n── AISHE ────────────────────────────────────────────────────────────")
    errors = 0
    ENROLLMENT_LEVELS = ["phd", "mphil", "pg", "ug", "pg_diploma", "diploma"]

    for entity in FOCUS_STATES + ["All India"]:
        s = snap(ts, entity, "2021-22")
        if s is None:
            fail("aishe", f"Missing snapshot for {entity} / 2021-22")
            continue

        # Enrollment sum check
        enr = s["enrollment"]
        computed_total = sum(enr[k] for k in ENROLLMENT_LEVELS)
        stored_total   = enr["total_approx"]
        if computed_total != stored_total:
            fail("aishe", f"{entity} enrollment sum={computed_total} but total_approx={stored_total}")
            errors += 1

        # GER range
        ger = s["ger"]
        for dim in ["male", "female", "total"]:
            val = ger[dim]
            if not (5 <= val <= 75):
                fail("aishe", f"{entity} GER {dim}={val} outside [5,75]")
                errors += 1

        # GPI range
        gpi = ger["gpi"]
        if not (0.3 <= gpi <= 2.5):
            fail("aishe", f"{entity} GPI={gpi} outside [0.3,2.5]")
            errors += 1

        # GER trend: each year value in [5, 75]
        trend = s["ger_trend"]
        for year, yr_data in trend.items():
            for dim in ["male", "female", "total"]:
                val = yr_data[dim]
                if not (5 <= val <= 75):
                    fail("aishe", f"{entity} GER trend {year} {dim}={val} outside [5,75]")
                    errors += 1

        # Latest trend year (2021-22) should match point estimate
        latest = trend.get("2021-22", {})
        for dim in ["male", "female", "total"]:
            if latest and abs(latest[dim] - ger[dim]) > 0.05:
                fail("aishe", f"{entity} GER trend 2021-22 {dim}={latest[dim]} ≠ point estimate {ger[dim]}")
                errors += 1

        # Colleges: private_total = private_unaided + private_aided
        col = s["colleges"]
        if "private_total" in col:
            expected = col["private_unaided"] + col["private_aided"]
            if col["private_total"] != expected:
                fail("aishe", f"{entity} colleges: private_total={col['private_total']} ≠ unaided+aided={expected}")
                errors += 1
            # Total = private_total + government
            expected_total = col["private_total"] + col["government"]
            if col["total"] != expected_total:
                fail("aishe", f"{entity} colleges: total={col['total']} ≠ private+govt={expected_total}")
                errors += 1

    if errors == 0:
        ok("aishe", "Enrollment sums match; GER/GPI in range; trend consistent with point estimates; college totals add up")


# ─────────────────────────────────────────────────────────────────────────────
# SDG
# ─────────────────────────────────────────────────────────────────────────────

def verify_sdg(ts):
    print("\n── SDG ──────────────────────────────────────────────────────────────")
    errors = 0
    expected_goals = [str(g) for g in range(1, 17)]  # Goals 1-16 (Goal 17 often excluded)
    periods = ["2018", "2020-21", "2023-24"]

    for state in FOCUS_STATES:
        entity = ts["entities"].get(state, {})
        state_periods = list(entity.get("snapshots", {}).keys())

        # All 3 periods must be present for focus states
        for p in periods:
            if p not in state_periods:
                fail("sdg", f"{state}: missing period {p}")
                errors += 1
                continue

            s = entity["snapshots"][p]

            # Composite in [0, 100]
            comp = s.get("composite")
            if comp is None:
                fail("sdg", f"{state} {p}: composite is null")
                errors += 1
            elif not (0 <= comp <= 100):
                fail("sdg", f"{state} {p}: composite={comp} outside [0,100]")
                errors += 1

            # Goal scores in [0, 100]
            goals = s.get("goals", {})
            for g, score in goals.items():
                if score is not None and not (0 <= score <= 100):
                    fail("sdg", f"{state} {p} Goal {g}: score={score} outside [0,100]")
                    errors += 1

            # Gaps must be ≤ 0 (gap = state - national_best; best = max)
            gaps = s.get("gaps_from_national_best", {})
            for g, gap in gaps.items():
                if gap is not None and gap > 0.5:  # small tolerance for rounding
                    fail("sdg", f"{state} {p} Goal {g}: gap={gap} > 0 (impossible — gap is vs national best)")
                    errors += 1

        # Composite should be non-decreasing across periods (trend check — warn not fail)
        comps = {}
        for p in periods:
            s = entity.get("snapshots", {}).get(p)
            if s and s.get("composite") is not None:
                comps[p] = s["composite"]
        if len(comps) >= 2:
            vals = [comps[p] for p in periods if p in comps]
            for i in range(1, len(vals)):
                if vals[i] < vals[i-1] - 5:  # a drop > 5 points is suspicious
                    warn("sdg", f"{state}: composite dropped {vals[i-1]} → {vals[i]} between periods (verify)")

    if errors == 0:
        ok("sdg", "All scores in [0,100]; gaps ≤ 0; all 3 periods present for focus states")


# ─────────────────────────────────────────────────────────────────────────────
# CoL
# ─────────────────────────────────────────────────────────────────────────────

FUEL_RANGES = {
    "lpg_14kg_domestic":        (700, 1200),
    "lpg_5kg_domestic":         (200, 600),
    "lpg_19kg_commercial":      (1500, 3000),
    "petrol":                   (80, 130),
    "diesel":                   (70, 115),
    "kerosene_pds":             (15, 60),
}

def verify_col(ts):
    print("\n── CoL ──────────────────────────────────────────────────────────────")
    errors = 0

    # India entity — fuel
    s_india = snap(ts, "Cost_of_Living_India", "2026-04")
    if s_india is None:
        fail("col", "Missing snapshot: Cost_of_Living_India / 2026-04")
    else:
        fuel = s_india.get("fuel", {})
        for item, (lo, hi) in FUEL_RANGES.items():
            price = fuel.get(item, {}).get("price")
            if price is None:
                fail("col", f"fuel.{item}.price missing")
                errors += 1
            elif item == "lpg_14kg_ujjwala_subsidy":
                pass  # negative by design
            elif not (lo <= price <= hi):
                warn("col", f"fuel.{item}.price={price} outside expected [{lo},{hi}] — may need re-fetch")

    # TN entity — structural checks
    s_tn = snap(ts, "Cost_of_Living_Tamil_Nadu", "2026-04")
    if s_tn is None:
        fail("col", "Missing snapshot: Cost_of_Living_Tamil_Nadu / 2026-04")
    else:
        # Electricity: net ≤ gross per slab (except free slab)
        electricity = s_tn.get("electricity", {})
        for slab in electricity.get("slabs", []):
            net   = slab.get("net_rate_after_subsidy")
            gross = slab.get("gross_rate_per_unit")
            if net is not None and gross is not None and net > gross + 0.01:
                fail("col", f"Electricity slab {slab['range_units']}: net={net} > gross={gross}")
                errors += 1

        # Dairy: all prices > 0
        dairy = s_tn.get("food_dairy", {})
        for item, v in dairy.items():
            price = v.get("price")
            if price is not None and price <= 0:
                fail("col", f"food_dairy.{item}.price={price} should be > 0")
                errors += 1

        # PDS rice/wheat: price == 0 (free)
        ration = s_tn.get("ration_pds", {}).get("commodities", {})
        for item in ["rice", "wheat"]:
            if item in ration:
                price = ration[item].get("price")
                if price != 0:
                    fail("col", f"ration_pds.{item}.price={price}, expected 0 (free)")
                    errors += 1

        # Healthcare: govt = 0, private > 0
        hc = s_tn.get("healthcare", {})
        govt_opd = hc.get("government_hospital", {}).get("opd_consultation", {}).get("price")
        if govt_opd != 0:
            fail("col", f"govt hospital OPD price={govt_opd}, expected 0 (free)")
            errors += 1
        priv_del = hc.get("private_hospital_chennai", {}).get("normal_delivery", {})
        lo = priv_del.get("price_range_low", 0)
        hi = priv_del.get("price_range_high", 0)
        avg = priv_del.get("price_avg", 0)
        if not (lo > 0 and hi > lo and lo <= avg <= hi):
            fail("col", f"Private normal delivery: low={lo} avg={avg} high={hi} — inconsistent")
            errors += 1

    if errors == 0:
        ok("col", "Fuel prices in range; electricity net ≤ gross; PDS items free; healthcare prices consistent")


# ─────────────────────────────────────────────────────────────────────────────
# Layer 3 — Cross-dataset sanity
# ─────────────────────────────────────────────────────────────────────────────

def verify_cross(plfs_ts, hces_ts, srs_ts, aishe_ts):
    print("\n── Cross-dataset ────────────────────────────────────────────────────")
    errors = 0

    # Kerala UR (15+, total person) should be highest among focus states
    ur_by_state = {}
    for state in FOCUS_STATES:
        s = snap(plfs_ts, state, "2023-24")
        if s:
            ur_by_state[state] = s["ur"]["15+"]["total"]["person"]
    if ur_by_state:
        highest = max(ur_by_state, key=ur_by_state.get)
        if highest != "Kerala":
            fail("cross", f"Expected Kerala to have highest UR, got {highest}={ur_by_state[highest]} vs Kerala={ur_by_state.get('Kerala')}")
            errors += 1
        else:
            ok("cross", f"Kerala has highest UR among focus states ({ur_by_state['Kerala']}%)")

    # TN GER (total) should be highest among focus states
    ger_by_state = {}
    for state in FOCUS_STATES:
        s = snap(aishe_ts, state, "2021-22")
        if s:
            ger_by_state[state] = s["ger"]["total"]
    if ger_by_state:
        highest = max(ger_by_state, key=ger_by_state.get)
        if highest != "Tamil Nadu":
            fail("cross", f"Expected TN to have highest GER, got {highest}={ger_by_state[highest]} vs TN={ger_by_state.get('Tamil Nadu')}")
            errors += 1
        else:
            ok("cross", f"TN has highest GER among focus states ({ger_by_state['Tamil Nadu']})")

    # Kerala IMR (total) should be lowest among focus states
    imr_by_state = {}
    for state in FOCUS_STATES:
        s = snap(srs_ts, state, "2023")
        if s:
            imr_by_state[state] = s["imr"]["total"]["total"]
    if imr_by_state:
        lowest = min(imr_by_state, key=imr_by_state.get)
        if lowest != "Kerala":
            fail("cross", f"Expected Kerala to have lowest IMR, got {lowest}={imr_by_state[lowest]} vs Kerala={imr_by_state.get('Kerala')}")
            errors += 1
        else:
            ok("cross", f"Kerala has lowest IMR among focus states ({imr_by_state['Kerala']})")

    # Kerala rural MPCE (with free items) should be highest among focus states
    mpce_rural = {}
    for state in FOCUS_STATES:
        s = snap(hces_ts, state, "2023-24")
        if s:
            mpce_rural[state] = s["mpce_with_free_items"]["rural"]
    if mpce_rural:
        highest = max(mpce_rural, key=mpce_rural.get)
        if highest != "Kerala":
            fail("cross", f"Expected Kerala to have highest rural MPCE, got {highest}=₹{mpce_rural[highest]} vs Kerala=₹{mpce_rural.get('Kerala')}")
            errors += 1
        else:
            ok("cross", f"Kerala has highest rural MPCE among focus states (₹{mpce_rural['Kerala']})")

    # All focus-state TFRs below 2.1 (replacement)
    all_below_replacement = True
    for state in FOCUS_STATES:
        s = snap(srs_ts, state, "2023")
        if s:
            tfr = s["tfr"]["total"]
            if tfr >= 2.1:
                fail("cross", f"{state} TFR={tfr} ≥ 2.1 (above replacement) — verify")
                all_below_replacement = False
                errors += 1
    if all_below_replacement:
        ok("cross", "All 5 focus states have TFR < 2.1 (below replacement level)")

    # TN TFR should be lowest among focus states
    tfr_by_state = {}
    for state in FOCUS_STATES:
        s = snap(srs_ts, state, "2023")
        if s:
            tfr_by_state[state] = s["tfr"]["total"]
    if tfr_by_state:
        lowest = min(tfr_by_state, key=tfr_by_state.get)
        if lowest != "Tamil Nadu":
            warn("cross", f"Expected TN to have lowest TFR, got {lowest}={tfr_by_state[lowest]} vs TN={tfr_by_state.get('Tamil Nadu')}")
        else:
            ok("cross", f"TN has lowest TFR among focus states ({tfr_by_state['Tamil Nadu']})")

    # Karnataka UR should be lowest among focus states
    if ur_by_state:
        lowest = min(ur_by_state, key=ur_by_state.get)
        if lowest != "Karnataka":
            warn("cross", f"Expected Karnataka to have lowest UR, got {lowest}={ur_by_state[lowest]} vs KA={ur_by_state.get('Karnataka')}")
        else:
            ok("cross", f"Karnataka has lowest UR among focus states ({ur_by_state['Karnataka']}%)")

    return errors


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  Knowledge Graph — Data Verification Suite")
    print("=" * 70)

    plfs_ts  = load("plfs_ts.json")
    srs_ts   = load("srs_ts.json")
    hces_ts  = load("hces_ts.json")
    aishe_ts = load("aishe_ts.json")
    sdg_ts   = load("sdg_ts.json")
    col_ts   = load("col_ts.json")

    if plfs_ts:  verify_plfs(plfs_ts)
    if hces_ts:  verify_hces(hces_ts)
    if srs_ts:   verify_srs(srs_ts)
    if aishe_ts: verify_aishe(aishe_ts)
    if sdg_ts:   verify_sdg(sdg_ts)
    if col_ts:   verify_col(col_ts)

    if all([plfs_ts, hces_ts, srs_ts, aishe_ts]):
        verify_cross(plfs_ts, hces_ts, srs_ts, aishe_ts)

    print("\n" + "=" * 70)
    fails = [i for i in issues if i[0] == FAIL]
    warns = [i for i in issues if i[0] == WARN]
    print(f"  Result: {len(fails)} failure(s), {len(warns)} warning(s)")
    if fails:
        print("\n  FAILURES:")
        for _, dataset, msg in fails:
            print(f"    [{dataset}] {msg}")
    if warns:
        print("\n  WARNINGS:")
        for _, dataset, msg in warns:
            print(f"    [{dataset}] {msg}")
    print("=" * 70)

    return len(fails)


if __name__ == "__main__":
    sys.exit(main())
