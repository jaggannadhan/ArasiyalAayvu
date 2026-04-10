"""
name_utils.py — Candidate / MLA name normalisation and variant generation.

Used at ingest time to attach an alias_names[] array to every candidate /
MLA document so that lookups and searches are resilient to the many ways
Tamil political names are written across ECI, ADR, assembly websites, etc.

Typical variants covered
------------------------
Input form            → also generates
"K.ARJUNAN"          → K.Arjunan  K. Arjunan  K Arjunan  ARJUNAN K.  Arjunan K.  ARJUNAN K  Arjunan  ARJUNAN
"K.arjunan"          → same set
"Perarivalan V."     → V.Perarivalan  V. Perarivalan  V PERARIVALAN  PERARIVALAN V  Perarivalan  PERARIVALAN ...
"V SENTHILBALAJI"    → V.Senthilbalaji  V.SENTHILBALAJI  V. SENTHILBALAJI  SENTHILBALAJI V  ...
"Vanathi Srinivasan" → VANATHI SRINIVASAN  vanathi srinivasan  Srinivasan Vanathi  ...
"Amman K.Arjunan"    → AMMAN K.ARJUNAN  Amman K. Arjunan  Amman K Arjunan  ...
"""
from __future__ import annotations

import re
from typing import NamedTuple


# ---------------------------------------------------------------------------
# Internal parse result
# ---------------------------------------------------------------------------

class _ParsedName(NamedTuple):
    initials: list[str]    # e.g. ["K"] or ["V", "R"]
    words: list[str]       # main name words, in parse order
    extra_prefix: str      # any leading non-initial prefix word (e.g. "Amman" in "Amman K.Arjunan")


_IS_SINGLE_INITIAL = re.compile(r"^[A-Za-z]\.?$")
_FUSED_INITIAL     = re.compile(r"^((?:[A-Za-z]\.)+)([A-Za-z]\S*)$")


def _parse(raw: str) -> _ParsedName:
    """
    Split a raw name string into (initials, name_words, extra_prefix).

    Handles:
      "K.ARJUNAN"       → initials=["K"],      words=["ARJUNAN"],       prefix=""
      "Perarivalan V."  → initials=["V"],       words=["Perarivalan"],   prefix=""
      "V SENTHILBALAJI" → initials=["V"],       words=["SENTHILBALAJI"], prefix=""
      "Amman K.Arjunan" → initials=["K"],       words=["Arjunan"],       prefix="Amman"
      "Vanathi Srinivasan" → initials=[],       words=["Vanathi","Srinivasan"], prefix=""
    """
    tokens = raw.strip().replace(",", "").split()
    if not tokens:
        return _ParsedName([], [], "")

    initials: list[str] = []
    lo, hi = 0, len(tokens) - 1

    # Peel leading standalone initials: "V", "K.", "K.L."
    while lo <= hi and _IS_SINGLE_INITIAL.match(tokens[lo]):
        initials.append(tokens[lo][0].upper())
        lo += 1

    # Peel trailing standalone initials
    trailing: list[str] = []
    while hi >= lo and _IS_SINGLE_INITIAL.match(tokens[hi]):
        trailing.insert(0, tokens[hi][0].upper())
        hi -= 1
    initials.extend(trailing)

    # Process middle tokens — expand fused "K.Arjunan"
    extra_prefix = ""
    words: list[str] = []
    middle = tokens[lo: hi + 1]

    for i, tok in enumerate(middle):
        m = _FUSED_INITIAL.match(tok)
        if m:
            # Extract the embedded initials (they belong to the following name)
            for c in re.findall(r"[A-Za-z]", m.group(1)):
                initials.append(c.upper())
            words.append(m.group(2))
        else:
            # Only treat as a prefix word if the NEXT token contains a fused initial
            # (e.g. "Amman" in "Amman K.Arjunan") — otherwise it's a regular name word.
            next_is_fused = (i + 1 < len(middle) and bool(_FUSED_INITIAL.match(middle[i + 1])))
            if not words and i == 0 and next_is_fused:
                extra_prefix = tok
            else:
                words.append(tok)

    return _ParsedName(initials, words, extra_prefix)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def name_variants(raw: str) -> list[str]:
    """
    Return a sorted, deduplicated list of all common name variants for *raw*.
    Always includes the original string.
    """
    raw = (raw or "").strip().replace(",", "")
    if not raw:
        return []

    seen: set[str] = {raw}

    parsed = _parse(raw)
    initials = parsed.initials
    words    = parsed.words
    prefix   = parsed.extra_prefix

    if not words:
        return sorted(seen)

    # ── Name-part forms ───────────────────────────────────────────────────────
    name_title = " ".join(w.capitalize() for w in words)
    name_upper = " ".join(w.upper()      for w in words)
    name_lower = " ".join(w.lower()      for w in words)
    name_forms = [name_title, name_upper, name_lower]

    # ── With prefix (e.g. "Amman") ────────────────────────────────────────────
    pfx = (prefix + " ") if prefix else ""

    if initials:
        dot_init   = ".".join(initials) + "."        # "K."   / "K.L."
        space_init = " ".join(initials)               # "K"    / "K L"
        fused_init = "".join(initials)                # "K"    / "KL"

        for name in name_forms:
            name_nsp = name.replace(" ", "")          # "Arjunan" / "ARJUNAN"

            # Initials-first, fused (K.Arjunan, K.ARJUNAN, k.arjunan)
            seen.add(pfx + dot_init + name_nsp)
            # Initials-first, space after dot (K. Arjunan, K. ARJUNAN)
            seen.add(pfx + dot_init + " " + name)
            # Initials-first, no dot (K Arjunan, K ARJUNAN)
            seen.add(pfx + space_init + " " + name)

        # Name-first variants (use only UPPER and Title to keep list manageable)
        for name in [name_title, name_upper]:
            # ARJUNAN K. / Arjunan K.
            seen.add(name + " " + dot_init)
            # ARJUNAN K / Arjunan K
            seen.add(name + " " + space_init)

        # Name-only (drop initials entirely)
        seen.update([name_title, name_upper, name_lower])

        # Compact all-letters (KARJUNAN — seen in some old ECI records)
        for name in [name_title, name_upper]:
            seen.add(pfx + fused_init + name.replace(" ", ""))

    else:
        # No initials: case variants + reversed word order
        seen.update([name_title, name_upper, name_lower])
        if len(words) > 1:
            rev_title = " ".join(w.capitalize() for w in reversed(words))
            rev_upper = " ".join(w.upper()      for w in reversed(words))
            seen.update([rev_title, rev_upper])

    return sorted(seen)


def canonical_name(raw: str) -> str:
    """
    Return the single canonical display form of *raw*:
      trailing/leading initials → front, fused, title-cased name.
    Mirrors the TypeScript normalizeName() in web/src/lib/formatters.ts.
    """
    parsed = _parse(raw)
    initials = parsed.initials
    words    = parsed.words
    prefix   = parsed.extra_prefix

    pfx = (prefix.capitalize() + " ") if prefix else ""

    name_title = " ".join(w.capitalize() for w in words) if words else ""
    init_str   = ".".join(initials) + "." if initials else ""

    if init_str and name_title:
        return pfx + init_str + name_title.replace(" ", "")
    if init_str:
        return pfx + init_str.rstrip(".")
    return pfx + name_title
