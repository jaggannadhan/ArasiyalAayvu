#!/usr/bin/env python3
"""
scrapers/gemini_constituency_pincode_ingest.py
===============================================
Queries Gemini AI for each TN assembly constituency to get authoritative:
  1. Pincodes in the constituency
  2. Lok Sabha constituency name
  3. Taluks
  4. Key localities

Uses Gemini REST API with Google Search grounding (same pipeline as AI Overview).

Free tier: ~1,500 req/day. Search grounding needs billing enabled on the key.
208 TN constituencies at 2s spacing = ~7 minutes total.

Setup:
  export GEMINI_API_KEY="your-key"   # from https://aistudio.google.com/apikey

Usage:
  # Test a single constituency
  .venv/bin/python scrapers/gemini_constituency_pincode_ingest.py --slug kolathur

  # Full run (saves after each one, safe to interrupt)
  .venv/bin/python scrapers/gemini_constituency_pincode_ingest.py

  # Resume after an interruption
  .venv/bin/python scrapers/gemini_constituency_pincode_ingest.py --resume

  # Patch constituency-pincodes.json after run
  .venv/bin/python scrapers/gemini_constituency_pincode_ingest.py --apply
"""

import argparse
import json
import os
import re
import time
from pathlib import Path

import httpx

ROOT                  = Path(__file__).parent.parent
CONSTITUENCY_MAP_FILE = ROOT / "web/src/lib/constituency-map.json"
PINCODE_MAP_FILE      = ROOT / "web/src/lib/constituency-pincodes.json"
OUTPUT_FILE           = ROOT / "data/processed/gemini_constituency_details.json"

# Try these models in order until one works (2.5-flash has highest free quota)
GEMINI_MODELS = [
    "gemini-2.5-flash",
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite",
    "gemini-flash-latest",
]
GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"


# ── Prompt ────────────────────────────────────────────────────────────────────
def build_prompt(name: str, district: str) -> str:
    return (
        f"Give me the following details of this constituency - {name} "
        f"(in {district.title()} district, Tamil Nadu)\n"
        "1. Pin codes that fall in the constituency\n"
        "2. Lok Sabha constituency name\n"
        "3. Taluks\n"
        "4. Key localities"
    )


BROWSER_SYSTEM_PROMPT = (
    "I will give you Tamil Nadu Assembly Constituency names one by one. "
    "For each constituency I give you, respond with exactly these 4 points:\n"
    "1. Pin codes that fall in the constituency\n"
    "2. Lok Sabha constituency name\n"
    "3. Taluks\n"
    "4. Key localities\n\n"
    "Rules:\n"
    "- Use ONLY verified and trustworthy sources: India Post (indiapost.gov.in), "
    "Election Commission of India (eci.gov.in), Tamil Nadu government portals, "
    "or official census/delimitation documents.\n"
    "- Include the source URL for each data point.\n"
    "- For pincodes, list each 6-digit code on a separate line with a dash.\n"
    "- Do not add commentary — just the 4 numbered points with sources.\n\n"
    "Ready? I will now send you constituency names one at a time."
)


def build_google_prompt(name: str, district: str) -> str:
    """Short per-constituency message — system prompt was already sent at init."""
    return f"{name} (in {district.title()} district, Tamil Nadu)"


# ── API mode ──────────────────────────────────────────────────────────────────
def call_gemini_api(prompt: str, api_key: str, use_grounding: bool = True) -> str:
    payload: dict = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 1024},
    }
    if use_grounding:
        payload["tools"] = [{"google_search": {}}]

    keys = api_key if isinstance(api_key, list) else [api_key]

    last_err = None
    for model in GEMINI_MODELS:
        url = GEMINI_BASE.format(model=model)
        for key in keys:
            try:
                resp = httpx.post(
                    url,
                    json=payload,
                    timeout=45,
                    headers={"Content-Type": "application/json", "X-goog-api-key": key},
                )
                if resp.status_code == 429:
                    data = resp.json()
                    msg = data.get("error", {}).get("message", "quota exceeded")
                    print(f"    [{model}|...{key[-6:]}] 429 — trying next key")
                    last_err = f"429 on {model}"
                    time.sleep(1)
                    continue
                resp.raise_for_status()
                data = resp.json()
                candidate = data["candidates"][0]
                finish = candidate.get("finishReason", "STOP")
                if finish not in ("STOP", "MAX_TOKENS"):
                    raise ValueError(f"Gemini finishReason={finish} — no text (RECITATION/SAFETY block)")
                return candidate["content"]["parts"][0]["text"]
            except (KeyError, IndexError):
                raise ValueError(f"Unexpected response: {json.dumps(data)[:300]}")
            except httpx.HTTPStatusError as e:
                print(f"    [{model}|...{key[-6:]}] HTTP {e.response.status_code}")
                last_err = str(e)
                time.sleep(2)

    # All keys × all models failed — if grounding was on, retry without it
    if use_grounding:
        print("    All keys hit quota with grounding — retrying without grounding")
        return call_gemini_api(prompt, api_key, use_grounding=False)

    raise RuntimeError(f"All keys + models failed. Last error: {last_err}")


# ── Browser mode (Playwright → gemini.google.com) ────────────────────────────
# One persistent browser + one Gemini tab, reused across ALL constituencies.
# Uses the user's existing Chrome profile so they're already logged in.
_pw_ctx = None   # persistent browser context
_gemini_page = None
_playwright_inst = None

CHROME_BASE = "/Users/jv/Library/Application Support/Google/Chrome"

def _find_free_profile() -> str:
    """
    Return a Chrome profile directory that is NOT currently locked by a running Chrome.
    Tries Default, Profile 1, Profile 2, Profile 3 in order.
    Falls back to copying Default into a temp directory.
    """
    import shutil, tempfile
    from pathlib import Path as _Path

    candidates = ["Default", "Profile 1", "Profile 2", "Profile 3"]
    for name in candidates:
        profile_dir = f"{CHROME_BASE}/{name}"
        lock_file = f"{profile_dir}/SingletonLock"
        if _Path(profile_dir).exists() and not _Path(lock_file).exists():
            print(f"  [browser] Using Chrome profile: {name}")
            return profile_dir

    # All profiles locked — copy Default to a temp dir
    print("  [browser] All Chrome profiles locked — copying Default to temp dir…")
    src = f"{CHROME_BASE}/Default"
    tmp = tempfile.mkdtemp(prefix="chrome_profile_")
    dst = f"{tmp}/Default"
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns(
        "*.lock", "SingletonLock", "SingletonCookie", "*.tmp",
        "BrowserMetrics*", "chrome_debug.log",
    ))
    return dst


def _init_browser():
    """Open Chrome once with a free profile and navigate to Gemini."""
    global _pw_ctx, _gemini_page, _playwright_inst
    from playwright.sync_api import sync_playwright as _sync_pw

    profile_dir = _find_free_profile()
    print("  [browser] Launching Chrome… (window will open)")
    _playwright_inst = _sync_pw().__enter__()

    _pw_ctx = _playwright_inst.chromium.launch_persistent_context(
        user_data_dir=profile_dir,
        channel="chrome",
        headless=False,
        slow_mo=150,
        args=["--disable-blink-features=AutomationControlled"],
        ignore_default_args=["--enable-automation"],
    )

    _gemini_page = _pw_ctx.new_page()
    _gemini_page.goto("https://gemini.google.com/app", wait_until="domcontentloaded", timeout=30000)
    # Wait for chat input to appear (confirms Gemini is loaded and logged in)
    INPUT_SELS = [
        'div[contenteditable="true"]',
        'rich-textarea',
        'textarea[placeholder]',
    ]
    loaded = False
    for sel in INPUT_SELS:
        try:
            _gemini_page.wait_for_selector(sel, timeout=15000)
            loaded = True
            break
        except Exception:
            continue
    if not loaded:
        print("  [browser] WARNING: Chat input not found — you may need to log in manually in the Chrome window.")
        input("  Press Enter once you are logged in and see the Gemini chat interface…")

    # Send system prompt ONCE to set up the conversation context
    print("  [browser] Sending system prompt…")
    _send_message(_gemini_page, BROWSER_SYSTEM_PROMPT)
    _gemini_page.wait_for_timeout(1000)
    print("  [browser] System prompt sent. Ready — tab stays open for all constituencies.")


def _close_browser():
    global _pw_ctx, _gemini_page, _playwright_inst
    try:
        if _pw_ctx:
            _pw_ctx.close()
        if _playwright_inst:
            _playwright_inst.__exit__(None, None, None)
    except Exception:
        pass
    _pw_ctx = _gemini_page = _playwright_inst = None


def _send_message(page, text: str) -> str:
    """Type text into Gemini chat, submit, wait for response, return response text."""
    INPUT_SELS = [
        'div[contenteditable="true"][class*="ql-editor"]',
        'div[contenteditable="true"]',
        'rich-textarea div[contenteditable]',
        'textarea',
    ]
    input_el = None
    for sel in INPUT_SELS:
        try:
            page.wait_for_selector(sel, timeout=6000)
            input_el = page.locator(sel).first
            break
        except Exception:
            continue

    if not input_el:
        raise ValueError("Could not find Gemini chat input")

    # Count existing model-responses BEFORE sending so we can wait for a new one
    RESPONSE_SELS = [
        "model-response",
        ".model-response",
        'div[data-response-index]',
        'message-content',
    ]
    before_count = 0
    for sel in RESPONSE_SELS:
        try:
            els = page.query_selector_all(sel)
            if els:
                before_count = len(els)
                break
        except Exception:
            continue

    input_el.click()
    page.wait_for_timeout(300)
    input_el.fill("")
    page.wait_for_timeout(200)

    # Paste via clipboard — avoids \n being interpreted as form-submit
    import pyperclip as _clip
    try:
        _clip.copy(text)
        page.keyboard.press("Meta+v")   # macOS paste
    except Exception:
        # Fallback: type line by line using Shift+Enter for newlines
        lines = text.split("\n")
        for i, line in enumerate(lines):
            input_el.type(line, delay=10)
            if i < len(lines) - 1:
                page.keyboard.press("Shift+Enter")

    page.wait_for_timeout(600)
    page.keyboard.press("Enter")  # submit

    # Wait for a NEW model-response to appear (count increases)
    deadline = 10_000  # 10 s to see new response appear
    waited = 0
    new_response_sel = None
    while waited < deadline:
        for sel in RESPONSE_SELS:
            try:
                els = page.query_selector_all(sel)
                if els and len(els) > before_count:
                    new_response_sel = sel
                    break
            except Exception:
                pass
        if new_response_sel:
            break
        page.wait_for_timeout(300)
        waited += 300

    # Wait for streaming to finish (Stop button disappears)
    try:
        page.wait_for_selector('button[aria-label*="Stop"]', timeout=8000)
        page.wait_for_selector('button[aria-label*="Stop"]', state="hidden", timeout=60000)
    except Exception:
        page.wait_for_timeout(8000)

    # Extract the LAST model response (guaranteed new if count increased)
    response_text = ""
    for sel in RESPONSE_SELS:
        try:
            els = page.query_selector_all(sel)
            if els:
                response_text = els[-1].inner_text().strip()
                if len(response_text) > 30:
                    break
        except Exception:
            continue

    return response_text


def call_gemini_browser(prompt: str, headless: bool = False) -> str:
    """
    Sends the constituency name to the already-open Gemini chat.
    The system prompt was sent once at init; this just adds the next constituency.
    """
    global _pw_ctx, _gemini_page

    if _pw_ctx is None or not _pw_ctx.browser.is_connected():
        _init_browser()

    response_text = _send_message(_gemini_page, prompt)

    if not response_text or len(response_text) < 30:
        raise ValueError("Empty response from Gemini")

    return response_text


# ── Parser ────────────────────────────────────────────────────────────────────
def parse_response(text: str) -> dict:
    result: dict = {"pincodes": [], "lok_sabha": None, "taluks": [], "localities": [], "sources": []}

    # 0. Source URLs
    result["sources"] = re.findall(r'https?://[^\s\)\]\"\']+', text)

    # 1. Pincodes — all 6-digit TN codes (600xxx – 643xxx)
    pincodes = re.findall(r'\b6(?:0[0-9]|[123][0-9]|4[0-3])\d{3}\b', text)
    result["pincodes"] = sorted(set(pincodes))

    # Split into numbered sections
    sections = _split_sections(text)

    # 2. Lok Sabha name — try bullet first, then extract from prose sentence
    if "2" in sections:
        sec2 = sections["2"]
        lines = _bullet_lines(sec2, max_len=80)
        # Filter out lines that just echo the heading
        lines = [l for l in lines if "lok sabha" not in l.lower() and "constituency" not in l.lower()]
        if lines:
            result["lok_sabha"] = lines[0].strip("*_ ")
        else:
            # Prose format: "is part of the X Lok Sabha constituency"
            m = re.search(
                r'(?:part of|falls? (?:under|in|within)|included? in|under)\s+(?:the\s+)?'
                r'([A-Z][A-Za-z\s\(\)]+?)\s+Lok Sabha',
                sec2
            )
            if not m:
                # Direct "Lok Sabha constituency: X" or "X (Lok Sabha)"
                m = re.search(r'([A-Z][A-Za-z\s\(\)]{3,40})\s+(?:Lok Sabha|parliamentary)', sec2)
            if m:
                candidate = m.group(1).strip().rstrip(".,").strip()
                # Reject phone-number-like matches or single characters
                if len(candidate) > 3 and not re.match(r'^[\+\d\s\-]+$', candidate):
                    result["lok_sabha"] = candidate

    # 3. Taluks — Gemini sometimes appends long descriptions; strip at first paren
    if "3" in sections:
        raw_taluks = _bullet_lines(sections["3"], max_len=300)
        cleaned = []
        for t in raw_taluks:
            t = re.split(r'\s*[\(]', t)[0].strip().rstrip(".,")
            # Remove trailing " Taluk" / " taluk" suffix (redundant)
            t = re.sub(r'\s+[Tt]aluk$', '', t).strip()
            if t and len(t) < 60:
                cleaned.append(t)
        result["taluks"] = cleaned[:8]

    # 4. Key localities
    if "4" in sections:
        result["localities"] = _parse_localities(sections["4"])[:20]

    return result


_SECTION_HEADINGS = {
    "1": ["pin code", "pincode", "postal"],
    "2": ["lok sabha", "parliament", "mp constituency"],
    "3": ["taluk", "block"],
    "4": ["key localit", "key area", "important area", "notable area"],
}

def _split_sections(text: str) -> dict[str, str]:
    """
    Split Gemini response into sections keyed as "1"/"2"/"3"/"4".
    Handles:
      - Numbered: "1. Pin codes..." or "1. **Pin codes**"
      - Plain heading: "Pin codes that fall in the constituency"
    """
    # Strip "Gemini said" prefix that browser scraping adds
    text = re.sub(r'^Gemini said\s*\n', '', text, flags=re.IGNORECASE).strip()

    sections: dict[str, str] = {}
    current_key: str | None = None
    current_lines: list[str] = []

    def _flush():
        if current_key:
            sections[current_key] = "\n".join(current_lines).strip()

    for line in text.split("\n"):
        stripped = line.strip().lower()

        # Try numbered heading: "1." or "1)"
        m = re.match(r'^\s*(\d+)[.)]\s*(?:\*{0,2}[^*\n]*\*{0,2})?\s*$', line)
        if m:
            _flush(); current_key = m.group(1); current_lines = []
            continue

        # Try plain keyword heading
        matched_key = None
        for key, keywords in _SECTION_HEADINGS.items():
            if any(kw in stripped for kw in keywords) and len(stripped) < 60:
                matched_key = key
                break

        if matched_key:
            _flush(); current_key = matched_key; current_lines = []
            continue

        if current_key:
            current_lines.append(line)

    _flush()
    return sections


def _bullet_lines(block: str, max_len: int = 80) -> list[str]:
    out = []
    for line in block.split("\n"):
        line = line.strip().lstrip("•*·-–— \t")
        line = re.sub(r'^[0-9a-zA-Z]{1,2}[.)]\s*', '', line)
        # Skip source citation lines and bare URL lines
        if re.match(r'^[Ss]ource[s]?:', line) or re.match(r'^https?://', line):
            continue
        if line and len(line) < max_len:
            out.append(line)
    return out


def _parse_localities(block: str) -> list[str]:
    """Handle both bullet-per-line and comma-separated locality lists."""
    # Filter source lines first
    clean_lines = []
    for line in block.split("\n"):
        line = line.strip()
        if not line or re.match(r'^[Ss]ource', line) or re.match(r'^https?://', line):
            continue
        clean_lines.append(line)

    # If lines are short (one place per line), use as-is
    if all(len(l) < 50 for l in clean_lines if l):
        return _bullet_lines("\n".join(clean_lines), max_len=80)

    # Otherwise split comma-separated text
    combined = " ".join(clean_lines)
    places = [p.strip().lstrip("•*·-–— ").rstrip(".,") for p in re.split(r',\s+|\band\b', combined)]
    return [p for p in places if p and len(p) < 60][:20]


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode",    choices=["api", "browser"], default="api")
    parser.add_argument("--limit",   type=int,  default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resume",  action="store_true", help="Skip already-done slugs")
    parser.add_argument("--slug",    help="Process only this slug")
    parser.add_argument("--apply",   action="store_true", help="Patch constituency-pincodes.json from saved results")
    parser.add_argument("--show-browser", action="store_true", help="Run browser in non-headless mode (for debugging)")
    args = parser.parse_args()

    if args.apply:
        apply_results()
        return

    # Support multiple keys via comma-separated GEMINI_API_KEY or GEMINI_API_KEYS
    raw_keys = os.environ.get("GEMINI_API_KEYS", os.environ.get("GEMINI_API_KEY", ""))
    api_key = [k.strip() for k in raw_keys.split(",") if k.strip()]
    if args.mode == "api" and not api_key and not args.dry_run:
        raise SystemExit("ERROR: set GEMINI_API_KEY env variable (https://aistudio.google.com/apikey)")

    with open(CONSTITUENCY_MAP_FILE) as f:
        c_map: dict = json.load(f)
    with open(PINCODE_MAP_FILE) as f:
        pincode_map: dict = json.load(f)

    existing: dict = {}
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE) as f:
            existing = json.load(f)

    slugs = list(pincode_map.keys())
    if args.slug:
        slugs = [args.slug]
    elif args.resume:
        slugs = [s for s in slugs if s not in existing or "error" in existing.get(s, {})]
    if args.limit:
        slugs = slugs[:args.limit]

    print(f"Mode: {args.mode} | Processing {len(slugs)} / {len(pincode_map)} constituencies\n")

    for i, slug in enumerate(slugs):
        info     = c_map.get(slug, {})
        name     = info.get("name", slug.replace("_", " ").upper())
        district = pincode_map.get(slug, {}).get("district", "Tamil Nadu")
        prompt   = build_prompt(name, district)

        print(f"[{i+1:03d}/{len(slugs)}] {slug}  ({name})")

        if args.dry_run:
            print(f"  PROMPT: {prompt[:120].replace(chr(10), ' | ')}\n")
            continue

        try:
            if args.mode == "api":
                raw = call_gemini_api(prompt, api_key)
            else:
                g_prompt = build_google_prompt(name, district)
                raw = call_gemini_browser(g_prompt, headless=not args.show_browser)

            parsed = parse_response(raw)
            entry = {
                "name": name, "slug": slug, "district": district,
                "pincodes":   parsed["pincodes"],
                "lok_sabha":  parsed["lok_sabha"],
                "taluks":     parsed["taluks"],
                "localities": parsed["localities"],
                "sources":    parsed["sources"],
                "raw":        raw,
            }
            existing[slug] = entry

            print(f"  pincodes : {parsed['pincodes'] or '(none found)'}")
            print(f"  lok_sabha: {parsed['lok_sabha'] or '(not found)'}")
            print(f"  taluks   : {parsed['taluks']}")
            if not parsed['pincodes']:
                print(f"  RAW RESPONSE:\n{raw[:600]}")
            print()

            OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(OUTPUT_FILE, "w") as f:
                json.dump(existing, f, indent=2, ensure_ascii=False)

            # Polite delay: 2s for API, 4s for browser
            time.sleep(4 if args.mode == "browser" else 2)

        except Exception as e:
            print(f"  ERROR: {e}\n")
            existing[slug] = {"error": str(e), "name": name, "slug": slug}
            time.sleep(5)

    if args.mode == "browser":
        _close_browser()

    if not args.dry_run:
        print(f"Done. Results saved to {OUTPUT_FILE}")
        if not args.slug:
            print("Run with --apply to patch constituency-pincodes.json")


# ── Apply ─────────────────────────────────────────────────────────────────────
def apply_results():
    if not OUTPUT_FILE.exists():
        raise SystemExit(f"{OUTPUT_FILE} not found — run the scraper first.")

    with open(OUTPUT_FILE) as f:
        gemini_data: dict = json.load(f)
    with open(PINCODE_MAP_FILE) as f:
        pincode_map: dict = json.load(f)

    changed = 0
    for slug, data in gemini_data.items():
        if "error" in data or not data.get("pincodes"):
            continue
        if slug not in pincode_map:
            continue

        gemini_pins   = set(data["pincodes"])
        current_exact = set(pincode_map[slug]["pincodes"])
        current_ambig = set(pincode_map[slug].get("ambiguous_pincodes", []))
        current_all   = current_exact | current_ambig

        added   = gemini_pins - current_all
        missing = current_exact - gemini_pins

        if added:
            print(f"  {slug}: +{sorted(added)}")
            pincode_map[slug]["pincodes"] = sorted(current_exact | added)
            changed += 1
        if missing:
            print(f"  {slug}: ⚠ in our data but NOT in Gemini: {sorted(missing)}")

    with open(PINCODE_MAP_FILE, "w") as f:
        json.dump(pincode_map, f, indent=2, ensure_ascii=False)
    print(f"\nPatched {changed} constituencies.")


if __name__ == "__main__":
    main()
