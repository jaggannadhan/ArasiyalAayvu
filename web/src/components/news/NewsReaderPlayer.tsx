"use client";

import { useRef, useEffect, useState, useCallback } from "react";
import { useLanguage } from "@/lib/LanguageContext";

/* ── Types ── */
interface MetaArticle {
  title: string;
  summary: string;
  category: string;
  sdg_alignment: string[];
  source_name: string;
  source_url: string;
  audio_url: string;
  duration: number;
}

interface ReaderMeta {
  generated_at: string;
  lang: string;
  intro: { audio_url: string; duration: number };
  outro: { audio_url: string; duration: number };
  articles: MetaArticle[];
}

const GCS_BASE = "https://storage.googleapis.com/naatunadappu-media/news-reader";
const ANIM_MS = 1200;
const TRANSITION_MS = 2400;
const COUNTDOWN_SEC = 4;
const BG_VOLUME = 0.08; // low-volume bed under the whole broadcast

type TransitionType = "page-flip" | "card-shuffle";
type Phase = "loading" | "countdown" | "ready" | "intro" | "intro-pause" | "article" | "transition" | "outro" | "done";

async function preloadAudio(url: string): Promise<string> {
  const resp = await fetch(url);
  const blob = await resp.blob();
  return URL.createObjectURL(blob);
}

function formatTime(s: number): string {
  const m = Math.floor(s / 60);
  const sec = Math.floor(s % 60);
  return `${m}:${sec.toString().padStart(2, "0")}`;
}

export function NewsReaderPlayer() {
  const { lang, toggleLang } = useLanguage();
  const isTA = lang === "ta";
  const containerRef = useRef<HTMLDivElement>(null);
  const audioRef = useRef<HTMLAudioElement>(null);
  const bgAudioRef = useRef<HTMLAudioElement | null>(null);
  const vizCanvasRef = useRef<HTMLCanvasElement>(null);
  const audioCtxRef = useRef<AudioContext | null>(null);
  const analyserRef = useRef<AnalyserNode | null>(null);
  const fadeIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const [meta, setMeta] = useState<ReaderMeta | null>(null);
  const [phase, setPhase] = useState<Phase>("loading");
  const [currentArticle, setCurrentArticle] = useState(0);
  const [countdown, setCountdown] = useState(COUNTDOWN_SEC);
  const [transition, setTransition] = useState<TransitionType | null>(null);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [volume, setVolume] = useState(1);
  const [muted, setMuted] = useState(false);
  const [isPaused, setIsPaused] = useState(false);
  const [elapsed, setElapsed] = useState(0);
  const [atLiveEdge, setAtLiveEdge] = useState(true);

  const volumeRef = useRef(1);
  const mutedRef = useRef(false);
  const animFrameRef = useRef<number>(0);
  const transitionIdx = useRef(0);
  const elapsedRef = useRef(0);
  const liveElapsedRef = useRef(0);
  const elapsedTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const sequencerIdRef = useRef(0);
  const pendingResumeRef = useRef<{ phase: Phase; articleIdx: number } | null>(null);

  const preloadedRef = useRef<Map<string, string>>(new Map());

  const articles = meta?.articles ?? [];

  interface Segment { type: "intro" | "pause" | "transition" | "article" | "outro" | "gap"; index: number; url?: string; startAt: number; duration: number; }
  const timeline = useRef<Segment[]>([]);

  const totalDuration = (() => {
    if (!meta) return 0;
    const segs: Segment[] = [];
    let t = 0;
    segs.push({ type: "intro", index: -1, url: meta.intro.audio_url, startAt: t, duration: meta.intro.duration });
    t += meta.intro.duration;
    segs.push({ type: "pause", index: -1, startAt: t, duration: 2 });
    t += 2;
    for (let i = 0; i < meta.articles.length; i++) {
      segs.push({ type: "transition", index: i, startAt: t, duration: TRANSITION_MS / 1000 });
      t += TRANSITION_MS / 1000;
      segs.push({ type: "article", index: i, url: meta.articles[i].audio_url, startAt: t, duration: meta.articles[i].duration });
      t += meta.articles[i].duration;
      segs.push({ type: "gap", index: i, startAt: t, duration: 0.3 });
      t += 0.3;
    }
    segs.push({ type: "outro", index: -1, url: meta.outro.audio_url, startAt: t, duration: meta.outro.duration });
    t += meta.outro.duration;
    timeline.current = segs;
    return t;
  })();

  /* ── Elapsed timer ── */
  const startElapsedTimer = useCallback(() => {
    if (elapsedTimerRef.current) return;
    elapsedTimerRef.current = setInterval(() => {
      elapsedRef.current += 0.2;
      liveElapsedRef.current = Math.max(liveElapsedRef.current, elapsedRef.current);
      setElapsed(elapsedRef.current);
      setAtLiveEdge(Math.abs(elapsedRef.current - liveElapsedRef.current) < 0.5);
    }, 200);
  }, []);

  const stopElapsedTimer = useCallback(() => {
    if (elapsedTimerRef.current) { clearInterval(elapsedTimerRef.current); elapsedTimerRef.current = null; }
  }, []);

  /* ── Fullscreen ── */
  useEffect(() => {
    const fn = () => setIsFullscreen(!!document.fullscreenElement);
    document.addEventListener("fullscreenchange", fn);
    return () => document.removeEventListener("fullscreenchange", fn);
  }, []);

  const toggleFullscreen = () => {
    const el = containerRef.current;
    if (!el) return;
    if (document.fullscreenElement) {
      document.exitFullscreen();
    } else {
      el.requestFullscreen();
    }
  };

  /* ── Fetch metadata (refetches when lang flips) ──
   * If a broadcast is in flight, remember which article was playing so we can
   * resume in the new language at the same article. The resume effect below
   * picks this up once new meta lands.
   */
  useEffect(() => {
    let cancelled = false;

    const wasMidBroadcast = phase === "intro" || phase === "intro-pause" ||
      phase === "article" || phase === "transition" || phase === "outro";
    pendingResumeRef.current = wasMidBroadcast
      ? { phase, articleIdx: currentArticle }
      : null;

    // Hard stop the previous-language broadcast — don't let stale audio bleed under the new UI
    sequencerIdRef.current++;
    audioRef.current?.pause();
    if (audioRef.current) audioRef.current.src = "";
    stopFade();
    stopElapsedTimer();
    setIsPaused(false);

    setPhase("loading");
    setMeta(null);
    fetch(`${GCS_BASE}/latest_meta_${lang}.json?t=${Date.now()}`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((data: ReaderMeta) => {
        if (cancelled) return;
        setMeta(data);
        if (!pendingResumeRef.current) {
          // Cold start (no broadcast was running) — show the countdown intro
          setCountdown(COUNTDOWN_SEC);
          setCurrentArticle(0);
          elapsedRef.current = 0; liveElapsedRef.current = 0; setElapsed(0);
          transitionIdx.current = 0;
          setPhase("countdown");
        }
        // Otherwise the resume effect (below) jumps to the matching article in the new language
      })
      .catch((err) => {
        if (!cancelled) console.error("Failed to load metadata:", err);
      });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [lang]);

  /* ── Preload priority clips during countdown ── */
  useEffect(() => {
    if (!meta || phase !== "countdown") return;
    [meta.intro.audio_url, meta.outro.audio_url, meta.articles[0]?.audio_url].filter(Boolean).forEach(async (url) => {
      if (preloadedRef.current.has(url)) return;
      try { preloadedRef.current.set(url, await preloadAudio(url)); } catch { /* swallow */ }
    });
  }, [meta, phase]);

  const preloadNext = useCallback((idx: number) => {
    if (!meta || idx >= meta.articles.length) return;
    const url = meta.articles[idx].audio_url;
    if (!preloadedRef.current.has(url)) preloadAudio(url).then(b => preloadedRef.current.set(url, b)).catch(() => {});
  }, [meta]);

  /* ── Background music ── */
  useEffect(() => {
    const a = new Audio(); a.preload = "none"; a.loop = true; a.volume = 0;
    bgAudioRef.current = a;
    return () => { a.pause(); a.src = ""; };
  }, []);

  const stopFade = useCallback(() => {
    if (fadeIntervalRef.current) { clearInterval(fadeIntervalRef.current); fadeIntervalRef.current = null; }
  }, []);

  const fadeInBg = useCallback((targetVol: number, ms: number) => {
    const a = bgAudioRef.current; if (!a) return;
    if (!a.src) { a.src = "/news-bg.mp3"; a.load(); }
    stopFade();
    const start = a.volume;
    a.play().catch(() => {});
    const steps = 20, step = (targetVol - start) / steps; let n = 0;
    fadeIntervalRef.current = setInterval(() => {
      n++;
      a.volume = Math.max(0, Math.min(targetVol, start + step * n));
      if (n >= steps) stopFade();
    }, ms / steps);
  }, [stopFade]);

  const fadeOutBg = useCallback((ms: number) => {
    const a = bgAudioRef.current; if (!a || a.paused) return;
    stopFade();
    const start = a.volume;
    if (start <= 0) { a.pause(); return; }
    const steps = 20, step = start / steps; let n = 0;
    fadeIntervalRef.current = setInterval(() => {
      n++;
      a.volume = Math.max(0, start - step * n);
      if (n >= steps) { stopFade(); a.pause(); a.volume = 0; }
    }, ms / steps);
  }, [stopFade]);

  /* ── Sync volume ── */
  useEffect(() => {
    volumeRef.current = volume; mutedRef.current = muted;
    const a = audioRef.current; if (a) { a.volume = volume; a.muted = muted; }
  }, [volume, muted]);

  /* ── WebAudio analyser (one-time wiring, lazy on first play) ── */
  const ensureAnalyser = useCallback(() => {
    if (analyserRef.current) return;
    const audio = audioRef.current; if (!audio) return;
    try {
      const Ctor = window.AudioContext || (window as unknown as { webkitAudioContext: typeof AudioContext }).webkitAudioContext;
      const ctx = new Ctor();
      audioCtxRef.current = ctx;
      const source = ctx.createMediaElementSource(audio);
      const analyser = ctx.createAnalyser();
      analyser.fftSize = 64;
      analyser.smoothingTimeConstant = 0.75;
      source.connect(analyser);
      analyser.connect(ctx.destination);
      analyserRef.current = analyser;
    } catch (e) {
      console.warn("Analyser setup failed:", e);
    }
  }, []);

  /* ── EQ visualizer draw loop ── */
  const drawViz = useCallback(() => {
    animFrameRef.current = requestAnimationFrame(drawViz);
    const canvas = vizCanvasRef.current;
    const analyser = analyserRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d"); if (!ctx) return;
    const w = canvas.width, h = canvas.height;
    ctx.clearRect(0, 0, w, h);

    const N = 24;
    const gap = 3;
    const barW = (w - gap * (N - 1)) / N;

    if (!analyser) {
      // Idle: flat baseline shimmer
      for (let i = 0; i < N; i++) {
        ctx.fillStyle = "rgba(245, 158, 11, 0.18)";
        ctx.fillRect(i * (barW + gap), h - 2, barW, 2);
      }
      return;
    }

    const data = new Uint8Array(analyser.frequencyBinCount);
    analyser.getByteFrequencyData(data);
    for (let i = 0; i < N; i++) {
      const idx = Math.floor((i / N) * data.length);
      const v = data[idx] / 255;
      const barH = Math.max(2, v * h);
      const alpha = 0.4 + v * 0.6;
      ctx.fillStyle = `rgba(245, 158, 11, ${alpha})`;
      ctx.fillRect(i * (barW + gap), h - barH, barW, barH);
    }
  }, []);

  useEffect(() => {
    animFrameRef.current = requestAnimationFrame(drawViz);
    return () => cancelAnimationFrame(animFrameRef.current);
  }, [drawViz]);

  /* ── Play a clip ── */
  const playClip = useCallback(async (url: string, seqId: number): Promise<boolean> => {
    const audio = audioRef.current; if (!audio) return false;
    const src = preloadedRef.current.get(url) || url;
    audio.src = src; audio.volume = volumeRef.current; audio.muted = mutedRef.current;
    await new Promise<void>(res => {
      audio.oncanplay = () => { audio.oncanplay = null; res(); };
      audio.onerror = () => { audio.onerror = null; res(); };
      audio.load();
    });
    if (sequencerIdRef.current !== seqId) return false;
    try { await audio.play(); } catch { return false; }
    await new Promise<void>(res => {
      audio.onended = () => { audio.onended = null; res(); };
      audio.onerror = () => { audio.onerror = null; res(); };
    });
    return sequencerIdRef.current === seqId;
  }, []);

  const waitAbortable = useCallback((ms: number, seqId: number): Promise<boolean> => {
    return new Promise(res => {
      setTimeout(() => res(sequencerIdRef.current === seqId), ms);
    });
  }, []);

  /* ── Sequencer ── */
  const runShowFrom = useCallback(async (startSegIdx: number = 0) => {
    if (!meta) return;
    const seqId = ++sequencerIdRef.current;
    setIsPaused(false);
    startElapsedTimer();

    const segs = timeline.current;
    if (segs.length === 0) return;

    // Bg bed: fade in if not already playing (also handles language-toggle resume)
    if (bgAudioRef.current?.paused !== false) fadeInBg(BG_VOLUME, 1500);

    for (let si = startSegIdx; si < segs.length; si++) {
      if (sequencerIdRef.current !== seqId) return;

      const seg = segs[si];

      if (seg.type === "intro") {
        setPhase("intro");
        const audio = audioRef.current;
        if (audio && !audio.paused && !audio.ended && audio.readyState >= 2) {
          const ok = await new Promise<boolean>(res => {
            audio.onended = () => { audio.onended = null; res(sequencerIdRef.current === seqId); };
            audio.onerror = () => { audio.onerror = null; res(false); };
          });
          if (!ok) return;
        } else {
          const ok = await playClip(meta.intro.audio_url, seqId);
          if (!ok) return;
        }
      } else if (seg.type === "pause") {
        setPhase("intro-pause");
        if (!await waitAbortable(2000, seqId)) return;
      } else if (seg.type === "transition") {
        preloadNext(seg.index + 1);
        setPhase("transition");
        setCurrentArticle(seg.index);
        const types: TransitionType[] = ["page-flip", "card-shuffle"];
        setTransition(types[transitionIdx.current % types.length]);
        transitionIdx.current++;
        if (!await waitAbortable(ANIM_MS, seqId)) return;
        setTransition(null);
        if (!await waitAbortable(TRANSITION_MS - ANIM_MS, seqId)) return;
      } else if (seg.type === "article") {
        setPhase("article");
        setCurrentArticle(seg.index);
        const audio = audioRef.current;
        if (audio && !audio.paused && !audio.ended && audio.readyState >= 2) {
          const ok = await new Promise<boolean>(res => {
            audio.onended = () => { audio.onended = null; res(sequencerIdRef.current === seqId); };
            audio.onerror = () => { audio.onerror = null; res(false); };
          });
          if (!ok) return;
        } else {
          const ok = await playClip(meta.articles[seg.index].audio_url, seqId);
          if (!ok) return;
        }
        if (!await waitAbortable(300, seqId)) return;
      } else if (seg.type === "outro") {
        setPhase("outro");
        const audio = audioRef.current;
        if (audio && !audio.paused && !audio.ended && audio.readyState >= 2) {
          const ok = await new Promise<boolean>(res => {
            audio.onended = () => { audio.onended = null; res(sequencerIdRef.current === seqId); };
            audio.onerror = () => { audio.onerror = null; res(false); };
          });
          if (!ok) return;
        } else {
          const ok = await playClip(meta.outro.audio_url, seqId);
          if (!ok) return;
        }
        if (!await waitAbortable(800, seqId)) return;
      } else if (seg.type === "gap") {
        if (!await waitAbortable(300, seqId)) return;
      }
    }

    if (sequencerIdRef.current === seqId) {
      fadeOutBg(1500);
      setPhase("done");
      stopElapsedTimer();
    }
  }, [meta, playClip, waitAbortable, fadeInBg, fadeOutBg, preloadNext, startElapsedTimer, stopElapsedTimer]);

  const runShow = useCallback(() => runShowFrom(0), [runShowFrom]);

  /* ── Resume in new language at the matching article when meta refreshes ── */
  useEffect(() => {
    if (!meta) return;
    const resume = pendingResumeRef.current;
    if (!resume) return;
    pendingResumeRef.current = null;

    const segs = timeline.current;
    let targetIdx = -1;
    if (resume.phase === "outro") {
      targetIdx = segs.findIndex(s => s.type === "outro");
    } else {
      // intro / intro-pause / article / transition → land on the matching article
      // (or article 0 if we hadn't started one yet)
      const targetArticle = (resume.phase === "intro" || resume.phase === "intro-pause") ? 0 : resume.articleIdx;
      targetIdx = segs.findIndex(s => s.type === "article" && s.index === targetArticle);
    }

    if (targetIdx < 0) {
      // Fallback — couldn't find a matching segment, do a cold countdown
      setCountdown(COUNTDOWN_SEC);
      setCurrentArticle(0);
      elapsedRef.current = 0; liveElapsedRef.current = 0; setElapsed(0);
      setPhase("countdown");
      return;
    }

    const seg = segs[targetIdx];
    setCurrentArticle(seg.type === "article" ? seg.index : 0);
    elapsedRef.current = seg.startAt;
    liveElapsedRef.current = Math.max(liveElapsedRef.current, seg.startAt);
    setElapsed(elapsedRef.current);
    runShowFrom(targetIdx);
  }, [meta, runShowFrom]);

  /* ── User clicks play ── */
  const handleStart = useCallback(() => {
    ensureAnalyser();
    audioCtxRef.current?.resume?.().catch(() => {});
    const audio = audioRef.current;
    if (audio && meta) {
      const src = preloadedRef.current.get(meta.intro.audio_url) || meta.intro.audio_url;
      audio.src = src; audio.volume = volumeRef.current; audio.muted = mutedRef.current;
      audio.load(); audio.play().catch(() => {});
    }
    runShow();
  }, [runShow, meta, ensureAnalyser]);

  /* ── Countdown ── */
  useEffect(() => {
    if (phase !== "countdown") return;
    if (countdown <= 0) { setPhase("ready"); return; }
    const timer = setTimeout(() => setCountdown(c => c - 1), 1000);
    return () => clearTimeout(timer);
  }, [phase, countdown]);

  /* ── Pause / Resume ── */
  const handlePause = useCallback(() => {
    audioRef.current?.pause();
    fadeOutBg(400);
    stopElapsedTimer(); setIsPaused(true);
  }, [fadeOutBg, stopElapsedTimer]);

  const handleResume = useCallback(() => {
    audioRef.current?.play().catch(() => {});
    fadeInBg(BG_VOLUME, 400);
    setIsPaused(false); startElapsedTimer();
  }, [fadeInBg, startElapsedTimer]);

  const handleTogglePlayPause = useCallback(() => {
    if (isPaused) handleResume(); else handlePause();
  }, [isPaused, handlePause, handleResume]);

  /* ── Seek helpers ── */
  const findPlayableAt = useCallback((t: number): { segIdx: number; offset: number } | null => {
    const segs = timeline.current;
    let hitIdx = 0;
    for (let i = segs.length - 1; i >= 0; i--) {
      if (t >= segs[i].startAt) { hitIdx = i; break; }
    }
    const hit = segs[hitIdx];
    const offset = t - hit.startAt;

    if (hit.url && (hit.type === "intro" || hit.type === "article" || hit.type === "outro")) {
      return { segIdx: hitIdx, offset };
    }
    for (let i = hitIdx - 1; i >= 0; i--) {
      if (segs[i].url && (segs[i].type === "intro" || segs[i].type === "article" || segs[i].type === "outro")) {
        const timeIntoGap = t - hit.startAt;
        const prevClipOffset = Math.max(0, segs[i].duration - (hit.duration - timeIntoGap));
        return { segIdx: i, offset: prevClipOffset };
      }
    }
    for (let i = hitIdx + 1; i < segs.length; i++) {
      if (segs[i].url) return { segIdx: i, offset: 0 };
    }
    return null;
  }, []);

  const seekTo = useCallback(async (targetElapsed: number) => {
    const audio = audioRef.current;
    if (!audio || !meta) return;

    const result = findPlayableAt(targetElapsed);
    if (!result) return;

    const { segIdx, offset } = result;
    const seg = timeline.current[segIdx];

    elapsedRef.current = seg.startAt + offset;
    setElapsed(elapsedRef.current);
    setAtLiveEdge(Math.abs(elapsedRef.current - liveElapsedRef.current) < 0.5);

    const seqId = ++sequencerIdRef.current;
    audio.pause();
    audio.onended = null; audio.onerror = null; audio.oncanplay = null;

    if (seg.type === "article") { setPhase("article"); setCurrentArticle(seg.index); }
    else if (seg.type === "intro") { setPhase("intro"); }
    else { setPhase("outro"); }

    const src = preloadedRef.current.get(seg.url!) || seg.url!;
    audio.src = src;
    audio.volume = volumeRef.current; audio.muted = mutedRef.current;

    await new Promise<void>(res => {
      audio.oncanplay = () => { audio.oncanplay = null; res(); };
      audio.onerror = () => { audio.onerror = null; res(); };
      audio.load();
    });

    if (sequencerIdRef.current !== seqId) return;
    audio.currentTime = Math.min(offset, audio.duration || offset);
    try { await audio.play(); } catch { return; }

    await new Promise<void>(res => {
      audio.onended = () => { audio.onended = null; res(); };
      audio.onerror = () => { audio.onerror = null; res(); };
    });

    if (sequencerIdRef.current !== seqId) return;
    runShowFrom(segIdx + 1);
  }, [meta, findPlayableAt, runShowFrom]);

  const handleSkipBack = useCallback(() => {
    const newElapsed = Math.max(0, elapsedRef.current - 5);
    seekTo(newElapsed);
  }, [seekTo]);

  const handleSkipForward = useCallback(() => {
    const maxElapsed = liveElapsedRef.current;
    const newElapsed = Math.min(elapsedRef.current + 5, maxElapsed);
    seekTo(newElapsed);
  }, [seekTo]);

  const handleReplay = useCallback(() => {
    setCurrentArticle(0); transitionIdx.current = 0;
    elapsedRef.current = 0; liveElapsedRef.current = 0; setElapsed(0);
    runShow();
  }, [runShow]);

  /* ── Cleanup ── */
  useEffect(() => {
    return () => {
      cancelAnimationFrame(animFrameRef.current);
      audioRef.current?.pause(); bgAudioRef.current?.pause();
      preloadedRef.current.forEach(b => URL.revokeObjectURL(b));
      if (elapsedTimerRef.current) clearInterval(elapsedTimerRef.current);
      audioCtxRef.current?.close().catch(() => {});
    };
  }, []);

  /* ── Derived ── */
  const article = articles[currentArticle] ?? null;
  const transitionClass = transition === "page-flip" ? "animate-page-flip" : transition === "card-shuffle" ? "animate-card-shuffle" : "";
  const isPlaying = phase === "intro" || phase === "article" || phase === "outro" || phase === "transition" || phase === "intro-pause";
  const showArticle = phase === "article" || phase === "transition";
  const progressPct = totalDuration > 0 ? Math.min((elapsed / totalDuration) * 100, 100) : 0;

  return (
    <>
      <style jsx global>{`
        @keyframes pageFlip {
          0% { transform: perspective(1200px) rotateY(-90deg); opacity: 0; }
          40% { transform: perspective(1200px) rotateY(10deg); opacity: 1; }
          70% { transform: perspective(1200px) rotateY(-5deg); }
          100% { transform: perspective(1200px) rotateY(0deg); opacity: 1; }
        }
        @keyframes cardShuffle {
          0% { transform: translateX(60%) scale(0.9) rotate(3deg); opacity: 0; }
          50% { transform: translateX(-4%) scale(1.02) rotate(-1deg); opacity: 1; }
          75% { transform: translateX(2%) scale(0.99) rotate(0.5deg); }
          100% { transform: translateX(0) scale(1) rotate(0deg); opacity: 1; }
        }
        .animate-page-flip { animation: pageFlip ${ANIM_MS}ms cubic-bezier(0.22, 1, 0.36, 1) both; transform-origin: left center; }
        .animate-card-shuffle { animation: cardShuffle ${ANIM_MS}ms cubic-bezier(0.22, 1, 0.36, 1) both; }
      `}</style>

      <div ref={containerRef} className={`bg-gradient-to-br from-gray-900 to-gray-800 overflow-hidden shadow-lg ${isFullscreen ? "rounded-none flex flex-col h-screen" : "rounded-2xl"}`}>
        <div className={`relative flex flex-col ${isFullscreen ? "flex-1" : ""}`} style={isFullscreen ? undefined : { minHeight: 360 }}>

          {/* ── Article pane (full width) ── */}
          <div className="flex-1 flex flex-col px-5 py-5 overflow-hidden">

            {/* Loading */}
            {phase === "loading" && (
              <div className="flex-1 flex items-center justify-center">
                <div className="flex items-center gap-3 text-gray-400 text-sm font-semibold">
                  <div className="w-4 h-4 border-2 border-amber-400/40 border-t-amber-400 rounded-full animate-spin" />
                  {isTA ? "செய்திகள் ஏற்றப்படுகின்றன..." : "Loading broadcast..."}
                </div>
              </div>
            )}

            {/* Countdown / ready / done — show logo */}
            {(phase === "countdown" || phase === "ready" || phase === "done") && (
              <div className="flex-1 flex items-center justify-center relative">
                {/* eslint-disable-next-line @next/next/no-img-element */}
                <img src="/logo.gif" alt="Arasiyal Aayvu" className="max-h-44 opacity-90" />

                {phase === "countdown" && countdown > 0 && (
                  <div className="absolute inset-0 flex items-center justify-center">
                    <div className="w-20 h-20 rounded-full bg-white/10 border-2 border-white/40 flex items-center justify-center backdrop-blur-sm">
                      <span className="text-3xl font-black text-white">{countdown}</span>
                    </div>
                  </div>
                )}

                {phase === "ready" && (
                  <button onClick={handleStart} className="absolute inset-0 flex items-center justify-center bg-black/30 cursor-pointer group">
                    <div className="w-16 h-16 rounded-full bg-white/90 flex items-center justify-center shadow-lg group-hover:scale-110 transition-transform">
                      <svg className="w-7 h-7 text-gray-900 ml-1" fill="currentColor" viewBox="0 0 24 24"><path d="M8 5v14l11-7z" /></svg>
                    </div>
                  </button>
                )}

                {phase === "done" && (
                  <button onClick={handleReplay} className="absolute inset-0 flex items-center justify-center bg-black/40 cursor-pointer group">
                    <div className="flex flex-col items-center gap-3">
                      <div className="w-16 h-16 rounded-full bg-white/90 flex items-center justify-center shadow-lg group-hover:scale-110 transition-transform">
                        <svg className="w-7 h-7 text-gray-900" fill="currentColor" viewBox="0 0 24 24">
                          <path d="M12 5V1L7 6l5 5V7c3.31 0 6 2.69 6 6s-2.69 6-6 6-6-2.69-6-6H4c0 4.42 3.58 8 8 8s8-3.58 8-8-3.58-8-8-8z" />
                        </svg>
                      </div>
                      <span className="text-xs font-bold text-white/80">{isTA ? "மீண்டும் கேட்க" : "Replay"}</span>
                    </div>
                  </button>
                )}
              </div>
            )}

            {/* Intro / pause / outro — show logo + label */}
            {(phase === "intro" || phase === "intro-pause" || phase === "outro") && (
              <div className="flex-1 flex flex-col items-center justify-center gap-4">
                {/* eslint-disable-next-line @next/next/no-img-element */}
                <img src="/logo.gif" alt="Arasiyal Aayvu" className="max-h-40 opacity-90" />
                <p className="text-xs font-bold text-amber-400/80 uppercase tracking-widest">
                  {phase === "outro"
                    ? (isTA ? "முடிவு" : "Signing off")
                    : (isTA ? "தமிழ் செல்வி ஒளிபரப்பு" : "TamilSelvi Broadcast")}
                </p>
              </div>
            )}

            {/* Article */}
            {showArticle && article && (
              <div key={`article-${currentArticle}-${lang}`} className={`flex-1 flex flex-col ${transitionClass}`}>
                <div className="flex items-center gap-2 mb-3 flex-wrap">
                  <span className="text-[10px] font-bold px-2.5 py-1 rounded-full bg-amber-500/20 text-amber-400 uppercase tracking-wide">
                    {article.category}
                  </span>
                  {article.sdg_alignment?.slice(0, 3).map(sdg => (
                    <span key={sdg} className="text-[9px] font-bold px-2 py-0.5 rounded bg-cyan-500/15 text-cyan-400">{sdg}</span>
                  ))}
                </div>
                <h3 className={`font-black text-white leading-snug mb-3 ${isFullscreen ? "text-4xl" : "text-xl"}`}>
                  {article.title}
                </h3>
                <div className="w-12 h-0.5 bg-amber-500/60 rounded-full mb-3" />
                {article.summary && (
                  <p className={`text-gray-300 leading-relaxed flex-1 ${isFullscreen ? "text-xl" : "text-sm"}`}>
                    {article.summary}
                  </p>
                )}
                <div className="flex items-center justify-between mt-4 pt-3 border-t border-gray-700/40">
                  <span className="text-[10px] text-gray-500 font-medium">{article.source_name}</span>
                  {article.source_url && (
                    <a href={article.source_url} target="_blank" rel="noopener noreferrer"
                      className="text-[10px] font-bold text-amber-400 hover:text-amber-300 transition-colors cursor-pointer flex items-center gap-1">
                      {isTA ? "முழு செய்தி" : "Read full article"}
                      <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                      </svg>
                    </a>
                  )}
                </div>
                {articles.length > 1 && (
                  <div className="flex gap-1.5 mt-3">
                    {articles.map((_, i) => (
                      <div key={i} className={`h-1 rounded-full transition-all ${
                        i === currentArticle ? "w-6 bg-amber-500" :
                        i < currentArticle ? "w-3 bg-amber-500/40" :
                        "w-3 bg-gray-700"
                      }`} />
                    ))}
                  </div>
                )}
              </div>
            )}

            {/* EQ visualizer — always visible in the player chrome */}
            <div className="mt-4 pt-3 border-t border-gray-700/40">
              <canvas ref={vizCanvasRef} width={640} height={40} className="w-full h-10" />
            </div>
          </div>

          {/* Off-screen audio (speech) */}
          <audio ref={audioRef} preload="auto" className="hidden" crossOrigin="anonymous" />
        </div>

        {/* ── Progress bar ── */}
        {isPlaying && (
          <div className="relative h-1 bg-gray-700/50 cursor-default">
            <div className="absolute top-0 left-0 h-full bg-gray-600/50" style={{ width: `${totalDuration > 0 ? Math.min((liveElapsedRef.current / totalDuration) * 100, 100) : 0}%` }} />
            <div className="absolute top-0 left-0 h-full bg-red-500 transition-all duration-200" style={{ width: `${progressPct}%` }} />
          </div>
        )}

        {/* ── Controls bar ── */}
        <div className="px-3 py-1.5 border-t border-gray-700/50 flex items-center gap-2">
          {/* Play/Pause */}
          {isPlaying ? (
            <button onClick={handleTogglePlayPause} className="text-white hover:text-amber-400 transition-colors cursor-pointer p-1" title={isPaused ? "Play" : "Pause"}>
              {isPaused ? (
                <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 24 24"><path d="M8 5v14l11-7z"/></svg>
              ) : (
                <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 24 24"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg>
              )}
            </button>
          ) : (
            <div className="w-7" />
          )}

          {/* Skip back */}
          {isPlaying && (
            <button onClick={handleSkipBack} className="text-white/70 hover:text-white transition-colors cursor-pointer p-1" title="Back 5s">
              <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">
                <path d="M11.99 5V1l-5 5 5 5V7c3.31 0 6 2.69 6 6s-2.69 6-6 6-6-2.69-6-6h-2c0 4.42 3.58 8 8 8s8-3.58 8-8-3.58-8-8-8z"/>
                <text x="9" y="15.5" fontSize="7" fontWeight="bold" fill="currentColor">5</text>
              </svg>
            </button>
          )}

          {/* Skip forward */}
          {isPlaying && (
            <button onClick={handleSkipForward} className={`p-1 transition-colors cursor-pointer ${atLiveEdge ? "text-white/30 cursor-not-allowed" : "text-white/70 hover:text-white"}`}
              title="Forward 5s" disabled={atLiveEdge}>
              <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">
                <path d="M12.01 5V1l5 5-5 5V7c-3.31 0-6 2.69-6 6s2.69 6 6 6 6-2.69 6-6h2c0 4.42-3.58 8-8 8s-8-3.58-8-8 3.58-8 8-8z"/>
                <text x="9" y="15.5" fontSize="7" fontWeight="bold" fill="currentColor">5</text>
              </svg>
            </button>
          )}

          {/* Time display */}
          {isPlaying && (
            <span className="text-[10px] text-gray-400 font-mono tabular-nums ml-1">
              {formatTime(elapsed)} / {formatTime(totalDuration)}
            </span>
          )}

          <div className="flex-1" />

          {/* Language toggle */}
          <button
            onClick={toggleLang}
            className="text-[10px] font-bold px-2 py-1 rounded border border-white/20 text-white/80 hover:bg-white/10 hover:text-white transition-colors cursor-pointer"
            title={isTA ? "Switch to English" : "தமிழுக்கு மாற்று"}
          >
            {isTA ? "EN" : "தமிழ்"}
          </button>

          {/* LIVE badge */}
          {isPlaying && (
            <div className={`flex items-center gap-1 px-2 py-0.5 rounded text-[9px] font-bold uppercase tracking-wider ${atLiveEdge ? "bg-red-600 text-white" : "bg-gray-600 text-gray-300 cursor-pointer hover:bg-red-600 hover:text-white"}`}
              onClick={() => { if (!atLiveEdge) seekTo(liveElapsedRef.current); }}
              title={atLiveEdge ? "Live" : "Click to go live"}>
              <div className={`w-1.5 h-1.5 rounded-full ${atLiveEdge ? "bg-white animate-pulse" : "bg-gray-400"}`} />
              LIVE
            </div>
          )}

          {/* Volume */}
          <div className="flex items-center gap-1">
            <button onClick={() => { setMuted(!muted); mutedRef.current = !muted; }}
              className="text-white/70 hover:text-white transition-colors cursor-pointer p-1" title={muted ? "Unmute" : "Mute"}>
              {muted || volume === 0 ? (
                <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24"><path d="M16.5 12c0-1.77-1.02-3.29-2.5-4.03v2.21l2.45 2.45c.03-.2.05-.41.05-.63zm2.5 0c0 .94-.2 1.82-.54 2.64l1.51 1.51A8.8 8.8 0 0021 12c0-4.28-2.99-7.86-7-8.77v2.06c2.89.86 5 3.54 5 6.71zM4.27 3L3 4.27 7.73 9H3v6h4l5 5v-6.73l4.25 4.25c-.67.52-1.42.93-2.25 1.18v2.06a8.99 8.99 0 003.69-1.81L19.73 21 21 19.73l-9-9L4.27 3zM12 4L9.91 6.09 12 8.18V4z"/></svg>
              ) : volume < 0.5 ? (
                <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24"><path d="M18.5 12A4.5 4.5 0 0016 8.97v6.06c1.48-.73 2.5-2.25 2.5-4.03zM5 9v6h4l5 5V4L9 9H5z"/></svg>
              ) : (
                <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24"><path d="M3 9v6h4l5 5V4L7 9H3zm13.5 3A4.5 4.5 0 0014 8.97v6.06c1.48-.73 2.5-2.25 2.5-4.03zM14 3.23v2.06c2.89.86 5 3.54 5 6.71s-2.11 5.85-5 6.71v2.06c4.01-.91 7-4.49 7-8.77s-2.99-7.86-7-8.77z"/></svg>
              )}
            </button>
            <input type="range" min={0} max={1} step={0.05} value={muted ? 0 : volume}
              onChange={(e) => { const v = parseFloat(e.target.value); setVolume(v); setMuted(v === 0); volumeRef.current = v; mutedRef.current = v === 0; }}
              className="w-14 h-1 accent-white cursor-pointer" />
          </div>

          {/* Fullscreen */}
          <button onClick={toggleFullscreen} className="text-white/70 hover:text-white transition-colors cursor-pointer p-1" title={isFullscreen ? "Exit fullscreen" : "Fullscreen"}>
            {isFullscreen ? (
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 9L4 4m0 0v5m0-5h5m6 6l5 5m0 0v-5m0 5h-5M9 15l-5 5m0 0h5m-5 0v-5m11-6l5-5m0 0h-5m5 0v5" />
              </svg>
            ) : (
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 8V4m0 0h4M4 4l5 5m11-1V4m0 0h-4m4 0l-5 5M4 16v4m0 0h4m-4 0l5-5m11 5v-4m0 4h-4m4 0l-5-5" />
              </svg>
            )}
          </button>
        </div>

        {/* Credit line */}
        <div className="px-3 py-1 border-t border-gray-700/30">
          <p className="text-[8px] text-gray-600">
            {isTA ? "தமிழ் செல்வி · AI செய்தி வாசிப்பாளர்" : "TamilSelvi · AI News Reader"} · Gemini · edge-tts
          </p>
        </div>
      </div>
    </>
  );
}
