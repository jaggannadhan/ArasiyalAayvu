"use client";

import { useRef, useEffect, useState, useCallback } from "react";

/* ── Types ── */
interface MetaArticle {
  title: string;
  summary: string;
  category: string;
  sdg_alignment: string[];
  source_name: string;
  source_url: string;
  video_url: string;
  duration: number;
}

interface ReaderMeta {
  generated_at: string;
  lang: string;
  intro: { video_url: string; duration: number };
  outro: { video_url: string; duration: number };
  articles: MetaArticle[];
}

interface Props {
  lang?: "en" | "ta";
}

const GCS_BASE = "https://storage.googleapis.com/naatunadappu-media/news-reader";
const META_URL = `${GCS_BASE}/latest_meta.json`;
const ANCHOR_IMG = "/news-reader-anchor.png";
const BLACK_THRESHOLD = 30;
const ANIM_MS = 1500;
const TRANSITION_MS = 3000;
const COUNTDOWN_SEC = 4;

type TransitionType = "page-flip" | "card-shuffle";
type Phase = "loading" | "countdown" | "ready" | "intro" | "intro-pause" | "article" | "transition" | "outro" | "done";

async function preloadVideo(url: string): Promise<string> {
  const resp = await fetch(url);
  const blob = await resp.blob();
  return URL.createObjectURL(blob);
}

function formatTime(s: number): string {
  const m = Math.floor(s / 60);
  const sec = Math.floor(s % 60);
  return `${m}:${sec.toString().padStart(2, "0")}`;
}

export function NewsReaderPlayer({ lang = "en" }: Props) {
  const isTA = lang === "ta";
  const containerRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const videoRef = useRef<HTMLVideoElement>(null);
  const anchorImgRef = useRef<HTMLImageElement | null>(null);
  const bgAudioRef = useRef<HTMLAudioElement | null>(null);
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
  const [elapsed, setElapsed] = useState(0); // total elapsed seconds in the broadcast
  const [atLiveEdge, setAtLiveEdge] = useState(true);

  const volumeRef = useRef(1);
  const mutedRef = useRef(false);
  const animFrameRef = useRef<number>(0);
  const transitionIdx = useRef(0);
  const elapsedRef = useRef(0);
  const liveElapsedRef = useRef(0); // the furthest point reached
  const elapsedTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const preloadedRef = useRef<Map<string, string>>(new Map());

  const articles = meta?.articles ?? [];

  // Total broadcast duration (approximate)
  const totalDuration = meta
    ? meta.intro.duration + 3 + // intro + intro-pause
      meta.articles.reduce((sum, a) => sum + a.duration + TRANSITION_MS / 1000 + 0.3, 0) +
      meta.outro.duration + 1.2
    : 0;

  /* ── Elapsed timer — ticks every 200ms while playing ── */
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
    document.fullscreenElement ? document.exitFullscreen() : el.requestFullscreen();
  };

  /* ── Fetch metadata ── */
  useEffect(() => {
    fetch(`${META_URL}?t=${Date.now()}`)
      .then((r) => r.json())
      .then((data: ReaderMeta) => { setMeta(data); setPhase("countdown"); })
      .catch((err) => console.error("Failed to load metadata:", err));
  }, []);

  /* ── Load anchor image ── */
  useEffect(() => {
    const img = new Image();
    img.crossOrigin = "anonymous";
    img.src = ANCHOR_IMG;
    img.onload = () => anchorImgRef.current = img;
  }, []);

  /* ── Preload priority clips during countdown ── */
  useEffect(() => {
    if (!meta || phase !== "countdown") return;
    [meta.intro.video_url, meta.outro.video_url, meta.articles[0]?.video_url].filter(Boolean).forEach(async (url) => {
      if (preloadedRef.current.has(url)) return;
      try { preloadedRef.current.set(url, await preloadVideo(url)); } catch {}
    });
  }, [meta, phase]);

  const preloadNext = useCallback((idx: number) => {
    if (!meta || idx >= meta.articles.length) return;
    const url = meta.articles[idx].video_url;
    if (!preloadedRef.current.has(url)) preloadVideo(url).then(b => preloadedRef.current.set(url, b)).catch(() => {});
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

  const fadeInBg = useCallback((vol: number, ms: number) => {
    const a = bgAudioRef.current; if (!a) return;
    if (!a.src) { a.src = "/news-bg.mp3"; a.load(); }
    stopFade(); a.volume = 0; a.play().catch(() => {});
    const steps = 20, step = vol / steps; let n = 0;
    fadeIntervalRef.current = setInterval(() => { n++; a.volume = Math.min(n * step, vol); if (n >= steps) stopFade(); }, ms / steps);
  }, [stopFade]);

  const fadeOutBg = useCallback((ms: number) => {
    const a = bgAudioRef.current; if (!a || a.paused) return;
    stopFade(); const start = a.volume; if (start <= 0) { a.pause(); return; }
    const steps = 20, step = start / steps; let n = 0;
    fadeIntervalRef.current = setInterval(() => { n++; a.volume = Math.max(start - n * step, 0); if (n >= steps) { stopFade(); a.pause(); a.volume = 0; } }, ms / steps);
  }, [stopFade]);

  /* ── Sync volume refs ── */
  useEffect(() => {
    volumeRef.current = volume; mutedRef.current = muted;
    const v = videoRef.current; if (v) { v.volume = volume; v.muted = muted; }
  }, [volume, muted]);

  /* ── Canvas render ── */
  const renderFrame = useCallback(() => {
    const canvas = canvasRef.current, video = videoRef.current, img = anchorImgRef.current;
    if (!canvas) { animFrameRef.current = requestAnimationFrame(renderFrame); return; }
    const ctx = canvas.getContext("2d", { willReadFrequently: true }); if (!ctx) return;
    const w = canvas.width, h = canvas.height; ctx.clearRect(0, 0, w, h);
    const source = (video && !video.paused && !video.ended && video.readyState >= 2) ? video : img;
    if (!source) { animFrameRef.current = requestAnimationFrame(renderFrame); return; }
    const srcW = source instanceof HTMLVideoElement ? source.videoWidth : source.width;
    const srcH = source instanceof HTMLVideoElement ? source.videoHeight : source.height;
    if (!srcW || !srcH) { animFrameRef.current = requestAnimationFrame(renderFrame); return; }
    const aspect = srcW / srcH, drawH = Math.floor(h * 1.15), drawW = Math.floor(drawH * aspect);
    const drawX = Math.floor((w - drawW) / 2), drawY = h - drawH;
    ctx.drawImage(source, drawX, drawY, drawW, drawH);
    const imageData = ctx.getImageData(0, 0, w, h); const data = imageData.data;
    for (let i = 0; i < data.length; i += 4) { if (data[i] < BLACK_THRESHOLD && data[i+1] < BLACK_THRESHOLD && data[i+2] < BLACK_THRESHOLD) data[i+3] = 0; }
    ctx.putImageData(imageData, 0, 0);
    animFrameRef.current = requestAnimationFrame(renderFrame);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    const t = setTimeout(() => { animFrameRef.current = requestAnimationFrame(renderFrame); }, 300);
    return () => { clearTimeout(t); cancelAnimationFrame(animFrameRef.current); };
  }, [renderFrame]);

  /* ── Play a clip ── */
  const playClip = useCallback(async (url: string): Promise<void> => {
    const video = videoRef.current; if (!video) return;
    const src = preloadedRef.current.get(url) || url;
    video.src = src; video.volume = volumeRef.current; video.muted = mutedRef.current;
    await new Promise<void>(res => { video.oncanplay = () => { video.oncanplay = null; res(); }; video.onerror = () => { video.onerror = null; res(); }; video.load(); });
    try { await video.play(); } catch { return; }
    await new Promise<void>(res => { video.onended = () => { video.onended = null; res(); }; video.onerror = () => { video.onerror = null; res(); }; });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const wait = (ms: number) => new Promise(r => setTimeout(r, ms));

  /* ── Main sequencer ── */
  const runShow = useCallback(async () => {
    if (!meta) return;
    setIsPaused(false);
    startElapsedTimer();

    setPhase("intro");
    const video = videoRef.current;
    if (video && !video.paused && !video.ended) {
      await new Promise<void>(res => { video.onended = () => { video.onended = null; res(); }; video.onerror = () => { video.onerror = null; res(); }; });
    } else {
      await playClip(meta.intro.video_url);
    }

    setPhase("intro-pause");
    fadeInBg(0.15, 400); await wait(2000); fadeOutBg(800); await wait(1000);

    for (let i = 0; i < meta.articles.length; i++) {
      preloadNext(i + 1);
      setPhase("transition"); setCurrentArticle(i); fadeInBg(0.5, 300);
      const types: TransitionType[] = ["page-flip", "card-shuffle"];
      setTransition(types[transitionIdx.current % types.length]); transitionIdx.current++;
      await wait(ANIM_MS); setTransition(null); fadeOutBg(800); await wait(TRANSITION_MS - ANIM_MS);
      setPhase("article"); await playClip(meta.articles[i].video_url); await wait(300);
    }

    setPhase("outro"); fadeInBg(0.15, 500); await playClip(meta.outro.video_url); fadeOutBg(1000); await wait(1200);
    setPhase("done"); stopElapsedTimer();
  }, [meta, playClip, fadeInBg, fadeOutBg, preloadNext, startElapsedTimer, stopElapsedTimer]);

  /* ── User clicks play ── */
  const handleStart = useCallback(() => {
    const video = videoRef.current;
    if (video && meta) {
      const src = preloadedRef.current.get(meta.intro.video_url) || meta.intro.video_url;
      video.src = src; video.volume = volumeRef.current; video.muted = mutedRef.current;
      video.load(); video.play().catch(() => {});
    }
    runShow();
  }, [runShow, meta]);

  /* ── Countdown ── */
  useEffect(() => {
    if (phase !== "countdown") return;
    if (countdown <= 0) { setPhase("ready"); return; }
    const timer = setTimeout(() => setCountdown(c => c - 1), 1000);
    return () => clearTimeout(timer);
  }, [phase, countdown]);

  /* ── Pause / Resume ── */
  const handlePause = useCallback(() => {
    videoRef.current?.pause(); bgAudioRef.current?.pause();
    stopElapsedTimer(); setIsPaused(true);
  }, [stopElapsedTimer]);

  const handleResume = useCallback(() => {
    videoRef.current?.play(); setIsPaused(false); startElapsedTimer();
  }, [startElapsedTimer]);

  const handleTogglePlayPause = useCallback(() => {
    isPaused ? handleResume() : handlePause();
  }, [isPaused, handlePause, handleResume]);

  /* ── Skip back/forward 5s ── */
  const handleSkipBack = useCallback(() => {
    const video = videoRef.current; if (!video) return;
    video.currentTime = Math.max(0, video.currentTime - 5);
    elapsedRef.current = Math.max(0, elapsedRef.current - 5);
    setElapsed(elapsedRef.current);
    setAtLiveEdge(false);
  }, []);

  const handleSkipForward = useCallback(() => {
    const video = videoRef.current; if (!video) return;
    const maxForward = Math.min(video.currentTime + 5, video.duration);
    // Can't go beyond live edge
    const maxElapsed = liveElapsedRef.current;
    const newElapsed = Math.min(elapsedRef.current + 5, maxElapsed);
    video.currentTime = maxForward;
    elapsedRef.current = newElapsed;
    setElapsed(newElapsed);
    setAtLiveEdge(Math.abs(newElapsed - maxElapsed) < 0.5);
  }, []);

  /* ── Replay ── */
  const handleReplay = useCallback(() => {
    setCurrentArticle(0); transitionIdx.current = 0;
    elapsedRef.current = 0; liveElapsedRef.current = 0; setElapsed(0);
    runShow();
  }, [runShow]);

  /* ── Cleanup ── */
  useEffect(() => {
    return () => {
      cancelAnimationFrame(animFrameRef.current);
      videoRef.current?.pause(); bgAudioRef.current?.pause();
      preloadedRef.current.forEach(b => URL.revokeObjectURL(b));
      if (elapsedTimerRef.current) clearInterval(elapsedTimerRef.current);
    };
  }, []);

  /* ── Derived ── */
  const article = articles[currentArticle] ?? null;
  const transitionClass = transition === "page-flip" ? "animate-page-flip" : transition === "card-shuffle" ? "animate-card-shuffle" : "";
  const isBlurred = phase === "countdown" || phase === "loading";
  const showLogo = phase === "intro" || phase === "intro-pause" || phase === "outro" || phase === "ready" || phase === "countdown" || phase === "loading";
  const showArticle = phase === "article" || phase === "transition";
  const isPlaying = phase === "intro" || phase === "article" || phase === "outro" || phase === "transition" || phase === "intro-pause";
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
          0% { transform: translateX(100%) scale(0.85) rotate(4deg); opacity: 0; }
          50% { transform: translateX(-8%) scale(1.03) rotate(-1deg); opacity: 1; }
          75% { transform: translateX(3%) scale(0.99) rotate(0.5deg); }
          100% { transform: translateX(0) scale(1) rotate(0deg); opacity: 1; }
        }
        .animate-page-flip { animation: pageFlip ${ANIM_MS}ms cubic-bezier(0.22, 1, 0.36, 1) both; transform-origin: left center; }
        .animate-card-shuffle { animation: cardShuffle ${ANIM_MS}ms cubic-bezier(0.22, 1, 0.36, 1) both; }
      `}</style>

      <div ref={containerRef} className={`bg-gradient-to-br from-gray-900 to-gray-800 overflow-hidden shadow-lg ${isFullscreen ? "rounded-none flex flex-col h-screen" : "rounded-2xl"}`}>
        <div className={`relative flex ${isFullscreen ? "flex-1" : ""}`} style={isFullscreen ? undefined : { minHeight: 420 }}>

          {/* ── Left: News Reader ── */}
          <div className="w-[50%] relative">
            <canvas ref={canvasRef} width={550} height={480}
              className={`w-full h-full transition-[filter] duration-500 ${isBlurred ? "blur-sm brightness-50" : ""}`} />

            {phase === "countdown" && countdown > 0 && (
              <div className="absolute inset-0 flex items-center justify-center">
                <div className="w-20 h-20 rounded-full bg-white/10 border-2 border-white/40 flex items-center justify-center backdrop-blur-sm">
                  <span className="text-3xl font-black text-white">{countdown}</span>
                </div>
              </div>
            )}

            {phase === "ready" && (
              <button onClick={handleStart} className="absolute inset-0 flex items-center justify-center bg-black/20 cursor-pointer">
                <div className="w-16 h-16 rounded-full bg-white/90 flex items-center justify-center shadow-lg hover:scale-110 transition-transform">
                  <svg className="w-7 h-7 text-gray-900 ml-1" fill="currentColor" viewBox="0 0 24 24"><path d="M8 5v14l11-7z" /></svg>
                </div>
              </button>
            )}

            {phase === "done" && (
              <button onClick={handleReplay} className="absolute inset-0 flex items-center justify-center bg-black/40 cursor-pointer">
                <div className="flex flex-col items-center gap-3">
                  <div className="w-16 h-16 rounded-full bg-white/90 flex items-center justify-center shadow-lg hover:scale-110 transition-transform">
                    <svg className="w-7 h-7 text-gray-900" fill="currentColor" viewBox="0 0 24 24">
                      <path d="M12 5V1L7 6l5 5V7c3.31 0 6 2.69 6 6s-2.69 6-6 6-6-2.69-6-6H4c0 4.42 3.58 8 8 8s8-3.58 8-8-3.58-8-8-8z" />
                    </svg>
                  </div>
                  <span className="text-xs font-bold text-white/80">{isTA ? "மீண்டும் பார்க்க" : "Replay"}</span>
                </div>
              </button>
            )}
          </div>

          {/* ── Right: News Viewer ── */}
          <div className={`w-[50%] flex flex-col overflow-hidden transition-[filter] duration-500 ${isBlurred ? "blur-sm brightness-50" : ""}`}>
            {showLogo ? (
              <div className="flex-1 flex items-center justify-center">
                {/* eslint-disable-next-line @next/next/no-img-element */}
                <img src="/logo.gif" alt="Arasiyal Aayvu" className="w-full h-full object-offset" />
              </div>
            ) : showArticle && article ? (
              <div key={`article-${currentArticle}`} className={`flex-1 flex flex-col p-5 ${transitionClass}`}>
                <div className="flex items-center gap-2 mb-3">
                  <span className="text-[10px] font-bold px-2.5 py-1 rounded-full bg-amber-500/20 text-amber-400 uppercase tracking-wide">{article.category}</span>
                  {article.sdg_alignment?.slice(0, 3).map(sdg => (
                    <span key={sdg} className="text-[9px] font-bold px-2 py-0.5 rounded bg-cyan-500/15 text-cyan-400">{sdg}</span>
                  ))}
                </div>
                <h3 className={`font-black text-white leading-snug mb-3 ${isFullscreen ? "text-3xl" : "text-lg"}`}>{article.title}</h3>
                <div className="w-10 h-0.5 bg-amber-500/60 rounded-full mb-3" />
                {article.summary && <p className={`text-gray-300 leading-relaxed flex-1 ${isFullscreen ? "text-lg" : "text-sm"}`}>{article.summary}</p>}
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
                      <div key={i} className={`h-1 rounded-full transition-all ${i === currentArticle ? "w-6 bg-amber-500" : i < currentArticle ? "w-3 bg-amber-500/40" : "w-3 bg-gray-700"}`} />
                    ))}
                  </div>
                )}
              </div>
            ) : phase === "done" ? (
              <div className="flex-1 flex items-center justify-center">
                {/* eslint-disable-next-line @next/next/no-img-element */}
                <img src="/logo.gif" alt="Arasiyal Aayvu" className="w-full h-full object-offset" />
              </div>
            ) : null}
          </div>

          {/* Off-screen video */}
          {/* eslint-disable-next-line jsx-a11y/media-has-caption */}
          <video ref={videoRef} playsInline preload="auto" className="absolute w-0 h-0 opacity-0 pointer-events-none" crossOrigin="anonymous" />
        </div>

        {/* ── Progress bar ── */}
        {isPlaying && (
          <div className="relative h-1 bg-gray-700/50 cursor-default">
            {/* Live edge (furthest reached) */}
            <div className="absolute top-0 left-0 h-full bg-gray-600/50" style={{ width: `${totalDuration > 0 ? Math.min((liveElapsedRef.current / totalDuration) * 100, 100) : 0}%` }} />
            {/* Current position */}
            <div className="absolute top-0 left-0 h-full bg-red-500 transition-all duration-200" style={{ width: `${progressPct}%` }} />
          </div>
        )}

        {/* ── Controls bar (YouTube-style) ── */}
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
            <div className="w-7" /> /* spacer when not playing */
          )}

          {/* Skip back 5s */}
          {isPlaying && (
            <button onClick={handleSkipBack} className="text-white/70 hover:text-white transition-colors cursor-pointer p-1" title="Back 5s">
              <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">
                <path d="M11.99 5V1l-5 5 5 5V7c3.31 0 6 2.69 6 6s-2.69 6-6 6-6-2.69-6-6h-2c0 4.42 3.58 8 8 8s8-3.58 8-8-3.58-8-8-8z"/>
                <text x="9" y="15.5" fontSize="7" fontWeight="bold" fill="currentColor">5</text>
              </svg>
            </button>
          )}

          {/* Skip forward 5s */}
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

          {/* Spacer */}
          <div className="flex-1" />

          {/* LIVE badge */}
          {isPlaying && (
            <div className={`flex items-center gap-1 px-2 py-0.5 rounded text-[9px] font-bold uppercase tracking-wider ${atLiveEdge ? "bg-red-600 text-white" : "bg-gray-600 text-gray-300 cursor-pointer hover:bg-red-600 hover:text-white"}`}
              onClick={() => { if (!atLiveEdge) { elapsedRef.current = liveElapsedRef.current; setElapsed(liveElapsedRef.current); setAtLiveEdge(true); } }}
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

        {/* ── Credit line ── */}
        <div className="px-3 py-1 border-t border-gray-700/30">
          <p className="text-[8px] text-gray-600">
            {isTA ? "தமிழ் செல்வி · AI செய்தி வாசிப்பாளர்" : "TamilSelvi · AI News Reader"} · Gemini · edge-tts · Wav2Lip
          </p>
        </div>
      </div>
    </>
  );
}
