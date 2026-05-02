"use client";

import { useRef, useEffect, useState, useCallback } from "react";

interface MetaArticle {
  title: string;
  summary: string;
  category: string;
  sdg_alignment: string[];
  source_name: string;
  source_url: string;
  start: number;
  end: number;
}

interface ReaderMeta {
  generated_at: string;
  lang: string;
  timestamps: number[];
  intro_duration: number;
  transition_gap: number;
  articles: MetaArticle[];
}

interface Props {
  lang?: "en" | "ta";
}

const GCS_BASE = "https://storage.googleapis.com/naatunadappu-media/news-reader";
const VIDEO_URL = `${GCS_BASE}/latest.mp4`;
const META_URL = `${GCS_BASE}/latest_meta.json`;
const BLACK_THRESHOLD = 30;
const ANIM_MS = 1500; // CSS animation duration (1.5s of the 3s gap)

type TransitionType = "page-flip" | "card-shuffle";
type Phase = "intro" | "article" | "transition" | "outro";

export function NewsReaderPlayer({ lang = "en" }: Props) {
  const isTA = lang === "ta";
  const containerRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const videoRef = useRef<HTMLVideoElement>(null);
  const bgAudioRef = useRef<HTMLAudioElement | null>(null);
  const fadeIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const [playing, setPlaying] = useState(false);
  const [currentArticle, setCurrentArticle] = useState(0);
  const [meta, setMeta] = useState<ReaderMeta | null>(null);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [transition, setTransition] = useState<TransitionType | null>(null);
  const [phase, setPhase] = useState<Phase>("intro");
  const [countdown, setCountdown] = useState<number | null>(null);
  const [stageRevealed, setStageRevealed] = useState(false); // false = full-width canvas, true = split layout
  const [volume, setVolume] = useState(1);
  const [muted, setMuted] = useState(false);
  const animFrameRef = useRef<number>(0);
  const prevPhaseRef = useRef<Phase>("intro");
  const prevArticleRef = useRef(0);
  const transitionIdx = useRef(0);
  const autoPlayStarted = useRef(false);

  // ── Fullscreen ──
  useEffect(() => {
    const onFsChange = () => setIsFullscreen(!!document.fullscreenElement);
    document.addEventListener("fullscreenchange", onFsChange);
    return () => document.removeEventListener("fullscreenchange", onFsChange);
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

  // ── Fetch metadata ──
  useEffect(() => {
    fetch(META_URL)
      .then((r) => r.json())
      .then((data: ReaderMeta) => setMeta(data))
      .catch((err) => console.error("Failed to load news reader metadata:", err));
  }, []);

  const articles = meta?.articles ?? [];

  // ── Lazy-load background music ──
  useEffect(() => {
    const audio = new Audio();
    audio.preload = "none";
    audio.loop = true;
    audio.volume = 0;
    bgAudioRef.current = audio;
    return () => {
      audio.pause();
      audio.src = "";
    };
  }, []);

  const ensureBgAudioLoaded = useCallback(() => {
    const audio = bgAudioRef.current;
    if (audio && !audio.src) {
      audio.src = "/news-bg.mp3";
      audio.load();
    }
  }, []);

  const stopFade = useCallback(() => {
    if (fadeIntervalRef.current) {
      clearInterval(fadeIntervalRef.current);
      fadeIntervalRef.current = null;
    }
  }, []);

  const fadeInBgMusic = useCallback((targetVol: number, durationMs: number) => {
    const audio = bgAudioRef.current;
    if (!audio) return;
    ensureBgAudioLoaded();
    stopFade();
    audio.volume = 0;
    audio.play().catch(() => {});
    const steps = 20;
    const stepMs = durationMs / steps;
    const stepVol = targetVol / steps;
    let step = 0;
    fadeIntervalRef.current = setInterval(() => {
      step++;
      audio.volume = Math.min(step * stepVol, targetVol);
      if (step >= steps) stopFade();
    }, stepMs);
  }, [ensureBgAudioLoaded, stopFade]);

  const fadeOutBgMusic = useCallback((durationMs: number) => {
    const audio = bgAudioRef.current;
    if (!audio || audio.paused) return;
    stopFade();
    const startVol = audio.volume;
    if (startVol <= 0) { audio.pause(); return; }
    const steps = 20;
    const stepMs = durationMs / steps;
    const stepVol = startVol / steps;
    let step = 0;
    fadeIntervalRef.current = setInterval(() => {
      step++;
      audio.volume = Math.max(startVol - step * stepVol, 0);
      if (step >= steps) {
        stopFade();
        audio.pause();
        audio.volume = 0;
      }
    }, stepMs);
  }, [stopFade]);

  // ── Canvas render loop ──
  const renderFrame = useCallback(() => {
    const canvas = canvasRef.current;
    const video = videoRef.current;
    if (!canvas || !video || video.paused || video.ended) return;

    const ctx = canvas.getContext("2d", { willReadFrequently: true });
    if (!ctx) return;

    const w = canvas.width;
    const h = canvas.height;

    const vAspect = video.videoWidth / video.videoHeight;
    const drawH = Math.floor(h * 1.15);
    const drawW = Math.floor(drawH * vAspect);
    const drawX = Math.floor((w - drawW) / 2);
    const drawY = h - drawH;

    ctx.clearRect(0, 0, w, h);
    ctx.drawImage(video, drawX, drawY, drawW, drawH);

    const imageData = ctx.getImageData(0, 0, w, h);
    const data = imageData.data;
    for (let i = 0; i < data.length; i += 4) {
      const r = data[i];
      const g = data[i + 1];
      const b = data[i + 2];
      if (r < BLACK_THRESHOLD && g < BLACK_THRESHOLD && b < BLACK_THRESHOLD) {
        data[i + 3] = 0;
      }
    }
    ctx.putImageData(imageData, 0, 0);

    animFrameRef.current = requestAnimationFrame(renderFrame);
  }, []);

  // ── Determine phase & current article from video time ──
  useEffect(() => {
    const video = videoRef.current;
    if (!video || articles.length === 0) return;

    const onTimeUpdate = () => {
      const t = video.currentTime;
      const firstArticleStart = articles[0]?.start ?? 0;
      const lastArticle = articles[articles.length - 1];
      const outroStart = lastArticle?.end ?? Infinity;

      // Determine phase
      let newPhase: Phase;
      let newIdx = 0;

      if (t < firstArticleStart) {
        // Before first article = intro (includes the 15s pad + 3s gap)
        newPhase = "intro";
        newIdx = 0;
      } else if (t >= outroStart) {
        newPhase = "outro";
        newIdx = articles.length - 1;
      } else {
        // Check if we're inside an article or in a gap between articles
        let inArticle = false;
        for (let i = articles.length - 1; i >= 0; i--) {
          if (t >= articles[i].start && t < articles[i].end) {
            newPhase = "article";
            newIdx = i;
            inArticle = true;
            break;
          }
        }
        if (!inArticle) {
          // We're in a transition gap between articles
          newPhase = "transition";
          // Find which article comes next
          for (let i = 0; i < articles.length; i++) {
            if (t < articles[i].start) {
              newIdx = i;
              break;
            }
          }
        } else {
          newPhase = newPhase!;
        }
      }

      setPhase(newPhase);
      setCurrentArticle(newIdx);
    };

    video.addEventListener("timeupdate", onTimeUpdate);
    return () => video.removeEventListener("timeupdate", onTimeUpdate);
  }, [articles]);

  // ── React to phase changes: bg music + transition animations ──
  useEffect(() => {
    const prev = prevPhaseRef.current;
    prevPhaseRef.current = phase;

    if (!playing) return;

    if (phase === "intro" && prev !== "intro") {
      // Entering intro — soft bg music
      fadeInBgMusic(0.15, 500);
    } else if (phase === "transition") {
      // Entering a transition gap — louder bg music + animation
      if (prev !== "transition") {
        fadeInBgMusic(0.5, 300);

        // Trigger animation
        const types: TransitionType[] = ["page-flip", "card-shuffle"];
        const anim = types[transitionIdx.current % types.length];
        transitionIdx.current++;
        setTransition(anim);

        // Clear animation after ANIM_MS, start fading music so it's gone by gap end
        const timer = setTimeout(() => {
          setTransition(null);
          fadeOutBgMusic(1200); // smooth fade over remaining ~1.5s of the gap
        }, ANIM_MS);
        return () => clearTimeout(timer);
      }
    } else if (phase === "article") {
      // Entering article reading — ensure music is off (safety net)
      if (prev === "transition" || prev === "intro") {
        fadeOutBgMusic(400);
      }
    } else if (phase === "outro" && prev !== "outro") {
      // Entering outro — soft bg music
      fadeInBgMusic(0.15, 500);
    }
  }, [phase, playing, fadeInBgMusic, fadeOutBgMusic]);

  // ── Also trigger animation on article index change (safety net) ──
  useEffect(() => {
    const prev = prevArticleRef.current;
    if (prev !== currentArticle && phase === "transition") {
      prevArticleRef.current = currentArticle;
    } else {
      prevArticleRef.current = currentArticle;
    }
  }, [currentArticle, phase]);

  const handlePlay = () => {
    const video = videoRef.current;
    if (!video) return;
    setCountdown(null);
    ensureBgAudioLoaded();
    video.muted = false;

    const onSuccess = () => {
      setPlaying(true);
      animFrameRef.current = requestAnimationFrame(renderFrame);
      fadeInBgMusic(0.15, 500);
      setTimeout(() => setStageRevealed(true), 200);
    };

    video.play().then(onSuccess).catch(() => {
      // Browser blocked unmuted — play muted, unmute on first interaction
      video.muted = true;
      video.play().then(() => {
        setPlaying(true);
        animFrameRef.current = requestAnimationFrame(renderFrame);
        setTimeout(() => setStageRevealed(true), 200);
        const unmute = () => {
          video.muted = false;
          fadeInBgMusic(0.15, 500);
        };
        document.addEventListener("click", unmute, { once: true });
        document.addEventListener("touchstart", unmute, { once: true });
      }).catch(() => {});
    });
  };

  // ── Auto-play with 4-second countdown ──
  // Uses a one-time click listener to gain user gesture, then auto-plays unmuted
  useEffect(() => {
    if (!meta || autoPlayStarted.current) return;
    autoPlayStarted.current = true;
    setCountdown(4);

    // Gain user gesture via a one-time interaction listener on the whole document
    const onGesture = () => { /* captures user gesture for autoplay policy */ };
    document.addEventListener("click", onGesture);
    document.addEventListener("touchstart", onGesture);
    document.addEventListener("keydown", onGesture);

    let count = 4;
    const interval = setInterval(() => {
      count--;
      if (count > 0) {
        setCountdown(count);
      } else {
        clearInterval(interval);
        // Clean up gesture listeners
        document.removeEventListener("click", onGesture);
        document.removeEventListener("touchstart", onGesture);
        document.removeEventListener("keydown", onGesture);

        setCountdown(null);
        // Use handlePlay which handles everything
        handlePlay();
      }
    }, 1000);

    return () => {
      clearInterval(interval);
      document.removeEventListener("click", onGesture);
      document.removeEventListener("touchstart", onGesture);
      document.removeEventListener("keydown", onGesture);
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [meta]);

  const handlePause = useCallback(() => {
    setPlaying(false);
    cancelAnimationFrame(animFrameRef.current);
    fadeOutBgMusic(300);
  }, [fadeOutBgMusic]);

  useEffect(() => {
    const video = videoRef.current;
    if (!video) return;
    const onPlay = () => {
      setPlaying(true);
      animFrameRef.current = requestAnimationFrame(renderFrame);
    };
    const onEnd = () => handlePause();
    video.addEventListener("play", onPlay);
    video.addEventListener("pause", handlePause);
    video.addEventListener("ended", onEnd);
    return () => {
      video.removeEventListener("play", onPlay);
      video.removeEventListener("pause", handlePause);
      video.removeEventListener("ended", onEnd);
      cancelAnimationFrame(animFrameRef.current);
    };
  }, [renderFrame, handlePause]);

  const article = articles[currentArticle] ?? null;

  const transitionClass = transition === "page-flip"
    ? "animate-page-flip"
    : transition === "card-shuffle"
      ? "animate-card-shuffle"
      : "";

  return (
    <>
      <style jsx global>{`
        @keyframes pageFlip {
          0% {
            transform: perspective(1200px) rotateY(-90deg);
            opacity: 0;
          }
          40% {
            transform: perspective(1200px) rotateY(10deg);
            opacity: 1;
          }
          70% {
            transform: perspective(1200px) rotateY(-5deg);
          }
          100% {
            transform: perspective(1200px) rotateY(0deg);
            opacity: 1;
          }
        }
        @keyframes cardShuffle {
          0% {
            transform: translateX(100%) scale(0.85) rotate(4deg);
            opacity: 0;
          }
          50% {
            transform: translateX(-8%) scale(1.03) rotate(-1deg);
            opacity: 1;
          }
          75% {
            transform: translateX(3%) scale(0.99) rotate(0.5deg);
          }
          100% {
            transform: translateX(0) scale(1) rotate(0deg);
            opacity: 1;
          }
        }
        .animate-page-flip {
          animation: pageFlip ${ANIM_MS}ms cubic-bezier(0.22, 1, 0.36, 1) both;
          transform-origin: left center;
        }
        .animate-card-shuffle {
          animation: cardShuffle ${ANIM_MS}ms cubic-bezier(0.22, 1, 0.36, 1) both;
        }
        @keyframes revealPane {
          0% {
            transform: translateX(100%);
            opacity: 0;
          }
          60% {
            transform: translateX(-3%);
            opacity: 1;
          }
          100% {
            transform: translateX(0);
            opacity: 1;
          }
        }
        .animate-reveal-pane {
          animation: revealPane 1.2s cubic-bezier(0.22, 1, 0.36, 1) both;
        }
      `}</style>

      <div ref={containerRef} className={`bg-gradient-to-br from-gray-900 to-gray-800 overflow-hidden shadow-lg ${isFullscreen ? "rounded-none flex flex-col h-screen" : "rounded-2xl"}`}>
        <div className={`relative flex ${isFullscreen ? "flex-1" : ""}`} style={isFullscreen ? undefined : { minHeight: 420 }}>
          {/* Left: Canvas with transparent news reader */}
          <div
            className="relative transition-all duration-1000 ease-in-out"
            style={{ width: stageRevealed ? "50%" : "100%" }}
          >
            <canvas
              ref={canvasRef}
              width={550}
              height={480}
              className="w-full h-full"
            />
            {!playing && (
              <button
                onClick={handlePlay}
                className="absolute inset-0 flex items-center justify-center bg-black/30 transition-opacity hover:bg-black/20"
              >
                {countdown !== null ? (
                  <div className="w-20 h-20 rounded-full bg-white/10 border-2 border-white/40 flex items-center justify-center backdrop-blur-sm">
                    <span className="text-3xl font-black text-white">{countdown}</span>
                  </div>
                ) : (
                  <div className="w-16 h-16 rounded-full bg-white/90 flex items-center justify-center shadow-lg">
                    <svg className="w-7 h-7 text-gray-900 ml-1" fill="currentColor" viewBox="0 0 24 24">
                      <path d="M8 5v14l11-7z" />
                    </svg>
                  </div>
                )}
              </button>
            )}
          </div>

          {/* Right: Article card with transitions — hidden until stage reveal */}
          <div className={`flex flex-col overflow-hidden transition-all duration-1000 ease-in-out ${stageRevealed ? "w-[60%] opacity-100 animate-reveal-pane" : "w-0 opacity-0"}`}>
            {phase === "intro" ? (
              <div className="flex-1 flex items-center justify-center">
                <div className="flex flex-col items-center justify-center w-full h-full">
                  {/* eslint-disable-next-line @next/next/no-img-element */}
                  <img
                    src="/logo.gif"
                    alt="Arasiyal Aayvu"
                    className="w-full h-full object-unset"
                  />
                </div>
              </div>
            ) : phase === "outro" ? (
              <div className="flex-1 flex items-center justify-center p-5">
                <div className="text-center space-y-3">
                  <p className={`font-bold text-amber-400 ${isFullscreen ? "text-xl" : "text-sm"}`}>
                    {isTA ? "நன்றி!" : "Thank you!"}
                  </p>
                  <p className={`text-gray-400 ${isFullscreen ? "text-base" : "text-xs"}`}>
                    {isTA ? "மீண்டும் சந்திப்போம்" : "See you again"}
                  </p>
                </div>
              </div>
            ) : article ? (
              <div
                key={`article-${currentArticle}`}
                className={`flex-1 flex flex-col p-5 ${transitionClass}`}
              >
                {/* Category + SDG badges */}
                <div className="flex items-center gap-2 mb-3">
                  <span className="text-[10px] font-bold px-2.5 py-1 rounded-full bg-amber-500/20 text-amber-400 uppercase tracking-wide">
                    {article.category}
                  </span>
                  {article.sdg_alignment?.slice(0, 3).map((sdg) => (
                    <span
                      key={sdg}
                      className="text-[9px] font-bold px-2 py-0.5 rounded bg-cyan-500/15 text-cyan-400"
                    >
                      {sdg}
                    </span>
                  ))}
                </div>

                {/* Headline */}
                <h3 className={`font-black text-white leading-snug mb-3 ${isFullscreen ? "text-3xl" : "text-lg"}`}>
                  {article.title}
                </h3>

                {/* Divider */}
                <div className="w-10 h-0.5 bg-amber-500/60 rounded-full mb-3" />

                {/* Full summary */}
                {article.summary && (
                  <p className={`text-gray-300 leading-relaxed flex-1 ${isFullscreen ? "text-lg" : "text-sm"}`}>
                    {article.summary}
                  </p>
                )}

                {/* Source + read link */}
                <div className="flex items-center justify-between mt-4 pt-3 border-t border-gray-700/40">
                  <span className="text-[10px] text-gray-500 font-medium">
                    {article.source_name}
                  </span>
                  {article.source_url && (
                    <a
                      href={article.source_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-[10px] font-bold text-amber-400 hover:text-amber-300 transition-colors flex items-center gap-1"
                    >
                      {isTA ? "முழு செய்தி" : "Read full article"}
                      <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                      </svg>
                    </a>
                  )}
                </div>

                {/* Progress dots */}
                {articles.length > 1 && (
                  <div className="flex gap-1.5 mt-3">
                    {articles.map((_, i) => (
                      <div
                        key={i}
                        className={`h-1 rounded-full transition-all ${
                          i === currentArticle
                            ? "w-6 bg-amber-500"
                            : i < currentArticle
                              ? "w-3 bg-amber-500/40"
                              : "w-3 bg-gray-700"
                        }`}
                      />
                    ))}
                  </div>
                )}
              </div>
            ) : (
              <div className="flex-1 flex items-center justify-center">
                <p className="text-sm text-gray-500">
                  {isTA ? "செய்தி வாசிப்பைத் தொடங்க ▶ அழுத்தவும்" : "Press ▶ to start the news"}
                </p>
              </div>
            )}
          </div>

          {/* Hidden video element */}
          {/* eslint-disable-next-line jsx-a11y/media-has-caption */}
          <video
            ref={videoRef}
            src={VIDEO_URL}
            playsInline
            preload="auto"
            className="hidden"
            crossOrigin="anonymous"
          />
        </div>

        {/* Bottom bar */}
        <div className="px-4 py-2 border-t border-gray-700/50 flex items-center justify-between">
          <p className="text-[9px] text-gray-500">
            {isTA ? "தமிழ் செல்வி · AI செய்தி வாசிப்பாளர்" : "TamilSelvi · AI News Reader"} · Gemini · edge-tts · Wav2Lip
          </p>
          <div className="flex items-center gap-3">
            {/* Volume */}
            <div className="flex items-center gap-1.5">
              <button
                onClick={() => {
                  const v = videoRef.current;
                  if (!v) return;
                  const next = !muted;
                  setMuted(next);
                  v.muted = next;
                }}
                className="text-gray-400 hover:text-white transition-colors"
                title={muted ? "Unmute" : "Mute"}
              >
                {muted || volume === 0 ? (
                  <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24"><path d="M16.5 12c0-1.77-1.02-3.29-2.5-4.03v2.21l2.45 2.45c.03-.2.05-.41.05-.63zm2.5 0c0 .94-.2 1.82-.54 2.64l1.51 1.51A8.8 8.8 0 0021 12c0-4.28-2.99-7.86-7-8.77v2.06c2.89.86 5 3.54 5 6.71zM4.27 3L3 4.27 7.73 9H3v6h4l5 5v-6.73l4.25 4.25c-.67.52-1.42.93-2.25 1.18v2.06a8.99 8.99 0 003.69-1.81L19.73 21 21 19.73l-9-9L4.27 3zM12 4L9.91 6.09 12 8.18V4z"/></svg>
                ) : volume < 0.5 ? (
                  <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24"><path d="M18.5 12A4.5 4.5 0 0016 8.97v6.06c1.48-.73 2.5-2.25 2.5-4.03zM5 9v6h4l5 5V4L9 9H5z"/></svg>
                ) : (
                  <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24"><path d="M3 9v6h4l5 5V4L7 9H3zm13.5 3A4.5 4.5 0 0014 8.97v6.06c1.48-.73 2.5-2.25 2.5-4.03zM14 3.23v2.06c2.89.86 5 3.54 5 6.71s-2.11 5.85-5 6.71v2.06c4.01-.91 7-4.49 7-8.77s-2.99-7.86-7-8.77z"/></svg>
                )}
              </button>
              <input
                type="range"
                min={0}
                max={1}
                step={0.05}
                value={muted ? 0 : volume}
                onChange={(e) => {
                  const val = parseFloat(e.target.value);
                  setVolume(val);
                  setMuted(val === 0);
                  const v = videoRef.current;
                  if (v) {
                    v.volume = val;
                    v.muted = val === 0;
                  }
                }}
                className="w-16 h-1 accent-amber-500 cursor-pointer"
              />
            </div>
            {/* Play/Pause */}
            <button
              onClick={() => {
                const v = videoRef.current;
                if (v) { v.paused ? handlePlay() : v.pause(); }
              }}
              className="text-gray-400 hover:text-white transition-colors"
            >
              {playing ? (
                <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg>
              ) : (
                <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24"><path d="M8 5v14l11-7z"/></svg>
              )}
            </button>
            <button
              onClick={toggleFullscreen}
              className="text-gray-400 hover:text-white transition-colors"
              title={isFullscreen ? "Exit fullscreen" : "Fullscreen"}
            >
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
        </div>
      </div>
    </>
  );
}
