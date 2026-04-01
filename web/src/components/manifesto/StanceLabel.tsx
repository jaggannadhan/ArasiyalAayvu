"use client";

import { VIBE_META, STATUS_META } from "@/lib/types";
import type { StanceVibe, PromiseStatus } from "@/lib/types";

interface StanceLabelProps {
  vibe: StanceVibe;
  lang?: "en" | "ta";
  size?: "sm" | "md";
}

export function StanceLabel({ vibe, lang = "en", size = "sm" }: StanceLabelProps) {
  const meta = VIBE_META[vibe];
  const label = lang === "ta" ? meta.label : vibe;
  const sizeClass = size === "sm" ? "text-xs px-2 py-0.5" : "text-sm px-3 py-1";
  return (
    <span className={`inline-flex items-center rounded-full font-medium ${sizeClass} ${meta.bg} ${meta.text}`}>
      {label}
    </span>
  );
}

interface StatusBadgeProps {
  status: PromiseStatus;
  lang?: "en" | "ta";
  size?: "sm" | "md";
}

export function StatusBadge({ status, lang = "en", size = "sm" }: StatusBadgeProps) {
  const meta = STATUS_META[status];
  const label = lang === "ta" ? meta.label_ta : meta.label_en;
  const sizeClass = size === "sm" ? "text-xs px-2 py-0.5" : "text-sm px-3 py-1";
  return (
    <span className={`inline-flex items-center rounded-full font-medium ${sizeClass} ${meta.bg} ${meta.text}`}>
      {label}
    </span>
  );
}
