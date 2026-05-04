"use client";

// ── Types (shared across politicians page + constituency page) ───────────────

export interface TimelineEntry {
  year: number | null;
  constituency_slug?: string;
  constituency?: string;
  party?: string;
  won?: boolean | null;
  votes?: number | null;
  vote_share_pct?: number | null;
  assets_cr?: number | null;
  movable_assets_cr?: number | null;
  immovable_assets_cr?: number | null;
  liabilities_cr?: number | null;
  net_assets_cr?: number | null;
  is_crorepati?: boolean | null;
  criminal_cases?: Array<Record<string, unknown>>;
  criminal_cases_total?: number | null;
  criminal_severity?: string | null;
  education?: string | null;
  education_tier?: string | null;
  source_url?: string | null;
  source_doc_id?: string | null;
  source_collection?: string | null;
  affidavit_url?: string | null;
  eci_photo_url?: string | null;
}

export interface PoliticianProfile {
  doc_id: string;
  canonical_name: string;
  aliases: string[];
  photo_url?: string | null;
  gender?: string | null;
  dob?: string | null;
  education?: string | null;
  age?: number | null;
  timeline: TimelineEntry[];
  current_party?: string | null;
  current_constituency?: string | null;
  current_constituency_slug?: string | null;
  win_count?: number | null;
  loss_count?: number | null;
  total_contested?: number | null;
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function titleCase(s: string): string {
  return s.toLowerCase().replace(/(^|\s|\.)\S/g, (c) => c.toUpperCase());
}

function fCr(v: number | null | undefined): string {
  if (v == null) return "—";
  return "₹" + v.toLocaleString("en-IN", { maximumFractionDigits: 2 }) + " Cr";
}

const SEVERITY_COLOR: Record<string, string> = {
  CLEAN: "text-emerald-600",
  MINOR: "text-amber-600",
  MODERATE: "text-orange-600",
  SERIOUS: "text-rose-600",
};

// ── Modal ────────────────────────────────────────────────────────────────────

interface ProfileModalProps {
  profile: PoliticianProfile;
  onClose: () => void;
}

export function ProfileModal({ profile, onClose }: ProfileModalProps) {
  return (
    <div
      className="fixed inset-0 z-50 flex items-end sm:items-center justify-center bg-black/50 backdrop-blur-sm p-0 sm:p-4"
      onClick={onClose}
    >
      <div
        className="w-full sm:max-w-lg bg-white rounded-t-2xl sm:rounded-2xl shadow-2xl border border-gray-200 max-h-[90vh] overflow-y-auto"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header with photo + name */}
        <div className="bg-gray-50 p-5 flex items-center gap-4 border-b border-gray-200 rounded-t-2xl">
          {profile.photo_url ? (
            // eslint-disable-next-line @next/next/no-img-element
            <img src={profile.photo_url} alt="" className="w-16 h-20 object-cover rounded-lg shadow-sm" />
          ) : (
            <div className="w-16 h-20 bg-gray-200 rounded-lg flex items-center justify-center text-gray-400 text-xl">👤</div>
          )}
          <div className="min-w-0 flex-1">
            <h2 className="text-lg font-black text-gray-900 truncate">{titleCase(profile.canonical_name)}</h2>
            <p className="text-sm text-gray-600">{profile.current_party ?? "—"}</p>
            {profile.current_constituency && (
              <p className="text-xs text-gray-400">{profile.current_constituency}</p>
            )}
          </div>
          <button
            type="button"
            onClick={onClose}
            className="text-gray-400 hover:text-gray-700 text-2xl leading-none p-1 -m-1 transition-colors self-start"
          >
            ×
          </button>
        </div>

        <div className="p-5 space-y-5">
          {/* Quick stats */}
          <div className="grid grid-cols-3 gap-3 text-center">
            <div className="bg-gray-50 rounded-xl p-3">
              <p className="text-[10px] text-gray-500 font-semibold">Gender</p>
              <p className="text-sm font-black text-gray-900">{profile.gender ?? "—"}</p>
            </div>
            <div className="bg-gray-50 rounded-xl p-3">
              <p className="text-[10px] text-gray-500 font-semibold">Age</p>
              <p className="text-sm font-black text-gray-900">{profile.age ?? "—"}</p>
            </div>
            <div className="bg-gray-50 rounded-xl p-3">
              <p className="text-[10px] text-gray-500 font-semibold">Education</p>
              <p className="text-sm font-black text-gray-900">{profile.education ?? "—"}</p>
            </div>
          </div>

          {/* Win/Loss record */}
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-2">
              <span className="text-emerald-600 font-black text-lg">{profile.win_count ?? 0}</span>
              <span className="text-xs text-gray-500">Wins</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-rose-500 font-black text-lg">{profile.loss_count ?? 0}</span>
              <span className="text-xs text-gray-500">Losses</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-gray-700 font-black text-lg">{profile.total_contested ?? 0}</span>
              <span className="text-xs text-gray-500">Contested</span>
            </div>
          </div>

          {/* Asset Information */}
          {(() => {
            const withAssets = [...profile.timeline]
              .filter((t) => t.assets_cr != null)
              .sort((a, b) => (b.year ?? 0) - (a.year ?? 0));
            if (withAssets.length === 0) return null;
            const latest = withAssets[0];
            const oldest = withAssets[withAssets.length - 1];
            const hasGrowth = withAssets.length >= 2 && oldest.assets_cr != null && latest.assets_cr != null && oldest.assets_cr > 0;
            const growthPct = hasGrowth ? ((latest.assets_cr! - oldest.assets_cr!) / oldest.assets_cr! * 100) : null;

            return (
              <div>
                <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-3">Asset Information</p>
                <div className="grid grid-cols-2 gap-3">
                  <div className="bg-gray-50 rounded-xl p-3">
                    <p className="text-[10px] text-gray-500 font-semibold">Total Assets</p>
                    <p className="text-sm font-black text-gray-900">{fCr(latest.assets_cr)}</p>
                    <p className="text-[9px] text-gray-400">as of {latest.year}</p>
                  </div>
                  <div className="bg-gray-50 rounded-xl p-3">
                    <p className="text-[10px] text-gray-500 font-semibold">Liabilities</p>
                    <p className="text-sm font-black text-gray-900">{fCr(latest.liabilities_cr)}</p>
                    <p className="text-[9px] text-gray-400">as of {latest.year}</p>
                  </div>
                  {latest.movable_assets_cr != null && (
                    <div className="bg-gray-50 rounded-xl p-3">
                      <p className="text-[10px] text-gray-500 font-semibold">Movable Assets</p>
                      <p className="text-sm font-black text-gray-900">{fCr(latest.movable_assets_cr)}</p>
                    </div>
                  )}
                  {latest.immovable_assets_cr != null && (
                    <div className="bg-gray-50 rounded-xl p-3">
                      <p className="text-[10px] text-gray-500 font-semibold">Immovable Assets</p>
                      <p className="text-sm font-black text-gray-900">{fCr(latest.immovable_assets_cr)}</p>
                    </div>
                  )}
                </div>
                {hasGrowth && growthPct != null && (
                  <div className={`mt-3 rounded-lg px-3 py-2 text-[11px] font-semibold ${
                    growthPct >= 0
                      ? "bg-amber-50 border border-amber-200 text-amber-800"
                      : "bg-emerald-50 border border-emerald-200 text-emerald-800"
                  }`}>
                    📊 Asset growth: {fCr(oldest.assets_cr)} ({oldest.year}) → {fCr(latest.assets_cr)} ({latest.year})
                    {" · "}
                    <span className="font-black">{growthPct >= 0 ? "+" : ""}{growthPct.toFixed(0)}%</span>
                    {" "}over {(latest.year ?? 0) - (oldest.year ?? 0)} years
                  </div>
                )}
              </div>
            );
          })()}

          {/* Timeline */}
          {profile.timeline.length > 0 && (
            <div>
              <p className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-3">Political Timeline</p>
              <div className="space-y-3">
                {[...profile.timeline].sort((a, b) => (b.year ?? 0) - (a.year ?? 0)).map((t, i) => (
                  <div key={i} className="bg-gray-50 rounded-xl p-3 border border-gray-100">
                    <div className="flex items-center justify-between gap-2 mb-2">
                      <div className="flex items-center gap-2">
                        <span className="text-sm font-black text-gray-900">{t.year}</span>
                        <span className="text-xs font-semibold text-gray-600">{t.party}</span>
                      </div>
                      {t.won === true && (
                        <span className="text-[10px] font-bold text-emerald-700 bg-emerald-50 px-2 py-0.5 rounded-full">✓ Won</span>
                      )}
                      {t.won === false && (
                        <span className="text-[10px] font-bold text-rose-700 bg-rose-50 px-2 py-0.5 rounded-full">✗ Lost</span>
                      )}
                      {t.won == null && (
                        <span className="text-[10px] font-bold text-gray-500 bg-gray-100 px-2 py-0.5 rounded-full">—</span>
                      )}
                    </div>
                    <p className="text-xs text-gray-600 mb-2">📍 {t.constituency}</p>
                    {t.votes != null && (
                      <div className="flex items-center gap-3 mb-2">
                        <span className="text-[11px] font-bold text-gray-800">
                          {t.votes.toLocaleString()} votes
                        </span>
                        {t.vote_share_pct != null && (
                          <span className="text-[10px] text-gray-500">
                            ({t.vote_share_pct}%)
                          </span>
                        )}
                      </div>
                    )}
                    <div className="grid grid-cols-2 gap-2 text-[10px]">
                      {t.assets_cr != null && (
                        <div>
                          <span className="text-gray-500">Assets: </span>
                          <span className="font-semibold text-gray-800">{fCr(t.assets_cr)}</span>
                        </div>
                      )}
                      {t.liabilities_cr != null && (
                        <div>
                          <span className="text-gray-500">Liabilities: </span>
                          <span className="font-semibold text-gray-800">{fCr(t.liabilities_cr)}</span>
                        </div>
                      )}
                      {(t.criminal_cases_total ?? 0) > 0 && (
                        <div>
                          <span className="text-gray-500">Criminal: </span>
                          <span className={`font-semibold ${SEVERITY_COLOR[t.criminal_severity ?? ""] ?? "text-gray-800"}`}>
                            {t.criminal_cases_total} cases ({t.criminal_severity})
                          </span>
                        </div>
                      )}
                      {(t.criminal_cases_total ?? 0) === 0 && t.criminal_severity === "CLEAN" && (
                        <div>
                          <span className="text-gray-500">Criminal: </span>
                          <span className="font-semibold text-emerald-600">Clean</span>
                        </div>
                      )}
                      {t.education && (
                        <div>
                          <span className="text-gray-500">Education: </span>
                          <span className="font-semibold text-gray-800">{t.education}</span>
                        </div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
