/**
 * Card-shaped skeleton placeholder with a light band that sweeps across the
 * whole card. Mimics the content shape of the State Vitals section cards
 * (header row + a few body rows) so the loading state looks like the real
 * content outline, not empty boxes.
 */

interface SkeletonBarProps {
  widthClass?: string;
  heightClass?: string;
  className?: string;
}

function SkeletonBar({
  widthClass = "w-full",
  heightClass = "h-3",
  className = "",
}: SkeletonBarProps) {
  return <div className={`bg-gray-200 rounded ${widthClass} ${heightClass} ${className}`} />;
}

interface SkeletonCardProps {
  /** Number of body rows (excluding header). Defaults to 3. */
  rows?: number;
  heightClass?: string;
}

export function SkeletonCard({ rows = 3, heightClass }: SkeletonCardProps) {
  return (
    // The skeleton-shimmer utility adds a single light band that sweeps
    // left-to-right across the whole card every ~1.4s.
    <div
      className={`skeleton-shimmer bg-white rounded-2xl border border-gray-200 p-4 flex flex-col gap-3 ${heightClass ?? ""}`}
      aria-busy="true"
      aria-label="Loading data"
    >
      {/* Header — shorter bar + tiny "source" bar on the right */}
      <div className="flex items-center gap-2">
        <SkeletonBar widthClass="w-28" heightClass="h-3" />
        <SkeletonBar widthClass="w-16" heightClass="h-2.5" className="ml-auto" />
      </div>

      {/* Body rows of varying width */}
      {Array.from({ length: rows }).map((_, i) => (
        <div key={i} className="flex items-center gap-3">
          <SkeletonBar
            widthClass={i % 3 === 0 ? "w-2/3" : i % 3 === 1 ? "w-5/6" : "w-1/2"}
            heightClass="h-3.5"
          />
          <SkeletonBar widthClass="w-12" heightClass="h-3" className="ml-auto" />
        </div>
      ))}
    </div>
  );
}

/**
 * Stack of SkeletonCards — drop in place of a whole section while the initial
 * data fetch is in flight.
 */
export function SkeletonSection({ count = 3 }: { count?: number }) {
  return (
    <div className="space-y-3">
      {Array.from({ length: count }).map((_, i) => (
        <SkeletonCard key={i} rows={i === 0 ? 4 : 3} />
      ))}
    </div>
  );
}
