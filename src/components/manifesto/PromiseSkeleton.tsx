export function PromiseCardSkeleton() {
  return (
    <div className="rounded-xl border border-gray-200 bg-white p-4 flex flex-col gap-3 animate-pulse">
      {/* Party tag + status row */}
      <div className="flex items-center justify-between gap-2">
        <div className="h-5 w-16 rounded-full bg-gray-200" />
        <div className="h-5 w-20 rounded-full bg-gray-200" />
      </div>
      {/* Promise text — two lines */}
      <div className="space-y-1.5">
        <div className="h-3.5 w-full rounded bg-gray-200" />
        <div className="h-3.5 w-4/5 rounded bg-gray-200" />
      </div>
      {/* Stance chip */}
      <div className="h-5 w-28 rounded-full bg-gray-200" />
      {/* Show source link */}
      <div className="h-3 w-32 rounded bg-gray-200 mt-1" />
    </div>
  );
}

interface ComparisonSkeletonProps {
  rows?: number;
}

export function ComparisonSkeleton({ rows = 3 }: ComparisonSkeletonProps) {
  return (
    <div className="space-y-4">
      {/* Pillar context banner */}
      <div className="rounded-xl bg-gray-100 border border-gray-200 px-4 py-3 flex items-center gap-3 animate-pulse">
        <div className="h-8 w-8 rounded-full bg-gray-200 shrink-0" />
        <div className="space-y-1.5 flex-1">
          <div className="h-3 w-24 rounded bg-gray-200" />
          <div className="h-3.5 w-64 rounded bg-gray-200" />
        </div>
      </div>

      {/* Column headers */}
      <div className="grid grid-cols-2 gap-3">
        {[0, 1].map((i) => (
          <div key={i} className="rounded-xl border-2 border-gray-200 px-4 py-2.5 text-center animate-pulse">
            <div className="h-6 w-16 rounded bg-gray-200 mx-auto mb-1" />
            <div className="h-3 w-20 rounded bg-gray-200 mx-auto" />
          </div>
        ))}
      </div>

      {/* Cards */}
      <div className="grid grid-cols-2 gap-3">
        {[0, 1].map((col) => (
          <div key={col} className="space-y-3">
            {Array.from({ length: rows }).map((_, i) => (
              <PromiseCardSkeleton key={i} />
            ))}
          </div>
        ))}
      </div>
    </div>
  );
}
