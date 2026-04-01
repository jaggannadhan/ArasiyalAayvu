export function ConstituencySkeleton() {
  return (
    <div className="space-y-4 animate-pulse">
      {/* MLA Card skeleton */}
      <div className="bg-white rounded-2xl border border-gray-200 p-5 space-y-4">
        <div className="flex items-start gap-4">
          <div className="w-14 h-14 rounded-full bg-gray-200 shrink-0" />
          <div className="flex-1 space-y-2">
            <div className="h-3 w-24 rounded bg-gray-200" />
            <div className="h-5 w-40 rounded bg-gray-200" />
            <div className="h-4 w-28 rounded-full bg-gray-200" />
          </div>
        </div>
        <div className="grid grid-cols-3 gap-3">
          {[0,1,2].map(i => (
            <div key={i} className="space-y-1.5">
              <div className="h-3 w-12 rounded bg-gray-200" />
              <div className="h-7 rounded-lg bg-gray-200" />
            </div>
          ))}
        </div>
      </div>

      {/* Socio panel skeleton */}
      <div className="bg-white rounded-2xl border border-gray-200 p-5 space-y-4">
        <div className="h-4 w-48 rounded bg-gray-200" />
        <div className="grid grid-cols-2 gap-4">
          {[0,1,2,3,4,5].map(i => (
            <div key={i} className="space-y-1.5">
              <div className="flex justify-between">
                <div className="h-3 w-28 rounded bg-gray-200" />
                <div className="h-3 w-10 rounded bg-gray-200" />
              </div>
              <div className="h-2 w-full rounded-full bg-gray-200" />
            </div>
          ))}
        </div>
      </div>

      {/* Promise matrix skeleton */}
      <div className="bg-white rounded-2xl border border-gray-200 p-5 space-y-3">
        <div className="h-4 w-36 rounded bg-gray-200" />
        <div className="flex gap-2">
          {[0,1,2,3].map(i => (
            <div key={i} className="h-6 w-20 rounded-full bg-gray-200" />
          ))}
        </div>
        {[0,1,2].map(i => (
          <div key={i} className="rounded-xl border border-gray-100 bg-gray-50 p-3 flex gap-3">
            <div className="w-5 h-5 rounded bg-gray-200 shrink-0 mt-0.5" />
            <div className="flex-1 space-y-1.5">
              <div className="h-3.5 w-full rounded bg-gray-200" />
              <div className="h-3.5 w-3/4 rounded bg-gray-200" />
              <div className="h-5 w-20 rounded-full bg-gray-200" />
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
