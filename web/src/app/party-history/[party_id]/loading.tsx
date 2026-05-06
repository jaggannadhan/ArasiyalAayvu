export default function PartyDetailLoading() {
  return (
    <main className="min-h-full bg-gray-50">
      {/* Header skeleton */}
      <header className="bg-white border-b border-gray-200 shadow-sm sticky top-0 z-20">
        <div className="max-w-2xl mx-auto px-4 py-3 flex items-center gap-3">
          <div className="w-5 h-5 rounded bg-gray-200" />
          <div className="w-7 h-7 rounded-lg bg-gray-200" />
          <div className="h-5 w-40 bg-gray-200 rounded animate-pulse" />
        </div>
      </header>

      <div className="max-w-2xl mx-auto px-4 py-6">
        {/* Founding Hero skeleton */}
        <div className="rounded-3xl p-6 mb-8 border-2 border-gray-200 bg-gray-50 animate-pulse">
          <div className="flex items-start gap-4 mb-4">
            <div className="w-16 h-16 rounded-xl bg-gray-200" />
            <div className="flex-1">
              <div className="h-6 w-48 bg-gray-200 rounded mb-2" />
              <div className="h-4 w-64 bg-gray-100 rounded" />
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3 mb-5">
            <div className="bg-white/70 rounded-xl p-3 h-20" />
            <div className="bg-white/70 rounded-xl p-3 h-20" />
          </div>

          {/* Motto skeleton */}
          <div className="border-l-4 border-gray-200 pl-3 mb-5">
            <div className="h-4 w-72 bg-gray-200 rounded" />
          </div>

          {/* Founders skeleton */}
          <div className="mb-4">
            <div className="h-3 w-20 bg-gray-200 rounded mb-3" />
            <div className="space-y-2">
              {[1, 2, 3].map((i) => (
                <div key={i} className="bg-white/70 rounded-lg p-3 flex items-start gap-3">
                  <div className="w-12 h-12 rounded-full bg-gray-200" />
                  <div className="flex-1">
                    <div className="h-4 w-32 bg-gray-200 rounded mb-1.5" />
                    <div className="h-3 w-24 bg-gray-100 rounded mb-1" />
                    <div className="h-3 w-full bg-gray-100 rounded" />
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Timeline spine skeleton */}
        <div className="py-3 mb-4">
          <div className="flex gap-1">
            {[1, 2, 3, 4, 5, 6, 7, 8].map((i) => (
              <div key={i} className="h-7 w-14 rounded-full bg-gray-200 animate-pulse" />
            ))}
          </div>
        </div>

        {/* Chapter skeletons */}
        <div className="space-y-8">
          {[1, 2, 3].map((i) => (
            <div key={i} className="pl-6 border-l-2 border-gray-200 animate-pulse">
              <div className="h-3 w-24 bg-gray-200 rounded mb-2" />
              <div className="h-5 w-72 bg-gray-200 rounded mb-3" />
              <div className="space-y-2 mb-4">
                <div className="h-3 w-full bg-gray-100 rounded" />
                <div className="h-3 w-full bg-gray-100 rounded" />
                <div className="h-3 w-5/6 bg-gray-100 rounded" />
                <div className="h-3 w-4/6 bg-gray-100 rounded" />
              </div>
              <div className="flex gap-2">
                <div className="h-20 w-20 bg-gray-100 rounded-lg" />
                <div className="h-20 w-20 bg-gray-100 rounded-lg" />
              </div>
            </div>
          ))}
        </div>
      </div>
    </main>
  );
}
