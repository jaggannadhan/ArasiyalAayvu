export default function PartyHistoryLoading() {
  return (
    <main className="min-h-full bg-gray-50">
      <header className="bg-white border-b border-gray-200 shadow-sm sticky top-0 z-10">
        <div className="max-w-2xl mx-auto px-4 py-4 flex items-center gap-3">
          <div className="w-5 h-5 rounded bg-gray-200" />
          <div>
            <div className="h-5 w-32 bg-gray-200 rounded animate-pulse" />
            <div className="h-3 w-56 bg-gray-100 rounded animate-pulse mt-1" />
          </div>
        </div>
      </header>

      <div className="max-w-2xl mx-auto px-4 py-6 space-y-4">
        {[1, 2, 3, 4, 5].map((i) => (
          <div
            key={i}
            className="bg-white rounded-2xl border-2 border-gray-200 p-5 animate-pulse"
          >
            <div className="flex items-center gap-4">
              <div className="w-14 h-14 rounded-xl bg-gray-200" />
              <div className="flex-1">
                <div className="h-5 w-20 bg-gray-200 rounded mb-2" />
                <div className="h-4 w-48 bg-gray-100 rounded" />
              </div>
              <div className="w-5 h-5 rounded bg-gray-100" />
            </div>
          </div>
        ))}
      </div>
    </main>
  );
}
