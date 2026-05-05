"use client";

import { useEffect } from "react";

export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error("[GlobalError]", error);
  }, [error]);

  return (
    <main className="min-h-full bg-gray-50 flex items-center justify-center px-4">
      <div className="max-w-sm w-full text-center space-y-4 py-16">
        <p className="text-3xl">⚠️</p>
        <h1 className="text-lg font-black text-gray-900">Something went wrong</h1>
        <p className="text-sm text-gray-600">
          An unexpected error occurred. This has been logged.
        </p>
        <button
          onClick={reset}
          className="inline-flex items-center gap-1.5 text-sm font-bold px-5 py-2.5 rounded-full bg-gray-900 text-white hover:bg-gray-700 transition-colors"
        >
          Try again
        </button>
      </div>
    </main>
  );
}
