/** @type {import('next').NextConfig} */
const nextConfig = {
  images: {
    // Candidate photos in GCS are uploaded with `public, max-age=31536000,
    // immutable`, so we can safely cache them at the Next.js image-optimizer
    // layer for the full year too. A shorter TTL here was causing the browser
    // to hit the optimizer on every reload even though the underlying blob
    // never changes.
    minimumCacheTTL: 60 * 60 * 24 * 365,   // 1 year
    remotePatterns: [
      {
        protocol: "https",
        hostname: "assembly.tn.gov.in",
      },
      {
        protocol: "https",
        hostname: "storage.googleapis.com",
        pathname: "/naatunadappu-media/**",
      },
      {
        protocol: "https",
        hostname: "results.eci.gov.in",
      },
      {
        protocol: "https",
        hostname: "affidavit.eci.gov.in",
      },
    ],
  },
};

module.exports = nextConfig;
