/** @type {import('next').NextConfig} */
const nextConfig = {
  images: {
    minimumCacheTTL: 60 * 60 * 24 * 30, // 30 days — photos are immutable once on GCS
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
    ],
  },
};

module.exports = nextConfig;
