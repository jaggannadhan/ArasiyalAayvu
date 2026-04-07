/** @type {import('next').NextConfig} */
const nextConfig = {
  images: {
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
