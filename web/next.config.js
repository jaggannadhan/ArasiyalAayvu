/** @type {import('next').NextConfig} */
const nextConfig = {
  images: {
    remotePatterns: [
      {
        protocol: "https",
        hostname: "assembly.tn.gov.in",
      },
    ],
  },
};

module.exports = nextConfig;
