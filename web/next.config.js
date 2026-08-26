/** @type {import('next').NextConfig} */
const nextConfig = {
  // Phase 1 web: same-origin proxy in dev so the browser can call the gateway
  // on 8000 without CORS preflights. The helper ``rewrite`` strips the
  // ``/api/v1`` prefix and forwards the rest to ``AIDP_GATEWAY_URL``.
  async rewrites() {
    const gateway =
      process.env.AIDP_GATEWAY_URL || "http://127.0.0.1:8000";
    return [
      {
        source: "/api/v1/:path*",
        destination: `${gateway.replace(/\/$/, "")}/api/v1/:path*`,
      },
    ];
  },
  reactStrictMode: true,
};

module.exports = nextConfig;

