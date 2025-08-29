const nextConfig = {
  // Next will now use SWC since the root babel.config.js has been moved under react-native/
  eslint: {
    ignoreDuringBuilds: true,
  },
  typescript: {
    ignoreBuildErrors: true,
  },
  images: {
    unoptimized: true,
  },
  // ... rest of code here ...
};

export default nextConfig;
