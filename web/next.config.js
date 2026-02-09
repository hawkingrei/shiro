/** @type {import('next').NextConfig} */
const path = require("path");

const basePath = process.env.NEXT_PUBLIC_BASE_PATH || "";
const localComputeLines = path.resolve(
  __dirname,
  "vendor/react-diff-viewer-continued/compute-lines.js"
);

const workerAlias = {
  "react-diff-viewer-continued/lib/esm/src/compute-lines.js": localComputeLines,
  "react-diff-viewer-continued/lib/esm/src/computeWorker.ts":
    "react-diff-viewer-continued/lib/esm/src/computeWorker.js",
  "react-diff-viewer-continued/lib/cjs/src/computeWorker.ts":
    "react-diff-viewer-continued/lib/cjs/src/computeWorker.js",
};

const nextConfig = {
  output: "export",
  trailingSlash: true,
  basePath,
  turbopack: {
    resolveAlias: workerAlias,
  },
  experimental: {
    extensionAlias: {
      ".ts": [".ts", ".js"],
      ".tsx": [".tsx", ".jsx", ".js"],
    },
  },
  webpack: (config) => {
    config.resolve = config.resolve || {};
    config.resolve.alias = {
      ...(config.resolve.alias || {}),
      ...workerAlias,
    };
    config.resolve.extensionAlias = {
      ...(config.resolve.extensionAlias || {}),
      ".ts": [".ts", ".js"],
      ".tsx": [".tsx", ".jsx", ".js"],
    };
    return config;
  },
};

module.exports = nextConfig;
