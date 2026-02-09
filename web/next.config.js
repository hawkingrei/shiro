/** @type {import('next').NextConfig} */
const path = require("path");

const webpackLib = (() => {
  try {
    return require("next/dist/compiled/webpack/webpack");
  } catch {
    try {
      return require("webpack");
    } catch {
      return null;
    }
  }
})();

const resolveNormalModuleReplacementPlugin = (webpackModule) => {
  const candidates = [
    webpackModule?.NormalModuleReplacementPlugin,
    webpackModule?.webpack?.NormalModuleReplacementPlugin,
    webpackModule?.default?.NormalModuleReplacementPlugin,
    webpackModule?.default?.webpack?.NormalModuleReplacementPlugin,
  ];
  for (const candidate of candidates) {
    if (typeof candidate === "function") {
      return candidate;
    }
  }

  try {
    const fallback = require("webpack");
    if (typeof fallback?.NormalModuleReplacementPlugin === "function") {
      return fallback.NormalModuleReplacementPlugin;
    }
  } catch {}

  return null;
};

const NormalModuleReplacementPlugin =
  resolveNormalModuleReplacementPlugin(webpackLib);

const basePath = process.env.NEXT_PUBLIC_BASE_PATH || "";
const vendorComputeLinesRel =
  "./vendor/react-diff-viewer-continued/compute-lines.js";
const vendorComputeWorkerRel =
  "./vendor/react-diff-viewer-continued/computeWorker.js";
const localComputeLines = path.resolve(__dirname, vendorComputeLinesRel);
const computeLinesContext = path.join(
  "react-diff-viewer-continued",
  "lib",
  "esm",
  "src"
);

const workerAlias = {
  "react-diff-viewer-continued/lib/esm/src/compute-lines.js": localComputeLines,
  "react-diff-viewer-continued/lib/esm/src/computeWorker.ts":
    "react-diff-viewer-continued/lib/esm/src/computeWorker.js",
  "react-diff-viewer-continued/lib/cjs/src/computeWorker.ts":
    "react-diff-viewer-continued/lib/cjs/src/computeWorker.js",
};

const turbopackAlias = {
  "./compute-lines.js": vendorComputeLinesRel,
  "./computeWorker.ts": vendorComputeWorkerRel,
  "react-diff-viewer-continued/lib/esm/src/compute-lines.js":
    vendorComputeLinesRel,
  "react-diff-viewer-continued/lib/esm/src/computeWorker.ts":
    vendorComputeWorkerRel,
  "react-diff-viewer-continued/lib/cjs/src/computeWorker.ts":
    "react-diff-viewer-continued/lib/cjs/src/computeWorker.js",
};

const nextConfig = {
  output: "export",
  trailingSlash: true,
  basePath,
  turbopack: {
    resolveAlias: turbopackAlias,
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
    if (NormalModuleReplacementPlugin) {
      const replacementPlugin = new NormalModuleReplacementPlugin(
        /^\.\/compute-lines\.js$/,
        (resource) => {
          if (
            resource.context &&
            resource.context.includes(computeLinesContext)
          ) {
            resource.request = localComputeLines;
          }
        }
      );
      replacementPlugin.__shiro_label =
        "replace-react-diff-viewer-compute-lines";
      config.plugins = [...(config.plugins || []), replacementPlugin];
    }
    return config;
  },
};

module.exports = nextConfig;
