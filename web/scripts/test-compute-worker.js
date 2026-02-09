const fs = require("fs");
const path = require("path");

const nextConfig = require("../next.config.js");

const base = path.join(
  __dirname,
  "..",
  "node_modules",
  "react-diff-viewer-continued",
  "lib"
);

const workerFiles = [
  path.join(base, "esm", "src", "computeWorker.js"),
  path.join(base, "cjs", "src", "computeWorker.js"),
];

const expectedAlias = {
  "react-diff-viewer-continued/lib/esm/src/computeWorker.ts":
    "react-diff-viewer-continued/lib/esm/src/computeWorker.js",
  "react-diff-viewer-continued/lib/cjs/src/computeWorker.ts":
    "react-diff-viewer-continued/lib/cjs/src/computeWorker.js",
};

for (const file of workerFiles) {
  if (!fs.existsSync(file)) {
    throw new Error(`missing worker file: ${file}`);
  }
}

const turbopackAlias = nextConfig.turbopack?.resolveAlias;
for (const [from, to] of Object.entries(expectedAlias)) {
  if (!turbopackAlias || turbopackAlias[from] !== to) {
    throw new Error(`turbopack alias missing for ${from}`);
  }
}

const experimentalAlias = nextConfig.experimental?.extensionAlias;
if (
  !experimentalAlias ||
  !Array.isArray(experimentalAlias[".ts"]) ||
  !experimentalAlias[".ts"].includes(".js")
) {
  throw new Error("experimental extensionAlias for .ts must include .js");
}

if (typeof nextConfig.webpack !== "function") {
  throw new Error("webpack config hook is missing");
}

const webpackConfig = nextConfig.webpack({ resolve: {} });
const webpackAlias = webpackConfig.resolve?.alias || {};
for (const [from, to] of Object.entries(expectedAlias)) {
  if (webpackAlias[from] !== to) {
    throw new Error(`webpack alias missing for ${from}`);
  }
}

const webpackExtAlias = webpackConfig.resolve?.extensionAlias || {};
if (
  !Array.isArray(webpackExtAlias[".ts"]) ||
  !webpackExtAlias[".ts"].includes(".js")
) {
  throw new Error("webpack extensionAlias for .ts must include .js");
}

console.log("compute worker config aliases verified");
