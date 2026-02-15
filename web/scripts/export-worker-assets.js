const fs = require("fs");
const path = require("path");

const webRoot = path.resolve(__dirname, "..");
const outDir = path.join(webRoot, "out");
const assetsDir = path.join(webRoot, "cloudflare-worker", "assets");

if (!fs.existsSync(outDir)) {
  console.error("Missing static export output. Run `next build` first.");
  process.exit(1);
}

fs.mkdirSync(assetsDir, { recursive: true });
for (const entry of fs.readdirSync(assetsDir)) {
  if (entry === ".gitkeep") {
    continue;
  }
  fs.rmSync(path.join(assetsDir, entry), { recursive: true, force: true });
}

fs.cpSync(outDir, assetsDir, { recursive: true });
console.log(`Copied ${outDir} -> ${assetsDir}`);
