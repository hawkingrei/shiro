const fs = require("fs");
const path = require("path");

const base = path.join(
  __dirname,
  "..",
  "node_modules",
  "react-diff-viewer-continued",
  "lib"
);

const targets = [
  path.join(base, "esm", "src", "compute-lines.js"),
  path.join(base, "cjs", "src", "compute-lines.js"),
];

const workerFiles = [
  path.join(base, "esm", "src", "computeWorker.js"),
  path.join(base, "cjs", "src", "computeWorker.js"),
];

for (const file of workerFiles) {
  if (!fs.existsSync(file)) {
    throw new Error(`missing worker file: ${file}`);
  }
}

for (const target of targets) {
  if (!fs.existsSync(target)) {
    throw new Error(`missing compute lines file: ${target}`);
  }
  const content = fs.readFileSync(target, "utf8");
  if (!content.includes("./computeWorker.js")) {
    throw new Error(`worker path not patched in ${target}`);
  }
  if (content.includes("./computeWorker.ts")) {
    throw new Error(`unexpected ts worker path in ${target}`);
  }
}

console.log("compute worker patch verified");
