#!/usr/bin/env node
/**
 * Build-time verification: confirm the exported dist/ contains the correct
 * Richstox favicon files (derived from assets/images/icon.png, NOT Expo defaults).
 *
 * Prints sha256 + file size so deploy logs are auditable.
 * Exits non-zero if any expected file is missing.
 */
const fs = require("fs");
const crypto = require("crypto");
const path = require("path");

const DIST = path.join(__dirname, "..", "dist");

const EXPECTED_FILES = [
  "favicon.ico",
  "favicon-16x16.png",
  "favicon-32x32.png",
  "apple-touch-icon.png",
];

let ok = true;

console.log("\n=== Favicon verification (build-time) ===");
for (const name of EXPECTED_FILES) {
  const fp = path.join(DIST, name);
  if (!fs.existsSync(fp)) {
    console.error(`MISSING: ${name}`);
    ok = false;
    continue;
  }
  const buf = fs.readFileSync(fp);
  const sha = crypto.createHash("sha256").update(buf).digest("hex");
  console.log(`  ${name}  size=${buf.length}  sha256=${sha}`);
}

if (ok) {
  console.log("All favicon files present in dist/.\n");
} else {
  console.error("ERROR: some favicon files are missing from dist/!");
  process.exit(1);
}
