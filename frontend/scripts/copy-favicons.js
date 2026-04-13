#!/usr/bin/env node
/**
 * Post-export step: copy the authoritative Richstox favicon files from
 * public/ into dist/ root, overwriting any Expo-generated defaults.
 *
 * Expo's `npx expo export -p web` copies public/ into dist/, but the
 * web.favicon setting in app.json can cause Expo to regenerate and
 * overwrite favicon.ico.  This script guarantees the final dist/
 * contains the pre-generated Richstox favicons.
 */
const fs = require("fs");
const path = require("path");

const PUBLIC = path.join(__dirname, "..", "public");
const DIST = path.join(__dirname, "..", "dist");

const FILES = [
  "favicon.ico",
  "favicon-16x16.png",
  "favicon-32x32.png",
  "apple-touch-icon.png",
];

console.log("\n=== Copying Richstox favicons into dist/ ===");

if (!fs.existsSync(DIST)) {
  console.error("ERROR: dist/ does not exist. Run `npx expo export -p web` first.");
  process.exit(1);
}

for (const name of FILES) {
  const src = path.join(PUBLIC, name);
  const dest = path.join(DIST, name);
  if (!fs.existsSync(src)) {
    console.error(`ERROR: source file missing: public/${name}`);
    process.exit(1);
  }
  fs.copyFileSync(src, dest);
  console.log(`  copied public/${name} → dist/${name}`);
}

console.log("Done.\n");
