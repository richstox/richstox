#!/usr/bin/env node
/**
 * Build-time verification: confirm the exported dist/ contains the correct
 * Richstox favicon files that are byte-identical to public/ sources.
 *
 * Compares sha256 hashes between public/ (source of truth) and dist/ (deploy
 * artifact).  Exits non-zero if any file is missing or hashes diverge
 * (which would mean Expo regenerated a default over our custom icons).
 */
const fs = require("fs");
const crypto = require("crypto");
const path = require("path");

const PUBLIC = path.join(__dirname, "..", "public");
const DIST = path.join(__dirname, "..", "dist");

const EXPECTED_FILES = [
  "favicon.ico",
  "favicon-16x16.png",
  "favicon-32x32.png",
  "apple-touch-icon.png",
  "icon-192x192.png",
  "icon-512x512.png",
  "manifest.webmanifest",
];

function sha256(filePath) {
  const buf = fs.readFileSync(filePath);
  return { hash: crypto.createHash("sha256").update(buf).digest("hex"), size: buf.length };
}

let ok = true;

console.log("\n=== Favicon verification (build-time) ===");
for (const name of EXPECTED_FILES) {
  const distPath = path.join(DIST, name);
  const pubPath = path.join(PUBLIC, name);

  if (!fs.existsSync(pubPath)) {
    console.error(`MISSING source: public/${name}`);
    ok = false;
    continue;
  }
  if (!fs.existsSync(distPath)) {
    console.error(`MISSING in dist: ${name}`);
    ok = false;
    continue;
  }

  const pub = sha256(pubPath);
  const dist = sha256(distPath);

  const match = pub.hash === dist.hash;
  const status = match ? "OK" : "MISMATCH";
  console.log(`  ${name}  dist_size=${dist.size}  dist_sha256=${dist.hash}  ${status}`);
  if (!match) {
    console.error(`    ↳ public/${name} sha256=${pub.hash} (expected)`);
    ok = false;
  }
}

if (ok) {
  console.log("All favicon files present in dist/ and match public/ sources.\n");
} else {
  console.error("ERROR: favicon verification failed! See above for details.");
  process.exit(1);
}
