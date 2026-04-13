#!/usr/bin/env node
/**
 * Build-time verification: confirm the exported dist/ contains the correct
 * Richstox favicon files that are byte-identical to public/ sources AND that
 * the browser-tab favicons have a transparent (RGBA) background.
 *
 * 1. Compares sha256 hashes between public/ (source of truth) and dist/.
 * 2. Reads the raw PNG bytes of favicon-16x16.png and favicon-32x32.png to
 *    verify: RGBA colour type, ≥ 20 % fully-transparent pixels, and all
 *    four corner pixels have alpha = 0.
 */
const fs = require("fs");
const crypto = require("crypto");
const path = require("path");
const zlib = require("zlib");

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

/* Files that MUST have a transparent RGBA background. */
const TRANSPARENT_PNGS = ["favicon-16x16.png", "favicon-32x32.png"];
const MIN_TRANSPARENT_PCT = 20; // at least 20 % of pixels fully transparent

// ── helpers ────────────────────────────────────────────────────────────

function sha256(filePath) {
  const buf = fs.readFileSync(filePath);
  return {
    hash: crypto.createHash("sha256").update(buf).digest("hex"),
    size: buf.length,
  };
}

/**
 * Minimal PNG parser – extracts raw RGBA pixel rows from a PNG file.
 * Only supports 8-bit RGBA (color type 6) which is what we generate.
 */
function readPngRGBA(filePath) {
  const buf = fs.readFileSync(filePath);

  // Verify PNG signature
  const sig = buf.slice(0, 8);
  if (sig.toString("hex") !== "89504e470d0a1a0a") {
    throw new Error(`${filePath}: not a valid PNG`);
  }

  let offset = 8;
  let width = 0;
  let height = 0;
  let bitDepth = 0;
  let colorType = 0;
  const idatChunks = [];

  while (offset < buf.length) {
    const len = buf.readUInt32BE(offset);
    const type = buf.slice(offset + 4, offset + 8).toString("ascii");
    const data = buf.slice(offset + 8, offset + 8 + len);
    offset += 12 + len; // length + type + data + crc

    if (type === "IHDR") {
      width = data.readUInt32BE(0);
      height = data.readUInt32BE(4);
      bitDepth = data[8];
      colorType = data[9];
    } else if (type === "IDAT") {
      idatChunks.push(data);
    } else if (type === "IEND") {
      break;
    }
  }

  if (colorType !== 6) {
    throw new Error(
      `${filePath}: expected RGBA color type 6 but got ${colorType}`
    );
  }
  if (bitDepth !== 8) {
    throw new Error(
      `${filePath}: expected 8-bit depth but got ${bitDepth}`
    );
  }

  const compressed = Buffer.concat(idatChunks);
  const raw = zlib.inflateSync(compressed);

  // Each row: 1 filter byte + width * 4 (RGBA)
  const stride = 1 + width * 4;
  const pixels = Buffer.alloc(width * height * 4);

  // Reconstruct with PNG row filters (Sub, Up, Average, Paeth)
  const prevRow = Buffer.alloc(width * 4);
  for (let y = 0; y < height; y++) {
    const filter = raw[y * stride];
    const rowStart = y * stride + 1;
    const curRow = Buffer.alloc(width * 4);

    for (let x = 0; x < width * 4; x++) {
      const rawByte = raw[rowStart + x];
      const a = x >= 4 ? curRow[x - 4] : 0; // left
      const b = prevRow[x]; // above
      const c = x >= 4 ? prevRow[x - 4] : 0; // upper-left

      let val;
      switch (filter) {
        case 0:
          val = rawByte;
          break;
        case 1:
          val = (rawByte + a) & 0xff;
          break;
        case 2:
          val = (rawByte + b) & 0xff;
          break;
        case 3:
          val = (rawByte + ((a + b) >> 1)) & 0xff;
          break;
        case 4: {
          // Paeth
          const p = a + b - c;
          const pa = Math.abs(p - a);
          const pb = Math.abs(p - b);
          const pc = Math.abs(p - c);
          const pr = pa <= pb && pa <= pc ? a : pb <= pc ? b : c;
          val = (rawByte + pr) & 0xff;
          break;
        }
        default:
          throw new Error(`Unknown PNG row filter ${filter} at row ${y}`);
      }
      curRow[x] = val;
    }

    curRow.copy(pixels, y * width * 4);
    curRow.copy(prevRow);
  }

  return { width, height, pixels };
}

/**
 * Returns the alpha value of the pixel at (x, y).
 */
function alphaAt(img, x, y) {
  const idx = (y * img.width + x) * 4 + 3;
  return img.pixels[idx];
}

// ── main checks ────────────────────────────────────────────────────────

let ok = true;

console.log("\n=== Favicon verification (build-time) ===");

// 1) Hash comparison
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
  console.log(
    `  ${name}  dist_size=${dist.size}  dist_sha256=${dist.hash}  ${status}`
  );
  if (!match) {
    console.error(`    ↳ public/${name} sha256=${pub.hash} (expected)`);
    ok = false;
  }
}

// 2) Transparency verification for browser-tab favicons
console.log("\n=== Transparency verification ===");
for (const name of TRANSPARENT_PNGS) {
  const filePath = path.join(DIST, name);
  if (!fs.existsSync(filePath)) {
    // Already flagged above
    continue;
  }

  try {
    const img = readPngRGBA(filePath);
    const totalPixels = img.width * img.height;

    // Count fully-transparent pixels (alpha === 0)
    let transparentCount = 0;
    for (let i = 3; i < img.pixels.length; i += 4) {
      if (img.pixels[i] === 0) transparentCount++;
    }
    const transparentPct = ((transparentCount / totalPixels) * 100).toFixed(1);

    // Corner alpha values
    const corners = {
      topLeft: alphaAt(img, 0, 0),
      topRight: alphaAt(img, img.width - 1, 0),
      bottomLeft: alphaAt(img, 0, img.height - 1),
      bottomRight: alphaAt(img, img.width - 1, img.height - 1),
    };
    const allCornersTransparent = Object.values(corners).every(
      (a) => a === 0
    );

    console.log(`  ${name}  ${img.width}×${img.height}  RGBA`);
    console.log(
      `    transparent pixels: ${transparentCount}/${totalPixels} (${transparentPct}%)`
    );
    console.log(`    corners alpha: ${JSON.stringify(corners)}`);

    if (!allCornersTransparent) {
      console.error(`    FAIL: not all corner pixels are fully transparent`);
      ok = false;
    } else {
      console.log(`    corners alpha=0: PASS`);
    }

    if (parseFloat(transparentPct) < MIN_TRANSPARENT_PCT) {
      console.error(
        `    FAIL: only ${transparentPct}% transparent (need ≥${MIN_TRANSPARENT_PCT}%)`
      );
      ok = false;
    } else {
      console.log(`    ≥${MIN_TRANSPARENT_PCT}% transparent: PASS`);
    }
  } catch (err) {
    console.error(`  ${name}: ${err.message}`);
    ok = false;
  }
}

// ── result ─────────────────────────────────────────────────────────────
if (ok) {
  console.log(
    "\nAll favicon files verified: hashes match, transparency confirmed.\n"
  );
} else {
  console.error("\nERROR: favicon verification failed! See above for details.");
  process.exit(1);
}
