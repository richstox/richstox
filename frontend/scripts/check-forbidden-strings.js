#!/usr/bin/env node
/**
 * BINDING: Frontend Forbidden Strings Guard
 * ==========================================
 * This script MUST run in CI and locally on build/test.
 * It fails if any frontend source file contains forbidden patterns
 * that indicate direct external API calls.
 * 
 * FORBIDDEN PATTERNS:
 * - api_token= (EODHD API token parameter)
 * - X-RapidAPI (RapidAPI headers)
 * - fetch("https://eodhd.com/api (direct API calls)
 * - httpx.get("https://eodhd (direct API calls)
 * - Logo/CDN URLs pointing to eodhd.com or eodhistoricaldata.com
 * 
 * ALLOWED (not flagged):
 * - UI text mentions of "EODHD" in admin pipeline docs (apiUrl text fields)
 * - Backend files (this only checks frontend)
 * 
 * Exit codes:
 * 0 = PASS (no violations)
 * 1 = FAIL (violations found - BLOCKS DEPLOY)
 */

const fs = require('fs');
const path = require('path');

// Forbidden patterns that indicate direct API calls
const FORBIDDEN_PATTERNS = [
  {
    pattern: /api_token\s*[=:]/gi,
    description: 'API token parameter (suggests direct API call)',
    allowedContexts: [] // Never allowed in frontend
  },
  {
    pattern: /X-RapidAPI/gi,
    description: 'RapidAPI header',
    allowedContexts: []
  },
  {
    pattern: /fetch\s*\(\s*["'`]https?:\/\/eodhd\.com\/api/gi,
    description: 'Direct fetch to EODHD API',
    allowedContexts: []
  },
  {
    pattern: /axios\s*\.\s*(get|post|put|delete)\s*\(\s*["'`]https?:\/\/eodhd\.com\/api/gi,
    description: 'Direct axios call to EODHD API',
    allowedContexts: []
  },
  {
    pattern: /httpx\s*\.\s*(get|post)\s*\(\s*["'`]https?:\/\/eodhd/gi,
    description: 'Direct httpx call to EODHD',
    allowedContexts: []
  },
  {
    pattern: /\.com\/api\/eod\//gi,
    description: 'EODHD EOD API endpoint',
    allowedContexts: []
  },
  {
    pattern: /\.com\/api\/fundamentals\//gi,
    description: 'EODHD Fundamentals API endpoint',
    allowedContexts: []
  },
  {
    pattern: /\.com\/api\/search\//gi,
    description: 'EODHD Search API endpoint (direct)',
    allowedContexts: []
  },
  {
    pattern: /https?:\/\/eodhd\.com\/img\//gi,
    description: 'Direct EODHD logo/image CDN URL — use /api/logo/{ticker} instead',
    allowedContexts: ['pipeline.tsx']  // Admin docs only
  },
  {
    pattern: /https?:\/\/eodhistoricaldata\.com/gi,
    description: 'Direct EODHD CDN URL — use /api/logo/{ticker} instead',
    allowedContexts: ['pipeline.tsx']  // Admin docs only
  }
];

// Directories to check
const FRONTEND_DIRS = [
  'app',
  'components',
  'contexts',
  'services',
  'hooks',
  'utils',
  'lib'
];

// Extensions to check
const SOURCE_EXTENSIONS = ['.ts', '.tsx', '.js', '.jsx'];

// Directories to skip
const SKIP_DIRS = ['node_modules', '.next', '.expo', '.metro-cache', 'dist', 'build'];

function walkDir(dir, callback) {
  if (!fs.existsSync(dir)) return;
  
  const files = fs.readdirSync(dir);
  
  for (const file of files) {
    const filepath = path.join(dir, file);
    const stat = fs.statSync(filepath);
    
    if (stat.isDirectory()) {
      if (!SKIP_DIRS.includes(file)) {
        walkDir(filepath, callback);
      }
    } else if (stat.isFile()) {
      const ext = path.extname(file).toLowerCase();
      if (SOURCE_EXTENSIONS.includes(ext)) {
        callback(filepath);
      }
    }
  }
}

function checkFile(filepath) {
  const content = fs.readFileSync(filepath, 'utf8');
  const basename = path.basename(filepath);
  const violations = [];
  
  for (const { pattern, description, allowedContexts } of FORBIDDEN_PATTERNS) {
    // Skip if this file is in the allowed list for this pattern
    if (allowedContexts && allowedContexts.length > 0 && allowedContexts.includes(basename)) {
      continue;
    }
    const matches = content.match(pattern);
    if (matches) {
      // Find line numbers
      const lines = content.split('\n');
      for (let i = 0; i < lines.length; i++) {
        if (pattern.test(lines[i])) {
          violations.push({
            file: filepath,
            line: i + 1,
            pattern: description,
            content: lines[i].trim().substring(0, 100)
          });
        }
        // Reset lastIndex for global regex
        pattern.lastIndex = 0;
      }
    }
  }
  
  return violations;
}

function main() {
  console.log('========================================');
  console.log('FRONTEND FORBIDDEN STRINGS GUARD');
  console.log('========================================\n');
  
  const frontendRoot = path.resolve(__dirname, '..');
  const allViolations = [];
  
  for (const dir of FRONTEND_DIRS) {
    const fullPath = path.join(frontendRoot, dir);
    walkDir(fullPath, (filepath) => {
      const violations = checkFile(filepath);
      allViolations.push(...violations);
    });
  }
  
  if (allViolations.length === 0) {
    console.log('✅ PASS: No forbidden strings found in frontend source files\n');
    console.log('Checked directories:', FRONTEND_DIRS.join(', '));
    console.log('Forbidden patterns checked:', FORBIDDEN_PATTERNS.length);
    process.exit(0);
  } else {
    console.log('❌ FAIL: Forbidden strings detected!\n');
    console.log('VIOLATIONS:');
    console.log('-'.repeat(60));
    
    for (const v of allViolations) {
      console.log(`\nFile: ${v.file}`);
      console.log(`Line: ${v.line}`);
      console.log(`Pattern: ${v.pattern}`);
      console.log(`Content: ${v.content}`);
    }
    
    console.log('\n' + '-'.repeat(60));
    console.log(`\nTotal violations: ${allViolations.length}`);
    console.log('\n⛔ BUILD BLOCKED: Fix violations before deploying');
    process.exit(1);
  }
}

main();
