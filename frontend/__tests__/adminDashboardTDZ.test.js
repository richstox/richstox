/**
 * ADMIN DASHBOARD TDZ REGRESSION TEST
 *
 * Validates that bulk-completeness variables (bc, bcHasBaseline, bcMissing,
 * bcGapFree) are declared BEFORE they are referenced in the alerts block.
 *
 * Background: A production crash ("Cannot access 're' before initialization")
 * was caused by these const variables being used before their declaration —
 * a Temporal Dead Zone (TDZ) violation that only surfaced after minification.
 *
 * CI/CD: Run with `npx jest __tests__/adminDashboardTDZ.test.js`
 */

const fs = require('fs');
const path = require('path');

describe('Admin Dashboard: no TDZ on bulk-completeness variables', () => {
  const adminPath = path.join(__dirname, '../app/(tabs)/admin.tsx');
  let lines;

  beforeAll(() => {
    const content = fs.readFileSync(adminPath, 'utf-8');
    lines = content.split('\n');
  });

  // Find the first line index where a pattern appears
  const firstLine = (pattern) => lines.findIndex((l) => pattern.test(l));

  it('should declare `const bc` before the alerts block uses it', () => {
    const declLine = firstLine(/const\s+bc\s*=\s*overview\?\.bulk_completeness/);
    const useLine = firstLine(/if\s*\(\s*!bcHasBaseline\s*&&\s*bc\s*\)/);
    expect(declLine).toBeGreaterThan(-1);
    expect(useLine).toBeGreaterThan(-1);
    expect(declLine).toBeLessThan(useLine);
  });

  it('should declare `const bcHasBaseline` before the alerts block uses it', () => {
    const declLine = firstLine(/const\s+bcHasBaseline\s*=/);
    const useLine = firstLine(/if\s*\(\s*bcHasBaseline\s*&&\s*!bcGapFree\s*\)/);
    expect(declLine).toBeGreaterThan(-1);
    expect(useLine).toBeGreaterThan(-1);
    expect(declLine).toBeLessThan(useLine);
  });

  it('should declare `const bcMissing` before the alerts block uses it', () => {
    const declLine = firstLine(/const\s+bcMissing\s*=/);
    const useLine = firstLine(/bcMissing\s*===\s*1/);
    expect(declLine).toBeGreaterThan(-1);
    expect(useLine).toBeGreaterThan(-1);
    expect(declLine).toBeLessThan(useLine);
  });

  it('should declare `const bcGapFree` before the alerts block uses it', () => {
    const declLine = firstLine(/const\s+bcGapFree\s*=/);
    const useLine = firstLine(/if\s*\(\s*bcHasBaseline\s*&&\s*!bcGapFree\s*\)/);
    expect(declLine).toBeGreaterThan(-1);
    expect(useLine).toBeGreaterThan(-1);
    expect(declLine).toBeLessThan(useLine);
  });
});
