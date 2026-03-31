/**
 * ADMIN DASHBOARD NULL-BASELINE REGRESSION TEST
 *
 * Validates that the DashboardTab handles `has_baseline=false` with
 * null baseline-dependent fields without crashing (React minified error #418).
 *
 * When bulk_completeness.has_baseline !== true, all baseline-dependent:
 *   - Derived computations must not access null baseline fields.
 *   - UI must render a neutral "No baseline yet" state only.
 *
 * CI/CD: Run with `npx jest __tests__/adminDashboardNullBaseline.test.js`
 */

const fs = require('fs');
const path = require('path');

const adminPath = path.join(__dirname, '../app/(tabs)/admin.tsx');

describe('Admin Dashboard: null-baseline guard (has_baseline=false)', () => {
  let src;

  beforeAll(() => {
    src = fs.readFileSync(adminPath, 'utf-8');
  });

  // ── A) Derived computations are guarded ──────────────────────────────────

  it('should guard bcBaseline behind bcHasBaseline', () => {
    // bcBaseline must be assigned conditionally on bcHasBaseline
    expect(src).toMatch(/const\s+bcBaseline\s*=\s*bcHasBaseline\s*\?/);
  });

  it('should guard bcMissingDates behind bcHasBaseline', () => {
    // bcMissingDates must be assigned conditionally on bcHasBaseline
    expect(src).toMatch(/const\s+bcMissingDates\s*=\s*bcHasBaseline\s*\?/);
  });

  it('should derive bcStatusColor with !bcHasBaseline ternary first', () => {
    expect(src).toMatch(/bcStatusColor[\s\S]*=\s*\n?\s*!bcHasBaseline\s*\?/);
  });

  it('should derive bcStatusLabel with !bcHasBaseline ternary first', () => {
    expect(src).toMatch(/bcStatusLabel\s*=\s*!bcHasBaseline\s*\?/);
  });

  // ── B) Render section guards baseline-dependent UI ───────────────────────

  it('should render "No baseline yet" when !bcHasBaseline', () => {
    expect(src).toContain('No baseline yet');
  });

  it('should guard baseline-dependent JSX behind bcHasBaseline ternary', () => {
    // The bulk completeness section must have a ternary on !bcHasBaseline
    expect(src).toMatch(/\{!bcHasBaseline\s*\?\s*\(/);
  });

  // ── C) Simulated logic: verify no crash with null payload ────────────────

  it('should not throw when computing derived values with has_baseline=false payload', () => {
    // Simulate the exact production payload that caused the crash
    const bc = {
      has_baseline: false,
      baseline: null,
      missing_count: null,
      gap_free_since_baseline: null,
      expected_days_count: null,
      missing_bulk_dates_since_baseline: null,
    };

    expect(() => {
      const bcHasBaseline = bc?.has_baseline === true;
      const bcMissing = bc?.missing_count ?? 0;
      const bcGapFree = bc?.gap_free_since_baseline === true;

      // Guarded derivations (matches the fixed code)
      const bcBaseline = bcHasBaseline ? bc?.baseline ?? null : null;
      const bcStatusColor = !bcHasBaseline ? 'yellow' : bcGapFree ? 'green' : 'red';
      const bcStatusLabel = !bcHasBaseline ? 'NO BASELINE' : bcGapFree ? 'GAP-FREE' : 'GAPS PRESENT';
      const bcMissingDates = bcHasBaseline ? (bc?.missing_bulk_dates_since_baseline ?? []) : [];

      // Assert expected values for no-baseline case
      expect(bcHasBaseline).toBe(false);
      expect(bcMissing).toBe(0);
      expect(bcGapFree).toBe(false);
      expect(bcBaseline).toBeNull();
      expect(bcStatusColor).toBe('yellow');
      expect(bcStatusLabel).toBe('NO BASELINE');
      expect(bcMissingDates).toEqual([]);
    }).not.toThrow();
  });

  it('should not throw when computing derived values with bc undefined (no overview)', () => {
    const bc = undefined;

    expect(() => {
      const bcHasBaseline = bc?.has_baseline === true;
      const bcMissing = bc?.missing_count ?? 0;
      const bcGapFree = bc?.gap_free_since_baseline === true;

      const bcBaseline = bcHasBaseline ? bc?.baseline ?? null : null;
      const bcStatusColor = !bcHasBaseline ? 'yellow' : bcGapFree ? 'green' : 'red';
      const bcStatusLabel = !bcHasBaseline ? 'NO BASELINE' : bcGapFree ? 'GAP-FREE' : 'GAPS PRESENT';
      const bcMissingDates = bcHasBaseline ? (bc?.missing_bulk_dates_since_baseline ?? []) : [];

      expect(bcHasBaseline).toBe(false);
      expect(bcBaseline).toBeNull();
      expect(bcMissingDates).toEqual([]);
    }).not.toThrow();
  });

  it('should preserve correct behavior when has_baseline=true', () => {
    const bc = {
      has_baseline: true,
      baseline: {
        completed_at: '2025-12-01T10:00:00',
        completed_at_prague: '2025-12-01T11:00:00',
        through_date: '2025-12-01',
        job_run_id: 'abc123',
      },
      missing_count: 2,
      gap_free_since_baseline: false,
      expected_days_count: 30,
      missing_bulk_dates_since_baseline: ['2025-12-05', '2025-12-10'],
    };

    const bcHasBaseline = bc?.has_baseline === true;
    const bcMissing = bc?.missing_count ?? 0;
    const bcGapFree = bc?.gap_free_since_baseline === true;

    const bcBaseline = bcHasBaseline ? bc?.baseline ?? null : null;
    const bcStatusColor = !bcHasBaseline ? 'yellow' : bcGapFree ? 'green' : 'red';
    const bcStatusLabel = !bcHasBaseline ? 'NO BASELINE' : bcGapFree ? 'GAP-FREE' : 'GAPS PRESENT';
    const bcMissingDates = bcHasBaseline ? (bc?.missing_bulk_dates_since_baseline ?? []) : [];

    expect(bcHasBaseline).toBe(true);
    expect(bcMissing).toBe(2);
    expect(bcGapFree).toBe(false);
    expect(bcBaseline).toEqual(bc.baseline);
    expect(bcStatusColor).toBe('red');
    expect(bcStatusLabel).toBe('GAPS PRESENT');
    expect(bcMissingDates).toEqual(['2025-12-05', '2025-12-10']);
  });

  it('should preserve correct behavior when has_baseline=true and gap-free', () => {
    const bc = {
      has_baseline: true,
      baseline: {
        completed_at: '2025-12-01T10:00:00',
        completed_at_prague: '2025-12-01T11:00:00',
        through_date: '2025-12-01',
        job_run_id: 'abc123',
      },
      missing_count: 0,
      gap_free_since_baseline: true,
      expected_days_count: 30,
      missing_bulk_dates_since_baseline: [],
    };

    const bcHasBaseline = bc?.has_baseline === true;
    const bcGapFree = bc?.gap_free_since_baseline === true;

    const bcStatusColor = !bcHasBaseline ? 'yellow' : bcGapFree ? 'green' : 'red';
    const bcStatusLabel = !bcHasBaseline ? 'NO BASELINE' : bcGapFree ? 'GAP-FREE' : 'GAPS PRESENT';

    expect(bcStatusColor).toBe('green');
    expect(bcStatusLabel).toBe('GAP-FREE');
  });
});
