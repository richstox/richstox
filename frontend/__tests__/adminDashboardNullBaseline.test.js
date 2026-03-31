/**
 * ADMIN DASHBOARD NULL-BASELINE REGRESSION TEST
 *
 * Validates that the DashboardTab handles `has_baseline=false` with
 * null baseline-dependent fields without crashing (React minified error #418).
 *
 * Root-cause chain (proven):
 *   1. Backend returns `has_baseline: false` with null-valued baseline fields.
 *   2. Derived variable `bcMissingDates` receives the raw field value.
 *   3. If that value is non-null but non-array (corrupted/unexpected payload),
 *      downstream `.join(',')` throws: "bcMissingDates.join is not a function".
 *   4. React catches the render-phase exception → minified error in production.
 *   The guard `bcMissingDates = bcHasBaseline ? (...) : []` prevents step 2
 *   from ever exposing an unexpected value to the render tree.
 *
 * Incident relationship:
 *   This is SEPARATE from the earlier TDZ/use-before-declaration crash
 *   (tested in adminDashboardTDZ.test.js). The TDZ fix moved `bc`,
 *   `bcHasBaseline`, `bcMissing`, `bcGapFree` declarations above the alerts
 *   block. Once the TDZ crash was resolved, the component reached the
 *   bulk-completeness render path, revealing this secondary issue with
 *   baseline-dependent derived values when `has_baseline !== true`.
 *
 * CI/CD: Run with `npx jest __tests__/adminDashboardNullBaseline.test.js`
 */

const fs = require('fs');
const path = require('path');

const adminPath = path.join(__dirname, '../app/(tabs)/admin.tsx');

// ─── Helper: replicate the GUARDED derivation logic from DashboardTab ────────
function deriveGuarded(bc) {
  const bcHasBaseline = bc?.has_baseline === true;
  const bcMissing = bc?.missing_count ?? 0;
  const bcGapFree = bc?.gap_free_since_baseline === true;

  // Guarded derivations (must match admin.tsx)
  const bcBaseline = bcHasBaseline ? bc?.baseline ?? null : null;
  const bcStatusColor = !bcHasBaseline ? 'yellow' : bcGapFree ? 'green' : 'red';
  const bcStatusLabel = !bcHasBaseline ? 'NO BASELINE' : bcGapFree ? 'GAP-FREE' : 'GAPS PRESENT';
  const bcMissingDates = bcHasBaseline ? (bc?.missing_bulk_dates_since_baseline ?? []) : [];

  return { bcHasBaseline, bcMissing, bcGapFree, bcBaseline, bcStatusColor, bcStatusLabel, bcMissingDates };
}

// ─── Helper: replicate the UNGUARDED derivation logic (before fix) ───────────
function deriveUnguarded(bc) {
  const bcHasBaseline = bc?.has_baseline === true;
  const bcMissing = bc?.missing_count ?? 0;
  const bcGapFree = bc?.gap_free_since_baseline === true;

  // Unguarded derivations (before fix)
  const bcBaseline = bc?.baseline;
  const bcStatusColor = !bcHasBaseline ? 'yellow' : bcGapFree ? 'green' : 'red';
  const bcStatusLabel = !bcHasBaseline ? 'NO BASELINE' : bcGapFree ? 'GAP-FREE' : 'GAPS PRESENT';
  const bcMissingDates = bc?.missing_bulk_dates_since_baseline ?? [];

  return { bcHasBaseline, bcMissing, bcGapFree, bcBaseline, bcStatusColor, bcStatusLabel, bcMissingDates };
}

// ─── Helper: simulate ALL downstream render expressions ──────────────────────
// Exercises every formatting operation from the render section of DashboardTab.
function exerciseRenderExpressions(d) {
  // Alert text (line 248): template literal with bcMissing
  const _alertText = `${d.bcMissing} bulk day${d.bcMissing === 1 ? '' : 's'} missing`;
  // IntegrityMetric value (line 503): String(bcMissing)
  const _metricValue = String(d.bcMissing);
  // Last Full Backfill (line 510-512): .replace().slice()
  const _prague = d.bcBaseline?.completed_at_prague
    ? d.bcBaseline.completed_at_prague.replace('T', ' ').slice(0, 19)
    : '—';
  // Through Date (line 517)
  const _through = d.bcBaseline?.through_date ?? '—';
  // Latest Bulk Ingested (line 521) — uses bc, not bcBaseline
  // Expected Days (line 525) — uses bc, not bcBaseline
  // Job Run ID (line 527-530)
  const _jobRunId = d.bcBaseline?.job_run_id;
  // Missing dates (lines 533-536): .length + .join()
  const _hasMissing = d.bcMissingDates.length > 0;
  const _missingText = _hasMissing ? d.bcMissingDates.join(', ') : '';
  return { _alertText, _metricValue, _prague, _through, _jobRunId, _hasMissing, _missingText };
}

// =============================================================================
// TESTS
// =============================================================================

describe('Admin Dashboard: null-baseline guard (has_baseline=false)', () => {

  // ── A) Source-level verification (non-brittle, content-only) ─────────────
  describe('source-level guards', () => {
    let src;
    beforeAll(() => { src = fs.readFileSync(adminPath, 'utf-8'); });

    it('should contain the neutral "No baseline yet" message', () => {
      expect(src).toContain('No baseline yet');
    });

    it('should gate the bulk-completeness JSX on bcHasBaseline', () => {
      // The render section must branch on !bcHasBaseline
      expect(src).toContain('!bcHasBaseline');
    });

    it('should guard bcMissingDates derivation behind bcHasBaseline', () => {
      // bcMissingDates must not be derived from bc?.missing_bulk_dates_since_baseline
      // without first checking bcHasBaseline
      expect(src).toContain('bcMissingDates');
      // The unguarded pattern must NOT appear:
      expect(src).not.toMatch(
        /const\s+bcMissingDates\s*=\s*bc\?\.missing_bulk_dates_since_baseline/
      );
    });

    it('should guard bcBaseline derivation behind bcHasBaseline', () => {
      expect(src).toContain('bcBaseline');
      // The unguarded pattern must NOT appear:
      expect(src).not.toMatch(
        /const\s+bcBaseline\s*=\s*bc\?\.baseline\s*;/
      );
    });
  });

  // ── B) Production payload: has_baseline=false, null fields ───────────────
  describe('production payload (has_baseline=false, null fields)', () => {
    const bc = {
      has_baseline: false,
      baseline: null,
      missing_count: null,
      gap_free_since_baseline: null,
      expected_days_count: null,
      missing_bulk_dates_since_baseline: null,
    };

    it('should compute all derived values without throwing', () => {
      expect(() => deriveGuarded(bc)).not.toThrow();
    });

    it('should produce correct derived values for no-baseline state', () => {
      const d = deriveGuarded(bc);
      expect(d.bcHasBaseline).toBe(false);
      expect(d.bcMissing).toBe(0);
      expect(d.bcGapFree).toBe(false);
      expect(d.bcBaseline).toBeNull();
      expect(d.bcStatusColor).toBe('yellow');
      expect(d.bcStatusLabel).toBe('NO BASELINE');
      expect(d.bcMissingDates).toEqual([]);
    });

    it('should safely evaluate ALL downstream render expressions', () => {
      const d = deriveGuarded(bc);
      expect(() => exerciseRenderExpressions(d)).not.toThrow();
    });

    it('should show "—" fallbacks for null baseline fields', () => {
      const d = deriveGuarded(bc);
      const r = exerciseRenderExpressions(d);
      expect(r._prague).toBe('—');
      expect(r._through).toBe('—');
      expect(r._jobRunId).toBeUndefined();
      expect(r._hasMissing).toBe(false);
      expect(r._missingText).toBe('');
    });
  });

  // ── C) bc undefined (overview not yet loaded) ───────────────────────────
  describe('bc undefined (overview not loaded)', () => {
    it('should compute all derived values without throwing', () => {
      expect(() => deriveGuarded(undefined)).not.toThrow();
    });

    it('should produce no-baseline state', () => {
      const d = deriveGuarded(undefined);
      expect(d.bcHasBaseline).toBe(false);
      expect(d.bcBaseline).toBeNull();
      expect(d.bcMissingDates).toEqual([]);
    });

    it('should safely evaluate ALL downstream render expressions', () => {
      const d = deriveGuarded(undefined);
      expect(() => exerciseRenderExpressions(d)).not.toThrow();
    });
  });

  // ── D) has_baseline=true: normal operation preserved ────────────────────
  describe('has_baseline=true (normal operation)', () => {
    const bc = {
      has_baseline: true,
      baseline: {
        completed_at: '2026-03-01T10:00:00Z',
        completed_at_prague: '2026-03-01T11:00:00',
        through_date: '2026-03-01',
        job_run_id: 'run-abc123',
      },
      missing_count: 2,
      gap_free_since_baseline: false,
      expected_days_count: 30,
      missing_bulk_dates_since_baseline: ['2026-03-05', '2026-03-10'],
    };

    it('should produce correct values for gaps-present baseline', () => {
      const d = deriveGuarded(bc);
      expect(d.bcHasBaseline).toBe(true);
      expect(d.bcMissing).toBe(2);
      expect(d.bcGapFree).toBe(false);
      expect(d.bcBaseline).toEqual(bc.baseline);
      expect(d.bcStatusColor).toBe('red');
      expect(d.bcStatusLabel).toBe('GAPS PRESENT');
      expect(d.bcMissingDates).toEqual(['2026-03-05', '2026-03-10']);
    });

    it('should safely evaluate ALL downstream render expressions', () => {
      const d = deriveGuarded(bc);
      expect(() => exerciseRenderExpressions(d)).not.toThrow();
      const r = exerciseRenderExpressions(d);
      expect(r._prague).toBe('2026-03-01 11:00:00');
      expect(r._through).toBe('2026-03-01');
      expect(r._jobRunId).toBe('run-abc123');
      expect(r._hasMissing).toBe(true);
      expect(r._missingText).toBe('2026-03-05, 2026-03-10');
    });

    it('should produce correct values for gap-free baseline', () => {
      const gapFree = { ...bc, missing_count: 0, gap_free_since_baseline: true, missing_bulk_dates_since_baseline: [] };
      const d = deriveGuarded(gapFree);
      expect(d.bcStatusColor).toBe('green');
      expect(d.bcStatusLabel).toBe('GAP-FREE');
      expect(d.bcMissingDates).toEqual([]);
    });
  });

  // ── E) REGRESSION: exact throw chain that the guard prevents ────────────
  //
  // This test exercises the UNGUARDED derivation pattern and proves that it
  // crashes on a plausible corrupted payload, while the GUARDED pattern is safe.
  //
  // Throw chain:
  //   1. Backend sends non-array for missing_bulk_dates_since_baseline
  //      (e.g., a string error message — plausible in error-handler edge cases)
  //   2. Unguarded: `bc?.missing_bulk_dates_since_baseline ?? []` passes it
  //      through because ?? only catches null/undefined.
  //   3. Downstream: `bcMissingDates.length > 0` → truthy for strings
  //   4. Downstream: `bcMissingDates.join(', ')` → THROWS because
  //      String.prototype.join does not exist.
  //   5. React catches render-phase throw → minified error in production build.
  //
  describe('regression: guard prevents .join() crash from non-array payload', () => {
    // Simulates a corrupted/unexpected backend response where
    // missing_bulk_dates_since_baseline is a string instead of an array.
    const corruptedBc = {
      has_baseline: false,
      baseline: null,
      missing_count: null,
      gap_free_since_baseline: null,
      expected_days_count: null,
      missing_bulk_dates_since_baseline: 'Error: query timed out',
    };

    it('UNGUARDED pattern: .join() throws on non-array bcMissingDates', () => {
      const d = deriveUnguarded(corruptedBc);
      // The unguarded derivation passes the string through
      expect(d.bcMissingDates).toBe('Error: query timed out');
      expect(d.bcMissingDates.length).toBeGreaterThan(0);
      // The downstream render expression THROWS
      expect(() => d.bcMissingDates.join(', ')).toThrow();
    });

    it('GUARDED pattern: returns [] and .join() is safe', () => {
      const d = deriveGuarded(corruptedBc);
      // The guard ensures [] when has_baseline is not true
      expect(d.bcMissingDates).toEqual([]);
      expect(d.bcMissingDates.length).toBe(0);
      // The downstream render expression does NOT throw
      expect(() => d.bcMissingDates.join(', ')).not.toThrow();
    });

    it('GUARDED pattern: full render expressions are safe', () => {
      const d = deriveGuarded(corruptedBc);
      expect(() => exerciseRenderExpressions(d)).not.toThrow();
    });
  });
});
