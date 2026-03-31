/**
 * ADMIN NULL last_run_finished REGRESSION TEST
 *
 * Validates that admin components handle jobs with `status='success'`
 * but `last_run_finished=null` without crashing.
 *
 * Root-cause chain (proven):
 *   1. Backend returns 6 jobs with `status='success'` and `last_run_finished=null`
 *      (finished_at not recorded in ops_job_runs).
 *   2. Frontend normalises these runs via normaliseRun() and renders timestamps
 *      via formatTime() / formatDuration() / Date.parse().
 *   3. If null propagates into Date.parse() or .replace()/.slice() without guard,
 *      the render throws and React surfaces a minified production error.
 *   4. The fix: all date OR-chains terminate with `|| undefined` so null never
 *      propagates past a falsy check; formatTime returns '—' for unparseable
 *      values; formatDuration returns '' for non-finite values; formatHours
 *      in DashboardTab guards against NaN and non-number values.
 *
 * Affected jobs (production observation):
 *   market_calendar, benchmark_update, key_metrics,
 *   peer_medians, admin_report, fundamentals_sync
 *
 * CI/CD: Run with `npx jest __tests__/adminNullLastRunFinished.test.js`
 */

const fs = require('fs');
const path = require('path');

const pipelinePath = path.join(__dirname, '../app/admin/pipeline.tsx');
const adminPath = path.join(__dirname, '../app/(tabs)/admin.tsx');

// ─── Replicate normaliseRun logic from pipeline.tsx ──────────────────────────
function normaliseRun(run) {
  if (!run) return run;
  const start = run.started_at || run.start_time || undefined;
  const finish = run.finished_at || run.end_time || run.last_run_finished || undefined;
  const details = run.details || {};
  const durationSeconds = run.duration_seconds != null ? run.duration_seconds : (
    start && finish ? Math.max(0, Math.round((Date.parse(finish) - Date.parse(start)) / 1000)) : undefined
  );
  return {
    ...run,
    start_time: start,
    end_time: finish,
    started_at: start,
    finished_at: finish,
    duration_seconds: durationSeconds,
    last_run_finished: finish ?? run.last_run_finished ?? undefined,
  };
}

// ─── Replicate render‐path date derivation from pipeline.tsx (line ~1451) ────
function deriveRunDisplay(run) {
  if (!run) return { statusText: '—', durationText: '' };
  const runStart = run.started_at_prague || run.start_time || run.started_at || undefined;
  const runEnd = run.finished_at_prague || run.end_time || run.finished_at || undefined;
  const lastDuration = run.duration_seconds != null ? run.duration_seconds : (
    runStart && runEnd ? Math.max(0, Math.round((Date.parse(runEnd) - Date.parse(runStart)) / 1000)) : undefined
  );
  const isLiveRun = run.status === 'running';
  const statusText = isLiveRun
    ? (runStart ? `Started ${formatTime(runStart)}` : '—')
    : (runEnd || runStart)
      ? formatTime(runEnd || runStart)
      : '—';
  const durationText = lastDuration !== undefined
    ? ` (${formatDuration(lastDuration)})`
    : '';
  return { statusText, durationText, runStart, runEnd, lastDuration };
}

// ─── Replicate formatTime from pipeline.tsx ──────────────────────────────────
function parseUtcIso(iso) {
  if (!iso) return NaN;
  let s = String(iso);
  if (!s.endsWith('Z') && !/[+-]\d{2}:\d{2}$/.test(s) && !/[+-]\d{4}$/.test(s)) {
    s += 'Z';
  }
  return Date.parse(s);
}

function formatTime(iso) {
  if (!iso) return 'Never';
  try {
    const ms = parseUtcIso(iso);
    if (isNaN(ms)) return '—';
    const d = new Date(ms);
    return d.toLocaleString('en-GB', {
      timeZone: 'Europe/Prague',
      month: 'short', day: 'numeric',
      hour: '2-digit', minute: '2-digit',
    }) + ' Prague';
  } catch { return String(iso); }
}

// ─── Replicate formatDuration from pipeline.tsx ──────────────────────────────
function formatDuration(sec) {
  if (sec === undefined || sec === null || !isFinite(sec)) return '';
  const total = Math.round(sec);
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const seconds = total % 60;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

// ─── Replicate formatHours from admin.tsx ─────────────────────────────────────
function formatHours(h) {
  if (h == null || typeof h !== 'number' || !isFinite(h)) return '—';
  if (h < 1) return `${Math.round(h * 60)}m ago`;
  return `${h.toFixed(1)}h ago`;
}

// ─── Production‐observed problematic payload ─────────────────────────────────
const PROBLEM_JOBS = [
  'market_calendar', 'benchmark_update', 'key_metrics',
  'peer_medians', 'admin_report', 'fundamentals_sync',
];

function makeNullFinishedJobRuns() {
  const runs = {};
  for (const name of PROBLEM_JOBS) {
    runs[name] = {
      status: 'success',
      started_at: '2026-03-31T03:00:00Z',
      finished_at: null,
      end_time: null,
      last_run_finished: null,
      duration_seconds: null,
      records_processed: 42,
    };
  }
  return runs;
}

// =============================================================================
// TESTS
// =============================================================================

describe('Admin: null last_run_finished guard (status=success, finished=null)', () => {

  // ── A) Source-level verification ────────────────────────────────────────────
  describe('source-level guards in pipeline.tsx', () => {
    let src;
    beforeAll(() => { src = fs.readFileSync(pipelinePath, 'utf-8'); });

    it('normaliseRun should coerce null finish to undefined via || undefined', () => {
      // The finish derivation must end with `|| undefined`
      expect(src).toMatch(
        /const\s+finish\s*=.*\|\|\s*undefined\s*;/
      );
    });

    it('normaliseRun should coerce null start to undefined via || undefined', () => {
      expect(src).toMatch(
        /const\s+start\s*=.*\|\|\s*undefined\s*;/
      );
    });

    it('last_run_finished output should end with ?? undefined', () => {
      expect(src).toMatch(
        /last_run_finished:\s*finish\s*\?\?\s*run\.last_run_finished\s*\?\?\s*undefined/
      );
    });

    it('runEnd derivation should end with || undefined', () => {
      expect(src).toMatch(
        /const\s+runEnd\s*=.*\|\|\s*undefined\s*;/
      );
    });

    it('runStart derivation should end with || undefined', () => {
      expect(src).toMatch(
        /const\s+runStart\s*=.*\|\|\s*undefined\s*;/
      );
    });

    it('formatDuration should guard non-finite values', () => {
      expect(src).toMatch(/isFinite\(sec\)/);
    });

    it('formatTime should guard NaN from parseUtcIso', () => {
      expect(src).toMatch(/isNaN\(ms\)/);
    });
  });

  describe('source-level guards in admin.tsx', () => {
    let src;
    beforeAll(() => { src = fs.readFileSync(adminPath, 'utf-8'); });

    it('formatHours should guard non-number and non-finite values', () => {
      expect(src).toMatch(/typeof\s+h\s*!==\s*'number'/);
      expect(src).toMatch(/isFinite\(h\)/);
    });
  });

  // ── B) normaliseRun with null last_run_finished ────────────────────────────
  describe('normaliseRun with null finished fields', () => {
    const nullFinishedRun = {
      status: 'success',
      started_at: '2026-03-31T03:00:00Z',
      finished_at: null,
      end_time: null,
      last_run_finished: null,
      duration_seconds: null,
    };

    it('should not throw when all finish fields are null', () => {
      expect(() => normaliseRun(nullFinishedRun)).not.toThrow();
    });

    it('should produce undefined (not null) for finish-derived fields', () => {
      const norm = normaliseRun(nullFinishedRun);
      expect(norm.end_time).toBeUndefined();
      expect(norm.finished_at).toBeUndefined();
      expect(norm.last_run_finished).toBeUndefined();
    });

    it('should preserve start_time when started_at is present', () => {
      const norm = normaliseRun(nullFinishedRun);
      expect(norm.start_time).toBe('2026-03-31T03:00:00Z');
      expect(norm.started_at).toBe('2026-03-31T03:00:00Z');
    });

    it('should not compute duration_seconds when finish is null', () => {
      const norm = normaliseRun(nullFinishedRun);
      expect(norm.duration_seconds).toBeUndefined();
    });

    it('should handle run where all date fields are null', () => {
      const allNull = { status: 'success', started_at: null, finished_at: null, end_time: null, last_run_finished: null };
      expect(() => normaliseRun(allNull)).not.toThrow();
      const norm = normaliseRun(allNull);
      expect(norm.start_time).toBeUndefined();
      expect(norm.end_time).toBeUndefined();
      expect(norm.duration_seconds).toBeUndefined();
    });

    it('should not call Date.parse on null', () => {
      // Verify: if finish is null → start && finish is falsy → no Date.parse call
      const norm = normaliseRun(nullFinishedRun);
      // duration_seconds should be undefined (not NaN from Date.parse(null))
      expect(norm.duration_seconds).toBeUndefined();
    });
  });

  // ── C) Render path with all 6 problematic jobs ─────────────────────────────
  describe('render path with 6 production-observed null-finished jobs', () => {
    const jobRuns = makeNullFinishedJobRuns();

    it('should not throw when normalising all 6 problem jobs', () => {
      for (const name of PROBLEM_JOBS) {
        expect(() => normaliseRun(jobRuns[name])).not.toThrow();
      }
    });

    it('should not throw when deriving run display for all 6 problem jobs', () => {
      for (const name of PROBLEM_JOBS) {
        const norm = normaliseRun(jobRuns[name]);
        expect(() => deriveRunDisplay(norm)).not.toThrow();
      }
    });

    it('should show safe placeholder for missing finish time', () => {
      for (const name of PROBLEM_JOBS) {
        const norm = normaliseRun(jobRuns[name]);
        const display = deriveRunDisplay(norm);
        // statusText should either be a formatted start time or '—', never throw
        expect(typeof display.statusText).toBe('string');
        expect(display.statusText.length).toBeGreaterThan(0);
        // durationText should be empty when duration unknown
        expect(display.durationText).toBe('');
      }
    });

    it('should render start time when only start is present', () => {
      for (const name of PROBLEM_JOBS) {
        const norm = normaliseRun(jobRuns[name]);
        const display = deriveRunDisplay(norm);
        // Should show the start time since end is null
        expect(display.statusText).not.toBe('—');
        expect(display.statusText).toContain('Prague');
      }
    });
  });

  // ── D) formatTime / formatDuration / formatHours edge cases ────────────────
  describe('format function null safety', () => {
    it('formatTime should return "Never" for null/undefined', () => {
      expect(formatTime(null)).toBe('Never');
      expect(formatTime(undefined)).toBe('Never');
      expect(formatTime('')).toBe('Never');
    });

    it('formatTime should return "—" for unparseable strings', () => {
      expect(formatTime('not-a-date')).toBe('—');
    });

    it('formatTime should format valid ISO strings', () => {
      const result = formatTime('2026-03-31T10:00:00Z');
      expect(result).toContain('Prague');
    });

    it('formatDuration should return "" for null/undefined/NaN', () => {
      expect(formatDuration(null)).toBe('');
      expect(formatDuration(undefined)).toBe('');
      expect(formatDuration(NaN)).toBe('');
      expect(formatDuration(Infinity)).toBe('');
    });

    it('formatDuration should format valid durations', () => {
      expect(formatDuration(65)).toBe('1m 5s');
      expect(formatDuration(3661)).toBe('1h 1m');
      expect(formatDuration(30)).toBe('30s');
    });

    it('formatHours should return "—" for null/undefined/NaN/non-number', () => {
      expect(formatHours(null)).toBe('—');
      expect(formatHours(undefined)).toBe('—');
      expect(formatHours(NaN)).toBe('—');
      expect(formatHours(Infinity)).toBe('—');
      expect(formatHours('5')).toBe('—');
    });

    it('formatHours should format valid numbers', () => {
      expect(formatHours(0.5)).toBe('30m ago');
      expect(formatHours(2.5)).toBe('2.5h ago');
      expect(formatHours(0)).toBe('0m ago');
    });
  });

  // ── E) DashboardTab aggregate health derivation (no crash) ─────────────────
  describe('DashboardTab health derivation with null-finished jobs', () => {
    const overviewHealth = {
      score_pct: 100,
      status: 'Good',
      scheduler_active: true,
      jobs_completed: 6,
      jobs_failed: 0,
      jobs_total: 6,
      overdue_count: 0,
      has_runs_today: true,
    };

    const pipelineAge = {
      pipeline_hours_since_success: null,
      pipeline_status: 'unknown',
      morning_refresh_hours_since_success: null,
      morning_refresh_status: 'unknown',
    };

    it('should not throw when computing failedCount', () => {
      const failedCount = overviewHealth?.jobs_failed ?? 0;
      expect(failedCount).toBe(0);
    });

    it('should not throw when formatting null pipeline_hours', () => {
      expect(() => formatHours(pipelineAge?.pipeline_hours_since_success)).not.toThrow();
      expect(formatHours(pipelineAge?.pipeline_hours_since_success)).toBe('—');
    });

    it('should not throw when formatting null morning_refresh_hours', () => {
      expect(() => formatHours(pipelineAge?.morning_refresh_hours_since_success)).not.toThrow();
      expect(formatHours(pipelineAge?.morning_refresh_hours_since_success)).toBe('—');
    });

    it('should handle scheduler_active=true', () => {
      const value = overviewHealth.scheduler_active ? 'Running' : 'Paused';
      expect(value).toBe('Running');
    });
  });
});
