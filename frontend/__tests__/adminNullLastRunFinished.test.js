/**
 * DEFENSIVE HARDENING: null last_run_finished display quality
 *
 * Status: UNPROVEN crash root-cause.
 *
 * Investigation result:
 *   The backend can return jobs with status='success' but last_run_finished=null
 *   (6 observed: market_calendar, benchmark_update, key_metrics, peer_medians,
 *   admin_report, fundamentals_sync).
 *
 *   In the original frontend code, null is falsy in JavaScript, so:
 *   - normaliseRun: `null || null || null` → null (falsy) → guarded by `start && finish`
 *   - formatTime: `!null` → true → returns 'Never' (no exception)
 *   - formatDuration: `null === null` → true → returns '' (no exception)
 *   - formatHours: `null == null` → true → returns '—' (no exception)
 *
 *   No thrown exception was proven. The React minified error #418 in production
 *   is a hydration mismatch (SSR vs client) — a separate architectural issue.
 *
 * This hardening prevents:
 *   - formatDuration(NaN) displaying "NaNs" instead of ''
 *   - formatTime('garbage') displaying "Invalid Date Prague" instead of '—'
 *   - formatHours(NaN) displaying "NaNh ago" instead of '—'
 *
 * CI/CD: Run with `npx jest __tests__/adminNullLastRunFinished.test.js`
 */

const fs = require('fs');
const path = require('path');

const pipelinePath = path.join(__dirname, '../app/admin/pipeline.tsx');
const adminPath = path.join(__dirname, '../app/(tabs)/admin.tsx');

// ─── Source verification ─────────────────────────────────────────────────────

describe('Defensive hardening: format functions guard non-finite inputs', () => {

  let pipelineSrc, adminSrc;
  beforeAll(() => {
    pipelineSrc = fs.readFileSync(pipelinePath, 'utf-8');
    adminSrc = fs.readFileSync(adminPath, 'utf-8');
  });

  it('formatDuration guards non-finite values', () => {
    expect(pipelineSrc).toMatch(/function formatDuration/);
    expect(pipelineSrc).toMatch(/isFinite\(sec\)/);
  });

  it('formatTime guards NaN from parseUtcIso', () => {
    expect(pipelineSrc).toMatch(/function formatTime/);
    expect(pipelineSrc).toMatch(/isNaN\(ms\)/);
  });

  it('formatTime catch returns dash, not raw input', () => {
    // The catch handler should return '—' not the raw iso string
    expect(pipelineSrc).toMatch(/catch\s*\{[^}]*return\s+['"]—['"]/);
  });

  it('formatHours guards non-finite values', () => {
    expect(adminSrc).toMatch(/function formatHours/);
    expect(adminSrc).toMatch(/isFinite\(h\)/);
  });
});

// ─── Logic verification ──────────────────────────────────────────────────────

describe('Format function behavior with edge-case inputs', () => {

  // Replicate the hardened formatDuration
  function formatDuration(sec) {
    if (sec == null || !isFinite(sec)) return '';
    const total = Math.round(sec);
    const hours = Math.floor(total / 3600);
    const minutes = Math.floor((total % 3600) / 60);
    const seconds = total % 60;
    if (hours > 0) return `${hours}h ${minutes}m`;
    if (minutes > 0) return `${minutes}m ${seconds}s`;
    return `${seconds}s`;
  }

  // Replicate the hardened formatHours
  function formatHours(h) {
    if (h == null || !isFinite(h)) return '—';
    if (h < 1) return `${Math.round(h * 60)}m ago`;
    return h.toFixed(1) + 'h ago';
  }

  // Replicate the hardened formatTime
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
    } catch { return '—'; }
  }

  describe('formatDuration', () => {
    it('returns empty for null/undefined', () => {
      expect(formatDuration(null)).toBe('');
      expect(formatDuration(undefined)).toBe('');
    });
    it('returns empty for NaN (hardening)', () => {
      expect(formatDuration(NaN)).toBe('');
    });
    it('returns empty for Infinity', () => {
      expect(formatDuration(Infinity)).toBe('');
    });
    it('formats valid durations correctly', () => {
      expect(formatDuration(30)).toBe('30s');
      expect(formatDuration(90)).toBe('1m 30s');
      expect(formatDuration(3661)).toBe('1h 1m');
    });
  });

  describe('formatHours', () => {
    it('returns dash for null/undefined', () => {
      expect(formatHours(null)).toBe('—');
      expect(formatHours(undefined)).toBe('—');
    });
    it('returns dash for NaN (hardening)', () => {
      expect(formatHours(NaN)).toBe('—');
    });
    it('formats valid hours correctly', () => {
      expect(formatHours(0.5)).toBe('30m ago');
      expect(formatHours(2.5)).toBe('2.5h ago');
    });
  });

  describe('formatTime', () => {
    it('returns Never for null/undefined/empty', () => {
      expect(formatTime(null)).toBe('Never');
      expect(formatTime(undefined)).toBe('Never');
      expect(formatTime('')).toBe('Never');
    });
    it('returns dash for unparseable strings (hardening)', () => {
      expect(formatTime('not-a-date')).toBe('—');
    });
    it('formats valid ISO timestamps', () => {
      const result = formatTime('2026-03-31T10:00:00Z');
      expect(result).toContain('Prague');
      expect(result).not.toContain('Invalid');
    });
  });
});
