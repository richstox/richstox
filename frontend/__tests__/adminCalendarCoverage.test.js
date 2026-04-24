const fs = require('fs');
const path = require('path');

describe('Admin calendar coverage regression', () => {
  const adminPath = path.join(__dirname, '../app/(tabs)/admin.tsx');
  let fileContent;

  beforeAll(() => {
    fileContent = fs.readFileSync(adminPath, 'utf-8');
  });

  it('supports coverage counts for calendar jobs that do not return per-day arrays', () => {
    expect(fileContent).toContain('function readResultCount(');
    expect(fileContent).toContain('function formatDurationSeconds(');
    expect(fileContent).toContain("['requested_days', 'requested_days_count', 'coverage_requested_days_count']");
    expect(fileContent).toContain("['days_fetched_ok', 'days_fetched_ok_count', 'coverage_ok_days_count']");
    expect(fileContent).toContain("['days_failed', 'days_failed_count', 'coverage_failed_days_count']");
    expect(fileContent).toContain('Coverage: ${daysOk}/${requestedDays}');
    expect(fileContent).toContain('result: lastRun?.result ?? lastRun?.latest_completed_result ?? null');
    expect(fileContent).toContain('Last OK: {lastCompletedText}');
    expect(fileContent).toContain('Started: {job.runningStartedPrague ? formatPragueDisplay(String(job.runningStartedPrague)) : (lastRunText || \'—\')}');
  });
});
