export const ADMIN_CALENDAR_JOBS = [
  { jobName: 'dividend_upcoming_calendar', label: 'Dividends', hour: 4, minute: 50 },
  { jobName: 'earnings_upcoming_calendar', label: 'Earnings', hour: 4, minute: 55 },
  { jobName: 'splits_upcoming_calendar', label: 'Splits', hour: 4, minute: 57 },
  { jobName: 'ipos_upcoming_calendar', label: 'IPOs', hour: 4, minute: 58 },
] as const;

export function formatAdminJobSchedule(hour: number, minute: number): string {
  return `Daily ${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')} Prague`;
}
