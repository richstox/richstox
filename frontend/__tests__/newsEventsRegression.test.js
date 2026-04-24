const fs = require('fs');
const path = require('path');

describe('News & Events regressions', () => {
  const tickerPagePath = path.join(__dirname, '../app/stock/[ticker].tsx');
  let fileContent;

  beforeAll(() => {
    fileContent = fs.readFileSync(tickerPagePath, 'utf-8');
  });

  it('declares the shared date formatter before dividend event date formatting uses it', () => {
    const formatterIndex = fileContent.indexOf('const formatDateDMY = (dateStr: string | null | undefined): string => {');
    const dividendDateIndex = fileContent.indexOf('const formatDividendEventDate = (dateStr: string | null | undefined): string => {');
    expect(formatterIndex).toBeGreaterThan(-1);
    expect(dividendDateIndex).toBeGreaterThan(-1);
    expect(formatterIndex).toBeLessThan(dividendDateIndex);
  });

  it('keeps earnings events enriched with estimate and market timing', () => {
    expect(fileContent).toContain("title: 'Upcoming Earnings'");
    expect(fileContent).toContain('formatUpcomingEarningsEstimate(upcomingEarnings.estimate, upcomingEarnings.currency)');
    expect(fileContent).toContain('${EVENT_SUBTITLE_SEPARATOR}${marketLabel}');
  });

  it('keeps dividend events enriched with amount, ex-date, and pay date', () => {
    expect(fileContent).toContain("title: 'Upcoming Ex-Dividend'");
    expect(fileContent).toContain('formatDividendAmount(');
    expect(fileContent).toContain('dividendSubtitleParts.push(`Ex ${formatDividendEventDate(nextDividendInfo.next_ex_date)}`)');
    expect(fileContent).toContain('dividendSubtitleParts.push(`Pay ${formatDividendEventDate(nextDividendInfo.next_pay_date)}`)');
  });

  it('keeps split events enriched with the split ratio', () => {
    expect(fileContent).toContain("title: 'Upcoming Split'");
    expect(fileContent).toContain("subtitle: getFormattedSplitRatio(upcomingSplit) || 'Upcoming split'");
  });

  it('renders event subtitles into the News & Events card text', () => {
    expect(fileContent).toContain('const eventText = formatEventMessage(item.title, item.subtitle);');
    expect(fileContent).toContain('<Text style={styles.newsTitle} numberOfLines={3}>{eventText}</Text>');
  });
});
