const fs = require('fs');
const path = require('path');

describe('Markets events regressions', () => {
  const marketsPagePath = path.join(__dirname, '../app/(tabs)/markets.tsx');
  let fileContent;

  beforeAll(() => {
    fileContent = fs.readFileSync(marketsPagePath, 'utf-8');
  });

  it('keeps compact event-type tabs and inline legend help for all calendar categories', () => {
    expect(fileContent).toContain("earnings: { label: 'Earnings', shortLabel: 'E', legendLabel: 'E = Earnings'");
    expect(fileContent).toContain("dividend: { label: 'Dividends', shortLabel: 'D', legendLabel: 'D = Dividends'");
    expect(fileContent).toContain("split: { label: 'Splits', shortLabel: 'S', legendLabel: 'S = Splits'");
    expect(fileContent).toContain("ipo: { label: 'IPOs', shortLabel: 'IPO', legendLabel: 'IPO = IPOs'");
    expect(fileContent).toContain("EVENT_TYPE_ORDER.map((type) => EVENT_META[type].legendLabel).join(' • ')");
  });

  it('shows ticker/company filtering only when the event list is large enough', () => {
    expect(fileContent).toContain('const TICKER_FILTER_THRESHOLD = 6;');
    expect(fileContent).toContain('const shouldShowTickerFilter = tickerOptions.length >= TICKER_FILTER_THRESHOLD;');
    expect(fileContent).toContain('placeholder="Search ticker or company"');
  });

  it('renders event logos with ticker logo fallback and links ticker rows to stock detail', () => {
    expect(fileContent).toContain('const EventLogo = ({ logoUrl, fallbackKey }');
    expect(fileContent).toContain('const normalizedTicker = ticker?.trim().toUpperCase();');
    expect(fileContent).toContain('if (!rawUrl && normalizedTicker) return `${API_URL}/api/logo/${normalizedTicker}`;');
    expect(fileContent).toContain('router.push(`/stock/${event.ticker}`)');
    expect(fileContent).toContain('<EventLogo');
  });

  it('formats event dates in DD/MM/YYYY on Markets event cards', () => {
    expect(fileContent).toContain("const formatDateDMY = (dateStr: string | null | undefined): string => {");
    expect(fileContent).toContain('details.push(`Pay ${formatDateDMY(payDate)}`)');
    expect(fileContent).toContain('<Text style={styles.eventsDateTitle}>{formatDateDMY(selectedDateKey)}</Text>');
  });
});
