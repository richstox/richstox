const fs = require('fs');
const path = require('path');

describe('Markets events regressions', () => {
  const marketsPagePath = path.join(__dirname, '../app/(tabs)/markets.tsx');
  let fileContent;

  beforeAll(() => {
    fileContent = fs.readFileSync(marketsPagePath, 'utf-8');
  });

  it('keeps event-type tabs for all calendar event categories', () => {
    expect(fileContent).toContain("earnings: { label: 'Earnings'");
    expect(fileContent).toContain("dividend: { label: 'Dividends'");
    expect(fileContent).toContain("split: { label: 'Splits'");
    expect(fileContent).toContain("ipo: { label: 'IPOs'");
  });

  it('shows ticker/company filtering only when the event list is large enough', () => {
    expect(fileContent).toContain('const TICKER_FILTER_THRESHOLD = 6;');
    expect(fileContent).toContain('const shouldShowTickerFilter = tickerOptions.length >= TICKER_FILTER_THRESHOLD;');
    expect(fileContent).toContain('placeholder="Search ticker or company"');
  });

  it('renders event logos and links ticker rows to stock detail', () => {
    expect(fileContent).toContain('const EventLogo = ({ logoUrl, fallbackKey }');
    expect(fileContent).toContain('router.push(`/stock/${event.ticker}`)');
    expect(fileContent).toContain('<EventLogo');
  });
});
