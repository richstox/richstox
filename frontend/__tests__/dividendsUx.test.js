const fs = require('fs');
const path = require('path');

describe('Dividends UX regression', () => {
  const tickerPagePath = path.join(__dirname, '../app/stock/[ticker].tsx');
  let fileContent;

  beforeAll(() => {
    fileContent = fs.readFileSync(tickerPagePath, 'utf-8');
  });

  it('shows up to 10 recent payments', () => {
    expect(fileContent).toContain('dividendPayments.slice(0, 10)');
  });

  it('labels payment list dates explicitly', () => {
    expect(fileContent).toContain('Ex-date:');
    expect(fileContent).toContain('Payment date:');
  });

  it('has upcoming dividend empty-state copy', () => {
    expect(fileContent).toContain('No upcoming dividend information available.');
  });

  it('falls back to Unknown for missing dividend dates', () => {
    expect(fileContent).toContain("if (!dateStr) return 'Unknown'");
    expect(fileContent).toContain("return formatted === 'N/A' ? 'Unknown' : formatted");
  });

  it('classifies special/irregular dividend tags from canonical event fields', () => {
    expect(fileContent).toContain("eventTypeLabel = isSpecial ? 'Special dividend' : isIrregular ? 'Irregular dividend' : null");
    expect(fileContent).toContain('event_type_label: eventTypeLabel');
  });

  it('shows frequency and currency pills', () => {
    expect(fileContent).toContain('Frequency');
    expect(fileContent).toContain('Currency');
  });

  it('marks partial year neutrally in annual view', () => {
    expect(fileContent).toContain("if (isPartial) return { label: 'Partial', tone: 'neutral' as const, helper: 'Partial year' }");
    expect(fileContent).toContain("'Partial year'");
  });

  it('renders payment-to-payment growth copy', () => {
    expect(fileContent).toContain("'Growth: not comparable'");
    expect(fileContent).toContain("'Growth: flat'");
  });
});

module.exports = {};
