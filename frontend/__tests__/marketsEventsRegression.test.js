const fs = require('fs');
const path = require('path');

describe('Markets events regressions', () => {
  const marketsPagePath = path.join(__dirname, '../app/(tabs)/markets.tsx');
  let fileContent;

  beforeAll(() => {
    fileContent = fs.readFileSync(marketsPagePath, 'utf-8');
  });

  it('keeps compact event-type tabs with expanded short labels and removes the legend helper row', () => {
    expect(fileContent).toContain("earnings: { label: 'Earnings', shortLabel: 'EARN', legendLabel: 'E = Earnings'");
    expect(fileContent).toContain("dividend: { label: 'Dividends', shortLabel: 'DIV', legendLabel: 'D = Dividends'");
    expect(fileContent).toContain("split: { label: 'Splits', shortLabel: 'SPLIT', legendLabel: 'S = Splits'");
    expect(fileContent).toContain("ipo: { label: 'IPOs', shortLabel: 'IPO', legendLabel: 'IPO = IPOs'");
    expect(fileContent).not.toContain("EVENT_TYPE_ORDER.map((type) => EVENT_META[type].legendLabel).join(' • ')");
    expect(fileContent).toContain('<View style={styles.eventTabLabelRow}>');
  });

  it('keeps the ticker/company search visible for all event tabs', () => {
    expect(fileContent).toContain('placeholder="Search ticker or company"');
    expect(fileContent).toContain("Platform.OS === 'web' ? { outlineStyle: 'none', outlineWidth: 0 } : null");
    expect(fileContent).not.toContain('const TICKER_FILTER_THRESHOLD = 6;');
    expect(fileContent).not.toContain('const shouldShowTickerFilter = tickerOptions.length >= TICKER_FILTER_THRESHOLD;');
    expect(fileContent).toContain('<View style={styles.filterSearchWrap}>');
  });

  it('keeps earnings subtitles on the expected label and shows simplified compact horizontal day cards', () => {
    expect(fileContent).toContain('details.push(`Exp. ${formattedEstimate}`)');
    expect(fileContent).not.toContain('const ACTIVE_DAY_DOT_LAYOUT: EventType[][] = [');
    expect(fileContent).not.toContain('const activeDayTypes = new Set(dayEvents.map((event) => event.type));');
    expect(fileContent).not.toContain('<Text style={[styles.activeDayCount, isSelected && styles.activeDayTextSelected]}>');
    expect(fileContent).toContain('const ACTIVE_DAYS_SCROLL_THRESHOLD = 4;');
    expect(fileContent).toContain('const shouldShowActiveDaysArrows = activeDaysContentWidth > activeDaysLayoutWidth + ACTIVE_DAYS_SCROLL_THRESHOLD;');
    expect(fileContent).toContain("const scrollActiveDaysBy = (scrollDirection: 'left' | 'right') => {");
    expect(fileContent).toContain("scrollActiveDaysBy('left')");
    expect(fileContent).toContain("scrollActiveDaysBy('right')");
    expect(fileContent).toContain('minWidth: 58,');
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
    expect(fileContent).toContain('<Text style={styles.eventsDateTitle}>{selectedPeriodLabel}</Text>');
  });

  it('supports daily monthly and yearly calendar views and keeps the logo clickable', () => {
    expect(fileContent).toContain("type CalendarViewMode = 'daily' | 'monthly' | 'yearly';");
    expect(fileContent).toContain("const CALENDAR_VIEW_ORDER: CalendarViewMode[] = ['daily', 'monthly', 'yearly'];");
    expect(fileContent).toContain('const MAX_VISIBLE_MONTH_CARDS = 4;');
    expect(fileContent).toContain('const activeDayKeysForDisplayMonth = useMemo(');
    expect(fileContent).toContain("setVisibleEventLimit(INITIAL_VISIBLE_EVENTS);");
    expect(fileContent).toContain("activeMonthKeys.find((monthKey) => monthKey >= todayMonthKey) ?? activeMonthKeys[0]");
    expect(fileContent).toContain("getYearMonthKey(nextYear, 'last')");
    expect(fileContent).toContain("getYearMonthKey(nextYear, 'first')");
    expect(fileContent).toContain("activeDayKeysForDisplayMonth.map((dayKey) => {");
    expect(fileContent).toContain("Show full calendar");
    expect(fileContent).toContain("Load more events");
    expect(fileContent).toContain("calendarView === 'daily' ? (");
    expect(fileContent).toContain("calendarView === 'monthly' ? (");
    expect(fileContent).toContain("setSelectedYear(year);");
    expect(fileContent).toContain("import AppHeader from '../../components/AppHeader';");
    expect(fileContent).toContain('<AppHeader title="Markets" />');
  });

  it('adds icons to the calendar and events headlines and removes the redundant top-right total', () => {
    expect(fileContent).toContain('<Ionicons name="calendar-clear-outline" size={18} color={COLORS.primary} />');
    expect(fileContent).toContain('<Text style={styles.sectionTitle}>Calendar</Text>');
    expect(fileContent).toContain('<Ionicons name="newspaper-outline" size={18} color={COLORS.primary} />');
    expect(fileContent).toContain('<Text style={styles.sectionTitle}>Events</Text>');
    expect(fileContent).not.toContain('<Text style={styles.eventsCount}>{periodEvents.length}</Text>');
  });
});
