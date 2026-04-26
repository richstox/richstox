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
    expect(fileContent).toContain('activeDayKeysForDisplayMonth.map((dayKey) => {');
    expect(fileContent).toContain('<View style={styles.activeDaysCarouselRow}>');
    expect(fileContent).toContain('contentContainerStyle={styles.activeDaysScrollContent}');
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

  it('supports daily monthly and yearly calendar views inside the selector popup and keeps the logo clickable', () => {
    expect(fileContent).toContain("type CalendarViewMode = 'daily' | 'monthly' | 'yearly';");
    expect(fileContent).toContain("const CALENDAR_VIEW_ORDER: CalendarViewMode[] = ['daily', 'monthly', 'yearly'];");
    expect(fileContent).toContain('const activeDayKeysForDisplayMonth = useMemo(');
    expect(fileContent).toContain("activeMonthKeys.find((monthKey) => monthKey >= todayMonthKey) ?? activeMonthKeys[0]");
    expect(fileContent).toContain("getYearMonthKey(nextYear, 'last')");
    expect(fileContent).toContain("getYearMonthKey(nextYear, 'first')");
    expect(fileContent).toContain('const [activeDaysScrollX, setActiveDaysScrollX] = useState(0);');
    expect(fileContent).toContain('const ACTIVE_DAY_SCROLL_TOLERANCE = 4;');
    expect(fileContent).toContain('const canScrollActiveDaysNext = activeDaysScrollX < maxActiveDaysScrollX - ACTIVE_DAY_SCROLL_TOLERANCE;');
    expect(fileContent).toContain('activeDaysScrollRef.current?.scrollTo({ x: nextX, animated: true });');
    expect(fileContent).toContain("activeDayKeysForDisplayMonth.map((dayKey) => {");
    expect(fileContent).toContain("const INITIAL_VISIBLE_FEED_ITEMS = 5;");
    expect(fileContent).toContain("const [calendarPickerVisible, setCalendarPickerVisible] = useState(false);");
    expect(fileContent).toContain('<Text style={styles.eventsDateSelectText}>Select</Text>');
    expect(fileContent).toContain('visible={calendarPickerVisible}');
    expect(fileContent).toContain("calendarView === 'daily' ? (");
    expect(fileContent).toContain("calendarView === 'monthly' ? (");
    expect(fileContent).toContain('accessibilityLabel="Scroll days left"');
    expect(fileContent).toContain('accessibilityLabel="Scroll days right"');
    expect(fileContent).toContain('style={styles.activeDaysCarouselRow}');
    expect(fileContent).toContain('Keep the selected date visible immediately when the picker opens or the month changes.');
    expect(fileContent).toContain("setSelectedYear(year);");
    expect(fileContent).toContain('style={styles.eventsDateSelectControl}');
    expect(fileContent).toContain('style={styles.selectorDetailSection}');
    expect(fileContent).toContain('style={styles.selectorMonthHeader}');
    expect(fileContent).toContain("import AppHeader from '../../components/AppHeader';");
    expect(fileContent).toContain('<AppHeader title="Markets" />');
  });

  it('replaces the Prague date label, disables zero-count tabs, and merges +News into Events & News', () => {
    expect(fileContent).not.toContain('Prague date');
    expect(fileContent).toContain('const [includeNews, setIncludeNews] = useState(true);');
    expect(fileContent).toContain('<Text style={styles.portfolioToggleLabelInline}>+News</Text>');
    expect(fileContent).toContain('const MARKET_NEWS_PER_TICKER = 3;');
    expect(fileContent).toContain('const MARKET_DIGEST_LIMIT = 100;');
    expect(fileContent).toContain('/api/v1/markets/news?limit=${MARKET_NEWS_LIMIT}&market_limit=${MARKET_DIGEST_LIMIT}&per_ticker_limit=${MARKET_NEWS_PER_TICKER}&offset=0');
    expect(fileContent).toContain('const isDisabled = selectedEventCounts[type] === 0;');
    expect(fileContent).toContain('disabled={isDisabled}');
    expect(fileContent).toContain('style={[styles.eventTab, isActive && styles.eventTabActive, isDisabled && styles.eventTabDisabled]}');
    expect(fileContent).toContain('<Text style={styles.sectionTitle}>Events & News</Text>');
    expect(fileContent).toContain('const visibleNewsItems = useMemo(() => {');
    expect(fileContent).toContain('const filteredFeedItems = useMemo<MarketFeedItem[]>(() => {');
    expect(fileContent).toContain('const displayedFeedItems = useMemo(');
    expect(fileContent).toContain('formatAggregateSentimentLabel(aggregateSentiment.label, aggregateSentiment.score)');
    expect(fileContent).toContain('aggregateSentiment && (');
    expect(fileContent).toContain('No saved market or ticker news available right now');
    expect(fileContent).toContain('Load more</Text>');
    expect(fileContent).not.toContain('Load more news</Text>');
  });

  it('keeps the events headline icon and removes the standalone calendar card', () => {
    expect(fileContent).toContain('<Ionicons name="newspaper-outline" size={18} color={COLORS.primary} />');
    expect(fileContent).toContain('<Text style={styles.sectionTitle}>Events & News</Text>');
    expect(fileContent).toContain('<Text style={styles.selectorTitle}>Events & News calendar</Text>');
    expect(fileContent).not.toContain('<Text style={styles.sectionTitle}>Calendar</Text>');
    expect(fileContent).not.toContain('<Text style={styles.eventsCount}>{periodEvents.length}</Text>');
  });

  it('uses the compact selector flow instead of a standalone show-details button and resolves news logos like event logos', () => {
    expect(fileContent).toContain("<Text style={styles.eventsDateSelectText}>Select</Text>");
    expect(fileContent).toContain('style={styles.selectorDetailSection}');
    expect(fileContent).not.toContain('setIsCalendarExpanded((prev) => !prev);');
    expect(fileContent).not.toContain('Hide Details');
    expect(fileContent).not.toContain('Show calendar details');
    expect(fileContent).toContain('resolveEventLogoUrl(news.logo_url, news.ticker)');
  });
});
