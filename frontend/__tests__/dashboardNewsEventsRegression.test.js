const fs = require('fs');
const path = require('path');

describe('Dashboard News & Events regressions', () => {
  const dashboardPath = path.join(__dirname, '../app/(tabs)/dashboard.tsx');
  let fileContent;

  beforeAll(() => {
    fileContent = fs.readFileSync(dashboardPath, 'utf-8');
  });

  it('renames the homepage feed to Events & News and merges upcoming events into the feed', () => {
    expect(fileContent).toContain("<Text style={styles.sectionTitle}>Events & News</Text>");
    expect(fileContent).not.toContain("News plus upcoming followed-ticker events");
    expect(fileContent).toContain("const homepageEvents = useMemo<HomepageEvent[]>(");
    expect(fileContent).toContain("const newsFeedItems = useMemo<DashboardFeedItem[]>(");
    expect(fileContent).toContain("const filteredNewsFeedItems = useMemo<DashboardFeedItem[]>(");
    expect(fileContent).toContain("kind: 'event' as const");
    expect(fileContent).toContain("kind: 'article' as const");
    expect(fileContent).toContain("const [homepageFeedSort, setHomepageFeedSort] = useState<HomepageFeedSort>('date_desc');");
    expect(fileContent).toContain("const [includeHomepageNews, setIncludeHomepageNews] = useState(true);");
    expect(fileContent).toContain("const [newsFeedFilter, setNewsFeedFilter] = useState('');");
    expect(fileContent).toContain("getDashboardFeedDateValue");
    expect(fileContent).toContain("getDashboardFeedAlphaKey");
    expect(fileContent).toContain("const eventItems = homepageEvents.map((event) => ({");
    expect(fileContent).toContain("const articleItems = includeHomepageNews");
    expect(fileContent).toContain("No news or events available");
  });

  it('renders homepage event badges and subtitles in the shared feed rows', () => {
    expect(fileContent).toContain('formatHomepageEventSubtitle');
    expect(fileContent).toContain('<View style={styles.homepageEventBadge}>');
    expect(fileContent).toContain('<Text style={styles.homepageEventBadgeText}>{item.event.event_type}</Text>');
    expect(fileContent).toContain('<Text style={styles.homepageEventSubtitle} numberOfLines={2}>{eventSubtitle}</Text>');
  });

  it('adds homepage sort controls and a +News toggle to the shared feed header', () => {
    expect(fileContent).toContain('<View style={styles.newsControlsRow}>');
    expect(fileContent).toContain("Date {homepageFeedSort === 'date_asc' ? '↑' : '↓'}");
    expect(fileContent).toContain("A‑Z {homepageFeedSort === 'za' ? '↑' : '↓'}");
    expect(fileContent).toContain('placeholder="Search news & events..."');
    expect(fileContent).toContain('<View style={[styles.myStocksSearchWrapper, styles.newsSearchWrapper]}>');
    expect(fileContent).toContain('newsSearchWrapper: {');
    expect(fileContent).toContain('marginTop: 8,');
    expect(fileContent).toContain('data-testid="homepage-events-toggle"');
    expect(fileContent).toContain('<Text style={styles.portfolioToggleLabelInline}>+News</Text>');
    expect(fileContent).toContain('includeHomepageNews && aggregateSentiment');
    expect(fileContent).toContain('formatAggregateSentimentLabel(aggregateSentiment.label, aggregateSentiment.score)');
  });

  it('keeps homepage paging at five items and uses API-provided aggregate sentiment for the full corpus', () => {
    expect(fileContent).toContain('const INITIAL_NEWS_LIMIT = 5;');
    expect(fileContent).toContain('const NEWS_PAGE_SIZE = 5;');
    expect(fileContent).toContain('axios.get(`${API_URL}/api/news?offset=${offset}&limit=${NEWS_PAGE_SIZE}`)');
    expect(fileContent).toContain('setAggregateSentiment(response.data.aggregate_sentiment || null);');
    expect(fileContent).toContain('<Text style={styles.loadMoreText}>Load more events & news</Text>');
  });
});
