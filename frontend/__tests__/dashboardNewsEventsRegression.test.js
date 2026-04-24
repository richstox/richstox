const fs = require('fs');
const path = require('path');

describe('Dashboard News & Events regressions', () => {
  const dashboardPath = path.join(__dirname, '../app/(tabs)/dashboard.tsx');
  let fileContent;

  beforeAll(() => {
    fileContent = fs.readFileSync(dashboardPath, 'utf-8');
  });

  it('renames the homepage feed to News & Events and merges upcoming events into the feed', () => {
    expect(fileContent).toContain("<Text style={styles.sectionTitle}>News & Events</Text>");
    expect(fileContent).toContain("const homepageEvents = useMemo<HomepageEvent[]>(");
    expect(fileContent).toContain("const newsFeedItems = useMemo<DashboardFeedItem[]>(");
    expect(fileContent).toContain("kind: 'event' as const");
    expect(fileContent).toContain("kind: 'article' as const");
    expect(fileContent).toContain("const [homepageFeedSort, setHomepageFeedSort] = useState<HomepageFeedSort>('date_desc');");
    expect(fileContent).toContain("const [includeHomepageEvents, setIncludeHomepageEvents] = useState(true);");
    expect(fileContent).toContain("getDashboardFeedDateValue");
    expect(fileContent).toContain("getDashboardFeedAlphaKey");
    expect(fileContent).toContain("No news or events available");
  });

  it('renders homepage event badges and subtitles in the shared feed rows', () => {
    expect(fileContent).toContain('formatHomepageEventSubtitle');
    expect(fileContent).toContain('<View style={styles.homepageEventBadge}>');
    expect(fileContent).toContain('<Text style={styles.homepageEventBadgeText}>{item.event.event_type}</Text>');
    expect(fileContent).toContain('<Text style={styles.homepageEventSubtitle} numberOfLines={2}>{eventSubtitle}</Text>');
  });

  it('adds homepage sort controls and an events toggle to the shared feed header', () => {
    expect(fileContent).toContain('<View style={styles.newsControlsRow}>');
    expect(fileContent).toContain("Date {homepageFeedSort === 'date_asc' ? '↑' : '↓'}");
    expect(fileContent).toContain("A‑Z {homepageFeedSort === 'za' ? '↑' : '↓'}");
    expect(fileContent).toContain('data-testid="homepage-events-toggle"');
    expect(fileContent).toContain('<Text style={styles.portfolioToggleLabelInline}>Events</Text>');
  });
});
