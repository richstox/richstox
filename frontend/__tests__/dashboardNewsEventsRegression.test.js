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
    expect(fileContent).toContain("No news or events available");
  });

  it('renders homepage event badges and subtitles in the shared feed rows', () => {
    expect(fileContent).toContain('formatHomepageEventSubtitle');
    expect(fileContent).toContain('<View style={styles.homepageEventBadge}>');
    expect(fileContent).toContain('<Text style={styles.homepageEventBadgeText}>{item.event.event_type}</Text>');
    expect(fileContent).toContain('<Text style={styles.homepageEventSubtitle} numberOfLines={2}>{eventSubtitle}</Text>');
  });
});
