const fs = require('fs');
const path = require('path');

describe('Stock news sentiment regressions', () => {
  const stockPagePath = path.join(__dirname, '../app/stock/[ticker].tsx');
  let fileContent;

  beforeAll(() => {
    fileContent = fs.readFileSync(stockPagePath, 'utf-8');
  });

  it('shows the aggregate sentiment score and routes helper copy through the shared tooltip on ticker detail', () => {
    expect(fileContent).toContain("import { formatAggregateSentimentLabel, getAggregateSentimentTooltipContent } from '../../utils/sentiment';");
    expect(fileContent).toContain('<View style={styles.aggregateSentimentInfo}>');
    expect(fileContent).toContain('formatAggregateSentimentLabel(aggregateSentiment.label, aggregateSentiment.score)');
    expect(fileContent).toContain("onPress={() => showTooltip('aggregateSentiment')}");
    expect(fileContent).toContain("activeTooltip === 'aggregateSentiment'");
  });

  it('keeps the article logo routed to ticker detail while article taps stay in-app', () => {
    expect(fileContent).toContain('<TouchableOpacity onPress={() => router.push(`/stock/${article.ticker || ticker}`)}>');
    expect(fileContent).toContain('<TouchableOpacity style={styles.newsContent} onPress={() => openArticle(article)}>');
    expect(fileContent).toContain('const formatNewsDate = (dateStr?: string | null): string => {');
    expect(fileContent).toContain('return formatDateDMY(d.toISOString().slice(0, 10));');
    expect(fileContent).toContain('<Text style={styles.newsSubmeta} numberOfLines={1}>');
  });
});
