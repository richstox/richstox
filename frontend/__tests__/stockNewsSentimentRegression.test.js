const fs = require('fs');
const path = require('path');

describe('Stock news sentiment regressions', () => {
  const stockPagePath = path.join(__dirname, '../app/stock/[ticker].tsx');
  let fileContent;

  beforeAll(() => {
    fileContent = fs.readFileSync(stockPagePath, 'utf-8');
  });

  it('shows the aggregate sentiment score and helper range copy on ticker detail', () => {
    expect(fileContent).toContain("import { AGGREGATE_SENTIMENT_HELPER_TEXT, formatAggregateSentimentLabel } from '../../utils/sentiment';");
    expect(fileContent).toContain('<View style={styles.aggregateSentimentInfo}>');
    expect(fileContent).toContain('formatAggregateSentimentLabel(aggregateSentiment.label, aggregateSentiment.score)');
    expect(fileContent).toContain('<Text style={styles.aggregateSentimentHelperText}>{AGGREGATE_SENTIMENT_HELPER_TEXT}</Text>');
  });

  it('keeps the article logo routed to ticker detail while article taps stay in-app', () => {
    expect(fileContent).toContain('<TouchableOpacity onPress={() => router.push(`/stock/${article.ticker || ticker}`)}>');
    expect(fileContent).toContain('<TouchableOpacity style={styles.newsContent} onPress={() => openArticle(article)}>');
  });
});
