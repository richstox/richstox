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
});
