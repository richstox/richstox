export const AGGREGATE_SENTIMENT_HELPER_TEXT = 'Score = (positive − negative) ÷ total articles. Range: -1.00 bearish to +1.00 bullish.';

type AggregateSentimentBreakdown = {
  total_articles?: number | null;
  positive_count?: number | null;
  negative_count?: number | null;
  neutral_count?: number | null;
};

export const formatAggregateSentimentLabel = (label?: string | null, score?: number | null): string => {
  const normalizedLabel = typeof label === 'string' && label.trim().length > 0
    ? `${label.trim().charAt(0).toUpperCase()}${label.trim().slice(1)}`
    : 'Neutral';
  const normalizedScore = typeof score === 'number' && Number.isFinite(score) ? score : 0;
  return `${normalizedLabel} ${normalizedScore >= 0 ? '+' : ''}${normalizedScore.toFixed(2)}`;
};

export const formatAggregateSentimentHelperText = (aggregateSentiment?: AggregateSentimentBreakdown | null): string => {
  const positiveCount = typeof aggregateSentiment?.positive_count === 'number' && Number.isFinite(aggregateSentiment.positive_count)
    ? Math.max(0, Math.round(aggregateSentiment.positive_count))
    : 0;
  const negativeCount = typeof aggregateSentiment?.negative_count === 'number' && Number.isFinite(aggregateSentiment.negative_count)
    ? Math.max(0, Math.round(aggregateSentiment.negative_count))
    : 0;
  const neutralCount = typeof aggregateSentiment?.neutral_count === 'number' && Number.isFinite(aggregateSentiment.neutral_count)
    ? Math.max(0, Math.round(aggregateSentiment.neutral_count))
    : 0;
  const totalArticles = typeof aggregateSentiment?.total_articles === 'number' && Number.isFinite(aggregateSentiment.total_articles)
    ? Math.max(0, Math.round(aggregateSentiment.total_articles))
    : positiveCount + negativeCount + neutralCount;
  const articleLabel = totalArticles === 1 ? '1 article' : `${totalArticles} articles`;

  return [
    AGGREGATE_SENTIMENT_HELPER_TEXT,
    '+1.00 means all tracked articles are positive. -1.00 means all tracked articles are negative.',
    `Current mix: ${positiveCount} positive • ${negativeCount} negative • ${neutralCount} neutral (${articleLabel}).`,
  ].join('\n');
};
