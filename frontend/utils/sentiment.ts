export const AGGREGATE_SENTIMENT_HELPER_TEXT = 'Sentiment = (positive − negative) ÷ total articles, shown as -100% to +100%.';

export type AggregateSentimentBreakdown = {
  total_articles?: number | null;
  positive_count?: number | null;
  negative_count?: number | null;
  neutral_count?: number | null;
};

export const getAggregateSentimentTooltipContent = (aggregateSentiment?: AggregateSentimentBreakdown | null) => {
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
  const articleCountLabel = totalArticles === 1 ? '1 article' : `${totalArticles} articles`;

  return {
    title: 'Aggregate Sentiment',
    body: [
      AGGREGATE_SENTIMENT_HELPER_TEXT,
      '+100% means all tracked articles are positive. -100% means all tracked articles are negative.',
    ].join('\n\n'),
    howToRead: `Current mix: ${positiveCount} positive • ${negativeCount} negative • ${neutralCount} neutral (${articleCountLabel}).`,
  };
};

export const formatAggregateSentimentLabel = (label?: string | null, score?: number | null): string => {
  const normalizedLabel = typeof label === 'string' && label.trim().length > 0
    ? `${label.trim().charAt(0).toUpperCase()}${label.trim().slice(1)}`
    : 'Neutral';
  const normalizedScore = typeof score === 'number' && Number.isFinite(score) ? score : 0;
  const percentage = Math.round(normalizedScore * 100);
  return `${normalizedLabel} ${percentage >= 0 ? '+' : ''}${percentage}%`;
};

export const formatAggregateSentimentHelperText = (aggregateSentiment?: AggregateSentimentBreakdown | null): string => {
  const tooltipContent = getAggregateSentimentTooltipContent(aggregateSentiment);
  return [
    tooltipContent.body.replace(/\n\n/g, '\n'),
    tooltipContent.howToRead,
  ].join('\n');
};
