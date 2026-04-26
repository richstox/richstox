export const AGGREGATE_SENTIMENT_HELPER_TEXT = 'Score range: -1.00 to +1.00 • -1 bearish • 0 neutral • +1 bullish';

export const formatAggregateSentimentLabel = (label?: string | null, score?: number | null): string => {
  const normalizedLabel = typeof label === 'string' && label.trim().length > 0
    ? `${label.trim().charAt(0).toUpperCase()}${label.trim().slice(1)}`
    : 'Neutral';
  const normalizedScore = typeof score === 'number' && Number.isFinite(score) ? score : 0;
  return `${normalizedLabel} ${normalizedScore >= 0 ? '+' : ''}${normalizedScore.toFixed(2)}`;
};
