export const MEMBERSHIP_PILL_CONFIG: Record<string, { label: string; bg: string; text: string }> = {
  watchlist: { label: 'Watchlist', bg: '#FEF3C7', text: '#B45309' },
  tracklist: { label: 'Tracklist', bg: '#DBEAFE', text: '#1D4ED8' },
  portfolio: { label: 'Portfolio', bg: '#EDE9FE', text: '#7C3AED' },
};

export const getMembershipPillConfig = (membership: unknown) => {
  if (typeof membership !== 'string') return null;
  const trimmed = membership.trim();
  const normalized = trimmed.toLowerCase();
  if (!normalized) return null;
  const fallbackLabel = trimmed === normalized
    ? trimmed.charAt(0).toUpperCase() + trimmed.slice(1)
    : trimmed;
  return MEMBERSHIP_PILL_CONFIG[normalized] ?? {
    label: fallbackLabel,
    bg: '#F3F4F6',
    text: '#374151',
  };
};
