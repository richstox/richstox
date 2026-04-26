import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  ActivityIndicator,
  Image,
  Linking,
  Modal,
  Platform,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useRouter } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import {
  addDays,
  endOfMonth,
  endOfWeek,
  format,
  isSameDay,
  isSameMonth,
  parseISO,
  startOfMonth,
  startOfWeek,
  subDays,
} from 'date-fns';
import { useLayoutSpacing } from '../../constants/layout';
import { API_URL } from '../../utils/config';
import { formatAggregateSentimentLabel } from '../../utils/sentiment';
import AppHeader from '../../components/AppHeader';

const COLORS = {
  primary: '#1E3A5F',
  primarySoft: 'rgba(30, 58, 95, 0.08)',
  accent: '#10B981',
  warning: '#F59E0B',
  danger: '#EF4444',
  text: '#1F2937',
  textLight: '#6B7280',
  textMuted: '#9CA3AF',
  background: '#F5F7FA',
  card: '#FFFFFF',
  border: '#E5E7EB',
};

type EventType = 'earnings' | 'dividend' | 'split' | 'ipo';
type CalendarViewMode = 'daily' | 'monthly' | 'yearly';
type SentimentCategory = 'positive' | 'negative' | 'neutral';

type CalendarEvent = {
  date: string;
  type: EventType;
  ticker: string | null;
  company_name?: string | null;
  logo_url?: string | null;
  label: string;
  description?: string | null;
  amount?: number | null;
  ratio?: string | null;
  estimate?: number | null;
  currency?: string | null;
  metadata?: Record<string, unknown>;
};

type TickerNewsApiArticle = {
  article_id?: string | null;
  title?: string | null;
  published_at?: string | null;
  source_link?: string | null;
  sentiment_label?: SentimentCategory | null;
};

type MarketNewsItem = {
  id: string;
  ticker: string | null;
  company_name?: string | null;
  logo_url?: string;
  fallback_logo_key: string;
  title: string;
  date?: string | null;
  source?: string | null;
  link?: string | null;
  sentiment_label?: SentimentCategory | null;
  scope?: 'market' | 'ticker';
};

type MarketFeedItem =
  | { kind: 'event'; id: string; date: string; event: CalendarEvent }
  | { kind: 'news'; id: string; date?: string | null; news: MarketNewsItem };

type AggregateSentiment = {
  score: number;
  label: SentimentCategory;
  color: string;
  total_articles: number;
  positive_count: number;
  negative_count: number;
  neutral_count: number;
};

const WEEKDAY_LABELS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
const INITIAL_VISIBLE_FEED_ITEMS = 5;
const MAX_VISIBLE_MONTH_CARDS = 4;
const ACTIVE_DAYS_SCROLL_THRESHOLD = 4;
const ACTIVE_DAYS_SCROLL_PERCENTAGE = 0.8;
const MIN_ACTIVE_DAYS_SCROLL_STEP = 140;
const EVENT_TYPE_ORDER: EventType[] = ['earnings', 'dividend', 'split', 'ipo'];
const CALENDAR_VIEW_ORDER: CalendarViewMode[] = ['daily', 'monthly', 'yearly'];
const MARKET_NEWS_PER_TICKER = 3;
// Keep these values aligned with backend/server.py GLOBAL_MARKETS_* constants.
const MARKET_WATCHLIST_TICKER_LIMIT = 10;
const MARKET_TRACKLIST_TICKER_LIMIT = 10;
const MARKET_DIGEST_LIMIT = 100;
const MARKET_NEWS_LIMIT = MARKET_DIGEST_LIMIT + ((MARKET_WATCHLIST_TICKER_LIMIT + MARKET_TRACKLIST_TICKER_LIMIT) * MARKET_NEWS_PER_TICKER);
const YMD_DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

const formatDateDMY = (dateStr: string | null | undefined): string => {
  if (!dateStr || !isValidYmd(dateStr)) return 'N/A';
  const d = parseYmd(dateStr);
  return format(d, 'dd/MM/yyyy');
};

const EVENT_META: Record<EventType, {
  label: string;
  shortLabel: string;
  legendLabel: string;
  singularLabel: string;
  color: string;
  icon: keyof typeof Ionicons.glyphMap;
}> = {
  earnings: { label: 'Earnings', shortLabel: 'EARN', legendLabel: 'E = Earnings', singularLabel: 'Earnings', color: '#3B82F6', icon: 'bar-chart-outline' },
  dividend: { label: 'Dividends', shortLabel: 'DIV', legendLabel: 'D = Dividends', singularLabel: 'Dividend', color: '#10B981', icon: 'cash-outline' },
  split: { label: 'Splits', shortLabel: 'SPLIT', legendLabel: 'S = Splits', singularLabel: 'Split', color: '#F59E0B', icon: 'git-compare-outline' },
  ipo: { label: 'IPOs', shortLabel: 'IPO', legendLabel: 'IPO = IPOs', singularLabel: 'IPO', color: '#A855F7', icon: 'rocket-outline' },
};

const CALENDAR_VIEW_META: Record<CalendarViewMode, { label: string; emptyLabel: string }> = {
  daily: { label: 'Daily', emptyLabel: 'date' },
  monthly: { label: 'Monthly', emptyLabel: 'month' },
  yearly: { label: 'Yearly', emptyLabel: 'year' },
};
const getPragueDateString = (value: Date = new Date()): string => {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Europe/Prague',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(value);
  const year = parts.find((part) => part.type === 'year')?.value ?? '1970';
  const month = parts.find((part) => part.type === 'month')?.value ?? '01';
  const day = parts.find((part) => part.type === 'day')?.value ?? '01';
  return `${year}-${month}-${day}`;
};

const parseYmd = (value: string): Date => parseISO(`${value}T00:00:00Z`);

const parseEventNumber = (value?: number | string | null): number | null => {
  const numericValue = typeof value === 'string' ? Number(value) : value;
  return typeof numericValue === 'number' && Number.isFinite(numericValue) ? numericValue : null;
};

const formatEventAmount = (amount?: number | string | null, currency?: string | null): string | null => {
  const numericAmount = parseEventNumber(amount);
  if (numericAmount == null) return null;
  const prefix = currency && currency !== 'USD' ? `${currency} ` : '$';
  return `${prefix}${numericAmount.toFixed(2)}`;
};

const hashSymbolToColor = (symbol: string) => {
  const colors = ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#EC4899', '#06B6D4', '#84CC16'];
  let hash = 0;
  for (let i = 0; i < symbol.length; i++) {
    hash = ((hash << 5) - hash + symbol.charCodeAt(i)) | 0;
  }
  return colors[Math.abs(hash) % colors.length];
};

const getEventFallbackKey = (ticker?: string | null, companyName?: string | null): string => {
  if (ticker?.trim()) return ticker.trim().charAt(0).toUpperCase();
  if (companyName?.trim()) return companyName.trim().charAt(0).toUpperCase();
  return '?';
};

const resolveEventLogoUrl = (rawUrl?: string | null, ticker?: string | null): string | undefined => {
  const normalizedTicker = ticker?.trim().toUpperCase();
  if (!rawUrl && normalizedTicker) return `${API_URL}/api/logo/${normalizedTicker}`;
  if (!rawUrl) return undefined;
  return rawUrl.startsWith('http') ? rawUrl : `${API_URL}${rawUrl}`;
};

const getMarketNewsSource = (sourceLink?: string | null): string | null => {
  if (!sourceLink) return null;
  try {
    return new URL(sourceLink).hostname.replace(/^www\./, '');
  } catch {
    return null;
  }
};

const getMarketNewsDateLabel = (dateStr?: string | null): string => {
  if (!dateStr) return 'Latest';
  const parsed = new Date(dateStr);
  if (Number.isNaN(parsed.getTime())) return 'Latest';
  return parsed.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
};

const getSentimentTone = (sentiment?: SentimentCategory | null) => {
  if (sentiment === 'positive') {
    return { backgroundColor: '#D1FAE5', color: COLORS.accent, label: 'Positive' };
  }
  if (sentiment === 'negative') {
    return { backgroundColor: '#FEE2E2', color: COLORS.danger, label: 'Negative' };
  }
  return { backgroundColor: '#E0E7FF', color: '#4F46E5', label: 'News' };
};

const generateNewsItemId = (ticker: string, article: TickerNewsApiArticle, index: number): string => {
  return (
    article.article_id ||
    article.source_link ||
    `${encodeURIComponent(ticker)}-${encodeURIComponent(article.title ?? 'untitled')}-${encodeURIComponent(article.published_at ?? 'no-date')}-${index}`
  );
};

const isValidYmd = (value: string): boolean => {
  if (!YMD_DATE_PATTERN.test(value)) return false;
  const parsed = parseYmd(value);
  return !Number.isNaN(parsed.getTime()) && format(parsed, 'yyyy-MM-dd') === value;
};

const EventLogo = ({ logoUrl, fallbackKey }: { logoUrl?: string; fallbackKey: string }) => {
  const [imageError, setImageError] = useState(false);

  if (!logoUrl || imageError) {
    return (
      <View style={[styles.eventLogoFallback, { backgroundColor: hashSymbolToColor(fallbackKey) }]}>
        <Text style={styles.eventLogoFallbackText}>{fallbackKey}</Text>
      </View>
    );
  }

  return (
    <Image
      source={{ uri: logoUrl }}
      style={styles.eventLogo}
      onError={() => setImageError(true)}
    />
  );
};

export default function Markets() {
  const router = useRouter();
  const sp = useLayoutSpacing();

  const todayPragueStr = getPragueDateString();
  const todayPrague = parseYmd(todayPragueStr);
  const todayMonthKey = todayPragueStr.slice(0, 7);
  const rangeStart = subDays(todayPrague, 1);
  const rangeEnd = addDays(todayPrague, 90);
  const rangeStartStr = format(rangeStart, 'yyyy-MM-dd');
  const rangeEndStr = format(rangeEnd, 'yyyy-MM-dd');
  const currentYear = Number(todayPragueStr.slice(0, 4));

  const [events, setEvents] = useState<CalendarEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [calendarView, setCalendarView] = useState<CalendarViewMode>('daily');
  const [selectedDate, setSelectedDate] = useState<Date>(todayPrague);
  const [displayMonth, setDisplayMonth] = useState<Date>(startOfMonth(todayPrague));
  const [selectedYear, setSelectedYear] = useState<number>(currentYear);
  const [selectedEventType, setSelectedEventType] = useState<EventType>('earnings');
  const [tickerFilter, setTickerFilter] = useState('');
  const [visibleFeedLimit, setVisibleFeedLimit] = useState(INITIAL_VISIBLE_FEED_ITEMS);
  const [isCalendarExpanded, setIsCalendarExpanded] = useState(false);
  const [isFullCalendarExpanded, setIsFullCalendarExpanded] = useState(false);
  const [calendarPickerVisible, setCalendarPickerVisible] = useState(false);
  const [includeNews, setIncludeNews] = useState(true);
  const [newsItems, setNewsItems] = useState<MarketNewsItem[]>([]);
  const [newsTotalCount, setNewsTotalCount] = useState(0);
  const [aggregateSentiment, setAggregateSentiment] = useState<AggregateSentiment | null>(null);
  const [newsLoading, setNewsLoading] = useState(false);
  const [newsError, setNewsError] = useState<string | null>(null);
  const [activeDaysScrollX, setActiveDaysScrollX] = useState(0);
  const [activeDaysContentWidth, setActiveDaysContentWidth] = useState(0);
  const [activeDaysLayoutWidth, setActiveDaysLayoutWidth] = useState(0);
  const activeDaysScrollRef = useRef<ScrollView>(null);

  useEffect(() => {
    const fetchEvents = async () => {
      try {
        setLoading(true);
        setError(null);
        const response = await fetch(`${API_URL}/api/v1/calendar/events?from=${rangeStartStr}&to=${rangeEndStr}`);
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        const payload = await response.json();
        setEvents(Array.isArray(payload?.events) ? payload.events : []);
      } catch (err: any) {
        console.error('Error fetching calendar events:', err);
        setError('Could not load calendar events');
        setEvents([]);
      } finally {
        setLoading(false);
      }
    };

    fetchEvents();
  }, [rangeEndStr, rangeStartStr]);

  const eventsByDate = useMemo(() => {
    return events.reduce<Record<string, CalendarEvent[]>>((acc, event) => {
      if (!acc[event.date]) acc[event.date] = [];
      acc[event.date].push(event);
      return acc;
    }, {});
  }, [events]);

  const selectedDateKey = format(selectedDate, 'yyyy-MM-dd');
  const selectedEvents = useMemo(
    () => eventsByDate[selectedDateKey] || [],
    [eventsByDate, selectedDateKey],
  );

  const selectedMonthKey = format(displayMonth, 'yyyy-MM');
  const selectedYearKey = String(selectedYear);

  const activeDateKeys = useMemo(
    () => Array.from(new Set(events.map((event) => event.date).filter(isValidYmd))).sort(),
    [events],
  );

  const activeMonthKeys = useMemo(
    () => Array.from(new Set(activeDateKeys.map((dateKey) => dateKey.slice(0, 7)))).sort(),
    [activeDateKeys],
  );

  const activeYearKeys = useMemo(
    () => Array.from(new Set(activeDateKeys.map((dateKey) => dateKey.slice(0, 4)))).sort(),
    [activeDateKeys],
  );

  const activeDayKeysForDisplayMonth = useMemo(
    () => activeDateKeys.filter((dateKey) => dateKey.startsWith(selectedMonthKey)),
    [activeDateKeys, selectedMonthKey],
  );

  const monthlyEventCounts = useMemo(() => {
    return events.reduce<Record<string, number>>((acc, event) => {
      const monthKey = event.date.slice(0, 7);
      acc[monthKey] = (acc[monthKey] || 0) + 1;
      return acc;
    }, {});
  }, [events]);

  const yearlyEventCounts = useMemo(() => {
    return events.reduce<Record<string, number>>((acc, event) => {
      const yearKey = event.date.slice(0, 4);
      acc[yearKey] = (acc[yearKey] || 0) + 1;
      return acc;
    }, {});
  }, [events]);

  const monthCards = useMemo(() => {
    return activeMonthKeys
      .filter((monthKey) => monthKey.startsWith(`${selectedYearKey}-`))
      .slice(0, MAX_VISIBLE_MONTH_CARDS)
      .map((monthKey) => startOfMonth(parseYmd(`${monthKey}-01`)));
  }, [activeMonthKeys, selectedYearKey]);

  const yearCards = useMemo(() => {
    return activeYearKeys.map((yearKey) => Number(yearKey));
  }, [activeYearKeys]);

  const periodEvents = useMemo(() => {
    if (calendarView === 'daily') return selectedEvents;
    if (calendarView === 'monthly') {
      return events.filter((event) => event.date.startsWith(selectedMonthKey));
    }
    return events.filter((event) => event.date.startsWith(`${selectedYearKey}-`));
  }, [calendarView, events, selectedEvents, selectedMonthKey, selectedYearKey]);

  const selectedEventCounts = useMemo(() => {
    return periodEvents.reduce<Record<EventType, number>>((acc, event) => {
      acc[event.type] += 1;
      return acc;
    }, {
      earnings: 0,
      dividend: 0,
      split: 0,
      ipo: 0,
    });
  }, [periodEvents]);

  const selectedPeriodLabel = useMemo(() => {
    if (calendarView === 'daily') return formatDateDMY(selectedDateKey);
    if (calendarView === 'monthly') return format(displayMonth, 'MMMM yyyy');
    return selectedYearKey;
  }, [calendarView, displayMonth, selectedDateKey, selectedYearKey]);

  useEffect(() => {
    setTickerFilter('');
    setVisibleFeedLimit(INITIAL_VISIBLE_FEED_ITEMS);
  }, [calendarView, selectedDateKey, selectedMonthKey, selectedYearKey]);

  useEffect(() => {
    setIsFullCalendarExpanded(false);
  }, [calendarView, selectedMonthKey]);

  useEffect(() => {
    setSelectedYear(Number(format(displayMonth, 'yyyy')));
  }, [displayMonth]);

  useEffect(() => {
    // Keep the compact month/day selectors pinned to real event dates after fresh data loads.
    if (activeMonthKeys.length === 0 || activeMonthKeys.includes(selectedMonthKey)) return;
    const nextMonthKey = activeMonthKeys.find((monthKey) => monthKey >= todayMonthKey) ?? activeMonthKeys[0];
    setDisplayMonth(startOfMonth(parseYmd(`${nextMonthKey}-01`)));
  }, [activeMonthKeys, selectedMonthKey, todayMonthKey]);

  useEffect(() => {
    if (calendarView !== 'daily' || activeDayKeysForDisplayMonth.length === 0) return;
    if (activeDayKeysForDisplayMonth.includes(selectedDateKey)) return;
    const nextDateKey = activeDayKeysForDisplayMonth.find((dateKey) => dateKey >= todayPragueStr) ?? activeDayKeysForDisplayMonth[0];
    if (nextDateKey) setSelectedDate(parseYmd(nextDateKey));
  }, [activeDayKeysForDisplayMonth, calendarView, selectedDateKey, todayPragueStr]);

  useEffect(() => {
    const nextType = EVENT_TYPE_ORDER.find((type) => selectedEventCounts[type] > 0) ?? 'earnings';
    if (selectedEventCounts[selectedEventType] === 0 && nextType !== selectedEventType) {
      setSelectedEventType(nextType);
      setVisibleFeedLimit(INITIAL_VISIBLE_FEED_ITEMS);
      setTickerFilter('');
    }
  }, [selectedEventCounts, selectedEventType]);

  useEffect(() => {
    activeDaysScrollRef.current?.scrollTo({ x: 0, animated: false });
  }, [selectedMonthKey]);

  const typeFilteredEvents = useMemo(
    () => periodEvents.filter((event) => event.type === selectedEventType),
    [periodEvents, selectedEventType],
  );

  const normalizedTickerFilter = tickerFilter.trim().toLowerCase();
  const visibleEvents = useMemo(() => {
    if (!normalizedTickerFilter) return typeFilteredEvents;
    return typeFilteredEvents.filter((event) => {
      const haystack = [
        event.ticker,
        event.company_name,
        event.label,
        event.description,
      ]
        .filter(Boolean)
        .join(' ')
        .toLowerCase();
      return haystack.includes(normalizedTickerFilter);
    });
  }, [normalizedTickerFilter, typeFilteredEvents]);

  const visibleNewsItems = useMemo(() => {
    if (!includeNews) return [];
    if (!normalizedTickerFilter) return newsItems;
    return newsItems.filter((item) => {
      const haystack = [
        item.ticker,
        item.company_name,
        item.title,
        item.source,
      ]
        .filter(Boolean)
        .join(' ')
        .toLowerCase();
      return haystack.includes(normalizedTickerFilter);
    });
  }, [includeNews, newsItems, normalizedTickerFilter]);

  useEffect(() => {
    setVisibleFeedLimit(INITIAL_VISIBLE_FEED_ITEMS);
  }, [includeNews, normalizedTickerFilter]);

  const filteredFeedItems = useMemo<MarketFeedItem[]>(() => {
    const eventItems = visibleEvents.map((event, index) => ({
      kind: 'event' as const,
      id: `${event.type}-${event.ticker || 'na'}-${event.date}-${index}`,
      date: event.date,
      event,
    }));
    const mergedItems: MarketFeedItem[] = [
      ...eventItems,
      ...visibleNewsItems.map((news) => ({
        kind: 'news' as const,
        id: news.id,
        date: news.date,
        news,
      })),
    ];

    return mergedItems.sort((left, right) => {
      const leftDate = left.date
        ? Date.parse(YMD_DATE_PATTERN.test(left.date) ? `${left.date}T00:00:00Z` : left.date)
        : 0;
      const rightDate = right.date
        ? Date.parse(YMD_DATE_PATTERN.test(right.date) ? `${right.date}T00:00:00Z` : right.date)
        : 0;
      return (Number.isFinite(rightDate) ? rightDate : 0) - (Number.isFinite(leftDate) ? leftDate : 0);
    });
  }, [visibleEvents, visibleNewsItems]);

  const displayedFeedItems = useMemo(
    () => filteredFeedItems.slice(0, visibleFeedLimit),
    [filteredFeedItems, visibleFeedLimit],
  );

  useEffect(() => {
    let cancelled = false;

    const fetchVisibleTickerNews = async () => {
      if (!includeNews) {
        setNewsItems([]);
        setNewsTotalCount(0);
        setAggregateSentiment(null);
        setNewsError(null);
        setNewsLoading(false);
        return;
      }

      try {
        setNewsLoading(true);
        setNewsError(null);
        const response = await fetch(
          `${API_URL}/api/v1/markets/news?limit=${MARKET_NEWS_LIMIT}&market_limit=${MARKET_DIGEST_LIMIT}&per_ticker_limit=${MARKET_NEWS_PER_TICKER}&offset=0`,
        );
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        const payload = await response.json();

        if (cancelled) return;

        const seenIds = new Set<string>();
        const mergedItems = (Array.isArray(payload?.news) ? payload.news : [])
          .map((item: any, index: number) => {
            const ticker = typeof item?.ticker === 'string' ? item.ticker.trim().toUpperCase() : null;
            const sourceLink = item?.link ?? null;
            return {
              id: item?.id || generateNewsItemId(ticker ?? 'MARKET', {
                article_id: item?.id ?? null,
                title: item?.title ?? null,
                published_at: item?.date ?? null,
                source_link: sourceLink,
              }, index),
              ticker,
              company_name: item?.scope === 'market' ? null : (item?.company_name ?? null),
              logo_url: item?.scope === 'market' ? undefined : item?.logo_url,
              fallback_logo_key: item?.fallback_logo_key || getEventFallbackKey(ticker, item?.company_name ?? null),
              title: item?.title || `${ticker ?? 'Market'} news`,
              date: item?.date ?? null,
              source: item?.source ?? getMarketNewsSource(sourceLink),
              link: sourceLink,
              sentiment_label: item?.sentiment_label ?? null,
              scope: item?.scope === 'market' ? 'market' : 'ticker',
            } as MarketNewsItem;
          })
          .filter((item) => {
            if (seenIds.has(item.id)) return false;
            seenIds.add(item.id);
            return true;
          })
          .sort((a, b) => {
            const left = a.date ? Date.parse(a.date) : 0;
            const right = b.date ? Date.parse(b.date) : 0;
            return (Number.isFinite(right) ? right : 0) - (Number.isFinite(left) ? left : 0);
          });

        setNewsItems(mergedItems);
        setNewsTotalCount(typeof payload?.total_news_count === 'number' ? payload.total_news_count : mergedItems.length);
        setAggregateSentiment(payload?.aggregate_sentiment ?? null);
      } catch (err) {
        if (cancelled) return;
        console.error('Error fetching Markets news:', err);
        setNewsItems([]);
        setNewsTotalCount(0);
        setAggregateSentiment(null);
        setNewsError('Could not load news');
      } finally {
        if (!cancelled) setNewsLoading(false);
      }
    };

    void fetchVisibleTickerNews();

    return () => {
      cancelled = true;
    };
  }, [includeNews]);

  const shouldShowActiveDaysArrows = activeDaysContentWidth > activeDaysLayoutWidth + ACTIVE_DAYS_SCROLL_THRESHOLD;
  const canScrollActiveDaysLeft = activeDaysScrollX > ACTIVE_DAYS_SCROLL_THRESHOLD;
  const canScrollActiveDaysRight =
    activeDaysContentWidth - activeDaysLayoutWidth - activeDaysScrollX > ACTIVE_DAYS_SCROLL_THRESHOLD;
  const scrollActiveDaysBy = (scrollDirection: 'left' | 'right') => {
    const step = Math.max(activeDaysLayoutWidth * ACTIVE_DAYS_SCROLL_PERCENTAGE, MIN_ACTIVE_DAYS_SCROLL_STEP);
    activeDaysScrollRef.current?.scrollTo({
      x: Math.max(0, activeDaysScrollX + (scrollDirection === 'right' ? step : -step)),
      animated: true,
    });
  };

  const calendarDays = useMemo(() => {
    const monthStart = startOfMonth(displayMonth);
    const monthEnd = endOfMonth(displayMonth);
    const gridStart = startOfWeek(monthStart, { weekStartsOn: 1 });
    const gridEnd = endOfWeek(monthEnd, { weekStartsOn: 1 });
    const days: Date[] = [];
    let cursor = gridStart;
    while (cursor <= gridEnd) {
      days.push(cursor);
      cursor = addDays(cursor, 1);
    }
    return days;
  }, [displayMonth]);

  const selectedMonthIndex = activeMonthKeys.indexOf(selectedMonthKey);
  const selectedYearIndex = yearCards.indexOf(selectedYear);
  const canGoPrev = selectedMonthIndex > 0;
  const canGoNext = selectedMonthIndex >= 0 && selectedMonthIndex < activeMonthKeys.length - 1;
  const canGoPrevYear = selectedYearIndex > 0;
  const canGoNextYear = selectedYearIndex >= 0 && selectedYearIndex < yearCards.length - 1;
  const getYearMonthKey = (year: number, edge: 'first' | 'last'): string | undefined => {
    const yearMonthKeys = activeMonthKeys.filter((monthKey) => monthKey.startsWith(`${year}-`));
    if (yearMonthKeys.length === 0) return undefined;
    return edge === 'first' ? yearMonthKeys[0] : yearMonthKeys[yearMonthKeys.length - 1];
  };

  const formatEventSecondary = (event: CalendarEvent): string => {
    if (event.type === 'dividend') {
      const details: string[] = [];
      const formattedAmount = formatEventAmount(event.amount, event.currency);
      if (formattedAmount) details.push(formattedAmount);
      const payDate = typeof event.metadata?.pay_date === 'string' ? event.metadata.pay_date : null;
      if (payDate && isValidYmd(payDate)) details.push(`Pay ${formatDateDMY(payDate)}`);
      return details.join(' • ') || (event.description || 'Upcoming dividend');
    }
    if (event.type === 'split') {
      return event.ratio || event.description || 'Upcoming split';
    }
    if (event.type === 'earnings') {
      const details: string[] = [];
      const formattedEstimate = formatEventAmount(event.estimate, event.currency);
      if (formattedEstimate) details.push(`Exp. ${formattedEstimate}`);
      if (event.description) details.push(event.description);
      return details.join(' • ') || 'Scheduled earnings';
    }
    if (event.type === 'ipo') {
      const details: string[] = [];
      const formattedAmount = formatEventAmount(event.amount, null);
      if (formattedAmount) details.push(`IPO ${formattedAmount}`);
      const priceFrom = typeof event.metadata?.price_from === 'number' ? event.metadata.price_from : null;
      const priceTo = typeof event.metadata?.price_to === 'number' ? event.metadata.price_to : null;
      if (event.amount == null && priceFrom != null && priceTo != null && priceFrom > 0 && priceTo > 0) {
        details.push(`Range $${priceFrom.toFixed(2)}-$${priceTo.toFixed(2)}`);
      }
      if (event.description) details.push(event.description);
      return details.join(' • ') || 'Upcoming IPO';
    }
    return event.description || 'Scheduled';
  };

  const openNewsItem = async (item: MarketNewsItem) => {
    if (item.link) {
      try {
        await Linking.openURL(item.link);
        return;
      } catch (err) {
        console.error('Error opening Markets news article:', err);
      }
    }
    if (item.ticker) {
      router.push(`/stock/${item.ticker}`);
    }
  };

  return (
    <SafeAreaView style={styles.container} edges={['top']}>
      <AppHeader title="Markets" />

      <ScrollView style={styles.scroll} contentContainerStyle={{ padding: sp.pageGutter, gap: 12 }}>
        {isCalendarExpanded ? (
        <View style={styles.card}>
          <View style={styles.cardHeader}>
            <View>
              <View style={styles.sectionTitleRow}>
                <Ionicons name="calendar-clear-outline" size={18} color={COLORS.primary} />
                <Text style={styles.sectionTitle}>Calendar</Text>
              </View>
              <Text style={styles.sectionSubtitle}>For next 3 months</Text>
            </View>
            <TouchableOpacity
              onPress={() => setIsCalendarExpanded((prev) => !prev)}
              accessibilityRole="button"
              accessibilityLabel="Hide details"
            >
              <Text style={styles.calendarHeaderActionText}>Hide Details</Text>
            </TouchableOpacity>
          </View>

          {calendarView === 'daily' ? (
            <>
              <View style={styles.monthHeader}>
                <TouchableOpacity
                  style={[styles.monthNavButton, !canGoPrev && styles.monthNavButtonDisabled]}
                  onPress={() => {
                    if (!canGoPrev) return;
                    setDisplayMonth(startOfMonth(parseYmd(`${activeMonthKeys[selectedMonthIndex - 1]}-01`)));
                  }}
                  disabled={!canGoPrev}
                >
                  <Ionicons name="chevron-back" size={16} color={canGoPrev ? COLORS.primary : COLORS.textMuted} />
                </TouchableOpacity>
                <Text style={styles.monthTitle}>{format(displayMonth, 'MMMM yyyy')}</Text>
                <TouchableOpacity
                  style={[styles.monthNavButton, !canGoNext && styles.monthNavButtonDisabled]}
                  onPress={() => {
                    if (!canGoNext) return;
                    setDisplayMonth(startOfMonth(parseYmd(`${activeMonthKeys[selectedMonthIndex + 1]}-01`)));
                  }}
                  disabled={!canGoNext}
                >
                  <Ionicons name="chevron-forward" size={16} color={canGoNext ? COLORS.primary : COLORS.textMuted} />
                </TouchableOpacity>
              </View>

              <View style={styles.activeDaysScrollerRow}>
                {shouldShowActiveDaysArrows && (
                  <TouchableOpacity
                    style={[styles.horizontalNavButton, !canScrollActiveDaysLeft && styles.horizontalNavButtonDisabled]}
                    onPress={() => scrollActiveDaysBy('left')}
                    disabled={!canScrollActiveDaysLeft}
                  >
                    <Ionicons name="chevron-back" size={16} color={canScrollActiveDaysLeft ? COLORS.primary : COLORS.textMuted} />
                  </TouchableOpacity>
                )}
                <ScrollView
                  ref={activeDaysScrollRef}
                  horizontal
                  showsHorizontalScrollIndicator={false}
                  style={styles.activeDaysScroll}
                  contentContainerStyle={styles.activeDaysScrollContent}
                  onScroll={(event) => setActiveDaysScrollX(event.nativeEvent.contentOffset.x)}
                  onContentSizeChange={(width) => setActiveDaysContentWidth(width)}
                  onLayout={(event) => setActiveDaysLayoutWidth(event.nativeEvent.layout.width)}
                  scrollEventThrottle={16}
                >
                  {activeDayKeysForDisplayMonth.map((dayKey) => {
                    const day = parseYmd(dayKey);
                    const isSelected = dayKey === selectedDateKey;
                    return (
                      <TouchableOpacity
                        key={dayKey}
                        style={[styles.activeDayCard, isSelected && styles.activeDayCardSelected]}
                        onPress={() => setSelectedDate(day)}
                      >
                        <Text style={[styles.activeDayWeekday, isSelected && styles.activeDayTextSelected]}>
                          {format(day, 'EEE')}
                        </Text>
                        <Text style={[styles.activeDayNumber, isSelected && styles.activeDayTextSelected]}>
                          {format(day, 'd')}
                        </Text>
                      </TouchableOpacity>
                    );
                  })}
                </ScrollView>
                {shouldShowActiveDaysArrows && (
                  <TouchableOpacity
                    style={[styles.horizontalNavButton, !canScrollActiveDaysRight && styles.horizontalNavButtonDisabled]}
                    onPress={() => scrollActiveDaysBy('right')}
                    disabled={!canScrollActiveDaysRight}
                  >
                    <Ionicons name="chevron-forward" size={16} color={canScrollActiveDaysRight ? COLORS.primary : COLORS.textMuted} />
                  </TouchableOpacity>
                )}
              </View>

              <TouchableOpacity
                style={styles.calendarToggleButton}
                onPress={() => setIsFullCalendarExpanded((prev) => !prev)}
                accessibilityRole="button"
                accessibilityLabel={isFullCalendarExpanded ? 'Hide full calendar' : 'Show full calendar'}
              >
                <Text style={styles.calendarToggleText}>
                  {isFullCalendarExpanded ? 'Hide full calendar' : 'Show full calendar'}
                </Text>
                <Ionicons
                  name={isFullCalendarExpanded ? 'chevron-up' : 'chevron-down'}
                  size={16}
                  color={COLORS.primary}
                />
              </TouchableOpacity>

              {isFullCalendarExpanded && (
                <>
                  <View style={styles.weekdayRow}>
                    {WEEKDAY_LABELS.map((day) => (
                      <Text key={day} style={styles.weekdayLabel}>{day}</Text>
                    ))}
                  </View>

                  <View style={styles.calendarGrid}>
                    {calendarDays.map((day) => {
                      const dayKey = format(day, 'yyyy-MM-dd');
                      const inRange = day >= rangeStart && day <= rangeEnd;
                      const isSelected = isSameDay(day, selectedDate);
                      const dayEvents = eventsByDate[dayKey] || [];
                      return (
                        <TouchableOpacity
                          key={dayKey}
                          style={[
                            styles.dayCell,
                            !isSameMonth(day, displayMonth) && styles.dayCellOutsideMonth,
                            isSelected && styles.dayCellSelected,
                            !inRange && styles.dayCellDisabled,
                          ]}
                          onPress={() => inRange && setSelectedDate(day)}
                          disabled={!inRange}
                        >
                          <Text
                            style={[
                              styles.dayLabel,
                              !isSameMonth(day, displayMonth) && styles.dayLabelOutsideMonth,
                              isSelected && styles.dayLabelSelected,
                              !inRange && styles.dayLabelDisabled,
                            ]}
                          >
                            {format(day, 'd')}
                          </Text>
                          {dayEvents.length > 0 && (
                            <View style={[styles.dayDot, isSelected && styles.dayDotSelected]}>
                              <Text style={[styles.dayDotText, isSelected && styles.dayDotTextSelected]}>{dayEvents.length}</Text>
                            </View>
                          )}
                        </TouchableOpacity>
                      );
                    })}
                  </View>
                </>
              )}
            </>
          ) : calendarView === 'monthly' ? (
            <>
              <View style={styles.monthHeader}>
                <TouchableOpacity
                  style={[styles.monthNavButton, !canGoPrevYear && styles.monthNavButtonDisabled]}
                  onPress={() => {
                    if (!canGoPrevYear) return;
                    const nextYear = yearCards[selectedYearIndex - 1];
                    const nextMonthKey = getYearMonthKey(nextYear, 'last');
                    setSelectedYear(nextYear);
                    setDisplayMonth(startOfMonth(parseYmd(`${(nextMonthKey ?? `${nextYear}-01`)}-01`)));
                  }}
                  disabled={!canGoPrevYear}
                >
                  <Ionicons name="chevron-back" size={16} color={canGoPrevYear ? COLORS.primary : COLORS.textMuted} />
                </TouchableOpacity>
                <Text style={styles.monthTitle}>{selectedYear}</Text>
                <TouchableOpacity
                  style={[styles.monthNavButton, !canGoNextYear && styles.monthNavButtonDisabled]}
                  onPress={() => {
                    if (!canGoNextYear) return;
                    const nextYear = yearCards[selectedYearIndex + 1];
                    const nextMonthKey = getYearMonthKey(nextYear, 'first');
                    setSelectedYear(nextYear);
                    setDisplayMonth(startOfMonth(parseYmd(`${(nextMonthKey ?? `${nextYear}-01`)}-01`)));
                  }}
                  disabled={!canGoNextYear}
                >
                  <Ionicons name="chevron-forward" size={16} color={canGoNextYear ? COLORS.primary : COLORS.textMuted} />
                </TouchableOpacity>
              </View>
              <View style={styles.periodGrid}>
                {monthCards.map((month) => {
                  const monthKey = format(month, 'yyyy-MM');
                  const inRange = monthKey >= rangeStartStr.slice(0, 7) && monthKey <= rangeEndStr.slice(0, 7);
                  const isSelected = monthKey === selectedMonthKey;
                  return (
                    <TouchableOpacity
                      key={monthKey}
                      style={[
                        styles.periodCell,
                        isSelected && styles.periodCellSelected,
                        !inRange && styles.periodCellDisabled,
                      ]}
                      onPress={() => {
                        if (!inRange) return;
                        setDisplayMonth(startOfMonth(month));
                        setCalendarView('monthly');
                      }}
                      disabled={!inRange}
                    >
                      <Text style={[styles.periodCellLabel, isSelected && styles.periodCellLabelSelected]}>
                        {format(month, 'MMM')}
                      </Text>
                      <Text style={[styles.periodCellCount, isSelected && styles.periodCellCountSelected]}>
                        {monthlyEventCounts[monthKey] || 0}
                      </Text>
                    </TouchableOpacity>
                  );
                })}
              </View>
            </>
          ) : (
            <View style={styles.periodGrid}>
              {yearCards.map((year) => {
                const yearKey = String(year);
                const isSelected = year === selectedYear;
                return (
                  <TouchableOpacity
                    key={yearKey}
                    style={[styles.periodCell, styles.yearCell, isSelected && styles.periodCellSelected]}
                    onPress={() => {
                      setSelectedYear(year);
                      const nextMonthKey = getYearMonthKey(year, 'first');
                      setDisplayMonth(startOfMonth(parseYmd(`${(nextMonthKey ?? `${year}-01`)}-01`)));
                    }}
                  >
                    <Text style={[styles.periodCellLabel, isSelected && styles.periodCellLabelSelected]}>
                      {yearKey}
                    </Text>
                    <Text style={[styles.periodCellCount, isSelected && styles.periodCellCountSelected]}>
                      {yearlyEventCounts[yearKey] || 0}
                    </Text>
                  </TouchableOpacity>
                );
              })}
            </View>
          )}
        </View>
        ) : null}

        <View style={styles.card}>
          <View style={styles.eventsHeader}>
            <View style={styles.eventsHeaderTop}>
              <View style={styles.eventsTitleBlock}>
                <View style={styles.sectionTitleRow}>
                  <Ionicons name="newspaper-outline" size={18} color={COLORS.primary} />
                  <Text style={styles.sectionTitle}>Events & News</Text>
                </View>
                <View style={styles.eventsDateRow}>
                  <Text style={styles.eventsDateTitle}>{selectedPeriodLabel}</Text>
                  <TouchableOpacity onPress={() => setCalendarPickerVisible(true)} accessibilityRole="button">
                    <Text style={styles.eventsDateSelectText}>Select</Text>
                  </TouchableOpacity>
                </View>
                <Text style={styles.sectionSubtitle}>
                  {includeNews ? `${periodEvents.length} events • ${newsTotalCount} news` : `${periodEvents.length} events`}
                </Text>
              </View>
              <View style={styles.eventsHeaderActions}>
                {includeNews && aggregateSentiment && (
                  <View
                    style={[
                      styles.aggregateSentimentBadge,
                      styles.aggregateSentimentHeadlineBadge,
                      { backgroundColor: `${aggregateSentiment.color}20` },
                    ]}
                  >
                    <View style={[styles.aggregateSentimentDot, { backgroundColor: aggregateSentiment.color }]} />
                    <Text style={[styles.aggregateSentimentText, { color: aggregateSentiment.color }]}>
                      {formatAggregateSentimentLabel(aggregateSentiment.label, aggregateSentiment.score)}
                    </Text>
                  </View>
                )}
                <TouchableOpacity
                  style={styles.portfolioToggleInline}
                  onPress={() => setIncludeNews((prev) => !prev)}
                  accessibilityRole="switch"
                  accessibilityLabel="Toggle market news"
                  accessibilityState={{ checked: includeNews }}
                >
                  <Text style={styles.portfolioToggleLabelInline}>+News</Text>
                  <View style={[styles.toggleSwitch, includeNews && styles.toggleSwitchOn]}>
                    <View style={[styles.toggleKnob, includeNews && styles.toggleKnobOn]} />
                  </View>
                </TouchableOpacity>
              </View>
            </View>
          </View>

          {loading ? (
            <View style={styles.loadingWrap}>
              <ActivityIndicator size="small" color={COLORS.primary} />
            </View>
          ) : error ? (
            <Text style={styles.errorText}>{error}</Text>
          ) : periodEvents.length === 0 && (!includeNews || (!newsLoading && !newsError && newsItems.length === 0)) ? (
            <View style={styles.emptyWrap}>
              <Ionicons name="calendar-outline" size={28} color={COLORS.textMuted} />
              <Text style={styles.emptyText}>No events for this {CALENDAR_VIEW_META[calendarView].emptyLabel}</Text>
            </View>
          ) : (
            <>
              <View style={styles.eventTabsRow}>
                {EVENT_TYPE_ORDER.map((type) => {
                  const meta = EVENT_META[type];
                  const isActive = selectedEventType === type;
                  const isDisabled = selectedEventCounts[type] === 0;
                  return (
                    <TouchableOpacity
                      key={type}
                      style={[styles.eventTab, isActive && styles.eventTabActive, isDisabled && styles.eventTabDisabled]}
                      onPress={() => {
                        setSelectedEventType(type);
                        setVisibleFeedLimit(INITIAL_VISIBLE_FEED_ITEMS);
                      }}
                      disabled={isDisabled}
                      accessibilityRole="button"
                    >
                      <View style={styles.eventTabLabelRow}>
                        <View style={[styles.eventTabDot, { backgroundColor: meta.color }]} />
                        <Text style={[styles.eventTabText, isActive && styles.eventTabTextActive, isDisabled && styles.eventTabTextDisabled]}>
                          {meta.shortLabel}
                        </Text>
                      </View>
                      <Text style={[styles.eventTabCountText, isActive && styles.eventTabCountTextActive, isDisabled && styles.eventTabTextDisabled]}>
                        {selectedEventCounts[type]}
                      </Text>
                    </TouchableOpacity>
                  );
                })}
              </View>
              <View style={styles.filterSearchWrap}>
                <Ionicons name="search" size={20} color={COLORS.textMuted} />
                <TextInput
                  style={[
                    styles.filterSearchInput,
                    Platform.OS === 'web' ? { outlineStyle: 'none', outlineWidth: 0 } : null,
                  ]}
                  placeholder="Search ticker or company"
                  placeholderTextColor={COLORS.textMuted}
                  value={tickerFilter}
                  onChangeText={setTickerFilter}
                  autoCapitalize="characters"
                  autoCorrect={false}
                />
                {tickerFilter.length > 0 && (
                  <TouchableOpacity onPress={() => setTickerFilter('')}>
                    <Ionicons name="close-circle" size={20} color={COLORS.textMuted} />
                  </TouchableOpacity>
                )}
              </View>

              {newsLoading && newsItems.length === 0 && visibleEvents.length === 0 ? (
                <View style={styles.loadingWrap}>
                  <ActivityIndicator size="small" color={COLORS.primary} />
                </View>
              ) : includeNews && newsError && visibleEvents.length === 0 ? (
                <Text style={styles.errorText}>{newsError}</Text>
              ) : typeFilteredEvents.length === 0 && (!includeNews || visibleNewsItems.length === 0) ? (
                <View style={styles.emptyWrap}>
                  <Text style={styles.emptyText}>No {EVENT_META[selectedEventType].label.toLowerCase()} for this {CALENDAR_VIEW_META[calendarView].emptyLabel}</Text>
                </View>
              ) : filteredFeedItems.length === 0 ? (
                <View style={styles.emptyWrap}>
                  <Text style={styles.emptyText}>
                    {normalizedTickerFilter ? `No matches for “${tickerFilter}”` : 'No saved market or ticker news available right now'}
                  </Text>
                </View>
              ) : (
                <>
                  {displayedFeedItems.map((item, index) => {
                    const isLastRow = index === displayedFeedItems.length - 1;

                    if (item.kind === 'event') {
                      const event = item.event;
                      const meta = EVENT_META[event.type];
                      const fallbackKey = getEventFallbackKey(event.ticker, event.company_name);
                      const canOpenTicker = Boolean(event.ticker);
                      return (
                        <TouchableOpacity
                          key={item.id}
                          style={[styles.eventRow, isLastRow && styles.lastEventRow]}
                          onPress={() => {
                            if (event.ticker) router.push(`/stock/${event.ticker}`);
                          }}
                          disabled={!canOpenTicker}
                          activeOpacity={canOpenTicker ? 0.8 : 1}
                        >
                          <EventLogo
                            logoUrl={resolveEventLogoUrl(event.logo_url, event.ticker)}
                            fallbackKey={fallbackKey}
                          />
                          <View style={styles.eventContent}>
                            <View style={styles.eventTopRow}>
                              <View style={styles.eventTickerBlock}>
                                <Text style={styles.eventTicker}>{event.ticker || 'Market'}</Text>
                                {event.company_name ? (
                                  <Text style={styles.eventCompanyName} numberOfLines={1}>{event.company_name}</Text>
                                ) : null}
                              </View>
                              <View style={[styles.eventPill, { backgroundColor: `${meta.color}15` }]}>
                                <Text style={[styles.eventPillText, { color: meta.color }]}>{meta.singularLabel}</Text>
                              </View>
                            </View>
                            <Text style={styles.eventTitle} numberOfLines={2}>{event.label}</Text>
                            <Text style={styles.eventMeta}>{formatEventSecondary(event)}</Text>
                          </View>
                          {canOpenTicker ? (
                            <Ionicons name="chevron-forward" size={18} color={COLORS.textMuted} />
                          ) : null}
                        </TouchableOpacity>
                      );
                    }

                    const news = item.news;
                    const tone = getSentimentTone(news.sentiment_label);
                    const canOpenItem = Boolean(news.link || news.ticker);
                    return (
                      <TouchableOpacity
                        key={item.id}
                        style={[styles.eventRow, isLastRow && styles.lastEventRow]}
                        onPress={() => {
                          if (!canOpenItem) return;
                          void openNewsItem(news);
                        }}
                        disabled={!canOpenItem}
                        activeOpacity={canOpenItem ? 0.8 : 1}
                      >
                        <EventLogo
                          logoUrl={resolveEventLogoUrl(news.logo_url, news.ticker)}
                          fallbackKey={news.fallback_logo_key}
                        />
                        <View style={styles.eventContent}>
                          <View style={styles.eventTopRow}>
                            <View style={styles.eventTickerBlock}>
                              <Text style={styles.eventTicker}>{news.ticker || 'Market'}</Text>
                              {news.company_name ? (
                                <Text style={styles.eventCompanyName} numberOfLines={1}>{news.company_name}</Text>
                              ) : null}
                            </View>
                            <View style={[styles.eventPill, { backgroundColor: tone.backgroundColor }]}>
                              <Text style={[styles.eventPillText, { color: tone.color }]}>{tone.label}</Text>
                            </View>
                          </View>
                          <Text style={styles.eventTitle} numberOfLines={2}>{news.title}</Text>
                          <Text style={styles.eventMeta}>
                            {[news.source, getMarketNewsDateLabel(news.date)].filter(Boolean).join(' • ')}
                          </Text>
                        </View>
                        {canOpenItem ? (
                          <Ionicons name="chevron-forward" size={18} color={COLORS.textMuted} />
                        ) : null}
                      </TouchableOpacity>
                    );
                  })}
                </>
              )}

              <View style={styles.eventsButtonsRow}>
                {visibleFeedLimit < filteredFeedItems.length && (
                  <TouchableOpacity
                    style={styles.loadMoreButtonFull}
                    onPress={() => setVisibleFeedLimit((prev) => prev + INITIAL_VISIBLE_FEED_ITEMS)}
                  >
                    <Text style={styles.loadMoreText}>Load more events & news</Text>
                  </TouchableOpacity>
                )}
                {visibleFeedLimit > INITIAL_VISIBLE_FEED_ITEMS && (
                  <TouchableOpacity
                    style={styles.seeLessButtonFull}
                    onPress={() => setVisibleFeedLimit(INITIAL_VISIBLE_FEED_ITEMS)}
                  >
                    <Text style={styles.seeLessText}>See less</Text>
                  </TouchableOpacity>
                )}
              </View>
            </>
          )}
        </View>
      </ScrollView>

      <Modal
        visible={calendarPickerVisible}
        transparent
        animationType="slide"
        onRequestClose={() => setCalendarPickerVisible(false)}
      >
        <Pressable style={styles.selectorOverlay} onPress={() => setCalendarPickerVisible(false)}>
          <Pressable style={styles.selectorSheet} onPress={(event) => event.stopPropagation()}>
            <View style={styles.selectorHandle} />
            <Text style={styles.selectorTitle}>Events & News calendar</Text>
            {CALENDAR_VIEW_ORDER.map((viewMode) => {
              const isActive = calendarView === viewMode;
              return (
                <TouchableOpacity
                  key={viewMode}
                  style={styles.selectorOption}
                  onPress={() => {
                    setCalendarView(viewMode);
                    setCalendarPickerVisible(false);
                  }}
                >
                  <Text style={[styles.selectorOptionText, isActive && styles.selectorOptionTextActive]}>
                    {CALENDAR_VIEW_META[viewMode].label}
                  </Text>
                  {isActive ? (
                    <Ionicons name="checkmark" size={18} color={COLORS.primary} />
                  ) : null}
                </TouchableOpacity>
              );
            })}
            <TouchableOpacity
              style={styles.selectorOption}
              onPress={() => {
                setIsCalendarExpanded((prev) => !prev);
                setCalendarPickerVisible(false);
              }}
            >
              <Text style={styles.selectorOptionText}>
                {isCalendarExpanded ? 'Hide Details' : 'Show Details'}
              </Text>
              <Ionicons
                name={isCalendarExpanded ? 'chevron-up' : 'chevron-down'}
                size={18}
                color={COLORS.textLight}
              />
            </TouchableOpacity>
          </Pressable>
        </Pressable>
      </Modal>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.background },
  scroll: { flex: 1 },
  card: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 16,
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  cardHeader: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    marginBottom: 14,
  },
  sectionTitleRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  sectionTitle: { fontSize: 16, fontWeight: '700', color: COLORS.text },
  sectionSubtitle: { fontSize: 12, color: COLORS.textMuted, marginTop: 2 },
  calendarHeaderActionText: {
    fontSize: 13,
    fontWeight: '700',
    color: COLORS.primary,
  },
  viewToggleRow: {
    flexDirection: 'row',
    gap: 8,
    marginBottom: 14,
  },
  viewToggleButton: {
    flex: 1,
    paddingVertical: 10,
    borderRadius: 12,
    backgroundColor: '#F3F4F6',
    alignItems: 'center',
  },
  viewToggleButtonActive: {
    backgroundColor: COLORS.primary,
  },
  viewToggleText: {
    fontSize: 12,
    fontWeight: '700',
    color: COLORS.textLight,
  },
  viewToggleTextActive: {
    color: '#FFFFFF',
  },
  monthHeader: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    marginBottom: 12,
  },
  monthTitle: { fontSize: 16, fontWeight: '700', color: COLORS.primary },
  monthNavButton: {
    width: 32,
    height: 32,
    borderRadius: 16,
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: '#EEF2FF',
  },
  monthNavButtonDisabled: { backgroundColor: '#F3F4F6' },
  weekdayRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    marginBottom: 8,
  },
  activeDaysScroll: {
    flex: 1,
    marginBottom: 12,
  },
  activeDaysScrollContent: {
    gap: 8,
    paddingRight: 4,
  },
  activeDaysScrollerRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  activeDayCard: {
    minWidth: 58,
    paddingVertical: 12,
    paddingHorizontal: 8,
    borderRadius: 14,
    borderWidth: 1,
    borderColor: COLORS.border,
    backgroundColor: '#F8FAFC',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 4,
  },
  activeDayCardSelected: {
    backgroundColor: COLORS.primary,
    borderColor: COLORS.primary,
  },
  activeDayWeekday: {
    fontSize: 11,
    fontWeight: '700',
    color: COLORS.textMuted,
    textTransform: 'uppercase',
  },
  activeDayNumber: {
    fontSize: 20,
    fontWeight: '800',
    color: COLORS.text,
  },
  activeDayTextSelected: {
    color: '#FFFFFF',
  },
  calendarToggleButton: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 6,
    paddingVertical: 10,
    borderRadius: 12,
    backgroundColor: COLORS.primarySoft,
    marginBottom: 12,
  },
  calendarToggleText: {
    fontSize: 13,
    fontWeight: '700',
    color: COLORS.primary,
  },
  weekdayLabel: {
    width: `${100 / 7}%`,
    textAlign: 'center',
    fontSize: 12,
    fontWeight: '600',
    color: COLORS.textMuted,
  },
  periodGrid: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 10,
  },
  periodCell: {
    width: '22%',
    minWidth: 68,
    borderRadius: 14,
    paddingVertical: 12,
    paddingHorizontal: 10,
    backgroundColor: '#F8FAFC',
    borderWidth: 1,
    borderColor: COLORS.border,
    alignItems: 'center',
    gap: 6,
  },
  yearCell: {
    width: '30%',
  },
  periodCellSelected: {
    backgroundColor: COLORS.primary,
    borderColor: COLORS.primary,
  },
  periodCellDisabled: {
    opacity: 0.35,
  },
  periodCellLabel: {
    fontSize: 12,
    fontWeight: '700',
    color: COLORS.text,
  },
  periodCellLabelSelected: {
    color: '#FFFFFF',
  },
  periodCellCount: {
    fontSize: 18,
    fontWeight: '800',
    color: COLORS.primary,
  },
  periodCellCountSelected: {
    color: '#FFFFFF',
  },
  calendarGrid: {
    flexDirection: 'row',
    flexWrap: 'wrap',
  },
  dayCell: {
    width: `${100 / 7}%`,
    aspectRatio: 1,
    alignItems: 'center',
    justifyContent: 'center',
    borderRadius: 12,
    marginBottom: 4,
    gap: 4,
  },
  dayCellOutsideMonth: { opacity: 0.55 },
  dayCellSelected: { backgroundColor: COLORS.primary },
  dayCellDisabled: { opacity: 0.3 },
  dayLabel: { fontSize: 14, color: COLORS.text, fontWeight: '600' },
  dayLabelOutsideMonth: { color: COLORS.textLight },
  dayLabelSelected: { color: '#FFFFFF' },
  dayLabelDisabled: { color: COLORS.textMuted },
  dayDot: {
    minWidth: 18,
    paddingHorizontal: 5,
    borderRadius: 9,
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: '#E5E7EB',
  },
  dayDotSelected: { backgroundColor: '#FFFFFF' },
  dayDotText: { fontSize: 10, color: COLORS.textLight, fontWeight: '700' },
  dayDotTextSelected: { color: COLORS.primary },
  eventsHeader: {
    marginBottom: 10,
  },
  eventsHeaderTop: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    justifyContent: 'space-between',
    gap: 12,
  },
  eventsHeaderActions: {
    alignItems: 'flex-end',
    gap: 8,
  },
  eventsTitleBlock: {
    flex: 1,
  },
  eventsDateRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
    marginTop: 2,
  },
  eventsDateTitle: { fontSize: 17, fontWeight: '800', color: COLORS.text },
  eventsDateSelectText: {
    fontSize: 13,
    fontWeight: '700',
    color: COLORS.primary,
  },
  eventTabsRow: {
    flexDirection: 'row',
    gap: 8,
    paddingBottom: 12,
    marginBottom: 14,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  eventTab: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    gap: 6,
    paddingBottom: 8,
    borderBottomWidth: 2,
    borderBottomColor: 'transparent',
  },
  eventTabLabelRow: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 6,
  },
  eventTabActive: {
    borderBottomColor: '#3B82F6',
  },
  eventTabDisabled: {
    opacity: 0.4,
  },
  eventTabDot: {
    width: 8,
    height: 8,
    borderRadius: 999,
  },
  eventTabText: {
    fontSize: 11,
    fontWeight: '700',
    color: COLORS.textLight,
  },
  eventTabTextActive: {
    color: '#2563EB',
  },
  eventTabTextDisabled: {
    color: COLORS.textMuted,
  },
  eventTabCountText: {
    fontSize: 13,
    fontWeight: '800',
    color: COLORS.text,
  },
  eventTabCountTextActive: {
    color: '#2563EB',
  },
  horizontalNavButton: {
    width: 32,
    height: 32,
    borderRadius: 16,
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: '#EEF2FF',
    marginBottom: 12,
  },
  horizontalNavButtonDisabled: {
    backgroundColor: '#F3F4F6',
  },
  filterSearchWrap: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 10,
    backgroundColor: '#F3F4F6',
    borderRadius: 18,
    paddingHorizontal: 14,
    paddingVertical: 12,
    marginBottom: 16,
  },
  filterSearchInput: {
    flex: 1,
    fontSize: 16,
    color: COLORS.text,
    paddingVertical: 0,
    borderWidth: 0,
    backgroundColor: 'transparent',
  },
  loadingWrap: { paddingVertical: 32, alignItems: 'center' },
  errorText: { fontSize: 13, color: COLORS.danger, paddingVertical: 8 },
  emptyWrap: { paddingVertical: 28, alignItems: 'center', gap: 8 },
  emptyText: { fontSize: 14, color: COLORS.textMuted, textAlign: 'center' },
  eventsButtonsRow: {
    flexDirection: 'row',
    justifyContent: 'center',
    flexWrap: 'wrap',
    gap: 12,
    marginTop: 12,
  },
  eventRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 12,
    paddingVertical: 14,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  lastEventRow: { borderBottomWidth: 0, paddingBottom: 0 },
  eventLogo: {
    width: 40,
    height: 40,
    borderRadius: 10,
    backgroundColor: '#F3F4F6',
  },
  eventLogoFallback: {
    width: 40,
    height: 40,
    borderRadius: 10,
    alignItems: 'center',
    justifyContent: 'center',
  },
  eventLogoFallbackText: {
    fontSize: 16,
    fontWeight: '700',
    color: '#FFFFFF',
  },
  eventContent: { flex: 1, gap: 4 },
  eventTopRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    gap: 8,
  },
  eventTickerBlock: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
    flexShrink: 1,
  },
  eventTicker: { fontSize: 13, fontWeight: '700', color: COLORS.primary, textTransform: 'uppercase' },
  eventCompanyName: { fontSize: 13, fontWeight: '600', color: COLORS.textMuted, flexShrink: 1 },
  eventPill: { paddingHorizontal: 10, paddingVertical: 5, borderRadius: 999 },
  eventPillText: { fontSize: 11, fontWeight: '700' },
  eventTitle: { fontSize: 14, fontWeight: '600', color: COLORS.text, lineHeight: 20 },
  eventMeta: { fontSize: 12, color: COLORS.textLight, lineHeight: 18 },
  loadMoreButtonFull: {
    flex: 1,
    paddingVertical: 12,
    borderRadius: 8,
    backgroundColor: COLORS.primarySoft,
    alignItems: 'center',
  },
  seeLessButtonFull: {
    flex: 1,
    paddingVertical: 12,
    borderRadius: 8,
    backgroundColor: COLORS.background,
    alignItems: 'center',
  },
  loadMoreText: {
    fontSize: 13,
    color: COLORS.primary,
    fontWeight: '700',
  },
  seeLessText: {
    fontSize: 13,
    color: COLORS.textLight,
    fontWeight: '500',
  },
  marketNewsSection: {
    marginTop: 16,
    paddingTop: 16,
    borderTopWidth: 1,
    borderTopColor: COLORS.border,
  },
  marketNewsHeader: {
    marginBottom: 10,
  },
  marketNewsHeaderTop: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    gap: 10,
  },
  marketNewsTitle: {
    fontSize: 15,
    fontWeight: '700',
    color: COLORS.text,
  },
  marketNewsSubtitle: {
    fontSize: 12,
    color: COLORS.textMuted,
    marginTop: 2,
  },
  aggregateSentimentBadge: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 10,
    paddingVertical: 6,
    borderRadius: 999,
    gap: 6,
  },
  aggregateSentimentHeadlineBadge: {
    maxWidth: 220,
  },
  aggregateSentimentDot: {
    width: 8,
    height: 8,
    borderRadius: 999,
  },
  aggregateSentimentText: {
    fontSize: 12,
    fontWeight: '700',
  },
  toggleSwitch: {
    width: 44,
    height: 24,
    borderRadius: 12,
    backgroundColor: COLORS.border,
    padding: 2,
    justifyContent: 'center',
  },
  toggleSwitchOn: {
    backgroundColor: COLORS.primary,
  },
  toggleKnob: {
    width: 20,
    height: 20,
    borderRadius: 10,
    backgroundColor: '#FFFFFF',
  },
  toggleKnobOn: {
    alignSelf: 'flex-end',
  },
  portfolioToggleInline: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
  },
  portfolioToggleLabelInline: {
    fontSize: 12,
    color: COLORS.textLight,
  },
  selectorOverlay: {
    flex: 1,
    backgroundColor: 'rgba(15, 23, 42, 0.35)',
    justifyContent: 'flex-end',
  },
  selectorSheet: {
    backgroundColor: COLORS.card,
    borderTopLeftRadius: 24,
    borderTopRightRadius: 24,
    paddingHorizontal: 20,
    paddingTop: 12,
    paddingBottom: 32,
    gap: 4,
  },
  selectorHandle: {
    alignSelf: 'center',
    width: 44,
    height: 5,
    borderRadius: 999,
    backgroundColor: COLORS.border,
    marginBottom: 12,
  },
  selectorTitle: {
    fontSize: 16,
    fontWeight: '700',
    color: COLORS.text,
    marginBottom: 8,
  },
  selectorOption: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    paddingVertical: 14,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  selectorOptionText: {
    fontSize: 15,
    color: COLORS.text,
    fontWeight: '600',
  },
  selectorOptionTextActive: {
    color: COLORS.primary,
  },
});
