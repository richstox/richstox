import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  ActivityIndicator,
  Image,
  type LayoutChangeEvent,
  Linking,
  Modal,
  type NativeScrollEvent,
  type NativeSyntheticEvent,
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
  format,
  parseISO,
  startOfMonth,
  subDays,
} from 'date-fns';
import { useLayoutSpacing } from '../../constants/layout';
import { API_URL } from '../../utils/config';
import { COLORS as APP_COLORS } from '../_layout';
import { formatAggregateSentimentLabel, getAggregateSentimentTooltipContent } from '../../utils/sentiment';
import AppHeader from '../../components/AppHeader';
import { MetricTooltip } from '../../components/MetricTooltip';
import { useMarketsStore } from '../../stores/marketsStore';

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
type MarketFeedMode = 'events' | 'news';

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
  content?: string | null;
  date?: string | null;
  source?: string | null;
  link?: string | null;
  sentiment_label?: SentimentCategory | null;
  scope?: 'market' | 'ticker';
};

type MarketFeedItem =
  | { kind: 'event'; id: string; date: string; sortTimestamp: number; event: CalendarEvent }
  | { kind: 'news'; id: string; date?: string | null; sortTimestamp: number; news: MarketNewsItem };

type AggregateSentiment = {
  score: number;
  label: SentimentCategory;
  color: string;
  total_articles: number;
  positive_count: number;
  negative_count: number;
  neutral_count: number;
};

const INITIAL_VISIBLE_FEED_ITEMS = 5;
const EVENT_TYPE_ORDER: EventType[] = ['earnings', 'dividend', 'split', 'ipo'];
const CALENDAR_VIEW_ORDER: CalendarViewMode[] = ['daily', 'monthly', 'yearly'];
const MARKET_NEWS_PER_TICKER = 3;
const MARKET_DIGEST_LIMIT = 100;
const EARNINGS_FALLBACK_LABEL = 'Scheduled earnings';
// Markets aggregates ALL distinct Watchlist/Tracklist tickers across users,
// so request a large enough page to avoid truncating the merged corpus client-side.
// 1000 keeps the current full corpus retrievable (3 per ticker + 100 MARKETS)
// while we still fetch this feed in a single request before incremental paging lands.
const MARKET_NEWS_LIMIT = 1000;
const YMD_DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const ACTIVE_DAY_CARD_ESTIMATED_WIDTH = 74;
const ACTIVE_DAY_CARD_GAP = 8;
// Allow a few pixels of tolerance before disabling the day-strip arrows after inertial scrolling.
const ACTIVE_DAY_SCROLL_TOLERANCE = 4;
// Keep a small overlap between pages so users retain context while stepping through dates.
const ACTIVE_DAY_SCROLL_PAGE_MARGIN = 72;
const MARKET_FEED_MODE_OPTIONS: { key: MarketFeedMode; label: string }[] = [
  { key: 'events', label: 'Events' },
  { key: 'news', label: 'News' },
];

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
  return formatDateDMY(parsed.toISOString().slice(0, 10));
};

const getFeedTimestamp = (dateStr?: string | null): number => {
  if (!dateStr) return 0;
  const parsed = Date.parse(YMD_DATE_PATTERN.test(dateStr) ? `${dateStr}T00:00:00Z` : dateStr);
  return Number.isFinite(parsed) ? parsed : 0;
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

const EventLogo = ({
  logoUrl,
  fallbackKey,
  useRichstoxIcon = false,
}: {
  logoUrl?: string;
  fallbackKey: string;
  useRichstoxIcon?: boolean;
}) => {
  const [imageError, setImageError] = useState(false);

  if (!logoUrl || imageError) {
    if (useRichstoxIcon) {
      return (
        <View style={[styles.eventLogoFallback, styles.marketLogoFallback]}>
          <Image
            source={require('../../assets/images/richstox_icon_only.png')}
            style={styles.marketLogoIcon}
          />
        </View>
      );
    }
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
  const initialMarketsStateRef = useRef(useMarketsStore.getState());
  const setMarketsState = useMarketsStore((state) => state.setState);

  const todayPragueStr = getPragueDateString();
  const todayPrague = parseYmd(todayPragueStr);
  const todayMonthKey = todayPragueStr.slice(0, 7);
  const rangeStart = subDays(todayPrague, 1);
  const rangeEnd = addDays(todayPrague, 90);
  const rangeStartStr = format(rangeStart, 'yyyy-MM-dd');
  const rangeEndStr = format(rangeEnd, 'yyyy-MM-dd');
  const currentYear = Number(todayPragueStr.slice(0, 4));
  const storedSelectedDateKey = initialMarketsStateRef.current.selectedDateKey;
  const storedDisplayMonthKey = initialMarketsStateRef.current.displayMonthKey;
  const storedCalendarView = initialMarketsStateRef.current.calendarView;
  const storedSelectedYear = initialMarketsStateRef.current.selectedYear;
  const storedSelectedEventType = initialMarketsStateRef.current.selectedEventType;
  const storedVisibleFeedLimit = initialMarketsStateRef.current.visibleFeedLimit;
  const storedMarketFeedModes = initialMarketsStateRef.current.marketFeedModes;
  const initialSelectedDate = storedSelectedDateKey && isValidYmd(storedSelectedDateKey)
    ? parseYmd(storedSelectedDateKey)
    : todayPrague;
  const initialDisplayMonth = storedDisplayMonthKey && isValidYmd(`${storedDisplayMonthKey}-01`)
    ? startOfMonth(parseYmd(`${storedDisplayMonthKey}-01`))
    : startOfMonth(todayPrague);
  const initialCalendarView = CALENDAR_VIEW_ORDER.includes(storedCalendarView as CalendarViewMode)
    ? storedCalendarView as CalendarViewMode
    : 'daily';
  const initialSelectedYear = Number.isInteger(storedSelectedYear) && storedSelectedYear >= 2000 && storedSelectedYear <= currentYear + 10
    ? storedSelectedYear
    : currentYear;
  const initialSelectedEventType = EVENT_TYPE_ORDER.includes(storedSelectedEventType as EventType)
    ? storedSelectedEventType as EventType
    : 'earnings';
  const initialVisibleFeedLimit = storedVisibleFeedLimit && storedVisibleFeedLimit > 0
    ? storedVisibleFeedLimit
    : INITIAL_VISIBLE_FEED_ITEMS;
  const initialMarketFeedModes = storedMarketFeedModes.filter((mode): mode is MarketFeedMode =>
    MARKET_FEED_MODE_OPTIONS.some((option) => option.key === mode),
  );

  const [events, setEvents] = useState<CalendarEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [calendarView, setCalendarView] = useState<CalendarViewMode>(initialCalendarView);
  const [selectedDate, setSelectedDate] = useState<Date>(initialSelectedDate);
  const [displayMonth, setDisplayMonth] = useState<Date>(initialDisplayMonth);
  const [selectedYear, setSelectedYear] = useState<number>(initialSelectedYear);
  const [selectedEventType, setSelectedEventType] = useState<EventType>(initialSelectedEventType);
  const [tickerFilter, setTickerFilter] = useState(initialMarketsStateRef.current.tickerFilter);
  const [visibleFeedLimit, setVisibleFeedLimit] = useState(initialVisibleFeedLimit);
  const [calendarPickerVisible, setCalendarPickerVisible] = useState(false);
  const [marketFeedModes, setMarketFeedModes] = useState<MarketFeedMode[]>(
    initialMarketFeedModes.length > 0 ? initialMarketFeedModes : ['events', 'news'],
  );
  const [newsItems, setNewsItems] = useState<MarketNewsItem[]>([]);
  const [aggregateSentiment, setAggregateSentiment] = useState<AggregateSentiment | null>(null);
  const [newsLoading, setNewsLoading] = useState(false);
  const [newsError, setNewsError] = useState<string | null>(null);
  const [selectedArticle, setSelectedArticle] = useState<MarketNewsItem | null>(null);
  const [aggregateSentimentTooltipVisible, setAggregateSentimentTooltipVisible] = useState(false);
  const marketsScrollRef = useRef<ScrollView | null>(null);
  const didHydratePeriodResetRef = useRef(false);
  const didHydrateFeedResetRef = useRef(false);
  const didRestoreScrollRef = useRef(initialMarketsStateRef.current.scrollY <= 0);
  const activeDaysScrollRef = useRef<ScrollView | null>(null);
  const [activeDaysViewportWidth, setActiveDaysViewportWidth] = useState(0);
  const [activeDaysContentWidth, setActiveDaysContentWidth] = useState(0);
  const [activeDaysScrollX, setActiveDaysScrollX] = useState(0);

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

  useEffect(() => {
    setMarketsState({
      calendarView,
      selectedDateKey,
      displayMonthKey: selectedMonthKey,
      selectedYear,
      selectedEventType,
      tickerFilter,
      visibleFeedLimit,
      marketFeedModes,
    });
  }, [
    calendarView,
    marketFeedModes,
    selectedDateKey,
    selectedEventType,
    selectedMonthKey,
    selectedYear,
    setMarketsState,
    tickerFilter,
    visibleFeedLimit,
  ]);

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
      .map((monthKey) => startOfMonth(parseYmd(`${monthKey}-01`)));
  }, [activeMonthKeys, selectedYearKey]);

  const yearCards = useMemo(() => {
    return activeYearKeys.map((yearKey) => Number(yearKey));
  }, [activeYearKeys]);

  const maxActiveDaysScrollX = Math.max(0, activeDaysContentWidth - activeDaysViewportWidth);
  const canScrollActiveDaysPrev = activeDaysScrollX > ACTIVE_DAY_SCROLL_TOLERANCE;
  const canScrollActiveDaysNext = activeDaysScrollX < maxActiveDaysScrollX - ACTIVE_DAY_SCROLL_TOLERANCE;

  const handleActiveDaysLayout = useCallback((event: LayoutChangeEvent) => {
    setActiveDaysViewportWidth(event.nativeEvent.layout.width);
  }, []);

  const handleActiveDaysScroll = useCallback((event: NativeSyntheticEvent<NativeScrollEvent>) => {
    const { contentOffset, contentSize, layoutMeasurement } = event.nativeEvent;
    setActiveDaysScrollX(Math.max(0, contentOffset.x));
    setActiveDaysViewportWidth(layoutMeasurement.width);
    setActiveDaysContentWidth(contentSize.width);
  }, []);

  const scrollActiveDaysBy = useCallback((direction: -1 | 1) => {
    const step = activeDaysViewportWidth > 0
      ? Math.max(activeDaysViewportWidth - ACTIVE_DAY_SCROLL_PAGE_MARGIN, ACTIVE_DAY_CARD_ESTIMATED_WIDTH + ACTIVE_DAY_CARD_GAP)
      : ACTIVE_DAY_CARD_ESTIMATED_WIDTH * 3;
    const nextX = Math.max(0, Math.min(maxActiveDaysScrollX, activeDaysScrollX + (direction * step)));
    activeDaysScrollRef.current?.scrollTo({ x: nextX, animated: true });
  }, [activeDaysScrollX, activeDaysViewportWidth, maxActiveDaysScrollX]);

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
    if (!didHydratePeriodResetRef.current) {
      didHydratePeriodResetRef.current = true;
      return;
    }
    setTickerFilter('');
    setVisibleFeedLimit(INITIAL_VISIBLE_FEED_ITEMS);
  }, [calendarView, selectedDateKey, selectedMonthKey, selectedYearKey]);

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
    if (calendarView !== 'daily' || activeDayKeysForDisplayMonth.length === 0 || activeDaysViewportWidth <= 0) return;
    const selectedDayIndex = Math.max(activeDayKeysForDisplayMonth.indexOf(selectedDateKey), 0);
    const centeredOffset = Math.max(
      0,
      (selectedDayIndex * (ACTIVE_DAY_CARD_ESTIMATED_WIDTH + ACTIVE_DAY_CARD_GAP))
        - Math.max(0, (activeDaysViewportWidth - ACTIVE_DAY_CARD_ESTIMATED_WIDTH) / 2),
    );
    const nextMaxActiveDaysScrollX = Math.max(0, activeDaysContentWidth - activeDaysViewportWidth);
    const nextX = centeredOffset > nextMaxActiveDaysScrollX ? nextMaxActiveDaysScrollX : centeredOffset;
    // Keep the selected date visible immediately when the picker opens or the month changes.
    activeDaysScrollRef.current?.scrollTo({ x: nextX, animated: false });
    setActiveDaysScrollX(nextX);
  }, [
    activeDayKeysForDisplayMonth,
    activeDaysContentWidth,
    activeDaysViewportWidth,
    calendarPickerVisible,
    calendarView,
    selectedDateKey,
  ]);

  useEffect(() => {
    const nextType = EVENT_TYPE_ORDER.find((type) => selectedEventCounts[type] > 0) ?? 'earnings';
    if (selectedEventCounts[selectedEventType] === 0 && nextType !== selectedEventType) {
      setSelectedEventType(nextType);
      setVisibleFeedLimit(INITIAL_VISIBLE_FEED_ITEMS);
      setTickerFilter('');
    }
  }, [selectedEventCounts, selectedEventType]);

  const typeFilteredEvents = useMemo(
    () => periodEvents.filter((event) => event.type === selectedEventType),
    [periodEvents, selectedEventType],
  );
  const marketShowsEvents = marketFeedModes.includes('events');
  const marketShowsNews = marketFeedModes.includes('news');

  const toggleMarketFeedMode = useCallback((mode: MarketFeedMode) => {
    setMarketFeedModes((prev) => {
      if (prev.includes(mode)) {
        // Keep at least one content mode active so the feed never collapses into an empty toggle state.
        return prev.length === 1 ? prev : prev.filter((item) => item !== mode);
      }
      return [...prev, mode];
    });
  }, []);

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

  const visibleEventToggleCount = useMemo(() => {
    if (!normalizedTickerFilter) return periodEvents.length;
    return periodEvents.filter((event) => {
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
    }).length;
  }, [normalizedTickerFilter, periodEvents]);

  const visibleNewsItems = useMemo(() => {
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
  }, [newsItems, normalizedTickerFilter]);
  const visibleNewsToggleCount = visibleNewsItems.length;

  useEffect(() => {
    if (!didHydrateFeedResetRef.current) {
      didHydrateFeedResetRef.current = true;
      return;
    }
    setVisibleFeedLimit(INITIAL_VISIBLE_FEED_ITEMS);
  }, [marketFeedModes, normalizedTickerFilter]);

  const filteredFeedItems = useMemo<MarketFeedItem[]>(() => {
    const eventItems = marketShowsEvents
      ? visibleEvents.map((event, index) => ({
          kind: 'event' as const,
          id: `${event.type}-${event.ticker || 'na'}-${event.date}-${index}`,
          date: event.date,
          sortTimestamp: getFeedTimestamp(event.date),
          event,
        }))
      : [];
    const mergedItems: MarketFeedItem[] = [
      ...eventItems,
      ...(marketShowsNews
        ? visibleNewsItems.map((news) => ({
            kind: 'news' as const,
            id: news.id,
            date: news.date,
            sortTimestamp: getFeedTimestamp(news.date),
            news,
          }))
        : []),
    ];

    return mergedItems.sort((left, right) => right.sortTimestamp - left.sortTimestamp);
  }, [marketShowsEvents, marketShowsNews, visibleEvents, visibleNewsItems]);

  const displayedFeedItems = useMemo(
    () => filteredFeedItems.slice(0, visibleFeedLimit),
    [filteredFeedItems, visibleFeedLimit],
  );

  useEffect(() => {
    let cancelled = false;

    const fetchVisibleTickerNews = async () => {
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
              content: item?.content ?? null,
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
        setAggregateSentiment(payload?.aggregate_sentiment ?? null);
      } catch (err) {
        if (cancelled) return;
        console.error('Error fetching Markets news:', err);
        setNewsItems([]);
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
  }, []);

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

  const getEventMarketTimingLabel = (event: CalendarEvent): string | null => {
    if (event.type !== 'earnings') return null;
    const hasStructuredTiming = typeof event.metadata?.before_after_market === 'string';
    const rawTiming = hasStructuredTiming ? event.metadata?.before_after_market : event.description;
    if (!rawTiming) return null;
    const trimmedTiming = rawTiming.trim();
    const normalized = trimmedTiming.toLowerCase().replace(/[\s_-]+/g, '');
    if (normalized.startsWith('before')) return 'Before Market';
    if (normalized.startsWith('after')) return 'After Market';
    if (!hasStructuredTiming && /\bmarket\b/i.test(trimmedTiming) && trimmedTiming !== EARNINGS_FALLBACK_LABEL) {
      return trimmedTiming;
    }
    return null;
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
      return details.join(' • ') || EARNINGS_FALLBACK_LABEL;
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

  const formatEventHeaderMeta = (event: CalendarEvent): string => {
    return [getMarketNewsDateLabel(event.date), getEventMarketTimingLabel(event)]
      .filter(Boolean)
      .join(' • ');
  };

  const openNewsItem = (item: MarketNewsItem) => {
    setSelectedArticle(item);
  };

  const closeArticle = () => {
    setSelectedArticle(null);
  };

  const openExternalLink = async (url: string) => {
    try {
      await Linking.openURL(url);
    } catch (err) {
      console.error('Error opening Markets news article externally:', err);
    }
  };

  const restoreMarketsScroll = useCallback(() => {
    if (didRestoreScrollRef.current) return;
    const nextY = initialMarketsStateRef.current.scrollY;
    if (nextY <= 0) {
      didRestoreScrollRef.current = true;
      return;
    }
    marketsScrollRef.current?.scrollTo({ y: nextY, animated: false });
    didRestoreScrollRef.current = true;
  }, []);

  useEffect(() => {
    if (loading || newsLoading) return;
    const frame = requestAnimationFrame(() => {
      restoreMarketsScroll();
    });
    return () => cancelAnimationFrame(frame);
  }, [displayedFeedItems.length, loading, newsLoading, restoreMarketsScroll]);

  return (
    <SafeAreaView style={styles.container} edges={['top']}>
      <AppHeader title="Markets" />

      <ScrollView
        ref={marketsScrollRef}
        style={styles.scroll}
        contentContainerStyle={{ padding: sp.pageGutter, gap: 12 }}
        onScroll={(event) => {
          setMarketsState({ scrollY: event.nativeEvent.contentOffset.y });
        }}
        onContentSizeChange={() => {
          restoreMarketsScroll();
        }}
        scrollEventThrottle={16}
      >
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
                  <TouchableOpacity
                    style={styles.eventsDateSelectControl}
                    onPress={() => setCalendarPickerVisible(true)}
                    accessibilityRole="button"
                  >
                    <Text style={styles.eventsDateSelectText}>Select</Text>
                    <Ionicons name="chevron-down" size={12} color={APP_COLORS.primary} />
                  </TouchableOpacity>
                </View>
              </View>
              <View style={styles.eventsHeaderActions}>
                {aggregateSentiment && (
                  <TouchableOpacity
                    style={[
                      styles.aggregateSentimentBadge,
                      styles.aggregateSentimentHeadlineBadge,
                      { backgroundColor: `${aggregateSentiment.color}20` },
                    ]}
                    onPress={() => setAggregateSentimentTooltipVisible(true)}
                    accessibilityRole="button"
                    accessibilityLabel="Show aggregate sentiment help"
                  >
                    <View style={[styles.aggregateSentimentDot, { backgroundColor: aggregateSentiment.color }]} />
                    <Text style={[styles.aggregateSentimentText, { color: aggregateSentiment.color }]}>
                      {formatAggregateSentimentLabel(aggregateSentiment.label, aggregateSentiment.score)}
                    </Text>
                  </TouchableOpacity>
                )}
                <View style={styles.feedModeRow}>
                  <View style={styles.feedModeGroup}>
                    {MARKET_FEED_MODE_OPTIONS.map((option) => {
                      const isActive = marketFeedModes.includes(option.key);
                      const isLocked = isActive && marketFeedModes.length === 1;
                      return (
                        <TouchableOpacity
                          key={option.key}
                          style={[styles.feedModeChip, isActive && styles.feedModeChipActive, isLocked && styles.feedModeChipLocked]}
                          onPress={() => toggleMarketFeedMode(option.key)}
                          disabled={isLocked}
                          accessibilityRole="button"
                          accessibilityLabel={isLocked
                            ? `Cannot disable last active ${option.label.toLowerCase()} filter on markets`
                            : `Show ${option.label.toLowerCase()} on markets`}
                        >
                          <Text style={[styles.feedModeChipText, isActive && styles.feedModeChipTextActive]}>
                            {option.label} ({option.key === 'events' ? visibleEventToggleCount : visibleNewsToggleCount})
                          </Text>
                        </TouchableOpacity>
                      );
                    })}
                  </View>
                </View>
              </View>
            </View>
          </View>

          {loading ? (
            <View style={styles.loadingWrap}>
              <ActivityIndicator size="small" color={COLORS.primary} />
            </View>
          ) : error ? (
            <Text style={styles.errorText}>{error}</Text>
          ) : periodEvents.length === 0 && (!newsLoading && !newsError && newsItems.length === 0) ? (
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
              ) : marketShowsNews && newsError && visibleEvents.length === 0 ? (
                <Text style={styles.errorText}>{newsError}</Text>
              ) : !marketShowsNews && typeFilteredEvents.length === 0 ? (
                <View style={styles.emptyWrap}>
                  <Text style={styles.emptyText}>
                    {normalizedTickerFilter ? `No matches for “${tickerFilter}”` : `No ${EVENT_META[selectedEventType].label.toLowerCase()} for this ${CALENDAR_VIEW_META[calendarView].emptyLabel}`}
                  </Text>
                </View>
              ) : !marketShowsEvents && visibleNewsItems.length === 0 ? (
                <View style={styles.emptyWrap}>
                  <Text style={styles.emptyText}>
                    {normalizedTickerFilter ? `No matches for “${tickerFilter}”` : 'No saved market or ticker news available right now'}
                  </Text>
                </View>
              ) : filteredFeedItems.length === 0 ? (
                <View style={styles.emptyWrap}>
                  <Text style={styles.emptyText}>
                    {normalizedTickerFilter
                      ? `No matches for “${tickerFilter}”`
                      : marketShowsEvents && marketShowsNews
                        ? `No ${EVENT_META[selectedEventType].label.toLowerCase()} or news for this ${CALENDAR_VIEW_META[calendarView].emptyLabel}`
                        : !marketShowsNews
                          ? `No ${EVENT_META[selectedEventType].label.toLowerCase()} for this ${CALENDAR_VIEW_META[calendarView].emptyLabel}`
                          : 'No saved market or ticker news available right now'}
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
                              <View style={styles.eventMetaColumn}>
                                <View style={[styles.eventPill, { backgroundColor: `${meta.color}15` }]}>
                                  <Text style={[styles.eventPillText, { color: meta.color }]}>{meta.singularLabel}</Text>
                                </View>
                                <Text style={styles.eventHeaderMeta}>{formatEventHeaderMeta(event)}</Text>
                              </View>
                            </View>
                            <Text style={styles.eventPrimaryText} numberOfLines={2}>{formatEventSecondary(event)}</Text>
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
                      <View key={item.id} style={[styles.eventRow, isLastRow && styles.lastEventRow]}>
                        <TouchableOpacity
                          onPress={() => {
                            if (news.ticker) router.push(`/stock/${news.ticker}`);
                          }}
                          disabled={!news.ticker}
                          activeOpacity={news.ticker ? 0.8 : 1}
                        >
                          <EventLogo
                            logoUrl={resolveEventLogoUrl(news.logo_url, news.ticker)}
                            fallbackKey={news.fallback_logo_key}
                            useRichstoxIcon={news.scope === 'market'}
                          />
                        </TouchableOpacity>
                        <TouchableOpacity
                          style={styles.eventContent}
                          onPress={() => {
                            if (!canOpenItem) return;
                            openNewsItem(news);
                          }}
                          disabled={!canOpenItem}
                          activeOpacity={canOpenItem ? 0.8 : 1}
                        >
                          <View style={styles.marketNewsTickerRow}>
                            <Text style={styles.eventTicker}>{news.ticker || 'Market'}</Text>
                            <View style={[styles.eventPill, { backgroundColor: tone.backgroundColor }]}>
                              <Text style={[styles.eventPillText, { color: tone.color }]}>{tone.label}</Text>
                            </View>
                          </View>
                          {news.company_name ? (
                            <Text style={styles.marketNewsCompanyName} numberOfLines={1}>{news.company_name}</Text>
                          ) : null}
                          <Text style={styles.eventTitle} numberOfLines={2}>{news.title}</Text>
                          <Text style={styles.marketNewsFooterMeta}>
                            {[getMarketNewsDateLabel(news.date), news.source].filter(Boolean).join(' • ')}
                          </Text>
                        </TouchableOpacity>
                        {canOpenItem ? (
                          <Ionicons name="chevron-forward" size={18} color={COLORS.textMuted} />
                        ) : null}
                      </View>
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
                    <Text style={styles.loadMoreText}>Load more</Text>
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
        visible={!!selectedArticle}
        animationType="slide"
        presentationStyle="pageSheet"
        onRequestClose={closeArticle}
      >
        <SafeAreaView style={styles.modalContainer}>
          <View style={styles.modalHeader}>
            <TouchableOpacity onPress={closeArticle} style={styles.modalCloseButton}>
              <Ionicons name="close" size={28} color={COLORS.text} />
            </TouchableOpacity>
            <Text style={styles.modalHeaderTitle}>Article</Text>
            <TouchableOpacity
              onPress={() => selectedArticle?.link && openExternalLink(selectedArticle.link)}
              style={styles.modalExternalButton}
              disabled={!selectedArticle?.link}
            >
              <Ionicons name="open-outline" size={22} color={selectedArticle?.link ? COLORS.primary : COLORS.textMuted} />
            </TouchableOpacity>
          </View>

          <ScrollView style={styles.modalScroll} contentContainerStyle={styles.modalScrollContent}>
            {selectedArticle && (
              <>
                <View style={styles.articleHeader}>
                  <TouchableOpacity
                    style={styles.articleTickerRow}
                    onPress={() => {
                      closeArticle();
                      if (selectedArticle.ticker) {
                        router.push(`/stock/${selectedArticle.ticker}`);
                      }
                    }}
                    disabled={!selectedArticle.ticker}
                  >
                    <EventLogo
                      logoUrl={resolveEventLogoUrl(selectedArticle.logo_url, selectedArticle.ticker)}
                      fallbackKey={selectedArticle.fallback_logo_key}
                      useRichstoxIcon={selectedArticle.scope === 'market'}
                    />
                    <View>
                      <Text style={styles.articleTicker}>{selectedArticle.ticker || 'Market News'}</Text>
                      <Text style={styles.articleCompany}>{selectedArticle.company_name || selectedArticle.source || 'Market News'}</Text>
                    </View>
                  </TouchableOpacity>
                  <Text style={styles.articleMeta}>
                    {[getMarketNewsDateLabel(selectedArticle.date), selectedArticle.source].filter(Boolean).join(' • ')}
                  </Text>
                </View>

                <Text style={styles.articleTitle}>{selectedArticle.title}</Text>

                {(() => {
                  const selectedArticleTone = getSentimentTone(selectedArticle.sentiment_label);
                  return (
                    <View style={styles.sentimentRow}>
                      <View style={[styles.eventPill, { backgroundColor: selectedArticleTone.backgroundColor }]}>
                        <Text style={[styles.eventPillText, { color: selectedArticleTone.color }]}>
                          {selectedArticleTone.label} sentiment
                        </Text>
                      </View>
                    </View>
                  );
                })()}

                <Text style={styles.articleContent}>
                  {selectedArticle.content?.trim() || 'Open the original article to read the full story'}
                </Text>

                {selectedArticle.link && (
                  <TouchableOpacity
                    style={styles.readOriginalButton}
                    onPress={() => openExternalLink(selectedArticle.link)}
                  >
                    <Text style={styles.readOriginalText}>Read original article</Text>
                    <Ionicons name="open-outline" size={16} color={COLORS.primary} />
                  </TouchableOpacity>
                )}
              </>
            )}
          </ScrollView>
        </SafeAreaView>
      </Modal>

      <MetricTooltip
        visible={aggregateSentimentTooltipVisible}
        onClose={() => setAggregateSentimentTooltipVisible(false)}
        content={getAggregateSentimentTooltipContent(aggregateSentiment)}
      />

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
            <ScrollView showsVerticalScrollIndicator={false} contentContainerStyle={styles.selectorScrollContent}>
              {CALENDAR_VIEW_ORDER.map((viewMode) => {
                const isActive = calendarView === viewMode;
                return (
                  <TouchableOpacity
                    key={viewMode}
                    style={styles.selectorOption}
                    onPress={() => setCalendarView(viewMode)}
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

              <View style={styles.selectorDetailSection}>
                {calendarView === 'daily' ? (
                  <>
                    <View style={styles.selectorMonthHeader}>
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
                      <Text style={styles.selectorMonthTitle}>{format(displayMonth, 'MMMM yyyy')}</Text>
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

                    {activeDayKeysForDisplayMonth.length === 0 ? (
                      <Text style={styles.selectorEmptyText}>No events in this month.</Text>
                    ) : (
                      <View style={styles.activeDaysCarouselRow}>
                        <TouchableOpacity
                          style={[styles.monthNavButton, styles.activeDaysNavButton, !canScrollActiveDaysPrev && styles.monthNavButtonDisabled]}
                          onPress={() => scrollActiveDaysBy(-1)}
                          disabled={!canScrollActiveDaysPrev}
                          accessibilityLabel="Scroll days left"
                        >
                          <Ionicons name="chevron-back" size={16} color={canScrollActiveDaysPrev ? COLORS.primary : COLORS.textMuted} />
                        </TouchableOpacity>
                        <ScrollView
                          ref={activeDaysScrollRef}
                          horizontal
                          showsHorizontalScrollIndicator={false}
                          contentContainerStyle={styles.activeDaysScrollContent}
                          style={styles.activeDaysScroll}
                          onLayout={handleActiveDaysLayout}
                          onContentSizeChange={(contentWidth) => setActiveDaysContentWidth(contentWidth)}
                          onScroll={handleActiveDaysScroll}
                          scrollEventThrottle={16}
                        >
                          {activeDayKeysForDisplayMonth.map((dayKey) => {
                            const day = parseYmd(dayKey);
                            const isSelected = dayKey === selectedDateKey;
                            return (
                              <TouchableOpacity
                                key={dayKey}
                                style={[styles.activeDayCard, isSelected && styles.activeDayCardSelected]}
                                onPress={() => {
                                  setSelectedDate(day);
                                  setCalendarPickerVisible(false);
                                }}
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
                        <TouchableOpacity
                          style={[styles.monthNavButton, styles.activeDaysNavButton, !canScrollActiveDaysNext && styles.monthNavButtonDisabled]}
                          onPress={() => scrollActiveDaysBy(1)}
                          disabled={!canScrollActiveDaysNext}
                          accessibilityLabel="Scroll days right"
                        >
                          <Ionicons name="chevron-forward" size={16} color={canScrollActiveDaysNext ? COLORS.primary : COLORS.textMuted} />
                        </TouchableOpacity>
                      </View>
                    )}
                  </>
                ) : calendarView === 'monthly' ? (
                  <>
                    <View style={styles.selectorMonthHeader}>
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
                      <Text style={styles.selectorMonthTitle}>{selectedYear}</Text>
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
                              setCalendarPickerVisible(false);
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
                            setCalendarPickerVisible(false);
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
            </ScrollView>
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
  sectionTitleRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  sectionTitle: { fontSize: 16, fontWeight: '700', color: COLORS.text },
  monthNavButton: {
    width: 32,
    height: 32,
    borderRadius: 16,
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: '#EEF2FF',
  },
  monthNavButtonDisabled: { backgroundColor: '#F3F4F6' },
  activeDaysCarouselRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 10,
  },
  activeDaysNavButton: {
    flexShrink: 0,
  },
  activeDaysScroll: {
    flex: 1,
  },
  activeDaysScrollContent: {
    gap: 8,
    paddingRight: 4,
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
  eventsHeader: {
    marginBottom: 10,
  },
  eventsHeaderTop: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    gap: 12,
  },
  eventsHeaderActions: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'flex-end',
    flexShrink: 1,
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
  eventsDateSelectControl: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
  },
  eventsDateTitle: { fontSize: 17, fontWeight: '800', color: COLORS.text },
  eventsDateSelectText: {
    fontSize: 12,
    fontWeight: '600',
    color: APP_COLORS.primary,
  },
  aggregateSentimentHelperText: {
    marginTop: 8,
    fontSize: 11,
    lineHeight: 16,
    color: COLORS.textMuted,
  },
  modalContainer: { flex: 1, backgroundColor: COLORS.background },
  modalHeader: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    paddingHorizontal: 20,
    paddingVertical: 16,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
    backgroundColor: COLORS.card,
  },
  modalCloseButton: { padding: 4 },
  modalHeaderTitle: {
    fontSize: 18,
    fontWeight: '700',
    color: COLORS.text,
  },
  modalExternalButton: { padding: 4 },
  modalScroll: { flex: 1 },
  modalScrollContent: { padding: 20, paddingBottom: 40 },
  articleHeader: { marginBottom: 20 },
  articleTickerRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 12,
    marginBottom: 12,
  },
  articleTicker: {
    fontSize: 18,
    fontWeight: '700',
    color: COLORS.primary,
    textTransform: 'uppercase',
  },
  articleCompany: {
    fontSize: 15,
    color: COLORS.textMuted,
    marginTop: 2,
  },
  articleMeta: {
    fontSize: 14,
    color: COLORS.textLight,
  },
  articleTitle: {
    fontSize: 28,
    fontWeight: '700',
    color: COLORS.text,
    lineHeight: 36,
    marginBottom: 16,
  },
  sentimentRow: { marginBottom: 20 },
  readOriginalButton: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 8,
    paddingVertical: 14,
    borderRadius: 12,
    backgroundColor: COLORS.primarySoft,
  },
  readOriginalText: {
    fontSize: 15,
    fontWeight: '600',
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
  marketLogoFallback: {
    backgroundColor: '#FFFFFF',
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  marketLogoIcon: {
    width: 28,
    height: 28,
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
    alignItems: 'flex-start',
    gap: 8,
  },
  eventTickerBlock: {
    gap: 2,
    flexShrink: 1,
  },
  eventTicker: { fontSize: 13, fontWeight: '700', color: COLORS.primary, textTransform: 'uppercase' },
  eventCompanyName: { fontSize: 13, fontWeight: '600', color: COLORS.textMuted, flexShrink: 1 },
  marketNewsTickerRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  marketNewsCompanyName: {
    fontSize: 13,
    fontWeight: '600',
    color: COLORS.textMuted,
    lineHeight: 18,
    marginBottom: 4,
  },
  marketNewsFooterMeta: {
    fontSize: 11,
    color: COLORS.textMuted,
    lineHeight: 16,
    marginTop: 8,
  },
  eventMetaColumn: {
    alignItems: 'flex-end',
    gap: 6,
    maxWidth: '45%',
  },
  eventPill: { paddingHorizontal: 10, paddingVertical: 5, borderRadius: 999 },
  eventPillText: { fontSize: 11, fontWeight: '700' },
  eventHeaderMeta: { fontSize: 11, color: COLORS.textMuted, lineHeight: 16, textAlign: 'right' },
  eventPrimaryText: { fontSize: 14, fontWeight: '600', color: COLORS.text, lineHeight: 20 },
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
  feedModeRow: {
    flexDirection: 'row',
    alignItems: 'center',
    flexShrink: 1,
    justifyContent: 'flex-end',
    minWidth: 0,
  },
  feedModeGroup: {
    flexDirection: 'row',
    flexShrink: 1,
    justifyContent: 'flex-end',
    gap: 6,
  },
  feedModeChip: {
    paddingHorizontal: 9,
    paddingVertical: 6,
    borderRadius: 999,
    backgroundColor: COLORS.background,
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  feedModeChipActive: {
    backgroundColor: APP_COLORS.primary,
    borderColor: APP_COLORS.primary,
  },
  feedModeChipLocked: {
    opacity: 0.75,
  },
  feedModeChipText: {
    fontSize: 10,
    fontWeight: '700',
    color: COLORS.textLight,
  },
  feedModeChipTextActive: {
    color: '#FFFFFF',
  },
  articleContent: {
    fontSize: 15,
    lineHeight: 24,
    color: COLORS.text,
    marginBottom: 20,
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
    paddingBottom: 20,
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
  selectorScrollContent: {
    paddingBottom: 12,
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
  selectorDetailSection: {
    paddingTop: 16,
    gap: 12,
  },
  selectorMonthHeader: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    gap: 12,
  },
  selectorMonthTitle: {
    flex: 1,
    textAlign: 'center',
    fontSize: 16,
    fontWeight: '700',
    color: COLORS.primary,
  },
  selectorEmptyText: {
    fontSize: 13,
    color: COLORS.textMuted,
    textAlign: 'center',
    paddingVertical: 12,
  },
});
