import React, { useEffect, useMemo, useState } from 'react';
import {
  ActivityIndicator,
  Image,
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
  addMonths,
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
import { FONTS } from '../_layout';
import { useLayoutSpacing } from '../../constants/layout';
import { API_URL } from '../../utils/config';

const COLORS = {
  primary: '#1E3A5F',
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

const WEEKDAY_LABELS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
const TICKER_FILTER_THRESHOLD = 6;
const EVENT_TYPE_ORDER: EventType[] = ['earnings', 'dividend', 'split', 'ipo'];

const EVENT_META: Record<EventType, { label: string; singularLabel: string; color: string; icon: keyof typeof Ionicons.glyphMap }> = {
  earnings: { label: 'Earnings', singularLabel: 'Earnings', color: '#3B82F6', icon: 'bar-chart-outline' },
  dividend: { label: 'Dividends', singularLabel: 'Dividend', color: '#10B981', icon: 'cash-outline' },
  split: { label: 'Splits', singularLabel: 'Split', color: '#F59E0B', icon: 'git-compare-outline' },
  ipo: { label: 'IPOs', singularLabel: 'IPO', color: '#A855F7', icon: 'rocket-outline' },
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

const formatEventAmount = (amount: number, currency?: string | null): string => {
  const prefix = currency && currency !== 'USD' ? `${currency} ` : '$';
  return `${prefix}${amount.toFixed(2)}`;
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

const resolveEventLogoUrl = (rawUrl?: string | null): string | undefined => {
  if (!rawUrl) return undefined;
  return rawUrl.startsWith('http') ? rawUrl : `${API_URL}${rawUrl}`;
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

  const todayPrague = parseYmd(getPragueDateString());
  const rangeStart = subDays(todayPrague, 1);
  const rangeEnd = addDays(todayPrague, 90);
  const rangeStartStr = format(rangeStart, 'yyyy-MM-dd');
  const rangeEndStr = format(rangeEnd, 'yyyy-MM-dd');

  const [events, setEvents] = useState<CalendarEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedDate, setSelectedDate] = useState<Date>(todayPrague);
  const [displayMonth, setDisplayMonth] = useState<Date>(startOfMonth(todayPrague));
  const [selectedEventType, setSelectedEventType] = useState<EventType>('earnings');
  const [tickerFilter, setTickerFilter] = useState('');

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
  const selectedEvents = eventsByDate[selectedDateKey] || [];

  const selectedEventCounts = useMemo(() => {
    return selectedEvents.reduce<Record<EventType, number>>((acc, event) => {
      acc[event.type] += 1;
      return acc;
    }, {
      earnings: 0,
      dividend: 0,
      split: 0,
      ipo: 0,
    });
  }, [selectedEvents]);

  useEffect(() => {
    const nextType = EVENT_TYPE_ORDER.find((type) => selectedEventCounts[type] > 0) ?? 'earnings';
    if (selectedEventCounts[selectedEventType] === 0 && nextType !== selectedEventType) {
      setSelectedEventType(nextType);
    }
    setTickerFilter('');
  }, [selectedDateKey, selectedEventCounts, selectedEventType]);

  const typeFilteredEvents = useMemo(
    () => selectedEvents.filter((event) => event.type === selectedEventType),
    [selectedEventType, selectedEvents],
  );

  const tickerOptions = useMemo(() => {
    const options = typeFilteredEvents.map((event) => event.ticker || event.company_name || '').filter(Boolean);
    return Array.from(new Set(options));
  }, [typeFilteredEvents]);

  const shouldShowTickerFilter = tickerOptions.length >= TICKER_FILTER_THRESHOLD;

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

  const canGoPrev = startOfMonth(displayMonth) > startOfMonth(rangeStart);
  const canGoNext = startOfMonth(displayMonth) < startOfMonth(rangeEnd);

  const formatEventSecondary = (event: CalendarEvent): string => {
    if (event.type === 'dividend') {
      const details: string[] = [];
      if (event.amount != null) details.push(formatEventAmount(event.amount, event.currency));
      const payDate = typeof event.metadata?.pay_date === 'string' ? event.metadata.pay_date : null;
      if (payDate) details.push(`Pay ${format(parseYmd(payDate), 'dd MMM yyyy')}`);
      return details.join(' • ') || (event.description || 'Upcoming dividend');
    }
    if (event.type === 'split') {
      return event.ratio || event.description || 'Upcoming split';
    }
    if (event.type === 'earnings') {
      const details: string[] = [];
      if (event.estimate != null) details.push(`Est. ${formatEventAmount(event.estimate, event.currency)}`);
      if (event.description) details.push(event.description);
      return details.join(' • ') || 'Scheduled earnings';
    }
    if (event.type === 'ipo') {
      const details: string[] = [];
      if (event.amount != null) details.push(`IPO ${formatEventAmount(event.amount, null)}`);
      if (event.description) details.push(event.description);
      return details.join(' • ') || 'Upcoming IPO';
    }
    return event.description || 'Scheduled';
  };

  return (
    <SafeAreaView style={styles.container} edges={['top']}>
      <View style={[styles.header, { paddingHorizontal: sp.pageGutter }]}>
        <Image source={require('../../assets/images/richstox_icon.png')} style={styles.logo} />
        <Text style={styles.headerTitle}>Markets</Text>
        <View style={styles.headerRight}>
          <TouchableOpacity style={styles.headerIcon} onPress={() => router.push('/(tabs)/search')}>
            <Ionicons name="search-outline" size={22} color={COLORS.text} />
          </TouchableOpacity>
        </View>
      </View>

      <ScrollView style={styles.scroll} contentContainerStyle={{ padding: sp.pageGutter, gap: 12 }}>
        <View style={styles.card}>
          <View style={styles.cardHeader}>
            <View>
              <Text style={styles.sectionTitle}>Calendar</Text>
              <Text style={styles.sectionSubtitle}>{rangeStartStr} → {rangeEndStr} · Prague date</Text>
            </View>
          </View>

          <View style={styles.monthHeader}>
            <TouchableOpacity
              style={[styles.monthNavButton, !canGoPrev && styles.monthNavButtonDisabled]}
              onPress={() => canGoPrev && setDisplayMonth((prev) => startOfMonth(addMonths(prev, -1)))}
              disabled={!canGoPrev}
            >
              <Ionicons name="chevron-back" size={16} color={canGoPrev ? COLORS.primary : COLORS.textMuted} />
            </TouchableOpacity>
            <Text style={styles.monthTitle}>{format(displayMonth, 'MMMM yyyy')}</Text>
            <TouchableOpacity
              style={[styles.monthNavButton, !canGoNext && styles.monthNavButtonDisabled]}
              onPress={() => canGoNext && setDisplayMonth((prev) => startOfMonth(addMonths(prev, 1)))}
              disabled={!canGoNext}
            >
              <Ionicons name="chevron-forward" size={16} color={canGoNext ? COLORS.primary : COLORS.textMuted} />
            </TouchableOpacity>
          </View>

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
        </View>

        <View style={styles.card}>
          <View style={styles.eventsHeader}>
            <View>
              <Text style={styles.eventsDateTitle}>{format(selectedDate, 'EEEE, MMMM d')}</Text>
              <Text style={styles.sectionSubtitle}>{selectedEvents.length} events</Text>
            </View>
            <Text style={styles.eventsCount}>{selectedEvents.length}</Text>
          </View>

          <View style={styles.eventTabsRow}>
            {EVENT_TYPE_ORDER.map((type) => {
              const meta = EVENT_META[type];
              const isActive = selectedEventType === type;
              return (
                <TouchableOpacity
                  key={type}
                  style={[styles.eventTab, isActive && styles.eventTabActive]}
                  onPress={() => setSelectedEventType(type)}
                  accessibilityRole="button"
                >
                  <View style={[styles.eventTabDot, { backgroundColor: meta.color }]} />
                  <Text style={[styles.eventTabText, isActive && styles.eventTabTextActive]}>
                    {meta.label}
                  </Text>
                  <View style={[styles.eventTabCountPill, isActive && styles.eventTabCountPillActive]}>
                    <Text style={[styles.eventTabCountText, isActive && styles.eventTabCountTextActive]}>
                      {selectedEventCounts[type]}
                    </Text>
                  </View>
                </TouchableOpacity>
              );
            })}
          </View>

          {shouldShowTickerFilter && (
            <View style={styles.filterSearchWrap}>
              <Ionicons name="search" size={20} color={COLORS.textMuted} />
              <TextInput
                style={styles.filterSearchInput}
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
          )}

          {loading ? (
            <View style={styles.loadingWrap}>
              <ActivityIndicator size="small" color={COLORS.primary} />
            </View>
          ) : error ? (
            <Text style={styles.errorText}>{error}</Text>
          ) : selectedEvents.length === 0 ? (
            <View style={styles.emptyWrap}>
              <Ionicons name="calendar-outline" size={28} color={COLORS.textMuted} />
              <Text style={styles.emptyText}>No events for this date</Text>
            </View>
          ) : typeFilteredEvents.length === 0 ? (
            <View style={styles.emptyWrap}>
              <Text style={styles.emptyText}>No {EVENT_META[selectedEventType].label.toLowerCase()} for this date</Text>
            </View>
          ) : visibleEvents.length === 0 ? (
            <View style={styles.emptyWrap}>
              <Text style={styles.emptyText}>No matches for “{tickerFilter}”</Text>
            </View>
          ) : (
            visibleEvents.map((event, index) => {
              const meta = EVENT_META[event.type];
              const fallbackKey = getEventFallbackKey(event.ticker, event.company_name);
              const canOpenTicker = Boolean(event.ticker);
              return (
                <TouchableOpacity
                  key={`${event.type}-${event.ticker || 'na'}-${event.date}-${index}`}
                  style={[styles.eventRow, index === visibleEvents.length - 1 && styles.lastEventRow]}
                  onPress={() => {
                    if (event.ticker) router.push(`/stock/${event.ticker}`);
                  }}
                  disabled={!canOpenTicker}
                  activeOpacity={canOpenTicker ? 0.8 : 1}
                >
                  <EventLogo
                    logoUrl={resolveEventLogoUrl(event.logo_url)}
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
            })
          )}
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.background },
  header: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingVertical: 12,
    backgroundColor: COLORS.card,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  logo: { width: 36, height: 36 },
  headerTitle: {
    flex: 1,
    fontSize: 18,
    fontFamily: FONTS.heading,
    color: COLORS.primary,
    marginLeft: 10,
  },
  headerRight: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  headerIcon: { padding: 6 },
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
  sectionTitle: { fontSize: 16, fontWeight: '700', color: COLORS.text },
  sectionSubtitle: { fontSize: 12, color: COLORS.textMuted, marginTop: 2 },
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
  weekdayLabel: {
    width: `${100 / 7}%`,
    textAlign: 'center',
    fontSize: 12,
    fontWeight: '600',
    color: COLORS.textMuted,
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
    flexDirection: 'row',
    alignItems: 'flex-start',
    justifyContent: 'space-between',
    marginBottom: 10,
  },
  eventsDateTitle: { fontSize: 17, fontWeight: '800', color: COLORS.text },
  eventsCount: { fontSize: 20, fontWeight: '800', color: COLORS.primary },
  eventTabsRow: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 10,
    paddingBottom: 12,
    marginBottom: 14,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  eventTab: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
    paddingBottom: 8,
    borderBottomWidth: 2,
    borderBottomColor: 'transparent',
  },
  eventTabActive: {
    borderBottomColor: '#3B82F6',
  },
  eventTabDot: {
    width: 8,
    height: 8,
    borderRadius: 999,
  },
  eventTabText: {
    fontSize: 12,
    fontWeight: '700',
    color: COLORS.textLight,
  },
  eventTabTextActive: {
    color: '#2563EB',
  },
  eventTabCountPill: {
    minWidth: 28,
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 999,
    alignItems: 'center',
    backgroundColor: '#F3F4F6',
  },
  eventTabCountPillActive: {
    backgroundColor: '#DBEAFE',
  },
  eventTabCountText: {
    fontSize: 12,
    fontWeight: '700',
    color: COLORS.textLight,
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
  },
  loadingWrap: { paddingVertical: 32, alignItems: 'center' },
  errorText: { fontSize: 13, color: COLORS.danger, paddingVertical: 8 },
  emptyWrap: { paddingVertical: 28, alignItems: 'center', gap: 8 },
  emptyText: { fontSize: 14, color: COLORS.textMuted, textAlign: 'center' },
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
});
