import React, { useEffect, useMemo, useState } from 'react';
import {
  ActivityIndicator,
  Image,
  Platform,
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
import AppHeader from '../../components/AppHeader';

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
type CalendarViewMode = 'daily' | 'monthly' | 'yearly';

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
const INITIAL_VISIBLE_EVENTS = 10;
const MAX_VISIBLE_MONTH_CARDS = 4;
const EVENT_TYPE_ORDER: EventType[] = ['earnings', 'dividend', 'split', 'ipo'];
const CALENDAR_VIEW_ORDER: CalendarViewMode[] = ['daily', 'monthly', 'yearly'];

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
  split: { label: 'Splits', shortLabel: 'SPL', legendLabel: 'S = Splits', singularLabel: 'Split', color: '#F59E0B', icon: 'git-compare-outline' },
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

const resolveEventLogoUrl = (rawUrl?: string | null, ticker?: string | null): string | undefined => {
  const normalizedTicker = ticker?.trim().toUpperCase();
  if (!rawUrl && normalizedTicker) return `${API_URL}/api/logo/${normalizedTicker}`;
  if (!rawUrl) return undefined;
  return rawUrl.startsWith('http') ? rawUrl : `${API_URL}${rawUrl}`;
};

const isValidYmd = (value: string): boolean => {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
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
  const [visibleEventLimit, setVisibleEventLimit] = useState(INITIAL_VISIBLE_EVENTS);
  const [isCalendarExpanded, setIsCalendarExpanded] = useState(false);

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
    setVisibleEventLimit(INITIAL_VISIBLE_EVENTS);
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
    const nextType = EVENT_TYPE_ORDER.find((type) => selectedEventCounts[type] > 0) ?? 'earnings';
    if (selectedEventCounts[selectedEventType] === 0 && nextType !== selectedEventType) {
      setSelectedEventType(nextType);
      setTickerFilter('');
    }
  }, [selectedEventCounts, selectedEventType]);

  const typeFilteredEvents = useMemo(
    () => periodEvents.filter((event) => event.type === selectedEventType),
    [periodEvents, selectedEventType],
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

  const displayedEvents = useMemo(
    () => visibleEvents.slice(0, visibleEventLimit),
    [visibleEventLimit, visibleEvents],
  );

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
      if (event.amount != null) details.push(formatEventAmount(event.amount, event.currency));
      const payDate = typeof event.metadata?.pay_date === 'string' ? event.metadata.pay_date : null;
      if (payDate && isValidYmd(payDate)) details.push(`Pay ${formatDateDMY(payDate)}`);
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

  return (
    <SafeAreaView style={styles.container} edges={['top']}>
      <AppHeader title="Markets" />

      <ScrollView style={styles.scroll} contentContainerStyle={{ padding: sp.pageGutter, gap: 12 }}>
        <View style={styles.card}>
          <View style={styles.cardHeader}>
            <View>
              <Text style={styles.sectionTitle}>Calendar</Text>
              <Text style={styles.sectionSubtitle}>{rangeStartStr} → {rangeEndStr} · Prague date</Text>
            </View>
          </View>
          <View style={styles.viewToggleRow}>
            {CALENDAR_VIEW_ORDER.map((viewMode) => {
              const isActive = calendarView === viewMode;
              return (
                <TouchableOpacity
                  key={viewMode}
                  style={[styles.viewToggleButton, isActive && styles.viewToggleButtonActive]}
                  onPress={() => setCalendarView(viewMode)}
                >
                  <Text style={[styles.viewToggleText, isActive && styles.viewToggleTextActive]}>
                    {CALENDAR_VIEW_META[viewMode].label}
                  </Text>
                </TouchableOpacity>
              );
            })}
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

              <ScrollView
                horizontal
                showsHorizontalScrollIndicator={false}
                style={styles.activeDaysScroll}
                contentContainerStyle={styles.activeDaysScrollContent}
              >
                {activeDayKeysForDisplayMonth.map((dayKey) => {
                  const day = parseYmd(dayKey);
                  const isSelected = dayKey === selectedDateKey;
                  const dayEvents = eventsByDate[dayKey] || [];
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
                      <Text style={[styles.activeDayCount, isSelected && styles.activeDayTextSelected]}>
                        {dayEvents.length} events
                      </Text>
                    </TouchableOpacity>
                  );
                })}
              </ScrollView>

              <TouchableOpacity
                style={styles.calendarToggleButton}
                onPress={() => setIsCalendarExpanded((prev) => !prev)}
              >
                <Text style={styles.calendarToggleText}>
                  {isCalendarExpanded ? 'Hide full calendar' : 'Show full calendar'}
                </Text>
                <Ionicons
                  name={isCalendarExpanded ? 'chevron-up' : 'chevron-down'}
                  size={16}
                  color={COLORS.primary}
                />
              </TouchableOpacity>

              {isCalendarExpanded && (
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

        <View style={styles.card}>
          <View style={styles.eventsHeader}>
            <View>
              <Text style={styles.eventsDateTitle}>{selectedPeriodLabel}</Text>
              <Text style={styles.sectionSubtitle}>{periodEvents.length} events</Text>
            </View>
            <Text style={styles.eventsCount}>{periodEvents.length}</Text>
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
                    {meta.shortLabel}
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
          )}

          {loading ? (
            <View style={styles.loadingWrap}>
              <ActivityIndicator size="small" color={COLORS.primary} />
            </View>
          ) : error ? (
            <Text style={styles.errorText}>{error}</Text>
          ) : periodEvents.length === 0 ? (
            <View style={styles.emptyWrap}>
              <Ionicons name="calendar-outline" size={28} color={COLORS.textMuted} />
              <Text style={styles.emptyText}>No events for this {CALENDAR_VIEW_META[calendarView].emptyLabel}</Text>
            </View>
          ) : typeFilteredEvents.length === 0 ? (
            <View style={styles.emptyWrap}>
              <Text style={styles.emptyText}>No {EVENT_META[selectedEventType].label.toLowerCase()} for this {CALENDAR_VIEW_META[calendarView].emptyLabel}</Text>
            </View>
          ) : visibleEvents.length === 0 ? (
            <View style={styles.emptyWrap}>
              <Text style={styles.emptyText}>No matches for “{tickerFilter}”</Text>
            </View>
          ) : (
            displayedEvents.map((event, index) => {
              const meta = EVENT_META[event.type];
              const fallbackKey = getEventFallbackKey(event.ticker, event.company_name);
              const canOpenTicker = Boolean(event.ticker);
              return (
                <TouchableOpacity
                  key={`${event.type}-${event.ticker || 'na'}-${event.date}-${index}`}
                  style={[styles.eventRow, index === displayedEvents.length - 1 && styles.lastEventRow]}
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
            })
          )}

          {visibleEvents.length > 0 && (
            <View style={styles.eventsButtonsRow}>
              {visibleEventLimit < visibleEvents.length && (
                <TouchableOpacity
                  style={styles.loadMoreButtonFull}
                  onPress={() => setVisibleEventLimit((prev) => prev + INITIAL_VISIBLE_EVENTS)}
                >
                  <Text style={styles.loadMoreText}>Load more events</Text>
                </TouchableOpacity>
              )}
              {visibleEventLimit > INITIAL_VISIBLE_EVENTS && (
                <TouchableOpacity
                  style={styles.seeLessButtonFull}
                  onPress={() => setVisibleEventLimit(INITIAL_VISIBLE_EVENTS)}
                >
                  <Text style={styles.seeLessText}>See less</Text>
                </TouchableOpacity>
              )}
            </View>
          )}
        </View>
      </ScrollView>
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
  sectionTitle: { fontSize: 16, fontWeight: '700', color: COLORS.text },
  sectionSubtitle: { fontSize: 12, color: COLORS.textMuted, marginTop: 2 },
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
    marginBottom: 12,
  },
  activeDaysScrollContent: {
    gap: 10,
    paddingRight: 4,
  },
  activeDayCard: {
    minWidth: 88,
    paddingVertical: 10,
    paddingHorizontal: 12,
    borderRadius: 14,
    borderWidth: 1,
    borderColor: COLORS.border,
    backgroundColor: '#F8FAFC',
    gap: 2,
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
  activeDayCount: {
    fontSize: 11,
    color: COLORS.textLight,
    fontWeight: '600',
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
    backgroundColor: COLORS.primary + '10',
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
    backgroundColor: COLORS.primary + '10',
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
});
