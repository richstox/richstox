import React, { useEffect, useMemo, useState } from 'react';
import {
  ActivityIndicator,
  Image,
  ScrollView,
  StyleSheet,
  Text,
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

type CalendarEvent = {
  date: string;
  type: 'earnings' | 'dividend' | 'split' | 'ipo';
  ticker: string | null;
  label: string;
  description?: string | null;
  amount?: number | null;
  ratio?: string | null;
  estimate?: number | null;
  currency?: string | null;
  metadata?: Record<string, unknown>;
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

const EVENT_META: Record<CalendarEvent['type'], { label: string; color: string; icon: keyof typeof Ionicons.glyphMap }> = {
  earnings: { label: 'Earnings', color: COLORS.primary, icon: 'bar-chart-outline' },
  dividend: { label: 'Dividend', color: COLORS.accent, icon: 'cash-outline' },
  split: { label: 'Split', color: '#8B5CF6', icon: 'git-compare-outline' },
  ipo: { label: 'IPO', color: COLORS.warning, icon: 'rocket-outline' },
};

const WEEKDAY_LABELS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']; // Prague calendar uses Monday-start weeks.

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
    if (event.type === 'dividend' && event.amount != null) {
      return formatEventAmount(event.amount, event.currency);
    }
    if (event.type === 'split' && event.ratio) {
      return event.ratio;
    }
    if (event.type === 'earnings' && event.estimate != null) {
      return `Est. ${formatEventAmount(event.estimate, event.currency)}`;
    }
    if (event.type === 'ipo' && event.amount != null) {
      return `IPO ${formatEventAmount(event.amount, null)}`;
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
          <View style={styles.cardHeader}>
            <View>
              <Text style={styles.sectionTitle}>Events</Text>
              <Text style={styles.sectionSubtitle}>{format(selectedDate, 'dd MMM yyyy')}</Text>
            </View>
            <Text style={styles.eventsCount}>{selectedEvents.length}</Text>
          </View>

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
          ) : (
            selectedEvents.map((event, index) => {
              const meta = EVENT_META[event.type];
              return (
                <View
                  key={`${event.type}-${event.ticker || 'na'}-${event.date}-${index}`}
                  style={[styles.eventRow, index === selectedEvents.length - 1 && styles.lastEventRow]}
                >
                  <View style={[styles.eventIconWrap, { backgroundColor: `${meta.color}15` }]}>
                    <Ionicons name={meta.icon} size={16} color={meta.color} />
                  </View>
                  <View style={styles.eventContent}>
                    <View style={styles.eventTopRow}>
                      <Text style={styles.eventTicker}>{event.ticker || 'Market'}</Text>
                      <View style={[styles.eventPill, { backgroundColor: `${meta.color}15` }]}>
                        <Text style={[styles.eventPillText, { color: meta.color }]}>{meta.label}</Text>
                      </View>
                    </View>
                    <Text style={styles.eventTitle}>{event.label}</Text>
                    <Text style={styles.eventMeta}>{formatEventSecondary(event)}</Text>
                  </View>
                </View>
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
  eventsCount: { fontSize: 20, fontWeight: '800', color: COLORS.primary },
  loadingWrap: { paddingVertical: 32, alignItems: 'center' },
  errorText: { fontSize: 13, color: COLORS.danger, paddingVertical: 8 },
  emptyWrap: { paddingVertical: 28, alignItems: 'center', gap: 8 },
  emptyText: { fontSize: 14, color: COLORS.textMuted },
  eventRow: {
    flexDirection: 'row',
    gap: 12,
    paddingVertical: 12,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  lastEventRow: { borderBottomWidth: 0, paddingBottom: 0 },
  eventIconWrap: {
    width: 34,
    height: 34,
    borderRadius: 17,
    alignItems: 'center',
    justifyContent: 'center',
    marginTop: 2,
  },
  eventContent: { flex: 1, gap: 4 },
  eventTopRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    gap: 8,
  },
  eventTicker: { fontSize: 12, fontWeight: '700', color: COLORS.textMuted, textTransform: 'uppercase' },
  eventPill: { paddingHorizontal: 8, paddingVertical: 4, borderRadius: 999 },
  eventPillText: { fontSize: 11, fontWeight: '700' },
  eventTitle: { fontSize: 14, fontWeight: '600', color: COLORS.text, lineHeight: 20 },
  eventMeta: { fontSize: 12, color: COLORS.textLight, lineHeight: 18 },
});
