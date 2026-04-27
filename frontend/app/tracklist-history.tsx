import React, { useCallback, useMemo, useRef, useState } from 'react';
import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  RefreshControl,
  TouchableOpacity,
  Platform,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useFocusEffect, useRouter } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import { LineChart } from 'react-native-gifted-charts';
import axios from 'axios';
import AppHeader from '../components/AppHeader';
import { useAuth } from '../contexts/AuthContext';
import { useLayoutSpacing } from '../constants/layout';
import { API_URL } from '../utils/config';

const COLORS = {
  primary: '#1E3A5F',
  positive: '#34D399',
  negative: '#F87171',
  text: '#1F2937',
  textLight: '#6B7280',
  textMuted: '#9CA3AF',
  background: '#F5F7FA',
  card: '#FFFFFF',
  border: '#E5E7EB',
  warning: '#F59E0B',
};

const EVENT_ICONS: Record<string, string> = {
  auto_seed: '🌱',
  replace: '🔄',
  rebalance: '⚖️',
};

const EVENT_LABELS: Record<string, string> = {
  auto_seed: 'Portfolio opened',
  replace: 'Position replaced',
  rebalance: 'Rebalanced',
};

type ChartMode = 'USD' | '%';

function formatDate(iso?: string | null): string {
  if (!iso) return '—';
  const [y, m, d] = iso.split('-');
  return `${d}/${m}/${y}`;
}

function formatMoney(v?: number | null): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  const sign = v < 0 ? '-' : '';
  return `${sign}$${Math.abs(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function formatSignedMoney(v?: number | null): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  const sign = v >= 0 ? '+' : '-';
  return `${sign}$${Math.abs(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function formatSignedPct(v?: number | null): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;
}

export default function TracklistHistoryPage() {
  const router = useRouter();
  const { sessionToken } = useAuth();
  const sp = useLayoutSpacing();
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [data, setData] = useState<any>(null);
  const [chartMode, setChartMode] = useState<ChartMode>('%');

  const authHeaders = useMemo(
    () => (sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {}),
    [sessionToken],
  );

  const fetchData = useCallback(async () => {
    if (!sessionToken) return;
    try {
      const res = await axios.get(`${API_URL}/api/v1/tracklist`, { headers: authHeaders });
      setData(res.data);
    } catch (err) {
      console.error('TracklistHistory fetch error:', err);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [authHeaders, sessionToken]);

  useFocusEffect(useCallback(() => { fetchData(); }, [fetchData]));

  const onRefresh = () => { setRefreshing(true); fetchData(); };

  const performance = data?.performance;
  const metrics = performance?.metrics;
  const eventsHistory: any[] = data?.events_history || [];

  // Build full chart series (all points, not just last 30)
  const fullSeries: { date: string; value: number }[] = useMemo(() => {
    if (!performance?.ready) return [];
    return chartMode === 'USD' ? (performance.series_usd || []) : (performance.series_pct || []);
  }, [performance, chartMode]);

  const chartData = useMemo(() => {
    return fullSeries.map((pt: any, idx: number) => ({
      value: Number(pt.value ?? 0),
      label: idx === 0 || idx === fullSeries.length - 1 ? String(pt.date || '').slice(5) : '',
    }));
  }, [fullSeries]);

  const chartIndicators = useMemo(() => {
    if (!fullSeries.length) return null;
    let high = fullSeries[0];
    let low = fullSeries[0];
    for (const pt of fullSeries) {
      if ((pt.value ?? 0) >= (high.value ?? 0)) high = pt;
      if ((pt.value ?? 0) <= (low.value ?? 0)) low = pt;
    }
    return { high, low, current: fullSeries[fullSeries.length - 1], start: fullSeries[0] };
  }, [fullSeries]);

  const formatMetric = (v?: number | null) =>
    chartMode === 'USD' ? formatMoney(v) : (typeof v === 'number' ? `${v >= 0 ? '+' : ''}${v.toFixed(2)}%` : '—');

  return (
    <SafeAreaView style={styles.container} edges={['top']}>
      <AppHeader title="Portfolio History" />
      <ScrollView
        contentContainerStyle={[styles.scrollContent, { padding: sp.pageGutter }]}
        refreshControl={<RefreshControl refreshing={refreshing} onRefresh={onRefresh} />}
        showsVerticalScrollIndicator={false}
      >
        {/* ── Performance chart ── */}
        <View style={styles.chartCard}>
          <View style={styles.chartCardHeader}>
            <View>
              <Text style={styles.chartCardTitle}>Portfolio performance</Text>
              <Text style={styles.chartCardSubtitle}>Full history from inception</Text>
            </View>
            <View style={styles.modeToggle}>
              {(['%', 'USD'] as ChartMode[]).map((m) => (
                <TouchableOpacity
                  key={m}
                  style={[styles.modeChip, chartMode === m && styles.modeChipActive]}
                  onPress={() => setChartMode(m)}
                >
                  <Text style={[styles.modeChipText, chartMode === m && styles.modeChipTextActive]}>{m}</Text>
                </TouchableOpacity>
              ))}
            </View>
          </View>

          {!performance?.ready ? (
            <View style={styles.emptyChart}>
              <Text style={styles.emptyChartText}>Performance data not yet available.</Text>
            </View>
          ) : (
            <>
              {chartIndicators && (
                <View style={styles.chartLegend}>
                  <View style={styles.chartLegendItem}>
                    <Text style={[styles.chartLegendTag, { color: COLORS.positive }]}>HIGH</Text>
                    <Text style={styles.chartLegendVal}>{formatMetric(chartIndicators.high.value)}</Text>
                    <Text style={styles.chartLegendDate}>{formatDate(chartIndicators.high.date)}</Text>
                  </View>
                  <View style={styles.chartLegendItem}>
                    <Text style={[styles.chartLegendTag, { color: COLORS.negative }]}>LOW</Text>
                    <Text style={styles.chartLegendVal}>{formatMetric(chartIndicators.low.value)}</Text>
                    <Text style={styles.chartLegendDate}>{formatDate(chartIndicators.low.date)}</Text>
                  </View>
                  <View style={styles.chartLegendItem}>
                    <Text style={styles.chartLegendTag}>NOW</Text>
                    <Text style={styles.chartLegendVal}>{formatMetric(chartIndicators.current.value)}</Text>
                    <Text style={styles.chartLegendDate}>{formatDate(chartIndicators.current.date)}</Text>
                  </View>
                </View>
              )}
              {chartData.length > 1 && (
                <LineChart
                  data={chartData}
                  width={300}
                  height={180}
                  spacing={Math.max(4, Math.floor(280 / Math.max(chartData.length - 1, 1)))}
                  initialSpacing={0}
                  endSpacing={0}
                  color="#1E3A5F"
                  thickness={2}
                  hideDataPoints
                  curved
                  areaChart
                  startFillColor="#1E3A5F"
                  endFillColor="#1E3A5F"
                  startOpacity={0.15}
                  endOpacity={0.02}
                  hideYAxisText
                  yAxisColor="transparent"
                  xAxisColor={COLORS.border}
                  xAxisLabelTextStyle={{ color: COLORS.textMuted, fontSize: 10 }}
                  rulesColor={COLORS.border}
                />
              )}
            </>
          )}
        </View>

        {/* ── Performance metrics ── */}
        {metrics && (
          <View style={styles.card}>
            <Text style={styles.cardTitle}>Performance summary</Text>
            <View style={styles.metricsGrid}>
              {[
                { label: 'Total return', value: formatSignedPct(metrics.total_profit_pct) },
                { label: 'Equity value', value: formatMoney(performance?.equity_value) },
                { label: 'Realized P/L', value: formatSignedMoney(metrics.realized_pnl_usd), colored: true, v: metrics.realized_pnl_usd },
                { label: 'Unrealized P/L', value: formatSignedMoney(metrics.unrealized_pnl_usd), colored: true, v: metrics.unrealized_pnl_usd },
                { label: 'Avg / year', value: formatSignedPct(metrics.avg_per_year_pct) },
                { label: 'Max drawdown', value: `-${Math.abs(metrics.max_drawdown_pct ?? 0).toFixed(2)}%` },
                { label: 'Track record', value: `${metrics.track_record_days ?? 0} days` },
                { label: 'Vs. S&P 500', value: formatSignedPct(metrics.vs_benchmark_pct), colored: true, v: metrics.vs_benchmark_pct },
              ].map(({ label, value, colored, v }) => (
                <View key={label} style={styles.metricCell}>
                  <Text style={styles.metricLabel}>{label}</Text>
                  <Text style={[
                    styles.metricValue,
                    colored ? ((v ?? 0) >= 0 ? styles.positive : styles.negative) : undefined,
                  ]}>
                    {value}
                  </Text>
                </View>
              ))}
            </View>
          </View>
        )}

        {/* ── Events timeline ── */}
        <View style={styles.card}>
          <Text style={styles.cardTitle}>Events timeline</Text>
          {eventsHistory.length === 0 ? (
            <Text style={styles.emptyText}>No events recorded yet.</Text>
          ) : (
            eventsHistory.map((evt, idx) => {
              const isReplace = evt.event_type === 'replace';
              const isSeed = evt.event_type === 'auto_seed';
              const plValue = evt.realized_pnl ?? 0;
              const showPl = isReplace && Math.abs(plValue) > 0.001;
              return (
                <View key={idx} style={[styles.eventRow, idx === eventsHistory.length - 1 && styles.eventRowLast]}>
                  {/* Icon + vertical line */}
                  <View style={styles.eventIconCol}>
                    <View style={[styles.eventIconCircle, isSeed && styles.eventIconCircleSeed, isReplace && styles.eventIconCircleReplace]}>
                      <Text style={styles.eventIconText}>{EVENT_ICONS[evt.event_type] ?? '•'}</Text>
                    </View>
                    {idx < eventsHistory.length - 1 && <View style={styles.eventLine} />}
                  </View>
                  {/* Content */}
                  <View style={styles.eventContent}>
                    <View style={styles.eventHeaderRow}>
                      <Text style={styles.eventLabel}>{EVENT_LABELS[evt.event_type] ?? evt.event_type}</Text>
                      <Text style={styles.eventDate}>{evt.effective_date_display}</Text>
                    </View>
                    {isReplace && (
                      <Text style={styles.eventDetail}>
                        {evt.removed_ticker} → {evt.added_ticker}
                      </Text>
                    )}
                    {isSeed && evt.capital > 0 && (
                      <Text style={styles.eventDetail}>
                        Starting capital: {formatMoney(evt.capital)}
                      </Text>
                    )}
                    {showPl && (
                      <Text style={[styles.eventPl, plValue >= 0 ? styles.positive : styles.negative]}>
                        Realized P/L: {formatSignedMoney(plValue)}
                      </Text>
                    )}
                  </View>
                </View>
              );
            })
          )}
        </View>

        {/* ── Navigate to individual tickers ── */}
        {(data?.positions || []).length > 0 && (
          <View style={styles.card}>
            <Text style={styles.cardTitle}>Current positions</Text>
            {(data.positions as any[]).map((pos: any) => (
              <TouchableOpacity
                key={pos.ticker}
                style={styles.positionRow}
                onPress={() => router.push(`/stock/${pos.ticker}`)}
              >
                <View>
                  <Text style={styles.positionTicker}>{pos.ticker}</Text>
                  <Text style={styles.positionMeta}>
                    {pos.shares} pcs · Avg ${Number(pos.avg_cost || pos.entry_price || 0).toFixed(2)}
                  </Text>
                </View>
                <View style={styles.positionRight}>
                  <Text style={styles.positionSince}>Opened {pos.entry_date ? formatDate(pos.entry_date) : '—'}</Text>
                  <Ionicons name="chevron-forward" size={16} color={COLORS.textMuted} />
                </View>
              </TouchableOpacity>
            ))}
          </View>
        )}
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.background },
  scrollContent: { paddingBottom: 32 },

  chartCard: {
    backgroundColor: COLORS.primary,
    borderRadius: 16,
    padding: 16,
    marginBottom: 16,
    ...Platform.select({
      web: { boxShadow: '0 2px 8px rgba(0,0,0,0.12)' },
      default: { elevation: 3 },
    }),
  },
  chartCardHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    marginBottom: 12,
  },
  chartCardTitle: { fontSize: 16, fontWeight: '700', color: '#FFF' },
  chartCardSubtitle: { fontSize: 12, color: 'rgba(255,255,255,0.7)', marginTop: 2 },
  modeToggle: { flexDirection: 'row', gap: 4 },
  modeChip: {
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 6,
    backgroundColor: 'rgba(255,255,255,0.15)',
  },
  modeChipActive: { backgroundColor: 'rgba(255,255,255,0.9)' },
  modeChipText: { fontSize: 12, fontWeight: '600', color: 'rgba(255,255,255,0.8)' },
  modeChipTextActive: { color: COLORS.primary },
  emptyChart: { paddingVertical: 32, alignItems: 'center' },
  emptyChartText: { color: 'rgba(255,255,255,0.6)', fontSize: 14 },
  chartLegend: { flexDirection: 'row', justifyContent: 'space-around', marginBottom: 12 },
  chartLegendItem: { alignItems: 'center' },
  chartLegendTag: { fontSize: 10, fontWeight: '700', color: 'rgba(255,255,255,0.7)', letterSpacing: 0.5 },
  chartLegendVal: { fontSize: 13, fontWeight: '700', color: '#FFF', marginTop: 2 },
  chartLegendDate: { fontSize: 10, color: 'rgba(255,255,255,0.6)', marginTop: 1 },

  card: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 16,
    marginBottom: 16,
    ...Platform.select({
      web: { boxShadow: '0 1px 3px rgba(0,0,0,0.08)' },
      default: { elevation: 1 },
    }),
  },
  cardTitle: { fontSize: 15, fontWeight: '700', color: COLORS.text, marginBottom: 14 },
  emptyText: { fontSize: 14, color: COLORS.textMuted, textAlign: 'center', paddingVertical: 16 },

  metricsGrid: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 0,
  },
  metricCell: {
    width: '50%',
    paddingVertical: 8,
    paddingHorizontal: 4,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  metricLabel: { fontSize: 11, color: COLORS.textMuted, marginBottom: 2 },
  metricValue: { fontSize: 14, fontWeight: '700', color: COLORS.text },
  positive: { color: COLORS.positive },
  negative: { color: COLORS.negative },

  // Events timeline
  eventRow: {
    flexDirection: 'row',
    gap: 12,
    paddingBottom: 16,
  },
  eventRowLast: { paddingBottom: 0 },
  eventIconCol: { alignItems: 'center', width: 36 },
  eventIconCircle: {
    width: 36,
    height: 36,
    borderRadius: 18,
    backgroundColor: COLORS.border,
    alignItems: 'center',
    justifyContent: 'center',
  },
  eventIconCircleSeed: { backgroundColor: '#D1FAE5' },
  eventIconCircleReplace: { backgroundColor: '#FEF3C7' },
  eventIconText: { fontSize: 16 },
  eventLine: {
    width: 2,
    flex: 1,
    backgroundColor: COLORS.border,
    marginTop: 4,
    minHeight: 16,
  },
  eventContent: { flex: 1, paddingTop: 6 },
  eventHeaderRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    marginBottom: 2,
  },
  eventLabel: { fontSize: 14, fontWeight: '600', color: COLORS.text },
  eventDate: { fontSize: 12, color: COLORS.textMuted },
  eventDetail: { fontSize: 13, color: COLORS.textLight, marginTop: 2 },
  eventPl: { fontSize: 13, fontWeight: '600', marginTop: 4 },

  // Current positions list
  positionRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    paddingVertical: 10,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  positionTicker: { fontSize: 15, fontWeight: '700', color: COLORS.text },
  positionMeta: { fontSize: 12, color: COLORS.textLight, marginTop: 2 },
  positionRight: { flexDirection: 'row', alignItems: 'center', gap: 4 },
  positionSince: { fontSize: 12, color: COLORS.textMuted },
});
