/**
 * Trading History page
 *
 * Shows all tracklist positions ever held — both currently open and previously
 * closed (replaced). When navigated with a `?ticker=TSLA` query parameter only
 * that ticker's entries are shown.
 *
 * Navigation targets:
 *   /trading-history              — all positions
 *   /trading-history?ticker=TSLA  — TSLA only
 */
import React, { useCallback, useMemo, useState } from 'react';
import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  RefreshControl,
  TouchableOpacity,
  Platform,
  ActivityIndicator,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useFocusEffect, useLocalSearchParams } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
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

// ─── helpers ────────────────────────────────────────────────────────────────

function fmtDate(iso?: string | null): string {
  if (!iso) return '—';
  const [y, m, d] = iso.split('-');
  return `${d}/${m}/${y}`;
}

function fmtMoney(v?: number | null): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  const sign = v < 0 ? '-' : '';
  return `${sign}$${Math.abs(v).toLocaleString('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function fmtSignedMoney(v?: number | null): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return `${v >= 0 ? '+' : ''}${fmtMoney(v)}`;
}

function fmtSignedPct(v?: number | null): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;
}

function fmtDuration(days?: number | null): string {
  if (days == null || days < 0) return '—';
  if (days < 30) return `${days}d`;
  const months = Math.floor(days / 30.44);
  if (months < 12) return `${months}mo`;
  const years = Math.floor(months / 12);
  const remMonths = months % 12;
  return remMonths > 0 ? `${years}y ${remMonths}mo` : `${years}y`;
}

// ─── sub-components ──────────────────────────────────────────────────────────

function SummaryRow({ label, value, valueStyle }: { label: string; value: string; valueStyle?: object }) {
  return (
    <View style={styles.summaryRow}>
      <Text style={styles.summaryLabel}>{label}</Text>
      <Text style={[styles.summaryValue, valueStyle]}>{value}</Text>
    </View>
  );
}

interface ClosedPosition {
  ticker: string;
  status: 'closed';
  shares: number;
  entry_price: number;
  avg_cost: number;
  entry_date: string | null;
  entry_date_display: string | null;
  exit_price: number | null;
  exit_date: string | null;
  exit_date_display: string | null;
  realized_pnl_usd: number | null;
  realized_pnl_pct: number | null;
  realized_pnl_pct_of_capital: number | null;
  duration_days: number | null;
  replaced_by: string | null;
}

interface OpenPosition {
  ticker: string;
  status: 'open';
  shares: number;
  entry_price: number;
  avg_cost: number;
  entry_date: string | null;
  entry_date_display: string | null;
  current_price: number | null;
  unrealized_pnl_usd: number | null;
  unrealized_pnl_pct: number | null;
}

function ClosedCard({ pos }: { pos: ClosedPosition }) {
  const pl = pos.realized_pnl_usd ?? 0;
  const isPositive = pl >= 0;
  return (
    <View style={styles.tradeCard}>
      {/* Header row */}
      <View style={styles.tradeCardHeader}>
        <View style={styles.tradeTickerRow}>
          <Text style={styles.tradeTicker}>{pos.ticker}</Text>
          <View style={styles.closedPill}>
            <Text style={styles.closedPillText}>CLOSED</Text>
          </View>
        </View>
        {pos.replaced_by && (
          <Text style={styles.tradeSubtext}>Replaced by {pos.replaced_by}</Text>
        )}
      </View>

      {/* Dates row */}
      <View style={styles.tradeRow}>
        <View style={styles.tradeCell}>
          <Text style={styles.tradeCellLabel}>Entry date</Text>
          <Text style={styles.tradeCellValue}>{fmtDate(pos.entry_date)}</Text>
        </View>
        <Ionicons name="arrow-forward" size={14} color={COLORS.textMuted} style={styles.tradeArrow} />
        <View style={[styles.tradeCell, styles.tradeCellRight]}>
          <Text style={[styles.tradeCellLabel, styles.tradeCellLabelRight]}>Exit date</Text>
          <Text style={[styles.tradeCellValue, styles.tradeCellValueRight]}>{fmtDate(pos.exit_date)}</Text>
        </View>
      </View>

      {/* Prices row */}
      <View style={styles.tradeRow}>
        <View style={styles.tradeCell}>
          <Text style={styles.tradeCellLabel}>Entry price (avg)</Text>
          <Text style={styles.tradeCellValue}>{fmtMoney(pos.avg_cost)}</Text>
        </View>
        <Ionicons name="arrow-forward" size={14} color={COLORS.textMuted} style={styles.tradeArrow} />
        <View style={[styles.tradeCell, styles.tradeCellRight]}>
          <Text style={[styles.tradeCellLabel, styles.tradeCellLabelRight]}>Exit price</Text>
          <Text style={[styles.tradeCellValue, styles.tradeCellValueRight]}>{fmtMoney(pos.exit_price)}</Text>
        </View>
      </View>

      {/* Shares + duration */}
      <View style={styles.tradeRow}>
        <View style={styles.tradeCell}>
          <Text style={styles.tradeCellLabel}>Shares</Text>
          <Text style={styles.tradeCellValue}>{pos.shares} pcs</Text>
        </View>
        <View style={[styles.tradeCell, styles.tradeCellRight]}>
          <Text style={[styles.tradeCellLabel, styles.tradeCellLabelRight]}>Duration</Text>
          <Text style={[styles.tradeCellValue, styles.tradeCellValueRight]}>{fmtDuration(pos.duration_days)}</Text>
        </View>
      </View>

      {/* P/L highlight */}
      <View style={[styles.plRow, isPositive ? styles.plRowPositive : styles.plRowNegative]}>
        <Text style={[styles.plLabel, isPositive ? styles.positive : styles.negative]}>Realized P/L</Text>
        <View style={styles.plValues}>
          <Text style={[styles.plUsd, isPositive ? styles.positive : styles.negative]}>
            {fmtSignedMoney(pos.realized_pnl_usd)}
          </Text>
          <Text style={[styles.plPct, isPositive ? styles.positive : styles.negative]}>
            ({fmtSignedPct(pos.realized_pnl_pct)})
          </Text>
        </View>
      </View>
    </View>
  );
}

function OpenCard({ pos }: { pos: OpenPosition }) {
  const pl = pos.unrealized_pnl_usd ?? 0;
  const isPositive = pl >= 0;
  return (
    <View style={styles.tradeCard}>
      <View style={styles.tradeCardHeader}>
        <View style={styles.tradeTickerRow}>
          <Text style={styles.tradeTicker}>{pos.ticker}</Text>
          <View style={styles.openPill}>
            <Text style={styles.openPillText}>OPEN</Text>
          </View>
        </View>
        <Text style={styles.tradeSubtext}>Since {fmtDate(pos.entry_date)}</Text>
      </View>

      {/* Prices row */}
      <View style={styles.tradeRow}>
        <View style={styles.tradeCell}>
          <Text style={styles.tradeCellLabel}>Entry price (avg)</Text>
          <Text style={styles.tradeCellValue}>{fmtMoney(pos.avg_cost)}</Text>
        </View>
        <Ionicons name="arrow-forward" size={14} color={COLORS.textMuted} style={styles.tradeArrow} />
        <View style={[styles.tradeCell, styles.tradeCellRight]}>
          <Text style={[styles.tradeCellLabel, styles.tradeCellLabelRight]}>Last close</Text>
          <Text style={[styles.tradeCellValue, styles.tradeCellValueRight]}>{fmtMoney(pos.current_price)}</Text>
        </View>
      </View>

      {/* Shares */}
      <View style={[styles.tradeRow, { borderBottomWidth: 0, paddingBottom: 0 }]}>
        <View style={styles.tradeCell}>
          <Text style={styles.tradeCellLabel}>Shares</Text>
          <Text style={styles.tradeCellValue}>{pos.shares} pcs</Text>
        </View>
      </View>

      {/* Open P/L */}
      <View style={[styles.plRow, isPositive ? styles.plRowPositive : styles.plRowNegative]}>
        <Text style={[styles.plLabel, isPositive ? styles.positive : styles.negative]}>Open P/L</Text>
        <View style={styles.plValues}>
          <Text style={[styles.plUsd, isPositive ? styles.positive : styles.negative]}>
            {fmtSignedMoney(pos.unrealized_pnl_usd)}
          </Text>
          <Text style={[styles.plPct, isPositive ? styles.positive : styles.negative]}>
            ({fmtSignedPct(pos.unrealized_pnl_pct)})
          </Text>
        </View>
      </View>
    </View>
  );
}

// ─── main component ──────────────────────────────────────────────────────────

export default function TradingHistoryPage() {
  const { ticker: tickerParam } = useLocalSearchParams<{ ticker?: string }>();
  const filterTicker = typeof tickerParam === 'string' ? tickerParam.toUpperCase() : null;
  const { sessionToken } = useAuth();
  const sp = useLayoutSpacing();
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [data, setData] = useState<any>(null);

  const authHeaders = useMemo(
    () => (sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {}),
    [sessionToken],
  );

  const fetchData = useCallback(async () => {
    if (!sessionToken) return;
    try {
      const res = await axios.get(`${API_URL}/api/v1/tracklist/positions-history`, {
        headers: authHeaders,
      });
      setData(res.data);
    } catch (err) {
      console.error('TradingHistory fetch error:', err);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [authHeaders, sessionToken]);

  useFocusEffect(useCallback(() => { fetchData(); }, [fetchData]));

  const onRefresh = () => { setRefreshing(true); fetchData(); };

  const allOpen: OpenPosition[] = data?.open_positions ?? [];
  const allClosed: ClosedPosition[] = data?.closed_positions ?? [];

  const openPositions = filterTicker
    ? allOpen.filter((p) => p.ticker === filterTicker)
    : allOpen;
  const closedPositions = filterTicker
    ? allClosed.filter((p) => p.ticker === filterTicker)
    : allClosed;

  const summary = data?.summary;

  const headerTitle = filterTicker ? `${filterTicker} — Trading History` : 'All Positions History';

  return (
    <SafeAreaView style={styles.container} edges={['top']}>
      <AppHeader title={headerTitle} />
      <ScrollView
        contentContainerStyle={[styles.scrollContent, { padding: sp.pageGutter }]}
        refreshControl={<RefreshControl refreshing={refreshing} onRefresh={onRefresh} />}
        showsVerticalScrollIndicator={false}
      >
        {loading ? (
          <ActivityIndicator size="large" color={COLORS.primary} style={styles.loader} />
        ) : (
          <>
            {/* Summary card — only show on all-positions view */}
            {!filterTicker && summary && (
              <View style={styles.summaryCard}>
                <Text style={styles.summaryTitle}>Summary</Text>
                <SummaryRow label="Open positions" value={`${summary.total_open}`} />
                <SummaryRow label="Closed positions" value={`${summary.total_closed}`} />
                <SummaryRow
                  label="Total realized P/L"
                  value={fmtSignedMoney(summary.realized_pnl_usd)}
                  valueStyle={(summary.realized_pnl_usd ?? 0) >= 0 ? styles.positive : styles.negative}
                />
              </View>
            )}

            {/* Open positions */}
            {openPositions.length > 0 && (
              <>
                <Text style={styles.sectionHeader}>
                  Currently open{filterTicker ? '' : ` (${openPositions.length})`}
                </Text>
                {openPositions.map((pos) => (
                  <OpenCard key={pos.ticker} pos={pos} />
                ))}
              </>
            )}

            {/* Closed positions */}
            {closedPositions.length > 0 && (
              <>
                <Text style={styles.sectionHeader}>
                  Closed / replaced{filterTicker ? '' : ` (${closedPositions.length})`}
                </Text>
                {closedPositions.map((pos, idx) => (
                  <ClosedCard key={`${pos.ticker}-${idx}`} pos={pos} />
                ))}
              </>
            )}

            {openPositions.length === 0 && closedPositions.length === 0 && (
              <View style={styles.emptyState}>
                <Ionicons name="stats-chart-outline" size={40} color={COLORS.textMuted} />
                <Text style={styles.emptyText}>No trading history yet.</Text>
                <Text style={styles.emptySubtext}>
                  History will appear once your Tracklist is active.
                </Text>
              </View>
            )}
          </>
        )}
      </ScrollView>
    </SafeAreaView>
  );
}

// ─── styles ──────────────────────────────────────────────────────────────────

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.background },
  scrollContent: { paddingBottom: 40 },
  loader: { marginTop: 60 },

  summaryCard: {
    backgroundColor: COLORS.primary,
    borderRadius: 16,
    padding: 16,
    marginBottom: 20,
    ...Platform.select({
      web: { boxShadow: '0 2px 8px rgba(0,0,0,0.12)' },
      default: { elevation: 3 },
    }),
  },
  summaryTitle: { fontSize: 14, fontWeight: '700', color: 'rgba(255,255,255,0.7)', marginBottom: 10, textTransform: 'uppercase', letterSpacing: 0.5 },
  summaryRow: { flexDirection: 'row', justifyContent: 'space-between', paddingVertical: 5, borderBottomWidth: 1, borderBottomColor: 'rgba(255,255,255,0.1)' },
  summaryLabel: { fontSize: 14, color: 'rgba(255,255,255,0.8)' },
  summaryValue: { fontSize: 14, fontWeight: '700', color: '#FFF' },

  sectionHeader: {
    fontSize: 13,
    fontWeight: '700',
    color: COLORS.textMuted,
    textTransform: 'uppercase',
    letterSpacing: 0.5,
    marginBottom: 10,
    marginTop: 4,
  },

  tradeCard: {
    backgroundColor: COLORS.card,
    borderRadius: 14,
    marginBottom: 12,
    overflow: 'hidden',
    ...Platform.select({
      web: { boxShadow: '0 1px 4px rgba(0,0,0,0.07)' },
      default: { elevation: 1 },
    }),
  },
  tradeCardHeader: {
    paddingHorizontal: 14,
    paddingTop: 14,
    paddingBottom: 10,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  tradeTickerRow: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  tradeTicker: { fontSize: 17, fontWeight: '800', color: COLORS.text },
  tradeSubtext: { fontSize: 12, color: COLORS.textMuted, marginTop: 2 },

  closedPill: {
    paddingHorizontal: 7,
    paddingVertical: 2,
    borderRadius: 6,
    backgroundColor: '#FEE2E2',
  },
  closedPillText: { fontSize: 10, fontWeight: '700', color: '#DC2626', letterSpacing: 0.3 },
  openPill: {
    paddingHorizontal: 7,
    paddingVertical: 2,
    borderRadius: 6,
    backgroundColor: '#D1FAE5',
  },
  openPillText: { fontSize: 10, fontWeight: '700', color: '#059669', letterSpacing: 0.3 },

  tradeRow: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 14,
    paddingVertical: 10,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  tradeCell: { flex: 1 },
  tradeCellRight: { alignItems: 'flex-end' },
  tradeCellLabel: { fontSize: 10, color: COLORS.textMuted, textTransform: 'uppercase', letterSpacing: 0.3, marginBottom: 2 },
  tradeCellLabelRight: { textAlign: 'right' },
  tradeCellValue: { fontSize: 14, fontWeight: '600', color: COLORS.text },
  tradeCellValueRight: { textAlign: 'right' },
  tradeArrow: { marginHorizontal: 8 },

  plRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    paddingHorizontal: 14,
    paddingVertical: 12,
  },
  plRowPositive: { backgroundColor: '#F0FDF4' },
  plRowNegative: { backgroundColor: '#FFF1F2' },
  plLabel: { fontSize: 13, fontWeight: '700' },
  plValues: { flexDirection: 'row', alignItems: 'center', gap: 6 },
  plUsd: { fontSize: 15, fontWeight: '800' },
  plPct: { fontSize: 13, fontWeight: '600' },

  positive: { color: COLORS.positive },
  negative: { color: COLORS.negative },

  emptyState: { alignItems: 'center', paddingTop: 60, gap: 10 },
  emptyText: { fontSize: 16, fontWeight: '600', color: COLORS.text },
  emptySubtext: { fontSize: 13, color: COLORS.textMuted, textAlign: 'center' },
});
