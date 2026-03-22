/**
 * RICHSTOX Admin Panel
 * ====================
 * 3 tabs: Dashboard · Pipeline · Customers
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  View, Text, ScrollView, TouchableOpacity, StyleSheet,
  RefreshControl, ActivityIndicator, SafeAreaView,
} from 'react-native';
import { Ionicons } from '@expo/vector-icons';
import { useAuth } from '../../contexts/AuthContext';
import { COLORS } from '../_layout';
import AppHeader from '../../components/AppHeader';
import PipelineTab from '../admin/pipeline';
import CustomersTab from '../admin/customers';

const API_URL = process.env.EXPO_PUBLIC_BACKEND_URL || '';

type Tab = 'dashboard' | 'pipeline' | 'customers';

// ─── Types ────────────────────────────────────────────────────────────────────

interface CoverageCheckpoint {
  date?: string | null;
  have_price_count?: number;
  today_visible?: number;
  kind?: 'recent' | 'historical';
}

interface PriceIntegrity {
  today_visible?: number;
  today_visible_source?: {
    chain_run_id?: string | null;
    generated_at_prague?: string | null;
  } | null;
  last_bulk_trading_date?: string | null;
  needs_price_redownload?: number;
  price_history_incomplete?: number;
  full_price_history_count?: number;
  history_download_completed_count?: number;
  gap_free_since_history_download_count?: number;
  missing_expected_dates?: number;
  coverage_checkpoints?: Record<string, CoverageCheckpoint>;
}

interface PipelineAge {
  pipeline_hours_since_success?: number | null;
  pipeline_status?: string;
  morning_refresh_hours_since_success?: number | null;
  morning_refresh_status?: string;
}

interface OverviewData {
  health?: {
    scheduler_active?: boolean;
    jobs_total?: number;
    jobs_completed?: number;
    jobs_failed?: number;
  };
  jobs?: {
    overdue?: any[];
    failed?: any[];
    completed?: any[];
  };
  universe_funnel?: {
    counts?: {
      visible_tickers?: number;
    };
  };
  price_integrity?: PriceIntegrity;
  pipeline_age?: PipelineAge;
}

interface StatsData {
  users?: number;
  portfolios?: number;
  positions?: number;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function statusColor(status?: string): string {
  if (status === 'green') return '#22C55E';
  if (status === 'yellow') return '#F59E0B';
  if (status === 'red') return '#EF4444';
  return COLORS.textMuted;
}

function statusIcon(status?: string): string {
  if (status === 'green') return 'checkmark-circle';
  if (status === 'yellow') return 'warning';
  if (status === 'red') return 'close-circle';
  return 'help-circle';
}

function formatHours(h?: number | null): string {
  if (h == null) return '—';
  if (h < 1) return `${Math.round(h * 60)}m ago`;
  return `${h.toFixed(1)}h ago`;
}

// ─── Dashboard Tab ────────────────────────────────────────────────────────────

interface DashboardProps {
  sessionToken: string | null;
}

function DashboardTab({ sessionToken }: DashboardProps) {
  const [overview, setOverview] = useState<OverviewData | null>(null);
  const [stats, setStats] = useState<StatsData | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  const fetchAll = useCallback(async () => {
    try {
      const requestHeaders = sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {};
      const [ovRes, statsRes] = await Promise.allSettled([
        fetch(`${API_URL}/api/admin/overview`, { headers: requestHeaders }),
        fetch(`${API_URL}/api/admin/stats`, { headers: requestHeaders }),
      ]);
      if (ovRes.status === 'fulfilled' && ovRes.value.ok) setOverview(await ovRes.value.json());
      if (statsRes.status === 'fulfilled' && statsRes.value.ok) setStats(await statsRes.value.json());
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [sessionToken]);

  useEffect(() => { fetchAll(); }, [fetchAll]);
  const onRefresh = () => { setRefreshing(true); fetchAll(); };

  if (loading) return <View style={d.center}><ActivityIndicator size="large" color={COLORS.primary} /></View>;

  const health = overview?.health;
  const failedCount = health?.jobs_failed ?? 0;
  const schedulerActive = health?.scheduler_active;
  const pAge = overview?.pipeline_age;
  const pi = overview?.price_integrity;
  const cp = pi?.coverage_checkpoints || {};

  // Build alerts
  const alerts: { color: string; icon: string; text: string }[] = [];
  if (failedCount > 0) alerts.push({ color: '#EF4444', icon: 'close-circle', text: `${failedCount} pipeline job${failedCount > 1 ? 's' : ''} failed` });
  if (schedulerActive === false) alerts.push({ color: '#EF4444', icon: 'pause-circle', text: 'Scheduler is paused' });
  if ((pi?.today_visible ?? 0) === 0) alerts.push({ color: '#EF4444', icon: 'eye-off', text: '0 visible tickers — universe not seeded' });
  if (pi && (pi.missing_expected_dates ?? 0) > 0) alerts.push({ color: '#F59E0B', icon: 'alert-circle', text: `${pi.missing_expected_dates} date(s) with incomplete price coverage` });
  if (pi && (pi.needs_price_redownload ?? 0) > 0) alerts.push({ color: '#F59E0B', icon: 'refresh-circle', text: `${pi.needs_price_redownload} ticker(s) need price re-download` });

  // Process-truth metrics for Historical Depth / Price Integrity section
  const hdcCount = pi?.history_download_completed_count ?? 0;
  const gfCount = pi?.gap_free_since_history_download_count ?? 0;
  const tvTotal = pi?.today_visible ?? 0;
  const hdcPct = tvTotal > 0 ? Math.round((hdcCount / tvTotal) * 100) : 0;
  const gfPct = tvTotal > 0 ? Math.round((gfCount / tvTotal) * 100) : 0;
  const hdcValue = tvTotal > 0 ? `${hdcCount}/${tvTotal} (${hdcPct}%)` : `${hdcCount}/${tvTotal}`;
  const gfValue = tvTotal > 0 ? `${gfCount}/${tvTotal} (${gfPct}%)` : `${gfCount}/${tvTotal}`;

  // Legacy heuristic depth (secondary informational)
  const fphCount = pi?.full_price_history_count ?? 0;
  const fphPct = tvTotal > 0 ? Math.round((fphCount / tvTotal) * 100) : 0;
  const fphValue = tvTotal > 0 ? `${fphCount}/${tvTotal} (${fphPct}%)` : `${fphCount}/${tvTotal}`;

  // Coverage checkpoint helper
  const renderCheckpoint = (label: string, key: string) => {
    const c = cp[key];
    if (!c) return null;
    const have = c.have_price_count ?? 0;
    const total = c.today_visible ?? 0;
    const pct = total > 0 ? Math.round((have / total) * 100) : 0;
    const isHistorical = c.kind === 'historical';
    const isCoverageGap = total > 0 && have < total;
    // Historical checkpoints: use neutral blue for incomplete depth (not amber warning)
    const dotColor = !isCoverageGap ? '#22C55E' : isHistorical ? '#60A5FA' : '#F59E0B';
    const textColor = !isCoverageGap ? undefined : isHistorical ? { color: '#60A5FA' } : { color: '#F59E0B' };
    return (
      <View key={key} style={d.cpRow}>
        <View style={[d.cpDot, { backgroundColor: dotColor }]} />
        <Text style={d.cpLabel}>{label}</Text>
        <Text style={d.cpDate}>{c.date ?? '—'}</Text>
        <Text style={[d.cpValue, textColor]}>{have}/{total} ({pct}%)</Text>
      </View>
    );
  };

  return (
    <ScrollView
      style={d.container}
      refreshControl={<RefreshControl refreshing={refreshing} onRefresh={onRefresh} tintColor={COLORS.primary} />}
    >
      {/* Alerts */}
      {alerts.length > 0 && (
        <View style={d.alertsCard}>
          {alerts.map((a, i) => (
            <View key={i} style={d.alertRow}>
              <Ionicons name={a.icon as any} size={14} color={a.color} />
              <Text style={[d.alertText, { color: a.color }]}>{a.text}</Text>
            </View>
          ))}
        </View>
      )}

      {/* A) Business (compact) */}
      <View style={d.card}>
        <Text style={d.sectionTitle}>Business</Text>
        <View style={d.bizRow}>
          <BizStat label="Users" value={String(stats?.users ?? 0)} icon="people" />
          <BizStat label="Portfolios" value={String(stats?.portfolios ?? 0)} icon="briefcase" />
          <BizStat label="Positions" value={String(stats?.positions ?? 0)} icon="layers" />
        </View>
      </View>

      {/* B) Ops Health (compact) */}
      <View style={d.card}>
        <Text style={d.sectionTitle}>Ops Health</Text>
        <View style={d.opsGrid}>
          <OpsItem
            label="Pipeline (1–3)"
            value={formatHours(pAge?.pipeline_hours_since_success)}
            status={pAge?.pipeline_status}
          />
          <OpsItem
            label="Morning Refresh"
            value={formatHours(pAge?.morning_refresh_hours_since_success)}
            status={pAge?.morning_refresh_status}
          />
          <OpsItem
            label="Scheduler"
            value={schedulerActive ? 'Running' : 'Paused'}
            status={schedulerActive ? 'green' : 'red'}
          />
          <OpsItem
            label="Failed Jobs"
            value={String(failedCount)}
            status={failedCount > 0 ? 'red' : 'green'}
          />
        </View>
      </View>

      {/* C) Price Integrity / Coverage */}
      <View style={d.card}>
        <Text style={d.sectionTitle}>Price Integrity / Coverage</Text>

        {/* Key metrics row */}
        <View style={d.integrityGrid}>
          <IntegrityMetric
            label="Last Bulk Date"
            value={pi?.last_bulk_trading_date ?? '—'}
            warn={!pi?.last_bulk_trading_date}
          />
          <IntegrityMetric
            label="Missing Bulk Dates"
            value={String(pi?.missing_expected_dates ?? 0)}
            warn={(pi?.missing_expected_dates ?? 0) > 0}
          />
          <IntegrityMetric
            label="Need Re-download"
            value={String(pi?.needs_price_redownload ?? 0)}
            warn={(pi?.needs_price_redownload ?? 0) > 0}
          />
          <IntegrityMetric
            label="Incomplete History (remediation)"
            value={String(pi?.price_history_incomplete ?? 0)}
            warn={(pi?.price_history_incomplete ?? 0) > 0}
          />
        </View>

        {/* Recent bulk coverage */}
        <Text style={d.subSection}>
          Recent Bulk Coverage ({pi?.today_visible ?? 0} visible
          {pi?.today_visible_source?.chain_run_id ? ` · run ${pi.today_visible_source.chain_run_id.slice(-8)}` : ''})
        </Text>
        <Text style={d.cpHint}>Tickers with price data on recently ingested trading dates</Text>
        {renderCheckpoint('Latest trading day', 'latest_trading_day')}
        {renderCheckpoint('1 week ago', '1_week_ago')}

        {/* Historical / Price Integrity truth */}
        <Text style={[d.subSection, { marginTop: 12 }]}>Price Completeness (process truth)</Text>
        <Text style={d.cpHint}>
          Proven historical download + no missing bulk dates since download
        </Text>
        <IntegrityMetric
          label="History Download Completed"
          value={hdcValue}
          warn={false}
        />
        <IntegrityMetric
          label="Gap-Free Since Download"
          value={gfValue}
          warn={false}
        />

        {/* Historical coverage depth (secondary informational) */}
        <Text style={[d.subSection, { marginTop: 12 }]}>Historical Depth (heuristic)</Text>
        <Text style={d.cpHint}>
          Heuristic depth indicator (≥252 rows, min date ≥1yr ago) — not the canonical truth
        </Text>
        <IntegrityMetric
          label="Full Price History (heuristic)"
          value={fphValue}
          warn={false}
        />
        {renderCheckpoint('1 month ago', '1_month_ago')}
        {renderCheckpoint('1 year ago', '1_year_ago')}
      </View>

      <View style={{ height: 40 }} />
    </ScrollView>
  );
}

function BizStat({ label, value, icon }: { label: string; value: string; icon: string }) {
  return (
    <View style={d.bizStat}>
      <Ionicons name={icon as any} size={16} color={COLORS.primary} />
      <Text style={d.bizValue}>{value}</Text>
      <Text style={d.bizLabel}>{label}</Text>
    </View>
  );
}

function OpsItem({ label, value, status }: { label: string; value: string; status?: string }) {
  return (
    <View style={d.opsItem}>
      <Ionicons name={statusIcon(status) as any} size={14} color={statusColor(status)} />
      <Text style={d.opsLabel}>{label}</Text>
      <Text style={[d.opsValue, { color: statusColor(status) }]}>{value}</Text>
    </View>
  );
}

function IntegrityMetric({ label, value, warn }: { label: string; value: string; warn?: boolean }) {
  return (
    <View style={d.intMetric}>
      <Text style={[d.intValue, warn && { color: '#F59E0B' }]}>{value}</Text>
      <Text style={d.intLabel}>{label}</Text>
    </View>
  );
}

const d = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.background },
  center: { flex: 1, alignItems: 'center', justifyContent: 'center' },

  alertsCard: { margin: 12, marginBottom: 0, backgroundColor: '#EF444411', borderRadius: 10, padding: 12, borderWidth: 1, borderColor: '#EF444433' },
  alertRow: { flexDirection: 'row', alignItems: 'center', gap: 6, marginBottom: 4 },
  alertText: { fontSize: 12, fontWeight: '500', flex: 1 },

  card: { margin: 12, marginBottom: 0, backgroundColor: COLORS.card, borderRadius: 10, padding: 14, borderWidth: 1, borderColor: COLORS.border },
  sectionTitle: { fontSize: 12, fontWeight: '700', color: COLORS.textMuted, letterSpacing: 0.6, marginBottom: 10, textTransform: 'uppercase' },

  // A) Business
  bizRow: { flexDirection: 'row', justifyContent: 'space-around' },
  bizStat: { alignItems: 'center', gap: 2 },
  bizValue: { fontSize: 18, fontWeight: '800', color: COLORS.text },
  bizLabel: { fontSize: 10, color: COLORS.textMuted },

  // B) Ops Health
  opsGrid: { flexDirection: 'row', flexWrap: 'wrap', gap: 8 },
  opsItem: { width: '47%', flexDirection: 'row', alignItems: 'center', gap: 6, backgroundColor: COLORS.background, borderRadius: 8, padding: 10, borderWidth: 1, borderColor: COLORS.border },
  opsLabel: { flex: 1, fontSize: 11, color: COLORS.text },
  opsValue: { fontSize: 11, fontWeight: '700' },

  // C) Price Integrity
  integrityGrid: { flexDirection: 'row', flexWrap: 'wrap', gap: 8, marginBottom: 12 },
  intMetric: { width: '47%', backgroundColor: COLORS.background, borderRadius: 8, padding: 10, borderWidth: 1, borderColor: COLORS.border, alignItems: 'center' },
  intValue: { fontSize: 16, fontWeight: '800', color: COLORS.text },
  intLabel: { fontSize: 9, color: COLORS.textMuted, textAlign: 'center', marginTop: 2 },

  subSection: { fontSize: 11, fontWeight: '600', color: COLORS.textMuted, marginBottom: 4 },
  cpHint: { fontSize: 9, color: COLORS.textMuted, marginBottom: 8, fontStyle: 'italic' },

  // Coverage checkpoints
  cpRow: { flexDirection: 'row', alignItems: 'center', paddingVertical: 5, borderBottomWidth: 1, borderBottomColor: COLORS.border + '55' },
  cpDot: { width: 6, height: 6, borderRadius: 3, marginRight: 8 },
  cpLabel: { fontSize: 11, color: COLORS.text, width: 110 },
  cpDate: { fontSize: 10, color: COLORS.textMuted, flex: 1 },
  cpValue: { fontSize: 11, fontWeight: '600', color: '#22C55E' },
});

// ─── Main Admin Screen ────────────────────────────────────────────────────────

export default function AdminScreen() {
  const { isAdmin, sessionToken, isLoading } = useAuth();
  const [activeTab, setActiveTab] = useState<Tab>('dashboard');

  if (isLoading) {
    return (
      <SafeAreaView style={a.container}>
        <View style={a.center}><ActivityIndicator size="large" color={COLORS.primary} /></View>
      </SafeAreaView>
    );
  }

  if (!isAdmin) {
    return (
      <SafeAreaView style={a.container}>
        <View style={a.center}>
          <Ionicons name="shield-outline" size={48} color={COLORS.textMuted} />
          <Text style={a.accessTitle}>Admin Access Required</Text>
          <Text style={a.accessSub}>This area is restricted to administrators.</Text>
        </View>
      </SafeAreaView>
    );
  }

  const tabs: { id: Tab; label: string; icon: string }[] = [
    { id: 'dashboard', label: 'Dashboard', icon: 'grid-outline' },
    { id: 'pipeline', label: 'Pipeline', icon: 'git-network-outline' },
    { id: 'customers', label: 'Customers', icon: 'people-outline' },
  ];

  return (
    <SafeAreaView style={a.container}>
      <AppHeader title="Admin Panel" />

      {/* Tabs */}
      <View style={a.tabBar}>
        {tabs.map(tab => (
          <TouchableOpacity
            key={tab.id}
            style={[a.tabBtn, activeTab === tab.id && a.tabBtnActive]}
            onPress={() => setActiveTab(tab.id)}
          >
            <Ionicons
              name={tab.icon as any}
              size={15}
              color={activeTab === tab.id ? COLORS.primary : COLORS.textMuted}
            />
            <Text style={[a.tabLabel, activeTab === tab.id && a.tabLabelActive]}>
              {tab.label}
            </Text>
          </TouchableOpacity>
        ))}
      </View>

      {/* Tab Content */}
      {activeTab === 'dashboard' && <DashboardTab sessionToken={sessionToken} />}
      {activeTab === 'pipeline' && <PipelineTab sessionToken={sessionToken} />}
      {activeTab === 'customers' && <CustomersTab sessionToken={sessionToken} />}
    </SafeAreaView>
  );
}

const a = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.background },
  center: { flex: 1, alignItems: 'center', justifyContent: 'center', gap: 8 },

  header: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', paddingHorizontal: 14, paddingVertical: 10, borderBottomWidth: 1, borderBottomColor: COLORS.border },
  headerLeft: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  headerTitle: { fontSize: 16, fontWeight: '700', color: COLORS.text },
  adminBadge: { backgroundColor: COLORS.primary, paddingHorizontal: 6, paddingVertical: 2, borderRadius: 4 },
  adminBadgeText: { fontSize: 9, fontWeight: '800', color: '#fff', letterSpacing: 0.5 },
  headerEmail: { fontSize: 11, color: COLORS.textMuted },

  tabBar: { flexDirection: 'row', borderBottomWidth: 1, borderBottomColor: COLORS.border, backgroundColor: COLORS.card },
  tabBtn: { flex: 1, flexDirection: 'row', alignItems: 'center', justifyContent: 'center', gap: 5, paddingVertical: 10, borderBottomWidth: 2, borderBottomColor: 'transparent' },
  tabBtnActive: { borderBottomColor: COLORS.primary },
  tabLabel: { fontSize: 12, color: COLORS.textMuted, fontWeight: '500' },
  tabLabelActive: { color: COLORS.primary, fontWeight: '700' },

  accessTitle: { fontSize: 16, fontWeight: '700', color: COLORS.text },
  accessSub: { fontSize: 13, color: COLORS.textMuted },
});
