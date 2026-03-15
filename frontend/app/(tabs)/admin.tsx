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

interface JobRun {
  status?: string;
  start_time?: string;
  duration_seconds?: number;
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
  job_last_runs?: Record<string, JobRun>;
  universe_funnel?: {
    counts?: {
      seeded_us_total?: number;
      with_price_data?: number;
      with_classification?: number;
      visible_tickers?: number;
    };
  };
}

interface StatsData {
  users?: number;
  portfolios?: number;
  visible_tickers?: number;
  fundamentals?: number;
  news_articles?: number;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function formatTime(iso?: string): string {
  if (!iso) return 'Never';
  try {
    const d = new Date(iso);
    return `${d.toLocaleString('en-GB', {
      timeZone: 'Europe/Prague',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    })} Prague`;
  } catch { return iso; }
}

const PIPELINE_JOB_NAMES = ['universe_seed', 'price_sync', 'fundamentals_sync', 'peer_medians'];

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

  const jobRuns = overview?.job_last_runs || {};
  const completedSteps = PIPELINE_JOB_NAMES.filter(j => {
    const s = jobRuns[j]?.status;
    return s === 'success' || s === 'completed';
  }).length;
  const healthPct = Math.round((completedSteps / PIPELINE_JOB_NAMES.length) * 100);
  const healthColor = healthPct === 100 ? '#22C55E' : healthPct >= 60 ? '#F59E0B' : '#EF4444';

  const overdueCount = overview?.jobs?.overdue?.length ?? 0;
  const failedCount = overview?.jobs?.failed?.length ?? 0;
  const visibleCount = overview?.universe_funnel?.counts?.visible_tickers ?? 0;
  const schedulerActive = overview?.health?.scheduler_active;

  // Build alerts
  const alerts: { color: string; icon: string; text: string }[] = [];
  if (failedCount > 0) alerts.push({ color: '#EF4444', icon: 'close-circle', text: `${failedCount} pipeline job${failedCount > 1 ? 's' : ''} failed` });
  if (overdueCount > 0) alerts.push({ color: '#F59E0B', icon: 'warning', text: `${overdueCount} job${overdueCount > 1 ? 's' : ''} overdue` });
  if (visibleCount === 0) alerts.push({ color: '#EF4444', icon: 'eye-off', text: '0 visible tickers — universe not seeded' });
  if (schedulerActive === false) alerts.push({ color: '#EF4444', icon: 'pause-circle', text: 'Scheduler is paused' });

  // Pipeline step summaries
  const stepSummaries = [
    { name: 'universe_seed', label: 'Universe Seed', icon: 'globe-outline', color: '#6366F1' },
    { name: 'price_sync', label: 'Price Sync', icon: 'trending-up-outline', color: '#10B981' },
    { name: 'fundamentals_sync', label: 'Fundamentals', icon: 'library-outline', color: '#F59E0B' },
    { name: 'compute_visible_universe', label: 'Visible Universe', icon: 'eye-outline', color: '#8B5CF6' },
    { name: 'peer_medians', label: 'Peer Medians', icon: 'stats-chart-outline', color: '#EC4899' },
  ];

  if (loading) return <View style={d.center}><ActivityIndicator size="large" color={COLORS.primary} /></View>;

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

      {/* System Health */}
      <View style={d.card}>
        <Text style={d.sectionTitle}>System Health</Text>
        <View style={d.healthRow}>
          <HealthPill
            label="Pipeline"
            value={`${healthPct}%`}
            color={healthColor}
            icon={healthPct === 100 ? 'checkmark-circle' : 'warning'}
          />
          <HealthPill
            label="Scheduler"
            value={schedulerActive ? 'Active' : 'Paused'}
            color={schedulerActive ? '#22C55E' : '#EF4444'}
            icon={schedulerActive ? 'play-circle' : 'pause-circle'}
          />
          <HealthPill
            label="DB"
            value="Online"
            color="#22C55E"
            icon="server"
          />
          <HealthPill
            label="Failed Jobs"
            value={String(failedCount)}
            color={failedCount > 0 ? '#EF4444' : '#22C55E'}
            icon={failedCount > 0 ? 'close-circle' : 'checkmark-circle'}
          />
        </View>
        {/* Pipeline Progress */}
        <View style={d.progressWrap}>
          <View style={d.progressBg}>
            <View style={[d.progressFill, { width: `${healthPct}%` as any, backgroundColor: healthColor }]} />
          </View>
          <Text style={d.progressLabel}>{completedSteps}/5 steps completed today</Text>
        </View>
      </View>

      {/* Key Numbers */}
      <View style={d.card}>
        <Text style={d.sectionTitle}>Key Numbers</Text>
        <View style={d.statsGrid}>
          <StatCard label="Visible Tickers" value={(visibleCount || stats?.visible_tickers || 0).toLocaleString()} icon="list" color="#6366F1" />
          <StatCard label="Total Users" value={String(stats?.users ?? 0)} icon="people" color="#10B981" />
          <StatCard label="PRO Users" value="—" icon="star" color="#F59E0B" />
          <StatCard label="Portfolios" value={String(stats?.portfolios ?? 0)} icon="briefcase" color="#8B5CF6" />
          <StatCard label="News Articles" value={(stats?.news_articles ?? 0).toLocaleString()} icon="newspaper" color="#06B6D4" />
          <StatCard label="Fundamentals" value={(stats?.fundamentals ?? 0).toLocaleString()} icon="library" color="#EC4899" />
        </View>
      </View>

      {/* Today's Pipeline */}
      <View style={d.card}>
        <Text style={d.sectionTitle}>Today&apos;s Pipeline</Text>
        {stepSummaries.map((step, i) => {
          const run = jobRuns[step.name];
          const status = run?.status;
          const ok = status === 'success' || status === 'completed';
          const failed = status === 'failed' || status === 'error';
          return (
            <View key={step.name} style={d.pipelineRow}>
              <View style={[d.pipelineDot, { backgroundColor: ok ? '#22C55E' : failed ? '#EF4444' : COLORS.border }]} />
              <Ionicons name={step.icon as any} size={13} color={step.color} style={{ marginRight: 6 }} />
              <Text style={d.pipelineLabel}>{step.label}</Text>
              {run ? (
                <Text style={[d.pipelineStatus, { color: ok ? '#22C55E' : failed ? '#EF4444' : '#F59E0B' }]}>
                  {ok ? `✓ ${formatTime(run.start_time)}` : failed ? '✗ Failed' : status}
                </Text>
              ) : (
                <Text style={d.pipelineNever}>Never run</Text>
              )}
            </View>
          );
        })}
      </View>

      {/* API Usage */}
      <View style={d.card}>
        <Text style={d.sectionTitle}>API Usage</Text>
        <View style={d.apiRow}>
          <Ionicons name="information-circle-outline" size={13} color={COLORS.textMuted} />
          <Text style={d.apiNote}>
            API call tracking is in-memory only — counter resets on Railway restart.
            Persistent tracking coming soon.
          </Text>
        </View>
        <View style={d.apiItems}>
          <View style={d.apiItem}><Text style={d.apiLabel}>Universe Seed</Text><Text style={d.apiValue}>2 calls/day</Text></View>
          <View style={d.apiItem}><Text style={d.apiLabel}>Price Sync</Text><Text style={d.apiValue}>1 call/day</Text></View>
          <View style={d.apiItem}><Text style={d.apiLabel}>Fundamentals</Text><Text style={d.apiValue}>~10 calls/ticker</Text></View>
          <View style={d.apiItem}><Text style={d.apiLabel}>Morning Fresh</Text><Text style={d.apiValue}>1 call/followed ticker</Text></View>
        </View>
      </View>

      <View style={{ height: 40 }} />
    </ScrollView>
  );
}

function HealthPill({ label, value, color, icon }: { label: string; value: string; color: string; icon: string }) {
  return (
    <View style={d.healthPill}>
      <Ionicons name={icon as any} size={14} color={color} />
      <Text style={[d.healthPillValue, { color }]}>{value}</Text>
      <Text style={d.healthPillLabel}>{label}</Text>
    </View>
  );
}

function StatCard({ label, value, icon, color }: { label: string; value: string; icon: string; color: string }) {
  return (
    <View style={d.statCard}>
      <View style={[d.statIcon, { backgroundColor: color + '18' }]}>
        <Ionicons name={icon as any} size={14} color={color} />
      </View>
      <Text style={d.statValue}>{value}</Text>
      <Text style={d.statLabel}>{label}</Text>
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
  sectionTitle: { fontSize: 12, fontWeight: '700', color: COLORS.textMuted, letterSpacing: 0.6, marginBottom: 12, textTransform: 'uppercase' },

  healthRow: { flexDirection: 'row', gap: 8, marginBottom: 12 },
  healthPill: { flex: 1, alignItems: 'center', gap: 2, backgroundColor: COLORS.background, borderRadius: 8, paddingVertical: 8, borderWidth: 1, borderColor: COLORS.border },
  healthPillValue: { fontSize: 12, fontWeight: '700' },
  healthPillLabel: { fontSize: 9, color: COLORS.textMuted },

  progressWrap: { gap: 4 },
  progressBg: { height: 4, backgroundColor: COLORS.border, borderRadius: 2, overflow: 'hidden' },
  progressFill: { height: 4, borderRadius: 2 },
  progressLabel: { fontSize: 10, color: COLORS.textMuted },

  statsGrid: { flexDirection: 'row', flexWrap: 'wrap', gap: 8 },
  statCard: { width: '30.5%', backgroundColor: COLORS.background, borderRadius: 8, padding: 10, borderWidth: 1, borderColor: COLORS.border, alignItems: 'center', gap: 4 },
  statIcon: { width: 28, height: 28, borderRadius: 8, alignItems: 'center', justifyContent: 'center' },
  statValue: { fontSize: 16, fontWeight: '800', color: COLORS.text },
  statLabel: { fontSize: 9, color: COLORS.textMuted, textAlign: 'center' },

  pipelineRow: { flexDirection: 'row', alignItems: 'center', paddingVertical: 6, borderBottomWidth: 1, borderBottomColor: COLORS.border + '55' },
  pipelineDot: { width: 8, height: 8, borderRadius: 4, marginRight: 8 },
  pipelineLabel: { fontSize: 12, color: COLORS.text, flex: 1 },
  pipelineStatus: { fontSize: 11, fontWeight: '500' },
  pipelineNever: { fontSize: 11, color: COLORS.textMuted, fontStyle: 'italic' },

  apiRow: { flexDirection: 'row', alignItems: 'flex-start', gap: 6, marginBottom: 10 },
  apiNote: { fontSize: 11, color: COLORS.textMuted, flex: 1, lineHeight: 16 },
  apiItems: { gap: 6 },
  apiItem: { flexDirection: 'row', justifyContent: 'space-between' },
  apiLabel: { fontSize: 12, color: COLORS.text },
  apiValue: { fontSize: 12, color: COLORS.textMuted },
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
