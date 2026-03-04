/**
 * RICHSTOX Admin Pipeline
 * Universe Pipeline — 5-step sequential process
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  View, Text, ScrollView, TouchableOpacity, StyleSheet,
  RefreshControl, ActivityIndicator,
} from 'react-native';
import { Ionicons } from '@expo/vector-icons';
import { COLORS } from '../_layout';

const API_URL = process.env.EXPO_PUBLIC_BACKEND_URL || '';

interface PipelineProps {
  sessionToken: string | null;
}

interface JobRun {
  status: string;
  start_time?: string;
  end_time?: string;
  duration_seconds?: number;
  records_processed?: number;
  error_message?: string;
  triggered_by?: string;
}

interface OverviewData {
  job_last_runs?: Record<string, JobRun>;
  jobs?: {
    registry?: any[];
    overdue?: any[];
    completed?: any[];
    not_scheduled?: any[];
  };
  universe_funnel?: {
    counts?: {
      seeded_us_total?: number;
      with_price_data?: number;
      with_classification?: number;
      visible_tickers?: number;
    };
  };
}

const PIPELINE_STEPS = [
  {
    step: 1,
    job_name: 'universe_seed',
    title: 'Universe Seed',
    schedule: 'Daily 23:00 Prague',
    description: 'Fetch NYSE + NASDAQ, filter Common Stock USD, upsert to tracked_tickers',
    apis: [
      'eodhd.com/api/exchange-symbol-list/NYSE',
      'eodhd.com/api/exchange-symbol-list/NASDAQ',
    ],
    filters: ['Type = "Common Stock"', 'Currency = "USD"', 'Exclude: warrants, units, preferred, rights'],
    result_key: 'added_pending',
    result_label: 'seeded',
    icon: 'globe-outline' as const,
    color: '#6366F1',
    scheduledHour: 23,
    scheduledMinute: 0,
  },
  {
    step: 2,
    job_name: 'price_sync',
    title: 'Price Sync',
    schedule: 'Daily 23:00 Prague',
    description: 'Fetch bulk prices, set has_price_data flag',
    apis: ['eodhd.com/api/eod-bulk-last-day/US'],
    filters: ['Has price data in EODHD bulk response'],
    result_key: 'processed',
    result_label: 'tickers with prices',
    icon: 'trending-up-outline' as const,
    color: '#10B981',
    scheduledHour: 23,
    scheduledMinute: 0,
  },
  {
    step: 3,
    job_name: 'fundamentals_sync',
    title: 'Fundamentals Sync',
    schedule: 'Daily 23:30 Prague',
    description: 'Fetch fundamentals per ticker, set classification',
    apis: ['eodhd.com/api/fundamentals/{TICKER}.US (~10 credits/ticker)'],
    filters: ['Sector present', 'Industry present'],
    result_key: 'processed',
    result_label: 'tickers classified',
    icon: 'library-outline' as const,
    color: '#F59E0B',
    scheduledHour: 23,
    scheduledMinute: 30,
  },
  {
    step: 4,
    job_name: 'compute_visible_universe',
    title: 'Compute Visible Universe',
    schedule: 'Daily 23:30 Prague',
    description: 'Apply visibility sieve, set is_visible flag',
    apis: ['Local DB only — no external API'],
    filters: ['is_delisted ≠ true', 'shares_outstanding > 0', 'financial_currency present'],
    result_key: 'visible_count',
    result_label: 'tickers visible',
    icon: 'eye-outline' as const,
    color: '#8B5CF6',
    scheduledHour: 23,
    scheduledMinute: 30,
  },
  {
    step: 5,
    job_name: 'peer_medians',
    title: 'Peer Medians',
    schedule: 'Daily 23:45 Prague',
    description: 'Compute PE, PS, PB, EV/EBITDA medians by sector & industry',
    apis: ['Local DB only — no external API'],
    filters: ['USD-reporting tickers', 'Exclude self from peer group', 'Winsorize outliers (1-99%)'],
    result_key: 'tickers_processed',
    result_label: 'tickers processed',
    icon: 'stats-chart-outline' as const,
    color: '#EC4899',
    scheduledHour: 23,
    scheduledMinute: 45,
  },
];

const MORNING_FRESH = {
  job_name: 'news_refresh',
  title: 'Morning Fresh',
  schedule: 'Daily 13:00 Prague',
  description: 'Fetch news + compute sentiment for tracked tickers',
  apis: ['eodhd.com/api/news (per tracked ticker)'],
  icon: 'newspaper-outline' as const,
  color: '#06B6D4',
};

function getNextRun(hour: number, minute: number): string {
  try {
    const now = new Date();
    const pragueNow = new Date(now.toLocaleString('en-US', { timeZone: 'Europe/Prague' }));
    const nextRun = new Date(pragueNow);
    nextRun.setHours(hour, minute, 0, 0);
    if (nextRun <= pragueNow) nextRun.setDate(nextRun.getDate() + 1);
    return nextRun.toLocaleString('en-GB', {
      timeZone: 'Europe/Prague', month: 'short', day: 'numeric',
      hour: '2-digit', minute: '2-digit'
    }) + ' Prague';
  } catch { return '—'; }
}

function formatDuration(sec?: number): string {
  if (!sec) return '';
  if (sec < 60) return `${Math.round(sec)}s`;
  return `${Math.floor(sec / 60)}m ${Math.round(sec % 60)}s`;
}

function formatTime(iso?: string): string {
  if (!iso) return 'Never';
  try {
    const d = new Date(iso);
    return d.toLocaleString('en-GB', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
  } catch { return iso; }
}

function getStatusColor(status?: string): string {
  if (!status) return COLORS.textMuted;
  if (status === 'success' || status === 'completed') return '#22C55E';
  if (status === 'failed' || status === 'error') return '#EF4444';
  return '#F59E0B';
}

function getStatusIcon(status?: string): string {
  if (status === 'success' || status === 'completed') return 'checkmark-circle';
  if (status === 'failed' || status === 'error') return 'close-circle';
  return 'time-outline';
}

export default function PipelineTab({ sessionToken }: PipelineProps) {
  const [data, setData] = useState<OverviewData | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [runningJob, setRunningJob] = useState<string | null>(null);
  const [runResult, setRunResult] = useState<Record<string, string>>({});
  const [expandedSteps, setExpandedSteps] = useState<Set<number>>(new Set());

  const headers = sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {};

  const fetchData = useCallback(async () => {
    try {
      const res = await fetch(`${API_URL}/api/admin/overview`, { headers });
      if (res.ok) setData(await res.json());
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [sessionToken]);

  useEffect(() => { fetchData(); }, [fetchData]);

  const onRefresh = () => { setRefreshing(true); fetchData(); };

  const handleRunNow = async (jobName: string) => {
    if (runningJob) return;
    setRunningJob(jobName);
    setRunResult(prev => ({ ...prev, [jobName]: '' }));
    try {
      // Jobs that support sync mode (?wait=true) — no polling needed
      const SYNC_JOBS = ['price_sync', 'fundamentals_sync', 'news_refresh'];
      const useWait = SYNC_JOBS.includes(jobName);
      let endpoint = jobName === 'universe_seed'
        ? `${API_URL}/api/admin/jobs/universe-seed`
        : `${API_URL}/api/admin/scheduler/run/${jobName.replace(/_/g, '-')}`;
      if (useWait) endpoint += '?wait=true';
      const res = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...headers },
      });
      const json = await res.json();
      if (res.ok) {
        setRunResult(prev => ({ ...prev, [jobName]: `✅ ${json.status || 'Triggered'}` }));
        setTimeout(fetchData, 2000);
      } else {
        setRunResult(prev => ({ ...prev, [jobName]: `❌ ${json.error || res.statusText}` }));
      }
    } catch (e: any) {
      setRunResult(prev => ({ ...prev, [jobName]: `❌ ${e.message}` }));
    } finally {
      setRunningJob(null);
    }
  };

  const toggleExpand = (step: number) => {
    setExpandedSteps(prev => {
      const next = new Set(prev);
      next.has(step) ? next.delete(step) : next.add(step);
      return next;
    });
  };

  const jobRuns = data?.job_last_runs || {};
  const counts = data?.universe_funnel?.counts || {};

  const completedSteps = PIPELINE_STEPS.filter(s => {
    const run = jobRuns[s.job_name];
    return run?.status === 'success' || run?.status === 'completed';
  }).length;

  const healthPct = Math.round((completedSteps / PIPELINE_STEPS.length) * 100);
  const healthColor = healthPct === 100 ? '#22C55E' : healthPct >= 60 ? '#F59E0B' : '#EF4444';

  const funnelSteps = [
    { label: 'Seeded (NYSE+NASDAQ Common Stock)', count: counts.seeded_us_total, total: counts.seeded_us_total },
    { label: 'With Price Data', count: counts.with_price_data, total: counts.seeded_us_total },
    { label: 'With Classification', count: counts.with_classification, total: counts.seeded_us_total },
    { label: 'Visible Universe', count: counts.visible_tickers, total: counts.seeded_us_total },
  ];

  if (loading) return (
    <View style={s.center}><ActivityIndicator size="large" color={COLORS.primary} /></View>
  );

  return (
    <ScrollView
      style={s.container}
      refreshControl={<RefreshControl refreshing={refreshing} onRefresh={onRefresh} tintColor={COLORS.primary} />}
    >
      {/* Health Bar */}
      <View style={s.healthCard}>
        <View style={s.healthHeader}>
          <Text style={s.healthTitle}>Universe Pipeline</Text>
          <Text style={[s.healthPct, { color: healthColor }]}>{healthPct}%</Text>
          <Ionicons
            name={healthPct === 100 ? 'checkmark-circle' : healthPct >= 60 ? 'warning' : 'close-circle'}
            size={18} color={healthColor}
          />
        </View>
        <View style={s.progressBg}>
          <View style={[s.progressFill, { width: `${healthPct}%` as any, backgroundColor: healthColor }]} />
        </View>
        <Text style={s.healthSub}>{completedSteps}/{PIPELINE_STEPS.length} steps completed today</Text>
      </View>

      {/* Pipeline Steps */}
      {PIPELINE_STEPS.map((step, idx) => {
        const run = jobRuns[step.job_name];
        const status = run?.status;
        const isExpanded = expandedSteps.has(step.step);
        const isRunning = runningJob === step.job_name;

        return (
          <View key={step.job_name}>
            <View style={s.stepCard}>
              {/* Step Header */}
              <View style={s.stepHeader}>
                <View style={[s.stepBadge, { backgroundColor: step.color + '22' }]}>
                  <Ionicons name={step.icon} size={16} color={step.color} />
                </View>
                <View style={s.stepMeta}>
                  <View style={s.stepTitleRow}>
                    <Text style={s.stepNum}>STEP {step.step}</Text>
                    <Text style={s.stepTitle}>{step.title}</Text>
                    {status && (
                      <Ionicons
                        name={getStatusIcon(status) as any}
                        size={14} color={getStatusColor(status)}
                        style={{ marginLeft: 4 }}
                      />
                    )}
                  </View>
                  <Text style={s.stepSchedule}>{step.schedule}</Text>
                </View>
                <TouchableOpacity
                  style={[s.runBtn, isRunning && s.runBtnDisabled]}
                  onPress={() => handleRunNow(step.job_name)}
                  disabled={!!runningJob}
                >
                  {isRunning
                    ? <ActivityIndicator size="small" color="#fff" />
                    : <Text style={s.runBtnText}>▶ Run</Text>
                  }
                </TouchableOpacity>
              </View>

              {/* Run Result */}
              {runResult[step.job_name] ? (
                <Text style={s.runResultText}>{runResult[step.job_name]}</Text>
              ) : null}

              {/* Last Run Info */}
              {run ? (
                <View style={s.runInfo}>
                  <View style={s.runInfoRow}>
                    <Text style={s.runLabel}>Last run:</Text>
                    <Text style={[s.runValue, { color: getStatusColor(run.status) }]}>
                      {formatTime(run.start_time)} · {formatDuration(run.duration_seconds)}
                    </Text>
                  </View>
                  {run.triggered_by && (
                    <View style={s.runInfoRow}>
                      <Text style={s.runLabel}>Triggered by:</Text>
                      <Text style={s.runValue}>{run.triggered_by}</Text>
                    </View>
                  )}
                  {run.records_processed !== undefined && run.records_processed > 0 && (
                    <View style={s.runInfoRow}>
                      <Text style={s.runLabel}>Result:</Text>
                      <Text style={s.runValue}>{run.records_processed.toLocaleString()} {step.result_label}</Text>
                    </View>
                  )}
                  <View style={s.runInfoRow}>
                    <Text style={s.runLabel}>Next run:</Text>
                    <Text style={s.runValue}>{getNextRun(step.scheduledHour, step.scheduledMinute)}</Text>
                  </View>
                  {run.error_message && (
                    <Text style={s.errorText}>⚠️ {run.error_message}</Text>
                  )}
                </View>
              ) : (
<View style={s.runInfo}>
                  <View style={s.runInfoRow}>
                    <Text style={s.runLabel}>Next run:</Text>
                    <Text style={s.runValue}>{getNextRun(step.scheduledHour, step.scheduledMinute)}</Text>
                  </View>
                </View>
              )}

              {/* Expand Details */}
              <TouchableOpacity style={s.expandBtn} onPress={() => toggleExpand(step.step)}>
                <Text style={s.expandText}>Details</Text>
                <Ionicons name={isExpanded ? 'chevron-up' : 'chevron-down'} size={12} color={COLORS.textMuted} />
              </TouchableOpacity>

              {isExpanded && (
                <View style={s.details}>
                  <Text style={s.detailLabel}>Description</Text>
                  <Text style={s.detailValue}>{step.description}</Text>
                  <Text style={[s.detailLabel, { marginTop: 8 }]}>API Endpoints</Text>
                  {step.apis.map(api => (
                    <Text key={api} style={s.apiText}>· {api}</Text>
                  ))}
                  <Text style={[s.detailLabel, { marginTop: 8 }]}>Filters</Text>
                  {step.filters.map(f => (
                    <Text key={f} style={s.filterText}>✓ {f}</Text>
                  ))}
                </View>
              )}
            </View>

            {/* Arrow between steps */}
            {idx < PIPELINE_STEPS.length - 1 && (
              <View style={s.arrow}>
                <View style={s.arrowLine} />
                <Ionicons name="chevron-down" size={12} color={COLORS.textMuted} />
              </View>
            )}
          </View>
        );
      })}

      {/* Morning Fresh */}
      <View style={[s.stepCard, { borderLeftColor: MORNING_FRESH.color, borderLeftWidth: 3 }]}>
        <View style={s.stepHeader}>
          <View style={[s.stepBadge, { backgroundColor: MORNING_FRESH.color + '22' }]}>
            <Ionicons name={MORNING_FRESH.icon} size={16} color={MORNING_FRESH.color} />
          </View>
          <View style={s.stepMeta}>
            <View style={s.stepTitleRow}>
              <Text style={s.stepTitle}>{MORNING_FRESH.title}</Text>
              {(() => {
                const run = jobRuns[MORNING_FRESH.job_name];
                return run ? (
                  <Ionicons name={getStatusIcon(run.status) as any} size={14} color={getStatusColor(run.status)} style={{ marginLeft: 4 }} />
                ) : null;
              })()}
            </View>
            <Text style={s.stepSchedule}>{MORNING_FRESH.schedule}</Text>
          </View>
          <TouchableOpacity
            style={[s.runBtn, { backgroundColor: MORNING_FRESH.color }, runningJob === MORNING_FRESH.job_name && s.runBtnDisabled]}
            onPress={() => handleRunNow(MORNING_FRESH.job_name)}
            disabled={!!runningJob}
          >
            {runningJob === MORNING_FRESH.job_name
              ? <ActivityIndicator size="small" color="#fff" />
              : <Text style={s.runBtnText}>▶ Run</Text>
            }
          </TouchableOpacity>
        </View>
        {jobRuns[MORNING_FRESH.job_name] ? (
          <View style={s.runInfo}>
            <Text style={s.runValue}>
              Last: {formatTime(jobRuns[MORNING_FRESH.job_name].start_time)} · {formatDuration(jobRuns[MORNING_FRESH.job_name].duration_seconds)}
            </Text>
          </View>
        ) : (
          <Text style={s.neverRun}>Never run</Text>
        )}
        <Text style={s.detailValue}>{MORNING_FRESH.description}</Text>
        <Text style={s.apiText}>· {MORNING_FRESH.apis[0]}</Text>
      </View>

      {/* Universe Funnel */}
      <View style={s.funnelCard}>
        <View style={s.cardHeader}>
          <Ionicons name="funnel" size={16} color="#6366F1" />
          <Text style={s.cardTitle}>Universe Funnel</Text>
          <Text style={s.funnelTotal}>{(counts.visible_tickers || 0).toLocaleString()} visible</Text>
        </View>
        {funnelSteps.map((f, i) => {
          const pct = f.total ? Math.round(((f.count || 0) / f.total) * 100) : 0;
          const isLast = i === funnelSteps.length - 1;
          return (
            <View key={f.label} style={s.funnelRow}>
              <Text style={[s.funnelLabel, isLast && { fontWeight: '700', color: '#22C55E' }]}>{f.label}</Text>
              <View style={s.funnelBarWrap}>
                <View style={[s.funnelBar, {
                  width: `${pct}%` as any,
                  backgroundColor: isLast ? '#22C55E' : '#6366F1',
                  opacity: isLast ? 1 : 0.6 + (i * 0.1),
                }]} />
              </View>
              <Text style={[s.funnelCount, isLast && { color: '#22C55E', fontWeight: '700' }]}>
                {(f.count || 0).toLocaleString()}
                <Text style={s.funnelPct}>  {pct}%</Text>
              </Text>
            </View>
          );
        })}
      </View>

      <View style={{ height: 40 }} />
    </ScrollView>
  );
}

const s = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.background },
  center: { flex: 1, alignItems: 'center', justifyContent: 'center' },

  healthCard: { margin: 12, backgroundColor: COLORS.card, borderRadius: 10, padding: 14, borderWidth: 1, borderColor: COLORS.border },
  healthHeader: { flexDirection: 'row', alignItems: 'center', marginBottom: 8 },
  healthTitle: { fontSize: 13, fontWeight: '700', color: COLORS.text, flex: 1 },
  healthPct: { fontSize: 20, fontWeight: '800', marginRight: 4 },
  progressBg: { height: 6, backgroundColor: COLORS.border, borderRadius: 3, overflow: 'hidden', marginBottom: 6 },
  progressFill: { height: 6, borderRadius: 3 },
  healthSub: { fontSize: 11, color: COLORS.textMuted },

  stepCard: { marginHorizontal: 12, marginBottom: 0, backgroundColor: COLORS.card, borderRadius: 10, padding: 12, borderWidth: 1, borderColor: COLORS.border },
  stepHeader: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  stepBadge: { width: 32, height: 32, borderRadius: 8, alignItems: 'center', justifyContent: 'center' },
  stepMeta: { flex: 1 },
  stepTitleRow: { flexDirection: 'row', alignItems: 'center', gap: 4 },
  stepNum: { fontSize: 9, fontWeight: '700', color: COLORS.textMuted, letterSpacing: 0.5 },
  stepTitle: { fontSize: 13, fontWeight: '600', color: COLORS.text },
  stepSchedule: { fontSize: 10, color: COLORS.textMuted, marginTop: 1 },

  runBtn: { backgroundColor: '#6366F1', paddingHorizontal: 10, paddingVertical: 5, borderRadius: 6, minWidth: 54, alignItems: 'center' },
  runBtnDisabled: { opacity: 0.5 },
  runBtnText: { color: '#fff', fontSize: 11, fontWeight: '600' },
  runResultText: { fontSize: 11, marginTop: 6, color: COLORS.textMuted },

  runInfo: { marginTop: 8, paddingTop: 8, borderTopWidth: 1, borderTopColor: COLORS.border },
  runInfoRow: { flexDirection: 'row', gap: 6, marginBottom: 2 },
  runLabel: { fontSize: 11, color: COLORS.textMuted, width: 80 },
  runValue: { fontSize: 11, color: COLORS.text, flex: 1 },
  errorText: { fontSize: 11, color: '#EF4444', marginTop: 4 },
  neverRun: { fontSize: 11, color: COLORS.textMuted, marginTop: 8, fontStyle: 'italic' },

  expandBtn: { flexDirection: 'row', alignItems: 'center', gap: 4, marginTop: 8, alignSelf: 'flex-start' },
  expandText: { fontSize: 11, color: COLORS.textMuted },
  details: { marginTop: 8, paddingTop: 8, borderTopWidth: 1, borderTopColor: COLORS.border },
  detailLabel: { fontSize: 10, fontWeight: '700', color: COLORS.textMuted, letterSpacing: 0.5, marginBottom: 3 },
  detailValue: { fontSize: 11, color: COLORS.text, marginBottom: 2 },
  apiText: { fontSize: 10, color: '#6366F1', fontFamily: 'monospace', marginBottom: 1 },
  filterText: { fontSize: 10, color: '#22C55E', marginBottom: 1 },

  arrow: { alignItems: 'center', paddingVertical: 4 },
  arrowLine: { width: 1, height: 8, backgroundColor: COLORS.border },

  funnelCard: { margin: 12, backgroundColor: COLORS.card, borderRadius: 10, padding: 14, borderWidth: 1, borderColor: COLORS.border },
  cardHeader: { flexDirection: 'row', alignItems: 'center', gap: 6, marginBottom: 12 },
  cardTitle: { fontSize: 13, fontWeight: '600', color: COLORS.text, flex: 1 },
  funnelTotal: { fontSize: 12, fontWeight: '700', color: '#22C55E' },
  funnelRow: { marginBottom: 8 },
  funnelLabel: { fontSize: 11, color: COLORS.textMuted, marginBottom: 3 },
  funnelBarWrap: { height: 6, backgroundColor: COLORS.border, borderRadius: 3, overflow: 'hidden', marginBottom: 3 },
  funnelBar: { height: 6, borderRadius: 3 },
  funnelCount: { fontSize: 12, color: COLORS.text, fontWeight: '600' },
  funnelPct: { fontSize: 10, color: COLORS.textMuted, fontWeight: '400' },
});
