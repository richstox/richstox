/**
 * RICHSTOX Admin Pipeline
 * Universe Pipeline — 5-step sequential process with integrated funnel per step
 */

import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  View, Text, ScrollView, TouchableOpacity, StyleSheet,
  RefreshControl, ActivityIndicator, Alert, Linking, Platform,
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

interface Step2SubStep {
  mock_mode?: boolean;
  api_endpoint?: string;
  api_endpoints_all?: string[];
  dates_checked?: string[];
  raw_count?: number;
  universe_count?: number;
  flagged_count?: number;
  tickers_sample?: string[];
}

interface PipelineSyncStatus {
  total_visible_tickers?: number;
  price_history_complete?: number;
  price_history_pct?: number;
  fundamentals_complete?: number;
  fundamentals_pct?: number;
  needs_price_redownload?: number;
  needs_fundamentals_refresh?: number;
  credits_today?: number;
  credits_limit?: number;
  credits_pct?: number;
}

interface OverviewData {
  health?: {
    scheduler_active?: boolean;
  };
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
  pipeline_sync_status?: PipelineSyncStatus;
}

interface PipelineExclusionRow {
  ticker: string;
  name: string;
  step: string;
  reason: string;
}

interface PipelineExclusionReport {
  report_date: string;
  total_rows: number;
  has_rows?: boolean;
  empty_report_hint?: string | null;
  rows: PipelineExclusionRow[];
  by_reason: Record<string, number>;
}

function getNextRun(hour: number, minute: number, skipSunday: boolean = false): string {
  try {
    const now = new Date();
    const pragueNow = new Date(now.toLocaleString('en-US', { timeZone: 'Europe/Prague' }));
    const nextRun = new Date(pragueNow);
    nextRun.setHours(hour, minute, 0, 0);
    if (nextRun <= pragueNow) nextRun.setDate(nextRun.getDate() + 1);
    if (skipSunday) {
      while (nextRun.getDay() === 0) {
        nextRun.setDate(nextRun.getDate() + 1);
      }
    }
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
    return `${d.toLocaleString('en-GB', {
      timeZone: 'Europe/Prague',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    })} Prague`;
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

function fmt(n?: number): string {
  if (n === undefined || n === null) return '—';
  return n.toLocaleString();
}

function safeCount(v: any): number {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  return 0;
}

export default function PipelineTab({ sessionToken }: PipelineProps) {
  const [data, setData] = useState<OverviewData | null>(null);
  const [exclusionReport, setExclusionReport] = useState<PipelineExclusionReport | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [runningJob, setRunningJob] = useState<string | null>(null);
  const [downloadingReport, setDownloadingReport] = useState(false);
  const [schedulerUpdating, setSchedulerUpdating] = useState(false);
  const [runResult, setRunResult] = useState<Record<string, string>>({});
  const [expandedSteps, setExpandedSteps] = useState<Set<number>>(new Set());
  const [elapsedSeconds, setElapsedSeconds] = useState(0);
  const [liveProgress, setLiveProgress] = useState<string>('');
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchData = useCallback(async () => {
    try {
      const requestHeaders = sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {};
      const [overviewRes, exclusionRes] = await Promise.all([
        fetch(`${API_URL}/api/admin/overview`, { headers: requestHeaders }),
        fetch(`${API_URL}/api/admin/pipeline/exclusion-report?limit=20`, { headers: requestHeaders }),
      ]);

      if (overviewRes.ok) setData(await overviewRes.json());
      if (exclusionRes.ok) setExclusionReport(await exclusionRes.json());
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [sessionToken]);

  useEffect(() => { fetchData(); }, [fetchData]);

  const onRefresh = () => { setRefreshing(true); fetchData(); };

  const handleDownloadExclusionReport = async () => {
    if (!exclusionReport?.report_date || downloadingReport) return;
    setDownloadingReport(true);
    try {
      const params = new URLSearchParams({ report_date: exclusionReport.report_date });
      const downloadUrl = `${API_URL}/api/admin/pipeline/exclusion-report/download?${params.toString()}`;
      if (Platform.OS === 'web' && typeof window !== 'undefined') {
        const requestHeaders = sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {};
        const response = await fetch(downloadUrl, { headers: requestHeaders });
        if (!response.ok) {
          throw new Error(`Download failed (${response.status})`);
        }
        const blob = await response.blob();
        const objectUrl = window.URL.createObjectURL(blob);
        const link = window.document.createElement('a');
        link.href = objectUrl;
        link.download = `pipeline_exclusion_report_${exclusionReport.report_date}.csv`;
        window.document.body.appendChild(link);
        link.click();
        link.remove();
        window.URL.revokeObjectURL(objectUrl);
      } else {
        await Linking.openURL(downloadUrl);
      }
    } catch (e: any) {
      Alert.alert('Download failed', e?.message || 'Could not open report download');
    } finally {
      setDownloadingReport(false);
    }
  };

  const hasExclusionRows = (exclusionReport?.total_rows ?? 0) > 0;
  const schedulerActive = data?.health?.scheduler_active;

  const handleSchedulerToggle = async () => {
    if (schedulerUpdating) return;
    if (typeof schedulerActive !== 'boolean') {
      Alert.alert('Scheduler status unavailable', 'Please refresh and try again.');
      return;
    }
    setSchedulerUpdating(true);
    try {
      const targetEnabled = !schedulerActive;
      const requestHeaders = sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {};
      const res = await fetch(
        `${API_URL}/api/admin/scheduler/kill-switch?enabled=${targetEnabled ? 'true' : 'false'}`,
        { method: 'POST', headers: requestHeaders }
      );
      const payload = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(payload?.detail || payload?.message || res.statusText);
      }
      await fetchData();
      Alert.alert('Scheduler updated', targetEnabled ? 'Scheduler resumed.' : 'Scheduler paused.');
    } catch (e: any) {
      Alert.alert('Update failed', e?.message || 'Could not update scheduler state');
    } finally {
      setSchedulerUpdating(false);
    }
  };

  const stopPolling = () => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
    if (timerRef.current) {
      clearInterval(timerRef.current);
      timerRef.current = null;
    }
    setElapsedSeconds(0);
    setLiveProgress('');
  };

  const startPolling = (jobName: string, startedAt: number) => {
    stopPolling();
    setElapsedSeconds(0);
    // Local 1-second timer for elapsed display
    timerRef.current = setInterval(() => {
      setElapsedSeconds(Math.round((Date.now() - startedAt) / 1000));
    }, 1000);
    let pollTick = 0;
    pollRef.current = setInterval(async () => {
      try {
        pollTick += 1;
        const requestHeaders = sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {};
        const res = await fetch(`${API_URL}/api/admin/jobs/${jobName}/status`, { headers: requestHeaders });
        if (!res.ok) return;
        const json = await res.json();
        const lastRun = json.last_run;
        if (!lastRun) return;
        // MongoDB datetimes may lack timezone suffix — force UTC interpretation
        const rawStart = lastRun.started_at || '';
        const utcStart = rawStart.endsWith('Z') || rawStart.includes('+') ? rawStart : rawStart + 'Z';
        const runStart = utcStart ? Date.parse(utcStart) : 0;
        if (runStart >= startedAt) {
          const st = lastRun.status || 'completed';
          if (st === 'running') {
            // Update live progress message from server (timer shown separately)
            const progressMsg = lastRun.progress || JOB_DESCRIPTIONS[jobName] || 'Running…';
            setLiveProgress(progressMsg);
            // Refresh overview data every ~9s (every 3rd poll) so counts update live
            if (pollTick % 3 === 0) {
              fetchData();
            }
            return; // keep polling
          }
          stopPolling();
          setRunningJob(null);
          if (st === 'cancelled') {
            setRunResult(prev => ({ ...prev, [jobName]: '🛑 Cancelled' }));
          } else if (st === 'failed' || st === 'error') {
            setRunResult(prev => ({ ...prev, [jobName]: `❌ ${st}` }));
          } else {
            setRunResult(prev => ({ ...prev, [jobName]: `✅ ${st}` }));
          }
          fetchData();
        }
      } catch { /* ignore poll errors */ }
    }, 3000);
  };

  const JOB_DESCRIPTIONS: Record<string, string> = {
    universe_seed: 'Fetching NYSE + NASDAQ symbols from EODHD…',
    price_sync: 'Downloading bulk prices + running split/dividend/earnings detectors…',
    fundamentals_sync: 'Syncing fundamentals for queued tickers…',
    compute_visible_universe: 'Computing visibility rules for all tickers…',
    peer_medians: 'Computing peer benchmark medians…',
    news_refresh: 'Fetching news and sentiment…',
    full_price_history_sync: 'Downloading complete price history per ticker (eod/{TICKER}.US)…',
    full_fundamentals_sync: 'Downloading complete fundamentals per ticker (fundamentals/{TICKER}.US)…',
  };

  const handleRunNow = async (jobName: string) => {
    if (runningJob) return;
    setRunningJob(jobName);
    setRunResult(prev => ({ ...prev, [jobName]: JOB_DESCRIPTIONS[jobName] || 'Starting…' }));
    const triggeredAt = Date.now();
    try {
      const requestHeaders = sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {};
      const endpoint = jobName === 'universe_seed'
        ? `${API_URL}/api/admin/jobs/universe-seed`
        : `${API_URL}/api/admin/scheduler/run/${jobName.replace(/_/g, '-')}`;
      const res = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...requestHeaders },
      });
      const json = await res.json();
      if (res.ok) {
        setRunResult(prev => ({ ...prev, [jobName]: JOB_DESCRIPTIONS[jobName] || '⏳ Running…' }));
        startPolling(jobName, triggeredAt);
      } else {
        setRunResult(prev => ({ ...prev, [jobName]: `❌ ${json.error || json.detail || res.statusText}` }));
        setRunningJob(null);
      }
    } catch (e: any) {
      setRunResult(prev => ({ ...prev, [jobName]: `❌ ${e.message}` }));
      setRunningJob(null);
    }
  };

  const handleCancelJob = async (jobName: string) => {
    try {
      const requestHeaders = sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {};
      await fetch(`${API_URL}/api/admin/jobs/${jobName}/cancel`, {
        method: 'POST',
        headers: requestHeaders,
      });
      setRunResult(prev => ({ ...prev, [jobName]: '🛑 Cancel requested…' }));
    } catch { /* ignore */ }
  };

  const handleFullSync = async (jobName: string) => {
    if (runningJob) return;
    setRunningJob(jobName);
    setRunResult(prev => ({ ...prev, [jobName]: '' }));
    const triggeredAt = Date.now();
    try {
      const requestHeaders = sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {};
      const endpoint = `${API_URL}/api/admin/jobs/${jobName.replace(/_/g, '-')}`;
      const res = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...requestHeaders },
      });
      const json = await res.json();
      if (res.ok) {
        setRunResult(prev => ({ ...prev, [jobName]: '⏳ Running… (may take 15–30 min)' }));
        startPolling(jobName, triggeredAt);
      } else {
        setRunResult(prev => ({ ...prev, [jobName]: `❌ ${json.detail || res.statusText}` }));
        setRunningJob(null);
      }
    } catch (e: any) {
      setRunResult(prev => ({ ...prev, [jobName]: `❌ ${e.message}` }));
      setRunningJob(null);
    }
  };

  const toggleExpand = (step: number) => {
    setExpandedSteps(prev => {
      const next = new Set(prev);
      if (next.has(step)) {
        next.delete(step);
      } else {
        next.add(step);
      }
      return next;
    });
  };

  const jobRuns = data?.job_last_runs || {};
  const counts = data?.universe_funnel?.counts || {};
  const syncStatus = data?.pipeline_sync_status || {};
  const todayStr = new Date().toISOString().split('T')[0];

  const rawSymbols = (jobRuns['universe_seed'] as any)?.raw_symbols_fetched as number | undefined;
  const seeded = counts.seeded_us_total;
  const withPrice = counts.with_price_data;
  const withClass = counts.with_classification;
  const visible = counts.visible_tickers;

  const JOB_OUTPUT: Record<string, number | undefined> = {
    universe_seed: seeded,
    price_sync: withPrice,
    fundamentals_sync: withClass,
    compute_visible_universe: visible,
    peer_medians: visible,
  };
  const completedCount = ['universe_seed', 'price_sync', 'fundamentals_sync', 'compute_visible_universe', 'peer_medians'].filter(j => {
    const r = jobRuns[j];
    const ok = r?.status === 'success' || r?.status === 'completed';
    return ok && (JOB_OUTPUT[j] === undefined || (JOB_OUTPUT[j] ?? 0) > 0);
  }).length;
  const healthPct = Math.round((completedCount / 5) * 100);
  const healthColor = healthPct === 100 ? '#22C55E' : healthPct >= 60 ? '#F59E0B' : '#EF4444';

  const steps = [
    {
      step: 1,
      job_name: 'universe_seed',
      title: 'Universe Seed',
      schedule: 'Mon–Sat 23:00 Prague',
      scheduledHour: 23,
      scheduledMinute: 0,
      icon: 'globe-outline' as const,
      color: '#6366F1',
      apiUrl: 'https://eodhd.com/api/exchange-symbol-list/{NYSE|NASDAQ}',
      inputLabel: 'Raw symbols (EODHD)',
      inputCount: rawSymbols,
      outputCount: seeded,
      outputLabel: 'seeded tickers',
      filters: [
        'Type ≠ "Common Stock"',
        'Ticker contains dot (ADR / preferred)',
        'Suffix -WT / -WS / -WI (warrants)',
        'Suffix -U / -UN (units)',
        'Suffix -PA .. -PJ (preferred shares)',
        'Suffix -R / -RI (rights)',
      ],
    },
    {
      step: 2,
      job_name: 'price_sync',
      title: 'Price Sync',
      schedule: 'After Step 1 completion',
      scheduledHour: 4,
      scheduledMinute: 0,
      icon: 'trending-up-outline' as const,
      color: '#10B981',
      apiUrl: 'https://eodhd.com/api/eod-bulk-last-day/US',
      inputLabel: 'Seeded tickers',
      inputCount: seeded,
      outputCount: withPrice,
      outputLabel: 'with price data',
      filters: [
        'Not present in EODHD bulk response',
        'Close price = 0 or missing',
      ],
    },
    {
      step: 3,
      job_name: 'fundamentals_sync',
      title: 'Fundamentals Sync',
      schedule: 'After Step 2 completion',
      scheduledHour: 4,
      scheduledMinute: 30,
      icon: 'library-outline' as const,
      color: '#F59E0B',
      apiUrl: 'https://eodhd.com/api/fundamentals/{TICKER}.US  (~10 credits/ticker)',
      inputLabel: 'Tickers with prices',
      inputCount: withPrice,
      outputCount: withClass,
      outputLabel: 'classified',
      filters: [
        'EODHD returns no fundamentals (404)',
        'Sector missing or empty',
        'Industry missing or empty',
      ],
    },
    {
      step: 4,
      job_name: 'compute_visible_universe',
      title: 'Compute Visible Universe',
      schedule: 'After Step 3 completion',
      scheduledHour: 4,
      scheduledMinute: 30,
      icon: 'eye-outline' as const,
      color: '#8B5CF6',
      apiUrl: 'Local DB only — no external API',
      inputLabel: 'Classified tickers',
      inputCount: withClass,
      outputCount: visible,
      outputLabel: 'visible',
      filters: [
        'is_delisted = true',
        'shares_outstanding = 0 or missing',
        'financial_currency missing',
      ],
    },
    {
      step: 5,
      job_name: 'peer_medians',
      title: 'Peer Medians',
      schedule: 'After Step 4 completion',
      scheduledHour: 5,
      scheduledMinute: 30,
      icon: 'stats-chart-outline' as const,
      color: '#EC4899',
      apiUrl: 'Local DB only — no external API',
      inputLabel: 'Visible tickers',
      inputCount: visible,
      outputCount: visible,
      outputLabel: 'with medians',
      filters: [
        'Winsorize outliers (1–99%)',
        'Exclude self from own peer group',
      ],
    },
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
        <Text style={s.healthSub}>{completedCount}/5 steps completed today</Text>
        <View style={s.schedulerControlRow}>
          <Text style={s.schedulerControlText}>
            Scheduler is currently {schedulerActive ? 'active' : 'paused'}.
          </Text>
          <TouchableOpacity
            style={[
              s.schedulerBtn,
              schedulerActive ? s.schedulerPauseBtn : s.schedulerResumeBtn,
              (schedulerUpdating || typeof schedulerActive !== 'boolean') && s.schedulerBtnDisabled,
            ]}
            onPress={handleSchedulerToggle}
            disabled={schedulerUpdating || typeof schedulerActive !== 'boolean'}
          >
            {schedulerUpdating
              ? <ActivityIndicator size="small" color="#fff" />
              : <Text style={s.schedulerBtnText}>{schedulerActive ? 'Pause Scheduler' : 'Resume Scheduler'}</Text>}
          </TouchableOpacity>
        </View>
        {/* Mini funnel summary */}
        <View style={s.miniSummary}>
          <View style={s.miniItem}>
            <Text style={s.miniNum}>{fmt(rawSymbols)}</Text>
            <Text style={s.miniLabel}>raw</Text>
          </View>
          <Ionicons name="chevron-forward" size={10} color={COLORS.textMuted} />
          <View style={s.miniItem}>
            <Text style={s.miniNum}>{fmt(seeded)}</Text>
            <Text style={s.miniLabel}>seeded</Text>
          </View>
          <Ionicons name="chevron-forward" size={10} color={COLORS.textMuted} />
          <View style={s.miniItem}>
            <Text style={s.miniNum}>{fmt(withPrice)}</Text>
            <Text style={s.miniLabel}>prices</Text>
          </View>
          <Ionicons name="chevron-forward" size={10} color={COLORS.textMuted} />
          <View style={s.miniItem}>
            <Text style={s.miniNum}>{fmt(withClass)}</Text>
            <Text style={s.miniLabel}>classified</Text>
          </View>
          <Ionicons name="chevron-forward" size={10} color={COLORS.textMuted} />
          <View style={s.miniItem}>
            <Text style={[s.miniNum, { color: '#22C55E' }]}>{fmt(visible)}</Text>
            <Text style={[s.miniLabel, { color: '#22C55E' }]}>visible</Text>
          </View>
        </View>
      </View>

      {/* Pipeline Steps */}
      {steps.map((step, idx) => {
        const run = jobRuns[step.job_name];
        const status = run?.status;
        const isExpanded = expandedSteps.has(step.step);
        const isRunning = runningJob === step.job_name;

        const inCount = step.inputCount;
        const outCount = step.outputCount;
        const droppedCount = (inCount !== undefined && outCount !== undefined)
          ? Math.max(inCount - outCount, 0)
          : undefined;
        const passPct = (inCount !== undefined && inCount > 0 && outCount !== undefined)
          ? Math.round((outCount / inCount) * 100)
          : null;
        const prevStep = idx > 0 ? steps[idx - 1] : null;
        const prevRun = prevStep ? jobRuns[prevStep.job_name] : null;
        const prevRunOk = !!prevRun && (prevRun.status === 'success' || prevRun.status === 'completed');
        const prevRunTs = prevRun?.start_time ? Date.parse(prevRun.start_time) : 0;
        const currentRunTs = run?.start_time ? Date.parse(run.start_time) : 0;
        const eventDetectors = step.job_name === 'price_sync'
          ? ((run as any)?.details?.event_detectors || {})
          : {};
        const splitDetector: Step2SubStep = eventDetectors?.step_2_2_split || {};
        const dividendDetector: Step2SubStep = eventDetectors?.step_2_4_dividend || {};
        const earningsDetector: Step2SubStep = eventDetectors?.step_2_6_earnings || {};
        const hasStep2DetectorPayload = Object.keys(eventDetectors || {}).length > 0;
        const nextRunLabel = step.step === 1
          ? getNextRun(step.scheduledHour, step.scheduledMinute, true)
          : (!prevRunOk || inCount === 0)
            ? `After Step ${step.step - 1} completion`
            : (!run || currentRunTs < prevRunTs)
              ? 'Ready now'
              : `After next Step ${step.step - 1} completion`;
        const processedCount = outCount ?? run?.records_processed;
        const processedLabel = step.job_name === 'price_sync' ? 'Processed tickers:' : 'Processed:';

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
                    {isRunning ? (
                      <ActivityIndicator size="small" color="#F59E0B" style={{ marginLeft: 4 }} />
                    ) : status ? (
                      <Ionicons
                        name={getStatusIcon(status) as any}
                        size={14} color={getStatusColor(status)}
                        style={{ marginLeft: 4 }}
                      />
                    ) : null}
                  </View>
                  <Text style={s.stepSchedule}>{step.schedule}</Text>
                </View>
                <View style={s.jobBtnGroup}>
                  {isRunning ? (
                    <TouchableOpacity
                      style={s.cancelBtn}
                      onPress={() => handleCancelJob(step.job_name)}
                    >
                      <Text style={s.cancelBtnText}>■ Stop</Text>
                    </TouchableOpacity>
                  ) : (
                    <TouchableOpacity
                      style={[s.runBtn, { backgroundColor: step.color }, !!runningJob && s.runBtnDisabled]}
                      onPress={() => handleRunNow(step.job_name)}
                      disabled={!!runningJob}
                    >
                      <Text style={s.runBtnText}>▶ Run</Text>
                    </TouchableOpacity>
                  )}
                </View>
              </View>

              {/* Run Result */}
              {isRunning ? (
                <View style={s.progressRow}>
                  <Text style={s.progressText}>
                    {liveProgress || runResult[step.job_name] || JOB_DESCRIPTIONS[step.job_name] || 'Starting…'}
                  </Text>
                  <Text style={s.elapsedText}>{elapsedSeconds}s</Text>
                </View>
              ) : runResult[step.job_name] ? (
                <Text style={[s.runResultText, isRunning && { color: '#F59E0B' }]}>{runResult[step.job_name]}</Text>
              ) : null}

              {/* ── Integrated Funnel Row ── */}
              {/* Steps 2+: waiting state if previous step has 0 output */}
              {inCount === 0 && step.step > 1 ? (
                <View style={s.waitingRow}>
                  <Ionicons name="time-outline" size={13} color={COLORS.textMuted} />
                  <Text style={s.waitingText}>Waiting for Step {step.step - 1} to complete</Text>
                </View>
              ) : (
                <View style={s.funnelRow}>
                  {/* INPUT */}
                  {inCount !== undefined && (
                    <View style={s.funnelBox}>
                      <Text style={s.funnelBoxNum}>{fmt(inCount)}</Text>
                      <Text style={s.funnelBoxLabel}>{step.inputLabel}</Text>
                    </View>
                  )}

                  {/* Arrow + pass rate — only when we have both sides */}
                  {inCount !== undefined && (
                    <View style={s.funnelArrow}>
                      <Ionicons name="arrow-forward" size={16} color={step.color} />
                      {passPct !== null && (
                        <Text style={[s.funnelPct, { color: passPct > 50 ? '#22C55E' : '#EF4444' }]}>
                          {passPct}%
                        </Text>
                      )}
                    </View>
                  )}

                  {/* OUTPUT */}
                  <View style={[s.funnelBox, s.funnelBoxOut, { borderColor: step.color + '88', flex: 2.5 }]}>
                    <Text style={[s.funnelBoxNum, { color: step.color }]}>
                      {outCount !== undefined ? fmt(outCount) : '—'}
                    </Text>
                    <Text style={s.funnelBoxLabel}>{step.outputLabel}</Text>
                  </View>

                  {/* DROPPED */}
                  {droppedCount !== undefined && droppedCount > 0 && (
                    <View style={s.funnelDropped}>
                      <Text style={s.funnelDropNum}>−{fmt(droppedCount)}</Text>
                      <Text style={s.funnelDropLabel}>filtered out</Text>
                    </View>
                  )}
                </View>
              )}

              {/* Progress bar */}
              {passPct !== null && (
                <View style={s.funnelBarWrap}>
                  <View style={[s.funnelBar, {
                    width: `${passPct}%` as any,
                    backgroundColor: step.color,
                  }]} />
                </View>
              )}

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
                  {processedCount !== undefined && processedCount > 0 && (
                    <View style={s.runInfoRow}>
                      <Text style={s.runLabel}>{processedLabel}</Text>
                      <Text style={s.runValue}>{processedCount.toLocaleString()}</Text>
                    </View>
                  )}
                  <View style={s.runInfoRow}>
                    <Text style={s.runLabel}>Next run:</Text>
                    <Text style={s.runValue}>{nextRunLabel}</Text>
                  </View>
                  {run.error_message && (
                    <Text style={s.errorText}>⚠️ {run.error_message}</Text>
                  )}
                </View>
              ) : (
                <View style={s.runInfo}>
                  <View style={s.runInfoRow}>
                    <Text style={s.runLabel}>Next run:</Text>
                    <Text style={s.runValue}>{nextRunLabel}</Text>
                  </View>
                </View>
              )}

              {step.job_name === 'price_sync' && (
                <View style={s.substepsCard}>
                  <Text style={s.substepsTitle}>Step 2 Sub-Steps</Text>

                  {!hasStep2DetectorPayload && (
                    <Text style={s.substepMeta}>No data yet — run Step 2 to populate.</Text>
                  )}

                  {/* 2.2 Split Detector */}
                  <View style={s.substepBlock}>
                    <View style={s.substepHeaderRow}>
                      <Text style={s.substepName}>2.2 Split Detector</Text>
                      {splitDetector.mock_mode && (
                        <View style={s.mockBadge}><Text style={s.mockBadgeText}>NO API KEY</Text></View>
                      )}
                    </View>
                    <Text style={s.substepDesc}>Detects stock splits today. Flagged tickers need full price history re-download (adjusted prices change).</Text>
                    <Text style={s.substepEndpoint} numberOfLines={1}>
                      {splitDetector.api_endpoint || `https://eodhd.com/api/eod-bulk-last-day/US?type=splits&date=${eventDetectors.today || todayStr}`}
                    </Text>
                    {(splitDetector.dates_checked?.length ?? 0) > 1 && (
                      <Text style={s.catchupBadge}>↩ Catchup: {splitDetector.dates_checked?.length} days ({splitDetector.dates_checked?.[0]} → {splitDetector.dates_checked?.slice(-1)[0]})</Text>
                    )}
                    <View style={s.substepStatsRow}>
                      <View style={s.substepStat}>
                        <Text style={s.substepStatNum}>{fmt(safeCount(splitDetector.raw_count))}</Text>
                        <Text style={s.substepStatLabel}>in feed</Text>
                      </View>
                      <Text style={s.substepStatSep}>→</Text>
                      <View style={s.substepStat}>
                        <Text style={s.substepStatNum}>{fmt(safeCount(splitDetector.universe_count))}</Text>
                        <Text style={s.substepStatLabel}>in universe</Text>
                      </View>
                      <Text style={s.substepStatSep}>→</Text>
                      <View style={s.substepStat}>
                        <Text style={[s.substepStatNum, { color: safeCount(splitDetector.flagged_count) > 0 ? '#F59E0B' : COLORS.textMuted }]}>
                          {fmt(safeCount(splitDetector.flagged_count))}
                        </Text>
                        <Text style={s.substepStatLabel}>flagged</Text>
                      </View>
                    </View>
                    {(splitDetector.tickers_sample?.length ?? 0) > 0 && (
                      <Text style={s.substepTickers} numberOfLines={1}>
                        {splitDetector.tickers_sample?.slice(0, 8).join(', ')}
                        {(splitDetector.tickers_sample?.length ?? 0) > 8 ? '…' : ''}
                      </Text>
                    )}
                  </View>

                  {/* 2.4 Dividend Detector */}
                  <View style={s.substepBlock}>
                    <View style={s.substepHeaderRow}>
                      <Text style={s.substepName}>2.4 Dividend Detector</Text>
                      {dividendDetector.mock_mode && (
                        <View style={s.mockBadge}><Text style={s.mockBadgeText}>NO API KEY</Text></View>
                      )}
                    </View>
                    <Text style={s.substepDesc}>Detects ex-dividend events today. Flagged tickers need fundamentals refresh (dividend yield, payout ratio).</Text>
                    <Text style={s.substepEndpoint} numberOfLines={1}>
                      {dividendDetector.api_endpoint || `https://eodhd.com/api/eod-bulk-last-day/US?type=dividends&date=${eventDetectors.today || todayStr}`}
                    </Text>
                    {(dividendDetector.dates_checked?.length ?? 0) > 1 && (
                      <Text style={s.catchupBadge}>↩ Catchup: {dividendDetector.dates_checked?.length} days ({dividendDetector.dates_checked?.[0]} → {dividendDetector.dates_checked?.slice(-1)[0]})</Text>
                    )}
                    <View style={s.substepStatsRow}>
                      <View style={s.substepStat}>
                        <Text style={s.substepStatNum}>{fmt(safeCount(dividendDetector.raw_count))}</Text>
                        <Text style={s.substepStatLabel}>in feed</Text>
                      </View>
                      <Text style={s.substepStatSep}>→</Text>
                      <View style={s.substepStat}>
                        <Text style={s.substepStatNum}>{fmt(safeCount(dividendDetector.universe_count))}</Text>
                        <Text style={s.substepStatLabel}>in universe</Text>
                      </View>
                      <Text style={s.substepStatSep}>→</Text>
                      <View style={s.substepStat}>
                        <Text style={[s.substepStatNum, { color: safeCount(dividendDetector.flagged_count) > 0 ? '#10B981' : COLORS.textMuted }]}>
                          {fmt(safeCount(dividendDetector.flagged_count))}
                        </Text>
                        <Text style={s.substepStatLabel}>flagged</Text>
                      </View>
                    </View>
                    {(dividendDetector.tickers_sample?.length ?? 0) > 0 && (
                      <Text style={s.substepTickers} numberOfLines={1}>
                        {dividendDetector.tickers_sample?.slice(0, 8).join(', ')}
                        {(dividendDetector.tickers_sample?.length ?? 0) > 8 ? '…' : ''}
                      </Text>
                    )}
                  </View>

                  {/* 2.6 Earnings Detector */}
                  <View style={[s.substepBlock, { borderBottomWidth: 0 }]}>
                    <View style={s.substepHeaderRow}>
                      <Text style={s.substepName}>2.6 Earnings Detector</Text>
                      {earningsDetector.mock_mode && (
                        <View style={s.mockBadge}><Text style={s.mockBadgeText}>NO API KEY</Text></View>
                      )}
                    </View>
                    <Text style={s.substepDesc}>Detects earnings reports due today. Flagged tickers need fundamentals refresh (EPS, revenue, guidance).</Text>
                    <Text style={s.substepEndpoint} numberOfLines={1}>
                      {earningsDetector.api_endpoint || `https://eodhd.com/api/calendar/earnings?from=${eventDetectors.today || todayStr}&to=${eventDetectors.today || todayStr}`}
                    </Text>
                    <View style={s.substepStatsRow}>
                      <View style={s.substepStat}>
                        <Text style={s.substepStatNum}>{fmt(safeCount(earningsDetector.raw_count))}</Text>
                        <Text style={s.substepStatLabel}>in feed</Text>
                      </View>
                      <Text style={s.substepStatSep}>→</Text>
                      <View style={s.substepStat}>
                        <Text style={s.substepStatNum}>{fmt(safeCount(earningsDetector.universe_count))}</Text>
                        <Text style={s.substepStatLabel}>in universe</Text>
                      </View>
                      <Text style={s.substepStatSep}>→</Text>
                      <View style={s.substepStat}>
                        <Text style={[s.substepStatNum, { color: safeCount(earningsDetector.flagged_count) > 0 ? '#6366F1' : COLORS.textMuted }]}>
                          {fmt(safeCount(earningsDetector.flagged_count))}
                        </Text>
                        <Text style={s.substepStatLabel}>flagged</Text>
                      </View>
                    </View>
                    {(earningsDetector.tickers_sample?.length ?? 0) > 0 && (
                      <Text style={s.substepTickers} numberOfLines={1}>
                        {earningsDetector.tickers_sample?.slice(0, 8).join(', ')}
                        {(earningsDetector.tickers_sample?.length ?? 0) > 8 ? '…' : ''}
                      </Text>
                    )}
                  </View>
                </View>
              )}

              {step.job_name === 'fundamentals_sync' && run?.details?.requested_event_types && (
                <View style={s.substepsCard}>
                  <Text style={s.substepsTitle}>Step 3 input event types</Text>
                  {Object.entries(run.details.requested_event_types as Record<string, number>).map(([eventType, count]) => (
                    <View key={eventType} style={s.substepRow}>
                      <Text style={s.substepName}>{eventType}</Text>
                      <Text style={s.substepValue}>{fmt(count as number)}</Text>
                    </View>
                  ))}
                </View>
              )}

              {/* Full Price History button — Step 2 only */}
              {step.job_name === 'price_sync' && (
                <View style={s.fullSyncBlock}>
                  <View style={s.fullSyncInfo}>
                    <Text style={s.fullSyncTitle}>Full Price History Download</Text>
                    <Text style={s.fullSyncDesc}>
                      Downloads complete EOD history (IPO → today) for all visible tickers.{'\n'}
                      ~{fmt(safeCount(syncStatus.total_visible_tickers))} tickers · ~1 credit each · ~15 min
                    </Text>
                    <Text style={s.substepEndpoint} numberOfLines={1}>
                      https://eodhd.com/api/eod/{'{'+'TICKER'+'}'}.US?fmt=json&period=d
                    </Text>
                    {runningJob === 'full_price_history_sync' && (
                      <View style={s.progressRow}>
                        <Text style={s.progressText}>{liveProgress || JOB_DESCRIPTIONS['full_price_history_sync'] || 'Downloading…'}</Text>
                        <Text style={s.elapsedText}>{elapsedSeconds}s</Text>
                      </View>
                    )}
                    {runResult['full_price_history_sync'] && runningJob !== 'full_price_history_sync' ? (
                      <Text style={s.fullSyncResult}>{runResult['full_price_history_sync']}</Text>
                    ) : null}
                  </View>
                  {runningJob === 'full_price_history_sync' ? (
                    <TouchableOpacity style={s.cancelBtn} onPress={() => handleCancelJob('full_price_history_sync')}>
                      <Text style={s.cancelBtnText}>■ Stop</Text>
                    </TouchableOpacity>
                  ) : (
                    <TouchableOpacity
                      style={[s.fullSyncBtn, !!runningJob && s.runBtnDisabled]}
                      onPress={() => handleFullSync('full_price_history_sync')}
                      disabled={!!runningJob}
                    >
                      <Text style={s.fullSyncBtnText}>⬇ Full Sync</Text>
                    </TouchableOpacity>
                  )}
                </View>
              )}

              {/* Full Fundamentals button — Step 3 only */}
              {step.job_name === 'fundamentals_sync' && (
                <View style={s.fullSyncBlock}>
                  <View style={s.fullSyncInfo}>
                    <Text style={s.fullSyncTitle}>Full Fundamentals Download</Text>
                    <Text style={s.fullSyncDesc}>
                      Downloads complete fundamentals for all visible tickers.{'\n'}
                      ~{fmt(safeCount(syncStatus.total_visible_tickers))} tickers · ~10 credits each · ~20 min
                    </Text>
                    <Text style={s.substepEndpoint} numberOfLines={1}>
                      https://eodhd.com/api/fundamentals/{'{'+'TICKER'+'}'}.US?fmt=json
                    </Text>
                    {runningJob === 'full_fundamentals_sync' && (
                      <View style={s.progressRow}>
                        <Text style={s.progressText}>{liveProgress || JOB_DESCRIPTIONS['full_fundamentals_sync'] || 'Downloading…'}</Text>
                        <Text style={s.elapsedText}>{elapsedSeconds}s</Text>
                      </View>
                    )}
                    {runResult['full_fundamentals_sync'] && runningJob !== 'full_fundamentals_sync' ? (
                      <Text style={s.fullSyncResult}>{runResult['full_fundamentals_sync']}</Text>
                    ) : null}
                  </View>
                  {runningJob === 'full_fundamentals_sync' ? (
                    <TouchableOpacity style={s.cancelBtn} onPress={() => handleCancelJob('full_fundamentals_sync')}>
                      <Text style={s.cancelBtnText}>■ Stop</Text>
                    </TouchableOpacity>
                  ) : (
                    <TouchableOpacity
                      style={[s.fullSyncBtn, !!runningJob && s.runBtnDisabled]}
                      onPress={() => handleFullSync('full_fundamentals_sync')}
                      disabled={!!runningJob}
                    >
                      <Text style={s.fullSyncBtnText}>⬇ Full Sync</Text>
                    </TouchableOpacity>
                  )}
                </View>
              )}

              {/* Expand Filter Details */}
              <TouchableOpacity style={s.expandBtn} onPress={() => toggleExpand(step.step)}>
                <Text style={s.expandText}>Filter details</Text>
                <Ionicons name={isExpanded ? 'chevron-up' : 'chevron-down'} size={12} color={COLORS.textMuted} />
              </TouchableOpacity>

              {isExpanded && (
                <View style={s.details}>
                  <Text style={s.detailLabel}>API ENDPOINT</Text>
                  <Text style={s.apiText}>· {step.apiUrl}</Text>
                  <Text style={[s.detailLabel, { marginTop: 8 }]}>EXCLUDED IF</Text>
                  {step.filters.map(f => (
                    <Text key={f} style={s.filterText}>✕ {f}</Text>
                  ))}
                </View>
              )}
            </View>

            {/* Arrow between steps */}
            {idx < steps.length - 1 && (
              <View style={s.arrow}>
                <View style={s.arrowLine} />
                <Ionicons name="chevron-down" size={12} color={COLORS.textMuted} />
              </View>
            )}
          </View>
        );
      })}

      {/* Exclusion Report */}
      <View style={s.reportCard}>
        <View style={s.reportHeader}>
          <View>
            <Text style={s.reportTitle}>Filtered-out tickers report</Text>
            <Text style={s.reportMeta}>
              {exclusionReport
                ? `${exclusionReport.report_date} · ${fmt(exclusionReport.total_rows)} rows`
                : 'No report generated yet'}
            </Text>
          </View>
          <TouchableOpacity
            style={[
              s.downloadBtn,
              (!hasExclusionRows || downloadingReport) && s.downloadBtnDisabled,
            ]}
            onPress={handleDownloadExclusionReport}
            disabled={!exclusionReport || downloadingReport || !hasExclusionRows}
          >
            {downloadingReport
              ? <ActivityIndicator size="small" color="#fff" />
              : <Text style={[s.downloadBtnText, !hasExclusionRows && s.downloadBtnTextDisabled]}>Download CSV</Text>}
          </TouchableOpacity>
        </View>

        {exclusionReport?.rows?.length ? (
          <View style={s.reportList}>
            <View style={s.reportListHeader}>
              <Text style={[s.reportHeadCell, { flex: 1.1 }]}>Ticker</Text>
              <Text style={[s.reportHeadCell, { flex: 2.2 }]}>Name</Text>
              <Text style={[s.reportHeadCell, { flex: 1.7 }]}>Reason</Text>
            </View>
            {exclusionReport.rows.slice(0, 8).map((row, idx) => (
              <View key={`${row.ticker}-${row.reason}-${idx}`} style={s.reportListRow}>
                <Text style={[s.reportCellTicker, { flex: 1.1 }]}>{row.ticker}</Text>
                <Text style={[s.reportCell, { flex: 2.2 }]} numberOfLines={1}>{row.name}</Text>
                <Text style={[s.reportCell, { flex: 1.7 }]} numberOfLines={1}>{row.reason}</Text>
              </View>
            ))}
            <Text style={s.reportFootnote}>Showing first 8 rows from latest report</Text>
          </View>
        ) : (
          <Text style={s.reportEmpty}>
            {exclusionReport?.empty_report_hint || 'No report rows yet. Run Step 1 (Universe Seed) to generate filtered-out rows.'}
          </Text>
        )}
      </View>

      {/* Data Completeness Dashboard */}
      <View style={s.syncCard}>
        <Text style={s.syncTitle}>Data Completeness</Text>

        {/* Price History */}
        <View style={s.syncRow}>
          <View style={s.syncLabelRow}>
            <Text style={s.syncLabel}>Price History</Text>
            <Text style={s.syncCount}>
              {fmt(syncStatus.price_history_complete ?? 0)} / {fmt(syncStatus.total_visible_tickers ?? 0)}
              {(syncStatus.price_history_pct !== undefined) ? `  ${syncStatus.price_history_pct}%` : ''}
            </Text>
          </View>
          <View style={s.syncBarBg}>
            <View style={[s.syncBarFill, {
              width: `${Math.min(syncStatus.price_history_pct ?? 0, 100)}%` as any,
              backgroundColor: (syncStatus.price_history_pct ?? 0) >= 100 ? '#22C55E' : '#10B981',
            }]} />
          </View>
          {(syncStatus.needs_price_redownload ?? 0) > 0 && (
            <Text style={s.syncQueueText}>
              ⚠ {fmt(syncStatus.needs_price_redownload)} pending re-download (splits)
            </Text>
          )}
        </View>

        {/* Fundamentals */}
        <View style={[s.syncRow, { marginTop: 10 }]}>
          <View style={s.syncLabelRow}>
            <Text style={s.syncLabel}>Fundamentals</Text>
            <Text style={s.syncCount}>
              {fmt(syncStatus.fundamentals_complete ?? 0)} / {fmt(syncStatus.total_visible_tickers ?? 0)}
              {(syncStatus.fundamentals_pct !== undefined) ? `  ${syncStatus.fundamentals_pct}%` : ''}
            </Text>
          </View>
          <View style={s.syncBarBg}>
            <View style={[s.syncBarFill, {
              width: `${Math.min(syncStatus.fundamentals_pct ?? 0, 100)}%` as any,
              backgroundColor: (syncStatus.fundamentals_pct ?? 0) >= 100 ? '#22C55E' : '#F59E0B',
            }]} />
          </View>
          {(syncStatus.needs_fundamentals_refresh ?? 0) > 0 && (
            <Text style={s.syncQueueText}>
              🔄 {fmt(syncStatus.needs_fundamentals_refresh)} pending refresh (events)
            </Text>
          )}
        </View>

        {/* API Credits */}
        <View style={[s.syncRow, { marginTop: 10 }]}>
          <View style={s.syncLabelRow}>
            <Text style={s.syncLabel}>API Credits Today</Text>
            <Text style={s.syncCount}>
              {fmt(syncStatus.credits_today ?? 0)} / {fmt(syncStatus.credits_limit ?? 100000)}
              {(syncStatus.credits_pct !== undefined) ? `  ${syncStatus.credits_pct}%` : ''}
            </Text>
          </View>
          <View style={s.syncBarBg}>
            <View style={[s.syncBarFill, {
              width: `${Math.min(syncStatus.credits_pct ?? 0, 100)}%` as any,
              backgroundColor: (syncStatus.credits_pct ?? 0) >= 90 ? '#EF4444' : (syncStatus.credits_pct ?? 0) >= 70 ? '#F59E0B' : '#6366F1',
            }]} />
          </View>
        </View>
      </View>

      {/* Morning Fresh — independent job, not part of universe pipeline */}
      <View style={[s.stepCard, { marginTop: 16, borderLeftColor: '#06B6D4', borderLeftWidth: 3 }]}>
        <View style={s.stepHeader}>
          <View style={[s.stepBadge, { backgroundColor: '#06B6D422' }]}>
            <Ionicons name="newspaper-outline" size={16} color="#06B6D4" />
          </View>
          <View style={s.stepMeta}>
            <View style={s.stepTitleRow}>
              <Text style={s.stepTitle}>Morning Fresh</Text>
              {(() => {
                const run = jobRuns['news_refresh'];
                return run ? (
                  <Ionicons name={getStatusIcon(run.status) as any} size={14} color={getStatusColor(run.status)} style={{ marginLeft: 4 }} />
                ) : null;
              })()}
            </View>
            <Text style={s.stepSchedule}>Daily 13:00 Prague</Text>
          </View>
          <TouchableOpacity
            style={[s.runBtn, { backgroundColor: '#06B6D4' }, runningJob === 'news_refresh' && s.runBtnDisabled]}
            onPress={() => handleRunNow('news_refresh')}
            disabled={!!runningJob}
          >
            {runningJob === 'news_refresh'
              ? <ActivityIndicator size="small" color="#fff" />
              : <Text style={s.runBtnText}>▶ Run</Text>
            }
          </TouchableOpacity>
        </View>
        {jobRuns['news_refresh'] ? (
          <View style={s.runInfo}>
            <Text style={s.runValue}>
              Last: {formatTime(jobRuns['news_refresh'].start_time)} · {formatDuration(jobRuns['news_refresh'].duration_seconds)}
            </Text>
          </View>
        ) : (
          <Text style={s.neverRun}>Never run</Text>
        )}
        <Text style={[s.detailValue, { marginTop: 4 }]}>Fetch news + compute sentiment for tracked tickers</Text>
        <Text style={s.apiText}>· https://eodhd.com/api/news (per tracked ticker)</Text>
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
  healthSub: { fontSize: 11, color: COLORS.textMuted, marginBottom: 10 },
  schedulerControlRow: { marginBottom: 10, gap: 8 },
  schedulerControlText: { fontSize: 11, color: COLORS.textMuted },
  schedulerBtn: { alignSelf: 'flex-start', paddingHorizontal: 12, paddingVertical: 7, borderRadius: 7, minWidth: 140, alignItems: 'center' },
  schedulerPauseBtn: { backgroundColor: '#EF4444' },
  schedulerResumeBtn: { backgroundColor: '#22C55E' },
  schedulerBtnDisabled: { opacity: 0.6 },
  schedulerBtnText: { color: '#fff', fontSize: 11, fontWeight: '700' },

  miniSummary: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', marginTop: 4, paddingTop: 10, borderTopWidth: 1, borderTopColor: COLORS.border },
  miniItem: { alignItems: 'center', flex: 1 },
  miniNum: { fontSize: 14, fontWeight: '700', color: COLORS.text },
  miniLabel: { fontSize: 9, color: COLORS.textMuted, marginTop: 1 },

  stepCard: { marginHorizontal: 12, marginBottom: 0, backgroundColor: COLORS.card, borderRadius: 10, padding: 12, borderWidth: 1, borderColor: COLORS.border },
  stepHeader: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  stepBadge: { width: 32, height: 32, borderRadius: 8, alignItems: 'center', justifyContent: 'center' },
  stepMeta: { flex: 1 },
  stepTitleRow: { flexDirection: 'row', alignItems: 'center', gap: 4 },
  stepNum: { fontSize: 9, fontWeight: '700', color: COLORS.textMuted, letterSpacing: 0.5 },
  stepTitle: { fontSize: 13, fontWeight: '600', color: COLORS.text },
  stepSchedule: { fontSize: 10, color: COLORS.textMuted, marginTop: 1 },

  jobBtnGroup: { flexDirection: 'row', gap: 6 },
  runBtn: { paddingHorizontal: 10, paddingVertical: 5, borderRadius: 6, minWidth: 54, alignItems: 'center' },
  runBtnDisabled: { opacity: 0.5 },
  runBtnText: { color: '#fff', fontSize: 11, fontWeight: '600' },
  cancelBtn: { paddingHorizontal: 10, paddingVertical: 5, borderRadius: 6, minWidth: 54, alignItems: 'center', backgroundColor: '#EF4444' },
  cancelBtnText: { color: '#fff', fontSize: 11, fontWeight: '600' },
  runResultText: { fontSize: 11, marginTop: 6, color: COLORS.textMuted },
  progressRow: { flexDirection: 'row', alignItems: 'center', marginTop: 6, gap: 8 },
  progressText: { flex: 1, fontSize: 11, color: '#F59E0B' },
  elapsedText: { fontSize: 13, fontWeight: '700', color: '#F59E0B', minWidth: 36, textAlign: 'right' },

  funnelRow: { flexDirection: 'row', alignItems: 'center', marginTop: 12, marginBottom: 4, gap: 6 },
  funnelBox: { alignItems: 'center', flex: 2.5, backgroundColor: COLORS.border + '44', borderRadius: 8, paddingVertical: 8, paddingHorizontal: 4 },
  funnelBoxOut: { borderWidth: 1 },
  funnelBoxNum: { fontSize: 18, fontWeight: '800', color: COLORS.text },
  funnelBoxLabel: { fontSize: 9, color: COLORS.textMuted, marginTop: 2, textAlign: 'center' },
  funnelArrow: { alignItems: 'center', flex: 1 },
  funnelPct: { fontSize: 9, fontWeight: '700', marginTop: 2 },
  funnelDropped: { alignItems: 'center', flex: 1.8, backgroundColor: '#EF444414', borderRadius: 8, paddingVertical: 8, paddingHorizontal: 4, borderWidth: 1, borderColor: '#EF444433' },
  funnelDropNum: { fontSize: 14, fontWeight: '700', color: '#EF4444' },
  funnelDropLabel: { fontSize: 9, color: '#EF4444', marginTop: 1 },

  waitingRow: { flexDirection: 'row', alignItems: 'center', gap: 6, marginTop: 10, marginBottom: 6, paddingVertical: 8, paddingHorizontal: 10, backgroundColor: COLORS.border + '33', borderRadius: 8 },
  waitingText: { fontSize: 11, color: COLORS.textMuted, fontStyle: 'italic' },
  funnelBarWrap: { height: 4, backgroundColor: COLORS.border, borderRadius: 2, overflow: 'hidden', marginBottom: 8 },
  funnelBar: { height: 4, borderRadius: 2 },

  runInfo: { marginTop: 6, paddingTop: 8, borderTopWidth: 1, borderTopColor: COLORS.border },
  runInfoRow: { flexDirection: 'row', gap: 6, marginBottom: 2 },
  runLabel: { fontSize: 11, color: COLORS.textMuted, width: 80 },
  runValue: { fontSize: 11, color: COLORS.text, flex: 1 },
  errorText: { fontSize: 11, color: '#EF4444', marginTop: 4 },
  neverRun: { fontSize: 11, color: COLORS.textMuted, marginTop: 8, fontStyle: 'italic' },
  substepsCard: {
    marginTop: 8,
    borderWidth: 1,
    borderColor: COLORS.border,
    borderRadius: 8,
    paddingVertical: 6,
    paddingHorizontal: 8,
    backgroundColor: COLORS.border + '33',
  },
  substepsTitle: {
    fontSize: 10,
    fontWeight: '700',
    color: COLORS.textMuted,
    marginBottom: 4,
    textTransform: 'uppercase',
  },
  substepBlock: {
    marginTop: 6,
    marginBottom: 4,
    paddingBottom: 8,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border + '66',
  },
  substepMeta: {
    fontSize: 10,
    color: COLORS.textMuted,
    marginBottom: 2,
  },
  substepHeaderRow: { flexDirection: 'row', alignItems: 'center', gap: 6, marginBottom: 3 },
  substepName: { fontSize: 11, fontWeight: '600', color: COLORS.text, flex: 1 },
  substepDesc: { fontSize: 10, color: COLORS.textMuted, marginBottom: 4, lineHeight: 14 },
  substepEndpoint: { fontSize: 9, color: '#6366F1', fontFamily: 'monospace', marginBottom: 4 },
  substepStatsRow: { flexDirection: 'row', alignItems: 'center', gap: 6 },
  substepStat: { alignItems: 'center', flex: 1 },
  substepStatNum: { fontSize: 14, fontWeight: '700', color: COLORS.text },
  substepStatLabel: { fontSize: 8, color: COLORS.textMuted, marginTop: 1 },
  substepStatSep: { fontSize: 11, color: COLORS.textMuted },
  substepTickers: { fontSize: 9, color: COLORS.textMuted, marginTop: 4, fontFamily: 'monospace' },
  substepRow: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', gap: 6, marginBottom: 2 },
  substepValue: { fontSize: 10, color: COLORS.textMuted },
  mockBadge: { backgroundColor: '#F59E0B33', paddingHorizontal: 5, paddingVertical: 1, borderRadius: 4 },
  mockBadgeText: { fontSize: 8, fontWeight: '700', color: '#F59E0B' },
  catchupBadge: { fontSize: 9, color: '#6366F1', marginBottom: 4, fontStyle: 'italic' },

  fullSyncBlock: { marginTop: 10, paddingTop: 10, borderTopWidth: 1, borderTopColor: COLORS.border, flexDirection: 'row', alignItems: 'center', gap: 10 },
  fullSyncInfo: { flex: 1 },
  fullSyncTitle: { fontSize: 11, fontWeight: '700', color: COLORS.text, marginBottom: 2 },
  fullSyncDesc: { fontSize: 10, color: COLORS.textMuted, lineHeight: 14 },
  fullSyncResult: { fontSize: 10, color: '#10B981', marginTop: 3 },
  fullSyncBtn: { backgroundColor: '#10B981', paddingHorizontal: 10, paddingVertical: 7, borderRadius: 6, alignItems: 'center', minWidth: 70 },
  fullSyncBtnText: { color: '#fff', fontSize: 11, fontWeight: '700' },

  syncCard: { marginHorizontal: 12, marginTop: 12, backgroundColor: COLORS.card, borderRadius: 10, padding: 12, borderWidth: 1, borderColor: COLORS.border },
  syncTitle: { fontSize: 12, fontWeight: '700', color: COLORS.text, marginBottom: 10 },
  syncRow: {},
  syncLabelRow: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 },
  syncLabel: { fontSize: 11, color: COLORS.text },
  syncCount: { fontSize: 10, color: COLORS.textMuted },
  syncBarBg: { height: 5, backgroundColor: COLORS.border, borderRadius: 3, overflow: 'hidden' },
  syncBarFill: { height: 5, borderRadius: 3 },
  syncQueueText: { fontSize: 9, color: '#F59E0B', marginTop: 3 },

  expandBtn: { flexDirection: 'row', alignItems: 'center', gap: 4, marginTop: 8, alignSelf: 'flex-start' },
  expandText: { fontSize: 11, color: COLORS.textMuted },
  details: { marginTop: 8, paddingTop: 8, borderTopWidth: 1, borderTopColor: COLORS.border },
  detailLabel: { fontSize: 10, fontWeight: '700', color: COLORS.textMuted, letterSpacing: 0.5, marginBottom: 3 },
  detailValue: { fontSize: 11, color: COLORS.text, marginBottom: 2 },
  apiText: { fontSize: 10, color: '#6366F1', fontFamily: 'monospace', marginBottom: 1 },
  filterText: { fontSize: 10, color: '#EF4444', marginBottom: 1 },

  reportCard: { marginHorizontal: 12, marginTop: 12, backgroundColor: COLORS.card, borderRadius: 10, padding: 12, borderWidth: 1, borderColor: COLORS.border },
  reportHeader: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', gap: 10 },
  reportTitle: { fontSize: 12, fontWeight: '700', color: COLORS.text },
  reportMeta: { fontSize: 10, color: COLORS.textMuted, marginTop: 2 },
  downloadBtn: { backgroundColor: '#6366F1', paddingHorizontal: 10, paddingVertical: 6, borderRadius: 6 },
  downloadBtnText: { color: '#fff', fontSize: 10, fontWeight: '700' },
  downloadBtnDisabled: { backgroundColor: COLORS.border },
  downloadBtnTextDisabled: { color: COLORS.textMuted },
  reportList: { marginTop: 10, borderWidth: 1, borderColor: COLORS.border, borderRadius: 8, overflow: 'hidden' },
  reportListHeader: { flexDirection: 'row', backgroundColor: COLORS.border + '55', paddingHorizontal: 8, paddingVertical: 6 },
  reportHeadCell: { fontSize: 9, fontWeight: '700', color: COLORS.textMuted, textTransform: 'uppercase' },
  reportListRow: { flexDirection: 'row', paddingHorizontal: 8, paddingVertical: 6, borderTopWidth: 1, borderTopColor: COLORS.border + '66' },
  reportCell: { fontSize: 10, color: COLORS.text },
  reportCellTicker: { fontSize: 10, color: '#6366F1', fontWeight: '700' },
  reportFootnote: { fontSize: 9, color: COLORS.textMuted, paddingHorizontal: 8, paddingVertical: 6, borderTopWidth: 1, borderTopColor: COLORS.border + '66' },
  reportEmpty: { fontSize: 10, color: COLORS.textMuted, fontStyle: 'italic', marginTop: 8 },

  arrow: { alignItems: 'center', paddingVertical: 4 },
  arrowLine: { width: 1, height: 8, backgroundColor: COLORS.border },
});
