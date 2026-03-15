/**
 * RICHSTOX Admin Pipeline
 * Universe Pipeline — 5-step sequential process with integrated funnel per step
 */

import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import {
  View, Text, ScrollView, TouchableOpacity, StyleSheet,
  RefreshControl, ActivityIndicator, Alert, Linking, Platform, TextInput,
} from 'react-native';
import { Ionicons } from '@expo/vector-icons';
import { COLORS } from '../_layout';
import { authenticatedFetch } from '../../utils/api_client';

const API_URL = process.env.EXPO_PUBLIC_BACKEND_URL || '';

interface PipelineProps {
  sessionToken: string | null;
}

interface JobRun {
  status: string;
  start_time?: string;
  end_time?: string;
  last_run_finished?: string;
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
  verified_through_date?: string;
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
      // Primary (canonical) names
      raw?: number;
      seeded?: number;
      with_price?: number;
      classified?: number;
      visible?: number;
      // Backward-compat aliases
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
  latest_run_id_per_step?: Record<string, string> | null;
  step1_counts?: {
    raw_distinct?: number;
    seeded_count?: number;
    filtered_out_total_step1?: number;
    fetched_raw_per_exchange?: Record<string, number>;
    run_id?: string;
  } | null;
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

function formatElapsed(seconds: number): string {
  return seconds < 60
    ? `${seconds}s`
    : `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
}

function formatDuration(sec?: number): string {
  if (sec === undefined || sec === null) return '';
  const total = Math.round(sec);
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const seconds = total % 60;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

function formatTime(iso?: string): string {
  if (!iso) return 'Never';
  try {
    let s = iso;
    if (s && !s.endsWith('Z') && !/[+-]\d{2}:\d{2}$/.test(s) && !/[+-]\d{4}$/.test(s)) {
      s = s + 'Z';
    }
    const d = new Date(s);
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

function extractDayProgress(message: string | undefined): string | null {
  if (!message) return null;
  const match = message.match(/\((\d+)\/(\d+)\)/);
  if (!match) return null;
  return `Day ${match[1]}/${match[2]}`;
}

function normaliseRun(run: any): any {
  if (!run) return run;
  const start = run.started_at || run.start_time;
  const finish = run.finished_at || run.end_time || run.last_run_finished;
  const details = run.details || {};
  const seededTotal = run.progress_total ?? details.seeded_total ?? details.tickers_seeded_total;
  const processedCount =
    run.progress_processed ??
    details.tickers_with_price_data ??
    details.records_upserted ??
    run.records_processed;
  // Explicit phase field; fall back to inferring from progress message prefix
  // so the label updates even when the overview aggregation omits top-level phase.
  const progressStr: string = run.progress || '';
  const inferredPhase = progressStr.startsWith('2.1') ? '2.1_bulk_catchup'
    : progressStr.startsWith('2.2') ? '2.2_split'
    : progressStr.startsWith('2.4') ? '2.4_dividend'
    : (progressStr.startsWith('2.6') || progressStr.startsWith('2.7')) ? '2.6_earnings'
    : undefined;
  const phase = run.phase ?? details.phase ?? inferredPhase;
  const durationSeconds = run.duration_seconds ?? (
    start && finish ? Math.max(0, Math.round((Date.parse(finish) - Date.parse(start)) / 1000)) : undefined
  );
  return {
    ...run,
    start_time: start,
    end_time: finish,
    started_at: start,
    finished_at: finish,
    duration_seconds: durationSeconds,
    progress_total: seededTotal,
    progress_processed: processedCount,
    progress_pct: run.progress_pct ?? (seededTotal ? Math.min(Math.round((processedCount ?? 0) / seededTotal * 100), 100) : undefined),
    records_processed: processedCount,
    last_run_finished: finish ?? run.last_run_finished,
    phase,
    details: {
      ...details,
      seeded_total: seededTotal ?? details.seeded_total,
      tickers_with_price_data: details.tickers_with_price_data ?? processedCount,
      records_upserted: details.records_upserted ?? run.records_upserted,
      phase,
    },
  };
}

function deriveProgress(run: any) {
  if (!run) return null;
  const normalized = normaliseRun(run);
  const total = normalized.progress_total;
  const processed = normalized.progress_processed ?? 0;
  const pct = total ? Math.min(Math.round((processed / total) * 100), 100) : (processed ? 100 : 0);
  // Auto-advance past 2.1@100%: when bulk sync is done the backend transitions
  // to 2.2 but the frontend poll may not catch it before completion.
  const effectivePhase = (normalized.phase === '2.1_bulk_catchup' && pct >= 100)
    ? '2.2_split'
    : normalized.phase;
  return {
    processed,
    total,
    pct,
    phase: effectivePhase,
    message: normalized.progress,
  };
}

export default function PipelineTab({ sessionToken }: PipelineProps) {
  const [data, setData] = useState<OverviewData | null>(null);
  const [exclusionReport, setExclusionReport] = useState<PipelineExclusionReport | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [downloadingReport, setDownloadingReport] = useState(false);
  const [schedulerUpdating, setSchedulerUpdating] = useState(false);
  const [liveLastRuns, setLiveLastRuns] = useState<Record<string, any>>({});
  const [expandedSteps, setExpandedSteps] = useState<Set<number>>(new Set());
  const [elapsedSeconds, setElapsedSeconds] = useState(0);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null); // chain run elapsed timer

  // ── Data Freshness ────────────────────────────────────────────────────────
  const [freshness, setFreshness] = useState<Record<string, any> | null>(null);

  // ── Step 1 universe seed progress ────────────────────────────────────────
  const [step1Progress, setStep1Progress] = useState<{processed: number; total: number; pct: number} | null>(null);

  // ── Step 2 price sync progress ────────────────────────────────────────────
  const [step2Progress, setStep2Progress] = useState<{processed: number; total: number; pct: number; phase?: string; message?: string} | null>(null);

  // ── Per-ticker audit state ────────────────────────────────────────────────
  const [auditTicker, setAuditTicker] = useState('');
  const [auditLive, setAuditLive] = useState(false);
  const [auditLoading, setAuditLoading] = useState(false);
  const [auditResult, setAuditResult] = useState<Record<string, any> | null>(null);

  // ── Full pipeline chain run state ─────────────────────────────────────────
  const [chainRunId, setChainRunId] = useState<string | null>(null);
  const [chainStatus, setChainStatus] = useState<string | null>(null);
  const [chainRunning, setChainRunning] = useState(false);
  const [chainCurrentStep, setChainCurrentStep] = useState<number | null>(null);
  const [chainStepsDone, setChainStepsDone] = useState<number[]>([]);
  const chainPollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // ── Manual / Auto run mode toggle ─────────────────────────────────────────
  const [runMode, setRunMode] = useState<'MANUAL' | 'AUTO'>('MANUAL');
  const runModeInitialised = useRef(false);

  const fetchData = useCallback(async () => {
    try {
      const [overviewRes, exclusionRes, freshnessRes] = await Promise.all([
        authenticatedFetch(`${API_URL}/api/admin/overview`, {}, sessionToken),
        authenticatedFetch(`${API_URL}/api/admin/pipeline/exclusion-report?limit=20`, {}, sessionToken),
        authenticatedFetch(`${API_URL}/api/admin/pipeline/data-freshness`, {}, sessionToken),
      ]);

      if (overviewRes.ok) setData(await overviewRes.json());
      if (freshnessRes.ok) setFreshness(await freshnessRes.json());
      if (exclusionRes.ok) {
        const excl = await exclusionRes.json();
        // Fetch step1_counts by re-querying with the latest Step 1 run_id so
        // seededFromRun is available without backend changes to the default path.
        const step1RunId: string | undefined =
          (excl as any)?.latest_run_id_per_step?.['Step 1 - Universe Seed'];
        if (step1RunId) {
          try {
              const s1Res = await authenticatedFetch(
              `${API_URL}/api/admin/pipeline/exclusion-report?run_id=${encodeURIComponent(step1RunId)}&limit=1`,
              {},
              sessionToken,
            );
            if (s1Res.ok) {
              const s1Data = await s1Res.json();
              excl.step1_counts = s1Data.step1_counts ?? null;
            }
          } catch (_) { /* non-fatal */ }
        }
        setExclusionReport(excl);
      }
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
        const response = await authenticatedFetch(downloadUrl, {}, sessionToken);
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

  // Set initial runMode once data is loaded for the first time.
  useEffect(() => {
    if (!runModeInitialised.current && typeof schedulerActive === 'boolean') {
      setRunMode(schedulerActive ? 'AUTO' : 'MANUAL');
      runModeInitialised.current = true;
    }
  }, [schedulerActive]);

  const handleSchedulerToggle = async () => {
    if (schedulerUpdating) return;
    if (typeof schedulerActive !== 'boolean') {
      Alert.alert('Scheduler status unavailable', 'Please refresh and try again.');
      return;
    }
    setSchedulerUpdating(true);
    try {
      const targetEnabled = !schedulerActive;
      const res = await authenticatedFetch(
        `${API_URL}/api/admin/scheduler/kill-switch?enabled=${targetEnabled ? 'true' : 'false'}`,
        { method: 'POST' },
        sessionToken,
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

  const handleRunAudit = async () => {
    const raw = auditTicker.trim().toUpperCase();
    if (!raw) return;
    // Append .US if the input has no dot (e.g. "EFT" → "EFT.US", "EFT.US" stays)
    const t = raw.includes('.') ? raw : `${raw}.US`;
    setAuditLoading(true);
    setAuditResult(null);
    try {
      const res = await authenticatedFetch(
        `${API_URL}/api/admin/ticker/${t}/fundamentals-audit?live=${auditLive ? 1 : 0}`,
        {},
        sessionToken,
      );
      if (res.ok) {
        setAuditResult(await res.json());
      } else {
        setAuditResult({ error: `HTTP ${res.status}` });
      }
    } catch (e: any) {
      setAuditResult({ error: e.message });
    } finally {
      setAuditLoading(false);
    }
  };

  const CHAIN_STEP_NAMES: Record<number, string> = {
    1: 'Universe Seed',
    2: 'Price Sync',
    3: 'Fundamentals & Visibility',
  };

   const handleRunFullPipeline = async () => {
    if (chainRunning) return;
    setChainRunning(true);
    setChainStatus('starting');
    setChainRunId(null);
    setChainCurrentStep(null);
    setChainStepsDone([]);
    setElapsedSeconds(0);
    try {
      const res = await authenticatedFetch(
        `${API_URL}/api/admin/pipeline/run-full-now`,
        { method: 'POST', headers: { 'Content-Type': 'application/json' } },
        sessionToken,
      );
      const data = await res.json();
      if (!res.ok) { setChainStatus('error'); setChainRunning(false); return; }
      const cid: string = data.chain_run_id;
      setChainRunId(cid);
      setChainStatus('running');
      setChainCurrentStep(1);
      // Start elapsed timer for chain run
      if (timerRef.current) { clearInterval(timerRef.current); timerRef.current = null; }
      const chainStartedAt = Date.now();
      timerRef.current = setInterval(() => {
        setElapsedSeconds(Math.round((Date.now() - chainStartedAt) / 1000));
      }, 1000);
      // Poll chain status every 2 s until completed/failed/cancelled.
      if (chainPollRef.current) clearInterval(chainPollRef.current);
      chainPollRef.current = setInterval(async () => {
        try {
          const sr = await authenticatedFetch(
            `${API_URL}/api/admin/pipeline/chain-status/${cid}`,
            {},
            sessionToken,
          );
          const sd = await sr.json();
          setChainStatus(sd.status);
          setChainCurrentStep(sd.current_step ?? null);
          setChainStepsDone(sd.steps_done ?? []);
          // Refresh overview on every tick so "Last run" / counts stay live.
          fetchData();
          // While Step 1 is the active step, poll its progress for the live bar.
          // Also accept 'completed' status so the final 100% state is picked up
          // before the chain advances to current_step=2.
          if (sd.current_step === 1) {
            try {
              const jr = await authenticatedFetch(
                `${API_URL}/api/admin/jobs/universe_seed/status`,
                {},
                sessionToken,
              );
              if (jr.ok) {
                const jd = await jr.json();
                const lr = jd.last_run;
                if (lr?.progress_total) {
                  setStep1Progress({
                    processed: lr.progress_processed || 0,
                    total:     lr.progress_total,
                    pct:       lr.progress_pct || 0,
                  });
                }
              }
            } catch { /* non-fatal */ }
          }
          // While Step 2 is the active step, poll its progress for the live bar.
          if (sd.current_step === 2) {
            try {
              const jr = await authenticatedFetch(
                `${API_URL}/api/admin/job/price_sync/status`,
                {},
                sessionToken,
              );
              if (jr.ok) {
                const jd = await jr.json();
                const lr = normaliseRun(jd.last_run);
                if (lr) {
                  if (jd.previous_completed_run) {
                    lr.previous_completed_run = jd.previous_completed_run;
                  }
                  setLiveLastRuns(prev => ({ ...prev, price_sync: lr }));
                  const progress = deriveProgress(lr);
                  if (progress) setStep2Progress(progress);
                }
              }
            } catch { /* non-fatal */ }
          }
          if (sd.status === 'completed' || sd.status === 'failed' || sd.status === 'cancelled') {
            clearInterval(chainPollRef.current!);
            chainPollRef.current = null;
            if (timerRef.current) { clearInterval(timerRef.current); timerRef.current = null; }
            setElapsedSeconds(0);
            setChainRunning(false);
            // Freeze final progress bar states
            if (sd.status === 'completed') {
              if (sd.steps_done?.includes(1)) {
                setStep1Progress(prev => prev ? { processed: prev.total, total: prev.total, pct: 100 } : null);
              }
              if (sd.steps_done?.includes(2)) {
                setStep2Progress(prev => prev ? { ...prev, pct: 100, phase: 'completed' } : null);
              }
            }
            // Delayed refresh so backend has time to flush final run timestamps
            setTimeout(() => fetchData(), 3000);
          }
        } catch { /* keep polling */ }
      }, 2000);
    } catch (e: any) {
      setChainStatus('error');
      if (timerRef.current) { clearInterval(timerRef.current); timerRef.current = null; }
      setElapsedSeconds(0);
      setChainRunning(false);
    }
  };

  const handleStopChain = async () => {
    if (!chainRunId) return;
    try {
      await authenticatedFetch(
        `${API_URL}/api/admin/pipeline/chain-cancel/${chainRunId}`,
        { method: 'POST' },
        sessionToken,
      );
    } catch { /* ignore network error — polling will detect cancellation */ }
    if (chainPollRef.current) {
      clearInterval(chainPollRef.current);
      chainPollRef.current = null;
    }
    if (timerRef.current) {
      clearInterval(timerRef.current);
      timerRef.current = null;
    }
    setElapsedSeconds(0);
    setChainRunning(false);
    setChainStatus('cancelled');
    setChainCurrentStep(null);
    // Immediately show Stopped for any in-progress step 2 progress
    setStep2Progress(prev => prev ? { ...prev, phase: 'stopped' } : null);
  };

  const handleDownloadFullCsv = () => {
    if (!chainRunId || chainStatus !== 'completed') return;
    const url = `${API_URL}/api/admin/pipeline/export/full?chain_run_id=${encodeURIComponent(chainRunId)}`;
    if (typeof window !== 'undefined') {
      const link = window.document.createElement('a');
      link.href = url;
      link.download = `pipeline_full_${chainRunId}.csv`;
      window.document.body.appendChild(link);
      link.click();
      link.remove();
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

  const jobRunsRaw = data?.job_last_runs || {};
  const jobRuns = useMemo(() => {
    const merged: Record<string, any> = { ...jobRunsRaw };
    Object.entries(liveLastRuns).forEach(([k, v]) => { merged[k] = v; });
    const normalized: Record<string, any> = {};
    Object.entries(merged).forEach(([k, v]) => { normalized[k] = normaliseRun(v); });
    return normalized;
  }, [jobRunsRaw, liveLastRuns]);
  useEffect(() => {
    const lr = jobRuns['price_sync'];
    if (lr) {
      setStep2Progress(deriveProgress(lr));
    }
    const s1 = jobRuns['universe_seed'];
    if (s1) {
      setStep1Progress(deriveProgress(s1));
    }
  }, [jobRuns]);
  const counts = data?.universe_funnel?.counts || {};
  const syncStatus = data?.pipeline_sync_status || {};
  const todayStr = new Date().toISOString().split('T')[0];

  const rawSymbols = (jobRuns['universe_seed'] as any)?.raw_symbols_fetched
    ?? (jobRuns['universe_seed'] as any)?.details?.raw_symbols_fetched as number | undefined;
  const rawPerExchange = (jobRuns['universe_seed'] as any)?.fetched_raw_per_exchange
    ?? (jobRuns['universe_seed'] as any)?.details?.fetched_raw_per_exchange
    ?? exclusionReport?.step1_counts?.fetched_raw_per_exchange as Record<string, number> | undefined;
  const seededFromRun = (jobRuns['universe_seed'] as any)?.progress_total as number | undefined
    || (jobRuns['universe_seed'] as any)?.details?.seeded_total as number | undefined;
  // Prefer canonical 'seeded' field; fall back to legacy alias and run-derived value.
  const seeded = seededFromRun ?? counts.seeded ?? counts.seeded_us_total;
  const withPriceFromRun = (jobRuns['price_sync'] as any)?.details?.tickers_with_price_data as number | undefined
    || (jobRuns['price_sync'] as any)?.progress_processed as number | undefined;
  // Prefer canonical 'with_price' field; fall back to legacy alias and run-derived value.
  const withPrice = withPriceFromRun ?? counts.with_price ?? counts.with_price_data;
  // Prefer canonical 'classified' field; fall back to legacy alias.
  const withClass = counts.classified ?? counts.with_classification;
  // Prefer canonical 'visible' field; fall back to legacy alias.
  const visible = counts.visible ?? counts.visible_tickers;

  // Exclusion-report filtered_out counts — authoritative source for funnel arithmetic.
  const byStep = (exclusionReport as any)?.by_step as Record<string, number> | undefined;
  const step1Filtered = byStep?.['Step 1 - Universe Seed'];
  const step2Filtered = byStep?.['Step 2 - Price Sync'];
  const step3Filtered = byStep?.['Step 3 - Fundamentals Sync'];

  // s1In: total rows in Step 1 export = seeded + filtered = authoritative raw count.
  // Computed as seeded_count + step1Filtered so it matches the CSV row count exactly.
  const _s1Seeded = (exclusionReport?.step1_counts?.seeded_count as number | undefined);
  const s1In: number | undefined =
    _s1Seeded !== undefined && step1Filtered !== undefined
      ? _s1Seeded + step1Filtered
      : rawSymbols;

  // Arithmetic chain: Output = Input − FilteredOut. Each step chains from previous.
  const s1Out: number | undefined =
    s1In !== undefined && step1Filtered !== undefined ? s1In - step1Filtered
    : (exclusionReport?.step1_counts?.seeded_count as number | undefined) ?? seeded;
  const s2Out: number | undefined =
    s1Out !== undefined && step2Filtered !== undefined ? s1Out - step2Filtered : withPrice;
  // Step 3 now includes fundamentals + visibility gates (merged old steps 3+4)
  const s3Out: number | undefined =
    s2Out !== undefined && step3Filtered !== undefined ? s2Out - step3Filtered : visible;

  const JOB_OUTPUT: Record<string, number | undefined> = {
    universe_seed: s1Out,
    price_sync: s2Out,
    fundamentals_sync: s3Out,
    peer_medians: visible,
  };
  const completedCount = ['universe_seed', 'price_sync', 'fundamentals_sync', 'peer_medians'].filter(j => {
    const r = jobRuns[j];
    const ok = r?.status === 'success' || r?.status === 'completed';
    return ok && (JOB_OUTPUT[j] === undefined || (JOB_OUTPUT[j] ?? 0) > 0);
  }).length;
  const healthPct = Math.round((completedCount / 4) * 100);
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
      inputCount: s1In,
      outputCount: s1Out,
      droppedCount: step1Filtered,
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
      inputCount: s1Out,
      outputCount: s2Out,
      droppedCount: step2Filtered,
      outputLabel: 'with price data',
      filters: [
        'Not present in EODHD bulk response',
        'Close price = 0 or missing',
      ],
    },
    {
      step: 3,
      job_name: 'fundamentals_sync',
      title: 'Fundamentals & Visibility',
      schedule: 'After Step 2 completion',
      scheduledHour: 4,
      scheduledMinute: 30,
      icon: 'library-outline' as const,
      color: '#F59E0B',
      apiUrl: 'https://eodhd.com/api/fundamentals/{TICKER}.US  (~10 credits/ticker)',
      inputLabel: 'Tickers with prices',
      inputCount: s2Out,
      outputCount: s3Out,
      droppedCount: step3Filtered,
      outputLabel: 'visible',
      filters: [
        'EODHD returns no fundamentals (404)',
        'Sector missing or empty',
        'Industry missing or empty',
        'Ticker is delisted',
        'Shares outstanding missing or zero',
        'Financial currency missing',
      ],
    },
    {
      step: 4,
      job_name: 'peer_medians',
      title: 'Peer Medians',
      schedule: 'After Step 3 completion',
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

  const isRunDisabled = runMode === 'AUTO' || chainRunning;
  const isChainFailed = chainStatus === 'failed' || chainStatus === 'error';
  const isChainCancelled = chainStatus === 'cancelled';

  // Map job_name → chain step number for icon override.
  const CHAIN_STEP_FOR_JOB: Record<string, number> = {
    universe_seed: 1,
    price_sync: 2,
    fundamentals_sync: 3,
  };
  // Chain icon override is active when a chain is running or just finished.
  const chainIconActive = chainRunning || chainStatus === 'completed' || isChainFailed || isChainCancelled;

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

        {/* Full Pipeline Audit — above scheduler control */}
        <View style={s.fullChainInlineSection}>
          <View style={s.fullChainInlineTitleRow}>
            <View style={{ flex: 1 }}>
              <Text style={s.fullChainInlineTitle}>Full Pipeline Audit</Text>
              <Text style={s.fullChainInlineDesc} numberOfLines={1}>
                {runMode === 'AUTO' ? 'Scheduler controls automatic runs.' : 'Runs Step 1→4 now, generates unified CSV.'}
              </Text>
            </View>
            {/* MANUAL / AUTO toggle — aligned top-right */}
            <View style={s.manualAutoToggle}>
              <TouchableOpacity
                style={[s.toggleBtn, runMode === 'MANUAL' && s.toggleBtnActive]}
                onPress={() => setRunMode('MANUAL')}
              >
                <Text style={[s.toggleBtnText, runMode === 'MANUAL' && s.toggleBtnTextActive]}>MANUAL</Text>
              </TouchableOpacity>
              <TouchableOpacity
                style={[s.toggleBtn, runMode === 'AUTO' && s.toggleBtnActive]}
                onPress={() => setRunMode('AUTO')}
              >
                <Text style={[s.toggleBtnText, runMode === 'AUTO' && s.toggleBtnTextActive]}>AUTO</Text>
              </TouchableOpacity>
            </View>
          </View>
          <View style={{ flexDirection: 'column', width: '100%', marginTop: 8 }}>
  {chainRunning ? (
    <TouchableOpacity
      style={[s.fullChainBtn, { width: '100%', minHeight: 48, borderRadius: 8, justifyContent: 'center', backgroundColor: '#EF4444' }]}
      onPress={handleStopChain}
    >
      <Text style={[s.fullChainBtnText, { fontSize: 14 }]} numberOfLines={1}>■ Stop Chain Run</Text>
    </TouchableOpacity>
  ) : (
    <TouchableOpacity
      style={[s.fullChainBtn, { width: '100%', minHeight: 48, borderRadius: 8, justifyContent: 'center' }, isRunDisabled && s.runBtnDisabled]}
      onPress={handleRunFullPipeline}
      disabled={isRunDisabled}
    >
      <Text style={[s.fullChainBtnText, { fontSize: 14 }]} numberOfLines={1}>▶ Run Full Pipeline Now</Text>
    </TouchableOpacity>
  )}

  <TouchableOpacity
    style={[
      s.schedulerBtn,
      { width: '100%', minHeight: 48, borderRadius: 8, justifyContent: 'center', marginTop: 12, alignSelf: 'stretch' },
      schedulerActive ? s.schedulerPauseBtn : s.schedulerResumeBtn,
      (schedulerUpdating || typeof schedulerActive !== 'boolean') && s.schedulerBtnDisabled,
    ]}
    onPress={handleSchedulerToggle}
    disabled={schedulerUpdating || typeof schedulerActive !== 'boolean'}
  >
    {schedulerUpdating
      ? <ActivityIndicator size="small" color="#fff" />
      : <Text style={[s.schedulerBtnText, { fontSize: 14 }]} numberOfLines={1}>{schedulerActive ? 'Pause Scheduler' : 'Resume Scheduler'}</Text>}
  </TouchableOpacity>

  <Text style={[s.schedulerControlText, { marginTop: 8 }]}>
    Scheduler is currently {schedulerActive ? 'active' : 'paused'}.
  </Text>

  {chainRunId && chainStatus === 'completed' && (
    <TouchableOpacity
      style={[s.fullChainDownloadBtn, { marginTop: 12, width: '100%', minHeight: 48, borderRadius: 8, justifyContent: 'center' }]}
      onPress={handleDownloadFullCsv}
    >
      <Ionicons name="download-outline" size={13} color="#fff" />
      <Text style={s.fullChainDownloadBtnText}>Download Unified CSV</Text>
    </TouchableOpacity>
  )}

  {chainStatus && chainStatus !== 'starting' && (
    <Text style={[
      s.fullChainStatus,
      chainStatus === 'completed' ? { color: '#22C55E' }
      : isChainFailed ? { color: '#EF4444' }
      : isChainCancelled ? { color: '#6B7280' }
      : { color: '#F59E0B' },
      { marginTop: 6 },
    ]}>
      {chainStatus === 'completed'
        ? `Done — chain_run_id: ${chainRunId}`
        : isChainFailed
        ? 'Failed — check logs'
        : isChainCancelled
        ? 'Cancelled'
        : chainCurrentStep !== null
        ? `Running — Step ${chainCurrentStep}/3 (${CHAIN_STEP_NAMES[chainCurrentStep] ?? ''}) · ${formatElapsed(elapsedSeconds)}`
        : 'Running…'}
    </Text>
  )}
</View>
        </View>
        {/* Mini funnel summary */}
        <View style={s.miniSummary}>
          <View style={s.miniItem}>
            <Text style={s.miniNum}>{fmt(s1In)}</Text>
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

        // Chain icon override: when a chain run is active, derive icon state from chain progress.
        const chainStepNum = CHAIN_STEP_FOR_JOB[step.job_name];
        const chainStepDone = chainIconActive && chainStepNum !== undefined && chainStepsDone.includes(chainStepNum);
        const chainStepRunning = chainRunning && chainStepNum !== undefined && chainCurrentStep === chainStepNum;
        const chainStepPending = chainRunning && chainStepNum !== undefined && !chainStepDone && !chainStepRunning;

        const inCount = step.inputCount;
        const outCount = step.outputCount;
        const droppedCount = step.droppedCount !== undefined
          ? step.droppedCount
          : (inCount !== undefined && outCount !== undefined)
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
          ? ((run as any)?.details?.event_detectors
            || (data?.job_last_runs?.[step.job_name] as any)?.details?.event_detectors
            || {})
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
        const processedLabel = 'Processed:';

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
                    {chainStepRunning ? (
                      <>
                        <ActivityIndicator size="small" color="#F59E0B" style={{ marginLeft: 4 }} />
                        <Text style={{ marginLeft: 4, color: '#F59E0B', fontSize: 12 }}>
                          {formatElapsed(elapsedSeconds)}
                        </Text>
                      </>
                    ) : chainStepDone ? (
                      <Ionicons name="checkmark-circle" size={14} color="#22C55E" style={{ marginLeft: 4 }} />
                    ) : chainStepPending ? (
                      <Ionicons name="time-outline" size={14} color={COLORS.textMuted} style={{ marginLeft: 4 }} />
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

            </View>

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
              {run ? (() => {
                const runStart = run.started_at_prague || run.start_time || run.started_at;
                const runEnd = run.finished_at_prague || run.end_time || run.finished_at;
                const lastDuration = run.duration_seconds ?? (
                  runStart && runEnd ? Math.max(0, Math.round((Date.parse(runEnd) - Date.parse(runStart)) / 1000)) : undefined
                );
                const isLiveRun = run.status === 'running';
                const prevCompleted = run.previous_completed_run;
                const prevEnd = prevCompleted?.finished_at_prague || prevCompleted?.finished_at;
                const statusText = isLiveRun
                  ? (prevEnd
                    ? formatTime(prevEnd)
                    : `Started ${formatTime(runStart)}`)
                  : (runEnd || runStart)
                    ? formatTime(runEnd || runStart)
                    : '—';
                const prevDuration = prevCompleted?.duration_seconds;
                const durationText = isLiveRun
                  ? (prevEnd && prevDuration !== undefined && prevDuration !== null
                    ? ` (${formatDuration(prevDuration)})`
                    : chainStepRunning
                      ? ` · ${formatElapsed(elapsedSeconds)}`
                      : '')
                  : run.status === 'cancelled'
                    ? (lastDuration !== undefined ? ` (stopped after ${formatDuration(lastDuration)})` : '')
                    : lastDuration !== undefined
                      ? ` (${formatDuration(lastDuration)})`
                      : '';

                return (
                <View style={s.runInfo}>
                  <View style={s.runInfoRow}>
                    <Text style={s.runLabel}>Last run:</Text>
                    <Text style={[s.runValue, { color: getStatusColor(run.status) }]}>
                      {statusText}{durationText}
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
                      <Text style={s.runValue}>
                        {processedCount.toLocaleString()}
                        {step.job_name === 'universe_seed' && rawSymbols ? ` / ${rawSymbols.toLocaleString()}` : ''}
                      </Text>
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
                );
              })() : (
                <View style={s.runInfo}>
                  <View style={s.runInfoRow}>
                    <Text style={s.runLabel}>Next run:</Text>
                    <Text style={s.runValue}>{nextRunLabel}</Text>
                  </View>
                </View>
              )}

              {step.job_name === 'universe_seed' && (s1In !== undefined || rawPerExchange) && (
                <View style={s.substepsCard}>
                  <Text style={s.substepsTitle}>Step 1 raw breakdown</Text>
                  <View style={s.substepRow}>
                    <Text style={s.substepName}>Raw distinct (fetched)</Text>
                    <Text style={s.substepValue}>{s1In !== undefined ? fmt(s1In) : '—'}</Text>
                  </View>
                  {rawPerExchange && Object.entries(rawPerExchange).map(([exch, n]) => (
                    <View key={exch} style={s.substepRow}>
                      <Text style={s.substepName}>{exch} (raw before dedup)</Text>
                      <Text style={s.substepValue}>{fmt(n as number)}</Text>
                    </View>
                  ))}
                </View>
              )}

              {/* Step 1 seeding progress bar */}
              {step.job_name === 'universe_seed' && (() => {
                const seedRun = jobRuns['universe_seed'];
                const seedStatus = seedRun?.status;
                const isStep1Running = seedStatus === 'running';
                const seedRawFetched = rawSymbols;
                const seedProcessed = seedRun?.records_processed as number | undefined;
                if (isStep1Running && step1Progress !== null) {
                  // Live progress while running
                  return (
                    <View style={s.step4ProgressWrap}>
                      <View style={s.step4ProgressBarBg}>
                        <View style={[s.step4ProgressBarFill, {
                          width: `${Math.min(step1Progress.pct, 100)}%` as any,
                          backgroundColor: '#6366F1',
                        }]} />
                      </View>
                      <View style={s.step4ProgressRow}>
                        <Text style={s.step4ProgressLabel}>Seeding tickers</Text>
                        <Text style={[s.step4ProgressValue, { color: '#6366F1' }]}>
                          {fmt(step1Progress.processed)} / {fmt(step1Progress.total)} · {step1Progress.pct}%
                        </Text>
                      </View>
                    </View>
                  );
                }
                if (!isStep1Running && (seedRawFetched || seedProcessed)) {
                  // Final summary after completion
                  return (
                    <View style={s.step4ProgressWrap}>
                      <View style={s.step4ProgressBarBg}>
                        <View style={[s.step4ProgressBarFill, {
                          width: '100%' as any,
                          backgroundColor: '#6366F1',
                        }]} />
                      </View>
                      <View style={s.step4ProgressRow}>
                        <Text style={s.step4ProgressLabel}>Seeding tickers</Text>
                        <Text style={[s.step4ProgressValue, { color: '#6366F1' }]}>
                          {seedRawFetched ? `${fmt(seedRawFetched)} fetched` : ''}
                          {seedRawFetched && seedProcessed ? ' → ' : ''}
                          {seedProcessed ? `${fmt(seedProcessed)} Common Stock → ${fmt(seedProcessed)} written` : ''}
                        </Text>
                      </View>
                    </View>
                  );
                }
                // No data — hide entirely
                return null;
              })()}

              {/* Step 2 price sync live progress bar */}
              {step.job_name === 'price_sync' && step2Progress !== null && (() => {
                const isDetectorPhase = step2Progress.phase &&
                  ['2.2_split', '2.4_dividend', '2.6_earnings'].includes(step2Progress.phase);
                const isBulkPhase = step2Progress.phase === '2.1_bulk_catchup';
                const isTerminal = step2Progress.phase === 'completed' || step2Progress.phase === 'stopped';

                const detectorPhasePct: Record<string, number> = {
                  '2.2_split': 33,
                  '2.4_dividend': 66,
                  '2.6_earnings': 90,
                };
                const displayPct = isDetectorPhase
                  ? detectorPhasePct[step2Progress.phase!] ?? 50
                  : isTerminal ? 100
                  : step2Progress.pct;

                return (
                  <View style={s.step4ProgressWrap}>
                    <View style={s.step4ProgressBarBg}>
                      <View style={[s.step4ProgressBarFill, {
                        width: `${Math.min(displayPct, 100)}%` as any,
                        backgroundColor: '#10B981',
                      }]} />
                    </View>
                    <View style={s.step4ProgressRow}>
                      <Text style={s.step4ProgressLabel}>
                        {step2Progress.phase === '2.1_bulk_catchup' ? '2.1 Bulk price sync'
                          : step2Progress.phase === '2.2_split' ? '2.2 Split detector'
                          : step2Progress.phase === '2.4_dividend' ? '2.4 Dividend detector'
                          : step2Progress.phase === '2.6_earnings' ? '2.6 Earnings detector'
                          : step2Progress.phase === 'completed' ? 'Complete'
                          : step2Progress.phase === 'stopped' ? 'Stopped'
                          : 'Price sync'}
                      </Text>
                      <Text style={[s.step4ProgressValue, { color: '#10B981' }]}>
                        {isBulkPhase && step2Progress.total > 0
                          ? `${fmt(step2Progress.processed)} / ${fmt(step2Progress.total)} · ${step2Progress.pct}%`
                          : isDetectorPhase
                            ? `${extractDayProgress(step2Progress.message) ?? 'Phase'} · ${displayPct}%`
                            : isTerminal
                              ? '100%'
                              : `${step2Progress.pct}%`}
                      </Text>
                    </View>
                    {step2Progress.message && (
                      <Text style={s.substepMeta} numberOfLines={2}>{step2Progress.message}</Text>
                    )}
                  </View>
                );
              })()}

              {step.job_name === 'price_sync' && (
                <View style={s.substepsCard}>
                  <Text style={s.substepsTitle}>Step 2 Sub-Steps</Text>

                  {!hasStep2DetectorPayload && (
                    <Text style={s.substepMeta}>
                      {run?.status === 'running'
                        ? 'Detectors running — results will appear when Step 2 completes.'
                        : 'No data yet — run Step 2 to populate.'}
                    </Text>
                  )}

                  {(() => {
                    const priceSyncRun = jobRuns['price_sync'] as any;
                    const substepTs: string | undefined =
                      priceSyncRun?.last_run_finished ||
                      priceSyncRun?.end_time ||
                      priceSyncRun?.start_time;
                    const substepLastRunLabel = substepTs
                      ? `Last run: ${formatTime(substepTs)}`
                      : null;

                    return (
                      <>
                  {/* 2.2 Split Detector */}
                  <View style={s.substepBlock}>
                    <View style={s.substepHeaderRow}>
                      <Text style={s.substepName}>2.2 Split Detector</Text>
                      {splitDetector.mock_mode && (
                        <View style={s.mockBadge}><Text style={s.mockBadgeText}>NO API KEY</Text></View>
                      )}
                    </View>
                    {substepLastRunLabel && (
                      <Text style={s.substepLastRun}>{substepLastRunLabel}</Text>
                    )}
                    {splitDetector.verified_through_date && (
                      <Text style={s.substepLastRun}>Verified through: {splitDetector.verified_through_date}</Text>
                    )}
                    <Text style={s.substepDesc}>Detects stock splits today. Flagged tickers need full price history re-download (adjusted prices change) and a fundamentals refresh.</Text>
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
                    {substepLastRunLabel && (
                      <Text style={s.substepLastRun}>{substepLastRunLabel}</Text>
                    )}
                    {dividendDetector.verified_through_date && (
                      <Text style={s.substepLastRun}>Verified through: {dividendDetector.verified_through_date}</Text>
                    )}
                    <Text style={s.substepDesc}>Detects ex-dividend events today. Flagged tickers need a full price history re-download and a fundamentals refresh (dividend yield, payout ratio).</Text>
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
                    {substepLastRunLabel && (
                      <Text style={s.substepLastRun}>{substepLastRunLabel}</Text>
                    )}
                    {earningsDetector.verified_through_date && (
                      <Text style={s.substepLastRun}>Verified through: {earningsDetector.verified_through_date}</Text>
                    )}
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
                      </>
                    );
                  })()}
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


              {/* Per-ticker Fundamentals Audit — Step 3 only */}
              {step.job_name === 'fundamentals_sync' && (
                <View style={s.auditCard}>
                  <Text style={s.auditTitle}>Ticker Fundamentals Audit</Text>
                  <View style={s.auditInputRow}>
                    <TextInput
                      style={s.auditInput}
                      value={auditTicker}
                      onChangeText={setAuditTicker}
                      placeholder="e.g. A.US"
                      placeholderTextColor={COLORS.textMuted}
                      autoCapitalize="characters"
                    />
                    <TouchableOpacity
                      style={[s.auditLiveBtn, auditLive && s.auditLiveBtnActive]}
                      onPress={() => setAuditLive(v => !v)}
                    >
                      <Text style={[s.auditLiveBtnText, auditLive && s.auditLiveBtnTextActive]}>
                        {auditLive ? 'Live ✓' : 'DB only'}
                      </Text>
                    </TouchableOpacity>
                    <TouchableOpacity
                      style={[s.auditRunBtn, auditLoading && s.runBtnDisabled]}
                      onPress={handleRunAudit}
                      disabled={auditLoading}
                    >
                      {auditLoading
                        ? <ActivityIndicator size="small" color="#fff" />
                        : <Text style={s.auditRunBtnText}>Audit</Text>}
                    </TouchableOpacity>
                  </View>

                  {auditResult && !auditResult.error && (
                    <>
                      {/* Verdict badge */}
                      <View style={s.auditVerdictRow}>
                        <View style={[
                          s.auditVerdictBadge,
                          { backgroundColor: auditResult.verdict === 'PASS' ? '#22C55E22' : '#EF444422' },
                        ]}>
                          <Text style={[
                            s.auditVerdictText,
                            { color: auditResult.verdict === 'PASS' ? '#22C55E' : '#EF4444' },
                          ]}>
                            {auditResult.verdict}
                            {auditResult.credits_used > 0 ? `  ·  ${auditResult.credits_used} credits` : ''}
                          </Text>
                        </View>
                        <Text style={s.auditTickerLabel}>{auditResult.ticker}</Text>
                      </View>

                      {/* Integrity failures — emphasized */}
                      {(auditResult.integrity_failures?.length ?? 0) > 0 && (
                        <View style={s.auditSection}>
                          <Text style={s.auditSectionTitleError}>Integrity Failures</Text>
                          {auditResult.integrity_failures.map((msg: string, i: number) => (
                            <Text key={i} style={s.auditItemError}>• {msg}</Text>
                          ))}
                        </View>
                      )}

                      {/* Required missing (visibility blockers) */}
                      {(auditResult.required_missing ?? auditResult.missing ?? []).length > 0 && (
                        <View style={s.auditSection}>
                          <Text style={s.auditSectionTitleWarn}>Required (visibility blockers)</Text>
                          {(auditResult.required_missing ?? auditResult.missing).map((msg: string, i: number) => (
                            <Text key={i} style={s.auditItemWarn}>• {msg}</Text>
                          ))}
                        </View>
                      )}

                      {/* Warnings (cosmetic) */}
                      {(auditResult.warnings?.length ?? 0) > 0 && (
                        <View style={s.auditSection}>
                          <Text style={s.auditSectionTitleMuted}>Warnings (optional)</Text>
                          {auditResult.warnings.map((msg: string, i: number) => (
                            <Text key={i} style={s.auditItemMuted}>• {msg}</Text>
                          ))}
                        </View>
                      )}

                      {/* Values check summary */}
                      {auditResult.values_check && (
                        <View style={s.auditSection}>
                          <Text style={s.auditSectionTitleMuted}>DB values</Text>
                          {Object.entries(auditResult.values_check as Record<string, any>).map(([k, v]) => (
                            <Text key={k} style={s.auditItemMuted}>
                              {k}: <Text style={{ color: v ? COLORS.text : '#EF4444' }}>{v == null ? 'null' : String(v)}</Text>
                            </Text>
                          ))}
                        </View>
                      )}
                    </>
                  )}

                  {auditResult?.error && (
                    <Text style={s.auditItemError}>Error: {auditResult.error}</Text>
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

      {/* Data Freshness Dashboard */}
      {freshness && (
        <View style={s.syncCard}>
          <Text style={s.syncTitle}>Data Freshness</Text>

          {/* Events Watermark */}
          {freshness.events_watermark && (
            <View style={s.syncRow}>
              <View style={s.syncLabelRow}>
                <Text style={s.syncLabel}>Events Watermark</Text>
                <Text style={[s.syncCount, {
                  color: freshness.events_watermark.status === 'current' ? '#22C55E'
                    : freshness.events_watermark.status === 'behind' ? '#F59E0B'
                    : freshness.events_watermark.status === 'stale' ? '#EF4444'
                    : COLORS.textMuted,
                }]}>
                  {freshness.events_watermark.date ?? 'unknown'}
                  {freshness.events_watermark.days_behind != null
                    ? ` (${freshness.events_watermark.days_behind} trading day${freshness.events_watermark.days_behind !== 1 ? 's' : ''} behind)`
                    : ''}
                </Text>
              </View>
            </View>
          )}

          {/* Fundamentals Age Distribution */}
          {freshness.fundamentals_age && (() => {
            const fa = freshness.fundamentals_age;
            return (
              <View style={[s.syncRow, { marginTop: 10 }]}>
                <View style={s.syncLabelRow}>
                  <Text style={s.syncLabel}>Fundamentals Age</Text>
                  <Text style={s.syncCount}>{fmt(fa.total)} tickers</Text>
                </View>
                <View style={[s.syncBarBg, { height: 8, flexDirection: 'row' }]}>
                  {fa.fresh_7d?.pct > 0 && (
                    <View style={{ width: `${fa.fresh_7d.pct}%` as any, height: 8, backgroundColor: '#22C55E' }} />
                  )}
                  {fa.stale_7_30d?.pct > 0 && (
                    <View style={{ width: `${fa.stale_7_30d.pct}%` as any, height: 8, backgroundColor: '#F59E0B' }} />
                  )}
                  {fa.stale_30d_plus?.pct > 0 && (
                    <View style={{ width: `${fa.stale_30d_plus.pct}%` as any, height: 8, backgroundColor: '#EF4444' }} />
                  )}
                  {fa.never_synced?.pct > 0 && (
                    <View style={{ width: `${fa.never_synced.pct}%` as any, height: 8, backgroundColor: '#6B7280' }} />
                  )}
                </View>
                <View style={{ flexDirection: 'row', flexWrap: 'wrap', marginTop: 4, gap: 8 }}>
                  <Text style={{ fontSize: 9, color: '#22C55E' }}>● &lt;7d: {fmt(fa.fresh_7d?.count)} ({fa.fresh_7d?.pct}%)</Text>
                  <Text style={{ fontSize: 9, color: '#F59E0B' }}>● 7–30d: {fmt(fa.stale_7_30d?.count)} ({fa.stale_7_30d?.pct}%)</Text>
                  <Text style={{ fontSize: 9, color: '#EF4444' }}>● &gt;30d: {fmt(fa.stale_30d_plus?.count)} ({fa.stale_30d_plus?.pct}%)</Text>
                  <Text style={{ fontSize: 9, color: '#6B7280' }}>● Never: {fmt(fa.never_synced?.count)} ({fa.never_synced?.pct}%)</Text>
                </View>
                {fa.oldest && (
                  <Text style={{ fontSize: 9, color: COLORS.textMuted, marginTop: 2 }}>
                    Oldest: {fa.oldest.ticker} · Newest: {fa.newest?.ticker}
                  </Text>
                )}
              </View>
            );
          })()}

          {/* Pending Fundamentals Events */}
          {freshness.pending_events && (
            <View style={[s.syncRow, { marginTop: 10 }]}>
              <View style={s.syncLabelRow}>
                <Text style={s.syncLabel}>Pending Events Queue</Text>
                <Text style={[s.syncCount, {
                  color: freshness.pending_events.count > 20 ? '#F59E0B' : COLORS.textMuted,
                }]}>
                  {fmt(freshness.pending_events.count)} pending
                </Text>
              </View>
              {freshness.pending_events.count > 0 && freshness.pending_events.by_type && (
                <View style={{ flexDirection: 'row', flexWrap: 'wrap', marginTop: 2, gap: 6 }}>
                  {Object.entries(freshness.pending_events.by_type).map(([type, count]) => (
                    <Text key={type} style={{ fontSize: 9, color: COLORS.textMuted }}>{type}: {fmt(count as number)}</Text>
                  ))}
                </View>
              )}
              {freshness.pending_events.oldest_ticker && (
                <Text style={{ fontSize: 9, color: COLORS.textMuted, marginTop: 2 }}>
                  Oldest: {freshness.pending_events.oldest_ticker}
                  {freshness.pending_events.oldest_created_at
                    ? ` (${new Date(freshness.pending_events.oldest_created_at).toLocaleDateString()})`
                    : ''}
                </Text>
              )}
            </View>
          )}
        </View>
      )}

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
  schedulerControlRow: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', borderTopWidth: 1, borderTopColor: COLORS.border, paddingTop: 12, marginTop: 14, marginBottom: 10 },
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
  runBtnDisabled: { opacity: 0.5 },

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
  substepLastRun: { fontSize: 9, color: COLORS.textMuted, marginBottom: 3, fontStyle: 'italic' },
  catchupBadge: { fontSize: 9, color: '#6366F1', marginBottom: 4, fontStyle: 'italic' },

  // ── Per-ticker audit panel ──────────────────────────────────────────────
  auditCard: { marginTop: 10, paddingTop: 10, borderTopWidth: 1, borderTopColor: COLORS.border },
  auditTitle: { fontSize: 11, fontWeight: '700', color: COLORS.text, marginBottom: 7 },
  auditInputRow: { flexDirection: 'row', alignItems: 'center', gap: 6, marginBottom: 8 },
  auditInput: {
    flex: 1, height: 32, borderWidth: 1, borderColor: COLORS.border,
    borderRadius: 6, paddingHorizontal: 8, color: COLORS.text,
    backgroundColor: COLORS.background, fontSize: 11,
  },
  auditLiveBtn: { paddingHorizontal: 8, paddingVertical: 5, borderRadius: 6, borderWidth: 1, borderColor: COLORS.border },
  auditLiveBtnActive: { backgroundColor: '#6366F122', borderColor: '#6366F1' },
  auditLiveBtnText: { fontSize: 10, color: COLORS.textMuted },
  auditLiveBtnTextActive: { color: '#6366F1', fontWeight: '700' },
  auditRunBtn: { backgroundColor: '#6366F1', paddingHorizontal: 10, paddingVertical: 5, borderRadius: 6 },
  auditRunBtnText: { color: '#fff', fontSize: 11, fontWeight: '700' },
  auditVerdictRow: { flexDirection: 'row', alignItems: 'center', gap: 8, marginBottom: 6 },
  auditVerdictBadge: { paddingHorizontal: 8, paddingVertical: 3, borderRadius: 5 },
  auditVerdictText: { fontSize: 11, fontWeight: '700' },
  auditTickerLabel: { fontSize: 10, color: COLORS.textMuted },
  auditSection: { marginBottom: 6 },
  auditSectionTitleError: { fontSize: 10, fontWeight: '700', color: '#EF4444', marginBottom: 2 },
  auditSectionTitleWarn:  { fontSize: 10, fontWeight: '700', color: '#F59E0B', marginBottom: 2 },
  auditSectionTitleMuted: { fontSize: 10, fontWeight: '700', color: COLORS.textMuted, marginBottom: 2 },
  auditItemError: { fontSize: 10, color: '#EF4444', marginBottom: 1 },
  auditItemWarn:  { fontSize: 10, color: '#F59E0B', marginBottom: 1 },
  auditItemMuted: { fontSize: 10, color: COLORS.textMuted, marginBottom: 1 },

  // ── Step 4 progress bar ──────────────────────────────────────────────────
  step4ProgressWrap:    { marginTop: 8, marginBottom: 4 },
  step4ProgressBarBg:   { height: 5, backgroundColor: COLORS.border, borderRadius: 3, overflow: 'hidden', marginBottom: 4 },
  step4ProgressBarFill: { height: 5, borderRadius: 3, backgroundColor: '#8B5CF6' },
  step4ProgressRow:     { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center' },
  step4ProgressLabel:   { fontSize: 10, color: COLORS.textMuted },
  step4ProgressValue:   { fontSize: 10, color: '#8B5CF6', fontWeight: '600' },

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
  exportBtn: { flexDirection: 'row', alignItems: 'center', gap: 4, marginTop: 8, alignSelf: 'flex-start', backgroundColor: '#6366F111', borderWidth: 1, borderColor: '#6366F144', borderRadius: 6, paddingHorizontal: 8, paddingVertical: 4 },
  exportBtnText: { fontSize: 11, color: '#6366F1', fontWeight: '600' },

  fullChainBtn: { backgroundColor: '#6366F1', paddingHorizontal: 12, paddingVertical: 7, borderRadius: 6, alignItems: 'center', justifyContent: 'center' },
  fullChainBtnText: { color: '#fff', fontSize: 12, fontWeight: '700' },
  fullChainDownloadBtn: { flexDirection: 'row', alignItems: 'center', gap: 4, backgroundColor: '#22C55E', paddingHorizontal: 12, paddingVertical: 7, borderRadius: 6 },
  fullChainDownloadBtnText: { color: '#fff', fontSize: 12, fontWeight: '700' },
  fullChainStatus: { marginTop: 6, fontSize: 11, fontWeight: '600' },

  // Inline Full Pipeline Audit section (inside healthCard)
  fullChainInlineSection: { marginTop: 10, paddingTop: 10, borderTopWidth: 1, borderTopColor: COLORS.border + '55' },
  fullChainInlineTitleRow: { flexDirection: 'row', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: 6 },
  fullChainInlineTitle: { fontSize: 12, fontWeight: '700', color: COLORS.text },
  fullChainInlineDesc: { fontSize: 11, color: COLORS.textMuted, lineHeight: 15 },
  manualAutoToggle: { flexDirection: 'row', borderRadius: 6, overflow: 'hidden', borderWidth: 1, borderColor: COLORS.border },
  toggleBtn: { paddingHorizontal: 8, paddingVertical: 3 },
  toggleBtnActive: { backgroundColor: '#6366F1' },
  toggleBtnText: { fontSize: 10, fontWeight: '700', color: COLORS.textMuted },
  toggleBtnTextActive: { color: '#fff' },
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
