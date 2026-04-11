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
  pending_events_audit?: number;
  pending_event_counts?: {
    split?: number;
    dividend?: number;
    earnings?: number;
  };
}

interface OverviewData {
  health?: {
    scheduler_active?: boolean;
  };
  universe_seed?: {
    result?: {
      raw_rows_total?: number;
      seeded_total?: number;
    };
    details?: {
      raw_rows_total?: number;
      seeded_total?: number;
    };
  };
  fundamentals_sync?: {
    details?: {
      processed?: number;
    };
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
      visible_universe_count?: number;
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
  by_step?: Record<string, number>;
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
  if (sec == null || !isFinite(sec)) return '';
  const total = Math.round(sec);
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const seconds = total % 60;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

/** Parse an ISO timestamp, treating naive (no-offset) strings as UTC. */
function parseUtcIso(iso?: string | null): number {
  if (!iso) return NaN;
  let s: string = iso;
  if (!s.endsWith('Z') && !/[+-]\d{2}:\d{2}$/.test(s) && !/[+-]\d{4}$/.test(s)) {
    s += 'Z';
  }
  return Date.parse(s);
}

function formatTime(iso?: string): string {
  if (!iso) return 'Never';
  try {
    const ms = parseUtcIso(iso);
    if (isNaN(ms)) return '—';
    const d = new Date(ms);
    return `${d.toLocaleString('en-GB', {
      timeZone: 'Europe/Prague',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    })} Prague`;
  } catch { return '—'; }
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

function asFiniteNumber(v: any): number | undefined {
  return typeof v === 'number' && Number.isFinite(v) ? v : undefined;
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

const CHAIN_STATUS_POLL_MS = 5000;

// ── Collapsible step accordion helpers ──────────────────────────────────────
const ALL_PIPELINE_STEPS = [1, 2, 3, 4];
const JOB_NAME_BY_STEP: Record<number, string> = {
  1: 'universe_seed', 2: 'price_sync', 3: 'fundamentals_sync', 4: 'peer_medians',
};

function readCollapsedFromStorage(): Set<number> {
  const s = new Set<number>();
  if (Platform.OS !== 'web' || typeof window === 'undefined') return s;
  for (const n of ALL_PIPELINE_STEPS) {
    try {
      if (window.localStorage.getItem(`pipeline_step_${n}_collapsed`) === 'true') s.add(n);
    } catch { /* ignore */ }
  }
  return s;
}

function hasStoredCollapsedPrefs(): boolean {
  if (Platform.OS !== 'web' || typeof window === 'undefined') return false;
  return ALL_PIPELINE_STEPS.some(n => {
    try { return window.localStorage.getItem(`pipeline_step_${n}_collapsed`) !== null; } catch { return false; }
  });
}

function persistCollapsedToStorage(collapsed: Set<number>): void {
  if (Platform.OS !== 'web' || typeof window === 'undefined') return;
  for (const n of ALL_PIPELINE_STEPS) {
    try { window.localStorage.setItem(`pipeline_step_${n}_collapsed`, String(collapsed.has(n))); } catch { /* ignore */ }
  }
}

// ── Step 3 phase telemetry constants ────────────────────────────────────────
const S3_PHASE_LABELS: Record<string, string> = { A: 'Phase A — Fundamentals', B: 'Phase B — Visibility', C: 'Phase C — Price History' };
const S3_PHASE_COLORS: Record<string, string> = { A: '#6366F1', B: '#F59E0B', C: '#10B981' };
const S3_STATUS_ICONS: Record<string, { icon: string; color: string }> = {
  idle: { icon: 'time-outline', color: '#888' },
  running: { icon: 'sync-outline', color: '#3B82F6' },
  done: { icon: 'checkmark-circle', color: '#22C55E' },
  error: { icon: 'close-circle', color: '#EF4444' },
};
const S3_TERMINAL_STATUSES = ['cancelled', 'failed', 'error', 'completed', 'success'];

function isChainStatusActive(status?: string | null): boolean {
  return status != null && !['completed', 'success', 'failed', 'error', 'cancelled'].includes(status);
}

function getLatestChainRun(jobLastRuns?: Record<string, any> | null): { chainRunId: string; startedAt?: string; status?: string } | null {
  if (!jobLastRuns) return null;
  let latestChainRun: { chainRunId: string; startedAt?: string; startedAtMs?: number; status?: string } | null = null;
  for (const jobName of ['universe_seed', 'price_sync', 'fundamentals_sync']) {
    const run = jobLastRuns[jobName];
    const chainRunId = run?.details?.chain_run_id;
    if (!chainRunId) continue;
    const startedAt = run?.started_at;
    const startedAtMs = startedAt ? Date.parse(startedAt) : undefined;
    if (
      !latestChainRun ||
      ((startedAtMs ?? Number.NEGATIVE_INFINITY) > (latestChainRun.startedAtMs ?? Number.NEGATIVE_INFINITY))
    ) {
      latestChainRun = {
        chainRunId,
        startedAt,
        startedAtMs,
        status: run?.status,
      };
    }
  }
  return latestChainRun ? {
    chainRunId: latestChainRun.chainRunId,
    startedAt: latestChainRun.startedAt,
    status: latestChainRun.status === 'success' ? 'completed' : latestChainRun.status,
  } : null;
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
  const isComplete = normalized.status === 'success' || normalized.status === 'completed';
  const isError = normalized.status === 'error' || normalized.status === 'failed';
  // A run is terminal when it completed, errored, or has finished_at set
  // (e.g. status still "running" but backend already wrote finished_at).
  const isTerminal = isComplete || isError || (normalized.status === 'running' && !!normalized.finished_at);
  const total = normalized.progress_total;
  const processed = normalized.progress_processed ?? 0;
  const pct = isComplete ? 100 : (total ? Math.min(Math.round((processed / total) * 100), 100) : (processed ? 100 : 0));
  // Auto-advance past 2.1@100%: when bulk sync is done the backend transitions
  // to 2.2 but the frontend poll may not catch it before completion.
  const effectivePhase = isTerminal ? (isComplete ? 'completed' : normalized.phase)
    : (normalized.phase === '2.1_bulk_catchup' && pct >= 100)
    ? '2.2_split'
    : normalized.phase;
  return {
    processed,
    total,
    pct,
    phase: effectivePhase,
    message: isTerminal ? undefined : normalized.progress,
  };
}

/** Extract error text from whichever field is present on a run object. */
function extractErrorText(run: any): string | null {
  if (!run) return null;
  if (run.error_message) return run.error_message;
  if (run.result?.error) return run.result.error;
  if (run.details?.error_message) return run.details.error_message;
  const days = run.details?.price_bulk_gapfill?.days;
  if (Array.isArray(days) && days.length > 0) {
    if (days[0]?.error_message) return days[0].error_message;
    if (days[0]?.error) return days[0].error;
  }
  return run.error || null;
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
  // ── Collapsible step accordion ──────────────────────────────────────────
  const [collapsedSteps, setCollapsedSteps] = useState<Set<number>>(readCollapsedFromStorage);
  const collapsedDefaultsApplied = useRef(hasStoredCollapsedPrefs());
  // Seed from localStorage: any step the user previously collapsed is treated as
  // a manual collapse so auto-expand won't override it on refresh.
  const userManualCollapseRef = useRef<Set<number>>(readCollapsedFromStorage());
  const [elapsedSeconds, setElapsedSeconds] = useState(0);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null); // chain run elapsed timer

  // ── Step 1 universe seed progress ────────────────────────────────────────
  const [step1Progress, setStep1Progress] = useState<{processed: number; total: number; pct: number} | null>(null);

  // ── Step 2 price sync progress ────────────────────────────────────────────
  const [step2Progress, setStep2Progress] = useState<{processed: number; total: number; pct: number; phase?: string; message?: string} | null>(null);

  // ── Step 3 live telemetry ─────────────────────────────────────────────────
  const [step3Telemetry, setStep3Telemetry] = useState<Record<string, any> | null>(null);

  // ── Benchmark update state ────────────────────────────────────────────────
  const [benchmarkUpdating, setBenchmarkUpdating] = useState(false);
  const benchmarkPollRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // ── News refresh (Morning Fresh) state ─────────────────────────────────────
  const [newsRefreshRunning, setNewsRefreshRunning] = useState(false);
  const newsRefreshPollRef = useRef<ReturnType<typeof setTimeout> | null>(null);

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
  const [chainFailedStep, setChainFailedStep] = useState<number | null>(null);
  const [canonicalReport, setCanonicalReport] = useState<Record<string, any> | null>(null);
  const chainStateRef = useRef<{ chainRunId: string | null; chainStatus: string | null }>({
    chainRunId: null,
    chainStatus: null,
  });
  const userStartedRunRef = useRef(false);
  const pollingControllerRef = useRef<{
    active: boolean;
    timeout: ReturnType<typeof setTimeout> | null;
    chainRunId: string | null;
  }>({ active: false, timeout: null, chainRunId: null });
  const stepRunFetchRef = useRef<Set<string>>(new Set());

  // ── Manual / Auto run mode toggle ─────────────────────────────────────────
  const [runMode, setRunMode] = useState<'MANUAL' | 'AUTO'>('MANUAL');
  const runModeInitialised = useRef(false);

  const stopChainTimer = useCallback(() => {
    if (timerRef.current) {
      clearInterval(timerRef.current);
      timerRef.current = null;
    }
  }, []);

  const startChainTimer = useCallback((startedAtIso?: string | null) => {
    stopChainTimer();
    const parsedStartedAt = parseUtcIso(startedAtIso);
    if (!Number.isFinite(parsedStartedAt)) {
      setElapsedSeconds(0);
      return;
    }
    const chainStartedAt = parsedStartedAt;
    const getElapsed = () => Math.max(0, Math.round((Date.now() - chainStartedAt) / 1000));
    setElapsedSeconds(getElapsed());
    timerRef.current = setInterval(() => {
      setElapsedSeconds(getElapsed());
    }, 1000);
  }, [stopChainTimer]);

  const stopPolling = useCallback(() => {
    const ctrl = pollingControllerRef.current;
    ctrl.active = false;
    ctrl.chainRunId = null;
    if (ctrl.timeout) {
      clearTimeout(ctrl.timeout);
      ctrl.timeout = null;
    }
  }, []);

  const pollChainStatus = useCallback(async (cid: string) => {
    try {
      const sr = await authenticatedFetch(
        `${API_URL}/api/admin/pipeline/chain-status/${cid}`,
        {},
        sessionToken,
      );
      if (!sr.ok) return;
      const sd = await sr.json();
      const nextStatus = sd.status ?? null;
      const nextRunning = isChainStatusActive(nextStatus);

      setChainRunId(cid);
      setChainStatus(nextStatus);
      chainStateRef.current = { chainRunId: cid, chainStatus: nextStatus };
      setChainCurrentStep(sd.current_step ?? null);
      setChainStepsDone(sd.steps_done ?? []);
      setChainFailedStep(sd.failed_step ?? null);
      setChainRunning(nextRunning);

      if (nextRunning) {
        startChainTimer(sd.started_at);
      } else {
        userStartedRunRef.current = false;
        stopChainTimer();
        setElapsedSeconds(0);
        stopPolling();
      }

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

      if (sd.current_step === 2) {
        try {
          const jr = await authenticatedFetch(
            `${API_URL}/api/admin/jobs/price_sync/status`,
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

      // Fetch Step 3 telemetry when running Step 3
      if (sd.current_step === 3) {
        try {
          const tr = await authenticatedFetch(`${API_URL}/api/admin/step3/telemetry`, {}, sessionToken);
          if (tr.ok) setStep3Telemetry(await tr.json());
        } catch { /* non-fatal */ }
      }

      if (sd.steps_done?.includes(2)) {
        setStep2Progress(prev => prev ? { ...prev, pct: 100, phase: 'completed', message: undefined } : null);
      }
      if (!nextRunning && sd.status === 'completed') {
        if (sd.steps_done?.includes(1)) {
          setStep1Progress(prev => prev ? { processed: prev.total, total: prev.total, pct: 100 } : null);
        }
      }
    } catch { /* keep polling */ }
  }, [sessionToken, startChainTimer, stopChainTimer, stopPolling]);

  useEffect(() => {
    chainStateRef.current = { chainRunId, chainStatus };
  }, [chainRunId, chainStatus]);

  // ── Fetch canonical report when chain is completed ─────────────────────────
  useEffect(() => {
    if (!chainRunId || chainStatus !== 'completed' || !sessionToken) {
      setCanonicalReport(null);
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        const res = await authenticatedFetch(
          `${API_URL}/api/admin/pipeline/report?chain_run_id=${encodeURIComponent(chainRunId)}`,
          {},
          sessionToken,
        );
        if (res.ok && !cancelled) {
          setCanonicalReport(await res.json());
        }
      } catch { /* non-fatal */ }
    })();
    return () => { cancelled = true; };
  }, [chainRunId, chainStatus, sessionToken]);

  const fetchSnapshotOnce = useCallback(async () => {
    if (!sessionToken) {
      setLoading(false);
      setRefreshing(false);
      return;
    }
    try {
      const [overviewRes, exclusionRes, step3TelRes] = await Promise.allSettled([
        authenticatedFetch(`${API_URL}/api/admin/overview`, {}, sessionToken),
        authenticatedFetch(`${API_URL}/api/admin/pipeline/exclusion-report?limit=20`, {}, sessionToken),
        authenticatedFetch(`${API_URL}/api/admin/step3/telemetry`, {}, sessionToken),
      ]);

      if (overviewRes.status === 'fulfilled' && overviewRes.value.ok) {
        const overviewData = await overviewRes.value.json();
        setData(overviewData);
        const latestChainRun = getLatestChainRun(overviewData?.job_last_runs);
        if (latestChainRun?.chainRunId) {
          // The overview returns the *job* status which can disagree with
          // the actual *chain* status (e.g. a stale "running" sentinel in
          // ops_job_runs while the chain document is already "completed").
          // Verify against the chain document to avoid a polling loop where
          // fetchSnapshotOnce starts polling, pollChainStatus finds the chain
          // done, stops, re-calls fetchSnapshotOnce which sees the stale job
          // status and restarts polling — causing the button to blink.
          await pollChainStatus(latestChainRun.chainRunId);
          const { chainStatus: verifiedStatus } = chainStateRef.current;
          const active = isChainStatusActive(verifiedStatus);
          if (active) {
            startChainTimer(latestChainRun.startedAt);
            startPolling(latestChainRun.chainRunId, verifiedStatus);
          }
          // pollChainStatus already handled the non-active case (stops timer,
          // resets elapsed, stops polling, sets chainRunning=false).
        } else {
          setChainRunId(null);
          setChainStatus(null);
          chainStateRef.current = { chainRunId: null, chainStatus: null };
          setChainCurrentStep(null);
          setChainStepsDone([]);
          setChainFailedStep(null);
          setChainRunning(false);
          userStartedRunRef.current = false;
          stopChainTimer();
          setElapsedSeconds(0);
          stopPolling();
        }
      }
      if (exclusionRes.status === 'fulfilled' && exclusionRes.value.ok) {
        try {
          const excl = await exclusionRes.value.json();
          stepRunFetchRef.current.clear();
          setExclusionReport(excl);
        } catch (e) { console.error('exclusion processing error', e); }
      }
      if (step3TelRes.status === 'fulfilled' && step3TelRes.value.ok) {
        try { setStep3Telemetry(await step3TelRes.value.json()); } catch { /* non-fatal */ }
      }
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [sessionToken, stopChainTimer, pollChainStatus]);

  const pollingTick = useCallback(async () => {
    const ctrl = pollingControllerRef.current;
    if (!ctrl.active || !ctrl.chainRunId || !sessionToken) return;
    await pollChainStatus(ctrl.chainRunId);
    const { chainStatus: latestStatus } = chainStateRef.current;
    if (!isChainStatusActive(latestStatus)) {
      stopPolling();
      await fetchSnapshotOnce();
      return;
    }
    ctrl.timeout = setTimeout(pollingTick, CHAIN_STATUS_POLL_MS);
  }, [fetchSnapshotOnce, pollChainStatus, sessionToken, stopPolling]);

  const startPolling = useCallback((cid: string, status?: string | null) => {
    if (!sessionToken) return;
    const latestStatus = status ?? chainStateRef.current.chainStatus;
    if (!isChainStatusActive(latestStatus)) {
      stopPolling();
      return;
    }
    const ctrl = pollingControllerRef.current;
    if (ctrl.active && ctrl.chainRunId === cid) return;
    stopPolling();
    ctrl.active = true;
    ctrl.chainRunId = cid;
    pollingTick();
  }, [pollingTick, sessionToken, stopPolling]);

  useEffect(() => { fetchSnapshotOnce(); }, [fetchSnapshotOnce]);

  // Cleanup polling loop on unmount.
  useEffect(() => {
    return () => {
      userStartedRunRef.current = false;
      stopPolling();
    };
  }, [stopPolling]);

  useEffect(() => {
    if (!sessionToken) {
      stopPolling();
    }
  }, [sessionToken, stopPolling]);

  const onRefresh = () => { setRefreshing(true); fetchSnapshotOnce(); };

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
      await fetchSnapshotOnce();
      Alert.alert('Scheduler updated', targetEnabled ? 'Scheduler resumed.' : 'Scheduler paused.');
    } catch (e: any) {
      Alert.alert('Update failed', e?.message || 'Could not update scheduler state');
    } finally {
      setSchedulerUpdating(false);
    }
  };

  const handleRunBenchmarkUpdate = async () => {
    setBenchmarkUpdating(true);
    try {
      const res = await authenticatedFetch(
        `${API_URL}/api/admin/job/benchmark_update/run`,
        { method: 'POST' },
        sessionToken,
      );
      const payload = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(payload?.detail || payload?.message || res.statusText);
      }
      Alert.alert('Benchmark Update', 'Benchmark update started in background.');
      await fetchSnapshotOnce();
    } catch (e: any) {
      Alert.alert('Benchmark Update Failed', e?.message || 'Could not start benchmark update');
    } finally {
      setBenchmarkUpdating(false);
    }
  };

  const handleRunNewsRefresh = async () => {
    setNewsRefreshRunning(true);
    try {
      const res = await authenticatedFetch(
        `${API_URL}/api/admin/job/news_refresh/run`,
        { method: 'POST' },
        sessionToken,
      );
      const payload = await res.json().catch(() => ({}));
      if (!res.ok) {
        const detail = payload?.detail;
        const msg = typeof detail === 'object' ? detail?.message : detail || payload?.message || res.statusText;
        throw new Error(msg);
      }
      Alert.alert('Morning Fresh', 'News refresh started in background.');
      await fetchSnapshotOnce();
    } catch (e: any) {
      Alert.alert('Morning Fresh', e?.message || 'Could not start news refresh');
    } finally {
      setNewsRefreshRunning(false);
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

  const handleRunFullPipeline = useCallback(async () => {
    if (chainRunning) return;
    const startingStatus = 'starting';
    setChainStatus(startingStatus);
    setChainRunning(false);
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
      if (!res.ok) {
        const existingChainRunId = data?.detail?.chain_run_id || data?.chain_run_id;
        if (existingChainRunId) {
          setChainRunId(existingChainRunId);
          await pollChainStatus(existingChainRunId);
          const latestStatus = chainStateRef.current.chainStatus;
          if (isChainStatusActive(latestStatus)) {
            startPolling(existingChainRunId, latestStatus);
          }
          return;
        }
        setChainStatus('error');
        setChainRunning(false);
        stopChainTimer();
        setElapsedSeconds(0);
        return;
      }
      const cid: string = data.chain_run_id;
      setChainRunId(cid);
      await pollChainStatus(cid);
      const latestStatus = chainStateRef.current.chainStatus;
      if (isChainStatusActive(latestStatus)) {
        startPolling(cid, latestStatus);
      }
    } catch {
      setChainStatus('error');
      stopChainTimer();
      setElapsedSeconds(0);
      setChainRunning(false);
      userStartedRunRef.current = false;
    }
  }, [chainRunning, sessionToken, startChainTimer, startPolling, stopChainTimer]);

  const handleStopChain = async () => {
    if (!chainRunId) return;
    try {
      await authenticatedFetch(
        `${API_URL}/api/admin/pipeline/chain-cancel/${chainRunId}`,
        { method: 'POST' },
        sessionToken,
      );
    } catch { /* ignore network error — polling will detect cancellation */ }
    startPolling(chainRunId, chainStatus);
  };

  const handleCancelRunningJob = async (jobName: 'price_sync' | 'fundamentals_sync') => {
    try {
      const res = await authenticatedFetch(
        `${API_URL}/api/admin/jobs/cancel_running?job_name=${encodeURIComponent(jobName)}`,
        { method: 'POST' },
        sessionToken,
      );
      const payload = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(payload?.detail || payload?.message || res.statusText);
      }
      await fetchSnapshotOnce();
      Alert.alert('Cancel requested', `Job ${jobName} is now marked as cancel_requested.`);
    } catch (e: any) {
      Alert.alert('Cancel failed', e?.message || 'Could not request cancellation.');
    }
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

  const fetchExclusionByRunId = useCallback(async (runId: string) => {
    if (!sessionToken || !runId) return;
    if (stepRunFetchRef.current.has(runId)) return;
    stepRunFetchRef.current.add(runId);
    try {
      const res = await authenticatedFetch(
        `${API_URL}/api/admin/pipeline/exclusion-report?run_id=${encodeURIComponent(runId)}&limit=1`,
        {},
        sessionToken,
      );
      if (!res.ok) return;
      const data = await res.json();
      setExclusionReport(prev => {
        if (!prev) return prev;
        return {
          ...prev,
          step1_counts: ('step1_counts' in data) ? data.step1_counts : prev.step1_counts,
          by_step: ('by_step' in data) && data.by_step
            ? { ...(prev.by_step || {}), ...data.by_step }
            : ('by_step' in data ? data.by_step : prev.by_step),
        };
      });
    } catch { /* non-fatal */ }
    finally {
      stepRunFetchRef.current.delete(runId);
    }
  }, [sessionToken]);

  const toggleExpand = (step: number) => {
    setExpandedSteps(prev => {
      const next = new Set(prev);
      if (next.has(step)) {
        next.delete(step);
      } else {
        next.add(step);
        if (step === 1) {
          const runId = exclusionReport?.latest_run_id_per_step?.['Step 1 - Universe Seed'];
          if (runId) fetchExclusionByRunId(runId);
        }
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

  // Derive benchmark running state from real backend status
  // "running" only when status=="running" AND finished_at is null
  const benchmarkRun = jobRuns['benchmark_update'];
  const benchmarkRunStatus = benchmarkRun?.status;
  const isBenchmarkRunning = benchmarkUpdating || (benchmarkRunStatus === 'running' && !benchmarkRun?.finished_at);

  // Derive news refresh running state from real backend status
  const newsRefreshRun = jobRuns['news_refresh'];
  const newsRefreshStatus = newsRefreshRun?.status;
  const isNewsRefreshRunning = newsRefreshRunning || (newsRefreshStatus === 'running' && !newsRefreshRun?.finished_at);

  // Poll benchmark status while it is running so UI updates when the job finishes
  useEffect(() => {
    if (!isBenchmarkRunning || !sessionToken) {
      if (benchmarkPollRef.current) {
        clearTimeout(benchmarkPollRef.current);
        benchmarkPollRef.current = null;
      }
      return;
    }
    let cancelled = false;
    const poll = async () => {
      try {
        const res = await authenticatedFetch(
          `${API_URL}/api/admin/job/benchmark_update/status`,
          {},
          sessionToken,
        );
        if (res.ok && !cancelled) {
          const payload = await res.json();
          const lr = normaliseRun(payload.last_run);
          if (lr) {
            setLiveLastRuns(prev => ({ ...prev, benchmark_update: lr }));
          }
        }
      } catch { /* non-fatal */ }
      if (!cancelled) {
        benchmarkPollRef.current = setTimeout(poll, 5000);
      }
    };
    poll();
    return () => {
      cancelled = true;
      if (benchmarkPollRef.current) {
        clearTimeout(benchmarkPollRef.current);
        benchmarkPollRef.current = null;
      }
    };
  }, [isBenchmarkRunning, sessionToken]);

  // Poll news_refresh status while it is running so UI updates when the job finishes
  useEffect(() => {
    if (!isNewsRefreshRunning || !sessionToken) {
      if (newsRefreshPollRef.current) {
        clearTimeout(newsRefreshPollRef.current);
        newsRefreshPollRef.current = null;
      }
      return;
    }
    let cancelled = false;
    const poll = async () => {
      try {
        const res = await authenticatedFetch(
          `${API_URL}/api/admin/job/news_refresh/status`,
          {},
          sessionToken,
        );
        if (res.ok && !cancelled) {
          const payload = await res.json();
          const lr = normaliseRun(payload.last_run);
          if (lr) {
            setLiveLastRuns(prev => ({ ...prev, news_refresh: lr }));
          }
        }
      } catch { /* non-fatal */ }
      if (!cancelled) {
        newsRefreshPollRef.current = setTimeout(poll, 5000);
      }
    };
    poll();
    return () => {
      cancelled = true;
      if (newsRefreshPollRef.current) {
        clearTimeout(newsRefreshPollRef.current);
        newsRefreshPollRef.current = null;
      }
    };
  }, [isNewsRefreshRunning, sessionToken]);

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
  useEffect(() => () => stopChainTimer(), [stopChainTimer]);

  // ── Collapsible step defaults + persistence ─────────────────────────────
  // Set default collapsed state based on step statuses when data first loads
  // (only if no stored localStorage preference exists)
  useEffect(() => {
    if (collapsedDefaultsApplied.current || !data) return;
    collapsedDefaultsApplied.current = true;
    let expandStep: number | null = null;
    for (const n of ALL_PIPELINE_STEPS) {
      const run = jobRuns[JOB_NAME_BY_STEP[n]];
      const st = run?.status;
      if (st === 'running' || st === 'error' || st === 'failed') {
        expandStep = n;
        break;
      }
    }
    const collapsed = new Set(ALL_PIPELINE_STEPS);
    if (expandStep !== null) collapsed.delete(expandStep);
    setCollapsedSteps(collapsed);
    persistCollapsedToStorage(collapsed);
  }, [data, jobRuns]);

  // Persist collapsed state to localStorage whenever it changes
  useEffect(() => {
    persistCollapsedToStorage(collapsedSteps);
  }, [collapsedSteps]);

  // Auto-expand running steps (unless user manually collapsed them)
  useEffect(() => {
    if (!data) return;
    setCollapsedSteps(prev => {
      const next = new Set(prev);
      let changed = false;
      for (const n of ALL_PIPELINE_STEPS) {
        const run = jobRuns[JOB_NAME_BY_STEP[n]];
        const isRunning = run?.status === 'running' && !run?.finished_at;
        if (isRunning && next.has(n) && !userManualCollapseRef.current.has(n)) {
          next.delete(n);
          changed = true;
        }
      }
      return changed ? next : prev;
    });
  }, [data, jobRuns]);

  const toggleStepCollapsed = useCallback((stepNum: number) => {
    setCollapsedSteps(prev => {
      const next = new Set(prev);
      if (next.has(stepNum)) {
        // Expanding — clear manual-collapse flag so auto-expand can work again
        next.delete(stepNum);
        userManualCollapseRef.current.delete(stepNum);
      } else {
        // Collapsing — mark as manual so auto-expand won't override
        next.add(stepNum);
        userManualCollapseRef.current.add(stepNum);
      }
      return next;
    });
  }, []);

  const expandAllSteps = useCallback(() => {
    // Clear all manual-collapse flags so auto-expand can resume
    userManualCollapseRef.current.clear();
    setCollapsedSteps(new Set());
  }, []);

  const collapseAllSteps = useCallback(() => {
    // Mark all as manually collapsed so auto-expand won't override
    ALL_PIPELINE_STEPS.forEach(n => userManualCollapseRef.current.add(n));
    setCollapsedSteps(new Set(ALL_PIPELINE_STEPS));
  }, []);

  const counts = data?.universe_funnel?.counts || {};
  const fundamentalsSyncDetails = data?.fundamentals_sync?.details || {};
  const syncStatus = data?.pipeline_sync_status || {};
  const todayStr = new Date().toISOString().split('T')[0];

  const rawSymbols = (jobRuns['universe_seed'] as any)?.raw_symbols_fetched
    ?? (jobRuns['universe_seed'] as any)?.details?.raw_symbols_fetched as number | undefined;
  const rawPerExchange = (jobRuns['universe_seed'] as any)?.fetched_raw_per_exchange
    ?? (jobRuns['universe_seed'] as any)?.details?.fetched_raw_per_exchange
    ?? exclusionReport?.step1_counts?.fetched_raw_per_exchange as Record<string, number> | undefined;
  // Admin funnel: when a completed chain report is available, use it as the
  // single source of truth so UI and CSV always agree for the same chain_run_id.
  const raw = canonicalReport
    ? asFiniteNumber(canonicalReport.raw_symbols)
    : (asFiniteNumber((jobRuns['universe_seed'] as any)?.raw_rows_total)
      ?? asFiniteNumber((jobRuns['universe_seed'] as any)?.details?.raw_rows_total)
      ?? asFiniteNumber((jobRuns['universe_seed'] as any)?.raw_symbols_fetched));
  const seeded = canonicalReport
    ? asFiniteNumber(canonicalReport.seeded_tickers)
    : asFiniteNumber(counts.seeded);
  const withPrice = canonicalReport
    ? asFiniteNumber(canonicalReport.with_price)
    : asFiniteNumber(counts.with_price);
  const visible = canonicalReport
    ? asFiniteNumber(canonicalReport.visible)
    : asFiniteNumber(counts.visible);

  const byStep = exclusionReport?.by_step;
  const step1Filtered = canonicalReport
    ? asFiniteNumber(canonicalReport.step1_filtered_out)
    : (raw !== undefined && seeded !== undefined ? Math.max(raw - seeded, 0)
      : byStep?.['Step 1 - Universe Seed'] ?? exclusionReport?.step1_counts?.filtered_out_total_step1);
  const step2Filtered = canonicalReport
    ? asFiniteNumber(canonicalReport.step2_filtered_out)
    : (seeded !== undefined && withPrice !== undefined ? Math.max(seeded - withPrice, 0)
      : byStep?.['Step 2 - Price Sync']);
  const step3Filtered = canonicalReport
    ? asFiniteNumber(canonicalReport.step3_filtered_out)
    : (withPrice !== undefined && visible !== undefined ? Math.max(withPrice - visible, 0)
      : byStep?.['Step 3 - Fundamentals Sync']);

  const s1In: number | undefined = raw;
  const s1Out: number | undefined = seeded;
  const s2Out: number | undefined = withPrice;
  // Step 3 now includes fundamentals + visibility gates (merged old steps 3+4)
  const s3Out: number | undefined = visible;

  const JOB_OUTPUT: Record<string, number | undefined> = {
    universe_seed: s1Out,
    price_sync: s2Out,
    fundamentals_sync: s3Out,
  };
  const completedCount = (chainStatus === 'completed' && chainStepsDone.length > 0)
    ? chainStepsDone.length
    : ['universe_seed', 'price_sync', 'fundamentals_sync'].filter(j => {
        const r = jobRuns[j];
        const ok = r?.status === 'success' || r?.status === 'completed';
        return ok && (JOB_OUTPUT[j] === undefined || (JOB_OUTPUT[j] ?? 0) > 0);
      }).length;
  const healthPct = Math.round((completedCount / 3) * 100);
  const healthColor = healthPct === 100 ? '#22C55E' : healthPct >= 60 ? '#F59E0B' : '#EF4444';

  const steps = [
    {
      step: 1,
      job_name: 'universe_seed',
      title: 'Universe Seed',
      schedule: 'Mon–Sat 03:00 Prague',
      scheduledHour: 3,
      scheduledMinute: 0,
      icon: 'globe-outline' as const,
      color: '#6366F1',
      apiUrl: 'https://eodhd.com/api/exchange-symbol-list/{NYSE|NASDAQ}?api_token=YOUR_API_TOKEN&fmt=json  (1 credit/exchange)',
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
      apiUrl: 'https://eodhd.com/api/eod-bulk-last-day/US?date={DATE}&api_token=YOUR_API_TOKEN&fmt=json  (1 credit)',
      apiUrl2: 'https://eodhd.com/api/eod/{TICKER}.US?api_token=YOUR_API_TOKEN&fmt=json  (remediation: 1 credit/ticker)',
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
      apiUrl: 'https://eodhd.com/api/fundamentals/{TICKER}.US?api_token=YOUR_API_TOKEN&fmt=json  (~10 credits/ticker)',
      apiUrl2: 'https://eodhd.com/img/logos/US/{TICKER}.png  (logo CDN, 0 credits)',
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
      title: 'Peer Medians (independent)',
      schedule: 'Daily 05:30 Prague (separate from pipeline)',
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
    <ScrollView style={s.container} contentContainerStyle={{ paddingBottom: 40 }}>
      {/* Skeleton: Health card */}
      <View style={s.healthCard}>
        <View style={{ flexDirection: 'row', alignItems: 'center', marginBottom: 8 }}>
          <View style={{ width: 100, height: 13, borderRadius: 4, backgroundColor: COLORS.border, flex: 1 }} />
          <View style={{ width: 36, height: 20, borderRadius: 4, backgroundColor: COLORS.border, marginLeft: 8 }} />
        </View>
        <View style={s.progressBg} />
        <View style={{ width: 140, height: 11, borderRadius: 4, backgroundColor: COLORS.border, marginBottom: 10 }} />
        <View style={{ width: '100%', height: 48, borderRadius: 8, backgroundColor: COLORS.border, marginTop: 8 }} />
        <View style={{ width: '100%', height: 48, borderRadius: 8, backgroundColor: COLORS.border, marginTop: 12 }} />
      </View>
      {/* Skeleton: Step cards */}
      {[1, 2, 3].map(i => (
        <View key={i} style={[s.stepCard, { marginTop: 10 }]}>
          <View style={s.stepHeader}>
            <View style={[s.stepBadge, { backgroundColor: COLORS.border }]} />
            <View style={s.stepMeta}>
              <View style={{ width: 80, height: 9, borderRadius: 4, backgroundColor: COLORS.border, marginBottom: 4 }} />
              <View style={{ width: 120, height: 13, borderRadius: 4, backgroundColor: COLORS.border }} />
            </View>
            <View style={{ width: 54, height: 20, borderRadius: 4, backgroundColor: COLORS.border }} />
          </View>
        </View>
      ))}
    </ScrollView>
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
        <Text style={s.healthSub}>{completedCount}/3 steps completed today</Text>

        {/* Full Pipeline Audit — above scheduler control */}
        <View style={s.fullChainInlineSection}>
          <View style={s.fullChainInlineTitleRow}>
            <View style={{ flex: 1 }}>
              <Text style={s.fullChainInlineTitle}>Full Pipeline Audit</Text>
              <Text style={s.fullChainInlineDesc} numberOfLines={1}>
                {runMode === 'AUTO' ? 'Scheduler controls automatic runs.' : 'Runs Step 1\u21923 now, generates unified CSV.'}
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
      <Text style={[s.fullChainBtnText, { fontSize: 14 }]} numberOfLines={1}>STOP chain run</Text>
    </TouchableOpacity>
  ) : (
    <TouchableOpacity
      style={[s.fullChainBtn, { width: '100%', minHeight: 48, borderRadius: 8, justifyContent: 'center' }, isRunDisabled && s.runBtnDisabled]}
      onPress={handleRunFullPipeline}
      disabled={isRunDisabled}
    >
      <Text style={[s.fullChainBtnText, { fontSize: 14 }]} numberOfLines={1}>RUN FULL PIPELINE NOW</Text>
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
    <>
    <TouchableOpacity
      style={[s.fullChainDownloadBtn, { marginTop: 12, width: '100%', minHeight: 48, borderRadius: 8, justifyContent: 'center' }]}
      onPress={handleDownloadFullCsv}
    >
      <Ionicons name="download-outline" size={13} color="#fff" />
      <Text style={s.fullChainDownloadBtnText}>Download Unified CSV</Text>
    </TouchableOpacity>
    {canonicalReport?.last_generated_at_prague && (
      <Text style={{ fontSize: 10, color: COLORS.textMuted, marginTop: 4, textAlign: 'center' }}>
        Chain completed: {canonicalReport.last_generated_at_prague}
      </Text>
    )}
    </>
  )}

  {chainStatus && chainStatus !== 'starting' && (() => {
    const failStep = chainFailedStep ?? chainCurrentStep;
    const failStepLabel = failStep != null ? `Step ${failStep}/3 (${CHAIN_STEP_NAMES[failStep] ?? 'unknown'})` : 'unknown step';
    return (
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
        ? `Failed — ${failStepLabel}`
        : isChainCancelled
        ? 'Cancelled'
        : chainCurrentStep !== null
        ? `Running — Step ${chainCurrentStep}/3 (${CHAIN_STEP_NAMES[chainCurrentStep] ?? ''}) · ${formatElapsed(elapsedSeconds)}`
        : 'Running…'}
    </Text>
    );
  })()}
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
            <Text style={s.miniLabel}>with price</Text>
          </View>
          <Ionicons name="chevron-forward" size={10} color={COLORS.textMuted} />
          <View style={s.miniItem}>
            <Text style={[s.miniNum, { color: '#22C55E' }]}>{fmt(visible)}</Text>
            <Text style={[s.miniLabel, { color: '#22C55E' }]}>visible</Text>
          </View>
        </View>
      </View>

      {/* Expand / Collapse all controls */}
      <View style={s.collapseControlRow}>
        <TouchableOpacity style={s.collapseControlBtn} onPress={expandAllSteps}>
          <Ionicons name="expand-outline" size={13} color={COLORS.textMuted} />
          <Text style={s.collapseControlText}>Expand all</Text>
        </TouchableOpacity>
        <TouchableOpacity style={s.collapseControlBtn} onPress={collapseAllSteps}>
          <Ionicons name="contract-outline" size={13} color={COLORS.textMuted} />
          <Text style={s.collapseControlText}>Collapse all</Text>
        </TouchableOpacity>
      </View>

      {/* Pipeline Steps */}
      {steps.map((step, idx) => {
        const run = jobRuns[step.job_name];
        const rawStatus = run?.status;
        // Treat status="running" with finished_at present as an error (stale/stuck run).
        const status = (rawStatus === 'running' && !!run?.finished_at) ? 'error' : rawStatus;
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
        const pendingEventCounts = data?.pipeline_sync_status?.pending_event_counts || {};
        const nextRunLabel = step.step === 1
          ? getNextRun(step.scheduledHour, step.scheduledMinute, true)
          : (!prevRunOk || inCount === 0)
            ? `After Step ${step.step - 1} completion`
            : (!run || currentRunTs < prevRunTs)
              ? 'Ready now'
              : `After next Step ${step.step - 1} completion`;
        const processedCount = step.job_name === 'fundamentals_sync'
          ? asFiniteNumber(fundamentalsSyncDetails.processed)
          : outCount ?? run?.records_processed;
        const processedLabel = step.job_name === 'fundamentals_sync'
          ? 'Processed events/tickers:'
          : 'Processed:';

        // ── Pre-compute run info for header / body split ──────────────────────
        const isStepCollapsed = collapsedSteps.has(step.step);
        let runStatusText = '—';
        let runDurationText = '';
        let isLiveRun = false;
        const showRunError = !!run && (run.status === 'error' || run.status === 'failed' || (run.status === 'running' && !!run.finished_at));
        const runErrorText = showRunError ? extractErrorText(run) : null;

        if (run) {
          const runStart = run.started_at_prague || run.start_time || run.started_at;
          const runEnd = run.finished_at_prague || run.end_time || run.finished_at;
          const lastDuration = run.duration_seconds ?? (
            runStart && runEnd ? Math.max(0, Math.round((Date.parse(runEnd) - Date.parse(runStart)) / 1000)) : undefined
          );
          isLiveRun = run.status === 'running' && !run.finished_at && !chainStepDone;
          const prevCompleted = run.previous_completed_run;
          const prevEnd = prevCompleted?.finished_at_prague || prevCompleted?.finished_at;
          runStatusText = isLiveRun
            ? (prevEnd
              ? formatTime(prevEnd)
              : `Started ${formatTime(runStart)}`)
            : (runEnd || runStart)
              ? formatTime(runEnd || runStart)
              : '—';
          const prevDuration = prevCompleted?.duration_seconds;
          runDurationText = isLiveRun
            ? (prevEnd && prevDuration !== undefined && prevDuration !== null
              ? ` (${formatDuration(prevDuration)})`
              : '')
            : run.status === 'cancelled'
              ? (lastDuration !== undefined ? ` (stopped after ${formatDuration(lastDuration)})` : '')
              : lastDuration !== undefined
                ? ` (${formatDuration(lastDuration)})`
                : '';
        }

      return (
        <View key={step.job_name}>
          <View style={s.stepCard}>

            {/* ── Clickable Step Header ── */}
            <TouchableOpacity activeOpacity={0.7} onPress={() => toggleStepCollapsed(step.step)}>
            <View style={s.stepHeader}>
                <View style={[s.stepBadge, { backgroundColor: step.color + '22' }]}>
                  <Ionicons name={step.icon} size={16} color={step.color} />
                </View>
                <View style={s.stepMeta}>
                  <View style={s.stepTitleRow}>
                    <Text style={s.stepNum}>STEP {step.step}</Text>
                    <Text style={s.stepTitle}>{step.title}</Text>
                    {chainStepRunning ? (
                      <ActivityIndicator size="small" color="#F59E0B" style={{ marginLeft: 4 }} />
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
                <Ionicons
                  name={isStepCollapsed ? 'chevron-down' : 'chevron-up'}
                  size={16}
                  color={COLORS.textMuted}
                  style={{ marginLeft: 4 }}
                />
            </View>
            </TouchableOpacity>

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

              {/* ── Always-visible: Last run compact + cancel + error ── */}
              {run ? (
                <View style={s.runInfo}>
                  <View style={s.runInfoRow}>
                    <Text style={s.runLabel}>Last run:</Text>
                    <Text style={[s.runValue, { color: getStatusColor(run.status) }]}>
                      {runStatusText}{runDurationText}
                    </Text>
                  </View>
                  {processedCount !== undefined && processedCount > 0 && (
                    <View style={s.runInfoRow}>
                      <Text style={s.runLabel}>{processedLabel}</Text>
                      <Text style={s.runValue}>
                        {processedCount.toLocaleString()}
                        {step.job_name === 'universe_seed' && rawSymbols ? ` / ${rawSymbols.toLocaleString()}` : ''}
                      </Text>
                    </View>
                  )}
                  {(step.job_name === 'price_sync' || step.job_name === 'fundamentals_sync') && run.status === 'running' && !run.finished_at && (
                    <TouchableOpacity
                      style={s.cancelRunningBtn}
                      onPress={() => handleCancelRunningJob(step.job_name as 'price_sync' | 'fundamentals_sync')}
                    >
                      <Text style={s.cancelRunningBtnText}>Cancel running</Text>
                    </TouchableOpacity>
                  )}
                  {runErrorText ? <Text style={s.errorText}>⚠️ {runErrorText}</Text> : null}
                  {/* Collapsible: extra run details */}
                  {!isStepCollapsed && (
                    <>
                      {run.triggered_by && (
                        <View style={s.runInfoRow}>
                          <Text style={s.runLabel}>Triggered by:</Text>
                          <Text style={s.runValue}>{run.triggered_by}</Text>
                        </View>
                      )}
                      {step.job_name !== 'price_sync' && (
                        <View style={s.runInfoRow}>
                          <Text style={s.runLabel}>Next run:</Text>
                          <Text style={s.runValue}>{nextRunLabel}</Text>
                        </View>
                      )}
                    </>
                  )}
                </View>
              ) : step.job_name !== 'price_sync' ? (
                <View style={s.runInfo}>
                  <View style={s.runInfoRow}>
                    <Text style={s.runLabel}>Next run:</Text>
                    <Text style={s.runValue}>{nextRunLabel}</Text>
                  </View>
                </View>
              ) : null}

              {/* ── Running progress summary (visible even when collapsed) ── */}
              {isLiveRun && isStepCollapsed && step.job_name === 'price_sync' && step2Progress !== null && (
                <View style={s.headerProgressRow}>
                  <View style={[s.headerProgressBar, { backgroundColor: COLORS.border }]}>
                    <View style={[s.headerProgressBarFill, {
                      width: `${Math.min(step2Progress.pct, 100)}%` as any,
                      backgroundColor: '#10B981',
                    }]} />
                  </View>
                  <Text style={s.headerProgressText}>
                    {step2Progress.phase === '2.1_bulk_catchup' ? '2.1 Bulk' : step2Progress.phase?.replaceAll('_', ' ') ?? 'Syncing'}
                    {' · '}{step2Progress.pct}%
                    {step2Progress.total > 0 ? ` · ${fmt(step2Progress.processed)}/${fmt(step2Progress.total)}` : ''}
                  </Text>
                </View>
              )}
              {isLiveRun && isStepCollapsed && step.job_name === 'universe_seed' && step1Progress !== null && (
                <View style={s.headerProgressRow}>
                  <View style={[s.headerProgressBar, { backgroundColor: COLORS.border }]}>
                    <View style={[s.headerProgressBarFill, {
                      width: `${Math.min(step1Progress.pct, 100)}%` as any,
                      backgroundColor: '#6366F1',
                    }]} />
                  </View>
                  <Text style={s.headerProgressText}>
                    Seeding · {step1Progress.pct}% · {fmt(step1Progress.processed)}/{fmt(step1Progress.total)}
                  </Text>
                </View>
              )}

              {/* ── Collapsible Body ── */}
              {!isStepCollapsed && (
                <>
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
                const seedSeeded = seeded ?? step1Progress?.total;
                if (isStep1Running && step1Progress !== null) {
                  // Live progress while running
                  const liveSeeded = seedSeeded ?? step1Progress.total;
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
                          {seedRawFetched ? `${fmt(seedRawFetched)} fetched → ` : ''}{fmt(liveSeeded)} seeded → {fmt(step1Progress.processed)} written
                        </Text>
                      </View>
                    </View>
                  );
                }
                if (!isStep1Running && (seedRawFetched || seedSeeded)) {
                  // Final summary after completion: written == seeded
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
                          {seedRawFetched && seedSeeded ? ' → ' : ''}
                          {seedSeeded ? `${fmt(seedSeeded)} seeded → ${fmt(seedSeeded)} written` : ''}
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
                  {/* 2.1 Bulk catchup (dated) */}
                  <View style={s.substepBlock}>
                    <View style={s.substepHeaderRow}>
                      <Text style={s.substepName}>2.1 Bulk catchup (dated)</Text>
                    </View>
                    {substepLastRunLabel && (
                      <Text style={s.substepLastRun}>{substepLastRunLabel}</Text>
                    )}
                    <Text style={s.substepDesc}>Fetches bulk EOD prices for a specific trading day (explicit ?date= parameter). Writes to stock_prices for all seeded NYSE/NASDAQ common stock tickers present in the EODHD bulk response.</Text>
                    <Text style={s.substepEndpoint} numberOfLines={1}>
                      {`https://eodhd.com/api/eod-bulk-last-day/US?date=${eventDetectors.today || todayStr}&api_token=YOUR_API_TOKEN&fmt=json`}
                    </Text>
                  </View>

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
                    <Text style={s.substepDesc}>Detects stock splits today. Flagged tickers will be processed in Step 3 (after fundamentals + visibility) for any needed price history remediation and fundamentals refresh.</Text>
                    <Text style={s.substepEndpoint} numberOfLines={1}>
                      {splitDetector.api_endpoint || `https://eodhd.com/api/eod-bulk-last-day/US?type=splits&date=${eventDetectors.today || todayStr}&api_token=YOUR_API_TOKEN&fmt=json`}
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
                    <View style={s.substepAuditPendingRow}>
                      <Text style={s.substepAuditLabel}>Detected (audit): {fmt(safeCount(splitDetector.flagged_count))}</Text>
                      <Text style={s.substepPendingLabel}>Pending (queue): {fmt(safeCount(pendingEventCounts.split))}</Text>
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
                    <Text style={s.substepDesc}>Detects ex-dividend events today. Flagged tickers will be processed in Step 3 (after fundamentals + visibility) for any needed price history remediation and fundamentals refresh (dividend yield, payout ratio).</Text>
                    <Text style={s.substepEndpoint} numberOfLines={1}>
                      {dividendDetector.api_endpoint || `https://eodhd.com/api/eod-bulk-last-day/US?type=dividends&date=${eventDetectors.today || todayStr}&api_token=YOUR_API_TOKEN&fmt=json`}
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
                    <View style={s.substepAuditPendingRow}>
                      <Text style={s.substepAuditLabel}>Detected (audit): {fmt(safeCount(dividendDetector.flagged_count))}</Text>
                      <Text style={s.substepPendingLabel}>Pending (queue): {fmt(safeCount(pendingEventCounts.dividend))}</Text>
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
                      {earningsDetector.api_endpoint || `https://eodhd.com/api/calendar/earnings?from=${eventDetectors.today || todayStr}&to=${eventDetectors.today || todayStr}&api_token=YOUR_API_TOKEN&fmt=json`}
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
                    <View style={s.substepAuditPendingRow}>
                      <Text style={s.substepAuditLabel}>Detected (audit): {fmt(safeCount(earningsDetector.flagged_count))}</Text>
                      <Text style={s.substepPendingLabel}>Pending (queue): {fmt(safeCount(pendingEventCounts.earnings))}</Text>
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

              {/* Fundamentals completeness — inside Step 3 */}
              {step.job_name === 'fundamentals_sync' && (
                <View style={{ marginTop: 8, marginHorizontal: 4 }}>
                  <View style={s.syncRow}>
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
                        🔄 {fmt(syncStatus.needs_fundamentals_refresh)} Pending Refresh
                      </Text>
                    )}
                    <Text style={s.syncQueueText}>
                      Pending events (audit only): {fmt(syncStatus.pending_events_audit ?? 0)}
                    </Text>
                  </View>
                </View>
              )}

              {/* ── Step 3 Phase Telemetry ── */}
              {step.job_name === 'fundamentals_sync' && step3Telemetry && step3Telemetry.run_id && (() => {
                const tel = step3Telemetry;
                const phases = tel.phases || {};
                const activePhase: string | null = tel.active_phase ?? null;
                const isTerminal = S3_TERMINAL_STATUSES.includes(tel.status);

                return (
                  <View style={s.substepsCard}>
                    <View style={{ flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
                      <Text style={s.substepsTitle}>Step 3 Phase Progress</Text>
                      {isTerminal ? (
                        <View style={{ backgroundColor: tel.status === 'success' || tel.status === 'completed' ? '#22C55E22' : '#EF444422', paddingHorizontal: 6, paddingVertical: 2, borderRadius: 4 }}>
                          <Text style={{ fontSize: 9, fontWeight: '700', color: tel.status === 'success' || tel.status === 'completed' ? '#22C55E' : '#EF4444' }}>
                            {tel.status.toUpperCase()}
                          </Text>
                        </View>
                      ) : tel.status === 'running' ? (
                        <View style={{ flexDirection: 'row', alignItems: 'center', gap: 4 }}>
                          <ActivityIndicator size="small" color="#3B82F6" />
                          <Text style={{ fontSize: 9, fontWeight: '700', color: '#3B82F6' }}>RUNNING</Text>
                        </View>
                      ) : null}
                    </View>

                    {/* Updated at */}
                    {tel.updated_at_prague && (
                      <Text style={{ fontSize: 9, color: COLORS.textMuted, marginBottom: 6 }}>
                        Last telemetry: {formatTime(tel.updated_at_prague)}
                      </Text>
                    )}

                    {/* Per-phase rows */}
                    {(['A', 'B', 'C'] as const).map((pk) => {
                      const phase = phases[pk];
                      if (!phase) return null;
                      const isActive = activePhase === pk && !isTerminal;
                      const si = S3_STATUS_ICONS[phase.status] || S3_STATUS_ICONS.idle;
                      const hasCounts = phase.total != null && phase.total > 0;
                      const phasePct = phase.pct != null ? Math.min(phase.pct, 100) : 0;

                      return (
                        <View
                          key={pk}
                          style={{
                            marginBottom: 8,
                            paddingVertical: 6,
                            paddingHorizontal: 8,
                            borderRadius: 6,
                            backgroundColor: isActive ? S3_PHASE_COLORS[pk] + '11' : 'transparent',
                            borderWidth: isActive ? 1 : 0,
                            borderColor: isActive ? S3_PHASE_COLORS[pk] + '44' : 'transparent',
                          }}
                        >
                          {/* Phase header */}
                          <View style={{ flexDirection: 'row', alignItems: 'center', gap: 6, marginBottom: 3 }}>
                            <Ionicons name={si.icon as any} size={13} color={si.color} />
                            <Text style={{ fontSize: 11, fontWeight: '600', color: COLORS.text, flex: 1 }}>
                              {S3_PHASE_LABELS[pk]}
                            </Text>
                            <Text style={{ fontSize: 9, fontWeight: '600', color: si.color }}>
                              {phase.status.toUpperCase()}
                            </Text>
                          </View>

                          {/* Progress bar + counts */}
                          {hasCounts && (
                            <View style={{ marginTop: 2 }}>
                              <View style={{ height: 4, backgroundColor: COLORS.border, borderRadius: 2, overflow: 'hidden', marginBottom: 3 }}>
                                <View style={{ height: 4, borderRadius: 2, width: `${phasePct}%` as any, backgroundColor: S3_PHASE_COLORS[pk] }} />
                              </View>
                              <View style={{ flexDirection: 'row', justifyContent: 'space-between' }}>
                                <Text style={{ fontSize: 10, color: COLORS.textMuted }}>
                                  {fmt(phase.processed)} / {fmt(phase.total)}
                                </Text>
                                <Text style={{ fontSize: 10, fontWeight: '600', color: S3_PHASE_COLORS[pk] }}>
                                  {phase.pct != null ? `${Math.round(phase.pct * 10) / 10}%` : '—'}
                                </Text>
                              </View>
                            </View>
                          )}

                          {/* Message */}
                          {phase.message && (
                            <Text style={{ fontSize: 10, color: COLORS.textMuted, marginTop: 2 }} numberOfLines={2}>
                              {phase.message}
                            </Text>
                          )}

                          {/* Phase C selection audit */}
                          {pk === 'C' && phase.selection_audit && (() => {
                            const sa = phase.selection_audit;
                            const counts = sa.counts_by_reason || {};
                            const samples = sa.sample_tickers_by_reason || {};
                            const reasons = Object.keys(counts);
                            if (reasons.length === 0) return null;
                            return (
                              <View style={{ marginTop: 6, paddingTop: 6, borderTopWidth: 1, borderTopColor: COLORS.border + '66' }}>
                                <Text style={{ fontSize: 9, fontWeight: '700', color: COLORS.textMuted, textTransform: 'uppercase', marginBottom: 4 }}>
                                  Selection Summary
                                </Text>
                                {reasons.map((reason) => (
                                  <View key={reason} style={{ marginBottom: 4 }}>
                                    <View style={{ flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center' }}>
                                      <Text style={{ fontSize: 10, color: COLORS.text }}>{reason.replace(/_/g, ' ')}</Text>
                                      <Text style={{ fontSize: 10, fontWeight: '700', color: S3_PHASE_COLORS.C }}>{fmt(counts[reason])}</Text>
                                    </View>
                                    {samples[reason] && samples[reason].length > 0 && (
                                      <Text style={{ fontSize: 9, color: COLORS.textMuted, fontFamily: 'monospace', marginTop: 1 }} numberOfLines={1}>
                                        {samples[reason].slice(0, 5).join(', ')}{samples[reason].length > 5 ? ' …' : ''}
                                      </Text>
                                    )}
                                  </View>
                                ))}
                              </View>
                            );
                          })()}
                        </View>
                      );
                    })}
                  </View>
                );
              })()}

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
                      {/* Visibility badge + Funnel step */}
                      <View style={s.auditVerdictRow}>
                        <View style={[
                          s.auditVerdictBadge,
                          { backgroundColor: auditResult.is_visible ? '#22C55E22' : '#EF444422' },
                        ]}>
                          <Text style={[
                            s.auditVerdictText,
                            { color: auditResult.is_visible ? '#22C55E' : '#EF4444' },
                          ]}>
                            {auditResult.is_visible ? 'VISIBLE' : 'NOT VISIBLE'}
                          </Text>
                        </View>
                        <Text style={s.auditTickerLabel}>{auditResult.ticker}</Text>
                      </View>

                      {/* Primary funnel reason */}
                      {auditResult.funnel_step && (
                        <View style={s.auditSection}>
                          <Text style={[s.auditSectionTitleMuted, { fontWeight: '700' }]}>
                            Funnel: {auditResult.funnel_step}
                          </Text>
                          {auditResult.primary_reason ? (
                            <Text style={s.auditItemMuted}>↳ {auditResult.primary_reason}</Text>
                          ) : null}
                        </View>
                      )}

                      {/* Audit verdict */}
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
                  <Text style={s.detailLabel}>API ENDPOINT{step.apiUrl2 ? 'S' : ''}</Text>
                  <Text style={s.apiText}>· {step.apiUrl}</Text>
                  {step.apiUrl2 && <Text style={s.apiText}>· {step.apiUrl2}</Text>}
                  <Text style={[s.detailLabel, { marginTop: 8 }]}>EXCLUDED IF</Text>
                  {step.filters.map(f => (
                    <Text key={f} style={s.filterText}>✕ {f}</Text>
                  ))}
                </View>
              )}
                </>
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
                if (isNewsRefreshRunning) {
                  return <ActivityIndicator size="small" color="#06B6D4" style={{ marginLeft: 4 }} />;
                }
                return run ? (
                  <Ionicons name={getStatusIcon(run.status) as any} size={14} color={getStatusColor(run.status)} style={{ marginLeft: 4 }} />
                ) : null;
              })()}
            </View>
            <Text style={s.stepSchedule}>Daily 13:00 Prague</Text>
          </View>
          <TouchableOpacity
            style={[
              { paddingHorizontal: 12, paddingVertical: 6, borderRadius: 6, backgroundColor: '#06B6D4' },
              isNewsRefreshRunning && { opacity: 0.5 },
            ]}
            onPress={handleRunNewsRefresh}
            disabled={isNewsRefreshRunning}
          >
            {isNewsRefreshRunning
              ? <ActivityIndicator size="small" color="#fff" />
              : <Text style={{ color: '#fff', fontSize: 11, fontWeight: '700' }}>Run Now</Text>}
          </TouchableOpacity>
        </View>
        {isNewsRefreshRunning ? (
          <View style={s.runInfo}>
            <Text style={[s.runValue, { color: '#06B6D4' }]}>Running…</Text>
            {(() => {
              const tel = jobRuns['news_refresh']?.details?.news_refresh_telemetry;
              if (!tel) return <Text style={[s.detailValue, { color: '#94A3B8', marginTop: 2 }]}>No telemetry yet</Text>;
              return (
                <View style={{ marginTop: 4 }}>
                  <Text style={[s.detailValue, { color: '#06B6D4' }]}>{tel.phase}: {tel.message}</Text>
                  {tel.tickers_total > 0 && (
                    <Text style={s.detailValue}>Progress: {tel.tickers_done}/{tel.tickers_total}{tel.tickers_failed > 0 ? ` · ${tel.tickers_failed} failed` : ''}</Text>
                  )}
                  {tel.api_calls > 0 && <Text style={s.detailValue}>API calls: {tel.api_calls}</Text>}
                  {tel.last_ticker ? <Text style={s.detailValue}>Last ticker: {tel.last_ticker}</Text> : null}
                  {tel.last_error ? <Text style={[s.detailValue, { color: '#EF4444' }]}>Error: {tel.last_error}</Text> : null}
                </View>
              );
            })()}
          </View>
        ) : jobRuns['news_refresh'] ? (
          <View style={s.runInfo}>
            <Text style={s.runValue}>
              Last: {formatTime(jobRuns['news_refresh'].start_time)} · {formatDuration(jobRuns['news_refresh'].duration_seconds)}
            </Text>
          </View>
        ) : (
          <Text style={s.neverRun}>Never run</Text>
        )}
        <Text style={[s.detailValue, { marginTop: 4 }]}>Fetch news + compute sentiment for tracked tickers</Text>
        <Text style={s.apiText}>· https://eodhd.com/api/news?s={'{SYMBOL}'}.US&api_token=YOUR_API_TOKEN&limit=10&offset=0&fmt=json  (per tracked ticker)</Text>
      </View>

      {/* Benchmark Update — independent job, not part of universe pipeline */}
      <View style={[s.stepCard, { marginTop: 16, borderLeftColor: '#8B5CF6', borderLeftWidth: 3 }]}>
        <View style={s.stepHeader}>
          <View style={[s.stepBadge, { backgroundColor: '#8B5CF622' }]}>
            <Ionicons name="trending-up-outline" size={16} color="#8B5CF6" />
          </View>
          <View style={s.stepMeta}>
            <View style={s.stepTitleRow}>
              <Text style={s.stepTitle}>Benchmark Update</Text>
              {(() => {
                const run = jobRuns['benchmark_update'];
                if (isBenchmarkRunning) {
                  return <ActivityIndicator size="small" color="#F59E0B" style={{ marginLeft: 4 }} />;
                }
                return run ? (
                  <Ionicons name={getStatusIcon(run.status) as any} size={14} color={getStatusColor(run.status)} style={{ marginLeft: 4 }} />
                ) : null;
              })()}
            </View>
            <Text style={s.stepSchedule}>Daily 04:15 Prague · SP500TR</Text>
          </View>
          <TouchableOpacity
            style={[
              { paddingHorizontal: 12, paddingVertical: 6, borderRadius: 6, backgroundColor: '#8B5CF6' },
              isBenchmarkRunning && { opacity: 0.5 },
            ]}
            onPress={handleRunBenchmarkUpdate}
            disabled={isBenchmarkRunning}
          >
            {isBenchmarkRunning
              ? <ActivityIndicator size="small" color="#fff" />
              : <Text style={{ color: '#fff', fontSize: 11, fontWeight: '700' }}>Run Now</Text>}
          </TouchableOpacity>
        </View>
        {isBenchmarkRunning ? (
          <View style={s.runInfo}>
            <Text style={[s.runValue, { color: '#F59E0B' }]}>Running…</Text>
            {jobRuns['benchmark_update']?.phase ? (
              <Text style={[s.runValue, { marginTop: 2, fontSize: 11, color: '#9CA3AF' }]}>
                {jobRuns['benchmark_update'].phase}
              </Text>
            ) : null}
            {jobRuns['benchmark_update']?.progress ? (
              <Text style={[s.runValue, { marginTop: 1, fontSize: 10, color: '#6B7280' }]}>
                {jobRuns['benchmark_update'].progress}
              </Text>
            ) : null}
          </View>
        ) : jobRuns['benchmark_update'] ? (
          <View style={s.runInfo}>
            <Text style={s.runValue}>
              Last: {formatTime(jobRuns['benchmark_update'].start_time)} · {formatDuration(jobRuns['benchmark_update'].duration_seconds)}
            </Text>
            {jobRuns['benchmark_update'].triggered_by && (
              <Text style={[s.runValue, { marginTop: 2 }]}>
                Triggered by: {jobRuns['benchmark_update'].triggered_by}
              </Text>
            )}
          </View>
        ) : (
          <Text style={s.neverRun}>Never run</Text>
        )}
        <Text style={[s.detailValue, { marginTop: 4 }]}>Incremental update of S&P 500 Total Return benchmark (SP500TR.INDX)</Text>
        <Text style={s.apiText}>· https://eodhd.com/api/eod/SP500TR.INDX?api_token=YOUR_API_TOKEN&from={'{DATE}'}&to={'{DATE}'}&fmt=json</Text>
      </View>

      {/* ── All External API Endpoints Reference ── */}
      <View style={[s.stepCard, { marginTop: 20, borderLeftColor: '#64748B', borderLeftWidth: 3 }]}>
        <View style={s.stepHeader}>
          <View style={[s.stepBadge, { backgroundColor: '#64748B22' }]}>
            <Ionicons name="link-outline" size={16} color="#64748B" />
          </View>
          <View style={s.stepMeta}>
            <Text style={s.stepTitle}>All External API Endpoints</Text>
            <Text style={s.stepSchedule}>Complete reference of every external call</Text>
          </View>
        </View>

        <View style={{ marginTop: 10 }}>
          <Text style={[s.detailLabel, { marginBottom: 4 }]}>EODHD — PIPELINE (Steps 1-3)</Text>
          <Text style={s.apiText}>· GET https://eodhd.com/api/exchange-symbol-list/{'{NYSE|NASDAQ}'}?api_token=YOUR_API_TOKEN&fmt=json  (Step 1 · 1 credit/exchange)</Text>
          <Text style={s.apiText}>· GET https://eodhd.com/api/eod-bulk-last-day/US?date={'{DATE}'}&api_token=YOUR_API_TOKEN&fmt=json  (Step 2.1 · 1 credit)</Text>
          <Text style={s.apiText}>· GET https://eodhd.com/api/eod-bulk-last-day/US?type=splits&date={'{DATE}'}&api_token=YOUR_API_TOKEN&fmt=json  (Step 2.2 · 1 credit)</Text>
          <Text style={s.apiText}>· GET https://eodhd.com/api/eod-bulk-last-day/US?type=dividends&date={'{DATE}'}&api_token=YOUR_API_TOKEN&fmt=json  (Step 2.4 · 1 credit)</Text>
          <Text style={s.apiText}>· GET https://eodhd.com/api/calendar/earnings?from={'{DATE}'}&to={'{DATE}'}&api_token=YOUR_API_TOKEN&fmt=json  (Step 2.6 · 1 credit)</Text>
          <Text style={s.apiText}>· GET https://eodhd.com/api/fundamentals/{'{TICKER}'}.US?api_token=YOUR_API_TOKEN&fmt=json  (Step 3 · 10 credits/ticker)</Text>
          <Text style={s.apiText}>· GET https://eodhd.com/img/logos/US/{'{TICKER}'}.png  (Step 3 logo · CDN, 0 credits)</Text>

          <Text style={[s.detailLabel, { marginTop: 10, marginBottom: 4 }]}>EODHD — REMEDIATION & BACKFILL</Text>
          <Text style={s.apiText}>· GET https://eodhd.com/api/eod/{'{TICKER}'}.US?api_token=YOUR_API_TOKEN&from={'{DATE}'}&to={'{DATE}'}&fmt=json  (price redownload · 1 credit/ticker)</Text>

          <Text style={[s.detailLabel, { marginTop: 10, marginBottom: 4 }]}>EODHD — INDEPENDENT JOBS</Text>
          <Text style={s.apiText}>· GET https://eodhd.com/api/exchange-details/US?api_token=YOUR_API_TOKEN&fmt=json  (Market Calendar · daily 02:00 · 1 credit)</Text>
          <Text style={s.apiText}>· GET https://eodhd.com/api/news?s={'{SYMBOL}'}.US&api_token=YOUR_API_TOKEN&limit=10&offset=0&fmt=json  (News Refresh · 13:00 · 1 credit/ticker)</Text>
          <Text style={s.apiText}>· GET https://eodhd.com/api/eod/SP500TR.INDX?api_token=YOUR_API_TOKEN&from={'{DATE}'}&to={'{DATE}'}&fmt=json  (Benchmark · 04:15 · 1 credit)</Text>

          <Text style={[s.detailLabel, { marginTop: 10, marginBottom: 4 }]}>GOOGLE OAUTH (on user login)</Text>
          <Text style={s.apiText}>· POST https://oauth2.googleapis.com/token  (exchange code for token)</Text>
          <Text style={s.apiText}>· GET https://www.googleapis.com/oauth2/v2/userinfo  (fetch user profile)</Text>
        </View>
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
  stepTitleRow: { flexDirection: 'row', alignItems: 'center', gap: 4, flexWrap: 'wrap' },
  stepNum: { fontSize: 9, fontWeight: '700', color: COLORS.textMuted, letterSpacing: 0.5 },
  stepTitle: { fontSize: 13, fontWeight: '600', color: COLORS.text, flexShrink: 1 },
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
  cancelRunningBtn: {
    marginTop: 8,
    alignSelf: 'flex-start',
    backgroundColor: '#EF4444',
    borderRadius: 6,
    paddingHorizontal: 10,
    paddingVertical: 6,
  },
  cancelRunningBtnText: { color: '#fff', fontSize: 11, fontWeight: '700' },
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
  substepAuditPendingRow: { flexDirection: 'row', justifyContent: 'space-between', marginTop: 6, paddingHorizontal: 2 },
  substepAuditLabel: { fontSize: 9, color: COLORS.textMuted },
  substepPendingLabel: { fontSize: 9, fontWeight: '600', color: '#6366F1' },
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
  // ── Collapse controls ───────────────────────────────────────────────────
  collapseControlRow: { flexDirection: 'row', justifyContent: 'flex-end', gap: 12, marginHorizontal: 12, marginTop: 12, marginBottom: 2 },
  collapseControlBtn: { flexDirection: 'row', alignItems: 'center', gap: 4 },
  collapseControlText: { fontSize: 11, color: COLORS.textMuted },
  headerProgressRow: { marginTop: 6, gap: 3 },
  headerProgressBar: { height: 4, borderRadius: 2, overflow: 'hidden' },
  headerProgressBarFill: { height: 4, borderRadius: 2 },
  headerProgressText: { fontSize: 10, color: COLORS.textMuted },
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
