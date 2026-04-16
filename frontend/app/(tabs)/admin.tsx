/**
 * RICHSTOX Admin Panel
 * ====================
 * 3 tabs: Dashboard · Pipeline · Customers
 */

import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  View, Text, ScrollView, TouchableOpacity, StyleSheet,
  RefreshControl, ActivityIndicator, TextInput, Modal,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { Ionicons } from '@expo/vector-icons';
import { useAuth } from '../../contexts/AuthContext';
import { useAppDialog } from '../../contexts/AppDialogContext';
import { COLORS } from '../_layout';
import AppHeader from '../../components/AppHeader';
import BrandedLoading from '../../components/BrandedLoading';
import PipelineTab from '../admin/pipeline';
import CustomersTab from '../admin/customers';
import { API_URL } from '../../utils/config';

type Tab = 'dashboard' | 'pipeline' | 'customers';

// ─── Types ────────────────────────────────────────────────────────────────────

interface CoverageCheckpoint {
  date?: string | null;
  have_price_count?: number;
  today_visible?: number;
  kind?: 'recent' | 'historical';
}

interface CompletedTradingDayHealth {
  days?: { date: string; ok: boolean }[];
  ok_count?: number;
  missing_count?: number;
  missing_dates?: string[];
  status?: 'green' | 'yellow' | 'red';
  calendar_stale?: boolean;
  calendar_gap_dates?: string[];
  message?: string;
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
  non_gap_free_sample?: { ticker: string; missing_dates: string[]; classification?: string }[];
  gap_excluded_sample?: { ticker: string; excluded_dates: { date: string; reason: string }[]; classification?: string }[];
  top_missing_dates?: { date: string; missing_ticker_count: number }[];
  fundamentals_complete_count?: number;
  completed_trading_days_health?: CompletedTradingDayHealth | null;
  coverage_checkpoints?: Record<string, CoverageCheckpoint>;
}

interface PipelineAge {
  pipeline_hours_since_success?: number | null;
  pipeline_status?: string;
  morning_refresh_hours_since_success?: number | null;
  morning_refresh_status?: string;
}

interface BulkCompletenessBaseline {
  completed_at?: string | null;
  completed_at_prague?: string | null;
  through_date?: string | null;
  job_run_id?: string | null;
}

interface BulkCompleteness {
  has_baseline?: boolean;
  baseline?: BulkCompletenessBaseline | null;
  missing_bulk_dates_since_baseline?: string[];
  missing_count?: number | null;
  latest_bulk_date_ingested?: string | null;
  gap_free_since_baseline?: boolean | null;
  expected_days_count?: number | null;
  ingested_days_count?: number | null;
  ingested_dates?: string[];
  message?: string;
}

interface VisibleCoverage {
  visible_total?: number;
  latest_bulk_date?: string | null;
  price_coverage_count?: number;
  price_coverage_pct?: number;
  fundamentals_complete_count?: number;
  fundamentals_complete_pct?: number;
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
  eodhd_api_usage?: {
    eodhd_api_calls_today?: number | null;
    eodhd_daily_limit?: number;
  };
  bulk_completeness?: BulkCompleteness;
  visible_coverage?: VisibleCoverage;
  job_last_runs?: Record<string, any>;
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
  if (h == null || !isFinite(h)) return '—';
  if (h < 1) return `${Math.round(h * 60)}m ago`;
  return `${h.toFixed(1)}h ago`;
}

// ─── Dashboard Tab ────────────────────────────────────────────────────────────

interface DashboardProps {
  sessionToken: string | null;
}

function DashboardTab({ sessionToken }: DashboardProps) {
  const dialog = useAppDialog();
  const [overview, setOverview] = useState<OverviewData | null>(null);
  const [stats, setStats] = useState<StatsData | null>(null);
  const [calendarSummary, setCalendarSummary] = useState<{
    today?: string;
    today_is_trading_day?: boolean;
    today_holiday_name?: string | null;
    last_closing_day?: string | null;
    latest_trading_day?: string | null;
    next_trading_day?: string | null;
    calendar_fresh?: boolean;
  } | null>(null);
  const [initialLoad, setInitialLoad] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  // ── Calendar refresh state ──────────────────────────────────────────────
  const [calendarRefreshing, setCalendarRefreshing] = useState(false);

  // ── Collapse toggles for vertical-space savings ────────────────────────
  const [showTradingDays, setShowTradingDays] = useState(false);
  const [showBulkDetails, setShowBulkDetails] = useState(false);

  // ── News refresh (Morning Refresh) state ────────────────────────────────
  const [newsRefreshTriggered, setNewsRefreshTriggered] = useState(false);
  const newsRefreshPollRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [liveNewsRun, setLiveNewsRun] = useState<Record<string, any> | null>(null);
  const wasPollingNewsRef = useRef(false);

  const authHeaders: Record<string, string> = sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {};

  const fetchAll = useCallback(async () => {
    const requestHeaders = sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {};

    // Fire all requests in parallel but process each response as soon as it arrives
    // so the UI can render progressively instead of blocking on the slowest call.
    const settle = async <T,>(
      promise: Promise<Response>,
      setter: React.Dispatch<React.SetStateAction<T | null>>,
    ) => {
      try {
        const res = await promise;
        if (res.ok) setter(await res.json());
      } catch (e) { console.error('Dashboard fetch error', e); }
    };

    const promises = [
      settle(fetch(`${API_URL}/api/admin/overview`, { headers: requestHeaders }), setOverview),
      settle(fetch(`${API_URL}/api/admin/stats`, { headers: requestHeaders }), setStats),
      settle(fetch(`${API_URL}/api/admin/market-calendar-summary`, { headers: requestHeaders }), setCalendarSummary),
    ];

    await Promise.allSettled(promises);
    setInitialLoad(false);
    setRefreshing(false);
  }, [sessionToken]);

  useEffect(() => { fetchAll(); }, [fetchAll]);
  const onRefresh = () => { setRefreshing(true); fetchAll(); };

  // Derive news_refresh running state from overview snapshot + live poll data
  const snapshotNewsRun = overview?.job_last_runs?.['news_refresh'] ?? null;
  const newsRun = liveNewsRun ?? snapshotNewsRun;
  const newsRunStatus = newsRun?.status as string | undefined;
  const isNewsRefreshRunning = newsRefreshTriggered || (newsRunStatus === 'running' && !newsRun?.finished_at && !newsRun?.end_time);

  // Poll news_refresh status while running (mirrors Benchmark Update pattern)
  useEffect(() => {
    if (newsRunStatus !== 'running' || !!newsRun?.finished_at || !!newsRun?.end_time || !sessionToken) {
      if (newsRefreshPollRef.current) {
        clearTimeout(newsRefreshPollRef.current);
        newsRefreshPollRef.current = null;
      }
      if (!sessionToken) {
        setLiveNewsRun(null);
        wasPollingNewsRef.current = false;
        return;
      }
      // When job finishes after we were polling, refresh overview for pipeline_age
      if (wasPollingNewsRef.current && newsRunStatus && newsRunStatus !== 'running') {
        fetchAll();
        setLiveNewsRun(null);
      }
      wasPollingNewsRef.current = false;
      return;
    }
    wasPollingNewsRef.current = true;
    let cancelled = false;
    const poll = async () => {
      try {
        const res = await fetch(`${API_URL}/api/admin/job/news_refresh/status`, {
          headers: { Authorization: `Bearer ${sessionToken}` },
        });
        if (res.ok && !cancelled) {
          const payload = await res.json();
          if (payload.last_run) {
            setLiveNewsRun(payload.last_run);
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
  }, [newsRunStatus, sessionToken]);

  const handleRunNewsRefresh = async () => {
    setNewsRefreshTriggered(true);
    try {
      const res = await fetch(`${API_URL}/api/admin/job/news_refresh/run`, {
        method: 'POST',
        headers: authHeaders,
      });
      const payload = await res.json().catch(() => ({}));
      if (!res.ok) {
        const detail = payload?.detail;
        const msg = typeof detail === 'object' ? detail?.message : detail || payload?.message || res.statusText;
        throw new Error(msg);
      }
      // Refresh overview to pick up the new "running" sentinel
      await fetchAll();
    } catch (e: any) {
      dialog.alert('Morning Refresh', e?.message || 'Could not start news refresh');
    } finally {
      setNewsRefreshTriggered(false);
    }
  };

  const handleRefreshCalendar = async () => {
    setCalendarRefreshing(true);
    try {
      const res = await fetch(`${API_URL}/api/admin/job/market_calendar/run`, {
        method: 'POST',
        headers: authHeaders,
      });
      const payload = await res.json().catch(() => ({}));
      if (!res.ok) {
        const detail = payload?.detail;
        const msg = typeof detail === 'object' ? detail?.message : detail || payload?.message || res.statusText;
        throw new Error(msg);
      }
      // Refresh to pick up new calendar data
      await fetchAll();
    } catch (e: any) {
      dialog.alert('Calendar Refresh', e?.message || 'Could not refresh calendar');
    } finally {
      setCalendarRefreshing(false);
    }
  };

  // Show branded loading only on first load when nothing has arrived yet
  if (initialLoad && !overview && !stats && !calendarSummary) return (
    <BrandedLoading message="Loading Dashboard..." subtitle="Crunching the numbers." />
  );

  const health = overview?.health;
  const failedCount = health?.jobs_failed ?? 0;
  const schedulerActive = health?.scheduler_active;
  const pAge = overview?.pipeline_age;
  // Morning Refresh display status: derive from live job status, fallback to pipeline_age
  const mrDisplayStatus: string | undefined = isNewsRefreshRunning
    ? undefined
    : newsRunStatus === 'completed' || newsRunStatus === 'success'
      ? 'green'
      : newsRunStatus === 'error' || newsRunStatus === 'failed'
        ? 'red'
        : pAge?.morning_refresh_status;
  const pi = overview?.price_integrity;
  const cp = pi?.coverage_checkpoints || {};
  const eodhd = overview?.eodhd_api_usage;

  // ── Bulk Completeness (since last full backfill) ──
  const bc = overview?.bulk_completeness;
  const bcHasBaseline = bc?.has_baseline === true;
  const bcMissing = bc?.missing_count ?? 0;
  const bcGapFree = bc?.gap_free_since_baseline === true;

  // Build alerts
  const failedJobs: { name?: string; error_summary?: string }[] = overview?.jobs?.failed ?? [];
  const alerts: { color: string; icon: string; text: string }[] = [];
  if (failedCount > 0) {
    // Show each failed job name + error reason individually
    if (failedJobs.length > 0) {
      for (const fj of failedJobs) {
        const name = fj.name ?? 'unknown';
        const err = fj.error_summary ?? 'Unknown error';
        alerts.push({ color: '#EF4444', icon: 'close-circle', text: `${name} failed: ${err}` });
      }
    } else {
      alerts.push({ color: '#EF4444', icon: 'close-circle', text: `${failedCount} pipeline job${failedCount > 1 ? 's' : ''} failed` });
    }
  }
  if (schedulerActive === false) alerts.push({ color: '#EF4444', icon: 'pause-circle', text: 'Scheduler is paused' });
  if ((pi?.today_visible ?? 0) === 0) alerts.push({ color: '#EF4444', icon: 'eye-off', text: '0 visible tickers — universe not seeded' });
  const ctdh = pi?.completed_trading_days_health;
  const ctdhStaleMsg = ctdh?.message || 'Market calendar missing recent rows';
  if (ctdh?.calendar_stale) alerts.push({ color: '#F59E0B', icon: 'calendar-outline', text: ctdhStaleMsg });
  if (ctdh && (ctdh.missing_count ?? 0) > 0) alerts.push({ color: '#F59E0B', icon: 'alert-circle', text: `${ctdh.missing_count} of last 10 completed trading ${ctdh.missing_count === 1 ? 'day' : 'days'} missing price data` });
  if (pi && (pi.needs_price_redownload ?? 0) > 0) alerts.push({ color: '#F59E0B', icon: 'refresh-circle', text: `${pi.needs_price_redownload} ticker(s) need price re-download` });
  if (bcHasBaseline && !bcGapFree) alerts.push({ color: '#EF4444', icon: 'alert-circle', text: `${bcMissing} bulk day${bcMissing === 1 ? '' : 's'} missing since last full backfill` });
  if (!bcHasBaseline && bc) alerts.push({ color: '#F59E0B', icon: 'information-circle', text: 'No full backfill baseline — run a successful full backfill' });

  // Format count/total with percentage
  const fmtRatio = (count: number, total: number) => {
    const pct = total > 0 ? Math.round((count / total) * 100) : 0;
    return total > 0 ? `${count}/${total} (${pct}%)` : `${count}/${total}`;
  };

  // Process-truth metrics for Price Integrity section
  const tvTotal = pi?.today_visible ?? 0;
  const hdcCount = pi?.history_download_completed_count ?? 0;
  const gfCount = pi?.gap_free_since_history_download_count ?? 0;
  const fundCount = pi?.fundamentals_complete_count ?? 0;
  const hdcValue = fmtRatio(hdcCount, tvTotal);
  const gfValue = fmtRatio(gfCount, tvTotal);
  const fundValue = pi ? fmtRatio(fundCount, tvTotal) : '—';

  // ── Tristate status logic for Price Integrity cards ──
  // GREEN = confirmed OK, YELLOW = unknown/pending, RED = confirmed problem
  const lastBulkStatus: 'green' | 'yellow' = pi?.last_bulk_trading_date ? 'green' : 'yellow';

  // ── Completed Trading Days Health metric ──
  const ctdhData = pi?.completed_trading_days_health;
  const ctdhStale = ctdhData?.calendar_stale === true;
  const ctdhMissing = ctdhData?.missing_count ?? 0;
  const ctdhDisplay = ctdhData
    ? (ctdhStale ? 'Calendar stale' : `${ctdhMissing} missing`)
    : '—';
  const ctdhStatus: 'green' | 'yellow' | 'red' = (ctdhData?.status as 'green' | 'yellow' | 'red') ?? 'yellow';

  const needRedl = pi?.needs_price_redownload;
  const needRedlDisplay = needRedl != null ? String(needRedl) : '—';
  const needRedlStatus: 'green' | 'yellow' | 'red' =
    needRedl != null ? (needRedl === 0 ? 'green' : 'red') : 'yellow';

  const hdcStatus: 'green' | 'yellow' =
    pi && tvTotal > 0 && hdcCount === tvTotal ? 'green' : 'yellow';
  const gfStatus: 'green' | 'yellow' =
    pi && tvTotal > 0 && gfCount === tvTotal ? 'green' : 'yellow';
  const fundStatus: 'green' | 'yellow' =
    pi && tvTotal > 0 && fundCount === tvTotal ? 'green' : 'yellow';

  // EODHD API usage from provider endpoint
  const eodhCallsToday = eodhd?.eodhd_api_calls_today;
  const eodhLimit = eodhd?.eodhd_daily_limit ?? 100000;
  const eodhDisplay = eodhCallsToday != null ? `${eodhCallsToday} / ${eodhLimit}` : '—';

  // ── Bulk Completeness derived display values ──
  // Guard: baseline-dependent derivations must not run when has_baseline !== true
  const bcBaseline = bcHasBaseline ? bc?.baseline ?? null : null;
  const bcStatusColor: 'green' | 'yellow' | 'red' =
    !bcHasBaseline ? 'yellow' : bcGapFree ? 'green' : 'red';
  const bcStatusLabel = !bcHasBaseline ? 'NO BASELINE' : bcGapFree ? 'GAP-FREE' : 'GAPS PRESENT';
  const bcMissingDates = bcHasBaseline ? (bc?.missing_bulk_dates_since_baseline ?? []) : [];

  // ── Visible Coverage ──
  const vc = overview?.visible_coverage;
  const vcTotal = vc?.visible_total ?? 0;
  const vcPriceCount = vc?.price_coverage_count ?? 0;
  const vcPricePct = vc?.price_coverage_pct ?? 0;
  const vcFundCount = vc?.fundamentals_complete_count ?? 0;
  const vcFundPct = vc?.fundamentals_complete_pct ?? 0;

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
        {stats ? (
          <View style={d.bizRow}>
            <BizStat label="Users" value={String(stats.users ?? 0)} icon="people" />
            <BizStat label="Portfolios" value={String(stats.portfolios ?? 0)} icon="briefcase" />
            <BizStat label="Positions" value={String(stats.positions ?? 0)} icon="layers" />
          </View>
        ) : (
          <View style={d.bizRow}>
            {[0, 1, 2].map(i => (
              <View key={i} style={d.bizStat}>
                <View style={[sk.circle, { width: 16, height: 16 }]} />
                <View style={[sk.bar, { width: 32, height: 18 }]} />
                <View style={[sk.bar, { width: 48, height: 10 }]} />
              </View>
            ))}
          </View>
        )}
      </View>

      {/* B) Ops Health (compact) */}
      <View style={d.card}>
        <Text style={d.sectionTitle}>Ops Health</Text>
        {overview ? (
        <View style={d.opsGrid}>
          <OpsItem
            label="Pipeline (1–3)"
            value={formatHours(pAge?.pipeline_hours_since_success)}
            status={pAge?.pipeline_status}
          />
          {/* Morning Refresh — enhanced tile with Run Now + running state */}
          {(() => {
            const hasError = mrDisplayStatus === 'red' && !!newsRun?.error_message;
            return (
            <View style={hasError ? d.opsItemCol : d.opsItem}>
              <View style={{ flexDirection: 'row', alignItems: 'center', gap: 6, width: '100%' }}>
                {isNewsRefreshRunning ? (
                  <ActivityIndicator size={14} color="#F59E0B" />
                ) : (
                  <Ionicons name={statusIcon(mrDisplayStatus) as any} size={14} color={statusColor(mrDisplayStatus)} />
                )}
                <Text style={d.opsLabel}>Morning Refresh</Text>
                {isNewsRefreshRunning ? (
                  <Text style={[d.opsValue, { color: '#F59E0B' }]}>
                    {(newsRun as any)?.details?.news_refresh_telemetry?.message || 'Running…'}
                  </Text>
                ) : !newsRun && pAge?.morning_refresh_hours_since_success == null ? (
                  <Text style={[d.opsValue, { color: COLORS.textMuted }]}>Never run</Text>
                ) : (
                  <Text style={[d.opsValue, { color: statusColor(mrDisplayStatus) }]}>
                    {formatHours(pAge?.morning_refresh_hours_since_success)}
                  </Text>
                )}
                {!isNewsRefreshRunning && (
                  <TouchableOpacity
                    style={d.opsRunBtn}
                    onPress={handleRunNewsRefresh}
                  >
                    <Text style={d.opsRunBtnText}>Run</Text>
                  </TouchableOpacity>
                )}
              </View>
              {hasError && (
                <Text style={d.opsErrorDetail} numberOfLines={2}>⚠️ {newsRun.error_message}</Text>
              )}
            </View>
            );
          })()}
          <OpsItem
            label="Scheduler"
            value={schedulerActive ? 'Running' : 'Paused'}
            status={schedulerActive ? 'green' : 'red'}
          />
          <OpsItem
            label="Failed Jobs"
            value={failedCount > 0 && failedJobs.length > 0
              ? failedJobs.map(fj => fj.name ?? '?').join(', ')
              : String(failedCount)}
            status={failedCount > 0 ? 'red' : 'green'}
          />
          <OpsItem
            label="EODHD API Today"
            value={eodhDisplay}
            status={eodhCallsToday != null ? 'green' : undefined}
          />
        </View>
        ) : (
          <View style={d.opsGrid}>
            {[0, 1, 2, 3].map(i => (
              <View key={i} style={[d.opsItem, { minHeight: 36 }]}>
                <View style={[sk.circle, { width: 14, height: 14 }]} />
                <View style={[sk.bar, { flex: 1, height: 11 }]} />
              </View>
            ))}
          </View>
        )}
      </View>

      {/* US Market Calendar Widget */}
      <View style={d.card}>
        <View style={{ flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between' }}>
          <Text style={d.sectionTitle}>US Market Calendar</Text>
          <TouchableOpacity
            style={[d.opsRunBtn, calendarRefreshing && { opacity: 0.5 }]}
            onPress={handleRefreshCalendar}
            disabled={calendarRefreshing}
          >
            {calendarRefreshing ? (
              <ActivityIndicator size={10} color={COLORS.primary} />
            ) : (
              <Text style={d.opsRunBtnText}>Refresh</Text>
            )}
          </TouchableOpacity>
        </View>
        {calendarSummary ? (
          <View style={d.opsGrid}>
            <View style={d.opsItem}>
              <Ionicons
                name={calendarSummary.today_is_trading_day ? 'checkmark-circle' : 'close-circle'}
                size={14}
                color={calendarSummary.today_is_trading_day ? '#22C55E' : '#F59E0B'}
              />
              <Text style={d.opsLabel}>Today</Text>
              <Text style={[d.opsValue, { color: calendarSummary.today_is_trading_day ? '#22C55E' : '#F59E0B' }]}>
                {calendarSummary.today_is_trading_day
                  ? 'Trading day'
                  : calendarSummary.today_holiday_name || 'Holiday / Weekend'}
              </Text>
            </View>
            <View style={d.opsItem}>
              <Ionicons name="calendar-outline" size={14} color={COLORS.textMuted} />
              <Text style={d.opsLabel}>Last Closing Day</Text>
              <Text style={[d.opsValue, { color: COLORS.text }]}>
                {calendarSummary.last_closing_day ?? '—'}
              </Text>
            </View>
            <View style={d.opsItem}>
              <Ionicons name="arrow-forward-circle-outline" size={14} color={COLORS.textMuted} />
              <Text style={d.opsLabel}>Next Trading Day</Text>
              <Text style={[d.opsValue, { color: COLORS.text }]}>
                {calendarSummary.next_trading_day ?? '—'}
              </Text>
            </View>
            <View style={d.opsItem}>
              <Ionicons
                name={calendarSummary.calendar_fresh ? 'checkmark-circle' : 'warning'}
                size={14}
                color={calendarSummary.calendar_fresh ? '#22C55E' : '#EF4444'}
              />
              <Text style={d.opsLabel}>Calendar</Text>
              <Text style={[d.opsValue, { color: calendarSummary.calendar_fresh ? '#22C55E' : '#EF4444' }]}>
                {calendarSummary.calendar_fresh ? 'Fresh' : 'Stale'}
              </Text>
            </View>
          </View>
        ) : (
          <Text style={d.cpHint}>Loading…</Text>
        )}
      </View>

      {/* C) Price Integrity / Coverage */}
      <View style={d.cardCompact}>
        <Text style={d.sectionTitleSm}>Price Integrity / Coverage</Text>

        {/* Key metrics row – tighter grid */}
        <View style={d.integrityGridCompact}>
          <IntegrityMetric
            label="Last Bulk Date"
            value={pi?.last_bulk_trading_date ?? '—'}
            status={lastBulkStatus}
          />
          <IntegrityMetric
            label="Last 10 Trading Days"
            value={ctdhDisplay}
            status={ctdhStatus}
          />
          <IntegrityMetric
            label="Need Re-download"
            value={needRedlDisplay}
            status={needRedlStatus}
          />
          <IntegrityMetric
            label="Fundamentals"
            value={fundValue}
            status={fundStatus}
          />
          <IntegrityMetric
            label="SP500TR"
            value={pi?.benchmark_freshness?.label ?? '—'}
            status={pi?.benchmark_freshness?.status as 'green' | 'yellow' | 'red' | undefined}
          />
        </View>

        {/* Collapsed: Last 10 Completed Trading Days – single row + toggle */}
        {ctdhData?.days && ctdhData.days.length > 0 && (
          <View style={{ marginBottom: 4 }}>
            <TouchableOpacity
              style={d.compactToggleRow}
              onPress={() => setShowTradingDays(v => !v)}
              activeOpacity={0.7}
            >
              <Ionicons name={statusIcon(ctdhStatus) as any} size={12} color={statusColor(ctdhStatus)} />
              <Text style={d.compactToggleText}>
                Last 10 completed days: {ctdhMissing} missing
                {ctdhData.days.length > 0 ? ` · last=${ctdhData.days[0].date}` : ''}
              </Text>
              <Text style={d.compactToggleBtn}>{showTradingDays ? 'Hide' : 'Show'}</Text>
            </TouchableOpacity>
            {showTradingDays && (
              <View style={{ marginTop: 4 }}>
                {ctdhStale && (
                  <Text style={[d.cpHint, { color: '#F59E0B', marginBottom: 2 }]}>⚠ {ctdhStaleMsg}</Text>
                )}
                {ctdhData.missing_dates && ctdhData.missing_dates.length > 0 && (
                  <Text style={[d.cpHint, { color: '#EF4444', marginBottom: 2 }]}>
                    Missing: {ctdhData.missing_dates.join(', ')}
                  </Text>
                )}
                {ctdhData.days.map((day) => (
                  <View key={day.date} style={d.cpRow}>
                    <View style={[d.cpDot, { backgroundColor: day.ok ? '#22C55E' : '#EF4444' }]} />
                    <Text style={d.cpLabel}>{day.date}</Text>
                    <Text style={[d.cpValue, { color: day.ok ? '#22C55E' : '#EF4444' }]}>
                      {day.ok ? '✓ OK' : '✗ Missing'}
                    </Text>
                  </View>
                ))}
              </View>
            )}
          </View>
        )}

        {/* Recent bulk coverage – single compact line */}
        {(() => {
          const cpLatest = cp['latest_trading_day'];
          const cpWeek = cp['1_week_ago'];
          const fmtCp = (c?: CoverageCheckpoint) => {
            if (!c) return null;
            const have = c.have_price_count ?? 0;
            const total = c.today_visible ?? 0;
            const pct = total > 0 ? Math.round((have / total) * 100) : 0;
            return `${have}/${total} (${pct}%)`;
          };
          const latestStr = fmtCp(cpLatest);
          const weekStr = fmtCp(cpWeek);
          if (!latestStr && !weekStr) return null;
          return (
            <View style={d.compactToggleRow}>
              <Ionicons name="analytics-outline" size={12} color={COLORS.textMuted} />
              <Text style={d.compactToggleText} numberOfLines={1}>
                Bulk coverage: latest={latestStr ?? '—'}
                {weekStr ? ` · 1w ago=${weekStr}` : ''}
              </Text>
            </View>
          );
        })()}

        {/* Historical / Price Integrity truth */}
        <Text style={[d.subSection, { marginTop: 6 }]}>Price Completeness (process truth)</Text>
        <Text style={d.cpHint}>
          Per-ticker proof: full history downloaded + no missing daily bulk dates since that download
        </Text>
        <IntegrityMetric
          label="Complete Prices (strict proof)"
          value={hdcValue}
          status={hdcStatus}
        />
        <IntegrityMetric
          label="Gap-Free (no missing bulk days)"
          value={gfValue}
          status={gfStatus}
        />
        {((pi?.non_gap_free_sample ?? []).length > 0 || (pi?.gap_excluded_sample ?? []).length > 0) && (
          <View style={{ marginTop: 6 }}>
            {(pi?.non_gap_free_sample ?? []).length > 0 && (
              <>
                <Text style={[d.subSection, { marginTop: 2 }]}>Why not gap-free?</Text>
                <Text style={d.cpHint}>True gaps: bulk found, close {'>'} 0, DB row still missing</Text>
                {(pi?.non_gap_free_sample ?? []).map((item) => (
                  <View key={item.ticker} style={d.cpRow}>
                    <Text style={[d.cpLabel, { width: 90 }]}>{item.ticker}</Text>
                    <Text style={[d.cpDate, { flex: 1, color: '#EF4444' }]}>{item.missing_dates.join(', ')}</Text>
                  </View>
                ))}
              </>
            )}
            {(pi?.gap_excluded_sample ?? []).length > 0 && (
              <>
                <Text style={[d.subSection, { marginTop: 6 }]}>Not applicable (not a gap)</Text>
                <Text style={d.cpHint}>Ticker absent from bulk or close=0 — halted/delisted/no trade</Text>
                {(pi?.gap_excluded_sample ?? []).map((item) => (
                  <View key={item.ticker} style={d.cpRow}>
                    <Text style={[d.cpLabel, { width: 90 }]}>{item.ticker}</Text>
                    <Text style={[d.cpDate, { flex: 1, color: '#9CA3AF' }]}>
                      {item.excluded_dates.map(ed => `${ed.date} (${ed.reason.replace(/_/g, ' ')})`).join(', ')}
                    </Text>
                  </View>
                ))}
              </>
            )}
            {(pi?.top_missing_dates ?? []).length > 0 && (
              <>
                <Text style={[d.subSection, { marginTop: 6 }]}>Top missing dates</Text>
                <Text style={d.cpHint}>Dates most commonly absent across all non-gap-free tickers (desc)</Text>
                {(pi?.top_missing_dates ?? []).map((item) => (
                  <View key={item.date} style={d.cpRow}>
                    <Text style={[d.cpLabel, { width: 90 }]}>{item.date}</Text>
                    <Text style={[d.cpDate, { flex: 1 }]}>
                      {item.missing_ticker_count} ticker{item.missing_ticker_count === 1 ? '' : 's'}
                    </Text>
                  </View>
                ))}
              </>
            )}
          </View>
        )}
      </View>

      {/* D) Daily Bulk Ingestion – default collapsed */}
      <View style={d.cardCompact}>
        <TouchableOpacity
          style={d.compactToggleRow}
          onPress={() => setShowBulkDetails(v => !v)}
          activeOpacity={0.7}
        >
          <Text style={d.sectionTitleSm}>Daily Bulk Ingestion</Text>
          {bcHasBaseline && (
            <View style={{ flexDirection: 'row', alignItems: 'center', gap: 6, flex: 1, marginLeft: 8 }}>
              <Ionicons name={statusIcon(bcStatusColor) as any} size={12} color={statusColor(bcStatusColor)} />
              <Text style={[d.compactToggleText, { flex: 0 }]}>{bcStatusLabel}</Text>
              <Text style={d.compactToggleText}>
                {bc?.ingested_days_count ?? '?'}/{bc?.expected_days_count ?? '?'}
              </Text>
              {bcMissing > 0 && (
                <Text style={[d.compactToggleText, { color: '#EF4444' }]}>
                  {bcMissing} missing
                </Text>
              )}
            </View>
          )}
          {!bcHasBaseline && (
            <Text style={[d.compactToggleText, { color: '#F59E0B', flex: 1, marginLeft: 8 }]}>No baseline</Text>
          )}
          <Text style={d.compactToggleBtn}>{showBulkDetails ? 'Hide' : 'Show'}</Text>
        </TouchableOpacity>

        {showBulkDetails && (
          <>
            <Text style={[d.cpHint, { marginTop: 6 }]}>
              After the full backfill downloads all individual prices, EODHD daily bulk reports fill in each new trading day.
            </Text>
            {!bcHasBaseline ? (
              <>
                <Text style={[d.cpHint, { color: '#F59E0B', fontStyle: 'normal', fontSize: 11 }]}>
                  ⚠ No baseline yet
                </Text>
                <Text style={d.cpHint}>
                  A successful full backfill must be run first to establish a baseline.
                </Text>
              </>
            ) : (
              <>
                <View style={d.cpRow}>
                  <Text style={d.cpLabel}>Backfill Finished</Text>
                  <Text style={d.cpDate}>
                    {bcBaseline?.completed_at_prague
                      ? bcBaseline.completed_at_prague.replace('T', ' ').slice(0, 19)
                      : '—'}
                  </Text>
                </View>
                <View style={d.cpRow}>
                  <Text style={d.cpLabel}>All Prices Downloaded Through</Text>
                  <Text style={d.cpDate}>{bcBaseline?.through_date ?? '—'}</Text>
                </View>
                <View style={d.cpRow}>
                  <Text style={d.cpLabel}>Latest Daily Bulk Date</Text>
                  <Text style={d.cpDate}>{bc?.latest_bulk_date_ingested ?? '—'}</Text>
                </View>
                {(bc?.ingested_dates ?? []).length > 0 && (
                  <>
                    <Text style={[d.subSection, { marginTop: 6 }]}>Ingested Bulk Dates</Text>
                    {(bc?.ingested_dates ?? []).map((dt) => (
                      <View key={dt} style={d.cpRow}>
                        <View style={[d.cpDot, { backgroundColor: '#22C55E' }]} />
                        <Text style={d.cpLabel}>{dt}</Text>
                        <Text style={[d.cpValue, { color: '#22C55E' }]}>✓</Text>
                      </View>
                    ))}
                  </>
                )}
                {bcBaseline?.job_run_id && (
                  <View style={d.cpRow}>
                    <Text style={d.cpLabel}>Job Run ID</Text>
                    <Text style={[d.cpDate, { fontSize: 9 }]}>{bcBaseline.job_run_id}</Text>
                  </View>
                )}
                {bcMissingDates.length > 0 && (
                  <Text style={[d.cpHint, { color: '#EF4444', marginTop: 6 }]}>
                    Missing dates: {bcMissingDates.join(', ')}
                  </Text>
                )}
              </>
            )}
          </>
        )}
      </View>

      {/* E) Coverage (visible tickers) */}
      <View style={d.card}>
        <Text style={d.sectionTitle}>Coverage (visible tickers)</Text>
        <Text style={d.cpHint}>Current visible tickers coverage on latest bulk day + fundamentals completeness</Text>
        <View style={d.integrityGrid}>
          <IntegrityMetric
            label="Visible Tickers"
            value={String(vcTotal)}
            status={vcTotal > 0 ? 'green' : 'yellow'}
          />
          <IntegrityMetric
            label={`Price Coverage${vc?.latest_bulk_date ? ` (${vc.latest_bulk_date})` : ''}`}
            value={vcTotal > 0 ? `${vcPriceCount}/${vcTotal} (${vcPricePct}%)` : '—'}
            status={vcTotal > 0 && vcPricePct === 100 ? 'green' : vcTotal > 0 ? 'yellow' : undefined}
          />
          <IntegrityMetric
            label="Fundamentals Complete"
            value={vcTotal > 0 ? `${vcFundCount}/${vcTotal} (${vcFundPct}%)` : '—'}
            status={vcTotal > 0 && vcFundPct === 100 ? 'green' : vcTotal > 0 ? 'yellow' : undefined}
          />
        </View>
      </View>

      {/* Key Metrics Proof */}
      <KeyMetricsProofCard sessionToken={sessionToken} />

      {/* Benchmark Medians */}
      <BenchmarkMediansCard sessionToken={sessionToken} />

      <View style={{ height: 40 }} />
    </ScrollView>
  );
}

// ─── Key Metrics Proof Card ──────────────────────────────────────────────────

interface ProofMetric {
  value?: number | null;
  formatted?: string | null;
  na_reason?: string | null;
  formula?: string;
  source?: string;
  [key: string]: any;
}

function KeyMetricsProofCard({ sessionToken }: { sessionToken: string | null }) {
  const dialog = useAppDialog();
  const [ticker, setTicker] = useState('');
  const [loading, setLoading] = useState(false);
  const [proof, setProof] = useState<Record<string, any> | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});

  const headers: Record<string, string> = sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {};

  const handleRun = async () => {
    const t = ticker.trim();
    if (!t) { dialog.alert('Key Metrics Proof', 'Enter a ticker symbol'); return; }
    setLoading(true);
    setError(null);
    setProof(null);
    setExpanded({});
    try {
      const res = await fetch(
        `${API_URL}/api/admin/key-metrics-proof?ticker=${encodeURIComponent(t)}`,
        { headers },
      );
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body?.detail || body?.message || `HTTP ${res.status}`);
      }
      setProof(await res.json());
    } catch (e: any) {
      setError(e?.message || 'Request failed');
    } finally {
      setLoading(false);
    }
  };

  const metricKeys = [
    'pe_ttm', 'net_margin_ttm', 'fcf_yield',
    'roe', 'net_debt_ebitda', 'revenue_growth_3y', 'dividend_yield_ttm',
  ];
  const metricLabels: Record<string, string> = {
    pe_ttm: 'P/E (TTM)',
    net_margin_ttm: 'Net Margin (TTM)',
    fcf_yield: 'Free Cash Flow Yield',
    roe: 'ROE',
    net_debt_ebitda: 'Net Debt / EBITDA',
    revenue_growth_3y: 'Revenue Growth (3Y CAGR)',
    dividend_yield_ttm: 'Dividend Yield (TTM)',
  };

  const toggleExpand = (key: string) => {
    setExpanded(prev => ({ ...prev, [key]: !prev[key] }));
  };

  // Render detail rows for a metric's raw inputs
  const renderDetails = (key: string, m: ProofMetric) => {
    const skip = new Set(['value', 'formatted', 'na_reason', 'formula', 'source']);
    const entries = Object.entries(m).filter(([k]) => !skip.has(k));
    if (entries.length === 0) return null;
    return (
      <View style={kmp.detailBox}>
        {m.formula && (
          <Text style={kmp.detailRow}>
            <Text style={kmp.detailKey}>Formula: </Text>
            <Text style={kmp.detailVal}>{m.formula}</Text>
          </Text>
        )}
        {m.source && (
          <Text style={kmp.detailRow}>
            <Text style={kmp.detailKey}>Source: </Text>
            <Text style={kmp.detailVal}>{m.source}</Text>
          </Text>
        )}
        {entries.map(([k, v]) => (
          <Text key={k} style={kmp.detailRow} numberOfLines={3}>
            <Text style={kmp.detailKey}>{k}: </Text>
            <Text style={kmp.detailVal}>
              {v == null ? 'null' : typeof v === 'object' ? JSON.stringify(v, null, 0) : String(v)}
            </Text>
          </Text>
        ))}
      </View>
    );
  };

  return (
    <View style={d.card}>
      <Text style={d.sectionTitle}>Key Metrics Proof</Text>
      <Text style={d.cpHint}>
        Audit tool: enter a ticker to see every raw input, formula, and result for the 7 Key Metrics.
      </Text>

      {/* Input row */}
      <View style={kmp.inputRow}>
        <TextInput
          style={kmp.input}
          placeholder="e.g. AAPL.US"
          placeholderTextColor={COLORS.textMuted}
          value={ticker}
          onChangeText={setTicker}
          autoCapitalize="characters"
          returnKeyType="go"
          onSubmitEditing={handleRun}
        />
        <TouchableOpacity
          style={[kmp.runBtn, loading && { opacity: 0.5 }]}
          onPress={handleRun}
          disabled={loading}
        >
          {loading ? (
            <ActivityIndicator size={12} color="#fff" />
          ) : (
            <Text style={kmp.runBtnText}>Run Proof</Text>
          )}
        </TouchableOpacity>
      </View>

      {/* Error */}
      {error && (
        <View style={kmp.errorBox}>
          <Ionicons name="close-circle" size={13} color="#EF4444" />
          <Text style={kmp.errorText}>{error}</Text>
        </View>
      )}

      {/* Results */}
      {proof && (
        <View style={kmp.results}>
          {/* Meta info */}
          {proof.ticker && (
            <View style={kmp.metaRow}>
              <Text style={kmp.metaText}>Ticker: {proof.ticker}</Text>
              {proof.price_date && <Text style={kmp.metaText}>Price date: {proof.price_date}</Text>}
            </View>
          )}
          {proof.current_price != null && (
            <View style={kmp.metaRow}>
              <Text style={kmp.metaText}>Price: ${proof.current_price}</Text>
              {proof.shares_outstanding_formatted && (
                <Text style={kmp.metaText}>Shares: {proof.shares_outstanding_formatted}</Text>
              )}
            </View>
          )}

          {/* Metric cards */}
          {metricKeys.map(key => {
            const m = proof[key] as ProofMetric | undefined;
            if (!m) return null;
            const hasValue = m.value != null;
            const isExpanded = expanded[key] === true;
            return (
              <View key={key} style={kmp.metricCard}>
                <TouchableOpacity style={kmp.metricHeader} onPress={() => toggleExpand(key)} activeOpacity={0.7}>
                  <View style={kmp.metricLeft}>
                    <Ionicons
                      name={hasValue ? 'checkmark-circle' : 'alert-circle'}
                      size={14}
                      color={hasValue ? '#22C55E' : '#F59E0B'}
                    />
                    <Text style={kmp.metricName}>{metricLabels[key] || key}</Text>
                  </View>
                  <View style={kmp.metricRight}>
                    <Text style={[kmp.metricValue, !hasValue && { color: COLORS.textMuted }]}>
                      {m.formatted ?? (m.na_reason ? `N/A (${m.na_reason.replace(/_/g, ' ')})` : 'N/A')}
                    </Text>
                    <Ionicons name={isExpanded ? 'chevron-up' : 'chevron-down'} size={12} color={COLORS.textMuted} />
                  </View>
                </TouchableOpacity>
                {isExpanded && renderDetails(key, m)}
              </View>
            );
          })}
        </View>
      )}
    </View>
  );
}

const kmp = StyleSheet.create({
  inputRow: { flexDirection: 'row', gap: 8, marginBottom: 10 },
  input: {
    flex: 1, fontSize: 13, color: COLORS.text, backgroundColor: COLORS.background,
    borderRadius: 8, borderWidth: 1, borderColor: COLORS.border, paddingHorizontal: 12, paddingVertical: 8,
  },
  runBtn: {
    backgroundColor: COLORS.primary, borderRadius: 8, paddingHorizontal: 14, paddingVertical: 8,
    alignItems: 'center', justifyContent: 'center',
  },
  runBtnText: { color: '#fff', fontSize: 12, fontWeight: '700' },

  errorBox: { flexDirection: 'row', alignItems: 'center', gap: 6, backgroundColor: '#EF444414', borderRadius: 6, padding: 8, marginBottom: 8, borderWidth: 1, borderColor: '#EF444433' },
  errorText: { fontSize: 11, color: '#EF4444', flex: 1 },

  results: { marginTop: 4 },
  metaRow: { flexDirection: 'row', justifyContent: 'space-between', marginBottom: 4 },
  metaText: { fontSize: 10, color: COLORS.textMuted },

  metricCard: { backgroundColor: COLORS.background, borderRadius: 8, borderWidth: 1, borderColor: COLORS.border, marginBottom: 6, overflow: 'hidden' },
  metricHeader: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', padding: 10 },
  metricLeft: { flexDirection: 'row', alignItems: 'center', gap: 6, flex: 1 },
  metricRight: { flexDirection: 'row', alignItems: 'center', gap: 6 },
  metricName: { fontSize: 12, fontWeight: '600', color: COLORS.text },
  metricValue: { fontSize: 12, fontWeight: '700', color: COLORS.text },

  detailBox: { paddingHorizontal: 10, paddingBottom: 10, borderTopWidth: 1, borderTopColor: COLORS.border + '55' },
  detailRow: { fontSize: 10, color: COLORS.text, marginTop: 4 },
  detailKey: { fontWeight: '600', color: COLORS.textMuted },
  detailVal: { color: COLORS.text },
});

// ─── Benchmark Medians Card ──────────────────────────────────────────────────

type BmLevel = 'industry' | 'sector' | 'market';

interface BmMetric {
  name: string;
  median: number | null;
  n_used?: number | null;
}

interface BmData {
  level: string;
  key: string;
  ticker_count: number;
  updated_at_prague: string | null;
  warning: string | null;
  metrics: Record<string, BmMetric>;
}

function BenchmarkMediansCard({ sessionToken }: { sessionToken: string | null }) {
  const [level, setLevel] = useState<BmLevel>('industry');
  const [groups, setGroups] = useState<string[]>([]);
  const [search, setSearch] = useState('');
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [data, setData] = useState<BmData | null>(null);
  const [loading, setLoading] = useState(false);
  const [groupsLoading, setGroupsLoading] = useState(false);
  const [dropdownOpen, setDropdownOpen] = useState(false);

  // Pool ticker list modal
  const [poolModalVisible, setPoolModalVisible] = useState(false);
  const [poolModalLoading, setPoolModalLoading] = useState(false);
  const [poolModalData, setPoolModalData] = useState<{
    level: string; group?: string; metric: string;
    median?: number; n_used?: number;
    n_unique_tickers?: number; n_records?: number;
    duplicates?: Record<string, number>;
    filters_applied?: Record<string, any>;
    tickers: { ticker: string; value: number | null }[];
    note?: string;
  } | null>(null);

  const headers: Record<string, string> = sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {};

  const openPoolModal = async (bmLevel: string, metric: string, group?: string) => {
    setPoolModalVisible(true);
    setPoolModalLoading(true);
    setPoolModalData(null);
    try {
      const params = new URLSearchParams({ level: bmLevel, metric });
      if (group) params.set('group', group);
      const res = await fetch(
        `${API_URL}/api/admin/peer-pool-tickers?${params}`,
        { headers },
      );
      if (res.ok) {
        setPoolModalData(await res.json());
      } else {
        const err = await res.json().catch(() => ({}));
        setPoolModalData({
          level: bmLevel, group, metric, tickers: [],
          note: err?.detail || 'Failed to load ticker list',
        });
      }
    } catch (e: any) {
      setPoolModalData({
        level: bmLevel, group, metric, tickers: [],
        note: e?.message || 'Network error',
      });
    } finally {
      setPoolModalLoading(false);
    }
  };

  // Fetch groups when level changes
  useEffect(() => {
    setGroups([]);
    setSelectedKey(null);
    setData(null);
    setSearch('');
    setDropdownOpen(false);
    (async () => {
      setGroupsLoading(true);
      try {
        const res = await fetch(`${API_URL}/api/admin/peer-medians/groups?level=${level}`, { headers });
        if (res.ok) {
          const j = await res.json();
          setGroups(j.groups || []);
          // Auto-select first for market (only one group)
          if (level === 'market' && j.groups?.length > 0) setSelectedKey(j.groups[0]);
        }
      } catch { /* non-fatal */ }
      setGroupsLoading(false);
    })();
  }, [level, sessionToken]);

  // Fetch medians when key selected
  useEffect(() => {
    if (!selectedKey) { setData(null); return; }
    let cancelled = false;
    (async () => {
      setLoading(true);
      try {
        const res = await fetch(
          `${API_URL}/api/admin/peer-medians?level=${level}&key=${encodeURIComponent(selectedKey)}`,
          { headers },
        );
        if (res.ok && !cancelled) setData(await res.json());
        else if (!cancelled) setData(null);
      } catch { if (!cancelled) setData(null); }
      if (!cancelled) setLoading(false);
    })();
    return () => { cancelled = true; };
  }, [level, selectedKey, sessionToken]);

  const filtered = search
    ? groups.filter(g => g.toLowerCase().includes(search.toLowerCase()))
    : groups;

  // Display order: the 7 required Key Metrics only
  const allKeys = ['net_margin_ttm', 'fcf_yield', 'net_debt_ebitda', 'revenue_growth_3y', 'dividend_yield_ttm', 'pe_ttm', 'roe'];

  // Compute max absolute median for bar scaling within this tab
  const allMedians = data ? allKeys.map(k => Math.abs(data.metrics[k]?.median ?? 0)).filter(v => v > 0) : [];
  const maxMedian = allMedians.length > 0 ? Math.max(...allMedians) : 1;

  const formatVal = (m: BmMetric): string => {
    if (m.median == null) return 'N/A';
    const v = m.median;
    // Percentage metrics
    if (['Net Margin (TTM)', 'Free Cash Flow Yield', 'Revenue Growth (3Y CAGR)', 'Dividend Yield (TTM)', 'ROE'].includes(m.name))
      return `${v.toFixed(2)} %`;
    // Ratio metrics
    return v.toFixed(2);
  };

  const tabs: { id: BmLevel; label: string }[] = [
    { id: 'industry', label: 'Industry' },
    { id: 'sector', label: 'Sector' },
    { id: 'market', label: 'Market' },
  ];

  return (
    <View style={d.card}>
      <Text style={d.sectionTitle}>Benchmark Medians</Text>

      {/* Level tabs */}
      <View style={bm.tabRow}>
        {tabs.map(t => (
          <TouchableOpacity
            key={t.id}
            style={[bm.tab, level === t.id && bm.tabActive]}
            onPress={() => setLevel(t.id)}
          >
            <Text style={[bm.tabText, level === t.id && bm.tabTextActive]}>{t.label}</Text>
          </TouchableOpacity>
        ))}
      </View>

      {/* Searchable dropdown (not for market) */}
      {level !== 'market' && (
        <View style={bm.dropdownWrap}>
          <TouchableOpacity style={bm.dropdownBtn} onPress={() => setDropdownOpen(!dropdownOpen)}>
            <Text style={bm.dropdownBtnText} numberOfLines={1}>
              {selectedKey || (groupsLoading ? 'Loading…' : 'Select group…')}
            </Text>
            <Ionicons name={dropdownOpen ? 'chevron-up' : 'chevron-down'} size={14} color={COLORS.textMuted} />
          </TouchableOpacity>
          {dropdownOpen && (
            <View style={bm.dropdownList}>
              <View style={bm.searchRow}>
                <Ionicons name="search" size={13} color={COLORS.textMuted} />
                <TextInput
                  style={bm.searchInput}
                  placeholder="Search…"
                  placeholderTextColor={COLORS.textMuted}
                  value={search}
                  onChangeText={setSearch}
                  autoFocus
                />
              </View>
              <ScrollView style={bm.dropdownScroll} nestedScrollEnabled>
                {filtered.length === 0 && (
                  <Text style={bm.emptyText}>{groupsLoading ? 'Loading…' : 'No matches'}</Text>
                )}
                {filtered.map(g => (
                  <TouchableOpacity
                    key={g}
                    style={[bm.dropdownItem, selectedKey === g && bm.dropdownItemActive]}
                    onPress={() => { setSelectedKey(g); setDropdownOpen(false); setSearch(''); }}
                  >
                    <Text style={[bm.dropdownItemText, selectedKey === g && bm.dropdownItemTextActive]} numberOfLines={1}>{g}</Text>
                  </TouchableOpacity>
                ))}
              </ScrollView>
            </View>
          )}
        </View>
      )}

      {/* Content */}
      {loading && <ActivityIndicator size="small" color={COLORS.primary} style={{ marginVertical: 16 }} />}

      {!loading && data && (
        <View style={bm.content}>
          {/* Ticker count + updated */}
          <View style={bm.metaRow}>
            <Text style={bm.metaText}>{data.ticker_count} tickers</Text>
            {data.updated_at_prague && (
              <Text style={bm.metaText}>Updated: {data.updated_at_prague.slice(0, 16).replace('T', ' ')}</Text>
            )}
          </View>

          {/* Warning */}
          {data.warning && (
            <View style={bm.warningBox}>
              <Ionicons name="warning" size={13} color="#D97706" />
              <Text style={bm.warningText}>{data.warning}</Text>
            </View>
          )}

          {/* Metrics */}
          {allKeys.map(mk => {
            const m = data.metrics[mk];
            if (!m) return null;
            const isNA = m.median == null;
            const barPct = isNA ? 0 : Math.min(Math.abs(m.median!) / maxMedian * 100, 100);
            return (
              <View key={mk} style={bm.metricRow}>
                <View style={bm.metricHeader}>
                  <Text style={bm.metricName}>{m.name}</Text>
                  <Text style={[bm.metricValue, isNA && { color: COLORS.textMuted }]}>
                    {isNA ? 'N/A' : formatVal(m)}
                  </Text>
                </View>
                <View style={bm.barTrack}>
                  <View style={[bm.barFill, { width: `${barPct}%` }, isNA && { backgroundColor: COLORS.border }]} />
                </View>
                {m.n_used != null && (
                  <TouchableOpacity
                    onPress={() => openPoolModal(level, mk, level !== 'market' ? selectedKey ?? undefined : undefined)}
                    accessibilityLabel={`View ticker list for ${m.name}`}
                  >
                    <Text style={[bm.metricNUsed, bm.metricNUsedClickable]}>n={m.n_used}</Text>
                  </TouchableOpacity>
                )}
              </View>
            );
          })}
        </View>
      )}

      {!loading && !data && selectedKey && (
        <Text style={[bm.emptyText, { marginVertical: 12 }]}>No data found for this group.</Text>
      )}

      {/* Pool ticker list modal */}
      <Modal
        visible={poolModalVisible}
        transparent
        animationType="fade"
        onRequestClose={() => setPoolModalVisible(false)}
      >
        <View style={bm.modalOverlay}>
          <View style={bm.modalCard}>
            <View style={bm.modalHeader}>
              <Text style={bm.modalTitle}>
                {poolModalData
                  ? `${poolModalData.metric} — ${poolModalData.level}${poolModalData.group ? `: ${poolModalData.group}` : ''}`
                  : 'Loading…'}
              </Text>
              <TouchableOpacity onPress={() => setPoolModalVisible(false)}>
                <Ionicons name="close" size={20} color={COLORS.textLight} />
              </TouchableOpacity>
            </View>
            {poolModalLoading ? (
              <ActivityIndicator size="small" color={COLORS.primary} style={{ marginVertical: 20 }} />
            ) : poolModalData ? (
              <>
                {poolModalData.median != null && (
                  <Text style={bm.modalSub}>
                    Median: {poolModalData.median}  ·  n_used={poolModalData.n_used ?? poolModalData.tickers.length}
                  </Text>
                )}
                {(poolModalData.n_unique_tickers != null || poolModalData.n_records != null) && (
                  <Text style={bm.modalSub}>
                    Unique tickers: {poolModalData.n_unique_tickers ?? '—'}  ·  Records: {poolModalData.n_records ?? '—'}
                  </Text>
                )}
                {poolModalData.filters_applied && (
                  <Text style={[bm.modalSub, { color: COLORS.textMuted }]}>
                    Filters: currency={poolModalData.filters_applied.currency_filter ?? '?'}, values={poolModalData.filters_applied.value_filter ?? '?'}, visible=true, fundamentals=complete
                  </Text>
                )}
                {poolModalData.duplicates && Object.keys(poolModalData.duplicates).length > 0 && (
                  <Text style={[bm.modalSub, { color: '#F59E0B', fontWeight: '600' }]}>
                    Duplicates: {Object.entries(poolModalData.duplicates).map(([t, c]) => `${t}×${c}`).join(', ')}
                  </Text>
                )}
                {poolModalData.note ? (
                  <Text style={[bm.modalSub, { color: '#F59E0B' }]}>{poolModalData.note}</Text>
                ) : null}
                <ScrollView style={bm.modalScroll}>
                  {poolModalData.tickers.map((t, i) => (
                    <View key={`${t.ticker}-${i}`} style={bm.modalTickerRow}>
                      <Text style={bm.modalIdx}>{i + 1}.</Text>
                      <Text style={bm.modalTicker}>{t.ticker}</Text>
                      <Text style={bm.modalVal}>{t.value != null ? t.value : '—'}</Text>
                    </View>
                  ))}
                  {poolModalData.tickers.length === 0 && !poolModalData.note && (
                    <Text style={[bm.emptyText, { textAlign: 'center', marginVertical: 12 }]}>No tickers</Text>
                  )}
                </ScrollView>
              </>
            ) : null}
          </View>
        </View>
      </Modal>
    </View>
  );
}

const bm = StyleSheet.create({
  tabRow: { flexDirection: 'row', gap: 4, marginBottom: 10 },
  tab: { flex: 1, paddingVertical: 6, borderRadius: 6, backgroundColor: COLORS.background, alignItems: 'center', borderWidth: 1, borderColor: COLORS.border },
  tabActive: { backgroundColor: COLORS.primary, borderColor: COLORS.primary },
  tabText: { fontSize: 11, fontWeight: '600', color: COLORS.textMuted },
  tabTextActive: { color: '#fff' },

  dropdownWrap: { marginBottom: 10, zIndex: 10 },
  dropdownBtn: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', padding: 10, borderRadius: 8, backgroundColor: COLORS.background, borderWidth: 1, borderColor: COLORS.border },
  dropdownBtnText: { fontSize: 12, color: COLORS.text, flex: 1 },
  dropdownList: { borderRadius: 8, borderWidth: 1, borderColor: COLORS.border, backgroundColor: COLORS.card, marginTop: 4, overflow: 'hidden' },
  searchRow: { flexDirection: 'row', alignItems: 'center', gap: 6, paddingHorizontal: 10, paddingVertical: 6, borderBottomWidth: 1, borderBottomColor: COLORS.border },
  searchInput: { flex: 1, fontSize: 12, color: COLORS.text, padding: 0 },
  dropdownScroll: { maxHeight: 180 },
  dropdownItem: { paddingHorizontal: 12, paddingVertical: 8, borderBottomWidth: StyleSheet.hairlineWidth, borderBottomColor: COLORS.border },
  dropdownItemActive: { backgroundColor: COLORS.primary + '18' },
  dropdownItemText: { fontSize: 12, color: COLORS.text },
  dropdownItemTextActive: { color: COLORS.primary, fontWeight: '600' },
  emptyText: { fontSize: 11, color: COLORS.textMuted, textAlign: 'center', paddingVertical: 12 },

  content: { marginTop: 4 },
  metaRow: { flexDirection: 'row', justifyContent: 'space-between', marginBottom: 8 },
  metaText: { fontSize: 10, color: COLORS.textMuted },

  warningBox: { flexDirection: 'row', alignItems: 'center', gap: 6, backgroundColor: '#F59E0B14', borderRadius: 6, padding: 8, marginBottom: 10, borderWidth: 1, borderColor: '#F59E0B33' },
  warningText: { fontSize: 11, color: '#D97706', flex: 1 },

  metricRow: { marginBottom: 10 },
  metricHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', marginBottom: 3 },
  metricName: { fontSize: 11, color: COLORS.text, fontWeight: '500' },
  metricValue: { fontSize: 12, fontWeight: '700', color: COLORS.text },
  barTrack: { height: 6, backgroundColor: COLORS.border, borderRadius: 3, overflow: 'hidden' },
  barFill: { height: 6, backgroundColor: COLORS.primary, borderRadius: 3 },
  metricNUsed: { fontSize: 9, color: COLORS.textMuted, marginTop: 1 },
  metricNUsedClickable: { color: '#6366F1', textDecorationLine: 'underline' },

  // Pool ticker list modal
  modalOverlay: { flex: 1, backgroundColor: 'rgba(0,0,0,0.5)', justifyContent: 'center', alignItems: 'center', padding: 24 },
  modalCard: { backgroundColor: COLORS.card, borderRadius: 12, padding: 16, width: '100%', maxWidth: 420, maxHeight: '80%', borderWidth: 1, borderColor: COLORS.border },
  modalHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 },
  modalTitle: { fontSize: 12, fontWeight: '700', color: COLORS.text, flex: 1, marginRight: 8 },
  modalSub: { fontSize: 11, color: COLORS.textLight, marginBottom: 4 },
  modalScroll: { maxHeight: 400 },
  modalTickerRow: { flexDirection: 'row', alignItems: 'center', paddingVertical: 4, borderBottomWidth: 1, borderBottomColor: COLORS.border + '44' },
  modalIdx: { fontSize: 10, color: COLORS.textMuted, width: 28, textAlign: 'right', marginRight: 6 },
  modalTicker: { fontSize: 11, color: COLORS.text, fontWeight: '600', flex: 1 },
  modalVal: { fontSize: 11, color: COLORS.textLight, textAlign: 'right', minWidth: 60 },
});

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

function IntegrityMetric({ label, value, status }: { label: string; value: string; status?: 'green' | 'yellow' | 'red' }) {
  const bg = status === 'green' ? '#22C55E18' : status === 'yellow' ? '#F59E0B18' : status === 'red' ? '#EF444418' : undefined;
  const border = status === 'green' ? '#22C55E44' : status === 'yellow' ? '#F59E0B44' : status === 'red' ? '#EF444444' : COLORS.border;
  const fg = status === 'green' ? '#16A34A' : status === 'yellow' ? '#D97706' : status === 'red' ? '#DC2626' : COLORS.text;
  return (
    <View style={[d.intMetric, bg != null && { backgroundColor: bg }, { borderColor: border }]}>
      <Text style={[d.intValue, { color: fg }]}>{value}</Text>
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
  opsItemCol: { width: '47%', flexDirection: 'column', gap: 4, backgroundColor: COLORS.background, borderRadius: 8, padding: 10, borderWidth: 1, borderColor: COLORS.border },
  opsLabel: { flex: 1, fontSize: 11, color: COLORS.text },
  opsValue: { fontSize: 11, fontWeight: '700' },
  opsErrorDetail: { fontSize: 9, color: '#DC2626', marginTop: 2 },
  opsRunBtn: { marginLeft: 4, backgroundColor: '#06B6D4', paddingHorizontal: 6, paddingVertical: 3, borderRadius: 4 },
  opsRunBtnText: { color: '#fff', fontSize: 9, fontWeight: '700' },

  // C) Price Integrity
  integrityGridCompact: { flexDirection: 'row', flexWrap: 'wrap', gap: 6, marginBottom: 8 },
  integrityGrid: { flexDirection: 'row', flexWrap: 'wrap', gap: 8, marginBottom: 12 },
  intMetric: { width: '47%', backgroundColor: COLORS.background, borderRadius: 6, padding: 7, borderWidth: 1, borderColor: COLORS.border, alignItems: 'center' },
  intValue: { fontSize: 14, fontWeight: '800', color: COLORS.text },
  intLabel: { fontSize: 8, color: COLORS.textMuted, textAlign: 'center', marginTop: 1 },

  // Compact card & section title
  cardCompact: { margin: 12, marginBottom: 0, backgroundColor: COLORS.card, borderRadius: 10, padding: 10, borderWidth: 1, borderColor: COLORS.border },
  sectionTitleSm: { fontSize: 11, fontWeight: '700', color: COLORS.textMuted, letterSpacing: 0.5, marginBottom: 6, textTransform: 'uppercase' },

  // Compact toggle row
  compactToggleRow: { flexDirection: 'row', alignItems: 'center', gap: 6, paddingVertical: 3 },
  compactToggleText: { fontSize: 10, color: COLORS.text, flex: 1 },
  compactToggleBtn: { fontSize: 9, fontWeight: '700', color: COLORS.primary, paddingHorizontal: 4 },

  subSection: { fontSize: 11, fontWeight: '600', color: COLORS.textMuted, marginBottom: 4 },
  cpHint: { fontSize: 9, color: COLORS.textMuted, marginBottom: 8, fontStyle: 'italic' },

  // Coverage checkpoints
  cpRow: { flexDirection: 'row', alignItems: 'center', paddingVertical: 5, borderBottomWidth: 1, borderBottomColor: COLORS.border + '55' },
  cpDot: { width: 6, height: 6, borderRadius: 3, marginRight: 8 },
  cpLabel: { fontSize: 11, color: COLORS.text, width: 110 },
  cpDate: { fontSize: 10, color: COLORS.textMuted, flex: 1 },
  cpValue: { fontSize: 11, fontWeight: '600', color: '#22C55E' },
});

// ─── Skeleton placeholder styles (shared across admin tabs) ───────────────────
const sk = StyleSheet.create({
  titleBar: { width: 80, height: 12, borderRadius: 4, backgroundColor: COLORS.border, marginBottom: 10 },
  bar: { borderRadius: 4, backgroundColor: COLORS.border },
  circle: { borderRadius: 999, backgroundColor: COLORS.border },
});

// ─── Main Admin Screen ────────────────────────────────────────────────────────

export default function AdminScreen() {
  const { isAdmin, sessionToken, isLoading } = useAuth();
  const [activeTab, setActiveTab] = useState<Tab>('dashboard');
  // Track which tabs have been visited so we lazy-mount them on first visit
  // but keep them mounted (retain state / avoid re-fetch) on subsequent switches.
  const [visitedTabs, setVisitedTabs] = useState<Set<Tab>>(new Set(['dashboard']));

  const handleTabPress = useCallback((tab: Tab) => {
    setActiveTab(tab);
    setVisitedTabs(prev => {
      if (prev.has(tab)) return prev;
      return new Set([...prev, tab]);
    });
  }, []);

  if (isLoading) {
    return (
      <SafeAreaView style={a.container}>
        <BrandedLoading message="Checking admin access..." />
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
    <SafeAreaView style={a.container} edges={['top']}>
      <AppHeader title="Admin Panel" />

      {/* Tabs */}
      <View style={a.tabBar}>
        {tabs.map(tab => (
          <TouchableOpacity
            key={tab.id}
            style={[a.tabBtn, activeTab === tab.id && a.tabBtnActive]}
            onPress={() => handleTabPress(tab.id)}
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

      {/* Tab Content — lazy-mount on first visit, keep mounted to preserve state */}
      {visitedTabs.has('dashboard') && (
        <View style={activeTab === 'dashboard' ? a.tabContentVisible : a.tabContentHidden}>
          <DashboardTab sessionToken={sessionToken} />
        </View>
      )}
      {visitedTabs.has('pipeline') && (
        <View style={activeTab === 'pipeline' ? a.tabContentVisible : a.tabContentHidden}>
          <PipelineTab sessionToken={sessionToken} />
        </View>
      )}
      {visitedTabs.has('customers') && (
        <View style={activeTab === 'customers' ? a.tabContentVisible : a.tabContentHidden}>
          <CustomersTab sessionToken={sessionToken} />
        </View>
      )}
    </SafeAreaView>
  );
}

const a = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.background },
  center: { flex: 1, alignItems: 'center', justifyContent: 'center', gap: 8 },

  tabContentVisible: { display: 'flex' as any, flex: 1 },
  tabContentHidden: { display: 'none' as any },

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
