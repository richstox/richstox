/**
 * Admin Panel v2 (P48 - Single Source of Truth)
 * ==============================================
 * 0) Uses GLOBAL AppHeader component - same as end-user app
 * 1) Health = Unknown when no runs today
 * 2) API calls = Unknown/null (not 0)
 * 3) Universe Funnel from shared service (Talk-aligned)
 * 4) Jobs sorted by next run time
 * 
 * BINDING: Do not change without Richard's approval.
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  RefreshControl,
  TouchableOpacity,
  ActivityIndicator,
  Platform,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useRouter } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import { useAuth } from '../contexts/AuthContext';
import { COLORS } from './_layout';
import AppHeader from '../components/AppHeader';

const API_URL = process.env.EXPO_PUBLIC_BACKEND_URL || '';

// Types
interface Job {
  name: string;
  status: string;
  scheduled_time: string;
  next_run: string;
  next_run_iso: string | null;
  has_api_calls: boolean;
  sunday_only: boolean;
  ran_today: boolean;
  expected_by_now: boolean;
  last_run_started?: string;
  last_run_finished?: string;
  duration_ms?: number;
  api_calls?: number | string;
  records_updated?: number;
  message?: string;
  error_summary?: string;
  api_endpoint?: string;  // P53: API endpoint URL for external jobs
  schedule_type?: 'auto' | 'manual';  // P1: Manual jobs
  is_manual?: boolean;
}

interface FunnelStep {
  step: number;
  name: string;
  count: number;
  query: string;
  source_job: string;
  warning?: string;
  breakdown?: string;
  note?: string;
}

interface Inconsistency {
  type: string;
  message: string;
}

// A4: Environment info interface
interface EnvironmentInfo {
  environment: string;
  db_name: string;
  db_host: string;
}

interface AdminOverview {
  generated_at: string;
  load_time_ms: number;
  today_boundary_prague: string;
  environment?: EnvironmentInfo;  // A4: Added
  health: {
    score_pct: number | null;
    status: string;
    deductions: string[];
    scheduler_active: boolean;
    jobs_completed: number;
    jobs_failed: number;
    jobs_total: number;
    overdue_count: number;
    has_runs_today: boolean;
    latest_data_date: string | null;
    api_calls_today: number | null;
    api_breakdown: Record<string, number> | null;
    api_guard?: {
      passed: boolean | null;
      last_check: string | null;
      status: string;
    };
  };
  jobs: {
    registry_count: number;
    all_sorted: Job[];
    overdue: Job[];
    failed: Job[];
    completed: Job[];
    pending: Job[];
    not_scheduled: Job[];
  };
  universe_funnel: {
    generated_at: string;
    funnel_steps: FunnelStep[];
    inconsistencies: Inconsistency[];
    has_inconsistency: boolean;
    counts: Record<string, number>;
    visible_universe_count: number;
  };
  visible_universe_count: number;
  // NEW: Job last runs from system_job_logs (observability layer)
  job_last_runs?: Record<string, {
    status: string;
    start_time: string;
    end_time: string;
    duration_seconds: number;
    records_processed: number;
    error_message: string | null;
  } | null>;
}

export default function AdminPanel() {
  const router = useRouter();
  const { user, isAdmin, sessionToken } = useAuth();
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [data, setData] = useState<AdminOverview | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showCompleted, setShowCompleted] = useState(false);
  const [showNotScheduled, setShowNotScheduled] = useState(false);
  const [showPending, setShowPending] = useState(true);
  const [visibilityAudit, setVisibilityAudit] = useState<any>(null);
  const [runningJob, setRunningJob] = useState<string | null>(null);
  const [showAuditDetails, setShowAuditDetails] = useState(false);

  const fetchVisibilityAudit = useCallback(async () => {
    try {
      const headers: Record<string, string> = { 'Content-Type': 'application/json' };
      if (sessionToken) headers['Authorization'] = `Bearer ${sessionToken}`;
      
      const res = await fetch(`${API_URL}/api/admin/visibility-audit`, { headers });
      if (res.ok) {
        setVisibilityAudit(await res.json());
      }
    } catch (e) {
      console.error('Failed to fetch visibility audit:', e);
    }
  }, [sessionToken]);

  const runVisibilityCleanup = useCallback(async () => {
    if (runningJob) return;
    setRunningJob('recompute_visibility_all');
    try {
      const headers: Record<string, string> = { 'Content-Type': 'application/json' };
      if (sessionToken) headers['Authorization'] = `Bearer ${sessionToken}`;
      
      const res = await fetch(`${API_URL}/api/admin/job/recompute_visibility_all/run?wait=false`, { 
        method: 'POST',
        headers 
      });
      if (res.ok) {
        alert('Visibility cleanup job started. Refresh in ~30s to see results.');
      } else {
        alert('Failed to start job');
      }
    } catch (e) {
      alert('Network error');
    } finally {
      setRunningJob(null);
    }
  }, [sessionToken, runningJob]);

  const fetchData = useCallback(async () => {
    try {
      const headers: Record<string, string> = { 'Content-Type': 'application/json' };
      if (sessionToken) headers['Authorization'] = `Bearer ${sessionToken}`;
      
      const res = await fetch(`${API_URL}/api/admin/overview`, { headers });
      if (res.ok) {
        setData(await res.json());
        setError(null);
      } else {
        setError('Failed to load');
      }
    } catch {
      setError('Network error');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [sessionToken]);

  useEffect(() => {
    if (isAdmin) fetchData();
    else setLoading(false);
  }, [isAdmin, fetchData]);

  // Fetch visibility audit data
  useEffect(() => {
    if (isAdmin) fetchVisibilityAudit();
  }, [isAdmin, fetchVisibilityAudit]);

  const onRefresh = useCallback(() => {
    setRefreshing(true);
    fetchData();
    fetchVisibilityAudit();
  }, [fetchData, fetchVisibilityAudit]);

  // Access denied
  if (!isAdmin) {
    return (
      <SafeAreaView style={styles.container}>
        <AppHeader showBackButton />
        <View style={styles.centered}>
          <Ionicons name="shield-outline" size={48} color={COLORS.textMuted} />
          <Text style={styles.accessDenied}>Admin Access Required</Text>
        </View>
      </SafeAreaView>
    );
  }

  if (loading) {
    return (
      <SafeAreaView style={styles.container}>
        <AppHeader showBackButton />
        <View style={styles.centered}>
          <ActivityIndicator size="large" color={COLORS.primary} />
        </View>
      </SafeAreaView>
    );
  }

  if (error || !data) {
    return (
      <SafeAreaView style={styles.container}>
        <AppHeader showBackButton />
        <View style={styles.centered}>
          <Text style={styles.errorText}>{error}</Text>
          <TouchableOpacity style={styles.retryBtn} onPress={fetchData}>
            <Text style={styles.retryBtnText}>Retry</Text>
          </TouchableOpacity>
        </View>
      </SafeAreaView>
    );
  }

  const { health, jobs, universe_funnel } = data;
  const env = data.environment;

  return (
    <SafeAreaView style={styles.container}>
      {/* GLOBAL HEADER - Same as end-user app */}
      <AppHeader showBackButton />

      <ScrollView
        style={styles.scroll}
        refreshControl={<RefreshControl refreshing={refreshing} onRefresh={onRefresh} tintColor={COLORS.primary} />}
      >
        {/* ADMIN PANEL TITLE */}
        <View style={styles.titleRow}>
          <Text style={styles.title}>Admin Panel</Text>
          <View style={styles.badge}><Text style={styles.badgeText}>ADMIN</Text></View>
          <Text style={styles.loadTime}>{data.load_time_ms}ms</Text>
        </View>
        <Text style={styles.boundary}>Today: {data.today_boundary_prague} (Prague)</Text>
        
        {/* A4: ENVIRONMENT INFO */}
        {env && (
          <View style={styles.envRow}>
            <View style={[styles.envBadge, env.environment === 'production' ? styles.envProd : styles.envDev]}>
              <Text style={styles.envBadgeText}>{env.environment?.toUpperCase() || 'DEV'}</Text>
            </View>
            <Text style={styles.envText}>DB: {env.db_name} @ {env.db_host}</Text>
          </View>
        )}

        {/* QUICK ACTIONS */}
        <View style={styles.quickActions}>
          <TouchableOpacity 
            style={styles.quickActionButton}
            onPress={() => router.push('/admin-fundamentals-refill')}
            data-testid="goto-fundamentals-refill"
          >
            <Ionicons name="document-lock" size={16} color={COLORS.primary} />
            <Text style={styles.quickActionText}>Fundamentals Refill</Text>
          </TouchableOpacity>
        </View>

        {/* HEALTH SCORE */}
        <View style={styles.card}>
          <View style={styles.cardHeader}>
            <Ionicons 
              name="heart" 
              size={18} 
              color={health.score_pct === null ? '#6B7280' : health.score_pct >= 80 ? '#22C55E' : health.score_pct >= 50 ? '#F59E0B' : '#EF4444'} 
            />
            <Text style={styles.cardTitle}>System Health</Text>
          </View>
          
          <View style={styles.healthScoreRow}>
            <Text style={[
              styles.healthScore, 
              { color: health.score_pct === null ? '#6B7280' : health.score_pct >= 80 ? '#22C55E' : health.score_pct >= 50 ? '#F59E0B' : '#EF4444' }
            ]}>
              {health.score_pct !== null ? `${health.score_pct}%` : '—'}
            </Text>
            <Text style={[
              styles.healthStatus,
              { color: health.status === 'Good' ? '#22C55E' : health.status === 'Warning' ? '#F59E0B' : health.status === 'Critical' ? '#EF4444' : '#6B7280' }
            ]}>
              {health.status}
            </Text>
          </View>

          {health.deductions.length > 0 && (
            <View style={styles.deductions}>
              {health.deductions.map((d, i) => (
                <Text key={i} style={styles.deductionText}>{d}</Text>
              ))}
            </View>
          )}

          <View style={styles.healthGrid}>
            <HealthItem label="Scheduler" value={health.scheduler_active ? 'Active' : 'Paused'} />
            <HealthItem label="Jobs" value={`${health.jobs_completed}/${health.jobs_total}`} />
            <HealthItem label="Overdue" value={health.overdue_count} warn={health.overdue_count > 0} />
            <HealthItem label="Failed" value={health.jobs_failed} error={health.jobs_failed > 0} />
          </View>

          {/* API Calls */}
          <View style={styles.apiBox}>
            <Text style={styles.apiTitle}>
              API Calls Today: {health.api_calls_today !== null ? health.api_calls_today : 'Unknown'}
            </Text>
            {health.api_breakdown && Object.keys(health.api_breakdown).length > 0 ? (
              <View style={styles.apiBreakdown}>
                {Object.entries(health.api_breakdown).map(([key, val]) => (
                  <Text key={key} style={styles.apiItem}>{key}: {val}</Text>
                ))}
              </View>
            ) : (
              <Text style={styles.apiItem}>No API call data logged today</Text>
            )}
          </View>

          <Text style={styles.latestData}>Latest data: {health.latest_data_date || 'Unknown'}</Text>
        </View>

        {/* UNIVERSE FUNNEL - Summary (numbers only) */}
        <View style={styles.card}>
          <View style={styles.cardHeader}>
            <Ionicons name="funnel" size={18} color="#6366F1" />
            <Text style={styles.cardTitle}>Universe Funnel</Text>
            <Text style={[
              styles.timestamp,
              { color: visibilityAudit?.mismatch > 0 ? '#EF4444' : '#22C55E' }
            ]}>
              {visibilityAudit?.mismatch > 0 ? 'MISMATCH' : 'OK'}
            </Text>
          </View>
          
          {visibilityAudit?.mismatch > 0 && (
            <View style={styles.warningBox}>
              <Ionicons name="warning" size={14} color="#EF4444" />
              <Text style={styles.warningText}>
                Canonical sieve ({visibilityAudit.canonical_sieve_count}) != is_visible ({visibilityAudit.is_visible_count})
              </Text>
              <TouchableOpacity 
                style={styles.runJobButton}
                onPress={runVisibilityCleanup}
                disabled={runningJob === 'recompute_visibility_all'}
              >
                {runningJob === 'recompute_visibility_all' ? (
                  <ActivityIndicator size="small" color="#fff" />
                ) : (
                  <Text style={styles.runJobButtonText}>Run Cleanup</Text>
                )}
              </TouchableOpacity>
            </View>
          )}

          {/* Simplified funnel - numbers only */}
          <View style={styles.simpleFunnelRow}>
            <Text style={styles.simpleFunnelLabel}>Seeded (NYSE+NASDAQ Common Stock)</Text>
            <Text style={styles.simpleFunnelCount}>{universe_funnel.counts?.seeded_us_total?.toLocaleString() || '-'}</Text>
          </View>
          <View style={styles.simpleFunnelRow}>
            <Text style={styles.simpleFunnelLabel}>With Price Data</Text>
            <Text style={styles.simpleFunnelCount}>{universe_funnel.counts?.with_price_data?.toLocaleString() || '-'}</Text>
          </View>
          <View style={styles.simpleFunnelRow}>
            <Text style={styles.simpleFunnelLabel}>With Classification (Sector+Industry)</Text>
            <Text style={styles.simpleFunnelCount}>{universe_funnel.counts?.with_classification?.toLocaleString() || '-'}</Text>
          </View>
          <View style={styles.simpleFunnelRow}>
            <Text style={styles.simpleFunnelLabel}>Not Delisted</Text>
            <Text style={styles.simpleFunnelCount}>{universe_funnel.counts?.passes_visibility_rule?.toLocaleString() || '-'}</Text>
          </View>
          <View style={[styles.simpleFunnelRow, styles.simpleFunnelFinal]}>
            <Text style={styles.simpleFunnelLabelFinal}>Visible (final)</Text>
            <Text style={styles.simpleFunnelCountFinal}>{universe_funnel.counts?.visible_tickers?.toLocaleString() || '-'}</Text>
          </View>
        </View>

        {/* DETAILS ACCORDION */}
        {visibilityAudit && (
          <View style={styles.card}>
            <TouchableOpacity 
              style={styles.accordionHeader}
              onPress={() => setShowAuditDetails(!showAuditDetails)}
              data-testid="audit-details-toggle"
            >
              <View style={styles.cardHeader}>
                <Ionicons name="document-text" size={18} color="#6B7280" />
                <Text style={styles.cardTitle}>Details</Text>
              </View>
              <Ionicons 
                name={showAuditDetails ? "chevron-up" : "chevron-down"} 
                size={20} 
                color="#6B7280" 
              />
            </TouchableOpacity>

            {showAuditDetails && (
              <View style={styles.accordionContent}>
                {/* API URLs */}
                {visibilityAudit.api_calls && (
                  <>
                    <Text style={styles.subsectionTitle}>API URLs:</Text>
                    <View style={styles.apiCallsBox}>
                      {Object.entries(visibilityAudit.api_calls).map(([key, api]: [string, any]) => (
                        <View key={key} style={styles.apiCallRow}>
                          <Text style={styles.apiCallName}>{api.description}</Text>
                          <Text style={styles.apiCallUrl}>{api.url}</Text>
                        </View>
                      ))}
                    </View>
                  </>
                )}

                {/* Exclude Patterns */}
                {visibilityAudit.exclude_patterns && (
                  <>
                    <Text style={styles.subsectionTitle}>Exclude Patterns:</Text>
                    <View style={styles.excludePatternsBox}>
                      {Object.entries(visibilityAudit.exclude_patterns).map(([category, patterns]: [string, any]) => (
                        <Text key={category} style={styles.excludePatternText}>
                          {category}: {patterns.join(', ')}
                        </Text>
                      ))}
                    </View>
                  </>
                )}

                {/* Exchange Breakdown */}
                <Text style={styles.subsectionTitle}>Exchange Breakdown:</Text>
                <View style={styles.exchangeBox}>
                  {visibilityAudit.exchange_breakdown && (
                    <>
                      <View style={styles.exchangeRow}>
                        <Text style={styles.exchangeName}>NYSE</Text>
                        <Text style={styles.exchangeCount}>{visibilityAudit.exchange_breakdown.NYSE?.toLocaleString()}</Text>
                      </View>
                      <View style={styles.exchangeRow}>
                        <Text style={styles.exchangeName}>NASDAQ</Text>
                        <Text style={styles.exchangeCount}>{visibilityAudit.exchange_breakdown.NASDAQ?.toLocaleString()}</Text>
                      </View>
                      <View style={[styles.exchangeRow, styles.exchangeExcluded]}>
                        <Text style={styles.exchangeNameExcluded}>NYSE MKT (excluded)</Text>
                        <Text style={styles.exchangeCountExcluded}>{visibilityAudit.exchange_breakdown.NYSE_MKT?.toLocaleString()}</Text>
                      </View>
                      <View style={[styles.exchangeRow, styles.exchangeExcluded]}>
                        <Text style={styles.exchangeNameExcluded}>NYSE ARCA (excluded)</Text>
                        <Text style={styles.exchangeCountExcluded}>{visibilityAudit.exchange_breakdown.NYSE_ARCA?.toLocaleString()}</Text>
                      </View>
                    </>
                  )}
                </View>

                {/* Step-by-step losses */}
                <Text style={styles.subsectionTitle}>Step-by-step Losses:</Text>
                {visibilityAudit.funnel && visibilityAudit.funnel.map((step: any) => (
                  <View key={step.step} style={styles.funnelStepRow}>
                    <View style={styles.funnelStepHeader}>
                      <Text style={styles.funnelStepNumber}>Step {step.step}</Text>
                      <Text style={styles.funnelStepCount}>{step.count.toLocaleString()}</Text>
                      {step.lost > 0 && (
                        <Text style={styles.funnelStepLost}>-{step.lost.toLocaleString()}</Text>
                      )}
                    </View>
                    <Text style={styles.funnelStepName}>{step.name}</Text>
                    <Text style={styles.funnelStepQuery}>{step.query}</Text>
                    {step.lost > 0 && step.lost_reason && (
                      <Text style={styles.funnelStepLostReason}>{step.lost_reason}</Text>
                    )}
                  </View>
                ))}

                {/* Failed Reasons */}
                {visibilityAudit.failed_reasons && Object.keys(visibilityAudit.failed_reasons).length > 0 && (
                  <View style={styles.failedReasonsBox}>
                    <Text style={styles.subsectionTitle}>Failed Reasons:</Text>
                    {Object.entries(visibilityAudit.failed_reasons).map(([reason, count]: [string, any]) => (
                      <View key={reason} style={styles.failedReasonRow}>
                        <Text style={styles.failedReasonName}>{reason}</Text>
                        <Text style={styles.failedReasonCount}>{count.toLocaleString()}</Text>
                      </View>
                    ))}
                  </View>
                )}

                {/* Last Audit Timestamp + PASS/FAIL */}
                <View style={styles.auditStatusRow}>
                  <Text style={styles.latestData}>
                    Last audit: {visibilityAudit.last_audit?.status || 'NEVER'}
                    {visibilityAudit.last_audit?.timestamp && ` at ${new Date(visibilityAudit.last_audit.timestamp).toLocaleString()}`}
                  </Text>
                  <View style={[
                    styles.auditBadge,
                    { backgroundColor: visibilityAudit.mismatch > 0 ? '#FEE2E2' : '#DCFCE7' }
                  ]}>
                    <Text style={[
                      styles.auditBadgeText,
                      { color: visibilityAudit.mismatch > 0 ? '#DC2626' : '#16A34A' }
                    ]}>
                      {visibilityAudit.mismatch > 0 ? 'FAIL' : 'PASS'}
                    </Text>
                  </View>
                </View>
              </View>
            )}
          </View>
        )}

        {/* SCHEDULER JOBS (sorted by next run) */}
        <View style={styles.card}>
          <View style={styles.cardHeader}>
            <Ionicons name="time" size={18} color="#F59E0B" />
            <Text style={styles.cardTitle}>Scheduler Jobs ({jobs.registry_count})</Text>
            <Text style={styles.timestamp}>Sorted by next run</Text>
          </View>

          {/* Overdue */}
          {jobs.overdue.length > 0 && (
            <JobSection title={`Overdue (${jobs.overdue.length})`} jobs={jobs.overdue} defaultOpen onRefresh={fetchData} jobLastRuns={data?.job_last_runs} />
          )}

          {/* Failed */}
          {jobs.failed.length > 0 && (
            <JobSection title={`Failed (${jobs.failed.length})`} jobs={jobs.failed} defaultOpen type="error" onRefresh={fetchData} jobLastRuns={data?.job_last_runs} />
          )}

          {/* Pending */}
          {jobs.pending.length > 0 && (
            <JobSection 
              title={`Pending (${jobs.pending.length})`} 
              jobs={jobs.pending} 
              defaultOpen={showPending}
              onToggle={() => setShowPending(!showPending)}
              onRefresh={fetchData}
              jobLastRuns={data?.job_last_runs}
            />
          )}

          {/* Completed */}
          {jobs.completed.length > 0 && (
            <JobSection 
              title={`Completed (${jobs.completed.length})`} 
              jobs={jobs.completed} 
              defaultOpen={showCompleted}
              onToggle={() => setShowCompleted(!showCompleted)}
              type="success"
              onRefresh={fetchData}
              jobLastRuns={data?.job_last_runs}
            />
          )}

          {/* Not Scheduled */}
          {jobs.not_scheduled.length > 0 && (
            <JobSection 
              title={`Not Scheduled Today (${jobs.not_scheduled.length})`} 
              jobs={jobs.not_scheduled} 
              defaultOpen={showNotScheduled}
              onToggle={() => setShowNotScheduled(!showNotScheduled)}
              onRefresh={fetchData}
              jobLastRuns={data?.job_last_runs}
            />
          )}
        </View>

        <View style={{ height: 60 }} />
      </ScrollView>
    </SafeAreaView>
  );
}

// ============================================================================
// COMPONENTS
// ============================================================================

function HealthItem({ label, value, warn, error }: { label: string; value: any; warn?: boolean; error?: boolean }) {
  return (
    <View style={styles.healthItem}>
      <Text style={[styles.healthValue, warn && styles.warnText, error && styles.errorTextVal]}>{value}</Text>
      <Text style={styles.healthLabel}>{label}</Text>
    </View>
  );
}

function JobSection({ title, jobs, defaultOpen, onToggle, type, onRefresh, jobLastRuns }: { 
  title: string; 
  jobs: Job[]; 
  defaultOpen?: boolean;
  onToggle?: () => void;
  type?: string;
  onRefresh?: () => void;
  jobLastRuns?: AdminOverview['job_last_runs'];
}) {
  const [open, setOpen] = useState(defaultOpen ?? false);
  
  const handleToggle = () => {
    if (onToggle) onToggle();
    else setOpen(!open);
  };
  
  const isOpen = onToggle ? defaultOpen : open;

  return (
    <View style={styles.jobSection}>
      <TouchableOpacity style={styles.jobSectionHeader} onPress={handleToggle}>
        <Text style={styles.jobSectionTitle}>{title}</Text>
        <Ionicons name={isOpen ? 'chevron-up' : 'chevron-down'} size={16} color={COLORS.textMuted} />
      </TouchableOpacity>
      {isOpen && jobs.map((job) => (
        <JobRow key={job.name} job={job} type={type} onRefresh={onRefresh} lastRun={jobLastRuns?.[job.name]} />
      ))}
    </View>
  );
}

function JobRow({ job, type, onRefresh, lastRun }: { 
  job: Job; 
  type?: string; 
  onRefresh?: () => void;
  lastRun?: AdminOverview['job_last_runs'] extends Record<string, infer T> ? T : never;
}) {
  const [isRunning, setIsRunning] = useState(false);
  
  const getIcon = () => {
    if (job.status === 'success') return { name: 'checkmark-circle' as const, color: '#22C55E' };
    if (job.status === 'error') return { name: 'close-circle' as const, color: '#EF4444' };
    if (job.status === 'not_scheduled') return { name: 'calendar-outline' as const, color: '#6B7280' };
    if (job.status === 'overdue') return { name: 'alert-circle' as const, color: '#F59E0B' };
    return { name: 'time-outline' as const, color: '#6B7280' };
  };
  
  const icon = getIcon();
  const isExternal = job.has_api_calls;
  const isManual = job.is_manual || job.schedule_type === 'manual';
  
  // Format relative time
  const formatRelativeTime = (isoString: string | null | undefined): string => {
    if (!isoString) return 'Never';
    const date = new Date(isoString);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMins / 60);
    const diffDays = Math.floor(diffHours / 24);
    
    if (diffMins < 1) return 'Just now';
    if (diffMins < 60) return `${diffMins}m ago`;
    if (diffHours < 24) return `${diffHours}h ago`;
    return `${diffDays}d ago`;
  };
  
  const formatDuration = (seconds: number | null | undefined): string => {
    if (!seconds) return '-';
    if (seconds < 60) return `${seconds.toFixed(1)}s`;
    const mins = Math.floor(seconds / 60);
    const secs = Math.round(seconds % 60);
    return `${mins}m ${secs}s`;
  };

  const handleRunNow = async () => {
    if (isRunning) return;
    setIsRunning(true);
    try {
      const endpoint = job.name === 'universe_seed'
        ? `${API_URL}/api/admin/jobs/universe-seed`
        : `${API_URL}/api/admin/job/${job.name}/run`;
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      });
      const result = await response.json();
      console.log('Job run result:', result);
      // Refresh admin panel after job starts
      setTimeout(() => {
        onRefresh?.();
      }, 1000);
    } catch (error) {
      console.error('Failed to run job:', error);
    } finally {
      setIsRunning(false);
    }
  };

  // Shorten URL: remove https://, api_token param, trim to domain+path
  const shortenUrl = (url: string): string => {
    if (!url) return '';
    return url
      .replace(/https?:\/\//, '')
      .replace(/[?&]api_token=[^&]*/g, '')
      .replace(/[?&]fmt=[^&]*/g, '')
      .replace(/[?&]$/, '')
      .replace(/\?$/, '');
  };

  return (
    <View style={[styles.jobRow, type === 'error' && styles.jobRowError, type === 'success' && styles.jobRowSuccess]}>
      <Ionicons name={icon.name} size={16} color={icon.color} style={{ marginTop: 2 }} />
      <View style={styles.jobInfo}>

        {/* LINE 1: Name + badges + Run Now */}
        <View style={styles.jobNameRow}>
          <Text style={styles.jobName}>{job.name.replace(/_/g, ' ')}</Text>
          <View style={[styles.typeBadge, isExternal ? styles.typeBadgeExternal : styles.typeBadgeInternal]}>
            <Text style={[styles.typeBadgeText, isExternal ? styles.typeBadgeTextExternal : styles.typeBadgeTextInternal]}>
              {isExternal ? 'API' : 'Calc'}
            </Text>
          </View>
          {isManual && (
            <View style={styles.manualBadge}>
              <Text style={styles.manualBadgeText}>Manual</Text>
            </View>
          )}
          {/* Run Now inline for manual jobs */}
          {isManual && (
            <TouchableOpacity
              style={[styles.runNowBtn, isRunning && styles.runNowBtnDisabled]}
              onPress={handleRunNow}
              disabled={isRunning}
            >
              <Ionicons name={isRunning ? 'hourglass-outline' : 'play-circle-outline'} size={12} color="#FFF" />
              <Text style={styles.runNowBtnText}>{isRunning ? 'Running…' : 'Run Now'}</Text>
            </TouchableOpacity>
          )}
        </View>

        {/* LINE 2: Next | Sched | Last run */}
        <View style={styles.jobMetaRow}>
          <Text style={styles.jobMeta}>
            Next: {job.next_run}{job.sunday_only ? ' (Sun)' : ''} · Sched: {job.scheduled_time}
          </Text>
          {lastRun ? (
            <View style={styles.lastRunInline}>
              <View style={[styles.statusPill, lastRun.status === 'success' ? styles.statusPillSuccess : styles.statusPillError]}>
                <Text style={styles.statusPillText}>{lastRun.status === 'success' ? 'OK' : 'ERR'}</Text>
              </View>
              <Text style={styles.lastRunText}>
                {formatRelativeTime(lastRun.end_time)} · {formatDuration(lastRun.duration_seconds)}
              </Text>
            </View>
          ) : (
            <Text style={styles.neverRunText}>Never run</Text>
          )}
        </View>

        {/* LINE 3 (optional): shortened URL */}
        {isExternal && job.api_endpoint && (
          <Text style={styles.jobApiUrl} numberOfLines={1}>{shortenUrl(job.api_endpoint)}</Text>
        )}

        {job.error_summary && <Text style={styles.jobError}>⚠ {job.error_summary}</Text>}
      </View>
    </View>
  );
}

// ============================================================================
// STYLES
// ============================================================================

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.background },
  
  scroll: { flex: 1 },
  centered: { flex: 1, justifyContent: 'center', alignItems: 'center', padding: 20 },
  accessDenied: { fontSize: 14, color: COLORS.textMuted, marginTop: 12 },
  errorText: { fontSize: 14, color: '#EF4444' },
  retryBtn: { marginTop: 12, backgroundColor: COLORS.primary, paddingHorizontal: 16, paddingVertical: 8, borderRadius: 6 },
  retryBtnText: { color: '#FFF', fontWeight: '600' },

  // Title
  titleRow: { flexDirection: 'row', alignItems: 'center', paddingHorizontal: 12, paddingTop: 12, gap: 8 },
  title: { fontSize: 18, fontWeight: '600', color: COLORS.text },
  badge: { backgroundColor: COLORS.primary, paddingHorizontal: 6, paddingVertical: 2, borderRadius: 4 },
  badgeText: { fontSize: 9, fontWeight: '700', color: '#FFF' },
  loadTime: { marginLeft: 'auto', fontSize: 11, color: COLORS.textMuted },
  boundary: { fontSize: 10, color: COLORS.textMuted, paddingHorizontal: 12, marginBottom: 8 },

  // Card
  card: { margin: 10, backgroundColor: COLORS.card, borderRadius: 10, padding: 12, borderWidth: 1, borderColor: COLORS.border },
  cardHeader: { flexDirection: 'row', alignItems: 'center', marginBottom: 10, gap: 6 },
  cardTitle: { fontSize: 14, fontWeight: '600', color: COLORS.text, flex: 1 },
  timestamp: { fontSize: 10, color: COLORS.textMuted },

  // Health Score
  healthScoreRow: { flexDirection: 'row', alignItems: 'baseline', gap: 8, marginBottom: 8 },
  healthScore: { fontSize: 36, fontWeight: '700' },
  healthStatus: { fontSize: 16, fontWeight: '600' },
  deductions: { backgroundColor: 'rgba(239,68,68,0.1)', padding: 8, borderRadius: 6, marginBottom: 10 },
  deductionText: { fontSize: 11, color: '#EF4444' },
  healthGrid: { flexDirection: 'row', flexWrap: 'wrap', marginBottom: 8 },
  healthItem: { width: '25%', alignItems: 'center', paddingVertical: 4 },
  healthValue: { fontSize: 14, fontWeight: '700', color: COLORS.text },
  healthLabel: { fontSize: 10, color: COLORS.textMuted },
  warnText: { color: '#F59E0B' },
  errorTextVal: { color: '#EF4444' },
  latestData: { fontSize: 10, color: COLORS.textMuted, textAlign: 'center' },

  // A4: Environment info
  envRow: { flexDirection: 'row', alignItems: 'center', gap: 8, marginBottom: 12, paddingHorizontal: 16 },
  envBadge: { paddingHorizontal: 8, paddingVertical: 3, borderRadius: 4 },
  envProd: { backgroundColor: '#22C55E' },
  envDev: { backgroundColor: '#6B7280' },
  envBadgeText: { fontSize: 10, fontWeight: '700', color: '#FFF' },
  envText: { fontSize: 11, color: COLORS.textMuted, fontFamily: 'monospace' },

  // API Box
  apiBox: { backgroundColor: 'rgba(99,102,241,0.1)', padding: 8, borderRadius: 6, marginTop: 8, marginBottom: 8 },
  apiTitle: { fontSize: 12, fontWeight: '600', color: '#6366F1', marginBottom: 4 },
  apiBreakdown: { flexDirection: 'row', flexWrap: 'wrap', gap: 8 },
  apiItem: { fontSize: 10, color: COLORS.text },

  // Funnel
  funnelRow: { flexDirection: 'row', alignItems: 'flex-start', paddingVertical: 6, paddingHorizontal: 8, gap: 8 },
  funnelWarning: { backgroundColor: 'rgba(239,68,68,0.1)', borderRadius: 4 },
  funnelStep: { fontSize: 11, color: COLORS.textMuted, width: 20, marginTop: 2 },
  funnelInfo: { flex: 1 },
  funnelName: { fontSize: 12, fontWeight: '500', color: COLORS.text },
  funnelQuery: { fontSize: 9, color: COLORS.textMuted },
  funnelBreakdown: { fontSize: 9, color: '#6366F1' },
  funnelCount: { fontSize: 13, fontWeight: '600', color: COLORS.text },
  funnelCountWarning: { color: '#EF4444' },
  warningBox: { flexDirection: 'row', alignItems: 'center', gap: 6, backgroundColor: 'rgba(239,68,68,0.1)', padding: 8, borderRadius: 6, marginBottom: 8 },
  warningText: { fontSize: 11, color: '#EF4444' },
  inconsistencyBox: { marginTop: 8, padding: 8, backgroundColor: 'rgba(239,68,68,0.05)', borderRadius: 6 },
  inconsistencyTitle: { fontSize: 11, fontWeight: '600', color: '#EF4444', marginBottom: 4 },
  inconsistencyText: { fontSize: 10, color: '#EF4444' },

  // Jobs
  jobSection: { marginTop: 6, backgroundColor: 'rgba(0,0,0,0.02)', padding: 6, borderRadius: 6 },
  jobSectionHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center' },
  jobSectionTitle: { fontSize: 11, fontWeight: '600', color: COLORS.textMuted },
  jobRow: { flexDirection: 'row', alignItems: 'flex-start', paddingVertical: 5, gap: 6 },
  jobRowError: { backgroundColor: 'rgba(239,68,68,0.05)', borderRadius: 4, padding: 5, marginVertical: 1 },
  jobRowSuccess: { backgroundColor: 'rgba(34,197,94,0.05)', borderRadius: 4, padding: 5, marginVertical: 1 },
  jobInfo: { flex: 1 },
  jobNameRow: { flexDirection: 'row', alignItems: 'center', gap: 5, flexWrap: 'wrap' },
  jobName: { fontSize: 12, fontWeight: '500', color: COLORS.text, textTransform: 'capitalize' },
  jobMetaRow: { flexDirection: 'row', alignItems: 'center', gap: 8, marginTop: 2, flexWrap: 'wrap' },
  jobMeta: { fontSize: 10, color: COLORS.textMuted },
  jobError: { fontSize: 10, color: '#EF4444', marginTop: 2 },
  jobApiUrl: { fontSize: 9, color: '#0EA5E9', marginTop: 1, fontFamily: 'monospace' },
  
  // Type badges (API/Calc)
  typeBadge: { flexDirection: 'row', alignItems: 'center', paddingHorizontal: 5, paddingVertical: 1, borderRadius: 3, gap: 2 },
  typeBadgeExternal: { backgroundColor: 'rgba(14,165,233,0.1)' },
  typeBadgeInternal: { backgroundColor: 'rgba(139,92,246,0.1)' },
  typeBadgeText: { fontSize: 9, fontWeight: '600' },
  typeBadgeTextExternal: { color: '#0EA5E9' },
  typeBadgeTextInternal: { color: '#8B5CF6' },
  
  // Manual badge
  manualBadge: { flexDirection: 'row', alignItems: 'center', paddingHorizontal: 5, paddingVertical: 1, borderRadius: 3, gap: 2, backgroundColor: 'rgba(245,158,11,0.1)' },
  manualBadgeText: { fontSize: 9, fontWeight: '600', color: '#F59E0B' },
  
  // Run Now button — inline, smaller
  runNowBtn: { flexDirection: 'row', alignItems: 'center', gap: 3, backgroundColor: '#22C55E', paddingHorizontal: 8, paddingVertical: 3, borderRadius: 5, marginLeft: 4 },
  runNowBtnDisabled: { backgroundColor: '#6B7280' },
  runNowBtnText: { fontSize: 10, fontWeight: '600', color: '#FFF' },
  
  // Last Run inline
  lastRunInline: { flexDirection: 'row', alignItems: 'center', gap: 4 },
  lastRunRow: { flexDirection: 'row', alignItems: 'center', gap: 6, marginTop: 2 },
  statusPill: { flexDirection: 'row', alignItems: 'center', paddingHorizontal: 5, paddingVertical: 1, borderRadius: 3 },
  statusPillSuccess: { backgroundColor: '#22C55E' },
  statusPillError: { backgroundColor: '#EF4444' },
  statusPillText: { fontSize: 9, fontWeight: '700', color: '#FFF' },
  lastRunText: { fontSize: 10, color: COLORS.textMuted },
  neverRunText: { fontSize: 10, color: COLORS.textMuted, fontStyle: 'italic' },

  // Visibility Audit (DATA SUPREMACY MANIFESTO v1.0)
  runJobButton: { backgroundColor: '#EF4444', paddingHorizontal: 10, paddingVertical: 4, borderRadius: 4, marginLeft: 'auto' },
  runJobButtonText: { fontSize: 10, fontWeight: '600', color: '#FFF' },
  subsectionTitle: { fontSize: 10, fontWeight: '600', color: COLORS.textMuted, marginTop: 8, marginBottom: 3 },
  failedReasonsBox: { marginTop: 6, backgroundColor: 'rgba(239,68,68,0.05)', padding: 6, borderRadius: 4 },
  failedReasonRow: { flexDirection: 'row', justifyContent: 'space-between', paddingVertical: 1 },
  failedReasonName: { fontSize: 9, color: '#EF4444' },
  failedReasonCount: { fontSize: 9, fontWeight: '600', color: '#EF4444' },
  apiUrlsBox: { marginTop: 6, backgroundColor: 'rgba(99,102,241,0.05)', padding: 6, borderRadius: 4 },
  apiUrlsTitle: { fontSize: 9, fontWeight: '600', color: '#6366F1', marginBottom: 3 },
  apiUrlText: { fontSize: 8, color: COLORS.textMuted, fontFamily: 'monospace' },
  apiCallsBox: { backgroundColor: 'rgba(34,197,94,0.05)', padding: 6, borderRadius: 4 },
  apiCallRow: { marginBottom: 4 },
  apiCallName: { fontSize: 9, fontWeight: '600', color: '#22C55E' },
  apiCallUrl: { fontSize: 8, color: COLORS.textMuted, fontFamily: 'monospace' },
  excludePatternsBox: { backgroundColor: 'rgba(239,68,68,0.05)', padding: 6, borderRadius: 4 },
  excludePatternText: { fontSize: 9, color: '#EF4444', fontFamily: 'monospace' },
  exchangeBox: { backgroundColor: 'rgba(99,102,241,0.05)', padding: 6, borderRadius: 4 },
  exchangeRow: { flexDirection: 'row', justifyContent: 'space-between', paddingVertical: 1 },
  exchangeName: { fontSize: 10, color: COLORS.text },
  exchangeCount: { fontSize: 10, fontWeight: '600', color: COLORS.text },
  exchangeExcluded: { opacity: 0.5 },
  exchangeNameExcluded: { fontSize: 9, color: '#EF4444', fontStyle: 'italic' },
  exchangeCountExcluded: { fontSize: 9, fontWeight: '600', color: '#EF4444' },
  funnelStepRow: { marginBottom: 5, padding: 6, backgroundColor: 'rgba(255,255,255,0.02)', borderRadius: 4, borderLeftWidth: 2, borderLeftColor: '#22C55E' },
  funnelStepHeader: { flexDirection: 'row', alignItems: 'center', gap: 6 },
  funnelStepNumber: { fontSize: 9, fontWeight: '700', color: '#22C55E' },
  funnelStepCount: { fontSize: 11, fontWeight: '700', color: COLORS.text },
  funnelStepLost: { fontSize: 9, fontWeight: '600', color: '#EF4444' },
  funnelStepName: { fontSize: 9, fontWeight: '600', color: COLORS.text, marginTop: 1 },
  funnelStepQuery: { fontSize: 8, color: COLORS.textMuted, fontFamily: 'monospace', marginTop: 1 },
  funnelStepLostReason: { fontSize: 8, color: '#EF4444', fontStyle: 'italic', marginTop: 1 },
  
  // Simple Funnel (Summary)
  simpleFunnelRow: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', paddingVertical: 6, paddingHorizontal: 8, borderBottomWidth: 1, borderBottomColor: 'rgba(0,0,0,0.05)' },
  simpleFunnelLabel: { fontSize: 12, color: COLORS.text },
  simpleFunnelCount: { fontSize: 13, fontWeight: '600', color: COLORS.text },
  simpleFunnelFinal: { backgroundColor: 'rgba(34,197,94,0.1)', borderRadius: 6, borderBottomWidth: 0, marginTop: 4 },
  simpleFunnelLabelFinal: { fontSize: 12, fontWeight: '600', color: '#22C55E' },
  simpleFunnelCountFinal: { fontSize: 14, fontWeight: '700', color: '#22C55E' },
  
  // Accordion
  accordionHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', paddingVertical: 4 },
  accordionContent: { marginTop: 8, paddingTop: 8, borderTopWidth: 1, borderTopColor: COLORS.border },
  
  // Audit Status
  auditStatusRow: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', marginTop: 12, paddingTop: 8, borderTopWidth: 1, borderTopColor: COLORS.border },
  auditBadge: { paddingHorizontal: 10, paddingVertical: 4, borderRadius: 4 },
  auditBadgeText: { fontSize: 11, fontWeight: '700' },
  
  // Quick Actions
  quickActions: { flexDirection: 'row', gap: 8, marginBottom: 16 },
  quickActionButton: { flexDirection: 'row', alignItems: 'center', gap: 6, backgroundColor: COLORS.card, paddingHorizontal: 12, paddingVertical: 8, borderRadius: 8, borderWidth: 1, borderColor: COLORS.border },
  quickActionText: { fontSize: 12, color: COLORS.text, fontWeight: '500' },
});
