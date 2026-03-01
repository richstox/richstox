/**
 * Admin Fundamentals Refill Page
 * ================================
 * BINDING: Uses LOCKED whitelist mapper - only approved fields stored.
 * Status: LOCKED. No changes without explicit Richard approval (2026-02-25).
 */

import { useState, useEffect, useCallback } from 'react';
import { View, Text, ScrollView, TouchableOpacity, ActivityIndicator, StyleSheet } from 'react-native';
import { Ionicons } from '@expo/vector-icons';
import { useRouter } from 'expo-router';
import { useAuth } from '../contexts/AuthContext';
import AppHeader from '../components/AppHeader';

const API_URL = process.env.EXPO_PUBLIC_BACKEND_URL;

// Colors matching admin.tsx
const COLORS = {
  bg: '#0A0F1C',
  card: '#111827',
  border: '#1F2937',
  text: '#F3F4F6',
  textMuted: '#9CA3AF',
  primary: '#6366F1',
  success: '#22C55E',
  warning: '#F59E0B',
  error: '#EF4444',
};

interface WhitelistSection {
  title: string;
  fields: string[];
  status: 'KEEP' | 'DELETE';
}

interface WhitelistDocument {
  version: string;
  status: string;
  approved_by: string;
  approved_date: string;
  is_locked: boolean;
  sections: Record<string, WhitelistSection>;
  binding_rules: string[];
}

interface RefillJob {
  job_id: string;
  status: string;
  started_at: string;
  finished_at?: string;
  tickers_targeted: number;
  tickers_updated: number;
  tickers_failed: number;
  api_calls: number;
  whitelist_version: string;
}

export default function AdminFundamentalsRefill() {
  const router = useRouter();
  const { user, isAdmin, sessionToken } = useAuth();
  const [loading, setLoading] = useState(true);
  const [whitelist, setWhitelist] = useState<WhitelistDocument | null>(null);
  const [confirmed, setConfirmed] = useState(false);
  const [running, setRunning] = useState(false);
  const [jobHistory, setJobHistory] = useState<RefillJob[]>([]);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);
  const [latestJob, setLatestJob] = useState<RefillJob | null>(null);
  const [error, setError] = useState<string | null>(null);

  const headers = { 'Authorization': `Bearer ${sessionToken}` };

  const fetchData = useCallback(async () => {
    try {
      setLoading(true);
      
      // Fetch whitelist document
      const whitelistRes = await fetch(`${API_URL}/api/admin/fundamentals-whitelist`, { headers });
      if (whitelistRes.ok) {
        setWhitelist(await whitelistRes.json());
      }
      
      // Fetch job history
      const historyRes = await fetch(`${API_URL}/api/admin/fundamentals-refill/history?limit=5`, { headers });
      if (historyRes.ok) {
        const data = await historyRes.json();
        setJobHistory(data.jobs || []);
      }
      
      // Fetch latest job status
      const statusRes = await fetch(`${API_URL}/api/admin/fundamentals-refill/status`, { headers });
      if (statusRes.ok) {
        const data = await statusRes.json();
        if (data.status !== 'no_runs') {
          setLatestJob(data);
        }
      }
      
    } catch (e) {
      setError('Failed to load data');
    } finally {
      setLoading(false);
    }
  }, [sessionToken]);

  useEffect(() => {
    if (isAdmin) {
      fetchData();
    }
  }, [isAdmin, fetchData]);

  const runRefill = async () => {
    if (!confirmed) {
      setError('You must confirm the whitelist checkbox first');
      return;
    }
    
    try {
      setRunning(true);
      setError(null);
      
      const res = await fetch(`${API_URL}/api/admin/fundamentals-refill?confirmed=true`, {
        method: 'POST',
        headers,
      });
      
      if (res.ok) {
        const data = await res.json();
        setSuccessMessage(`Job started! Processing ${data.tickers_to_process} tickers...`);
        // Refresh after a short delay
        setTimeout(fetchData, 2000);
      } else {
        const err = await res.json();
        setError(err.detail || 'Failed to start refill');
      }
    } catch (e) {
      setError('Network error');
    } finally {
      setRunning(false);
    }
  };

  if (!isAdmin) {
    return (
      <View style={styles.container}>
        <AppHeader showBackButton />
        <View style={styles.errorBox}>
          <Ionicons name="lock-closed" size={24} color={COLORS.error} />
          <Text style={styles.errorText}>Admin access required</Text>
        </View>
      </View>
    );
  }

  if (loading) {
    return (
      <View style={styles.container}>
        <AppHeader showBackButton />
        <View style={styles.loadingContainer}>
          <ActivityIndicator size="large" color={COLORS.primary} />
          <Text style={styles.loadingText}>Loading...</Text>
        </View>
      </View>
    );
  }

  return (
    <ScrollView style={styles.container}>
      <AppHeader showBackButton />
      
      <View style={styles.content}>
        <Text style={styles.pageTitle}>Fundamentals Refill</Text>
        <Text style={styles.pageSubtitle}>LOCKED Whitelist Mapper</Text>

        {/* LOCKED WHITELIST DOCUMENT */}
        {whitelist && (
          <View style={styles.card}>
            <View style={styles.cardHeader}>
              <Ionicons name="document-lock" size={20} color={COLORS.success} />
              <Text style={styles.cardTitle}>FUNDAMENTALS ALLOWED FIELDS</Text>
              <View style={styles.lockedBadge}>
                <Ionicons name="lock-closed" size={12} color="#fff" />
                <Text style={styles.lockedBadgeText}>LOCKED</Text>
              </View>
            </View>
            
            <View style={styles.metaRow}>
              <Text style={styles.metaText}>Version: {whitelist.version}</Text>
              <Text style={styles.metaText}>Approved: {whitelist.approved_date}</Text>
            </View>
            <Text style={styles.metaText}>By: {whitelist.approved_by}</Text>

            {/* Sections */}
            {Object.entries(whitelist.sections).map(([key, section]) => (
              <View key={key} style={[
                styles.sectionBox,
                section.status === 'DELETE' && styles.sectionBoxDelete
              ]}>
                <View style={styles.sectionHeader}>
                  <Ionicons 
                    name={section.status === 'KEEP' ? 'checkmark-circle' : 'close-circle'} 
                    size={16} 
                    color={section.status === 'KEEP' ? COLORS.success : COLORS.error} 
                  />
                  <Text style={styles.sectionTitle}>{section.title}</Text>
                  <Text style={[
                    styles.sectionStatus,
                    { color: section.status === 'KEEP' ? COLORS.success : COLORS.error }
                  ]}>
                    {section.status}
                  </Text>
                </View>
                <View style={styles.fieldsList}>
                  {section.fields.map((field, i) => (
                    <Text key={i} style={styles.fieldItem}>• {field}</Text>
                  ))}
                </View>
              </View>
            ))}

            {/* Binding Rules */}
            <View style={styles.rulesBox}>
              <Text style={styles.rulesTitle}>BINDING RULES:</Text>
              {whitelist.binding_rules.map((rule, i) => (
                <Text key={i} style={styles.ruleItem}>• {rule}</Text>
              ))}
            </View>
          </View>
        )}

        {/* CONFIRMATION CHECKBOX */}
        <View style={styles.card}>
          <TouchableOpacity 
            style={styles.checkboxRow}
            onPress={() => setConfirmed(!confirmed)}
            data-testid="confirm-whitelist-checkbox"
          >
            <View style={[styles.checkbox, confirmed && styles.checkboxChecked]}>
              {confirmed && <Ionicons name="checkmark" size={16} color="#fff" />}
            </View>
            <Text style={styles.checkboxLabel}>
              I confirm this whitelist is the law and I understand all non-approved fields will be deleted.
            </Text>
          </TouchableOpacity>
        </View>

        {/* JOB CONFIGURATION */}
        <View style={styles.card}>
          <View style={styles.cardHeader}>
            <Ionicons name="settings" size={18} color={COLORS.primary} />
            <Text style={styles.cardTitle}>Job Configuration</Text>
          </View>
          
          <View style={styles.configRow}>
            <Text style={styles.configLabel}>Scope:</Text>
            <Text style={styles.configValue}>All visible tickers (~5,672)</Text>
          </View>
          <View style={styles.configRow}>
            <Text style={styles.configLabel}>API Calls:</Text>
            <Text style={styles.configValue}>~5,672 calls to EODHD /fundamentals</Text>
          </View>
          <View style={styles.configRow}>
            <Text style={styles.configLabel}>Est. Duration:</Text>
            <Text style={styles.configValue}>30-60 minutes</Text>
          </View>
          <View style={styles.configRow}>
            <Text style={styles.configLabel}>Whitelist Version:</Text>
            <Text style={styles.configValue}>{whitelist?.version || '2026-02-25'}</Text>
          </View>
        </View>

        {/* RUN BUTTON */}
        <TouchableOpacity
          style={[
            styles.runButton,
            !confirmed && styles.runButtonDisabled
          ]}
          onPress={runRefill}
          disabled={!confirmed || running}
          data-testid="run-refill-button"
        >
          {running ? (
            <ActivityIndicator size="small" color="#fff" />
          ) : (
            <>
              <Ionicons name="play" size={20} color="#fff" />
              <Text style={styles.runButtonText}>Run Refill Fundamentals</Text>
            </>
          )}
        </TouchableOpacity>

        {error && (
          <View style={styles.errorBox}>
            <Ionicons name="alert-circle" size={16} color={COLORS.error} />
            <Text style={styles.errorText}>{error}</Text>
          </View>
        )}

        {/* LATEST JOB STATUS */}
        {latestJob && (
          <View style={styles.card}>
            <View style={styles.cardHeader}>
              <Ionicons name="time" size={18} color={COLORS.primary} />
              <Text style={styles.cardTitle}>Latest Job</Text>
              <View style={[
                styles.statusBadge,
                { backgroundColor: latestJob.status === 'completed' ? COLORS.success : COLORS.warning }
              ]}>
                <Text style={styles.statusBadgeText}>{latestJob.status.toUpperCase()}</Text>
              </View>
            </View>
            
            <View style={styles.jobStats}>
              <View style={styles.statItem}>
                <Text style={styles.statValue}>{latestJob.tickers_updated}</Text>
                <Text style={styles.statLabel}>Updated</Text>
              </View>
              <View style={styles.statItem}>
                <Text style={styles.statValue}>{latestJob.tickers_failed}</Text>
                <Text style={styles.statLabel}>Failed</Text>
              </View>
              <View style={styles.statItem}>
                <Text style={styles.statValue}>{latestJob.api_calls}</Text>
                <Text style={styles.statLabel}>API Calls</Text>
              </View>
            </View>
            
            <Text style={styles.jobMeta}>
              Started: {new Date(latestJob.started_at).toLocaleString()}
            </Text>
            {latestJob.finished_at && (
              <Text style={styles.jobMeta}>
                Finished: {new Date(latestJob.finished_at).toLocaleString()}
              </Text>
            )}
          </View>
        )}

        {/* JOB HISTORY */}
        {jobHistory.length > 0 && (
          <View style={styles.card}>
            <View style={styles.cardHeader}>
              <Ionicons name="list" size={18} color={COLORS.textMuted} />
              <Text style={styles.cardTitle}>Job History</Text>
            </View>
            
            {jobHistory.map((job, i) => (
              <View key={i} style={styles.historyRow}>
                <View style={[
                  styles.historyDot,
                  { backgroundColor: job.status === 'completed' ? COLORS.success : COLORS.warning }
                ]} />
                <View style={styles.historyContent}>
                  <Text style={styles.historyDate}>
                    {new Date(job.started_at).toLocaleString()}
                  </Text>
                  <Text style={styles.historyStats}>
                    {job.tickers_updated} updated, {job.api_calls} API calls
                  </Text>
                </View>
                <Text style={[
                  styles.historyStatus,
                  { color: job.status === 'completed' ? COLORS.success : COLORS.warning }
                ]}>
                  {job.status}
                </Text>
              </View>
            ))}
          </View>
        )}

        {/* FOOTER NOTE */}
        <View style={styles.footerNote}>
          <Ionicons name="information-circle" size={14} color={COLORS.textMuted} />
          <Text style={styles.footerNoteText}>
            This whitelist is IMMUTABLE. Any changes require explicit Richard approval.
          </Text>
        </View>
      </View>
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.bg },
  content: { padding: 16, paddingBottom: 40 },
  loadingContainer: { flex: 1, justifyContent: 'center', alignItems: 'center', padding: 40 },
  loadingText: { color: COLORS.textMuted, marginTop: 8 },
  
  pageTitle: { fontSize: 24, fontWeight: '700', color: COLORS.text, marginBottom: 4 },
  pageSubtitle: { fontSize: 14, color: COLORS.textMuted, marginBottom: 16 },
  
  card: { backgroundColor: COLORS.card, borderRadius: 12, padding: 16, marginBottom: 16, borderWidth: 1, borderColor: COLORS.border },
  cardHeader: { flexDirection: 'row', alignItems: 'center', gap: 8, marginBottom: 12 },
  cardTitle: { fontSize: 14, fontWeight: '600', color: COLORS.text, flex: 1 },
  
  lockedBadge: { flexDirection: 'row', alignItems: 'center', gap: 4, backgroundColor: COLORS.success, paddingHorizontal: 8, paddingVertical: 4, borderRadius: 4 },
  lockedBadgeText: { fontSize: 10, fontWeight: '700', color: '#fff' },
  
  metaRow: { flexDirection: 'row', gap: 16, marginBottom: 4 },
  metaText: { fontSize: 12, color: COLORS.textMuted },
  
  sectionBox: { backgroundColor: 'rgba(34,197,94,0.05)', borderRadius: 8, padding: 12, marginTop: 12, borderLeftWidth: 3, borderLeftColor: COLORS.success },
  sectionBoxDelete: { backgroundColor: 'rgba(239,68,68,0.05)', borderLeftColor: COLORS.error },
  sectionHeader: { flexDirection: 'row', alignItems: 'center', gap: 8, marginBottom: 8 },
  sectionTitle: { fontSize: 12, fontWeight: '600', color: COLORS.text, flex: 1 },
  sectionStatus: { fontSize: 10, fontWeight: '700' },
  fieldsList: { marginLeft: 24 },
  fieldItem: { fontSize: 11, color: COLORS.textMuted, marginBottom: 2 },
  
  rulesBox: { marginTop: 16, padding: 12, backgroundColor: 'rgba(99,102,241,0.1)', borderRadius: 8 },
  rulesTitle: { fontSize: 11, fontWeight: '700', color: COLORS.primary, marginBottom: 8 },
  ruleItem: { fontSize: 10, color: COLORS.textMuted, marginBottom: 4 },
  
  checkboxRow: { flexDirection: 'row', alignItems: 'flex-start', gap: 12 },
  checkbox: { width: 24, height: 24, borderRadius: 4, borderWidth: 2, borderColor: COLORS.border, justifyContent: 'center', alignItems: 'center' },
  checkboxChecked: { backgroundColor: COLORS.primary, borderColor: COLORS.primary },
  checkboxLabel: { flex: 1, fontSize: 13, color: COLORS.text, lineHeight: 20 },
  
  configRow: { flexDirection: 'row', justifyContent: 'space-between', marginBottom: 8 },
  configLabel: { fontSize: 12, color: COLORS.textMuted },
  configValue: { fontSize: 12, color: COLORS.text, fontWeight: '500' },
  
  runButton: { backgroundColor: COLORS.primary, flexDirection: 'row', alignItems: 'center', justifyContent: 'center', gap: 8, padding: 16, borderRadius: 12, marginBottom: 16 },
  runButtonDisabled: { backgroundColor: COLORS.border, opacity: 0.5 },
  runButtonText: { fontSize: 16, fontWeight: '600', color: '#fff' },
  
  errorBox: { flexDirection: 'row', alignItems: 'center', gap: 8, padding: 12, backgroundColor: 'rgba(239,68,68,0.1)', borderRadius: 8, marginBottom: 16 },
  errorText: { fontSize: 12, color: COLORS.error, flex: 1 },
  
  statusBadge: { paddingHorizontal: 8, paddingVertical: 4, borderRadius: 4 },
  statusBadgeText: { fontSize: 10, fontWeight: '700', color: '#fff' },
  
  jobStats: { flexDirection: 'row', justifyContent: 'space-around', marginVertical: 16 },
  statItem: { alignItems: 'center' },
  statValue: { fontSize: 24, fontWeight: '700', color: COLORS.text },
  statLabel: { fontSize: 10, color: COLORS.textMuted, marginTop: 4 },
  
  jobMeta: { fontSize: 11, color: COLORS.textMuted, marginTop: 4 },
  
  historyRow: { flexDirection: 'row', alignItems: 'center', paddingVertical: 8, borderBottomWidth: 1, borderBottomColor: COLORS.border },
  historyDot: { width: 8, height: 8, borderRadius: 4, marginRight: 12 },
  historyContent: { flex: 1 },
  historyDate: { fontSize: 12, color: COLORS.text },
  historyStats: { fontSize: 10, color: COLORS.textMuted },
  historyStatus: { fontSize: 10, fontWeight: '600' },
  
  footerNote: { flexDirection: 'row', alignItems: 'center', gap: 8, padding: 12 },
  footerNoteText: { fontSize: 11, color: COLORS.textMuted, fontStyle: 'italic', flex: 1 },
});
