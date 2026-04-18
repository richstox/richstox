/**
 * RICHSTOX Admin — Price History Remediation
 * ===========================================
 * 3 sections:
 *   A) Malformed docs — scan, preview purge, execute purge
 *   B) Force redownload — single ticker reflag
 *   C) Batch short-history — preview + execute reflag
 */

import React, { useState, useCallback } from 'react';
import {
  View, Text, ScrollView, TouchableOpacity, StyleSheet,
  TextInput, ActivityIndicator, Modal,
} from 'react-native';
import { Ionicons } from '@expo/vector-icons';
import { COLORS } from '../_layout';
import { useAppDialog } from '../../contexts/AppDialogContext';
import { authenticatedFetch } from '../../utils/api_client';
import { API_URL } from '../../utils/config';

// ─── Props ────────────────────────────────────────────────────────────────────

interface RemediationProps {
  sessionToken: string | null;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/** Parse an ISO timestamp, treating naive (no-offset) strings as UTC. */
function parseUtcIso(iso?: string | null): number {
  if (!iso) return NaN;
  let s: string = iso;
  if (!s.endsWith('Z') && !/[+-]\d{2}:\d{2}$/.test(s) && !/[+-]\d{4}$/.test(s)) {
    s += 'Z';
  }
  return Date.parse(s);
}

function formatPrague(iso?: string | null): string {
  if (!iso) return '—';
  try {
    const ms = parseUtcIso(iso);
    if (isNaN(ms)) return '—';
    return new Date(ms).toLocaleString('en-GB', {
      timeZone: 'Europe/Prague',
      day: 'numeric', month: 'short', year: 'numeric',
      hour: '2-digit', minute: '2-digit',
    }) + ' Prague';
  } catch { return '—'; }
}

/**
 * Normalize ticker input:
 * - trim whitespace, uppercase
 * - if no exchange suffix (contains '.'), append '.US'
 */
function normalizeTicker(raw: string): string {
  const t = raw.trim().toUpperCase();
  if (!t) return '';
  if (t.includes('.')) return t;
  return `${t}.US`;
}

// ─── Section card wrapper ─────────────────────────────────────────────────────

function SectionCard({ title, icon, children }: { title: string; icon: string; children: React.ReactNode }) {
  return (
    <View style={r.card}>
      <View style={r.cardHeader}>
        <Ionicons name={icon as any} size={16} color={COLORS.primary} />
        <Text style={r.cardTitle}>{title}</Text>
      </View>
      {children}
    </View>
  );
}

// ─── Action button ────────────────────────────────────────────────────────────

function ActionButton({
  label, icon, onPress, loading, disabled, destructive,
}: {
  label: string; icon: string; onPress: () => void;
  loading?: boolean; disabled?: boolean; destructive?: boolean;
}) {
  return (
    <TouchableOpacity
      style={[r.actionBtn, destructive && r.actionBtnDestructive, (disabled || loading) && r.actionBtnDisabled]}
      onPress={onPress}
      disabled={disabled || loading}
      activeOpacity={0.7}
    >
      {loading ? (
        <ActivityIndicator size="small" color="#fff" />
      ) : (
        <Ionicons name={icon as any} size={14} color="#fff" style={{ marginRight: 6 }} />
      )}
      <Text style={r.actionBtnText}>{label}</Text>
    </TouchableOpacity>
  );
}

// ─── Result display ───────────────────────────────────────────────────────────

function ResultBox({ children }: { children: React.ReactNode }) {
  return <View style={r.resultBox}>{children}</View>;
}

function KV({ label, value }: { label: string; value: string | number | undefined | null }) {
  return (
    <View style={r.kvRow}>
      <Text style={r.kvLabel}>{label}</Text>
      <Text style={r.kvValue}>{value ?? '—'}</Text>
    </View>
  );
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN COMPONENT
// ═══════════════════════════════════════════════════════════════════════════════

export default function RemediationTab({ sessionToken }: RemediationProps) {
  const dialog = useAppDialog();

  // ── Section A: Malformed docs ───────────────────────────────────────────
  const [malformedLoading, setMalformedLoading] = useState(false);
  const [malformedResult, setMalformedResult] = useState<any>(null);
  const [purgePreviewLoading, setPurgePreviewLoading] = useState(false);
  const [purgePreviewResult, setPurgePreviewResult] = useState<any>(null);
  const [purgeExecLoading, setPurgeExecLoading] = useState(false);
  const [purgeExecResult, setPurgeExecResult] = useState<any>(null);

  // ── Section B: Force redownload ─────────────────────────────────────────
  const [tickerInput, setTickerInput] = useState('');
  const [reasonInput, setReasonInput] = useState('');
  const [forceLoading, setForceLoading] = useState(false);
  const [forceResult, setForceResult] = useState<any>(null);

  // ── Section C: Batch short history ──────────────────────────────────────
  const [minDays, setMinDays] = useState('30');
  const [maxTickers, setMaxTickers] = useState('500');
  const [shortPreviewLoading, setShortPreviewLoading] = useState(false);
  const [shortPreviewResult, setShortPreviewResult] = useState<any>(null);
  const [shortExecLoading, setShortExecLoading] = useState(false);
  const [shortExecResult, setShortExecResult] = useState<any>(null);

  // ── Confirmation modal ──────────────────────────────────────────────────
  const [confirmModal, setConfirmModal] = useState<{
    visible: boolean;
    keyword: string;        // e.g. "DELETE" or "REFLAG"
    title: string;
    message: string;
    onConfirm: () => void;
  }>({ visible: false, keyword: '', title: '', message: '', onConfirm: () => {} });
  const [confirmInput, setConfirmInput] = useState('');

  // ── API helper ──────────────────────────────────────────────────────────
  const apiFetch = useCallback(async (
    path: string,
    opts: RequestInit = {},
  ): Promise<any> => {
    const res = await authenticatedFetch(`${API_URL}${path}`, {
      ...opts,
      headers: { 'Content-Type': 'application/json', ...opts.headers },
    }, sessionToken);
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      const detail = body?.detail;
      const msg = typeof detail === 'object' ? detail?.message : detail || body?.message || res.statusText;
      throw new Error(msg || `HTTP ${res.status}`);
    }
    return res.json();
  }, [sessionToken]);

  // ── Section A handlers ──────────────────────────────────────────────────

  const handleScanMalformed = async () => {
    setMalformedLoading(true);
    setMalformedResult(null);
    try {
      const data = await apiFetch('/api/admin/prices/malformed');
      setMalformedResult(data);
    } catch (e: any) {
      dialog.alert('Scan Failed', e?.message || 'Could not scan malformed docs');
    } finally {
      setMalformedLoading(false);
    }
  };

  const handlePurgePreview = async () => {
    setPurgePreviewLoading(true);
    setPurgePreviewResult(null);
    try {
      const data = await apiFetch('/api/admin/prices/purge-malformed?dry_run=true', { method: 'POST' });
      setPurgePreviewResult(data);
    } catch (e: any) {
      dialog.alert('Preview Failed', e?.message || 'Could not preview purge');
    } finally {
      setPurgePreviewLoading(false);
    }
  };

  const handlePurgeExecute = () => {
    setConfirmModal({
      visible: true,
      keyword: 'DELETE',
      title: 'Confirm Purge Malformed',
      message: 'This will permanently delete malformed price docs and reflag affected tickers.\n\nType DELETE to confirm.',
      onConfirm: async () => {
        setPurgeExecLoading(true);
        setPurgeExecResult(null);
        try {
          const data = await apiFetch('/api/admin/prices/purge-malformed?dry_run=false', { method: 'POST' });
          setPurgeExecResult(data);
          dialog.alert('Purge Complete', `Deleted ${data.deleted_count} docs, reflagged ${data.reflagged_count} tickers.`);
        } catch (e: any) {
          dialog.alert('Purge Failed', e?.message || 'Could not execute purge');
        } finally {
          setPurgeExecLoading(false);
        }
      },
    });
    setConfirmInput('');
  };

  // ── Section B handlers ──────────────────────────────────────────────────

  const handleForceRedownload = async () => {
    const normalized = normalizeTicker(tickerInput);
    if (!normalized) {
      dialog.alert('Missing ticker', 'Please enter a ticker symbol.');
      return;
    }
    const reason = reasonInput.trim() || 'admin_manual';
    setForceLoading(true);
    setForceResult(null);
    try {
      const data = await apiFetch('/api/admin/prices/force-redownload', {
        method: 'POST',
        body: JSON.stringify({ tickers: [normalized], reason }),
      });
      setForceResult(data);
      const reflagged = data.tickers_reflagged ?? 0;
      const skipped = data.tickers_skipped ?? 0;
      if (reflagged > 0) {
        dialog.alert('Reflagged', `${normalized} flagged for full history redownload.`);
      } else if (skipped > 0) {
        dialog.alert('Skipped', `${normalized} not found in tracked tickers.`);
      }
    } catch (e: any) {
      dialog.alert('Redownload Failed', e?.message || 'Could not flag ticker');
    } finally {
      setForceLoading(false);
    }
  };

  // ── Section C handlers ──────────────────────────────────────────────────

  const handleShortPreview = async () => {
    setShortPreviewLoading(true);
    setShortPreviewResult(null);
    try {
      const data = await apiFetch('/api/admin/prices/reflag-short-history?dry_run=true', {
        method: 'POST',
        body: JSON.stringify({
          min_trading_days: parseInt(minDays, 10) || 30,
          max_tickers: parseInt(maxTickers, 10) || 500,
        }),
      });
      setShortPreviewResult(data);
    } catch (e: any) {
      dialog.alert('Preview Failed', e?.message || 'Could not preview short-history candidates');
    } finally {
      setShortPreviewLoading(false);
    }
  };

  const handleShortExecute = () => {
    setConfirmModal({
      visible: true,
      keyword: 'REFLAG',
      title: 'Confirm Batch Reflag',
      message: 'This will reflag short-history tickers for full redownload.\n\nType REFLAG to confirm.',
      onConfirm: async () => {
        setShortExecLoading(true);
        setShortExecResult(null);
        try {
          const data = await apiFetch('/api/admin/prices/reflag-short-history?dry_run=false', {
            method: 'POST',
            body: JSON.stringify({
              min_trading_days: parseInt(minDays, 10) || 30,
              max_tickers: parseInt(maxTickers, 10) || 500,
            }),
          });
          setShortExecResult(data);
          dialog.alert('Reflag Complete', `Updated ${data.updated_count ?? 0}, already flagged ${data.already_flagged_count ?? 0}.`);
        } catch (e: any) {
          dialog.alert('Reflag Failed', e?.message || 'Could not execute batch reflag');
        } finally {
          setShortExecLoading(false);
        }
      },
    });
    setConfirmInput('');
  };

  // ═════════════════════════════════════════════════════════════════════════
  // RENDER
  // ═════════════════════════════════════════════════════════════════════════

  return (
    <View style={r.container}>
      <ScrollView contentContainerStyle={r.scrollContent}>
        {/* Banner */}
        <View style={r.banner}>
          <Ionicons name="construct-outline" size={18} color={COLORS.primary} />
          <Text style={r.bannerText}>Price History Remediation</Text>
        </View>

        {/* ── Section A: Malformed docs ──────────────────────────────── */}
        <SectionCard title="Malformed Price Docs" icon="bug-outline">
          <Text style={r.hint}>Detect stock_prices documents missing date or close fields.</Text>

          <ActionButton
            label="Scan malformed price docs"
            icon="search-outline"
            onPress={handleScanMalformed}
            loading={malformedLoading}
          />

          {malformedResult && (
            <ResultBox>
              <KV label="Affected tickers" value={malformedResult.totals?.affected_tickers} />
              <KV label="Malformed docs" value={malformedResult.totals?.malformed_docs} />
              <KV label="Visible affected" value={malformedResult.totals?.visible_affected} />
              {malformedResult.items?.length > 0 && (
                <>
                  <Text style={r.subHeading}>Top tickers:</Text>
                  {malformedResult.items.slice(0, 10).map((it: any) => (
                    <Text key={it.ticker} style={r.listItem}>
                      {it.ticker} — {it.malformed_count} docs {it.is_visible ? '(visible)' : ''}
                    </Text>
                  ))}
                </>
              )}
            </ResultBox>
          )}

          <View style={r.divider} />

          <ActionButton
            label="Preview purge malformed"
            icon="eye-outline"
            onPress={handlePurgePreview}
            loading={purgePreviewLoading}
          />

          {purgePreviewResult && (
            <ResultBox>
              <KV label="Affected tickers" value={purgePreviewResult.totals?.affected_tickers} />
              <KV label="Malformed docs" value={purgePreviewResult.totals?.malformed_docs} />
              <KV label="Visible affected" value={purgePreviewResult.totals?.visible_affected} />
              <Text style={r.previewLabel}>Dry run — no changes made</Text>
            </ResultBox>
          )}

          <View style={r.divider} />

          <ActionButton
            label="Purge malformed (execute)"
            icon="trash-outline"
            onPress={handlePurgeExecute}
            loading={purgeExecLoading}
            destructive
          />

          {purgeExecResult && (
            <ResultBox>
              <KV label="Deleted docs" value={purgeExecResult.deleted_count} />
              <KV label="Reflagged tickers" value={purgeExecResult.reflagged_count} />
              <Text style={r.timestampText}>{formatPrague(new Date().toISOString())}</Text>
            </ResultBox>
          )}
        </SectionCard>

        {/* ── Section B: Force redownload ────────────────────────────── */}
        <SectionCard title="Force Redownload (Single Ticker)" icon="refresh-outline">
          <Text style={r.hint}>Flag a single ticker for full price history redownload.</Text>

          <Text style={r.inputLabel}>Ticker</Text>
          <TextInput
            style={r.input}
            placeholder="e.g. AAPL or AAPL.US"
            placeholderTextColor={COLORS.textMuted}
            value={tickerInput}
            onChangeText={setTickerInput}
            autoCapitalize="characters"
          />
          {tickerInput.trim() !== '' && (
            <Text style={r.normalizedHint}>
              Will submit as: <Text style={{ fontWeight: '700' }}>{normalizeTicker(tickerInput)}</Text>
            </Text>
          )}

          <Text style={r.inputLabel}>Reason (optional)</Text>
          <TextInput
            style={r.input}
            placeholder="admin_manual"
            placeholderTextColor={COLORS.textMuted}
            value={reasonInput}
            onChangeText={setReasonInput}
          />

          <ActionButton
            label="Flag ticker for full history redownload"
            icon="flag-outline"
            onPress={handleForceRedownload}
            loading={forceLoading}
            disabled={!tickerInput.trim()}
          />

          {forceResult && (
            <ResultBox>
              <KV label="Requested" value={forceResult.tickers_requested} />
              <KV label="Reflagged" value={forceResult.tickers_reflagged} />
              <KV label="Skipped" value={forceResult.tickers_skipped} />
              <KV label="Reason" value={forceResult.reason} />
              {forceResult.results?.map((r: any) => (
                <Text key={r.ticker} style={[
                  r.tickerStatus,
                  { color: r.status === 'reflagged' ? '#22C55E' : '#EF4444' },
                ]}>
                  {r.ticker}: {r.status === 'reflagged' ? '✅ reflagged' : `❌ ${r.reason || 'skipped'}`}
                </Text>
              ))}
            </ResultBox>
          )}

          <Text style={r.helperText}>
            After flagging, run Phase C / Full Pipeline.
          </Text>
        </SectionCard>

        {/* ── Section C: Batch short history ─────────────────────────── */}
        <SectionCard title="Batch Fix Short History" icon="analytics-outline">
          <Text style={r.hint}>Find and reflag tickers with too few trading days.</Text>

          <View style={r.inputRow}>
            <View style={r.inputHalf}>
              <Text style={r.inputLabel}>Min trading days</Text>
              <TextInput
                style={r.input}
                placeholder="30"
                placeholderTextColor={COLORS.textMuted}
                value={minDays}
                onChangeText={setMinDays}
                keyboardType="numeric"
              />
            </View>
            <View style={r.inputHalf}>
              <Text style={r.inputLabel}>Max tickers</Text>
              <TextInput
                style={r.input}
                placeholder="500"
                placeholderTextColor={COLORS.textMuted}
                value={maxTickers}
                onChangeText={setMaxTickers}
                keyboardType="numeric"
              />
            </View>
          </View>

          <ActionButton
            label="Preview short-history candidates"
            icon="eye-outline"
            onPress={handleShortPreview}
            loading={shortPreviewLoading}
          />

          {shortPreviewResult && (
            <ResultBox>
              <KV label="Candidates" value={shortPreviewResult.candidate_count} />
              {shortPreviewResult.sample?.length > 0 && (
                <>
                  <Text style={r.subHeading}>Sample tickers:</Text>
                  {shortPreviewResult.sample.slice(0, 10).map((t: any) => (
                    <Text key={typeof t === 'string' ? t : t.ticker} style={r.listItem}>
                      {typeof t === 'string' ? t : `${t.ticker} — ${t.trading_days ?? '?'} days`}
                    </Text>
                  ))}
                </>
              )}
              <Text style={r.previewLabel}>Dry run — no changes made</Text>
            </ResultBox>
          )}

          <View style={r.divider} />

          <ActionButton
            label="Execute batch reflag"
            icon="hammer-outline"
            onPress={handleShortExecute}
            loading={shortExecLoading}
            destructive
          />

          {shortExecResult && (
            <ResultBox>
              <KV label="Updated" value={shortExecResult.updated_count} />
              <KV label="Already flagged" value={shortExecResult.already_flagged_count} />
              <Text style={r.timestampText}>{formatPrague(new Date().toISOString())}</Text>
            </ResultBox>
          )}
        </SectionCard>
      </ScrollView>

      {/* ── Confirmation modal ───────────────────────────────────────── */}
      <Modal
        visible={confirmModal.visible}
        transparent
        animationType="fade"
        onRequestClose={() => setConfirmModal(prev => ({ ...prev, visible: false }))}
      >
        <View style={r.modalOverlay}>
          <View style={r.modalCard}>
            <Text style={r.modalTitle}>{confirmModal.title}</Text>
            <Text style={r.modalMessage}>{confirmModal.message}</Text>
            <TextInput
              style={r.modalInput}
              placeholder={`Type ${confirmModal.keyword}`}
              placeholderTextColor={COLORS.textMuted}
              value={confirmInput}
              onChangeText={setConfirmInput}
              autoCapitalize="characters"
              autoFocus
            />
            <View style={r.modalButtons}>
              <TouchableOpacity
                style={[r.modalBtn, r.modalBtnCancel]}
                onPress={() => setConfirmModal(prev => ({ ...prev, visible: false }))}
              >
                <Text style={r.modalBtnCancelText}>Cancel</Text>
              </TouchableOpacity>
              <TouchableOpacity
                style={[
                  r.modalBtn, r.modalBtnConfirm,
                  confirmInput.trim().toUpperCase() !== confirmModal.keyword && r.modalBtnConfirmDisabled,
                ]}
                onPress={() => {
                  if (confirmInput.trim().toUpperCase() === confirmModal.keyword) {
                    setConfirmModal(prev => ({ ...prev, visible: false }));
                    confirmModal.onConfirm();
                  }
                }}
                disabled={confirmInput.trim().toUpperCase() !== confirmModal.keyword}
              >
                <Text style={r.modalBtnConfirmText}>Confirm</Text>
              </TouchableOpacity>
            </View>
          </View>
        </View>
      </Modal>
    </View>
  );
}

// ─── Styles ───────────────────────────────────────────────────────────────────

const r = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.background },
  scrollContent: { padding: 12, paddingBottom: 40, gap: 14 },

  banner: {
    flexDirection: 'row', alignItems: 'center', gap: 8,
    paddingVertical: 10, paddingHorizontal: 12,
    backgroundColor: COLORS.card, borderRadius: 10,
    borderWidth: 1, borderColor: COLORS.border,
  },
  bannerText: { fontSize: 15, fontWeight: '700', color: COLORS.text },

  // Card
  card: {
    backgroundColor: COLORS.card, borderRadius: 10,
    borderWidth: 1, borderColor: COLORS.border,
    padding: 14,
  },
  cardHeader: { flexDirection: 'row', alignItems: 'center', gap: 8, marginBottom: 10 },
  cardTitle: { fontSize: 14, fontWeight: '700', color: COLORS.text },

  // Hints / labels
  hint: { fontSize: 12, color: COLORS.textLight, marginBottom: 10, lineHeight: 17 },
  inputLabel: { fontSize: 11, fontWeight: '600', color: COLORS.textLight, marginBottom: 4, marginTop: 6 },
  normalizedHint: { fontSize: 11, color: COLORS.primary, marginBottom: 6 },
  helperText: {
    fontSize: 11, color: COLORS.textMuted, marginTop: 10,
    fontStyle: 'italic', lineHeight: 16,
  },
  previewLabel: { fontSize: 11, color: COLORS.accent, fontStyle: 'italic', marginTop: 6 },
  timestampText: { fontSize: 10, color: COLORS.textMuted, marginTop: 6 },

  // Inputs
  input: {
    borderWidth: 1, borderColor: COLORS.border, borderRadius: 8,
    paddingHorizontal: 10, paddingVertical: 8,
    fontSize: 13, color: COLORS.text, backgroundColor: COLORS.background,
    marginBottom: 6,
  },
  inputRow: { flexDirection: 'row', gap: 10 },
  inputHalf: { flex: 1 },

  // Action buttons
  actionBtn: {
    flexDirection: 'row', alignItems: 'center', justifyContent: 'center',
    backgroundColor: COLORS.primary, borderRadius: 8,
    paddingVertical: 10, paddingHorizontal: 14,
    marginTop: 6,
  },
  actionBtnDestructive: { backgroundColor: '#EF4444' },
  actionBtnDisabled: { opacity: 0.5 },
  actionBtnText: { color: '#fff', fontSize: 13, fontWeight: '600' },

  divider: { height: 1, backgroundColor: COLORS.border, marginVertical: 10 },

  // Result box
  resultBox: {
    backgroundColor: COLORS.background, borderRadius: 8,
    padding: 10, marginTop: 8,
    borderWidth: 1, borderColor: COLORS.border,
  },
  kvRow: { flexDirection: 'row', justifyContent: 'space-between', paddingVertical: 2 },
  kvLabel: { fontSize: 12, color: COLORS.textLight },
  kvValue: { fontSize: 12, fontWeight: '600', color: COLORS.text },
  subHeading: { fontSize: 11, fontWeight: '600', color: COLORS.text, marginTop: 8, marginBottom: 2 },
  listItem: { fontSize: 11, color: COLORS.textLight, marginLeft: 8, lineHeight: 16 },
  tickerStatus: { fontSize: 12, marginTop: 2 },

  // Confirmation modal
  modalOverlay: {
    flex: 1, backgroundColor: 'rgba(0,0,0,0.45)',
    justifyContent: 'center', alignItems: 'center', padding: 24,
  },
  modalCard: {
    backgroundColor: '#fff', borderRadius: 16,
    padding: 24, width: '100%', maxWidth: 360,
  },
  modalTitle: { fontSize: 18, fontWeight: '700', color: '#111827', marginBottom: 8 },
  modalMessage: { fontSize: 14, color: '#4B5563', lineHeight: 20, marginBottom: 16 },
  modalInput: {
    borderWidth: 1, borderColor: '#D1D5DB', borderRadius: 10,
    paddingHorizontal: 14, paddingVertical: 10,
    fontSize: 15, color: '#111827', marginBottom: 16,
  },
  modalButtons: { flexDirection: 'row', gap: 10 },
  modalBtn: { flex: 1, paddingVertical: 12, borderRadius: 10, alignItems: 'center' },
  modalBtnCancel: { backgroundColor: '#F3F4F6' },
  modalBtnCancelText: { fontSize: 15, fontWeight: '600', color: '#4B5563' },
  modalBtnConfirm: { backgroundColor: '#EF4444' },
  modalBtnConfirmDisabled: { opacity: 0.4 },
  modalBtnConfirmText: { fontSize: 15, fontWeight: '600', color: '#fff' },
});
