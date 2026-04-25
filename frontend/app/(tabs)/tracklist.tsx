import React, { useCallback, useMemo, useState } from 'react';
import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  RefreshControl,
  ActivityIndicator,
  TouchableOpacity,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useFocusEffect, useLocalSearchParams, useRouter } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import axios from 'axios';
import AppHeader from '../../components/AppHeader';
import { useAuth } from '../../contexts/AuthContext';
import { useAppDialog } from '../../contexts/AppDialogContext';
import { useLayoutSpacing } from '../../constants/layout';
import { API_URL } from '../../utils/config';

const COLORS = {
  primary: '#1E3A5F',
  accent: '#10B981',
  text: '#1F2937',
  textLight: '#6B7280',
  textMuted: '#9CA3AF',
  background: '#F5F7FA',
  card: '#FFFFFF',
  border: '#E5E7EB',
  warning: '#F59E0B',
};

export default function TracklistPage() {
  const router = useRouter();
  const params = useLocalSearchParams();
  const { sessionToken } = useAuth();
  const dialog = useAppDialog();
  const sp = useLayoutSpacing();
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [tracklistData, setTracklistData] = useState<any>(null);

  const authHeaders = useMemo(
    () => (sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {}),
    [sessionToken]
  );

  const fetchTracklist = useCallback(async () => {
    if (!sessionToken) return;
    try {
      const response = await axios.get(`${API_URL}/api/v1/tracklist`, {
        headers: authHeaders,
      });
      setTracklistData(response.data);
    } catch (error) {
      console.error('Error fetching tracklist:', error);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [authHeaders, sessionToken]);

  useFocusEffect(
    useCallback(() => {
      fetchTracklist();
    }, [fetchTracklist])
  );

  const onRefresh = () => {
    setRefreshing(true);
    fetchTracklist();
  };

  const positions = tracklistData?.positions || [];
  const candidate = typeof params.candidate === 'string' ? params.candidate.toUpperCase() : '';
  const manageMode = params.manage === '1' || Boolean(candidate);
  const seededAt = tracklistData?.seeded_at;
  const seedTickers = tracklistData?.seed_tickers || [];
  const chart = tracklistData?.performance?.chart;
  const candidateAlreadyUsed = candidate ? positions.some((position: any) => position.ticker === candidate) : false;

  const openReplaceOverview = () => {
    router.push({ pathname: '/(tabs)/tracklist', params: { manage: '1' } });
  };

  const handleReplaceCandidate = async (oldTicker: string) => {
    if (!candidate || candidateAlreadyUsed || submitting || oldTicker === candidate) return;
    setSubmitting(true);
    try {
      await axios.post(
        `${API_URL}/api/v1/tracklist/replace`,
        { old_ticker: oldTicker, new_ticker: candidate },
        { headers: authHeaders }
      );
      await fetchTracklist();
      router.replace('/(tabs)/tracklist');
    } catch (error: any) {
      console.error('Error replacing tracklist ticker:', error);
      dialog.alert('Replace failed', error?.response?.data?.detail || 'Please try again.');
    } finally {
      setSubmitting(false);
    }
  };

  if (loading) {
    return (
      <View style={styles.loadingContainer}>
        <ActivityIndicator size="large" color={COLORS.primary} />
      </View>
    );
  }

  return (
    <SafeAreaView style={styles.container} edges={['top']}>
      <AppHeader title="Tracklist" showSubscriptionBadge={false} />
      <ScrollView
        style={styles.scrollView}
        contentContainerStyle={[styles.scrollContent, { padding: sp.pageGutter }]}
        refreshControl={<RefreshControl refreshing={refreshing} onRefresh={onRefresh} />}
        showsVerticalScrollIndicator={false}
      >
        <View style={styles.heroCard}>
          <View style={styles.heroTop}>
            <View style={styles.heroTextWrap}>
              <Text style={styles.heroTitle}>Your Tracklist</Text>
              <Text style={styles.heroSubtitle}>
                Every customer gets the same Magnificent 7 basket automatically from the date of the first login. You can still replace names from this overview.
              </Text>
            </View>
            <View style={styles.heroActions}>
              <TouchableOpacity style={styles.settingsButton} onPress={openReplaceOverview}>
                <Ionicons name="settings-outline" size={18} color={COLORS.primary} />
              </TouchableOpacity>
              <View style={styles.countBadge}>
                <Text style={styles.countBadgeText}>{positions.length}/7</Text>
              </View>
            </View>
          </View>
          <Text style={styles.heroHint}>{tracklistData?.changes_apply_note}</Text>
          {seededAt ? (
            <Text style={styles.seededMeta}>Started on {String(seededAt).slice(0, 10)}</Text>
          ) : null}
        </View>

        {manageMode ? (
          <View style={styles.summaryCard}>
            <View style={styles.summaryHeader}>
              <Ionicons name="swap-horizontal-outline" size={18} color={COLORS.primary} />
              <Text style={styles.summaryTitle}>Replace flow</Text>
            </View>
            <Text style={styles.summaryText}>
              {candidate
                ? candidateAlreadyUsed
                  ? `${candidate} is already in your Tracklist. Pick a different ticker from Stock detail.`
                  : `Selected ticker: ${candidate}. Choose which current Tracklist position should be replaced.`
                : 'Open any Stock detail and tap + Add to → Tracklist. The selected ticker will land here and you can replace one current position.'}
            </Text>
          </View>
        ) : null}

        <View style={styles.summaryCard}>
          <View style={styles.summaryHeader}>
            <Ionicons name="calendar-outline" size={18} color={COLORS.primary} />
            <Text style={styles.summaryTitle}>Auto-assigned basket</Text>
          </View>
          <Text style={styles.summaryText}>
            Seed: {seedTickers.join(', ')}
          </Text>
          {chart ? (
            <Text style={styles.summaryText}>
              Performance window: {chart.start_date} → {chart.end_date}
            </Text>
          ) : null}
        </View>

        <View style={styles.sectionHeader}>
          <Text style={styles.sectionTitle}>Tracked stocks</Text>
        </View>

        {positions.map((position: any, index: number) => (
          <View key={position.ticker} style={styles.positionCard}>
            <View style={styles.positionBody}>
              <Text style={styles.positionTicker}>{position.ticker}</Text>
              <Text style={styles.positionMeta}>Started {position.entry_date || position.added_at || '—'}</Text>
            </View>
            <View style={styles.positionRight}>
              <Text style={styles.positionStep}>#{index + 1}</Text>
              {manageMode ? (
                <TouchableOpacity
                  style={[
                    styles.replaceButton,
                    (!candidate || candidateAlreadyUsed || candidate === position.ticker || submitting) && styles.replaceButtonDisabled,
                  ]}
                  onPress={() => handleReplaceCandidate(position.ticker)}
                  disabled={!candidate || candidateAlreadyUsed || candidate === position.ticker || submitting}
                >
                  <Text
                    style={[
                      styles.replaceButtonText,
                      (!candidate || candidateAlreadyUsed || candidate === position.ticker || submitting) && styles.replaceButtonTextDisabled,
                    ]}
                  >
                    {!candidate
                      ? 'Pick ticker'
                      : candidate === position.ticker
                        ? 'Current'
                        : candidateAlreadyUsed
                          ? 'Used'
                          : submitting
                            ? 'Saving…'
                            : 'Replace'}
                  </Text>
                </TouchableOpacity>
              ) : null}
            </View>
          </View>
        ))}
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: COLORS.background,
  },
  scrollView: {
    flex: 1,
  },
  scrollContent: {
    paddingBottom: 32,
  },
  loadingContainer: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: COLORS.background,
  },
  heroCard: {
    backgroundColor: COLORS.card,
    borderRadius: 18,
    padding: 18,
    marginBottom: 16,
  },
  heroTop: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    gap: 12,
  },
  heroActions: {
    alignItems: 'flex-end',
    gap: 10,
  },
  heroTextWrap: {
    flex: 1,
  },
  heroTitle: {
    fontSize: 22,
    fontWeight: '700',
    color: COLORS.text,
  },
  heroSubtitle: {
    marginTop: 4,
    fontSize: 13,
    lineHeight: 19,
    color: COLORS.textLight,
  },
  heroHint: {
    marginTop: 12,
    fontSize: 13,
    color: COLORS.primary,
  },
  countBadge: {
    alignSelf: 'flex-start',
    backgroundColor: '#EEF2FF',
    borderRadius: 999,
    paddingHorizontal: 12,
    paddingVertical: 6,
  },
  countBadgeText: {
    fontSize: 13,
    fontWeight: '700',
    color: COLORS.primary,
  },
  settingsButton: {
    width: 40,
    height: 40,
    borderRadius: 12,
    backgroundColor: '#EEF2FF',
    alignItems: 'center',
    justifyContent: 'center',
  },
  summaryCard: {
    backgroundColor: '#EEF6FF',
    borderRadius: 18,
    padding: 18,
    marginBottom: 16,
    borderWidth: 1,
    borderColor: '#BFDBFE',
  },
  summaryHeader: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  summaryTitle: {
    fontSize: 13,
    fontWeight: '700',
    color: COLORS.primary,
    textTransform: 'uppercase',
    letterSpacing: 0.4,
  },
  summaryText: {
    marginTop: 10,
    fontSize: 14,
    lineHeight: 20,
    color: COLORS.textLight,
  },
  sectionHeader: {
    marginBottom: 10,
  },
  sectionTitle: {
    fontSize: 14,
    fontWeight: '700',
    letterSpacing: 0.5,
    textTransform: 'uppercase',
    color: COLORS.text,
  },
  positionCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 16,
    borderWidth: 1,
    borderColor: COLORS.border,
    marginBottom: 12,
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    gap: 12,
  },
  positionBody: {
    flex: 1,
  },
  positionRight: {
    alignItems: 'flex-end',
    gap: 8,
  },
  emptySlotCard: {
    borderStyle: 'dashed',
  },
  positionTicker: {
    fontSize: 18,
    fontWeight: '700',
    color: COLORS.text,
  },
  positionMeta: {
    marginTop: 4,
    fontSize: 13,
    color: COLORS.textLight,
    maxWidth: 220,
  },
  positionStep: {
    fontSize: 13,
    fontWeight: '700',
    color: COLORS.warning,
  },
  replaceButton: {
    minWidth: 92,
    paddingHorizontal: 14,
    paddingVertical: 10,
    borderRadius: 12,
    backgroundColor: COLORS.primary,
    alignItems: 'center',
  },
  replaceButtonDisabled: {
    backgroundColor: '#E5E7EB',
  },
  replaceButtonText: {
    fontSize: 13,
    fontWeight: '700',
    color: '#FFFFFF',
  },
  replaceButtonTextDisabled: {
    color: COLORS.textMuted,
  },
  seededMeta: {
    marginTop: 6,
    fontSize: 12,
    color: COLORS.textMuted,
  },
});
