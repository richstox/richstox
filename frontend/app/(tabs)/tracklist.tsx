import React, { useCallback, useMemo, useState } from 'react';
import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  RefreshControl,
  TouchableOpacity,
  ActivityIndicator,
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

  const candidate = useMemo(
    () => (typeof params.candidate === 'string' ? params.candidate.toUpperCase() : ''),
    [params.candidate]
  );

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
      dialog.alert('Tracklist unavailable', 'Please try again.');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [authHeaders, dialog, sessionToken]);

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
  const draftTickers = tracklistData?.draft_tickers || [];
  const ready = Boolean(tracklistData?.ready);
  const reservedTickers = new Set<string>([...draftTickers, ...positions.map((position: any) => position.ticker)]);
  const candidateAlreadyUsed = candidate ? reservedTickers.has(candidate) : false;

  const handleAddCandidateToSetup = async () => {
    if (!candidate || candidateAlreadyUsed || submitting) return;
    setSubmitting(true);
    try {
      await axios.post(`${API_URL}/api/v1/tracklist/draft/${candidate}`, {}, {
        headers: authHeaders,
      });
      await fetchTracklist();
      router.replace('/(tabs)/tracklist');
    } catch (error: any) {
      console.error('Error queueing tracklist ticker:', error);
      dialog.alert('Tracklist setup failed', error?.response?.data?.detail || 'Please try again.');
    } finally {
      setSubmitting(false);
    }
  };

  const handleReplaceCandidate = async (oldTicker: string) => {
    if (!candidate || submitting) return;
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
              <Text style={styles.heroTitle}>{ready ? 'Your Tracklist' : 'Tracklist setup'}</Text>
              <Text style={styles.heroSubtitle}>
                {ready
                  ? 'Exactly 7 stocks, equal-weight, and every replacement applies at the next close.'
                  : 'Tracklist turns on only after you lock exactly 7 stocks. Add them from the Last close card on ticker detail pages.'}
              </Text>
            </View>
            <View style={styles.countBadge}>
              <Text style={styles.countBadgeText}>{ready ? `${positions.length}/7` : `${draftTickers.length}/7`}</Text>
            </View>
          </View>
          <Text style={styles.heroHint}>{tracklistData?.changes_apply_note}</Text>
        </View>

        {candidate ? (
          <View style={styles.candidateCard}>
            <View style={styles.candidateHeader}>
              <Ionicons name="sparkles-outline" size={18} color={COLORS.primary} />
              <Text style={styles.candidateTitle}>Selected from ticker detail</Text>
            </View>
            <Text style={styles.candidateTicker}>{candidate}</Text>
            <Text style={styles.candidateText}>
              {candidateAlreadyUsed
                ? `${candidate} is already reserved in your Tracklist flow.`
                : ready
                  ? `Choose which current Tracklist stock should be replaced by ${candidate}.`
                  : `Add ${candidate} into your 7-stock Tracklist setup.`}
            </Text>
            {!ready && !candidateAlreadyUsed ? (
              <TouchableOpacity
                style={[styles.primaryButton, submitting && styles.primaryButtonDisabled]}
                onPress={handleAddCandidateToSetup}
                disabled={submitting}
              >
                <Text style={styles.primaryButtonText}>
                  {submitting ? 'Saving…' : `Add ${candidate} to Tracklist`}
                </Text>
              </TouchableOpacity>
            ) : null}
          </View>
        ) : null}

        {!ready ? (
          <>
            <View style={styles.sectionHeader}>
              <Text style={styles.sectionTitle}>Setup queue</Text>
            </View>

            {draftTickers.map((ticker: string, index: number) => (
              <View key={ticker} style={styles.positionCard}>
                <View>
                  <Text style={styles.positionTicker}>{ticker}</Text>
                  <Text style={styles.positionMeta}>Queued for the next Tracklist close snapshot</Text>
                </View>
                <Text style={styles.positionStep}>#{index + 1}</Text>
              </View>
            ))}

            {Array.from({ length: Math.max(0, 7 - draftTickers.length) }).map((_, index) => (
              <View key={`slot-${index}`} style={[styles.positionCard, styles.emptySlotCard]}>
                <View>
                  <Text style={styles.positionTicker}>Empty slot</Text>
                  <Text style={styles.positionMeta}>Open a ticker detail page and use + Add to → Tracklist.</Text>
                </View>
              </View>
            ))}
          </>
        ) : (
          <>
            <View style={styles.sectionHeader}>
              <Text style={styles.sectionTitle}>Managed stocks</Text>
            </View>

            {positions.map((position: any) => {
              const replaceDisabled = !candidate || candidate === position.ticker || candidateAlreadyUsed || submitting;
              return (
                <View key={position.ticker} style={styles.positionCard}>
                  <View>
                    <Text style={styles.positionTicker}>{position.ticker}</Text>
                    <Text style={styles.positionMeta}>Added {position.added_at || '—'}</Text>
                  </View>
                  {candidate ? (
                    <TouchableOpacity
                      style={[styles.replaceButton, replaceDisabled && styles.replaceButtonDisabled]}
                      onPress={() => handleReplaceCandidate(position.ticker)}
                      disabled={replaceDisabled}
                    >
                      <Text style={[styles.replaceButtonText, replaceDisabled && styles.replaceButtonTextDisabled]}>
                        {candidate === position.ticker
                          ? 'Current'
                          : candidateAlreadyUsed
                            ? 'Unavailable'
                            : submitting
                              ? 'Saving…'
                              : `Replace`}
                      </Text>
                    </TouchableOpacity>
                  ) : (
                    <TouchableOpacity
                      style={styles.replaceButton}
                      onPress={() => router.push({ pathname: '/(tabs)/search', params: { mode: 'tracklist-replace', oldTicker: position.ticker } })}
                    >
                      <Text style={styles.replaceButtonText}>Choose replacement</Text>
                    </TouchableOpacity>
                  )}
                </View>
              );
            })}
          </>
        )}
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
  candidateCard: {
    backgroundColor: '#EEF6FF',
    borderRadius: 18,
    padding: 18,
    marginBottom: 16,
    borderWidth: 1,
    borderColor: '#BFDBFE',
  },
  candidateHeader: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  candidateTitle: {
    fontSize: 13,
    fontWeight: '700',
    color: COLORS.primary,
    textTransform: 'uppercase',
    letterSpacing: 0.4,
  },
  candidateTicker: {
    marginTop: 10,
    fontSize: 26,
    fontWeight: '800',
    color: COLORS.text,
  },
  candidateText: {
    marginTop: 6,
    fontSize: 14,
    lineHeight: 20,
    color: COLORS.textLight,
  },
  primaryButton: {
    marginTop: 14,
    alignSelf: 'flex-start',
    backgroundColor: COLORS.primary,
    borderRadius: 12,
    paddingHorizontal: 16,
    paddingVertical: 12,
  },
  primaryButtonDisabled: {
    opacity: 0.6,
  },
  primaryButtonText: {
    color: '#FFFFFF',
    fontSize: 14,
    fontWeight: '700',
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
    paddingHorizontal: 14,
    paddingVertical: 10,
    borderRadius: 12,
    backgroundColor: COLORS.primary + '12',
  },
  replaceButtonDisabled: {
    backgroundColor: '#F3F4F6',
  },
  replaceButtonText: {
    fontSize: 13,
    fontWeight: '700',
    color: COLORS.primary,
  },
  replaceButtonTextDisabled: {
    color: COLORS.textMuted,
  },
});
