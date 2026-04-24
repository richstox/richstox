import React, { useCallback, useState } from 'react';
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
import { useRouter, useFocusEffect } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import axios from 'axios';
import AppHeader from '../../components/AppHeader';
import { useAuth } from '../../contexts/AuthContext';
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
};

export default function TracklistPage() {
  const router = useRouter();
  const { sessionToken } = useAuth();
  const sp = useLayoutSpacing();
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [tracklistData, setTracklistData] = useState<any>(null);

  const fetchTracklist = useCallback(async () => {
    if (!sessionToken) return;
    try {
      const response = await axios.get(`${API_URL}/api/v1/tracklist`, {
        headers: { Authorization: `Bearer ${sessionToken}` },
      });
      setTracklistData(response.data);
    } catch (error) {
      console.error('Error fetching tracklist:', error);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [sessionToken]);

  useFocusEffect(
    useCallback(() => {
      fetchTracklist();
    }, [fetchTracklist])
  );

  const onRefresh = () => {
    setRefreshing(true);
    fetchTracklist();
  };

  if (loading) {
    return (
      <View style={styles.loadingContainer}>
        <ActivityIndicator size="large" color={COLORS.primary} />
      </View>
    );
  }

  const positions = tracklistData?.positions || [];
  const slotsRemaining = tracklistData?.slots_remaining || 0;

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
            <View>
              <Text style={styles.heroTitle}>Your Tracklist</Text>
              <Text style={styles.heroSubtitle}>Virtual 100,000 USD · equal-weight · changes apply at next close.</Text>
            </View>
            <View style={styles.countBadge}>
              <Text style={styles.countBadgeText}>{positions.length}/7</Text>
            </View>
          </View>
          <Text style={styles.heroHint}>
            Use Replace buttons below to swap stocks. Search is read-only unless opened from here in replace mode.
          </Text>
        </View>

        <View style={styles.sectionHeader}>
          <Text style={styles.sectionTitle}>Managed stocks</Text>
        </View>

        {positions.map((position: any) => (
          <View key={position.ticker} style={styles.positionCard}>
            <View>
              <Text style={styles.positionTicker}>{position.ticker}</Text>
              <Text style={styles.positionMeta}>Added {position.added_at || '—'}</Text>
            </View>
            <TouchableOpacity
              style={styles.replaceButton}
              onPress={() => router.push({ pathname: '/(tabs)/search', params: { mode: 'tracklist-replace', oldTicker: position.ticker } })}
            >
              <Text style={styles.replaceButtonText}>Replace</Text>
            </TouchableOpacity>
          </View>
        ))}

        {Array.from({ length: slotsRemaining }).map((_, index) => (
          <View key={`slot-${index}`} style={[styles.positionCard, styles.emptySlotCard]}>
            <View>
              <Text style={styles.positionTicker}>Empty slot</Text>
              <Text style={styles.positionMeta}>Add from a stock detail page to move toward 7 positions.</Text>
            </View>
          </View>
        ))}

        {!positions.length ? (
          <View style={styles.emptyState}>
            <Ionicons name="analytics-outline" size={40} color={COLORS.textMuted} />
            <Text style={styles.emptyTitle}>Tracklist is empty</Text>
            <Text style={styles.emptyText}>Pick stocks from the detail page using the new + Add to control.</Text>
          </View>
        ) : null}
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
  replaceButton: {
    paddingHorizontal: 14,
    paddingVertical: 10,
    borderRadius: 12,
    backgroundColor: COLORS.primary + '12',
  },
  replaceButtonText: {
    fontSize: 13,
    fontWeight: '700',
    color: COLORS.primary,
  },
  emptyState: {
    alignItems: 'center',
    paddingVertical: 36,
    paddingHorizontal: 18,
  },
  emptyTitle: {
    marginTop: 12,
    fontSize: 18,
    fontWeight: '700',
    color: COLORS.text,
  },
  emptyText: {
    marginTop: 6,
    fontSize: 14,
    lineHeight: 20,
    textAlign: 'center',
    color: COLORS.textLight,
  },
});
