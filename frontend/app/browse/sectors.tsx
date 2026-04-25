import React, { useCallback, useEffect, useState } from 'react';
import {
  View,
  Text,
  StyleSheet,
  FlatList,
  TouchableOpacity,
  ActivityIndicator,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useRouter } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import axios from 'axios';
import { API_URL } from '../../utils/config';
import { useLayoutSpacing } from '../../constants/layout';

const COLORS = {
  primary: '#4A6FA5',
  text: '#2D3436',
  textLight: '#636E72',
  textMuted: '#95A5A6',
  background: '#F8F6F3',
  card: '#FFFFFF',
  border: '#E8E4DF',
};

function formatMarketCap(n: number | null | undefined): string {
  if (!n || n <= 0) return '—';
  if (n >= 1e12) return `$${(n / 1e12).toFixed(1)}T`;
  if (n >= 1e9) return `$${(n / 1e9).toFixed(1)}B`;
  if (n >= 1e6) return `$${(n / 1e6).toFixed(1)}M`;
  return `$${n.toFixed(0)}`;
}

type Sector = {
  name: string;
  total_market_cap: number;
  industry_count: number;
  company_count: number;
  top3_tickers: string[];
};

export default function AllSectorsScreen() {
  const router = useRouter();
  const sp = useLayoutSpacing();
  const [sectors, setSectors] = useState<Sector[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadSectors = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await axios.get(`${API_URL}/api/v1/browse/sectors`);
      setSectors(res.data.sectors || []);
    } catch {
      setError('Could not load sectors. Please try again.');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadSectors();
  }, [loadSectors]);

  const handleSectorPress = (name: string) => {
    router.push({ pathname: '/browse/sector/[name]', params: { name } });
  };

  return (
    <SafeAreaView style={styles.container} edges={['top']}>
      <View style={styles.header}>
        <TouchableOpacity style={styles.backButton} onPress={() => router.back()}>
          <Ionicons name="arrow-back" size={24} color={COLORS.text} />
        </TouchableOpacity>
        <View style={styles.headerCenter}>
          <Text style={styles.headerTitle}>All Sectors</Text>
        </View>
        <View style={styles.headerPlaceholder} />
      </View>

      {loading ? (
        <View style={styles.center}>
          <ActivityIndicator size="large" color={COLORS.primary} />
        </View>
      ) : error ? (
        <View style={styles.center}>
          <Text style={styles.errorText}>{error}</Text>
          <TouchableOpacity style={styles.retryButton} onPress={loadSectors}>
            <Text style={styles.retryText}>Retry</Text>
          </TouchableOpacity>
        </View>
      ) : (
        <FlatList
          data={sectors}
          keyExtractor={(item) => item.name}
          contentContainerStyle={[styles.list, { paddingHorizontal: sp.pageGutter }]}
          showsVerticalScrollIndicator={false}
          ListHeaderComponent={
            <View style={styles.listHeader}>
              <Text style={styles.subheading}>
                {sectors.length} sectors · sorted by market cap
              </Text>
            </View>
          }
          renderItem={({ item }) => (
            <TouchableOpacity
              style={styles.card}
              onPress={() => handleSectorPress(item.name)}
              activeOpacity={0.75}
            >
              <View style={styles.cardMain}>
                <Text style={styles.sectorName}>{item.name}</Text>
                <View style={styles.meta}>
                  <Text style={styles.metaText}>
                    {item.industry_count} {item.industry_count === 1 ? 'industry' : 'industries'}
                  </Text>
                  <Text style={styles.metaDot}>·</Text>
                  <Text style={styles.metaText}>{item.company_count} companies</Text>
                </View>
                <View style={styles.pillsRow}>
                  {item.top3_tickers.map((ticker) => (
                    <View key={ticker} style={styles.tickerPill}>
                      <Text style={styles.tickerPillText}>{ticker}</Text>
                    </View>
                  ))}
                </View>
              </View>
              <View style={styles.cardRight}>
                <Text style={styles.marketCap}>{formatMarketCap(item.total_market_cap)}</Text>
                <Ionicons name="chevron-forward" size={18} color={COLORS.textMuted} />
              </View>
            </TouchableOpacity>
          )}
        />
      )}
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.background },
  header: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 8,
    paddingVertical: 12,
    backgroundColor: COLORS.card,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  backButton: { padding: 16, marginLeft: -8 },
  headerCenter: { flex: 1, alignItems: 'center' },
  headerTitle: { fontSize: 18, fontWeight: '700', color: COLORS.text },
  headerPlaceholder: { width: 48 },
  center: { flex: 1, alignItems: 'center', justifyContent: 'center', padding: 24 },
  errorText: { fontSize: 15, color: COLORS.textLight, textAlign: 'center', marginBottom: 16 },
  retryButton: { paddingHorizontal: 20, paddingVertical: 10, borderRadius: 8, backgroundColor: COLORS.primary },
  retryText: { color: '#fff', fontWeight: '700' },
  list: { paddingBottom: 32 },
  listHeader: { paddingVertical: 14 },
  subheading: { fontSize: 13, color: COLORS.textMuted, fontWeight: '600' },
  card: {
    backgroundColor: COLORS.card,
    borderRadius: 14,
    padding: 14,
    marginBottom: 10,
    flexDirection: 'row',
    alignItems: 'center',
    gap: 10,
  },
  cardMain: { flex: 1, gap: 4 },
  sectorName: { fontSize: 16, fontWeight: '700', color: COLORS.text },
  meta: { flexDirection: 'row', alignItems: 'center', gap: 6 },
  metaText: { fontSize: 12, color: COLORS.textLight },
  metaDot: { fontSize: 12, color: COLORS.textMuted },
  pillsRow: { flexDirection: 'row', gap: 6, marginTop: 4, flexWrap: 'wrap' },
  tickerPill: {
    paddingHorizontal: 8,
    paddingVertical: 3,
    borderRadius: 999,
    backgroundColor: '#EEF2FF',
  },
  tickerPillText: { fontSize: 11, fontWeight: '700', color: COLORS.primary },
  cardRight: { alignItems: 'flex-end', gap: 6 },
  marketCap: { fontSize: 13, fontWeight: '700', color: COLORS.text },
});
