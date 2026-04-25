import React, { useCallback, useEffect, useState } from 'react';
import {
  View,
  Text,
  StyleSheet,
  FlatList,
  TouchableOpacity,
  ActivityIndicator,
  Image,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useRouter, useLocalSearchParams } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import axios from 'axios';
import { API_URL } from '../../../utils/config';
import { useLayoutSpacing } from '../../../constants/layout';

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

type TopCompany = { ticker: string; logo: string | null };

function SectorLogo({ uri, ticker }: { uri: string | null; ticker: string }) {
  const [imgError, setImgError] = React.useState(false);
  if (!uri || imgError) {
    return (
      <View style={[styles.companyLogo, styles.companyLogoFallback]}>
        <Text style={styles.companyLogoInitial}>{ticker[0]}</Text>
      </View>
    );
  }
  return (
    <Image
      source={{ uri: uri.startsWith('http') ? uri : `${API_URL}${uri}` }}
      style={styles.companyLogo}
      onError={() => setImgError(true)}
    />
  );
}

type Industry = {
  name: string;
  total_market_cap: number;
  company_count: number;
  top3_companies: TopCompany[];
};

export default function SectorDetailScreen() {
  const router = useRouter();
  const sp = useLayoutSpacing();
  const { name } = useLocalSearchParams<{ name: string }>();

  const [industries, setIndustries] = useState<Industry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadIndustries = useCallback(async () => {
    if (!name) return;
    setLoading(true);
    setError(null);
    try {
      const encodedName = encodeURIComponent(name);
      const res = await axios.get(`${API_URL}/api/v1/browse/sectors/${encodedName}`);
      setIndustries(res.data.industries || []);
    } catch {
      setError('Could not load industries. Please try again.');
    } finally {
      setLoading(false);
    }
  }, [name]);

  useEffect(() => {
    loadIndustries();
  }, [loadIndustries]);

  const handleIndustryPress = (industryName: string) => {
    router.push({ pathname: '/browse/industry/[name]', params: { name: industryName } });
  };

  return (
    <SafeAreaView style={styles.container} edges={['top']}>
      <View style={styles.header}>
        <TouchableOpacity style={styles.backButton} onPress={() => router.back()}>
          <Ionicons name="arrow-back" size={24} color={COLORS.text} />
        </TouchableOpacity>
        <View style={styles.headerCenter}>
          <Text style={styles.headerTitle} numberOfLines={1}>{name || 'Sector'}</Text>
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
          <TouchableOpacity style={styles.retryButton} onPress={loadIndustries}>
            <Text style={styles.retryText}>Retry</Text>
          </TouchableOpacity>
        </View>
      ) : (
        <FlatList
          data={industries}
          keyExtractor={(item) => item.name}
          contentContainerStyle={[styles.list, { paddingHorizontal: sp.pageGutter }]}
          showsVerticalScrollIndicator={false}
          ListHeaderComponent={
            <View style={styles.listHeader}>
              <Text style={styles.sectorHeadline}>{name}</Text>
              <Text style={styles.subheading}>
                {industries.length} {industries.length === 1 ? 'industry' : 'industries'} · sorted by market cap
              </Text>
            </View>
          }
          renderItem={({ item }) => (
            <TouchableOpacity
              style={styles.card}
              onPress={() => handleIndustryPress(item.name)}
              activeOpacity={0.75}
            >
              <View style={styles.cardMain}>
                <Text style={styles.industryName}>{item.name}</Text>
                <Text style={styles.companyCount}>
                  {item.company_count} {item.company_count === 1 ? 'company' : 'companies'}
                </Text>
                <View style={styles.logosRow}>
                  {item.top3_companies.map((c) => (
                    <SectorLogo key={c.ticker} uri={c.logo} ticker={c.ticker} />
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
  listHeader: { paddingVertical: 14, gap: 4 },
  sectorHeadline: { fontSize: 22, fontWeight: '700', color: COLORS.text },
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
  industryName: { fontSize: 15, fontWeight: '700', color: COLORS.text },
  companyCount: { fontSize: 12, color: COLORS.textLight },
  logosRow: { flexDirection: 'row', gap: 6, marginTop: 4 },
  companyLogo: { width: 28, height: 28, borderRadius: 6, backgroundColor: '#F3F4F6' },
  companyLogoFallback: { alignItems: 'center', justifyContent: 'center', backgroundColor: '#EEF2FF' },
  companyLogoInitial: { fontSize: 11, fontWeight: '700', color: COLORS.primary },
  cardRight: { alignItems: 'flex-end', gap: 6 },
  marketCap: { fontSize: 13, fontWeight: '700', color: COLORS.text },
});
