import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  View,
  Text,
  StyleSheet,
  TextInput,
  TouchableOpacity,
  FlatList,
  Keyboard,
  Image,
  ScrollView,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useRouter, useFocusEffect } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import axios from 'axios';
import { useAuth } from '../../contexts/AuthContext';
import { useSearchStore } from '../../stores/searchStore';
import BrandedLoading from '../../components/BrandedLoading';
import { useLayoutSpacing } from '../../constants/layout';
import { API_URL } from '../../utils/config';

const COLORS = {
  primary: '#6366F1',
  accent: '#10B981',
  warning: '#F59E0B',
  danger: '#EF4444',
  text: '#1F2937',
  textLight: '#6B7280',
  textMuted: '#9CA3AF',
  background: '#F9FAFB',
  card: '#FFFFFF',
  border: '#E5E7EB',
};

const MEMBERSHIP_CONFIG: Record<string, { label: string; bg: string; text: string }> = {
  watchlist: { label: 'Watchlist', bg: '#FEF3C7', text: '#B45309' },
  tracklist: { label: 'Tracklist', bg: '#DBEAFE', text: '#1D4ED8' },
};

const getMembershipConfig = (membership: unknown) => {
  if (typeof membership !== 'string') return null;
  const normalized = membership.trim().toLowerCase();
  if (!normalized) return null;
  return MEMBERSHIP_CONFIG[normalized] ?? { label: normalized.charAt(0).toUpperCase() + normalized.slice(1), bg: '#F3F4F6', text: '#374151' };
};

// GICS-style sectors with icon and color
const BROWSE_SECTORS: Array<{
  name: string;
  icon: React.ComponentProps<typeof Ionicons>['name'];
  color: string;
}> = [
  { name: 'Technology',          icon: 'hardware-chip-outline',  color: '#6366F1' },
  { name: 'Healthcare',          icon: 'medkit-outline',         color: '#10B981' },
  { name: 'Financial Services',  icon: 'bar-chart-outline',      color: '#3B82F6' },
  { name: 'Industrials',         icon: 'build-outline',          color: '#F59E0B' },
  { name: 'Energy',              icon: 'flash-outline',          color: '#EF4444' },
  { name: 'Consumer Cyclical',   icon: 'cart-outline',           color: '#8B5CF6' },
  { name: 'Real Estate',         icon: 'home-outline',           color: '#EC4899' },
  { name: 'Utilities',           icon: 'water-outline',          color: '#06B6D4' },
];

export default function Search() {
  const router = useRouter();
  const inputRef = useRef<TextInput>(null);
  const { sessionToken } = useAuth();
  const { query: storedQuery, results: storedResults, setSearch } = useSearchStore();
  const sp = useLayoutSpacing();

  const [searchQuery, setSearchQuery] = useState(storedQuery);
  const [results, setResults] = useState<any[]>(storedResults);
  const [loading, setLoading] = useState(false);

  useFocusEffect(
    useCallback(() => {
      const timeout = setTimeout(() => inputRef.current?.focus(), 100);
      return () => clearTimeout(timeout);
    }, [])
  );

  const authHeaders = useMemo(
    () => (sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {}),
    [sessionToken]
  );

  const searchTickers = useCallback(async () => {
    if (searchQuery.length < 1) {
      setResults([]);
      return;
    }
    setLoading(true);
    try {
      const response = await axios.get(`${API_URL}/api/whitelist/search?q=${searchQuery}`, {
        headers: authHeaders,
      });
      const searchResults = response.data.results || [];
      setResults(searchResults);
      if (searchResults.length > 0) {
        setSearch(searchQuery, searchResults);
      }
    } catch (error) {
      console.error('Error searching:', error);
    } finally {
      setLoading(false);
    }
  }, [authHeaders, searchQuery, setSearch]);

  useEffect(() => {
    searchTickers();
  }, [searchTickers]);

  const handleTickerPress = async (item: any) => {
    Keyboard.dismiss();
    router.push(`/stock/${item.ticker}`);
  };

  const handleSectorPress = (sectorName: string) => {
    Keyboard.dismiss();
    router.push({ pathname: '/browse/sector/[name]', params: { name: sectorName } });
  };

  const handleViewAllSectors = () => {
    Keyboard.dismiss();
    router.push('/browse/sectors');
  };

  const renderMembershipPills = (item: any) => {
    const memberships = Array.isArray(item.memberships) ? item.memberships : [];
    if (!memberships.length) return null;
    return (
      <View style={styles.pillsRow}>
        {memberships.map((membership: string) => {
          const config = getMembershipConfig(membership);
          if (!config) return null;
          return (
            <View key={`${item.ticker}-${membership}`} style={[styles.membershipPill, { backgroundColor: config.bg }]}>
              <Text style={[styles.membershipPillText, { color: config.text }]}>{config.label}</Text>
            </View>
          );
        })}
      </View>
    );
  };

  return (
    <SafeAreaView style={styles.container} edges={['top']}>
      <View style={styles.header}>
        <TouchableOpacity style={styles.backButton} onPress={() => router.back()}>
          <Ionicons name="arrow-back" size={24} color={COLORS.text} />
        </TouchableOpacity>
        <View style={styles.headerCenter}>
          <Text style={styles.headerTitle}>Search Stocks</Text>
        </View>
        <View style={styles.headerPlaceholder} />
      </View>

      <View style={[styles.searchWrapper, { marginHorizontal: sp.pageGutter }]}>
        <Ionicons name="search" size={22} color={COLORS.textMuted} />
        <TextInput
          ref={inputRef}
          style={styles.searchInput}
          placeholder="Search ticker or company name..."
          placeholderTextColor={COLORS.textMuted}
          value={searchQuery}
          onChangeText={(text) => setSearchQuery(text.toUpperCase())}
          autoCapitalize="characters"
          autoCorrect={false}
          autoFocus
        />
        {searchQuery.length > 0 && (
          <TouchableOpacity onPress={() => setSearchQuery('')}>
            <Ionicons name="close-circle" size={22} color={COLORS.textMuted} />
          </TouchableOpacity>
        )}
      </View>

      {loading && results.length === 0 ? (
        <BrandedLoading message="Searching stocks..." />
      ) : searchQuery.length === 0 ? (
        <ScrollView
          style={styles.discoveryScroll}
          contentContainerStyle={[styles.discoveryContent, { paddingHorizontal: sp.pageGutter }]}
          keyboardShouldPersistTaps="handled"
          showsVerticalScrollIndicator={false}
        >
          {/* Headline */}
          <View style={styles.heroBlock}>
            <Text style={styles.heroTitle}>Find companies you understand</Text>
            <Text style={styles.heroSubtitle}>
              Verified fundamentals, dividends, and valuation — with clear risk context.
            </Text>
          </View>

          {/* Browse by sector */}
          <View style={styles.browseBlock}>
            <View style={styles.browseHeaderRow}>
              <Text style={styles.browseTitle}>Browse by sector</Text>
              <TouchableOpacity onPress={handleViewAllSectors} hitSlop={{ top: 8, bottom: 8, left: 8, right: 8 }}>
                <Text style={styles.viewAllLink}>View all sectors →</Text>
              </TouchableOpacity>
            </View>

            <View style={styles.sectorGrid}>
              {BROWSE_SECTORS.map((s) => (
                <TouchableOpacity
                  key={s.name}
                  style={styles.sectorChip}
                  onPress={() => handleSectorPress(s.name)}
                  activeOpacity={0.75}
                >
                  <Ionicons name={s.icon} size={16} color={s.color} />
                  <Text style={styles.sectorChipText}>{s.name}</Text>
                </TouchableOpacity>
              ))}
            </View>

            <Text style={styles.sectorHint}>Tap a sector to see the biggest companies.</Text>
          </View>

          {/* Footer tagline */}
          <View style={styles.taglineBlock}>
            <Text style={styles.tagline}>No picks. No hype. Just facts you can verify.</Text>
            <Text style={styles.taglineSub}>Start free — no signup required.</Text>
          </View>
        </ScrollView>
      ) : results.length === 0 ? (
        <View style={styles.center}>
          <Ionicons name="alert-circle-outline" size={64} color={COLORS.border} />
          <Text style={styles.emptyTitle}>No results</Text>
        </View>
      ) : (
        <FlatList
          data={results}
          keyExtractor={(item) => item.ticker}
          contentContainerStyle={[styles.list, { padding: sp.pageGutter, paddingTop: 0 }]}
          showsVerticalScrollIndicator={false}
          keyboardShouldPersistTaps="handled"
          ListHeaderComponent={
            <View style={styles.resultsHeader}>
              <Text style={styles.resultsCount}>{results.length} result{results.length !== 1 ? 's' : ''}</Text>
            </View>
          }
          renderItem={({ item }) => {
            return (
              <TouchableOpacity
                style={styles.item}
                onPress={() => handleTickerPress(item)}
              >
                {item.logo ? (
                  <Image
                    source={{ uri: item.logo.startsWith('http') ? item.logo : `${API_URL}${item.logo}` }}
                    style={styles.itemLogo}
                  />
                ) : (
                  <View style={styles.itemIcon}>
                    <Text style={styles.itemInitial}>{item.ticker[0]}</Text>
                  </View>
                )}
                <View style={styles.itemInfo}>
                  <View style={styles.itemTopRow}>
                    <Text style={styles.itemTicker}>{item.ticker}</Text>
                    {renderMembershipPills(item)}
                  </View>
                  <Text style={styles.itemName} numberOfLines={1}>{item.name}</Text>
                </View>
                <View style={styles.itemRight}>
                  <Text style={styles.itemExchange}>{item.exchange}</Text>
                  <Ionicons name="chevron-forward" size={20} color={COLORS.textMuted} />
                </View>
              </TouchableOpacity>
            );
          }}
        />
      )}
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: COLORS.background,
  },
  header: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 8,
    paddingVertical: 12,
    backgroundColor: COLORS.card,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  backButton: {
    padding: 16,
    marginLeft: -8,
  },
  headerCenter: {
    flex: 1,
    alignItems: 'center',
  },
  headerTitle: {
    fontSize: 18,
    fontWeight: '700',
    color: COLORS.text,
  },
  headerPlaceholder: {
    width: 48,
  },
  searchWrapper: {
    marginTop: 16,
    marginBottom: 16,
    backgroundColor: COLORS.card,
    borderRadius: 16,
    paddingHorizontal: 16,
    paddingVertical: 14,
    flexDirection: 'row',
    alignItems: 'center',
    gap: 12,
  },
  searchInput: {
    flex: 1,
    fontSize: 16,
    color: COLORS.text,
  },
  // ── Discovery / landing state ─────────────────────────────────────────────
  discoveryScroll: { flex: 1 },
  discoveryContent: { paddingBottom: 40 },
  heroBlock: { marginTop: 8, marginBottom: 24, gap: 8 },
  heroTitle: { fontSize: 22, fontWeight: '800', color: COLORS.text, lineHeight: 30 },
  heroSubtitle: { fontSize: 14, color: COLORS.textLight, lineHeight: 21 },
  browseBlock: { marginBottom: 28, gap: 14 },
  browseHeaderRow: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between' },
  browseTitle: { fontSize: 16, fontWeight: '700', color: COLORS.text },
  viewAllLink: { fontSize: 13, fontWeight: '600', color: COLORS.primary },
  sectorGrid: { flexDirection: 'row', flexWrap: 'wrap', gap: 8 },
  sectorChip: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
    paddingHorizontal: 12,
    paddingVertical: 8,
    borderRadius: 999,
    backgroundColor: COLORS.card,
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  sectorChipText: { fontSize: 13, fontWeight: '600', color: COLORS.text },
  sectorHint: { fontSize: 12, color: COLORS.textMuted },
  taglineBlock: { marginTop: 8, gap: 6, alignItems: 'center' },
  tagline: { fontSize: 14, fontWeight: '600', color: COLORS.text, textAlign: 'center' },
  taglineSub: { fontSize: 13, color: COLORS.textMuted, textAlign: 'center' },
  // ── Results ───────────────────────────────────────────────────────────────
  list: {
    paddingBottom: 32,
  },
  resultsHeader: {
    paddingBottom: 12,
  },
  resultsCount: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.textLight,
  },
  item: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    borderWidth: 1,
    borderColor: COLORS.border,
    padding: 14,
    marginBottom: 12,
    flexDirection: 'row',
    alignItems: 'center',
    gap: 12,
  },
  itemDisabled: {
    opacity: 0.55,
  },
  itemLogo: {
    width: 42,
    height: 42,
    borderRadius: 10,
    backgroundColor: '#F3F4F6',
  },
  itemIcon: {
    width: 42,
    height: 42,
    borderRadius: 10,
    backgroundColor: '#E0E7FF',
    alignItems: 'center',
    justifyContent: 'center',
  },
  itemInitial: {
    fontSize: 16,
    fontWeight: '700',
    color: COLORS.primary,
  },
  itemInfo: {
    flex: 1,
    minWidth: 0,
  },
  itemTopRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
    marginBottom: 2,
  },
  itemTicker: {
    fontSize: 16,
    fontWeight: '700',
    color: COLORS.text,
  },
  itemName: {
    fontSize: 13,
    color: COLORS.textLight,
  },
  disabledHint: {
    marginTop: 4,
    fontSize: 12,
    color: COLORS.textMuted,
  },
  pillsRow: {
    flexDirection: 'row',
    gap: 6,
  },
  membershipPill: {
    paddingHorizontal: 8,
    paddingVertical: 3,
    borderRadius: 999,
  },
  membershipPillText: {
    fontSize: 11,
    fontWeight: '700',
  },
  itemRight: {
    alignItems: 'flex-end',
    gap: 8,
  },
  itemExchange: {
    fontSize: 12,
    color: COLORS.textMuted,
  },
  itemAction: {
    fontSize: 12,
    fontWeight: '700',
    color: COLORS.primary,
  },
  itemActionDisabled: {
    color: COLORS.textMuted,
  },
  center: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    paddingHorizontal: 24,
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
    color: COLORS.textLight,
    textAlign: 'center',
  },
});
