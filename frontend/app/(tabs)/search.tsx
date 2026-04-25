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
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useRouter, useLocalSearchParams, useFocusEffect } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import axios from 'axios';
import { useAuth } from '../../contexts/AuthContext';
import { useAppDialog } from '../../contexts/AppDialogContext';
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

const MEMBERSHIP_LABELS: Record<string, string> = {
  watchlist: 'W',
  tracklist: 'T',
};

const getMembershipLabel = (membership: unknown): string => {
  if (typeof membership !== 'string') return '?';
  const normalized = membership.trim();
  if (!normalized) return '?';
  return MEMBERSHIP_LABELS[normalized] || normalized.charAt(0).toUpperCase();
};

export default function Search() {
  const router = useRouter();
  const params = useLocalSearchParams();
  const inputRef = useRef<TextInput>(null);
  const { sessionToken } = useAuth();
  const dialog = useAppDialog();
  const { query: storedQuery, results: storedResults, setSearch } = useSearchStore();
  const sp = useLayoutSpacing();

  const replaceMode = params.mode === 'tracklist-replace';
  const oldTicker = typeof params.oldTicker === 'string' ? params.oldTicker.toUpperCase() : '';

  const [searchQuery, setSearchQuery] = useState(storedQuery);
  const [results, setResults] = useState<any[]>(storedResults);
  const [loading, setLoading] = useState(false);
  const [replacingTicker, setReplacingTicker] = useState<string | null>(null);

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
      if (!replaceMode && searchResults.length > 0) {
        setSearch(searchQuery, searchResults);
      }
    } catch (error) {
      console.error('Error searching:', error);
    } finally {
      setLoading(false);
    }
  }, [authHeaders, replaceMode, searchQuery, setSearch]);

  useEffect(() => {
    searchTickers();
  }, [searchTickers]);

  const handleTickerPress = async (item: any) => {
    Keyboard.dismiss();
    if (!replaceMode) {
      router.push(`/stock/${item.ticker}`);
      return;
    }

    const memberships = Array.isArray(item.memberships) ? item.memberships : [];
    const isDisabled = memberships.includes('tracklist') || memberships.includes('watchlist');
    if (isDisabled || !oldTicker || replacingTicker) return;

    setReplacingTicker(item.ticker);
    try {
      await axios.post(
        `${API_URL}/api/v1/tracklist/replace`,
        { old_ticker: oldTicker, new_ticker: item.ticker },
        { headers: authHeaders }
      );
      router.replace('/(tabs)/tracklist');
    } catch (error: any) {
      console.error('Error replacing tracklist ticker:', error);
      dialog.alert('Replace failed', error?.response?.data?.detail || 'Please try again.');
    } finally {
      setReplacingTicker(null);
    }
  };

  const renderMembershipBadges = (item: any) => {
    const memberships = Array.isArray(item.memberships) ? item.memberships : [];
    if (!memberships.length) return null;
    return (
      <View style={styles.badgesRow}>
        {memberships.map((membership: string) => (
          <View key={`${item.ticker}-${membership}`} style={styles.badge}>
            <Text style={styles.badgeText}>
              {getMembershipLabel(membership)}
            </Text>
          </View>
        ))}
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
          <Text style={styles.headerTitle}>{replaceMode ? 'Replace Tracklist stock' : 'Search Stocks'}</Text>
          {replaceMode && oldTicker ? (
            <Text style={styles.headerSubtitle}>Replace {oldTicker} with a new stock</Text>
          ) : null}
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
        <View style={styles.center}>
          <Ionicons name="search-outline" size={64} color={COLORS.border} />
          <Text style={styles.emptyTitle}>Search for stocks</Text>
          <Text style={styles.emptyText}>Enter a ticker or company name</Text>
        </View>
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
              {!replaceMode ? (
                <Text style={styles.resultsHint}>W / T badges show where each ticker already lives.</Text>
              ) : (
                <Text style={styles.resultsHint}>Current Watchlist and Tracklist members are disabled here.</Text>
              )}
            </View>
          }
          renderItem={({ item }) => {
            const memberships = Array.isArray(item.memberships) ? item.memberships : [];
            const disabledForReplace = replaceMode && (memberships.includes('tracklist') || memberships.includes('watchlist'));
            const isReplacing = replacingTicker === item.ticker;

            return (
              <TouchableOpacity
                style={[styles.item, disabledForReplace && styles.itemDisabled]}
                onPress={() => handleTickerPress(item)}
                disabled={isReplacing}
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
                    {renderMembershipBadges(item)}
                  </View>
                  <Text style={styles.itemName} numberOfLines={1}>{item.name}</Text>
                  {replaceMode && disabledForReplace ? (
                    <Text style={styles.disabledHint}>Already used in one of your lists</Text>
                  ) : null}
                </View>
                <View style={styles.itemRight}>
                  <Text style={styles.itemExchange}>{item.exchange}</Text>
                  {replaceMode ? (
                    isReplacing ? (
                      <Text style={styles.itemAction}>Saving…</Text>
                    ) : (
                      <Text style={[styles.itemAction, disabledForReplace && styles.itemActionDisabled]}>
                        {disabledForReplace ? 'Unavailable' : 'Replace'}
                      </Text>
                    )
                  ) : (
                    <Ionicons name="chevron-forward" size={20} color={COLORS.textMuted} />
                  )}
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
  headerSubtitle: {
    marginTop: 2,
    fontSize: 12,
    color: COLORS.textLight,
  },
  headerPlaceholder: {
    width: 48,
  },
  searchWrapper: {
    marginTop: 16,
    marginBottom: 16,
    backgroundColor: COLORS.card,
    borderRadius: 16,
    borderWidth: 1,
    borderColor: COLORS.border,
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
  resultsHint: {
    marginTop: 4,
    fontSize: 12,
    color: COLORS.textMuted,
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
  badgesRow: {
    flexDirection: 'row',
    gap: 6,
  },
  badge: {
    minWidth: 22,
    paddingHorizontal: 7,
    paddingVertical: 3,
    borderRadius: 999,
    backgroundColor: '#EEF2FF',
    alignItems: 'center',
  },
  badgeText: {
    fontSize: 11,
    fontWeight: '700',
    color: COLORS.primary,
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
