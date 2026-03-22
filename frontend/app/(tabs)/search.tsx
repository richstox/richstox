/**
 * P34 (BINDING): Search Screen with Star Toggle
 * Fix 2: Auto-focus input when opened via "+" from dashboard
 * Fix 3: Each search result shows star toggle (on/off) reflecting user_watchlist
 * 
 * DO NOT CHANGE WITHOUT RICHARD APPROVAL (kurtarichard@gmail.com)
 */
import React, { useState, useEffect, useRef } from 'react';
import {
  View,
  Text,
  StyleSheet,
  TextInput,
  TouchableOpacity,
  FlatList,
  ActivityIndicator,
  Keyboard,
  Image,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useRouter, useLocalSearchParams } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import axios from 'axios';
import { useAuth } from '../../contexts/AuthContext';
import { useSearchStore } from '../../stores/searchStore';

const API_URL = process.env.EXPO_PUBLIC_BACKEND_URL;

const COLORS = {
  primary: '#6366F1',
  accent: '#10B981',
  warning: '#F59E0B',
  text: '#1F2937',
  textLight: '#6B7280',
  textMuted: '#9CA3AF',
  background: '#F9FAFB',
  card: '#FFFFFF',
  border: '#E5E7EB',
};

export default function Search() {
  const router = useRouter();
  const params = useLocalSearchParams();
  const inputRef = useRef<TextInput>(null);
  const { sessionToken } = useAuth();
  const { query: storedQuery, results: storedResults, setSearch } = useSearchStore();
  
  const [searchQuery, setSearchQuery] = useState(storedQuery);
  const [results, setResults] = useState<any[]>(storedResults);
  const [loading, setLoading] = useState(false);
  
  // P34 Fix 3: Track watchlist state for each ticker
  const [watchlistState, setWatchlistState] = useState<Record<string, boolean>>({});
  const [toggleLoading, setToggleLoading] = useState<Record<string, boolean>>({});
  
  // P36 Item 5: Track tickers added this session
  const [addedThisSession, setAddedThisSession] = useState<string[]>([]);

  // P34 Fix 2: Auto-focus search input on mount/navigation
  useEffect(() => {
    setTimeout(() => {
      inputRef.current?.focus();
    }, 100);
  }, []);

  useEffect(() => {
    if (searchQuery.length >= 1) {
      searchTickers();
    } else {
      setResults([]);
      setWatchlistState({});
    }
  }, [searchQuery]);

  const searchTickers = async () => {
    setLoading(true);
    try {
      const response = await axios.get(`${API_URL}/api/whitelist/search?q=${searchQuery}`);
      const searchResults = response.data.results || [];
      setResults(searchResults);
      setLoading(false);
      
      // Persist to store for ticker-to-ticker navigation
      if (searchResults.length > 0) {
        setSearch(searchQuery, searchResults);
      }
      
      // Load watchlist status in background (non-blocking)
      const watchlistChecks: Record<string, boolean> = {};
      const authHeaders = sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {};
      await Promise.all(
        searchResults.slice(0, 20).map(async (item: any) => {
          try {
            const checkRes = await axios.get(`${API_URL}/api/v1/watchlist/check/${item.ticker}`, {
              headers: authHeaders,
            });
            watchlistChecks[item.ticker] = checkRes.data.is_followed || false;
          } catch {
            watchlistChecks[item.ticker] = false;
          }
        })
      );
      setWatchlistState(watchlistChecks);
    } catch (error) {
      console.error('Error searching:', error);
      setLoading(false);
    }
  };

  // P35 Item 3 & 4 + P36 Item 5: Toggle watchlist with error handling + clear input + track added
  const toggleWatchlist = async (ticker: string) => {
    if (toggleLoading[ticker]) return;
    
    setToggleLoading(prev => ({ ...prev, [ticker]: true }));
    const isCurrentlyFollowed = watchlistState[ticker];
    const authHeaders = sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {};
    
    try {
      if (isCurrentlyFollowed) {
        await axios.delete(`${API_URL}/api/v1/watchlist/${ticker}`, {
          headers: authHeaders,
        });
        setWatchlistState(prev => ({ ...prev, [ticker]: false }));
        // P36 Item 5: Remove from added this session
        setAddedThisSession(prev => prev.filter(t => t !== ticker));
      } else {
        await axios.post(`${API_URL}/api/v1/watchlist/${ticker}`, {}, {
          headers: authHeaders,
        });
        setWatchlistState(prev => ({ ...prev, [ticker]: true }));
        
        // P36 Item 5: Add to "added this session" list
        setAddedThisSession(prev => prev.includes(ticker) ? prev : [...prev, ticker]);
        
        // P35 Item 3: Clear input after adding, keep cursor ready
        setSearchQuery('');
        setResults([]);
        // Keep input focused for fast multiple adds
        setTimeout(() => {
          inputRef.current?.focus();
        }, 100);
      }
    } catch (error) {
      // P35 Item 4: Show error toast and do not change star UI state
      console.error('Error toggling watchlist:', error);
      alert(`Failed to ${isCurrentlyFollowed ? 'unfollow' : 'follow'} ${ticker}. Please try again.`);
    } finally {
      setToggleLoading(prev => ({ ...prev, [ticker]: false }));
    }
  };
  
  // P36 Item 5: Remove from added this session (undo)
  const removeFromSession = async (ticker: string) => {
    const authHeaders = sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {};
    try {
      await axios.delete(`${API_URL}/api/v1/watchlist/${ticker}`, {
        headers: authHeaders,
      });
      setAddedThisSession(prev => prev.filter(t => t !== ticker));
      setWatchlistState(prev => ({ ...prev, [ticker]: false }));
    } catch (error) {
      console.error('Error removing from watchlist:', error);
      alert(`Failed to remove ${ticker}. Please try again.`);
    }
  };

  const handleTickerPress = (ticker: string) => {
    Keyboard.dismiss();
    router.push(`/stock/${ticker}`);
  };

  const handleDonePress = () => {
    setAddedThisSession([]);
    router.push('/(tabs)/dashboard');
  };

  return (
    <SafeAreaView style={styles.container} edges={['top']}>
      {/* P37+ Part 1 (D): Header with Done button */}
      <View style={styles.header}>
        <TouchableOpacity style={styles.backButton} onPress={() => router.back()}>
          <Ionicons name="arrow-back" size={24} color={COLORS.text} />
        </TouchableOpacity>
        <Text style={styles.headerTitle}>Add Stocks</Text>
        <TouchableOpacity style={styles.doneButton} onPress={handleDonePress}>
          <Text style={styles.doneButtonText}>Done</Text>
        </TouchableOpacity>
      </View>

      {/* P34 Fix 2: Search Input with ref for auto-focus */}
      <View style={styles.searchWrapper}>
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
      
      {/* P36 Item 5: Added this session chips */}
      {addedThisSession.length > 0 && (
        <View style={styles.addedSessionContainer}>
          <Text style={styles.addedSessionLabel}>Added:</Text>
          <View style={styles.addedSessionChips}>
            {addedThisSession.map((ticker) => (
              <View key={ticker} style={styles.addedChip}>
                <Text style={styles.addedChipText}>{ticker}</Text>
                <TouchableOpacity 
                  onPress={() => removeFromSession(ticker)}
                  style={styles.addedChipRemove}
                >
                  <Ionicons name="close" size={14} color={COLORS.textMuted} />
                </TouchableOpacity>
              </View>
            ))}
          </View>
        </View>
      )}

      {/* Results */}
      {loading && results.length === 0 ? (
        <View style={styles.center}>
          <ActivityIndicator size="large" color={COLORS.primary} />
        </View>
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
          contentContainerStyle={styles.list}
          showsVerticalScrollIndicator={false}
          keyboardShouldPersistTaps="handled"
          ListHeaderComponent={
            <View style={styles.resultsHeader}>
              <Text style={styles.resultsCount}>{results.length} result{results.length !== 1 ? 's' : ''}</Text>
            </View>
          }
          renderItem={({ item }) => {
            const isFollowed = watchlistState[item.ticker] || false;
            const isToggling = toggleLoading[item.ticker] || false;
            
            return (
              <View style={styles.itemRow}>
                {/* P34 Fix 3: Star toggle button */}
                <TouchableOpacity
                  style={styles.starButton}
                  onPress={() => toggleWatchlist(item.ticker)}
                  disabled={isToggling}
                  data-testid={`star-toggle-${item.ticker}`}
                >
                  {isToggling ? (
                    <ActivityIndicator size="small" color={COLORS.warning} />
                  ) : (
                    <Ionicons
                      name={isFollowed ? "star" : "star-outline"}
                      size={24}
                      color={isFollowed ? COLORS.warning : COLORS.textMuted}
                    />
                  )}
                </TouchableOpacity>
                
                {/* Stock info - navigates to detail */}
                <TouchableOpacity
                  style={styles.item}
                  onPress={() => handleTickerPress(item.ticker)}
                >
                  {item.logo ? (
                    <Image 
                      source={{ uri: item.logo.startsWith('http') ? item.logo : `https://eodhd.com${item.logo}` }}
                      style={styles.itemLogo}
                    />
                  ) : (
                    <View style={styles.itemIcon}>
                      <Text style={styles.itemInitial}>{item.ticker[0]}</Text>
                    </View>
                  )}
                  <View style={styles.itemInfo}>
                    <Text style={styles.itemTicker}>{item.ticker}</Text>
                    <Text style={styles.itemName} numberOfLines={1}>{item.name}</Text>
                  </View>
                  <Text style={styles.itemExchange}>{item.exchange}</Text>
                  <Ionicons name="chevron-forward" size={20} color={COLORS.textMuted} />
                </TouchableOpacity>
              </View>
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
    marginRight: 8,
  },
  headerTitle: { flex: 1, fontSize: 18, fontWeight: '600', color: COLORS.text, textAlign: 'center' },
  // P41: Done button - Apply style (solid filled)
  doneButton: {
    paddingHorizontal: 20,
    paddingVertical: 10,
    backgroundColor: '#1E3A5F',
    borderRadius: 8,
  },
  doneButtonText: {
    fontSize: 14,
    fontWeight: '600',
    color: '#FFFFFF',
  },
  closeButton: { 
    padding: 16,
    marginRight: -8,
    marginLeft: 8,
  },

  // P37+ Part 1 (A): Search - same style as main search
  searchWrapper: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: COLORS.background,
    marginHorizontal: 16,
    marginVertical: 12,
    paddingHorizontal: 14,
    paddingVertical: 10,
    borderRadius: 10,
    gap: 10,
  },
  searchInput: {
    flex: 1,
    fontSize: 16,
    color: COLORS.text,
    padding: 0,
    margin: 0,
    outlineStyle: 'none', // Remove web focus outline
    borderWidth: 0,       // No border on input itself
  },
  
  // P36 Item 5: Added this session chips
  addedSessionContainer: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    paddingHorizontal: 16,
    paddingBottom: 8,
    gap: 8,
    flexWrap: 'wrap',
  },
  addedSessionLabel: {
    fontSize: 12,
    color: COLORS.textMuted,
    paddingTop: 4,
  },
  addedSessionChips: {
    flex: 1,
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 6,
  },
  addedChip: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: COLORS.accent + '20',
    paddingLeft: 10,
    paddingRight: 4,
    paddingVertical: 4,
    borderRadius: 16,
    gap: 4,
  },
  addedChipText: {
    fontSize: 12,
    fontWeight: '600',
    color: COLORS.accent,
  },
  addedChipRemove: {
    width: 20,
    height: 20,
    borderRadius: 10,
    alignItems: 'center',
    justifyContent: 'center',
  },

  center: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    padding: 32,
  },
  emptyTitle: { fontSize: 18, fontWeight: '600', color: COLORS.text, marginTop: 16 },
  emptyText: { fontSize: 14, color: COLORS.textMuted, marginTop: 8 },

  list: { padding: 16, paddingTop: 0 },
  
  resultsHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    paddingBottom: 12,
    paddingTop: 4,
  },
  resultsCount: {
    fontSize: 13,
    color: COLORS.textMuted,
  },
  
  // P34 Fix 3: Row container with star + item
  itemRow: {
    flexDirection: 'row',
    alignItems: 'center',
    marginBottom: 8,
    gap: 8,
  },
  starButton: {
    width: 44,
    height: 44,
    alignItems: 'center',
    justifyContent: 'center',
  },
  item: {
    flex: 1,
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: COLORS.card,
    borderRadius: 12,
    padding: 14,
    gap: 12,
  },
  itemIcon: {
    width: 44,
    height: 44,
    borderRadius: 22,
    backgroundColor: COLORS.primary,
    alignItems: 'center',
    justifyContent: 'center',
  },
  itemInitial: { color: '#FFF', fontWeight: '700', fontSize: 18 },
  itemLogo: {
    width: 44,
    height: 44,
    borderRadius: 22,
    backgroundColor: COLORS.background,
  },
  itemInfo: { flex: 1 },
  itemTicker: { fontSize: 16, fontWeight: '700', color: COLORS.text },
  itemName: { fontSize: 13, color: COLORS.textLight, marginTop: 2 },
  itemExchange: {
    fontSize: 12,
    color: COLORS.textMuted,
    backgroundColor: COLORS.background,
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 6,
  },
});
