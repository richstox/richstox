import React, { useState, useCallback, useEffect, useRef, useMemo } from 'react';
import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  RefreshControl,
  ActivityIndicator,
  TouchableOpacity,
  TextInput,
  Modal,
  Image,
  Keyboard,
  Dimensions,
  FlatList,
  Platform,
  Pressable,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useFocusEffect, useRouter } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import axios from 'axios';
import { useAuth } from '../../contexts/AuthContext';
import BrandedLoading from '../../components/BrandedLoading';

const API_URL = process.env.EXPO_PUBLIC_BACKEND_URL;
const { width: SCREEN_WIDTH } = Dimensions.get('window');

const COLORS = {
  primary: '#1E3A5F',
  accent: '#10B981',
  warning: '#F59E0B',
  danger: '#EF4444',
  text: '#1F2937',
  textLight: '#6B7280',
  textMuted: '#9CA3AF',
  background: '#F5F7FA',
  card: '#FFFFFF',
  border: '#E5E7EB',
  subscribed: '#F59E0B',
};

// Sector icons mapping
const SECTOR_ICONS: Record<string, string> = {
  'Technology': 'laptop-outline',
  'Healthcare': 'medical-outline',
  'Financial Services': 'cash-outline',
  'Consumer Cyclical': 'cart-outline',
  'Consumer Defensive': 'basket-outline',
  'Industrials': 'construct-outline',
  'Energy': 'flash-outline',
  'Utilities': 'bulb-outline',
  'Basic Materials': 'cube-outline',
  'Real Estate': 'business-outline',
  'Communication Services': 'chatbubbles-outline',
};

interface TalkPost {
  post_id: string;
  user_id: string;
  text: string;
  symbol?: string;
  symbols?: string[];  // Multiple tickers support
  created_at: string;
  edited_at?: string;  // Timestamp when post was edited
  deleted_at?: string; // Soft delete timestamp
  user?: {
    user_id: string;
    name: string;
    picture?: string;
  };
  rrr?: number;
}

interface FilterOptions {
  countries: { value: string; label: string; icon: string }[];
  exchanges: { value: string; label: string }[];
  sectors: { value: string; label: string }[];
  industries: { value: string; label: string }[];
}

interface Subscription {
  subscription_id: string;
  type: string;
  value: string;
  value_canonical: string;
}

export default function TalkScreen() {
  const router = useRouter();
  const { user, isAuthenticated, sessionToken, isLoading: authLoading } = useAuth();
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [posts, setPosts] = useState<TalkPost[]>([]);
  const [hasMore, setHasMore] = useState(false);
  const [offset, setOffset] = useState(0);
  
  // Create post modal
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [newPostText, setNewPostText] = useState('');
  const [posting, setPosting] = useState(false);
  const [postError, setPostError] = useState<string | null>(null);
  
  // Symbol autocomplete for new post - supports multiple tickers (max 3)
  const [selectedSymbols, setSelectedSymbols] = useState<string[]>([]);
  const [symbolSearchText, setSymbolSearchText] = useState('');
  const [symbolSuggestions, setSymbolSuggestions] = useState<{ symbol: string; name: string; logo_url?: string }[]>([]);
  const [showSymbolSuggestions, setShowSymbolSuggestions] = useState(false);
  const MAX_TICKERS = 3;
  
  // Edit/Delete post
  const [showPostMenu, setShowPostMenu] = useState<string | null>(null); // post_id of visible menu
  const [editingPost, setEditingPost] = useState<TalkPost | null>(null);
  const [editText, setEditText] = useState('');
  const [editLoading, setEditLoading] = useState(false);
  const EDIT_WINDOW_MINUTES = 15;
  
  // Filter options
  const [filterOptions, setFilterOptions] = useState<FilterOptions>({
    countries: [{ value: 'US', label: 'United States' }],
    exchanges: [],
    sectors: [],
    industries: [],
  });
  const [filtersLoading, setFiltersLoading] = useState(true);
  const [filtersError, setFiltersError] = useState<string | null>(null);
  
  // Active filters - now arrays for multi-select
  const [countryFilter, setCountryFilter] = useState<string[]>([]);
  const [exchangeFilter, setExchangeFilter] = useState<string[]>([]);
  const [sectorFilter, setSectorFilter] = useState<string[]>([]);
  const [industryFilter, setIndustryFilter] = useState<string[]>([]);
  const [symbolFilter, setSymbolFilter] = useState<string[]>([]);
  
  // Dropdown modal with multi-select support
  const [dropdownModal, setDropdownModal] = useState<{
    type: string;
    title: string;
    options: { value: string; label: string; icon?: string }[];
    currentValues: string[];
    onApply: (values: string[]) => void;
  } | null>(null);
  const [dropdownSearch, setDropdownSearch] = useState('');
  const [dropdownSelected, setDropdownSelected] = useState<string[]>([]);
  const [showOnlySubscribed, setShowOnlySubscribed] = useState(false);
  
  // Delete confirmation modal
  const [deleteConfirmPost, setDeleteConfirmPost] = useState<string | null>(null);
  
  // Local canonicalize helper (matches backend logic)
  const canonicalizeLocal = (value: string, type: string): string => {
    if (!value) return '';
    if (type === 'symbol') {
      return value.replace('.US', '').toUpperCase().trim();
    }
    return value.split(/\s+/).join(' ').trim().toLowerCase();
  };
  
  // Subscriptions
  const [subscriptions, setSubscriptions] = useState<Subscription[]>([]);
  const [subscriptionLoading, setSubscriptionLoading] = useState<string | null>(null);
  
  // Per-type subscription sets: subSetByType[type] = Set<value_canonical>
  const subSetByType = useMemo(() => {
    const map: Record<string, Set<string>> = {};
    subscriptions.forEach(s => {
      if (!map[s.type]) map[s.type] = new Set();
      map[s.type].add(s.value_canonical);
    });
    return map;
  }, [subscriptions]);
  
  // Per-type subscription ID map: subIdByType[type][value_canonical] = subscription_id
  const subIdByType = useMemo(() => {
    const map: Record<string, Record<string, string>> = {};
    subscriptions.forEach(s => {
      if (!map[s.type]) map[s.type] = {};
      map[s.type][s.value_canonical] = s.subscription_id;
    });
    return map;
  }, [subscriptions]);
  
  // Check if subscribed for a specific type/value
  const isSubscribed = (type: string, value: string): boolean => {
    const canonical = canonicalizeLocal(value, type);
    return subSetByType[type]?.has(canonical) ?? false;
  };
  
  // Get subscription_id for a type/value
  const getSubscriptionId = (type: string, value: string): string | undefined => {
    const canonical = canonicalizeLocal(value, type);
    return subIdByType[type]?.[canonical];
  };
  
  // Count subscriptions for a specific type
  const getSubscriptionCount = (type: string): number => {
    return subSetByType[type]?.size ?? 0;
  };
  
  // Scroll ref
  const filterScrollRef = useRef<ScrollView>(null);

  // Tickers for Company filter
  const [tickers, setTickers] = useState<{ symbol: string; name: string; exchange: string; sector?: string; industry?: string; logo_url?: string }[]>([]);
  const [tickersLoading, setTickersLoading] = useState(false);
  
  // Compute available filter options from tickers (client-side, no API calls)
  const availableFilters = useMemo(() => {
    // Start with all tickers
    let filteredTickers = tickers;
    
    // Apply exchange filter
    if (exchangeFilter.length > 0) {
      filteredTickers = filteredTickers.filter(t => exchangeFilter.includes(t.exchange));
    }
    
    // Compute available sectors from filtered tickers
    const sectorCounts: Record<string, number> = {};
    filteredTickers.forEach(t => {
      if (t.sector) {
        sectorCounts[t.sector] = (sectorCounts[t.sector] || 0) + 1;
      }
    });
    const sectors = Object.entries(sectorCounts)
      .map(([value, count]) => ({ value, count }))
      .sort((a, b) => a.value.localeCompare(b.value));
    
    // Apply sector filter for industries
    let tickersForIndustries = filteredTickers;
    if (sectorFilter.length > 0) {
      tickersForIndustries = filteredTickers.filter(t => sectorFilter.includes(t.sector || ''));
    }
    
    // Compute available industries from filtered tickers
    const industryCounts: Record<string, number> = {};
    tickersForIndustries.forEach(t => {
      if (t.industry) {
        industryCounts[t.industry] = (industryCounts[t.industry] || 0) + 1;
      }
    });
    const industries = Object.entries(industryCounts)
      .map(([value, count]) => ({ value, count }))
      .sort((a, b) => a.value.localeCompare(b.value));
    
    // Compute final company list (filtered by all selections)
    let finalTickers = tickersForIndustries;
    if (industryFilter.length > 0) {
      finalTickers = tickersForIndustries.filter(t => industryFilter.includes(t.industry || ''));
    }
    
    return {
      sectors,
      industries,
      filteredTickers: finalTickers,  // Companies matching all filters
      companyCount: finalTickers.length,
      totalTickerCount: tickers.length,  // Total tickers for Country filter
      exchanges: [
        { value: 'NASDAQ', count: tickers.filter(t => t.exchange === 'NASDAQ').length },
        { value: 'NYSE', count: tickers.filter(t => t.exchange === 'NYSE').length },
      ]
    };
  }, [tickers, exchangeFilter, sectorFilter, industryFilter]);
  
  // Auto-remove invalid selections when available options change
  useEffect(() => {
    const validSectors = new Set(availableFilters.sectors.map(s => s.value));
    const validIndustries = new Set(availableFilters.industries.map(i => i.value));
    
    const invalidSectors = sectorFilter.filter(s => !validSectors.has(s));
    const invalidIndustries = industryFilter.filter(i => !validIndustries.has(i));
    
    if (invalidSectors.length > 0) {
      setSectorFilter(sectorFilter.filter(s => validSectors.has(s)));
    }
    if (invalidIndustries.length > 0) {
      setIndustryFilter(industryFilter.filter(i => validIndustries.has(i)));
    }
  }, [availableFilters.sectors.length, availableFilters.industries.length]);

  // Fetch filter options (base data only, no dependent calls)
  const fetchFilterOptions = async () => {
    setFiltersLoading(true);
    setFiltersError(null);
    try {
      const filtersRes = await axios.get(`${API_URL}/api/v1/talk/filters`);
      const data = filtersRes.data;
      
      setFilterOptions({
        countries: [{ value: 'US', label: 'United States' }],
        exchanges: (data.exchanges || [])
          .filter((e: string) => e === 'NASDAQ' || e === 'NYSE')
          .map((e: string) => ({ value: e, label: e })),
        sectors: (data.sectors || []).map((s: string) => ({ value: s, label: s })),
        industries: (data.industries || []).map((i: string) => ({ value: i, label: i })),
      });
    } catch (err) {
      console.error('Error fetching filters:', err);
      setFiltersError('Failed to load filters');
    } finally {
      setFiltersLoading(false);
    }
  };

  // Fetch tickers based on current filters
  // Fetch ALL tickers once (no filters - filtering is done client-side)
  const fetchTickers = async () => {
    setTickersLoading(true);
    try {
      // Load all tickers - filtering happens client-side via availableFilters useMemo
      const response = await axios.get(`${API_URL}/api/v1/talk/tickers`);
      setTickers(response.data.tickers || []);
    } catch (err) {
      console.error('Error fetching tickers:', err);
    } finally {
      setTickersLoading(false);
    }
  };

  // Fetch subscription counts from API
  const fetchSubscriptionCounts = async (token: string) => {
    if (!token) return;
    try {
      const response = await axios.get(`${API_URL}/api/v1/talk/subscriptions/counts`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      setSubscriptionCounts(response.data.counts || { country: 0, exchange: 0, sector: 0, industry: 0, symbol: 0 });
    } catch (err) {
      console.error('Error fetching subscription counts:', err);
    }
  };

  // Fetch subscriptions and counts
  const fetchSubscriptions = async (token: string) => {
    if (!token) return;
    try {
      const response = await axios.get(`${API_URL}/api/v1/talk/subscriptions`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      
      // Backend returns: { subscriptions: [{ subscription_id, type, value, value_canonical }, ...] }
      const subs: Subscription[] = (response.data.subscriptions || []).map((s: any) => ({
        subscription_id: s.subscription_id,
        type: s.type,
        value: s.value,
        value_canonical: s.value_canonical || canonicalizeLocal(s.value || '', s.type)
      }));
      
      console.log(`[SUBS_LOADED] Count: ${subs.length}`);
      setSubscriptions(subs);
    } catch (err) {
      console.error('Error fetching subscriptions:', err);
    }
  };

  // Toggle subscription using explicit SUBSCRIBE/UNSUBSCRIBE by _id
  const toggleSubscription = async (type: string, value: string, e?: any) => {
    if (e) e.stopPropagation();
    if (!sessionToken || !value) return;
    
    const canonical = canonicalizeLocal(value, type);
    const itemKey = `${type}:${canonical}`;
    
    // Prevent double-click
    if (subscriptionLoading === itemKey) return;
    
    setSubscriptionLoading(itemKey);
    
    // Check if already subscribed - use getSubscriptionId
    const existingSubscriptionId = getSubscriptionId(type, value);
    
    try {
      if (existingSubscriptionId) {
        // UNSUBSCRIBE: DELETE by _id - always works
        await axios.delete(
          `${API_URL}/api/v1/talk/subscriptions/${existingSubscriptionId}`,
          { headers: { Authorization: `Bearer ${sessionToken}` } }
        );
        
        // Remove from state by subscription_id
        setSubscriptions(prev => prev.filter(s => s.subscription_id !== existingSubscriptionId));
        
      } else {
        // SUBSCRIBE: POST - upsert
        const response = await axios.post(
          `${API_URL}/api/v1/talk/subscriptions`,
          { type, value: value.trim() },
          { headers: { Authorization: `Bearer ${sessionToken}` } }
        );
        
        // Add to state
        const newSub: Subscription = {
          subscription_id: response.data.subscription_id,
          type: response.data.type,
          value: response.data.value,
          value_canonical: response.data.value_canonical
        };
        setSubscriptions(prev => [...prev, newSub]);
      }
      
    } catch (err) {
      console.error('[TOGGLE_ERROR]', err);
    } finally {
      setSubscriptionLoading(null);
    }
  };

  // Fetch posts
  const fetchPosts = async (newOffset: number = 0, append: boolean = false) => {
    try {
      if (!append) setLoading(true);
      
      let url = `${API_URL}/api/v1/talk?limit=20&offset=${newOffset}`;
      // Support array filters - use first value for API
      if (countryFilter && countryFilter.length > 0) url += `&country=${countryFilter[0]}`;
      if (exchangeFilter && exchangeFilter.length > 0) url += `&exchange=${exchangeFilter[0]}`;
      if (sectorFilter && sectorFilter.length > 0) url += `&sector=${encodeURIComponent(sectorFilter[0])}`;
      if (industryFilter && industryFilter.length > 0) url += `&industry=${encodeURIComponent(industryFilter[0])}`;
      if (symbolFilter && symbolFilter.length > 0) url += `&symbol=${symbolFilter[0]}`;
      
      console.log('[FEED] FINAL_TALK_URL:', url);
      console.log('[FEED] Filters:', { countryFilter, exchangeFilter, sectorFilter, industryFilter, symbolFilter });
      
      const response = await axios.get(url);
      const newPosts = response.data.posts || [];
      
      console.log('[FEED] Response:', { count: newPosts.length, hasMore: response.data.has_more });
      
      if (append) {
        setPosts([...posts, ...newPosts]);
      } else {
        setPosts(newPosts);
      }
      
      setHasMore(response.data.has_more || false);
      setOffset(newOffset);
    } catch (err) {
      console.error('Error fetching posts:', err);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  // Re-fetch posts when filters change
  useEffect(() => {
    console.log('[FEED] Filter changed, re-fetching posts');
    fetchPosts(0, false);
  }, [
    JSON.stringify(countryFilter),
    JSON.stringify(exchangeFilter),
    JSON.stringify(sectorFilter),
    JSON.stringify(industryFilter),
    JSON.stringify(symbolFilter)
  ]);
  
  // Fetch filter options and tickers on mount
  useFocusEffect(
    useCallback(() => {
      fetchFilterOptions();
      fetchTickers();
    }, [])
  );
  
  // Fetch subscriptions when auth is ready and sessionToken is available
  useEffect(() => {
    if (!authLoading && sessionToken) {
      fetchSubscriptions(sessionToken);
    }
  }, [sessionToken, authLoading]);

  const onRefresh = () => {
    setRefreshing(true);
    fetchFilterOptions();
    fetchPosts(0, false);
  };

  const loadMore = () => {
    if (hasMore && !loading) {
      fetchPosts(offset + 20, true);
    }
  };

  // Search symbols for autocomplete in New Post modal
  const handleSymbolSearch = (text: string) => {
    setSymbolSearchText(text);
    
    if (text.length < 1) {
      setSymbolSuggestions([]);
      setShowSymbolSuggestions(false);
      return;
    }
    
    // Filter tickers based on input (search by symbol or name)
    // Exclude already selected symbols
    const searchTerm = text.toUpperCase();
    const filtered = tickers
      .filter(t => {
        const cleanSymbol = t.symbol.replace('.US', '');
        return !selectedSymbols.includes(cleanSymbol) && (
          t.symbol.toUpperCase().includes(searchTerm) || 
          t.name.toUpperCase().includes(searchTerm)
        );
      })
      .slice(0, 8); // Limit to 8 suggestions
    
    setSymbolSuggestions(filtered);
    setShowSymbolSuggestions(filtered.length > 0);
  };

  // Select symbol from suggestions - add to array
  const selectSymbol = (symbol: string) => {
    if (selectedSymbols.length >= MAX_TICKERS) return;
    
    const cleanSymbol = symbol.replace('.US', '');
    if (!selectedSymbols.includes(cleanSymbol)) {
      setSelectedSymbols([...selectedSymbols, cleanSymbol]);
    }
    setSymbolSearchText('');
    setSymbolSuggestions([]);
    setShowSymbolSuggestions(false);
  };

  // Remove a selected symbol
  const removeSymbol = (symbol: string) => {
    setSelectedSymbols(selectedSymbols.filter(s => s !== symbol));
  };

  const createPost = async () => {
    if (!newPostText.trim()) return;
    
    setPosting(true);
    setPostError(null);
    
    try {
      // Use first selected symbol as primary (backend currently supports single symbol)
      // Future: backend can be updated to support multiple symbols
      const primarySymbol = selectedSymbols.length > 0 ? selectedSymbols[0] : null;
      
      const response = await axios.post(
        `${API_URL}/api/v1/talk`,
        { text: newPostText.trim(), symbol: primarySymbol, symbols: selectedSymbols },
        { headers: sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {}, withCredentials: true }
      );
      
      // Success - prepend new post to feed
      const newPost = response.data.post;
      if (newPost) {
        setPosts(prev => [newPost, ...prev]);
      }
      
      setNewPostText('');
      setSelectedSymbols([]);
      setSymbolSearchText('');
      setShowCreateModal(false);
      Keyboard.dismiss();
    } catch (err: any) {
      console.error('Error creating post:', err);
      const errorCode = err?.response?.data?.detail || '';
      // Map technical error codes to user-friendly messages
      let errorMsg = 'Failed to create post. Please try again.';
      if (errorCode === 'rate_limit') {
        errorMsg = "You've reached your daily limit (5 posts per day)";
      } else if (errorCode === 'text_too_short') {
        errorMsg = 'Your post is too short. Please write at least 10 characters.';
      } else if (errorCode === 'text_too_long') {
        errorMsg = 'Your post is too long. Maximum 2000 characters allowed.';
      } else if (errorCode === 'forbidden_content') {
        errorMsg = 'Your post contains prohibited content.';
      }
      setPostError(errorMsg);
    } finally {
      setPosting(false);
    }
  };

  // Check if post can be edited (within 15 minutes)
  const canEditPost = (post: TalkPost) => {
    if (!user?.user_id || post.user_id !== user.user_id) return false;
    const created = new Date(post.created_at);
    const now = new Date();
    const diffMinutes = (now.getTime() - created.getTime()) / 60000;
    return diffMinutes <= EDIT_WINDOW_MINUTES;
  };

  // Check if current user can delete post (author or admin)
  const canDeletePost = (post: TalkPost) => {
    if (!user?.user_id) return false;
    return post.user_id === user.user_id || user.role === 'admin';
  };
  
  // Check if menu should be shown
  const shouldShowMenu = (post: TalkPost) => {
    if (!user?.user_id) return false;
    return post.user_id === user.user_id || user.role === 'admin';
  };

  // Start editing a post
  const startEditPost = (post: TalkPost) => {
    setEditingPost(post);
    setEditText(post.text);
    setShowPostMenu(null);
  };

  // Save edited post
  const saveEditPost = async () => {
    if (!editingPost || editText.trim().length < 10) return;
    
    setEditLoading(true);
    try {
      await axios.patch(
        `${API_URL}/api/v1/talk/${editingPost.post_id}`,
        { text: editText.trim() },
        { headers: sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {}, withCredentials: true }
      );
      
      // Update post in local state
      setPosts(prev => prev.map(p => 
        p.post_id === editingPost.post_id 
          ? { ...p, text: editText.trim(), edited_at: new Date().toISOString() }
          : p
      ));
      
      setEditingPost(null);
      setEditText('');
    } catch (err: any) {
      console.error('Error editing post:', err);
      const errorMsg = err?.response?.data?.detail || 'Failed to edit post';
      alert(errorMsg);
    } finally {
      setEditLoading(false);
    }
  };

  // Delete post - shows confirmation modal first
  const deletePost = (postId: string) => {
    setDeleteConfirmPost(postId);
    setShowPostMenu(null);
  };
  
  // Actually perform the delete after confirmation
  const confirmDelete = async () => {
    if (!deleteConfirmPost) return;
    
    console.log('Deleting post:', deleteConfirmPost, 'with token:', sessionToken ? 'exists' : 'missing');
    
    try {
      const response = await axios.delete(
        `${API_URL}/api/v1/talk/${deleteConfirmPost}`,
        { headers: sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {}, withCredentials: true }
      );
      console.log('Delete response:', response.data);
      
      // Remove post from local state
      setPosts(prev => prev.filter(p => p.post_id !== deleteConfirmPost));
    } catch (err: any) {
      console.error('Error deleting post:', err);
      const errorMsg = err?.response?.data?.detail || 'Failed to delete post';
      alert(errorMsg);
    } finally {
      setDeleteConfirmPost(null);
    }
  };

  const formatTimeAgo = (dateStr: string) => {
    const date = new Date(dateStr);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);
    
    if (diffMins < 1) return 'now';
    if (diffMins < 60) return `${diffMins}m`;
    if (diffHours < 24) return `${diffHours}h`;
    return `${diffDays}d`;
  };

  const navigateToUser = (userId: string) => router.push(`/user/${userId}`);
  const navigateToStock = (symbol: string) => router.push(`/stock/${symbol.replace('.US', '')}`);

  const clearAllFilters = () => {
    setCountryFilter([]);
    setExchangeFilter([]);
    setSectorFilter([]);
    setIndustryFilter([]);
    setSymbolFilter([]);
  };

  // Safe check - all filters are arrays, check length > 0
  const hasActiveFilters = (countryFilter && countryFilter.length > 0) || 
    (exchangeFilter && exchangeFilter.length > 0) || 
    (sectorFilter && sectorFilter.length > 0) || 
    (industryFilter && industryFilter.length > 0) || 
    (symbolFilter && symbolFilter.length > 0);

  // Get icon for filter type
  const getFilterIcon = (type: string, value?: string): string => {
    if (type === 'country') return 'globe-outline';
    if (type === 'exchange') return 'stats-chart-outline';
    if (type === 'sector') return SECTOR_ICONS[value || ''] || 'business-outline';
    if (type === 'industry') return 'briefcase-outline';
    if (type === 'company') return 'search-outline';
    return 'filter-outline';
  };

  // Open dropdown modal with multi-select
  const openDropdown = (
    type: string,
    title: string,
    options: { value: string; label: string; icon?: string }[],
    currentValues: string[],
    onApply: (values: string[]) => void
  ) => {
    setDropdownSelected([...currentValues]); // Copy current selection
    setDropdownModal({ type, title, options, currentValues, onApply });
  };
  
  // Helper to pluralize filter titles
  const getPluralTitle = (title: string): string => {
    const plurals: { [key: string]: string } = {
      'Company': 'Companies',
      'Industry': 'Industries', 
      'Sector': 'Sectors',
      'Exchange': 'Exchanges',
      'Country': 'Countries',
    };
    return plurals[title] || title;
  };
  
  // Toggle item selection in dropdown
  const toggleDropdownItem = (value: string) => {
    setDropdownSelected(prev => 
      prev.includes(value) 
        ? prev.filter(v => v !== value)
        : [...prev, value]
    );
  };
  
  // Apply dropdown selection
  const applyDropdownSelection = () => {
    if (dropdownModal) {
      dropdownModal.onApply(dropdownSelected);
      setDropdownModal(null);
      setDropdownSearch('');
      setDropdownSelected([]);
      setShowOnlySubscribed(false);
    }
  };
  
  // Clear all selections in dropdown and apply immediately
  const clearDropdownSelection = () => {
    if (dropdownModal) {
      // Apply empty selection immediately to close modal and clear filter
      dropdownModal.onApply([]);
      setDropdownModal(null);
      setDropdownSearch('');
      setDropdownSelected([]);
      setShowOnlySubscribed(false);
    } else {
      setDropdownSelected([]);
    }
  };

  // Get icon for dropdown item
  const getItemIcon = (type: string, value: string): string => {
    if (type === 'country') return 'globe-outline';
    if (type === 'exchange') return 'stats-chart-outline';
    if (type === 'sector') return SECTOR_ICONS[value] || 'business-outline';
    if (type === 'industry') return 'briefcase-outline';
    return 'ellipse-outline';
  };

  // Filter chip component - shows selected count and subscription count
  const FilterChip = ({
    type,
    label,
    values = [],
    icon,
    options = [],
    onApply,
    subscriptionCount,
    totalCount,
  }: {
    type: string;
    label: string;
    values: string[];
    icon?: string;
    options: { value: string; label: string; icon?: string; logo_url?: string }[];
    onApply: (v: string[]) => void;
    subscriptionCount?: number;
    totalCount?: number;
  }) => {
    const safeValues = values || [];
    const hasSelection = safeValues.length > 0;
    
    // Always show label, append selection info
    let displayText = label;
    if (hasSelection) {
      if (safeValues.length === 1) {
        // For single selection, show "Label: Value" but truncate long values
        const val = safeValues[0];
        const truncated = val.length > 12 ? val.substring(0, 12) + '...' : val;
        displayText = `${label}: ${truncated}`;
      } else {
        displayText = `${label}: ${safeValues.length}`;
      }
    }
    
    // Show subscription count badge for any filter type with subscriptions
    const showSubscriptionBadge = subscriptionCount !== undefined && subscriptionCount > 0 && totalCount !== undefined;
    
    return (
      <TouchableOpacity
        style={[styles.filterChip, hasSelection && styles.filterChipActive]}
        onPress={() => openDropdown(type, label, options || [], safeValues, onApply)}
        data-testid={`filter-chip-${type}`}
        activeOpacity={0.7}
      >
        {/* Icon for all types including country */}
        {icon && (
          <Ionicons name={icon as any} size={14} color={hasSelection ? '#fff' : COLORS.textLight} />
        )}
        
        {/* Label - always show label prefix */}
        <Text style={[styles.filterChipText, hasSelection && styles.filterChipTextActive]} numberOfLines={1}>
          {displayText}
        </Text>
        
        {/* Subscription count badge - for all filter types */}
        {showSubscriptionBadge && (
          <View style={[styles.subscriptionCountBadge, hasSelection && styles.subscriptionCountBadgeActive]}>
            <Text style={[styles.subscriptionCountText, hasSelection && styles.subscriptionCountTextActive]}>
              {subscriptionCount}/{totalCount}
            </Text>
          </View>
        )}
        
        <Ionicons name="chevron-down" size={12} color={hasSelection ? '#fff' : COLORS.textLight} />
      </TouchableOpacity>
    );
  };

  // Render post
  const renderPost = (post: TalkPost) => (
    <View key={post.post_id} style={styles.postCard}>
      <View style={styles.postHeader}>
        <TouchableOpacity
          style={styles.userInfo}
          onPress={() => post.user?.user_id && navigateToUser(post.user.user_id)}
        >
          {post.user?.picture ? (
            <Image source={{ uri: post.user.picture }} style={styles.avatar} />
          ) : (
            <View style={styles.avatarPlaceholder}>
              <Ionicons name="person" size={16} color={COLORS.textMuted} />
            </View>
          )}
          <View style={styles.userMeta}>
            <Text style={styles.userName}>{post.user?.name || 'Anonymous'}</Text>
            {post.rrr != null && (
              <View style={styles.rrrBadge}>
                <Text style={styles.rrrText}>RRR {post.rrr.toFixed(1)}</Text>
              </View>
            )}
          </View>
        </TouchableOpacity>
        <View style={styles.postHeaderRight}>
          <Text style={styles.postTime}>
            {formatTimeAgo(post.created_at)}
            {post.edited_at && <Text style={styles.editedBadge}> · Edited</Text>}
          </Text>
          {/* Menu button - only show if user can interact with the post */}
          {shouldShowMenu(post) && (
            <TouchableOpacity 
              style={styles.postMenuButton}
              onPress={() => setShowPostMenu(showPostMenu === post.post_id ? null : post.post_id)}
              data-testid={`post-menu-btn-${post.post_id}`}
            >
              <Ionicons name="ellipsis-vertical" size={20} color={COLORS.text} />
            </TouchableOpacity>
          )}
        </View>
      </View>
      
      {/* Dropdown menu */}
      {showPostMenu === post.post_id && shouldShowMenu(post) && (
        <View style={styles.postMenuDropdown}>
          {canEditPost(post) && (
            <TouchableOpacity 
              style={styles.postMenuItem} 
              onPress={() => startEditPost(post)}
              data-testid={`post-edit-btn-${post.post_id}`}
            >
              <Ionicons name="pencil" size={16} color={COLORS.text} />
              <Text style={styles.postMenuItemText}>Edit</Text>
            </TouchableOpacity>
          )}
          {canDeletePost(post) && (
            <TouchableOpacity 
              style={[
                styles.postMenuItem, 
                canEditPost(post) && styles.postMenuItemDanger,
                { cursor: 'pointer' } as any
              ]} 
              onPress={() => {
                console.log('Delete onPress for post:', post.post_id);
                deletePost(post.post_id);
              }}
              onPressIn={() => console.log('Delete onPressIn')}
              data-testid={`post-delete-btn-${post.post_id}`}
            >
              <Ionicons name="trash" size={16} color={COLORS.danger} />
              <Text style={[styles.postMenuItemText, { color: COLORS.danger }]}>Delete</Text>
            </TouchableOpacity>
          )}
        </View>
      )}
      
      <Text style={styles.postText}>{post.text}</Text>
      {/* Display all symbols */}
      {(post.symbols && post.symbols.length > 0) ? (
        <View style={styles.symbolTagsRow}>
          {post.symbols.map((sym) => (
            <TouchableOpacity 
              key={sym} 
              style={styles.symbolTag} 
              onPress={() => navigateToStock(sym)}
            >
              <Text style={styles.symbolTagText}>${sym.replace('.US', '')}</Text>
            </TouchableOpacity>
          ))}
        </View>
      ) : post.symbol && (
        <TouchableOpacity style={styles.symbolTag} onPress={() => navigateToStock(post.symbol!)}>
          <Text style={styles.symbolTagText}>${post.symbol.replace('.US', '')}</Text>
        </TouchableOpacity>
      )}
    </View>
  );

  return (
    <SafeAreaView style={styles.container} edges={['top']}>
      {/* Header */}
      <View style={styles.header}>
        <View>
          <Text style={styles.title}>Talk</Text>
          <Text style={styles.subtitle}>Community discussions</Text>
        </View>
        <View style={styles.headerRight}>
          {hasActiveFilters && (
            <TouchableOpacity onPress={clearAllFilters} style={styles.clearFiltersBtn}>
              <Ionicons name="close-circle" size={18} color={COLORS.danger} />
              <Text style={styles.clearFiltersText}>Clear</Text>
            </TouchableOpacity>
          )}
          {/* FAB in header */}
          <TouchableOpacity style={styles.headerFab} onPress={() => setShowCreateModal(true)} data-testid="create-post-btn">
            <Ionicons name="add" size={24} color="#FFFFFF" />
          </TouchableOpacity>
        </View>
      </View>

      {/* Filter Bar - Livesport style */}
      <View style={styles.filterBar}>
        {filtersError ? (
          <View style={styles.filterError}>
            <Ionicons name="warning-outline" size={16} color={COLORS.danger} />
            <Text style={styles.filterErrorText}>{filtersError}</Text>
            <TouchableOpacity onPress={fetchFilterOptions}>
              <Text style={styles.retryText}>Retry</Text>
            </TouchableOpacity>
          </View>
        ) : (
          <View style={styles.filterScrollWrapper}>
            <ScrollView
              ref={filterScrollRef}
              horizontal
              showsHorizontalScrollIndicator={false}
              contentContainerStyle={styles.filterScrollContent}
              scrollEventThrottle={16}
            >
              <FilterChip
                type="country"
                label="Country"
                values={countryFilter}
                icon="globe-outline"
                options={[{ value: 'US', label: `United States (${availableFilters.totalTickerCount || 0})`, icon: '🇺🇸' }]}
                onApply={setCountryFilter}
                subscriptionCount={getSubscriptionCount('country')}
                totalCount={availableFilters.totalTickerCount || 0}
              />
              <FilterChip
                type="exchange"
                label="Exchange"
                values={exchangeFilter}
                icon="stats-chart-outline"
                options={availableFilters.exchanges.length > 0 
                  ? availableFilters.exchanges.filter(e => e.count > 0).map(e => ({ value: e.value, label: `${e.value} (${e.count})` }))
                  : filterOptions.exchanges}
                onApply={setExchangeFilter}
                subscriptionCount={getSubscriptionCount('exchange')}
                totalCount={availableFilters.exchanges.reduce((sum, e) => sum + e.count, 0) || filterOptions.exchanges.length}
              />
              <FilterChip
                type="sector"
                label="Sector"
                values={sectorFilter}
                icon="business-outline"
                options={availableFilters.sectors.length > 0
                  ? availableFilters.sectors.filter(s => s.count > 0).map(s => ({ value: s.value, label: `${s.value} (${s.count})` }))
                  : filterOptions.sectors}
                onApply={setSectorFilter}
                subscriptionCount={getSubscriptionCount('sector')}
                totalCount={availableFilters.sectors.reduce((sum, s) => sum + s.count, 0) || filterOptions.sectors.length}
              />
              <FilterChip
                type="industry"
                label="Industry"
                values={industryFilter}
                icon="briefcase-outline"
                options={availableFilters.industries.length > 0
                  ? availableFilters.industries.filter(i => i.count > 0).map(i => ({ value: i.value, label: `${i.value} (${i.count})` }))
                  : filterOptions.industries}
                onApply={setIndustryFilter}
                subscriptionCount={getSubscriptionCount('industry')}
                totalCount={availableFilters.industries.reduce((sum, i) => sum + i.count, 0) || filterOptions.industries.length}
              />
              <FilterChip
                type="symbol"
                label="Company"
                values={symbolFilter}
                icon="search-outline"
                options={availableFilters.filteredTickers.map(t => {
                  // Display canonical symbol (without .US suffix)
                  const canonicalSymbol = t.symbol.replace('.US', '').toUpperCase();
                  return { 
                    value: t.symbol,
                    label: `${canonicalSymbol} - ${t.name}`,
                    logo_url: t.logo_url,
                  };
                })}
                onApply={setSymbolFilter}
                subscriptionCount={getSubscriptionCount('symbol')}
                totalCount={availableFilters.companyCount}
              />
            </ScrollView>
          </View>
        )}
      </View>

      {/* Posts list */}
      <ScrollView
        style={styles.content}
        refreshControl={<RefreshControl refreshing={refreshing} onRefresh={onRefresh} />}
        showsVerticalScrollIndicator={false}
      >
        {loading && posts.length === 0 ? (
          <BrandedLoading message="Loading community posts..." />
        ) : posts.length === 0 ? (
          <View style={styles.emptyContainer}>
            <Ionicons name="chatbubbles-outline" size={48} color={COLORS.textMuted} />
            <Text style={styles.emptyText}>No posts yet</Text>
            <Text style={styles.emptySubtext}>Be the first to share your thoughts!</Text>
          </View>
        ) : (
          <>
            {posts.map(renderPost)}
            {hasMore && (
              <TouchableOpacity style={styles.loadMoreButton} onPress={loadMore} disabled={loading}>
                {loading ? (
                  <ActivityIndicator size="small" color={COLORS.primary} />
                ) : (
                  <Text style={styles.loadMoreText}>Load more</Text>
                )}
              </TouchableOpacity>
            )}
          </>
        )}
        <View style={{ height: 100 }} />
      </ScrollView>

      {/* Dropdown Modal - Multi-select with Apply button */}
      <Modal
        visible={dropdownModal !== null}
        animationType="slide"
        transparent={true}
        onRequestClose={() => { setDropdownModal(null); setDropdownSearch(''); setDropdownSelected([]); setShowOnlySubscribed(false); }}
      >
        <View style={styles.dropdownModalOverlay}>
          <TouchableOpacity
            style={styles.dropdownModalBackground}
            activeOpacity={1}
            onPress={() => { setDropdownModal(null); setDropdownSearch(''); setDropdownSelected([]); setShowOnlySubscribed(false); }}
          />
          {dropdownModal && (
            <View style={styles.dropdownSheet}>
              {/* Header with title */}
              <View style={styles.dropdownSheetHeader}>
                <View style={{ flex: 1 }}>
                  <Text style={styles.dropdownSheetTitle}>{dropdownModal.title}</Text>
                </View>
                <TouchableOpacity onPress={() => { setDropdownModal(null); setDropdownSearch(''); setDropdownSelected([]); setShowOnlySubscribed(false); }} data-testid="dropdown-close-btn">
                  <Ionicons name="close" size={24} color={COLORS.text} />
                </TouchableOpacity>
              </View>
              
              {/* Search bar with Subscribed only chip */}
              <View style={styles.dropdownSearchRow}>
                <View style={styles.dropdownSearchContainer}>
                  <Ionicons name="search" size={18} color={COLORS.textMuted} />
                  <TextInput
                    style={styles.dropdownSearchInput}
                    placeholder={
                      dropdownModal.type === 'symbol' 
                        ? "Search ticker or company..." 
                        : `Search ${dropdownModal.title.toLowerCase()}...`
                    }
                    placeholderTextColor={COLORS.textMuted}
                    value={dropdownSearch}
                    onChangeText={setDropdownSearch}
                    autoCapitalize="none"
                    autoCorrect={false}
                  />
                  {dropdownSearch.length > 0 && (
                    <TouchableOpacity onPress={() => setDropdownSearch('')}>
                      <Ionicons name="close-circle" size={18} color={COLORS.textMuted} />
                    </TouchableOpacity>
                  )}
                </View>
                
                {/* Subscribed only chip - available for all filter types */}
                {getSubscriptionCount(dropdownModal.type) > 0 && (
                  <Pressable 
                    style={({ pressed }) => [
                      styles.subscribedChip,
                      showOnlySubscribed && styles.subscribedChipActive,
                      pressed && { opacity: 0.7 }
                    ]}
                    onPress={() => setShowOnlySubscribed(!showOnlySubscribed)}
                  >
                    <Ionicons 
                      name={showOnlySubscribed ? "notifications" : "notifications-outline"} 
                      size={14} 
                      color={showOnlySubscribed ? "#fff" : COLORS.subscribed} 
                    />
                    <Text style={[
                      styles.subscribedChipText,
                      showOnlySubscribed && styles.subscribedChipTextActive
                    ]}>
                      {showOnlySubscribed 
                        ? `Subscribed only (${getSubscriptionCount(dropdownModal.type)})` 
                        : 'Subscribed only'}
                    </Text>
                  </Pressable>
                )}
              </View>
              
              <ScrollView style={styles.dropdownSheetList} data-testid="dropdown-options-list">
                {/* Selected items section - shown at top */}
                {dropdownSelected.length > 0 && (
                  <View style={styles.selectedItemsSection}>
                    <View style={styles.selectedItemsHeader}>
                      <Text style={styles.selectedItemsTitle}>Selected ({dropdownSelected.length})</Text>
                      <TouchableOpacity onPress={() => setDropdownSelected([])} style={styles.clearAllBtn}>
                        <Ionicons name="close-circle" size={16} color={COLORS.danger} />
                        <Text style={styles.clearAllText}>Clear all</Text>
                      </TouchableOpacity>
                    </View>
                    {dropdownSelected.map((selectedValue) => {
                      const opt = dropdownModal.options.find(o => o.value === selectedValue);
                      if (!opt) return null;
                      const itemSubscribed = isSubscribed(dropdownModal.type, opt.value);
                      return (
                        <View key={`selected-${opt.value}`} style={styles.selectedItemRow}>
                          <Pressable
                            style={({ pressed }) => [styles.selectedItemContent, pressed && { opacity: 0.7 }]}
                            onPress={() => toggleDropdownItem(opt.value)}
                          >
                            <View style={[styles.dropdownCheckbox, styles.dropdownCheckboxSelected]}>
                              <Ionicons name="checkmark" size={14} color="#fff" />
                            </View>
                            <Text style={styles.selectedItemText} numberOfLines={1}>{opt.label}</Text>
                          </Pressable>
                          {dropdownModal.type === 'symbol' && (
                            <Pressable
                              style={[styles.dropdownBellSmall, itemSubscribed && styles.dropdownBellActive]}
                              onPress={() => toggleSubscription(dropdownModal.type, opt.value)}
                            >
                              <Ionicons
                                name={itemSubscribed ? "notifications" : "notifications-outline"}
                                size={16}
                                color={itemSubscribed ? COLORS.subscribed : COLORS.textMuted}
                              />
                            </Pressable>
                          )}
                        </View>
                      );
                    })}
                  </View>
                )}
                
                {/* Clear all / All option - hidden when showOnlySubscribed is active for symbol filter */}
                {!(dropdownModal.type === 'symbol' && showOnlySubscribed) && (
                  <TouchableOpacity
                    style={[styles.dropdownSheetItem, dropdownSelected.length === 0 && !showOnlySubscribed && styles.dropdownSheetItemActive]}
                    onPress={clearDropdownSelection}
                    data-testid={`dropdown-option-all-${dropdownModal.type}`}
                  >
                    <Ionicons name="globe-outline" size={20} color={COLORS.textLight} style={styles.dropdownItemIcon} />
                    <Text style={styles.dropdownSheetItemText}>
                      All {getPluralTitle(dropdownModal.title)} ({(() => {
                        // Sum the ticker counts from labels for all filter types
                        let total = 0;
                        dropdownModal.options.forEach(opt => {
                          const match = opt.label.match(/\((\d+)\)$/);
                          if (match) total += parseInt(match[1], 10);
                        });
                        // If no counts found in labels, fall back to options length
                        return total > 0 ? total : dropdownModal.options.length;
                      })()})
                    </Text>
                  </TouchableOpacity>
                )}
                
                {/* Options - multi-select with checkmarks */}
                {/* Sort selected items to top, then apply filters */}
                {(() => {
                  // Filter options based on search and subscription toggle
                  let filteredOptions = dropdownModal.options.filter(opt => {
                    // Check subscription toggle for all filter types
                    if (showOnlySubscribed) {
                      const subscribed = isSubscribed(dropdownModal.type, opt.value);
                      if (!subscribed) {
                        return false;
                      }
                    }
                    // Apply search filter for all types
                    if (dropdownSearch.length > 0) {
                      const searchLower = dropdownSearch.toLowerCase();
                      return opt.value.toLowerCase().includes(searchLower) ||
                             opt.label.toLowerCase().includes(searchLower);
                    }
                    return true;
                  });
                  
                  // Sort selected items to top - create new array to force re-render
                  const sortedOptions = filteredOptions
                    .map((opt, idx) => ({ ...opt, originalIdx: idx }))
                    .sort((a, b) => {
                      const aSelected = dropdownSelected.includes(a.value);
                      const bSelected = dropdownSelected.includes(b.value);
                      if (aSelected && !bSelected) return -1;
                      if (!aSelected && bSelected) return 1;
                      return a.originalIdx - b.originalIdx;
                    });
                  
                  return sortedOptions.slice(0, dropdownModal.type === 'symbol' ? 100 : undefined).map((opt, sortedIdx) => {
                  const isSelected = dropdownSelected.includes(opt.value);
                  const optCanonical = canonicalizeLocal(opt.value, dropdownModal.type);
                  const itemSubscribed = isSubscribed(dropdownModal.type, opt.value);
                  const isLoadingItem = subscriptionLoading === `${dropdownModal.type}:${optCanonical}`;
                  
                  return (
                    <View key={opt.value} style={styles.dropdownItemRow}>
                      {/* Main item - tap to select/deselect */}
                      <Pressable
                        style={({ pressed }) => [
                          styles.dropdownSheetItem,
                          styles.dropdownSheetItemFlex,
                          isSelected && styles.dropdownSheetItemSelected,
                          pressed && { opacity: 0.7 },
                        ]}
                        onPress={() => {
                          console.log('Checkbox pressed for:', opt.value);
                          toggleDropdownItem(opt.value);
                        }}
                        data-testid={`dropdown-option-${opt.value.replace(/\s+/g, '-').toLowerCase()}`}
                      >
                        {/* Checkbox circle */}
                        <View style={[styles.dropdownCheckbox, isSelected && styles.dropdownCheckboxSelected]}>
                          {isSelected && <Ionicons name="checkmark" size={14} color="#fff" />}
                        </View>
                        
                        {/* Logo/Icon - show logo if available, otherwise ticker letter placeholder */}
                        {dropdownModal.type === 'symbol' ? (
                          opt.logo_url ? (
                            <Image 
                              source={{ uri: opt.logo_url.startsWith('http') ? opt.logo_url : `${API_URL}${opt.logo_url}` }} 
                              style={styles.companyLogo}
                              onError={() => {
                                // Logo failed to load - logged for debugging
                                console.log(`Logo failed: ${opt.value}`);
                              }}
                            />
                          ) : (
                            <View style={styles.companyLogoPlaceholder}>
                              <Text style={styles.companyLogoText}>
                                {(opt.value || '?').replace('.US', '').charAt(0).toUpperCase()}
                              </Text>
                            </View>
                          )
                        ) : opt.icon ? (
                          <Text style={styles.dropdownItemEmoji}>{opt.icon}</Text>
                        ) : (
                          <Ionicons 
                            name={getItemIcon(dropdownModal.type, opt.value) as any} 
                            size={18} 
                            color={COLORS.textLight} 
                            style={styles.dropdownItemIcon} 
                          />
                        )}
                        
                        <Text style={[
                          styles.dropdownSheetItemText,
                          isSelected && styles.dropdownSheetItemTextSelected,
                        ]} numberOfLines={1}>
                          {opt.label}
                        </Text>
                      </Pressable>
                      
                      {/* Bell for subscribing - separate action */}
                      <Pressable
                        style={({ pressed }) => [
                          styles.dropdownBell, 
                          itemSubscribed && styles.dropdownBellActive,
                          pressed && { opacity: 0.6, transform: [{ scale: 0.95 }] }
                        ]}
                        onPress={(e) => {
                          if (e && e.stopPropagation) e.stopPropagation();
                          if (e && e.preventDefault) e.preventDefault();
                          console.log('Bell clicked for:', opt.value, 'subscribed:', itemSubscribed);
                          toggleSubscription(dropdownModal.type, opt.value, e);
                        }}
                        disabled={isLoadingItem}
                        accessibilityRole="button"
                        accessibilityLabel={itemSubscribed ? `Unsubscribe from ${opt.value}` : `Subscribe to ${opt.value}`}
                      >
                        {isLoadingItem ? (
                          <ActivityIndicator size={14} color={itemSubscribed ? COLORS.subscribed : COLORS.textMuted} />
                        ) : (
                          <Ionicons
                            name={itemSubscribed ? "notifications" : "notifications-outline"}
                            size={20}
                            color={itemSubscribed ? COLORS.subscribed : COLORS.textMuted}
                          />
                        )}
                      </Pressable>
                    </View>
                  );
                });
                })()}
              </ScrollView>
              
              {/* Footer with Apply button */}
              <View style={styles.dropdownFooter}>
                <TouchableOpacity 
                  style={styles.dropdownClearBtn}
                  onPress={clearDropdownSelection}
                >
                  <Text style={styles.dropdownClearText}>Clear</Text>
                </TouchableOpacity>
                <TouchableOpacity 
                  style={styles.dropdownApplyBtn}
                  onPress={applyDropdownSelection}
                >
                  <Text style={styles.dropdownApplyText}>
                    Apply{dropdownSelected.length > 0 ? ` (${dropdownSelected.length})` : ''}
                  </Text>
                </TouchableOpacity>
              </View>
            </View>
          )}
        </View>
      </Modal>

      {/* Create Post Modal */}
      <Modal
        visible={showCreateModal}
        animationType="slide"
        transparent={true}
        onRequestClose={() => { setShowCreateModal(false); setPostError(null); }}
      >
        <View style={styles.modalOverlay}>
          <View style={styles.modalContent}>
            <View style={styles.modalHeader}>
              <Text style={styles.modalTitle}>New Post</Text>
              <TouchableOpacity onPress={() => { setShowCreateModal(false); setPostError(null); }}>
                <Ionicons name="close" size={24} color={COLORS.text} />
              </TouchableOpacity>
            </View>
            
            {/* Error message */}
            {postError && (
              <View style={styles.errorBanner}>
                <Ionicons name="alert-circle" size={16} color={COLORS.danger} />
                <Text style={styles.errorText}>{postError}</Text>
              </View>
            )}
            
            <TextInput
              style={styles.postInput}
              placeholder="What's on your mind?"
              value={newPostText}
              onChangeText={(text) => { setNewPostText(text); setPostError(null); }}
              multiline
              maxLength={500}
              placeholderTextColor={COLORS.textMuted}
              data-testid="post-text-input"
            />
            
            {/* Selected symbols as chips */}
            {selectedSymbols.length > 0 && (
              <View style={styles.selectedSymbolsContainer}>
                {selectedSymbols.map((symbol) => (
                  <View key={symbol} style={styles.symbolChip}>
                    <Text style={styles.symbolChipText}>${symbol}</Text>
                    <TouchableOpacity onPress={() => removeSymbol(symbol)} style={styles.symbolChipRemove}>
                      <Ionicons name="close" size={14} color={COLORS.primary} />
                    </TouchableOpacity>
                  </View>
                ))}
              </View>
            )}
            
            {/* Symbol input with autocomplete - only show if less than MAX_TICKERS */}
            {selectedSymbols.length < MAX_TICKERS && (
              <View style={styles.symbolInputWrapper}>
                <View style={styles.symbolInputContainer}>
                  <Ionicons name="search" size={18} color={COLORS.textMuted} />
                  {Platform.OS === 'web' ? (
                    <input
                      type="text"
                      placeholder={selectedSymbols.length > 0 ? "Add another ticker..." : "Add a company ticker e.g. AAPL, GOOG"}
                      value={symbolSearchText}
                      onChange={(e) => handleSymbolSearch(e.target.value.toUpperCase())}
                      style={{
                        flex: 1,
                        fontSize: 15,
                        color: COLORS.text,
                        padding: 0,
                        border: 'none',
                        outline: 'none',
                        backgroundColor: 'transparent',
                        fontFamily: 'inherit',
                      }}
                      maxLength={10}
                      data-testid="post-symbol-input"
                    />
                  ) : (
                    <TextInput
                      style={styles.symbolInputField}
                      placeholder={selectedSymbols.length > 0 ? "Add another ticker..." : "Add a company ticker e.g. AAPL, GOOG"}
                      value={symbolSearchText}
                      onChangeText={(text) => handleSymbolSearch(text.toUpperCase())}
                      autoCapitalize="characters"
                      maxLength={10}
                      placeholderTextColor={COLORS.textMuted}
                      data-testid="post-symbol-input"
                    />
                  )}
                  {symbolSearchText.length > 0 && (
                    <TouchableOpacity onPress={() => { setSymbolSearchText(''); setShowSymbolSuggestions(false); }}>
                      <Ionicons name="close-circle" size={18} color={COLORS.textMuted} />
                    </TouchableOpacity>
                  )}
                </View>
                
                {/* Autocomplete suggestions dropdown */}
                {showSymbolSuggestions && symbolSuggestions.length > 0 && (
                  <View style={styles.symbolSuggestions}>
                    <ScrollView style={styles.symbolSuggestionsList} keyboardShouldPersistTaps="handled">
                      {symbolSuggestions.map((item) => (
                        <TouchableOpacity
                          key={item.symbol}
                          style={styles.symbolSuggestionItem}
                          onPress={() => selectSymbol(item.symbol)}
                        >
                          {item.logo_url ? (
                            <Image source={{ uri: item.logo_url.startsWith('http') ? item.logo_url : `${API_URL}${item.logo_url}` }} style={styles.suggestionLogo} />
                          ) : (
                            <View style={styles.suggestionLogoPlaceholder}>
                              <Text style={styles.suggestionLogoText}>
                                {item.symbol.replace('.US', '').charAt(0)}
                              </Text>
                            </View>
                          )}
                          <View style={styles.suggestionTextContainer}>
                            <Text style={styles.suggestionSymbol}>{item.symbol.replace('.US', '')}</Text>
                            <Text style={styles.suggestionName} numberOfLines={1}>{item.name}</Text>
                          </View>
                        </TouchableOpacity>
                    ))}
                  </ScrollView>
                </View>
              )}
              </View>
            )}
            
            <TouchableOpacity
              style={[styles.postButton, (newPostText.trim().length < 10 || posting) && styles.postButtonDisabled]}
              onPress={createPost}
              disabled={newPostText.trim().length < 10 || posting}
              data-testid="post-submit-btn"
            >
              {posting ? (
                <ActivityIndicator size="small" color="#FFFFFF" />
              ) : (
                <Text style={styles.postButtonText}>Post</Text>
              )}
            </TouchableOpacity>
            {newPostText.trim().length > 0 && newPostText.trim().length < 10 && (
              <Text style={styles.charCountHint}>Minimum 10 characters required</Text>
            )}
          </View>
        </View>
      </Modal>
      
      {/* Edit Post Modal */}
      <Modal
        visible={!!editingPost}
        animationType="slide"
        transparent={true}
        onRequestClose={() => setEditingPost(null)}
      >
        <View style={styles.modalOverlay}>
          <View style={styles.editModalContent}>
            <View style={styles.modalHeader}>
              <Text style={styles.modalTitle}>Edit Post</Text>
              <TouchableOpacity onPress={() => setEditingPost(null)}>
                <Ionicons name="close" size={24} color={COLORS.text} />
              </TouchableOpacity>
            </View>
            
            <TextInput
              style={styles.editTextInput}
              value={editText}
              onChangeText={setEditText}
              multiline
              maxLength={2000}
              placeholderTextColor={COLORS.textMuted}
              data-testid="edit-text-input"
            />
            
            <View style={styles.editModalFooter}>
              <Text style={styles.editCharCount}>
                {editText.trim().length}/2000 characters
              </Text>
              <View style={styles.editModalButtons}>
                <TouchableOpacity 
                  style={styles.editCancelButton}
                  onPress={() => setEditingPost(null)}
                >
                  <Text style={styles.editCancelButtonText}>Cancel</Text>
                </TouchableOpacity>
                <TouchableOpacity 
                  style={[styles.editSaveButton, editText.trim().length < 10 && styles.postButtonDisabled]}
                  onPress={saveEditPost}
                  disabled={editText.trim().length < 10 || editLoading}
                >
                  {editLoading ? (
                    <ActivityIndicator size="small" color="#FFF" />
                  ) : (
                    <Text style={styles.editSaveButtonText}>Save</Text>
                  )}
                </TouchableOpacity>
              </View>
            </View>
          </View>
        </View>
      </Modal>
      
      {/* Delete Confirmation Modal */}
      <Modal
        visible={!!deleteConfirmPost}
        animationType="fade"
        transparent={true}
        onRequestClose={() => setDeleteConfirmPost(null)}
      >
        <View style={styles.deleteModalOverlay}>
          <View style={styles.deleteModalContent}>
            <View style={styles.deleteModalIcon}>
              <Ionicons name="trash-outline" size={32} color={COLORS.danger} />
            </View>
            <Text style={styles.deleteModalTitle}>Delete Post?</Text>
            <Text style={styles.deleteModalText}>
              Are you sure you want to delete this post? This action cannot be undone.
            </Text>
            <View style={styles.deleteModalButtons}>
              <TouchableOpacity 
                style={styles.deleteCancelButton}
                onPress={() => setDeleteConfirmPost(null)}
              >
                <Text style={styles.deleteCancelButtonText}>Cancel</Text>
              </TouchableOpacity>
              <TouchableOpacity 
                style={styles.deleteConfirmButton}
                onPress={confirmDelete}
              >
                <Text style={styles.deleteConfirmButtonText}>Delete</Text>
              </TouchableOpacity>
            </View>
          </View>
        </View>
      </Modal>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.background },
  header: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    paddingHorizontal: 16,
    paddingTop: 8,
    paddingBottom: 12,
    backgroundColor: COLORS.card,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  title: { fontSize: 28, fontWeight: '700', color: COLORS.text },
  subtitle: { fontSize: 14, color: COLORS.textLight, marginTop: 2 },
  clearFiltersBtn: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
    paddingVertical: 6,
    paddingHorizontal: 10,
    backgroundColor: '#FEE2E2',
    borderRadius: 16,
  },
  clearFiltersText: { fontSize: 12, fontWeight: '500', color: COLORS.danger },
  headerRight: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 10,
  },
  headerFab: {
    width: 40,
    height: 40,
    borderRadius: 20,
    backgroundColor: COLORS.primary,
    justifyContent: 'center',
    alignItems: 'center',
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.15,
    shadowRadius: 3,
    elevation: 3,
  },
  
  // Filter bar - Livesport style
  filterBar: {
    backgroundColor: COLORS.card,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
    paddingVertical: 10,
  },
  filterScrollWrapper: {
    flexDirection: 'row',
    alignItems: 'center',
  },
  filterScrollContent: {
    paddingHorizontal: 12,
    gap: 8,
    paddingRight: 16,
  },
  filterError: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 8,
    paddingVertical: 8,
  },
  filterErrorText: { fontSize: 13, color: COLORS.danger },
  retryText: { fontSize: 13, color: COLORS.primary, fontWeight: '600' },
  
  // Filter chip - simplified without bells
  filterChip: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
    paddingHorizontal: 12,
    paddingVertical: 8,
    backgroundColor: COLORS.background,
    borderRadius: 20,
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  filterChipActive: {
    backgroundColor: COLORS.primary,
    borderColor: COLORS.primary,
  },
  filterChipText: {
    fontSize: 13,
    fontWeight: '500',
    color: COLORS.text,
    maxWidth: 80,
  },
  filterChipTextActive: { color: '#FFFFFF' },
  flagImage: { width: 20, height: 14, borderRadius: 2 },
  
  // Subscription count badge on filter chip
  subscriptionCountBadge: {
    backgroundColor: COLORS.subscribed + '20',
    paddingHorizontal: 6,
    paddingVertical: 2,
    borderRadius: 10,
    marginLeft: 4,
  },
  subscriptionCountBadgeActive: {
    backgroundColor: 'rgba(255, 255, 255, 0.25)',
  },
  subscriptionCountText: {
    fontSize: 11,
    fontWeight: '600',
    color: COLORS.subscribed,
  },
  subscriptionCountTextActive: {
    color: '#FFFFFF',
  },
  
  // Content
  content: { flex: 1, paddingHorizontal: 16, paddingTop: 16 },
  loadingContainer: { flex: 1, justifyContent: 'center', alignItems: 'center', paddingTop: 100 },
  emptyContainer: { flex: 1, justifyContent: 'center', alignItems: 'center', paddingTop: 100 },
  emptyText: { fontSize: 18, fontWeight: '600', color: COLORS.text, marginTop: 16 },
  emptySubtext: { fontSize: 14, color: COLORS.textMuted, marginTop: 4 },
  
  // Post card
  postCard: {
    backgroundColor: COLORS.card,
    borderRadius: 12,
    padding: 16,
    marginBottom: 12,
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  postHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    marginBottom: 12,
  },
  postHeaderRight: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  postMenuButton: {
    padding: 4,
  },
  postMenuDropdown: {
    position: 'absolute',
    top: 45,
    right: 16,
    backgroundColor: COLORS.card,
    borderRadius: 8,
    borderWidth: 1,
    borderColor: COLORS.border,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.1,
    shadowRadius: 4,
    elevation: 5,
    zIndex: 100,
  },
  postMenuItem: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingVertical: 10,
    paddingHorizontal: 16,
    gap: 10,
  },
  postMenuItemDanger: {
    borderTopWidth: 1,
    borderTopColor: COLORS.border,
  },
  postMenuItemText: {
    fontSize: 14,
    color: COLORS.text,
  },
  editedBadge: {
    color: COLORS.textMuted,
    fontStyle: 'italic',
  },
  userInfo: { flexDirection: 'row', alignItems: 'center', flex: 1 },
  avatar: { width: 36, height: 36, borderRadius: 18, marginRight: 10 },
  avatarPlaceholder: {
    width: 36,
    height: 36,
    borderRadius: 18,
    backgroundColor: COLORS.background,
    justifyContent: 'center',
    alignItems: 'center',
    marginRight: 10,
  },
  userMeta: { flex: 1 },
  userName: { fontSize: 14, fontWeight: '600', color: COLORS.text },
  rrrBadge: {
    backgroundColor: COLORS.accent + '20',
    paddingHorizontal: 6,
    paddingVertical: 2,
    borderRadius: 4,
    alignSelf: 'flex-start',
    marginTop: 2,
  },
  rrrText: { fontSize: 11, fontWeight: '600', color: COLORS.accent },
  postTime: { fontSize: 12, color: COLORS.textMuted },
  postText: { fontSize: 15, lineHeight: 22, color: COLORS.text },
  symbolTagsRow: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 8,
    marginTop: 12,
  },
  symbolTag: {
    backgroundColor: COLORS.primary + '15',
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 6,
    alignSelf: 'flex-start',
  },
  symbolTagText: { fontSize: 13, fontWeight: '600', color: COLORS.primary },
  loadMoreButton: {
    backgroundColor: COLORS.card,
    borderRadius: 8,
    paddingVertical: 12,
    alignItems: 'center',
    marginTop: 8,
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  loadMoreText: { fontSize: 14, fontWeight: '500', color: COLORS.primary },
  
  // Dropdown Modal overlay
  dropdownModalOverlay: {
    flex: 1,
    justifyContent: 'flex-end',
    backgroundColor: 'transparent',
  },
  dropdownModalBackground: {
    position: 'absolute',
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    backgroundColor: 'rgba(0,0,0,0.5)',
  },
  dropdownSheet: {
    backgroundColor: COLORS.card,
    borderTopLeftRadius: 24,
    borderTopRightRadius: 24,
    maxHeight: '85%',
  },
  dropdownSheetHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    padding: 20,
    paddingBottom: 16,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  dropdownSheetTitle: { fontSize: 20, fontWeight: '700', color: COLORS.text },
  dropdownSelectionCount: { fontSize: 13, color: COLORS.textMuted, marginTop: 2 },
  selectionInfoRow: {
    flexDirection: 'row',
    alignItems: 'center',
    marginTop: 4,
    gap: 12,
  },
  clearSelectionBtn: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingVertical: 4,
    paddingHorizontal: 8,
    backgroundColor: COLORS.danger + '15',
    borderRadius: 12,
    gap: 4,
  },
  clearSelectionText: {
    fontSize: 12,
    fontWeight: '600',
    color: COLORS.danger,
  },
  
  // Selected items section at top of dropdown
  selectedItemsSection: {
    backgroundColor: COLORS.primary + '08',
    borderBottomWidth: 2,
    borderBottomColor: COLORS.primary + '30',
    paddingBottom: 8,
    marginBottom: 8,
  },
  selectedItemsHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    paddingHorizontal: 16,
    paddingVertical: 10,
  },
  selectedItemsTitle: {
    fontSize: 13,
    fontWeight: '700',
    color: COLORS.primary,
    textTransform: 'uppercase',
    letterSpacing: 0.5,
  },
  clearAllBtn: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingVertical: 4,
    paddingHorizontal: 10,
    backgroundColor: COLORS.danger + '15',
    borderRadius: 12,
    gap: 4,
  },
  clearAllText: {
    fontSize: 12,
    fontWeight: '600',
    color: COLORS.danger,
  },
  selectedItemRow: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 16,
    paddingVertical: 8,
    backgroundColor: COLORS.primary + '10',
    marginHorizontal: 8,
    marginVertical: 2,
    borderRadius: 8,
  },
  selectedItemContent: {
    flex: 1,
    flexDirection: 'row',
    alignItems: 'center',
  },
  selectedItemText: {
    fontSize: 14,
    color: COLORS.text,
    fontWeight: '500',
    marginLeft: 10,
    flex: 1,
  },
  dropdownBellSmall: {
    width: 36,
    height: 36,
    justifyContent: 'center',
    alignItems: 'center',
  },
  
  dropdownSubscribedCount: { 
    fontSize: 13, 
    color: COLORS.subscribed, 
    fontWeight: '600',
    marginTop: 2,
  },
  
  // Show only subscribed toggle
  // Search row with subscribed chip
  dropdownSearchRow: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 16,
    paddingVertical: 8,
    gap: 8,
  },
  dropdownSearchContainer: {
    flex: 1,
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: COLORS.background,
    paddingHorizontal: 12,
    paddingVertical: 10,
    borderRadius: 10,
    gap: 8,
  },
  dropdownSearchInput: {
    flex: 1,
    fontSize: 14,
    color: COLORS.text,
    padding: 0,
    borderWidth: 0,
    outlineWidth: 0,
    outlineStyle: 'none',
  },
  subscribedChip: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 10,
    paddingVertical: 8,
    borderRadius: 16,
    borderWidth: 1.5,
    borderColor: COLORS.subscribed,
    backgroundColor: 'transparent',
    gap: 4,
  },
  subscribedChipActive: {
    backgroundColor: COLORS.subscribed,
    borderColor: COLORS.subscribed,
  },
  subscribedChipText: {
    fontSize: 12,
    fontWeight: '600',
    color: COLORS.subscribed,
  },
  subscribedChipTextActive: {
    color: '#fff',
  },
  
  dropdownFlagImage: { width: 24, height: 16, borderRadius: 2, marginRight: 12 },
  companyLogo: { 
    width: 28, 
    height: 28, 
    borderRadius: 6, 
    marginRight: 12,
    resizeMode: 'contain',
  },
  companyLogoPlaceholder: {
    width: 28,
    height: 28,
    borderRadius: 6,
    marginRight: 12,
    backgroundColor: COLORS.primary + '15',
    justifyContent: 'center',
    alignItems: 'center',
  },
  companyLogoText: {
    fontSize: 10,
    fontWeight: '700',
    color: COLORS.primary,
  },
  dropdownSheetList: { maxHeight: 380 },
  dropdownSheetItem: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingVertical: 14,
    paddingHorizontal: 20,
  },
  dropdownSheetItemFlex: {
    flex: 1,
  },
  dropdownSheetItemActive: { backgroundColor: COLORS.primary + '08' },
  dropdownSheetItemSelected: { backgroundColor: COLORS.primary + '12' },
  
  // Checkbox for multi-select
  dropdownCheckbox: {
    width: 22,
    height: 22,
    borderRadius: 6,
    borderWidth: 2,
    borderColor: COLORS.border,
    marginRight: 14,
    justifyContent: 'center',
    alignItems: 'center',
  },
  dropdownCheckboxSelected: {
    backgroundColor: COLORS.primary,
    borderColor: COLORS.primary,
  },
  
  dropdownSheetItemText: { flex: 1, fontSize: 15, color: COLORS.text },
  dropdownSheetItemTextActive: { color: COLORS.primary, fontWeight: '600' },
  dropdownSheetItemTextSelected: { color: COLORS.text, fontWeight: '500' },
  dropdownItemIcon: { marginRight: 14 },
  dropdownItemEmoji: { fontSize: 18, marginRight: 14 },
  dropdownItemRow: {
    flexDirection: 'row',
    alignItems: 'center',
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  dropdownBell: {
    width: 52,
    height: 52,
    justifyContent: 'center',
    alignItems: 'center',
    zIndex: 10,
    cursor: 'pointer',
  },
  dropdownBellActive: {
    backgroundColor: COLORS.subscribed + '15',
  },
  
  // Footer with Apply button
  dropdownFooter: {
    flexDirection: 'row',
    padding: 16,
    paddingBottom: 34,
    borderTopWidth: 1,
    borderTopColor: COLORS.border,
    gap: 12,
  },
  dropdownClearBtn: {
    flex: 1,
    paddingVertical: 14,
    borderRadius: 12,
    borderWidth: 1,
    borderColor: COLORS.border,
    alignItems: 'center',
  },
  dropdownClearText: { fontSize: 16, fontWeight: '600', color: COLORS.textMuted },
  dropdownApplyBtn: {
    flex: 2,
    paddingVertical: 14,
    borderRadius: 12,
    backgroundColor: COLORS.primary,
    alignItems: 'center',
  },
  dropdownApplyText: { fontSize: 16, fontWeight: '600', color: '#FFFFFF' },
  dropdownSheetItemText: { flex: 1, fontSize: 15, color: COLORS.text },
  dropdownSheetItemTextActive: { color: COLORS.primary, fontWeight: '600' },
  dropdownItemIcon: { marginRight: 12 },
  dropdownItemEmoji: { fontSize: 18, marginRight: 12 },
  dropdownItemRow: {
    flexDirection: 'row',
    alignItems: 'center',
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  dropdownBell: {
    width: 44,
    height: 48,
    justifyContent: 'center',
    alignItems: 'center',
    borderLeftWidth: 1,
    borderLeftColor: COLORS.border,
  },
  dropdownBellActive: {
    backgroundColor: COLORS.subscribed + '15',
  },
  
  // Create Modal
  modalOverlay: {
    flex: 1,
    backgroundColor: 'rgba(0,0,0,0.5)',
    justifyContent: 'flex-end',
  },
  modalContent: {
    backgroundColor: COLORS.card,
    borderTopLeftRadius: 20,
    borderTopRightRadius: 20,
    padding: 20,
    paddingTop: 16,
    flex: 1,
    marginTop: 60,
  },
  modalHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: 20,
  },
  modalTitle: { fontSize: 20, fontWeight: '600', color: COLORS.text },
  errorBanner: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: '#FEE2E2',
    padding: 12,
    borderRadius: 8,
    marginBottom: 12,
    gap: 8,
  },
  errorText: { flex: 1, fontSize: 14, color: COLORS.danger },
  postInput: {
    backgroundColor: COLORS.background,
    borderRadius: 12,
    padding: 16,
    fontSize: 16,
    color: COLORS.text,
    minHeight: 180,
    textAlignVertical: 'top',
    marginBottom: 12,
  },
  // Selected symbols chips
  selectedSymbolsContainer: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 8,
    marginBottom: 12,
  },
  symbolChip: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: COLORS.primary + '15',
    borderRadius: 20,
    paddingVertical: 6,
    paddingLeft: 12,
    paddingRight: 6,
    gap: 6,
  },
  symbolChipText: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.primary,
  },
  symbolChipRemove: {
    width: 20,
    height: 20,
    borderRadius: 10,
    backgroundColor: COLORS.primary + '20',
    justifyContent: 'center',
    alignItems: 'center',
  },
  symbolInput: {
    backgroundColor: COLORS.background,
    borderRadius: 12,
    padding: 16,
    fontSize: 16,
    color: COLORS.text,
    marginBottom: 16,
  },
  // Symbol autocomplete styles
  symbolInputWrapper: {
    marginBottom: 16,
    zIndex: 10,
  },
  symbolInputContainer: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: COLORS.background,
    borderRadius: 12,
    paddingHorizontal: 14,
    paddingVertical: 12,
    gap: 10,
  },
  symbolInputField: {
    flex: 1,
    fontSize: 15,
    color: COLORS.text,
    padding: 0,
  },
  symbolSuggestions: {
    position: 'absolute',
    top: '100%',
    left: 0,
    right: 0,
    backgroundColor: COLORS.card,
    borderRadius: 12,
    marginTop: 4,
    maxHeight: 200,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 4 },
    shadowOpacity: 0.15,
    shadowRadius: 8,
    elevation: 8,
    borderWidth: 1,
    borderColor: COLORS.border,
    zIndex: 100,
  },
  symbolSuggestionsList: {
    maxHeight: 200,
  },
  symbolSuggestionItem: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingVertical: 10,
    paddingHorizontal: 14,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  suggestionLogo: {
    width: 28,
    height: 28,
    borderRadius: 6,
    marginRight: 12,
  },
  suggestionLogoPlaceholder: {
    width: 28,
    height: 28,
    borderRadius: 6,
    backgroundColor: COLORS.primary + '15',
    justifyContent: 'center',
    alignItems: 'center',
    marginRight: 12,
  },
  suggestionLogoText: {
    fontSize: 12,
    fontWeight: '700',
    color: COLORS.primary,
  },
  suggestionTextContainer: {
    flex: 1,
  },
  suggestionSymbol: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.text,
  },
  suggestionName: {
    fontSize: 12,
    color: COLORS.textMuted,
    marginTop: 1,
  },
  postButton: {
    backgroundColor: COLORS.primary,
    borderRadius: 12,
    paddingVertical: 14,
    alignItems: 'center',
  },
  postButtonDisabled: { backgroundColor: COLORS.textMuted },
  postButtonText: { fontSize: 16, fontWeight: '600', color: '#FFFFFF' },
  charCountHint: { 
    fontSize: 12, 
    color: COLORS.textMuted, 
    textAlign: 'center', 
    marginTop: 8 
  },
  
  // Edit Modal
  editModalContent: {
    backgroundColor: COLORS.card,
    borderTopLeftRadius: 20,
    borderTopRightRadius: 20,
    padding: 20,
    paddingTop: 16,
    maxHeight: '70%',
  },
  editTextInput: {
    backgroundColor: COLORS.background,
    borderRadius: 12,
    padding: 16,
    fontSize: 16,
    color: COLORS.text,
    minHeight: 150,
    textAlignVertical: 'top',
  },
  editModalFooter: {
    marginTop: 16,
  },
  editCharCount: {
    fontSize: 12,
    color: COLORS.textMuted,
    marginBottom: 12,
  },
  editModalButtons: {
    flexDirection: 'row',
    gap: 12,
  },
  editCancelButton: {
    flex: 1,
    paddingVertical: 12,
    borderRadius: 12,
    backgroundColor: COLORS.background,
    alignItems: 'center',
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  editCancelButtonText: {
    fontSize: 15,
    fontWeight: '600',
    color: COLORS.text,
  },
  editSaveButton: {
    flex: 1,
    paddingVertical: 12,
    borderRadius: 12,
    backgroundColor: COLORS.primary,
    alignItems: 'center',
  },
  editSaveButtonText: {
    fontSize: 15,
    fontWeight: '600',
    color: '#FFFFFF',
  },
  
  // Delete Confirmation Modal
  deleteModalOverlay: {
    flex: 1,
    backgroundColor: 'rgba(0, 0, 0, 0.6)',
    justifyContent: 'center',
    alignItems: 'center',
    padding: 24,
  },
  deleteModalContent: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 24,
    width: '100%',
    maxWidth: 340,
    alignItems: 'center',
  },
  deleteModalIcon: {
    width: 64,
    height: 64,
    borderRadius: 32,
    backgroundColor: `${COLORS.danger}15`,
    justifyContent: 'center',
    alignItems: 'center',
    marginBottom: 16,
  },
  deleteModalTitle: {
    fontSize: 18,
    fontWeight: '700',
    color: COLORS.text,
    marginBottom: 8,
  },
  deleteModalText: {
    fontSize: 14,
    color: COLORS.textMuted,
    textAlign: 'center',
    marginBottom: 24,
    lineHeight: 20,
  },
  deleteModalButtons: {
    flexDirection: 'row',
    gap: 12,
    width: '100%',
  },
  deleteCancelButton: {
    flex: 1,
    paddingVertical: 14,
    borderRadius: 12,
    backgroundColor: COLORS.background,
    alignItems: 'center',
  },
  deleteCancelButtonText: {
    fontSize: 15,
    fontWeight: '600',
    color: COLORS.text,
  },
  deleteConfirmButton: {
    flex: 1,
    paddingVertical: 14,
    borderRadius: 12,
    backgroundColor: COLORS.danger,
    alignItems: 'center',
  },
  deleteConfirmButtonText: {
    fontSize: 15,
    fontWeight: '600',
    color: '#FFFFFF',
  },
});
