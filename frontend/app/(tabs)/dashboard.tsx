import React, { useState, useCallback, useEffect, useMemo } from 'react';
import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  RefreshControl,
  ActivityIndicator,
  TouchableOpacity,
  Platform,
  Image,
  Linking,
  Modal,
  TextInput,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useFocusEffect, useRouter } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import axios from 'axios';
import { useAuth } from '../../contexts/AuthContext';
import AppHeader from '../../components/AppHeader';
import { FONTS } from '../_layout';
import { useLayoutSpacing } from '../../constants/layout';
import { API_URL } from '../../utils/config';
import { useMyStocksStore } from '../../stores/myStocksStore';

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
};

const HOMEPAGE_EVENT_SEPARATOR = ' • ';

type HomepageEvent = {
  id: string;
  ticker: string;
  company_name: string;
  logo_url?: string | null;
  event_type: 'Earnings' | 'Dividend' | 'Split';
  title: string;
  date: string;
  before_after_market?: string | null;
  estimate?: number | null;
  currency?: string | null;
  amount?: number | null;
  pay_date?: string | null;
  split_ratio?: string | null;
};

type DashboardFeedItem =
  | { kind: 'event'; id: string; event: HomepageEvent }
  | { kind: 'article'; id: string; article: any };

type HomepageFeedSort = 'date_desc' | 'date_asc' | 'az' | 'za';

const formatDashboardDate = (dateStr?: string | null): string => {
  if (!dateStr) return '';
  if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) return '';
  const parsed = new Date(`${dateStr}T00:00:00Z`);
  if (Number.isNaN(parsed.getTime())) return '';
  return parsed.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
};

const formatDashboardCurrency = (value?: number | string | null, currency?: string | null): string | null => {
  const numericValue = typeof value === 'string' ? Number(value) : value;
  if (typeof numericValue !== 'number' || !Number.isFinite(numericValue)) return null;
  const prefix = currency && currency !== 'USD' ? `${currency} ` : '$';
  return `${prefix}${numericValue.toFixed(2)}`;
};

const getDashboardMarketTimingLabel = (value?: string | null): string | null => {
  if (!value) return null;
  const normalized = value.toLowerCase().replace(/[\s_-]+/g, '');
  if (normalized.startsWith('before')) return 'Before Market';
  if (normalized.startsWith('after')) return 'After Market';
  return null;
};

const formatHomepageEventSubtitle = (event: HomepageEvent): string => {
  if (event.event_type === 'Earnings') {
    const details = [
      formatDashboardCurrency(event.estimate, event.currency) ? `Exp. ${formatDashboardCurrency(event.estimate, event.currency)}` : null,
      getDashboardMarketTimingLabel(event.before_after_market),
    ].filter(Boolean);
    return details.join(HOMEPAGE_EVENT_SEPARATOR) || 'Scheduled earnings';
  }
  if (event.event_type === 'Dividend') {
    const details = [
      formatDashboardCurrency(event.amount, event.currency),
      event.date ? `Ex ${formatDashboardDate(event.date)}` : null,
      event.pay_date ? `Pay ${formatDashboardDate(event.pay_date)}` : null,
    ].filter(Boolean);
    return details.join(HOMEPAGE_EVENT_SEPARATOR) || 'Upcoming dividend';
  }
  return event.split_ratio || 'Upcoming split';
};

const getDashboardFeedDateValue = (item: DashboardFeedItem): number => {
  const rawDate = item.kind === 'event' ? item.event.date : item.article?.date;
  if (!rawDate || typeof rawDate !== 'string') return 0;
  const normalized = /^\d{4}-\d{2}-\d{2}$/.test(rawDate) ? `${rawDate}T00:00:00Z` : rawDate;
  const timestamp = new Date(normalized).getTime();
  return Number.isNaN(timestamp) ? 0 : timestamp;
};

const getDashboardFeedAlphaKey = (item: DashboardFeedItem): string => {
  const primary = item.kind === 'event'
    ? item.event.ticker || item.event.company_name || item.event.title
    : item.article?.ticker || item.article?.company_name || item.article?.title || '';
  return String(primary).toUpperCase();
};

// P31 LOGO GUARANTEE: Component that always renders logo or fallback badge
// DO NOT REMOVE WITHOUT RICHARD APPROVAL (kurtarichard@gmail.com)
const NewsLogo = ({ logoUrl, fallbackKey, ticker }: { logoUrl?: string; fallbackKey: string; ticker?: string }) => {
  const [imageError, setImageError] = useState(false);
  
  // Helper to get consistent color based on ticker
  const getCompanyColor = (symbol: string) => {
    const colors = ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#EC4899', '#06B6D4', '#84CC16'];
    let hash = 0;
    for (let i = 0; i < symbol.length; i++) {
      hash = symbol.charCodeAt(i) + ((hash << 5) - hash);
    }
    return colors[Math.abs(hash) % colors.length];
  };
  
  // If no URL or image failed to load, show fallback badge
  if (!logoUrl || imageError) {
    return (
      <View style={[newsLogoStyles.fallback, { backgroundColor: getCompanyColor(ticker || fallbackKey) }]}>
        <Text style={newsLogoStyles.fallbackText}>{fallbackKey}</Text>
      </View>
    );
  }
  
  return (
    <Image 
      source={{ uri: logoUrl }} 
      style={newsLogoStyles.logo}
      onError={() => setImageError(true)}
    />
  );
};

const newsLogoStyles = StyleSheet.create({
  logo: {
    width: 40,
    height: 40,
    borderRadius: 8,
    backgroundColor: '#F3F4F6',
  },
  fallback: {
    width: 40,
    height: 40,
    borderRadius: 8,
    justifyContent: 'center',
    alignItems: 'center',
  },
  fallbackText: {
    color: '#FFFFFF',
    fontSize: 16,
    fontWeight: '600',
  },
});

// P31+ Stock list logo with onError fallback to letter badge
const StockLogo = ({ logoUrl, ticker, color }: { logoUrl: string | null; ticker: string; color: string }) => {
  const [imageError, setImageError] = useState(false);

  if (!logoUrl || imageError) {
    return (
      <View style={[stockLogoStyles.fallback, { backgroundColor: color }]}>
        <Text style={stockLogoStyles.fallbackText}>{ticker.charAt(0)}</Text>
      </View>
    );
  }

  return (
    <Image
      source={{ uri: logoUrl }}
      style={stockLogoStyles.logo}
      onError={() => setImageError(true)}
    />
  );
};

const stockLogoStyles = StyleSheet.create({
  logo: {
    width: 40,
    height: 40,
    borderRadius: 10,
    backgroundColor: '#F3F4F6',
  },
  fallback: {
    width: 40,
    height: 40,
    borderRadius: 10,
    alignItems: 'center',
    justifyContent: 'center',
  },
  fallbackText: {
    fontSize: 18,
    fontWeight: '700',
    color: '#FFFFFF',
  },
});

export default function Dashboard() {
  const router = useRouter();
  const { user, isAuthenticated, sessionToken } = useAuth();
  const sp = useLayoutSpacing();
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [data, setData] = useState<any>(null);
  const [newsItems, setNewsItems] = useState<any[]>([]);
  const [newsLoading, setNewsLoading] = useState(true);
  const [newsOffset, setNewsOffset] = useState(0);
  const [hasMoreNews, setHasMoreNews] = useState(true);
  const [selectedArticle, setSelectedArticle] = useState<any>(null);
  const [aggregateSentiment, setAggregateSentiment] = useState<any>(null);
  const [homepageFeedSort, setHomepageFeedSort] = useState<HomepageFeedSort>('date_desc');
  const [includeHomepageEvents, setIncludeHomepageEvents] = useState(true);
  const [newsFeedFilter, setNewsFeedFilter] = useState('');
  
  // Fix 3: News pagination with See less
  const INITIAL_NEWS_LIMIT = 5;
  const [newsLimit, setNewsLimit] = useState(INITIAL_NEWS_LIMIT);
  
  // P34 Fix 1: My Stocks pagination state
  const [stocksLimit, setStocksLimit] = useState(5);
  const STOCKS_PAGE_SIZE = 5;
  
  // P35 Item 2: Unfollow confirmation modal
  // Notifications
  const [notificationCount, setNotificationCount] = useState(0);
  const [showNotifications, setShowNotifications] = useState(false);
  const [notifications, setNotifications] = useState<any[]>([]);
  const [notificationsLoading, setNotificationsLoading] = useState(false);
  
  // View as tier (admin feature to switch between subscription views)
  const [viewAsTier, setViewAsTier] = useState<string>(user?.subscription_tier || 'free');
  
  // Update viewAsTier when user changes
  React.useEffect(() => {
    if (user?.subscription_tier) {
      setViewAsTier(user.subscription_tier);
    }
  }, [user?.subscription_tier]);

  // P36 Item 3: My Stocks filter state
  const [stocksFilter, setStocksFilter] = useState('');
  
  // P37++ A) Sort state with ascending/descending options
  // Options: 'date_desc' (default), 'date_asc', 'az', 'za', 'total_desc', 'total_asc', '1d_desc', '1d_asc'
  const [stocksSort, setStocksSort] = useState<string>('date_desc');
  
  // P37++ B) Portfolio toggle - default ON (show both Watchlist + Portfolio)
  const [includePortfolio, setIncludePortfolio] = useState(true);
  
  // P34/P36: My Stocks computed values
  const myStocks = data?.my_stocks || [];
  
  // P37++ Filter and sort stocks
  const filteredStocks = useMemo(() => {
    let stocks = [...myStocks];
    
    // P37++ B) Filter by portfolio toggle
    if (!includePortfolio) {
      // Show only Watchlist items (exclude Portfolio-only)
      stocks = stocks.filter((stock: any) => stock.pill === 'Watchlist' || stock.pill === 'Both');
    }
    
    // Filter by ticker or name search
    if (stocksFilter) {
      const query = stocksFilter.toUpperCase();
      stocks = stocks.filter((stock: any) => 
        stock.ticker?.toUpperCase().includes(query) ||
        stock.name?.toUpperCase().includes(query)
      );
    }
    
    // P37++ A) Sort with ascending/descending
    switch (stocksSort) {
      case 'az':
        stocks.sort((a: any, b: any) => a.ticker.localeCompare(b.ticker));
        break;
      case 'za':
        stocks.sort((a: any, b: any) => b.ticker.localeCompare(a.ticker));
        break;
      case 'total_desc':
        stocks.sort((a: any, b: any) => (b.change_since_added || -999) - (a.change_since_added || -999));
        break;
      case 'total_asc':
        stocks.sort((a: any, b: any) => (a.change_since_added || 999) - (b.change_since_added || 999));
        break;
      case '1d_desc':
        stocks.sort((a: any, b: any) => (b.change_1d_pct || 0) - (a.change_1d_pct || 0));
        break;
      case '1d_asc':
        stocks.sort((a: any, b: any) => (a.change_1d_pct || 0) - (b.change_1d_pct || 0));
        break;
      case 'date_asc':
        // Oldest first
        stocks.sort((a: any, b: any) => {
          const dateA = a.added_at ? new Date(a.added_at.split('/').reverse().join('-')).getTime() : 0;
          const dateB = b.added_at ? new Date(b.added_at.split('/').reverse().join('-')).getTime() : 0;
          return dateA - dateB;
        });
        break;
      case 'date_desc':
      default:
        // Newest first (default)
        stocks.sort((a: any, b: any) => {
          const dateA = a.added_at ? new Date(a.added_at.split('/').reverse().join('-')).getTime() : 0;
          const dateB = b.added_at ? new Date(b.added_at.split('/').reverse().join('-')).getTime() : 0;
          return dateB - dateA;
        });
        break;
    }
    
    return stocks;
  }, [myStocks, stocksFilter, stocksSort, includePortfolio]);
  
  // P36 Item 4: hasMoreStocks and hasLessStocks for Load more / See less
  const INITIAL_STOCKS_LIMIT = 5;
  const hasMoreStocks = stocksLimit < filteredStocks.length;
  const hasLessStocks = stocksLimit > INITIAL_STOCKS_LIMIT && filteredStocks.length > INITIAL_STOCKS_LIMIT;
  
  const loadMoreStocks = () => {
    setStocksLimit(prev => prev + STOCKS_PAGE_SIZE);
  };
  
  // P36 Item 4: See less - collapse back to initial limit
  const seeLessStocks = () => {
    setStocksLimit(INITIAL_STOCKS_LIMIT);
  };

  const fetchNotificationCount = async () => {
    if (!sessionToken) return;
    try {
      const response = await axios.get(`${API_URL}/api/v1/me/notifications/count`, {
        headers: { Authorization: `Bearer ${sessionToken}` },
      });
      setNotificationCount(response.data.unseen_count || 0);
    } catch (err) {
      console.error('Error fetching notification count:', err);
    }
  };

  const fetchNotifications = async () => {
    if (!sessionToken) return;
    setNotificationsLoading(true);
    try {
      const response = await axios.get(`${API_URL}/api/v1/me/notifications?limit=20`, {
        headers: { Authorization: `Bearer ${sessionToken}` },
      });
      setNotifications(response.data.notifications || []);
      setNotificationCount(response.data.unseen_count || 0);
    } catch (err) {
      console.error('Error fetching notifications:', err);
    } finally {
      setNotificationsLoading(false);
    }
  };

  const markNotificationsSeen = async () => {
    if (!sessionToken) return;
    try {
      await axios.post(`${API_URL}/api/v1/me/notifications/mark_seen`, {}, {
        headers: { Authorization: `Bearer ${sessionToken}` },
      });
      setNotificationCount(0);
    } catch (err) {
      console.error('Error marking notifications seen:', err);
    }
  };

  const openNotifications = async () => {
    setShowNotifications(true);
    await fetchNotifications();
    // Mark as seen after a short delay
    setTimeout(() => {
      markNotificationsSeen();
    }, 1000);
  };

  // Poll for notification count
  useEffect(() => {
    if (sessionToken) {
      fetchNotificationCount();
      const interval = setInterval(fetchNotificationCount, 30000); // Every 30 seconds
      return () => clearInterval(interval);
    }
  }, [sessionToken]);

  const fetchData = async () => {
    try {
      const response = await axios.get(`${API_URL}/api/homepage`);
      setData(response.data);
    } catch (err) {
      console.error('Error:', err);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  const fetchNews = async (offset: number = 0, append: boolean = false) => {
    try {
      setNewsLoading(true);
      const response = await axios.get(`${API_URL}/api/news?offset=${offset}&limit=10`);
      const newNews = response.data.news || [];
      
      let allNewsItems: any[];
      if (append) {
        allNewsItems = [...newsItems, ...newNews];
        setNewsItems(allNewsItems);
      } else {
        allNewsItems = newNews;
        setNewsItems(newNews);
      }
      
      // Recalculate aggregate sentiment from all loaded news
      // Formula: average of all sentiment scores
      // +1 = positive (pos > neg), -1 = negative (neg > pos), 0 = neutral
      // Result: >0.3 = Positive, <-0.3 = Negative, else Neutral
      const sentimentScores = allNewsItems.map((n: any) => {
        if (n.sentiment_label === 'positive') return 1;
        if (n.sentiment_label === 'negative') return -1;
        return 0;
      });
      
      const total = sentimentScores.length;
      const avgScore = total > 0 ? sentimentScores.reduce((a: number, b: number) => a + b, 0) / total : 0;
      
      let label = 'neutral';
      let color = '#F59E0B'; // Yellow
      if (avgScore > 0.3) {
        label = 'positive';
        color = '#10B981'; // Green
      } else if (avgScore < -0.3) {
        label = 'negative';
        color = '#EF4444'; // Red
      }
      
      setAggregateSentiment({
        score: Math.round(avgScore * 100) / 100,
        label,
        color,
      });
      
      setHasMoreNews(response.data.has_more);
      setNewsOffset(offset + newNews.length);
    } catch (err) {
      console.error('Error fetching news:', err);
    } finally {
      setNewsLoading(false);
    }
  };

  useFocusEffect(useCallback(() => { 
    fetchData(); 
  }, []));
  
  // Defer news fetch until after main dashboard data has rendered.
  // This avoids two parallel API calls blocking first paint.
  const dataLoaded = !loading && data != null;
  useEffect(() => {
    if (dataLoaded) {
      fetchNews(0, false);
    }
  }, [dataLoaded]);
  
  const onRefresh = () => { 
    setRefreshing(true); 
    fetchData(); 
    fetchNews(0, false);
  };

  const loadMoreNews = () => {
    if (!newsLoading && hasMoreNews) {
      fetchNews(newsOffset, true);
    }
  };

  const homepageEvents = useMemo<HomepageEvent[]>(
    () => (Array.isArray(data?.upcoming_events) ? data.upcoming_events : []),
    [data?.upcoming_events],
  );

  const newsFeedItems = useMemo<DashboardFeedItem[]>(() => {
    const eventItems = includeHomepageEvents
      ? homepageEvents.map((event) => ({
          kind: 'event' as const,
          id: event.id,
          event,
        }))
      : [];
    const articleItems = newsItems.map((article) => ({
      kind: 'article' as const,
      id: article.id,
      article,
    }));
    const mergedItems = [...eventItems, ...articleItems];

    switch (homepageFeedSort) {
      case 'az':
        return mergedItems.sort((a, b) => getDashboardFeedAlphaKey(a).localeCompare(getDashboardFeedAlphaKey(b)));
      case 'za':
        return mergedItems.sort((a, b) => getDashboardFeedAlphaKey(b).localeCompare(getDashboardFeedAlphaKey(a)));
      case 'date_asc':
        return mergedItems.sort((a, b) => getDashboardFeedDateValue(a) - getDashboardFeedDateValue(b));
      case 'date_desc':
      default:
        return mergedItems.sort((a, b) => getDashboardFeedDateValue(b) - getDashboardFeedDateValue(a));
    }
  }, [homepageEvents, homepageFeedSort, includeHomepageEvents, newsItems]);

  const normalizedNewsFeedFilter = newsFeedFilter.trim().toLowerCase();
  const filteredNewsFeedItems = useMemo<DashboardFeedItem[]>(() => {
    if (!normalizedNewsFeedFilter) return newsFeedItems;
    return newsFeedItems.filter((item) => {
      const searchFields = item.kind === 'event'
        ? [
            item.event.ticker,
            item.event.company_name,
            item.event.title,
            item.event.event_type,
            formatHomepageEventSubtitle(item.event),
          ]
        : [
            item.article?.ticker,
            item.article?.company_name,
            item.article?.title,
            item.article?.source,
          ];
      return searchFields
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
        .includes(normalizedNewsFeedFilter);
    });
  }, [newsFeedItems, normalizedNewsFeedFilter]);

  useEffect(() => {
    setNewsLimit(INITIAL_NEWS_LIMIT);
  }, [homepageFeedSort, includeHomepageEvents]);

  const formatPercent = (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;
  const formatDrawdown = (v: number) => `-${Math.abs(v).toFixed(2)}%`;

  const openArticle = (article: any) => {
    setSelectedArticle(article);
  };

  const closeArticle = () => {
    setSelectedArticle(null);
  };

  const openExternalLink = (url: string) => {
    Linking.openURL(url);
  };

  // Mock data for demo (will be replaced with real API data)
  const myPerformance = {
    return: 12.45,
    maxDrawdown: 8.32,
    trackRecord: 142, // days
  };

  const myPortfolios = {
    public: 1,
    private: 2,
    total: 3,
    maxFree: 1,
    maxPro: 10,
  };

  // Get logo URL from stock data — backend now returns internal /api/logo/ paths
  const getLogoUrl = (stock: any): string | null => {
    if (stock?.logo_url) {
      if (stock.logo_url.startsWith("http")) {
        return stock.logo_url;
      }
      // Relative internal path — prepend our backend URL
      return `${API_URL}${stock.logo_url}`;
    }
    return null;
  };

  // Fallback: first letter of ticker with company color
  const COMPANY_COLORS: Record<string, string> = {
    'AAPL': '#000000',
    'MSFT': '#00A4EF',
    'GOOGL': '#4285F4',
    'AMZN': '#FF9900',
    'NVDA': '#76B900',
    'META': '#0081FB',
    'TSLA': '#E82127',
  };

  const getCompanyColor = (ticker: string) => {
    return COMPANY_COLORS[ticker] || '#6B7280';
  };

  return (
    <SafeAreaView style={styles.container} edges={['top']}>
      {/* Global Header with Avatar Menu — always visible immediately */}
      <AppHeader
        title="Home"
        onNotificationPress={openNotifications}
        notificationCount={notificationCount}
      />

      <ScrollView
        style={styles.scroll}
        contentContainerStyle={[styles.scrollContent, { padding: sp.pageGutter }]}
        refreshControl={<RefreshControl refreshing={refreshing} onRefresh={onRefresh} />}
        showsVerticalScrollIndicator={false}
      >
        {/* Welcome */}
        <Text style={styles.welcomeText}>
          Welcome back{user?.name ? `, ${user.name.split(' ')[0]}` : ''}
        </Text>

        {/* ===== MY PERFORMANCE ===== */}
        <View style={styles.performanceCard} data-testid="my-performance">
          <Text style={[styles.sectionTitle, { color: '#FFF' }]}>My Performance</Text>
          <Text style={styles.performanceSubtitle}>Based on your Follow watchlist (equal-weight)</Text>
          
          <View style={styles.performanceMetrics}>
            <View style={styles.metricBox}>
              <Text style={styles.metricLabel}>Return</Text>
              <Text style={[
                styles.metricValue,
                myPerformance.return >= 0 ? styles.positive : styles.negative
              ]}>
                {formatPercent(myPerformance.return)}
              </Text>
            </View>
            
            <View style={styles.metricDivider} />
            
            <View style={styles.metricBox}>
              <Text style={styles.metricLabel}>Max Drawdown</Text>
              <Text style={[styles.metricValue, styles.negative]}>
                {formatDrawdown(myPerformance.maxDrawdown)}
              </Text>
            </View>
          </View>

          <View style={styles.trackRecord}>
            <Ionicons name="calendar-outline" size={14} color={COLORS.textMuted} />
            <Text style={styles.trackRecordText}>
              Track Record: {myPerformance.trackRecord} days
            </Text>
          </View>
        </View>

        {/* ===== MY PORTFOLIOS ===== */}
        <View style={styles.card} data-testid="my-portfolios">
          <View style={styles.cardHeader}>
            <Text style={styles.sectionTitle}>My Portfolios</Text>
            <TouchableOpacity onPress={() => router.push('/(tabs)/portfolio')}>
              <Text style={styles.seeAllLink}>Manage</Text>
            </TouchableOpacity>
          </View>
          
          <View style={styles.portfolioStats}>
            <View style={styles.portfolioStat}>
              <View style={[styles.portfolioIcon, { backgroundColor: '#D1FAE5' }]}>
                <Ionicons name="globe-outline" size={18} color={COLORS.accent} />
              </View>
              <View>
                <Text style={styles.portfolioCount}>{myPortfolios.public}</Text>
                <Text style={styles.portfolioLabel}>Public</Text>
              </View>
            </View>
            
            <View style={styles.portfolioStat}>
              <View style={[styles.portfolioIcon, { backgroundColor: '#E0E7FF' }]}>
                <Ionicons name="lock-closed-outline" size={18} color={COLORS.primary} />
              </View>
              <View>
                <Text style={styles.portfolioCount}>{myPortfolios.private}</Text>
                <Text style={styles.portfolioLabel}>Private</Text>
              </View>
            </View>
            
            <View style={styles.portfolioStat}>
              <View style={[styles.portfolioIcon, { backgroundColor: '#F3F4F6' }]}>
                <Ionicons name="folder-outline" size={18} color={COLORS.textLight} />
              </View>
              <View>
                <Text style={styles.portfolioCount}>{myPortfolios.total}/{myPortfolios.maxPro}</Text>
                <Text style={styles.portfolioLabel}>Total</Text>
              </View>
            </View>
          </View>
        </View>

        {/* ===== P40: MY STOCKS - Portfolio in title + 4 sort buttons ===== */}
        <View style={styles.card} data-testid="my-stocks">
          {/* P40 A) Title row with count + Portfolio toggle + Add */}
          <View style={styles.cardHeader}>
            <View style={styles.sectionTitleRow}>
              <Ionicons name="star" size={18} color={COLORS.warning} />
              <Text style={styles.sectionTitle}>My Stocks {loading ? '' : `(${filteredStocks.length})`}</Text>
            </View>
            <View style={styles.titleRightControls}>
              {/* P40 A) Portfolio toggle in title row */}
              <TouchableOpacity 
                style={styles.portfolioToggleInline}
                onPress={() => setIncludePortfolio(!includePortfolio)}
                data-testid="portfolio-toggle"
              >
                <Text style={styles.portfolioToggleLabelInline}>Portfolio</Text>
                <View style={[
                  styles.toggleSwitch,
                  includePortfolio && styles.toggleSwitchOn
                ]}>
                  <View style={[
                    styles.toggleKnob,
                    includePortfolio && styles.toggleKnobOn
                  ]} />
                </View>
              </TouchableOpacity>
              <TouchableOpacity 
                style={styles.addButtonWithLabel}
                onPress={() => router.push('/(tabs)/search?autofocus=true')}
                data-testid="add-to-watchlist-btn"
              >
                <Ionicons name="add-circle" size={24} color={COLORS.primary} />
                <Text style={styles.addButtonLabel}>Add</Text>
              </TouchableOpacity>
            </View>
          </View>

          {/* Skeleton loading state for My Stocks content */}
          {loading ? (
            <View style={styles.stocksSkeletonContainer}>
              {[0, 1, 2].map((i) => (
                <View key={i} style={styles.stocksSkeletonRow}>
                  <View style={styles.stocksSkeletonAvatar} />
                  <View style={styles.stocksSkeletonLines}>
                    <View style={[styles.stocksSkeletonLine, { width: '50%' }]} />
                    <View style={[styles.stocksSkeletonLine, { width: '70%', marginTop: 6 }]} />
                  </View>
                  <View style={[styles.stocksSkeletonLine, { width: 48, height: 16 }]} />
                </View>
              ))}
            </View>
          ) : (
          <>
          {/* Search field */}
          {myStocks.length > 0 && (
            <View style={styles.myStocksSearchWrapper}>
              <Ionicons name="search" size={16} color={COLORS.textMuted} />
              <TextInput
                style={styles.myStocksSearchInput}
                placeholder="Search my stocks..."
                placeholderTextColor={COLORS.textMuted}
                value={stocksFilter}
                onChangeText={setStocksFilter}
                autoCapitalize="characters"
                autoCorrect={false}
              />
              {stocksFilter.length > 0 && (
                <TouchableOpacity onPress={() => setStocksFilter('')}>
                  <Ionicons name="close-circle" size={16} color={COLORS.textMuted} />
                </TouchableOpacity>
              )}
            </View>
          )}

          {/* P40 B) 4 Sort buttons row */}
          <View style={styles.sortButtonsRow}>
            {/* Date button */}
            <TouchableOpacity 
              style={[
                styles.sortBtn,
                (stocksSort === 'date_desc' || stocksSort === 'date_asc') && styles.sortBtnActive
              ]}
              onPress={() => {
                if (stocksSort === 'date_desc') setStocksSort('date_asc');
                else if (stocksSort === 'date_asc') setStocksSort('date_desc');
                else setStocksSort('date_desc');
              }}
            >
              <Text style={[
                styles.sortBtnText,
                (stocksSort === 'date_desc' || stocksSort === 'date_asc') && styles.sortBtnTextActive
              ]}>
                Date {stocksSort === 'date_asc' ? '↑' : '↓'}
              </Text>
            </TouchableOpacity>
            
            {/* A-Z button */}
            <TouchableOpacity 
              style={[
                styles.sortBtn,
                (stocksSort === 'az' || stocksSort === 'za') && styles.sortBtnActive
              ]}
              onPress={() => {
                if (stocksSort === 'az') setStocksSort('za');
                else if (stocksSort === 'za') setStocksSort('az');
                else setStocksSort('az');
              }}
            >
              <Text style={[
                styles.sortBtnText,
                (stocksSort === 'az' || stocksSort === 'za') && styles.sortBtnTextActive
              ]}>
                A‑Z {stocksSort === 'za' ? '↑' : '↓'}
              </Text>
            </TouchableOpacity>
            
            {/* Total % button */}
            <TouchableOpacity 
              style={[
                styles.sortBtn,
                (stocksSort === 'total_desc' || stocksSort === 'total_asc') && styles.sortBtnActive
              ]}
              onPress={() => {
                if (stocksSort === 'total_desc') setStocksSort('total_asc');
                else if (stocksSort === 'total_asc') setStocksSort('total_desc');
                else setStocksSort('total_desc');
              }}
            >
              <Text style={[
                styles.sortBtnText,
                (stocksSort === 'total_desc' || stocksSort === 'total_asc') && styles.sortBtnTextActive
              ]}>
                Total % {stocksSort === 'total_asc' ? '↑' : '↓'}
              </Text>
            </TouchableOpacity>
            
            {/* 1D % button */}
            <TouchableOpacity 
              style={[
                styles.sortBtn,
                (stocksSort === '1d_desc' || stocksSort === '1d_asc') && styles.sortBtnActive
              ]}
              onPress={() => {
                if (stocksSort === '1d_desc') setStocksSort('1d_asc');
                else if (stocksSort === '1d_asc') setStocksSort('1d_desc');
                else setStocksSort('1d_desc');
              }}
            >
              <Text style={[
                styles.sortBtnText,
                (stocksSort === '1d_desc' || stocksSort === '1d_asc') && styles.sortBtnTextActive
              ]}>
                1D % {stocksSort === '1d_asc' ? '↑' : '↓'}
              </Text>
            </TouchableOpacity>
            
            {/* Column header for values */}
            <View style={styles.columnHeaderSpacer} />
            <Text style={styles.columnHeader}>Total{'\n'}(1D)</Text>
          </View>

          {myStocks.length === 0 ? (
            <View style={styles.emptyState}>
              <Ionicons name="star-outline" size={32} color={COLORS.textMuted} />
              <Text style={styles.emptyText}>No companies followed yet</Text>
              <TouchableOpacity 
                style={styles.emptyButton}
                onPress={() => router.push('/(tabs)/search?autofocus=true')}
              >
                <Text style={styles.emptyButtonText}>Find companies</Text>
              </TouchableOpacity>
            </View>
          ) : filteredStocks.length === 0 ? (
            <View style={styles.emptyState}>
              <Ionicons name="search-outline" size={32} color={COLORS.textMuted} />
              <Text style={styles.emptyText}>No matches for "{stocksFilter}"</Text>
            </View>
          ) : (
            <>
              {filteredStocks.slice(0, stocksLimit).map((stock: any, index: number) => {
                const logoUrl = getLogoUrl(stock);
                const isLastVisible = index === Math.min(stocksLimit, filteredStocks.length) - 1;
                return (
                  <View 
                    key={stock.ticker}
                    style={[
                      styles.companyRowWrapper,
                      isLastVisible && !hasMoreStocks && !hasLessStocks && styles.lastRow
                    ]}
                  >
                    {/* P41: Removed unfollow star - unfollow only via ticker detail page */}
                    
                    <TouchableOpacity
                      style={styles.companyRow}
                      onPress={() => {
                        useMyStocksStore.getState().setTickers(filteredStocks.map((s: any) => s.ticker));
                        router.push(`/stock/${stock.ticker}`);
                      }}
                    >
                      <View style={styles.companyLeft}>
                        <StockLogo logoUrl={logoUrl} ticker={stock.ticker} color={getCompanyColor(stock.ticker)} />
                        <View style={styles.stockInfoColumn}>
                          <View style={styles.tickerRow}>
                            <Text style={styles.companyTicker}>{stock.ticker}</Text>
                            {/* P33: Pill indicator */}
                            <View style={[
                              styles.stockPill,
                              stock.pill === 'Portfolio' && styles.stockPillPortfolio,
                              stock.pill === 'Both' && styles.stockPillBoth,
                            ]}>
                              <Text style={[
                                styles.stockPillText,
                                stock.pill === 'Portfolio' && styles.stockPillTextPortfolio,
                                stock.pill === 'Both' && styles.stockPillTextBoth,
                              ]}>
                                {stock.pill}
                              </Text>
                            </View>
                          </View>
                          <Text style={styles.companyName} numberOfLines={1}>{stock.name || stock.ticker}</Text>
                          {/* P37+ Part 3 (G): Show added date */}
                          {stock.added_at && (
                            <Text style={styles.stockAddedAt}>Added: {stock.added_at}</Text>
                          )}
                        </View>
                      </View>
                      {/* P37+ Part 3 (G): Show change since added + 1D change */}
                      <View style={styles.stockChangesColumn}>
                        {stock.change_since_added !== null && stock.change_since_added !== undefined && (
                          <Text style={[
                            styles.stockChangeSinceAdded,
                            stock.change_since_added >= 0 ? styles.positive : styles.negative
                          ]}>
                            {stock.change_since_added >= 0 ? '+' : ''}{stock.change_since_added.toFixed(2)}%
                          </Text>
                        )}
                        {/* Fix 2: Daily change in brackets without "1D:" */}
                        <Text style={[
                          styles.stock1dChange,
                          (stock.change_1d_pct || 0) >= 0 ? styles.positiveLight : styles.negativeLight
                        ]}>
                          ({(stock.change_1d_pct || 0) >= 0 ? '+' : ''}{(stock.change_1d_pct || 0).toFixed(2)}%)
                        </Text>
                      </View>
                    </TouchableOpacity>
                  </View>
                );
              })}
              {/* Load more / See less - full width */}
              {(hasMoreStocks || hasLessStocks) && (
                <View style={styles.stocksButtonsRow}>
                  {hasMoreStocks && (
                    <TouchableOpacity 
                      style={styles.loadMoreButtonFull}
                      onPress={loadMoreStocks}
                      data-testid="load-more-stocks-btn"
                    >
                      <Text style={styles.loadMoreText}>Load more stocks</Text>
                    </TouchableOpacity>
                  )}
                  {hasLessStocks && (
                    <TouchableOpacity 
                      style={styles.seeLessButtonFull}
                      onPress={seeLessStocks}
                      data-testid="see-less-stocks-btn"
                    >
                      <Text style={styles.seeLessText}>See less</Text>
                    </TouchableOpacity>
                  )}
                </View>
              )}
            </>
          )}
          </>
          )}
        </View>

        {/* ===== Homepage News & Events feed ===== */}
        <View style={styles.card} data-testid="news-feed">
          {/* Fixed Header - stays visible */}
          <View style={styles.newsHeader}>
            <View style={styles.newsTitleRow}>
              <View style={styles.newsTitleWithIcon}>
                {/* P35 Item 1: Section icon */}
                <Ionicons name="newspaper" size={18} color={COLORS.primary} />
                <View>
                  <Text style={styles.sectionTitle}>News & Events</Text>
                </View>
              </View>
              {/* Aggregate Sentiment Badge */}
              {aggregateSentiment && (
                <View 
                  style={[
                    styles.aggregateSentimentBadge,
                    { backgroundColor: aggregateSentiment.color + '20' }
                  ]}
                  data-testid="aggregate-sentiment"
                >
                  <View style={[styles.aggregateSentimentDot, { backgroundColor: aggregateSentiment.color }]} />
                  <Text style={[styles.aggregateSentimentText, { color: aggregateSentiment.color }]}>
                    {aggregateSentiment.label === 'positive' ? 'Positive' : 
                     aggregateSentiment.label === 'negative' ? 'Negative' : 'Neutral'}
                  </Text>
                </View>
              )}
            </View>
            <View style={styles.newsControlsRow}>
              <View style={styles.newsSortButtons}>
                <TouchableOpacity
                  style={[
                    styles.sortBtn,
                    (homepageFeedSort === 'date_desc' || homepageFeedSort === 'date_asc') && styles.sortBtnActive,
                  ]}
                  onPress={() => {
                    if (homepageFeedSort === 'date_desc') setHomepageFeedSort('date_asc');
                    else if (homepageFeedSort === 'date_asc') setHomepageFeedSort('date_desc');
                    else setHomepageFeedSort('date_desc');
                  }}
                >
                  <Text
                    style={[
                      styles.sortBtnText,
                      (homepageFeedSort === 'date_desc' || homepageFeedSort === 'date_asc') && styles.sortBtnTextActive,
                    ]}
                  >
                    Date {homepageFeedSort === 'date_asc' ? '↑' : '↓'}
                  </Text>
                </TouchableOpacity>
                <TouchableOpacity
                  style={[
                    styles.sortBtn,
                    (homepageFeedSort === 'az' || homepageFeedSort === 'za') && styles.sortBtnActive,
                  ]}
                  onPress={() => {
                    if (homepageFeedSort === 'az') setHomepageFeedSort('za');
                    else if (homepageFeedSort === 'za') setHomepageFeedSort('az');
                    else setHomepageFeedSort('az');
                  }}
                >
                  <Text
                    style={[
                      styles.sortBtnText,
                      (homepageFeedSort === 'az' || homepageFeedSort === 'za') && styles.sortBtnTextActive,
                    ]}
                  >
                    A‑Z {homepageFeedSort === 'za' ? '↑' : '↓'}
                  </Text>
                </TouchableOpacity>
              </View>
              <TouchableOpacity
                style={styles.portfolioToggleInline}
                onPress={() => setIncludeHomepageEvents((prev) => !prev)}
                data-testid="homepage-events-toggle"
              >
                <Text style={styles.portfolioToggleLabelInline}>Events</Text>
                <View style={[styles.toggleSwitch, includeHomepageEvents && styles.toggleSwitchOn]}>
                  <View style={[styles.toggleKnob, includeHomepageEvents && styles.toggleKnobOn]} />
                </View>
              </TouchableOpacity>
            </View>
            {newsFeedItems.length > 0 && (
              <View style={styles.myStocksSearchWrapper}>
                <Ionicons name="search" size={16} color={COLORS.textMuted} />
                <TextInput
                  style={styles.myStocksSearchInput}
                  placeholder="Search news & events..."
                  placeholderTextColor={COLORS.textMuted}
                  value={newsFeedFilter}
                  onChangeText={setNewsFeedFilter}
                  autoCorrect={false}
                />
                {newsFeedFilter.length > 0 && (
                  <TouchableOpacity onPress={() => setNewsFeedFilter('')}>
                    <Ionicons name="close-circle" size={16} color={COLORS.textMuted} />
                  </TouchableOpacity>
                )}
              </View>
            )}
          </View>
          
          {/* News List */}

          {newsLoading && newsItems.length === 0 && homepageEvents.length === 0 ? (
            <View style={styles.newsLoadingContainer}>
              <ActivityIndicator size="small" color={COLORS.primary} />
              <Text style={styles.newsLoadingText}>Loading news...</Text>
            </View>
          ) : filteredNewsFeedItems.length === 0 ? (
            <Text style={styles.noNewsText}>
              {normalizedNewsFeedFilter ? `No matches for "${newsFeedFilter}"` : 'No news or events available'}
            </Text>
          ) : (
            filteredNewsFeedItems.slice(0, newsLimit).map((item, index) => {
              const isEvent = item.kind === 'event';
              const news = isEvent ? item.event : item.article;
              const eventSubtitle = isEvent ? formatHomepageEventSubtitle(item.event) : null;
              return (
              <View
                key={item.id}
                style={[
                  styles.newsRow,
                  index === Math.min(newsLimit, filteredNewsFeedItems.length) - 1 && styles.lastRow
                ]}
              >
                {/* P31 LOGO GUARANTEE: Always show logo or fallback badge */}
                <TouchableOpacity 
                  onPress={() => news.ticker && router.push(`/stock/${news.ticker}`)}
                  disabled={!news.ticker}
                >
                  <NewsLogo 
                    logoUrl={news.logo_url ? (news.logo_url.startsWith('http') ? news.logo_url : `${API_URL}${news.logo_url}`) : undefined} 
                    fallbackKey={news.fallback_logo_key || (news.ticker || '?').charAt(0)}
                    ticker={news.ticker}
                  />
                </TouchableOpacity>
                
                {/* Article content - clickable to open full article */}
                <TouchableOpacity 
                  style={styles.newsContent}
                  onPress={() => {
                    if (isEvent) {
                      if (news.ticker) router.push(`/stock/${news.ticker}`);
                      return;
                    }
                    openArticle(news);
                  }}
                >
                  <View style={styles.newsTickerRow}>
                    <Text style={styles.newsTickerText}>{news.ticker || 'Market'}</Text>
                    {isEvent ? (
                      <View style={styles.homepageEventBadge}>
                        <Text style={styles.homepageEventBadgeText}>{item.event.event_type}</Text>
                      </View>
                    ) : news.sentiment_label ? (
                      <View style={[
                        styles.sentimentBadgeSmall,
                        news.sentiment_label === 'positive' && { backgroundColor: '#D1FAE5' },
                        news.sentiment_label === 'negative' && { backgroundColor: '#FEE2E2' },
                        news.sentiment_label === 'neutral' && { backgroundColor: '#FEF3C7' },
                      ]}>
                        <Text style={[
                          styles.sentimentTextSmall,
                          news.sentiment_label === 'positive' && { color: COLORS.accent },
                          news.sentiment_label === 'negative' && { color: COLORS.danger },
                          news.sentiment_label === 'neutral' && { color: '#D97706' },
                        ]}>
                          {news.sentiment_label === 'positive' ? 'Positive' : 
                           news.sentiment_label === 'negative' ? 'Negative' : 'Neutral'}
                        </Text>
                      </View>
                    ) : null}
                    <Text style={styles.newsMeta}>
                      {isEvent ? formatDashboardDate(news.date) : (news.date ? new Date(news.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : '')}
                    </Text>
                  </View>
                  <Text style={styles.newsTitle} numberOfLines={2}>{news.title}</Text>
                  {eventSubtitle ? (
                    <Text style={styles.homepageEventSubtitle} numberOfLines={2}>{eventSubtitle}</Text>
                  ) : null}
                </TouchableOpacity>
                
                <Ionicons name="chevron-forward" size={18} color={COLORS.textMuted} />
              </View>
            )})
          )}

          {/* Fix 3: Load More + See Less for News */}
          {filteredNewsFeedItems.length > 0 && (
            <View style={styles.stocksButtonsRow}>
              {(hasMoreNews || newsLimit < filteredNewsFeedItems.length) && (
                <TouchableOpacity 
                  style={styles.loadMoreButtonFull}
                  onPress={() => {
                    if (newsLimit >= filteredNewsFeedItems.length) {
                      loadMoreNews();
                    }
                    setNewsLimit(prev => prev + 5);
                  }}
                  disabled={newsLoading}
                  data-testid="load-more-news-btn"
                >
                  {newsLoading ? (
                    <ActivityIndicator size="small" color={COLORS.primary} />
                  ) : (
                    <Text style={styles.loadMoreText}>Load more news & events</Text>
                  )}
                </TouchableOpacity>
              )}
              {newsLimit > INITIAL_NEWS_LIMIT && (
                <TouchableOpacity 
                  style={styles.seeLessButtonFull}
                  onPress={() => setNewsLimit(INITIAL_NEWS_LIMIT)}
                  data-testid="see-less-news-btn"
                >
                  <Text style={styles.seeLessText}>See less</Text>
                </TouchableOpacity>
              )}
            </View>
          )}
        </View>

        {/* Bottom padding for tab bar */}
        <View style={{ height: 100 }} />
      </ScrollView>

      {/* Article Modal */}
      <Modal
        visible={!!selectedArticle}
        animationType="slide"
        presentationStyle="pageSheet"
        onRequestClose={closeArticle}
      >
        <SafeAreaView style={styles.modalContainer}>
          <View style={styles.modalHeader}>
            <TouchableOpacity onPress={closeArticle} style={styles.modalCloseButton}>
              <Ionicons name="close" size={28} color={COLORS.text} />
            </TouchableOpacity>
            <Text style={styles.modalHeaderTitle}>Article</Text>
            <TouchableOpacity 
              onPress={() => selectedArticle?.link && openExternalLink(selectedArticle.link)}
              style={styles.modalExternalButton}
            >
              <Ionicons name="open-outline" size={22} color={COLORS.primary} />
            </TouchableOpacity>
          </View>
          
          <ScrollView style={styles.modalScroll} contentContainerStyle={styles.modalScrollContent}>
            {selectedArticle && (
              <>
                {/* Article Header */}
                <View style={styles.articleHeader}>
                  <TouchableOpacity 
                    style={styles.articleTickerRow}
                    onPress={() => {
                      closeArticle();
                      if (selectedArticle.ticker) {
                        router.push(`/stock/${selectedArticle.ticker}`);
                      }
                    }}
                  >
                    {selectedArticle.logo_url ? (
                      <Image source={{ uri: selectedArticle.logo_url.startsWith('http') ? selectedArticle.logo_url : `${API_URL}${selectedArticle.logo_url}` }} style={styles.articleLogo} />
                    ) : (
                      <View style={[styles.articleLogoFallback, { backgroundColor: getCompanyColor(selectedArticle.ticker || '') }]}>
                        <Text style={styles.articleLogoText}>{(selectedArticle.ticker || '?').charAt(0)}</Text>
                      </View>
                    )}
                    <View>
                      <Text style={styles.articleTicker}>{selectedArticle.ticker || 'Market News'}</Text>
                      <Text style={styles.articleCompany}>{selectedArticle.company_name}</Text>
                    </View>
                  </TouchableOpacity>
                  <Text style={styles.articleMeta}>
                    {selectedArticle.date ? new Date(selectedArticle.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : ''} · {selectedArticle.source}
                  </Text>
                </View>
                
                {/* Article Title */}
                <Text style={styles.articleTitle}>{selectedArticle.title}</Text>
                
                {/* Sentiment */}
                {selectedArticle.sentiment && (
                  <View style={styles.sentimentRow}>
                    <View style={[
                      styles.sentimentBadge,
                      { backgroundColor: 
                        selectedArticle.sentiment.pos > selectedArticle.sentiment.neg ? '#D1FAE5' : 
                        selectedArticle.sentiment.neg > selectedArticle.sentiment.pos ? '#FEE2E2' : 
                        '#FEF3C7' 
                      }
                    ]}>
                      <Text style={[
                        styles.sentimentText,
                        { color: 
                          selectedArticle.sentiment.pos > selectedArticle.sentiment.neg ? COLORS.accent : 
                          selectedArticle.sentiment.neg > selectedArticle.sentiment.pos ? COLORS.danger : 
                          '#D97706' 
                        }
                      ]}>
                        {selectedArticle.sentiment.pos > selectedArticle.sentiment.neg ? 'Positive' : 
                         selectedArticle.sentiment.neg > selectedArticle.sentiment.pos ? 'Negative' : 
                         'Neutral'} Sentiment
                      </Text>
                    </View>
                  </View>
                )}
                
                {/* Tags */}
                {selectedArticle.tags && selectedArticle.tags.length > 0 && (
                  <View style={styles.tagsRow}>
                    {selectedArticle.tags.slice(0, 5).map((tag: string, i: number) => (
                      <View key={i} style={styles.tagBadge}>
                        <Text style={styles.tagText}>{tag}</Text>
                      </View>
                    ))}
                  </View>
                )}
                
                {/* Article Content */}
                <Text style={styles.articleContent}>{selectedArticle.content}</Text>
                
                {/* Read Original */}
                <TouchableOpacity 
                  style={styles.readOriginalButton}
                  onPress={() => selectedArticle.link && openExternalLink(selectedArticle.link)}
                >
                  <Text style={styles.readOriginalText}>Read original article</Text>
                  <Ionicons name="open-outline" size={16} color={COLORS.primary} />
                </TouchableOpacity>
              </>
            )}
          </ScrollView>
        </SafeAreaView>
      </Modal>

      {/* Notifications Modal */}
      <Modal
        visible={showNotifications}
        animationType="slide"
        transparent={true}
        onRequestClose={() => setShowNotifications(false)}
      >
        <View style={styles.notifModalOverlay}>
          <View style={styles.notifModalContent}>
            <View style={styles.notifModalHeader}>
              <Text style={styles.notifModalTitle}>Notifications</Text>
              <TouchableOpacity onPress={() => setShowNotifications(false)}>
                <Ionicons name="close" size={24} color={COLORS.text} />
              </TouchableOpacity>
            </View>
            
            {notificationsLoading ? (
              <View style={styles.notifLoading}>
                <ActivityIndicator size="large" color={COLORS.primary} />
              </View>
            ) : notifications.length === 0 ? (
              <View style={styles.notifEmpty}>
                <Ionicons name="notifications-off-outline" size={48} color={COLORS.textMuted} />
                <Text style={styles.notifEmptyText}>No notifications yet</Text>
                <Text style={styles.notifEmptySubtext}>Subscribe to filters in Talk to receive notifications</Text>
              </View>
            ) : (
              <ScrollView style={styles.notifList} showsVerticalScrollIndicator={false}>
                {notifications.map((notif, index) => (
                  <TouchableOpacity 
                    key={index}
                    style={[styles.notifItem, !notif.seen_at && styles.notifItemUnseen]}
                    onPress={() => {
                      setShowNotifications(false);
                      if (notif.post?.symbol) {
                        router.push(`/stock/${notif.post.symbol.replace('.US', '')}`);
                      } else {
                        router.push('/(tabs)/talk');
                      }
                    }}
                  >
                    {notif.author?.picture ? (
                      <Image source={{ uri: notif.author.picture }} style={styles.notifAvatar} />
                    ) : (
                      <View style={styles.notifAvatarPlaceholder}>
                        <Ionicons name="person" size={14} color={COLORS.textMuted} />
                      </View>
                    )}
                    <View style={styles.notifContent}>
                      <Text style={styles.notifAuthor}>{notif.author?.name || 'Someone'}</Text>
                      <Text style={styles.notifText} numberOfLines={2}>{notif.post?.text}</Text>
                      {notif.post?.symbol && (
                        <Text style={styles.notifSymbol}>${notif.post.symbol.replace('.US', '')}</Text>
                      )}
                    </View>
                    {!notif.seen_at && <View style={styles.notifUnseenDot} />}
                  </TouchableOpacity>
                ))}
              </ScrollView>
            )}
          </View>
        </View>
      </Modal>
      
      {/* P41: Removed unfollow confirmation modal - unfollow only via ticker detail */}
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.background },
  loadingContainer: { 
    flex: 1, 
    alignItems: 'center', 
    justifyContent: 'center', 
    backgroundColor: COLORS.background 
  },
  loadingTitle: {
    fontSize: 24,
    fontWeight: '700',
    color: COLORS.primary,
    marginTop: 12,
    letterSpacing: 2,
  },
  // Inline loading state (renders inside app shell, not full-screen)
  inlineLoadingContainer: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    paddingVertical: 60,
  },
  inlineLoadingText: {
    marginTop: 12,
    fontSize: 14,
    color: COLORS.textLight,
  },

  // Header
  header: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 16,
    paddingVertical: 12,
    backgroundColor: COLORS.card,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  logo: { width: 36, height: 36 },
  headerTitle: {
    flex: 1,
    fontSize: 18,
    fontWeight: '700',
    color: COLORS.primary,
    marginLeft: 10,
    letterSpacing: 1,
  },
  headerRight: { flexDirection: 'row', alignItems: 'center', gap: 12 },
  headerIcon: { padding: 4, position: 'relative' },
  notificationBadge: {
    position: 'absolute',
    top: 0,
    right: 0,
    backgroundColor: COLORS.danger,
    borderRadius: 10,
    minWidth: 18,
    height: 18,
    justifyContent: 'center',
    alignItems: 'center',
    paddingHorizontal: 4,
  },
  notificationBadgeText: {
    color: '#FFFFFF',
    fontSize: 10,
    fontWeight: '700',
  },
  avatar: {
    width: 36,
    height: 36,
    borderRadius: 18,
    backgroundColor: COLORS.background,
    alignItems: 'center',
    justifyContent: 'center',
    borderWidth: 2,
    borderColor: COLORS.border,
  },
  avatarImage: { width: 32, height: 32, borderRadius: 16 },
  
  // Subscription Badge
  subscriptionBadge: {
    paddingHorizontal: 8,
    paddingVertical: 3,
    borderRadius: 10,
    backgroundColor: COLORS.border,
    marginLeft: 6,
    flexDirection: 'row',
    alignItems: 'center',
  },
  subscriptionBadgePro: {
    backgroundColor: '#8B5CF6',
  },
  subscriptionBadgeProPlus: {
    backgroundColor: '#6D28D9',
  },
  subscriptionBadgeText: {
    fontSize: 10,
    fontWeight: '700',
    color: COLORS.textMuted,
  },
  subscriptionBadgeTextPro: {
    color: '#FFFFFF',
  },

  scroll: { flex: 1 },
  scrollContent: { padding: 16 },

  welcomeText: {
    fontSize: 22,
    fontFamily: FONTS.heading,
    color: COLORS.text,
    marginBottom: 16,
  },

  // Performance Card
  performanceCard: {
    backgroundColor: COLORS.primary,
    borderRadius: 16,
    padding: 20,
    marginBottom: 16,
  },
  sectionTitle: {
    fontSize: 14,
    fontFamily: FONTS.bodyBold,
    color: COLORS.text,
    letterSpacing: 0.5,
    textTransform: 'uppercase',
  },
  performanceSubtitle: {
    fontSize: 12,
    color: 'rgba(255,255,255,0.7)',
    marginTop: 4,
    marginBottom: 16,
  },
  performanceMetrics: {
    flexDirection: 'row',
    alignItems: 'center',
  },
  metricBox: {
    flex: 1,
    alignItems: 'center',
  },
  metricLabel: {
    fontSize: 12,
    color: 'rgba(255,255,255,0.7)',
    marginBottom: 4,
  },
  metricValue: {
    fontSize: 28,
    fontFamily: FONTS.bodySemiBold,
  },
  metricDivider: {
    width: 1,
    height: 50,
    backgroundColor: 'rgba(255,255,255,0.2)',
    marginHorizontal: 16,
  },
  positive: { color: '#34D399' },
  negative: { color: '#F87171' },
  trackRecord: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    marginTop: 16,
    paddingTop: 16,
    borderTopWidth: 1,
    borderTopColor: 'rgba(255,255,255,0.15)',
    gap: 6,
  },
  trackRecordText: {
    fontSize: 13,
    color: 'rgba(255,255,255,0.7)',
  },

  // Cards
  card: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 16,
    marginBottom: 16,
    ...Platform.select({
      web: { boxShadow: '0 1px 3px rgba(0,0,0,0.08)' },
      default: { elevation: 1 },
    }),
  },
  cardHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    marginBottom: 12,
  },
  
  // P35 Item 1: Section title with icon
  sectionTitleRow: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    gap: 8,
  },
  // P36 Item 1: Microcopy description
  sectionSubtitle: {
    fontSize: 12,
    color: COLORS.textMuted,
    marginTop: 2,
  },
  newsTitleWithIcon: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    gap: 8,
  },
  
  seeAllLink: {
    fontSize: 14,
    color: COLORS.primary,
    fontWeight: '600',
  },
  
  // P36 Item 2: "+" with Add label
  addButtonWithLabel: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 8,
    backgroundColor: COLORS.primary + '10',
  },
  addButtonLabel: {
    fontSize: 12,
    fontWeight: '600',
    color: COLORS.primary,
  },
  
  // Fix 1: My Stocks search - no frame, same as other search
  myStocksSearchWrapper: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: COLORS.background,
    borderRadius: 8,
    paddingHorizontal: 12,
    paddingVertical: 8,
    marginBottom: 8,
    gap: 8,
  },
  myStocksSearchInput: {
    flex: 1,
    fontSize: 14,
    color: COLORS.text,
    paddingVertical: 0,
    outlineStyle: 'none',
  },
  
  // P37++ Sort controls + Portfolio toggle row
  stocksControlRow: {
    flexDirection: 'column',
    gap: 8,
    marginBottom: 8,
    paddingBottom: 8,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  
  // P37++ B) Portfolio toggle
  portfolioToggle: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
  },
  portfolioToggleText: {
    fontSize: 12,
    color: COLORS.textMuted,
  },
  portfolioToggleTextActive: {
    color: COLORS.primary,
    fontWeight: '500',
  },
  
  // P37++ A) Sort dropdown
  sortDropdown: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  sortLabel: {
    fontSize: 12,
    color: COLORS.textMuted,
  },
  sortButtonsScroll: {
    flexGrow: 0,
  },
  sortButtons: {
    flexDirection: 'row',
    gap: 4,
  },
  sortButton: {
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 4,
    backgroundColor: COLORS.background,
  },
  sortButtonActive: {
    backgroundColor: COLORS.primary,
  },
  sortButtonText: {
    fontSize: 10,
    fontWeight: '500',
    color: COLORS.textMuted,
  },
  sortButtonTextActive: {
    color: '#FFFFFF',
  },
  
  // P39: Compact controls row (kept for reference)
  compactControlsRow: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    marginBottom: 12,
    paddingBottom: 12,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  sortButtonCompact: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
    paddingHorizontal: 12,
    paddingVertical: 8,
    borderRadius: 8,
    backgroundColor: COLORS.primary + '10',
  },
  sortButtonCompactText: {
    fontSize: 13,
    fontWeight: '500',
    color: COLORS.primary,
  },
  portfolioToggleCompact: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  portfolioToggleLabel: {
    fontSize: 13,
    color: COLORS.textLight,
  },
  toggleSwitch: {
    width: 44,
    height: 24,
    borderRadius: 12,
    backgroundColor: COLORS.border,
    padding: 2,
    justifyContent: 'center',
  },
  toggleSwitchOn: {
    backgroundColor: COLORS.primary,
  },
  toggleKnob: {
    width: 20,
    height: 20,
    borderRadius: 10,
    backgroundColor: '#FFFFFF',
  },
  toggleKnobOn: {
    alignSelf: 'flex-end',
  },
  
  // P40: Title right controls (Portfolio toggle + Add)
  titleRightControls: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 12,
  },
  portfolioToggleInline: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
  },
  portfolioToggleLabelInline: {
    fontSize: 12,
    color: COLORS.textLight,
  },
  
  // P40: 4 Sort buttons row
  sortButtonsRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
    marginBottom: 12,
    paddingBottom: 12,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  sortBtn: {
    paddingHorizontal: 10,
    paddingVertical: 6,
    borderRadius: 6,
    backgroundColor: COLORS.background,
  },
  sortBtnActive: {
    backgroundColor: COLORS.primary,
  },
  sortBtnText: {
    fontSize: 11,
    fontWeight: '500',
    color: COLORS.textMuted,
  },
  sortBtnTextActive: {
    color: '#FFFFFF',
  },
  
  // Load more / See less row
  stocksButtonsRow: {
    flexDirection: 'row',
    justifyContent: 'center',
    gap: 12,
    marginTop: 12,
  },
  seeLessButton: {
    paddingHorizontal: 16,
    paddingVertical: 8,
    borderRadius: 8,
    backgroundColor: COLORS.background,
  },
  seeLessText: {
    fontSize: 13,
    color: COLORS.textLight,
    fontWeight: '500',
  },
  
  // P35 Item 2: Stock row wrapper with star button
  companyRowWrapper: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
    paddingVertical: 8,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  stockStarButton: {
    width: 32,
    height: 32,
    alignItems: 'center',
    justifyContent: 'center',
  },
  
  // P37+ Part 3 (G): Stock info column with added date
  stockInfoColumn: {
    flex: 1,
  },
  stockAddedAt: {
    fontSize: 10,
    color: COLORS.textMuted,
    marginTop: 2,
  },
  stockChangesColumn: {
    alignItems: 'flex-end',
  },
  stockChangeSinceAdded: {
    fontSize: 14,
    fontWeight: '600',
  },
  stock1dChange: {
    fontSize: 11,
    marginTop: 2,
  },
  positiveLight: {
    color: '#34D399',
  },
  negativeLight: {
    color: '#F87171',
  },
  
  // Column header for Total (1D)
  columnHeaderSpacer: {
    flex: 1,
  },
  columnHeader: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.textMuted,
    textAlign: 'right',
    lineHeight: 18,
  },
  
  // P33: Stock counts row
  stockCounts: {
    marginBottom: 8,
    paddingBottom: 8,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  stockCountText: {
    fontSize: 12,
    color: COLORS.textMuted,
  },
  
  // P33: Ticker row with pill
  tickerRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
  },
  stockPill: {
    paddingHorizontal: 6,
    paddingVertical: 2,
    borderRadius: 4,
    backgroundColor: '#FEF3C7',
  },
  stockPillPortfolio: {
    backgroundColor: '#D1FAE5',
  },
  stockPillBoth: {
    backgroundColor: '#E0E7FF',
  },
  stockPillText: {
    fontSize: 9,
    fontWeight: '600',
    color: '#D97706',
  },
  stockPillTextPortfolio: {
    color: '#059669',
  },
  stockPillTextBoth: {
    color: '#4F46E5',
  },
  
  // P37+ Part 1 (B): Full width load more/see less buttons
  loadMoreButtonFull: {
    flex: 1,
    paddingVertical: 12,
    borderRadius: 8,
    backgroundColor: COLORS.primary + '10',
    alignItems: 'center',
  },
  seeLessButtonFull: {
    flex: 1,
    paddingVertical: 12,
    borderRadius: 8,
    backgroundColor: COLORS.background,
    alignItems: 'center',
  },
  
  // P35 Item 2: Unfollow confirmation modal
  unfollowModalOverlay: {
    flex: 1,
    backgroundColor: 'rgba(0,0,0,0.5)',
    justifyContent: 'center',
    alignItems: 'center',
    padding: 24,
  },
  unfollowModalContent: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 24,
    alignItems: 'center',
    width: '100%',
    maxWidth: 320,
  },
  unfollowModalTitle: {
    fontSize: 18,
    fontWeight: '700',
    color: COLORS.text,
    marginTop: 16,
    marginBottom: 8,
  },
  unfollowModalText: {
    fontSize: 14,
    color: COLORS.textLight,
    textAlign: 'center',
    marginBottom: 24,
  },
  unfollowModalButtons: {
    flexDirection: 'row',
    gap: 12,
    width: '100%',
  },
  unfollowCancelButton: {
    flex: 1,
    paddingVertical: 12,
    borderRadius: 8,
    backgroundColor: COLORS.background,
    alignItems: 'center',
  },
  unfollowCancelText: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.text,
  },
  unfollowConfirmButton: {
    flex: 1,
    paddingVertical: 12,
    borderRadius: 8,
    backgroundColor: COLORS.danger,
    alignItems: 'center',
  },
  unfollowConfirmText: {
    fontSize: 14,
    fontWeight: '600',
    color: '#FFFFFF',
  },

  // Portfolio stats
  portfolioStats: {
    flexDirection: 'row',
    justifyContent: 'space-around',
  },
  portfolioStat: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 10,
  },
  portfolioIcon: {
    width: 40,
    height: 40,
    borderRadius: 12,
    alignItems: 'center',
    justifyContent: 'center',
  },
  portfolioCount: {
    fontSize: 18,
    fontWeight: '700',
    color: COLORS.text,
  },
  portfolioLabel: {
    fontSize: 12,
    color: COLORS.textMuted,
  },

  // Company rows
  companyRow: {
    flex: 1,
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    paddingVertical: 4,
  },
  lastRow: { borderBottomWidth: 0 },
  companyLeft: {
    flexDirection: 'row',
    alignItems: 'center',
    flex: 1,
    gap: 12,
  },
  companyLogo: {
    width: 40,
    height: 40,
    borderRadius: 10,
    backgroundColor: '#F3F4F6',
  },
  companyLogoFallback: {
    width: 40,
    height: 40,
    borderRadius: 10,
    alignItems: 'center',
    justifyContent: 'center',
  },
  companyLogoText: {
    fontSize: 18,
    fontWeight: '700',
    color: '#FFFFFF',
  },
  companyTicker: {
    fontSize: 14,
    fontFamily: FONTS.bodyBold,
    color: COLORS.text,
  },
  companyName: {
    fontSize: 12,
    fontFamily: FONTS.body,
    color: COLORS.textMuted,
    marginTop: 2,
  },
  companyChange: {
    fontSize: 15,
    fontWeight: '700',
  },

  // Empty state
  emptyState: {
    alignItems: 'center',
    paddingVertical: 24,
  },
  emptyText: {
    fontSize: 14,
    color: COLORS.textMuted,
    marginTop: 8,
    marginBottom: 16,
  },
  emptyButton: {
    backgroundColor: COLORS.primary,
    paddingHorizontal: 20,
    paddingVertical: 10,
    borderRadius: 8,
  },
  emptyButtonText: {
    fontSize: 14,
    fontWeight: '600',
    color: '#FFF',
  },

  // News
  newsHeader: {
    backgroundColor: COLORS.card,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
    paddingBottom: 12,
    marginBottom: 8,
  },
  newsTitleRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
  },
  newsControlsRow: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    gap: 12,
    marginTop: 12,
  },
  newsSortButtons: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 8,
    flex: 1,
  },
  newsSubtitle: {
    fontSize: 12,
    color: COLORS.textMuted,
    marginTop: 4,
  },
  newsScrollContainer: {
    // Removed - using main ScrollView for scrolling
  },
  aggregateSentimentBadge: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 10,
    paddingVertical: 6,
    borderRadius: 12,
    gap: 6,
  },
  aggregateSentimentDot: {
    width: 8,
    height: 8,
    borderRadius: 4,
  },
  aggregateSentimentText: {
    fontSize: 12,
    fontWeight: '600',
  },
  aggregateSentimentScore: {
    fontSize: 11,
    color: COLORS.textMuted,
  },
  newsRow: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingVertical: 12,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
    gap: 12,
  },
  newsLogo: {
    width: 36,
    height: 36,
    borderRadius: 8,
    backgroundColor: '#F3F4F6',
  },
  newsLogoFallback: {
    width: 36,
    height: 36,
    borderRadius: 8,
    alignItems: 'center',
    justifyContent: 'center',
  },
  newsLogoText: {
    fontSize: 14,
    fontWeight: '700',
    color: '#FFFFFF',
  },
  newsContent: {
    flex: 1,
  },
  newsTickerRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
    marginBottom: 4,
  },
  newsTickerText: {
    fontSize: 12,
    fontWeight: '700',
    color: COLORS.primary,
  },
  sentimentBadgeSmall: {
    paddingHorizontal: 6,
    paddingVertical: 2,
    borderRadius: 4,
    marginLeft: 6,
  },
  sentimentTextSmall: {
    fontSize: 10,
    fontWeight: '700',
  },
  homepageEventBadge: {
    paddingHorizontal: 8,
    paddingVertical: 3,
    borderRadius: 999,
    backgroundColor: '#DBEAFE',
  },
  homepageEventBadgeText: {
    fontSize: 10,
    fontWeight: '700',
    color: '#2563EB',
  },
  newsTitle: {
    fontSize: 14,
    fontWeight: '500',
    color: COLORS.text,
    lineHeight: 20,
  },
  homepageEventSubtitle: {
    fontSize: 12,
    color: COLORS.textLight,
    lineHeight: 18,
    marginTop: 4,
  },
  newsMeta: {
    fontSize: 11,
    color: COLORS.textMuted,
    marginLeft: 'auto',
  },
  newsLoadingContainer: {
    alignItems: 'center',
    paddingVertical: 24,
    gap: 8,
  },
  newsLoadingText: {
    fontSize: 13,
    color: COLORS.textMuted,
  },
  noNewsText: {
    fontSize: 14,
    color: COLORS.textMuted,
    textAlign: 'center',
    paddingVertical: 24,
  },
  loadMoreButton: {
    alignItems: 'center',
    justifyContent: 'center',
    paddingVertical: 14,
    marginTop: 12,
    marginHorizontal: 8,
    backgroundColor: COLORS.primary + '10',
    borderRadius: 10,
    borderWidth: 1,
    borderColor: COLORS.primary + '30',
  },
  loadMoreText: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.primary,
  },

  // Modal Styles
  modalContainer: {
    flex: 1,
    backgroundColor: COLORS.background,
  },
  modalHeader: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    paddingHorizontal: 16,
    paddingVertical: 12,
    backgroundColor: COLORS.card,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  modalCloseButton: {
    padding: 12,
    marginLeft: -8,
  },
  modalHeaderTitle: {
    fontSize: 17,
    fontWeight: '600',
    color: COLORS.text,
  },
  modalExternalButton: {
    padding: 4,
  },
  modalScroll: {
    flex: 1,
  },
  modalScrollContent: {
    padding: 16,
  },
  articleHeader: {
    marginBottom: 16,
  },
  articleTickerRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 12,
    marginBottom: 8,
  },
  articleLogo: {
    width: 48,
    height: 48,
    borderRadius: 12,
    backgroundColor: '#F3F4F6',
  },
  articleLogoFallback: {
    width: 48,
    height: 48,
    borderRadius: 12,
    alignItems: 'center',
    justifyContent: 'center',
  },
  articleLogoText: {
    fontSize: 20,
    fontWeight: '700',
    color: '#FFFFFF',
  },
  articleTicker: {
    fontSize: 16,
    fontWeight: '700',
    color: COLORS.text,
  },
  articleCompany: {
    fontSize: 13,
    color: COLORS.textMuted,
  },
  articleMeta: {
    fontSize: 12,
    color: COLORS.textMuted,
  },
  articleTitle: {
    fontSize: 22,
    fontWeight: '700',
    color: COLORS.text,
    lineHeight: 30,
    marginBottom: 16,
  },
  sentimentRow: {
    flexDirection: 'row',
    marginBottom: 12,
  },
  sentimentBadge: {
    paddingHorizontal: 12,
    paddingVertical: 6,
    borderRadius: 16,
  },
  sentimentText: {
    fontSize: 12,
    fontWeight: '600',
  },
  tagsRow: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 8,
    marginBottom: 16,
  },
  tagBadge: {
    backgroundColor: COLORS.background,
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 12,
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  tagText: {
    fontSize: 11,
    color: COLORS.textLight,
  },
  articleContent: {
    fontSize: 16,
    lineHeight: 26,
    color: COLORS.text,
    marginBottom: 24,
  },
  readOriginalButton: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 8,
    paddingVertical: 14,
    backgroundColor: COLORS.card,
    borderRadius: 12,
    borderWidth: 1,
    borderColor: COLORS.primary,
  },
  readOriginalText: {
    fontSize: 15,
    fontWeight: '600',
    color: COLORS.primary,
  },
  
  // Notification Modal
  notifModalOverlay: {
    flex: 1,
    backgroundColor: 'rgba(0,0,0,0.5)',
    justifyContent: 'flex-end',
  },
  notifModalContent: {
    backgroundColor: COLORS.card,
    borderTopLeftRadius: 20,
    borderTopRightRadius: 20,
    maxHeight: '70%',
    minHeight: 300,
  },
  notifModalHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: 20,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  notifModalTitle: {
    fontSize: 20,
    fontWeight: '600',
    color: COLORS.text,
  },
  notifLoading: {
    padding: 40,
    alignItems: 'center',
  },
  notifEmpty: {
    padding: 40,
    alignItems: 'center',
  },
  notifEmptyText: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.text,
    marginTop: 16,
  },
  notifEmptySubtext: {
    fontSize: 14,
    color: COLORS.textMuted,
    textAlign: 'center',
    marginTop: 4,
  },
  notifList: {
    flex: 1,
  },
  notifItem: {
    flexDirection: 'row',
    padding: 16,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  notifItemUnseen: {
    backgroundColor: COLORS.primary + '08',
  },
  notifAvatar: {
    width: 36,
    height: 36,
    borderRadius: 18,
    marginRight: 12,
  },
  notifAvatarPlaceholder: {
    width: 36,
    height: 36,
    borderRadius: 18,
    backgroundColor: COLORS.background,
    justifyContent: 'center',
    alignItems: 'center',
    marginRight: 12,
  },
  notifContent: {
    flex: 1,
  },
  notifAuthor: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.text,
  },
  notifText: {
    fontSize: 13,
    color: COLORS.textLight,
    marginTop: 2,
  },
  notifSymbol: {
    fontSize: 12,
    fontWeight: '600',
    color: COLORS.primary,
    marginTop: 4,
  },
  notifUnseenDot: {
    width: 8,
    height: 8,
    borderRadius: 4,
    backgroundColor: COLORS.danger,
    alignSelf: 'center',
  },
  // My Stocks skeleton loading state
  stocksSkeletonContainer: {
    paddingVertical: 8,
    gap: 16,
  },
  stocksSkeletonRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 12,
  },
  stocksSkeletonAvatar: {
    width: 40,
    height: 40,
    borderRadius: 8,
    backgroundColor: COLORS.border,
  },
  stocksSkeletonLines: {
    flex: 1,
  },
  stocksSkeletonLine: {
    height: 12,
    borderRadius: 4,
    backgroundColor: COLORS.border,
  },
});
