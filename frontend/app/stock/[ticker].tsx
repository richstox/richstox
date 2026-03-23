/**
 * P34 (BINDING): Stock Detail Page
 * Fix 5: Star mismatch - refresh star state on focus (after returning from search)
 * Star reads from user_watchlist via /api/v1/watchlist/check/{ticker}
 * 
 * DO NOT CHANGE WITHOUT RICHARD APPROVAL (kurtarichard@gmail.com)
 */
import React, { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  TouchableOpacity,
  ActivityIndicator,
  RefreshControl,
  useWindowDimensions,
  Image,
  Linking,
  Platform,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useLocalSearchParams, useRouter, useFocusEffect } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import axios from 'axios';
import { COLORS } from '../_layout';
import Svg, { Path, Line, Text as SvgText, Circle, Rect, G } from 'react-native-svg';
import FinancialHub from '../../components/FinancialHub';
import BottomNav from '../../components/BottomNav';
import { MetricTooltip, TOOLTIP_CONTENT } from '../../components/MetricTooltip';
import AppHeader from '../../components/AppHeader';
import BrandedLoading from '../../components/BrandedLoading';
import { useSearchStore } from '../../stores/searchStore';
import { useAuth } from '../../contexts/AuthContext';
import { useLayoutSpacing } from '../../constants/layout';

const API_URL = process.env.EXPO_PUBLIC_BACKEND_URL;
const EODHD_LOGO_BASE = 'https://eodhd.com';
// Delay before fetching below-the-fold content (talk posts) to prioritize critical data
const DEFERRED_FETCH_MS = 800;

interface CompanyData {
  ticker: string;
  code: string;
  name: string;
  exchange: string;
  sector: string;
  industry: string;
  description: string;
  website: string;
  logo_url: string;
  full_time_employees: number;
  ipo_date: string;
  city: string;
  state: string;
  country_name: string;
  market_cap: number;
  pe_ratio: number;
  eps_ttm: number;
  beta: number;
  dividend_yield: number;
  fifty_two_week_high: number;
  fifty_two_week_low: number;
  pct_insiders: number;
  pct_institutions: number;
  profit_margin: number;
  roe: number;
  revenue_ttm: number;
}

interface PriceData {
  last_close: number;
  previous_close: number;
  change: number;
  change_pct: number;
  date: string;
}

interface FinancialsData {
  period_date: string;
  revenue: number;
  net_income: number;
  operating_cash_flow: number;
}

interface EarningsData {
  quarter_date: string;
  reported_eps: number;
  estimated_eps: number;
  surprise_pct: number;
  beat_miss: string;
}

interface InsiderData {
  status: string;
  buyers_count: number;
  sellers_count: number;
  total_buy_value_6m: number;
  total_sell_value_6m: number;
  net_value_6m: number;
}

interface StockOverview {
  ticker: string;
  company: CompanyData;
  price: PriceData;
  key_metrics: KeyMetrics;
  valuation: ValuationData | null;
  gradient_colors: Record<string, GradientColor>;
  peer_context: PeerContext | null;
  financials: { 
    annual?: { period_date: string; revenue: number | null; net_income: number | null }[];
    quarterly: FinancialsData[]; 
    ttm?: TTMData;
  } | null;
  earnings: EarningsData[] | null;
  insider_activity: InsiderData | null;
  dividends: DividendData | null;
  lite_mode: boolean;
  has_benchmark: boolean;
  fundamentals_pending?: boolean;
}

interface KeyMetrics {
  market_cap: number;
  pe_ratio: number;
  pe_ratio_source: string;
  pe_benchmark: number | null;
  eps_ttm: number;
  ps_ratio: number;
  ps_benchmark: number | null;
  pb_ratio: number;
  pb_benchmark: number | null;
  ev_ebitda: number;
  ev_ebitda_benchmark: number | null;
  net_margin_ttm: number;
  net_margin_benchmark: number | null;
  dividend_yield: number;
  dividend_yield_ttm: number;
  dividend_benchmark: number | null;
  beta: number;
  fifty_two_week_high: number;
  fifty_two_week_low: number;
  pct_insiders: number;
  pct_institutions: number;
}

interface ValuationData {
  score: number;
  status: string;
  status_label: string;
  net_adjustments: number;
  metrics_comparison: Record<string, MetricComparison>;
}

interface MetricComparison {
  company_value: number;
  benchmark_value: number;
  deviation_pct: number;
  status: string;
  adjustment: number;
}

interface GradientColor {
  deviation_pct: number;
  intensity: string;
  color_class: string;
  rgb: string;
}

interface PeerContext {
  industry: string;
  sector: string;
  company_count: number;
  has_sufficient_peers: boolean;
}

interface TTMData {
  revenue: number;
  net_income: number;
  ebitda: number;
}

interface DividendData {
  annual_dividends: { year: number; total: number; is_partial: boolean }[];
  recent_payments: { ex_date: string; amount: number }[];
  yoy_growth: number | null;
  status: string;
}

// Price range options for chart - including MAX
type PriceRange = '3M' | '6M' | 'YTD' | '1Y' | '3Y' | '5Y' | 'MAX';

interface PriceHistoryPoint {
  date: string;
  adjusted_close: number;
}

const RANGE_DAYS: Record<PriceRange, number | 'YTD' | 'MAX'> = {
  '3M': 90,
  '6M': 180,
  'YTD': 'YTD',
  '1Y': 365,
  '3Y': 1095,
  '5Y': 1825,
  'MAX': 'MAX',
};

// ============================================================================
// NEW MOBILE DETAIL API TYPES (RAW FACTS ONLY)
// ============================================================================

interface MobileDetailData {
  ticker: string;
  symbol: string;
  safety?: {
    type: 'standard' | 'spac_shell' | 'recent_ipo';
    badge_text: string | null;
    badge_color: 'amber' | 'blue' | null;
    tooltip: string | null;
  };
  company: {
    name: string;
    exchange: string;
    sector: string;
    industry: string;
    logo_url: string | null;
  };
  price: {
    current: number;
    as_of: string;
    daily_change: number;
    daily_change_pct: number;
  };
  reality_check: {
    total_return_pct: number;
    max_drawdown_pct: number;
    cagr_pct: number;
    benchmark_cagr_pct: number | null;
    outperformance_pct: number | null;
    efficiency_score: number | null;
    benchmark_start_date: string | null;
    start_date: string;
    end_date: string;
    years: number;
  } | null;
  // P25/P26: PAIN details from cache (exact dates from full daily series)
  pain: {
    pain_pct: number;  // Internal (positive)
    pain_percentage: number;  // UI display (negative, e.g., -89.7)
    pain_peak_date: string;
    pain_trough_date: string;
    pain_duration_days: number;
    pain_recovery_date: string | null;
    is_recovered: boolean;
  } | null;
  period_stats: {
    period: string;
    profit_pct: number;
    max_drawdown_pct: number;
    cagr_pct: number | null;
    benchmark_total_pct: number | null;
    outperformance_pct: number | null;
    start_date: string;
    end_date: string;
  } | null;
  valuation: {
    available: boolean;
    current_pe: number;
    peer_comparison: 'cheaper' | 'around' | 'more_expensive' | null;
    peer_median_pe: number | null;
    peer_count: number;
    peer_type: 'industry' | 'sector' | 'none';
    self_comparison: 'cheaper' | 'around' | 'more_expensive' | null;
    five_year_avg_pe: number | null;
    reason?: string;
    overall_vs_peers?: 'cheaper' | 'around' | 'more_expensive' | null;
    metrics_used?: number;
  } | null;
  // Hybrid 7 Key Metrics (P0)
  key_metrics?: {
    market_cap: { name: string; value: number | null; formatted: string | null; na_reason: string | null };
    shares_outstanding: { name: string; value: number | null; formatted: string | null; na_reason: string | null };
    net_margin_ttm: { name: string; value: number | null; formatted: string | null; na_reason: string | null };
    fcf_yield: { name: string; value: number | null; formatted: string | null; na_reason: string | null };
    net_debt_ebitda: { name: string; value: number | null; formatted: string | null; na_reason: string | null };
    revenue_growth_3y: { name: string; value: number | null; formatted: string | null; na_reason: string | null };
    dividend_yield_ttm: { name: string; value: number | null; formatted: string | null; na_reason: string | null };
  } | null;
  // Peer Transparency (P0)
  peer_transparency?: {
    total_industry_peers: number;
    valid_metric_peers: Record<string, number>;
    industry: string | null;
    group_type: 'industry' | 'sector' | 'market' | null;
  };
  company_details: {
    description: string | null;
    website: string | null;
    employees: number | null;
    ipo_date: string | null;
    address: string | null;
    phone: string | null;
  };
  // P8/P9: Financials data (5 essential metrics + prior_ttm)
  financials?: {
    annual: { 
      period_date: string; 
      revenue: number | null; 
      net_income: number | null;
      free_cash_flow: number | null;
      cash: number | null;
      total_debt: number | null;
    }[];
    quarterly: { 
      period_date: string; 
      revenue: number | null; 
      net_income: number | null;
      free_cash_flow: number | null;
      cash: number | null;
      total_debt: number | null;
    }[];
    ttm: {
      revenue: number | null;
      net_income: number | null;
      free_cash_flow: number | null;
      cash: number | null;
      total_debt: number | null;
    };
    prior_ttm?: {
      revenue: number | null;
      net_income: number | null;
      free_cash_flow: number | null;
    } | null;
  } | null;
}

export default function StockDetail() {
  const { ticker } = useLocalSearchParams();
  const router = useRouter();
  const { width } = useWindowDimensions();
  const { sessionToken } = useAuth();
  const sp = useLayoutSpacing();
  
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [data, setData] = useState<StockOverview | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [showFullDescription, setShowFullDescription] = useState(false);
  // P4: Single vertical scroll, no tabs - Key Metrics collapsed by default
  const [keyMetricsExpanded, setKeyMetricsExpanded] = useState(false); // Collapsed by default
  // P5: Collapsed sections with summary pills
  const [financialsExpanded, setFinancialsExpanded] = useState(false);
  const [earningsDividendsExpanded, setEarningsDividendsExpanded] = useState(false);
  const [insiderExpanded, setInsiderExpanded] = useState(false);
  
  // NEW: Mobile detail data (RAW FACTS ONLY)
  const [mobileData, setMobileData] = useState<MobileDetailData | null>(null);
  
  // P32: Watchlist/Follow state
  const [isFollowed, setIsFollowed] = useState(false);
  const [followLoading, setFollowLoading] = useState(false);
  
  // Company details accordion state
  const [companyDetailsExpanded, setCompanyDetailsExpanded] = useState(false);
  
  // Price chart state
  const [priceRange, setPriceRange] = useState<PriceRange>('MAX'); // P1 CRITICAL: Default to MAX
  const [chartData, setChartData] = useState<PriceHistoryPoint[]>([]);
  const [chartLoading, setChartLoading] = useState(false);
  const [chartError, setChartError] = useState<string | null>(null);
  
  // Chart tooltip state (CHART-TOOLTIP: simple hover/touch, like stockanalysis.com)
  const [chartTooltipVisible, setChartTooltipVisible] = useState(false);
  const [chartTooltipIndex, setChartTooltipIndex] = useState<number | null>(null);
  
  // Dividends state
  const [dividendPayments, setDividendPayments] = useState<{ex_date: string; amount: number}[]>([]);
  
  // Financials period toggle - handled internally by FinancialHub component
  
  // Benchmark chart data (SP500TR.INDX normalized to 100)
  const [benchmarkChartData, setBenchmarkChartData] = useState<{date: string; normalized: number}[]>([]);

  // Talk posts state
  const [talkPosts, setTalkPosts] = useState<any[]>([]);
  const [talkLoading, setTalkLoading] = useState(false);
  const [hasMoreTalk, setHasMoreTalk] = useState(false);

  // Valuation details expandable state
  const [valuationDetailsExpanded, setValuationDetailsExpanded] = useState(false);

  // P1 UX POLISH: Tooltip state for native BottomSheet
  const [tooltipVisible, setTooltipVisible] = useState(false);
  const [activeTooltip, setActiveTooltip] = useState<keyof typeof TOOLTIP_CONTENT>('rrr');
  
  // P1 UX POLISH: Valuation Overview collapsible state
  const [valuationExpanded, setValuationExpanded] = useState(false);

  // P1 UX POLISH: Show tooltip helper
  const showTooltip = (key: keyof typeof TOOLTIP_CONTENT) => {
    setActiveTooltip(key);
    setTooltipVisible(true);
  };

  // Search results navigation
  const { query: searchQuery, results: searchResults, clearSearch } = useSearchStore();
  const searchIndex = useMemo(
    () => searchResults.findIndex(r => r.ticker === ticker),
    [searchResults, ticker]
  );
  const hasSearchNav = searchIndex >= 0 && searchResults.length > 1;
  const prevTicker = searchIndex > 0 ? searchResults[searchIndex - 1].ticker : null;
  const nextTicker = searchIndex >= 0 && searchIndex < searchResults.length - 1 ? searchResults[searchIndex + 1].ticker : null;
  const navigateToTicker = useCallback((target: string) => {
    router.replace(`/stock/${target}`);
  }, [router]);

  // Swipe detection for search result navigation
  const swipeRef = useRef({ startX: 0, startY: 0 });

  const fetchMobileDetail = async (period: PriceRange = '1Y') => {
    try {
      const response = await axios.get(`${API_URL}/api/v1/ticker/${ticker}/detail?period=${period}`);
      setMobileData(response.data);
    } catch (err: any) {
      console.error('Error fetching mobile detail:', err.message || err);
    }
  };

  const fetchStock = async (lite = true) => {
    try {
      const response = await axios.get(`${API_URL}/api/stock-overview/${ticker}?lite=${lite}`);
      setData(response.data);
      setError(null);
    } catch (err: any) {
      console.error('Error fetching stock:', err);
      if (err.response?.status === 404) {
        setError('Stock not found');
      } else {
        setError('Failed to load stock data');
      }
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  // Fetch dividend history
  const fetchDividends = async () => {
    try {
      const response = await axios.get(`${API_URL}/api/dividends/${ticker}`);
      setDividendPayments(response.data.recent_payments || []);
    } catch (err) {
      console.error('Error fetching dividends:', err);
      setDividendPayments([]);
    }
  };

  // Fetch price history for chart (now includes benchmark)
  const fetchChartData = async (range: PriceRange) => {
    setChartLoading(true);
    setChartError(null);
    setBenchmarkChartData([]);
    
    try {
      // Fetch chart data with benchmark from new endpoint
      const response = await axios.get(`${API_URL}/api/v1/ticker/${ticker}/chart?period=${range}&include_benchmark=true`);
      const prices = response.data.prices || [];
      const benchmark = response.data.benchmark;
      
      // Downsample to ~400 points for performance
      const targetPoints = 400;
      const step = Math.max(1, Math.floor(prices.length / targetPoints));
      const downsampled = prices.filter((_: any, i: number) => i % step === 0 || i === prices.length - 1);
      
      // Convert to expected format
      const formattedPrices = downsampled.map((p: any) => ({
        date: p.date,
        open: p.close,
        high: p.close,
        low: p.close,
        close: p.close,
        adjusted_close: p.adjusted_close || p.close,
        volume: p.volume || 0,
        normalized: p.normalized
      }));
      
      setChartData(formattedPrices);
      
      // Set benchmark data (already normalized)
      if (benchmark && benchmark.prices) {
        const benchStep = Math.max(1, Math.floor(benchmark.prices.length / targetPoints));
        const benchDownsampled = benchmark.prices.filter((_: any, i: number) => i % benchStep === 0 || i === benchmark.prices.length - 1);
        setBenchmarkChartData(benchDownsampled);
      }
    } catch (err: any) {
      console.error('Error fetching chart data:', err);
      setChartError('Failed to load chart');
    } finally {
      setChartLoading(false);
    }
  };

  // P25/P26: PAIN data now comes from ticker_pain_cache via /v1/ticker/{ticker}/detail API

  useEffect(() => {
    if (!ticker) return;
    fetchStock(false);
    fetchDividends();
  }, [ticker]);

  useEffect(() => {
    if (ticker) {
      fetchChartData(priceRange);
      fetchMobileDetail(priceRange);
    }
  }, [ticker, priceRange]);

  // P34 Fix 5: Check if ticker is in watchlist
  const checkIfFollowed = useCallback(async () => {
    if (!sessionToken) {
      setIsFollowed(false);
      return;
    }
    try {
      // P33/P34: Use watchlist endpoint - source of truth
      const response = await axios.get(`${API_URL}/api/v1/watchlist/check/${ticker}`, {
        headers: { Authorization: `Bearer ${sessionToken}` },
      });
      setIsFollowed(response.data.is_followed || false);
    } catch (err) {
      console.error('Error checking follow status:', err);
      setIsFollowed(false);
    }
  }, [ticker, sessionToken]);

  // P34 Fix 5: Toggle follow status (Watchlist only, NOT Portfolio)
  const toggleFollow = async () => {
    if (followLoading) return;
    
    setFollowLoading(true);
    if (!sessionToken) {
      setFollowLoading(false);
      return;
    }
    const authHeaders = { Authorization: `Bearer ${sessionToken}` };
    try {
      if (isFollowed) {
        // Unfollow - remove from watchlist
        await axios.delete(`${API_URL}/api/v1/watchlist/${ticker}`, {
          headers: authHeaders,
        });
        setIsFollowed(false);
      } else {
        // Follow - add to watchlist
        await axios.post(`${API_URL}/api/v1/watchlist/${ticker}`, {}, {
          headers: authHeaders,
        });
        setIsFollowed(true);
      }
    } catch (err) {
      console.error('Error toggling follow:', err);
    } finally {
      setFollowLoading(false);
    }
  };

  // P34 Fix 5: Refresh star state when screen gains focus (after returning from search)
  useFocusEffect(
    useCallback(() => {
      if (ticker) {
        checkIfFollowed();
      }
    }, [ticker, checkIfFollowed])
  );

  // Fetch Talk posts for this stock
  const fetchTalkPosts = async () => {
    try {
      setTalkLoading(true);
      const response = await axios.get(`${API_URL}/api/v1/stocks/${ticker}/talk?limit=5&offset=0`);
      setTalkPosts(response.data.posts || []);
      setHasMoreTalk(response.data.has_more || false);
    } catch (err) {
      console.error('Error fetching talk posts:', err);
      setTalkPosts([]);
    } finally {
      setTalkLoading(false);
    }
  };

  // ===== CHART-TOOLTIP: Simple handlers (stockanalysis.com style) =====
  // Chart dimension constants (must match rendering)
  const CHART_PADDING_LEFT = 50;
  const CHART_PADDING_RIGHT = 10;
  const CHART_PADDING_TOP = 15;
  const CHART_PADDING_BOTTOM = 10;
  
  // Compute tooltip index from X coordinate
  const computeTooltipIndex = useCallback((locationX: number, chartWidth: number): number | null => {
    if (chartData.length === 0) return null;
    const graphW = chartWidth - CHART_PADDING_LEFT - CHART_PADDING_RIGHT;
    const relativeX = Math.max(0, Math.min(graphW, locationX - CHART_PADDING_LEFT));
    const ratio = relativeX / graphW;
    return Math.round(ratio * (chartData.length - 1));
  }, [chartData]);
  
  // Hide tooltip
  const hideChartTooltip = useCallback(() => {
    setChartTooltipVisible(false);
    setChartTooltipIndex(null);
  }, []);
  // ===== END CHART-TOOLTIP handlers =====

  // Fetch talk posts when ticker changes - deferred to avoid blocking initial render
  useEffect(() => {
    if (!ticker) return;
    // Defer talk posts fetch (below-the-fold content) until after critical data loads
    const timer = setTimeout(() => {
      fetchTalkPosts();
    }, DEFERRED_FETCH_MS);
    return () => clearTimeout(timer);
  }, [ticker]);

  const onRefresh = () => {
    setRefreshing(true);
    fetchStock(false); // P4: Always load full data for single vertical scroll
    fetchChartData(priceRange);
    fetchDividends();
    fetchTalkPosts();
  };
  
  // UTC-safe date formatter for MM/YY format
  const formatMMYY = (dateStr: string): string => {
    const d = new Date(dateStr + 'T00:00:00Z');
    const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
    const yy = String(d.getUTCFullYear()).slice(-2);
    return `${mm}/${yy}`;
  };

  // Calculate chart performance
  const chartPerformance = useMemo(() => {
    if (chartData.length < 2) return null;
    
    const startPrice = chartData[0].adjusted_close;
    const endPrice = chartData[chartData.length - 1].adjusted_close;
    const change = endPrice - startPrice;
    const changePercent = ((endPrice - startPrice) / startPrice) * 100;
    
    return {
      startPrice,
      endPrice,
      change,
      changePercent,
      isPositive: changePercent >= 0,
    };
  }, [chartData]);

  // Memoize drawdown peak/trough calculation (avoids recomputing 400+ points on every render)
  const drawdownDetails = useMemo(() => {
    if (chartData.length <= 10) return null;
    
    let peak = { idx: 0, value: chartData[0]?.adjusted_close || 0, date: chartData[0]?.date };
    let trough = { idx: 0, value: chartData[0]?.adjusted_close || 0, date: chartData[0]?.date };
    let maxDrawdown = 0;
    let runningMax = chartData[0]?.adjusted_close || 0;
    let runningMaxIdx = 0;
    
    chartData.forEach((d, i) => {
      if (d.adjusted_close > runningMax) {
        runningMax = d.adjusted_close;
        runningMaxIdx = i;
      }
      const drawdown = (runningMax - d.adjusted_close) / runningMax;
      if (drawdown > maxDrawdown) {
        maxDrawdown = drawdown;
        peak = { idx: runningMaxIdx, value: runningMax, date: chartData[runningMaxIdx]?.date };
        trough = { idx: i, value: d.adjusted_close, date: d.date };
      }
    });
    
    if (!peak.date || !trough.date || maxDrawdown <= 0.01) return null;
    
    const peakDate = new Date(peak.date + 'T00:00:00Z');
    const troughDate = new Date(trough.date + 'T00:00:00Z');
    const durationDays = Math.round((troughDate.getTime() - peakDate.getTime()) / (1000 * 60 * 60 * 24));
    
    let recoveryDate: string | null = null;
    for (let i = trough.idx + 1; i < chartData.length; i++) {
      if (chartData[i].adjusted_close >= peak.value) {
        recoveryDate = chartData[i].date;
        break;
      }
    }
    
    return { peak, trough, durationDays, recoveryDate };
  }, [chartData]);

  // P21: EU/CZ Number Formatting - import utility
  // Thousands separator: . (dot), Decimal separator: , (comma)
  const toEU = (value: number, decimals: number = 2): string => {
    if (value === null || value === undefined || isNaN(value)) return 'N/A';
    const fixed = value.toFixed(decimals);
    const [intPart, decPart] = fixed.split('.');
    const intWithDots = intPart.replace(/\B(?=(\d{3})+(?!\d))/g, '.');
    return decPart ? `${intWithDots},${decPart}` : intWithDots;
  };

  /**
   * P1 CRITICAL: RRR Formatting Rules
   * - RRR >= 100: show as integer (no decimals) e.g., 4869.93 -> "4 870"
   * - RRR < 100: show 1 decimal e.g., 1.03 -> "1,0"
   */
  const formatRRR = (rrr: number): string => {
    if (rrr >= 100) {
      return toEU(Math.round(rrr), 0);
    }
    return toEU(rrr, 1);
  };

  const formatCurrency = (value: number | null | undefined) => {
    if (!value) return 'N/A';
    if (value >= 1e12) return `$${toEU(value / 1e12, 2)}T`;
    if (value >= 1e9) return `$${toEU(value / 1e9, 2)}B`;
    if (value >= 1e6) return `$${toEU(value / 1e6, 2)}M`;
    return `$${toEU(value, 2)}`;
  };

  const formatNumber = (value: number | null | undefined) => {
    if (!value) return 'N/A';
    if (value >= 1e6) return `${toEU(value / 1e6, 1)}M`;
    if (value >= 1e3) return `${toEU(value / 1e3, 1)}K`;
    return toEU(value, 0);
  };

  const formatPercent = (value: number | null | undefined) => {
    if (value === null || value === undefined) return 'N/A';
    const sign = value >= 0 ? '+' : '';
    return `${sign}${toEU(value, 2)} %`;
  };

  // Format large percentages: >100% no decimals, thousands separated (EU style)
  const formatLargePercent = (value: number | null | undefined, showSign: boolean = true) => {
    if (value === null || value === undefined) return 'N/A';
    const sign = value >= 0 ? (showSign ? '+' : '') : '-';
    const absValue = Math.abs(value);
    
    if (absValue >= 1000) {
      return `${sign}${toEU(absValue, 0)} %`;
    } else if (absValue >= 100) {
      return `${sign}${toEU(absValue, 0)} %`;
    } else {
      return `${sign}${toEU(absValue, 1)} %`;
    }
  };

  /**
   * P1 FINAL: Compute RRR (Upside/Downside Ratio) for a price series
   * ALWAYS returns >= 0 or null (NEVER negative)
   * 
   * Formula:
   *   reward_hist = P_max - P_start (upside from start)
   *   risk_hist = P_start - P_min (downside from start)
   *   RRR = reward_hist / risk_hist
   * 
   * @param prices - Array of price points with adjusted_close
   * @returns RRR value (>= 0) or null if insufficient data or no downside risk
   */
  const computeRRR = (prices: { adjusted_close: number }[]): number | null => {
    if (!prices || prices.length < 2) return null;
    
    const closes = prices.map(p => p.adjusted_close).filter(c => c != null && c > 0);
    if (closes.length < 2) return null;
    
    const P_start = closes[0];           // First price in period
    const P_max = Math.max(...closes);   // Maximum price in period
    const P_min = Math.min(...closes);   // Minimum price in period
    
    const reward_hist = P_max - P_start; // Upside from start
    const risk_hist = P_start - P_min;   // Downside from start
    
    // Guard: if no downside risk (stock never fell below start) or invalid data, return null
    if (risk_hist <= 0) return null;
    
    // RRR = upside / downside (always >= 0)
    const rrr = reward_hist / risk_hist;
    
    return rrr;
  };

  // P22: Global date formatter - DD/MM/YYYY format (e.g., 23/02/2026)
  const formatDateDMY = (dateStr: string | null | undefined): string => {
    if (!dateStr) return 'N/A';
    const d = new Date(dateStr + 'T00:00:00Z');
    if (isNaN(d.getTime())) return 'N/A';
    const dd = String(d.getUTCDate()).padStart(2, '0');
    const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
    const yyyy = d.getUTCFullYear();
    return `${dd}/${mm}/${yyyy}`;
  };

  const getMarketCapLabel = (cap: number | null | undefined) => {
    if (!cap) return 'N/A';
    if (cap >= 300e9) return 'Mega Cap';
    if (cap >= 10e9) return 'Large Cap';
    if (cap >= 2e9) return 'Mid Cap';
    if (cap >= 300e6) return 'Small Cap';
    return 'Micro Cap';
  };

  // FIXED: Helper to display metric values with N/A reason codes
  // P1 UI OVERHAUL: Simplified labels - "Unprofitable" instead of verbose warnings
  // P1 UX POLISH: Format metric with data-empathetic N/A reasons (NO "Missing data")
  const formatMetricWithReason = (metric: { value: number | null; formatted: string | null; na_reason: string | null } | undefined) => {
    if (!metric) return 'N/A';
    if (metric.formatted) return metric.formatted;
    if (metric.na_reason) {
      // P1 UX: Data-empathetic N/A reasons in English
      const reasonLabels: Record<string, string> = {
        'unprofitable': 'Unprofitable',
        'negative_earnings': 'Unprofitable',
        'negative_ebitda': 'N/A (Negative EBITDA)',
        'missing_shares': 'N/A (Not reported)',
        'missing_data': 'N/A (Not reported)',
        'missing_cf_data': 'N/A (Cash flow not reported)',
        'missing_debt_data': 'N/A (Debt data missing)',
        'insufficient_history': 'N/A (Insufficient history)',
        'negative_value': 'Negative',
        'negative_fcf': 'Burning cash',
        'missing_revenue': 'N/A (Revenue not reported)',
        'near_zero_denominator': 'N/A (Negative EBITDA)',
        'no_dividend': '0.00% (No dividend)',
        'price_missing': 'N/A (Price or shares missing)',
        'shares_missing': 'N/A (Price or shares missing)',
        'income_missing': 'N/A (Income not reported)',
        'ebitda_missing': 'N/A (EBITDA not reported)',
        'data_pending': 'N/A (Data pending)',
      };
      return reasonLabels[metric.na_reason] || 'N/A';
    }
    return 'N/A';
  };

  // P1 UX POLISH: Format Key Metric with empathetic reason + industry context for dividend
  const formatKeyMetricWithEmpathy = (
    metric: any, 
    metricType: string,
    industryDivMedian?: number | null
  ): { text: string; color: string } => {
    const MUTED = COLORS.textMuted;
    const RED = '#EF4444';
    const TEXT = COLORS.text;
    
    // Empathetic N/A mapping
    const empathyMap: Record<string, Record<string, string>> = {
      market_cap: {
        'price_missing': 'N/A (Price or shares missing)',
        'shares_missing': 'N/A (Price or shares missing)',
        'default': 'N/A (Not calculated)'
      },
      shares_outstanding: {
        'not_reported': 'N/A (Not reported)',
        'default': 'N/A (Not reported)'
      },
      net_margin_ttm: {
        'income_missing': 'N/A (Income not reported)',
        'revenue_missing': 'N/A (Revenue not reported)',
        'unprofitable': 'Unprofitable',
        'negative_earnings': 'Unprofitable',
        'default': 'N/A (Income not reported)'
      },
      fcf_yield: {
        'fcf_missing': 'N/A (Cash flow not reported)',
        'missing_cf_data': 'N/A (Cash flow not reported)',
        'market_cap_missing': 'N/A (Market cap missing)',
        'negative_fcf': 'Burning cash',
        'default': 'N/A (Cash flow not reported)'
      },
      net_debt_ebitda: {
        'negative_ebitda': 'N/A (Negative EBITDA)',
        'near_zero_denominator': 'N/A (Negative EBITDA)',
        'ebitda_missing': 'N/A (EBITDA not reported)',
        'missing_debt_data': 'N/A (Debt data missing)',
        'missing_cash_data': 'N/A (Cash data missing)',
        'default': 'N/A (Data missing)'
      },
      revenue_growth_3y: {
        'insufficient_history': 'N/A (Insufficient history)',
        'data_pending': 'N/A (Data pending)',
        'default': 'N/A (Insufficient history)'
      },
      dividend_yield_ttm: {
        'no_dividend': industryDivMedian !== null && industryDivMedian !== undefined
          ? `0.00% (Industry avg: ${toEU(industryDivMedian, 2)}%)`
          : '0.00% (No dividend)',
        'default': '0.00% (No dividend)'
      }
    };
    
    // Has valid value
    if (metric?.value !== null && metric?.value !== undefined) {
      const isNegative = metric.value < 0;
      
      // D) Dividend with industry context + RED color rule
      if (metricType === 'dividend_yield_ttm') {
        const industryText = industryDivMedian !== null && industryDivMedian !== undefined
          ? ` (Industry avg: ${toEU(industryDivMedian, 2)}%)`
          : '';
        // D) RED if dividend == 0% AND industry median > 1% (opportunity cost)
        const isOpportunityCost = metric.value === 0 && industryDivMedian !== null && industryDivMedian > 1;
        return {
          text: `${toEU(metric.value, 2)}%${industryText}`,
          color: isOpportunityCost ? RED : (metric.value === 0 ? MUTED : TEXT)
        };
      }
      
      // Percentage metrics
      if (['net_margin_ttm', 'fcf_yield'].includes(metricType)) {
        const label = isNegative && metricType === 'net_margin_ttm' ? ' (Unprofitable)' : 
                     isNegative && metricType === 'fcf_yield' ? ' (Burning cash)' : '';
        return {
          text: `${toEU(metric.value, 1)}%${label}`,
          color: isNegative ? RED : TEXT
        };
      }
      
      // Ratio metrics
      if (metricType === 'net_debt_ebitda') {
        return {
          text: `${toEU(metric.value, 1)}x`,
          color: TEXT
        };
      }
      
      // Revenue growth
      if (metricType === 'revenue_growth_3y') {
        return {
          text: `${toEU(metric.value, 1)}%`,
          color: isNegative ? RED : TEXT
        };
      }
      
      // Format based on type (Market Cap, Shares)
      return {
        text: metric.formatted || `${metric.value}`,
        color: TEXT
      };
    }
    
    // No value - use empathetic N/A reason
    const reasonMap = empathyMap[metricType] || {};
    const naReason = metric?.na_reason || 'default';
    const displayText = reasonMap[naReason] || reasonMap['default'] || 'N/A';
    
    // Red for negative states, muted for N/A
    const isNegativeState = ['unprofitable', 'negative_fcf', 'burning_cash', 'negative_earnings'].includes(naReason);
    
    return {
      text: displayText,
      color: isNegativeState ? RED : MUTED
    };
  };

  // P1 UX POLISH: Valuation Pulse calculation (Peer + 5Y integrated)
  const getValuationPulse = (valuation: any): { 
    label: string; 
    color: string; 
    delta: number | null; 
    source: 'peers' | '5y_avg' | null 
  } => {
    if (!valuation?.metrics) {
      return { label: 'N/A', color: COLORS.textMuted, delta: null, source: null };
    }
    
    const metrics = valuation.metrics;
    const peerDeltas: number[] = [];
    const fiveYearDeltas: number[] = [];
    
    // Collect deltas with explicit null/undefined checks
    const metricKeys = ['pe', 'ps', 'pb', 'ev_ebitda', 'ev_revenue'];
    
    metricKeys.forEach(key => {
      const metric = metrics[key];
      if (metric?.current !== null && metric?.current !== undefined) {
        // Peer median delta (explicit null check)
        if (metric.peer_median !== null && metric.peer_median !== undefined && metric.peer_median !== 0) {
          const pctDelta = ((metric.current - metric.peer_median) / metric.peer_median) * 100;
          peerDeltas.push(pctDelta);
        }
        // 5Y average delta (explicit null check)
        if (metric.avg_5y !== null && metric.avg_5y !== undefined && metric.avg_5y !== 0) {
          const pctDelta5Y = ((metric.current - metric.avg_5y) / metric.avg_5y) * 100;
          fiveYearDeltas.push(pctDelta5Y);
        }
      }
    });
    
    // Determine PRIMARY signal
    let primaryDeltas: number[];
    let source: 'peers' | '5y_avg' | null;
    
    if (peerDeltas.length > 0) {
      primaryDeltas = peerDeltas;
      source = 'peers';
    } else if (fiveYearDeltas.length > 0) {
      primaryDeltas = fiveYearDeltas;
      source = '5y_avg';
    } else {
      return { label: 'N/A', color: COLORS.textMuted, delta: null, source: null };
    }
    
    // Calculate median
    primaryDeltas.sort((a, b) => a - b);
    const mid = Math.floor(primaryDeltas.length / 2);
    const medianDelta = primaryDeltas.length % 2 
      ? primaryDeltas[mid] 
      : (primaryDeltas[mid - 1] + primaryDeltas[mid]) / 2;
    
    // Classify
    let label: string;
    let color: string;
    if (medianDelta <= -20) {
      label = 'DISCOUNTED';
      color = '#10B981'; // green
    } else if (medianDelta >= 20) {
      label = 'OVERHEATED';
      color = '#EF4444'; // red
    } else {
      label = 'ALIGNED';
      color = '#F59E0B'; // yellow/orange
    }
    
    return { 
      label, 
      color, 
      delta: Math.round(medianDelta),
      source
    };
  };

  if (loading) {
    return <BrandedLoading message={`Loading ${ticker}...`} />;
  }

  if (error || !data?.company) {
    return (
      <SafeAreaView style={styles.container} edges={['left', 'right', 'bottom']}>
        <AppHeader title={String(ticker || 'RICHSTOX')} showSubscriptionBadge={false} />
        <View style={styles.errorContainer}>
          <Ionicons name="alert-circle-outline" size={64} color={COLORS.textMuted} />
          <Text style={styles.errorText}>{error || 'No data available'}</Text>
          <TouchableOpacity style={styles.retryButton} onPress={() => fetchStock()}>
            <Text style={styles.retryText}>Try Again</Text>
          </TouchableOpacity>
        </View>
      </SafeAreaView>
    );
  }

  const company = data.company;
  const price = data.price;
  const logoUrl = company.logo_url ? `${EODHD_LOGO_BASE}${company.logo_url}` : null;

  // =============================================================================
  // P5: SUMMARY PILLS LOGIC (Honest Data - never guess)
  // =============================================================================
  
  /**
   * A) Key Metrics Pills (max 2 takeaways)
   * Priority order: Unprofitable > High debt > FCF negative > 3Y rev down > Dividend: none
   */
  /**
   * P6: RICHSTOX ELITE MATRIX - Key Metrics Summary Pills
   * 
   * Rules:
   * - Max 2 pills ("Rule of 2")
   * - Risk/Warn pills have priority over positive pills
   * - Priority cascade: evaluate in exact order, stop after 2 matches
   * - "In line" if no triggers match and data exists
   * - "N/A (Data pending)" if critical data missing
   */
  const getKeyMetricsPills = (): string[] => {
    if (!mobileData?.key_metrics) return ['N/A (Data pending)'];
    
    const km = mobileData.key_metrics;
    const pills: string[] = [];
    
    // Count how many metrics have valid data
    // Backend returns null for missing metrics, or an object with {value, na_reason} for present ones
    const hasMargin = km.net_margin_ttm != null && km.net_margin_ttm.value != null;
    const hasDebt = km.net_debt_ebitda != null && (km.net_debt_ebitda.value != null || km.net_debt_ebitda.na_reason != null);
    const hasFCF = km.fcf_yield != null && km.fcf_yield.value != null;
    const hasRevGrowth = km.revenue_growth_3y != null && km.revenue_growth_3y.value != null;
    const hasDividend = km.dividend_yield_ttm != null && km.dividend_yield_ttm.value != null;
    
    const validMetricsCount = [hasMargin, hasDebt, hasFCF, hasRevGrowth, hasDividend].filter(Boolean).length;
    if (validMetricsCount < 2) return ['N/A (Data pending)'];
    
    // ========================================================================
    // PRIORITY 1-5: RISK & WARNING (always checked first)
    // ========================================================================
    
    // 1. Unprofitable - safely extract value first
    const marginValue = km.net_margin_ttm?.value;
    const isUnprofitable = 
      (typeof marginValue === 'number' && marginValue < 0) ||
      km.net_debt_ebitda?.na_reason === 'unprofitable' ||
      km.fcf_yield?.na_reason === 'unprofitable';
    if (pills.length < 2 && isUnprofitable) {
      pills.push('Unprofitable');
    }
    
    // 2. Overleveraged (Net Debt/EBITDA > 4.0)
    const debtValue = km.net_debt_ebitda?.value;
    if (pills.length < 2 && typeof debtValue === 'number' && debtValue > 4.0) {
      pills.push('Overleveraged');
    }
    
    // 3. Burning Cash (FCF Yield < 0)
    const fcfValue = km.fcf_yield?.value;
    if (pills.length < 2 && typeof fcfValue === 'number' && fcfValue < 0) {
      pills.push('Burning Cash');
    }
    
    // 4. Revenue Decline (3Y CAGR < 0)
    const revGrowthValue = km.revenue_growth_3y?.value;
    if (pills.length < 2 && typeof revGrowthValue === 'number' && revGrowthValue < 0) {
      pills.push('Revenue Decline');
    }
    
    // 5. Debt: N/A (missing debt data)
    if (pills.length < 2 && km.net_debt_ebitda?.na_reason === 'missing_debt_data') {
      pills.push('Debt: N/A');
    }
    
    // ========================================================================
    // PRIORITY 6-10: STRENGTH & ELITE (only if no risk pills matched)
    // ========================================================================
    
    const hasRiskPills = pills.length > 0;
    
    // 6. Profit Leader (Net Margin > 20%)
    if (pills.length < 2 && !hasRiskPills && typeof marginValue === 'number' && marginValue > 0.20) {
      pills.push('Profit Leader');
    }
    
    // 7. Cash King (Net Debt/EBITDA < 0 = net cash position)
    if (pills.length < 2 && !hasRiskPills && typeof debtValue === 'number' && debtValue < 0) {
      pills.push('Cash King');
    }
    
    // 8. FCF Powerhouse (FCF Yield > 8%)
    if (pills.length < 2 && !hasRiskPills && typeof fcfValue === 'number' && fcfValue > 0.08) {
      pills.push('FCF Powerhouse');
    }
    
    // 9. Hyper Growth (3Y Revenue CAGR > 25%)
    if (pills.length < 2 && !hasRiskPills && typeof revGrowthValue === 'number' && revGrowthValue > 0.25) {
      pills.push('Hyper Growth');
    }
    
    // 10. Dividend Elite (Dividend Yield > 4%)
    const dividendValue = km.dividend_yield_ttm?.value;
    if (pills.length < 2 && !hasRiskPills && typeof dividendValue === 'number' && dividendValue > 0.04) {
      pills.push('Dividend Elite');
    }
    
    // ========================================================================
    // FALLBACK: "In line" if no triggers matched but data exists
    // ========================================================================
    if (pills.length === 0) {
      return ['In line'];
    }
    
    // If only 1 pill and it's positive, add "In line" as second
    if (pills.length === 1 && !hasRiskPills) {
      pills.push('In line');
    }
    
    return pills;
  };
  
  /**
   * B) Dividends Pill (exactly one)
   * Growing / Stable / Cutting / No dividends / N/A (Insufficient history)
   */
  const getDividendPill = (): string => {
    if (!dividendPayments || dividendPayments.length === 0) {
      return 'No dividends';
    }
    
    if (dividendPayments.length < 4) {
      return 'N/A (Insufficient history)';
    }
    
    // Compare first half vs second half of payments
    const midpoint = Math.floor(dividendPayments.length / 2);
    const recentAvg = dividendPayments.slice(0, midpoint).reduce((sum, d) => sum + d.amount, 0) / midpoint;
    const olderAvg = dividendPayments.slice(midpoint).reduce((sum, d) => sum + d.amount, 0) / (dividendPayments.length - midpoint);
    
    if (olderAvg === 0) return 'N/A (Insufficient history)';
    
    const changeRatio = recentAvg / olderAvg;
    
    if (changeRatio > 1.05) return 'Growing';
    if (changeRatio < 0.95) return 'Cutting';
    return 'Stable';
  };
  
  /**
   * C) Financials Pill (exactly one)
   * Revenue up / Revenue down / No financials
   */
  const getFinancialsPill = (): string => {
    // P8 FIX: Use mobileData.financials (from new API) or fallback to data.financials
    const financials = mobileData?.financials || data?.financials;
    if (!financials) return 'No financials';
    
    const annual = financials.annual || [];
    if (annual.length < 2) return 'N/A (Insufficient history)';
    
    // Compare two most recent years
    const latest = annual[0]?.revenue;
    const previous = annual[1]?.revenue;
    
    if (!latest || !previous || previous === 0) return 'N/A (Insufficient history)';
    
    const yoyGrowth = (latest - previous) / previous;
    
    if (yoyGrowth > 0) return 'Revenue up';
    if (yoyGrowth < 0) return 'Revenue down';
    return 'Revenue flat';
  };

  // Pill component for rendering
  const SummaryPill = ({ label, variant = 'neutral' }: { label: string; variant?: 'negative' | 'positive' | 'neutral' }) => (
    <View style={[
      styles.summaryPill,
      variant === 'negative' && styles.summaryPillNegative,
      variant === 'positive' && styles.summaryPillPositive,
    ]}>
      <Text style={[
        styles.summaryPillText,
        variant === 'negative' && styles.summaryPillTextNegative,
        variant === 'positive' && styles.summaryPillTextPositive,
      ]}>{label}</Text>
    </View>
  );

  // P6: Determine pill variant based on Elite Matrix labels
  const getPillVariant = (label: string): 'negative' | 'positive' | 'neutral' => {
    // Risk & Warning pills (negative/red)
    const negativeLabels = [
      'Unprofitable', 'Overleveraged', 'Burning Cash', 'Revenue Decline', 'Debt: N/A',
      'No dividends', 'Cutting', 'Revenue down', 'No financials'
    ];
    // Strength & Elite pills (positive/green)
    const positiveLabels = [
      'Profit Leader', 'Cash King', 'FCF Powerhouse', 'Hyper Growth', 'Dividend Elite',
      'Growing', 'Revenue up', 'Stable'
    ];
    
    if (negativeLabels.some(l => label === l)) return 'negative';
    if (positiveLabels.some(l => label === l)) return 'positive';
    return 'neutral'; // "In line", "N/A (Missing data)", etc.
  };

  return (
    <SafeAreaView
      style={styles.container}
      edges={['left', 'right', 'bottom']}
      onTouchStart={(e: any) => {
        swipeRef.current = { startX: e.nativeEvent.pageX, startY: e.nativeEvent.pageY };
      }}
      onTouchEnd={(e: any) => {
        if (!hasSearchNav) return;
        const dx = e.nativeEvent.pageX - swipeRef.current.startX;
        const dy = e.nativeEvent.pageY - swipeRef.current.startY;
        if (Math.abs(dx) > 80 && Math.abs(dx) > Math.abs(dy) * 2) {
          if (dx < 0 && nextTicker) navigateToTicker(nextTicker);
          else if (dx > 0 && prevTicker) navigateToTicker(prevTicker);
        }
      }}
    >
      {/* Persistent Top Bar */}
      <AppHeader title={company.code} showSubscriptionBadge={false} />

      {/* Search results navigation bar */}
      {hasSearchNav && (
        <View style={styles.searchNavBar}>
          <TouchableOpacity
            style={[styles.searchNavButton, !prevTicker && styles.searchNavButtonDisabled]}
            onPress={() => prevTicker && navigateToTicker(prevTicker)}
            disabled={!prevTicker}
          >
            <Ionicons name="chevron-back" size={16} color={prevTicker ? COLORS.primary : COLORS.textMuted} />
            {prevTicker && <Text style={styles.searchNavTicker} numberOfLines={1}>{prevTicker}</Text>}
          </TouchableOpacity>

          <View style={styles.searchNavCenter}>
            <Text style={styles.searchNavCounter}>
              {searchIndex + 1} of {searchResults.length}
            </Text>
            <TouchableOpacity
              style={styles.searchNavClose}
              onPress={clearSearch}
            >
              <Ionicons name="close" size={16} color={COLORS.textMuted} />
            </TouchableOpacity>
          </View>

          <TouchableOpacity
            style={[styles.searchNavButton, styles.searchNavButtonRight, !nextTicker && styles.searchNavButtonDisabled]}
            onPress={() => nextTicker && navigateToTicker(nextTicker)}
            disabled={!nextTicker}
          >
            {nextTicker && <Text style={styles.searchNavTicker} numberOfLines={1}>{nextTicker}</Text>}
            <Ionicons name="chevron-forward" size={16} color={nextTicker ? COLORS.primary : COLORS.textMuted} />
          </TouchableOpacity>
        </View>
      )}

      <ScrollView
        style={styles.scrollView}
        contentContainerStyle={[styles.scrollContent, { padding: sp.pageGutter }]}
        showsVerticalScrollIndicator={false}
        refreshControl={<RefreshControl refreshing={refreshing} onRefresh={onRefresh} />}
      >
        {/* ===== FUNDAMENTALS PENDING BANNER ===== */}
        {data?.fundamentals_pending && (
          <View style={styles.pendingBanner} data-testid="fundamentals-pending-banner">
            <Ionicons name="time-outline" size={18} color="#D97706" />
            <View style={styles.pendingBannerContent}>
              <Text style={styles.pendingBannerTitle}>Fundamental Data Pending</Text>
              <Text style={styles.pendingBannerText}>
                PE ratio, market cap, and other metrics will be available soon.
              </Text>
            </View>
          </View>
        )}

        {/* ===== COMPACT HEADER ROW (ticker + name + sector/industry) ===== */}
        <View style={styles.compactHeader} data-testid="compact-header">
          {/* Logo */}
          {logoUrl ? (
            <Image source={{ uri: logoUrl }} style={styles.compactLogo} resizeMode="contain" />
          ) : (
            <View style={styles.compactLogoPlaceholder}>
              <Text style={styles.compactLogoText}>{company.code?.charAt(0) || ticker?.toString().charAt(0)}</Text>
            </View>
          )}
          
          {/* Name & Classification */}
          <View style={styles.compactInfo}>
            <View style={styles.compactNameRow}>
              <Text style={styles.compactName} numberOfLines={1}>{company.name || ticker}</Text>
              {company.exchange && (
                <View style={styles.exchangePill}>
                  <Text style={styles.exchangePillText}>{company.exchange}</Text>
                </View>
              )}
            </View>
            
            {/* Safety Badge - on its own row for visibility */}
            {mobileData?.safety && mobileData.safety.type !== 'standard' && mobileData.safety.badge_text && (
              <View style={styles.safetyBadgeRow}>
                <View 
                  style={[
                    styles.safetyBadge,
                    mobileData.safety.badge_color === 'amber' ? styles.safetyBadgeAmber : styles.safetyBadgeBlue
                  ]}
                  data-testid="safety-badge"
                >
                  <Ionicons 
                    name={mobileData.safety.badge_color === 'amber' ? 'warning-outline' : 'time-outline'} 
                    size={12} 
                    color={mobileData.safety.badge_color === 'amber' ? '#92400E' : '#1E40AF'} 
                  />
                  <Text 
                    style={[
                      styles.safetyBadgeText,
                      mobileData.safety.badge_color === 'amber' ? styles.safetyBadgeTextAmber : styles.safetyBadgeTextBlue
                    ]}
                  >
                    {mobileData.safety.badge_text}
                  </Text>
                </View>
                {mobileData.safety.tooltip && (
                  <Text style={styles.safetyTooltipInline} numberOfLines={2}>
                    {mobileData.safety.tooltip}
                  </Text>
                )}
              </View>
            )}
            
            {/* Sector & Industry on separate line */}
            <View style={styles.classificationRow}>
              {company.sector && (
                <Text style={styles.classificationText}>
                  Sector: <Text style={styles.classificationValue}>{company.sector}</Text>
                </Text>
              )}
              {company.industry && (
                <Text style={styles.classificationText}>
                  {' · '}Industry: <Text style={styles.classificationValue}>{company.industry}</Text>
                </Text>
              )}
            </View>
          </View>
        </View>

        {/* ===== COMPANY DETAILS (Minimal one-line, collapsed) ===== */}
        <TouchableOpacity 
          style={styles.companyDetailsMinimal}
          onPress={() => setCompanyDetailsExpanded(!companyDetailsExpanded)}
          data-testid="company-details-toggle"
        >
          <Ionicons 
            name={companyDetailsExpanded ? 'chevron-up' : 'chevron-down'} 
            size={14} 
            color={COLORS.textMuted} 
          />
          <Text style={styles.companyDetailsMinimalText}>Company Details</Text>
        </TouchableOpacity>
        
        {companyDetailsExpanded && (
          <View style={styles.companyDetailsExpanded}>
            {(company.city || company.state || company.country_name) && (
              <View style={styles.detailRowCompact}>
                <Ionicons name="location-outline" size={14} color={COLORS.textMuted} />
                <Text style={styles.detailTextCompact}>
                  {[company.city, company.state, company.country_name].filter(Boolean).join(', ')}
                </Text>
              </View>
            )}
            {company.website && (
              <TouchableOpacity 
                style={styles.detailRowCompact}
                onPress={() => Linking.openURL(company.website)}
              >
                <Ionicons name="globe-outline" size={14} color={COLORS.accent} />
                <Text style={[styles.detailTextCompact, styles.linkText]} numberOfLines={1}>
                  {company.website.replace(/^https?:\/\//, '').replace(/\/$/, '')}
                </Text>
              </TouchableOpacity>
            )}
            {company.full_time_employees && (
              <View style={styles.detailRowCompact}>
                <Ionicons name="people-outline" size={14} color={COLORS.textMuted} />
                <Text style={styles.detailTextCompact}>{formatNumber(company.full_time_employees)} employees</Text>
              </View>
            )}
            {company.ipo_date && (
              <View style={styles.detailRowCompact}>
                <Ionicons name="calendar-outline" size={14} color={COLORS.textMuted} />
                <Text style={styles.detailTextCompact}>IPO: {formatDateDMY(company.ipo_date)}</Text>
              </View>
            )}
            {company.description && (
              <>
                <Text 
                  style={styles.descriptionTextCompact} 
                  numberOfLines={showFullDescription ? undefined : 3}
                >
                  {company.description}
                </Text>
                {company.description.length > 150 && (
                  <TouchableOpacity onPress={() => setShowFullDescription(!showFullDescription)}>
                    <Text style={styles.showMoreText}>
                      {showFullDescription ? 'Show less' : 'Show more'}
                    </Text>
                  </TouchableOpacity>
                )}
              </>
            )}
          </View>
        )}

        {/* ===== PRICE CARD ===== */}
        {price && (
          <View style={styles.priceCard}>
            <View style={styles.priceRow}>
              <Text style={styles.priceValue}>{formatCurrency(price.last_close)}</Text>
              <View style={[
                styles.changeChip,
                (price.change_pct || 0) >= 0 ? styles.positiveChip : styles.negativeChip
              ]}>
                <Ionicons 
                  name={(price.change_pct || 0) >= 0 ? 'trending-up' : 'trending-down'} 
                  size={14} 
                  color={(price.change_pct || 0) >= 0 ? '#10B981' : '#EF4444'} 
                />
                <Text style={[
                  styles.changeText,
                  (price.change_pct || 0) >= 0 ? styles.positiveText : styles.negativeText
                ]}>
                  {formatPercent(price.change_pct)}
                </Text>
              </View>
            </View>
            <Text style={styles.priceDate}>as of {formatDateDMY(price.date)}</Text>
          </View>
        )}

        {/* ===== PRICE CHART (P1 UX: Chart-first flow) ===== */}
        <View 
          style={styles.sectionCard} 
          data-testid="price-chart-card"
        >
          <View style={styles.sectionHeader}>
            <Text style={styles.sectionIcon}>📈</Text>
            <Text style={styles.sectionTitleBold}>Price History</Text>
          </View>
          
          {/* Range Selector */}
          <ScrollView 
            horizontal 
            showsHorizontalScrollIndicator={false}
            contentContainerStyle={styles.rangeSelectorContent}
            style={styles.rangeSelectorScroll}
          >
            {(['3M', '6M', 'YTD', '1Y', '3Y', '5Y', 'MAX'] as PriceRange[]).map((range) => (
              <TouchableOpacity
                key={range}
                style={[styles.rangeButton, priceRange === range && styles.rangeButtonActive]}
                onPress={() => setPriceRange(range)}
                data-testid={`range-btn-${range}`}
              >
                <Text style={[styles.rangeButtonText, priceRange === range && styles.rangeButtonTextActive]}>
                  {range}
                </Text>
              </TouchableOpacity>
            ))}
          </ScrollView>
          
          {chartData.length > 0 && (
            <Text style={styles.dateRangeText}>
              {formatDateDMY(chartData[0]?.date)} – {formatDateDMY(chartData[chartData.length - 1]?.date)}
            </Text>
          )}
          
          <View style={styles.chartContainer}>
            {chartLoading ? (
              <View style={styles.chartLoading}>
                <ActivityIndicator size="small" color={COLORS.primary} />
                <Text style={styles.chartLoadingText}>Loading chart...</Text>
              </View>
            ) : chartError ? (
              <View style={styles.chartLoading}>
                <Text style={styles.chartErrorText}>{chartError}</Text>
                <TouchableOpacity style={styles.chartRetryButton} onPress={() => fetchChartData(priceRange)}>
                  <Text style={styles.chartRetryText}>Retry</Text>
                </TouchableOpacity>
              </View>
            ) : chartData.length > 0 ? (
              (() => {
                const chartW = width - 48;
                const chartH = 180;
                
                // Chart label positioning with deterministic stacking
                // Rules:
                // - HIGH is always top-most, LOW is always bottom-most, CURRENT is between
                // - If CURRENT == LOW (same formatted value): merge into LOW
                // - If CURRENT == HIGH (same formatted value): merge into HIGH
                type ChartLabelData = {
                  id: 'high' | 'low' | 'current';
                  adjustedY: number;
                  text: string;
                  color: string;
                };
                
                const computeChartLabels = (
                  highY: number, lowY: number, currentY: number,
                  dataMax: number, dataMin: number, currentPrice: number,
                  chartHeight: number,
                  formatPriceFn: (p: number) => string
                ): ChartLabelData[] => {
                  const LABEL_HEIGHT = 14;
                  const MIN_GAP = 2;
                  const TOP_BOUND = 8;
                  const BOTTOM_BOUND = chartHeight - 4;
                  const SPACING = LABEL_HEIGHT + MIN_GAP;
                  
                  // Step 1: Compute formatted strings
                  const formattedHigh = formatPriceFn(dataMax);
                  const formattedLow = formatPriceFn(dataMin);
                  const formattedCurrent = formatPriceFn(currentPrice);
                  
                  // Step 2: Determine which labels to render (merge rule)
                  const mergeWithLow = formattedCurrent === formattedLow;
                  const mergeWithHigh = formattedCurrent === formattedHigh;
                  const showCurrent = !mergeWithLow && !mergeWithHigh;
                  
                  // Step 3: Clamp helper
                  const clamp = (val: number, min: number, max: number) => Math.max(min, Math.min(max, val));
                  
                  // Step 4: Compute adjusted positions with hard constraints
                  // HIGH is always at top, LOW is always at bottom
                  let adjustedHighY = clamp(highY, TOP_BOUND, BOTTOM_BOUND - SPACING);
                  let adjustedLowY = clamp(lowY, TOP_BOUND + SPACING, BOTTOM_BOUND);
                  
                  // Ensure LOW is below HIGH with minimum spacing
                  if (adjustedLowY < adjustedHighY + SPACING) {
                    // Need to push them apart
                    const midpoint = (adjustedHighY + adjustedLowY) / 2;
                    adjustedHighY = clamp(midpoint - SPACING / 2, TOP_BOUND, BOTTOM_BOUND - SPACING);
                    adjustedLowY = clamp(midpoint + SPACING / 2, TOP_BOUND + SPACING, BOTTOM_BOUND);
                    
                    // If still overlapping after midpoint spread, force separation
                    if (adjustedLowY < adjustedHighY + SPACING) {
                      adjustedHighY = TOP_BOUND;
                      adjustedLowY = TOP_BOUND + SPACING;
                    }
                  }
                  
                  // Build result array
                  const labels: ChartLabelData[] = [];
                  
                  // HIGH label (optionally merged with CURRENT)
                  labels.push({
                    id: 'high',
                    adjustedY: adjustedHighY,
                    text: formattedHigh,
                    color: '#10B981'
                  });
                  
                  // CURRENT label (only if not merged)
                  if (showCurrent) {
                    // Current must be between HIGH and LOW
                    const minCurrentY = adjustedHighY + SPACING;
                    const maxCurrentY = adjustedLowY - SPACING;
                    
                    let adjustedCurrentY: number;
                    if (maxCurrentY >= minCurrentY) {
                      // There's valid space - clamp current to it
                      adjustedCurrentY = clamp(currentY, minCurrentY, maxCurrentY);
                    } else {
                      // Not enough space - place at midpoint and nudge HIGH/LOW
                      const totalHeight = 3 * SPACING; // 3 labels need this much space
                      const availableHeight = BOTTOM_BOUND - TOP_BOUND;
                      
                      if (availableHeight >= totalHeight) {
                        // Redistribute evenly
                        adjustedHighY = TOP_BOUND;
                        adjustedCurrentY = TOP_BOUND + SPACING;
                        adjustedLowY = TOP_BOUND + 2 * SPACING;
                        // Update HIGH label position
                        labels[0].adjustedY = adjustedHighY;
                      } else {
                        // Absolute minimum - stack them tightly
                        adjustedCurrentY = (adjustedHighY + adjustedLowY) / 2;
                      }
                    }
                    
                    labels.push({
                      id: 'current',
                      adjustedY: adjustedCurrentY,
                      text: formattedCurrent,
                      color: '#111827'
                    });
                  }
                  
                  // LOW label (optionally merged with CURRENT)
                  labels.push({
                    id: 'low',
                    adjustedY: adjustedLowY,
                    text: formattedLow,
                    color: '#EF4444'
                  });
                  
                  return labels;
                };
                const paddingLeft = 50;
                const paddingRight = 10;
                const paddingTop = 15;
                const paddingBottom = 10;
                const graphW = chartW - paddingLeft - paddingRight;
                const graphH = chartH - paddingTop - paddingBottom;
                
                const values = chartData.map(d => d.adjusted_close);
                const dataMin = Math.min(...values);
                const dataMax = Math.max(...values);
                const currentPrice = chartData[chartData.length - 1].adjusted_close;
                
                const highIdx = values.indexOf(dataMax);
                const lowIdx = values.indexOf(dataMin);
                
                const range = dataMax - dataMin || 1;
                const yPad = range * 0.1;
                const yMin = dataMin - yPad;
                const yMax = dataMax + yPad;
                
                const priceToY = (price: number) => paddingTop + graphH - ((price - yMin) / (yMax - yMin)) * graphH;
                
                const points = chartData.map((d, i) => {
                  const x = paddingLeft + (i / (chartData.length - 1)) * graphW;
                  const y = priceToY(d.adjusted_close);
                  return { x, y };
                });
                
                const pathD = points.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(' ');
                const lineColor = '#6B7280';
                
                let benchmarkPathD = '';
                if (benchmarkChartData.length > 1 && chartData.length > 1 && chartData[0].normalized) {
                  const stockNormValues = chartData.map(d => d.normalized || 100);
                  const benchNormValues = benchmarkChartData.map(d => d.normalized || 100);
                  const allNormValues = [...stockNormValues, ...benchNormValues];
                  const normMin = Math.min(...allNormValues);
                  const normMax = Math.max(...allNormValues);
                  const normRange = normMax - normMin || 1;
                  const normYPad = normRange * 0.1;
                  const normYMin = normMin - normYPad;
                  const normYMax = normMax + normYPad;
                  
                  const normToY = (val: number) => paddingTop + graphH - ((val - normYMin) / (normYMax - normYMin)) * graphH;
                  
                  const benchPoints = benchmarkChartData.map((d, i) => {
                    const x = paddingLeft + (i / (benchmarkChartData.length - 1)) * graphW;
                    const y = normToY(d.normalized);
                    return { x, y };
                  });
                  benchmarkPathD = benchPoints.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(' ');
                }
                
                const highY = priceToY(dataMax);
                const lowY = priceToY(dataMin);
                const currentY = priceToY(currentPrice);
                const highX = paddingLeft + (highIdx / (chartData.length - 1)) * graphW;
                const lowX = paddingLeft + (lowIdx / (chartData.length - 1)) * graphW;
                const formatPrice = (p: number) => p >= 1000 ? `$${toEU(p / 1000, 1)}k` : `$${toEU(p, 0)}`;
                
                // Compute chart labels with deterministic stacking
                const chartLabels = computeChartLabels(
                  highY, lowY, currentY,
                  dataMax, dataMin, currentPrice,
                  chartH,
                  formatPrice
                );
                
                // ===== CHART-TOOLTIP: Simple crosshair (stockanalysis.com style) =====
                const tooltipPoint = chartTooltipVisible && chartTooltipIndex !== null && chartData[chartTooltipIndex] 
                  ? chartData[chartTooltipIndex] 
                  : null;
                
                const tooltipX = tooltipPoint 
                  ? paddingLeft + (chartTooltipIndex! / (chartData.length - 1)) * graphW 
                  : 0;
                const tooltipY = tooltipPoint 
                  ? priceToY(tooltipPoint.adjusted_close) 
                  : 0;
                
                // Format date as DD.MM.YYYY for tooltip
                const tooltipDateStr = tooltipPoint 
                  ? (() => {
                      const d = new Date(tooltipPoint.date + 'T00:00:00Z');
                      return `${d.getUTCDate().toString().padStart(2, '0')}.${(d.getUTCMonth() + 1).toString().padStart(2, '0')}.${d.getUTCFullYear()}`;
                    })()
                  : '';
                
                // Bottom tooltip dimensions and clamping
                const BOTTOM_TIP_W = 72;
                const BOTTOM_TIP_H = 18;
                let bottomTipX = tooltipX - BOTTOM_TIP_W / 2;
                if (bottomTipX < paddingLeft) bottomTipX = paddingLeft;
                if (bottomTipX + BOTTOM_TIP_W > chartW - paddingRight) bottomTipX = chartW - paddingRight - BOTTOM_TIP_W;
                
                // Price tooltip dimensions and clamping
                const PRICE_TIP_W = 60;
                const PRICE_TIP_H = 22;
                const PRICE_TIP_OFFSET = 8;
                let priceTipX = tooltipX + PRICE_TIP_OFFSET;
                let priceTipY = tooltipY - PRICE_TIP_H / 2;
                // Flip to left if too close to right edge
                if (priceTipX + PRICE_TIP_W > chartW - 5) {
                  priceTipX = tooltipX - PRICE_TIP_W - PRICE_TIP_OFFSET;
                }
                // Clamp Y
                if (priceTipY < paddingTop) priceTipY = paddingTop;
                if (priceTipY + PRICE_TIP_H > chartH - paddingBottom) priceTipY = chartH - paddingBottom - PRICE_TIP_H;
                // ===== END CHART-TOOLTIP computation =====
                
                return (
                  <View
                    style={{ width: chartW, height: chartH, cursor: Platform.OS === 'web' ? 'crosshair' : undefined } as any}
                    // Native responder handlers (for native mobile)
                    onStartShouldSetResponder={() => true}
                    onMoveShouldSetResponder={() => true}
                    onResponderGrant={(e) => {
                      const idx = computeTooltipIndex((e.nativeEvent as any).locationX || 0, chartW);
                      if (idx !== null) {
                        setChartTooltipVisible(true);
                        setChartTooltipIndex(idx);
                      }
                    }}
                    onResponderMove={(e) => {
                      const idx = computeTooltipIndex((e.nativeEvent as any).locationX || 0, chartW);
                      if (idx !== null) setChartTooltipIndex(idx);
                    }}
                    onResponderRelease={hideChartTooltip}
                    // Web: Use ref callback to attach DOM events directly
                    ref={(el) => {
                      if (Platform.OS !== 'web' || !el) return;
                      const domEl = el as unknown as HTMLElement;
                      if (!domEl || domEl.dataset.chartEvents === '1') return;
                      domEl.dataset.chartEvents = '1';
                      
                      const getX = (ev: MouseEvent | TouchEvent): number => {
                        const rect = domEl.getBoundingClientRect();
                        if ('touches' in ev && ev.touches.length > 0) return ev.touches[0].clientX - rect.left;
                        return (ev as MouseEvent).clientX - rect.left;
                      };
                      
                      domEl.addEventListener('mousemove', (ev) => {
                        const idx = computeTooltipIndex(getX(ev), chartW);
                        if (idx !== null) {
                          setChartTooltipVisible(true);
                          setChartTooltipIndex(idx);
                        }
                      });
                      domEl.addEventListener('mouseleave', () => {
                        setChartTooltipVisible(false);
                        setChartTooltipIndex(null);
                      });
                      domEl.addEventListener('touchstart', (ev) => {
                        const idx = computeTooltipIndex(getX(ev), chartW);
                        if (idx !== null) {
                          setChartTooltipVisible(true);
                          setChartTooltipIndex(idx);
                        }
                      });
                      domEl.addEventListener('touchmove', (ev) => {
                        const idx = computeTooltipIndex(getX(ev), chartW);
                        if (idx !== null) setChartTooltipIndex(idx);
                      });
                      domEl.addEventListener('touchend', () => {
                        setChartTooltipVisible(false);
                        setChartTooltipIndex(null);
                      });
                    }}
                  >
                    <Svg width={chartW} height={chartH} style={{ position: 'absolute', top: 0, left: 0 }}>
                      {/* High/Low/Current reference lines */}
                      <Line x1={paddingLeft} y1={highY} x2={paddingLeft + graphW} y2={highY}
                        stroke="#10B981" strokeWidth={1} strokeDasharray="4,4" />
                      <Line x1={paddingLeft} y1={lowY} x2={paddingLeft + graphW} y2={lowY}
                        stroke="#EF4444" strokeWidth={1} strokeDasharray="4,4" />
                      <Line x1={paddingLeft} y1={currentY} x2={paddingLeft + graphW} y2={currentY}
                        stroke="#374151" strokeWidth={1} strokeDasharray="2,2" />
                      
                      {/* Benchmark path (S&P 500) */}
                      {benchmarkPathD && (
                        <Path d={benchmarkPathD} stroke="#9CA3AF" strokeWidth={1.5}
                          strokeOpacity={0.6} fill="none" strokeDasharray="4,2" />
                      )}
                      
                      {/* Price line */}
                      <Path d={pathD} stroke={lineColor} strokeWidth={2} fill="none" />
                      
                      {/* High/Low markers */}
                      <Circle cx={highX} cy={highY} r={5} fill="#10B981" />
                      <Circle cx={lowX} cy={lowY} r={5} fill="#EF4444" />
                      
                      {/* Price labels with deterministic stacking */}
                      {chartLabels.map(label => (
                        <SvgText 
                          key={label.id}
                          x={paddingLeft - 5} 
                          y={label.adjustedY + 4} 
                          fontSize={11} 
                          fill={label.color} 
                          fontWeight={label.id === 'current' ? '700' : '600'} 
                          textAnchor="end"
                        >
                          {label.text}
                        </SvgText>
                      ))}
                      
                      {/* ===== CHART-TOOLTIP: Crosshair + Date + Price ===== */}
                      {tooltipPoint && (
                        <G>
                          {/* Vertical crosshair line */}
                          <Line 
                            x1={tooltipX} 
                            y1={paddingTop} 
                            x2={tooltipX} 
                            y2={chartH - paddingBottom}
                            stroke="#6B7280" 
                            strokeWidth={1} 
                            strokeDasharray="4,4"
                          />
                          
                          {/* Point marker */}
                          <Circle 
                            cx={tooltipX} 
                            cy={tooltipY} 
                            r={4} 
                            fill="#111827"
                            stroke="#FFFFFF"
                            strokeWidth={2}
                          />
                          
                          {/* Bottom date tooltip */}
                          <Rect 
                            x={bottomTipX} 
                            y={chartH - paddingBottom + 2} 
                            width={BOTTOM_TIP_W} 
                            height={BOTTOM_TIP_H}
                            fill="#111827"
                            rx={3}
                          />
                          <SvgText 
                            x={bottomTipX + BOTTOM_TIP_W / 2} 
                            y={chartH - paddingBottom + 2 + 13}
                            fill="#FFFFFF"
                            fontSize={10}
                            fontWeight="500"
                            textAnchor="middle"
                          >
                            {tooltipDateStr}
                          </SvgText>
                          
                          {/* Price tooltip near marker */}
                          <Rect 
                            x={priceTipX} 
                            y={priceTipY} 
                            width={PRICE_TIP_W} 
                            height={PRICE_TIP_H}
                            fill="#111827"
                            rx={3}
                          />
                          <SvgText 
                            x={priceTipX + PRICE_TIP_W / 2} 
                            y={priceTipY + 15}
                            fill="#FFFFFF"
                            fontSize={11}
                            fontWeight="700"
                            textAnchor="middle"
                          >
                            {formatPrice(tooltipPoint.adjusted_close)}
                          </SvgText>
                        </G>
                      )}
                      {/* ===== END CHART-TOOLTIP rendering ===== */}
                    </Svg>
                  </View>
                );
              })()
            ) : (
              <View style={styles.chartLoading}>
                <Text style={styles.chartLoadingText}>No data available</Text>
              </View>
            )}
          </View>
          
          {chartData.length > 0 && (
            <View style={styles.chartLegend} data-testid="chart-legend">
              <View style={styles.legendItem}>
                <View style={[styles.legendDot, { backgroundColor: '#10B981' }]} />
                <Text style={[styles.legendLabel, { color: '#10B981' }]}>HIGH</Text>
              </View>
              <View style={styles.legendItem}>
                <View style={[styles.legendDot, { backgroundColor: '#EF4444' }]} />
                <Text style={[styles.legendLabel, { color: '#EF4444' }]}>LOW</Text>
              </View>
              <View style={styles.legendItem}>
                <View style={[styles.legendDot, { backgroundColor: '#111827' }]} />
                <Text style={[styles.legendLabel, { color: '#111827' }]}>PRICE</Text>
              </View>
              {benchmarkChartData.length > 0 && (
                <View style={styles.legendItem}>
                  <View style={[styles.legendDot, { backgroundColor: '#9CA3AF', opacity: 0.6 }]} />
                  <Text style={[styles.legendLabel, { color: '#9CA3AF' }]}>S&P 500 TR</Text>
                </View>
              )}
            </View>
          )}
        </View>

        {/* ===== UNIFIED PERFORMANCE CHECK (Dynamic based on period) ===== */}
        {/* P1 CRITICAL: Single source of truth - stats change with period selector */}
        {mobileData?.period_stats && (
          <View 
            style={styles.sectionCard} 
            data-testid="performance-check-card"
          >
            {/* Dynamic Header - changes based on selected period */}
            <View style={styles.sectionHeader}>
              <Text style={styles.sectionIcon}>🧭</Text>
              <Text style={styles.sectionTitleBold}>
                {priceRange === 'MAX' ? 'Performance Check (Full History)' : `Performance Check (Past ${priceRange})`}
              </Text>
            </View>
            <Text style={styles.realityCheckSubtitle}>
              {formatDateDMY(mobileData.period_stats.start_date)} – {formatDateDMY(mobileData.period_stats.end_date)}
            </Text>
            
            {/* Two-column layout: Reward | Pain */}
            <View style={styles.realityCheckColumns}>
              {/* LEFT COLUMN - Reward - GREEN */}
              <View style={styles.realityCheckColumnLeft}>
                <Text style={styles.realityCheckColumnHeader}>Reward</Text>
                
                {/* Total profit (for period) */}
                <View style={styles.realityCheckMetricCompact}>
                  <Text style={styles.realityCheckLabelCompact}>Total profit</Text>
                  <Text style={[
                    styles.realityCheckValueCompact,
                    mobileData.period_stats.profit_pct >= 0 ? styles.positiveText : styles.negativeText
                  ]}>
                    {formatLargePercent(mobileData.period_stats.profit_pct)}
                  </Text>
                </View>
                
                {/* Average per year (CAGR) - only show if > 1 year */}
                {mobileData.period_stats.cagr_pct !== null && (
                  <View style={styles.realityCheckMetricCompact}>
                    <Text style={styles.realityCheckLabelCompact}>Average per year</Text>
                    <Text style={[
                      styles.realityCheckValueCompact, 
                      mobileData.period_stats.cagr_pct >= 0 ? styles.positiveText : styles.negativeText
                    ]}>
                      {formatLargePercent(mobileData.period_stats.cagr_pct)}
                    </Text>
                  </View>
                )}
                
                {/* P1 CRITICAL: RRR inside Reward column, ABOVE benchmark */}
                {(() => {
                  // Use chartData for current period RRR
                  const rrr = computeRRR(chartData);
                  if (rrr === null) return null;
                  
                  return (
                    <TouchableOpacity 
                      style={styles.realityCheckMetricCompact}
                      onPress={() => alert('RRR (Upside/Downside): how much upside the stock had vs how much it dropped, measured from the start of the period. The higher, the better.')}
                      data-testid="rrr-performance-check"
                    >
                      <Text style={styles.realityCheckLabelCompact}>RRR (Risk/Reward)</Text>
                      <View style={styles.rrrValueRow}>
                        <Text style={[
                          styles.realityCheckValueCompact,
                          rrr > 2 ? styles.positiveText :
                          rrr >= 1 ? styles.neutralText :
                          styles.rrrNegativeText
                        ]}>
                          {formatRRR(rrr)}
                        </Text>
                        <Ionicons name="help-circle-outline" size={12} color={COLORS.textMuted} />
                      </View>
                    </TouchableOpacity>
                  );
                })()}
              </View>
              
              {/* RIGHT COLUMN - Pain - RED */}
              <View style={styles.realityCheckColumnRight}>
                <Text style={styles.realityCheckColumnHeaderRed}>Pain</Text>
                
                {/* Worst drawdown for this period */}
                <View style={styles.realityCheckMetricCompact}>
                  <Text style={styles.realityCheckLabelCompact}>Worst drawdown</Text>
                  <Text style={[styles.realityCheckValueCompact, styles.negativeText]}>
                    {formatLargePercent(-Math.abs(mobileData.period_stats.max_drawdown_pct))}
                  </Text>
                </View>
                
                {/* P22: Drawdown details from chartData (memoized) */}
                {drawdownDetails && (
                  <View style={styles.painDetails}>
                    <Text style={styles.painDateRange}>
                      {formatDateDMY(drawdownDetails.peak.date)} → {formatDateDMY(drawdownDetails.trough.date)}
                    </Text>
                    <Text style={styles.painDuration}>
                      Duration: {drawdownDetails.durationDays} days
                    </Text>
                    <Text style={styles.painRecovery}>
                      {drawdownDetails.recoveryDate 
                        ? `Recovered: ${formatDateDMY(drawdownDetails.recoveryDate)}`
                        : 'Recovered: Not recovered'}
                    </Text>
                  </View>
                )}
              </View>
            </View>
            
            {/* BENCHMARK STRIP - Index comparison */}
            <View style={styles.benchmarkStrip}>
              <View style={styles.benchmarkDivider} />
              <Text style={styles.benchmarkText}>
                Index (S&P 500 TR):{' '}
                <Text style={styles.benchmarkValue}>
                  {mobileData.period_stats.benchmark_total_pct !== null 
                    ? `${mobileData.period_stats.benchmark_total_pct >= 0 ? '+' : ''}${toEU(mobileData.period_stats.benchmark_total_pct, 1)}%`
                    : 'N/A'}
                </Text>
                {/* P0 FIX: Use backend's Wealth Gap calculation (outperformance_pct) */}
                {(() => {
                  // Use backend-calculated outperformance_pct (Wealth Gap formula)
                  // NOT: profit_pct - benchmark_total_pct (wrong!)
                  const wealthGap = mobileData.period_stats.outperformance_pct;
                  if (wealthGap === null || wealthGap === undefined) return null;
                  const deltaClamped = Math.max(wealthGap, -100);
                  const sign = deltaClamped >= 0 ? '+' : '';
                  return (
                    <Text style={[
                      styles.benchmarkValue,
                      deltaClamped > 0 ? styles.positiveText : 
                      deltaClamped < 0 ? styles.negativeText : null
                    ]}>
                      {' • vs S&P 500 TR: '}{sign}{toEU(deltaClamped, 1)}%
                    </Text>
                  );
                })()}
              </Text>
            </View>
            
            {/* Footer disclaimer */}
            <Text style={styles.realityCheckDisclaimer}>
              Past returns do not guarantee future gains. Context only, not advice.
            </Text>
          </View>
        )}

        {/* ===== VALUATION OVERVIEW (P1 UX: Collapsible with Pulse) ===== */}
        {mobileData?.valuation?.available && (
          <View style={[styles.sectionCard]} data-testid="valuation-card">
            {/* Collapsible Header with Valuation Pulse */}
            <TouchableOpacity 
              style={styles.collapsibleHeader}
              onPress={() => setValuationExpanded(!valuationExpanded)}
              data-testid="valuation-toggle"
            >
              <View style={styles.collapsibleTitleRow}>
                <View style={styles.sectionHeader}>
                  <Text style={styles.sectionIcon}>🧾</Text>
                  <Text style={styles.sectionTitleBold}>Valuation Overview</Text>
                </View>
                {/* Pulse summary when collapsed */}
                {!valuationExpanded && (() => {
                  const pulse = getValuationPulse(mobileData.valuation);
                  const sourceLabel = pulse.source === 'peers' ? 'vs peers' : 
                                     pulse.source === '5y_avg' ? 'vs 5Y avg' : '';
                  return (
                    <Text style={[styles.valuationPulse, { color: pulse.color }]}>
                      {pulse.label}
                      {pulse.delta !== null && ` (~${pulse.delta > 0 ? '+' : ''}${pulse.delta}% ${sourceLabel})`}
                    </Text>
                  );
                })()}
              </View>
              <Ionicons 
                name={valuationExpanded ? 'chevron-up' : 'chevron-down'} 
                size={20} 
                color={COLORS.textMuted} 
              />
            </TouchableOpacity>
            
            {/* Expanded Content */}
            {valuationExpanded && (
              <>
                {/* Row 1: Cheaper vs peers */}
                <View style={{ display: 'flex', flexDirection: 'row', alignItems: 'center', gap: 10, marginBottom: 8, marginTop: 12 }}>
                  <View style={[
                    styles.valuationBadge,
                    mobileData.valuation.overall_vs_peers === 'cheaper' ? styles.valuationBadgeGreen :
                    mobileData.valuation.overall_vs_peers === 'more_expensive' ? styles.valuationBadgeRed :
                    styles.valuationBadgeYellow
                  ]}>
                    <Text style={styles.valuationBadgeEmoji}>
                      {mobileData.valuation.overall_vs_peers === 'cheaper' ? '🟢' :
                       mobileData.valuation.overall_vs_peers === 'more_expensive' ? '🔴' : '🟡'}
                    </Text>
                  </View>
                  <View style={styles.valuationTextBlock}>
                    <Text style={styles.valuationMainText}>
                      {mobileData.valuation.overall_vs_peers === 'cheaper' ? 'Cheaper vs peers' :
                       mobileData.valuation.overall_vs_peers === 'more_expensive' ? 'More expensive vs peers' :
                       'Around peers'}
                    </Text>
                    <Text style={styles.valuationSubText}>
                      {(() => {
                        const metrics = mobileData.valuation.metrics;
                        const peMetric = metrics?.pe;
                        const peerCount = peMetric?.peer_count || mobileData.valuation.peer_count || 0;
                        const peerSource = peMetric?.peer_source || 'industry';
                        const peerLabel = peerSource === 'sector' 
                          ? (mobileData.company?.sector || 'sector')
                          : (mobileData.peer_transparency?.industry || mobileData.company?.industry || 'industry');
                        return `(vs ${peerCount} ${peerLabel} peers)`;
                      })()}
                    </Text>
                  </View>
                </View>
                
                {/* Row 2: vs 5Y Average */}
                <View style={styles.valuationRow}>
                  <View style={[
                    styles.valuationBadge,
                    mobileData.valuation.overall_vs_5y_avg === 'cheaper' ? styles.valuationBadgeGreen :
                    mobileData.valuation.overall_vs_5y_avg === 'more_expensive' ? styles.valuationBadgeRed :
                    mobileData.valuation.overall_vs_5y_avg === 'around' ? styles.valuationBadgeYellow :
                    styles.valuationBadgeGray
                  ]}>
                    <Text style={styles.valuationBadgeEmoji}>
                      {mobileData.valuation.overall_vs_5y_avg === 'cheaper' ? '🟢' :
                       mobileData.valuation.overall_vs_5y_avg === 'more_expensive' ? '🔴' :
                       mobileData.valuation.overall_vs_5y_avg === 'around' ? '🟡' : '⚪'}
                    </Text>
                  </View>
                  <View style={styles.valuationTextBlock}>
                    {mobileData.valuation.overall_vs_5y_avg ? (
                      <Text style={styles.valuationMainText}>
                        {mobileData.valuation.overall_vs_5y_avg === 'cheaper' ? 'Cheaper vs its 5Y average' :
                         mobileData.valuation.overall_vs_5y_avg === 'more_expensive' ? 'More expensive vs its 5Y average' :
                         'Around its 5Y average'}
                      </Text>
                    ) : (
                      <>
                        <Text style={styles.valuationMainText}>vs its 5Y average</Text>
                        <Text style={styles.valuationNaReason}>N/A (Not calculated yet)</Text>
                      </>
                    )}
                  </View>
                </View>
                
                <Text style={styles.valuationMetricsCount}>
                  Based on {mobileData.valuation.metrics_used} available metric{mobileData.valuation.metrics_used !== 1 ? 's' : ''}
                </Text>
                <Text style={styles.valuationDisclaimer}>Context only, not advice.</Text>
                
                {/* Details Table with TWO columns: vs Peers + vs 5Y Avg */}
                <TouchableOpacity 
                  style={styles.valuationDetailsToggle}
                  onPress={() => setValuationDetailsExpanded(!valuationDetailsExpanded)}
                >
                  <Ionicons name={valuationDetailsExpanded ? 'chevron-up' : 'chevron-down'} size={14} color={COLORS.textMuted} />
                  <Text style={styles.valuationDetailsToggleText}>
                    {valuationDetailsExpanded ? 'Hide details' : 'Show details'}
                  </Text>
                </TouchableOpacity>
                
                {valuationDetailsExpanded && mobileData.valuation.metrics && (
                  <View style={styles.valuationDetailsContent}>
                    <View style={styles.valuationTableHeader}>
                      <Text style={styles.valuationColMetricHeader}>Metric</Text>
                      <Text style={styles.valuationColPeersHeader}>vs Peers</Text>
                      <Text style={styles.valuationCol5YHeader}>vs 5Y Avg</Text>
                    </View>
                    
                    {Object.entries(mobileData.valuation.metrics).map(([key, metric]: [string, any]) => {
                      const hasValue = metric.current !== null && metric.current !== undefined;
                      const hasMedian = metric.peer_median !== null && metric.peer_median !== undefined;
                      const history5y = mobileData.valuation.history_5y?.metrics?.[key];
                      const has5YAvg = history5y?.avg_5y !== null && history5y?.avg_5y !== undefined;
                      
                      // Calculate deltas with explicit null checks
                      let peerDelta: number | null = null;
                      let fiveYDelta: number | null = null;
                      
                      if (hasValue && hasMedian && metric.peer_median !== 0) {
                        peerDelta = ((metric.current - metric.peer_median) / metric.peer_median) * 100;
                      }
                      if (hasValue && has5YAvg && history5y.avg_5y !== 0) {
                        fiveYDelta = ((metric.current - history5y.avg_5y) / history5y.avg_5y) * 100;
                      }
                      
                      const getColor = (delta: number | null) => {
                        if (delta === null) return COLORS.textMuted;
                        if (delta <= -20) return '#10B981';
                        if (delta >= 20) return '#EF4444';
                        return '#F59E0B';
                      };
                      
                      return (
                        <View key={key} style={styles.valuationTableRow}>
                          <View style={styles.valuationColMetric}>
                            <Text style={styles.valuationMetricLabel}>{metric.name}</Text>
                            {hasValue ? (
                              <Text style={styles.valuationMetricCurrent}>{toEU(metric.current, 1)}</Text>
                            ) : (
                              <Text style={styles.naUnprofitable}>
                                {metric.na_reason_display?.includes('Negative') ? 'Unprofitable' : 'N/A'}
                              </Text>
                            )}
                          </View>
                          
                          <View style={styles.valuationColPeers}>
                            {hasMedian ? (
                              <Text style={[styles.valuationDeltaText, { color: getColor(peerDelta) }]}>
                                {peerDelta !== null ? `${peerDelta > 0 ? '+' : ''}${Math.round(peerDelta)}%` : 'N/A'}
                              </Text>
                            ) : (
                              <Text style={styles.valuationNaDash}>N/A</Text>
                            )}
                          </View>
                          
                          <View style={styles.valuationCol5Y}>
                            {has5YAvg ? (
                              <Text style={[styles.valuationDeltaText, { color: getColor(fiveYDelta) }]}>
                                {fiveYDelta !== null ? `${fiveYDelta > 0 ? '+' : ''}${Math.round(fiveYDelta)}%` : 'N/A'}
                              </Text>
                            ) : (
                              <Text style={styles.valuationNaDash}>N/A (Insufficient history)</Text>
                            )}
                          </View>
                        </View>
                      );
                    })}
                  </View>
                )}
              </>
            )}
          </View>
        )}

        {/* ===== SECTION 4: KEY METRICS (Hybrid 7) - Collapsed by default ===== */}
        <View 
          style={[styles.sectionCard]} 
          data-testid="key-metrics-section"
          
        >
          <TouchableOpacity 
            style={styles.collapsibleHeader} 
            onPress={() => setKeyMetricsExpanded(!keyMetricsExpanded)}
            data-testid="key-metrics-toggle"
          >
            <View style={styles.collapsibleTitleRow}>
              <View style={styles.sectionHeader}>
                <Text style={styles.sectionIcon}>🧩</Text>
                <Text style={styles.sectionTitleBold}>Key Metrics</Text>
              </View>
              {/* P5: Summary Pills (only when collapsed) */}
              {!keyMetricsExpanded && (
                <View style={styles.summaryPillsContainer} data-testid="key-metrics-pills">
                  {getKeyMetricsPills().map((pill, i) => (
                    <SummaryPill key={i} label={pill} variant={getPillVariant(pill)} />
                  ))}
                </View>
              )}
            </View>
            <Ionicons 
              name={keyMetricsExpanded ? 'chevron-up' : 'chevron-down'} 
              size={20} 
              color={COLORS.textMuted} 
            />
          </TouchableOpacity>
          
          {keyMetricsExpanded && (
            <>
              {/* HYBRID 7 METRICS - P1 UX Polish with tooltips + empathetic N/A */}
              {mobileData?.key_metrics ? (
                <>
                  {/* Market Cap */}
                  <View style={styles.metricRow}>
                    <TouchableOpacity style={styles.metricLabelRow} onPress={() => showTooltip('marketCap')}>
                      <Text style={styles.metricLabel}>Market Cap</Text>
                      <Ionicons name="information-circle-outline" size={14} color={COLORS.textMuted} />
                    </TouchableOpacity>
                    <View style={styles.metricValueRow}>
                      {(() => {
                        const { text, color } = formatKeyMetricWithEmpathy(mobileData.key_metrics.market_cap, 'market_cap');
                        return <Text style={[styles.metricValue, { color }]}>{text}</Text>;
                      })()}
                      {mobileData.key_metrics.market_cap?.value && (
                        <View style={styles.capBadge}>
                          <Text style={styles.capBadgeText}>{getMarketCapLabel(mobileData.key_metrics.market_cap.value)}</Text>
                        </View>
                      )}
                    </View>
                  </View>
                
                  {/* Shares Outstanding */}
                  <View style={styles.metricRow}>
                    <TouchableOpacity style={styles.metricLabelRow} onPress={() => showTooltip('sharesOutstanding')}>
                      <Text style={styles.metricLabel}>Shares Outstanding</Text>
                      <Ionicons name="information-circle-outline" size={14} color={COLORS.textMuted} />
                    </TouchableOpacity>
                    {(() => {
                      const { text, color } = formatKeyMetricWithEmpathy(mobileData.key_metrics.shares_outstanding, 'shares_outstanding');
                      return <Text style={[styles.metricValue, { color }]}>{text}</Text>;
                    })()}
                  </View>
                
                  {/* Net Margin TTM */}
                  <View style={styles.metricRow}>
                    <TouchableOpacity style={styles.metricLabelRow} onPress={() => showTooltip('netMargin')}>
                      <Text style={styles.metricLabel}>Net Margin (TTM)</Text>
                      <Ionicons name="information-circle-outline" size={14} color={COLORS.textMuted} />
                    </TouchableOpacity>
                    {(() => {
                      const { text, color } = formatKeyMetricWithEmpathy(mobileData.key_metrics.net_margin_ttm, 'net_margin_ttm');
                      return <Text style={[styles.metricValue, { color }]}>{text}</Text>;
                    })()}
                  </View>
                
                  {/* FCF Yield */}
                  <View style={styles.metricRow}>
                    <TouchableOpacity style={styles.metricLabelRow} onPress={() => showTooltip('fcfYield')}>
                      <Text style={styles.metricLabel}>Free Cash Flow Yield</Text>
                      <Ionicons name="information-circle-outline" size={14} color={COLORS.textMuted} />
                    </TouchableOpacity>
                    {(() => {
                      const { text, color } = formatKeyMetricWithEmpathy(mobileData.key_metrics.fcf_yield, 'fcf_yield');
                      return <Text style={[styles.metricValue, { color }]}>{text}</Text>;
                    })()}
                  </View>
                
                  {/* Net Debt / EBITDA */}
                  <View style={styles.metricRow}>
                    <TouchableOpacity style={styles.metricLabelRow} onPress={() => showTooltip('netDebtEbitda')}>
                      <Text style={styles.metricLabel}>Net Debt / EBITDA</Text>
                      <Ionicons name="information-circle-outline" size={14} color={COLORS.textMuted} />
                    </TouchableOpacity>
                    {(() => {
                      const { text, color } = formatKeyMetricWithEmpathy(mobileData.key_metrics.net_debt_ebitda, 'net_debt_ebitda');
                      return <Text style={[styles.metricValue, { color }]}>{text}</Text>;
                    })()}
                  </View>
                
                  {/* Revenue Growth 3Y CAGR */}
                  <View style={styles.metricRow}>
                    <TouchableOpacity style={styles.metricLabelRow} onPress={() => showTooltip('revenueGrowth')}>
                      <Text style={styles.metricLabel}>Revenue Growth (3Y CAGR)</Text>
                      <Ionicons name="information-circle-outline" size={14} color={COLORS.textMuted} />
                    </TouchableOpacity>
                    {(() => {
                      const { text, color } = formatKeyMetricWithEmpathy(mobileData.key_metrics.revenue_growth_3y, 'revenue_growth_3y');
                      return <Text style={[styles.metricValue, { color }]}>{text}</Text>;
                    })()}
                  </View>
                
                  {/* Dividend Yield TTM with sector context */}
                  <View style={styles.metricRow}>
                    <TouchableOpacity style={styles.metricLabelRow} onPress={() => showTooltip('dividendYield')}>
                      <Text style={styles.metricLabel}>Dividend Yield (TTM)</Text>
                      <Ionicons name="information-circle-outline" size={14} color={COLORS.textMuted} />
                    </TouchableOpacity>
                    {(() => {
                      const div = mobileData.key_metrics.dividend_yield_ttm;
                      const industryMedian = div?.industry_dividend_yield_median;
                      const { text, color } = formatKeyMetricWithEmpathy(div, 'dividend_yield_ttm', industryMedian);
                      return <Text style={[styles.metricValue, { color }]}>{text}</Text>;
                    })()}
                  </View>
                </>
              ) : (
              /* Fallback to legacy data if mobileData.key_metrics not available */
              data && (
              <>
                {/* Market Cap */}
                <View style={styles.metricRow}>
                  <Text style={styles.metricLabel}>Market Cap</Text>
                  <View style={styles.metricValueRow}>
                    <Text style={styles.metricValue}>{formatCurrency(data.key_metrics?.market_cap)}</Text>
                    <View style={styles.capBadge}>
                      <Text style={styles.capBadgeText}>{getMarketCapLabel(data.key_metrics?.market_cap)}</Text>
                    </View>
                  </View>
                </View>
                
                {/* P/E Ratio with benchmark */}
                <View style={styles.metricRow}>
                  <Text style={styles.metricLabel}>P/E Ratio (TTM)</Text>
                  <View style={styles.metricWithBenchmark}>
                    <Text style={[
                      styles.metricValue,
                      data.gradient_colors?.pe_ratio?.color_class === 'positive' && styles.metricPositive,
                      data.gradient_colors?.pe_ratio?.color_class === 'negative' && styles.metricNegative
                    ]}>
                      {data.key_metrics?.pe_ratio ? toEU(data.key_metrics.pe_ratio, 2) : 'N/A' || 'N/A'}
                    </Text>
                    {data.key_metrics?.pe_benchmark && (
                      <View style={styles.peerComparison}>
                        <Text style={[
                          styles.peerComparisonText,
                          data.gradient_colors?.pe_ratio?.direction === 'above' && styles.peerAbove,
                          data.gradient_colors?.pe_ratio?.direction === 'below' && styles.peerBelow
                        ]}>
                          {data.gradient_colors?.pe_ratio?.direction === 'above' ? 'Above' : 
                           data.gradient_colors?.pe_ratio?.direction === 'below' ? 'Below' : 'In line'} peers ({toEU(data.key_metrics.pe_benchmark, 1)})
                        </Text>
                      </View>
                    )}
                  </View>
                </View>
                
                {/* EPS */}
                <View style={styles.metricRow}>
                  <Text style={styles.metricLabel}>EPS (TTM)</Text>
                  <Text style={styles.metricValue}>${data.key_metrics?.eps_ttm ? toEU(data.key_metrics.eps_ttm, 2) : 'N/A' || 'N/A'}</Text>
                </View>

                {/* Net Margin TTM with benchmark */}
                <View style={styles.metricRow}>
                  <Text style={styles.metricLabel}>Net Margin (TTM)</Text>
                  <View style={styles.metricWithBenchmark}>
                    <Text style={[
                      styles.metricValue,
                      data.gradient_colors?.net_margin_ttm?.color_class === 'positive' && styles.metricPositive,
                      data.gradient_colors?.net_margin_ttm?.color_class === 'negative' && styles.metricNegative
                    ]}>
                      {data.key_metrics?.net_margin_ttm ? `${toEU(data.key_metrics.net_margin_ttm, 1)}%` : 'N/A'}
                    </Text>
                    {data.key_metrics?.net_margin_benchmark && (
                      <View style={styles.peerComparison}>
                        <Text style={[
                          styles.peerComparisonText,
                          data.gradient_colors?.net_margin_ttm?.direction === 'above' && styles.peerAbove,
                          data.gradient_colors?.net_margin_ttm?.direction === 'below' && styles.peerBelow
                        ]}>
                          {data.gradient_colors?.net_margin_ttm?.direction === 'above' ? 'Above' : 
                           data.gradient_colors?.net_margin_ttm?.direction === 'below' ? 'Below' : 'In line'} peers ({toEU(data.key_metrics.net_margin_benchmark, 1)}%)
                        </Text>
                      </View>
                    )}
                  </View>
                </View>
                
                {/* Beta */}
                <View style={styles.metricRow}>
                  <Text style={styles.metricLabel}>Beta</Text>
                  <Text style={styles.metricValue}>{data.key_metrics?.beta ? toEU(data.key_metrics.beta, 2) : 'N/A' || 'N/A'}</Text>
                </View>
                
                {/* Dividend Yield TTM */}
                <View style={styles.metricRow}>
                  <Text style={styles.metricLabel}>Dividend Yield (TTM)</Text>
                  <View style={styles.metricWithBenchmark}>
                    <Text style={styles.metricValue}>
                      {data.key_metrics?.dividend_yield_ttm ? `${toEU(data.key_metrics.dividend_yield_ttm, 2)}%` : 
                       data.key_metrics?.dividend_yield ? `${toEU(data.key_metrics.dividend_yield * 100, 2)}%` : 'N/A'}
                    </Text>
                    {data.key_metrics?.dividend_benchmark && (
                      <Text style={styles.benchmarkText}>
                        vs peers: {toEU(data.key_metrics.dividend_benchmark * 100, 2)}%
                      </Text>
                    )}
                  </View>
                </View>
              </>
              )
            )}


            {/* P1 FIX: REMOVED duplicate "Valuation Multiples" section.
                Single source of truth is now "Valuation Overview" at the top.
                This section was duplicating data from valuation.metrics */}

            {/* Ownership */}
            {(data.key_metrics?.pct_insiders || data.key_metrics?.pct_institutions) && (
              <>
                <Text style={[styles.sectionTitle, { marginTop: 20 }]}>Ownership</Text>
                <View style={styles.ownershipBar}>
                  <View style={[styles.ownershipSegment, styles.insiderSegment, { flex: data.key_metrics?.pct_insiders || 0 }]} />
                  <View style={[styles.ownershipSegment, styles.institutionSegment, { flex: data.key_metrics?.pct_institutions || 0 }]} />
                  <View style={[styles.ownershipSegment, styles.retailSegment, { flex: 100 - (data.key_metrics?.pct_insiders || 0) - (data.key_metrics?.pct_institutions || 0) }]} />
                </View>
                <View style={styles.ownershipLegend}>
                  <View style={styles.legendItem}>
                    <View style={[styles.legendDot, styles.insiderSegment]} />
                    <Text style={styles.legendText}>Insiders {data.key_metrics?.pct_insiders ? toEU(data.key_metrics.pct_insiders, 1) : '0'}%</Text>
                  </View>
                  <View style={styles.legendItem}>
                    <View style={[styles.legendDot, styles.institutionSegment]} />
                    <Text style={styles.legendText}>Institutions {data.key_metrics?.pct_institutions ? toEU(data.key_metrics.pct_institutions, 1) : '0'}%</Text>
                  </View>
                </View>
              </>
            )}

            {/* Peer Context Disclaimer */}
            {data.peer_context && (
              <View style={styles.peerDisclaimer}>
                <Ionicons name="information-circle-outline" size={14} color={COLORS.textMuted} />
                <Text style={styles.disclaimerText}>
                  Compared to {data.peer_context.company_count} companies in {data.peer_context.industry}. 
                  This provides context only and is not investment advice.
                </Text>
              </View>
            )}

            {/* No Benchmark Warning */}
            {!data.has_benchmark && data.company?.industry && (
              <View style={[styles.peerDisclaimer, { backgroundColor: '#FEF3C7' }]}>
                <Ionicons name="alert-circle-outline" size={14} color="#D97706" />
                <Text style={[styles.disclaimerText, { color: '#92400E' }]}>
                  No peer benchmark available for {data.company.industry}. 
                  Industry has fewer than 5 companies in our database.
                </Text>
              </View>
            )}
            </>
          )}
        </View>

        {/* ===== SECTION 5: FINANCIAL HUB (P9) - Replaces old Financials ===== */}
        <View 
          style={styles.sectionCard}
          
        >
          <FinancialHub
            financials={mobileData?.financials || data?.financials}
            expanded={financialsExpanded}
            onToggle={() => setFinancialsExpanded(!financialsExpanded)}
          />
        </View>

        {/* ===== SECTION 6: EARNINGS & DIVIDENDS - Collapsible with Dividend pill ===== */}
        <View 
          style={[styles.sectionCard]} 
          data-testid="earnings-section"
          
        >
          <TouchableOpacity 
            style={styles.collapsibleHeader} 
            onPress={() => setEarningsDividendsExpanded(!earningsDividendsExpanded)}
            data-testid="earnings-dividends-toggle"
          >
            <View style={styles.collapsibleTitleRow}>
              <View style={styles.sectionHeader}>
                <Text style={styles.sectionIcon}>💸</Text>
                <Text style={styles.sectionTitleBold}>Earnings & Dividends</Text>
              </View>
              {/* P5: Summary Pill (only when collapsed) - Shows dividend status */}
              {!earningsDividendsExpanded && (
                <View style={styles.summaryPillsContainer} data-testid="dividends-pill">
                  <SummaryPill label={getDividendPill()} variant={getPillVariant(getDividendPill())} />
                </View>
              )}
            </View>
            <Ionicons 
              name={earningsDividendsExpanded ? 'chevron-up' : 'chevron-down'} 
              size={20} 
              color={COLORS.textMuted} 
            />
          </TouchableOpacity>
          
          {earningsDividendsExpanded && (
            <>
              {/* Earnings History */}
              {data?.earnings && data.earnings.length > 0 ? (
                <>
                  <Text style={[styles.subsectionTitle, { marginTop: 8 }]}>Earnings History</Text>
                  {data.earnings.slice(0, 8).map((e, i) => (
                    <View key={i} style={styles.earningsRow}>
                      <Text style={styles.earningsDate}>{formatDateDMY(e.quarter_date)}</Text>
                      <View style={styles.earningsData}>
                        <Text style={styles.earningsValue}>
                          ${e.reported_eps ? toEU(e.reported_eps, 2) : 'N/A'} vs ${e.estimated_eps ? toEU(e.estimated_eps, 2) : 'N/A'}
                        </Text>
                        <View style={[
                          styles.beatMissBadge,
                          e.beat_miss === 'beat' ? styles.beatBadge : styles.missBadge
                        ]}>
                          <Ionicons 
                            name={e.beat_miss === 'beat' ? 'checkmark' : 'close'} 
                            size={12} 
                            color={e.beat_miss === 'beat' ? '#10B981' : '#EF4444'} 
                          />
                          <Text style={[
                            styles.beatMissText,
                            e.beat_miss === 'beat' ? styles.beatText : styles.missText
                          ]}>
                            {e.surprise_pct ? toEU(e.surprise_pct, 1) : 'N/A'}%
                          </Text>
                        </View>
                      </View>
                    </View>
                  ))}
                </>
              ) : (
                <View style={styles.noDataPlaceholder}>
                  <Text style={styles.noDataText}>No earnings data available</Text>
                </View>
              )}
              
              {/* Dividends - always show */}
              <Text style={[styles.subsectionTitle, { marginTop: 16 }]}>Dividends</Text>
              {dividendPayments && dividendPayments.length > 0 ? (
                <View style={styles.dividendsList}>
                  {dividendPayments.slice(0, 4).map((d, i) => (
                    <View key={i} style={styles.dividendRow}>
                      <Text style={styles.dividendDate}>{formatDateDMY(d.ex_date)}</Text>
                      <Text style={styles.dividendAmount}>${toEU(d.amount, 4)}</Text>
                    </View>
                  ))}
                </View>
              ) : (
                <View style={styles.noDataPlaceholder}>
                  <Text style={styles.noDataText}>No dividend payments</Text>
                </View>
              )}
            </>
          )}
        </View>

        {/* ===== SECTION 7: INSIDER TRANSACTIONS - Collapsible ===== */}
        <View 
          style={styles.sectionCard} 
          data-testid="insider-section"
        >
          <TouchableOpacity 
            style={styles.collapsibleHeader} 
            onPress={() => setInsiderExpanded(!insiderExpanded)}
            data-testid="insider-toggle"
          >
            <View style={styles.collapsibleTitleRow}>
              <View style={styles.sectionHeader}>
                <Text style={styles.sectionIcon}>🕵️</Text>
                <Text style={styles.sectionTitleBold}>Insider Transactions</Text>
              </View>
              {/* Summary pill when collapsed */}
              {!insiderExpanded && data?.insider_activity && (
                <View style={styles.summaryPillsContainer}>
                  <SummaryPill 
                    label={data.insider_activity.status === 'net_buying' ? 'Net Buying' :
                           data.insider_activity.status === 'net_selling' ? 'Net Selling' : 'Neutral'} 
                    variant={data.insider_activity.status === 'net_buying' ? 'positive' :
                             data.insider_activity.status === 'net_selling' ? 'negative' : 'neutral'} 
                  />
                </View>
              )}
            </View>
            <Ionicons 
              name={insiderExpanded ? 'chevron-up' : 'chevron-down'} 
              size={20} 
              color={COLORS.textMuted} 
            />
          </TouchableOpacity>

          {insiderExpanded && (
            <>
              {data?.insider_activity ? (
                <>
                  {/* Status Badge */}
                  <View style={styles.insiderStatus}>
                    <View style={[
                      styles.statusBadge,
                      data.insider_activity.status === 'net_buying' ? styles.buyingBadge :
                      data.insider_activity.status === 'net_selling' ? styles.sellingBadge :
                      styles.neutralBadge
                    ]}>
                      <Text style={styles.statusText}>
                        {data.insider_activity.status === 'net_buying' ? 'Net Buying' :
                         data.insider_activity.status === 'net_selling' ? 'Net Selling' :
                         'Neutral'}
                      </Text>
                    </View>
                  </View>

                  <View style={styles.insiderGrid}>
                    <View style={styles.insiderItem}>
                      <Text style={styles.insiderValue}>{data.insider_activity.buyers_count}</Text>
                      <Text style={styles.insiderLabel}>Buyers</Text>
                    </View>
                    <View style={styles.insiderItem}>
                      <Text style={styles.insiderValue}>{data.insider_activity.sellers_count}</Text>
                      <Text style={styles.insiderLabel}>Sellers</Text>
                    </View>
                  </View>

                  <View style={styles.metricRow}>
                    <Text style={styles.metricLabel}>Total Buy Value</Text>
                    <Text style={[styles.metricValue, styles.positiveText]}>
                      {formatCurrency(data.insider_activity.total_buy_value_6m)}
                    </Text>
                  </View>
                  <View style={styles.metricRow}>
                    <Text style={styles.metricLabel}>Total Sell Value</Text>
                    <Text style={[styles.metricValue, styles.negativeText]}>
                      {formatCurrency(data.insider_activity.total_sell_value_6m)}
                    </Text>
                  </View>
                  <View style={styles.metricRow}>
                    <Text style={styles.metricLabel}>Net Value</Text>
                    <Text style={[
                      styles.metricValue,
                      (data.insider_activity.net_value_6m || 0) >= 0 ? styles.positiveText : styles.negativeText
                    ]}>
                      {formatCurrency(data.insider_activity.net_value_6m)}
                    </Text>
                  </View>
                </>
              ) : (
                <View style={styles.noDataContainer}>
                  <Ionicons name="people-outline" size={32} color={COLORS.textMuted} />
                  <Text style={styles.noDataText}>No insider activity data</Text>
                </View>
              )}
            </>
          )}
        </View>

        {/* ===== CALCULATOR BUTTONS ===== */}
        <View style={styles.calculatorSection}>
          <Text style={styles.sectionTitle}>Calculators</Text>
          <View style={styles.calculatorButtons}>
            <TouchableOpacity 
              style={styles.calcButton}
              onPress={() => router.push(`/calculator/buy-hold?ticker=${company.code}`)}
            >
              <Ionicons name="trending-up" size={20} color={COLORS.primary} />
              <Text style={styles.calcButtonText}>Buy & Hold</Text>
            </TouchableOpacity>
            <TouchableOpacity 
              style={styles.calcButton}
              onPress={() => router.push(`/calculator/dca?ticker=${company.code}`)}
            >
              <Ionicons name="repeat" size={20} color={COLORS.primary} />
              <Text style={styles.calcButtonText}>DCA</Text>
            </TouchableOpacity>
          </View>
        </View>

        {/* ===== SECTION 8: NEWS & TALK (unified feed) ===== */}
        <View 
          style={[styles.sectionCard]} 
          data-testid="news-talk-section"
          
        >
          <View style={styles.sectionHeader}>
            <Text style={styles.sectionIcon}>💬</Text>
            <Text style={styles.sectionTitleBold}>News & Talk</Text>
            <TouchableOpacity onPress={() => router.push('/talk')} style={styles.seeAllButton}>
              <Text style={styles.seeAllLink}>See all</Text>
            </TouchableOpacity>
          </View>
          
          {talkLoading ? (
            <View style={styles.talkLoading}>
              <ActivityIndicator size="small" color={COLORS.primary} />
            </View>
          ) : talkPosts.length === 0 ? (
            <View style={styles.talkEmpty}>
              <Ionicons name="chatbubbles-outline" size={32} color={COLORS.textMuted} />
              <Text style={styles.talkEmptyText}>No discussions yet</Text>
              <Text style={styles.talkEmptySubtext}>Be the first to share your thoughts on ${company.code}</Text>
            </View>
          ) : (
            <>
              {talkPosts.slice(0, 3).map((post: any) => (
                <TouchableOpacity 
                  key={post.post_id} 
                  style={styles.talkPost}
                  onPress={() => post.user?.user_id && router.push(`/user/${post.user.user_id}`)}
                >
                  <View style={styles.talkPostHeader}>
                    {post.user?.picture ? (
                      <Image source={{ uri: post.user.picture }} style={styles.talkAvatar} />
                    ) : (
                      <View style={styles.talkAvatarPlaceholder}>
                        <Ionicons name="person" size={12} color={COLORS.textMuted} />
                      </View>
                    )}
                    <Text style={styles.talkUserName}>{post.user?.name || 'Anonymous'}</Text>
                    {post.rrr !== null && post.rrr !== undefined && (
                      <View style={styles.talkRrrBadge}>
                        <Text style={styles.talkRrrText}>RRR {toEU(post.rrr, 1)}</Text>
                      </View>
                    )}
                  </View>
                  <Text style={styles.talkPostText} numberOfLines={2}>{post.text}</Text>
                </TouchableOpacity>
              ))}
              
              {hasMoreTalk && (
                <TouchableOpacity 
                  style={styles.talkViewMore}
                  onPress={() => router.push('/talk')}
                >
                  <Text style={styles.talkViewMoreText}>View more discussions</Text>
                  <Ionicons name="chevron-forward" size={16} color={COLORS.primary} />
                </TouchableOpacity>
              )}
            </>
          )}
        </View>

        {/* Footer */}
        <View style={styles.footer}>
          <Text style={styles.footerText}>
            Data from tracked_tickers.fundamentals • {data.lite_mode ? 'Lite' : 'Full'} mode
          </Text>
        </View>
        
        {/* Bottom padding for navigation */}
        <View style={{ height: 84 }} />
      </ScrollView>
      
      {/* P1 UX: Native BottomSheet Tooltip */}
      <MetricTooltip 
        visible={tooltipVisible} 
        onClose={() => setTooltipVisible(false)} 
        content={TOOLTIP_CONTENT[activeTooltip]} 
      />
      
      {/* Persistent Bottom Navigation */}
      <BottomNav />
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.background },
  header: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', paddingLeft: 4, paddingRight: 16, paddingVertical: 12, borderBottomWidth: 1, borderBottomColor: COLORS.border },
  backButton: { 
    width: 44, 
    height: 44, 
    alignItems: 'center', 
    justifyContent: 'center',
  },
  headerTitle: { fontSize: 18, fontWeight: '700', color: COLORS.text, letterSpacing: 1 },
  shareButton: { width: 44, height: 44, alignItems: 'center', justifyContent: 'center' },
  placeholder: { width: 44 },
  
  scrollView: { flex: 1 },
  scrollContent: { padding: 12, paddingBottom: 32 },
  
  // Clean section styling (no borders)
  sectionCard: {
    marginBottom: 16,
  },
  sectionHeader: {
    flexDirection: 'row',
    alignItems: 'center',
    marginBottom: 10,
    gap: 6,
  },
  sectionIcon: {
    fontSize: 16,
  },
  sectionTitleBold: {
    fontSize: 15,
    fontWeight: '700',
    color: COLORS.text,
    flex: 1,
  },
  seeAllButton: {
    marginLeft: 'auto',
  },
  
  loadingContainer: { flex: 1, alignItems: 'center', justifyContent: 'center', backgroundColor: COLORS.background },
  loadingText: { marginTop: 16, fontSize: 16, color: COLORS.textLight },
  errorContainer: { flex: 1, alignItems: 'center', justifyContent: 'center', padding: 24 },
  errorText: { fontSize: 18, color: COLORS.textLight, marginTop: 16, textAlign: 'center' },
  retryButton: { marginTop: 24, paddingHorizontal: 32, paddingVertical: 14, backgroundColor: COLORS.primary, borderRadius: 12 },
  retryText: { color: '#FFFFFF', fontWeight: '600', fontSize: 16 },
  
  // Pending Banner
  pendingBanner: { flexDirection: 'row', alignItems: 'flex-start', gap: 10, backgroundColor: '#FEF3C7', borderRadius: 12, padding: 12, marginBottom: 8, borderWidth: 1, borderColor: '#FCD34D' },
  pendingBannerContent: { flex: 1 },
  pendingBannerTitle: { fontSize: 14, fontWeight: '600', color: '#92400E', marginBottom: 2 },
  pendingBannerText: { fontSize: 12, color: '#B45309', lineHeight: 16 },
  
  // ============================================================================
  // NEW: Compact Header Row (replaces big identity card)
  // ============================================================================
  compactHeader: { 
    flexDirection: 'row', 
    alignItems: 'center', 
    gap: 10, 
    marginBottom: 8,  // Reduced from 12
  },
  compactLogo: { 
    width: 40, 
    height: 40, 
    borderRadius: 8, 
    backgroundColor: '#F5F8FC',
  },
  compactLogoPlaceholder: { 
    width: 40, 
    height: 40, 
    borderRadius: 8, 
    backgroundColor: COLORS.primary, 
    alignItems: 'center', 
    justifyContent: 'center',
  },
  compactLogoText: { 
    fontSize: 18, 
    fontWeight: '700', 
    color: '#FFF',
  },
  compactInfo: { 
    flex: 1,
  },
  compactName: { 
    fontSize: 16, 
    fontWeight: '600', 
    color: COLORS.text, 
    marginBottom: 4,
  },
  compactNameRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
    marginBottom: 2,
  },
  classificationRow: {
    flexDirection: 'row',
    flexWrap: 'wrap',
  },
  classificationText: {
    fontSize: 11,
    color: COLORS.textMuted,
  },
  classificationValue: {
    color: COLORS.text,
    fontWeight: '500',
  },
  exchangePill: {
    backgroundColor: '#1A365D',
    paddingHorizontal: 5,
    paddingVertical: 1,
    borderRadius: 3,
  },
  exchangePillText: {
    fontSize: 9,
    color: '#FFF',
    fontWeight: '600',
  },
  // Safety Badge styles
  safetyBadgeRow: {
    flexDirection: 'row',
    alignItems: 'center',
    marginVertical: 4,
    flexWrap: 'wrap',
    gap: 6,
  },
  safetyBadge: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 6,
    gap: 4,
  },
  safetyBadgeAmber: {
    backgroundColor: '#FEF3C7',
    borderWidth: 1,
    borderColor: '#F59E0B',
  },
  safetyBadgeBlue: {
    backgroundColor: '#DBEAFE',
    borderWidth: 1,
    borderColor: '#3B82F6',
  },
  safetyBadgeText: {
    fontSize: 11,
    fontWeight: '700',
  },
  safetyBadgeTextAmber: {
    color: '#92400E',
  },
  safetyBadgeTextBlue: {
    color: '#1E40AF',
  },
  safetyTooltip: {
    fontSize: 10,
    color: COLORS.textMuted,
    fontStyle: 'italic',
    marginTop: 4,
    lineHeight: 14,
  },
  safetyTooltipInline: {
    fontSize: 10,
    color: COLORS.textMuted,
    fontStyle: 'italic',
    flex: 1,
    lineHeight: 14,
  },
  compactPills: { 
    flexDirection: 'row', 
    gap: 6, 
    flexWrap: 'wrap',
  },
  compactPill: { 
    backgroundColor: '#1A365D', 
    paddingHorizontal: 6, 
    paddingVertical: 2, 
    borderRadius: 4,
  },
  compactPillSector: { 
    backgroundColor: '#F5F8FC',
  },
  compactPillText: { 
    fontSize: 10, 
    color: '#FFF', 
    fontWeight: '600',
  },
  
  // OLD Identity Card (kept for reference, not used)
  identityCard: { backgroundColor: COLORS.card, borderRadius: 16, padding: 16, marginBottom: 12 },
  identityHeader: { flexDirection: 'row', gap: 12, marginBottom: 12 },
  companyLogo: { width: 56, height: 56, borderRadius: 12, backgroundColor: '#F5F8FC' },
  logoPlaceholder: { width: 56, height: 56, borderRadius: 12, backgroundColor: COLORS.primary, alignItems: 'center', justifyContent: 'center' },
  logoPlaceholderText: { fontSize: 24, fontWeight: '700', color: '#FFF' },
  identityInfo: { flex: 1, justifyContent: 'center' },
  companyName: { fontSize: 18, fontWeight: '600', color: COLORS.text, marginBottom: 6 },
  badgeRow: { flexDirection: 'row', gap: 6, flexWrap: 'wrap' },
  exchangeBadge: { backgroundColor: '#1A365D', paddingHorizontal: 8, paddingVertical: 3, borderRadius: 4 },
  exchangeText: { fontSize: 11, color: '#FFF', fontWeight: '600' },
  sectorBadge: { backgroundColor: '#F5F8FC', paddingHorizontal: 8, paddingVertical: 3, borderRadius: 4 },
  sectorText: { fontSize: 11, color: COLORS.primary, fontWeight: '500' },
  identityDetails: { gap: 6 },
  detailRow: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  detailText: { fontSize: 13, color: COLORS.textMuted },
  linkText: { color: COLORS.accent },
  
  // Price Card - MORE COMPACT
  priceCard: { backgroundColor: COLORS.card, borderRadius: 12, padding: 12, marginBottom: 8 },  // Reduced padding and margin
  priceRow: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between' },
  priceValue: { fontSize: 28, fontWeight: '700', color: COLORS.text },  // Slightly smaller
  changeChip: { flexDirection: 'row', alignItems: 'center', paddingHorizontal: 8, paddingVertical: 4, borderRadius: 6, gap: 4 },
  positiveChip: { backgroundColor: '#D1FAE5' },
  negativeChip: { backgroundColor: '#FEE2E2' },
  changeText: { fontSize: 13, fontWeight: '600' },
  positiveText: { color: '#10B981' },
  negativeText: { color: '#EF4444' },
  priceDate: { fontSize: 11, color: COLORS.textMuted, marginTop: 2 },
  
  // Price Chart - MORE COMPACT
  chartCard: { backgroundColor: COLORS.card, borderRadius: 12, padding: 12, marginBottom: 8 },  // Reduced
  chartHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 8 },  // Reduced
  chartMetricsWrapper: { alignItems: 'flex-end', gap: 4 },
  chartMetricRow: { alignItems: 'flex-end' },
  chartPerformance: { flexDirection: 'row', alignItems: 'center', paddingHorizontal: 6, paddingVertical: 3, borderRadius: 6, gap: 3 },
  chartPositive: { backgroundColor: '#D1FAE5' },
  chartNegative: { backgroundColor: '#FEE2E2' },
  chartDividend: { backgroundColor: '#EDE9FE' },
  chartPerformanceText: { fontSize: 13, fontWeight: '600' },
  dividendText: { color: '#8B5CF6' },
  chartMetricLabel: { fontSize: 10, color: COLORS.textMuted, marginTop: 2 },
  priceReturnLabel: { flexDirection: 'row', alignItems: 'center', gap: 3, marginTop: 4 },
  priceReturnText: { fontSize: 9, color: COLORS.textMuted },
  rangeSelector: { flexDirection: 'row', justifyContent: 'space-between', marginBottom: 16, gap: 4 },
  rangeSelectorScroll: { marginBottom: 8 },
  rangeSelectorContent: { flexDirection: 'row', gap: 6, paddingHorizontal: 2 },
  // P22: Date range text under range selector
  dateRangeText: { fontSize: 11, color: COLORS.textMuted, textAlign: 'center', marginBottom: 12 },
  rangeButton: { paddingVertical: 8, paddingHorizontal: 12, borderRadius: 6, backgroundColor: '#F5F8FC', alignItems: 'center', minWidth: 44 },
  rangeButtonActive: { backgroundColor: COLORS.primary },
  rangeButtonText: { fontSize: 12, fontWeight: '500', color: COLORS.textMuted },
  rangeButtonTextActive: { color: '#FFF' },
  chartContainer: { marginHorizontal: -8, minHeight: 200 },
  chartLoading: { height: 200, justifyContent: 'center', alignItems: 'center' },
  chartLoadingText: { fontSize: 13, color: COLORS.textMuted, marginTop: 8 },
  chartErrorText: { fontSize: 13, color: '#EF4444' },
  chartRetryButton: { marginTop: 8, paddingHorizontal: 16, paddingVertical: 8, backgroundColor: COLORS.primary, borderRadius: 6 },
  chartRetryText: { fontSize: 13, color: '#FFF', fontWeight: '500' },
  chartTooltip: { backgroundColor: COLORS.text, paddingHorizontal: 10, paddingVertical: 6, borderRadius: 6, alignItems: 'center' },
  chartTooltipText: { fontSize: 13, color: '#FFF', fontWeight: '600' },
  chartTooltipDate: { fontSize: 10, color: 'rgba(255,255,255,0.7)', marginTop: 2 },
  chartInfo: { flexDirection: 'row', justifyContent: 'space-between', marginTop: 12, paddingTop: 12, borderTopWidth: 1, borderTopColor: COLORS.border },
  chartInfoItem: { flex: 1, alignItems: 'center' },
  chartInfoLabel: { fontSize: 11, color: COLORS.textMuted, marginBottom: 2 },
  chartInfoValue: { fontSize: 14, fontWeight: '600', color: COLORS.text },
  chartInfoDate: { fontSize: 10, color: COLORS.textMuted, marginTop: 2 },
  chartLegend: { flexDirection: 'row', justifyContent: 'center', alignItems: 'center', gap: 24, marginTop: 12, paddingVertical: 10, borderTopWidth: 1, borderTopColor: '#E5E7EB' },
  legendLabel: { fontSize: 12, fontWeight: '600' },
  customXAxis: { flexDirection: 'row', justifyContent: 'space-between', paddingHorizontal: 10, marginTop: 4 },
  customXAxisLabel: { fontSize: 10, color: COLORS.textMuted, textAlign: 'center', minWidth: 40 },
  
  // Description
  descriptionCard: { backgroundColor: COLORS.card, borderRadius: 16, padding: 16, marginBottom: 12 },
  sectionTitle: { fontSize: 15, fontWeight: '600', color: COLORS.text, marginBottom: 10 },
  subsectionTitle: { fontSize: 13, fontWeight: '600', color: COLORS.textMuted, marginBottom: 8 },
  descriptionText: { fontSize: 14, color: COLORS.textLight, lineHeight: 20 },
  showMoreText: { fontSize: 13, color: COLORS.accent, marginTop: 8, fontWeight: '500' },
  
  // P4: Collapsible header for Key Metrics
  collapsibleHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', paddingVertical: 4 },
  collapsibleTitleRow: { flexDirection: 'row', alignItems: 'center', flex: 1, gap: 8 },
  
  // P5: Summary Pills (subtle, light background)
  summaryPillsContainer: { flexDirection: 'row', flexWrap: 'wrap', gap: 6, marginLeft: 8 },
  summaryPill: { 
    backgroundColor: '#F3F4F6', 
    paddingHorizontal: 8, 
    paddingVertical: 3, 
    borderRadius: 12,
    borderWidth: 1,
    borderColor: '#E5E7EB',
  },
  summaryPillNegative: { 
    backgroundColor: '#FEF2F2', 
    borderColor: '#FECACA',
  },
  summaryPillPositive: { 
    backgroundColor: '#F0FDF4', 
    borderColor: '#BBF7D0',
  },
  summaryPillText: { 
    fontSize: 11, 
    fontWeight: '500', 
    color: '#6B7280',
  },
  summaryPillTextNegative: { 
    color: '#DC2626',
  },
  summaryPillTextPositive: { 
    color: '#16A34A',
  },
  
  // P4: No data placeholders
  noDataContainer: { alignItems: 'center', justifyContent: 'center', paddingVertical: 24, gap: 8 },
  noDataPlaceholder: { paddingVertical: 12 },
  noDataText: { fontSize: 14, color: COLORS.textMuted, textAlign: 'center' },
  
  // P4: Dividends list
  dividendsList: { marginTop: 8 },
  dividendRow: { flexDirection: 'row', justifyContent: 'space-between', paddingVertical: 8, borderBottomWidth: 1, borderBottomColor: COLORS.border },
  dividendDate: { fontSize: 13, color: COLORS.textMuted },
  dividendAmount: { fontSize: 13, fontWeight: '600', color: COLORS.text },
  
  // P4: Tab styles removed - Single vertical scroll, no tabs
  
  // Metrics Card
  metricsCard: { backgroundColor: COLORS.card, borderRadius: 16, padding: 16, marginBottom: 12 },
  metricRow: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', paddingVertical: 10, borderBottomWidth: 1, borderBottomColor: COLORS.border },
  metricLabel: { fontSize: 14, color: COLORS.textMuted },
  // P1 UX: Metric label row with info icon
  metricLabelRow: { flexDirection: 'row', alignItems: 'center', gap: 6, flex: 1 },
  metricValue: { fontSize: 14, fontWeight: '600', color: COLORS.text },
  metricValueRow: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  capBadge: { backgroundColor: '#F5F8FC', paddingHorizontal: 8, paddingVertical: 2, borderRadius: 4 },
  capBadgeText: { fontSize: 11, color: COLORS.primary, fontWeight: '500' },
  
  // Valuation Section
  valuationSection: { marginBottom: 16, paddingBottom: 16, borderBottomWidth: 1, borderBottomColor: COLORS.border },
  valuationBadgeContainer: { flexDirection: 'row', alignItems: 'center', gap: 16 },
  valuationBadge: { width: 72, height: 72, borderRadius: 36, alignItems: 'center', justifyContent: 'center' },
  valuationGood: { backgroundColor: '#D1FAE5' },
  valuationHigh: { backgroundColor: '#FEE2E2' },
  valuationNeutral: { backgroundColor: '#F3F4F6' },
  valuationScore: { fontSize: 24, fontWeight: '700', color: COLORS.text },
  valuationMax: { fontSize: 12, color: COLORS.textMuted },
  valuationInfo: { flex: 1 },
  valuationStatus: { fontSize: 16, fontWeight: '600', color: COLORS.text, marginBottom: 4 },
  peerContextText: { fontSize: 13, color: COLORS.textMuted },
  
  // Metric with Benchmark
  metricWithBenchmark: { alignItems: 'flex-end' },
  benchmarkText: { fontSize: 11, color: COLORS.textMuted, marginTop: 2 },
  peerComparison: { marginTop: 2 },
  peerComparisonText: { fontSize: 11, color: COLORS.textMuted },
  peerAbove: { color: '#EF4444' },  // Red for higher valuation (worse)
  peerBelow: { color: '#10B981' },  // Green for lower valuation (better)
  metricPositive: { color: '#10B981' },
  metricNegative: { color: '#EF4444' },
  
  // Peer Disclaimer
  peerDisclaimer: { flexDirection: 'row', alignItems: 'flex-start', gap: 6, marginTop: 16, paddingTop: 12, borderTopWidth: 1, borderTopColor: COLORS.border },
  disclaimerText: { flex: 1, fontSize: 11, color: COLORS.textMuted, lineHeight: 16 },
  
  // Ownership Bar
  ownershipBar: { flexDirection: 'row', height: 8, borderRadius: 4, overflow: 'hidden', marginBottom: 8 },
  ownershipSegment: { height: '100%' },
  insiderSegment: { backgroundColor: '#6366F1' },
  institutionSegment: { backgroundColor: '#10B981' },
  retailSegment: { backgroundColor: '#E5E7EB' },
  ownershipLegend: { flexDirection: 'row', justifyContent: 'center', gap: 16 },
  legendItem: { flexDirection: 'row', alignItems: 'center', gap: 4 },
  legendDot: { width: 8, height: 8, borderRadius: 4 },
  legendText: { fontSize: 11, color: COLORS.textMuted },
  
  // Financials
  periodSwitcher: { flexDirection: 'row', backgroundColor: '#F5F8FC', borderRadius: 8, padding: 4, marginBottom: 12 },
  periodButton: { flex: 1, paddingVertical: 8, alignItems: 'center', borderRadius: 6 },
  periodButtonActive: { backgroundColor: COLORS.primary },
  periodButtonText: { fontSize: 13, fontWeight: '500', color: COLORS.textMuted },
  periodButtonTextActive: { color: '#FFF' },
  chartSectionTitle: { fontSize: 13, fontWeight: '600', color: COLORS.text, marginBottom: 8, marginTop: 8 },
  verticalBarChart: { flexDirection: 'row', height: 120, alignItems: 'flex-end', justifyContent: 'space-around', marginBottom: 8, paddingTop: 20 },
  barColumn: { flex: 1, alignItems: 'center', justifyContent: 'flex-end', height: '100%' },
  barChangeLabel: { fontSize: 10, fontWeight: '500', marginBottom: 4, position: 'absolute', top: 0 },
  barWrapper: { width: '70%', height: '80%', justifyContent: 'flex-end', alignItems: 'center' },
  verticalBar: { width: '100%', borderRadius: 4 },
  barPeriodLabel: { fontSize: 10, color: COLORS.textMuted, marginTop: 4 },
  financialsTable: { marginTop: 12, borderTopWidth: 1, borderTopColor: COLORS.border, paddingTop: 8 },
  quarterRow: { paddingVertical: 10, borderBottomWidth: 1, borderBottomColor: COLORS.border },
  quarterDate: { fontSize: 12, fontWeight: '600', color: COLORS.text, marginBottom: 6 },
  quarterMetrics: { flexDirection: 'row', gap: 16 },
  quarterMetric: { flex: 1 },
  quarterLabel: { fontSize: 11, color: COLORS.textMuted, marginBottom: 2 },
  quarterValue: { fontSize: 13, fontWeight: '600', color: COLORS.text },
  valueNegative: { color: '#EF4444' },
  noDataContainer: { alignItems: 'center', paddingVertical: 32 },
  noDataText: { fontSize: 14, color: COLORS.textMuted, marginTop: 8 },
  ttmSection: { marginTop: 16, paddingTop: 12, borderTopWidth: 1, borderTopColor: COLORS.border },
  ttmTitle: { fontSize: 13, fontWeight: '600', color: COLORS.text, marginBottom: 8 },
  ttmRow: { flexDirection: 'row', justifyContent: 'space-between', paddingVertical: 4 },
  ttmLabel: { fontSize: 13, color: COLORS.textMuted },
  ttmValue: { fontSize: 13, fontWeight: '600', color: COLORS.text },
  metricHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', marginBottom: 2 },
  changeText: { fontSize: 11, fontWeight: '500' },
  changePositive: { color: '#10B981' },
  changeNegative: { color: '#EF4444' },
  barContainer: { flexDirection: 'row', height: 6, backgroundColor: '#F5F8FC', borderRadius: 3, marginTop: 4, overflow: 'hidden' },
  bar: { height: '100%', borderRadius: 3 },
  barPositive: { backgroundColor: '#10B981' },
  barNegative: { backgroundColor: '#EF4444' },
  
  // Earnings
  earningsRow: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', paddingVertical: 10, borderBottomWidth: 1, borderBottomColor: COLORS.border },
  earningsDate: { fontSize: 13, color: COLORS.textMuted },
  earningsData: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  earningsValue: { fontSize: 13, color: COLORS.text },
  beatMissBadge: { flexDirection: 'row', alignItems: 'center', paddingHorizontal: 6, paddingVertical: 2, borderRadius: 4, gap: 2 },
  beatBadge: { backgroundColor: '#D1FAE5' },
  missBadge: { backgroundColor: '#FEE2E2' },
  beatMissText: { fontSize: 11, fontWeight: '600' },
  beatText: { color: '#10B981' },
  missText: { color: '#EF4444' },
  
  // Insider
  insiderStatus: { alignItems: 'center', marginBottom: 16 },
  statusBadge: { paddingHorizontal: 16, paddingVertical: 8, borderRadius: 20 },
  buyingBadge: { backgroundColor: '#D1FAE5' },
  sellingBadge: { backgroundColor: '#FEE2E2' },
  neutralBadge: { backgroundColor: '#F3F4F6' },
  statusText: { fontSize: 14, fontWeight: '600', color: COLORS.text },
  insiderGrid: { flexDirection: 'row', marginBottom: 16 },
  insiderItem: { flex: 1, alignItems: 'center' },
  insiderValue: { fontSize: 28, fontWeight: '700', color: COLORS.text },
  insiderLabel: { fontSize: 12, color: COLORS.textMuted },
  
  // Calculator
  calculatorSection: { marginBottom: 16 },
  calculatorButtons: { flexDirection: 'row', gap: 12 },
  calcButton: { flex: 1, flexDirection: 'row', alignItems: 'center', justifyContent: 'center', backgroundColor: COLORS.card, borderRadius: 12, paddingVertical: 14, gap: 8, borderWidth: 1, borderColor: COLORS.border },
  calcButtonText: { fontSize: 14, fontWeight: '600', color: COLORS.primary },
  
  // Talk Section
  talkSection: { marginBottom: 16 },
  talkHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 },
  seeAllLink: { fontSize: 14, color: COLORS.primary, fontWeight: '500' },
  talkLoading: { padding: 24, alignItems: 'center' },
  talkEmpty: { backgroundColor: COLORS.card, borderRadius: 12, padding: 24, alignItems: 'center', borderWidth: 1, borderColor: COLORS.border },
  talkEmptyText: { fontSize: 15, fontWeight: '600', color: COLORS.text, marginTop: 8 },
  talkEmptySubtext: { fontSize: 13, color: COLORS.textMuted, textAlign: 'center', marginTop: 4 },
  talkPost: { backgroundColor: COLORS.card, borderRadius: 12, padding: 14, marginBottom: 8, borderWidth: 1, borderColor: COLORS.border },
  talkPostHeader: { flexDirection: 'row', alignItems: 'center', marginBottom: 8 },
  talkAvatar: { width: 24, height: 24, borderRadius: 12, marginRight: 8 },
  talkAvatarPlaceholder: { width: 24, height: 24, borderRadius: 12, backgroundColor: COLORS.background, justifyContent: 'center', alignItems: 'center', marginRight: 8 },
  talkUserName: { fontSize: 13, fontWeight: '600', color: COLORS.text, flex: 1 },
  talkRrrBadge: { backgroundColor: '#10B98120', paddingHorizontal: 6, paddingVertical: 2, borderRadius: 4 },
  talkRrrText: { fontSize: 10, fontWeight: '600', color: '#10B981' },
  talkPostText: { fontSize: 14, lineHeight: 20, color: COLORS.text },
  talkViewMore: { flexDirection: 'row', alignItems: 'center', justifyContent: 'center', padding: 12, backgroundColor: COLORS.card, borderRadius: 8, borderWidth: 1, borderColor: COLORS.border },
  talkViewMoreText: { fontSize: 14, fontWeight: '500', color: COLORS.primary, marginRight: 4 },
  
  // ============================================================================
  // NEW: Reality Check Card Styles - MORE COMPACT
  // ============================================================================
  realityCheckCard: { 
    backgroundColor: COLORS.card, 
    borderRadius: 12, 
    padding: 12, 
    marginBottom: 8,
    borderWidth: 1,
    borderColor: '#E5E7EB',
  },
  realityCheckHeader: { 
    marginBottom: 10,
  },
  realityCheckTitleRow: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    marginBottom: 2,
  },
  realityCheckTitle: { 
    fontSize: 14, 
    fontWeight: '700', 
    color: COLORS.text,
  },
  // Legacy RICHSTOX Score styles (kept for compatibility)
  richstoxScoreRow: {
    flexDirection: 'row',
    alignItems: 'center',
    marginTop: 8,
    gap: 6,
  },
  richstoxScoreLabel: {
    fontSize: 13,
    color: COLORS.textMuted,
    fontWeight: '500',
  },
  richstoxScoreValue: {
    fontSize: 14,
    fontWeight: '700',
    color: COLORS.text,
  },
  richstoxScoreIndicator: {
    fontSize: 12,
  },
  realityCheckSubtitle: { 
    fontSize: 11, 
    color: COLORS.textMuted,
  },
  // NEW: Two-column layout
  realityCheckColumns: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    marginBottom: 10,
  },
  realityCheckColumnLeft: {
    flex: 1,
    paddingRight: 8,
  },
  realityCheckColumnRight: {
    flex: 1,
    paddingLeft: 8,
    borderLeftWidth: 1,
    borderLeftColor: '#E5E7EB',
  },
  realityCheckColumnHeader: {
    fontSize: 11,
    fontWeight: '600',
    color: '#10B981',
    marginBottom: 8,
    textTransform: 'uppercase',
    letterSpacing: 0.5,
  },
  realityCheckColumnHeaderRed: {
    fontSize: 11,
    fontWeight: '600',
    color: '#EF4444',
    marginBottom: 8,
    textTransform: 'uppercase',
    letterSpacing: 0.5,
  },
  realityCheckMetricCompact: {
    marginBottom: 6,
  },
  realityCheckLabelCompact: {
    fontSize: 11,
    color: COLORS.textMuted,
    marginBottom: 2,
  },
  realityCheckValueCompact: {
    fontSize: 16,
    fontWeight: '700',
  },
  // P1 CRITICAL: RRR value row with help icon
  rrrValueRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
  },
  // Benchmark strip (neutral)
  benchmarkStrip: {
    borderTopWidth: 1,
    borderTopColor: '#E5E7EB',
    paddingTop: 8,
    marginTop: 4,
  },
  benchmarkDivider: {
    // Empty, divider is the border
  },
  benchmarkText: {
    fontSize: 12,
    color: '#6B7280',
  },
  benchmarkValue: {
    fontWeight: '600',
    color: '#374151',
  },
  // P1 FINAL: RRR Line Item in Reality Check
  rrrLineItem: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
    paddingTop: 8,
    marginTop: 4,
    borderTopWidth: 1,
    borderTopColor: '#E5E7EB',
  },
  rrrLineLabel: {
    fontSize: 12,
    color: COLORS.textMuted,
  },
  rrrLineValue: {
    fontSize: 14,
    fontWeight: '700',
  },
  neutralText: {
    color: '#F59E0B',
  },
  rrrNegativeText: {
    color: '#EF4444',
  },
  // P1 FINAL: RRR under Price Chart
  chartRrrRow: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 6,
    paddingTop: 10,
    marginTop: 8,
    borderTopWidth: 1,
    borderTopColor: '#F3F4F6',
  },
  chartRrrLabel: {
    fontSize: 11,
    color: COLORS.textMuted,
  },
  chartRrrValue: {
    fontSize: 13,
    fontWeight: '600',
  },
  // P1 FINAL: Peer median stack (median value + % vs median underneath)
  peerMedianStack: {
    flexDirection: 'column',
    alignItems: 'flex-end',
    gap: 2,
  },
  peerMedianRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 3,
  },
  medianPctTextSmall: {
    fontSize: 9,
    fontWeight: '500',
    textAlign: 'right',
  },
  // Disclaimer
  realityCheckDisclaimer: {
    fontSize: 10,
    color: COLORS.textMuted,
    fontStyle: 'italic',
    marginTop: 6,
  },
  // Legacy (keep for compatibility)
  realityCheckGrid: { 
    flexDirection: 'row', 
    flexWrap: 'wrap',
    justifyContent: 'space-between',
  },
  realityCheckMetric: { 
    width: '48%', 
    marginBottom: 6,
  },
  realityCheckLabel: { 
    fontSize: 11, 
    color: COLORS.textMuted, 
    marginBottom: 2,
  },
  realityCheckValue: { 
    fontSize: 18, 
    fontWeight: '700',
  },
  realityCheckValueMuted: { 
    fontSize: 18, 
    fontWeight: '700', 
    color: COLORS.textMuted,
  },
  
  // ============================================================================
  // NEW: Valuation Card Styles - MORE COMPACT
  // ============================================================================
  valuationCard: { 
    backgroundColor: COLORS.card, 
    borderRadius: 12, 
    padding: 12, 
    marginBottom: 8,
    borderWidth: 1,
    borderColor: '#E5E7EB',
  },
  valuationCardTitle: { 
    fontSize: 14, 
    fontWeight: '700', 
    color: COLORS.text,
    marginBottom: 10,
  },
  valuationRow: { 
    flexDirection: 'row', 
    alignItems: 'center', 
    marginBottom: 8,
    gap: 10,
  },
  valuationBadge: { 
    width: 28, 
    height: 28, 
    borderRadius: 14, 
    alignItems: 'center', 
    justifyContent: 'center',
  },
  valuationBadgeGreen: { backgroundColor: '#D1FAE5' },
  valuationBadgeRed: { backgroundColor: '#FEE2E2' },
  valuationBadgeYellow: { backgroundColor: '#FEF3C7' },
  valuationBadgeGray: { backgroundColor: '#F3F4F6' },  // P2: For N/A state
  valuationNaReason: {
    fontSize: 11,
    color: COLORS.textMuted,
    fontStyle: 'italic',
  },
  // P1 UX: Valuation Pulse style for collapsed header
  valuationPulse: {
    fontSize: 12,
    fontWeight: '600',
    marginLeft: 8,
  },
  // P1 UX: Delta text for valuation table
  valuationDeltaText: {
    fontSize: 12,
    fontWeight: '500',
    textAlign: 'center',
  },
  // P1 UX: N/A dash for valuation table
  valuationNaDash: {
    fontSize: 11,
    color: COLORS.textMuted,
    textAlign: 'center',
  },
  valuationBadgeEmoji: { fontSize: 12 },
  valuationTextBlock: { flex: 1 },
  valuationMainText: { 
    fontSize: 14, 
    fontWeight: '600', 
    color: COLORS.text,
  },
  valuationSubText: { 
    fontSize: 12, 
    color: COLORS.textMuted,
    marginTop: 2,
  },
  valuationDisclaimer: { 
    flexDirection: 'row', 
    alignItems: 'center', 
    gap: 6, 
    marginTop: 8,
    paddingTop: 10,
    borderTopWidth: 1,
    borderTopColor: '#F3F4F6',
  },
  valuationDisclaimerText: { 
    fontSize: 11, 
    color: COLORS.textMuted,
    fontStyle: 'italic',
  },
  valuationCurrentPE: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
    marginBottom: 12,
    paddingBottom: 8,
    borderBottomWidth: 1,
    borderBottomColor: '#F3F4F6',
  },
  valuationCurrentPELabel: {
    fontSize: 13,
    color: COLORS.textMuted,
  },
  valuationCurrentPEValue: {
    fontSize: 18,
    fontWeight: '700',
    color: COLORS.text,
  },
  
  // ============================================================================
  // NEW: Period Stats Styles - TWO COLUMN LAYOUT
  // ============================================================================
  periodStats: { 
    backgroundColor: '#F9FAFB', 
    borderRadius: 6, 
    padding: 10,
    marginTop: 8,
  },
  periodStatsTitle: { 
    fontSize: 12, 
    fontWeight: '600', 
    color: COLORS.text,
    marginBottom: 2,
  },
  // P22: Period Stats date range
  periodStatsDateRange: {
    fontSize: 10,
    color: COLORS.textMuted,
    marginBottom: 8,
  },
  // Two-column layout
  periodStatsColumns: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    marginBottom: 6,
  },
  periodStatsColumnLeft: {
    flex: 1,
    paddingRight: 8,
  },
  periodStatsColumnRight: {
    flex: 1,
    paddingLeft: 8,
    borderLeftWidth: 1,
    borderLeftColor: '#E5E7EB',
  },
  periodStatsColumnHeader: {
    fontSize: 10,
    fontWeight: '600',
    color: '#10B981',
    marginBottom: 4,
    textTransform: 'uppercase',
    letterSpacing: 0.5,
  },
  periodStatsColumnHeaderRed: {
    fontSize: 10,
    fontWeight: '600',
    color: '#EF4444',
    marginBottom: 4,
    textTransform: 'uppercase',
    letterSpacing: 0.5,
  },
  periodStatsMetricCompact: {
    marginBottom: 2,
  },
  periodStatsLabelCompact: {
    fontSize: 10,
    color: COLORS.textMuted,
    marginBottom: 1,
  },
  periodStatsValueCompact: {
    fontSize: 14,
    fontWeight: '600',
  },
  // P22: Pain details container
  painDetails: {
    marginTop: 4,
  },
  // P22: Pain date range
  painDateRange: {
    fontSize: 10,
    color: COLORS.textMuted,
    marginTop: 2,
  },
  // P22: Pain duration
  painDuration: {
    fontSize: 10,
    color: COLORS.textMuted,
    marginTop: 1,
  },
  // P22: Pain recovery status
  painRecovery: {
    fontSize: 10,
    color: COLORS.textMuted,
    marginTop: 1,
  },
  // P22: Hint to select MAX period
  painHint: {
    fontSize: 9,
    color: COLORS.textMuted,
    fontStyle: 'italic',
    marginTop: 4,
  },
  // P22: PAIN stats box in Price History section
  painStatsBox: {
    backgroundColor: '#FEF2F2',
    borderRadius: 8,
    padding: 12,
    marginTop: 12,
    alignItems: 'center',
  },
  painStatsHeader: {
    fontSize: 12,
    fontWeight: '700',
    color: '#EF4444',
    marginBottom: 2,
  },
  painStatsLabel: {
    fontSize: 10,
    color: COLORS.textMuted,
  },
  painStatsValue: {
    fontSize: 16,
    fontWeight: '700',
    color: '#EF4444',
    marginVertical: 2,
  },
  painStatsDateRange: {
    fontSize: 10,
    color: COLORS.textMuted,
    marginTop: 4,
  },
  painStatsDuration: {
    fontSize: 10,
    color: COLORS.textMuted,
    marginTop: 1,
  },
  painStatsRecovery: {
    fontSize: 10,
    color: COLORS.textMuted,
    marginTop: 1,
  },
  // Benchmark strip (neutral)
  periodStatsBenchmark: {
    borderTopWidth: 1,
    borderTopColor: '#E5E7EB',
    paddingTop: 6,
    marginTop: 2,
  },
  // Legacy (keep for compatibility)
  periodStatsGrid: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    justifyContent: 'space-between',
  },
  periodStatsMetric: {
    width: '48%',
    marginBottom: 8,
  },
  periodStatsLabel: {
    fontSize: 11,
    color: COLORS.textMuted,
    marginBottom: 2,
  },
  periodStatsValue: {
    fontSize: 15,
    fontWeight: '600',
  },
  periodStatsRow: { 
    flexDirection: 'row', 
    flexWrap: 'wrap',
    alignItems: 'center',
  },
  periodStatsItem: { 
    fontSize: 11, 
    color: COLORS.textMuted,
  },
  periodStatsSeparator: { 
    fontSize: 11, 
    color: COLORS.textMuted,
    marginHorizontal: 6,
  },
  
  // ============================================================================
  // NEW: Minimal Company Details (one line, collapsed)
  // ============================================================================
  companyDetailsMinimal: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
    paddingVertical: 6,
    marginBottom: 4,
  },
  companyDetailsMinimalText: {
    fontSize: 12,
    color: COLORS.textMuted,
  },
  companyDetailsExpanded: {
    backgroundColor: '#F9FAFB',
    borderRadius: 8,
    padding: 10,
    marginBottom: 8,
    gap: 6,
  },
  detailRowCompact: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
  },
  detailTextCompact: {
    fontSize: 12,
    color: COLORS.textMuted,
  },
  descriptionTextCompact: {
    fontSize: 11,
    color: COLORS.textMuted,
    lineHeight: 16,
    marginTop: 4,
  },
  
  // ============================================================================
  // NEW: Valuation Overview Styles
  // ============================================================================
  valuationMetricsCount: {
    fontSize: 11,
    color: COLORS.textMuted,
    fontStyle: 'italic',
    marginTop: 8,
    marginBottom: 4,
  },
  valuationDisclaimer: {
    fontSize: 10,
    color: COLORS.textMuted,
    fontStyle: 'italic',
    marginBottom: 8,
  },
  excludedSummaryText: {
    color: '#F59E0B', // amber warning color
    fontSize: 10,
  },
  naValueContainer: {
    flexDirection: 'column',
  },
  naReasonText: {
    fontSize: 8,
    color: COLORS.textMuted,
    fontStyle: 'italic',
    marginTop: 1,
  },
  valuationDetailsToggle: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
    paddingTop: 8,
    borderTopWidth: 1,
    borderTopColor: '#F3F4F6',
  },
  valuationDetailsToggleText: {
    fontSize: 12,
    color: COLORS.textMuted,
  },
  valuationDetailsContent: {
    marginTop: 10,
    gap: 6,
  },
  valuationMetricRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
    // P1 LAYOUT FIX: Fixed widths prevent overflow
    // Name(80) + Value(45) + Vs(60) + Reason(80) + Badge(24) = 289px
    // DO NOT use flex:1 on any column to prevent text overflow
  },
  valuationMetricName: {
    fontSize: 12,
    color: COLORS.textMuted,
    width: 80, // FIXED: prevent overflow
  },
  valuationMetricValue: {
    fontSize: 12,
    fontWeight: '600',
    color: COLORS.text,
    width: 45, // FIXED: only number or "N/A", no reason text here
  },
  valuationMetricVs: {
    fontSize: 11,
    color: COLORS.textMuted,
    width: 60,
  },
  valuationMetricReason: {
    fontSize: 10,
    color: COLORS.textMuted,
    fontStyle: 'italic',
    width: 80,
    textAlign: 'right',
  },
  valuationMetricBadge: {
    fontSize: 12,
    width: 24,
    textAlign: 'center',
  },
  
  // ============================================================================
  // P1 REFACTOR: Unified 3-Column Table Styles
  // ============================================================================
  valuationTableHeader: {
    flexDirection: 'row',
    paddingBottom: 6,
    marginBottom: 4,
    borderBottomWidth: 1,
    borderBottomColor: '#E5E7EB',
  },
  valuationTableRow: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingVertical: 5,
  },
  // Column widths optimized for 375px mobile
  valuationColMetric: {
    flex: 1.3,
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
  },
  valuationColPeers: {
    flex: 0.85,
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'flex-end',
    gap: 3,
  },
  valuationCol5Y: {
    flex: 0.85,
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'flex-end',
    gap: 3,
  },
  // Header text styles
  valuationColMetricHeader: {
    flex: 1.3,
    fontSize: 10,
    fontWeight: '600',
    color: COLORS.textMuted,
    textTransform: 'uppercase',
  },
  valuationColPeersHeader: {
    flex: 0.85,
    fontSize: 10,
    fontWeight: '600',
    color: COLORS.textMuted,
    textTransform: 'uppercase',
    textAlign: 'right',
  },
  valuationCol5YHeader: {
    flex: 0.85,
    fontSize: 10,
    fontWeight: '600',
    color: COLORS.textMuted,
    textTransform: 'uppercase',
    textAlign: 'right',
  },
  // Data text styles
  valuationMetricLabel: {
    fontSize: 11,
    fontWeight: '500',
    color: COLORS.text,
  },
  valuationMetricCurrent: {
    fontSize: 11,
    fontWeight: '600',
    color: COLORS.text,
  },
  // P1 UI OVERHAUL: Increased font size (+2pt) and bold for vs Peers value
  valuationVsValue: {
    fontSize: 12,          // Changed from 10 to 12
    fontWeight: '600',     // Added bold
    color: '#374151',      // Darker for better readability
  },
  valuationDot: {
    fontSize: 8,
  },
  valuationDash: {
    fontSize: 12,
    color: COLORS.textMuted,
  },
  valuationTextMuted: {
    color: COLORS.textMuted,
  },
  dotGreen: { color: '#10B981' },
  dotRed: { color: '#EF4444' },
  dotYellow: { color: '#F59E0B' },
  
  // P1 UI OVERHAUL: N/A Unprofitable style (simplified single word)
  naUnprofitable: {
    fontSize: 11,
    fontWeight: '500',
    color: COLORS.textMuted,
  },
  naUnprofitableRed: {
    color: '#EF4444',
    fontWeight: '600',
  },
  
  // ============================================================================
  // OLD: Company Details Accordion Styles (keep for reference)
  // ============================================================================
  companyDetailsCard: { 
    backgroundColor: COLORS.card, 
    borderRadius: 12, 
    marginBottom: 8,
    borderWidth: 1,
    borderColor: '#E5E7EB',
    overflow: 'hidden',
  },
  companyDetailsHeader: { 
    flexDirection: 'row', 
    justifyContent: 'flex-start', 
    alignItems: 'center',
    padding: 12,
  },
  companyDetailsHeaderLeft: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
  },
  companyDetailsTitle: { 
    fontSize: 14, 
    fontWeight: '600', 
    color: COLORS.textMuted,
  },
  companyDetailsContent: { 
    padding: 12, 
    paddingTop: 0,
    gap: 10,
  },
  
  // Footer
  footer: { alignItems: 'center', paddingTop: 8 },
  footerText: { fontSize: 11, color: COLORS.textMuted, textAlign: 'center' },

  // Search results navigation bar
  searchNavBar: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    paddingHorizontal: 12,
    paddingVertical: 6,
    backgroundColor: '#F0F4FF',
    borderBottomWidth: 1,
    borderBottomColor: '#E5E7EB',
  },
  searchNavButton: {
    flexDirection: 'row',
    alignItems: 'center',
    padding: 4,
    gap: 4,
    minWidth: 60,
  },
  searchNavButtonRight: {
    justifyContent: 'flex-end',
  },
  searchNavButtonDisabled: {
    opacity: 0.3,
  },
  searchNavTicker: {
    fontSize: 13,
    fontWeight: '600',
    color: '#1E3A5F',
  },
  searchNavCenter: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
  },
  searchNavCounter: {
    fontSize: 12,
    color: '#9CA3AF',
  },
  searchNavClose: {
    padding: 4,
  },
});
