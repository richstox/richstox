/**
 * Stock Detail Page
 * Canonical add-to entry point lives in the Last close card.
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
  Image,
  Linking,
  Platform,
  Modal,
  Pressable,
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
import { useMyStocksStore } from '../../stores/myStocksStore';
import { useAuth } from '../../contexts/AuthContext';
import { useAppDialog } from '../../contexts/AppDialogContext';
import { useLayoutSpacing } from '../../constants/layout';
import { API_URL } from '../../utils/config';

// Delay before fetching below-the-fold content (talk posts) to prioritize critical data
const DEFERRED_FETCH_MS = 800;
const EARNINGS_NEUTRAL_COLOR = '#6B7280';
const INITIAL_NEWS_EVENTS_LIMIT = 5;
const NEWS_EVENTS_PAGE_SIZE = 5;
const NEWS_ARTICLES_FETCH_PAGE_SIZE = 10;
const EVENT_SUBTITLE_SEPARATOR = ' • ';
const AGGREGATE_SENTIMENT_POSITIVE_THRESHOLD = 0.3;
const AGGREGATE_SENTIMENT_NEGATIVE_THRESHOLD = -0.3;

type SentimentCategory = 'positive' | 'negative' | 'neutral';

type AggregateSentiment = {
  score: number;
  label: SentimentCategory;
  color: string;
};

const getSentimentText = (label?: SentimentCategory | null): string => {
  if (label === 'positive') return 'Positive';
  if (label === 'negative') return 'Negative';
  return 'Neutral';
};

const getAggregateSentimentFromArticles = (
  articles: { sentiment_label?: SentimentCategory | null }[]
): AggregateSentiment | null => {
  if (!articles.length) return null;
  const sentimentScores = articles.map((article) => {
    if (article.sentiment_label === 'positive') return 1;
    if (article.sentiment_label === 'negative') return -1;
    return 0;
  });
  const score = sentimentScores.reduce((sum, value) => sum + value, 0) / sentimentScores.length;
  if (score > AGGREGATE_SENTIMENT_POSITIVE_THRESHOLD) {
    return { score: Number(score.toFixed(2)), label: 'positive', color: '#10B981' };
  }
  if (score < AGGREGATE_SENTIMENT_NEGATIVE_THRESHOLD) {
    return { score: Number(score.toFixed(2)), label: 'negative', color: '#EF4444' };
  }
  return { score: Number(score.toFixed(2)), label: 'neutral', color: '#F59E0B' };
};

const formatEventMessage = (title: string, subtitle?: string): string => {
  if (!subtitle) return title;
  return `${title}: ${subtitle.split(EVENT_SUBTITLE_SEPARATOR).join(', ')}`;
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

const formatDividendEventDate = (dateStr: string | null | undefined): string => {
  if (!dateStr) return 'Unknown';
  const formatted = formatDateDMY(dateStr);
  return formatted === 'N/A' ? 'Unknown' : formatted;
};

const getFormattedSplitRatio = (split?: UpcomingSplitInfo): string | null => {
  if (!split) return null;
  if (typeof split.split_ratio === 'string' && split.split_ratio.trim()) {
    return split.split_ratio.trim();
  }
  if (
    typeof split.old_shares === 'number'
    && Number.isFinite(split.old_shares)
    && split.old_shares > 0
    && typeof split.new_shares === 'number'
    && Number.isFinite(split.new_shares)
    && split.new_shares > 0
  ) {
    return `${split.old_shares}:${split.new_shares}`;
  }
  return null;
};

const getSentimentTone = (label?: SentimentCategory | null) => {
  if (label === 'positive') {
    return { backgroundColor: '#D1FAE5', textColor: COLORS.accent };
  }
  if (label === 'negative') {
    return { backgroundColor: '#FEE2E2', textColor: COLORS.danger };
  }
  return { backgroundColor: '#FEF3C7', textColor: '#D97706' };
};

const getSentimentLabelFromScores = (
  sentiment?: { pos?: number; neg?: number; neu?: number } | null,
  fallbackLabel?: SentimentCategory | null,
): SentimentCategory => {
  const pos = sentiment?.pos ?? 0;
  const neg = sentiment?.neg ?? 0;
  if (pos > neg) return 'positive';
  if (neg > pos) return 'negative';
  return fallbackLabel ?? 'neutral';
};

const getMarketTimingLabel = (value?: string | null): string | null => {
  if (!value) return null;
  const normalized = value.toLowerCase().replace(/[\s_-]+/g, '');
  if (normalized.startsWith('before')) return 'Before Market';
  if (normalized.startsWith('after')) return 'After Market';
  return null;
};

const getNewsFallbackKey = (
  fallbackKey?: string | null,
  symbol?: string | null,
): string => {
  if (fallbackKey && fallbackKey.trim()) return fallbackKey.trim().toUpperCase();
  if (symbol && symbol.trim()) return symbol.trim().charAt(0).toUpperCase();
  return '?';
};

const hashSymbolToColor = (symbol: string) => {
  const colors = ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#EC4899', '#06B6D4', '#84CC16'];
  let hash = 0;
  for (let i = 0; i < symbol.length; i++) {
    hash = ((hash << 5) - hash + symbol.charCodeAt(i)) | 0;
  }
  return colors[Math.abs(hash) % colors.length];
};

const NewsLogo = ({ logoUrl, fallbackKey }: { logoUrl?: string; fallbackKey: string }) => {
  const [imageError, setImageError] = useState(false);

  if (!logoUrl || imageError) {
    return (
      <View style={[newsLogoStyles.fallback, { backgroundColor: hashSymbolToColor(fallbackKey) }]}>
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
    width: 36,
    height: 36,
    borderRadius: 8,
    backgroundColor: '#F3F4F6',
  },
  fallback: {
    width: 36,
    height: 36,
    borderRadius: 8,
    alignItems: 'center',
    justifyContent: 'center',
  },
  fallbackText: {
    fontSize: 14,
    fontWeight: '700',
    color: '#FFFFFF',
  },
});

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
  reported_eps: number | null;
  estimated_eps: number | null;
  surprise_pct: number | null;
  show_badge?: boolean;
  is_upcoming?: boolean;
}

interface UpcomingEarningsData {
  report_date: string;
  fiscal_period_end: string | null;
  before_after_market: string | null;
  currency: string | null;
  estimate: number | null;
}

interface EarningsApiResponse {
  ticker: string;
  metadata: {
    default_currency: string;
    currencies: string[];
    default_frequency: string;
    frequencies: string[];
  };
  upcoming_earnings: UpcomingEarningsData | null;
  earnings_history: EarningsData[];
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
  frequency?: {
    label: string;
    source?: string;
    has_special?: boolean;
    has_irregular?: boolean;
  };
  display_currency?: string | null;
  next_dividend?: {
    next_ex_date: string | null;
    next_pay_date?: string | null;
    next_dividend_amount?: number | null;
    next_dividend_currency?: string | null;
    event_type_label?: string | null;
  };
  recent_payments: {
    ex_date: string;
    amount: number;
    currency?: string | null;
    payment_date?: string | null;
    is_special?: boolean;
    is_irregular?: boolean;
    dividend_type?: string | null;
    event_type_label?: string | null;
  }[];
  yoy_growth: number | null;
  status: string;
}

type DividendEvent = {
  ex_date: string;
  amount: number;
  currency?: string | null;
  period?: string | null;
  payment_date?: string | null;
  is_special?: boolean;
  is_irregular?: boolean;
  dividend_type?: string | null;
  frequency_label?: string | null;
  event_type_label?: string | null;
};

type NextDividendInfo = {
  next_ex_date: string | null;
  next_pay_date?: string | null;
  next_dividend_amount?: number | null;
  next_dividend_currency?: string | null;
  event_type_label?: string | null;
} | null;

type NewsArticle = {
  id: string;
  title: string;
  content?: string | null;
  source?: string | null;
  link?: string | null;
  date?: string | null;
  ticker?: string | null;
  company_name?: string | null;
  logo_url?: string | null;
  fallback_logo_key?: string | null;
  sentiment?: {
    pos?: number;
    neg?: number;
    neu?: number;
  } | null;
  sentiment_label?: SentimentCategory | null;
  tags?: string[];
  time_ago?: string | null;
};

type TickerNewsApiArticle = {
  article_id?: string;
  title?: string | null;
  content?: string | null;
  source?: string | null;
  source_link?: string | null;
  link?: string | null;
  published_at?: string | null;
  date?: string | null;
  sentiment?: NewsArticle['sentiment'];
  sentiment_label?: SentimentCategory | null;
  tags?: string[];
  time_ago?: string | null;
};

type UpcomingSplitInfo = {
  split_date: string;
  split_ratio?: string | null;
  old_shares?: number | null;
  new_shares?: number | null;
} | null;

type NewsEventFeedItem =
  | {
      kind: 'event';
      id: string;
      eventType: 'Earnings' | 'Dividend' | 'Split';
      title: string;
      subtitle: string;
      date: string;
    }
  | {
      kind: 'article';
      id: string;
      article: NewsArticle;
    };

const round4 = (value: number): number => Number(value.toFixed(4));
const DEFAULT_FREQUENCY_LABEL = 'Irregular';
const FLAT_GROWTH_THRESHOLD_PCT = 1.0;
const parseDividendExDateMs = (exDate: string): number | null => {
  if (!exDate) return null;
  const normalized = exDate.includes('T') ? exDate : `${exDate}T00:00:00Z`;
  const ms = Date.parse(normalized);
  return Number.isFinite(ms) ? ms : null;
};

const MONTH_ABBR = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];

const parseExDateParts = (dateStr: string | null | undefined): { day: string; month: string; year: string } => {
  if (!dateStr) return { day: '—', month: '—', year: '—' };
  const d = new Date(dateStr.includes('T') ? dateStr : `${dateStr}T00:00:00Z`);
  if (isNaN(d.getTime())) return { day: '—', month: '—', year: '—' };
  return {
    day: String(d.getUTCDate()).padStart(2, '0'),
    month: MONTH_ABBR[d.getUTCMonth()],
    year: String(d.getUTCFullYear()),
  };
};

const resolveDividendCurrency = (eventCurrency?: string | null, fallbackCurrency?: string | null): string | null => {
  if (typeof eventCurrency === 'string' && eventCurrency.trim()) return eventCurrency;
  if (typeof fallbackCurrency === 'string' && fallbackCurrency.trim()) return fallbackCurrency;
  return null;
};

// Price range options for chart - including MAX
type PriceRange = '3M' | '6M' | 'YTD' | '1Y' | '3Y' | '5Y' | 'MAX';

/** Periods available in the Performance Check period selector (3Y excluded). */
const PERFORMANCE_PERIODS: PriceRange[] = ['3M', '6M', 'YTD', '1Y', '5Y', 'MAX'];

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

interface BenchmarkMetadata {
  benchmark_value: number | null;
  benchmark_level: 'industry' | 'sector' | 'market' | null;
  benchmark_n: number | null;
  statistic_type: 'median';
}

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
  // Benchmark metadata (canonical struct per metric from API)
  // Hybrid 7 Key Metrics (P0)
  has_benchmark?: boolean;
  benchmark_fallback?: 'industry' | 'sector' | 'market' | null;
  key_metrics?: {
    market_cap: { name: string; value: number | null; formatted: string | null; na_reason: string | null };
    shares_outstanding: { name: string; value: number | null; formatted: string | null; na_reason: string | null };
    net_margin_ttm: { name: string; value: number | null; formatted: string | null; na_reason: string | null; peer_median?: number | null; peer_median_n?: number | null; peer_median_level?: string | null; benchmark_metadata?: BenchmarkMetadata | null };
    fcf_yield: { name: string; value: number | null; formatted: string | null; na_reason: string | null; peer_median?: number | null; peer_median_n?: number | null; peer_median_level?: string | null; benchmark_metadata?: BenchmarkMetadata | null };
    net_debt_ebitda: { name: string; value: number | null; formatted: string | null; na_reason: string | null; peer_median?: number | null; peer_median_n?: number | null; peer_median_level?: string | null; benchmark_metadata?: BenchmarkMetadata | null };
    revenue_growth_3y: { name: string; value: number | null; formatted: string | null; na_reason: string | null; peer_median?: number | null; peer_median_n?: number | null; peer_median_level?: string | null; benchmark_metadata?: BenchmarkMetadata | null };
    dividend_yield_ttm: { name: string; value: number | null; formatted: string | null; na_reason: string | null; peer_median?: number | null; peer_median_n?: number | null; peer_median_level?: string | null; benchmark_metadata?: BenchmarkMetadata | null; industry_dividend_yield_median?: number | null };
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
    city: string | null;
    state: string | null;
    country_name: string | null;
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
  const { sessionToken, isAuthenticated, isSessionValidated } = useAuth();
  const dialog = useAppDialog();
  const sp = useLayoutSpacing();
  
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [data, setData] = useState<StockOverview | null>(null);
  const [error, setError] = useState<string | null>(null);
  // Track whether initial data fetches are still in progress (to avoid flashing negative pills)
  const [mobileDataLoading, setMobileDataLoading] = useState(true);
  const [dividendsLoading, setDividendsLoading] = useState(true);
  const [showFullDescription, setShowFullDescription] = useState(false);
  // P4: Single vertical scroll, no tabs - Key Metrics collapsed by default
  const [keyMetricsExpanded, setKeyMetricsExpanded] = useState(false); // Collapsed by default
  // P5: Collapsed sections with summary pills
  const [financialsExpanded, setFinancialsExpanded] = useState(false);
  const [earningsDividendsExpanded, setEarningsDividendsExpanded] = useState(false);
  const [insiderExpanded, setInsiderExpanded] = useState(false);
  
  // NEW: Mobile detail data (RAW FACTS ONLY)
  const [mobileData, setMobileData] = useState<MobileDetailData | null>(null);
  
  const [listMemberships, setListMemberships] = useState<{ watchlist: boolean; tracklist: boolean }>({ watchlist: false, tracklist: false });
  const [listActionLoading, setListActionLoading] = useState(false);
  const [addToVisible, setAddToVisible] = useState(false);
  
  // Company details accordion state
  const [companyDetailsExpanded, setCompanyDetailsExpanded] = useState(false);
  
  // Performance Check period selector
  const [perfCheckPeriodVisible, setPerfCheckPeriodVisible] = useState(false);
  // Price History range selector
  const [priceRangeSelectorVisible, setPriceRangeSelectorVisible] = useState(false);
  
  // Price chart state
  const [priceRange, setPriceRange] = useState<PriceRange>('MAX'); // P1 CRITICAL: Default to MAX
  const [chartData, setChartData] = useState<PriceHistoryPoint[]>([]);
  const [chartLoading, setChartLoading] = useState(false);
  const [chartError, setChartError] = useState<string | null>(null);
  const [chartWMeasured, setChartWMeasured] = useState(0);
  const [chartDataNotices, setChartDataNotices] = useState<string[]>([]);
  
  // Chart tooltip state (CHART-TOOLTIP: simple hover/touch, like stockanalysis.com)
  const [chartTooltipVisible, setChartTooltipVisible] = useState(false);
  const [chartTooltipIndex, setChartTooltipIndex] = useState<number | null>(null);
  // Refs to keep web DOM event-listener closures up-to-date across re-renders
  const computeTooltipIndexRef = useRef<(x: number, w: number) => number | null>(() => null);
  const chartWRef = useRef(0);
  
  // Dividends state
  const [dividendPayments, setDividendPayments] = useState<DividendEvent[]>([]);
  const [dividendHistory, setDividendHistory] = useState<DividendEvent[]>([]);
  const [dividendViewMode, setDividendViewMode] = useState<'payments' | 'annual'>('annual');
  const [dividendFrequencyLabel, setDividendFrequencyLabel] = useState<string>(DEFAULT_FREQUENCY_LABEL);
  const [dividendFrequencyFlags, setDividendFrequencyFlags] = useState<{ hasSpecial: boolean; hasIrregular: boolean }>({
    hasSpecial: false,
    hasIrregular: false,
  });
  const [dividendDisplayCurrency, setDividendDisplayCurrency] = useState<string>('USD');
  const [nextDividendInfo, setNextDividendInfo] = useState<NextDividendInfo>(null);

  // Earnings from dedicated /v1/ticker/{ticker}/earnings endpoint
  const [upcomingEarnings, setUpcomingEarnings] = useState<UpcomingEarningsData | null>(null);
  const [earningsHistory, setEarningsHistory] = useState<EarningsData[]>([]);
  const [earningsLoading, setEarningsLoading] = useState(true);
  const [earningsCurrency, setEarningsCurrency] = useState<string | null>(null);
  const [earningsDivMode, setEarningsDivMode] = useState<'earnings' | 'dividends'>('dividends');
  const [earningsViewMode, setEarningsViewMode] = useState<'annual' | 'history'>('annual');
  
  // Financials period toggle - handled internally by FinancialHub component
  
  // Benchmark chart data (SP500TR.INDX normalized to 100)
  const [benchmarkChartData, setBenchmarkChartData] = useState<{date: string; normalized: number}[]>([]);
  const [showBenchmark, setShowBenchmark] = useState(false);

  // Per-range cache so switching back to MAX (or any range) is instant after first load
  const chartCacheRef = useRef<Record<string, { prices: PriceHistoryPoint[]; benchmark: {date: string; normalized: number}[] }>>({});

  // News & events state
  const [newsArticles, setNewsArticles] = useState<NewsArticle[]>([]);
  const [newsLoading, setNewsLoading] = useState(false);
  const [newsHasMore, setNewsHasMore] = useState(false);
  const [newsVisibleCount, setNewsVisibleCount] = useState(INITIAL_NEWS_EVENTS_LIMIT);
  const [selectedArticle, setSelectedArticle] = useState<NewsArticle | null>(null);
  const [upcomingSplit, setUpcomingSplit] = useState<UpcomingSplitInfo>(null);
  const scrollViewRef = useRef<ScrollView>(null);

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

  // MY STOCKS list navigation (same UX as search pager)
  const { tickers: myStocksTickers, clearTickers: clearMyStocks } = useMyStocksStore();
  const myStocksIndex = useMemo(
    () => myStocksTickers.findIndex(t => t === ticker),
    [myStocksTickers, ticker]
  );
  const hasMyStocksNav = !hasSearchNav && myStocksIndex >= 0 && myStocksTickers.length > 1;
  const prevMyStocksTicker = myStocksIndex > 0 ? myStocksTickers[myStocksIndex - 1] : null;
  const nextMyStocksTicker = myStocksIndex >= 0 && myStocksIndex < myStocksTickers.length - 1 ? myStocksTickers[myStocksIndex + 1] : null;

  // Unified pager state: search takes priority, then MY STOCKS
  const hasPagerNav = hasSearchNav || hasMyStocksNav;
  const pagerPrev = hasSearchNav ? prevTicker : prevMyStocksTicker;
  const pagerNext = hasSearchNav ? nextTicker : nextMyStocksTicker;
  const pagerIndex = hasSearchNav ? searchIndex : myStocksIndex;
  const pagerTotal = hasSearchNav ? searchResults.length : myStocksTickers.length;
  const pagerLabel = `${pagerIndex + 1} of ${pagerTotal}`;
  const pagerClear = hasSearchNav ? clearSearch : clearMyStocks;

  // Safe back navigation: fallback when browser history is unavailable (e.g. hard refresh / direct entry)
  const safeBack = useCallback(() => {
    if (Platform.OS === 'web' && typeof window !== 'undefined') {
      // Detect hard refresh via Performance Navigation Timing API
      const navEntry = performance?.getEntriesByType?.('navigation')?.[0] as PerformanceNavigationTiming | undefined;
      const isReload = navEntry?.type === 'reload';
      // Direct URL entry or hard refresh: no meaningful SPA history to go back to
      if (isReload || window.history.length <= 2) {
        router.push('/(tabs)/markets' as any);
        return;
      }
    }
    router.back();
  }, [router]);

  // Swipe detection for search result navigation
  const swipeRef = useRef({ startX: 0, startY: 0 });

  const fetchMobileDetail = async (period: PriceRange = '1Y') => {
    try {
      const response = await axios.get(`${API_URL}/api/v1/ticker/${ticker}/detail?period=${period}`);
      setMobileData(response.data);
    } catch (err: any) {
      console.error('Error fetching mobile detail:', err.message || err);
    } finally {
      setMobileDataLoading(false);
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
      const responseData: DividendData = response.data || {};
      const recentPaymentsRaw = Array.isArray(response.data?.recent_payments) ? response.data.recent_payments : [];
      const historyRaw = Array.isArray(response.data?.history) ? response.data.history : [];
      const resolveDividendType = (row: any): string | null => {
        if (typeof row?.dividend_type === 'string') return row.dividend_type;
        if (typeof row?.type === 'string') return row.type;
        return null;
      };

      const normalize = (rows: any[]): DividendEvent[] =>
        rows
          .map((d) => {
            const amount = typeof d?.amount === 'number' ? d.amount : Number(d?.amount);
            const paymentDateRaw = d?.payment_date ?? d?.paymentDate ?? null;
            const dividendTypeRaw = resolveDividendType(d);
            const isSpecial = d?.is_special === true
              || d?.special === true
              || (typeof dividendTypeRaw === 'string' && dividendTypeRaw.toLowerCase().includes('special'));
            const isIrregular = d?.is_irregular === true
              || d?.irregular === true
              || (typeof dividendTypeRaw === 'string' && dividendTypeRaw.toLowerCase().includes('irregular'));
            const eventTypeLabel = isSpecial ? 'Special dividend' : isIrregular ? 'Irregular dividend' : null;
            return {
              ex_date: typeof d?.ex_date === 'string' ? d.ex_date : '',
              amount,
              currency: typeof d?.currency === 'string' ? d.currency : null,
              period: typeof d?.period === 'string' ? d.period : null,
              payment_date: typeof paymentDateRaw === 'string' && paymentDateRaw ? paymentDateRaw : null,
              dividend_type: dividendTypeRaw,
              frequency_label: typeof d?.frequency_label === 'string' ? d.frequency_label : null,
              is_special: isSpecial,
              is_irregular: isIrregular,
              event_type_label: eventTypeLabel,
            };
          })
          .filter((d) => d.ex_date && Number.isFinite(d.amount))
          .sort((a, b) => b.ex_date.localeCompare(a.ex_date));

      const normalizedRecent = normalize(recentPaymentsRaw);
      const normalizedHistory = normalize(historyRaw);

      setDividendPayments(normalizedRecent);
      setDividendHistory(normalizedHistory.length > 0 ? normalizedHistory : normalizedRecent);
      setDividendFrequencyLabel(responseData?.frequency?.label || DEFAULT_FREQUENCY_LABEL);
      setDividendFrequencyFlags({
        hasSpecial: responseData?.frequency?.has_special === true,
        hasIrregular: responseData?.frequency?.has_irregular === true,
      });
      setDividendDisplayCurrency(
        responseData?.display_currency
          || responseData?.next_dividend?.next_dividend_currency
          || normalizedRecent.find((d) => typeof d.currency === 'string' && d.currency)?.currency
          || 'USD'
      );
      setNextDividendInfo(responseData?.next_dividend || null);
    } catch (err) {
      console.error('Error fetching dividends:', err, (err as any)?.response?.data);
      setDividendPayments([]);
      setDividendHistory([]);
      setDividendFrequencyLabel(DEFAULT_FREQUENCY_LABEL);
      setDividendFrequencyFlags({ hasSpecial: false, hasIrregular: false });
      setDividendDisplayCurrency('USD');
      setNextDividendInfo(null);
    } finally {
      setDividendsLoading(false);
    }
  };

  // Fetch earnings history + upcoming from dedicated endpoint
  const fetchEarningsData = async () => {
    try {
      const response = await axios.get<EarningsApiResponse>(`${API_URL}/api/v1/ticker/${ticker}/earnings`);
      setUpcomingEarnings(response.data.upcoming_earnings || null);
      setEarningsCurrency(response.data.metadata?.default_currency || null);
      // Exclude is_upcoming rows from the history list — upcoming rows belong only
      // in the "Next earnings" section, not the history table.
      const history = (response.data.earnings_history || []).filter(
        (e) => !(e.is_upcoming ?? false)
      );
      setEarningsHistory(history);
    } catch (err: any) {
      // 404 is expected for non-visible tickers; swallow silently.
      if (err?.response?.status !== 404) {
        console.error('Error fetching earnings:', err.message || err);
      }
      setUpcomingEarnings(null);
      setEarningsHistory([]);
      setEarningsCurrency(null);
    } finally {
      setEarningsLoading(false);
    }
  };

  // Fetch price history for chart (now includes benchmark)
  // Uses per-range cache so repeated MAX/range switches are instant
  const fetchChartData = async (range: PriceRange) => {
    // Return cached data instantly if available (e.g. switching back to MAX)
    const cached = chartCacheRef.current[range];
    if (cached) {
      setChartData(cached.prices);
      setBenchmarkChartData(cached.benchmark);
      setChartError(null);
      return;
    }

    setChartLoading(true);
    setChartError(null);
    setBenchmarkChartData([]);
    
    try {
      // Fetch chart data with benchmark from new endpoint
      const response = await axios.get(`${API_URL}/api/v1/ticker/${ticker}/chart?period=${range}&include_benchmark=true`);
      const prices = response.data.prices || [];
      const benchmark = response.data.benchmark;
      const notices: string[] = response.data.data_notices || [];
      
      // Downsample to ~400 points for performance
      const targetPoints = 400;
      const step = Math.max(1, Math.floor(prices.length / targetPoints));
      const downsampled = prices.filter((_: any, i: number) => i % step === 0 || i === prices.length - 1);

      // Force-include true max and min points from the full series so
      // displayed HIGH/LOW always reflect the true extrema.
      if (prices.length > 0) {
        const getPlotValue = (p: any) => p.adjusted_close || p.close;
        let trueMaxPoint = prices[0];
        let trueMinPoint = prices[0];
        let maxVal = getPlotValue(prices[0]);
        let minVal = maxVal;
        for (const p of prices) {
          const v = getPlotValue(p);
          if (v > maxVal) { trueMaxPoint = p; maxVal = v; }
          if (v < minVal) { trueMinPoint = p; minVal = v; }
        }
        const dates = new Set(downsampled.map((p: any) => p.date));
        if (!dates.has(trueMaxPoint.date)) downsampled.push(trueMaxPoint);
        if (!dates.has(trueMinPoint.date)) downsampled.push(trueMinPoint);
        downsampled.sort((a: any, b: any) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));
      }
      
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
      
      // Cache benchmark data
      let benchData: {date: string; normalized: number}[] = [];
      if (benchmark && benchmark.prices) {
        const benchStep = Math.max(1, Math.floor(benchmark.prices.length / targetPoints));
        benchData = benchmark.prices.filter((_: any, i: number) => i % benchStep === 0 || i === benchmark.prices.length - 1);
      }

      // Store in cache for instant re-access
      chartCacheRef.current[range] = { prices: formattedPrices, benchmark: benchData };

      setChartData(formattedPrices);
      setBenchmarkChartData(benchData);
      setChartDataNotices(notices);
    } catch (err: any) {
      console.error('Error fetching chart data:', err);
      setChartError('Failed to load chart');
    } finally {
      setChartLoading(false);
    }
  };

  // P25/P26: PAIN data now comes from ticker_pain_cache via /v1/ticker/{ticker}/detail API

  // Auth gating: redirect to login when session validation confirms unauthenticated
  useEffect(() => {
    if (isSessionValidated && !isAuthenticated) {
      const returnTo = `/stock/${ticker}`;
      router.replace(`/login?returnTo=${encodeURIComponent(returnTo)}` as any);
    }
  }, [isSessionValidated, isAuthenticated, ticker, router]);

  useEffect(() => {
    if (!ticker) return;
    // Clear chart cache when ticker changes (data is ticker-specific)
    chartCacheRef.current = {};
    // Reset loading states for new ticker to avoid flashing negative pills
    setMobileDataLoading(true);
    setDividendsLoading(true);
    setEarningsLoading(true);
    setDividendViewMode('annual');
    setEarningsDivMode('dividends');
    setEarningsViewMode('annual');
    fetchStock(false);
    fetchDividends();
    fetchEarningsData();
  }, [ticker]);

  useEffect(() => {
    if (ticker) {
      fetchChartData(priceRange);
      fetchMobileDetail(priceRange);
    }
  }, [ticker, priceRange]);

  const fetchListMemberships = useCallback(async () => {
    if (!sessionToken) {
      setListMemberships({ watchlist: false, tracklist: false });
      return;
    }
    try {
      const response = await axios.get(`${API_URL}/api/v1/lists/check/${ticker}`, {
        headers: { Authorization: `Bearer ${sessionToken}` },
      });
      setListMemberships(response.data.memberships || { watchlist: false, tracklist: false });
    } catch (err) {
      console.error('Error checking list memberships:', err);
      setListMemberships({ watchlist: false, tracklist: false });
    }
  }, [ticker, sessionToken]);

  const handleOpenAddTo = useCallback(() => {
    setAddToVisible(true);
    fetchListMemberships();
  }, [fetchListMemberships]);

  const handleAddTo = async (target: 'watchlist' | 'tracklist') => {
    if (listActionLoading || !sessionToken) {
      return;
    }
    if (target === 'watchlist' && listMemberships.tracklist) {
      dialog.alert('Unavailable', `${ticker} is already in your Tracklist.`);
      return;
    }
    if (target === 'tracklist' && listMemberships.watchlist) {
      dialog.alert('Unavailable', `${ticker} is already in your Watchlist.`);
      return;
    }
    if (target === 'tracklist') {
      setAddToVisible(false);
      router.push({ pathname: '/(tabs)/tracklist', params: { candidate: ticker, manage: '1' } });
      return;
    }
    setListActionLoading(true);
    const authHeaders = { Authorization: `Bearer ${sessionToken}` };
    try {
      if (target === 'watchlist' && listMemberships.watchlist) {
        await axios.delete(`${API_URL}/api/v1/watchlist/${ticker}`, {
          headers: authHeaders,
        });
      } else if (target === 'watchlist') {
        await axios.post(`${API_URL}/api/v1/watchlist/${ticker}`, {}, {
          headers: authHeaders,
        });
      }
      await fetchListMemberships();
      setAddToVisible(false);
    } catch (err) {
      console.error('Error updating list membership:', err);
      dialog.alert('Error', `Failed to update ${ticker}. Please try again.`);
    } finally {
      setListActionLoading(false);
    }
  };

  useFocusEffect(
    useCallback(() => {
      if (ticker) {
        fetchListMemberships();
      }
    }, [ticker, fetchListMemberships])
  );

  const transformTickerNewsArticles = useCallback((rawArticles: TickerNewsApiArticle[], offset: number): NewsArticle[] => {
    return rawArticles.map((article, index) => ({
      id:
        article.article_id ||
        article.source_link ||
        article.link ||
        `${ticker}-fallback-${article.title ?? 'no-title'}-${article.published_at ?? article.date ?? 'no-date'}-${offset + index}`,
      title: article.title || 'Untitled',
      content: article.content ?? null,
      source: article.source ?? null,
      link: article.source_link ?? article.link ?? null,
      date: article.published_at ?? article.date ?? null,
      ticker,
      company_name: data?.company?.name ?? null,
      logo_url: null,
      fallback_logo_key: ticker.charAt(0).toUpperCase(),
      sentiment: article.sentiment ?? null,
      sentiment_label: article.sentiment_label ?? 'neutral',
      tags: article.tags ?? [],
      time_ago: article.time_ago ?? null,
    }));
  }, [ticker, data?.company?.name]);

  const fetchInitialNews = useCallback(async () => {
    try {
      setNewsLoading(true);
      const response = await axios.get(`${API_URL}/api/news/ticker/${ticker}`, {
        params: {
          offset: 0,
          limit: NEWS_ARTICLES_FETCH_PAGE_SIZE,
        },
      });
      const rawArticles: TickerNewsApiArticle[] = Array.isArray(response.data?.articles) ? response.data.articles : [];
      setNewsArticles(transformTickerNewsArticles(rawArticles, 0));
      setNewsHasMore(Boolean(response.data?.has_more));
    } catch (err) {
      console.error('Error fetching ticker news:', err);
      setNewsArticles([]);
      setNewsHasMore(false);
    } finally {
      setNewsLoading(false);
    }
  }, [ticker, transformTickerNewsArticles]);

  const fetchMoreNews = useCallback(async () => {
    if (newsLoading) return;
    try {
      setNewsLoading(true);
      const offset = newsArticles.length;
      const response = await axios.get(`${API_URL}/api/news/ticker/${ticker}`, {
        params: {
          offset,
          limit: NEWS_ARTICLES_FETCH_PAGE_SIZE,
        },
      });
      const rawArticles: TickerNewsApiArticle[] = Array.isArray(response.data?.articles) ? response.data.articles : [];
      const incomingArticles = transformTickerNewsArticles(rawArticles, offset);
      setNewsArticles((prev) => {
        const existingIds = new Set(prev.map((article) => article.id));
        return [...prev, ...incomingArticles.filter((article) => !existingIds.has(article.id))];
      });
      setNewsHasMore(Boolean(response.data?.has_more));
    } catch (err) {
      console.error('Error fetching more ticker news:', err);
    } finally {
      setNewsLoading(false);
    }
  }, [ticker, newsArticles.length, newsLoading, transformTickerNewsArticles]);

  const fetchUpcomingSplit = useCallback(async () => {
    try {
      const response = await axios.get(`${API_URL}/api/v1/ticker/${ticker}/splits`);
      setUpcomingSplit(response.data?.upcoming_split || null);
    } catch (err: any) {
      if (err?.response?.status !== 404) {
        console.error('Error fetching upcoming split:', err?.message || err);
      }
      setUpcomingSplit(null);
    }
  }, [ticker]);

  // ===== CHART-TOOLTIP: Simple handlers (stockanalysis.com style) =====
  // Chart dimension constants (must match rendering)
  const CHART_PADDING_LEFT = 64;
  const CHART_PADDING_RIGHT = 16;
  const CHART_PADDING_TOP = 20;
  const CHART_PADDING_BOTTOM = 32;
  
  // Compute visible chart data based on benchmark toggle
  // Benchmark OFF = full ticker history | Benchmark ON = overlapping date range only
  const { visibleChartData, visibleBenchmarkData } = useMemo(() => {
    if (!showBenchmark || benchmarkChartData.length === 0 || chartData.length === 0) {
      return { visibleChartData: chartData, visibleBenchmarkData: [] as {date: string; normalized: number}[] };
    }

    // Common start = the later of first ticker date and first benchmark date
    const benchStartDate = benchmarkChartData[0].date;
    const tickerStartDate = chartData[0].date;
    const commonStartDate = benchStartDate > tickerStartDate ? benchStartDate : tickerStartDate;

    // Trim ticker data to common range
    const trimmed = chartData.filter(d => d.date >= commonStartDate);
    if (trimmed.length === 0) {
      return { visibleChartData: chartData, visibleBenchmarkData: benchmarkChartData };
    }

    // Re-normalize ticker from common start so both begin at 100
    const tickerBase = trimmed[0].normalized || 100;
    const reNormTicker = trimmed.map(d => ({
      ...d,
      normalized: ((d.normalized || 100) / tickerBase) * 100,
    }));

    // Trim benchmark to common range and re-normalize
    const trimmedBench = benchmarkChartData.filter(d => d.date >= commonStartDate);
    const benchBase = trimmedBench.length > 0 ? (trimmedBench[0].normalized || 100) : 100;
    const reNormBench = trimmedBench.map(d => ({
      ...d,
      normalized: ((d.normalized || 100) / benchBase) * 100,
    }));

    return { visibleChartData: reNormTicker, visibleBenchmarkData: reNormBench };
  }, [chartData, benchmarkChartData, showBenchmark]);

  // Compute tooltip index from X coordinate (uses visibleChartData for correct mapping)
  const computeTooltipIndex = useCallback((locationX: number, chartWidth: number): number | null => {
    if (visibleChartData.length === 0) return null;
    const graphW = chartWidth - CHART_PADDING_LEFT - CHART_PADDING_RIGHT;
    const relativeX = Math.max(0, Math.min(graphW, locationX - CHART_PADDING_LEFT));
    const ratio = relativeX / graphW;
    return Math.round(ratio * (visibleChartData.length - 1));
  }, [visibleChartData]);
  computeTooltipIndexRef.current = computeTooltipIndex;
  
  // Hide tooltip
  const hideChartTooltip = useCallback(() => {
    setChartTooltipVisible(false);
    setChartTooltipIndex(null);
  }, []);
  // ===== END CHART-TOOLTIP handlers =====

  // Fetch News & Events when ticker changes - deferred to avoid blocking initial render
  useEffect(() => {
    if (!ticker) return;
    setNewsArticles([]);
    setNewsHasMore(false);
    setNewsVisibleCount(INITIAL_NEWS_EVENTS_LIMIT);
    const timer = setTimeout(() => {
      fetchInitialNews();
      fetchUpcomingSplit();
    }, DEFERRED_FETCH_MS);
    return () => clearTimeout(timer);
  }, [ticker, fetchInitialNews, fetchUpcomingSplit]);

  const onRefresh = () => {
    setRefreshing(true);
    // Clear chart cache on refresh so fresh data is fetched
    chartCacheRef.current = {};
    fetchStock(false); // P4: Always load full data for single vertical scroll
    fetchChartData(priceRange);
    fetchDividends();
    fetchEarningsData();
    fetchInitialNews();
    fetchUpcomingSplit();
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

  // Merge company details from stock-overview (primary) and detail endpoint (fallback)
  // MUST be before conditional returns to satisfy React Rules of Hooks
  const companyDetails = useMemo(() => {
    const co = data?.company;
    const cd = mobileData?.company_details;
    return {
      city: co?.city || cd?.city,
      state: co?.state || cd?.state,
      country_name: co?.country_name || cd?.country_name,
      website: co?.website || cd?.website,
      employees: co?.full_time_employees || cd?.employees,
      ipo_date: co?.ipo_date || cd?.ipo_date,
      description: co?.description || cd?.description,
    };
  }, [data?.company, mobileData?.company_details]);

  // P21: EU/CZ Number Formatting - import utility
  // Thousands separator: space, Decimal separator: , (comma)
  const toEU = (value: number, decimals: number = 2): string => {
    if (value === null || value === undefined || isNaN(value)) return 'N/A';
    const fixed = value.toFixed(decimals);
    const [intPart, decPart] = fixed.split('.');
    const intWithSpaces = intPart.replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
    return decPart ? `${intWithSpaces},${decPart}` : intWithSpaces;
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
    if (value === null || value === undefined) return 'N/A';
    const absValue = Math.abs(value);
    const sign = value < 0 ? '-' : '';
    if (absValue >= 1e12) return `${sign}$${toEU(absValue / 1e12, 2)}T`;
    if (absValue >= 1e9) return `${sign}$${toEU(absValue / 1e9, 2)}B`;
    if (absValue >= 1e6) return `${sign}$${toEU(absValue / 1e6, 2)}M`;
    return `${sign}$${toEU(absValue, 2)}`;
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

  // Format Performance Check percentages as whole numbers with space-separated thousands
  const formatLargePercent = (value: number | null | undefined, showSign: boolean = true) => {
    if (value === null || value === undefined) return 'N/A';
    const sign = value >= 0 ? (showSign ? '+' : '') : '-';
    const absValue = Math.abs(value);

    return `${sign}${toEU(absValue, 0)}%`;
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

  const formatDividendAmount = (value: number | null | undefined, currency?: string | null): string => {
    if (typeof value !== 'number' || !Number.isFinite(value)) return 'N/A';
    const normalizedCurrency = resolveDividendCurrency(currency, null);
    return normalizedCurrency ? `${normalizedCurrency} ${toEU(value, 2)}` : toEU(value, 2);
  };

  const formatUpcomingEarningsEstimate = (estimate?: number | string | null, currency?: string | null): string => {
    const numericEstimate = typeof estimate === 'string' ? Number(estimate) : estimate;
    if (typeof numericEstimate !== 'number' || !Number.isFinite(numericEstimate)) return 'Expected —';
    const currencyPrefix = currency && currency !== 'USD' ? `${currency} ` : '$';
    return `Exp. ${currencyPrefix}${toEU(numericEstimate, 2)}`;
  };

  const getPragueDateString = (): string => {
    const parts = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Europe/Prague',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).formatToParts(new Date());
    const year = parts.find((part) => part.type === 'year')?.value ?? '1970';
    const month = parts.find((part) => part.type === 'month')?.value ?? '01';
    const day = parts.find((part) => part.type === 'day')?.value ?? '01';
    return `${year}-${month}-${day}`;
  };

  const todayPrague = getPragueDateString();

  const newsEventItems = useMemo<NewsEventFeedItem[]>(() => {
    const eventItems: NewsEventFeedItem[] = [];

    if (upcomingEarnings?.report_date && upcomingEarnings.report_date >= todayPrague) {
      const marketLabel = getMarketTimingLabel(upcomingEarnings.before_after_market);
      eventItems.push({
        kind: 'event',
        id: `earnings-${upcomingEarnings.report_date}`,
        eventType: 'Earnings',
        title: 'Upcoming Earnings',
        subtitle: marketLabel
          ? `${formatUpcomingEarningsEstimate(upcomingEarnings.estimate, upcomingEarnings.currency)}${EVENT_SUBTITLE_SEPARATOR}${marketLabel}`
          : formatUpcomingEarningsEstimate(upcomingEarnings.estimate, upcomingEarnings.currency),
        date: upcomingEarnings.report_date,
      });
    }

    if (nextDividendInfo?.next_ex_date && nextDividendInfo.next_ex_date >= todayPrague) {
      const dividendSubtitleParts: string[] = [];
      if (typeof nextDividendInfo.next_dividend_amount === 'number') {
        dividendSubtitleParts.push(
          formatDividendAmount(
            nextDividendInfo.next_dividend_amount,
            resolveDividendCurrency(nextDividendInfo.next_dividend_currency, dividendDisplayCurrency),
          ),
        );
      }
      dividendSubtitleParts.push(`Ex ${formatDividendEventDate(nextDividendInfo.next_ex_date)}`);
      if (nextDividendInfo.next_pay_date) {
        dividendSubtitleParts.push(`Pay ${formatDividendEventDate(nextDividendInfo.next_pay_date)}`);
      }
      eventItems.push({
        kind: 'event',
        id: `dividend-${nextDividendInfo.next_ex_date}`,
        eventType: 'Dividend',
        title: 'Upcoming Ex-Dividend',
        subtitle: dividendSubtitleParts.join(EVENT_SUBTITLE_SEPARATOR),
        date: nextDividendInfo.next_ex_date,
      });
    }

    if (upcomingSplit?.split_date && upcomingSplit.split_date >= todayPrague) {
      eventItems.push({
        kind: 'event',
        id: `split-${upcomingSplit.split_date}`,
        eventType: 'Split',
        title: 'Upcoming Split',
        subtitle: getFormattedSplitRatio(upcomingSplit) || 'Upcoming split',
        date: upcomingSplit.split_date,
      });
    }

    const articleItems: NewsEventFeedItem[] = newsArticles.map((article) => ({
      kind: 'article',
      id: article.id,
      article,
    }));

    return [...eventItems, ...articleItems];
  }, [
    upcomingEarnings,
    nextDividendInfo,
    upcomingSplit,
    todayPrague,
    newsArticles,
    dividendDisplayCurrency,
    formatDividendAmount,
    formatUpcomingEarningsEstimate,
  ]);

  const aggregateSentiment = useMemo(
    () => getAggregateSentimentFromArticles(newsArticles),
    [newsArticles],
  );

  const shouldFetchMoreNews = useMemo(
    () => newsVisibleCount + NEWS_EVENTS_PAGE_SIZE > newsEventItems.length && newsHasMore && !newsLoading,
    [newsVisibleCount, newsEventItems.length, newsHasMore, newsLoading],
  );

  type AnnualDividendPeriod = {
    key: string;
    label: string;
    total: number;
    previousTotal: number | null;
    isPartial?: boolean;
    isTTM?: boolean;
  };

  type AnnualEarningsPeriod = {
    key: string;
    label: string;
    annualReportedEps: number;
    reportsCount: number;
    beatCount: number;
    missCount: number;
    inlineCount: number;
    naCount: number;
    neutralCount: number;
    previousAnnualReportedEps: number | null;
    previousReportsCount: number | null;
    isPartial: boolean;
  };

  const annualDividendPeriods = useMemo<AnnualDividendPeriod[]>(() => {
    const events = dividendHistory
      .map((d) => ({ amount: d.amount, exDateMs: parseDividendExDateMs(d.ex_date) }))
      .filter((d): d is { amount: number; exDateMs: number } => d.exDateMs !== null);

    if (events.length === 0) return [];

    const nowDate = new Date();

    const yearTotals = new Map<number, number>();
    for (const event of events) {
      const year = new Date(event.exDateMs).getUTCFullYear();
      yearTotals.set(year, (yearTotals.get(year) || 0) + event.amount);
    }

    const periods: AnnualDividendPeriod[] = [];

    const currentYear = nowDate.getUTCFullYear();
    const years = Array.from(yearTotals.keys()).sort((a, b) => b - a).slice(0, 10);
    for (const year of years) {
      periods.push({
        key: String(year),
        label: String(year),
        total: round4(yearTotals.get(year) || 0),
        previousTotal: yearTotals.has(year - 1) ? round4(yearTotals.get(year - 1) || 0) : null,
        isTTM: false,
        isPartial: year === currentYear,
      });
    }

    return periods;
  }, [dividendHistory]);

  const hasAnnualDividendData = annualDividendPeriods.length > 0 && annualDividendPeriods.some((p) => p.total > 0);

  const getAnnualYoyDisplay = (current: number | null, previous: number | null, isPartial = false, isTTM = false) => {
    if (isPartial) return { label: 'Partial', tone: 'neutral' as const, helper: 'Partial year' };
    if (current === null || previous === null) return { label: '—', tone: 'neutral' as const };
    if (current === 0 && previous === 0) return { label: '—', tone: 'neutral' as const };
    if (previous === 0) return current > 0
      ? { label: 'New', tone: 'neutral' as const }
      : { label: '—', tone: 'neutral' as const };
    if (current === 0) {
      return isTTM
        ? { label: 'Suspended', tone: 'negative' as const }
        : { label: '0%', tone: 'negative' as const };
    }
    const pct = ((current - previous) / previous) * 100;
    return {
      label: `${pct >= 0 ? '+' : ''}${toEU(pct, 1)}%`,
      tone: pct > 0 ? ('positive' as const) : pct < 0 ? ('negative' as const) : ('neutral' as const),
    };
  };

  const annualEarningsPeriods = useMemo<AnnualEarningsPeriod[]>(() => {
    const yearBuckets = new Map<number, {
      annualReportedEps: number;
      reportsCount: number;
      beatCount: number;
      missCount: number;
      inlineCount: number;
      naCount: number;
    }>();

    for (const row of earningsHistory) {
      if (row.reported_eps == null) continue;
      const yearText = typeof row.quarter_date === 'string' ? row.quarter_date.slice(0, 4) : '';
      const year = Number.parseInt(yearText, 10);
      if (!Number.isFinite(year)) continue;

      const bucket = yearBuckets.get(year) ?? {
        annualReportedEps: 0,
        reportsCount: 0,
        beatCount: 0,
        missCount: 0,
        inlineCount: 0,
        naCount: 0,
      };

      bucket.annualReportedEps += row.reported_eps;
      bucket.reportsCount += 1;

      if (row.show_badge === true && row.surprise_pct != null && row.estimated_eps != null && row.estimated_eps !== 0) {
        if (row.surprise_pct > 0) bucket.beatCount += 1;
        else if (row.surprise_pct < 0) bucket.missCount += 1;
        else bucket.inlineCount += 1;
      } else {
        bucket.naCount += 1;
      }

      yearBuckets.set(year, bucket);
    }

    const years = Array.from(yearBuckets.keys()).sort((a, b) => b - a);
    return years.map((year) => {
      const current = yearBuckets.get(year)!;
      const previous = yearBuckets.get(year - 1) ?? null;
      return {
        key: String(year),
        label: String(year),
        annualReportedEps: round4(current.annualReportedEps),
        reportsCount: current.reportsCount,
        beatCount: current.beatCount,
        missCount: current.missCount,
        inlineCount: current.inlineCount,
        naCount: current.naCount,
        neutralCount: current.inlineCount + current.naCount,
        previousAnnualReportedEps: previous ? round4(previous.annualReportedEps) : null,
        previousReportsCount: previous ? previous.reportsCount : null,
        isPartial: previous !== null && current.reportsCount < previous.reportsCount,
      };
    });
  }, [earningsHistory]);

  const getAnnualEarningsYoyDisplay = (period: AnnualEarningsPeriod) => {
    const previous = period.previousAnnualReportedEps;
    const previousReportsCount = period.previousReportsCount;
    if (
      previous == null ||
      previousReportsCount == null ||
      period.reportsCount !== previousReportsCount ||
      previous === 0
    ) {
      return { label: '—', tone: 'neutral' as const };
    }

    const pct = ((period.annualReportedEps - previous) / Math.abs(previous)) * 100;
    return {
      label: `${pct >= 0 ? '+' : ''}${toEU(pct, 1)}%`,
      tone: pct >= 0 ? ('positive' as const) : ('negative' as const),
    };
  };

  const showAnnualEarningsBreakdown = (period: AnnualEarningsPeriod) => {
    void dialog.alert(
      `Annual Earnings · ${period.label}`,
      `Beat ${period.beatCount}, Miss ${period.missCount}, Other ${period.neutralCount}`,
    );
  };

  const getDividendToneStyle = (tone: 'positive' | 'negative' | 'neutral') => {
    if (tone === 'positive') return styles.dividendValuePositive;
    if (tone === 'negative') return styles.dividendValueNegative;
    return styles.dividendValueNeutral;
  };

  const paymentItems = useMemo(() => {
    const seenKeys = new Map<string, number>();
    return dividendPayments.slice(0, 10).map((event) => {
      const occurrenceCount = seenKeys.get(event.ex_date) || 0;
      seenKeys.set(event.ex_date, occurrenceCount + 1);
      const key = occurrenceCount === 0 ? event.ex_date : `${event.ex_date}-${occurrenceCount + 1}`;
      return { key, event };
    });
  }, [dividendPayments]);

  const getPaymentGrowthDisplay = (current: DividendEvent, previous: DividendEvent | null) => {
    if (!previous) return { label: 'Growth: —', badgeLabel: '—', tone: 'neutral' as const };
    if (
      current.is_special || previous.is_special
      || current.is_irregular || previous.is_irregular
      || (current.currency && previous.currency && current.currency !== previous.currency)
      || previous.amount <= 0
    ) {
      return { label: 'Growth: not comparable', badgeLabel: '—', tone: 'neutral' as const };
    }
    const pct = ((current.amount - previous.amount) / previous.amount) * 100;
    if (Math.abs(pct) < FLAT_GROWTH_THRESHOLD_PCT) return { label: 'Growth: flat', badgeLabel: 'flat', tone: 'neutral' as const };
    const pctStr = `${pct >= 0 ? '+' : ''}${toEU(pct, 1)}%`;
    return {
      label: `Growth: ${pctStr}`,
      badgeLabel: pctStr,
      tone: pct > 0 ? ('positive' as const) : ('negative' as const),
    };
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
        'extreme_outlier': 'N/A (Data unreliable)',
        'missing_inputs': 'N/A (Data missing)',
        'not_reported': 'N/A (Not reported)',
        'unreliable': 'N/A (Data unreliable)',
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
        'no_dividend': '0.00% (No dividend)',
        'extreme_outlier': 'N/A (Data unreliable)',
        'unreliable': 'N/A (Data unreliable)',
        'missing_inputs': 'N/A (Data missing)',
        'not_reported': 'N/A (Not reported)',
        'default': '0.00% (No dividend)'
      }
    };
    
    // Has valid value
    if (metric?.value !== null && metric?.value !== undefined) {
      const isNegative = metric.value < 0;
      
      // D) Dividend with industry context + RED color rule
      if (metricType === 'dividend_yield_ttm') {
        // D) RED if dividend == 0% AND industry median > 1% (opportunity cost)
        const isOpportunityCost = metric.value === 0 && industryDivMedian !== null && industryDivMedian > 1;
        return {
          text: `${toEU(metric.value, 2)}%`,
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

  /** Format peer median hint for a key metric (e.g. "Industry median: 12,3%") */
  const formatPeerMedianHint = (median: number | null | undefined, unit: '%' | 'x', level?: string | null): string | null => {
    if (median == null) return null;
    const levelLabel = level ? level.charAt(0).toUpperCase() + level.slice(1) : 'Peer';
    return `${levelLabel} median: ${toEU(median, unit === 'x' ? 1 : 2)}${unit}`;
  };

  /** Compute comparison pill for company value vs benchmark */
  const getBenchmarkPill = (
    companyValue: number | null | undefined,
    benchmarkMeta: BenchmarkMetadata | null | undefined,
    metricKey: string
  ): { label: string; color: string; bgColor: string } | null => {
    if (companyValue == null || benchmarkMeta == null || benchmarkMeta.benchmark_level == null) return null;

    const bv = benchmarkMeta.benchmark_value;
    if (bv == null) return null;

    const delta = companyValue - bv;

    // Determine epsilon threshold
    // percentage metrics: 0.1 pp; multiple/x metrics: 0.05x; pe_ttm: 0.1
    const pctMetrics = new Set(['net_margin_ttm', 'fcf_yield', 'revenue_growth_3y', 'dividend_yield_ttm', 'roe']);
    const multipleMetrics = new Set(['net_debt_ebitda']);
    let epsilon: number;
    if (pctMetrics.has(metricKey)) {
      epsilon = 0.1;
    } else if (multipleMetrics.has(metricKey)) {
      epsilon = 0.05;
    } else {
      epsilon = 0.1; // pe_ttm default
    }

    if (Math.abs(delta) < epsilon) {
      return { label: 'Same', color: '#6B7280', bgColor: '#F3F4F6' };
    }

    // Determine direction: higher is better vs lower is better
    const lowerIsBetter = new Set(['pe_ttm', 'net_debt_ebitda']);
    const isLower = lowerIsBetter.has(metricKey);
    const isBetter = isLower ? delta < 0 : delta > 0;

    if (isBetter) {
      return { label: 'Better than peers', color: '#166534', bgColor: '#DCFCE7' };
    } else {
      return { label: 'Worse than peers', color: '#991B1B', bgColor: '#FEE2E2' };
    }
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

  // Auth gating: show loading skeleton while session is being validated (no broken header)
  if (!isSessionValidated) {
    return <BrandedLoading message={`Loading ${ticker}...`} />;
  }

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
  const rawLogoUrl = company.logo_url || mobileData?.company?.logo_url;
  const logoUrl = rawLogoUrl
    ? (rawLogoUrl.startsWith('http') ? rawLogoUrl : `${API_URL}${rawLogoUrl}`)
    : null;

  const resolveNewsLogoUrl = (rawUrl?: string | null): string | undefined => {
    if (!rawUrl) return logoUrl ?? undefined;
    return rawUrl.startsWith('http') ? rawUrl : `${API_URL}${rawUrl}`;
  };

  const formatNewsDate = (dateStr?: string | null): string => {
    if (!dateStr) return '';
    const d = dateStr.includes('T')
      ? new Date(dateStr)
      : new Date(`${dateStr}T12:00:00Z`);
    if (isNaN(d.getTime())) return '';
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
  };

  const openArticle = (article: NewsArticle) => {
    setSelectedArticle(article);
  };

  const closeArticle = () => {
    setSelectedArticle(null);
  };

  const openExternalLink = (url: string) => {
    Linking.openURL(url);
  };

  const selectedArticleSentimentLabel = selectedArticle
    ? getSentimentLabelFromScores(selectedArticle.sentiment, selectedArticle.sentiment_label)
    : 'neutral';
  const selectedArticleSentimentTone = getSentimentTone(selectedArticleSentimentLabel);

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
   * - "Verifying…" while initial data fetch is in progress
   */
  const getKeyMetricsPills = (): string[] => {
    // Show neutral loading pill while initial fetch is in progress
    if (mobileDataLoading) return ['Verifying…'];
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
    // Show neutral loading pill while initial fetch is in progress
    if (dividendsLoading) return 'Verifying…';

    // Canonical na_reason from Key Metrics — takes precedence over local dividend_history
    const canonicalNaReason = mobileData?.key_metrics?.dividend_yield_ttm?.na_reason;
    if (canonicalNaReason === 'unreliable') return 'Unreliable data';
    if (canonicalNaReason === 'extreme_outlier') return 'Unreliable data';

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
    // Show neutral loading pill while initial fetch is in progress
    if (mobileDataLoading) return 'Verifying…';
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
      'No dividends', 'Cutting', 'Revenue down', 'No financials', 'Unreliable data'
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
        if (!hasPagerNav) return;
        const dx = e.nativeEvent.pageX - swipeRef.current.startX;
        const dy = e.nativeEvent.pageY - swipeRef.current.startY;
        if (Math.abs(dx) > 80 && Math.abs(dx) > Math.abs(dy) * 2) {
          if (dx < 0 && pagerNext) navigateToTicker(pagerNext);
          else if (dx > 0 && pagerPrev) navigateToTicker(pagerPrev);
        }
      }}
    >
      {/* Persistent Top Bar */}
      <AppHeader
        title={company.code}
        showSubscriptionBadge={false}
      />

      {/* Navigation row below AppHeader */}
      {hasPagerNav ? (
        <View style={styles.searchNavBar}>
          <TouchableOpacity
            style={[styles.searchNavButton, !pagerPrev && styles.searchNavButtonDisabled]}
            onPress={() => pagerPrev && navigateToTicker(pagerPrev)}
            disabled={!pagerPrev}
          >
            <Ionicons name="chevron-back" size={16} color={pagerPrev ? COLORS.primary : COLORS.textMuted} />
            {pagerPrev && <Text style={styles.searchNavTicker} numberOfLines={1}>{pagerPrev}</Text>}
          </TouchableOpacity>

          <View style={styles.searchNavCenter}>
            <Text style={styles.searchNavCounter}>
              {pagerLabel}
            </Text>
            <TouchableOpacity
              style={styles.searchNavClose}
              onPress={pagerClear}
            >
              <Ionicons name="close" size={16} color={COLORS.textMuted} />
            </TouchableOpacity>
          </View>

          <TouchableOpacity
            style={[styles.searchNavButton, styles.searchNavButtonRight, !pagerNext && styles.searchNavButtonDisabled]}
            onPress={() => pagerNext && navigateToTicker(pagerNext)}
            disabled={!pagerNext}
          >
            {pagerNext && <Text style={styles.searchNavTicker} numberOfLines={1}>{pagerNext}</Text>}
            <Ionicons name="chevron-forward" size={16} color={pagerNext ? COLORS.primary : COLORS.textMuted} />
          </TouchableOpacity>
        </View>
      ) : (
        <View style={styles.searchNavBar}>
          <TouchableOpacity
            style={styles.searchNavButton}
            onPress={safeBack}
            accessibilityLabel="Go back"
            accessibilityRole="button"
          >
            <Ionicons name="chevron-back" size={16} color={COLORS.primary} />
            <Text style={styles.searchNavTicker}>Back</Text>
          </TouchableOpacity>
        </View>
      )}

      <ScrollView
        ref={scrollViewRef}
        style={styles.scrollView}
        contentContainerStyle={[styles.scrollContent, { padding: sp.pageGutter }]}
        showsVerticalScrollIndicator={false}
        refreshControl={<RefreshControl refreshing={refreshing} onRefresh={onRefresh} />}
      >
        {/* ===== FUNDAMENTALS PENDING BANNER ===== */}
        {data?.fundamentals_pending && !mobileData?.key_metrics && (
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
              <Text
                style={styles.compactName}
                numberOfLines={1}
                ellipsizeMode="tail"
                accessibilityLabel={company.name || ticker?.toString() || ''}
              >
                {company.name || ticker}
              </Text>
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
            
            {/* Sector & Industry pills */}
            <View style={styles.classificationRow}>
              {company.sector && (
                <View style={styles.companyMetaPill}>
                  <Text style={styles.companyMetaPillLabel}>Sector</Text>
                  <Text style={styles.companyMetaPillValue}>{company.sector}</Text>
                </View>
              )}
              {company.industry && (
                <View style={styles.companyMetaPill}>
                  <Text style={styles.companyMetaPillLabel}>Industry</Text>
                  <Text style={styles.companyMetaPillValue}>{company.industry}</Text>
                </View>
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
          <Text style={styles.companyDetailsMinimalText}>Company Details</Text>
          <Ionicons 
            name={companyDetailsExpanded ? 'chevron-up' : 'chevron-down'} 
            size={18} 
            color={COLORS.textMuted} 
          />
        </TouchableOpacity>
        
        {companyDetailsExpanded && (
          <View style={styles.companyDetailsExpanded}>
            {(companyDetails.city || companyDetails.state || companyDetails.country_name) && (
              <View style={styles.companyDetailRowReadable}>
                <Text style={styles.companyDetailLabel}>Location</Text>
                <Text style={styles.companyDetailValue}>
                  {[companyDetails.city, companyDetails.state, companyDetails.country_name].filter(Boolean).join(', ')}
                </Text>
              </View>
            )}
            {companyDetails.website && (
              <TouchableOpacity 
                style={styles.companyDetailRowReadable}
                onPress={() => Linking.openURL(companyDetails.website!)}
              >
                <Text style={styles.companyDetailLabel}>Website</Text>
                <Text style={[styles.companyDetailValue, styles.companyDetailLink]}>
                  {companyDetails.website.replace(/^https?:\/\//, '').replace(/\/$/, '')}
                </Text>
              </TouchableOpacity>
            )}
            {companyDetails.employees && (
              <View style={styles.companyDetailRowReadable}>
                <Text style={styles.companyDetailLabel}>Employees</Text>
                <Text style={styles.companyDetailValue}>{formatNumber(companyDetails.employees)}</Text>
              </View>
            )}
            {companyDetails.ipo_date && (
              <View style={styles.companyDetailRowReadable}>
                <Text style={styles.companyDetailLabel}>IPO Date</Text>
                <Text style={styles.companyDetailValue}>{formatDateDMY(companyDetails.ipo_date)}</Text>
              </View>
            )}
            {companyDetails.description && (
              <>
                <Text style={styles.companyDetailLabel}>Description</Text>
                <Text 
                  style={styles.descriptionTextCompact} 
                  numberOfLines={showFullDescription ? undefined : 3}
                >
                  {companyDetails.description}
                </Text>
                {companyDetails.description.length > 150 && (
                  <TouchableOpacity onPress={() => setShowFullDescription(!showFullDescription)}>
                    <Text style={styles.companyShowMoreText}>
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
            <View style={styles.lastCloseHeader}>
              <View style={styles.lastCloseMeta}>
                <View style={styles.lastCloseTitleRow}>
                  <Ionicons name="time-outline" size={18} color={COLORS.primary} />
                  <Text style={styles.lastCloseTitle}>Last Close</Text>
                </View>
                <Text style={styles.priceDate}>as of {formatDateDMY(price.date)}</Text>
              </View>
              <TouchableOpacity
                style={[
                  styles.addToButton,
                  listMemberships.watchlist && styles.addToButtonActiveWatchlist,
                  listMemberships.tracklist && styles.addToButtonActiveTracklist,
                ]}
                onPress={handleOpenAddTo}
              >
                <Ionicons name="add" size={16} color={COLORS.text} />
                <Text style={styles.addToButtonText}>
                  {listMemberships.watchlist
                    ? 'In Watchlist'
                    : listMemberships.tracklist
                      ? 'In Tracklist'
                      : 'Add to'}
                </Text>
              </TouchableOpacity>
            </View>
            <View style={styles.priceRow}>
              <View style={styles.priceValueRow}>
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
            </View>
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

        {/* ===== PRICE CHART (P1 UX: Chart-first flow) ===== */}
        <View 
          style={styles.priceChartCard} 
          data-testid="price-chart-card"
        >
          <View style={styles.sectionHeader}>
            <Text style={styles.sectionIcon}>📈</Text>
            <Text style={styles.sectionTitleBold}>Price History</Text>
            <TouchableOpacity
              style={styles.perfCheckPeriodTouchable}
              onPress={() => setPriceRangeSelectorVisible(true)}
              accessibilityRole="button"
              accessibilityLabel="Change price history period"
            >
              <Text style={styles.perfCheckPeriodBadge}>
                {priceRange === 'MAX' ? 'Full History' : `Past ${priceRange}`}
              </Text>
              <Ionicons name="chevron-down" size={12} color={COLORS.primary} />
            </TouchableOpacity>
          </View>

          {visibleChartData.length > 0 && (
            <Text style={styles.dateRangeText}>
              {formatDateDMY(visibleChartData[0]?.date)} – {formatDateDMY(visibleChartData[visibleChartData.length - 1]?.date)}
            </Text>
          )}
          
          {/* ===== DATA NOTICE BANNER ===== */}
          {chartDataNotices.length > 0 && (
            <View style={styles.dataNoticeBanner} data-testid="data-notice-banner">
              <Ionicons name="information-circle-outline" size={16} color="#92400E" />
              <View style={{ flex: 1 }}>
                {chartDataNotices.map((notice, idx) => (
                  <Text key={idx} style={styles.dataNoticeText}>{notice}</Text>
                ))}
              </View>
            </View>
          )}

          {showBenchmark && visibleBenchmarkData.length > 0 && (
            <Text style={styles.benchmarkNote}>Comparison starts at first common date.</Text>
          )}
          
          <View style={styles.chartContainer}
            onLayout={(e) => {
              const w = e.nativeEvent.layout.width;
              if (w > 0 && w !== chartWMeasured) setChartWMeasured(w);
            }}
          >
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
            ) : visibleChartData.length > 0 && chartWMeasured > 0 ? (
              (() => {
                const BADGE_FONT_SIZE = 12;
                const BADGE_CHAR_W = 7;
                const BADGE_PAD_H = 7;
                const BADGE_H = 22;
                const chartW = chartWMeasured;
                chartWRef.current = chartW;
                const chartH = 240;
                
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
                  const LABEL_HEIGHT = 20;
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
                const paddingLeft = 64;
                const paddingRight = 16;
                const paddingTop = 20;
                const paddingBottom = 20;
                const graphW = chartW - paddingLeft - paddingRight;
                const graphH = chartH - paddingTop - paddingBottom;
                
                const values = visibleChartData.map(d => d.adjusted_close);
                const dataMin = Math.min(...values);
                const dataMax = Math.max(...values);
                const currentPrice = visibleChartData[visibleChartData.length - 1].adjusted_close;
                
                const highIdx = values.indexOf(dataMax);
                const lowIdx = values.indexOf(dataMin);
                
                const range = dataMax - dataMin || 1;
                const yPad = range * 0.1;
                const yMin = dataMin - yPad;
                const yMax = dataMax + yPad;
                
                const priceToY = (price: number) => paddingTop + graphH - ((price - yMin) / (yMax - yMin)) * graphH;
                
                const dataCount = visibleChartData.length;
                const points = visibleChartData.map((d, i) => {
                  const x = dataCount === 1
                    ? paddingLeft + graphW / 2
                    : paddingLeft + (i / (dataCount - 1)) * graphW;
                  const y = priceToY(d.adjusted_close);
                  return { x, y };
                });
                
                const pathD = points.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(' ');
                const lineColor = '#6B7280';
                
                let benchmarkPathD = '';
                if (visibleBenchmarkData.length > 1 && visibleChartData.length > 1 && visibleChartData[0].normalized) {
                  const stockNormValues = visibleChartData.map(d => d.normalized || 100);
                  const benchNormValues = visibleBenchmarkData.map(d => d.normalized || 100);
                  const allNormValues = [...stockNormValues, ...benchNormValues];
                  const normMin = Math.min(...allNormValues);
                  const normMax = Math.max(...allNormValues);
                  const normRange = normMax - normMin || 1;
                  const normYPad = normRange * 0.1;
                  const normYMin = normMin - normYPad;
                  const normYMax = normMax + normYPad;
                  
                  const normToY = (val: number) => paddingTop + graphH - ((val - normYMin) / (normYMax - normYMin)) * graphH;
                  
                  const benchPoints = visibleBenchmarkData.map((d, i) => {
                    const x = paddingLeft + (i / (visibleBenchmarkData.length - 1)) * graphW;
                    const y = normToY(d.normalized);
                    return { x, y };
                  });
                  benchmarkPathD = benchPoints.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(' ');
                }
                
                const highY = priceToY(dataMax);
                const lowY = priceToY(dataMin);
                const currentY = priceToY(currentPrice);
                const highX = dataCount === 1
                  ? paddingLeft + graphW / 2
                  : paddingLeft + (highIdx / (dataCount - 1)) * graphW;
                const lowX = dataCount === 1
                  ? paddingLeft + graphW / 2
                  : paddingLeft + (lowIdx / (dataCount - 1)) * graphW;
                const formatPrice = (p: number) => {
                  if (p >= 1_000_000) return `$${toEU(p / 1_000_000, 1)}M`;
                  if (p >= 1000) return `$${toEU(p / 1000, 1)}k`;
                  return `$${toEU(p, 0)}`;
                };
                const formatChartDate = (dateStr?: string) => formatDateDMY(dateStr);
                const X_AXIS_DATE_LABEL_W = 72;
                const X_AXIS_DATE_LABEL_GAP = 12;
                const clampXAxisDateX = (x: number) => {
                  const minX = paddingLeft + X_AXIS_DATE_LABEL_W / 2;
                  const maxX = paddingLeft + graphW - X_AXIS_DATE_LABEL_W / 2;
                  return Math.max(minX, Math.min(maxX, x));
                };
                const highDateLabel = formatChartDate(visibleChartData[highIdx]?.date);
                const lowDateLabel = formatChartDate(visibleChartData[lowIdx]?.date);
                let highDateX = clampXAxisDateX(highX);
                let lowDateX = clampXAxisDateX(lowX);
                let highDateY = chartH - 6;
                let lowDateY = chartH - 6;
                const xAxisDateTargetGap = X_AXIS_DATE_LABEL_W + X_AXIS_DATE_LABEL_GAP;
                if (Math.abs(highDateX - lowDateX) < xAxisDateTargetGap) {
                  const midpoint = (highDateX + lowDateX) / 2;
                  const shift = xAxisDateTargetGap / 2;
                  highDateX = clampXAxisDateX(highX >= lowX ? midpoint + shift : midpoint - shift);
                  lowDateX = clampXAxisDateX(lowX <= highX ? midpoint - shift : midpoint + shift);
                }
                if (Math.abs(highDateX - lowDateX) < xAxisDateTargetGap) {
                  if (lowX <= highX) {
                    lowDateY -= 12;
                  } else {
                    highDateY -= 12;
                  }
                }
                
                // ===== Y-AXIS GRID: Compute ~4 evenly spaced horizontal grid lines =====
                const yAxisTicks: { price: number; y: number; label: string }[] = (() => {
                  const numTicks = 4;
                  const step = (yMax - yMin) / (numTicks + 1);
                  const ticks: { price: number; y: number; label: string }[] = [];
                  for (let i = 1; i <= numTicks; i++) {
                    const price = yMin + step * i;
                    ticks.push({ price, y: priceToY(price), label: formatPrice(price) });
                  }
                  return ticks;
                })();
                

                // Compute chart labels with deterministic stacking
                const chartLabels = computeChartLabels(
                  highY, lowY, currentY,
                  dataMax, dataMin, currentPrice,
                  chartH,
                  formatPrice
                );
                
                // ===== CHART-TOOLTIP: Simple crosshair (stockanalysis.com style) =====
                const tooltipPoint = chartTooltipVisible && chartTooltipIndex !== null && visibleChartData[chartTooltipIndex] 
                  ? visibleChartData[chartTooltipIndex] 
                  : null;
                
                const tooltipX = tooltipPoint 
                  ? (dataCount === 1
                      ? paddingLeft + graphW / 2
                      : paddingLeft + (chartTooltipIndex! / (dataCount - 1)) * graphW)
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
                        const idx = computeTooltipIndexRef.current(getX(ev), chartWRef.current);
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
                        const idx = computeTooltipIndexRef.current(getX(ev), chartWRef.current);
                        if (idx !== null) {
                          setChartTooltipVisible(true);
                          setChartTooltipIndex(idx);
                        }
                      });
                      domEl.addEventListener('touchmove', (ev) => {
                        const idx = computeTooltipIndexRef.current(getX(ev), chartWRef.current);
                        if (idx !== null) setChartTooltipIndex(idx);
                      });
                      domEl.addEventListener('touchend', () => {
                        setChartTooltipVisible(false);
                        setChartTooltipIndex(null);
                      });
                    }}
                  >
                    <Svg width={chartW} height={chartH} style={{ position: 'absolute', top: 0, left: 0 }}>
                      {/* Y-axis grid lines */}
                      {yAxisTicks.map((tick, i) => (
                        <G key={`y-grid-${i}`}>
                          <Line x1={paddingLeft} y1={tick.y} x2={paddingLeft + graphW} y2={tick.y}
                            stroke="#E5E7EB" strokeWidth={0.5} />
                          <SvgText x={paddingLeft - 6} y={tick.y + 4} fontSize={10} fill="#9CA3AF" textAnchor="end">
                            {tick.label}
                          </SvgText>
                        </G>
                      ))}
                      
                      {/* X-axis baseline */}
                      <Line x1={paddingLeft} y1={chartH - paddingBottom} x2={paddingLeft + graphW} y2={chartH - paddingBottom}
                        stroke="#E5E7EB" strokeWidth={0.5} />
                      
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
                       {highIdx !== lowIdx && (
                         <>
                           <Line x1={highX} y1={chartH - paddingBottom} x2={highX} y2={chartH - paddingBottom + 6}
                             stroke="#10B981" strokeWidth={1} />
                           <Line x1={lowX} y1={chartH - paddingBottom} x2={lowX} y2={chartH - paddingBottom + 6}
                             stroke="#EF4444" strokeWidth={1} />
                           <SvgText x={highDateX} y={highDateY} fontSize={10} fill="#10B981" fontWeight="600" textAnchor="middle">
                             {highDateLabel}
                           </SvgText>
                           <SvgText x={lowDateX} y={lowDateY} fontSize={10} fill="#EF4444" fontWeight="600" textAnchor="middle">
                             {lowDateLabel}
                           </SvgText>
                         </>
                       )}
                       
                       {/* Price labels as colored badges (green=HIGH, red=LOW, dark=PRICE) */}
                       {chartLabels.map(label => {
                        const badgeW = Math.max(label.text.length * BADGE_CHAR_W + BADGE_PAD_H * 2, 28);
                        const badgeX = paddingLeft - 3 - badgeW;
                        const badgeY = label.adjustedY - BADGE_H / 2;
                        return (
                          <G key={label.id}>
                            <Rect
                              x={badgeX}
                              y={badgeY}
                              width={badgeW}
                              height={BADGE_H}
                              rx={4}
                              fill={label.color}
                            />
                            <SvgText
                              x={badgeX + badgeW / 2}
                              y={badgeY + BADGE_H / 2 + 4.5}
                              fontSize={BADGE_FONT_SIZE}
                              fill="#FFFFFF"
                              fontWeight="700"
                              textAnchor="middle"
                            >
                              {label.text}
                            </SvgText>
                          </G>
                        );
                      })}
                      
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
            ) : visibleChartData.length === 0 ? (
              <View style={styles.chartLoading}>
                <Text style={styles.chartLoadingText}>No data available</Text>
              </View>
            ) : null}
          </View>
          
          {visibleChartData.length > 0 && (
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
                <TouchableOpacity
                  style={[styles.benchmarkToggle, showBenchmark && styles.benchmarkToggleActive]}
                  onPress={() => setShowBenchmark(!showBenchmark)}
                  data-testid="benchmark-toggle"
                >
                  {showBenchmark ? (
                    <View style={styles.benchmarkToggleRow}>
                      <View style={styles.benchmarkToggleDot} />
                      <Text style={[styles.benchmarkToggleText, styles.benchmarkToggleTextActive]}>S&P 500 TR</Text>
                      <Text style={styles.benchmarkToggleDismiss}>✕</Text>
                    </View>
                  ) : (
                    <Text style={styles.benchmarkToggleText}>+ Compare S&P 500 TR</Text>
                  )}
                </TouchableOpacity>
              )}
            </View>
          )}
        </View>

        {/* ===== UNIFIED PERFORMANCE CHECK (Dynamic based on period) ===== */}
        {/* P1 CRITICAL: Single source of truth - stats change with period selector */}
        {mobileData?.period_stats && (
          <View 
            style={styles.perfCheckCard} 
            data-testid="reality-check-card"
          >
            {/* Header row: title left, period badge right */}
            <View style={styles.perfCheckHeaderRow}>
              <View style={styles.perfCheckTitleRow}>
                <Text style={styles.sectionIcon}>📊</Text>
                <Text style={styles.sectionTitleBold}>Performance Check</Text>
              </View>
              <TouchableOpacity
                style={styles.perfCheckPeriodTouchable}
                onPress={() => setPerfCheckPeriodVisible(true)}
                accessibilityRole="button"
                accessibilityLabel="Change performance period"
              >
                <Text style={styles.perfCheckPeriodBadge}>
                  {priceRange === 'MAX' ? 'Full History' : `Past ${priceRange}`}
                </Text>
                <Ionicons name="chevron-down" size={12} color={COLORS.primary} />
              </TouchableOpacity>
            </View>
            <Text style={styles.perfCheckDateRange}>
              {formatDateDMY(mobileData.period_stats.start_date)} – {formatDateDMY(mobileData.period_stats.end_date)}
            </Text>
            
            {/* Two sub-cards: Reward | Risk */}
            <View style={styles.perfCheckColumns}>
              {/* REWARD sub-card - GREEN */}
              <View style={styles.perfCheckRewardCard}>
                <TouchableOpacity
                  style={styles.perfCheckCardHeader}
                  onPress={() => showTooltip('perfCheckReward')}
                  accessibilityRole="button"
                >
                  <Ionicons name="trending-up" size={18} color="#059669" />
                  <Text style={styles.perfCheckRewardTitle}>REWARD</Text>
                  <Ionicons name="help-circle-outline" size={14} color={COLORS.textMuted} />
                </TouchableOpacity>
                
                {/* Total Profit */}
                <TouchableOpacity
                  style={styles.perfCheckMetricLabelRow}
                  onPress={() => showTooltip('perfCheckTotalProfit')}
                  accessibilityRole="button"
                >
                  <Text style={styles.perfCheckMetricLabel}>Total Profit</Text>
                  <Ionicons name="help-circle-outline" size={14} color={COLORS.textMuted} />
                </TouchableOpacity>
                <Text style={[
                  styles.perfCheckMetricValueLarge,
                  mobileData.period_stats.profit_pct >= 0 ? styles.positiveText : styles.negativeText
                ]}>
                  {formatLargePercent(mobileData.period_stats.profit_pct)}
                </Text>
                
                {/* Average per year (CAGR) */}
                {mobileData.period_stats.cagr_pct !== null && (
                  <View style={styles.perfCheckMetricRow}>
                    <TouchableOpacity
                      style={styles.perfCheckMetricLabelRow}
                      onPress={() => showTooltip('perfCheckAvgPerYear')}
                      accessibilityRole="button"
                    >
                      <Text style={styles.perfCheckMetricLabel}>Avg. per Year</Text>
                      <Ionicons name="help-circle-outline" size={14} color={COLORS.textMuted} />
                    </TouchableOpacity>
                    <View style={styles.perfCheckMetricInlineRow}>
                      <Ionicons name={mobileData.period_stats.cagr_pct >= 0 ? "arrow-up-outline" : "arrow-down-outline"} size={14} color={mobileData.period_stats.cagr_pct >= 0 ? '#10B981' : '#EF4444'} />
                      <Text style={[
                        styles.perfCheckMetricValue, 
                        mobileData.period_stats.cagr_pct >= 0 ? styles.positiveText : styles.negativeText
                      ]}>
                        {formatLargePercent(mobileData.period_stats.cagr_pct)}
                      </Text>
                    </View>
                  </View>
                )}
                
                {/* Reward / Risk (RRR) */}
                {(() => {
                  const rrr = computeRRR(chartData);
                  if (rrr === null) return null;
                  
                  return (
                    <View style={styles.perfCheckMetricRow} data-testid="rrr-performance-check">
                      <TouchableOpacity
                        style={styles.perfCheckMetricLabelRow}
                        onPress={() => showTooltip('perfCheckRewardRisk')}
                        accessibilityRole="button"
                      >
                        <Text style={styles.perfCheckMetricLabel}>Reward / Risk</Text>
                        <Ionicons name="help-circle-outline" size={14} color={COLORS.textMuted} />
                      </TouchableOpacity>
                      <View style={styles.perfCheckMetricInlineRow}>
                        <Text style={[
                          styles.perfCheckMetricValue,
                          rrr > 2 ? styles.positiveText :
                          rrr >= 1 ? styles.neutralText :
                          styles.rrrNegativeText
                        ]}>
                          {formatRRR(rrr)}
                        </Text>
                      </View>
                    </View>
                  );
                })()}
              </View>
              
              {/* RISK sub-card - RED */}
              <View style={styles.perfCheckRiskCard}>
                <TouchableOpacity
                  style={styles.perfCheckCardHeader}
                  onPress={() => showTooltip('perfCheckRisk')}
                  accessibilityRole="button"
                >
                  <Ionicons name="alert-circle" size={18} color="#DC2626" />
                  <Text style={styles.perfCheckRiskTitle}>RISK</Text>
                  <Ionicons name="help-circle-outline" size={14} color={COLORS.textMuted} />
                </TouchableOpacity>
                
                {/* Max Drawdown */}
                <TouchableOpacity
                  style={styles.perfCheckMetricLabelRow}
                  onPress={() => showTooltip('perfCheckMaxDrawdown')}
                  accessibilityRole="button"
                >
                  <Text style={styles.perfCheckMetricLabel}>Max. Drawdown</Text>
                  <Ionicons name="help-circle-outline" size={14} color={COLORS.textMuted} />
                </TouchableOpacity>
                <Text style={[styles.perfCheckMetricValueLarge, styles.negativeText]}>
                  {formatLargePercent(-Math.abs(mobileData.period_stats.max_drawdown_pct))}
                </Text>
                
                {/* Drawdown details */}
                {drawdownDetails && (
                  <>
                    <View style={styles.perfCheckMetricRow}>
                      <TouchableOpacity
                        style={styles.perfCheckMetricLabelRow}
                        onPress={() => showTooltip('perfCheckDuration')}
                        accessibilityRole="button"
                      >
                        <Text style={styles.perfCheckMetricLabel}>Duration</Text>
                        <Ionicons name="help-circle-outline" size={14} color={COLORS.textMuted} />
                      </TouchableOpacity>
                      <Text style={styles.perfCheckMetricValue}>
                        {drawdownDetails.durationDays} days
                      </Text>
                    </View>
                    <View style={styles.perfCheckMetricRow}>
                      <TouchableOpacity
                        style={styles.perfCheckMetricLabelRow}
                        onPress={() => showTooltip('perfCheckRecovered')}
                        accessibilityRole="button"
                      >
                        <Text style={styles.perfCheckMetricLabel}>Recovered</Text>
                        <Ionicons name="help-circle-outline" size={14} color={COLORS.textMuted} />
                      </TouchableOpacity>
                      <Text style={styles.perfCheckMetricValue}>
                        {drawdownDetails.recoveryDate 
                          ? formatDateDMY(drawdownDetails.recoveryDate)
                          : 'Not yet'}
                      </Text>
                    </View>
                  </>
                )}
              </View>
            </View>
            
            {/* INDEX BLOCK - Index comparison (stacked, readable) */}
            <View style={styles.perfCheckIndexBlock}>
              <View style={styles.perfCheckIndexTitleRow}>
                <Text style={styles.perfCheckIndexTitle}>Index (S&P 500 TR)</Text>
                <TouchableOpacity onPress={() => showTooltip('perfCheckIndex')} accessibilityRole="button">
                  <Ionicons name="help-circle-outline" size={14} color={COLORS.textMuted} />
                </TouchableOpacity>
              </View>
              {mobileData.period_stats.benchmark_total_pct !== null && (
                <Text style={styles.perfCheckIndexRow}>
                  Total return:{' '}
                  <Text style={styles.perfCheckIndexRowValue}>
                    {formatLargePercent(mobileData.period_stats.benchmark_total_pct)}
                  </Text>
                </Text>
              )}
              {/* P0 FIX: Use backend's Wealth Gap calculation (outperformance_pct) */}
              {(() => {
                const wealthGap = mobileData.period_stats.outperformance_pct;
                if (wealthGap === null || wealthGap === undefined) return null;
                const deltaClamped = Math.max(wealthGap, -100);
                return (
                  <Text style={styles.perfCheckIndexRow}>
                    {'Stock vs. index: '}
                    <Text style={[
                      styles.perfCheckIndexRowValue,
                      deltaClamped > 0 ? styles.positiveText :
                      deltaClamped < 0 ? styles.negativeText : null
                    ]}>
                      {formatLargePercent(deltaClamped)}
                    </Text>
                  </Text>
                );
              })()}
            </View>
            
            {/* Footer disclaimer */}
            <Text style={styles.perfCheckDisclaimer}>
              Past returns do not guarantee future gains. Context only, not advice.
            </Text>
          </View>
        )}


        {/* ===== SECTION 4: KEY METRICS (Hybrid 7) - Collapsed by default ===== */}
        <View 
          style={styles.sectionSurfaceCard} 
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
                    <View style={styles.metricValueCol}>
                      <View style={styles.metricValueRow}>
                        {(() => {
                          const { text, color } = formatKeyMetricWithEmpathy(mobileData.key_metrics.net_margin_ttm, 'net_margin_ttm');
                          return <Text style={[styles.metricValue, { color }]}>{text}</Text>;
                        })()}
                        {(() => {
                          const pill = getBenchmarkPill(mobileData.key_metrics.net_margin_ttm?.value, mobileData.key_metrics.net_margin_ttm?.benchmark_metadata, 'net_margin_ttm');
                          return pill ? <View style={[styles.benchmarkPill, { backgroundColor: pill.bgColor }]}><Text style={[styles.benchmarkPillText, { color: pill.color }]}>{pill.label}</Text></View> : null;
                        })()}
                      </View>
                      {formatPeerMedianHint(mobileData.key_metrics.net_margin_ttm?.peer_median, '%', mobileData.key_metrics.net_margin_ttm?.peer_median_level) && (
                        <Text style={styles.peerMedianHint}>{formatPeerMedianHint(mobileData.key_metrics.net_margin_ttm?.peer_median, '%', mobileData.key_metrics.net_margin_ttm?.peer_median_level)}</Text>
                      )}
                    </View>
                  </View>
                
                  {/* FCF Yield */}
                  <View style={styles.metricRow}>
                    <TouchableOpacity style={styles.metricLabelRow} onPress={() => showTooltip('fcfYield')}>
                      <Text style={styles.metricLabel}>Free Cash Flow Yield</Text>
                      <Ionicons name="information-circle-outline" size={14} color={COLORS.textMuted} />
                    </TouchableOpacity>
                    <View style={styles.metricValueCol}>
                      <View style={styles.metricValueRow}>
                        {(() => {
                          const { text, color } = formatKeyMetricWithEmpathy(mobileData.key_metrics.fcf_yield, 'fcf_yield');
                          return <Text style={[styles.metricValue, { color }]}>{text}</Text>;
                        })()}
                        {(() => {
                          const pill = getBenchmarkPill(mobileData.key_metrics.fcf_yield?.value, mobileData.key_metrics.fcf_yield?.benchmark_metadata, 'fcf_yield');
                          return pill ? <View style={[styles.benchmarkPill, { backgroundColor: pill.bgColor }]}><Text style={[styles.benchmarkPillText, { color: pill.color }]}>{pill.label}</Text></View> : null;
                        })()}
                      </View>
                      {formatPeerMedianHint(mobileData.key_metrics.fcf_yield?.peer_median, '%', mobileData.key_metrics.fcf_yield?.peer_median_level) && (
                        <Text style={styles.peerMedianHint}>{formatPeerMedianHint(mobileData.key_metrics.fcf_yield?.peer_median, '%', mobileData.key_metrics.fcf_yield?.peer_median_level)}</Text>
                      )}
                    </View>
                  </View>
                
                  {/* Net Debt / EBITDA */}
                  <View style={styles.metricRow}>
                    <TouchableOpacity style={styles.metricLabelRow} onPress={() => showTooltip('netDebtEbitda')}>
                      <Text style={styles.metricLabel}>Net Debt / EBITDA</Text>
                      <Ionicons name="information-circle-outline" size={14} color={COLORS.textMuted} />
                    </TouchableOpacity>
                    <View style={styles.metricValueCol}>
                      <View style={styles.metricValueRow}>
                        {(() => {
                          const { text, color } = formatKeyMetricWithEmpathy(mobileData.key_metrics.net_debt_ebitda, 'net_debt_ebitda');
                          return <Text style={[styles.metricValue, { color }]}>{text}</Text>;
                        })()}
                        {(() => {
                          const pill = getBenchmarkPill(mobileData.key_metrics.net_debt_ebitda?.value, mobileData.key_metrics.net_debt_ebitda?.benchmark_metadata, 'net_debt_ebitda');
                          return pill ? <View style={[styles.benchmarkPill, { backgroundColor: pill.bgColor }]}><Text style={[styles.benchmarkPillText, { color: pill.color }]}>{pill.label}</Text></View> : null;
                        })()}
                      </View>
                      {formatPeerMedianHint(mobileData.key_metrics.net_debt_ebitda?.peer_median, 'x', mobileData.key_metrics.net_debt_ebitda?.peer_median_level) && (
                        <Text style={styles.peerMedianHint}>{formatPeerMedianHint(mobileData.key_metrics.net_debt_ebitda?.peer_median, 'x', mobileData.key_metrics.net_debt_ebitda?.peer_median_level)}</Text>
                      )}
                    </View>
                  </View>
                
                  {/* Revenue Growth 3Y CAGR */}
                  <View style={styles.metricRow}>
                    <TouchableOpacity style={styles.metricLabelRow} onPress={() => showTooltip('revenueGrowth')}>
                      <Text style={styles.metricLabel}>Revenue Growth (3Y CAGR)</Text>
                      <Ionicons name="information-circle-outline" size={14} color={COLORS.textMuted} />
                    </TouchableOpacity>
                    <View style={styles.metricValueCol}>
                      <View style={styles.metricValueRow}>
                        {(() => {
                          const { text, color } = formatKeyMetricWithEmpathy(mobileData.key_metrics.revenue_growth_3y, 'revenue_growth_3y');
                          return <Text style={[styles.metricValue, { color }]}>{text}</Text>;
                        })()}
                        {(() => {
                          const pill = getBenchmarkPill(mobileData.key_metrics.revenue_growth_3y?.value, mobileData.key_metrics.revenue_growth_3y?.benchmark_metadata, 'revenue_growth_3y');
                          return pill ? <View style={[styles.benchmarkPill, { backgroundColor: pill.bgColor }]}><Text style={[styles.benchmarkPillText, { color: pill.color }]}>{pill.label}</Text></View> : null;
                        })()}
                      </View>
                      {formatPeerMedianHint(mobileData.key_metrics.revenue_growth_3y?.peer_median, '%', mobileData.key_metrics.revenue_growth_3y?.peer_median_level) && (
                        <Text style={styles.peerMedianHint}>{formatPeerMedianHint(mobileData.key_metrics.revenue_growth_3y?.peer_median, '%', mobileData.key_metrics.revenue_growth_3y?.peer_median_level)}</Text>
                      )}
                    </View>
                  </View>
                
                  {/* Dividend Yield TTM with peer median context */}
                  <View style={styles.metricRow}>
                    <TouchableOpacity style={styles.metricLabelRow} onPress={() => showTooltip('dividendYield')}>
                      <Text style={styles.metricLabel}>Dividend Yield (TTM)</Text>
                      <Ionicons name="information-circle-outline" size={14} color={COLORS.textMuted} />
                    </TouchableOpacity>
                    <View style={styles.metricValueCol}>
                      <View style={styles.metricValueRow}>
                        {(() => {
                          const div = mobileData.key_metrics.dividend_yield_ttm;
                          const industryMedian = div?.industry_dividend_yield_median;
                          const { text, color } = formatKeyMetricWithEmpathy(div, 'dividend_yield_ttm', industryMedian);
                          return <Text style={[styles.metricValue, { color }]}>{text}</Text>;
                        })()}
                        {(() => {
                          const pill = getBenchmarkPill(mobileData.key_metrics.dividend_yield_ttm?.value, mobileData.key_metrics.dividend_yield_ttm?.benchmark_metadata, 'dividend_yield_ttm');
                          return pill ? <View style={[styles.benchmarkPill, { backgroundColor: pill.bgColor }]}><Text style={[styles.benchmarkPillText, { color: pill.color }]}>{pill.label}</Text></View> : null;
                        })()}
                      </View>
                      {formatPeerMedianHint(mobileData.key_metrics.dividend_yield_ttm?.peer_median, '%', mobileData.key_metrics.dividend_yield_ttm?.peer_median_level) && (
                        <Text style={styles.peerMedianHint}>{formatPeerMedianHint(mobileData.key_metrics.dividend_yield_ttm?.peer_median, '%', mobileData.key_metrics.dividend_yield_ttm?.peer_median_level)}</Text>
                      )}
                    </View>
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

            {/* No Benchmark Warning — uses per-metric fallback chain from new endpoint */}
            {(() => {
              const hasBenchmark = mobileData?.has_benchmark ?? data?.has_benchmark;
              const benchmarkFallback = mobileData?.benchmark_fallback ?? data?.benchmark_fallback;
              const industryName = mobileData?.company?.industry || data?.company?.industry;
              const sectorName = mobileData?.company?.sector || data?.company?.sector;

              // Don't flash a yellow banner while data is still loading
              if (mobileDataLoading) return null;

              // If we have benchmarks, show info about fallback level if not industry
              if (hasBenchmark) {
                if (benchmarkFallback && benchmarkFallback !== 'industry' && industryName) {
                  const levelLabel = benchmarkFallback === 'sector' ? `${sectorName || 'Sector'}` : 'Market';
                  return (
                    <View style={[styles.peerDisclaimer, { backgroundColor: '#EFF6FF' }]}>
                      <Ionicons name="information-circle-outline" size={14} color="#3B82F6" />
                      <Text style={[styles.disclaimerText, { color: '#1E40AF' }]}>
                        Benchmarks use {levelLabel} peers (insufficient data for {industryName} alone).
                      </Text>
                    </View>
                  );
                }
                return null; // Industry-level benchmarks — no warning needed
              }

              // No benchmark at any level
              if (!industryName) return null;
              return (
                <View style={[styles.peerDisclaimer, { backgroundColor: '#FEF3C7' }]}>
                  <Ionicons name="alert-circle-outline" size={14} color="#D97706" />
                  <Text style={[styles.disclaimerText, { color: '#92400E' }]}>
                    No peer benchmark available for {industryName}. Insufficient data at all levels.
                  </Text>
                </View>
              );
            })()}
            </>
          )}
        </View>

        {/* ===== SECTION 5: FINANCIAL HUB (P9) - Replaces old Financials ===== */}
        <View 
          style={styles.sectionSurfaceCard}
          data-testid="financials-section"
        >
          <FinancialHub
            financials={mobileData?.financials || data?.financials}
            expanded={financialsExpanded}
            onToggle={() => setFinancialsExpanded(!financialsExpanded)}
            loading={mobileDataLoading}
            emptyStateMessage="No financials data available"
          />
        </View>

        {/* ===== SECTION 6: EARNINGS & DIVIDENDS - Collapsible with Dividend pill ===== */}
        <View 
          style={styles.perfCheckCard} 
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
                <Text style={styles.sectionTitleBold}>Dividends & Earnings</Text>
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
              {/* ── Main tab selector: Dividends | Earnings (full-width segmented control) ── */}
              <View style={styles.earningsDivTabRow}>
                <TouchableOpacity
                  style={[styles.earningsDivTab, earningsDivMode === 'dividends' && styles.earningsDivTabActive]}
                  onPress={() => setEarningsDivMode('dividends')}
                >
                  <Text style={[styles.earningsDivTabText, earningsDivMode === 'dividends' && styles.earningsDivTabTextActive]}>
                    Dividends
                  </Text>
                </TouchableOpacity>
                <TouchableOpacity
                  style={[styles.earningsDivTab, earningsDivMode === 'earnings' && styles.earningsDivTabActive]}
                  onPress={() => setEarningsDivMode('earnings')}
                >
                  <Text style={[styles.earningsDivTabText, earningsDivMode === 'earnings' && styles.earningsDivTabTextActive]}>
                    Earnings
                  </Text>
                </TouchableOpacity>
              </View>

              {/* ══════════ EARNINGS TAB ══════════ */}
              {earningsDivMode === 'earnings' && (
                <>
                  {/* Header tiles: Currency + Next Earnings */}
                  <View style={styles.dividendMetaRow}>
                    {earningsCurrency ? (
                      <View style={styles.dividendMetaPill}>
                        <Text style={styles.dividendMetaPillLabel}>Currency</Text>
                        <Text style={styles.dividendMetaPillValue}>{earningsCurrency}</Text>
                      </View>
                    ) : null}
                    {/* Next Earnings calendar-style tile */}
                    <View style={[styles.dividendMetaPill, styles.earningsNextTilePill]}>
                      <View style={styles.earningsNextTileHeader}>
                        <Ionicons name="calendar-outline" size={12} color={COLORS.textMuted} />
                        <Text style={styles.earningsNextTileLabel}>NEXT EARNINGS</Text>
                      </View>
                      <View style={styles.earningsNextTileBody}>
                        <View style={{ flex: 1 }}>
                          <Text style={styles.earningsNextTilePrimary}>
                            {upcomingEarnings ? formatDateDMY(upcomingEarnings.report_date) : 'No upcoming earnings'}
                          </Text>
                          <Text style={styles.earningsNextTileSecondary}>
                            {formatUpcomingEarningsEstimate(upcomingEarnings?.estimate, upcomingEarnings?.currency)}
                          </Text>
                        </View>
                        <View style={{ alignItems: 'flex-end', gap: 4 }}>
                          {upcomingEarnings ? (() => {
                            const marketTiming = (upcomingEarnings.before_after_market ?? '').toLowerCase();
                            const isBefore = marketTiming.startsWith('before');
                            const isAfter = marketTiming.startsWith('after');
                            return (
                              <View style={[styles.beatMissBadge, styles.dividendYoYBadgeNeutralBase, { flexDirection: 'row', alignItems: 'center', gap: 4 }]}>
                                {(isBefore || isAfter) && (
                                  <Ionicons
                                    name={isBefore ? 'sunny-outline' : 'moon-outline'}
                                    size={12}
                                    color="#6B7280"
                                  />
                                )}
                                <Text style={[styles.beatMissText, styles.dividendYoYBadgeTextNeutral]}>
                                  {isBefore ? 'Before Market' : isAfter ? 'After Market' : 'Scheduled'}
                                </Text>
                              </View>
                            );
                          })() : (
                            <View style={[styles.beatMissBadge, styles.dividendYoYBadgeNeutralBase]}>
                              <Text style={[styles.beatMissText, styles.dividendYoYBadgeTextNeutral]}>—</Text>
                            </View>
                          )}
                          {(() => {
                            if (!upcomingEarnings?.report_date) return null;
                            const daysLeft = Math.ceil(
                              (new Date(upcomingEarnings.report_date + 'T00:00:00Z').getTime() - Date.now()) / 86400000
                            );
                            if (daysLeft > 0 && daysLeft <= 365) {
                              return (
                                <View style={styles.earningsCountdownBadge}>
                                  <Ionicons name="time-outline" size={11} color="#6B7280" />
                                  <Text style={styles.earningsCountdownText}>{daysLeft}d</Text>
                                </View>
                              );
                            }
                            return null;
                          })()}
                        </View>
                      </View>
                    </View>
                  </View>

                  <View style={styles.dividendViewSwitch}>
                    <TouchableOpacity
                      style={[styles.dividendViewButton, earningsViewMode === 'annual' && styles.dividendViewButtonActive]}
                      onPress={() => setEarningsViewMode('annual')}
                    >
                      <Text style={[styles.dividendViewButtonText, earningsViewMode === 'annual' && styles.dividendViewButtonTextActive]}>
                        Annual
                      </Text>
                    </TouchableOpacity>
                    <TouchableOpacity
                      style={[styles.dividendViewButton, earningsViewMode === 'history' && styles.dividendViewButtonActive]}
                      onPress={() => setEarningsViewMode('history')}
                    >
                      <Text style={[styles.dividendViewButtonText, earningsViewMode === 'history' && styles.dividendViewButtonTextActive]}>
                        History
                      </Text>
                    </TouchableOpacity>
                  </View>

                  {earningsViewMode === 'annual' ? (
                    <>
                      <Text style={styles.subsectionTitle}>Annual Earnings</Text>
                      {annualEarningsPeriods.length > 0 ? (
                        <View style={styles.dividendAnnualSection}>
                          <View style={styles.dividendAnnualList}>
                            {annualEarningsPeriods.map((period) => {
                              const yoy = getAnnualEarningsYoyDisplay(period);
                              return (
                                <View key={period.key} style={styles.earningsRow}>
                                  <View style={styles.earningsLeft}>
                                    <View style={styles.earningsEpsRow}>
                                      <Text style={styles.earningsDate}>{period.label}</Text>
                                      {period.isPartial ? (
                                        <View style={styles.earningsPartialBadge}>
                                          <Text style={styles.earningsPartialText}>Partial</Text>
                                        </View>
                                      ) : null}
                                    </View>
                                    <Text style={styles.earningsAnnualPrimaryValue}>
                                      Act ${toEU(period.annualReportedEps, 2)}
                                    </Text>
                                    <Text style={styles.earningsAnnualSummary}>
                                      Reports: {period.reportsCount}
                                    </Text>
                                    {period.reportsCount > 0 ? (
                                      <TouchableOpacity
                                        style={styles.earningsAnnualBarButton}
                                        onPress={() => showAnnualEarningsBreakdown(period)}
                                        accessibilityRole="button"
                                        accessibilityLabel={`Show ${period.label} earnings outcome breakdown`}
                                        accessibilityHint={`Beat ${period.beatCount}, miss ${period.missCount}, other ${period.neutralCount}`}
                                      >
                                        <View style={styles.earningsAnnualBarTrack}>
                                          {period.beatCount > 0 ? (
                                            <View style={[styles.earningsAnnualBarSegment, styles.earningsAnnualBarSegmentBeat, { flex: period.beatCount }]} />
                                          ) : null}
                                          {period.missCount > 0 ? (
                                            <View style={[styles.earningsAnnualBarSegment, styles.earningsAnnualBarSegmentMiss, { flex: period.missCount }]} />
                                          ) : null}
                                          {period.neutralCount > 0 ? (
                                            <View style={[styles.earningsAnnualBarSegment, styles.earningsAnnualBarSegmentOther, { flex: period.neutralCount }]} />
                                          ) : null}
                                        </View>
                                        <Ionicons name="help-circle-outline" size={13} color={EARNINGS_NEUTRAL_COLOR} />
                                      </TouchableOpacity>
                                    ) : null}
                                  </View>
                                  <View
                                    style={[
                                      styles.beatMissBadge,
                                      yoy.tone === 'positive' ? styles.beatBadge
                                      : yoy.tone === 'negative' ? styles.missBadge
                                      : styles.dividendYoYBadgeNeutralBase,
                                    ]}
                                  >
                                    <Text
                                      style={[
                                        styles.beatMissText,
                                        yoy.tone === 'positive' ? styles.beatText
                                        : yoy.tone === 'negative' ? styles.missText
                                        : styles.dividendYoYBadgeTextNeutral,
                                      ]}
                                    >
                                      {yoy.label}
                                    </Text>
                                  </View>
                                </View>
                              );
                            })}
                          </View>
                        </View>
                      ) : !earningsLoading ? (
                        <View style={styles.noDataPlaceholder}>
                          <Text style={styles.noDataText}>No earnings data available</Text>
                        </View>
                      ) : null}
                    </>
                  ) : earningsHistory.length > 0 ? (
                    <>
                      <TouchableOpacity style={[styles.subsectionTitleRow, { marginTop: 8 }]} onPress={() => showTooltip('earningsHeader')} accessibilityRole="button" accessibilityLabel="Show earnings history help">
                        <Text style={styles.subsectionTitle}>Earnings History</Text>
                        <Ionicons name="help-circle-outline" size={14} color={COLORS.textMuted} />
                      </TouchableOpacity>
                      {earningsHistory.slice(0, 8).map((e, i) => {
                        const hasEstimate = e.estimated_eps != null && e.estimated_eps !== 0;
                        const showBadge = e.show_badge != null
                          ? (e.show_badge === true && hasEstimate && e.surprise_pct != null)
                          : (
                              e.reported_eps != null &&
                              hasEstimate &&
                              e.surprise_pct != null
                            );
                        const badgeIsPositive = (e.surprise_pct ?? 0) >= 0;
                        const neutralTooltipKey = hasEstimate ? 'earningsNA' : 'earningsNoEstimate';
                        return (
                          <View key={i} style={styles.earningsRow}>
                            <View style={styles.earningsLeft}>
                              <Text style={styles.earningsDate}>{formatDateDMY(e.quarter_date)}</Text>
                              <View style={styles.earningsEpsRow}>
                                <TouchableOpacity onPress={() => showTooltip('earningsActual')} accessibilityRole="button" accessibilityLabel="Show actual earnings help">
                                  <Text style={styles.earningsEpsLabel}>Act</Text>
                                </TouchableOpacity>
                                <Text style={styles.earningsEpsValue}>
                                  {'$'}{e.reported_eps != null
                                    ? toEU(e.reported_eps, 2)
                                    : <Text style={styles.earningsNAText} onPress={() => showTooltip('earningsNA')}>N/A</Text>}
                                </Text>
                                <Text style={styles.earningsEpsSep}>·</Text>
                                <TouchableOpacity onPress={() => showTooltip('earningsExpected')} accessibilityRole="button" accessibilityLabel="Show expected earnings help">
                                  <Text style={styles.earningsEpsLabel}>Exp</Text>
                                </TouchableOpacity>
                                <Text style={styles.earningsEpsValue}>
                                  {hasEstimate
                                    ? `$${toEU(e.estimated_eps!, 2)}`
                                    : <Text style={styles.earningsNAText} onPress={() => showTooltip('earningsNoEstimate')}>N/A</Text>}
                                </Text>
                              </View>
                            </View>
                            <TouchableOpacity
                              style={[
                                styles.beatMissBadge,
                                showBadge
                                  ? (badgeIsPositive ? styles.beatBadge : styles.missBadge)
                                  : styles.dividendYoYBadgeNeutralBase,
                              ]}
                              onPress={() => showTooltip(showBadge ? 'earningsBeatMiss' : neutralTooltipKey)}
                              accessibilityRole="button"
                              accessibilityLabel="Show beat or miss help"
                            >
                              {showBadge && (
                                <Ionicons
                                  name={badgeIsPositive ? 'checkmark' : 'close'}
                                  size={14}
                                  color={badgeIsPositive ? '#10B981' : '#EF4444'}
                                />
                              )}
                              <Text style={[
                                styles.beatMissText,
                                showBadge
                                  ? (badgeIsPositive ? styles.beatText : styles.missText)
                                  : styles.dividendYoYBadgeTextNeutral,
                              ]}>
                                {showBadge && e.surprise_pct != null
                                  ? `${toEU(e.surprise_pct, 1)}%`
                                  : '—'}
                              </Text>
                            </TouchableOpacity>
                          </View>
                        );
                      })}
                    </>
                  ) : !earningsLoading ? (
                    <View style={styles.noDataPlaceholder}>
                      <Text style={styles.noDataText}>No earnings data available</Text>
                    </View>
                  ) : null}
                </>
              )}

              {/* ══════════ DIVIDENDS TAB ══════════ */}
              {earningsDivMode === 'dividends' && (
                <>
                  {/* Meta tiles: Frequency, Currency */}
                  <View style={styles.dividendMetaRow}>
                    <TouchableOpacity style={styles.dividendMetaPill} onPress={() => showTooltip('dividendsFrequency')} accessibilityRole="button" accessibilityLabel="Show dividend frequency help">
                      <Text style={styles.dividendMetaPillLabel}>Frequency</Text>
                      <Text style={styles.dividendMetaPillValue}>{dividendFrequencyLabel}</Text>
                    </TouchableOpacity>
                    <TouchableOpacity style={styles.dividendMetaPill} onPress={() => showTooltip('dividendsCurrency')} accessibilityRole="button" accessibilityLabel="Show dividend currency help">
                      <Text style={styles.dividendMetaPillLabel}>Currency</Text>
                      <Text style={styles.dividendMetaPillValue}>{dividendDisplayCurrency}</Text>
                    </TouchableOpacity>
                    {dividendFrequencyFlags.hasSpecial && (
                      <View style={[styles.dividendMetaPill, styles.dividendMetaPillAccent]}>
                        <Text style={styles.dividendMetaPillValue}>Special</Text>
                      </View>
                    )}
                    {dividendFrequencyFlags.hasIrregular && (
                      <View style={[styles.dividendMetaPill, styles.dividendMetaPillAccent]}>
                        <Text style={styles.dividendMetaPillValue}>Irregular</Text>
                      </View>
                    )}
                    <View style={[styles.dividendMetaPill, styles.dividendNextTilePill]}>
                      <TouchableOpacity style={styles.earningsNextTileHeader} onPress={() => showTooltip('dividendsNextDividend')} accessibilityRole="button" accessibilityLabel="Show next dividend help">
                        <Ionicons name="calendar-outline" size={12} color={COLORS.textMuted} />
                        <Text style={styles.earningsNextTileLabel}>NEXT DIVIDEND</Text>
                        {nextDividendInfo?.event_type_label && (
                          <View style={[styles.dividendEventTag, { marginLeft: 4 }]}>
                            <Text style={styles.dividendEventTagText}>{nextDividendInfo.event_type_label}</Text>
                          </View>
                        )}
                      </TouchableOpacity>
                      {nextDividendInfo?.next_ex_date ? (
                        <View style={styles.earningsNextTileBody}>
                          <View style={{ flex: 1 }}>
                            <Text style={styles.earningsNextTilePrimary}>
                              {typeof nextDividendInfo.next_dividend_amount === 'number'
                                ? formatDividendAmount(nextDividendInfo.next_dividend_amount, resolveDividendCurrency(nextDividendInfo.next_dividend_currency, dividendDisplayCurrency))
                                : '—'}
                            </Text>
                            <Text style={styles.earningsNextTileDate}>
                              Ex {formatDividendEventDate(nextDividendInfo.next_ex_date)}
                            </Text>
                            {nextDividendInfo.next_pay_date && (
                              <Text style={styles.earningsNextTileDate}>
                                Pay {formatDividendEventDate(nextDividendInfo.next_pay_date)}
                              </Text>
                            )}
                          </View>
                          <View style={{ alignItems: 'flex-end', gap: 4 }}>
                            {(() => {
                              const daysLeft = Math.ceil(
                                (new Date(nextDividendInfo.next_ex_date + 'T00:00:00Z').getTime() - Date.now()) / 86400000
                              );
                              if (daysLeft > 0 && daysLeft <= 365) {
                                return (
                                  <View style={styles.earningsCountdownBadge}>
                                    <Ionicons name="time-outline" size={11} color="#6B7280" />
                                    <Text style={styles.earningsCountdownText}>{daysLeft}d</Text>
                                  </View>
                                );
                              }
                              return null;
                            })()}
                          </View>
                        </View>
                      ) : (
                        <Text style={styles.nextDividendNeutralText}>No upcoming dividend information available.</Text>
                      )}
                    </View>
                  </View>

                  {/* Annual / Payments sub-tab switcher (full-width, Annual left/default) */}
                  <View style={styles.dividendViewSwitch}>
                    <TouchableOpacity
                      style={[styles.dividendViewButton, dividendViewMode === 'annual' && styles.dividendViewButtonActive]}
                      onPress={() => setDividendViewMode('annual')}
                    >
                      <Text style={[styles.dividendViewButtonText, dividendViewMode === 'annual' && styles.dividendViewButtonTextActive]}>
                        Annual
                      </Text>
                    </TouchableOpacity>
                    <TouchableOpacity
                      style={[styles.dividendViewButton, dividendViewMode === 'payments' && styles.dividendViewButtonActive]}
                      onPress={() => setDividendViewMode('payments')}
                    >
                      <Text style={[styles.dividendViewButtonText, dividendViewMode === 'payments' && styles.dividendViewButtonTextActive]}>
                        Payments
                      </Text>
                    </TouchableOpacity>
                  </View>

                  {/* ── Payments list (EX tile + amount + pay date) ── */}
                  {dividendViewMode === 'payments' ? (
                    dividendPayments && dividendPayments.length > 0 ? (
                      <>
                        {paymentItems.map(({ key, event: d }) => {
                          const exParts = parseExDateParts(d.ex_date);
                          const rowCurrency = resolveDividendCurrency(d.currency, dividendDisplayCurrency);
                          return (
                            <View key={key} style={styles.paymentRow}>
                              {/* EX calendar tile */}
                              <View style={styles.exCalendarTile}>
                                <Text style={styles.exCalLabel}>EX</Text>
                                <Text style={styles.exCalDay}>{exParts.day}</Text>
                                <Text style={styles.exCalMonth}>{exParts.month}</Text>
                              </View>
                              {/* Amount + pay-date */}
                              <View style={styles.paymentRowBody}>
                                <Text style={styles.paymentAmount}>{formatDividendAmount(d.amount, rowCurrency)}</Text>
                                <Text style={styles.paymentSubLabel}>
                                  Ex-date: {formatDividendEventDate(d.ex_date)}
                                </Text>
                                <Text style={styles.paymentSubLabel}>
                                  Payment date: {d.payment_date ? formatDividendEventDate(d.payment_date) : '—'} · {exParts.year}
                                </Text>
                              </View>
                            </View>
                          );
                        })}
                      </>
                    ) : (
                      <View style={styles.noDataPlaceholder}>
                        <Text style={styles.noDataText}>No dividend payments</Text>
                      </View>
                    )
                  ) : (
                    /* ── Annual list (no TTM) ── */
                    <>
                      {hasAnnualDividendData ? (
                        <>
                          {annualDividendPeriods.map((period) => {
                            const yoy = getAnnualYoyDisplay(period.total, period.previousTotal, period.isPartial === true);
                            return (
                              <View key={period.key} style={styles.earningsRow}>
                                <View style={styles.earningsLeft}>
                                  <View style={styles.earningsEpsRow}>
                                    <Text style={styles.earningsDate}>{period.label}</Text>
                                  </View>
                                  <Text style={styles.earningsEpsValue}>
                                    {formatDividendAmount(period.total, dividendDisplayCurrency)}
                                  </Text>
                                </View>
                                {period.isPartial ? (
                                  <TouchableOpacity
                                    style={[styles.beatMissBadge, styles.dividendYoYBadgeNeutralBase]}
                                    onPress={() => showTooltip('dividendsPartialYear')}
                                    accessibilityRole="button"
                                    accessibilityLabel="Show partial year help"
                                  >
                                    <Text style={[styles.beatMissText, styles.dividendYoYBadgeTextNeutral]}>Partial</Text>
                                    <Ionicons name="help-circle-outline" size={11} color="#6B7280" />
                                  </TouchableOpacity>
                                ) : (
                                  <TouchableOpacity
                                    style={[
                                      styles.beatMissBadge,
                                      yoy.tone === 'positive' ? styles.beatBadge
                                      : yoy.tone === 'negative' ? styles.missBadge
                                      : styles.dividendYoYBadgeNeutralBase,
                                    ]}
                                    onPress={() => showTooltip('dividendsYoY')}
                                    accessibilityRole="button"
                                    accessibilityLabel="Show year over year change help"
                                  >
                                    <Text style={[
                                      styles.beatMissText,
                                      yoy.tone === 'positive' ? styles.beatText
                                      : yoy.tone === 'negative' ? styles.missText
                                      : styles.dividendYoYBadgeTextNeutral,
                                    ]}>
                                      {yoy.label}
                                    </Text>
                                  </TouchableOpacity>
                                )}
                              </View>
                            );
                          })}
                        </>
                      ) : (
                        <View style={styles.noDataPlaceholder}>
                          <Text style={styles.noDataText}>Not enough dividend history for annual view</Text>
                        </View>
                      )}
                    </>
                  )}
                </>
              )}
            </>
          )}
        </View>

        {/* ===== SECTION 7: INSIDER TRANSACTIONS - Collapsible ===== */}
        <View 
          style={styles.sectionSurfaceCard} 
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

        {/* ===== SECTION 8: NEWS & EVENTS ===== */}
        <View 
          style={styles.perfCheckCard} 
          data-testid="news-events-section"
        >
          <View style={styles.newsSectionHeader}>
            <View style={[styles.sectionHeader, styles.newsSectionTitleWrap]}>
              <Text style={styles.sectionIcon}>📰</Text>
              <Text style={styles.sectionTitleBold}>News & Events</Text>
            </View>
            {aggregateSentiment && (
              <View
                style={[
                  styles.aggregateSentimentBadge,
                  { backgroundColor: `${aggregateSentiment.color}20` },
                ]}
              >
                <View style={[styles.aggregateSentimentDot, { backgroundColor: aggregateSentiment.color }]} />
                <Text style={[styles.aggregateSentimentText, { color: aggregateSentiment.color }]}>
                  {getSentimentText(aggregateSentiment.label)}
                </Text>
              </View>
            )}
          </View>

          {newsLoading && newsEventItems.length === 0 ? (
            <View style={styles.newsLoading}>
              <ActivityIndicator size="small" color={COLORS.primary} />
            </View>
          ) : newsEventItems.length === 0 ? (
            <View style={styles.newsEmpty}>
              <Ionicons name="newspaper-outline" size={32} color={COLORS.textMuted} />
              <Text style={styles.newsEmptyText}>No news or events available</Text>
              <Text style={styles.newsEmptySubtext}>Only ticker-specific items are shown here.</Text>
            </View>
          ) : (
            <>
              {(() => {
                const lastVisibleIndex = Math.min(newsVisibleCount, newsEventItems.length) - 1;
                return newsEventItems.slice(0, newsVisibleCount).map((item, index) => {
                  const isLastVisible = index === lastVisibleIndex;
                  if (item.kind === 'event') {
                    const eventColor = item.eventType === 'Earnings'
                      ? COLORS.primary
                      : item.eventType === 'Dividend'
                        ? COLORS.accent
                        : '#8B5CF6';
                    const eventText = formatEventMessage(item.title, item.subtitle);
                    return (
                      <View
                        key={item.id}
                        style={[styles.newsRow, isLastVisible && styles.lastNewsRow]}
                      >
                        <NewsLogo
                          logoUrl={logoUrl ?? undefined}
                          fallbackKey={getNewsFallbackKey(null, ticker)}
                        />
                        <View style={styles.newsContent}>
                          <View style={styles.newsTickerRow}>
                            <Text style={styles.newsTickerText}>{ticker.toUpperCase()}</Text>
                            <View style={[styles.eventPill, { backgroundColor: `${eventColor}15` }]}>
                              <Text style={[styles.eventPillText, { color: eventColor }]}>{item.eventType}</Text>
                            </View>
                            <View style={styles.newsTickerSpacer} />
                            <Text style={styles.newsMeta}>{formatDateDMY(item.date)}</Text>
                          </View>
                          <Text style={styles.newsTitle} numberOfLines={3}>{eventText}</Text>
                        </View>
                      </View>
                    );
                  }

                const article = item.article;
                const articleTone = getSentimentTone(article.sentiment_label);
                return (
                  <TouchableOpacity
                    key={item.id}
                    style={[styles.newsRow, isLastVisible && styles.lastNewsRow]}
                    onPress={() => openArticle(article)}
                  >
                    <NewsLogo
                      logoUrl={resolveNewsLogoUrl(article.logo_url)}
                      fallbackKey={getNewsFallbackKey(article.fallback_logo_key, article.ticker || ticker)}
                    />
                    <View style={styles.newsContent}>
                      <View style={styles.newsTickerRow}>
                        <Text style={styles.newsTickerText}>{(article.ticker || ticker).toUpperCase()}</Text>
                        {article.sentiment_label && (
                          <View style={[
                            styles.sentimentBadgeSmall,
                            { backgroundColor: articleTone.backgroundColor },
                          ]}>
                            <Text style={[
                              styles.sentimentTextSmall,
                              { color: articleTone.textColor },
                            ]}>
                              {getSentimentText(article.sentiment_label)}
                            </Text>
                          </View>
                        )}
                        <View style={styles.newsTickerSpacer} />
                        {article.date ? (
                          <Text style={styles.newsMeta}>{formatNewsDate(article.date)}</Text>
                        ) : null}
                      </View>
                      <Text style={styles.newsTitle} numberOfLines={2}>{article.title}</Text>
                    </View>
                    <Ionicons name="chevron-forward" size={18} color={COLORS.textMuted} />
                  </TouchableOpacity>
                );
              });
              })()}

              {(newsVisibleCount < newsEventItems.length || newsHasMore || newsVisibleCount > INITIAL_NEWS_EVENTS_LIMIT) && (
                <View style={styles.newsActionsRow}>
                  {(newsVisibleCount < newsEventItems.length || newsHasMore) && (
                    <TouchableOpacity
                      style={styles.newsActionButton}
                      onPress={() => {
                        if (shouldFetchMoreNews) {
                          fetchMoreNews();
                        }
                        setNewsVisibleCount((prev) => prev + NEWS_EVENTS_PAGE_SIZE);
                      }}
                      disabled={newsLoading}
                    >
                      {newsLoading ? (
                        <ActivityIndicator size="small" color={COLORS.primary} />
                      ) : (
                        <Text style={styles.newsActionText}>Load more news</Text>
                      )}
                    </TouchableOpacity>
                  )}
                  {newsVisibleCount > INITIAL_NEWS_EVENTS_LIMIT && (
                    <TouchableOpacity
                      style={[styles.newsActionButton, styles.newsActionButtonSecondary]}
                      onPress={() => setNewsVisibleCount(INITIAL_NEWS_EVENTS_LIMIT)}
                    >
                      <Text style={[styles.newsActionText, styles.newsActionTextSecondary]}>See less</Text>
                    </TouchableOpacity>
                  )}
                </View>
              )}
            </>
          )}
        </View>

        {/* ===== SECTION 9: CALCULATORS ===== */}
        <View style={styles.sectionSurfaceCard} data-testid="calculator-section">
          <View style={styles.sectionHeader}>
            <Ionicons
              name="calculator-outline"
              size={18}
              color={COLORS.textMuted}
              accessibilityElementsHidden
              importantForAccessibility="no"
            />
            <Text style={styles.sectionTitleBold}>Calculators</Text>
          </View>
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
         
        {/* Bottom padding for navigation */}
        <View style={{ height: 84 }} />
      </ScrollView>

      <Modal
        visible={!!selectedArticle}
        animationType="slide"
        presentationStyle="pageSheet"
        onRequestClose={closeArticle}
      >
        <SafeAreaView style={styles.articleModalContainer}>
          <View style={styles.articleModalHeader}>
            <TouchableOpacity onPress={closeArticle} style={styles.articleModalCloseButton}>
              <Ionicons name="close" size={28} color={COLORS.text} />
            </TouchableOpacity>
            <Text style={styles.articleModalHeaderTitle}>Article</Text>
            <TouchableOpacity
              onPress={() => selectedArticle?.link && openExternalLink(selectedArticle.link)}
              style={styles.articleModalExternalButton}
              disabled={!selectedArticle?.link}
            >
              <Ionicons name="open-outline" size={22} color={selectedArticle?.link ? COLORS.primary : COLORS.textMuted} />
            </TouchableOpacity>
          </View>

          <ScrollView style={styles.articleModalScroll} contentContainerStyle={styles.articleModalScrollContent}>
            {selectedArticle && (
              <>
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
                    <NewsLogo
                      logoUrl={resolveNewsLogoUrl(selectedArticle.logo_url)}
                      fallbackKey={getNewsFallbackKey(selectedArticle.fallback_logo_key, selectedArticle.ticker || ticker)}
                    />
                    <View>
                      <Text style={styles.articleTicker}>{(selectedArticle.ticker || ticker).toUpperCase()}</Text>
                      <Text style={styles.articleCompany}>{selectedArticle.company_name || company.name}</Text>
                    </View>
                  </TouchableOpacity>
                  {selectedArticle.date ? (
                    <Text style={styles.articleMeta}>{formatNewsDate(selectedArticle.date)}</Text>
                  ) : null}
                </View>

                <Text style={styles.articleTitle}>{selectedArticle.title}</Text>

                {selectedArticle.sentiment && (
                  <View style={styles.articleSentimentRow}>
                    <View style={[
                      styles.articleSentimentBadge,
                      { backgroundColor: selectedArticleSentimentTone.backgroundColor },
                    ]}>
                      <Text style={[
                        styles.articleSentimentText,
                        { color: selectedArticleSentimentTone.textColor },
                      ]}>
                        {getSentimentText(selectedArticleSentimentLabel)} Sentiment
                      </Text>
                    </View>
                  </View>
                )}

                {selectedArticle.tags && selectedArticle.tags.length > 0 && (
                  <View style={styles.articleTagsRow}>
                    {selectedArticle.tags.slice(0, 5).map((tag, index) => (
                      <View key={index} style={styles.articleTagBadge}>
                        <Text style={styles.articleTagText}>{tag}</Text>
                      </View>
                    ))}
                  </View>
                )}

                <Text
                  style={styles.articleContent}
                  accessibilityLabel={selectedArticle.content?.trim()
                    ? 'Article content'
                    : 'Article preview unavailable. Open the original article to read the full story'}
                >
                  {selectedArticle.content?.trim() || 'Open the original article to read the full story'}
                </Text>

                {selectedArticle.link && (
                  <TouchableOpacity
                    style={styles.readOriginalButton}
                    onPress={() => openExternalLink(selectedArticle.link)}
                  >
                    <Ionicons name="open-outline" size={18} color={COLORS.primary} />
                    <Text style={styles.readOriginalText}>Read original article</Text>
                  </TouchableOpacity>
                )}
              </>
            )}
          </ScrollView>
        </SafeAreaView>
      </Modal>

      <Modal
        visible={addToVisible}
        transparent
        animationType="slide"
        onRequestClose={() => setAddToVisible(false)}
      >
        <Pressable style={styles.periodSelectorOverlay} onPress={() => setAddToVisible(false)}>
          <Pressable style={styles.addToSheet} onPress={(e) => e.stopPropagation()}>
            <View style={styles.periodSelectorHandle} />
            <View style={styles.addToSheetHeader}>
              <Text style={styles.addToSheetTitle}>Add to</Text>
              <TouchableOpacity onPress={() => setAddToVisible(false)}>
                <Ionicons name="close" size={22} color={COLORS.textMuted} />
              </TouchableOpacity>
            </View>

            <TouchableOpacity
              style={[
                styles.addToSheetItem,
                listMemberships.tracklist && styles.addToSheetItemDisabled,
                listMemberships.watchlist && styles.addToSheetItemActive,
              ]}
              onPress={() => handleAddTo('watchlist')}
              disabled={listActionLoading || listMemberships.watchlist || listMemberships.tracklist}
            >
              <View style={[styles.addToSheetIcon, styles.addToSheetIconWatchlist]}>
                <Ionicons name="eye-outline" size={18} color="#B45309" />
              </View>
              <View style={styles.addToSheetTextWrap}>
                <Text style={styles.addToSheetItemTitle}>Watchlist</Text>
                <Text style={styles.addToSheetItemText}>
                  {listMemberships.watchlist
                    ? 'Already in your Watchlist.'
                    : listMemberships.tracklist
                    ? 'Unavailable while this stock is in your Tracklist.'
                    : 'Starts tracking from the next close and appears in My Stocks.'}
                </Text>
              </View>
              {listMemberships.watchlist ? <Ionicons name="checkmark" size={20} color="#B45309" /> : null}
            </TouchableOpacity>

            <TouchableOpacity
              style={[
                styles.addToSheetItem,
                listMemberships.watchlist && styles.addToSheetItemDisabled,
                listMemberships.tracklist && styles.addToSheetItemActiveTracklist,
              ]}
              onPress={() => handleAddTo('tracklist')}
              disabled={listMemberships.watchlist || listMemberships.tracklist}
            >
              <View style={[styles.addToSheetIcon, styles.addToSheetIconTracklist]}>
                <Ionicons name="analytics-outline" size={18} color="#1D4ED8" />
              </View>
              <View style={styles.addToSheetTextWrap}>
                <Text style={styles.addToSheetItemTitle}>Tracklist</Text>
                <Text style={styles.addToSheetItemText}>
                  {listMemberships.tracklist
                    ? 'Already managed in your Tracklist.'
                    : listMemberships.watchlist
                      ? 'Unavailable while this stock is in your Watchlist.'
                      : 'Opens your Tracklist overview where you can replace one current name.'}
                </Text>
              </View>
              {listMemberships.tracklist ? <Ionicons name="checkmark" size={20} color="#1D4ED8" /> : null}
            </TouchableOpacity>

            <View style={[styles.addToSheetItem, styles.addToSheetItemDisabled]}>
              <View style={[styles.addToSheetIcon, styles.addToSheetIconPortfolio]}>
                <Ionicons name="briefcase-outline" size={18} color="#065F46" />
              </View>
              <View style={styles.addToSheetTextWrap}>
                <Text style={styles.addToSheetItemTitle}>Portfolio</Text>
                <Text style={styles.addToSheetItemText}>Soon.</Text>
              </View>
              <View style={styles.addToSoonBadge}>
                <Text style={styles.addToSoonBadgeText}>Soon</Text>
              </View>
            </View>
          </Pressable>
        </Pressable>
      </Modal>
       
      {/* P1 UX: Native BottomSheet Tooltip */}
      <MetricTooltip 
        visible={tooltipVisible} 
        onClose={() => setTooltipVisible(false)} 
        content={TOOLTIP_CONTENT[activeTooltip]} 
      />
      
      {/* Performance Check period selector */}
      <Modal
        visible={perfCheckPeriodVisible}
        transparent
        animationType="slide"
        onRequestClose={() => setPerfCheckPeriodVisible(false)}
      >
        <Pressable style={styles.periodSelectorOverlay} onPress={() => setPerfCheckPeriodVisible(false)}>
          <Pressable style={styles.periodSelectorSheet} onPress={(e) => e.stopPropagation()}>
            <View style={styles.periodSelectorHandle} />
            <Text style={styles.periodSelectorTitle}>Performance period</Text>
            {PERFORMANCE_PERIODS.map((r) => (
              <TouchableOpacity
                key={r}
                style={styles.periodSelectorOption}
                onPress={() => {
                  setPriceRange(r);
                  setPerfCheckPeriodVisible(false);
                }}
                accessibilityRole="button"
              >
                <Text style={[styles.periodSelectorOptionText, priceRange === r && styles.periodSelectorOptionTextActive]}>
                  {r === 'MAX' ? 'Full History' : `Past ${r}`}
                </Text>
                {priceRange === r && <Ionicons name="checkmark" size={18} color={COLORS.primary} />}
              </TouchableOpacity>
            ))}
          </Pressable>
        </Pressable>
      </Modal>

      {/* Price History range selector */}
      <Modal
        visible={priceRangeSelectorVisible}
        transparent
        animationType="slide"
        onRequestClose={() => setPriceRangeSelectorVisible(false)}
      >
        <Pressable style={styles.periodSelectorOverlay} onPress={() => setPriceRangeSelectorVisible(false)}>
          <Pressable style={styles.periodSelectorSheet} onPress={(e) => e.stopPropagation()}>
            <View style={styles.periodSelectorHandle} />
            <Text style={styles.periodSelectorTitle}>Price history period</Text>
            {PERFORMANCE_PERIODS.map((r) => (
              <TouchableOpacity
                key={r}
                style={styles.periodSelectorOption}
                onPress={() => {
                  setPriceRange(r);
                  setPriceRangeSelectorVisible(false);
                }}
                accessibilityRole="button"
              >
                <Text style={[styles.periodSelectorOptionText, priceRange === r && styles.periodSelectorOptionTextActive]}>
                  {r === 'MAX' ? 'Full History' : `Past ${r}`}
                </Text>
                {priceRange === r && <Ionicons name="checkmark" size={18} color={COLORS.primary} />}
              </TouchableOpacity>
            ))}
          </Pressable>
        </Pressable>
      </Modal>
      
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
  sectionSurfaceCard: {
    backgroundColor: COLORS.card,
    borderRadius: 12,
    padding: 14,
    marginBottom: 16,
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  // Price History chart card with white background and rounded corners
  priceChartCard: {
    backgroundColor: COLORS.card,
    borderRadius: 12,
    padding: 14,
    marginBottom: 16,
    borderWidth: 1,
    borderColor: COLORS.border,
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
  dataNoticeBanner: { flexDirection: 'row', alignItems: 'flex-start', gap: 6, backgroundColor: '#FEF9C3', borderRadius: 8, padding: 8, marginTop: 6, marginBottom: 2, borderWidth: 1, borderColor: '#FDE68A' },
  dataNoticeText: { fontSize: 11, color: '#92400E', lineHeight: 15 },
  
  // ============================================================================
  // NEW: Compact Header Row (replaces big identity card)
  // ============================================================================
  compactHeader: { 
    flexDirection: 'row', 
    alignItems: 'flex-start', 
    gap: 12, 
    marginBottom: 12,
  },
  compactLogo: { 
    width: 38, 
    height: 38, 
    borderRadius: 8, 
    backgroundColor: '#F5F8FC',
  },
  compactLogoPlaceholder: { 
    width: 38, 
    height: 38, 
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
    fontSize: 20,
    fontWeight: '700',
    color: '#111827',
    lineHeight: 30,
    flexShrink: 1,
  },
  compactNameRow: {
    flexDirection: 'row',
    alignItems: 'center',
    flexWrap: 'nowrap',
    flex: 1,
    minWidth: 0,
    gap: 8,
    marginBottom: 6,
  },
  classificationRow: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 6,
    marginTop: 2,
  },
  exchangePill: {
    backgroundColor: '#E5E7EB',
    paddingHorizontal: 10,
    paddingVertical: 5,
    borderRadius: 999,
    borderWidth: 1,
    borderColor: '#D1D5DB',
    flexShrink: 0,
  },
  exchangePillText: {
    fontSize: 14,
    color: '#111827',
    fontWeight: '700',
  },
  companyMetaPill: {
    borderRadius: 10,
    borderWidth: 1,
    borderColor: '#E5E7EB',
    backgroundColor: '#F9FAFB',
    paddingVertical: 5,
    paddingHorizontal: 8,
  },
  companyMetaPillLabel: {
    fontSize: 12,
    color: '#1F2937',
    fontWeight: '500',
  },
  companyMetaPillValue: {
    fontSize: 14,
    color: '#111827',
    fontWeight: '700',
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
    fontSize: 14,
    fontWeight: '700',
  },
  safetyBadgeTextAmber: {
    color: '#92400E',
  },
  safetyBadgeTextBlue: {
    color: '#1E40AF',
  },
  safetyTooltip: {
    fontSize: 14,
    color: COLORS.textMuted,
    fontStyle: 'italic',
    marginTop: 4,
    lineHeight: 20,
  },
  safetyTooltipInline: {
    fontSize: 14,
    color: COLORS.textMuted,
    fontStyle: 'italic',
    flex: 1,
    lineHeight: 20,
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
  lastCloseHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 10 },
  lastCloseMeta: { gap: 4 },
  lastCloseTitleRow: { flexDirection: 'row', alignItems: 'center', gap: 6 },
  lastCloseTitle: { fontSize: 16, fontWeight: '700', color: COLORS.text },
  addToButton: { flexDirection: 'row', alignItems: 'center', gap: 6, paddingHorizontal: 12, paddingVertical: 8, borderRadius: 999, backgroundColor: '#F3F4F6' },
  addToButtonActive: { backgroundColor: '#E0E7FF' },
  addToButtonActiveWatchlist: { backgroundColor: '#FEF3C7' },
  addToButtonActiveTracklist: { backgroundColor: '#DBEAFE' },
  addToButtonText: { fontSize: 13, fontWeight: '700', color: COLORS.text },
  priceRow: { flexDirection: 'row', alignItems: 'center' },
  priceValueRow: { flexDirection: 'row', alignItems: 'center', gap: 10, flexWrap: 'wrap' },
  priceValue: { fontSize: 28, fontWeight: '700', color: COLORS.text },  // Slightly smaller
  changeChip: { flexDirection: 'row', alignItems: 'center', paddingHorizontal: 8, paddingVertical: 4, borderRadius: 6, gap: 4 },
  positiveChip: { backgroundColor: '#D1FAE5' },
  negativeChip: { backgroundColor: '#FEE2E2' },
  changeText: { fontSize: 13, fontWeight: '600' },
  positiveText: { color: '#10B981' },
  negativeText: { color: '#EF4444' },
  priceDate: { fontSize: 14, color: '#374151', lineHeight: 20 },
  
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
  dateRangeText: { fontSize: 14, color: '#374151', lineHeight: 20, marginBottom: 8 },
  benchmarkNote: { fontSize: 10, color: '#9CA3AF', textAlign: 'center', marginTop: -4, marginBottom: 8, fontStyle: 'italic' },
  rangeButton: { paddingVertical: 8, paddingHorizontal: 12, borderRadius: 6, backgroundColor: '#F5F8FC', alignItems: 'center', minWidth: 44 },
  rangeButtonActive: { backgroundColor: COLORS.primary },
  rangeButtonText: { fontSize: 12, fontWeight: '500', color: COLORS.textMuted },
  rangeButtonTextActive: { color: '#FFF' },
  // No overflow:'hidden' — Y-axis price badges extend beyond SVG bounds
  chartContainer: { minHeight: 260 },
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
  chartLegend: { flexDirection: 'row', justifyContent: 'center', alignItems: 'center', gap: 16, marginTop: 12, paddingVertical: 10, borderTopWidth: 1, borderTopColor: '#E5E7EB', flexWrap: 'wrap' },
  legendLabel: { fontSize: 12, fontWeight: '600' },
  benchmarkToggle: { paddingHorizontal: 10, paddingVertical: 4, borderRadius: 12, borderWidth: 1, borderStyle: 'dashed', borderColor: '#D1D5DB', backgroundColor: '#F9FAFB' },
  benchmarkToggleActive: { backgroundColor: '#EEF2FF', borderColor: '#6366F1', borderStyle: 'solid' },
  benchmarkToggleRow: { flexDirection: 'row', alignItems: 'center', gap: 4 },
  benchmarkToggleDot: { width: 8, height: 8, borderRadius: 4, backgroundColor: '#9CA3AF', opacity: 0.6 },
  benchmarkToggleDismiss: { fontSize: 11, color: '#6366F1', marginLeft: 2 },
  benchmarkToggleText: { fontSize: 11, fontWeight: '600', color: '#9CA3AF' },
  benchmarkToggleTextActive: { color: '#6366F1' },
  customXAxis: { flexDirection: 'row', justifyContent: 'space-between', paddingHorizontal: 10, marginTop: 4 },
  customXAxisLabel: { fontSize: 10, color: COLORS.textMuted, textAlign: 'center', minWidth: 40 },
  
  // Description
  descriptionCard: { backgroundColor: COLORS.card, borderRadius: 16, padding: 16, marginBottom: 12 },
  sectionTitle: { fontSize: 15, fontWeight: '600', color: COLORS.text, marginBottom: 10 },
  subsectionTitle: { fontSize: 15, fontWeight: '700', color: COLORS.text, marginBottom: 8 },
  dividendsSubsectionTitle: { fontSize: 15, fontWeight: '700', color: '#111827' },
  dividendsSubsectionTitleRow: { flexDirection: 'row', alignItems: 'center', gap: 6, marginTop: 16, marginBottom: 10 },
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
  dividendMetaRow: { flexDirection: 'row', flexWrap: 'wrap', gap: 8, marginBottom: 10 },
  dividendMetaPill: { borderRadius: 10, borderWidth: 1, borderColor: '#D1D5DB', backgroundColor: '#FFFFFF', paddingVertical: 6, paddingHorizontal: 10 },
  dividendMetaPillAccent: { backgroundColor: '#F3F4F6' },
  dividendMetaPillLabel: { fontSize: 14, color: '#4B5563', fontWeight: '700' },
  dividendMetaPillValue: { fontSize: 14, color: '#111827', fontWeight: '800' },
  nextDividendCard: { borderWidth: 1, borderColor: '#D1D5DB', borderRadius: 12, backgroundColor: '#FFFFFF', padding: 12, marginBottom: 10 },
  nextDividendHeader: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 },
  nextDividendTitle: { fontSize: 18, fontWeight: '800', color: '#111827' },
  nextDividendGrid: { gap: 8 },
  nextDividendMetric: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center' },
  nextDividendMetricLabel: { fontSize: 14, color: '#374151', fontWeight: '700' },
  nextDividendMetricValue: { fontSize: 15, fontWeight: '800', color: '#111827' },
  nextDividendEmptyText: { fontSize: 14, color: '#4B5563', lineHeight: 20 },
  dividendEventTag: { borderWidth: 1, borderColor: '#FECACA', backgroundColor: '#FEF2F2', paddingHorizontal: 8, paddingVertical: 2, borderRadius: 999 },
  dividendEventTagText: { fontSize: 11, fontWeight: '700', color: '#7F1D1D' },
  dividendsList: { marginTop: 4, gap: 10 },
  dividendPaymentItem: { borderRadius: 12, backgroundColor: '#F9FAFB', paddingVertical: 12, paddingHorizontal: 14 },
  dividendPaymentTopRow: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 },
  dividendAmount: { fontSize: 26, fontWeight: '900', color: '#111827' },
  dividendDateDetail: { fontSize: 14, color: '#374151', marginTop: 2, fontWeight: '600' },
  dividendGrowthText: { fontSize: 14, marginTop: 6, fontWeight: '800' },
  dividendViewSwitch: { flexDirection: 'row', backgroundColor: '#E5E7EB', borderRadius: 10, padding: 4, marginTop: 6, marginBottom: 10 },
  dividendViewButton: { flex: 1, paddingHorizontal: 16, paddingVertical: 8, borderRadius: 8, alignItems: 'center' },
  dividendViewButtonActive: { backgroundColor: '#FFFFFF' },
  dividendViewButtonText: { fontSize: 15, color: '#4B5563', fontWeight: '700' },
  dividendViewButtonTextActive: { color: '#111827' },
  dividendAnnualSection: { marginTop: 4 },
  dividendAnnualList: { gap: 10, marginTop: 2 },
  dividendAnnualItem: { flexDirection: 'row', alignItems: 'center', borderRadius: 12, backgroundColor: '#F9FAFB', paddingVertical: 12, paddingHorizontal: 14, gap: 10 },
  dividendTrendBar: { width: 4, borderRadius: 3, alignSelf: 'stretch' },
  dividendAnnualItemBody: { flex: 1 },
  dividendAnnualLabelRow: { flexDirection: 'row', alignItems: 'center', gap: 4, marginBottom: 2 },
  dividendAnnualPeriodLabel: { fontSize: 26, fontWeight: '900', color: '#111827' },
  dividendAnnualSecondaryValue: { fontSize: 14, color: '#374151', fontWeight: '600' },
  dividendAnnualHelperText: { fontSize: 14, color: '#374151', fontWeight: '700' },
  dividendPartialHelperText: { color: '#6B7280' },
  dividendAnnualPrimaryValue: { fontSize: 24, fontWeight: '900', color: '#111827' },
  // YoY badge (pill) for annual dividend rows
  dividendYoYBadge: { flexDirection: 'row', alignItems: 'center', paddingHorizontal: 8, paddingVertical: 5, borderRadius: 6, gap: 3, minWidth: 62, justifyContent: 'center' },
  dividendYoYBadgePositive: { backgroundColor: '#D1FAE5' },
  dividendYoYBadgeNegative: { backgroundColor: '#FEE2E2' },
  dividendYoYBadgeNeutralBase: { backgroundColor: '#F3F4F6' },
  dividendYoYBadgeText: { fontSize: 14, fontWeight: '700' },
  dividendYoYBadgeTextPositive: { color: '#10B981' },
  dividendYoYBadgeTextNegative: { color: '#EF4444' },
  dividendYoYBadgeTextNeutral: { color: '#6B7280' },
  dividendYoYBadgeNeutral: { flexDirection: 'row', alignItems: 'center', gap: 3, backgroundColor: '#F3F4F6', paddingHorizontal: 8, paddingVertical: 5, borderRadius: 6 },
  dividendYoYBadgeNeutralText: { fontSize: 14, fontWeight: '700', color: '#6B7280' },
  nextDividendTitleRow: { flexDirection: 'row', alignItems: 'center', gap: 6 },
  dividendValuePositive: { color: '#10B981' },
  dividendValueNegative: { color: '#EF4444' },
  dividendValueNeutral: { color: '#111827' },
  nextDividendNeutralText: { fontSize: 14, color: '#6B7280', lineHeight: 20, marginBottom: 8 },

  // Earnings | Dividends main tab bar (full-width segmented control)
  earningsDivTabRow: { flexDirection: 'row', backgroundColor: '#E5E7EB', borderRadius: 10, padding: 4, marginBottom: 14 },
  earningsDivTab: { flex: 1, paddingVertical: 10, borderRadius: 8, alignItems: 'center' },
  earningsDivTabActive: { backgroundColor: '#FFFFFF' },
  earningsDivTabText: { fontSize: 15, fontWeight: '600', color: '#4B5563', textAlign: 'center' },
  earningsDivTabTextActive: { fontSize: 15, fontWeight: '700', color: '#111827', textAlign: 'center' },

  // Next Earnings header tile (wider pill)
  earningsNextTilePill: { flexGrow: 1, flexShrink: 1, flexBasis: 0, minWidth: 220, alignSelf: 'stretch' },
  dividendNextTilePill: { flexGrow: 1, flexShrink: 1, flexBasis: 0, minWidth: 140, alignSelf: 'stretch' },
  earningsNextTileHeader: { flexDirection: 'row', alignItems: 'center', gap: 4, marginBottom: 6 },
  earningsNextTileLabel: { fontSize: 11, fontWeight: '700', color: '#6B7280', letterSpacing: 0.5 },
  earningsNextTileBody: { flexDirection: 'row', alignItems: 'flex-start', justifyContent: 'space-between', gap: 8 },
  earningsNextTilePrimary: { fontSize: 16, fontWeight: '800', color: '#111827', marginBottom: 2 },
  earningsNextTileSecondary: { fontSize: 13, color: '#6B7280', fontWeight: '600' },
  earningsNextTileDate: { fontSize: 13, color: '#6B7280', fontWeight: '500' },
  earningsCountdownBadge: { flexDirection: 'row', alignItems: 'center', gap: 2, backgroundColor: '#F3F4F6', paddingHorizontal: 6, paddingVertical: 2, borderRadius: 10 },
  earningsCountdownText: { fontSize: 12, fontWeight: '700', color: '#6B7280' },

  // Dividend Payments row — EX calendar tile
  paymentRow: { flexDirection: 'row', alignItems: 'center', paddingVertical: 12, borderBottomWidth: 1, borderBottomColor: '#F3F4F6', gap: 12 },
  exCalendarTile: { width: 52, borderRadius: 8, borderWidth: 1, borderColor: '#D1D5DB', backgroundColor: '#F9FAFB', alignItems: 'center', paddingVertical: 6 },
  exCalLabel: { fontSize: 10, fontWeight: '700', color: '#6B7280', letterSpacing: 0.5 },
  exCalDay: { fontSize: 20, fontWeight: '900', color: '#111827', lineHeight: 24 },
  exCalMonth: { fontSize: 11, fontWeight: '700', color: '#6B7280' },
  paymentRowBody: { flex: 1 },
  paymentAmount: { fontSize: 18, fontWeight: '800', color: '#111827', marginBottom: 2 },
  paymentSubLabel: { fontSize: 13, color: '#6B7280', fontWeight: '500' },

  // P4: Tab styles removed - Single vertical scroll, no tabs
  
  // Metrics Card
  metricsCard: { backgroundColor: COLORS.card, borderRadius: 16, padding: 16, marginBottom: 12 },
  metricRow: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', paddingVertical: 10, borderBottomWidth: 1, borderBottomColor: COLORS.border },
  metricLabel: { fontSize: 14, color: COLORS.textMuted },
  // P1 UX: Metric label row with info icon
  metricLabelRow: { flexDirection: 'row', alignItems: 'center', gap: 6, flex: 1 },
  metricValue: { fontSize: 14, fontWeight: '600', color: COLORS.text },
  metricValueRow: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  metricValueCol: { alignItems: 'flex-end' },
  peerMedianHint: { fontSize: 11, color: COLORS.textMuted, marginTop: 2 },
  benchmarkPill: { paddingHorizontal: 6, paddingVertical: 2, borderRadius: 4, marginLeft: 6 },
  benchmarkPillText: { fontSize: 10, fontWeight: '600' },
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
  earningsRow: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', paddingVertical: 12, borderBottomWidth: 1, borderBottomColor: '#F3F4F6' },
  earningsLeft: { flex: 1, marginRight: 8 },
  earningsDate: { fontSize: 14, color: COLORS.textMuted, marginBottom: 3 },
  earningsAnnualPrimaryValue: { fontSize: 18, fontWeight: '800', color: '#111827', marginBottom: 2 },
  earningsAnnualSummary: { fontSize: 13, color: EARNINGS_NEUTRAL_COLOR, fontWeight: '500', lineHeight: 18 },
  earningsPartialBadge: { backgroundColor: '#F3F4F6', paddingHorizontal: 6, paddingVertical: 2, borderRadius: 999 },
  earningsPartialText: { fontSize: 11, fontWeight: '700', color: EARNINGS_NEUTRAL_COLOR },
  earningsAnnualBarButton: { flexDirection: 'row', alignItems: 'center', gap: 6, marginTop: 6, maxWidth: 180 },
  earningsAnnualBarTrack: { flex: 1, flexDirection: 'row', height: 8, backgroundColor: '#F3F4F6', borderRadius: 999, overflow: 'hidden' },
  earningsAnnualBarSegment: { height: '100%' },
  earningsAnnualBarSegmentBeat: { backgroundColor: '#10B981' },
  earningsAnnualBarSegmentMiss: { backgroundColor: '#EF4444' },
  earningsAnnualBarSegmentOther: { backgroundColor: EARNINGS_NEUTRAL_COLOR },
  earningsEpsRow: { flexDirection: 'row', alignItems: 'center', gap: 4 },
  earningsEpsLabel: { fontSize: 14, fontWeight: '600', color: COLORS.textMuted, letterSpacing: 0.3 },
  earningsEpsValue: { fontSize: 14, fontWeight: '600', color: '#374151' },
  earningsEpsSep: { fontSize: 14, color: COLORS.textMuted },
  earningsNAText: { fontSize: 14, fontWeight: '600', color: COLORS.textMuted },
  earningsData: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  earningsValue: { fontSize: 15, fontWeight: '700', color: '#111827' },
  beatMissBadge: { flexDirection: 'row', alignItems: 'center', paddingHorizontal: 8, paddingVertical: 5, borderRadius: 6, gap: 3, minWidth: 62 },
  beatBadge: { backgroundColor: '#D1FAE5' },
  missBadge: { backgroundColor: '#FEE2E2' },
  beatMissText: { fontSize: 14, fontWeight: '700' },
  beatText: { color: '#10B981' },
  missText: { color: '#EF4444' },
  subsectionTitleRow: { flexDirection: 'row', alignItems: 'center', gap: 6, marginBottom: 8 },
  
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
  calculatorButtons: { flexDirection: 'row', gap: 12 },
  calcButton: { flex: 1, flexDirection: 'row', alignItems: 'center', justifyContent: 'center', backgroundColor: COLORS.card, borderRadius: 12, paddingVertical: 14, gap: 8, borderWidth: 1, borderColor: COLORS.border },
  calcButtonText: { fontSize: 14, fontWeight: '600', color: COLORS.primary },
  
  // News & Events
  newsLoading: { padding: 24, alignItems: 'center' },
  newsSectionHeader: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', gap: 12, marginBottom: 10 },
  newsSectionTitleWrap: { marginBottom: 0, flex: 1 },
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
  newsEmpty: { padding: 24, alignItems: 'center' },
  newsEmptyText: { fontSize: 15, fontWeight: '600', color: COLORS.text, marginTop: 8 },
  newsEmptySubtext: { fontSize: 13, color: COLORS.textMuted, textAlign: 'center', marginTop: 4 },
  newsRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 12,
    paddingVertical: 12,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  lastNewsRow: { borderBottomWidth: 0, paddingBottom: 4 },
  newsContent: { flex: 1, gap: 4 },
  newsTickerRow: { flexDirection: 'row', alignItems: 'center', gap: 8, marginBottom: 4 },
  newsTickerSpacer: { flex: 1 },
  newsTickerText: { fontSize: 12, fontWeight: '700', color: COLORS.primary },
  sentimentBadgeSmall: { paddingHorizontal: 6, paddingVertical: 2, borderRadius: 4 },
  sentimentTextSmall: { fontSize: 10, fontWeight: '700' },
  newsTitle: { fontSize: 14, fontWeight: '500', lineHeight: 20, color: COLORS.text },
  newsMeta: { fontSize: 11, color: COLORS.textMuted, marginLeft: 'auto' },
  newsSubmeta: { fontSize: 12, color: COLORS.textMuted, lineHeight: 18 },
  eventPill: { paddingHorizontal: 8, paddingVertical: 4, borderRadius: 999 },
  eventPillText: { fontSize: 11, fontWeight: '700' },
  newsActionsRow: { flexDirection: 'row', gap: 10, paddingTop: 12 },
  newsActionButton: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    paddingVertical: 14,
    borderRadius: 10,
    borderWidth: 1,
    borderColor: `${COLORS.primary}30`,
    backgroundColor: `${COLORS.primary}10`,
  },
  newsActionButtonSecondary: {
    borderColor: COLORS.border,
    backgroundColor: COLORS.card,
  },
  newsActionText: { fontSize: 14, fontWeight: '600', color: COLORS.primary },
  newsActionTextSecondary: { color: COLORS.textLight },
  articleModalContainer: {
    flex: 1,
    backgroundColor: COLORS.background,
  },
  articleModalHeader: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    paddingHorizontal: 16,
    paddingVertical: 12,
    backgroundColor: COLORS.card,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  articleModalCloseButton: {
    padding: 12,
    marginLeft: -8,
  },
  articleModalHeaderTitle: {
    fontSize: 17,
    fontWeight: '600',
    color: COLORS.text,
  },
  articleModalExternalButton: {
    padding: 4,
  },
  articleModalScroll: {
    flex: 1,
  },
  articleModalScrollContent: {
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
  articleSentimentRow: {
    flexDirection: 'row',
    marginBottom: 12,
  },
  articleSentimentBadge: {
    paddingHorizontal: 12,
    paddingVertical: 6,
    borderRadius: 16,
  },
  articleSentimentText: {
    fontSize: 12,
    fontWeight: '600',
  },
  articleTagsRow: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 8,
    marginBottom: 16,
  },
  articleTagBadge: {
    backgroundColor: COLORS.background,
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 12,
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  articleTagText: {
    fontSize: 11,
    color: '#6B7280',
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
  // ============================================================================
  // Performance Check Card (redesigned)
  // ============================================================================
  perfCheckCard: {
    backgroundColor: COLORS.card,
    borderRadius: 12,
    padding: 14,
    marginBottom: 16,
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  perfCheckHeaderRow: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    marginBottom: 4,
  },
  perfCheckTitleRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
  },
  perfCheckPeriodBadge: {
    fontSize: 12,
    fontWeight: '600',
    color: COLORS.primary,
  },
  perfCheckDateRange: {
    fontSize: 14,
    color: '#374151',
    lineHeight: 20,
    marginBottom: 12,
  },
  perfCheckColumns: {
    flexDirection: 'column',
    gap: 10,
    marginBottom: 12,
  },
  perfCheckRewardCard: {
    backgroundColor: '#F0FDF4',
    borderRadius: 10,
    padding: 14,
    borderWidth: 1,
    borderColor: '#D1FAE5',
  },
  perfCheckRiskCard: {
    backgroundColor: '#FEF2F2',
    borderRadius: 10,
    padding: 14,
    borderWidth: 1,
    borderColor: '#FECACA',
  },
  perfCheckCardHeader: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
    marginBottom: 12,
  },
  perfCheckRewardTitle: {
    fontSize: 14,
    fontWeight: '700',
    color: '#059669',
    textTransform: 'uppercase',
    letterSpacing: 0.5,
  },
  perfCheckRiskTitle: {
    fontSize: 14,
    fontWeight: '700',
    color: '#DC2626',
    textTransform: 'uppercase',
    letterSpacing: 0.5,
  },
  perfCheckMetricLabel: {
    fontSize: 14,
    color: COLORS.textMuted,
    marginBottom: 4,
  },
  perfCheckMetricValueLarge: {
    fontSize: 26,
    fontWeight: '700',
    color: '#111827',
    marginBottom: 12,
  },
  perfCheckMetricValue: {
    fontSize: 15,
    fontWeight: '600',
    color: COLORS.text,
    flexShrink: 1,
    textAlign: 'right',
  },
  perfCheckMetricRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    marginBottom: 8,
  },
  perfCheckMetricInlineRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
  },
  perfCheckBenchmarkStrip: {
    borderTopWidth: 1,
    borderTopColor: COLORS.border,
    paddingTop: 10,
    paddingBottom: 2,
    alignItems: 'center',
  },
  perfCheckBenchmarkText: {
    fontSize: 14,
    color: '#6B7280',
    textAlign: 'center',
  },
  perfCheckBenchmarkValue: {
    fontWeight: '600',
    color: '#374151',
  },
  perfCheckDisclaimer: {
    fontSize: 11,
    color: COLORS.textMuted,
    fontStyle: 'italic',
    marginTop: 8,
    textAlign: 'center',
  },
  // Period badge touchable
  perfCheckPeriodTouchable: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
  },
  // Metric label row with ? icon (reuses metricLabelRow pattern)
  perfCheckMetricLabelRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
    flexShrink: 1,
  },
  // Index comparison block (replaces benchmark strip)
  perfCheckIndexBlock: {
    borderTopWidth: 1,
    borderTopColor: COLORS.border,
    paddingTop: 10,
    paddingBottom: 2,
  },
  perfCheckIndexTitleRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
    marginBottom: 6,
  },
  perfCheckIndexTitle: {
    fontSize: 13,
    fontWeight: '700',
    color: '#374151',
  },
  perfCheckIndexRow: {
    fontSize: 15,
    color: COLORS.textMuted,
    lineHeight: 22,
  },
  perfCheckIndexRowValue: {
    fontWeight: '700',
    color: COLORS.text,
  },
  // Period selector modal
  periodSelectorOverlay: {
    flex: 1,
    backgroundColor: 'rgba(0,0,0,0.4)',
    justifyContent: 'flex-end',
  },
  periodSelectorSheet: {
    backgroundColor: '#FFFFFF',
    borderTopLeftRadius: 20,
    borderTopRightRadius: 20,
    padding: 24,
    paddingBottom: 40,
  },
  addToSheet: {
    backgroundColor: '#FFFFFF',
    borderTopLeftRadius: 24,
    borderTopRightRadius: 24,
    padding: 24,
    paddingBottom: 32,
  },
  addToSheetHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: 12,
  },
  addToSheetTitle: {
    fontSize: 22,
    fontWeight: '700',
    color: COLORS.text,
  },
  addToSheetItem: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 12,
    paddingVertical: 16,
    borderBottomWidth: 1,
    borderBottomColor: '#F3F4F6',
  },
  addToSheetItemActive: {
    backgroundColor: '#FFFBEB',
  },
  addToSheetItemActiveTracklist: {
    backgroundColor: '#EFF6FF',
  },
  addToSheetItemDisabled: {
    opacity: 0.5,
  },
  addToSheetIcon: {
    width: 42,
    height: 42,
    borderRadius: 999,
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: '#EEF2FF',
  },
  addToSheetIconWatchlist: {
    backgroundColor: '#FEF3C7',
  },
  addToSheetIconTracklist: {
    backgroundColor: '#DBEAFE',
  },
  addToSheetIconPortfolio: {
    backgroundColor: '#D1FAE5',
  },
  addToSheetTextWrap: {
    flex: 1,
  },
  addToSheetItemTitle: {
    fontSize: 17,
    fontWeight: '700',
    color: COLORS.text,
  },
  addToSheetItemText: {
    fontSize: 13,
    lineHeight: 19,
    color: COLORS.textLight,
    marginTop: 2,
  },
  addToSoonBadge: {
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 999,
    backgroundColor: '#FEF3C7',
  },
  addToSoonBadgeText: {
    fontSize: 11,
    fontWeight: '700',
    color: '#B45309',
  },
  periodSelectorHandle: {
    width: 40,
    height: 4,
    backgroundColor: '#D1D5DB',
    borderRadius: 2,
    alignSelf: 'center',
    marginBottom: 20,
  },
  periodSelectorTitle: {
    fontSize: 18,
    fontWeight: '700',
    color: '#111827',
    marginBottom: 16,
  },
  periodSelectorOption: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    paddingVertical: 14,
    borderBottomWidth: 1,
    borderBottomColor: '#F3F4F6',
  },
  periodSelectorOptionText: {
    fontSize: 16,
    color: '#374151',
  },
  periodSelectorOptionTextActive: {
    color: COLORS.primary,
    fontWeight: '700',
  },
  // Legacy styles kept for compatibility
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
  rrrValueRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
  },
  benchmarkStrip: {
    borderTopWidth: 1,
    borderTopColor: '#E5E7EB',
    paddingTop: 8,
    marginTop: 4,
  },
  benchmarkDivider: {
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
    justifyContent: 'space-between',
    paddingVertical: 6,
    marginBottom: 8,
  },
  companyDetailsMinimalText: {
    fontSize: 16,
    color: '#111827',
    fontWeight: '700',
  },
  companyDetailsExpanded: {
    backgroundColor: COLORS.card,
    borderRadius: 12,
    padding: 14,
    marginBottom: 16,
    borderWidth: 1,
    borderColor: COLORS.border,
    gap: 12,
  },
  companyDetailRowReadable: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    gap: 12,
  },
  companyDetailLabel: {
    fontSize: 16,
    fontWeight: '600',
    color: '#1F2937',
    flex: 2,
  },
  companyDetailValue: {
    flex: 3,
    textAlign: 'left',
    fontSize: 16,
    color: '#111827',
    lineHeight: 24,
  },
  companyDetailLink: {
    color: '#2563EB',
    textDecorationLine: 'underline',
  },
  descriptionTextCompact: {
    fontSize: 15,
    color: '#374151',
    lineHeight: 23,
  },
  companyShowMoreText: {
    fontSize: 15,
    color: '#2563EB',
    marginTop: 4,
    fontWeight: '600',
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
