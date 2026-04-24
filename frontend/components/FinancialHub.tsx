/**
 * P9 FINAL: Financial Hub Component
 * 
 * FEATURES:
 * - USD/% switcher controls BOTH chart AND table
 * - USD Mode: Show only absolute values
 * - % Mode: Show only YoY percentages + tap tooltip for absolute
 * - Green/Red backgrounds for trend direction
 * - Left-to-right chronology (oldest → latest → TTM)
 * - YoY enforcement (never QoQ fallback)
 */

import React, { useState, useMemo } from 'react';
import { View, Text, TouchableOpacity, StyleSheet, Modal, Pressable } from 'react-native';
import { Ionicons } from '@expo/vector-icons';

const COLORS = {
  primary: '#2563EB',
  background: '#F8FAFC',
  card: '#FFFFFF',
  text: '#1E293B',
  textLight: '#64748B',
  textMuted: '#94A3B8',
  border: '#E2E8F0',
  positive: '#10B981',
  negative: '#EF4444',
  positiveLight: '#D1FAE5',
  negativeLight: '#FEE2E2',
  neutral: '#F3F4F6',
  ttmBar: '#3B82F6',
  tooltipBg: '#1E293B',
};

// =============================================================================
// USER'S EXACT 6 RULES FOR COLORS (FINAL - DO NOT CHANGE)
// =============================================================================
// 1. PREVIOUS POSITIVE → NOW MORE POSITIVE = DARK green
// 2. PREVIOUS POSITIVE → NOW LESS POSITIVE = LIGHT green  
// 3. PREVIOUS NEGATIVE → NOW MORE NEGATIVE = DARK red
// 4. PREVIOUS NEGATIVE → NOW LESS NEGATIVE = LIGHT red
// 5. PREVIOUS NEGATIVE → NOW POSITIVE = ↗ Profit badge (green)
// 6. PREVIOUS POSITIVE → NOW NEGATIVE = ↘ Loss badge (red)
// =============================================================================

const CELL_COLORS = {
  darkGreen: '#059669',    // Rule 1: Profit growing
  lightGreen: '#D1FAE5',   // Rule 2: Profit declining (light mint green)
  darkRed: '#DC2626',      // Rule 3: Loss worsening
  lightRed: '#FEE2E2',     // Rule 4: Loss improving (light pink/red)
  profitBadge: '#10B981',  // Rule 5: Turned to profit
  lossBadge: '#EF4444',    // Rule 6: Turned to loss
};

// Main helper: Get cell appearance based on PREVIOUS and CURRENT absolute values
interface CellAppearance {
  backgroundColor: string | null;
  textColor: string;
  label: string | null;  // null = show YoY%, string = show this label
  isWhiteText: boolean;
}

const getCellAppearance = (
  currentValue: number | null,
  priorValue: number | null
): CellAppearance => {
  // No data case
  if (currentValue === null || priorValue === null) {
    return { backgroundColor: null, textColor: COLORS.textMuted, label: 'N/A', isWhiteText: false };
  }
  
  const wasProfitable = priorValue >= 0;
  const isProfitable = currentValue >= 0;
  
  // Rule 5: PREVIOUS NEGATIVE → NOW POSITIVE = ↗ Profit
  if (!wasProfitable && isProfitable) {
    return { backgroundColor: CELL_COLORS.darkGreen, textColor: '#FFFFFF', label: '↗ Profit', isWhiteText: true };
  }
  
  // Rule 6: PREVIOUS POSITIVE → NOW NEGATIVE = ↘ Loss
  if (wasProfitable && !isProfitable) {
    return { backgroundColor: CELL_COLORS.darkRed, textColor: '#FFFFFF', label: '↘ Loss', isWhiteText: true };
  }
  
  // Both periods are PROFITABLE (positive)
  if (wasProfitable && isProfitable) {
    // Rule 1: NOW MORE POSITIVE = DARK green
    if (currentValue >= priorValue) {
      return { backgroundColor: CELL_COLORS.darkGreen, textColor: '#FFFFFF', label: null, isWhiteText: true };
    }
    // Rule 2: NOW LESS POSITIVE = LIGHT green background, RED text
    return { backgroundColor: CELL_COLORS.lightGreen, textColor: COLORS.negative, label: null, isWhiteText: false };
  }
  
  // Both periods are LOSS (negative)
  if (!wasProfitable && !isProfitable) {
    // Rule 3: NOW MORE NEGATIVE = DARK red (loss got bigger = currentValue < priorValue since both negative)
    if (currentValue <= priorValue) {
      return { backgroundColor: CELL_COLORS.darkRed, textColor: '#FFFFFF', label: null, isWhiteText: true };
    }
    // Rule 4: NOW LESS NEGATIVE = LIGHT red (loss got smaller = currentValue > priorValue)
    return { backgroundColor: CELL_COLORS.lightRed, textColor: COLORS.negative, label: null, isWhiteText: false };
  }
  
  // Fallback (should never happen)
  return { backgroundColor: null, textColor: COLORS.textMuted, label: null, isWhiteText: false };
};

// INVERTED logic for Total Debt: debt decrease = GOOD (green), debt increase = BAD (red)
const getCellAppearanceDebt = (
  currentValue: number | null,
  priorValue: number | null
): CellAppearance => {
  // No data case
  if (currentValue === null || priorValue === null) {
    return { backgroundColor: null, textColor: COLORS.textMuted, label: 'N/A', isWhiteText: false };
  }
  
  // For debt: DECREASE is GOOD, INCREASE is BAD
  // Debt values are always positive numbers
  
  if (currentValue < priorValue) {
    // Debt DECREASED → GOOD → GREEN
    // Big decrease (> 10%) = DARK green, small decrease = LIGHT green
    const decreasePct = ((priorValue - currentValue) / priorValue) * 100;
    if (decreasePct >= 10) {
      return { backgroundColor: CELL_COLORS.darkGreen, textColor: '#FFFFFF', label: null, isWhiteText: true };
    }
    return { backgroundColor: CELL_COLORS.lightGreen, textColor: COLORS.positive, label: null, isWhiteText: false };
  }
  
  if (currentValue > priorValue) {
    // Debt INCREASED → BAD → RED
    // Big increase (> 10%) = DARK red, small increase = LIGHT red
    const increasePct = ((currentValue - priorValue) / priorValue) * 100;
    if (increasePct >= 10) {
      return { backgroundColor: CELL_COLORS.darkRed, textColor: '#FFFFFF', label: null, isWhiteText: true };
    }
    return { backgroundColor: CELL_COLORS.lightRed, textColor: COLORS.negative, label: null, isWhiteText: false };
  }
  
  // No change
  return { backgroundColor: null, textColor: COLORS.textMuted, label: null, isWhiteText: false };
};

// Types
interface Core5Period {
  period_date: string;
  revenue: number | null;
  net_income: number | null;
  free_cash_flow: number | null;
  cash: number | null;
  total_debt: number | null;
}

interface FinancialsData {
  annual: Core5Period[];
  quarterly: Core5Period[];
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
    cash?: number | null;
    total_debt?: number | null;
  } | null;
}

interface FinancialHubProps {
  financials: FinancialsData | null;
  expanded: boolean;
  onToggle: () => void;
  loading?: boolean;
  emptyStateMessage?: string;
}

interface TooltipData {
  visible: boolean;
  value: string;
  label: string;
}

// NEW: Detail popup showing PREVIOUS and CURRENT period + value
interface DetailPopupData {
  visible: boolean;
  metric: string;
  currentPeriod: string;
  currentValue: number | null;
  priorPeriod: string;
  priorValue: number | null;
}

// Helper functions
/**
 * Compact USD formatting with max ~3 significant digits + suffix
 * Rules:
 * - >= 100B: 0 decimals (435B)
 * - 10B-99.9B: 1 decimal (45.3B)
 * - 1B-9.99B: 2 decimals (5.53B)
 * - 100M-999M: 0 decimals (503M)
 * - 10M-99.9M: 1 decimal (68.8M)
 * - 1M-9.99M: 2 decimals (3.80M)
 * - Similar for K
 * - Preserves sign (-$367M)
 */
const formatCurrency = (value: number | null): string => {
  if (value === null || value === undefined) return 'N/A';
  
  const absValue = Math.abs(value);
  const sign = value < 0 ? '-' : '';
  
  // Trillions
  if (absValue >= 1e12) {
    const scaled = absValue / 1e12;
    if (scaled >= 100) return `${sign}$${Math.round(scaled)}T`;
    if (scaled >= 10) return `${sign}$${scaled.toFixed(1)}T`;
    return `${sign}$${scaled.toFixed(2)}T`;
  }
  
  // Billions
  if (absValue >= 1e9) {
    const scaled = absValue / 1e9;
    if (scaled >= 100) return `${sign}$${Math.round(scaled)}B`;
    if (scaled >= 10) return `${sign}$${scaled.toFixed(1)}B`;
    return `${sign}$${scaled.toFixed(2)}B`;
  }
  
  // Millions
  if (absValue >= 1e6) {
    const scaled = absValue / 1e6;
    if (scaled >= 100) return `${sign}$${Math.round(scaled)}M`;
    if (scaled >= 10) return `${sign}$${scaled.toFixed(1)}M`;
    return `${sign}$${scaled.toFixed(2)}M`;
  }
  
  // Thousands
  if (absValue >= 1e3) {
    const scaled = absValue / 1e3;
    if (scaled >= 100) return `${sign}$${Math.round(scaled)}K`;
    if (scaled >= 10) return `${sign}$${scaled.toFixed(1)}K`;
    return `${sign}$${scaled.toFixed(2)}K`;
  }
  
  // Small numbers
  return `${sign}$${absValue.toFixed(0)}`;
};

// % formula: (current/prior - 1) * 100
const formatPercent = (value: number | null): string => {
  if (value === null || value === undefined) return 'N/A';
  const sign = value >= 0 ? '+' : '';
  return `${sign}${(value * 100).toFixed(1)}%`;
};

const formatDeltaUSD = (delta: number | null): string => {
  if (delta === null) return 'N/A';
  const sign = delta >= 0 ? '+' : '';
  const absValue = Math.abs(delta);
  if (absValue >= 1e9) return `${sign}$${(delta / 1e9).toFixed(1)}B`;
  if (absValue >= 1e6) return `${sign}$${(delta / 1e6).toFixed(1)}M`;
  return `${sign}$${delta.toLocaleString()}`;
};

const formatPeriodLabel = (periodDate: string, isQuarterly: boolean): string => {
  if (!periodDate) return '';
  const date = new Date(periodDate);
  const year = date.getFullYear().toString().slice(-2);
  
  if (isQuarterly) {
    const month = date.getMonth();
    const quarter = Math.floor(month / 3) + 1;
    return `Q${quarter}'${year}`;
  }
  return `FY'${year}`;
};

// =============================================================================
// TOP 1% TRUSTWORTHY YoY Delta Calculation
// =============================================================================
// Rules:
// - Revenue: normal YoY% (if negative revenue => data error)
// - Net Income / FCF: 
//   - |prior| < $1M => "N/A (Prior too small)"
//   - Sign flip (profit→loss) => "N/A (Turned to loss)"
//   - Sign flip (loss→profit) => "N/A (Turned to profit)"
//   - Both negative => "N/A (Loss vs loss)"
//   - Both positive, prior >= $1M => normal YoY%
// =============================================================================

const PRIOR_THRESHOLD_USD = 1_000_000; // $1M minimum for meaningful YoY%

// Exact reason codes for matching
const REASON_TURNED_TO_PROFIT = 'N/A (Turned to profit)';
const REASON_TURNED_TO_LOSS = 'N/A (Turned to loss)';
const REASON_PRIOR_TOO_SMALL = 'N/A (Prior too small)';
const REASON_DATA_ERROR = 'N/A (Data error)';
const REASON_PRIOR_ZERO = 'N/A (Prior = 0)';
// New: Loss vs Loss semantic codes (with computed yoyPct)
const REASON_LOSS_IMPROVED = 'LOSS_IMPROVED';
const REASON_LOSS_WORSENED = 'LOSS_WORSENED';
const REASON_LOSS_UNCHANGED = 'LOSS_UNCHANGED';

// Helper to map reason to short display label with styling
interface ReasonDisplay {
  label: string;
  isPositive: boolean;
  isNegative: boolean;
  tooltip?: string;
  useYoyPct?: boolean;  // If true, use the yoyPct value for display
}

const getReasonDisplay = (reason: string | undefined, yoyPct?: number | null): ReasonDisplay | null => {
  if (!reason) return null;
  
  // Exact equality matching for semantic reasons
  if (reason === REASON_TURNED_TO_PROFIT) {
    return { 
      label: '↗ Profit', 
      isPositive: true, 
      isNegative: false,
      tooltip: 'Turned from loss to profit vs prior period.'
    };
  }
  if (reason === REASON_TURNED_TO_LOSS) {
    return { 
      label: '↘ Loss', 
      isPositive: false, 
      isNegative: true,
      tooltip: 'Turned from profit to loss vs prior period.'
    };
  }
  
  // Loss vs Loss with computed YoY% - use actual percentage with semantic label
  if (reason === REASON_LOSS_IMPROVED && yoyPct !== null && yoyPct !== undefined) {
    const pctStr = `+${(yoyPct * 100).toFixed(1)}%`;
    return { 
      label: pctStr, 
      isPositive: true, 
      isNegative: false,
      tooltip: 'Loss improved vs prior period (smaller loss).',
      useYoyPct: true
    };
  }
  if (reason === REASON_LOSS_WORSENED && yoyPct !== null && yoyPct !== undefined) {
    const pctStr = `${(yoyPct * 100).toFixed(1)}%`;
    return { 
      label: pctStr, 
      isPositive: false, 
      isNegative: true,
      tooltip: 'Loss worsened vs prior period (bigger loss).',
      useYoyPct: true
    };
  }
  if (reason === REASON_LOSS_UNCHANGED) {
    return { 
      label: '0.0%', 
      isPositive: false, 
      isNegative: false,
      tooltip: 'Loss unchanged vs prior period.'
    };
  }
  
  // All other N/A reasons -> neutral gray "N/A"
  if (reason === REASON_PRIOR_TOO_SMALL ||
      reason === REASON_DATA_ERROR ||
      reason === REASON_PRIOR_ZERO ||
      reason === 'N/A') {
    return { label: 'N/A', isPositive: false, isNegative: false };
  }
  
  // Fallback for any unknown reason
  return { label: 'N/A', isPositive: false, isNegative: false };
};

const calculateYoYDelta = (
  periods: Core5Period[],
  currentIndex: number,
  metric: keyof Core5Period,
  isQuarterly: boolean
): { delta: number | null; yoyPct: number | null; reason?: string } => {
  const current = periods[currentIndex];
  const currentValue = current?.[metric] as number | null;
  
  // Case 1: Missing current data
  if (currentValue === null || currentValue === undefined) {
    return { delta: null, yoyPct: null, reason: 'N/A' };
  }
  
  // YoY enforcement: Quarterly needs +4, Annual needs +1
  const priorIndex = isQuarterly ? currentIndex + 4 : currentIndex + 1;
  const prior = periods[priorIndex];
  const priorValue = prior?.[metric] as number | null;
  
  // Case 1b: Missing prior data
  if (priorValue === null || priorValue === undefined) {
    return { delta: null, yoyPct: null, reason: 'N/A' };
  }
  
  // Case 2: Revenue special handling
  if (metric === 'revenue') {
    if (currentValue < 0 || priorValue < 0) {
      return { delta: null, yoyPct: null, reason: 'N/A (Data error)' };
    }
    if (priorValue === 0) {
      return { delta: null, yoyPct: null, reason: 'N/A (Prior = 0)' };
    }
    // Normal calculation for revenue
    const delta = currentValue - priorValue;
    const yoyPct = (currentValue / priorValue) - 1;
    return { delta, yoyPct };
  }
  
  // Case 2b: Cash and Total Debt (snapshot metrics) - neutral coloring, simple YoY%
  if (metric === 'cash' || metric === 'total_debt') {
    // Prior too small guardrail
    if (Math.abs(priorValue) < PRIOR_THRESHOLD_USD) {
      return { delta: null, yoyPct: null, reason: 'N/A (Prior too small)' };
    }
    const delta = currentValue - priorValue;
    const yoyPct = (currentValue - priorValue) / Math.abs(priorValue);
    return { delta, yoyPct };
  }
  
  // Cases 3-6 apply to Net Income and FCF only
  
  // Case 3: Prior too small (fixed epsilon: $1M)
  if (Math.abs(priorValue) < PRIOR_THRESHOLD_USD) {
    return { delta: null, yoyPct: null, reason: 'N/A (Prior too small)' };
  }
  
  // Case 4: Sign flip - profit to loss
  if (priorValue > 0 && currentValue < 0) {
    return { delta: null, yoyPct: null, reason: 'N/A (Turned to loss)' };
  }
  
  // Case 5: Sign flip - loss to profit
  if (priorValue < 0 && currentValue > 0) {
    return { delta: null, yoyPct: null, reason: 'N/A (Turned to profit)' };
  }
  
  // Case 6: Both negative (loss vs loss) - compute meaningful YoY%
  // Formula: ((current - prior) / abs(prior)) * 100
  // Positive = loss improved (got smaller), Negative = loss worsened (got bigger)
  if (currentValue < 0 && priorValue < 0) {
    const delta = currentValue - priorValue;
    // Use abs(prior) for denominator to get intuitive direction
    const yoyPct = (currentValue - priorValue) / Math.abs(priorValue);
    
    // Return with special reason codes for styling
    if (yoyPct > 0) {
      return { delta, yoyPct, reason: 'LOSS_IMPROVED' };
    } else if (yoyPct < 0) {
      return { delta, yoyPct, reason: 'LOSS_WORSENED' };
    } else {
      return { delta, yoyPct: 0, reason: 'LOSS_UNCHANGED' };
    }
  }
  
  // Case 7: Normal calculation (both positive, prior >= $1M)
  const delta = currentValue - priorValue;
  const yoyPct = (currentValue / priorValue) - 1;
  return { delta, yoyPct };
};

// Financial Vitals Pill Logic (complete cascade)
const getFinancialVitalsPill = (financials: FinancialsData | null): { label: string; variant: 'negative' | 'positive' | 'neutral' } => {
  if (!financials) return { label: 'No Data', variant: 'neutral' };
  
  const ttm = financials.ttm;
  const priorTtm = financials.prior_ttm;
  
  if (!ttm?.revenue && !ttm?.net_income) return { label: 'Pending', variant: 'neutral' };
  
  // Burning Cash: TTM FCF < 0
  if (ttm.free_cash_flow !== null && ttm.free_cash_flow < 0) {
    return { label: 'Burning Cash', variant: 'negative' };
  }
  
  let revenueYoY: number | null = null;
  let netIncomeYoY: number | null = null;
  
  if (ttm.revenue && priorTtm?.revenue && priorTtm.revenue !== 0) {
    revenueYoY = (ttm.revenue / priorTtm.revenue) - 1;
  }
  if (ttm.net_income !== null && priorTtm?.net_income && priorTtm.net_income !== 0) {
    netIncomeYoY = (ttm.net_income / priorTtm.net_income) - 1;
  }
  
  // Declining Business
  if (revenueYoY !== null && revenueYoY < 0 && netIncomeYoY !== null && netIncomeYoY < 0) {
    return { label: 'Declining Business', variant: 'negative' };
  }
  // Healthy Growth
  if (revenueYoY !== null && revenueYoY > 0 && netIncomeYoY !== null && netIncomeYoY > 0) {
    return { label: 'Healthy Growth', variant: 'positive' };
  }
  // Expansion
  if (revenueYoY !== null && revenueYoY > 0 && (netIncomeYoY === null || netIncomeYoY < 0)) {
    return { label: 'Expansion', variant: 'neutral' };
  }
  // Cutting Costs
  if (revenueYoY !== null && revenueYoY < 0 && netIncomeYoY !== null && netIncomeYoY > 0) {
    return { label: 'Cutting Costs', variant: 'neutral' };
  }
  
  return { label: 'In line', variant: 'neutral' };
};

// Main Component
const FinancialHub: React.FC<FinancialHubProps> = ({
  financials,
  expanded,
  onToggle,
  loading = false,
  emptyStateMessage = 'No financial data available',
}) => {
  const [period, setPeriod] = useState<'quarterly' | 'annual'>('annual');
  const [displayMode, setDisplayMode] = useState<'usd' | 'pct'>('pct');
  const [tooltip, setTooltip] = useState<TooltipData>({ visible: false, value: '', label: '' });
  const [chartMetric, setChartMetric] = useState<'revenue' | 'net_income' | 'free_cash_flow' | 'cash' | 'total_debt'>('revenue');
  const [dropdownOpen, setDropdownOpen] = useState(false);
  
  // NEW: Detail popup state for showing PREVIOUS and CURRENT period + value
  const [detailPopup, setDetailPopup] = useState<DetailPopupData>({
    visible: false,
    metric: '',
    currentPeriod: '',
    currentValue: null,
    priorPeriod: '',
    priorValue: null,
  });
  
  // Show detail popup with previous and current values
  const showDetailPopup = (
    metric: string,
    currentPeriod: string,
    currentValue: number | null,
    priorPeriod: string,
    priorValue: number | null
  ) => {
    setDetailPopup({
      visible: true,
      metric,
      currentPeriod,
      currentValue,
      priorPeriod,
      priorValue,
    });
  };
  
  const hideDetailPopup = () => {
    setDetailPopup(prev => ({ ...prev, visible: false }));
  };
  
  // Chart metric options for dropdown
  const METRIC_OPTIONS = [
    { key: 'revenue' as const, label: 'Revenue' },
    { key: 'net_income' as const, label: 'Net Income' },
    { key: 'free_cash_flow' as const, label: 'Free Cash Flow' },
    { key: 'cash' as const, label: 'Cash' },
    { key: 'total_debt' as const, label: 'Total Debt' },
  ];
  
  // Dynamic chart title based on selected metric
  const getChartTitle = () => {
    switch (chartMetric) {
      case 'revenue': return 'Revenue Delta (YoY)';
      case 'net_income': return 'Net Income Delta (YoY)';
      case 'free_cash_flow': return 'Free Cash Flow Delta (YoY)';
      case 'cash': return 'Cash Delta (YoY)';
      case 'total_debt': return 'Total Debt Delta (YoY)';
    }
  };
  
  // Helper to get short label for chart bars (NO "N/A" - always short labels)
  const getChartBarLabel = (reason: string | undefined, yoyPct: number | null, delta: number | null) => {
    // Sign flip labels - always short
    if (reason === 'N/A (Turned to profit)') return '↗ Profit';
    if (reason === 'N/A (Turned to loss)') return '↘ Loss';
    
    // Loss vs Loss - show the percentage
    if (reason === 'LOSS_IMPROVED' && yoyPct !== null) return `+${(yoyPct * 100).toFixed(1)}%`;
    if (reason === 'LOSS_WORSENED' && yoyPct !== null) return `${(yoyPct * 100).toFixed(1)}%`;
    if (reason === 'LOSS_UNCHANGED') return '0.0%';
    
    // Any other reason (Prior too small, Data error, etc.) - show short "—"
    if (reason) return '—';
    
    // No data
    if (yoyPct === null || yoyPct === undefined || Number.isNaN(yoyPct)) return '—';
    
    // Normal case - show percentage or USD
    return displayMode === 'pct' ? formatPercent(yoyPct) : formatDeltaUSD(delta);
  };
  
  const vitalsPill = useMemo(() => getFinancialVitalsPill(financials), [financials]);
  
  // Get periods REVERSED for left-to-right chronology (oldest first)
  const periods = useMemo(() => {
    if (!financials) return [];
    const rawPeriods = period === 'quarterly' ? financials.quarterly : financials.annual;
    const sliced = rawPeriods.slice(0, 4);
    return [...sliced].reverse(); // Oldest on left, newest on right
  }, [financials, period]);
  
  // TTM YoY deltas - using TOP 1% TRUSTWORTHY rules
  const ttmDeltas = useMemo(() => {
    if (!financials?.ttm || !financials?.prior_ttm) return null;
    
    // Revenue: normal YoY% (negative revenue = data error)
    const calcRevenueDelta = (current: number | null, prior: number | null): { delta: number | null; yoyPct: number | null; reason?: string } => {
      if (current === null || prior === null) return { delta: null, yoyPct: null, reason: 'N/A' };
      if (current < 0 || prior < 0) return { delta: null, yoyPct: null, reason: 'N/A (Data error)' };
      if (prior === 0) return { delta: null, yoyPct: null, reason: 'N/A (Prior = 0)' };
      return { delta: current - prior, yoyPct: (current / prior) - 1 };
    };
    
    // Net Income / FCF: trustworthy rules
    const calcProfitDelta = (current: number | null, prior: number | null): { delta: number | null; yoyPct: number | null; reason?: string } => {
      if (current === null || prior === null) return { delta: null, yoyPct: null, reason: 'N/A' };
      
      // Case 3: Prior too small
      if (Math.abs(prior) < PRIOR_THRESHOLD_USD) {
        return { delta: null, yoyPct: null, reason: 'N/A (Prior too small)' };
      }
      
      // Case 4: Sign flip - profit to loss
      if (prior > 0 && current < 0) {
        return { delta: null, yoyPct: null, reason: 'N/A (Turned to loss)' };
      }
      
      // Case 5: Sign flip - loss to profit
      if (prior < 0 && current > 0) {
        return { delta: null, yoyPct: null, reason: 'N/A (Turned to profit)' };
      }
      
      // Case 6: Both negative (loss vs loss) - compute meaningful YoY%
      if (current < 0 && prior < 0) {
        const delta = current - prior;
        const yoyPct = (current - prior) / Math.abs(prior);
        if (yoyPct > 0) {
          return { delta, yoyPct, reason: 'LOSS_IMPROVED' };
        } else if (yoyPct < 0) {
          return { delta, yoyPct, reason: 'LOSS_WORSENED' };
        } else {
          return { delta, yoyPct: 0, reason: 'LOSS_UNCHANGED' };
        }
      }
      
      // Case 7: Normal (both positive, prior >= $1M)
      return { delta: current - prior, yoyPct: (current / prior) - 1 };
    };
    
    // Snapshot metrics (Cash, Total Debt) - neutral coloring, simple YoY%
    const calcSnapshotDelta = (current: number | null | undefined, prior: number | null | undefined): { delta: number | null; yoyPct: number | null; reason?: string } => {
      if (current === null || current === undefined || prior === null || prior === undefined) {
        return { delta: null, yoyPct: null, reason: 'N/A' };
      }
      
      // Reuse existing prior-too-small guardrail
      if (Math.abs(prior) < PRIOR_THRESHOLD_USD) {
        return { delta: null, yoyPct: null, reason: 'N/A (Prior too small)' };
      }
      
      const delta = current - prior;
      const yoyPct = (current - prior) / Math.abs(prior);
      return { delta, yoyPct };
    };
    
    return {
      revenue: calcRevenueDelta(financials.ttm.revenue, financials.prior_ttm.revenue),
      net_income: calcProfitDelta(financials.ttm.net_income, financials.prior_ttm.net_income),
      fcf: calcProfitDelta(financials.ttm.free_cash_flow, financials.prior_ttm.free_cash_flow),
      cash: calcSnapshotDelta(financials.ttm.cash, financials.prior_ttm?.cash),
      total_debt: calcSnapshotDelta(financials.ttm.total_debt, financials.prior_ttm?.total_debt),
    };
  }, [financials]);
  
  // Tooltip handler - shows absolute value when in % mode
  const showTooltip = (label: string, value: number | null) => {
    if (displayMode === 'pct' && value !== null) {
      setTooltip({ visible: true, value: formatCurrency(value), label });
    }
  };
  
  const hideTooltip = () => setTooltip({ visible: false, value: '', label: '' });
  
  if (!financials) {
    return (
      <View style={styles.container}>
        <TouchableOpacity style={styles.header} onPress={onToggle}>
          <View style={styles.headerLeft}>
            <Text style={styles.title}>Financials</Text>
            <View style={[styles.pill, styles.pillNeutral]}>
              <Text style={styles.pillText}>{loading ? 'Verifying…' : 'No Data'}</Text>
            </View>
          </View>
          <Ionicons name={expanded ? 'chevron-up' : 'chevron-down'} size={20} color={COLORS.textMuted} />
        </TouchableOpacity>
        {expanded && !loading ? (
          <View style={styles.noDataState}>
            <Text style={styles.noDataText}>{emptyStateMessage}</Text>
          </View>
        ) : null}
      </View>
    );
  }
  
  // Render table cell with USER'S 6 RULES + click shows PREVIOUS and CURRENT
  const renderTableCell = (
    currentValue: number | null,
    priorValue: number | null,
    currentPeriodLabel: string,
    priorPeriodLabel: string,
    metric: string,
    useColorRules: boolean = false,
    isDebt: boolean = false  // Use inverted logic for debt
  ) => {
    // Get appearance based on USER'S 6 RULES (or inverted for debt)
    const appearance = useColorRules 
      ? (isDebt ? getCellAppearanceDebt(currentValue, priorValue) : getCellAppearance(currentValue, priorValue))
      : { backgroundColor: null, textColor: COLORS.text, label: null, isWhiteText: false };
    
    // Calculate YoY % for display (only if no special label)
    let displayText = '';
    if (appearance.label) {
      displayText = appearance.label;
    } else if (currentValue !== null && priorValue !== null && priorValue !== 0) {
      const yoyPct = ((currentValue - priorValue) / Math.abs(priorValue)) * 100;
      displayText = yoyPct >= 0 ? `+${yoyPct.toFixed(1)}%` : `${yoyPct.toFixed(1)}%`;
    } else if (currentValue !== null) {
      displayText = displayMode === 'usd' ? formatCurrency(currentValue) : 'N/A';
    } else {
      displayText = 'N/A';
    }
    
    // In USD mode, always show absolute value
    if (displayMode === 'usd') {
      displayText = formatCurrency(currentValue);
    }
    
    return (
      <TouchableOpacity
        style={[
          styles.tableValueCell, 
          useColorRules && appearance.backgroundColor 
            ? { backgroundColor: appearance.backgroundColor } 
            : null
        ]}
        onPress={() => showDetailPopup(metric, currentPeriodLabel, currentValue, priorPeriodLabel, priorValue)}
        activeOpacity={0.7}
        data-testid={`cell-${metric.toLowerCase().replace(/\s/g, '-')}-${currentPeriodLabel}`}
      >
        <Text style={[
          displayMode === 'usd' ? styles.cellValueUSD : styles.cellValuePct,
          useColorRules ? { color: appearance.textColor } : null
        ]}>
          {displayText}
        </Text>
      </TouchableOpacity>
    );
  };
  
  // Render TTM cell with USER'S 6 RULES + click shows PREVIOUS and CURRENT
  const renderTTMCell = (
    currentValue: number | null,
    priorValue: number | null,
    metric: string,
    useColorRules: boolean = false,
    isDebt: boolean = false  // Use inverted logic for debt
  ) => {
    // Get appearance based on USER'S 6 RULES (or inverted for debt)
    const appearance = useColorRules 
      ? (isDebt ? getCellAppearanceDebt(currentValue, priorValue) : getCellAppearance(currentValue, priorValue))
      : { backgroundColor: null, textColor: COLORS.text, label: null, isWhiteText: false };
    
    // Calculate YoY % for display (only if no special label)
    let displayText = '';
    if (appearance.label) {
      displayText = appearance.label;
    } else if (currentValue !== null && priorValue !== null && priorValue !== 0) {
      const yoyPct = ((currentValue - priorValue) / Math.abs(priorValue)) * 100;
      displayText = yoyPct >= 0 ? `+${yoyPct.toFixed(1)}%` : `${yoyPct.toFixed(1)}%`;
    } else if (currentValue !== null) {
      displayText = displayMode === 'usd' ? formatCurrency(currentValue) : 'N/A';
    } else {
      displayText = 'N/A';
    }
    
    // In USD mode, always show absolute value
    if (displayMode === 'usd') {
      displayText = formatCurrency(currentValue);
    }
    
    return (
      <TouchableOpacity
        style={[
          styles.tableValueCell,
          styles.ttmCell,
          useColorRules && appearance.backgroundColor 
            ? { backgroundColor: appearance.backgroundColor, borderWidth: 2, borderColor: COLORS.ttmBar } 
            : null
        ]}
        onPress={() => showDetailPopup(metric, 'TTM', currentValue, 'Prior TTM', priorValue)}
        activeOpacity={0.7}
        data-testid={`cell-${metric.toLowerCase().replace(/\s/g, '-')}-ttm`}
      >
        <Text style={[
          displayMode === 'usd' ? styles.cellValueUSD : styles.cellValuePct,
          useColorRules ? { color: appearance.textColor } : null
        ]}>
          {displayText}
        </Text>
      </TouchableOpacity>
    );
  };
  
  return (
    <View style={styles.container} data-testid="financial-hub">
      {/* Detail Popup Modal - shows PREVIOUS and CURRENT period + value */}
      <Modal
        visible={detailPopup.visible}
        transparent
        animationType="fade"
        onRequestClose={hideDetailPopup}
      >
        <Pressable style={styles.tooltipOverlay} onPress={hideDetailPopup}>
          <View style={styles.detailPopupBox}>
            <Text style={styles.detailPopupTitle}>{detailPopup.metric}</Text>
            
            {/* Special case: Snapshot metric (Cash/Debt TTM) - show explanation instead of prior/current */}
            {detailPopup.priorPeriod === 'Not applicable' ? (
              <>
                <Text style={styles.detailExplanation}>
                  Not applicable: Cash and Total Debt are point-in-time balance sheet values (no TTM).
                </Text>
                <View style={styles.detailRow}>
                  <Text style={styles.detailPeriodLabel}>Current Value</Text>
                  <Text style={[styles.detailValue, styles.detailValueCurrent]}>{formatCurrency(detailPopup.currentValue)}</Text>
                </View>
              </>
            ) : (
              <>
                {/* Prior Period */}
                <View style={styles.detailRow}>
                  <Text style={styles.detailPeriodLabel}>{detailPopup.priorPeriod}</Text>
                  <Text style={styles.detailValue}>{formatCurrency(detailPopup.priorValue)}</Text>
                </View>
                
                {/* Arrow */}
                <Ionicons name="arrow-down" size={20} color={COLORS.textMuted} />
                
                {/* Current Period */}
                <View style={styles.detailRow}>
                  <Text style={styles.detailPeriodLabel}>{detailPopup.currentPeriod}</Text>
                  <Text style={[styles.detailValue, styles.detailValueCurrent]}>{formatCurrency(detailPopup.currentValue)}</Text>
                </View>
              </>
            )}
            
            {/* Close hint */}
            <Text style={styles.detailHint}>Tap anywhere to close</Text>
          </View>
        </Pressable>
      </Modal>
      
      {/* Header */}
      <TouchableOpacity style={styles.header} onPress={onToggle} data-testid="financials-toggle">
        <View style={styles.headerLeft}>
          <Text style={styles.sectionIcon}>📊</Text>
          <Text style={styles.title}>Financials</Text>
          {!expanded && (
            <View style={[
              styles.pill,
              vitalsPill.variant === 'positive' && styles.pillPositive,
              vitalsPill.variant === 'negative' && styles.pillNegative,
              vitalsPill.variant === 'neutral' && styles.pillNeutral,
            ]} data-testid="financials-vitals-pill">
              <Text style={[
                styles.pillText,
                vitalsPill.variant === 'positive' && styles.pillTextPositive,
                vitalsPill.variant === 'negative' && styles.pillTextNegative,
              ]}>{vitalsPill.label}</Text>
            </View>
          )}
        </View>
        <Ionicons name={expanded ? 'chevron-up' : 'chevron-down'} size={20} color={COLORS.textMuted} />
      </TouchableOpacity>
      
      {expanded && (
        <>
          {/* Switchers */}
          <View style={styles.switcherRow}>
            <View style={styles.periodSwitcher}>
              <TouchableOpacity
                style={[styles.switchBtn, period === 'quarterly' && styles.switchBtnActive]}
                onPress={() => setPeriod('quarterly')}
              >
                <Text style={[styles.switchText, period === 'quarterly' && styles.switchTextActive]}>Quarterly</Text>
              </TouchableOpacity>
              <TouchableOpacity
                style={[styles.switchBtn, period === 'annual' && styles.switchBtnActive]}
                onPress={() => setPeriod('annual')}
              >
                <Text style={[styles.switchText, period === 'annual' && styles.switchTextActive]}>Annual</Text>
              </TouchableOpacity>
            </View>
            
            <View style={styles.modeSwitcher}>
              <TouchableOpacity
                style={[styles.modeBtn, displayMode === 'usd' && styles.modeBtnActive]}
                onPress={() => setDisplayMode('usd')}
              >
                <Text style={[styles.modeText, displayMode === 'usd' && styles.modeTextActive]}>USD</Text>
              </TouchableOpacity>
              <TouchableOpacity
                style={[styles.modeBtn, displayMode === 'pct' && styles.modeBtnActive]}
                onPress={() => setDisplayMode('pct')}
              >
                <Text style={[styles.modeText, displayMode === 'pct' && styles.modeTextActive]}>%</Text>
              </TouchableOpacity>
            </View>
          </View>
          
          {/* Delta Bar Chart */}
          <View style={styles.chartSection}>
            <View style={styles.chartHeader}>
              {/* Dropdown Title */}
              <TouchableOpacity 
                style={styles.chartTitleBtn}
                onPress={() => setDropdownOpen(true)}
              >
                <Text style={styles.chartTitle}>{getChartTitle()}</Text>
                <Ionicons name="chevron-down" size={14} color={COLORS.textLight} />
              </TouchableOpacity>
            </View>
            
            <View style={styles.chartWrapper}>
              <View style={styles.chartContainer}>
                {periods.map((p, displayIndex) => {
                  const rawPeriods = period === 'quarterly' ? financials.quarterly : financials.annual;
                  const originalIndex = periods.length - 1 - displayIndex;
                  const priorIndex = period === 'quarterly' ? originalIndex + 4 : originalIndex + 1;
                  
                  const currentValue = p[chartMetric] as number | null;
                  const priorPeriod = rawPeriods[priorIndex];
                  const priorValue = priorPeriod?.[chartMetric] as number | null ?? null;
                  
                  const isProfitLossMetric = chartMetric === 'net_income' || chartMetric === 'free_cash_flow';
                  const isDebtMetric = chartMetric === 'total_debt';
                  
                  // Use USER'S 6 RULES for Net Income & FCF
                  const appearance = isProfitLossMetric 
                    ? getCellAppearance(currentValue, priorValue)
                    : { backgroundColor: null, textColor: COLORS.text, label: null, isWhiteText: false };
                  
                  // Calculate YoY% for bar height and display
                  let yoyPct: number | null = null;
                  let displayLabel = '—';
                  
                  if (currentValue !== null && priorValue !== null && priorValue !== 0) {
                    yoyPct = (currentValue - priorValue) / Math.abs(priorValue);
                    displayLabel = yoyPct >= 0 ? `+${(yoyPct * 100).toFixed(1)}%` : `${(yoyPct * 100).toFixed(1)}%`;
                  }
                  
                  // Override with special labels from 6 rules
                  if (isProfitLossMetric && appearance.label) {
                    displayLabel = appearance.label;
                  }
                  
                  // Determine bar color and text color
                  let barColor: string | null = null;
                  let textColor: string = COLORS.textMuted;
                  
                  if (isProfitLossMetric && appearance.backgroundColor) {
                    // Use 6 rules colors for NI/FCF
                    barColor = appearance.backgroundColor;
                    textColor = appearance.textColor;
                  } else if (isDebtMetric && currentValue !== null && priorValue !== null) {
                    // Debt: decrease = good (green)
                    const isGood = currentValue <= priorValue;
                    barColor = isGood ? COLORS.positive : COLORS.negative;
                    textColor = isGood ? COLORS.positive : COLORS.negative;
                  } else if (yoyPct !== null) {
                    // Revenue/Cash: simple green/red
                    barColor = yoyPct >= 0 ? COLORS.positive : COLORS.negative;
                    textColor = yoyPct >= 0 ? COLORS.positive : COLORS.negative;
                  }
                  
                  const barHeight = Math.min(Math.abs(yoyPct || 0) * 200, 60);
                  
                  return (
                    <View key={p.period_date} style={styles.barColumn}>
                      <Text 
                        style={[styles.barLabel, { color: textColor }]} 
                        numberOfLines={1}
                      >
                        {displayLabel}
                      </Text>
                      <View style={[
                        styles.bar,
                        { height: Math.max(barHeight, 16) },
                        barColor ? { backgroundColor: barColor } : styles.barNA,
                      ]} />
                      <Text style={styles.barPeriod}>{formatPeriodLabel(p.period_date, period === 'quarterly')}</Text>
                    </View>
                  );
                })}
                
                {/* TTM Bar - uses selected metric with USER'S 6 RULES */}
                {(() => {
                  const isProfitLossMetric = chartMetric === 'net_income' || chartMetric === 'free_cash_flow';
                  const isDebtMetric = chartMetric === 'total_debt';
                  
                  const ttmValue = chartMetric === 'revenue' ? financials?.ttm?.revenue
                    : chartMetric === 'net_income' ? financials?.ttm?.net_income
                    : chartMetric === 'free_cash_flow' ? financials?.ttm?.free_cash_flow
                    : chartMetric === 'cash' ? financials?.ttm?.cash
                    : financials?.ttm?.total_debt;
                  
                  const priorTtmValue = chartMetric === 'revenue' ? financials?.prior_ttm?.revenue
                    : chartMetric === 'net_income' ? financials?.prior_ttm?.net_income
                    : chartMetric === 'free_cash_flow' ? financials?.prior_ttm?.free_cash_flow
                    : chartMetric === 'cash' ? financials?.prior_ttm?.cash
                    : financials?.prior_ttm?.total_debt;
                  
                  // Use USER'S 6 RULES for Net Income & FCF
                  const appearance = isProfitLossMetric 
                    ? getCellAppearance(ttmValue ?? null, priorTtmValue ?? null)
                    : { backgroundColor: null, textColor: COLORS.text, label: null, isWhiteText: false };
                  
                  // Calculate YoY%
                  let yoyPct: number | null = null;
                  let displayLabel = '—';
                  
                  if (ttmValue !== null && ttmValue !== undefined && priorTtmValue !== null && priorTtmValue !== undefined && priorTtmValue !== 0) {
                    yoyPct = (ttmValue - priorTtmValue) / Math.abs(priorTtmValue);
                    displayLabel = yoyPct >= 0 ? `+${(yoyPct * 100).toFixed(1)}%` : `${(yoyPct * 100).toFixed(1)}%`;
                  }
                  
                  // Override with special labels from 6 rules
                  if (isProfitLossMetric && appearance.label) {
                    displayLabel = appearance.label;
                  }
                  
                  // Determine bar color and text color
                  let barColor: string | null = null;
                  let textColor: string = COLORS.textMuted;
                  
                  if (isProfitLossMetric && appearance.backgroundColor) {
                    barColor = appearance.backgroundColor;
                    textColor = appearance.textColor;
                  } else if (isDebtMetric && ttmValue !== null && ttmValue !== undefined && priorTtmValue !== null && priorTtmValue !== undefined) {
                    const isGood = ttmValue <= priorTtmValue;
                    barColor = isGood ? COLORS.positive : COLORS.negative;
                    textColor = isGood ? COLORS.positive : COLORS.negative;
                  } else if (yoyPct !== null) {
                    barColor = yoyPct >= 0 ? COLORS.positive : COLORS.negative;
                    textColor = yoyPct >= 0 ? COLORS.positive : COLORS.negative;
                  }
                  
                  return (
                    <View style={styles.barColumn}>
                      <Text style={[styles.barLabel, { color: textColor }]} numberOfLines={1}>
                        {displayLabel}
                      </Text>
                      <View style={[
                        styles.bar,
                        styles.ttmBar,
                        { height: Math.max(Math.min(Math.abs(yoyPct || 0) * 200, 60), 16) },
                        barColor ? { backgroundColor: barColor } : styles.barNA,
                      ]} />
                      <Text style={[styles.barPeriod, styles.ttmLabel]}>TTM</Text>
                    </View>
                  );
                })()}
              </View>
            </View>
            
            {/* Legend - only show for NI/FCF (profit/loss metrics) */}
            {(chartMetric === 'net_income' || chartMetric === 'free_cash_flow') && (
              <View style={styles.legendRow}>
                <View style={styles.legendItem}>
                  <View style={[styles.legendDot, { backgroundColor: CELL_COLORS.darkRed }]} />
                  <Text style={styles.legendText}>Loss worse</Text>
                </View>
                <View style={styles.legendItem}>
                  <View style={[styles.legendDot, { backgroundColor: CELL_COLORS.lightRed }]} />
                  <Text style={styles.legendText}>Loss better</Text>
                </View>
                <View style={styles.legendItem}>
                  <View style={[styles.legendDot, { backgroundColor: CELL_COLORS.lightGreen }]} />
                  <Text style={styles.legendText}>Profit down</Text>
                </View>
                <View style={styles.legendItem}>
                  <View style={[styles.legendDot, { backgroundColor: CELL_COLORS.darkGreen }]} />
                  <Text style={styles.legendText}>Profit up</Text>
                </View>
              </View>
            )}
            
            {/* Debt tooltip hint */}
            {chartMetric === 'total_debt' && (
              <Text style={styles.debtHint}>Lower is better (green = debt decreased)</Text>
            )}
          </View>
          
          {/* Dropdown Modal */}
          <Modal visible={dropdownOpen} transparent animationType="fade">
            <Pressable style={styles.dropdownOverlay} onPress={() => setDropdownOpen(false)}>
              <View style={styles.dropdownMenu}>
                {METRIC_OPTIONS.map(({ key, label }) => (
                  <TouchableOpacity
                    key={key}
                    style={[styles.dropdownItem, chartMetric === key && styles.dropdownItemActive]}
                    onPress={() => { setChartMetric(key); setDropdownOpen(false); }}
                  >
                    {chartMetric === key && <Ionicons name="checkmark" size={16} color={COLORS.primary} />}
                    <Text style={[styles.dropdownItemText, chartMetric === key && styles.dropdownItemTextActive]}>
                      {label}
                    </Text>
                  </TouchableOpacity>
                ))}
              </View>
            </Pressable>
          </Modal>
          
          {/* Core 5 Metrics Table */}
          <View style={styles.tableSection}>
            <Text style={styles.tableTitle}>Core 5 Metrics</Text>
            
            {/* Table Header */}
            <View style={styles.tableHeader}>
              <View style={styles.tableMetricCell}><Text style={styles.headerText}>Metric</Text></View>
              {periods.map(p => (
                <View key={p.period_date} style={styles.tableHeaderCell}>
                  <Text style={styles.headerText}>{formatPeriodLabel(p.period_date, period === 'quarterly')}</Text>
                </View>
              ))}
              <View style={[styles.tableHeaderCell, styles.ttmHeaderCell]}>
                <Text style={[styles.headerText, styles.ttmHeaderText]}>TTM</Text>
              </View>
            </View>
            
            {/* Revenue Row - NO color rules (always positive) */}
            <View style={styles.tableRow}>
              <View style={styles.tableMetricCell}><Text style={styles.metricName}>Revenue</Text></View>
              {periods.map((p, displayIndex) => {
                const rawPeriods = period === 'quarterly' ? financials.quarterly : financials.annual;
                const originalIndex = periods.length - 1 - displayIndex;
                const priorIndex = period === 'quarterly' ? originalIndex + 4 : originalIndex + 1;
                const priorPeriod = rawPeriods[priorIndex];
                const currentLabel = formatPeriodLabel(p.period_date, period === 'quarterly');
                const priorLabel = priorPeriod ? formatPeriodLabel(priorPeriod.period_date, period === 'quarterly') : 'N/A';
                
                return (
                  <React.Fragment key={p.period_date}>
                    {renderTableCell(
                      p.revenue, 
                      priorPeriod?.revenue ?? null, 
                      currentLabel,
                      priorLabel,
                      'Revenue',
                      true  // Apply 6 color rules
                    )}
                  </React.Fragment>
                );
              })}
              {renderTTMCell(
                financials.ttm.revenue, 
                financials.prior_ttm?.revenue ?? null, 
                'Revenue',
                true  // Apply 6 color rules
              )}
            </View>
            
            {/* Net Income Row - WITH 6 color rules */}
            <View style={styles.tableRow}>
              <View style={styles.tableMetricCell}><Text style={styles.metricName}>Net Income</Text></View>
              {periods.map((p, displayIndex) => {
                const rawPeriods = period === 'quarterly' ? financials.quarterly : financials.annual;
                const originalIndex = periods.length - 1 - displayIndex;
                const priorIndex = period === 'quarterly' ? originalIndex + 4 : originalIndex + 1;
                const priorPeriod = rawPeriods[priorIndex];
                const currentLabel = formatPeriodLabel(p.period_date, period === 'quarterly');
                const priorLabel = priorPeriod ? formatPeriodLabel(priorPeriod.period_date, period === 'quarterly') : 'N/A';
                
                return (
                  <React.Fragment key={p.period_date}>
                    {renderTableCell(
                      p.net_income, 
                      priorPeriod?.net_income ?? null, 
                      currentLabel,
                      priorLabel,
                      'Net Income',
                      true  // Apply 6 color rules
                    )}
                  </React.Fragment>
                );
              })}
              {renderTTMCell(
                financials.ttm.net_income, 
                financials.prior_ttm?.net_income ?? null, 
                'Net Income',
                true  // Apply 6 color rules
              )}
            </View>
            
            {/* FCF Row - WITH 6 color rules */}
            <View style={styles.tableRow}>
              <View style={styles.tableMetricCell}><Text style={styles.metricName}>Free Cash Flow</Text></View>
              {periods.map((p, displayIndex) => {
                const rawPeriods = period === 'quarterly' ? financials.quarterly : financials.annual;
                const originalIndex = periods.length - 1 - displayIndex;
                const priorIndex = period === 'quarterly' ? originalIndex + 4 : originalIndex + 1;
                const priorPeriod = rawPeriods[priorIndex];
                const currentLabel = formatPeriodLabel(p.period_date, period === 'quarterly');
                const priorLabel = priorPeriod ? formatPeriodLabel(priorPeriod.period_date, period === 'quarterly') : 'N/A';
                
                return (
                  <React.Fragment key={p.period_date}>
                    {renderTableCell(
                      p.free_cash_flow, 
                      priorPeriod?.free_cash_flow ?? null, 
                      currentLabel,
                      priorLabel,
                      'Free Cash Flow',
                      true  // Apply 6 color rules
                    )}
                  </React.Fragment>
                );
              })}
              {renderTTMCell(
                financials.ttm.free_cash_flow, 
                financials.prior_ttm?.free_cash_flow ?? null, 
                'Free Cash Flow',
                true  // Apply 6 color rules
              )}
            </View>
            
            {/* Cash Row - WITH 6 color rules */}
            <View style={styles.tableRow}>
              <View style={styles.tableMetricCell}><Text style={styles.metricName}>Cash</Text></View>
              {periods.map((p, displayIndex) => {
                const rawPeriods = period === 'quarterly' ? financials.quarterly : financials.annual;
                const originalIndex = periods.length - 1 - displayIndex;
                const priorIndex = period === 'quarterly' ? originalIndex + 4 : originalIndex + 1;
                const priorPeriod = rawPeriods[priorIndex];
                const currentLabel = formatPeriodLabel(p.period_date, period === 'quarterly');
                const priorLabel = priorPeriod ? formatPeriodLabel(priorPeriod.period_date, period === 'quarterly') : 'N/A';
                
                return (
                  <React.Fragment key={p.period_date}>
                    {renderTableCell(
                      p.cash, 
                      priorPeriod?.cash ?? null, 
                      currentLabel,
                      priorLabel,
                      'Cash',
                      true  // Apply 6 color rules
                    )}
                  </React.Fragment>
                );
              })}
              {/* Cash TTM - In USD mode show value, in % mode show "—" with tooltip */}
              <TouchableOpacity
                style={[styles.tableValueCell, styles.ttmCell]}
                onPress={() => showDetailPopup(
                  'Cash (TTM)',
                  'TTM',
                  financials.ttm.cash,
                  displayMode === 'pct' ? 'Not applicable' : 'Prior TTM',
                  null
                )}
                activeOpacity={0.7}
              >
                <Text style={[
                  displayMode === 'usd' ? styles.cellValueUSD : styles.cellValuePct,
                  displayMode === 'pct' && styles.textMuted
                ]}>
                  {displayMode === 'usd' ? formatCurrency(financials.ttm.cash) : '—'}
                </Text>
              </TouchableOpacity>
            </View>
            
            {/* Debt Row - INVERTED 6 color rules (decrease = good = green) */}
            <View style={styles.tableRow}>
              <View style={styles.tableMetricCell}><Text style={styles.metricName}>Total Debt</Text></View>
              {periods.map((p, displayIndex) => {
                const rawPeriods = period === 'quarterly' ? financials.quarterly : financials.annual;
                const originalIndex = periods.length - 1 - displayIndex;
                const priorIndex = period === 'quarterly' ? originalIndex + 4 : originalIndex + 1;
                const priorPeriod = rawPeriods[priorIndex];
                const currentLabel = formatPeriodLabel(p.period_date, period === 'quarterly');
                const priorLabel = priorPeriod ? formatPeriodLabel(priorPeriod.period_date, period === 'quarterly') : 'N/A';
                
                return (
                  <React.Fragment key={p.period_date}>
                    {renderTableCell(
                      p.total_debt, 
                      priorPeriod?.total_debt ?? null, 
                      currentLabel,
                      priorLabel,
                      'Total Debt',
                      true,  // Apply color rules
                      true   // isDebt = INVERTED logic
                    )}
                  </React.Fragment>
                );
              })}
              {/* Total Debt TTM - In USD mode show value, in % mode show "—" with tooltip */}
              <TouchableOpacity
                style={[styles.tableValueCell, styles.ttmCell]}
                onPress={() => showDetailPopup(
                  'Total Debt (TTM)',
                  'TTM',
                  financials.ttm.total_debt,
                  displayMode === 'pct' ? 'Not applicable' : 'Prior TTM',
                  null
                )}
                activeOpacity={0.7}
              >
                <Text style={[
                  displayMode === 'usd' ? styles.cellValueUSD : styles.cellValuePct,
                  displayMode === 'pct' && styles.textMuted
                ]}>
                  {displayMode === 'usd' ? formatCurrency(financials.ttm.total_debt) : '—'}
                </Text>
              </TouchableOpacity>
            </View>
          </View>
        </>
      )}
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    // No margin - handled by parent sectionCard in [ticker].tsx
  },
  // Header matches collapsibleHeader from [ticker].tsx
  header: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    paddingVertical: 4,
  },
  headerLeft: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
  },
  title: {
    fontSize: 15,
    fontWeight: '700',
    color: COLORS.text,
  },
  sectionIcon: {
    fontSize: 16,
  },
  // Unified pill styling (matches [ticker].tsx summaryPill)
  pill: {
    backgroundColor: '#F3F4F6',
    paddingHorizontal: 8,
    paddingVertical: 3,
    borderRadius: 12,
    borderWidth: 1,
    borderColor: '#E5E7EB',
  },
  pillPositive: { 
    backgroundColor: '#F0FDF4', 
    borderColor: '#BBF7D0',
  },
  pillNegative: { 
    backgroundColor: '#FEF2F2', 
    borderColor: '#FECACA',
  },
  pillNeutral: { 
    backgroundColor: '#F3F4F6', 
    borderColor: '#E5E7EB',
  },
  pillText: { fontSize: 11, fontWeight: '500', color: '#6B7280' },
  pillTextPositive: { color: '#16A34A' },
  pillTextNegative: { color: '#DC2626' },
  noDataState: {
    paddingTop: 8,
    paddingBottom: 4,
  },
  noDataText: {
    fontSize: 13,
    color: COLORS.textMuted,
  },
  switcherRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    paddingHorizontal: 16,
    paddingBottom: 8,
  },
  periodSwitcher: {
    flexDirection: 'row',
    backgroundColor: COLORS.neutral,
    borderRadius: 8,
    padding: 2,
  },
  switchBtn: {
    paddingHorizontal: 12,
    paddingVertical: 6,
    borderRadius: 6,
  },
  switchBtnActive: { backgroundColor: COLORS.card },
  switchText: { fontSize: 13, color: COLORS.textMuted, fontWeight: '500' },
  switchTextActive: { color: COLORS.text },
  modeSwitcher: {
    flexDirection: 'row',
    backgroundColor: COLORS.neutral,
    borderRadius: 6,
    padding: 2,
  },
  modeBtn: {
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 4,
  },
  modeBtnActive: { backgroundColor: COLORS.primary },
  modeText: { fontSize: 12, color: COLORS.textMuted, fontWeight: '600' },
  modeTextActive: { color: '#FFFFFF' },
  hintText: {
    fontSize: 11,
    color: COLORS.textMuted,
    textAlign: 'center',
    marginBottom: 8,
    fontStyle: 'italic',
  },
  chartSection: {
    paddingHorizontal: 16,
    paddingBottom: 16,
  },
  chartHeader: {
    flexDirection: 'row',
    justifyContent: 'flex-start',
    alignItems: 'center',
    marginBottom: 8,
  },
  chartTitleBtn: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
  },
  chartTitle: {
    fontSize: 13,
    fontWeight: '600',
    color: COLORS.textLight,
  },
  // Legend styles
  legendRow: {
    flexDirection: 'row',
    justifyContent: 'center',
    gap: 12,
    marginTop: 12,
    paddingVertical: 4,
  },
  legendItem: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
  },
  legendDot: {
    width: 10,
    height: 10,
    borderRadius: 2,
  },
  legendText: {
    fontSize: 9,
    color: COLORS.textMuted,
  },
  debtHint: {
    fontSize: 10,
    color: COLORS.textMuted,
    textAlign: 'center',
    marginTop: 8,
    fontStyle: 'italic',
  },
  // Dropdown styles
  dropdownOverlay: {
    flex: 1,
    backgroundColor: 'rgba(0, 0, 0, 0.3)',
    justifyContent: 'flex-start',
    paddingTop: 200,
    paddingHorizontal: 16,
  },
  dropdownMenu: {
    backgroundColor: COLORS.card,
    borderRadius: 12,
    paddingVertical: 8,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 4 },
    shadowOpacity: 0.15,
    shadowRadius: 12,
    elevation: 8,
  },
  dropdownItem: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingVertical: 12,
    paddingHorizontal: 16,
    gap: 8,
  },
  dropdownItemActive: {
    backgroundColor: 'rgba(37, 99, 235, 0.08)',
  },
  dropdownItemText: {
    fontSize: 15,
    color: COLORS.text,
  },
  dropdownItemTextActive: {
    color: COLORS.primary,
    fontWeight: '600',
  },
  chartWrapper: { paddingTop: 24 },
  chartContainer: {
    flexDirection: 'row',
    justifyContent: 'space-around',
    alignItems: 'flex-end',
    height: 100,
  },
  barColumn: {
    alignItems: 'center',
    flex: 1,
    maxWidth: 60,
  },
  barLabel: {
    fontSize: 10,
    fontWeight: '600',
    marginBottom: 4,
    textAlign: 'center',
  },
  labelPositive: { color: COLORS.positive },
  labelNegative: { color: COLORS.negative },
  bar: {
    width: 28,
    borderRadius: 4,
    minHeight: 16,
  },
  barPositive: { backgroundColor: COLORS.positive },
  barNegative: { backgroundColor: COLORS.negative },
  barNA: { backgroundColor: COLORS.neutral },
  ttmBar: {
    borderWidth: 2,
    borderColor: COLORS.ttmBar,
    backgroundColor: 'rgba(59, 130, 246, 0.15)',
  },
  barPeriod: {
    fontSize: 10,
    color: COLORS.textMuted,
    marginTop: 4,
  },
  ttmLabel: {
    color: COLORS.ttmBar,
    fontWeight: '600',
  },
  tableSection: {
    paddingHorizontal: 16,
    paddingBottom: 16,
  },
  tableTitle: {
    fontSize: 13,
    fontWeight: '600',
    color: COLORS.textLight,
    marginBottom: 8,
  },
  tableHeader: {
    flexDirection: 'row',
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
    paddingBottom: 8,
  },
  tableMetricCell: {
    flex: 1.5,
    minWidth: 90,
  },
  tableHeaderCell: {
    flex: 1,
    alignItems: 'center',
  },
  ttmHeaderCell: {
    backgroundColor: 'rgba(59, 130, 246, 0.05)',
    borderRadius: 4,
    paddingVertical: 2,
  },
  headerText: {
    fontSize: 10,
    fontWeight: '600',
    color: COLORS.textMuted,
  },
  ttmHeaderText: { color: COLORS.ttmBar },
  tableRow: {
    flexDirection: 'row',
    paddingVertical: 10,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
    alignItems: 'center',
  },
  metricName: {
    fontSize: 12,
    fontWeight: '500',
    color: COLORS.text,
  },
  tableValueCell: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    paddingHorizontal: 2,
    paddingVertical: 4,
    borderRadius: 4,
    marginHorizontal: 1,
  },
  ttmCell: {
    backgroundColor: 'rgba(59, 130, 246, 0.08)',
  },
  // TTM cell variants for semantic status (↗ Profit, ↘ Loss)
  ttmCellPositive: {
    backgroundColor: COLORS.positiveLight,
    borderWidth: 2,
    borderColor: COLORS.ttmBar,
  },
  ttmCellNegative: {
    backgroundColor: COLORS.negativeLight,
    borderWidth: 2,
    borderColor: COLORS.ttmBar,
  },
  cellPositive: { backgroundColor: COLORS.positiveLight },
  cellNegative: { backgroundColor: COLORS.negativeLight },
  cellValueUSD: {
    fontSize: 11,
    fontWeight: '500',
    color: COLORS.text,
    textAlign: 'center',
  },
  cellValuePct: {
    fontSize: 12,
    fontWeight: '600',
    textAlign: 'center',
  },
  textPositive: { color: COLORS.positive },
  textNegative: { color: COLORS.negative },
  textMuted: { color: COLORS.textMuted },
  // Overlay for modals
  tooltipOverlay: {
    flex: 1,
    backgroundColor: 'rgba(0,0,0,0.5)',
    justifyContent: 'center',
    alignItems: 'center',
  },
  // Detail popup - shows PREVIOUS and CURRENT period + value
  detailPopupBox: {
    backgroundColor: COLORS.tooltipBg,
    paddingHorizontal: 24,
    paddingVertical: 20,
    borderRadius: 16,
    alignItems: 'center',
    minWidth: 220,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 8 },
    shadowOpacity: 0.3,
    shadowRadius: 16,
    elevation: 10,
  },
  detailPopupTitle: {
    fontSize: 14,
    fontWeight: '700',
    color: '#FFFFFF',
    marginBottom: 16,
  },
  detailRow: {
    alignItems: 'center',
    paddingVertical: 8,
  },
  detailPeriodLabel: {
    fontSize: 11,
    color: '#94A3B8',
    marginBottom: 4,
  },
  detailValue: {
    fontSize: 18,
    fontWeight: '700',
    color: '#94A3B8',
  },
  detailValueCurrent: {
    color: '#FFFFFF',
  },
  detailHint: {
    fontSize: 10,
    color: '#64748B',
    marginTop: 12,
  },
  detailExplanation: {
    fontSize: 12,
    color: '#94A3B8',
    textAlign: 'center',
    marginBottom: 16,
    paddingHorizontal: 8,
    lineHeight: 18,
  },
});

export default FinancialHub;
