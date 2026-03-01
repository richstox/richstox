import React, { useState, useEffect } from 'react';
import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  TextInput,
  TouchableOpacity,
  Image,
  Dimensions,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useRouter } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import { LineChart } from 'react-native-gifted-charts';

const { width } = Dimensions.get('window');

// Brand colors
const COLORS = {
  primary: '#4F46E5', // Indigo
  secondary: '#7C3AED', // Purple
  background: '#FFFFFF',
  surface: '#F9FAFB',
  text: '#111827',
  textLight: '#6B7280',
  textMuted: '#9CA3AF',
  success: '#10B981',
  warning: '#F59E0B',
  danger: '#EF4444',
  border: '#E5E7EB',
  gold: '#F59E0B',
};

// Sample sectors
const SECTORS = [
  { name: 'Technology', icon: 'laptop-outline' },
  { name: 'Healthcare', icon: 'medical-outline' },
  { name: 'Financial', icon: 'card-outline' },
  { name: 'Industrials', icon: 'construct-outline' },
  { name: 'Energy', icon: 'flash-outline' },
];

// Sample simulator data
const SIMULATOR_DATA = {
  ticker: 'AAPL',
  name: 'Apple Inc',
  exchange: 'NASDAQ',
  ipo: '1980',
  oneTime: {
    initial: 12206,
    final: 48286,
    profit: 36080,
    profitPct: 295,
    annualized: 14.6,
    maxDrawdown: -38.5,
    maxDrawdownAmt: 1999,
    dividends: 1220,
  },
};

// Magnificent 7 portfolio data
const MAG7_DATA = {
  name: 'Magnificent 7 Portfolio',
  description: 'Equal-weight simulation of Apple, Microsoft, Google, Amazon, Nvidia, Meta, and Tesla',
  initial: 10000,
  final: 502113,
  profit: 492113,
  profitPct: 4921,
  annualized: 48,
  maxDrawdown: -56.5,
  maxDrawdownAmt: 150790,
  vsSpy: 4604,
  dividends: 1761,
};

// Pricing plans
const PRICING = [
  {
    name: 'FREE',
    price: '$0',
    period: '',
    features: [
      'Company fundamentals, dividends',
      'Daily price chart (full history)',
      'Watchlist + news/events',
      'Investment simulator (historical)',
      'S&P 500 TR benchmark',
      '1 Practice portfolio (max 7 positions)',
    ],
    cta: 'Start Free',
    highlighted: false,
  },
  {
    name: 'PRO',
    price: '$9.99',
    period: '/ month',
    features: [
      'Everything in FREE',
      '10 portfolios total',
      'Backdated Tracking portfolios',
      'Backtest: simulated history',
      'Portfolio review tools',
      'S&P 500 TR benchmarks (full)',
      'Public portfolios with trade log',
    ],
    cta: 'Continue to checkout',
    highlighted: true,
  },
  {
    name: 'PRO+',
    price: '$19.99',
    period: '/ month',
    icon: 'sparkles',
    features: [
      'Everything in PRO',
      'RICHIE AI assistant (chat)',
      'AI company summaries',
      'AI dashboard insights',
      'All future AI features',
    ],
    cta: 'Continue to checkout',
    highlighted: false,
  },
];

export default function LandingPage() {
  const router = useRouter();
  const [searchQuery, setSearchQuery] = useState('');
  const [timeHorizon, setTimeHorizon] = useState('10Y');
  const [contribution, setContribution] = useState(100);

  // Sample chart data for Mag7
  const chartData = [
    { value: 10000 },
    { value: 15000 },
    { value: 12000 },
    { value: 25000 },
    { value: 35000 },
    { value: 28000 },
    { value: 45000 },
    { value: 80000 },
    { value: 120000 },
    { value: 180000 },
    { value: 250000 },
    { value: 320000 },
    { value: 380000 },
    { value: 450000 },
    { value: 502113 },
  ];

  const spyData = [
    { value: 10000 },
    { value: 11000 },
    { value: 10500 },
    { value: 12000 },
    { value: 13500 },
    { value: 12800 },
    { value: 15000 },
    { value: 18000 },
    { value: 22000 },
    { value: 26000 },
    { value: 30000 },
    { value: 35000 },
    { value: 40000 },
    { value: 45000 },
    { value: 48000 },
  ];

  return (
    <SafeAreaView style={styles.container}>
      <ScrollView showsVerticalScrollIndicator={false}>
        {/* Header */}
        <View style={styles.header}>
          <View style={styles.logoContainer}>
            <View style={styles.logoIcon}>
              <Ionicons name="trending-up" size={20} color="#fff" />
            </View>
            <Text style={styles.logoText}>RICHSTOX</Text>
          </View>
          <View style={styles.headerRight}>
            <TouchableOpacity style={styles.viewAsBtn}>
              <Ionicons name="eye-outline" size={16} color={COLORS.textLight} />
              <Text style={styles.viewAsText}>View as</Text>
            </TouchableOpacity>
            <TouchableOpacity style={styles.proBtn}>
              <Ionicons name="sparkles" size={14} color="#fff" />
              <Text style={styles.proBtnText}>PRO+</Text>
            </TouchableOpacity>
          </View>
        </View>

        {/* Hero Section */}
        <View style={styles.heroSection}>
          <View style={styles.heroIcon}>
            <Ionicons name="trending-up" size={32} color={COLORS.primary} />
          </View>
          <Text style={styles.heroTitle}>Find companies you{'\n'}understand</Text>
          <Text style={styles.heroSubtitle}>
            Verified fundamentals, dividends, and valuation — with clear risk context.
          </Text>

          {/* Search */}
          <View style={styles.searchContainer}>
            <Ionicons name="search" size={20} color={COLORS.textMuted} />
            <TextInput
              style={styles.searchInput}
              placeholder="Search a company (e.g. Microsoft, Nestle)"
              placeholderTextColor={COLORS.textMuted}
              value={searchQuery}
              onChangeText={setSearchQuery}
            />
          </View>
          <TouchableOpacity 
            style={styles.searchBtn}
            onPress={() => router.push('/stock/AAPL')}
          >
            <Text style={styles.searchBtnText}>Search</Text>
          </TouchableOpacity>

          {/* Sectors */}
          <Text style={styles.browseSectors}>Browse by sector</Text>
          <ScrollView horizontal showsHorizontalScrollIndicator={false} style={styles.sectorsScroll}>
            {SECTORS.map((sector, index) => (
              <TouchableOpacity key={index} style={styles.sectorChip}>
                <Ionicons name={sector.icon as any} size={14} color={COLORS.textLight} />
                <Text style={styles.sectorChipText}>{sector.name}</Text>
              </TouchableOpacity>
            ))}
            <TouchableOpacity style={styles.sectorChip}>
              <Text style={styles.sectorChipText}>View all sectors →</Text>
            </TouchableOpacity>
          </ScrollView>
        </View>

        {/* Simulator Preview */}
        <View style={styles.simulatorCard}>
          <Text style={styles.cardLabel}>Company</Text>
          <View style={styles.companyRow}>
            <View style={styles.companyLogo}>
              <Ionicons name="logo-apple" size={24} color="#000" />
            </View>
            <View>
              <Text style={styles.companyName}>{SIMULATOR_DATA.name}</Text>
              <Text style={styles.companyMeta}>
                {SIMULATOR_DATA.ticker} • {SIMULATOR_DATA.exchange} • IPO {SIMULATOR_DATA.ipo}
              </Text>
            </View>
          </View>

          {/* Investment Type Toggle */}
          <View style={styles.toggleContainer}>
            <TouchableOpacity style={[styles.toggleBtn, styles.toggleBtnActive]}>
              <Ionicons name="arrow-forward" size={14} color={COLORS.primary} />
              <Text style={[styles.toggleText, styles.toggleTextActive]}>One-time</Text>
            </TouchableOpacity>
            <TouchableOpacity style={styles.toggleBtn}>
              <Ionicons name="repeat" size={14} color={COLORS.textMuted} />
              <Text style={styles.toggleText}>Recurring</Text>
            </TouchableOpacity>
          </View>

          <Text style={styles.compareText}>
            Compare outcome + max drawdown (profit + pain).
          </Text>

          {/* Time Horizon */}
          <Text style={styles.inputLabel}>Time horizon</Text>
          <View style={styles.horizonRow}>
            {['5Y', '10Y', 'MAX'].map((h) => (
              <TouchableOpacity
                key={h}
                style={[styles.horizonBtn, timeHorizon === h && styles.horizonBtnActive]}
                onPress={() => setTimeHorizon(h)}
              >
                <Text style={[styles.horizonText, timeHorizon === h && styles.horizonTextActive]}>
                  {h}
                </Text>
              </TouchableOpacity>
            ))}
          </View>

          {/* Results */}
          <View style={styles.resultsGrid}>
            <View style={styles.resultItem}>
              <Text style={styles.resultLabel}>Invested → Ending Value</Text>
              <Text style={styles.resultValue}>
                ${SIMULATOR_DATA.oneTime.initial.toLocaleString()} → ${SIMULATOR_DATA.oneTime.final.toLocaleString()}
              </Text>
            </View>
            <View style={styles.resultItem}>
              <Text style={styles.resultLabel}>Profit / Loss (+{SIMULATOR_DATA.oneTime.profitPct}%)</Text>
              <Text style={[styles.resultValue, styles.successText]}>
                +${SIMULATOR_DATA.oneTime.profit.toLocaleString()}
              </Text>
            </View>
            <View style={styles.resultItem}>
              <Text style={styles.resultLabel}>Annualized (p.a.)</Text>
              <Text style={[styles.resultValue, styles.successText]}>
                +{SIMULATOR_DATA.oneTime.annualized}%
              </Text>
            </View>
            <View style={styles.resultItem}>
              <Text style={styles.resultLabel}>⚠️ Max. Drawdown</Text>
              <Text style={[styles.resultValue, styles.dangerText]}>
                {SIMULATOR_DATA.oneTime.maxDrawdown}% (${SIMULATOR_DATA.oneTime.maxDrawdownAmt.toLocaleString()})
              </Text>
            </View>
            <View style={styles.resultItem}>
              <Text style={styles.resultLabel}>💰 Dividends received</Text>
              <Text style={[styles.resultValue, styles.successText]}>
                ${SIMULATOR_DATA.oneTime.dividends.toLocaleString()}
              </Text>
            </View>
          </View>

          <Text style={styles.ctaText}>Create a free account to simulate any company.</Text>
          <TouchableOpacity style={styles.googleBtn}>
            <Ionicons name="logo-google" size={18} color="#fff" />
            <Text style={styles.googleBtnText}>Start FREE with Google</Text>
          </TouchableOpacity>
        </View>

        {/* Model Portfolios Section */}
        <View style={styles.section}>
          <Text style={styles.sectionTitle}>Model portfolios before you invest</Text>
          <Text style={styles.sectionSubtitle}>
            Practice, track, or publish portfolios — with the same risk-first stats.
          </Text>

          <View style={styles.portfolioCard}>
            <Text style={styles.portfolioName}>{MAG7_DATA.name}</Text>
            <Text style={styles.portfolioDesc}>{MAG7_DATA.description} — compared to the S&P 500 Total Return.</Text>

            {/* Time Horizon */}
            <View style={styles.horizonRow}>
              {['5Y', '10Y', 'MAX'].map((h) => (
                <TouchableOpacity
                  key={h}
                  style={[styles.horizonBtn, h === '10Y' && styles.horizonBtnActive]}
                >
                  <Text style={[styles.horizonText, h === '10Y' && styles.horizonTextActive]}>
                    {h}
                  </Text>
                </TouchableOpacity>
              ))}
            </View>

            {/* Portfolio Results */}
            <View style={styles.portfolioResults}>
              <View style={styles.portfolioResultItem}>
                <Text style={styles.portfolioResultLabel}>Starting → Ending Value (simulation)</Text>
                <Text style={styles.portfolioResultValue}>
                  ${MAG7_DATA.initial.toLocaleString()} → ${MAG7_DATA.final.toLocaleString()}
                </Text>
              </View>
              <View style={styles.portfolioResultItem}>
                <Text style={styles.portfolioResultLabel}>Profit / Loss (+{MAG7_DATA.profitPct}%)</Text>
                <Text style={[styles.portfolioResultValue, styles.successText]}>
                  +${MAG7_DATA.profit.toLocaleString()}
                </Text>
              </View>
              <View style={styles.portfolioResultItem}>
                <Text style={styles.portfolioResultLabel}>Annualized (p.a.)</Text>
                <Text style={[styles.portfolioResultValue, styles.successText]}>
                  +{MAG7_DATA.annualized}%
                </Text>
              </View>
              <View style={styles.portfolioResultItem}>
                <Text style={styles.portfolioResultLabel}>⚠️ Max drawdown</Text>
                <Text style={[styles.portfolioResultValue, styles.dangerText]}>
                  {MAG7_DATA.maxDrawdown}% (${MAG7_DATA.maxDrawdownAmt.toLocaleString()})
                </Text>
              </View>
              <View style={styles.portfolioResultItem}>
                <Text style={styles.portfolioResultLabel}>📊 vs S&P 500 TR</Text>
                <Text style={[styles.portfolioResultValue, styles.successText]}>
                  +{MAG7_DATA.vsSpy}%
                </Text>
              </View>
              <View style={styles.portfolioResultItem}>
                <Text style={styles.portfolioResultLabel}>💰 Dividends received</Text>
                <Text style={[styles.portfolioResultValue, styles.successText]}>
                  ${MAG7_DATA.dividends.toLocaleString()}
                </Text>
              </View>
            </View>

            {/* Chart */}
            <View style={styles.chartContainer}>
              <LineChart
                data={chartData}
                data2={spyData}
                width={width - 80}
                height={150}
                color1={COLORS.primary}
                color2={COLORS.textMuted}
                thickness={2}
                hideDataPoints
                hideRules
                hideYAxisText
                yAxisColor="transparent"
                xAxisColor={COLORS.border}
                curved
                areaChart
                startFillColor1={COLORS.primary}
                endFillColor1="white"
                startOpacity1={0.2}
                endOpacity1={0}
              />
              <View style={styles.chartLegend}>
                <View style={styles.legendItem}>
                  <View style={[styles.legendDot, { backgroundColor: COLORS.primary }]} />
                  <Text style={styles.legendText}>Magnificent 7 Portfolio</Text>
                </View>
                <View style={styles.legendItem}>
                  <View style={[styles.legendDot, { backgroundColor: COLORS.textMuted }]} />
                  <Text style={styles.legendText}>S&P 500 TR</Text>
                </View>
              </View>
            </View>
          </View>
        </View>

        {/* Pricing Section */}
        <View style={styles.pricingSection}>
          {PRICING.map((plan, index) => (
            <View 
              key={index} 
              style={[
                styles.pricingCard, 
                plan.highlighted && styles.pricingCardHighlighted
              ]}
            >
              <View style={styles.pricingHeader}>
                {plan.icon && (
                  <Ionicons name={plan.icon as any} size={16} color={COLORS.gold} />
                )}
                <Text style={[styles.planName, plan.name === 'PRO' && styles.proPlanName]}>
                  {plan.name}
                </Text>
              </View>
              <View style={styles.priceRow}>
                <Text style={styles.price}>{plan.price}</Text>
                <Text style={styles.pricePeriod}>{plan.period}</Text>
              </View>
              <Text style={styles.planDesc}>
                {plan.name === 'FREE' && 'Best for research and learning the workflow.'}
                {plan.name === 'PRO' && 'Best for disciplined tracking and portfolio review.'}
                {plan.name === 'PRO+' && 'Best for faster explanations and AI help.'}
              </Text>
              {plan.features.map((feature, i) => (
                <View key={i} style={styles.featureRow}>
                  <Ionicons 
                    name="checkmark" 
                    size={16} 
                    color={plan.name === 'PRO+' ? COLORS.gold : COLORS.success} 
                  />
                  <Text style={styles.featureText}>{feature}</Text>
                </View>
              ))}
              <TouchableOpacity 
                style={[
                  styles.ctaBtn,
                  plan.highlighted && styles.ctaBtnHighlighted
                ]}
              >
                <Text style={[
                  styles.ctaBtnText,
                  plan.highlighted && styles.ctaBtnTextHighlighted
                ]}>
                  {plan.cta}
                </Text>
              </TouchableOpacity>
            </View>
          ))}
        </View>

        {/* Disclaimer */}
        <Text style={styles.disclaimer}>
          PRO and PRO+ don't promise better returns. They unlock more structure, history, and evidence.
        </Text>

        {/* Footer */}
        <View style={styles.footer}>
          <View style={styles.footerLogo}>
            <View style={styles.logoIcon}>
              <Ionicons name="trending-up" size={16} color="#fff" />
            </View>
            <Text style={styles.footerLogoText}>RICHSTOX</Text>
          </View>
          <Text style={styles.footerTagline}>Verify before you invest.</Text>
          <View style={styles.footerLinks}>
            <Text style={styles.footerLink}>Privacy</Text>
            <Text style={styles.footerLink}>Terms</Text>
            <Text style={styles.footerLink}>Contact</Text>
            <Text style={styles.footerLink}>Pricing</Text>
          </View>
          <Text style={styles.footerDisclaimer}>
            Richstox is a tracking and learning platform. Not a broker. Not financial advice.
          </Text>
          <Text style={styles.copyright}>© 2026 Richstox. All rights reserved.</Text>
        </View>
      </ScrollView>
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
    justifyContent: 'space-between',
    alignItems: 'center',
    paddingHorizontal: 16,
    paddingVertical: 12,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  logoContainer: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  logoIcon: {
    width: 28,
    height: 28,
    borderRadius: 6,
    backgroundColor: COLORS.primary,
    alignItems: 'center',
    justifyContent: 'center',
  },
  logoText: {
    fontSize: 16,
    fontWeight: '700',
    color: COLORS.text,
  },
  headerRight: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  viewAsBtn: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
    paddingHorizontal: 10,
    paddingVertical: 6,
  },
  viewAsText: {
    fontSize: 12,
    color: COLORS.textLight,
  },
  proBtn: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
    backgroundColor: COLORS.secondary,
    paddingHorizontal: 12,
    paddingVertical: 6,
    borderRadius: 16,
  },
  proBtnText: {
    fontSize: 12,
    fontWeight: '600',
    color: '#fff',
  },
  heroSection: {
    alignItems: 'center',
    paddingHorizontal: 20,
    paddingTop: 32,
    paddingBottom: 24,
  },
  heroIcon: {
    width: 56,
    height: 56,
    borderRadius: 28,
    backgroundColor: '#EEF2FF',
    alignItems: 'center',
    justifyContent: 'center',
    marginBottom: 16,
  },
  heroTitle: {
    fontSize: 28,
    fontWeight: '700',
    color: COLORS.text,
    textAlign: 'center',
    lineHeight: 36,
  },
  heroSubtitle: {
    fontSize: 15,
    color: COLORS.textLight,
    textAlign: 'center',
    marginTop: 12,
    lineHeight: 22,
  },
  searchContainer: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: COLORS.surface,
    borderWidth: 1,
    borderColor: COLORS.border,
    borderRadius: 10,
    paddingHorizontal: 14,
    marginTop: 24,
    width: '100%',
  },
  searchInput: {
    flex: 1,
    paddingVertical: 14,
    paddingHorizontal: 10,
    fontSize: 15,
    color: COLORS.text,
  },
  searchBtn: {
    backgroundColor: COLORS.text,
    paddingHorizontal: 32,
    paddingVertical: 12,
    borderRadius: 8,
    marginTop: 12,
  },
  searchBtnText: {
    fontSize: 14,
    fontWeight: '600',
    color: '#fff',
  },
  browseSectors: {
    fontSize: 13,
    color: COLORS.textLight,
    marginTop: 24,
  },
  sectorsScroll: {
    marginTop: 12,
  },
  sectorChip: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
    backgroundColor: COLORS.surface,
    borderWidth: 1,
    borderColor: COLORS.border,
    borderRadius: 20,
    paddingHorizontal: 14,
    paddingVertical: 8,
    marginRight: 8,
  },
  sectorChipText: {
    fontSize: 13,
    color: COLORS.textLight,
  },
  simulatorCard: {
    backgroundColor: COLORS.surface,
    marginHorizontal: 16,
    borderRadius: 16,
    padding: 20,
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  cardLabel: {
    fontSize: 13,
    color: COLORS.textLight,
    marginBottom: 12,
  },
  companyRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 12,
    marginBottom: 16,
  },
  companyLogo: {
    width: 48,
    height: 48,
    borderRadius: 12,
    backgroundColor: '#fff',
    alignItems: 'center',
    justifyContent: 'center',
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  companyName: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.text,
  },
  companyMeta: {
    fontSize: 12,
    color: COLORS.textMuted,
    marginTop: 2,
  },
  toggleContainer: {
    flexDirection: 'row',
    backgroundColor: '#fff',
    borderRadius: 8,
    padding: 4,
    marginBottom: 12,
  },
  toggleBtn: {
    flex: 1,
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 6,
    paddingVertical: 10,
    borderRadius: 6,
  },
  toggleBtnActive: {
    backgroundColor: COLORS.surface,
  },
  toggleText: {
    fontSize: 13,
    color: COLORS.textMuted,
  },
  toggleTextActive: {
    color: COLORS.primary,
    fontWeight: '500',
  },
  compareText: {
    fontSize: 12,
    color: COLORS.textLight,
    textAlign: 'center',
    marginBottom: 16,
  },
  inputLabel: {
    fontSize: 13,
    color: COLORS.textLight,
    marginBottom: 8,
  },
  horizonRow: {
    flexDirection: 'row',
    backgroundColor: '#fff',
    borderRadius: 8,
    padding: 4,
    marginBottom: 16,
  },
  horizonBtn: {
    flex: 1,
    paddingVertical: 10,
    alignItems: 'center',
    borderRadius: 6,
  },
  horizonBtnActive: {
    backgroundColor: COLORS.text,
  },
  horizonText: {
    fontSize: 13,
    color: COLORS.textMuted,
  },
  horizonTextActive: {
    color: '#fff',
    fontWeight: '600',
  },
  resultsGrid: {
    gap: 12,
  },
  resultItem: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
  },
  resultLabel: {
    fontSize: 13,
    color: COLORS.textLight,
  },
  resultValue: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.text,
  },
  successText: {
    color: COLORS.success,
  },
  dangerText: {
    color: COLORS.danger,
  },
  ctaText: {
    fontSize: 13,
    color: COLORS.textLight,
    textAlign: 'center',
    marginTop: 20,
    marginBottom: 12,
  },
  googleBtn: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 8,
    backgroundColor: COLORS.primary,
    borderRadius: 8,
    paddingVertical: 14,
  },
  googleBtnText: {
    fontSize: 14,
    fontWeight: '600',
    color: '#fff',
  },
  section: {
    paddingHorizontal: 16,
    paddingTop: 40,
  },
  sectionTitle: {
    fontSize: 24,
    fontWeight: '700',
    color: COLORS.text,
    textAlign: 'center',
  },
  sectionSubtitle: {
    fontSize: 14,
    color: COLORS.textLight,
    textAlign: 'center',
    marginTop: 8,
  },
  portfolioCard: {
    backgroundColor: COLORS.surface,
    borderRadius: 16,
    padding: 20,
    marginTop: 24,
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  portfolioName: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.text,
  },
  portfolioDesc: {
    fontSize: 13,
    color: COLORS.textLight,
    marginTop: 4,
    marginBottom: 16,
    lineHeight: 18,
  },
  portfolioResults: {
    gap: 10,
    marginBottom: 16,
  },
  portfolioResultItem: {
    flexDirection: 'row',
    justifyContent: 'space-between',
  },
  portfolioResultLabel: {
    fontSize: 12,
    color: COLORS.textLight,
  },
  portfolioResultValue: {
    fontSize: 13,
    fontWeight: '600',
    color: COLORS.text,
  },
  chartContainer: {
    marginTop: 16,
  },
  chartLegend: {
    flexDirection: 'row',
    justifyContent: 'center',
    gap: 20,
    marginTop: 12,
  },
  legendItem: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
  },
  legendDot: {
    width: 8,
    height: 8,
    borderRadius: 4,
  },
  legendText: {
    fontSize: 11,
    color: COLORS.textMuted,
  },
  pricingSection: {
    paddingHorizontal: 16,
    paddingTop: 40,
    gap: 16,
  },
  pricingCard: {
    backgroundColor: COLORS.surface,
    borderRadius: 16,
    padding: 20,
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  pricingCardHighlighted: {
    borderColor: COLORS.primary,
    borderWidth: 2,
  },
  pricingHeader: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
  },
  planName: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.textLight,
  },
  proPlanName: {
    color: COLORS.primary,
  },
  priceRow: {
    flexDirection: 'row',
    alignItems: 'baseline',
    marginTop: 8,
  },
  price: {
    fontSize: 32,
    fontWeight: '700',
    color: COLORS.text,
  },
  pricePeriod: {
    fontSize: 14,
    color: COLORS.textMuted,
    marginLeft: 4,
  },
  planDesc: {
    fontSize: 13,
    color: COLORS.textLight,
    marginTop: 8,
    marginBottom: 16,
  },
  featureRow: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    gap: 8,
    marginBottom: 8,
  },
  featureText: {
    flex: 1,
    fontSize: 13,
    color: COLORS.textLight,
    lineHeight: 18,
  },
  ctaBtn: {
    backgroundColor: COLORS.surface,
    borderWidth: 1,
    borderColor: COLORS.border,
    borderRadius: 8,
    paddingVertical: 14,
    alignItems: 'center',
    marginTop: 16,
  },
  ctaBtnHighlighted: {
    backgroundColor: COLORS.primary,
    borderColor: COLORS.primary,
  },
  ctaBtnText: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.text,
  },
  ctaBtnTextHighlighted: {
    color: '#fff',
  },
  disclaimer: {
    fontSize: 12,
    color: COLORS.textMuted,
    textAlign: 'center',
    paddingHorizontal: 20,
    marginTop: 24,
  },
  footer: {
    alignItems: 'center',
    paddingVertical: 40,
    paddingHorizontal: 20,
    marginTop: 40,
    borderTopWidth: 1,
    borderTopColor: COLORS.border,
  },
  footerLogo: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  footerLogoText: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.text,
  },
  footerTagline: {
    fontSize: 13,
    color: COLORS.textLight,
    marginTop: 4,
  },
  footerLinks: {
    flexDirection: 'row',
    gap: 20,
    marginTop: 16,
  },
  footerLink: {
    fontSize: 13,
    color: COLORS.textLight,
  },
  footerDisclaimer: {
    fontSize: 12,
    color: COLORS.textMuted,
    textAlign: 'center',
    marginTop: 16,
    lineHeight: 18,
  },
  copyright: {
    fontSize: 11,
    color: COLORS.textMuted,
    marginTop: 8,
  },
});
