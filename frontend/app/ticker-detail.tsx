import React, { useState, useEffect } from 'react';
import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  TouchableOpacity,
  ActivityIndicator,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useLocalSearchParams, useRouter } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import axios from 'axios';
import { LineChart } from 'react-native-gifted-charts';
import { COLORS } from './_layout';
import { API_URL } from '../utils/config';

export default function TickerDetail() {
  const { ticker } = useLocalSearchParams();
  const router = useRouter();
  const [loading, setLoading] = useState(true);
  const [tickerInfo, setTickerInfo] = useState<any>(null);
  const [prices, setPrices] = useState<any[]>([]);

  useEffect(() => {
    fetchTickerData();
  }, [ticker]);

  const fetchTickerData = async () => {
    try {
      const [infoResponse, pricesResponse] = await Promise.all([
        axios.get(`${API_URL}/api/stock/${ticker}`),
        axios.get(`${API_URL}/api/stock/${ticker}/prices?days=90`),
      ]);
      setTickerInfo(infoResponse.data);
      setPrices(pricesResponse.data);
    } catch (error) {
      console.error('Error fetching ticker data:', error);
    } finally {
      setLoading(false);
    }
  };

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2,
    }).format(value);
  };

  if (loading) {
    return (
      <View style={styles.loadingContainer}>
        <ActivityIndicator size="large" color={COLORS.primary} />
      </View>
    );
  }

  if (!tickerInfo) {
    return (
      <View style={styles.errorContainer}>
        <Ionicons name="alert-circle-outline" size={48} color={COLORS.textMuted} />
        <Text style={styles.errorText}>Ticker not found</Text>
      </View>
    );
  }

  // Prepare chart data
  const chartData = prices.map((item: any, index: number) => ({
    value: item.close,
    label: index % 15 === 0 ? item.date.slice(5) : '',
  }));

  return (
    <SafeAreaView style={styles.container}>
      {/* Header */}
      <View style={styles.header}>
        <TouchableOpacity
          style={styles.backButton}
          onPress={() => router.back()}
        >
          <Ionicons name="arrow-back" size={24} color={COLORS.text} />
        </TouchableOpacity>
        <Text style={styles.headerTitle}>{ticker}</Text>
        <View style={styles.placeholder} />
      </View>

      <ScrollView
        style={styles.scrollView}
        contentContainerStyle={styles.scrollContent}
        showsVerticalScrollIndicator={false}
      >
        {/* Ticker Info */}
        <View style={styles.tickerHeader}>
          <Text style={styles.tickerSymbol}>{tickerInfo.ticker}</Text>
          <Text style={styles.companyName}>{tickerInfo.name}</Text>
          <View style={styles.sectorBadge}>
            <Text style={styles.sectorText}>{tickerInfo.sector}</Text>
          </View>
        </View>

        {/* Current Price Card */}
        <View style={styles.priceCard}>
          <Text style={styles.priceLabel}>Current Price</Text>
          <Text style={styles.priceValue}>
            {formatCurrency(tickerInfo.current_price)}
          </Text>
          <View style={styles.priceStats}>
            <View style={styles.priceStat}>
              <Text style={styles.priceStatLabel}>52W High</Text>
              <Text style={styles.priceStatValue}>
                {formatCurrency(tickerInfo.week_52_high)}
              </Text>
            </View>
            <View style={styles.priceStatDivider} />
            <View style={styles.priceStat}>
              <Text style={styles.priceStatLabel}>52W Low</Text>
              <Text style={styles.priceStatValue}>
                {formatCurrency(tickerInfo.week_52_low)}
              </Text>
            </View>
            <View style={styles.priceStatDivider} />
            <View style={styles.priceStat}>
              <Text style={styles.priceStatLabel}>Max DD</Text>
              <Text style={[styles.priceStatValue, styles.neutralValue]}>
                -{tickerInfo.max_drawdown}%
              </Text>
            </View>
          </View>
        </View>

        {/* Price Chart */}
        <View style={styles.chartCard}>
          <Text style={styles.sectionTitle}>Price (90 Days)</Text>
          <View style={styles.chartContainer}>
            {chartData.length > 0 && (
              <LineChart
                data={chartData}
                width={300}
                height={180}
                color={COLORS.primary}
                thickness={2}
                hideDataPoints
                hideRules
                hideYAxisText
                yAxisColor="transparent"
                xAxisColor={COLORS.border}
                xAxisLabelTextStyle={{ fontSize: 10, color: COLORS.textMuted }}
                areaChart
                startFillColor={COLORS.primary}
                endFillColor="white"
                startOpacity={0.2}
                endOpacity={0}
                curved
              />
            )}
          </View>
        </View>

        {/* Dividend Info */}
        {tickerInfo.dividend_yield > 0 && (
          <View style={styles.dividendCard}>
            <View style={styles.dividendHeader}>
              <Ionicons name="cash-outline" size={22} color={COLORS.accent} />
              <Text style={styles.sectionTitle}>Dividend</Text>
            </View>
            <Text style={styles.dividendYield}>
              {(tickerInfo.dividend_yield * 100).toFixed(2)}% Annual Yield
            </Text>
            <Text style={styles.dividendNote}>
              Dividends are paid quarterly and automatically tracked when you add this to your portfolio.
            </Text>
          </View>
        )}

        {/* Open canonical stock detail */}
        <TouchableOpacity
          style={styles.addButton}
          onPress={() => router.push(`/stock/${ticker}`)}
          activeOpacity={0.8}
        >
          <Ionicons name="open-outline" size={20} color="#FFFFFF" />
          <Text style={styles.addButtonText}>Open stock detail</Text>
        </TouchableOpacity>

        {/* Calm Reminder */}
        <View style={styles.calmReminder}>
          <Ionicons name="leaf-outline" size={18} color={COLORS.accent} />
          <Text style={styles.calmText}>
            Take your time. Good investments require patience and research.
          </Text>
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
    alignItems: 'center',
    justifyContent: 'space-between',
    paddingHorizontal: 16,
    paddingVertical: 12,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  backButton: {
    width: 44,
    height: 44,
    alignItems: 'center',
    justifyContent: 'center',
  },
  headerTitle: {
    fontSize: 18,
    fontWeight: '600',
    color: COLORS.text,
  },
  placeholder: {
    width: 44,
  },
  loadingContainer: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: COLORS.background,
  },
  errorContainer: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: COLORS.background,
  },
  errorText: {
    fontSize: 16,
    color: COLORS.textLight,
    marginTop: 16,
  },
  scrollView: {
    flex: 1,
  },
  scrollContent: {
    padding: 16,
    paddingBottom: 32,
  },
  tickerHeader: {
    marginBottom: 16,
  },
  tickerSymbol: {
    fontSize: 32,
    fontWeight: '700',
    color: COLORS.text,
  },
  companyName: {
    fontSize: 16,
    color: COLORS.textLight,
    marginTop: 4,
  },
  sectorBadge: {
    alignSelf: 'flex-start',
    backgroundColor: '#F5F8FC',
    paddingHorizontal: 12,
    paddingVertical: 6,
    borderRadius: 16,
    marginTop: 12,
  },
  sectorText: {
    fontSize: 13,
    color: COLORS.primary,
  },
  priceCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 20,
    marginBottom: 16,
  },
  priceLabel: {
    fontSize: 13,
    color: COLORS.textMuted,
    marginBottom: 4,
  },
  priceValue: {
    fontSize: 36,
    fontWeight: '700',
    color: COLORS.text,
    marginBottom: 16,
  },
  priceStats: {
    flexDirection: 'row',
    alignItems: 'center',
  },
  priceStat: {
    flex: 1,
    alignItems: 'center',
  },
  priceStatLabel: {
    fontSize: 11,
    color: COLORS.textMuted,
    marginBottom: 4,
  },
  priceStatValue: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.text,
  },
  priceStatDivider: {
    width: 1,
    height: 32,
    backgroundColor: COLORS.border,
  },
  neutralValue: {
    color: COLORS.textLight,
  },
  chartCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 16,
    marginBottom: 16,
  },
  sectionTitle: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.text,
    marginBottom: 12,
  },
  chartContainer: {
    alignItems: 'center',
  },
  dividendCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 16,
    marginBottom: 16,
  },
  dividendHeader: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 10,
    marginBottom: 12,
  },
  dividendYield: {
    fontSize: 22,
    fontWeight: '600',
    color: COLORS.accent,
    marginBottom: 8,
  },
  dividendNote: {
    fontSize: 13,
    color: COLORS.textLight,
    lineHeight: 18,
  },
  addButton: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: COLORS.primary,
    borderRadius: 12,
    paddingVertical: 16,
    gap: 8,
    marginBottom: 16,
  },
  addButtonText: {
    fontSize: 16,
    fontWeight: '600',
    color: '#FFFFFF',
  },
  calmReminder: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 8,
    paddingVertical: 16,
  },
  calmText: {
    fontSize: 13,
    color: COLORS.textLight,
    fontStyle: 'italic',
    textAlign: 'center',
    flex: 1,
  },
});
