import React, { useState, useCallback } from 'react';
import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  RefreshControl,
  TouchableOpacity,
  ActivityIndicator,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useRouter, useFocusEffect } from 'expo-router';
import AsyncStorage from '@react-native-async-storage/async-storage';
import { Ionicons } from '@expo/vector-icons';
import axios from 'axios';
import { COLORS } from '../_layout';
import { useAppDialog } from '../../contexts/AppDialogContext';
import { useLayoutSpacing } from '../../constants/layout';
import { API_URL } from '../../utils/config';

export default function Portfolio() {
  const router = useRouter();
  const dialog = useAppDialog();
  const sp = useLayoutSpacing();
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [portfolioData, setPortfolioData] = useState<any>(null);

  const fetchPortfolio = async () => {
    try {
      const portfolioId = await AsyncStorage.getItem('portfolioId');
      if (!portfolioId) return;

      const response = await axios.get(`${API_URL}/api/portfolios/${portfolioId}`);
      setPortfolioData(response.data);
    } catch (error) {
      console.error('Error fetching portfolio:', error);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useFocusEffect(
    useCallback(() => {
      fetchPortfolio();
    }, [])
  );

  const onRefresh = () => {
    setRefreshing(true);
    fetchPortfolio();
  };

  const handleAddPosition = () => {
    if (portfolioData && portfolioData.position_count >= 7) {
      dialog.alert(
        'Position Limit Reached',
        'You can have a maximum of 7 positions. Consider consolidating or closing an existing position.',
      );
      return;
    }
    router.push('/add-position');
  };

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2,
    }).format(value);
  };

  const formatPercent = (value: number) => {
    const sign = value >= 0 ? '+' : '';
    return `${sign}${value.toFixed(2)}%`;
  };

  if (loading) {
    return (
      <View style={styles.loadingContainer}>
        <ActivityIndicator size="large" color={COLORS.primary} />
      </View>
    );
  }

  if (!portfolioData) {
    return (
      <View style={styles.errorContainer}>
        <Text style={styles.errorText}>Portfolio not found</Text>
      </View>
    );
  }

  return (
    <SafeAreaView style={styles.container} edges={['left', 'right']}>
      <ScrollView
        style={styles.scrollView}
        contentContainerStyle={[styles.scrollContent, { padding: sp.pageGutter }]}
        refreshControl={
          <RefreshControl refreshing={refreshing} onRefresh={onRefresh} />
        }
        showsVerticalScrollIndicator={false}
      >
        {/* Portfolio Header */}
        <View style={styles.headerCard}>
          <View style={styles.headerTop}>
            <View>
              <Text style={styles.portfolioName}>{portfolioData.name}</Text>
              <Text style={styles.portfolioType}>
                {portfolioData.portfolio_type} portfolio
              </Text>
            </View>
            <View style={styles.positionBadge}>
              <Text style={styles.positionBadgeText}>
                {portfolioData.position_count}/7
              </Text>
            </View>
          </View>
        </View>

        {/* Cash Position */}
        <View style={styles.cashCard}>
          <View style={styles.cashLeft}>
            <Ionicons name="wallet-outline" size={20} color={COLORS.textLight} />
            <Text style={styles.cashLabel}>Cash</Text>
          </View>
          <View style={styles.cashRight}>
            <Text style={styles.cashAmount}>
              {formatCurrency(portfolioData.cash)}
            </Text>
            <Text style={styles.cashAllocation}>
              {portfolioData.cash_allocation.toFixed(1)}% of portfolio
            </Text>
          </View>
        </View>

        {/* Positions List */}
        <View style={styles.sectionHeader}>
          <Text style={styles.sectionTitle}>Positions</Text>
          <TouchableOpacity
            style={styles.addButton}
            onPress={handleAddPosition}
            activeOpacity={0.7}
          >
            <Ionicons name="add" size={20} color={COLORS.primary} />
            <Text style={styles.addButtonText}>Add</Text>
          </TouchableOpacity>
        </View>

        {portfolioData.positions.length === 0 ? (
          <View style={styles.emptyState}>
            <Ionicons name="layers-outline" size={48} color={COLORS.textMuted} />
            <Text style={styles.emptyText}>No positions yet</Text>
            <Text style={styles.emptySubtext}>
              Add your first position to start tracking
            </Text>
          </View>
        ) : (
          <View style={styles.positionsList}>
            {portfolioData.positions.map((position: any) => (
              <TouchableOpacity
                key={position.id}
                style={styles.positionCard}
                onPress={() => router.push(`/position/${position.id}`)}
                activeOpacity={0.7}
              >
                <View style={styles.positionTop}>
                  <View style={styles.positionTicker}>
                    <Text style={styles.tickerText}>{position.ticker}</Text>
                    <Text style={styles.sharesText}>
                      {position.shares} shares
                    </Text>
                  </View>
                  <View style={styles.positionValue}>
                    <Text style={styles.marketValue}>
                      {formatCurrency(position.market_value)}
                    </Text>
                    <Text style={styles.allocationText}>
                      {position.allocation.toFixed(1)}%
                    </Text>
                  </View>
                </View>

                <View style={styles.positionMetrics}>
                  <View style={styles.metricBox}>
                    <Text style={styles.metricLabel}>Return</Text>
                    <Text
                      style={[
                        styles.metricValue,
                        position.return_pct >= 0
                          ? styles.positiveValue
                          : styles.negativeValue,
                      ]}
                    >
                      {formatPercent(position.return_pct)}
                    </Text>
                  </View>
                  <View style={styles.metricBox}>
                    <Text style={styles.metricLabel}>Drawdown</Text>
                    <Text style={[styles.metricValue, styles.neutralValue]}>
                      -{position.drawdown.toFixed(1)}%
                    </Text>
                  </View>
                  <View style={styles.metricBox}>
                    <Text style={styles.metricLabel}>Dividends</Text>
                    <Text style={styles.metricValue}>
                      {formatCurrency(position.dividends)}
                    </Text>
                  </View>
                </View>

                <View style={styles.thesisContainer}>
                  <Ionicons name="document-text-outline" size={14} color={COLORS.textMuted} />
                  <Text style={styles.thesisText} numberOfLines={1}>
                    {position.thesis}
                  </Text>
                </View>
              </TouchableOpacity>
            ))}
          </View>
        )}
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: COLORS.background,
  },
  scrollView: {
    flex: 1,
  },
  scrollContent: {
    padding: 16,
    paddingBottom: 32,
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
  },
  headerCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 20,
    marginBottom: 12,
  },
  headerTop: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
  },
  portfolioName: {
    fontSize: 22,
    fontWeight: '700',
    color: COLORS.text,
  },
  portfolioType: {
    fontSize: 14,
    color: COLORS.textMuted,
    marginTop: 4,
    textTransform: 'capitalize',
  },
  positionBadge: {
    backgroundColor: '#F5F8FC',
    paddingHorizontal: 12,
    paddingVertical: 6,
    borderRadius: 20,
  },
  positionBadgeText: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.primary,
  },
  cashCard: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    backgroundColor: COLORS.card,
    borderRadius: 12,
    padding: 16,
    marginBottom: 24,
  },
  cashLeft: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  cashLabel: {
    fontSize: 14,
    color: COLORS.textLight,
  },
  cashRight: {
    alignItems: 'flex-end',
  },
  cashAmount: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.text,
  },
  cashAllocation: {
    fontSize: 12,
    color: COLORS.textMuted,
    marginTop: 2,
  },
  sectionHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: 16,
  },
  sectionTitle: {
    fontSize: 18,
    fontWeight: '600',
    color: COLORS.text,
  },
  addButton: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
    paddingHorizontal: 12,
    paddingVertical: 8,
    backgroundColor: '#F5F8FC',
    borderRadius: 8,
  },
  addButtonText: {
    fontSize: 14,
    fontWeight: '500',
    color: COLORS.primary,
  },
  emptyState: {
    alignItems: 'center',
    paddingVertical: 48,
  },
  emptyText: {
    fontSize: 18,
    fontWeight: '600',
    color: COLORS.text,
    marginTop: 16,
  },
  emptySubtext: {
    fontSize: 14,
    color: COLORS.textMuted,
    marginTop: 8,
  },
  positionsList: {
    gap: 12,
  },
  positionCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 16,
  },
  positionTop: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    marginBottom: 12,
  },
  positionTicker: {},
  tickerText: {
    fontSize: 18,
    fontWeight: '700',
    color: COLORS.text,
  },
  sharesText: {
    fontSize: 12,
    color: COLORS.textMuted,
    marginTop: 2,
  },
  positionValue: {
    alignItems: 'flex-end',
  },
  marketValue: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.text,
  },
  allocationText: {
    fontSize: 12,
    color: COLORS.textMuted,
    marginTop: 2,
  },
  positionMetrics: {
    flexDirection: 'row',
    backgroundColor: COLORS.background,
    borderRadius: 10,
    padding: 12,
    marginBottom: 12,
  },
  metricBox: {
    flex: 1,
    alignItems: 'center',
  },
  metricLabel: {
    fontSize: 11,
    color: COLORS.textMuted,
    marginBottom: 4,
  },
  metricValue: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.text,
  },
  positiveValue: {
    color: COLORS.positive,
  },
  negativeValue: {
    color: COLORS.negative,
  },
  neutralValue: {
    color: COLORS.textLight,
  },
  thesisContainer: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
  },
  thesisText: {
    flex: 1,
    fontSize: 13,
    color: COLORS.textLight,
    fontStyle: 'italic',
  },
});
