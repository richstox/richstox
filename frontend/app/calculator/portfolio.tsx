import React, { useState } from 'react';
import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  TextInput,
  TouchableOpacity,
  ActivityIndicator,
  KeyboardAvoidingView,
  Platform,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useRouter } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import axios from 'axios';
import { COLORS } from '../_layout';
import { API_URL } from '../../utils/config';

interface PortfolioResult {
  target_value: number;
  tickers: string[];
  composition: Array<{
    ticker: string;
    weight: number;
    allocation: number;
    shares: number;
    current_price: number;
  }>;
  start_date: string;
  start_value: number;
  end_date: string;
  end_value: number;
  total_return_pct: number;
  max_drawdown: number;
}

export default function PortfolioCalculator() {
  const router = useRouter();
  const [targetValue, setTargetValue] = useState('100000');
  const [tickers, setTickers] = useState('AAPL,MSFT,GOOGL');
  const [weights, setWeights] = useState('40,30,30');
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<PortfolioResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  const calculate = async () => {
    if (!targetValue || !tickers || !weights) return;
    
    setLoading(true);
    setError(null);
    
    try {
      const response = await axios.get(`${API_URL}/api/calculator/portfolio-value`, {
        params: {
          target_value: parseFloat(targetValue),
          tickers: tickers.toUpperCase().replace(/\s/g, ''),
          weights: weights.replace(/\s/g, ''),
        },
      });
      setResult(response.data);
    } catch (err: any) {
      console.error('Error calculating:', err);
      setError(err.response?.data?.detail || 'Failed to calculate. Check tickers and weights.');
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

  const formatPercent = (value: number) => {
    const sign = value >= 0 ? '+' : '';
    return `${sign}${value.toFixed(2)}%`;
  };

  return (
    <SafeAreaView style={styles.container}>
      <KeyboardAvoidingView 
        style={styles.keyboardView}
        behavior={Platform.OS === 'ios' ? 'padding' : 'height'}
      >
        <View style={styles.header}>
          <TouchableOpacity style={styles.backButton} onPress={() => router.back()}>
            <Ionicons name="arrow-back" size={24} color={COLORS.text} />
          </TouchableOpacity>
          <Text style={styles.headerTitle}>Portfolio Value</Text>
          <View style={styles.placeholder} />
        </View>

        <ScrollView style={styles.content} showsVerticalScrollIndicator={false}>
          {/* Explanation */}
          <View style={styles.infoCard}>
            <Ionicons name="information-circle" size={20} color={COLORS.primary} />
            <Text style={styles.infoText}>
              See what a portfolio would have been worth 1 year ago if it's worth your target value today.
            </Text>
          </View>

          {/* Input Form */}
          <View style={styles.formCard}>
            <Text style={styles.formTitle}>Portfolio Setup</Text>
            
            <View style={styles.inputGroup}>
              <Text style={styles.inputLabel}>Target Value Today ($)</Text>
              <TextInput
                style={styles.input}
                value={targetValue}
                onChangeText={setTargetValue}
                placeholder="100000"
                keyboardType="numeric"
              />
            </View>

            <View style={styles.inputGroup}>
              <Text style={styles.inputLabel}>Tickers (comma separated)</Text>
              <TextInput
                style={styles.input}
                value={tickers}
                onChangeText={setTickers}
                placeholder="AAPL,MSFT,GOOGL"
                autoCapitalize="characters"
              />
            </View>

            <View style={styles.inputGroup}>
              <Text style={styles.inputLabel}>Weights % (comma separated)</Text>
              <TextInput
                style={styles.input}
                value={weights}
                onChangeText={setWeights}
                placeholder="40,30,30"
                keyboardType="numeric"
              />
              <Text style={styles.inputHint}>Weights will be normalized to 100%</Text>
            </View>

            <TouchableOpacity
              style={[styles.calcButton, loading && styles.calcButtonDisabled]}
              onPress={calculate}
              disabled={loading}
            >
              {loading ? (
                <ActivityIndicator color="#FFFFFF" />
              ) : (
                <>
                  <Ionicons name="calculator" size={20} color="#FFFFFF" />
                  <Text style={styles.calcButtonText}>Calculate</Text>
                </>
              )}
            </TouchableOpacity>
          </View>

          {/* Error */}
          {error && (
            <View style={styles.errorCard}>
              <Ionicons name="alert-circle" size={20} color="#EF4444" />
              <Text style={styles.errorText}>{error}</Text>
            </View>
          )}

          {/* Results */}
          {result && (
            <>
              <View style={styles.resultCard}>
                <Text style={styles.resultTitle}>Portfolio Analysis</Text>
                <Text style={styles.resultSubtitle}>
                  {result.start_date} → {result.end_date}
                </Text>

                <View style={styles.valueComparison}>
                  <View style={styles.valueItem}>
                    <Text style={styles.valueLabel}>1 Year Ago</Text>
                    <Text style={styles.valueAmount}>{formatCurrency(result.start_value)}</Text>
                  </View>
                  <Ionicons name="arrow-forward" size={24} color={COLORS.textMuted} />
                  <View style={styles.valueItem}>
                    <Text style={styles.valueLabel}>Today</Text>
                    <Text style={styles.valueAmount}>{formatCurrency(result.end_value)}</Text>
                  </View>
                </View>

                <View style={[
                  styles.returnBadge,
                  result.total_return_pct >= 0 ? styles.positiveBadge : styles.negativeBadge
                ]}>
                  <Text style={[
                    styles.returnBadgeText,
                    result.total_return_pct >= 0 ? styles.positiveText : styles.negativeText
                  ]}>
                    {formatPercent(result.total_return_pct)} Return
                  </Text>
                </View>
              </View>

              {/* Portfolio Composition */}
              <View style={styles.compositionCard}>
                <Text style={styles.sectionTitle}>Portfolio Composition</Text>
                {result.composition.map((item) => (
                  <View key={item.ticker} style={styles.compositionItem}>
                    <View style={styles.compositionLeft}>
                      <Text style={styles.compositionTicker}>{item.ticker}</Text>
                      <Text style={styles.compositionWeight}>{item.weight.toFixed(1)}%</Text>
                    </View>
                    <View style={styles.compositionRight}>
                      <Text style={styles.compositionAllocation}>{formatCurrency(item.allocation)}</Text>
                      <Text style={styles.compositionShares}>{item.shares.toFixed(4)} shares</Text>
                    </View>
                  </View>
                ))}
              </View>

              {/* Risk Metrics */}
              <View style={styles.riskCard}>
                <Text style={styles.sectionTitle}>Risk Metrics</Text>
                <View style={styles.riskItem}>
                  <Text style={styles.riskLabel}>Maximum Drawdown</Text>
                  <Text style={[styles.riskValue, styles.negativeText]}>
                    -{result.max_drawdown.toFixed(2)}%
                  </Text>
                </View>
                <Text style={styles.riskNote}>
                  The largest peak-to-trough decline during this period.
                </Text>
              </View>
            </>
          )}
        </ScrollView>
      </KeyboardAvoidingView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: COLORS.background,
  },
  keyboardView: {
    flex: 1,
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
  content: {
    padding: 16,
  },
  infoCard: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    backgroundColor: '#EBF5FF',
    borderRadius: 12,
    padding: 16,
    marginBottom: 16,
    gap: 12,
  },
  infoText: {
    flex: 1,
    fontSize: 14,
    color: COLORS.primary,
    lineHeight: 20,
  },
  formCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 20,
    marginBottom: 16,
  },
  formTitle: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.text,
    marginBottom: 16,
  },
  inputGroup: {
    marginBottom: 16,
  },
  inputLabel: {
    fontSize: 14,
    color: COLORS.textMuted,
    marginBottom: 8,
  },
  input: {
    backgroundColor: COLORS.background,
    borderWidth: 1,
    borderColor: COLORS.border,
    borderRadius: 12,
    paddingHorizontal: 16,
    paddingVertical: 14,
    fontSize: 16,
    color: COLORS.text,
  },
  inputHint: {
    fontSize: 12,
    color: COLORS.textMuted,
    marginTop: 6,
    fontStyle: 'italic',
  },
  calcButton: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: COLORS.primary,
    borderRadius: 12,
    paddingVertical: 16,
    gap: 8,
  },
  calcButtonDisabled: {
    opacity: 0.6,
  },
  calcButtonText: {
    color: '#FFFFFF',
    fontSize: 16,
    fontWeight: '600',
  },
  errorCard: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: '#FEE2E2',
    borderRadius: 12,
    padding: 16,
    marginBottom: 16,
    gap: 12,
  },
  errorText: {
    flex: 1,
    color: '#EF4444',
    fontSize: 14,
  },
  resultCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 20,
    marginBottom: 16,
    alignItems: 'center',
  },
  resultTitle: {
    fontSize: 20,
    fontWeight: '700',
    color: COLORS.text,
  },
  resultSubtitle: {
    fontSize: 14,
    color: COLORS.textMuted,
    marginBottom: 20,
  },
  valueComparison: {
    flexDirection: 'row',
    alignItems: 'center',
    marginBottom: 16,
    gap: 16,
  },
  valueItem: {
    alignItems: 'center',
  },
  valueLabel: {
    fontSize: 12,
    color: COLORS.textMuted,
    marginBottom: 4,
  },
  valueAmount: {
    fontSize: 22,
    fontWeight: '700',
    color: COLORS.text,
  },
  returnBadge: {
    paddingHorizontal: 20,
    paddingVertical: 10,
    borderRadius: 20,
  },
  positiveBadge: {
    backgroundColor: '#D1FAE5',
  },
  negativeBadge: {
    backgroundColor: '#FEE2E2',
  },
  returnBadgeText: {
    fontSize: 18,
    fontWeight: '600',
  },
  positiveText: {
    color: '#10B981',
  },
  negativeText: {
    color: '#EF4444',
  },
  compositionCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 20,
    marginBottom: 16,
  },
  sectionTitle: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.text,
    marginBottom: 16,
  },
  compositionItem: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    paddingVertical: 12,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  compositionLeft: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 12,
  },
  compositionTicker: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.text,
  },
  compositionWeight: {
    fontSize: 14,
    color: COLORS.primary,
    backgroundColor: '#EBF5FF',
    paddingHorizontal: 8,
    paddingVertical: 2,
    borderRadius: 8,
  },
  compositionRight: {
    alignItems: 'flex-end',
  },
  compositionAllocation: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.text,
  },
  compositionShares: {
    fontSize: 12,
    color: COLORS.textMuted,
  },
  riskCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 20,
    marginBottom: 32,
  },
  riskItem: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: 8,
  },
  riskLabel: {
    fontSize: 14,
    color: COLORS.textMuted,
  },
  riskValue: {
    fontSize: 18,
    fontWeight: '600',
  },
  riskNote: {
    fontSize: 12,
    color: COLORS.textMuted,
    fontStyle: 'italic',
  },
});
