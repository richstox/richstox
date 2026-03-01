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
import { useRouter, useLocalSearchParams } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import axios from 'axios';
import { COLORS } from '../_layout';

const API_URL = process.env.EXPO_PUBLIC_BACKEND_URL;

interface BuyHoldResult {
  ticker: string;
  strategy: string;
  initial_investment: number;
  shares_bought: number;
  buy_price: number;
  buy_date: string;
  current_price: number;
  current_date: string;
  final_value: number;
  total_return: number;
  total_return_pct: number;
  cagr: number;
  max_drawdown: number;
  benchmark_return_pct: number;
  vs_benchmark: number;
  days_held: number;
}

export default function BuyHoldCalculator() {
  const router = useRouter();
  const params = useLocalSearchParams();
  const [ticker, setTicker] = useState(params.ticker?.toString() || 'AAPL');
  const [investment, setInvestment] = useState('10000');
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<BuyHoldResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  const calculate = async () => {
    if (!ticker || !investment) return;
    
    setLoading(true);
    setError(null);
    
    try {
      const response = await axios.get(`${API_URL}/api/calculator/buy-hold`, {
        params: {
          ticker: ticker.toUpperCase(),
          initial_investment: parseFloat(investment),
        },
      });
      setResult(response.data);
    } catch (err: any) {
      console.error('Error calculating:', err);
      setError(err.response?.data?.detail || 'Failed to calculate. Try a different ticker.');
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
          <Text style={styles.headerTitle}>Buy & Hold</Text>
          <View style={styles.placeholder} />
        </View>

        <ScrollView style={styles.content} showsVerticalScrollIndicator={false}>
          {/* Input Form */}
          <View style={styles.formCard}>
            <Text style={styles.formTitle}>Investment Parameters</Text>
            
            <View style={styles.inputGroup}>
              <Text style={styles.inputLabel}>Stock Ticker</Text>
              <TextInput
                style={styles.input}
                value={ticker}
                onChangeText={setTicker}
                placeholder="AAPL"
                autoCapitalize="characters"
                maxLength={10}
              />
            </View>

            <View style={styles.inputGroup}>
              <Text style={styles.inputLabel}>Initial Investment ($)</Text>
              <TextInput
                style={styles.input}
                value={investment}
                onChangeText={setInvestment}
                placeholder="10000"
                keyboardType="numeric"
              />
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
                <Text style={styles.resultTitle}>{result.ticker}</Text>
                <Text style={styles.resultSubtitle}>
                  {result.buy_date} → {result.current_date}
                </Text>

                <View style={styles.mainResult}>
                  <Text style={styles.mainResultLabel}>Final Value</Text>
                  <Text style={styles.mainResultValue}>{formatCurrency(result.final_value)}</Text>
                  <View style={[
                    styles.returnBadge,
                    result.total_return_pct >= 0 ? styles.positiveBadge : styles.negativeBadge
                  ]}>
                    <Text style={[
                      styles.returnBadgeText,
                      result.total_return_pct >= 0 ? styles.positiveText : styles.negativeText
                    ]}>
                      {formatPercent(result.total_return_pct)} ({formatCurrency(result.total_return)})
                    </Text>
                  </View>
                </View>
              </View>

              <View style={styles.detailsCard}>
                <View style={styles.detailRow}>
                  <Text style={styles.detailLabel}>Shares Bought</Text>
                  <Text style={styles.detailValue}>{result.shares_bought.toFixed(4)}</Text>
                </View>
                <View style={styles.detailRow}>
                  <Text style={styles.detailLabel}>Buy Price</Text>
                  <Text style={styles.detailValue}>{formatCurrency(result.buy_price)}</Text>
                </View>
                <View style={styles.detailRow}>
                  <Text style={styles.detailLabel}>Current Price</Text>
                  <Text style={styles.detailValue}>{formatCurrency(result.current_price)}</Text>
                </View>
                <View style={styles.detailRow}>
                  <Text style={styles.detailLabel}>CAGR</Text>
                  <Text style={[
                    styles.detailValue,
                    result.cagr >= 0 ? styles.positiveText : styles.negativeText
                  ]}>
                    {formatPercent(result.cagr)}
                  </Text>
                </View>
                <View style={styles.detailRow}>
                  <Text style={styles.detailLabel}>Max Drawdown</Text>
                  <Text style={[styles.detailValue, styles.negativeText]}>
                    -{result.max_drawdown.toFixed(2)}%
                  </Text>
                </View>
                <View style={styles.detailRow}>
                  <Text style={styles.detailLabel}>Days Held</Text>
                  <Text style={styles.detailValue}>{result.days_held}</Text>
                </View>
              </View>

              <View style={styles.benchmarkCard}>
                <Text style={styles.benchmarkTitle}>vs S&P 500</Text>
                <View style={styles.benchmarkRow}>
                  <View style={styles.benchmarkItem}>
                    <Text style={styles.benchmarkLabel}>Your Return</Text>
                    <Text style={[
                      styles.benchmarkValue,
                      result.total_return_pct >= 0 ? styles.positiveText : styles.negativeText
                    ]}>
                      {formatPercent(result.total_return_pct)}
                    </Text>
                  </View>
                  <Text style={styles.benchmarkVs}>vs</Text>
                  <View style={styles.benchmarkItem}>
                    <Text style={styles.benchmarkLabel}>Benchmark</Text>
                    <Text style={[
                      styles.benchmarkValue,
                      result.benchmark_return_pct >= 0 ? styles.positiveText : styles.negativeText
                    ]}>
                      {formatPercent(result.benchmark_return_pct)}
                    </Text>
                  </View>
                </View>
                <View style={[
                  styles.outperformBadge,
                  result.vs_benchmark >= 0 ? styles.positiveBadge : styles.negativeBadge
                ]}>
                  <Text style={[
                    styles.outperformText,
                    result.vs_benchmark >= 0 ? styles.positiveText : styles.negativeText
                  ]}>
                    {result.vs_benchmark >= 0 ? 'Outperformed' : 'Underperformed'} by {Math.abs(result.vs_benchmark).toFixed(2)}%
                  </Text>
                </View>
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
  },
  resultTitle: {
    fontSize: 24,
    fontWeight: '700',
    color: COLORS.text,
  },
  resultSubtitle: {
    fontSize: 14,
    color: COLORS.textMuted,
    marginBottom: 20,
  },
  mainResult: {
    alignItems: 'center',
  },
  mainResultLabel: {
    fontSize: 14,
    color: COLORS.textMuted,
    marginBottom: 4,
  },
  mainResultValue: {
    fontSize: 40,
    fontWeight: '700',
    color: COLORS.text,
    marginBottom: 12,
  },
  returnBadge: {
    paddingHorizontal: 16,
    paddingVertical: 8,
    borderRadius: 20,
  },
  positiveBadge: {
    backgroundColor: '#D1FAE5',
  },
  negativeBadge: {
    backgroundColor: '#FEE2E2',
  },
  returnBadgeText: {
    fontSize: 16,
    fontWeight: '600',
  },
  positiveText: {
    color: '#10B981',
  },
  negativeText: {
    color: '#EF4444',
  },
  detailsCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 20,
    marginBottom: 16,
  },
  detailRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    paddingVertical: 10,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  detailLabel: {
    fontSize: 14,
    color: COLORS.textMuted,
  },
  detailValue: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.text,
  },
  benchmarkCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 20,
    marginBottom: 32,
  },
  benchmarkTitle: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.text,
    marginBottom: 16,
    textAlign: 'center',
  },
  benchmarkRow: {
    flexDirection: 'row',
    alignItems: 'center',
    marginBottom: 16,
  },
  benchmarkItem: {
    flex: 1,
    alignItems: 'center',
  },
  benchmarkLabel: {
    fontSize: 12,
    color: COLORS.textMuted,
    marginBottom: 4,
  },
  benchmarkValue: {
    fontSize: 20,
    fontWeight: '700',
  },
  benchmarkVs: {
    fontSize: 14,
    color: COLORS.textMuted,
    paddingHorizontal: 16,
  },
  outperformBadge: {
    alignSelf: 'center',
    paddingHorizontal: 16,
    paddingVertical: 8,
    borderRadius: 20,
  },
  outperformText: {
    fontSize: 14,
    fontWeight: '600',
  },
});
