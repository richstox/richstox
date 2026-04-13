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
import { API_URL } from '../../utils/config';

interface DCAResult {
  ticker: string;
  strategy: string;
  monthly_investment: number;
  months_invested: number;
  total_invested: number;
  total_shares: number;
  average_cost: number;
  current_price: number;
  final_value: number;
  total_return: number;
  total_return_pct: number;
}

export default function DCACalculator() {
  const router = useRouter();
  const params = useLocalSearchParams();
  const [ticker, setTicker] = useState(params.ticker?.toString() || 'AAPL');
  const [monthly, setMonthly] = useState('500');
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<DCAResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  const calculate = async () => {
    if (!ticker || !monthly) return;
    
    setLoading(true);
    setError(null);
    
    try {
      const response = await axios.get(`${API_URL}/api/calculator/dca`, {
        params: {
          ticker: ticker.toUpperCase(),
          monthly_investment: parseFloat(monthly),
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
          <Text style={styles.headerTitle}>DCA Calculator</Text>
          <View style={styles.placeholder} />
        </View>

        <ScrollView style={styles.content} showsVerticalScrollIndicator={false}>
          {/* Explanation */}
          <View style={styles.infoCard}>
            <Ionicons name="information-circle" size={20} color={COLORS.primary} />
            <Text style={styles.infoText}>
              Dollar Cost Averaging (DCA) means investing a fixed amount regularly, regardless of price.
            </Text>
          </View>

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
              <Text style={styles.inputLabel}>Monthly Investment ($)</Text>
              <TextInput
                style={styles.input}
                value={monthly}
                onChangeText={setMonthly}
                placeholder="500"
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
                  {result.months_invested} months of investing
                </Text>

                <View style={styles.mainResult}>
                  <Text style={styles.mainResultLabel}>Current Value</Text>
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

              <View style={styles.summaryCard}>
                <View style={styles.summaryRow}>
                  <View style={styles.summaryItem}>
                    <Text style={styles.summaryValue}>{formatCurrency(result.total_invested)}</Text>
                    <Text style={styles.summaryLabel}>Total Invested</Text>
                  </View>
                  <View style={styles.summaryItem}>
                    <Text style={styles.summaryValue}>{result.total_shares.toFixed(4)}</Text>
                    <Text style={styles.summaryLabel}>Shares Owned</Text>
                  </View>
                </View>
                <View style={styles.summaryRow}>
                  <View style={styles.summaryItem}>
                    <Text style={styles.summaryValue}>{formatCurrency(result.average_cost)}</Text>
                    <Text style={styles.summaryLabel}>Avg Cost/Share</Text>
                  </View>
                  <View style={styles.summaryItem}>
                    <Text style={styles.summaryValue}>{formatCurrency(result.current_price)}</Text>
                    <Text style={styles.summaryLabel}>Current Price</Text>
                  </View>
                </View>
              </View>

              {/* Cost Basis Advantage */}
              {result.average_cost < result.current_price && (
                <View style={styles.advantageCard}>
                  <Ionicons name="checkmark-circle" size={24} color={COLORS.accent} />
                  <View style={styles.advantageContent}>
                    <Text style={styles.advantageTitle}>DCA Advantage</Text>
                    <Text style={styles.advantageText}>
                      Your average cost ({formatCurrency(result.average_cost)}) is lower than the current price ({formatCurrency(result.current_price)}), showing the benefit of buying at different price points.
                    </Text>
                  </View>
                </View>
              )}
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
  summaryCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 20,
    marginBottom: 16,
  },
  summaryRow: {
    flexDirection: 'row',
    marginBottom: 16,
  },
  summaryItem: {
    flex: 1,
    alignItems: 'center',
  },
  summaryValue: {
    fontSize: 18,
    fontWeight: '600',
    color: COLORS.text,
    marginBottom: 4,
  },
  summaryLabel: {
    fontSize: 12,
    color: COLORS.textMuted,
  },
  advantageCard: {
    flexDirection: 'row',
    backgroundColor: '#F0FDF4',
    borderRadius: 16,
    padding: 16,
    marginBottom: 32,
    gap: 12,
  },
  advantageContent: {
    flex: 1,
  },
  advantageTitle: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.accent,
    marginBottom: 4,
  },
  advantageText: {
    fontSize: 13,
    color: COLORS.textLight,
    lineHeight: 18,
  },
});
