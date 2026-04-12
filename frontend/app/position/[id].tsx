import React, { useState, useEffect, useCallback } from 'react';
import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  TouchableOpacity,
  TextInput,
  ActivityIndicator,
  RefreshControl,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useLocalSearchParams, useRouter, useFocusEffect } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import axios from 'axios';
import { LineChart } from 'react-native-gifted-charts';
import { COLORS } from '../_layout';
import { useAppDialog } from '../../contexts/AppDialogContext';

const API_URL = process.env.EXPO_PUBLIC_BACKEND_URL;

export default function PositionDetail() {
  const { id } = useLocalSearchParams();
  const router = useRouter();
  const dialog = useAppDialog();
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [position, setPosition] = useState<any>(null);
  const [editingThesis, setEditingThesis] = useState(false);
  const [newThesis, setNewThesis] = useState('');
  const [editingNotes, setEditingNotes] = useState(false);
  const [newNotes, setNewNotes] = useState('');

  const fetchPosition = async () => {
    try {
      const response = await axios.get(`${API_URL}/api/positions/${id}`);
      setPosition(response.data);
      setNewThesis(response.data.thesis);
      setNewNotes(response.data.notes || '');
    } catch (error) {
      console.error('Error fetching position:', error);
      dialog.alert('Error', 'Failed to load position');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useFocusEffect(
    useCallback(() => {
      fetchPosition();
    }, [id])
  );

  const onRefresh = () => {
    setRefreshing(true);
    fetchPosition();
  };

  const handleSaveThesis = async () => {
    try {
      await axios.put(`${API_URL}/api/positions/${id}`, { thesis: newThesis });
      setPosition({ ...position, thesis: newThesis });
      setEditingThesis(false);
    } catch (error) {
      dialog.alert('Error', 'Failed to update thesis');
    }
  };

  const handleSaveNotes = async () => {
    try {
      await axios.put(`${API_URL}/api/positions/${id}`, { notes: newNotes });
      setPosition({ ...position, notes: newNotes });
      setEditingNotes(false);
    } catch (error) {
      dialog.alert('Error', 'Failed to update notes');
    }
  };

  const handleToggleRule = async (ruleId: string) => {
    const updatedRules = position.rules.map((rule: any) =>
      rule.id === ruleId ? { ...rule, is_followed: !rule.is_followed } : rule
    );
    try {
      await axios.put(`${API_URL}/api/positions/${id}`, { rules: updatedRules });
      setPosition({ ...position, rules: updatedRules });
    } catch (error) {
      dialog.alert('Error', 'Failed to update rule');
    }
  };

  const handleDeletePosition = async () => {
    const confirmed = await dialog.confirm(
      'Delete Position',
      `Are you sure you want to remove ${position?.ticker} from your portfolio?`,
      { confirmLabel: 'Delete', confirmStyle: 'destructive' },
    );
    if (!confirmed) return;
    try {
      await axios.delete(`${API_URL}/api/positions/${id}`);
      router.back();
    } catch (error) {
      dialog.alert('Error', 'Failed to delete position');
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

  if (loading) {
    return (
      <View style={styles.loadingContainer}>
        <ActivityIndicator size="large" color={COLORS.primary} />
      </View>
    );
  }

  if (!position) {
    return (
      <View style={styles.errorContainer}>
        <Text style={styles.errorText}>Position not found</Text>
      </View>
    );
  }

  // Prepare chart data
  const chartData = position.price_history.map((item: any, index: number) => ({
    value: item.close,
    label: index % 15 === 0 ? item.date.slice(5) : '',
  }));

  return (
    <SafeAreaView style={styles.container} edges={['left', 'right', 'bottom']}>
      <ScrollView
        style={styles.scrollView}
        contentContainerStyle={styles.scrollContent}
        refreshControl={
          <RefreshControl refreshing={refreshing} onRefresh={onRefresh} />
        }
        showsVerticalScrollIndicator={false}
      >
        {/* Header */}
        <View style={styles.header}>
          <View>
            <Text style={styles.ticker}>{position.ticker}</Text>
            <Text style={styles.companyName}>{position.company_name}</Text>
          </View>
          <View style={styles.currentPrice}>
            <Text style={styles.priceLabel}>Current</Text>
            <Text style={styles.priceValue}>
              {formatCurrency(position.current_price)}
            </Text>
          </View>
        </View>

        {/* Key Metrics */}
        <View style={styles.metricsCard}>
          <View style={styles.metricsRow}>
            <View style={styles.metricItem}>
              <Text style={styles.metricLabel}>Shares</Text>
              <Text style={styles.metricValue}>{position.shares}</Text>
            </View>
            <View style={styles.metricItem}>
              <Text style={styles.metricLabel}>Entry Price</Text>
              <Text style={styles.metricValue}>
                {formatCurrency(position.entry_price)}
              </Text>
            </View>
            <View style={styles.metricItem}>
              <Text style={styles.metricLabel}>Cost Basis</Text>
              <Text style={styles.metricValue}>
                {formatCurrency(position.cost_basis)}
              </Text>
            </View>
          </View>
          <View style={styles.divider} />
          <View style={styles.metricsRow}>
            <View style={styles.metricItem}>
              <Text style={styles.metricLabel}>Market Value</Text>
              <Text style={styles.metricValue}>
                {formatCurrency(position.market_value)}
              </Text>
            </View>
            <View style={styles.metricItem}>
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
            <View style={styles.metricItem}>
              <Text style={styles.metricLabel}>Max Drawdown</Text>
              <Text style={[styles.metricValue, styles.neutralValue]}>
                -{position.max_drawdown}%
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

        {/* Thesis */}
        <View style={styles.sectionCard}>
          <View style={styles.sectionHeader}>
            <View style={styles.sectionTitleRow}>
              <Ionicons name="bulb-outline" size={20} color={COLORS.primary} />
              <Text style={styles.sectionTitle}>Investment Thesis</Text>
            </View>
            <TouchableOpacity
              onPress={() => setEditingThesis(!editingThesis)}
              style={styles.editButton}
            >
              <Ionicons
                name={editingThesis ? 'close' : 'pencil'}
                size={18}
                color={COLORS.primary}
              />
            </TouchableOpacity>
          </View>
          {editingThesis ? (
            <View>
              <TextInput
                style={styles.textInput}
                value={newThesis}
                onChangeText={setNewThesis}
                multiline
                maxLength={150}
              />
              <TouchableOpacity
                style={styles.saveButton}
                onPress={handleSaveThesis}
              >
                <Text style={styles.saveButtonText}>Save</Text>
              </TouchableOpacity>
            </View>
          ) : (
            <Text style={styles.thesisText}>{position.thesis}</Text>
          )}
        </View>

        {/* Notes */}
        <View style={styles.sectionCard}>
          <View style={styles.sectionHeader}>
            <View style={styles.sectionTitleRow}>
              <Ionicons name="document-text-outline" size={20} color={COLORS.primary} />
              <Text style={styles.sectionTitle}>Notes</Text>
            </View>
            <TouchableOpacity
              onPress={() => setEditingNotes(!editingNotes)}
              style={styles.editButton}
            >
              <Ionicons
                name={editingNotes ? 'close' : 'pencil'}
                size={18}
                color={COLORS.primary}
              />
            </TouchableOpacity>
          </View>
          {editingNotes ? (
            <View>
              <TextInput
                style={[styles.textInput, { minHeight: 100 }]}
                value={newNotes}
                onChangeText={setNewNotes}
                multiline
                placeholder="Add notes about this position..."
                placeholderTextColor={COLORS.textMuted}
              />
              <TouchableOpacity
                style={styles.saveButton}
                onPress={handleSaveNotes}
              >
                <Text style={styles.saveButtonText}>Save</Text>
              </TouchableOpacity>
            </View>
          ) : (
            <Text style={styles.notesText}>
              {position.notes || 'No notes yet. Tap edit to add.'}
            </Text>
          )}
        </View>

        {/* Rules */}
        <View style={styles.sectionCard}>
          <View style={styles.sectionTitleRow}>
            <Ionicons name="shield-checkmark-outline" size={20} color={COLORS.primary} />
            <Text style={styles.sectionTitle}>Rules (Self-Tracking)</Text>
          </View>
          <Text style={styles.rulesSubtitle}>
            Track your discipline. No automated alerts.
          </Text>
          {position.rules.map((rule: any) => (
            <TouchableOpacity
              key={rule.id}
              style={styles.ruleItem}
              onPress={() => handleToggleRule(rule.id)}
              activeOpacity={0.7}
            >
              <Ionicons
                name={rule.is_followed ? 'checkmark-circle' : 'ellipse-outline'}
                size={24}
                color={rule.is_followed ? COLORS.positive : COLORS.textMuted}
              />
              <Text
                style={[
                  styles.ruleText,
                  !rule.is_followed && styles.ruleTextInactive,
                ]}
              >
                {rule.description}
              </Text>
            </TouchableOpacity>
          ))}
        </View>

        {/* Dividends */}
        <View style={styles.sectionCard}>
          <View style={styles.sectionTitleRow}>
            <Ionicons name="cash-outline" size={20} color={COLORS.accent} />
            <Text style={styles.sectionTitle}>Dividend History</Text>
          </View>
          <Text style={styles.totalDividends}>
            Total: {formatCurrency(position.total_dividends)}
          </Text>
          {position.dividend_history.length > 0 ? (
            position.dividend_history.map((dividend: any, index: number) => (
              <View key={index} style={styles.dividendItem}>
                <Text style={styles.dividendDate}>{dividend.date}</Text>
                <Text style={styles.dividendAmount}>
                  {formatCurrency(dividend.amount * (position.shares / 100))}
                </Text>
              </View>
            ))
          ) : (
            <Text style={styles.noDividends}>No dividends received</Text>
          )}
        </View>

        {/* Delete Button */}
        <TouchableOpacity
          style={styles.deleteButton}
          onPress={handleDeletePosition}
          activeOpacity={0.7}
        >
          <Ionicons name="trash-outline" size={20} color={COLORS.negative} />
          <Text style={styles.deleteButtonText}>Remove Position</Text>
        </TouchableOpacity>
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
  header: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    marginBottom: 16,
  },
  ticker: {
    fontSize: 28,
    fontWeight: '700',
    color: COLORS.text,
  },
  companyName: {
    fontSize: 14,
    color: COLORS.textLight,
    marginTop: 4,
  },
  currentPrice: {
    alignItems: 'flex-end',
  },
  priceLabel: {
    fontSize: 12,
    color: COLORS.textMuted,
  },
  priceValue: {
    fontSize: 22,
    fontWeight: '600',
    color: COLORS.text,
  },
  metricsCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 16,
    marginBottom: 16,
  },
  metricsRow: {
    flexDirection: 'row',
  },
  metricItem: {
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
  divider: {
    height: 1,
    backgroundColor: COLORS.border,
    marginVertical: 12,
  },
  chartCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 16,
    marginBottom: 16,
  },
  chartContainer: {
    alignItems: 'center',
    marginTop: 12,
  },
  sectionCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 16,
    marginBottom: 16,
  },
  sectionHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: 12,
  },
  sectionTitleRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
    marginBottom: 12,
  },
  sectionTitle: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.text,
  },
  editButton: {
    padding: 4,
  },
  thesisText: {
    fontSize: 15,
    color: COLORS.textLight,
    fontStyle: 'italic',
    lineHeight: 22,
  },
  notesText: {
    fontSize: 14,
    color: COLORS.textLight,
    lineHeight: 20,
  },
  textInput: {
    backgroundColor: COLORS.background,
    borderRadius: 10,
    padding: 12,
    fontSize: 14,
    color: COLORS.text,
    borderWidth: 1,
    borderColor: COLORS.border,
    minHeight: 60,
    textAlignVertical: 'top',
  },
  saveButton: {
    backgroundColor: COLORS.primary,
    borderRadius: 8,
    paddingVertical: 10,
    alignItems: 'center',
    marginTop: 10,
  },
  saveButtonText: {
    fontSize: 14,
    fontWeight: '600',
    color: '#FFFFFF',
  },
  rulesSubtitle: {
    fontSize: 12,
    color: COLORS.textMuted,
    marginBottom: 12,
    marginTop: -8,
  },
  ruleItem: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 12,
    paddingVertical: 10,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  ruleText: {
    flex: 1,
    fontSize: 14,
    color: COLORS.text,
  },
  ruleTextInactive: {
    color: COLORS.textMuted,
    textDecorationLine: 'line-through',
  },
  totalDividends: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.accent,
    marginBottom: 12,
    marginTop: -4,
  },
  dividendItem: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    paddingVertical: 8,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  dividendDate: {
    fontSize: 14,
    color: COLORS.textLight,
  },
  dividendAmount: {
    fontSize: 14,
    fontWeight: '500',
    color: COLORS.text,
  },
  noDividends: {
    fontSize: 14,
    color: COLORS.textMuted,
    fontStyle: 'italic',
  },
  deleteButton: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 8,
    paddingVertical: 14,
    marginTop: 8,
  },
  deleteButtonText: {
    fontSize: 14,
    color: COLORS.negative,
  },
});
