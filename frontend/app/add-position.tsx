import React, { useState, useEffect } from 'react';
import {
  View,
  Text,
  StyleSheet,
  TouchableOpacity,
  TextInput,
  ScrollView,
  KeyboardAvoidingView,
  Platform,
  ActivityIndicator,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useRouter } from 'expo-router';
import AsyncStorage from '@react-native-async-storage/async-storage';
import { Ionicons } from '@expo/vector-icons';
import axios from 'axios';
import { COLORS } from './_layout';
import { useAppDialog } from '../contexts/AppDialogContext';
import { API_URL } from '../utils/config';

export default function AddPosition() {
  const router = useRouter();
  const dialog = useAppDialog();
  const [loading, setLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<any[]>([]);
  const [selectedTicker, setSelectedTicker] = useState<any>(null);
  const [buyDate, setBuyDate] = useState('');
  const [entryPrice, setEntryPrice] = useState('');
  const [shares, setShares] = useState('');
  const [thesis, setThesis] = useState('');

  useEffect(() => {
    if (searchQuery.length >= 1) {
      searchTickers();
    } else {
      setSearchResults([]);
    }
  }, [searchQuery]);

  const searchTickers = async () => {
    try {
      const response = await axios.get(`${API_URL}/api/search?q=${searchQuery}`);
      setSearchResults((response.data.results || []).slice(0, 5));
    } catch (error) {
      console.error('Error searching tickers:', error);
    }
  };

  const selectTicker = async (ticker: any) => {
    setSelectedTicker(ticker);
    setSearchQuery(ticker.ticker);
    setSearchResults([]);

    try {
      const response = await axios.get(`${API_URL}/api/stock/${ticker.ticker}`);
      setEntryPrice(response.data.current_price.toFixed(2));
    } catch (error) {
      console.error('Error getting ticker info:', error);
    }
  };

  const formatDate = (text: string) => {
    const cleaned = text.replace(/[^0-9]/g, '');
    if (cleaned.length <= 4) return cleaned;
    if (cleaned.length <= 6) return `${cleaned.slice(0, 4)}-${cleaned.slice(4)}`;
    return `${cleaned.slice(0, 4)}-${cleaned.slice(4, 6)}-${cleaned.slice(6, 8)}`;
  };

  const handleAddPosition = async () => {
    if (!selectedTicker || !buyDate || !entryPrice || !shares || !thesis.trim()) {
      dialog.alert('Missing Information', 'Please fill in all fields');
      return;
    }

    setLoading(true);
    try {
      const portfolioId = await AsyncStorage.getItem('portfolioId');
      if (!portfolioId) {
        dialog.alert('Error', 'Portfolio not found');
        return;
      }

      await axios.post(`${API_URL}/api/positions`, {
        portfolio_id: portfolioId,
        ticker: selectedTicker.ticker,
        buy_date: buyDate,
        entry_price: parseFloat(entryPrice),
        shares: parseFloat(shares),
        thesis: thesis.trim(),
        rules: [
          { description: 'Sell if price drops 20%', is_followed: true },
          { description: 'Review quarterly earnings', is_followed: true },
        ],
      });

      router.back();
    } catch (error: any) {
      if (error.response?.status === 400) {
        dialog.alert('Limit Reached', error.response.data.detail);
      } else {
        dialog.alert('Error', 'Failed to add position');
      }
    } finally {
      setLoading(false);
    }
  };

  const canSubmit = selectedTicker && buyDate.length === 10 && entryPrice && shares && thesis.trim();

  return (
    <SafeAreaView style={styles.container}>
      <KeyboardAvoidingView
        behavior={Platform.OS === 'ios' ? 'padding' : 'height'}
        style={styles.keyboardView}
      >
        <View style={styles.header}>
          <TouchableOpacity
            style={styles.backButton}
            onPress={() => router.back()}
          >
            <Ionicons name="close" size={24} color={COLORS.text} />
          </TouchableOpacity>
          <Text style={styles.headerTitle}>Add Position</Text>
          <View style={styles.placeholder} />
        </View>

        <ScrollView
          style={styles.scrollView}
          contentContainerStyle={styles.scrollContent}
          keyboardShouldPersistTaps="handled"
        >
          {/* Ticker Search */}
          <View style={styles.inputGroup}>
            <Text style={styles.label}>Ticker Symbol</Text>
            <TextInput
              style={styles.textInput}
              placeholder="Search ticker (e.g., AAPL)"
              placeholderTextColor={COLORS.textMuted}
              value={searchQuery}
              onChangeText={(text) => {
                setSearchQuery(text.toUpperCase());
                if (selectedTicker && text.toUpperCase() !== selectedTicker.ticker) {
                  setSelectedTicker(null);
                }
              }}
              autoCapitalize="characters"
            />
            {searchResults.length > 0 && (
              <View style={styles.searchResults}>
                {searchResults.map((result) => (
                  <TouchableOpacity
                    key={result.ticker}
                    style={styles.searchResultItem}
                    onPress={() => selectTicker(result)}
                  >
                    <Text style={styles.searchResultTicker}>{result.ticker}</Text>
                    <Text style={styles.searchResultName} numberOfLines={1}>
                      {result.name}
                    </Text>
                  </TouchableOpacity>
                ))}
              </View>
            )}
            {selectedTicker && (
              <View style={styles.selectedTicker}>
                <Ionicons name="checkmark-circle" size={18} color={COLORS.positive} />
                <Text style={styles.selectedTickerText}>
                  {selectedTicker.name}
                </Text>
              </View>
            )}
          </View>

          {/* Buy Date */}
          <View style={styles.inputGroup}>
            <Text style={styles.label}>Buy Date</Text>
            <TextInput
              style={styles.textInput}
              placeholder="YYYY-MM-DD"
              placeholderTextColor={COLORS.textMuted}
              value={buyDate}
              onChangeText={(text) => setBuyDate(formatDate(text))}
              keyboardType="number-pad"
              maxLength={10}
            />
          </View>

          {/* Entry Price & Shares */}
          <View style={styles.rowInputs}>
            <View style={[styles.inputGroup, { flex: 1 }]}>
              <Text style={styles.label}>Entry Price ($)</Text>
              <TextInput
                style={styles.textInput}
                placeholder="0.00"
                placeholderTextColor={COLORS.textMuted}
                value={entryPrice}
                onChangeText={setEntryPrice}
                keyboardType="decimal-pad"
              />
            </View>
            <View style={[styles.inputGroup, { flex: 1, marginLeft: 12 }]}>
              <Text style={styles.label}>Shares</Text>
              <TextInput
                style={styles.textInput}
                placeholder="0"
                placeholderTextColor={COLORS.textMuted}
                value={shares}
                onChangeText={setShares}
                keyboardType="decimal-pad"
              />
            </View>
          </View>

          {/* Thesis */}
          <View style={styles.inputGroup}>
            <Text style={styles.label}>Investment Thesis (1 sentence)</Text>
            <TextInput
              style={[styles.textInput, styles.textArea]}
              placeholder="Why are you investing in this stock?"
              placeholderTextColor={COLORS.textMuted}
              value={thesis}
              onChangeText={setThesis}
              multiline
              numberOfLines={3}
              maxLength={150}
            />
            <Text style={styles.charCount}>{thesis.length}/150</Text>
          </View>

          {/* Info Box */}
          <View style={styles.infoBox}>
            <Ionicons name="information-circle-outline" size={20} color={COLORS.accent} />
            <Text style={styles.infoText}>
              Default rules will be added: "Sell if price drops 20%" and "Review quarterly earnings". You can edit these in position details.
            </Text>
          </View>

          <TouchableOpacity
            style={[
              styles.submitButton,
              !canSubmit && styles.submitButtonDisabled,
            ]}
            onPress={handleAddPosition}
            disabled={!canSubmit || loading}
            activeOpacity={0.8}
          >
            {loading ? (
              <ActivityIndicator color="#FFFFFF" />
            ) : (
              <>
                <Ionicons name="add" size={20} color="#FFFFFF" />
                <Text style={styles.submitButtonText}>Add Position</Text>
              </>
            )}
          </TouchableOpacity>
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
  scrollView: {
    flex: 1,
  },
  scrollContent: {
    padding: 16,
    paddingBottom: 32,
  },
  inputGroup: {
    marginBottom: 20,
  },
  label: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.text,
    marginBottom: 8,
  },
  textInput: {
    backgroundColor: COLORS.card,
    borderRadius: 12,
    paddingHorizontal: 16,
    paddingVertical: 14,
    fontSize: 16,
    color: COLORS.text,
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  textArea: {
    minHeight: 80,
    textAlignVertical: 'top',
  },
  charCount: {
    fontSize: 12,
    color: COLORS.textMuted,
    textAlign: 'right',
    marginTop: 4,
  },
  rowInputs: {
    flexDirection: 'row',
  },
  searchResults: {
    backgroundColor: COLORS.card,
    borderRadius: 12,
    marginTop: 8,
    borderWidth: 1,
    borderColor: COLORS.border,
    overflow: 'hidden',
  },
  searchResultItem: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 16,
    paddingVertical: 12,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  searchResultTicker: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.primary,
    width: 60,
  },
  searchResultName: {
    fontSize: 14,
    color: COLORS.textLight,
    flex: 1,
  },
  selectedTicker: {
    flexDirection: 'row',
    alignItems: 'center',
    marginTop: 8,
    gap: 6,
  },
  selectedTickerText: {
    fontSize: 14,
    color: COLORS.positive,
  },
  infoBox: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    backgroundColor: '#F0F5F3',
    borderRadius: 12,
    padding: 14,
    gap: 10,
    marginBottom: 24,
  },
  infoText: {
    flex: 1,
    fontSize: 13,
    color: COLORS.textLight,
    lineHeight: 18,
  },
  submitButton: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: COLORS.primary,
    borderRadius: 12,
    paddingVertical: 16,
    gap: 8,
  },
  submitButtonDisabled: {
    backgroundColor: COLORS.textMuted,
  },
  submitButtonText: {
    fontSize: 16,
    fontWeight: '600',
    color: '#FFFFFF',
  },
});
