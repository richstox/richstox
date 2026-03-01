import React, { useState } from 'react';
import {
  View,
  Text,
  StyleSheet,
  TouchableOpacity,
  Alert,
  ActivityIndicator,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useLocalSearchParams, useRouter } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import axios from 'axios';
import { COLORS } from './_layout';

const API_URL = process.env.EXPO_PUBLIC_BACKEND_URL;

export default function TickerNotFound() {
  const { ticker } = useLocalSearchParams();
  const router = useRouter();
  const [loading, setLoading] = useState(false);
  const [submitted, setSubmitted] = useState(false);

  const handleRequestTicker = async () => {
    setLoading(true);
    try {
      await axios.post(`${API_URL}/api/ticker-requests`, {
        ticker: (ticker as string).toUpperCase(),
      });
      setSubmitted(true);
    } catch (error) {
      Alert.alert('Error', 'Failed to submit request. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <SafeAreaView style={styles.container}>
      <View style={styles.header}>
        <TouchableOpacity
          style={styles.closeButton}
          onPress={() => router.back()}
        >
          <Ionicons name="close" size={24} color={COLORS.text} />
        </TouchableOpacity>
      </View>

      <View style={styles.content}>
        <View style={styles.iconContainer}>
          <Ionicons name="search-outline" size={64} color={COLORS.textMuted} />
        </View>

        <Text style={styles.title}>Ticker Not Found</Text>
        
        <Text style={styles.tickerText}>
          "{(ticker as string)?.toUpperCase()}"
        </Text>

        <Text style={styles.description}>
          This ticker is not currently in our whitelisted list. We focus on top S&P 500 companies to help you stay disciplined with quality investments.
        </Text>

        {!submitted ? (
          <>
            <View style={styles.infoBox}>
              <Ionicons name="information-circle-outline" size={20} color={COLORS.accent} />
              <Text style={styles.infoText}>
                You can request this ticker to be added. We review requests periodically and add tickers that meet our quality criteria.
              </Text>
            </View>

            <TouchableOpacity
              style={styles.requestButton}
              onPress={handleRequestTicker}
              disabled={loading}
              activeOpacity={0.8}
            >
              {loading ? (
                <ActivityIndicator color="#FFFFFF" />
              ) : (
                <>
                  <Ionicons name="mail-outline" size={20} color="#FFFFFF" />
                  <Text style={styles.requestButtonText}>Request This Ticker</Text>
                </>
              )}
            </TouchableOpacity>
          </>
        ) : (
          <View style={styles.successBox}>
            <Ionicons name="checkmark-circle" size={32} color={COLORS.positive} />
            <Text style={styles.successTitle}>Request Submitted</Text>
            <Text style={styles.successText}>
              Thank you for your request. We'll review it and notify you if this ticker is added.
            </Text>
          </View>
        )}

        <TouchableOpacity
          style={styles.backButton}
          onPress={() => router.back()}
          activeOpacity={0.7}
        >
          <Ionicons name="arrow-back" size={18} color={COLORS.primary} />
          <Text style={styles.backButtonText}>Back to Search</Text>
        </TouchableOpacity>

        {/* Calm Message */}
        <View style={styles.calmMessage}>
          <Ionicons name="leaf-outline" size={16} color={COLORS.accent} />
          <Text style={styles.calmText}>
            Quality over quantity. Focus on what you know.
          </Text>
        </View>
      </View>
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
    justifyContent: 'flex-end',
    paddingHorizontal: 16,
    paddingVertical: 8,
  },
  closeButton: {
    width: 44,
    height: 44,
    alignItems: 'center',
    justifyContent: 'center',
  },
  content: {
    flex: 1,
    paddingHorizontal: 24,
    alignItems: 'center',
    justifyContent: 'center',
  },
  iconContainer: {
    width: 120,
    height: 120,
    borderRadius: 60,
    backgroundColor: '#F5F5F5',
    alignItems: 'center',
    justifyContent: 'center',
    marginBottom: 24,
  },
  title: {
    fontSize: 24,
    fontWeight: '700',
    color: COLORS.text,
    marginBottom: 8,
  },
  tickerText: {
    fontSize: 20,
    fontWeight: '600',
    color: COLORS.primary,
    marginBottom: 16,
  },
  description: {
    fontSize: 15,
    color: COLORS.textLight,
    textAlign: 'center',
    lineHeight: 22,
    marginBottom: 24,
  },
  infoBox: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    backgroundColor: '#F0F5F3',
    borderRadius: 12,
    padding: 16,
    gap: 12,
    marginBottom: 24,
  },
  infoText: {
    flex: 1,
    fontSize: 13,
    color: COLORS.textLight,
    lineHeight: 18,
  },
  requestButton: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: COLORS.primary,
    borderRadius: 12,
    paddingVertical: 16,
    paddingHorizontal: 32,
    gap: 8,
    marginBottom: 16,
    width: '100%',
  },
  requestButtonText: {
    fontSize: 16,
    fontWeight: '600',
    color: '#FFFFFF',
  },
  successBox: {
    alignItems: 'center',
    backgroundColor: '#F0F5F3',
    borderRadius: 16,
    padding: 24,
    marginBottom: 24,
    width: '100%',
  },
  successTitle: {
    fontSize: 18,
    fontWeight: '600',
    color: COLORS.text,
    marginTop: 12,
    marginBottom: 8,
  },
  successText: {
    fontSize: 14,
    color: COLORS.textLight,
    textAlign: 'center',
    lineHeight: 20,
  },
  backButton: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
    paddingVertical: 12,
  },
  backButtonText: {
    fontSize: 14,
    color: COLORS.primary,
  },
  calmMessage: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
    marginTop: 32,
  },
  calmText: {
    fontSize: 13,
    color: COLORS.textLight,
    fontStyle: 'italic',
  },
});
