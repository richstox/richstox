import React from 'react';
import {
  View,
  Text,
  StyleSheet,
  TouchableOpacity,
  SafeAreaView,
} from 'react-native';
import { useRouter } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import { COLORS } from '../_layout';

export default function OnboardingStep2() {
  const router = useRouter();

  return (
    <SafeAreaView style={styles.container}>
      <View style={styles.content}>
        <TouchableOpacity
          style={styles.backButton}
          onPress={() => router.back()}
        >
          <Ionicons name="arrow-back" size={24} color={COLORS.text} />
        </TouchableOpacity>

        <View style={styles.header}>
          <Text style={styles.stepText}>Step 2 of 3</Text>
          <Text style={styles.title}>How tracking works now</Text>
          <Text style={styles.subtitle}>
            Keep the mental model simple and strict.
          </Text>
        </View>

        <View style={styles.stack}>
          <View style={styles.optionCard}>
            <Ionicons name="eye-outline" size={28} color={COLORS.primary} />
            <Text style={styles.optionTitle}>Watchlist</Text>
            <Text style={styles.optionText}>
              Add from Last close. Tracking starts from the next close and shows up in My Stocks.
            </Text>
          </View>

          <View style={styles.optionCard}>
            <Ionicons name="analytics-outline" size={28} color={COLORS.primary} />
            <Text style={styles.optionTitle}>Tracklist</Text>
            <Text style={styles.optionText}>
              Exactly 7 stocks, equal-weight, replace-only after setup, and daily performance on the homepage.
            </Text>
          </View>
        </View>

        <TouchableOpacity
          style={styles.continueButton}
          onPress={() => router.push('/onboarding/step3')}
          activeOpacity={0.8}
        >
          <Text style={styles.continueButtonText}>Continue</Text>
          <Ionicons name="arrow-forward" size={20} color="#FFFFFF" />
        </TouchableOpacity>
      </View>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: COLORS.background,
  },
  content: {
    flex: 1,
    paddingHorizontal: 24,
    paddingTop: 16,
    paddingBottom: 40,
  },
  backButton: {
    width: 44,
    height: 44,
    alignItems: 'center',
    justifyContent: 'center',
    marginLeft: -8,
  },
  header: {
    marginBottom: 32,
    marginTop: 8,
  },
  stepText: {
    fontSize: 14,
    color: COLORS.textMuted,
    marginBottom: 8,
  },
  title: {
    fontSize: 28,
    fontWeight: '700',
    color: COLORS.text,
    marginBottom: 8,
  },
  subtitle: {
    fontSize: 16,
    color: COLORS.textLight,
    lineHeight: 24,
  },
  stack: {
    flex: 1,
    gap: 16,
  },
  optionCard: {
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 24,
    gap: 10,
  },
  optionTitle: {
    fontSize: 18,
    fontWeight: '700',
    color: COLORS.text,
  },
  optionText: {
    fontSize: 14,
    lineHeight: 20,
    color: COLORS.textLight,
  },
  continueButton: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: COLORS.primary,
    borderRadius: 12,
    paddingVertical: 16,
    gap: 8,
    marginTop: 24,
  },
  continueButtonText: {
    fontSize: 16,
    fontWeight: '600',
    color: '#FFFFFF',
  },
});
