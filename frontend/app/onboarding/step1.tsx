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

export default function OnboardingStep1() {
  const router = useRouter();

  return (
    <SafeAreaView style={styles.container}>
      <View style={styles.content}>
        <View style={styles.header}>
          <Text style={styles.stepText}>Step 1 of 3</Text>
          <Text style={styles.title}>Welcome back to RICHSTOX</Text>
          <Text style={styles.subtitle}>
            We now focus on two clear flows: Watchlist for close-based tracking and Tracklist for your 7-stock scorecard.
          </Text>
        </View>

        <View style={styles.card}>
          <Ionicons name="sparkles-outline" size={34} color={COLORS.primary} />
          <Text style={styles.cardTitle}>Portfolio is being rebuilt</Text>
          <Text style={styles.cardText}>
            Portfolio is temporarily disabled while we rebuild it from scratch. You can still track ideas with Watchlist and Tracklist.
          </Text>
        </View>

        <TouchableOpacity
          style={styles.continueButton}
          onPress={() => router.push('/onboarding/step2')}
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
    paddingTop: 40,
    paddingBottom: 40,
  },
  header: {
    marginBottom: 32,
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
  card: {
    flex: 1,
    backgroundColor: COLORS.card,
    borderRadius: 18,
    padding: 24,
    gap: 14,
  },
  cardTitle: {
    fontSize: 20,
    fontWeight: '700',
    color: COLORS.text,
  },
  cardText: {
    fontSize: 15,
    lineHeight: 22,
    color: COLORS.textLight,
  },
  continueButton: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: COLORS.primary,
    borderRadius: 12,
    paddingVertical: 16,
    marginTop: 24,
    gap: 8,
  },
  continueButtonText: {
    fontSize: 16,
    fontWeight: '600',
    color: '#FFFFFF',
  },
});
