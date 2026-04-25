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

export default function OnboardingStep3() {
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
          <Text style={styles.stepText}>Step 3 of 3</Text>
          <Text style={styles.title}>You are ready</Text>
          <Text style={styles.subtitle}>
            Open any ticker, use the Last close card, and build your Watchlist or 7-stock Tracklist from there.
          </Text>
        </View>

        <View style={styles.card}>
          <Ionicons name="checkmark-circle-outline" size={36} color={COLORS.primary} />
          <Text style={styles.cardTitle}>Single source of action</Text>
          <Text style={styles.cardText}>
            The only place to add a stock is now the + Add to control in Last close. Search stays discovery-only, except for Tracklist replacement mode.
          </Text>
        </View>

        <TouchableOpacity
          style={styles.completeButton}
          onPress={() => router.replace('/(tabs)/dashboard')}
          activeOpacity={0.8}
        >
          <Text style={styles.completeButtonText}>Go to Home</Text>
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
    marginBottom: 24,
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
  completeButton: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: COLORS.primary,
    borderRadius: 12,
    paddingVertical: 16,
    gap: 8,
    marginTop: 24,
  },
  completeButtonText: {
    fontSize: 16,
    fontWeight: '600',
    color: '#FFFFFF',
  },
});
