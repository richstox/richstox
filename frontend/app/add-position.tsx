import React from 'react';
import { View, Text, StyleSheet, TouchableOpacity } from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useRouter } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import { COLORS } from './_layout';

export default function AddPosition() {
  const router = useRouter();

  return (
    <SafeAreaView style={styles.container}>
      <View style={styles.header}>
        <TouchableOpacity style={styles.backButton} onPress={() => router.back()}>
          <Ionicons name="close" size={24} color={COLORS.text} />
        </TouchableOpacity>
        <Text style={styles.headerTitle}>Legacy Portfolio</Text>
        <View style={styles.placeholder} />
      </View>

      <View style={styles.content}>
        <View style={styles.card}>
          <View style={styles.badge}>
            <Text style={styles.badgeText}>Disabled</Text>
          </View>
          <Ionicons name="briefcase-outline" size={34} color={COLORS.warning} />
          <Text style={styles.title}>Add Position is no longer available</Text>
          <Text style={styles.text}>
            Portfolio flows were retired. Open a ticker detail page and use + Add to in Last close for Watchlist or Tracklist.
          </Text>
          <TouchableOpacity style={styles.primaryButton} onPress={() => router.replace('/(tabs)/dashboard')}>
            <Text style={styles.primaryButtonText}>Go to Home</Text>
          </TouchableOpacity>
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
    alignItems: 'center',
    justifyContent: 'space-between',
    paddingHorizontal: 16,
    paddingVertical: 12,
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
    flex: 1,
    padding: 24,
  },
  card: {
    backgroundColor: COLORS.card,
    borderRadius: 18,
    padding: 24,
    gap: 14,
  },
  badge: {
    alignSelf: 'flex-start',
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 999,
    backgroundColor: '#FEE2E2',
  },
  badgeText: {
    fontSize: 12,
    fontWeight: '700',
    color: '#B91C1C',
  },
  title: {
    fontSize: 22,
    fontWeight: '700',
    color: COLORS.text,
  },
  text: {
    fontSize: 15,
    lineHeight: 22,
    color: COLORS.textLight,
  },
  primaryButton: {
    marginTop: 6,
    alignSelf: 'flex-start',
    paddingHorizontal: 16,
    paddingVertical: 12,
    borderRadius: 12,
    backgroundColor: COLORS.primary,
  },
  primaryButtonText: {
    fontSize: 14,
    fontWeight: '700',
    color: '#FFFFFF',
  },
});
