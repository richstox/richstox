import React from 'react';
import { View, Text, StyleSheet } from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { Ionicons } from '@expo/vector-icons';
import AppHeader from '../../components/AppHeader';
import { useLayoutSpacing } from '../../constants/layout';

const COLORS = {
  primary: '#1E3A5F',
  text: '#1F2937',
  textLight: '#6B7280',
  background: '#F5F7FA',
  card: '#FFFFFF',
  border: '#E5E7EB',
};

export default function PortfolioSoonPage() {
  const sp = useLayoutSpacing();

  return (
    <SafeAreaView style={styles.container} edges={['top']}>
      <AppHeader title="Portfolio" showSubscriptionBadge={false} />
      <View style={[styles.content, { padding: sp.pageGutter }]}>
        <View style={styles.card}>
          <View style={styles.badge}>
            <Text style={styles.badgeText}>Soon</Text>
          </View>
          <Ionicons name="briefcase-outline" size={34} color={COLORS.primary} />
          <Text style={styles.title}>Portfolio is temporarily disabled</Text>
          <Text style={styles.text}>
            We are rebuilding Portfolio from scratch. For now, use Watchlist for close-based tracking and Tracklist for your 7-stock scorecard.
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
  content: {
    flex: 1,
  },
  card: {
    backgroundColor: COLORS.card,
    borderRadius: 20,
    borderWidth: 1,
    borderColor: COLORS.border,
    padding: 24,
    gap: 14,
  },
  badge: {
    alignSelf: 'flex-start',
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 999,
    backgroundColor: '#FEF3C7',
  },
  badgeText: {
    fontSize: 12,
    fontWeight: '700',
    color: '#B45309',
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
});
