import React from 'react';
import {
  View,
  Text,
  StyleSheet,
  TouchableOpacity,
  ScrollView,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useRouter } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import { COLORS } from '../_layout';

export default function CalculatorIndex() {
  const router = useRouter();

  return (
    <SafeAreaView style={styles.container}>
      <View style={styles.header}>
        <TouchableOpacity style={styles.backButton} onPress={() => router.back()}>
          <Ionicons name="arrow-back" size={24} color={COLORS.text} />
        </TouchableOpacity>
        <Text style={styles.headerTitle}>Calculators</Text>
        <View style={styles.placeholder} />
      </View>

      <ScrollView style={styles.content}>
        <Text style={styles.description}>
          Simulate different investment strategies to understand potential outcomes.
        </Text>

        <TouchableOpacity
          style={styles.calcCard}
          onPress={() => router.push('/calculator/buy-hold')}
        >
          <View style={[styles.iconContainer, { backgroundColor: '#EBF5FF' }]}>
            <Ionicons name="trending-up" size={28} color={COLORS.primary} />
          </View>
          <View style={styles.calcInfo}>
            <Text style={styles.calcTitle}>Buy & Hold</Text>
            <Text style={styles.calcDesc}>
              Calculate returns for a one-time lump sum investment held over time.
            </Text>
          </View>
          <Ionicons name="chevron-forward" size={24} color={COLORS.textMuted} />
        </TouchableOpacity>

        <TouchableOpacity
          style={styles.calcCard}
          onPress={() => router.push('/calculator/dca')}
        >
          <View style={[styles.iconContainer, { backgroundColor: '#F0FDF4' }]}>
            <Ionicons name="repeat" size={28} color={COLORS.accent} />
          </View>
          <View style={styles.calcInfo}>
            <Text style={styles.calcTitle}>Dollar Cost Averaging</Text>
            <Text style={styles.calcDesc}>
              Simulate regular monthly investments over time.
            </Text>
          </View>
          <Ionicons name="chevron-forward" size={24} color={COLORS.textMuted} />
        </TouchableOpacity>

        <TouchableOpacity
          style={styles.calcCard}
          onPress={() => router.push('/calculator/portfolio')}
        >
          <View style={[styles.iconContainer, { backgroundColor: '#FEF3C7' }]}>
            <Ionicons name="pie-chart" size={28} color="#F59E0B" />
          </View>
          <View style={styles.calcInfo}>
            <Text style={styles.calcTitle}>Portfolio Value</Text>
            <Text style={styles.calcDesc}>
              See historical value of a portfolio with custom allocations.
            </Text>
          </View>
          <Ionicons name="chevron-forward" size={24} color={COLORS.textMuted} />
        </TouchableOpacity>

        <View style={styles.tip}>
          <Ionicons name="bulb-outline" size={20} color={COLORS.accent} />
          <Text style={styles.tipText}>
            Past performance doesn't guarantee future results. Use these tools for education only.
          </Text>
        </View>
      </ScrollView>
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
  description: {
    fontSize: 15,
    color: COLORS.textLight,
    marginBottom: 24,
    lineHeight: 22,
  },
  calcCard: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: COLORS.card,
    borderRadius: 16,
    padding: 16,
    marginBottom: 12,
  },
  iconContainer: {
    width: 56,
    height: 56,
    borderRadius: 16,
    alignItems: 'center',
    justifyContent: 'center',
    marginRight: 16,
  },
  calcInfo: {
    flex: 1,
  },
  calcTitle: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.text,
    marginBottom: 4,
  },
  calcDesc: {
    fontSize: 13,
    color: COLORS.textLight,
    lineHeight: 18,
  },
  tip: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    backgroundColor: '#F0F5F3',
    borderRadius: 12,
    padding: 16,
    marginTop: 16,
    gap: 12,
  },
  tipText: {
    flex: 1,
    fontSize: 13,
    color: COLORS.textLight,
    fontStyle: 'italic',
    lineHeight: 18,
  },
});
