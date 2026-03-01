import React, { useState } from 'react';
import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  TouchableOpacity,
  Platform,
  Image,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { useRouter } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';

const COLORS = {
  primary: '#1E3A5F',
  accent: '#10B981',
  warning: '#F59E0B',
  danger: '#EF4444',
  text: '#1F2937',
  textLight: '#6B7280',
  textMuted: '#9CA3AF',
  background: '#F5F7FA',
  card: '#FFFFFF',
  border: '#E5E7EB',
  gold: '#F59E0B',
  silver: '#9CA3AF',
  bronze: '#CD7F32',
};

// Mock leaderboard data
const LEADERBOARD = [
  { rank: 1, name: 'TechInvestor', return: 45.2, drawdown: 12.3, days: 365, companies: 15 },
  { rank: 2, name: 'ValueHunter', return: 38.7, drawdown: 8.5, days: 420, companies: 8 },
  { rank: 3, name: 'DividendKing', return: 32.1, drawdown: 5.2, days: 512, companies: 22 },
  { rank: 4, name: 'GrowthSeeker', return: 28.9, drawdown: 15.7, days: 180, companies: 12 },
  { rank: 5, name: 'SafetyFirst', return: 24.5, drawdown: 3.8, days: 730, companies: 6 },
  { rank: 6, name: 'MarketWatcher', return: 21.3, drawdown: 9.4, days: 290, companies: 18 },
  { rank: 7, name: 'LongTermView', return: 19.8, drawdown: 7.1, days: 580, companies: 10 },
  { rank: 8, name: 'PatientTrader', return: 17.2, drawdown: 4.5, days: 445, companies: 14 },
];

export default function Leagues() {
  const router = useRouter();
  const [sortBy, setSortBy] = useState<'return' | 'drawdown' | 'days'>('return');

  const getRankColor = (rank: number) => {
    if (rank === 1) return COLORS.gold;
    if (rank === 2) return COLORS.silver;
    if (rank === 3) return COLORS.bronze;
    return COLORS.textMuted;
  };

  const getRankIcon = (rank: number) => {
    if (rank <= 3) return 'trophy';
    return 'medal-outline';
  };

  const sortedLeaderboard = [...LEADERBOARD].sort((a, b) => {
    if (sortBy === 'return') return b.return - a.return;
    if (sortBy === 'drawdown') return a.drawdown - b.drawdown; // Lower is better
    return b.days - a.days;
  });

  return (
    <SafeAreaView style={styles.container} edges={['top']}>
      {/* Header */}
      <View style={styles.header}>
        <Image source={require('../../assets/images/richstox_icon.png')} style={styles.logo} />
        <Text style={styles.headerTitle}>Leagues</Text>
        <View style={styles.headerRight}>
          <TouchableOpacity style={styles.headerIcon} onPress={() => router.push('/(tabs)/search')}>
            <Ionicons name="search-outline" size={22} color={COLORS.text} />
          </TouchableOpacity>
          <TouchableOpacity style={styles.headerIcon}>
            <Ionicons name="notifications-outline" size={22} color={COLORS.text} />
          </TouchableOpacity>
        </View>
      </View>

      <ScrollView style={styles.scroll} showsVerticalScrollIndicator={false}>
        {/* Info Banner */}
        <View style={styles.infoBanner}>
          <Ionicons name="information-circle-outline" size={20} color={COLORS.primary} />
          <Text style={styles.infoText}>
            Rankings based on public watchlists. Return + Max Drawdown shown.
          </Text>
        </View>

        {/* Sort Options */}
        <View style={styles.sortSection}>
          <Text style={styles.sortLabel}>Sort by:</Text>
          <View style={styles.sortButtons}>
            {[
              { key: 'return', label: 'Return' },
              { key: 'drawdown', label: 'Risk' },
              { key: 'days', label: 'Track Record' },
            ].map(option => (
              <TouchableOpacity
                key={option.key}
                style={[
                  styles.sortButton,
                  sortBy === option.key && styles.sortButtonActive
                ]}
                onPress={() => setSortBy(option.key as any)}
              >
                <Text style={[
                  styles.sortButtonText,
                  sortBy === option.key && styles.sortButtonTextActive
                ]}>{option.label}</Text>
              </TouchableOpacity>
            ))}
          </View>
        </View>

        {/* Leaderboard */}
        <View style={styles.leaderboard}>
          <Text style={styles.sectionTitle}>Leaderboard</Text>
          
          {/* Header Row */}
          <View style={styles.tableHeader}>
            <Text style={[styles.tableHeaderText, { width: 40 }]}>#</Text>
            <Text style={[styles.tableHeaderText, { flex: 1 }]}>User</Text>
            <Text style={[styles.tableHeaderText, { width: 70, textAlign: 'right' }]}>Return</Text>
            <Text style={[styles.tableHeaderText, { width: 70, textAlign: 'right' }]}>Drawdown</Text>
          </View>

          {sortedLeaderboard.map((user, index) => (
            <TouchableOpacity
              key={user.rank}
              style={[
                styles.leaderboardRow,
                index === sortedLeaderboard.length - 1 && styles.lastRow
              ]}
            >
              <View style={[styles.rankBadge, { backgroundColor: getRankColor(user.rank) + '20' }]}>
                <Ionicons 
                  name={getRankIcon(user.rank) as any} 
                  size={14} 
                  color={getRankColor(user.rank)} 
                />
                <Text style={[styles.rankText, { color: getRankColor(user.rank) }]}>
                  {user.rank}
                </Text>
              </View>
              
              <View style={styles.userInfo}>
                <Text style={styles.userName}>{user.name}</Text>
                <Text style={styles.userMeta}>
                  {user.days}d · {user.companies} companies
                </Text>
              </View>
              
              <Text style={[styles.returnValue, { color: COLORS.accent }]}>
                +{user.return.toFixed(1)}%
              </Text>
              
              <Text style={[styles.drawdownValue, { color: COLORS.danger }]}>
                -{user.drawdown.toFixed(1)}%
              </Text>
            </TouchableOpacity>
          ))}
        </View>

        {/* Your Position */}
        <View style={styles.yourPosition}>
          <Text style={styles.yourPositionTitle}>Your Position</Text>
          <View style={styles.yourPositionCard}>
            <View style={styles.yourPositionLeft}>
              <Text style={styles.yourRank}>#42</Text>
              <Text style={styles.yourRankLabel}>of 1,234 users</Text>
            </View>
            <View style={styles.yourPositionRight}>
              <View style={styles.yourStat}>
                <Text style={styles.yourStatLabel}>Return</Text>
                <Text style={[styles.yourStatValue, { color: COLORS.accent }]}>+12.5%</Text>
              </View>
              <View style={styles.yourStat}>
                <Text style={styles.yourStatLabel}>Drawdown</Text>
                <Text style={[styles.yourStatValue, { color: COLORS.danger }]}>-8.3%</Text>
              </View>
            </View>
          </View>
        </View>

        <View style={{ height: 100 }} />
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.background },
  
  header: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 16,
    paddingVertical: 12,
    backgroundColor: COLORS.card,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  logo: { width: 36, height: 36 },
  headerTitle: {
    flex: 1,
    fontSize: 18,
    fontWeight: '700',
    color: COLORS.primary,
    marginLeft: 10,
  },
  headerRight: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  headerIcon: { padding: 6 },

  scroll: { flex: 1 },

  infoBanner: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: '#EBF5FF',
    padding: 12,
    margin: 16,
    borderRadius: 10,
    gap: 10,
  },
  infoText: {
    flex: 1,
    fontSize: 13,
    color: COLORS.primary,
    lineHeight: 18,
  },

  sortSection: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 16,
    marginBottom: 12,
    gap: 10,
  },
  sortLabel: {
    fontSize: 13,
    color: COLORS.textMuted,
  },
  sortButtons: {
    flexDirection: 'row',
    gap: 6,
  },
  sortButton: {
    paddingHorizontal: 12,
    paddingVertical: 6,
    borderRadius: 16,
    backgroundColor: COLORS.card,
    borderWidth: 1,
    borderColor: COLORS.border,
  },
  sortButtonActive: {
    backgroundColor: COLORS.primary,
    borderColor: COLORS.primary,
  },
  sortButtonText: {
    fontSize: 12,
    fontWeight: '500',
    color: COLORS.textLight,
  },
  sortButtonTextActive: {
    color: '#FFF',
  },

  leaderboard: {
    backgroundColor: COLORS.card,
    marginHorizontal: 16,
    borderRadius: 16,
    padding: 16,
    ...Platform.select({
      web: { boxShadow: '0 1px 3px rgba(0,0,0,0.08)' },
      default: { elevation: 1 },
    }),
  },
  sectionTitle: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.text,
    marginBottom: 12,
  },
  tableHeader: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingBottom: 10,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  tableHeaderText: {
    fontSize: 11,
    fontWeight: '600',
    color: COLORS.textMuted,
    textTransform: 'uppercase',
  },
  leaderboardRow: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingVertical: 12,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  lastRow: {
    borderBottomWidth: 0,
  },
  rankBadge: {
    flexDirection: 'row',
    alignItems: 'center',
    width: 40,
    gap: 4,
  },
  rankText: {
    fontSize: 13,
    fontWeight: '700',
  },
  userInfo: {
    flex: 1,
  },
  userName: {
    fontSize: 14,
    fontWeight: '600',
    color: COLORS.text,
  },
  userMeta: {
    fontSize: 11,
    color: COLORS.textMuted,
    marginTop: 2,
  },
  returnValue: {
    width: 70,
    fontSize: 14,
    fontWeight: '700',
    textAlign: 'right',
  },
  drawdownValue: {
    width: 70,
    fontSize: 14,
    fontWeight: '600',
    textAlign: 'right',
  },

  yourPosition: {
    margin: 16,
  },
  yourPositionTitle: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.text,
    marginBottom: 12,
  },
  yourPositionCard: {
    flexDirection: 'row',
    backgroundColor: COLORS.primary,
    borderRadius: 16,
    padding: 20,
  },
  yourPositionLeft: {
    flex: 1,
  },
  yourRank: {
    fontSize: 36,
    fontWeight: '700',
    color: '#FFF',
  },
  yourRankLabel: {
    fontSize: 12,
    color: 'rgba(255,255,255,0.7)',
    marginTop: 4,
  },
  yourPositionRight: {
    flexDirection: 'row',
    gap: 20,
  },
  yourStat: {
    alignItems: 'flex-end',
  },
  yourStatLabel: {
    fontSize: 11,
    color: 'rgba(255,255,255,0.7)',
    marginBottom: 4,
  },
  yourStatValue: {
    fontSize: 20,
    fontWeight: '700',
  },
});
