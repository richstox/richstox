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
import { FONTS } from '../_layout';
import { useLayoutSpacing } from '../../constants/layout';

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
};

// Mock sectors data
const SECTORS = [
  { name: 'Technology', count: 245, icon: 'hardware-chip-outline' },
  { name: 'Healthcare', count: 180, icon: 'medkit-outline' },
  { name: 'Finance', count: 156, icon: 'cash-outline' },
  { name: 'Consumer', count: 134, icon: 'cart-outline' },
  { name: 'Energy', count: 89, icon: 'flash-outline' },
  { name: 'Industrial', count: 112, icon: 'construct-outline' },
];

// Mock news data
const NEWS = [
  { id: 1, ticker: 'AAPL', sector: 'Technology', title: 'Apple announces new AI features for iPhone 17', time: '2h ago', source: 'Reuters' },
  { id: 2, ticker: 'MSFT', sector: 'Technology', title: 'Microsoft Azure revenue grows 29% YoY in Q4', time: '3h ago', source: 'Bloomberg' },
  { id: 3, ticker: 'JPM', sector: 'Finance', title: 'JPMorgan raises dividend by 12% after stress test', time: '4h ago', source: 'CNBC' },
  { id: 4, ticker: 'JNJ', sector: 'Healthcare', title: 'Johnson & Johnson wins FDA approval for new drug', time: '5h ago', source: 'FT' },
  { id: 5, ticker: 'GOOGL', sector: 'Technology', title: 'Google faces new antitrust probe in European Union', time: '6h ago', source: 'WSJ' },
  { id: 6, ticker: 'XOM', sector: 'Energy', title: 'Exxon reports record quarterly profit on oil prices', time: '7h ago', source: 'Reuters' },
];

export default function Markets() {
  const router = useRouter();
  const sp = useLayoutSpacing();
  const [selectedSector, setSelectedSector] = useState<string | null>(null);
  const [notifications, setNotifications] = useState<Set<string>>(new Set());

  const filteredNews = selectedSector 
    ? NEWS.filter(n => n.sector === selectedSector)
    : NEWS;

  const toggleNotification = (sector: string) => {
    setNotifications(prev => {
      const next = new Set(prev);
      if (next.has(sector)) {
        next.delete(sector);
      } else {
        next.add(sector);
      }
      return next;
    });
  };

  return (
    <SafeAreaView style={styles.container} edges={['top']}>
      {/* Header */}
      <View style={[styles.header, { paddingHorizontal: sp.pageGutter }]}>
        <Image source={require('../../assets/images/richstox_icon.png')} style={styles.logo} />
        <Text style={styles.headerTitle}>Markets</Text>
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
        {/* Sector Filters - Livesport Style */}
        <View style={styles.filtersSection}>
          <Text style={styles.sectionTitle}>Filter by Sector</Text>
          <ScrollView 
            horizontal 
            showsHorizontalScrollIndicator={false}
            contentContainerStyle={styles.filtersScroll}
          >
            <TouchableOpacity
              style={[
                styles.filterChip,
                !selectedSector && styles.filterChipActive
              ]}
              onPress={() => setSelectedSector(null)}
            >
              <Text style={[
                styles.filterChipText,
                !selectedSector && styles.filterChipTextActive
              ]}>All</Text>
            </TouchableOpacity>
            
            {SECTORS.map(sector => (
              <View key={sector.name} style={styles.filterWithBell}>
                <TouchableOpacity
                  style={[
                    styles.filterChip,
                    selectedSector === sector.name && styles.filterChipActive
                  ]}
                  onPress={() => setSelectedSector(
                    selectedSector === sector.name ? null : sector.name
                  )}
                >
                  <Ionicons 
                    name={sector.icon as any} 
                    size={14} 
                    color={selectedSector === sector.name ? '#FFF' : COLORS.textLight} 
                  />
                  <Text style={[
                    styles.filterChipText,
                    selectedSector === sector.name && styles.filterChipTextActive
                  ]}>{sector.name}</Text>
                </TouchableOpacity>
                
                {/* Notification Bell */}
                <TouchableOpacity
                  style={styles.bellButton}
                  onPress={() => toggleNotification(sector.name)}
                >
                  <Ionicons 
                    name={notifications.has(sector.name) ? 'notifications' : 'notifications-outline'} 
                    size={16} 
                    color={notifications.has(sector.name) ? COLORS.warning : COLORS.textMuted} 
                  />
                </TouchableOpacity>
              </View>
            ))}
          </ScrollView>
        </View>

        {/* News Feed */}
        <View style={styles.newsSection}>
          <Text style={styles.sectionTitle}>
            {selectedSector ? `${selectedSector} News` : 'All News'}
          </Text>
          <Text style={styles.newsCount}>{filteredNews.length} articles</Text>

          {filteredNews.map((news, index) => (
            <TouchableOpacity
              key={news.id}
              style={[
                styles.newsCard,
                index === filteredNews.length - 1 && styles.lastCard
              ]}
            >
              <View style={styles.newsHeader}>
                <View style={styles.tickerBadge}>
                  <Text style={styles.tickerText}>{news.ticker}</Text>
                </View>
                <Text style={styles.newsSector}>{news.sector}</Text>
                <TouchableOpacity style={styles.newsbell}>
                  <Ionicons name="notifications-outline" size={14} color={COLORS.textMuted} />
                </TouchableOpacity>
              </View>
              <Text style={styles.newsTitle}>{news.title}</Text>
              <Text style={styles.newsMeta}>{news.time} · {news.source}</Text>
            </TouchableOpacity>
          ))}
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
    fontFamily: FONTS.heading,
    color: COLORS.primary,
    marginLeft: 10,
  },
  headerRight: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  headerIcon: { padding: 6 },

  scroll: { flex: 1 },

  filtersSection: {
    backgroundColor: COLORS.card,
    paddingVertical: 16,
    marginBottom: 8,
  },
  sectionTitle: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.text,
    paddingHorizontal: 16,
    marginBottom: 12,
  },
  filtersScroll: {
    paddingHorizontal: 16,
    gap: 8,
  },
  filterWithBell: {
    flexDirection: 'row',
    alignItems: 'center',
    marginRight: 4,
  },
  filterChip: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 14,
    paddingVertical: 8,
    borderRadius: 20,
    backgroundColor: COLORS.background,
    gap: 6,
  },
  filterChipActive: {
    backgroundColor: COLORS.primary,
  },
  filterChipText: {
    fontSize: 13,
    fontWeight: '500',
    color: COLORS.textLight,
  },
  filterChipTextActive: {
    color: '#FFF',
  },
  bellButton: {
    padding: 6,
    marginLeft: 2,
  },

  newsSection: {
    backgroundColor: COLORS.card,
    padding: 16,
  },
  newsCount: {
    fontSize: 12,
    color: COLORS.textMuted,
    marginTop: -8,
    marginBottom: 12,
  },
  newsCard: {
    paddingVertical: 14,
    borderBottomWidth: 1,
    borderBottomColor: COLORS.border,
  },
  lastCard: {
    borderBottomWidth: 0,
  },
  newsHeader: {
    flexDirection: 'row',
    alignItems: 'center',
    marginBottom: 8,
    gap: 8,
  },
  tickerBadge: {
    backgroundColor: COLORS.primary,
    paddingHorizontal: 8,
    paddingVertical: 3,
    borderRadius: 4,
  },
  tickerText: {
    fontSize: 11,
    fontWeight: '700',
    color: '#FFF',
  },
  newsSector: {
    flex: 1,
    fontSize: 12,
    color: COLORS.textMuted,
  },
  newsbell: {
    padding: 4,
  },
  newsTitle: {
    fontSize: 15,
    fontWeight: '500',
    color: COLORS.text,
    lineHeight: 22,
    marginBottom: 6,
  },
  newsMeta: {
    fontSize: 12,
    color: COLORS.textMuted,
  },
});
