/**
 * Persistent Bottom Navigation Component
 * Used on screens outside the main (tabs) layout
 */

import React from 'react';
import { View, TouchableOpacity, Text, StyleSheet, Platform } from 'react-native';
import { useRouter, usePathname } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';

const COLORS = {
  primary: '#2563EB',
  surface: '#FFFFFF',
  textMuted: '#94A3B8',
  border: '#E2E8F0',
};

const SEARCH_FAB_COLOR = '#1E3A5F';
const SEARCH_FAB_SIZE = 52;

const leftTabs = [
  { name: 'Home', route: '/(tabs)/dashboard', icon: 'home-outline' as const },
  { name: 'Markets', route: '/(tabs)/markets', icon: 'newspaper-outline' as const },
];

const rightTabs = [
  { name: 'Leagues', route: '/(tabs)/leagues', icon: 'trophy-outline' as const },
  { name: 'Talk', route: '/(tabs)/talk', icon: 'chatbubbles-outline' as const },
];

interface BottomNavProps {
  activeTab?: string;
}

export const BottomNav: React.FC<BottomNavProps> = ({ activeTab }) => {
  const router = useRouter();
  const pathname = usePathname();

  const renderTab = (tab: typeof leftTabs[0]) => {
    const isActive = activeTab === tab.name || pathname.includes(tab.route);
    return (
      <TouchableOpacity
        key={tab.name}
        style={styles.tab}
        onPress={() => router.push(tab.route as any)}
        data-testid={`bottom-nav-${tab.name.toLowerCase()}`}
      >
        <Ionicons
          name={tab.icon}
          size={24}
          color={isActive ? COLORS.primary : COLORS.textMuted}
        />
        <Text style={[styles.tabLabel, isActive && styles.tabLabelActive]}>
          {tab.name}
        </Text>
      </TouchableOpacity>
    );
  };

  return (
    <View style={styles.container}>
      {leftTabs.map(renderTab)}

      {/* Search FAB — prominent center button, matches the (tabs) layout */}
      <TouchableOpacity
        style={styles.fabWrapper}
        onPress={() => router.push('/(tabs)/search' as any)}
        activeOpacity={0.82}
        data-testid="bottom-nav-search"
      >
        <View style={styles.fabCircle}>
          <Ionicons name="search" size={24} color="#FFFFFF" />
        </View>
      </TouchableOpacity>

      {rightTabs.map(renderTab)}
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    flexDirection: 'row',
    backgroundColor: COLORS.surface,
    borderTopWidth: 1,
    borderTopColor: COLORS.border,
    paddingTop: 8,
    paddingBottom: Platform.OS === 'ios' ? 24 : 12,
    height: Platform.OS === 'ios' ? 84 : 64,
    overflow: 'visible',
  },
  tab: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    gap: 4,
  },
  tabLabel: {
    fontSize: 11,
    fontWeight: '500',
    color: COLORS.textMuted,
  },
  tabLabelActive: {
    color: COLORS.primary,
  },
  fabWrapper: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    marginTop: -14,
  },
  fabCircle: {
    width: SEARCH_FAB_SIZE,
    height: SEARCH_FAB_SIZE,
    borderRadius: SEARCH_FAB_SIZE / 2,
    backgroundColor: SEARCH_FAB_COLOR,
    alignItems: 'center',
    justifyContent: 'center',
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 4 },
    shadowOpacity: 0.28,
    shadowRadius: 8,
    elevation: 8,
  },
});

export default BottomNav;
