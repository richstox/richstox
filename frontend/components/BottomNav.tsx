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

const tabs = [
  { name: 'Home', route: '/(tabs)/dashboard', icon: 'home-outline' as const },
  { name: 'Markets', route: '/(tabs)/markets', icon: 'newspaper-outline' as const },
  { name: 'Leagues', route: '/(tabs)/leagues', icon: 'trophy-outline' as const },
  { name: 'Talk', route: '/(tabs)/talk', icon: 'chatbubbles-outline' as const },
];

interface BottomNavProps {
  activeTab?: string;
}

export const BottomNav: React.FC<BottomNavProps> = ({ activeTab }) => {
  const router = useRouter();
  const pathname = usePathname();
  
  return (
    <View style={styles.container}>
      {tabs.map((tab) => {
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
      })}
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
});

export default BottomNav;
