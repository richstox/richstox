import React from 'react';
import { Tabs, Redirect } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import { COLORS, FONTS } from '../_layout';
import { Platform } from 'react-native';
import { useAuth } from '../../contexts/AuthContext';
import BrandedLoading from '../../components/BrandedLoading';
import { useCompactMode } from '../../constants/layout';

export default function TabsLayout() {
  const { isAuthenticated, isLoading } = useAuth();
  const compact = useCompactMode();

  if (isLoading) {
    return <BrandedLoading message="Checking your account..." />;
  }

  if (!isAuthenticated) {
    return <Redirect href="/login" />;
  }

  return (
    <Tabs
      screenOptions={{
        tabBarActiveTintColor: COLORS.primary,
        tabBarInactiveTintColor: COLORS.textMuted,
        tabBarStyle: {
          backgroundColor: COLORS.surface,
          borderTopWidth: 1,
          borderTopColor: COLORS.border,
          paddingTop: compact ? 6 : 8,
          paddingBottom: Platform.OS === 'ios' ? (compact ? 20 : 24) : (compact ? 8 : 12),
          height: Platform.OS === 'ios' ? (compact ? 76 : 84) : (compact ? 56 : 64),
        },
        tabBarLabelStyle: {
          fontSize: 11,
          fontFamily: FONTS.bodyMedium,
        },
        headerShown: false,
      }}
    >
      <Tabs.Screen
        name="dashboard"
        options={{
          title: 'Home',
          tabBarIcon: ({ color, size }) => (
            <Ionicons name="home-outline" size={size} color={color} />
          ),
        }}
      />
      <Tabs.Screen
        name="markets"
        options={{
          title: 'Markets',
          tabBarIcon: ({ color, size }) => (
            <Ionicons name="newspaper-outline" size={size} color={color} />
          ),
        }}
      />
      <Tabs.Screen
        name="leagues"
        options={{
          title: 'Leagues',
          tabBarIcon: ({ color, size }) => (
            <Ionicons name="trophy-outline" size={size} color={color} />
          ),
        }}
      />
      <Tabs.Screen
        name="talk"
        options={{
          title: 'Talk',
          tabBarIcon: ({ color, size }) => (
            <Ionicons name="chatbubbles-outline" size={size} color={color} />
          ),
        }}
      />
      {/* Hidden tabs - accessible via navigation, not shown in tab bar */}
      <Tabs.Screen
        name="search"
        options={{
          href: null,
        }}
      />
      <Tabs.Screen
        name="settings"
        options={{
          href: null,
        }}
      />
      <Tabs.Screen
        name="portfolio"
        options={{
          href: null,
        }}
      />
      <Tabs.Screen
        name="tracklist"
        options={{
          href: null,
        }}
      />
      <Tabs.Screen
        name="admin"
        options={{
          href: null,
        }}
      />
    </Tabs>
  );
}
