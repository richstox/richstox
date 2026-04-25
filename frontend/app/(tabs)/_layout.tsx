import React from 'react';
import { Tabs, Redirect } from 'expo-router';
import { Ionicons } from '@expo/vector-icons';
import { COLORS, FONTS } from '../_layout';
import { Platform, View, TouchableOpacity, StyleSheet } from 'react-native';
import { useAuth } from '../../contexts/AuthContext';
import BrandedLoading from '../../components/BrandedLoading';
import { useCompactMode } from '../../constants/layout';

const SEARCH_FAB_COLOR = '#1E3A5F';
const SEARCH_FAB_SIZE = 52;

function SearchFABButton({ onPress }: { onPress?: () => void }) {
  return (
    <TouchableOpacity
      onPress={onPress}
      activeOpacity={0.82}
      style={fabStyles.wrapper}
      testID="tab-search-fab"
    >
      <View style={fabStyles.circle}>
        <Ionicons name="search" size={24} color="#FFFFFF" />
      </View>
    </TouchableOpacity>
  );
}

const fabStyles = StyleSheet.create({
  wrapper: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    marginTop: -14,
  },
  circle: {
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
          overflow: 'visible',
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
      {/* Search — prominent center FAB button */}
      <Tabs.Screen
        name="search"
        options={{
          title: '',
          tabBarButton: (props) => (
            <SearchFABButton onPress={props.onPress} />
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
