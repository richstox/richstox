import React from 'react';
import { Stack } from 'expo-router';
import { StatusBar } from 'expo-status-bar';
import { View, StyleSheet, Platform } from 'react-native';
import { SafeAreaProvider } from 'react-native-safe-area-context';
import { AuthProvider, useAuth } from '../contexts/AuthContext';
import { useFonts, DMSerifDisplay_400Regular } from '@expo-google-fonts/dm-serif-display';
import { Inter_400Regular, Inter_500Medium, Inter_600SemiBold, Inter_700Bold } from '@expo-google-fonts/inter';
import { FontDisplay } from 'expo-font';
import BrandedLoading from '../components/BrandedLoading';
import WebRails from '../components/WebRails';
import { APP_SHELL_MAX_WIDTH, RAIL_BACKGROUND, IS_WEB } from '../constants/layout';

// Re-export layout tokens so screens can import from '_layout' or 'constants/layout'
export { SPACING, PAGE_GUTTER, SECTION_GAP, CARD_PADDING, ROW_GAP, TITLE_GAP, BANNER_GAP, LINE_HEIGHT, lineHeight, COMPACT_BREAKPOINT, COMPACT_SPACING, useCompactMode, useLayoutSpacing } from '../constants/layout';

// Calm color palette
export const COLORS = {
  background: '#F8F6F3',
  surface: '#FFFFFF',
  primary: '#4A6FA5',
  secondary: '#7B8FA1',
  text: '#2D3436',
  textLight: '#636E72',
  textMuted: '#95A5A6',
  accent: '#5C8A97',
  positive: '#5C8A97', // Muted teal instead of green
  negative: '#A67B5B', // Muted amber instead of red
  warning: '#C9A857',
  border: '#E8E4DF',
  card: '#FFFFFF',
  danger: '#EF4444',
};

// On web, include system font fallback stacks so text renders immediately while
// custom fonts load (works with font-display: swap). On native, single names
// are required and fonts are guaranteed loaded before use.
export const FONTS = Platform.OS === 'web'
  ? {
      heading: '"DMSerifDisplay_400Regular", Georgia, "Times New Roman", serif',
      body: '"Inter_400Regular", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif',
      bodyMedium: '"Inter_500Medium", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif',
      bodySemiBold: '"Inter_600SemiBold", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif',
      bodyBold: '"Inter_700Bold", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif',
    }
  : {
      heading: 'DMSerifDisplay_400Regular',
      body: 'Inter_400Regular',
      bodyMedium: 'Inter_500Medium',
      bodySemiBold: 'Inter_600SemiBold',
      bodyBold: 'Inter_700Bold',
    };

export const TYPOGRAPHY = {
  h1: { fontFamily: FONTS.heading, fontSize: 48, lineHeight: 58, letterSpacing: 0 },
  h2: { fontFamily: FONTS.heading, fontSize: 28, lineHeight: 34, letterSpacing: 0 },
  h3: { fontFamily: FONTS.heading, fontSize: 22, lineHeight: 28, letterSpacing: 0 },
  subtitle: { fontFamily: FONTS.body, fontSize: 18, lineHeight: 27, color: '#444444' },
  body: { fontFamily: FONTS.body, fontSize: 14, lineHeight: 21 },
  bodyLarge: { fontFamily: FONTS.body, fontSize: 16, lineHeight: 24 },
  label: { fontFamily: FONTS.bodyMedium, fontSize: 13, lineHeight: 18 },
  button: { fontFamily: FONTS.bodyMedium, fontSize: 16, lineHeight: 24 },
  caption: { fontFamily: FONTS.body, fontSize: 12, lineHeight: 18 },
  tabLabel: { fontFamily: FONTS.bodyMedium, fontSize: 11, lineHeight: 14 },
  metric: { fontFamily: FONTS.bodySemiBold, fontSize: 20, lineHeight: 24 },
  metricSmall: { fontFamily: FONTS.bodySemiBold, fontSize: 14, lineHeight: 17 },
  sectionTitle: { fontFamily: FONTS.bodyBold, fontSize: 14, lineHeight: 18, letterSpacing: 0.5, textTransform: 'uppercase' as const },
};

export default function RootLayout() {
  const [fontsLoaded] = useFonts({
    DMSerifDisplay_400Regular: { uri: DMSerifDisplay_400Regular, display: FontDisplay.SWAP },
    Inter_400Regular: { uri: Inter_400Regular, display: FontDisplay.SWAP },
    Inter_500Medium: { uri: Inter_500Medium, display: FontDisplay.SWAP },
    Inter_600SemiBold: { uri: Inter_600SemiBold, display: FontDisplay.SWAP },
    Inter_700Bold: { uri: Inter_700Bold, display: FontDisplay.SWAP },
  });

  // On web, font-display:swap renders text immediately with system fallbacks
  // while custom fonts load in the background — no blocking needed.
  // On native, fonts must finish loading before use (or the app crashes).
  if (!fontsLoaded && Platform.OS !== 'web') {
    return <BrandedLoading message="Starting RICHSTOX..." />;
  }

  return (
    <AuthProvider>
      <SafeAreaProvider>
        <RootShell />
      </SafeAreaProvider>
    </AuthProvider>
  );
}

/**
 * Inner shell — must be inside AuthProvider so it can read subscription tier.
 */
function RootShell() {
  const { user } = useAuth();
  const tier = user?.subscription_tier ?? 'free';

  return (
    <View style={styles.outerContainer}>
      <WebRails subscriptionTier={tier}>
        <View style={styles.container}>
          <StatusBar style="dark" />
          <Stack
            screenOptions={{
              headerShown: false,
              contentStyle: { backgroundColor: COLORS.background },
              animation: 'fade',
            }}
          >
            <Stack.Screen name="index" />
            <Stack.Screen name="login" />
            <Stack.Screen name="auth/callback" />
            <Stack.Screen name="onboarding" />
            <Stack.Screen name="(tabs)" />
            <Stack.Screen
              name="position/[id]"
              options={{
                headerShown: true,
                headerTitle: 'Position Details',
                headerStyle: { backgroundColor: COLORS.background },
                headerTintColor: COLORS.text,
                headerShadowVisible: false,
              }}
            />
            <Stack.Screen
              name="ticker-not-found"
              options={{
                presentation: 'modal',
              }}
            />
          </Stack>
        </View>
      </WebRails>
    </View>
  );
}

const styles = StyleSheet.create({
  outerContainer: {
    flex: 1,
    backgroundColor: RAIL_BACKGROUND,
    alignItems: 'center',
  },
  container: {
    flex: 1,
    backgroundColor: COLORS.background,
    width: '100%',
    ...(IS_WEB ? { maxWidth: APP_SHELL_MAX_WIDTH } : {}),
  },
});
