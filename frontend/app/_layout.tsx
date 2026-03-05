import React from 'react';
import { Stack } from 'expo-router';
import { StatusBar } from 'expo-status-bar';
import { View, StyleSheet, Platform, ActivityIndicator } from 'react-native';
import { SafeAreaProvider } from 'react-native-safe-area-context';
import { AuthProvider } from '../contexts/AuthContext';
import { useFonts, DMSerifDisplay_400Regular } from '@expo-google-fonts/dm-serif-display';
import { Inter_400Regular, Inter_500Medium, Inter_600SemiBold, Inter_700Bold } from '@expo-google-fonts/inter';

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

export const FONTS = {
  heading: 'DMSerifDisplay_400Regular',
  body: 'Inter_400Regular',
  bodyMedium: 'Inter_500Medium',
  bodySemiBold: 'Inter_600SemiBold',
  bodyBold: 'Inter_700Bold',
};

export default function RootLayout() {
  const [fontsLoaded] = useFonts({
    DMSerifDisplay_400Regular,
    Inter_400Regular,
    Inter_500Medium,
    Inter_600SemiBold,
    Inter_700Bold,
  });

  if (!fontsLoaded) {
    return (
      <View style={{ flex: 1, justifyContent: 'center', alignItems: 'center', backgroundColor: COLORS.background }}>
        <ActivityIndicator size="large" color={COLORS.primary} />
      </View>
    );
  }

  return (
    <AuthProvider>
      <SafeAreaProvider>
        <View style={styles.outerContainer}>
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
        </View>
      </SafeAreaProvider>
    </AuthProvider>
  );
}

const WEB_MAX_WIDTH = 480;

const styles = StyleSheet.create({
  outerContainer: {
    flex: 1,
    backgroundColor: '#E8E4DF',
    alignItems: 'center',
  },
  container: {
    flex: 1,
    backgroundColor: COLORS.background,
    width: '100%',
    ...(Platform.OS === 'web' ? { maxWidth: WEB_MAX_WIDTH } : {}),
  },
});
