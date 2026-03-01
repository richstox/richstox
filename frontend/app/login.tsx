/**
 * Login Screen
 * =============
 * Displays login options: Google OAuth and Apple Sign-In (pending).
 * 
 * REMINDER: DO NOT HARDCODE THE URL, OR ADD ANY FALLBACKS OR REDIRECT URLS, THIS BREAKS THE AUTH
 */

import React, { useState } from 'react';
import { View, Text, TouchableOpacity, StyleSheet, Image, Platform, ActivityIndicator } from 'react-native';
import { useRouter } from 'expo-router';
import { useAuth } from '../contexts/AuthContext';
import { COLORS } from './_layout';
import { Ionicons } from '@expo/vector-icons';

export default function LoginScreen() {
  const router = useRouter();
  const { login, isLoading, isAuthenticated } = useAuth();
  const [isLoggingIn, setIsLoggingIn] = useState(false);

  // If already authenticated, redirect to dashboard
  React.useEffect(() => {
    if (isAuthenticated) {
      router.replace('/(tabs)/dashboard');
    }
  }, [isAuthenticated]);

  const handleGoogleLogin = async () => {
    setIsLoggingIn(true);
    try {
      await login();
    } catch (error) {
      console.error('Login error:', error);
    } finally {
      setIsLoggingIn(false);
    }
  };

  const handleAppleLogin = () => {
    // Apple Sign-In - pending credentials
    alert('Apple Sign-In bude brzy k dispozici');
  };

  return (
    <View style={styles.container}>
      <View style={styles.header}>
        <Image 
          source={require('../assets/images/richstox_logo.png')}
          style={styles.logo}
          resizeMode="contain"
        />
        <Text style={styles.title}>Vítejte v RICHSTOX</Text>
        <Text style={styles.subtitle}>
          Vaše chytrá investiční analytika
        </Text>
      </View>

      <View style={styles.buttonContainer}>
        {/* Google Sign In */}
        <TouchableOpacity 
          style={styles.googleButton}
          onPress={handleGoogleLogin}
          disabled={isLoading || isLoggingIn}
          testID="google-login-btn"
          accessibilityLabel="Přihlásit se přes Google"
        >
          <View style={styles.buttonContent}>
            {isLoggingIn ? (
              <ActivityIndicator size="small" color="#4285F4" />
            ) : (
              <Ionicons name="logo-google" size={20} color="#4285F4" />
            )}
            <Text style={styles.googleButtonText}>
              {isLoggingIn ? 'Přihlašování...' : 'Pokračovat s Google'}
            </Text>
          </View>
        </TouchableOpacity>

        {/* Apple Sign In - Pending */}
        <TouchableOpacity 
          style={[styles.appleButton, styles.pendingButton]}
          onPress={handleAppleLogin}
          disabled={true}
          testID="apple-login-btn"
          accessibilityLabel="Přihlásit se přes Apple"
        >
          <View style={styles.buttonContent}>
            <Ionicons name="logo-apple" size={20} color="#666" />
            <Text style={[styles.appleButtonText, styles.pendingText]}>
              Pokračovat s Apple
            </Text>
          </View>
          <View style={styles.pendingBadge}>
            <Text style={styles.pendingBadgeText}>Brzy</Text>
          </View>
        </TouchableOpacity>
      </View>

      <View style={styles.footer}>
        <Text style={styles.footerText}>
          Přihlášením souhlasíte s našimi
        </Text>
        <Text style={styles.footerLink}>
          Podmínkami použití a Ochranou soukromí
        </Text>
      </View>

      {/* Skip for now - demo mode */}
      <TouchableOpacity 
        style={styles.skipButton}
        onPress={() => router.replace('/(tabs)/dashboard')}
        data-testid="skip-login-btn"
      >
        <Text style={styles.skipText}>Pokračovat bez přihlášení</Text>
      </TouchableOpacity>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: COLORS.background,
    paddingHorizontal: 24,
    justifyContent: 'center',
  },
  header: {
    alignItems: 'center',
    marginBottom: 48,
  },
  logo: {
    width: 200,
    height: 70,
    marginBottom: 24,
  },
  title: {
    fontSize: 28,
    fontWeight: '600',
    color: COLORS.text,
    marginBottom: 8,
  },
  subtitle: {
    fontSize: 16,
    color: COLORS.textLight,
    textAlign: 'center',
  },
  buttonContainer: {
    gap: 16,
  },
  googleButton: {
    backgroundColor: '#FFFFFF',
    borderRadius: 12,
    paddingVertical: 16,
    paddingHorizontal: 24,
    borderWidth: 1,
    borderColor: COLORS.border,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.05,
    shadowRadius: 4,
    elevation: 2,
  },
  appleButton: {
    backgroundColor: '#000000',
    borderRadius: 12,
    paddingVertical: 16,
    paddingHorizontal: 24,
    position: 'relative',
  },
  pendingButton: {
    backgroundColor: '#E5E5E5',
    opacity: 0.7,
  },
  buttonContent: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 12,
  },
  googleButtonText: {
    fontSize: 16,
    fontWeight: '600',
    color: COLORS.text,
  },
  appleButtonText: {
    fontSize: 16,
    fontWeight: '600',
    color: '#FFFFFF',
  },
  pendingText: {
    color: '#666',
  },
  pendingBadge: {
    position: 'absolute',
    top: -8,
    right: -8,
    backgroundColor: COLORS.warning,
    paddingHorizontal: 8,
    paddingVertical: 2,
    borderRadius: 8,
  },
  pendingBadgeText: {
    fontSize: 10,
    fontWeight: '600',
    color: '#FFFFFF',
  },
  footer: {
    marginTop: 32,
    alignItems: 'center',
  },
  footerText: {
    fontSize: 12,
    color: COLORS.textMuted,
  },
  footerLink: {
    fontSize: 12,
    color: COLORS.primary,
    marginTop: 4,
  },
  skipButton: {
    marginTop: 24,
    alignItems: 'center',
    paddingVertical: 12,
  },
  skipText: {
    fontSize: 14,
    color: COLORS.textMuted,
    textDecorationLine: 'underline',
  },
});
