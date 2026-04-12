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
import { useAppDialog } from '../contexts/AppDialogContext';
import { COLORS, FONTS, TYPOGRAPHY } from './_layout';
import { Ionicons } from '@expo/vector-icons';

export default function LoginScreen() {
  const router = useRouter();
  const { login, isLoading, isAuthenticated, devLogin } = useAuth();
  const dialog = useAppDialog();
  const [isLoggingIn, setIsLoggingIn] = useState(false);
  const [isDevLoggingIn, setIsDevLoggingIn] = useState(false);

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
    dialog.alert('Coming Soon', 'Apple Sign-In coming soon');
  };

  const handleDevLogin = async () => {
    setIsDevLoggingIn(true);
    try {
      await devLogin();
      router.replace('/(tabs)/dashboard');
    } catch (error) {
      console.error('Dev login error:', error);
    } finally {
      setIsDevLoggingIn(false);
    }
  };

  return (
    <View style={styles.container}>
      <View style={styles.header}>
        <Image 
          source={require('../assets/images/richstox_icon_only.png')}
          style={styles.logo}
          resizeMode="contain"
        />
        <Text style={styles.title}>Welcome to RICHSTOX</Text>
        <Text style={styles.subtitle}>
          Verify before you invest.
        </Text>
      </View>

      <View style={styles.buttonContainer}>
        {/* Google Sign In */}
        <TouchableOpacity 
          style={styles.googleButton}
          onPress={handleGoogleLogin}
          disabled={isLoading || isLoggingIn}
          testID="google-login-btn"
          accessibilityLabel="Sign in with Google"
        >
          <View style={styles.buttonContent}>
            {isLoggingIn ? (
              <ActivityIndicator size="small" color="#4285F4" />
            ) : (
              <Ionicons name="logo-google" size={20} color="#4285F4" />
            )}
            <Text style={styles.googleButtonText}>
              {isLoggingIn ? 'Signing in...' : 'Continue with Google'}
            </Text>
          </View>
        </TouchableOpacity>

        {/* Apple Sign In - Pending */}
        <TouchableOpacity 
          style={[styles.appleButton, styles.pendingButton]}
          onPress={handleAppleLogin}
          disabled={true}
          testID="apple-login-btn"
          accessibilityLabel="Sign in with Apple"
        >
          <View style={styles.buttonContent}>
            <Ionicons name="logo-apple" size={20} color="#666" />
            <Text style={[styles.appleButtonText, styles.pendingText]}>
              Continue with Apple
            </Text>
          </View>
          <View style={styles.pendingBadge}>
            <Text style={styles.pendingBadgeText}>Soon</Text>
          </View>
        </TouchableOpacity>
      </View>

      <View style={styles.footer}>
        <Text style={styles.footerText}>
          By signing in, you agree to our
        </Text>
        <Text style={styles.footerLink}>
          Terms of Service and Privacy Policy
        </Text>
      </View>

      {/* Dev Login - only visible in development */}
      {__DEV__ && (
        <TouchableOpacity 
          style={styles.devLoginButton}
          onPress={handleDevLogin}
          disabled={isDevLoggingIn}
          data-testid="dev-login-btn"
        >
          <View style={styles.buttonContent}>
            <Ionicons name="code-slash" size={18} color={COLORS.accent} />
            <Text style={styles.devLoginText}>
              {isDevLoggingIn ? 'Signing in...' : 'Dev Login (Admin)'}
            </Text>
          </View>
        </TouchableOpacity>
      )}
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
    width: 100,
    height: 100,
    marginBottom: 16,
  },
  title: {
    ...TYPOGRAPHY.h1,
    color: COLORS.text,
    marginBottom: 8,
    textAlign: 'center',
  },
  subtitle: {
    ...TYPOGRAPHY.subtitle,
    textAlign: 'center',
    fontStyle: 'italic',
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
    ...TYPOGRAPHY.button,
    color: COLORS.text,
  },
  appleButtonText: {
    ...TYPOGRAPHY.button,
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
  devLoginButton: {
    marginTop: 24,
    alignItems: 'center',
    paddingVertical: 12,
    paddingHorizontal: 24,
    borderWidth: 1,
    borderColor: COLORS.accent,
    borderRadius: 12,
    borderStyle: 'dashed',
  },
  devLoginText: {
    fontSize: 14,
    fontWeight: '500',
    color: COLORS.accent,
  },
});
