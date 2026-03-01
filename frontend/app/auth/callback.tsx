/**
 * Auth Callback Page
 * ==================
 * Handles OAuth redirect from Emergent Auth.
 * Processes session_id from URL fragment and redirects to dashboard.
 * 
 * REMINDER: DO NOT HARDCODE THE URL, OR ADD ANY FALLBACKS OR REDIRECT URLS, THIS BREAKS THE AUTH
 */

import React, { useEffect, useRef, useState } from 'react';
import { View, Text, ActivityIndicator, StyleSheet, Platform } from 'react-native';
import { useRouter } from 'expo-router';
import { useAuth } from '../../contexts/AuthContext';
import { COLORS } from '../_layout';

export default function AuthCallback() {
  const router = useRouter();
  const { processSessionId } = useAuth();
  const [status, setStatus] = useState('Processing...');
  const hasProcessed = useRef(false);

  useEffect(() => {
    // Small delay to ensure layout is mounted
    const timer = setTimeout(() => {
      handleCallback();
    }, 100);
    
    return () => clearTimeout(timer);
  }, []);

  const handleCallback = async () => {
    // Prevent double processing in StrictMode
    if (hasProcessed.current) return;
    hasProcessed.current = true;

    if (Platform.OS !== 'web') {
      // Mobile: redirect to dashboard
      setStatus('Redirecting...');
      router.replace('/(tabs)/dashboard');
      return;
    }

    // Get session_id from URL fragment (after #)
    const hash = window.location.hash;
    const sessionIdMatch = hash.match(/session_id=([^&]+)/);
    
    if (!sessionIdMatch) {
      console.error('No session_id in URL');
      setStatus('No session found');
      setTimeout(() => router.replace('/(tabs)/dashboard'), 1000);
      return;
    }

    const sessionId = sessionIdMatch[1];
    setStatus('Signing in...');
    
    // Exchange session_id for session_token
    const success = await processSessionId(sessionId);
    
    if (success) {
      // Clear the hash and redirect to dashboard
      window.history.replaceState(null, '', window.location.pathname);
      setStatus('Success! Redirecting...');
      setTimeout(() => router.replace('/(tabs)/dashboard'), 500);
    } else {
      setStatus('Sign in failed');
      setTimeout(() => router.replace('/(tabs)/dashboard'), 1000);
    }
  };

  return (
    <View style={styles.container}>
      <ActivityIndicator size="large" color={COLORS.primary} />
      <Text style={styles.text}>{status}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
    backgroundColor: COLORS.background,
  },
  text: {
    marginTop: 16,
    fontSize: 16,
    color: COLORS.textLight,
  },
});
