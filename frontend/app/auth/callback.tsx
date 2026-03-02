/**
 * Auth Callback Page
 * ==================
 * Handles OAuth redirect from Google.
 */

import React, { useEffect, useRef, useState } from 'react';
import { View, Text, ActivityIndicator, StyleSheet } from 'react-native';
import { useRouter } from 'expo-router';
import { useAuth } from '../../contexts/AuthContext';
import { COLORS } from '../_layout';

export default function AuthCallback() {
  const router = useRouter();
  const { processSessionId } = useAuth();
  const [status, setStatus] = useState('Přihlašování...');
  const hasProcessed = useRef(false);

  useEffect(() => {
    const timer = setTimeout(() => {
      handleCallback();
    }, 100);
    return () => clearTimeout(timer);
  }, []);

  const handleCallback = async () => {
    if (hasProcessed.current) return;
    hasProcessed.current = true;

    // Get session_id from query params (?session_id=...)
    const params = new URLSearchParams(window.location.search);
    let sessionId = params.get('session_id');
    
    // Also check hash fragment (#session_id=...)
    if (!sessionId) {
      const hash = window.location.hash;
      const match = hash.match(/session_id=([^&]+)/);
      sessionId = match ? match[1] : null;
    }
    
    if (!sessionId) {
      setStatus('Chyba přihlášení');
      setTimeout(() => router.replace('/login'), 1500);
      return;
    }

    setStatus('Přihlašování...');
    const success = await processSessionId(sessionId);
    
    if (success) {
      setStatus('Úspěch!');
      window.history.replaceState(null, '', window.location.pathname);
      setTimeout(() => router.replace('/(tabs)/dashboard'), 500);
    } else {
      setStatus('Přihlášení selhalo');
      setTimeout(() => router.replace('/login'), 1500);
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
