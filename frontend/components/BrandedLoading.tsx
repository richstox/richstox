import React from 'react';
import { View, Image, Text, StyleSheet, ActivityIndicator } from 'react-native';

const ICON = require('../assets/images/richstox_icon.png');

interface BrandedLoadingProps {
  /** Contextual message shown below the icon, e.g. "Loading your portfolio..." */
  message?: string;
  /** Optional subtitle shown below the message */
  subtitle?: string;
}

export default function BrandedLoading({
  message = 'Loading...',
  subtitle = 'Verify before you invest.',
}: BrandedLoadingProps) {
  return (
    <View style={styles.container}>
      <Image source={ICON} style={styles.icon} />
      <ActivityIndicator size="small" color="#4A6FA5" style={styles.spinner} />
      <Text style={styles.message}>{message}</Text>
      <Text style={styles.subtitle}>{subtitle}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: '#F8F6F3',
    paddingHorizontal: 32,
    paddingVertical: 48,
  },
  icon: {
    width: 64,
    height: 64,
    marginBottom: 16,
  },
  spinner: {
    marginBottom: 12,
  },
  message: {
    fontSize: 16,
    color: '#636E72',
    textAlign: 'center',
    marginBottom: 4,
  },
  subtitle: {
    fontSize: 13,
    color: '#95A5A6',
    textAlign: 'center',
  },
});
