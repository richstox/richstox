/**
 * WebRails – desktop side-rail wrapper (web-only).
 *
 * On viewports wider than the app shell (430px), the extra horizontal space
 * becomes left/right "rails".
 *
 * On native (iOS / Android) this component is a no-op pass-through.
 *
 * RULES (from the Global Layout Standard):
 *   - Rails are OUTSIDE the main app shell; they never affect its width.
 *   - Rail content must not add horizontal scroll.
 *   - Rail monetisation is web-only.
 */

import React from 'react';
import { View, StyleSheet, Platform, useWindowDimensions } from 'react-native';
import {
  APP_SHELL_MAX_WIDTH,
  RAIL_BACKGROUND,
  RAIL_BREAKPOINT,
} from '../constants/layout';

interface WebRailsProps {
  children: React.ReactNode;
  /** Retained for compatibility while tier behavior is disabled. */
  subscriptionTier?: 'free' | 'pro' | 'pro_plus';
}

/**
 * Renders the app shell centred with optional side rails on web.
 * On native platforms it just passes children through.
 */
export default function WebRails({ children, subscriptionTier = 'free' }: WebRailsProps) {
  // On native, skip all rail logic.
  if (Platform.OS !== 'web') {
    return <>{children}</>;
  }

  void subscriptionTier;
  return <WebRailsInner>{children}</WebRailsInner>;
}

function WebRailsInner({
  children,
}: WebRailsProps) {
  const { width: windowWidth } = useWindowDimensions();

  // Hydration safety: during SSG / static export, useWindowDimensions()
  // returns width=0 (no browser window), so showRails would be false.
  // On the client the real viewport width is available immediately, which
  // can flip showRails to true and produce a different DOM tree — a
  // structural mismatch that triggers React error #418.
  //
  // Fix: always render the "no rails" layout on the first (server-matching)
  // render, then allow rails after the component has mounted on the client.
  const [mounted, setMounted] = React.useState(false);
  React.useEffect(() => { setMounted(true); }, []);

  const showRails = mounted && windowWidth >= RAIL_BREAKPOINT;

  if (!showRails) {
    // Narrow web — no rails, shell fills viewport.
    return <>{children}</>;
  }

  return (
    <View style={railStyles.wrapper}>
      {/* ── Left rail ── */}
      <View style={railStyles.rail} />

      {/* ── App shell (children rendered by parent) ── */}
      <View style={railStyles.center}>{children}</View>

      {/* ── Right rail ── */}
      <View style={railStyles.rail} />
    </View>
  );
}

const railStyles = StyleSheet.create({
  wrapper: {
    flex: 1,
    flexDirection: 'row',
    backgroundColor: RAIL_BACKGROUND,
    justifyContent: 'center',
    width: '100%',
  },
  rail: {
    flex: 1,
    alignItems: 'stretch',
    justifyContent: 'flex-start',
    // Rails MUST NOT have a fixed width; they take remaining space.
  },
  center: {
    width: '100%',
    maxWidth: APP_SHELL_MAX_WIDTH,
    flexShrink: 0,
  },
});
