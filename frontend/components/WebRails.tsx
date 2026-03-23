/**
 * WebRails – desktop side-rail wrapper (web-only).
 *
 * On viewports wider than the app shell (430px), the extra horizontal space
 * becomes left/right "rails".  Content of the rails depends on the user's
 * subscription tier:
 *
 *   FREE  → may show ads, upgrade promos, or branded surfaces
 *   PRO / PRO+  → clean / branded empty rails (no ads)
 *
 * On native (iOS / Android) this component is a no-op pass-through.
 *
 * RULES (from the Global Layout Standard):
 *   - Rails are OUTSIDE the main app shell; they never affect its width.
 *   - Rail content must not add horizontal scroll.
 *   - Rail monetisation is web-only.
 */

import React from 'react';
import { View, Text, StyleSheet, Platform, useWindowDimensions } from 'react-native';
import {
  APP_SHELL_MAX_WIDTH,
  RAIL_BACKGROUND,
  RAIL_BREAKPOINT,
  SPACING,
} from '../constants/layout';

interface WebRailsProps {
  children: React.ReactNode;
  /** 'free' | 'pro' | 'pro_plus' – drives rail content visibility. */
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

  return <WebRailsInner subscriptionTier={subscriptionTier}>{children}</WebRailsInner>;
}

function WebRailsInner({
  children,
  subscriptionTier,
}: WebRailsProps) {
  const { width: windowWidth } = useWindowDimensions();
  const showRails = windowWidth >= RAIL_BREAKPOINT;
  const isPaid = subscriptionTier === 'pro' || subscriptionTier === 'pro_plus';

  if (!showRails) {
    // Narrow web — no rails, shell fills viewport.
    return <>{children}</>;
  }

  return (
    <View style={railStyles.wrapper}>
      {/* ── Left rail ── */}
      <View style={railStyles.rail}>
        {!isPaid && <RailPromoPlaceholder side="left" />}
      </View>

      {/* ── App shell (children rendered by parent) ── */}
      <View style={railStyles.center}>{children}</View>

      {/* ── Right rail ── */}
      <View style={railStyles.rail}>
        {!isPaid && <RailPromoPlaceholder side="right" />}
      </View>
    </View>
  );
}

/**
 * Placeholder for future ad / promo surfaces.
 * Replace with real ad SDK or promo component later.
 */
function RailPromoPlaceholder({ side }: { side: 'left' | 'right' }) {
  return (
    <View style={railStyles.promoBox}>
      <Text style={railStyles.promoText}>Ad / Promo</Text>
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
    alignItems: 'center',
    justifyContent: 'flex-start',
    paddingTop: 120,
    // Rails MUST NOT have a fixed width; they take remaining space.
  },
  center: {
    width: '100%',
    maxWidth: APP_SHELL_MAX_WIDTH,
    flexShrink: 0,
  },
  promoBox: {
    width: 140,
    padding: SPACING.lg,
    borderRadius: 12,
    backgroundColor: '#FFFFFF',
    alignItems: 'center',
    justifyContent: 'center',
    opacity: 0.6,
  },
  promoText: {
    fontSize: 12,
    color: '#95A5A6',
    fontStyle: 'italic',
  },
});
