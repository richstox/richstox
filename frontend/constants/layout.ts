/**
 * RICHSTOX Global Layout & Spacing Tokens
 * ========================================
 * Single source of truth for the mobile-first app shell.
 *
 * See docs/GLOBAL_LAYOUT_STANDARD.md for the full specification.
 *
 * RULES
 * - App shell is always 100% width, max 430px, centered.
 * - Side rails live *outside* the shell (web only).
 * - All screens reuse these tokens — no per-page overrides for core spacing.
 */

import { Platform, useWindowDimensions } from 'react-native';

// ─── App Shell ────────────────────────────────────────────────────────────────

/** Hard ceiling for the app content column on web. */
export const APP_SHELL_MAX_WIDTH = 430;

/** Background colour shown behind the rails on wide web viewports. */
export const RAIL_BACKGROUND = '#E8E4DF';

// ─── Spacing Scale (4-point) ─────────────────────────────────────────────────

export const SPACING = {
  /** 4px – hairline gaps, icon–text nudges */
  xs: 4,
  /** 8px – tight row gaps, inline elements */
  sm: 8,
  /** 12px – default inner gap, list-item vertical padding */
  md: 12,
  /** 16px – page gutters, card padding, section gaps on mobile */
  lg: 16,
  /** 24px – section spacing, generous card padding */
  xl: 24,
  /** 32px – major section dividers */
  xxl: 32,
} as const;

// ─── Semantic Aliases ────────────────────────────────────────────────────────

/** Horizontal padding inside every page (left + right gutters). */
export const PAGE_GUTTER = SPACING.lg; // 16

/** Vertical gap between top-level content sections. */
export const SECTION_GAP = SPACING.xl; // 24

/** Inner padding of cards. */
export const CARD_PADDING = SPACING.lg; // 16

/** Vertical gap between rows inside a list or card. */
export const ROW_GAP = SPACING.md; // 12

/** Gap between a section title and its first child. */
export const TITLE_GAP = SPACING.sm; // 8

/** Gap between a banner and surrounding content. */
export const BANNER_GAP = SPACING.lg; // 16

// ─── Typography / Text Density ───────────────────────────────────────────────

export const LINE_HEIGHT = {
  /** Tight – single-line metrics, badges (1.2×) */
  tight: 1.2,
  /** Normal – body text, descriptions (1.5×) */
  normal: 1.5,
  /** Relaxed – long-form, helper text (1.6×) */
  relaxed: 1.6,
} as const;

/**
 * Helper: given a fontSize, return a comfortable lineHeight.
 * Use `variant` to pick density.
 */
export function lineHeight(
  fontSize: number,
  variant: keyof typeof LINE_HEIGHT = 'normal',
): number {
  return Math.round(fontSize * LINE_HEIGHT[variant]);
}

// ─── Compact Mode (ultra-narrow viewports) ───────────────────────────────────

/**
 * Compact mode activates below this viewport width.
 * Designed for ultra-narrow devices (320–359 px).
 */
export const COMPACT_BREAKPOINT = 360;

/**
 * Compact spacing overrides — used only when viewport < COMPACT_BREAKPOINT.
 *
 * These are the least-destructive adaptations: reduce spacing first,
 * preserve readability, keep the same app shell concept.
 */
export const COMPACT_SPACING = {
  PAGE_GUTTER: SPACING.md,   // 16 → 12
  CARD_PADDING: SPACING.md,  // 16 → 12
  SECTION_GAP: SPACING.md,   // 24 → 12
  ROW_GAP: SPACING.sm,       //  12 → 8
  TITLE_GAP: 6,              //   8 → 6
} as const;

/**
 * Hook: returns true when the viewport is below the compact breakpoint.
 */
export function useCompactMode(): boolean {
  const { width } = useWindowDimensions();
  return width < COMPACT_BREAKPOINT;
}

/**
 * Hook: returns responsive spacing values.
 * On ultra-narrow viewports (< 360 px) returns compact overrides;
 * otherwise returns the standard values.
 *
 * Usage:
 *   const sp = useLayoutSpacing();
 *   <View style={{ padding: sp.pageGutter }} />
 */
export function useLayoutSpacing() {
  const compact = useCompactMode();
  return compact
    ? {
        pageGutter: COMPACT_SPACING.PAGE_GUTTER,
        cardPadding: COMPACT_SPACING.CARD_PADDING,
        sectionGap: COMPACT_SPACING.SECTION_GAP,
        rowGap: COMPACT_SPACING.ROW_GAP,
        titleGap: COMPACT_SPACING.TITLE_GAP,
        bannerGap: BANNER_GAP,
        compact: true as const,
      }
    : {
        pageGutter: PAGE_GUTTER,
        cardPadding: CARD_PADDING,
        sectionGap: SECTION_GAP,
        rowGap: ROW_GAP,
        titleGap: TITLE_GAP,
        bannerGap: BANNER_GAP,
        compact: false as const,
      };
}

// ─── Breakpoints (web only) ──────────────────────────────────────────────────

/**
 * Rail content becomes visible at this width.
 * Below this, the shell fills the viewport edge-to-edge.
 */
export const RAIL_BREAKPOINT = 600;

// ─── Platform helpers ────────────────────────────────────────────────────────

export const IS_WEB = Platform.OS === 'web';
