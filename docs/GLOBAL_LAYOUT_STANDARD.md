# RICHSTOX Global Layout & Readability Standard

> Single internal reference for every current and future screen.
> **Do not deviate without explicit approval.**

---

## 1. App Shell Rules

| Property | Value | Notes |
|----------|-------|-------|
| `width` | `100%` | Always fills available space up to the cap |
| `max-width` | **430 px** | Enforced at the root layout level on web |
| Centering | `alignItems: 'center'` on outer wrapper | Shell is always horizontally centred |
| Native | Full-screen, no max-width cap | iOS / Android fill the device naturally |

**The app must never expand into a wide desktop content layout.**  
On wider web viewports the extra horizontal space is used as side rails (see §6).

Source: `frontend/app/_layout.tsx` → `styles.container.maxWidth`  
Constant: `frontend/constants/layout.ts` → `APP_SHELL_MAX_WIDTH`

---

## 2. Width / Max-Width Behaviour

```
┌──────────────────────────────────────────────────────────────┐
│  browser viewport (any width)                                │
│                                                              │
│   ┌──────┐  ┌──────────────────────┐  ┌──────┐              │
│   │ rail │  │  app shell (≤430px)  │  │ rail │              │
│   │ left │  │                      │  │right │              │
│   └──────┘  └──────────────────────┘  └──────┘              │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

- Below **430 px** → shell fills 100 % of viewport, no rails visible.
- **430–599 px** → shell is 430 px centred, minimal background margins.
- **≥ 600 px** → rails become visible alongside the centred shell.
- The shell must remain fully usable down to **320 px** without horizontal scrolling.

---

## 3. Spacing Tokens

All values live in `frontend/constants/layout.ts` and are re-exported from `frontend/app/_layout.tsx`.

| Token | px | Use |
|-------|---:|-----|
| `SPACING.xs` | 4 | Hairline gaps, icon–text nudges |
| `SPACING.sm` | 8 | Tight row gaps, inline elements |
| `SPACING.md` | 12 | Default inner gap, list-item vertical padding |
| `SPACING.lg` | 16 | Page gutters, card padding, section gaps on mobile |
| `SPACING.xl` | 24 | Section spacing, generous card padding |
| `SPACING.xxl` | 32 | Major section dividers |

### Semantic aliases

| Alias | Value | Meaning |
|-------|------:|---------|
| `PAGE_GUTTER` | 16 | Horizontal padding for every page |
| `SECTION_GAP` | 24 | Vertical gap between top-level sections |
| `CARD_PADDING` | 16 | Inner padding of every card |
| `ROW_GAP` | 12 | Vertical gap between list rows / card rows |
| `TITLE_GAP` | 8 | Space below a section title before its content |
| `BANNER_GAP` | 16 | Space around info/status banners |

### Compact mode (ultra-narrow viewports)

Viewports **below 360 px** (`COMPACT_BREAKPOINT`) activate compact spacing overrides.
This prevents layout breakage and overcrowding on devices like 320 px screens
while preserving the app's minimalist, premium feel.

| Standard Token | Standard | Compact (< 360 px) |
|----------------|--------:|-----------:|
| `PAGE_GUTTER`  | 16      | 12         |
| `CARD_PADDING` | 16      | 12         |
| `SECTION_GAP`  | 24      | 12         |
| `ROW_GAP`      | 12      |  8         |
| `TITLE_GAP`    |  8      |  6         |
| `BANNER_GAP`   | 16      | 16 (unchanged) |

**Hooks:**

| Hook | Returns |
|------|---------|
| `useCompactMode()` | `boolean` — true when viewport < 360 px |
| `useLayoutSpacing()` | Object with `pageGutter`, `cardPadding`, `sectionGap`, `rowGap`, `titleGap`, `bannerGap`, `compact` — auto-switches between standard and compact values |

**Rules:**

1. Body text must remain ≥ 12 px; line-height must remain ≥ 1.5×.
2. Primary navigation, critical actions, and primary content must stay visible.
3. No horizontal scrolling on the main app shell.
4. Adaptation priority: reduce spacing → reduce decorative space → tighten secondary UI → shorten labels → move non-primary actions.
5. No separate alternative layout system — use the same app shell concept.

**Validation widths:** 320 px, 340 px, and standard 390 px+ (no regression).

Source: `frontend/constants/layout.ts` → `COMPACT_BREAKPOINT`, `COMPACT_SPACING`, `useCompactMode`, `useLayoutSpacing`

---

## 4. Section / Card / List / Banner Spacing Rules

### Page structure

Every screen should follow this vertical structure (items marked _optional_ may be omitted):

1. **Top bar** (AppHeader) — fixed height, full shell width
2. _Optional_ context / navigation row
3. _Optional_ status / info banner
4. **Main content sections** — separated by `SECTION_GAP` (24 px)
5. **Bottom navigation** (tab bar) — fixed height, full shell width

### Card rules

- Border radius: **16 px** (existing convention)
- Inner padding: `CARD_PADDING` (16 px)
- Inter-card gap: `SECTION_GAP` (24 px) when stacked vertically
- Row gap inside a card: `ROW_GAP` (12 px)

### List item rules

- Vertical padding per item: `SPACING.md` (12 px) top + bottom
- Horizontal padding: `PAGE_GUTTER` (16 px)
- Divider: 1 px `COLORS.border` between items

### Banner rules

- Margin above / below: `BANNER_GAP` (16 px)
- Inner padding: `CARD_PADDING` (16 px)
- Border radius: 12 px (slightly tighter than cards)

---

## 5. Text Density & Line-Height Rules

### Line-height multipliers

| Variant | Multiplier | Use |
|---------|------------|-----|
| `tight` | **1.2×** | Single-line metrics, badges, numbers |
| `normal` | **1.5×** | Body text, descriptions, labels |
| `relaxed` | **1.6×** | Long-form helper text, disclaimers |

Use the `lineHeight(fontSize, variant)` helper or the pre-computed values in `TYPOGRAPHY`.

### Typography presets (with line-heights)

| Preset | Font | Size | Line-height |
|--------|------|-----:|------------:|
| `h1` | DM Serif Display | 48 | 58 |
| `h2` | DM Serif Display | 28 | 34 |
| `h3` | DM Serif Display | 22 | 28 |
| `subtitle` | Inter 400 | 18 | 27 |
| `body` | Inter 400 | 14 | 21 |
| `bodyLarge` | Inter 400 | 16 | 24 |
| `label` | Inter 500 | 13 | 18 |
| `button` | Inter 500 | 16 | 24 |
| `caption` | Inter 400 | 12 | 18 |
| `tabLabel` | Inter 500 | 11 | 14 |
| `metric` | Inter 600 | 20 | 24 |
| `metricSmall` | Inter 600 | 14 | 17 |
| `sectionTitle` | Inter 700 | 14 | 18 |

### Text must never feel cramped

- Primary text: `body` or `bodyLarge` preset
- Secondary text: `label` preset
- Metadata / helper text: `caption` preset
- Numbers / status values: `metric` or `metricSmall` preset

---

## 6. Rail Behaviour

Rails are rendered by `frontend/components/WebRails.tsx`.

| Rule | Detail |
|------|--------|
| Rails are **web-only** | Native apps ignore them entirely |
| Rails live **outside** the app shell | They never affect shell width, navigation, or content spacing |
| Show threshold | Viewport ≥ 600 px (`RAIL_BREAKPOINT`) |
| FREE users | May see ads, upgrade promos, or branded surfaces in the rails |
| PRO / PRO+ users | Clean / branded empty rails — **no ads** |
| Ads inside the shell | **Forbidden** — ads must never appear inside the main app container |
| Rail content must not | Add horizontal scroll, shift bottom nav, or change content width |

---

## 7. Page Structure Rules

### Top bar

- Use `AppHeader` component (see `frontend/components/AppHeader.tsx`)
- Consistent height and padding across all screens
- Title, back button, search, notifications, avatar menu

### Bottom navigation

- Tab bar managed by `frontend/app/(tabs)/_layout.tsx`
- Fixed height: 84 px (iOS) / 64 px (Android/web)
- Never shifts due to rail content

### Per-page consistency

- All pages use `SafeAreaView` with `flex: 1` and `backgroundColor: COLORS.background`
- Content area uses `PAGE_GUTTER` horizontal padding
- Sections separated by `SECTION_GAP`
- No per-page spacing drift

---

## 8. Allowed Patterns

✅ Import spacing tokens from `constants/layout` or `app/_layout`  
✅ Use `TYPOGRAPHY` presets for text styles  
✅ Use `SPACING` tokens for all padding/margin/gap values  
✅ Use `AppHeader` for the top bar  
✅ Use `SafeAreaView` with appropriate edges  
✅ Use `COLORS` from `app/_layout` for all colours  
✅ Cards with `CARD_PADDING` inner padding and 16 px border radius  

---

## 9. Forbidden Patterns

🚫 Hard-coded pixel values for padding/margin (use tokens)  
🚫 Per-page max-width overrides  
🚫 Desktop-wide layouts (content stretched across full viewport)  
🚫 Ads/promos inside the main app shell container  
🚫 Rail content that affects app shell width or navigation  
🚫 Horizontal scrolling on any screen  
🚫 `lineHeight` below 1.2× on readable text  
🚫 Page-level spacing that doesn't use the shared tokens  
🚫 Custom top bars that don't follow AppHeader conventions  

---

## 10. File Reference

| File | Purpose |
|------|---------|
| `frontend/constants/layout.ts` | All spacing tokens, app shell constants, breakpoints, compact mode |
| `frontend/app/_layout.tsx` | Root layout — shell container, COLORS, FONTS, TYPOGRAPHY, re-exports tokens |
| `frontend/components/WebRails.tsx` | Desktop side-rail wrapper (web-only, subscription-aware) |
| `frontend/app/+html.tsx` | HTML template — viewport meta, overflow-x prevention, rail background |
| `frontend/app/(tabs)/_layout.tsx` | Tab bar configuration |
| `frontend/components/AppHeader.tsx` | Global header component |
| `frontend/__tests__/compactMode.test.js` | Compact mode regression tests (26 tests) |
| `docs/GLOBAL_LAYOUT_STANDARD.md` | This document |

---

## 11. Migration Notes

### Phase 1 (this PR)

- Global app shell enforced at 430 px max-width (was 480 px)
- Shared spacing tokens created and exported
- WebRails component created (subscription-aware, web-only)
- Typography presets updated with explicit line-heights
- HTML template hardened against horizontal overflow
- This documentation created

### Phase 1 follow-up: Compact mode

- Compact breakpoint added at 360 px (`COMPACT_BREAKPOINT`)
- Compact spacing overrides defined (`COMPACT_SPACING`)
- `useCompactMode()` and `useLayoutSpacing()` hooks added
- Re-exported from `_layout.tsx` alongside existing tokens
- Documentation updated with compact mode rules (§3)
- Regression tests added (`__tests__/compactMode.test.js`, 26 tests)

### Phase 2 (future)

- Roll spacing tokens across all remaining screens
- Replace hard-coded padding/margin values with token references
- Audit every card, list, and banner for consistency
- **Do not start Phase 2 until Phase 1 is approved**
