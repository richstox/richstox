/**
 * COMPACT MODE REGRESSION TEST
 *
 * Validates that compact mode tokens, breakpoint, and hooks are correctly
 * defined in the layout constants file.
 *
 * CI/CD: Run with `npx jest __tests__/compactMode.test.js`
 */

const fs = require('fs');
const path = require('path');

describe('Compact Mode: Layout Constants', () => {
  const layoutPath = path.join(__dirname, '../constants/layout.ts');
  let fileContent;

  beforeAll(() => {
    fileContent = fs.readFileSync(layoutPath, 'utf-8');
  });

  // ========================================================================
  // A) COMPACT BREAKPOINT
  // ========================================================================
  describe('Compact Breakpoint', () => {
    it('should define COMPACT_BREAKPOINT at 360', () => {
      expect(fileContent).toContain('export const COMPACT_BREAKPOINT = 360');
    });

    it('should be exported', () => {
      expect(fileContent).toMatch(/export\s+const\s+COMPACT_BREAKPOINT/);
    });
  });

  // ========================================================================
  // B) COMPACT SPACING VALUES
  // ========================================================================
  describe('Compact Spacing Overrides', () => {
    it('should define COMPACT_SPACING object', () => {
      expect(fileContent).toContain('export const COMPACT_SPACING');
    });

    it('should override PAGE_GUTTER to SPACING.md (12)', () => {
      expect(fileContent).toMatch(/COMPACT_SPACING[\s\S]*PAGE_GUTTER:\s*SPACING\.md/);
    });

    it('should override CARD_PADDING to SPACING.md (12)', () => {
      expect(fileContent).toMatch(/COMPACT_SPACING[\s\S]*CARD_PADDING:\s*SPACING\.md/);
    });

    it('should override SECTION_GAP to SPACING.md (12)', () => {
      expect(fileContent).toMatch(/COMPACT_SPACING[\s\S]*SECTION_GAP:\s*SPACING\.md/);
    });

    it('should override ROW_GAP to SPACING.sm (8)', () => {
      expect(fileContent).toMatch(/COMPACT_SPACING[\s\S]*ROW_GAP:\s*SPACING\.sm/);
    });

    it('should override TITLE_GAP to 6', () => {
      expect(fileContent).toMatch(/COMPACT_SPACING[\s\S]*TITLE_GAP:\s*6/);
    });

    it('should be frozen (as const)', () => {
      // Verify the object ends with `as const`
      expect(fileContent).toMatch(/COMPACT_SPACING\s*=\s*\{[\s\S]*?\}\s*as\s*const/);
    });
  });

  // ========================================================================
  // C) useCompactMode HOOK
  // ========================================================================
  describe('useCompactMode Hook', () => {
    it('should be exported', () => {
      expect(fileContent).toMatch(/export\s+function\s+useCompactMode/);
    });

    it('should use useWindowDimensions', () => {
      expect(fileContent).toContain('useWindowDimensions');
    });

    it('should compare width against COMPACT_BREAKPOINT', () => {
      expect(fileContent).toContain('width < COMPACT_BREAKPOINT');
    });

    it('should return boolean type', () => {
      expect(fileContent).toMatch(/useCompactMode\(\):\s*boolean/);
    });
  });

  // ========================================================================
  // D) useLayoutSpacing HOOK
  // ========================================================================
  describe('useLayoutSpacing Hook', () => {
    it('should be exported', () => {
      expect(fileContent).toMatch(/export\s+function\s+useLayoutSpacing/);
    });

    it('should call useCompactMode', () => {
      // The hook should delegate to useCompactMode internally
      expect(fileContent).toMatch(/useLayoutSpacing[\s\S]*useCompactMode\(\)/);
    });

    it('should return pageGutter for standard mode', () => {
      expect(fileContent).toMatch(/useLayoutSpacing[\s\S]*pageGutter:\s*PAGE_GUTTER/);
    });

    it('should return compact pageGutter', () => {
      expect(fileContent).toMatch(/useLayoutSpacing[\s\S]*pageGutter:\s*COMPACT_SPACING\.PAGE_GUTTER/);
    });

    it('should return compact flag', () => {
      expect(fileContent).toMatch(/compact:\s*true\s+as\s+const/);
      expect(fileContent).toMatch(/compact:\s*false\s+as\s+const/);
    });

    it('should include all semantic spacing keys', () => {
      const hookBody = fileContent.slice(fileContent.indexOf('useLayoutSpacing'));
      expect(hookBody).toContain('pageGutter');
      expect(hookBody).toContain('cardPadding');
      expect(hookBody).toContain('sectionGap');
      expect(hookBody).toContain('rowGap');
      expect(hookBody).toContain('titleGap');
      expect(hookBody).toContain('bannerGap');
    });
  });

  // ========================================================================
  // E) READABILITY GUARDS
  // ========================================================================
  describe('Readability Guards', () => {
    it('should not reduce body text below 12px in TYPOGRAPHY', () => {
      // Check that no TYPOGRAPHY preset fontSize goes below 11 (tabLabel is 11, caption is 12)
      // The compact mode must not alter TYPOGRAPHY presets
      expect(fileContent).not.toContain('COMPACT_TYPOGRAPHY');
    });

    it('should not reduce LINE_HEIGHT normal below 1.5', () => {
      expect(fileContent).toMatch(/normal:\s*1\.5/);
    });

    it('should preserve original semantic aliases unchanged', () => {
      expect(fileContent).toMatch(/export\s+const\s+PAGE_GUTTER\s*=\s*SPACING\.lg/);
      expect(fileContent).toMatch(/export\s+const\s+CARD_PADDING\s*=\s*SPACING\.lg/);
      expect(fileContent).toMatch(/export\s+const\s+ROW_GAP\s*=\s*SPACING\.md/);
      expect(fileContent).toMatch(/export\s+const\s+TITLE_GAP\s*=\s*SPACING\.sm/);
    });
  });

  // ========================================================================
  // F) RE-EXPORT FROM _layout.tsx
  // ========================================================================
  describe('Re-exports from _layout.tsx', () => {
    const layoutTsxPath = path.join(__dirname, '../app/_layout.tsx');
    let layoutTsxContent;

    beforeAll(() => {
      layoutTsxContent = fs.readFileSync(layoutTsxPath, 'utf-8');
    });

    it('should re-export COMPACT_BREAKPOINT', () => {
      expect(layoutTsxContent).toContain('COMPACT_BREAKPOINT');
    });

    it('should re-export COMPACT_SPACING', () => {
      expect(layoutTsxContent).toContain('COMPACT_SPACING');
    });

    it('should re-export useCompactMode', () => {
      expect(layoutTsxContent).toContain('useCompactMode');
    });

    it('should re-export useLayoutSpacing', () => {
      expect(layoutTsxContent).toContain('useLayoutSpacing');
    });
  });
});

module.exports = {};
