/**
 * FONT PERFORMANCE & LCP REGRESSION TEST
 *
 * Validates that font loading uses font-display: swap and does not block
 * web rendering, ensuring fast LCP across all pages.
 *
 * CI/CD: Run with `npx jest __tests__/fontPerformance.test.js`
 */

const fs = require('fs');
const path = require('path');

describe('Font Performance & LCP', () => {
  const layoutPath = path.join(__dirname, '../app/_layout.tsx');
  let fileContent;

  beforeAll(() => {
    fileContent = fs.readFileSync(layoutPath, 'utf-8');
  });

  // ========================================================================
  // A) font-display: swap
  // ========================================================================
  describe('font-display: swap', () => {
    it('should import FontDisplay from expo-font', () => {
      expect(fileContent).toMatch(/import\s+\{[^}]*FontDisplay[^}]*\}\s+from\s+['"]expo-font['"]/);
    });

    it('should use FontDisplay.SWAP for DMSerifDisplay_400Regular', () => {
      expect(fileContent).toContain('DMSerifDisplay_400Regular: { uri: DMSerifDisplay_400Regular, display: FontDisplay.SWAP }');
    });

    it('should use FontDisplay.SWAP for Inter_400Regular', () => {
      expect(fileContent).toContain('Inter_400Regular: { uri: Inter_400Regular, display: FontDisplay.SWAP }');
    });

    it('should use FontDisplay.SWAP for Inter_500Medium', () => {
      expect(fileContent).toContain('Inter_500Medium: { uri: Inter_500Medium, display: FontDisplay.SWAP }');
    });

    it('should use FontDisplay.SWAP for Inter_600SemiBold', () => {
      expect(fileContent).toContain('Inter_600SemiBold: { uri: Inter_600SemiBold, display: FontDisplay.SWAP }');
    });

    it('should use FontDisplay.SWAP for Inter_700Bold', () => {
      expect(fileContent).toContain('Inter_700Bold: { uri: Inter_700Bold, display: FontDisplay.SWAP }');
    });

    it('should NOT use FontDisplay.AUTO or FontDisplay.BLOCK', () => {
      expect(fileContent).not.toContain('FontDisplay.AUTO');
      expect(fileContent).not.toContain('FontDisplay.BLOCK');
    });
  });

  // ========================================================================
  // B) Non-blocking font load on web
  // ========================================================================
  describe('Non-blocking web render', () => {
    it('should only block rendering on non-web platforms', () => {
      // The font gate should check Platform.OS !== 'web'
      expect(fileContent).toMatch(/!fontsLoaded\s+&&\s+Platform\.OS\s*!==\s*['"]web['"]/);
    });

    it('should NOT unconditionally block on !fontsLoaded', () => {
      // Must not have a bare "if (!fontsLoaded)" without a platform check
      const lines = fileContent.split('\n');
      const bareBlock = lines.some(
        (line) =>
          line.includes('if (!fontsLoaded)') &&
          !line.includes('Platform') &&
          !line.includes('//')
      );
      expect(bareBlock).toBe(false);
    });
  });

  // ========================================================================
  // C) Web font fallback stacks
  // ========================================================================
  describe('Web font fallback stacks', () => {
    it('should use platform-specific FONTS (web vs native)', () => {
      expect(fileContent).toMatch(/Platform\.OS\s*===\s*['"]web['"]/);
    });

    it('should include serif fallback for heading font on web', () => {
      // Web heading should fall back to Georgia/serif
      expect(fileContent).toMatch(/heading:.*Georgia.*serif/);
    });

    it('should include sans-serif fallback for body font on web', () => {
      // Web body should fall back to system sans-serif stack
      expect(fileContent).toMatch(/body:.*-apple-system.*sans-serif/);
    });

    it('should keep bare font names for native', () => {
      // Native block should have simple font name without fallbacks
      expect(fileContent).toContain("heading: 'DMSerifDisplay_400Regular'");
      expect(fileContent).toContain("body: 'Inter_400Regular'");
    });
  });

  // ========================================================================
  // D) No unused font assets
  // ========================================================================
  describe('Unused font assets', () => {
    it('should NOT have SpaceMono-Regular.ttf in assets/fonts', () => {
      const fontsDir = path.join(__dirname, '../assets/fonts');
      const files = fs.existsSync(fontsDir) ? fs.readdirSync(fontsDir) : [];
      expect(files).not.toContain('SpaceMono-Regular.ttf');
    });
  });

  // ========================================================================
  // E) TYPOGRAPHY consistency
  // ========================================================================
  describe('TYPOGRAPHY presets', () => {
    it('should define h1 with FONTS.heading', () => {
      expect(fileContent).toMatch(/h1:.*fontFamily:\s*FONTS\.heading/);
    });

    it('should define body with FONTS.body', () => {
      expect(fileContent).toMatch(/body:.*fontFamily:\s*FONTS\.body/);
    });

    it('should define all expected typography presets', () => {
      const expectedKeys = ['h1', 'h2', 'h3', 'subtitle', 'body', 'bodyLarge', 'label', 'button', 'caption', 'tabLabel', 'metric', 'metricSmall', 'sectionTitle'];
      for (const key of expectedKeys) {
        expect(fileContent).toContain(`${key}:`);
      }
    });
  });
});
