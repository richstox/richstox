/**
 * WEB RAILS HYDRATION SAFETY TEST
 *
 * Regression guard for React hydration error #418 caused by WebRails
 * rendering different DOM structures on server (SSG) vs client.
 *
 * Root cause: useWindowDimensions() returns width=0 during static export
 * (no browser window), so showRails=false → renders <>{children}</>.
 * On the client with a wide viewport, showRails=true → renders a wrapper
 * with three children (left rail, center, right rail). This structural
 * mismatch triggers React error #418.
 *
 * Fix: defer the showRails decision until after mount via a `mounted`
 * state flag set in useEffect, so the first render always matches SSG.
 *
 * CI/CD: Run with `npx jest __tests__/webRailsHydration.test.js`
 */

const fs = require('fs');
const path = require('path');

describe('WebRails: Hydration Safety', () => {
  const webRailsPath = path.join(__dirname, '../components/WebRails.tsx');
  let src;

  beforeAll(() => {
    src = fs.readFileSync(webRailsPath, 'utf-8');
  });

  // ========================================================================
  // A) MOUNTED GUARD EXISTS
  // ========================================================================
  describe('Mounted guard (anti-hydration-mismatch)', () => {
    it('should define a mounted state initialised to false', () => {
      // Must start false so the first render matches SSG output.
      expect(src).toMatch(/useState\s*\(\s*false\s*\)/);
    });

    it('should set mounted to true inside a useEffect', () => {
      // useEffect fires only on the client after hydration.
      expect(src).toMatch(/useEffect\s*\(\s*\(\)\s*=>\s*\{\s*setMounted\s*\(\s*true\s*\)/);
    });

    it('should gate showRails behind the mounted flag', () => {
      // showRails must include `mounted &&` to prevent SSG/client divergence.
      expect(src).toMatch(/showRails\s*=\s*mounted\s*&&/);
    });
  });

  // ========================================================================
  // B) STRUCTURAL INVARIANT
  // ========================================================================
  describe('First-render invariant', () => {
    it('should NOT derive showRails purely from windowWidth (would cause mismatch)', () => {
      // Negative test: the old code had `showRails = windowWidth >= RAIL_BREAKPOINT`
      // without the mounted guard. Every non-comment assignment to showRails
      // must include `mounted` on the right-hand side.
      const lines = src.split('\n');
      const dangerous = lines.some(line => {
        const trimmed = line.trim();
        // Skip comment lines
        if (trimmed.startsWith('//') || trimmed.startsWith('*')) return false;
        // Only look at lines that assign showRails
        if (!/showRails\s*=/.test(trimmed)) return false;
        // It MUST contain 'mounted' to be safe
        return !trimmed.includes('mounted');
      });
      expect(dangerous).toBe(false);
    });
  });

  // ========================================================================
  // C) PLATFORM GATE
  // ========================================================================
  describe('Native pass-through', () => {
    it('should bail out early for non-web platforms', () => {
      // Native platforms never run the rails logic, so no hydration concern.
      expect(src).toMatch(/Platform\.OS\s*!==\s*['"]web['"]/);
    });
  });
});
