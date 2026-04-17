/**
 * Unit tests for number formatting functions used in Financials table
 * and Key Metrics display.
 *
 * Verifies magnitude-to-suffix mapping:
 *   1e3 = K, 1e6 = M, 1e9 = B, 1e12 = T
 *
 * CI/CD: Run with `npx jest __tests__/formatCurrency.test.js`
 */

// ── FinancialHub.tsx formatCurrency (local function) ─────────────────────────
// Extracted verbatim so we can unit-test it without importing React Native.
const formatCurrencyHub = (value) => {
  if (value === null || value === undefined) return 'N/A';

  const absValue = Math.abs(value);
  const sign = value < 0 ? '-' : '';

  // Trillions
  if (absValue >= 1e12) {
    const scaled = absValue / 1e12;
    if (scaled >= 100) return `${sign}$${Math.round(scaled)}T`;
    if (scaled >= 10) return `${sign}$${scaled.toFixed(1)}T`;
    return `${sign}$${scaled.toFixed(2)}T`;
  }

  // Billions
  if (absValue >= 1e9) {
    const scaled = absValue / 1e9;
    if (scaled >= 100) return `${sign}$${Math.round(scaled)}B`;
    if (scaled >= 10) return `${sign}$${scaled.toFixed(1)}B`;
    return `${sign}$${scaled.toFixed(2)}B`;
  }

  // Millions
  if (absValue >= 1e6) {
    const scaled = absValue / 1e6;
    if (scaled >= 100) return `${sign}$${Math.round(scaled)}M`;
    if (scaled >= 10) return `${sign}$${scaled.toFixed(1)}M`;
    return `${sign}$${scaled.toFixed(2)}M`;
  }

  // Thousands
  if (absValue >= 1e3) {
    const scaled = absValue / 1e3;
    if (scaled >= 100) return `${sign}$${Math.round(scaled)}K`;
    if (scaled >= 10) return `${sign}$${scaled.toFixed(1)}K`;
    return `${sign}$${scaled.toFixed(2)}K`;
  }

  // Small numbers
  return `${sign}$${absValue.toFixed(0)}`;
};

// ── [ticker].tsx formatCurrency (EU style with negative handling) ────────────
// EU-style (dots for thousands, comma for decimal)
const toEU = (value, decimals = 2) => {
  if (value === null || value === undefined || isNaN(value)) return 'N/A';
  const fixed = value.toFixed(decimals);
  const [intPart, decPart] = fixed.split('.');
  const intWithDots = intPart.replace(/\B(?=(\d{3})+(?!\d))/g, '.');
  return decPart ? `${intWithDots},${decPart}` : intWithDots;
};

const formatCurrencyTicker = (value) => {
  if (value === null || value === undefined) return 'N/A';
  const absValue = Math.abs(value);
  const sign = value < 0 ? '-' : '';
  if (absValue >= 1e12) return `${sign}$${toEU(absValue / 1e12, 2)}T`;
  if (absValue >= 1e9) return `${sign}$${toEU(absValue / 1e9, 2)}B`;
  if (absValue >= 1e6) return `${sign}$${toEU(absValue / 1e6, 2)}M`;
  return `${sign}$${toEU(absValue, 2)}`;
};

// =============================================================================
// TESTS: FinancialHub formatCurrency
// =============================================================================
describe('FinancialHub formatCurrency', () => {
  describe('null / undefined', () => {
    test('null → N/A', () => expect(formatCurrencyHub(null)).toBe('N/A'));
    test('undefined → N/A', () => expect(formatCurrencyHub(undefined)).toBe('N/A'));
  });

  describe('boundary values – positive', () => {
    test('999 → $999', () => expect(formatCurrencyHub(999)).toBe('$999'));
    test('1_000 → $1.00K', () => expect(formatCurrencyHub(1_000)).toBe('$1.00K'));
    test('999_999 → $1000K rounds to $1000K', () => {
      const r = formatCurrencyHub(999_999);
      // 999999/1000 = 999.999 → scaled >= 100 → Math.round(999.999)=1000 → "$1000K"
      expect(r).toBe('$1000K');
    });
    test('1_000_000 → $1.00M', () => expect(formatCurrencyHub(1_000_000)).toBe('$1.00M'));
    test('10_700_000 → $10.7M', () => expect(formatCurrencyHub(10_700_000)).toBe('$10.7M'));
    test('999_999_999 → $1000M', () => {
      const r = formatCurrencyHub(999_999_999);
      // 999999999/1e6 = 999.999999 → scaled >= 100 → Math.round=1000 → "$1000M"
      expect(r).toBe('$1000M');
    });
    test('1_000_000_000 → $1.00B', () => expect(formatCurrencyHub(1_000_000_000)).toBe('$1.00B'));
    test('1_000_000_000_000 → $1.00T', () => expect(formatCurrencyHub(1_000_000_000_000)).toBe('$1.00T'));
  });

  describe('boundary values – negative', () => {
    test('-999 → -$999', () => expect(formatCurrencyHub(-999)).toBe('-$999'));
    test('-1_000 → -$1.00K', () => expect(formatCurrencyHub(-1_000)).toBe('-$1.00K'));
    test('-1_000_000 → -$1.00M', () => expect(formatCurrencyHub(-1_000_000)).toBe('-$1.00M'));
    test('-2_590_000 → -$2.59M', () => expect(formatCurrencyHub(-2_590_000)).toBe('-$2.59M'));
    test('-1_000_000_000 → -$1.00B', () => expect(formatCurrencyHub(-1_000_000_000)).toBe('-$1.00B'));
    test('-1_000_000_000_000 → -$1.00T', () => expect(formatCurrencyHub(-1_000_000_000_000)).toBe('-$1.00T'));
  });

  describe('ONFO.US expected after fix', () => {
    test('Revenue ~10.7M → $10.7M (not T)', () => {
      expect(formatCurrencyHub(10_700_000)).toBe('$10.7M');
    });
    test('Net Income ~-2.59M → -$2.59M (not T)', () => {
      expect(formatCurrencyHub(-2_590_000)).toBe('-$2.59M');
    });
  });

  describe('never uses T suffix below 1e12', () => {
    test('999_999_999_999 → $1000B (not T)', () => {
      // 999_999_999_999 < 1e12 → falls into billions
      const r = formatCurrencyHub(999_999_999_999);
      expect(r).not.toContain('T');
      expect(r).toContain('B');
    });
    test('1e12 exactly → $1.00T', () => {
      expect(formatCurrencyHub(1e12)).toBe('$1.00T');
    });
  });
});

// =============================================================================
// TESTS: [ticker].tsx formatCurrency (EU style, with fix for negatives)
// =============================================================================
describe('[ticker].tsx formatCurrency (EU style)', () => {
  describe('null / undefined / zero', () => {
    test('null → N/A', () => expect(formatCurrencyTicker(null)).toBe('N/A'));
    test('undefined → N/A', () => expect(formatCurrencyTicker(undefined)).toBe('N/A'));
    test('0 → $0,00', () => expect(formatCurrencyTicker(0)).toBe('$0,00'));
  });

  describe('positive values with EU formatting', () => {
    test('1_000_000 → $1,00M', () => expect(formatCurrencyTicker(1_000_000)).toBe('$1,00M'));
    test('10_700_000 → $10,70M', () => expect(formatCurrencyTicker(10_700_000)).toBe('$10,70M'));
    test('1_000_000_000 → $1,00B', () => expect(formatCurrencyTicker(1_000_000_000)).toBe('$1,00B'));
    test('1_000_000_000_000 → $1,00T', () => expect(formatCurrencyTicker(1_000_000_000_000)).toBe('$1,00T'));
  });

  describe('negative values handled correctly (fix)', () => {
    test('-2_590_000 → -$2,59M', () => expect(formatCurrencyTicker(-2_590_000)).toBe('-$2,59M'));
    test('-1_000_000_000 → -$1,00B', () => expect(formatCurrencyTicker(-1_000_000_000)).toBe('-$1,00B'));
  });

  describe('never uses T suffix below 1e12', () => {
    test('999_999_999_999 uses B not T', () => {
      const r = formatCurrencyTicker(999_999_999_999);
      expect(r).not.toContain('T');
      expect(r).toContain('B');
    });
  });
});
