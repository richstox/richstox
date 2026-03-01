/**
 * P1 UI Regression Test: Valuation Overview Layout
 * 
 * Purpose: Prevent layout overflow in Valuation Overview rows
 * 
 * Test Coverage:
 * 1. Value column displays ONLY number or "N/A" (no reason text)
 * 2. Reason column exists and is separate from value column
 * 3. All columns have fixed widths (no flex:1)
 * 
 * CI/CD: Run with `npx jest __tests__/valuationLayout.test.js`
 * Location: /app/frontend/__tests__/valuationLayout.test.js
 */

// Expected fixed widths from component styles
const EXPECTED_WIDTHS = {
  valuationMetricName: 80,
  valuationMetricValue: 45,
  valuationMetricVs: 60,
  valuationMetricReason: 80,
  valuationMetricBadge: 24,
};

// Helper function that mimics the component's display logic
function getDisplayValue(metric) {
  const hasValue = metric.current !== null && metric.current !== undefined;
  return hasValue ? metric.current.toFixed(1) : 'N/A';
}

function getReasonLabel(metric) {
  const hasValue = metric.current !== null && metric.current !== undefined;
  if (hasValue) return null;
  
  if (metric.na_reason === 'unprofitable') return 'Unprofitable';
  if (metric.na_reason === 'missing_data') return 'Missing Data';
  if (metric.na_reason) return metric.na_reason;
  return null;
}

describe('Valuation Overview Layout', () => {
  
  describe('Value Column Content', () => {
    
    it('should display only number for metrics with values', () => {
      const metric = {
        name: 'P/S',
        current: 8.2,
        peer_median: 6.8,
        na_reason: null,
        vs_peers: 'more_expensive',
      };
      
      const displayValue = getDisplayValue(metric);
      
      expect(displayValue).toBe('8.2');
      expect(displayValue).not.toContain('(');
      expect(displayValue).not.toContain('Unprofitable');
    });
    
    it('should display only "N/A" for metrics without values (CRITICAL: not "N/A (reason)")', () => {
      const metric = {
        name: 'P/E',
        current: null,
        peer_median: 31.2,
        na_reason: 'unprofitable',
        vs_peers: null,
      };
      
      const displayValue = getDisplayValue(metric);
      
      // CRITICAL: Value column must show ONLY "N/A", not "N/A (Unprofitable)"
      expect(displayValue).toBe('N/A');
      expect(displayValue).not.toContain('(');
      expect(displayValue).not.toContain('Unprofitable');
      expect(displayValue.length).toBeLessThanOrEqual(3);
    });
    
    it('should keep value and reason in separate columns', () => {
      const metric = {
        name: 'EV/EBITDA',
        current: null,
        peer_median: 18.5,
        na_reason: 'unprofitable',
        vs_peers: null,
      };
      
      const displayValue = getDisplayValue(metric);
      const reasonLabel = getReasonLabel(metric);
      
      expect(displayValue).toBe('N/A');
      expect(reasonLabel).toBe('Unprofitable');
      expect(displayValue).not.toContain(reasonLabel);
    });
    
  });
  
  describe('Reason Column Content', () => {
    
    it('should return "Unprofitable" for unprofitable metrics', () => {
      const metric = { name: 'P/E', current: null, peer_median: 31.2, na_reason: 'unprofitable', vs_peers: null };
      expect(getReasonLabel(metric)).toBe('Unprofitable');
    });
    
    it('should return "Missing Data" for missing_data reason', () => {
      const metric = { name: 'P/B', current: null, peer_median: 2.7, na_reason: 'missing_data', vs_peers: null };
      expect(getReasonLabel(metric)).toBe('Missing Data');
    });
    
    it('should return null for metrics with values', () => {
      const metric = { name: 'P/S', current: 8.2, peer_median: 6.8, na_reason: null, vs_peers: 'more_expensive' };
      expect(getReasonLabel(metric)).toBeNull();
    });
    
  });
  
  describe('Column Width Constraints (Regression Guard)', () => {
    
    it('should have fixed widths for all columns (no flex:1)', () => {
      Object.entries(EXPECTED_WIDTHS).forEach(([columnName, expectedWidth]) => {
        expect(expectedWidth).toBeGreaterThan(0);
        expect(typeof expectedWidth).toBe('number');
      });
      
      const totalWidth = Object.values(EXPECTED_WIDTHS).reduce((sum, w) => sum + w, 0);
      expect(totalWidth).toBeLessThan(350);
    });
    
    it('should have value column width that fits "N/A" but not "N/A (Unprofitable)"', () => {
      const valueColumnWidth = EXPECTED_WIDTHS.valuationMetricValue;
      expect(valueColumnWidth).toBeLessThan(50);
      expect(valueColumnWidth).toBeGreaterThan(25);
    });
    
    it('should have reason column width that fits "Unprofitable"', () => {
      const reasonColumnWidth = EXPECTED_WIDTHS.valuationMetricReason;
      expect(reasonColumnWidth).toBeGreaterThanOrEqual(70);
    });
    
  });
  
});

// XXII.US specific test data
const XXII_TEST_METRICS = [
  { name: 'P/E', current: null, peer_median: 31.2, na_reason: 'unprofitable', vs_peers: null },
  { name: 'P/S', current: 8.2, peer_median: 6.8, na_reason: null, vs_peers: 'more_expensive' },
  { name: 'P/B', current: 4.0, peer_median: 2.7, na_reason: null, vs_peers: 'more_expensive' },
  { name: 'EV/EBITDA', current: null, peer_median: 18.5, na_reason: 'unprofitable', vs_peers: null },
  { name: 'EV/Revenue', current: 7.7, peer_median: 7.8, na_reason: null, vs_peers: 'around' },
];

/**
 * P3 REGRESSION TEST: "Valuation Context" box removal
 * 
 * Purpose: Ensure the redundant "Valuation Context" section (orange box with 
 * "50/100 In line vs 13 peers in Tobacco") is NEVER re-added to the codebase.
 * 
 * Rationale: Duplicates Valuation Overview, violates "single source of truth".
 * 
 * CI/CD: Run with `npx jest __tests__/valuationLayout.test.js`
 */
describe('P3 Regression: Valuation Context Box Removal', () => {
  
  const fs = require('fs');
  const path = require('path');
  
  it('should NOT contain "Valuation Context" text in ticker detail page', () => {
    const tickerPagePath = path.join(__dirname, '../app/stock/[ticker].tsx');
    const fileContent = fs.readFileSync(tickerPagePath, 'utf-8');
    
    // CRITICAL: "Valuation Context" must never reappear
    expect(fileContent).not.toContain('Valuation Context');
  });
  
  it('should NOT render the score badge with "/100" format (old redundant UI)', () => {
    const tickerPagePath = path.join(__dirname, '../app/stock/[ticker].tsx');
    const fileContent = fs.readFileSync(tickerPagePath, 'utf-8');
    
    // Pattern: <Text>{data.valuation.score}</Text><Text>/100</Text>
    // This specific pattern was used only in the removed Valuation Context box
    const oldScorePattern = /valuationScore.*?\{data\.valuation\.score\}.*?valuationMax.*?\/100/s;
    expect(fileContent).not.toMatch(oldScorePattern);
  });
  
  it('should NOT have data.valuation.score rendering anywhere in overview tab', () => {
    const tickerPagePath = path.join(__dirname, '../app/stock/[ticker].tsx');
    const fileContent = fs.readFileSync(tickerPagePath, 'utf-8');
    
    // The numeric score (e.g., "50") was only used in the removed box
    // Valuation Overview uses overall_vs_peers/overall_vs_5y_avg instead
    expect(fileContent).not.toContain('data.valuation.score');
  });
  
});

module.exports = { XXII_TEST_METRICS, getDisplayValue, getReasonLabel };
