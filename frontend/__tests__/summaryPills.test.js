/**
 * P5/P6 REGRESSION TEST: Summary Pills with RICHSTOX Elite Matrix
 * 
 * P6: Elite Matrix Rules:
 * - Max 2 pills ("Rule of 2")
 * - Risk pills (priority 1-5) before Strength pills (priority 6-10)
 * - "In line" if no triggers match
 * - "N/A (Missing data)" if critical data missing
 * - NEVER show "Data loaded"
 * 
 * CI/CD: Run with `npx jest __tests__/summaryPills.test.js`
 */

const fs = require('fs');
const path = require('path');

describe('P6 Regression: RICHSTOX Elite Matrix', () => {
  
  const tickerPagePath = path.join(__dirname, '../app/stock/[ticker].tsx');
  let fileContent;
  
  beforeAll(() => {
    fileContent = fs.readFileSync(tickerPagePath, 'utf-8');
  });
  
  // ==========================================================================
  // P6 CRITICAL: "Data loaded" must NEVER appear
  // ==========================================================================
  describe('Critical: No "Data loaded" placeholder', () => {
    
    it('should NOT contain "Data loaded" string anywhere', () => {
      expect(fileContent).not.toContain("'Data loaded'");
      expect(fileContent).not.toContain('"Data loaded"');
    });
    
  });
  
  // ==========================================================================
  // A) KEY METRICS ELITE MATRIX
  // ==========================================================================
  describe('Key Metrics Elite Matrix', () => {
    
    it('should have getKeyMetricsPills function with Elite Matrix comment', () => {
      expect(fileContent).toContain('RICHSTOX ELITE MATRIX');
    });
    
    it('should return N/A when key_metrics is missing', () => {
      expect(fileContent).toContain("return ['N/A (Missing data)']");
    });
    
    // Priority 1-5: Risk & Warning pills
    it('should check for Unprofitable (priority 1)', () => {
      expect(fileContent).toContain("pills.push('Unprofitable')");
    });
    
    it('should check for Overleveraged (priority 2)', () => {
      expect(fileContent).toContain("pills.push('Overleveraged')");
    });
    
    it('should check for Burning Cash (priority 3)', () => {
      expect(fileContent).toContain("pills.push('Burning Cash')");
    });
    
    it('should check for Revenue Decline (priority 4)', () => {
      expect(fileContent).toContain("pills.push('Revenue Decline')");
    });
    
    it('should check for Debt: N/A (priority 5)', () => {
      expect(fileContent).toContain("pills.push('Debt: N/A')");
    });
    
    // Priority 6-10: Strength & Elite pills
    it('should check for Profit Leader (priority 6)', () => {
      expect(fileContent).toContain("pills.push('Profit Leader')");
    });
    
    it('should check for Cash King (priority 7)', () => {
      expect(fileContent).toContain("pills.push('Cash King')");
    });
    
    it('should check for FCF Powerhouse (priority 8)', () => {
      expect(fileContent).toContain("pills.push('FCF Powerhouse')");
    });
    
    it('should check for Hyper Growth (priority 9)', () => {
      expect(fileContent).toContain("pills.push('Hyper Growth')");
    });
    
    it('should check for Dividend Elite (priority 10)', () => {
      expect(fileContent).toContain("pills.push('Dividend Elite')");
    });
    
    // Rule of 2
    it('should limit to max 2 pills (Rule of 2)', () => {
      expect(fileContent).toMatch(/pills\.length\s*<\s*2/);
    });
    
    // Fallback
    it('should return "In line" when no triggers match', () => {
      expect(fileContent).toContain("return ['In line']");
    });
    
    it('should render pills only when collapsed', () => {
      expect(fileContent).toContain('!keyMetricsExpanded && (');
      expect(fileContent).toContain('data-testid="key-metrics-pills"');
    });
    
  });
  
  // ==========================================================================
  // B) DIVIDENDS PILL (unchanged from P5)
  // ==========================================================================
  describe('Dividends Pill', () => {
    
    it('should have getDividendPill function defined', () => {
      expect(fileContent).toContain('const getDividendPill');
    });
    
    it('should classify dividend trend (Growing/Stable/Cutting)', () => {
      expect(fileContent).toContain("return 'Growing'");
      expect(fileContent).toContain("return 'Stable'");
      expect(fileContent).toContain("return 'Cutting'");
    });
    
  });
  
  // ==========================================================================
  // C) FINANCIALS PILL (unchanged from P5)
  // ==========================================================================
  describe('Financials Pill', () => {
    
    it('should have getFinancialsPill function defined', () => {
      expect(fileContent).toContain('const getFinancialsPill');
    });
    
    it('should classify revenue trend (Revenue up/down)', () => {
      expect(fileContent).toContain("return 'Revenue up'");
      expect(fileContent).toContain("return 'Revenue down'");
    });
    
  });
  
  // ==========================================================================
  // D) PILL VARIANT STYLING
  // ==========================================================================
  describe('Pill Variant Styling', () => {
    
    it('should have negative variant for risk pills', () => {
      expect(fileContent).toContain("'Unprofitable'");
      expect(fileContent).toContain("'Overleveraged'");
      expect(fileContent).toContain("'Burning Cash'");
      expect(fileContent).toContain("'Revenue Decline'");
    });
    
    it('should have positive variant for elite pills', () => {
      expect(fileContent).toContain("'Profit Leader'");
      expect(fileContent).toContain("'Cash King'");
      expect(fileContent).toContain("'FCF Powerhouse'");
      expect(fileContent).toContain("'Hyper Growth'");
      expect(fileContent).toContain("'Dividend Elite'");
    });
    
  });
  
});

module.exports = {};
