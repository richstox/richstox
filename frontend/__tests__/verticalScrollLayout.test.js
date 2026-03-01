/**
 * P4 REGRESSION TEST: Single Vertical Scroll Layout
 * 
 * Purpose: Ensure tabs are never re-added and section order is preserved.
 * Binding comment: "Single vertical scroll, no tabs"
 * 
 * CI/CD: Run with `npx jest __tests__/verticalScrollLayout.test.js`
 */

const fs = require('fs');
const path = require('path');

describe('P4 Regression: Single Vertical Scroll Layout', () => {
  
  const tickerPagePath = path.join(__dirname, '../app/stock/[ticker].tsx');
  let fileContent;
  
  beforeAll(() => {
    fileContent = fs.readFileSync(tickerPagePath, 'utf-8');
  });
  
  describe('Tab Navigation Removal', () => {
    
    it('should NOT contain activeTab state', () => {
      expect(fileContent).not.toContain('activeTab');
      expect(fileContent).not.toContain('setActiveTab');
    });
    
    it('should NOT contain tab navigation UI (tabsContainer)', () => {
      // Tab container pattern: ['overview', 'financials', 'earnings', 'insider'].map
      expect(fileContent).not.toMatch(/\['overview',\s*'financials',\s*'earnings',\s*'insider'\]\.map/);
    });
    
    it('should NOT have conditional tab rendering (activeTab === "xxx")', () => {
      expect(fileContent).not.toMatch(/activeTab\s*===\s*['"]overview['"]/);
      expect(fileContent).not.toMatch(/activeTab\s*===\s*['"]financials['"]/);
      expect(fileContent).not.toMatch(/activeTab\s*===\s*['"]earnings['"]/);
      expect(fileContent).not.toMatch(/activeTab\s*===\s*['"]insider['"]/);
    });
    
    it('should contain binding comment for single vertical scroll', () => {
      expect(fileContent).toContain('Single vertical scroll, no tabs');
    });
    
  });
  
  describe('Section Order Verification', () => {
    
    // Expected section order (by data-testid)
    const EXPECTED_SECTION_ORDER = [
      'reality-check-card',      // 1. Price & Reality Check
      'valuation-card',          // 2. Valuation Overview
      'price-chart-card',        // 3. Price History Chart
      'key-metrics-section',     // 4. Key Metrics (collapsed)
      'financials-section',      // 5. Financials
      'earnings-section',        // 6. Earnings & Dividends
      'insider-section',         // 7. Insider Transactions
      'news-talk-section',       // 8. News & Talk
    ];
    
    it('should have all required sections with data-testid', () => {
      EXPECTED_SECTION_ORDER.forEach(testId => {
        expect(fileContent).toContain(`data-testid="${testId}"`);
      });
    });
    
    it('should have sections in correct order', () => {
      const positions = EXPECTED_SECTION_ORDER.map(testId => {
        const match = fileContent.indexOf(`data-testid="${testId}"`);
        return { testId, position: match };
      });
      
      // Verify each section appears after the previous one
      for (let i = 1; i < positions.length; i++) {
        const prev = positions[i - 1];
        const curr = positions[i];
        
        expect(curr.position).toBeGreaterThan(prev.position);
      }
    });
    
  });
  
  describe('Key Metrics Collapsible Behavior', () => {
    
    it('should have keyMetricsExpanded state (collapsed by default)', () => {
      // State initialization: useState(false) for collapsed
      expect(fileContent).toMatch(/keyMetricsExpanded.*useState\(false\)/);
    });
    
    it('should have collapsible header for Key Metrics', () => {
      expect(fileContent).toContain('collapsibleHeader');
      expect(fileContent).toContain('key-metrics-toggle');
    });
    
    it('should conditionally render Key Metrics content', () => {
      expect(fileContent).toMatch(/keyMetricsExpanded\s*&&/);
    });
    
  });
  
  describe('No Hidden Sections', () => {
    
    it('should always show sections (no data-based hiding)', () => {
      // Financials section should have N/A fallback
      expect(fileContent).toContain('No financials data available');
      
      // Earnings section should have N/A fallback
      expect(fileContent).toContain('No earnings data available');
      
      // Insider section should have N/A fallback
      expect(fileContent).toContain('No insider activity data');
    });
    
  });
  
});

module.exports = {};
