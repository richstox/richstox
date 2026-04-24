const fs = require('fs');
const path = require('path');

describe('Tracklist UX regression', () => {
  const stockDetailPath = path.join(__dirname, '../app/stock/[ticker].tsx');
  const dashboardPath = path.join(__dirname, '../app/(tabs)/dashboard.tsx');
  const searchPath = path.join(__dirname, '../app/(tabs)/search.tsx');
  const tabsLayoutPath = path.join(__dirname, '../app/(tabs)/_layout.tsx');
  const portfolioPath = path.join(__dirname, '../app/(tabs)/portfolio.tsx');

  const stockDetail = fs.readFileSync(stockDetailPath, 'utf-8');
  const dashboard = fs.readFileSync(dashboardPath, 'utf-8');
  const search = fs.readFileSync(searchPath, 'utf-8');
  const tabsLayout = fs.readFileSync(tabsLayoutPath, 'utf-8');
  const tracklistPage = fs.readFileSync(portfolioPath, 'utf-8');

  it('moves list actions into the Last close card', () => {
    expect(stockDetail).toContain('Last close');
    expect(stockDetail).toContain('Add to');
    expect(stockDetail).toContain('Watchlist');
    expect(stockDetail).toContain('Tracklist');
    expect(stockDetail).toContain('Changes apply at next close.');
  });

  it('makes search passive and badge-driven outside replace mode', () => {
    expect(search).toContain("const MEMBERSHIP_LABELS");
    expect(search).toContain("W / T badges show where each ticker already lives.");
    expect(search).not.toContain('star-toggle-');
    expect(search).not.toContain("/api/v1/watchlist/");
    expect(search).toContain("memberships.includes('tracklist')");
  });

  it('shows tracklist performance on the homepage', () => {
    expect(dashboard).toContain('My Tracklist performance');
    expect(dashboard).toContain('Tracklist (equal-weight, 7 stocks)');
    expect(dashboard).toContain("setPerformanceMode('USD')");
    expect(dashboard).toContain('Track Record');
  });

  it('renames the portfolio tab to Tracklist and adds a replace screen', () => {
    expect(tabsLayout).toContain("title: 'Tracklist'");
    expect(tracklistPage).toContain('Your Tracklist');
    expect(tracklistPage).toContain('Replace');
    expect(tracklistPage).toContain("mode: 'tracklist-replace'");
  });
});

module.exports = {};
