const fs = require('fs');
const path = require('path');

describe('Tracklist UX regression', () => {
  const stockDetailPath = path.join(__dirname, '../app/stock/[ticker].tsx');
  const dashboardPath = path.join(__dirname, '../app/(tabs)/dashboard.tsx');
  const searchPath = path.join(__dirname, '../app/(tabs)/search.tsx');
  const tabsLayoutPath = path.join(__dirname, '../app/(tabs)/_layout.tsx');
  const portfolioPath = path.join(__dirname, '../app/(tabs)/portfolio.tsx');
  const tracklistPath = path.join(__dirname, '../app/(tabs)/tracklist.tsx');
  const appHeaderPath = path.join(__dirname, '../components/AppHeader.tsx');

  const stockDetail = fs.readFileSync(stockDetailPath, 'utf-8');
  const dashboard = fs.readFileSync(dashboardPath, 'utf-8');
  const search = fs.readFileSync(searchPath, 'utf-8');
  const tabsLayout = fs.readFileSync(tabsLayoutPath, 'utf-8');
  const portfolioSoonPage = fs.readFileSync(portfolioPath, 'utf-8');
  const tracklistPage = fs.readFileSync(tracklistPath, 'utf-8');
  const appHeader = fs.readFileSync(appHeaderPath, 'utf-8');

  it('moves list actions into the Last close card', () => {
    expect(stockDetail).toContain('Last Close');
    expect(stockDetail).toContain('Add to');
    expect(stockDetail).toContain('Watchlist');
    expect(stockDetail).toContain('Tracklist');
    expect(stockDetail).toContain('Portfolio');
    expect(stockDetail).not.toContain('Changes apply at next close.');
    expect(stockDetail).toContain("pathname: '/(tabs)/tracklist'");
  });

  it('keeps search passive and badge-driven only', () => {
    expect(search).toContain("const MEMBERSHIP_LABELS");
    expect(search).toContain("W / T badges show where each ticker already lives.");
    expect(search).not.toContain('star-toggle-');
    expect(search).not.toContain("/api/v1/watchlist/");
    expect(search).toContain('renderMembershipBadges');
    expect(search).not.toContain('/api/v1/tracklist/replace');
  });

  it('shows tracklist performance on the homepage', () => {
    expect(dashboard).toContain('My Tracklist performance');
    expect(dashboard).toContain('Based on your Tracklist (equal-weight)');
    expect(dashboard).toContain("setPerformanceMode('USD')");
    expect(dashboard).toContain('Reward / Risk');
    expect(dashboard).toContain('Vs. Index');
    expect(dashboard).toContain('HIGH');
    expect(dashboard).toContain('LOW');
  });

  it('hides portfolio and tracklist from bottom tabs while keeping dedicated screens', () => {
    expect(tabsLayout).toContain("name=\"portfolio\"");
    expect(tabsLayout).toContain("name=\"tracklist\"");
    expect(tabsLayout).toContain('href: null');
    expect(portfolioSoonPage).toContain('Portfolio is temporarily disabled');
    expect(appHeader).toContain('menu-portfolio');
    expect(appHeader).toContain('Soon');
    expect(tracklistPage).toContain('Your Tracklist');
    expect(tracklistPage).toContain('You can still replace names from this overview.');
    expect(tracklistPage).toContain('Auto-assigned basket');
    expect(tracklistPage).toContain('settings-outline');
    expect(tracklistPage).toContain('Replace flow');
    expect(tracklistPage).toContain('Replace');
  });
});

module.exports = {};
