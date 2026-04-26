const fs = require('fs');
const path = require('path');

describe('Tracklist UX regression', () => {
  const stockDetailPath = path.join(__dirname, '../app/stock/[ticker].tsx');
  const dashboardPath = path.join(__dirname, '../app/(tabs)/dashboard.tsx');
  const searchPath = path.join(__dirname, '../app/(tabs)/search.tsx');
  const membershipPillsPath = path.join(__dirname, '../constants/membershipPills.ts');
  const tabsLayoutPath = path.join(__dirname, '../app/(tabs)/_layout.tsx');
  const portfolioPath = path.join(__dirname, '../app/(tabs)/portfolio.tsx');
  const tracklistPath = path.join(__dirname, '../app/(tabs)/tracklist.tsx');
  const appHeaderPath = path.join(__dirname, '../components/AppHeader.tsx');

  const stockDetail = fs.readFileSync(stockDetailPath, 'utf-8');
  const dashboard = fs.readFileSync(dashboardPath, 'utf-8');
  const search = fs.readFileSync(searchPath, 'utf-8');
  const membershipPills = fs.readFileSync(membershipPillsPath, 'utf-8');
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

  it('disables Tracklist actions for tickers already in Tracklist', () => {
    expect(stockDetail).toContain("if (target === 'tracklist') {");
    expect(stockDetail).toContain('if (listMemberships.tracklist) {');
    expect(stockDetail).toContain('disabled={listMemberships.tracklist}');
    expect(stockDetail).toContain('listMemberships.tracklist && styles.addToButtonDisabled');
    expect(stockDetail).toContain('listMemberships.tracklist && styles.addToSheetItemDisabled');
    expect(stockDetail).toContain('disabled={listActionLoading || listMemberships.tracklist}');
  });

  it('keeps search passive and pill-driven only', () => {
    expect(search).toContain("getMembershipPillConfig");
    expect(search).not.toContain("W / T badges show where each ticker already lives.");
    expect(search).not.toContain("Passive search only");
    expect(search).not.toContain('star-toggle-');
    expect(search).not.toContain("/api/v1/watchlist/");
    expect(search).toContain('renderMembershipPills');
    expect(search).not.toContain('/api/v1/tracklist/replace');
  });

  it('shows tracklist performance on the homepage', () => {
    expect(dashboard).toContain('My Tracklist performance');
    expect(dashboard).toContain('Based on your Tracklist (equal-weight)');
    expect(dashboard).toContain('+Watchlist');
    expect(dashboard).toContain("getMembershipPillConfig");
    expect(dashboard).toContain("setPerformanceMode('USD')");
    expect(dashboard).toContain('Reward / Risk');
    expect(dashboard).toContain('Vs. Index');
    expect(dashboard).toContain('HIGH');
    expect(dashboard).toContain('LOW');
  });

  it('shares membership pill colors across screens', () => {
    expect(membershipPills).toContain("watchlist: { label: 'Watchlist', bg: '#FEF3C7', text: '#B45309' }");
    expect(membershipPills).toContain("tracklist: { label: 'Tracklist', bg: '#DBEAFE', text: '#1D4ED8' }");
    expect(membershipPills).toContain("portfolio: { label: 'Portfolio', bg: '#EDE9FE', text: '#7C3AED' }");
    expect(membershipPills).toContain('getMembershipPillConfig');
  });

  it('hides portfolio and tracklist from bottom tabs while keeping dedicated screens', () => {
    expect(tabsLayout).toContain("name=\"portfolio\"");
    expect(tabsLayout).toContain("name=\"tracklist\"");
    expect(tabsLayout).toContain('href: null');
    expect(portfolioSoonPage).toContain('Portfolio is temporarily disabled');
    expect(appHeader).toContain('menu-portfolio');
    expect(appHeader).toContain('Soon');
    expect(appHeader).toContain("style={[styles.menuItem, styles.menuItemDisabled]}");
    expect(appHeader).not.toContain("handleMenuItemPress('portfolio')");
    expect(tracklistPage).toContain('Your Tracklist');
    expect(tracklistPage).toContain('You can still replace names from this overview.');
    expect(tracklistPage).toContain('Auto-assigned basket');
    expect(tracklistPage).toContain('settings-outline');
    expect(tracklistPage).toContain('Replace flow');
    expect(tracklistPage).toContain('Replace');
  });
});

module.exports = {};
