/**
 * MY STOCKS navigation store – persists the ordered ticker list
 * so users can page through tickers from the MY STOCKS section
 * without going back to the dashboard.
 *
 * Mirrors the searchStore pattern used for Search-results paging.
 */
import { create } from 'zustand';

interface MyStocksNavState {
  /** Ordered ticker symbols matching the visible MY STOCKS list at navigation time */
  tickers: string[];
  /** Populate with the current visible list when user taps a stock */
  setTickers: (tickers: string[]) => void;
  /** Clear the navigation context (e.g. user dismisses pager) */
  clearTickers: () => void;
}

export const useMyStocksStore = create<MyStocksNavState>((set) => ({
  tickers: [],
  setTickers: (tickers) => set({ tickers }),
  clearTickers: () => set({ tickers: [] }),
}));
