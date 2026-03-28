/**
 * Search results store - persists last search query and results
 * so users can navigate between tickers from search results.
 */
import { create } from 'zustand';

export interface SearchResult {
  ticker: string;
  name: string;
  exchange: string;
  logo?: string;
  is_following?: boolean;
}

interface SearchState {
  query: string;
  results: SearchResult[];
  setSearch: (query: string, results: SearchResult[]) => void;
  clearSearch: () => void;
}

export const useSearchStore = create<SearchState>((set) => ({
  query: '',
  results: [],
  setSearch: (query, results) => set({ query, results }),
  clearSearch: () => set({ query: '', results: [] }),
}));
