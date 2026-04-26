import { create } from 'zustand';

interface MarketsStateSnapshot {
  calendarView: string | null;
  selectedDateKey: string | null;
  displayMonthKey: string | null;
  selectedYear: number | null;
  selectedEventType: string | null;
  tickerFilter: string;
  visibleFeedLimit: number | null;
  marketFeedModes: string[];
  scrollY: number;
}

interface MarketsState extends MarketsStateSnapshot {
  setState: (partial: Partial<MarketsStateSnapshot>) => void;
  reset: () => void;
}

const INITIAL_STATE: MarketsStateSnapshot = {
  calendarView: null,
  selectedDateKey: null,
  displayMonthKey: null,
  selectedYear: null,
  selectedEventType: null,
  tickerFilter: '',
  visibleFeedLimit: null,
  marketFeedModes: [],
  scrollY: 0,
};

export const useMarketsStore = create<MarketsState>((set) => ({
  ...INITIAL_STATE,
  setState: (partial) => set((state) => ({ ...state, ...partial })),
  reset: () => set(INITIAL_STATE),
}));
