import React, { useEffect } from 'react';
import { View, Text, TouchableOpacity, Modal, Pressable, StyleSheet } from 'react-native';

interface TooltipContent {
  title: string;
  body: string;
  howToRead: string;
}

interface MetricTooltipProps {
  visible: boolean;
  onClose: () => void;
  content: TooltipContent;
}

export const MetricTooltip = ({ visible, onClose, content }: MetricTooltipProps) => {
  useEffect(() => {
    if (visible) {
      // Auto-close after 5 seconds of inactivity
      const timer = setTimeout(onClose, 5000);
      return () => clearTimeout(timer);
    }
  }, [visible, onClose]);

  if (!visible) return null;

  return (
    <Modal transparent visible={visible} animationType="slide" onRequestClose={onClose}>
      <Pressable style={styles.overlay} onPress={onClose}>
        <Pressable style={styles.sheet} onPress={(e) => e.stopPropagation()}>
          {/* Drag handle */}
          <View style={styles.dragHandle} />
          
          {/* Title */}
          <Text style={styles.title}>{content.title}</Text>
          
          {/* Body */}
          <Text style={styles.body}>{content.body}</Text>
          
          {/* How to read */}
          <Text style={styles.howToReadLabel}>HOW TO READ:</Text>
          <Text style={styles.howToReadText}>{content.howToRead}</Text>
          
          {/* Got it button */}
          <TouchableOpacity style={styles.gotItButton} onPress={onClose} data-testid="tooltip-got-it">
            <Text style={styles.gotItText}>Got it</Text>
          </TouchableOpacity>
        </Pressable>
      </Pressable>
    </Modal>
  );
};

const styles = StyleSheet.create({
  overlay: {
    flex: 1,
    backgroundColor: 'rgba(0,0,0,0.4)',
    justifyContent: 'flex-end',
  },
  sheet: {
    backgroundColor: '#FFFFFF',
    borderTopLeftRadius: 20,
    borderTopRightRadius: 20,
    padding: 24,
    paddingBottom: 40,
  },
  dragHandle: {
    width: 40,
    height: 4,
    backgroundColor: '#D1D5DB',
    borderRadius: 2,
    alignSelf: 'center',
    marginBottom: 20,
  },
  title: {
    fontSize: 20,
    fontWeight: '700',
    color: '#111827',
    marginBottom: 12,
  },
  body: {
    fontSize: 15,
    color: '#4B5563',
    lineHeight: 22,
    marginBottom: 16,
  },
  howToReadLabel: {
    fontSize: 11,
    fontWeight: '600',
    color: '#9CA3AF',
    letterSpacing: 0.5,
    marginBottom: 6,
  },
  howToReadText: {
    fontSize: 14,
    color: '#6B7280',
    lineHeight: 20,
    marginBottom: 24,
  },
  gotItButton: {
    backgroundColor: '#111827',
    paddingVertical: 14,
    borderRadius: 10,
    alignItems: 'center',
  },
  gotItText: {
    color: '#FFFFFF',
    fontSize: 16,
    fontWeight: '600',
  },
});

// English-only tooltip content
export const TOOLTIP_CONTENT = {
  rrr: {
    title: "Risk/Reward Ratio",
    body: "Upside potential divided by downside risk. Higher = better risk-adjusted returns.",
    howToRead: "Above 1.0 = upside exceeds downside. Below 1.0 = downside risk is larger."
  },
  marketCap: {
    title: "Market Cap",
    body: "Total value of all shares at today's price. Indicates company size.",
    howToRead: "Higher = larger company. No inherent good or bad."
  },
  sharesOutstanding: {
    title: "Shares Outstanding",
    body: "Total number of shares issued. Used to calculate per-share metrics.",
    howToRead: "No direction. Just shows how many pieces the company is divided into."
  },
  netMargin: {
    title: "Net Margin",
    body: "Profit left after all expenses, as % of revenue. Trailing twelve months.",
    howToRead: "Higher = more profit per dollar of sales. Negative = losing money."
  },
  fcfYield: {
    title: "Free Cash Flow Yield",
    body: "Cash generated after bills and investments, as % of market value.",
    howToRead: "Higher = more cash relative to company value. Negative = burning cash."
  },
  netDebtEbitda: {
    title: "Net Debt / EBITDA",
    body: "Years of operating profit needed to pay off net debt. Measures leverage.",
    howToRead: "Lower = less debt burden. Negative = net cash (no debt)."
  },
  revenueGrowth: {
    title: "Revenue Growth",
    body: "Average annual growth rate over past 3 years. Shows if company is expanding.",
    howToRead: "Positive = growing. Negative = shrinking. Higher does not equal better investment."
  },
  dividendYield: {
    title: "Dividend Yield",
    body: "Annual dividend per share as % of stock price. Only for dividend-paying companies.",
    howToRead: "Higher = more cash to shareholders. Zero = no dividend."
  },
  wealthGap: {
    title: "Performance vs Index",
    body: "If you invested $100 in this stock vs $100 in S&P 500, how much more or less would you have today?",
    howToRead: "Positive = you would have more. Negative = you would have less. -100% = stock went to zero."
  }
};

export default MetricTooltip;
