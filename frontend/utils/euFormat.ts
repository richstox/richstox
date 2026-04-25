/**
 * EU/CZ Number Formatting Utility
 * ================================
 * Formats numbers according to European (Czech) conventions:
 * - Thousands separator: space
 * - Decimal separator: , (comma)
 * 
 * Examples:
 * - 214517 → "214 517"
 * - 2772.3 → "2 772,3"
 * - 435.62 → "435,62"
 */

/**
 * Convert a number to EU format string
 * @param value - The number to format
 * @param decimals - Number of decimal places (default: 2)
 */
export const toEU = (value: number, decimals: number = 2): string => {
  if (value === null || value === undefined || isNaN(value)) return 'N/A';
  
  // Format with fixed decimals first
  const fixed = value.toFixed(decimals);
  
  // Split into integer and decimal parts
  const [intPart, decPart] = fixed.split('.');
  
  // Add thousands separators (spaces) to integer part
  const intWithSpaces = intPart.replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
  
  // Join with comma as decimal separator
  return decPart ? `${intWithSpaces},${decPart}` : intWithSpaces;
};

/**
 * Format currency in EU style
 * @param value - The number to format
 * @param symbol - Currency symbol (default: $)
 */
export const formatCurrencyEU = (value: number | null | undefined, symbol: string = '$'): string => {
  if (value === null || value === undefined || isNaN(value)) return 'N/A';
  
  const absValue = Math.abs(value);
  const sign = value < 0 ? '-' : '';
  
  if (absValue >= 1e12) {
    return `${sign}${symbol}${toEU(absValue / 1e12, 2)}T`;
  }
  if (absValue >= 1e9) {
    return `${sign}${symbol}${toEU(absValue / 1e9, 2)}B`;
  }
  if (absValue >= 1e6) {
    return `${sign}${symbol}${toEU(absValue / 1e6, 2)}M`;
  }
  if (absValue >= 1e3) {
    return `${sign}${symbol}${toEU(absValue, 0)}`;
  }
  return `${sign}${symbol}${toEU(absValue, 2)}`;
};

/**
 * Format large numbers with K/M suffix in EU style
 * @param value - The number to format
 */
export const formatLargeNumberEU = (value: number | null | undefined): string => {
  if (value === null || value === undefined || isNaN(value)) return 'N/A';
  
  const absValue = Math.abs(value);
  const sign = value < 0 ? '-' : '';
  
  if (absValue >= 1e6) {
    return `${sign}${toEU(absValue / 1e6, 1)}M`;
  }
  if (absValue >= 1e3) {
    return `${sign}${toEU(absValue / 1e3, 1)}K`;
  }
  return `${sign}${toEU(absValue, 0)}`;
};

/**
 * Format percentage in EU style
 * @param value - The percentage value (e.g., 5.25 for 5.25%)
 * @param decimals - Number of decimal places (default: 2)
 * @param showSign - Whether to show + for positive values (default: false)
 */
export const formatPercentEU = (
  value: number | null | undefined, 
  decimals: number = 2,
  showSign: boolean = false
): string => {
  if (value === null || value === undefined || isNaN(value)) return 'N/A';
  
  const sign = value > 0 && showSign ? '+' : '';
  return `${sign}${toEU(value, decimals)} %`;
};

/**
 * Format a simple decimal number in EU style
 * @param value - The number to format
 * @param decimals - Number of decimal places (default: 2)
 */
export const formatDecimalEU = (
  value: number | null | undefined, 
  decimals: number = 2
): string => {
  if (value === null || value === undefined || isNaN(value)) return 'N/A';
  return toEU(value, decimals);
};

/**
 * Format price for charts (compact) in EU style
 * @param value - The price value
 */
export const formatPriceCompactEU = (value: number): string => {
  if (value >= 1000) {
    return `$${toEU(value / 1000, 1)}k`;
  }
  return `$${toEU(value, 0)}`;
};

/**
 * P22: Global date formatter - DD/MM/YYYY format
 * @param dateStr - Date string in ISO format (YYYY-MM-DD)
 * @returns Formatted date string (e.g., "23/02/2026")
 * 
 * Examples:
 * - "2026-02-23" → "23/02/2026"
 * - "1999-01-04" → "04/01/1999"
 */
export const formatDateDMY = (dateStr: string | null | undefined): string => {
  if (!dateStr) return 'N/A';
  const d = new Date(dateStr + 'T00:00:00Z');
  if (isNaN(d.getTime())) return 'N/A';
  const dd = String(d.getUTCDate()).padStart(2, '0');
  const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
  const yyyy = d.getUTCFullYear();
  return `${dd}/${mm}/${yyyy}`;
};
