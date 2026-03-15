"""
RICHSTOX Calculator Functions
=============================
Pure helper functions for financial calculations.
Extracted from server.py — no DB / API access.

Functions:
- calculate_cagr: Compound Annual Growth Rate
- calculate_max_drawdown: Max drawdown from price history (dict list)
- calculate_pain_details: Full PAIN (max drawdown) with dates
- calculate_volatility: Annualized volatility
- calculate_52w_high_low: 52-week high and low
"""

import math
from datetime import datetime
from typing import List


def calculate_cagr(start_value: float, end_value: float, years: float) -> float:
    """Calculate Compound Annual Growth Rate."""
    if start_value <= 0 or years <= 0:
        return 0
    return ((end_value / start_value) ** (1 / years) - 1) * 100


def calculate_max_drawdown(prices: List[dict]) -> float:
    """Calculate maximum drawdown from price history."""
    if not prices:
        return 0

    peak = prices[0].get("adjusted_close", prices[0].get("close", 0))
    max_dd = 0

    for p in prices:
        close = p.get("adjusted_close", p.get("close", 0))
        if close > peak:
            peak = close
        drawdown = (peak - close) / peak if peak > 0 else 0
        max_dd = max(max_dd, drawdown)

    return max_dd * 100  # Return as percentage


def calculate_pain_details(prices: List[dict]) -> dict:
    """
    P25: Calculate PAIN (max drawdown) with exact dates from full daily series.

    Returns:
        dict with: pain_pct, pain_percentage, pain_peak_date, pain_trough_date,
                   pain_duration_days, pain_recovery_date, is_recovered
    """
    if not prices or len(prices) < 2:
        return {
            "pain_pct": 0,
            "pain_percentage": 0,
            "pain_peak_date": None,
            "pain_trough_date": None,
            "pain_duration_days": 0,
            "pain_recovery_date": None,
            "is_recovered": False
        }

    # Find max drawdown with exact dates
    peak_idx = 0
    peak_val = prices[0].get("adjusted_close", prices[0].get("close", 0))
    peak_date = prices[0].get("date")

    trough_idx = 0
    trough_val = peak_val
    trough_date = peak_date

    max_drawdown = 0
    running_max = peak_val
    running_max_idx = 0
    running_max_date = peak_date

    for i, p in enumerate(prices):
        close = p.get("adjusted_close", p.get("close", 0))

        if close > running_max:
            running_max = close
            running_max_idx = i
            running_max_date = p.get("date")

        drawdown = (running_max - close) / running_max if running_max > 0 else 0

        if drawdown > max_drawdown:
            max_drawdown = drawdown
            peak_idx = running_max_idx
            peak_val = running_max
            peak_date = running_max_date
            trough_idx = i
            trough_val = close
            trough_date = p.get("date")

    # Calculate duration in days
    duration_days = 0
    if peak_date and trough_date:
        try:
            peak_dt = datetime.strptime(peak_date, "%Y-%m-%d")
            trough_dt = datetime.strptime(trough_date, "%Y-%m-%d")
            duration_days = (trough_dt - peak_dt).days
        except:
            pass

    # Find recovery date: first day after trough where adjusted_close >= peak_val
    recovery_date = None
    is_recovered = False

    for i in range(trough_idx + 1, len(prices)):
        close = prices[i].get("adjusted_close", prices[i].get("close", 0))
        if close >= peak_val:
            recovery_date = prices[i].get("date")
            is_recovered = True
            break

    # P26 ADDENDUM: pain_percentage = (trough / peak - 1) * 100 (negative value for UI)
    pain_percentage = 0
    if peak_val > 0:
        pain_percentage = round((trough_val / peak_val - 1) * 100, 2)

    return {
        "pain_pct": round(max_drawdown * 100, 2),  # Internal use (positive)
        "pain_percentage": pain_percentage,  # UI display (negative, e.g., -89.7)
        "pain_peak_date": peak_date,
        "pain_trough_date": trough_date,
        "pain_duration_days": duration_days,
        "pain_recovery_date": recovery_date,
        "is_recovered": is_recovered
    }


def calculate_volatility(prices: List[dict]) -> float:
    """Calculate annualized volatility (standard deviation of daily returns)."""
    if len(prices) < 2:
        return 0

    returns = []
    for i in range(1, len(prices)):
        prev = prices[i-1].get("adjusted_close", prices[i-1].get("close", 1))
        curr = prices[i].get("adjusted_close", prices[i].get("close", 1))
        if prev > 0:
            returns.append((curr - prev) / prev)

    if not returns:
        return 0

    avg = sum(returns) / len(returns)
    variance = sum((r - avg) ** 2 for r in returns) / len(returns)
    daily_vol = math.sqrt(variance)

    # Annualize (252 trading days)
    return daily_vol * math.sqrt(252) * 100


def calculate_52w_high_low(prices: List[dict]) -> tuple:
    """Calculate 52-week high and low."""
    if not prices:
        return 0, 0

    highs = [p.get("high", 0) for p in prices[-252:]]
    lows = [p.get("low", float('inf')) for p in prices[-252:]]

    return max(highs) if highs else 0, min(lows) if lows else 0
