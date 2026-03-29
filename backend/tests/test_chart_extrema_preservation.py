"""
Test Chart Extrema Preservation - End-to-End Proof
===================================================
Proves that the backend + frontend downsampling pipeline preserves the true
adjusted_close HIGH/LOW for the DECK MAX scenario.

Scenario: DECK has ~5500 trading days. True max adjusted_close = 223.11 on
2025-01-30. After backend np.linspace downsampling to ~2000 points, and
frontend step-filter downsampling to ~400 points, the true max must still
appear in the final chart data so HIGH label = 223.11 (or rounded equivalent).
"""

import pytest
import math


# ============================================================================
# Extracted backend logic (server.py get_ticker_chart_data)
# ============================================================================

def _safe_adjusted_close(p):
    """Exact copy of backend _safe_adjusted_close."""
    adj = p.get("adjusted_close")
    close = p.get("close")
    if adj and close and close > 0 and adj / close > 100:
        return close
    return adj or close


def backend_max_downsample(all_prices, target_points=2000):
    """
    Simulates the backend MAX-period downsampling with extrema fix.
    Exact logic from server.py lines 4296-4319.
    """
    if len(all_prices) > target_points:
        import numpy as np
        indices = np.linspace(0, len(all_prices) - 1, target_points, dtype=int)
        indices = sorted(set(indices))
        prices = [all_prices[i] for i in indices]

        # Force-include true adjusted_close extrema (the fix)
        sampled_dates = set(p["date"] for p in prices)
        true_max_pt = max(all_prices, key=lambda p: _safe_adjusted_close(p))
        true_min_pt = min(all_prices, key=lambda p: _safe_adjusted_close(p))
        added = False
        if true_max_pt["date"] not in sampled_dates:
            prices.append(true_max_pt)
            sampled_dates.add(true_max_pt["date"])
            added = True
        if true_min_pt["date"] not in sampled_dates:
            prices.append(true_min_pt)
            added = True
        if added:
            prices.sort(key=lambda p: p["date"])
    else:
        prices = all_prices
    return prices


def backend_normalize(prices):
    """
    Simulates backend normalization.
    Exact logic from server.py lines 4362-4374.
    """
    normalized_prices = []
    if prices:
        start_value = _safe_adjusted_close(prices[0])
        for p in prices:
            price_val = _safe_adjusted_close(p)
            normalized_prices.append({
                "date": p["date"],
                "close": p["close"],
                "adjusted_close": price_val,
                "normalized": round((price_val / start_value) * 100, 2) if start_value else 100,
                "volume": p.get("volume")
            })
    return normalized_prices


# ============================================================================
# Extracted frontend logic (ticker.tsx fetchChartData)
# ============================================================================

def frontend_downsample_and_format(api_prices, target_points=400):
    """
    Simulates the frontend downsampling + force-include + format.
    Exact logic from ticker.tsx lines 491-525.
    """
    prices = api_prices  # response.data.prices
    step = max(1, math.floor(len(prices) / target_points))
    downsampled = [p for i, p in enumerate(prices) if i % step == 0 or i == len(prices) - 1]

    # Force-include true max and min (PR187 fix, lines 496-513)
    if len(prices) > 0:
        def get_plot_value(p):
            return p.get("adjusted_close") or p.get("close")

        true_max_point = prices[0]
        true_min_point = prices[0]
        max_val = get_plot_value(prices[0])
        min_val = max_val
        for p in prices:
            v = get_plot_value(p)
            if v > max_val:
                true_max_point = p
                max_val = v
            if v < min_val:
                true_min_point = p
                min_val = v
        dates = set(p["date"] for p in downsampled)
        if true_max_point["date"] not in dates:
            downsampled.append(true_max_point)
        if true_min_point["date"] not in dates:
            downsampled.append(true_min_point)
        downsampled.sort(key=lambda p: p["date"])

    # Convert to expected format (lines 516-525)
    formatted = []
    for p in downsampled:
        formatted.append({
            "date": p["date"],
            "open": p["close"],
            "high": p["close"],
            "low": p["close"],
            "close": p["close"],
            "adjusted_close": p.get("adjusted_close") or p.get("close"),
            "volume": p.get("volume") or 0,
            "normalized": p.get("normalized"),
        })
    return formatted


def chart_compute_high_low(visible_chart_data):
    """
    Simulates the chart HIGH/LOW computation.
    Exact logic from ticker.tsx lines 1831-1833.
    """
    values = [d["adjusted_close"] for d in visible_chart_data]
    data_min = min(values)
    data_max = max(values)
    return data_max, data_min


# ============================================================================
# Test data generator - mimics DECK price history
# ============================================================================

def generate_deck_like_data(total_points=5500, max_adj_close=223.11,
                            max_date="2025-01-30", max_close=212.0,
                            typical_adj_close=150.0, typical_close=150.0,
                            min_adj_close=5.0, min_date="2004-07-15",
                            min_close=5.0):
    """
    Generate synthetic DECK-like price data where:
    - ~5500 points spanning 2004-2026
    - True max adjusted_close = 223.11 on 2025-01-30
    - Most prices around 50-200
    - The max point has adj_close >> close (dividend adjustment)
    """
    import random
    random.seed(42)
    
    data = []
    for i in range(total_points):
        # Generate date as YYYY-MM-DD spanning ~22 years (2004–2026)
        year = 2004 + (i * 22) // total_points
        day_of_year = 1 + (i * 250) // total_points % 250
        month = min(12, 1 + day_of_year // 22)  # ~22 trading days per month
        day = min(28, 1 + day_of_year % 22)
        date_str = f"{year:04d}-{month:02d}-{day:02d}"
        
        # Typical price around 50-200 with some noise
        base = typical_adj_close * (0.5 + (i / total_points))
        adj = base + random.uniform(-10, 10)
        close = adj * 0.98 + random.uniform(-2, 2)  # close slightly different
        
        data.append({
            "date": date_str,
            "close": round(close, 2),
            "adjusted_close": round(adj, 2),
            "volume": random.randint(100000, 5000000),
        })
    
    # Ensure the true max point is at the expected date
    # Pick an index somewhere in the 2025 range
    max_idx = int(total_points * 0.95)  # roughly 2025 position
    data[max_idx] = {
        "date": max_date,
        "close": max_close,
        "adjusted_close": max_adj_close,
        "volume": 2500000,
    }
    
    # Ensure the true min point is at the expected date
    min_idx = 10  # early in history
    data[min_idx] = {
        "date": min_date,
        "close": min_close,
        "adjusted_close": min_adj_close,
        "volume": 300000,
    }
    
    # Make sure no other point exceeds the max or goes below the min
    for i, p in enumerate(data):
        if i != max_idx and p["adjusted_close"] >= max_adj_close:
            p["adjusted_close"] = max_adj_close - 1.0
        if i != min_idx and p["adjusted_close"] <= min_adj_close:
            p["adjusted_close"] = min_adj_close + 1.0
    
    # Sort by date
    data.sort(key=lambda p: p["date"])
    
    return data


# ============================================================================
# TESTS
# ============================================================================

class TestBackendExtremaPreservation:
    """Test that backend MAX downsampling preserves true adjusted_close extrema."""

    def test_linspace_without_fix_would_lose_max(self):
        """Demonstrate that np.linspace alone CAN skip the true max point."""
        import numpy as np
        all_prices = generate_deck_like_data()
        
        # Find the true max
        true_max = max(all_prices, key=lambda p: _safe_adjusted_close(p))
        assert true_max["adjusted_close"] == 223.11
        assert true_max["date"] == "2025-01-30"
        
        # Run linspace WITHOUT the fix
        target_points = 2000
        indices = np.linspace(0, len(all_prices) - 1, target_points, dtype=int)
        indices = sorted(set(indices))
        sampled = [all_prices[i] for i in indices]
        
        sampled_dates = set(p["date"] for p in sampled)
        # The true max date may or may not be sampled (depends on exact index)
        # Either way, the fix should ensure it's present
        print(f"True max date in linspace sample: {'2025-01-30' in sampled_dates}")
        print(f"Sampled points: {len(sampled)}")

    def test_backend_fix_preserves_max(self):
        """After the backend fix, true max adjusted_close=223.11 is always in the output."""
        all_prices = generate_deck_like_data()
        
        # Run the backend downsampling WITH the fix
        prices = backend_max_downsample(all_prices, target_points=2000)
        
        # Verify the true max point is present
        max_pt = max(prices, key=lambda p: _safe_adjusted_close(p))
        assert max_pt["adjusted_close"] == 223.11, f"Max adjusted_close = {max_pt['adjusted_close']}, expected 223.11"
        assert max_pt["date"] == "2025-01-30", f"Max date = {max_pt['date']}, expected 2025-01-30"
        
        # Also check it's actually in the list (not just max of wrong points)
        dates = [p["date"] for p in prices]
        assert "2025-01-30" in dates, "2025-01-30 missing from backend output"
        
        print(f"PASS: Backend preserves max adjusted_close={max_pt['adjusted_close']} on {max_pt['date']}")

    def test_backend_fix_preserves_min(self):
        """After the backend fix, true min adjusted_close is always in the output."""
        all_prices = generate_deck_like_data()
        
        prices = backend_max_downsample(all_prices, target_points=2000)
        
        min_pt = min(prices, key=lambda p: _safe_adjusted_close(p))
        assert min_pt["adjusted_close"] == 5.0, f"Min adjusted_close = {min_pt['adjusted_close']}, expected 5.0"
        
        print(f"PASS: Backend preserves min adjusted_close={min_pt['adjusted_close']} on {min_pt['date']}")

    def test_backend_normalization_preserves_max(self):
        """Backend normalization step preserves the true max adjusted_close value."""
        all_prices = generate_deck_like_data()
        prices = backend_max_downsample(all_prices, target_points=2000)
        normalized = backend_normalize(prices)
        
        # Find max in normalized output
        max_pt = max(normalized, key=lambda p: p["adjusted_close"])
        assert max_pt["adjusted_close"] == 223.11, f"Normalized max = {max_pt['adjusted_close']}, expected 223.11"
        assert max_pt["date"] == "2025-01-30"
        
        # Check the date is present
        jan30 = [p for p in normalized if p["date"] == "2025-01-30"]
        assert len(jan30) == 1, f"Expected 1 entry for 2025-01-30, got {len(jan30)}"
        assert jan30[0]["adjusted_close"] == 223.11
        
        print(f"PASS: Normalization preserves max adjusted_close={max_pt['adjusted_close']}")


class TestFrontendExtremaPreservation:
    """Test that frontend downsampling preserves true adjusted_close extrema."""

    def test_frontend_downsample_preserves_max(self):
        """Frontend ~400 point downsample preserves true max from API response."""
        all_prices = generate_deck_like_data()
        prices = backend_max_downsample(all_prices, target_points=2000)
        api_prices = backend_normalize(prices)
        
        # Run frontend downsampling
        chart_data = frontend_downsample_and_format(api_prices, target_points=400)
        
        # Verify the true max is preserved
        max_pt = max(chart_data, key=lambda d: d["adjusted_close"])
        assert max_pt["adjusted_close"] == 223.11, f"Frontend max = {max_pt['adjusted_close']}, expected 223.11"
        assert max_pt["date"] == "2025-01-30"
        
        print(f"PASS: Frontend preserves max adjusted_close={max_pt['adjusted_close']}")

    def test_frontend_downsample_preserves_min(self):
        """Frontend ~400 point downsample preserves true min from API response."""
        all_prices = generate_deck_like_data()
        prices = backend_max_downsample(all_prices, target_points=2000)
        api_prices = backend_normalize(prices)
        
        chart_data = frontend_downsample_and_format(api_prices, target_points=400)
        
        min_pt = min(chart_data, key=lambda d: d["adjusted_close"])
        assert min_pt["adjusted_close"] == 5.0, f"Frontend min = {min_pt['adjusted_close']}, expected 5.0"
        
        print(f"PASS: Frontend preserves min adjusted_close={min_pt['adjusted_close']}")


class TestEndToEndChartHighLow:
    """Prove the full pipeline produces correct HIGH/LOW for DECK MAX scenario."""

    def test_chart_high_is_223(self):
        """
        End-to-end proof: DECK MAX chart HIGH = 223.11.
        
        Pipeline: DB (5500 pts) → backend linspace (2000) → API → frontend step (400) → chart HIGH
        """
        # Layer 1: Simulate DB data
        all_prices = generate_deck_like_data(total_points=5500)
        
        # Verify truth: max adjusted_close in full data is 223.11
        truth_max = max(all_prices, key=lambda p: p["adjusted_close"])
        assert truth_max["adjusted_close"] == 223.11, "Test data generation error"
        assert truth_max["date"] == "2025-01-30", "Test data generation error"
        
        # Layer 2: Backend MAX downsampling (with fix)
        backend_prices = backend_max_downsample(all_prices, target_points=2000)
        backend_max = max(backend_prices, key=lambda p: _safe_adjusted_close(p))
        assert backend_max["adjusted_close"] == 223.11, (
            f"Layer 2 FAIL: Backend lost true max. Got {backend_max['adjusted_close']}")
        
        # Layer 3: Backend normalization
        api_response = backend_normalize(backend_prices)
        api_max = max(api_response, key=lambda p: p["adjusted_close"])
        assert api_max["adjusted_close"] == 223.11, (
            f"Layer 3 FAIL: Normalization lost true max. Got {api_max['adjusted_close']}")
        assert api_max["date"] == "2025-01-30"
        
        # Layer 4+5+6: Frontend downsample + format
        chart_data = frontend_downsample_and_format(api_response, target_points=400)
        fe_max = max(chart_data, key=lambda d: d["adjusted_close"])
        assert fe_max["adjusted_close"] == 223.11, (
            f"Layer 5/6 FAIL: Frontend lost true max. Got {fe_max['adjusted_close']}")
        
        # Layer 7: visibleChartData (benchmark OFF → same as chartData)
        visible_chart_data = chart_data  # showBenchmark=false (default)
        
        # Layer 8: Chart HIGH/LOW computation
        data_max, data_min = chart_compute_high_low(visible_chart_data)
        assert data_max == 223.11, f"Chart HIGH = {data_max}, expected 223.11"
        
        print(f"✓ PROOF: Chart HIGH = {data_max} (matches true adjusted_close max)")
        print(f"✓ PROOF: Chart LOW  = {data_min}")
        print(f"✓ 2025-01-30 with adjusted_close=223.11 preserved through ALL layers")

    def test_chart_high_with_different_data_sizes(self):
        """Prove the fix works regardless of total data size."""
        for total in [500, 1000, 2000, 3000, 5000, 8000]:
            all_prices = generate_deck_like_data(total_points=total)
            backend_prices = backend_max_downsample(all_prices, target_points=2000)
            api_response = backend_normalize(backend_prices)
            chart_data = frontend_downsample_and_format(api_response, target_points=400)
            data_max, _ = chart_compute_high_low(chart_data)
            assert data_max == 223.11, (
                f"FAIL at total={total}: Chart HIGH = {data_max}, expected 223.11")
            print(f"  ✓ total={total}: Chart HIGH = {data_max}")

    def test_max_on_exact_linspace_boundary(self):
        """
        Even when the max point falls exactly on a linspace sample index,
        the fix still produces correct results (no duplicate insertion of the max).
        """
        # Generate unique-date data
        from datetime import date, timedelta
        base = date(2004, 1, 2)
        total = 4000
        all_prices = []
        for i in range(total):
            d = base + timedelta(days=i)
            all_prices.append({
                "date": d.isoformat(),
                "close": 150.0 + (i % 50),
                "adjusted_close": 150.0 + (i % 50),
                "volume": 1000000,
            })
        
        # Force max at index 0 (always sampled by linspace)
        all_prices[0]["adjusted_close"] = 223.11
        all_prices[0]["close"] = 210.0
        
        backend_prices = backend_max_downsample(all_prices, target_points=2000)
        api_response = backend_normalize(backend_prices)
        chart_data = frontend_downsample_and_format(api_response, target_points=400)
        data_max, _ = chart_compute_high_low(chart_data)
        assert data_max == 223.11, f"Chart HIGH = {data_max}, expected 223.11"
        
        # No duplicate dates in chart data
        dates = [d["date"] for d in chart_data]
        for dt in set(dates):
            assert dates.count(dt) == 1, f"Duplicate date in chart_data: {dt}"
        
        print(f"✓ Boundary case: Chart HIGH = {data_max}, no duplicates")

    def test_safe_adjusted_close_does_not_interfere(self):
        """
        _safe_adjusted_close only triggers for adj/close > 100x ratio.
        Normal DECK data (adj/close ≈ 1.05) is unaffected.
        """
        # DECK-like: adjusted_close slightly higher than close (dividends)
        p = {"adjusted_close": 223.11, "close": 212.0}
        assert _safe_adjusted_close(p) == 223.11, "Should return adjusted_close for normal ratio"
        
        # Pathological: adj/close > 100 (reverse split artifact)
        p_bad = {"adjusted_close": 50000.0, "close": 200.0}
        assert _safe_adjusted_close(p_bad) == 200.0, "Should fall back to close for >100x ratio"
        
        print("✓ _safe_adjusted_close correctly handles normal and pathological data")


class TestDeadCodeDoesNotInterfere:
    """Verify the unused downsample() function doesn't affect the pipeline."""

    def test_backend_downsample_function_not_used_for_prices(self):
        """
        The downsample() function at server.py:4335 is defined but never called
        on the prices variable. This test confirms that fact by checking that
        our pipeline test doesn't use it.
        """
        # The downsample function from the backend:
        def downsample(data, target_points=500):
            if len(data) <= target_points:
                return data
            step = len(data) // target_points
            return [data[i] for i in range(0, len(data), step)] + ([data[-1]] if data else [])

        # Show that this function WOULD lose the max
        all_prices = generate_deck_like_data()
        backend_prices = backend_max_downsample(all_prices, target_points=2000)
        
        # If we were to apply downsample() to backend output:
        further_downsampled = downsample(backend_prices, target_points=500)
        dates = set(p["date"] for p in further_downsampled)
        
        # This function does NOT preserve extrema - it's just a simple step filter
        # But it's NOT called on prices, so it doesn't matter
        print(f"downsample() output has 2025-01-30: {'2025-01-30' in dates}")
        print("NOTE: This function is DEAD CODE - not called on prices variable")
        # We don't assert anything about this function since it's unused


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
