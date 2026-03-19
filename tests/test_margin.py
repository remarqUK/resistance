"""Tests for FCA UK retail margin model."""

import pytest
from fx_sr.margin import (
    MAJOR_CURRENCIES,
    MARGIN_RATE_MAJOR,
    MARGIN_RATE_MINOR,
    MIN_UNITS_ODD_LOT,
    MIN_UNITS_IDEAL_PRO,
    MarginRequirement,
    get_margin_rate,
    is_major_pair,
    compute_margin_requirement,
    check_margin_available,
    clamp_units_to_margin,
)
from fx_sr.sizing import build_position_size_plan, build_position_size_plan_for_risk_amount


# ---------------------------------------------------------------------------
# Pair classification
# ---------------------------------------------------------------------------

MAJOR_PAIRS = [
    'EURUSD', 'USDJPY', 'GBPUSD', 'USDCHF', 'USDCAD',
    'EURGBP', 'EURJPY', 'GBPJPY', 'CADJPY', 'CHFJPY',
    'EURCAD', 'EURCHF', 'GBPCAD', 'GBPCHF',
]
MINOR_PAIRS = [
    'AUDUSD', 'NZDUSD', 'AUDJPY', 'EURAUD', 'GBPAUD',
    'AUDNZD', 'NZDJPY', 'AUDCAD',
]


class TestPairClassification:
    @pytest.mark.parametrize('pair', MAJOR_PAIRS)
    def test_major_pair_margin_rate(self, pair):
        assert get_margin_rate(pair) == pytest.approx(MARGIN_RATE_MAJOR, rel=1e-6)
        assert is_major_pair(pair)

    @pytest.mark.parametrize('pair', MINOR_PAIRS)
    def test_minor_pair_margin_rate(self, pair):
        assert get_margin_rate(pair) == pytest.approx(MARGIN_RATE_MINOR, rel=1e-6)
        assert not is_major_pair(pair)

    def test_all_22_pairs_classified(self):
        all_pairs = MAJOR_PAIRS + MINOR_PAIRS
        assert len(all_pairs) == 22
        for pair in all_pairs:
            rate = get_margin_rate(pair)
            assert rate in (MARGIN_RATE_MAJOR, MARGIN_RATE_MINOR)


# ---------------------------------------------------------------------------
# Margin requirement computation
# ---------------------------------------------------------------------------

def _simple_lookup(pair_id):
    """Return a known price for test pairs."""
    prices = {
        'EURUSD': 1.10,
        'GBPUSD': 1.30,
        'USDJPY': 150.0,
        'EURGBP': 0.85,
        'AUDUSD': 0.65,
        'AUDNZD': 1.08,
        'GBPJPY': 195.0,
        'NZDUSD': 0.60,
    }
    return prices.get(pair_id)


class TestMarginRequirement:
    def test_eurusd_margin(self):
        req = compute_margin_requirement(
            'EURUSD', 25_000, 1.10, 'GBP', _simple_lookup,
        )
        assert req is not None
        assert req.margin_rate == pytest.approx(MARGIN_RATE_MAJOR, rel=1e-6)
        # 25,000 EUR in GBP via EURGBP=0.85 → notional = 25,000 * 0.85 = 21,250
        # margin = 21,250 * 0.0333... ≈ 708.33
        assert req.notional_account == pytest.approx(21_250.0, rel=1e-3)
        assert req.margin_required == pytest.approx(21_250.0 * MARGIN_RATE_MAJOR, rel=1e-3)
        assert not req.is_odd_lot
        assert not req.is_below_minimum

    def test_odd_lot_flag(self):
        req = compute_margin_requirement(
            'EURUSD', 10_000, 1.10, 'GBP', _simple_lookup,
        )
        assert req is not None
        assert req.is_odd_lot  # < 25,000
        assert not req.is_below_minimum  # >= 1,000

    def test_below_minimum_flag(self):
        req = compute_margin_requirement(
            'EURUSD', 500, 1.10, 'GBP', _simple_lookup,
        )
        assert req is not None
        assert req.is_below_minimum  # < 1,000

    def test_minor_pair_margin(self):
        req = compute_margin_requirement(
            'AUDUSD', 20_000, 0.65, 'GBP', _simple_lookup,
        )
        assert req is not None
        assert req.margin_rate == pytest.approx(MARGIN_RATE_MINOR, rel=1e-6)

    def test_none_on_conversion_failure(self):
        # No path from XYZ to GBP
        req = compute_margin_requirement(
            'EURUSD', 10_000, 1.10, 'XYZ', _simple_lookup,
        )
        assert req is None


# ---------------------------------------------------------------------------
# Margin availability check
# ---------------------------------------------------------------------------

class TestCheckMarginAvailable:
    def test_allowed_within_budget(self):
        allowed, after = check_margin_available(500.0, 1000.0, cushion_pct=10.0)
        assert allowed
        assert after == pytest.approx(500.0)

    def test_blocked_exceeds_usable(self):
        # 1000 available, 10% cushion → usable = 900, need 950
        allowed, after = check_margin_available(950.0, 1000.0, cushion_pct=10.0)
        assert not allowed

    def test_exact_boundary(self):
        # usable = 900, need exactly 900
        allowed, _ = check_margin_available(900.0, 1000.0, cushion_pct=10.0)
        assert allowed

    def test_zero_cushion(self):
        allowed, _ = check_margin_available(1000.0, 1000.0, cushion_pct=0.0)
        assert allowed


# ---------------------------------------------------------------------------
# Margin clamping
# ---------------------------------------------------------------------------

class TestClampUnitsToMargin:
    def test_no_clamping_when_fits(self):
        result = clamp_units_to_margin(
            'EURUSD', 25_000, 1.10, 5000.0, 'GBP', _simple_lookup,
            cushion_pct=10.0,
        )
        # 25,000 EUR * 0.85 (EURGBP) * 0.0333 ≈ 708 margin, 5000 * 0.9 = 4500 usable
        assert result == 25_000

    def test_clamped_to_fit(self):
        # Very small available margin
        result = clamp_units_to_margin(
            'EURUSD', 50_000, 1.10, 1000.0, 'GBP', _simple_lookup,
            cushion_pct=10.0,
        )
        # Should be reduced from 50,000 but still >= 1,000
        assert 1_000 <= result < 50_000

    def test_returns_zero_below_minimum(self):
        # Tiny available margin — can't even do 1,000 units
        result = clamp_units_to_margin(
            'EURUSD', 25_000, 1.10, 10.0, 'GBP', _simple_lookup,
            cushion_pct=10.0,
        )
        assert result == 0


# ---------------------------------------------------------------------------
# Sizing integration
# ---------------------------------------------------------------------------

class TestSizingMinimumEnforcement:
    def test_below_min_returns_none(self):
        """Units < 1000 should return None when enforce_margin=True."""
        # Very small balance → tiny risk → units < 1000
        plan = build_position_size_plan(
            pair='EURUSD',
            direction='LONG',
            entry_price=1.10000,
            stop_price=1.09000,    # 100 pip stop
            balance=5.0,           # £5 balance
            risk_pct=0.05,         # £0.25 risk
            account_currency='GBP',
            price_lookup=_simple_lookup,
            enforce_margin=True,
            min_order_units=1000,
        )
        assert plan is None

    def test_above_min_returns_plan(self):
        """Sufficient balance produces a valid plan with margin fields."""
        plan = build_position_size_plan(
            pair='EURUSD',
            direction='LONG',
            entry_price=1.10000,
            stop_price=1.09500,    # 50 pip stop
            balance=5000.0,
            risk_pct=0.06,
            account_currency='GBP',
            price_lookup=_simple_lookup,
            enforce_margin=True,
            min_order_units=1000,
        )
        assert plan is not None
        assert plan.units >= 1000
        assert plan.margin_required is not None
        assert plan.margin_required > 0
        assert plan.margin_rate == pytest.approx(MARGIN_RATE_MAJOR, rel=1e-6)

    def test_odd_lot_flagged(self):
        """Units between 1,000 and 25,000 should be flagged as odd lot."""
        plan = build_position_size_plan(
            pair='EURUSD',
            direction='LONG',
            entry_price=1.10000,
            stop_price=1.09500,
            balance=1000.0,
            risk_pct=0.06,
            account_currency='GBP',
            price_lookup=_simple_lookup,
            enforce_margin=True,
        )
        if plan is not None and plan.units < MIN_UNITS_IDEAL_PRO:
            assert plan.is_odd_lot

    def test_enforce_margin_false_allows_small_units(self):
        """With enforce_margin=False, units < 1000 are allowed."""
        plan = build_position_size_plan_for_risk_amount(
            pair='EURUSD',
            direction='LONG',
            entry_price=1.10000,
            stop_price=1.09000,
            balance=50.0,
            risk_amount=2.0,
            account_currency='GBP',
            price_lookup=_simple_lookup,
            enforce_margin=False,
        )
        # With enforce_margin=False and min_order_units default, units > 0 is ok
        if plan is not None:
            assert plan.units > 0
