"""Tests for IBKR FX commission model."""

import pytest
from fx_sr.commission import compute_round_turn_commission, commission_as_pips
from fx_sr.walkforward import finalize_trade
from fx_sr.strategy import Trade
import pandas as pd


# ---------------------------------------------------------------------------
# Price lookup helper
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
        'USDCAD': 1.36,
        'EURJPY': 165.0,
    }
    return prices.get(pair_id)


# ---------------------------------------------------------------------------
# Round-turn commission
# ---------------------------------------------------------------------------

class TestRoundTurnCommission:
    def test_small_position_hits_minimum(self):
        """A 15,000 unit EURUSD trade has notional ~$16,500 which is below
        the bps threshold, so the $2 minimum applies per side."""
        cost = compute_round_turn_commission(
            units=15_000,
            entry_price=1.10,
            pair='EURUSD',
            account_currency='GBP',
            price_lookup=_simple_lookup,
        )
        assert cost is not None
        # Per-side: max(15000*1.10*0.00002, 2.00) = max(0.33, 2.00) = $2.00
        # Round-turn: $4.00 USD -> GBP via GBPUSD=1.30 -> 4.00/1.30 ≈ 3.077
        expected_gbp = 4.00 / 1.30
        assert cost == pytest.approx(expected_gbp, rel=1e-3)

    def test_large_position_uses_bps_rate(self):
        """A 500,000 unit EURUSD trade uses the bps rate, not the minimum."""
        cost = compute_round_turn_commission(
            units=500_000,
            entry_price=1.10,
            pair='EURUSD',
            account_currency='GBP',
            price_lookup=_simple_lookup,
        )
        assert cost is not None
        # Notional: 500,000 * 1.10 = $550,000 USD (quote is already USD)
        # Per-side: max(550000 * 0.00002, 2.00) = max(11.0, 2.0) = $11.0
        # Round-turn: $22.0 -> GBP: 22.0 / 1.30 ≈ 16.923
        expected_gbp = 22.0 / 1.30
        assert cost == pytest.approx(expected_gbp, rel=1e-3)

    def test_jpy_pair_commission(self):
        """USDJPY: quote is JPY, needs JPY->USD conversion."""
        cost = compute_round_turn_commission(
            units=10_000,
            entry_price=150.0,
            pair='USDJPY',
            account_currency='GBP',
            price_lookup=_simple_lookup,
        )
        assert cost is not None
        # Notional in JPY: 10,000 * 150 = 1,500,000 JPY
        # JPY -> USD: 1,500,000 / 150 = $10,000 USD
        # Per-side: max(10000 * 0.00002, 2.00) = max(0.20, 2.00) = $2.00
        # Round-turn: $4.00 -> GBP: 4.00 / 1.30 ≈ 3.077
        expected_gbp = 4.00 / 1.30
        assert cost == pytest.approx(expected_gbp, rel=1e-3)

    def test_custom_bps_and_minimum(self):
        """Override commission_bps and commission_min_usd."""
        cost = compute_round_turn_commission(
            units=500_000,
            entry_price=1.10,
            pair='EURUSD',
            account_currency='GBP',
            price_lookup=_simple_lookup,
            commission_bps=0.50,
            commission_min_usd=5.00,
        )
        assert cost is not None
        # Notional: 500,000 * 1.10 = $550,000
        # Per-side: max(550000 * 0.00005, 5.00) = max(27.50, 5.00) = $27.50
        # Round-turn: $55.00 -> GBP: 55.0 / 1.30 ≈ 42.308
        expected_gbp = 55.0 / 1.30
        assert cost == pytest.approx(expected_gbp, rel=1e-3)

    def test_zero_commission_bps(self):
        """With 0 bps, the minimum still applies."""
        cost = compute_round_turn_commission(
            units=15_000,
            entry_price=1.10,
            pair='EURUSD',
            account_currency='GBP',
            price_lookup=_simple_lookup,
            commission_bps=0.0,
            commission_min_usd=2.00,
        )
        assert cost is not None
        # Per-side: max(0, 2.00) = $2.00, round-turn = $4.00
        expected_gbp = 4.00 / 1.30
        assert cost == pytest.approx(expected_gbp, rel=1e-3)

    def test_zero_units_returns_zero(self):
        cost = compute_round_turn_commission(
            units=0,
            entry_price=1.10,
            pair='EURUSD',
            account_currency='GBP',
            price_lookup=_simple_lookup,
        )
        assert cost == 0.0

    def test_usd_account_no_conversion(self):
        """When account currency is USD, no final conversion needed."""
        cost = compute_round_turn_commission(
            units=15_000,
            entry_price=1.10,
            pair='EURUSD',
            account_currency='USD',
            price_lookup=_simple_lookup,
        )
        assert cost is not None
        # Round-turn: $4.00 USD, account is USD, no conversion
        assert cost == pytest.approx(4.00, rel=1e-3)


# ---------------------------------------------------------------------------
# Commission as pips
# ---------------------------------------------------------------------------

class TestCommissionAsPips:
    def test_commission_pips_eurusd(self):
        """Convert a GBP commission to pip-equivalent for EURUSD."""
        # 3.077 GBP commission, 15000 units, pip=0.0001, quote=USD, account=GBP
        commission_gbp = 4.00 / 1.30
        pips = commission_as_pips(
            commission_account=commission_gbp,
            units=15_000,
            pip=0.0001,
            account_currency='GBP',
            quote_currency='USD',
            price_lookup=_simple_lookup,
        )
        assert pips is not None
        # GBP -> USD: 3.077 * 1.30 = $4.00
        # pips = 4.00 / (15000 * 0.0001) = 4.00 / 1.50 ≈ 2.667 pips
        expected = 4.00 / (15_000 * 0.0001)
        assert pips == pytest.approx(expected, rel=1e-3)

    def test_zero_commission_returns_zero(self):
        pips = commission_as_pips(
            commission_account=0.0,
            units=15_000,
            pip=0.0001,
            account_currency='GBP',
            quote_currency='USD',
            price_lookup=_simple_lookup,
        )
        assert pips == 0.0


# ---------------------------------------------------------------------------
# finalize_trade with commission deduction
# ---------------------------------------------------------------------------

class TestFinalizeTradeCommission:
    def _make_trade(self, direction='LONG', entry=1.10, sl=1.0950, tp=1.1100):
        return Trade(
            entry_time=pd.Timestamp('2025-01-01 10:00', tz='UTC'),
            entry_price=entry,
            direction=direction,
            sl_price=sl,
            tp_price=tp,
            zone_upper=1.1050,
            zone_lower=1.0950,
            zone_strength='moderate',
            risk=abs(entry - sl),
        )

    def test_long_trade_no_commission(self):
        trade = self._make_trade()
        result = finalize_trade(
            trade,
            exit_time=pd.Timestamp('2025-01-02 10:00', tz='UTC'),
            exit_price=1.1050,
            exit_reason='TP',
            bars_held=24,
            pip=0.0001,
        )
        # Raw: (1.1050 - 1.10) / 0.0001 = 50 pips
        assert result.pnl_pips == pytest.approx(50.0, rel=1e-6)
        assert result.commission_cost == 0.0

    def test_long_trade_with_commission(self):
        trade = self._make_trade()
        result = finalize_trade(
            trade,
            exit_time=pd.Timestamp('2025-01-02 10:00', tz='UTC'),
            exit_price=1.1050,
            exit_reason='TP',
            bars_held=24,
            pip=0.0001,
            commission_pips=2.5,
            commission_cost=3.08,
        )
        # Raw: 50 pips - 2.5 commission pips = 47.5
        assert result.pnl_pips == pytest.approx(47.5, rel=1e-6)
        assert result.commission_cost == pytest.approx(3.08, rel=1e-6)
        # pnl_r: (0.0050 - 2.5*0.0001) / 0.005 = (0.005 - 0.00025) / 0.005 = 0.95
        assert result.pnl_r == pytest.approx(0.95, rel=1e-3)

    def test_short_trade_with_commission(self):
        trade = self._make_trade(direction='SHORT', entry=1.10, sl=1.1050, tp=1.0900)
        result = finalize_trade(
            trade,
            exit_time=pd.Timestamp('2025-01-02 10:00', tz='UTC'),
            exit_price=1.0950,
            exit_reason='TP',
            bars_held=24,
            pip=0.0001,
            commission_pips=2.0,
            commission_cost=2.50,
        )
        # Raw: (1.10 - 1.0950) / 0.0001 = 50 pips - 2.0 = 48 pips
        assert result.pnl_pips == pytest.approx(48.0, rel=1e-6)
        # pnl_r: (0.0050 - 2.0*0.0001) / 0.005 = (0.005 - 0.0002) / 0.005 = 0.96
        assert result.pnl_r == pytest.approx(0.96, rel=1e-3)

    def test_losing_trade_commission_makes_loss_worse(self):
        trade = self._make_trade()
        result = finalize_trade(
            trade,
            exit_time=pd.Timestamp('2025-01-02 10:00', tz='UTC'),
            exit_price=1.0980,
            exit_reason='SL',
            bars_held=10,
            pip=0.0001,
            commission_pips=2.0,
        )
        # Raw: (1.0980 - 1.10) / 0.0001 = -20 pips - 2.0 = -22 pips
        assert result.pnl_pips == pytest.approx(-22.0, rel=1e-6)
