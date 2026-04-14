"""Tests for resumable walk-forward state (Phase 1 of unified engine)."""

import unittest

import pandas as pd

from fx_sr.levels import SRZone
from fx_sr.strategy import StrategyParams
from fx_sr.walkforward import (
    WalkForwardState,
    WalkForwardResult,
    resume_walk_forward,
    run_walk_forward,
)


def _make_hourly_df(n_bars: int, start: str = '2026-01-02 00:00:00') -> pd.DataFrame:
    """Build a synthetic hourly OHLC DataFrame with a clear trend."""
    idx = pd.date_range(start, periods=n_bars, freq='h', tz='UTC')
    rows = []
    price = 1.1000
    for i in range(n_bars):
        # Alternate: up bar then down bar, creating a range-bound market
        if i % 4 < 2:
            o, c = price, price + 0.0010
        else:
            o, c = price + 0.0010, price
        h = max(o, c) + 0.0003
        l = min(o, c) - 0.0003
        rows.append({'Open': o, 'High': h, 'Low': l, 'Close': c, 'Volume': 100})
    return pd.DataFrame(rows, index=idx)


def _support_zone(price: float = 1.0990) -> SRZone:
    return SRZone(
        upper=price + 0.0005,
        lower=price - 0.0005,
        midpoint=price,
        touches=5,
        zone_type='support',
        strength='major',
    )


def _resistance_zone(price: float = 1.1020) -> SRZone:
    return SRZone(
        upper=price + 0.0005,
        lower=price - 0.0005,
        midpoint=price,
        touches=5,
        zone_type='resistance',
        strength='major',
    )


def _basic_zone_provider(_time, _date, _idx):
    return [_support_zone(), _resistance_zone()]


def _basic_params() -> StrategyParams:
    return StrategyParams(
        rr_ratio=1.0,
        sl_buffer_pct=0.10,
        spread_pips=0.0,
        stop_slippage_pips=0.0,
        cooldown_bars=0,
        use_time_filters=False,
        use_pair_direction_filter=False,
        min_entry_candle_body_pct=0.0,
        momentum_lookback=1,
        momentum_threshold=0.0,
        zone_penetration_pct=0.01,
        min_zone_touches=1,
        sl_mode='fixed',
        partial_close_enabled=False,
        trailing_mode='none',
    )


class WalkForwardResumeTests(unittest.TestCase):
    """Verify that split + resume produces the same trades as a single full run."""

    def test_result_has_state(self):
        """run_walk_forward returns a WalkForwardState in the result."""
        df = _make_hourly_df(20)
        result = run_walk_forward(
            df,
            pair='EURUSD',
            params=_basic_params(),
            pip=0.0001,
            zone_provider=_basic_zone_provider,
        )
        self.assertIsNotNone(result.state)
        self.assertEqual(result.state.bars_processed, 20)
        self.assertEqual(result.state.last_bar_time, df.index[-1])

    def test_resume_no_new_bars_is_noop(self):
        """Resuming with no new bars produces identical result."""
        df = _make_hourly_df(20)
        params = _basic_params()
        full_result = run_walk_forward(
            df,
            pair='EURUSD',
            params=params,
            pip=0.0001,
            zone_provider=_basic_zone_provider,
            force_close_end=False,
        )
        self.assertIsNotNone(full_result.state)

        resumed = resume_walk_forward(
            full_result.state,
            df,
            pair='EURUSD',
            params=params,
            pip=0.0001,
            zone_provider=_basic_zone_provider,
            force_close_end=False,
        )
        self.assertEqual(len(resumed.trades), len(full_result.trades))
        self.assertEqual(
            resumed.open_trade is not None,
            full_result.open_trade is not None,
        )

    def test_split_resume_matches_full_run(self):
        """Running first half, then resuming with full df matches single full run."""
        df = _make_hourly_df(100)
        params = _basic_params()
        pip = 0.0001

        # Full run
        full_result = run_walk_forward(
            df,
            pair='EURUSD',
            params=params,
            pip=pip,
            zone_provider=_basic_zone_provider,
            force_close_end=True,
        )

        # Split: first 50 bars, then resume with full df
        first_half = df.iloc[:50]
        first_result = run_walk_forward(
            first_half,
            pair='EURUSD',
            params=params,
            pip=pip,
            zone_provider=_basic_zone_provider,
            force_close_end=False,  # don't close — we're continuing
        )
        self.assertIsNotNone(first_result.state)

        resumed_result = resume_walk_forward(
            first_result.state,
            df,
            pair='EURUSD',
            params=params,
            pip=pip,
            zone_provider=_basic_zone_provider,
            force_close_end=True,
        )

        # Trade count must match
        self.assertEqual(
            len(full_result.trades),
            len(resumed_result.trades),
            f'Full run: {len(full_result.trades)} trades, '
            f'split+resume: {len(resumed_result.trades)} trades',
        )

        # Each trade's key attributes must match
        for i, (ft, rt) in enumerate(zip(full_result.trades, resumed_result.trades)):
            self.assertEqual(ft.entry_time, rt.entry_time, f'trade {i} entry_time')
            self.assertEqual(ft.direction, rt.direction, f'trade {i} direction')
            self.assertAlmostEqual(
                ft.entry_price, rt.entry_price, places=6,
                msg=f'trade {i} entry_price',
            )
            self.assertEqual(ft.exit_reason, rt.exit_reason, f'trade {i} exit_reason')
            self.assertAlmostEqual(
                ft.pnl_r, rt.pnl_r, places=4,
                msg=f'trade {i} pnl_r',
            )

    def test_resume_invalid_checkpoint_raises(self):
        """Resuming with a df that doesn't contain the checkpoint bar raises."""
        df = _make_hourly_df(20)
        params = _basic_params()
        result = run_walk_forward(
            df,
            pair='EURUSD',
            params=params,
            pip=0.0001,
            zone_provider=_basic_zone_provider,
            force_close_end=False,
        )

        # Different df that doesn't contain the checkpoint timestamp
        other_df = _make_hourly_df(20, start='2025-06-01 00:00:00')
        with self.assertRaises(ValueError):
            resume_walk_forward(
                result.state,
                other_df,
                pair='EURUSD',
                params=params,
                pip=0.0001,
                zone_provider=_basic_zone_provider,
            )

    def test_incremental_bar_by_bar_matches_full(self):
        """Processing bars one at a time via resume produces same result as full run."""
        df = _make_hourly_df(30)
        params = _basic_params()
        pip = 0.0001

        # Full run (no force close so we can compare open trade state)
        full_result = run_walk_forward(
            df,
            pair='EURUSD',
            params=params,
            pip=pip,
            zone_provider=_basic_zone_provider,
            force_close_end=True,
        )

        # Incremental: start with first 5 bars, then add one at a time
        state = None
        for end in range(5, len(df)):
            partial_df = df.iloc[:end]
            if state is None:
                result = run_walk_forward(
                    partial_df,
                    pair='EURUSD',
                    params=params,
                    pip=pip,
                    zone_provider=_basic_zone_provider,
                    force_close_end=False,
                )
            else:
                result = resume_walk_forward(
                    state,
                    partial_df,
                    pair='EURUSD',
                    params=params,
                    pip=pip,
                    zone_provider=_basic_zone_provider,
                    force_close_end=False,
                )
            state = result.state

        # Final step: include all bars with force_close
        final = resume_walk_forward(
            state,
            df,
            pair='EURUSD',
            params=params,
            pip=pip,
            zone_provider=_basic_zone_provider,
            force_close_end=True,
        )

        self.assertEqual(
            len(full_result.trades),
            len(final.trades),
            f'Full: {len(full_result.trades)}, incremental: {len(final.trades)}',
        )
        for i, (ft, rt) in enumerate(zip(full_result.trades, final.trades)):
            self.assertEqual(ft.entry_time, rt.entry_time, f'trade {i} entry_time')
            self.assertEqual(ft.direction, rt.direction, f'trade {i} direction')
            self.assertEqual(ft.exit_reason, rt.exit_reason, f'trade {i} exit_reason')


if __name__ == '__main__':
    unittest.main()
