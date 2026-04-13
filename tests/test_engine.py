"""Tests for the unified PairEngine (Phase 5)."""

import unittest

import pandas as pd

from fx_sr.engine import PairEngine
from fx_sr.levels import SRZone
from fx_sr.strategy import StrategyParams


def _make_hourly_df(n_bars: int, start: str = '2026-01-02 00:00:00') -> pd.DataFrame:
    idx = pd.date_range(start, periods=n_bars, freq='h', tz='UTC')
    rows = []
    price = 1.1000
    for i in range(n_bars):
        if i % 4 < 2:
            o, c = price, price + 0.0010
        else:
            o, c = price + 0.0010, price
        h = max(o, c) + 0.0003
        l = min(o, c) - 0.0003
        rows.append({'Open': o, 'High': h, 'Low': l, 'Close': c, 'Volume': 100})
    return pd.DataFrame(rows, index=idx)


def _make_daily_df(n_days: int = 40, start: str = '2025-10-01') -> pd.DataFrame:
    idx = pd.date_range(start, periods=n_days, freq='D', tz='UTC')
    return pd.DataFrame(
        {
            'Open': [1.1000] * n_days,
            'High': [1.1100] * n_days,
            'Low': [1.0900] * n_days,
            'Close': [1.1000] * n_days,
            'Volume': [0.0] * n_days,
        },
        index=idx,
    )


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


PAIR_INFO = {'ticker': 'EURUSD=X', 'name': 'EUR/USD', 'decimals': 5, 'pip': 0.0001}


class PairEngineTests(unittest.TestCase):

    def _engine(self, hourly_df, daily_df=None):
        """Create an engine with pre-loaded data (bypasses load_data)."""
        from fx_sr.data_pipeline import PairDataBundle
        from fx_sr.walkforward import make_zone_provider, make_execution_quote_provider

        engine = PairEngine(
            'EURUSD', PAIR_INFO, _basic_params(),
            zone_history_days=180,
            hourly_days=365,
            snapshot_source='test',
        )
        if daily_df is None:
            daily_df = _make_daily_df()
        engine._data = PairDataBundle(
            pair='EURUSD',
            ticker='EURUSD=X',
            daily_df=daily_df,
            hourly_df=hourly_df,
            minute_df=pd.DataFrame(),
            l2_snapshots=pd.DataFrame(),
            pip=0.0001,
        )
        engine._zone_provider = make_zone_provider(daily_df, 180, cache={})
        engine._quote_provider = make_execution_quote_provider(
            _basic_params(), pd.DataFrame(), pd.DataFrame(),
        )
        return engine

    def test_backtest_lifecycle(self):
        """load → run_historical(force_close_end=True) → trades produced."""
        df = _make_hourly_df(50)
        engine = self._engine(df)

        result = engine.run_historical(force_close_end=True)
        self.assertIsNotNone(result)
        self.assertIsNone(result.open_trade)  # force-closed
        self.assertIsNotNone(engine.state)

    def test_live_lifecycle_incremental(self):
        """run_historical → process_new_bars produces same result as full run."""
        full_df = _make_hourly_df(60)
        first_df = full_df.iloc[:40]

        # Full run for reference
        full_engine = self._engine(full_df)
        full_result = full_engine.run_historical(force_close_end=True)

        # Incremental: historical on first 40, then extend to 60
        inc_engine = self._engine(first_df)
        inc_engine.run_historical(force_close_end=False)
        self.assertIsNotNone(inc_engine.state)

        # Process remaining bars
        result = inc_engine.process_new_bars(full_df)
        final = inc_engine.finalize()

        self.assertEqual(len(full_result.trades), len(final.trades))

    def test_finalize_force_closes_open_trade(self):
        """finalize() closes any open trade from run_historical."""
        df = _make_hourly_df(30)
        engine = self._engine(df)
        result = engine.run_historical(force_close_end=False)

        finalized = engine.finalize()
        self.assertIsNone(finalized.open_trade)
        # If there was an open trade, finalize adds an END trade
        if result.open_trade is not None:
            self.assertTrue(
                any(t.exit_reason == 'END' for t in finalized.trades),
            )

    def test_signal_from_open_trade(self):
        """signal_from_open_trade builds a Signal when trade is open."""
        df = _make_hourly_df(30)
        engine = self._engine(df)
        engine.run_historical(force_close_end=False)

        signal = engine.signal_from_open_trade()
        if engine.last_result.open_trade is not None:
            self.assertIsNotNone(signal)
            self.assertEqual(signal.pair, 'EURUSD')
        else:
            self.assertIsNone(signal)

    def test_process_new_bars_requires_run_historical(self):
        """process_new_bars raises if called before run_historical."""
        engine = self._engine(_make_hourly_df(20))
        with self.assertRaises(RuntimeError):
            engine.process_new_bars(_make_hourly_df(25))


if __name__ == '__main__':
    unittest.main()
