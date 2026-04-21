import unittest
from unittest.mock import patch

import pandas as pd

from fx_sr.backtest import (
    BACKTEST_CACHE_VERSION,
    _data_signature,
    _params_signature,
    _serialize_backtest_result,
    _backtest_pair,
    BacktestCacheMissingError,
    build_backtest_run_config_json,
    run_all_backtests_parallel,
)
from fx_sr.backtest import BacktestResult
from fx_sr.strategy import StrategyParams


def _sample_daily_df() -> pd.DataFrame:
    index = pd.date_range('2026-01-01', periods=12, freq='D', tz='UTC')
    return pd.DataFrame(
        {
            'Open': [1.1000] * 12,
            'High': [1.1050] * 12,
            'Low': [1.0950] * 12,
            'Close': [1.1000] * 12,
            'Volume': [1000.0] * 12,
        },
        index=index,
    )


def _sample_hourly_df() -> pd.DataFrame:
    index = pd.date_range('2026-01-01', periods=24, freq='h', tz='UTC')
    return pd.DataFrame(
        {
            'Open': [1.1000] * 24,
            'High': [1.1020] * 24,
            'Low': [1.0980] * 24,
            'Close': [1.1000] * 24,
            'Volume': [100.0] * 24,
        },
        index=index,
    )


def _sample_minute_df() -> pd.DataFrame:
    return pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume'])


def _empty_backtest() -> BacktestResult:
    return _empty_backtest_for_pair('EURUSD')


def _empty_backtest_for_pair(pair: str) -> BacktestResult:
    return BacktestResult(
        pair=pair,
        total_trades=0,
        winning_trades=0,
        losing_trades=0,
        early_exits=0,
        win_rate=0.0,
        total_pnl_pips=0.0,
        avg_pnl_pips=0.0,
        avg_win_r=0.0,
        avg_loss_r=0.0,
        max_win_pips=0.0,
        max_loss_pips=0.0,
        profit_factor=0.0,
        trades=[],
        pending_trades=[],
        zones=[],
    )


class BacktestCacheTests(unittest.TestCase):
    def setUp(self):
        self.pair = 'EURUSD'
        self.info = {'ticker': 'EURUSD=X'}
        self.params = StrategyParams()
        self.hourly_days = 7
        self.zone_history_days = 20
        self.daily_df = _sample_daily_df()
        self.hourly_df = _sample_hourly_df()
        self.minute_df = _sample_minute_df()
        self.expected_signature = _data_signature(self.daily_df, self.hourly_df, self.minute_df)

    def test_cached_result_short_circuits_run(self):
        cached = _serialize_backtest_result(_empty_backtest())
        with patch('fx_sr.backtest._load_cached_backtest_data', return_value=(self.daily_df, self.hourly_df, self.minute_df)), \
                patch('fx_sr.backtest.load_l2_snapshots', return_value=[]), \
                patch('fx_sr.backtest.load_backtest_result', return_value=(
                    self.expected_signature,
                    cached,
                    BACKTEST_CACHE_VERSION,
                    None,
                )), \
                patch('fx_sr.backtest.save_backtest_result') as save_result, \
                patch('fx_sr.backtest.run_backtest') as run_backtest:
            _, result = _backtest_pair(
                self.pair,
                self.info,
                self.params,
                hourly_days=self.hourly_days,
                zone_history_days=self.zone_history_days,
                force_refresh=False,
            )

        self.assertEqual(result.total_trades, 0)
        run_backtest.assert_not_called()
        save_result.assert_not_called()

    def test_stale_cache_runs_backtest_and_saves(self):
        stale = _empty_backtest()
        stale_sig = 'stale-sig'
        with patch('fx_sr.backtest._load_cached_backtest_data', return_value=(self.daily_df, self.hourly_df, self.minute_df)), \
                patch('fx_sr.backtest.load_l2_snapshots', return_value=[]), \
                patch('fx_sr.backtest.load_backtest_result', return_value=(
                    stale_sig,
                    _serialize_backtest_result(stale),
                    BACKTEST_CACHE_VERSION,
                    None,
                )), \
                patch('fx_sr.backtest.run_backtest', return_value=stale) as run_backtest, \
                patch('fx_sr.backtest.save_backtest_result') as save_result:
            _, result = _backtest_pair(
                self.pair,
                self.info,
                self.params,
                hourly_days=self.hourly_days,
                zone_history_days=self.zone_history_days,
                force_refresh=False,
            )

        self.assertEqual(result.total_trades, 0)
        run_backtest.assert_called_once()
        self.assertEqual(save_result.call_count, 1)
        _, kwargs = save_result.call_args
        self.assertEqual(kwargs['data_signature'], self.expected_signature)
        self.assertEqual(kwargs['pair'], self.pair)
        self.assertEqual(kwargs['params_hash'], _params_signature(self.params))

    def test_force_refresh_ignores_cache(self):
        cached = _serialize_backtest_result(_empty_backtest())
        with patch('fx_sr.backtest._load_cached_backtest_data', return_value=(self.daily_df, self.hourly_df, self.minute_df)), \
                patch('fx_sr.backtest.load_l2_snapshots', return_value=[]), \
                patch('fx_sr.backtest.load_backtest_result') as load_result, \
                patch('fx_sr.backtest.run_backtest') as run_backtest, \
                patch('fx_sr.backtest.save_backtest_result') as save_result:
            run_backtest.return_value = _empty_backtest()
            load_result.return_value = (
                self.expected_signature,
                cached,
                BACKTEST_CACHE_VERSION,
                None,
            )

            _, result = _backtest_pair(
                self.pair,
                self.info,
                self.params,
                hourly_days=self.hourly_days,
                zone_history_days=self.zone_history_days,
                force_refresh=True,
            )

        load_result.assert_not_called()
        run_backtest.assert_called_once()
        save_result.assert_called_once()
        self.assertEqual(result.total_trades, 0)

    def test_cached_result_refreshes_missing_run_config(self):
        cached = _serialize_backtest_result(_empty_backtest())
        run_config_json = build_backtest_run_config_json(
            self.params,
            hourly_days=self.hourly_days,
            zone_history_days=self.zone_history_days,
            requested_profile='high_volume',
            starting_balance=1000.0,
            risk_pct=8.0,
            selection_label='baseline',
        )
        with patch('fx_sr.backtest._load_cached_backtest_data', return_value=(self.daily_df, self.hourly_df, self.minute_df)), \
                patch('fx_sr.backtest.load_l2_snapshots', return_value=[]), \
                patch('fx_sr.backtest.load_backtest_result', return_value=(
                    self.expected_signature,
                    cached,
                    BACKTEST_CACHE_VERSION,
                    None,
                )), \
                patch('fx_sr.backtest.save_backtest_result') as save_result, \
                patch('fx_sr.backtest.run_backtest') as run_backtest:
            _, result = _backtest_pair(
                self.pair,
                self.info,
                self.params,
                hourly_days=self.hourly_days,
                zone_history_days=self.zone_history_days,
                force_refresh=False,
                run_config_json=run_config_json,
            )

        self.assertEqual(result.total_trades, 0)
        run_backtest.assert_not_called()
        save_result.assert_called_once()
        _, kwargs = save_result.call_args
        self.assertEqual(kwargs['run_config_json'], run_config_json)

    def test_run_all_backtests_parallel_skips_cache_gapped_pairs(self):
        pairs = {
            'EURUSD': {'ticker': 'EURUSD=X'},
            'USDJPY': {'ticker': 'JPY=X'},
        }
        cached = _serialize_backtest_result(_empty_backtest_for_pair('USDJPY'))
        expected_signature = _data_signature(
            self.daily_df,
            self.hourly_df,
            self.minute_df,
            execution_mode='intrabar',
        )

        def fake_fetch(pair, pair_info, *_args, **_kwargs):
            if pair == 'EURUSD':
                raise BacktestCacheMissingError('Cached 1d data for EURUSD=X has a gap')
            return pair, self.daily_df, self.hourly_df, self.minute_df, []

        with patch('fx_sr.backtest._fetch_pair_data_only', side_effect=fake_fetch), \
                patch('fx_sr.backtest.load_backtest_result', return_value=(
                    expected_signature,
                    cached,
                    BACKTEST_CACHE_VERSION,
                    None,
                )), \
                patch('builtins.print'):
            results = run_all_backtests_parallel(
                params=self.params,
                hourly_days=self.hourly_days,
                zone_history_days=self.zone_history_days,
                pairs=pairs,
                run_id='test',
                fetch_workers=1,
                backtest_workers=1,
            )

        self.assertIn('USDJPY', results)
        self.assertNotIn('EURUSD', results)
        self.assertEqual(results['USDJPY'].pair, 'USDJPY')
        self.assertEqual(len(results), 1)


if __name__ == '__main__':
    unittest.main()
