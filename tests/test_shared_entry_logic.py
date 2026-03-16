import unittest
from unittest.mock import patch
from types import SimpleNamespace

import pandas as pd

from fx_sr.backtest import run_backtest
from fx_sr.backtest import run_backtest_fast
from fx_sr.backtest import run_all_backtests_parallel
from fx_sr.config import PAIRS
from fx_sr import live as live_module
from fx_sr.levels import SRZone
from fx_sr.live import _scan_pair
from fx_sr.profiles import BLOCKED_PAIR_DIRECTIONS
from fx_sr.strategy import StrategyParams, get_entry_execution_price
from fx_sr.strategy import is_pair_fully_blocked


def _build_daily_df(rows: int = 40) -> pd.DataFrame:
    index = pd.date_range('2026-01-01', periods=rows, freq='D', tz='UTC')
    return pd.DataFrame(
        {
            'Open': [1.1000] * rows,
            'High': [1.1100] * rows,
            'Low': [1.0900] * rows,
            'Close': [1.1000] * rows,
            'Volume': [0.0] * rows,
        },
        index=index,
    )


def _build_hourly_df(rows: list[tuple[str, float, float, float, float]]) -> pd.DataFrame:
    index = pd.DatetimeIndex([pd.Timestamp(ts, tz='UTC') for ts, *_ in rows])
    return pd.DataFrame(
        {
            'Open': [row[1] for row in rows],
            'High': [row[2] for row in rows],
            'Low': [row[3] for row in rows],
            'Close': [row[4] for row in rows],
            'Volume': [0.0] * len(rows),
        },
        index=index,
    )


def _build_minute_df(rows: list[tuple[str, float]]) -> pd.DataFrame:
    index = pd.DatetimeIndex([pd.Timestamp(ts, tz='UTC') for ts, _ in rows])
    return pd.DataFrame(
        {
            'Open': [price for _, price in rows],
            'High': [price for _, price in rows],
            'Low': [price for _, price in rows],
            'Close': [price for _, price in rows],
            'Volume': [0.0] * len(rows),
        },
        index=index,
    )


def _support_zone(lower: float, upper: float) -> SRZone:
    return SRZone(
        lower=lower,
        upper=upper,
        midpoint=(lower + upper) / 2.0,
        touches=4,
        zone_type='support',
        strength='major',
    )


def _resistance_zone(lower: float, upper: float) -> SRZone:
    return SRZone(
        lower=lower,
        upper=upper,
        midpoint=(lower + upper) / 2.0,
        touches=4,
        zone_type='resistance',
        strength='major',
    )


def _fully_blocked_pair() -> str:
    blocked: dict[str, set[str]] = {}
    for pair, direction in BLOCKED_PAIR_DIRECTIONS:
        blocked.setdefault(pair, set()).add(direction)
    for pair, directions in blocked.items():
        if {'LONG', 'SHORT'}.issubset(directions):
            return pair
    raise AssertionError('Expected at least one pair to be blocked in both directions')


class SharedEntryLogicTests(unittest.TestCase):
    def setUp(self):
        live_module._LIVE_DAILY_DATA_CACHE.clear()
        live_module._LIVE_ZONE_CACHE.clear()
        live_module._LIVE_HOURLY_DATA_CACHE.clear()

    def test_live_scan_and_backtest_both_honor_time_filters(self):
        daily_df = _build_daily_df()
        hourly_df = _build_hourly_df(
            [
                ('2026-02-02 01:00:00', 1.1020, 1.1025, 1.1010, 1.1015),
                ('2026-02-02 02:00:00', 1.1000, 1.1004, 1.0999, 1.1003),
            ]
        )
        params = StrategyParams(
            min_entry_candle_body_pct=0.0,
            momentum_lookback=1,
            momentum_threshold=0.99,
            blocked_hours=frozenset({2}),
            blocked_days=frozenset(),
            use_time_filters=True,
            use_pair_direction_filter=False,
        )

        def zones(_):
            return [_support_zone(1.1000, 1.1010)]

        with patch('fx_sr.backtest.detect_zones', side_effect=zones):
            result = run_backtest(daily_df, hourly_df, 'EURUSD', params=params, zone_history_days=20)

        self.assertEqual(result.total_trades, 0)

        with patch('fx_sr.live.fetch_daily_data', return_value=daily_df), \
                patch('fx_sr.live.fetch_hourly_data', return_value=hourly_df), \
                patch('fx_sr.live.detect_zones', side_effect=zones):
            row, signal = _scan_pair(
                'EURUSD',
                {'ticker': 'EURUSD=X', 'name': 'EUR/USD', 'decimals': 5},
                params,
                zone_history_days=20,
                tracked_pairs={},
                blocked_pairs=set(),
                daily_data_cache={},
                zone_cache={},
                hourly_data_cache={},
            )

        self.assertIsNone(signal)
        self.assertEqual(row.state, 'INSIDE')

    def test_live_scan_and_backtest_use_same_current_bar_zone_selection(self):
        daily_df = _build_daily_df()
        hourly_df = _build_hourly_df(
            [
                ('2026-02-03 00:00:00', 1.1040, 1.1045, 1.1035, 1.1040),
                ('2026-02-03 01:00:00', 1.0950, 1.0955, 1.0945, 1.0950),
                ('2026-02-03 04:00:00', 1.0900, 1.0904, 1.0899, 1.0903),
                ('2026-02-03 05:00:00', 1.0903, 1.0905, 1.0900, 1.0903),
            ]
        )
        live_hourly_df = hourly_df.iloc[:-1].copy()
        minute_df = _build_minute_df([('2026-02-03 05:00:00', 1.0903)])
        params = StrategyParams(
            min_entry_candle_body_pct=0.0,
            momentum_lookback=1,
            momentum_threshold=0.99,
            use_time_filters=False,
            use_pair_direction_filter=False,
            strict_backtest_execution=True,
        )

        def zones(_):
            return [
                _support_zone(1.1000, 1.1010),
                _support_zone(1.0900, 1.0910),
            ]

        with patch('fx_sr.backtest.detect_zones', side_effect=zones):
            result = run_backtest(
                daily_df,
                hourly_df,
                'EURUSD',
                params=params,
                zone_history_days=20,
                minute_df=minute_df,
            )

        self.assertEqual(result.total_trades, 1)
        self.assertEqual(result.trades[0].zone_lower, 1.0900)
        self.assertEqual(result.trades[0].direction, 'LONG')
        self.assertEqual(result.trades[0].entry_time, pd.Timestamp('2026-02-03 05:00:00', tz='UTC'))

        with patch('fx_sr.live.fetch_daily_data', return_value=daily_df), \
                patch('fx_sr.live.fetch_hourly_data', return_value=live_hourly_df), \
                patch('fx_sr.live.detect_zones', side_effect=zones):
            _, signal = _scan_pair(
                'EURUSD',
                {'ticker': 'EURUSD=X', 'name': 'EUR/USD', 'decimals': 5},
                params,
                zone_history_days=20,
                tracked_pairs={},
                blocked_pairs=set(),
                daily_data_cache={},
                zone_cache={},
                hourly_data_cache={},
            )

        self.assertIsNotNone(signal)
        self.assertEqual(signal.zone_lower, 1.0900)
        self.assertEqual(signal.direction, result.trades[0].direction)

    def test_live_scan_and_backtest_share_short_resistance_entries(self):
        daily_df = _build_daily_df()
        hourly_df = _build_hourly_df(
            [
                ('2026-02-04 00:00:00', 1.0960, 1.0965, 1.0955, 1.0960),
                ('2026-02-04 01:00:00', 1.1040, 1.1045, 1.1035, 1.1040),
                ('2026-02-04 04:00:00', 1.1110, 1.1112, 1.1104, 1.1106),
                ('2026-02-04 05:00:00', 1.1106, 1.1108, 1.1102, 1.1105),
            ]
        )
        live_hourly_df = hourly_df.iloc[:-1].copy()
        minute_df = _build_minute_df([('2026-02-04 05:00:00', 1.1106)])
        params = StrategyParams(
            min_entry_candle_body_pct=0.0,
            momentum_lookback=1,
            momentum_threshold=0.99,
            use_time_filters=False,
            use_pair_direction_filter=False,
            strict_backtest_execution=True,
        )

        def zones(_):
            return [
                _resistance_zone(1.1000, 1.1010),
                _resistance_zone(1.1100, 1.1110),
            ]

        with patch('fx_sr.backtest.detect_zones', side_effect=zones):
            result = run_backtest(
                daily_df,
                hourly_df,
                'EURUSD',
                params=params,
                zone_history_days=20,
                minute_df=minute_df,
            )

        self.assertEqual(result.total_trades, 1)
        self.assertEqual(result.trades[0].zone_upper, 1.1110)
        self.assertEqual(result.trades[0].direction, 'SHORT')
        self.assertEqual(result.trades[0].entry_time, pd.Timestamp('2026-02-04 05:00:00', tz='UTC'))

        with patch('fx_sr.live.fetch_daily_data', return_value=daily_df), \
                patch('fx_sr.live.fetch_hourly_data', return_value=live_hourly_df), \
                patch('fx_sr.live.detect_zones', side_effect=zones):
            _, signal = _scan_pair(
                'EURUSD',
                {'ticker': 'EURUSD=X', 'name': 'EUR/USD', 'decimals': 5},
                params,
                zone_history_days=20,
                tracked_pairs={},
                blocked_pairs=set(),
                daily_data_cache={},
                zone_cache={},
                hourly_data_cache={},
            )

        self.assertIsNotNone(signal)
        self.assertEqual(signal.zone_upper, 1.1110)
        self.assertEqual(signal.direction, result.trades[0].direction)

    def test_run_backtest_fast_matches_walk_forward_backtest(self):
        daily_df = _build_daily_df()
        hourly_df = _build_hourly_df(
            [
                ('2026-02-03 00:00:00', 1.1040, 1.1045, 1.1035, 1.1040),
                ('2026-02-03 01:00:00', 1.0950, 1.0955, 1.0945, 1.0950),
                ('2026-02-03 04:00:00', 1.0900, 1.0904, 1.0899, 1.0903),
                ('2026-02-03 05:00:00', 1.0903, 1.0905, 1.0900, 1.0903),
            ]
        )
        params = StrategyParams(
            min_entry_candle_body_pct=0.0,
            momentum_lookback=1,
            momentum_threshold=0.99,
            use_time_filters=False,
            use_pair_direction_filter=False,
        )
        minute_df = _build_minute_df([('2026-02-03 05:00:00', 1.0903)])
        zones = [
            _support_zone(1.1000, 1.1010),
            _support_zone(1.0900, 1.0910),
        ]
        zone_cache = {('EURUSD', '2026-02-03'): zones}

        with patch('fx_sr.backtest.detect_zones', return_value=zones):
            standard = run_backtest(
                daily_df,
                hourly_df,
                'EURUSD',
                params=params,
                zone_history_days=20,
                minute_df=minute_df,
            )

        fast = run_backtest_fast(
            hourly_df,
            'EURUSD',
            params,
            zone_cache,
            pip=0.0001,
            minute_df=minute_df,
        )

        self.assertEqual(fast.total_trades, standard.total_trades)
        self.assertEqual(len(fast.trades), len(standard.trades))
        self.assertEqual(fast.trades[0].entry_time, standard.trades[0].entry_time)
        self.assertEqual(fast.trades[0].exit_time, standard.trades[0].exit_time)
        self.assertEqual(fast.trades[0].exit_reason, standard.trades[0].exit_reason)
        self.assertAlmostEqual(fast.trades[0].pnl_r, standard.trades[0].pnl_r)
        self.assertAlmostEqual(fast.trades[0].zone_lower, standard.trades[0].zone_lower)

    def test_backtest_executes_on_next_bar_open_with_repriced_entry(self):
        daily_df = _build_daily_df()
        hourly_df = _build_hourly_df(
            [
                ('2026-02-05 00:00:00', 1.1030, 1.1034, 1.1028, 1.1030),
                ('2026-02-05 01:00:00', 1.1000, 1.1005, 1.0999, 1.1003),
                ('2026-02-05 02:00:00', 1.1004, 1.1006, 1.1001, 1.1004),
            ]
        )
        params = StrategyParams(
            min_entry_candle_body_pct=0.0,
            momentum_lookback=1,
            momentum_threshold=0.99,
            use_time_filters=False,
            use_pair_direction_filter=False,
            strict_backtest_execution=True,
            allow_h1_execution_fallback=False,
        )
        minute_df = _build_minute_df([('2026-02-05 02:00:00', 1.1004)])
        zone = _support_zone(1.1000, 1.1010)

        with patch('fx_sr.backtest.detect_zones', return_value=[zone]):
            result = run_backtest(
                daily_df,
                hourly_df,
                'EURUSD',
                params=params,
                zone_history_days=20,
                minute_df=minute_df,
            )

        self.assertEqual(result.total_trades, 1)
        self.assertEqual(result.trades[0].entry_time, pd.Timestamp('2026-02-05 02:00:00', tz='UTC'))
        self.assertAlmostEqual(
            result.trades[0].entry_price,
            get_entry_execution_price(1.1004, 'LONG', 0.0001, params),
        )

    def test_backtest_skips_entry_when_strict_execution_quote_is_missing(self):
        daily_df = _build_daily_df()
        hourly_df = _build_hourly_df(
            [
                ('2026-02-05 00:00:00', 1.1030, 1.1034, 1.1028, 1.1030),
                ('2026-02-05 01:00:00', 1.1000, 1.1005, 1.0999, 1.1003),
                ('2026-02-05 02:00:00', 1.1004, 1.1006, 1.1001, 1.1004),
            ]
        )
        params = StrategyParams(
            min_entry_candle_body_pct=0.0,
            momentum_lookback=1,
            momentum_threshold=0.99,
            use_time_filters=False,
            use_pair_direction_filter=False,
            strict_backtest_execution=True,
            allow_h1_execution_fallback=False,
        )
        zone = _support_zone(1.1000, 1.1010)

        with patch('fx_sr.backtest.detect_zones', return_value=[zone]):
            result = run_backtest(
                daily_df,
                hourly_df,
                'EURUSD',
                params=params,
                zone_history_days=20,
            )

        self.assertEqual(result.total_trades, 0)

    def test_pair_fully_blocked_predicate(self):
        blocked_pair = _fully_blocked_pair()
        self.assertTrue(is_pair_fully_blocked(blocked_pair, StrategyParams()))
        self.assertFalse(is_pair_fully_blocked('EURUSD', StrategyParams()))
        self.assertFalse(
            is_pair_fully_blocked(blocked_pair, StrategyParams(use_pair_direction_filter=False)),
        )

    def test_collect_scan_rows_skips_pair_with_both_directions_blocked(self):
        blocked_pair = _fully_blocked_pair()
        pairs = {
            blocked_pair: {
                'ticker': PAIRS[blocked_pair]['ticker'],
                'name': PAIRS[blocked_pair]['name'],
                'decimals': PAIRS[blocked_pair]['decimals'],
            },
            'EURUSD': {'ticker': 'EURUSD=X', 'name': 'EUR/USD', 'decimals': 5},
        }
        row = live_module.PairScanRow(
            pair='EURUSD',
            name='EUR/USD',
            decimals=5,
            price=1.1,
            state='INSIDE',
            note='No signal',
            support_text='-',
            resistance_text='-',
        )

        with patch('fx_sr.live._scan_pair', return_value=(row, None)) as scan_pair:
            _, rows = live_module.collect_scan_rows(pairs=pairs, params=StrategyParams())

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].pair, 'EURUSD')
        scan_pair.assert_called_once()
        self.assertEqual(scan_pair.call_args.args[0], 'EURUSD')

    def test_run_all_backtests_parallel_skips_fully_blocked_pair(self):
        blocked_pair = _fully_blocked_pair()
        pairs = {
            blocked_pair: {'ticker': PAIRS[blocked_pair]['ticker']},
            'EURUSD': {'ticker': 'EURUSD=X'},
        }
        scanned = []

        fake_result = SimpleNamespace(
            total_trades=0,
            win_rate=0.0,
            total_pnl_pips=0.0,
        )

        def fake_backtest_pair(pair, info, params, hourly_days, zone_history_days, force_refresh, client_id):
            scanned.append(pair)
            return pair, fake_result

        with patch('fx_sr.backtest._backtest_pair', side_effect=fake_backtest_pair):
            results = run_all_backtests_parallel(
                params=StrategyParams(),
                hourly_days=10,
                zone_history_days=20,
                pairs=pairs,
                force_refresh=True,
                base_client_id=1000,
            )

        self.assertEqual(scanned, ['EURUSD'])
        self.assertEqual(set(results.keys()), {'EURUSD'})

    def test_run_all_backtests_parallel_includes_fully_blocked_pair_when_filter_disabled(self):
        blocked_pair = _fully_blocked_pair()
        pairs = {
            blocked_pair: {'ticker': PAIRS[blocked_pair]['ticker']},
            'EURUSD': {'ticker': 'EURUSD=X'},
        }
        scanned = []

        fake_result = SimpleNamespace(
            total_trades=0,
            win_rate=0.0,
            total_pnl_pips=0.0,
        )

        def fake_backtest_pair(pair, info, params, hourly_days, zone_history_days, force_refresh, client_id):
            scanned.append(pair)
            return pair, fake_result

        with patch('fx_sr.backtest._backtest_pair', side_effect=fake_backtest_pair):
            results = run_all_backtests_parallel(
                params=StrategyParams(use_pair_direction_filter=False),
                hourly_days=10,
                zone_history_days=20,
                pairs=pairs,
                force_refresh=True,
                base_client_id=1000,
            )

        self.assertEqual(scanned, [blocked_pair, 'EURUSD'])
        self.assertEqual(set(results.keys()), {blocked_pair, 'EURUSD'})

    def test_run_all_backtests_parallel_with_only_blocked_pairs_returns_empty(self):
        blocked_pair = _fully_blocked_pair()
        pairs = {blocked_pair: {'ticker': PAIRS[blocked_pair]['ticker']}}

        with patch('fx_sr.backtest._backtest_pair') as backtest_pair:
            results = run_all_backtests_parallel(
                params=StrategyParams(),
                hourly_days=10,
                zone_history_days=20,
                pairs=pairs,
                force_refresh=True,
                base_client_id=1000,
            )

        self.assertEqual(results, {})
        backtest_pair.assert_not_called()


if __name__ == '__main__':
    unittest.main()
