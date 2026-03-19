import unittest
from datetime import date
from unittest.mock import patch

import pandas as pd

from fx_sr.backtest import BACKTEST_CACHE_VERSION, BacktestResult, _params_signature, _serialize_backtest_result
from fx_sr.levels import SRZone
from fx_sr.profiles import get_profile
from fx_sr.replay import (
    _build_account_day_summary,
    _extend_hourly_with_minute_tail,
    _load_cached_backtest_trades,
    _select_cached_backtest_rows,
    _trade_active_dates,
    _trade_is_active_on_date,
    generate_replay_frames,
)
from fx_sr.strategy import StrategyParams, Trade, params_from_profile


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


def _trade(entry: str, exit_: str, pnl_pips: float, pnl_r: float) -> Trade:
    entry_price = 1.1000
    exit_price = entry_price + (0.0010 if pnl_pips >= 0 else -0.0010)
    return Trade(
        entry_time=pd.Timestamp(entry, tz='UTC'),
        entry_price=entry_price,
        direction='LONG',
        sl_price=entry_price - 0.0020,
        tp_price=entry_price + 0.0040,
        zone_upper=entry_price + 0.0010,
        zone_lower=entry_price - 0.0010,
        zone_strength='major',
        risk=0.0020,
        exit_time=pd.Timestamp(exit_, tz='UTC'),
        exit_price=exit_price,
        exit_reason='TP' if pnl_pips >= 0 else 'SL',
        pnl_pips=pnl_pips,
        pnl_r=pnl_r,
        bars_held=4,
    )


def _result(pair: str, trades: list[Trade]) -> BacktestResult:
    winning_trades = sum(1 for trade in trades if trade.pnl_pips > 0)
    losing_trades = len(trades) - winning_trades
    total_pnl = sum(trade.pnl_pips for trade in trades)
    return BacktestResult(
        pair=pair,
        total_trades=len(trades),
        winning_trades=winning_trades,
        losing_trades=losing_trades,
        early_exits=0,
        win_rate=(winning_trades / len(trades) * 100) if trades else 0.0,
        total_pnl_pips=total_pnl,
        avg_pnl_pips=(total_pnl / len(trades)) if trades else 0.0,
        avg_win_r=sum(trade.pnl_r for trade in trades if trade.pnl_pips > 0) / winning_trades if winning_trades else 0.0,
        avg_loss_r=sum(trade.pnl_r for trade in trades if trade.pnl_pips <= 0) / losing_trades if losing_trades else 0.0,
        max_win_pips=max((trade.pnl_pips for trade in trades), default=0.0),
        max_loss_pips=min((trade.pnl_pips for trade in trades), default=0.0),
        profit_factor=float('inf'),
        trades=trades,
        zones=[],
    )


class ReplayTests(unittest.TestCase):
    def test_extend_hourly_with_minute_tail_builds_missing_trailing_hours(self):
        hourly_df = _build_hourly_df(
            [
                ('2026-03-18 08:00:00', 1.1000, 1.1010, 1.0990, 1.1005),
                ('2026-03-18 09:00:00', 1.1005, 1.1015, 1.1000, 1.1010),
            ]
        )
        minute_df = _build_minute_df(
            [
                ('2026-03-18 10:05:00', 1.1012),
                ('2026-03-18 10:35:00', 1.1018),
                ('2026-03-18 10:50:00', 1.1014),
                ('2026-03-18 11:10:00', 1.1016),
                ('2026-03-18 11:40:00', 1.1022),
                ('2026-03-18 11:55:00', 1.1019),
            ]
        )

        extended = _extend_hourly_with_minute_tail(hourly_df, minute_df)

        self.assertEqual(str(extended.index[-2]), '2026-03-18 10:00:00+00:00')
        self.assertEqual(str(extended.index[-1]), '2026-03-18 11:00:00+00:00')
        self.assertAlmostEqual(float(extended.iloc[-2]['Open']), 1.1012)
        self.assertAlmostEqual(float(extended.iloc[-2]['High']), 1.1018)
        self.assertAlmostEqual(float(extended.iloc[-2]['Low']), 1.1012)
        self.assertAlmostEqual(float(extended.iloc[-2]['Close']), 1.1014)
        self.assertAlmostEqual(float(extended.iloc[-1]['Open']), 1.1016)
        self.assertAlmostEqual(float(extended.iloc[-1]['High']), 1.1022)
        self.assertAlmostEqual(float(extended.iloc[-1]['Low']), 1.1016)
        self.assertAlmostEqual(float(extended.iloc[-1]['Close']), 1.1019)

    def test_trade_active_dates_span_every_day_until_exit(self):
        active_dates = _trade_active_dates(
            pd.Timestamp('2026-02-03 22:00:00', tz='UTC'),
            pd.Timestamp('2026-02-05 09:00:00', tz='UTC'),
        )

        self.assertEqual(active_dates, ['2026-02-03', '2026-02-04', '2026-02-05'])
        self.assertTrue(_trade_is_active_on_date({'active_dates': active_dates}, '2026-02-04'))
        self.assertFalse(_trade_is_active_on_date({'active_dates': active_dates}, '2026-02-06'))

    def test_generate_replay_frames_extend_until_selected_day_trade_exits(self):
        daily_df = _build_daily_df()
        hourly_df = _build_hourly_df(
            [
                ('2026-02-02 22:00:00', 1.1030, 1.1035, 1.1025, 1.1030),
                ('2026-02-03 23:00:00', 1.1001, 1.1005, 1.1000, 1.1004),
                ('2026-02-04 00:00:00', 1.1006, 1.1065, 1.1004, 1.1060),
                ('2026-02-04 01:00:00', 1.1060, 1.1062, 1.1055, 1.1058),
            ]
        )
        params = StrategyParams(
            min_entry_candle_body_pct=0.0,
            momentum_lookback=1,
            momentum_threshold=0.99,
            use_time_filters=False,
            use_pair_direction_filter=False,
        )
        minute_df = _build_minute_df([('2026-02-04 00:00:00', 1.1006)])

        with patch('fx_sr.replay.detect_zones', return_value=[_support_zone(1.1000, 1.1010)]):
            result = generate_replay_frames(
                daily_df,
                hourly_df,
                'EURUSD',
                date(2026, 2, 3),
                params=params,
                zone_history_days=20,
                minute_df=minute_df,
            )

        self.assertEqual(result['summary']['selected_day_bars'], 1)
        self.assertEqual(result['summary']['replay_bars'], 1)
        self.assertFalse(result['summary']['continues_after_selected_day'])
        self.assertEqual(result['frames'][0]['time'][:10], '2026-02-03')
        self.assertEqual(result['all_completed_trades'][0]['active_dates'], ['2026-02-04'])
        self.assertEqual(result['summary']['total_trades'], 0)

    def test_load_cached_backtest_trades_adds_running_balance(self):
        trades = [
            _trade('2026-03-01 08:00:00', '2026-03-01 10:00:00', 10.0, 1.0),
            _trade('2026-03-02 08:00:00', '2026-03-02 10:00:00', -5.0, -0.5),
        ]
        result = _result('EURUSD', trades)
        profile = get_profile('aggressive')
        params = params_from_profile(profile)
        row = {
            'pair': 'EURUSD',
            'params_hash': _params_signature(params),
            'hourly_days': 365,
            'zone_history_days': 180,
            'strategy_version': BACKTEST_CACHE_VERSION,
            'updated_at': '2026-03-10T12:00:00',
            'result_json': _serialize_backtest_result(result),
        }

        with patch('fx_sr.replay._select_cached_backtest_rows', return_value=([row], [], None)):
            loaded_trades, compounding = _load_cached_backtest_trades()

        self.assertEqual(compounding['profile_name'], 'aggressive')
        self.assertFalse(compounding['assumed'])
        self.assertEqual(len(loaded_trades), 2)
        self.assertAlmostEqual(loaded_trades[0]['risk_amount'], 52.5)
        self.assertAlmostEqual(loaded_trades[0]['pnl_amount'], -26.25)
        self.assertAlmostEqual(loaded_trades[0]['balance_after'], 1023.75)
        self.assertAlmostEqual(loaded_trades[1]['risk_amount'], 50.0)
        self.assertAlmostEqual(loaded_trades[1]['pnl_amount'], 50.0)
        self.assertAlmostEqual(loaded_trades[1]['balance_after'], 1050.0)

    def test_load_cached_backtest_trades_keeps_account_compounding_when_filtered_by_pair(self):
        eur_result = _result('EURUSD', [_trade('2026-03-02 08:00:00', '2026-03-02 10:00:00', 10.0, 1.0)])
        aud_result = _result('AUDUSD', [_trade('2026-03-01 08:00:00', '2026-03-01 10:00:00', 10.0, 1.0)])
        profile = get_profile('aggressive')
        params = params_from_profile(profile)

        eur_row = {
            'pair': 'EURUSD',
            'params_hash': _params_signature(params),
            'hourly_days': 365,
            'zone_history_days': 180,
            'strategy_version': BACKTEST_CACHE_VERSION,
            'updated_at': '2026-03-10T12:00:00',
            'result_json': _serialize_backtest_result(eur_result),
        }
        aud_row = {
            'pair': 'AUDUSD',
            'params_hash': _params_signature(params),
            'hourly_days': 365,
            'zone_history_days': 180,
            'strategy_version': BACKTEST_CACHE_VERSION,
            'updated_at': '2026-03-10T12:00:00',
            'result_json': _serialize_backtest_result(aud_result),
        }

        with patch('fx_sr.replay._select_cached_backtest_rows', return_value=([eur_row, aud_row], [], None)):
            loaded_trades, compounding = _load_cached_backtest_trades(pair='EURUSD')

        self.assertEqual(compounding['profile_name'], 'aggressive')
        self.assertEqual(len(loaded_trades), 1)
        self.assertAlmostEqual(loaded_trades[0]['risk_amount'], 52.5)
        self.assertAlmostEqual(loaded_trades[0]['pnl_amount'], 52.5)
        self.assertAlmostEqual(loaded_trades[0]['balance_after'], 1102.5)

    def test_build_account_day_summary_uses_realized_money_pnl_and_latest_balance(self):
        trades = [
            _trade('2026-03-01 08:00:00', '2026-03-01 10:00:00', 10.0, 1.0),
            _trade('2026-03-02 08:00:00', '2026-03-02 10:00:00', -5.0, -0.5),
        ]
        result = _result('EURUSD', trades)
        profile = get_profile('aggressive')
        params = params_from_profile(profile)
        row = {
            'pair': 'EURUSD',
            'params_hash': _params_signature(params),
            'hourly_days': 365,
            'zone_history_days': 180,
            'strategy_version': BACKTEST_CACHE_VERSION,
            'updated_at': '2026-03-10T12:00:00',
            'result_json': _serialize_backtest_result(result),
        }

        with patch('fx_sr.replay._select_cached_backtest_rows', return_value=([row], [], None)):
            loaded_trades, compounding = _load_cached_backtest_trades()

        march_first = _build_account_day_summary(date(2026, 3, 1), loaded_trades, compounding)
        self.assertAlmostEqual(march_first['day_pnl_amount'], 50.0)
        self.assertAlmostEqual(march_first['balance'], 1050.0)

        march_second = _build_account_day_summary(date(2026, 3, 2), loaded_trades, compounding)
        self.assertAlmostEqual(march_second['day_pnl_amount'], -26.25)
        self.assertAlmostEqual(march_second['balance'], 1023.75)

        march_third = _build_account_day_summary(date(2026, 3, 3), loaded_trades, compounding)
        self.assertAlmostEqual(march_third['day_pnl_amount'], 0.0)
        self.assertAlmostEqual(march_third['balance'], 1023.75)

    def test_select_cached_backtest_rows_prefers_default_profile(self):
        default_params = params_from_profile(get_profile('high_volume'))
        other_params = params_from_profile(get_profile('balanced'))
        rows = [
            {
                'pair': 'EURUSD',
                'params_hash': _params_signature(other_params),
                'hourly_days': 365,
                'zone_history_days': 180,
                'strategy_version': BACKTEST_CACHE_VERSION,
                'updated_at': '2026-03-13T21:32:09+00:00',
                'run_config_json': None,
                'result_json': _serialize_backtest_result(_result('EURUSD', [])),
            },
            {
                'pair': 'EURUSD',
                'params_hash': _params_signature(default_params),
                'hourly_days': 365,
                'zone_history_days': 180,
                'strategy_version': BACKTEST_CACHE_VERSION,
                'updated_at': '2026-03-13T21:24:10+00:00',
                'run_config_json': None,
                'result_json': _serialize_backtest_result(_result('EURUSD', [])),
            },
        ]

        with patch('fx_sr.replay.db.load_backtest_results', return_value=rows):
            selected_rows, backtests, selected = _select_cached_backtest_rows()

        self.assertEqual(len(selected_rows), 1)
        self.assertEqual(selected['profile_name'], 'high_volume')
        self.assertEqual(selected['description'], get_profile('high_volume')['description'])
        self.assertEqual(selected_rows[0]['params_hash'], _params_signature(default_params))
        self.assertEqual(backtests[0]['profile_name'], 'high_volume')
        self.assertEqual(backtests[0]['description'], get_profile('high_volume')['description'])


if __name__ == '__main__':
    unittest.main()
