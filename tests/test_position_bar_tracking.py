import unittest
from unittest.mock import patch

import pandas as pd

from fx_sr.positions import check_position_exits
from fx_sr.strategy import StrategyParams, Trade


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


def _trade() -> Trade:
    return Trade(
        entry_time=pd.Timestamp('2026-02-03 09:00:00', tz='UTC'),
        entry_price=1.1000,
        direction='LONG',
        sl_price=1.0950,
        tp_price=1.1100,
        zone_upper=1.1010,
        zone_lower=1.1000,
        zone_strength='major',
        risk=0.0050,
    )


class PositionBarTrackingTests(unittest.TestCase):
    def test_initial_scan_marks_latest_bar_without_aging_trade(self):
        tracked = {
            'EURUSD:LONG': {
                'pair': 'EURUSD',
                'trade': _trade(),
                'bars_monitored': 0,
                'ibkr_size': 10000,
                'last_processed_bar_time': None,
            }
        }
        hourly_df = _build_hourly_df(
            [
                ('2026-02-03 10:00:00', 1.1000, 1.1010, 1.0990, 1.1005),
            ]
        )

        with patch('fx_sr.positions.fetch_hourly_data', return_value=hourly_df), \
                patch('fx_sr.positions.check_exit', return_value=None) as check_exit_mock, \
                patch('fx_sr.positions._save_bar_tracking') as save_tracking_mock:
            alerts, snapshots = check_position_exits(tracked, StrategyParams())

        self.assertEqual(alerts, [])
        self.assertIn('EURUSD:LONG', snapshots)
        self.assertEqual(check_exit_mock.call_count, 1)
        self.assertEqual(check_exit_mock.call_args.kwargs['bars_held'], 0)
        self.assertEqual(tracked['EURUSD:LONG']['bars_monitored'], 0)
        self.assertEqual(tracked['EURUSD:LONG']['last_processed_bar_time'], hourly_df.index[-1])
        save_tracking_mock.assert_called_once_with('EURUSD', 'LONG', 0, hourly_df.index[-1])

    def test_only_new_hourly_bars_advance_trade_age_and_trigger_exit_checks(self):
        tracked = {
            'EURUSD:LONG': {
                'pair': 'EURUSD',
                'trade': _trade(),
                'bars_monitored': 0,
                'ibkr_size': 10000,
                'last_processed_bar_time': pd.Timestamp('2026-02-03 10:00:00', tz='UTC'),
            }
        }
        hourly_df = _build_hourly_df(
            [
                ('2026-02-03 10:00:00', 1.1000, 1.1010, 1.0990, 1.1005),
                ('2026-02-03 11:00:00', 1.1005, 1.1015, 1.1000, 1.1010),
                ('2026-02-03 12:00:00', 1.1010, 1.1012, 1.0995, 1.1002),
                ('2026-02-03 13:00:00', 1.1002, 1.1006, 1.0985, 1.0990),
            ]
        )

        with patch('fx_sr.positions.fetch_hourly_data', return_value=hourly_df), \
                patch(
                    'fx_sr.positions.check_exit',
                    side_effect=[None, ('TIME', 1.1000)],
                ) as check_exit_mock, \
                patch('fx_sr.positions._save_bar_tracking') as save_tracking_mock:
            alerts, _ = check_position_exits(tracked, StrategyParams())

        self.assertEqual(check_exit_mock.call_count, 2)
        self.assertEqual(check_exit_mock.call_args_list[0].kwargs['bars_held'], 1)
        self.assertEqual(check_exit_mock.call_args_list[1].kwargs['bars_held'], 2)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]['exit_reason'], 'TIME')
        self.assertEqual(alerts[0]['bars_monitored'], 2)
        self.assertEqual(tracked['EURUSD:LONG']['bars_monitored'], 2)
        self.assertEqual(tracked['EURUSD:LONG']['last_processed_bar_time'], hourly_df.index[2])
        save_tracking_mock.assert_called_once_with('EURUSD', 'LONG', 2, hourly_df.index[2])

        with patch('fx_sr.positions.fetch_hourly_data', return_value=hourly_df), \
                patch('fx_sr.positions.check_exit', return_value=None) as check_exit_mock, \
                patch('fx_sr.positions._save_bar_tracking') as save_tracking_mock:
            alerts, _ = check_position_exits(tracked, StrategyParams())

        self.assertEqual(alerts, [])
        self.assertEqual(check_exit_mock.call_count, 1)
        self.assertEqual(check_exit_mock.call_args.kwargs['bars_held'], 3)
        self.assertEqual(tracked['EURUSD:LONG']['bars_monitored'], 3)
        self.assertEqual(tracked['EURUSD:LONG']['last_processed_bar_time'], hourly_df.index[-1])
        save_tracking_mock.assert_called_once_with('EURUSD', 'LONG', 3, hourly_df.index[-1])


if __name__ == '__main__':
    unittest.main()
