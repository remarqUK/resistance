import unittest
from unittest.mock import patch

import pandas as pd

import fx_sr.data as data_module
import run


class FillGapDetectionTests(unittest.TestCase):
    def test_find_cache_gaps_marks_stale_hourly_and_minute_intervals(self):
        now = pd.Timestamp('2026-03-18T12:00:00Z')
        start = now - pd.Timedelta(days=365)
        summary = pd.DataFrame(
            [
                {
                    'ticker': 'EURUSD=X',
                    'interval': '1d',
                    'first_ts': start,
                    'last_ts': now - pd.Timedelta(hours=12),
                    'bars': 300,
                },
                {
                    'ticker': 'EURUSD=X',
                    'interval': '1h',
                    'first_ts': start,
                    'last_ts': now - pd.Timedelta(hours=8),
                    'bars': 6000,
                },
                {
                    'ticker': 'EURUSD=X',
                    'interval': '1m',
                    'first_ts': start,
                    'last_ts': now - pd.Timedelta(hours=8),
                    'bars': 400000,
                },
            ]
        )

        with patch.object(run, 'PAIRS', {'EURUSD': {'ticker': 'EURUSD=X'}}), \
                patch('fx_sr.fill_pipeline.init_db'), \
                patch('fx_sr.fill_pipeline.get_cache_summary', return_value=summary):
            gaps = run._find_cache_gaps(365, now=now)

        self.assertEqual(
            gaps,
            [
                ('EURUSD', 'EURUSD=X', '1h'),
                ('EURUSD', 'EURUSD=X', '1m'),
            ],
        )

    def test_find_cache_gaps_ignores_fresh_cache(self):
        now = pd.Timestamp('2026-03-18T12:00:00Z')
        start = now - pd.Timedelta(days=365)
        summary = pd.DataFrame(
            [
                {
                    'ticker': 'EURUSD=X',
                    'interval': '1d',
                    'first_ts': start,
                    'last_ts': now - pd.Timedelta(hours=12),
                    'bars': 300,
                },
                {
                    'ticker': 'EURUSD=X',
                    'interval': '1h',
                    'first_ts': start,
                    'last_ts': now - pd.Timedelta(minutes=30),
                    'bars': 6000,
                },
                {
                    'ticker': 'EURUSD=X',
                    'interval': '1m',
                    'first_ts': start,
                    'last_ts': now - pd.Timedelta(seconds=30),
                    'bars': 400000,
                },
            ]
        )

        with patch.object(run, 'PAIRS', {'EURUSD': {'ticker': 'EURUSD=X'}}), \
                patch('fx_sr.fill_pipeline.init_db'), \
                patch('fx_sr.fill_pipeline.get_cache_summary', return_value=summary):
            gaps = run._find_cache_gaps(365, now=now)

        self.assertEqual(gaps, [])

    def test_record_closure_gaps_and_step_back_uses_latest_cached_bar_before_gap(self):
        cached = pd.DataFrame(
            {'Close': [1.0, 1.1, 1.2]},
            index=pd.DatetimeIndex([
                '2025-12-24T21:59:00Z',
                '2025-12-29T22:15:00Z',
                '2025-12-29T22:16:00Z',
            ]),
        )

        with patch('fx_sr.data._record_closure_gaps', return_value=1815), \
                patch('fx_sr.data.load_ohlc', return_value=cached):
            recorded, next_end = data_module._record_closure_gaps_and_step_back(
                'AUDNZD=X',
                '1m',
                seed_gap=pd.Timestamp('2025-12-29T22:14:00Z'),
            )

        self.assertEqual(recorded, 1815)
        self.assertEqual(next_end, pd.Timestamp('2025-12-24T21:58:00Z'))

    def test_refill_interval_from_skips_ib_fetch_for_cache_confirmed_minute_closure(self):
        refilled = pd.DataFrame(
            {'Close': [1.0]},
            index=pd.DatetimeIndex(['2025-12-30T00:00:00Z']),
        )

        with patch(
            'fx_sr.data._record_closure_gaps_and_step_back',
            side_effect=[(1815, pd.Timestamp('2025-12-24T21:58:00Z')), (0, None)],
        ), \
                patch('fx_sr.data.load_ohlc', return_value=refilled), \
                patch('fx_sr.data.find_first_missing_cached_bar', return_value=None), \
                patch('fx_sr.data.ibkr.fetch_historical') as fetch_historical_mock, \
                patch('builtins.print'):
            result = data_module.refill_interval_from(
                'AUDNZD=X',
                '1m',
                pd.Timestamp('2025-12-24T22:00:00Z'),
            )

        fetch_historical_mock.assert_not_called()
        self.assertFalse(result.empty)


if __name__ == '__main__':
    unittest.main()
