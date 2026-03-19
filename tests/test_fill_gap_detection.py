import unittest
from unittest.mock import patch

import pandas as pd

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
                patch('fx_sr.db.init_db'), \
                patch('fx_sr.db.get_cache_summary', return_value=summary):
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
                patch('fx_sr.db.init_db'), \
                patch('fx_sr.db.get_cache_summary', return_value=summary):
            gaps = run._find_cache_gaps(365, now=now)

        self.assertEqual(gaps, [])


if __name__ == '__main__':
    unittest.main()
