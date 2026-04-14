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

    def test_find_cache_gap_work_items_keeps_full_cache_quick_without_data_copy(self):
        now = pd.Timestamp('2026-04-13T12:00:00Z')
        start = now - pd.Timedelta(days=365)
        summary = pd.DataFrame(
            [
                {
                    'ticker': 'EURUSD=X',
                    'interval': '1d',
                    'first_ts': start,
                    'last_ts': now,
                    'bars': 260,
                },
                {
                    'ticker': 'EURUSD=X',
                    'interval': '1h',
                    'first_ts': start,
                    'last_ts': now,
                    'bars': 8760,
                },
                {
                    'ticker': 'EURUSD=X',
                    'interval': '1m',
                    'first_ts': start,
                    'last_ts': now,
                    'bars': 525000,
                },
            ]
        )

        with patch.object(run, 'PAIRS', {'EURUSD': {'ticker': 'EURUSD=X'}}), \
                patch('fx_sr.fill_pipeline.init_db'), \
                patch('fx_sr.fill_pipeline.get_cache_summary', return_value=summary):
            gaps = run._find_cache_gap_work_items(
                target_days=365,
                now=now,
            )

        self.assertEqual(gaps, [])

    def test_find_cache_gap_work_items_marks_missing_timestamps_for_coverage_gaps(self):
        now = pd.Timestamp('2026-04-13T12:00:00Z')
        start = now - pd.Timedelta(days=365)
        summary = pd.DataFrame(
            [
                {
                    'ticker': 'EURUSD=X',
                    'interval': '1d',
                    'first_ts': start,
                    'last_ts': now,
                    'bars': 80,
                },
                {
                    'ticker': 'EURUSD=X',
                    'interval': '1h',
                    'first_ts': start,
                    'last_ts': now - pd.Timedelta(hours=2),
                    'bars': 200,
                },
                {
                    'ticker': 'EURUSD=X',
                    'interval': '1m',
                    'first_ts': start,
                    'last_ts': now - pd.Timedelta(minutes=30),
                    'bars': 40,
                },
            ]
        )

        with patch.object(run, 'PAIRS', {'EURUSD': {'ticker': 'EURUSD=X'}}), \
                patch('fx_sr.fill_pipeline.init_db'), \
                patch('fx_sr.fill_pipeline.get_cache_summary', return_value=summary):
            gaps = run._find_cache_gap_work_items(
                    target_days=365,
                    now=now,
                )
        expected_gap_start_1d = pd.Timestamp('2025-04-14T00:00:00Z')
        expected_gap_start_1h = pd.Timestamp('2025-04-13T21:15:00Z')
        expected_gap_start_1m = pd.Timestamp('2025-04-13T21:15:00Z')

        self.assertEqual(
            set(gaps),
            {
                ('EURUSD', 'EURUSD=X', '1d', expected_gap_start_1d),
                ('EURUSD', 'EURUSD=X', '1h', expected_gap_start_1h),
                ('EURUSD', 'EURUSD=X', '1m', expected_gap_start_1m),
            },
        )

    def test_find_cache_gaps_verbose_tolerates_mixed_timezone_summary_timestamps(self):
        now = pd.Timestamp('2026-04-13T12:00:00Z')
        summary = pd.DataFrame(
            [
                {
                    'ticker': 'EURUSD=X',
                    'interval': '1d',
                    'first_ts': now - pd.Timedelta(days=365),
                    'last_ts': now - pd.Timedelta(hours=2),
                    'bars': 80,
                },
                {
                    'ticker': 'EURUSD=X',
                    'interval': '1h',
                    'first_ts': pd.Timestamp('2026-03-14 00:00:00', tz='Europe/London'),
                    'last_ts': pd.Timestamp('2026-04-12 23:59:00', tz='Europe/London'),
                    'bars': 200,
                },
                {
                    'ticker': 'EURUSD=X',
                    'interval': '1m',
                    'first_ts': pd.Timestamp('2026-03-14 00:00:00', tz='Europe/London'),
                    'last_ts': pd.Timestamp('2026-04-12 23:59:00', tz='Europe/London'),
                    'bars': 40,
                },
            ]
        )

        with patch.object(run, 'PAIRS', {'EURUSD': {'ticker': 'EURUSD=X'}}), \
                patch('fx_sr.fill_pipeline.init_db'), \
                patch('fx_sr.fill_pipeline.get_cache_summary', return_value=summary), \
                patch('fx_sr.fill_pipeline._remaining_days_to_fetch', return_value=1):
            gaps_verbose = run._find_cache_gaps_verbose(target_days=365, now=now)

        self.assertEqual(len(gaps_verbose), 3)
        for pair_id, _ticker, interval, detail in gaps_verbose:
            self.assertEqual(pair_id, 'EURUSD')
            self.assertIn(interval, {'1d', '1h', '1m'})
            self.assertIsInstance(detail, str)


if __name__ == '__main__':
    unittest.main()
