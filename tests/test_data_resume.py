import unittest

import pandas as pd

from fx_sr.data import _remaining_days_to_fetch


class RemainingDaysToFetchTests(unittest.TestCase):
    def test_no_cache_fetches_entire_request(self):
        now = pd.Timestamp('2026-03-16T12:00:00Z')
        self.assertEqual(
            _remaining_days_to_fetch(
                interval='1d',
                requested_days=30,
                cached_range=None,
                now=now,
            ),
            30,
        )

    def test_full_daily_coverage_requires_no_fetch(self):
        now = pd.Timestamp('2026-03-16T12:00:00Z')
        start = now - pd.Timedelta(days=365)
        end = now - pd.Timedelta(minutes=30)
        self.assertEqual(
            _remaining_days_to_fetch(
                interval='1h',
                requested_days=365,
                cached_range=(start, end, 100000),
                now=now,
            ),
            0,
        )

    def test_hourly_cache_gapped_since_last_bar_fetches_tail_days(self):
        now = pd.Timestamp('2026-03-16T12:00:00Z')
        start = now - pd.Timedelta(days=365)
        end = now - pd.Timedelta(hours=30)
        self.assertEqual(
            _remaining_days_to_fetch(
                interval='1h',
                requested_days=365,
                cached_range=(start, end, 100000),
                now=now,
            ),
            2,
        )

    def test_hourly_cache_starts_too_recent_for_full_request(self):
        now = pd.Timestamp('2026-03-16T12:00:00Z')
        start = now - pd.Timedelta(days=100)
        end = now - pd.Timedelta(hours=10)
        self.assertEqual(
            _remaining_days_to_fetch(
                interval='1h',
                requested_days=365,
                cached_range=(start, end, 2000),
                now=now,
            ),
            365,
        )

    def test_daily_gap_beyond_one_bar_fetches_days_needed(self):
        now = pd.Timestamp('2026-03-16T12:00:00Z')
        start = now - pd.Timedelta(days=365)
        end = now - pd.Timedelta(days=3, hours=1)
        self.assertEqual(
            _remaining_days_to_fetch(
                interval='1d',
                requested_days=365,
                cached_range=(start, end, 200),
                now=now,
            ),
            4,
        )


if __name__ == '__main__':
    unittest.main()
