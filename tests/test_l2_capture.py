import sqlite3
import unittest
from unittest.mock import patch

import pandas as pd

from fx_sr import db
from fx_sr.l2 import capture_l2_stream, format_l2_snapshot


def _snapshot(ts: str, pair: str = 'EURUSD', ticker: str = 'EURUSD=X') -> dict:
    return {
        'pair': pair,
        'ticker': ticker,
        'captured_at': pd.Timestamp(ts, tz='UTC'),
        'depth_requested': 2,
        'best_bid': 1.1000,
        'best_ask': 1.1002,
        'mid_price': 1.1001,
        'spread': 0.0002,
        'bids': [
            {'side': 'BID', 'level': 1, 'price': 1.1000, 'size': 2_000_000.0, 'market_maker': 'A'},
            {'side': 'BID', 'level': 2, 'price': 1.0999, 'size': 1_500_000.0, 'market_maker': 'B'},
        ],
        'asks': [
            {'side': 'ASK', 'level': 1, 'price': 1.1002, 'size': 1_800_000.0, 'market_maker': 'C'},
            {'side': 'ASK', 'level': 2, 'price': 1.1003, 'size': 1_200_000.0, 'market_maker': 'D'},
        ],
    }


class L2CaptureTests(unittest.TestCase):
    def setUp(self):
        self.db_path = 'file:test_l2_capture?mode=memory&cache=shared'
        self._conn = sqlite3.connect(self.db_path, uri=True)

    def tearDown(self):
        self._conn.close()

    def test_save_and_load_l2_snapshot_round_trip(self):
        snapshot_id = db.save_l2_snapshot(
            ticker='EURUSD=X',
            pair='EURUSD',
            captured_at='2026-03-12 10:00:00+00:00',
            bids=_snapshot('2026-03-12 10:00:00+00:00')['bids'],
            asks=_snapshot('2026-03-12 10:00:00+00:00')['asks'],
            depth_requested=2,
            mid_price=1.1001,
            db_path=self.db_path,
        )

        self.assertGreater(snapshot_id, 0)

        snapshots = db.load_l2_snapshots('EURUSD=X', db_path=self.db_path)
        levels = db.load_l2_levels('EURUSD=X', db_path=self.db_path)
        summary = db.get_l2_summary(ticker='EURUSD=X', db_path=self.db_path)

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(int(snapshots.iloc[0]['bid_levels']), 2)
        self.assertEqual(int(snapshots.iloc[0]['ask_levels']), 2)
        self.assertEqual(len(levels), 4)
        self.assertEqual(set(levels['side']), {'ASK', 'BID'})
        self.assertEqual(int(summary.iloc[0]['snapshots']), 1)

    def test_capture_l2_stream_persists_until_max_snapshots(self):
        emitted = [
            _snapshot('2026-03-12 10:00:00+00:00'),
            _snapshot('2026-03-12 10:00:01+00:00'),
            _snapshot('2026-03-12 10:00:02+00:00'),
        ]

        def fake_stream(pairs, on_snapshot, stop_event, depth, interval_seconds, client_id=None):
            for snapshot in emitted:
                if stop_event.is_set():
                    break
                on_snapshot(snapshot)

        with patch('fx_sr.l2.ibkr.stream_market_depth', side_effect=fake_stream):
            stats = capture_l2_stream(
                {'EURUSD': {'ticker': 'EURUSD=X'}},
                depth=2,
                interval_seconds=0.1,
                duration_seconds=None,
                max_snapshots=2,
                db_path=self.db_path,
            )

        summary = db.get_l2_summary(ticker='EURUSD=X', db_path=self.db_path)
        self.assertEqual(stats['snapshots_saved'], 2)
        self.assertEqual(stats['snapshots_per_pair'], {'EURUSD': 2})
        self.assertEqual(int(summary.iloc[0]['snapshots']), 2)

    def test_format_l2_snapshot_renders_depth_book(self):
        rendered = format_l2_snapshot(_snapshot('2026-03-12 10:00:00+00:00'))
        self.assertIn('L2 SNAPSHOT EURUSD', rendered)
        self.assertIn('1.100000', rendered)
        self.assertIn('1.100200', rendered)


if __name__ == '__main__':
    unittest.main()
