import unittest

import fx_sr.db as db_module
from tests._test_db_helpers import temporary_test_database


class DbHelperTests(unittest.TestCase):
    def setUp(self):
        self._db_ctx = temporary_test_database()
        self.db_path = self._db_ctx.__enter__()
        db_module.init_db(self.db_path)

    def tearDown(self):
        if self._db_ctx is not None:
            self._db_ctx.__exit__(None, None, None)

    def test_connect_and_backend_detection(self):
        with db_module._connect(self.db_path) as conn:
            self.assertEqual(conn.backend, 'postgres')

    def test_db_transaction_commits_and_rolls_back(self):
        with db_module.db_transaction(self.db_path) as conn:
            conn.execute("CREATE TABLE sample (id SERIAL PRIMARY KEY, value TEXT)")
            conn.execute("INSERT INTO sample (value) VALUES ('ok')")

        with self.assertRaises(RuntimeError):
            with db_module.db_transaction(self.db_path) as conn:
                conn.execute("INSERT INTO sample (value) VALUES ('nope')")
                raise RuntimeError('boom')

        conn = db_module._connect(self.db_path)
        try:
            rows = conn.execute("SELECT value FROM sample ORDER BY id").fetchall()
        finally:
            conn.close()

        self.assertEqual(rows, [('ok',)])

    def test_l2_load_helpers_round_trip(self):
        db_module.save_l2_snapshot(
            ticker='EURUSD=X',
            pair='EURUSD',
            captured_at='2026-03-12 10:00:00+00:00',
            bids=[
                {'level': 1, 'price': 1.1000, 'size': 2_000_000.0, 'market_maker': 'A'},
                {'level': 2, 'price': 1.0999, 'size': 1_500_000.0, 'market_maker': 'B'},
            ],
            asks=[
                {'level': 1, 'price': 1.1002, 'size': 1_800_000.0, 'market_maker': 'C'},
                {'level': 2, 'price': 1.1003, 'size': 1_200_000.0, 'market_maker': 'D'},
            ],
            depth_requested=2,
            mid_price=1.1001,
            db_path=self.db_path,
        )

        levels = db_module.load_l2_levels('EURUSD=X', db_path=self.db_path)
        summary = db_module.get_l2_summary(ticker='EURUSD=X', db_path=self.db_path)

        self.assertEqual(len(levels), 4)
        self.assertEqual(set(levels['side']), {'ASK', 'BID'})
        self.assertEqual(int(summary.iloc[0]['snapshots']), 1)


if __name__ == "__main__":
    unittest.main()
