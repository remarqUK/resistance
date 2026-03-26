import unittest

import pandas as pd

from fx_sr.execution import historical_execution_quote, quote_age_seconds
from fx_sr.ibkr import ExecutionQuote
from fx_sr.strategy import StrategyParams


class ExecutionTimingTests(unittest.TestCase):
    def test_quote_age_seconds_rejects_future_quote(self):
        quote = ExecutionQuote(
            pair='EURUSD',
            bid=1.0999,
            ask=1.1001,
            mid=1.1000,
            spread=0.0002,
            source='historical_l2',
            captured_at=pd.Timestamp('2026-03-16 00:16:33.929433', tz='UTC'),
        )

        age = quote_age_seconds(
            quote,
            now=pd.Timestamp('2025-05-20 07:00:00', tz='UTC'),
        )

        self.assertEqual(age, float('inf'))

    def test_historical_execution_quote_ignores_future_l2_snapshot(self):
        params = StrategyParams(
            spread_pips=0.6,
            max_submit_quote_age_seconds=2.0,
        )
        submit_ts = pd.Timestamp('2025-05-20 07:00:00', tz='UTC')
        l2_snapshots = pd.DataFrame(
            [
                {
                    'best_bid': 1.0999,
                    'best_ask': 1.1001,
                    'mid_price': 1.1000,
                }
            ],
            index=[pd.Timestamp('2026-03-16 00:16:33.929433', tz='UTC')],
        )

        quote, note = historical_execution_quote(
            'EURUSD',
            submit_ts,
            params,
            l2_snapshots=l2_snapshots,
            allow_h1_fallback=True,
            fallback_mid_price=1.1010,
        )

        self.assertEqual(note, '')
        self.assertIsNotNone(quote)
        self.assertEqual(quote.source, 'historical_1h_fallback')
        self.assertEqual(pd.Timestamp(quote.captured_at), submit_ts)


if __name__ == '__main__':
    unittest.main()
