import unittest

import pandas as pd

from fx_sr.strategy import StrategyParams, Trade, check_exit, get_market_exit_price


def _trade(direction: str) -> Trade:
    if direction == 'LONG':
        return Trade(
            entry_time=pd.Timestamp('2026-02-03 09:00:00', tz='UTC'),
            entry_price=1.1000,
            direction='LONG',
            sl_price=1.0950,
            tp_price=1.1100,
            zone_upper=1.1010,
            zone_lower=1.0990,
            zone_strength='major',
            risk=0.0050,
        )
    return Trade(
        entry_time=pd.Timestamp('2026-02-03 09:00:00', tz='UTC'),
        entry_price=1.1000,
        direction='SHORT',
        sl_price=1.1050,
        tp_price=1.0900,
        zone_upper=1.1010,
        zone_lower=1.0990,
        zone_strength='major',
        risk=0.0050,
    )


class StrategyExitTests(unittest.TestCase):
    def test_sideways_exit_skipped_while_long_is_slightly_profitable(self):
        """Slightly profitable trades should NOT be cut via sideways — let winners run."""
        params = StrategyParams(sideways_bars=3, sideways_threshold=0.5)

        result = check_exit(
            _trade('LONG'),
            bar_high=1.1003,
            bar_low=1.0998,
            bar_close=1.1001,
            bar_time=pd.Timestamp('2026-02-03 12:00:00', tz='UTC'),
            bars_held=3,
            params=params,
            pip=0.0001,
        )

        self.assertIsNone(result)

    def test_sideways_exit_fires_when_long_is_losing(self):
        """Losing trades should be cut via sideways exit."""
        params = StrategyParams(sideways_bars=3, sideways_threshold=0.5)

        result = check_exit(
            _trade('LONG'),
            bar_high=1.0999,
            bar_low=1.0990,
            bar_close=1.0995,
            bar_time=pd.Timestamp('2026-02-03 12:00:00', tz='UTC'),
            bars_held=3,
            params=params,
            pip=0.0001,
        )

        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'SIDEWAYS')

    def test_time_exit_skipped_while_short_is_slightly_profitable(self):
        """Slightly profitable trades should NOT be cut via time exit — let winners run."""
        params = StrategyParams(max_hold_bars=3)

        result = check_exit(
            _trade('SHORT'),
            bar_high=1.1002,
            bar_low=1.0995,
            bar_close=1.0998,
            bar_time=pd.Timestamp('2026-02-03 12:00:00', tz='UTC'),
            bars_held=3,
            params=params,
            pip=0.0001,
        )

        self.assertIsNone(result)

    def test_time_exit_fires_when_short_is_losing(self):
        """Losing short trades should be cut via time exit."""
        params = StrategyParams(max_hold_bars=3)

        result = check_exit(
            _trade('SHORT'),
            bar_high=1.1010,
            bar_low=1.1002,
            bar_close=1.1005,
            bar_time=pd.Timestamp('2026-02-03 12:00:00', tz='UTC'),
            bars_held=3,
            params=params,
            pip=0.0001,
        )

        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'TIME')


if __name__ == '__main__':
    unittest.main()
