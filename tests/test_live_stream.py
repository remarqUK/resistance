"""Tests for the tick-reactive streaming scanner."""

import unittest
from unittest.mock import patch

import pandas as pd

from fx_sr.live_stream import StreamingScanner, check_tick_exit
from fx_sr.strategy import StrategyParams, Trade, Signal
from fx_sr.levels import SRZone


def _trade(direction='LONG', entry=1.1000, sl=1.0950, tp=1.1100,
           zone_lower=1.0990, zone_upper=1.1010):
    risk = entry - sl if direction == 'LONG' else sl - entry
    return Trade(
        entry_time=pd.Timestamp('2026-02-03 10:00', tz='UTC'),
        entry_price=entry,
        direction=direction,
        sl_price=sl,
        tp_price=tp,
        zone_upper=zone_upper,
        zone_lower=zone_lower,
        zone_strength='major',
        risk=risk,
    )


def _zone(lower, upper, zone_type='support'):
    return SRZone(
        lower=lower,
        upper=upper,
        midpoint=(lower + upper) / 2,
        zone_type=zone_type,
        touches=3,
        strength='major',
    )


class TickExitTests(unittest.TestCase):

    def test_tp_hit_long(self):
        trade = _trade('LONG', entry=1.1000, sl=1.0950, tp=1.1100)
        result = check_tick_exit(trade, 1.1105, 0.0001, StrategyParams())
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'TP')

    def test_sl_hit_long(self):
        trade = _trade('LONG', entry=1.1000, sl=1.0950, tp=1.1100)
        result = check_tick_exit(trade, 1.0945, 0.0001, StrategyParams())
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'SL')

    def test_no_exit_mid_range(self):
        trade = _trade('LONG', entry=1.1000, sl=1.0950, tp=1.1100)
        result = check_tick_exit(trade, 1.1050, 0.0001, StrategyParams())
        self.assertIsNone(result)

    def test_tp_hit_short(self):
        trade = _trade('SHORT', entry=1.1000, sl=1.1050, tp=1.0900)
        result = check_tick_exit(trade, 1.0895, 0.0001, StrategyParams())
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'TP')

    def test_sl_hit_short(self):
        trade = _trade('SHORT', entry=1.1000, sl=1.1050, tp=1.0900)
        result = check_tick_exit(trade, 1.1055, 0.0001, StrategyParams())
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'SL')

    def test_zone_break_long(self):
        trade = _trade('LONG', entry=1.1000, sl=1.0950, tp=1.1100,
                        zone_lower=1.0990)
        result = check_tick_exit(trade, 1.0985, 0.0001, StrategyParams())
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'EARLY_EXIT')

    def test_zone_break_short(self):
        trade = _trade('SHORT', entry=1.1000, sl=1.1050, tp=1.0900,
                        zone_upper=1.1010)
        result = check_tick_exit(trade, 1.1015, 0.0001, StrategyParams())
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'EARLY_EXIT')


class StreamingScannerGateTests(unittest.TestCase):

    def _scanner_with_zones(self):
        scanner = StreamingScanner(
            params=StrategyParams(),
            eval_cooldown_seconds=0,
        )
        support = _zone(1.0990, 1.1010, 'support')
        resistance = _zone(1.1090, 1.1110, 'resistance')
        scanner._zones['EURUSD'] = (support, resistance, [support, resistance])
        return scanner

    def test_near_zone_returns_true_when_inside(self):
        scanner = self._scanner_with_zones()
        self.assertTrue(scanner._is_near_zone('EURUSD', 1.1000))

    def test_near_zone_returns_true_when_close(self):
        scanner = self._scanner_with_zones()
        # 0.20% above zone upper (1.1010) -> ~1.1032
        self.assertTrue(scanner._is_near_zone('EURUSD', 1.1030))

    def test_near_zone_returns_false_when_far(self):
        scanner = self._scanner_with_zones()
        # Midway between support and resistance
        self.assertFalse(scanner._is_near_zone('EURUSD', 1.1050))

    def test_on_tick_skips_far_price(self):
        scanner = self._scanner_with_zones()
        result = scanner.on_tick('EURUSD', 1.1050)
        self.assertIsNone(result)

    def test_on_tick_deduplicates_same_signal(self):
        scanner = self._scanner_with_zones()
        scanner.eval_cooldown_seconds = 0

        signal = Signal(
            time=pd.Timestamp('2026-02-03 10:00', tz='UTC'),
            pair='EURUSD', direction='LONG',
            entry_price=1.1000, sl_price=1.0950, tp_price=1.1100,
            zone_upper=1.1010, zone_lower=1.0990,
            zone_strength='major', zone_type='support',
        )

        with patch.object(scanner, '_evaluate_signal', return_value=signal):
            first = scanner.on_tick('EURUSD', 1.1000)
            second = scanner.on_tick('EURUSD', 1.1001)

        self.assertIsNotNone(first)
        self.assertIsNone(second)  # deduplicated

    def test_check_tick_exits_returns_alert(self):
        scanner = self._scanner_with_zones()
        tracked = {
            'EURUSD:LONG': {
                'pair': 'EURUSD',
                'trade': _trade('LONG', entry=1.1000, sl=1.0950, tp=1.1100),
                'bars_monitored': 5,
            }
        }
        alerts = scanner.check_tick_exits('EURUSD', 1.1105, tracked)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]['exit_reason'], 'TP')
        self.assertEqual(alerts[0]['source'], 'tick')

    def test_check_tick_exits_no_alert_mid_range(self):
        scanner = self._scanner_with_zones()
        tracked = {
            'EURUSD:LONG': {
                'pair': 'EURUSD',
                'trade': _trade('LONG', entry=1.1000, sl=1.0950, tp=1.1100),
                'bars_monitored': 5,
            }
        }
        alerts = scanner.check_tick_exits('EURUSD', 1.1050, tracked)
        self.assertEqual(len(alerts), 0)


if __name__ == '__main__':
    unittest.main()
