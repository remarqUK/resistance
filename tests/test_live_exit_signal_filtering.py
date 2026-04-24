import unittest
from types import SimpleNamespace
from unittest.mock import patch
from dataclasses import replace

import pandas as pd

from fx_sr import live as live_module
from fx_sr.live import PairScanRow, StrategyParams


def _freeze_datetime_now(value):
    class _FrozenDateTime:
        @classmethod
        def now(cls, tz=None):  # pragma: no cover - tiny test helper
            return value

    return patch('fx_sr.live.datetime', _FrozenDateTime)


class ExitSignalFilterTests(unittest.TestCase):
    def test_tracked_pair_state_blocks_recent_exit_signal(self):
        tracked_now = pd.Timestamp('2026-04-24 12:00:00', tz='UTC')
        tracked = {
            'AUDJPY:LONG': {
                'pair': 'AUDJPY',
                'trade': SimpleNamespace(direction='LONG'),
                'signal_status': 'EXIT_SIGNAL',
                'pending_exit_detected_at': tracked_now - pd.Timedelta(minutes=30),
            },
        }

        with patch('fx_sr.live._EXIT_SIGNAL_BARRIER_SECONDS', 3600), _freeze_datetime_now(
            tracked_now.to_pydatetime()
        ):
            tracked_pairs, tracked_states = live_module._tracked_pair_state_for_scan(tracked)

        self.assertEqual(tracked_pairs, {'AUDJPY': {'LONG'}})
        self.assertEqual(tracked_states, {'AUDJPY': 'OPEN'})

        blocked_pairs = live_module._tracked_pair_set_for_execution(tracked)
        self.assertEqual(blocked_pairs, set())

    def test_tracked_pair_state_ignores_expired_exit_signal(self):
        tracked_now = pd.Timestamp('2026-04-24 12:00:00', tz='UTC')
        tracked = {
            'AUDJPY:LONG': {
                'pair': 'AUDJPY',
                'trade': SimpleNamespace(direction='LONG'),
                'signal_status': 'EXIT_SIGNAL',
                'pending_exit_detected_at': tracked_now - pd.Timedelta(hours=2),
            },
        }

        with patch('fx_sr.live._EXIT_SIGNAL_BARRIER_SECONDS', 3600), _freeze_datetime_now(
            tracked_now.to_pydatetime()
        ):
            tracked_pairs, tracked_states = live_module._tracked_pair_state_for_scan(tracked)

        self.assertEqual(tracked_pairs, {})
        self.assertEqual(tracked_states, {})
        self.assertEqual(live_module._tracked_pair_set_for_execution(tracked), set())

    def test_collect_scan_rows_checks_exit_signal_barrier(self):
        tracked_now = pd.Timestamp('2026-04-24 12:00:00', tz='UTC')
        row = PairScanRow(
            pair='AUDJPY',
            name='AUD/JPY',
            decimals=3,
            price=100.0,
            state='INSIDE',
            note='No signal',
            support_text='-',
            resistance_text='-',
        )
        tracked = {
            'AUDJPY:LONG': {
                'pair': 'AUDJPY',
                'trade': SimpleNamespace(direction='LONG'),
                'signal_status': 'EXIT_SIGNAL',
                'pending_exit_detected_at': tracked_now - pd.Timedelta(minutes=30),
            },
        }
        pairs = {'AUDJPY': {'ticker': 'AUDJPY=X', 'name': 'AUD/JPY', 'decimals': 3}}
        params = StrategyParams(use_pair_direction_filter=False)

        observed: list[tuple[str, dict[str, set[str]], dict[str, str]]] = []

        def _scan(pair_id, pair_info, params, zone_history_days, tracked_pairs, tracked_states, blocked_pairs, **_):
            observed.append((pair_id, dict(tracked_pairs), dict(tracked_states)))
            return (
                replace(
                    row,
                    state='Tracked position (LONG)' if pair_id in tracked_pairs else 'INSIDE',
                    note='Tracked position' if pair_id in tracked_pairs else 'No signal',
                ),
                None,
                [],
            )

        with patch('fx_sr.live._scan_pair', side_effect=_scan) as scan_pair, _freeze_datetime_now(
            tracked_now.to_pydatetime()
        ):
            _, rows, _ = live_module.collect_scan_rows(
                pairs=pairs,
                params=params,
                zone_history_days=180,
                tracked_positions=tracked,
                minute_data_cache={},
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].state, 'Tracked position (LONG)')
        self.assertEqual(observed, [('AUDJPY', {'AUDJPY': {'LONG'}}, {'AUDJPY': 'OPEN'})])

        expired_tracked = {
            'AUDJPY:LONG': {
                'pair': 'AUDJPY',
                'trade': SimpleNamespace(direction='LONG'),
                'signal_status': 'EXIT_SIGNAL',
                'pending_exit_detected_at': tracked_now - pd.Timedelta(hours=2),
            },
        }
        observed.clear()
        with patch('fx_sr.live._scan_pair', side_effect=_scan), _freeze_datetime_now(
            tracked_now.to_pydatetime()
        ):
            _, rows, _ = live_module.collect_scan_rows(
                pairs=pairs,
                params=params,
                zone_history_days=180,
                tracked_positions=expired_tracked,
                minute_data_cache={},
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].pair, 'AUDJPY')
        self.assertEqual(observed, [('AUDJPY', {}, {})])

    def test_collect_scan_rows_accepts_precomputed_tracked_pairs_shape(self):
        row = PairScanRow(
            pair='AUDJPY',
            name='AUD/JPY',
            decimals=3,
            price=100.0,
            state='INSIDE',
            note='No signal',
            support_text='-',
            resistance_text='-',
        )
        pairs = {'AUDJPY': {'ticker': 'AUDJPY=X', 'name': 'AUD/JPY', 'decimals': 3}}
        tracked_positions = {'AUDJPY': {'LONG', 'SHORT'}}
        observed: list[tuple[dict[str, set[str]], dict[str, str]]] = []

        def _scan(pair_id, pair_info, params, zone_history_days, tracked_pairs, tracked_states, blocked_pairs, **_):
            observed.append((dict(tracked_pairs), dict(tracked_states)))
            return (replace(row, state='ok'), None, [])

        with patch('fx_sr.live._scan_pair', side_effect=_scan):
            _, rows, _ = live_module.collect_scan_rows(
                pairs=pairs,
                params=StrategyParams(use_pair_direction_filter=False),
                zone_history_days=180,
                tracked_positions=tracked_positions,
                minute_data_cache={},
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(observed, [({'AUDJPY': {'LONG', 'SHORT'}}, {'AUDJPY': 'OPEN'})])


if __name__ == '__main__':
    unittest.main()
