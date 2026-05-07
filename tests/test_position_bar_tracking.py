import unittest
from contextlib import contextmanager
from unittest.mock import Mock, patch

import pandas as pd

from fx_sr.positions import (
    _resolve_closed_position_details,
    check_position_exits,
    sync_positions,
)
from fx_sr.strategy import StrategyParams, Trade


def _build_hourly_df(rows: list[tuple[str, float, float, float, float]]) -> pd.DataFrame:
    index = pd.DatetimeIndex([pd.Timestamp(ts, tz='UTC') for ts, *_ in rows])
    return pd.DataFrame(
        {
            'Open': [row[1] for row in rows],
            'High': [row[2] for row in rows],
            'Low': [row[3] for row in rows],
            'Close': [row[4] for row in rows],
            'Volume': [0.0] * len(rows),
        },
        index=index,
    )


def _trade() -> Trade:
    return Trade(
        entry_time=pd.Timestamp('2026-02-03 09:00:00', tz='UTC'),
        entry_price=1.1000,
        direction='LONG',
        sl_price=1.0950,
        tp_price=1.1100,
        zone_upper=1.1010,
        zone_lower=1.1000,
        zone_strength='major',
        risk=0.0050,
    )


def _short_trade() -> Trade:
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


@contextmanager
def _tracking_transaction(conn):
    yield conn


class PositionBarTrackingTests(unittest.TestCase):
    def test_initial_scan_marks_latest_bar_without_aging_trade(self):
        tracked = {
            'EURUSD:LONG': {
                'pair': 'EURUSD',
                'trade': _trade(),
                'bars_monitored': 0,
                'ibkr_size': 10000,
                'last_processed_bar_time': None,
            }
        }
        hourly_df = _build_hourly_df(
            [
                ('2026-02-03 10:00:00', 1.1000, 1.1010, 1.0990, 1.1005),
            ]
        )

        with patch('fx_sr.positions.fetch_hourly_data', return_value=hourly_df), \
                patch('fx_sr.positions.check_exit', return_value=None) as check_exit_mock, \
                patch('fx_sr.positions._save_bar_tracking') as save_tracking_mock:
            alerts, snapshots = check_position_exits(tracked, StrategyParams())

        self.assertEqual(alerts, [])
        self.assertIn('EURUSD:LONG', snapshots)
        self.assertEqual(check_exit_mock.call_count, 1)
        self.assertEqual(check_exit_mock.call_args.kwargs['bars_held'], 0)
        self.assertEqual(tracked['EURUSD:LONG']['bars_monitored'], 0)
        self.assertEqual(tracked['EURUSD:LONG']['last_processed_bar_time'], hourly_df.index[-1])
        save_tracking_mock.assert_called_once_with('EURUSD', 'LONG', 0, hourly_df.index[-1])

    def test_only_new_hourly_bars_advance_trade_age_and_trigger_exit_checks(self):
        tracked = {
            'EURUSD:LONG': {
                'pair': 'EURUSD',
                'trade': _trade(),
                'bars_monitored': 0,
                'ibkr_size': 10000,
                'last_processed_bar_time': pd.Timestamp('2026-02-03 10:00:00', tz='UTC'),
            }
        }
        hourly_df = _build_hourly_df(
            [
                ('2026-02-03 10:00:00', 1.1000, 1.1010, 1.0990, 1.1005),
                ('2026-02-03 11:00:00', 1.1005, 1.1015, 1.1000, 1.1010),
                ('2026-02-03 12:00:00', 1.1010, 1.1012, 1.0995, 1.1002),
                ('2026-02-03 13:00:00', 1.1002, 1.1006, 1.0985, 1.0990),
            ]
        )

        with patch('fx_sr.positions.fetch_hourly_data', return_value=hourly_df), \
                patch(
                    'fx_sr.positions.check_exit',
                    side_effect=[None, ('TIME', 1.1000)],
                ) as check_exit_mock, \
                patch('fx_sr.positions._save_bar_tracking') as save_tracking_mock:
            alerts, _ = check_position_exits(tracked, StrategyParams())

        self.assertEqual(check_exit_mock.call_count, 2)
        self.assertEqual(check_exit_mock.call_args_list[0].kwargs['bars_held'], 1)
        self.assertEqual(check_exit_mock.call_args_list[1].kwargs['bars_held'], 2)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]['exit_reason'], 'TIME')
        self.assertEqual(alerts[0]['bars_monitored'], 2)
        self.assertEqual(tracked['EURUSD:LONG']['bars_monitored'], 2)
        self.assertEqual(tracked['EURUSD:LONG']['last_processed_bar_time'], hourly_df.index[2])
        save_tracking_mock.assert_called_once_with('EURUSD', 'LONG', 2, hourly_df.index[2])

        with patch('fx_sr.positions.fetch_hourly_data', return_value=hourly_df), \
                patch('fx_sr.positions.check_exit', return_value=None) as check_exit_mock, \
                patch('fx_sr.positions._save_bar_tracking') as save_tracking_mock:
            alerts, _ = check_position_exits(tracked, StrategyParams())

        self.assertEqual(alerts, [])
        self.assertEqual(check_exit_mock.call_count, 1)
        self.assertEqual(check_exit_mock.call_args.kwargs['bars_held'], 3)
        self.assertEqual(tracked['EURUSD:LONG']['bars_monitored'], 3)
        self.assertEqual(tracked['EURUSD:LONG']['last_processed_bar_time'], hourly_df.index[-1])
        save_tracking_mock.assert_called_once_with('EURUSD', 'LONG', 3, hourly_df.index[-1])

    def test_short_position_skips_time_exit_when_profitable(self):
        """Profitable short should NOT be time-exited — let winners run."""
        tracked = {
            'EURUSD:SHORT': {
                'pair': 'EURUSD',
                'trade': _short_trade(),
                'bars_monitored': 0,
                'ibkr_size': -10000,
                'last_processed_bar_time': pd.Timestamp('2026-02-03 10:00:00', tz='UTC'),
            }
        }
        hourly_df = _build_hourly_df(
            [
                ('2026-02-03 10:00:00', 1.1005, 1.1008, 1.1000, 1.1004),
                ('2026-02-03 11:00:00', 1.1003, 1.1004, 1.0994, 1.0998),
            ]
        )
        params = StrategyParams(max_hold_bars=1)

        with patch('fx_sr.positions.fetch_hourly_data', return_value=hourly_df), \
                patch('fx_sr.positions._save_bar_tracking') as save_tracking_mock:
            alerts, snapshots = check_position_exits(tracked, params)

        self.assertEqual(len(alerts), 0)
        self.assertEqual(snapshots['EURUSD:SHORT']['current_price'], 1.0998)
        self.assertGreater(snapshots['EURUSD:SHORT']['pnl_pips'], 0.0)
        self.assertEqual(tracked['EURUSD:SHORT']['bars_monitored'], 1)

    def test_sync_positions_uses_actual_broker_entry_price_for_claimed_signal(self):
        signal_row = {
            'signal_id': 'sig-1',
            'pair': 'EURUSD',
            'direction': 'LONG',
            'signal_time': '2026-02-03 10:00:00+00:00',
            'opened_at': '2026-02-03 10:00:05+00:00',
            'opened_price': 1.1002,
            'entry_price': 1.1000,
            'sl_price': 1.0950,
            'tp_price': 1.1100,
            'zone_upper': 1.1010,
            'zone_lower': 1.1000,
            'zone_strength': 'major',
            'quality_score': 0.8,
        }

        conn = Mock()

        with patch('fx_sr.positions.reconcile_broker_ledger', return_value=[]), \
                patch('fx_sr.positions._load_trades', return_value={}), \
                patch('fx_sr.positions.load_open_broker_execution_positions', return_value=[]), \
                patch('fx_sr.positions.ibkr.fetch_positions', return_value=[{
                    'pair': 'EURUSD',
                    'size': 10000.0,
                    'avg_cost': 1.1002,
                }]), \
                patch('fx_sr.positions._tracking_db_transaction', return_value=_tracking_transaction(conn)), \
                patch('fx_sr.positions.claim_signal_for_position_conn', return_value=signal_row), \
                patch('fx_sr.positions._save_trade_conn') as save_trade_mock:
            tracked = sync_positions(StrategyParams())

        saved_trade = save_trade_mock.call_args.args[2]
        self.assertAlmostEqual(saved_trade.entry_price, 1.1002)
        self.assertEqual(saved_trade.entry_time, pd.Timestamp('2026-02-03 10:00:05+00:00'))
        self.assertAlmostEqual(saved_trade.risk, 0.0052)
        self.assertAlmostEqual(tracked['EURUSD:LONG']['trade'].entry_price, 1.1002)

    def test_sync_positions_tracks_broker_execution_exposure_when_positions_empty(self):
        signal_row = {
            'signal_id': 'sig-usdjpy',
            'pair': 'USDJPY',
            'direction': 'LONG',
            'signal_time': '2026-02-03 10:00:00+00:00',
            'opened_at': '2026-02-03 10:00:05+00:00',
            'opened_price': 159.40,
            'entry_price': 159.30,
            'submitted_entry_price': 159.40,
            'sl_price': 159.00,
            'submitted_sl_price': 159.00,
            'tp_price': 160.00,
            'submitted_tp_price': 160.00,
            'zone_upper': 159.45,
            'zone_lower': 159.17,
            'zone_strength': 'major',
            'quality_score': 0.8,
            'status': 'OPEN',
        }
        broker_position = {
            'pair': 'USDJPY',
            'direction': 'LONG',
            'size': 40000.0,
            'avg_cost': 159.40,
            'position_source': 'broker_execution',
            'signal_id': 'sig-usdjpy',
            'broker_fill_count': 2,
        }
        conn = Mock()

        with patch('fx_sr.positions.reconcile_broker_ledger', return_value=[]), \
                patch('fx_sr.positions._load_trades', return_value={}), \
                patch('fx_sr.positions.ibkr.fetch_positions', return_value=[]), \
                patch('fx_sr.positions.load_open_broker_execution_positions', return_value=[broker_position]), \
                patch('fx_sr.positions.load_neutralization_positions', return_value=set()), \
                patch('fx_sr.positions.ibkr.fetch_open_order_counts', return_value={}), \
                patch('fx_sr.positions._resubmit_missing_brackets'), \
                patch('fx_sr.positions._tracking_db_transaction', return_value=_tracking_transaction(conn)), \
                patch('fx_sr.positions.claim_signal_for_position_conn', return_value=signal_row), \
                patch('fx_sr.positions._save_trade_conn') as save_trade_mock:
            tracked = sync_positions(StrategyParams())

        self.assertIn('USDJPY:LONG', tracked)
        self.assertAlmostEqual(tracked['USDJPY:LONG']['ibkr_size'], 40000.0)
        self.assertAlmostEqual(tracked['USDJPY:LONG']['ibkr_avg_cost'], 159.40)
        self.assertEqual(tracked['USDJPY:LONG']['position_source'], 'broker_execution')
        save_trade_mock.assert_called_once()
        saved_trade = save_trade_mock.call_args.args[2]
        self.assertAlmostEqual(saved_trade.entry_price, 159.40)

    def test_sync_positions_does_not_submit_brackets_for_broker_execution_only_position(self):
        signal_row = {
            'signal_id': 'sig-usdjpy',
            'pair': 'USDJPY',
            'direction': 'LONG',
            'signal_time': '2026-02-03 10:00:00+00:00',
            'opened_at': '2026-02-03 10:00:05+00:00',
            'opened_price': 159.40,
            'entry_price': 159.30,
            'submitted_entry_price': 159.40,
            'sl_price': 159.00,
            'submitted_sl_price': 159.00,
            'tp_price': 160.00,
            'submitted_tp_price': 160.00,
            'zone_upper': 159.45,
            'zone_lower': 159.17,
            'zone_strength': 'major',
            'quality_score': 0.8,
            'status': 'OPEN',
        }
        broker_position = {
            'pair': 'USDJPY',
            'direction': 'LONG',
            'size': 20802.0,
            'avg_cost': 159.39,
            'position_source': 'broker_execution',
            'signal_id': 'sig-usdjpy',
            'broker_fill_count': 1,
        }
        conn = Mock()

        with patch('fx_sr.positions.reconcile_broker_ledger', return_value=[]), \
                patch('fx_sr.positions._load_trades', return_value={}), \
                patch('fx_sr.positions.ibkr.fetch_positions', return_value=[]), \
                patch('fx_sr.positions.load_open_broker_execution_positions', return_value=[broker_position]), \
                patch('fx_sr.positions.load_neutralization_positions', return_value=set()), \
                patch('fx_sr.positions.ibkr.fetch_open_order_counts', return_value={'USDJPY': 2}), \
                patch('fx_sr.positions._resubmit_missing_brackets') as resubmit_mock, \
                patch('fx_sr.positions._cancel_orders_for_pairs') as cancel_pairs_mock, \
                patch('fx_sr.positions._tracking_db_transaction', return_value=_tracking_transaction(conn)), \
                patch('fx_sr.positions.claim_signal_for_position_conn', return_value=signal_row), \
                patch('fx_sr.positions._save_trade_conn'):
            tracked = sync_positions(StrategyParams())

        self.assertIn('USDJPY:LONG', tracked)
        self.assertEqual(tracked['USDJPY:LONG']['position_source'], 'broker_execution')
        resubmit_mock.assert_not_called()
        cancel_pairs_mock.assert_not_called()

    def test_sync_positions_does_not_repeat_broker_ledger_message_for_tracked_exposure(self):
        broker_position = {
            'pair': 'USDJPY',
            'direction': 'LONG',
            'size': 20802.0,
            'avg_cost': 159.39,
            'position_source': 'broker_execution',
            'signal_id': 'sig-usdjpy',
            'broker_fill_count': 1,
        }
        existing_trade = _trade()

        with patch('fx_sr.positions.reconcile_broker_ledger', return_value=[]), \
                patch('fx_sr.positions._load_trades', return_value={
                    'USDJPY:LONG': {
                        'pair': 'USDJPY',
                        'trade': existing_trade,
                        'signal_id': None,
                        'ibkr_size': 20802.0,
                        'ibkr_avg_cost': 159.39,
                        'bars_monitored': 0,
                        'signal_status': 'OPEN',
                        'pending_exit_reason': None,
                        'pending_exit_price': None,
                        'pending_exit_detected_at': None,
                        'last_processed_bar_time': None,
                    }
                }), \
                patch('fx_sr.positions.ibkr.fetch_positions', return_value=[]), \
                patch('fx_sr.positions.load_open_broker_execution_positions', return_value=[broker_position]), \
                patch('fx_sr.positions.load_neutralization_positions', return_value=set()), \
                patch('fx_sr.positions.ibkr.fetch_open_order_counts', return_value={}), \
                patch('builtins.print') as print_mock:
            tracked = sync_positions(StrategyParams())

        self.assertIn('USDJPY:LONG', tracked)
        messages = [str(call.args[0]) for call in print_mock.call_args_list if call.args]
        self.assertFalse(any('Broker-ledger FX exposure detected' in msg for msg in messages))

    def test_resubmitted_brackets_keep_strategy_order_ref(self):
        signal_row = {
            'signal_id': 'sig-usdjpy',
            'pair': 'USDJPY',
            'direction': 'LONG',
            'signal_time': '2026-02-03 10:00:00+00:00',
            'submitted_tp_price': 160.0,
            'submitted_sl_price': 159.0,
            'tp_price': 160.0,
            'sl_price': 159.0,
        }

        with patch('fx_sr.positions.ibkr.fetch_open_order_counts', return_value={}), \
                patch('fx_sr.positions.ibkr.submit_bracket_for_existing_position', return_value={
                    'take_profit_order_id': 301,
                    'stop_loss_order_id': 302,
                }) as submit_mock, \
                patch('fx_sr.positions._update_signal_bracket_ids'):
            from fx_sr.positions import _resubmit_missing_brackets
            _resubmit_missing_brackets(signal_row, 10000.0)

        submit_mock.assert_called_once_with(
            pair='USDJPY',
            direction='LONG',
            quantity=10000,
            take_profit_price=160.0,
            stop_loss_price=159.0,
            order_ref='fxsr:USDJPY:LONG:20260203100000',
        )

    def test_sync_positions_prefers_live_net_position_over_opposing_broker_ledger(self):
        signal_row = {
            'signal_id': 'sig-usdjpy-short',
            'pair': 'USDJPY',
            'direction': 'SHORT',
            'signal_time': '2026-02-03 10:00:00+00:00',
            'opened_at': '2026-02-03 10:00:05+00:00',
            'opened_price': 159.73,
            'entry_price': 159.73,
            'submitted_entry_price': 159.73,
            'sl_price': 160.27,
            'submitted_sl_price': 160.27,
            'tp_price': 159.15,
            'submitted_tp_price': 159.15,
            'zone_upper': 160.00,
            'zone_lower': 159.50,
            'zone_strength': 'major',
            'quality_score': 0.8,
            'status': 'OPEN',
        }
        live_position = {
            'pair': 'USDJPY',
            'size': -416044.0,
            'avg_cost': 159.73005,
        }
        stale_broker_position = {
            'pair': 'USDJPY',
            'direction': 'LONG',
            'size': 104011.0,
            'avg_cost': 159.40598,
            'position_source': 'broker_execution',
            'signal_id': 'stale-long',
            'broker_fill_count': 5,
        }
        conn = Mock()

        with patch('fx_sr.positions.reconcile_broker_ledger', return_value=[]), \
                patch('fx_sr.positions._load_trades', return_value={}), \
                patch('fx_sr.positions.ibkr.fetch_positions', return_value=[live_position]), \
                patch('fx_sr.positions.load_open_broker_execution_positions', return_value=[stale_broker_position]), \
                patch('fx_sr.positions.load_neutralization_positions', return_value=set()), \
                patch('fx_sr.positions.ibkr.fetch_open_order_counts', return_value={}), \
                patch('fx_sr.positions._resubmit_missing_brackets'), \
                patch('fx_sr.positions._tracking_db_transaction', return_value=_tracking_transaction(conn)), \
                patch('fx_sr.positions.claim_signal_for_position_conn', return_value=signal_row), \
                patch('fx_sr.positions._save_trade_conn') as save_trade_mock:
            tracked = sync_positions(StrategyParams())

        self.assertEqual(set(tracked.keys()), {'USDJPY:SHORT'})
        self.assertAlmostEqual(tracked['USDJPY:SHORT']['ibkr_size'], -416044.0)
        self.assertEqual(tracked['USDJPY:SHORT']['position_source'], 'ibkr_position')
        save_trade_mock.assert_called_once()

    def test_sync_positions_marks_broker_take_profit_close(self):
        tracked = {
            'EURUSD:LONG': {
                'pair': 'EURUSD',
                'trade': _trade(),
                'bars_monitored': 0,
                'ibkr_size': 10000,
                'signal_id': 'sig-1',
                'pending_exit_reason': None,
                'pending_exit_price': None,
                'pending_exit_detected_at': None,
                'last_processed_bar_time': None,
            }
        }
        signal_row = {
            'signal_id': 'sig-1',
            'pair': 'EURUSD',
            'direction': 'LONG',
            'signal_time': '2026-02-03 10:00:00+00:00',
            'opened_at': '2026-02-03 10:00:05+00:00',
            'order_id': 101,
            'take_profit_order_id': 102,
            'stop_loss_order_id': 103,
        }

        conn = Mock()

        with patch('fx_sr.positions.reconcile_broker_ledger', return_value=[]), \
                patch('fx_sr.positions._load_trades', return_value=tracked), \
                patch('fx_sr.positions.load_open_broker_execution_positions', return_value=[]), \
                patch('fx_sr.positions.ibkr.fetch_positions', return_value=[]), \
                patch('fx_sr.positions.load_detected_signal', return_value=signal_row), \
                patch('fx_sr.positions.ibkr.fetch_fx_fills', return_value=[{
                    'order_id': 102,
                    'price': 1.1101,
                    'avg_price': 1.1101,
                    'side': 'SELL',
                }]), \
                patch('fx_sr.positions._tracking_db_transaction', return_value=_tracking_transaction(conn)), \
                patch('fx_sr.positions.record_closed_signal_conn') as close_mock, \
                patch('fx_sr.positions._remove_trade_conn') as remove_mock:
            result = sync_positions(StrategyParams())

        self.assertEqual(result, {})
        close_mock.assert_called_once_with(
            conn,
            'sig-1',
            close_reason='TP',
            close_price=1.1101,
            close_source='broker_tp',
        )
        remove_mock.assert_called_once_with(conn, 'EURUSD', 'LONG')

    def test_resolve_closed_position_prefers_manual_fill_when_child_orders_exist(self):
        info = {
            'pair': 'EURUSD',
            'trade': _trade(),
            'signal_id': 'sig-1',
        }
        signal_row = {
            'signal_id': 'sig-1',
            'pair': 'EURUSD',
            'direction': 'LONG',
            'signal_time': '2026-02-03 10:00:00+00:00',
            'opened_at': '2026-02-03 10:00:05+00:00',
            'order_id': 101,
            'take_profit_order_id': 102,
            'stop_loss_order_id': 103,
        }
        manual_fill = {
            'order_id': 999,
            'price': 1.1102,
            'avg_price': 1.1102,
            'side': 'SELL',
        }

        with patch('fx_sr.positions.load_detected_signal', return_value=signal_row), \
                patch('fx_sr.positions.ibkr.fetch_fx_fills', side_effect=[ [], [manual_fill] ]) as fill_mock, \
                patch('fx_sr.positions.ibkr.fetch_completed_fx_orders', return_value=[]):
            close_reason, close_price, close_source = _resolve_closed_position_details(info)

        self.assertEqual(close_reason, 'MANUAL')
        self.assertAlmostEqual(close_price, 1.1102)
        self.assertEqual(close_source, 'broker_fill')
        self.assertEqual(fill_mock.call_count, 2)

    def test_sync_positions_updates_existing_trade_after_partial_fill_grows(self):
        tracked = {
            'EURUSD:LONG': {
                'pair': 'EURUSD',
                'trade': _trade(),
                'bars_monitored': 2,
                'ibkr_avg_cost': 1.1001,
                'ibkr_size': 4000.0,
                'signal_id': 'sig-1',
                'signal_status': 'PARTIAL',
                'pending_exit_reason': None,
                'pending_exit_price': None,
                'pending_exit_detected_at': None,
                'last_processed_bar_time': pd.Timestamp('2026-02-03 11:00:00', tz='UTC'),
            }
        }
        signal_row = {
            'signal_id': 'sig-1',
            'pair': 'EURUSD',
            'direction': 'LONG',
            'signal_time': '2026-02-03 10:00:00+00:00',
            'opened_at': '2026-02-03 10:00:02+00:00',
            'opened_price': 1.1003,
            'entry_price': 1.1000,
            'sl_price': 1.0950,
            'tp_price': 1.1100,
            'submitted_tp_price': 1.1106,
            'submitted_sl_price': 1.0950,
            'zone_upper': 1.1010,
            'zone_lower': 1.1000,
            'zone_strength': 'major',
            'quality_score': 0.8,
            'status': 'OPEN',
        }
        conn = Mock()

        with patch('fx_sr.positions.reconcile_broker_ledger', return_value=[]), \
                patch('fx_sr.positions._load_trades', return_value=tracked), \
                patch('fx_sr.positions.load_open_broker_execution_positions', return_value=[]), \
                patch('fx_sr.positions.ibkr.fetch_positions', return_value=[{
                    'pair': 'EURUSD',
                    'size': 10000.0,
                    'avg_cost': 1.1003,
                }]), \
                patch('fx_sr.positions._tracking_db_transaction', return_value=_tracking_transaction(conn)), \
                patch('fx_sr.positions.claim_signal_for_position_conn', return_value=signal_row), \
                patch('fx_sr.positions._save_trade_conn') as save_trade_mock:
            updated = sync_positions(StrategyParams())

        saved_trade = save_trade_mock.call_args.args[2]
        self.assertAlmostEqual(saved_trade.entry_price, 1.1003)
        self.assertAlmostEqual(updated['EURUSD:LONG']['ibkr_size'], 10000.0)
        self.assertAlmostEqual(updated['EURUSD:LONG']['ibkr_avg_cost'], 1.1003)
        self.assertEqual(updated['EURUSD:LONG']['signal_status'], 'OPEN')
        self.assertEqual(updated['EURUSD:LONG']['bars_monitored'], 2)

    def test_sync_positions_emits_closed_signal_row_callback(self):
        tracked = {
            'EURUSD:LONG': {
                'pair': 'EURUSD',
                'trade': _trade(),
                'bars_monitored': 0,
                'ibkr_size': 10000,
                'signal_id': 'sig-1',
                'pending_exit_reason': None,
                'pending_exit_price': None,
                'pending_exit_detected_at': None,
                'last_processed_bar_time': None,
            }
        }
        signal_row = {
            'signal_id': 'sig-1',
            'pair': 'EURUSD',
            'direction': 'LONG',
            'signal_time': '2026-02-03 10:00:00+00:00',
            'opened_at': '2026-02-03 10:00:05+00:00',
        }
        closed_row = dict(signal_row, closed_at='2026-02-03 14:00:00+00:00', closed_price=1.1100)
        captured = []

        conn = Mock()

        with patch('fx_sr.positions.reconcile_broker_ledger', return_value=[]), \
                patch('fx_sr.positions._load_trades', return_value=tracked), \
                patch('fx_sr.positions.load_open_broker_execution_positions', return_value=[]), \
                patch('fx_sr.positions.ibkr.fetch_positions', return_value=[]), \
                patch('fx_sr.positions._resolve_closed_position_details', return_value=('MANUAL', 1.1100, 'broker_fill')), \
                patch('fx_sr.positions._tracking_db_transaction', return_value=_tracking_transaction(conn)), \
                patch('fx_sr.positions.record_closed_signal_conn', return_value=closed_row), \
                patch('fx_sr.positions._remove_trade_conn'):
            sync_positions(StrategyParams(), on_signal_closed=captured.append)

        self.assertEqual(captured, [closed_row])

    def test_sync_positions_uses_signal_history_exit_when_broker_has_no_fill_detail(self):
        tracked = {
            'EURUSD:LONG': {
                'pair': 'EURUSD',
                'trade': _trade(),
                'bars_monitored': 0,
                'ibkr_size': 10000,
                'signal_id': 'sig-1',
                'pending_exit_reason': None,
                'pending_exit_price': None,
                'pending_exit_detected_at': None,
                'last_processed_bar_time': None,
            }
        }
        signal_row = {
            'signal_id': 'sig-1',
            'pair': 'EURUSD',
            'direction': 'LONG',
            'signal_time': '2026-02-03 10:00:00+00:00',
            'opened_at': '2026-02-03 10:00:05+00:00',
            'order_id': 101,
            'take_profit_order_id': 102,
            'stop_loss_order_id': 103,
            'exit_signal_reason': 'REVERSAL',
            'exit_signal_price': 1.1045,
        }

        conn = Mock()

        with patch('fx_sr.positions.reconcile_broker_ledger', return_value=[]), \
                patch('fx_sr.positions._load_trades', return_value=tracked), \
                patch('fx_sr.positions.load_open_broker_execution_positions', return_value=[]), \
                patch('fx_sr.positions.ibkr.fetch_positions', return_value=[]), \
                patch('fx_sr.positions.load_detected_signal', return_value=signal_row), \
                patch('fx_sr.positions.ibkr.fetch_fx_fills', return_value=[]), \
                patch('fx_sr.positions.ibkr.fetch_completed_fx_orders', return_value=[]), \
                patch('fx_sr.positions.ibkr.fetch_latest_price', return_value=1.1045), \
                patch('fx_sr.positions._tracking_db_transaction', return_value=_tracking_transaction(conn)), \
                patch('fx_sr.positions.record_closed_signal_conn') as close_mock, \
                patch('fx_sr.positions._remove_trade_conn') as remove_mock:
            result = sync_positions(StrategyParams())

        self.assertEqual(result, {})
        close_mock.assert_called_once_with(
            conn,
            'sig-1',
            close_reason='REVERSAL',
            close_price=1.1045,
            close_source='market_price',
        )
        remove_mock.assert_called_once_with(conn, 'EURUSD', 'LONG')

    def test_sync_positions_rolls_back_claim_when_trade_cannot_be_built(self):
        signal_row = {
            'signal_id': 'sig-1',
            'pair': 'EURUSD',
            'direction': 'LONG',
            'signal_time': '2026-02-03 10:00:00+00:00',
            'opened_at': '2026-02-03 10:00:05+00:00',
            'opened_price': 1.1002,
            'entry_price': 1.1000,
            'sl_price': 1.0950,
            'tp_price': 1.1100,
            'zone_upper': 1.1010,
            'zone_lower': 1.1000,
            'zone_strength': 'major',
            'quality_score': 0.8,
        }
        conn = Mock()

        with patch('fx_sr.positions.reconcile_broker_ledger', return_value=[]), \
                patch('fx_sr.positions._load_trades', return_value={}), \
                patch('fx_sr.positions.load_open_broker_execution_positions', return_value=[]), \
                patch('fx_sr.positions.ibkr.fetch_positions', return_value=[{
                    'pair': 'EURUSD',
                    'size': 10000.0,
                    'avg_cost': 1.1002,
                }]), \
                patch('fx_sr.positions._tracking_db_transaction', return_value=_tracking_transaction(conn)), \
                patch('fx_sr.positions.claim_signal_for_position_conn', return_value=signal_row), \
                patch('fx_sr.positions._build_trade_from_signal_row', return_value=None), \
                patch('fx_sr.positions._save_trade_conn') as save_trade_mock:
            tracked = sync_positions(StrategyParams())

        self.assertEqual(tracked, {})
        conn.rollback.assert_called_once()
        save_trade_mock.assert_not_called()


if __name__ == '__main__':
    unittest.main()
