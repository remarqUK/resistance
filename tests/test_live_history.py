import unittest
from unittest.mock import patch

import pandas as pd

import fx_sr.live_history as live_history_module
from fx_sr.live import ExecutionResult
from fx_sr.live_history import (
    claim_signal_for_position,
    enqueue_write,
    load_detected_signal,
    load_detected_signal_fills,
    load_detected_signals,
    reconcile_detected_signal_orders,
    record_closed_signal,
    record_detected_signals,
    record_execution_results,
    record_exit_signal,
    start_background_writer,
    stop_background_writer,
)
from fx_sr.sizing import PositionSizePlan
from fx_sr.strategy import Signal
from tests._test_db_helpers import temporary_test_database


def _signal(pair: str, direction: str = 'LONG') -> Signal:
    entry = 1.1000 if direction == 'LONG' else 1.1000
    stop = 1.0950 if direction == 'LONG' else 1.1050
    target = 1.1100 if direction == 'LONG' else 1.0900
    return Signal(
        time=pd.Timestamp('2026-02-03 10:00:00', tz='UTC'),
        pair=pair,
        direction=direction,
        entry_price=entry,
        sl_price=stop,
        tp_price=target,
        zone_upper=1.1010,
        zone_lower=1.1000,
        zone_strength='major',
        zone_type='support' if direction == 'LONG' else 'resistance',
        quality_score=0.75,
    )


def _plan(pair: str, direction: str = 'LONG', risk_amount: float = 200.0) -> PositionSizePlan:
    return PositionSizePlan(
        pair=pair,
        direction=direction,
        units=10000,
        risk_amount=risk_amount,
        risk_pct=risk_amount / 10000.0,
        balance=10000.0,
        account_currency='USD',
        risk_per_unit_account=0.02,
        notional_account=11000.0,
    )


class LiveHistoryTests(unittest.TestCase):
    def setUp(self):
        self._db_ctx = temporary_test_database()
        self.db_path = self._db_ctx.__enter__()
        stop_background_writer()

    def tearDown(self):
        stop_background_writer()
        if self._db_ctx is not None:
            self._db_ctx.__exit__(None, None, None)

    def test_background_writer_stop_drains_queue_and_clears_worker_state(self):
        seen = []

        start_background_writer()
        enqueue_write(lambda: seen.append('done'))
        stop_background_writer()

        self.assertEqual(seen, ['done'])
        self.assertIsNone(live_history_module._write_thread)
        self.assertIsNone(live_history_module._write_queue)

    def test_signal_lifecycle_round_trip_is_persisted(self):
        signal = _signal('EURUSD')
        plan = _plan('EURUSD')

        signal_ids = record_detected_signals(
            [signal],
            [plan],
            execute_orders=True,
            db_path=self.db_path,
        )
        signal_id = signal_ids[0]

        row = load_detected_signal(signal_id, db_path=self.db_path)
        self.assertEqual(row['status'], 'DETECTED')
        self.assertEqual(row['planned_units'], 10000)
        self.assertEqual(row['transacted'], 0)

        record_execution_results(
            [signal],
            [plan],
            [
                ExecutionResult(
                    pair='EURUSD',
                    direction='LONG',
                    units=10000,
                    status='Submitted',
                    order_id=101,
                    take_profit_order_id=102,
                    stop_loss_order_id=103,
                    submitted_entry_price=1.1000,
                    submitted_tp_price=1.1100,
                    submitted_sl_price=1.0950,
                    submit_bid=1.0998,
                    submit_ask=1.1000,
                    submit_spread=0.0002,
                    quote_source='l2',
                    quote_time=pd.Timestamp('2026-02-03 10:00:01', tz='UTC'),
                    note='tp/sl attached',
                )
            ],
            db_path=self.db_path,
        )

        row = load_detected_signal(signal_id, db_path=self.db_path)
        self.assertEqual(row['status'], 'SUBMITTED')
        self.assertEqual(row['transacted'], 1)
        self.assertEqual(row['order_id'], 101)
        self.assertEqual(row['take_profit_order_id'], 102)
        self.assertEqual(row['stop_loss_order_id'], 103)
        self.assertAlmostEqual(row['submitted_entry_price'], 1.1000)
        self.assertAlmostEqual(row['submitted_tp_price'], 1.1100)
        self.assertAlmostEqual(row['submitted_sl_price'], 1.0950)
        self.assertAlmostEqual(row['submit_bid'], 1.0998)
        self.assertAlmostEqual(row['submit_ask'], 1.1000)
        self.assertAlmostEqual(row['submit_spread'], 0.0002)
        self.assertEqual(row['quote_source'], 'l2')
        self.assertEqual(row['quote_time'], '2026-02-03 10:00:01+00:00')

        claimed = claim_signal_for_position(
            'EURUSD',
            'LONG',
            opened_price=1.1002,
            open_units=10000,
            db_path=self.db_path,
        )
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed['signal_id'], signal_id)

        row = load_detected_signal(signal_id, db_path=self.db_path)
        self.assertEqual(row['status'], 'OPEN')
        self.assertEqual(row['open_units'], 10000)
        self.assertAlmostEqual(row['opened_price'], 1.1002)

        record_exit_signal(
            signal_id,
            exit_reason='TIME',
            exit_price=1.1015,
            db_path=self.db_path,
        )

        row = load_detected_signal(signal_id, db_path=self.db_path)
        self.assertEqual(row['status'], 'EXIT_SIGNAL')
        self.assertEqual(row['exit_signal_reason'], 'TIME')
        self.assertAlmostEqual(row['exit_signal_price'], 1.1015)

        record_closed_signal(
            signal_id,
            close_source='position_sync',
            db_path=self.db_path,
        )

        row = load_detected_signal(signal_id, db_path=self.db_path)
        self.assertEqual(row['status'], 'CLOSED')
        self.assertEqual(row['close_reason'], 'TIME')
        self.assertEqual(row['close_source'], 'position_sync')
        self.assertAlmostEqual(row['closed_price'], 1.1015)
        self.assertAlmostEqual(row['pnl_pips'], 13.0)

    def test_claim_signal_for_position_marks_partial_fill_state(self):
        signal = _signal('EURUSD')
        plan = _plan('EURUSD')
        signal_id = record_detected_signals(
            [signal],
            [plan],
            execute_orders=True,
            db_path=self.db_path,
        )[0]
        record_execution_results(
            [signal],
            [plan],
            [ExecutionResult(pair='EURUSD', direction='LONG', units=10000, status='Submitted', order_id=101)],
            db_path=self.db_path,
        )

        claimed = claim_signal_for_position(
            'EURUSD',
            'LONG',
            opened_price=1.1002,
            open_units=4000,
            db_path=self.db_path,
        )

        self.assertIsNotNone(claimed)
        self.assertEqual(claimed['signal_id'], signal_id)
        self.assertEqual(claimed['status'], 'PARTIAL')
        self.assertEqual(claimed['open_units'], 4000)
        self.assertEqual(claimed['remaining_units'], 6000)

    def test_reconcile_detected_signal_orders_is_idempotent_and_opens_full_size(self):
        signal = _signal('EURUSD')
        plan = _plan('EURUSD')
        signal_id = record_detected_signals(
            [signal],
            [plan],
            execute_orders=True,
            db_path=self.db_path,
        )[0]
        record_execution_results(
            [signal],
            [plan],
            [
                ExecutionResult(
                    pair='EURUSD',
                    direction='LONG',
                    units=10000,
                    status='Submitted',
                    order_id=101,
                    filled_units=4000,
                    remaining_units=6000,
                    broker_status='Submitted',
                )
            ],
            db_path=self.db_path,
        )

        first_fill = [{
            'order_id': 101,
            'price': 1.1002,
            'avg_price': 1.1002,
            'shares': 4000.0,
            'cum_qty': 4000.0,
            'side': 'BOT',
            'order_ref': 'fxsr:EURUSD:LONG:20260203100000',
            'time': pd.Timestamp('2026-02-03 10:00:02', tz='UTC'),
            'exec_id': 'exec-1',
        }]
        partial_status = [{
            'order_id': 101,
            'pair': 'EURUSD',
            'status': 'Submitted',
            'filled_units': 4000,
            'remaining_units': 6000,
            'avg_fill_price': 1.1002,
        }]

        with patch('fx_sr.ibkr.fetch_fx_fills', return_value=first_fill), \
                patch('fx_sr.ibkr.fetch_fx_order_statuses', return_value=partial_status):
            rows = reconcile_detected_signal_orders(signal_ids=[signal_id], db_path=self.db_path)

        self.assertEqual(len(rows), 1)
        row = load_detected_signal(signal_id, db_path=self.db_path)
        self.assertEqual(row['status'], 'PARTIAL')
        self.assertEqual(row['open_units'], 4000)
        self.assertEqual(row['remaining_units'], 6000)
        self.assertEqual(row['fill_count'], 1)
        self.assertAlmostEqual(row['opened_price'], 1.1002)
        self.assertEqual(row['opened_at'], '2026-02-03 10:00:02+00:00')
        self.assertEqual(len(load_detected_signal_fills(signal_id, db_path=self.db_path)), 1)

        with patch('fx_sr.ibkr.fetch_fx_fills', return_value=first_fill), \
                patch('fx_sr.ibkr.fetch_fx_order_statuses', return_value=partial_status):
            reconcile_detected_signal_orders(signal_ids=[signal_id], db_path=self.db_path)

        self.assertEqual(len(load_detected_signal_fills(signal_id, db_path=self.db_path)), 1)

        second_fill = first_fill + [{
            'order_id': 101,
            'price': 1.1004,
            'avg_price': 1.10032,
            'shares': 6000.0,
            'cum_qty': 10000.0,
            'side': 'BOT',
            'order_ref': 'fxsr:EURUSD:LONG:20260203100000',
            'time': pd.Timestamp('2026-02-03 10:00:03', tz='UTC'),
            'exec_id': 'exec-2',
        }]
        filled_status = [{
            'order_id': 101,
            'pair': 'EURUSD',
            'status': 'Filled',
            'filled_units': 10000,
            'remaining_units': 0,
            'avg_fill_price': 1.10032,
        }]

        with patch('fx_sr.ibkr.fetch_fx_fills', return_value=second_fill), \
                patch('fx_sr.ibkr.fetch_fx_order_statuses', return_value=filled_status):
            reconcile_detected_signal_orders(signal_ids=[signal_id], db_path=self.db_path)

        row = load_detected_signal(signal_id, db_path=self.db_path)
        self.assertEqual(row['status'], 'OPEN')
        self.assertEqual(row['open_units'], 10000)
        self.assertEqual(row['remaining_units'], 0)
        self.assertEqual(row['fill_count'], 2)
        self.assertAlmostEqual(row['opened_price'], 1.10032)

    def test_repeated_detection_does_not_downgrade_open_trade(self):
        signal = _signal('GBPUSD')
        plan = _plan('GBPUSD')

        signal_id = record_detected_signals(
            [signal],
            [plan],
            execute_orders=True,
            db_path=self.db_path,
        )[0]
        record_execution_results(
            [signal],
            [plan],
            [ExecutionResult(pair='GBPUSD', direction='LONG', units=10000, status='Submitted', order_id=201)],
            db_path=self.db_path,
        )
        claim_signal_for_position(
            'GBPUSD',
            'LONG',
            opened_price=1.1001,
            open_units=10000,
            db_path=self.db_path,
        )

        record_detected_signals(
            [signal],
            [plan],
            execute_orders=False,
            db_path=self.db_path,
        )

        row = load_detected_signal(signal_id, db_path=self.db_path)
        self.assertEqual(row['status'], 'OPEN')
        self.assertEqual(row['transacted'], 1)
        self.assertEqual(row['execution_enabled'], 1)

        rows = load_detected_signals(pair='GBPUSD', db_path=self.db_path)
        self.assertEqual(len(rows), 1)

    def test_execution_mode_and_account_are_persisted(self):
        signal = _signal('EURUSD')
        plan = _plan('EURUSD')

        signal_id = record_detected_signals(
            [signal],
            [plan],
            execute_orders=True,
            execution_mode='paper',
            ibkr_account='DU1234567',
            db_path=self.db_path,
        )[0]

        row = load_detected_signal(signal_id, db_path=self.db_path)
        self.assertEqual(row['execution_mode'], 'paper')
        self.assertEqual(row['ibkr_account'], 'DU1234567')

        # Re-detect as scan — should preserve original mode/account
        record_detected_signals(
            [signal],
            [plan],
            execute_orders=False,
            execution_mode='scan',
            db_path=self.db_path,
        )

        row = load_detected_signal(signal_id, db_path=self.db_path)
        self.assertEqual(row['execution_mode'], 'paper')
        self.assertEqual(row['ibkr_account'], 'DU1234567')

    def test_execution_results_store_mode_and_account(self):
        signal = _signal('AUDUSD', 'SHORT')
        plan = _plan('AUDUSD', 'SHORT')

        record_execution_results(
            [signal],
            [plan],
            [ExecutionResult(pair='AUDUSD', direction='SHORT', units=10000, status='Submitted', order_id=301)],
            execution_mode='live',
            ibkr_account='U9876543',
            db_path=self.db_path,
        )

        from fx_sr.live_history import build_signal_id
        signal_id = build_signal_id(signal)
        row = load_detected_signal(signal_id, db_path=self.db_path)
        self.assertEqual(row['execution_mode'], 'live')
        self.assertEqual(row['ibkr_account'], 'U9876543')
        self.assertEqual(row['transacted'], 1)

    def test_scan_mode_records_no_account(self):
        signal = _signal('USDCHF')
        plan = _plan('USDCHF')

        signal_id = record_detected_signals(
            [signal],
            [plan],
            execute_orders=False,
            execution_mode='scan',
            db_path=self.db_path,
        )[0]

        row = load_detected_signal(signal_id, db_path=self.db_path)
        self.assertEqual(row['execution_mode'], 'scan')
        self.assertIsNone(row['ibkr_account'])


if __name__ == '__main__':
    unittest.main()
