import unittest
from datetime import date, datetime, timezone
from unittest.mock import patch

import pandas as pd

import fx_sr.broker_ledger as broker_ledger_module
import fx_sr.live_history as live_history_module
from fx_sr.live import ExecutionResult
from fx_sr.live_history import (
    claim_signal_for_position,
    enqueue_write,
    has_active_broker_activity_for_order_ref,
    load_detected_signal,
    load_detected_signal_fills,
    load_open_broker_execution_positions,
    load_detected_signals,
    compute_daily_pnl_gbp,
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

    def test_today_snapshot_includes_open_unrealized(self):
        signal = _signal('EURUSD')
        plan = _plan('EURUSD')

        record_detected_signals(
            [signal],
            [plan],
            execute_orders=True,
            db_path=self.db_path,
        )
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
                ),
            ],
            db_path=self.db_path,
        )
        claim_signal_for_position(
            'EURUSD',
            'LONG',
            opened_price=1.1000,
            open_units=10000,
            db_path=self.db_path,
        )

        from fx_sr.db import INTERVAL_TO_CODE, TICKER_TO_CODE

        market_time = datetime.now(timezone.utc)

        with live_history_module.db_transaction(self.db_path) as conn:
            conn.execute(
                "INSERT INTO account_daily_snapshot (snapshot_date, net_liquidation, daily_pnl_gbp, currency) "
                "VALUES (%s, %s, %s, %s)",
                (date.today(), 10000.0, 0.0, 'USD'),
            )
            conn.execute(
                "INSERT INTO ohlc (ticker, interval, ts, open, high, low, close, volume) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    TICKER_TO_CODE['EURUSD=X'],
                    INTERVAL_TO_CODE['1h'],
                    market_time,
                    1.1000,
                    1.1010,
                    1.1000,
                    1.1010,
                    0,
                ),
            )

        snapshot = live_history_module.get_or_fetch_today_snapshot(db_path=self.db_path)
        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot['equity'], 10010.0)

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

    def test_skipped_execution_result_does_not_downgrade_open_trade(self):
        signal = _signal('USDJPY')
        plan = _plan('USDJPY')
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
                    pair='USDJPY',
                    direction='LONG',
                    units=10000,
                    status='OPEN',
                    order_id=501,
                    filled_units=10000,
                    remaining_units=0,
                    avg_fill_price=159.39,
                    broker_status='Filled',
                    note='filled 10,000/10,000',
                )
            ],
            db_path=self.db_path,
        )

        record_execution_results(
            [signal],
            [plan],
            [
                ExecutionResult(
                    pair='USDJPY',
                    direction='LONG',
                    units=10000,
                    status='SKIPPED',
                    note='entry drift too large',
                )
            ],
            db_path=self.db_path,
        )

        row = load_detected_signal(signal_id, db_path=self.db_path)
        self.assertEqual(row['status'], 'OPEN')
        self.assertEqual(row['transacted'], 1)
        self.assertEqual(row['order_id'], 501)
        self.assertEqual(row['open_units'], 10000)
        self.assertEqual(row['broker_order_status'], 'Filled')
        self.assertEqual(row['note'], 'filled 10,000/10,000')

    def test_reconcile_repairs_skipped_row_with_execution_evidence(self):
        signal = _signal('EURJPY')
        plan = _plan('EURJPY')
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
                    pair='EURJPY',
                    direction='LONG',
                    units=10000,
                    status='OPEN',
                    order_id=601,
                    filled_units=10000,
                    remaining_units=0,
                    avg_fill_price=159.55,
                    broker_status='Filled',
                    note='filled 10,000/10,000',
                )
            ],
            db_path=self.db_path,
        )
        with live_history_module.db_transaction(self.db_path) as conn:
            conn.execute(
                """
                UPDATE detected_signal
                SET status='SKIPPED', transacted=0, note='position/order exists'
                WHERE signal_id=%s
                """,
                (signal_id,),
            )

        with patch('fx_sr.ibkr.fetch_fx_fills', return_value=[]), \
                patch('fx_sr.ibkr.fetch_fx_order_statuses', return_value=[{
                    'order_id': 601,
                    'pair': 'EURJPY',
                    'status': 'Filled',
                    'filled_units': 10000,
                    'remaining_units': 0,
                    'avg_fill_price': 159.55,
                }]):
            rows = reconcile_detected_signal_orders(
                signal_ids=[signal_id],
                live_position_keys={'EURJPY:LONG'},
                db_path=self.db_path,
            )

        self.assertEqual(len(rows), 1)
        row = load_detected_signal(signal_id, db_path=self.db_path)
        self.assertEqual(row['status'], 'OPEN')
        self.assertEqual(row['transacted'], 1)
        self.assertEqual(row['open_units'], 10000)
        self.assertEqual(row['remaining_units'], 0)
        self.assertEqual(row['note'], 'filled 10,000/10,000')

    def test_reconcile_collects_repeated_parent_fills_by_order_ref(self):
        signal = _signal('USDJPY')
        plan = _plan('USDJPY')
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
                    pair='USDJPY',
                    direction='LONG',
                    units=10000,
                    status='Submitted',
                    order_id=105,
                    broker_status='Submitted',
                )
            ],
            db_path=self.db_path,
        )

        ref = 'fxsr:USDJPY:LONG:20260203100000'
        repeated_fills = [
            {
                'order_id': 101,
                'price': 159.30,
                'avg_price': 159.30,
                'shares': 10000.0,
                'cum_qty': 10000.0,
                'side': 'BOT',
                'order_ref': ref,
                'time': pd.Timestamp('2026-02-03 10:01:00', tz='UTC'),
                'exec_id': 'exec-ref-1',
            },
            {
                'order_id': 102,
                'price': 159.50,
                'avg_price': 159.50,
                'shares': 10000.0,
                'cum_qty': 10000.0,
                'side': 'BOT',
                'order_ref': ref,
                'time': pd.Timestamp('2026-02-03 10:02:00', tz='UTC'),
                'exec_id': 'exec-ref-2',
            },
        ]

        with patch('fx_sr.ibkr.fetch_fx_fills', return_value=repeated_fills), \
                patch('fx_sr.ibkr.fetch_fx_order_statuses', return_value=[{
                    'order_id': 105,
                    'pair': 'USDJPY',
                    'status': 'Submitted',
                    'filled_units': 0,
                    'remaining_units': 10000,
                }]):
            rows = reconcile_detected_signal_orders(
                signal_ids=[signal_id],
                live_position_keys={'USDJPY:LONG'},
                db_path=self.db_path,
            )

        self.assertEqual(len(rows), 1)
        row = load_detected_signal(signal_id, db_path=self.db_path)
        self.assertEqual(row['status'], 'OPEN')
        self.assertEqual(row['open_units'], 20000)
        self.assertEqual(row['fill_count'], 2)
        self.assertAlmostEqual(row['opened_price'], 159.40)
        fills = load_detected_signal_fills(signal_id, db_path=self.db_path)
        self.assertEqual({fill['order_id'] for fill in fills}, {101, 102})
        self.assertTrue(has_active_broker_activity_for_order_ref(ref, db_path=self.db_path))

        broker_positions = load_open_broker_execution_positions(db_path=self.db_path)
        self.assertEqual(len(broker_positions), 1)
        self.assertEqual(broker_positions[0]['pair'], 'USDJPY')
        self.assertEqual(broker_positions[0]['direction'], 'LONG')
        self.assertEqual(broker_positions[0]['size'], 20000)
        self.assertAlmostEqual(broker_positions[0]['avg_cost'], 159.40)

    def test_reconcile_nets_exit_fill_and_removes_closed_broker_position(self):
        signal = _signal('USDJPY')
        plan = _plan('USDJPY')
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
                    pair='USDJPY',
                    direction='LONG',
                    units=10000,
                    status='Submitted',
                    order_id=105,
                    take_profit_order_id=106,
                    stop_loss_order_id=107,
                    broker_status='Submitted',
                )
            ],
            db_path=self.db_path,
        )

        ref = 'fxsr:USDJPY:LONG:20260203100000'
        fills = [
            {
                'pair': 'USDJPY',
                'order_id': 105,
                'price': 159.30,
                'avg_price': 159.30,
                'shares': 10000.0,
                'cum_qty': 10000.0,
                'side': 'BOT',
                'order_ref': ref,
                'time': pd.Timestamp('2026-02-03 10:01:00', tz='UTC'),
                'exec_id': 'exec-entry',
            },
            {
                'pair': 'USDJPY',
                'order_id': 106,
                'price': 160.00,
                'avg_price': 160.00,
                'shares': 10000.0,
                'cum_qty': 10000.0,
                'side': 'SLD',
                'order_ref': f'{ref}:tp',
                'time': pd.Timestamp('2026-02-03 10:05:00', tz='UTC'),
                'exec_id': 'exec-tp',
            },
        ]
        statuses = [
            {
                'order_id': 105,
                'pair': 'USDJPY',
                'order_ref': ref,
                'status': 'Filled',
                'filled_units': 10000,
                'remaining_units': 0,
                'avg_fill_price': 159.30,
            },
            {
                'order_id': 106,
                'pair': 'USDJPY',
                'order_ref': f'{ref}:tp',
                'status': 'Filled',
                'filled_units': 10000,
                'remaining_units': 0,
                'avg_fill_price': 160.00,
            },
        ]

        with patch('fx_sr.ibkr.fetch_fx_fills', return_value=fills), \
                patch('fx_sr.ibkr.fetch_fx_order_statuses', return_value=statuses), \
                patch('fx_sr.ibkr.fetch_completed_fx_orders', return_value=[]):
            rows = reconcile_detected_signal_orders(signal_ids=[signal_id], db_path=self.db_path)

        self.assertEqual(len(rows), 1)
        row = load_detected_signal(signal_id, db_path=self.db_path)
        self.assertEqual(row['status'], 'CLOSED')
        self.assertEqual(row['open_units'], 0)
        self.assertEqual(row['remaining_units'], 0)
        self.assertEqual(row['close_reason'], 'TP')
        self.assertEqual(row['close_source'], 'broker_tp')
        self.assertAlmostEqual(row['closed_price'], 160.00)
        self.assertAlmostEqual(row['pnl_pips'], 70.0)
        self.assertEqual(load_open_broker_execution_positions(db_path=self.db_path), [])

    def test_open_broker_positions_are_net_of_partial_exits(self):
        signal = _signal('USDJPY')
        plan = _plan('USDJPY')
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
                    pair='USDJPY',
                    direction='LONG',
                    units=10000,
                    status='Submitted',
                    order_id=205,
                    take_profit_order_id=206,
                    stop_loss_order_id=207,
                    broker_status='Submitted',
                )
            ],
            db_path=self.db_path,
        )

        ref = 'fxsr:USDJPY:LONG:20260203100000'
        fills = [
            {
                'pair': 'USDJPY',
                'order_id': 205,
                'price': 159.30,
                'avg_price': 159.30,
                'shares': 10000.0,
                'cum_qty': 10000.0,
                'side': 'BOT',
                'order_ref': ref,
                'time': pd.Timestamp('2026-02-03 10:01:00', tz='UTC'),
                'exec_id': 'exec-entry-partial-close',
            },
            {
                'pair': 'USDJPY',
                'order_id': 206,
                'price': 159.80,
                'avg_price': 159.80,
                'shares': 4000.0,
                'cum_qty': 4000.0,
                'side': 'SLD',
                'order_ref': f'{ref}:tp',
                'time': pd.Timestamp('2026-02-03 10:05:00', tz='UTC'),
                'exec_id': 'exec-partial-tp',
            },
        ]

        with patch('fx_sr.ibkr.fetch_fx_fills', return_value=fills), \
                patch('fx_sr.ibkr.fetch_fx_order_statuses', return_value=[]), \
                patch('fx_sr.ibkr.fetch_completed_fx_orders', return_value=[]):
            rows = reconcile_detected_signal_orders(
                signal_ids=[signal_id],
                live_position_keys={'USDJPY:LONG'},
                db_path=self.db_path,
            )

        self.assertEqual(len(rows), 1)
        row = load_detected_signal(signal_id, db_path=self.db_path)
        self.assertEqual(row['status'], 'OPEN')
        self.assertEqual(row['open_units'], 6000)
        self.assertEqual(row['remaining_units'], 0)
        broker_positions = load_open_broker_execution_positions(db_path=self.db_path)
        self.assertEqual(len(broker_positions), 1)
        self.assertEqual(broker_positions[0]['pair'], 'USDJPY')
        self.assertEqual(broker_positions[0]['direction'], 'LONG')
        self.assertEqual(broker_positions[0]['size'], 6000)
        self.assertAlmostEqual(broker_positions[0]['avg_cost'], 159.30)

    def test_unmatched_broker_liquidation_is_exposed_as_closed_trade(self):
        conn = broker_ledger_module.signal_store.ensure_signal_tables(self.db_path)
        del conn
        db_conn = broker_ledger_module._connect(self.db_path)
        try:
            now = pd.Timestamp('2026-02-03 10:00:00', tz='UTC')
            rows = [
                ('entry-1', 'USDJPY', 'UNKNOWN', 'SLD', 1001, 'rebracket:tp', now, 160.00, 100),
                ('entry-2', 'USDJPY', 'UNKNOWN', 'SLD', 1002, 'fxsr:close:USDJPY:LONG:123', now + pd.Timedelta(minutes=5), 159.80, 50),
                ('close-1', 'USDJPY', 'UNKNOWN', 'BOT', 1003, 'fxsr:liquidate:USDJPY:123', now + pd.Timedelta(minutes=10), 159.50, 150),
            ]
            for exec_id, pair, role, side, order_id, order_ref, fill_time, price, units in rows:
                db_conn.execute(
                    """
                    INSERT INTO broker_execution (
                        exec_id, signal_id, pair, direction, role, side, order_id,
                        order_ref, fill_time, fill_price, fill_units, recorded_at, updated_at
                    )
                    VALUES (%s, NULL, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (exec_id, pair, role, side, order_id, order_ref, fill_time, price, units, now, now),
                )
            db_conn.commit()

            trades = broker_ledger_module.load_unmatched_broker_liquidation_trades_conn(db_conn)
        finally:
            db_conn.close()

        self.assertEqual(len(trades), 1)
        trade = trades[0]
        self.assertEqual(trade['pair'], 'USDJPY')
        self.assertEqual(trade['direction'], 'SHORT')
        self.assertEqual(trade['status'], 'CLOSED')
        self.assertEqual(trade['close_source'], 'broker_liquidation')
        self.assertEqual(trade['open_units'], 150)
        self.assertAlmostEqual(trade['opened_price'], 159.9333333333)
        self.assertAlmostEqual(trade['closed_price'], 159.50)
        self.assertAlmostEqual(trade['pnl_pips'], 43.3333333333)

    def test_daily_pnl_includes_unmatched_broker_liquidation(self):
        broker_ledger_module.signal_store.ensure_signal_tables(self.db_path)
        conn = broker_ledger_module._connect(self.db_path)
        try:
            now = pd.Timestamp('2026-02-03 10:00:00', tz='UTC')
            rows = [
                ('eg-entry', 'EURGBP', 'UNKNOWN', 'SLD', 2001, 'rebracket:tp', now, 0.9000, 10000),
                ('eg-close', 'EURGBP', 'UNKNOWN', 'BOT', 2002, 'fxsr:liquidate:EURGBP:123', now + pd.Timedelta(minutes=10), 0.8900, 10000),
            ]
            for exec_id, pair, role, side, order_id, order_ref, fill_time, price, units in rows:
                conn.execute(
                    """
                    INSERT INTO broker_execution (
                        exec_id, signal_id, pair, direction, role, side, order_id,
                        order_ref, fill_time, fill_price, fill_units, recorded_at, updated_at
                    )
                    VALUES (%s, NULL, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (exec_id, pair, role, side, order_id, order_ref, fill_time, price, units, now, now),
                )
            conn.commit()

            pnl = compute_daily_pnl_gbp(conn, date(2026, 2, 3))
        finally:
            conn.close()

        self.assertAlmostEqual(pnl, 100.0)

    def test_order_ref_activity_checks_live_ibkr_when_local_ledger_is_empty(self):
        ref = 'fxsr:EURUSD:LONG:20260203100000'

        with patch('fx_sr.broker_ledger.ibkr.fetch_fx_order_statuses', return_value=[{
                'order_id': 301,
                'pair': 'EURUSD',
                'order_ref': ref,
                'status': 'Submitted',
            }]), \
                patch('fx_sr.broker_ledger.ibkr.fetch_fx_fills', return_value=[]):
            self.assertTrue(
                has_active_broker_activity_for_order_ref(ref, db_path=self.db_path)
            )

    def test_broker_ledger_reconcile_uses_bounded_cursor_after_initial_scan(self):
        signal = _signal('EURUSD')
        plan = _plan('EURUSD')
        record_detected_signals(
            [signal],
            [plan],
            execute_orders=True,
            db_path=self.db_path,
        )
        record_execution_results(
            [signal],
            [plan],
            [
                ExecutionResult(
                    pair='EURUSD',
                    direction='LONG',
                    units=10000,
                    status='Submitted',
                    order_id=401,
                    broker_status='Submitted',
                )
            ],
            db_path=self.db_path,
        )
        seen_since = []

        def fake_fetch_fills(*args, **kwargs):
            seen_since.append(kwargs.get('since'))
            return []

        with patch('fx_sr.broker_ledger.ibkr.fetch_fx_fills', side_effect=fake_fetch_fills), \
                patch('fx_sr.broker_ledger.ibkr.fetch_fx_order_statuses', return_value=[]), \
                patch('fx_sr.broker_ledger.ibkr.fetch_completed_fx_orders', return_value=[]):
            broker_ledger_module.reconcile_broker_ledger(db_path=self.db_path)
            broker_ledger_module.reconcile_broker_ledger(db_path=self.db_path)

        self.assertEqual(len(seen_since), 2)
        self.assertIsNone(seen_since[0])
        self.assertIsNotNone(seen_since[1])
        self.assertGreater(
            pd.Timestamp(seen_since[1]),
            pd.Timestamp.now(tz='UTC') - pd.Timedelta(minutes=10),
        )

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

    def test_detection_source_marker_is_persisted(self):
        signal = _signal('GBPJPY', 'SHORT')

        signal_id = record_detected_signals(
            [signal],
            execute_orders=False,
            execution_mode='intrabar',
            detection_source='startup_replay',
            db_path=self.db_path,
        )[0]

        row = load_detected_signal(signal_id, db_path=self.db_path)
        self.assertEqual(row['quote_source'], 'startup_replay')
        self.assertEqual(row['note'], 'detected via startup replay')
        self.assertEqual(row['execution_mode'], 'intrabar')


if __name__ == '__main__':
    unittest.main()
