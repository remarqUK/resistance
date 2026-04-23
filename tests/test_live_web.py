import asyncio
import json
import contextlib
from datetime import date
import unittest
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, patch

import pandas as pd
from aiohttp import web

from fx_sr.live import ExecutionResult, PairScanRow
from fx_sr.live_history import (
    claim_signal_for_position,
    load_detected_signal,
    record_detected_signals,
    record_execution_results,
    record_exit_signal,
    stop_background_writer,
)
from fx_sr.live_web import (
    ALERT_LIMIT,
    EXECUTION_LIMIT,
    LiveDashboardHub,
    _configure_windows_event_loop_policy,
    _account_history_api,
    _set_execution_mode,
    _validate_websocket_request,
)
from fx_sr.sizing import PositionSizePlan
from fx_sr.strategy import Signal
from fx_sr.strategy import StrategyParams, Trade
from tests._test_db_helpers import temporary_test_database


def _bar(time, open_, high, low, close, volume=0):
    return SimpleNamespace(
        time=time, open_=open_, high=high, low=low, close=close, volume=volume,
    )


def _trade() -> Trade:
    return Trade(
        entry_time=pd.Timestamp('2026-03-10 13:00:00', tz='UTC'),
        entry_price=1.1000,
        direction='LONG',
        sl_price=1.0950,
        tp_price=1.1100,
        zone_upper=1.1010,
        zone_lower=1.0990,
        zone_strength='major',
        risk=0.0050,
    )


def _signal(pair: str, direction: str = 'LONG') -> Signal:
    return Signal(
        time=pd.Timestamp('2026-03-10 13:00:00', tz='UTC'),
        pair=pair,
        direction=direction,
        entry_price=1.1000,
        sl_price=1.0950,
        tp_price=1.1100,
        zone_upper=1.1010,
        zone_lower=1.0990,
        zone_strength='major',
        zone_type='support' if direction == 'LONG' else 'resistance',
        quality_score=0.75,
    )


def _plan(pair: str, direction: str = 'LONG') -> PositionSizePlan:
    return PositionSizePlan(
        pair=pair,
        direction=direction,
        units=10000,
        risk_amount=100.0,
        risk_pct=0.01,
        balance=10000.0,
        account_currency='USD',
        risk_per_unit_account=0.01,
        notional_account=11000.0,
    )


class LiveDashboardHubTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.hub = LiveDashboardHub(
            pairs={
                'EURUSD': {
                    'name': 'EUR/USD',
                    'ticker': 'EURUSD=X',
                    'decimals': 5,
                },
            },
            params=StrategyParams(),
            interval=60,
            zone_history_days=30,
            track_positions=True,
            balance=10000.0,
            risk_pct=0.01,
            account_currency='USD',
            execute_orders=False,
            strategy_label=None,
            client_id=None,
            port=8080,
        )
        self.hub._loop = asyncio.get_running_loop()
        self.hub._broadcast = AsyncMock()
        self.hub._pair_rows = {
            'EURUSD': PairScanRow(
                pair='EURUSD',
                name='EUR/USD',
                decimals=5,
                price=1.1000,
                state='WATCH',
                note='Watching',
                support_text='1.0990-1.1010',
                resistance_text='-',
            ),
        }
        self.hub._tracked = {
            'EURUSD:LONG': {
                'pair': 'EURUSD',
                'trade': _trade(),
                'bars_monitored': 3,
                'signal_id': 'sig-1',
            },
        }
        self.hub._accumulator.seed(
            'EURUSD',
            pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume']),
        )
        self.hub._accumulator.on_realtime_bar('EURUSD', _bar(
            pd.Timestamp('2026-03-10 14:59:55', tz='UTC'),
            1.1000, 1.1020, 1.0995, 1.1010, 1,
        ))
        self.hub._accumulator.on_realtime_bar('EURUSD', _bar(
            pd.Timestamp('2026-03-10 15:00:05', tz='UTC'),
            1.2000, 1.2010, 1.1990, 1.2005, 1,
        ))

    async def asyncTearDown(self):
        self.hub._scan_executor.shutdown(wait=True)

    async def test_hourly_bar_complete_uses_finalized_bar_and_persists_tracking(self):
        self.hub._backfill_done = True
        captured = {}

        def _capture_signal_eval(pair, tracked_positions=None, blocked_pairs=None, price=None, hourly_df=None):
            captured['pair'] = pair
            captured['price'] = price
            captured['tracked_pairs'] = tracked_positions
            captured['blocked_pairs'] = blocked_pairs
            captured['hourly_df'] = hourly_df.copy()
            return None, None, []

        with patch('fx_sr.positions.check_exit', return_value=None), \
                patch('fx_sr.positions._save_bar_tracking') as save_tracking_mock, \
                patch.object(self.hub, '_evaluate_pair_row', side_effect=_capture_signal_eval):
            await self.hub._handle_hourly_bar_complete(
                'EURUSD',
                pd.Timestamp('2026-03-10 14:00:00', tz='UTC'),
            )

        completed_time = pd.Timestamp('2026-03-10 14:00:00', tz='UTC')
        self.assertEqual(captured['pair'], 'EURUSD')
        self.assertAlmostEqual(captured['price'], 1.1010)
        self.assertEqual(set(captured['tracked_pairs']), {'EURUSD:LONG'})
        self.assertEqual(captured['tracked_pairs']['EURUSD:LONG']['pair'], 'EURUSD')
        self.assertEqual(captured['tracked_pairs']['EURUSD:LONG']['trade'].direction, 'LONG')
        self.assertEqual(captured['blocked_pairs'], set())
        self.assertEqual(list(captured['hourly_df'].index), [completed_time])
        self.assertAlmostEqual(captured['hourly_df'].iloc[-1]['Close'], 1.1010)

        self.assertEqual(self.hub._tracked['EURUSD:LONG']['bars_monitored'], 4)
        self.assertEqual(self.hub._tracked['EURUSD:LONG']['last_processed_bar_time'], completed_time)
        save_tracking_mock.assert_called_once_with('EURUSD', 'LONG', 4, completed_time)

    async def test_tick_exit_persistence_awaits_outside_dashboard_lock(self):
        self.hub._backfill_done = True
        alert = {
            'pair': 'EURUSD',
            'direction': 'LONG',
            'exit_reason': 'SL',
            'exit_price': 1.0949,
        }

        async def _assert_unlocked(fn, timeout=30.0):
            self.assertFalse(self.hub._lock.locked())
            fn()

        with patch.object(self.hub._scanner, 'check_tick_exits', return_value=[alert]), \
                patch('fx_sr.live_web.enqueue_write_async', new=AsyncMock(side_effect=_assert_unlocked)) as write_mock, \
                patch('fx_sr.live_web.record_exit_signal') as record_exit_mock:
            await self.hub._handle_quote_update('EURUSD', 1.0948)

        write_mock.assert_awaited_once()
        record_exit_mock.assert_called_once_with(
            'sig-1',
            exit_reason='SL',
            exit_price=1.0949,
        )

    async def test_tick_exit_rejection_latches_exit_intent_without_retry_loop(self):
        db_ctx = temporary_test_database()
        db_path = db_ctx.__enter__()
        stop_background_writer()
        try:
            signal = _signal('EURUSD')
            plan = _plan('EURUSD')
            signal_id = record_detected_signals(
                [signal],
                [plan],
                execute_orders=True,
                db_path=db_path,
            )[0]
            record_execution_results(
                [signal],
                [plan],
                [ExecutionResult(
                    pair='EURUSD',
                    direction='LONG',
                    units=10000,
                    status='Submitted',
                    order_id=101,
                    avg_fill_price=1.1002,
                    filled_units=10000,
                    remaining_units=0,
                )],
                db_path=db_path,
            )
            claim_signal_for_position(
                'EURUSD',
                'LONG',
                opened_price=1.1002,
                open_units=10000,
                db_path=db_path,
            )
            self.hub._tracked['EURUSD:LONG']['signal_id'] = signal_id
            self.hub._tracked['EURUSD:LONG']['ibkr_size'] = 10000
            self.hub._backfill_done = True
            alert = {
                'pair': 'EURUSD',
                'direction': 'LONG',
                'exit_reason': 'TIME',
                'exit_price': 1.1015,
            }

            def _record_exit(signal_id_arg, *, exit_reason, exit_price):
                record_exit_signal(
                    signal_id_arg,
                    exit_reason=exit_reason,
                    exit_price=exit_price,
                    db_path=db_path,
                )

            with patch.object(self.hub._scanner, 'check_tick_exits', return_value=[alert]), \
                    patch('fx_sr.live_web.record_exit_signal', side_effect=_record_exit), \
                    patch('fx_sr.live_web.cancel_bracket_children', return_value=set()) as cancel_mock, \
                    patch('fx_sr.live_web.ibkr.liquidate_fx_position', return_value={'status': 'FAILED', 'order_id': 555, 'error': 'inactive'}) as liquidate_mock:
                await self.hub._handle_quote_update('EURUSD', 1.1016)
                await self.hub._handle_quote_update('EURUSD', 1.1017)

            row = load_detected_signal(signal_id, db_path=db_path)
            self.assertEqual(row['status'], 'EXIT_SIGNAL')
            self.assertEqual(row['exit_signal_reason'], 'TIME')
            self.assertIsNotNone(row['exit_signal_at'])
            self.assertEqual(row['exit_signal_price'], 1.1015)
            self.assertIn('EURUSD:LONG', self.hub._tick_exit_alerted)
            self.assertNotIn('EURUSD:LONG', self.hub._inflight_close_orders)
            self.assertIn('EURUSD:LONG', self.hub._failed_close_orders)
            cancel_mock.assert_not_called()
            self.assertEqual(liquidate_mock.call_count, 1)
        finally:
            stop_background_writer()
            db_ctx.__exit__(None, None, None)

    async def test_manual_close_uses_verified_live_ibkr_size(self):
        self.hub._execution_available = True
        self.hub._tracked['EURUSD:LONG']['ibkr_size'] = 25000

        with patch('fx_sr.live_web.ibkr.fetch_positions', return_value=[{
                'pair': 'EURUSD',
                'size': 10000.0,
                'avg_cost': 1.1002,
            }]), \
                patch('fx_sr.live_web.cancel_bracket_children', return_value=set()) as cancel_mock, \
                patch('fx_sr.live_web.ibkr.submit_fx_market_order', return_value={
                    'status': 'Submitted',
                    'order_id': 123,
                    'avg_fill_price': None,
                }) as submit_mock, \
                patch('fx_sr.live_web.sync_positions', return_value=self.hub._tracked.copy()):
            result = await self.hub.close_tracked_position(pair='EURUSD', direction='LONG')

        cancel_mock.assert_called_once_with('sig-1')
        submit_mock.assert_called_once_with(
            pair='EURUSD',
            direction='SHORT',
            quantity=10000,
            order_ref=ANY,
        )
        self.assertEqual(result['result']['status'], 'SUBMITTED')
        self.assertEqual(result['result']['size'], 10000)

    async def test_manual_close_refuses_conflicting_live_ibkr_direction(self):
        self.hub._execution_available = True
        self.hub._tracked['EURUSD:LONG']['ibkr_size'] = 10000

        with patch('fx_sr.live_web.ibkr.fetch_positions', return_value=[{
                'pair': 'EURUSD',
                'size': -10000.0,
                'avg_cost': 1.1002,
            }]), \
                patch('fx_sr.live_web.cancel_bracket_children', return_value=set()) as cancel_mock, \
                patch('fx_sr.live_web.ibkr.submit_fx_market_order') as submit_mock:
            with self.assertRaises(RuntimeError) as ctx:
                await self.hub.close_tracked_position(pair='EURUSD', direction='LONG')

        self.assertIn('IBKR reports EURUSD SHORT 10,000 units', str(ctx.exception))
        cancel_mock.assert_not_called()
        submit_mock.assert_not_called()

    async def test_liquidate_live_position_uses_live_ibkr_reduce_only_helper(self):
        self.hub._execution_available = True
        self.hub._execution_paused = True

        with patch('fx_sr.live_web.ibkr.liquidate_fx_position', return_value={
                'pair': 'EURUSD',
                'direction': 'SHORT',
                'close_direction': 'LONG',
                'quantity': 416044,
                'order_id': 777,
                'status': 'Submitted',
                'avg_fill_price': None,
                'cancelled_order_ids': [8725, 8726],
                'remaining_open_orders': [],
            }) as liquidate_mock, \
                patch('fx_sr.live_web.sync_positions', return_value={}) as sync_mock, \
                patch.object(self.hub, '_export_state', return_value={'state': 'ok'}):
            result = await self.hub.liquidate_live_position(pair='EURUSD', direction='SHORT')

        liquidate_mock.assert_called_once_with(
            pair='EURUSD',
            expected_direction='SHORT',
            order_ref=ANY,
        )
        sync_mock.assert_called_once()
        self.assertEqual(result['result']['status'], 'SUBMITTED')
        self.assertEqual(result['result']['close_direction'], 'LONG')
        self.assertEqual(result['result']['size'], 416044)
        self.assertEqual(result['result']['cancelled_order_ids'], [8725, 8726])

    async def test_liquidate_live_position_does_not_sync_after_remaining_orders_failure(self):
        self.hub._execution_available = True

        with patch('fx_sr.live_web.ibkr.liquidate_fx_position', return_value={
                'pair': 'EURUSD',
                'direction': 'SHORT',
                'size': -416044,
                'status': 'FAILED',
                'error': '2 working EURUSD order(s) are still visible after cancellation; retry once they are gone.',
                'cancelled_order_ids': [8725, 8726],
                'remaining_open_orders': [{'order_id': 8725, 'status': 'PendingCancel'}],
            }), \
                patch('fx_sr.live_web.sync_positions') as sync_mock, \
                patch.object(self.hub, '_export_state', return_value={'state': 'ok'}):
            result = await self.hub.liquidate_live_position(pair='EURUSD', direction='SHORT')

        sync_mock.assert_not_called()
        self.assertEqual(result['result']['status'], 'FAILED')
        self.assertEqual(result['result']['remaining_open_orders'][0]['order_id'], 8725)

    async def test_startup_recovery_preserves_original_exit_reason(self):
        self.hub._accumulator.seeded_pairs.add('EURUSD')
        tracked = {
            'EURUSD:LONG': {
                'pair': 'EURUSD',
                'trade': _trade(),
                'bars_monitored': 3,
                'signal_id': 'sig-1',
                'ibkr_size': 10000,
                'pending_exit_reason': 'TIME',
                'pending_exit_price': 1.1015,
            },
        }

        with patch.object(
            self.hub,
            '_backfill_data',
            return_value=([], list(self.hub._pair_rows.values()), [], [], 0.0),
        ), \
                patch('fx_sr.positions.sync_positions', return_value=tracked), \
                patch('fx_sr.live_web.ibkr.fetch_account_net_liquidation', return_value=(10000.0, 'USD')), \
                patch('fx_sr.live_web.cancel_bracket_children', return_value=set()) as cancel_mock, \
                patch('fx_sr.live_web.ibkr.liquidate_fx_position', return_value={'status': 'Submitted', 'order_id': 123, 'quantity': 10000}):
            await self.hub._run_backfill()

            cancel_mock.assert_not_called()
            self.assertEqual(
                self.hub._inflight_close_orders['EURUSD:LONG'],
                (123, 'TIME', 'sig-1', 1.1015),
            )

    async def test_run_backfill_emits_completion_beep(self):
        with patch.object(
            self.hub,
            '_backfill_data',
            return_value=([], list(self.hub._pair_rows.values()), [], [], 0.0),
        ), \
                patch.object(self.hub, '_emit_backfill_complete_beep') as beep_mock, \
                patch('fx_sr.positions.sync_positions', return_value={}), \
                patch('fx_sr.live_web.ibkr.fetch_account_net_liquidation', return_value=(10000.0, 'USD')), \
                patch('fx_sr.live_web.cancel_bracket_children', return_value=set()), \
                patch('fx_sr.live_web.ibkr.liquidate_fx_position', return_value={'status': 'Submitted', 'order_id': 123, 'quantity': 10000}):
            await self.hub._run_backfill()

        beep_mock.assert_called_once()

    async def test_alert_and_execution_buffers_are_bounded(self):
        for idx in range(ALERT_LIMIT + 5):
            self.hub._alerts.append({'pair': 'EURUSD', 'direction': 'LONG', 'exit_reason': str(idx)})
        for idx in range(EXECUTION_LIMIT + 7):
            self.hub._execution_results.append(SimpleNamespace(
                pair='EURUSD',
                direction='LONG',
                units=10000,
                status=f'status-{idx}',
                note='ok',
            ))

        self.assertEqual(len(self.hub._alerts), ALERT_LIMIT)
        self.assertEqual(len(self.hub._execution_results), EXECUTION_LIMIT)
        self.assertEqual(self.hub._alerts[0]['exit_reason'], '5')
        self.assertEqual(self.hub._execution_results[0].status, 'status-7')

    async def test_hydrate_execution_activity_restores_recent_db_rows(self):
        rows = [
            {
                'pair': 'GBPUSD',
                'direction': 'SHORT',
                'planned_units': 9000,
                'open_units': None,
                'status': 'FAILED',
                'order_id': None,
                'take_profit_order_id': None,
                'stop_loss_order_id': None,
                'opened_price': None,
                'remaining_units': None,
                'broker_order_status': None,
                'submitted_entry_price': None,
                'submitted_tp_price': None,
                'submitted_sl_price': None,
                'submit_bid': None,
                'submit_ask': None,
                'submit_spread': None,
                'quote_source': None,
                'quote_time': None,
                'note': 'broker rejected',
                'closed_at': None,
            },
            {
                'pair': 'EURUSD',
                'direction': 'LONG',
                'planned_units': 12000,
                'open_units': 4000,
                'status': 'PARTIAL',
                'order_id': 101,
                'take_profit_order_id': 102,
                'stop_loss_order_id': 103,
                'opened_price': 1.1002,
                'remaining_units': 8000,
                'broker_order_status': 'Submitted',
                'submitted_entry_price': 1.1,
                'submitted_tp_price': 1.11,
                'submitted_sl_price': 1.095,
                'submit_bid': 1.0998,
                'submit_ask': 1.1,
                'submit_spread': 0.0002,
                'quote_source': 'l2',
                'quote_time': '2026-03-18T15:00:00+00:00',
                'note': 'partial fill 4,000/12,000',
                'closed_at': None,
            },
        ]

        with patch('fx_sr.live_web.load_execution_activity', return_value=rows):
            self.hub._hydrate_execution_activity()

        self.assertEqual(len(self.hub._execution_results), 2)
        self.assertEqual(self.hub._execution_results[0].pair, 'EURUSD')
        self.assertEqual(self.hub._execution_results[0].status, 'PARTIAL')
        self.assertEqual(self.hub._execution_results[0].order_id, 101)
        self.assertEqual(
            self.hub._serialize_executions()[0]['time'],
            '2026-03-18T15:00:00+00:00',
        )
        self.assertEqual(self.hub._execution_results[1].pair, 'GBPUSD')
        self.assertEqual(self.hub._execution_results[1].status, 'FAILED')
        self.assertEqual(self.hub._tick_pending_pairs, {'EURUSD'})

    async def test_serialize_positions_marks_partial_signal_status(self):
        self.hub._tracked['EURUSD:LONG']['signal_status'] = 'PARTIAL'

        rows = self.hub._serialize_positions()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['status'], 'PARTIAL')

    async def test_register_rolls_back_client_when_bootstrap_send_fails(self):
        ws = AsyncMock()
        ws.send_json.side_effect = ConnectionResetError('socket closed')

        with self.assertRaises(ConnectionResetError):
            await self.hub.register(ws)

        self.assertNotIn(ws, self.hub._clients)

    async def test_set_execution_paused_updates_summary_and_broadcasts(self):
        tradable_hub = LiveDashboardHub(
            pairs=self.hub.pairs,
            params=StrategyParams(),
            interval=60,
            zone_history_days=30,
            track_positions=True,
            balance=10000.0,
            risk_pct=0.01,
            account_currency='USD',
            execute_orders=True,
            strategy_label=None,
            client_id=None,
            port=8080,
        )
        tradable_hub._broadcast = AsyncMock()

        try:
            state = await tradable_hub.set_execution_paused(True)
        finally:
            tradable_hub._scan_executor.shutdown(wait=True)

        self.assertFalse(state['summary']['execution_enabled'])
        self.assertTrue(state['summary']['execution_available'])
        self.assertTrue(state['summary']['execution_paused'])
        self.assertEqual(state['log'][-1]['message'], 'New trade execution paused from dashboard')
        tradable_hub._broadcast.assert_awaited_once()

    def test_build_summary_includes_fill_progress(self):
        summary = self.hub._build_summary(status='live')
        self.assertIn('fill', summary)
        self.assertEqual(summary['fill']['status'], 'idle')
        self.assertEqual(summary['fill']['items_requested'], 0)

    async def test_set_execution_paused_rejects_scan_only_mode(self):
        with self.assertRaisesRegex(RuntimeError, 'scan-only mode'):
            await self.hub.set_execution_paused(True)

    async def test_handle_signal_skips_order_submission_when_execution_paused(self):
        tradable_hub = LiveDashboardHub(
            pairs=self.hub.pairs,
            params=StrategyParams(),
            interval=60,
            zone_history_days=30,
            track_positions=True,
            balance=10000.0,
            risk_pct=0.01,
            account_currency='USD',
            execute_orders=True,
            strategy_label=None,
            client_id=None,
            port=8080,
        )
        tradable_hub._loop = asyncio.get_running_loop()
        tradable_hub._broadcast = AsyncMock()
        tradable_hub._pair_rows = dict(self.hub._pair_rows)
        signal = SimpleNamespace(
            pair='EURUSD',
            time=pd.Timestamp('2026-03-10 16:00:00', tz='UTC'),
            direction='LONG',
            entry_price=1.1000,
            sl_price=1.0950,
            tp_price=1.1100,
            zone_upper=1.1010,
            zone_lower=1.0990,
            zone_strength='major',
            zone_type='support',
        )
        size_plan = SimpleNamespace(
            units=10000,
            risk_amount=100.0,
            account_currency='USD',
            notional_account=11000.0,
        )

        try:
            await tradable_hub.set_execution_paused(True)
            tradable_hub._broadcast.reset_mock()

            with patch('fx_sr.live_web.get_entry_block', return_value=None), \
                    patch('fx_sr.live_web.build_live_size_plans', return_value=[size_plan]), \
                    patch('fx_sr.live_web.record_detected_signals') as record_detected_mock, \
                    patch('fx_sr.live_web.execute_signal_plans') as execute_mock, \
                    patch('fx_sr.live_web.record_execution_results') as record_execution_mock:
                await tradable_hub._handle_signal(signal, source='hourly')
        finally:
            tradable_hub._scan_executor.shutdown(wait=True)

        execute_mock.assert_not_called()
        record_detected_mock.assert_called_once()
        record_execution_mock.assert_called_once()
        results = record_execution_mock.call_args.args[2]
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'SKIPPED')
        self.assertEqual(results[0].note, 'execution paused')

    async def test_replay_startup_bars_is_single_shot_and_flips_flag(self):
        """Regression test for the livelock fix.

        The old drain loop iterated ``while True`` until the buffer was
        empty, but new bars arriving during each drain phase refilled the
        buffer faster than it drained at production tick rates, so
        ``_startup_bar_buffering`` never flipped and the persistence thread
        never started. The fix is to snapshot the buffer and flip the flag
        *inside the same lock* so new bars post-flip bypass the buffer and
        go through the main ``_process_realtime_bar`` path.
        """

        # Arrange: buffer 50 synthetic startup bars and confirm initial state.
        self.hub._startup_bar_buffering = True
        self.hub._startup_bar_buffer = []
        for minute in range(50):
            ts = pd.Timestamp('2026-03-10 12:00:00', tz='UTC') + pd.Timedelta(seconds=5 * minute)
            self.hub._startup_bar_buffer.append((
                ts, minute, 'EURUSD',
                _bar(ts, 1.1000, 1.1001, 1.0999, 1.1000, 1),
            ))
        self.assertTrue(self.hub._startup_bar_buffering)
        self.assertEqual(len(self.hub._startup_bar_buffer), 50)

        with patch.object(self.hub, '_ingest_realtime_bar', return_value=(1.1000, False)) as ingest_mock, \
                patch.object(self.hub, '_run_post_ingest_work', new=AsyncMock()) as post_mock:
            replayed = await self.hub._replay_startup_bars()

            # Flag flipped, buffer empty, every bar processed exactly once.
            self.assertEqual(replayed, 50)
            self.assertFalse(
                self.hub._startup_bar_buffering,
                'flag must be False after replay, or persistence thread never starts',
            )
            self.assertEqual(self.hub._startup_bar_buffer, [])
            self.assertEqual(ingest_mock.call_count, 50)
            self.assertEqual(post_mock.await_count, 50)

            # Contract check: bars arriving *after* replay must bypass the
            # buffer. The main _handle_bar_update path reads
            # _startup_bar_buffering; if it's False, it calls
            # _process_realtime_bar directly.
            post_replay_bar = _bar(
                pd.Timestamp('2026-03-10 13:00:00', tz='UTC'),
                1.2000, 1.2001, 1.1999, 1.2000, 1,
            )
            await self.hub._handle_bar_update('EURUSD', post_replay_bar)
            self.assertEqual(
                self.hub._startup_bar_buffer, [],
                'bars arriving post-replay must not re-enter the startup buffer',
            )
            self.assertEqual(ingest_mock.call_count, 51)
            self.assertEqual(post_mock.await_count, 51)

    async def test_replay_startup_bars_orders_buffer_before_new_bars(self):
        self.hub._startup_bar_buffering = True
        self.hub._startup_bar_buffer = []
        buffered_times = [
            pd.Timestamp('2026-03-10 12:00:00', tz='UTC'),
            pd.Timestamp('2026-03-10 12:00:05', tz='UTC'),
            pd.Timestamp('2026-03-10 12:00:10', tz='UTC'),
        ]
        for idx, ts in enumerate(buffered_times):
            self.hub._startup_bar_buffer.append((
                ts, idx, 'EURUSD',
                _bar(ts, 1.1000, 1.1001, 1.0999, 1.1000, 1),
            ))

        processed_times = []

        def _capture_ingest(_pair, bar):
            processed_times.append(pd.Timestamp(bar.time))
            return float(getattr(bar, 'close', 0) or 0), False

        async def _slow_post_work(_pair, _price, _minute_completed):
            await asyncio.sleep(0.01)

        with patch.object(self.hub, '_ingest_realtime_bar', side_effect=_capture_ingest), \
                patch.object(self.hub, '_run_post_ingest_work', side_effect=_slow_post_work):
            replay_task = asyncio.create_task(self.hub._replay_startup_bars())

            fresh_time = pd.Timestamp('2026-03-10 12:00:15', tz='UTC')
            fresh_task = asyncio.create_task(
                self.hub._handle_bar_update(
                    'EURUSD',
                    _bar(fresh_time, 1.2000, 1.2001, 1.1999, 1.2000, 1),
                )
            )
            await asyncio.wait_for(asyncio.gather(replay_task, fresh_task), timeout=1)

        self.assertEqual(processed_times, buffered_times + [fresh_time])

    async def test_quote_update_delay_does_not_block_accumulator_ingest(self):
        self.hub._startup_bar_buffering = False
        self.hub._realtime_bars_enabled = True
        first_quote_started = asyncio.Event()
        release_first_quote = asyncio.Event()
        quote_calls = 0

        async def _slow_first_quote(_pair, _price):
            nonlocal quote_calls
            quote_calls += 1
            if quote_calls == 1:
                first_quote_started.set()
                await release_first_quote.wait()

        with patch.object(self.hub, '_handle_quote_update', side_effect=_slow_first_quote), \
                patch.object(self.hub, '_handle_minute_bar_complete', new=AsyncMock()):
            first_task = asyncio.create_task(self.hub._handle_bar_update(
                'EURUSD',
                _bar(pd.Timestamp('2026-03-10 14:00:05', tz='UTC'), 1.10, 1.11, 1.09, 1.105, 1),
            ))
            await asyncio.wait_for(first_quote_started.wait(), timeout=1)

            second_task = asyncio.create_task(self.hub._handle_bar_update(
                'EURUSD',
                _bar(pd.Timestamp('2026-03-10 14:01:05', tz='UTC'), 1.11, 1.12, 1.10, 1.115, 1),
            ))
            await asyncio.sleep(0.05)

            completed = self.hub._accumulator._completed_minutes.get('EURUSD')
            self.assertIsNotNone(completed)
            self.assertIn(pd.Timestamp('2026-03-10 14:00:00', tz='UTC'), completed.index)

            release_first_quote.set()
            await asyncio.wait_for(asyncio.gather(first_task, second_task), timeout=1)

    async def test_minute_scan_delay_does_not_block_accumulator_ingest(self):
        self.hub._startup_bar_buffering = False
        self.hub._realtime_bars_enabled = True
        self.hub._backfill_done = True
        first_scan_started = asyncio.Event()
        release_first_scan = asyncio.Event()
        scan_calls = 0

        async def _slow_first_scan(_pair, _price):
            nonlocal scan_calls
            scan_calls += 1
            if scan_calls == 1:
                first_scan_started.set()
                await release_first_scan.wait()

        with patch.object(self.hub, '_handle_quote_update', new=AsyncMock()), \
                patch.object(self.hub, '_handle_minute_bar_complete', side_effect=_slow_first_scan):
            await self.hub._handle_bar_update(
                'EURUSD',
                _bar(pd.Timestamp('2026-03-10 14:00:05', tz='UTC'), 1.10, 1.11, 1.09, 1.105, 1),
            )
            second_task = asyncio.create_task(self.hub._handle_bar_update(
                'EURUSD',
                _bar(pd.Timestamp('2026-03-10 14:01:05', tz='UTC'), 1.11, 1.12, 1.10, 1.115, 1),
            ))
            await asyncio.wait_for(first_scan_started.wait(), timeout=1)

            third_task = asyncio.create_task(self.hub._handle_bar_update(
                'EURUSD',
                _bar(pd.Timestamp('2026-03-10 14:02:05', tz='UTC'), 1.12, 1.13, 1.11, 1.125, 1),
            ))
            await asyncio.sleep(0.05)

            completed = self.hub._accumulator._completed_minutes.get('EURUSD')
            self.assertIsNotNone(completed)
            self.assertIn(pd.Timestamp('2026-03-10 14:00:00', tz='UTC'), completed.index)
            self.assertIn(pd.Timestamp('2026-03-10 14:01:00', tz='UTC'), completed.index)

            release_first_scan.set()
            await asyncio.wait_for(asyncio.gather(second_task, third_task), timeout=1)

    async def test_missing_realtime_bar_timestamp_is_diagnosed_but_quote_still_updates(self):
        self.hub._startup_bar_buffering = False
        self.hub._realtime_bars_enabled = True
        bar = SimpleNamespace(open_=1.10, high=1.11, low=1.09, close=1.105, volume=1)

        with patch.object(self.hub, '_handle_quote_update', new=AsyncMock()) as quote_mock:
            await self.hub._handle_bar_update('EURUSD', bar)

        quote_mock.assert_awaited_once_with('EURUSD', 1.105)
        self.assertEqual(
            self.hub._realtime_bar_skip_counts['EURUSD']['missing_timestamp'],
            1,
        )
        self.assertNotIn('EURUSD', self.hub._realtime_bar_ingest_count)

    async def test_start_enables_persistence_before_startup_replay(self):
        events: list[str] = []

        async def _backfill():
            events.append('backfill')

        def _start_persistence(pair_ticker_map):
            events.append('persist')
            self.assertEqual(pair_ticker_map, {'EURUSD': 'EURUSD=X'})

        def _hydrate_execution_activity():
            events.append('hydrate')

        async def _replay_startup_bars():
            events.append('replay')
            return 0

        with patch.object(self.hub, '_ensure_quote_stream_started', return_value=False), \
                patch('fx_sr.live_web.start_background_writer'), \
                patch('fx_sr.live_history.record_system_event'), \
                patch.object(self.hub, '_run_backfill', new=AsyncMock(side_effect=_backfill)), \
                patch.object(self.hub._accumulator, 'start_persistence', side_effect=_start_persistence), \
                patch.object(self.hub, '_compute_data_health', return_value={'overall': 'ok'}), \
                patch.object(self.hub, '_data_health_loop', new=AsyncMock(return_value=None)), \
                patch.object(self.hub, '_hydrate_execution_activity', side_effect=_hydrate_execution_activity), \
                patch.object(self.hub, '_replay_startup_bars', new=AsyncMock(side_effect=_replay_startup_bars)), \
                patch.object(self.hub, '_housekeeping_loop', new=AsyncMock(return_value=None)), \
                patch.object(self.hub, '_export_state', return_value={}), \
                patch.object(self.hub, '_broadcast_log', new=AsyncMock()), \
                patch.object(self.hub, '_broadcast', new=AsyncMock()):
            await self.hub.start()

        self.assertEqual(events, ['backfill', 'persist', 'hydrate', 'replay'])

    async def test_data_health_reports_dead_persistence_thread(self):
        self.hub._backfill_done = True
        self.hub._backfill_completed_at = pd.Timestamp.now(tz='UTC') - pd.Timedelta(minutes=10)
        self.hub._accumulator._persist_enabled = True

        stale_summary = pd.DataFrame([{
            'ticker': 'EURUSD=X',
            'interval': '1m',
            'first_ts': pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=1),
            'last_ts': pd.Timestamp.now(tz='UTC') - pd.Timedelta(hours=1),
            'bars': 100,
        }])

        with patch('fx_sr.live_web.fx_market_is_open', return_value=True), \
                patch('fx_sr.live_web.get_cache_summary', return_value=stale_summary):
            health = self.hub._compute_data_health()

        self.assertEqual(health['overall'], 'stale')
        self.assertEqual(health['pipeline_status'], 'persistence_stopped')
        self.assertFalse(health['persist_thread_alive'])
        self.assertIn('persistence thread is not running', health['pipeline_message'])


class AccountHistoryApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_account_history_api_refreshes_today_snapshot_and_exposes_equity(self):
        snapshots = [
            {
                'date': '2026-03-29',
                'balance': 10000.0,
                'daily_pnl_gbp': 12.0,
                'currency': 'GBP',
            },
        ]
        today_snapshot = {
            'date': '2026-03-30',
            'balance': 10123.45,
            'daily_pnl_gbp': 5.0,
            'currency': 'GBP',
            'equity': 10123.45,
        }

        with patch(
            'fx_sr.live_history.load_daily_snapshots',
            return_value=snapshots,
        ) as load_snapshots_mock, patch(
            'fx_sr.live_history.get_or_fetch_today_snapshot',
            return_value=today_snapshot,
        ) as today_snapshot_mock, patch(
            'fx_sr.live_web.fx_market_is_open',
            return_value=True,
        ):
            response = await _account_history_api(SimpleNamespace(app={}, query={}))

        load_snapshots_mock.assert_called_once_with()
        today_snapshot_mock.assert_called_once_with(force_refresh=True)

        payload = json.loads(response.text)
        self.assertEqual(len(payload['snapshots']), 2)
        self.assertEqual(payload['snapshots'][1]['date'], '2026-03-30')
        self.assertNotIn('equity', payload['snapshots'][0])
        self.assertEqual(payload['snapshots'][1]['equity'], 10123.45)

    async def test_account_history_api_uses_cached_today_without_forced_refresh(self):
        today = date.today().isoformat()
        snapshots = [
            {
                'date': today,
                'balance': 10000.0,
                'daily_pnl_gbp': 12.0,
                'currency': 'GBP',
            },
        ]

        with patch(
            'fx_sr.live_history.load_daily_snapshots',
            return_value=snapshots,
        ) as load_snapshots_mock, patch(
            'fx_sr.live_history.get_or_fetch_today_snapshot',
        ) as today_snapshot_mock, patch(
            'fx_sr.live_web.fx_market_is_open',
            return_value=False,
        ):
            response = await _account_history_api(SimpleNamespace(app={}, query={}))

        load_snapshots_mock.assert_called_once_with()
        today_snapshot_mock.assert_not_called()

        payload = json.loads(response.text)
        self.assertEqual(len(payload['snapshots']), 1)
        self.assertEqual(payload['snapshots'][0]['date'], today)

    async def test_account_history_api_falls_back_when_live_snapshot_fetch_times_out(self):
        snapshots = [
            {
                'date': '2026-03-29',
                'balance': 10000.0,
                'daily_pnl_gbp': 12.0,
                'currency': 'GBP',
            },
        ]

        def slow_fetch(*, force_refresh: bool = False):
            import time

            time.sleep(0.2)
            return {
                'date': '2026-03-30',
                'balance': 10123.45,
                'daily_pnl_gbp': 5.0,
                'currency': 'GBP',
                'equity': 10123.45,
            }

        with patch(
            'fx_sr.live_history.load_daily_snapshots',
            return_value=snapshots,
        ) as load_snapshots_mock, patch(
            'fx_sr.live_history.get_or_fetch_today_snapshot',
            side_effect=slow_fetch,
        ) as today_snapshot_mock, patch(
            'fx_sr.live_web.fx_market_is_open',
            return_value=True,
        ), patch(
            'fx_sr.live_web._ACCOUNT_HISTORY_REFRESH_TIMEOUT',
            0.05,
        ):
            response = await _account_history_api(SimpleNamespace(app={}, query={'refresh': '1'}))

        load_snapshots_mock.assert_called_once_with()
        today_snapshot_mock.assert_called_once()
        payload = json.loads(response.text)
        self.assertEqual(len(payload['snapshots']), 1)
        self.assertEqual(payload['snapshots'][0]['date'], '2026-03-29')


class WebsocketRequestValidationTests(unittest.TestCase):
    def _request(self, *, origin='http://127.0.0.1:8765'):
        return SimpleNamespace(
            app={},
            query={},
            headers={'Origin': origin} if origin is not None else {},
            scheme='http',
            host='127.0.0.1:8765',
        )

    def test_valid_origin_is_accepted(self):
        _validate_websocket_request(self._request())

    def test_no_origin_is_accepted(self):
        _validate_websocket_request(self._request(origin=None))

    def test_localhost_aliases_are_accepted(self):
        _validate_websocket_request(self._request(origin='http://localhost:8765'))
        _validate_websocket_request(self._request(origin='http://127.0.0.1:8765'))

    def test_mismatched_origin_is_rejected(self):
        with self.assertRaises(web.HTTPForbidden):
            _validate_websocket_request(self._request(origin='http://10.0.0.1:8765'))


class ExecutionModeEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_execution_mode_endpoint_updates_hub_state(self):
        hub = LiveDashboardHub(
            pairs={
                'EURUSD': {
                    'name': 'EUR/USD',
                    'ticker': 'EURUSD=X',
                    'decimals': 5,
                },
            },
            params=StrategyParams(),
            interval=60,
            zone_history_days=30,
            track_positions=True,
            balance=10000.0,
            risk_pct=0.01,
            account_currency='USD',
            execute_orders=True,
            strategy_label=None,
            client_id=None,
            port=8765,
        )
        hub._broadcast = AsyncMock()
        request = SimpleNamespace(
            app={'hub': hub},
            query={},
            headers={'Origin': 'http://127.0.0.1:8765'},
            scheme='http',
            host='127.0.0.1:8765',
            json=AsyncMock(return_value={'paused': True}),
        )

        try:
            response = await _set_execution_mode(request)
        finally:
            hub._scan_executor.shutdown(wait=True)

        payload = json.loads(response.text)
        self.assertEqual(response.status, 200)
        self.assertTrue(payload['state']['summary']['execution_paused'])
        self.assertFalse(payload['state']['summary']['execution_enabled'])


class BacktestRerunTests(unittest.IsolatedAsyncioTestCase):
    async def test_backtest_client_id_base_offsets_live_60(self):
        hub = LiveDashboardHub(
            pairs={
                'EURUSD': {
                    'name': 'EUR/USD',
                    'ticker': 'EURUSD=X',
                    'decimals': 5,
                },
            },
            params=StrategyParams(),
            interval=60,
            zone_history_days=30,
            track_positions=True,
            balance=10000.0,
            risk_pct=0.01,
            account_currency='USD',
            execute_orders=False,
            strategy_label=None,
            client_id=60,
            port=8080,
        )
        try:
            self.assertEqual(hub._backtest_client_id_base(), 4060)
        finally:
            hub._scan_executor.shutdown(wait=True)

    async def test_build_backtest_cli_args_include_dashboard_settings(self):
        hub = LiveDashboardHub(
            pairs={
                'EURUSD': {
                    'name': 'EUR/USD',
                    'ticker': 'EURUSD=X',
                    'decimals': 5,
                },
            },
            params=StrategyParams(),
            interval=60,
            zone_history_days=45,
            track_positions=True,
            balance=1000.0,
            risk_pct=0.02,
            account_currency='USD',
            execute_orders=False,
            strategy_label=None,
            client_id=12,
            port=8080,
        )
        try:
            args = hub._build_backtest_cli_args()
            self.assertIn('--ibkr-client-id', args)
            client_idx = args.index('--ibkr-client-id')
            self.assertEqual(args[client_idx + 1], '3012')
            self.assertIn('--zone-history', args)
            zone_idx = args.index('--zone-history')
            self.assertEqual(args[zone_idx + 1], '45')
            self.assertIn('--pair', args)
            pair_idx = args.index('--pair')
            self.assertEqual(args[pair_idx + 1], 'EURUSD')
            self.assertIn('--risk-pct', args)
            risk_idx = args.index('--risk-pct')
            self.assertEqual(args[risk_idx + 1], '2.0')
            self.assertIn('--balance', args)
            balance_idx = args.index('--balance')
            self.assertEqual(args[balance_idx + 1], '1000.0')
            self.assertIn('--blocked-hours', args)
            blocked_hours_idx = args.index('--blocked-hours')
            self.assertIn('2', args[blocked_hours_idx + 1 : blocked_hours_idx + 3])
        finally:
            hub._scan_executor.shutdown(wait=True)

    async def test_run_backtest_rejects_concurrent_invocations(self):
        hub = LiveDashboardHub(
            pairs={
                'EURUSD': {
                    'name': 'EUR/USD',
                    'ticker': 'EURUSD=X',
                    'decimals': 5,
                },
            },
            params=StrategyParams(),
            interval=60,
            zone_history_days=30,
            track_positions=True,
            balance=10000.0,
            risk_pct=0.01,
            account_currency='USD',
            execute_orders=False,
            strategy_label=None,
            client_id=12,
            port=8080,
        )
        running = asyncio.create_task(asyncio.sleep(30))
        hub._backtest_task = running
        try:
            result = await hub.run_backtest()
            self.assertEqual(result['status'], 'running')
            self.assertIn('already', result['message'])
        finally:
            running.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await running
            hub._scan_executor.shutdown(wait=True)


class WindowsEventLoopPolicyTests(unittest.TestCase):
    def test_windows_uses_selector_policy_when_needed(self):
        selector_policy = type('SelectorPolicy', (), {})

        with patch('fx_sr.live_web.sys.platform', 'win32'), \
                patch('fx_sr.live_web.asyncio.WindowsSelectorEventLoopPolicy', selector_policy, create=True), \
                patch('fx_sr.live_web.asyncio.get_event_loop_policy', return_value=object()), \
                patch('fx_sr.live_web.asyncio.set_event_loop_policy') as set_policy_mock:
            _configure_windows_event_loop_policy()

        set_policy_mock.assert_called_once()
        self.assertIsInstance(set_policy_mock.call_args.args[0], selector_policy)

    def test_windows_does_not_reset_selector_policy_if_already_active(self):
        selector_policy = type('SelectorPolicy', (), {})

        with patch('fx_sr.live_web.sys.platform', 'win32'), \
                patch('fx_sr.live_web.asyncio.WindowsSelectorEventLoopPolicy', selector_policy, create=True), \
                patch('fx_sr.live_web.asyncio.get_event_loop_policy', return_value=selector_policy()), \
                patch('fx_sr.live_web.asyncio.set_event_loop_policy') as set_policy_mock:
            _configure_windows_event_loop_policy()

        set_policy_mock.assert_not_called()


if __name__ == '__main__':
    unittest.main()
