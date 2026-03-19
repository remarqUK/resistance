import asyncio
import json
import contextlib
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pandas as pd
from aiohttp import web

from fx_sr.live import PairScanRow
from fx_sr.live_web import (
    ALERT_LIMIT,
    EXECUTION_LIMIT,
    LiveDashboardHub,
    _configure_windows_event_loop_policy,
    _set_execution_mode,
    _validate_websocket_request,
)
from fx_sr.strategy import StrategyParams, Trade


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
        captured = {}

        def _capture_signal_eval(pair, price, tracked_pairs=None, blocked_pairs=None, hourly_df=None):
            captured['pair'] = pair
            captured['price'] = price
            captured['tracked_pairs'] = tracked_pairs
            captured['blocked_pairs'] = blocked_pairs
            captured['hourly_df'] = hourly_df.copy()
            return None

        with patch('fx_sr.positions.check_exit', return_value=None), \
                patch('fx_sr.positions._save_bar_tracking') as save_tracking_mock, \
                patch.object(self.hub._scanner, 'evaluate_completed_bar', side_effect=_capture_signal_eval):
            await self.hub._handle_hourly_bar_complete(
                'EURUSD',
                pd.Timestamp('2026-03-10 14:00:00', tz='UTC'),
            )

        completed_time = pd.Timestamp('2026-03-10 14:00:00', tz='UTC')
        self.assertEqual(captured['pair'], 'EURUSD')
        self.assertAlmostEqual(captured['price'], 1.1010)
        self.assertEqual(captured['tracked_pairs'], {'EURUSD': {'LONG'}})
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

    def test_mismatched_origin_is_rejected(self):
        with self.assertRaises(web.HTTPForbidden):
            _validate_websocket_request(self._request(origin='http://localhost:8765'))


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
