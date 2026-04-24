import sys
import types
import sys
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

import fx_sr.ibkr as ibkr
import fx_sr.db as db_module
from tests._test_db_helpers import temporary_test_database


def _fake_ib_async_module():
    module = types.ModuleType('ib_async')
    module.util = types.SimpleNamespace(df=lambda bars: bars)
    return module


class IbkrHistoricalFetchTests(unittest.TestCase):
    def test_get_connection_without_fallback_uses_exact_client_id(self):
        connect_calls = []

        class FailingIb:
            def connect(self, _host, _port, clientId, timeout=5):
                connect_calls.append(clientId)
                raise RuntimeError('Unable to connect as the client id is already in use')

            def isConnected(self):
                return False

            def disconnect(self):
                pass

        fake_ib_async = _fake_ib_async_module()
        fake_ib_async.IB = FailingIb

        with patch.dict(sys.modules, {'ib_async': fake_ib_async}):
            ibkr.disconnect()
            previous = ibkr.set_client_id_fallback(False)
            try:
                ib, connected = ibkr._get_connection(client_id=99, retries=3)
            finally:
                ibkr.set_client_id_fallback(previous)

        self.assertIsNone(ib)
        self.assertFalse(connected)
        self.assertEqual(connect_calls, [99, 99, 99])

    def test_get_connection_with_fallback_advances_client_id(self):
        connect_calls = []

        class FailingIb:
            def connect(self, _host, _port, clientId, timeout=5):
                connect_calls.append(clientId)
                raise RuntimeError('Unable to connect as the client id is already in use')

            def isConnected(self):
                return False

            def disconnect(self):
                pass

        fake_ib_async = _fake_ib_async_module()
        fake_ib_async.IB = FailingIb

        with patch.dict(sys.modules, {'ib_async': fake_ib_async}):
            ibkr.disconnect()
            previous = ibkr.set_client_id_fallback(True)
            previous_offset = ibkr._LAST_FALLBACK_OFFSET
            ibkr._LAST_FALLBACK_OFFSET = 0
            try:
                ib, connected = ibkr._get_connection(client_id=99, retries=3)
            finally:
                ibkr._LAST_FALLBACK_OFFSET = previous_offset
                ibkr.set_client_id_fallback(previous)

        self.assertIsNone(ib)
        self.assertFalse(connected)
        self.assertEqual(connect_calls, [99, 100, 101])

    def test_get_connection_logs_successful_fallback_client_id(self):
        connect_calls = []

        class FallbackIb:
            def __init__(self):
                self.connected = False

            def connect(self, _host, _port, clientId, timeout=5):
                connect_calls.append(clientId)
                if clientId == 100:
                    raise RuntimeError('Unable to connect as the client id is already in use')
                self.connected = True

            def isConnected(self):
                return self.connected

            def disconnect(self):
                self.connected = False

        fake_ib_async = _fake_ib_async_module()
        fake_ib_async.IB = FallbackIb

        with patch.dict(sys.modules, {'ib_async': fake_ib_async}), \
                patch('builtins.print') as print_mock:
            ibkr.disconnect()
            previous_fallback = ibkr.set_client_id_fallback(True)
            previous_offset = ibkr._LAST_FALLBACK_OFFSET
            ibkr._LAST_FALLBACK_OFFSET = 0
            try:
                ib, connected = ibkr._get_connection(client_id=100, retries=3)
            finally:
                ibkr.disconnect()
                ibkr._LAST_FALLBACK_OFFSET = previous_offset
                ibkr.set_client_id_fallback(previous_fallback)

        self.assertIsNotNone(ib)
        self.assertTrue(connected)
        self.assertEqual(connect_calls, [100, 101])
        print_mock.assert_called_once_with(
            '  IBKR connected with fallback client ID 101 (requested 100).'
        )

    def test_fetch_historical_uses_connection_with_client_id(self):
        fake_ib_async = _fake_ib_async_module()
        ib = MagicMock()
        ib.reqHistoricalData.return_value = []

        with patch.dict(sys.modules, {'ib_async': fake_ib_async}), \
                patch('fx_sr.ibkr._get_connection', return_value=(ib, True)) as get_connection, \
                patch('fx_sr.ibkr._make_contract', return_value=object()):
            result = ibkr.fetch_historical('EURUSD=X', '1h', 5)

        self.assertIsNone(result)
        get_connection.assert_called_once_with(client_id=None)

    def test_fetch_historical_returns_none_on_broker_failure(self):
        fake_ib_async = _fake_ib_async_module()
        ib = MagicMock()
        ib.reqHistoricalData.side_effect = RuntimeError('boom')

        with patch.dict(sys.modules, {'ib_async': fake_ib_async}), \
                patch('fx_sr.ibkr._get_connection', return_value=(ib, True)), \
                patch('fx_sr.ibkr._make_contract', return_value=object()), \
                patch('builtins.print') as print_mock:
            result = ibkr.fetch_historical('EURUSD=X', '1h', 5)

        self.assertIsNone(result)
        print_mock.assert_called_once()

    def test_fetch_historical_formats_end_datetime_for_minute_requests(self):
        fake_ib_async = _fake_ib_async_module()
        fake_ib_async.util = types.SimpleNamespace(
            df=lambda bars: pd.DataFrame([
                {
                    'date': '2026-03-10 11:59:00+00:00',
                    'open': 1.1,
                    'high': 1.2,
                    'low': 1.0,
                    'close': 1.15,
                    'volume': 0.0,
                }
            ])
        )
        ib = MagicMock()
        ib.reqHistoricalData.return_value = [object()]

        with patch.dict(sys.modules, {'ib_async': fake_ib_async}), \
                patch('fx_sr.ibkr._get_connection', return_value=(ib, True)), \
                patch('fx_sr.ibkr._make_contract', return_value=object()):
            result = ibkr.fetch_historical(
                'EURUSD=X',
                '1m',
                30,
                end_datetime=pd.Timestamp('2026-03-10 12:00:00', tz='UTC'),
            )

        self.assertFalse(result.empty)
        _, kwargs = ib.reqHistoricalData.call_args
        self.assertEqual(kwargs['durationStr'], '7 D')
        self.assertEqual(kwargs['endDateTime'], '20260310 12:00:00 UTC')

    def test_fetch_execution_quote_prefers_depth_top_of_book(self):
        snapshot = {
            'best_bid': 1.0998,
            'best_ask': 1.1000,
            'captured_at': pd.Timestamp('2026-03-15 09:00:00', tz='UTC'),
        }

        with patch('fx_sr.ibkr.fetch_market_depth_snapshot', return_value=snapshot):
            quote = ibkr.fetch_execution_quote('EURUSD', prefer_depth=True)

        self.assertIsNotNone(quote)
        self.assertEqual(quote.source, 'l2')
        self.assertAlmostEqual(quote.bid, 1.0998)
        self.assertAlmostEqual(quote.ask, 1.1000)
        self.assertAlmostEqual(quote.mid, 1.0999)
        self.assertAlmostEqual(quote.spread, 0.0002)

    def test_fetch_execution_quote_falls_back_to_l1_snapshot(self):
        ticker = types.SimpleNamespace(bid=1.0998, ask=1.1000)
        ib = MagicMock()
        ib.reqMktData.return_value = ticker

        with patch('fx_sr.ibkr.fetch_market_depth_snapshot', return_value=None), \
                patch('fx_sr.ibkr._get_connection', return_value=(ib, True)), \
                patch('fx_sr.ibkr._make_contract', return_value=object()):
            quote = ibkr.fetch_execution_quote('EURUSD', prefer_depth=True)

        self.assertIsNotNone(quote)
        self.assertEqual(quote.source, 'l1')
        self.assertAlmostEqual(quote.bid, 1.0998)
        self.assertAlmostEqual(quote.ask, 1.1000)

    def test_fetch_fx_order_statuses_reads_open_and_completed_snapshots(self):
        open_trade = types.SimpleNamespace(
            contract=types.SimpleNamespace(secType='CASH', localSymbol='EUR.USD', symbol='EUR', currency='USD'),
            order=types.SimpleNamespace(
                orderId=101,
                parentId=0,
                orderRef='fxsr',
                orderType='MKT',
                action='BUY',
                totalQuantity=10000,
            ),
            orderStatus=types.SimpleNamespace(
                status='Submitted',
                avgFillPrice=1.1002,
                filled=4000,
                remaining=6000,
            ),
        )
        completed_trade = types.SimpleNamespace(
            contract=types.SimpleNamespace(secType='CASH', localSymbol='GBP.USD', symbol='GBP', currency='USD'),
            order=types.SimpleNamespace(
                orderId=201,
                parentId=0,
                orderRef='fxsr2',
                orderType='MKT',
                action='SELL',
                totalQuantity=8000,
            ),
            orderStatus=types.SimpleNamespace(
                status='Filled',
                avgFillPrice=1.2501,
                filled=8000,
                remaining=0,
            ),
        )
        ib = MagicMock()
        ib.openTrades.return_value = [open_trade]
        ib.reqCompletedOrders.return_value = [completed_trade]

        with patch('fx_sr.ibkr._get_connection', return_value=(ib, True)):
            rows = ibkr.fetch_fx_order_statuses(order_ids={101, 201})

        rows_by_id = {row['order_id']: row for row in rows}
        self.assertEqual(rows_by_id[101]['status'], 'Submitted')
        self.assertEqual(rows_by_id[101]['filled_units'], 4000.0)
        self.assertEqual(rows_by_id[101]['remaining_units'], 6000.0)
        self.assertEqual(rows_by_id[101]['total_units'], 10000.0)
        self.assertEqual(rows_by_id[201]['status'], 'Filled')
        self.assertEqual(rows_by_id[201]['filled_units'], 8000.0)
        self.assertEqual(rows_by_id[201]['remaining_units'], 0.0)


class StreamRealtimeBarsBootTests(unittest.TestCase):
    """Boot-phase failures in stream_realtime_bars must fail closed.

    Silently retrying a broken initial subscribe hides a degraded stream
    behind a "quote thread started" banner — the dashboard renders normally
    while nothing ever flows. Only after one full subscription set is
    healthy should we enter the auto-reconnect loop.
    """

    def test_boot_connection_failure_raises(self):
        import threading as _threading

        stop_event = _threading.Event()
        with patch.object(ibkr, '_get_connection', return_value=(None, False)):
            with self.assertRaises(RuntimeError) as ctx:
                ibkr.stream_realtime_bars(
                    pairs=['EURUSD'],
                    on_bar=lambda _p, _b: None,
                    stop_event=stop_event,
                    client_id=99,
                )
        self.assertIn('startup', str(ctx.exception).lower())

    def test_boot_subscribe_failure_raises_with_offending_pair(self):
        import threading as _threading

        stop_event = _threading.Event()
        mock_ib = MagicMock()
        mock_ib.qualifyContracts = MagicMock()
        mock_ib.reqRealTimeBars = MagicMock(
            side_effect=RuntimeError('pair not found')
        )
        mock_ib.sleep = MagicMock()
        mock_ib.isConnected = MagicMock(return_value=True)

        with patch.object(ibkr, '_get_connection', return_value=(mock_ib, True)), \
                patch.object(ibkr, '_make_contract', return_value=MagicMock()), \
                patch.object(ibkr, 'disconnect'):
            with self.assertRaises(RuntimeError) as ctx:
                ibkr.stream_realtime_bars(
                    pairs=['EURUSD'],
                    on_bar=lambda _p, _b: None,
                    stop_event=stop_event,
                    client_id=99,
                )
        message = str(ctx.exception)
        self.assertIn('EURUSD', message)
        self.assertIn('pair not found', message)


class LogOrderEventTests(unittest.TestCase):
    def setUp(self):
        self._db_ctx = temporary_test_database()
        self.db_path = self._db_ctx.__enter__()
        db_module.init_db(self.db_path)

    def tearDown(self):
        if self._db_ctx is not None:
            self._db_ctx.__exit__(None, None, None)

    @patch('fx_sr.ibkr.get_db_path')
    def test_log_order_event_inserts_row(self, mock_db_path):
        mock_db_path.return_value = self.db_path
        from fx_sr.ibkr import log_order_event

        log_order_event(
            function_name='submit_fx_market_bracket_order',
            pair='EURUSD',
            direction='LONG',
            action='submit',
            request_data={'quantity': 10000, 'tp': 1.15, 'sl': 1.13},
            response_data={'order_id': 123, 'status': 'Filled'},
            order_ids=[123, 124, 125],
            duration_ms=342.5,
        )

        conn = db_module._connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT function_name, pair, direction, action, request_json, "
                "response_json, order_ids, duration_ms, error "
                "FROM order_audit_log ORDER BY id DESC LIMIT 1"
            ).fetchone()
            self.assertEqual(row[0], 'submit_fx_market_bracket_order')
            self.assertEqual(row[1], 'EURUSD')
            self.assertEqual(row[2], 'LONG')
            self.assertEqual(row[3], 'submit')
            self.assertIn('10000', row[4])
            self.assertIn('123', row[5])
            self.assertEqual(row[6], '123,124,125')
            self.assertAlmostEqual(row[7], 342.5)
            self.assertIsNone(row[8])
        finally:
            conn.close()

    @patch('fx_sr.ibkr.get_db_path')
    def test_log_order_event_stores_error(self, mock_db_path):
        mock_db_path.return_value = self.db_path
        from fx_sr.ibkr import log_order_event

        log_order_event(
            function_name='cancel_orders',
            action='cancel',
            error='Connection lost',
            order_ids=[999],
        )

        conn = db_module._connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT function_name, error, order_ids FROM order_audit_log LIMIT 1"
            ).fetchone()
            self.assertEqual(row[0], 'cancel_orders')
            self.assertEqual(row[1], 'Connection lost')
            self.assertEqual(row[2], '999')
        finally:
            conn.close()

    @patch('fx_sr.ibkr.get_db_path')
    @patch('fx_sr.ibkr._connect', side_effect=RuntimeError('DB down'))
    def test_log_order_event_survives_db_failure(self, _mock_connect, mock_db_path):
        mock_db_path.return_value = self.db_path
        from fx_sr.ibkr import log_order_event

        # Must not raise — audit failures are silent
        log_order_event(
            function_name='test',
            action='test',
        )

class IbkrOrderRoundingTests(unittest.TestCase):
    def test_submit_fx_market_bracket_order_rounds_jpy_exit_prices_to_min_tick(self):
        fake_ib_async = _fake_ib_async_module()

        class _BaseOrder:
            def __init__(self, action, totalQuantity, **kwargs):
                self.action = action
                self.totalQuantity = totalQuantity
                for key, value in kwargs.items():
                    setattr(self, key, value)

        class MarketOrder(_BaseOrder):
            pass

        class LimitOrder(_BaseOrder):
            def __init__(self, action, totalQuantity, lmtPrice, **kwargs):
                super().__init__(action, totalQuantity, lmtPrice=lmtPrice, **kwargs)

        class StopOrder(_BaseOrder):
            def __init__(self, action, totalQuantity, stopPrice, **kwargs):
                super().__init__(action, totalQuantity, auxPrice=stopPrice, **kwargs)

        fake_ib_async.MarketOrder = MarketOrder
        fake_ib_async.LimitOrder = LimitOrder
        fake_ib_async.StopOrder = StopOrder

        contract = object()
        placed_orders = []
        ib = MagicMock()
        ib.client.getReqId.side_effect = [15, 16, 17, 18]
        ib.reqContractDetails.return_value = [types.SimpleNamespace(minTick=0.005)]

        def _place_order(_contract, order):
            placed_orders.append(order)
            return types.SimpleNamespace(
                order=order,
                orderStatus=types.SimpleNamespace(
                    status='Filled',
                    avgFillPrice=0.0,
                    filled=float(order.totalQuantity),
                    remaining=0.0,
                ),
            )

        ib.placeOrder.side_effect = _place_order

        with patch.dict(sys.modules, {'ib_async': fake_ib_async}), \
                patch('fx_sr.ibkr._get_connection', return_value=(ib, True)), \
                patch('fx_sr.ibkr._make_contract', return_value=contract), \
                patch('fx_sr.ibkr.whatif_margin_check', return_value=None):
            result = ibkr.submit_fx_market_bracket_order(
                pair='USDJPY',
                direction='SHORT',
                quantity=62252,
                take_profit_price=159.10159925,
                stop_loss_price=159.6941825,
                order_ref='fxsr:USDJPY:SHORT:20260318140000',
            )

        self.assertIsNotNone(result)
        self.assertEqual(len(placed_orders), 3)
        # Brackets-first: [0]=TP limit, [1]=SL stop, [2]=entry market
        self.assertAlmostEqual(placed_orders[0].lmtPrice, 159.105)
        self.assertAlmostEqual(placed_orders[1].auxPrice, 159.69)
        self.assertAlmostEqual(result['take_profit_price'], 159.105)
        self.assertAlmostEqual(result['stop_loss_price'], 159.69)


class SubmitBracketForExistingPositionTests(unittest.TestCase):
    def test_submits_limit_and_stop_orders(self):
        fake_ib_async = types.ModuleType('ib_async')
        fake_ib_async.LimitOrder = MagicMock()
        fake_ib_async.StopOrder = MagicMock()

        ib = MagicMock()
        ib.client.getReqId.side_effect = [199, 200, 201]
        ib.qualifyContracts = MagicMock()
        contract = MagicMock()

        tp_trade = MagicMock()
        sl_trade = MagicMock()
        tp_trade.order = MagicMock(orderId=200)
        sl_trade.order = MagicMock(orderId=201)
        ib.placeOrder.side_effect = [tp_trade, sl_trade]

        with patch.dict(sys.modules, {'ib_async': fake_ib_async}), \
                patch('fx_sr.ibkr._get_connection', return_value=(ib, True)), \
                patch('fx_sr.ibkr._make_contract', return_value=contract), \
                patch('fx_sr.ibkr._round_bracket_exit_prices', return_value=(1.1050, 1.0950)):
            result = ibkr.submit_bracket_for_existing_position(
                pair='EURUSD',
                direction='LONG',
                quantity=10000,
                take_profit_price=1.1050,
                stop_loss_price=1.0950,
            )

        self.assertIsNotNone(result)
        self.assertEqual(result['take_profit_order_id'], 200)
        self.assertEqual(result['stop_loss_order_id'], 201)
        self.assertEqual(ib.placeOrder.call_count, 2)

    def test_returns_none_when_quantity_zero(self):
        result = ibkr.submit_bracket_for_existing_position(
            pair='EURUSD', direction='LONG', quantity=0,
            take_profit_price=1.1050, stop_loss_price=1.0950,
        )
        self.assertIsNone(result)

    def test_returns_none_when_not_connected(self):
        with patch('fx_sr.ibkr._get_connection', return_value=(None, False)):
            result = ibkr.submit_bracket_for_existing_position(
                pair='EURUSD',
                direction='LONG',
                quantity=10000,
                take_profit_price=1.1050,
                stop_loss_price=1.0950,
            )
        self.assertIsNone(result)


class SubmitBracketAuditTests(unittest.TestCase):
    def setUp(self):
        self._db_ctx = temporary_test_database()
        self.db_path = self._db_ctx.__enter__()
        db_module.init_db(self.db_path)

    def tearDown(self):
        if self._db_ctx is not None:
            self._db_ctx.__exit__(None, None, None)

    @patch('fx_sr.ibkr.get_db_path')
    @patch('fx_sr.ibkr._get_connection')
    @patch('fx_sr.ibkr.whatif_margin_check', return_value=None)
    def test_submit_bracket_logs_success(self, _margin, mock_conn, mock_db_path):
        mock_db_path.return_value = self.db_path

        ib = MagicMock()
        ib.client.getReqId.side_effect = [100, 200, 101, 102]
        mock_conn.return_value = (ib, True)

        entry_trade = MagicMock()
        entry_trade.orderStatus.status = 'Filled'
        entry_trade.orderStatus.filled = 10000.0
        entry_trade.orderStatus.remaining = 0.0
        entry_trade.orderStatus.avgFillPrice = 1.145
        entry_trade.order.orderId = 100
        entry_trade.order.totalQuantity = 10000.0

        tp_trade = MagicMock()
        tp_trade.order.orderId = 101
        sl_trade = MagicMock()
        sl_trade.order.orderId = 102

        ib.placeOrder.side_effect = [entry_trade, tp_trade, sl_trade]

        from fx_sr.ibkr import submit_fx_market_bracket_order
        result = submit_fx_market_bracket_order(
            pair='EURUSD', direction='LONG', quantity=10000,
            take_profit_price=1.15, stop_loss_price=1.13,
            order_ref='test',
        )

        conn = db_module._connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT action, pair, direction, request_json, response_json, error "
                "FROM order_audit_log LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(row, "Expected an audit log row")
            # Brackets-first: first logged event is the bracket placement
            self.assertEqual(row[0], 'submit_bracket')
            self.assertEqual(row[1], 'EURUSD')
            self.assertEqual(row[2], 'LONG')
            self.assertIsNone(row[5])  # no error
        finally:
            conn.close()


class ResubmitBracketAuditTests(unittest.TestCase):
    def setUp(self):
        self._db_ctx = temporary_test_database()
        self.db_path = self._db_ctx.__enter__()
        db_module.init_db(self.db_path)

    def tearDown(self):
        if self._db_ctx is not None:
            self._db_ctx.__exit__(None, None, None)

    @patch('fx_sr.ibkr.get_db_path')
    @patch('fx_sr.ibkr._get_connection')
    @patch('fx_sr.ibkr.whatif_margin_check', return_value=None)
    def test_resubmit_bracket_logs_success(self, _margin, mock_conn, mock_db_path):
        mock_db_path.return_value = self.db_path

        ib = MagicMock()
        ib.client.getReqId.side_effect = [200, 201, 202]
        mock_conn.return_value = (ib, True)

        tp_trade = MagicMock()
        tp_trade.order.orderId = 201
        sl_trade = MagicMock()
        sl_trade.order.orderId = 202
        ib.placeOrder.side_effect = [tp_trade, sl_trade]

        from fx_sr.ibkr import submit_bracket_for_existing_position
        result = submit_bracket_for_existing_position(
            pair='GBPUSD', direction='SHORT', quantity=5000,
            take_profit_price=1.30, stop_loss_price=1.33,
        )

        conn = db_module._connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT action, pair, direction, request_json, response_json, error "
                "FROM order_audit_log LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(row, "Expected an audit log row")
            self.assertEqual(row[0], 'resubmit')
            self.assertEqual(row[1], 'GBPUSD')
            self.assertEqual(row[2], 'SHORT')
            self.assertIsNone(row[5])  # no error
        finally:
            conn.close()


class CancelOrdersAuditTests(unittest.TestCase):
    def setUp(self):
        self._db_ctx = temporary_test_database()
        self.db_path = self._db_ctx.__enter__()
        db_module.init_db(self.db_path)
        ibkr._STALE_CANCEL_ORDER_IDS.clear()

    def tearDown(self):
        ibkr._STALE_CANCEL_ORDER_IDS.clear()
        if self._db_ctx is not None:
            self._db_ctx.__exit__(None, None, None)

    @patch('fx_sr.ibkr.get_db_path')
    @patch('fx_sr.ibkr._get_connection')
    def test_cancel_orders_logs_audit(self, mock_conn, mock_db_path):
        mock_db_path.return_value = self.db_path

        ib = MagicMock()
        mock_conn.return_value = (ib, True)

        # Mock openTrades to return matching trade objects
        trade1 = MagicMock()
        trade1.order.orderId = 500
        trade2 = MagicMock()
        trade2.order.orderId = 501
        ib.openTrades.return_value = [trade1, trade2]

        from fx_sr.ibkr import cancel_orders
        cancel_orders({500, 501})

        conn = db_module._connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT action, order_ids, error FROM order_audit_log LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(row, "Expected an audit log row")
            self.assertEqual(row[0], 'cancel')
            self.assertIn('500', row[1])
            self.assertIn('501', row[1])
        finally:
            conn.close()

    @patch('fx_sr.ibkr.get_db_path')
    @patch('fx_sr.ibkr._get_connection')
    def test_cancel_orders_suppress_not_found_skips_unknown_open_order_ids(self, mock_conn, mock_db_path):
        mock_db_path.return_value = self.db_path

        ib = MagicMock()
        ib.reqAllOpenOrders.return_value = []
        mock_conn.return_value = (ib, True)

        from fx_sr.ibkr import cancel_orders
        cancelled = cancel_orders({500, 501}, suppress_not_found=True)

        self.assertEqual(cancelled, [])
        ib.cancelOrder.assert_not_called()

    @patch('fx_sr.ibkr.get_db_path')
    @patch('fx_sr.ibkr._get_connection')
    def test_cancel_orders_remembers_10147_as_stale_not_cancelled(self, mock_conn, mock_db_path):
        mock_db_path.return_value = self.db_path

        class FakeEvent:
            def __init__(self):
                self.handlers = []

            def __iadd__(self, handler):
                self.handlers.append(handler)
                return self

            def __isub__(self, handler):
                self.handlers = [h for h in self.handlers if h != handler]
                return self

            def emit(self, *args):
                for handler in list(self.handlers):
                    handler(*args)

        class FakeOrder:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        fake_ib_async = types.ModuleType('ib_async')
        fake_ib_async.Order = FakeOrder

        trade = types.SimpleNamespace(
            contract=types.SimpleNamespace(secType='CASH', localSymbol='EUR.USD', symbol='EUR', currency='USD'),
            order=types.SimpleNamespace(orderId=777),
            orderStatus=types.SimpleNamespace(status='Submitted'),
        )
        ib = MagicMock()
        ib.errorEvent = FakeEvent()
        ib._onError = MagicMock()
        ib.reqAllOpenOrders.return_value = [trade]

        def _cancel_order(_order):
            ib.errorEvent.emit(
                777,
                10147,
                'OrderId 777 that needs to be cancelled is not found.',
                None,
            )

        ib.cancelOrder.side_effect = _cancel_order
        mock_conn.return_value = (ib, True)

        with patch.dict(sys.modules, {'ib_async': fake_ib_async}):
            cancelled = ibkr.cancel_orders({777}, suppress_not_found=True)

        self.assertEqual(cancelled, [])
        self.assertTrue(ibkr._is_stale_cancel_order_id(777))
        ib._onError.assert_not_called()

    @patch('fx_sr.ibkr._get_connection')
    def test_fetch_open_order_counts_ignores_recent_stale_cancel_ids(self, mock_conn):
        trade = types.SimpleNamespace(
            contract=types.SimpleNamespace(secType='CASH', localSymbol='EUR.USD', symbol='EUR', currency='USD'),
            order=types.SimpleNamespace(orderId=777),
            orderStatus=types.SimpleNamespace(status='Submitted'),
        )
        ib = MagicMock()
        ib.reqAllOpenOrders.return_value = [trade]
        mock_conn.return_value = (ib, True)
        ibkr._remember_stale_cancel_order_ids({777})

        self.assertEqual(ibkr.fetch_open_order_counts(), {})


class LiquidateFxPositionTests(unittest.TestCase):
    def setUp(self):
        self._db_ctx = temporary_test_database()
        self.db_path = self._db_ctx.__enter__()
        db_module.init_db(self.db_path)

    def tearDown(self):
        if self._db_ctx is not None:
            self._db_ctx.__exit__(None, None, None)

    @patch('fx_sr.ibkr.get_db_path')
    @patch('fx_sr.ibkr._get_connection')
    def test_liquidate_cancels_pair_orders_when_broker_reports_flat(self, mock_conn, mock_db_path):
        mock_db_path.return_value = self.db_path

        class FakeOrder:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        fake_ib_async = types.ModuleType('ib_async')
        fake_ib_async.MarketOrder = FakeOrder
        fake_ib_async.Order = FakeOrder

        trade = types.SimpleNamespace(
            contract=types.SimpleNamespace(
                secType='CASH',
                localSymbol='EUR.USD',
                symbol='EUR',
                currency='USD',
            ),
            order=types.SimpleNamespace(
                orderId=900,
                parentId=0,
                permId=1,
                orderRef='fxsr:test',
                orderType='STP',
                action='SELL',
                totalQuantity=10000,
            ),
            orderStatus=types.SimpleNamespace(
                status='Submitted',
                avgFillPrice=0,
                filled=0,
                remaining=10000,
            ),
        )
        ib = MagicMock()
        ib.positions.return_value = []
        ib.reqAllOpenOrders.side_effect = [[trade], []]
        mock_conn.return_value = (ib, True)

        with patch.dict(sys.modules, {'ib_async': fake_ib_async}):
            result = ibkr.liquidate_fx_position('EURUSD', expected_direction='LONG')

        self.assertEqual(result['status'], 'FAILED')
        self.assertEqual(result['error'], 'IBKR has no live EURUSD position.')
        self.assertEqual(result['cancelled_order_ids'], [900])
        self.assertEqual(result['remaining_open_orders'], [])
        ib.cancelOrder.assert_called_once()


class SubmitMarketOrderAuditTests(unittest.TestCase):
    def setUp(self):
        self._db_ctx = temporary_test_database()
        self.db_path = self._db_ctx.__enter__()
        db_module.init_db(self.db_path)

    def tearDown(self):
        if self._db_ctx is not None:
            self._db_ctx.__exit__(None, None, None)

    @patch('fx_sr.ibkr.get_db_path')
    @patch('fx_sr.ibkr._get_connection')
    def test_submit_market_order_logs_audit(self, mock_conn, mock_db_path):
        mock_db_path.return_value = self.db_path

        ib = MagicMock()
        ib.client.getReqId.return_value = 300
        mock_conn.return_value = (ib, True)

        trade = MagicMock()
        trade.order.orderId = 300
        trade.orderStatus.status = 'Filled'
        trade.orderStatus.avgFillPrice = 1.145
        ib.placeOrder.return_value = trade

        from fx_sr.ibkr import submit_fx_market_order
        result = submit_fx_market_order(
            pair='EURUSD', direction='LONG', quantity=10000,
        )

        conn = db_module._connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT action, pair, direction, error FROM order_audit_log LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(row, "Expected an audit log row")
            self.assertEqual(row[0], 'submit')
            self.assertEqual(row[1], 'EURUSD')
            self.assertEqual(row[2], 'LONG')
        finally:
            conn.close()


class OrphanSweepAuditTests(unittest.TestCase):
    def setUp(self):
        self._db_ctx = temporary_test_database()
        self.db_path = self._db_ctx.__enter__()
        db_module.init_db(self.db_path)

    def tearDown(self):
        if self._db_ctx is not None:
            self._db_ctx.__exit__(None, None, None)

    @patch('fx_sr.positions.set_setting')
    @patch('fx_sr.positions.get_db_path')
    @patch('fx_sr.ibkr.get_db_path')
    @patch('fx_sr.positions._cancel_orders_for_pairs')
    @patch('fx_sr.positions._resubmit_missing_brackets')
    @patch('fx_sr.positions.ibkr')
    @patch('fx_sr.positions._load_trades')
    @patch('fx_sr.positions.load_open_broker_execution_positions', return_value=[])
    @patch('fx_sr.positions.reconcile_broker_ledger')
    def test_orphan_sweep_logs_audit(
        self,
        _recon,
        _broker_positions,
        mock_load_trades,
        mock_ibkr,
        mock_resubmit,
        mock_cancel_pairs,
        mock_pos_db,
        mock_ibkr_db,
        _set_setting,
    ):
        mock_pos_db.return_value = self.db_path
        mock_ibkr_db.return_value = self.db_path

        # One live position on USDJPY — so early-return guard is bypassed
        ibkr_positions = [{'pair': 'USDJPY', 'size': 10000, 'avg_cost': 150.0}]
        mock_ibkr.fetch_positions.return_value = ibkr_positions
        # One open order pair on CHFJPY — orphaned (no position)
        mock_ibkr.fetch_open_order_counts.return_value = {'CHFJPY': 1}
        mock_ibkr.fetch_open_order_pairs.return_value = {'CHFJPY'}

        # Existing DB trade matching the USDJPY:LONG position (no size change)
        mock_trade = MagicMock()
        mock_trade.tp_price = None
        mock_trade.sl_price = None
        mock_load_trades.return_value = {
            'USDJPY:LONG': {
                'pair': 'USDJPY',
                'trade': mock_trade,
                'signal_id': None,
                'ibkr_size': 10000.0,
                'ibkr_avg_cost': 150.0,
                'bars_monitored': 0,
                'signal_status': None,
                'pending_exit_reason': None,
                'pending_exit_price': None,
                'pending_exit_detected_at': None,
                'last_processed_bar_time': None,
            }
        }

        from fx_sr.positions import sync_positions
        sync_positions()

        conn = db_module._connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT action, request_json FROM order_audit_log "
                "WHERE action = 'sweep' LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(row, "Expected a sweep audit log row")
            self.assertIn('CHFJPY', row[1])
        finally:
            conn.close()


if __name__ == '__main__':
    unittest.main()
