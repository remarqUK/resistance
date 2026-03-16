import sys
import types
import sys
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

import fx_sr.ibkr as ibkr


def _fake_ib_async_module():
    module = types.ModuleType('ib_async')
    module.util = types.SimpleNamespace(df=lambda bars: bars)
    return module


class IbkrHistoricalFetchTests(unittest.TestCase):
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


if __name__ == '__main__':
    unittest.main()
