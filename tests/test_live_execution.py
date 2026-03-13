import unittest
from unittest.mock import patch

import pandas as pd

from fx_sr.live import execute_signal_plans
from fx_sr.sizing import PositionSizePlan
from fx_sr.strategy import Signal, StrategyParams, Trade


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
    )


def _plan(pair: str, risk_amount: float = 200.0) -> PositionSizePlan:
    return PositionSizePlan(
        pair=pair,
        direction='LONG',
        units=10000,
        risk_amount=risk_amount,
        risk_pct=risk_amount / 10000.0,
        balance=10000.0,
        account_currency='USD',
        risk_per_unit_account=0.02,
        notional_account=11000.0,
    )


class LiveExecutionTests(unittest.TestCase):
    def test_execute_signal_plans_submits_market_bracket_orders(self):
        signal = _signal('EURUSD')
        plan = _plan('EURUSD')

        with patch(
            'fx_sr.live.ibkr.submit_fx_market_bracket_order',
            return_value={'order_id': 101, 'status': 'Submitted'},
        ) as submit_mock:
            results = execute_signal_plans(
                [signal],
                [plan],
                execute_orders=True,
                existing_pairs=set(),
                pending_pairs=set(),
                params=StrategyParams(max_correlated_trades=2),
                tracked_positions={},
                balance=10000.0,
                risk_pct=0.02,
                account_currency='USD',
            )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, 'Submitted')
        self.assertIn('tp/sl attached', results[0].note)
        submit_mock.assert_called_once_with(
            pair='EURUSD',
            direction='LONG',
            quantity=10000,
            take_profit_price=signal.tp_price,
            stop_loss_price=signal.sl_price,
            order_ref='fxsr:EURUSD:LONG:20260203100000',
        )

    def test_execute_signal_plans_enforces_correlation_cap(self):
        signals = [_signal('EURUSD'), _signal('GBPUSD')]
        plans = [_plan('EURUSD'), _plan('GBPUSD')]

        with patch(
            'fx_sr.live.ibkr.submit_fx_market_bracket_order',
            return_value={'order_id': 101, 'status': 'Submitted'},
        ) as submit_mock:
            results = execute_signal_plans(
                signals,
                plans,
                execute_orders=True,
                existing_pairs=set(),
                pending_pairs=set(),
                params=StrategyParams(max_correlated_trades=1),
                tracked_positions={},
                balance=10000.0,
                risk_pct=0.02,
                account_currency='USD',
            )

        self.assertEqual(submit_mock.call_count, 1)
        self.assertEqual(results[0].status, 'Submitted')
        self.assertEqual(results[1].status, 'SKIPPED')
        self.assertEqual(results[1].note, 'correlation cap reached')

    def test_execute_signal_plans_enforces_risk_budget(self):
        signals = [_signal('EURUSD'), _signal('AUDNZD')]
        plans = [_plan('EURUSD'), _plan('AUDNZD')]
        tracked_positions = {
            'GBPUSD:LONG': {
                'pair': 'GBPUSD',
                'trade': Trade(
                    entry_time=pd.Timestamp('2026-02-03 09:00:00', tz='UTC'),
                    entry_price=1.3000,
                    direction='LONG',
                    sl_price=1.2950,
                    tp_price=1.3100,
                    zone_upper=1.3010,
                    zone_lower=1.3000,
                    zone_strength='major',
                    risk=0.0050,
                ),
                'ibkr_size': 10000,
            }
        }

        with patch(
            'fx_sr.live.estimate_position_risk_amount',
            return_value=200.0,
        ), patch(
            'fx_sr.live.ibkr.submit_fx_market_bracket_order',
            return_value={'order_id': 101, 'status': 'Submitted'},
        ) as submit_mock:
            results = execute_signal_plans(
                signals,
                plans,
                execute_orders=True,
                existing_pairs={'GBPUSD'},
                pending_pairs=set(),
                params=StrategyParams(max_correlated_trades=2),
                tracked_positions=tracked_positions,
                balance=10000.0,
                risk_pct=0.02,
                account_currency='USD',
            )

        self.assertEqual(submit_mock.call_count, 1)
        self.assertEqual(results[0].status, 'Submitted')
        self.assertEqual(results[1].status, 'SKIPPED')
        self.assertEqual(results[1].note, 'risk budget full')


if __name__ == '__main__':
    unittest.main()
