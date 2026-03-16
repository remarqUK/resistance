import unittest
from unittest.mock import patch

import pandas as pd

from fx_sr import live as live_module
from fx_sr.ibkr import ExecutionQuote
from fx_sr.live import build_live_size_plans, execute_signal_plans, load_portfolio_state
from fx_sr.portfolio import ClosedTradeSummary
from fx_sr.sizing import PositionSizePlan
from fx_sr.strategy import Signal, StrategyParams, Trade


def _signal(pair: str, direction: str = 'LONG', quality_score: float = 0.0) -> Signal:
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
        quality_score=quality_score,
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


def _quote(
    pair: str,
    *,
    bid: float = 1.0998,
    ask: float = 1.1000,
    source: str = 'l1',
    captured_at: pd.Timestamp | None = None,
) -> ExecutionQuote:
    return ExecutionQuote(
        pair=pair,
        bid=bid,
        ask=ask,
        mid=(bid + ask) / 2.0,
        spread=ask - bid,
        source=source,
        captured_at=captured_at or pd.Timestamp.now(tz='UTC'),
    )


class LiveExecutionTests(unittest.TestCase):
    def setUp(self):
        live_module._PORTFOLIO_STATE_CACHE.clear()

    def test_execute_signal_plans_submits_market_bracket_orders(self):
        signal = _signal('EURUSD')
        plan = _plan('EURUSD')

        with patch(
            'fx_sr.live.ibkr.fetch_execution_quote',
            return_value=_quote('EURUSD', bid=1.0998, ask=1.1000, source='l2'),
        ), patch(
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
        self.assertIn('order submitted', results[0].note)
        self.assertAlmostEqual(results[0].submitted_entry_price, 1.1000)
        self.assertAlmostEqual(results[0].submitted_tp_price, 1.1100)
        self.assertAlmostEqual(results[0].submitted_sl_price, 1.0950)
        self.assertEqual(results[0].quote_source, 'l2')
        submit_mock.assert_called_once()
        submit_kwargs = submit_mock.call_args.kwargs
        self.assertEqual(submit_kwargs['pair'], 'EURUSD')
        self.assertEqual(submit_kwargs['direction'], 'LONG')
        self.assertEqual(submit_kwargs['order_ref'], 'fxsr:EURUSD:LONG:20260203100000')
        self.assertEqual(submit_kwargs['quantity'], 39999)
        self.assertAlmostEqual(submit_kwargs['take_profit_price'], 1.1100)
        self.assertAlmostEqual(submit_kwargs['stop_loss_price'], 1.0950)

    def test_execute_signal_plans_marks_initial_partial_fill(self):
        signal = _signal('EURUSD')
        plan = _plan('EURUSD')

        with patch(
            'fx_sr.live.ibkr.fetch_execution_quote',
            return_value=_quote('EURUSD', bid=1.0998, ask=1.1000, source='l1'),
        ), patch(
            'fx_sr.live.ibkr.submit_fx_market_bracket_order',
            return_value={
                'order_id': 101,
                'status': 'Submitted',
                'avg_fill_price': 1.1001,
                'filled_units': 4000,
                'remaining_units': 6000,
            },
        ):
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

        self.assertEqual(results[0].status, 'PARTIAL')
        self.assertEqual(results[0].filled_units, 4000)
        self.assertEqual(results[0].remaining_units, 6000)
        self.assertEqual(results[0].broker_status, 'Submitted')
        self.assertEqual(results[0].avg_fill_price, 1.1001)
        self.assertEqual(results[0].note, 'partial fill 4,000/39,999')

    def test_execute_signal_plans_enforces_correlation_cap(self):
        signals = [_signal('EURUSD'), _signal('GBPUSD')]
        plans = [_plan('EURUSD'), _plan('GBPUSD')]

        with patch(
            'fx_sr.live.ibkr.fetch_execution_quote',
            side_effect=[
                _quote('EURUSD', bid=1.0998, ask=1.1000),
                _quote('GBPUSD', bid=1.0998, ask=1.1000),
            ],
        ), patch(
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

    def test_execute_signal_plans_honors_disabled_correlation_filter(self):
        signals = [_signal('EURUSD'), _signal('GBPUSD')]
        plans = [_plan('EURUSD'), _plan('GBPUSD')]

        with patch(
            'fx_sr.live.ibkr.fetch_execution_quote',
            side_effect=[
                _quote('EURUSD', bid=1.0998, ask=1.1000),
                _quote('GBPUSD', bid=1.0998, ask=1.1000),
            ],
        ), patch(
            'fx_sr.live.ibkr.submit_fx_market_bracket_order',
            side_effect=[
                {'order_id': 101, 'status': 'Submitted'},
                {'order_id': 102, 'status': 'Submitted'},
            ],
        ) as submit_mock:
            results = execute_signal_plans(
                signals,
                plans,
                execute_orders=True,
                existing_pairs=set(),
                pending_pairs=set(),
                params=StrategyParams(max_correlated_trades=1, use_correlation_filter=False),
                tracked_positions={},
                balance=10000.0,
                risk_pct=0.02,
                account_currency='USD',
            )

        self.assertEqual(submit_mock.call_count, 2)
        self.assertEqual([result.status for result in results], ['Submitted', 'Submitted'])

    def test_execute_signal_plans_prefers_higher_quality_correlated_signal(self):
        signals = [
            _signal('EURUSD', quality_score=0.2),
            _signal('GBPUSD', quality_score=0.8),
        ]
        plans = [_plan('EURUSD'), _plan('GBPUSD')]

        with patch(
            'fx_sr.live.ibkr.fetch_execution_quote',
            side_effect=[
                _quote('EURUSD', bid=1.0998, ask=1.1000),
                _quote('GBPUSD', bid=1.0998, ask=1.1000),
            ],
        ), patch(
            'fx_sr.live.ibkr.submit_fx_market_bracket_order',
            return_value={'order_id': 101, 'status': 'Submitted'},
        ) as submit_mock:
            results = execute_signal_plans(
                signals,
                plans,
                execute_orders=True,
                existing_pairs=set(),
                pending_pairs=set(),
                params=StrategyParams(
                    max_correlated_trades=1,
                    correlation_prefer_quality=True,
                ),
                tracked_positions={},
                balance=10000.0,
                risk_pct=0.02,
                account_currency='USD',
            )

        self.assertEqual(submit_mock.call_count, 1)
        self.assertEqual(results[0].status, 'SKIPPED')
        self.assertEqual(results[0].note, 'replaced by higher-quality correlated signal')
        self.assertEqual(results[1].status, 'Submitted')

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
            'fx_sr.live.ibkr.fetch_execution_quote',
            side_effect=[
                _quote('EURUSD', bid=1.0998, ask=1.1000),
                _quote('AUDNZD', bid=1.0998, ask=1.1000),
            ],
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

    def test_execute_signal_plans_skips_stale_quote(self):
        signal = _signal('EURUSD')
        plan = _plan('EURUSD')

        with patch(
            'fx_sr.live.ibkr.fetch_execution_quote',
            return_value=_quote(
                'EURUSD',
                captured_at=pd.Timestamp.now(tz='UTC') - pd.Timedelta(seconds=10),
            ),
        ), patch(
            'fx_sr.live.ibkr.submit_fx_market_bracket_order',
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

        self.assertEqual(results[0].status, 'SKIPPED')
        self.assertEqual(results[0].note, 'stale quote')
        submit_mock.assert_not_called()

    def test_execute_signal_plans_skips_wide_spread(self):
        signal = _signal('EURUSD')
        plan = _plan('EURUSD')

        with patch(
            'fx_sr.live.ibkr.fetch_execution_quote',
            return_value=_quote('EURUSD', bid=1.0995, ask=1.1000),
        ), patch(
            'fx_sr.live.ibkr.submit_fx_market_bracket_order',
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

        self.assertEqual(results[0].status, 'SKIPPED')
        self.assertEqual(results[0].note, 'spread too wide')
        submit_mock.assert_not_called()

    def test_execute_signal_plans_skips_when_price_leaves_zone(self):
        signal = _signal('EURUSD')
        plan = _plan('EURUSD')

        with patch(
            'fx_sr.live.ibkr.fetch_execution_quote',
            return_value=_quote('EURUSD', bid=1.1048, ask=1.1050),
        ), patch(
            'fx_sr.live.ibkr.submit_fx_market_bracket_order',
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

        self.assertEqual(results[0].status, 'SKIPPED')
        self.assertEqual(results[0].note, 'price left zone')
        submit_mock.assert_not_called()

    def test_build_live_size_plans_applies_shared_dynamic_and_quality_risk(self):
        closed_trades = [
            ClosedTradeSummary(
                pair='EURUSD',
                entry_time=pd.Timestamp('2026-02-01 10:00:00', tz='UTC'),
                exit_time=pd.Timestamp('2026-02-01 15:00:00', tz='UTC'),
                pnl_r=1.0,
                pnl_amount=2000.0,
            ),
            ClosedTradeSummary(
                pair='GBPUSD',
                entry_time=pd.Timestamp('2026-02-02 10:00:00', tz='UTC'),
                exit_time=pd.Timestamp('2026-02-02 15:00:00', tz='UTC'),
                pnl_r=-1.0,
                pnl_amount=-2000.0,
            ),
        ]
        signal = _signal('EURUSD', quality_score=1.0)
        params = StrategyParams(
            dynamic_risk=True,
            dd_risk_start=5.0,
            dd_risk_full=10.0,
            dd_risk_floor=0.5,
            quality_sizing=True,
            quality_risk_min=0.5,
            quality_risk_max=1.5,
        )

        with patch(
            'fx_sr.live.build_position_size_plan',
            return_value=_plan('EURUSD'),
        ) as size_mock:
            build_live_size_plans(
                [signal],
                balance=10000.0,
                risk_pct=0.02,
                account_currency='USD',
                params=params,
                closed_trades=closed_trades,
                price_cache={'EURUSD': 1.1000},
            )

        self.assertAlmostEqual(size_mock.call_args.kwargs['risk_pct'], 0.0075)

    def test_load_portfolio_state_reuses_cached_history_when_closed_stats_unchanged(self):
        row = {
            'signal_id': 'sig-1',
            'pair': 'EURUSD',
            'direction': 'LONG',
            'opened_at': '2026-02-01 10:00:00+00:00',
            'opened_price': 1.1000,
            'sl_price': 1.0950,
            'closed_at': '2026-02-01 14:00:00+00:00',
            'closed_price': 1.1100,
            'risk_amount': 100.0,
        }

        with patch(
            'fx_sr.live.load_detected_signal_stats',
            side_effect=[
                {'count': 1, 'max_last_updated': '2026-02-01T14:00:00+00:00'},
                {'count': 1, 'max_last_updated': '2026-02-01T14:00:00+00:00'},
            ],
        ), patch(
            'fx_sr.live.load_detected_signals',
            return_value=[row],
        ) as load_rows_mock:
            first = load_portfolio_state(StrategyParams(), current_balance=1010.0)
            second = load_portfolio_state(StrategyParams(), current_balance=1020.0)

        self.assertIs(first, second)
        self.assertEqual(load_rows_mock.call_count, 1)
        self.assertAlmostEqual(second.balance, 1020.0)
        self.assertAlmostEqual(second.peak_balance, 1020.0)

    def test_load_portfolio_state_merges_new_closed_rows_incrementally(self):
        first_row = {
            'signal_id': 'sig-1',
            'pair': 'EURUSD',
            'direction': 'LONG',
            'opened_at': '2026-02-01 10:00:00+00:00',
            'opened_price': 1.1000,
            'sl_price': 1.0950,
            'closed_at': '2026-02-01 14:00:00+00:00',
            'closed_price': 1.1100,
            'risk_amount': 100.0,
        }
        second_row = {
            'signal_id': 'sig-2',
            'pair': 'GBPUSD',
            'direction': 'LONG',
            'opened_at': '2026-02-02 10:00:00+00:00',
            'opened_price': 1.3000,
            'sl_price': 1.2950,
            'closed_at': '2026-02-02 15:00:00+00:00',
            'closed_price': 1.2950,
            'risk_amount': 100.0,
        }

        with patch(
            'fx_sr.live.load_detected_signal_stats',
            side_effect=[
                {'count': 1, 'max_last_updated': '2026-02-01T14:00:00+00:00'},
                {'count': 2, 'max_last_updated': '2026-02-02T15:00:00+00:00'},
            ],
        ), patch(
            'fx_sr.live.load_detected_signals',
            side_effect=[
                [first_row],
                [second_row],
            ],
        ) as load_rows_mock:
            state = load_portfolio_state(StrategyParams(), current_balance=1000.0)
            updated = load_portfolio_state(StrategyParams(), current_balance=900.0)

        self.assertIs(state, updated)
        self.assertEqual(load_rows_mock.call_args_list[1].kwargs['updated_after'], '2026-02-01T14:00:00+00:00')
        self.assertIsNotNone(updated.latest_pair_close('GBPUSD'))
        self.assertEqual(updated.latest_pair_close('GBPUSD').pnl_r, -1.0)


if __name__ == '__main__':
    unittest.main()
