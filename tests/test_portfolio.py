import unittest

import pandas as pd

from fx_sr.portfolio import (
    ClosedTradeSummary,
    CorrelationExposure,
    PortfolioState,
    apply_correlation_policy,
    build_portfolio_state,
    calculate_effective_risk_pct,
    closed_trade_summary_from_row,
    compute_pause_until,
    cooldown_end_time,
    is_pair_cooldown_active,
)
from fx_sr.strategy import StrategyParams


class PortfolioPolicyTests(unittest.TestCase):
    def test_correlation_policy_respects_disabled_filter(self):
        allowed, replaced = apply_correlation_policy(
            [CorrelationExposure(pair='EURUSD', quality_score=0.1)],
            candidate_pair='GBPUSD',
            candidate_quality=0.9,
            params=StrategyParams(max_correlated_trades=1, use_correlation_filter=False),
        )
        self.assertTrue(allowed)
        self.assertIsNone(replaced)

    def test_correlation_policy_replaces_lower_quality_exposure(self):
        allowed, replaced = apply_correlation_policy(
            [CorrelationExposure(pair='EURUSD', quality_score=0.2, payload='low')],
            candidate_pair='GBPUSD',
            candidate_quality=0.8,
            params=StrategyParams(
                max_correlated_trades=1,
                correlation_prefer_quality=True,
            ),
        )
        self.assertTrue(allowed)
        self.assertIsNotNone(replaced)
        self.assertEqual(replaced.payload, 'low')

    def test_cooldown_uses_loss_specific_window(self):
        params = StrategyParams(cooldown_bars=2, loss_cooldown_bars=5)
        exit_time = pd.Timestamp('2026-02-03 10:00:00', tz='UTC')

        self.assertEqual(
            cooldown_end_time(exit_time, -1.0, params),
            pd.Timestamp('2026-02-03 15:00:00', tz='UTC'),
        )
        self.assertTrue(
            is_pair_cooldown_active(
                pd.Timestamp('2026-02-03 14:59:59', tz='UTC'),
                last_exit_time=exit_time,
                last_pnl_r=-1.0,
                params=params,
            )
        )
        self.assertFalse(
            is_pair_cooldown_active(
                pd.Timestamp('2026-02-03 15:00:00', tz='UTC'),
                last_exit_time=exit_time,
                last_pnl_r=-1.0,
                params=params,
            )
        )

    def test_compute_pause_until_replays_losing_streak(self):
        params = StrategyParams(streak_pause_trigger=2, streak_pause_hours=24)
        closed_trades = [
            ClosedTradeSummary(
                pair='EURUSD',
                entry_time=pd.Timestamp('2026-02-01 10:00:00', tz='UTC'),
                exit_time=pd.Timestamp('2026-02-01 14:00:00', tz='UTC'),
                pnl_r=-1.0,
            ),
            ClosedTradeSummary(
                pair='GBPUSD',
                entry_time=pd.Timestamp('2026-02-02 10:00:00', tz='UTC'),
                exit_time=pd.Timestamp('2026-02-02 16:00:00', tz='UTC'),
                pnl_r=-0.5,
            ),
        ]

        self.assertEqual(
            compute_pause_until(closed_trades, params),
            pd.Timestamp('2026-02-03 16:00:00', tz='UTC'),
        )

    def test_effective_risk_pct_applies_drawdown_and_quality(self):
        params = StrategyParams(
            dynamic_risk=True,
            dd_risk_start=5.0,
            dd_risk_full=10.0,
            dd_risk_floor=0.5,
            quality_sizing=True,
            quality_risk_min=0.5,
            quality_risk_max=1.5,
        )
        risk_pct = calculate_effective_risk_pct(
            0.02,
            params=params,
            balance=10000.0,
            peak_balance=12000.0,
            quality_score=1.0,
        )
        self.assertAlmostEqual(risk_pct, 0.0075)

    def test_closed_trade_summary_from_row_uses_actual_fill_and_risk_amount(self):
        row = {
            'pair': 'EURUSD',
            'direction': 'LONG',
            'signal_time': '2026-02-03 10:00:00+00:00',
            'opened_at': '2026-02-03 10:00:05+00:00',
            'opened_price': 1.1002,
            'entry_price': 1.1000,
            'sl_price': 1.0950,
            'closed_at': '2026-02-03 14:00:00+00:00',
            'closed_price': 1.1106,
            'quality_score': 0.8,
            'risk_amount': 200.0,
        }

        summary = closed_trade_summary_from_row(row)
        self.assertIsNotNone(summary)
        self.assertAlmostEqual(summary.pnl_r, 2.0)
        self.assertAlmostEqual(summary.pnl_amount, 400.0)

    def test_build_portfolio_state_tracks_peak_balance_and_pair_blocks(self):
        params = StrategyParams(cooldown_bars=2, loss_cooldown_bars=5)
        closed_trades = [
            ClosedTradeSummary(
                pair='EURUSD',
                entry_time=pd.Timestamp('2026-02-01 08:00:00', tz='UTC'),
                exit_time=pd.Timestamp('2026-02-01 10:00:00', tz='UTC'),
                pnl_r=1.0,
                pnl_amount=100.0,
            ),
            ClosedTradeSummary(
                pair='EURUSD',
                entry_time=pd.Timestamp('2026-02-02 08:00:00', tz='UTC'),
                exit_time=pd.Timestamp('2026-02-02 12:00:00', tz='UTC'),
                pnl_r=-1.0,
                pnl_amount=-40.0,
            ),
        ]

        state = build_portfolio_state(
            closed_trades,
            params=params,
            current_balance=1060.0,
        )

        self.assertAlmostEqual(state.peak_balance, 1100.0)
        self.assertAlmostEqual(state.balance, 1060.0)
        self.assertEqual(state.latest_pair_close('EURUSD').exit_time, closed_trades[-1].exit_time)
        self.assertEqual(
            state.entry_block('EURUSD', pd.Timestamp('2026-02-02 16:59:59', tz='UTC'))[0],
            'COOLDOWN',
        )
        self.assertIsNone(state.entry_block('EURUSD', pd.Timestamp('2026-02-02 17:00:00', tz='UTC')))

    def test_portfolio_state_record_closed_trade_updates_pause_incrementally(self):
        params = StrategyParams(streak_pause_trigger=2, streak_pause_hours=24)
        state = PortfolioState(params=params, balance=1000.0, peak_balance=1000.0)

        state.record_closed_trade(
            ClosedTradeSummary(
                pair='EURUSD',
                entry_time=pd.Timestamp('2026-02-01 08:00:00', tz='UTC'),
                exit_time=pd.Timestamp('2026-02-01 10:00:00', tz='UTC'),
                pnl_r=-1.0,
                pnl_amount=-50.0,
            )
        )
        self.assertIsNone(state.pause_until)

        state.record_closed_trade(
            ClosedTradeSummary(
                pair='GBPUSD',
                entry_time=pd.Timestamp('2026-02-02 08:00:00', tz='UTC'),
                exit_time=pd.Timestamp('2026-02-02 12:00:00', tz='UTC'),
                pnl_r=-0.5,
                pnl_amount=-25.0,
            )
        )

        self.assertEqual(state.pause_until, pd.Timestamp('2026-02-03 12:00:00', tz='UTC'))
        self.assertAlmostEqual(state.balance, 925.0)
        self.assertEqual(
            state.entry_block('AUDUSD', pd.Timestamp('2026-02-03 11:59:59', tz='UTC'))[0],
            'PAUSED',
        )


if __name__ == '__main__':
    unittest.main()
