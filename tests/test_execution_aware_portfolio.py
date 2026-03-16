import unittest

import pandas as pd

from fx_sr.backtest import BacktestResult, calculate_execution_aware_compounding_pnl
from fx_sr.strategy import StrategyParams, Trade


def _trade(
    pair: str,
    entry: str,
    exit_: str,
    pnl_pips: float,
    pnl_r: float,
    *,
    quality_score: float = 0.0,
) -> Trade:
    entry_price = 1.1000 if not pair.endswith('JPY') else 150.000
    pip = 0.0001 if not pair.endswith('JPY') else 0.01
    exit_price = entry_price + (pip * 10 if pnl_pips >= 0 else -pip * 10)
    return Trade(
        entry_time=pd.Timestamp(entry, tz='UTC'),
        entry_price=entry_price,
        direction='LONG',
        sl_price=entry_price - pip * 20,
        tp_price=entry_price + pip * 40,
        zone_upper=entry_price + pip * 10,
        zone_lower=entry_price - pip * 10,
        zone_strength='major',
        risk=pip * 20,
        exit_time=pd.Timestamp(exit_, tz='UTC'),
        exit_price=exit_price,
        exit_reason='TP' if pnl_pips >= 0 else 'SL',
        pnl_pips=pnl_pips,
        pnl_r=pnl_r,
        bars_held=4,
        quality_score=quality_score,
    )


def _result(pair: str, trades: list[Trade]) -> BacktestResult:
    winning_trades = sum(1 for trade in trades if trade.pnl_pips > 0)
    losing_trades = len(trades) - winning_trades
    total_pnl = sum(trade.pnl_pips for trade in trades)
    return BacktestResult(
        pair=pair,
        total_trades=len(trades),
        winning_trades=winning_trades,
        losing_trades=losing_trades,
        early_exits=0,
        win_rate=(winning_trades / len(trades) * 100) if trades else 0.0,
        total_pnl_pips=total_pnl,
        avg_pnl_pips=(total_pnl / len(trades)) if trades else 0.0,
        avg_win_r=(
            sum(trade.pnl_r for trade in trades if trade.pnl_pips > 0) / winning_trades
            if winning_trades else 0.0
        ),
        avg_loss_r=(
            sum(trade.pnl_r for trade in trades if trade.pnl_pips <= 0) / losing_trades
            if losing_trades else 0.0
        ),
        max_win_pips=max((trade.pnl_pips for trade in trades), default=0.0),
        max_loss_pips=min((trade.pnl_pips for trade in trades), default=0.0),
        profit_factor=float('inf'),
        trades=trades,
        zones=[],
    )


class ExecutionAwarePortfolioTests(unittest.TestCase):
    def test_same_timestamp_higher_quality_trade_replaces_lower_quality_candidate(self):
        params = StrategyParams(
            max_correlated_trades=1,
            correlation_prefer_quality=True,
        )
        results = {
            'EURUSD': _result(
                'EURUSD',
                [_trade('EURUSD', '2026-03-01 10:00:00', '2026-03-01 12:00:00', 10.0, 1.0, quality_score=0.1)],
            ),
            'GBPUSD': _result(
                'GBPUSD',
                [_trade('GBPUSD', '2026-03-01 10:00:00', '2026-03-01 13:00:00', 8.0, 0.8, quality_score=0.9)],
            ),
        }

        simulation = calculate_execution_aware_compounding_pnl(
            results,
            starting_balance=1000.0,
            risk_pct=0.05,
            params=params,
        )

        self.assertEqual(simulation.total_trades, 1)
        self.assertEqual(simulation.trade_log[0][0], 'GBPUSD')
        self.assertEqual(simulation.skip_counts, {'REPLACED_BY_HIGHER_QUALITY': 1})

    def test_active_trade_is_nonreplaceable_for_later_correlated_candidate(self):
        params = StrategyParams(
            max_correlated_trades=1,
            correlation_prefer_quality=True,
        )
        results = {
            'EURUSD': _result(
                'EURUSD',
                [_trade('EURUSD', '2026-03-01 10:00:00', '2026-03-01 12:00:00', 10.0, 1.0, quality_score=0.1)],
            ),
            'GBPUSD': _result(
                'GBPUSD',
                [_trade('GBPUSD', '2026-03-01 11:00:00', '2026-03-01 13:00:00', 8.0, 0.8, quality_score=0.9)],
            ),
        }

        simulation = calculate_execution_aware_compounding_pnl(
            results,
            starting_balance=1000.0,
            risk_pct=0.05,
            params=params,
        )

        self.assertEqual(simulation.total_trades, 1)
        self.assertEqual(simulation.trade_log[0][0], 'EURUSD')
        self.assertEqual(simulation.skip_counts, {'CORRELATION_CAP': 1})

    def test_pair_cooldown_blocks_same_pair_reentry_after_loss(self):
        params = StrategyParams(cooldown_bars=2)
        results = {
            'EURUSD': _result(
                'EURUSD',
                [
                    _trade('EURUSD', '2026-03-01 08:00:00', '2026-03-01 10:00:00', -10.0, -1.0),
                    _trade('EURUSD', '2026-03-01 11:00:00', '2026-03-01 13:00:00', 10.0, 1.0),
                ],
            ),
        }

        simulation = calculate_execution_aware_compounding_pnl(
            results,
            starting_balance=1000.0,
            risk_pct=0.05,
            params=params,
        )

        self.assertEqual(simulation.total_trades, 1)
        self.assertEqual(simulation.skip_counts, {'COOLDOWN': 1})

    def test_risk_budget_blocks_second_same_timestamp_candidate(self):
        params = StrategyParams(
            max_correlated_trades=2,
            quality_sizing=True,
            quality_risk_min=1.0,
            quality_risk_max=1.5,
        )
        results = {
            'AUDCAD': _result(
                'AUDCAD',
                [_trade('AUDCAD', '2026-03-01 10:00:00', '2026-03-01 12:00:00', 10.0, 1.0, quality_score=1.0)],
            ),
            'EURUSD': _result(
                'EURUSD',
                [_trade('EURUSD', '2026-03-01 10:00:00', '2026-03-01 12:00:00', 10.0, 1.0, quality_score=1.0)],
            ),
        }

        simulation = calculate_execution_aware_compounding_pnl(
            results,
            starting_balance=1000.0,
            risk_pct=0.05,
            params=params,
        )

        self.assertEqual(simulation.total_trades, 1)
        self.assertEqual(simulation.skip_counts, {'RISK_BUDGET_FULL': 1})


if __name__ == '__main__':
    unittest.main()
