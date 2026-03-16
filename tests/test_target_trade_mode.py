import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from fx_sr.backtest import BacktestResult
from fx_sr.strategy import StrategyParams, Trade
from run import _portfolio_summary, _run_backtests_until_target


def _trade(pair: str, entry: str, exit_: str, pnl_pips: float, pnl_r: float) -> Trade:
    direction = 'LONG'
    entry_price = 1.1000 if pair.endswith('USD') else 150.000
    exit_price = entry_price + (0.0010 if pnl_pips >= 0 else -0.0010)
    return Trade(
        entry_time=pd.Timestamp(entry, tz='UTC'),
        entry_price=entry_price,
        direction=direction,
        sl_price=entry_price - 0.0020,
        tp_price=entry_price + 0.0040,
        zone_upper=entry_price + 0.0010,
        zone_lower=entry_price - 0.0010,
        zone_strength='major',
        risk=0.0020,
        exit_time=pd.Timestamp(exit_, tz='UTC'),
        exit_price=exit_price,
        exit_reason='TP' if pnl_pips >= 0 else 'SL',
        pnl_pips=pnl_pips,
        pnl_r=pnl_r,
        bars_held=4,
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
        avg_win_r=sum(trade.pnl_r for trade in trades if trade.pnl_pips > 0) / winning_trades if winning_trades else 0.0,
        avg_loss_r=sum(trade.pnl_r for trade in trades if trade.pnl_pips <= 0) / losing_trades if losing_trades else 0.0,
        max_win_pips=max((trade.pnl_pips for trade in trades), default=0.0),
        max_loss_pips=min((trade.pnl_pips for trade in trades), default=0.0),
        profit_factor=float('inf'),
        trades=trades,
        zones=[],
    )


class TargetTradeModeTests(unittest.TestCase):
    def test_portfolio_summary_uses_correlation_filtered_trades(self):
        params = StrategyParams(
            max_correlated_trades=1,
            use_pair_direction_filter=False,
        )
        results = {
            'EURUSD': _result('EURUSD', [_trade('EURUSD', '2026-03-01 10:00:00', '2026-03-01 12:00:00', 10.0, 1.0)]),
            'GBPUSD': _result('GBPUSD', [_trade('GBPUSD', '2026-03-01 10:00:00', '2026-03-01 13:00:00', 8.0, 0.8)]),
        }

        summary = _portfolio_summary(
            results,
            params,
            starting_balance=1000.0,
            risk_pct=0.05,
        )

        self.assertEqual(summary['raw_total_trades'], 2)
        self.assertEqual(summary['raw_total_pnl'], 18.0)
        self.assertEqual(summary['total_trades'], 1)
        self.assertEqual(summary['total_pnl'], 10.0)
        self.assertEqual(summary['win_rate'], 100.0)
        self.assertEqual(summary['skip_counts'], {'CORRELATION_CAP': 1})

    def test_run_backtests_until_target_returns_best_profile_label(self):
        baseline_params = StrategyParams()
        candidate_params = StrategyParams(cooldown_bars=0)
        last_params = StrategyParams(use_time_filters=False)
        args = SimpleNamespace(
            target_profit_floor=1.0,
            target_win_rate_floor=1.0,
            no_cache=False,
        )

        baseline_results = {
            'EURUSD': _result('EURUSD', [_trade('EURUSD', '2026-03-01 08:00:00', '2026-03-01 10:00:00', 5.0, 0.5)]),
        }
        candidate_results = {
            'EURUSD': _result(
                'EURUSD',
                [
                    _trade('EURUSD', '2026-03-01 08:00:00', '2026-03-01 10:00:00', 5.0, 0.5),
                    _trade('EURUSD', '2026-03-02 08:00:00', '2026-03-02 10:00:00', 5.0, 0.5),
                ],
            ),
        }
        last_results = {
            'EURUSD': _result('EURUSD', [_trade('EURUSD', '2026-03-03 08:00:00', '2026-03-03 10:00:00', 4.0, 0.4)]),
        }

        with patch(
            'run._build_target_trade_profile_attempts',
            return_value=[
                ('baseline', baseline_params),
                ('candidate', candidate_params),
                ('last', last_params),
            ],
        ), patch(
            'run.run_all_backtests_parallel',
            side_effect=[baseline_results, candidate_results, last_results],
        ):
            results, params, attempt_logs, summary, label = _run_backtests_until_target(
                params=baseline_params,
                target_trades=99,
                args=args,
                pairs={'EURUSD': {'ticker': 'EURUSD=X'}},
                zone_days=180,
                active_client_id=60,
                hourly_days=365,
            )

        self.assertEqual(label, 'candidate')
        self.assertIs(results, candidate_results)
        self.assertEqual(params, candidate_params)
        self.assertEqual(summary['total_trades'], 2)
        self.assertEqual(attempt_logs[-1]['label'], 'last')

    def test_run_backtests_until_target_prefers_closest_profitable_profile_above_target(self):
        baseline_params = StrategyParams()
        first_over_params = StrategyParams(cooldown_bars=0)
        closest_over_params = StrategyParams(use_time_filters=False)
        args = SimpleNamespace(
            target_profit_floor=1.0,
            target_win_rate_floor=1.0,
            no_cache=False,
        )

        baseline_results = {
            'EURUSD': _result(
                'EURUSD',
                [
                    _trade('EURUSD', '2026-03-01 08:00:00', '2026-03-01 10:00:00', 5.0, 0.5),
                    _trade('EURUSD', '2026-03-02 08:00:00', '2026-03-02 10:00:00', 5.0, 0.5),
                ],
            ),
        }
        first_over_results = {
            'EURUSD': _result(
                'EURUSD',
                [
                    _trade('EURUSD', '2026-03-01 08:00:00', '2026-03-01 10:00:00', 5.0, 0.5),
                    _trade('EURUSD', '2026-03-02 08:00:00', '2026-03-02 10:00:00', 5.0, 0.5),
                    _trade('EURUSD', '2026-03-03 08:00:00', '2026-03-03 10:00:00', 5.0, 0.5),
                    _trade('EURUSD', '2026-03-04 08:00:00', '2026-03-04 10:00:00', 5.0, 0.5),
                    _trade('EURUSD', '2026-03-05 08:00:00', '2026-03-05 10:00:00', 5.0, 0.5),
                ],
            ),
        }
        closest_over_results = {
            'EURUSD': _result(
                'EURUSD',
                [
                    _trade('EURUSD', '2026-03-01 08:00:00', '2026-03-01 10:00:00', 5.0, 0.5),
                    _trade('EURUSD', '2026-03-02 08:00:00', '2026-03-02 10:00:00', 5.0, 0.5),
                    _trade('EURUSD', '2026-03-03 08:00:00', '2026-03-03 10:00:00', 5.0, 0.5),
                    _trade('EURUSD', '2026-03-04 08:00:00', '2026-03-04 10:00:00', 5.0, 0.5),
                ],
            ),
        }

        with patch(
            'run._build_target_trade_profile_attempts',
            return_value=[
                ('baseline', baseline_params),
                ('first-over', first_over_params),
                ('closest-over', closest_over_params),
            ],
        ), patch(
            'run.run_all_backtests_parallel',
            side_effect=[baseline_results, first_over_results, closest_over_results],
        ):
            results, params, attempt_logs, summary, label = _run_backtests_until_target(
                params=baseline_params,
                target_trades=4,
                args=args,
                pairs={'EURUSD': {'ticker': 'EURUSD=X'}},
                zone_days=180,
                active_client_id=60,
                hourly_days=365,
            )

        self.assertEqual(label, 'closest-over')
        self.assertIs(results, closest_over_results)
        self.assertEqual(params, closest_over_params)
        self.assertEqual(summary['total_trades'], 4)
        self.assertEqual(len(attempt_logs), 3)


if __name__ == '__main__':
    unittest.main()
