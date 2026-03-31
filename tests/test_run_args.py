import sys
import unittest
import sys
from types import SimpleNamespace
from unittest.mock import ANY, patch

from fx_sr.strategy import StrategyParams
import run


class RunArgumentTests(unittest.TestCase):
    def test_main_preserves_explicit_backtest_days_and_risk_pct(self):
        argv = ['run.py', 'backtest', '--days', '30', '--risk-pct', '5.0']

        with patch.object(sys, 'argv', argv), \
                patch('run.cmd_backtest') as cmd_backtest:
            run.main()

        parsed = cmd_backtest.call_args.args[0]
        self.assertEqual(parsed.days, 30)
        self.assertEqual(parsed.risk_pct, 5.0)

    def test_main_parses_backtest_baseline_flags(self):
        argv = [
            'run.py',
            'backtest',
            '--save-baseline',
            'artifacts/current.json',
            '--compare-baseline',
            'artifacts/expected.json',
        ]

        with patch.object(sys, 'argv', argv), \
                patch('run.cmd_backtest') as cmd_backtest:
            run.main()

        parsed = cmd_backtest.call_args.args[0]
        self.assertEqual(parsed.save_baseline, 'artifacts/current.json')
        self.assertEqual(parsed.compare_baseline, 'artifacts/expected.json')

    def test_main_parses_backtest_execution_mode(self):
        argv = [
            'run.py',
            'backtest',
            '--execution-mode',
            'intrabar',
        ]

        with patch.object(sys, 'argv', argv), \
                patch('run.cmd_backtest') as cmd_backtest:
            run.main()

        parsed = cmd_backtest.call_args.args[0]
        self.assertEqual(parsed.execution_mode, 'intrabar')

    def test_main_parses_download_minute_backfill_flags(self):
        argv = [
            'run.py',
            'download',
            '--minute-days',
            '365',
            '--minute-only',
        ]

        with patch.object(sys, 'argv', argv), \
                patch('run.cmd_download') as cmd_download:
            run.main()

        parsed = cmd_download.call_args.args[0]
        self.assertEqual(parsed.minute_days, 365)
        self.assertTrue(parsed.minute_only)

    def test_main_parses_sync_alias(self):
        argv = ['run.py', 'sync']

        with patch.object(sys, 'argv', argv), \
                patch('run.cmd_download') as cmd_download:
            run.main()

        cmd_download.assert_called_once()

    def test_main_parses_sync_workers(self):
        argv = [
            'run.py',
            'download',
            '--sync-workers',
            '3',
        ]

        with patch.object(sys, 'argv', argv), \
                patch('run.cmd_download') as cmd_download:
            run.main()

        parsed = cmd_download.call_args.args[0]
        self.assertEqual(parsed.sync_workers, 3)

    def test_main_parses_refresh_all(self):
        argv = [
            'run.py',
            'download',
            '--refresh-all',
        ]

        with patch.object(sys, 'argv', argv), \
                patch('run.cmd_download') as cmd_download:
            run.main()

        parsed = cmd_download.call_args.args[0]
        self.assertTrue(parsed.refresh_all)

    def test_main_parses_live_without_forced_client_id(self):
        argv = ['run.py', 'live', '--once']

        with patch.object(sys, 'argv', argv), \
                patch('run.cmd_live') as cmd_live:
            run.main()

        parsed = cmd_live.call_args.args[0]
        self.assertIsNone(parsed.ibkr_client_id)

    def test_cmd_live_defaults_client_id_to_99(self):
        args = SimpleNamespace(
            pair=None,
            profile='aggressive',
            preset=None,
            zone_history=180,
            execution_mode=None,
            interval=60,
            balance=1000.0,
            account_currency='USD',
            no_positions=True,
            once=True,
            zones=False,
            risk_pct=None,
            paper_trade=False,
            port=8765,
            no_browser=False,
            ibkr_client_id=None,
        )

        with patch('run._configure_ibkr') as configure_ibkr, \
                patch('run._build_strategy_params', return_value=StrategyParams()), \
                patch('run.get_profile', return_value={'execution_mode': 'intrabar', 'risk_pct': 5.0}), \
                patch('run._resolve_pairs', return_value={'EURUSD': {'ticker': 'EURUSD=X'}}), \
                patch('run.load_portfolio_state', return_value={}), \
                patch('run.ibkr.fetch_open_order_pairs', return_value=set()), \
                patch('run.scan_opportunities', return_value=[]), \
                patch('run.build_live_size_plans', return_value={}), \
                patch('run.record_detected_signals'), \
                patch('run.format_signals_with_sizes', return_value=''):
            configure_ibkr.return_value = 99
            run.cmd_live(args)

        configure_ibkr.assert_called_once()
        self.assertEqual(configure_ibkr.call_args.args[0].ibkr_client_id, 99)

    def test_cmd_backtest_does_not_override_explicit_profile_matching_values(self):
        args = SimpleNamespace(
            pair='EURUSD',
            profile='test-profile',
            preset=None,
            zone_history=None,
            days=30,
            balance=None,
            risk_pct=5.0,
            execution_mode='next_bar',
            no_cache=False,
            target_trades=None,
            target_profit_floor=1.0,
            target_win_rate_floor=1.0,
            save_baseline=None,
            compare_baseline=None,
            verbose=False,
            ibkr_client_id=None,
        )
        fake_result = SimpleNamespace(total_trades=0, winning_trades=0, total_pnl_pips=0.0, trades=[], zones=[])
        summary = {
            'total_trades': 0,
            'total_wins': 0,
            'total_pnl': 0.0,
            'win_rate': 0.0,
            'raw_total_trades': 0,
            'raw_total_wins': 0,
            'raw_total_pnl': 0.0,
            'raw_win_rate': 0.0,
        }

        with patch('run._configure_ibkr', return_value=60), \
                patch('run.get_profile', return_value={
                    'hourly_days': 365,
                    'zone_history_days': 180,
                    'risk_pct': 2.0,
                    'starting_balance': None,
                }), \
                patch('run._build_strategy_params', return_value=StrategyParams()), \
                patch('run._resolve_pairs', return_value={'EURUSD': {'ticker': 'EURUSD=X'}}), \
                patch('run.build_backtest_run_config_json', return_value='cfg') as build_config_mock, \
                patch('run.run_all_backtests_parallel', return_value={'EURUSD': fake_result}), \
                patch('run._portfolio_summary', return_value=summary), \
                patch('run.format_results', return_value='formatted'), \
                patch('builtins.print'):
            run.cmd_backtest(args)

        build_config_mock.assert_called_once_with(
            ANY,
            hourly_days=30,
            zone_history_days=180,
            requested_profile='test-profile',
            starting_balance=None,
            risk_pct=5.0,
            selection_label='baseline',
        )


if __name__ == '__main__':
    unittest.main()
