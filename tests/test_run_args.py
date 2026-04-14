import sys
import unittest
import sys
from types import SimpleNamespace
from unittest.mock import ANY, patch

import pandas as pd

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

    def test_main_parses_backtest_default_days_and_workers(self):
        argv = ['run.py', 'backtest']

        with patch.object(sys, 'argv', argv), \
                patch('run.cmd_backtest') as cmd_backtest:
            run.main()

        parsed = cmd_backtest.call_args.args[0]
        self.assertEqual(parsed.days, 365)
        self.assertEqual(parsed.backtest_workers, 18)

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
            execution_mode='intrabar',
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

    def test_cmd_fill_uses_dedicated_fill_client_id_range(self):
        args = SimpleNamespace(
            days=30,
            zone_history_days=0,
            verbose=False,
            fill_debug=False,
            pair='EURUSD',
            ib_historical_fetch_concurrency=1,
            ibkr_client_id=None,
        )
        used_client_ids = []

        def fake_download_single_interval(pair_id, pair_info, interval, item_days, client_id=None, verbose=False):
            used_client_ids.append(client_id)
            return 4

        with patch('run._configure_ibkr', return_value=60), \
                patch('fx_sr.fill_pipeline.find_cache_gap_work_items', side_effect=[
                    [('EURUSD', 'EURUSD=X', '1m', None)],
                    [],
                ]), \
                patch('fx_sr.db.init_db'), \
                patch('fx_sr.db.get_db_path', return_value='test-db'), \
                patch('fx_sr.fill_pipeline.download_single_interval', side_effect=fake_download_single_interval), \
                patch('fx_sr.fill_pipeline.refill_interval_from', return_value=[]), \
                patch('run.ibkr.set_historical_fetch_concurrency', return_value=1), \
                patch('run.ibkr.disconnect'), \
                patch('builtins.print'):
            run.cmd_fill(args)

        self.assertEqual(used_client_ids, [2060])

    def test_cmd_fill_recheck_is_announced_and_verbose(self):
        args = SimpleNamespace(
            days=30,
            zone_history_days=0,
            verbose=False,
            fill_debug=False,
            pair='EURUSD',
            ib_historical_fetch_concurrency=1,
            ibkr_client_id=None,
        )

        with patch('run._configure_ibkr', return_value=60), \
                patch('fx_sr.fill_pipeline.find_cache_gap_work_items', side_effect=[
                    [('EURUSD', 'EURUSD=X', '1m', None)],
                    [],
                ]) as find_items_mock, \
                patch('fx_sr.db.init_db'), \
                patch('fx_sr.db.get_db_path', return_value='test-db'), \
                patch('fx_sr.fill_pipeline.download_single_interval', return_value=4), \
                patch('fx_sr.fill_pipeline.refill_interval_from', return_value=[]), \
                patch('run.ibkr.set_historical_fetch_concurrency', return_value=1), \
                patch('run.ibkr.disconnect'), \
                patch('builtins.print') as print_mock:
            run.cmd_fill(args)

        self.assertEqual(find_items_mock.call_args_list[0].kwargs.get('verbose'), True)
        self.assertEqual(find_items_mock.call_args_list[1].kwargs.get('verbose'), True)
        print_mock.assert_any_call('  Rechecking gaps...')

    def test_find_cache_gaps_counts_provider_confirmed_gaps_toward_coverage(self):
        summary = pd.DataFrame([
            {
                'ticker': 'EURUSD=X',
                'interval': '1d',
                'first_ts': '2026-03-01T00:00:00+00:00',
                'last_ts': '2026-04-13T00:00:00+00:00',
                'bars': 30,
            },
            {
                'ticker': 'EURUSD=X',
                'interval': '1h',
                'first_ts': '2026-03-14T00:00:00+00:00',
                'last_ts': '2026-04-13T00:00:00+00:00',
                'bars': 500,
            },
            {
                'ticker': 'EURUSD=X',
                'interval': '1m',
                'first_ts': '2026-03-14T00:00:00+00:00',
                'last_ts': '2026-04-13T00:00:00+00:00',
                'bars': 22000,
            },
        ])

        with patch.object(run, 'PAIRS', {'EURUSD': {'ticker': 'EURUSD=X'}}), \
                patch('fx_sr.fill_pipeline.init_db'), \
                patch('fx_sr.fill_pipeline.get_cache_summary', return_value=summary), \
                patch('fx_sr.fill_pipeline._remaining_days_to_fetch', return_value=0):
            gaps = run._find_cache_gaps(target_days=30, now=pd.Timestamp('2026-04-13T12:00:00+00:00'))

        self.assertEqual(gaps, [])

    def test_main_bootstraps_all_supported_cli_commands(self):
        command_handlers = {
            'status': 'run.cmd_status',
            'fill': 'run.cmd_fill',
            'download': 'run.cmd_download',
            'sync': 'run.cmd_download',
            'backtest': 'run.cmd_backtest',
            'live': 'run.cmd_live',
            'run': 'run.cmd_run',
            'l2': 'run.cmd_l2',
            'viz': 'run.cmd_viz',
        }
        command_argvs = {
            'status': ['run.py', 'status'],
            'fill': ['run.py', 'fill', '--pair', 'EURUSD'],
            'download': ['run.py', 'download', '--days', '30'],
            'sync': ['run.py', 'sync', '--days', '30'],
            'backtest': ['run.py', 'backtest', '--days', '30'],
            'live': ['run.py', 'live', '--once'],
            'run': ['run.py', 'run', '--mode', 'backtest', '--days', '30'],
            'l2': ['run.py', 'l2', '--pair', 'EURUSD', '--once'],
            'viz': ['run.py', 'viz'],
        }

        for command, target in command_handlers.items():
            with self.subTest(command=command):
                with patch.object(sys, 'argv', command_argvs[command]), \
                        patch(target) as command_handler:
                    run.main()
                    command_handler.assert_called_once()


if __name__ == '__main__':
    unittest.main()
