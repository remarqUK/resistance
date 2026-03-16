import json
import unittest
from unittest.mock import patch

from fx_sr.backtest import BacktestResult, _params_signature, _serialize_backtest_result
from fx_sr.backtest_baseline import (
    build_backtest_baseline_artifact,
    compare_backtest_baseline_artifacts,
    format_backtest_baseline_comparison,
    save_backtest_baseline_artifact,
)
from fx_sr.strategy import StrategyParams


def _empty_result(pair: str = 'EURUSD') -> BacktestResult:
    return BacktestResult(
        pair=pair,
        total_trades=0,
        winning_trades=0,
        losing_trades=0,
        early_exits=0,
        win_rate=0.0,
        total_pnl_pips=0.0,
        avg_pnl_pips=0.0,
        avg_win_r=0.0,
        avg_loss_r=0.0,
        max_win_pips=0.0,
        max_loss_pips=0.0,
        profit_factor=0.0,
        trades=[],
        zones=[],
    )


class BacktestBaselineTests(unittest.TestCase):
    def test_build_artifact_uses_matching_cached_signature(self):
        params = StrategyParams()
        result = _empty_result()
        cached_row = {
            'pair': 'EURUSD',
            'params_hash': _params_signature(params),
            'hourly_days': 30,
            'zone_history_days': 180,
            'data_signature': 'sig-123',
            'result_json': _serialize_backtest_result(result),
            'created_at': '2026-03-14T10:00:00+00:00',
            'updated_at': '2026-03-14T10:00:00+00:00',
        }

        with patch('fx_sr.backtest_baseline.load_backtest_results', return_value=[cached_row]):
            artifact = build_backtest_baseline_artifact(
                results={'EURUSD': result},
                params=params,
                requested_profile='high_volume',
                selection_label='baseline',
                hourly_days=30,
                zone_history_days=180,
                starting_balance=None,
                risk_pct=None,
                portfolio_summary={'total_trades': 0, 'total_pnl': 0.0},
            )

        self.assertEqual(artifact['pairs']['EURUSD']['data_signature'], 'sig-123')
        self.assertEqual(artifact['run']['config']['params_hash'], _params_signature(params))
        self.assertEqual(artifact['run']['config']['requested_profile'], 'high_volume')

    def test_compare_reports_exact_match(self):
        params = StrategyParams()
        result = _empty_result()
        with patch('fx_sr.backtest_baseline.load_backtest_results', return_value=[]):
            artifact = build_backtest_baseline_artifact(
                results={'EURUSD': result},
                params=params,
                requested_profile='high_volume',
                selection_label='baseline',
                hourly_days=30,
                zone_history_days=180,
                starting_balance=None,
                risk_pct=None,
                portfolio_summary={'total_trades': 0, 'total_pnl': 0.0},
            )

        comparison = compare_backtest_baseline_artifacts(
            artifact,
            json.loads(json.dumps(artifact)),
        )

        self.assertTrue(comparison['match'])
        self.assertIn('Exact match', format_backtest_baseline_comparison(comparison))

    def test_compare_reports_pair_result_mismatch(self):
        params = StrategyParams()
        result = _empty_result()
        with patch('fx_sr.backtest_baseline.load_backtest_results', return_value=[]):
            expected = build_backtest_baseline_artifact(
                results={'EURUSD': result},
                params=params,
                requested_profile='high_volume',
                selection_label='baseline',
                hourly_days=30,
                zone_history_days=180,
                starting_balance=None,
                risk_pct=None,
                portfolio_summary={'total_trades': 0, 'total_pnl': 0.0},
            )
        actual = json.loads(json.dumps(expected))
        actual['pairs']['EURUSD']['result']['total_trades'] = 1

        comparison = compare_backtest_baseline_artifacts(expected, actual)

        self.assertFalse(comparison['match'])
        self.assertTrue(any('EURUSD: result changed' in item for item in comparison['mismatches']))

    def test_save_artifact_writes_json_to_requested_path(self):
        artifact = {'artifact_version': '1'}

        with patch('fx_sr.backtest_baseline.Path.mkdir') as mkdir_mock, \
                patch('fx_sr.backtest_baseline.Path.write_text') as write_text_mock:
            path = save_backtest_baseline_artifact('artifacts/backtest.json', artifact)

        self.assertEqual(str(path), 'artifacts/backtest.json')
        mkdir_mock.assert_called_once()
        write_text_mock.assert_called_once()


if __name__ == '__main__':
    unittest.main()
