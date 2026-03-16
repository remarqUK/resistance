"""Helpers for freezing and comparing reproducible backtest baselines."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from .backtest import (
    _params_signature,
    _serialize_backtest_result,
    build_backtest_run_config_json,
    calculate_execution_aware_compounding_pnl,
)
from .db import load_backtest_results


BASELINE_ARTIFACT_VERSION = '2'


def _matching_cache_rows(
    pairs: list[str],
    *,
    params_hash: str,
    hourly_days: int,
    zone_history_days: int,
) -> dict[str, dict]:
    """Return matching cached backtest rows for one specific run configuration."""

    rows = load_backtest_results(pairs=pairs)
    matched: dict[str, dict] = {}
    for row in rows:
        pair = row['pair']
        if pair in matched:
            continue
        if row['params_hash'] != params_hash:
            continue
        if int(row['hourly_days']) != int(hourly_days):
            continue
        if int(row['zone_history_days']) != int(zone_history_days):
            continue
        matched[pair] = row
    return matched


def _compounding_summary(
    results: dict[str, object],
    *,
    starting_balance: float | None,
    risk_pct: float | None,
    params,
) -> dict[str, Any] | None:
    """Return structured compounding summary stats used for baseline comparison."""

    if starting_balance is None or risk_pct is None:
        return None

    simulation = calculate_execution_aware_compounding_pnl(
        results,
        starting_balance=float(starting_balance),
        risk_pct=float(risk_pct) / 100.0,
        params=params,
    )
    trade_log = simulation.trade_log
    final_balance = simulation.final_balance

    peak_balance = float(starting_balance)
    max_drawdown_pct = 0.0
    losing_streak = 0
    max_losing_streak = 0
    exit_types: dict[str, int] = {}

    for _pair, trade, _risk_amount, _pnl_amount, balance in trade_log:
        peak_balance = max(peak_balance, float(balance))
        drawdown_pct = (
            (peak_balance - float(balance)) / peak_balance * 100.0
            if peak_balance > 0
            else 0.0
        )
        max_drawdown_pct = max(max_drawdown_pct, drawdown_pct)

        if trade.pnl_r <= 0:
            losing_streak += 1
            max_losing_streak = max(max_losing_streak, losing_streak)
        else:
            losing_streak = 0

        reason = trade.exit_reason or 'UNKNOWN'
        exit_types[reason] = exit_types.get(reason, 0) + 1

    wins = [entry for entry in trade_log if entry[1].pnl_r > 0]
    losses = [entry for entry in trade_log if entry[1].pnl_r <= 0]
    avg_win_r = (
        sum(float(entry[1].pnl_r) for entry in wins) / len(wins)
        if wins
        else 0.0
    )
    avg_loss_r = (
        sum(float(entry[1].pnl_r) for entry in losses) / len(losses)
        if losses
        else 0.0
    )
    return {
        'starting_balance': float(starting_balance),
        'final_balance': float(final_balance),
        'net_pnl': float(final_balance - float(starting_balance)),
        'return_pct': (
            float(final_balance - float(starting_balance)) / float(starting_balance) * 100.0
            if float(starting_balance) > 0
            else 0.0
        ),
        'trade_count': len(trade_log),
        'raw_total_trades': simulation.raw_total_trades,
        'wins': len(wins),
        'losses': len(losses),
        'win_rate': (len(wins) / len(trade_log) * 100.0) if trade_log else 0.0,
        'avg_win_r': float(avg_win_r),
        'avg_loss_r': float(avg_loss_r),
        'peak_balance': float(peak_balance),
        'max_drawdown_pct': float(max_drawdown_pct),
        'max_losing_streak': int(max_losing_streak),
        'exit_types': dict(sorted(exit_types.items())),
        'skip_counts': dict(sorted(simulation.skip_counts.items())),
    }


def build_backtest_baseline_artifact(
    *,
    results: dict[str, object],
    params,
    requested_profile: str | None,
    selection_label: str | None,
    hourly_days: int,
    zone_history_days: int,
    starting_balance: float | None,
    risk_pct: float | None,
    portfolio_summary: dict[str, Any],
    attempt_logs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a self-contained baseline artifact for one completed backtest run."""

    params_hash = _params_signature(params)
    run_config_json = build_backtest_run_config_json(
        params,
        hourly_days=hourly_days,
        zone_history_days=zone_history_days,
        requested_profile=requested_profile,
        starting_balance=starting_balance,
        risk_pct=risk_pct,
        selection_label=selection_label,
    )
    cached_rows = _matching_cache_rows(
        sorted(results.keys()),
        params_hash=params_hash,
        hourly_days=hourly_days,
        zone_history_days=zone_history_days,
    )

    pair_payloads: dict[str, dict[str, Any]] = {}
    for pair, result in sorted(results.items()):
        cached = cached_rows.get(pair)
        result_json = (
            cached['result_json']
            if cached is not None and cached.get('result_json')
            else _serialize_backtest_result(result)
        )
        pair_payloads[pair] = {
            'data_signature': cached.get('data_signature') if cached is not None else None,
            'result': json.loads(result_json),
        }

    return {
        'artifact_version': BASELINE_ARTIFACT_VERSION,
        'created_at': datetime.now(timezone.utc).isoformat(),
        'run': {
            'pair_count': len(results),
            'pairs': sorted(results.keys()),
            'config': json.loads(run_config_json),
        },
        'portfolio_summary': dict(portfolio_summary),
        'compounding_summary': _compounding_summary(
            results,
            starting_balance=starting_balance,
            risk_pct=risk_pct,
            params=params,
        ),
        'attempt_logs': list(attempt_logs or []),
        'pairs': pair_payloads,
    }


def save_backtest_baseline_artifact(path: str, artifact: dict[str, Any]) -> Path:
    """Write one baseline artifact JSON file to disk."""

    baseline_path = Path(path)
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_text(json.dumps(artifact, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    return baseline_path


def load_backtest_baseline_artifact(path: str) -> dict[str, Any]:
    """Load one previously saved baseline artifact."""

    return json.loads(Path(path).read_text(encoding='utf-8'))


def _numbers_equal(left: Any, right: Any) -> bool:
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        scale = max(1.0, abs(float(left)), abs(float(right)))
        return abs(float(left) - float(right)) <= 1e-9 * scale
    return left == right


def _compare_summary(
    label: str,
    expected: dict[str, Any] | None,
    actual: dict[str, Any] | None,
    *,
    mismatches: list[str],
) -> None:
    """Compare two summary dictionaries and append readable mismatches."""

    if expected is None or actual is None:
        if expected != actual:
            mismatches.append(f'{label}: one side is missing')
        return

    keys = sorted(set(expected) | set(actual))
    for key in keys:
        left = expected.get(key)
        right = actual.get(key)
        if isinstance(left, dict) and isinstance(right, dict):
            _compare_summary(f'{label}.{key}', left, right, mismatches=mismatches)
            continue
        if not _numbers_equal(left, right):
            mismatches.append(f'{label}.{key}: expected {left!r}, got {right!r}')


def _normalized_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def compare_backtest_baseline_artifacts(
    expected: dict[str, Any],
    actual: dict[str, Any],
) -> dict[str, Any]:
    """Compare two baseline artifacts and return a structured diff summary."""

    mismatches: list[str] = []

    _compare_summary('run.config', expected.get('run', {}).get('config'), actual.get('run', {}).get('config'), mismatches=mismatches)
    _compare_summary('portfolio_summary', expected.get('portfolio_summary'), actual.get('portfolio_summary'), mismatches=mismatches)
    _compare_summary('compounding_summary', expected.get('compounding_summary'), actual.get('compounding_summary'), mismatches=mismatches)

    expected_pairs = expected.get('pairs', {})
    actual_pairs = actual.get('pairs', {})
    expected_names = set(expected_pairs)
    actual_names = set(actual_pairs)
    if expected_names != actual_names:
        missing = sorted(expected_names - actual_names)
        extra = sorted(actual_names - expected_names)
        if missing:
            mismatches.append(f'pair set missing: {missing}')
        if extra:
            mismatches.append(f'pair set unexpected: {extra}')

    for pair in sorted(expected_names & actual_names):
        expected_payload = expected_pairs[pair]
        actual_payload = actual_pairs[pair]
        if expected_payload.get('data_signature') != actual_payload.get('data_signature'):
            mismatches.append(
                f"{pair}: data_signature expected {expected_payload.get('data_signature')!r}, "
                f"got {actual_payload.get('data_signature')!r}"
            )
        if _normalized_json(expected_payload.get('result')) != _normalized_json(actual_payload.get('result')):
            expected_result = expected_payload.get('result', {})
            actual_result = actual_payload.get('result', {})
            mismatches.append(
                f"{pair}: result changed "
                f"(trades {expected_result.get('total_trades')} -> {actual_result.get('total_trades')}, "
                f"pnl {expected_result.get('total_pnl_pips')} -> {actual_result.get('total_pnl_pips')})"
            )

    return {
        'match': not mismatches,
        'mismatches': mismatches,
    }


def format_backtest_baseline_comparison(comparison: dict[str, Any]) -> str:
    """Render one baseline comparison summary for the CLI."""

    lines = []
    lines.append('')
    lines.append('  BASELINE COMPARISON')
    lines.append('  ' + '-' * 72)
    if comparison.get('match'):
        lines.append('  Exact match against saved baseline artifact.')
        return '\n'.join(lines)

    mismatches = comparison.get('mismatches', [])
    lines.append(f'  MISMATCH: {len(mismatches)} difference(s) found.')
    for mismatch in mismatches[:20]:
        lines.append(f'  - {mismatch}')
    if len(mismatches) > 20:
        lines.append(f'  - ... {len(mismatches) - 20} more')
    return '\n'.join(lines)
