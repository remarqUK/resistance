#!/usr/bin/env python3
"""Run multi-variant backtests with parallel evaluation and constraint filtering."""

from __future__ import annotations

import argparse
import json
import os
import random
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures import ThreadPoolExecutor

from fx_sr.profiles import PAIRS, PROFILES
from fx_sr.backtest import (
    calculate_execution_aware_compounding_pnl,
    precompute_zone_cache_parallel,
    run_backtest_fast,
)
from fx_sr.data import fetch_daily_data, fetch_hourly_data
from fx_sr.strategy import params_from_profile


_WORKER_HOURLY_DATA = None
_WORKER_ZONE_CACHE = None
_WORKER_PIPS = None
_WORKER_PROFILE = None
_WORKER_PAIR_ORDER = None
_WORKER_STARTING_BALANCE = 1000.0
_WORKER_RISK_PCT = 0.05


def _init_worker(hourly_data, zone_cache, pips, base_profile, pair_order, starting_balance, risk_pct):
    global _WORKER_HOURLY_DATA, _WORKER_ZONE_CACHE, _WORKER_PIPS, _WORKER_PROFILE
    global _WORKER_PAIR_ORDER, _WORKER_STARTING_BALANCE, _WORKER_RISK_PCT

    _WORKER_HOURLY_DATA = hourly_data
    _WORKER_ZONE_CACHE = zone_cache
    _WORKER_PIPS = pips
    _WORKER_PROFILE = dict(base_profile)
    _WORKER_PAIR_ORDER = list(pair_order)
    _WORKER_STARTING_BALANCE = float(starting_balance)
    _WORKER_RISK_PCT = float(risk_pct)


def _score_candidate(overrides: dict) -> dict:
    merged = dict(_WORKER_PROFILE)
    merged.update(overrides)
    params = params_from_profile(merged)

    pair_results = {}
    for pair in _WORKER_PAIR_ORDER:
        hourly_df = _WORKER_HOURLY_DATA[pair]
        pip = _WORKER_PIPS[pair]
        pair_results[pair] = run_backtest_fast(hourly_df, pair, params, _WORKER_ZONE_CACHE, pip)

    if not pair_results:
        return {
            'status': 'no_data',
            'overrides': overrides,
            'trades': 0,
            'return_pct': 0.0,
            'max_dd': 0.0,
            'wr': 0.0,
        }

    simulation = calculate_execution_aware_compounding_pnl(
        pair_results,
        starting_balance=_WORKER_STARTING_BALANCE,
        risk_pct=_WORKER_RISK_PCT,
        params=params,
    )

    trade_log = simulation.trade_log
    if not trade_log:
        return {
            'status': 'empty_log',
            'overrides': overrides,
            'trades': 0,
            'return_pct': 0.0,
            'max_dd': 0.0,
            'wr': 0.0,
            'raw_trades': simulation.raw_total_trades,
        }

    peak = _WORKER_STARTING_BALANCE
    max_dd = 0.0
    for _, _, _, _, balance in trade_log:
        if balance > peak:
            peak = float(balance)
        dd = (peak - float(balance)) / peak * 100.0 if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = float(dd)

    trades = len(trade_log)
    wr = simulation.win_rate
    final_balance = float(simulation.final_balance)
    return_pct = (final_balance - _WORKER_STARTING_BALANCE) / _WORKER_STARTING_BALANCE * 100.0

    return {
        'status': 'ok',
        'overrides': overrides,
        'trades': trades,
        'return_pct': float(return_pct),
        'max_dd': max_dd,
        'wr': float(wr),
        'raw_trades': int(simulation.raw_total_trades),
        'raw_wr': 0.0 if simulation.raw_total_trades == 0 else simulation.raw_total_wins / simulation.raw_total_trades * 100.0,
        'final_balance': final_balance,
        'skip_counts': simulation.skip_counts,
    }


def _dedupe_variants(variants: list[dict]) -> list[dict]:
    seen = set()
    uniq = []
    for variant in variants:
        marker = tuple(sorted(variant.items()))
        if marker in seen:
            continue
        seen.add(marker)
        uniq.append(variant)
    return uniq


def _build_variant_space(seed: int, max_variants: int, base_profile: dict) -> list[dict]:
    rng = random.Random(seed)

    param_space = {
        'rr_ratio': [1.0, 1.1, 1.2, 1.3, 1.4, 1.5],
        'early_exit_r': [0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55],
        'sl_buffer_pct': [0.10, 0.12, 0.15, 0.18, 0.20],
        'min_entry_candle_body_pct': [0.0, 0.02, 0.03, 0.05, 0.08],
        'min_zone_touches': [1, 2, 3, 4],
        'zone_penetration_pct': [0.28, 0.32, 0.36, 0.40, 0.44, 0.48],
        'momentum_lookback': [1, 2, 3, 4],
        'momentum_threshold': [0.5, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85],
        'cooldown_bars': [0, 1, 2, 3],
        'max_correlated_trades': [2, 3, 4, 5, 6, 7],
    }

    variants = [{}]

    for key, values in param_space.items():
        baseline_value = base_profile.get(key)
        for value in values:
            if baseline_value is not None and value == baseline_value:
                continue
            variants.append({key: value})

    entry_grid = [
        ('zone_penetration_pct', ('min_entry_candle_body_pct', 'min_zone_touches', 'momentum_lookback')),
        ('min_entry_candle_body_pct', ('zone_penetration_pct', 'momentum_threshold')),
        ('max_correlated_trades', ('cooldown_bars', 'rr_ratio')),
    ]

    for anchor_key, extra_keys in entry_grid:
        key_values = [param_space[anchor_key]] + [param_space[k] for k in extra_keys]
        if len(key_values) == 1:
            continue
        for anchor_value in key_values[0]:
            if anchor_key in base_profile and anchor_value == base_profile[anchor_key]:
                continue
            for combo in range(200):
                variant = {anchor_key: anchor_value}
                for k in extra_keys:
                    values = param_space[k]
                    variant[k] = values[rng.randrange(len(values))]
                variants.append(variant)

    while len(variants) < max_variants:
        variant = {}
        for key, values in param_space.items():
            # 85% chance we sample this dimension to keep candidates focused.
            if rng.random() < 0.85:
                variant[key] = values[rng.randrange(len(values))]
        if variant:
            variants.append(variant)

    variants = _dedupe_variants(variants)
    if len(variants) > max_variants:
        variants = variants[:max_variants]
    return variants


def _fetch_data(profile: dict) -> tuple[dict, dict]:
    hourly_days = int(profile['hourly_days'])
    zone_history_days = int(profile['zone_history_days'])
    data = {}
    hourly_data = {}
    for pair, info in PAIRS.items():
        daily_df = fetch_daily_data(
            info['ticker'],
            days=hourly_days + zone_history_days,
            allow_stale_cache=True,
        )
        hourly_df = fetch_hourly_data(
            info['ticker'],
            days=hourly_days,
            allow_stale_cache=True,
        )
        if daily_df.empty or hourly_df.empty:
            continue
        data[pair] = (daily_df, hourly_df)
        hourly_data[pair] = hourly_df
    if not data:
        raise RuntimeError('No usable pair data found in cache')
    return data, hourly_data


def _evaluate_in_parallel(
    variants: list[dict],
    hourly_data: dict,
    pair_order: list[str],
    zone_cache: dict,
    profile: dict,
    starting_balance: float,
    risk_pct: float,
    max_workers: int,
) -> list[dict]:
    pips = {pair: PAIRS[pair].get('pip', 0.0001) for pair in pair_order}
    tasks = list(enumerate(variants))
    results = []
    init_args = (hourly_data, zone_cache, pips, profile, pair_order, starting_balance, risk_pct)
    try:
        with ProcessPoolExecutor(
            max_workers=max_workers,
            initializer=_init_worker,
            initargs=init_args,
        ) as executor:
            futures = {executor.submit(_score_candidate, variant): idx for idx, variant in tasks}
            for future in as_completed(futures):
                variant_idx = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result = {
                        'status': f'error:{exc}',
                        'overrides': variants[variant_idx],
                        'trades': 0,
                        'return_pct': 0.0,
                        'max_dd': 0.0,
                        'wr': 0.0,
                        'raw_trades': 0,
                        'raw_wr': 0.0,
                        'skip_counts': {},
                    }
                result['variant_idx'] = variant_idx
                results.append(result)
    except (OSError, ValueError, PermissionError, RuntimeError) as exc:
        print(f'Process pool unavailable ({exc}); falling back to thread pool for evaluation.')
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_score_candidate, variant): idx for idx, variant in tasks}
            for future in as_completed(futures):
                variant_idx = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result = {
                        'status': f'error:{exc}',
                        'overrides': variants[variant_idx],
                        'trades': 0,
                        'return_pct': 0.0,
                        'max_dd': 0.0,
                        'wr': 0.0,
                        'raw_trades': 0,
                        'raw_wr': 0.0,
                        'skip_counts': {},
                    }
                result['variant_idx'] = variant_idx
                results.append(result)

    return results


def _render_top(title, rows, limit=20):
    print()
    print(f'{title}')
    print('=' * 80)
    if not rows:
        print('  No rows to display')
        return

    print(f"{'Rank':>4} {'Trades':>7} {'Return%':>10} {'DD%':>7} {'WR%':>7} {'Score':>8} {'Overrides':}")
    print(f'{"":4} {"":7} {"":10} {"":7} {"":7} {"":8} {"":20} {"-"*50}')
    for idx, row in enumerate(rows[:limit], 1):
        overrides = ', '.join(f'{k}={v}' for k, v in sorted(row['overrides'].items())) or 'baseline'
        print(
            f'{idx:>4} {row["trades"]:>7} {row["return_pct"]:>9.1f}% '
            f'{row["max_dd"]:>6.2f}% {row["wr"]:>6.1f}% {row["score"]:>7.3f} {overrides}'
        )


def _label_profile(profile_name: str) -> str:
    return (
        f'{profile_name}'
        f' ({PROFILES.get(profile_name, {}).get("description", "")})'
    )


def _parse_args():
    parser = argparse.ArgumentParser(
        description='Run multivariant backtests for the high-volume strategy with parallel scoring.',
    )
    parser.add_argument(
        '--profile',
        default='high_volume',
        choices=sorted(PROFILES.keys()),
        help='Strategy profile to use as search baseline',
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=min(30, os.cpu_count() or 1),
        help='Max worker processes for parallel backtest variants (default: up to 30)',
    )
    parser.add_argument(
        '--max-variants',
        type=int,
        default=600,
        help='Upper bound for generated candidate variants',
    )
    parser.add_argument(
        '--seed',
        type=int,
        default=42,
        help='PRNG seed for stochastic variant generation',
    )
    parser.add_argument(
        '--target-trades',
        type=int,
        default=768,
        help='Minimum trade count requirement',
    )
    parser.add_argument(
        '--target-dd',
        type=float,
        default=17.3,
        help='Maximum max drawdown percent allowed',
    )
    parser.add_argument(
        '--target-wr',
        type=float,
        default=51.0,
        help='Minimum win-rate percent required',
    )
    parser.add_argument(
        '--zone-cache-workers',
        type=int,
        default=None,
        help='Workers for zone-cache precompute (defaults to max-workers)',
    )
    parser.add_argument(
        '--output-json',
        type=str,
        default=None,
        help='Optional JSON report output path',
    )
    parser.add_argument(
        '--days',
        type=int,
        default=None,
        help='Override hourly_days in profile',
    )
    parser.add_argument(
        '--zone-days',
        type=int,
        default=None,
        help='Override zone_history_days in profile',
    )
    parser.add_argument(
        '--starting-balance',
        type=float,
        default=None,
        help='Override starting balance for compounding',
    )
    parser.add_argument(
        '--risk-pct',
        type=float,
        default=None,
        help='Override risk %% used for compounding',
    )
    return parser.parse_args()


def main():
    args = _parse_args()

    max_workers = max(1, min(30, args.max_workers))
    profile = dict(PROFILES[args.profile])
    if args.days is not None:
        profile['hourly_days'] = int(args.days)
    if args.zone_days is not None:
        profile['zone_history_days'] = int(args.zone_days)

    starting_balance = float(args.starting_balance) if args.starting_balance is not None else float(profile.get('starting_balance', 1000.0))
    risk_pct = float(args.risk_pct) / 100.0 if args.risk_pct is not None else float(profile.get('risk_pct', 5.0)) / 100.0

    print(f'Loading cached data for profile: {_label_profile(args.profile)}')
    data, hourly_data = _fetch_data(profile)
    pair_order = sorted(hourly_data.keys())
    print(f'Loaded {len(pair_order)} pairs')

    t0 = time.time()
    zone_workers = max(1, min(max_workers, args.zone_cache_workers or max_workers))
    zone_cache = precompute_zone_cache_parallel(data, profile['zone_history_days'], max_workers=zone_workers)
    print(f'Zone cache built: {len(zone_cache)} entries in {time.time() - t0:.1f}s')

    variants = _build_variant_space(seed=args.seed, max_variants=args.max_variants, base_profile=profile)
    print(f'Generated {len(variants)} candidate variants')

    # Score baseline in-process for normalization. We share the same evaluator path
    # by priming worker globals once.
    pips = {pair: PAIRS[pair].get('pip', 0.0001) for pair in pair_order}
    _init_worker(hourly_data, zone_cache, pips, profile, pair_order, starting_balance, risk_pct)
    baseline_result = _score_candidate({})
    if baseline_result['status'] != 'ok':
        raise RuntimeError(f'Could not evaluate baseline: {baseline_result["status"]}')
    print(
        f'Baseline: trades={baseline_result["trades"]}, return={baseline_result["return_pct"]:.1f}%, '
        f'dd={baseline_result["max_dd"]:.2f}%, wr={baseline_result["wr"]:.1f}%'
    )

    t1 = time.time()
    results = _evaluate_in_parallel(
        variants,
        hourly_data,
        pair_order,
        zone_cache,
        profile,
        starting_balance,
        risk_pct,
        max_workers=max_workers,
    )
    print(f'Finished {len(results)} variants in {time.time() - t1:.1f}s')

    for r in results:
        r['score'] = 0.6 * (r['return_pct'] / baseline_result['return_pct'] if baseline_result['return_pct'] else 0.0) + \
            0.4 * (r['trades'] / baseline_result['trades'] if baseline_result['trades'] else 0.0)

    valid = [
        r for r in results
        if r['status'] == 'ok' and r['max_dd'] <= args.target_dd
        and r['wr'] >= args.target_wr and r['trades'] >= args.target_trades
    ]
    valid.sort(key=lambda r: r['score'], reverse=True)

    near = [r for r in results if r['status'] == 'ok' and r['max_dd'] <= args.target_dd and r['wr'] >= args.target_wr]
    near.sort(key=lambda r: r['score'], reverse=True)
    near_dd = [r for r in results if r['status'] == 'ok' and r['max_dd'] <= args.target_dd]
    near_dd.sort(key=lambda r: r['score'], reverse=True)

    if valid:
        print(f'\nCandidates meeting all constraints (DD <= {args.target_dd}%, WR >= {args.target_wr}%, trades >= {args.target_trades}):')
        _render_top('Top constrained results', valid, limit=20)
        best = valid[0]
    else:
        print(f'\nNo candidate met all constraints. Showing closest DD+WR-safe results (best score among DD/WR-valid only).')
        _render_top('Top DD+WR safe results', near, limit=20)
        if not near and near_dd:
            print(f'\nNo candidate met the WR floor. Showing best DD-safe candidates.')
            _render_top('Top DD-safe results', near_dd, limit=20)
        best = near[0] if near else None

    if best is None:
        print('No usable candidate produced execution-ready trades.')
    else:
        print(
            '\nBest configuration: '
            + ('baseline' if not best['overrides'] else ', '.join(f'{k}={v}' for k, v in sorted(best["overrides"].items()))
               )
        )
        print(
            f"  Score={best['score']:.3f} trades={best['trades']} return={best['return_pct']:.2f}% "
            f"DD={best['max_dd']:.2f}% WR={best['wr']:.2f}%"
        )

    if args.output_json:
        payload = {
            'profile': args.profile,
            'profile_overrides': {
                k: profile[k] for k in (
                    'hourly_days',
                    'zone_history_days',
                    'starting_balance',
                    'risk_pct',
                )
            },
            'search': {
                'seed': args.seed,
                'max_variants': args.max_variants,
                'max_workers': args.max_workers,
                'target_dd': args.target_dd,
                'target_wr': args.target_wr,
                'target_trades': args.target_trades,
            },
            'generated_variants': len(variants),
            'evaluated_variants': len(results),
            'baseline': baseline_result,
            'best': best,
            'valid_sorted': valid[:50],
            'dd_wr_sorted': near[:50],
        }
        with open(args.output_json, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2, sort_keys=True)
        print(f'\nSaved JSON report: {args.output_json}')


if __name__ == '__main__':
    main()
