"""Run the multi-zone-per-bar prototype over preloaded data.

Usage:
    python mz_sweep_cli.py --days 365 [--pairs EURUSD,USDJPY,...]

Tests three additions on top of the base multi-zone engine:
- Retest cooldown (allow re-entry on the same zone after N bars)
- Breakout signals (bullish break of resistance = LONG, etc.)
- H1 zones merged with D1 zones (shorter-timeframe S/R overlay)
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

from fx_sr.profiles import PAIRS
from fx_sr.multi_zone.mz_h1_zones import build_h1_zone_cache
from fx_sr.multi_zone.mz_runner import (
    MultiZoneSweepResult,
    format_mz_table,
    run_mz_variant,
)
from fx_sr.sweep import calc_max_workers, preload_sweep_dataset


def build_variants() -> list[dict]:
    """Each variant is a dict with:
        label, overrides, cap, cooldown, breakout, h1
    Where `h1` is a bool indicating whether to merge the H1 zone cache
    (the CLI builds it once and passes it when True).
    """
    MINOR = {'allow_minor_zones': True}

    v: list[dict] = []
    v.append({'label': 'baseline_cap_1', 'overrides': {}, 'cap': 1,
              'cooldown': 0, 'breakout': False, 'h1': False})
    v.append({'label': 'cap_3', 'overrides': {}, 'cap': 3,
              'cooldown': 0, 'breakout': False, 'h1': False})
    # --- Retest cooldown variants ---
    v.append({'label': 'cap_3_cooldown_24', 'overrides': {}, 'cap': 3,
              'cooldown': 24, 'breakout': False, 'h1': False})
    v.append({'label': 'cap_3_cooldown_72', 'overrides': {}, 'cap': 3,
              'cooldown': 72, 'breakout': False, 'h1': False})
    # --- Breakout variants ---
    v.append({'label': 'cap_3_breakout', 'overrides': {}, 'cap': 3,
              'cooldown': 0, 'breakout': True, 'h1': False})
    v.append({'label': 'cap_3_breakout_cooldown_24', 'overrides': {}, 'cap': 3,
              'cooldown': 24, 'breakout': True, 'h1': False})
    # --- H1-zone variants ---
    v.append({'label': 'cap_3_h1', 'overrides': {}, 'cap': 3,
              'cooldown': 0, 'breakout': False, 'h1': True})
    v.append({'label': 'cap_5_h1', 'overrides': {}, 'cap': 5,
              'cooldown': 0, 'breakout': False, 'h1': True})
    v.append({'label': 'cap_5_h1_breakout', 'overrides': {}, 'cap': 5,
              'cooldown': 0, 'breakout': True, 'h1': True})
    v.append({'label': 'cap_5_h1_minor', 'overrides': MINOR, 'cap': 5,
              'cooldown': 0, 'breakout': False, 'h1': True})
    # --- Kitchen sink ---
    v.append({'label': 'ALL_minor_h1_break_cooldown24',
              'overrides': MINOR, 'cap': 5,
              'cooldown': 24, 'breakout': True, 'h1': True})
    return v


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--days', type=int, default=365)
    parser.add_argument('--zone-days', type=int, default=180)
    parser.add_argument('--pairs', type=str, default=','.join(sorted(PAIRS)))
    parser.add_argument('--profile', type=str, default='high_volume')
    parser.add_argument('--out-dir', type=str, default='sweep_reports')
    args = parser.parse_args(argv)

    pairs = [p.strip().upper() for p in args.pairs.split(',') if p.strip()]
    run_stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    out_dir = Path(args.out_dir) / f'mz-{run_stamp}'
    out_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    print(f'Preloading {len(pairs)} pairs x {args.days}d ...')
    dataset = preload_sweep_dataset(
        pairs=pairs,
        hourly_days=args.days,
        zone_history_days=args.zone_days,
        profile_name=args.profile,
    )
    print(f'Preload complete in {time.perf_counter() - t0:.1f}s  '
          f'(workers: {calc_max_workers()})')

    # Build H1-zone cache once if any variant needs it
    variants = build_variants()
    need_h1 = any(v['h1'] for v in variants)
    h1_cache = None
    if need_h1:
        t_h1 = time.perf_counter()
        print('Building H1 zone cache ...')
        h1_cache = build_h1_zone_cache(
            dataset.hourly_df_by_pair,
            lookback_bars=500,
            pivot_window=12,
            cluster_tolerance=0.08,
            major_threshold=3,
            max_zone_width_pct=0.35,
            max_workers=calc_max_workers(),
        )
        print(f'H1 zones: {len(h1_cache)} (pair,date) entries in '
              f'{time.perf_counter() - t_h1:.1f}s')

    results: list[MultiZoneSweepResult] = []
    for i, variant in enumerate(variants, 1):
        t_variant = time.perf_counter()
        label = variant['label']
        desc = (
            f"cap={variant['cap']} cooldown={variant['cooldown']} "
            f"break={variant['breakout']} h1={variant['h1']} "
            f"over={variant['overrides']}"
        )
        print(f'[{i}/{len(variants)}] {label}  {desc}', flush=True)
        result = run_mz_variant(
            dataset=dataset,
            label=label,
            overrides=variant['overrides'],
            max_concurrent_per_pair=variant['cap'],
            zone_reentry_cooldown_bars=variant['cooldown'],
            enable_breakout_signals=variant['breakout'],
            h1_zone_cache=(h1_cache if variant['h1'] else None),
        )
        results.append(result)
        rv = sum(p.signals_reversal for p in result.per_pair.values())
        br = sum(p.signals_breakout for p in result.per_pair.values())
        cd = sum(p.entries_blocked_cooldown for p in result.per_pair.values())
        dup = sum(p.entries_blocked_duplicate for p in result.per_pair.values())
        print(
            f'  -> {result.total_trades} trades (rev={rv} brk={br}), '
            f'WR {result.win_rate * 100:.1f}%, exp {result.expectancy_r:+.2f}R, '
            f'final GBP {result.final_balance:,.2g}, '
            f'night {result.night_trades}, '
            f'blocked(dup={dup}, cooldown={cd}), '
            f'{time.perf_counter() - t_variant:.1f}s',
            flush=True,
        )

    table = format_mz_table(results, baseline_idx=0)
    print()
    print(table)
    (out_dir / '_summary.txt').write_text(table, encoding='utf-8')
    print(f'\nResults in: {out_dir}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
