"""Run a targeted parameter sweep aimed at doubling trade count.

Usage:
    python sweep_cli.py --days 365 [--pairs EURUSD,USDJPY,...]

Preloads data once, then runs each variant. Prints the comparison table and
writes a per-variant detail report to `sweep_reports/<timestamp>/<label>.txt`.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

from fx_sr.backtest import format_compounding_results
from fx_sr.profiles import PAIRS
from fx_sr.sweep import (
    SweepResult,
    format_sweep_table,
    preload_sweep_dataset,
    run_variant,
)

def build_variants() -> list[tuple[str, dict]]:
    """Minor-zone unlock sweep. The previous two sweeps showed that entry
    and zone-detection tuning can't break the ~2,500-trade ceiling because
    `get_tradeable_zones` hardcodes major-only. `allow_minor_zones=True`
    uses the new `get_tradeable_zones_permissive` path so minor zones also
    produce signals. All variants keep `max_correlated_trades=5`."""

    CORR5 = {'max_correlated_trades': 5}
    MINOR = {'allow_minor_zones': True}

    return [
        ('baseline', {}),
        ('corr5_only', CORR5),
        ('corr5_minor', {**CORR5, **MINOR}),
        # Minor zones + entry-filter loosening
        ('corr5_minor_pen_25', {**CORR5, **MINOR, 'zone_penetration_pct': 0.25}),
        ('corr5_minor_mom_60', {**CORR5, **MINOR, 'momentum_threshold': 0.60}),
        # Minor zones + zone-detection tweaks
        ('corr5_minor_touches_2', {**CORR5, **MINOR, 'major_touches': 2}),
        # Full-throttle — minor on, aggressive entries
        ('combo_minor_aggressive', {
            **CORR5, **MINOR,
            'zone_penetration_pct': 0.25,
            'momentum_threshold': 0.55,
        }),
        # Minor + tighter SL cap to control risk on lower-conviction setups
        ('corr5_minor_sl_tight', {**CORR5, **MINOR, 'max_sl_pct': 0.18}),
        # Minor + reduced quality risk range (sizing cushion)
        ('corr5_minor_quality_cap', {
            **CORR5, **MINOR,
            'quality_risk_max': 1.2,
        }),
        ('corr5_minor_combo_safe', {
            **CORR5, **MINOR,
            'max_sl_pct': 0.18,
            'quality_risk_max': 1.2,
        }),
    ]


def _write_detail(out_dir: Path, result: SweepResult) -> None:
    detail = format_compounding_results(
        result.trade_log,
        starting_balance=result.starting_balance,
        final_balance=result.final_balance,
        total_pre_filter=result.total_legs,
        title=f'VARIANT DETAIL: {result.label}',
    )
    (out_dir / f'{result.label}.txt').write_text(detail, encoding='utf-8')


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--days', type=int, default=365)
    parser.add_argument('--zone-days', type=int, default=180)
    parser.add_argument(
        '--pairs',
        type=str,
        default=','.join(sorted(PAIRS)),
        help='Comma-separated pair list. Defaults to all configured pairs.',
    )
    parser.add_argument('--profile', type=str, default='high_volume')
    parser.add_argument(
        '--out-dir',
        type=str,
        default='sweep_reports',
        help='Root directory for per-variant detail reports + summary.',
    )
    args = parser.parse_args(argv)

    pairs = [p.strip().upper() for p in args.pairs.split(',') if p.strip()]
    run_stamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    out_dir = Path(args.out_dir) / run_stamp
    out_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    print(f'Preloading {len(pairs)} pairs x {args.days}d ...')
    dataset = preload_sweep_dataset(
        pairs=pairs,
        hourly_days=args.days,
        zone_history_days=args.zone_days,
        profile_name=args.profile,
    )
    print(f'Preload complete in {time.perf_counter() - t0:.1f}s')

    variants = build_variants()
    results: list[SweepResult] = []
    total_variants = len(variants)
    for i, (label, overrides) in enumerate(variants, 1):
        t_variant = time.perf_counter()
        print(
            f'[{i}/{total_variants}] variant: {label}  overrides={overrides}',
            flush=True,
        )
        pair_done = {'n': 0, 'trades': 0}
        t0_variant = time.perf_counter()

        def _on_pair(pair, result):  # noqa: ANN001 — callback signature fixed
            pair_done['n'] += 1
            tc = getattr(result, 'total_trades', 0) if result is not None else 0
            pair_done['trades'] += tc
            print(
                f'    ({pair_done["n"]}/{len(dataset.pairs)}) {pair}: '
                f'{tc} trade rows, '
                f'cum={pair_done["trades"]} '
                f'[{time.perf_counter() - t0_variant:.1f}s]',
                flush=True,
            )

        result = run_variant(
            dataset=dataset, label=label, overrides=overrides,
            progress_callback=_on_pair,
        )
        results.append(result)
        _write_detail(out_dir, result)
        print(
            f'  -> {result.total_trades_real} trades, '
            f'{result.win_rate * 100:.1f}% WR, '
            f'avgW={result.avg_win_r:+.2f}R avgL={result.avg_loss_r:+.2f}R '
            f'exp={result.expectancy_r:+.2f}R, '
            f'final GBP {result.final_balance:,.2g}, '
            f'{result.night_trades} night entries, '
            f'{time.perf_counter() - t_variant:.1f}s total',
            flush=True,
        )

    table = format_sweep_table(results, baseline_idx=0)
    print()
    print(table)
    (out_dir / '_summary.txt').write_text(table, encoding='utf-8')
    print(f'\nPer-variant reports in: {out_dir}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
