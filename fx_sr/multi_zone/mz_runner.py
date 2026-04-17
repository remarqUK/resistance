"""Driver that runs the multi-zone prototype across all pairs in a SweepDataset.

Produces a `MultiZoneSweepResult` with:
- Total trade count
- Win rate (naive, per-trade — no group collapse since prototype has no partials)
- Avg win/loss R
- Expectancy per trade
- Manual compounded balance at `risk_pct%` per trade
- Entry-hour histogram + night-trade count

Output is directly comparable to the main sweep's SweepResult numbers so
we can tell whether multi-zone breaks the ~2,400-trade ceiling.
"""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from ..config import PAIRS
from ..strategy import StrategyParams, Trade, params_from_profile
from ..sweep import SweepDataset, calc_max_workers

from .mz_engine import MultiZoneResult, run_multi_zone

NIGHT_HOURS_UTC = frozenset({23, 0, 1, 2, 3, 4, 5, 6, 7})


@dataclass
class MultiZoneSweepResult:
    label: str
    params: StrategyParams
    overrides: dict
    max_concurrent_per_pair: int
    cooldown_bars: int = 0
    breakout: bool = False
    h1_zones: bool = False
    per_pair: dict[str, MultiZoneResult] = field(default_factory=dict)
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    flats: int = 0
    win_rate: float = 0.0
    avg_win_r: float = 0.0
    avg_loss_r: float = 0.0
    expectancy_r: float = 0.0
    starting_balance: float = 1000.0
    final_balance: float = 1000.0
    trades_by_hour_utc: dict[int, int] = field(default_factory=dict)
    night_trades: int = 0


def _run_one_pair(args) -> tuple[str, MultiZoneResult]:
    (
        pair,
        hourly_df,
        zones_by_date,
        params,
        max_concurrent_per_pair,
        cooldown_bars,
        breakout,
        h1_zones_slice,
    ) = args
    return pair, run_multi_zone(
        pair, hourly_df, zones_by_date, params,
        max_concurrent_per_pair=max_concurrent_per_pair,
        zone_reentry_cooldown_bars=cooldown_bars,
        enable_breakout_signals=breakout,
        h1_zones_by_date=h1_zones_slice,
    )


def _pair_zones_slice(zone_cache: dict, pair: str) -> dict:
    """Extract only this pair's entries from the global zone cache."""
    return {k: v for k, v in zone_cache.items() if k[0] == pair}


def run_mz_variant(
    dataset: SweepDataset,
    label: str,
    overrides: Optional[dict] = None,
    max_concurrent_per_pair: int = 3,
    zone_reentry_cooldown_bars: int = 0,
    enable_breakout_signals: bool = False,
    h1_zone_cache: Optional[dict] = None,
    max_workers: Optional[int] = None,
) -> MultiZoneSweepResult:
    """Run the multi-zone prototype with the given overrides."""

    overrides = overrides or {}
    params = params_from_profile(dataset.baseline_profile, **overrides)
    if max_workers is None:
        max_workers = calc_max_workers()

    work: list[tuple] = []
    for pair in dataset.pairs:
        hourly = dataset.hourly_df_by_pair.get(pair)
        if hourly is None or hourly.empty:
            continue
        zones_slice = _pair_zones_slice(dataset.zone_cache, pair)
        if not zones_slice:
            continue
        h1_slice = None
        if h1_zone_cache is not None:
            h1_slice = {k: v for k, v in h1_zone_cache.items() if k[0] == pair}
            if not h1_slice:
                h1_slice = None
        work.append((
            pair, hourly, zones_slice, params, max_concurrent_per_pair,
            zone_reentry_cooldown_bars, enable_breakout_signals, h1_slice,
        ))

    per_pair: dict[str, MultiZoneResult] = {}
    if max_workers <= 1 or len(work) <= 1:
        for args in work:
            pair, mz_result = _run_one_pair(args)
            per_pair[pair] = mz_result
    else:
        with ProcessPoolExecutor(max_workers=min(max_workers, len(work))) as pool:
            futures = {pool.submit(_run_one_pair, a): a[0] for a in work}
            for fut in as_completed(futures):
                pair, mz_result = fut.result()
                per_pair[pair] = mz_result

    # Aggregate trades chronologically and apply naive compounding.
    all_trades: list[tuple[pd.Timestamp, str, Trade]] = []
    for pair, mz in per_pair.items():
        for t in mz.trades:
            if t.exit_time is None:
                continue
            all_trades.append((pd.Timestamp(t.exit_time), pair, t))
    all_trades.sort(key=lambda row: row[0])

    starting = float(dataset.starting_balance)
    balance = starting
    wins_r: list[float] = []
    losses_r: list[float] = []
    flats_r = 0
    hour_hist: dict[int, int] = defaultdict(int)
    night_count = 0
    risk_frac = float(dataset.risk_pct_fraction)

    for _exit_ts, _pair, trade in all_trades:
        r = float(trade.pnl_r)
        # Balance compounding (assumes one share of risk per trade)
        balance *= (1.0 + risk_frac * r)
        if r > 0:
            wins_r.append(r)
        elif r < 0:
            losses_r.append(r)
        else:
            flats_r += 1
        # Entry-hour histogram
        ts = pd.Timestamp(trade.entry_time)
        if ts.tzinfo is None:
            ts = ts.tz_localize('UTC')
        else:
            ts = ts.tz_convert('UTC')
        h = int(ts.hour)
        hour_hist[h] += 1
        if h in NIGHT_HOURS_UTC:
            night_count += 1

    total = len(all_trades)
    win_rate = (len(wins_r) / total) if total else 0.0
    avg_win = (sum(wins_r) / len(wins_r)) if wins_r else 0.0
    avg_loss = (sum(losses_r) / len(losses_r)) if losses_r else 0.0
    sum_r = sum(wins_r) + sum(losses_r)
    expectancy = (sum_r / total) if total else 0.0

    return MultiZoneSweepResult(
        label=label,
        params=params,
        overrides=overrides,
        max_concurrent_per_pair=max_concurrent_per_pair,
        cooldown_bars=zone_reentry_cooldown_bars,
        breakout=enable_breakout_signals,
        h1_zones=h1_zone_cache is not None,
        per_pair=per_pair,
        total_trades=total,
        wins=len(wins_r),
        losses=len(losses_r),
        flats=flats_r,
        win_rate=win_rate,
        avg_win_r=avg_win,
        avg_loss_r=avg_loss,
        expectancy_r=expectancy,
        starting_balance=starting,
        final_balance=balance,
        trades_by_hour_utc=dict(hour_hist),
        night_trades=night_count,
    )


def format_mz_table(
    results: list[MultiZoneSweepResult],
    baseline_idx: int = 0,
) -> str:
    if not results:
        return '(no results)'
    baseline = results[baseline_idx]
    lines = []
    lines.append('=' * 140)
    lines.append('  MULTI-ZONE PROTOTYPE COMPARISON')
    lines.append(
        f'  Baseline: {baseline.label}  '
        f'({baseline.total_trades} trades, '
        f'WR {baseline.win_rate * 100:.1f}%, '
        f'final balance GBP {baseline.final_balance:,.2g})'
    )
    lines.append('=' * 140)
    lines.append(
        f"  {'Variant':<30} {'ConcCap':>7} {'Trades':>7} {'xBase':>6} "
        f"{'WR':>6} {'AvgW':>6} {'AvgL':>6} {'Exp':>6} "
        f"{'Final GBP':>16} {'Night':>6} {'Night%':>7}"
    )
    lines.append('-' * 140)
    for r in results:
        mult = r.total_trades / baseline.total_trades if baseline.total_trades else 0.0
        night_pct = r.night_trades / r.total_trades * 100 if r.total_trades else 0.0
        lines.append(
            f"  {r.label:<30} {r.max_concurrent_per_pair:>7} "
            f"{r.total_trades:>7} {mult:>5.2f}x "
            f"{r.win_rate * 100:>5.1f}% {r.avg_win_r:>+5.2f} {r.avg_loss_r:>+5.2f} "
            f"{r.expectancy_r:>+5.2f} {r.final_balance:>16,.2g} "
            f"{r.night_trades:>6} {night_pct:>6.1f}%"
        )
    lines.append('=' * 140)
    return '\n'.join(lines)
