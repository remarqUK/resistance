"""In-memory sweep harness: preload pair data once, run many variants.

Reuses existing fast-path primitives:
- `params_from_profile` (strategy.py) — convert profile dict + overrides to StrategyParams.
- `precompute_zone_cache_parallel` (backtest.py) — zones per (pair, date), shared across variants.
- `run_backtest_fast` (backtest.py) — walk-forward that reads zones from the cache.
- `calculate_execution_aware_compounding_pnl` (backtest.py) — portfolio simulation.

The dataset preloads daily/hourly/minute OHLC + L2 snapshots + zones once. Each
`run_variant` call accepts a profile-override dict and produces a SweepResult
with both leg-level and collapsed group-level stats (so we can honestly
compare win rate and expectancy across variants).
"""

from __future__ import annotations

import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd

from .backtest import (
    BacktestResult,
    _load_cached_backtest_data,
    _slice_daily_window,
    calculate_execution_aware_compounding_pnl,
    precompute_zone_cache_parallel,
    run_backtest_fast,
)
from .config import PAIRS
from .db import load_l2_snapshots
from .levels import SRZone, detect_zones
from .profiles import get_profile
from .strategy import StrategyParams, params_from_profile

# Keys in a profile/overrides dict that, if changed, invalidate the zone
# cache because they feed detect_zones().
ZONE_DETECTION_KEYS: frozenset[str] = frozenset({
    'pivot_window',
    'cluster_tolerance',
    'major_touches',
    'max_zone_width_pct',
})

NIGHT_HOURS_UTC: frozenset[int] = frozenset({23, 0, 1, 2, 3, 4, 5, 6, 7})


def _system_available_memory_gb() -> Optional[float]:
    """Return available physical memory in GB, or None if we can't measure it."""
    try:
        import ctypes

        class _MemStatus(ctypes.Structure):
            _fields_ = [
                ('dwLength', ctypes.c_ulong),
                ('dwMemoryLoad', ctypes.c_ulong),
                ('ullTotalPhys', ctypes.c_ulonglong),
                ('ullAvailPhys', ctypes.c_ulonglong),
                ('ullTotalPageFile', ctypes.c_ulonglong),
                ('ullAvailPageFile', ctypes.c_ulonglong),
                ('ullTotalVirtual', ctypes.c_ulonglong),
                ('ullAvailVirtual', ctypes.c_ulonglong),
                ('ullAvailExtendedVirtual', ctypes.c_ulonglong),
            ]

        status = _MemStatus()
        status.dwLength = ctypes.sizeof(_MemStatus)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status))
        return status.ullAvailPhys / (1024 ** 3)
    except Exception:
        return None


def calc_max_workers(
    budget_fraction: float = 0.85,
    est_gb_per_worker: float = 1.5,
    hard_cap: Optional[int] = None,
) -> int:
    """Pick a worker count that stays roughly under `budget_fraction` of both
    CPU and available memory. Each worker spawns a pandas-importing subprocess,
    so we budget ~1.5 GB per worker by default. Respect `hard_cap` if set."""
    cpu = os.cpu_count() or 4
    cpu_budget = max(2, int(cpu * budget_fraction))
    avail_gb = _system_available_memory_gb()
    if avail_gb is not None:
        mem_budget = max(2, int((avail_gb * budget_fraction) / est_gb_per_worker))
    else:
        mem_budget = cpu_budget
    workers = min(cpu_budget, mem_budget)
    if hard_cap is not None:
        workers = min(workers, hard_cap)
    return max(2, workers)


@dataclass
class SweepDataset:
    """All data required to run a backtest variant without touching Postgres."""

    pairs: list[str]
    hourly_days: int
    zone_history_days: int
    execution_mode: str = 'intrabar'
    starting_balance: float = 1000.0
    risk_pct_fraction: float = 0.06   # stored as a fraction (profile value/100)
    baseline_profile: dict = field(default_factory=dict)
    daily_df_by_pair: dict[str, pd.DataFrame] = field(default_factory=dict)
    hourly_df_by_pair: dict[str, pd.DataFrame] = field(default_factory=dict)
    minute_df_by_pair: dict[str, pd.DataFrame] = field(default_factory=dict)
    l2_by_pair: dict[str, pd.DataFrame] = field(default_factory=dict)
    zone_cache: dict[tuple, Any] = field(default_factory=dict)
    # Cache of zone-caches keyed by a tuple of zone-detection kwargs so
    # variants that change zone detection don't rebuild every time.
    zone_cache_by_kwargs: dict[tuple, dict[tuple, list[SRZone]]] = field(default_factory=dict)


@dataclass
class SweepResult:
    """One variant's outcome — leg-level totals plus group-collapsed stats."""

    label: str
    params: StrategyParams
    overrides: dict
    per_pair_results: dict[str, BacktestResult]
    trade_log: list
    starting_balance: float
    final_balance: float
    total_trades_real: int       # collapsed trade-group count
    total_legs: int              # raw record count incl. partial legs
    win_rate: float              # 0-1, group-level, fraction-weighted
    avg_win_r: float
    avg_loss_r: float
    expectancy_r: float
    trades_by_hour_utc: dict[int, int]
    night_trades: int            # entries in UTC 23:00-07:59 inclusive


def preload_sweep_dataset(
    pairs: list[str],
    hourly_days: int,
    zone_history_days: int,
    profile_name: str = 'high_volume',
) -> SweepDataset:
    """Load OHLC, L2 snapshots, and the zone cache once for reuse across variants."""

    profile = dict(get_profile(profile_name))
    dataset = SweepDataset(
        pairs=list(pairs),
        hourly_days=hourly_days,
        zone_history_days=zone_history_days,
        execution_mode=str(profile.get('execution_mode', 'intrabar')),
        starting_balance=float(profile.get('starting_balance', 1000.0)),
        risk_pct_fraction=float(profile.get('risk_pct', 6.0)) / 100.0,
        baseline_profile=profile,
    )

    end_ts = pd.Timestamp.now(tz='UTC')
    start_ts = end_ts - pd.Timedelta(days=int(hourly_days))

    for pair in pairs:
        pair_info = PAIRS.get(pair) or PAIRS.get(pair.upper())
        if pair_info is None:
            raise KeyError(f'Unknown pair {pair} (not in fx_sr.config.PAIRS)')
        daily_df, hourly_df, minute_df = _load_cached_backtest_data(
            pair_info['ticker'], hourly_days, zone_history_days,
        )
        dataset.daily_df_by_pair[pair] = daily_df
        dataset.hourly_df_by_pair[pair] = hourly_df
        dataset.minute_df_by_pair[pair] = minute_df
        dataset.l2_by_pair[pair] = load_l2_snapshots(
            pair_info['ticker'],
            start=start_ts.to_pydatetime(),
            end=end_ts.to_pydatetime(),
        )

    zone_input = {
        pair: (dataset.daily_df_by_pair[pair], dataset.hourly_df_by_pair[pair])
        for pair in pairs
        if not dataset.daily_df_by_pair[pair].empty
        and not dataset.hourly_df_by_pair[pair].empty
    }
    dataset.zone_cache = precompute_zone_cache_parallel(
        zone_input,
        zone_history_days=zone_history_days,
        max_workers=calc_max_workers(),
    )
    return dataset


def _detect_zones_for_pair_chunk(args) -> dict[tuple, list[SRZone]]:
    """Worker — build zones for one (pair, date_strs, daily_df, kwargs) chunk."""
    pair, daily_df, date_strs, zone_history_days, kwargs = args
    out: dict[tuple, list[SRZone]] = {}
    for date_str in date_strs:
        window = _slice_daily_window(daily_df, date_str, zone_history_days)
        if len(window) >= 20:
            out[(pair, date_str)] = detect_zones(window, **kwargs)
        else:
            out[(pair, date_str)] = []
    return out


def _zone_kwargs_from_overrides(baseline_profile: dict, overrides: dict) -> dict:
    """Extract detect_zones kwargs from profile + overrides, mapping names."""
    merged = {**baseline_profile, **overrides}
    return {
        'pivot_window': int(merged.get('pivot_window', 5)),
        'cluster_tolerance': float(merged.get('cluster_tolerance', 0.08)),
        'major_threshold': int(merged.get('major_touches', 3)),
        'max_zone_width_pct': float(merged.get('max_zone_width_pct', 0.35)),
    }


def build_zone_cache(
    dataset: SweepDataset,
    zone_kwargs: dict,
    max_workers: Optional[int] = None,
) -> dict[tuple, list[SRZone]]:
    """Build (or fetch cached) zone cache using specific zone-detection kwargs.

    Caches by kwargs signature on the dataset so repeated variants with the
    same zone-detection params reuse a single build."""
    key = tuple(sorted(zone_kwargs.items()))
    cached = dataset.zone_cache_by_kwargs.get(key)
    if cached is not None:
        return cached
    if max_workers is None:
        max_workers = calc_max_workers()

    # Build per-pair date list and distribute across workers.
    work: list[tuple] = []
    for pair in dataset.pairs:
        hourly = dataset.hourly_df_by_pair.get(pair)
        daily = dataset.daily_df_by_pair.get(pair)
        if hourly is None or hourly.empty or daily is None or daily.empty:
            continue
        seen = set()
        dates: list[str] = []
        for ts in hourly.index:
            d = ts.date() if hasattr(ts, 'date') else ts
            ds = str(d)
            if ds in seen:
                continue
            seen.add(ds)
            dates.append(ds)
        if not dates:
            continue
        chunk = max(1, len(dates) // max(1, max_workers))
        for i in range(0, len(dates), chunk):
            work.append((pair, daily, dates[i:i + chunk], dataset.zone_history_days, zone_kwargs))

    result: dict[tuple, list[SRZone]] = {}
    if max_workers <= 1 or len(work) <= 1:
        for args in work:
            result.update(_detect_zones_for_pair_chunk(args))
    else:
        with ProcessPoolExecutor(max_workers=min(max_workers, len(work))) as pool:
            for part in pool.map(_detect_zones_for_pair_chunk, work):
                result.update(part)

    dataset.zone_cache_by_kwargs[key] = result
    return result


def _run_backtest_fast_for_pair(args) -> tuple[str, Optional[BacktestResult]]:
    """Worker shim — unpacked args pickle better than kwargs in ProcessPool."""

    (
        pair,
        hourly_df,
        params,
        zone_cache,
        pip,
        minute_df,
        l2_snapshots,
        execution_mode,
    ) = args
    result = run_backtest_fast(
        hourly_df,
        pair,
        params,
        zone_cache,
        pip,
        minute_df=minute_df,
        l2_snapshots=l2_snapshots,
        execution_mode=execution_mode,
    )
    return pair, result


def run_variant(
    dataset: SweepDataset,
    label: str,
    overrides: Optional[dict] = None,
    max_workers: Optional[int] = None,
    progress_callback: Optional[Any] = None,
) -> SweepResult:
    if max_workers is None:
        max_workers = calc_max_workers()
    """Run one variant (profile + overrides) across all pairs in parallel."""

    overrides = overrides or {}
    params = params_from_profile(dataset.baseline_profile, **overrides)

    # Pick the right zone cache — rebuild if any zone-detection key changed.
    if ZONE_DETECTION_KEYS & set(overrides.keys()):
        zone_kwargs = _zone_kwargs_from_overrides(dataset.baseline_profile, overrides)
        zone_cache = build_zone_cache(dataset, zone_kwargs, max_workers=max_workers)
    else:
        zone_cache = dataset.zone_cache

    work: list[tuple] = []
    for pair in dataset.pairs:
        daily_df = dataset.daily_df_by_pair.get(pair)
        hourly_df = dataset.hourly_df_by_pair.get(pair)
        if daily_df is None or daily_df.empty or hourly_df is None or hourly_df.empty:
            continue
        pip = PAIRS[pair].get('pip', 0.0001)
        work.append((
            pair,
            hourly_df,
            params,
            zone_cache,
            pip,
            dataset.minute_df_by_pair.get(pair),
            dataset.l2_by_pair.get(pair),
            dataset.execution_mode,
        ))

    per_pair: dict[str, BacktestResult] = {}
    if max_workers <= 1 or len(work) <= 1:
        for args in work:
            pair, result = _run_backtest_fast_for_pair(args)
            if result is not None:
                per_pair[pair] = result
            if progress_callback is not None:
                progress_callback(pair, result)
    else:
        with ProcessPoolExecutor(max_workers=min(max_workers, len(work))) as pool:
            futures = {pool.submit(_run_backtest_fast_for_pair, a): a[0] for a in work}
            for fut in as_completed(futures):
                pair, result = fut.result()
                if result is not None:
                    per_pair[pair] = result
                if progress_callback is not None:
                    progress_callback(pair, result)

    portfolio = calculate_execution_aware_compounding_pnl(
        per_pair,
        starting_balance=dataset.starting_balance,
        risk_pct=dataset.risk_pct_fraction,
        params=params,
    )
    return _summarize_variant(
        label=label,
        params=params,
        overrides=overrides,
        per_pair=per_pair,
        trade_log=portfolio.trade_log,
        starting_balance=dataset.starting_balance,
        final_balance=float(portfolio.final_balance),
    )


def _summarize_variant(
    *,
    label: str,
    params: StrategyParams,
    overrides: dict,
    per_pair: dict[str, BacktestResult],
    trade_log: list,
    starting_balance: float,
    final_balance: float,
) -> SweepResult:
    """Collapse leg-level trade_log to group-level stats + hour distribution."""

    group_r: dict[str, float] = {}
    group_first_entry: dict[str, pd.Timestamp] = {}
    leg_count = len(trade_log)
    for pair, trade, _risk, _pnl, _bal in trade_log:
        gid = getattr(trade, 'trade_group_id', None) or f'{pair}|{trade.entry_time}'
        fraction = float(getattr(trade, 'position_fraction', 1.0))
        group_r[gid] = group_r.get(gid, 0.0) + float(trade.pnl_r) * fraction
        if gid not in group_first_entry:
            group_first_entry[gid] = pd.Timestamp(trade.entry_time)

    hour_hist: dict[int, int] = defaultdict(int)
    night_count = 0
    for entry_ts in group_first_entry.values():
        if entry_ts.tzinfo is None:
            entry_ts = entry_ts.tz_localize('UTC')
        else:
            entry_ts = entry_ts.tz_convert('UTC')
        hour = int(entry_ts.hour)
        hour_hist[hour] += 1
        if hour in NIGHT_HOURS_UTC:
            night_count += 1

    wins = [r for r in group_r.values() if r > 0]
    losses = [r for r in group_r.values() if r < 0]
    n_trades = len(group_r)
    win_rate = (len(wins) / n_trades) if n_trades else 0.0
    avg_win = (sum(wins) / len(wins)) if wins else 0.0
    avg_loss = (sum(losses) / len(losses)) if losses else 0.0
    expectancy = (sum(group_r.values()) / n_trades) if n_trades else 0.0

    return SweepResult(
        label=label,
        params=params,
        overrides=overrides,
        per_pair_results=per_pair,
        trade_log=trade_log,
        starting_balance=starting_balance,
        final_balance=final_balance,
        total_trades_real=n_trades,
        total_legs=leg_count,
        win_rate=win_rate,
        avg_win_r=avg_win,
        avg_loss_r=avg_loss,
        expectancy_r=expectancy,
        trades_by_hour_utc=dict(hour_hist),
        night_trades=night_count,
    )


def format_sweep_table(
    results: list[SweepResult],
    baseline_idx: int = 0,
    balance_threshold: float = 0.80,
    trade_multiplier_target: float = 1.5,
) -> str:
    """Render a side-by-side comparison table with PASS/FAIL flags.

    The "Night%" column shows the fraction of each variant's TOTAL trades that
    fall in UTC 23:00-07:59. "NewNight%" shows the fraction of *incremental*
    trades (vs baseline) that fall in that window — this is the number to
    watch if the goal is to add trades outside the trader's off-hours. A
    variant whose new trades are almost all at night is rejected even if its
    headline numbers look good.
    """

    if not results:
        return '(no results)'
    baseline = results[baseline_idx]
    baseline_night_share = (
        baseline.night_trades / baseline.total_trades_real
        if baseline.total_trades_real else 0.0
    )
    lines = []
    lines.append('=' * 156)
    lines.append('  SWEEP COMPARISON')
    lines.append(
        f'  Baseline: {baseline.label}  '
        f'(final balance GBP {baseline.final_balance:,.2g}, '
        f'{baseline.total_trades_real} trades, '
        f'{baseline.night_trades} at night = {baseline_night_share * 100:.1f}%)'
    )
    lines.append(
        f'  PASS: trades >= {trade_multiplier_target:.1f}x baseline AND '
        f'final balance >= {balance_threshold * 100:.0f}% of baseline AND '
        f"new trades' night-share <= baseline night-share"
    )
    lines.append('=' * 156)
    lines.append(
        f"  {'Variant':<22} {'Trades':>7} {'xBase':>6} "
        f"{'WR':>6} {'AvgW':>6} {'AvgL':>6} {'Exp':>6} "
        f"{'Final GBP':>14} {'%Base':>8} "
        f"{'Night':>6} {'Night%':>7} {'NewNight%':>10} {'Flag':>5}"
    )
    lines.append('-' * 156)
    for r in results:
        trade_mult = (
            r.total_trades_real / baseline.total_trades_real
            if baseline.total_trades_real else 0.0
        )
        balance_pct = (
            r.final_balance / baseline.final_balance * 100.0
            if baseline.final_balance else 0.0
        )
        night_share = (
            r.night_trades / r.total_trades_real if r.total_trades_real else 0.0
        )
        inc_trades = r.total_trades_real - baseline.total_trades_real
        inc_night = r.night_trades - baseline.night_trades
        if inc_trades > 0:
            new_night_share = max(0, inc_night) / inc_trades
            new_night_str = f'{new_night_share * 100:>8.1f}%'
            new_night_ok = new_night_share <= baseline_night_share + 1e-9
        else:
            new_night_str = '       n/a'
            new_night_ok = True  # no new trades, nothing to police
        passes = (
            balance_pct >= balance_threshold * 100.0
            and trade_mult >= trade_multiplier_target
            and new_night_ok
        )
        flag = 'PASS' if passes else 'FAIL'
        lines.append(
            f"  {r.label:<22} {r.total_trades_real:>7} {trade_mult:>5.2f}x "
            f"{r.win_rate * 100:>5.1f}% {r.avg_win_r:>+5.2f} {r.avg_loss_r:>+5.2f} "
            f"{r.expectancy_r:>+5.2f} {r.final_balance:>14,.2g} {balance_pct:>7.2g}% "
            f"{r.night_trades:>6} {night_share * 100:>6.1f}% {new_night_str} {flag:>5}"
        )
    lines.append('=' * 156)
    return '\n'.join(lines)
