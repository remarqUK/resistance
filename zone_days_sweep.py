"""Sweep zone_history_days to find the optimal zone lookback window.

Backtests the last 6 months (hourly_days=180).
Tests zone_history_days: [60, 90, 120, 150, 180] (30-day increments)

Note: unlike other param sweeps, this must rebuild the zone cache for each
value tested — zone_history_days affects the cache itself, not just strategy params.
"""
import sys
import os
import time

sys.path.insert(0, os.path.dirname(__file__))

from fx_sr.profiles import DEFAULT_PROFILE, PROFILES, PAIRS
from fx_sr.strategy import params_from_profile
from fx_sr.backtest import (
    _load_cached_backtest_data,
    precompute_zone_cache_parallel,
    run_backtest_fast,
    calculate_execution_aware_compounding_pnl,
)

PROFILE_NAME = DEFAULT_PROFILE
HOURLY_DAYS = 178           # backtest window: ~6 months (180 hits cache boundary)
SWEEP_VALUES = [60, 90, 120, 150, 180]  # 30-day increments


def _score(profile, data, minute_data, execution_mode, zone_history_days):
    """Rebuild zone cache for given lookback and run full backtest."""
    t0 = time.time()
    zone_cache = precompute_zone_cache_parallel(data, zone_history_days=zone_history_days)

    merged = dict(profile)
    merged['zone_history_days'] = zone_history_days
    params = params_from_profile(merged)

    results = {}
    for pair, (_, hourly_df) in data.items():
        pip = PAIRS[pair]['pip']
        minute_df = minute_data.get(pair)
        results[pair] = run_backtest_fast(
            hourly_df, pair, params, zone_cache, pip,
            minute_df=minute_df,
            execution_mode=execution_mode,
        )

    if not results:
        return None

    starting_balance = profile['starting_balance']
    risk_pct = profile['risk_pct'] / 100.0

    simulation = calculate_execution_aware_compounding_pnl(
        results,
        starting_balance=starting_balance,
        risk_pct=risk_pct,
        params=params,
    )

    trade_log = simulation.trade_log
    if not trade_log:
        return None

    final_balance = simulation.final_balance
    wins = sum(1 for _, trade, _, _, _ in trade_log if trade.pnl_r > 0)
    win_rate = wins / len(trade_log) * 100

    peak = starting_balance
    max_dd = 0.0
    for _, _, _, _, balance in trade_log:
        if balance > peak:
            peak = balance
        dd = (peak - balance) / peak * 100 if peak else 0.0
        if dd > max_dd:
            max_dd = dd

    ret_pct = (final_balance - starting_balance) / starting_balance * 100

    return {
        'trades': len(trade_log),
        'wr': win_rate,
        'ret': ret_pct,
        'final': final_balance,
        'max_dd': max_dd,
        'elapsed': time.time() - t0,
    }


def main():
    t_total = time.time()
    profile = PROFILES[PROFILE_NAME]
    current_days = profile['zone_history_days']
    execution_mode = profile.get('execution_mode', 'next_bar')

    # Load data once with max zone lookback — enough for all sweep values
    max_zone_days = max(SWEEP_VALUES)

    print(f"Profile: {PROFILE_NAME}  (current zone_history_days={current_days})")
    print(f"Backtest window: {HOURLY_DAYS} days  |  Zone sweep: {SWEEP_VALUES}")
    print(f"Execution mode: {execution_mode}")
    print(f"Loading data ({HOURLY_DAYS} hourly days, up to {max_zone_days} zone days)...")

    t0 = time.time()
    data = {}
    minute_data = {}
    for pair, info in PAIRS.items():
        daily_df, hourly_df, minute_df = _load_cached_backtest_data(
            info['ticker'], HOURLY_DAYS, max_zone_days
        )
        if daily_df.empty or hourly_df.empty:
            continue
        data[pair] = (daily_df, hourly_df)
        if execution_mode == 'intrabar' and not minute_df.empty:
            minute_data[pair] = minute_df

    print(f"  {len(data)} pairs loaded in {time.time()-t0:.1f}s")
    if execution_mode == 'intrabar':
        print(f"  Minute data: {len(minute_data)}/{len(data)} pairs")
    print()

    print('=' * 72)
    print(f"  {'Days':>6} {'Trades':>7} {'WR%':>7} {'Return%':>10} {'Final':>12} {'MaxDD%':>8}  {'Time':>6}")
    print(f"  {'-' * 66}")

    all_results = {}
    for days in SWEEP_VALUES:
        r = _score(profile, data, minute_data, execution_mode, days)
        if r:
            all_results[days] = r
            marker = ' <-- current' if days == current_days else ''
            print(
                f"  {days:>6} {r['trades']:>7} {r['wr']:>6.1f}% "
                f"{r['ret']:>+9.1f}%  {r['final']:>10,.0f} "
                f"{r['max_dd']:>7.1f}%  {r['elapsed']:>5.1f}s{marker}"
            )
        else:
            marker = ' <-- current' if days == current_days else ''
            print(f"  {days:>6}       0     n/a       n/a         n/a     n/a{marker}")

    if not all_results:
        print("\nNo results — check that data cache is populated for the last 6 months.")
        return

    best = max(all_results, key=lambda d: all_results[d]['ret'])
    best_r = all_results[best]

    print(f"\n{'=' * 72}")
    print(f"  BEST: zone_history_days={best}")
    print(f"  Return={best_r['ret']:+.1f}%  Trades={best_r['trades']}  WR={best_r['wr']:.1f}%  MaxDD={best_r['max_dd']:.1f}%")
    if best != current_days:
        print(f"  Suggestion: update zone_history_days {current_days} -> {best} in high_volume profile")
    else:
        print(f"  Current value ({current_days}) is already optimal for this window.")
    print(f"{'=' * 72}")
    print(f"\n  Total time: {time.time()-t_total:.0f}s")


if __name__ == '__main__':
    main()
