#!/usr/bin/env python3
"""Parameter optimizer using coordinate descent + focused grid refinement.

Constraints: DD<=19.5%, risk<=7.5%, trades>=44, same tickers, return>250%.
"""

import sys
import time
import itertools
from multiprocessing import Pool, cpu_count
from functools import partial

from fx_sr.config import PAIRS, DEFAULT_ZONE_HISTORY_DAYS
from fx_sr.data import fetch_daily_data, fetch_hourly_data
from fx_sr.backtest import precompute_zone_cache, run_backtest_fast, apply_correlation_filter
from fx_sr.strategy import StrategyParams

BALANCE = 10000.0
RISK = 0.075
DAYS = 30
ZONE_DAYS = DEFAULT_ZONE_HISTORY_DAYS
MIN_TRADES = 44
MAX_DD = 19.5
TARGET = 250.0

# Module-level globals for multiprocessing (set in main)
_hourly_data = None
_zone_cache = None


def calc_stats(params, hourly_data=None, zone_cache=None):
    """Run fast backtest and return (return%, max_dd%, trade_count)."""
    hd = hourly_data or _hourly_data
    zc = zone_cache or _zone_cache
    results = {}
    for pair in hd:
        pip = PAIRS[pair].get('pip', 0.0001)
        results[pair] = run_backtest_fast(hd[pair], pair, params, zc, pip)

    all_trades = []
    for pair, r in results.items():
        for t in r.trades:
            all_trades.append((pair, t))
    all_trades.sort(key=lambda x: x[1].entry_time)
    filtered = apply_correlation_filter(all_trades, params)

    balance = BALANCE
    peak = BALANCE
    max_dd = 0.0
    for pair, t in filtered:
        pnl = balance * RISK * t.pnl_r
        balance += pnl
        if balance > peak:
            peak = balance
        dd = (peak - balance) / peak * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    ret = (balance - BALANCE) / BALANCE * 100
    return ret, max_dd, len(filtered)


def build_params(cfg):
    """Build StrategyParams from a config dict."""
    return StrategyParams(
        rr_ratio=cfg['rr'], sl_buffer_pct=cfg['sl'], early_exit_r=cfg['early'],
        cooldown_bars=1, min_entry_candle_body_pct=cfg['body'],
        momentum_lookback=cfg['mom_lb'], max_correlated_trades=cfg['corr'],
        zone_penetration_pct=cfg['pen'],
        sideways_bars=cfg['sw_bars'], sideways_threshold=cfg['sw_thresh'],
        max_hold_bars=cfg['max_hold'], friday_tp_pct=cfg['friday'],
        momentum_threshold=cfg['mom_thresh'],
    )


def evaluate_cfg(cfg, hourly_data=None, zone_cache=None):
    """Evaluate a config dict, return (cfg_with_results)."""
    params = build_params(cfg)
    ret, dd, trades = calc_stats(params, hourly_data, zone_cache)
    return {**cfg, 'ret': ret, 'dd': dd, 'trades': trades}


def is_valid(r):
    return r['dd'] <= MAX_DD and r['trades'] >= MIN_TRADES


def sweep_param(base_cfg, param_name, values, hourly_data, zone_cache):
    """Sweep a single parameter, return best valid config."""
    best = None
    results = []
    for v in values:
        cfg = {**base_cfg, param_name: v}
        r = evaluate_cfg(cfg, hourly_data, zone_cache)
        results.append(r)
        if is_valid(r) and (best is None or r['ret'] > best['ret']):
            best = r
    return best, results


def main():
    global _hourly_data, _zone_cache

    print("Loading data from cache...")
    data = {}
    hourly_data = {}
    for pair, info in PAIRS.items():
        d = fetch_daily_data(info['ticker'], days=ZONE_DAYS + DAYS, allow_stale_cache=True)
        h = fetch_hourly_data(info['ticker'], days=DAYS, allow_stale_cache=True)
        if not d.empty and not h.empty:
            data[pair] = (d, h)
            hourly_data[pair] = h
    print(f"Loaded {len(data)} pairs")

    print("Precomputing zone cache...")
    zone_cache = precompute_zone_cache(data)
    print(f"Zone cache: {len(zone_cache)} entries")
    _hourly_data = hourly_data
    _zone_cache = zone_cache

    # Baseline
    base_cfg = {
        'rr': 1.2, 'early': 0.5, 'pen': 0.50, 'corr': 5,
        'sl': 0.15, 'body': 0.15, 'mom_lb': 2, 'mom_thresh': 0.7,
        'sw_bars': 15, 'sw_thresh': 0.3, 'max_hold': 72, 'friday': 0.70,
    }
    r = evaluate_cfg(base_cfg, hourly_data, zone_cache)
    print(f"\nBaseline: {r['ret']:+.1f}% return, {r['dd']:.1f}% DD, {r['trades']} trades\n")

    # ===== PHASE 1: Coordinate descent =====
    print("=" * 70)
    print("PHASE 1: Coordinate descent optimization")
    print("=" * 70)

    param_ranges = {
        'rr':         [0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.8, 2.0, 2.5],
        'early':      [0.2, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.7, 0.8],
        'pen':        [0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60],
        'corr':       [3, 4, 5, 6, 7, 8, 9, 10],
        'sl':         [0.05, 0.08, 0.10, 0.12, 0.15, 0.18, 0.20, 0.25],
        'body':       [0.05, 0.08, 0.10, 0.12, 0.15, 0.18, 0.20],
        'mom_lb':     [1, 2, 3, 4, 5],
        'mom_thresh': [0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
        'sw_bars':    [8, 10, 12, 15, 18, 20, 24],
        'sw_thresh':  [0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.5],
        'max_hold':   [36, 48, 60, 72, 96, 120],
        'friday':     [0.40, 0.50, 0.60, 0.70, 0.80, 0.90],
    }

    current = dict(base_cfg)
    current_ret = r['ret']

    for iteration in range(4):  # Multiple passes
        improved = False
        print(f"\n--- Pass {iteration + 1} ---")

        for param_name, values in param_ranges.items():
            best, results = sweep_param(current, param_name, values, hourly_data, zone_cache)
            if best and best['ret'] > current_ret:
                old_val = current[param_name]
                current = {k: v for k, v in best.items() if k in current}
                current_ret = best['ret']
                improved = True
                print(f"  {param_name}: {old_val} -> {current[param_name]} | "
                      f"{best['ret']:+.1f}% DD={best['dd']:.1f}% T={best['trades']}")

        if not improved:
            print("  No improvement this pass, stopping.")
            break

    r_final = evaluate_cfg(current, hourly_data, zone_cache)
    print(f"\nPhase 1 result: {r_final['ret']:+.1f}% return, {r_final['dd']:.1f}% DD, "
          f"{r_final['trades']} trades")
    print(f"  Config: {current}")

    # ===== PHASE 2: Focused grid around best =====
    print("\n" + "=" * 70)
    print("PHASE 2: Focused grid refinement")
    print("=" * 70)

    def frange(center, step, count, lo=0.01, hi=99):
        vals = set()
        for i in range(-count, count + 1):
            v = round(center + i * step, 4)
            if lo <= v <= hi:
                vals.add(v)
        return sorted(vals)

    fine_ranges = {
        'rr':     frange(current['rr'], 0.05, 3, 0.5),
        'early':  frange(current['early'], 0.05, 3, 0.1),
        'pen':    frange(current['pen'], 0.025, 3, 0.15, 0.65),
        'corr':   frange(current['corr'], 1, 2, 2, 12),
        'sl':     frange(current['sl'], 0.01, 3, 0.03, 0.30),
        'body':   frange(current['body'], 0.01, 3, 0.03, 0.25),
    }

    # Only sweep entry params in the fine grid (6 params)
    # Management params already optimized by coordinate descent
    keys = list(fine_ranges.keys())
    value_lists = [fine_ranges[k] for k in keys]
    grid = list(itertools.product(*value_lists))
    print(f"Fine grid: {len(grid)} combinations across {keys}")

    best2 = r_final
    top_results = []
    t0 = time.time()

    for i, combo in enumerate(grid):
        cfg = dict(current)
        for k, v in zip(keys, combo):
            cfg[k] = v
        r = evaluate_cfg(cfg, hourly_data, zone_cache)

        if is_valid(r):
            top_results.append(r)
            if r['ret'] > best2['ret']:
                best2 = r

        if (i + 1) % 500 == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            eta = (len(grid) - i - 1) / rate
            print(f"  [{i+1}/{len(grid)}] {rate:.1f}/s ETA {eta:.0f}s | best: {best2['ret']:+.1f}%")

    elapsed = time.time() - t0
    print(f"Phase 2 done in {elapsed:.1f}s ({len(grid)} combos)")

    top_results.sort(key=lambda x: x['ret'], reverse=True)
    print(f"\nTop 10 (of {len(top_results)} valid):")
    for j, r in enumerate(top_results[:10]):
        print(f"  {j+1}. {r['ret']:+.1f}% | DD={r['dd']:.1f}% | {r['trades']}T | "
              f"rr={r['rr']} early={r['early']} pen={r['pen']} corr={r['corr']} "
              f"sl={r['sl']} body={r['body']}")

    # ===== PHASE 3: Final coordinate descent from best grid point =====
    if best2['ret'] > r_final['ret']:
        print("\n" + "=" * 70)
        print("PHASE 3: Final polish (coordinate descent from best grid point)")
        print("=" * 70)

        current = {k: v for k, v in best2.items() if k in base_cfg}
        current_ret = best2['ret']

        # Finer ranges for final polish
        fine_param_ranges = {
            'rr':         frange(current['rr'], 0.02, 4, 0.5),
            'early':      frange(current['early'], 0.02, 4, 0.1),
            'pen':        frange(current['pen'], 0.01, 4, 0.15, 0.65),
            'sl':         frange(current['sl'], 0.005, 4, 0.03),
            'body':       frange(current['body'], 0.005, 4, 0.03),
            'sw_bars':    list(range(max(5, current['sw_bars']-4), current['sw_bars']+5)),
            'sw_thresh':  frange(current['sw_thresh'], 0.02, 4, 0.05, 0.6),
            'max_hold':   list(range(max(24, current['max_hold']-12), current['max_hold']+13, 3)),
            'friday':     frange(current['friday'], 0.02, 4, 0.3, 0.95),
            'mom_lb':     [1, 2, 3, 4],
            'mom_thresh': frange(current['mom_thresh'], 0.05, 3, 0.3, 0.95),
        }

        for iteration in range(3):
            improved = False
            for param_name, values in fine_param_ranges.items():
                best, results = sweep_param(current, param_name, values, hourly_data, zone_cache)
                if best and best['ret'] > current_ret:
                    old_val = current[param_name]
                    current = {k: v for k, v in best.items() if k in current}
                    current_ret = best['ret']
                    improved = True
                    print(f"  {param_name}: {old_val} -> {current[param_name]} | "
                          f"{best['ret']:+.1f}% DD={best['dd']:.1f}% T={best['trades']}")
            if not improved:
                break

        best2 = evaluate_cfg(current, hourly_data, zone_cache)

    # ===== Final output =====
    r = best2
    print("\n" + "=" * 70)
    print(f"FINAL BEST: {r['ret']:+.1f}% return, {r['dd']:.1f}% DD, {r['trades']} trades")
    cfg_keys = [k for k in sorted(r.keys()) if k not in ('ret', 'dd', 'trades')]
    for k in cfg_keys:
        print(f"  {k}: {r[k]}")
    print("=" * 70)

    if r['ret'] >= TARGET:
        print(f"\nTarget of {TARGET}% ACHIEVED!")
    else:
        print(f"\nTarget of {TARGET}% not reached. Best: {r['ret']:+.1f}%")


if __name__ == '__main__':
    main()
