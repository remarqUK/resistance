"""Parallel sweep targeting 500 trades.

Key insight: zone boundaries/touches are identical regardless of major_touches.
Compute zones ONCE with major_touches=1 (all zones are 'major'), then use
min_zone_touches in strategy params to filter. This avoids recomputing zones.
"""
import sys, os, time, itertools
sys.path.insert(0, os.path.dirname(__file__))

import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed
from fx_sr.profiles import PROFILES, PAIRS, BLOCKED_PAIR_DIRECTIONS
from fx_sr.strategy import params_from_profile
from fx_sr.backtest import (
    precompute_zone_cache_parallel, run_backtest_fast,
    calculate_compounding_pnl, BacktestResult,
)
from fx_sr.data import fetch_daily_data, fetch_hourly_data

MAX_WORKERS = min(60, os.cpu_count() or 1, 61)

_w_hourly = None
_w_zc = None
_w_pips = None
_w_base = None


def _init_worker(hourly_dict, zone_cache, pips, base_profile):
    global _w_hourly, _w_zc, _w_pips, _w_base
    _w_hourly = hourly_dict
    _w_zc = zone_cache
    _w_pips = pips
    _w_base = base_profile


def _run_one_config(args):
    cfg_idx, overrides = args
    merged = {**_w_base, **overrides}
    params = params_from_profile(merged)
    pair_trades = {}
    for pair, hourly_df in _w_hourly.items():
        result = run_backtest_fast(hourly_df, pair, params, _w_zc, _w_pips[pair])
        pair_trades[pair] = [(t.entry_time, t.exit_time, t.direction, t.pnl_pips,
                              t.pnl_r, t.exit_reason, t.entry_price, t.exit_price,
                              t.sl_price, t.tp_price, t.zone_upper, t.zone_lower,
                              t.zone_strength, t.risk, t.bars_held)
                             for t in result.trades]
    return cfg_idx, pair_trades


def fetch_all_data(profile):
    data = {}
    for pair, info in PAIRS.items():
        daily_df = fetch_daily_data(info['ticker'],
                                    days=profile['zone_history_days'] + profile['hourly_days'])
        hourly_df = fetch_hourly_data(info['ticker'], days=profile['hourly_days'])
        if not daily_df.empty and not hourly_df.empty:
            data[pair] = (daily_df, hourly_df)
    return data


def rebuild_trade(tt):
    from fx_sr.strategy import Trade
    return Trade(
        entry_time=tt[0], exit_time=tt[1], direction=tt[2],
        pnl_pips=tt[3], pnl_r=tt[4], exit_reason=tt[5],
        entry_price=tt[6], exit_price=tt[7], sl_price=tt[8], tp_price=tt[9],
        zone_upper=tt[10], zone_lower=tt[11], zone_strength=tt[12],
        risk=tt[13], bars_held=tt[14])


def apply_blocks_and_score(pair_trades, blocks, base_profile):
    all_trades = []
    for pair, t_tuples in pair_trades.items():
        for tt in t_tuples:
            if blocks and (pair, tt[2]) in blocks:
                continue
            all_trades.append((pair, rebuild_trade(tt)))
    if not all_trades:
        return None
    all_trades.sort(key=lambda x: x[1].entry_time)

    balance = base_profile['starting_balance']
    risk_pct = base_profile['risk_pct'] / 100.0
    trade_log = []
    for pair, t in all_trades:
        risk_amt = balance * risk_pct
        pnl = risk_amt * t.pnl_r
        balance += pnl
        trade_log.append((pair, t, risk_amt, pnl, balance))
    if not trade_log:
        return None

    peak = base_profile['starting_balance']
    max_dd = 0.0
    streak = 0
    max_streak = 0
    wins = 0
    exit_reasons = {}
    months = {}
    pd_stats = {}

    for pair, t, _, _, bal in trade_log:
        if bal > peak: peak = bal
        dd = (peak - bal) / peak * 100
        if dd > max_dd: max_dd = dd
        if t.pnl_r <= 0:
            streak += 1
            if streak > max_streak: max_streak = streak
        else:
            streak = 0
            wins += 1
        r = t.exit_reason or 'UNKNOWN'
        exit_reasons[r] = exit_reasons.get(r, 0) + 1
        m = str(t.entry_time)[:7]
        months[m] = months.get(m, 0) + 1
        k = f"{pair}_{t.direction}"
        if k not in pd_stats: pd_stats[k] = [0, 0]
        if t.pnl_r > 0: pd_stats[k][0] += 1
        else: pd_stats[k][1] += 1

    n = len(trade_log)
    return {
        'trades': n, 'return': (balance / base_profile['starting_balance'] - 1) * 100,
        'max_dd': max_dd, 'streak': max_streak, 'wr': wins / n * 100,
        'months': months, 'min_month': min(months.values()) if months else 0,
        'final_balance': balance, 'exit_reasons': exit_reasons, 'pd_stats': pd_stats,
    }


def main():
    t0 = time.time()
    base = PROFILES['high_volume']

    print("Loading data...")
    data = fetch_all_data(base)
    print(f"  {len(data)} pairs loaded in {time.time()-t0:.1f}s")

    # Compute zone cache ONCE with major_touches=1 so ALL zones are 'major'.
    # min_zone_touches in strategy params will do the actual filtering.
    print("Pre-computing zone cache (parallel, major_touches=1 to keep all zones)...")
    t1 = time.time()

    # Temporarily override the detect_zones default to use major_touches=1
    import fx_sr.levels as levels_mod
    orig_major = levels_mod.DEFAULT_MAJOR_TOUCHES
    levels_mod.DEFAULT_MAJOR_TOUCHES = 1

    zone_cache = precompute_zone_cache_parallel(data, base['zone_history_days'])

    levels_mod.DEFAULT_MAJOR_TOUCHES = orig_major
    print(f"  {len(zone_cache)} zone entries in {time.time()-t1:.1f}s")

    hourly_dict = {pair: hourly_df for pair, (_, hourly_df) in data.items()}
    pips = {pair: PAIRS[pair]['pip'] for pair in data}

    # ── Parameter grid (focused: fine granularity between 409-trade and 600-trade configs) ──
    grid = {
        'rr_ratio': [1.1],
        'early_exit_r': [0.40],
        'sideways_bars': [15, 20],
        'zone_penetration_pct': [0.40, 0.41, 0.42, 0.43, 0.44, 0.45],
        'momentum_threshold': [0.6, 0.65, 0.7, 0.75, 0.8],
        'min_entry_candle_body_pct': [0.03, 0.05],
        'min_zone_touches': [2, 3],
    }

    keys = list(grid.keys())
    combos = list(itertools.product(*[grid[k] for k in keys]))
    print(f"\n  Grid: {' x '.join(f'{k}({len(grid[k])})' for k in keys)}")
    print(f"  Total: {len(combos)} configs")

    configs = []
    for combo in combos:
        overrides = {'use_pair_direction_filter': False}
        for k, v in zip(keys, combo):
            overrides[k] = v
        configs.append(overrides)

    print(f"\n  Launching {len(configs)} backtests across {MAX_WORKERS} workers...")
    t2 = time.time()

    tasks = [(idx, cfg) for idx, cfg in enumerate(configs)]
    raw_results = {}

    with ProcessPoolExecutor(
        max_workers=MAX_WORKERS,
        initializer=_init_worker,
        initargs=(hourly_dict, zone_cache, pips, base),
    ) as executor:
        futures = {executor.submit(_run_one_config, task): task[0] for task in tasks}
        done = 0
        for future in as_completed(futures):
            cfg_idx, pair_trades = future.result()
            raw_results[cfg_idx] = pair_trades
            done += 1
            if done % 100 == 0 or done == len(configs):
                print(f"    {done}/{len(configs)} done...")

    print(f"  Backtests done in {time.time()-t2:.1f}s")

    # ── Block levels ──
    zero_wr = {
        ('USDCAD', 'LONG'), ('NZDUSD', 'LONG'), ('NZDUSD', 'SHORT'),
        ('GBPJPY', 'LONG'), ('GBPJPY', 'SHORT'),
        ('AUDJPY', 'LONG'), ('AUDJPY', 'SHORT'),
        ('EURCHF', 'LONG'), ('EURCHF', 'SHORT'),
    }
    low15 = zero_wr | {('GBPUSD', 'LONG')}
    low20 = low15 | {
        ('USDCAD', 'SHORT'), ('GBPCAD', 'LONG'), ('GBPCAD', 'SHORT'),
        ('EURCAD', 'LONG'), ('EURCAD', 'SHORT'),
    }
    low25 = low20 | {('AUDUSD', 'SHORT'), ('EURGBP', 'SHORT')}

    block_levels = [
        ('NONE', set()),
        ('0%WR', zero_wr),
        ('<15%', low15),
        ('<20%', low20),
        ('<25%', low25),
        ('ALL', BLOCKED_PAIR_DIRECTIONS),
    ]

    total_evals = len(configs) * len(block_levels)
    print(f"\n  Scoring {total_evals} combos...")
    t3 = time.time()

    all_results = []
    for cfg_idx, overrides in enumerate(configs):
        pair_trades = raw_results[cfg_idx]
        parts = []
        for k in keys:
            v = overrides[k]
            s = k.replace('zone_penetration_pct','zp').replace('early_exit_r','ee') \
                 .replace('rr_ratio','rr').replace('sideways_bars','sw') \
                 .replace('momentum_threshold','mom').replace('min_entry_candle_body_pct','body') \
                 .replace('min_zone_touches','zt')
            parts.append(f"{s}={v}")
        param_label = ' '.join(parts)

        for block_name, blocks in block_levels:
            r = apply_blocks_and_score(pair_trades, blocks, base)
            if r:
                r['label'] = f"[{block_name:>4}] {param_label}"
                r['block_name'] = block_name
                r['overrides'] = overrides
                all_results.append(r)

    print(f"  Scored {len(all_results)} results in {time.time()-t3:.1f}s")

    w = 130

    # ── Results ──
    print(f"\n{'='*w}")
    print("TARGET ~500: trades 400-600, streak <= 7, sorted by return")
    print(f"{'='*w}")
    hdr = f"  {'Label':<85} {'Tr':>4} {'Ret':>9} {'DD':>6} {'Str':>4} {'WR':>5} {'MinM':>5}"
    print(hdr)
    print(f"  {'-'*(w-2)}")

    t500 = [r for r in all_results if 400 <= r['trades'] <= 600 and r['streak'] <= 7]
    t500.sort(key=lambda x: -x['return'])
    for r in t500[:30]:
        print(f"  {r['label']:<85} {r['trades']:>4} {r['return']:>+8.1f}% {r['max_dd']:>5.1f}% {r['streak']:>4} {r['wr']:>4.1f}% {r['min_month']:>5}")

    print(f"\n{'='*w}")
    print("HIGH TRADE COUNT: trades >= 500, sorted by return (any streak)")
    print(f"{'='*w}")
    print(hdr)
    print(f"  {'-'*(w-2)}")

    h500 = [r for r in all_results if r['trades'] >= 500]
    h500.sort(key=lambda x: -x['return'])
    for r in h500[:30]:
        print(f"  {r['label']:<85} {r['trades']:>4} {r['return']:>+8.1f}% {r['max_dd']:>5.1f}% {r['streak']:>4} {r['wr']:>4.1f}% {r['min_month']:>5}")

    print(f"\n{'='*w}")
    print("BEST BALANCED: trades >= 350, streak <= 6, sorted by return")
    print(f"{'='*w}")
    print(hdr)
    print(f"  {'-'*(w-2)}")

    bal = [r for r in all_results if r['trades'] >= 350 and r['streak'] <= 6]
    bal.sort(key=lambda x: -x['return'])
    for r in bal[:20]:
        print(f"  {r['label']:<85} {r['trades']:>4} {r['return']:>+8.1f}% {r['max_dd']:>5.1f}% {r['streak']:>4} {r['wr']:>4.1f}% {r['min_month']:>5}")

    # ── Best ~500 detail ──
    candidates = [r for r in all_results if 400 <= r['trades'] <= 600 and r['streak'] <= 8]
    candidates.sort(key=lambda x: -x['return'])
    if candidates:
        b = candidates[0]
        print(f"\n{'='*w}")
        print(f"BEST ~500 DETAIL: {b['label']}")
        print(f"{'='*w}")
        print(f"  Trades={b['trades']} Ret={b['return']:+.1f}% DD={b['max_dd']:.1f}% Streak={b['streak']} WR={b['wr']:.1f}%")
        print(f"  Exit: {b['exit_reasons']}")
        print(f"  Monthly: {b['months']}")
        print(f"  MinMonth={b['min_month']}")
        print(f"\n  Per pair+direction:")
        for k in sorted(b['pd_stats'].keys()):
            w_c, l_c = b['pd_stats'][k]
            tot = w_c + l_c
            wr = w_c/tot*100 if tot else 0
            flag = " *** LOW" if wr < 25 and tot >= 5 else ""
            print(f"    {k:<22} W={w_c:>3} L={l_c:>3} T={tot:>3} WR={wr:>5.1f}%{flag}")

    print(f"\n  Total time: {time.time()-t0:.0f}s")


if __name__ == '__main__':
    main()
