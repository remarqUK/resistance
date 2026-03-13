"""Parallel sweep: new entry filters + volume drivers + quality sizing.

Baseline: high_volume profile (532 trades, +87710%, 20% DD, dynamic risk).
Goal: more trades, more profit, DD <= 20%.

Trade-generation params go through backtest. Post-processing params
(quality_sizing, dynamic_risk) are applied at scoring time for efficiency.
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
                              t.zone_strength, t.risk, t.bars_held, t.quality_score)
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
        risk=tt[13], bars_held=tt[14],
        quality_score=tt[15] if len(tt) > 15 else 0.0)


def apply_blocks_and_score(pair_trades, blocks, base_profile, quality_sizing=False):
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
    peak_balance = balance

    # Dynamic risk params (always on for high_volume baseline comparison)
    use_dynamic = base_profile.get('dynamic_risk', False)
    dd_start = base_profile.get('dd_risk_start', 5.0)
    dd_full = base_profile.get('dd_risk_full', 18.0)
    dd_floor = base_profile.get('dd_risk_floor', 0.5)

    # Quality sizing params
    q_min = base_profile.get('quality_risk_min', 0.5)
    q_max = base_profile.get('quality_risk_max', 1.5)

    trade_log = []
    for pair, t in all_trades:
        # Dynamic risk sizing
        if use_dynamic and peak_balance > 0:
            dd_pct = (peak_balance - balance) / peak_balance * 100
            if dd_pct <= dd_start:
                effective_risk = risk_pct
            elif dd_pct >= dd_full:
                effective_risk = dd_floor / 100.0
            else:
                frac = (dd_pct - dd_start) / (dd_full - dd_start)
                effective_risk = risk_pct - (risk_pct - dd_floor / 100.0) * frac
        else:
            effective_risk = risk_pct

        # Quality-based risk scaling
        if quality_sizing:
            multiplier = q_min + t.quality_score * (q_max - q_min)
            effective_risk *= multiplier

        risk_amt = balance * effective_risk
        pnl = risk_amt * t.pnl_r
        balance += pnl
        if balance > peak_balance:
            peak_balance = balance
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


# Short label for param names
ABBREV = {
    'zone_penetration_pct': 'zp', 'min_entry_candle_body_pct': 'body',
    'min_zone_touches': 'zt', 'max_linger_bars': 'ling',
    'linger_lookback': 'llb', 'zone_exhaustion_threshold': 'exh',
    'zone_exhaustion_lookback': 'elb',
}


def main():
    t0 = time.time()
    base = PROFILES['high_volume']

    print("Loading data...")
    data = fetch_all_data(base)
    print(f"  {len(data)} pairs loaded in {time.time()-t0:.1f}s")

    print("Pre-computing zone cache (parallel, major_touches=1)...")
    t1 = time.time()
    import fx_sr.levels as levels_mod
    orig_major = levels_mod.DEFAULT_MAJOR_TOUCHES
    levels_mod.DEFAULT_MAJOR_TOUCHES = 1
    zone_cache = precompute_zone_cache_parallel(data, base['zone_history_days'])
    levels_mod.DEFAULT_MAJOR_TOUCHES = orig_major
    print(f"  {len(zone_cache)} zone entries in {time.time()-t1:.1f}s")

    hourly_dict = {pair: hourly_df for pair, (_, hourly_df) in data.items()}
    pips = {pair: PAIRS[pair]['pip'] for pair in data}

    # ── Trade-generation grid ──
    # Volume drivers + new entry filters. Exit params locked to high_volume.
    grid = {
        'zone_penetration_pct': [0.38, 0.40, 0.42],
        'min_entry_candle_body_pct': [0.03, 0.05],
        'min_zone_touches': [2, 3],
        'max_linger_bars': [0, 3, 5],
        'linger_lookback': [6, 8],
        'zone_exhaustion_threshold': [0, 3, 5],
        'zone_exhaustion_lookback': [30, 50],
    }

    keys = list(grid.keys())
    combos = list(itertools.product(*[grid[k] for k in keys]))
    print(f"\n  Grid: {' x '.join(f'{k}({len(grid[k])})' for k in keys)}")
    print(f"  Total: {len(combos)} backtest configs")

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

    # ── Scoring: block levels x quality_sizing ──
    quality_modes = [('Q:off', False), ('Q:on', True)]
    total_evals = len(configs) * len(block_levels) * len(quality_modes)
    print(f"\n  Scoring {total_evals} combos (blocks x quality_sizing)...")
    t3 = time.time()

    all_results = []
    for cfg_idx, overrides in enumerate(configs):
        pair_trades = raw_results[cfg_idx]
        parts = []
        for k in keys:
            v = overrides[k]
            parts.append(f"{ABBREV.get(k,k)}={v}")
        param_label = ' '.join(parts)

        for block_name, blocks in block_levels:
            for q_label, q_on in quality_modes:
                r = apply_blocks_and_score(pair_trades, blocks, base, quality_sizing=q_on)
                if r:
                    r['label'] = f"[{block_name:>4}|{q_label}] {param_label}"
                    r['block_name'] = block_name
                    r['q_sizing'] = q_label
                    r['overrides'] = overrides
                    all_results.append(r)

    print(f"  Scored {len(all_results)} results in {time.time()-t3:.1f}s")

    w = 140

    # ── PRIMARY: DD <= 20%, trades > 532 (beat baseline), sorted by return ──
    print(f"\n{'='*w}")
    print("BEAT BASELINE: trades > 532, DD <= 20%, sorted by return")
    print(f"  Baseline: 532 trades, +87710% return, 20.0% DD (high_volume)")
    print(f"{'='*w}")
    hdr = f"  {'Label':<95} {'Tr':>4} {'Ret':>10} {'DD':>6} {'Str':>4} {'WR':>5} {'MinM':>5}"
    print(hdr)
    print(f"  {'-'*(w-2)}")

    beat = [r for r in all_results if r['trades'] > 532 and r['max_dd'] <= 20.0]
    beat.sort(key=lambda x: -x['return'])
    for r in beat[:40]:
        print(f"  {r['label']:<95} {r['trades']:>4} {r['return']:>+9.0f}% {r['max_dd']:>5.1f}% {r['streak']:>4} {r['wr']:>4.1f}% {r['min_month']:>5}")

    # ── HIGHEST RETURN: DD <= 20%, any trade count, sorted by return ──
    print(f"\n{'='*w}")
    print("HIGHEST RETURN: DD <= 20%, any trade count, sorted by return")
    print(f"{'='*w}")
    print(hdr)
    print(f"  {'-'*(w-2)}")

    best_ret = [r for r in all_results if r['max_dd'] <= 20.0]
    best_ret.sort(key=lambda x: -x['return'])
    for r in best_ret[:40]:
        print(f"  {r['label']:<95} {r['trades']:>4} {r['return']:>+9.0f}% {r['max_dd']:>5.1f}% {r['streak']:>4} {r['wr']:>4.1f}% {r['min_month']:>5}")

    # ── MOST TRADES: DD <= 20%, sorted by trade count ──
    print(f"\n{'='*w}")
    print("MOST TRADES: DD <= 20%, sorted by trade count")
    print(f"{'='*w}")
    print(hdr)
    print(f"  {'-'*(w-2)}")

    most = [r for r in all_results if r['max_dd'] <= 20.0]
    most.sort(key=lambda x: (-x['trades'], -x['return']))
    for r in most[:30]:
        print(f"  {r['label']:<95} {r['trades']:>4} {r['return']:>+9.0f}% {r['max_dd']:>5.1f}% {r['streak']:>4} {r['wr']:>4.1f}% {r['min_month']:>5}")

    # ── QUALITY SIZING IMPACT: show best Q:on vs best Q:off for same backtest config ──
    print(f"\n{'='*w}")
    print("QUALITY SIZING IMPACT: best Q:on vs Q:off pairs (DD <= 20%, trades >= 400)")
    print(f"{'='*w}")
    by_cfg = {}
    for r in all_results:
        if r['max_dd'] > 20.0 or r['trades'] < 400:
            continue
        # Key on config + block level (quality is the variable)
        cfg_key = (r['block_name'], str(r['overrides']))
        if cfg_key not in by_cfg:
            by_cfg[cfg_key] = {}
        by_cfg[cfg_key][r['q_sizing']] = r

    comparisons = []
    for cfg_key, modes in by_cfg.items():
        if 'Q:off' in modes and 'Q:on' in modes:
            off = modes['Q:off']
            on = modes['Q:on']
            delta_ret = on['return'] - off['return']
            comparisons.append((delta_ret, off, on))
    comparisons.sort(key=lambda x: -x[0])

    print(f"  {'Delta':>8}  {'Q:off ret':>10}  {'Q:on ret':>10}  {'Tr':>4}  {'Config'}")
    print(f"  {'-'*(w-2)}")
    for delta, off, on in comparisons[:20]:
        print(f"  {delta:>+7.0f}%  {off['return']:>+9.0f}%  {on['return']:>+9.0f}%  {off['trades']:>4}  [{off['block_name']:>4}] {' '.join(f'{ABBREV.get(k,k)}={v}' for k,v in off['overrides'].items() if k != 'use_pair_direction_filter')}")

    # ── Best overall detail ──
    candidates = [r for r in all_results if r['max_dd'] <= 20.0 and r['trades'] >= 400]
    candidates.sort(key=lambda x: -x['return'])
    if candidates:
        b = candidates[0]
        print(f"\n{'='*w}")
        print(f"BEST OVERALL DETAIL: {b['label']}")
        print(f"{'='*w}")
        print(f"  Trades={b['trades']} Ret={b['return']:+.0f}% DD={b['max_dd']:.1f}% Streak={b['streak']} WR={b['wr']:.1f}%")
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
