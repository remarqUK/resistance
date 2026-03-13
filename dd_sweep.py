"""Sweep dynamic risk parameters to find ~80k% return with <=20% DD.

Strategy: use full risk when equity is strong, reduce during drawdowns.
This preserves upside compounding while capping drawdown.
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(__file__))

from fx_sr.profiles import PROFILES, PAIRS
from fx_sr.strategy import params_from_profile, StrategyParams
from fx_sr.backtest import (
    precompute_zone_cache_parallel, run_backtest_fast,
    apply_correlation_filter,
)
from fx_sr.data import fetch_daily_data, fetch_hourly_data


def fetch_all_data(profile):
    data = {}
    for pair, info in PAIRS.items():
        daily_df = fetch_daily_data(info['ticker'],
                                    days=profile['zone_history_days'] + profile['hourly_days'])
        hourly_df = fetch_hourly_data(info['ticker'], days=profile['hourly_days'])
        if not daily_df.empty and not hourly_df.empty:
            data[pair] = (daily_df, hourly_df)
    return data


def score_dynamic(filtered_trades, base_risk, dd_start, dd_full, floor_risk, starting_balance):
    """Compound with dynamic risk sizing.

    Risk scaling:
      - DD < dd_start%: use base_risk%
      - DD between dd_start% and dd_full%: linear scale from base_risk to floor_risk
      - DD >= dd_full%: use floor_risk%
    """
    balance = starting_balance
    peak = starting_balance
    max_dd = 0.0
    streak = 0
    max_streak = 0
    wins = 0
    n = 0

    for pair, t in filtered_trades:
        # Current drawdown from peak
        dd_pct = (peak - balance) / peak * 100 if peak > 0 else 0

        # Dynamic risk
        if dd_pct <= dd_start:
            risk = base_risk
        elif dd_pct >= dd_full:
            risk = floor_risk
        else:
            frac = (dd_pct - dd_start) / (dd_full - dd_start)
            risk = base_risk - (base_risk - floor_risk) * frac

        risk_amt = balance * (risk / 100.0)
        pnl = risk_amt * t.pnl_r
        balance += pnl
        n += 1

        if balance > peak:
            peak = balance
        dd = (peak - balance) / peak * 100
        if dd > max_dd:
            max_dd = dd

        if t.pnl_r <= 0:
            streak += 1
            if streak > max_streak:
                max_streak = streak
        else:
            streak = 0
            wins += 1

    if n == 0:
        return None
    return {
        'trades': n,
        'return': (balance / starting_balance - 1) * 100,
        'max_dd': max_dd,
        'streak': max_streak,
        'wr': wins / n * 100,
        'final_balance': balance,
    }


def main():
    t0 = time.time()
    base = PROFILES['high_volume']

    print("Loading data...")
    data = fetch_all_data(base)
    print(f"  {len(data)} pairs loaded in {time.time()-t0:.1f}s")

    print("Computing zones + backtests...")
    t1 = time.time()
    zone_cache = precompute_zone_cache_parallel(data, base['zone_history_days'])

    params = params_from_profile(base)
    hourly_dict = {pair: hourly_df for pair, (_, hourly_df) in data.items()}
    pips = {pair: PAIRS[pair]['pip'] for pair in data}

    all_trades = []
    for pair, hourly_df in hourly_dict.items():
        result = run_backtest_fast(hourly_df, pair, params, zone_cache, pips[pair])
        for t in result.trades:
            all_trades.append((pair, t))
    all_trades.sort(key=lambda x: x[1].entry_time)

    # Apply correlation filter once
    filtered = apply_correlation_filter(all_trades, params)
    print(f"  {len(filtered)} trades after correlation filter in {time.time()-t1:.1f}s")

    # Fixed risk baseline
    print("\n  BASELINE (fixed 5% risk):")
    r = score_dynamic(filtered, 5.0, 100, 100, 5.0, base['starting_balance'])
    print(f"  {r['trades']} trades, +{r['return']:.1f}%, DD={r['max_dd']:.1f}%, streak={r['streak']}")

    # Dynamic risk sweep
    base_risks = [5.0, 6.0, 7.0, 8.0, 10.0]
    dd_starts = [3, 5, 8, 10]
    dd_fulls = [12, 15, 18, 20]
    floor_risks = [0.5, 1.0, 1.5, 2.0]

    total = len(base_risks) * len(dd_starts) * len(dd_fulls) * len(floor_risks)
    print(f"\n  Sweeping {total} dynamic risk combos...\n")

    results = []
    for br in base_risks:
        for ds in dd_starts:
            for df in dd_fulls:
                if df <= ds:
                    continue
                for fr in floor_risks:
                    if fr >= br:
                        continue
                    r = score_dynamic(filtered, br, ds, df, fr, base['starting_balance'])
                    if r:
                        results.append({
                            'base_risk': br, 'dd_start': ds, 'dd_full': df,
                            'floor_risk': fr, **r
                        })

    # Filter: DD <= 20%, trades >= 500, sort by return
    w = 120
    print(f"{'='*w}")
    print("DD <= 20%, trades >= 500, sorted by return:")
    print(f"{'='*w}")
    print(f"  {'BaseR':>6} {'DDstart':>8} {'DDfull':>7} {'Floor':>6} {'Trades':>7} {'Return':>14} {'DD':>7} {'Str':>5} {'WR':>6}")
    print(f"  {'-'*(w-2)}")

    targets = [r for r in results if r['max_dd'] <= 20.0 and r['trades'] >= 500]
    targets.sort(key=lambda x: -x['return'])
    for r in targets[:30]:
        print(f"  {r['base_risk']:>5.1f}% {r['dd_start']:>7}% {r['dd_full']:>6}% {r['floor_risk']:>5.1f}% {r['trades']:>7} {r['return']:>+13.1f}% {r['max_dd']:>6.1f}% {r['streak']:>5} {r['wr']:>5.1f}%")

    # Also show DD <= 22% for near-misses
    print(f"\n{'='*w}")
    print("DD 20-22% (near misses), sorted by return:")
    print(f"{'='*w}")
    near = [r for r in results if 20.0 < r['max_dd'] <= 22.0 and r['trades'] >= 500]
    near.sort(key=lambda x: -x['return'])
    for r in near[:15]:
        print(f"  {r['base_risk']:>5.1f}% {r['dd_start']:>7}% {r['dd_full']:>6}% {r['floor_risk']:>5.1f}% {r['trades']:>7} {r['return']:>+13.1f}% {r['max_dd']:>6.1f}% {r['streak']:>5} {r['wr']:>5.1f}%")

    print(f"\n  Total time: {time.time()-t0:.0f}s")


if __name__ == '__main__':
    main()
