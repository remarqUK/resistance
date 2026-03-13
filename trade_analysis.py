"""Analyze trade generation across pairs and months, test relaxed filters."""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import numpy as np
import pandas as pd
from fx_sr.profiles import PROFILES, PAIRS, BLOCKED_PAIR_DIRECTIONS
from fx_sr.strategy import params_from_profile, StrategyParams
from fx_sr.backtest import (
    precompute_zone_cache_parallel, run_backtest_fast,
    calculate_compounding_pnl, BacktestResult,
)
from fx_sr.data import fetch_daily_data, fetch_hourly_data

def fetch_all_data(profile):
    data = {}
    for pair, info in PAIRS.items():
        daily_df = fetch_daily_data(info['ticker'], days=profile['zone_history_days'] + profile['hourly_days'])
        hourly_df = fetch_hourly_data(info['ticker'], days=profile['hourly_days'])
        if not daily_df.empty and not hourly_df.empty:
            data[pair] = (daily_df, hourly_df)
    return data

def calc_stats(trade_log, starting_balance):
    peak = starting_balance
    max_dd = 0.0
    streak = 0
    max_streak = 0
    for _, t, _, pnl, balance in trade_log:
        if balance > peak: peak = balance
        dd = (peak - balance) / peak * 100
        if dd > max_dd: max_dd = dd
        if t.pnl_r <= 0:
            streak += 1
            if streak > max_streak: max_streak = streak
        else:
            streak = 0
    return max_dd, max_streak

def monthly_breakdown(trade_log):
    months = {}
    for _, t, _, pnl, _ in trade_log:
        m = str(t.entry_time)[:7]
        if m not in months:
            months[m] = 0
        months[m] += 1
    return months

def run_config(data, zone_cache, profile_overrides, extra_blocks=None):
    """Run backtest with profile overrides and optional extra pair+direction blocks."""
    base = {**PROFILES['high_volume'], **profile_overrides}
    params = params_from_profile(base)

    results = {}
    for pair, (daily_df, hourly_df) in data.items():
        pip = PAIRS[pair]['pip']
        results[pair] = run_backtest_fast(hourly_df, pair, params, zone_cache, pip)

    # Apply extra blocks post-hoc
    if extra_blocks:
        for p, r in list(results.items()):
            kept = [t for t in r.trades if (p, t.direction) not in extra_blocks]
            if len(kept) != len(r.trades):
                wins_k = [t for t in kept if t.pnl_pips > 0]
                losses_k = [t for t in kept if t.pnl_pips <= 0]
                results[p] = BacktestResult(
                    pair=p, total_trades=len(kept),
                    winning_trades=len(wins_k), losing_trades=len(losses_k),
                    early_exits=sum(1 for t in kept if t.exit_reason in ('EARLY_EXIT','SIDEWAYS','TIME')),
                    win_rate=len(wins_k)/len(kept)*100 if kept else 0,
                    total_pnl_pips=sum(t.pnl_pips for t in kept),
                    avg_pnl_pips=sum(t.pnl_pips for t in kept)/len(kept) if kept else 0,
                    avg_win_r=np.mean([t.pnl_r for t in wins_k]) if wins_k else 0,
                    avg_loss_r=np.mean([t.pnl_r for t in losses_k]) if losses_k else 0,
                    max_win_pips=max((t.pnl_pips for t in kept), default=0),
                    max_loss_pips=min((t.pnl_pips for t in kept), default=0),
                    profit_factor=sum(t.pnl_pips for t in wins_k)/(abs(sum(t.pnl_pips for t in losses_k)) or 1),
                    trades=kept, zones=r.zones)

    tl, fb = calculate_compounding_pnl(results, base['starting_balance'], base['risk_pct']/100, params)
    if not tl:
        return None
    dd, ms = calc_stats(tl, base['starting_balance'])
    ret = (fb / base['starting_balance'] - 1) * 100
    wins = sum(1 for _, t, _, _, _ in tl if t.pnl_r > 0)
    months = monthly_breakdown(tl)
    min_month_trades = min(months.values()) if months else 0
    return {
        'trades': len(tl), 'return': ret, 'max_dd': dd,
        'streak': ms, 'wr': wins/len(tl)*100,
        'months': months, 'min_month': min_month_trades,
        'final_balance': fb,
    }


def main():
    base = PROFILES['high_volume']
    print("Fetching data...")
    data = fetch_all_data(base)
    print("Pre-computing zones...")
    zone_cache = precompute_zone_cache_parallel(data, base['zone_history_days'])

    # ─── Current state (with all blocks including the 4 new ones) ───
    print("\n" + "="*90)
    print("CURRENT high_volume profile (4 extra blocks, sw_bars=20):")
    print("="*90)
    r = run_config(data, zone_cache, {'sideways_bars': 20})
    if r:
        print(f"  {r['trades']} trades, +{r['return']:.1f}%, DD={r['max_dd']:.1f}%, streak={r['streak']}, WR={r['wr']:.1f}%")
        print(f"  Monthly: {r['months']}")
        print(f"  Min month: {r['min_month']} trades")

    # ─── Remove the 4 new blocks, keep sw_bars=20 ───
    print("\n" + "="*90)
    print("UNBLOCK 4 new pair+dirs (EURJPY SHORT, EURUSD SHORT, GBPUSD LONG, USDJPY SHORT):")
    print("="*90)

    # The new 4 blocks are applied via BLOCKED_PAIR_DIRECTIONS which is global.
    # To test without them, we need to use use_pair_direction_filter=False and
    # apply only the ORIGINAL blocks manually.
    original_blocks = BLOCKED_PAIR_DIRECTIONS - {
        ('EURJPY', 'SHORT'), ('EURUSD', 'SHORT'),
        ('GBPUSD', 'LONG'), ('USDJPY', 'SHORT'),
    }

    # Run with pair_direction_filter OFF, then apply original blocks post-hoc
    r2 = run_config(data, zone_cache, {
        'sideways_bars': 20,
        'use_pair_direction_filter': False,
    }, extra_blocks=original_blocks)
    if r2:
        print(f"  {r2['trades']} trades, +{r2['return']:.1f}%, DD={r2['max_dd']:.1f}%, streak={r2['streak']}, WR={r2['wr']:.1f}%")
        print(f"  Monthly: {r2['months']}")
        print(f"  Min month: {r2['min_month']} trades")

    # ─── Sweep: unblock 4 + relax entries to get more trades ───
    print("\n" + "="*90)
    print("SWEEP: unblock 4 + varying entry relaxation + streak reduction methods:")
    print("="*90)
    print(f"{'Label':<45} {'Tr':>4} {'Ret':>8} {'DD':>6} {'Str':>4} {'WR':>5} {'MinM':>5}")
    print("-"*80)

    configs = [
        # More entry relaxation with original blocks only
        {'zone_penetration_pct': 0.45, 'label': 'zp=0.45'},
        {'zone_penetration_pct': 0.42, 'label': 'zp=0.42'},
        {'zone_penetration_pct': 0.40, 'label': 'zp=0.40'},
        {'min_entry_candle_body_pct': 0.05, 'label': 'body=0.05'},
        {'min_zone_touches': 2, 'label': 'touches=2'},
        {'min_zone_touches': 2, 'zone_penetration_pct': 0.45, 'label': 'touches=2+zp=0.45'},
        # Combo: more entries + streak management
        {'zone_penetration_pct': 0.45, 'max_correlated_trades': 3, 'label': 'zp=0.45+mct=3'},
        {'zone_penetration_pct': 0.45, 'cooldown_bars': 2, 'label': 'zp=0.45+cd=2'},
        {'zone_penetration_pct': 0.42, 'max_correlated_trades': 3, 'label': 'zp=0.42+mct=3'},
        {'zone_penetration_pct': 0.42, 'cooldown_bars': 2, 'label': 'zp=0.42+cd=2'},
        {'zone_penetration_pct': 0.42, 'max_correlated_trades': 2, 'label': 'zp=0.42+mct=2'},
        {'min_zone_touches': 2, 'max_correlated_trades': 3, 'label': 'touches=2+mct=3'},
        {'min_zone_touches': 2, 'zone_penetration_pct': 0.45, 'max_correlated_trades': 3, 'label': 'touches=2+zp=0.45+mct=3'},
        # Very relaxed + aggressive streak control
        {'zone_penetration_pct': 0.40, 'max_correlated_trades': 2, 'label': 'zp=0.40+mct=2'},
        {'zone_penetration_pct': 0.40, 'max_correlated_trades': 3, 'label': 'zp=0.40+mct=3'},
        {'min_zone_touches': 2, 'zone_penetration_pct': 0.42, 'max_correlated_trades': 3, 'label': 'touches=2+zp=0.42+mct=3'},
        # Early exit variations
        {'zone_penetration_pct': 0.45, 'early_exit_r': 0.25, 'label': 'zp=0.45+ee=0.25'},
        {'zone_penetration_pct': 0.45, 'early_exit_r': 0.22, 'label': 'zp=0.45+ee=0.22'},
        # Momentum relaxation
        {'zone_penetration_pct': 0.45, 'momentum_threshold': 0.7, 'label': 'zp=0.45+mom=0.7'},
        {'zone_penetration_pct': 0.45, 'momentum_threshold': 0.6, 'label': 'zp=0.45+mom=0.6'},
    ]

    good = []
    for cfg in configs:
        label = cfg.pop('label')
        overrides = {'sideways_bars': 20, 'use_pair_direction_filter': False, **cfg}
        r = run_config(data, zone_cache, overrides, extra_blocks=original_blocks)
        if r:
            marker = " ***" if r['streak'] <= 5 and r['trades'] >= 180 and r['return'] > 500 else ""
            print(f"  {label:<43} {r['trades']:>4} {r['return']:>+7.1f}% {r['max_dd']:>5.1f}% {r['streak']:>4} {r['wr']:>4.1f}% {r['min_month']:>5}{marker}")
            if r['streak'] <= 6 and r['trades'] >= 150:
                good.append({'label': label, **r})

    # ─── Now test SELECTIVE blocks (only block the worst offenders) ───
    print("\n" + "="*90)
    print("SELECTIVE BLOCKS: only block the very worst (GBPUSD LONG 13%WR), relax entries:")
    print("="*90)
    print(f"{'Label':<45} {'Tr':>4} {'Ret':>8} {'DD':>6} {'Str':>4} {'WR':>5} {'MinM':>5}")
    print("-"*80)

    # Only block GBPUSD LONG (13.3% WR) - clearly the worst
    minimal_blocks = original_blocks | {('GBPUSD', 'LONG')}

    selective_configs = [
        {'label': 'block GBPUSD_L only'},
        {'zone_penetration_pct': 0.45, 'label': '+zp=0.45'},
        {'zone_penetration_pct': 0.42, 'label': '+zp=0.42'},
        {'zone_penetration_pct': 0.45, 'max_correlated_trades': 3, 'label': '+zp=0.45+mct=3'},
        {'zone_penetration_pct': 0.42, 'max_correlated_trades': 3, 'label': '+zp=0.42+mct=3'},
        {'zone_penetration_pct': 0.45, 'cooldown_bars': 2, 'max_correlated_trades': 3, 'label': '+zp=0.45+cd=2+mct=3'},
    ]

    for cfg in selective_configs:
        label = cfg.pop('label')
        overrides = {'sideways_bars': 20, 'use_pair_direction_filter': False, **cfg}
        r = run_config(data, zone_cache, overrides, extra_blocks=minimal_blocks)
        if r:
            marker = " ***" if r['streak'] <= 5 and r['trades'] >= 180 and r['return'] > 500 else ""
            print(f"  {label:<43} {r['trades']:>4} {r['return']:>+7.1f}% {r['max_dd']:>5.1f}% {r['streak']:>4} {r['wr']:>4.1f}% {r['min_month']:>5}{marker}")

    # ─── Best results ───
    if good:
        print("\n" + "="*90)
        print("BEST: streak<=6, trades>=150, sorted by return:")
        print("="*90)
        good.sort(key=lambda x: -x['return'])
        for r in good[:10]:
            print(f"  {r['label']:<43} {r['trades']:>4} {r['return']:>+7.1f}% {r['max_dd']:>5.1f}% streak={r['streak']} WR={r['wr']:.1f}% minMonth={r['min_month']}")


if __name__ == '__main__':
    main()
