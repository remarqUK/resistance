"""Parameter sweep runner — uses profiles.py as the baseline."""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fx_sr.profiles import PAIRS, get_profile, DEFAULT_PROFILE
from fx_sr.data import fetch_daily_data, fetch_hourly_data
from fx_sr.strategy import StrategyParams, params_from_profile
from fx_sr.backtest import run_backtest_fast, precompute_zone_cache_parallel, calculate_compounding_pnl

_PROFILE = get_profile()
HOURLY_DAYS = _PROFILE['hourly_days']
ZONE_HISTORY_DAYS = _PROFILE['zone_history_days']
STARTING_BALANCE = _PROFILE['starting_balance']
RISK_PCT = _PROFILE['risk_pct'] / 100.0

PARAMS = {
    'rr_ratio': [1.0, 1.2, 1.5, 2.0],
    'early_exit_r': [0.3, 0.4, 0.5, 0.6],
    'sl_buffer_pct': [0.10, 0.15, 0.20, 0.25],
    'cooldown_bars': [1, 2, 3, 5, 8],
    'min_entry_candle_body_pct': [0.10, 0.15, 0.20, 0.30, 0.40],
    'momentum_lookback': [1, 2, 3, 4, 5],
    'max_correlated_trades': [2, 3, 4, 5],
}

COMBOS = [
    ('Default (tuned baseline)', {}),
    ('Source-like 1:1 profile', {'rr_ratio': 1.0, 'early_exit_r': 0.4, 'max_correlated_trades': 3}),
    ('Balanced alt: rr1.2 + early0.5 + corr4', {'rr_ratio': 1.2, 'early_exit_r': 0.5, 'max_correlated_trades': 4}),
    ('Higher return: + sl0.10 + body0.10', {
        'rr_ratio': 1.2,
        'early_exit_r': 0.5,
        'sl_buffer_pct': 0.10,
        'min_entry_candle_body_pct': 0.10,
        'max_correlated_trades': 4,
    }),
    ('Legacy rr1.5 + sl0.10 + early0.3', {
        'rr_ratio': 1.5,
        'sl_buffer_pct': 0.10,
        'early_exit_r': 0.3,
    }),
]


def run_sweep_iteration(data, zone_cache, overrides):
    """Run all pair backtests with given param overrides using cached zones."""
    params = params_from_profile(_PROFILE, **overrides)

    results = {}
    for pair, (daily_df, hourly_df) in data.items():
        if daily_df.empty or hourly_df.empty:
            continue
        pip = PAIRS[pair].get('pip', 0.0001)
        results[pair] = run_backtest_fast(hourly_df, pair, params, zone_cache, pip)

    if not results:
        return None

    total_trades = sum(r.total_trades for r in results.values())
    trade_log, final_balance = calculate_compounding_pnl(
        results,
        starting_balance=STARTING_BALANCE,
        risk_pct=RISK_PCT,
        params=params,
    )

    wins = sum(1 for _, trade, _, _, _ in trade_log if trade.pnl_r > 0)
    win_rate = wins / len(trade_log) * 100 if trade_log else 0

    peak = STARTING_BALANCE
    max_dd = 0.0
    for _, _, _, _, balance in trade_log:
        if balance > peak:
            peak = balance
        dd = (peak - balance) / peak * 100 if peak else 0.0
        if dd > max_dd:
            max_dd = dd

    return {
        'trades': len(trade_log),
        'trades_pre': total_trades,
        'wr': win_rate,
        'final': final_balance,
        'ret': (final_balance - STARTING_BALANCE) / STARTING_BALANCE * 100,
        'max_dd': max_dd,
    }


if __name__ == '__main__':
    print(f'  Profile: {DEFAULT_PROFILE}')
    print('  Loading data from SQLite cache...')
    t0 = time.time()
    data = {}
    for pair, info in PAIRS.items():
        daily_df = fetch_daily_data(info['ticker'], days=ZONE_HISTORY_DAYS + HOURLY_DAYS, allow_stale_cache=True)
        hourly_df = fetch_hourly_data(info['ticker'], days=HOURLY_DAYS, allow_stale_cache=True)
        data[pair] = (daily_df, hourly_df)
        print(f'    {pair}: {len(daily_df)} daily, {len(hourly_df)} hourly')
    print(f'  Data loaded in {time.time() - t0:.1f}s')

    print('\n  Pre-computing zones for all pairs and dates...')
    t1 = time.time()
    zone_cache = precompute_zone_cache_parallel(data, zone_history_days=ZONE_HISTORY_DAYS)
    print(f'  Zone cache built: {len(zone_cache)} entries in {time.time() - t1:.1f}s\n')

    print('=' * 100)
    print('  PARAMETER SWEEP - one parameter at a time, others at tuned baseline')
    print('=' * 100)

    for param_name, values in PARAMS.items():
        print(f'\n  --- {param_name} ---')
        print(f"  {'Value':>8} {'Trades':>7} {'WR%':>7} {'Return%':>9} {'Final':>14} {'MaxDD%':>8}")
        print(f"  {'-' * 66}")

        for value in values:
            ts = time.time()
            result = run_sweep_iteration(data, zone_cache, {param_name: value})
            elapsed = time.time() - ts
            if result:
                marker = ' <-- current' if value == _PROFILE.get(param_name) else ''
                print(
                    f"  {value:>8} {result['trades']:>7} {result['wr']:>6.1f}% "
                    f"{result['ret']:>+8.1f}%  GBP {result['final']:>10,.0f} "
                    f"{result['max_dd']:>7.1f}%{marker}  ({elapsed:.1f}s)"
                )

    print(f"\n  {'=' * 100}")
    print('  COMBO TESTS')
    print(f"  {'=' * 100}")
    print(f"  {'Config':<42} {'Trades':>7} {'WR%':>7} {'Return%':>9} {'Final':>14} {'MaxDD%':>8}")
    print(f"  {'-' * 92}")

    for label, overrides in COMBOS:
        ts = time.time()
        result = run_sweep_iteration(data, zone_cache, overrides)
        elapsed = time.time() - ts
        if result:
            print(
                f"  {label:<42} {result['trades']:>7} {result['wr']:>6.1f}% "
                f"{result['ret']:>+8.1f}%  GBP {result['final']:>10,.0f} "
                f"{result['max_dd']:>7.1f}%  ({elapsed:.1f}s)"
            )

    print(f'\n  Total sweep time: {time.time() - t0:.0f}s')
