"""Export backtest results to JSON for the interactive chart viewer."""

import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fx_sr.config import PAIRS, DEFAULT_ZONE_HISTORY_DAYS
from fx_sr.data import fetch_daily_data, fetch_hourly_data
from fx_sr.backtest import run_backtest
from fx_sr.strategy import StrategyParams, BLOCKED_PAIR_DIRECTIONS


def export_backtest_data(hourly_days=365, zone_history_days=DEFAULT_ZONE_HISTORY_DAYS):
    """Run backtests and export all data to viz_data.json."""
    params = StrategyParams()
    output = {}

    for pair, info in PAIRS.items():
        print(f"  Processing {pair}...")
        daily_df = fetch_daily_data(info['ticker'], days=zone_history_days + hourly_days)
        hourly_df = fetch_hourly_data(info['ticker'], days=hourly_days)

        if daily_df.empty or hourly_df.empty:
            print(f"    Skipped (no data)")
            continue

        result = run_backtest(daily_df, hourly_df, pair, params, zone_history_days)

        # OHLC as list of [timestamp_s, open, high, low, close]
        ohlc = []
        for ts, row in hourly_df.iterrows():
            ohlc.append({
                'time': int(ts.timestamp()),
                'open': round(float(row['Open']), info['decimals']),
                'high': round(float(row['High']), info['decimals']),
                'low': round(float(row['Low']), info['decimals']),
                'close': round(float(row['Close']), info['decimals']),
            })

        # Trades
        trades = []
        for t in result.trades:
            trades.append({
                'entry_time': int(t.entry_time.timestamp()),
                'exit_time': int(t.exit_time.timestamp()) if t.exit_time else None,
                'entry_price': round(float(t.entry_price), info['decimals']),
                'exit_price': round(float(t.exit_price), info['decimals']) if t.exit_price else None,
                'direction': t.direction,
                'sl_price': round(float(t.sl_price), info['decimals']),
                'tp_price': round(float(t.tp_price), info['decimals']),
                'zone_upper': round(float(t.zone_upper), info['decimals']),
                'zone_lower': round(float(t.zone_lower), info['decimals']),
                'exit_reason': t.exit_reason,
                'pnl_r': round(float(t.pnl_r), 2),
                'pnl_pips': round(float(t.pnl_pips), 1),
                'bars_held': t.bars_held,
            })

        # Zones
        zones = []
        for z in result.zones:
            zones.append({
                'upper': round(float(z.upper), info['decimals']),
                'lower': round(float(z.lower), info['decimals']),
                'type': z.zone_type,
                'touches': z.touches,
                'strength': z.strength,
            })

        # Determine which directions are blocked for this pair
        blocked_dirs = [d for (p, d) in BLOCKED_PAIR_DIRECTIONS if p == pair]

        output[pair] = {
            'name': info['name'],
            'decimals': info['decimals'],
            'pip': info['pip'],
            'blocked_directions': blocked_dirs,
            'ohlc': ohlc,
            'trades': trades,
            'zones': zones,
            'stats': {
                'total_trades': result.total_trades,
                'winning_trades': result.winning_trades,
                'losing_trades': result.losing_trades,
                'win_rate': round(result.win_rate, 1),
                'total_pnl_pips': round(result.total_pnl_pips, 1),
                'profit_factor': round(result.profit_factor, 2),
                'avg_win_r': round(result.avg_win_r, 2),
                'avg_loss_r': round(result.avg_loss_r, 2),
            },
        }

        print(f"    {result.total_trades} trades, {len(ohlc)} candles, {len(zones)} zones")

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'viz_data.json')
    with open(out_path, 'w') as f:
        json.dump(output, f)
    size_mb = os.path.getsize(out_path) / (1024 * 1024)
    print(f"\n  Exported to {out_path} ({size_mb:.1f} MB)")
    return out_path


if __name__ == '__main__':
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 365
    print(f"  Exporting {days}-day backtest data...")
    export_backtest_data(hourly_days=days)
