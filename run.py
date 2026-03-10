#!/usr/bin/env python3
"""FX support/resistance trading tool - CLI entry point."""

import argparse
import os
import sys

from fx_sr.config import (
    PAIRS,
    DEFAULT_ZONE_HISTORY_DAYS,
    DEFAULT_RR_RATIO,
    DEFAULT_SL_BUFFER_PCT,
    DEFAULT_EARLY_EXIT_R,
    DEFAULT_COOLDOWN_BARS,
    DEFAULT_MIN_ENTRY_CANDLE_BODY_PCT,
    DEFAULT_MOMENTUM_LOOKBACK,
    DEFAULT_MAX_CORRELATED_TRADES,
    DEFAULT_EXECUTION_SPREAD_PIPS,
    DEFAULT_STOP_SLIPPAGE_PIPS,
    DEFAULT_STRATEGY_PRESET,
    STRATEGY_PRESETS,
    STRATEGY_PRESET_DESCRIPTIONS,
)
from fx_sr.strategy import StrategyParams, DEFAULT_BLOCKED_HOURS, DEFAULT_BLOCKED_DAYS
from fx_sr.backtest import (
    run_all_backtests_parallel,
    format_results,
    calculate_compounding_pnl,
    format_compounding_results,
)
from fx_sr.live import scan_opportunities, format_signals, live_monitor, show_zones
from fx_sr import ibkr


def _configure_ibkr(args) -> int:
    """Apply optional CLI IBKR connection overrides."""
    client_id = getattr(args, 'ibkr_client_id', None)
    if client_id is not None:
        ibkr.configure_connection(client_id=client_id)
    return ibkr.TWS_CLIENT_ID


def _add_ibkr_args(parser):
    parser.add_argument(
        '--ibkr-client-id',
        type=int,
        default=None,
        help='Override IBKR/TWS client ID (default: env IBKR_CLIENT_ID or 60)',
    )


def _resolve_pairs(pair_arg: str | None) -> dict:
    if not pair_arg:
        return PAIRS

    key = pair_arg.upper().replace('/', '')
    if key not in PAIRS:
        print(f'  Unknown pair: {pair_arg}')
        print(f"  Available: {', '.join(PAIRS.keys())}")
        sys.exit(1)
    return {key: PAIRS[key]}


def _format_param_summary(params: StrategyParams) -> str:
    return (
        f"rr={params.rr_ratio}, sl={params.sl_buffer_pct}, early={params.early_exit_r}, "
        f"cooldown={params.cooldown_bars}, body={params.min_entry_candle_body_pct}, "
        f"momentum={params.momentum_lookback}, corr={params.max_correlated_trades}, "
        f"spread={params.spread_pips}, stop_slip={params.stop_slippage_pips}"
    )


def _format_preset_label(preset_name: str) -> str:
    return f"{preset_name} ({STRATEGY_PRESET_DESCRIPTIONS[preset_name]})"


def _build_strategy_params(args) -> StrategyParams:
    preset = STRATEGY_PRESETS[args.preset]
    blocked_hours = (
        frozenset(args.blocked_hours)
        if getattr(args, 'blocked_hours', None) is not None
        else DEFAULT_BLOCKED_HOURS
    )
    blocked_days = (
        frozenset(args.blocked_days)
        if getattr(args, 'blocked_days', None) is not None
        else DEFAULT_BLOCKED_DAYS
    )

    return StrategyParams(
        rr_ratio=args.rr_ratio if args.rr_ratio is not None else preset['rr_ratio'],
        sl_buffer_pct=args.sl_buffer if args.sl_buffer is not None else preset['sl_buffer_pct'],
        early_exit_r=args.early_exit if args.early_exit is not None else preset['early_exit_r'],
        cooldown_bars=args.cooldown_bars if args.cooldown_bars is not None else preset['cooldown_bars'],
        min_entry_candle_body_pct=(
            args.min_entry_body
            if args.min_entry_body is not None
            else preset['min_entry_candle_body_pct']
        ),
        momentum_lookback=(
            args.momentum_lookback
            if args.momentum_lookback is not None
            else preset['momentum_lookback']
        ),
        max_correlated_trades=(
            args.max_correlated_trades
            if args.max_correlated_trades is not None
            else preset['max_correlated_trades']
        ),
        spread_pips=(
            args.spread_pips
            if args.spread_pips is not None
            else DEFAULT_EXECUTION_SPREAD_PIPS
        ),
        stop_slippage_pips=(
            args.stop_slippage_pips
            if args.stop_slippage_pips is not None
            else DEFAULT_STOP_SLIPPAGE_PIPS
        ),
        use_time_filters=not args.no_time_filters,
        use_pair_direction_filter=not args.no_pair_direction_filter,
        blocked_hours=blocked_hours,
        blocked_days=blocked_days,
    )


def _add_strategy_args(parser):
    parser.add_argument(
        '--preset',
        choices=tuple(STRATEGY_PRESETS.keys()),
        default=DEFAULT_STRATEGY_PRESET,
        help=f'Named strategy preset (default: {DEFAULT_STRATEGY_PRESET})',
    )
    parser.add_argument('--rr-ratio', type=float, default=None,
                        help=f'Override preset risk:reward ratio (balanced default: {DEFAULT_RR_RATIO})')
    parser.add_argument('--sl-buffer', type=float, default=None,
                        help=f'Override preset SL buffer %% beyond zone (balanced default: {DEFAULT_SL_BUFFER_PCT})')
    parser.add_argument('--early-exit', type=float, default=None,
                        help=f'Override preset early-exit R threshold (balanced default: {DEFAULT_EARLY_EXIT_R})')
    parser.add_argument('--cooldown-bars', type=int, default=None,
                        help=f'Override preset bars between entries (balanced default: {DEFAULT_COOLDOWN_BARS})')
    parser.add_argument('--min-entry-body', type=float, default=None,
                        help=(
                            'Override preset minimum entry candle body/range ratio '
                            f'(balanced default: {DEFAULT_MIN_ENTRY_CANDLE_BODY_PCT})'
                        ))
    parser.add_argument('--momentum-lookback', type=int, default=None,
                        help=f'Override preset momentum lookback (balanced default: {DEFAULT_MOMENTUM_LOOKBACK})')
    parser.add_argument('--max-correlated-trades', type=int, default=None,
                        help=(
                            'Override preset correlated-trade cap '
                            f'(balanced default: {DEFAULT_MAX_CORRELATED_TRADES})'
                        ))
    parser.add_argument('--spread-pips', type=float, default=None,
                        help=(
                            'Override explicit midpoint spread assumption in pips ' 
                            f'(default: {DEFAULT_EXECUTION_SPREAD_PIPS})'
                        ))
    parser.add_argument('--stop-slippage-pips', type=float, default=None,
                        help=(
                            'Override adverse stop slippage assumption in pips ' 
                            f'(default: {DEFAULT_STOP_SLIPPAGE_PIPS})'
                        ))
    parser.add_argument('--no-time-filters', action='store_true',
                        help='Disable blocked hours/days entry filters')
    parser.add_argument('--no-pair-direction-filter', action='store_true',
                        help='Disable historically weak pair-direction blocks')
    parser.add_argument('--blocked-hours', type=int, nargs='*', default=None, metavar='HOUR',
                        help=(
                            'Override blocked UTC entry hours. '
                            f'Default: {sorted(DEFAULT_BLOCKED_HOURS)}. '
                            'Pass no values to clear the block list.'
                        ))
    parser.add_argument('--blocked-days', type=int, nargs='*', default=None, metavar='DAY',
                        help=(
                            'Override blocked weekdays (Monday=0). '
                            f'Default: {sorted(DEFAULT_BLOCKED_DAYS)}. '
                            'Pass no values to clear the block list.'
                        ))


def cmd_backtest(args):
    """Run backtesting mode."""
    active_client_id = _configure_ibkr(args)
    params = _build_strategy_params(args)
    pairs = _resolve_pairs(args.pair)
    zone_days = args.zone_history

    print(f"\n  IBKR client ID: {active_client_id}")
    print(f"  Strategy preset: {_format_preset_label(args.preset)}")
    print(f"  Active params: {_format_param_summary(params)}")
    print(
        f"  Backtest: {len(pairs)} pair(s), {args.days} days hourly, "
        f"{zone_days} days daily zones"
    )

    import time
    t0 = time.time()

    results = run_all_backtests_parallel(
        params=params,
        hourly_days=args.days,
        zone_history_days=zone_days,
        pairs=pairs,
        force_refresh=args.no_cache,
        base_client_id=active_client_id,
    )

    elapsed = time.time() - t0

    if not results:
        print('\n  No data available for any pair. Exiting.')
        sys.exit(1)

    print(f'\n  Completed in {elapsed:.1f}s')
    print(f'\n{format_results(results)}')

    if args.balance:
        risk_pct = args.risk_pct / 100.0
        total_pre = sum(r.total_trades for r in results.values())
        trade_log, final_bal = calculate_compounding_pnl(
            results,
            starting_balance=args.balance,
            risk_pct=risk_pct,
            params=params,
        )
        print(f'\n{format_compounding_results(trade_log, args.balance, final_bal, total_pre)}')

    if args.pair and args.verbose:
        key = args.pair.upper().replace('/', '')
        if key in results:
            r = results[key]
            pair_info = PAIRS[key]
            d = pair_info.get('decimals', 5)

            print(f'\n  Detailed trades for {key}:')
            print(
                f"  {'Entry Time':<22} {'Dir':>5} {'Entry':>12} {'Exit':>12} "
                f"{'P/L pips':>9} {'P/L R':>7} {'Reason':>10}"
            )
            print('  ' + '-' * 90)
            for t in r.trades:
                print(
                    f"  {str(t.entry_time):<22} {t.direction:>5} "
                    f"{t.entry_price:>{12}.{d}f} {t.exit_price:>{12}.{d}f} "
                    f"{t.pnl_pips:>9.1f} {t.pnl_r:>7.2f} {t.exit_reason:>10}"
                )

            print(f'\n  S/R Zones (final snapshot):')
            for zone in r.zones:
                print(
                    f"    [{zone.lower:.{d}f} - {zone.upper:.{d}f}]  "
                    f"{zone.zone_type:<12} {zone.strength:<8} "
                    f"({zone.touches} touches)"
                )


def cmd_download(args):
    """Download and cache price data to SQLite."""
    from fx_sr.data import download_all_data
    from fx_sr.db import get_cache_summary, get_db_path

    active_client_id = _configure_ibkr(args)
    pairs = _resolve_pairs(args.pair)

    import time
    t0 = time.time()

    print(f'\n  Database: {get_db_path()}')
    print(f'  IBKR client ID: {active_client_id}')
    download_all_data(pairs, hourly_days=args.days, daily_days=args.days)

    elapsed = time.time() - t0
    print(f'\n  Download completed in {elapsed:.1f}s')

    summary = get_cache_summary()
    if not summary.empty:
        print(f'\n  Cache contents:')
        print(f"  {'Ticker':<12} {'Interval':<10} {'From':<28} {'To':<28} {'Bars':>7}")
        print('  ' + '-' * 90)
        for _, row in summary.iterrows():
            print(
                f"  {row['ticker']:<12} {row['interval']:<10} "
                f"{row['first_ts']:<28} {row['last_ts']:<28} {row['bars']:>7}"
            )


def cmd_viz(args):
    """Export backtest data and open interactive chart via local HTTP server."""
    active_client_id = _configure_ibkr(args)
    import http.server
    import threading
    import webbrowser

    serve_dir = os.path.dirname(os.path.abspath(__file__))
    viz_path = os.path.join(serve_dir, 'viz_data.json')

    if args.refresh or not os.path.exists(viz_path):
        from export_viz import export_backtest_data
        print(f'\n  Generating visualization data ({args.days} days)...')
        print(f'  IBKR client ID: {active_client_id}')
        export_backtest_data(hourly_days=args.days)
    else:
        import time
        age_hrs = (time.time() - os.path.getmtime(viz_path)) / 3600
        print(f'\n  Using existing viz_data.json ({age_hrs:.1f}h old, use --refresh to regenerate)')

    port = args.port
    handler = lambda *a, **kw: http.server.SimpleHTTPRequestHandler(*a, directory=serve_dir, **kw)

    server = http.server.HTTPServer(('127.0.0.1', port), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    url = f'http://localhost:{port}/chart.html'
    print(f'  Serving at {url}  (Ctrl+C to stop)')
    webbrowser.open(url)

    try:
        thread.join()
    except KeyboardInterrupt:
        print('\n  Server stopped.')
        server.shutdown()


def cmd_live(args):
    """Run live monitoring mode."""
    active_client_id = _configure_ibkr(args)
    params = _build_strategy_params(args)
    pairs = _resolve_pairs(args.pair)
    zone_days = args.zone_history

    if args.zones:
        for pair_id, pair_info in pairs.items():
            print(show_zones(pair_id, pair_info, zone_history_days=zone_days))
        return

    print(f"\n  IBKR client ID: {active_client_id}")
    print(f"  Strategy preset: {_format_preset_label(args.preset)}")
    print(f"  Active params: {_format_param_summary(params)}")

    if args.once:
        print(f'  Scanning {len(pairs)} pairs for opportunities...')
        tracked = {}
        if not args.no_positions:
            from fx_sr.positions import sync_positions
            tracked = sync_positions(params, zone_days)
        signals = scan_opportunities(
            pairs,
            params,
            zone_history_days=zone_days,
            tracked_positions=tracked,
        )
        print(format_signals(signals))
        return

    live_monitor(
        pairs,
        params,
        interval=args.interval,
        zone_history_days=zone_days,
        track_positions=not args.no_positions,
    )


def main():
    preset_lines = '\n'.join(
        (
            f"  {name:<10} {STRATEGY_PRESET_DESCRIPTIONS[name]} "
            f"(rr={STRATEGY_PRESETS[name]['rr_ratio']}, sl={STRATEGY_PRESETS[name]['sl_buffer_pct']}, "
            f"early={STRATEGY_PRESETS[name]['early_exit_r']}, corr={STRATEGY_PRESETS[name]['max_correlated_trades']})"
        )
        for name in STRATEGY_PRESETS
    )
    epilog = (
        'Examples:\n'
        '  python run.py download\n'
        '  python run.py download --days 365 --pair EURUSD\n'
        '  python run.py backtest --days 365 --balance 10000 --risk-pct 5\n'
        '  python run.py backtest --preset source\n'
        '  python run.py backtest --preset aggressive\n'
        '  python run.py backtest --preset source --rr-ratio 1.2\n'
        '  python run.py live --preset aggressive --once\n\n'
        'Named presets:\n'
        f'{preset_lines}'
    )

    parser = argparse.ArgumentParser(
        description='FX S/R zone trading tool (daily zones + hourly execution)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
    )

    subparsers = parser.add_subparsers(dest='command', help='Mode')

    dl = subparsers.add_parser('download', help='Download and cache price data from IBKR')
    dl.add_argument('--pair', type=str, help='Specific pair (e.g., EURUSD). Default: all 10')
    dl.add_argument('--days', type=int, default=730,
                    help='Days of data to download (default: 730, max for hourly)')
    _add_ibkr_args(dl)

    bt = subparsers.add_parser('backtest', help='Backtest using daily zones + hourly execution')
    bt.add_argument('--pair', type=str, help='Specific pair (e.g., EURUSD). Default: all 10')
    bt.add_argument('--days', type=int, default=30,
                    help='Days of hourly data for execution (default: 30)')
    bt.add_argument('--zone-history', type=int, default=DEFAULT_ZONE_HISTORY_DAYS,
                    help=f'Days of daily data for zone detection (default: {DEFAULT_ZONE_HISTORY_DAYS})')
    _add_ibkr_args(bt)
    _add_strategy_args(bt)
    bt.add_argument('--balance', type=float, default=None,
                    help='Starting balance for compounding P&L (for example 10000)')
    bt.add_argument('--risk-pct', type=float, default=5.0,
                    help='Risk per trade as %% of balance (default: 5)')
    bt.add_argument('--no-cache', action='store_true',
                    help='Bypass SQLite cache and refresh directly from IBKR')
    bt.add_argument('-v', '--verbose', action='store_true', help='Show individual trade details')

    lv = subparsers.add_parser('live', help='Monitor live data for zone opportunities')
    lv.add_argument('--pair', type=str, help='Specific pair to monitor')
    lv.add_argument('--interval', type=int, default=60,
                    help='Scan interval in seconds (default: 60)')
    lv.add_argument('--zone-history', type=int, default=DEFAULT_ZONE_HISTORY_DAYS,
                    help=f'Days of daily data for zones (default: {DEFAULT_ZONE_HISTORY_DAYS})')
    _add_ibkr_args(lv)
    _add_strategy_args(lv)
    lv.add_argument('--once', action='store_true', help='Single scan then exit')
    lv.add_argument('--zones', action='store_true', help='Show current S/R zones and exit')
    lv.add_argument('--no-positions', action='store_true',
                    help='Disable IBKR position tracking and duplicate-position filtering')

    vz = subparsers.add_parser('viz', help='Export backtest data and open interactive chart')
    vz.add_argument('--days', type=int, default=365,
                    help='Days of hourly data for backtest (default: 365)')
    _add_ibkr_args(vz)
    vz.add_argument('--port', type=int, default=8080, help='Local server port (default: 8080)')
    vz.add_argument('--refresh', action='store_true',
                    help='Force regenerate viz_data.json (default: reuse if exists)')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == 'download':
        cmd_download(args)
    elif args.command == 'backtest':
        cmd_backtest(args)
    elif args.command == 'live':
        cmd_live(args)
    elif args.command == 'viz':
        cmd_viz(args)


if __name__ == '__main__':
    main()


