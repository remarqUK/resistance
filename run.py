#!/usr/bin/env python3
"""FX support/resistance trading tool - CLI entry point."""

import argparse
from dataclasses import replace
import os
import threading
import uuid
import sys
import time

# Unbuffer stdout so progress output appears immediately
sys.stdout.reconfigure(line_buffering=True)

from fx_sr import ibkr
from fx_sr.backtest import (
    apply_correlation_filter,
    build_backtest_run_config_json,
    calculate_compounding_pnl,
    calculate_execution_aware_compounding_pnl,
    format_compounding_results,
    format_results,
    _params_signature,
    run_all_backtests_parallel,
)
from fx_sr.config import (
    DEFAULT_COOLDOWN_BARS,
    DEFAULT_EARLY_EXIT_R,
    DEFAULT_EXECUTION_SPREAD_PIPS,
    DEFAULT_MAX_CORRELATED_TRADES,
    DEFAULT_MIN_ENTRY_CANDLE_BODY_PCT,
    DEFAULT_MOMENTUM_LOOKBACK,
    DEFAULT_RR_RATIO,
    DEFAULT_SL_BUFFER_PCT,
    DEFAULT_STOP_SLIPPAGE_PIPS,
    DEFAULT_STRATEGY_PRESET,
    DEFAULT_ZONE_HISTORY_DAYS,
    PAIRS,
    STRATEGY_PRESET_DESCRIPTIONS,
    STRATEGY_PRESETS,
)
from fx_sr.live import (
    build_live_size_plans,
    format_signals_with_sizes,
    load_portfolio_state,
    scan_opportunities,
    show_zones,
)
from fx_sr.backtest_baseline import (
    build_backtest_baseline_artifact,
    compare_backtest_baseline_artifacts,
    format_backtest_baseline_comparison,
    load_backtest_baseline_artifact,
    save_backtest_baseline_artifact,
)
from fx_sr.live_history import record_detected_signals, record_execution_results
from fx_sr.profiles import PROFILES, DEFAULT_PROFILE, get_profile
from fx_sr.strategy import (
    DEFAULT_BLOCKED_DAYS,
    DEFAULT_BLOCKED_HOURS,
    StrategyParams,
    params_from_profile,
)
from fx_sr import fill_pipeline


def _configure_ibkr(args, *, allow_client_id_fallback: bool = True) -> int:
    """Apply optional CLI IBKR connection overrides."""

    client_id = getattr(args, 'ibkr_client_id', None)
    if client_id is not None:
        ibkr.configure_connection(client_id=client_id)
    previous = ibkr.set_client_id_fallback(allow_client_id_fallback)
    if previous != allow_client_id_fallback:
        ibkr.disconnect()
    hist_workers = getattr(args, 'ib_historical_fetch_concurrency', None)
    if hist_workers is not None:
        ibkr.set_historical_fetch_concurrency(hist_workers)
    return ibkr.TWS_CLIENT_ID


def _fill_client_id_base(active_client_id: int) -> int:
    """Return a dedicated client-id base for CLI cache fills."""

    return int(active_client_id) + 2000


def _requested_profile_name(args) -> str | None:
    """Return the requested profile/preset name from argparse or tests."""

    return getattr(args, 'profile', None) or getattr(args, 'preset', None)


def _normalize_execution_mode(mode: str | None, *, context: str) -> str:
    """Force execution mode to intrabar; fail fast for anything else."""

    resolved = mode or 'intrabar'
    if resolved != 'intrabar':
        raise SystemExit(
            f"  Unsupported execution mode for {context}: {resolved!r}. "
            "Only 'intrabar' is supported."
        )
    return 'intrabar'


def _build_backtest_run_config(
    params: StrategyParams,
    *,
    hourly_days: int,
    zone_history_days: int,
    execution_mode: str = 'intrabar',
    requested_profile: str | None = None,
    starting_balance: float | None = None,
    risk_pct: float | None = None,
    selection_label: str | None = None,
) -> str:
    """Build a stable backtest cache signature, omitting default execution mode."""

    run_config_kwargs = {
        'requested_profile': requested_profile,
        'starting_balance': starting_balance,
        'risk_pct': risk_pct,
        'selection_label': selection_label,
    }
    if execution_mode != 'intrabar':
        run_config_kwargs['execution_mode'] = execution_mode

    return build_backtest_run_config_json(
        params,
        hourly_days=hourly_days,
        zone_history_days=zone_history_days,
        **run_config_kwargs,
    )


def _add_ibkr_args(parser):
    parser.add_argument(
        '--ibkr-client-id',
        type=int,
        default=None,
        help='Override IBKR/TWS client ID (default: env IBKR_CLIENT_ID or 60)',
    )


def _add_download_args(parser):
    parser.add_argument(
        '--pair',
        type=str,
        help='Specific pair (e.g., EURUSD). Default: all configured pairs',
    )
    parser.add_argument(
        '--days',
        type=int,
        default=730,
        help='Days of data to download (default: 730, max for hourly)',
    )
    parser.add_argument(
        '--minute-days',
        type=int,
        default=0,
        help='Backfill 1-minute bars for this many days in 7-day chunks (default: 0)',
    )
    parser.add_argument(
        '--minute-only',
        action='store_true',
        help='Skip daily/hourly downloads and only backfill 1-minute bars',
    )
    parser.add_argument(
        '--sync-workers',
        type=int,
        default=5,
        help='Concurrent ticker workers for IBKR sync (1-5, default: 5)',
    )
    parser.add_argument(
        '--refresh-all',
        action='store_true',
        help='Disable resume mode and force full-range re-download for every interval requested',
    )
    _add_ibkr_args(parser)


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
    desc = PROFILES.get(preset_name, {}).get('description', STRATEGY_PRESET_DESCRIPTIONS.get(preset_name, ''))
    return f"{preset_name} ({desc})"


def _portfolio_summary(
    results: dict[str, object],
    params: StrategyParams,
    *,
    starting_balance: float | None = None,
    risk_pct: float | None = None,
) -> dict[str, float | int]:
    """Return aggregate stats using the execution-aware portfolio benchmark."""
    raw_total_trades = 0
    raw_total_wins = 0
    raw_total_pnl = 0.0
    all_trades = []

    for pair, result in results.items():
        raw_total_trades += result.total_trades
        raw_total_wins += result.winning_trades
        raw_total_pnl += result.total_pnl_pips
        for trade in result.trades:
            all_trades.append((pair, trade))

    raw_win_rate = (raw_total_wins / raw_total_trades * 100) if raw_total_trades > 0 else 0.0

    if starting_balance is None or risk_pct is None:
        all_trades.sort(key=lambda item: item[1].entry_time)
        filtered_trades = apply_correlation_filter(all_trades, params=params)
        total_trades = len(filtered_trades)
        total_wins = sum(1 for _, trade in filtered_trades if trade.pnl_pips > 0)
        total_pnl = sum(trade.pnl_pips for _, trade in filtered_trades)
        win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0.0
        skip_counts: dict[str, int] = {}
    else:
        simulation = calculate_execution_aware_compounding_pnl(
            results,
            starting_balance=float(starting_balance),
            risk_pct=float(risk_pct),
            params=params,
        )
        total_trades = simulation.total_trades
        total_wins = simulation.total_wins
        total_pnl = simulation.total_pnl
        win_rate = simulation.win_rate
        skip_counts = dict(simulation.skip_counts)

    return {
        'total_trades': total_trades,
        'total_wins': total_wins,
        'total_pnl': total_pnl,
        'win_rate': win_rate,
        'raw_total_trades': raw_total_trades,
        'raw_total_wins': raw_total_wins,
        'raw_total_pnl': raw_total_pnl,
        'raw_win_rate': raw_win_rate,
        'skip_counts': skip_counts,
    }


def _build_target_trade_profile_attempts(base_params: StrategyParams) -> list[tuple[str, StrategyParams]]:
    """Build progressively relaxed parameter sets to reach a trade-count target."""
    attempts: list[tuple[str, StrategyParams]] = []
    seen: set[str] = set()

    def add(label: str, params: StrategyParams):
        sig = _params_signature(params)
        if sig in seen:
            return
        seen.add(sig)
        attempts.append((label, params))

    current = replace(base_params)
    add('baseline', current)

    # Aggressively open up entries first, then filters and exposure caps.
    if current.min_entry_candle_body_pct > 0.0:
        current = replace(current, min_entry_candle_body_pct=0.0)
        add('min_entry_body=0.0', current)

    if current.min_zone_touches > 2:
        current = replace(current, min_zone_touches=2)
        add('min_zone_touches=2', current)

    if current.momentum_threshold > 0.0:
        current = replace(current, momentum_threshold=0.0)
        add('momentum_filter=off', current)

    if current.momentum_lookback > 1:
        current = replace(current, momentum_lookback=1)
        add('momentum_lookback=1', current)

    if current.zone_penetration_pct > 0.25:
        current = replace(current, zone_penetration_pct=0.25)
        add('zone_penetration=25%', current)

    if current.zone_penetration_pct > 0.10:
        current = replace(current, zone_penetration_pct=0.10)
        add('zone_penetration=10%', current)

    if current.cooldown_bars > 0:
        current = replace(current, cooldown_bars=0)
        add('cooldown=0', current)

    for corr in range(max(1, current.max_correlated_trades), 12 + 1):
        if corr == current.max_correlated_trades:
            continue
        current = replace(current, max_correlated_trades=corr)
        add(f'max_correlated_trades={corr}', current)

    if current.use_time_filters:
        current = replace(current, use_time_filters=False)
        add('time_filters=off', current)

    if current.use_pair_direction_filter:
        current = replace(current, use_pair_direction_filter=False)
        add('pair_direction_filter=off', current)

    if current.min_zone_touches > 1:
        current = replace(current, min_zone_touches=1)
        add('min_zone_touches=1', current)

    if current.max_correlated_trades < len(PAIRS):
        current = replace(current, max_correlated_trades=len(PAIRS))
        add(f'max_correlated_trades={len(PAIRS)}', current)

    return attempts


def _run_backtests_until_target(
    params: StrategyParams,
    target_trades: int,
    args,
    pairs: dict,
    zone_days: int,
    active_client_id: int,
    hourly_days: int,
    run_id: str | None = None,
    execution_mode: str = 'intrabar',
    fetch_workers: int | None = None,
    backtest_workers: int | None = None,
    allow_stale_cache: bool = False,
) -> tuple[
    dict[str, object],
    StrategyParams,
    list[dict[str, float | int | str]],
    dict[str, float | int],
    str,
]:
    """Run successive relaxations until target trades are reached."""
    target_profit_floor = float(getattr(args, 'target_profit_floor', 1.0))
    target_win_rate_floor = float(getattr(args, 'target_win_rate_floor', 1.0))
    starting_balance = getattr(args, 'balance', None)
    risk_pct = getattr(args, 'risk_pct', None)
    force_refresh = bool(getattr(args, 'no_cache', False))

    attempts = _build_target_trade_profile_attempts(params)
    print(f'\n  Target trade mode enabled: need >= {target_trades} total trades')
    print(
        f'  Profit floor: >= {target_profit_floor:.2f}x baseline, '
        f'Win-rate floor: >= {target_win_rate_floor:.2f}x baseline'
    )

    baseline_summary = None
    best_any: tuple[str, dict[str, object], StrategyParams, dict[str, float | int]] | None = None
    best_above_target: tuple[str, dict[str, object], StrategyParams, dict[str, float | int]] | None = None
    best_below_target: tuple[str, dict[str, object], StrategyParams, dict[str, float | int]] | None = None
    attempt_logs: list[dict[str, float | int | str]] = []

    for idx, (label, attempt_params) in enumerate(attempts, 1):
        attempt_run_id = f'{run_id}:{idx:02d}' if run_id else f'run-{idx:02d}'
        print(
            f'\n  Attempt {idx}/{len(attempts)}: {label}'
            f' | corr={attempt_params.max_correlated_trades}, body={attempt_params.min_entry_candle_body_pct}, '
            f'momentum={attempt_params.momentum_lookback}, time_filters={attempt_params.use_time_filters}, '
            f'pair_dir_filter={attempt_params.use_pair_direction_filter}'
        )
        print(f'    Active params: {_format_param_summary(attempt_params)}')

        t0 = time.time()
        run_config_json = _build_backtest_run_config(
            attempt_params,
            hourly_days=hourly_days,
            zone_history_days=zone_days,
            requested_profile=_requested_profile_name(args),
            execution_mode=execution_mode,
            starting_balance=starting_balance,
            risk_pct=risk_pct,
            selection_label=label,
        )
        results = run_all_backtests_parallel(
            params=attempt_params,
            hourly_days=hourly_days,
            zone_history_days=zone_days,
            pairs=pairs,
            run_id=attempt_run_id,
            force_refresh=force_refresh,
            base_client_id=active_client_id,
            run_config_json=run_config_json,
            debug=bool(getattr(args, 'backtest_debug', False)),
            execution_mode=execution_mode,
            fetch_workers=fetch_workers,
            backtest_workers=backtest_workers,
            allow_stale_cache=allow_stale_cache,
        )
        elapsed = time.time() - t0

        if not results:
            print(f'    Completed in {elapsed:.1f}s with no results.')
            continue

        summary = _portfolio_summary(
            results,
            attempt_params,
            starting_balance=starting_balance,
            risk_pct=risk_pct / 100.0 if risk_pct is not None else None,
        )
        summary_line = (
            f"execution_portfolio_trades={summary['total_trades']} "
            f"wr={summary['win_rate']:.1f}% "
            f"pnl={summary['total_pnl']:.1f} "
            f"(raw={summary['raw_total_trades']} trades, {summary['raw_total_pnl']:.1f} pips)"
        )
        attempt_logs.append({
            'label': label,
            'trades': summary['total_trades'],
            'win_rate': summary['win_rate'],
            'total_pnl': summary['total_pnl'],
            'raw_trades': summary['raw_total_trades'],
            'raw_total_pnl': summary['raw_total_pnl'],
            'elapsed_s': elapsed,
        })
        print(f'    Completed in {elapsed:.1f}s | {summary_line}')

        if idx == 1:
            baseline_summary = summary

        if baseline_summary is None:
            baseline_summary = summary

        min_pnl = baseline_summary['total_pnl'] * target_profit_floor
        min_wr = baseline_summary['win_rate'] * target_win_rate_floor

        qualifies = (
            summary['total_trades'] >= target_trades
            and summary['total_pnl'] >= min_pnl
            and summary['win_rate'] >= min_wr
        )

        best_any_candidate = (
            best_any is None
            or summary['total_trades'] > best_any[3]['total_trades']
            or (
                summary['total_trades'] == best_any[3]['total_trades']
                and summary['total_pnl'] > best_any[3]['total_pnl']
            )
        )
        if best_any_candidate:
            best_any = (label, results, attempt_params, summary)

        if qualifies:
            best_above_candidate = (
                best_above_target is None
                or summary['total_trades'] < best_above_target[3]['total_trades']
                or (
                    summary['total_trades'] == best_above_target[3]['total_trades']
                    and summary['total_pnl'] > best_above_target[3]['total_pnl']
                )
            )
            if best_above_candidate:
                best_above_target = (label, results, attempt_params, summary)
        elif (
            summary['total_pnl'] >= min_pnl
            and summary['win_rate'] >= min_wr
        ):
            best_below_candidate = (
                best_below_target is None
                or summary['total_trades'] > best_below_target[3]['total_trades']
                or (
                    summary['total_trades'] == best_below_target[3]['total_trades']
                    and summary['total_pnl'] > best_below_target[3]['total_pnl']
                )
            )
            if best_below_candidate:
                best_below_target = (label, results, attempt_params, summary)

    if best_above_target is not None:
        print(f'\n  Closest profitable profile meeting target: "{best_above_target[0]}".')
        return (
            best_above_target[1],
            best_above_target[2],
            attempt_logs,
            best_above_target[3],
            best_above_target[0],
        )

    if best_below_target is not None:
        print(
            '\n  Unable to meet target while holding the profitability floor.'
            f' Using highest-frequency profitable attempt: {best_below_target[0]}'
        )
        return (
            best_below_target[1],
            best_below_target[2],
            attempt_logs,
            best_below_target[3],
            best_below_target[0],
        )

    if best_any is not None:
        print(
            '\n  No attempt preserved the profitability floor.'
            f' Using best available attempt: {best_any[0]}'
        )
        return best_any[1], best_any[2], attempt_logs, best_any[3], best_any[0]

    return (
        {},
        params,
        attempt_logs,
        {
            'total_trades': 0,
            'total_wins': 0,
            'total_pnl': 0.0,
            'win_rate': 0.0,
            'raw_total_trades': 0,
            'raw_total_wins': 0,
            'raw_total_pnl': 0.0,
            'raw_win_rate': 0.0,
            'skip_counts': {},
        },
        'baseline',
    )


def _build_strategy_params(args) -> StrategyParams:
    # Use --profile if provided, otherwise fall back to --preset
    profile_name = _requested_profile_name(args)
    profile = get_profile(profile_name)

    # CLI overrides take precedence over profile values
    overrides = {}
    if args.rr_ratio is not None:
        overrides['rr_ratio'] = args.rr_ratio
    if args.sl_buffer is not None:
        overrides['sl_buffer_pct'] = args.sl_buffer
    if args.early_exit is not None:
        overrides['early_exit_r'] = args.early_exit
    if args.cooldown_bars is not None:
        overrides['cooldown_bars'] = args.cooldown_bars
    if args.min_entry_body is not None:
        overrides['min_entry_candle_body_pct'] = args.min_entry_body
    if args.momentum_lookback is not None:
        overrides['momentum_lookback'] = args.momentum_lookback
    if args.max_correlated_trades is not None:
        overrides['max_correlated_trades'] = args.max_correlated_trades
    if args.spread_pips is not None:
        overrides['spread_pips'] = args.spread_pips
    if args.stop_slippage_pips is not None:
        overrides['stop_slippage_pips'] = args.stop_slippage_pips
    if args.no_time_filters:
        overrides['use_time_filters'] = False
    if args.no_pair_direction_filter:
        overrides['use_pair_direction_filter'] = False
    if getattr(args, 'blocked_hours', None) is not None:
        overrides['blocked_hours'] = args.blocked_hours
    if getattr(args, 'blocked_days', None) is not None:
        overrides['blocked_days'] = args.blocked_days
    if getattr(args, 'max_sl_pct', None) is not None:
        overrides['max_sl_pct'] = args.max_sl_pct
    if getattr(args, 'no_margin', False):
        overrides['enforce_margin'] = False
    if getattr(args, 'no_l2', False):
        overrides['strict_backtest_execution'] = False
        overrides['prefer_l2_submit_quote'] = False

    return params_from_profile(profile, **overrides)


def _add_strategy_args(parser):
    parser.add_argument(
        '--profile',
        choices=tuple(PROFILES.keys()),
        default=None,
        help=f'Strategy profile from profiles.py (default: {DEFAULT_PROFILE}). Overrides --preset.',
    )
    parser.add_argument(
        '--preset',
        choices=tuple(STRATEGY_PRESETS.keys()),
        default=DEFAULT_STRATEGY_PRESET,
        help=f'Named strategy preset (default: {DEFAULT_STRATEGY_PRESET}). Use --profile instead.',
    )
    parser.add_argument(
        '--rr-ratio',
        type=float,
        default=None,
        help=f'Override preset risk:reward ratio (balanced default: {DEFAULT_RR_RATIO})',
    )
    parser.add_argument(
        '--sl-buffer',
        type=float,
        default=None,
        help=f'Override preset SL buffer %% beyond zone (balanced default: {DEFAULT_SL_BUFFER_PCT})',
    )
    parser.add_argument(
        '--early-exit',
        type=float,
        default=None,
        help=f'Override preset early-exit R threshold (balanced default: {DEFAULT_EARLY_EXIT_R})',
    )
    parser.add_argument(
        '--cooldown-bars',
        type=int,
        default=None,
        help=f'Override preset bars between entries (balanced default: {DEFAULT_COOLDOWN_BARS})',
    )
    parser.add_argument(
        '--min-entry-body',
        type=float,
        default=None,
        help=(
            'Override preset minimum entry candle body/range ratio '
            f'(balanced default: {DEFAULT_MIN_ENTRY_CANDLE_BODY_PCT})'
        ),
    )
    parser.add_argument(
        '--momentum-lookback',
        type=int,
        default=None,
        help=f'Override preset momentum lookback (balanced default: {DEFAULT_MOMENTUM_LOOKBACK})',
    )
    parser.add_argument(
        '--max-correlated-trades',
        type=int,
        default=None,
        help=(
            'Override preset correlated-trade cap '
            f'(balanced default: {DEFAULT_MAX_CORRELATED_TRADES})'
        ),
    )
    parser.add_argument(
        '--spread-pips',
        type=float,
        default=None,
        help=(
            'Override explicit midpoint spread assumption in pips '
            f'(default: {DEFAULT_EXECUTION_SPREAD_PIPS})'
        ),
    )
    parser.add_argument(
        '--stop-slippage-pips',
        type=float,
        default=None,
        help=(
            'Override adverse stop slippage assumption in pips '
            f'(default: {DEFAULT_STOP_SLIPPAGE_PIPS})'
        ),
    )
    parser.add_argument(
        '--no-time-filters',
        action='store_true',
        help='Disable blocked hours/days entry filters',
    )
    parser.add_argument(
        '--no-pair-direction-filter',
        action='store_true',
        help='Disable historically weak pair-direction blocks',
    )
    parser.add_argument(
        '--blocked-hours',
        type=int,
        nargs='*',
        default=None,
        metavar='HOUR',
        help=(
            'Override blocked UTC entry hours. '
            f'Default: {sorted(DEFAULT_BLOCKED_HOURS)}. '
            'Pass no values to clear the block list.'
        ),
    )
    parser.add_argument(
        '--blocked-days',
        type=int,
        nargs='*',
        default=None,
        metavar='DAY',
        help=(
            'Override blocked weekdays (Monday=0). '
            f'Default: {sorted(DEFAULT_BLOCKED_DAYS)}. '
            'Pass no values to clear the block list.'
        ),
    )
    parser.add_argument(
        '--max-sl-pct',
        type=float,
        default=None,
        help='Skip signals where SL distance exceeds this %% of entry price (0 = disabled)',
    )


def _add_risk_sizing_args(
    parser,
    include_balance: bool = True,
    include_account_currency: bool = False,
):
    if include_balance:
        parser.add_argument(
            '--balance',
            type=float,
            default=None,
            help='Starting balance for compounding/live sizing (default: from IBKR NetLiquidation)',
        )
    parser.add_argument(
        '--risk-pct',
        type=float,
        default=None,
        help='Risk per trade as %% of balance (default: from profile)',
    )
    if include_account_currency:
        parser.add_argument(
            '--account-currency',
            type=str,
            default='GBP',
            help='Account currency for live sizing (default: GBP; used as fallback when IBKR returns BASE)',
        )


def _resolve_live_sizing(args) -> tuple[float | None, str | None]:
    """Resolve balance/currency used for live signal sizing."""

    balance = args.balance
    env_currency = os.getenv('IBKR_ACCOUNT_CURRENCY')
    currency = (
        args.account_currency.upper()
        if getattr(args, 'account_currency', None)
        else (env_currency.upper() if env_currency else None)
    )

    if balance is None:
        balance, fetched_currency = ibkr.fetch_account_net_liquidation()
        if currency is None and fetched_currency not in (None, 'BASE'):
            currency = fetched_currency

    return balance, currency


def cmd_backtest(args):
    """Run backtesting mode."""

    from fx_sr.db import init_db

    active_client_id = _configure_ibkr(args)
    init_db(migrate_legacy=False)
    profile_name = _requested_profile_name(args)
    profile = get_profile(profile_name)
    params = _build_strategy_params(args)
    pairs = _resolve_pairs(args.pair)
    # Use profile zone-history by default, unless explicitly overridden on CLI.
    if args.zone_history is None:
        args.zone_history = 180
    zone_days = args.zone_history
    if args.days is None:
        args.days = 365
    # Use profile defaults for balance/risk-pct if not overridden on CLI
    if args.balance is None:
        args.balance = profile.get('starting_balance', None)
    if args.risk_pct is None:
        args.risk_pct = profile.get('risk_pct', 5.0)
    execution_mode = _normalize_execution_mode(
        getattr(args, 'execution_mode', None) or profile.get('execution_mode', 'intrabar'),
        context='backtest',
    )
    run_id = uuid.uuid4().hex

    profile_name = _requested_profile_name(args)
    print(f"\n  IBKR client ID: {active_client_id}")
    print(f"  Strategy profile: {_format_preset_label(profile_name)}")
    print(f"  Backtest run ID: {run_id}")
    print(f"  Active params: {_format_param_summary(params)}")
    print(
        f"  Backtest: {len(pairs)} pair(s), {args.days} days hourly, "
        f"{zone_days} days daily zones"
    )

    t0 = time.time()

    if args.target_trades is not None:
        if args.target_trades < 0:
            print(f'  target_trades must be >= 0 (got {args.target_trades})')
            sys.exit(1)
        if args.target_profit_floor <= 0:
            print(f'  target_profit_floor must be > 0 (got {args.target_profit_floor})')
            sys.exit(1)
        if args.target_win_rate_floor <= 0:
            print(f'  target_win_rate_floor must be > 0 (got {args.target_win_rate_floor})')
            sys.exit(1)

        results, params, attempt_logs, summary, selected_label = _run_backtests_until_target(
            params=params,
            target_trades=args.target_trades,
            args=args,
            pairs=pairs,
            zone_days=zone_days,
            active_client_id=active_client_id,
            hourly_days=args.days,
            execution_mode=execution_mode,
            run_id=run_id,
            fetch_workers=getattr(args, 'fetch_workers', None),
            backtest_workers=getattr(args, 'backtest_workers', None),
            allow_stale_cache=getattr(args, 'allow_stale_cache', False),
        )
    else:
        run_config_json = _build_backtest_run_config(
            params,
            hourly_days=args.days,
            zone_history_days=zone_days,
            requested_profile=profile_name,
            execution_mode=execution_mode,
            starting_balance=args.balance,
            risk_pct=args.risk_pct,
            selection_label='baseline',
        )
        results = run_all_backtests_parallel(
            params=params,
            hourly_days=args.days,
            zone_history_days=zone_days,
            pairs=pairs,
            force_refresh=args.no_cache,
            run_id=run_id,
            base_client_id=active_client_id,
            run_config_json=run_config_json,
            execution_mode=execution_mode,
            debug=bool(getattr(args, 'backtest_debug', False)),
            fetch_workers=getattr(args, 'fetch_workers', None),
            backtest_workers=getattr(args, 'backtest_workers', None),
            allow_stale_cache=getattr(args, 'allow_stale_cache', False),
        )
        attempt_logs: list[dict[str, float | int | str]] = []
        summary = _portfolio_summary(
            results,
            params,
            starting_balance=args.balance,
            risk_pct=args.risk_pct / 100.0 if args.risk_pct is not None else None,
        ) if results else {
            'total_trades': 0,
            'total_wins': 0,
            'total_pnl': 0.0,
            'win_rate': 0.0,
            'raw_total_trades': 0,
            'raw_total_wins': 0,
            'raw_total_pnl': 0.0,
            'raw_win_rate': 0.0,
            'skip_counts': {},
        }
        selected_label = 'baseline'
        attempt_logs.append({
            'label': selected_label,
            'trades': summary['total_trades'],
            'win_rate': summary['win_rate'],
            'total_pnl': summary['total_pnl'],
            'raw_trades': summary['raw_total_trades'],
            'raw_total_pnl': summary['raw_total_pnl'],
            'elapsed_s': time.time() - t0,
        })

    if results:
        print(
            f"\n  Selected profile: {selected_label} "
            f"({summary.get('total_trades', 0)} execution-aware portfolio trades, "
            f"{summary.get('win_rate', 0.0):.1f}% WR, "
            f"{summary.get('total_pnl', 0.0):.1f} pips; "
            f"raw={summary.get('raw_total_trades', 0)} trades, "
            f"{summary.get('raw_total_pnl', 0.0):.1f} pips)"
        )

    elapsed = time.time() - t0

    if not results:
        print('\n  No data available for any pair. Exiting.')
        sys.exit(1)

    print(f'\n  Completed in {elapsed:.1f}s')
    print(f'\n{format_results(results, params=params)}')

    if args.balance:
        risk_pct = args.risk_pct / 100.0
        execution_sim = calculate_execution_aware_compounding_pnl(
            results,
            starting_balance=args.balance,
            risk_pct=risk_pct,
            params=params,
        )
        total_pre = execution_sim.raw_total_trades
        trade_log, final_bal = calculate_compounding_pnl(
            results,
            starting_balance=args.balance,
            risk_pct=risk_pct,
            params=params,
        )
        execution_report = format_compounding_results(
            execution_sim.trade_log,
            args.balance,
            execution_sim.final_balance,
            total_pre,
            title='EXECUTION-AWARE COMPOUNDING REPORT',
            filter_note='accepted from {total_pre_filter} raw candidates after execution-aware portfolio filtering',
            skip_counts=execution_sim.skip_counts,
        )
        raw_report = format_compounding_results(
            trade_log,
            args.balance,
            final_bal,
            total_pre,
            title='RAW COMPOUNDING REPORT',
            filter_note='filtered from {total_pre_filter} by correlation only',
        )
        print(f'\n{execution_report}')
        print(f'\n{raw_report}')

    if args.save_baseline or args.compare_baseline:
        artifact = build_backtest_baseline_artifact(
            results=results,
            params=params,
            requested_profile=profile_name,
            selection_label=selected_label,
            hourly_days=args.days,
            zone_history_days=zone_days,
            starting_balance=args.balance,
            risk_pct=args.risk_pct,
            portfolio_summary=summary,
            attempt_logs=attempt_logs,
        )
        if args.save_baseline:
            baseline_path = save_backtest_baseline_artifact(args.save_baseline, artifact)
            print(f'\n  Saved baseline artifact: {baseline_path}')
        if args.compare_baseline:
            expected = load_backtest_baseline_artifact(args.compare_baseline)
            comparison = compare_backtest_baseline_artifacts(expected, artifact)
            print(format_backtest_baseline_comparison(comparison))
            if not comparison['match']:
                sys.exit(1)

    if args.pair and args.verbose:
        key = args.pair.upper().replace('/', '')
        if key in results:
            result = results[key]
            pair_info = PAIRS[key]
            decimals = pair_info.get('decimals', 5)

            print(f'\n  Detailed trades for {key}:')
            print(
                f"  {'Entry Time':<22} {'Dir':>5} {'Entry':>12} {'Exit':>12} "
                f"{'P/L pips':>9} {'P/L R':>7} {'Reason':>10}"
            )
            print('  ' + '-' * 90)
            for trade in result.trades:
                print(
                    f"  {str(trade.entry_time):<22} {trade.direction:>5} "
                    f"{trade.entry_price:>{12}.{decimals}f} {trade.exit_price:>{12}.{decimals}f} "
                    f"{trade.pnl_pips:>9.1f} {trade.pnl_r:>7.2f} {trade.exit_reason:>10}"
                )

            print(f'\n  S/R Zones (final snapshot):')
            for zone in result.zones:
                print(
                    f"    [{zone.lower:.{decimals}f} - {zone.upper:.{decimals}f}]  "
                    f"{zone.zone_type:<12} {zone.strength:<8} "
                    f"({zone.touches} touches)"
                )


def cmd_status():
    """Show cache coverage for all pairs and intervals."""

    from fx_sr.db import get_cache_summary, get_db_path, init_db

    init_db()
    print(f'\n  Database: {get_db_path()}')

    summary = get_cache_summary()
    all_tickers = sorted(info['ticker'] for info in PAIRS.values())
    ticker_to_pair = {v['ticker']: k for k, v in PAIRS.items()}
    intervals = ['1d', '1h', '1m']

    if summary.empty:
        print('  No cached data found.\n')
        return

    # Build lookup: (ticker, interval) -> (bars, last_ts)
    cached = {}
    for _, row in summary.iterrows():
        cached[(row['ticker'], row['interval'])] = (row['bars'], str(row['last_ts'])[:19])

    print(f"\n  {'Pair':<10} {'Ticker':<14} {'1d bars':>8} {'1d last':>20}  {'1h bars':>8} {'1h last':>20}  {'1m bars':>8} {'1m last':>20}")
    print('  ' + '-' * 118)

    missing = []
    for ticker in all_tickers:
        pair = ticker_to_pair.get(ticker, ticker)
        parts = []
        for iv in intervals:
            if (ticker, iv) in cached:
                bars, last = cached[(ticker, iv)]
                parts.append((str(bars), last))
            else:
                parts.append(('---', '---'))
                missing.append((pair, iv))
        print(
            f'  {pair:<10} {ticker:<14} '
            f'{parts[0][0]:>8} {parts[0][1]:>20}  '
            f'{parts[1][0]:>8} {parts[1][1]:>20}  '
            f'{parts[2][0]:>8} {parts[2][1]:>20}'
        )

    print()
    if missing:
        print(f'  Missing ({len(missing)}):')
        for pair, iv in missing:
            print(f'    {pair:<10} {iv}')
    else:
        print('  All 22 pairs synced for all intervals.')
    print()


def _find_cache_gaps(
    target_days: int = 365,
    *,
    now=None,
    daily_extra_days: int = 0,
    only_pair: str | None = None,
) -> list[tuple[str, str, str]]:
    """Return list of (pair, ticker, interval) tuples that are missing or stale."""
    return fill_pipeline.find_cache_gaps(
        pairs=PAIRS,
        target_days=target_days,
        now=now,
        daily_extra_days=daily_extra_days,
        only_pair=only_pair,
    )


def _find_cache_gap_work_items(
    target_days: int = 365,
    *,
    now=None,
    daily_extra_days: int = 0,
    only_pair: str | None = None,
    verbose: bool = False,
) -> list[tuple[str, str, str, object]]:
    """Return gap work items including the first missing timestamp when known."""
    return fill_pipeline.find_cache_gap_work_items(
        pairs=PAIRS,
        target_days=target_days,
        now=now,
        daily_extra_days=daily_extra_days,
        only_pair=only_pair,
        verbose=verbose,
    )


def _find_cache_gaps_verbose(
    target_days: int = 365,
    *,
    now=None,
    daily_extra_days: int = 0,
    only_pair: str | None = None,
) -> list[tuple[str, str, str, str]]:
    """Like _find_cache_gaps but returns (pair, ticker, interval, detail) with diagnostic info."""
    return fill_pipeline.find_cache_gaps_verbose(
        pairs=PAIRS,
        target_days=target_days,
        now=now,
        daily_extra_days=daily_extra_days,
        only_pair=only_pair,
    )


def cmd_fill(args):
    """Identify cache gaps and fill them from IBKR."""

    import pandas as pd
    from fx_sr.db import get_db_path, init_db

    active_client_id = _configure_ibkr(args)
    target_days = args.days
    daily_extra_days = max(0, int(getattr(args, 'zone_history_days', 0)))
    verbose = getattr(args, 'verbose', False)
    debug = getattr(args, 'fill_debug', False)
    if debug:
        verbose = True
    daily_target_days = target_days + daily_extra_days
    pair_filter = getattr(args, 'pair', None)
    if pair_filter:
        pair_filter = pair_filter.upper().replace('/', '')
        if pair_filter not in PAIRS:
            print(f'  Unknown pair: {pair_filter}')
            print(f"  Available: {', '.join(PAIRS.keys())}")
            sys.exit(1)

    init_db()
    print(f'\n  Database: {get_db_path()}')
    print(f'  IBKR client ID: {active_client_id}')
    pair_label = pair_filter if pair_filter else 'all pairs'
    if daily_extra_days > 0:
        print(f'  Target: {target_days}d for {pair_label}, daily {daily_target_days}d (+{daily_extra_days}d zone history)')
    else:
        print(f'  Target: {target_days}d for {pair_label}')

    print(f'  Scanning for gaps...')
    gap_scan_start = time.perf_counter()
    gap_items = _find_cache_gap_work_items(target_days, daily_extra_days=daily_extra_days, only_pair=pair_filter, verbose=True)
    gap_scan_elapsed = time.perf_counter() - gap_scan_start
    print(f'  Scan complete ({gap_scan_elapsed:.1f}s)')
    if debug:
        print(f'  [dbg] gap scan took {gap_scan_elapsed:.2f}s')

    if not gap_items:
        print(f'\n  {pair_label.title()} fully synced for 1d, 1h, and 1m. Nothing to do.')
        return

    if verbose:
        gaps_verbose = _find_cache_gaps_verbose(
            target_days,
            daily_extra_days=daily_extra_days,
            only_pair=pair_filter,
        )
        if pair_filter:
            gaps_verbose = [(p, t, iv, d) for p, t, iv, d in gaps_verbose if p == pair_filter]
        print(f'\n  Gaps found ({len(gap_items)}):')
        for pair_id, _ticker, iv, detail in gaps_verbose:
            print(f'    {pair_id:<10} {iv:<4} {detail}')
        for pair_id, _ticker, iv, gap_start in gap_items:
            if gap_start is not None:
                print(f'    {pair_id:<10} {iv:<4} internal/trailing hole starts at {gap_start}')
    else:
        print(f'\n  Gaps found ({len(gap_items)}):')
        for pair_id, _ticker, iv, gap_start in gap_items:
            suffix = f' from {gap_start}' if gap_start is not None else ''
            print(f'    {pair_id:<10} {iv}{suffix}')

    # Each work item is a single (pair, interval) - run 3 at a time to stay
    # under IBKR's ~5 concurrent historical-data request limit.
    # Each thread keeps a stable client ID so it reuses the same IBKR
    # connection across all its work items (avoids Error 326 collisions).
    hist_fetch_workers = max(1, ibkr.set_historical_fetch_concurrency(getattr(args, 'ib_historical_fetch_concurrency', None)))
    MAX_WORKERS = min(3, hist_fetch_workers)
    base_fill_client_id = _fill_client_id_base(active_client_id)
    client_id_suffix = (
        f'{base_fill_client_id}'
        if MAX_WORKERS == 1
        else f'{base_fill_client_id}-{base_fill_client_id + MAX_WORKERS - 1}'
    )
    print(f'  Fill worker client IDs: {client_id_suffix}')
    work_items = fill_pipeline.build_fill_execution_items(
        gap_items,
        pairs=PAIRS,
        target_days=target_days,
        daily_target_days=daily_target_days,
    )

    def _format_wait_window(item: fill_pipeline.FillExecutionItem, waiting_s: float) -> str:
        now_ts = pd.Timestamp.now(tz='UTC')
        if item.gap_start is not None:
            start_ts = pd.Timestamp(item.gap_start)
            if start_ts.tzinfo is None:
                start_ts = start_ts.tz_localize('UTC')
            else:
                start_ts = start_ts.tz_convert('UTC')
            wait_window = f'{start_ts:%Y-%m-%d %H:%M} -> {now_ts:%Y-%m-%d %H:%M} UTC'
            return f'{item.pair_id} {item.interval} [{wait_window}] ({waiting_s:.0f}s)'

        start_ts = now_ts - pd.Timedelta(days=max(1, int(item.item_days)))
        if item.interval == '1d':
            wait_window = f'~{start_ts:%Y-%m-%d} -> {now_ts:%Y-%m-%d} UTC'
        else:
            wait_window = f'~{start_ts:%Y-%m-%d %H:%M} -> {now_ts:%Y-%m-%d %H:%M} UTC'
        return f'{item.pair_id} {item.interval} [{wait_window}] ({waiting_s:.0f}s)'

    result = fill_pipeline.execute_fill_work_items(
        work_items,
        base_fill_client_id=base_fill_client_id,
        max_workers=MAX_WORKERS,
        max_retries=3,
        wait_timeout_s=15.0,
        verbose=verbose,
        debug=debug,
        before_retry=lambda attempt, pending_count: print(
            f'\n  Retry {attempt}/3 - {pending_count} items remaining, waiting 5s...'
        ),
        wait_formatter=_format_wait_window,
        on_wait=lambda waiting_parts: print(
            f"  Still working on {len(waiting_parts)} item(s): {', '.join(waiting_parts)}"
        ),
        on_item_done=lambda item, rows, item_elapsed, completed, total: print(
            f'  [{completed}/{total}] {item.pair_id} {item.interval} done ({rows} rows, {item_elapsed:.2f}s)'
        ),
        on_item_failed=lambda item, exc, completed, total: print(
            f'  [{completed}/{total}] {item.pair_id} {item.interval} FAILED: {exc}'
        ),
        on_attempt_complete=(
            (lambda attempt, failed_count, attempt_elapsed:
                print(f'  [dbg] attempt {attempt}/3 completed in {attempt_elapsed:.2f}s ({failed_count} failed)'))
            if debug else None
        ),
    )

    # Clean up all IBKR connections so TWS releases the client IDs immediately.
    ibkr.disconnect_all()

    elapsed = float(result['elapsed'])
    print(f'\n  Fill completed in {elapsed:.1f}s')

    # Re-check
    print('  Rechecking gaps...')
    recheck_start = time.perf_counter()
    remaining = _find_cache_gap_work_items(
        target_days,
        daily_extra_days=daily_extra_days,
        only_pair=pair_filter,
        verbose=True,
    )
    recheck_elapsed = time.perf_counter() - recheck_start
    if debug:
        print(f'  [dbg] post-fill recheck took {recheck_elapsed:.2f}s')
    if remaining:
        print(f'\n  Still missing ({len(remaining)}):')
        for pair_id, _ticker, iv, gap_start in remaining:
            suffix = f' from {gap_start}' if gap_start is not None else ''
            print(f'    {pair_id:<10} {iv}{suffix}')
    else:
        print('\n  All gaps filled.')


def cmd_download(args):
    """Download and cache price data to PostgreSQL."""

    from fx_sr.data import download_all_data
    from fx_sr.db import get_cache_summary, get_db_path

    active_client_id = _configure_ibkr(args)
    pairs = _resolve_pairs(args.pair)

    import time
    t0 = time.time()

    print(f'\n  Database: {get_db_path()}')
    print(f'  IBKR client ID: {active_client_id}')
    if args.minute_only and args.minute_days <= 0:
        print('  --minute-only requires --minute-days > 0')
        sys.exit(1)
    max_workers = max(1, min(5, int(args.sync_workers)))
    download_all_data(
        pairs,
        hourly_days=0 if args.minute_only else args.days,
        daily_days=0 if args.minute_only else args.days,
        minute_days=args.minute_days,
        minute_only=args.minute_only,
        client_id=active_client_id,
        max_workers=max_workers,
        resume=not args.refresh_all,
    )

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


def cmd_l2(args):
    """Capture or inspect L2 market-depth snapshots."""

    from fx_sr import db as db_module
    from fx_sr.l2 import (
        capture_l2_once,
        capture_l2_stream,
        format_l2_capture_summary,
        format_l2_library_summary,
        format_l2_snapshot,
    )

    active_client_id = _configure_ibkr(args)
    pairs = _resolve_pairs(args.pair)

    print(f"\n  IBKR client ID: {active_client_id}")

    if args.summary:
        rows = []
        for pair_info in pairs.values():
            summary = db_module.get_l2_summary(ticker=pair_info['ticker'])
            if not summary.empty:
                rows.append(summary)
        if rows:
            import pandas as pd

            print(format_l2_library_summary(pd.concat(rows, ignore_index=True)))
        else:
            print("\n  No cached L2 snapshots found.\n")
        return

    if args.once:
        for pair_id, pair_info in pairs.items():
            snapshot = capture_l2_once(
                pair_id,
                pair_info,
                depth=args.depth,
                client_id=active_client_id,
            )
            if snapshot is None:
                print(f"  No L2 depth returned for {pair_id}.")
                continue
            print(format_l2_snapshot(snapshot))
        return

    print(
        f"  Capturing L2 depth for {len(pairs)} pair(s) at {args.interval:.2f}s intervals, "
        f"depth {args.depth}, for {args.seconds:.1f}s"
    )
    stats = capture_l2_stream(
        pairs,
        depth=args.depth,
        interval_seconds=args.interval,
        duration_seconds=args.seconds,
        max_snapshots=args.snapshots,
        client_id=active_client_id,
    )
    print(format_l2_capture_summary(stats))


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


def cmd_run(args):
    """Unified engine: backfill → walk-forward → backtest or live."""

    mode = args.mode

    # Resolve cache_only: explicit flag overrides, else default per mode
    cache_only = getattr(args, 'cache_only', None)
    if cache_only is None:
        cache_only = (mode == 'backtest')

    if mode == 'backtest':
        # Fill in defaults for backtest-only args not on the 'run' parser
        _backtest_defaults = {
            'target_trades': None,
            'target_profit_floor': 1.0,
            'target_win_rate_floor': 1.0,
            'no_l2': False,
            'once': False,
            'zones': False,
        }
        for attr, default in _backtest_defaults.items():
            if not hasattr(args, attr):
                setattr(args, attr, default)
        cmd_backtest(args)
    elif mode == 'live':
        # Fill in defaults for live-only args not on the 'run' parser
        _live_defaults = {
            'once': False,
            'zones': False,
        }
        for attr, default in _live_defaults.items():
            if not hasattr(args, attr):
                setattr(args, attr, default)
        cmd_live(args)


def cmd_live(args):
    """Run live monitoring mode."""

    client_id_provided = getattr(args, 'ibkr_client_id', None) is not None
    if not client_id_provided:
        args.ibkr_client_id = 99
    # Keep client-id fallback enabled so startup can recover if any configured ID is already active.
    active_client_id = _configure_ibkr(args, allow_client_id_fallback=True)
    params = _build_strategy_params(args)
    pairs = _resolve_pairs(args.pair)
    zone_days = args.zone_history
    profile_name = _requested_profile_name(args)
    profile = get_profile(profile_name)
    execution_mode = _normalize_execution_mode(
        getattr(args, 'execution_mode', None) or profile.get('execution_mode', 'intrabar'),
        context='live',
    )
    chart_tf = profile.get('chart_tf', '1h')
    if args.risk_pct is None:
        args.risk_pct = profile.get('risk_pct', 5.0)

    if args.zones:
        for pair_id, pair_info in pairs.items():
            print(show_zones(pair_id, pair_info, zone_history_days=zone_days))
        return

    live_web_client_id = active_client_id
    if not args.once and not client_id_provided:
        live_web_client_id = active_client_id + 1000
    print(f"\n  IBKR client ID: {active_client_id}")
    if not args.once and not client_id_provided and live_web_client_id != active_client_id:
        print(f"  Live dashboard IBKR client ID: {live_web_client_id}")
    print(f"  Strategy profile: {_format_preset_label(profile_name)}")
    print(f"  Active params: {_format_param_summary(params)}")
    print(f"  Signal timing: {execution_mode}")
    live_balance, live_currency = _resolve_live_sizing(args)
    # Release the main-thread IBKR connection so worker threads in the web
    # app can reuse the same client_id without TWS rejecting them (Error 326).
    ibkr.disconnect()

    # Keep the shared IBKR default at the base live ID; the dashboard stream
    # thread uses its own client-id offset internally.
    if client_id_provided and not args.once:
        ibkr.configure_connection(client_id=live_web_client_id)

    if live_balance is not None and live_currency:
        print(
            f"  Live sizing: {live_currency} {live_balance:,.2f} balance, "
            f"{args.risk_pct:.2f}% risk/trade"
        )
    elif live_balance is not None:
        print(
            "  Live sizing: balance resolved but account currency is unknown. "
            "Pass --account-currency or set IBKR_ACCOUNT_CURRENCY to enable sizing/execution."
        )
    else:
        print("  Live sizing: unavailable (could not resolve balance)")

    can_execute = live_balance is not None and live_currency is not None
    if args.paper_trade and not can_execute:
        print(
            "\n  ERROR: paper trading requires both balance and account currency.\n"
            "  Ensure IB Gateway is running on port 4002, or pass --balance and --account-currency.\n"
            "  Use --no-paper-trade for scan-only mode."
        )
        sys.exit(1)

    if args.once:
        print(f'  Scanning {len(pairs)} pairs for opportunities...')
        tracked = {}
        if not args.no_positions:
            from fx_sr.positions import sync_positions

            tracked = sync_positions(params, zone_days)
        portfolio_state = load_portfolio_state(params, current_balance=live_balance)
        pending_pairs = ibkr.fetch_open_order_pairs()
        market_prices = {}
        hourly_data_cache = {}
        daily_data_cache = {}
        zone_cache = {}
        signals = scan_opportunities(
            pairs,
            params,
            zone_history_days=zone_days,
            tracked_positions=tracked,
            blocked_pairs=pending_pairs,
            price_cache=market_prices,
            daily_data_cache=daily_data_cache,
            zone_cache=zone_cache,
            hourly_data_cache=hourly_data_cache,
            execution_mode=execution_mode,
            portfolio_state=portfolio_state,
        )
        size_plans = build_live_size_plans(
            signals,
            balance=live_balance,
            risk_pct=args.risk_pct / 100.0,
            account_currency=live_currency,
            params=params,
            portfolio_state=portfolio_state,
            price_cache=market_prices,
            hourly_data_cache=hourly_data_cache,
        )
        from fx_sr import ibkr as _ibkr_mod
        _exec_mode = _ibkr_mod.get_execution_mode() if args.paper_trade else 'scan'
        _ibkr_acct = _ibkr_mod.fetch_account_id() if args.paper_trade else None
        record_detected_signals(
            signals,
            size_plans,
            execute_orders=args.paper_trade,
            execution_mode=_exec_mode,
            ibkr_account=_ibkr_acct,
        )
        print(format_signals_with_sizes(signals, size_plans))
        if args.paper_trade:
            from fx_sr.live import execute_signal_plans, format_execution_results

            execution_results = execute_signal_plans(
                signals,
                size_plans,
                execute_orders=True,
                existing_pairs={info['pair'] for info in tracked.values()},
                pending_pairs=pending_pairs,
                params=params,
                tracked_positions=tracked,
                balance=live_balance,
                risk_pct=args.risk_pct / 100.0,
                account_currency=live_currency,
                price_cache=market_prices,
                hourly_data_cache=hourly_data_cache,
                execution_mode=execution_mode,
            )
            record_execution_results(
                signals, size_plans, execution_results,
                execution_mode=_exec_mode,
                ibkr_account=_ibkr_acct,
            )
            print(format_execution_results(execution_results))
        return

    from fx_sr.live_web import run_live_web_app

    hourly_days = getattr(args, 'days', None) or profile.get('hourly_days', 1)
    run_live_web_app(
        pairs=pairs,
        params=params,
        interval=args.interval,
        zone_history_days=zone_days,
        track_positions=not args.no_positions,
        balance=live_balance,
        risk_pct=args.risk_pct / 100.0,
        account_currency=live_currency,
        execute_orders=args.paper_trade,
        strategy_label=_format_preset_label(profile_name),
        client_id=live_web_client_id,
        port=args.port,
        open_browser=not args.no_browser,
        execution_mode=execution_mode,
        chart_tf=chart_tf,
        hourly_days=hourly_days,
    )


def main():
    profile_lines = '\n'.join(
        f"  {name:<12} {p['description']} "
        f"(rr={p['rr_ratio']}, sl={p['sl_buffer_pct']}, "
        f"early={p['early_exit_r']}, corr={p['max_correlated_trades']})"
        for name in PROFILES
        for p in [get_profile(name)]
    )
    epilog = (
        'Examples:\n'
        '  python run.py sync\n'
        '  python run.py sync --days 365 --pair EURUSD\n'
        '  (legacy: python run.py download)\n'
        '  python run.py backtest --days 365 --balance 1000 --risk-pct 5\n'
        '  python run.py backtest --profile source\n'
        '  python run.py backtest --profile aggressive\n'
        '  python run.py backtest --profile optimized --rr-ratio 1.5\n'
        '  python run.py l2 --pair EURUSD --once\n'
        '  python run.py l2 --pair EURUSD --seconds 300 --interval 1\n'
        '  python run.py live --profile aggressive --once\n'
        '  python run.py live --port 7755\n\n'
        'Profiles (edit fx_sr/profiles.py to add/modify):\n'
        f'{profile_lines}'
    )

    parser = argparse.ArgumentParser(
        description='FX S/R zone trading tool (daily zones + hourly execution + L2 capture)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
    )

    subparsers = parser.add_subparsers(dest='command', help='Mode')

    subparsers.add_parser('status', help='Show cache coverage for all pairs and intervals')

    fl = subparsers.add_parser('fill', help='Detect cache gaps and fill from IBKR')
    fl.add_argument(
        '--pair',
        type=str,
        help='Specific pair (e.g., EURUSD). Default: all configured pairs',
    )
    fl.add_argument(
        '--days',
        type=int,
        default=365,
        help='Target days of coverage per interval (default: 365)',
    )
    fl.add_argument(
        '--zone-history-days',
        type=int,
        default=0,
        help='Additional daily history beyond --days (default: 0; zone lookback uses the same 365d window)',
    )
    fl.add_argument(
        '--ib-historical-fetch-concurrency',
        type=int,
        default=None,
        help='Limit concurrent IB historical requests (1..N, default auto, env IBKR_HISTORICAL_FETCH_CONCURRENCY or 2)',
    )
    fl.add_argument('--fill-debug', action='store_true', help='Show detailed fill timing and retry diagnostics')
    fl.add_argument('-v', '--verbose', action='store_true', help='Show detailed cache diagnostics and download progress')
    _add_ibkr_args(fl)

    dl = subparsers.add_parser('sync', help='Sync and cache price data from IBKR')
    _add_download_args(dl)

    legacy_dl = subparsers.add_parser('download', help='[legacy] Same as `sync`')
    _add_download_args(legacy_dl)

    bt = subparsers.add_parser('backtest', help='Backtest using daily zones + hourly execution')
    bt.add_argument('--pair', type=str, help='Specific pair (e.g., EURUSD). Default: all configured pairs')
    bt.add_argument(
        '--days',
        type=int,
        default=365,
        help='Days of hourly data for execution (default: 365)',
    )
    bt.add_argument(
        '--zone-history',
        type=int,
        default=180,
        help='Days of daily data for zone detection (default: 180)',
    )
    _add_ibkr_args(bt)
    _add_strategy_args(bt)
    _add_risk_sizing_args(bt, include_balance=True, include_account_currency=False)
    bt.add_argument(
        '--no-cache',
        action='store_true',
        help='Compatibility flag only; backtests always run from cache and will fail fast if data is missing',
    )
    bt.add_argument(
        '--allow-stale-cache',
        action='store_true',
        help='Allow stale cached bars in backtests (skip recency checks) while keeping shape checks active',
    )
    bt.add_argument(
        '--execution-mode',
        choices=('intrabar',),
        default='intrabar',
        help='Execution timing mode for backtests (fixed: intrabar)',
    )
    bt.add_argument(
        '--target-trades',
        type=int,
        default=None,
        help='Retry with progressively relaxed filters until at least this many total portfolio trades are reached',
    )
    bt.add_argument(
        '--target-profit-floor',
        type=float,
        default=1.0,
        help='Minimum baseline profitability multiplier for target mode (default: 1.0)',
    )
    bt.add_argument(
        '--target-win-rate-floor',
        type=float,
        default=1.0,
        help='Minimum baseline win-rate multiplier for target mode (default: 1.0)',
    )
    bt.add_argument(
        '--save-baseline',
        type=str,
        default=None,
        help='Write a reproducible backtest baseline artifact JSON to this path',
    )
    bt.add_argument(
        '--compare-baseline',
        type=str,
        default=None,
        help='Compare the current backtest run against a saved baseline artifact and exit non-zero on mismatch',
    )
    bt.add_argument(
        '--backtest-debug',
        action='store_true',
        help='Emit detailed backtest execution tracing for parallel pipeline progress',
    )
    bt.add_argument(
        '--fetch-workers',
        type=int,
        default=None,
        help='Limit parallel Phase-1 fetch workers used before backtests start (1..N, default auto)',
    )
    bt.add_argument(
        '--backtest-workers',
        type=int,
        default=18,
        help='Limit parallel compute workers for zone/walk-forward phases (default: 18)',
    )
    bt.add_argument(
        '--ib-historical-fetch-concurrency',
        type=int,
        default=None,
        help='Limit concurrent IB historical requests (1..N, default auto, env IBKR_HISTORICAL_FETCH_CONCURRENCY or 2)',
    )
    bt.add_argument('-v', '--verbose', action='store_true', help='Show individual trade details')
    bt.add_argument(
        '--no-margin',
        action='store_true',
        help='Disable margin and minimum-size checks in backtest (compare against realistic defaults)',
    )
    bt.add_argument(
        '--no-l2',
        action='store_true',
        help='Run without L2 order-book data — use minute/hourly fallback for execution quotes',
    )

    lv = subparsers.add_parser('live', help='Monitor live data for zone opportunities')
    lv.add_argument('--pair', type=str, help='Specific pair to monitor')
    lv.add_argument(
        '--interval',
        type=int,
        default=60,
        help='Scan interval in seconds (default: 60)',
    )
    lv.add_argument(
        '--zone-history',
        type=int,
        default=DEFAULT_ZONE_HISTORY_DAYS,
        help=f'Days of daily data for zones (default: {DEFAULT_ZONE_HISTORY_DAYS})',
    )
    _add_ibkr_args(lv)
    _add_strategy_args(lv)
    _add_risk_sizing_args(lv, include_balance=True, include_account_currency=True)
    lv.add_argument('--once', action='store_true', help='Single scan then exit')
    lv.add_argument('--zones', action='store_true', help='Show current S/R zones and exit')
    lv.add_argument(
        '--no-positions',
        action='store_true',
        help='Disable IBKR position tracking and duplicate-position filtering',
    )
    lv.add_argument(
        '--paper-trade',
        action='store_true',
        default=True,
        help='Submit paper-market orders for sized signals (default: on)',
    )
    lv.add_argument(
        '--no-paper-trade',
        action='store_false',
        dest='paper_trade',
        help='Disable paper-trade order submission (scan only)',
    )
    lv.add_argument(
        '--port',
        type=int,
        default=7755,
        help='Local dashboard server port (default: 7755)',
    )
    lv.add_argument(
        '--no-browser',
        action='store_true',
        help='Start the live dashboard server without opening a browser',
    )
    lv.add_argument(
        '--execution-mode',
        choices=('intrabar',),
        default='intrabar',
        help='Signal timing mode (fixed: intrabar)',
    )

    # ---- Unified 'run' subcommand ----
    rn = subparsers.add_parser(
        'run',
        help='Unified engine: backfill data, walk-forward, then backtest (quit) or live (keep going)',
    )
    rn.add_argument(
        '--mode',
        choices=('backtest', 'live'),
        required=True,
        help='backtest: walk-forward over cache, record trades, exit. '
             'live: walk-forward then keep processing incoming bars.',
    )
    rn.add_argument('--pair', type=str, help='Specific pair (e.g., EURUSD). Default: all configured pairs')
    rn.add_argument(
        '--days', type=int, default=365,
        help='Days of hourly data for execution (default: 365)',
    )
    rn.add_argument(
        '--zone-history', type=int, default=180,
        help='Days of daily data for zone detection (default: 180)',
    )
    rn.add_argument(
        '--cache-only', action='store_true', default=None,
        help='Use PostgreSQL cache only, no IBKR fetch (default for backtest)',
    )
    _add_ibkr_args(rn)
    _add_strategy_args(rn)
    _add_risk_sizing_args(rn, include_balance=True, include_account_currency=True)
    rn.add_argument(
        '--execution-mode', choices=('intrabar',), default='intrabar',
        help='Execution timing mode (fixed: intrabar)',
    )
    rn.add_argument(
        '--allow-stale-cache', action='store_true',
        help='Allow stale cached bars (skip recency checks)',
    )
    rn.add_argument('-v', '--verbose', action='store_true', help='Show individual trade details')
    # Backtest-specific
    rn.add_argument(
        '--save-baseline', type=str, default=None,
        help='[backtest] Write reproducible baseline artifact JSON',
    )
    rn.add_argument(
        '--compare-baseline', type=str, default=None,
        help='[backtest] Compare against saved baseline artifact',
    )
    rn.add_argument(
        '--backtest-debug', action='store_true',
        help='[backtest] Emit detailed execution tracing',
    )
    rn.add_argument(
        '--backtest-workers', type=int, default=18,
        help='[backtest] Parallel compute workers (default: 18)',
    )
    rn.add_argument(
        '--fetch-workers', type=int, default=None,
        help='[backtest] Limit Phase-1 fetch workers (default: auto)',
    )
    rn.add_argument('--no-cache', action='store_true', help='[compat] Ignored')
    rn.add_argument('--no-margin', action='store_true', help='[backtest] Disable margin checks')
    rn.add_argument('--no-l2', action='store_true', help='[backtest] Skip L2 data')
    # Live-specific
    rn.add_argument(
        '--interval', type=int, default=60,
        help='[live] Scan interval in seconds (default: 60)',
    )
    rn.add_argument(
        '--port', type=int, default=7755,
        help='[live] Dashboard server port (default: 7755)',
    )
    rn.add_argument(
        '--paper-trade', action='store_true', default=True,
        help='[live] Submit paper-market orders (default: on)',
    )
    rn.add_argument(
        '--no-paper-trade', action='store_false', dest='paper_trade',
        help='[live] Disable order submission (scan only)',
    )
    rn.add_argument(
        '--no-positions', action='store_true',
        help='[live] Disable IBKR position tracking',
    )
    rn.add_argument(
        '--no-browser', action='store_true',
        help='[live] Start dashboard without opening browser',
    )
    rn.add_argument(
        '--ib-historical-fetch-concurrency', type=int, default=None,
        help='Limit concurrent IB historical requests',
    )

    l2p = subparsers.add_parser('l2', help='Capture and inspect IBKR L2 market depth')
    l2p.add_argument(
        '--pair',
        type=str,
        required=False,
        help='Specific pair to capture (for example EURUSD)',
    )
    l2p.add_argument(
        '--depth',
        type=int,
        default=5,
        help='Depth levels per side to request from IBKR (default: 5)',
    )
    l2p.add_argument(
        '--interval',
        type=float,
        default=1.0,
        help='Seconds between saved snapshots during streaming capture (default: 1.0)',
    )
    l2p.add_argument(
        '--seconds',
        type=float,
        default=60.0,
        help='Streaming capture duration in seconds (default: 60)',
    )
    l2p.add_argument(
        '--snapshots',
        type=int,
        default=None,
        help='Stop after saving this many snapshots (default: unlimited within --seconds)',
    )
    l2p.add_argument('--once', action='store_true', help='Fetch and save one depth snapshot then exit')
    l2p.add_argument('--summary', action='store_true', help='Show cached L2 summary for the requested pair')
    _add_ibkr_args(l2p)

    vz = subparsers.add_parser('viz', help='Export backtest data and open interactive chart')
    vz.add_argument(
        '--days',
        type=int,
        default=365,
        help='Days of hourly data for backtest (default: 365)',
    )
    _add_ibkr_args(vz)
    vz.add_argument('--port', type=int, default=7755, help='Local server port (default: 7755)')
    vz.add_argument(
        '--refresh',
        action='store_true',
        help='Force regenerate viz_data.json (default: reuse if exists)',
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == 'status':
        cmd_status()
    elif args.command == 'fill':
        cmd_fill(args)
    elif args.command in ('download', 'sync'):
        cmd_download(args)
    elif args.command == 'backtest':
        cmd_backtest(args)
    elif args.command == 'l2':
        cmd_l2(args)
    elif args.command == 'live':
        cmd_live(args)
    elif args.command == 'run':
        cmd_run(args)
    elif args.command == 'viz':
        cmd_viz(args)


if __name__ == '__main__':
    main()







