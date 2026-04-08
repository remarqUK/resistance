"""Shared walk-forward execution helpers for backtest and replay."""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass, replace
from typing import Callable, Optional

import pandas as pd

from .execution import build_execution_plan, build_modeled_execution_quote
from .ibkr import ExecutionQuote
from .levels import SRZone
from .strategy import (
    Signal,
    StrategyParams,
    Trade,
    build_trade_from_signal,
    check_exit,
    check_price_exit,
    get_market_exit_price,
    get_tradeable_zones,
    select_entry_signal,
)
from .intrabar import find_intrabar_signal, intrabar_execution_time


_LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')


def _walk_debug_enabled() -> bool:
    """Whether to emit detailed walk-forward timing diagnostics."""
    value = os.getenv('FX_SR_WALK_DEBUG', '').strip().lower()
    return value in {'1', 'true', 'yes', 'on', 'debug'}


def _check_exit_minute_resolution(
    trade: Trade,
    minute_df: pd.DataFrame | None,
    hourly_time: pd.Timestamp,
    bars_held: int,
    params: StrategyParams,
    pip: float,
    minute_index: pd.Index | None = None,
) -> Optional[tuple[str, float, pd.Timestamp]]:
    """Check TP/SL using minute OHLC within the hourly bar's window.

    Iterates each minute bar chronologically; the first minute that triggers
    a TP or SL wins.  This eliminates the "both hit same bar → SL wins"
    pessimism of hourly-only evaluation.

    Non-price exits (FRIDAY, EARLY_EXIT, SIDEWAYS, TIME) are NOT checked
    here — they remain evaluated at hourly granularity by the caller.

    Returns (exit_reason, exit_price, exit_time) or None.
    """
    if minute_df is None or minute_df.empty:
        return None

    # Slice minute bars belonging to this hourly candle
    bar_start = hourly_time
    bar_end = hourly_time + pd.Timedelta(hours=1)
    index = minute_index if minute_index is not None else minute_df.index
    start_pos = index.searchsorted(bar_start, side='left')
    end_pos = index.searchsorted(bar_end, side='left')
    if start_pos >= end_pos or start_pos >= len(index):
        return None
    if end_pos > len(index):
        end_pos = len(index)
    window = minute_df.iloc[start_pos:end_pos]
    if window.empty:
        return None

    for ts, mrow in window.iterrows():
        high = float(mrow['High'])
        low = float(mrow['Low'])
        close = float(mrow['Close'])

        # Update best favorable price per minute bar
        if trade.direction == 'LONG':
            if trade.best_favorable_price is None or high > trade.best_favorable_price:
                trade.best_favorable_price = high
        else:
            if trade.best_favorable_price is None or low < trade.best_favorable_price:
                trade.best_favorable_price = low

        result = check_price_exit(
            trade, high, low, close,
            params=params, pip=pip,
            bar_time=ts, bars_held=bars_held,
            allow_friday=False, allow_sideways=False, allow_time=False,
        )
        if result:
            return (result[0], result[1], ts)

    return None


def _zone_snapshot(zone: SRZone | None) -> dict | None:
    if zone is None:
        return None
    return {'upper': zone.upper, 'lower': zone.lower, 'strength': zone.strength, 'touches': zone.touches}


def _write_trade_snapshot(
    *,
    source: str,
    event: str,
    pair: str,
    timestamp: pd.Timestamp,
    signal: Signal | None,
    trade: Trade | None,
    quote: ExecutionQuote | None = None,
    execution_plan=None,
    bar_index: int = 0,
    bar_time: pd.Timestamp | None = None,
    bar_row: pd.Series | None = None,
    bars_held: int = 0,
    nearest_support: SRZone | None = None,
    nearest_resistance: SRZone | None = None,
    exit_reason: str | None = None,
    exit_price: float | None = None,
) -> None:
    """Write a JSON trade snapshot to logs/. Fire-and-forget — never raises."""
    try:
        os.makedirs(_LOGS_DIR, exist_ok=True)
        ts_str = str(timestamp).replace(':', '').replace(' ', '-')[:13]
        filename = f"{source}-{event}-{pair}-{ts_str}.json"
        snapshot = {
            'source': source,
            'event': event,
            'pair': pair,
            'timestamp': str(timestamp),
            'signal': asdict(signal) if signal else None,
            'trade': asdict(trade) if trade else None,
            'execution_quote': asdict(quote) if quote else None,
            'execution_plan': {
                'entry_price': execution_plan.entry_price,
                'stop_price': execution_plan.stop_price,
                'take_profit_price': execution_plan.take_profit_price,
            } if execution_plan else None,
            'bar': {
                'time': str(bar_time),
                'open': float(bar_row['Open']) if bar_row is not None else None,
                'high': float(bar_row['High']) if bar_row is not None else None,
                'low': float(bar_row['Low']) if bar_row is not None else None,
                'close': float(bar_row['Close']) if bar_row is not None else None,
            },
            'bar_index': bar_index,
            'bars_held': bars_held,
            'zones': {
                'support': _zone_snapshot(nearest_support),
                'resistance': _zone_snapshot(nearest_resistance),
            },
            'exit_reason': exit_reason,
            'exit_price': exit_price,
        }
        filepath = os.path.join(_LOGS_DIR, filename)
        with open(filepath, 'w') as f:
            json.dump(snapshot, f, indent=2, default=str)
    except Exception:
        pass  # Snapshot failures must never break trading


def slice_daily_window(
    daily_df: pd.DataFrame,
    end_date,
    zone_history_days: int,
) -> pd.DataFrame:
    """Return a walk-forward daily window bounded by zone_history_days."""

    if daily_df.empty or zone_history_days <= 0:
        return daily_df.iloc[0:0]

    end_ts = pd.Timestamp(end_date)
    end_day = end_ts.date() if hasattr(end_ts, 'date') else end_date
    start_day = (end_ts - pd.Timedelta(days=max(zone_history_days - 1, 0))).date()

    if hasattr(daily_df.index, 'date'):
        index_dates = daily_df.index.date
        mask = (index_dates >= start_day) & (index_dates <= end_day)
        return daily_df[mask]

    bounded = daily_df[daily_df.index <= end_ts]
    return bounded.tail(zone_history_days)


def finalize_trade(
    trade: Trade,
    exit_time,
    exit_price: float,
    exit_reason: str,
    bars_held: int,
    pip: float,
    commission_pips: float = 0.0,
    commission_cost: float = 0.0,
) -> Trade:
    """Populate final trade state and derived P&L metrics."""

    trade.exit_time = exit_time
    trade.exit_price = float(exit_price)
    trade.exit_reason = exit_reason
    trade.bars_held = bars_held
    trade.commission_cost = commission_cost

    if trade.direction == 'LONG':
        raw_price_move = trade.exit_price - trade.entry_price
        trade.pnl_pips = raw_price_move / pip - commission_pips
        if trade.risk > 0:
            commission_price = commission_pips * pip
            trade.pnl_r = (raw_price_move - commission_price) / trade.risk
    else:
        raw_price_move = trade.entry_price - trade.exit_price
        trade.pnl_pips = raw_price_move / pip - commission_pips
        if trade.risk > 0:
            commission_price = commission_pips * pip
            trade.pnl_r = (raw_price_move - commission_price) / trade.risk

    return trade


@dataclass(frozen=True)
class WalkForwardBar:
    """Snapshot of one processed hourly bar in the shared walk-forward loop."""

    bar_index: int
    bar_time: pd.Timestamp
    row: pd.Series
    zones: list[SRZone]
    support_zone: Optional[SRZone]
    resistance_zone: Optional[SRZone]
    signal: Optional[Signal]
    opened_trade: Optional[Trade]
    exit_trade: Optional[Trade]
    open_trade: Optional[Trade]
    bars_held: int


@dataclass(frozen=True)
class WalkForwardResult:
    """Outcome of a shared walk-forward execution run."""

    trades: list[Trade]
    zones: list[SRZone]
    open_trade: Optional[Trade]


def _next_bar_submit_time(bar_time: pd.Timestamp) -> pd.Timestamp:
    """Return UTC submission time for a signal generated from a completed bar."""

    ts = pd.Timestamp(bar_time)
    if ts.tzinfo is None:
        ts = ts.tz_localize('UTC')
    else:
        ts = ts.tz_convert('UTC')
    return ts + pd.Timedelta(hours=1)


def resolve_entry_signal_for_bar(
    *,
    hourly_df: pd.DataFrame,
    bar_idx: int,
    pair: str,
    params: StrategyParams,
    current_bar_time: pd.Timestamp,
    nearest_support: Optional[SRZone],
    nearest_resistance: Optional[SRZone],
    execution_mode: str,
    minute_df: pd.DataFrame | None = None,
    align_signal_time: bool = False,
) -> tuple[Optional[Signal], Optional[pd.Timestamp]]:
    """Resolve a candidate signal and its preferred submit timestamp for a bar."""

    if execution_mode not in {'next_bar', 'intrabar'}:
        execution_mode = 'next_bar'

    intrabar_submit_time = None
    signal: Optional[Signal] = None
    if execution_mode == 'intrabar' and minute_df is not None:
        intrabar_signal = find_intrabar_signal(
            current_bar_time,
            minute_df,
            pair,
            params,
            nearest_support,
            nearest_resistance,
        )
        if intrabar_signal is not None:
            signal, intrabar_submit_time = intrabar_signal

    if signal is None:
        signal = select_entry_signal(
            hourly_df=hourly_df,
            bar_idx=bar_idx,
            pair=pair,
            params=params,
            support_zone=nearest_support,
            resistance_zone=nearest_resistance,
        )
        if signal is not None and execution_mode == 'next_bar':
            intrabar_submit_time = _next_bar_submit_time(current_bar_time)
            if align_signal_time:
                signal = replace(signal, time=intrabar_submit_time)

    return signal, intrabar_submit_time


def run_walk_forward(
    hourly_df: pd.DataFrame,
    *,
    pair: str,
    params: StrategyParams,
    pip: float,
    zone_provider: Callable[[pd.Timestamp, object, int], list[SRZone]],
    execution_quote_provider: Callable[[Signal, pd.Timestamp, int, pd.Series], tuple[Optional[ExecutionQuote], str]] | None = None,
    minute_df: pd.DataFrame | None = None,
    execution_mode: str = 'intrabar',
    on_bar: Callable[[WalkForwardBar], None] | None = None,
    force_close_end: bool = True,
    is_entry_blocked: Callable[[pd.Timestamp], bool] | None = None,
    debug: bool = False,
    snapshot_source: str = '',
) -> WalkForwardResult:
    """Run the shared per-bar execution loop for one pair.

    Signals are generated from completed hourly candles and queued for execution
    on the next bar's open, which is the earliest point at which live code could
    have acted on the prior candle's close.

    *is_entry_blocked* is an optional callback ``(bar_time) -> bool`` that
    gates new entries.  When provided and returning True, signal generation
    is skipped for that bar.  Use this to inject sequencing constraints
    (e.g. cooldown between trades) without coupling portfolio policy into
    the walk-forward engine.
    """

    debug = bool(debug) or _walk_debug_enabled()
    _snap = bool(snapshot_source and params.trade_snapshot_logging)

    def _dbg(message: str) -> None:
        if debug:
            print(f'    [DEBUG] {message}')

    total_rows = len(hourly_df)
    zone_lookups = 0
    zone_lookup_elapsed = 0.0
    minute_exit_checks = 0
    hourly_exit_checks = 0
    minute_exit_elapsed = 0.0
    hourly_exit_elapsed = 0.0
    signals_seen = 0
    entries_started = 0
    entry_signals = 0
    entry_total_elapsed = 0.0
    exits_finalized = 0
    signal_scan_elapsed = 0.0
    signal_quote_elapsed = 0.0
    signal_plan_elapsed = 0.0
    pending_quote_elapsed = 0.0
    pending_plan_elapsed = 0.0
    pending_activations = 0
    blocked_signals = 0
    t_start = time.perf_counter()

    _dbg(
        f'step=walk_start pair={pair} bars={total_rows} execution_mode={execution_mode} '
        f'force_close_end={force_close_end}'
    )

    trades: list[Trade] = []
    current_trade: Optional[Trade] = None
    pending_signal: Optional[Signal] = None
    pending_signal_submit_time: Optional[pd.Timestamp] = None
    current_zones: list[SRZone] = []
    last_zone_date = None
    trade_entry_bar = 0
    minute_index = None
    if minute_df is not None and not minute_df.empty:
        if not minute_df.index.is_monotonic_increasing:
            minute_df = minute_df.sort_index()
        minute_index = minute_df.index

    for i in range(len(hourly_df)):
        row = hourly_df.iloc[i]
        current_time = hourly_df.index[i]
        current_date = current_time.date() if hasattr(current_time, 'date') else current_time

        if last_zone_date is None or str(current_date) != str(last_zone_date):
            t_zone = time.perf_counter()
            current_zones = list(zone_provider(current_time, current_date, i) or [])
            last_zone_date = current_date
            zone_lookups += 1
            zone_lookup_elapsed += time.perf_counter() - t_zone
            if debug and (zone_lookups <= 3 or zone_lookups % 25 == 0):
                _dbg(
                    f'step=walk_zone_provider pair={pair} zone_lookups={zone_lookups} '
                    f'elapsed={zone_lookup_elapsed:.2f}s'
                )

        current_price = float(row['Close'])
        nearest_support, nearest_resistance = get_tradeable_zones(current_zones, current_price)

        signal: Optional[Signal] = None
        opened_trade: Optional[Trade] = None
        exit_trade: Optional[Trade] = None

        if current_trade is not None:
            bars_held = i - trade_entry_bar
            hourly_exit_checks += 1

            # --- Minute-resolution TP/SL check ---
            # best_favorable_price is updated inside the helper per minute bar.
            t_minute_exit = time.perf_counter()
            minute_result = _check_exit_minute_resolution(
                current_trade,
                minute_df,
                current_time,
                bars_held,
                params,
                pip,
                minute_index=minute_index,
            )
            minute_exit_checks += 1
            minute_exit_elapsed += time.perf_counter() - t_minute_exit
            if minute_result:
                exit_reason, exit_price, exit_time = minute_result
            else:
                # Ensure best_favorable_price reflects the hourly bar even
                # when no minute data was available (fallback path).
                t_hourly_exit = time.perf_counter()
                if current_trade.direction == 'LONG':
                    bar_best = float(row['High'])
                    if current_trade.best_favorable_price is None or bar_best > current_trade.best_favorable_price:
                        current_trade.best_favorable_price = bar_best
                else:
                    bar_best = float(row['Low'])
                    if current_trade.best_favorable_price is None or bar_best < current_trade.best_favorable_price:
                        current_trade.best_favorable_price = bar_best

                exit_time = current_time
                result = check_exit(
                    current_trade,
                    bar_high=row['High'],
                    bar_low=row['Low'],
                    bar_close=row['Close'],
                    bar_time=current_time,
                    bars_held=bars_held,
                    params=params,
                    pip=pip,
                )
                if result:
                    exit_reason, exit_price = result
                else:
                    exit_reason = None
                hourly_exit_elapsed += time.perf_counter() - t_hourly_exit

            if exit_reason is not None:
                exits_finalized += 1
                exit_trade = finalize_trade(
                    current_trade,
                    exit_time,
                    exit_price,
                    exit_reason,
                    bars_held,
                    pip,
                )
                trades.append(exit_trade)
                if _snap:
                    _write_trade_snapshot(
                        source=snapshot_source, event='exit', pair=pair,
                        timestamp=exit_time, signal=None, trade=exit_trade,
                        bar_index=i, bar_time=current_time, bar_row=row,
                        bars_held=bars_held, nearest_support=nearest_support,
                        nearest_resistance=nearest_resistance,
                        exit_reason=exit_reason, exit_price=exit_price,
                    )
                current_trade = None
                if on_bar is not None:
                    on_bar(
                        WalkForwardBar(
                            bar_index=i,
                            bar_time=current_time,
                            row=row,
                            zones=current_zones,
                            support_zone=nearest_support,
                            resistance_zone=nearest_resistance,
                            signal=None,
                            opened_trade=None,
                            exit_trade=exit_trade,
                            open_trade=None,
                            bars_held=0,
                        )
                    )
                continue

        if current_trade is None and pending_signal is not None:
            pending_activations += 1
            quote_note = ''
            submit_time = pd.Timestamp(current_time) if execution_mode == 'next_bar' else None
            if submit_time is None:
                submit_time = (
                    pending_signal_submit_time
                    if pending_signal_submit_time is not None
                    else intrabar_execution_time(pending_signal.time, minute_df)
                )
            should_skip_final_execution = (
                force_close_end
                and i == len(hourly_df) - 1
                and submit_time >= current_time + pd.Timedelta(hours=1)
            )
            execution_plan = None
            quote = None
            if not should_skip_final_execution:
                t_pending_total = time.perf_counter()
                if execution_quote_provider is not None:
                    t_pending_exec = time.perf_counter()
                    quote, quote_note = execution_quote_provider(
                        pending_signal,
                        submit_time,
                        i,
                        row,
                    )
                    pending_quote_elapsed += time.perf_counter() - t_pending_exec
                else:
                    quote = build_modeled_execution_quote(
                        pending_signal.pair,
                        float(row['Open']),
                        submit_time,
                        params,
                        source='historical_1h_fallback',
                    )

                if quote is not None:
                    t_pending_plan = time.perf_counter()
                    execution_plan, quote_note = build_execution_plan(
                        pending_signal,
                        quote,
                        params,
                        now=submit_time,
                    )
                    pending_plan_elapsed += time.perf_counter() - t_pending_plan

                    if execution_plan is not None:
                        current_trade = build_trade_from_signal(
                            pending_signal,
                            entry_price=execution_plan.entry_price,
                            entry_time=execution_plan.quote.captured_at,
                            sl_price=execution_plan.stop_price,
                            tp_price=execution_plan.take_profit_price,
                        )
                    else:
                        current_trade = None
                    if current_trade is not None:
                        _dbg(
                            f'step=walk_pending_entry pair={pair} bar={i} '
                            f'elapsed={time.perf_counter() - t_pending_total:.4f}s quote_note={quote_note}'
                        )

            if current_trade is not None:
                _entry_signal = pending_signal  # capture before clearing
                del quote_note
                pending_signal = None
                pending_signal_submit_time = None
                trade_entry_bar = i
                opened_trade = current_trade
                if _snap:
                    _write_trade_snapshot(
                        source=snapshot_source, event='entry', pair=pair,
                        timestamp=current_time, signal=_entry_signal, trade=current_trade,
                        quote=quote, execution_plan=execution_plan,
                        bar_index=i, bar_time=current_time, bar_row=row,
                        nearest_support=nearest_support, nearest_resistance=nearest_resistance,
                    )

        if opened_trade is not None:
            t_open_minute = time.perf_counter()
            minute_result = _check_exit_minute_resolution(
                opened_trade,
                minute_df,
                current_time,
                0,
                params,
                pip,
                minute_index=minute_index,
            )
            minute_exit_checks += 1
            minute_exit_elapsed += time.perf_counter() - t_open_minute
            if minute_result:
                exit_reason, exit_price, exit_time = minute_result
            else:
                hourly_exit_checks += 1
                t_open_hourly = time.perf_counter()
                exit_time = current_time
                result = check_exit(
                    opened_trade,
                    bar_high=row['High'],
                    bar_low=row['Low'],
                    bar_close=row['Close'],
                    bar_time=current_time,
                    bars_held=0,
                    params=params,
                    pip=pip,
                )
                if result:
                    exit_reason, exit_price = result
                else:
                    exit_reason = None
                hourly_exit_elapsed += time.perf_counter() - t_open_hourly

            if exit_reason is not None:
                exit_trade = finalize_trade(
                    opened_trade,
                    exit_time,
                    exit_price,
                    exit_reason,
                    0,
                    pip,
                )
                trades.append(exit_trade)
                if _snap:
                    _write_trade_snapshot(
                        source=snapshot_source, event='exit', pair=pair,
                        timestamp=exit_time, signal=None, trade=exit_trade,
                        bar_index=i, bar_time=current_time, bar_row=row,
                        bars_held=0, nearest_support=nearest_support,
                        nearest_resistance=nearest_resistance,
                        exit_reason=exit_reason, exit_price=exit_price,
                    )
                current_trade = None
                if on_bar is not None:
                    on_bar(
                        WalkForwardBar(
                            bar_index=i,
                            bar_time=current_time,
                            row=row,
                            zones=current_zones,
                            support_zone=nearest_support,
                            resistance_zone=nearest_resistance,
                            signal=None,
                            opened_trade=opened_trade,
                            exit_trade=exit_trade,
                            open_trade=None,
                            bars_held=0,
                        )
                    )
                continue

        if current_trade is None and not (
            is_entry_blocked is not None and is_entry_blocked(current_time)
        ):
            t_signal = time.perf_counter()
            signal, intrabar_submit_time = resolve_entry_signal_for_bar(
                hourly_df=hourly_df,
                bar_idx=i,
                pair=pair,
                params=params,
                current_bar_time=current_time,
                nearest_support=nearest_support,
                nearest_resistance=nearest_resistance,
                execution_mode=execution_mode,
                minute_df=minute_df,
                align_signal_time=False,
            )
            signal_scan_elapsed += time.perf_counter() - t_signal
            if signal is not None:
                signals_seen += 1
                if execution_mode == 'intrabar':
                    submit_time = (
                        intrabar_submit_time
                        if intrabar_submit_time is not None
                        else intrabar_execution_time(current_time, minute_df)
                    )
                else:
                    submit_time = _next_bar_submit_time(current_time)

                should_skip_final_execution = (
                    force_close_end
                    and i == len(hourly_df) - 1
                    and submit_time >= current_time + pd.Timedelta(hours=1)
                )
                execution_plan = None
                quote = None
                if not should_skip_final_execution:
                    entry_signals += 1
                    t_signal_total = time.perf_counter()
                    if execution_quote_provider is not None:
                        t_signal_exec = time.perf_counter()
                        quote, _quote_note = execution_quote_provider(
                            signal,
                            submit_time,
                            i,
                            row,
                        )
                        signal_quote_elapsed += time.perf_counter() - t_signal_exec
                    else:
                        quote = build_modeled_execution_quote(
                            signal.pair,
                            float(row['Open']),
                            submit_time,
                            params,
                            source='historical_1m',
                        )

                    if quote is not None:
                        t_signal_plan = time.perf_counter()
                        execution_plan, _ = build_execution_plan(
                            signal,
                            quote,
                            params,
                            now=submit_time,
                        )
                        signal_plan_elapsed += time.perf_counter() - t_signal_plan

                    if execution_plan is not None:
                        current_trade = build_trade_from_signal(
                            signal,
                            entry_price=execution_plan.entry_price,
                            entry_time=execution_plan.quote.captured_at,
                            sl_price=execution_plan.stop_price,
                            tp_price=execution_plan.take_profit_price,
                        )
                        trade_entry_bar = i
                        opened_trade = current_trade
                        entries_started += 1
                        if _snap:
                            _write_trade_snapshot(
                                source=snapshot_source, event='entry', pair=pair,
                                timestamp=current_time, signal=signal, trade=current_trade,
                                quote=quote, execution_plan=execution_plan,
                                bar_index=i, bar_time=current_time, bar_row=row,
                                nearest_support=nearest_support, nearest_resistance=nearest_resistance,
                            )
                        if debug:
                            _dbg(
                                f'step=walk_entry pair={pair} bar={i} '
                                f'submit_time={submit_time} elapsed={time.perf_counter() - t_signal_total:.4f}s'
                            )
                        if (time.perf_counter() - t_signal_total) >= 0.01:
                            _dbg(
                                f'step=walk_entry_slow pair={pair} bar={i} '
                                f'submit_time={submit_time} elapsed={time.perf_counter() - t_signal_total:.4f}s'
                            )
                        entry_total_elapsed += time.perf_counter() - t_signal_total
                else:
                    if debug:
                        _dbg(
                            f'step=walk_signal_defer pair={pair} bar={i} '
                            f'submit_time={submit_time} reason=forced final-bar defer'
                        )
                    pending_signal = signal
                    pending_signal_submit_time = submit_time
            else:
                pending_signal = signal
                blocked_signals += 1

            if debug and (i == 0 or i == total_rows - 1 or i % 200 == 0):
                _dbg(
                    f'step=walk_signal_scan pair={pair} bar={i} elapsed={time.perf_counter() - t_signal:.4f}s '
                    f'signals_seen={signals_seen} entries={entries_started} exits={exits_finalized}'
                )
        elif current_trade is None and is_entry_blocked is not None and is_entry_blocked(current_time):
            blocked_signals += 1

        bars_held = 0 if current_trade is None else i - trade_entry_bar
        if debug and (i == 0 or i == total_rows - 1 or i % 500 == 0):
            _dbg(
                f'step=walk_progress pair={pair} bar={i}/{total_rows} '
                f'open={current_trade is not None} pending={pending_signal is not None} '
                f'elapsed={time.perf_counter() - t_start:.2f}s'
            )
        if on_bar is not None:
            on_bar(
                WalkForwardBar(
                    bar_index=i,
                    bar_time=current_time,
                    row=row,
                    zones=current_zones,
                    support_zone=nearest_support,
                    resistance_zone=nearest_resistance,
                    signal=signal,
                    opened_trade=opened_trade,
                    exit_trade=None,
                    open_trade=current_trade,
                    bars_held=bars_held,
                )
            )

    if force_close_end and current_trade is not None:
        t_close = time.perf_counter()
        end_exit = finalize_trade(
            current_trade,
            hourly_df.index[-1],
            get_market_exit_price(
                float(hourly_df['Close'].iloc[-1]),
                current_trade.direction,
                pip,
                params,
            ),
            'END',
            len(hourly_df) - 1 - trade_entry_bar,
            pip,
        )
        trades.append(end_exit)
        exits_finalized += 1
        if _snap:
            _write_trade_snapshot(
                source=snapshot_source, event='exit', pair=pair,
                timestamp=hourly_df.index[-1], signal=None, trade=end_exit,
                bar_index=len(hourly_df) - 1, bar_time=hourly_df.index[-1],
                bar_row=hourly_df.iloc[-1],
                bars_held=len(hourly_df) - 1 - trade_entry_bar,
                exit_reason='END', exit_price=end_exit.exit_price,
            )
        current_trade = None
        if debug:
            _dbg(f'step=walk_force_close_exit pair={pair} elapsed={time.perf_counter() - t_close:.4f}s')

    _dbg(
        f'step=walk_end pair={pair} total_elapsed={time.perf_counter() - t_start:.2f}s '
        f'rows={total_rows} zone_lookups={zone_lookups} zone_lookup_elapsed={zone_lookup_elapsed:.2f}s '
        f'hourly_exit_checks={hourly_exit_checks} minute_exit_checks={minute_exit_checks} '
        f'hourly_exit_elapsed={hourly_exit_elapsed:.2f}s minute_exit_elapsed={minute_exit_elapsed:.2f}s '
        f'signal_scan_elapsed={signal_scan_elapsed:.4f}s signal_quote_elapsed={signal_quote_elapsed:.4f}s '
        f'signal_plan_elapsed={signal_plan_elapsed:.4f}s entry_signal_count={entry_signals} '
        f'entry_signal_elapsed={entry_total_elapsed:.4f}s '
        f'entry_signal_avg={entry_total_elapsed / max(entry_signals, 1):.4f}s '
        f'pending_quote_elapsed={pending_quote_elapsed:.4f}s '
        f'pending_plan_elapsed={pending_plan_elapsed:.4f}s '
        f'signals={signals_seen} entries={entries_started} exits={exits_finalized} '
        f'pending_hits={pending_activations} blocked={blocked_signals}'
    )

    return WalkForwardResult(trades=trades, zones=current_zones, open_trade=current_trade)
