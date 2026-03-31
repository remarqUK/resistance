"""Shared walk-forward execution helpers for backtest and replay."""

from __future__ import annotations

import json
import os
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
    get_market_exit_price,
    get_tradeable_zones,
    select_entry_signal,
)
from .intrabar import find_intrabar_signal, intrabar_execution_time


_LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')


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
    if execution_mode == 'intrabar':
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

    _snap = bool(snapshot_source and params.trade_snapshot_logging)

    trades: list[Trade] = []
    current_trade: Optional[Trade] = None
    pending_signal: Optional[Signal] = None
    pending_signal_submit_time: Optional[pd.Timestamp] = None
    current_zones: list[SRZone] = []
    last_zone_date = None
    trade_entry_bar = 0

    for i in range(len(hourly_df)):
        row = hourly_df.iloc[i]
        current_time = hourly_df.index[i]
        current_date = current_time.date() if hasattr(current_time, 'date') else current_time

        if last_zone_date is None or str(current_date) != str(last_zone_date):
            current_zones = list(zone_provider(current_time, current_date, i) or [])
            last_zone_date = current_date

        current_price = float(row['Close'])
        nearest_support, nearest_resistance = get_tradeable_zones(current_zones, current_price)

        signal: Optional[Signal] = None
        opened_trade: Optional[Trade] = None
        exit_trade: Optional[Trade] = None

        if current_trade is not None:
            # Track best favorable price for break-even stop logic
            if current_trade.direction == 'LONG':
                bar_best = float(row['High'])
                if current_trade.best_favorable_price is None or bar_best > current_trade.best_favorable_price:
                    current_trade.best_favorable_price = bar_best
            else:
                bar_best = float(row['Low'])
                if current_trade.best_favorable_price is None or bar_best < current_trade.best_favorable_price:
                    current_trade.best_favorable_price = bar_best

            bars_held = i - trade_entry_bar
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
                exit_trade = finalize_trade(
                    current_trade,
                    current_time,
                    exit_price,
                    exit_reason,
                    bars_held,
                    pip,
                )
                trades.append(exit_trade)
                if _snap:
                    _write_trade_snapshot(
                        source=snapshot_source, event='exit', pair=pair,
                        timestamp=current_time, signal=None, trade=exit_trade,
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
            quote_note = ''
            submit_time = pd.Timestamp(current_time) if execution_mode == 'next_bar' else None
            if submit_time is None:
                submit_time = (
                    pending_signal_submit_time
                    if pending_signal_submit_time is not None
                    else intrabar_execution_time(pending_signal.time, minute_df)
                )
            if execution_quote_provider is not None:
                quote, quote_note = execution_quote_provider(
                    pending_signal,
                    submit_time,
                    i,
                    row,
                )
            else:
                quote = build_modeled_execution_quote(
                    pending_signal.pair,
                    float(row['Open']),
                    submit_time,
                    params,
                    source='historical_1h_fallback',
                )

            execution_plan = None
            if quote is not None:
                execution_plan, quote_note = build_execution_plan(
                    pending_signal,
                    quote,
                    params,
                    now=submit_time,
                )

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

            _entry_signal = pending_signal  # capture before clearing
            del quote_note
            pending_signal = None
            pending_signal_submit_time = None
            if current_trade is not None:
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
                exit_trade = finalize_trade(
                    opened_trade,
                    current_time,
                    exit_price,
                    exit_reason,
                    0,
                    pip,
                )
                trades.append(exit_trade)
                if _snap:
                    _write_trade_snapshot(
                        source=snapshot_source, event='exit', pair=pair,
                        timestamp=current_time, signal=None, trade=exit_trade,
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

            if signal is not None:
                if execution_mode == 'intrabar':
                    submit_time = (
                        intrabar_submit_time
                        if intrabar_submit_time is not None
                        else intrabar_execution_time(current_time, minute_df)
                    )
                else:
                    submit_time = _next_bar_submit_time(current_time)

                if execution_quote_provider is not None:
                    quote, _quote_note = execution_quote_provider(
                        signal,
                        submit_time,
                        i,
                        row,
                    )
                else:
                    quote = build_modeled_execution_quote(
                        signal.pair,
                        float(row['Open']),
                        submit_time,
                        params,
                        source='historical_1m',
                    )

                execution_plan = None
                if quote is not None:
                    execution_plan, _ = build_execution_plan(
                        signal,
                        quote,
                        params,
                        now=submit_time,
                    )

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
                    if _snap:
                        _write_trade_snapshot(
                            source=snapshot_source, event='entry', pair=pair,
                            timestamp=current_time, signal=signal, trade=current_trade,
                            quote=quote, execution_plan=execution_plan,
                            bar_index=i, bar_time=current_time, bar_row=row,
                            nearest_support=nearest_support, nearest_resistance=nearest_resistance,
                        )
                else:
                    pending_signal = signal
                    pending_signal_submit_time = submit_time
            else:
                pending_signal = signal

        bars_held = 0 if current_trade is None else i - trade_entry_bar
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

    return WalkForwardResult(trades=trades, zones=current_zones, open_trade=current_trade)
