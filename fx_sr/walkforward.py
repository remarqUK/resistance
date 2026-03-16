"""Shared walk-forward execution helpers for backtest and replay."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

import pandas as pd

from .execution import build_execution_plan, build_modeled_execution_quote
from .ibkr import ExecutionQuote
from .levels import SRZone
from .portfolio import is_pair_cooldown_active
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
) -> Trade:
    """Populate final trade state and derived P&L metrics."""

    trade.exit_time = exit_time
    trade.exit_price = float(exit_price)
    trade.exit_reason = exit_reason
    trade.bars_held = bars_held

    if trade.direction == 'LONG':
        trade.pnl_pips = (trade.exit_price - trade.entry_price) / pip
        if trade.risk > 0:
            trade.pnl_r = (trade.exit_price - trade.entry_price) / trade.risk
    else:
        trade.pnl_pips = (trade.entry_price - trade.exit_price) / pip
        if trade.risk > 0:
            trade.pnl_r = (trade.entry_price - trade.exit_price) / trade.risk

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


def run_walk_forward(
    hourly_df: pd.DataFrame,
    *,
    pair: str,
    params: StrategyParams,
    pip: float,
    zone_provider: Callable[[pd.Timestamp, object, int], list[SRZone]],
    execution_quote_provider: Callable[[Signal, pd.Timestamp, int, pd.Series], tuple[Optional[ExecutionQuote], str]] | None = None,
    on_bar: Callable[[WalkForwardBar], None] | None = None,
    force_close_end: bool = True,
    skip_execution_plan: bool = False,
) -> WalkForwardResult:
    """Run the shared per-bar execution loop for one pair.

    Signals are generated from completed hourly candles and queued for execution
    on the next bar's open, which is the earliest point at which live code could
    have acted on the prior candle's close.
    """

    trades: list[Trade] = []
    current_trade: Optional[Trade] = None
    pending_signal: Optional[Signal] = None
    current_zones: list[SRZone] = []
    last_trade_exit_time: pd.Timestamp | None = None
    last_trade_pnl_r: float | None = None
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
                last_trade_exit_time = current_time
                last_trade_pnl_r = exit_trade.pnl_r
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
            if execution_quote_provider is not None:
                quote, quote_note = execution_quote_provider(
                    pending_signal,
                    current_time,
                    i,
                    row,
                )
            else:
                quote = build_modeled_execution_quote(
                    pending_signal.pair,
                    float(row['Open']),
                    current_time,
                    params,
                    source='historical_1h_fallback',
                )

            if skip_execution_plan:
                current_trade = build_trade_from_signal(pending_signal)
            else:
                execution_plan = None
                if quote is not None:
                    execution_plan, quote_note = build_execution_plan(
                        pending_signal,
                        quote,
                        params,
                        now=current_time,
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

            del quote_note
            pending_signal = None
            if current_trade is not None:
                trade_entry_bar = i
                opened_trade = current_trade

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
                last_trade_exit_time = current_time
                last_trade_pnl_r = exit_trade.pnl_r
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

        if current_trade is None and not is_pair_cooldown_active(
            current_time,
            last_exit_time=last_trade_exit_time,
            last_pnl_r=last_trade_pnl_r,
            params=params,
        ):
            signal = select_entry_signal(
                hourly_df=hourly_df,
                bar_idx=i,
                pair=pair,
                params=params,
                support_zone=nearest_support,
                resistance_zone=nearest_resistance,
            )
            if signal is not None:
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
        trades.append(
            finalize_trade(
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
        )
        current_trade = None

    return WalkForwardResult(trades=trades, zones=current_zones, open_trade=current_trade)
