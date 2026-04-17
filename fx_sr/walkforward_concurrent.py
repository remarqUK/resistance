"""Walk-forward variant that supports multiple concurrent trades per pair.

STATUS: WORK-IN-PROGRESS — DO NOT WIRE INTO PRODUCTION.

A cap=1 parity check on EURUSD/120d (tmp_wfc_parity.py) showed 120 vs
123 trades with differing exit reasons on the same entry bars. The port
reuses the legacy helpers but there is a subtle order-of-operations or
state-mutation bug between here and run_walk_forward. Until diagnosed,
this function is gated off — run_backtest_fast always uses legacy.

Sibling to ``run_walk_forward`` in ``walkforward.py``. Intended to reuse
the exact same exit mechanics (``check_exit``, ``check_partial_close``,
minute-resolution exit scanning), execution plan builder, and trade
finalizer, so per-trade P&L and partial-close behaviour is identical.

Differences from ``run_walk_forward``:
- Maintains a list of open trades instead of a singleton. Up to
  ``params.max_concurrent_per_pair`` trades can be live simultaneously.
- Rejects a new entry if its zone fingerprint matches any currently-
  open trade (avoids opening two trades on the same zone).
- Per-bar: iterates open trades through exit/partial/trailing in the
  same order they opened. Any that closes is finalized; remainders
  continue as legacy.
- A single pending-signal slot is maintained (same as legacy). Once
  activated it goes into the next available concurrent slot. So per
  bar you still only ADD one new trade, but you can CARRY multiple.

This function is dispatched to by ``run_backtest`` when
``params.max_concurrent_per_pair > 1``. When the param is 1 (default),
the legacy ``run_walk_forward`` is used and this module is not touched.

Scope intentionally does NOT include:
- ``initial_state`` resume (multi-trade checkpointing is out of scope)
- ``on_bar`` callback (replay needs a list, not a singleton — different API)
- ``is_entry_blocked`` callback (cooldown policy would need per-trade
  semantics that weren't in the prototype)
The caller wires these only when max_concurrent_per_pair == 1.
"""

from __future__ import annotations

import copy
import time
from dataclasses import replace
from typing import Callable, Optional

import pandas as pd

from .atr import compute_atr
from .execution import build_execution_plan, build_modeled_execution_quote
from .ibkr import ExecutionQuote
from .intrabar import intrabar_execution_time
from .levels import SRZone
from .strategy import (
    Signal,
    StrategyParams,
    Trade,
    build_trade_from_signal,
    check_exit,
    check_partial_close,
    get_market_exit_price,
    get_tradeable_zones_permissive,
)
from .walkforward import (
    WalkForwardResult,
    WalkForwardState,
    _check_exit_minute_resolution,
    _compute_daily_hash,
    _compute_minute_hash,
    _compute_params_hash,
    _compute_prefix_hash,
    _next_bar_submit_time,
    finalize_trade,
    resolve_entry_signal_for_bar,
)


def _zone_fingerprint(zone_upper: float, zone_lower: float, zone_type: str) -> tuple:
    return (zone_type, round(float(zone_lower), 5), round(float(zone_upper), 5))


def _trade_zone_fp(trade: Trade) -> tuple:
    return _zone_fingerprint(trade.zone_upper, trade.zone_lower, 'support' if trade.direction == 'LONG' else 'resistance')


def run_walk_forward_concurrent(
    hourly_df: pd.DataFrame,
    *,
    pair: str,
    params: StrategyParams,
    pip: float,
    zone_provider: Callable[[pd.Timestamp, object, int], list[SRZone]],
    execution_quote_provider: Callable[[Signal, pd.Timestamp, int, pd.Series], tuple[Optional[ExecutionQuote], str]] | None = None,
    minute_df: pd.DataFrame | None = None,
    execution_mode: str = 'intrabar',
    force_close_end: bool = True,
    debug: bool = False,
    daily_df: pd.DataFrame | None = None,
) -> WalkForwardResult:
    """Multi-concurrent-trade variant of run_walk_forward.

    All P&L, partial close, trailing, and minute-resolution exit mechanics
    are identical to the legacy function. Only the open-trade structure
    and entry gating differ.
    """

    cap = max(1, int(params.max_concurrent_per_pair))

    atr_series = (
        compute_atr(hourly_df, period=params.atr_period)
        if params.sl_mode == 'atr' or params.trailing_mode == 'atr' else None
    )

    minute_index = None
    if minute_df is not None and not minute_df.empty:
        if not minute_df.index.is_monotonic_increasing:
            minute_df = minute_df.sort_index()
        minute_index = minute_df.index

    # Each open-slot element: {trade, entry_bar}. Zone fingerprint is derived
    # from trade.zone_* when we need to dedup.
    open_slots: list[dict] = []

    trades: list[Trade] = []
    pending_signal: Optional[Signal] = None
    pending_signal_submit_time: Optional[pd.Timestamp] = None
    current_zones: list[SRZone] = []
    last_zone_date = None

    t_start = time.perf_counter()

    def _process_slot_exit(
        slot: dict, i: int, current_time: pd.Timestamp, row: pd.Series,
    ) -> tuple[Optional[str], Optional[float], Optional[pd.Timestamp]]:
        """Run the identical exit/partial check sequence as run_walk_forward
        for one open slot. Returns (exit_reason, exit_price, exit_time) or
        (None, None, None) if the slot stays open. Mutates the slot's trade
        and handles partial-close splitting inline (appends to trades and
        mutates the slot for the remainder)."""

        trade = slot['trade']
        bars_held = i - slot['entry_bar']

        minute_result = _check_exit_minute_resolution(
            trade, minute_df, current_time, bars_held,
            params, pip, minute_index=minute_index,
        )
        if minute_result:
            return minute_result

        # Hourly fallback — same as run_walk_forward lines 618-648
        if trade.direction == 'LONG':
            bar_best = float(row['High'])
            if trade.best_favorable_price is None or bar_best > trade.best_favorable_price:
                trade.best_favorable_price = bar_best
        else:
            bar_best = float(row['Low'])
            if trade.best_favorable_price is None or bar_best < trade.best_favorable_price:
                trade.best_favorable_price = bar_best

        partial_price = check_partial_close(
            trade, float(row['High']), float(row['Low']), params,
        )
        if partial_price is not None:
            return ('PARTIAL_TP', partial_price, current_time)

        result = check_exit(
            trade, bar_high=row['High'], bar_low=row['Low'], bar_close=row['Close'],
            bar_time=current_time, bars_held=bars_held, params=params, pip=pip,
        )
        if result:
            exit_reason, exit_price = result
            return (exit_reason, exit_price, current_time)
        return (None, None, None)

    def _handle_slot_partial(
        slot: dict, exit_time: pd.Timestamp, exit_price: float, i: int, current_time: pd.Timestamp,
    ) -> tuple[Optional[str], Optional[float], Optional[pd.Timestamp]]:
        """Split-trade handling mirrored from run_walk_forward lines 651-682."""
        trade = slot['trade']
        bars_held = i - slot['entry_bar']
        group_id = trade.trade_group_id or f'{trade.entry_time}_{pair}'
        closed_portion = copy.copy(trade)
        closed_portion.trade_group_id = group_id
        closed_portion.position_fraction = params.partial_close_fraction
        closed_trade = finalize_trade(
            closed_portion, exit_time, exit_price, 'PARTIAL_TP', bars_held, pip,
        )
        trades.append(closed_trade)
        # Mutate the remaining portion to live trade
        trade.trade_group_id = group_id
        trade.position_fraction = 1.0 - params.partial_close_fraction
        trade.is_remainder = True
        if trade.direction == 'LONG':
            trade.sl_price = max(trade.entry_price, trade.sl_price)
        else:
            trade.sl_price = min(trade.entry_price, trade.sl_price)

        # Re-check remainder against remaining minutes in this hour
        remainder_result = _check_exit_minute_resolution(
            trade, minute_df, current_time, bars_held,
            params, pip, minute_index=minute_index, start_after=exit_time,
        )
        if remainder_result:
            return remainder_result
        return (None, None, None)

    for i in range(len(hourly_df)):
        row = hourly_df.iloc[i]
        current_time = hourly_df.index[i]
        current_date = current_time.date() if hasattr(current_time, 'date') else current_time

        if last_zone_date is None or str(current_date) != str(last_zone_date):
            current_zones = list(zone_provider(current_time, current_date, i) or [])
            last_zone_date = current_date

        current_price = float(row['Close'])
        nearest_support, nearest_resistance = get_tradeable_zones_permissive(
            current_zones, current_price, allow_minor=params.allow_minor_zones,
        )

        # --- Step 1: process existing open slots for exits ---
        survivors: list[dict] = []
        for slot in open_slots:
            exit_reason, exit_price, exit_time = _process_slot_exit(slot, i, current_time, row)
            if exit_reason == 'PARTIAL_TP':
                # Split — finalize closed half, keep remainder in this slot
                exit_reason, exit_price, exit_time = _handle_slot_partial(
                    slot, exit_time, exit_price, i, current_time,
                )
            if exit_reason is not None:
                exit_trade = finalize_trade(
                    slot['trade'], exit_time, exit_price, exit_reason,
                    i - slot['entry_bar'], pip,
                )
                trades.append(exit_trade)
                # Do NOT append slot back — it's closed.
            else:
                survivors.append(slot)
        open_slots = survivors

        # --- Step 2: activate a pending signal if we have room and zone is free ---
        room = len(open_slots) < cap
        if room and pending_signal is not None:
            submit_time = (
                pd.Timestamp(current_time) if execution_mode == 'next_bar'
                else (pending_signal_submit_time or intrabar_execution_time(pending_signal.time, minute_df))
            )
            should_skip_final = (
                force_close_end and i == len(hourly_df) - 1
                and submit_time >= current_time + pd.Timedelta(hours=1)
            )
            execution_plan = None
            if not should_skip_final:
                if execution_quote_provider is not None:
                    quote, _note = execution_quote_provider(pending_signal, submit_time, i, row)
                else:
                    quote = build_modeled_execution_quote(
                        pending_signal.pair, float(row['Open']), submit_time, params,
                        source='historical_1h_fallback',
                    )
                if quote is not None:
                    execution_plan, _ = build_execution_plan(pending_signal, quote, params, now=submit_time)

            if execution_plan is not None:
                signal_fp = _zone_fingerprint(
                    pending_signal.zone_upper, pending_signal.zone_lower, pending_signal.zone_type,
                )
                if not any(_trade_zone_fp(s['trade']) == signal_fp for s in open_slots):
                    new_trade = build_trade_from_signal(
                        pending_signal,
                        entry_price=execution_plan.entry_price,
                        entry_time=execution_plan.quote.captured_at,
                        sl_price=execution_plan.stop_price,
                        tp_price=execution_plan.take_profit_price,
                    )
                    _bar_atr = float(atr_series.iloc[i]) if atr_series is not None and i < len(atr_series) else 0.0
                    new_trade.entry_atr = 0.0 if _bar_atr != _bar_atr else _bar_atr
                    # Entry-bar immediate exit scan (mirrors run_walk_forward lines 804-876)
                    entry_slot = {'trade': new_trade, 'entry_bar': i}
                    exit_reason, exit_price, exit_time = _process_slot_exit(entry_slot, i, current_time, row)
                    if exit_reason == 'PARTIAL_TP':
                        exit_reason, exit_price, exit_time = _handle_slot_partial(
                            entry_slot, exit_time, exit_price, i, current_time,
                        )
                    if exit_reason is not None:
                        exit_trade = finalize_trade(
                            new_trade, exit_time, exit_price, exit_reason, 0, pip,
                        )
                        trades.append(exit_trade)
                    else:
                        open_slots.append(entry_slot)
                # Whether or not we admitted the trade, the pending signal
                # has been tried for this bar. Clearing it matches legacy's
                # behavior of consuming the pending slot on activation.
                pending_signal = None
                pending_signal_submit_time = None

        # --- Step 3: generate new signal if room after activation ---
        if len(open_slots) < cap:
            current_atr = float(atr_series.iloc[i]) if atr_series is not None and i < len(atr_series) else 0.0
            if current_atr != current_atr:
                current_atr = 0.0
            signal, intrabar_submit_time = resolve_entry_signal_for_bar(
                hourly_df=hourly_df, bar_idx=i, pair=pair, params=params,
                current_bar_time=current_time,
                nearest_support=nearest_support, nearest_resistance=nearest_resistance,
                execution_mode=execution_mode, minute_df=minute_df,
                align_signal_time=False, current_atr=current_atr,
            )
            if signal is not None:
                # Skip signals whose zone already has an open trade.
                signal_fp = _zone_fingerprint(signal.zone_upper, signal.zone_lower, signal.zone_type)
                if any(_trade_zone_fp(s['trade']) == signal_fp for s in open_slots):
                    signal = None
            if signal is not None:
                submit_time = (
                    intrabar_submit_time if intrabar_submit_time is not None
                    else (intrabar_execution_time(current_time, minute_df)
                          if execution_mode == 'intrabar' else _next_bar_submit_time(current_time))
                )
                should_skip_final = (
                    force_close_end and i == len(hourly_df) - 1
                    and submit_time >= current_time + pd.Timedelta(hours=1)
                )
                execution_plan = None
                if not should_skip_final:
                    if execution_quote_provider is not None:
                        quote, _note = execution_quote_provider(signal, submit_time, i, row)
                    else:
                        quote = build_modeled_execution_quote(
                            signal.pair, float(row['Open']), submit_time, params,
                            source='historical_1m',
                        )
                    if quote is not None:
                        execution_plan, _ = build_execution_plan(signal, quote, params, now=submit_time)

                if execution_plan is not None:
                    new_trade = build_trade_from_signal(
                        signal,
                        entry_price=execution_plan.entry_price,
                        entry_time=execution_plan.quote.captured_at,
                        sl_price=execution_plan.stop_price,
                        tp_price=execution_plan.take_profit_price,
                    )
                    new_trade.entry_atr = current_atr
                    entry_slot = {'trade': new_trade, 'entry_bar': i}
                    exit_reason, exit_price, exit_time = _process_slot_exit(entry_slot, i, current_time, row)
                    if exit_reason == 'PARTIAL_TP':
                        exit_reason, exit_price, exit_time = _handle_slot_partial(
                            entry_slot, exit_time, exit_price, i, current_time,
                        )
                    if exit_reason is not None:
                        exit_trade = finalize_trade(
                            new_trade, exit_time, exit_price, exit_reason, 0, pip,
                        )
                        trades.append(exit_trade)
                    else:
                        open_slots.append(entry_slot)
                else:
                    pending_signal = signal
                    pending_signal_submit_time = submit_time

    # --- Force close any remaining open slots at the window end ---
    if force_close_end and open_slots and len(hourly_df) > 0:
        last_ts = hourly_df.index[-1]
        last_close = float(hourly_df['Close'].iloc[-1])
        for slot in open_slots:
            trade = slot['trade']
            end_exit = finalize_trade(
                trade, last_ts,
                get_market_exit_price(last_close, trade.direction, pip, params),
                'END', len(hourly_df) - 1 - slot['entry_bar'], pip,
            )
            trades.append(end_exit)
        open_slots = []

    primary_trade = open_slots[0]['trade'] if open_slots else None

    final_state = WalkForwardState(
        trades=trades,
        current_trade=primary_trade,
        pending_signal=pending_signal,
        pending_signal_submit_time=pending_signal_submit_time,
        current_zones=current_zones,
        last_zone_date=last_zone_date,
        trade_entry_bar=open_slots[0]['entry_bar'] if open_slots else 0,
        bars_processed=len(hourly_df),
        last_bar_time=hourly_df.index[-1] if len(hourly_df) > 0 else pd.Timestamp.min,
        prefix_hash=_compute_prefix_hash(hourly_df, len(hourly_df)),
        params_hash=_compute_params_hash(params, pair, execution_mode),
        daily_hash=_compute_daily_hash(daily_df),
        minute_hash=_compute_minute_hash(minute_df),
    )

    return WalkForwardResult(
        trades=trades,
        zones=current_zones,
        open_trade=primary_trade,
        state=final_state,
    )
