"""Multi-zone walk-forward engine.

Unlike the legacy `run_walk_forward`, this engine allows multiple
concurrent open trades per pair — one per qualifying S/R zone — so it
can surface the volume boost (or quality collapse) of "trade every
valid zone in range" without needing any change to the production code.

Limitations kept intentional for prototype clarity:
- No partial close / trailing stop (just flat SL/TP and time-based exits)
- Hourly-only exit checks (no minute-resolution intrabar check)
- No portfolio-level correlation / margin enforcement
- Per-pair cap on concurrent open trades (prevents unbounded exposure)
- No spread/commission in P&L (R-multiple accounting only)

These simplifications are fine because the prototype's purpose is to
answer "can multi-zone generate 2x trade count with reasonable
expectancy?". Economics precision matters only once volume is achieved.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Iterable, Optional

import pandas as pd

from ..atr import compute_atr
from ..levels import SRZone
from ..strategy import (
    StrategyParams,
    Trade,
    check_momentum_filter,
    generate_signal,
    get_pair_pip,
    is_entry_time_blocked,
    is_pair_direction_blocked,
)

from .mz_signals import generate_breakout_signal


def _zone_fingerprint(zone: SRZone) -> tuple:
    """Stable-ish key so we don't open two simultaneous trades on the
    same zone if zone clustering produces nearly-identical zones on
    consecutive bars."""
    return (
        zone.zone_type,
        round(float(zone.lower), 5),
        round(float(zone.upper), 5),
    )


@dataclass
class MultiZoneResult:
    pair: str
    trades: list[Trade] = field(default_factory=list)
    bars: int = 0
    zone_checks: int = 0
    signals_generated: int = 0
    signals_reversal: int = 0
    signals_breakout: int = 0
    entries_blocked_cap: int = 0
    entries_blocked_duplicate: int = 0
    entries_blocked_cooldown: int = 0


def _hour_in_blocked(ts: pd.Timestamp, params: StrategyParams) -> bool:
    return is_entry_time_blocked(ts, params)


def _close_trade(
    trade: Trade,
    exit_time: pd.Timestamp,
    exit_price: float,
    exit_reason: str,
    pip: float,
) -> None:
    trade.exit_time = exit_time
    trade.exit_price = float(exit_price)
    trade.exit_reason = exit_reason
    if trade.risk > 0:
        raw_move = (
            float(exit_price) - trade.entry_price
            if trade.direction == 'LONG'
            else trade.entry_price - float(exit_price)
        )
        trade.pnl_r = raw_move / trade.risk
        trade.pnl_pips = raw_move / pip


def _check_simple_exit(
    trade: Trade,
    bar_high: float,
    bar_low: float,
    bar_close: float,
    bars_held: int,
    params: StrategyParams,
) -> Optional[tuple[str, float]]:
    """Flat hourly-bar exit check. If both TP and SL print the same bar,
    SL wins (conservative). Also applies early_exit, max_hold."""

    if trade.direction == 'LONG':
        if bar_low <= trade.sl_price and bar_high >= trade.tp_price:
            return ('SL', trade.sl_price)
        if bar_high >= trade.tp_price:
            return ('TP', trade.tp_price)
        if bar_low <= trade.sl_price:
            return ('SL', trade.sl_price)
        loss_r = ((trade.entry_price - bar_close) / trade.risk) if trade.risk > 0 else 0.0
        if bar_close < trade.zone_lower or loss_r >= params.early_exit_r:
            return ('EARLY_EXIT', bar_close)
        if bars_held >= params.max_hold_bars and bar_close <= trade.entry_price:
            return ('TIME', bar_close)
    else:
        if bar_high >= trade.sl_price and bar_low <= trade.tp_price:
            return ('SL', trade.sl_price)
        if bar_low <= trade.tp_price:
            return ('TP', trade.tp_price)
        if bar_high >= trade.sl_price:
            return ('SL', trade.sl_price)
        loss_r = ((bar_close - trade.entry_price) / trade.risk) if trade.risk > 0 else 0.0
        if bar_close > trade.zone_upper or loss_r >= params.early_exit_r:
            return ('EARLY_EXIT', bar_close)
        if bars_held >= params.max_hold_bars and bar_close >= trade.entry_price:
            return ('TIME', bar_close)
    return None


def run_multi_zone(
    pair: str,
    hourly_df: pd.DataFrame,
    zones_by_date: dict[tuple, list[SRZone]],
    params: StrategyParams,
    *,
    max_concurrent_per_pair: int = 3,
    zone_reentry_cooldown_bars: int = 0,
    enable_breakout_signals: bool = False,
    h1_zones_by_date: Optional[dict[tuple, list[SRZone]]] = None,
) -> MultiZoneResult:
    """Run the multi-zone prototype over one pair.

    Args:
        pair: pair code (e.g. 'EURUSD').
        hourly_df: hourly OHLC indexed by UTC Timestamp.
        zones_by_date: dict keyed by (pair, date_str) -> [SRZone, ...].
        params: StrategyParams.
        max_concurrent_per_pair: cap on simultaneously open trades.
        zone_reentry_cooldown_bars: if > 0, allow another trade on the
            same zone after this many bars since the previous trade on
            it closed. Default 0 = legacy behavior (no re-entries;
            same-zone signals blocked while duplicate).
        enable_breakout_signals: if True, ALSO generate breakout-type
            signals (bullish break of resistance = LONG, bearish break
            of support = SHORT). Complementary to the reversal signal.
        h1_zones_by_date: optional additional zone cache keyed like
            `zones_by_date`. Merged per-date so H1 zones compete with
            D1 zones as tradeable candidates.

    Returns:
        MultiZoneResult with the closed trade list + telemetry counters.
    """

    pip = get_pair_pip(pair)
    atr_series = compute_atr(hourly_df, period=int(params.atr_period or 14))
    result = MultiZoneResult(pair=pair)
    result.bars = len(hourly_df)

    open_trades: list[tuple[Trade, tuple, int]] = []  # (trade, zone_fp, entry_bar_idx)
    # Cooldown: zone_fingerprint -> bar index when the last trade on that zone closed.
    zone_last_closed_bar: dict[tuple, int] = {}

    for i, (ts, row) in enumerate(hourly_df.iterrows()):
        bar_open = float(row['Open'])
        bar_close = float(row['Close'])
        bar_high = float(row['High'])
        bar_low = float(row['Low'])

        # --- Step 1: check all open trades for exit on this bar ---
        still_open: list[tuple[Trade, tuple, int]] = []
        for trade, fp, entry_idx in open_trades:
            bars_held = i - entry_idx
            exit_res = _check_simple_exit(
                trade, bar_high, bar_low, bar_close, bars_held, params,
            )
            if exit_res is not None:
                reason, price = exit_res
                _close_trade(trade, ts, price, reason, pip)
                result.trades.append(trade)
                zone_last_closed_bar[fp] = i
            else:
                still_open.append((trade, fp, entry_idx))
        open_trades = still_open

        # --- Step 2: consider new entries if we have room ---
        if _hour_in_blocked(ts, params):
            continue

        date_str = str(ts.date()) if hasattr(ts, 'date') else str(ts)[:10]
        zones = list(zones_by_date.get((pair, date_str)) or [])
        if h1_zones_by_date is not None:
            zones.extend(h1_zones_by_date.get((pair, date_str)) or [])
        if not zones:
            continue

        # ATR for this bar (used by ATR-mode SL). Fall back to 0.0 when ATR NaN.
        current_atr = 0.0
        if atr_series is not None and i < len(atr_series):
            v = atr_series.iloc[i]
            if v == v:  # NaN check
                current_atr = float(v)

        open_fingerprints = {fp for _, fp, _ in open_trades}

        for zone in zones:
            if len(open_trades) >= max_concurrent_per_pair:
                result.entries_blocked_cap += 1
                break
            result.zone_checks += 1
            fp = _zone_fingerprint(zone)
            if fp in open_fingerprints:
                result.entries_blocked_duplicate += 1
                continue
            # Cooldown gate — allow re-entry on a zone only after the
            # configured number of bars since the previous trade closed.
            if zone_reentry_cooldown_bars > 0:
                last_closed = zone_last_closed_bar.get(fp)
                if last_closed is not None and (i - last_closed) < zone_reentry_cooldown_bars:
                    result.entries_blocked_cooldown += 1
                    continue

            # Use opposing zone (for tp_zone logic) = any zone of opposite type
            opposing = next(
                (z for z in zones if z.zone_type != zone.zone_type),
                None,
            )
            signal = generate_signal(
                bar_open, bar_close, bar_high, bar_low,
                zone, pair, ts, params,
                opposing_zone=opposing,
                current_atr=current_atr,
            )
            signal_kind = 'reversal'
            if signal is None and enable_breakout_signals:
                signal = generate_breakout_signal(
                    bar_open, bar_close, bar_high, bar_low,
                    zone, pair, ts, params,
                    current_atr=current_atr,
                )
                signal_kind = 'breakout'
            if signal is None:
                continue
            # Direction filter from legacy strategy
            if is_pair_direction_blocked(pair, signal.direction, params):
                continue
            # Momentum filter (only for reversal — breakouts ARE momentum)
            if signal_kind == 'reversal' and check_momentum_filter(hourly_df, i, zone, params):
                continue

            result.signals_generated += 1
            if signal_kind == 'breakout':
                result.signals_breakout += 1
            else:
                result.signals_reversal += 1

            # Build the Trade from the Signal
            trade = Trade(
                entry_time=ts,
                entry_price=float(signal.entry_price),
                direction=signal.direction,
                sl_price=float(signal.sl_price),
                tp_price=float(signal.tp_price),
                zone_upper=float(signal.zone_upper),
                zone_lower=float(signal.zone_lower),
                zone_strength=signal.zone_strength,
                risk=abs(float(signal.entry_price) - float(signal.sl_price)),
                entry_atr=current_atr,
            )
            open_trades.append((trade, fp, i))
            open_fingerprints.add(fp)

    # --- Step 3: close any still-open trades at the last bar ---
    if open_trades and len(hourly_df) > 0:
        last_ts = hourly_df.index[-1]
        last_close = float(hourly_df.iloc[-1]['Close'])
        for trade, _, _ in open_trades:
            _close_trade(trade, last_ts, last_close, 'END', pip)
            result.trades.append(trade)

    return result
