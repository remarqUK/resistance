"""Extra signal generators for the multi-zone prototype.

Provides `generate_breakout_signal` as a new setup type that fires when
a candle closes DECISIVELY PAST an S/R zone, in the direction of the
break. Complementary to the legacy reversal signal (which fires when a
candle bounces *off* a zone).

Kept in its own module so production code (strategy.py) is untouched.
"""

from __future__ import annotations

from typing import Optional

import pandas as pd

from ..levels import SRZone
from ..strategy import Signal, StrategyParams, get_pair_pip, get_entry_execution_price


def generate_breakout_signal(
    bar_open: float,
    bar_close: float,
    bar_high: float,
    bar_low: float,
    zone: SRZone,
    pair: str,
    time: pd.Timestamp,
    params: StrategyParams,
    current_atr: float = 0.0,
) -> Optional[Signal]:
    """Detect a breakout setup at an S/R zone.

    Resistance break (LONG): bullish candle closes ABOVE zone.upper, body
    is meaningful, and the bar traded *into* the zone (low below upper).
    SL = zone.lower (failed breakout = price back in the zone).

    Support break (SHORT): bearish candle closes BELOW zone.lower, body
    meaningful, bar traded into the zone (high above lower).
    SL = zone.upper.

    Uses `rr_ratio` for TP. Gated by `min_zone_touches` on the zone
    (same as reversal). Rejects if risk exceeds `max_sl_pct`.
    """

    if zone.touches < params.min_zone_touches:
        return None

    candle_range = abs(bar_high - bar_low)
    candle_body = abs(bar_close - bar_open)
    if candle_range <= 0:
        return None
    body_ratio = candle_body / candle_range
    if body_ratio < max(params.min_entry_candle_body_pct, 0.35):
        # Require a decisive breakout body (default 35% of range).
        return None

    pip = get_pair_pip(pair)

    if zone.zone_type == 'resistance':
        # Bullish break above resistance
        if bar_close <= zone.upper:
            return None
        if bar_low > zone.upper:
            # Never traded into the zone — not a break, just above-zone price action
            return None
        if bar_close <= bar_open:
            return None  # not a bullish candle
        entry_price = get_entry_execution_price(bar_close, 'LONG', pip, params)
        # SL: if price falls back through the zone, breakout failed.
        if params.sl_mode == 'atr' and current_atr > 0:
            sl = zone.lower - current_atr * params.atr_sl_multiplier * 0.5
        else:
            sl = zone.lower * (1 - params.sl_buffer_pct / 100)
        risk = entry_price - sl
        if risk <= 0:
            return None
        if params.max_sl_pct > 0 and (risk / entry_price * 100) > params.max_sl_pct:
            return None
        tp = entry_price + risk * params.rr_ratio
        return Signal(
            time=time, pair=pair, direction='LONG',
            entry_price=entry_price, sl_price=sl, tp_price=tp,
            zone_upper=zone.upper, zone_lower=zone.lower,
            zone_strength=zone.strength, zone_type='resistance',
        )

    elif zone.zone_type == 'support':
        # Bearish break below support
        if bar_close >= zone.lower:
            return None
        if bar_high < zone.lower:
            return None  # never traded into the zone
        if bar_close >= bar_open:
            return None  # not a bearish candle
        entry_price = get_entry_execution_price(bar_close, 'SHORT', pip, params)
        if params.sl_mode == 'atr' and current_atr > 0:
            sl = zone.upper + current_atr * params.atr_sl_multiplier * 0.5
        else:
            sl = zone.upper * (1 + params.sl_buffer_pct / 100)
        risk = sl - entry_price
        if risk <= 0:
            return None
        if params.max_sl_pct > 0 and (risk / entry_price * 100) > params.max_sl_pct:
            return None
        tp = entry_price - risk * params.rr_ratio
        return Signal(
            time=time, pair=pair, direction='SHORT',
            entry_price=entry_price, sl_price=sl, tp_price=tp,
            zone_upper=zone.upper, zone_lower=zone.lower,
            zone_strength=zone.strength, zone_type='support',
        )

    return None
