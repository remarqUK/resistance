"""Shared submit-time execution planning for live and historical runs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from .config import PAIRS
from .ibkr import ExecutionQuote
from .levels import SRZone, is_price_in_zone
from .strategy import Signal, StrategyParams


NEAR_ZONE_THRESHOLD_PCT = 0.30


@dataclass(frozen=True)
class ExecutionPlanCore:
    """Submit-time execution plan shared by live and historical callers."""

    signal: Signal
    quote: ExecutionQuote
    entry_price: float
    stop_price: float
    take_profit_price: float


def _pair_pip(pair: str) -> float:
    pip = PAIRS.get((pair or '').upper(), {}).get('pip', 0.0001)
    return float(pip) if pip and pip > 0 else 0.0001


def signal_zone(signal: Signal) -> SRZone:
    """Build a lightweight zone object from a signal."""

    return SRZone(
        upper=float(signal.zone_upper),
        lower=float(signal.zone_lower),
        midpoint=(float(signal.zone_upper) + float(signal.zone_lower)) / 2.0,
        touches=0,
        zone_type=signal.zone_type,
        strength=signal.zone_strength,
    )


def signal_rr_ratio(signal: Signal, params: StrategyParams) -> float:
    """Infer the original RR ratio from the signal."""

    planned_risk = abs(float(signal.entry_price) - float(signal.sl_price))
    if planned_risk <= 0:
        return max(float(params.rr_ratio), 0.0)

    reward = abs(float(signal.tp_price) - float(signal.entry_price))
    if reward <= 0:
        return max(float(params.rr_ratio), 0.0)
    return float(reward / planned_risk)


def quote_age_seconds(
    quote: ExecutionQuote,
    *,
    now: pd.Timestamp | None = None,
) -> float:
    """Return the age of a quote snapshot in seconds."""

    captured_at = pd.Timestamp(quote.captured_at)
    if captured_at.tzinfo is None:
        captured_at = captured_at.tz_localize('UTC')
    else:
        captured_at = captured_at.tz_convert('UTC')

    current_time = pd.Timestamp.now(tz='UTC') if now is None else pd.Timestamp(now)
    if current_time.tzinfo is None:
        current_time = current_time.tz_localize('UTC')
    else:
        current_time = current_time.tz_convert('UTC')
    return max((current_time - captured_at).total_seconds(), 0.0)


def signal_zone_still_tradeable(signal: Signal, mid_price: float) -> bool:
    """Return True when the execution quote still sits in or near the signal zone."""

    zone = signal_zone(signal)
    if is_price_in_zone(float(mid_price), zone):
        return True

    if mid_price <= 0:
        return False
    edge = float(signal.zone_upper) if signal.zone_type == 'support' else float(signal.zone_lower)
    dist = abs(float(mid_price) - edge) / float(mid_price) * 100.0
    return dist <= NEAR_ZONE_THRESHOLD_PCT


def build_execution_plan(
    signal: Signal,
    quote: ExecutionQuote,
    params: StrategyParams,
    *,
    now: pd.Timestamp | None = None,
) -> tuple[Optional[ExecutionPlanCore], str]:
    """Validate and reprice a signal from a two-sided quote."""

    if quote_age_seconds(quote, now=now) > float(params.max_submit_quote_age_seconds) + 1e-9:
        return None, 'stale quote'

    pip = _pair_pip(signal.pair)
    spread_pips = float(quote.spread) / pip
    if spread_pips > float(params.max_submit_spread_pips) + 1e-9:
        return None, 'spread too wide'

    if not signal_zone_still_tradeable(signal, float(quote.mid)):
        return None, 'price left zone'

    planned_risk = abs(float(signal.entry_price) - float(signal.sl_price))
    if planned_risk <= 0:
        return None, 'size unavailable'

    actual_entry = float(quote.ask) if signal.direction == 'LONG' else float(quote.bid)
    drift_r = abs(actual_entry - float(signal.entry_price)) / planned_risk
    if drift_r > float(params.max_submit_entry_drift_r) + 1e-9:
        return None, 'entry drift too large'

    stop_price = float(signal.sl_price)
    if signal.direction == 'LONG':
        actual_risk = actual_entry - stop_price
    else:
        actual_risk = stop_price - actual_entry
    if actual_risk <= 0:
        return None, 'size unavailable'

    rr_ratio = signal_rr_ratio(signal, params)
    if signal.direction == 'LONG':
        take_profit_price = actual_entry + actual_risk * rr_ratio
    else:
        take_profit_price = actual_entry - actual_risk * rr_ratio

    return (
        ExecutionPlanCore(
            signal=signal,
            quote=quote,
            entry_price=actual_entry,
            stop_price=stop_price,
            take_profit_price=float(take_profit_price),
        ),
        '',
    )


def build_modeled_execution_quote(
    pair: str,
    mid_price: float,
    captured_at: pd.Timestamp,
    params: StrategyParams,
    *,
    source: str,
) -> Optional[ExecutionQuote]:
    """Create a synthetic two-sided quote from midpoint data plus modeled spread."""

    if mid_price <= 0:
        return None

    pip = _pair_pip(pair)
    half_spread = max(float(params.spread_pips), 0.0) * pip / 2.0
    bid = float(mid_price) - half_spread
    ask = float(mid_price) + half_spread
    if bid <= 0 or ask <= 0 or ask < bid:
        return None

    quote_time = pd.Timestamp(captured_at)
    if quote_time.tzinfo is None:
        quote_time = quote_time.tz_localize('UTC')
    else:
        quote_time = quote_time.tz_convert('UTC')

    return ExecutionQuote(
        pair=(pair or '').upper(),
        bid=bid,
        ask=ask,
        mid=float(mid_price),
        spread=float(ask - bid),
        source=source,
        captured_at=quote_time,
    )


def historical_execution_quote(
    pair: str,
    submit_time: pd.Timestamp,
    params: StrategyParams,
    *,
    minute_df: pd.DataFrame | None = None,
    l2_snapshots: pd.DataFrame | None = None,
    allow_h1_fallback: bool = False,
    fallback_mid_price: float | None = None,
) -> tuple[Optional[ExecutionQuote], str]:
    """Resolve the first historical execution quote for a submit timestamp."""

    submit_ts = pd.Timestamp(submit_time)
    if submit_ts.tzinfo is None:
        submit_ts = submit_ts.tz_localize('UTC')
    else:
        submit_ts = submit_ts.tz_convert('UTC')

    if l2_snapshots is not None and not l2_snapshots.empty:
        snapshots = l2_snapshots[l2_snapshots.index >= submit_ts]
        if not snapshots.empty:
            snapshot = snapshots.iloc[0]
            quote = ExecutionQuote(
                pair=(pair or '').upper(),
                bid=float(snapshot['best_bid']),
                ask=float(snapshot['best_ask']),
                mid=float(snapshot['mid_price']),
                spread=float(snapshot['best_ask'] - snapshot['best_bid']),
                source='historical_l2',
                captured_at=pd.Timestamp(snapshots.index[0]),
            )
            return quote, ''

    if minute_df is not None and not minute_df.empty:
        exact = minute_df.loc[minute_df.index == submit_ts]
        if not exact.empty:
            row = exact.iloc[0]
            quote = build_modeled_execution_quote(
                pair,
                float(row['Open']),
                submit_ts,
                params,
                source='historical_1m',
            )
            if quote is not None:
                return quote, ''

    if allow_h1_fallback and fallback_mid_price is not None:
        quote = build_modeled_execution_quote(
            pair,
            float(fallback_mid_price),
            submit_ts,
            params,
            source='historical_1h_fallback',
        )
        if quote is not None:
            return quote, ''

    return None, 'quote unavailable'
