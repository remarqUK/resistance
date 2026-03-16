"""Shared serialization helpers for backtest, replay, and UI payloads."""

from __future__ import annotations

from datetime import timedelta

import pandas as pd

from .levels import SRZone
from .strategy import Trade


def serialize_timestamp(value: pd.Timestamp | None) -> str | None:
    """Serialize one timestamp to ISO format."""

    if value is None:
        return None
    return pd.Timestamp(value).isoformat()


def deserialize_timestamp(value: str | None) -> pd.Timestamp | None:
    """Parse one ISO timestamp back into a pandas timestamp."""

    if not value:
        return None
    return pd.Timestamp(value)


def trade_active_dates(
    entry_time: pd.Timestamp | str | None,
    exit_time: pd.Timestamp | str | None,
) -> list[str]:
    """Return every calendar date touched by a trade, inclusive."""

    if entry_time is None:
        return []

    start_date = pd.Timestamp(entry_time).date()
    end_date = pd.Timestamp(exit_time).date() if exit_time is not None else start_date
    if end_date < start_date:
        end_date = start_date

    active_dates: list[str] = []
    current_date = start_date
    while current_date <= end_date:
        active_dates.append(str(current_date))
        current_date += timedelta(days=1)
    return active_dates


def serialize_zone(zone: SRZone, *, include_seen: bool = False) -> dict:
    """Serialize one S/R zone for storage or UI transport."""

    payload = {
        'upper': float(zone.upper),
        'lower': float(zone.lower),
        'midpoint': float(zone.midpoint),
        'touches': int(zone.touches),
        'zone_type': zone.zone_type,
        'strength': zone.strength,
    }
    if include_seen:
        payload['first_seen'] = serialize_timestamp(zone.first_seen)
        payload['last_seen'] = serialize_timestamp(zone.last_seen)
    return payload


def serialize_trade(
    trade: Trade,
    *,
    include_risk: bool = True,
    include_quality: bool = True,
    include_active_dates: bool = False,
    round_exit_metrics: bool = False,
) -> dict:
    """Serialize one trade with optional storage/UI extras."""

    payload = {
        'entry_time': serialize_timestamp(trade.entry_time),
        'entry_price': float(trade.entry_price),
        'direction': trade.direction,
        'sl_price': float(trade.sl_price),
        'tp_price': float(trade.tp_price),
        'zone_upper': float(trade.zone_upper),
        'zone_lower': float(trade.zone_lower),
        'zone_strength': trade.zone_strength,
    }
    if include_risk:
        payload['risk'] = float(trade.risk)
    if include_quality:
        payload['quality_score'] = float(trade.quality_score)
    if include_active_dates:
        payload['active_dates'] = trade_active_dates(trade.entry_time, trade.exit_time)

    if trade.exit_time is not None:
        pnl_pips = float(trade.pnl_pips)
        pnl_r = float(trade.pnl_r)
        if round_exit_metrics:
            pnl_pips = round(pnl_pips, 1)
            pnl_r = round(pnl_r, 2)
        payload.update(
            {
                'exit_time': serialize_timestamp(trade.exit_time),
                'exit_price': float(trade.exit_price) if trade.exit_price is not None else None,
                'exit_reason': trade.exit_reason,
                'pnl_pips': pnl_pips,
                'pnl_r': pnl_r,
                'bars_held': int(trade.bars_held),
            }
        )
    elif include_risk:
        payload.update(
            {
                'exit_time': None,
                'exit_price': None,
                'exit_reason': None,
                'pnl_pips': float(trade.pnl_pips),
                'pnl_r': float(trade.pnl_r),
                'bars_held': int(trade.bars_held),
            }
        )

    return payload
