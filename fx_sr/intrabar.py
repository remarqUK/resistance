"""Shared intrabar helpers for signal discovery and submit-time resolution."""

from __future__ import annotations

import pandas as pd

from .levels import SRZone, is_price_halfway_in_zone, is_price_in_zone
from .strategy import Signal, StrategyParams, select_entry_signal


def _normalize_hourly_bar_bounds(
    bar_start: pd.Timestamp,
    minute_index: pd.Index,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return UTC-aware hour window boundaries aligned to `minute_index` timezone."""

    bar_start_ts = pd.Timestamp(bar_start)
    if minute_index.tz is not None:
        if bar_start_ts.tzinfo is None:
            bar_start_ts = bar_start_ts.tz_localize(minute_index.tz)
        elif bar_start_ts.tz != minute_index.tz:
            bar_start_ts = bar_start_ts.tz_convert(minute_index.tz)
    elif bar_start_ts.tzinfo is not None:
        bar_start_ts = bar_start_ts.tz_localize(None)

    bar_end_ts = bar_start_ts + pd.Timedelta(hours=1)
    return bar_start_ts, bar_end_ts


def find_intrabar_signal(
    bar_start: pd.Timestamp,
    minute_df: pd.DataFrame,
    pair: str,
    params: StrategyParams,
    support_zone: SRZone | None,
    resistance_zone: SRZone | None,
    current_atr: float = 0.0,
) -> tuple[Signal, pd.Timestamp] | None:
    """Return the first signal candidate and its exact trigger timestamp.

    The first matching minute inside the hour bucket is used to keep signal
    trigger semantics stable for both forward and replayed bars.
    """

    if minute_df.empty:
        return None

    bar_start_ts, bar_end_ts = _normalize_hourly_bar_bounds(bar_start, minute_df.index)
    minute_idx = minute_df.index
    start = minute_idx.searchsorted(bar_start_ts, side='left')
    end = minute_idx.searchsorted(bar_end_ts, side='left')

    for i in range(start, end):
        close_price = float(minute_df.iloc[i]['Close'])
        support_candidate = (
            support_zone is not None
            and is_price_in_zone(close_price, support_zone)
            and is_price_halfway_in_zone(close_price, support_zone, params.zone_penetration_pct)
        )
        resistance_candidate = (
            resistance_zone is not None
            and is_price_in_zone(close_price, resistance_zone)
            and is_price_halfway_in_zone(close_price, resistance_zone, params.zone_penetration_pct)
        )
        if not support_candidate and not resistance_candidate:
            continue
        signal = select_entry_signal(
            hourly_df=minute_df,
            bar_idx=i,
            pair=pair,
            params=params,
            support_zone=support_zone,
            resistance_zone=resistance_zone,
            current_atr=current_atr,
        )
        if signal is not None:
            return signal, pd.Timestamp(signal.time)

    return None


def intrabar_execution_time(
    bar_start: pd.Timestamp,
    minute_df: pd.DataFrame | None,
) -> pd.Timestamp:
    """Resolve latest available intrabar submit time within the hour bucket."""

    if minute_df is None or minute_df.empty:
        bar_start_ts = pd.Timestamp(bar_start)
        return bar_start_ts + pd.Timedelta(hours=1)

    bar_start_ts, bar_end_ts = _normalize_hourly_bar_bounds(bar_start, minute_df.index)
    minute_window = minute_df[(minute_df.index >= bar_start_ts) & (minute_df.index < bar_end_ts)]
    if minute_window.empty:
        return bar_end_ts
    return pd.Timestamp(minute_window.index[-1])
