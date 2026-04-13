"""Unified data loading pipeline for backtest and live modes.

Both paths need the same four DataFrames per pair: daily, hourly, minute,
and L2 snapshots.  This module provides a single ``load_pair_data()``
entry point that delegates to the appropriate backend.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

import pandas as pd

from .config import PAIRS
from .db import load_l2_snapshots


@dataclass
class PairDataBundle:
    """All market data needed to run a walk-forward for one pair."""

    pair: str
    ticker: str
    daily_df: pd.DataFrame
    hourly_df: pd.DataFrame
    minute_df: pd.DataFrame
    l2_snapshots: pd.DataFrame
    pip: float


def load_pair_data(
    pair: str,
    pair_info: dict,
    *,
    hourly_days: int,
    zone_history_days: int,
    cache_only: bool = True,
    exclude_forming_bar: bool = False,
    allow_stale_cache: bool = False,
    debug: bool = False,
    # Live-mode per-call caches (ignored when cache_only=True)
    daily_data_cache: Optional[Dict] = None,
    hourly_data_cache: Optional[Dict] = None,
    minute_data_cache: Optional[Dict] = None,
) -> PairDataBundle:
    """Load all data for one pair regardless of mode.

    Parameters
    ----------
    cache_only : bool
        When True (backtest), load exclusively from the PostgreSQL cache
        and validate coverage.  When False (live), fetch from IBKR on
        cache miss and apply live-mode caching.
    exclude_forming_bar : bool
        When True (live), drop the currently forming hourly bar so
        signals are only generated from completed candles.
    """
    ticker = pair_info.get('ticker', '')
    pip = float(pair_info.get('pip', 0.0001))

    if cache_only:
        daily_df, hourly_df, minute_df, l2_snapshots = _load_cached(
            ticker,
            hourly_days=hourly_days,
            zone_history_days=zone_history_days,
            allow_stale_cache=allow_stale_cache,
            debug=debug,
        )
    else:
        daily_df, hourly_df, minute_df, l2_snapshots = _load_live(
            ticker,
            hourly_days=hourly_days,
            zone_history_days=zone_history_days,
            daily_data_cache=daily_data_cache,
            hourly_data_cache=hourly_data_cache,
            minute_data_cache=minute_data_cache,
        )

    if exclude_forming_bar and not hourly_df.empty:
        from .live import _completed_live_hourly_data
        hourly_df = _completed_live_hourly_data(hourly_df)

    return PairDataBundle(
        pair=pair,
        ticker=ticker,
        daily_df=daily_df,
        hourly_df=hourly_df,
        minute_df=minute_df,
        l2_snapshots=l2_snapshots,
        pip=pip,
    )


def _load_cached(
    ticker: str,
    *,
    hourly_days: int,
    zone_history_days: int,
    allow_stale_cache: bool,
    debug: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load from PostgreSQL cache (backtest mode)."""
    from .backtest import _load_cached_backtest_data

    daily_df, hourly_df, minute_df = _load_cached_backtest_data(
        ticker,
        hourly_days,
        zone_history_days,
        allow_stale_cache=allow_stale_cache,
        debug=debug,
    )

    end = pd.Timestamp.now(tz='UTC')
    start = end - pd.Timedelta(days=int(hourly_days))
    l2_snapshots = load_l2_snapshots(
        ticker,
        start=start.to_pydatetime(),
        end=end.to_pydatetime(),
    )

    return daily_df, hourly_df, minute_df, l2_snapshots


def _load_live(
    ticker: str,
    *,
    hourly_days: int,
    zone_history_days: int,
    daily_data_cache: Optional[Dict],
    hourly_data_cache: Optional[Dict],
    minute_data_cache: Optional[Dict],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load via live caching layer (live mode)."""
    from .live import (
        _get_live_daily_data,
        _get_live_hourly_data,
        _get_live_minute_data,
    )

    daily_df = _get_live_daily_data(
        ticker,
        zone_history_days,
        daily_data_cache=daily_data_cache,
    )

    hourly_df = _get_live_hourly_data(
        ticker,
        days=max(hourly_days, 1),
        hourly_data_cache=hourly_data_cache,
    )

    hourly_span_days = max(3, len(hourly_df) // 24 + 1) if not hourly_df.empty else 3
    minute_df = _get_live_minute_data(
        ticker,
        days=hourly_span_days,
        minute_data_cache=minute_data_cache,
    )

    # L2 snapshots for the hourly window
    l2_snapshots = pd.DataFrame()
    if not hourly_df.empty:
        l2_snapshots = load_l2_snapshots(
            ticker,
            start=hourly_df.index[0],
            end=hourly_df.index[-1],
        )

    return daily_df, hourly_df, minute_df, l2_snapshots
