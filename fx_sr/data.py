"""Data fetching with SQLite cache and IBKR as the only live source.

Supported timeframes:
- Daily: zone identification
- Hourly: entry confirmation and backtesting
- Minute: optional granular inspection

Data sources (in priority order):
1. SQLite cache
2. IBKR TWS / Gateway
"""

from datetime import datetime, timedelta

import pandas as pd

from .db import load_ohlc, save_ohlc
from . import ibkr


def _fetch_live(
    ticker_symbol: str,
    interval: str,
    days: int,
    client_id: int | None = None,
) -> pd.DataFrame:
    """Fetch fresh data from IBKR."""
    df = ibkr.fetch_historical(ticker_symbol, interval, days, client_id=client_id)
    if df is None or df.empty:
        return pd.DataFrame()
    return df


def _source_label() -> str:
    """Return a label for the current live data source."""
    return 'IBKR'


def _cache_age_days(cached: pd.DataFrame) -> int:
    """Return the age in days of the latest cached bar."""
    last_cached = pd.to_datetime(cached.index[-1])
    now = pd.Timestamp.now(tz=last_cached.tzinfo) if last_cached.tzinfo else pd.Timestamp.now()
    return (now - last_cached).days


def _is_cache_fresh(cached: pd.DataFrame, min_rows: int, max_age_days: int = 3) -> bool:
    """Check whether cached data is fresh enough to use directly."""
    if cached.empty:
        return False
    return _cache_age_days(cached) <= max_age_days and len(cached) >= min_rows


def fetch_daily_data(
    ticker_symbol: str,
    days: int = 180,
    force_refresh: bool = False,
    allow_stale_cache: bool = False,
    client_id: int | None = None,
) -> pd.DataFrame:
    """Fetch daily OHLC data, preferring SQLite cache when it is fresh."""
    end = datetime.now()
    start = end - timedelta(days=days)

    cached = pd.DataFrame()
    if not force_refresh:
        cached = load_ohlc(ticker_symbol, '1d', start, end)
        min_rows = max(20, int(days * 0.5))
        if _is_cache_fresh(cached, min_rows=min_rows):
            return cached
        if allow_stale_cache and len(cached) >= min_rows:
            return cached

    df = _fetch_live(ticker_symbol, '1d', days, client_id=client_id)
    if not df.empty:
        save_ohlc(ticker_symbol, '1d', df)
        return df

    return cached if not force_refresh and not cached.empty else pd.DataFrame()


def fetch_hourly_data(
    ticker_symbol: str,
    days: int = 30,
    force_refresh: bool = False,
    allow_stale_cache: bool = False,
    client_id: int | None = None,
) -> pd.DataFrame:
    """Fetch 1-hour OHLC data, preferring SQLite cache when it is fresh."""
    end = datetime.now()
    start = end - timedelta(days=days)

    cached = pd.DataFrame()
    if not force_refresh:
        cached = load_ohlc(ticker_symbol, '1h', start, end)
        # Small windows like 1 day only contain ~24 hourly bars, so requiring
        # 48 rows forces unnecessary live refreshes and parallel IBKR collisions.
        min_rows = max(24, int(days * 10))
        if _is_cache_fresh(cached, min_rows=min_rows):
            return cached
        if allow_stale_cache and len(cached) >= min_rows:
            return cached

    df = _fetch_live(ticker_symbol, '1h', days, client_id=client_id)
    if not df.empty:
        save_ohlc(ticker_symbol, '1h', df)
        return df

    return cached if not force_refresh and not cached.empty else pd.DataFrame()


def download_all_data(
    pairs: dict,
    hourly_days: int = 730,
    daily_days: int = 730,
) -> dict:
    """Download and cache fresh daily/hourly data for all pairs from IBKR."""
    if not ibkr.is_available():
        print('  IBKR/TWS is not connected. Fresh downloads require IBKR data access.')
        return {}

    print(f'  Data source: {_source_label()}')
    print(f'  Downloading {len(pairs)} pairs ({hourly_days}d hourly, {daily_days}d daily)...')

    results = {}
    total = len(pairs)

    for idx, (pair_id, pair_info) in enumerate(pairs.items(), 1):
        ticker = pair_info['ticker']
        daily_df = _fetch_live(ticker, '1d', daily_days)
        hourly_df = _fetch_live(ticker, '1h', hourly_days)

        daily_count = 0
        hourly_count = 0

        if not daily_df.empty:
            save_ohlc(ticker, '1d', daily_df)
            daily_count = len(daily_df)

        if not hourly_df.empty:
            save_ohlc(ticker, '1h', hourly_df)
            hourly_count = len(hourly_df)

        results[pair_id] = {'daily': daily_count, 'hourly': hourly_count}
        print(f'    [{idx}/{total}] {pair_id}: {daily_count} daily bars, {hourly_count} hourly bars')

    return results


def fetch_minute_data(ticker_symbol: str, days: int = 30) -> pd.DataFrame:
    """Fetch minute OHLC data from IBKR."""
    df = ibkr.fetch_historical(ticker_symbol, '1m', days)
    if df is None or df.empty:
        return pd.DataFrame()
    return df


def fetch_minute_data_cached(
    ticker_symbol: str,
    days: int = 2,
    allow_stale_cache: bool = True,
    client_id: int | None = None,
) -> pd.DataFrame:
    """Fetch 1-minute OHLC data, with SQLite caching."""
    end = datetime.now()
    start = end - timedelta(days=days)

    cached = load_ohlc(ticker_symbol, '1m', start, end)
    if _is_cache_fresh(cached, min_rows=max(60, days * 500), max_age_days=1):
        return cached
    if allow_stale_cache and not cached.empty:
        return cached

    df = _fetch_live(ticker_symbol, '1m', days, client_id=client_id)
    if not df.empty:
        save_ohlc(ticker_symbol, '1m', df)
        return df

    return cached if not cached.empty else pd.DataFrame()


def fetch_latest_price(ticker_symbol: str) -> float | None:
    """Fetch the latest mid price from IBKR."""
    return ibkr.fetch_latest_price(ticker_symbol)

