"""Data fetching with PostgreSQL cache and IBKR as the only live source.

Supported timeframes:
- Daily: zone identification
- Hourly: entry confirmation and backtesting
- Minute: optional granular inspection

Data sources (in priority order):
1. PostgreSQL cache
2. IBKR TWS / Gateway
"""

from datetime import datetime, timedelta
import math
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from .db import init_db, get_cached_range, load_ohlc, save_ohlc
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


def _as_utc(ts: pd.Timestamp | datetime | str) -> pd.Timestamp:
    """Normalize a timestamp to tz-aware UTC."""
    value = pd.Timestamp(ts)
    return value.tz_localize('UTC') if value.tzinfo is None else value.tz_convert('UTC')


def _remaining_days_to_fetch(
    *,
    interval: str,
    requested_days: int,
    cached_range: tuple[str, str, int] | None,
    now: pd.Timestamp | None = None,
) -> int:
    """Compute the minimum trailing days needed to catch up from cache to now.

    Args:
        interval: One of '1d', '1h', or '1m'
        requested_days: User-requested historical horizon.
        cached_range: Optional tuple of (first_ts, last_ts, row_count)
        now: Optional fixed current time (primarily for tests)
    """
    if requested_days <= 0:
        return 0
    if cached_range is None:
        return requested_days

    if interval not in {'1d', '1h', '1m'}:
        raise ValueError(f'Unsupported interval for resume logic: {interval}')

    cached_first = _as_utc(cached_range[0])
    cached_last = _as_utc(cached_range[1])
    now_ts = _as_utc(now or pd.Timestamp.utcnow())

    requested_start = now_ts - pd.Timedelta(days=requested_days)
    if cached_last < requested_start or cached_first > requested_start:
        return requested_days

    if interval == '1d':
        interval_delta = pd.Timedelta(days=1)
        gap_seconds = (now_ts - cached_last).total_seconds()
        if gap_seconds <= interval_delta.total_seconds():
            return 0

        bars_per_day = 1
        missing_bars = math.ceil(gap_seconds / interval_delta.total_seconds())
        missing_days = int(math.ceil(missing_bars / bars_per_day))
        return min(requested_days, max(1, missing_days))

    # For 1h/1m we need to be weekend-aware because FX is closed on weekends.
    interval_delta = {
        '1h': pd.Timedelta(hours=1),
        '1m': pd.Timedelta(minutes=1),
    }[interval]
    gap_seconds = (now_ts - cached_last).total_seconds()
    if gap_seconds <= interval_delta.total_seconds():
        return 0

    trading_days = _trading_days_between(cached_last, now_ts)
    if cached_last.normalize() < now_ts.normalize():
        trading_days += 1
    return min(requested_days, max(1, trading_days))


def _trading_days_between(start_ts, end_ts) -> int:
    """Count business (weekday) days between two timestamps, inclusive of endpoints."""

    if start_ts is None or end_ts is None:
        return 0

    start = pd.Timestamp(start_ts).normalize()
    end = pd.Timestamp(end_ts).normalize()
    if end < start:
        return 0
    return len(pd.bdate_range(start, end, freq='B'))


def _download_pair_data(
    pair_id: str,
    pair_info: dict,
    *,
    idx: int,
    total_pairs: int,
    daily_days: int,
    hourly_days: int,
    minute_days: int,
    minute_only: bool,
    client_id: int | None,
    resume: bool,
) -> tuple[str, dict[str, int]]:
    """Download one pair's requested history and save it to cache."""
    ticker = pair_info['ticker']
    daily_count = 0
    hourly_count = 0
    minute_count = 0
    print(f'  [{idx}/{total_pairs}] {pair_id} ({ticker}) starting')

    if not minute_only and daily_days > 0:
        daily_range = get_cached_range(ticker, '1d') if resume else None
        daily_fetch_days = _remaining_days_to_fetch(
            interval='1d',
            requested_days=daily_days,
            cached_range=daily_range,
            now=pd.Timestamp.now(tz='UTC'),
        )
        if daily_fetch_days <= 0:
            print(f'    {pair_id}: 1d cache already up to date')
        else:
            print(f'    {pair_id}: downloading 1d data ({daily_fetch_days}d target)')
            daily_df = _fetch_live(ticker, '1d', daily_fetch_days, client_id=client_id)
            if not daily_df.empty:
                save_ohlc(ticker, '1d', daily_df)
                daily_count = len(daily_df)
            print(f'    {pair_id}: 1d -> {daily_count}/{daily_fetch_days}d rows requested')

    if not minute_only and hourly_days > 0:
        hourly_range = get_cached_range(ticker, '1h') if resume else None
        hourly_fetch_days = _remaining_days_to_fetch(
            interval='1h',
            requested_days=hourly_days,
            cached_range=hourly_range,
            now=pd.Timestamp.now(tz='UTC'),
        )
        if hourly_fetch_days <= 0:
            print(f'    {pair_id}: 1h cache already up to date')
        else:
            expected_hourly = hourly_fetch_days * 24
            print(f'    {pair_id}: downloading 1h data (target {expected_hourly} rows)')
            hourly_df = _fetch_live(ticker, '1h', hourly_fetch_days, client_id=client_id)
            if not hourly_df.empty:
                save_ohlc(ticker, '1h', hourly_df)
                hourly_count = len(hourly_df)
            print(f'    {pair_id}: 1h -> {hourly_count}/{expected_hourly} rows requested')

    if minute_days > 0:
        expected_minute = minute_days * 24 * 60
        print(f'    {pair_id}: downloading 1m data (target {expected_minute} rows)')

        def _minute_progress(
            chunk_idx: int,
            chunk_total: int,
            first_ts: str,
            last_ts: str,
            row_count: int,
        ) -> None:
            print(
                f'    {pair_id}: minute chunk {chunk_idx}/{chunk_total}: '
                f'{first_ts} -> {last_ts} ({row_count} rows)'
            )

        minute_df = backfill_minute_data_cached(
            ticker,
            days=minute_days,
            client_id=client_id,
            progress_cb=_minute_progress,
        )
        minute_count = len(minute_df)
        print(f'    {pair_id}: 1m -> {minute_count}/{expected_minute} rows expected')

    summary_parts: list[str] = []
    if not minute_only:
        summary_parts.append(f'{daily_count} daily bars')
        summary_parts.append(f'{hourly_count} hourly bars')
    if minute_days > 0:
        summary_parts.append(f'{minute_count} minute bars')
    print(f"    [{idx}/{total_pairs}] {pair_id}: {', '.join(summary_parts)}")

    return pair_id, {'daily': daily_count, 'hourly': hourly_count, 'minute': minute_count}


def download_single_interval(
    pair_id: str,
    pair_info: dict,
    interval: str,
    days: int,
    *,
    client_id: int | None = None,
    verbose: bool = False,
) -> int:
    """Download one (pair, interval) combo. Returns rows saved."""

    ticker = pair_info['ticker']

    if interval in ('1d', '1h'):
        cached_range = get_cached_range(ticker, interval)
        cached_rows = int(cached_range[2]) if cached_range is not None else 0
        cached_last = cached_range[1] if cached_range is not None else None
        cached_first = cached_range[0] if cached_range is not None else None
        fetch_days = _remaining_days_to_fetch(
            interval=interval,
            requested_days=days,
            cached_range=cached_range,
            now=pd.Timestamp.now(tz='UTC'),
        )
        if fetch_days <= 0:
            if verbose:
                print(f'    {pair_id}: {interval} cache already up to date '
                      f'(rows={cached_rows}, {cached_first} -> {cached_last})')
            return 0
        if verbose:
            cached_last_display = (
                f', last={cached_last}' if cached_last is not None else ', last=none'
            )
            cached_first_display = (
                f', first={cached_first}' if cached_first is not None else ', first=none'
            )
            print(
                f'    {pair_id}: downloading {interval} data ({fetch_days}d target; '
                f'cached_rows={cached_rows}{cached_first_display}{cached_last_display})'
            )
        df = _fetch_live(ticker, interval, fetch_days, client_id=client_id)
        if df.empty:
            return 0
        save_ohlc(ticker, interval, df)
        if verbose:
            print(f'    {pair_id}: {interval} -> {len(df)} rows saved')
        return len(df)

    if interval == '1m':
        cached_range = get_cached_range(ticker, interval)
        cached_rows = int(cached_range[2]) if cached_range is not None else 0
        min_expected_rows = int(days * 1000)
        fetch_days = _remaining_days_to_fetch(
            interval=interval,
            requested_days=days,
            cached_range=cached_range,
            now=pd.Timestamp.now(tz='UTC'),
        )
        if cached_rows < min_expected_rows:
            fetch_days = days
        if fetch_days <= 0:
            if verbose:
                cached_last = cached_range[1] if cached_range is not None else None
                print(f'    {pair_id}: 1m cache already up to date (last={cached_last})')
            return 0
        if verbose:
            print(f'    {pair_id}: downloading 1m data ({fetch_days}d target)')

        def _progress(chunk_idx, chunk_total, first_ts, last_ts, row_count):
            print(
                f'    {pair_id}: 1m chunk {chunk_idx}/{chunk_total}: '
                f'{first_ts} -> {last_ts} ({row_count} rows)'
            )

        df = backfill_minute_data_cached(
            ticker, days=fetch_days, client_id=client_id,
            progress_cb=_progress if verbose else None,
        )
        count = len(df)
        if verbose:
            print(f'    {pair_id}: 1m -> {count} rows total')
        return count

    return 0


def _is_cache_fresh(
    cached: pd.DataFrame,
    *,
    interval: str,
    requested_days: int,
    min_rows: int,
    now: pd.Timestamp | None = None,
) -> bool:
    """Check whether cached data covers the requested trailing window."""

    if cached.empty:
        return False
    if len(cached) < min_rows:
        return False

    return _remaining_days_to_fetch(
        interval=interval,
        requested_days=requested_days,
        cached_range=(cached.index[0], cached.index[-1], len(cached)),
        now=now,
    ) <= 0


def fetch_daily_data(
    ticker_symbol: str,
    days: int = 180,
    force_refresh: bool = False,
    allow_stale_cache: bool = False,
    client_id: int | None = None,
) -> pd.DataFrame:
    """Fetch daily OHLC data, preferring PostgreSQL cache when it is fresh."""
    end = datetime.now()
    start = end - timedelta(days=days)

    cached = pd.DataFrame()
    if not force_refresh:
        cached = load_ohlc(ticker_symbol, '1d', start, end)
        min_rows = max(20, int(days * 0.5))
        if _is_cache_fresh(
            cached,
            interval='1d',
            requested_days=days,
            min_rows=min_rows,
        ):
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
    """Fetch 1-hour OHLC data, preferring PostgreSQL cache when it is fresh."""
    end = datetime.now()
    start = end - timedelta(days=days)

    cached = pd.DataFrame()
    if not force_refresh:
        cached = load_ohlc(ticker_symbol, '1h', start, end)
        # Small windows like 1 day only contain ~24 hourly bars, so requiring
        # 48 rows forces unnecessary live refreshes and parallel IBKR collisions.
        min_rows = max(24, int(days * 10))
        if _is_cache_fresh(
            cached,
            interval='1h',
            requested_days=days,
            min_rows=min_rows,
        ):
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
    minute_days: int = 0,
    minute_only: bool = False,
    client_id: int | None = None,
    max_workers: int = 5,
    resume: bool = True,
) -> dict:
    """Download and cache daily/hourly/minute data from IBKR."""
    if not ibkr.is_available():
        print('  IBKR/TWS is not connected. Fresh downloads require IBKR data access.')
        return {}

    init_db()

    print(f'  Data source: {_source_label()}')
    mode_parts: list[str] = []
    if not minute_only:
        if hourly_days > 0:
            mode_parts.append(f'{hourly_days}d hourly')
        if daily_days > 0:
            mode_parts.append(f'{daily_days}d daily')
    if minute_days > 0:
        mode_parts.append(f'{minute_days}d minute')
    mode_label = ', '.join(mode_parts) if mode_parts else 'nothing requested'
    print(f'  Downloading {len(pairs)} pairs ({mode_label})...')
    if resume:
        print('  Resume mode: enabled')
    else:
        print('  Resume mode: disabled (full refetch)')

    results = {}
    total = len(pairs)

    active_workers = max(1, min(max_workers, 5))
    if total <= 1 or active_workers <= 1:
        for idx, (pair_id, pair_info) in enumerate(pairs.items(), 1):
            _, pair_result = _download_pair_data(
                pair_id,
                pair_info,
                idx=idx,
                total_pairs=total,
                daily_days=daily_days,
                hourly_days=hourly_days,
                minute_days=minute_days,
                minute_only=minute_only,
                client_id=client_id,
                resume=resume,
            )
            results[pair_id] = pair_result
        return results

    base_client_id = ibkr.TWS_CLIENT_ID if client_id is None else client_id
    with ThreadPoolExecutor(max_workers=active_workers) as executor:
        futures = {}
        for idx, (pair_id, pair_info) in enumerate(pairs.items(), 1):
            worker_slot = (idx - 1) % active_workers
            pair_client_id = base_client_id + worker_slot
            futures[
                executor.submit(
                    _download_pair_data,
                    pair_id,
                    pair_info,
                    idx=idx,
                    total_pairs=total,
                    daily_days=daily_days,
                    hourly_days=hourly_days,
                    minute_days=minute_days,
                    minute_only=minute_only,
                    client_id=pair_client_id,
                    resume=resume,
                )
            ] = pair_id

        for future in as_completed(futures):
            pair_id = futures[future]
            _, pair_result = future.result()
            results[pair_id] = pair_result

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
    """Fetch 1-minute OHLC data, with PostgreSQL caching."""
    end = datetime.now()
    start = end - timedelta(days=days)

    cached = load_ohlc(ticker_symbol, '1m', start, end)
    if _is_cache_fresh(
        cached,
        interval='1m',
        requested_days=days,
        min_rows=max(60, days * 500),
    ):
        return cached
    if allow_stale_cache and not cached.empty:
        return cached

    df = _fetch_live(ticker_symbol, '1m', days, client_id=client_id)
    if not df.empty:
        save_ohlc(ticker_symbol, '1m', df)
        return df

    return cached if not cached.empty else pd.DataFrame()


def backfill_minute_data_cached(
    ticker_symbol: str,
    days: int,
    *,
    chunk_days: int = 7,
    client_id: int | None = None,
    progress_cb=None,
) -> pd.DataFrame:
    """Backfill 1-minute bars in IBKR-sized chunks and persist them to PostgreSQL."""

    if days <= 0:
        return pd.DataFrame()

    chunk_days = max(1, min(int(chunk_days), 7))
    now = pd.Timestamp.now(tz='UTC')
    start_ts = now - pd.Timedelta(days=int(days))

    # Refresh the most recent window first so reruns stay current near "now".
    fetch_minute_data_cached(
        ticker_symbol,
        days=min(chunk_days, int(days)),
        allow_stale_cache=False,
        client_id=client_id,
    )

    cached = load_ohlc(
        ticker_symbol,
        '1m',
        start_ts.to_pydatetime(),
        now.to_pydatetime(),
    )
    oldest_cached = cached.index.min() if not cached.empty else now
    if not cached.empty and oldest_cached <= start_ts:
        return cached

    end_ts = pd.Timestamp(oldest_cached) - pd.Timedelta(minutes=1)
    total_chunks = max(1, math.ceil(int(days) / chunk_days))
    chunk_idx = 1

    while end_ts > start_ts:
        remaining_days = max((end_ts - start_ts).total_seconds() / 86400.0, 0.0)
        fetch_days = max(1, min(chunk_days, math.ceil(remaining_days)))
        df = ibkr.fetch_historical(
            ticker_symbol,
            '1m',
            fetch_days,
            client_id=client_id,
            end_datetime=end_ts,
        )
        if df is None or df.empty:
            break

        save_ohlc(ticker_symbol, '1m', df)
        if progress_cb is not None:
            progress_cb(
                chunk_idx,
                total_chunks,
                pd.Timestamp(df.index.min()),
                pd.Timestamp(df.index.max()),
                len(df),
            )

        next_end = pd.Timestamp(df.index.min()) - pd.Timedelta(minutes=1)
        if next_end >= end_ts:
            break
        end_ts = next_end
        chunk_idx += 1

    return load_ohlc(
        ticker_symbol,
        '1m',
        start_ts.to_pydatetime(),
        now.to_pydatetime(),
    )


def fetch_latest_price(ticker_symbol: str) -> float | None:
    """Fetch the latest mid price from IBKR."""
    return ibkr.fetch_latest_price(ticker_symbol)
