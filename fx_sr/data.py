"""Data fetching with PostgreSQL cache and IBKR as the only live source.

Supported timeframes:
- Daily: zone identification
- Hourly: entry confirmation and backtesting
- Minute: optional granular inspection

Data sources (in priority order):
1. PostgreSQL cache
2. IBKR TWS / Gateway
"""

from datetime import datetime, timedelta, timezone
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo

import pandas as pd

from .db import (
    find_ohlc_gap_candidates,
    get_cached_range,
    has_provider_gap_exception,
    init_db,
    load_ohlc,
    save_ohlc,
    save_provider_gap_exception,
    save_provider_gap_exceptions_batch,
)
from . import ibkr


_HISTORICAL_FETCH_TIMEOUT_SECONDS = max(
    1.0,
    float(os.getenv('IBKR_HISTORICAL_FETCH_TIMEOUT_SECONDS', '20')),
)


def _easter_sunday(year: int) -> pd.Timestamp:
    """Return Easter Sunday for a Gregorian year."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return pd.Timestamp(year=year, month=month, day=day, tz='UTC')


def _is_fx_daily_holiday(ts: pd.Timestamp | datetime | str) -> bool:
    """Return True when a daily FX bar is not expected on that UTC date."""
    current = _as_utc(ts).normalize()
    month_day = (current.month, current.day)
    if month_day in {(1, 1), (12, 25)}:
        return True
    easter = _easter_sunday(current.year)
    good_friday = easter - pd.Timedelta(days=2)
    return current == good_friday.normalize()


def _easter_monday_delayed_reopen(year: int) -> pd.Timestamp:
    """Return the special Easter-weekend FX reopen time in UTC."""
    easter = _easter_sunday(year).normalize()
    return easter + pd.Timedelta(days=1)


def _special_weekend_reopen_time(ts: pd.Timestamp | datetime | str) -> pd.Timestamp | None:
    """Return a special weekend reopen timestamp when holiday trading hours differ."""
    current = _as_utc(ts)
    days_to_sunday = (6 - current.weekday()) % 7
    sunday = current.normalize() + pd.Timedelta(days=days_to_sunday)
    easter = _easter_sunday(sunday.year).normalize()
    if sunday == easter:
        good_friday = easter - pd.Timedelta(days=2)
        if current.normalize() < good_friday:
            return None
        return _easter_monday_delayed_reopen(sunday.year)
    return None


def _holiday_eve_intraday_resume_time(ts: pd.Timestamp | datetime | str) -> pd.Timestamp | None:
    """Return the next expected intraday resume time for Christmas/New Year closures."""

    current_utc = _as_utc(ts)
    current_ny = current_utc.tz_convert(ZoneInfo('America/New_York'))
    holiday_ny = (current_ny.normalize() + pd.Timedelta(days=1)).normalize()
    holiday_utc = holiday_ny.tz_convert('UTC').normalize()
    if (holiday_utc.month, holiday_utc.day) not in {(1, 1), (12, 25)}:
        return None
    close_minute = 13 * 60
    minute_of_day = current_ny.hour * 60 + current_ny.minute
    if minute_of_day < close_minute:
        return None
    return (holiday_ny + pd.Timedelta(hours=19)).tz_convert('UTC')


def _next_expected_daily_bar_time(ts: pd.Timestamp | datetime | str) -> pd.Timestamp:
    """Advance to the next expected daily FX bar timestamp."""
    next_ts = _as_utc(ts).normalize() + pd.Timedelta(days=1)
    while next_ts.weekday() >= 5 or _is_fx_daily_holiday(next_ts):
        next_ts += pd.Timedelta(days=1)
    return next_ts


def _last_cached_label(cached: pd.DataFrame) -> str:
    """Return a compact UTC label for the latest cached timestamp."""
    if cached.empty:
        return 'n/a'
    last = pd.Timestamp(cached.index[-1])
    if last.tzinfo is None:
        last = last.tz_localize('UTC')
    else:
        last = last.tz_convert('UTC')
    return last.isoformat()


def _trailing_gap_days(cached: pd.DataFrame, *, interval: str) -> int:
    """Return how many days of data are missing at the trailing (recent) end.

    Returns 0 when the cache is up to date, or a positive number of days
    that need fetching from a live source.  Weekend gaps are accounted for
    (FX markets are closed Sat–Sun).
    """
    if cached.empty:
        return 999  # no cache at all — caller should use its default days

    now = pd.Timestamp.now('UTC')
    last = pd.Timestamp(cached.index[-1])
    if last.tzinfo is None:
        last = last.tz_localize('UTC')
    else:
        last = last.tz_convert('UTC')

    # Tolerance: how old can the last bar be before we consider it stale?
    # Allow 2× the bar interval to account for the currently-forming bar
    # plus a small buffer.  On weekends, FX is closed ~48 h so we add that.
    tolerances = {'1d': pd.Timedelta(days=2), '1h': pd.Timedelta(hours=2), '1m': pd.Timedelta(minutes=2)}
    tolerance = tolerances.get(interval, pd.Timedelta(hours=2))

    # If it's weekend (Saturday or Sunday before market open), extend
    # tolerance to cover the Friday-close → Sunday-open gap.
    weekday = now.weekday()  # 0=Mon … 6=Sun
    if weekday == 5:  # Saturday
        tolerance += pd.Timedelta(days=1)
    elif weekday == 6:  # Sunday
        tolerance += pd.Timedelta(days=2)

    gap = now - last
    if gap <= tolerance:
        return 0

    # Compute business days in the gap as the fetch size
    trading = _trading_days_between(last, now)
    return max(1, trading + 1)


def _fetch_live(
    ticker_symbol: str,
    interval: str,
    days: int,
    client_id: int | None = None,
    timeout_s: float | None = None,
) -> pd.DataFrame:
    """Fetch fresh data from IBKR."""
    effective_timeout_s = (
        _HISTORICAL_FETCH_TIMEOUT_SECONDS if timeout_s is None else max(1.0, float(timeout_s))
    )
    df = ibkr.fetch_historical(
        ticker_symbol,
        interval,
        days,
        client_id=client_id,
        timeout_s=effective_timeout_s,
    )
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


def fx_market_is_open(ts: pd.Timestamp | datetime | str | None = None) -> bool:
    """Return whether bars are expected at a UTC timestamp.

    Data has a daily maintenance break from 17:00 to 17:14 New York time and
    the weekend closure runs from Friday 17:00 New York time to Sunday 17:15
    New York time, except for special holiday-delayed reopens.
    """

    now_utc = _as_utc(ts or pd.Timestamp.now(tz='UTC'))
    now_ny = now_utc.tz_convert(ZoneInfo('America/New_York'))
    weekday = now_ny.weekday()  # Mon=0 ... Sun=6
    minute_of_day = now_ny.hour * 60 + now_ny.minute
    close_minute = 17 * 60
    reopen_minute = 17 * 60 + 15
    holiday_resume_minute = 19 * 60
    tomorrow_ny = now_ny.normalize() + pd.Timedelta(days=1)
    tomorrow_utc = tomorrow_ny.tz_convert('UTC').normalize()
    tomorrow_month_day = (tomorrow_utc.month, tomorrow_utc.day)
    month_day = (now_ny.month, now_ny.day)
    special_reopen = _special_weekend_reopen_time(now_utc)
    if weekday == 5:
        return False
    if weekday == 6:
        if special_reopen is not None:
            return False
        if minute_of_day < reopen_minute:
            return False
    if month_day in {(1, 1), (12, 25)} and minute_of_day < holiday_resume_minute:
        return False
    if tomorrow_month_day in {(1, 1), (12, 25)} and minute_of_day >= 13 * 60:
        return False
    if weekday == 4 and minute_of_day >= close_minute:
        return False
    if weekday in {0, 1, 2, 3} and close_minute <= minute_of_day < reopen_minute:
        return False
    if special_reopen is not None and now_utc < special_reopen:
        return False
    return True


def _interval_delta(interval: str) -> pd.Timedelta:
    """Return the nominal bar delta for a supported interval."""

    return {
        '1d': pd.Timedelta(days=1),
        '1h': pd.Timedelta(hours=1),
        '1m': pd.Timedelta(minutes=1),
    }[interval]


def _weekend_reopen_time(ts: pd.Timestamp | datetime | str) -> pd.Timestamp:
    """Return the next UTC session restart timestamp."""

    current_utc = _as_utc(ts)
    special_reopen = _special_weekend_reopen_time(current_utc)
    if special_reopen is not None and current_utc < special_reopen:
        return special_reopen
    current_ny = current_utc.tz_convert(ZoneInfo('America/New_York'))
    weekday = current_ny.weekday()
    ny_day = current_ny.normalize()
    reopen_today_ny = ny_day + pd.Timedelta(hours=17, minutes=15)
    if weekday == 6 and current_ny <= reopen_today_ny:
        return reopen_today_ny.tz_convert('UTC')

    days_until_sunday = (6 - weekday) % 7
    if days_until_sunday == 0 and current_ny > reopen_today_ny:
        days_until_sunday = 7
    sunday_ny = ny_day + pd.Timedelta(days=days_until_sunday)
    reopen_ny = sunday_ny + pd.Timedelta(hours=17, minutes=15)
    return reopen_ny.tz_convert('UTC')


def _is_weekend_transition(
    prev_ts: pd.Timestamp | datetime | str,
    current_ts: pd.Timestamp | datetime | str,
    interval: str,
) -> bool:
    """Return True when a gap spans the normal FX weekend closure."""

    prev_utc = _as_utc(prev_ts)
    current_utc = _as_utc(current_ts)
    prev_ny = prev_utc.tz_convert(ZoneInfo('America/New_York'))
    current_ny = current_utc.tz_convert(ZoneInfo('America/New_York'))

    if current_utc <= prev_utc:
        return False
    if prev_ny.weekday() != 4:
        return False
    if current_ny.weekday() not in {6, 0}:
        return False

    close_ny = prev_ny.normalize() + pd.Timedelta(hours=17)
    last_valid_start = close_ny - _interval_delta(interval)
    if prev_ny < last_valid_start:
        return False
    return True


def _is_maintenance_transition(
    prev_ts: pd.Timestamp | datetime | str,
    current_ts: pd.Timestamp | datetime | str,
    interval: str,
) -> bool:
    """Return True when a gap matches the short daily maintenance break."""

    if interval != '1m':
        return False

    prev_utc = _as_utc(prev_ts)
    current_utc = _as_utc(current_ts)
    if current_utc <= prev_utc:
        return False
    if prev_utc.weekday() != current_utc.weekday():
        return False

    gap_seconds = (current_utc - prev_utc).total_seconds()
    if gap_seconds <= 60 or gap_seconds > 20 * 60:
        return False

    return prev_utc.minute >= 50 and current_utc.minute <= 20


def _next_expected_bar_time(ts: pd.Timestamp | datetime | str, interval: str) -> pd.Timestamp:
    """Advance one expected trading bar, skipping FX weekend closures."""

    current = _as_utc(ts)
    if interval == '1d':
        return _next_expected_daily_bar_time(current)

    if interval == '1h':
        ny = current.tz_convert(ZoneInfo('America/New_York'))
        reopen_utc = _weekend_reopen_time(current)
        reopen_ny = reopen_utc.tz_convert(ZoneInfo('America/New_York'))
        if ny.weekday() == 6 and current == reopen_utc:
            return current.ceil('h')

    step = _interval_delta(interval)
    next_ts = current + step
    while not fx_market_is_open(next_ts):
        next_ny = next_ts.tz_convert(ZoneInfo('America/New_York'))
        weekday = next_ny.weekday()
        minute_of_day = next_ny.hour * 60 + next_ny.minute
        close_minute = 17 * 60
        reopen_minute = 17 * 60 + 15
        special_reopen = _special_weekend_reopen_time(next_ts)
        if special_reopen is not None and next_ts < special_reopen:
            next_ts = special_reopen
            continue
        holiday_resume = _holiday_eve_intraday_resume_time(next_ts)
        if holiday_resume is not None:
            next_ts = holiday_resume
            continue
        if weekday == 5 or (weekday == 6 and minute_of_day < reopen_minute) or (weekday == 4 and minute_of_day >= close_minute):
            next_ts = _weekend_reopen_time(next_ts)
            continue
        if weekday in {0, 1, 2, 3} and close_minute <= minute_of_day < reopen_minute:
            reopen_today_ny = next_ny.normalize() + pd.Timedelta(hours=17, minutes=15)
            next_ts = reopen_today_ny.tz_convert('UTC')
            continue
        break
    return next_ts


def _aligned_expected_bar_at_or_after(
    ts: pd.Timestamp | datetime | str,
    interval: str,
) -> pd.Timestamp:
    """Return the first expected bar timestamp at or after a timestamp."""

    current = _as_utc(ts)
    if interval == '1d':
        current = current.normalize()
        while current.weekday() >= 5 or _is_fx_daily_holiday(current):
            current += pd.Timedelta(days=1)
    elif interval == '1h':
        reopen_utc = _weekend_reopen_time(current)
        ny = current.tz_convert(ZoneInfo('America/New_York'))
        reopen_ny = reopen_utc.tz_convert(ZoneInfo('America/New_York'))
        if ny.weekday() == 6 and current <= reopen_utc:
            current = reopen_utc
        elif ny.weekday() == 6 and reopen_utc < current < current.ceil('h'):
            current = current.ceil('h')
        else:
            current = current.floor('h')
    elif interval == '1m':
        reopen_utc = _weekend_reopen_time(current)
        ny = current.tz_convert(ZoneInfo('America/New_York'))
        if ny.weekday() == 6 and current <= reopen_utc:
            current = reopen_utc
        else:
            current = current.floor('min')

    if fx_market_is_open(current):
        return current
    if interval in {'1h', '1m'}:
        return _weekend_reopen_time(current)
    return _next_expected_bar_time(current - _interval_delta(interval), interval)


def _provider_gap_setting_key(
    ticker_symbol: str,
    interval: str,
    gap_ts: pd.Timestamp | datetime | str,
) -> str:
    """Return the stable key for a provider-confirmed missing bar."""

    gap = _as_utc(gap_ts)
    return f'{ticker_symbol}|{interval}|{gap.isoformat()}'


_CONFIRMED_GAP_CACHE: dict[tuple[str, str, str], set[pd.Timestamp]] = {}


def _load_confirmed_gaps(ticker_symbol: str, interval: str) -> set[pd.Timestamp]:
    """Load all confirmed gap timestamps for a ticker/interval into memory."""

    from .db import _connect, get_db_path, init_db, _ticker_to_db_value, _interval_to_db_value
    db_path = get_db_path()
    key = (db_path, ticker_symbol, interval)
    if key in _CONFIRMED_GAP_CACHE:
        return _CONFIRMED_GAP_CACHE[key]
    init_db(db_path, migrate_legacy=False)
    conn = _connect(db_path)
    try:
        db_ticker = _ticker_to_db_value(conn, ticker_symbol)
        db_interval = _interval_to_db_value(conn, interval)
        rows = conn.execute(
            'SELECT gap_ts FROM provider_gap_exception WHERE ticker=%s AND interval=%s',
            (db_ticker, db_interval),
        ).fetchall()
        gaps = set()
        for row in rows:
            ts = pd.Timestamp(row[0])
            if ts.tzinfo is None:
                ts = ts.tz_localize('UTC')
            else:
                ts = ts.tz_convert('UTC')
            gaps.add(ts)
        _CONFIRMED_GAP_CACHE[key] = gaps
        return gaps
    finally:
        conn.close()


def _is_provider_confirmed_gap(
    ticker_symbol: str,
    interval: str,
    gap_ts: pd.Timestamp | datetime | str,
) -> bool:
    """Return True when a missing bar was previously confirmed absent at IBKR."""

    gaps = _load_confirmed_gaps(ticker_symbol, interval)
    return _as_utc(gap_ts) in gaps


def _remember_provider_confirmed_gap(
    ticker_symbol: str,
    interval: str,
    gap_ts: pd.Timestamp | datetime | str,
) -> None:
    """Persist one provider-confirmed missing bar so future scans can skip it."""

    gap = _as_utc(gap_ts)
    # Invalidate cache — keyed by (db_path, ticker, interval)
    from .db import get_db_path
    _CONFIRMED_GAP_CACHE.pop((get_db_path(), ticker_symbol, interval), None)
    save_provider_gap_exception(
        ticker_symbol,
        interval,
        gap,
        source='ibkr',
        note=f'provider-confirmed gap {_provider_gap_setting_key(ticker_symbol, interval, gap)}',
    )


def _record_pre_horizon_gaps(
    ticker_symbol: str,
    interval: str,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> int:
    """Mark every expected-but-unreachable bar in [start, end) as provider-confirmed.

    Called when a refill walk has gone past IBKR's history horizon (typically
    the 365-day cliff for 1m data). These bars can never be retrieved so
    recording them stops subsequent fills and backtest gap scans from
    flagging them.
    """
    start_ts = _as_utc(start)
    end_ts = _as_utc(end)
    if end_ts <= start_ts:
        return 0

    freq = {'1h': 'h', '1m': 'min', '1d': 'D'}[interval]
    step = _interval_delta(interval)
    expected = pd.date_range(start_ts, end_ts - step, freq=freq, tz='UTC')
    if len(expected) == 0:
        return 0

    if interval in ('1m', '1h'):
        weekdays = expected.weekday
        hours = expected.hour
        is_weekend = (
            (weekdays == 5) |
            ((weekdays == 6) & (hours < 22)) |
            ((weekdays == 4) & (hours >= 22))
        )
        candidates = expected[~is_weekend].tolist()
    else:
        candidates = expected[expected.weekday < 5].tolist()

    if not candidates:
        return 0

    confirmed = _load_confirmed_gaps(ticker_symbol, interval)
    to_record = [ts for ts in candidates if ts not in confirmed]
    if not to_record:
        return 0

    from .db import get_db_path
    _CONFIRMED_GAP_CACHE.pop((get_db_path(), ticker_symbol, interval), None)

    return save_provider_gap_exceptions_batch(
        ticker_symbol,
        interval,
        to_record,
        source='ibkr',
        note='beyond IBKR history horizon',
    )


def _record_closure_gaps(
    ticker_symbol: str,
    interval: str,
    seed_gap: pd.Timestamp,
) -> int:
    """Detect an entire market closure from one known gap and record all missing bars.

    Uses only the local cache — no IBKR calls.  Loads a window around the seed
    gap, generates the expected bar schedule, and batch-records every bar that's
    missing from the cache as a confirmed provider gap.

    Must have cached bars on BOTH sides of the gap to confirm the closure.
    """
    import numpy as np

    seed = _as_utc(seed_gap)
    # Wide lookback to span weekends + multi-day holidays (e.g. Christmas
    # gap seed on Sunday Dec 28 needs to reach back to Wednesday Dec 24).
    window_before = pd.Timedelta(days=7)
    window_after = pd.Timedelta(days=5)
    cache_start = seed - window_before
    cache_end = seed + window_after
    cached_df = load_ohlc(
        ticker_symbol, interval,
        start=cache_start.to_pydatetime(),
        end=cache_end.to_pydatetime(),
    )
    if cached_df is None or cached_df.empty:
        return 0

    idx = pd.DatetimeIndex(cached_df.index)
    if idx.tz is None:
        idx = idx.tz_localize('UTC')
    else:
        idx = idx.tz_convert('UTC')

    # Must have bars BEFORE and AFTER the seed gap to confirm a closure.
    if not (idx.min() < seed and idx.max() > seed):
        return 0

    first_bar = idx.min()
    last_bar = idx.max()

    freq = {'1h': 'h', '1m': 'min', '1d': 'D'}[interval]
    expected = pd.date_range(first_bar, last_bar, freq=freq, tz='UTC')

    cached_set = set(idx)
    missing = expected.difference(pd.DatetimeIndex(list(cached_set)))

    if len(missing) == 0:
        return 0
    weekdays = missing.weekday
    hours = missing.hour
    # FX closed: Friday ≥22:00 UTC, all Saturday, Sunday <22:00 UTC
    is_weekend = (
        (weekdays == 5) |
        ((weekdays == 6) & (hours < 22)) |
        ((weekdays == 4) & (hours >= 22))
    )
    gaps_to_record = missing[~is_weekend].tolist()

    if not gaps_to_record:
        return 0

    confirmed = _load_confirmed_gaps(ticker_symbol, interval)
    gaps_to_record = [
        gap_ts
        for gap_ts in gaps_to_record
        if gap_ts not in confirmed
    ]
    if not gaps_to_record:
        return 0

    from .db import get_db_path
    _CONFIRMED_GAP_CACHE.pop((get_db_path(), ticker_symbol, interval), None)

    count = save_provider_gap_exceptions_batch(
        ticker_symbol,
        interval,
        gaps_to_record,
        source='ibkr',
        note=f'closure detected from seed {seed.isoformat()}',
    )
    return count


def find_first_missing_bar(
    cached: pd.DataFrame,
    interval: str,
    *,
    ticker_symbol: str | None = None,
    now: pd.Timestamp | datetime | str | None = None,
    check_trailing: bool = True,
) -> pd.Timestamp | None:
    """Return the first missing timestamp inside cached data, or at the trailing edge.

    Uses vectorized numpy diff to find gaps in O(suspects) time instead of
    O(n) per-bar Python calls.
    """
    import numpy as np

    if cached is None or cached.empty:
        return None

    idx = cached.index
    if idx.tz is None:
        normalized = idx.tz_localize('UTC')
    else:
        normalized = idx.tz_convert('UTC')

    if len(normalized) < 2:
        if not check_trailing:
            return None
        # Fall through to trailing check below
    else:
        # Vectorized gap detection via numpy diff on int64 timestamps.
        threshold_seconds = {'1d': 2 * 86400, '1h': 2 * 3600, '1m': 2 * 60}[interval]
        threshold = np.timedelta64(threshold_seconds, 's')
        ts_values = normalized.values
        diffs = np.diff(ts_values)
        suspect_indices = np.where(diffs > threshold)[0]

        if len(suspect_indices) > 0:
            if interval in {'1h', '1m'}:
                transition_mask = np.array(
                    [
                        _is_weekend_transition(prev_ts, next_ts, interval)
                        or _is_maintenance_transition(prev_ts, next_ts, interval)
                        for prev_ts, next_ts in zip(
                            normalized[suspect_indices],
                            normalized[suspect_indices + 1],
                        )
                    ],
                    dtype=bool,
                )
                suspect_indices = suspect_indices[~transition_mask]

            if len(suspect_indices) == 0:
                pass
            prev_times = normalized[suspect_indices]
            next_times = normalized[suspect_indices + 1]
            prev_days = np.array([t.weekday() for t in prev_times])
            next_days = np.array([t.weekday() for t in next_times])
            prev_hours = np.array([t.hour for t in prev_times])
            next_hours = np.array([t.hour for t in next_times])
            gap_seconds = np.array([d / np.timedelta64(1, 's') for d in diffs[suspect_indices]])

            if interval == '1d':
                # For daily bars, Friday → Monday is a normal weekend gap.
                is_expected = (prev_days == 4) & (next_days == 0)
            else:
                # Weekend: Friday near NY close → Sunday/Monday.
                is_weekend = (
                    (prev_days == 4) & (prev_hours >= 20)
                    & ((next_days == 6) | (next_days == 0))
                )
                # Daily maintenance break: ~17:00-17:15 NY (20:00-21:15 UTC
                # winter, 21:00-21:15 summer).  Gap is ≤20 minutes and both
                # bars are in the 20-21 UTC hour range on the same weekday.
                is_maintenance = np.array(
                    [
                        _is_maintenance_transition(prev_ts, next_ts, interval)
                        for prev_ts, next_ts in zip(prev_times, next_times)
                    ],
                    dtype=bool,
                )
                is_expected = is_weekend | is_maintenance
            suspect_indices = suspect_indices[~is_expected]

        # Load confirmed gaps once for this ticker/interval (cached).
        confirmed = _load_confirmed_gaps(ticker_symbol, interval) if ticker_symbol else set()

        for si in suspect_indices:
            prev_ts = _as_utc(normalized[si])
            current_ts = _as_utc(normalized[si + 1])
            expected = _next_expected_bar_time(prev_ts, interval)
            if expected in confirmed:
                continue
            if current_ts > expected:
                return expected

    # Trailing edge check
    if not check_trailing:
        return None

    now_ts = _as_utc(now or pd.Timestamp.now(tz='UTC'))
    if not fx_market_is_open(now_ts):
        return None

    tolerance = {
        '1d': pd.Timedelta(days=3),
        '1h': pd.Timedelta(hours=2),
        '1m': pd.Timedelta(minutes=2),
    }[interval]
    last_ts = _as_utc(normalized[-1])
    if now_ts - last_ts <= tolerance:
        return None
    expected = _next_expected_bar_time(last_ts, interval)
    if ticker_symbol and expected in _load_confirmed_gaps(ticker_symbol, interval):
        return None
    return expected


def find_first_missing_cached_bar(
    ticker_symbol: str,
    interval: str,
    *,
    start: pd.Timestamp | datetime | str | None = None,
    end: pd.Timestamp | datetime | str | None = None,
    now: pd.Timestamp | datetime | str | None = None,
    check_trailing: bool = True,
) -> pd.Timestamp | None:
    """Find the first missing cached bar using SQL-side gap detection."""

    query_start = (
        _aligned_expected_bar_at_or_after(start, interval)
        if start is not None
        else None
    )

    window_range = get_cached_range(ticker_symbol, interval, start=query_start, end=end)
    if window_range is None:
        return None

    first_ts, last_ts, _rows = window_range
    if start is not None:
        expected_start = _aligned_expected_bar_at_or_after(start, interval)
        first_seen = _as_utc(first_ts)
        if first_seen > expected_start and not _is_provider_confirmed_gap(
            ticker_symbol,
            interval,
            expected_start,
        ):
            return expected_start

    for prev_ts, current_ts in find_ohlc_gap_candidates(
        ticker_symbol,
        interval,
        start=query_start,
        end=end,
        limit=512,
    ):
        if interval in {'1h', '1m'} and (
            _is_weekend_transition(prev_ts, current_ts, interval)
            or _is_maintenance_transition(prev_ts, current_ts, interval)
        ):
            continue
        expected = _next_expected_bar_time(prev_ts, interval)
        if _is_provider_confirmed_gap(ticker_symbol, interval, expected):
            continue
        current = _as_utc(current_ts)
        if current > expected:
            return expected

    if not check_trailing:
        return None

    now_ts = _as_utc(now or pd.Timestamp.now(tz='UTC'))
    if end is not None and now_ts > _as_utc(end):
        now_ts = _as_utc(end)
    if not fx_market_is_open(now_ts):
        return None

    tolerance = {
        '1d': pd.Timedelta(days=3),
        '1h': pd.Timedelta(hours=2),
        '1m': pd.Timedelta(minutes=2),
    }[interval]
    last_seen = _as_utc(last_ts)
    if now_ts - last_seen <= tolerance:
        return None
    return _next_expected_bar_time(last_seen, interval)


def _learn_remaining_gaps_after_refill(
    ticker_symbol: str,
    interval: str,
    *,
    start: pd.Timestamp | datetime | str,
    end: pd.Timestamp | datetime | str,
    now: pd.Timestamp | datetime | str | None = None,
    client_id: int | None = None,
) -> pd.Timestamp | None:
    """Persist refill leftovers one gap at a time, committing each as learned."""

    while True:
        remaining_gap = find_first_missing_cached_bar(
            ticker_symbol,
            interval,
            start=start,
            end=end,
            now=now,
        )
        if remaining_gap is None:
            return None

        recorded = _record_closure_gaps(ticker_symbol, interval, remaining_gap)
        if recorded > 0:
            print(
                f'      {ticker_symbol} {interval}: recorded {recorded} closure gaps '
                f'from {remaining_gap.date()}'
            )
            continue

        if _provider_confirms_unfillable_gap(
            ticker_symbol,
            interval,
            remaining_gap,
            client_id=client_id,
        ):
            _remember_provider_confirmed_gap(ticker_symbol, interval, remaining_gap)
            print(f'      {ticker_symbol} {interval}: learned provider gap at {remaining_gap}')
            continue

        return remaining_gap


def refill_interval_from(
    ticker_symbol: str,
    interval: str,
    start_ts: pd.Timestamp | datetime | str,
    *,
    client_id: int | None = None,
) -> pd.DataFrame:
    """Refetch one interval from the first missing bar to now and persist it."""

    start = _as_utc(start_ts)
    now = pd.Timestamp.now(tz='UTC')
    fetch_days = max(1, int(math.ceil((now - start).total_seconds() / 86400.0)) + 1)

    if interval == '1m':
        # IBKR's 1m historical feed lags real-time by ~1–2 minutes. Asking
        # past that point returns empty and makes the refill raise even
        # though the "gap" is just waiting for IBKR to publish. Match the
        # trailing tolerance used by find_first_missing_cached_bar so we
        # only try to fetch bars IBKR can actually deliver.
        trailing_lag = pd.Timedelta(minutes=2)
        fetch_end = now - trailing_lag
        # IBKR's 1m history horizon is ~365 days relative to request time.
        # During a long walk-back the horizon slides forward — the original
        # edge becomes unreachable mid-fill, causing Error 162/timeouts for
        # the last few chunks. Keep the walk an hour inside the horizon
        # and record any skipped bars as provider-confirmed so consumers
        # don't flag them as refillable gaps.
        horizon_buffer = pd.Timedelta(hours=1)
        min_fetchable = now - pd.Timedelta(days=365) + horizon_buffer
        fetch_start = start if start > min_fetchable else min_fetchable
        if fetch_start > start:
            pre_recorded = _record_pre_horizon_gaps(
                ticker_symbol, interval, start=start, end=fetch_start,
            )
            if pre_recorded > 0:
                print(
                    f'      {ticker_symbol} {interval}: marked {pre_recorded} '
                    f'pre-horizon bars as unfillable ({start} -> {fetch_start})'
                )
        if fetch_end <= fetch_start:
            existing = load_ohlc(
                ticker_symbol, interval,
                start=start.to_pydatetime(),
                end=now.to_pydatetime(),
            )
            return existing if existing is not None else pd.DataFrame()
        end_ts = fetch_end
        chunk_days = 3
        max_chunk_days = chunk_days
        slow_chunk_threshold_s = 20.0
        fetched_any = False
        earliest_fetched_ts: pd.Timestamp | None = None
        # If cached data already covers the trailing portion of the window,
        # skip the walk past it — start just below the oldest cached bar.
        # This avoids refetching months of existing data when only an older
        # slice is actually missing.
        existing_cache = load_ohlc(
            ticker_symbol, interval,
            start=fetch_start.to_pydatetime(),
            end=fetch_end.to_pydatetime(),
        )
        if existing_cache is not None and not existing_cache.empty:
            cached_oldest = pd.Timestamp(existing_cache.index.min())
            if cached_oldest.tzinfo is None:
                cached_oldest = cached_oldest.tz_localize('UTC')
            else:
                cached_oldest = cached_oldest.tz_convert('UTC')
            if cached_oldest > fetch_start:
                end_ts = cached_oldest - pd.Timedelta(minutes=1)
                earliest_fetched_ts = cached_oldest
                fetched_any = True
                print(
                    f'      {ticker_symbol} {interval}: skipping cached '
                    f'tail ({cached_oldest} -> {fetch_end}); '
                    f'refill targets [{fetch_start} -> {cached_oldest})'
                )
        while end_ts > fetch_start:
            remaining_days = max((end_ts - fetch_start).total_seconds() / 86400.0, 0.0)
            fetch_chunk_days = max(1, min(max_chunk_days, math.ceil(remaining_days)))
            attempted_chunk_days: list[int] = []
            df = None
            last_attempt_elapsed_s = 0.0
            current_chunk_days = fetch_chunk_days
            while current_chunk_days >= 1:
                attempted_chunk_days.append(current_chunk_days)
                attempt_started = pd.Timestamp.now(tz='UTC')
                df = ibkr.fetch_historical(
                    ticker_symbol,
                    '1m',
                    current_chunk_days,
                    client_id=client_id,
                    end_datetime=end_ts,
                )
                last_attempt_elapsed_s = (
                    pd.Timestamp.now(tz='UTC') - attempt_started
                ).total_seconds()
                if df is not None and not df.empty:
                    break
                if current_chunk_days == 1:
                    break
                next_chunk_days = max(1, current_chunk_days // 2)
                if next_chunk_days == current_chunk_days:
                    next_chunk_days = current_chunk_days - 1
                current_chunk_days = next_chunk_days
                max_chunk_days = min(max_chunk_days, current_chunk_days)
            if df is None or df.empty:
                # Chunk returned nothing — try to record the closure from cache.
                gap_at = end_ts if end_ts.tzinfo is not None else end_ts.tz_localize('UTC')
                recorded = _record_closure_gaps(ticker_symbol, interval, gap_at)
                if recorded > 0:
                    print(f'      {ticker_symbol} {interval}: recorded {recorded} closure gaps from {gap_at.date()}')
                    # Skip past the closure — find the last bar before the gap.
                    pre_gap = load_ohlc(
                        ticker_symbol, interval,
                        start=fetch_start.to_pydatetime(),
                        end=gap_at.to_pydatetime(),
                    )
                    if pre_gap is not None and not pre_gap.empty:
                        pre_idx = pd.DatetimeIndex(pre_gap.index)
                        if pre_idx.tz is None:
                            pre_idx = pre_idx.tz_localize('UTC')
                        end_ts = pre_idx.min() - pd.Timedelta(minutes=1)
                        fetched_any = True
                        continue
                    break  # No earlier data — done.
                if fetched_any:
                    # We've already pulled some data but IBKR is now returning
                    # empty — we've walked past IBKR's earliest available bar
                    # for this pair. Stop gracefully instead of raising.
                    break
                raise RuntimeError(
                    f'IBKR returned no {interval} bars for {ticker_symbol} '
                    f'while refilling from {fetch_start} (request end {end_ts}, '
                    f'chunk attempts={attempted_chunk_days})'
                )
            fetched_any = True
            chunk_min = pd.Timestamp(df.index.min())
            if chunk_min.tzinfo is None:
                chunk_min = chunk_min.tz_localize('UTC')
            else:
                chunk_min = chunk_min.tz_convert('UTC')
            if earliest_fetched_ts is None or chunk_min < earliest_fetched_ts:
                earliest_fetched_ts = chunk_min
            if current_chunk_days < max_chunk_days:
                max_chunk_days = current_chunk_days
            elif last_attempt_elapsed_s >= slow_chunk_threshold_s and current_chunk_days > 1:
                max_chunk_days = current_chunk_days - 1
            save_ohlc(ticker_symbol, '1m', df)
            next_end = chunk_min - pd.Timedelta(minutes=1)
            if next_end >= end_ts:
                break
            end_ts = next_end
        if not fetched_any:
            raise RuntimeError(f'IBKR returned no {interval} bars for {ticker_symbol} while refilling from {fetch_start}')
        refilled = load_ohlc(
            ticker_symbol,
            interval,
            start=start.to_pydatetime(),
            end=fetch_end.to_pydatetime(),
        )
        # Only scan for missing bars within the range IBKR actually delivered.
        # Bars older than `earliest_fetched_ts` are beyond IBKR's history
        # horizon for this pair — not true gaps we can ever refill.
        effective_start = fetch_start
        if earliest_fetched_ts is not None and earliest_fetched_ts > fetch_start:
            effective_start = earliest_fetched_ts
            # Persist the pre-horizon bars as provider-confirmed gaps so the
            # next fill/backtest scan skips them instead of re-raising.
            recorded = _record_pre_horizon_gaps(
                ticker_symbol, interval,
                start=fetch_start,
                end=earliest_fetched_ts,
            )
            if recorded > 0:
                print(
                    f'      {ticker_symbol} {interval}: marked {recorded} pre-horizon '
                    f'bars as unfillable ({fetch_start} -> {earliest_fetched_ts})'
                )
        remaining_gap = _learn_remaining_gaps_after_refill(
            ticker_symbol,
            interval,
            start=effective_start,
            end=fetch_end,
            now=now,
            client_id=client_id,
        )
        if remaining_gap is not None:
            raise RuntimeError(
                f'{ticker_symbol} {interval} refill incomplete: first missing bar still at {remaining_gap}'
            )
        return refilled

    df = _fetch_live(ticker_symbol, interval, fetch_days, client_id=client_id)
    if df is None or df.empty:
        # Try to record closure from cache before raising.
        recorded = _record_closure_gaps(ticker_symbol, interval, start)
        if recorded > 0:
            print(f'      {ticker_symbol} {interval}: recorded {recorded} closure gaps from {start.date()}')
            cached = load_ohlc(ticker_symbol, interval, start=start.to_pydatetime(), end=now.to_pydatetime())
            return cached if cached is not None else pd.DataFrame()
        raise RuntimeError(
            f'IBKR returned no {interval} bars for {ticker_symbol} while refilling from {start}'
        )
    save_ohlc(ticker_symbol, interval, df)
    refilled = load_ohlc(
        ticker_symbol,
        interval,
        start=start.to_pydatetime(),
        end=now.to_pydatetime(),
    )
    remaining_gap = _learn_remaining_gaps_after_refill(
        ticker_symbol,
        interval,
        start=start,
        end=now,
        now=now,
        client_id=client_id,
    )
    if remaining_gap is not None:
        raise RuntimeError(
            f'{ticker_symbol} {interval} refill incomplete: first missing bar still at {remaining_gap}'
        )
    return refilled


def _gap_past_ibkr_horizon(
    interval: str,
    gap_ts: pd.Timestamp | datetime | str,
    *,
    now: pd.Timestamp | None = None,
) -> bool:
    """Return True when ``gap_ts`` is beyond IBKR's history cliff for ``interval``.

    Local-only check — no IBKR round-trip. Use this from backtest paths that
    must not contact the provider; the fill path still runs
    ``_provider_confirms_unfillable_gap`` for within-horizon gaps.
    """

    if interval != '1m':
        return False
    gap = _as_utc(gap_ts)
    now_ts = _as_utc(now or pd.Timestamp.now(tz='UTC'))
    min_fetchable = now_ts - pd.Timedelta(days=365) + pd.Timedelta(hours=1)
    return gap < min_fetchable


def _provider_confirms_unfillable_gap(
    ticker_symbol: str,
    interval: str,
    gap_ts: pd.Timestamp | datetime | str,
    *,
    client_id: int | None = None,
) -> bool:
    """Return True when IBKR historical data itself is missing the same bar."""

    gap = _as_utc(gap_ts)
    if interval == '1m':
        # IBKR's 1m history cliff is 365 days. Anything older than that can
        # never be retrieved, so don't waste a probe call (which would time
        # out or return empty and fall through as "not confirmed", forcing
        # the backtest to raise).
        if _gap_past_ibkr_horizon(interval, gap):
            return True
        fetch_days = 2
        end_ts = gap + pd.Timedelta(days=1)
        lookback = pd.Timedelta(minutes=30)
        lookahead = pd.Timedelta(minutes=30)
    elif interval == '1h':
        fetch_days = 7
        end_ts = gap + pd.Timedelta(days=2)
        lookback = pd.Timedelta(hours=6)
        lookahead = pd.Timedelta(hours=6)
    else:
        return False

    provider_df = ibkr.fetch_historical(
        ticker_symbol,
        interval,
        fetch_days,
        client_id=client_id,
        end_datetime=end_ts,
    )
    if provider_df is None or provider_df.empty:
        return False

    provider_index = pd.DatetimeIndex(provider_df.index)
    if provider_index.tz is None:
        provider_index = provider_index.tz_localize('UTC')
    else:
        provider_index = provider_index.tz_convert('UTC')
    provider_df = provider_df.copy()
    provider_df.index = provider_index

    if gap in provider_df.index:
        return False

    earlier = provider_df[(provider_df.index >= gap - lookback) & (provider_df.index < gap)]
    later = provider_df[(provider_df.index > gap) & (provider_df.index <= gap + lookahead)]
    if interval in {'1m', '1h'}:
        previous_expected = gap - _interval_delta(interval)
        if not fx_market_is_open(previous_expected) and not later.empty:
            return True
    return not earlier.empty and not later.empty


def effective_cached_bar_count(
    ticker_symbol: str,
    interval: str,
    *,
    cached_range: tuple[str, str, int] | None,
    requested_days: int,
    now: pd.Timestamp | None = None,
) -> int:
    """Return the number of cached bars within the requested trailing window.

    Queries the DB with window bounds so old history outside the window
    doesn't inflate the count.
    """

    if cached_range is None or requested_days <= 0:
        return 0
    now_ts = _as_utc(now or pd.Timestamp.now(tz='UTC'))
    window_start = (now_ts - pd.Timedelta(days=requested_days)).to_pydatetime()
    windowed = get_cached_range(
        ticker_symbol, interval,
        start=window_start,
        end=now_ts.to_pydatetime(),
    )
    if windowed is None:
        return 0
    return int(windowed[2])


def _remaining_days_to_fetch(
    *,
    interval: str,
    requested_days: int,
    cached_range: tuple[str, str, int] | None,
    now: pd.Timestamp | None = None,
    trailing_freshness_seconds: int = 0,
) -> int:
    """Compute the minimum trailing days needed to catch up from cache to now.

    Args:
        interval: One of '1d', '1h', or '1m'
        requested_days: User-requested historical horizon.
        cached_range: Optional tuple of (first_ts, last_ts, row_count)
        now: Optional fixed current time (primarily for tests)
        trailing_freshness_seconds: Opt-in — when >0, skip refetch if the
            cache is already within this many seconds of real time. Default 0
            preserves strict behavior for live; backtest-prep callers may
            pass 900 to accept a 15-minute trailing window.
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

    if trailing_freshness_seconds > 0 and (
        now_ts - cached_last
    ).total_seconds() <= trailing_freshness_seconds:
        return 0

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
    # Allow up to 2× the interval before flagging a gap, since the current
    # (incomplete) bar won't be cached until it closes.
    interval_delta = {
        '1h': pd.Timedelta(hours=1),
        '1m': pd.Timedelta(minutes=1),
    }[interval]
    gap_seconds = (now_ts - cached_last).total_seconds()
    if gap_seconds <= interval_delta.total_seconds() * 2:
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
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    cached = pd.DataFrame()
    if not force_refresh:
        cached = load_ohlc(ticker_symbol, '1d', start, end)
        gap_days = _trailing_gap_days(cached, interval='1d')
        if gap_days <= 0:
            print(f'      {ticker_symbol} 1d: cache up to date ({len(cached)} rows)')
            return cached
        if allow_stale_cache and not cached.empty:
            print(
                f'      {ticker_symbol} 1d: using cached history without refresh '
                f'({len(cached)} rows, last { _last_cached_label(cached) }, trailing gap {gap_days}d)'
            )
            return cached
        # Only fetch the gap, not the full window
        fetch_days = min(days, gap_days)
        print(f'      {ticker_symbol} 1d: cache has {len(cached)} rows, gap {gap_days}d, fetching {fetch_days}d from IBKR...')
    else:
        fetch_days = days

    df = _fetch_live(ticker_symbol, '1d', fetch_days, client_id=client_id)
    if not df.empty:
        save_ohlc(ticker_symbol, '1d', df)
        print(f'      {ticker_symbol} 1d: IBKR returned {len(df)} rows')
        # Merge with cached data if we only fetched the gap
        if not cached.empty:
            combined = pd.concat([cached[~cached.index.isin(df.index)], df]).sort_index()
            return combined
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
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    cached = pd.DataFrame()
    if not force_refresh:
        cached = load_ohlc(ticker_symbol, '1h', start, end)
        gap_days = _trailing_gap_days(cached, interval='1h')
        if gap_days <= 0:
            print(f'      {ticker_symbol} 1h: cache up to date ({len(cached)} rows)')
            return cached
        if allow_stale_cache and not cached.empty:
            print(
                f'      {ticker_symbol} 1h: using cached history without refresh '
                f'({len(cached)} rows, last { _last_cached_label(cached) }, trailing gap {gap_days}d)'
            )
            return cached
        # Only fetch the gap, not the full window
        fetch_days = min(days, gap_days)
        print(f'      {ticker_symbol} 1h: cache has {len(cached)} rows, gap {gap_days}d, fetching {fetch_days}d from IBKR...')
    else:
        fetch_days = days

    df = _fetch_live(ticker_symbol, '1h', fetch_days, client_id=client_id)
    if not df.empty:
        save_ohlc(ticker_symbol, '1h', df)
        print(f'      {ticker_symbol} 1h: IBKR returned {len(df)} rows')
        if not cached.empty:
            combined = pd.concat([cached[~cached.index.isin(df.index)], df]).sort_index()
            return combined
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
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    cached = load_ohlc(ticker_symbol, '1m', start, end)
    gap_days = _trailing_gap_days(cached, interval='1m')
    if gap_days <= 0:
        print(f'      {ticker_symbol} 1m: cache up to date ({len(cached)} rows)')
        return cached
    if allow_stale_cache and not cached.empty:
        print(
            f'      {ticker_symbol} 1m: using cached history without refresh '
            f'({len(cached)} rows, last { _last_cached_label(cached) }, trailing gap {gap_days}d)'
        )
        return cached
    fetch_days = min(days, gap_days)
    print(f'      {ticker_symbol} 1m: cache has {len(cached)} rows, gap {gap_days}d, fetching {fetch_days}d from IBKR...')

    df = _fetch_live(ticker_symbol, '1m', fetch_days, client_id=client_id)
    if not df.empty:
        save_ohlc(ticker_symbol, '1m', df)
        print(f'      {ticker_symbol} 1m: IBKR returned {len(df)} rows')
        if not cached.empty:
            combined = pd.concat([cached[~cached.index.isin(df.index)], df]).sort_index()
            return combined
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
