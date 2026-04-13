"""Maintain hourly and minute OHLC bars from real-time 5-second bar updates.

The accumulator is seeded once with historical data at startup, then updated
tick-by-tick from IBKR ``reqRealTimeBars``.  The hourly DataFrame has the same
schema as ``fetch_hourly_data()`` so the strategy evaluation code is unchanged.
Minute bars are available for charting via ``get_minute_df()``.
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
import threading
import time
from typing import Callable, Dict, List, Optional

import pandas as pd

_LOGGER = logging.getLogger(__name__)

_MINUTE_TAIL = 3000  # rolling cap: ~50 hours of minute bars kept in memory
_PERSIST_INTERVAL = 60  # seconds between bulk saves


def _hour_start(ts: datetime | pd.Timestamp) -> pd.Timestamp:
    """Return the start of the UTC hour for a given timestamp."""

    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize('UTC')
    else:
        t = t.tz_convert('UTC')
    return t.replace(minute=0, second=0, microsecond=0, nanosecond=0)


def _minute_start(ts: datetime | pd.Timestamp) -> pd.Timestamp:
    """Return the start of the UTC minute for a given timestamp."""

    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize('UTC')
    else:
        t = t.tz_convert('UTC')
    return t.replace(second=0, microsecond=0, nanosecond=0)


class HourlyBarAccumulator:
    """Build and maintain hourly and minute OHLC bars from 5-second real-time bars.

    Usage::

        acc = HourlyBarAccumulator()
        acc.seed('EURUSD', hourly_df)              # backfill hourly
        acc.seed_minutes('EURUSD', minute_df)      # backfill minute
        acc.on_realtime_bar('EURUSD', bar)         # each 5s bar
        df = acc.get_hourly_df('EURUSD')           # for signal eval
        df = acc.get_minute_df('EURUSD')           # for charting
    """

    def __init__(self) -> None:
        # Hourly bars
        self._completed: Dict[str, pd.DataFrame] = {}
        self._current: Dict[str, dict] = {}
        self._on_bar_complete: List[Callable[[str, pd.Timestamp], None]] = []

        # Minute bars
        self._completed_minutes: Dict[str, pd.DataFrame] = {}
        self._current_minute_bar: Dict[str, dict] = {}

        self._seeded: set[str] = set()

    @property
    def seeded_pairs(self) -> set[str]:
        return set(self._seeded)

    def seed(self, pair: str, hourly_df: pd.DataFrame) -> None:
        """Initialize a pair with backfilled historical hourly data."""

        if hourly_df.empty:
            self._completed[pair] = pd.DataFrame(
                columns=['Open', 'High', 'Low', 'Close', 'Volume'],
            )
        else:
            self._completed[pair] = hourly_df.copy()

        self._current.pop(pair, None)
        self._seeded.add(pair)

    def seed_minutes(self, pair: str, minute_df: pd.DataFrame) -> None:
        """Initialize a pair with backfilled historical minute data."""

        if minute_df.empty:
            self._completed_minutes[pair] = pd.DataFrame(
                columns=['Open', 'High', 'Low', 'Close', 'Volume'],
            )
        else:
            df = minute_df.copy()
            if len(df) > _MINUTE_TAIL:
                df = df.iloc[-_MINUTE_TAIL:]
            self._completed_minutes[pair] = df

        self._current_minute_bar.pop(pair, None)
        self._rebuild_current_hour_from_seeded_minutes(pair)

    def _rebuild_current_hour_from_seeded_minutes(self, pair: str) -> None:
        """Reconstruct the in-progress hourly bar from seeded minute history."""

        minute_df = self._completed_minutes.get(pair)
        if minute_df is None or minute_df.empty:
            self._current.pop(pair, None)
            return

        last_minute = pd.Timestamp(minute_df.index[-1])
        if last_minute.tzinfo is None:
            last_minute = last_minute.tz_localize('UTC')
        else:
            last_minute = last_minute.tz_convert('UTC')

        hour = _hour_start(last_minute)
        hour_end = hour + pd.Timedelta(hours=1)
        hour_slice = minute_df[(minute_df.index >= hour) & (minute_df.index < hour_end)]
        if hour_slice.empty:
            self._current.pop(pair, None)
            return

        completed = self._completed.get(pair)
        if completed is not None and not completed.empty:
            last_completed_hour = pd.Timestamp(completed.index[-1])
            if last_completed_hour.tzinfo is None:
                last_completed_hour = last_completed_hour.tz_localize('UTC')
            else:
                last_completed_hour = last_completed_hour.tz_convert('UTC')
            if last_completed_hour > hour:
                return

        self._current[pair] = {
            'hour': hour,
            'open': float(hour_slice['Open'].iloc[0]),
            'high': float(hour_slice['High'].max()),
            'low': float(hour_slice['Low'].min()),
            'close': float(hour_slice['Close'].iloc[-1]),
            'volume': float(hour_slice['Volume'].sum()),
        }

    def on_bar_complete(self, callback: Callable[[str, pd.Timestamp], None]) -> None:
        """Register a callback fired when an hourly bar completes.

        Signature: ``callback(pair, bar_time)``
        """
        self._on_bar_complete.append(callback)

    def on_realtime_bar(self, pair: str, bar) -> None:
        """Process one 5-second real-time bar from IBKR.

        ``bar`` should have ``.time``, ``.open_``, ``.high``, ``.low``,
        ``.close``, and ``.volume`` attributes (ib_async ``RealTimeBar``).
        """

        bar_time = getattr(bar, 'time', None)
        if bar_time is None:
            return

        o = float(getattr(bar, 'open_', 0) or 0)
        h = float(getattr(bar, 'high', 0) or 0)
        l = float(getattr(bar, 'low', 0) or 0)  # noqa: E741
        c = float(getattr(bar, 'close', 0) or 0)
        v = float(getattr(bar, 'volume', 0) or 0)

        # --- Hourly accumulation ---
        hour = _hour_start(bar_time)
        current = self._current.get(pair)

        if current is not None and current['hour'] != hour:
            self._finalize_bar(pair)

        if current is None or pair not in self._current:
            self._current[pair] = {
                'hour': hour,
                'open': o, 'high': h, 'low': l, 'close': c,
                'volume': v,
            }
        else:
            cur = self._current[pair]
            cur['high'] = max(cur['high'], h)
            cur['low'] = min(cur['low'], l)
            cur['close'] = c
            cur['volume'] = cur['volume'] + v

        # --- Minute accumulation ---
        minute = _minute_start(bar_time)
        cur_min = self._current_minute_bar.get(pair)

        if cur_min is not None and cur_min['minute'] != minute:
            self._finalize_minute_bar(pair)

        if pair not in self._current_minute_bar:
            self._current_minute_bar[pair] = {
                'minute': minute,
                'open': o, 'high': h, 'low': l, 'close': c,
                'volume': v,
            }
        else:
            cur_min = self._current_minute_bar[pair]
            cur_min['high'] = max(cur_min['high'], h)
            cur_min['low'] = min(cur_min['low'], l)
            cur_min['close'] = c
            cur_min['volume'] = cur_min['volume'] + v

    def on_price_tick(self, pair: str, price: float) -> None:
        """Update the current bar's high/low/close from a plain tick price.

        This is a lightweight alternative to ``on_realtime_bar`` when only
        a mid-price is available (e.g. from ``stream_live_quotes``).
        """

        now = pd.Timestamp.now('UTC')

        # --- Hourly ---
        hour = _hour_start(now)
        current = self._current.get(pair)

        if current is not None and current['hour'] != hour:
            self._finalize_bar(pair)
            current = None

        if current is None or pair not in self._current:
            self._current[pair] = {
                'hour': hour,
                'open': price, 'high': price, 'low': price, 'close': price,
                'volume': 0.0,
            }
        else:
            cur = self._current[pair]
            cur['high'] = max(cur['high'], price)
            cur['low'] = min(cur['low'], price)
            cur['close'] = price

        # --- Minute ---
        minute = _minute_start(now)
        cur_min = self._current_minute_bar.get(pair)

        if cur_min is not None and cur_min['minute'] != minute:
            self._finalize_minute_bar(pair)
            cur_min = None

        if cur_min is None or pair not in self._current_minute_bar:
            self._current_minute_bar[pair] = {
                'minute': minute,
                'open': price, 'high': price, 'low': price, 'close': price,
                'volume': 0.0,
            }
        else:
            cur_min = self._current_minute_bar[pair]
            cur_min['high'] = max(cur_min['high'], price)
            cur_min['low'] = min(cur_min['low'], price)
            cur_min['close'] = price

    def _finalize_bar(self, pair: str) -> None:
        """Append the current hourly bar to completed bars and notify listeners."""

        cur = self._current.pop(pair, None)
        if cur is None:
            return

        new_row = pd.DataFrame(
            [{
                'Open': cur['open'],
                'High': cur['high'],
                'Low': cur['low'],
                'Close': cur['close'],
                'Volume': cur['volume'],
            }],
            index=pd.DatetimeIndex([cur['hour']], name='Date'),
        )

        existing = self._completed.get(pair)
        if existing is not None and not existing.empty:
            if cur['hour'] in existing.index:
                existing = existing.drop(cur['hour'])
            self._completed[pair] = pd.concat([existing, new_row])
        else:
            self._completed[pair] = new_row

        for callback in self._on_bar_complete:
            try:
                callback(pair, cur['hour'])
            except Exception:
                _LOGGER.exception("Hourly bar completion callback failed for %s", pair)

    def _finalize_minute_bar(self, pair: str) -> None:
        """Append the current minute bar to completed minute bars."""

        cur = self._current_minute_bar.pop(pair, None)
        if cur is None:
            return

        new_row = pd.DataFrame(
            [{
                'Open': cur['open'],
                'High': cur['high'],
                'Low': cur['low'],
                'Close': cur['close'],
                'Volume': cur['volume'],
            }],
            index=pd.DatetimeIndex([cur['minute']], name='Date'),
        )

        existing = self._completed_minutes.get(pair)
        if existing is not None and not existing.empty:
            if cur['minute'] in existing.index:
                existing = existing.drop(cur['minute'])
            combined = pd.concat([existing, new_row])
            # Keep rolling window to avoid unbounded memory growth
            if len(combined) > _MINUTE_TAIL:
                combined = combined.iloc[-_MINUTE_TAIL:]
            self._completed_minutes[pair] = combined
        else:
            self._completed_minutes[pair] = new_row

    def get_completed_df(self, pair: str, tail_n: int = 0) -> pd.DataFrame:
        """Return completed hourly bars only, excluding the in-progress bar."""

        completed = self._completed.get(pair)
        if completed is None:
            completed = pd.DataFrame(
                columns=['Open', 'High', 'Low', 'Close', 'Volume'],
            )

        if tail_n and len(completed) > tail_n:
            return completed.iloc[-tail_n:]
        return completed

    def get_hourly_df(self, pair: str, tail_n: int = 0) -> pd.DataFrame:
        """Return completed hourly bars + the in-progress bar as the last row."""

        completed = self._completed.get(pair)
        if completed is None:
            completed = pd.DataFrame(
                columns=['Open', 'High', 'Low', 'Close', 'Volume'],
            )

        current = self._current.get(pair)
        if current is not None:
            in_progress = pd.DataFrame(
                [{
                    'Open': current['open'],
                    'High': current['high'],
                    'Low': current['low'],
                    'Close': current['close'],
                    'Volume': current['volume'],
                }],
                index=pd.DatetimeIndex([current['hour']], name='Date'),
            )
            if not completed.empty:
                if current['hour'] in completed.index:
                    completed = completed.drop(current['hour'])
                result = pd.concat([completed, in_progress])
            else:
                result = in_progress
        else:
            result = completed

        if tail_n and len(result) > tail_n:
            result = result.iloc[-tail_n:]

        return result

    def get_minute_df(self, pair: str, tail_n: int = _MINUTE_TAIL) -> pd.DataFrame:
        """Return completed minute bars + the in-progress bar as the last row."""

        completed = self._completed_minutes.get(pair)
        if completed is None:
            completed = pd.DataFrame(
                columns=['Open', 'High', 'Low', 'Close', 'Volume'],
            )

        cur_min = self._current_minute_bar.get(pair)
        if cur_min is not None:
            in_progress = pd.DataFrame(
                [{
                    'Open': cur_min['open'],
                    'High': cur_min['high'],
                    'Low': cur_min['low'],
                    'Close': cur_min['close'],
                    'Volume': cur_min['volume'],
                }],
                index=pd.DatetimeIndex([cur_min['minute']], name='Date'),
            )
            if not completed.empty:
                if cur_min['minute'] in completed.index:
                    completed = completed.drop(cur_min['minute'])
                result = pd.concat([completed, in_progress])
            else:
                result = in_progress
        else:
            result = completed

        if tail_n and len(result) > tail_n:
            result = result.iloc[-tail_n:]

        return result

    def get_latest_price(self, pair: str) -> Optional[float]:
        """Return the latest close price from the current or last completed bar."""

        current = self._current.get(pair)
        if current is not None:
            return current['close']
        completed = self._completed.get(pair)
        if completed is not None and not completed.empty:
            return float(completed['Close'].iloc[-1])
        return None

    # ------------------------------------------------------------------
    # Periodic persistence — bulk-save accumulated bars to PostgreSQL
    # ------------------------------------------------------------------

    def start_persistence(
        self,
        pair_ticker_map: Dict[str, str],
        interval: float = _PERSIST_INTERVAL,
    ) -> None:
        """Start a daemon thread that bulk-saves bars to PostgreSQL every *interval* seconds.

        *pair_ticker_map* maps pair IDs (e.g. ``'EURUSD'``) to cache ticker
        symbols (e.g. ``'EURUSD=X'``) used by ``save_ohlc``.
        """
        self._persist_stop = threading.Event()
        self._persist_ticker_map = dict(pair_ticker_map)
        self._persist_last_saved: Dict[str, Dict[str, pd.Timestamp]] = {}
        self._persist_thread = threading.Thread(
            target=self._persist_loop,
            args=(interval,),
            name='bar-persist',
            daemon=True,
        )
        self._persist_thread.start()
        _LOGGER.info("Bar persistence thread started (interval=%ss)", interval)

    def stop_persistence(self) -> None:
        """Stop the persistence thread and do a final flush."""
        stop = getattr(self, '_persist_stop', None)
        if stop is None:
            return
        stop.set()
        thread = getattr(self, '_persist_thread', None)
        if thread is not None:
            thread.join(timeout=10)
        self._flush_to_db()
        _LOGGER.info("Bar persistence thread stopped, final flush complete")

    def _persist_loop(self, interval: float) -> None:
        """Background loop: sleep then flush."""
        while not self._persist_stop.is_set():
            self._persist_stop.wait(timeout=interval)
            if not self._persist_stop.is_set():
                self._flush_to_db()
        # Final flush on stop is handled by stop_persistence

    def _flush_to_db(self) -> None:
        """Bulk-save new completed hourly and minute bars to PostgreSQL."""
        from .db import save_ohlc

        ticker_map = getattr(self, '_persist_ticker_map', {})
        last_saved = getattr(self, '_persist_last_saved', {})

        for pair in list(self._completed.keys()):
            ticker = ticker_map.get(pair)
            if not ticker:
                continue
            self._save_interval(pair, ticker, '1h', self._completed, last_saved, save_ohlc)

        for pair in list(self._completed_minutes.keys()):
            ticker = ticker_map.get(pair)
            if not ticker:
                continue
            self._save_interval(pair, ticker, '1m', self._completed_minutes, last_saved, save_ohlc)

    @staticmethod
    def _save_interval(
        pair: str,
        ticker: str,
        interval: str,
        store: Dict[str, pd.DataFrame],
        last_saved: Dict[str, Dict[str, pd.Timestamp]],
        save_fn,
    ) -> None:
        """Save only new bars (after last saved timestamp) for one pair+interval."""
        df = store.get(pair)
        if df is None or df.empty:
            return

        key = f"{pair}:{interval}"
        cutoff = last_saved.get(key)
        if cutoff is not None:
            new_bars = df[df.index > cutoff]
        else:
            new_bars = df

        if new_bars.empty:
            return

        try:
            save_fn(ticker, interval, new_bars)
            last_saved[key] = new_bars.index[-1]
            _LOGGER.debug("Saved %d %s bars for %s", len(new_bars), interval, pair)
        except Exception:
            _LOGGER.exception("Failed to save %s bars for %s", interval, pair)
