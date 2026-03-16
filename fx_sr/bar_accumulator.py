"""Maintain hourly OHLC bars from real-time 5-second bar updates.

The accumulator is seeded once with historical hourly data at startup,
then updated tick-by-tick from IBKR ``reqRealTimeBars``.  The resulting
DataFrame has the same schema as ``fetch_hourly_data()`` so the strategy
evaluation code is unchanged.
"""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Callable, Dict, List, Optional

import pandas as pd

_LOGGER = logging.getLogger(__name__)


def _hour_start(ts: datetime | pd.Timestamp) -> pd.Timestamp:
    """Return the start of the UTC hour for a given timestamp."""

    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize('UTC')
    else:
        t = t.tz_convert('UTC')
    return t.replace(minute=0, second=0, microsecond=0, nanosecond=0)


class HourlyBarAccumulator:
    """Build and maintain hourly OHLC bars from 5-second real-time bars.

    Usage::

        acc = HourlyBarAccumulator()
        acc.seed('EURUSD', hourly_df)          # backfill
        acc.on_realtime_bar('EURUSD', bar)     # each 5s bar
        df = acc.get_hourly_df('EURUSD')       # for signal eval
    """

    def __init__(self) -> None:
        self._completed: Dict[str, pd.DataFrame] = {}
        self._current: Dict[str, dict] = {}
        self._on_bar_complete: List[Callable[[str, pd.Timestamp], None]] = []
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

        hour = _hour_start(bar_time)
        current = self._current.get(pair)

        if current is not None and current['hour'] != hour:
            # Hour boundary crossed — finalize the old bar
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

    def on_price_tick(self, pair: str, price: float) -> None:
        """Update the current bar's high/low/close from a plain tick price.

        This is a lightweight alternative to ``on_realtime_bar`` when only
        a mid-price is available (e.g. from ``stream_live_quotes``).
        """

        hour = _hour_start(pd.Timestamp.now('UTC'))
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

    def _finalize_bar(self, pair: str) -> None:
        """Append the current bar to completed bars and notify listeners."""

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

    def get_completed_df(self, pair: str, tail_n: int = 168) -> pd.DataFrame:
        """Return completed hourly bars only, excluding the in-progress bar."""

        completed = self._completed.get(pair)
        if completed is None:
            completed = pd.DataFrame(
                columns=['Open', 'High', 'Low', 'Close', 'Volume'],
            )

        if tail_n and len(completed) > tail_n:
            return completed.iloc[-tail_n:]
        return completed

    def get_hourly_df(self, pair: str, tail_n: int = 168) -> pd.DataFrame:
        """Return completed bars + the in-progress bar as the last row.

        The result has the same schema as ``fetch_hourly_data()``.
        """

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

    def get_latest_price(self, pair: str) -> Optional[float]:
        """Return the latest close price from the current or last completed bar."""

        current = self._current.get(pair)
        if current is not None:
            return current['close']
        completed = self._completed.get(pair)
        if completed is not None and not completed.empty:
            return float(completed['Close'].iloc[-1])
        return None
