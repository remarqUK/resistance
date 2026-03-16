"""Tick-reactive streaming scanner for real-time signal detection.

Uses a two-tier architecture:
  Tier 1 (every tick): cheap zone proximity check
  Tier 2 (on-demand):  full hourly-bar signal evaluation when near a zone

Zones refresh once per day. Hourly bars are fetched on-demand when a tick
passes the zone gate. TP/SL exits are checked on every tick for tracked
positions.
"""

from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import Dict, List, Optional, Set

import pandas as pd

from .config import PAIRS, DEFAULT_ZONE_HISTORY_DAYS
from .levels import SRZone, is_price_in_zone
from .live import (
    NEAR_ZONE_THRESHOLD_PCT,
    _distance_to_zone_pct,
    _get_live_zones,
)
from .positions import calc_pnl_pips, pair_pip
from .strategy import (
    Signal,
    StrategyParams,
    check_price_exit,
    get_tradeable_zones,
    is_pair_fully_blocked,
    select_entry_signal,
)


def check_tick_exit(
    trade,
    price: float,
    pip: float,
    params: StrategyParams,
) -> Optional[tuple[str, float]]:
    """Tick-level TP/SL/early-exit check using the shared exit engine."""

    return check_price_exit(
        trade,
        price,
        price,
        price,
        params=params,
        pip=pip,
    )


class StreamingScanner:
    """Tick-reactive scanner that triggers signal evaluation on zone proximity."""

    def __init__(
        self,
        pairs: Dict | None = None,
        params: StrategyParams | None = None,
        zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS,
        *,
        eval_cooldown_seconds: float = 5.0,
        near_zone_pct: float = NEAR_ZONE_THRESHOLD_PCT,
    ) -> None:
        self.pairs = pairs if pairs is not None else PAIRS
        self.params = params if params is not None else StrategyParams()
        self.zone_history_days = zone_history_days
        self.eval_cooldown_seconds = eval_cooldown_seconds
        self.near_zone_pct = near_zone_pct

        # Cached zones per pair: (support, resistance, all_zones)
        self._zones: Dict[str, tuple[Optional[SRZone], Optional[SRZone], List[SRZone]]] = {}
        self._zones_day: str = ''

        # Debounce: pair -> monotonic time of last evaluation
        self._last_eval_time: Dict[str, float] = {}

        # Last signal emitted per pair (to avoid duplicate signals)
        self._last_signal_id: Dict[str, str] = {}

    def refresh_zones(self, price_hints: Optional[Dict[str, float]] = None) -> None:
        """Refresh daily zones for all pairs.

        Args:
            price_hints: optional latest prices per pair to select nearest zones.
                         If omitted, the latest daily close is used.
        """

        day_bucket = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        if day_bucket == self._zones_day and self._zones:
            return

        daily_data_cache: Dict[tuple[str, int], object] = {}
        zone_cache: Dict[tuple[str, int], List[SRZone]] = {}

        for pair_id, pair_info in self.pairs.items():
            if is_pair_fully_blocked(pair_id, self.params):
                continue
            ticker = pair_info.get('ticker')
            if not ticker:
                continue

            daily_df, zones = _get_live_zones(
                ticker,
                self.zone_history_days,
                daily_data_cache=daily_data_cache,
                zone_cache=zone_cache,
            )
            if daily_df.empty:
                self._zones[pair_id] = (None, None, [])
                continue

            ref_price = (price_hints or {}).get(pair_id) or float(daily_df['Close'].iloc[-1])
            support, resistance = get_tradeable_zones(zones, ref_price)
            self._zones[pair_id] = (support, resistance, zones)

        self._zones_day = day_bucket

    def _is_near_zone(self, pair: str, price: float) -> bool:
        """Tier 1 gate: is price near or inside a cached zone?

        Re-derives nearest zones from the full zone list using the live
        price so the gate stays accurate even when price has moved far
        from the reference price used at zone-refresh time.
        """

        zone_data = self._zones.get(pair)
        if zone_data is None:
            return False

        _, _, all_zones = zone_data
        support, resistance = get_tradeable_zones(all_zones, price)
        for zone, is_support in ((support, True), (resistance, False)):
            if zone is None:
                continue
            if is_price_in_zone(price, zone):
                return True
            dist = _distance_to_zone_pct(price, zone, is_support=is_support)
            if dist is not None and dist <= self.near_zone_pct:
                return True

        return False

    def _evaluate_signal(
        self,
        pair: str,
        price: float,
        tracked_pairs: Optional[Dict[str, Set[str]]] = None,
        blocked_pairs: Optional[Set[str]] = None,
        hourly_df: Optional[pd.DataFrame] = None,
    ) -> Optional[Signal]:
        """Tier 2: run full entry evaluation using provided hourly bars.

        If ``hourly_df`` is not provided, falls back to fetching from IBKR
        (for ``--once`` mode compatibility).
        """

        pair_info = self.pairs.get(pair)
        if pair_info is None:
            return None

        # Re-derive nearest zones using the live price
        zone_data = self._zones.get(pair)
        if zone_data is None:
            return None
        _, _, all_zones = zone_data
        support, resistance = get_tradeable_zones(all_zones, price)

        # Skip if pair has an open tracked position or pending order
        if tracked_pairs and pair in tracked_pairs:
            return None
        if blocked_pairs and pair in blocked_pairs:
            return None

        if hourly_df is None:
            from .live import _get_live_hourly_data
            ticker = pair_info.get('ticker')
            if not ticker:
                return None
            hourly_df = _get_live_hourly_data(ticker, days=3)
        if hourly_df.empty:
            return None

        signal = select_entry_signal(
            hourly_df=hourly_df,
            bar_idx=len(hourly_df) - 1,
            pair=pair,
            params=self.params,
            support_zone=support,
            resistance_zone=resistance,
        )

        return signal

    def _dedupe_signal(self, pair: str, signal: Optional[Signal]) -> Optional[Signal]:
        """Suppress repeated emission of the same signal identity."""

        if signal is None:
            return None

        signal_key = f"{signal.pair}:{signal.direction}:{signal.time}"
        if self._last_signal_id.get(pair) == signal_key:
            return None
        self._last_signal_id[pair] = signal_key
        return signal

    def evaluate_completed_bar(
        self,
        pair: str,
        price: float,
        tracked_pairs: Optional[Dict[str, Set[str]]] = None,
        blocked_pairs: Optional[Set[str]] = None,
        hourly_df: Optional[pd.DataFrame] = None,
    ) -> Optional[Signal]:
        """Evaluate one finalized hourly bar without tick gating or cooldown."""

        signal = self._evaluate_signal(
            pair,
            price,
            tracked_pairs=tracked_pairs,
            blocked_pairs=blocked_pairs,
            hourly_df=hourly_df,
        )
        return self._dedupe_signal(pair, signal)

    def on_tick(
        self,
        pair: str,
        price: float,
        tracked_pairs: Optional[Dict[str, Set[str]]] = None,
        blocked_pairs: Optional[Set[str]] = None,
        hourly_df: Optional[pd.DataFrame] = None,
    ) -> Optional[Signal]:
        """Process a single price tick and return a signal if one is generated.

        This is the main entry point called from the quote stream callback.
        Returns a Signal if the tick triggers a valid entry, None otherwise.
        """

        if pair not in self.pairs:
            return None

        # Tier 1: cheap zone proximity check
        if not self._is_near_zone(pair, price):
            return None

        # Debounce: don't re-evaluate the same pair too frequently
        now = time.monotonic()
        last_eval = self._last_eval_time.get(pair, 0.0)
        if now - last_eval < self.eval_cooldown_seconds:
            return None

        self._last_eval_time[pair] = now

        # Tier 2: full hourly evaluation
        signal = self._evaluate_signal(
            pair, price,
            tracked_pairs=tracked_pairs,
            blocked_pairs=blocked_pairs,
            hourly_df=hourly_df,
        )

        if signal is None:
            return None

        return self._dedupe_signal(pair, signal)

    def check_tick_exits(
        self,
        pair: str,
        price: float,
        tracked: Dict[str, dict],
    ) -> List[dict]:
        """Check tick-level TP/SL/zone-break exits for tracked positions on this pair.

        Returns a list of exit alert dicts (usually 0 or 1).
        """

        alerts: List[dict] = []
        for key, info in tracked.items():
            if info['pair'] != pair:
                continue
            trade = info['trade']
            pip = pair_pip(pair)
            result = check_tick_exit(trade, price, pip, self.params)
            if result is None:
                continue

            exit_reason, exit_price = result
            alerts.append({
                'pair': pair,
                'direction': trade.direction,
                'exit_reason': exit_reason,
                'exit_price': exit_price,
                'entry_price': trade.entry_price,
                'current_price': price,
                'pnl_pips': calc_pnl_pips(trade, price, pip, self.params),
                'bars_monitored': info.get('bars_monitored', 0),
                'source': 'tick',
            })

        return alerts
