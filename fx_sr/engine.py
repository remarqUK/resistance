"""Unified per-pair engine for backtest and live modes.

Provides a single code path: load data → walk-forward → branch.

    engine = PairEngine(pair, pair_info, params, ...)
    engine.load_data(cache_only=True)      # step 1: backfill
    result = engine.run_historical()       # step 2: walk-forward
    result = engine.finalize()             # step 3a: backtest — force-close, done
    # OR
    result = engine.process_new_bars(df)   # step 3b: live — keep going
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

import pandas as pd

from .config import PAIRS
from .data_pipeline import PairDataBundle, load_pair_data
from .levels import SRZone
from .strategy import Signal, StrategyParams, Trade, get_market_exit_price
from .walkforward import (
    WalkForwardResult,
    WalkForwardState,
    finalize_trade,
    make_execution_quote_provider,
    make_zone_provider,
    resume_walk_forward,
    run_walk_forward,
)


class PairEngine:
    """Unified per-pair engine for backtest and live modes.

    Lifecycle::

        engine = PairEngine('EURUSD', pair_info, params, ...)
        engine.load_data(cache_only=True)     # backfill / load
        result = engine.run_historical()      # walk-forward over history

        # Backtest: force-close and get stats
        result = engine.finalize()

        # Live: keep processing new bars as they arrive
        result = engine.process_new_bars(extended_df)
    """

    def __init__(
        self,
        pair: str,
        pair_info: dict,
        params: StrategyParams,
        *,
        zone_history_days: int = 180,
        hourly_days: int = 365,
        execution_mode: str = 'intrabar',
        snapshot_source: str = '',
    ) -> None:
        self.pair = pair
        self.pair_info = pair_info
        self.params = params
        self.zone_history_days = zone_history_days
        self.hourly_days = hourly_days
        self.execution_mode = execution_mode
        self.snapshot_source = snapshot_source
        self.pip = float(pair_info.get('pip', 0.0001))

        self._data: PairDataBundle | None = None
        self._state: WalkForwardState | None = None
        self._zone_provider: Callable | None = None
        self._quote_provider: Callable | None = None
        self._last_result: WalkForwardResult | None = None

    @property
    def state(self) -> WalkForwardState | None:
        return self._state

    @property
    def data(self) -> PairDataBundle | None:
        return self._data

    @property
    def last_result(self) -> WalkForwardResult | None:
        return self._last_result

    def load_data(
        self,
        *,
        cache_only: bool = True,
        exclude_forming_bar: bool = False,
        allow_stale_cache: bool = False,
        debug: bool = False,
        daily_data_cache: dict | None = None,
        hourly_data_cache: dict | None = None,
        minute_data_cache: dict | None = None,
    ) -> PairDataBundle:
        """Step 1: Load / backfill data and build providers."""
        self._data = load_pair_data(
            self.pair,
            self.pair_info,
            hourly_days=self.hourly_days,
            zone_history_days=self.zone_history_days,
            cache_only=cache_only,
            exclude_forming_bar=exclude_forming_bar,
            allow_stale_cache=allow_stale_cache,
            debug=debug,
            daily_data_cache=daily_data_cache,
            hourly_data_cache=hourly_data_cache,
            minute_data_cache=minute_data_cache,
        )
        self._zone_provider = make_zone_provider(
            self._data.daily_df,
            self.zone_history_days,
            cache={},
        )
        self._quote_provider = make_execution_quote_provider(
            self.params,
            self._data.minute_df,
            self._data.l2_snapshots,
        )
        return self._data

    def load_bundle(self, bundle: PairDataBundle) -> PairDataBundle:
        """Load a prebuilt data bundle and build providers.

        This lets higher-level orchestration preload/cache data once and still
        run the unified engine for walk-forward logic.
        """

        self._data = bundle
        self._zone_provider = make_zone_provider(
            bundle.daily_df,
            self.zone_history_days,
            cache={},
        )
        self._quote_provider = make_execution_quote_provider(
            self.params,
            bundle.minute_df,
            bundle.l2_snapshots,
        )
        return self._data

    def restore_state(self, state: WalkForwardState | None) -> None:
        """Restore a saved walk-forward state for incremental live resumes."""

        self._state = state

    def run_historical(self, *, force_close_end: bool = False) -> WalkForwardResult:
        """Step 2: Walk-forward over all historical data.

        Use ``force_close_end=False`` (default) to preserve an open trade
        for live continuation.  Use ``True`` for backtest final stats.
        """
        if self._data is None:
            raise RuntimeError('Call load_data() before run_historical()')
        if self._zone_provider is None or self._quote_provider is None:
            raise RuntimeError('Providers not initialized — call load_data()')

        result = run_walk_forward(
            self._data.hourly_df,
            pair=self.pair,
            params=self.params,
            pip=self.pip,
            zone_provider=self._zone_provider,
            execution_quote_provider=self._quote_provider,
            minute_df=self._data.minute_df,
            execution_mode=self.execution_mode,
            force_close_end=force_close_end,
            snapshot_source=self.snapshot_source,
            daily_df=self._data.daily_df,
        )
        self._state = result.state
        self._last_result = result
        return result

    def finalize(self) -> WalkForwardResult:
        """Step 3a (backtest): Force-close any open trade and return final result.

        Call after ``run_historical(force_close_end=False)`` if you deferred
        the close decision, or after ``run_historical(force_close_end=True)``
        as a no-op passthrough.
        """
        if self._last_result is None:
            raise RuntimeError('Call run_historical() before finalize()')

        result = self._last_result
        if result.open_trade is not None and self._data is not None:
            hourly_df = self._data.hourly_df
            if not hourly_df.empty:
                trade = result.open_trade
                bars_held = len(hourly_df) - 1
                if self._state is not None:
                    bars_held = max(0, self._state.bars_processed - 1 - self._state.trade_entry_bar)
                end_exit = finalize_trade(
                    trade,
                    hourly_df.index[-1],
                    get_market_exit_price(
                        float(hourly_df['Close'].iloc[-1]),
                        trade.direction,
                        self.pip,
                        self.params,
                    ),
                    'END',
                    bars_held,
                    self.pip,
                )
                trades = list(result.trades) + [end_exit]
                result = WalkForwardResult(
                    trades=trades,
                    zones=result.zones,
                    open_trade=None,
                    state=result.state,
                )
                self._last_result = result
        return result

    def process_new_bars(
        self,
        hourly_df: pd.DataFrame,
        *,
        minute_df: pd.DataFrame | None = None,
    ) -> WalkForwardResult:
        """Step 3b (live): Process new bars incrementally.

        The *hourly_df* must contain the checkpoint bar from the previous
        run plus any new bars.  Falls back to full replay if the state
        cannot be aligned.
        """
        if self._state is None:
            raise RuntimeError('No state to resume from — call run_historical() first')
        if self._zone_provider is None or self._quote_provider is None:
            raise RuntimeError('Providers not initialized — call load_data()')

        # Rebuild quote provider if minute_df was updated
        quote_provider = self._quote_provider
        if minute_df is not None:
            quote_provider = make_execution_quote_provider(
                self.params,
                minute_df,
                self._data.l2_snapshots if self._data else pd.DataFrame(),
            )

        _daily = self._data.daily_df if self._data else None
        _minute = minute_df if minute_df is not None else (self._data.minute_df if self._data else None)

        try:
            result = resume_walk_forward(
                self._state,
                hourly_df,
                pair=self.pair,
                params=self.params,
                pip=self.pip,
                zone_provider=self._zone_provider,
                execution_quote_provider=quote_provider,
                minute_df=_minute,
                execution_mode=self.execution_mode,
                force_close_end=False,
                snapshot_source=self.snapshot_source,
                daily_df=_daily,
            )
        except ValueError:
            # State doesn't align with new df — full replay
            result = run_walk_forward(
                hourly_df,
                pair=self.pair,
                params=self.params,
                pip=self.pip,
                zone_provider=self._zone_provider,
                execution_quote_provider=quote_provider,
                minute_df=_minute,
                execution_mode=self.execution_mode,
                force_close_end=False,
                snapshot_source=self.snapshot_source,
                daily_df=_daily,
            )

        self._state = result.state
        self._last_result = result
        return result

    def signal_from_open_trade(self) -> Signal | None:
        """Build a Signal from the current open trade, if any."""
        if self._last_result is None or self._last_result.open_trade is None:
            return None
        trade = self._last_result.open_trade
        return Signal(
            time=trade.entry_time,
            pair=self.pair,
            direction=trade.direction,
            entry_price=trade.entry_price,
            sl_price=trade.sl_price,
            tp_price=trade.tp_price,
            zone_upper=trade.zone_upper,
            zone_lower=trade.zone_lower,
            zone_strength=trade.zone_strength,
            zone_type='support' if trade.direction == 'LONG' else 'resistance',
            quality_score=trade.quality_score,
        )
