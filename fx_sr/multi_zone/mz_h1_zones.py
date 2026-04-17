"""Compute H1-timeframe S/R zones for the multi-zone prototype.

Uses the same `detect_zones` primitive but operates on the hourly OHLC
frame with a smaller pivot window (hours, not days). Produces many more
zones per date than the D1 detector. Intended to be merged with D1
zones as additional tradeable candidates.

Output shape matches `precompute_zone_cache` — `dict[(pair, date_str), list[SRZone]]`.
"""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from typing import Optional

import pandas as pd

from ..levels import SRZone, detect_zones


def _compute_h1_zones_for_pair(args) -> dict[tuple, list[SRZone]]:
    """Worker: walk each unique date in the hourly frame and compute H1
    zones using a trailing window of `lookback_bars` hourly bars."""
    pair, hourly_df, lookback_bars, pivot_window, cluster_tolerance, major_threshold, max_zone_width_pct = args
    result: dict[tuple, list[SRZone]] = {}
    seen = set()
    ordered_index = hourly_df.index
    for i, ts in enumerate(ordered_index):
        d = ts.date() if hasattr(ts, 'date') else ts
        ds = str(d)
        if ds in seen:
            continue
        seen.add(ds)
        lo = max(0, i - lookback_bars)
        window = hourly_df.iloc[lo:i + 1]
        if len(window) >= 40:
            try:
                zones = detect_zones(
                    window,
                    pivot_window=pivot_window,
                    cluster_tolerance=cluster_tolerance,
                    major_threshold=major_threshold,
                    max_zone_width_pct=max_zone_width_pct,
                )
                result[(pair, ds)] = zones
            except Exception:
                result[(pair, ds)] = []
        else:
            result[(pair, ds)] = []
    return result


def build_h1_zone_cache(
    hourly_df_by_pair: dict[str, pd.DataFrame],
    *,
    lookback_bars: int = 500,       # ~21 days of hourly bars
    pivot_window: int = 12,         # hours left/right for pivot
    cluster_tolerance: float = 0.08,
    major_threshold: int = 3,
    max_zone_width_pct: float = 0.35,
    max_workers: Optional[int] = None,
) -> dict[tuple, list[SRZone]]:
    """Build an H1-timeframe zone cache in parallel across pairs."""

    work = []
    for pair, hourly in hourly_df_by_pair.items():
        if hourly is None or hourly.empty:
            continue
        work.append((
            pair, hourly, lookback_bars, pivot_window,
            cluster_tolerance, major_threshold, max_zone_width_pct,
        ))
    if not work:
        return {}

    out: dict[tuple, list[SRZone]] = {}
    if max_workers is None or max_workers <= 1 or len(work) <= 1:
        for args in work:
            out.update(_compute_h1_zones_for_pair(args))
    else:
        with ProcessPoolExecutor(max_workers=min(max_workers, len(work))) as pool:
            for part in pool.map(_compute_h1_zones_for_pair, work):
                out.update(part)
    return out
