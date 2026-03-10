"""Support and resistance ZONE detection from daily chart data.

Strategy (from video):
- Use daily/weekly charts to find "painfully obvious" zones
- A zone is a price RANGE (upper/lower) where price has bounced multiple times
- Only use major zones closest to current price (one above, one below)
- Zone quality matters: clear, consistent bounces = tradeable zone

Algorithm:
1. Find pivot highs/lows on daily candles
2. Build TIGHT zones from the wick/reversal area of pivot candles
   - Resistance zone: from body top to candle high (the rejection wick)
   - Support zone: from candle low to body bottom (the rejection wick)
3. Cluster nearby pivot zones together
4. Count bounces where price enters the zone and reverses
5. Return nearest major zones above and below current price
"""

import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import List, Optional

from .config import (
    DEFAULT_PIVOT_WINDOW,
    DEFAULT_CLUSTER_TOL,
    DEFAULT_MAJOR_TOUCHES,
    DEFAULT_MAX_ZONE_WIDTH_PCT,
)


@dataclass
class SRZone:
    """A support or resistance price zone (range, not a single level)."""
    upper: float        # top boundary of the zone
    lower: float        # bottom boundary of the zone
    midpoint: float     # center of the zone
    touches: int        # number of bounces at this zone
    zone_type: str      # 'support' or 'resistance'
    strength: str       # 'major' or 'minor'
    first_seen: Optional[pd.Timestamp] = None
    last_seen: Optional[pd.Timestamp] = None


def find_daily_pivots(df: pd.DataFrame, left: int = 5, right: int = 5):
    """Find pivot highs and lows on daily candle data.

    Returns:
        (pivot_highs, pivot_lows)
        pivot_highs: list of (timestamp, high, body_top) for resistance zones
        pivot_lows: list of (timestamp, low, body_bottom) for support zones
    """
    highs = []
    lows = []

    high_vals = df['High'].values
    low_vals = df['Low'].values
    open_vals = df['Open'].values
    close_vals = df['Close'].values
    idx = df.index

    for i in range(left, len(df) - right):
        window_highs = high_vals[i - left:i + right + 1]
        if high_vals[i] == np.max(window_highs):
            body_top = max(open_vals[i], close_vals[i])
            highs.append((idx[i], float(high_vals[i]), float(body_top)))

        window_lows = low_vals[i - left:i + right + 1]
        if low_vals[i] == np.min(window_lows):
            body_bottom = min(open_vals[i], close_vals[i])
            lows.append((idx[i], float(low_vals[i]), float(body_bottom)))

    return highs, lows


def _build_resistance_zones(pivots, tolerance_pct: float) -> List[dict]:
    """Build resistance zones from pivot highs.

    For each pivot high, the zone is [body_top, high] - the rejection wick area.
    Cluster nearby pivots and combine their zones.
    """
    if not pivots:
        return []

    # Sort by the high price
    sorted_pivots = sorted(pivots, key=lambda x: x[1])
    clusters = [[sorted_pivots[0]]]

    for pivot in sorted_pivots[1:]:
        cluster = clusters[-1]
        cluster_ref = np.mean([p[1] for p in cluster])  # average high
        if abs(pivot[1] - cluster_ref) / cluster_ref * 100 <= tolerance_pct:
            cluster.append(pivot)
        else:
            clusters.append([pivot])

    zones = []
    for cluster in clusters:
        # Zone bounds: from lowest body_top to highest high
        zone_lower = float(min(p[2] for p in cluster))  # min body_top
        zone_upper = float(max(p[1] for p in cluster))  # max high
        times = [p[0] for p in cluster]

        zones.append({
            'upper': zone_upper,
            'lower': zone_lower,
            'midpoint': (zone_upper + zone_lower) / 2,
            'pivot_count': len(cluster),
            'first_seen': min(times),
            'last_seen': max(times),
        })

    return zones


def _build_support_zones(pivots, tolerance_pct: float) -> List[dict]:
    """Build support zones from pivot lows.

    For each pivot low, the zone is [low, body_bottom] - the rejection wick area.
    Cluster nearby pivots and combine their zones.
    """
    if not pivots:
        return []

    # Sort by the low price
    sorted_pivots = sorted(pivots, key=lambda x: x[1])
    clusters = [[sorted_pivots[0]]]

    for pivot in sorted_pivots[1:]:
        cluster = clusters[-1]
        cluster_ref = np.mean([p[1] for p in cluster])  # average low
        if abs(pivot[1] - cluster_ref) / cluster_ref * 100 <= tolerance_pct:
            cluster.append(pivot)
        else:
            clusters.append([pivot])

    zones = []
    for cluster in clusters:
        # Zone bounds: from lowest low to highest body_bottom
        zone_lower = float(min(p[1] for p in cluster))  # min low
        zone_upper = float(max(p[2] for p in cluster))  # max body_bottom
        times = [p[0] for p in cluster]

        zones.append({
            'upper': zone_upper,
            'lower': zone_lower,
            'midpoint': (zone_upper + zone_lower) / 2,
            'pivot_count': len(cluster),
            'first_seen': min(times),
            'last_seen': max(times),
        })

    return zones


def _count_zone_touches(df: pd.DataFrame, upper: float, lower: float) -> int:
    """Count how many daily candles interacted with a zone.

    A touch = candle's wick enters the zone (high >= lower or low <= upper)
    and the candle shows a bounce (doesn't close through the zone completely).
    """
    touches = 0
    for i in range(len(df)):
        candle_high = float(df['High'].iloc[i])
        candle_low = float(df['Low'].iloc[i])
        candle_close = float(df['Close'].iloc[i])

        # Candle must interact with the zone
        if candle_high < lower or candle_low > upper:
            continue

        # For a resistance zone: high enters zone, close below upper
        if candle_high >= lower and candle_close <= upper:
            touches += 1
        # For a support zone: low enters zone, close above lower
        elif candle_low <= upper and candle_close >= lower:
            touches += 1

    return touches


def detect_zones(
    df: pd.DataFrame,
    pivot_window: int = DEFAULT_PIVOT_WINDOW,
    cluster_tolerance: float = DEFAULT_CLUSTER_TOL,
    major_threshold: int = DEFAULT_MAJOR_TOUCHES,
    max_zone_width_pct: float = DEFAULT_MAX_ZONE_WIDTH_PCT,
) -> List[SRZone]:
    """Detect support and resistance zones from daily OHLC data.

    Args:
        df: Daily OHLC DataFrame
        pivot_window: bars left/right for pivot detection
        cluster_tolerance: % tolerance for clustering pivots
        major_threshold: minimum bounces for a 'major' zone
        max_zone_width_pct: maximum zone width as % of price

    Returns:
        List of SRZone objects sorted by price
    """
    if len(df) < pivot_window * 2 + 1:
        return []

    pivot_highs, pivot_lows = find_daily_pivots(df, left=pivot_window, right=pivot_window)

    resistance_raw = _build_resistance_zones(pivot_highs, cluster_tolerance)
    support_raw = _build_support_zones(pivot_lows, cluster_tolerance)

    current_price = float(df['Close'].iloc[-1])
    zones = []

    for z in resistance_raw:
        # Skip zones that are too wide (not "painfully obvious")
        width_pct = (z['upper'] - z['lower']) / z['midpoint'] * 100
        if width_pct > max_zone_width_pct:
            continue

        touches = _count_zone_touches(df, z['upper'], z['lower'])
        touches = max(touches, z['pivot_count'])

        zones.append(SRZone(
            upper=z['upper'],
            lower=z['lower'],
            midpoint=z['midpoint'],
            touches=touches,
            zone_type='resistance' if z['midpoint'] > current_price else 'support',
            strength='major' if touches >= major_threshold else 'minor',
            first_seen=z['first_seen'],
            last_seen=z['last_seen'],
        ))

    for z in support_raw:
        width_pct = (z['upper'] - z['lower']) / z['midpoint'] * 100
        if width_pct > max_zone_width_pct:
            continue

        touches = _count_zone_touches(df, z['upper'], z['lower'])
        touches = max(touches, z['pivot_count'])

        zones.append(SRZone(
            upper=z['upper'],
            lower=z['lower'],
            midpoint=z['midpoint'],
            touches=touches,
            zone_type='support' if z['midpoint'] < current_price else 'resistance',
            strength='major' if touches >= major_threshold else 'minor',
            first_seen=z['first_seen'],
            last_seen=z['last_seen'],
        ))

    # Merge only truly overlapping zones (not just adjacent)
    zones = _merge_overlapping_zones(zones, major_threshold)

    # Post-merge: filter out zones that became too wide from merging
    zones = [z for z in zones
             if (z.upper - z.lower) / z.midpoint * 100 <= max_zone_width_pct]

    zones.sort(key=lambda z: z.midpoint)

    return zones


def get_nearest_zones(
    zones: List[SRZone], current_price: float, major_only: bool = True
) -> tuple[Optional[SRZone], Optional[SRZone]]:
    """Get the nearest support zone below and resistance zone above price.

    Per the video strategy: only trade the closest major zones.

    Returns:
        (nearest_support, nearest_resistance) - either can be None
    """
    filtered = zones if not major_only else [z for z in zones if z.strength == 'major']

    # Support zones: midpoint below current price
    support_zones = [z for z in filtered if z.midpoint < current_price]
    # Resistance zones: midpoint above current price
    resistance_zones = [z for z in filtered if z.midpoint > current_price]

    nearest_support = max(support_zones, key=lambda z: z.midpoint) if support_zones else None
    nearest_resistance = min(resistance_zones, key=lambda z: z.midpoint) if resistance_zones else None

    if nearest_support:
        nearest_support.zone_type = 'support'
    if nearest_resistance:
        nearest_resistance.zone_type = 'resistance'

    return nearest_support, nearest_resistance


def _merge_overlapping_zones(
    zones: List[SRZone], major_threshold: int
) -> List[SRZone]:
    """Merge zones whose price ranges overlap."""
    if not zones:
        return []

    sorted_zones = sorted(zones, key=lambda z: z.lower)
    merged = [sorted_zones[0]]

    for zone in sorted_zones[1:]:
        prev = merged[-1]
        # Only merge if zones truly overlap (not just touching)
        if zone.lower < prev.upper:
            combined_touches = prev.touches + zone.touches
            new_upper = max(prev.upper, zone.upper)
            new_lower = min(prev.lower, zone.lower)
            merged[-1] = SRZone(
                upper=new_upper,
                lower=new_lower,
                midpoint=(new_upper + new_lower) / 2,
                touches=combined_touches,
                zone_type=prev.zone_type,
                strength='major' if combined_touches >= major_threshold else 'minor',
                first_seen=min(prev.first_seen, zone.first_seen)
                    if prev.first_seen and zone.first_seen else prev.first_seen,
                last_seen=max(prev.last_seen, zone.last_seen)
                    if prev.last_seen and zone.last_seen else prev.last_seen,
            )
        else:
            merged.append(zone)

    return merged


def is_price_in_zone(price: float, zone: SRZone) -> bool:
    """Check if price is inside a zone's boundaries."""
    return zone.lower <= price <= zone.upper


def is_price_halfway_in_zone(price: float, zone: SRZone, penetration_pct: float = 0.5) -> bool:
    """Check if price has penetrated sufficiently into a zone.

    Args:
        price: current price
        zone: S/R zone
        penetration_pct: fraction of zone width price must penetrate (0.5 = halfway)

    For a support zone: price should be at or below the penetration threshold
    For a resistance zone: price should be at or above the penetration threshold
    """
    zone_width = zone.upper - zone.lower
    if zone.zone_type == 'support':
        threshold = zone.upper - zone_width * penetration_pct
        return price <= threshold and price >= zone.lower
    else:  # resistance
        threshold = zone.lower + zone_width * penetration_pct
        return price >= threshold and price <= zone.upper
