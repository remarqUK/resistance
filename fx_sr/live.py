"""Live monitoring for trading opportunities.

Uses daily chart for zone detection and 1-hour chart for entry confirmation.
Scans pairs for price approaching or inside S/R zones.
"""

import time
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from .config import PAIRS, DEFAULT_ZONE_HISTORY_DAYS
from .data import fetch_daily_data, fetch_hourly_data
from .levels import detect_zones, get_nearest_zones, SRZone, is_price_in_zone
from .strategy import StrategyParams, generate_signal, Signal


def scan_opportunities(
    pairs: Dict = None,
    params: StrategyParams = None,
    zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS,
) -> List[Signal]:
    """Scan all pairs for current trading opportunities.

    For each pair:
    1. Fetch daily data -> detect zones
    2. Fetch recent hourly data -> check for entry signals
    3. Report if price is inside a zone with a reversal candle

    Returns:
        List of active signals
    """
    if pairs is None:
        pairs = PAIRS
    if params is None:
        params = StrategyParams()

    signals = []

    for pair_id, pair_info in pairs.items():
        decimals = pair_info.get('decimals', 5)
        print(f"  Scanning {pair_info['name']}...", end=" ", flush=True)

        # Fetch daily data for zone detection
        daily_df = fetch_daily_data(pair_info['ticker'], days=zone_history_days)
        if daily_df.empty:
            print("no daily data")
            continue

        zones = detect_zones(daily_df)
        current_price = float(daily_df['Close'].iloc[-1])

        nearest_support, nearest_resistance = get_nearest_zones(
            zones, current_price, major_only=True
        )

        # Fetch recent hourly data for entry confirmation
        hourly_df = fetch_hourly_data(pair_info['ticker'], days=3)
        if hourly_df.empty:
            print("no hourly data")
            continue

        last_bar = hourly_df.iloc[-1]
        current_price = float(last_bar['Close'])
        current_time = hourly_df.index[-1]

        signal = None

        # Check support zone
        if nearest_support:
            signal = generate_signal(
                bar_open=last_bar['Open'],
                bar_close=last_bar['Close'],
                bar_high=last_bar['High'],
                bar_low=last_bar['Low'],
                zone=nearest_support,
                pair=pair_id,
                time=current_time,
                params=params,
            )

        # Check resistance zone
        if signal is None and nearest_resistance:
            signal = generate_signal(
                bar_open=last_bar['Open'],
                bar_close=last_bar['Close'],
                bar_high=last_bar['High'],
                bar_low=last_bar['Low'],
                zone=nearest_resistance,
                pair=pair_id,
                time=current_time,
                params=params,
            )

        if signal:
            signals.append(signal)
            print(
                f">>> {signal.direction} @ {signal.entry_price:.{decimals}f} "
                f"(inside {signal.zone_type} zone "
                f"[{signal.zone_lower:.{decimals}f} - {signal.zone_upper:.{decimals}f}])"
            )
        else:
            _print_proximity(current_price, nearest_support, nearest_resistance, decimals)

    return signals


def _print_proximity(
    price: float,
    support: Optional[SRZone],
    resistance: Optional[SRZone],
    decimals: int,
):
    """Print how far price is from nearest zones."""
    parts = [f"price={price:.{decimals}f}"]

    if support:
        if is_price_in_zone(price, support):
            parts.append(f"IN support zone [{support.lower:.{decimals}f}-{support.upper:.{decimals}f}]")
        else:
            dist = abs(price - support.upper) / price * 100
            parts.append(f"support [{support.lower:.{decimals}f}-{support.upper:.{decimals}f}] ({dist:.3f}% away)")

    if resistance:
        if is_price_in_zone(price, resistance):
            parts.append(f"IN resistance zone [{resistance.lower:.{decimals}f}-{resistance.upper:.{decimals}f}]")
        else:
            dist = abs(resistance.lower - price) / price * 100
            parts.append(f"resistance [{resistance.lower:.{decimals}f}-{resistance.upper:.{decimals}f}] ({dist:.3f}% away)")

    if not support and not resistance:
        parts.append("no major zones found")

    print(", ".join(parts))


def format_signals(signals: List[Signal]) -> str:
    """Format live signals for display."""
    if not signals:
        return "\n  No opportunities detected at this time.\n"

    lines = [
        "",
        "=" * 110,
        "  TRADING OPPORTUNITIES (Daily Zone Strategy)",
        "=" * 110,
        f"  {'PAIR':<10} {'SIGNAL':>6} {'ENTRY':>12} {'ZONE':>27} "
        f"{'SL':>12} {'TP':>12} {'STR':>5}",
        "-" * 110,
    ]

    for s in signals:
        pair_info = PAIRS.get(s.pair, {})
        d = pair_info.get('decimals', 5)
        zone_str = f"[{s.zone_lower:.{d}f} - {s.zone_upper:.{d}f}]"
        lines.append(
            f"  {s.pair:<10} {s.direction:>6} {s.entry_price:>{12}.{d}f} "
            f"{zone_str:>27} {s.sl_price:>{12}.{d}f} "
            f"{s.tp_price:>{12}.{d}f} {s.zone_strength:>5}"
        )

    lines.append("=" * 110)
    return "\n".join(lines)


def show_zones(pair_id: str, pair_info: dict, zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS) -> str:
    """Show current S/R zones for a single pair."""
    daily_df = fetch_daily_data(pair_info['ticker'], days=zone_history_days)
    if daily_df.empty:
        return f"  No data available for {pair_info['name']}"

    zones = detect_zones(daily_df)
    current_price = float(daily_df['Close'].iloc[-1])
    decimals = pair_info.get('decimals', 5)

    nearest_sup, nearest_res = get_nearest_zones(zones, current_price, major_only=False)

    lines = [
        f"\n  {pair_info['name']} - Current Price: {current_price:.{decimals}f}",
        f"  {'Zone Range':>30}  {'Type':<12} {'Strength':<8} {'Touches':>8}",
        "  " + "-" * 70,
    ]

    for zone in zones:
        in_zone = zone.lower <= current_price <= zone.upper
        nearest = (zone is nearest_sup or zone is nearest_res)
        marker = " <<< PRICE IN ZONE" if in_zone else (" <<<" if nearest else "")
        zone_range = f"[{zone.lower:.{decimals}f} - {zone.upper:.{decimals}f}]"
        lines.append(
            f"  {zone_range:>30}  {zone.zone_type:<12} "
            f"{zone.strength:<8} {zone.touches:>8}{marker}"
        )

    if not zones:
        lines.append("  No zones detected")

    return "\n".join(lines)


def live_monitor(
    pairs: Dict = None,
    params: StrategyParams = None,
    interval: int = 60,
    zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS,
    track_positions: bool = True,
):
    """Continuously monitor for opportunities and open positions.

    Args:
        pairs: dict of pair configs (default: all top 10)
        params: strategy parameters
        interval: seconds between scans
        zone_history_days: days of daily data for zone detection
        track_positions: if True, monitor IBKR positions for exit signals
    """
    if pairs is None:
        pairs = PAIRS
    if params is None:
        params = StrategyParams()

    mode = "scanner + position monitor" if track_positions else "scanner only"
    print(f"\n  Live monitor started ({mode}). Scanning every {interval}s. Ctrl+C to stop.\n")

    try:
        while True:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n  [{now}] Scanning {len(pairs)} pairs...")

            signals = scan_opportunities(pairs, params, zone_history_days)
            print(format_signals(signals))

            # Position monitoring
            if track_positions:
                from .positions import (
                    sync_positions, check_position_exits,
                    format_positions_table, format_alerts,
                )
                tracked = sync_positions(params, zone_history_days)
                if tracked:
                    alerts, snapshots = check_position_exits(tracked, params)
                    print(format_positions_table(tracked, snapshots, alerts))
                    if alerts:
                        print(format_alerts(alerts))

            print(f"  Next scan in {interval}s...")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n  Monitor stopped.")
