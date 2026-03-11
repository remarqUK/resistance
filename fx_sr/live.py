"""Live monitoring for trading opportunities."""

from __future__ import annotations

from contextlib import nullcontext, redirect_stdout
from dataclasses import dataclass, field, replace
from datetime import datetime
import io
import sys
import time
from typing import Callable, Dict, List, Optional, Set

from .config import PAIRS, DEFAULT_ZONE_HISTORY_DAYS
from .data import fetch_daily_data, fetch_hourly_data
from .levels import detect_zones, get_nearest_zones, SRZone, is_price_in_zone
from .strategy import StrategyParams, Signal, generate_signal
from .sizing import PositionSizePlan, build_position_size_plan, format_units
from . import ibkr


@dataclass(frozen=True)
class ExecutionResult:
    """Outcome for a single live order submission attempt."""

    pair: str
    direction: str
    units: int
    status: str
    order_id: Optional[int] = None
    note: str = ''


@dataclass(frozen=True)
class PairScanRow:
    """Structured watchlist row for one pair."""

    pair: str
    name: str
    decimals: int
    price: Optional[float]
    state: str
    note: str
    support_text: str
    resistance_text: str
    signal: Optional[Signal] = None
    support_lower: Optional[float] = None
    support_upper: Optional[float] = None
    support_strength: Optional[str] = None
    resistance_lower: Optional[float] = None
    resistance_upper: Optional[float] = None
    resistance_strength: Optional[str] = None


@dataclass
class MonitorSnapshot:
    """Full state captured for a single monitor cycle."""

    scan_started_at: datetime
    scan_completed_at: datetime
    scan_duration: float
    pair_rows: List[PairScanRow]
    signals: List[Signal]
    size_plans: List[Optional[PositionSizePlan]]
    execution_results: List[ExecutionResult]
    tracked: Dict[str, dict]
    position_snapshots: Dict[str, dict]
    alerts: List[dict]
    active_balance: Optional[float]
    active_currency: Optional[str]
    pending_pairs: Set[str]
    risk_pct: float
    track_positions: bool
    execute_orders: bool
    messages: List[str] = field(default_factory=list)


def _rich_supported() -> bool:
    """Return True when terminal rendering can use Rich."""

    if not sys.stdout.isatty():
        return False
    try:
        import rich  # noqa: F401
    except ImportError:
        return False
    return True


def _format_number_compact(value: float) -> str:
    """Format a numeric amount compactly for dashboards."""

    abs_value = abs(float(value))
    if abs_value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"{value / 1_000:.1f}K"
    return f"{value:.0f}"


def _format_zone_band(zone: Optional[SRZone], decimals: int) -> str:
    """Format a zone range for display."""

    if zone is None:
        return "-"
    return f"{zone.lower:.{decimals}f}-{zone.upper:.{decimals}f}"


def _distance_to_zone_pct(
    price: float,
    zone: Optional[SRZone],
    is_support: bool,
) -> Optional[float]:
    """Return percentage distance from price to the relevant zone edge."""

    if zone is None or price <= 0:
        return None
    edge = zone.upper if is_support else zone.lower
    return abs(price - edge) / price * 100.0


def _format_zone_display(
    price: Optional[float],
    zone: Optional[SRZone],
    decimals: int,
    is_support: bool,
) -> str:
    """Format nearest-zone display text for the watchlist."""

    if zone is None:
        return "-"

    band = _format_zone_band(zone, decimals)
    if price is None or price <= 0:
        return band
    if is_price_in_zone(price, zone):
        return f"{band}  IN"

    dist = _distance_to_zone_pct(price, zone, is_support=is_support)
    if dist is None:
        return band
    return f"{band}  {dist:.2f}%"


def _describe_watch_state(
    price: float,
    support: Optional[SRZone],
    resistance: Optional[SRZone],
) -> tuple[str, str]:
    """Describe the current pair state when no executable signal exists."""

    if support and is_price_in_zone(price, support):
        return "INSIDE", f"Inside support zone ({support.strength})"
    if resistance and is_price_in_zone(price, resistance):
        return "INSIDE", f"Inside resistance zone ({resistance.strength})"

    support_dist = _distance_to_zone_pct(price, support, is_support=True)
    resistance_dist = _distance_to_zone_pct(price, resistance, is_support=False)

    if support_dist is not None and (
        resistance_dist is None or support_dist <= resistance_dist
    ):
        return "WATCH", f"{support_dist:.2f}% above support"
    if resistance_dist is not None:
        return "WATCH", f"{resistance_dist:.2f}% below resistance"
    return "WATCH", "No major zones found"


def _row_zone(lower: Optional[float], upper: Optional[float], zone_type: str, strength: Optional[str]) -> Optional[SRZone]:
    """Rebuild an ephemeral zone object from a serialized watch row."""

    if lower is None or upper is None:
        return None
    fl, fu = float(lower), float(upper)
    return SRZone(
        lower=fl,
        upper=fu,
        midpoint=(fl + fu) / 2,
        zone_type=zone_type,
        touches=0,
        strength=strength or 'major',
    )


def _pair_row_priority(row: PairScanRow) -> tuple[int, str]:
    """Sort watchlist rows so actionable items stay near the top."""

    if row.signal:
        return 0, row.pair

    priority = {
        'OPEN': 1,
        'PENDING': 2,
        'INSIDE': 3,
        'WATCH': 4,
        'NO DATA': 5,
    }.get(row.state, 6)
    return priority, row.pair


def refresh_pair_row_price(row: PairScanRow, price: float) -> PairScanRow:
    """Refresh a watchlist row from a subscribed live price update."""

    support = _row_zone(row.support_lower, row.support_upper, 'support', row.support_strength)
    resistance = _row_zone(
        row.resistance_lower,
        row.resistance_upper,
        'resistance',
        row.resistance_strength,
    )
    support_text = _format_zone_display(price, support, row.decimals, True)
    resistance_text = _format_zone_display(price, resistance, row.decimals, False)

    if row.signal or row.state in {'OPEN', 'PENDING', 'NO DATA'}:
        return replace(
            row,
            price=price,
            support_text=support_text,
            resistance_text=resistance_text,
        )

    state, note = _describe_watch_state(price, support, resistance)
    return replace(
        row,
        price=price,
        state=state,
        note=note,
        support_text=support_text,
        resistance_text=resistance_text,
    )


def _scan_pair(
    pair_id: str,
    pair_info: dict,
    params: StrategyParams,
    zone_history_days: int,
    tracked_pairs: Dict[str, Set[str]],
    blocked_pairs: Set[str],
    price_cache: Optional[Dict[str, float]] = None,
) -> tuple[PairScanRow, Optional[Signal]]:
    """Scan one pair and return a watchlist row plus optional signal."""

    decimals = pair_info.get('decimals', 5)
    name = pair_info.get('name', pair_id)

    daily_df = fetch_daily_data(pair_info['ticker'], days=zone_history_days)
    if daily_df.empty:
        return (
            PairScanRow(pair_id, name, decimals, None, "NO DATA", "No daily data", "-", "-"),
            None,
        )

    zones = detect_zones(daily_df)
    current_price = float(daily_df['Close'].iloc[-1])
    nearest_support, nearest_resistance = get_nearest_zones(zones, current_price, major_only=True)

    hourly_df = fetch_hourly_data(pair_info['ticker'], days=3)
    if hourly_df.empty:
        return (
            PairScanRow(
                pair_id,
                name,
                decimals,
                current_price,
                "NO DATA",
                "No hourly data",
                _format_zone_display(current_price, nearest_support, decimals, True),
                _format_zone_display(current_price, nearest_resistance, decimals, False),
                support_lower=nearest_support.lower if nearest_support else None,
                support_upper=nearest_support.upper if nearest_support else None,
                support_strength=nearest_support.strength if nearest_support else None,
                resistance_lower=nearest_resistance.lower if nearest_resistance else None,
                resistance_upper=nearest_resistance.upper if nearest_resistance else None,
                resistance_strength=nearest_resistance.strength if nearest_resistance else None,
            ),
            None,
        )

    last_bar = hourly_df.iloc[-1]
    current_price = float(last_bar['Close'])
    current_time = hourly_df.index[-1]
    nearest_support, nearest_resistance = get_nearest_zones(zones, current_price, major_only=True)
    if price_cache is not None:
        price_cache[pair_id] = current_price

    support_text = _format_zone_display(current_price, nearest_support, decimals, True)
    resistance_text = _format_zone_display(current_price, nearest_resistance, decimals, False)

    if pair_id in tracked_pairs:
        directions = "/".join(sorted(tracked_pairs[pair_id]))
        return (
            PairScanRow(
                pair_id,
                name,
                decimals,
                current_price,
                "OPEN",
                f"Tracked position ({directions})",
                support_text,
                resistance_text,
                support_lower=nearest_support.lower if nearest_support else None,
                support_upper=nearest_support.upper if nearest_support else None,
                support_strength=nearest_support.strength if nearest_support else None,
                resistance_lower=nearest_resistance.lower if nearest_resistance else None,
                resistance_upper=nearest_resistance.upper if nearest_resistance else None,
                resistance_strength=nearest_resistance.strength if nearest_resistance else None,
            ),
            None,
        )

    if pair_id in blocked_pairs:
        return (
            PairScanRow(
                pair_id,
                name,
                decimals,
                current_price,
                "PENDING",
                "Active order pending",
                support_text,
                resistance_text,
                support_lower=nearest_support.lower if nearest_support else None,
                support_upper=nearest_support.upper if nearest_support else None,
                support_strength=nearest_support.strength if nearest_support else None,
                resistance_lower=nearest_resistance.lower if nearest_resistance else None,
                resistance_upper=nearest_resistance.upper if nearest_resistance else None,
                resistance_strength=nearest_resistance.strength if nearest_resistance else None,
            ),
            None,
        )

    signal = None
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
        note = f"{signal.zone_type.title()} reversal ({signal.zone_strength})"
        return (
            PairScanRow(
                pair_id,
                name,
                decimals,
                current_price,
                signal.direction,
                note,
                support_text,
                resistance_text,
                signal,
                nearest_support.lower if nearest_support else None,
                nearest_support.upper if nearest_support else None,
                nearest_support.strength if nearest_support else None,
                nearest_resistance.lower if nearest_resistance else None,
                nearest_resistance.upper if nearest_resistance else None,
                nearest_resistance.strength if nearest_resistance else None,
            ),
            signal,
        )

    state, note = _describe_watch_state(current_price, nearest_support, nearest_resistance)
    return (
        PairScanRow(
            pair_id,
            name,
            decimals,
            current_price,
            state,
            note,
            support_text,
            resistance_text,
            support_lower=nearest_support.lower if nearest_support else None,
            support_upper=nearest_support.upper if nearest_support else None,
            support_strength=nearest_support.strength if nearest_support else None,
            resistance_lower=nearest_resistance.lower if nearest_resistance else None,
            resistance_upper=nearest_resistance.upper if nearest_resistance else None,
            resistance_strength=nearest_resistance.strength if nearest_resistance else None,
        ),
        None,
    )


def collect_scan_rows(
    pairs: Dict | None = None,
    params: StrategyParams | None = None,
    zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS,
    tracked_positions: Dict[str, dict] | None = None,
    blocked_pairs: Optional[Set[str]] = None,
    price_cache: Optional[Dict[str, float]] = None,
) -> tuple[List[Signal], List[PairScanRow]]:
    """Collect structured pair rows and the executable signals among them."""

    if pairs is None:
        pairs = PAIRS
    if params is None:
        params = StrategyParams()

    tracked_pairs: Dict[str, Set[str]] = {}
    blocked_pairs = blocked_pairs or set()
    if tracked_positions:
        for info in tracked_positions.values():
            pair = info.get('pair')
            trade = info.get('trade')
            if pair and trade:
                tracked_pairs.setdefault(pair, set()).add(trade.direction)

    signals: List[Signal] = []
    pair_rows: List[PairScanRow] = []
    for pair_id, pair_info in pairs.items():
        row, signal = _scan_pair(
            pair_id,
            pair_info,
            params,
            zone_history_days,
            tracked_pairs,
            blocked_pairs,
            price_cache=price_cache,
        )
        pair_rows.append(row)
        if signal:
            signals.append(signal)
    return signals, pair_rows


def format_scan_rows(pair_rows: List[PairScanRow]) -> str:
    """Format a plain-text market watch table."""

    if not pair_rows:
        return "\n  No pairs configured.\n"

    lines = [
        "",
        "=" * 148,
        "  MARKET WATCH",
        "=" * 148,
        f"  {'PAIR':<10} {'PRICE':>12} {'STATE':>10} {'SUPPORT':>28} "
        f"{'RESISTANCE':>28} {'NOTE':<46}",
        "-" * 148,
    ]
    for row in sorted(pair_rows, key=_pair_row_priority):
        price_display = "-" if row.price is None else f"{row.price:.{row.decimals}f}"
        lines.append(
            f"  {row.pair:<10} {price_display:>12} {row.state:>10} "
            f"{row.support_text:>28} {row.resistance_text:>28} {row.note:<46}"
        )
    lines.append("=" * 148)
    return "\n".join(lines)


def scan_opportunities(
    pairs: Dict = None,
    params: StrategyParams = None,
    zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS,
    tracked_positions: Dict[str, dict] | None = None,
    blocked_pairs: Optional[Set[str]] = None,
    price_cache: Optional[Dict[str, float]] = None,
) -> List[Signal]:
    """Scan all pairs and print a plain-text watch table."""

    signals, pair_rows = collect_scan_rows(
        pairs=pairs,
        params=params,
        zone_history_days=zone_history_days,
        tracked_positions=tracked_positions,
        blocked_pairs=blocked_pairs,
        price_cache=price_cache,
    )
    print(format_scan_rows(pair_rows))
    return signals


def format_signals(signals: List[Signal]) -> str:
    """Format live signals for display."""

    return format_signals_with_sizes(signals, size_plans=None)


def _build_price_lookup(price_cache: Optional[Dict[str, float]] = None) -> Callable[[str], Optional[float]]:
    """Return a cached pair-price lookup for conversion and sizing."""

    cache: Dict[str, Optional[float]] = {}
    if price_cache:
        cache.update({pair: float(price) for pair, price in price_cache.items()})

    def lookup(pair_id: str) -> Optional[float]:
        if pair_id in cache:
            return cache[pair_id]

        pair_info = PAIRS.get(pair_id)
        if not pair_info:
            cache[pair_id] = None
            return None

        price = None
        hourly_df = fetch_hourly_data(pair_info['ticker'], days=3, allow_stale_cache=True)
        if not hourly_df.empty:
            price = float(hourly_df['Close'].iloc[-1])

        cache[pair_id] = price
        return price

    return lookup


def build_live_size_plans(
    signals: List[Signal],
    balance: Optional[float],
    risk_pct: float,
    account_currency: Optional[str],
    price_cache: Optional[Dict[str, float]] = None,
) -> List[Optional[PositionSizePlan]]:
    """Build per-signal live size plans from the shared compounding rule."""

    if not signals or balance is None or balance <= 0 or not account_currency:
        return [None for _ in signals]

    price_lookup = _build_price_lookup(price_cache=price_cache)
    return [
        build_position_size_plan(
            pair=signal.pair,
            direction=signal.direction,
            entry_price=signal.entry_price,
            stop_price=signal.sl_price,
            balance=balance,
            risk_pct=risk_pct,
            account_currency=account_currency,
            price_lookup=price_lookup,
        )
        for signal in signals
    ]


def format_signals_with_sizes(
    signals: List[Signal],
    size_plans: Optional[List[Optional[PositionSizePlan]]] = None,
) -> str:
    """Format live signals for display, optionally with suggested sizing."""

    if not signals:
        return "\n  No opportunities detected at this time.\n"

    if size_plans is None or len(size_plans) != len(signals):
        size_plans = [None for _ in signals]

    lines = [
        "",
        "=" * 146,
        "  TRADING OPPORTUNITIES (Daily Zone Strategy)",
        "=" * 146,
        f"  {'PAIR':<10} {'SIGNAL':>6} {'ENTRY':>12} {'ZONE':>27} "
        f"{'SL':>12} {'TP':>12} {'STR':>5} {'RISK':>12} {'SIZE':>10} {'NOTIONAL':>13}",
        "-" * 146,
    ]
    for signal, plan in zip(signals, size_plans):
        pair_info = PAIRS.get(signal.pair, {})
        decimals = pair_info.get('decimals', 5)
        zone_str = f"[{signal.zone_lower:.{decimals}f} - {signal.zone_upper:.{decimals}f}]"
        risk_display = "-"
        size_display = "-"
        notional_display = "-"
        if plan:
            risk_display = f"{plan.account_currency} {plan.risk_amount:,.2f}"
            size_display = format_units(plan.units)
            notional_display = f"{plan.account_currency} {plan.notional_account:,.0f}"
        lines.append(
            f"  {signal.pair:<10} {signal.direction:>6} {signal.entry_price:>{12}.{decimals}f} "
            f"{zone_str:>27} {signal.sl_price:>{12}.{decimals}f} "
            f"{signal.tp_price:>{12}.{decimals}f} {signal.zone_strength:>5} "
            f"{risk_display:>12} {size_display:>10} {notional_display:>13}"
        )
    lines.append("=" * 146)
    return "\n".join(lines)


def format_execution_results(results: List[ExecutionResult]) -> str:
    """Format live execution attempts for display."""

    if not results:
        return ""

    lines = [
        "",
        "=" * 92,
        "  PAPER EXECUTION",
        "=" * 92,
        f"  {'PAIR':<10} {'DIR':>5} {'SIZE':>10} {'STATUS':>14} {'ORDER ID':>10} {'NOTE':<30}",
        "-" * 92,
    ]
    for result in results:
        order_id = '-' if result.order_id is None else str(result.order_id)
        note = result.note[:30]
        lines.append(
            f"  {result.pair:<10} {result.direction:>5} {format_units(result.units):>10} "
            f"{result.status:>14} {order_id:>10} {note:<30}"
        )
    lines.append("=" * 92)
    return "\n".join(lines)


def execute_signal_plans(
    signals: List[Signal],
    size_plans: List[Optional[PositionSizePlan]],
    execute_orders: bool,
    existing_pairs: Optional[Set[str]] = None,
    pending_pairs: Optional[Set[str]] = None,
) -> List[ExecutionResult]:
    """Submit market orders for valid size plans when execution is enabled."""

    if not execute_orders or not signals:
        return []

    existing_pairs = existing_pairs or set()
    pending_pairs = pending_pairs or set()
    results: List[ExecutionResult] = []
    for signal, plan in zip(signals, size_plans):
        if plan is None:
            results.append(ExecutionResult(signal.pair, signal.direction, 0, 'SKIPPED', note='size unavailable'))
            continue
        if signal.pair in existing_pairs or signal.pair in pending_pairs:
            results.append(
                ExecutionResult(signal.pair, signal.direction, plan.units, 'SKIPPED', note='position/order exists')
            )
            continue

        order_ref = f"fxsr:{signal.pair}:{signal.direction}:{signal.time.strftime('%Y%m%d%H%M%S')}"
        order = ibkr.submit_fx_market_order(
            pair=signal.pair,
            direction=signal.direction,
            quantity=plan.units,
            order_ref=order_ref,
        )
        if order is None:
            results.append(
                ExecutionResult(signal.pair, signal.direction, plan.units, 'FAILED', note='broker rejected/failed')
            )
            continue

        pending_pairs.add(signal.pair)
        results.append(
            ExecutionResult(
                signal.pair,
                signal.direction,
                plan.units,
                order.get('status') or 'SUBMITTED',
                order_id=order.get('order_id'),
                note=f"risk {plan.account_currency} {plan.risk_amount:,.2f}",
            )
        )
    return results


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
        nearest = zone is nearest_sup or zone is nearest_res
        marker = " <<< PRICE IN ZONE" if in_zone else (" <<<" if nearest else "")
        zone_range = f"[{zone.lower:.{decimals}f} - {zone.upper:.{decimals}f}]"
        lines.append(
            f"  {zone_range:>30}  {zone.zone_type:<12} {zone.strength:<8} {zone.touches:>8}{marker}"
        )
    if not zones:
        lines.append("  No zones detected")
    return "\n".join(lines)


def run_monitor_cycle(
    pairs: Dict | None = None,
    params: StrategyParams | None = None,
    zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS,
    track_positions: bool = True,
    balance: Optional[float] = None,
    risk_pct: float = 0.05,
    account_currency: Optional[str] = None,
    execute_orders: bool = False,
    capture_output: bool = False,
) -> MonitorSnapshot:
    """Execute one full live-monitor cycle and return a structured snapshot."""

    if pairs is None:
        pairs = PAIRS
    if params is None:
        params = StrategyParams()

    scan_started_at = datetime.now()
    buffer = io.StringIO()
    stdout_context = redirect_stdout(buffer) if capture_output else nullcontext()

    with stdout_context:
        sync_positions = None
        check_position_exits = None
        if track_positions:
            from .positions import sync_positions, check_position_exits

        pending_pairs = ibkr.fetch_open_order_pairs()
        tracked = sync_positions(params, zone_history_days) if track_positions else {}

        market_prices: Dict[str, float] = {}
        signals, pair_rows = collect_scan_rows(
            pairs=pairs,
            params=params,
            zone_history_days=zone_history_days,
            tracked_positions=tracked,
            blocked_pairs=pending_pairs,
            price_cache=market_prices,
        )

        active_balance = balance
        active_currency = account_currency.upper() if account_currency else None
        if active_balance is None:
            active_balance, fetched_currency = ibkr.fetch_account_net_liquidation()
            if active_currency is None and fetched_currency not in (None, 'BASE'):
                active_currency = fetched_currency

        size_plans = build_live_size_plans(
            signals,
            active_balance,
            risk_pct,
            active_currency,
            price_cache=market_prices,
        )
        execution_results = execute_signal_plans(
            signals,
            size_plans,
            execute_orders=execute_orders,
            existing_pairs={info['pair'] for info in tracked.values()},
            pending_pairs=pending_pairs,
        )

        alerts: List[dict] = []
        position_snapshots: Dict[str, dict] = {}
        if track_positions and tracked:
            alerts, position_snapshots = check_position_exits(tracked, params)

    messages = [line.strip() for line in buffer.getvalue().splitlines() if line.strip()] if capture_output else []
    scan_completed_at = datetime.now()
    return MonitorSnapshot(
        scan_started_at=scan_started_at,
        scan_completed_at=scan_completed_at,
        scan_duration=(scan_completed_at - scan_started_at).total_seconds(),
        pair_rows=pair_rows,
        signals=signals,
        size_plans=size_plans,
        execution_results=execution_results,
        tracked=tracked,
        position_snapshots=position_snapshots,
        alerts=alerts,
        active_balance=active_balance,
        active_currency=active_currency,
        pending_pairs=set(pending_pairs),
        risk_pct=risk_pct,
        track_positions=track_positions,
        execute_orders=execute_orders,
        messages=messages,
    )


def format_sizing_summary(snapshot: Optional[MonitorSnapshot]) -> str:
    """Format sizing summary for headers and plain output."""

    if snapshot is None:
        return "resolving"
    if snapshot.active_balance is not None and snapshot.active_currency:
        return f"{snapshot.active_currency} {snapshot.active_balance:,.2f} @ {snapshot.risk_pct * 100:.2f}% risk"
    if snapshot.active_balance is not None:
        return f"{snapshot.active_balance:,.2f} @ {snapshot.risk_pct * 100:.2f}% risk (currency unknown)"
    return f"unavailable @ {snapshot.risk_pct * 100:.2f}% risk"


def _display_snapshot_plain(
    snapshot: MonitorSnapshot,
    strategy_label: Optional[str],
    client_id: Optional[int],
) -> None:
    """Print a plain-text snapshot for one-shot mode."""

    mode = "scanner + position monitor" if snapshot.track_positions else "scanner only"
    print(f"\n  FX S/R live snapshot ({mode})")
    print(f"  Scan time: {snapshot.scan_completed_at:%Y-%m-%d %H:%M:%S} ({snapshot.scan_duration:.1f}s)")
    if client_id is not None:
        print(f"  IBKR client ID: {client_id}")
    if strategy_label:
        print(f"  Strategy: {strategy_label}")
    print(f"  Live sizing: {format_sizing_summary(snapshot)}")
    print(format_scan_rows(snapshot.pair_rows))
    print(format_signals_with_sizes(snapshot.signals, snapshot.size_plans))

    if snapshot.execution_results:
        print(format_execution_results(snapshot.execution_results))

    if snapshot.track_positions and snapshot.tracked:
        from .positions import format_alerts, format_positions_table

        print(format_positions_table(snapshot.tracked, snapshot.position_snapshots, snapshot.alerts))
        if snapshot.alerts:
            print(format_alerts(snapshot.alerts))

    if snapshot.messages:
        print("\n  Messages:")
        for message in snapshot.messages:
            print(f"    {message}")


def display_snapshot(
    snapshot: MonitorSnapshot,
    strategy_label: Optional[str] = None,
    client_id: Optional[int] = None,
) -> None:
    """Display a one-shot monitor snapshot using Rich when possible."""

    if _rich_supported():
        from . import live_dashboard

        live_dashboard.display_snapshot_rich(snapshot, strategy_label, client_id)
        return
    _display_snapshot_plain(snapshot, strategy_label, client_id)


def _live_monitor_plain(
    pairs: Dict,
    params: StrategyParams,
    interval: int,
    zone_history_days: int,
    track_positions: bool,
    balance: Optional[float],
    risk_pct: float,
    account_currency: Optional[str],
    execute_orders: bool,
    strategy_label: Optional[str],
    client_id: Optional[int],
) -> None:
    """Fallback monitor loop for non-interactive terminals."""

    mode = "scanner + position monitor" if track_positions else "scanner only"
    print(f"\n  Live monitor started ({mode}). Scanning every {interval}s. Ctrl+C to stop.")
    if client_id is not None:
        print(f"  IBKR client ID: {client_id}")
    if strategy_label:
        print(f"  Strategy: {strategy_label}")

    try:
        while True:
            snapshot = run_monitor_cycle(
                pairs=pairs,
                params=params,
                zone_history_days=zone_history_days,
                track_positions=track_positions,
                balance=balance,
                risk_pct=risk_pct,
                account_currency=account_currency,
                execute_orders=execute_orders,
                capture_output=False,
            )
            _display_snapshot_plain(snapshot, strategy_label, client_id)
            print(f"\n  Next scan in {interval}s...")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n  Monitor stopped.")


def live_monitor(
    pairs: Dict = None,
    params: StrategyParams = None,
    interval: int = 60,
    zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS,
    track_positions: bool = True,
    balance: Optional[float] = None,
    risk_pct: float = 0.05,
    account_currency: Optional[str] = None,
    execute_orders: bool = False,
    strategy_label: Optional[str] = None,
    client_id: Optional[int] = None,
) -> None:
    """Continuously monitor for opportunities and open positions."""

    if pairs is None:
        pairs = PAIRS
    if params is None:
        params = StrategyParams()

    if _rich_supported():
        from . import live_dashboard

        live_dashboard.run_live_dashboard(
            pairs=pairs,
            params=params,
            interval=interval,
            zone_history_days=zone_history_days,
            track_positions=track_positions,
            balance=balance,
            risk_pct=risk_pct,
            account_currency=account_currency,
            execute_orders=execute_orders,
            strategy_label=strategy_label,
            client_id=client_id,
        )
        return

    _live_monitor_plain(
        pairs=pairs,
        params=params,
        interval=interval,
        zone_history_days=zone_history_days,
        track_positions=track_positions,
        balance=balance,
        risk_pct=risk_pct,
        account_currency=account_currency,
        execute_orders=execute_orders,
        strategy_label=strategy_label,
        client_id=client_id,
    )
