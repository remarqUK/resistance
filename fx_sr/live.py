"""Live monitoring for trading opportunities."""

from __future__ import annotations

from contextlib import nullcontext, redirect_stdout
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
import io
import os
import pandas as pd
import sys
import time
from typing import Callable, Dict, List, Optional, Set

from .config import PAIRS, DEFAULT_ZONE_HISTORY_DAYS
from .data import fetch_daily_data, fetch_hourly_data
from .execution import build_execution_plan
from .levels import detect_zones, get_nearest_zones, SRZone, is_price_in_zone
from .live_history import (
    load_detected_signal_stats,
    load_detected_signals,
    reconcile_detected_signal_orders,
    record_detected_signals,
    record_execution_results,
)
from .portfolio import (
    CorrelationExposure,
    PortfolioState,
    apply_correlation_policy,
    build_portfolio_state,
    closed_trade_summary_key,
    closed_trade_summary_from_row,
    get_entry_block,
)
from .strategy import (
    StrategyParams,
    Signal,
    get_tradeable_zones,
    is_pair_fully_blocked,
    select_entry_signal,
)
from .sizing import (
    PositionSizePlan,
    build_position_size_plan,
    build_position_size_plan_for_risk_amount,
    calculate_risk_amount,
    estimate_position_risk_amount,
    format_units,
)
from . import ibkr


@dataclass(frozen=True)
class ExecutionResult:
    """Outcome for a single live order submission attempt."""

    pair: str
    direction: str
    units: int
    status: str
    order_id: Optional[int] = None
    take_profit_order_id: Optional[int] = None
    stop_loss_order_id: Optional[int] = None
    avg_fill_price: Optional[float] = None
    filled_units: Optional[int] = None
    remaining_units: Optional[int] = None
    broker_status: Optional[str] = None
    submitted_entry_price: Optional[float] = None
    submitted_tp_price: Optional[float] = None
    submitted_sl_price: Optional[float] = None
    submit_bid: Optional[float] = None
    submit_ask: Optional[float] = None
    submit_spread: Optional[float] = None
    quote_source: Optional[str] = None
    quote_time: Optional[pd.Timestamp] = None
    note: str = ''


@dataclass(frozen=True)
class PreparedExecutionPlan:
    """Submit-time execution plan repriced from a fresh quote."""

    signal: Signal
    size_plan: PositionSizePlan
    entry_price: float
    stop_price: float
    take_profit_price: float
    quote: ibkr.ExecutionQuote


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
    support_dist_pct: Optional[float] = None
    resistance_dist_pct: Optional[float] = None


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


NEAR_ZONE_THRESHOLD_PCT = 0.30
_LIVE_DAILY_DATA_CACHE: Dict[tuple[str, int], tuple[str, object]] = {}
_LIVE_ZONE_CACHE: Dict[tuple[str, int], tuple[str, List[SRZone]]] = {}
_LIVE_HOURLY_DATA_CACHE: Dict[tuple[str, int], tuple[str, object]] = {}


@dataclass
class _PortfolioStateCacheEntry:
    params_key: str
    state: PortfolioState
    trade_ids: set[str]
    closed_count: int
    max_last_updated: str | None


_PORTFOLIO_STATE_CACHE: Dict[str, _PortfolioStateCacheEntry] = {}


def _current_day_bucket() -> str:
    """Return the current UTC day bucket used for live daily cache refreshes."""

    return datetime.now(timezone.utc).strftime('%Y-%m-%d')


def _current_hour_bucket() -> str:
    """Return the current UTC hour bucket used for live hourly cache refreshes."""

    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H')


def _get_live_daily_data(
    ticker_symbol: str,
    days: int,
    daily_data_cache: Optional[Dict[tuple[str, int], object]] = None,
):
    """Fetch live daily data with an in-memory cache that refreshes once per UTC day."""

    cache_key = (ticker_symbol, int(days))
    if daily_data_cache is not None and cache_key in daily_data_cache:
        return daily_data_cache[cache_key]

    bucket = _current_day_bucket()
    cached = _LIVE_DAILY_DATA_CACHE.get(cache_key)
    if cached and cached[0] == bucket:
        daily_df = cached[1]
    else:
        daily_df = fetch_daily_data(ticker_symbol, days=days)
        _LIVE_DAILY_DATA_CACHE[cache_key] = (bucket, daily_df)

    if daily_data_cache is not None:
        daily_data_cache[cache_key] = daily_df
    return daily_df


def _get_live_zones(
    ticker_symbol: str,
    days: int,
    daily_data_cache: Optional[Dict[tuple[str, int], object]] = None,
    zone_cache: Optional[Dict[tuple[str, int], List[SRZone]]] = None,
) -> tuple[object, List[SRZone]]:
    """Fetch live daily data and detected zones with once-per-day memoization."""

    daily_df = _get_live_daily_data(
        ticker_symbol,
        days,
        daily_data_cache=daily_data_cache,
    )
    if daily_df.empty:
        return daily_df, []

    cache_key = (ticker_symbol, int(days))
    if zone_cache is not None and cache_key in zone_cache:
        return daily_df, zone_cache[cache_key]

    bucket = f"{_current_day_bucket()}:{daily_df.index[-1]}"
    cached = _LIVE_ZONE_CACHE.get(cache_key)
    if cached and cached[0] == bucket:
        zones = cached[1]
    else:
        zones = detect_zones(daily_df)
        _LIVE_ZONE_CACHE[cache_key] = (bucket, zones)

    if zone_cache is not None:
        zone_cache[cache_key] = zones
    return daily_df, zones


def _get_live_hourly_data(
    ticker_symbol: str,
    days: int,
    hourly_data_cache: Optional[Dict[str, object]] = None,
):
    """Fetch live hourly data with an in-memory cache that refreshes once per UTC hour."""

    if hourly_data_cache is not None and ticker_symbol in hourly_data_cache:
        return hourly_data_cache[ticker_symbol]

    cache_key = (ticker_symbol, int(days))
    bucket = _current_hour_bucket()
    cached = _LIVE_HOURLY_DATA_CACHE.get(cache_key)
    if cached and cached[0] == bucket:
        hourly_df = cached[1]
    else:
        hourly_df = fetch_hourly_data(ticker_symbol, days=days)
        _LIVE_HOURLY_DATA_CACHE[cache_key] = (bucket, hourly_df)

    if hourly_data_cache is not None:
        hourly_data_cache[ticker_symbol] = hourly_df
    return hourly_df


def load_closed_trade_summaries() -> list:
    """Load closed live trades from history and normalize them for portfolio policy."""

    rows = load_detected_signals(status='CLOSED')
    trades = []
    for row in rows:
        summary = closed_trade_summary_from_row(row)
        if summary is not None:
            trades.append(summary)
    return trades


def _portfolio_params_key(params: StrategyParams) -> str:
    """Return a stable cache key for portfolio-policy semantics."""

    return repr(
        (
            params.cooldown_bars,
            params.loss_cooldown_bars,
            params.streak_pause_trigger,
            params.streak_pause_hours,
            params.dynamic_risk,
            params.dd_risk_start,
            params.dd_risk_full,
            params.dd_risk_floor,
            params.quality_sizing,
            params.quality_risk_min,
            params.quality_risk_max,
        )
    )


def _merge_closed_trade_rows(rows: List[dict], known_trade_ids: set[str]) -> list:
    """Convert DB rows to unseen closed-trade summaries."""

    trades = []
    for row in rows:
        summary = closed_trade_summary_from_row(row)
        if summary is None:
            continue
        trade_id = closed_trade_summary_key(summary)
        if trade_id in known_trade_ids:
            raise ValueError(f"closed trade {trade_id} already present")
        trades.append(summary)
        known_trade_ids.add(trade_id)
    return trades


def load_portfolio_state(
    params: StrategyParams | None = None,
    *,
    current_balance: float | None = None,
    force_refresh: bool = False,
) -> PortfolioState:
    """Load closed-trade history and build a shared portfolio state snapshot."""

    if params is None:
        params = StrategyParams()
    params_key = _portfolio_params_key(params)
    stats = load_detected_signal_stats(status='CLOSED')
    cache_key = 'default'
    cache_entry = _PORTFOLIO_STATE_CACHE.get(cache_key)

    if (
        force_refresh
        or cache_entry is None
        or cache_entry.params_key != params_key
        or stats['count'] < cache_entry.closed_count
        or (
            stats['count'] == cache_entry.closed_count
            and stats['max_last_updated'] != cache_entry.max_last_updated
        )
    ):
        trades = load_closed_trade_summaries()
        state = build_portfolio_state(
            trades,
            params=params,
            current_balance=current_balance,
        )
        cache_entry = _PortfolioStateCacheEntry(
            params_key=params_key,
            state=state,
            trade_ids={closed_trade_summary_key(trade) for trade in trades},
            closed_count=int(stats['count']),
            max_last_updated=stats['max_last_updated'],
        )
        _PORTFOLIO_STATE_CACHE[cache_key] = cache_entry
        return state

    if stats['count'] > cache_entry.closed_count:
        if cache_entry.max_last_updated is None:
            trades = load_closed_trade_summaries()
            cache_entry.state = build_portfolio_state(
                trades,
                params=params,
                current_balance=current_balance,
            )
            cache_entry.trade_ids = {closed_trade_summary_key(trade) for trade in trades}
            cache_entry.closed_count = int(stats['count'])
            cache_entry.max_last_updated = stats['max_last_updated']
            return cache_entry.state

        rows = load_detected_signals(
            status='CLOSED',
            updated_after=cache_entry.max_last_updated,
        )
        try:
            for trade in sorted(
                _merge_closed_trade_rows(rows, cache_entry.trade_ids),
                key=lambda summary: (summary.entry_time, summary.exit_time),
            ):
                cache_entry.state.record_closed_trade(trade)
        except ValueError:
            trades = load_closed_trade_summaries()
            cache_entry.state = build_portfolio_state(
                trades,
                params=params,
                current_balance=current_balance,
            )
            cache_entry.trade_ids = {closed_trade_summary_key(trade) for trade in trades}

        cache_entry.closed_count = int(stats['count'])
        cache_entry.max_last_updated = stats['max_last_updated']

    cache_entry.state.sync_balance(current_balance)
    return cache_entry.state


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

    nearest_dist = min(
        d for d in (support_dist, resistance_dist) if d is not None
    ) if support_dist is not None or resistance_dist is not None else None

    if nearest_dist is not None and nearest_dist <= NEAR_ZONE_THRESHOLD_PCT:
        if support_dist is not None and (
            resistance_dist is None or support_dist <= resistance_dist
        ):
            return "NEAR", f"{support_dist:.2f}% from support ({support.strength})"
        return "NEAR", f"{resistance_dist:.2f}% from resistance ({resistance.strength})"

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
        'PARTIAL': 2,
        'PENDING': 3,
        'NEAR': 4,
        'INSIDE': 5,
        'WATCH': 6,
        'NO DATA': 7,
    }.get(row.state, 7)
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
    s_dist = _distance_to_zone_pct(price, support, is_support=True)
    r_dist = _distance_to_zone_pct(price, resistance, is_support=False)

    if row.signal or row.state in {'OPEN', 'PARTIAL', 'PENDING', 'NO DATA'}:
        return replace(
            row,
            price=price,
            support_text=support_text,
            resistance_text=resistance_text,
            support_dist_pct=s_dist,
            resistance_dist_pct=r_dist,
        )

    state, note = _describe_watch_state(price, support, resistance)
    return replace(
        row,
        price=price,
        state=state,
        note=note,
        support_text=support_text,
        resistance_text=resistance_text,
        support_dist_pct=s_dist,
        resistance_dist_pct=r_dist,
    )


def _scan_pair(
    pair_id: str,
    pair_info: dict,
    params: StrategyParams,
    zone_history_days: int,
    tracked_pairs: Dict[str, Set[str]],
    tracked_states: Dict[str, str] | Set[str] | None = None,
    blocked_pairs: Optional[Set[str]] = None,
    price_cache: Optional[Dict[str, float]] = None,
    daily_data_cache: Optional[Dict[tuple[str, int], object]] = None,
    zone_cache: Optional[Dict[tuple[str, int], List[SRZone]]] = None,
    hourly_data_cache: Optional[Dict[str, object]] = None,
    portfolio_state: Optional[PortfolioState] = None,
    closed_trades: Optional[List[object]] = None,
) -> tuple[PairScanRow, Optional[Signal]]:
    """Scan one pair and return a watchlist row plus optional signal."""

    # Backward compatibility: older callers passed `blocked_pairs` in the
    # `tracked_states` position before tracked state labels were introduced.
    if blocked_pairs is None and (
        tracked_states is None or isinstance(tracked_states, set)
    ):
        blocked_pairs = set() if tracked_states is None else set(tracked_states)
        tracked_states = {}
    elif tracked_states is None:
        tracked_states = {}
    if blocked_pairs is None:
        blocked_pairs = set()

    decimals = pair_info.get('decimals', 5)
    name = pair_info.get('name', pair_id)

    daily_df, zones = _get_live_zones(
        pair_info['ticker'],
        zone_history_days,
        daily_data_cache=daily_data_cache,
        zone_cache=zone_cache,
    )
    if daily_df.empty:
        return (
            PairScanRow(pair_id, name, decimals, None, "NO DATA", "No daily data", "-", "-"),
            None,
        )

    current_price = float(daily_df['Close'].iloc[-1])
    nearest_support, nearest_resistance = get_tradeable_zones(zones, current_price)

    hourly_df = _get_live_hourly_data(
        pair_info['ticker'],
        days=3,
        hourly_data_cache=hourly_data_cache,
    )
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
                support_dist_pct=_distance_to_zone_pct(current_price, nearest_support, is_support=True),
                resistance_dist_pct=_distance_to_zone_pct(current_price, nearest_resistance, is_support=False),
            ),
            None,
        )

    last_bar = hourly_df.iloc[-1]
    current_price = float(last_bar['Close'])
    nearest_support, nearest_resistance = get_tradeable_zones(zones, current_price)
    if price_cache is not None:
        price_cache[pair_id] = current_price

    support_text = _format_zone_display(current_price, nearest_support, decimals, True)
    resistance_text = _format_zone_display(current_price, nearest_resistance, decimals, False)
    s_dist = _distance_to_zone_pct(current_price, nearest_support, is_support=True)
    r_dist = _distance_to_zone_pct(current_price, nearest_resistance, is_support=False)

    zone_fields = dict(
        support_lower=nearest_support.lower if nearest_support else None,
        support_upper=nearest_support.upper if nearest_support else None,
        support_strength=nearest_support.strength if nearest_support else None,
        resistance_lower=nearest_resistance.lower if nearest_resistance else None,
        resistance_upper=nearest_resistance.upper if nearest_resistance else None,
        resistance_strength=nearest_resistance.strength if nearest_resistance else None,
        support_dist_pct=s_dist,
        resistance_dist_pct=r_dist,
    )

    if pair_id in tracked_pairs:
        directions = "/".join(sorted(tracked_pairs[pair_id]))
        state = tracked_states.get(pair_id, 'OPEN')
        note = (
            f"Partial fill tracked ({directions})"
            if state == 'PARTIAL'
            else f"Tracked position ({directions})"
        )
        return (
            PairScanRow(
                pair_id, name, decimals, current_price,
                state, note,
                support_text, resistance_text, **zone_fields,
            ),
            None,
        )

    if pair_id in blocked_pairs:
        return (
            PairScanRow(
                pair_id, name, decimals, current_price,
                "PENDING", "Active order pending",
                support_text, resistance_text, **zone_fields,
            ),
            None,
        )

    if portfolio_state is None and closed_trades is not None:
        portfolio_state = build_portfolio_state(closed_trades, params=params)

    entry_block = get_entry_block(
        pair_id,
        hourly_df.index[-1],
        portfolio_state or [],
        params,
    )
    if entry_block is not None:
        state, note = entry_block
        return (
            PairScanRow(
                pair_id, name, decimals, current_price,
                state, note, support_text, resistance_text, **zone_fields,
            ),
            None,
        )

    signal = select_entry_signal(
        hourly_df=hourly_df,
        bar_idx=len(hourly_df) - 1,
        pair=pair_id,
        params=params,
        support_zone=nearest_support,
        resistance_zone=nearest_resistance,
    )

    if signal:
        note = f"{signal.zone_type.title()} reversal ({signal.zone_strength})"
        return (
            PairScanRow(
                pair_id, name, decimals, current_price,
                signal.direction, note, support_text, resistance_text,
                signal, **zone_fields,
            ),
            signal,
        )

    state, note = _describe_watch_state(current_price, nearest_support, nearest_resistance)
    return (
        PairScanRow(
            pair_id, name, decimals, current_price,
            state, note, support_text, resistance_text,
            **zone_fields,
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
    daily_data_cache: Optional[Dict[tuple[str, int], object]] = None,
    zone_cache: Optional[Dict[tuple[str, int], List[SRZone]]] = None,
    hourly_data_cache: Optional[Dict[str, object]] = None,
    portfolio_state: Optional[PortfolioState] = None,
    closed_trades: Optional[List[object]] = None,
) -> tuple[List[Signal], List[PairScanRow]]:
    """Collect structured pair rows and the executable signals among them."""

    if pairs is None:
        pairs = PAIRS
    if params is None:
        params = StrategyParams()
    if portfolio_state is None and closed_trades is not None:
        portfolio_state = build_portfolio_state(closed_trades, params=params)

    tracked_pairs: Dict[str, Set[str]] = {}
    tracked_states: Dict[str, str] = {}
    blocked_pairs = blocked_pairs or set()
    if tracked_positions:
        for info in tracked_positions.values():
            pair = info.get('pair')
            trade = info.get('trade')
            if pair and trade:
                tracked_pairs.setdefault(pair, set()).add(trade.direction)
                if info.get('signal_status') == 'PARTIAL':
                    tracked_states[pair] = 'PARTIAL'
                else:
                    tracked_states.setdefault(pair, 'OPEN')

    signals: List[Signal] = []
    pair_rows: List[PairScanRow] = []
    for pair_id, pair_info in pairs.items():
        if is_pair_fully_blocked(pair_id, params):
            continue
        row, signal = _scan_pair(
            pair_id,
            pair_info,
            params,
            zone_history_days,
            tracked_pairs,
            tracked_states,
            blocked_pairs,
            price_cache=price_cache,
            daily_data_cache=daily_data_cache,
            zone_cache=zone_cache,
            hourly_data_cache=hourly_data_cache,
            portfolio_state=portfolio_state,
            closed_trades=closed_trades,
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
    daily_data_cache: Optional[Dict[tuple[str, int], object]] = None,
    zone_cache: Optional[Dict[tuple[str, int], List[SRZone]]] = None,
    hourly_data_cache: Optional[Dict[str, object]] = None,
    portfolio_state: Optional[PortfolioState] = None,
    closed_trades: Optional[List[object]] = None,
) -> List[Signal]:
    """Scan all pairs and print a plain-text watch table."""

    signals, pair_rows = collect_scan_rows(
        pairs=pairs,
        params=params,
        zone_history_days=zone_history_days,
        tracked_positions=tracked_positions,
        blocked_pairs=blocked_pairs,
        price_cache=price_cache,
        daily_data_cache=daily_data_cache,
        zone_cache=zone_cache,
        hourly_data_cache=hourly_data_cache,
        portfolio_state=portfolio_state,
        closed_trades=closed_trades,
    )
    print(format_scan_rows(pair_rows))
    return signals


def format_signals(signals: List[Signal]) -> str:
    """Format live signals for display."""

    return format_signals_with_sizes(signals, size_plans=None)


def _build_price_lookup(
    price_cache: Optional[Dict[str, float]] = None,
    hourly_data_cache: Optional[Dict[str, object]] = None,
) -> Callable[[str], Optional[float]]:
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
        hourly_df = _get_live_hourly_data(
            pair_info['ticker'],
            days=3,
            hourly_data_cache=hourly_data_cache,
        )
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
    params: Optional[StrategyParams] = None,
    portfolio_state: Optional[PortfolioState] = None,
    closed_trades: Optional[List[object]] = None,
    price_cache: Optional[Dict[str, float]] = None,
    hourly_data_cache: Optional[Dict[str, object]] = None,
) -> List[Optional[PositionSizePlan]]:
    """Build per-signal live size plans from the shared compounding rule."""

    if not signals or balance is None or balance <= 0 or not account_currency:
        return [None for _ in signals]
    if params is None:
        params = StrategyParams()
    if portfolio_state is None:
        portfolio_state = build_portfolio_state(
            closed_trades or [],
            params=params,
            current_balance=balance,
        )

    price_lookup = _build_price_lookup(
        price_cache=price_cache,
        hourly_data_cache=hourly_data_cache,
    )
    return [
        build_position_size_plan(
            pair=signal.pair,
            direction=signal.direction,
            entry_price=signal.entry_price,
            stop_price=signal.sl_price,
            balance=balance,
            risk_pct=portfolio_state.effective_risk_pct(
                risk_pct,
                balance=balance,
                quality_score=signal.quality_score,
            ),
            account_currency=account_currency,
            price_lookup=price_lookup,
        )
        for signal in signals
    ]


def _estimate_reserved_portfolio_risk(
    tracked_positions: Optional[Dict[str, dict]],
    pending_pairs: Set[str],
    slot_risk_amount: Optional[float],
    account_currency: Optional[str],
    price_lookup: Callable[[str], Optional[float]],
) -> Optional[float]:
    """Estimate currently reserved risk from tracked and pending live exposure."""

    if slot_risk_amount is None or slot_risk_amount <= 0 or not account_currency:
        return None

    reserved_risk = float(slot_risk_amount) * len(pending_pairs)
    if not tracked_positions:
        return reserved_risk

    for info in tracked_positions.values():
        pair = info.get('pair')
        trade = info.get('trade')
        units = int(abs(info.get('ibkr_size') or 0))
        if not pair or trade is None:
            continue
        if units <= 0:
            reserved_risk += float(slot_risk_amount)
            continue

        estimated = estimate_position_risk_amount(
            pair=pair,
            entry_price=trade.entry_price,
            stop_price=trade.sl_price,
            units=units,
            account_currency=account_currency,
            price_lookup=price_lookup,
        )
        reserved_risk += float(slot_risk_amount) if estimated is None else float(estimated)

    return reserved_risk


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
def _prepare_execution_plan(
    signal: Signal,
    size_plan: PositionSizePlan,
    params: StrategyParams,
    price_lookup: Callable[[str], Optional[float]],
) -> tuple[Optional[PreparedExecutionPlan], str]:
    """Reprice a signal from a fresh live quote and rebuild size for submit time."""

    quote = ibkr.fetch_execution_quote(
        signal.pair,
        prefer_depth=params.prefer_l2_submit_quote,
    )
    if quote is None:
        return None, 'quote unavailable'

    core_plan, skip_note = build_execution_plan(
        signal,
        quote,
        params,
    )
    if core_plan is None:
        return None, skip_note

    def quote_price_lookup(pair_id: str) -> Optional[float]:
        if pair_id == signal.pair:
            return float(quote.mid)
        return price_lookup(pair_id)

    repriced_plan = build_position_size_plan_for_risk_amount(
        pair=signal.pair,
        direction=signal.direction,
        entry_price=core_plan.entry_price,
        stop_price=core_plan.stop_price,
        balance=float(size_plan.balance),
        risk_amount=float(size_plan.risk_amount),
        account_currency=size_plan.account_currency,
        price_lookup=quote_price_lookup,
    )
    if repriced_plan is None:
        return None, 'size unavailable'

    return (
        PreparedExecutionPlan(
            signal=signal,
            size_plan=repriced_plan,
            entry_price=core_plan.entry_price,
            stop_price=core_plan.stop_price,
            take_profit_price=core_plan.take_profit_price,
            quote=quote,
        ),
        '',
    )


def execute_signal_plans(
    signals: List[Signal],
    size_plans: List[Optional[PositionSizePlan]],
    execute_orders: bool,
    existing_pairs: Optional[Set[str]] = None,
    pending_pairs: Optional[Set[str]] = None,
    params: Optional[StrategyParams] = None,
    tracked_positions: Optional[Dict[str, dict]] = None,
    balance: Optional[float] = None,
    risk_pct: Optional[float] = None,
    account_currency: Optional[str] = None,
    price_cache: Optional[Dict[str, float]] = None,
    hourly_data_cache: Optional[Dict[str, object]] = None,
) -> List[ExecutionResult]:
    """Submit market orders for valid size plans when execution is enabled."""

    if not execute_orders or not signals:
        return []

    if params is None:
        params = StrategyParams()
    existing_pairs = existing_pairs or set()
    pending_pairs = pending_pairs or set()
    slot_risk_amount = (
        calculate_risk_amount(balance, risk_pct)
        if balance is not None and risk_pct is not None and account_currency
        else None
    )
    price_lookup = _build_price_lookup(
        price_cache=price_cache,
        hourly_data_cache=hourly_data_cache,
    )
    reserved_risk = _estimate_reserved_portfolio_risk(
        tracked_positions=tracked_positions,
        pending_pairs=pending_pairs,
        slot_risk_amount=slot_risk_amount,
        account_currency=account_currency,
        price_lookup=price_lookup,
    )
    correlation_cap = max(int(params.max_correlated_trades), 1)
    max_total_risk = (
        float(slot_risk_amount) * correlation_cap
        if slot_risk_amount is not None and params.use_correlation_filter
        else None
    )
    results: list[Optional[ExecutionResult]] = [None] * len(signals)
    planned: dict[int, PreparedExecutionPlan] = {}
    planned_reserved_risk = 0.0

    exposures: list[CorrelationExposure] = []
    seen_nonreplaceable_pairs: set[str] = set()
    if tracked_positions:
        for info in tracked_positions.values():
            pair = (info.get('pair') or '').upper()
            trade = info.get('trade')
            if not pair or pair in seen_nonreplaceable_pairs:
                continue
            exposures.append(
                CorrelationExposure(
                    pair=pair,
                    quality_score=float(getattr(trade, 'quality_score', 0.0) or 0.0),
                    replaceable=False,
                    payload=pair,
                )
            )
            seen_nonreplaceable_pairs.add(pair)
    for pair in sorted(set(existing_pairs) | set(pending_pairs)):
        pair = pair.upper()
        if pair in seen_nonreplaceable_pairs:
            continue
        exposures.append(
            CorrelationExposure(
                pair=pair,
                quality_score=0.0,
                replaceable=False,
                payload=pair,
            )
        )
        seen_nonreplaceable_pairs.add(pair)

    for idx, (signal, plan) in enumerate(zip(signals, size_plans)):
        if plan is None:
            results[idx] = ExecutionResult(signal.pair, signal.direction, 0, 'SKIPPED', note='size unavailable')
            continue

        same_pair_nonreplaceable = any(
            exposure.pair == signal.pair and not exposure.replaceable
            for exposure in exposures
        )
        if same_pair_nonreplaceable:
            results[idx] = ExecutionResult(
                signal.pair,
                signal.direction,
                plan.units,
                'SKIPPED',
                note='position/order exists',
            )
            continue

        replace_candidates = {
            exposure.payload: exposure
            for exposure in exposures
            if exposure.pair == signal.pair and exposure.replaceable
        }
        if replace_candidates:
            worst_same_pair = min(
                replace_candidates.values(),
                key=lambda exposure: exposure.quality_score,
            )
            if float(signal.quality_score) > float(worst_same_pair.quality_score):
                candidate_exposures = [
                    exposure
                    for exposure in exposures
                    if exposure.payload != worst_same_pair.payload
                ]
            else:
                results[idx] = ExecutionResult(
                    signal.pair,
                    signal.direction,
                    plan.units,
                    'SKIPPED',
                    note='duplicate pair signal',
                )
                continue
        else:
            worst_same_pair = None
            candidate_exposures = list(exposures)

        allowed, replaced_corr = apply_correlation_policy(
            candidate_exposures,
            candidate_pair=signal.pair,
            candidate_quality=signal.quality_score,
            params=params,
        )
        if not allowed:
            results[idx] = ExecutionResult(
                signal.pair,
                signal.direction,
                plan.units,
                'SKIPPED',
                note='correlation cap reached',
            )
            continue

        replaced_exposures = {}
        for exposure in (worst_same_pair, replaced_corr):
            if exposure is not None:
                replaced_exposures[exposure.payload] = exposure

        prepared_plan, skip_note = _prepare_execution_plan(
            signal,
            plan,
            params,
            price_lookup,
        )
        if prepared_plan is None:
            results[idx] = ExecutionResult(
                signal.pair,
                signal.direction,
                plan.units,
                'SKIPPED',
                note=skip_note or 'quote unavailable',
            )
            continue

        candidate_reserved_risk = planned_reserved_risk + float(prepared_plan.size_plan.risk_amount)
        for payload in replaced_exposures:
            prior = planned.get(payload)
            if prior is not None:
                candidate_reserved_risk -= float(prior.size_plan.risk_amount)

        if (
            max_total_risk is not None
            and reserved_risk is not None
            and reserved_risk + candidate_reserved_risk > max_total_risk + 1e-9
        ):
            results[idx] = ExecutionResult(
                signal.pair,
                signal.direction,
                plan.units,
                'SKIPPED',
                note='risk budget full',
            )
            continue

        for payload in replaced_exposures:
            prior = planned.pop(payload, None)
            if prior is None:
                continue
            planned_reserved_risk -= float(prior.size_plan.risk_amount)
            results[payload] = ExecutionResult(
                prior.signal.pair,
                prior.signal.direction,
                prior.size_plan.units,
                'SKIPPED',
                note='replaced by higher-quality correlated signal',
            )
            exposures = [
                exposure for exposure in exposures
                if exposure.payload != payload
            ]

        planned[idx] = prepared_plan
        planned_reserved_risk += float(prepared_plan.size_plan.risk_amount)
        exposures.append(
            CorrelationExposure(
                pair=signal.pair,
                quality_score=float(signal.quality_score),
                replaceable=True,
                payload=idx,
            )
        )

    for idx, prepared in planned.items():
        signal = prepared.signal
        plan = prepared.size_plan
        quote = prepared.quote
        order_ref = f"fxsr:{signal.pair}:{signal.direction}:{signal.time.strftime('%Y%m%d%H%M%S')}"
        order = ibkr.submit_fx_market_bracket_order(
            pair=signal.pair,
            direction=signal.direction,
            quantity=plan.units,
            take_profit_price=prepared.take_profit_price,
            stop_loss_price=prepared.stop_price,
            order_ref=order_ref,
        )
        if order is None:
            results[idx] = ExecutionResult(
                signal.pair,
                signal.direction,
                plan.units,
                'FAILED',
                submitted_entry_price=prepared.entry_price,
                submitted_tp_price=prepared.take_profit_price,
                submitted_sl_price=prepared.stop_price,
                submit_bid=quote.bid,
                submit_ask=quote.ask,
                submit_spread=quote.spread,
                quote_source=quote.source,
                quote_time=quote.captured_at,
                note='broker rejected/failed',
            )
            continue

        filled_units = int(abs(order.get('filled_units') or 0))
        remaining_units = order.get('remaining_units')
        if remaining_units is not None:
            remaining_units = int(max(abs(float(remaining_units)), 0.0))
        broker_status = order.get('broker_status') or order.get('status')
        result_status = order.get('status') or 'SUBMITTED'
        note = f"risk {plan.account_currency} {plan.risk_amount:,.2f}; {quote.source} quote @ {prepared.entry_price:.5f}; order submitted"
        if filled_units > 0:
            if remaining_units is None:
                result_status = 'PARTIAL'
                note = f"partial fill {filled_units:,}/{plan.units:,}"
            elif remaining_units > 0:
                result_status = 'PARTIAL'
                note = f"partial fill {filled_units:,}/{plan.units:,}"
            else:
                result_status = 'OPEN'
                note = f"filled {filled_units:,}/{plan.units:,}"

        results[idx] = ExecutionResult(
            signal.pair,
            signal.direction,
            plan.units,
            result_status,
            order_id=order.get('order_id'),
            take_profit_order_id=order.get('take_profit_order_id'),
            stop_loss_order_id=order.get('stop_loss_order_id'),
            avg_fill_price=order.get('avg_fill_price'),
            filled_units=filled_units,
            remaining_units=remaining_units,
            broker_status=broker_status,
            submitted_entry_price=prepared.entry_price,
            submitted_tp_price=prepared.take_profit_price,
            submitted_sl_price=prepared.stop_price,
            submit_bid=quote.bid,
            submit_ask=quote.ask,
            submit_spread=quote.spread,
            quote_source=quote.source,
            quote_time=quote.captured_at,
            note=note,
        )

    return [
        result if result is not None else ExecutionResult(signal.pair, signal.direction, 0, 'SKIPPED', note='not planned')
        for signal, result in zip(signals, results)
    ]


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
        daily_data_cache: Dict[tuple[str, int], object] = {}
        zone_cache: Dict[tuple[str, int], List[SRZone]] = {}
        hourly_data_cache: Dict[str, object] = {}
        active_balance = balance
        env_currency = os.getenv('IBKR_ACCOUNT_CURRENCY')
        active_currency = account_currency.upper() if account_currency else (env_currency.upper() if env_currency else None)
        if active_balance is None:
            active_balance, fetched_currency = ibkr.fetch_account_net_liquidation()
            if active_currency is None and fetched_currency not in (None, 'BASE'):
                active_currency = fetched_currency
        portfolio_state = load_portfolio_state(params, current_balance=active_balance)

        signals, pair_rows = collect_scan_rows(
            pairs=pairs,
            params=params,
            zone_history_days=zone_history_days,
            tracked_positions=tracked,
            blocked_pairs=pending_pairs,
            price_cache=market_prices,
            daily_data_cache=daily_data_cache,
            zone_cache=zone_cache,
            hourly_data_cache=hourly_data_cache,
            portfolio_state=portfolio_state,
        )

        size_plans = build_live_size_plans(
            signals,
            active_balance,
            risk_pct,
            active_currency,
            params=params,
            portfolio_state=portfolio_state,
            price_cache=market_prices,
            hourly_data_cache=hourly_data_cache,
        )
        exec_mode = ibkr.get_execution_mode() if execute_orders else 'scan'
        ibkr_acct = ibkr.fetch_account_id() if execute_orders else None
        signal_ids = record_detected_signals(
            signals,
            size_plans,
            execute_orders=execute_orders,
            execution_mode=exec_mode,
            ibkr_account=ibkr_acct,
        )
        execution_results = execute_signal_plans(
            signals,
            size_plans,
            execute_orders=execute_orders,
            existing_pairs={info['pair'] for info in tracked.values()},
            pending_pairs=pending_pairs,
            params=params,
            tracked_positions=tracked,
            balance=active_balance,
            risk_pct=risk_pct,
            account_currency=active_currency,
            price_cache=market_prices,
            hourly_data_cache=hourly_data_cache,
        )
        record_execution_results(
            signals, size_plans, execution_results,
            execution_mode=exec_mode,
            ibkr_account=ibkr_acct,
        )
        if execute_orders:
            reconcile_detected_signal_orders(signal_ids=signal_ids)
            if track_positions:
                tracked = sync_positions(params, zone_history_days)

        execution_by_pair = {
            result.pair: result
            for result in execution_results
            if result.status in {'PARTIAL', 'OPEN', 'SUBMITTED', 'Submitted', 'PreSubmitted', 'PRESUBMITTED'}
        }
        if execution_by_pair:
            updated_rows: List[PairScanRow] = []
            for row in pair_rows:
                result = execution_by_pair.get(row.pair)
                if result is None:
                    updated_rows.append(row)
                    continue
                if result.status == 'PARTIAL':
                    updated_rows.append(replace(row, state='PARTIAL', note=result.note, signal=None))
                elif result.status == 'OPEN':
                    updated_rows.append(replace(row, state='OPEN', note=result.note, signal=None))
                else:
                    updated_rows.append(replace(row, state='PENDING', note=result.note, signal=None))
            pair_rows = updated_rows

        alerts: List[dict] = []
        position_snapshots: Dict[str, dict] = {}
        if track_positions and tracked:
            alerts, position_snapshots = check_position_exits(
                tracked,
                params,
                hourly_data_cache=hourly_data_cache,
            )

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
