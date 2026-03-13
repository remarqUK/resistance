"""Backtesting engine: daily zones + hourly execution.

Strategy flow:
1. Detect S/R zones from daily chart data
2. Walk forward through 1-hour bars
3. When price enters a zone on 1H, look for reversal candle -> enter
4. Manage trade: early exit on zone break, hold winners to TP
5. Re-detect zones when new daily bars form
"""

import time
import pandas as pd
import numpy as np
import hashlib
import json
import os
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

from .config import PAIRS, DEFAULT_ZONE_HISTORY_DAYS
from .data import fetch_daily_data, fetch_hourly_data
from .levels import detect_zones, SRZone
from .profiles import PROFILES
from .strategy import (
    Trade, StrategyParams, check_exit, get_correlated_pairs, get_market_exit_price,
    build_trade_from_signal, get_tradeable_zones, is_pair_fully_blocked, params_from_profile,
    select_entry_signal,
)
from .db import load_backtest_result, save_backtest_result
from . import ibkr
from .sizing import calculate_risk_amount


@dataclass
class BacktestResult:
    """Aggregated results from a single pair backtest."""
    pair: str
    total_trades: int
    winning_trades: int
    losing_trades: int
    early_exits: int
    win_rate: float
    total_pnl_pips: float
    avg_pnl_pips: float
    avg_win_r: float
    avg_loss_r: float
    max_win_pips: float
    max_loss_pips: float
    profit_factor: float
    trades: List[Trade]
    zones: List[SRZone]


BACKTEST_CACHE_VERSION = '5'


def _serialize_timestamp(value: pd.Timestamp | None) -> str | None:
    if value is None:
        return None
    return pd.Timestamp(value).isoformat()


def _deserialize_timestamp(value: str | None) -> pd.Timestamp | None:
    if not value:
        return None
    return pd.Timestamp(value)


def _trade_to_dict(trade: Trade) -> dict:
    return {
        'entry_time': _serialize_timestamp(trade.entry_time),
        'entry_price': float(trade.entry_price),
        'direction': trade.direction,
        'sl_price': float(trade.sl_price),
        'tp_price': float(trade.tp_price),
        'zone_upper': float(trade.zone_upper),
        'zone_lower': float(trade.zone_lower),
        'zone_strength': trade.zone_strength,
        'risk': float(trade.risk),
        'exit_time': _serialize_timestamp(trade.exit_time),
        'exit_price': float(trade.exit_price) if trade.exit_price is not None else None,
        'exit_reason': trade.exit_reason,
        'pnl_pips': float(trade.pnl_pips),
        'pnl_r': float(trade.pnl_r),
        'bars_held': int(trade.bars_held),
        'quality_score': float(trade.quality_score),
    }


def _zone_to_dict(zone: SRZone) -> dict:
    return {
        'upper': float(zone.upper),
        'lower': float(zone.lower),
        'midpoint': float(zone.midpoint),
        'touches': int(zone.touches),
        'zone_type': zone.zone_type,
        'strength': zone.strength,
        'first_seen': _serialize_timestamp(zone.first_seen),
        'last_seen': _serialize_timestamp(zone.last_seen),
    }


def _serialize_backtest_result(result: BacktestResult) -> str:
    payload = {
        'pair': result.pair,
        'total_trades': int(result.total_trades),
        'winning_trades': int(result.winning_trades),
        'losing_trades': int(result.losing_trades),
        'early_exits': int(result.early_exits),
        'win_rate': float(result.win_rate),
        'total_pnl_pips': float(result.total_pnl_pips),
        'avg_pnl_pips': float(result.avg_pnl_pips),
        'avg_win_r': float(result.avg_win_r),
        'avg_loss_r': float(result.avg_loss_r),
        'max_win_pips': float(result.max_win_pips),
        'max_loss_pips': float(result.max_loss_pips),
        'profit_factor': float(result.profit_factor),
        'trades': [_trade_to_dict(t) for t in result.trades],
        'zones': [_zone_to_dict(z) for z in result.zones],
    }
    return json.dumps(payload, sort_keys=True)


def _deserialize_backtest_result(raw: str) -> BacktestResult:
    data = json.loads(raw)

    trades = []
    for trade in data.get('trades', []):
        trades.append(
            Trade(
                entry_time=_deserialize_timestamp(trade.get('entry_time')),
                entry_price=float(trade['entry_price']),
                direction=trade['direction'],
                sl_price=float(trade['sl_price']),
                tp_price=float(trade['tp_price']),
                zone_upper=float(trade['zone_upper']),
                zone_lower=float(trade['zone_lower']),
                zone_strength=trade['zone_strength'],
                risk=float(trade['risk']),
                exit_time=_deserialize_timestamp(trade.get('exit_time')),
                exit_price=trade.get('exit_price'),
                exit_reason=trade.get('exit_reason'),
                pnl_pips=float(trade.get('pnl_pips', 0.0)),
                pnl_r=float(trade.get('pnl_r', 0.0)),
                bars_held=int(trade.get('bars_held', 0)),
                quality_score=float(trade.get('quality_score', 0.0)),
            )
        )

    zones = []
    for zone in data.get('zones', []):
        zones.append(
            SRZone(
                upper=float(zone['upper']),
                lower=float(zone['lower']),
                midpoint=float(zone['midpoint']),
                touches=int(zone['touches']),
                zone_type=zone['zone_type'],
                strength=zone['strength'],
                first_seen=_deserialize_timestamp(zone.get('first_seen')),
                last_seen=_deserialize_timestamp(zone.get('last_seen')),
            )
        )

    return BacktestResult(
        pair=data['pair'],
        total_trades=int(data['total_trades']),
        winning_trades=int(data['winning_trades']),
        losing_trades=int(data['losing_trades']),
        early_exits=int(data['early_exits']),
        win_rate=float(data['win_rate']),
        total_pnl_pips=float(data['total_pnl_pips']),
        avg_pnl_pips=float(data['avg_pnl_pips']),
        avg_win_r=float(data['avg_win_r']),
        avg_loss_r=float(data['avg_loss_r']),
        max_win_pips=float(data['max_win_pips']),
        max_loss_pips=float(data['max_loss_pips']),
        profit_factor=float(data['profit_factor']),
        trades=trades,
        zones=zones,
    )


def _params_signature(params: StrategyParams) -> str:
    payload = _strategy_params_to_dict(params)
    # Stable and deterministic for identical value objects.
    payload_json = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(payload_json.encode('utf-8')).hexdigest()


def _strategy_params_to_dict(params: StrategyParams) -> dict:
    payload = params.__dict__.copy()
    payload['blocked_hours'] = sorted(payload.get('blocked_hours', []) or [])
    payload['blocked_days'] = sorted(payload.get('blocked_days', []) or [])
    payload['zone_windows'] = list(payload.get('zone_windows', ()))
    return payload


def _profile_name_for_params_hash(params_hash: str) -> str | None:
    for profile_name, profile in PROFILES.items():
        if _params_signature(params_from_profile(profile)) == params_hash:
            return profile_name
    return None


def build_backtest_run_config_json(
    params: StrategyParams,
    hourly_days: int,
    zone_history_days: int,
    *,
    requested_profile: str | None = None,
    starting_balance: float | None = None,
    risk_pct: float | None = None,
    selection_label: str | None = None,
) -> str:
    """Serialize a self-describing run configuration for cache rows."""

    params_hash = _params_signature(params)
    payload = {
        'requested_profile': requested_profile,
        'resolved_profile': _profile_name_for_params_hash(params_hash),
        'selection_label': selection_label,
        'params_hash': params_hash,
        'strategy_version': BACKTEST_CACHE_VERSION,
        'hourly_days': int(hourly_days),
        'zone_history_days': int(zone_history_days),
        'starting_balance': (
            None if starting_balance is None else float(starting_balance)
        ),
        'risk_pct': None if risk_pct is None else float(risk_pct),
        'strategy_params': _strategy_params_to_dict(params),
    }
    return json.dumps(payload, sort_keys=True)


def _normalize_df_for_signature(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=['ts', 'Open', 'High', 'Low', 'Close', 'Volume'])

    normalized = df.copy()
    normalized = normalized.sort_index()
    normalized = normalized[['Open', 'High', 'Low', 'Close', 'Volume']]
    normalized.index = pd.to_datetime(normalized.index, utc=True)
    normalized = normalized.reset_index()
    normalized.rename(columns={'index': 'ts'}, inplace=True)
    normalized['ts'] = normalized['ts'].astype('int64')
    return normalized


def _data_signature(daily_df: pd.DataFrame, hourly_df: pd.DataFrame) -> str:
    signature_payload = json.dumps({
        'daily': _normalize_df_for_signature(daily_df).to_json(orient='split', date_unit='ns'),
        'hourly': _normalize_df_for_signature(hourly_df).to_json(orient='split', date_unit='ns'),
    }, sort_keys=True)
    return hashlib.sha256(signature_payload.encode('utf-8')).hexdigest()


def _slice_daily_window(
    daily_df: pd.DataFrame,
    end_date,
    zone_history_days: int,
) -> pd.DataFrame:
    """Return a walk-forward daily window bounded by zone_history_days."""
    if daily_df.empty or zone_history_days <= 0:
        return daily_df.iloc[0:0]

    end_ts = pd.Timestamp(end_date)
    end_day = end_ts.date() if hasattr(end_ts, 'date') else end_date
    start_day = (end_ts - pd.Timedelta(days=max(zone_history_days - 1, 0))).date()

    if hasattr(daily_df.index, 'date'):
        index_dates = daily_df.index.date
        mask = (index_dates >= start_day) & (index_dates <= end_day)
        return daily_df[mask]

    bounded = daily_df[daily_df.index <= end_ts]
    return bounded.tail(zone_history_days)


def _finalize_trade(
    trade: Trade,
    exit_time,
    exit_price: float,
    exit_reason: str,
    bars_held: int,
    pip: float,
) -> Trade:
    """Populate final trade state and derived P&L metrics."""
    trade.exit_time = exit_time
    trade.exit_price = float(exit_price)
    trade.exit_reason = exit_reason
    trade.bars_held = bars_held

    if trade.direction == 'LONG':
        trade.pnl_pips = (trade.exit_price - trade.entry_price) / pip
        if trade.risk > 0:
            trade.pnl_r = (trade.exit_price - trade.entry_price) / trade.risk
    else:
        trade.pnl_pips = (trade.entry_price - trade.exit_price) / pip
        if trade.risk > 0:
            trade.pnl_r = (trade.entry_price - trade.exit_price) / trade.risk

    return trade


def _deduplicate_zones(zones: List[SRZone]) -> List[SRZone]:
    """Merge overlapping zones from multiple detection windows.

    When two zones overlap significantly, keep the one with more touches.
    """
    if not zones:
        return []

    # Sort by midpoint
    sorted_zones = sorted(zones, key=lambda z: z.midpoint)
    merged = []

    for zone in sorted_zones:
        if not merged:
            merged.append(zone)
            continue

        last = merged[-1]
        # Check overlap: zones overlap if they share price range
        overlap = min(last.upper, zone.upper) - max(last.lower, zone.lower)
        min_width = min(last.upper - last.lower, zone.upper - zone.lower)
        if min_width > 0 and overlap / min_width > 0.5:
            # Overlapping: keep the one with more touches
            if zone.touches > last.touches:
                merged[-1] = zone
        else:
            merged.append(zone)

    return merged


def run_backtest(
    daily_df: pd.DataFrame,
    hourly_df: pd.DataFrame,
    pair: str,
    params: StrategyParams = None,
    zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS,
) -> BacktestResult:
    """Run multi-timeframe backtest: daily zones + hourly execution.

    Args:
        daily_df: Daily OHLC data (for zone detection)
        hourly_df: 1-hour OHLC data (for trade execution)
        pair: pair identifier (e.g., 'EURUSD')
        params: strategy parameters
        zone_history_days: days of daily data for zone detection

    Returns:
        BacktestResult with trade list and statistics
    """
    if params is None:
        params = StrategyParams()

    pair_info = PAIRS.get(pair, {})
    pip = pair_info.get('pip', 0.0001)

    trades: List[Trade] = []
    current_trade: Optional[Trade] = None
    current_zones: List[SRZone] = []
    nearest_support: Optional[SRZone] = None
    nearest_resistance: Optional[SRZone] = None
    last_trade_bar = -params.cooldown_bars
    last_trade_was_loss = False
    last_zone_date = None
    trade_entry_bar = 0  # bar index when current trade was opened

    for i in range(len(hourly_df)):
        row = hourly_df.iloc[i]
        current_time = hourly_df.index[i]
        current_date = current_time.date() if hasattr(current_time, 'date') else current_time

        # Re-detect zones when a new day starts
        bar_date = pd.Timestamp(current_date).tz_localize(current_time.tzinfo) if hasattr(current_time, 'tzinfo') and current_time.tzinfo else pd.Timestamp(current_date)

        if last_zone_date is None or str(current_date) != str(last_zone_date):
            # Use only daily data up to current date, bounded by the rolling lookback.
            daily_window = _slice_daily_window(daily_df, bar_date, zone_history_days)

            if len(daily_window) >= 20:
                current_zones = detect_zones(daily_window)
            else:
                current_zones = []
            last_zone_date = current_date

        current_price = float(row['Close'])
        nearest_support, nearest_resistance = get_tradeable_zones(current_zones, current_price)

        # Check exit if holding a position
        if current_trade is not None:
            bars_held = i - trade_entry_bar
            result = check_exit(
                current_trade,
                bar_high=row['High'],
                bar_low=row['Low'],
                bar_close=row['Close'],
                bar_time=current_time,
                bars_held=bars_held,
                params=params,
                pip=pip,
            )
            if result:
                exit_reason, exit_price = result
                closed = _finalize_trade(current_trade, current_time, exit_price, exit_reason, bars_held, pip)
                trades.append(closed)
                last_trade_bar = i
                last_trade_was_loss = closed.pnl_r <= 0
                current_trade = None
                continue

        # Check for new entry if flat and cooldown elapsed
        cooldown = params.cooldown_bars
        if last_trade_was_loss and params.loss_cooldown_bars > 0:
            cooldown = max(cooldown, params.loss_cooldown_bars)
        if current_trade is None and (i - last_trade_bar) >= cooldown:
            signal = select_entry_signal(
                hourly_df=hourly_df,
                bar_idx=i,
                pair=pair,
                params=params,
                support_zone=nearest_support,
                resistance_zone=nearest_resistance,
            )
            if signal:
                current_trade = build_trade_from_signal(signal)
                trade_entry_bar = i

    # Force-close any open trade at end of data
    if current_trade is not None:
        trades.append(
            _finalize_trade(
                current_trade,
                hourly_df.index[-1],
                get_market_exit_price(float(hourly_df['Close'].iloc[-1]), current_trade.direction, pip, params),
                'END',
                len(hourly_df) - 1 - trade_entry_bar,
                pip,
            )
        )

    return _compile_results(pair, trades, current_zones)


def precompute_zone_cache(
    data: Dict[str, tuple],
    zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS,
) -> Dict[tuple, List[SRZone]]:
    """Pre-compute zones for every (pair, date) combination.

    Zone detection only depends on daily data, not strategy params,
    so we can compute once and reuse across many parameter sweeps.

    Args:
        data: {pair: (daily_df, hourly_df)} dict

    Returns:
        {(pair, date_str): [SRZone, ...]} lookup dict
    """
    cache = {}
    for pair, (daily_df, hourly_df) in data.items():
        if daily_df.empty or hourly_df.empty:
            continue
        # Get unique dates from hourly data
        seen_dates = set()
        for ts in hourly_df.index:
            d = ts.date() if hasattr(ts, 'date') else ts
            date_str = str(d)
            if date_str in seen_dates:
                continue
            seen_dates.add(date_str)

            # Filter daily data up to this date using the same rolling window as the backtest.
            daily_window = _slice_daily_window(daily_df, d, zone_history_days)
            if len(daily_window) >= 20:
                cache[(pair, date_str)] = detect_zones(daily_window)
            else:
                cache[(pair, date_str)] = []

    return cache


def _detect_zones_for_dates(
    daily_df: pd.DataFrame,
    pair: str,
    date_strs: List[str],
    zone_history_days: int,
) -> Dict[tuple, List[SRZone]]:
    """Process pool worker: compute zones for a chunk of dates for one pair.

    Receives the full daily DataFrame and a list of date strings.
    Slices the appropriate daily window for each date and runs zone detection.
    """
    results = {}
    for date_str in date_strs:
        daily_window = _slice_daily_window(daily_df, date_str, zone_history_days)
        if len(daily_window) >= 20:
            results[(pair, date_str)] = detect_zones(daily_window)
        else:
            results[(pair, date_str)] = []
    return results


def precompute_zone_cache_parallel(
    data: Dict[str, tuple],
    zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS,
    max_workers: int = None,
) -> Dict[tuple, List[SRZone]]:
    """Pre-compute zones for all (pair, date) combinations using all CPU cores.

    Distributes zone detection across a process pool to saturate all available
    cores.  Each worker receives one pair's daily data and a chunk of dates,
    then computes zones for every date in that chunk.

    Args:
        data: {pair: (daily_df, hourly_df)} dict
        zone_history_days: rolling daily window size for zone detection
        max_workers: process pool size (default: os.cpu_count())

    Returns:
        {(pair, date_str): [SRZone, ...]} lookup dict
    """
    if max_workers is None:
        max_workers = os.cpu_count() or 1
    # Windows caps ProcessPoolExecutor at 61 workers
    max_workers = min(max_workers, 61)

    # Collect per-pair work: (daily_df, sorted unique date strings)
    pair_work: Dict[str, tuple] = {}
    total_dates = 0
    for pair, (daily_df, hourly_df) in data.items():
        if daily_df.empty or hourly_df.empty:
            continue
        seen: set = set()
        dates: List[str] = []
        for ts in hourly_df.index:
            d = ts.date() if hasattr(ts, 'date') else ts
            ds = str(d)
            if ds not in seen:
                seen.add(ds)
                dates.append(ds)
        dates.sort()
        pair_work[pair] = (daily_df, dates)
        total_dates += len(dates)

    if not pair_work:
        return {}

    # Build tasks: split each pair's dates into chunks so the total
    # number of tasks is roughly 2x workers for good load balancing.
    tasks: list = []
    dates_per_chunk = max(1, total_dates // max(max_workers * 2, 1))
    for pair, (daily_df, dates) in pair_work.items():
        for i in range(0, len(dates), dates_per_chunk):
            chunk = dates[i:i + dates_per_chunk]
            tasks.append((daily_df, pair, chunk, zone_history_days))

    zone_cache: Dict[tuple, List[SRZone]] = {}
    if len(tasks) <= 1:
        # Not worth the overhead for a single task
        for task in tasks:
            zone_cache.update(_detect_zones_for_dates(*task))
        return zone_cache

    try:
        with ProcessPoolExecutor(max_workers=min(max_workers, len(tasks))) as executor:
            futures = [
                executor.submit(_detect_zones_for_dates, *task)
                for task in tasks
            ]
            for future in as_completed(futures):
                zone_cache.update(future.result())
    except (OSError, ValueError):
        # Fallback to sequential if process pool is unavailable
        for task in tasks:
            zone_cache.update(_detect_zones_for_dates(*task))

    return zone_cache


def _fetch_pair_data_only(
    pair: str,
    pair_info: dict,
    hourly_days: int,
    zone_history_days: int,
    force_refresh: bool,
    client_id: int | None,
) -> Tuple[str, pd.DataFrame, pd.DataFrame]:
    """Fetch daily and hourly data for a pair without running the backtest."""
    daily_df = fetch_daily_data(
        pair_info['ticker'],
        days=zone_history_days + hourly_days,
        force_refresh=force_refresh,
        allow_stale_cache=not force_refresh,
        client_id=client_id,
    )
    hourly_df = fetch_hourly_data(
        pair_info['ticker'],
        days=hourly_days,
        force_refresh=force_refresh,
        allow_stale_cache=not force_refresh,
        client_id=client_id,
    )
    return pair, daily_df, hourly_df


def run_backtest_fast(
    hourly_df: pd.DataFrame,
    pair: str,
    params: StrategyParams,
    zone_cache: Dict[tuple, List[SRZone]],
    pip: float,
) -> BacktestResult:
    """Fast backtest using pre-computed zones (skips zone detection).

    Identical logic to run_backtest but looks up zones from zone_cache
    instead of calling detect_zones on each new day.
    """
    trades: List[Trade] = []
    current_trade: Optional[Trade] = None
    current_zones: List[SRZone] = []
    nearest_support: Optional[SRZone] = None
    nearest_resistance: Optional[SRZone] = None
    last_trade_bar = -params.cooldown_bars
    last_trade_was_loss = False
    last_zone_date = None
    trade_entry_bar = 0

    for i in range(len(hourly_df)):
        row = hourly_df.iloc[i]
        current_time = hourly_df.index[i]
        current_date = current_time.date() if hasattr(current_time, 'date') else current_time
        date_str = str(current_date)

        # Look up pre-computed zones on new day
        if last_zone_date is None or date_str != str(last_zone_date):
            cached = zone_cache.get((pair, date_str))
            if cached is not None:
                current_zones = cached
            else:
                current_zones = []
            last_zone_date = current_date

        current_price = float(row['Close'])
        nearest_support, nearest_resistance = get_tradeable_zones(current_zones, current_price)

        # Check exit if holding a position
        if current_trade is not None:
            bars_held = i - trade_entry_bar
            result = check_exit(
                current_trade,
                bar_high=row['High'],
                bar_low=row['Low'],
                bar_close=row['Close'],
                bar_time=current_time,
                bars_held=bars_held,
                params=params,
                pip=pip,
            )
            if result:
                exit_reason, exit_price = result
                closed = _finalize_trade(current_trade, current_time, exit_price, exit_reason, bars_held, pip)
                trades.append(closed)
                last_trade_bar = i
                last_trade_was_loss = closed.pnl_r <= 0
                current_trade = None
                continue

        # Check for new entry if flat and cooldown elapsed
        cooldown = params.cooldown_bars
        if last_trade_was_loss and params.loss_cooldown_bars > 0:
            cooldown = max(cooldown, params.loss_cooldown_bars)
        if current_trade is None and (i - last_trade_bar) >= cooldown:
            signal = select_entry_signal(
                hourly_df=hourly_df,
                bar_idx=i,
                pair=pair,
                params=params,
                support_zone=nearest_support,
                resistance_zone=nearest_resistance,
            )
            if signal:
                current_trade = build_trade_from_signal(signal)
                trade_entry_bar = i

    # Force-close any open trade at end of data
    if current_trade is not None:
        trades.append(
            _finalize_trade(
                current_trade,
                hourly_df.index[-1],
                get_market_exit_price(float(hourly_df['Close'].iloc[-1]), current_trade.direction, pip, params),
                'END',
                len(hourly_df) - 1 - trade_entry_bar,
                pip,
            )
        )

    return _compile_results(pair, trades, current_zones)


def _compile_results(
    pair: str, trades: List[Trade], zones: List[SRZone]
) -> BacktestResult:
    """Calculate performance statistics from trade list."""
    wins = [t for t in trades if t.pnl_pips > 0]
    losses = [t for t in trades if t.pnl_pips <= 0]
    early_exits = [t for t in trades if t.exit_reason in ('EARLY_EXIT', 'SIDEWAYS', 'TIME')]

    gross_profit = sum(t.pnl_pips for t in wins) if wins else 0
    gross_loss = abs(sum(t.pnl_pips for t in losses)) if losses else 0
    total_pnl = sum(t.pnl_pips for t in trades)

    avg_win_r = np.mean([t.pnl_r for t in wins]) if wins else 0
    avg_loss_r = np.mean([t.pnl_r for t in losses]) if losses else 0

    return BacktestResult(
        pair=pair,
        total_trades=len(trades),
        winning_trades=len(wins),
        losing_trades=len(losses),
        early_exits=len(early_exits),
        win_rate=len(wins) / len(trades) * 100 if trades else 0,
        total_pnl_pips=total_pnl,
        avg_pnl_pips=total_pnl / len(trades) if trades else 0,
        avg_win_r=avg_win_r,
        avg_loss_r=avg_loss_r,
        max_win_pips=max((t.pnl_pips for t in trades), default=0),
        max_loss_pips=min((t.pnl_pips for t in trades), default=0),
        profit_factor=gross_profit / gross_loss if gross_loss > 0 else float('inf'),
        trades=trades,
        zones=zones,
    )


def run_all_backtests(
    daily_data: Dict[str, pd.DataFrame],
    hourly_data: Dict[str, pd.DataFrame],
    params: StrategyParams = None,
) -> Dict[str, BacktestResult]:
    """Run backtests on all pairs."""
    results = {}
    if params is None:
        params = StrategyParams()
    for pair in daily_data:
        if is_pair_fully_blocked(pair, params):
            print(f"  Skipping {pair}: both directions are blocked by strategy filter")
            continue
        if pair not in hourly_data:
            print(f"  Skipping {pair}: missing hourly data")
            continue
        daily_df = daily_data[pair]
        hourly_df = hourly_data[pair]
        if daily_df.empty or hourly_df.empty:
            print(f"  Skipping {pair}: empty data")
            continue

        print(f"  Backtesting {pair} ({len(daily_df)} daily bars, {len(hourly_df)} hourly bars)...")
        results[pair] = run_backtest(daily_df, hourly_df, pair, params)
        r = results[pair]
        print(f"    -> {r.total_trades} trades, {r.win_rate:.1f}% win rate, "
              f"{r.total_pnl_pips:.1f} pips, avg loss {r.avg_loss_r:.2f}R")
    return results


def _backtest_pair(
    pair: str,
    pair_info: dict,
    params: StrategyParams,
    hourly_days: int,
    zone_history_days: int,
    force_refresh: bool = False,
    client_id: int | None = None,
    run_config_json: str | None = None,
) -> Tuple[str, Optional[BacktestResult]]:
    """Fetch data and run backtest for a single pair."""
    params_hash = _params_signature(params)

    daily_df = fetch_daily_data(
        pair_info['ticker'],
        days=zone_history_days + hourly_days,
        force_refresh=force_refresh,
        allow_stale_cache=not force_refresh,
        client_id=client_id,
    )
    hourly_df = fetch_hourly_data(
        pair_info['ticker'],
        days=hourly_days,
        force_refresh=force_refresh,
        allow_stale_cache=not force_refresh,
        client_id=client_id,
    )
    if daily_df.empty or hourly_df.empty:
        return pair, None
    data_sig = _data_signature(daily_df, hourly_df)

    if not force_refresh:
        cached = load_backtest_result(
            pair,
            params_hash,
            hourly_days,
            zone_history_days,
        )
        if cached is not None:
            cached_sig, cached_json, strategy_version, cached_run_config_json = cached
            if (
                strategy_version == BACKTEST_CACHE_VERSION
                and cached_sig == data_sig
            ):
                try:
                    if run_config_json is not None and cached_run_config_json != run_config_json:
                        save_backtest_result(
                            pair=pair,
                            params_hash=params_hash,
                            hourly_days=hourly_days,
                            zone_history_days=zone_history_days,
                            data_signature=data_sig,
                            ticker=pair_info['ticker'],
                            strategy_version=BACKTEST_CACHE_VERSION,
                            result_json=cached_json,
                            run_config_json=run_config_json,
                        )
                    return pair, _deserialize_backtest_result(cached_json)
                except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                    pass

    result = run_backtest(daily_df, hourly_df, pair, params, zone_history_days)
    if result is not None:
        save_backtest_result(
            pair=pair,
            params_hash=params_hash,
            hourly_days=hourly_days,
            zone_history_days=zone_history_days,
            data_signature=data_sig,
            ticker=pair_info['ticker'],
            strategy_version=BACKTEST_CACHE_VERSION,
            result_json=_serialize_backtest_result(result),
            run_config_json=run_config_json,
        )
    return pair, result


def _run_backtest_pair_on_core(
    pair: str,
    pair_info: dict,
    params: StrategyParams,
    hourly_days: int,
    zone_history_days: int,
    force_refresh: bool,
    client_id: int | None,
    core_id: int | None = None,
    run_config_json: str | None = None,
) -> Tuple[str, Optional[BacktestResult]]:
    """Run a pair backtest in the current process and bind to a dedicated core if possible."""
    if core_id is not None:
        try:
            os.sched_setaffinity(0, {core_id})
        except (AttributeError, OSError, ValueError):
            pass
    return _backtest_pair(
        pair=pair,
        pair_info=pair_info,
        params=params,
        hourly_days=hourly_days,
        zone_history_days=zone_history_days,
        force_refresh=force_refresh,
        client_id=client_id,
        run_config_json=run_config_json,
    )


def _pair_client_id(base_client_id: int | None, offset: int) -> int | None:
    """Derive a stable client ID for a pair from the configured base."""
    if base_client_id is None:
        return None
    return int(base_client_id) + offset


def run_all_backtests_parallel(
    params: StrategyParams = None,
    hourly_days: int = 30,
    zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS,
    pairs: Dict = None,
    force_refresh: bool = False,
    base_client_id: int | None = None,
    run_config_json: str | None = None,
) -> Dict[str, BacktestResult]:
    """Run all pair backtests with maximum CPU utilisation.

    Three-phase pipeline for non-refresh runs:
    1. Fetch data for all pairs concurrently (thread pool for I/O)
    2. Pre-compute zone cache across all CPU cores (process pool)
    3. Walk-forward backtests with pre-computed zones (process pool)

    Cached backtest results are returned immediately without recomputation.
    Forced refresh runs execute sequentially so IBKR pacing limits are respected.
    """
    if params is None:
        params = StrategyParams()
    if pairs is None:
        pairs = PAIRS
    if base_client_id is None:
        base_client_id = ibkr.TWS_CLIENT_ID

    results = {}
    pair_items = [
        (pair, info)
        for pair, info in pairs.items()
        if not is_pair_fully_blocked(pair, params)
    ]
    if len(pair_items) < len(pairs):
        skipped = len(pairs) - len(pair_items)
        print(f"  Skipping {skipped} pair(s) with both directions blocked by strategy filter")
    total = len(pair_items)
    if total == 0:
        print("  No pairs eligible after strategy pair-direction filter; nothing to backtest.")
        return results
    done = 0
    client_id_suffix = ''
    if total > 0 and base_client_id is not None:
        last_client_id = _pair_client_id(base_client_id, total - 1)
        if total == 1:
            client_id_suffix = f" with client ID {base_client_id}"
        else:
            client_id_suffix = f" with client IDs {base_client_id}-{last_client_id}"

    if force_refresh:
        print(f"  Refreshing {total} backtests from IBKR/TWS sequentially{client_id_suffix}...")
        for offset, (pair, info) in enumerate(pair_items):
            pair, result = _backtest_pair(
                pair,
                info,
                params,
                hourly_days,
                zone_history_days,
                force_refresh,
                client_id=_pair_client_id(base_client_id, offset),
            )
            done += 1
            if result:
                results[pair] = result
                r = result
                print(f"    [{done}/{total}] {pair}: {r.total_trades} trades, "
                      f"{r.win_rate:.0f}% WR, {r.total_pnl_pips:+.0f} pips")
            else:
                print(f"    [{done}/{total}] {pair}: no data")
        return results

    cpu_count = os.cpu_count() or 1
    # Windows caps ProcessPoolExecutor at 61 workers (MAXIMUM_WAIT_OBJECTS - 3)
    max_pool_workers = min(cpu_count, 61)
    print(f"  Launching {total} backtests across {max_pool_workers} workers "
          f"(using cache when available{client_id_suffix})...")

    # --- Phase 1: Fetch data for all pairs concurrently ---
    t_phase = time.time()
    pair_data: Dict[str, tuple] = {}
    fetch_workers = min(total, 20)
    try:
        with ThreadPoolExecutor(max_workers=fetch_workers) as executor:
            futures = {
                executor.submit(
                    _fetch_pair_data_only,
                    pair, info, hourly_days, zone_history_days,
                    force_refresh, _pair_client_id(base_client_id, offset),
                ): pair
                for offset, (pair, info) in enumerate(pair_items)
            }
            for future in as_completed(futures):
                pair, daily_df, hourly_df = future.result()
                if (daily_df is not None and not daily_df.empty
                        and hourly_df is not None and not hourly_df.empty):
                    pair_data[pair] = (daily_df, hourly_df)
                else:
                    done += 1
                    print(f"    [{done}/{total}] {pair}: no data")
    except (OSError, ValueError):
        for offset, (pair, info) in enumerate(pair_items):
            cid = _pair_client_id(base_client_id, offset)
            pair, daily_df, hourly_df = _fetch_pair_data_only(
                pair, info, hourly_days, zone_history_days, force_refresh, cid,
            )
            if (daily_df is not None and not daily_df.empty
                    and hourly_df is not None and not hourly_df.empty):
                pair_data[pair] = (daily_df, hourly_df)
            else:
                done += 1
                print(f"    [{done}/{total}] {pair}: no data")

    if not pair_data:
        return results

    print(f"    Data fetched in {time.time() - t_phase:.1f}s")

    # --- Phase 2: Check backtest result cache, identify cache misses ---
    params_hash = _params_signature(params)
    pairs_to_compute: Dict[str, tuple] = {}
    for pair, (daily_df, hourly_df) in pair_data.items():
        data_sig = _data_signature(daily_df, hourly_df)
        cached = load_backtest_result(pair, params_hash, hourly_days, zone_history_days)
        if cached is not None:
            cached_sig, cached_json, strategy_version, cached_run_config_json = cached
            if (strategy_version == BACKTEST_CACHE_VERSION
                    and cached_sig == data_sig):
                try:
                    result = _deserialize_backtest_result(cached_json)
                    if run_config_json is not None and cached_run_config_json != run_config_json:
                        save_backtest_result(
                            pair=pair,
                            params_hash=params_hash,
                            hourly_days=hourly_days,
                            zone_history_days=zone_history_days,
                            data_signature=data_sig,
                            ticker=pairs[pair].get('ticker', pair),
                            strategy_version=BACKTEST_CACHE_VERSION,
                            result_json=cached_json,
                            run_config_json=run_config_json,
                        )
                    results[pair] = result
                    done += 1
                    r = result
                    print(f"    [{done}/{total}] {pair}: {r.total_trades} trades, "
                          f"{r.win_rate:.0f}% WR, {r.total_pnl_pips:+.0f} pips (cached)")
                    continue
                except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                    pass
        pairs_to_compute[pair] = (daily_df, hourly_df, data_sig)

    if not pairs_to_compute:
        return results

    # --- Phase 3+4: Zone pre-computation + walk-forwards (single process pool) ---
    # Build zone computation tasks so we can saturate all cores.
    t_phase = time.time()
    # First pass: collect all dates per pair
    pair_dates: Dict[str, tuple] = {}
    total_zone_dates = 0
    for pair, (daily_df, hourly_df, _) in pairs_to_compute.items():
        seen: set = set()
        dates: List[str] = []
        for ts in hourly_df.index:
            d = ts.date() if hasattr(ts, 'date') else ts
            ds = str(d)
            if ds not in seen:
                seen.add(ds)
                dates.append(ds)
        dates.sort()
        pair_dates[pair] = (daily_df, dates)
        total_zone_dates += len(dates)

    # Second pass: chunk dates to create ~2x cpu_count tasks for load balancing
    zone_tasks: list = []
    dates_per_chunk = max(1, total_zone_dates // max(max_pool_workers * 2, 1))
    for pair, (daily_df, dates) in pair_dates.items():
        for i in range(0, len(dates), dates_per_chunk):
            zone_tasks.append((daily_df, pair, dates[i:i + dates_per_chunk], zone_history_days))

    num_compute = len(pairs_to_compute)
    print(f"    {total_zone_dates} zone detections + {num_compute} walk-forwards "
          f"across {max_pool_workers} workers...")

    zone_cache: Dict[tuple, List[SRZone]] = {}
    try:
        with ProcessPoolExecutor(max_workers=max_pool_workers) as executor:
            # Submit all zone computation tasks
            zone_futures = [
                executor.submit(_detect_zones_for_dates, *task)
                for task in zone_tasks
            ]
            for future in as_completed(zone_futures):
                zone_cache.update(future.result())

            t_zones = time.time()
            print(f"    Zones computed in {t_zones - t_phase:.1f}s")

            # Submit walk-forward tasks (reuses the same pool — no extra startup)
            walk_futures = {}
            for pair, (daily_df, hourly_df, data_sig) in pairs_to_compute.items():
                pip = pairs[pair].get('pip', 0.0001)
                pair_zones = {k: v for k, v in zone_cache.items() if k[0] == pair}
                walk_futures[executor.submit(
                    run_backtest_fast, hourly_df, pair, params, pair_zones, pip,
                )] = (pair, data_sig)
            for future in as_completed(walk_futures):
                pair, data_sig = walk_futures[future]
                result = future.result()
                results[pair] = result
                done += 1
                r = result
                print(f"    [{done}/{total}] {pair}: {r.total_trades} trades, "
                      f"{r.win_rate:.0f}% WR, {r.total_pnl_pips:+.0f} pips")
                save_backtest_result(
                    pair=pair,
                    params_hash=params_hash,
                    hourly_days=hourly_days,
                    zone_history_days=zone_history_days,
                    data_signature=data_sig,
                    ticker=pairs[pair].get('ticker', pair),
                    strategy_version=BACKTEST_CACHE_VERSION,
                    result_json=_serialize_backtest_result(result),
                    run_config_json=run_config_json,
                )
    except (OSError, ValueError) as exc:
        print(f"    Process pool unavailable ({exc}); falling back to sequential.")
        for task in zone_tasks:
            zone_cache.update(_detect_zones_for_dates(*task))
        for pair, (daily_df, hourly_df, data_sig) in pairs_to_compute.items():
            pip = pairs[pair].get('pip', 0.0001)
            pair_zones = {k: v for k, v in zone_cache.items() if k[0] == pair}
            result = run_backtest_fast(hourly_df, pair, params, pair_zones, pip)
            results[pair] = result
            done += 1
            r = result
            print(f"    [{done}/{total}] {pair}: {r.total_trades} trades, "
                  f"{r.win_rate:.0f}% WR, {r.total_pnl_pips:+.0f} pips")
            save_backtest_result(
                pair=pair,
                params_hash=params_hash,
                hourly_days=hourly_days,
                zone_history_days=zone_history_days,
                data_signature=data_sig,
                ticker=pairs[pair].get('ticker', pair),
                strategy_version=BACKTEST_CACHE_VERSION,
                result_json=_serialize_backtest_result(result),
                run_config_json=run_config_json,
            )

    print(f"    Total compute in {time.time() - t_phase:.1f}s")
    return results


def apply_correlation_filter(
    all_trades: List[Tuple[str, Trade]],
    params: StrategyParams = None,
) -> List[Tuple[str, Trade]]:
    """Filter trades to enforce correlation limits.

    Skips a trade entry if there are already max_correlated_trades
    open on correlated pairs at that time.
    """
    if params is None:
        params = StrategyParams()
    if not params.use_correlation_filter:
        return all_trades

    filtered = []
    active: List[Tuple[str, Trade]] = []

    for pair_id, trade in all_trades:
        # Remove closed trades from active list
        active = [(p, t) for p, t in active
                  if t.exit_time is None or t.exit_time > trade.entry_time]

        correlated = get_correlated_pairs(pair_id)
        active_correlated = [p for p, t in active if p in correlated or p == pair_id]

        if len(active_correlated) >= params.max_correlated_trades:
            if params.correlation_prefer_quality:
                # Find worst-quality active correlated trade
                worst_idx = None
                worst_quality = float('inf')
                for idx, (p, t) in enumerate(active):
                    if p in correlated or p == pair_id:
                        if t.quality_score < worst_quality:
                            worst_quality = t.quality_score
                            worst_idx = idx
                if worst_idx is not None and trade.quality_score > worst_quality:
                    removed_pair, removed_trade = active[worst_idx]
                    filtered = [(p, t) for p, t in filtered
                                if not (p == removed_pair and t is removed_trade)]
                    active.pop(worst_idx)
                else:
                    continue
            else:
                continue

        filtered.append((pair_id, trade))
        active.append((pair_id, trade))

    return filtered


def calculate_compounding_pnl(
    results: Dict[str, BacktestResult],
    starting_balance: float = 1000.0,
    risk_pct: float = 0.05,
    params: StrategyParams = None,
) -> Tuple[List[Tuple[str, Trade, float, float, float]], float]:
    """Calculate compounding P&L from backtest results.

    1. Collects all trades from all pairs
    2. Sorts chronologically
    3. Applies correlation filter
    4. Walks through trades, compounding the balance

    Returns:
        (trade_log, final_balance) where trade_log entries are
        (pair, trade, risk_amount, pnl_amount, running_balance)
    """
    if params is None:
        params = StrategyParams()

    # Collect and sort all trades chronologically
    all_trades = []
    for pair, r in results.items():
        for t in r.trades:
            all_trades.append((pair, t))
    all_trades.sort(key=lambda x: x[1].entry_time)

    # Apply correlation filter
    filtered = apply_correlation_filter(all_trades, params)

    # Compound with optional losing-streak circuit breaker and dynamic risk
    balance = starting_balance
    peak_balance = starting_balance
    trade_log = []
    consecutive_losses = 0
    pause_until = None  # timestamp after which entries resume

    for pair, t in filtered:
        # Streak pause: skip entries that start during the pause window
        if pause_until is not None and t.entry_time <= pause_until:
            continue

        # Dynamic risk sizing: scale risk down during drawdowns
        if params.dynamic_risk and peak_balance > 0:
            dd_pct = (peak_balance - balance) / peak_balance * 100
            if dd_pct <= params.dd_risk_start:
                effective_risk = risk_pct
            elif dd_pct >= params.dd_risk_full:
                effective_risk = params.dd_risk_floor / 100.0
            else:
                frac = (dd_pct - params.dd_risk_start) / (params.dd_risk_full - params.dd_risk_start)
                effective_risk = risk_pct - (risk_pct - params.dd_risk_floor / 100.0) * frac
        else:
            effective_risk = risk_pct

        # Quality-based risk scaling
        if params.quality_sizing:
            multiplier = params.quality_risk_min + t.quality_score * (params.quality_risk_max - params.quality_risk_min)
            effective_risk *= multiplier

        risk_amt = calculate_risk_amount(balance, effective_risk)
        pnl = risk_amt * t.pnl_r
        balance += pnl
        if balance > peak_balance:
            peak_balance = balance
        trade_log.append((pair, t, risk_amt, pnl, balance))

        # Track consecutive losses for circuit breaker
        if t.pnl_r <= 0:
            consecutive_losses += 1
        else:
            consecutive_losses = 0
            pause_until = None

        if (
            params.streak_pause_trigger > 0
            and consecutive_losses >= params.streak_pause_trigger
        ):
            pause_until = t.exit_time + pd.Timedelta(hours=params.streak_pause_hours)
            consecutive_losses = 0  # reset after triggering

    return trade_log, balance


def format_compounding_results(
    trade_log: List[Tuple[str, Trade, float, float, float]],
    starting_balance: float,
    final_balance: float,
    total_pre_filter: int,
) -> str:
    """Format compounding P&L results as a readable report."""
    lines = []
    lines.append("=" * 130)
    lines.append("  COMPOUNDING P&L REPORT")
    lines.append("=" * 130)
    lines.append(
        f"  {'#':>3} {'Date':<20} {'Pair':<8} {'Dir':>5} {'Exit':>10} "
        f"{'Bars':>5} {'R-Mult':>7} {'Risk':>10} {'P&L':>11} {'Balance':>12}"
    )
    lines.append("-" * 130)

    monthly = {}
    peak = starting_balance
    max_dd = 0.0
    streak = 0
    max_losing_streak = 0

    for idx, (pair, t, risk_amt, pnl, balance) in enumerate(trade_log, 1):
        lines.append(
            f"  {idx:>3} {str(t.entry_time):<20} {pair:<8} {t.direction:>5} "
            f"{t.exit_reason:>10} {t.bars_held:>5} {t.pnl_r:>+7.2f}R "
            f"  GBP {risk_amt:>8.2f}  GBP {pnl:>+9.2f}  GBP {balance:>11.2f}"
        )

        # Monthly tracking
        month_key = str(t.entry_time)[:7]
        if month_key not in monthly:
            monthly[month_key] = {'trades': 0, 'pnl': 0.0, 'start_bal': balance - pnl}
        monthly[month_key]['trades'] += 1
        monthly[month_key]['pnl'] += pnl

        # Drawdown tracking
        if balance > peak:
            peak = balance
        dd = (peak - balance) / peak * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

        # Losing streak tracking
        if t.pnl_r <= 0:
            streak += 1
            if streak > max_losing_streak:
                max_losing_streak = streak
        else:
            streak = 0

    # Exit type counts
    exit_counts = {}
    for _, t, _, _, _ in trade_log:
        r = t.exit_reason or 'UNKNOWN'
        exit_counts[r] = exit_counts.get(r, 0) + 1

    wins = [e for e in trade_log if e[1].pnl_r > 0]
    losses = [e for e in trade_log if e[1].pnl_r <= 0]
    avg_win = np.mean([e[1].pnl_r for e in wins]) if wins else 0
    avg_loss = np.mean([e[1].pnl_r for e in losses]) if losses else 0

    lines.append("=" * 130)

    # Monthly breakdown
    lines.append("")
    lines.append("  MONTHLY BREAKDOWN:")
    lines.append(
        f"  {'MONTH':<10} {'TRADES':>7} {'P&L':>13} {'START BAL':>13} "
        f"{'END BAL':>13} {'RETURN':>8}"
    )
    lines.append("  " + "-" * 70)

    for month in sorted(monthly.keys()):
        m = monthly[month]
        end_bal = m['start_bal'] + m['pnl']
        ret = m['pnl'] / m['start_bal'] * 100 if m['start_bal'] > 0 else 0
        lines.append(
            f"  {month:<10} {m['trades']:>7}   GBP {m['pnl']:>+10.2f}   "
            f"GBP {m['start_bal']:>10.2f}   GBP {end_bal:>10.2f}  {ret:>+7.1f}%"
        )

    lines.append("")
    lines.append("=" * 130)
    lines.append(f"  Starting balance:     GBP {starting_balance:,.2f}")
    lines.append(f"  Final balance:        GBP {final_balance:,.2f}")
    lines.append(f"  Net P&L:              GBP {final_balance - starting_balance:+,.2f} "
                 f"({(final_balance - starting_balance) / starting_balance * 100:+.1f}%)")
    lines.append(f"  Total trades:         {len(trade_log)} "
                 f"(filtered from {total_pre_filter} by correlation)")
    lines.append(f"  Wins: {len(wins)}  Losses: {len(losses)}  "
                 f"Win rate: {len(wins)/len(trade_log)*100:.1f}%" if trade_log else "")
    lines.append(f"  Avg win: {avg_win:+.2f}R  Avg loss: {avg_loss:+.2f}R")
    lines.append(f"  Peak balance:         GBP {peak:,.2f}")
    lines.append(f"  Max drawdown:         {max_dd:.1f}%")
    lines.append(f"  Max losing streak:    {max_losing_streak} trades")
    lines.append(f"  Exit types:           {dict(sorted(exit_counts.items()))}")
    lines.append("=" * 130)

    return "\n".join(lines)


def format_results(
    results: Dict[str, BacktestResult],
    params: StrategyParams | None = None,
) -> str:
    """Format backtest results as a readable summary table."""
    lines = []
    lines.append("=" * 115)
    lines.append("  BACKTEST RESULTS - Daily Zone S/R Strategy (per-pair raw stats, portfolio totals adjusted)")
    lines.append("=" * 115)
    lines.append(
        f"  {'PAIR':<10} {'TRADES':>7} {'WINS':>6} {'LOSSES':>7} {'EARLY':>6} "
        f"{'WIN%':>7} {'PNL(pips)':>10} {'AVG_W_R':>8} {'AVG_L_R':>8} {'PF':>6}"
    )
    lines.append("-" * 115)

    total_trades = 0
    total_wins = 0
    total_pnl = 0.0

    for pair, r in sorted(results.items()):
        lines.append(
            f"  {pair:<10} {r.total_trades:>7} {r.winning_trades:>6} "
            f"{r.losing_trades:>7} {r.early_exits:>6} {r.win_rate:>6.1f}% "
            f"{r.total_pnl_pips:>10.1f} {r.avg_win_r:>8.2f} "
            f"{r.avg_loss_r:>8.2f} {r.profit_factor:>6.2f}"
        )
        total_trades += r.total_trades
        total_wins += r.winning_trades
        total_pnl += r.total_pnl_pips

    lines.append("-" * 115)
    overall_wr = total_wins / total_trades * 100 if total_trades > 0 else 0
    lines.append(
        f"  {'RAW TOTAL':<10} {total_trades:>7} {total_wins:>6} "
        f"{total_trades - total_wins:>7} {'':>6} {overall_wr:>6.1f}% {total_pnl:>10.1f}"
    )

    if params is not None:
        all_trades: List[Tuple[str, Trade]] = []
        for pair, result in results.items():
            for trade in result.trades:
                all_trades.append((pair, trade))
        all_trades.sort(key=lambda x: x[1].entry_time)
        filtered_trades = apply_correlation_filter(all_trades, params)
        filtered_wins = sum(1 for _, trade in filtered_trades if trade.pnl_pips > 0)
        filtered_pnl = sum(trade.pnl_pips for _, trade in filtered_trades)
        filtered_total = len(filtered_trades)
        filtered_wr = filtered_wins / filtered_total * 100 if filtered_total > 0 else 0
        lines.append(
            f"  {'FILTERED':<10} {filtered_total:>7} {filtered_wins:>6} "
            f"{filtered_total - filtered_wins:>7} {'':>6} {filtered_wr:>6.1f}% {filtered_pnl:>10.1f}"
        )
    lines.append("=" * 115)

    return "\n".join(lines)
