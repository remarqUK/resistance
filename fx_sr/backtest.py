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
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED

from .config import PAIRS, DEFAULT_ZONE_HISTORY_DAYS
from .execution import build_execution_plan, historical_execution_quote
from .levels import detect_zones, SRZone
from .portfolio import (
    ClosedTradeSummary,
    CorrelationExposure,
    PortfolioState,
    apply_correlation_policy,
    calculate_effective_risk_pct,
    is_pair_cooldown_active,
    update_streak_pause_state,
)
from .profiles import PROFILES
from .strategy import (
    Trade, StrategyParams, check_exit, get_market_exit_price,
    build_trade_from_signal, get_tradeable_zones, is_pair_fully_blocked, params_from_profile,
    select_entry_signal,
)
from .walkforward import (
    finalize_trade as shared_finalize_trade,
    run_walk_forward,
    slice_daily_window as shared_slice_daily_window,
)
from .serialization import (
    deserialize_timestamp as shared_deserialize_timestamp,
    serialize_timestamp as shared_serialize_timestamp,
    serialize_trade as shared_serialize_trade,
    serialize_zone as shared_serialize_zone,
)
from .db import load_backtest_result, save_backtest_result, load_l2_snapshots, load_ohlc
from . import ibkr
from .commission import compute_round_turn_commission
from .sizing import calculate_risk_amount


class BacktestCacheMissingError(RuntimeError):
    """Raised when cached market data is missing or insufficient for a backtest run."""


def _load_cached_data_window(
    ticker: str,
    interval: str,
    days: int,
    *,
    enforce_coverage: bool = True,
) -> pd.DataFrame:
    """Load a trailing interval window from cache and enforce strict coverage checks."""
    if days <= 0:
        return pd.DataFrame()

    now = pd.Timestamp.now(tz='UTC')
    start = now - pd.Timedelta(days=int(days))
    df = load_ohlc(
        ticker,
        interval,
        start=start.to_pydatetime(),
        end=now.to_pydatetime(),
    )
    if df.empty:
        raise BacktestCacheMissingError(
            f'No cached {interval} data for {ticker}; run `python run.py fill` first'
        )

    if not enforce_coverage:
        return df

    now = pd.Timestamp.now(tz='UTC')
    start = now - pd.Timedelta(days=int(days))
    target_days = int(days)
    if interval == '1d':
        trading_days = len(pd.bdate_range(start.normalize(), now.normalize(), freq='B'))
        window_rows = max(1, int(trading_days * 0.9))
    else:
        # FX hourly candles are typically 16 trading hours per day (weekdays),
        # so use the same calendar-to-trading-hour assumption as fill checks.
        window_rows = {
            '1h': max(1, int(target_days * 16 * 0.9)),
            '1m': max(1, int(target_days * 1000 * 0.9)),
        }.get(interval, max(1, int(target_days * 0.9)))

    if len(df) < window_rows:
        raise BacktestCacheMissingError(
            f'Cached {interval} data for {ticker} is too short '
            f'({len(df)} rows; expected at least {window_rows})'
        )

    start_tolerance = {
        '1d': pd.Timedelta(days=2),
        '1h': pd.Timedelta(hours=6),
    }.get(interval, pd.Timedelta(hours=1))
    stale_tolerance = {
        '1d': pd.Timedelta(days=3),
        '1h': pd.Timedelta(hours=3),
    }.get(interval, pd.Timedelta(hours=1))

    first_ts = pd.Timestamp(df.index.min())
    last_ts = pd.Timestamp(df.index.max())
    if first_ts > start + start_tolerance:
        raise BacktestCacheMissingError(
            f'Cached {interval} data for {ticker} starts too late for '
            f'{days}d backtest window ({first_ts} -> {last_ts})'
        )

    if last_ts < now - stale_tolerance:
        raise BacktestCacheMissingError(
            f'Cached {interval} data for {ticker} is stale ({last_ts} < {now - stale_tolerance})'
        )

    return df


def _load_cached_backtest_data(
    ticker: str,
    hourly_days: int,
    zone_history_days: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load strictly cached inputs for one backtest pair."""
    # Daily zones are evaluated with a rolling lookback (`zone_history_days`) at each
    # hourly bar, so we only need enough daily rows to cover the larger of the
    # execution horizon and zone-history horizon for strict cache checks.
    daily_days = max(1, int(max(hourly_days, zone_history_days)))
    daily_df = _load_cached_data_window(ticker, '1d', daily_days)
    hourly_df = _load_cached_data_window(ticker, '1h', int(hourly_days))
    minute_df = _load_cached_data_window(
        ticker,
        '1m',
        max(1, min(int(hourly_days), 7)),
        enforce_coverage=False,
    )
    return daily_df, hourly_df, minute_df


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
    pending_trades: List[Trade]
    zones: List[SRZone]


@dataclass(frozen=True)
class ExecutionAwareSkip:
    """One candidate trade rejected by the execution-aware portfolio simulator."""

    pair: str
    trade: Trade
    reason: str


@dataclass
class ExecutionAwarePortfolioResult:
    """Portfolio-level backtest result that mirrors live execution blocking."""

    trade_log: List[Tuple[str, Trade, float, float, float]]
    skipped: List[ExecutionAwareSkip]
    skip_counts: Dict[str, int]
    raw_total_trades: int
    raw_total_wins: int
    raw_total_pnl: float
    final_balance: float

    @property
    def total_trades(self) -> int:
        return len(self.trade_log)

    @property
    def total_wins(self) -> int:
        return sum(1 for _, trade, _, _, _ in self.trade_log if trade.pnl_pips > 0)

    @property
    def total_pnl(self) -> float:
        return float(sum(trade.pnl_pips for _, trade, _, _, _ in self.trade_log))

    @property
    def win_rate(self) -> float:
        return (self.total_wins / self.total_trades * 100.0) if self.total_trades else 0.0


@dataclass(frozen=True)
class _ActiveExecutionExposure:
    """Accepted trade tracked until its historical exit time."""

    pair: str
    trade: Trade
    risk_amount: float
    pnl_amount: float
    margin_required: float = 0.0


class _MarginTracker:
    """Track margin utilisation across concurrent backtest positions."""

    def __init__(self, starting_balance: float, account_currency: str = 'GBP') -> None:
        self.balance = starting_balance
        self.account_currency = account_currency
        self._active_margins: dict[str, float] = {}

    @property
    def total_margin_used(self) -> float:
        return sum(self._active_margins.values())

    @property
    def available_margin(self) -> float:
        return max(0.0, self.balance - self.total_margin_used)

    def add_position(self, key: str, margin_required: float) -> None:
        self._active_margins[key] = margin_required

    def remove_position(self, key: str) -> None:
        self._active_margins.pop(key, None)

    def sync_balance(self, new_balance: float) -> None:
        self.balance = new_balance


BACKTEST_CACHE_VERSION = '11'


def _serialize_timestamp(value: pd.Timestamp | None) -> str | None:
    return shared_serialize_timestamp(value)


def _deserialize_timestamp(value: str | None) -> pd.Timestamp | None:
    return shared_deserialize_timestamp(value)


def _trade_to_dict(trade: Trade) -> dict:
    return shared_serialize_trade(trade)


def _zone_to_dict(zone: SRZone) -> dict:
    return shared_serialize_zone(zone, include_seen=True)


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
        'pending_trades': [_trade_to_dict(t) for t in result.pending_trades],
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
                commission_cost=float(trade.get('commission_cost', 0.0)),
            )
        )

    pending_trades = []
    for trade in data.get('pending_trades', []):
        pending_trades.append(
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
                commission_cost=float(trade.get('commission_cost', 0.0)),
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
        pending_trades=pending_trades,
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


def _data_signature(
    daily_df: pd.DataFrame,
    hourly_df: pd.DataFrame,
    minute_df: pd.DataFrame | None = None,
) -> str:
    signature_payload = json.dumps({
        'daily': _normalize_df_for_signature(daily_df).to_json(orient='split', date_unit='ns'),
        'hourly': _normalize_df_for_signature(hourly_df).to_json(orient='split', date_unit='ns'),
        'minute': _normalize_df_for_signature(minute_df if minute_df is not None else pd.DataFrame()).to_json(
            orient='split',
            date_unit='ns',
        ),
    }, sort_keys=True)
    return hashlib.sha256(signature_payload.encode('utf-8')).hexdigest()


def _slice_daily_window(
    daily_df: pd.DataFrame,
    end_date,
    zone_history_days: int,
) -> pd.DataFrame:
    """Return a walk-forward daily window bounded by zone_history_days."""
    return shared_slice_daily_window(daily_df, end_date, zone_history_days)


def _finalize_trade(
    trade: Trade,
    exit_time,
    exit_price: float,
    exit_reason: str,
    bars_held: int,
    pip: float,
) -> Trade:
    """Populate final trade state and derived P&L metrics."""
    return shared_finalize_trade(trade, exit_time, exit_price, exit_reason, bars_held, pip)


def _build_pending_end_trade(
    hourly_df: pd.DataFrame,
    pair: str,
    params: StrategyParams,
    pip: float,
    zone_provider,
    execution_quote_provider,
    trades: List[Trade],
) -> Trade | None:
    """Materialize a final pending signal when submit-time data already exists.

    Live submits on the hour after bar ``T`` closes. For the last cached hourly
    bar, strict backtests can still reproduce that submission if minute/L2 data
    already contains a quote for ``T+1`` even though the next 1h candle has not
    been persisted yet.
    """

    if hourly_df.empty or execution_quote_provider is None:
        return None
    if not bool(params.strict_backtest_execution):
        return None

    last_time = pd.Timestamp(hourly_df.index[-1])
    if any(
        trade.exit_reason == 'END'
        and trade.exit_time is not None
        and pd.Timestamp(trade.exit_time) == last_time
        for trade in trades
    ):
        return None

    last_row = hourly_df.iloc[-1]
    current_date = last_time.date() if hasattr(last_time, 'date') else last_time
    current_zones = list(zone_provider(last_time, current_date, len(hourly_df) - 1) or [])
    nearest_support, nearest_resistance = get_tradeable_zones(current_zones, float(last_row['Close']))

    last_closed_trade = max(
        (
            trade for trade in trades
            if trade.exit_time is not None and pd.Timestamp(trade.exit_time) <= last_time
        ),
        key=lambda trade: pd.Timestamp(trade.exit_time),
        default=None,
    )
    if is_pair_cooldown_active(
        last_time,
        last_exit_time=(
            pd.Timestamp(last_closed_trade.exit_time)
            if last_closed_trade is not None and last_closed_trade.exit_time is not None
            else None
        ),
        last_pnl_r=(last_closed_trade.pnl_r if last_closed_trade is not None else None),
        params=params,
    ):
        return None

    signal = select_entry_signal(
        hourly_df=hourly_df,
        bar_idx=len(hourly_df) - 1,
        pair=pair,
        params=params,
        support_zone=nearest_support,
        resistance_zone=nearest_resistance,
    )
    if signal is None:
        return None

    submit_time = last_time + pd.Timedelta(hours=1)
    quote, _quote_note = execution_quote_provider(
        signal,
        submit_time,
        len(hourly_df),
        last_row,
    )
    if quote is None:
        return None

    execution_plan, _plan_note = build_execution_plan(
        signal,
        quote,
        params,
        now=submit_time,
    )
    if execution_plan is None:
        return None

    return build_trade_from_signal(
        signal,
        entry_price=execution_plan.entry_price,
        entry_time=execution_plan.quote.captured_at,
        sl_price=execution_plan.stop_price,
        tp_price=execution_plan.take_profit_price,
    )


def _load_minute_execution_data(
    ticker: str,
    hourly_days: int,
    *,
    force_refresh: bool = False,
    client_id: int | None = None,
) -> pd.DataFrame:
    """Load cached minute bars used for strict execution-parity entries."""
    end = pd.Timestamp.now(tz='UTC')
    start = end - pd.Timedelta(days=int(hourly_days))
    return load_ohlc(
        ticker,
        '1m',
        start=start.to_pydatetime(),
        end=end.to_pydatetime(),
    )


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
    minute_df: pd.DataFrame | None = None,
    l2_snapshots: pd.DataFrame | None = None,
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

    def zone_provider(current_time, current_date, _bar_index):
        bar_date = pd.Timestamp(current_date)
        if hasattr(current_time, 'tzinfo') and current_time.tzinfo:
            bar_date = bar_date.tz_localize(current_time.tzinfo)
        daily_window = _slice_daily_window(daily_df, bar_date, zone_history_days)
        if len(daily_window) < 20:
            return []
        return detect_zones(daily_window)

    def execution_quote_provider(signal, submit_time, _bar_index, row):
        return historical_execution_quote(
            signal.pair,
            submit_time,
            params,
            minute_df=minute_df,
            l2_snapshots=l2_snapshots,
            allow_h1_fallback=(
                bool(params.allow_h1_execution_fallback)
                and not bool(params.strict_backtest_execution)
            ),
            fallback_mid_price=float(row['Open']),
        )

    result = run_walk_forward(
        hourly_df,
        pair=pair,
        params=params,
        pip=pip,
        zone_provider=zone_provider,
        execution_quote_provider=execution_quote_provider,
        force_close_end=True,
        skip_execution_plan=not bool(params.strict_backtest_execution),
    )
    pending_trade = _build_pending_end_trade(
        hourly_df,
        pair,
        params,
        pip,
        zone_provider,
        execution_quote_provider,
        result.trades,
    )
    pending_trades = [pending_trade] if pending_trade is not None else []
    return _compile_results(pair, result.trades, result.zones, pending_trades=pending_trades)


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
    *,
    debug: bool = False,
) -> Tuple[str, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Fetch daily and hourly data for a pair without running the backtest."""
    ticker = pair_info['ticker']
    t0 = time.perf_counter()

    def _dbg(message: str) -> None:
        if debug:
            print(f'    [DEBUG] {message}')

    _dbg(f'phase1-worker: start pair={pair} ticker={ticker} client_id={client_id}')
    try:
        t_stage = time.perf_counter()
        _dbg(f'phase1-worker: pair={pair} daily cache load start')
        daily_df, hourly_df, minute_df = _load_cached_backtest_data(
            ticker,
            hourly_days,
            zone_history_days,
        )
        _dbg(
            f'phase1-worker: pair={pair} daily cache load complete rows={len(daily_df)} '
            f'elapsed={time.perf_counter() - t_stage:.2f}s'
        )

        t_stage = time.perf_counter()
        _dbg(f'phase1-worker: pair={pair} hourly cache load start')
        _dbg(
            f'phase1-worker: pair={pair} hourly cache load complete rows={len(hourly_df)} '
            f'elapsed={time.perf_counter() - t_stage:.2f}s'
        )

        t_stage = time.perf_counter()
        _dbg(f'phase1-worker: pair={pair} minute cache load start')
        _dbg(
            f'phase1-worker: pair={pair} minute cache load complete rows={len(minute_df)} '
            f'elapsed={time.perf_counter() - t_stage:.2f}s'
        )

        end = pd.Timestamp.now(tz='UTC')
        start = end - pd.Timedelta(days=int(hourly_days))
        t_stage = time.perf_counter()
        _dbg(f'phase1-worker: pair={pair} l2 snapshot query start')
        l2_snapshots = load_l2_snapshots(
            ticker,
            start=start.to_pydatetime(),
            end=end.to_pydatetime(),
        )
        _dbg(
            f'phase1-worker: pair={pair} l2 snapshot query complete rows={len(l2_snapshots)} '
            f'elapsed={time.perf_counter() - t_stage:.2f}s'
        )
        _dbg(
            f'phase1-worker: finished pair={pair} total_elapsed={time.perf_counter() - t0:.2f}s'
        )
        return pair, daily_df, hourly_df, minute_df, l2_snapshots
    except Exception as exc:
        _dbg(
            f'phase1-worker: error pair={pair} client_id={client_id} '
            f'elapsed={time.perf_counter() - t0:.2f}s err={type(exc).__name__}: {exc}'
        )
        raise


def run_backtest_fast(
    hourly_df: pd.DataFrame,
    pair: str,
    params: StrategyParams,
    zone_cache: Dict[tuple, List[SRZone]],
    pip: float,
    minute_df: pd.DataFrame | None = None,
    l2_snapshots: pd.DataFrame | None = None,
) -> BacktestResult:
    """Fast backtest using pre-computed zones (skips zone detection).

    Identical logic to run_backtest but looks up zones from zone_cache
    instead of calling detect_zones on each new day.
    """
    skip_exec = not bool(params.strict_backtest_execution)

    def zone_provider(_current_time, current_date, _bar_index):
        return zone_cache.get((pair, str(current_date)), [])

    def execution_quote_provider(signal, submit_time, _bar_index, row):
        return historical_execution_quote(
            signal.pair,
            submit_time,
            params,
            minute_df=minute_df,
            l2_snapshots=l2_snapshots,
            allow_h1_fallback=(
                bool(params.allow_h1_execution_fallback)
                and not bool(params.strict_backtest_execution)
            ),
            fallback_mid_price=float(row['Open']),
        )

    result = run_walk_forward(
        hourly_df,
        pair=pair,
        params=params,
        pip=pip,
        zone_provider=zone_provider,
        execution_quote_provider=execution_quote_provider,
        force_close_end=True,
        skip_execution_plan=skip_exec,
    )
    pending_trade = _build_pending_end_trade(
        hourly_df,
        pair,
        params,
        pip,
        zone_provider,
        execution_quote_provider,
        result.trades,
    )
    pending_trades = [pending_trade] if pending_trade is not None else []
    return _compile_results(pair, result.trades, result.zones, pending_trades=pending_trades)


def _compile_results(
    pair: str,
    trades: List[Trade],
    zones: List[SRZone],
    *,
    pending_trades: List[Trade] | None = None,
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
        pending_trades=list(pending_trades or []),
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

    daily_df, hourly_df, minute_df = _load_cached_backtest_data(
        pair_info['ticker'],
        hourly_days,
        zone_history_days,
    )
    end = pd.Timestamp.now(tz='UTC')
    start = end - pd.Timedelta(days=int(hourly_days))
    l2_snapshots = load_l2_snapshots(
        pair_info['ticker'],
        start=start.to_pydatetime(),
        end=end.to_pydatetime(),
    )
    if daily_df.empty or hourly_df.empty:
        return pair, None
    data_sig = _data_signature(daily_df, hourly_df, minute_df)

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

    result = run_backtest(
        daily_df,
        hourly_df,
        pair,
        params,
        zone_history_days,
        minute_df=minute_df,
        l2_snapshots=l2_snapshots,
    )
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


def _backtest_debug_enabled() -> bool:
    """Whether to emit verbose backtest execution diagnostics."""
    value = os.getenv('FX_SR_BACKTEST_DEBUG', '').strip().lower()
    return value in {'1', 'true', 'yes', 'on', 'debug'}


def run_all_backtests_parallel(
    params: StrategyParams = None,
    hourly_days: int = 30,
    zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS,
    pairs: Dict = None,
    force_refresh: bool = False,
    base_client_id: int | None = None,
    run_config_json: str | None = None,
    debug: bool = False,
    fetch_workers: int | None = None,
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
    debug = bool(debug) or _backtest_debug_enabled()

    def _debug(message: str) -> None:
        if debug:
            print(f'    [DEBUG] {message}')

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
            cid = _pair_client_id(base_client_id, offset)
            _debug(f'phase0: sequential run pair={pair} client_id={cid}')
            t_pair = time.perf_counter()
            pair, result = _backtest_pair(
                pair,
                info,
                params,
                hourly_days,
                zone_history_days,
                force_refresh,
                client_id=cid,
            )
            done += 1
            if result:
                results[pair] = result
                r = result
                print(f"    [{done}/{total}] {pair}: {r.total_trades} trades, "
                      f"{r.win_rate:.0f}% WR, {r.total_pnl_pips:+.0f} pips")
                _debug(f'phase0: completed pair={pair} in {time.perf_counter() - t_pair:.2f}s')
            else:
                print(f"    [{done}/{total}] {pair}: no data")
                _debug(f'phase0: pair={pair} returned no data')
        return results

    cpu_count = os.cpu_count() or 1
    # Use only as many workers as we have actual pair jobs; avoid oversubscription.
    # Windows also caps ProcessPoolExecutor at 61 workers (MAXIMUM_WAIT_OBJECTS - 3).
    max_pool_workers = min(cpu_count, total, 61)
    print(f"  Launching {total} backtests across {max_pool_workers} workers "
          f"(using cache when available{client_id_suffix})...")
    _debug(f'launch plan: cpu_count={os.cpu_count() or 1} max_pool_workers={max_pool_workers}')

    # --- Phase 1: Fetch data for all pairs concurrently ---
    t_phase = time.perf_counter()
    t_phase_wall = time.time()
    pair_data: Dict[str, tuple] = {}
    # Thread fan-out is bounded to avoid overwhelming shared DB/IBKR resources.
    if fetch_workers is not None:
        if fetch_workers < 1:
            raise ValueError('fetch_workers must be >= 1')
        fetch_workers = min(total, fetch_workers)
        _debug(f'phase1: fetch_workers override from CLI = {fetch_workers} total_pairs={total}')
    else:
        env_fetch_workers = os.getenv('FX_SR_FETCH_WORKERS')
        if env_fetch_workers is not None:
            try:
                fetch_workers = max(1, int(env_fetch_workers.strip()))
            except ValueError:
                _debug(
                    f'phase1: invalid FX_SR_FETCH_WORKERS={env_fetch_workers!r}, '
                    'falling back to automatic sizing'
                )
                fetch_workers = None
        else:
            fetch_workers = None
    if fetch_workers is None:
        fetch_workers = min(total, max_pool_workers, 20)
    _debug(f'phase1: fetch_workers={fetch_workers} total_pairs={total}')
    try:
        fetch_futures = {}
        fetch_started = {}
        with ThreadPoolExecutor(max_workers=fetch_workers) as executor:
            for offset, (pair, info) in enumerate(pair_items):
                cid = _pair_client_id(base_client_id, offset)
                _debug(f'phase1: submit fetch pair={pair} client_id={cid}')
                future = executor.submit(
                    _fetch_pair_data_only,
                    pair,
                    info,
                    hourly_days,
                    zone_history_days,
                    force_refresh,
                    cid,
                    debug=debug,
                )
                fetch_futures[future] = pair
                fetch_started[future] = time.perf_counter()

            pending = set(fetch_futures.keys())
            while pending:
                done_futures, pending = wait(
                    pending,
                    timeout=15,
                    return_when=FIRST_COMPLETED,
                )
                if not done_futures:
                    pending_pairs = [fetch_futures[f] for f in list(pending)[:3]]
                    _debug(
                        f'phase1: still waiting after {time.perf_counter() - t_phase:.1f}s; '
                        f'pending={len(pending)} (e.g. {pending_pairs})'
                    )
                    continue

                for future in done_futures:
                    pair = fetch_futures[future]
                    try:
                        pair, daily_df, hourly_df, minute_df, l2_snapshots = future.result()
                    except Exception as exc:
                        done += 1
                        if isinstance(exc, BacktestCacheMissingError):
                            raise BacktestCacheMissingError(f'{pair}: {exc}') from exc
                        print(f"    [{done}/{total}] {pair}: fetch failed ({type(exc).__name__}: {exc})")
                        _debug(
                            f'phase1: fetch failed pair={pair} '
                            f'elapsed={time.perf_counter() - fetch_started[future]:.2f}s err={type(exc).__name__}: {exc}'
                        )
                        continue

                    _debug(
                        f'phase1: fetch done pair={pair} '
                        f'rows={len(daily_df)}/{len(hourly_df)}/{len(minute_df)} '
                        f'elapsed={time.perf_counter() - fetch_started[future]:.2f}s'
                    )
                    if (
                        daily_df is not None
                        and not daily_df.empty
                        and hourly_df is not None
                        and not hourly_df.empty
                    ):
                        pair_data[pair] = (daily_df, hourly_df, minute_df, l2_snapshots)
                        _debug(f'phase1: pair_data stored pair={pair}')
                    else:
                        done += 1
                        print(f"    [{done}/{total}] {pair}: no data")
                        _debug(f'phase1: no usable data pair={pair}')
    except (OSError, ValueError):
        _debug('phase1: process pool unavailable; using sequential fetch fallback')
        for offset, (pair, info) in enumerate(pair_items):
            cid = _pair_client_id(base_client_id, offset)
            t_fetch = time.perf_counter()
            _debug(f'phase1: sequential fetch pair={pair} client_id={cid}')
            pair, daily_df, hourly_df, minute_df, l2_snapshots = _fetch_pair_data_only(
                pair, info, hourly_days, zone_history_days, force_refresh, cid,
                debug=debug,
            )
            if (daily_df is not None and not daily_df.empty
                    and hourly_df is not None and not hourly_df.empty):
                pair_data[pair] = (daily_df, hourly_df, minute_df, l2_snapshots)
            else:
                done += 1
                print(f"    [{done}/{total}] {pair}: no data")
                _debug(f'phase1: sequential no data pair={pair}')
            _debug(f'phase1: sequential fetch complete pair={pair} in {time.perf_counter() - t_fetch:.2f}s')

    if not pair_data:
        return results

    print(f"    Data fetched in {time.time() - t_phase_wall:.1f}s")

    # --- Phase 2: Check backtest result cache, identify cache misses ---
    params_hash = _params_signature(params)
    pairs_to_compute: Dict[str, tuple] = {}
    cache_hits = 0
    cache_misses = 0
    cache_t = time.perf_counter()
    for pair, (daily_df, hourly_df, minute_df, l2_snapshots) in pair_data.items():
        data_sig = _data_signature(daily_df, hourly_df, minute_df)
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
                    cache_hits += 1
                    _debug(f'phase2: cache HIT pair={pair}')
                    continue
                except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                    pass
            else:
                _debug(f'phase2: cache MISS pair={pair} strategy_version={strategy_version}')
        pairs_to_compute[pair] = (daily_df, hourly_df, minute_df, l2_snapshots, data_sig)
        cache_misses += 1

    if not pairs_to_compute:
        _debug(f'phase2: completed cache scan hits={cache_hits} misses={cache_misses} in {time.perf_counter() - cache_t:.2f}s')
        return results
    _debug(f'phase2: completed cache scan hits={cache_hits} misses={cache_misses} in {time.perf_counter() - cache_t:.2f}s')

    # --- Phase 3+4: Zone pre-computation + walk-forwards (single process pool) ---
    # Build zone computation tasks so we can saturate all cores.
    t_phase = time.time()
    # First pass: collect all dates per pair
    pair_dates: Dict[str, tuple] = {}
    total_zone_dates = 0
    for pair, (daily_df, hourly_df, _minute_df, _l2_snapshots, _) in pairs_to_compute.items():
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
        _debug(f'phase3: pair={pair} unique_dates={len(dates)}')

    # Second pass: chunk dates to create ~2x cpu_count tasks for load balancing
    zone_tasks: list = []
    dates_per_chunk = max(1, total_zone_dates // max(max_pool_workers * 2, 1))
    for pair, (daily_df, dates) in pair_dates.items():
        for i in range(0, len(dates), dates_per_chunk):
            zone_tasks.append((daily_df, pair, dates[i:i + dates_per_chunk], zone_history_days))
            if len(zone_tasks) <= 6:
                chunk = dates[i:i + dates_per_chunk]
                _debug(f'phase3: zone task pair={pair} chunk={chunk[0]}..{chunk[-1]} size={len(chunk)}')

    num_compute = len(pairs_to_compute)
    print(f"    {total_zone_dates} zone detections + {num_compute} walk-forwards "
          f"across {max_pool_workers} workers...")
    _debug(f'phase3: total_zone_dates={total_zone_dates}, zone_tasks={len(zone_tasks)}, dates_per_chunk={dates_per_chunk}')

    zone_cache: Dict[tuple, List[SRZone]] = {}
    try:
        with ProcessPoolExecutor(max_workers=max_pool_workers) as executor:
            # Submit all zone computation tasks
            zone_futures = []
            zone_started: Dict = {}
            for task in zone_tasks:
                fut = executor.submit(_detect_zones_for_dates, *task)
                zone_futures.append(fut)
                zone_started[fut] = time.perf_counter()
            _debug(f'phase3: submitted {len(zone_futures)} zone tasks')
            for future in as_completed(zone_futures):
                _debug(f'phase3: zone task complete in {time.perf_counter() - zone_started[future]:.2f}s')
                zone_cache.update(future.result())

            t_zones = time.time()
            print(f"    Zones computed in {t_zones - t_phase:.1f}s")

            # Submit walk-forward tasks (reuses the same pool - no extra startup)
            walk_futures = {}
            walk_started: Dict = {}
            per_pair_zone_cache: Dict[str, Dict[tuple, List[SRZone]]] = {}
            for (pair_key, date_str), zones in zone_cache.items():
                per_pair_zone_cache.setdefault(pair_key, {})[(pair_key, date_str)] = zones
            for pair, (daily_df, hourly_df, minute_df, l2_snapshots, data_sig) in pairs_to_compute.items():
                pip = pairs[pair].get('pip', 0.0001)
                pair_zones = per_pair_zone_cache.get(pair, {})
                fut = executor.submit(
                    run_backtest_fast, hourly_df, pair, params, pair_zones, pip, minute_df, l2_snapshots,
                )
                walk_futures[fut] = (pair, data_sig)
                walk_started[fut] = time.perf_counter()
            _debug(f'phase4: submitted {len(walk_futures)} walk-forward tasks')
            for future in as_completed(walk_futures):
                pair, data_sig = walk_futures[future]
                result = future.result()
                results[pair] = result
                done += 1
                r = result
                print(f"    [{done}/{total}] {pair}: {r.total_trades} trades, "
                      f"{r.win_rate:.0f}% WR, {r.total_pnl_pips:+.0f} pips")
                _debug(f'phase4: walk task complete pair={pair} in {time.perf_counter() - walk_started[future]:.2f}s')
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
        _debug('phase3/4: process pool unavailable; falling back to sequential.')
        for task in zone_tasks:
            zone_cache.update(_detect_zones_for_dates(*task))
        for pair, (daily_df, hourly_df, minute_df, l2_snapshots, data_sig) in pairs_to_compute.items():
            pip = pairs[pair].get('pip', 0.0001)
            t_walk = time.perf_counter()
            pair_zones = {
                (k[0], k[1]): v
                for k, v in zone_cache.items()
                if k[0] == pair
            }
            result = run_backtest_fast(hourly_df, pair, params, pair_zones, pip, minute_df, l2_snapshots)
            results[pair] = result
            done += 1
            r = result
            print(f"    [{done}/{total}] {pair}: {r.total_trades} trades, "
                  f"{r.win_rate:.0f}% WR, {r.total_pnl_pips:+.0f} pips")
            _debug(f'phase4: sequential fallback complete pair={pair} in {time.perf_counter() - t_walk:.2f}s')
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

        exposures = [
            CorrelationExposure(
                pair=active_pair,
                quality_score=active_trade.quality_score,
                replaceable=True,
                payload=(active_pair, active_trade),
            )
            for active_pair, active_trade in active
        ]
        allowed, replaced = apply_correlation_policy(
            exposures,
            candidate_pair=pair_id,
            candidate_quality=trade.quality_score,
            params=params,
        )
        if not allowed:
            continue
        if replaced is not None:
            removed_pair, removed_trade = replaced.payload
            filtered = [
                (p, t) for p, t in filtered
                if not (p == removed_pair and t is removed_trade)
            ]
            active = [
                (p, t) for p, t in active
                if not (p == removed_pair and t is removed_trade)
            ]

        filtered.append((pair_id, trade))
        active.append((pair_id, trade))

    return filtered


def _record_execution_skip(
    skipped: List[ExecutionAwareSkip],
    skip_counts: Dict[str, int],
    pair: str,
    trade: Trade,
    reason: str,
) -> None:
    """Append one rejected candidate trade with its reason code."""

    skipped.append(ExecutionAwareSkip(pair=pair, trade=trade, reason=reason))
    skip_counts[reason] = skip_counts.get(reason, 0) + 1


def _settle_execution_exposures(
    active: List[_ActiveExecutionExposure],
    *,
    entry_time: pd.Timestamp,
    state: PortfolioState,
    margin_tracker: Optional[_MarginTracker] = None,
) -> List[_ActiveExecutionExposure]:
    """Apply closed accepted trades to the portfolio state before the next batch."""

    remaining: list[_ActiveExecutionExposure] = []
    closed: list[_ActiveExecutionExposure] = []
    for exposure in active:
        if exposure.trade.exit_time is not None and exposure.trade.exit_time <= entry_time:
            closed.append(exposure)
        else:
            remaining.append(exposure)

    closed.sort(
        key=lambda exposure: (
            exposure.trade.exit_time,
            exposure.trade.entry_time,
            exposure.pair,
        )
    )
    for exposure in closed:
        state.record_closed_trade(
            ClosedTradeSummary(
                pair=exposure.pair,
                entry_time=exposure.trade.entry_time,
                exit_time=exposure.trade.exit_time,
                pnl_r=exposure.trade.pnl_r,
                quality_score=exposure.trade.quality_score,
                risk_amount=exposure.risk_amount,
                pnl_amount=exposure.pnl_amount,
            )
        )
        if margin_tracker is not None:
            key = f"{exposure.pair}_{id(exposure.trade)}"
            margin_tracker.remove_position(key)
            if state.balance is not None:
                margin_tracker.sync_balance(state.balance)
    return remaining


def _compute_trade_commission(
    pair: str,
    est_units: int,
    entry_price: float,
    account_currency: str,
    price_lookup,
    commission_bps: float,
    commission_min_usd: float,
) -> float:
    """Compute round-turn commission in account currency for the portfolio sim."""
    cost = compute_round_turn_commission(
        units=est_units,
        entry_price=entry_price,
        pair=pair,
        account_currency=account_currency,
        price_lookup=price_lookup,
        commission_bps=commission_bps,
        commission_min_usd=commission_min_usd,
    )
    return float(cost) if cost is not None else 0.0


def calculate_execution_aware_compounding_pnl(
    results: Dict[str, BacktestResult],
    starting_balance: float = 1000.0,
    risk_pct: float = 0.05,
    params: StrategyParams = None,
    account_currency: str = 'GBP',
) -> ExecutionAwarePortfolioResult:
    """Run the live-style portfolio admission funnel over historical trades."""

    from .margin import compute_margin_requirement, check_margin_available

    if params is None:
        params = StrategyParams()

    candidates: list[tuple[str, Trade]] = []
    raw_total_trades = 0
    raw_total_wins = 0
    raw_total_pnl = 0.0
    for pair, result in results.items():
        raw_total_trades += int(result.total_trades)
        raw_total_wins += int(result.winning_trades)
        raw_total_pnl += float(result.total_pnl_pips)
        for trade in result.trades:
            candidates.append((pair, trade))

    candidates.sort(key=lambda item: (item[1].entry_time, item[0]))
    trade_log: list[tuple[str, Trade, float, float, float]] = []
    skipped: list[ExecutionAwareSkip] = []
    skip_counts: dict[str, int] = {}
    report_balance = float(starting_balance)
    state = PortfolioState(
        params=params,
        balance=float(starting_balance),
        peak_balance=float(starting_balance),
    )
    active: list[_ActiveExecutionExposure] = []
    margin_tracker = _MarginTracker(float(starting_balance), account_currency) if params.enforce_margin else None

    idx = 0
    while idx < len(candidates):
        batch_time = candidates[idx][1].entry_time
        active = _settle_execution_exposures(active, entry_time=batch_time, state=state, margin_tracker=margin_tracker)

        current_balance = (
            float(state.balance)
            if state.balance is not None
            else float(starting_balance)
        )
        slot_risk_amount = calculate_risk_amount(current_balance, risk_pct)
        active_reserved_risk = sum(exposure.risk_amount for exposure in active)
        correlation_cap = max(int(params.max_correlated_trades), 1)
        max_total_risk = (
            float(slot_risk_amount) * correlation_cap
            if params.use_correlation_filter
            else None
        )

        batch: list[tuple[str, Trade]] = []
        while idx < len(candidates) and candidates[idx][1].entry_time == batch_time:
            batch.append(candidates[idx])
            idx += 1

        exposures: list[CorrelationExposure] = [
            CorrelationExposure(
                pair=exposure.pair,
                quality_score=float(exposure.trade.quality_score),
                replaceable=False,
                payload=exposure.pair,
            )
            for exposure in active
        ]
        planned: dict[int, tuple[str, Trade, float]] = {}
        planned_reserved_risk = 0.0

        for batch_idx, (pair_id, trade) in enumerate(batch):
            block = state.entry_block(pair_id, batch_time)
            if block is not None:
                _record_execution_skip(skipped, skip_counts, pair_id, trade, block[0])
                continue

            effective_risk = calculate_effective_risk_pct(
                risk_pct,
                params=params,
                balance=current_balance,
                peak_balance=state.peak_balance,
                quality_score=trade.quality_score,
            )
            risk_amount = calculate_risk_amount(current_balance, effective_risk)

            same_pair_nonreplaceable = any(
                exposure.pair == pair_id and not exposure.replaceable
                for exposure in exposures
            )
            if same_pair_nonreplaceable:
                _record_execution_skip(skipped, skip_counts, pair_id, trade, 'POSITION_EXISTS')
                continue

            replace_candidates = {
                exposure.payload: exposure
                for exposure in exposures
                if exposure.pair == pair_id and exposure.replaceable
            }
            if replace_candidates:
                worst_same_pair = min(
                    replace_candidates.values(),
                    key=lambda exposure: exposure.quality_score,
                )
                if float(trade.quality_score) > float(worst_same_pair.quality_score):
                    candidate_exposures = [
                        exposure
                        for exposure in exposures
                        if exposure.payload != worst_same_pair.payload
                    ]
                else:
                    _record_execution_skip(
                        skipped,
                        skip_counts,
                        pair_id,
                        trade,
                        'DUPLICATE_PAIR_SIGNAL',
                    )
                    continue
            else:
                worst_same_pair = None
                candidate_exposures = list(exposures)

            allowed, replaced_corr = apply_correlation_policy(
                candidate_exposures,
                candidate_pair=pair_id,
                candidate_quality=trade.quality_score,
                params=params,
            )
            if not allowed:
                _record_execution_skip(skipped, skip_counts, pair_id, trade, 'CORRELATION_CAP')
                continue

            replaced_exposures: dict[object, CorrelationExposure] = {}
            for exposure in (worst_same_pair, replaced_corr):
                if exposure is not None:
                    replaced_exposures[exposure.payload] = exposure

            candidate_reserved_risk = planned_reserved_risk + float(risk_amount)
            for payload in replaced_exposures:
                prior = planned.get(payload)
                if prior is not None:
                    candidate_reserved_risk -= float(prior[2])

            if (
                max_total_risk is not None
                and active_reserved_risk + candidate_reserved_risk > max_total_risk + 1e-9
            ):
                _record_execution_skip(skipped, skip_counts, pair_id, trade, 'RISK_BUDGET_FULL')
                continue

            # Estimate units from risk amount and stop distance
            stop_dist = abs(trade.entry_price - trade.sl_price)
            est_units = int(risk_amount / stop_dist) if stop_dist > 0 else 0

            # Margin and minimum-size checks
            trade_margin = 0.0
            if margin_tracker is not None:
                if est_units < params.min_order_units:
                    _record_execution_skip(skipped, skip_counts, pair_id, trade, 'BELOW_MIN_SIZE')
                    continue

                # Build a simple price lookup from the batch entry prices
                batch_prices = {p: t.entry_price for p, t in batch}
                margin_req = compute_margin_requirement(
                    pair_id, est_units, trade.entry_price,
                    account_currency, lambda pid: batch_prices.get(pid),
                )
                if margin_req is not None:
                    allowed_margin, _ = check_margin_available(
                        margin_req.margin_required,
                        margin_tracker.available_margin,
                        params.margin_cushion_pct,
                    )
                    if not allowed_margin:
                        _record_execution_skip(skipped, skip_counts, pair_id, trade, 'MARGIN_INSUFFICIENT')
                        continue
                    trade_margin = margin_req.margin_required

            for payload in replaced_exposures:
                prior = planned.pop(payload, None)
                if prior is None:
                    continue
                planned_reserved_risk -= float(prior[2])
                _record_execution_skip(
                    skipped,
                    skip_counts,
                    prior[0],
                    prior[1],
                    'REPLACED_BY_HIGHER_QUALITY',
                )
                exposures = [
                    exposure
                    for exposure in exposures
                    if exposure.payload != payload
                ]

            planned[batch_idx] = (pair_id, trade, float(risk_amount), trade_margin, est_units)
            planned_reserved_risk += float(risk_amount)
            exposures.append(
                CorrelationExposure(
                    pair=pair_id,
                    quality_score=float(trade.quality_score),
                    replaceable=True,
                    payload=batch_idx,
                )
            )

        for batch_idx in sorted(planned):
            pair_id, trade, risk_amount, trade_margin, est_units = planned[batch_idx]

            # Compute commission and deduct from P&L
            commission_cost = 0.0
            if est_units > 0 and params.commission_bps > 0:
                batch_prices = {p: t.entry_price for p, t in batch}
                commission_cost = _compute_trade_commission(
                    pair_id, est_units, trade.entry_price,
                    account_currency, lambda pid, _bp=batch_prices: _bp.get(pid),
                    params.commission_bps, params.commission_min_usd,
                )
            trade.commission_cost = commission_cost
            pnl_amount = float(risk_amount) * float(trade.pnl_r) - commission_cost
            report_balance += pnl_amount
            trade_log.append((pair_id, trade, float(risk_amount), pnl_amount, report_balance))
            exposure = _ActiveExecutionExposure(
                pair=pair_id,
                trade=trade,
                risk_amount=float(risk_amount),
                pnl_amount=pnl_amount,
                margin_required=trade_margin,
            )
            active.append(exposure)
            if margin_tracker is not None and trade_margin > 0:
                key = f"{pair_id}_{id(trade)}"
                margin_tracker.add_position(key, trade_margin)
                margin_tracker.sync_balance(report_balance)

    return ExecutionAwarePortfolioResult(
        trade_log=trade_log,
        skipped=skipped,
        skip_counts=dict(sorted(skip_counts.items())),
        raw_total_trades=raw_total_trades,
        raw_total_wins=raw_total_wins,
        raw_total_pnl=raw_total_pnl,
        final_balance=report_balance,
    )


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

        effective_risk = calculate_effective_risk_pct(
            risk_pct,
            params=params,
            balance=balance,
            peak_balance=peak_balance,
            quality_score=t.quality_score,
        )

        risk_amt = calculate_risk_amount(balance, effective_risk)
        pnl = risk_amt * t.pnl_r
        balance += pnl
        if balance > peak_balance:
            peak_balance = balance
        trade_log.append((pair, t, risk_amt, pnl, balance))

        consecutive_losses, pause_until = update_streak_pause_state(
            consecutive_losses,
            pause_until,
            pnl_r=t.pnl_r,
            exit_time=t.exit_time,
            params=params,
        )

    return trade_log, balance


def format_compounding_results(
    trade_log: List[Tuple[str, Trade, float, float, float]],
    starting_balance: float,
    final_balance: float,
    total_pre_filter: int,
    *,
    title: str = "COMPOUNDING P&L REPORT",
    filter_note: str = "filtered from {total_pre_filter} by correlation",
    skip_counts: Dict[str, int] | None = None,
) -> str:
    """Format compounding P&L results as a readable report."""
    lines = []
    lines.append("=" * 130)
    lines.append(f"  {title}")
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
    lines.append(
        f"  Total trades:         {len(trade_log)} "
        f"({filter_note.format(total_pre_filter=total_pre_filter)})"
    )
    lines.append(f"  Wins: {len(wins)}  Losses: {len(losses)}  "
                 f"Win rate: {len(wins)/len(trade_log)*100:.1f}%" if trade_log else "")
    lines.append(f"  Avg win: {avg_win:+.2f}R  Avg loss: {avg_loss:+.2f}R")
    lines.append(f"  Peak balance:         GBP {peak:,.2f}")
    lines.append(f"  Max drawdown:         {max_dd:.1f}%")
    lines.append(f"  Max losing streak:    {max_losing_streak} trades")
    lines.append(f"  Exit types:           {dict(sorted(exit_counts.items()))}")
    if skip_counts:
        lines.append(f"  Skip reasons:         {dict(sorted(skip_counts.items()))}")
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
