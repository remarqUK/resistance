"""Backtesting engine: daily zones + hourly execution.

Strategy flow:
1. Detect S/R zones from daily chart data
2. Walk forward through 1-hour bars
3. When price enters a zone on 1H, look for reversal candle -> enter
4. Manage trade: early exit on zone break, hold winners to TP
5. Re-detect zones when new daily bars form
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import PAIRS, DEFAULT_ZONE_HISTORY_DAYS
from .data import fetch_daily_data, fetch_hourly_data
from .levels import detect_zones, get_nearest_zones, SRZone
from .strategy import (
    Trade, StrategyParams, Signal, generate_signal, check_exit,
    check_momentum_filter, get_correlated_pairs, get_market_exit_price,
    BLOCKED_PAIR_DIRECTIONS,
)


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
                current_price = float(row['Close'])
                nearest_support, nearest_resistance = get_nearest_zones(
                    current_zones, current_price, major_only=True
                )
            last_zone_date = current_date

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
                trades.append(
                    _finalize_trade(current_trade, current_time, exit_price, exit_reason, bars_held, pip)
                )
                last_trade_bar = i
                current_trade = None
                continue

        # Check for new entry if flat and cooldown elapsed
        if current_trade is None and (i - last_trade_bar) >= params.cooldown_bars:
            # Time filters: skip bad hours and days
            if params.use_time_filters:
                entry_hour = current_time.hour if hasattr(current_time, 'hour') else 0
                entry_weekday = current_time.weekday() if hasattr(current_time, 'weekday') else 0
                if entry_hour in params.blocked_hours or entry_weekday in params.blocked_days:
                    continue

            # Try support zone (with momentum filter)
            if nearest_support:
                # Skip if strong momentum into zone
                if not check_momentum_filter(hourly_df, i, nearest_support, params):
                    signal = generate_signal(
                        bar_open=row['Open'],
                        bar_close=row['Close'],
                        bar_high=row['High'],
                        bar_low=row['Low'],
                        zone=nearest_support,
                        pair=pair,
                        time=current_time,
                        params=params,
                    )
                    if signal:
                        # Pair+direction filter
                        if params.use_pair_direction_filter and \
                                (pair, signal.direction) in BLOCKED_PAIR_DIRECTIONS:
                            signal = None
                    if signal:
                        risk = signal.entry_price - signal.sl_price
                        current_trade = Trade(
                            entry_time=signal.time,
                            entry_price=signal.entry_price,
                            direction=signal.direction,
                            sl_price=signal.sl_price,
                            tp_price=signal.tp_price,
                            zone_upper=signal.zone_upper,
                            zone_lower=signal.zone_lower,
                            zone_strength=signal.zone_strength,
                            risk=risk,
                        )
                        trade_entry_bar = i
                        continue

            # Try resistance zone (with momentum filter)
            if nearest_resistance:
                if not check_momentum_filter(hourly_df, i, nearest_resistance, params):
                    signal = generate_signal(
                        bar_open=row['Open'],
                        bar_close=row['Close'],
                        bar_high=row['High'],
                        bar_low=row['Low'],
                        zone=nearest_resistance,
                        pair=pair,
                        time=current_time,
                        params=params,
                    )
                    if signal:
                        # Pair+direction filter
                        if params.use_pair_direction_filter and \
                                (pair, signal.direction) in BLOCKED_PAIR_DIRECTIONS:
                            signal = None
                    if signal:
                        risk = signal.sl_price - signal.entry_price
                        current_trade = Trade(
                            entry_time=signal.time,
                            entry_price=signal.entry_price,
                            direction=signal.direction,
                            sl_price=signal.sl_price,
                            tp_price=signal.tp_price,
                            zone_upper=signal.zone_upper,
                            zone_lower=signal.zone_lower,
                            zone_strength=signal.zone_strength,
                            risk=risk,
                        )
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
                current_price = float(row['Close'])
                nearest_support, nearest_resistance = get_nearest_zones(
                    current_zones, current_price, major_only=True
                )
            last_zone_date = current_date

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
                trades.append(
                    _finalize_trade(current_trade, current_time, exit_price, exit_reason, bars_held, pip)
                )
                last_trade_bar = i
                current_trade = None
                continue

        # Check for new entry if flat and cooldown elapsed
        if current_trade is None and (i - last_trade_bar) >= params.cooldown_bars:
            if params.use_time_filters:
                entry_hour = current_time.hour if hasattr(current_time, 'hour') else 0
                entry_weekday = current_time.weekday() if hasattr(current_time, 'weekday') else 0
                if entry_hour in params.blocked_hours or entry_weekday in params.blocked_days:
                    continue

            if nearest_support:
                if not check_momentum_filter(hourly_df, i, nearest_support, params):
                    signal = generate_signal(
                        bar_open=row['Open'], bar_close=row['Close'],
                        bar_high=row['High'], bar_low=row['Low'],
                        zone=nearest_support, pair=pair,
                        time=current_time, params=params,
                    )
                    if signal:
                        if params.use_pair_direction_filter and \
                                (pair, signal.direction) in BLOCKED_PAIR_DIRECTIONS:
                            signal = None
                    if signal:
                        risk = signal.entry_price - signal.sl_price
                        current_trade = Trade(
                            entry_time=signal.time, entry_price=signal.entry_price,
                            direction=signal.direction, sl_price=signal.sl_price,
                            tp_price=signal.tp_price, zone_upper=signal.zone_upper,
                            zone_lower=signal.zone_lower, zone_strength=signal.zone_strength,
                            risk=risk,
                        )
                        trade_entry_bar = i
                        continue

            if nearest_resistance:
                if not check_momentum_filter(hourly_df, i, nearest_resistance, params):
                    signal = generate_signal(
                        bar_open=row['Open'], bar_close=row['Close'],
                        bar_high=row['High'], bar_low=row['Low'],
                        zone=nearest_resistance, pair=pair,
                        time=current_time, params=params,
                    )
                    if signal:
                        if params.use_pair_direction_filter and \
                                (pair, signal.direction) in BLOCKED_PAIR_DIRECTIONS:
                            signal = None
                    if signal:
                        risk = signal.sl_price - signal.entry_price
                        current_trade = Trade(
                            entry_time=signal.time, entry_price=signal.entry_price,
                            direction=signal.direction, sl_price=signal.sl_price,
                            tp_price=signal.tp_price, zone_upper=signal.zone_upper,
                            zone_lower=signal.zone_lower, zone_strength=signal.zone_strength,
                            risk=risk,
                        )
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
    for pair in daily_data:
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
) -> Tuple[str, Optional[BacktestResult]]:
    """Fetch data and run backtest for a single pair. Thread-safe."""
    daily_df = fetch_daily_data(
        pair_info['ticker'],
        days=zone_history_days + hourly_days,
        force_refresh=force_refresh,
        allow_stale_cache=not force_refresh,
    )
    hourly_df = fetch_hourly_data(
        pair_info['ticker'],
        days=hourly_days,
        force_refresh=force_refresh,
        allow_stale_cache=not force_refresh,
    )
    if daily_df.empty or hourly_df.empty:
        return pair, None
    result = run_backtest(daily_df, hourly_df, pair, params, zone_history_days)
    return pair, result


def run_all_backtests_parallel(
    params: StrategyParams = None,
    hourly_days: int = 30,
    zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS,
    pairs: Dict = None,
    force_refresh: bool = False,
) -> Dict[str, BacktestResult]:
    """Run all pair backtests.

    Cached runs execute in parallel for speed. Forced refresh runs execute
    sequentially so IBKR pacing limits are respected.
    """
    if params is None:
        params = StrategyParams()
    if pairs is None:
        pairs = PAIRS

    results = {}
    total = len(pairs)
    done = 0

    if force_refresh:
        print(f"  Refreshing {total} backtests from IBKR/TWS sequentially...")
        for pair, info in pairs.items():
            pair, result = _backtest_pair(
                pair, info, params, hourly_days, zone_history_days, force_refresh
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

    print(f"  Launching {total} backtests in parallel (using cache when available)...")

    with ThreadPoolExecutor(max_workers=total) as executor:
        futures = {
            executor.submit(
                _backtest_pair, pair, info, params, hourly_days,
                zone_history_days, force_refresh,
            ): pair
            for pair, info in pairs.items()
        }
        for future in as_completed(futures):
            pair, result = future.result()
            done += 1
            if result:
                results[pair] = result
                r = result
                print(f"    [{done}/{total}] {pair}: {r.total_trades} trades, "
                      f"{r.win_rate:.0f}% WR, {r.total_pnl_pips:+.0f} pips")
            else:
                print(f"    [{done}/{total}] {pair}: no data")

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
            continue

        filtered.append((pair_id, trade))
        active.append((pair_id, trade))

    return filtered


def calculate_compounding_pnl(
    results: Dict[str, BacktestResult],
    starting_balance: float = 10000.0,
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

    # Compound
    balance = starting_balance
    trade_log = []
    for pair, t in filtered:
        risk_amt = balance * risk_pct
        pnl = risk_amt * t.pnl_r
        balance += pnl
        trade_log.append((pair, t, risk_amt, pnl, balance))

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


def format_results(results: Dict[str, BacktestResult]) -> str:
    """Format backtest results as a readable summary table."""
    lines = []
    lines.append("=" * 115)
    lines.append("  BACKTEST RESULTS - Daily Zone S/R Strategy (configurable R:R, early exit active)")
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
        f"  {'TOTAL':<10} {total_trades:>7} {total_wins:>6} "
        f"{total_trades - total_wins:>7} {'':>6} {overall_wr:>6.1f}% {total_pnl:>10.1f}"
    )
    lines.append("=" * 115)

    return "\n".join(lines)
