"""Replay engine: walk through a single day's hourly bars with full strategy state.

Mirrors the backtest loop but yields per-bar frames for progressive
visualization in the browser.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from aiohttp import web

from .backtest import _slice_daily_window, _finalize_trade
from .config import PAIRS, STRATEGY_PRESETS, DEFAULT_ZONE_HISTORY_DAYS
from . import db
from .data import fetch_daily_data, fetch_hourly_data, fetch_minute_data_cached
from .levels import detect_zones, get_nearest_zones, SRZone
from .strategy import (
    Trade, StrategyParams, generate_signal, check_exit,
    check_momentum_filter, BLOCKED_PAIR_DIRECTIONS,
)


WEB_DIR = Path(__file__).resolve().parent / 'web_live'


def _zone_to_dict(zone: SRZone) -> dict:
    return {
        'upper': zone.upper,
        'lower': zone.lower,
        'midpoint': zone.midpoint,
        'touches': zone.touches,
        'zone_type': zone.zone_type,
        'strength': zone.strength,
    }


def _trade_to_dict(trade: Trade, pip: float) -> dict:
    d = {
        'entry_time': str(trade.entry_time),
        'entry_price': trade.entry_price,
        'direction': trade.direction,
        'sl_price': trade.sl_price,
        'tp_price': trade.tp_price,
        'zone_upper': trade.zone_upper,
        'zone_lower': trade.zone_lower,
    }
    if trade.exit_time is not None:
        d.update({
            'exit_time': str(trade.exit_time),
            'exit_price': trade.exit_price,
            'exit_reason': trade.exit_reason,
            'pnl_pips': round(trade.pnl_pips, 1),
            'pnl_r': round(trade.pnl_r, 2),
            'bars_held': trade.bars_held,
        })
    return d


def generate_replay_frames(
    daily_df: pd.DataFrame,
    hourly_df: pd.DataFrame,
    pair: str,
    target_date: date,
    params: StrategyParams | None = None,
    zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS,
) -> dict:
    """Walk the full hourly history, emitting frames only for *target_date*.

    Returns ``{"frames": [...], "zones": [...], "summary": {...}}``.
    """
    if params is None:
        params = StrategyParams()

    pair_info = PAIRS.get(pair, {})
    pip = pair_info.get('pip', 0.0001)
    decimals = pair_info.get('decimals', 5)

    # --- walk-forward state (identical to run_backtest) ---
    current_trade: Optional[Trade] = None
    current_zones: list[SRZone] = []
    nearest_support: Optional[SRZone] = None
    nearest_resistance: Optional[SRZone] = None
    last_trade_bar = -params.cooldown_bars
    last_zone_date = None
    trade_entry_bar = 0

    frames: list[dict] = []
    context_bars: list[dict] = []
    completed_trades: list[dict] = []
    day_zones: list[dict] = []
    bar_index_in_day = 0

    for i in range(len(hourly_df)):
        row = hourly_df.iloc[i]
        current_time = hourly_df.index[i]
        current_date = current_time.date() if hasattr(current_time, 'date') else current_time

        # Re-detect zones on new day
        bar_date = pd.Timestamp(current_date)
        if hasattr(current_time, 'tzinfo') and current_time.tzinfo:
            bar_date = bar_date.tz_localize(current_time.tzinfo)

        if last_zone_date is None or str(current_date) != str(last_zone_date):
            daily_window = _slice_daily_window(daily_df, bar_date, zone_history_days)
            if len(daily_window) >= 20:
                current_zones = detect_zones(daily_window)
                current_price = float(row['Close'])
                nearest_support, nearest_resistance = get_nearest_zones(
                    current_zones, current_price, major_only=True,
                )
            last_zone_date = current_date

        # --- capture zones once we reach the target day ---
        is_target = str(current_date) == str(target_date)

        # Collect pre-target bars as context
        if not is_target:
            context_bars.append({
                'time': str(current_time),
                'open': round(float(row['Open']), decimals),
                'high': round(float(row['High']), decimals),
                'low': round(float(row['Low']), decimals),
                'close': round(float(row['Close']), decimals),
            })

        if is_target and not day_zones and current_zones:
            day_zones = [_zone_to_dict(z) for z in current_zones]

        # --- check exit ---
        exit_event = None
        signal_event = None

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
                finished = _finalize_trade(
                    current_trade, current_time, exit_price, exit_reason, bars_held, pip,
                )
                exit_event = {
                    'reason': finished.exit_reason,
                    'price': finished.exit_price,
                    'pnl_pips': round(finished.pnl_pips, 1),
                    'pnl_r': round(finished.pnl_r, 2),
                }
                completed_trades.append(_trade_to_dict(finished, pip))
                last_trade_bar = i
                current_trade = None

                if is_target:
                    frames.append(_build_frame(
                        bar_index_in_day, current_time, row,
                        current_zones, nearest_support, nearest_resistance,
                        signal_event, exit_event, None, list(completed_trades),
                        decimals,
                    ))
                    bar_index_in_day += 1
                continue

        # --- check entry ---
        if current_trade is None and (i - last_trade_bar) >= params.cooldown_bars:
            if params.use_time_filters:
                entry_hour = current_time.hour if hasattr(current_time, 'hour') else 0
                entry_weekday = current_time.weekday() if hasattr(current_time, 'weekday') else 0
                if entry_hour in params.blocked_hours or entry_weekday in params.blocked_days:
                    if is_target:
                        frames.append(_build_frame(
                            bar_index_in_day, current_time, row,
                            current_zones, nearest_support, nearest_resistance,
                            None, None, None, list(completed_trades),
                            decimals,
                        ))
                        bar_index_in_day += 1
                    continue

            signal = None
            # Try support
            if nearest_support:
                if not check_momentum_filter(hourly_df, i, nearest_support, params):
                    signal = generate_signal(
                        bar_open=row['Open'], bar_close=row['Close'],
                        bar_high=row['High'], bar_low=row['Low'],
                        zone=nearest_support, pair=pair,
                        time=current_time, params=params,
                    )
                    if signal and params.use_pair_direction_filter and \
                            (pair, signal.direction) in BLOCKED_PAIR_DIRECTIONS:
                        signal = None

            # Try resistance if no support signal
            if signal is None and nearest_resistance:
                if not check_momentum_filter(hourly_df, i, nearest_resistance, params):
                    signal = generate_signal(
                        bar_open=row['Open'], bar_close=row['Close'],
                        bar_high=row['High'], bar_low=row['Low'],
                        zone=nearest_resistance, pair=pair,
                        time=current_time, params=params,
                    )
                    if signal and params.use_pair_direction_filter and \
                            (pair, signal.direction) in BLOCKED_PAIR_DIRECTIONS:
                        signal = None

            if signal:
                if signal.direction == 'LONG':
                    risk = signal.entry_price - signal.sl_price
                else:
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
                signal_event = {
                    'direction': signal.direction,
                    'entry_price': signal.entry_price,
                    'sl_price': signal.sl_price,
                    'tp_price': signal.tp_price,
                    'zone_type': signal.zone_type,
                }

        # --- emit frame for target day ---
        if is_target:
            open_trade = None
            if current_trade is not None:
                bars_held = i - trade_entry_bar
                open_trade = {
                    'entry_time': str(current_trade.entry_time),
                    'entry_price': current_trade.entry_price,
                    'direction': current_trade.direction,
                    'sl_price': current_trade.sl_price,
                    'tp_price': current_trade.tp_price,
                    'zone_upper': current_trade.zone_upper,
                    'zone_lower': current_trade.zone_lower,
                    'bars_held': bars_held,
                }

            frames.append(_build_frame(
                bar_index_in_day, current_time, row,
                current_zones, nearest_support, nearest_resistance,
                signal_event, exit_event, open_trade, list(completed_trades),
                decimals,
            ))
            bar_index_in_day += 1

    # Summary
    wins = sum(1 for t in completed_trades if t.get('pnl_pips', 0) > 0)
    losses = sum(1 for t in completed_trades if t.get('pnl_pips', 0) <= 0)
    total_pips = sum(t.get('pnl_pips', 0) for t in completed_trades)

    from datetime import date as date_cls
    incomplete = target_date >= date_cls.today()

    return {
        'frames': frames,
        'context_bars': context_bars,
        'zones': day_zones,
        'summary': {
            'pair': pair,
            'date': str(target_date),
            'total_bars': len(frames),
            'total_trades': len(completed_trades),
            'wins': wins,
            'losses': losses,
            'total_pnl_pips': round(total_pips, 1),
            'decimals': decimals,
            'pip': pip,
            'incomplete': incomplete,
        },
    }


def _build_frame(
    bar_index, current_time, row,
    current_zones, nearest_support, nearest_resistance,
    signal_event, exit_event, open_trade, completed_trades,
    decimals,
) -> dict:
    return {
        'bar_index': bar_index,
        'time': str(current_time),
        'open': round(float(row['Open']), decimals),
        'high': round(float(row['High']), decimals),
        'low': round(float(row['Low']), decimals),
        'close': round(float(row['Close']), decimals),
        'nearest_support': _zone_to_dict(nearest_support) if nearest_support else None,
        'nearest_resistance': _zone_to_dict(nearest_resistance) if nearest_resistance else None,
        'signal': signal_event,
        'exit': exit_event,
        'open_trade': open_trade,
        'completed_trades': completed_trades,
    }


def _expand_hourly_to_minutes(
    frames: list[dict],
    minute_df: pd.DataFrame,
    decimals: int,
) -> list[dict]:
    """Replace hourly frames with minute-level frames, preserving strategy events.

    Strategy signals/exits are placed on the last minute bar of their hour.
    """
    if minute_df.empty:
        return frames

    minute_df = minute_df.sort_index()
    expanded: list[dict] = []

    for frame in frames:
        frame_time = pd.Timestamp(frame['time'])
        hour_end = frame_time + pd.Timedelta(hours=1)

        mask = (minute_df.index >= frame_time) & (minute_df.index < hour_end)
        minute_bars = minute_df[mask]

        if minute_bars.empty:
            frame['bar_index'] = len(expanded)
            expanded.append(frame)
            continue

        n = len(minute_bars)
        for j, (ts, mbar) in enumerate(minute_bars.iterrows()):
            is_last = (j == n - 1)
            expanded.append({
                'bar_index': len(expanded),
                'time': str(ts),
                'open': round(float(mbar['Open']), decimals),
                'high': round(float(mbar['High']), decimals),
                'low': round(float(mbar['Low']), decimals),
                'close': round(float(mbar['Close']), decimals),
                'nearest_support': frame['nearest_support'],
                'nearest_resistance': frame['nearest_resistance'],
                'signal': frame['signal'] if is_last else None,
                'exit': frame['exit'] if is_last else None,
                'open_trade': frame['open_trade'],
                'completed_trades': frame['completed_trades'],
            })

    return expanded


# ---------------------------------------------------------------------------
# HTTP handlers
# ---------------------------------------------------------------------------

def _build_params(preset_name: str) -> StrategyParams:
    """Build StrategyParams from a preset name."""
    preset = STRATEGY_PRESETS.get(preset_name, STRATEGY_PRESETS.get('optimized', {}))
    return StrategyParams(**preset)


async def handle_replay_page(_request: web.Request) -> web.StreamResponse:
    """Serve the replay HTML page."""
    return web.FileResponse(WEB_DIR / 'replay.html')


async def handle_replay(request: web.Request) -> web.Response:
    """``GET /api/replay?pair=EURUSD&date=2025-03-10&preset=optimized&tf=1m``"""
    pair = request.query.get('pair', '').upper()
    date_str = request.query.get('date', '')
    preset = request.query.get('preset', 'optimized')
    timeframe = request.query.get('tf', '1m')

    if pair not in PAIRS:
        return web.json_response({'error': f'Unknown pair: {pair}'}, status=400)

    try:
        target = date.fromisoformat(date_str)
    except (ValueError, TypeError):
        return web.json_response({'error': f'Invalid date: {date_str}'}, status=400)

    if target.weekday() >= 5:
        return web.json_response({'error': 'Weekends have no trading data'}, status=400)

    ticker = PAIRS[pair]['ticker']
    zone_history_days = DEFAULT_ZONE_HISTORY_DAYS

    # Load only reads from cache — use "Update Data" to fetch from IBKR
    daily_start = datetime(target.year, target.month, target.day) - timedelta(days=zone_history_days + 10)
    daily_end = datetime(target.year, target.month, target.day, 23, 59, 59)
    daily_df = db.load_ohlc(ticker, '1d', start=daily_start, end=daily_end)

    hourly_start = datetime(target.year, target.month, target.day) - timedelta(days=7)
    hourly_end = datetime(target.year, target.month, target.day, 23, 59, 59)
    hourly_df = db.load_ohlc(ticker, '1h', start=hourly_start, end=hourly_end)

    if daily_df.empty or hourly_df.empty:
        return web.json_response(
            {'error': f'No cached data for {pair}. Click "Update Data" to fetch from IBKR.'},
            status=404,
        )

    params = _build_params(preset)
    decimals = PAIRS[pair].get('decimals', 5)
    result = generate_replay_frames(daily_df, hourly_df, pair, target, params, zone_history_days)

    if not result['frames']:
        return web.json_response(
            {'error': f'No bars found for {pair} on {date_str}. Click "Update Data" to fetch from IBKR.'},
            status=404,
        )

    # Expand target-day frames to minute bars when requested
    actual_tf = '1h'
    if timeframe == '1m':
        minute_df = db.load_ohlc(ticker, '1m')
        if not minute_df.empty:
            result['frames'] = _expand_hourly_to_minutes(result['frames'], minute_df, decimals)
            result['summary']['total_bars'] = len(result['frames'])
            actual_tf = '1m'

    result['summary']['timeframe'] = actual_tf
    result['summary']['timeframe_requested'] = timeframe
    return web.json_response(result)


async def handle_replay_dates(request: web.Request) -> web.Response:
    """``GET /api/replay/dates?pair=EURUSD`` — return cached date range."""
    pair = request.query.get('pair', '').upper()
    if pair not in PAIRS:
        return web.json_response({'error': f'Unknown pair: {pair}'}, status=400)

    ticker = PAIRS[pair]['ticker']
    cached = db.get_cached_range(ticker, '1h')

    if cached is None:
        return web.json_response({'error': f'No cached hourly data for {pair}'}, status=404)

    min_ts, max_ts, count = cached
    # Parse to date strings
    try:
        first_date = str(pd.Timestamp(min_ts).date())
        last_date = str(pd.Timestamp(max_ts).date())
    except Exception:
        first_date = min_ts[:10]
        last_date = max_ts[:10]

    return web.json_response({
        'pair': pair,
        'first_date': first_date,
        'last_date': last_date,
        'bar_count': count,
    })


async def handle_replay_refresh(request: web.Request) -> web.Response:
    """``POST /api/replay/refresh?pair=EURUSD`` — force-fetch hourly + minute data from IBKR."""
    pair = request.query.get('pair', '').upper()
    if pair not in PAIRS:
        return web.json_response({'error': f'Unknown pair: {pair}'}, status=400)

    ticker = PAIRS[pair]['ticker']

    # Use a dedicated client ID (+5000) to avoid colliding with the live
    # dashboard's scan thread (base) and quote thread (+1000).
    from . import ibkr
    replay_client_id = int(ibkr.TWS_CLIENT_ID) + 5000

    def _fetch_all():
        """Fetch all timeframes sequentially on a single connection, then disconnect."""
        try:
            hourly = fetch_hourly_data(ticker, days=8, force_refresh=True, client_id=replay_client_id)
            minute = fetch_minute_data_cached(ticker, days=2, allow_stale_cache=False, client_id=replay_client_id)
            daily = fetch_daily_data(
                ticker, days=DEFAULT_ZONE_HISTORY_DAYS + 10, force_refresh=True, client_id=replay_client_id,
            )
            return daily, hourly, minute
        finally:
            # Release the IBKR connection so the client ID is free next time
            ibkr.disconnect()

    daily_df, hourly_df, minute_df = await request.loop.run_in_executor(None, _fetch_all)

    return web.json_response({
        'pair': pair,
        'daily_bars': len(daily_df),
        'hourly_bars': len(hourly_df),
        'minute_bars': len(minute_df),
    })
