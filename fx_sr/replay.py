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

from .backtest import _slice_daily_window, _deserialize_backtest_result, _finalize_trade
from .config import PAIRS, STRATEGY_PRESETS, DEFAULT_ZONE_HISTORY_DAYS
from .profiles import PROFILES, get_profile
from . import db
from .data import fetch_daily_data, fetch_hourly_data, fetch_minute_data_cached
from .levels import detect_zones, SRZone
from .strategy import (
    Trade, StrategyParams, check_exit, build_trade_from_signal,
    get_tradeable_zones, select_entry_signal,
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


def _trade_row_to_dict(
    pair: str,
    trade: Trade,
    decimals: int,
    source_row: dict,
) -> dict:
    return {
        'pair': pair,
        'entry_time': str(trade.entry_time),
        'exit_time': str(trade.exit_time) if trade.exit_time is not None else None,
        'direction': trade.direction,
        'entry_price': round(float(trade.entry_price), decimals),
        'exit_price': float(trade.exit_price) if trade.exit_price is not None else None,
        'sl_price': round(float(trade.sl_price), decimals),
        'tp_price': round(float(trade.tp_price), decimals),
        'decimals': decimals,
        'zone_upper': round(float(trade.zone_upper), decimals),
        'zone_lower': round(float(trade.zone_lower), decimals),
        'zone_strength': trade.zone_strength,
        'risk': float(trade.risk),
        'bars_held': int(trade.bars_held),
        'pnl_pips': round(float(trade.pnl_pips), 1),
        'pnl_r': round(float(trade.pnl_r), 2),
        'exit_reason': trade.exit_reason,
        'hourly_days': source_row['hourly_days'],
        'zone_history_days': source_row['zone_history_days'],
        'strategy_version': source_row['strategy_version'],
        'updated_at': source_row['updated_at'],
    }


def _load_latest_cached_backtest_rows(pair: str | None = None) -> list[dict]:
    from .backtest import BACKTEST_CACHE_VERSION
    rows = db.load_backtest_results(pairs=[pair] if pair else None)
    latest_by_pair = {}
    for row in rows:
        # Only show results from the current strategy version
        if row.get('strategy_version') != BACKTEST_CACHE_VERSION:
            continue
        pair_value = row['pair']
        if pair_value not in latest_by_pair:
            latest_by_pair[pair_value] = row
    return list(latest_by_pair.values())


def _load_cached_backtest_trades(pair: str | None = None) -> list[dict]:
    rows = _load_latest_cached_backtest_rows(pair)
    trades: list[dict] = []
    for row in rows:
        try:
            result = _deserialize_backtest_result(row['result_json'])
        except (ValueError, TypeError, KeyError):
            continue

        decimals = PAIRS.get(row['pair'], {}).get('decimals', 5)
        for trade in result.trades:
            trades.append(_trade_row_to_dict(row['pair'], trade, decimals, row))

    trades.sort(key=lambda item: item['entry_time'] or '', reverse=True)
    return trades


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
    last_trade_was_loss = False
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
            else:
                current_zones = []
            last_zone_date = current_date

        current_price = float(row['Close'])
        nearest_support, nearest_resistance = get_tradeable_zones(current_zones, current_price)

        # --- capture zones once we reach the target day ---
        is_target = str(current_date) == str(target_date)

        # Collect pre-target bars as context (7 days before target only)
        if not is_target and current_date < target_date:
            context_cutoff = target_date - timedelta(days=7)
            if current_date >= context_cutoff:
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
                last_trade_was_loss = finished.pnl_r <= 0
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

    # Summary — separate target-day trades from all trades
    target_str = str(target_date)
    day_trades = [t for t in completed_trades if str(t.get('entry_time', ''))[:10] == target_str]
    wins = sum(1 for t in day_trades if t.get('pnl_pips', 0) > 0)
    losses = sum(1 for t in day_trades if t.get('pnl_pips', 0) <= 0)
    total_pips = sum(t.get('pnl_pips', 0) for t in day_trades)
    total_r = round(sum(t.get('pnl_r', 0) for t in day_trades), 2)
    all_trades_count = len(completed_trades)
    all_pips = round(sum(t.get('pnl_pips', 0) for t in completed_trades), 1)
    all_r = round(sum(t.get('pnl_r', 0) for t in completed_trades), 2)

    from datetime import date as date_cls
    incomplete = target_date >= date_cls.today()

    return {
        'frames': frames,
        'context_bars': context_bars,
        'zones': day_zones,
        'all_completed_trades': completed_trades,
        'summary': {
            'pair': pair,
            'date': str(target_date),
            'total_bars': len(frames),
            'total_trades': len(day_trades),
            'wins': wins,
            'losses': losses,
            'total_pnl_pips': round(total_pips, 1),
            'total_pnl_r': total_r,
            'all_trades': all_trades_count,
            'all_pnl_pips': all_pips,
            'all_pnl_r': all_r,
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

def _pair_from_request(request: web.Request) -> str | None:
    """Normalize optional pair query param."""
    pair = request.query.get('pair', '')
    pair = pair.upper().strip()
    return pair or None


async def handle_backtest_trades_page(_request: web.Request) -> web.StreamResponse:
    """Serve the backtest trades page."""
    return web.FileResponse(WEB_DIR / 'backtest_trades.html')


async def handle_backtest_diary_page(_request: web.Request) -> web.StreamResponse:
    """Serve the backtest diary page."""
    return web.FileResponse(WEB_DIR / 'backtest_diary.html')


async def handle_backtest_trades_api(_request: web.Request) -> web.Response:
    """Return completed backtest trades for all cached pairs."""
    pair_filter = _pair_from_request(_request)
    if pair_filter is not None and pair_filter not in PAIRS:
        return web.json_response({'error': f'Unknown pair: {pair_filter}'}, status=400)

    trades = _load_cached_backtest_trades(pair=pair_filter)
    rows = _load_latest_cached_backtest_rows(pair=pair_filter)
    available_pairs = sorted({row['pair'] for row in rows})

    return web.json_response({
        'trades': trades,
        'pairs': available_pairs,
        'pair_filter': pair_filter,
        'count': len(trades),
    })


async def handle_backtest_diary_api(_request: web.Request) -> web.Response:
    """Return trades whose entry or exit occurs on the selected date."""
    pair_filter = _pair_from_request(_request)
    date_str = _request.query.get('date', '')
    if pair_filter is not None and pair_filter not in PAIRS:
        return web.json_response({'error': f'Unknown pair: {pair_filter}'}, status=400)
    if not date_str:
        return web.json_response({'error': 'Missing date'}, status=400)

    try:
        selected = date.fromisoformat(date_str)
    except ValueError:
        return web.json_response({'error': f'Invalid date: {date_str}'}, status=400)
    selected_str = str(selected)

    trades = _load_cached_backtest_trades(pair=pair_filter)
    matches = []
    for trade in trades:
        entry_match = (trade['entry_time'] or '').startswith(selected_str)
        exit_match = (trade['exit_time'] or '').startswith(selected_str) if trade['exit_time'] else False
        if entry_match or exit_match:
            matches.append(trade)

    wins = sum(1 for trade in matches if trade['pnl_pips'] > 0)
    losses = len(matches) - wins
    total_pnl_pips = round(sum(trade['pnl_pips'] for trade in matches), 1)
    total_pnl_r = round(sum(trade['pnl_r'] for trade in matches), 2)

    return web.json_response({
        'date': selected_str,
        'pair_filter': pair_filter,
        'trades': matches,
        'count': len(matches),
        'wins': wins,
        'losses': losses,
        'total_pnl_pips': total_pnl_pips,
        'total_pnl_r': total_pnl_r,
    })


def _build_params(preset_name: str) -> StrategyParams:
    """Build StrategyParams from a profile name, using all profile values."""
    from .strategy import params_from_profile
    try:
        profile = get_profile(preset_name)
    except KeyError:
        profile = get_profile('optimized')
    return params_from_profile(profile)


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

    hourly_start = datetime(target.year, target.month, target.day) - timedelta(days=30)
    today = date.today()
    hourly_end_date = max(target, today)
    hourly_end = datetime(hourly_end_date.year, hourly_end_date.month, hourly_end_date.day, 23, 59, 59)
    hourly_df = db.load_ohlc(ticker, '1h', start=hourly_start, end=hourly_end)

    if daily_df.empty or hourly_df.empty:
        return web.json_response(
            {'error': f'No cached data for {pair}. Click "Update Data" to fetch from IBKR.'},
            status=404,
        )

    params = _build_params(preset)
    decimals = PAIRS[pair].get('decimals', 5)
    result = generate_replay_frames(daily_df, hourly_df, pair, target, params, zone_history_days)

    if not result['frames'] and not result['context_bars']:
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


async def handle_replay_presets(_request: web.Request) -> web.Response:
    """``GET /api/replay/presets`` — return available profile names and descriptions."""
    presets = [
        {'name': name, 'description': p['description']}
        for name, p in PROFILES.items()
    ]
    return web.json_response({'presets': presets})
