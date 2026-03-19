"""Replay engine: walk through a single day's hourly bars with full strategy state.

Mirrors the backtest loop but yields per-bar frames for progressive
visualization in the browser.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import replace
from datetime import date, datetime, timedelta
from functools import lru_cache
import json
from pathlib import Path
from typing import Optional

import pandas as pd
from aiohttp import web

from .backtest import (
    _deserialize_backtest_result,
    _params_signature,
    calculate_compounding_pnl,
)
from .config import PAIRS, STRATEGY_PRESETS, DEFAULT_ZONE_HISTORY_DAYS
from .execution import historical_execution_quote
from .profiles import DEFAULT_PROFILE, PROFILES, get_profile
from . import db
from .data import fetch_daily_data, fetch_hourly_data, fetch_minute_data_cached
from .levels import detect_zones, SRZone
from .strategy import (
    Trade, StrategyParams, params_from_profile,
)
from .serialization import (
    serialize_trade as shared_serialize_trade,
    serialize_zone as shared_serialize_zone,
    trade_active_dates as shared_trade_active_dates,
)
from .walkforward import WalkForwardBar, run_walk_forward, slice_daily_window


WEB_DIR = Path(__file__).resolve().parent / 'web_live'


def _extend_hourly_with_minute_tail(
    hourly_df: pd.DataFrame,
    minute_df: pd.DataFrame,
) -> pd.DataFrame:
    """Fill missing trailing 1h bars from the finer-grained minute cache.

    Replay charts are driven from hourly bars. On the current trading day the
    hourly cache can lag while the minute cache already contains newer price
    action. Aggregate those trailing minute bars into synthetic 1h bars so the
    replay can continue past the last completed cached hourly candle.
    """

    if hourly_df.empty or minute_df.empty:
        return hourly_df

    hourly = hourly_df.sort_index()
    minute = minute_df.sort_index()

    last_hourly = pd.Timestamp(hourly.index[-1])
    tail_start = last_hourly + pd.Timedelta(hours=1)
    minute_tail = minute[minute.index >= tail_start]
    if minute_tail.empty:
        return hourly

    agg_map = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
    }
    if 'Volume' in minute_tail.columns:
        agg_map['Volume'] = 'sum'

    extended = minute_tail.resample('1h', label='left', closed='left').agg(agg_map)
    extended = extended.dropna(subset=['Open', 'High', 'Low', 'Close'])
    if extended.empty:
        return hourly

    if 'Volume' not in extended.columns:
        extended['Volume'] = 0.0

    extended = extended[~extended.index.isin(hourly.index)]
    if extended.empty:
        return hourly

    return pd.concat([hourly, extended]).sort_index()


def _trade_active_dates(
    entry_time: pd.Timestamp | str | None,
    exit_time: pd.Timestamp | str | None,
) -> list[str]:
    """Return every calendar date touched by a trade, inclusive."""

    return shared_trade_active_dates(entry_time, exit_time)


def _trade_realized_date(trade_row: dict) -> str:
    """Return the date on which the trade result should count for diary P&L."""

    if trade_row.get('exit_time'):
        return str(trade_row['exit_time'])[:10]
    if trade_row.get('entry_time'):
        return str(trade_row['entry_time'])[:10]
    return ''


def _trade_realized_timestamp(trade_row: dict) -> pd.Timestamp | None:
    """Return the timestamp used for realized-date ordering in UI summaries."""

    value = trade_row.get('exit_time') or trade_row.get('entry_time')
    if not value:
        return None

    try:
        ts = pd.Timestamp(value)
    except (ValueError, TypeError):
        return None

    if ts.tzinfo is None:
        return ts.tz_localize('UTC')
    return ts.tz_convert('UTC')


def _trade_is_active_on_date(trade_row: dict, selected_date: str) -> bool:
    """Return True when a trade was open at any point on the selected date."""

    active_dates = trade_row.get('active_dates')
    if not active_dates:
        active_dates = _trade_active_dates(
            trade_row.get('entry_time'),
            trade_row.get('exit_time'),
        )
    return selected_date in active_dates


def _zone_to_dict(zone: SRZone) -> dict:
    return shared_serialize_zone(zone)


def _trade_to_dict(trade: Trade, pip: float) -> dict:
    return shared_serialize_trade(
        trade,
        include_risk=False,
        include_quality=False,
        include_active_dates=True,
        round_exit_metrics=True,
    )


def _trade_row_to_dict(
    pair: str,
    trade: Trade,
    decimals: int,
    source_row: dict,
    *,
    balance_after: float | None = None,
    risk_amount: float | None = None,
    pnl_amount: float | None = None,
) -> dict:
    return {
        'pair': pair,
        'status': 'OPEN' if trade.exit_time is None else 'CLOSED',
        'entry_time': pd.Timestamp(trade.entry_time).isoformat(),
        'exit_time': pd.Timestamp(trade.exit_time).isoformat() if trade.exit_time is not None else None,
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
        'exit_reason': trade.exit_reason or ('OPEN' if trade.exit_time is None else None),
        'active_dates': _trade_active_dates(trade.entry_time, trade.exit_time),
        'hourly_days': source_row['hourly_days'],
        'zone_history_days': source_row['zone_history_days'],
        'strategy_version': source_row['strategy_version'],
        'updated_at': source_row['updated_at'].isoformat() if hasattr(source_row['updated_at'], 'isoformat') else str(source_row['updated_at']),
        'risk_amount': round(float(risk_amount), 2) if risk_amount is not None else None,
        'pnl_amount': round(float(pnl_amount), 2) if pnl_amount is not None else None,
        'balance_after': round(float(balance_after), 2) if balance_after is not None else None,
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


def _cached_backtest_key(row: dict) -> str:
    return (
        f"{row.get('params_hash', '')}|"
        f"{int(row.get('hourly_days', 0))}|"
        f"{int(row.get('zone_history_days', 0))}"
    )


def _parse_run_config_json(raw: str | None) -> dict:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _describe_backtest_row(row: dict) -> dict:
    run_config = _parse_run_config_json(row.get('run_config_json'))
    matched_profile = _known_compounding_profiles().get(row.get('params_hash'), {})
    profile_name = (
        run_config.get('resolved_profile')
        or run_config.get('requested_profile')
        or matched_profile.get('profile_name')
        or ''
    )
    profile_description = (
        run_config.get('profile_description')
        or PROFILES.get(profile_name, {}).get('description', '')
        or matched_profile.get('description', '')
        or ''
    )
    selection_label = run_config.get('selection_label') or ''
    label = profile_name or selection_label or row.get('params_hash', '')[:10] or 'cached run'
    if selection_label and selection_label not in {'baseline', profile_name}:
        label = f"{label} [{selection_label}]"
    return {
        'key': _cached_backtest_key(row),
        'label': label,
        'profile_name': profile_name,
        'description': profile_description,
        'selection_label': selection_label,
        'params_hash': row.get('params_hash'),
        'hourly_days': int(row.get('hourly_days', 0) or 0),
        'zone_history_days': int(row.get('zone_history_days', 0) or 0),
        'starting_balance': (
            float(run_config['starting_balance'])
            if run_config.get('starting_balance') is not None
            else matched_profile.get('starting_balance')
        ),
        'risk_pct': (
            float(run_config['risk_pct'])
            if run_config.get('risk_pct') is not None
            else (
                float(matched_profile.get('risk_pct', 0.0)) * 100.0
                if matched_profile.get('risk_pct') is not None
                else None
            )
        ),
        'updated_at': row['updated_at'].isoformat() if hasattr(row.get('updated_at'), 'isoformat') else str(row.get('updated_at') or ''),
    }


def _list_cached_backtests(rows: list[dict]) -> list[dict]:
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(_cached_backtest_key(row), []).append(row)

    backtests: list[dict] = []
    for key, grouped_rows in grouped.items():
        representative = max(grouped_rows, key=lambda item: item.get('updated_at') or '')
        descriptor = _describe_backtest_row(representative)
        descriptor['pair_count'] = len({row['pair'] for row in grouped_rows})
        backtests.append(descriptor)

    backtests.sort(
        key=lambda item: (
            0 if item.get('profile_name') == DEFAULT_PROFILE else 1,
            -(pd.Timestamp(item.get('updated_at') or 0).value if item.get('updated_at') else 0),
            item.get('label') or '',
        )
    )
    return backtests


def _select_cached_backtest_rows(backtest_key: str | None = None) -> tuple[list[dict], list[dict], dict | None]:
    from .backtest import BACKTEST_CACHE_VERSION

    rows = [
        row for row in db.load_backtest_results()
        if row.get('strategy_version') == BACKTEST_CACHE_VERSION
    ]
    backtests = _list_cached_backtests(rows)
    if not backtests:
        return [], [], None

    selected = None
    if backtest_key:
        for candidate in backtests:
            if candidate['key'] == backtest_key:
                selected = candidate
                break
    if selected is None:
        selected = backtests[0]

    selected_rows = [
        row for row in rows
        if _cached_backtest_key(row) == selected['key']
    ]
    latest_by_pair: dict[str, dict] = {}
    for row in selected_rows:
        pair_value = row['pair']
        if pair_value not in latest_by_pair:
            latest_by_pair[pair_value] = row
    return list(latest_by_pair.values()), backtests, selected


def _trade_compounding_key(
    pair: str,
    trade: Trade,
) -> tuple[str, str, str | None, str, float, float, float]:
    """Build a stable key for mapping running-balance data back onto trade rows."""

    return (
        pair,
        str(trade.entry_time),
        str(trade.exit_time) if trade.exit_time is not None else None,
        trade.direction,
        float(trade.entry_price),
        float(trade.sl_price),
        float(trade.tp_price),
    )


@lru_cache(maxsize=1)
def _known_compounding_profiles() -> dict[str, dict]:
    """Map profile parameter hashes to the balance assumptions used in the UI."""

    known_profiles: dict[str, dict] = {}
    for name, profile in PROFILES.items():
        profile_params = params_from_profile(profile)
        known_profiles[_params_signature(profile_params)] = {
            'profile_name': name,
            'description': profile.get('description', ''),
            'starting_balance': float(profile.get('starting_balance', 1000.0)),
            'risk_pct': float(profile.get('risk_pct', 5.0)) / 100.0,
            # The trade list shows all cached trades, so keep the risk model but
            # disable correlation filtering when calculating the running balance.
            'params': replace(profile_params, use_correlation_filter=False),
        }
    return known_profiles


def _default_compounding_profile() -> dict:
    """Return the fallback balance assumptions for cached trade tables."""

    profile = get_profile(DEFAULT_PROFILE)
    profile_params = params_from_profile(profile)
    return {
        'profile_name': DEFAULT_PROFILE,
        'starting_balance': float(profile.get('starting_balance', 1000.0)),
        'risk_pct': float(profile.get('risk_pct', 5.0)) / 100.0,
        'params': replace(profile_params, use_correlation_filter=False),
    }


def _resolve_trade_table_compounding(rows: list[dict]) -> dict:
    """Choose the most appropriate compounding settings for the cached rows."""

    known_profiles = _known_compounding_profiles()
    default_profile = _default_compounding_profile()

    hashes = [row.get('params_hash') for row in rows if row.get('params_hash')]
    unique_hashes = {params_hash for params_hash in hashes if params_hash}
    if not hashes:
        return {
            **default_profile,
            'assumed': True,
            'mixed_params': False,
        }

    if len(unique_hashes) == 1:
        matched = known_profiles.get(next(iter(unique_hashes)))
        if matched is not None:
            return {
                **matched,
                'assumed': False,
                'mixed_params': False,
            }

    counts = Counter(hashes)
    for params_hash, _count in counts.most_common():
        matched = known_profiles.get(params_hash)
        if matched is not None:
            return {
                **matched,
                'assumed': True,
                'mixed_params': len(unique_hashes) > 1,
            }

    return {
        **default_profile,
        'assumed': True,
        'mixed_params': len(unique_hashes) > 1,
    }


def _build_trade_balance_lookup(results_by_pair: dict[str, object], compounding: dict) -> dict:
    """Calculate running post-close balances for the cached trade list."""

    if not results_by_pair:
        return {}

    trade_log, _ = calculate_compounding_pnl(
        results_by_pair,
        starting_balance=float(compounding['starting_balance']),
        risk_pct=float(compounding['risk_pct']),
        params=compounding['params'],
    )

    balance_lookup: dict[tuple[str, str, str | None, str, float, float, float], dict] = {}
    for pair, trade, risk_amount, pnl_amount, balance_after in trade_log:
        balance_lookup[_trade_compounding_key(pair, trade)] = {
            'risk_amount': float(risk_amount),
            'pnl_amount': float(pnl_amount),
            'balance_after': float(balance_after),
        }
    return balance_lookup


def _load_cached_backtest_trades(
    pair: str | None = None,
    backtest_key: str | None = None,
) -> tuple[list[dict], dict]:
    all_rows, _backtests, _selected = _select_cached_backtest_rows(backtest_key=backtest_key)
    parsed_rows: list[tuple[dict, object, int]] = []
    results_by_pair: dict[str, object] = {}

    for row in all_rows:
        try:
            result = _deserialize_backtest_result(row['result_json'])
        except (ValueError, TypeError, KeyError):
            continue

        decimals = PAIRS.get(row['pair'], {}).get('decimals', 5)
        parsed_rows.append((row, result, decimals))
        results_by_pair[row['pair']] = result

    compounding = _resolve_trade_table_compounding([row for row, _, _ in parsed_rows])
    balance_lookup = _build_trade_balance_lookup(results_by_pair, compounding)

    trades: list[dict] = []
    for row, result, decimals in parsed_rows:
        for trade in result.trades:
            balance_data = balance_lookup.get(_trade_compounding_key(row['pair'], trade), {})
            trades.append(
                _trade_row_to_dict(
                    row['pair'],
                    trade,
                    decimals,
                    row,
                    balance_after=balance_data.get('balance_after'),
                    risk_amount=balance_data.get('risk_amount'),
                    pnl_amount=balance_data.get('pnl_amount'),
                )
            )
        for trade in getattr(result, 'pending_trades', []):
            trades.append(
                _trade_row_to_dict(
                    row['pair'],
                    trade,
                    decimals,
                    row,
                )
            )

    if pair is not None:
        trades = [trade for trade in trades if trade['pair'] == pair]

    trades.sort(key=lambda item: item['entry_time'] or '', reverse=True)
    return trades, {
        'profile_name': compounding['profile_name'],
        'starting_balance': round(float(compounding['starting_balance']), 2),
        'risk_pct': round(float(compounding['risk_pct']) * 100.0, 2),
        'assumed': bool(compounding.get('assumed')),
        'mixed_params': bool(compounding.get('mixed_params')),
    }


def _build_account_day_summary(
    selected_date: date | str,
    trades: list[dict],
    compounding: dict,
) -> dict:
    """Summarize realized account P&L and balance for a selected calendar day."""

    selected_str = str(selected_date)
    realized_today: list[dict] = []
    latest_balance: float | None = None
    latest_balance_ts: pd.Timestamp | None = None

    for trade in trades:
        realized_date = _trade_realized_date(trade)
        if not realized_date:
            continue

        if realized_date == selected_str:
            realized_today.append(trade)

        if realized_date > selected_str or trade.get('balance_after') is None:
            continue

        realized_ts = _trade_realized_timestamp(trade)
        if realized_ts is None:
            continue

        if latest_balance_ts is None or realized_ts > latest_balance_ts:
            latest_balance_ts = realized_ts
            latest_balance = float(trade['balance_after'])

    day_pnl_amount = round(sum(float(trade.get('pnl_amount') or 0.0) for trade in realized_today), 2)
    if latest_balance is None:
        starting_balance = compounding.get('starting_balance')
        latest_balance = float(starting_balance) if starting_balance is not None else None

    return {
        'day_pnl_amount': day_pnl_amount,
        'balance': round(float(latest_balance), 2) if latest_balance is not None else None,
        'realized_trades': len(realized_today),
        'profile_name': compounding.get('profile_name'),
        'starting_balance': compounding.get('starting_balance'),
        'risk_pct': compounding.get('risk_pct'),
        'assumed': bool(compounding.get('assumed')),
        'mixed_params': bool(compounding.get('mixed_params')),
    }


def generate_replay_frames(
    daily_df: pd.DataFrame,
    hourly_df: pd.DataFrame,
    pair: str,
    target_date: date,
    params: StrategyParams | None = None,
    zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS,
    minute_df: pd.DataFrame | None = None,
    l2_snapshots: pd.DataFrame | None = None,
) -> dict:
    """Walk the full hourly history, emitting frames only for *target_date*.

    Returns ``{"frames": [...], "zones": [...], "summary": {...}}``.
    """
    if params is None:
        params = StrategyParams()

    pair_info = PAIRS.get(pair, {})
    pip = pair_info.get('pip', 0.0001)
    decimals = pair_info.get('decimals', 5)

    frames: list[dict] = []
    context_bars: list[dict] = []
    completed_trades: list[dict] = []
    day_zones: list[dict] = []
    frame_index = 0
    selected_day_bar_count = 0
    carry_trade_entry_time: Optional[pd.Timestamp] = None

    def zone_provider(current_time, current_date, _bar_index):
        bar_date = pd.Timestamp(current_date)
        if hasattr(current_time, 'tzinfo') and current_time.tzinfo:
            bar_date = bar_date.tz_localize(current_time.tzinfo)
        daily_window = slice_daily_window(daily_df, bar_date, zone_history_days)
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

    def serialize_open_trade(trade: Trade | None, bars_held: int) -> dict | None:
        if trade is None:
            return None
        return {
            'entry_time': pd.Timestamp(trade.entry_time).isoformat(),
            'entry_price': trade.entry_price,
            'direction': trade.direction,
            'sl_price': trade.sl_price,
            'tp_price': trade.tp_price,
            'zone_upper': trade.zone_upper,
            'zone_lower': trade.zone_lower,
            'bars_held': bars_held,
        }

    # Track date of carry exit so we keep emitting frames for the rest of that day
    carry_exit_date: Optional[date] = None
    # Track last frame date so we can add tail context bars
    target_ended = False

    def on_bar(step: WalkForwardBar) -> None:
        nonlocal frame_index, selected_day_bar_count, day_zones, carry_trade_entry_time
        nonlocal carry_exit_date, target_ended

        row = step.row
        current_time = step.bar_time
        current_date = current_time.date() if hasattr(current_time, 'date') else current_time
        is_target = str(current_date) == str(target_date)

        if step.opened_trade is not None:
            opened_entry_time = pd.Timestamp(step.opened_trade.entry_time)
            if str(opened_entry_time.date()) == str(target_date):
                carry_trade_entry_time = opened_entry_time

        if not is_target and current_date < target_date:
            context_cutoff = target_date - timedelta(days=7)
            if current_date >= context_cutoff:
                context_bars.append({
                    'time': pd.Timestamp(current_time).isoformat(),
                    'open': round(float(row['Open']), decimals),
                    'high': round(float(row['High']), decimals),
                    'low': round(float(row['Low']), decimals),
                    'close': round(float(row['Close']), decimals),
                })

        # After target day ends (and any carry bars), add tail context bars
        # for the next 24h so the chart shows price action continuing
        if target_ended and not is_target:
            if carry_exit_date is not None and current_date <= carry_exit_date:
                # Still on carry exit day — include as tail context
                context_bars.append({
                    'time': pd.Timestamp(current_time).isoformat(),
                    'open': round(float(row['Open']), decimals),
                    'high': round(float(row['High']), decimals),
                    'low': round(float(row['Low']), decimals),
                    'close': round(float(row['Close']), decimals),
                })
                return
            tail_cutoff = target_date + timedelta(days=2)
            if current_date <= tail_cutoff and carry_trade_entry_time is None:
                context_bars.append({
                    'time': pd.Timestamp(current_time).isoformat(),
                    'open': round(float(row['Open']), decimals),
                    'high': round(float(row['High']), decimals),
                    'low': round(float(row['Low']), decimals),
                    'close': round(float(row['Close']), decimals),
                })
            return

        if is_target and not day_zones and step.zones:
            day_zones = [_zone_to_dict(zone) for zone in step.zones]

        signal_event = None
        if step.signal is not None:
            signal_event = {
                'direction': step.signal.direction,
                'entry_price': step.signal.entry_price,
                'sl_price': step.signal.sl_price,
                'tp_price': step.signal.tp_price,
                'zone_type': step.signal.zone_type,
            }

        if step.exit_trade is not None:
            was_carry_trade = (
                carry_trade_entry_time is not None
                and pd.Timestamp(step.exit_trade.entry_time) == carry_trade_entry_time
            )
            exit_event = {
                'reason': step.exit_trade.exit_reason,
                'price': step.exit_trade.exit_price,
                'pnl_pips': round(step.exit_trade.pnl_pips, 1),
                'pnl_r': round(step.exit_trade.pnl_r, 2),
            }
            completed_trades.append(_trade_to_dict(step.exit_trade, pip))
            if is_target or was_carry_trade:
                frames.append(_build_frame(
                    frame_index,
                    current_time,
                    row,
                    step.zones,
                    step.support_zone,
                    step.resistance_zone,
                    None,
                    exit_event,
                    None,
                    list(completed_trades),
                    decimals,
                ))
                frame_index += 1
                if is_target:
                    selected_day_bar_count += 1
            if was_carry_trade:
                carry_trade_entry_time = None
                carry_exit_date = current_date
            return

        is_carry_continuation = (
            current_date > target_date
            and carry_trade_entry_time is not None
            and step.open_trade is not None
            and pd.Timestamp(step.open_trade.entry_time) == carry_trade_entry_time
        )

        if is_target:
            if step.open_trade is not None:
                carry_trade_entry_time = pd.Timestamp(step.open_trade.entry_time)
            frames.append(_build_frame(
                frame_index,
                current_time,
                row,
                step.zones,
                step.support_zone,
                step.resistance_zone,
                signal_event,
                None,
                serialize_open_trade(step.open_trade, step.bars_held),
                list(completed_trades),
                decimals,
            ))
            frame_index += 1
            selected_day_bar_count += 1
            return

        if is_carry_continuation:
            frames.append(_build_frame(
                frame_index,
                current_time,
                row,
                step.zones,
                step.support_zone,
                step.resistance_zone,
                signal_event,
                None,
                serialize_open_trade(step.open_trade, step.bars_held),
                list(completed_trades),
                decimals,
            ))
            frame_index += 1
            return

        # Past target day, no carry — mark target as ended so tail bars get added
        if current_date > target_date:
            target_ended = True
            # Add this bar as tail context
            context_bars.append({
                'time': pd.Timestamp(current_time).isoformat(),
                'open': round(float(row['Open']), decimals),
                'high': round(float(row['High']), decimals),
                'low': round(float(row['Low']), decimals),
                'close': round(float(row['Close']), decimals),
            })

    run_walk_forward(
        hourly_df,
        pair=pair,
        params=params,
        pip=pip,
        zone_provider=zone_provider,
        execution_quote_provider=execution_quote_provider,
        on_bar=on_bar,
        force_close_end=False,
    )

    # Summary — separate target-day trades from all trades
    target_str = str(target_date)
    day_trades = [t for t in completed_trades if _trade_is_active_on_date(t, target_str)]
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
            'selected_day_bars': selected_day_bar_count,
            'replay_bars': len(frames),
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
            'continues_after_selected_day': len(frames) > selected_day_bar_count,
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
        'time': pd.Timestamp(current_time).isoformat(),
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


async def handle_trade_log_page(_request: web.Request) -> web.StreamResponse:
    """Serve the live trade log page."""
    return web.FileResponse(WEB_DIR / 'trade_log.html')


async def handle_trade_log_api(_request: web.Request) -> web.Response:
    """Return detected signals from the live history database."""
    from .live_history import load_detected_signals

    pair = _pair_from_request(_request)
    status = (_request.query.get('status', '') or '').strip() or None
    limit = int(_request.query.get('limit', '200'))

    signals = load_detected_signals(pair=pair, status=status, limit=limit)
    all_pairs = sorted({s['pair'] for s in signals})

    # Ensure JSON-safe values
    for s in signals:
        for k, v in s.items():
            if hasattr(v, 'isoformat'):
                s[k] = v.isoformat()

    return web.json_response({
        'signals': signals,
        'pairs': all_pairs,
        'count': len(signals),
    })


async def handle_backtest_trades_page(_request: web.Request) -> web.StreamResponse:
    """Serve the backtest trades page."""
    return web.FileResponse(WEB_DIR / 'backtest_trades.html')


async def handle_backtest_diary_page(_request: web.Request) -> web.StreamResponse:
    """Serve the backtest diary page."""
    return web.FileResponse(WEB_DIR / 'backtest_diary.html')


async def handle_backtest_trades_api(_request: web.Request) -> web.Response:
    """Return completed backtest trades for all cached pairs."""
    pair_filter = _pair_from_request(_request)
    backtest_key = (_request.query.get('backtest', '') or '').strip() or None
    if pair_filter is not None and pair_filter not in PAIRS:
        return web.json_response({'error': f'Unknown pair: {pair_filter}'}, status=400)

    selected_rows, backtests, selected_backtest = _select_cached_backtest_rows(backtest_key=backtest_key)
    trades, compounding = _load_cached_backtest_trades(pair=pair_filter, backtest_key=backtest_key)
    available_pairs = sorted({row['pair'] for row in selected_rows})

    return web.json_response({
        'trades': trades,
        'pairs': available_pairs,
        'backtests': backtests,
        'selected_backtest': selected_backtest,
        'pair_filter': pair_filter,
        'count': len(trades),
        'compounding': compounding,
    })


async def handle_backtest_diary_api(_request: web.Request) -> web.Response:
    """Return trades whose entry occurs on the selected date."""
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

    trades, compounding = _load_cached_backtest_trades(pair=pair_filter)
    matches = [
        trade for trade in trades
        if str(trade.get('entry_time') or '')[:10] == selected_str
    ]
    wins = sum(1 for trade in matches if trade['pnl_pips'] > 0)
    losses = sum(1 for trade in matches if trade['pnl_pips'] < 0)
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
        'compounding': compounding,
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
    import time as _time
    _t0 = _time.perf_counter()

    pair = request.query.get('pair', '').upper()
    date_str = request.query.get('date', '')
    preset = request.query.get('preset', 'optimized')
    timeframe = request.query.get('tf', '1m')
    backtest_key = (request.query.get('backtest', '') or '').strip() or None

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

    _t1 = _time.perf_counter()
    daily_df = db.load_ohlc(ticker, '1d', start=daily_start, end=daily_end)
    _t2 = _time.perf_counter()

    hourly_start = datetime(target.year, target.month, target.day) - timedelta(days=30)
    today = date.today()
    hourly_end_date = max(target, today)
    hourly_end = datetime(hourly_end_date.year, hourly_end_date.month, hourly_end_date.day, 23, 59, 59)
    hourly_df = db.load_ohlc(ticker, '1h', start=hourly_start, end=hourly_end)
    _t3 = _time.perf_counter()
    minute_df = db.load_ohlc(ticker, '1m', start=hourly_start, end=hourly_end)
    _t4 = _time.perf_counter()
    l2_snapshots = db.load_l2_snapshots(ticker, start=hourly_start, end=hourly_end)
    _t5 = _time.perf_counter()
    display_hourly_df = _extend_hourly_with_minute_tail(hourly_df, minute_df)

    if daily_df.empty or display_hourly_df.empty:
        return web.json_response(
            {'error': f'No cached data for {pair}. Click "Update Data" to fetch from IBKR.'},
            status=404,
        )

    params = _build_params(preset)
    decimals = PAIRS[pair].get('decimals', 5)
    result = generate_replay_frames(
        daily_df,
        display_hourly_df,
        pair,
        target,
        params,
        zone_history_days,
        minute_df=minute_df,
        l2_snapshots=l2_snapshots,
    )
    _t6 = _time.perf_counter()
    account_trades, compounding = _load_cached_backtest_trades(backtest_key=backtest_key)
    _t7 = _time.perf_counter()
    result['summary']['account'] = _build_account_day_summary(target, account_trades, compounding)

    if not result['frames'] and not result['context_bars']:
        return web.json_response(
            {'error': f'No bars found for {pair} on {date_str}. Click "Update Data" to fetch from IBKR.'},
            status=404,
        )

    # Expand target-day frames to minute bars when requested
    actual_tf = '1h'
    if timeframe == '1m':
        if not minute_df.empty:
            result['frames'] = _expand_hourly_to_minutes(result['frames'], minute_df, decimals)
            result['summary']['total_bars'] = len(result['frames'])
            actual_tf = '1m'

    result['summary']['timeframe'] = actual_tf
    result['summary']['timeframe_requested'] = timeframe

    _t8 = _time.perf_counter()
    print(f"[REPLAY TIMING] {pair} {date_str} tf={timeframe} total={_t8-_t0:.3f}s | "
          f"daily={_t2-_t1:.3f}s hourly={_t3-_t2:.3f}s minute={_t4-_t3:.3f}s "
          f"l2={_t5-_t4:.3f}s walkforward={_t6-_t5:.3f}s "
          f"backtest_trades={_t7-_t6:.3f}s serialize={_t8-_t7:.3f}s")

    return web.json_response(result)


async def handle_replay_bars(request: web.Request) -> web.Response:
    """``GET /api/replay/bars?pair=EURUSD&tf=1h&start=2025-03-01&end=2025-03-10``

    Lightweight endpoint returning raw OHLC bars from cache, used for
    scroll-based streaming in the replay chart.
    """
    pair = request.query.get('pair', '').upper()
    tf = request.query.get('tf', '1h')
    start_str = request.query.get('start', '')
    end_str = request.query.get('end', '')

    if pair not in PAIRS:
        return web.json_response({'error': f'Unknown pair: {pair}'}, status=400)
    if tf not in ('1h', '1m'):
        return web.json_response({'error': f'Unsupported timeframe: {tf}'}, status=400)

    def _parse_iso(s: str):
        if not s:
            return None
        # JS toISOString() produces "2025-03-10T00:00:00.000Z" — strip Z/milliseconds
        s = s.replace('Z', '+00:00')
        return pd.Timestamp(s).to_pydatetime().replace(tzinfo=None)

    try:
        start_dt = _parse_iso(start_str)
    except Exception:
        return web.json_response({'error': f'Invalid start: {start_str}'}, status=400)
    try:
        end_dt = _parse_iso(end_str)
    except Exception:
        return web.json_response({'error': f'Invalid end: {end_str}'}, status=400)

    ticker = PAIRS[pair]['ticker']
    decimals = PAIRS[pair].get('decimals', 5)
    df = db.load_ohlc(ticker, tf, start=start_dt, end=end_dt)
    if tf == '1h':
        minute_df = db.load_ohlc(ticker, '1m', start=start_dt, end=end_dt)
        df = _extend_hourly_with_minute_tail(df, minute_df)

    if df.empty:
        return web.json_response({'bars': []})

    bars = []
    for ts, row in df.iterrows():
        bars.append({
            'time': pd.Timestamp(ts).isoformat(),
            'open': round(float(row['Open']), decimals),
            'high': round(float(row['High']), decimals),
            'low': round(float(row['Low']), decimals),
            'close': round(float(row['Close']), decimals),
        })

    return web.json_response({'bars': bars})


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

    daily_df, hourly_df, minute_df = await asyncio.get_running_loop().run_in_executor(None, _fetch_all)

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
