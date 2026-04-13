"""Aiohttp live dashboard with IBKR quote subscriptions."""

from __future__ import annotations

import asyncio
from collections import deque
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import re
from pathlib import Path
import sys
import threading
import webbrowser
from typing import Optional
from urllib.parse import urlparse

from aiohttp import web
import pandas as pd

from . import ibkr
from .bar_accumulator import HourlyBarAccumulator
from .live import (
    ExecutionResult,
    PairScanRow,
    _scan_pair,
    apply_startup_scan_artifacts,
    build_live_size_plans,
    collect_scan_rows,
    execute_signal_plans,
    load_closed_trade_summaries,
    refresh_pair_row_price,
    run_startup_scan_pair,
    seed_seen_wf_trades,
)
from .live_history import (
    enqueue_write_async,
    load_detected_signal,
    load_execution_activity,
    record_detected_signals,
    record_exit_signal,
    record_execution_results,
    start_background_writer,
    stop_background_writer,
)
from .data import _remaining_days_to_fetch, download_single_interval, find_first_missing_cached_bar, refill_interval_from
from .db import _connect, get_cached_range, get_db_path, get_setting, init_db, load_ohlc, set_setting
from .portfolio import build_portfolio_state, closed_trade_summary_from_row, get_entry_block
from .live_stream import StreamingScanner
from .positions import calc_pnl_pips, cancel_bracket_children, pair_pip, process_hourly_exit_bars, sync_positions


WEB_DIR = Path(__file__).resolve().parent / 'web_live'
REACT_BUILD_DIR = WEB_DIR / 'react'
RUN_PY_PATH = Path(__file__).resolve().parent.parent / 'run.py'
LOG_LIMIT = 80
ALERT_LIMIT = 200
EXECUTION_LIMIT = 200
_BACKTEST_PROGRESS_RE = re.compile(r'^\s*\[(\d+)\s*/\s*(\d+)\]\s+([A-Za-z0-9]+)')
_STARTUP_WARM_CACHE_VERSION = 1
_STARTUP_WARM_CACHE_ALIAS = 'live_startup_warm:latest'
_HOLE_REFILL_MAX_AGE = pd.Timedelta(days=30)
_PHASE2_SCAN_WORKERS_DEFAULT = 4


@dataclass(slots=True)
class _BufferedRealtimeBar:
    """Minimal realtime-bar payload kept during startup catch-up."""

    time: pd.Timestamp
    open_: float
    high: float
    low: float
    close: float
    volume: float


def _normalize_execution_mode(mode: str | None) -> str:
    resolved = mode or 'intrabar'
    if resolved != 'intrabar':
        raise ValueError("Only 'intrabar' execution mode is supported.")
    return 'intrabar'


def _execution_mode_label(mode: str) -> str:
    return (
        'Intrabar (minute bars)'
        if mode == 'intrabar'
        else 'Next-bar (completed hourly)'
    )


def _configure_windows_event_loop_policy() -> None:
    """Use the selector loop for aiohttp on Windows to avoid Proactor reset noise."""

    if sys.platform != 'win32':
        return
    selector_policy = getattr(asyncio, 'WindowsSelectorEventLoopPolicy', None)
    if selector_policy is None:
        return
    current_policy = asyncio.get_event_loop_policy()
    if isinstance(current_policy, selector_policy):
        return
    asyncio.set_event_loop_policy(selector_policy())


class LiveDashboardHub:
    """Own dashboard state, quote subscriptions, scan loop, and websocket fan-out."""

    def __init__(
        self,
        *,
        pairs,
        params,
        interval: int,
        zone_history_days: int,
        track_positions: bool,
        balance: float | None,
        risk_pct: float,
        account_currency: str | None,
        execute_orders: bool,
        strategy_label: str | None,
        client_id: int | None,
        port: int,
        execution_mode: str = 'intrabar',
        chart_tf: str = '1h',
        hourly_days: int = 1,
    ) -> None:
        from .strategy import is_pair_fully_blocked

        self._blocked_live_pairs = {
            pair_id: pair_info
            for pair_id, pair_info in pairs.items()
            if is_pair_fully_blocked(pair_id, params)
        }
        self.pairs = {
            pair_id: pair_info
            for pair_id, pair_info in pairs.items()
            if pair_id not in self._blocked_live_pairs
        }
        self.params = params
        self.interval = interval
        self.zone_history_days = zone_history_days
        self.track_positions = track_positions
        self.balance = balance
        self.risk_pct = risk_pct
        self.account_currency = account_currency
        self._execution_available = bool(execute_orders)
        self._execution_paused = False
        self.strategy_label = strategy_label
        self.client_id = client_id
        self.port = port
        self.execution_mode = _normalize_execution_mode(execution_mode)
        self.chart_tf = chart_tf
        self.hourly_days = hourly_days

        self._pair_rows: dict[str, PairScanRow] = {}
        self._tracked: dict[str, dict] = {}
        self._position_snapshots: dict[str, dict] = {}
        self._alerts: deque[dict] = deque(maxlen=ALERT_LIMIT)
        self._early_exit_active: dict[str, dict] = {}  # pair:direction -> alert dict, cleared when price recovers
        self._execution_results = deque(maxlen=EXECUTION_LIMIT)
        self._last_quotes: dict[str, float] = {}
        self._currency_balances: dict[str, float] = {}
        self._log: deque[dict] = deque(maxlen=LOG_LIMIT)
        self._active_signal_meta: dict[str, dict[str, str]] = {}

        self._clients: set[web.WebSocketResponse] = set()
        self._lock = asyncio.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._scan_task: Optional[asyncio.Task] = None
        self._fill_task: Optional[asyncio.Task] = None
        self._fill_lock = asyncio.Lock()
        self._backtest_task: Optional[asyncio.Task] = None
        self._backtest_lock = asyncio.Lock()
        self._fill_progress = {
            'status': 'idle',
            'items_requested': 0,
            'items_processed': 0,
            'attempts': 0,
            'errors': 0,
            'remaining': 0,
            'current_item': None,
            'message': 'No fill running.',
            'last_pct_reported': -1,
        }
        self._backtest_progress = {
            'status': 'idle',
            'items_requested': 0,
            'items_processed': 0,
            'current_item': None,
            'message': 'No backtest running.',
            'last_pct_reported': -1,
            'returncode': None,
        }
        self._scan_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix='ibkr-scan',
        )
        self._housekeeping_nudge = asyncio.Event()
        self._quote_stop = threading.Event()
        self._quote_thread: Optional[threading.Thread] = None
        self._pending_tasks: set[asyncio.Task] = set()
        self._scanner = StreamingScanner(
            pairs=pairs,
            params=params,
            zone_history_days=zone_history_days,
        )
        self._accumulator = HourlyBarAccumulator()
        self._tick_pending_pairs: set[str] = set()
        self._tick_exit_alerted: set[str] = set()
        self._minute_tracker: dict[str, int] = {}
        self._realtime_bars_enabled = False
        self._startup_bar_buffering = True
        self._startup_bar_sequence = 0
        self._startup_bar_buffer: list[tuple[pd.Timestamp, int, str, _BufferedRealtimeBar]] = []
        self._portfolio_state = build_portfolio_state([], params=params, current_balance=balance)
        self._daily_closed_pnl: float = 0.0
        self._backfill_done = False
        self._backfill_progress: dict = {
            'phase': 'waiting',
            'total': len(pairs),
            'completed': 0,
            'current_pair': None,
            'pair_status': {pair_id: 'pending' for pair_id in pairs},
        }

        self.summary = self._build_summary(status='starting')

    def _ws_url(self) -> str:
        return f'ws://127.0.0.1:{self.port}/ws'

    def _execution_enabled(self) -> bool:
        """Return True when the dashboard is allowed to submit new orders."""

        return self._execution_available and not self._execution_paused

    def _build_summary(self, *, status: str) -> dict:
        """Build the summary payload consumed by the dashboard shell."""

        pairs_total = len(self.pairs)
        return {
            'status': status,
            'pairs_total': pairs_total,
            'pairs_completed': len(self._pair_rows),
            'signal_count': 0,
            'pending_count': len(self._tick_pending_pairs),
            'pending_pairs': sorted(self._tick_pending_pairs),
            'position_count': len(self._tracked),
            'execution_enabled': self._execution_enabled(),
            'execution_available': self._execution_available,
            'execution_paused': self._execution_paused,
            'execution_mode': self.execution_mode,
            'execution_mode_label': _execution_mode_label(self.execution_mode),
            'strategy_label': self.strategy_label or 'Strategy',
            'mode': 'scanner + positions' if self.track_positions else 'scanner',
            'url': self._ws_url(),
            'backfill': dict(self._backfill_progress),
            'fill': dict(self._fill_progress),
            'backtest': dict(self._backtest_progress),
            'balance': self.balance,
            'account_currency': self.account_currency,
            'risk_pct': self.risk_pct * 100.0 if self.risk_pct is not None else None,
            'daily_closed_pnl': self._daily_closed_pnl,
        }

    def _startup_warm_cache_key(self) -> str:
        """Return the app-settings key for this startup scan configuration."""

        identity = {
            'version': _STARTUP_WARM_CACHE_VERSION,
            'pairs': sorted(self.pairs.keys()),
            'strategy_label': self.strategy_label,
            'execution_mode': self.execution_mode,
            'zone_history_days': self.zone_history_days,
            'hourly_days': self.hourly_days,
            'params': repr(self.params),
        }
        digest = hashlib.sha1(
            json.dumps(identity, sort_keys=True, default=str).encode('utf-8')
        ).hexdigest()
        return f'live_startup_warm:{digest}'

    @staticmethod
    def _frame_fingerprint(df: pd.DataFrame | None) -> dict:
        """Return a lightweight fingerprint for one cached OHLC frame."""

        if df is None or df.empty:
            return {'rows': 0, 'first': None, 'last': None, 'close': None}
        return {
            'rows': int(len(df)),
            'first': str(pd.Timestamp(df.index[0])),
            'last': str(pd.Timestamp(df.index[-1])),
            'close': float(df['Close'].iloc[-1]),
        }

    @staticmethod
    def _range_fingerprint(cached_range) -> dict:
        """Return a lightweight fingerprint for cached range metadata."""

        if cached_range is None:
            return {'rows': 0, 'first': None, 'last': None}
        first_ts, last_ts, rows = cached_range
        return {
            'rows': int(rows),
            'first': str(first_ts) if first_ts is not None else None,
            'last': str(last_ts) if last_ts is not None else None,
        }

    def _pair_startup_fingerprint(
        self,
        pair_id: str,
        ticker: str | None,
        daily_df: pd.DataFrame | None,
        hourly_df: pd.DataFrame | None,
    ) -> dict:
        """Build a strict-enough fingerprint for one pair's phase-3 inputs."""

        daily_range = get_cached_range(ticker, '1d') if ticker else None
        minute_range = get_cached_range(ticker, '1m') if ticker else None
        return {
            'pair': pair_id,
            'ticker': ticker,
            'zone_history_days': int(self.zone_history_days),
            'hourly_days': int(self.hourly_days),
            'execution_mode': self.execution_mode,
            'daily_range': self._range_fingerprint(daily_range),
            'hourly': self._frame_fingerprint(hourly_df),
            'minute_range': self._range_fingerprint(minute_range),
        }

    def _startup_resume_hourly_df(
        self,
        hourly_df: pd.DataFrame | None,
        cached_entry: dict | None,
    ) -> tuple[pd.DataFrame | None, bool]:
        """Return a reduced replay window when a prior warm artifact exists."""

        if hourly_df is None or hourly_df.empty or not cached_entry:
            return hourly_df, False

        cached_last = (
            cached_entry.get('fingerprint', {})
            .get('hourly', {})
            .get('last')
        )
        if not cached_last:
            return hourly_df, False

        try:
            last_ts = pd.Timestamp(cached_last)
        except Exception:
            return hourly_df, False

        if last_ts.tzinfo is None:
            last_ts = last_ts.tz_localize('UTC')
        else:
            last_ts = last_ts.tz_convert('UTC')

        tail_bars = max(
            int(getattr(self.params, 'scan_lookback_bars', 72) or 72),
            int(getattr(self.params, 'max_hold_bars', 72) or 72),
        ) + 4
        resume_start = last_ts - pd.Timedelta(hours=tail_bars)
        reduced = hourly_df[hourly_df.index >= resume_start]
        if reduced.empty or len(reduced) >= len(hourly_df):
            return hourly_df, False
        return reduced, True

    @staticmethod
    def _serialize_signal_artifact(signal) -> dict | None:
        """Serialize a Signal plus warm-start metadata."""

        if signal is None:
            return None
        payload = {
            'time': pd.Timestamp(signal.time).isoformat(),
            'pair': signal.pair,
            'direction': signal.direction,
            'entry_price': float(signal.entry_price),
            'sl_price': float(signal.sl_price),
            'tp_price': float(signal.tp_price),
            'zone_upper': float(signal.zone_upper),
            'zone_lower': float(signal.zone_lower),
            'zone_strength': signal.zone_strength,
            'zone_type': signal.zone_type,
            'quality_score': float(getattr(signal, 'quality_score', 0.0) or 0.0),
        }
        for key in ('_wf_exit_reason', '_wf_pnl_r', '_wf_exit_time'):
            value = getattr(signal, key, None)
            if value is None:
                continue
            payload[key] = (
                pd.Timestamp(value).isoformat()
                if key == '_wf_exit_time'
                else value
            )
        return payload

    @staticmethod
    def _deserialize_signal_artifact(payload: dict | None):
        """Restore a Signal from warm-start storage."""

        if not payload:
            return None
        from .strategy import Signal

        signal = Signal(
            time=pd.Timestamp(payload['time']),
            pair=str(payload['pair']),
            direction=str(payload['direction']),
            entry_price=float(payload['entry_price']),
            sl_price=float(payload['sl_price']),
            tp_price=float(payload['tp_price']),
            zone_upper=float(payload['zone_upper']),
            zone_lower=float(payload['zone_lower']),
            zone_strength=str(payload['zone_strength']),
            zone_type=str(payload['zone_type']),
            quality_score=float(payload.get('quality_score') or 0.0),
        )
        if payload.get('_wf_exit_reason') is not None:
            signal._wf_exit_reason = payload.get('_wf_exit_reason')
        if payload.get('_wf_pnl_r') is not None:
            signal._wf_pnl_r = payload.get('_wf_pnl_r')
        if payload.get('_wf_exit_time') is not None:
            signal._wf_exit_time = pd.Timestamp(payload['_wf_exit_time'])
        return signal

    def _serialize_pair_row_artifact(self, row: PairScanRow) -> dict:
        """Serialize a PairScanRow without lifecycle-only fields."""

        return {
            'pair': row.pair,
            'name': row.name,
            'decimals': int(row.decimals),
            'price': None if row.price is None else float(row.price),
            'state': row.state,
            'note': row.note,
            'support_text': row.support_text,
            'resistance_text': row.resistance_text,
            'support_lower': None if row.support_lower is None else float(row.support_lower),
            'support_upper': None if row.support_upper is None else float(row.support_upper),
            'support_strength': row.support_strength,
            'resistance_lower': None if row.resistance_lower is None else float(row.resistance_lower),
            'resistance_upper': None if row.resistance_upper is None else float(row.resistance_upper),
            'resistance_strength': row.resistance_strength,
            'support_dist_pct': None if row.support_dist_pct is None else float(row.support_dist_pct),
            'resistance_dist_pct': None if row.resistance_dist_pct is None else float(row.resistance_dist_pct),
        }

    def _deserialize_pair_row_artifact(self, payload: dict, signal) -> PairScanRow:
        """Restore a PairScanRow from warm-start storage."""

        return PairScanRow(
            pair=str(payload['pair']),
            name=str(payload['name']),
            decimals=int(payload['decimals']),
            price=payload.get('price'),
            state=str(payload['state']),
            note=str(payload.get('note') or ''),
            support_text=str(payload.get('support_text') or '-'),
            resistance_text=str(payload.get('resistance_text') or '-'),
            signal=signal,
            support_lower=payload.get('support_lower'),
            support_upper=payload.get('support_upper'),
            support_strength=payload.get('support_strength'),
            resistance_lower=payload.get('resistance_lower'),
            resistance_upper=payload.get('resistance_upper'),
            resistance_strength=payload.get('resistance_strength'),
            support_dist_pct=payload.get('support_dist_pct'),
            resistance_dist_pct=payload.get('resistance_dist_pct'),
        )

    def _load_startup_warm_entries(self) -> dict[str, dict]:
        """Load the last successful startup-warm artifact for this config."""

        candidate_rows = []

        for key in (_STARTUP_WARM_CACHE_ALIAS, self._startup_warm_cache_key()):
            row = get_setting(key)
            raw = row.get('value_text') if row is not None else None
            if raw:
                candidate_rows.append(raw)

        if not candidate_rows:
            try:
                conn = _connect(get_db_path())
                try:
                    row = conn.execute(
                        "SELECT value_text FROM app_settings "
                        "WHERE key LIKE %s "
                        "ORDER BY updated_at DESC LIMIT 1",
                        ('live_startup_warm:%',),
                    ).fetchone()
                finally:
                    conn.close()
                if row is not None and row[0]:
                    candidate_rows.append(row[0])
            except Exception:
                pass

        for raw in candidate_rows:
            try:
                payload = json.loads(raw)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if payload.get('version') != _STARTUP_WARM_CACHE_VERSION:
                continue
            entries = payload.get('pairs')
            if isinstance(entries, dict):
                return entries
        return {}

    def _save_startup_warm_entries(self, entries: dict[str, dict]) -> None:
        """Persist the latest successful startup-warm artifact."""

        payload = json.dumps(
            {'version': _STARTUP_WARM_CACHE_VERSION, 'pairs': entries},
            sort_keys=True,
            default=str,
        )
        set_setting(self._startup_warm_cache_key(), value_text=payload)
        set_setting(_STARTUP_WARM_CACHE_ALIAS, value_text=payload)

    async def _persist_pair_startup_warm(self, pair: str, wf_signals: list | None = None) -> None:
        """Persist one pair's current startup-warm entry."""

        async with self._lock:
            row = self._pair_rows.get(pair)
        if row is None:
            return

        signal = row.signal
        ticker = self.pairs.get(pair, {}).get('ticker')
        wf_payload = list(wf_signals or [])

        def _save() -> None:
            entries = self._load_startup_warm_entries()
            hourly_df = self._accumulator.get_hourly_df(pair)
            fingerprint = self._pair_startup_fingerprint(
                pair,
                ticker,
                None,
                hourly_df,
            )
            entries[pair] = {
                'fingerprint': fingerprint,
                'row': self._serialize_pair_row_artifact(row),
                'signal': self._serialize_signal_artifact(signal),
                'wf_signals': [self._serialize_signal_artifact(item) for item in wf_payload],
            }
            self._save_startup_warm_entries(entries)

        await self._loop.run_in_executor(self._scan_executor, _save)

    def _append_log(self, level: str, message: str) -> dict:
        """Append a structured log entry."""

        entry = {
            'ts': datetime.now(timezone.utc).strftime('%H:%M:%S'),
            'level': level,
            'message': message,
        }
        self._log.append(entry)
        return entry

    @staticmethod
    def _signal_identity(signal) -> str:
        """Return a stable identity for one active signal instance."""

        signal_time = pd.Timestamp(signal.time)
        if signal_time.tzinfo is None:
            signal_time = signal_time.tz_localize('UTC')
        else:
            signal_time = signal_time.tz_convert('UTC')
        return f'{signal.pair}:{signal.direction}:{signal_time.isoformat()}'

    @staticmethod
    def _normalize_signal_seen_at(seen_at=None) -> pd.Timestamp:
        """Normalize a signal lifecycle timestamp to UTC."""

        ts = pd.Timestamp.now(tz='UTC') if seen_at is None else pd.Timestamp(seen_at)
        if ts.tzinfo is None:
            return ts.tz_localize('UTC')
        return ts.tz_convert('UTC')

    def _mark_signal_valid(self, signal, *, seen_at=None) -> None:
        """Record when a signal first appeared and when it was last revalidated."""

        signal_id = self._signal_identity(signal)
        now_iso = self._normalize_signal_seen_at(seen_at).isoformat()
        current = self._active_signal_meta.get(signal.pair)
        if current is not None and current.get('signal_id') == signal_id:
            current['last_valid_at'] = now_iso
            return

        self._active_signal_meta[signal.pair] = {
            'signal_id': signal_id,
            'arrived_at': now_iso,
            'last_valid_at': now_iso,
        }

    def _clear_signal_tracking(self, pair: str) -> None:
        """Drop lifecycle metadata once a signal is no longer active."""

        self._active_signal_meta.pop(pair, None)

    def _sync_active_signal_tracking(self, rows: list[PairScanRow], *, seen_at=None) -> None:
        """Refresh lifecycle metadata from the current authoritative pair rows."""

        active_pairs: set[str] = set()
        for row in rows:
            if row.signal is None:
                continue
            self._mark_signal_valid(row.signal, seen_at=seen_at)
            active_pairs.add(row.pair)

        for pair in list(self._active_signal_meta):
            if pair not in active_pairs:
                self._clear_signal_tracking(pair)

    def _evaluate_pair_row(
        self,
        pair: str,
        *,
        tracked_positions: dict[str, dict],
        blocked_pairs: set[str],
        price: float,
        hourly_df,
    ) -> tuple[PairScanRow | None, object | None, list]:
        """Rebuild one pair row from authoritative scan inputs."""

        signals, rows, wf_signals = collect_scan_rows(
            pairs={pair: self.pairs[pair]},
            params=self.params,
            zone_history_days=self.zone_history_days,
            tracked_positions=tracked_positions,
            blocked_pairs=blocked_pairs,
            price_cache={pair: price},
            hourly_data_cache={self.pairs[pair]['ticker']: hourly_df},
            execution_mode=self.execution_mode,
            portfolio_state=self._portfolio_state,
            hourly_days=self.hourly_days,
        )
        return (
            rows[0] if rows else None,
            signals[0] if signals else None,
            wf_signals,
        )

    async def _broadcast_log(self, level: str, message: str) -> None:
        """Append a log entry and push it to connected clients."""

        entry = self._append_log(level, message)
        await self._broadcast({'type': 'log_entry', 'entry': entry})

    async def _publish_task_progress(
        self,
        *,
        task_key: str,
        event_type: str,
        status: str,
        items_requested: int,
        items_processed: int,
        current_item: str | None = None,
        message: str | None = None,
        attempts: int | None = None,
        errors: int | None = None,
        remaining: int | None = None,
        returncode: int | None = None,
        log_level: str = 'info',
    ) -> None:
        """Store task progress and notify dashboard clients."""

        if task_key == 'fill':
            progress = self._fill_progress
        elif task_key == 'backtest':
            progress = self._backtest_progress
        else:
            raise ValueError(f'Unknown task_key: {task_key}')

        async with self._lock:
            current_items_requested = max(int(items_requested), 0)
            current_items_processed = max(int(items_processed), 0)
            current_pct = (
                round((current_items_processed / current_items_requested) * 100)
                if current_items_requested > 0 else 0
            )
            previous_status = progress.get('status')
            previous_pct = int(progress.get('last_pct_reported', -1))
            should_log_progress = False

            if status == 'running' and current_items_requested > 0 and (
                current_pct > previous_pct or current_items_processed == current_items_requested
            ):
                should_log_progress = True
                progress['last_pct_reported'] = current_pct

            if status != 'running' and status != previous_status:
                should_log_progress = True

            task_message = message or progress.get('message', '')
            progress.update({
                'status': status,
                'items_requested': items_requested,
                'items_processed': items_processed,
                'current_item': current_item,
                'message': task_message,
            })
            if attempts is not None:
                progress['attempts'] = attempts
            if errors is not None:
                progress['errors'] = errors
            if remaining is not None:
                progress['remaining'] = remaining
            if task_key == 'backtest':
                progress['returncode'] = returncode
            elif returncode is not None:
                progress['returncode'] = returncode

            self.summary = self._build_summary(status=self.summary.get('status', 'starting'))
            summary = dict(self.summary)

        if should_log_progress and task_message:
            await self._broadcast_log(log_level, task_message)

        await self._broadcast({'type': event_type, 'summary': summary})

    async def _publish_fill_progress(
        self,
        *,
        status: str,
        items_requested: int,
        items_processed: int,
        attempts: int,
        errors: int,
        remaining: int,
        current_item: str | None = None,
        message: str | None = None,
    ) -> None:
        """Store fill progress and notify dashboard clients."""

        await self._publish_task_progress(
            task_key='fill',
            event_type='fill_progress',
            status=status,
            items_requested=items_requested,
            items_processed=items_processed,
            attempts=attempts,
            errors=errors,
            remaining=remaining,
            current_item=current_item,
            message=message,
        )

    async def _publish_backtest_progress(
        self,
        *,
        status: str,
        items_requested: int,
        items_processed: int,
        current_item: str | None = None,
        returncode: int | None = None,
        message: str | None = None,
    ) -> None:
        """Store backtest progress and notify dashboard clients."""

        await self._publish_task_progress(
            task_key='backtest',
            event_type='backtest_progress',
            status=status,
            items_requested=items_requested,
            items_processed=items_processed,
            current_item=current_item,
            returncode=returncode,
            message=message,
            log_level='error' if status == 'error' else 'info',
        )

    def _backtest_client_id_base(self) -> int:
        """Return a client-id base dedicated to full backtest reruns."""

        base_client_id = int(self.client_id if self.client_id is not None else ibkr.TWS_CLIENT_ID)
        if base_client_id == 60:
            base_client_id += 1000
        return base_client_id + 3000

    def _build_backtest_cli_args(self) -> list[str]:
        """Build `python run.py backtest ...` arguments that mirror dashboard params."""

        args: list[str] = []

        args.extend(['--ibkr-client-id', str(self._backtest_client_id_base())])

        args.extend(['--execution-mode', 'intrabar'])
        args.extend(['--days', str(self.hourly_days)])
        if self.zone_history_days:
            args.extend(['--zone-history', str(self.zone_history_days)])

        if self.balance is not None:
            args.extend(['--balance', str(self.balance)])
        if self.risk_pct is not None:
            args.extend(['--risk-pct', str(self.risk_pct * 100.0)])

        if self.params:
            args.extend(['--rr-ratio', str(self.params.rr_ratio)])
            args.extend(['--sl-buffer', str(self.params.sl_buffer_pct)])
            args.extend(['--early-exit', str(self.params.early_exit_r)])
            args.extend(['--cooldown-bars', str(self.params.cooldown_bars)])
            args.extend(['--min-entry-body', str(self.params.min_entry_candle_body_pct)])
            args.extend(['--momentum-lookback', str(self.params.momentum_lookback)])
            args.extend(['--max-correlated-trades', str(self.params.max_correlated_trades)])
            args.extend(['--spread-pips', str(self.params.spread_pips)])
            args.extend(['--stop-slippage-pips', str(self.params.stop_slippage_pips)])
            if not self.params.use_time_filters:
                args.append('--no-time-filters')
            if not self.params.use_pair_direction_filter:
                args.append('--no-pair-direction-filter')
            args.append('--blocked-hours')
            args.extend([str(int(h)) for h in sorted(self.params.blocked_hours)])
            args.append('--blocked-days')
            args.extend([str(int(d)) for d in sorted(self.params.blocked_days)])

        pair_ids = sorted(self.pairs.keys())
        if len(pair_ids) == 1:
            args.extend(['--pair', pair_ids[0]])

        return args

    def _parse_backtest_line(self, line: str) -> tuple[int, int, str] | None:
        """Extract [done/total] and current pair from a progress line."""

        match = _BACKTEST_PROGRESS_RE.match(line or '')
        if not match:
            return None
        current = int(match.group(1))
        total = int(match.group(2))
        pair = match.group(3)
        return current, total, pair

    async def _run_backtest_task(self) -> dict[str, object]:
        """Run the full backtest pipeline as an async subprocess task."""

        command = [sys.executable, str(RUN_PY_PATH), 'backtest', *self._build_backtest_cli_args()]
        await self._publish_backtest_progress(
            status='starting',
            items_requested=0,
            items_processed=0,
            message='Starting backtest rerun via CLI.',
        )

        process = None
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                cwd=str(RUN_PY_PATH.parent),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except Exception as exc:
            return {
                'status': 'error',
                'items_requested': 0,
                'items_processed': 0,
                'returncode': 1,
                'message': f'Unable to launch backtest: {exc}',
            }

        items_requested = 0
        items_processed = 0
        current_item = None

        try:
            await self._publish_backtest_progress(
                status='running',
                items_requested=items_requested,
                items_processed=items_processed,
                current_item=current_item,
                message='Backtest running; waiting for progress...',
            )
            assert process.stdout is not None
            while True:
                raw_line = await process.stdout.readline()
                if not raw_line:
                    break
                line = raw_line.decode('utf-8', errors='replace').strip()
                if not line:
                    continue
                parsed = self._parse_backtest_line(line)
                if parsed is not None:
                    done, total, pair = parsed
                    items_requested = total
                    items_processed = done
                    current_item = pair
                    await self._publish_backtest_progress(
                        status='running',
                        items_requested=items_requested,
                        items_processed=items_processed,
                        current_item=current_item,
                        message=line,
                    )
                else:
                    for fallback in (
                        'Completed in',
                        'Unable to fetch',
                    ):
                        if line.startswith(fallback):
                            await self._publish_backtest_progress(
                                status='running',
                                items_requested=items_requested,
                                items_processed=items_processed,
                                current_item=current_item,
                                message=line,
                            )
                            break

            returncode = await process.wait()
            if returncode == 0:
                status = 'complete'
                message = f'Backtest rerun complete. Processed {items_processed}/{items_requested} pair(s).'
                level = 'success'
            else:
                status = 'error'
                message = f'Backtest rerun failed with return code {returncode}.'
                level = 'error'
            await self._publish_backtest_progress(
                status=status,
                items_requested=items_requested,
                items_processed=items_processed,
                current_item=current_item,
                returncode=returncode,
                message=message,
            )
            return {
                'status': status,
                'items_requested': items_requested,
                'items_processed': items_processed,
                'returncode': returncode,
                'message': message,
            }
        except asyncio.CancelledError:
            if process.returncode is None:
                process.kill()
                await process.wait()
            await self._publish_backtest_progress(
                status='canceled',
                items_requested=items_requested,
                items_processed=items_processed,
                current_item=current_item,
                message='Backtest rerun canceled.',
            )
            return {
                'status': 'canceled',
                'items_requested': items_requested,
                'items_processed': items_processed,
                'returncode': process.returncode if process else None,
                'message': 'Backtest rerun canceled.',
            }
        except Exception as exc:
            if process is not None and process.returncode is None:
                process.kill()
                await process.wait()
            error_message = f'Backtest rerun failed: {exc}'
            await self._publish_backtest_progress(
                status='error',
                items_requested=items_requested,
                items_processed=items_processed,
                current_item=current_item,
                message=error_message,
                returncode=process.returncode if process else 1,
            )
            return {
                'status': 'error',
                'items_requested': items_requested,
                'items_processed': items_processed,
                'returncode': process.returncode if process else 1,
                'message': error_message,
            }

    async def _on_backtest_done(self, task: asyncio.Task) -> None:
        """Finalize backtest-task state and emit summary logs."""

        self._pending_tasks.discard(task)
        async with self._backtest_lock:
            if self._backtest_task is task:
                self._backtest_task = None

        try:
            result = task.result()
        except asyncio.CancelledError:
            await self._publish_backtest_progress(
                status='canceled',
                items_requested=self._backtest_progress.get('items_requested', 0),
                items_processed=self._backtest_progress.get('items_processed', 0),
                message='Backtest rerun canceled.',
            )
            await self._broadcast_log('warning', 'Backtest rerun canceled.')
            return
        except Exception as exc:
            await self._publish_backtest_progress(
                status='error',
                items_requested=self._backtest_progress.get('items_requested', 0),
                items_processed=self._backtest_progress.get('items_processed', 0),
                message=f'Backtest rerun failed: {exc}',
            )
            await self._broadcast_log('error', f'Backtest rerun failed: {exc}')
            return

        status = result.get('status', 'incomplete')
        message = result.get('message', 'Backtest rerun finished.')
        if status == 'complete':
            await self._broadcast_log('success', message)
        elif status == 'running':
            await self._broadcast_log('warning', message)
        else:
            await self._broadcast_log('warning', message)

    async def run_backtest(self) -> dict[str, object]:
        """Kick off a full backtest rerun in a background worker."""

        async with self._backtest_lock:
            if self._backtest_task is not None and not self._backtest_task.done():
                return {
                    'status': 'running',
                    'message': 'Backtest rerun already in progress.',
                    'items_requested': 0,
                    'items_processed': 0,
                    'returncode': None,
                }

            self._backtest_task = asyncio.create_task(self._run_backtest_task())
            backtest_task = self._backtest_task
            self._pending_tasks.add(backtest_task)
            backtest_task.add_done_callback(
                lambda task: asyncio.create_task(self._on_backtest_done(task))
            )

        await self._broadcast_log('info', 'Backtest rerun requested.')
        return {
            'status': 'started',
            'message': 'Backtest rerun started in background.',
            'items_requested': 0,
            'items_processed': 0,
            'returncode': None,
        }

    def _fill_client_id_base(self) -> int:
        """Return a client-id base dedicated to cache fills."""

        base_client_id = int(self.client_id if self.client_id is not None else ibkr.TWS_CLIENT_ID)
        if base_client_id == 60:
            base_client_id += 1000
        return base_client_id + 2000

    def _backfill_client_id_base(self) -> int:
        """Return a client-id base dedicated to startup backfill."""

        base_client_id = int(self.client_id if self.client_id is not None else ibkr.TWS_CLIENT_ID)
        if base_client_id == 60:
            base_client_id += 1000
        return base_client_id + 4000

    def _find_fill_gaps(self, target_days: int) -> list[tuple[str, dict, str, pd.Timestamp | None]]:
        """Find cache gaps for supported intervals."""

        if target_days <= 0:
            return []

        min_rows = {
            '1d': int(target_days * 0.7),
            '1h': int(target_days * 16),
            '1m': int(target_days * 1000),
        }
        now = pd.Timestamp.now(tz='UTC')
        gaps: list[tuple[str, dict, str, pd.Timestamp | None]] = []

        for pair_id, pair_info in self.pairs.items():
            ticker = pair_info['ticker']
            for interval in ('1d', '1h', '1m'):
                cached_range = get_cached_range(ticker, interval)
                gap_start = find_first_missing_cached_bar(
                    ticker,
                    interval,
                    now=now,
                )
                if cached_range is None:
                    gaps.append((pair_id, pair_info, interval, gap_start))
                    continue

                first_ts, last_ts, rows = cached_range
                if int(rows) < min_rows.get(interval, 0):
                    gaps.append((pair_id, pair_info, interval, gap_start))
                    continue

                fetch_days = _remaining_days_to_fetch(
                    interval=interval,
                    requested_days=target_days,
                    cached_range=(
                        first_ts,
                        last_ts,
                        int(rows),
                    ),
                    now=now,
                )
                if fetch_days > 0 or gap_start is not None:
                    gaps.append((pair_id, pair_info, interval, gap_start))

        return gaps

    async def _run_fill_task(self, target_days: int) -> dict[str, object]:
        """Run cache fill work and return a compact status payload."""

        init_db()
        gaps = self._find_fill_gaps(target_days)
        if not gaps:
            await self._publish_fill_progress(
                status='complete',
                items_requested=0,
                items_processed=0,
                attempts=0,
                errors=0,
                remaining=0,
                message='No cache gaps detected.',
            )
            return {
                'status': 'complete',
                'items_processed': 0,
                'items_requested': 0,
                'attempts': 0,
                'errors': 0,
                'message': 'No cache gaps detected.',
            }

        work_items = [(pair_id, pair_info, interval, gap_start) for pair_id, pair_info, interval, gap_start in gaps]
        max_workers = min(3, len(work_items))
        base_fill_client_id = self._fill_client_id_base()
        max_retries = 3
        slot_lock = threading.Lock()
        client_slots = [0]

        def _thread_client_id() -> int:
            thread = threading.current_thread()
            slot = getattr(thread, '_fill_client_id_slot', None)
            if slot is None:
                with slot_lock:
                    slot = client_slots[0]
                    client_slots[0] += 1
                thread._fill_client_id_slot = slot
            return base_fill_client_id + int(slot)

        def _run_work_item(pair_id: str, pair_info: dict, interval: str, gap_start: pd.Timestamp | None) -> int:
            client_id = _thread_client_id()
            if gap_start is not None:
                return len(
                    refill_interval_from(
                        pair_info['ticker'],
                        interval,
                        gap_start,
                        client_id=client_id,
                    )
                )
            return download_single_interval(pair_id, pair_info, interval, target_days, client_id=client_id)

        pending = list(work_items)
        total_errors = 0
        attempt = 0
        total_items_processed = 0
        total_attempted = len(work_items)

        await self._publish_fill_progress(
            status='running',
            items_requested=total_attempted,
            items_processed=0,
            attempts=0,
            errors=0,
            remaining=total_attempted,
            message=f'Fill started ({total_attempted} items).',
        )

        while pending and attempt < max_retries:
            attempt += 1
            if attempt > 1:
                await asyncio.sleep(5)

            failed: list[tuple[str, dict, str]] = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(_run_work_item, pair_id, pair_info, interval, gap_start): (pair_id, pair_info, interval, gap_start)
                    for pair_id, pair_info, interval, gap_start in pending
                }

                for future in as_completed(futures):
                    pair_id, pair_info, interval, gap_start = futures[future]
                    try:
                        _ = future.result()
                        total_items_processed += 1
                    except Exception:
                        failed.append((pair_id, pair_info, interval, gap_start))
                        total_errors += 1
                    await self._publish_fill_progress(
                        status='running',
                        items_requested=total_attempted,
                        items_processed=total_items_processed,
                        attempts=attempt,
                        errors=total_errors,
                        remaining=max(total_attempted - total_items_processed, 0),
                        current_item=f'{pair_id}:{interval}',
                        message=f'Fill attempt {attempt}/{max_retries}: {total_items_processed}/{total_attempted} complete.',
                    )

            pending = failed
            await self._publish_fill_progress(
                status='running',
                items_requested=total_attempted,
                items_processed=total_items_processed,
                attempts=attempt,
                errors=total_errors,
                remaining=len(pending),
                current_item=None,
                message=f'Fill attempt {attempt}/{max_retries} complete. Remaining: {len(pending)}.',
            )

        status = 'incomplete' if pending else 'complete'
        final_message = (
            f'Fill {status} in {attempt} attempt(s). '
            f'Processed {total_items_processed}/{total_attempted} item(s), '
            f'errors: {total_errors}, remaining: {len(pending)}.'
        )
        await self._publish_fill_progress(
            status=status,
            items_requested=total_attempted,
            items_processed=total_items_processed,
            attempts=attempt,
            errors=total_errors,
            remaining=len(pending),
            message=final_message,
        )
        return {
            'status': status,
            'items_processed': total_items_processed,
            'items_requested': total_attempted,
            'attempts': attempt,
            'errors': total_errors,
            'remaining': len(pending),
            'message': final_message,
        }

    async def _on_fill_done(self, task: asyncio.Task) -> None:
        """Finalize fill-task state and emit summary logs."""

        self._pending_tasks.discard(task)
        async with self._fill_lock:
            if self._fill_task is task:
                self._fill_task = None

        try:
            result = task.result()
        except asyncio.CancelledError:
            await self._publish_fill_progress(
                status='canceled',
                items_requested=self._fill_progress.get('items_requested', 0),
                items_processed=self._fill_progress.get('items_processed', 0),
                attempts=self._fill_progress.get('attempts', 0),
                errors=self._fill_progress.get('errors', 0),
                remaining=self._fill_progress.get('remaining', 0),
                message='Cache fill canceled.',
            )
            await self._broadcast_log('warning', 'Cache fill canceled.')
            return
        except Exception as exc:
            await self._publish_fill_progress(
                status='error',
                items_requested=self._fill_progress.get('items_requested', 0),
                items_processed=self._fill_progress.get('items_processed', 0),
                attempts=self._fill_progress.get('attempts', 0),
                errors=self._fill_progress.get('errors', 0),
                remaining=self._fill_progress.get('remaining', 0),
                message=f'Cache fill failed: {exc}',
            )
            await self._broadcast_log('error', f'Cache fill failed: {exc}')
            return

        status = result.get('status', 'incomplete')
        remaining = int(result.get('remaining', 0))
        if status == 'complete' and remaining == 0:
            level = 'success'
            message = result.get('message', 'Cache fill complete.')
        elif status == 'running':
            level = 'warning'
            message = result.get('message', 'Cache fill already running.')
        else:
            level = 'warning'
            message = result.get(
                'message',
                'Cache fill ended with remaining gaps. Consider re-running.',
            )
        await self._broadcast_log(level, message)

    async def fill_cache(self, *, target_days: int) -> dict[str, object]:
        """Kick off a cache-fill run in a background worker and return status."""

        if target_days <= 0:
            return {
                'status': 'invalid',
                'message': 'target_days must be greater than 0',
                'items_requested': 0,
                'items_processed': 0,
                'attempts': 0,
                'errors': 0,
                'remaining': 0,
            }

        async with self._fill_lock:
            if self._fill_task is not None and not self._fill_task.done():
                return {
                    'status': 'running',
                    'message': 'Cache fill already in progress.',
                    'items_requested': 0,
                    'items_processed': 0,
                    'attempts': 0,
                    'errors': 0,
                    'remaining': 0,
                }

            self._fill_task = asyncio.create_task(self._run_fill_task(target_days))
            fill_task = self._fill_task
            self._pending_tasks.add(fill_task)
            fill_task.add_done_callback(
                lambda task: asyncio.create_task(self._on_fill_done(task))
            )

        await self._broadcast_log('info', f'Cache fill requested for {target_days} day(s).')
        return {
            'status': 'started',
            'message': 'Cache fill started in background.',
            'items_requested': 0,
            'items_processed': 0,
            'attempts': 0,
            'errors': 0,
            'remaining': 0,
        }

    def _serialize_signal(self, signal, size_plan) -> dict:
        """Serialize a signal for the browser."""

        pair_info = self.pairs.get(signal.pair, {})
        signal_id = self._signal_identity(signal)
        lifecycle = self._active_signal_meta.get(signal.pair, {})
        payload = {
            'time': signal.time.isoformat(),
            'pair': signal.pair,
            'direction': signal.direction,
            'entry_price': signal.entry_price,
            'sl_price': signal.sl_price,
            'tp_price': signal.tp_price,
            'zone_upper': signal.zone_upper,
            'zone_lower': signal.zone_lower,
            'zone_strength': signal.zone_strength,
            'zone_type': signal.zone_type,
            'decimals': pair_info.get('decimals', 5),
            'arrived_at': (
                lifecycle.get('arrived_at')
                if lifecycle.get('signal_id') == signal_id
                else None
            ),
            'last_valid_at': (
                lifecycle.get('last_valid_at')
                if lifecycle.get('signal_id') == signal_id
                else None
            ),
        }
        if size_plan is not None:
            payload['size_plan'] = {
                'pair': size_plan.pair,
                'direction': size_plan.direction,
                'units': size_plan.units,
                'risk_amount': size_plan.risk_amount,
                'risk_pct': size_plan.risk_pct,
                'balance': size_plan.balance,
                'account_currency': size_plan.account_currency,
                'risk_per_unit_account': size_plan.risk_per_unit_account,
                'notional_account': size_plan.notional_account,
            }
        else:
            payload['size_plan'] = None
        return payload

    def _serialize_pair_row(self, row: PairScanRow) -> dict:
        """Serialize a watchlist row for the browser."""

        payload = {
            'pair': row.pair,
            'name': row.name,
            'decimals': row.decimals,
            'price': row.price,
            'state': row.state,
            'note': row.note,
            'support_text': row.support_text,
            'resistance_text': row.resistance_text,
            'support_lower': row.support_lower,
            'support_upper': row.support_upper,
            'support_strength': row.support_strength,
            'resistance_lower': row.resistance_lower,
            'resistance_upper': row.resistance_upper,
            'resistance_strength': row.resistance_strength,
            'support_dist_pct': row.support_dist_pct,
            'resistance_dist_pct': row.resistance_dist_pct,
            'signal': None,
        }
        if row.signal is not None:
            payload['signal'] = self._serialize_signal(row.signal, None)
        return payload

    def _serialize_positions(self) -> list[dict]:
        """Serialize tracked positions with their latest live snapshot."""

        alert_lookup = {
            f"{alert['pair']}:{alert['direction']}": alert['exit_reason']
            for alert in self._alerts
        }
        from .sizing import split_pair, convert_currency
        account_currency = (self.account_currency or 'GBP').upper()

        def _price_lookup(pair_id: str):
            # Try live quotes first, then fall back to last known bar close
            price = self._last_quotes.get(pair_id)
            if price is not None:
                return price
            # Fallback: last hourly bar close from accumulator
            df = self._accumulator.get_hourly_df(pair_id, tail_n=1)
            if df is not None and not df.empty:
                return float(df['Close'].iloc[-1])
            return None

        rows: list[dict] = []
        for key in sorted(self._tracked):
            info = self._tracked[key]
            trade = info['trade']
            snap = self._position_snapshots.get(key, {})
            pair = info['pair']
            status = alert_lookup.get(key)
            if status is None:
                status = 'PARTIAL' if info.get('signal_status') == 'PARTIAL' else 'OK'

            size = int(abs(info.get('ibkr_size') or 0))
            if size == 0 and 'ibkr_size' in info:
                continue
            current_price = snap.get('current_price')
            pnl_amount = None
            if current_price is not None and size > 0:
                _, quote = split_pair(pair)
                if trade.direction == 'LONG':
                    pnl_quote = (float(current_price) - float(trade.entry_price)) * size
                else:
                    pnl_quote = (float(trade.entry_price) - float(current_price)) * size
                pnl_account = convert_currency(
                    abs(pnl_quote), from_currency=quote,
                    to_currency=account_currency, price_lookup=_price_lookup,
                )
                if pnl_account is not None:
                    pnl_amount = round(pnl_account if pnl_quote >= 0 else -pnl_account, 2)

            rows.append(
                {
                    'pair': pair,
                    'direction': trade.direction,
                    'size': size,
                    'signal_id': info.get('signal_id'),
                    'entry_price': trade.entry_price,
                    'entry_time': pd.Timestamp(trade.entry_time).isoformat()
                    if getattr(trade, 'entry_time', None) is not None
                    else None,
                    'sl_price': trade.sl_price,
                    'tp_price': trade.tp_price,
                    'current_price': current_price,
                    'pnl_pips': snap.get('pnl_pips'),
                    'pnl_amount': pnl_amount,
                    'account_currency': account_currency,
                    'status': status,
                    'decimals': self.pairs.get(pair, {}).get('decimals', 5),
                    'is_remainder': getattr(trade, 'is_remainder', False),
                    'position_fraction': getattr(trade, 'position_fraction', 1.0),
                    'trade_group_id': getattr(trade, 'trade_group_id', None),
                    'sl_at_breakeven': (
                        getattr(trade, 'is_remainder', False)
                        and abs(trade.sl_price - trade.entry_price) < abs(trade.entry_price * 0.0001)
                    ),
                }
            )
        return rows

    def _serialize_alerts(self) -> list[dict]:
        """Serialize exit alerts.

        Early exits are dynamic (present/absent based on live price).
        TP/SL are historical events kept in the deque.
        """

        rows = []
        # Dynamic early exits first so they appear at the top
        for alert in self._early_exit_active.values():
            rows.append({**alert, 'decimals': self.pairs.get(alert['pair'], {}).get('decimals', 5)})
        for alert in self._alerts:
            rows.append({**alert, 'decimals': self.pairs.get(alert['pair'], {}).get('decimals', 5)})
        return rows

    def _serialize_executions(self) -> list[dict]:
        """Serialize execution results."""

        rows = []
        for result in self._execution_results:
            risk_pips = None
            if (
                result.submitted_entry_price is not None
                and result.submitted_sl_price is not None
            ):
                pip = pair_pip(result.pair)
                if pip > 0:
                    risk_pips = abs(result.submitted_entry_price - result.submitted_sl_price) / pip

            pnl_pips = result.pnl_pips
            if pnl_pips is None and result.closed_price is not None:
                direction = result.direction.upper()
                if result.submitted_entry_price is not None:
                    pip = pair_pip(result.pair)
                    if pip > 0:
                        try:
                            if direction == 'LONG':
                                pnl_pips = (float(result.closed_price) - float(result.submitted_entry_price)) / pip
                            else:
                                pnl_pips = (float(result.submitted_entry_price) - float(result.closed_price)) / pip
                        except (TypeError, ValueError):
                            pnl_pips = None

            pnl_r = None
            if risk_pips and risk_pips > 0 and (pnl_pips is not None):
                try:
                    pnl_r = float(pnl_pips) / risk_pips
                except (TypeError, ValueError):
                    pnl_r = None

            rows.append(
                {
                    'pair': result.pair,
                    'direction': result.direction,
                    'units': result.units,
                    'status': result.status,
                    'order_id': result.order_id,
                    'submitted_entry_price': result.submitted_entry_price,
                    'submitted_tp_price': result.submitted_tp_price,
                    'submitted_sl_price': result.submitted_sl_price,
                    'time': (
                        pd.Timestamp(result.quote_time).isoformat()
                        if result.quote_time is not None
                        else None
                    ),
                    'note': result.note,
                    'pnl_pips': pnl_pips,
                    'pnl_r': pnl_r,
                    'pnl_amount': round(float(getattr(result, 'risk_amount', 0) or 0) * pnl_r, 2) if pnl_r is not None and getattr(result, 'risk_amount', None) else None,
                    'risk_amount': getattr(result, 'risk_amount', None),
                    'account_currency': getattr(result, 'account_currency', None) or self.account_currency or 'GBP',
                    'closed_price': result.closed_price,
                    'closed_at': (
                        pd.Timestamp(result.closed_at).isoformat()
                        if result.closed_at is not None
                        else None
                    ),
                    'close_reason': result.close_reason,
                }
            )
        return rows

    @staticmethod
    def _as_float(value) -> float | None:
        if value is None or value == '':
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _as_int(value) -> int | None:
        if value is None or value == '':
            return None
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None

    def _build_closed_execution_result(self, closed_row: dict) -> ExecutionResult | None:
        pair = str(closed_row.get('pair') or '').upper()
        direction = str(closed_row.get('direction') or '').upper()
        if not pair or direction not in {'LONG', 'SHORT'}:
            return None

        units = self._as_int(closed_row.get('open_units'))
        if units is None:
            units = self._as_int(closed_row.get('planned_units')) or 0
        if units < 0:
            units = abs(units)

        return ExecutionResult(
            pair=pair,
            direction=direction,
            units=units,
            status='CLOSED',
            order_id=self._as_int(closed_row.get('order_id')),
            take_profit_order_id=self._as_int(closed_row.get('take_profit_order_id')),
            stop_loss_order_id=self._as_int(closed_row.get('stop_loss_order_id')),
            avg_fill_price=self._as_float(closed_row.get('opened_price')),
            remaining_units=0,
            broker_status=closed_row.get('broker_order_status'),
            submitted_entry_price=self._as_float(
                closed_row.get('submitted_entry_price')
                if closed_row.get('submitted_entry_price') is not None
                else closed_row.get('entry_price'),
            ),
            submitted_tp_price=self._as_float(closed_row.get('submitted_tp_price')),
            submitted_sl_price=self._as_float(
                closed_row.get('submitted_sl_price')
                if closed_row.get('submitted_sl_price') is not None
                else closed_row.get('sl_price'),
            ),
            quote_time=(
                pd.Timestamp(closed_row['closed_at'])
                if closed_row.get('closed_at') is not None
                else pd.Timestamp.now('UTC')
            ),
            pnl_pips=self._as_float(closed_row.get('pnl_pips')),
            closed_price=self._as_float(closed_row.get('closed_price')),
            closed_at=(
                pd.Timestamp(closed_row['closed_at'])
                if closed_row.get('closed_at') is not None
                else None
            ),
            close_reason=str(closed_row.get('close_reason') or closed_row.get('close_source') or ''),
            note=str(closed_row.get('note') or ''),
        )

    def _append_or_merge_execution_result(self, result: ExecutionResult) -> None:
        open_statuses = {'SUBMITTED', 'PRESUBMITTED', 'PARTIAL', 'OPEN', 'FILLED'}
        for idx in range(len(self._execution_results) - 1, -1, -1):
            existing = self._execution_results[idx]
            if existing.pair != result.pair or existing.direction != result.direction:
                continue
            if existing.status in open_statuses or existing.status == 'EXIT_SIGNAL':
                if result.status == 'CLOSED':
                    self._execution_results[idx] = replace(
                        existing,
                        status=result.status,
                        order_id=result.order_id or existing.order_id,
                        take_profit_order_id=result.take_profit_order_id or existing.take_profit_order_id,
                        stop_loss_order_id=result.stop_loss_order_id or existing.stop_loss_order_id,
                        avg_fill_price=result.avg_fill_price or existing.avg_fill_price,
                        broker_status=result.broker_status or existing.broker_status,
                        submitted_entry_price=result.submitted_entry_price or existing.submitted_entry_price,
                        submitted_tp_price=result.submitted_tp_price or existing.submitted_tp_price,
                        submitted_sl_price=result.submitted_sl_price or existing.submitted_sl_price,
                        quote_time=result.quote_time or existing.quote_time,
                        pnl_pips=result.pnl_pips if result.pnl_pips is not None else existing.pnl_pips,
                        closed_price=result.closed_price if result.closed_price is not None else existing.closed_price,
                        closed_at=result.closed_at or existing.closed_at,
                        close_reason=result.close_reason or existing.close_reason,
                        note=result.note or existing.note,
                    )
                    return
            if result.order_id is not None and existing.order_id == result.order_id:
                self._execution_results[idx] = replace(
                    existing,
                    status=result.status,
                    order_id=result.order_id,
                    take_profit_order_id=result.take_profit_order_id or existing.take_profit_order_id,
                    stop_loss_order_id=result.stop_loss_order_id or existing.stop_loss_order_id,
                    avg_fill_price=result.avg_fill_price or existing.avg_fill_price,
                    remaining_units=result.remaining_units or existing.remaining_units,
                    broker_status=result.broker_status or existing.broker_status,
                    submitted_entry_price=result.submitted_entry_price or existing.submitted_entry_price,
                    submitted_tp_price=result.submitted_tp_price or existing.submitted_tp_price,
                    submitted_sl_price=result.submitted_sl_price or existing.submitted_sl_price,
                    quote_time=result.quote_time or existing.quote_time,
                    pnl_pips=result.pnl_pips if result.pnl_pips is not None else existing.pnl_pips,
                    closed_price=result.closed_price if result.closed_price is not None else existing.closed_price,
                    closed_at=result.closed_at or existing.closed_at,
                    close_reason=result.close_reason or existing.close_reason,
                    note=result.note or existing.note,
                )
                return

            if existing.status == 'CLOSED':
                break

        self._execution_results.append(result)

    def _hydrate_execution_activity(self) -> None:
        """Restore recent execution activity from the detected-signal history table."""

        rows = load_execution_activity(limit=EXECUTION_LIMIT)
        self._execution_results.clear()
        pending_pairs: set[str] = set()
        active_statuses = {'SUBMITTED', 'PRESUBMITTED', 'FILLED', 'PARTIAL', 'OPEN', 'EXIT_SIGNAL'}

        for row in reversed(rows):
            units_value = row.get('planned_units')
            if units_value in (None, ''):
                units_value = row.get('open_units')
            units = int(abs(float(units_value or 0)))
            order_id = row.get('order_id')
            status = str(row.get('status') or '').upper() or 'UNKNOWN'
            if row.get('pair') and order_id is not None and not row.get('closed_at') and status in active_statuses:
                pending_pairs.add(str(row['pair']))
            self._execution_results.append(
                ExecutionResult(
                    pair=str(row.get('pair') or ''),
                    direction=str(row.get('direction') or ''),
                    units=units,
                    status=status,
                    order_id=int(order_id) if order_id is not None else None,
                    take_profit_order_id=(
                        int(row['take_profit_order_id'])
                        if row.get('take_profit_order_id') is not None
                        else None
                    ),
                    stop_loss_order_id=(
                        int(row['stop_loss_order_id'])
                        if row.get('stop_loss_order_id') is not None
                        else None
                    ),
                    avg_fill_price=(
                        float(row['opened_price'])
                        if row.get('opened_price') is not None
                        else None
                    ),
                    filled_units=(
                        int(abs(float(row['open_units'])))
                        if row.get('open_units') is not None
                        else None
                    ),
                    remaining_units=(
                        int(abs(float(row['remaining_units'])))
                        if row.get('remaining_units') is not None
                        else None
                    ),
                    broker_status=(
                        str(row.get('broker_order_status'))
                        if row.get('broker_order_status') is not None
                        else None
                    ),
                    submitted_entry_price=(
                        float(row['submitted_entry_price'])
                        if row.get('submitted_entry_price') is not None
                        else None
                    ),
                    submitted_tp_price=(
                        float(row['submitted_tp_price'])
                        if row.get('submitted_tp_price') is not None
                        else None
                    ),
                    submitted_sl_price=(
                        float(row['submitted_sl_price'])
                        if row.get('submitted_sl_price') is not None
                        else None
                    ),
                    submit_bid=(
                        float(row['submit_bid'])
                        if row.get('submit_bid') is not None
                        else None
                    ),
                    submit_ask=(
                        float(row['submit_ask'])
                        if row.get('submit_ask') is not None
                        else None
                    ),
                    submit_spread=(
                        float(row['submit_spread'])
                        if row.get('submit_spread') is not None
                        else None
                    ),
                    pnl_pips=(
                        float(row['pnl_pips'])
                        if row.get('pnl_pips') is not None
                        else None
                    ),
                    closed_price=(
                        float(row['closed_price'])
                        if row.get('closed_price') is not None
                        else None
                    ),
                    quote_source=(
                        str(row.get('quote_source'))
                        if row.get('quote_source') is not None
                        else None
                    ),
                    close_reason=(
                        str(row.get('close_reason'))
                        if row.get('close_reason') is not None
                        else None
                    ),
                    closed_at=(
                        pd.Timestamp(row['closed_at'])
                        if row.get('closed_at') is not None
                        else None
                    ),
                    quote_time=(
                        pd.Timestamp(row['quote_time'])
                        if row.get('quote_time') is not None
                        else (
                            pd.Timestamp(row['executed_at'])
                            if row.get('executed_at') is not None
                            else None
                        )
                    ),
                    note=str(row.get('note') or ''),
                )
            )
        self._tick_pending_pairs = pending_pairs

    def _export_state(self) -> dict:
        """Serialize the entire dashboard state."""

        signals = []
        for pair, row in self._pair_rows.items():
            if row.signal is not None:
                signals.append(self._serialize_signal(row.signal, None))

        summary = dict(self.summary)
        summary['signal_count'] = len(signals)

        return {
            'summary': summary,
            'pairs': {
                pair: self._serialize_pair_row(row)
                for pair, row in sorted(self._pair_rows.items())
            },
            'signals': signals,
            'positions': self._serialize_positions(),
            'alerts': self._serialize_alerts(),
            'executions': self._serialize_executions(),
            'log': list(self._log),
            'currency_balances': dict(self._currency_balances),
        }

    def _export_position_state(self) -> dict:
        """Serialize the position-specific state updated on live price ticks."""

        return {
            'summary': dict(self.summary),
            'positions': self._serialize_positions(),
            'alerts': self._serialize_alerts(),
        }

    async def set_execution_paused(self, paused: bool) -> dict:
        """Pause or resume new order placement without restarting the dashboard."""

        async with self._lock:
            if not self._execution_available:
                raise RuntimeError('Dashboard started in scan-only mode; execution cannot be resumed.')

            changed = self._execution_paused != paused
            self._execution_paused = paused
            if changed:
                action = 'paused' if paused else 'resumed'
                level = 'warning' if paused else 'success'
                self._append_log(level, f'New trade execution {action} from dashboard')
            self.summary = self._build_summary(status=self.summary.get('status', 'starting'))
            state = self._export_state()

        if changed:
            await self._broadcast({'type': 'snapshot', 'state': state})
        return state

    async def _broadcast(self, payload: dict) -> None:
        """Fan out a JSON payload to all active websocket clients."""

        if not self._clients:
            return

        stale: list[web.WebSocketResponse] = []
        for ws in list(self._clients):
            try:
                await ws.send_json(payload)
            except Exception:
                stale.append(ws)

        for ws in stale:
            self._clients.discard(ws)

    def _track_task(self, task: asyncio.Task, *, label: str) -> None:
        """Track an asyncio task and surface failures in the dashboard log."""

        self._pending_tasks.add(task)
        task.add_done_callback(
            lambda done, task_label=label: self._on_tracked_task_done(done, label=task_label)
        )

    def _on_tracked_task_done(self, task: asyncio.Task, *, label: str) -> None:
        """Remove a tracked task and log any unhandled exception."""

        self._pending_tasks.discard(task)
        if task.cancelled():
            return
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            return
        if exc is None:
            return
        log_task = asyncio.create_task(
            self._broadcast_log('error', f'{label} failed: {exc}')
        )
        self._pending_tasks.add(log_task)
        log_task.add_done_callback(self._pending_tasks.discard)

    async def close_tracked_position(self, *, pair: str, direction: str) -> dict:
        """Submit a closing market order for a tracked position."""

        pair = str(pair).strip().upper()
        direction = str(direction).strip().upper()
        if direction not in {'LONG', 'SHORT'}:
            raise ValueError('Direction must be LONG or SHORT.')

        position_key = f'{pair}:{direction}'
        async with self._lock:
            if not self._execution_available:
                raise RuntimeError('Execution is unavailable in scan-only mode.')
            if not self._execution_enabled():
                raise RuntimeError('Execution is currently paused.')
            if self._loop is None:
                raise RuntimeError('Dashboard loop not initialized.')
            info = self._tracked.get(position_key)
            if info is None:
                raise LookupError(f'No tracked position found for {pair} {direction}.')
            size = int(abs(float(info.get('ibkr_size') or 0.0)))
            if size <= 0:
                raise RuntimeError('Tracked position has no executable size.')

            close_direction = 'SHORT' if direction == 'LONG' else 'LONG'
            trade = info['trade']
            order_ref = f'fxsr:close:{pair}:{direction}:{int(datetime.now(timezone.utc).timestamp() * 1000)}'

            signal_id = info.get('signal_id')

        # Cancel bracket TP/SL children first so they can't fire on a flat book
        await self._loop.run_in_executor(
            self._scan_executor,
            lambda: cancel_bracket_children(signal_id),
        )

        order = await self._loop.run_in_executor(
            self._scan_executor,
            lambda: ibkr.submit_fx_market_order(
                pair=pair,
                direction=close_direction,
                quantity=size,
                order_ref=order_ref,
            ),
        )

        if order is None:
            status = 'FAILED'
            order_id = None
            avg_fill_price = None
            broker_status = None
            note = 'manual close request rejected by broker'
        else:
            status = str(order.get('status') or 'SUBMITTED').upper()
            order_id = order.get('order_id')
            avg_fill_price = order.get('avg_fill_price')
            broker_status = order.get('status')
            if status == 'FILLED':
                status = 'CLOSED'
            note = f'Manual close submitted for {pair} ({direction} -> {close_direction}).'
            if status == 'CLOSED':
                note = f'Manual close filled for {pair} ({direction} -> {close_direction}).'

        result = ExecutionResult(
            pair=pair,
            direction=direction,
            units=size,
            status=status,
            order_id=order_id,
            avg_fill_price=avg_fill_price,
            remaining_units=0,
            broker_status=broker_status,
            submitted_entry_price=float(getattr(trade, 'entry_price')),
            submitted_sl_price=float(getattr(trade, 'sl_price', 0.0)),
            submitted_tp_price=float(getattr(trade, 'tp_price', 0.0)),
            pnl_pips=(
                calc_pnl_pips(trade, avg_fill_price, pair_pip(pair), self.params)
                if avg_fill_price is not None
                else None
            ),
            closed_price=self._as_float(avg_fill_price),
            closed_at=pd.Timestamp.now('UTC') if status == 'CLOSED' else None,
            close_reason='MANUAL',
            quote_time=pd.Timestamp.now('UTC'),
            note=note,
        )

        refreshed_tracked = None
        sync_error = None
        if status != 'FAILED':
            try:
                refreshed_tracked = await self._loop.run_in_executor(
                    self._scan_executor,
                    lambda: sync_positions(self.params, self.zone_history_days),
                )
            except Exception as exc:  # pragma: no cover - defensive
                sync_error = str(exc)

        async with self._lock:
            if status == 'CLOSED':
                self._append_or_merge_execution_result(result)
            else:
                self._execution_results.append(result)
            level = 'warning'
            if status == 'FAILED':
                level = 'error'
            elif status == 'PARTIAL' or status == 'OPEN':
                level = 'success'
            self._append_log(
                level,
                (
                    f'Close request {status}: {pair} {direction} '
                    f'{size:,} units (order {order_id or "n/a"})'
                ),
            )

            if status == 'FAILED':
                self._tick_pending_pairs.discard(pair)
            elif order_id is not None:
                self._tick_pending_pairs.add(pair)

            if refreshed_tracked is not None:
                self._tracked = refreshed_tracked
                self._apply_live_quotes()
            self.summary = self._build_summary(status=self.summary.get('status', 'live'))
            state = self._export_state()

        await self._broadcast({'type': 'snapshot', 'state': state})

        if sync_error is not None:
            return {
                'result': {
                    'status': status,
                    'pair': pair,
                    'direction': direction,
                    'order_id': order_id,
                    'size': size,
                },
                'state': state,
                'message': note,
                'warning': sync_error,
            }

        return {
            'result': {
                'status': status,
                'pair': pair,
                'direction': direction,
                'order_id': order_id,
                'size': size,
            },
            'state': state,
            'message': note,
        }

    def _apply_live_quotes(self) -> None:
        """Overlay the latest subscribed quotes onto the current snapshot-derived state."""

        if not self._last_quotes:
            return

        for pair, price in self._last_quotes.items():
            row = self._pair_rows.get(pair)
            if row is not None:
                self._pair_rows[pair] = refresh_pair_row_price(row, price)

        for key, info in self._tracked.items():
            pair = info['pair']
            price = self._last_quotes.get(pair)
            if price is None:
                continue
            self._position_snapshots[key] = {
                'current_price': price,
                'pnl_pips': calc_pnl_pips(info['trade'], price, pair_pip(pair), self.params),
            }

    async def _handle_quote_update(self, pair: str, price: float) -> None:
        """Apply a subscribed quote change to the in-memory dashboard state.

        Beyond display updates, this also:
        1. Checks tick-level TP/SL/zone-break exits (inline, no I/O)
        2. Updates the dashboard snapshot for subscribed clients
        """

        async with self._lock:
            self._last_quotes[pair] = price
            positions_changed = False
            for key, info in self._tracked.items():
                if info['pair'] != pair:
                    continue
                self._position_snapshots[key] = {
                    'current_price': price,
                    'pnl_pips': calc_pnl_pips(info['trade'], price, pair_pip(pair), self.params),
                }
                positions_changed = True

            row = self._pair_rows.get(pair)
            updated_row = None
            if row is not None:
                updated_row = refresh_pair_row_price(row, price)
                self._pair_rows[pair] = updated_row

            # --- Skip all trading logic until backfill is complete ---
            if not self._backfill_done:
                summary = dict(self.summary)
                row_payload = self._serialize_pair_row(updated_row) if updated_row is not None else None
                position_payload = self._export_position_state() if positions_changed else None

        if not self._backfill_done:
            if position_payload is not None:
                await self._broadcast({'type': 'positions_update', **position_payload})
            if row_payload is not None:
                await self._broadcast({'type': 'pair_update', 'row': row_payload, 'summary': summary})
            return

        exit_signal_writes: list[tuple[str, str, float | None]] = []
        async with self._lock:
            # --- Tick exit checks (inline — pure float math, no I/O) ---
            tick_alerts = self._scanner.check_tick_exits(pair, price, self._tracked)
            alerted_keys = {f"{a['pair']}:{a['direction']}" for a in tick_alerts}

            for alert in tick_alerts:
                alert_key = f"{alert['pair']}:{alert['direction']}"
                if alert['exit_reason'] == 'EARLY_EXIT':
                    # Dynamic: show while price is at/past threshold, clear when it recovers
                    if alert_key not in self._early_exit_active:
                        self._early_exit_active[alert_key] = alert
                        self._append_log(
                            'warning',
                            f"Tick exit: {alert['pair']} {alert['direction']} "
                            f"{alert['exit_reason']} @ {alert['exit_price']:.5f}",
                        )
                        positions_changed = True
                else:
                    # TP/SL: historical, one-time
                    if alert_key in self._tick_exit_alerted:
                        continue
                    self._tick_exit_alerted.add(alert_key)
                    self._alerts.append(alert)
                    self._append_log(
                        'warning',
                        f"Tick exit: {alert['pair']} {alert['direction']} "
                        f"{alert['exit_reason']} @ {alert['exit_price']:.5f}",
                    )
                    info = self._tracked.get(alert_key)
                    if info and info.get('signal_id'):
                        exit_signal_writes.append(
                            (info['signal_id'], alert['exit_reason'], alert['exit_price'])
                        )
                    positions_changed = True

            # Clear early exit alerts for tracked positions on this pair that have recovered
            for key in list(self._early_exit_active):
                if key.startswith(f"{pair}:") and key not in alerted_keys:
                    del self._early_exit_active[key]
                    positions_changed = True

            summary = dict(self.summary)
            row_payload = self._serialize_pair_row(updated_row) if updated_row is not None else None
            position_payload = self._export_position_state() if positions_changed else None

        for signal_id, exit_reason, exit_price in exit_signal_writes:
            await self._loop.run_in_executor(
                self._scan_executor,
                lambda sid=signal_id: cancel_bracket_children(sid),
            )
            await enqueue_write_async(
                lambda s=signal_id, r=exit_reason, p=exit_price: record_exit_signal(
                    s, exit_reason=r, exit_price=p,
                )
            )

        if positions_changed and position_payload is not None:
            await self._broadcast({'type': 'positions_update', **position_payload})

        if row_payload is not None:
            await self._broadcast({'type': 'pair_update', 'row': row_payload, 'summary': summary})

    async def _handle_signal(self, signal, *, source: str) -> None:
        """Process a streaming signal detected from the live bar feed."""

        async with self._lock:
            portfolio_state = self._portfolio_state
        block = get_entry_block(signal.pair, signal.time, portfolio_state, self.params)
        if block is not None:
            state, note = block
            async with self._lock:
                self._append_log('info', f"{source.title()} signal blocked: {signal.pair} {note}")
                self._clear_signal_tracking(signal.pair)
                row = self._pair_rows.get(signal.pair)
                if row is not None:
                    self._pair_rows[signal.pair] = replace(
                        row,
                        state=state,
                        note=note,
                        signal=None,
                    )
                state_payload = self._export_state()
            await self._broadcast({'type': 'snapshot', 'state': state_payload})
            return

        # Snapshot mutable state under the lock for the executor closure
        async with self._lock:
            self._append_log(
                'success',
                f"{source.title()} signal: {signal.pair} {signal.direction} @ {signal.entry_price:.5f}",
            )
            self._mark_signal_valid(signal)
            row = self._pair_rows.get(signal.pair)
            if row is not None:
                note = f"{signal.zone_type.title()} reversal ({signal.zone_strength})"
                self._pair_rows[signal.pair] = replace(
                    row,
                    state=signal.direction,
                    note=note,
                    signal=signal,
                )
            price_cache = dict(self._last_quotes)
            existing_pairs = {info['pair'] for info in self._tracked.values()}
            pending_pairs = set(self._tick_pending_pairs)
            tracked_copy = dict(self._tracked)
            execute_orders = self._execution_enabled()
            execution_available = self._execution_available
            execution_paused = self._execution_paused
            summary_status = self.summary.get('status', 'starting')

        # Build size plan and optionally execute (in executor — IBKR I/O)
        balance = self.balance
        risk_pct = self.risk_pct
        account_currency = self.account_currency
        params = self.params

        def _size_and_execute():
            if execute_orders:
                exec_mode = ibkr.get_execution_mode()
                ibkr_acct = ibkr.fetch_account_id()
            elif execution_available and execution_paused:
                exec_mode = 'paused'
                ibkr_acct = None
            else:
                exec_mode = 'scan'
                ibkr_acct = None
            size_plans = build_live_size_plans(
                [signal],
                balance,
                risk_pct,
                account_currency,
                params=params,
                portfolio_state=portfolio_state,
                price_cache=price_cache,
            )
            record_detected_signals(
                [signal],
                size_plans,
                execute_orders=execute_orders,
                execution_mode=exec_mode,
                ibkr_account=ibkr_acct,
            )
            execution_results = []
            if execute_orders:
                execution_results = execute_signal_plans(
                    [signal],
                    size_plans,
                    execute_orders=True,
                    existing_pairs=existing_pairs,
                    pending_pairs=pending_pairs,
                    params=params,
                    tracked_positions=tracked_copy,
                    balance=balance,
                    risk_pct=risk_pct,
                    account_currency=account_currency,
                    price_cache=price_cache,
                    execution_mode=self.execution_mode,
                )
                record_execution_results(
                    [signal], size_plans, execution_results,
                    execution_mode=exec_mode,
                    ibkr_account=ibkr_acct,
                )
            elif execution_available and execution_paused:
                plan = size_plans[0] if size_plans else None
                execution_results = [
                    ExecutionResult(
                        pair=signal.pair,
                        direction=signal.direction,
                        units=int(plan.units) if plan is not None else 0,
                        status='SKIPPED',
                        note='execution paused',
                    )
                ]
                record_execution_results(
                    [signal], size_plans, execution_results,
                    execution_mode=exec_mode,
                    ibkr_account=ibkr_acct,
                )
            return size_plans, execution_results

        size_plans, execution_results = await self._loop.run_in_executor(
            self._scan_executor,
            _size_and_execute,
        )
        refreshed_tracked = None
        if execute_orders and self.track_positions:
            refreshed_tracked = await self._loop.run_in_executor(
                self._scan_executor,
                lambda: sync_positions(
                    self.params,
                    self.zone_history_days,
                ),
            )

        async with self._lock:
            if refreshed_tracked is not None:
                self._tracked = refreshed_tracked
                self._apply_live_quotes()
            for result in execution_results:
                self._execution_results.append(result)
                level = 'success' if result.status.upper().endswith('SUBMITTED') else 'warning'
                if result.status in {'PARTIAL', 'OPEN'}:
                    level = 'success'
                if result.status == 'FAILED':
                    level = 'error'
                note_suffix = f' — {result.note}' if result.note else ''
                self._append_log(
                    level,
                    f'{source.title()} {result.status}: {result.pair} {result.direction}{note_suffix}',
                )
                if result.status == 'OPEN':
                    self._tick_pending_pairs.discard(result.pair)
                elif result.order_id is not None:
                    self._tick_pending_pairs.add(result.pair)
                row = self._pair_rows.get(result.pair)
                if row is not None:
                    self._clear_signal_tracking(result.pair)
                    if result.status == 'PARTIAL':
                        self._pair_rows[result.pair] = replace(row, state='PARTIAL', note=result.note, signal=None)
                    elif result.status == 'OPEN':
                        self._pair_rows[result.pair] = replace(row, state='OPEN', note=result.note, signal=None)
                    elif result.order_id is not None:
                        self._pair_rows[result.pair] = replace(row, state='PENDING', note=result.note, signal=None)
                    else:
                        # Clear the transient directional signal when execution was
                        # skipped or failed so the dashboard does not keep showing
                        # a stale LONG/SHORT setup as still actionable.
                        self._pair_rows[result.pair] = replace(row, state='WAIT', note=result.note, signal=None)
            self.summary = self._build_summary(status=summary_status)
            state = self._export_state()

        await self._broadcast({'type': 'snapshot', 'state': state})

        # Nudge housekeeping to run soon after any trade execution so position
        # tracking updates quickly instead of waiting up to 5 minutes.
        if any(r.order_id is not None or r.status in ('OPEN', 'PARTIAL') for r in execution_results):
            self._housekeeping_nudge.set()

    def _queue_quote_update(self, pair: str, price: float) -> None:
        """Marshal a thread-side quote callback onto the asyncio loop."""

        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._create_tracked_task, pair, price)

    def _create_tracked_task(self, pair: str, price: float) -> None:
        """Create an asyncio task and track it so it can be cleaned up."""

        task = asyncio.create_task(self._handle_quote_update(pair, price))
        self._track_task(task, label=f'quote update {pair}')

    def _queue_bar_update(self, pair: str, bar) -> None:
        """Marshal a real-time bar callback onto the asyncio loop."""

        if self._loop is None:
            return

        def _schedule():
            task = asyncio.create_task(self._handle_bar_update(pair, bar))
            self._track_task(task, label=f'realtime bar {pair}')

        self._loop.call_soon_threadsafe(_schedule)

    @staticmethod
    def _copy_realtime_bar(bar) -> _BufferedRealtimeBar | None:
        """Copy a realtime bar into a lightweight startup-buffer payload."""

        bar_time = getattr(bar, 'time', None) or getattr(bar, 'date', None)
        if bar_time is None:
            return None
        ts = pd.Timestamp(bar_time)
        if ts.tzinfo is None:
            ts = ts.tz_localize('UTC')
        else:
            ts = ts.tz_convert('UTC')
        return _BufferedRealtimeBar(
            time=ts,
            open_=float(getattr(bar, 'open_', 0) or 0),
            high=float(getattr(bar, 'high', 0) or 0),
            low=float(getattr(bar, 'low', 0) or 0),
            close=float(getattr(bar, 'close', 0) or 0),
            volume=float(getattr(bar, 'volume', 0) or 0),
        )

    def _buffer_startup_bar(self, pair: str, bar) -> None:
        """Append one incoming realtime bar to the startup replay buffer."""

        copied = self._copy_realtime_bar(bar)
        if copied is None:
            return
        self._startup_bar_sequence += 1
        self._startup_bar_buffer.append(
            (copied.time, self._startup_bar_sequence, pair, copied)
        )

    async def _process_realtime_bar(self, pair: str, bar) -> None:
        """Apply one realtime bar to quotes, accumulator, and live logic."""

        price = float(getattr(bar, 'close', 0) or 0)
        if price <= 0:
            return

        # Delegate to the existing quote handler for display and tick exits
        await self._handle_quote_update(pair, price)

        if not self._realtime_bars_enabled:
            return

        self._accumulator.on_realtime_bar(pair, bar)

        if self._backfill_done and self.execution_mode == 'intrabar':
            bar_time = getattr(bar, 'time', None) or getattr(bar, 'date', None)
            if bar_time is not None:
                import datetime as _dt
                if isinstance(bar_time, _dt.datetime):
                    minute_ts = int(bar_time.timestamp()) // 60
                else:
                    minute_ts = int(pd.Timestamp(bar_time).timestamp()) // 60
                prev_minute = self._minute_tracker.get(pair)
                self._minute_tracker[pair] = minute_ts
                if prev_minute is not None and minute_ts != prev_minute:
                    await self._handle_minute_bar_complete(pair, price)

    async def _replay_startup_bars(self) -> int:
        """Replay buffered realtime bars collected while startup work ran."""

        replayed = 0
        while True:
            async with self._lock:
                if not self._startup_bar_buffer:
                    break
                batch = sorted(
                    self._startup_bar_buffer,
                    key=lambda item: (item[0], item[1]),
                )
                self._startup_bar_buffer = []
            for _ts, _seq, pair, bar in batch:
                await self._process_realtime_bar(pair, bar)
                replayed += 1
            await asyncio.sleep(0)

        self._startup_bar_buffering = False
        return replayed

    async def _handle_bar_update(self, pair: str, bar) -> None:
        """Process a 5-second real-time bar: update accumulator + feed quote/exit handling."""

        price = float(getattr(bar, 'close', 0) or 0)
        if price <= 0:
            return

        if self._startup_bar_buffering:
            self._buffer_startup_bar(pair, bar)
            await self._handle_quote_update(pair, price)
            return

        await self._process_realtime_bar(pair, bar)

    async def _handle_minute_bar_complete(self, pair: str, price: float) -> None:
        """Intrabar mode: evaluate signal at each minute bar close."""

        if not self._backfill_done:
            return

        # Build hourly df including the in-progress bar
        hourly_df = self._accumulator.get_hourly_df(pair)
        if hourly_df is None or hourly_df.empty:
            return

        async with self._lock:
            tracked_copy = dict(self._tracked)
            blocked = set(self._tick_pending_pairs)
            current_signal_id = self._active_signal_meta.get(pair, {}).get('signal_id')

        updated_row, signal, wf_signals = await self._loop.run_in_executor(
            self._scan_executor,
            lambda: self._evaluate_pair_row(
                pair,
                tracked_positions=tracked_copy,
                blocked_pairs=set(blocked),
                price=price,
                hourly_df=hourly_df,
            ),
        )

        # Log and attempt execution for new WF trade signals
        if wf_signals:
            async with self._lock:
                for ws in wf_signals:
                    exit_reason = getattr(ws, '_wf_exit_reason', '?')
                    pnl_r = getattr(ws, '_wf_pnl_r', 0.0)
                    pnl_label = f'{pnl_r:+.1f}R' if pnl_r else 'open'
                    exit_label = f' exit={exit_reason} {pnl_label}' if exit_reason else ''
                    self._append_log(
                        'info',
                        f'WF signal: {ws.pair} {ws.direction} entry={ws.entry_price:.5f} '
                        f'@ {ws.time}{exit_label}',
                    )
            import datetime as _dt
            _cutoff = pd.Timestamp.now(tz='UTC') - pd.Timedelta(hours=2)
            for ws in wf_signals:
                sig_time = pd.Timestamp(ws.time)
                if sig_time.tzinfo is None:
                    sig_time = sig_time.tz_localize('UTC')
                if sig_time >= _cutoff:
                    await self._handle_signal(ws, source='WF trade')

        if signal is not None and self._signal_identity(signal) != current_signal_id:
            await self._handle_signal(signal, source='intrabar')
            return

        async with self._lock:
            if updated_row is not None:
                if signal is not None:
                    self._mark_signal_valid(signal)
                else:
                    self._clear_signal_tracking(pair)
                self._pair_rows[pair] = updated_row

    def _completed_hourly_df(self, pair: str, bar_time) -> pd.DataFrame:
        """Return completed hourly bars up to the finalized bar that triggered the callback."""

        hourly_df = self._accumulator.get_completed_df(pair)
        if hourly_df.empty:
            return hourly_df

        resolved_bar_time = pd.Timestamp(bar_time)
        ref_time = hourly_df.index[-1]
        if ref_time.tzinfo is not None and resolved_bar_time.tzinfo is None:
            resolved_bar_time = resolved_bar_time.tz_localize(ref_time.tzinfo)
        elif ref_time.tzinfo is None and resolved_bar_time.tzinfo is not None:
            resolved_bar_time = resolved_bar_time.tz_convert(None)

        return hourly_df[hourly_df.index <= resolved_bar_time]

    def _on_hourly_bar_complete(self, pair: str, bar_time) -> None:
        """Callback from HourlyBarAccumulator when an hourly bar finalizes."""

        if self._loop is None:
            return

        def _schedule():
            task = asyncio.create_task(self._handle_hourly_bar_complete(pair, bar_time))
            self._track_task(task, label=f'hourly bar {pair}')

        self._loop.call_soon_threadsafe(_schedule)

    async def _handle_hourly_bar_complete(self, pair: str, bar_time) -> None:
        """Run bar-shape exit checks and full signal evaluation on hourly bar completion."""

        if not self._backfill_done:
            return

        hourly_df = self._completed_hourly_df(pair, bar_time)
        if hourly_df.empty:
            return

        completed_time = hourly_df.index[-1]
        last_bar = hourly_df.iloc[-1]
        completed_close = float(last_bar['Close'])

        async with self._lock:
            self._append_log('info', f'Hourly bar complete: {pair} @ {completed_time}')

            # Bar-shape exit checks for tracked positions on this pair
            if self.track_positions:
                for key, info in self._tracked.items():
                    if info['pair'] != pair:
                        continue
                    if key in self._tick_exit_alerted:
                        continue
                    alert = process_hourly_exit_bars(
                        info,
                        hourly_df.tail(1),
                        self.params,
                        count_initial_unseen_bar=True,
                    )
                    if alert:
                        alert = {
                            'pair': pair,
                            'direction': alert['direction'],
                            'exit_reason': alert['exit_reason'],
                            'exit_price': alert['exit_price'],
                            'entry_price': alert['entry_price'],
                            'current_price': alert['current_price'],
                            'pnl_pips': alert['pnl_pips'],
                            'bars_monitored': alert['bars_monitored'],
                            'source': 'hourly',
                        }
                        self._tick_exit_alerted.add(key)
                        self._alerts.append(alert)
                        self._append_log(
                            'warning',
                            f"Bar exit: {pair} {alert['direction']} {alert['exit_reason']} @ {alert['exit_price']:.5f}",
                        )
                        # Nudge housekeeping — bracket likely filled or about to
                        self._housekeeping_nudge.set()

            tracked_copy = dict(self._tracked)
            blocked = set(self._tick_pending_pairs)
            current_signal_id = self._active_signal_meta.get(pair, {}).get('signal_id')
            current_price = self._last_quotes.get(pair, completed_close)

        updated_row, signal, wf_signals = await self._loop.run_in_executor(
            self._scan_executor,
            lambda: self._evaluate_pair_row(
                pair,
                tracked_positions=tracked_copy,
                blocked_pairs=set(blocked),
                price=current_price,
                hourly_df=hourly_df,
            ),
        )

        # Log new walk-forward trade signals for this pair
        if wf_signals:
            async with self._lock:
                for ws in wf_signals:
                    exit_reason = getattr(ws, '_wf_exit_reason', '?')
                    pnl_r = getattr(ws, '_wf_pnl_r', 0.0)
                    pnl_label = f'{pnl_r:+.1f}R' if pnl_r else 'open'
                    exit_label = f' exit={exit_reason} {pnl_label}' if exit_reason else ''
                    self._append_log(
                        'info',
                        f'WF signal: {ws.pair} {ws.direction} entry={ws.entry_price:.5f} '
                        f'@ {ws.time}{exit_label}',
                    )
            # Attempt execution for fresh WF signals (entered within last 2 hours)
            import datetime as _dt
            _cutoff = pd.Timestamp.now(tz='UTC') - pd.Timedelta(hours=2)
            for ws in wf_signals:
                sig_time = pd.Timestamp(ws.time)
                if sig_time.tzinfo is None:
                    sig_time = sig_time.tz_localize('UTC')
                if sig_time >= _cutoff:
                    await self._handle_signal(ws, source='WF trade')
        if signal is not None and self._signal_identity(signal) != current_signal_id:
            await self._handle_signal(signal, source='hourly')
            return

        async with self._lock:
            if updated_row is not None:
                if signal is not None:
                    self._mark_signal_valid(signal)
                else:
                    self._clear_signal_tracking(pair)
                self._pair_rows[pair] = updated_row
            self.summary = self._build_summary(status=self.summary.get('status', 'live'))
            state = self._export_state()

        await self._broadcast({'type': 'snapshot', 'state': state})

    def _run_realtime_bar_stream(self) -> None:
        """Run the blocking IBKR real-time bar subscription loop."""

        base_client_id = self.client_id if self.client_id is not None else ibkr.TWS_CLIENT_ID
        stream_client_id = int(base_client_id) + 1000
        ibkr.stream_realtime_bars(
            pairs=list(self.pairs.keys()),
            on_bar=self._queue_bar_update,
            stop_event=self._quote_stop,
            client_id=stream_client_id,
        )

    def _ensure_quote_stream_started(self) -> bool:
        """Start the real-time bar thread once and report whether it was newly started."""

        if self._quote_thread is not None and self._quote_thread.is_alive():
            return False
        self._quote_stop.clear()
        self._quote_thread = threading.Thread(
            target=self._run_realtime_bar_stream,
            name='ibkr-realtime-bars',
            daemon=True,
        )
        self._quote_thread.start()
        return True

    def _backfill_data(self) -> None:
        """Fetch historical daily + hourly data for all pairs (runs in executor)."""

        import time as _time
        from .levels import detect_zones
        from .strategy import get_tradeable_zones as _get_tz, is_pair_fully_blocked
        from .walkforward import slice_daily_window

        pair_list = list(self.pairs.items())
        total = len(pair_list)
        backfill_client_id = self._backfill_client_id_base()
        minute_seed_days = 2
        daily_cache: dict[tuple[str, int], object] = {}
        zone_cache: dict[tuple[str, int], object] = {}
        hourly_cache: dict[str, object] = {}
        pair_status = self._backfill_progress['pair_status']

        # Phase 2: scan for cache holes in parallel, refill recent holes
        # sequentially, then seed daily/hourly/minute state from cache.
        phase2_start = _time.monotonic()
        print(f'  [backfill] Phase 2: cache-hole check + seed for {total} pairs')
        self._backfill_progress.update(phase='seed', completed=0, total=total)
        self._backfill_progress['current_detail'] = ''

        def _scan_pair_gaps(pair_id: str, pair_info: dict) -> dict:
            ticker = pair_info.get('ticker')
            if not ticker:
                return {
                    'pair_id': pair_id,
                    'ticker': ticker,
                    'error': 'no ticker',
                }
            if is_pair_fully_blocked(pair_id, self.params):
                return {
                    'pair_id': pair_id,
                    'ticker': ticker,
                    'refill_holes': [],
                    'reported_only_holes': [],
                    'skipped': 'fully blocked',
                    'error': None,
                }

            now_utc = pd.Timestamp.now(tz='UTC')
            recent_cutoff = now_utc - _HOLE_REFILL_MAX_AGE
            refill_holes: list[tuple[str, pd.Timestamp]] = []
            reported_only_holes: list[str] = []

            for interval in ('1d', '1h', '1m'):
                gap_start_recent = find_first_missing_cached_bar(
                    ticker,
                    interval,
                    start=recent_cutoff,
                    end=now_utc,
                    now=now_utc,
                    check_trailing=True,
                )
                if gap_start_recent is not None:
                    gap_ts = pd.Timestamp(gap_start_recent)
                    if gap_ts.tzinfo is None:
                        gap_ts = gap_ts.tz_localize('UTC')
                    else:
                        gap_ts = gap_ts.tz_convert('UTC')
                    refill_holes.append((interval, gap_ts))

                # Old 1m holes are report-only and expensive to scan over long
                # histories. Do not block live startup on that audit path.
                if interval == '1m':
                    continue

                gap_start_old = find_first_missing_cached_bar(
                    ticker,
                    interval,
                    end=recent_cutoff,
                    now=recent_cutoff,
                    check_trailing=False,
                )
                if gap_start_old is None:
                    continue
                gap_ts = pd.Timestamp(gap_start_old)
                if gap_ts.tzinfo is None:
                    gap_ts = gap_ts.tz_localize('UTC')
                else:
                    gap_ts = gap_ts.tz_convert('UTC')
                gap_label = f'{interval}@{gap_ts}'
                reported_only_holes.append(gap_label)

            return {
                'pair_id': pair_id,
                'ticker': ticker,
                'refill_holes': refill_holes,
                'reported_only_holes': reported_only_holes,
                'skipped': None,
                'error': None,
            }

        env_scan_workers = os.getenv('FX_SR_PHASE2_SCAN_WORKERS')
        if env_scan_workers is not None:
            try:
                scan_workers = max(1, min(total, int(env_scan_workers.strip())))
            except ValueError:
                scan_workers = min(total, _PHASE2_SCAN_WORKERS_DEFAULT)
        else:
            scan_workers = min(total, _PHASE2_SCAN_WORKERS_DEFAULT)
        gap_scan_results: dict[str, dict] = {}
        refill_work: list[tuple[str, str, str, pd.Timestamp]] = []
        print(f'  [backfill] Phase 2a scan workers: {scan_workers}')
        with ThreadPoolExecutor(max_workers=scan_workers, thread_name_prefix='startup-gap-scan') as executor:
            futures = {
                executor.submit(_scan_pair_gaps, pair_id, pair_info): (idx, pair_id, pair_info)
                for idx, (pair_id, pair_info) in enumerate(pair_list)
            }
            completed = 0
            for future in as_completed(futures):
                _, pair_id, pair_info = futures[future]
                completed += 1
                self._backfill_progress.update(current_pair=pair_id, completed=completed)
                try:
                    payload = future.result()
                except Exception as exc:
                    pair_status[pair_id] = 'gap scan failed'
                    print(f'    [scan {completed}/{total}] {pair_id}: gap scan FAILED: {exc}')
                    continue

                gap_scan_results[pair_id] = payload
                if payload.get('error') is not None:
                    pair_status[pair_id] = str(payload['error'])
                    print(f'    [scan {completed}/{total}] {pair_id}: {payload["error"]}')
                    continue

                refill_holes = payload['refill_holes']
                reported_only = payload['reported_only_holes']
                skipped_reason = payload.get('skipped')
                if skipped_reason:
                    pair_status[pair_id] = skipped_reason
                    print(f'    [scan {completed}/{total}] {pair_id}: skipped gap scan/refill ({skipped_reason})')
                    continue
                for interval, gap_ts in refill_holes:
                    refill_work.append((pair_id, pair_info['ticker'], interval, gap_ts))

                note_parts: list[str] = []
                if refill_holes:
                    note_parts.append(
                        'refill ' + ', '.join(f'{interval}@{gap_ts}' for interval, gap_ts in refill_holes)
                    )
                if reported_only:
                    note_parts.append('reported only ' + ', '.join(reported_only))
                note = '; '.join(note_parts) if note_parts else 'no gaps'
                pair_status[pair_id] = 'gaps scanned'
                print(f'    [scan {completed}/{total}] {pair_id}: {note}')

        self._backfill_progress.update(current_pair=None, completed=0)
        if refill_work:
            print(f'  [backfill] Phase 2b: refilling {len(refill_work)} recent gap(s) sequentially')
        else:
            print('  [backfill] Phase 2b: no recent gaps to refill')
        for refill_idx, (pair_id, ticker, interval, gap_ts) in enumerate(refill_work, 1):
            pair_status[pair_id] = 'refilling holes'
            self._backfill_progress.update(current_pair=pair_id, completed=refill_idx)
            t0 = _time.monotonic()
            try:
                refill_interval_from(
                    ticker,
                    interval,
                    gap_ts,
                    client_id=backfill_client_id,
                )
            except Exception as exc:
                pair_status[pair_id] = f'refill failed: {exc}'
                print(
                    f'    [refill {refill_idx}/{len(refill_work)}] {pair_id}: '
                    f'{interval}@{gap_ts} FAILED after {_time.monotonic() - t0:.1f}s: {exc}'
                )
                raise
            print(
                f'    [refill {refill_idx}/{len(refill_work)}] {pair_id}: '
                f'{interval}@{gap_ts} ({_time.monotonic() - t0:.1f}s)'
            )

        def _prepare_pair(pair_id: str, pair_info: dict) -> dict:
            ticker = pair_info.get('ticker')
            started = _time.monotonic()
            timings: dict[str, float] = {}

            def _set_step(step: str) -> None:
                pair_status[pair_id] = step
                self._backfill_progress['current_pair'] = pair_id
                self._backfill_progress['current_detail'] = step

            if not ticker:
                return {
                    'pair_id': pair_id,
                    'ticker': ticker,
                    'error': 'no ticker',
                }

            now_utc = pd.Timestamp.now(tz='UTC')
            _set_step('loading hourly cache')
            t_step = _time.monotonic()
            hourly_df = load_ohlc(ticker, '1h')
            timings['hourly_load_s'] = _time.monotonic() - t_step
            if hourly_df.empty:
                return {
                    'pair_id': pair_id,
                    'ticker': ticker,
                    'error': 'no hourly data',
                }

            hourly_start = pd.Timestamp(hourly_df.index[0])
            if hourly_start.tzinfo is None:
                hourly_start = hourly_start.tz_localize('UTC')
            else:
                hourly_start = hourly_start.tz_convert('UTC')
            daily_start = hourly_start - pd.Timedelta(days=max(int(self.zone_history_days), 1))
            _set_step('loading daily cache')
            t_step = _time.monotonic()
            daily_df = load_ohlc(
                ticker,
                '1d',
                start=daily_start.to_pydatetime(),
                end=now_utc.to_pydatetime(),
            )
            timings['daily_load_s'] = _time.monotonic() - t_step
            zones = []
            support = None
            resistance = None
            if not daily_df.empty:
                _set_step('detecting zones')
                t_step = _time.monotonic()
                daily_window = slice_daily_window(daily_df, daily_df.index[-1], self.zone_history_days)
                zones = detect_zones(daily_window)
                ref_price = float(daily_df['Close'].iloc[-1])
                support, resistance = _get_tz(zones, ref_price)
                timings['zone_detect_s'] = _time.monotonic() - t_step
            else:
                timings['zone_detect_s'] = 0.0

            minute_start = max(hourly_start, now_utc - pd.Timedelta(days=max(int(minute_seed_days), 1)))
            _set_step('loading minute cache')
            t_step = _time.monotonic()
            minute_df = load_ohlc(
                ticker,
                '1m',
                start=minute_start.to_pydatetime(),
                end=now_utc.to_pydatetime(),
            )
            timings['minute_load_s'] = _time.monotonic() - t_step

            return {
                'pair_id': pair_id,
                'ticker': ticker,
                'daily_df': daily_df,
                'zones': zones,
                'support': support,
                'resistance': resistance,
                'hourly_df': hourly_df,
                'minute_df': minute_df,
                'refill_holes': gap_scan_results.get(pair_id, {}).get('refill_holes', []),
                'reported_only_holes': gap_scan_results.get(pair_id, {}).get('reported_only_holes', []),
                'timings': timings,
                'elapsed': _time.monotonic() - started,
                'error': None,
            }

        seed_workers = max(1, min(4, total))
        with ThreadPoolExecutor(max_workers=seed_workers, thread_name_prefix='startup-seed') as executor:
            futures = {
                executor.submit(_prepare_pair, pair_id, pair_info): (idx, pair_id, pair_info)
                for idx, (pair_id, pair_info) in enumerate(pair_list)
            }
            completed = 0
            for future in as_completed(futures):
                _, pair_id, pair_info = futures[future]
                completed += 1
                self._backfill_progress.update(current_pair=pair_id, completed=completed)
                ticker = pair_info.get('ticker')
                try:
                    payload = future.result()
                except Exception as exc:
                    pair_status[pair_id] = 'seed failed'
                    print(f'    [{completed}/{total}] {pair_id}: seed FAILED: {exc}')
                    continue

                if payload.get('error') is not None:
                    pair_status[pair_id] = str(payload['error'])
                    print(f'    [{completed}/{total}] {pair_id}: {payload["error"]}')
                    continue

                daily_df = payload['daily_df']
                zones = payload['zones']
                support = payload['support']
                resistance = payload['resistance']
                hourly_df = payload['hourly_df']
                minute_df = payload['minute_df']

                pair_status[pair_id] = 'seeding accumulator'
                self._backfill_progress['current_pair'] = pair_id
                self._backfill_progress['current_detail'] = 'seeding accumulator'
                if not daily_df.empty and ticker:
                    daily_cache[(ticker, int(self.zone_history_days))] = daily_df
                    zone_cache[(ticker, int(self.zone_history_days))] = zones
                    self._scanner._zones[pair_id] = (support, resistance, zones)

                self._accumulator.seed(pair_id, hourly_df)
                self._accumulator.seed_minutes(pair_id, minute_df)
                if ticker:
                    hourly_cache[ticker] = self._accumulator.get_hourly_df(pair_id)

                pair_status[pair_id] = 'ready'
                note_parts: list[str] = []
                if payload['refill_holes']:
                    note_parts.append(
                        "refilled " + ', '.join(f'{interval}@{gap_ts}' for interval, gap_ts in payload['refill_holes'])
                    )
                if payload['reported_only_holes']:
                    note_parts.append(f"reported only {', '.join(payload['reported_only_holes'])}")
                refill_note = f"; {'; '.join(note_parts)}" if note_parts else ''
                timings = payload.get('timings') or {}
                timing_note = (
                    f" [h1 {timings.get('hourly_load_s', 0.0):.1f}s"
                    f", d1 {timings.get('daily_load_s', 0.0):.1f}s"
                    f", zones {timings.get('zone_detect_s', 0.0):.1f}s"
                    f", m1 {timings.get('minute_load_s', 0.0):.1f}s]"
                )
                print(
                    f'    [{completed}/{total}] {pair_id}: '
                    f'{len(daily_df)} daily, {len(hourly_df)} hourly, {len(minute_df)} seed minute bars'
                    f'{refill_note}{timing_note} ({payload["elapsed"]:.1f}s)'
                )
        self._backfill_progress.update(completed=total)
        self._backfill_progress['current_detail'] = ''
        print(f'  [backfill] Phase 2 done in {_time.monotonic()-phase2_start:.1f}s')
        self._realtime_bars_enabled = True

        # Phase 3: Full walk-forward scan from the seeded cache window.
        phase3_start = _time.monotonic()
        scan_pairs = [
            (pair_id, pair_info)
            for pair_id, pair_info in pair_list
            if not is_pair_fully_blocked(pair_id, self.params)
        ]
        scan_total = len(scan_pairs)
        print(f'  [backfill] Phase 3: walk-forward scan for {scan_total} pairs')
        self._backfill_progress.update(phase='scan', current_pair=None, completed=0, total=scan_total)
        closed_trades = load_closed_trade_summaries()
        portfolio_state = build_portfolio_state(closed_trades, params=self.params)
        seed_seen_wf_trades()
        completed = 0
        signals = []
        pair_rows = []
        wf_signals = []
        phase3_workers = max(1, min(scan_total, int(os.getenv('FX_SR_PHASE3_WORKERS', '18') or '18')))

        def _run_phase3_local(pair_id: str, pair_info: dict) -> tuple[PairScanRow, object | None, list, float]:
            t0 = _time.monotonic()
            row, signal, pair_wf_signals = _scan_pair(
                pair_id,
                pair_info,
                self.params,
                self.zone_history_days,
                {},
                {},
                set(),
                daily_data_cache=daily_cache,
                zone_cache=zone_cache,
                hourly_data_cache=hourly_cache,
                minute_data_cache=None,
                execution_mode=self.execution_mode,
                portfolio_state=portfolio_state,
                hourly_days=self.hourly_days,
            )
            return row, signal, pair_wf_signals, (_time.monotonic() - t0)

        if phase3_workers <= 1:
            for pair_id, pair_info in scan_pairs:
                row, signal, pair_wf_signals, elapsed = _run_phase3_local(pair_id, pair_info)
                completed += 1
                self._pair_rows[row.pair] = row
                self._backfill_progress.update(current_pair=pair_id, completed=completed)
                sig_label = f', signal={signal.direction}' if signal else ''
                print(f'    [{completed}/{scan_total}] {pair_id}: {row.state}{sig_label} ({elapsed:.1f}s)')
                pair_rows.append(row)
                if signal is not None:
                    signals.append(signal)
                wf_signals.extend(pair_wf_signals)
        else:
            print(f'  [backfill] Phase 3 workers: {phase3_workers} process(es)')
            with ProcessPoolExecutor(max_workers=phase3_workers) as executor:
                futures = {}
                for pair_id, pair_info in scan_pairs:
                    ticker = pair_info.get('ticker')
                    futures[executor.submit(
                        run_startup_scan_pair,
                        pair_id=pair_id,
                        pair_info=pair_info,
                        params=self.params,
                        zone_history_days=self.zone_history_days,
                        execution_mode=self.execution_mode,
                        portfolio_state=portfolio_state,
                        hourly_days=self.hourly_days,
                        daily_df=daily_cache.get((ticker, int(self.zone_history_days))) if ticker else None,
                        zones=zone_cache.get((ticker, int(self.zone_history_days))) if ticker else None,
                        hourly_df=hourly_cache.get(ticker) if ticker else None,
                        minute_df=None,
                    )] = (pair_id, pair_info, _time.monotonic())

                for future in as_completed(futures):
                    pair_id, pair_info, started = futures[future]
                    try:
                        payload = future.result()
                        row = payload['row']
                        signal = payload['signal']
                        pair_wf_signals = payload['wf_signals']
                        apply_startup_scan_artifacts(
                            pair_id,
                            seen_trade_keys=payload.get('seen_trade_keys'),
                            walk_forward_cache_entry=payload.get('walk_forward_cache_entry'),
                        )
                        elapsed = _time.monotonic() - started
                    except Exception as exc:
                        print(f'    [phase3 worker fallback] {pair_id}: {exc}')
                        row, signal, pair_wf_signals, elapsed = _run_phase3_local(pair_id, pair_info)

                    completed += 1
                    self._pair_rows[row.pair] = row
                    self._backfill_progress.update(current_pair=pair_id, completed=completed)
                    sig_label = f', signal={signal.direction}' if signal else ''
                    print(f'    [{completed}/{scan_total}] {pair_id}: {row.state}{sig_label} ({elapsed:.1f}s)')
                    pair_rows.append(row)
                    if signal is not None:
                        signals.append(signal)
                    wf_signals.extend(pair_wf_signals)

        print(f'  [backfill] Phase 3 done in {_time.monotonic()-phase3_start:.1f}s '
              f'({len(signals)} signals, {len(pair_rows)} pairs)')
        # Compute daily closed-trade P&L at startup
        from .live_history import _compute_daily_pnl_gbp
        from .db import _connect, get_db_path, init_db
        from datetime import date
        daily_closed_pnl = 0.0
        try:
            db_path = get_db_path()
            init_db(db_path)
            conn = _connect(db_path)
            try:
                daily_closed_pnl = _compute_daily_pnl_gbp(conn, date.today())
            finally:
                conn.close()
        except Exception:
            pass

        return signals, pair_rows, closed_trades, wf_signals, daily_closed_pnl

    async def _run_backfill(self) -> None:
        """Run backfill in executor and publish progress to clients."""

        async with self._lock:
            self.summary = self._build_summary(status='backfilling')
            self._append_log('info', 'Starting startup backfill...')
        await self._broadcast({'type': 'scan_status', 'summary': dict(self.summary)})
        await self._broadcast_log('info', 'Startup backfill started. Loading zone and hourly history...')

        # Start a progress broadcast task
        progress_stop = asyncio.Event()

        _last_pair_count = [0]

        async def _broadcast_progress():
            while not progress_stop.is_set():
                async with self._lock:
                    self.summary = self._build_summary(status='backfilling')
                    summary = dict(self.summary)
                    # When new pairs have been scanned, send full snapshot so
                    # the dashboard grid populates incrementally.
                    current_count = len(self._pair_rows)
                    send_snapshot = current_count > _last_pair_count[0]
                    if send_snapshot:
                        _last_pair_count[0] = current_count
                        state = self._export_state()
                # Broadcast outside the lock to avoid holding it during I/O
                if send_snapshot:
                    await self._broadcast({'type': 'snapshot', 'state': state})
                else:
                    await self._broadcast({'type': 'backfill_progress', 'summary': summary})
                try:
                    await asyncio.wait_for(progress_stop.wait(), timeout=0.5)
                except asyncio.TimeoutError:
                    pass

        progress_task = asyncio.create_task(_broadcast_progress())

        try:
            signals, pair_rows, closed_trades, wf_signals, daily_closed_pnl = await self._loop.run_in_executor(
                self._scan_executor,
                self._backfill_data,
            )
        except Exception as exc:
            progress_stop.set()
            await progress_task
            async with self._lock:
                self.summary = self._build_summary(status='error')
            await self._broadcast_log('error', f'Backfill failed: {exc}')
            await self._broadcast({'type': 'error', 'summary': dict(self.summary), 'message': str(exc)})
            return

        progress_stop.set()
        await progress_task

        self._backfill_progress.update(phase='done', current_pair=None)
        await self._broadcast_log(
            'info',
            f'Backfill data loaded for {len(self._accumulator.seeded_pairs)} pairs.',
        )

        # Position sync + balance refresh
        def _post_backfill():
            import os
            tracked = {}
            closed_rows = []
            if self.track_positions:
                from .positions import sync_positions
                tracked = sync_positions(
                    self.params,
                    self.zone_history_days,
                    on_signal_closed=closed_rows.append,
                )

            balance, fetched_currency = ibkr.fetch_account_net_liquidation()
            currency = self.account_currency
            if fetched_currency not in (None, 'BASE'):
                currency = fetched_currency
            elif currency is None:
                env_currency = os.getenv('IBKR_ACCOUNT_CURRENCY')
                if env_currency:
                    currency = env_currency.upper()

            return tracked, balance, currency, closed_rows

        try:
            tracked, balance, currency, closed_rows = await self._loop.run_in_executor(
                self._scan_executor, _post_backfill,
            )
        except Exception:
            tracked, balance, currency, closed_rows = {}, self.balance, self.account_currency, []

        # Register hourly bar completion callback
        self._accumulator.on_bar_complete(self._on_hourly_bar_complete)

        async with self._lock:
            # pair_rows were already stored incrementally during Phase 2;
            # merge here to pick up any late updates without losing state.
            for row in pair_rows:
                self._pair_rows[row.pair] = row
            self._sync_active_signal_tracking(pair_rows)
            self._tracked = tracked
            if balance is not None:
                self.balance = balance
            if currency is not None:
                self.account_currency = currency
            self._daily_closed_pnl = daily_closed_pnl
            self._portfolio_state = build_portfolio_state(closed_trades, params=self.params)
            for row in closed_rows:
                summary = closed_trade_summary_from_row(row)
                if summary is not None:
                    self._portfolio_state.record_closed_trade(summary)
                closed_execution = self._build_closed_execution_result(row)
                if closed_execution is not None:
                    self._append_or_merge_execution_result(closed_execution)
            self._portfolio_state.sync_balance(self.balance)
            self._tick_pending_pairs = set()
            self._tick_exit_alerted = set()
            self._early_exit_active = {}
            self._backfill_done = True
            self._append_log('success', f'Backfill complete: {len(pair_rows)} pairs, {len(signals)} signals')
            # Log all walk-forward trade signals discovered at startup
            if wf_signals:
                self._append_log('info', f'Walk-forward found {len(wf_signals)} trade signal(s):')
                for ws in wf_signals:
                    exit_reason = getattr(ws, '_wf_exit_reason', '?')
                    pnl_r = getattr(ws, '_wf_pnl_r', 0.0)
                    pnl_label = f'{pnl_r:+.1f}R' if pnl_r else 'open'
                    exit_label = f' exit={exit_reason} {pnl_label}' if exit_reason else ''
                    self._append_log(
                        'info',
                        f'  WF signal: {ws.pair} {ws.direction} entry={ws.entry_price:.5f} '
                        f'@ {ws.time}{exit_label}',
                    )
            self.summary = self._build_summary(status='live')
            state = self._export_state()

        await self._broadcast({'type': 'snapshot', 'state': state})
        await self._broadcast_log(
            'success',
            f'Startup backfill complete: {len(pair_rows)} pairs, {len(signals)} signals.',
        )

    async def _housekeeping_loop(self) -> None:
        """Low-frequency periodic tasks: position sync, zone refresh, balance."""

        while True:
            # Wait up to 5 minutes, but wake early if nudged (e.g. after a trade)
            try:
                await asyncio.wait_for(self._housekeeping_nudge.wait(), timeout=300)
            except asyncio.TimeoutError:
                pass
            self._housekeeping_nudge.clear()

            try:
                async with self._lock:
                    price_hints = dict(self._last_quotes)

                def _housekeeping():
                    # Position sync
                    tracked = {}
                    closed_rows = []
                    if self.track_positions:
                        from .positions import sync_positions
                        tracked = sync_positions(
                            self.params,
                            self.zone_history_days,
                            on_signal_closed=closed_rows.append,
                        )

                    # Daily zone refresh
                    self._scanner.refresh_zones(price_hints=price_hints)

                    # Balance refresh — always fetch latest from IBKR
                    import os
                    currency = self.account_currency
                    balance, fetched_currency = ibkr.fetch_account_net_liquidation()
                    if fetched_currency not in (None, 'BASE'):
                        currency = fetched_currency
                    elif currency is None:
                        env_currency = os.getenv('IBKR_ACCOUNT_CURRENCY')
                        if env_currency:
                            currency = env_currency.upper()

                    cash_balances = ibkr.fetch_cash_balances()

                    # Daily closed-trade P&L (same source as live diary)
                    from .live_history import _compute_daily_pnl_gbp
                    from .db import _connect, get_db_path, init_db
                    from datetime import date
                    daily_closed_pnl = 0.0
                    try:
                        db_path = get_db_path()
                        init_db(db_path)
                        conn = _connect(db_path)
                        try:
                            daily_closed_pnl = _compute_daily_pnl_gbp(conn, date.today())
                        finally:
                            conn.close()
                    except Exception:
                        pass

                    return tracked, balance, currency, closed_rows, cash_balances, daily_closed_pnl

                tracked, balance, currency, closed_rows, cash_balances, daily_closed_pnl = await self._loop.run_in_executor(
                    self._scan_executor,
                    _housekeeping,
                )

                async with self._lock:
                    if self.track_positions:
                        self._tracked = tracked
                    if balance is not None:
                        self.balance = balance
                    if currency is not None:
                        self.account_currency = currency
                    if cash_balances:
                        self._currency_balances = cash_balances
                    if closed_rows:
                        for row in closed_rows:
                            summary = closed_trade_summary_from_row(row)
                            if summary is not None:
                                self._portfolio_state.record_closed_trade(summary)
                            closed_execution = self._build_closed_execution_result(row)
                            if closed_execution is not None:
                                self._append_or_merge_execution_result(closed_execution)
                    self._portfolio_state.sync_balance(self.balance)
                    self._tick_pending_pairs = set()
                    self._tick_exit_alerted = set()
                    self._early_exit_active = {}
                    self._daily_closed_pnl = daily_closed_pnl
                    self._apply_live_quotes()
                    self.summary = self._build_summary(status='live')
                    state = self._export_state()

                await self._broadcast({'type': 'snapshot', 'state': state})

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                async with self._lock:
                    self._append_log('error', f'Housekeeping failed: {exc}')

    async def start(self) -> None:
        """Start backfill, then streaming and housekeeping tasks."""

        self._loop = asyncio.get_running_loop()
        await self._broadcast_log('info', 'Live dashboard startup requested.')
        start_background_writer()
        from .live_history import record_system_event
        record_system_event('startup', f'profile={self.strategy_label} mode={self.execution_mode}')
        if self._blocked_live_pairs:
            blocked_pairs = ', '.join(sorted(self._blocked_live_pairs))
            await self._broadcast_log('info', f'Skipping fully blocked pairs: {blocked_pairs}')

        if self._ensure_quote_stream_started():
            await self._broadcast_log('success', 'Live quote stream thread started.')
        else:
            await self._broadcast_log('info', 'Live quote stream already running.')

        # Phase 1: backfill historical data with progress
        await self._run_backfill()
        await self._broadcast_log('info', 'Loading recent execution activity...')
        await self._loop.run_in_executor(
            self._scan_executor,
            self._hydrate_execution_activity,
        )
        replayed_bars = await self._replay_startup_bars()
        async with self._lock:
            self._apply_live_quotes()
            self.summary = self._build_summary(status=self.summary.get('status', 'live'))
            state = self._export_state()
        await self._broadcast({'type': 'snapshot', 'state': state})
        if replayed_bars:
            await self._broadcast_log('info', f'Replayed {replayed_bars} buffered live bars.')
        await self._broadcast_log('info', 'Startup scans complete.')

        # Phase 2: start bar persistence (bulk-save to PostgreSQL every ~60s)
        pair_ticker_map = {
            pair_id: pair_info['ticker']
            for pair_id, pair_info in self.pairs.items()
            if pair_info.get('ticker')
        }
        self._accumulator.start_persistence(pair_ticker_map)
        await self._broadcast_log('info', 'Bar persistence thread started.')

        # Phase 3: start low-frequency housekeeping
        self._scan_task = asyncio.create_task(self._housekeeping_loop())
        await self._broadcast_log('info', 'Housekeeping loop running.')

    async def stop(self) -> None:
        """Stop background tasks and tear down subscriptions."""

        self._quote_stop.set()

        if self._scan_task is not None:
            self._scan_task.cancel()
            try:
                await self._scan_task
            except asyncio.CancelledError:
                pass

        if self._quote_thread is not None and self._quote_thread.is_alive():
            await asyncio.to_thread(self._quote_thread.join, 5)
            if self._quote_thread.is_alive():
                ibkr.disconnect()

        self._accumulator.stop_persistence()

        for task in list(self._pending_tasks):
            task.cancel()
        self._pending_tasks.clear()

        stop_background_writer()
        self._scan_executor.shutdown(wait=False)

    async def register(self, ws: web.WebSocketResponse) -> None:
        """Register a browser client and send the current state."""

        async with self._lock:
            self._clients.add(ws)
            state = self._export_state()
        try:
            await ws.send_json({'type': 'bootstrap', 'state': state})
        except Exception:
            self._clients.discard(ws)
            raise

    async def unregister(self, ws: web.WebSocketResponse) -> None:
        """Remove a browser client."""

        self._clients.discard(ws)


async def _chart_page(_request: web.Request) -> web.StreamResponse:
    """Serve the live chart page."""

    return web.FileResponse(WEB_DIR / 'chart_live.html')



async def _account_history_api(_request: web.Request) -> web.Response:
    """Return daily account balance and P&L snapshots for the equity chart."""

    import asyncio
    from .live_history import load_daily_snapshots, get_or_fetch_today_snapshot

    hub = _request.app.get('hub')
    loop = getattr(hub, '_loop', None) if hub is not None else None

    if hub is None or loop is None:
        today_task = asyncio.to_thread(get_or_fetch_today_snapshot, force_refresh=True)
    else:
        today_task = loop.run_in_executor(
            hub._scan_executor,
            lambda: get_or_fetch_today_snapshot(force_refresh=True),
        )

    snapshots, today = await asyncio.gather(
        asyncio.to_thread(load_daily_snapshots),
        today_task,
    )

    # Merge today's live snapshot if it's not already the last entry
    if today:
        if not snapshots or snapshots[-1]['date'] != today['date']:
            snapshots.append(today)
        else:
            snapshots[-1] = today

    return web.json_response({'snapshots': snapshots})


async def _daily_reconciliation_api(request: web.Request) -> web.Response:
    """Return daily P&L reconciliation: trade P&L vs actual equity change."""
    from .live_history import load_daily_snapshots
    try:
        snapshots = load_daily_snapshots()
        rows = []
        for i in range(1, len(snapshots)):
            cur = snapshots[i]
            prev = snapshots[i - 1]
            bal = cur.get('balance') or 0
            prev_bal = prev.get('balance') or 0
            trade_pnl = cur.get('daily_pnl_gbp') or 0
            actual_change = bal - prev_bal
            rows.append({
                'date': str(cur.get('date', '')),
                'balance': round(bal, 2),
                'prev_balance': round(prev_bal, 2),
                'trade_pnl': round(float(trade_pnl), 2),
                'actual_change': round(actual_change, 2),
                'hidden_cost': round(actual_change - float(trade_pnl), 2),
            })
        return web.json_response({'rows': rows[-14:]})
    except Exception as exc:
        return web.json_response({'error': str(exc)}, status=500)


async def _daily_statement_api(request: web.Request) -> web.Response:
    """Return IBKR daily account statement via Flex Query."""
    from . import ibkr
    hub: LiveDashboardHub = request.app['hub']
    try:
        statement = await asyncio.get_event_loop().run_in_executor(
            None, ibkr.fetch_flex_daily_statement)
        statement['live'] = {
            'current_equity': hub.balance,
            'account_currency': hub.account_currency,
        }
        return web.json_response(statement)
    except Exception as exc:
        return web.json_response({'error': str(exc)}, status=500)


async def _debug_positions_api(request: web.Request) -> web.Response:
    """Return in-memory position tracking state for diagnostics."""
    hub: LiveDashboardHub = request.app['hub']
    async with hub._lock:
        return web.json_response({
            'track_positions': hub.track_positions,
            'tracked_count': len(hub._tracked),
            'tracked_keys': sorted(hub._tracked.keys()),
            'snapshot_keys': sorted(hub._position_snapshots.keys()),
            'last_quote_pairs': sorted(hub._last_quotes.keys()),
        })


async def _position_health_api(request: web.Request) -> web.Response:
    """Return live positions, their bracket orders, and recent closed trades from IBKR."""

    import asyncio
    from datetime import timedelta

    hours = int(request.query.get('hours', '12'))

    def _load():
        from . import ibkr
        from .db import _connect, get_db_path, init_db

        # 1. Live positions
        raw_positions = ibkr.fetch_positions()
        positions = []
        if raw_positions:
            for p in raw_positions:
                direction = 'LONG' if p['size'] > 0 else 'SHORT'
                positions.append({
                    'pair': p['pair'],
                    'direction': direction,
                    'size': p['size'],
                    'avg_cost': p['avg_cost'],
                })

        # 2. Open orders with detail
        open_orders = []
        terminal = {'Filled', 'Cancelled', 'ApiCancelled', 'Inactive'}
        with ibkr._IBKR_LOCK:
            ib, connected = ibkr._get_connection()
            if connected:
                try:
                    all_orders = ib.reqAllOpenOrders()
                    for t in all_orders:
                        status = getattr(getattr(t, 'orderStatus', None), 'status', '') or ''
                        if status in terminal:
                            continue
                        o = t.order
                        c = t.contract
                        pair_id = ibkr._contract_to_pair(c)
                        if not pair_id:
                            continue
                        open_orders.append({
                            'pair': pair_id,
                            'action': getattr(o, 'action', ''),
                            'quantity': float(getattr(o, 'totalQuantity', 0)),
                            'order_type': getattr(o, 'orderType', ''),
                            'lmt_price': float(getattr(o, 'lmtPrice', 0) or 0),
                            'aux_price': float(getattr(o, 'auxPrice', 0) or 0),
                            'order_id': getattr(o, 'orderId', None),
                            'parent_id': getattr(o, 'parentId', None),
                            'oca_group': getattr(o, 'ocaGroup', ''),
                            'tif': getattr(o, 'tif', ''),
                            'status': status,
                            'order_ref': getattr(o, 'orderRef', ''),
                        })
                except Exception:
                    pass

        # 3. Closed trades from DB
        closed_trades = []
        try:
            db_path = get_db_path()
            init_db(db_path)
            conn = _connect(db_path)
            try:
                from datetime import datetime, timezone
                cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
                rows = conn.execute(
                    """SELECT pair, direction, status,
                              entry_price, sl_price, tp_price,
                              closed_price, close_reason, close_source,
                              detected_at, closed_at, pnl_pips,
                              open_units, opened_price, account_currency
                       FROM detected_signal
                       WHERE closed_at IS NOT NULL AND closed_at >= %s
                       ORDER BY closed_at DESC""",
                    (cutoff,),
                ).fetchall()
                cols = ['pair', 'direction', 'status', 'entry_price', 'sl_price',
                        'tp_price', 'closed_price', 'close_reason', 'close_source',
                        'detected_at', 'closed_at', 'pnl_pips',
                        'open_units', 'opened_price', 'account_currency']
                for row in rows:
                    d = {cols[i]: row[i] for i in range(len(cols))}
                    for ts_key in ('detected_at', 'closed_at'):
                        if d.get(ts_key):
                            d[ts_key] = str(d[ts_key])
                    if d.get('pnl_pips') is not None:
                        d['pnl_pips'] = float(d['pnl_pips'])
                    for px_key in ('entry_price', 'sl_price', 'tp_price', 'closed_price', 'opened_price'):
                        if d.get(px_key) is not None:
                            d[px_key] = float(d[px_key])
                    if d.get('open_units') is not None:
                        d['open_units'] = int(d['open_units'])
                    # Compute £ P&L amount
                    from .live_history import _compute_pnl_gbp
                    pnl_amount = _compute_pnl_gbp(d, conn)
                    d['pnl_amount'] = round(pnl_amount, 2) if pnl_amount is not None else None
                    closed_trades.append(d)
            finally:
                conn.close()
        except Exception:
            pass

        # Cross-reference: which positions have brackets?
        order_pairs = {o['pair'] for o in open_orders}
        for pos in positions:
            pos['has_brackets'] = pos['pair'] in order_pairs
            pos['bracket_orders'] = [o for o in open_orders if o['pair'] == pos['pair']]

        # Orphaned orders (orders with no matching position)
        position_pairs = {p['pair'] for p in positions}
        orphaned_orders = [o for o in open_orders if o['pair'] not in position_pairs]

        return {
            'positions': positions,
            'open_orders': open_orders,
            'orphaned_orders': orphaned_orders,
            'closed_trades': closed_trades,
            'hours': hours,
        }

    try:
        result = await asyncio.to_thread(_load)
        return web.json_response(result)
    except Exception as exc:
        return web.json_response({'error': str(exc)}, status=500)


async def _order_audit_log_api(request: web.Request) -> web.Response:
    """Return recent order audit log entries with optional filtering."""

    import asyncio
    from .db import _connect, get_db_path, init_db

    pair = (request.query.get('pair', '') or '').strip().upper() or None
    action = (request.query.get('action', '') or '').strip() or None
    limit = int(request.query.get('limit', '200'))

    def _load():
        db_path = get_db_path()
        init_db(db_path)
        conn = _connect(db_path)
        try:
            query = "SELECT id, event_ts, function_name, pair, direction, action, request_json, response_json, error, duration_ms, order_ids FROM order_audit_log"
            filters = []
            params: list = []
            if pair:
                filters.append("pair = %s")
                params.append(pair)
            if action:
                filters.append("action = %s")
                params.append(action)
            if filters:
                query += " WHERE " + " AND ".join(filters)
            query += " ORDER BY event_ts DESC"
            if limit:
                query += " LIMIT %s"
                params.append(limit)
            cursor = conn.execute(query, params)
            columns = [d[0] for d in cursor.description]
            rows = []
            for row in cursor.fetchall():
                d = {columns[i]: row[i] for i in range(len(columns))}
                if d.get('event_ts'):
                    d['event_ts'] = str(d['event_ts'])
                rows.append(d)
            return rows
        finally:
            conn.close()

    try:
        rows = await asyncio.to_thread(_load)
        pairs = sorted({r['pair'] for r in rows if r.get('pair')})
        actions = sorted({r['action'] for r in rows if r.get('action')})
        return web.json_response({'entries': rows, 'pairs': pairs, 'actions': actions, 'count': len(rows)})
    except Exception as exc:
        return web.json_response({'error': str(exc)}, status=500)


async def _live_diary_api(_request: web.Request) -> web.Response:
    """Return live trades for the diary calendar."""

    from .live_history import load_live_diary_trades

    trades = load_live_diary_trades()
    return web.json_response({'trades': trades})


async def _live_trade_api(request: web.Request) -> web.Response:
    """Load a live trade payload by signal id (preferred), falling back to latest open trade."""

    signal_id = (request.query.get('signal_id') or '').strip()
    pair = (request.query.get('pair') or '').strip().upper() or None
    direction = (request.query.get('direction') or '').strip().upper() or None

    trade = None

    if signal_id:
        trade = load_detected_signal(signal_id)
        if trade is not None:
            return web.json_response({'trade': trade})

    if trade is None and not pair:
        if signal_id:
            return web.json_response({'error': 'Trade not found'}, status=404)
        return web.json_response({'error': 'Missing signal_id query parameter'}, status=400)

    if direction and direction not in {'LONG', 'SHORT'}:
        return web.json_response({'error': 'Invalid direction query parameter'}, status=400)

    from .live_history import load_detected_signals

    signals = load_detected_signals(pair=pair, limit=80)
    if direction:
        signals = [
            t for t in signals
            if str(t.get('direction') or '').upper() == direction
        ]
    if not signals:
        return web.json_response({'error': 'Trade not found'}, status=404)

    open_signals = [
        t for t in signals
        if str(t.get('status') or '').upper() in {'OPEN', 'PARTIAL'}
    ]
    trade = open_signals[0] if open_signals else signals[0]
    return web.json_response({'trade': trade})



def _spa_index_response() -> web.StreamResponse:
    """Serve the built React shell or fail with a clear build instruction."""

    index_path = REACT_BUILD_DIR / 'index.html'
    legacy_index_path = WEB_DIR / 'index.html'
    if index_path.exists():
        # If the React build output is incomplete, avoid serving a broken shell.
        react_assets_dir = REACT_BUILD_DIR / 'assets'
        has_react_bundle = (
            react_assets_dir.exists()
            and bool(list(react_assets_dir.glob('index-*.js')))
            and bool(list(react_assets_dir.glob('index-*.css')))
        )
        if has_react_bundle:
            return web.FileResponse(index_path)
    if legacy_index_path.exists():
        return web.FileResponse(legacy_index_path)

    if index_path.exists():
        return web.FileResponse(index_path)
    return web.Response(
        status=503,
        text=(
            'React live web build not found. Run `npm install` then `npm run build` '
            'from the repo root before starting the dashboard.'
        ),
        content_type='text/plain',
    )


async def _chart_data(request: web.Request) -> web.StreamResponse:
    """Return OHLC data for a pair from the accumulator."""

    hub: LiveDashboardHub = request.app["hub"]
    pair = request.query.get('pair', '').upper()
    if not pair or pair not in hub.pairs:
        return web.json_response({'error': 'unknown pair'}, status=400)

    tf = hub.chart_tf
    if tf == '1m':
        df = hub._accumulator.get_minute_df(pair, tail_n=1000)
    else:
        df = hub._accumulator.get_hourly_df(pair, tail_n=500)
    bars = []
    if not df.empty:
        for ts, row in df.iterrows():
            bars.append({
                'time': int(ts.timestamp()),
                'open': float(row['Open']),
                'high': float(row['High']),
                'low': float(row['Low']),
                'close': float(row['Close']),
            })

    pair_info = hub.pairs.get(pair, {})
    zone_data = hub._scanner._zones.get(pair)
    support, resistance = (None, None)
    if zone_data:
        _, _, all_zones = zone_data
        # Re-derive nearest zones using the latest price (same as the scan does)
        last_price = bars[-1]['close'] if bars else None
        live_price = hub._last_quotes.get(pair)
        ref_price = live_price or last_price
        if ref_price and all_zones:
            from .strategy import get_tradeable_zones
            s, r = get_tradeable_zones(all_zones, ref_price)
        else:
            s, r = zone_data[0], zone_data[1]
        if s:
            support = {'lower': s.lower, 'upper': s.upper, 'strength': s.strength}
        if r:
            resistance = {'lower': r.lower, 'upper': r.upper, 'strength': r.strength}

    return web.json_response({
        'pair': pair,
        'decimals': pair_info.get('decimals', 5),
        'bars': bars,
        'support': support,
        'resistance': resistance,
    })


async def _index(_request: web.Request) -> web.StreamResponse:
    """Serve the dashboard shell."""

    return _spa_index_response()


def _dashboard_url(port: int) -> str:
    """Return the dashboard URL."""

    return f'http://127.0.0.1:{port}/'


def _origin_allowed(origin: str, request: web.Request) -> bool:
    """Check whether a request origin exactly matches the expected host."""

    expected_origin = f'{request.scheme}://{request.host}'

    if origin.rstrip('/') == expected_origin:
        return True

    parsed_origin = urlparse(origin)
    parsed_expected = urlparse(expected_origin)

    if not parsed_origin.scheme or not parsed_origin.netloc:
        return False

    if parsed_origin.scheme != parsed_expected.scheme:
        return False

    if parsed_origin.port != parsed_expected.port:
        return False

    if parsed_origin.hostname is None or parsed_expected.hostname is None:
        return False

    origin_host = parsed_origin.hostname.lower()
    expected_host = parsed_expected.hostname.lower()

    local_aliases = {'localhost', '127.0.0.1', '::1', '0.0.0.0'}
    if origin_host in local_aliases and expected_host in local_aliases:
        return True

    return origin_host == expected_host


def _validate_dashboard_request(request: web.Request) -> None:
    """Reject dashboard requests with a mismatched origin."""

    origin = request.headers.get('Origin')
    if not origin:
        return
    if _origin_allowed(origin, request):
        return
    raise web.HTTPForbidden(text='Invalid dashboard origin')


def _validate_websocket_request(request: web.Request) -> None:
    """Reject websocket requests with a bad token or mismatched origin."""

    _validate_dashboard_request(request)


async def _set_execution_mode(request: web.Request) -> web.Response:
    """Pause or resume new order placement for the live dashboard."""

    _validate_dashboard_request(request)
    try:
        payload = await request.json()
    except Exception:
        return web.json_response({'error': 'Invalid JSON body'}, status=400)

    paused = payload.get('paused')
    if not isinstance(paused, bool):
        return web.json_response({'error': 'Expected boolean "paused" field'}, status=400)

    hub: LiveDashboardHub = request.app["hub"]
    try:
        state = await hub.set_execution_paused(paused)
    except RuntimeError as exc:
        return web.json_response({'error': str(exc)}, status=409)
    return web.json_response({'state': state})


async def _fill_cache(request: web.Request) -> web.Response:
    """Fill any cache gaps for configured pairs and intervals."""

    _validate_dashboard_request(request)

    raw_days = request.query.get('days', '365')
    try:
        target_days = int(raw_days)
    except ValueError:
        return web.json_response({'error': 'Invalid "days" value; must be an integer'}, status=400)

    hub: LiveDashboardHub = request.app["hub"]
    result = await hub.fill_cache(target_days=target_days)
    if result.get('status') == 'running':
        return web.json_response(result, status=409)
    if result.get('status') == 'invalid':
        return web.json_response(result, status=400)
    return web.json_response(result)


async def _rerun_backtest(request: web.Request) -> web.Response:
    """Re-run a full backtest using dashboard configuration."""

    _validate_dashboard_request(request)

    hub: LiveDashboardHub = request.app["hub"]
    result = await hub.run_backtest()
    if result.get('status') == 'running':
        return web.json_response(result, status=409)
    if result.get('status') == 'invalid':
        return web.json_response(result, status=400)
    return web.json_response(result)


async def _close_tracked_position(request: web.Request) -> web.Response:
    """Close a tracked position by submitting an opposite market order."""

    _validate_dashboard_request(request)
    try:
        payload = await request.json()
    except Exception:
        return web.json_response({'error': 'Invalid JSON body'}, status=400)

    pair = payload.get('pair')
    direction = payload.get('direction')
    if not pair or not isinstance(direction, str):
        return web.json_response(
            {'error': 'Expected JSON body with "pair" and "direction".'},
            status=400,
        )

    hub: LiveDashboardHub = request.app["hub"]
    try:
        result = await hub.close_tracked_position(pair=pair, direction=direction)
        if result.get('result', {}).get('status') == 'FAILED':
            return web.json_response(
                {'error': result.get('message', 'Failed to submit close order.'), 'result': result},
                status=502,
            )
        return web.json_response(result)
    except ValueError as exc:
        return web.json_response({'error': str(exc)}, status=400)
    except LookupError as exc:
        return web.json_response({'error': str(exc)}, status=404)
    except RuntimeError as exc:
        return web.json_response({'error': str(exc)}, status=409)
    except Exception as exc:
        return web.json_response({'error': str(exc)}, status=500)


async def _neutralize_currency(request: web.Request) -> web.Response:
    """Neutralize a residual currency balance by submitting an offsetting FX order."""

    _validate_dashboard_request(request)
    try:
        payload = await request.json()
    except Exception:
        return web.json_response({'error': 'Invalid JSON body'}, status=400)

    currency = (payload.get('currency') or '').strip().upper()
    amount = payload.get('amount')
    if not currency or amount is None:
        return web.json_response(
            {'error': 'Expected JSON body with "currency" and "amount".'},
            status=400,
        )

    try:
        amount = float(amount)
    except (TypeError, ValueError):
        return web.json_response({'error': 'Amount must be a number.'}, status=400)

    if abs(amount) < 1:
        return web.json_response({'error': 'Amount too small to neutralize.'}, status=400)

    hub: LiveDashboardHub = request.app["hub"]
    account_currency = hub.account_currency or 'GBP'

    import asyncio
    try:
        result = await asyncio.to_thread(
            ibkr.neutralize_currency_balance,
            currency,
            amount,
            account_currency,
        )
    except Exception as exc:
        return web.json_response({'error': str(exc)}, status=500)

    if result is None:
        return web.json_response(
            {'error': f'Failed to submit neutralization order for {currency}.'},
            status=502,
        )

    # Nudge housekeeping to refresh balances quickly
    hub._housekeeping_nudge.set()

    action = 'Sell' if amount > 0 else 'Buy'
    return web.json_response({
        'message': f'{action} {abs(int(amount)):,} {currency} submitted.',
        'result': result,
    })


def _register_fill_route(app: web.Application, handler: object) -> None:
    """Register all known dashboard fill routes for compatibility."""

    fill_routes = [
        '/api/fill',
        '/api/fill/',
        '/api/fill-cache',
        '/fill',
        '/fill/',
        '/fill-cache',
        '/fill_cache',
    ]
    for route in fill_routes:
        app.router.add_route('POST', route, handler)


async def _rebuild_ui(request: web.Request) -> web.Response:
    """Run npm build to rebuild the React frontend."""

    import subprocess
    _validate_dashboard_request(request)

    repo_root = Path(__file__).resolve().parent.parent
    try:
        result = subprocess.run(
            ['npm', 'run', 'build'],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            timeout=60,
            shell=True,
        )
        if result.returncode != 0:
            return web.json_response({
                'status': 'error',
                'message': result.stderr or result.stdout or 'Build failed',
            }, status=500)
        return web.json_response({'status': 'ok', 'message': 'React build complete. Hard-refresh your browser.'})
    except Exception as e:
        return web.json_response({'status': 'error', 'message': str(e)}, status=500)


async def _shutdown(request: web.Request) -> web.Response:
    """Gracefully shut down the live dashboard server."""

    import os
    _validate_dashboard_request(request)

    hub: LiveDashboardHub = request.app["hub"]

    async def _do_shutdown():
        await asyncio.sleep(0.3)
        try:
            await hub.stop()
        except Exception:
            pass
        os._exit(0)

    asyncio.ensure_future(_do_shutdown())
    return web.json_response({'status': 'shutting down'})


async def _restart(request: web.Request) -> web.Response:
    """Restart the live dashboard server process."""

    import os
    import sys
    _validate_dashboard_request(request)

    hub: LiveDashboardHub = request.app["hub"]

    async def _do_restart():
        await asyncio.sleep(0.5)
        try:
            await hub.stop()
        except Exception:
            pass
        # On Windows, os.execv spawns a new process rather than replacing,
        # so use subprocess + _exit to avoid port conflicts.
        if sys.platform == 'win32':
            import subprocess
            subprocess.Popen([sys.executable] + sys.argv)
            os._exit(0)
        else:
            os.execv(sys.executable, [sys.executable] + sys.argv)

    asyncio.ensure_future(_do_restart())
    return web.json_response({'status': 'restarting'})


async def _websocket(request: web.Request) -> web.StreamResponse:
    """Handle websocket clients for the live dashboard."""

    _validate_websocket_request(request)
    hub: LiveDashboardHub = request.app["hub"]
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)
    try:
        await hub.register(ws)
        async for _ in ws:
            continue
    finally:
        await hub.unregister(ws)

    return ws


async def _startup(app: web.Application) -> None:
    """Start background services when aiohttp comes up."""

    # Keep startup non-blocking so the dashboard shell is visible while
    # backfill/bootstrap runs in the background.
    app["hub"]._loop = asyncio.get_running_loop()
    await app["hub"]._broadcast_log('info', 'Dashboard startup task scheduled.')

    async def _hub_start_with_logging():
        try:
            await app["hub"].start()
        except Exception as exc:
            import traceback
            print(f"  Hub startup failed: {exc}")
            traceback.print_exc()

    app["_hub_task"] = asyncio.create_task(_hub_start_with_logging())


async def _cleanup(app: web.Application) -> None:
    """Stop background services during shutdown."""
    from .live_history import record_system_event
    record_system_event('shutdown')

    hub_task = app.get("_hub_task")
    if hub_task and not hub_task.done():
        hub_task.cancel()
        try:
            await hub_task
        except (asyncio.CancelledError, Exception):
            pass

    await app["hub"].stop()


def run_live_web_app(
    *,
    pairs,
    params,
    interval: int,
    zone_history_days: int,
    track_positions: bool,
    balance: float | None,
    risk_pct: float,
    account_currency: str | None,
    execute_orders: bool,
    strategy_label: str | None,
    client_id: int | None,
    port: int,
    open_browser: bool,
    execution_mode: str,
    chart_tf: str = '1h',
    hourly_days: int = 1,
) -> None:
    """Run the browser-based live dashboard server."""

    _configure_windows_event_loop_policy()
    init_db()
    app = web.Application()
    app["hub"] = LiveDashboardHub(
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
        port=port,
        execution_mode=execution_mode,
        chart_tf=chart_tf,
        hourly_days=hourly_days,
    )
    from .replay import handle_replay, handle_replay_bars, handle_replay_dates, handle_replay_refresh, handle_replay_presets
    from .replay import (
        handle_backtest_trades_api,
        handle_backtest_trades_page,
        handle_backtest_diary_api,
        handle_backtest_diary_page,
        handle_trade_log_api,
    )

    app.router.add_post('/api/rebuild-ui', _rebuild_ui)
    app.router.add_get('/', _index)
    app.router.add_get('/ws', _websocket)
    app.router.add_get('/chart', _chart_page)
    app.router.add_get('/api/chart-data', _chart_data)
    app.router.add_post('/api/position-close', _close_tracked_position)
    app.router.add_post('/position-close', _close_tracked_position)
    app.router.add_post('/api/neutralize-currency', _neutralize_currency)
    app.router.add_post('/api/execution-mode', _set_execution_mode)
    _register_fill_route(app, _fill_cache)
    async def _force_housekeeping(request: web.Request) -> web.Response:
        _validate_dashboard_request(request)
        hub: LiveDashboardHub = request.app["hub"]
        hub._housekeeping_nudge.set()
        return web.json_response({'message': 'Housekeeping triggered.'})

    app.router.add_post('/api/housekeeping', _force_housekeeping)
    app.router.add_post('/api/shutdown', _shutdown)
    app.router.add_post('/api/restart', _restart)
    app.router.add_post('/api/backtest-rerun', _rerun_backtest)
    app.router.add_post('/backtest-rerun', _rerun_backtest)
    app.router.add_post('/api/backtest-rerun/', _rerun_backtest)
    app.router.add_post('/backtest-rerun/', _rerun_backtest)
    app.router.add_get('/replay', _index)
    app.router.add_get('/backtest-trades', _index)
    app.router.add_get('/live-vs-backtest', _index)
    app.router.add_get('/api/backtest/trades', handle_backtest_trades_api)
    app.router.add_get('/backtest-diary', _index)
    app.router.add_get('/api/backtest/diary', handle_backtest_diary_api)
    app.router.add_get('/live-diary', _index)
    app.router.add_get('/live-trade', _index)
    app.router.add_get('/api/live-diary', _live_diary_api)
    app.router.add_get('/api/account-history', _account_history_api)
    app.router.add_get('/api/live-trade', _live_trade_api)
    app.router.add_get('/trade-log', _index)
    app.router.add_get('/api/trade-log', handle_trade_log_api)
    app.router.add_get('/order-audit-log', _index)
    app.router.add_get('/api/order-audit-log', _order_audit_log_api)
    app.router.add_get('/position-health', _index)
    app.router.add_get('/api/position-health', _position_health_api)
    app.router.add_get('/api/debug/positions', _debug_positions_api)
    app.router.add_get('/api/daily-statement', _daily_statement_api)
    app.router.add_get('/api/daily-reconciliation', _daily_reconciliation_api)
    app.router.add_get('/api/replay', handle_replay)
    app.router.add_get('/api/replay/bars', handle_replay_bars)
    app.router.add_get('/api/replay/dates', handle_replay_dates)
    app.router.add_post('/api/replay/refresh', handle_replay_refresh)
    app.router.add_get('/api/replay/presets', handle_replay_presets)
    app.router.add_static('/static/', str(WEB_DIR), show_index=False)
    @web.middleware
    async def no_cache(request, handler):
        response = await handler(request)
        if request.path.startswith('/static/') or request.path in (
            '/',
            '/chart',
            '/trade-log',
            '/replay',
            '/live-vs-backtest',
            '/order-audit-log',
            '/position-health',
        ):
            response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        return response

    app.middlewares.append(no_cache)
    app.on_startup.append(_startup)
    app.on_cleanup.append(_cleanup)

    url = _dashboard_url(port)
    print(f'\n  Live dashboard server: {url}')
    print('  Ctrl+C to stop.')

    if open_browser:
        threading.Timer(0.5, lambda: webbrowser.open(url)).start()

    web.run_app(app, host='0.0.0.0', port=port, print=None)
