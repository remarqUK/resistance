"""Aiohttp live dashboard with IBKR quote subscriptions."""

from __future__ import annotations

import asyncio
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import datetime, timedelta
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
    build_live_size_plans,
    collect_scan_rows,
    execute_signal_plans,
    load_closed_trade_summaries,
    refresh_pair_row_price,
)
from .live_history import (
    enqueue_write_async,
    load_execution_activity,
    record_detected_signals,
    record_exit_signal,
    record_execution_results,
    start_background_writer,
    stop_background_writer,
)
from .data import _remaining_days_to_fetch, download_single_interval
from .db import get_cached_range, init_db
from .portfolio import build_portfolio_state, closed_trade_summary_from_row, get_entry_block
from .live_stream import StreamingScanner
from .positions import calc_pnl_pips, pair_pip, process_hourly_exit_bars, sync_positions


WEB_DIR = Path(__file__).resolve().parent / 'web_live'
RUN_PY_PATH = Path(__file__).resolve().parent.parent / 'run.py'
LOG_LIMIT = 80
ALERT_LIMIT = 200
EXECUTION_LIMIT = 200
_BACKTEST_PROGRESS_RE = re.compile(r'^\s*\[(\d+)\s*/\s*(\d+)\]\s+([A-Za-z0-9]+)')


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
    ) -> None:
        self.pairs = pairs
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

        self._pair_rows: dict[str, PairScanRow] = {}
        self._tracked: dict[str, dict] = {}
        self._position_snapshots: dict[str, dict] = {}
        self._alerts: deque[dict] = deque(maxlen=ALERT_LIMIT)
        self._execution_results = deque(maxlen=EXECUTION_LIMIT)
        self._last_quotes: dict[str, float] = {}
        self._log: deque[dict] = deque(maxlen=LOG_LIMIT)

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
        self._portfolio_state = build_portfolio_state([], params=params, current_balance=balance)
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
            'position_count': len(self._tracked),
            'execution_enabled': self._execution_enabled(),
            'execution_available': self._execution_available,
            'execution_paused': self._execution_paused,
            'strategy_label': self.strategy_label or 'Strategy',
            'mode': 'scanner + positions' if self.track_positions else 'scanner',
            'url': self._ws_url(),
            'backfill': dict(self._backfill_progress),
            'fill': dict(self._fill_progress),
            'backtest': dict(self._backtest_progress),
            'balance': self.balance,
            'account_currency': self.account_currency,
            'risk_pct': self.risk_pct * 100.0 if self.risk_pct is not None else None,
        }

    def _append_log(self, level: str, message: str) -> dict:
        """Append a structured log entry."""

        entry = {
            'ts': datetime.now().strftime('%H:%M:%S'),
            'level': level,
            'message': message,
        }
        self._log.append(entry)
        return entry

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

    def _find_fill_gaps(self, target_days: int) -> list[tuple[str, dict, str]]:
        """Find cache gaps for supported intervals."""

        if target_days <= 0:
            return []

        min_rows = {
            '1d': int(target_days * 0.7),
            '1h': int(target_days * 16),
            '1m': int(target_days * 1000),
        }
        now = pd.Timestamp.now(tz='UTC')
        gaps: list[tuple[str, dict, str]] = []

        for pair_id, pair_info in self.pairs.items():
            ticker = pair_info['ticker']
            for interval in ('1d', '1h', '1m'):
                cached_range = get_cached_range(ticker, interval)
                if cached_range is None:
                    gaps.append((pair_id, pair_info, interval))
                    continue

                first_ts, last_ts, rows = cached_range
                if int(rows) < min_rows.get(interval, 0):
                    gaps.append((pair_id, pair_info, interval))
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
                if fetch_days > 0:
                    gaps.append((pair_id, pair_info, interval))

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

        work_items = [(pair_id, pair_info, interval) for pair_id, pair_info, interval in gaps]
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

        def _run_work_item(pair_id: str, pair_info: dict, interval: str) -> int:
            client_id = _thread_client_id()
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
                    executor.submit(_run_work_item, pair_id, pair_info, interval): (pair_id, pair_info, interval)
                    for pair_id, pair_info, interval in pending
                }

                for future in as_completed(futures):
                    pair_id, pair_info, interval = futures[future]
                    try:
                        _ = future.result()
                        total_items_processed += 1
                    except Exception:
                        failed.append((pair_id, pair_info, interval))
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
        rows: list[dict] = []
        for key in sorted(self._tracked):
            info = self._tracked[key]
            trade = info['trade']
            snap = self._position_snapshots.get(key, {})
            pair = info['pair']
            status = alert_lookup.get(key)
            if status is None:
                status = 'PARTIAL' if info.get('signal_status') == 'PARTIAL' else 'OK'
            rows.append(
                {
                    'pair': pair,
                    'direction': trade.direction,
                    'size': int(abs(info.get('ibkr_size') or 0)),
                    'entry_price': trade.entry_price,
                    'current_price': snap.get('current_price'),
                    'pnl_pips': snap.get('pnl_pips'),
                    'status': status,
                    'decimals': self.pairs.get(pair, {}).get('decimals', 5),
                }
            )
        return rows

    def _serialize_alerts(self) -> list[dict]:
        """Serialize exit alerts."""

        rows = []
        for alert in self._alerts:
            rows.append(
                {
                    **alert,
                    'decimals': self.pairs.get(alert['pair'], {}).get('decimals', 5),
                }
            )
        return rows

    def _serialize_executions(self) -> list[dict]:
        """Serialize execution results."""

        return [
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
            }
            for result in self._execution_results
        ]

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
                    quote_source=(
                        str(row.get('quote_source'))
                        if row.get('quote_source') is not None
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

        return {
            'summary': dict(self.summary),
            'pairs': {
                pair: self._serialize_pair_row(row)
                for pair, row in sorted(self._pair_rows.items())
            },
            'signals': signals,
            'positions': self._serialize_positions(),
            'alerts': self._serialize_alerts(),
            'executions': self._serialize_executions(),
            'log': list(self._log),
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
            row = self._pair_rows.get(pair)
            if row is None:
                return

            updated_row = refresh_pair_row_price(row, price)
            self._pair_rows[pair] = updated_row

            positions_changed = False
            for key, info in self._tracked.items():
                if info['pair'] != pair:
                    continue
                self._position_snapshots[key] = {
                    'current_price': price,
                    'pnl_pips': calc_pnl_pips(info['trade'], price, pair_pip(pair), self.params),
                }
                positions_changed = True

            # --- Skip all trading logic until backfill is complete ---
            if not self._backfill_done:
                summary = dict(self.summary)
                row_payload = self._serialize_pair_row(updated_row)

        if not self._backfill_done:
            await self._broadcast({'type': 'pair_update', 'row': row_payload, 'summary': summary})
            return

        exit_signal_writes: list[tuple[str, str, float | None]] = []
        async with self._lock:
            # --- Tick exit checks (inline — pure float math, no I/O) ---
            tick_alerts = self._scanner.check_tick_exits(pair, price, self._tracked)
            for alert in tick_alerts:
                alert_key = f"{alert['pair']}:{alert['direction']}"
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

            summary = dict(self.summary)
            row_payload = self._serialize_pair_row(updated_row)
            state_payload = self._export_state() if positions_changed else None

        for signal_id, exit_reason, exit_price in exit_signal_writes:
            await enqueue_write_async(
                lambda s=signal_id, r=exit_reason, p=exit_price: record_exit_signal(
                    s, exit_reason=r, exit_price=p,
                )
            )

        if positions_changed and state_payload is not None:
            await self._broadcast({'type': 'snapshot', 'state': state_payload})
            return

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
                self._append_log(level, f'{source.title()} {result.status}: {result.pair} {result.direction}')
                if result.status == 'OPEN':
                    self._tick_pending_pairs.discard(result.pair)
                elif result.order_id is not None:
                    self._tick_pending_pairs.add(result.pair)
                row = self._pair_rows.get(result.pair)
                if row is not None:
                    if result.status == 'PARTIAL':
                        self._pair_rows[result.pair] = replace(row, state='PARTIAL', note=result.note, signal=None)
                    elif result.status == 'OPEN':
                        self._pair_rows[result.pair] = replace(row, state='OPEN', note=result.note, signal=None)
                    elif result.order_id is not None:
                        self._pair_rows[result.pair] = replace(row, state='PENDING', note=result.note, signal=None)
            self.summary = self._build_summary(status=summary_status)
            state = self._export_state()

        await self._broadcast({'type': 'snapshot', 'state': state})

    def _queue_quote_update(self, pair: str, price: float) -> None:
        """Marshal a thread-side quote callback onto the asyncio loop."""

        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._create_tracked_task, pair, price)

    def _create_tracked_task(self, pair: str, price: float) -> None:
        """Create an asyncio task and track it so it can be cleaned up."""

        task = asyncio.create_task(self._handle_quote_update(pair, price))
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    def _queue_bar_update(self, pair: str, bar) -> None:
        """Marshal a real-time bar callback onto the asyncio loop."""

        if self._loop is None:
            return

        def _schedule():
            task = asyncio.create_task(self._handle_bar_update(pair, bar))
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)

        self._loop.call_soon_threadsafe(_schedule)

    async def _handle_bar_update(self, pair: str, bar) -> None:
        """Process a 5-second real-time bar: update accumulator + feed quote/exit handling."""

        price = float(getattr(bar, 'close', 0) or 0)
        if price <= 0:
            return

        # Update the bar accumulator (inline — fast)
        self._accumulator.on_realtime_bar(pair, bar)

        # Delegate to the existing quote handler for display and tick exits
        await self._handle_quote_update(pair, price)

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
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)

        self._loop.call_soon_threadsafe(_schedule)

    async def _handle_hourly_bar_complete(self, pair: str, bar_time) -> None:
        """Run bar-shape exit checks and full signal evaluation on hourly bar completion."""

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

            tracked_pairs: dict[str, set[str]] = {}
            for info in self._tracked.values():
                tracked_pair = info.get('pair')
                trade = info.get('trade')
                if tracked_pair and trade:
                    tracked_pairs.setdefault(tracked_pair, set()).add(trade.direction)
            blocked = set(self._tick_pending_pairs)
            state = self._export_state()

        signal = await self._loop.run_in_executor(
            self._scan_executor,
            lambda: self._scanner.evaluate_completed_bar(
                pair,
                completed_close,
                tracked_pairs={p: set(dirs) for p, dirs in tracked_pairs.items()},
                blocked_pairs=set(blocked),
                hourly_df=hourly_df,
            ),
        )
        if signal is not None:
            await self._handle_signal(signal, source='hourly')
            return

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

    def _backfill_data(self) -> None:
        """Fetch historical daily + hourly data for all pairs (runs in executor)."""

        from .data import fetch_daily_data, fetch_hourly_data
        from .levels import detect_zones
        from .strategy import get_tradeable_zones as _get_tz

        pair_list = list(self.pairs.items())
        total = len(pair_list)

        pair_status = self._backfill_progress['pair_status']

        # Phase 1: Daily data + zones
        self._backfill_progress.update(phase='zones', completed=0, total=total)
        for idx, (pair_id, pair_info) in enumerate(pair_list):
            pair_status[pair_id] = 'loading zones'
            self._backfill_progress.update(current_pair=pair_id, completed=idx)
            ticker = pair_info.get('ticker')
            if not ticker:
                pair_status[pair_id] = 'no ticker'
                continue
            try:
                daily_df = fetch_daily_data(ticker, days=self.zone_history_days)
                if not daily_df.empty:
                    zones = detect_zones(daily_df)
                    ref_price = float(daily_df['Close'].iloc[-1])
                    support, resistance = _get_tz(zones, ref_price)
                    self._scanner._zones[pair_id] = (support, resistance, zones)
                    pair_status[pair_id] = 'zones loaded'
                else:
                    pair_status[pair_id] = 'no daily data'
            except Exception:
                pair_status[pair_id] = 'zones failed'
        self._backfill_progress.update(completed=total)

        # Phase 2: Hourly data + accumulator seeding
        self._backfill_progress.update(phase='hourly', completed=0)
        for idx, (pair_id, pair_info) in enumerate(pair_list):
            prev = pair_status.get(pair_id, '')
            pair_status[pair_id] = 'loading hourly'
            self._backfill_progress.update(current_pair=pair_id, completed=idx)
            ticker = pair_info.get('ticker')
            if not ticker:
                continue
            try:
                hourly_df = fetch_hourly_data(ticker, days=7)
                self._accumulator.seed(pair_id, hourly_df)
                pair_status[pair_id] = 'ready'
            except Exception:
                pair_status[pair_id] = 'hourly failed'
        self._backfill_progress.update(completed=total)

        # Phase 3: Initial scan rows from backfilled data
        self._backfill_progress.update(phase='scan', current_pair=None)
        hourly_cache = {}
        for pair_id in self._accumulator.seeded_pairs:
            ticker = self.pairs.get(pair_id, {}).get('ticker')
            if ticker:
                hourly_cache[ticker] = self._accumulator.get_hourly_df(pair_id)
        closed_trades = load_closed_trade_summaries()
        portfolio_state = build_portfolio_state(closed_trades, params=self.params)

        signals, pair_rows = collect_scan_rows(
            pairs=self.pairs,
            params=self.params,
            zone_history_days=self.zone_history_days,
            hourly_data_cache=hourly_cache,
            portfolio_state=portfolio_state,
        )
        return signals, pair_rows, closed_trades

    async def _run_backfill(self) -> None:
        """Run backfill in executor and publish progress to clients."""

        async with self._lock:
            self.summary = self._build_summary(status='backfilling')
        await self._broadcast({'type': 'scan_status', 'summary': dict(self.summary)})

        # Start a progress broadcast task
        progress_stop = asyncio.Event()

        async def _broadcast_progress():
            while not progress_stop.is_set():
                async with self._lock:
                    self.summary = self._build_summary(status='backfilling')
                    summary = dict(self.summary)
                await self._broadcast({'type': 'backfill_progress', 'summary': summary})
                try:
                    await asyncio.wait_for(progress_stop.wait(), timeout=0.5)
                except asyncio.TimeoutError:
                    pass

        progress_task = asyncio.create_task(_broadcast_progress())

        try:
            signals, pair_rows, closed_trades = await self._loop.run_in_executor(
                self._scan_executor,
                self._backfill_data,
            )
        except Exception as exc:
            progress_stop.set()
            await progress_task
            async with self._lock:
                self._append_log('error', f'Backfill failed: {exc}')
                self.summary = self._build_summary(status='error')
            await self._broadcast({'type': 'error', 'summary': dict(self.summary), 'message': str(exc)})
            return

        progress_stop.set()
        await progress_task

        self._backfill_progress.update(phase='done', current_pair=None)

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
            self._pair_rows = {row.pair: row for row in pair_rows}
            self._tracked = tracked
            if balance is not None:
                self.balance = balance
            if currency is not None:
                self.account_currency = currency
            self._portfolio_state = build_portfolio_state(closed_trades, params=self.params)
            for row in closed_rows:
                summary = closed_trade_summary_from_row(row)
                if summary is not None:
                    self._portfolio_state.record_closed_trade(summary)
            self._portfolio_state.sync_balance(self.balance)
            self._tick_pending_pairs = set()
            self._tick_exit_alerted = set()
            self._backfill_done = True
            self._append_log('success', f'Backfill complete: {len(pair_rows)} pairs, {len(signals)} signals')
            self.summary = self._build_summary(status='live')
            state = self._export_state()

        await self._broadcast({'type': 'snapshot', 'state': state})

    async def _housekeeping_loop(self) -> None:
        """Low-frequency periodic tasks: position sync, zone refresh, balance."""

        while True:
            await asyncio.sleep(300)  # 5 minutes

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

                    return tracked, balance, currency, closed_rows

                tracked, balance, currency, closed_rows = await self._loop.run_in_executor(
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
                    if closed_rows:
                        for row in closed_rows:
                            summary = closed_trade_summary_from_row(row)
                            if summary is not None:
                                self._portfolio_state.record_closed_trade(summary)
                    self._portfolio_state.sync_balance(self.balance)
                    self._tick_pending_pairs = set()
                    self._tick_exit_alerted = set()
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
        start_background_writer()

        # Phase 1: backfill historical data with progress
        await self._run_backfill()
        await self._loop.run_in_executor(
            self._scan_executor,
            self._hydrate_execution_activity,
        )
        async with self._lock:
            self.summary = self._build_summary(status=self.summary.get('status', 'live'))

        # Phase 2: start real-time bar streaming
        self._quote_thread = threading.Thread(
            target=self._run_realtime_bar_stream,
            name='ibkr-realtime-bars',
            daemon=True,
        )
        self._quote_thread.start()

        # Phase 3: start low-frequency housekeeping
        self._scan_task = asyncio.create_task(self._housekeeping_loop())

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


async def _chart_data(request: web.Request) -> web.StreamResponse:
    """Return OHLC data for a pair from the accumulator."""

    hub: LiveDashboardHub = request.app["hub"]
    pair = request.query.get('pair', '').upper()
    if not pair or pair not in hub.pairs:
        return web.json_response({'error': 'unknown pair'}, status=400)

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
    zones = hub._scanner._zones.get(pair)
    support, resistance = (None, None)
    if zones:
        s, r, _ = zones
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

    return web.FileResponse(WEB_DIR / 'index.html')


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

    await app["hub"].start()


async def _cleanup(app: web.Application) -> None:
    """Stop background services during shutdown."""

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
) -> None:
    """Run the browser-based live dashboard server."""

    _configure_windows_event_loop_policy()
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
    )
    from .replay import handle_replay, handle_replay_bars, handle_replay_dates, handle_replay_page, handle_replay_refresh, handle_replay_presets
    from .replay import (
        handle_backtest_trades_api,
        handle_backtest_trades_page,
        handle_backtest_diary_api,
        handle_backtest_diary_page,
        handle_trade_log_page,
        handle_trade_log_api,
    )

    app.router.add_get('/', _index)
    app.router.add_get('/ws', _websocket)
    app.router.add_get('/chart', _chart_page)
    app.router.add_get('/api/chart-data', _chart_data)
    app.router.add_post('/api/execution-mode', _set_execution_mode)
    _register_fill_route(app, _fill_cache)
    app.router.add_post('/api/shutdown', _shutdown)
    app.router.add_post('/api/backtest-rerun', _rerun_backtest)
    app.router.add_post('/backtest-rerun', _rerun_backtest)
    app.router.add_post('/api/backtest-rerun/', _rerun_backtest)
    app.router.add_post('/backtest-rerun/', _rerun_backtest)
    app.router.add_get('/replay', handle_replay_page)
    app.router.add_get('/backtest-trades', handle_backtest_trades_page)
    app.router.add_get('/api/backtest/trades', handle_backtest_trades_api)
    app.router.add_get('/backtest-diary', handle_backtest_diary_page)
    app.router.add_get('/api/backtest/diary', handle_backtest_diary_api)
    app.router.add_get('/trade-log', handle_trade_log_page)
    app.router.add_get('/api/trade-log', handle_trade_log_api)
    app.router.add_get('/api/replay', handle_replay)
    app.router.add_get('/api/replay/bars', handle_replay_bars)
    app.router.add_get('/api/replay/dates', handle_replay_dates)
    app.router.add_post('/api/replay/refresh', handle_replay_refresh)
    app.router.add_get('/api/replay/presets', handle_replay_presets)
    app.router.add_static('/static/', str(WEB_DIR), show_index=False)
    @web.middleware
    async def no_cache(request, handler):
        response = await handler(request)
        if request.path.startswith('/static/') or request.path in ('/', '/chart'):
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

    web.run_app(app, host='127.0.0.1', port=port, print=None)
