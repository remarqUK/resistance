"""Aiohttp live dashboard with IBKR quote subscriptions."""

from __future__ import annotations

import asyncio
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
import threading
import webbrowser
from typing import Optional

from aiohttp import web

from . import ibkr
from .live import (
    MonitorSnapshot,
    PairScanRow,
    format_sizing_summary,
    refresh_pair_row_price,
    run_monitor_cycle,
)
from .positions import calc_pnl_pips, pair_pip


WEB_DIR = Path(__file__).resolve().parent / 'web_live'
LOG_LIMIT = 80


def _signal_key(signal) -> str:
    """Build a stable signal key for diffs."""

    return f'{signal.pair}:{signal.direction}'


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
        self.execute_orders = execute_orders
        self.strategy_label = strategy_label
        self.client_id = client_id
        self.port = port

        self._snapshot: Optional[MonitorSnapshot] = None
        self._previous_snapshot: Optional[MonitorSnapshot] = None
        self._pair_rows: dict[str, PairScanRow] = {}
        self._tracked: dict[str, dict] = {}
        self._position_snapshots: dict[str, dict] = {}
        self._alerts: list[dict] = []
        self._execution_results = []
        self._last_quotes: dict[str, float] = {}
        self._log: deque[dict] = deque(maxlen=LOG_LIMIT)
        self.summary = self._build_summary(status='starting')

        self._clients: set[web.WebSocketResponse] = set()
        self._lock = asyncio.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._scan_task: Optional[asyncio.Task] = None
        self._scan_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix='ibkr-scan',
        )
        self._quote_stop = threading.Event()
        self._quote_thread: Optional[threading.Thread] = None
        self._pending_tasks: set[asyncio.Task] = set()

    def _ws_url(self) -> str:
        return f'ws://127.0.0.1:{self.port}/ws'

    def _build_summary(
        self,
        *,
        status: str,
        snapshot: Optional[MonitorSnapshot] = None,
        next_scan_at: Optional[datetime] = None,
    ) -> dict:
        """Build the summary payload consumed by the dashboard shell."""

        pairs_total = len(self.pairs)
        return {
            'status': status,
            'pairs_total': pairs_total,
            'pairs_completed': 0 if status == 'scanning' else len(self._pair_rows),
            'signal_count': 0 if snapshot is None else len(snapshot.signals),
            'pending_count': 0 if snapshot is None else len(snapshot.pending_pairs),
            'position_count': 0 if snapshot is None else len(snapshot.tracked),
            'execution_enabled': self.execute_orders,
            'sizing_summary': format_sizing_summary(snapshot),
            'strategy_label': self.strategy_label or 'Strategy',
            'mode': 'scanner + positions' if self.track_positions else 'scanner',
            'url': self._ws_url(),
            'scan_completed_at': (
                None if snapshot is None else snapshot.scan_completed_at.isoformat()
            ),
            'next_scan_at': None if next_scan_at is None else next_scan_at.isoformat(),
        }

    def _append_log(self, level: str, message: str) -> None:
        """Append a structured log entry."""

        self._log.append(
            {
                'ts': datetime.now().strftime('%H:%M:%S'),
                'level': level,
                'message': message,
            }
        )

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
            rows.append(
                {
                    'pair': pair,
                    'direction': trade.direction,
                    'size': int(abs(info.get('ibkr_size') or 0)),
                    'entry_price': trade.entry_price,
                    'current_price': snap.get('current_price'),
                    'pnl_pips': snap.get('pnl_pips'),
                    'status': alert_lookup.get(key, 'OK'),
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
                'note': result.note,
            }
            for result in self._execution_results
        ]

    def _export_state(self) -> dict:
        """Serialize the entire dashboard state."""

        signals = []
        if self._snapshot is not None:
            signals = [
                self._serialize_signal(signal, size_plan)
                for signal, size_plan in zip(self._snapshot.signals, self._snapshot.size_plans)
            ]

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

    def _record_snapshot_events(self, snapshot: MonitorSnapshot) -> None:
        """Append high-signal snapshot events to the local log buffer."""

        self._append_log(
            'info',
            (
                f"Scan complete: {len(snapshot.pair_rows)} pairs, {len(snapshot.signals)} signals, "
                f"{len(snapshot.tracked)} open positions, {len(snapshot.alerts)} exit alerts"
            ),
        )

        previous = self._previous_snapshot
        previous_signals = {
            _signal_key(signal): signal
            for signal in (previous.signals if previous is not None else [])
        }
        current_signals = {_signal_key(signal): signal for signal in snapshot.signals}

        for key, signal in current_signals.items():
            if key not in previous_signals:
                self._append_log('success', f'New signal: {signal.pair} {signal.direction}')
        for key, signal in previous_signals.items():
            if key not in current_signals:
                self._append_log('muted', f'Signal cleared: {signal.pair} {signal.direction}')

        previous_tracked = set() if previous is None else set(previous.tracked)
        current_tracked = set(snapshot.tracked)
        for key in sorted(current_tracked - previous_tracked):
            info = snapshot.tracked[key]
            self._append_log('info', f"Position tracking: {info['pair']} {info['trade'].direction}")
        for key in sorted(previous_tracked - current_tracked):
            pair, direction = key.split(':', 1)
            self._append_log('muted', f'Position closed externally: {pair} {direction}')

        for alert in snapshot.alerts:
            self._append_log(
                'warning',
                f"Exit alert: {alert['pair']} {alert['direction']} {alert['exit_reason']}",
            )
        for result in snapshot.execution_results:
            level = 'success' if result.status.upper().endswith('SUBMITTED') else 'warning'
            if result.status == 'FAILED':
                level = 'error'
            self._append_log(level, f'{result.status}: {result.pair} {result.direction}')
        for message in snapshot.messages[-6:]:
            self._append_log('info', f'IBKR: {message}')

    async def _handle_quote_update(self, pair: str, price: float) -> None:
        """Apply a subscribed quote change to the in-memory dashboard state."""

        async with self._lock:
            self._last_quotes[pair] = price
            row = self._pair_rows.get(pair)
            if row is None:
                return

            updated_row = refresh_pair_row_price(row, price)
            if updated_row == row:
                return

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

            summary = dict(self.summary)
            row_payload = self._serialize_pair_row(updated_row)
            state_payload = self._export_state() if positions_changed else None

        if positions_changed and state_payload is not None:
            await self._broadcast({'type': 'snapshot', 'state': state_payload})
            return

        await self._broadcast({'type': 'pair_update', 'row': row_payload, 'summary': summary})

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

    def _run_quote_stream(self) -> None:
        """Run the blocking IBKR quote subscription loop in a side thread."""

        base_client_id = self.client_id if self.client_id is not None else ibkr.TWS_CLIENT_ID
        quote_client_id = int(base_client_id) + 1000
        ibkr.stream_live_quotes(
            pairs=list(self.pairs.keys()),
            on_price=self._queue_quote_update,
            stop_event=self._quote_stop,
            client_id=quote_client_id,
        )

    async def _scan_loop(self) -> None:
        """Run repeated monitor scans and publish the resulting snapshots."""

        while True:
            async with self._lock:
                self.summary = self._build_summary(status='scanning', snapshot=self._snapshot)
                scan_summary = dict(self.summary)
            await self._broadcast({'type': 'scan_status', 'summary': scan_summary})

            try:
                snapshot = await self._loop.run_in_executor(
                    self._scan_executor,
                    partial(
                        run_monitor_cycle,
                        pairs=self.pairs,
                        params=self.params,
                        zone_history_days=self.zone_history_days,
                        track_positions=self.track_positions,
                        balance=self.balance,
                        risk_pct=self.risk_pct,
                        account_currency=self.account_currency,
                        execute_orders=self.execute_orders,
                        capture_output=True,
                    ),
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                async with self._lock:
                    self.summary = self._build_summary(status='error', snapshot=self._snapshot)
                    self._append_log('error', f'Scan failed: {exc}')
                    error_summary = dict(self.summary)
                await self._broadcast(
                    {
                        'type': 'error',
                        'summary': error_summary,
                        'message': str(exc),
                    }
                )
                await asyncio.sleep(self.interval)
                continue

            next_scan_at = datetime.now() + timedelta(seconds=self.interval)
            async with self._lock:
                self._previous_snapshot = self._snapshot
                self._snapshot = snapshot
                self._pair_rows = {row.pair: row for row in snapshot.pair_rows}
                self._tracked = snapshot.tracked
                self._position_snapshots = dict(snapshot.position_snapshots)
                self._alerts = list(snapshot.alerts)
                self._execution_results = list(snapshot.execution_results)
                self._apply_live_quotes()
                self._record_snapshot_events(snapshot)
                self.summary = self._build_summary(
                    status='live',
                    snapshot=snapshot,
                    next_scan_at=next_scan_at,
                )
                state = self._export_state()

            await self._broadcast({'type': 'snapshot', 'state': state})

            # Start the live-quote stream after the first successful scan so
            # the scan's IBKR connection is fully established before the quote
            # thread opens a second connection with a different client ID.
            if self._quote_thread is None:
                self._quote_thread = threading.Thread(
                    target=self._run_quote_stream,
                    name='ibkr-live-quotes',
                    daemon=True,
                )
                self._quote_thread.start()

            await asyncio.sleep(self.interval)

    async def start(self) -> None:
        """Start background scan and quote tasks."""

        self._loop = asyncio.get_running_loop()
        self._scan_task = asyncio.create_task(self._scan_loop())

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
                # Thread didn't exit — force-disconnect IBKR to unblock it
                ibkr.disconnect()

        # Cancel any in-flight quote update tasks
        for task in list(self._pending_tasks):
            task.cancel()
        self._pending_tasks.clear()

        self._scan_executor.shutdown(wait=False)

    async def register(self, ws: web.WebSocketResponse) -> None:
        """Register a browser client and send the current state."""

        async with self._lock:
            self._clients.add(ws)
            state = self._export_state()
        await ws.send_json({'type': 'bootstrap', 'state': state})

    async def unregister(self, ws: web.WebSocketResponse) -> None:
        """Remove a browser client."""

        self._clients.discard(ws)


async def _index(_request: web.Request) -> web.StreamResponse:
    """Serve the dashboard shell."""

    return web.FileResponse(WEB_DIR / 'index.html')


async def _websocket(request: web.Request) -> web.StreamResponse:
    """Handle websocket clients for the live dashboard."""

    hub: LiveDashboardHub = request.app["hub"]
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)
    await hub.register(ws)

    try:
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
    from .replay import handle_replay, handle_replay_dates, handle_replay_page, handle_replay_refresh, handle_replay_presets
    from .replay import (
        handle_backtest_trades_api,
        handle_backtest_trades_page,
        handle_backtest_diary_api,
        handle_backtest_diary_page,
    )

    app.router.add_get('/', _index)
    app.router.add_get('/ws', _websocket)
    app.router.add_get('/replay', handle_replay_page)
    app.router.add_get('/backtest-trades', handle_backtest_trades_page)
    app.router.add_get('/api/backtest/trades', handle_backtest_trades_api)
    app.router.add_get('/backtest-diary', handle_backtest_diary_page)
    app.router.add_get('/api/backtest/diary', handle_backtest_diary_api)
    app.router.add_get('/api/replay', handle_replay)
    app.router.add_get('/api/replay/dates', handle_replay_dates)
    app.router.add_post('/api/replay/refresh', handle_replay_refresh)
    app.router.add_get('/api/replay/presets', handle_replay_presets)
    app.router.add_static('/static/', str(WEB_DIR), show_index=False)
    app.on_startup.append(_startup)
    app.on_cleanup.append(_cleanup)

    url = f'http://127.0.0.1:{port}/'
    print(f'\n  Live dashboard server: {url}')
    print(f'  WebSocket endpoint: ws://127.0.0.1:{port}/ws')
    print('  Ctrl+C to stop.')

    if open_browser:
        threading.Timer(0.5, lambda: webbrowser.open(url)).start()

    web.run_app(app, host='127.0.0.1', port=port, print=None)
