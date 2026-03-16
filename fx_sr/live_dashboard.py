"""Rich terminal dashboard for the FX live monitor."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
import time
from typing import Optional

from rich import box
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from .config import PAIRS
from .sizing import format_units
from .live import (
    MonitorSnapshot,
    PairScanRow,
    format_sizing_summary,
    run_monitor_cycle,
    _format_number_compact,
    _pair_row_priority,
)


DASHBOARD_LOG_MAX = 40


@dataclass
class ActivityLog:
    """Rolling event log shown at the bottom of the dashboard."""

    maxlen: int = DASHBOARD_LOG_MAX

    def __post_init__(self) -> None:
        self._entries: deque[str] = deque(maxlen=self.maxlen)

    def add(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        self._entries.append(f"[dim]{timestamp}[/dim]  {message}")

    def lines(self, limit: int = 10) -> list[str]:
        return list(self._entries)[-limit:]


def _signal_key(signal) -> str:
    """Build a stable key for signal diffing."""

    return f"{signal.pair}:{signal.direction}"


def _countdown_seconds(next_scan_at: Optional[datetime]) -> Optional[int]:
    """Return whole seconds remaining until the next scan."""

    if next_scan_at is None:
        return None
    return max(0, int((next_scan_at - datetime.now()).total_seconds()))


def _state_text(row: PairScanRow) -> Text:
    """Return a styled state cell."""

    if row.signal and row.signal.direction == 'LONG':
        return Text(" LONG ", style="black on green")
    if row.signal and row.signal.direction == 'SHORT':
        return Text(" SHORT ", style="black on red")

    style = {
        'OPEN': "black on blue",
        'PARTIAL': "black on bright_yellow",
        'PENDING': "black on yellow",
        'INSIDE': "black on magenta",
        'WATCH': "cyan",
        'NO DATA': "dim",
    }.get(row.state, "white")
    return Text(f" {row.state} ", style=style)


def _note_text(row: PairScanRow) -> Text:
    """Return a styled note cell."""

    if row.signal and row.signal.direction == 'LONG':
        return Text(row.note, style="green")
    if row.signal and row.signal.direction == 'SHORT':
        return Text(row.note, style="red")
    if row.state == 'OPEN':
        return Text(row.note, style="blue")
    if row.state == 'PARTIAL':
        return Text(row.note, style="yellow")
    if row.state == 'PENDING':
        return Text(row.note, style="yellow")
    if row.state == 'NO DATA':
        return Text(row.note, style="dim")
    return Text(row.note)


def _build_header_panel(
    snapshot: Optional[MonitorSnapshot],
    strategy_label: Optional[str],
    client_id: Optional[int],
    interval: Optional[int],
    next_scan_at: Optional[datetime],
    is_scanning: bool,
    last_error: Optional[str],
) -> Panel:
    """Build the dashboard header."""

    if snapshot is None:
        cadence = "single scan" if interval in (None, 0) else f"every {interval}s"
        return Panel(
            "[bold]FX S/R Live Dashboard[/bold]\n"
            f"Preparing first scan | cadence {cadence}",
            border_style="cyan",
        )

    if next_scan_at is None:
        status_markup = "[bold cyan]SNAPSHOT[/bold cyan]"
        cadence = "single scan"
    elif is_scanning:
        status_markup = "[yellow bold]SCANNING[/yellow bold]"
        cadence = f"every {interval}s"
    else:
        seconds = _countdown_seconds(next_scan_at) or 0
        status_markup = f"[green bold]NEXT {seconds:02d}s[/green bold]"
        cadence = f"every {interval}s"

    mode_label = "scanner + positions" if snapshot.track_positions else "scanner only"
    execution_label = "paper orders on" if snapshot.execute_orders else "paper orders off"
    strategy_text = strategy_label or "custom parameters"
    client_text = f" | Client {client_id}" if client_id is not None else ""

    content = (
        f"[bold]FX S/R Live Dashboard[/bold]  |  {snapshot.scan_completed_at:%Y-%m-%d %H:%M:%S}  |  "
        f"{status_markup}\n"
        f"{mode_label}  |  {len(snapshot.pair_rows)} pairs  |  {len(snapshot.signals)} signals  |  "
        f"{len(snapshot.tracked)} open positions  |  {len(snapshot.pending_pairs)} pending orders  |  "
        f"scan {snapshot.scan_duration:.1f}s\n"
        f"Strategy: {strategy_text}{client_text}  |  Sizing: {format_sizing_summary(snapshot)}  |  "
        f"Execution: {execution_label}  |  Cadence: {cadence}"
    )
    if last_error:
        content += f"\n[red]Last error:[/red] {last_error}"
    return Panel(content, border_style="cyan")


def _build_watchlist_panel(snapshot: Optional[MonitorSnapshot]) -> Panel:
    """Build the watchlist panel."""

    if snapshot is None:
        return Panel("[dim]Waiting for first scan...[/dim]", title="Market Watch", border_style="blue")

    table = Table(box=box.SIMPLE_HEAVY, expand=True)
    table.add_column("Pair", style="bold cyan", width=8, no_wrap=True)
    table.add_column("Price", justify="right", width=10, no_wrap=True)
    table.add_column("State", width=10, no_wrap=True)
    table.add_column("Support", width=24, no_wrap=True)
    table.add_column("Resistance", width=24, no_wrap=True)
    table.add_column("Read", overflow="fold")

    for row in sorted(snapshot.pair_rows, key=_pair_row_priority):
        price_display = "-" if row.price is None else f"{row.price:.{row.decimals}f}"
        table.add_row(
            row.pair,
            price_display,
            _state_text(row),
            row.support_text,
            row.resistance_text,
            _note_text(row),
        )

    return Panel(table, title=f"Market Watch ({len(snapshot.pair_rows)})", border_style="blue")


def _build_signals_panel(snapshot: Optional[MonitorSnapshot]) -> Panel:
    """Build the active-signal panel."""

    if snapshot is None or not snapshot.signals:
        return Panel(
            "[dim]No tradeable reversal signals on the latest hourly bar.[/dim]",
            title="Trade Setups",
            border_style="green",
        )

    table = Table(box=box.SIMPLE_HEAVY, expand=True)
    table.add_column("Pair", style="bold cyan", width=7, no_wrap=True)
    table.add_column("Dir", width=7, no_wrap=True)
    table.add_column("Entry", justify="right", width=10, no_wrap=True)
    table.add_column("Stop", justify="right", width=10, no_wrap=True)
    table.add_column("Target", justify="right", width=10, no_wrap=True)
    table.add_column("Zone", width=19, no_wrap=True)
    table.add_column("Plan", overflow="fold")

    for signal, plan in zip(snapshot.signals, snapshot.size_plans):
        decimals = PAIRS.get(signal.pair, {}).get('decimals', 5)
        dir_text = Text(
            f" {signal.direction} ",
            style="black on green" if signal.direction == 'LONG' else "black on red",
        )
        zone_display = (
            f"{signal.zone_type[:3].upper()} "
            f"{signal.zone_lower:.{decimals}f}-{signal.zone_upper:.{decimals}f}"
        )
        if plan:
            plan_display = (
                f"risk {plan.account_currency} {_format_number_compact(plan.risk_amount)} | "
                f"{format_units(plan.units)} | "
                f"notional {plan.account_currency} {_format_number_compact(plan.notional_account)}"
            )
        else:
            plan_display = "size unavailable"

        table.add_row(
            signal.pair,
            dir_text,
            f"{signal.entry_price:.{decimals}f}",
            f"{signal.sl_price:.{decimals}f}",
            f"{signal.tp_price:.{decimals}f}",
            zone_display,
            plan_display,
        )

    return Panel(table, title=f"Trade Setups ({len(snapshot.signals)})", border_style="green")


def _build_positions_panel(snapshot: Optional[MonitorSnapshot]) -> Panel:
    """Build the tracked-position panel."""

    if snapshot is None or not snapshot.tracked:
        return Panel(
            "[dim]No open IBKR FX positions are currently being tracked.[/dim]",
            title="Tracked Positions",
            border_style="yellow",
        )

    alert_lookup = {
        f"{alert['pair']}:{alert['direction']}": alert['exit_reason']
        for alert in snapshot.alerts
    }

    table = Table(box=box.SIMPLE_HEAVY, expand=True)
    table.add_column("Pair", style="bold cyan", width=7, no_wrap=True)
    table.add_column("Dir", width=7, no_wrap=True)
    table.add_column("Size", justify="right", width=9, no_wrap=True)
    table.add_column("Entry", justify="right", width=10, no_wrap=True)
    table.add_column("Last", justify="right", width=10, no_wrap=True)
    table.add_column("P/L", justify="right", width=9, no_wrap=True)
    table.add_column("Status", overflow="fold")

    for key in sorted(snapshot.tracked.keys()):
        info = snapshot.tracked[key]
        trade = info['trade']
        decimals = PAIRS.get(info['pair'], {}).get('decimals', 5)
        position_size = int(abs(info.get('ibkr_size') or 0))
        snap = snapshot.position_snapshots.get(key, {})
        current_price = snap.get('current_price')
        pnl_pips = snap.get('pnl_pips')

        last_display = "-" if current_price is None else f"{current_price:.{decimals}f}"
        pnl_display = "-" if pnl_pips is None else f"{pnl_pips:+.1f}p"
        status = alert_lookup.get(key)
        status_text = Text(status, style="red bold") if status else Text("OK", style="green")

        table.add_row(
            info['pair'],
            Text(
                f" {trade.direction} ",
                style="black on green" if trade.direction == 'LONG' else "black on red",
            ),
            format_units(position_size) if position_size else "-",
            f"{trade.entry_price:.{decimals}f}",
            last_display,
            pnl_display,
            status_text,
        )

    return Panel(table, title=f"Tracked Positions ({len(snapshot.tracked)})", border_style="yellow")


def _build_actions_panel(snapshot: Optional[MonitorSnapshot]) -> Panel:
    """Build the scan-action panel for alerts and order activity."""

    if snapshot is None:
        return Panel("[dim]Waiting for first scan...[/dim]", title="Alerts & Actions", border_style="magenta")

    lines: list[str] = []
    for alert in snapshot.alerts[:6]:
        decimals = PAIRS.get(alert['pair'], {}).get('decimals', 5)
        lines.append(
            f"[red bold]EXIT[/red bold] {alert['pair']} {alert['direction']} "
            f"{alert['exit_reason']} @ {alert['current_price']:.{decimals}f} "
            f"({alert['pnl_pips']:+.1f}p)"
        )

    for result in snapshot.execution_results[:6]:
        if result.status in {'Submitted', 'PreSubmitted', 'SUBMITTED', 'PARTIAL', 'OPEN'}:
            prefix = "[green bold]ORDER[/green bold]"
        elif result.status == 'SKIPPED':
            prefix = "[yellow]SKIP[/yellow]"
        else:
            prefix = "[red]FAIL[/red]"

        order_id = f" id={result.order_id}" if result.order_id is not None else ""
        size_display = format_units(result.units) if result.units else "-"
        note = f" | {result.note}" if result.note else ""
        lines.append(
            f"{prefix} {result.pair} {result.direction} {size_display} "
            f"{result.status}{order_id}{note}"
        )

    if not lines:
        lines.append("[dim]No alerts or order actions on this scan.[/dim]")
    return Panel("\n".join(lines), title="Alerts & Actions", border_style="magenta")


def _build_log_panel(activity_log: ActivityLog) -> Panel:
    """Build the rolling activity log panel."""

    lines = activity_log.lines(10)
    content = "[dim]No activity yet.[/dim]" if not lines else "\n".join(lines)
    return Panel(content, title="Activity Log", border_style="cyan")


def _build_dashboard(
    snapshot: Optional[MonitorSnapshot],
    activity_log: ActivityLog,
    strategy_label: Optional[str],
    client_id: Optional[int],
    interval: Optional[int],
    next_scan_at: Optional[datetime],
    is_scanning: bool,
    last_error: Optional[str],
) -> Layout:
    """Build the full dashboard layout."""

    layout = Layout()
    layout.split_column(
        Layout(
            _build_header_panel(
                snapshot,
                strategy_label,
                client_id,
                interval,
                next_scan_at,
                is_scanning,
                last_error,
            ),
            name="header",
            size=5,
        ),
        Layout(name="body", ratio=1),
        Layout(name="footer", size=11),
    )

    layout["body"].split_row(
        Layout(_build_watchlist_panel(snapshot), name="watchlist", ratio=2),
        Layout(name="sidebar", ratio=1),
    )
    layout["sidebar"].split_column(
        Layout(_build_signals_panel(snapshot), name="signals", ratio=1),
        Layout(_build_positions_panel(snapshot), name="positions", ratio=1),
    )
    layout["footer"].split_row(
        Layout(_build_actions_panel(snapshot), name="actions", ratio=1),
        Layout(_build_log_panel(activity_log), name="log", ratio=1),
    )
    return layout


def _append_cycle_events(
    activity_log: ActivityLog,
    previous_snapshot: Optional[MonitorSnapshot],
    snapshot: MonitorSnapshot,
) -> None:
    """Append the latest high-signal events to the activity log."""

    activity_log.add(
        f"[bold]Scan complete[/bold] {len(snapshot.pair_rows)} pairs | "
        f"{len(snapshot.signals)} signals | {len(snapshot.tracked)} open | "
        f"{len(snapshot.alerts)} exit alerts"
    )

    prev_signals = {
        _signal_key(signal): signal
        for signal in (previous_snapshot.signals if previous_snapshot else [])
    }
    curr_signals = {_signal_key(signal): signal for signal in snapshot.signals}

    for key, signal in curr_signals.items():
        if key not in prev_signals:
            direction_style = "green bold" if signal.direction == 'LONG' else "red bold"
            decimals = PAIRS.get(signal.pair, {}).get('decimals', 5)
            activity_log.add(
                f"[{direction_style}]SIGNAL[/{direction_style}] {signal.pair} {signal.direction} "
                f"@ {signal.entry_price:.{decimals}f}"
            )

    for key, signal in prev_signals.items():
        if key not in curr_signals:
            activity_log.add(f"[dim]Signal cleared[/dim] {signal.pair} {signal.direction}")

    prev_tracked = set(previous_snapshot.tracked.keys()) if previous_snapshot else set()
    curr_tracked = set(snapshot.tracked.keys())
    for key in sorted(curr_tracked - prev_tracked):
        info = snapshot.tracked[key]
        activity_log.add(f"[blue]Position tracking[/blue] {info['pair']} {info['trade'].direction}")
    for key in sorted(prev_tracked - curr_tracked):
        pair, direction = key.split(':', 1)
        activity_log.add(f"[dim]Position closed externally[/dim] {pair} {direction}")

    for alert in snapshot.alerts:
        activity_log.add(
            f"[red bold]EXIT ALERT[/red bold] {alert['pair']} {alert['direction']} "
            f"{alert['exit_reason']} ({alert['pnl_pips']:+.1f}p)"
        )

    for result in snapshot.execution_results:
        if result.status in {'Submitted', 'PreSubmitted', 'SUBMITTED', 'PARTIAL', 'OPEN'}:
            style = "green bold"
        elif result.status == 'SKIPPED':
            style = "yellow"
        else:
            style = "red"
        size_display = format_units(result.units) if result.units else "-"
        activity_log.add(f"[{style}]{result.status}[/{style}] {result.pair} {result.direction} {size_display}")

    for message in snapshot.messages[-6:]:
        activity_log.add(f"[yellow]IBKR[/yellow] {message}")


def display_snapshot_rich(
    snapshot: MonitorSnapshot,
    strategy_label: Optional[str],
    client_id: Optional[int],
) -> None:
    """Render a one-shot snapshot with Rich panels."""

    console = Console()
    activity_log = ActivityLog()
    for alert in snapshot.alerts:
        activity_log.add(
            f"[red bold]EXIT ALERT[/red bold] {alert['pair']} {alert['direction']} {alert['exit_reason']}"
        )
    for result in snapshot.execution_results:
        activity_log.add(f"[green]ORDER[/green] {result.pair} {result.status}")
    for message in snapshot.messages[-8:]:
        activity_log.add(f"[yellow]IBKR[/yellow] {message}")

    console.print(
        _build_header_panel(
            snapshot=snapshot,
            strategy_label=strategy_label,
            client_id=client_id,
            interval=None,
            next_scan_at=None,
            is_scanning=False,
            last_error=None,
        )
    )
    console.print(_build_watchlist_panel(snapshot))
    console.print(_build_signals_panel(snapshot))
    console.print(_build_positions_panel(snapshot))
    console.print(_build_actions_panel(snapshot))
    if snapshot.messages or snapshot.alerts or snapshot.execution_results:
        console.print(_build_log_panel(activity_log))


def run_live_dashboard(
    pairs,
    params,
    interval: int,
    zone_history_days: int,
    track_positions: bool,
    balance,
    risk_pct: float,
    account_currency,
    execute_orders: bool,
    strategy_label: Optional[str],
    client_id: Optional[int],
) -> None:
    """Run the in-place Rich dashboard loop without alternate-screen flicker."""

    console = Console()
    activity_log = ActivityLog()
    activity_log.add(
        "[bold]Monitor started[/bold] "
        + ("scanner + positions" if track_positions else "scanner only")
    )

    snapshot: Optional[MonitorSnapshot] = None
    next_scan_at = datetime.now()
    last_error: Optional[str] = None
    last_countdown: Optional[int] = None

    try:
        with Live(
            _build_dashboard(
                snapshot=snapshot,
                activity_log=activity_log,
                strategy_label=strategy_label,
                client_id=client_id,
                interval=interval,
                next_scan_at=next_scan_at,
                is_scanning=False,
                last_error=None,
            ),
            console=console,
            screen=False,
            auto_refresh=False,
            vertical_overflow="visible",
        ) as live:
            while True:
                now = datetime.now()
                if now >= next_scan_at:
                    live.update(
                        _build_dashboard(
                            snapshot=snapshot,
                            activity_log=activity_log,
                            strategy_label=strategy_label,
                            client_id=client_id,
                            interval=interval,
                            next_scan_at=next_scan_at,
                            is_scanning=True,
                            last_error=last_error,
                        ),
                        refresh=True,
                    )
                    try:
                        new_snapshot = run_monitor_cycle(
                            pairs=pairs,
                            params=params,
                            zone_history_days=zone_history_days,
                            track_positions=track_positions,
                            balance=balance,
                            risk_pct=risk_pct,
                            account_currency=account_currency,
                            execute_orders=execute_orders,
                            capture_output=True,
                        )
                        _append_cycle_events(activity_log, snapshot, new_snapshot)
                        snapshot = new_snapshot
                        last_error = None
                    except Exception as exc:  # pragma: no cover - live-only path
                        last_error = str(exc)
                        activity_log.add(f"[red bold]Scan failed[/red bold] {exc}")

                    next_scan_at = datetime.now() + timedelta(seconds=interval)
                    last_countdown = None
                    live.update(
                        _build_dashboard(
                            snapshot=snapshot,
                            activity_log=activity_log,
                            strategy_label=strategy_label,
                            client_id=client_id,
                            interval=interval,
                            next_scan_at=next_scan_at,
                            is_scanning=False,
                            last_error=last_error,
                        ),
                        refresh=True,
                    )
                    continue

                countdown = _countdown_seconds(next_scan_at)
                if countdown != last_countdown:
                    last_countdown = countdown
                    live.update(
                        _build_dashboard(
                            snapshot=snapshot,
                            activity_log=activity_log,
                            strategy_label=strategy_label,
                            client_id=client_id,
                            interval=interval,
                            next_scan_at=next_scan_at,
                            is_scanning=False,
                            last_error=last_error,
                        ),
                        refresh=True,
                    )
                time.sleep(0.20)
    except KeyboardInterrupt:
        console.print("\n[yellow]Monitor stopped.[/yellow]")
