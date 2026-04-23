"""Persistence helpers for live signal and trade lifecycle history."""

from __future__ import annotations

import asyncio
from concurrent.futures import Future
from dataclasses import dataclass
import hashlib
import logging
import queue
import threading
from datetime import date, datetime as dt_datetime
from typing import Callable, Iterable, Optional

import pandas as pd

from .data import fx_market_is_open
from .db import _connect, _normalize_ts, db_transaction, get_db_path
from .signal_store import (
    NON_TRANSACTIONAL_RESULT_STATUSES as _NON_TRANSACTIONAL_RESULT_STATUSES,
    derive_signal_execution_status as _derive_signal_execution_status,
    detected_signal_fill_summary_conn as _signal_fill_summary_conn,
    ensure_signal_tables as _ensure_table,
    existing_has_execution_evidence as _existing_has_execution_evidence,
    load_detected_signal_conn as _load_detected_signal_conn,
    merge_row as _merge_row,
    normalize_status as _normalize_status,
    normalize_units as _normalize_units,
    pair_pip as _pair_pip,
    record_detected_signal_fill_conn as _record_signal_fill_conn,
    replace_detected_signal_conn as _replace_row_conn,
    result_has_execution_evidence as _result_has_execution_evidence,
    row_to_dict as _row_to_dict,
    signal_order_ref as _signal_order_ref,
    status_from_existing_execution as _status_from_existing_execution,
)


# ---------------------------------------------------------------------------
# Background writer — offloads DB I/O from the caller's thread
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class _QueuedWrite:
    """One queued DB write operation."""

    fn: Callable[[], None]
    future: Future | None = None


_STOP_WRITER = object()
_write_queue: queue.Queue | None = None
_write_thread: threading.Thread | None = None
_write_stop = threading.Event()
_LOGGER = logging.getLogger(__name__)


def _execute_write(item: _QueuedWrite) -> None:
    """Execute one queued write and resolve its acknowledgement future."""

    try:
        item.fn()
    except Exception as exc:
        _LOGGER.exception("Background detected-signal write failed")
        if item.future is not None and not item.future.done():
            item.future.set_exception(exc)
        return
    if item.future is not None and not item.future.done():
        item.future.set_result(None)


def _writer_loop() -> None:
    """Drain the write queue and execute each callable in a dedicated thread."""

    while True:
        item = _write_queue.get()
        if item is _STOP_WRITER:
            break
        _execute_write(item)


def start_background_writer() -> None:
    """Start the singleton background DB writer thread (idempotent)."""

    global _write_queue, _write_thread
    if _write_thread is not None and _write_thread.is_alive():
        return
    _write_stop.clear()
    _write_queue = queue.Queue()
    _write_thread = threading.Thread(
        target=_writer_loop,
        name='signal-db-writer',
        daemon=False,
    )
    _write_thread.start()


def stop_background_writer() -> None:
    """Stop the background writer and drain remaining items."""

    global _write_queue, _write_thread
    queue_ref = _write_queue
    thread = _write_thread
    _write_stop.set()
    if queue_ref is None or thread is None:
        _write_queue = None
        _write_thread = None
        return

    if thread.is_alive():
        queue_ref.put(_STOP_WRITER)
        thread.join(timeout=5)
        if thread.is_alive():
            raise RuntimeError("Detected-signal writer did not stop within 5 seconds")
    else:
        while not queue_ref.empty():
            item = queue_ref.get_nowait()
            if item is _STOP_WRITER:
                continue
            _execute_write(item)

    _write_queue = None
    _write_thread = None


def enqueue_write(fn: Callable[[], None]) -> None:
    """Submit a write operation to the background thread.

    If the background writer is not running, the call executes inline
    so that ``--once`` mode and tests work without starting the writer.
    """

    if (
        not _write_stop.is_set()
        and _write_queue is not None
        and _write_thread is not None
        and _write_thread.is_alive()
    ):
        _write_queue.put(_QueuedWrite(fn))
    else:
        fn()


async def enqueue_write_async(
    fn: Callable[[], None],
    *,
    timeout: float = 30.0,
) -> None:
    """Await one queued DB write without blocking the event loop thread."""

    if (
        not _write_stop.is_set()
        and _write_queue is not None
        and _write_thread is not None
        and _write_thread.is_alive()
    ):
        future = Future()
        _write_queue.put(_QueuedWrite(fn, future=future))
        await asyncio.wait_for(asyncio.wrap_future(future), timeout=timeout)
        return
    await asyncio.to_thread(fn)


def ensure_detected_signal_table(db_path: str | None = None) -> str:
    """Public wrapper for detected-signal table initialization."""

    return _ensure_table(db_path)


def record_system_event(event_type: str, detail: str | None = None) -> None:
    """Record a system lifecycle event (startup, shutdown, etc.)."""
    try:
        db_path = _ensure_table()
        conn = _connect(db_path)
        try:
            conn.execute(
                "INSERT INTO system_event (event_type, detail) VALUES (%s, %s)",
                (event_type, detail),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass


def build_signal_id(signal) -> str:
    """Build a deterministic ID for one detected signal."""

    raw = "|".join(
        [
            signal.pair,
            signal.direction,
            _normalize_ts(signal.time),
            f"{float(signal.entry_price):.10f}",
            f"{float(signal.sl_price):.10f}",
            f"{float(signal.tp_price):.10f}",
            f"{float(signal.zone_lower):.10f}",
            f"{float(signal.zone_upper):.10f}",
            signal.zone_type,
        ]
    )
    digest = hashlib.blake2b(raw.encode("ascii"), digest_size=10).hexdigest()
    return f"{signal.pair}:{signal.direction}:{digest}"


def load_detected_signal(signal_id: str, db_path: str | None = None) -> dict | None:
    """Load one detected-signal row by ID."""

    db_path = _ensure_table(db_path)
    conn = _connect(db_path)
    try:
        return _load_detected_signal_conn(conn, signal_id)
    finally:
        conn.close()


def load_detected_signals(
    *,
    pair: str | None = None,
    status: str | None = None,
    limit: int | None = None,
    updated_after: str | None = None,
    db_path: str | None = None,
) -> list[dict]:
    """Load detected-signal history rows with optional filters."""

    db_path = _ensure_table(db_path)
    conn = _connect(db_path)
    try:
        query = "SELECT * FROM detected_signal"
        params: list[object] = []
        filters: list[str] = []

        if pair:
            filters.append("pair=%s")
            params.append(pair)
        if status:
            filters.append("status=%s")
            params.append(status.upper())
        if updated_after:
            filters.append("last_updated_at>%s")
            params.append(_normalize_ts(pd.Timestamp(updated_after)))
        if filters:
            query += " WHERE " + " AND ".join(filters)

        query += " ORDER BY signal_time DESC, detected_at DESC"
        if limit is not None:
            query += " LIMIT %s"
            params.append(int(limit))

        cursor = conn.execute(query, params)
        rows = cursor.fetchall()
        result = []
        for row in rows:
            d = _row_to_dict(cursor, row)
            _enrich_pnl(d, conn)
            result.append(d)
        return result
    finally:
        conn.close()


def load_detected_signal_stats(
    *,
    status: str | None = None,
    db_path: str | None = None,
) -> dict[str, object]:
    """Return lightweight metadata for cached live-history state."""

    db_path = _ensure_table(db_path)
    conn = _connect(db_path)
    try:
        query = "SELECT COUNT(*), MAX(last_updated_at) FROM detected_signal"
        params: list[object] = []
        if status:
            query += " WHERE status=%s"
            params.append(status.upper())
        count, max_updated_at = conn.execute(query, params).fetchone()
        return {
            'count': int(count or 0),
            'max_last_updated': max_updated_at,
        }
    finally:
        conn.close()


def load_execution_activity(
    *,
    limit: int | None = None,
    db_path: str | None = None,
) -> list[dict]:
    """Load recent execution activity rows for dashboard hydration."""

    db_path = _ensure_table(db_path)
    conn = _connect(db_path)
    try:
        query = """
            SELECT *
            FROM detected_signal
            WHERE executed_at IS NOT NULL
            ORDER BY COALESCE(executed_at, last_updated_at, detected_at) DESC
        """
        params: list[object] = []
        if limit is not None:
            query += " LIMIT %s"
            params.append(int(limit))

        cursor = conn.execute(query, params)
        rows = cursor.fetchall()
        result = []
        for row in rows:
            d = _row_to_dict(cursor, row)
            d['pnl_r'] = _compute_pnl_r(d)
            result.append(d)
        return result
    finally:
        conn.close()


def _compute_pnl_r(row: dict) -> float | None:
    """Derive P&L in R from entry, close, and SL prices."""

    opened = row.get('opened_price')
    closed = row.get('closed_price')
    sl = row.get('submitted_sl_price')
    direction = row.get('direction')

    if opened is None or closed is None or sl is None or direction is None:
        return None

    try:
        opened, closed, sl = float(opened), float(closed), float(sl)
    except (TypeError, ValueError):
        return None

    risk_dist = abs(opened - sl)
    if risk_dist < 1e-10:
        return None

    if direction.upper() == 'LONG':
        pnl_r = (closed - opened) / risk_dist
    else:
        pnl_r = (opened - closed) / risk_dist

    return round(pnl_r, 2)


def _compute_pnl_gbp(
    row: dict,
    conn,
    *,
    current_price: float | None = None,
    as_of: dt_datetime | date | None = None,
    to_currency: str = 'GBP',
) -> float | None:
    """Compute realised P&L in GBP from units, prices, and historical FX rate."""

    from .db import INTERVAL_TO_CODE, TICKER_TO_CODE
    from .sizing import split_pair, convert_currency

    opened = row.get('opened_price')
    closed = current_price if current_price is not None else row.get('closed_price')
    units = row.get('open_units')
    direction = row.get('direction')
    pair = row.get('pair')
    closed_at = row.get('closed_at')

    if any(v is None for v in (opened, closed, units, direction, pair)):
        return None

    try:
        opened, closed = float(opened), float(closed)
        units = int(units)
    except (TypeError, ValueError):
        return None

    if units <= 0:
        return None

    _, quote = split_pair(pair)
    currency = to_currency.upper() if to_currency else 'GBP'

    if direction.upper() == 'LONG':
        pnl_quote = units * (closed - opened)
    else:
        pnl_quote = units * (opened - closed)

    if quote.upper() == currency:
        return round(pnl_quote, 2)

    # Build a price_lookup that uses the latest 1h close no later than reference time.
    ref_ts = as_of or closed_at or row.get('opened_at') or row.get('signal_time')
    interval_code = INTERVAL_TO_CODE.get('1h')

    def _historical_price_lookup(lookup_pair: str) -> float | None:
        ticker_key = f"{lookup_pair[:3]}{lookup_pair[3:]}=X"
        ticker_code = TICKER_TO_CODE.get(ticker_key)
        if ticker_code is None:
            return None
        if ref_ts is not None:
            r = conn.execute(
                "SELECT close FROM ohlc WHERE ticker = %s AND interval = %s AND ts <= %s ORDER BY ts DESC LIMIT 1",
                (ticker_code, interval_code, ref_ts),
            ).fetchone()
        else:
            r = conn.execute(
                "SELECT close FROM ohlc WHERE ticker = %s AND interval = %s ORDER BY ts DESC LIMIT 1",
                (ticker_code, interval_code),
            ).fetchone()
        return float(r[0]) if r else None

    converted_amount = convert_currency(pnl_quote, quote, currency, _historical_price_lookup)
    return round(converted_amount, 2) if converted_amount is not None else None


def _latest_close_for_pair(
    conn,
    pair: str,
    as_of: dt_datetime | date | None = None,
) -> float | None:
    """Return the latest cached hourly close for *pair* at or before *as_of*."""

    from .db import INTERVAL_TO_CODE, TICKER_TO_CODE

    ticker_code = TICKER_TO_CODE.get(f"{pair[:3]}{pair[3:]}=X")
    if ticker_code is None:
        return None

    interval_code = INTERVAL_TO_CODE.get('1h')
    if interval_code is None:
        return None

    if as_of is None:
        row = conn.execute(
            "SELECT close FROM ohlc WHERE ticker = %s AND interval = %s ORDER BY ts DESC LIMIT 1",
            (ticker_code, interval_code),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT close FROM ohlc WHERE ticker = %s AND interval = %s AND ts <= %s ORDER BY ts DESC LIMIT 1",
            (ticker_code, interval_code, as_of),
        ).fetchone()
    return float(row[0]) if row else None


def _compute_unrealized_open_pnl(
    conn,
    *,
    as_of: dt_datetime | date | None = None,
    to_currency: str = 'GBP',
) -> float:
    """Sum unrealized P&L for currently open trades."""

    cursor = conn.execute(
        "SELECT * FROM detected_signal WHERE open_units IS NOT NULL AND open_units > 0 AND closed_at IS NULL",
    )
    total = 0.0
    for row in cursor.fetchall():
        d = _row_to_dict(cursor, row)
        pair = d.get('pair')
        if not isinstance(pair, str) or len(pair) != 6:
            continue
        market_price = _latest_close_for_pair(conn, pair, as_of=as_of)
        if market_price is None:
            continue
        pnl_amount = _compute_pnl_gbp(
            d,
            conn,
            current_price=market_price,
            as_of=as_of,
            to_currency=to_currency,
        )
        if pnl_amount is None:
            continue
        total += pnl_amount
    return round(total, 2)


def _enrich_pnl(d: dict, conn) -> None:
    """Add pnl_r and pnl_gbp fields to a detected-signal dict in-place."""

    pnl_r = _compute_pnl_r(d)
    d['pnl_r'] = pnl_r
    d['pnl_gbp'] = _compute_pnl_gbp(d, conn)


def _attach_equity(
    snapshot: dict,
    conn,
    *,
    as_of: dt_datetime | date,
    to_currency: str,
) -> None:
    """Add equity derived from open trades to *snapshot*."""

    open_unrealized = _compute_unrealized_open_pnl(
        conn,
        as_of=as_of,
        to_currency=to_currency,
    )
    snapshot['equity'] = snapshot['balance'] + open_unrealized


def load_live_diary_trades(
    *,
    db_path: str | None = None,
) -> list[dict]:
    """Load all filled live trades for the live diary calendar."""

    db_path = _ensure_table(db_path)
    conn = _connect(db_path)
    try:
        cursor = conn.execute("""
            SELECT *
            FROM detected_signal
            WHERE open_units > 0
               OR status IN ('OPEN', 'CLOSED', 'PARTIAL')
            ORDER BY signal_time
        """)
        rows = cursor.fetchall()
        result = []
        for row in rows:
            d = _row_to_dict(cursor, row)
            _enrich_pnl(d, conn)
            result.append(d)
        return result
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Account daily snapshot — balance + P&L history
# ---------------------------------------------------------------------------


def _compute_daily_pnl_gbp(conn, target_date: date) -> float:
    """Sum GBP P&L for all trades closed on *target_date*."""

    cursor = conn.execute(
        "SELECT * FROM detected_signal WHERE closed_at >= %s AND closed_at < %s + interval '1 day' AND status = 'CLOSED'",
        (target_date, target_date),
    )
    rows = cursor.fetchall()
    total = 0.0
    for row in rows:
        d = _row_to_dict(cursor, row)
        gbp = _compute_pnl_gbp(d, conn)
        if gbp is not None:
            total += gbp
    try:
        from .broker_ledger import load_unmatched_broker_liquidation_trades_conn
        broker_rows = load_unmatched_broker_liquidation_trades_conn(
            conn,
            closed_after=target_date,
            closed_before=target_date + pd.Timedelta(days=1),
        )
        for row in broker_rows:
            gbp = row.get('pnl_gbp')
            if gbp is not None:
                total += float(gbp)
    except Exception:
        pass
    return round(total, 2)


def compute_pnl_gbp(
    row: dict,
    conn,
    *,
    current_price: float | None = None,
    as_of: dt_datetime | date | None = None,
    to_currency: str = 'GBP',
) -> float | None:
    """Public wrapper for realised/unrealised GBP P&L calculation."""

    return _compute_pnl_gbp(
        row,
        conn,
        current_price=current_price,
        as_of=as_of,
        to_currency=to_currency,
    )


def compute_daily_pnl_gbp(conn, target_date: date) -> float:
    """Public wrapper for summing closed-trade GBP P&L for one date."""

    return _compute_daily_pnl_gbp(conn, target_date)


def record_daily_snapshot(
    balance: float,
    currency: str = 'GBP',
    daily_pnl_gbp: float | None = None,
    snapshot_date: date | None = None,
    *,
    db_path: str | None = None,
) -> None:
    """Upsert a daily account snapshot."""

    from .db import db_transaction

    if snapshot_date is None:
        snapshot_date = date.today()

    with db_transaction(db_path) as conn:
        if daily_pnl_gbp is None:
            daily_pnl_gbp = _compute_daily_pnl_gbp(conn, snapshot_date)
        conn.execute(
            """INSERT INTO account_daily_snapshot
                   (snapshot_date, net_liquidation, daily_pnl_gbp, currency)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (snapshot_date) DO UPDATE SET
                   net_liquidation = EXCLUDED.net_liquidation,
                   daily_pnl_gbp = EXCLUDED.daily_pnl_gbp,
                   recorded_at = NOW()
            """,
            (snapshot_date, balance, daily_pnl_gbp, currency),
        )


def load_daily_snapshots(*, db_path: str | None = None) -> list[dict]:
    """Return all daily snapshots ordered by date."""

    db_path = _ensure_table(db_path)
    conn = _connect(db_path)
    try:
        cursor = conn.execute(
            "SELECT snapshot_date, net_liquidation, daily_pnl_gbp, currency "
            "FROM account_daily_snapshot ORDER BY snapshot_date"
        )
        return [
            {
                'date': str(row[0]),
                'balance': float(row[1]),
                'daily_pnl_gbp': float(row[2]) if row[2] is not None else None,
                'currency': row[3],
            }
            for row in cursor.fetchall()
        ]
    finally:
        conn.close()


def get_or_fetch_today_snapshot(
    *,
    db_path: str | None = None,
    force_refresh: bool = False,
) -> dict | None:
    """Return today's snapshot, optionally refreshing from IBKR before returning."""

    import datetime as _dt
    from . import ibkr

    today = date.today()
    db_path = _ensure_table(db_path)
    now_utc = _dt.datetime.now(_dt.timezone.utc)

    # Existing snapshots are useful as a fallback and as a base currency anchor.
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT net_liquidation, daily_pnl_gbp, currency "
            "FROM account_daily_snapshot WHERE snapshot_date = %s",
            (today,),
        ).fetchone()
    finally:
        conn.close()

    if row is not None and not force_refresh:
        snapshot = {
            'date': str(today),
            'balance': float(row[0]),
            'daily_pnl_gbp': float(row[1]) if row[1] is not None else None,
            'currency': row[2],
        }
        conn = _connect(db_path)
        try:
            _attach_equity(
                snapshot,
                conn,
                as_of=now_utc,
                to_currency=snapshot.get('currency') or 'GBP',
            )
        finally:
            conn.close()
        return snapshot

    cached_row = None if row is None else {
        'date': str(today),
        'balance': float(row[0]),
        'daily_pnl_gbp': float(row[1]) if row[1] is not None else None,
        'currency': row[2],
    }

    def _attach_today_equity(snapshot: dict | None) -> dict | None:
        if snapshot is None:
            return None
        conn = _connect(db_path)
        try:
            _attach_equity(
                snapshot,
                conn,
                as_of=now_utc,
                to_currency=snapshot.get('currency') or 'GBP',
            )
        finally:
            conn.close()
        return snapshot

    cached_row = _attach_today_equity(cached_row)

    # Only fetch while the FX market should be open.
    now_utc = _dt.datetime.now(_dt.timezone.utc)
    if not fx_market_is_open(now_utc):
        return cached_row

    try:
        balance, currency = ibkr.fetch_account_net_liquidation()
    except Exception:
        return cached_row

    if balance is None:
        return cached_row

    currency = currency or 'GBP'

    # Compute today's realised P&L from closed trades
    conn2 = _connect(db_path)
    try:
        daily_pnl = _compute_daily_pnl_gbp(conn2, today)
    finally:
        conn2.close()

    record_daily_snapshot(balance, currency, daily_pnl, today, db_path=db_path)

    snapshot = {
        'date': str(today),
        'balance': float(balance),
        'daily_pnl_gbp': daily_pnl,
        'currency': currency,
    }
    conn = _connect(db_path)
    try:
        _attach_equity(
            snapshot,
            conn,
            as_of=now_utc,
            to_currency=currency,
        )
    finally:
        conn.close()
    return snapshot


def load_detected_signal_fills(
    signal_id: str,
    *,
    db_path: str | None = None,
) -> list[dict]:
    """Load all persisted broker fill rows for one detected signal."""

    db_path = _ensure_table(db_path)
    conn = _connect(db_path)
    try:
        cursor = conn.execute(
            """
            SELECT *
            FROM detected_signal_fill
            WHERE signal_id=%s
            ORDER BY fill_time ASC, recorded_at ASC, exec_id ASC
            """,
            (signal_id,),
        )
        return [_row_to_dict(cursor, row) for row in cursor.fetchall()]
    finally:
        conn.close()


def reconcile_detected_signal_orders(
    *,
    signal_ids: Iterable[str] | None = None,
    db_path: str | None = None,
    live_position_keys: set[str] | None = None,
) -> list[dict]:
    """Reconcile detected signals from the broker ledger."""

    from .broker_ledger import reconcile_detected_signal_orders as _reconcile

    return _reconcile(
        signal_ids=signal_ids,
        db_path=db_path,
        live_position_keys=live_position_keys,
    )


def reconcile_broker_ledger(
    *,
    signal_ids: Iterable[str] | None = None,
    db_path: str | None = None,
    live_position_keys: set[str] | None = None,
) -> dict:
    """Fetch IBKR state and upsert the local broker ledger."""

    from .broker_ledger import reconcile_broker_ledger as _reconcile

    return _reconcile(
        signal_ids=signal_ids,
        db_path=db_path,
        live_position_keys=live_position_keys,
    )


def has_active_broker_activity_for_order_ref(
    order_ref: str,
    *,
    db_path: str | None = None,
    check_broker: bool = True,
) -> bool:
    """Return True if this strategy orderRef already exists locally or at IBKR."""

    from .broker_ledger import has_active_broker_activity_for_order_ref as _has_activity

    return _has_activity(order_ref, db_path=db_path, check_broker=check_broker)


def load_open_broker_execution_positions(
    *,
    db_path: str | None = None,
) -> list[dict]:
    """Return net open exposure derived from linked broker executions."""

    from .broker_ledger import load_open_broker_execution_positions as _load_positions

    return _load_positions(db_path=db_path)


def record_pair_scan_log(
    pair_rows: Iterable,
    scan_time: pd.Timestamp | None = None,
    db_path: str | None = None,
) -> None:
    """Persist every pair's scan state so early-filtered pairs are visible.

    Records one row per pair per scan cycle — including pairs that never
    reached the execution engine (tracked, pending, entry-blocked, no data,
    or simply watching with no signal).
    """
    db_path = _ensure_table(db_path)
    rows = list(pair_rows)
    if not rows:
        return

    ts = _normalize_ts(scan_time if scan_time is not None else pd.Timestamp.now("UTC"))
    conn = _connect(db_path)
    try:
        for row in rows:
            signal = getattr(row, "signal", None)
            conn.execute(
                """
                INSERT INTO pair_scan_log
                    (scan_time, pair, state, note, price, signal_generated, direction)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    ts,
                    row.pair,
                    row.state,
                    row.note,
                    row.price,
                    signal is not None,
                    signal.direction if signal is not None else None,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def record_detected_signals(
    signals: Iterable,
    size_plans: Optional[Iterable] = None,
    *,
    execute_orders: bool,
    execution_mode: str | None = None,
    ibkr_account: str | None = None,
    detection_source: str | None = None,
    db_path: str | None = None,
) -> list[str]:
    """Upsert the currently detected signals into the history table."""

    db_path = _ensure_table(db_path)
    signals = list(signals)
    plans = list(size_plans) if size_plans is not None else [None] * len(signals)
    if len(plans) != len(signals):
        plans = [None] * len(signals)

    resolved_mode = execution_mode or _resolve_execution_mode(execute_orders)
    source_marker = str(detection_source or '').strip() or None
    source_note = (
        f'detected via {source_marker.replace("_", " ")}'
        if source_marker
        else None
    )

    now = _normalize_ts(pd.Timestamp.now('UTC'))
    signal_ids: list[str] = []
    conn = _connect(db_path)
    try:
        for signal, plan in zip(signals, plans):
            signal_id = build_signal_id(signal)
            signal_ids.append(signal_id)
            existing = _load_detected_signal_conn(conn, signal_id)

            status = "DETECTED"
            if existing and existing.get("status") in {"SUBMITTED", "PRESUBMITTED", "FILLED", "PARTIAL", "OPEN", "EXIT_SIGNAL", "CLOSED"}:
                status = existing["status"]

            merged = _merge_row(
                existing,
                signal_id=signal_id,
                pair=signal.pair,
                direction=signal.direction,
                signal_time=_normalize_ts(signal.time),
                detected_at=existing.get("detected_at", now) if existing else now,
                entry_price=float(signal.entry_price),
                sl_price=float(signal.sl_price),
                tp_price=float(signal.tp_price),
                zone_upper=float(signal.zone_upper),
                zone_lower=float(signal.zone_lower),
                zone_strength=signal.zone_strength,
                zone_type=signal.zone_type,
                quality_score=float(getattr(signal, "quality_score", 0.0) or 0.0),
                status=status,
                transacted=int(existing.get("transacted", 0) if existing else 0),
                execution_enabled=(
                    int(existing.get("execution_enabled", 0))
                    if existing and int(existing.get("transacted", 0) or 0) == 1
                    else int(bool(execute_orders))
                ),
                planned_units=int(plan.units) if plan is not None else None,
                risk_amount=float(plan.risk_amount) if plan is not None else None,
                account_currency=plan.account_currency if plan is not None else None,
                notional_account=float(plan.notional_account) if plan is not None else None,
                order_id=existing.get("order_id") if existing else None,
                take_profit_order_id=existing.get("take_profit_order_id") if existing else None,
                stop_loss_order_id=existing.get("stop_loss_order_id") if existing else None,
                note=(existing.get("note") if existing else None) or source_note,
                executed_at=existing.get("executed_at") if existing else None,
                opened_at=existing.get("opened_at") if existing else None,
                opened_price=existing.get("opened_price") if existing else None,
                open_units=existing.get("open_units") if existing else None,
                remaining_units=existing.get("remaining_units") if existing else None,
                fill_count=existing.get("fill_count") if existing else None,
                last_fill_at=existing.get("last_fill_at") if existing else None,
                broker_order_status=existing.get("broker_order_status") if existing else None,
                exit_signal_at=existing.get("exit_signal_at") if existing else None,
                exit_signal_reason=existing.get("exit_signal_reason") if existing else None,
                exit_signal_price=existing.get("exit_signal_price") if existing else None,
                closed_at=existing.get("closed_at") if existing else None,
                closed_price=existing.get("closed_price") if existing else None,
                close_reason=existing.get("close_reason") if existing else None,
                close_source=existing.get("close_source") if existing else None,
                pnl_pips=existing.get("pnl_pips") if existing else None,
                execution_mode=(existing.get("execution_mode") if existing else None) or resolved_mode,
                ibkr_account=(existing.get("ibkr_account") if existing else None) or ibkr_account,
                submitted_entry_price=existing.get("submitted_entry_price") if existing else None,
                submitted_tp_price=existing.get("submitted_tp_price") if existing else None,
                submitted_sl_price=existing.get("submitted_sl_price") if existing else None,
                submit_bid=existing.get("submit_bid") if existing else None,
                submit_ask=existing.get("submit_ask") if existing else None,
                submit_spread=existing.get("submit_spread") if existing else None,
                quote_source=(existing.get("quote_source") if existing else None) or source_marker,
                quote_time=existing.get("quote_time") if existing else None,
                last_updated_at=now,
            )
            _replace_row_conn(conn, merged)
        conn.commit()
    finally:
        conn.close()

    return signal_ids


def _resolve_execution_mode(execute_orders: bool) -> str:
    """Determine execution_mode from the IBKR port and execute flag."""

    if not execute_orders:
        return 'scan'
    try:
        from . import ibkr
        return ibkr.get_execution_mode()
    except Exception:
        return 'unknown'


def record_execution_results(
    signals: Iterable,
    size_plans: Optional[Iterable],
    execution_results: Iterable,
    *,
    execution_mode: str | None = None,
    ibkr_account: str | None = None,
    db_path: str | None = None,
) -> None:
    """Persist execution outcomes for the scanned signals."""

    db_path = _ensure_table(db_path)
    signals = list(signals)
    plans = list(size_plans) if size_plans is not None else [None] * len(signals)
    results = list(execution_results)
    if len(plans) != len(signals):
        plans = [None] * len(signals)

    resolved_mode = execution_mode or _resolve_execution_mode(True)

    now = _normalize_ts(pd.Timestamp.now('UTC'))
    conn = _connect(db_path)
    try:
        for signal, plan, result in zip(signals, plans, results):
            signal_id = build_signal_id(signal)
            existing = _load_detected_signal_conn(conn, signal_id)
            if existing is None:
                existing = _merge_row(
                    None,
                    signal_id=signal_id,
                    pair=signal.pair,
                    direction=signal.direction,
                    signal_time=_normalize_ts(signal.time),
                    detected_at=now,
                    entry_price=float(signal.entry_price),
                    sl_price=float(signal.sl_price),
                    tp_price=float(signal.tp_price),
                    zone_upper=float(signal.zone_upper),
                    zone_lower=float(signal.zone_lower),
                    zone_strength=signal.zone_strength,
                    zone_type=signal.zone_type,
                    quality_score=float(getattr(signal, "quality_score", 0.0) or 0.0),
                    status="DETECTED",
                    transacted=0,
                    execution_enabled=1,
                    planned_units=int(plan.units) if plan is not None else None,
                    risk_amount=float(plan.risk_amount) if plan is not None else None,
                    account_currency=plan.account_currency if plan is not None else None,
                    notional_account=float(plan.notional_account) if plan is not None else None,
                    order_id=None,
                    take_profit_order_id=None,
                    stop_loss_order_id=None,
                    note=None,
                    executed_at=None,
                    opened_at=None,
                    opened_price=None,
                    open_units=None,
                    remaining_units=None,
                    fill_count=None,
                    last_fill_at=None,
                    broker_order_status=None,
                    exit_signal_at=None,
                    exit_signal_reason=None,
                    exit_signal_price=None,
                    closed_at=None,
                    closed_price=None,
                    close_reason=None,
                    close_source=None,
                    pnl_pips=None,
                    execution_mode=resolved_mode,
                    ibkr_account=ibkr_account,
                    submitted_entry_price=None,
                    submitted_tp_price=None,
                    submitted_sl_price=None,
                    submit_bid=None,
                    submit_ask=None,
                    submit_spread=None,
                    quote_source=None,
                    quote_time=None,
                    last_updated_at=now,
                )

            incoming_status = _normalize_status(result.status)
            preserve_existing_execution = (
                incoming_status in _NON_TRANSACTIONAL_RESULT_STATUSES
                and _existing_has_execution_evidence(existing)
                and not _result_has_execution_evidence(result)
            )
            status = (
                _status_from_existing_execution(existing)
                if preserve_existing_execution
                else incoming_status
            )
            transacted = (
                int(existing.get("transacted") or 0) or 1
                if preserve_existing_execution
                else (0 if status in _NON_TRANSACTIONAL_RESULT_STATUSES else 1)
            )
            merged = _merge_row(
                existing,
                status=status,
                transacted=transacted,
                planned_units=int(plan.units) if plan is not None else existing.get("planned_units"),
                risk_amount=float(plan.risk_amount) if plan is not None else existing.get("risk_amount"),
                account_currency=plan.account_currency if plan is not None else existing.get("account_currency"),
                notional_account=float(plan.notional_account) if plan is not None else existing.get("notional_account"),
                order_id=result.order_id if result.order_id is not None else existing.get("order_id"),
                take_profit_order_id=(
                    result.take_profit_order_id
                    if getattr(result, "take_profit_order_id", None) is not None
                    else existing.get("take_profit_order_id")
                ),
                stop_loss_order_id=(
                    result.stop_loss_order_id
                    if getattr(result, "stop_loss_order_id", None) is not None
                    else existing.get("stop_loss_order_id")
                ),
                note=existing.get("note") if preserve_existing_execution else result.note,
                executed_at=(
                    existing.get("executed_at")
                    if preserve_existing_execution
                    else now
                ),
                opened_at=(
                    existing.get("opened_at")
                    if existing and existing.get("opened_at")
                    else (now if _normalize_units(getattr(result, "filled_units", None)) > 0 else None)
                ),
                opened_price=(
                    float(result.avg_fill_price)
                    if getattr(result, "avg_fill_price", None) not in (None, 0, 0.0)
                    else existing.get("opened_price")
                ),
                open_units=(
                    _normalize_units(getattr(result, "filled_units", None))
                    if _normalize_units(getattr(result, "filled_units", None)) > 0
                    else existing.get("open_units")
                ),
                remaining_units=(
                    _normalize_units(getattr(result, "remaining_units", None))
                    if getattr(result, "remaining_units", None) is not None
                    else existing.get("remaining_units")
                ),
                broker_order_status=(
                    getattr(result, "broker_status", None)
                    if getattr(result, "broker_status", None) is not None
                    else existing.get("broker_order_status")
                ),
                submitted_entry_price=(
                    float(result.submitted_entry_price)
                    if getattr(result, "submitted_entry_price", None) is not None
                    else existing.get("submitted_entry_price")
                ),
                submitted_tp_price=(
                    float(result.submitted_tp_price)
                    if getattr(result, "submitted_tp_price", None) is not None
                    else existing.get("submitted_tp_price")
                ),
                submitted_sl_price=(
                    float(result.submitted_sl_price)
                    if getattr(result, "submitted_sl_price", None) is not None
                    else existing.get("submitted_sl_price")
                ),
                submit_bid=(
                    float(result.submit_bid)
                    if getattr(result, "submit_bid", None) is not None
                    else existing.get("submit_bid")
                ),
                submit_ask=(
                    float(result.submit_ask)
                    if getattr(result, "submit_ask", None) is not None
                    else existing.get("submit_ask")
                ),
                submit_spread=(
                    float(result.submit_spread)
                    if getattr(result, "submit_spread", None) is not None
                    else existing.get("submit_spread")
                ),
                quote_source=(
                    getattr(result, "quote_source", None)
                    if getattr(result, "quote_source", None) is not None
                    else existing.get("quote_source")
                ),
                quote_time=(
                    _normalize_ts(pd.Timestamp(result.quote_time))
                    if getattr(result, "quote_time", None) is not None
                    else existing.get("quote_time")
                ),
                execution_mode=existing.get("execution_mode") or resolved_mode,
                ibkr_account=existing.get("ibkr_account") or ibkr_account,
                last_updated_at=now,
            )
            _replace_row_conn(conn, merged)
        conn.commit()
    finally:
        conn.close()


def claim_signal_for_position_conn(
    conn,
    pair: str,
    direction: str,
    *,
    opened_price: float,
    open_units: float,
) -> dict | None:
    """Claim the latest pending submitted signal using an existing transaction."""

    cursor = conn.execute(
        """
        SELECT *
        FROM detected_signal
        WHERE pair=%s AND direction=%s AND transacted=1
          AND closed_at IS NULL
          AND status IN ('SUBMITTED', 'PRESUBMITTED', 'FILLED', 'PARTIAL', 'OPEN', 'EXIT_SIGNAL')
        ORDER BY
            CASE WHEN opened_at IS NULL THEN 0 ELSE 1 END,
            COALESCE(executed_at, detected_at) DESC
        LIMIT 1
        """,
        (pair, direction),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    signal_row = _row_to_dict(cursor, row)

    now = _normalize_ts(pd.Timestamp.now('UTC'))
    filled_units = _normalize_units(open_units)
    planned_units = _normalize_units(signal_row.get('planned_units')) or filled_units
    remaining_units = max(planned_units - filled_units, 0) if planned_units > 0 else None
    if signal_row.get('status') == 'EXIT_SIGNAL':
        status = 'EXIT_SIGNAL'
    elif planned_units > 0 and filled_units < planned_units:
        status = 'PARTIAL'
    else:
        status = 'OPEN'
    merged = _merge_row(
        signal_row,
        status=status,
        opened_at=signal_row.get("opened_at") or signal_row.get("executed_at") or now,
        opened_price=float(opened_price),
        open_units=filled_units,
        remaining_units=remaining_units,
        last_updated_at=now,
    )
    _replace_row_conn(conn, merged)
    return merged


def claim_signal_for_position(
    pair: str,
    direction: str,
    *,
    opened_price: float,
    open_units: float,
    db_path: str | None = None,
) -> dict | None:
    """Claim the latest pending submitted signal for a newly opened position."""

    db_path = _ensure_table(db_path)
    with db_transaction(db_path) as conn:
        merged = claim_signal_for_position_conn(
            conn,
            pair,
            direction,
            opened_price=opened_price,
            open_units=open_units,
        )
        return merged


def record_exit_signal(
    signal_id: str,
    *,
    exit_reason: str,
    exit_price: float | None,
    db_path: str | None = None,
) -> None:
    """Persist the latest strategy-driven exit detection for an open trade."""

    db_path = _ensure_table(db_path)
    now = _normalize_ts(pd.Timestamp.now('UTC'))
    with db_transaction(db_path) as conn:
        existing = _load_detected_signal_conn(conn, signal_id)
        if existing is None:
            return

        merged = _merge_row(
            existing,
            status="EXIT_SIGNAL" if existing.get("closed_at") in (None, "") else existing.get("status"),
            exit_signal_at=now,
            exit_signal_reason=exit_reason,
            exit_signal_price=float(exit_price) if exit_price is not None else None,
            last_updated_at=now,
        )
        _replace_row_conn(conn, merged)


def _status_after_clearing_exit_signal(existing: dict) -> str:
    """Restore the open-trade lifecycle state after removing exit intent."""

    open_units = _normalize_units(existing.get('open_units'))
    planned_units = _normalize_units(existing.get('planned_units'))
    if open_units > 0:
        if planned_units > 0 and open_units < planned_units:
            return 'PARTIAL'
        return 'OPEN'

    broker_status = _normalize_status(existing.get('broker_order_status') or '')
    if broker_status in {'SUBMITTED', 'PRESUBMITTED', 'FILLED', 'PARTIAL', 'OPEN'}:
        return broker_status

    current_status = (existing.get('status') or '').upper()
    if current_status and current_status != 'EXIT_SIGNAL':
        return current_status
    return 'OPEN'


def clear_exit_signal(
    signal_id: str,
    *,
    db_path: str | None = None,
) -> None:
    """Clear a previously persisted strategy exit intent for an open trade."""

    db_path = _ensure_table(db_path)
    now = _normalize_ts(pd.Timestamp.now('UTC'))
    with db_transaction(db_path) as conn:
        existing = _load_detected_signal_conn(conn, signal_id)
        if existing is None:
            return

        merged = _merge_row(
            existing,
            status=(
                _status_after_clearing_exit_signal(existing)
                if existing.get('closed_at') in (None, '')
                else existing.get('status')
            ),
            exit_signal_at=None,
            exit_signal_reason=None,
            exit_signal_price=None,
            last_updated_at=now,
        )
        _replace_row_conn(conn, merged)


def record_closed_signal_conn(
    conn,
    signal_id: str,
    *,
    close_reason: str | None = None,
    close_price: float | None = None,
    close_source: str,
) -> dict | None:
    """Mark a transacted signal as closed using an existing transaction."""

    existing = _load_detected_signal_conn(conn, signal_id)
    if existing is None:
        return None

    resolved_price = (
        float(close_price)
        if close_price is not None
        else (
            float(existing["exit_signal_price"])
            if existing.get("exit_signal_price") is not None
            else None
        )
    )
    resolved_reason = close_reason or existing.get("exit_signal_reason") or "EXTERNAL_CLOSE"
    opened_price = (
        float(existing["opened_price"])
        if existing.get("opened_price") is not None
        else float(existing["entry_price"])
    )
    pnl_pips = None
    if resolved_price is not None:
        pip = _pair_pip(existing["pair"])
        if existing["direction"] == "LONG":
            pnl_pips = (resolved_price - opened_price) / pip
        else:
            pnl_pips = (opened_price - resolved_price) / pip

    now = _normalize_ts(pd.Timestamp.now('UTC'))
    merged = _merge_row(
        existing,
        status="CLOSED",
        transacted=1,
        closed_at=now,
        closed_price=resolved_price,
        close_reason=resolved_reason,
        close_source=close_source,
        pnl_pips=pnl_pips,
        last_updated_at=now,
    )
    _replace_row_conn(conn, merged)
    return merged


def record_closed_signal(
    signal_id: str,
    *,
    close_reason: str | None = None,
    close_price: float | None = None,
    close_source: str,
    db_path: str | None = None,
) -> dict | None:
    """Mark a transacted signal as closed and store the final outcome."""

    db_path = _ensure_table(db_path)
    with db_transaction(db_path) as conn:
        merged = record_closed_signal_conn(
            conn,
            signal_id,
            close_reason=close_reason,
            close_price=close_price,
            close_source=close_source,
        )
        return merged
