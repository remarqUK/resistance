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

from .config import PAIRS
from .db import _connect, _normalize_ts, _table_columns, db_transaction, get_db_path, init_db


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


_ENSURE_TABLE_PATHS: set[str] = set()


def _serialize_ts(value) -> str | None:
    if value is None:
        return None
    if value == '':
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (dt_datetime, pd.Timestamp, date)):
        return value.isoformat().replace('T', ' ')
    return value


def _ensure_table(db_path: str | None = None) -> str:
    """Create the detected-signal history table if needed (once per path)."""

    if db_path is None:
        db_path = get_db_path()

    if db_path in _ENSURE_TABLE_PATHS:
        return db_path

    init_db(db_path)
    conn = _connect(db_path)
    ts_type = "TIMESTAMPTZ"
    real_type = "DOUBLE PRECISION"
    int_type = "BIGINT"
    try:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS detected_signal (
                signal_id             TEXT PRIMARY KEY,
                pair                  TEXT NOT NULL,
                direction             TEXT NOT NULL,
                signal_time           {ts_type} NOT NULL,
                detected_at           {ts_type} NOT NULL,
                entry_price           {real_type} NOT NULL,
                sl_price              {real_type} NOT NULL,
                tp_price              {real_type} NOT NULL,
                zone_upper            {real_type} NOT NULL,
                zone_lower            {real_type} NOT NULL,
                zone_strength         TEXT NOT NULL,
                zone_type             TEXT NOT NULL,
                quality_score         {real_type} NOT NULL DEFAULT 0,
                status                TEXT NOT NULL,
                transacted            INTEGER NOT NULL DEFAULT 0,
                execution_enabled     INTEGER NOT NULL DEFAULT 0,
                planned_units         {int_type},
                risk_amount           {real_type},
                account_currency      TEXT,
                notional_account      {real_type},
                order_id              {int_type},
                take_profit_order_id  {int_type},
                stop_loss_order_id    {int_type},
                note                  TEXT,
                executed_at           {ts_type},
                opened_at             {ts_type},
                opened_price          {real_type},
                open_units            {int_type},
                remaining_units       {int_type},
                fill_count            {int_type},
                last_fill_at          {ts_type},
                broker_order_status   TEXT,
                exit_signal_at        {ts_type},
                exit_signal_reason    TEXT,
                exit_signal_price     {real_type},
                closed_at             {ts_type},
                closed_price          {real_type},
                close_reason          TEXT,
                close_source          TEXT,
                pnl_pips              {real_type},
                execution_mode        TEXT,
                ibkr_account          TEXT,
                submitted_entry_price {real_type},
                submitted_tp_price    {real_type},
                submitted_sl_price    {real_type},
                submit_bid            {real_type},
                submit_ask            {real_type},
                submit_spread         {real_type},
                quote_source          TEXT,
                quote_time            {ts_type},
                last_updated_at       {ts_type} NOT NULL
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS detected_signal_fill (
                exec_id      TEXT PRIMARY KEY,
                signal_id    TEXT NOT NULL,
                pair         TEXT NOT NULL,
                direction    TEXT NOT NULL,
                order_id     {int_type},
                fill_time    {ts_type},
                fill_price   {real_type} NOT NULL,
                fill_units   {int_type} NOT NULL,
                cum_qty      {real_type},
                avg_price    {real_type},
                side         TEXT,
                order_ref    TEXT,
                recorded_at  {ts_type} NOT NULL
            )
            """
        )
        # Migrate existing tables that lack the new columns
        existing = _table_columns(conn, "detected_signal")
        for col, ddl in (
            ('execution_mode', 'TEXT'),
            ('ibkr_account', 'TEXT'),
            ('submitted_entry_price', real_type),
            ('submitted_tp_price', real_type),
            ('submitted_sl_price', real_type),
            ('submit_bid', real_type),
            ('submit_ask', real_type),
            ('submit_spread', real_type),
            ('quote_source', 'TEXT'),
            ('quote_time', ts_type),
            ('remaining_units', 'INTEGER'),
            ('fill_count', 'INTEGER'),
            ('last_fill_at', ts_type),
            ('broker_order_status', 'TEXT'),
        ):
            if col not in existing:
                conn.execute(f"ALTER TABLE detected_signal ADD COLUMN {col} {ddl}")
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_detected_signal_pair_time
            ON detected_signal (pair, signal_time DESC)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_detected_signal_status
            ON detected_signal (status, transacted, pair, direction)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_detected_signal_pair_status_time
            ON detected_signal (pair, status, signal_time DESC, detected_at DESC)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_detected_signal_status_last_updated
            ON detected_signal (status, pair, last_updated_at DESC)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_detected_signal_claim
            ON detected_signal (
                pair,
                direction,
                status,
                opened_at,
                executed_at,
                detected_at
            )
            WHERE transacted = 1 AND closed_at IS NULL
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_detected_signal_reconcile
            ON detected_signal (order_id, status, pair)
            WHERE transacted = 1 AND closed_at IS NULL AND order_id IS NOT NULL
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_detected_signal_fill_signal_time
            ON detected_signal_fill (signal_id, fill_time DESC)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_detected_signal_fill_signal_time_asc
            ON detected_signal_fill (signal_id, fill_time ASC, recorded_at ASC, exec_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_detected_signal_fill_order
            ON detected_signal_fill (order_id, fill_time DESC)
            """
        )
        conn.commit()
    finally:
        conn.close()

    _ENSURE_TABLE_PATHS.add(db_path)
    return db_path


def _row_to_dict(cursor, row) -> dict:
    """Convert a DB row to a dict keyed by column name."""

    if row is None:
        return {}
    return {
        description[0]: _serialize_ts(row[idx])
        for idx, description in enumerate(cursor.description)
    }


def _pair_pip(pair: str) -> float:
    """Return the configured pip size for a pair."""

    return PAIRS.get(pair, {}).get('pip', 0.0001)


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
        return [_row_to_dict(cursor, row) for row in rows]
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
        return [_row_to_dict(cursor, row) for row in rows]
    finally:
        conn.close()


def _merge_row(existing: dict | None, **updates) -> dict:
    """Merge DB updates into an existing row payload."""

    merged = dict(existing or {})
    merged.update(updates)
    return merged


def _load_detected_signal_conn(conn, signal_id: str) -> dict | None:
    """Load one detected-signal row using an existing DB connection."""

    cursor = conn.execute(
        "SELECT * FROM detected_signal WHERE signal_id=%s",
        (signal_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return _row_to_dict(cursor, row)


def _replace_row_conn(conn, row: dict) -> None:
    """Insert or replace one fully materialized detected-signal row."""

    conn.execute(
        """
        INSERT INTO detected_signal (
            signal_id, pair, direction, signal_time, detected_at,
            entry_price, sl_price, tp_price, zone_upper, zone_lower,
            zone_strength, zone_type, quality_score, status, transacted,
            execution_enabled, planned_units, risk_amount, account_currency,
            notional_account, order_id, take_profit_order_id,
            stop_loss_order_id, note, executed_at, opened_at,
            opened_price, open_units, remaining_units, fill_count,
            last_fill_at, broker_order_status, exit_signal_at, exit_signal_reason,
            exit_signal_price, closed_at, closed_price, close_reason,
            close_source, pnl_pips, execution_mode, ibkr_account,
            submitted_entry_price, submitted_tp_price, submitted_sl_price,
            submit_bid, submit_ask, submit_spread, quote_source, quote_time,
            last_updated_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s
        )
        ON CONFLICT (signal_id) DO UPDATE SET
            pair = EXCLUDED.pair,
            direction = EXCLUDED.direction,
            signal_time = EXCLUDED.signal_time,
            detected_at = EXCLUDED.detected_at,
            entry_price = EXCLUDED.entry_price,
            sl_price = EXCLUDED.sl_price,
            tp_price = EXCLUDED.tp_price,
            zone_upper = EXCLUDED.zone_upper,
            zone_lower = EXCLUDED.zone_lower,
            zone_strength = EXCLUDED.zone_strength,
            zone_type = EXCLUDED.zone_type,
            quality_score = EXCLUDED.quality_score,
            status = EXCLUDED.status,
            transacted = EXCLUDED.transacted,
            execution_enabled = EXCLUDED.execution_enabled,
            planned_units = EXCLUDED.planned_units,
            risk_amount = EXCLUDED.risk_amount,
            account_currency = EXCLUDED.account_currency,
            notional_account = EXCLUDED.notional_account,
            order_id = EXCLUDED.order_id,
            take_profit_order_id = EXCLUDED.take_profit_order_id,
            stop_loss_order_id = EXCLUDED.stop_loss_order_id,
            note = EXCLUDED.note,
            executed_at = EXCLUDED.executed_at,
            opened_at = EXCLUDED.opened_at,
            opened_price = EXCLUDED.opened_price,
            open_units = EXCLUDED.open_units,
            remaining_units = EXCLUDED.remaining_units,
            fill_count = EXCLUDED.fill_count,
            last_fill_at = EXCLUDED.last_fill_at,
            broker_order_status = EXCLUDED.broker_order_status,
            exit_signal_at = EXCLUDED.exit_signal_at,
            exit_signal_reason = EXCLUDED.exit_signal_reason,
            exit_signal_price = EXCLUDED.exit_signal_price,
            closed_at = EXCLUDED.closed_at,
            closed_price = EXCLUDED.closed_price,
            close_reason = EXCLUDED.close_reason,
            close_source = EXCLUDED.close_source,
            pnl_pips = EXCLUDED.pnl_pips,
            execution_mode = EXCLUDED.execution_mode,
            ibkr_account = EXCLUDED.ibkr_account,
            submitted_entry_price = EXCLUDED.submitted_entry_price,
            submitted_tp_price = EXCLUDED.submitted_tp_price,
            submitted_sl_price = EXCLUDED.submitted_sl_price,
            submit_bid = EXCLUDED.submit_bid,
            submit_ask = EXCLUDED.submit_ask,
            submit_spread = EXCLUDED.submit_spread,
            quote_source = EXCLUDED.quote_source,
            quote_time = EXCLUDED.quote_time,
            last_updated_at = EXCLUDED.last_updated_at
        """,
        (
            row['signal_id'], row['pair'], row['direction'], row['signal_time'], row['detected_at'],
            row['entry_price'], row['sl_price'], row['tp_price'], row['zone_upper'], row['zone_lower'],
            row['zone_strength'], row['zone_type'], row['quality_score'], row['status'], row['transacted'],
            row['execution_enabled'], row['planned_units'], row['risk_amount'], row['account_currency'],
            row['notional_account'], row['order_id'], row['take_profit_order_id'],
            row['stop_loss_order_id'], row['note'], row['executed_at'], row['opened_at'],
            row['opened_price'], row['open_units'], row['remaining_units'], row['fill_count'],
            row['last_fill_at'], row['broker_order_status'], row['exit_signal_at'], row['exit_signal_reason'],
            row['exit_signal_price'], row['closed_at'], row['closed_price'], row['close_reason'],
            row['close_source'], row['pnl_pips'], row['execution_mode'], row['ibkr_account'],
            row['submitted_entry_price'], row['submitted_tp_price'], row['submitted_sl_price'],
            row['submit_bid'], row['submit_ask'], row['submit_spread'], row['quote_source'], row['quote_time'],
            row['last_updated_at'],
        ),
    )


def _normalize_units(value) -> int:
    """Normalize a broker quantity to a non-negative integer."""

    try:
        return int(abs(float(value or 0.0)))
    except (TypeError, ValueError):
        return 0


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


def _record_signal_fill_conn(conn, signal_row: dict, fill: dict, *, recorded_at: str) -> bool:
    """Insert one parent-order fill row if it has not been seen before."""

    exec_id = (fill.get('exec_id') or '').strip()
    if not exec_id:
        return False

    fill_units = _normalize_units(fill.get('shares'))
    if fill_units <= 0:
        return False

    fill_time = fill.get('time')
    normalized_fill_time = (
        _normalize_ts(pd.Timestamp(fill_time))
        if fill_time is not None
        else None
    )

    conn.execute(
        """
        INSERT INTO detected_signal_fill (
            exec_id, signal_id, pair, direction, order_id, fill_time,
            fill_price, fill_units, cum_qty, avg_price, side, order_ref, recorded_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (exec_id) DO NOTHING
        """,
        (
            exec_id,
            signal_row['signal_id'],
            signal_row['pair'],
            signal_row['direction'],
            int(fill['order_id']) if fill.get('order_id') is not None else None,
            normalized_fill_time,
            float(fill.get('price') or 0.0),
            fill_units,
            float(fill.get('cum_qty') or 0.0) if fill.get('cum_qty') is not None else None,
            float(fill.get('avg_price') or 0.0) if fill.get('avg_price') is not None else None,
            (fill.get('side') or '').upper() or None,
            fill.get('order_ref') or None,
            recorded_at,
        ),
    )
    return conn.total_changes > 0


def _signal_fill_summary_conn(conn, signal_id: str) -> dict:
    """Return aggregated persisted fill statistics for one detected signal."""

    row = conn.execute(
        """
        SELECT
            COALESCE(SUM(fill_units), 0) AS total_units,
            COALESCE(SUM(fill_price * fill_units), 0.0) AS weighted_sum,
            COUNT(*) AS fill_count,
            MIN(fill_time) AS first_fill_at,
            MAX(fill_time) AS last_fill_at
        FROM detected_signal_fill
        WHERE signal_id=%s
        """,
        (signal_id,),
    ).fetchone()
    total_units = int(row[0] or 0)
    weighted_sum = float(row[1] or 0.0)
    fill_count = int(row[2] or 0)
    average_price = weighted_sum / total_units if total_units > 0 else None
    return {
        'open_units': total_units,
        'opened_price': average_price,
        'fill_count': fill_count,
        'opened_at': row[3],
        'last_fill_at': row[4],
    }


def _derive_signal_execution_status(
    existing: dict,
    *,
    open_units: int,
    broker_order_status: str | None,
) -> str:
    """Derive the internal signal lifecycle from fills plus raw broker status."""

    planned_units = _normalize_units(existing.get('planned_units'))
    normalized_broker = _normalize_status(broker_order_status or existing.get('status') or 'SUBMITTED')
    if open_units <= 0:
        return normalized_broker
    if planned_units > 0 and open_units < planned_units:
        return 'PARTIAL'
    return 'OPEN'


def reconcile_detected_signal_orders(
    *,
    signal_ids: Iterable[str] | None = None,
    db_path: str | None = None,
) -> list[dict]:
    """Reconcile live detected signals with broker fills and parent order status."""

    from . import ibkr

    db_path = _ensure_table(db_path)
    now = _normalize_ts(pd.Timestamp.now('UTC'))
    conn = _connect(db_path)
    try:
        params: list[object] = []
        query = """
            SELECT *
            FROM detected_signal
            WHERE transacted=1
              AND order_id IS NOT NULL
              AND closed_at IS NULL
              AND status IN ('SUBMITTED', 'PRESUBMITTED', 'FILLED', 'PARTIAL', 'OPEN', 'EXIT_SIGNAL')
        """
        if signal_ids is not None:
            signal_ids = [str(signal_id) for signal_id in signal_ids if signal_id]
            if not signal_ids:
                return []
            query += " AND signal_id IN ({})".format(",".join(["%s"] * len(signal_ids)))
            params.extend(signal_ids)

        cursor = conn.execute(query, params)
        rows = [_row_to_dict(cursor, row) for row in cursor.fetchall()]
        if not rows:
            return []

        order_ids = {
            int(row['order_id'])
            for row in rows
            if row.get('order_id') is not None
        }
        fills_by_order: dict[int, list[dict]] = {}
        for fill in ibkr.fetch_fx_fills(order_ids=order_ids):
            order_id = fill.get('order_id')
            if order_id is None:
                continue
            fills_by_order.setdefault(int(order_id), []).append(fill)

        statuses_by_order = {
            int(snapshot['order_id']): snapshot
            for snapshot in ibkr.fetch_fx_order_statuses(order_ids=order_ids)
            if snapshot.get('order_id') is not None
        }

        updated_rows: list[dict] = []
        for existing in rows:
            order_id = existing.get('order_id')
            if order_id is None:
                continue
            for fill in fills_by_order.get(int(order_id), []):
                _record_signal_fill_conn(conn, existing, fill, recorded_at=now)

            fill_summary = _signal_fill_summary_conn(conn, existing['signal_id'])
            broker_snapshot = statuses_by_order.get(int(order_id), {})
            broker_order_status = broker_snapshot.get('status') or existing.get('broker_order_status')
            open_units = max(
                _normalize_units(fill_summary['open_units']),
                _normalize_units(existing.get('open_units')),
            )
            planned_units = _normalize_units(existing.get('planned_units'))
            if open_units <= 0:
                open_units = _normalize_units(broker_snapshot.get('filled_units'))

            if planned_units > 0:
                computed_remaining_units = max(planned_units - open_units, 0)
            else:
                computed_remaining_units = _normalize_units(broker_snapshot.get('remaining_units'))

            if broker_snapshot.get('remaining_units') is not None:
                broker_remaining_units = _normalize_units(broker_snapshot.get('remaining_units'))
                if open_units > 0:
                    remaining_units = min(computed_remaining_units, broker_remaining_units)
                else:
                    remaining_units = broker_remaining_units
            else:
                remaining_units = computed_remaining_units

            opened_price = fill_summary['opened_price']
            if opened_price is None and existing.get('opened_price') is not None:
                opened_price = float(existing['opened_price'])
            opened_at = fill_summary['opened_at'] or existing.get('opened_at')
            last_fill_at = fill_summary['last_fill_at'] or existing.get('last_fill_at')
            fill_count = max(int(fill_summary['fill_count'] or 0), int(existing.get('fill_count') or 0))
            status = _derive_signal_execution_status(
                existing,
                open_units=open_units,
                broker_order_status=broker_order_status,
            )

            if open_units > 0 and planned_units > 0 and open_units < planned_units:
                note = f"partial fill {open_units:,}/{planned_units:,}"
            elif open_units > 0 and planned_units > 0:
                note = f"filled {open_units:,}/{planned_units:,}"
            elif broker_order_status:
                note = f"broker status {broker_order_status}"
            else:
                note = existing.get('note')

            merged = _merge_row(
                existing,
                status=status,
                transacted=1,
                opened_at=opened_at,
                opened_price=float(opened_price) if opened_price is not None else existing.get('opened_price'),
                open_units=open_units if open_units > 0 else existing.get('open_units'),
                remaining_units=remaining_units,
                fill_count=fill_count,
                last_fill_at=last_fill_at,
                broker_order_status=broker_order_status,
                note=note,
                last_updated_at=now,
            )
            _replace_row_conn(conn, merged)
            updated_rows.append(merged)

        conn.commit()
        return updated_rows
    finally:
        conn.close()


def record_detected_signals(
    signals: Iterable,
    size_plans: Optional[Iterable] = None,
    *,
    execute_orders: bool,
    execution_mode: str | None = None,
    ibkr_account: str | None = None,
    db_path: str | None = None,
) -> list[str]:
    """Upsert the currently detected signals into the history table."""

    db_path = _ensure_table(db_path)
    signals = list(signals)
    plans = list(size_plans) if size_plans is not None else [None] * len(signals)
    if len(plans) != len(signals):
        plans = [None] * len(signals)

    resolved_mode = execution_mode or _resolve_execution_mode(execute_orders)

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
                note=existing.get("note") if existing else None,
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
                quote_source=existing.get("quote_source") if existing else None,
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


def _normalize_status(status: str) -> str:
    """Normalize execution state labels for storage."""

    normalized = (status or "").strip().upper()
    if not normalized:
        return "SUBMITTED"
    return normalized


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

            status = _normalize_status(result.status)
            transacted = 0 if status in {"SKIPPED", "FAILED"} else 1
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
                note=result.note,
                executed_at=now,
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
