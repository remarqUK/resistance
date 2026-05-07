"""Shared detected-signal persistence helpers.

This module is intentionally broker-agnostic.  It owns the database row
helpers used by live history, broker reconciliation, and position sync so those
modules do not need to reach into each other's private functions.
"""

from __future__ import annotations

from datetime import date, datetime as dt_datetime
import threading

import pandas as pd

from .config import PAIRS
from .db import _connect, _normalize_ts, _table_columns, get_db_path, init_db


ACTIVE_EXECUTION_STATUSES = {
    "SUBMITTED",
    "PRESUBMITTED",
    "FILLED",
    "PARTIAL",
    "OPEN",
    "EXIT_SIGNAL",
}
_VALIDATION_WARNING_STATUSES = {
    "VALIDATIONERROR",
}
BROKER_EXECUTION_EVIDENCE_STATUSES = {
    "SUBMITTED",
    "PRESUBMITTED",
    "FILLED",
    "PARTIAL",
    "OPEN",
}
NON_TRANSACTIONAL_RESULT_STATUSES = {"SKIPPED", "FAILED"}

_ENSURE_TABLE_PATHS: set[str] = set()
_ENSURE_TABLE_LOCK = threading.Lock()


def serialize_ts(value):
    if value is None:
        return None
    if value == '':
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (dt_datetime, pd.Timestamp, date)):
        return value.isoformat().replace('T', ' ')
    return value


def ensure_signal_tables(db_path: str | None = None) -> str:
    """Create detected-signal and broker-ledger tables if needed."""

    if db_path is None:
        db_path = get_db_path()

    with _ENSURE_TABLE_LOCK:
        if db_path in _ENSURE_TABLE_PATHS:
            return db_path

        init_db(db_path, migrate_legacy=False)
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
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS broker_execution (
                    exec_id             TEXT PRIMARY KEY,
                    signal_id           TEXT,
                    pair                TEXT NOT NULL,
                    direction           TEXT,
                    role                TEXT,
                    side                TEXT,
                    order_id            {int_type},
                    perm_id             {int_type},
                    order_ref           TEXT,
                    fill_time           {ts_type},
                    fill_price          {real_type} NOT NULL,
                    fill_units          {int_type} NOT NULL,
                    cum_qty             {real_type},
                    avg_price           {real_type},
                    commission          {real_type},
                    commission_currency TEXT,
                    realized_pnl        {real_type},
                    recorded_at         {ts_type} NOT NULL,
                    updated_at          {ts_type} NOT NULL
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS broker_order_snapshot (
                    order_id       {int_type} PRIMARY KEY,
                    signal_id      TEXT,
                    pair           TEXT NOT NULL,
                    direction      TEXT,
                    role           TEXT,
                    parent_id      {int_type},
                    perm_id        {int_type},
                    order_ref      TEXT,
                    order_type     TEXT,
                    action         TEXT,
                    status         TEXT,
                    total_units    {real_type},
                    filled_units   {real_type},
                    remaining_units {real_type},
                    avg_fill_price {real_type},
                    last_seen_at   {ts_type} NOT NULL,
                    updated_at     {ts_type} NOT NULL
                )
                """
            )
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
                CREATE INDEX IF NOT EXISTS idx_detected_signal_closed_at_desc
                ON detected_signal (closed_at DESC)
                WHERE closed_at IS NOT NULL
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_detected_signal_pair_dir_time_open
                ON detected_signal (pair, direction, signal_time)
                WHERE closed_at IS NULL
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_detected_signal_executed_at_desc
                ON detected_signal (executed_at DESC)
                WHERE executed_at IS NOT NULL
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
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_broker_execution_signal_time
                ON broker_execution (signal_id, fill_time DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_broker_execution_order
                ON broker_execution (order_id, fill_time DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_broker_execution_ref
                ON broker_execution (order_ref, fill_time DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_broker_execution_open
                ON broker_execution (pair, direction, role, fill_time DESC)
                WHERE signal_id IS NOT NULL
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_broker_order_snapshot_ref
                ON broker_order_snapshot (order_ref, updated_at DESC)
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS pair_scan_log (
                    id         BIGSERIAL PRIMARY KEY,
                    scan_time  {ts_type} NOT NULL,
                    pair       TEXT NOT NULL,
                    state      TEXT NOT NULL,
                    note       TEXT,
                    price      {real_type},
                    signal_generated BOOLEAN NOT NULL DEFAULT FALSE,
                    direction  TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pair_scan_log_pair_time
                ON pair_scan_log (pair, scan_time DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pair_scan_log_time
                ON pair_scan_log (scan_time DESC)
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS system_event (
                    id         BIGSERIAL PRIMARY KEY,
                    event_time {ts_type} NOT NULL DEFAULT NOW(),
                    event_type TEXT NOT NULL,
                    detail     TEXT
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

        _ENSURE_TABLE_PATHS.add(db_path)
        return db_path


def row_to_dict(cursor, row) -> dict:
    """Convert a DB row to a dict keyed by column name."""

    if row is None:
        return {}
    return {
        description[0]: serialize_ts(row[idx])
        for idx, description in enumerate(cursor.description)
    }


def pair_pip(pair: str) -> float:
    """Return the configured pip size for a pair."""

    return PAIRS.get(pair, {}).get('pip', 0.0001)


def merge_row(existing: dict | None, **updates) -> dict:
    """Merge DB updates into an existing row payload."""

    merged = dict(existing or {})
    merged.update(updates)
    return merged


def load_detected_signal_conn(conn, signal_id: str) -> dict | None:
    """Load one detected-signal row using an existing DB connection."""

    cursor = conn.execute(
        "SELECT * FROM detected_signal WHERE signal_id=%s",
        (signal_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return row_to_dict(cursor, row)


def replace_detected_signal_conn(conn, row: dict) -> None:
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


def normalize_units(value) -> int:
    """Normalize a broker quantity to a non-negative integer."""

    try:
        return int(abs(float(value or 0.0)))
    except (TypeError, ValueError):
        return 0


def record_detected_signal_fill_conn(conn, signal_row: dict, fill: dict, *, recorded_at: str) -> bool:
    """Insert one parent-order fill row if it has not been seen before."""

    exec_id = (fill.get('exec_id') or '').strip()
    if not exec_id:
        return False

    fill_units = normalize_units(fill.get('shares'))
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


def detected_signal_fill_summary_conn(conn, signal_id: str) -> dict:
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


def signal_order_ref(row: dict) -> str | None:
    """Return the strategy order reference expected for a detected signal."""

    pair = str(row.get('pair') or '').upper()
    direction = str(row.get('direction') or '').upper()
    signal_time = row.get('signal_time')
    if not pair or not direction or signal_time is None:
        return None
    try:
        ts = pd.Timestamp(signal_time)
    except Exception:
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize('UTC')
    else:
        ts = ts.tz_convert('UTC')
    return f"fxsr:{pair}:{direction}:{ts.strftime('%Y%m%d%H%M%S')}"


def normalize_status(status: str) -> str:
    """Normalize execution state labels for storage."""

    normalized = (status or "").strip().upper()
    if not normalized:
        return "SUBMITTED"
    normalized_key = normalized.replace(" ", "").replace("-", "").replace("_", "")
    if normalized_key in _VALIDATION_WARNING_STATUSES:
        # IBKR emits these warnings as non-fatal broker messages (\"ValidationError\").
        # Treat them as submitted while we wait for a hard terminal update.
        return "SUBMITTED"
    return normalized


def derive_signal_execution_status(
    existing: dict,
    *,
    open_units: int,
    broker_order_status: str | None,
    order_found: bool = True,
) -> str:
    """Derive the internal signal lifecycle from fills plus raw broker status."""

    planned_units = normalize_units(existing.get('planned_units'))
    normalized_broker = normalize_status(broker_order_status or existing.get('status') or 'SUBMITTED')

    previously_filled = (
        existing.get('opened_price') is not None
        or normalize_units(existing.get('open_units')) > 0
    )
    if not order_found and open_units <= 0 and not previously_filled:
        return 'CANCELLED'

    if open_units <= 0 and not previously_filled:
        return normalized_broker

    if open_units <= 0 and previously_filled:
        existing_status = (existing.get('status') or '').upper()
        if existing_status in ('OPEN', 'PARTIAL', 'EXIT_SIGNAL', 'FILLED'):
            return existing_status
        return 'OPEN'

    if planned_units > 0 and open_units < planned_units:
        return 'PARTIAL'
    return 'OPEN'


def existing_has_execution_evidence(existing: dict | None) -> bool:
    """Return True when a row already has broker-backed execution state."""

    if not existing:
        return False
    if int(existing.get("transacted") or 0) == 1:
        return True
    if existing.get("order_id") is not None:
        return True
    if existing.get("opened_price") is not None:
        return True
    if normalize_units(existing.get("open_units")) > 0:
        return True
    broker_status_raw = existing.get("broker_order_status")
    if not broker_status_raw:
        return False
    broker_status = normalize_status(broker_status_raw)
    return broker_status in BROKER_EXECUTION_EVIDENCE_STATUSES


def result_has_execution_evidence(result) -> bool:
    """Return True when an incoming execution result carries broker state."""

    if getattr(result, "order_id", None) is not None:
        return True
    if getattr(result, "avg_fill_price", None) not in (None, 0, 0.0):
        return True
    if normalize_units(getattr(result, "filled_units", None)) > 0:
        return True
    broker_status_raw = getattr(result, "broker_status", None)
    if not broker_status_raw:
        return False
    broker_status = normalize_status(broker_status_raw)
    return broker_status in BROKER_EXECUTION_EVIDENCE_STATUSES


def status_from_existing_execution(existing: dict) -> str:
    """Derive a lifecycle status from existing broker-backed fields."""

    current = normalize_status(existing.get("status") or "")
    if current in {"CLOSED", "EXIT_SIGNAL", "CANCELLED", "REJECTED"}:
        return current

    open_units = normalize_units(existing.get("open_units"))
    planned_units = normalize_units(existing.get("planned_units"))
    if open_units > 0:
        if planned_units > 0 and open_units < planned_units:
            return "PARTIAL"
        return "OPEN"

    if current in ACTIVE_EXECUTION_STATUSES:
        return current

    broker_status_raw = existing.get("broker_order_status")
    if broker_status_raw:
        broker_status = normalize_status(broker_status_raw)
        if broker_status in BROKER_EXECUTION_EVIDENCE_STATUSES:
            return broker_status

    if existing.get("order_id") is not None:
        return "SUBMITTED"
    return current or "SUBMITTED"
