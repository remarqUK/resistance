"""Position tracking: monitor IBKR positions against strategy exit rules.

Phase 1 (read-only):
- Reads open FX positions from TWS
- Matches them to S/R zones to construct Trade objects
- Runs check_exit() each scan cycle and alerts on exit conditions
- Persists state to the shared database so restarts resume monitoring
"""

from contextlib import contextmanager
from datetime import datetime
from typing import Callable, Dict, List, Optional

import pandas as pd

from .config import PAIRS, DEFAULT_ZONE_HISTORY_DAYS
from .data import fetch_daily_data, fetch_hourly_data
from .db import _connect, _normalize_ts, db_transaction, get_db_path
from .live_history import (
    claim_signal_for_position_conn,
    ensure_detected_signal_table,
    load_detected_signal,
    reconcile_detected_signal_orders,
    record_closed_signal_conn,
    record_exit_signal,
)
from .levels import detect_zones, SRZone
from .strategy import Trade, StrategyParams, check_exit, get_market_exit_price, get_tradeable_zones
from . import ibkr


# ---------------------------------------------------------------------------
# Helpers (shared across module)
# ---------------------------------------------------------------------------

def calc_pnl_pips(trade: Trade, current_mid_price: float, pip: float, params: StrategyParams) -> float:
    """Calculate executable P&L in pips using the shared midpoint fill model."""
    exit_price = get_market_exit_price(current_mid_price, trade.direction, pip, params)
    if trade.direction == 'LONG':
        return (exit_price - trade.entry_price) / pip
    return (trade.entry_price - exit_price) / pip


def format_size(size: float) -> str:
    """Format position size for display (e.g. 50000 -> '50K')."""
    abs_size = abs(size)
    if abs_size >= 1000:
        return f"{abs_size / 1000:.0f}K"
    return f"{abs_size:.0f}"


def pair_pip(pair: str) -> float:
    """Get pip size for a pair."""
    return PAIRS.get(pair, {}).get('pip', 0.0001)


def pair_decimals(pair: str) -> int:
    """Get display decimals for a pair."""
    return PAIRS.get(pair, {}).get('decimals', 5)


def pair_ticker(pair: str) -> Optional[str]:
    """Get the internal ticker/cache key for a pair."""
    info = PAIRS.get(pair)
    return info['ticker'] if info else None


# ---------------------------------------------------------------------------
# PostgreSQL persistence for tracked trades
# ---------------------------------------------------------------------------

_TABLE_INIT_PATHS: set[str] = set()


def _row_to_dict(cursor, row) -> dict:
    if row is None:
        return {}
    return {
        description[0]: row[idx]
        for idx, description in enumerate(cursor.description)
    }


def _to_ts(value):
    if value is None or value == '':
        return None
    if isinstance(value, str):
        return value
    return _normalize_ts(value)


def _ensure_columns(conn, table: str, required_columns: Dict[str, str]):
    """Add any missing columns required by the current schema."""
    existing = {
        row[0]
        for row in conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )
    }
    for column_name, column_ddl in required_columns.items():
        if column_name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column_name} {column_ddl}")


def _ensure_table(db_path: str = None):
    """Create the open_trades table if it doesn't exist (once per session)."""
    if db_path is None:
        db_path = get_db_path()
    if db_path in _TABLE_INIT_PATHS:
        return
    conn = _connect(db_path)
    ts_type = 'TIMESTAMPTZ'
    real_type = "DOUBLE PRECISION"
    try:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS open_trades (
                pair          TEXT NOT NULL,
                direction     TEXT NOT NULL,
                entry_time    {ts_type} NOT NULL,
                entry_price   {real_type} NOT NULL,
                sl_price      {real_type} NOT NULL,
                tp_price      {real_type} NOT NULL,
                zone_upper    {real_type} NOT NULL,
                zone_lower    {real_type} NOT NULL,
                zone_strength TEXT NOT NULL,
                risk          {real_type} NOT NULL,
                bars_monitored INTEGER DEFAULT 0,
                ibkr_avg_cost  {real_type},
                ibkr_size      {real_type},
                signal_id      TEXT,
                pending_exit_reason TEXT,
                pending_exit_price {real_type},
                pending_exit_detected_at {ts_type},
                last_processed_bar_time {ts_type},
                created_at    {ts_type} NOT NULL,
                PRIMARY KEY (pair, direction)
            )
        """)
        _ensure_columns(
            conn,
            'open_trades',
            {
                'signal_id': 'TEXT',
                'pending_exit_reason': 'TEXT',
                'pending_exit_price': real_type,
                'pending_exit_detected_at': ts_type,
                'last_processed_bar_time': ts_type,
                'created_at': ts_type,
                'entry_time': ts_type,
            },
        )
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_open_trades_pair_direction
            ON open_trades (pair, direction)
        """)
        conn.commit()
    finally:
        conn.close()
    _TABLE_INIT_PATHS.add(db_path)


def _ensure_tracking_tables(db_path: str | None = None) -> str:
    """Ensure both tracked-position tables exist for the shared DB."""

    if db_path is None:
        db_path = get_db_path()
    _ensure_table(db_path)
    ensure_detected_signal_table(db_path)
    return db_path


@contextmanager
def _tracking_db_transaction(db_path: str | None = None):
    """Yield one shared transaction covering open_trades and detected_signal."""

    db_path = _ensure_tracking_tables(db_path)
    with db_transaction(db_path) as conn:
        yield conn


def _db_execute(sql: str, params: tuple = (), db_path: str = None):
    """Execute a single SQL statement."""
    if db_path is None:
        db_path = get_db_path()
    _ensure_table(db_path)
    with db_transaction(db_path) as conn:
        conn.execute(sql, params)


def _save_trade(
    pair: str,
    trade: Trade,
    ibkr_avg_cost: float,
    ibkr_size: float,
    signal_id: str | None = None,
    last_processed_bar_time: Optional[pd.Timestamp] = None,
):
    """Save or update a tracked trade in the DB."""
    db_path = get_db_path()
    _ensure_table(db_path)
    with db_transaction(db_path) as conn:
        _save_trade_conn(
            conn,
            pair,
            trade,
            ibkr_avg_cost,
            ibkr_size,
            signal_id=signal_id,
            last_processed_bar_time=last_processed_bar_time,
        )


def _save_trade_conn(
    conn,
    pair: str,
    trade: Trade,
    ibkr_avg_cost: float,
    ibkr_size: float,
    signal_id: str | None = None,
    last_processed_bar_time: Optional[pd.Timestamp] = None,
):
    """Save or update a tracked trade using an existing transaction."""

    cursor = conn.execute(
        """
        SELECT bars_monitored, pending_exit_reason, pending_exit_price,
               pending_exit_detected_at, last_processed_bar_time, created_at
        FROM open_trades
        WHERE pair=%s AND direction=%s
        """,
        (pair, trade.direction),
    )
    row = cursor.fetchone()
    existing = _row_to_dict(cursor, row)
    last_processed = (
        _to_ts(last_processed_bar_time)
        if last_processed_bar_time is not None
        else existing.get('last_processed_bar_time')
    )
    created_at = existing.get('created_at') or _normalize_ts(pd.Timestamp.now('UTC'))

    conn.execute(
        """INSERT INTO open_trades
           (pair, direction, entry_time, entry_price, sl_price, tp_price,
            zone_upper, zone_lower, zone_strength, risk, bars_monitored,
            ibkr_avg_cost, ibkr_size, signal_id, pending_exit_reason,
            pending_exit_price, pending_exit_detected_at,
            last_processed_bar_time, created_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (pair, direction) DO UPDATE
           SET
               entry_time = EXCLUDED.entry_time,
               entry_price = EXCLUDED.entry_price,
               sl_price = EXCLUDED.sl_price,
               tp_price = EXCLUDED.tp_price,
               zone_upper = EXCLUDED.zone_upper,
               zone_lower = EXCLUDED.zone_lower,
               zone_strength = EXCLUDED.zone_strength,
               risk = EXCLUDED.risk,
               bars_monitored = EXCLUDED.bars_monitored,
               ibkr_avg_cost = EXCLUDED.ibkr_avg_cost,
               ibkr_size = EXCLUDED.ibkr_size,
               signal_id = EXCLUDED.signal_id,
               pending_exit_reason = EXCLUDED.pending_exit_reason,
               pending_exit_price = EXCLUDED.pending_exit_price,
               pending_exit_detected_at = EXCLUDED.pending_exit_detected_at,
               last_processed_bar_time = EXCLUDED.last_processed_bar_time,
               created_at = EXCLUDED.created_at
        """,
        (
            pair,
            trade.direction,
            _to_ts(trade.entry_time),
            trade.entry_price, trade.sl_price, trade.tp_price,
            trade.zone_upper, trade.zone_lower, trade.zone_strength,
            trade.risk,
            int(existing.get('bars_monitored') or 0),
            ibkr_avg_cost,
            ibkr_size,
            signal_id,
            existing.get('pending_exit_reason'),
            existing.get('pending_exit_price'),
            existing.get('pending_exit_detected_at'),
            last_processed,
            created_at,
        ),
    )


def _load_trades() -> Dict[str, dict]:
    """Load all tracked trades from DB. Returns dict keyed by 'PAIR:DIRECTION'."""
    db_path = get_db_path()
    _ensure_table(db_path)
    conn = _connect(db_path)
    try:
        cursor = conn.execute("SELECT * FROM open_trades")
        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
    finally:
        conn.close()

    result = {}
    for row in rows:
        r = {column: row[idx] for idx, column in enumerate(columns)}
        signal_id = r['signal_id']
        signal_row = load_detected_signal(signal_id) if signal_id else None
        if signal_row is not None and not signal_row.get('closed_at'):
            trade = _build_trade_from_signal_row(signal_row)
            pending_exit_reason = signal_row.get('exit_signal_reason')
            pending_exit_price = signal_row.get('exit_signal_price')
            pending_exit_detected_at = (
                pd.Timestamp(signal_row['exit_signal_at'])
                if signal_row.get('exit_signal_at')
                else None
            )
        else:
            trade = Trade(
                entry_time=pd.Timestamp(r['entry_time']),
                entry_price=r['entry_price'], direction=r['direction'],
                sl_price=r['sl_price'], tp_price=r['tp_price'],
                zone_upper=r['zone_upper'], zone_lower=r['zone_lower'],
                zone_strength=r['zone_strength'], risk=r['risk'],
            )
            pending_exit_reason = r['pending_exit_reason']
            pending_exit_price = r['pending_exit_price']
            pending_exit_detected_at = pd.Timestamp(r['pending_exit_detected_at']) if r['pending_exit_detected_at'] else None

        key = f"{r['pair']}:{r['direction']}"
        result[key] = {
            'pair': r['pair'],
            'trade': trade,
            'bars_monitored': r['bars_monitored'],
            'ibkr_avg_cost': r['ibkr_avg_cost'],
            'ibkr_size': r['ibkr_size'],
            'signal_id': signal_id,
            'signal_status': signal_row.get('status') if signal_row is not None else None,
            'pending_exit_reason': pending_exit_reason,
            'pending_exit_price': pending_exit_price,
            'pending_exit_detected_at': pending_exit_detected_at,
            'last_processed_bar_time': pd.Timestamp(r['last_processed_bar_time']) if r['last_processed_bar_time'] else None,
        }
    return result


def _remove_trade(pair: str, direction: str):
    """Remove a trade from the DB (position was closed)."""
    db_path = get_db_path()
    _ensure_table(db_path)
    with db_transaction(db_path) as conn:
        _remove_trade_conn(conn, pair, direction)


def _remove_trade_conn(conn, pair: str, direction: str):
    """Remove a tracked trade using an existing transaction."""

    conn.execute(
        "DELETE FROM open_trades WHERE pair=%s AND direction=%s",
        (pair, direction),
    )


def _save_bar_tracking(
    pair: str,
    direction: str,
    bars_monitored: int,
    last_processed_bar_time: Optional[pd.Timestamp],
):
    """Persist the latest processed hourly bar and monitored-bar count."""

    db_path = get_db_path()
    _db_execute(
        "UPDATE open_trades SET bars_monitored = %s, last_processed_bar_time = %s "
        "WHERE pair = %s AND direction = %s",
        (
            int(bars_monitored),
            _to_ts(last_processed_bar_time) if last_processed_bar_time is not None else None,
            pair,
            direction,
        ),
    )


def save_bar_tracking(
    pair: str,
    direction: str,
    bars_monitored: int,
    last_processed_bar_time: Optional[pd.Timestamp],
):
    """Persist hourly bar tracking state for one open trade."""

    _save_bar_tracking(pair, direction, bars_monitored, last_processed_bar_time)


# ---------------------------------------------------------------------------
# Trade construction from IBKR positions
# ---------------------------------------------------------------------------

def _compute_sl_tp(entry: float, zone: SRZone, direction: str, params: StrategyParams):
    """Compute SL, TP, and risk from zone + direction. Shared logic with strategy."""
    if direction == 'LONG':
        sl = zone.lower * (1 - params.sl_buffer_pct / 100)
        risk = entry - sl
        if risk <= 0:
            risk = entry * 0.003
            sl = entry - risk
        tp = entry + risk * params.rr_ratio
    else:
        sl = zone.upper * (1 + params.sl_buffer_pct / 100)
        risk = sl - entry
        if risk <= 0:
            risk = entry * 0.003
            sl = entry + risk
        tp = entry - risk * params.rr_ratio
    return sl, tp, risk


def _build_trade_from_position(
    pair: str,
    avg_cost: float,
    direction: str,
    params: StrategyParams,
    zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS,
) -> Optional[Trade]:
    """Construct an approximate Trade object from an IBKR position.

    Finds the nearest zone matching the direction and computes SL/TP.
    """
    ticker = pair_ticker(pair)
    if not ticker:
        return None

    daily_df = fetch_daily_data(ticker, days=zone_history_days)
    if daily_df.empty:
        return None

    zones = detect_zones(daily_df)
    nearest_sup, nearest_res = get_tradeable_zones(zones, avg_cost)

    # Pick zone matching direction, fallback to other, fallback to synthetic
    zone = None
    if direction == 'LONG':
        zone = nearest_sup or nearest_res
    else:
        zone = nearest_res or nearest_sup

    if zone is None:
        zone_width = avg_cost * 0.002
        zone = SRZone(
            upper=avg_cost + zone_width / 2,
            lower=avg_cost - zone_width / 2,
            zone_type='support' if direction == 'LONG' else 'resistance',
            touches=0, strength='synthetic',
        )

    sl, tp, risk = _compute_sl_tp(avg_cost, zone, direction, params)

    return Trade(
        entry_time=pd.Timestamp.now(tz='UTC'),
        entry_price=avg_cost, direction=direction,
        sl_price=sl, tp_price=tp,
        zone_upper=zone.upper, zone_lower=zone.lower,
        zone_strength=zone.strength, risk=risk,
    )


def _build_trade_from_signal_row(signal_row: dict) -> Trade:
    """Construct the tracked Trade from the original detected signal row."""

    stop_price = float(
        signal_row['submitted_sl_price']
        if signal_row.get('submitted_sl_price') is not None
        else signal_row['sl_price']
    )
    take_profit_price = float(
        signal_row['submitted_tp_price']
        if signal_row.get('submitted_tp_price') is not None
        else signal_row['tp_price']
    )
    entry_price = (
        float(signal_row['opened_price'])
        if signal_row.get('opened_price') is not None
        else float(
            signal_row['submitted_entry_price']
            if signal_row.get('submitted_entry_price') is not None
            else signal_row['entry_price']
        )
    )
    entry_time = (
        pd.Timestamp(signal_row['opened_at'])
        if signal_row.get('opened_at')
        else pd.Timestamp(signal_row['signal_time'])
    )
    planned_entry = float(
        signal_row['submitted_entry_price']
        if signal_row.get('submitted_entry_price') is not None
        else signal_row['entry_price']
    )
    if signal_row['direction'] == 'LONG':
        risk = entry_price - stop_price
        if risk <= 0:
            risk = planned_entry - stop_price
    else:
        risk = stop_price - entry_price
        if risk <= 0:
            risk = stop_price - planned_entry

    return Trade(
        entry_time=entry_time,
        entry_price=entry_price,
        direction=signal_row['direction'],
        sl_price=stop_price,
        tp_price=take_profit_price,
        zone_upper=float(signal_row['zone_upper']),
        zone_lower=float(signal_row['zone_lower']),
        zone_strength=signal_row['zone_strength'],
        risk=risk,
        quality_score=float(signal_row.get('quality_score') or 0.0),
    )


def _resolve_closed_position_details(info: dict) -> tuple[str, float | None, str]:
    """Resolve the most likely close reason/price/source for a disappeared position."""

    signal_id = info.get('signal_id')
    pending_reason = info.get('pending_exit_reason')
    pending_price = info.get('pending_exit_price')
    if not signal_id:
        return pending_reason or 'EXTERNAL_CLOSE', pending_price, 'position_sync'

    signal_row = load_detected_signal(signal_id)
    if signal_row is None:
        return pending_reason or 'EXTERNAL_CLOSE', pending_price, 'position_sync'

    pending_reason = signal_row.get('exit_signal_reason') or pending_reason
    pending_price = (
        float(signal_row['exit_signal_price'])
        if signal_row.get('exit_signal_price') is not None
        else pending_price
    )

    opened_at = signal_row.get('opened_at') or signal_row.get('executed_at') or signal_row.get('signal_time')
    tp_order_id_raw = signal_row.get('take_profit_order_id')
    sl_order_id_raw = signal_row.get('stop_loss_order_id')
    parent_order_id = signal_row.get('order_id')
    tp_order_id = int(tp_order_id_raw) if tp_order_id_raw is not None else None
    sl_order_id = int(sl_order_id_raw) if sl_order_id_raw is not None else None
    child_order_ids = {oid for oid in (tp_order_id, sl_order_id) if oid is not None}

    all_fills = ibkr.fetch_fx_fills(
        child_order_ids if child_order_ids else None,
        pair=info['pair'],
        since=opened_at,
    )
    if all_fills:
        latest_fill = all_fills[-1]
        if tp_order_id is not None and latest_fill.get('order_id') == tp_order_id:
            return 'TP', latest_fill.get('price') or latest_fill.get('avg_price'), 'broker_tp'
        if sl_order_id is not None and latest_fill.get('order_id') == sl_order_id:
            return 'SL', latest_fill.get('price') or latest_fill.get('avg_price'), 'broker_sl'

    completed_orders = ibkr.fetch_completed_fx_orders(
        child_order_ids if child_order_ids else None,
        pair=info['pair'],
    )
    for completed in reversed(completed_orders):
        order_id = completed.get('order_id')
        if tp_order_id is not None and order_id == tp_order_id:
            return 'TP', completed.get('avg_fill_price') or None, 'broker_tp'
        if sl_order_id is not None and order_id == sl_order_id:
            return 'SL', completed.get('avg_fill_price') or None, 'broker_sl'

    if not child_order_ids:
        all_fills = ibkr.fetch_fx_fills(pair=info['pair'], since=opened_at)
    expected_close_side = 'SELL' if info['trade'].direction == 'LONG' else 'BUY'
    manual_fills = [
        fill
        for fill in all_fills
        if (fill.get('side') or '').upper() == expected_close_side
        and fill.get('order_id') != parent_order_id
        and (not child_order_ids or fill.get('order_id') not in child_order_ids)
    ]
    if manual_fills:
        latest_fill = manual_fills[-1]
        return 'MANUAL', latest_fill.get('price') or latest_fill.get('avg_price'), 'broker_fill'

    return pending_reason or 'EXTERNAL_CLOSE', pending_price, 'position_sync'


# ---------------------------------------------------------------------------
# Sync + monitoring
# ---------------------------------------------------------------------------

def sync_positions(
    params: StrategyParams = None,
    zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS,
    on_signal_closed: Callable[[dict], None] | None = None,
) -> Dict[str, dict]:
    """Synchronize DB-tracked trades with live IBKR positions.

    - New positions (in IBKR, not DB) -> build Trade, save to DB
    - Closed positions (in DB, not IBKR) -> remove from DB
    - Returns merged dict of tracked trades
    """
    if params is None:
        params = StrategyParams()

    reconcile_detected_signal_orders()
    db_trades = _load_trades()
    ibkr_positions = ibkr.fetch_positions()

    if not ibkr_positions and not db_trades:
        return {}

    # Build set of current IBKR position keys
    ibkr_by_key = {}
    for pos in ibkr_positions:
        direction = 'LONG' if pos['size'] > 0 else 'SHORT'
        ibkr_by_key[f"{pos['pair']}:{direction}"] = pos

    # Remove trades no longer in IBKR (closed externally)
    for key in list(db_trades.keys()):
        if key not in ibkr_by_key:
            info = db_trades[key]
            print(f"    Position closed externally: {info['pair']} {info['trade'].direction}")
            closed_row = None
            if info.get('signal_id'):
                close_reason, close_price, close_source = _resolve_closed_position_details(info)
            with _tracking_db_transaction() as conn:
                if info.get('signal_id'):
                    closed_row = record_closed_signal_conn(
                        conn,
                        info['signal_id'],
                        close_reason=close_reason,
                        close_price=close_price,
                        close_source=close_source,
                    )
                _remove_trade_conn(conn, info['pair'], info['trade'].direction)
            if closed_row is not None and on_signal_closed is not None:
                on_signal_closed(closed_row)
            del db_trades[key]

    for key, pos in ibkr_by_key.items():
        direction = 'LONG' if pos['size'] > 0 else 'SHORT'
        existing_info = db_trades.get(key)
        is_new_position = existing_info is None
        size_changed = (
            is_new_position
            or abs(float(existing_info.get('ibkr_size') or 0.0) - float(pos['size'])) > 1e-9
            or abs(float(existing_info.get('ibkr_avg_cost') or 0.0) - float(pos['avg_cost'])) > 1e-9
        )
        if is_new_position:
            print(f"    New position detected: {pos['pair']} {direction} "
                  f"@ {pos['avg_cost']:.5f} (size: {pos['size']:.0f})")

        signal_row = None
        trade = existing_info['trade'] if existing_info is not None else None
        signal_id = existing_info.get('signal_id') if existing_info is not None else None
        if is_new_position or size_changed:
            with _tracking_db_transaction() as conn:
                signal_row = claim_signal_for_position_conn(
                    conn,
                    pos['pair'],
                    direction,
                    opened_price=pos['avg_cost'],
                    open_units=pos['size'],
                )
                trade = (
                    _build_trade_from_signal_row(signal_row)
                    if signal_row is not None
                    else _build_trade_from_position(
                        pos['pair'], pos['avg_cost'], direction,
                        params, zone_history_days,
                    )
                )
                if not trade:
                    conn.rollback()
                    continue
                signal_id = signal_row['signal_id'] if signal_row is not None else signal_id
                _save_trade_conn(
                    conn,
                    pos['pair'],
                    trade,
                    pos['avg_cost'],
                    pos['size'],
                    signal_id=signal_id,
                    last_processed_bar_time=(
                        existing_info.get('last_processed_bar_time')
                        if existing_info is not None
                        else None
                    ),
                )
        elif signal_id:
            signal_row = load_detected_signal(signal_id)

        signal_status = signal_row.get('status') if signal_row is not None else (
            existing_info.get('signal_status') if existing_info is not None else None
        )
        bars_monitored = existing_info.get('bars_monitored', 0) if existing_info is not None else 0
        pending_exit_reason = (
            signal_row.get('exit_signal_reason')
            if signal_row is not None and signal_row.get('exit_signal_reason')
            else (existing_info.get('pending_exit_reason') if existing_info is not None else None)
        )
        pending_exit_price = (
            signal_row.get('exit_signal_price')
            if signal_row is not None and signal_row.get('exit_signal_price') is not None
            else (existing_info.get('pending_exit_price') if existing_info is not None else None)
        )
        pending_exit_detected_at = (
            pd.Timestamp(signal_row['exit_signal_at'])
            if signal_row is not None and signal_row.get('exit_signal_at')
            else (existing_info.get('pending_exit_detected_at') if existing_info is not None else None)
        )
        last_processed_bar_time = (
            existing_info.get('last_processed_bar_time')
            if existing_info is not None
            else None
        )
        if trade is None:
            continue
        db_trades[key] = {
            'pair': pos['pair'],
            'trade': trade,
            'bars_monitored': bars_monitored,
            'ibkr_avg_cost': pos['avg_cost'],
            'ibkr_size': pos['size'],
            'signal_id': signal_id,
            'signal_status': signal_status,
            'pending_exit_reason': pending_exit_reason,
            'pending_exit_price': pending_exit_price,
            'pending_exit_detected_at': pending_exit_detected_at,
            'last_processed_bar_time': last_processed_bar_time,
        }

    return db_trades


def _align_timestamp_to_bar(
    value: Optional[pd.Timestamp],
    reference_bar_time: pd.Timestamp,
) -> Optional[pd.Timestamp]:
    """Align a persisted timestamp to the timezone of the fetched hourly bars."""

    if value is None:
        return None

    ts = pd.Timestamp(value)
    if reference_bar_time.tzinfo is not None and ts.tzinfo is None:
        return ts.tz_localize(reference_bar_time.tzinfo)
    if reference_bar_time.tzinfo is None and ts.tzinfo is not None:
        return ts.tz_convert(None)
    return ts


def _tracking_history_days(last_processed_bar_time: Optional[pd.Timestamp]) -> int:
    """Return enough hourly history to cover unseen bars since the last processed bar."""

    if last_processed_bar_time is None:
        return 2

    ts = pd.Timestamp(last_processed_bar_time)
    now = pd.Timestamp.now(tz=ts.tzinfo) if ts.tzinfo is not None else pd.Timestamp.now()
    age_days = max((now - ts).total_seconds(), 0.0) / 86400.0
    return max(2, min(14, int(age_days) + 2))


def _unseen_hourly_bars(
    hourly_df: pd.DataFrame,
    last_processed_bar_time: Optional[pd.Timestamp],
) -> pd.DataFrame:
    """Return the hourly bars that still need exit evaluation."""

    if hourly_df.empty:
        return hourly_df.iloc[0:0]
    if last_processed_bar_time is None:
        return hourly_df.tail(1)

    aligned_last_processed = _align_timestamp_to_bar(last_processed_bar_time, hourly_df.index[-1])
    return hourly_df[hourly_df.index > aligned_last_processed]


def process_hourly_exit_bars(
    info: dict,
    hourly_df: pd.DataFrame,
    params: StrategyParams,
    *,
    count_initial_unseen_bar: bool = False,
    record_exit_callback: Callable[..., None] = record_exit_signal,
) -> dict | None:
    """Process newly completed hourly bars for one tracked position."""

    if hourly_df.empty:
        return None

    pair = info['pair']
    trade = info['trade']
    bars = int(info.get('bars_monitored', 0) or 0)
    last_processed_bar_time = info.get('last_processed_bar_time')
    aligned_last_processed = _align_timestamp_to_bar(last_processed_bar_time, hourly_df.index[-1])
    unseen_bars = _unseen_hourly_bars(hourly_df, aligned_last_processed)
    if unseen_bars.empty:
        return None

    alert_payload = None
    processed_count = 0
    processed_bar_time = aligned_last_processed
    count_offset = 1 if aligned_last_processed is not None or count_initial_unseen_bar else 0

    for idx, (unseen_time, unseen_bar) in enumerate(unseen_bars.iterrows(), start=1):
        bars_held = bars + max(idx - 1 + count_offset, 0)
        unseen_close = float(unseen_bar['Close'])
        result = check_exit(
            trade,
            bar_high=float(unseen_bar['High']),
            bar_low=float(unseen_bar['Low']),
            bar_close=unseen_close,
            bar_time=unseen_time,
            bars_held=bars_held,
            params=params,
            pip=pair_pip(pair),
        )
        if result:
            exit_reason, exit_price = result
            alert_payload = {
                'pair': pair,
                'direction': trade.direction,
                'exit_reason': exit_reason,
                'exit_price': exit_price,
                'entry_price': trade.entry_price,
                'current_price': unseen_close,
                'pnl_pips': calc_pnl_pips(trade, unseen_close, pair_pip(pair), params),
                'bars_monitored': bars_held,
            }
            processed_count = max(idx - 1 + count_offset, 0)
            processed_bar_time = unseen_time
            break
        processed_count = max(idx - 1 + count_offset, 0)
        processed_bar_time = unseen_time

    info['bars_monitored'] = bars + processed_count
    info['last_processed_bar_time'] = processed_bar_time
    if processed_bar_time is not None:
        _save_bar_tracking(pair, trade.direction, info['bars_monitored'], processed_bar_time)

    if alert_payload:
        info['pending_exit_reason'] = alert_payload['exit_reason']
        info['pending_exit_price'] = alert_payload['exit_price']
        info['pending_exit_detected_at'] = processed_bar_time
        if info.get('signal_id'):
            record_exit_callback(
                info['signal_id'],
                exit_reason=alert_payload['exit_reason'],
                exit_price=alert_payload['exit_price'],
            )

    return alert_payload


def check_position_exits(
    tracked: Dict[str, dict],
    params: StrategyParams = None,
    hourly_data_cache: Optional[Dict[str, pd.DataFrame]] = None,
) -> tuple:
    """Check each tracked trade for exit signals.

    Returns (alerts, snapshots) where:
    - alerts: list of exit alert dicts
    - snapshots: dict of key -> {current_price, pnl_pips} for display
    """
    if params is None:
        params = StrategyParams()

    alerts = []
    snapshots = {}

    for key, info in tracked.items():
        pair = info['pair']
        trade = info['trade']
        bars = info['bars_monitored']
        last_processed_bar_time = info.get('last_processed_bar_time')

        ticker = pair_ticker(pair)
        if not ticker:
            continue

        if hourly_data_cache is not None and ticker in hourly_data_cache:
            hourly_df = hourly_data_cache[ticker]
        else:
            hourly_df = fetch_hourly_data(ticker, days=_tracking_history_days(last_processed_bar_time))
            if hourly_data_cache is not None:
                hourly_data_cache[ticker] = hourly_df
        if hourly_df.empty:
            continue

        last_bar = hourly_df.iloc[-1]
        bar_time = hourly_df.index[-1]
        aligned_last_processed = _align_timestamp_to_bar(last_processed_bar_time, bar_time)
        unseen_bars = _unseen_hourly_bars(hourly_df, aligned_last_processed)
        current_price = float(last_bar['Close'])
        pnl_pips = calc_pnl_pips(trade, current_price, pair_pip(pair), params)

        snapshots[key] = {
            'current_price': current_price,
            'pnl_pips': pnl_pips,
        }

        if not unseen_bars.empty:
            alert_payload = process_hourly_exit_bars(
                info,
                hourly_df,
                params,
                count_initial_unseen_bar=False,
            )
            if alert_payload:
                alerts.append(alert_payload)

    return alerts, snapshots


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def format_positions_table(
    tracked: Dict[str, dict],
    snapshots: Dict[str, dict],
    alerts: List[dict],
) -> str:
    """Format tracked positions as a readable table.

    Uses pre-fetched snapshots from check_position_exits() to avoid
    redundant data fetching.
    """
    if not tracked:
        return ""

    # Build alert lookup for status column
    alert_reasons = {}
    for a in alerts:
        alert_reasons[f"{a['pair']}:{a['direction']}"] = a['exit_reason']

    lines = [
        "",
        "=" * 110,
        "  OPEN POSITIONS (from TWS)",
        "=" * 110,
        f"  {'PAIR':<10} {'DIR':>5} {'SIZE':>10} {'ENTRY':>12} "
        f"{'SL':>12} {'TP':>12} {'BARS':>6} {'P&L':>10} {'STATUS':>14}",
        "-" * 110,
    ]

    for key in sorted(tracked.keys()):
        info = tracked[key]
        pair = info['pair']
        trade = info['trade']
        bars = info['bars_monitored']
        d = pair_decimals(pair)

        snap = snapshots.get(key)
        pnl_str = f"{snap['pnl_pips']:+.1f}p" if snap else "?"

        status = alert_reasons.get(key)
        status_str = f">>> {status}" if status else "OK"

        lines.append(
            f"  {pair:<10} {trade.direction:>5} {format_size(info.get('ibkr_size', 0)):>10} "
            f"{trade.entry_price:>{12}.{d}f} {trade.sl_price:>{12}.{d}f} "
            f"{trade.tp_price:>{12}.{d}f} {bars:>6} {pnl_str:>10} {status_str:>14}"
        )

    lines.append("=" * 110)
    return "\n".join(lines)


def format_alerts(alerts: List[dict]) -> str:
    """Format exit alerts for display."""
    if not alerts:
        return ""

    lines = [""]
    for a in alerts:
        d = pair_decimals(a['pair'])
        lines.append(
            f"  !!! EXIT ALERT: {a['pair']} {a['direction']} - {a['exit_reason']} "
            f"@ {a['current_price']:.{d}f} "
            f"(entry: {a['entry_price']:.{d}f}, {a['pnl_pips']:+.1f} pips, "
            f"{a['bars_monitored']} bars)"
        )

    return "\n".join(lines)
