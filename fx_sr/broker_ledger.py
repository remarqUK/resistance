"""IBKR broker execution/order ledger reconciliation."""

from __future__ import annotations

from datetime import datetime as dt_datetime
from typing import Iterable

import pandas as pd

from . import ibkr
from .db import _connect, _normalize_ts, get_setting, set_setting
from . import signal_store


ACTIVE_SIGNAL_STATUSES = {
    'SUBMITTED',
    'PRESUBMITTED',
    'FILLED',
    'PARTIAL',
    'OPEN',
    'EXIT_SIGNAL',
}
BROKER_TERMINAL_STATUSES = {
    'CANCELLED',
    'APICANCELLED',
    'INACTIVE',
    'REJECTED',
}
ENTRY_ROLE = 'ENTRY'
EXIT_ROLES = {'TAKE_PROFIT', 'STOP_LOSS', 'CLOSE'}
LAST_EXECUTION_CURSOR_KEY = 'broker_ledger.last_execution_time'
NORMAL_LOOKBACK_MINUTES = 360
CURSOR_OVERLAP_MINUTES = 5


def _to_utc_ts(value) -> pd.Timestamp | None:
    if value is None:
        return None
    try:
        ts = pd.Timestamp(value)
    except Exception:
        return None
    if ts.tzinfo is None:
        return ts.tz_localize('UTC')
    return ts.tz_convert('UTC')


def _nullable_int(value) -> int | None:
    if value in (None, ''):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _nullable_float(value) -> float | None:
    if value in (None, ''):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_broker_side(side: str | None) -> str:
    raw = str(side or '').strip().upper()
    if raw == 'BUY':
        return 'BOT'
    if raw == 'SELL':
        return 'SLD'
    return raw


def _parse_signal_order_ref(order_ref: str | None) -> dict | None:
    raw = str(order_ref or '').strip()
    parts = raw.split(':')
    if len(parts) < 4 or parts[0] != 'fxsr':
        return None
    pair = parts[1].upper()
    direction = parts[2].upper()
    stamp = parts[3]
    if len(pair) != 6 or direction not in {'LONG', 'SHORT'}:
        return None
    try:
        signal_time = pd.Timestamp(dt_datetime.strptime(stamp, '%Y%m%d%H%M%S'), tz='UTC')
    except Exception:
        return None
    suffix = ':'.join(parts[4:]) if len(parts) > 4 else ''
    return {
        'pair': pair,
        'direction': direction,
        'signal_time': signal_time,
        'parent_ref': f'fxsr:{pair}:{direction}:{stamp}',
        'suffix': suffix,
    }


def _broker_role_from_ref(order_ref: str | None, *, default: str = 'UNKNOWN') -> str:
    parsed = _parse_signal_order_ref(order_ref)
    suffix = (parsed or {}).get('suffix') or ''
    if not parsed:
        return default
    if not suffix:
        return ENTRY_ROLE
    suffix = suffix.lower()
    if suffix == 'tp' or ':tp' in suffix:
        return 'TAKE_PROFIT'
    if suffix == 'sl' or ':sl' in suffix:
        return 'STOP_LOSS'
    if 'close' in suffix or 'liquidate' in suffix or 'recovery' in suffix:
        return 'CLOSE'
    if 'rebracket' in suffix:
        return 'PROTECTION'
    return default


def _signal_lookup_rows_conn(conn, *, signal_ids: Iterable[str] | None = None) -> list[dict]:
    params: list[object] = []
    query = """
        SELECT *
        FROM detected_signal
        WHERE closed_at IS NULL
          AND (
                order_id IS NOT NULL
                OR take_profit_order_id IS NOT NULL
                OR stop_loss_order_id IS NOT NULL
                OR status IN ('SUBMITTED', 'PRESUBMITTED', 'FILLED', 'PARTIAL', 'OPEN', 'EXIT_SIGNAL')
                OR open_units > 0
                OR opened_price IS NOT NULL
          )
    """
    if signal_ids is not None:
        signal_ids = [str(signal_id) for signal_id in signal_ids if signal_id]
        if not signal_ids:
            return []
        query += " AND signal_id IN ({})".format(",".join(["%s"] * len(signal_ids)))
        params.extend(signal_ids)
    cursor = conn.execute(query, params)
    return [signal_store.row_to_dict(cursor, row) for row in cursor.fetchall()]


def _broker_lookup_maps(rows: list[dict]) -> dict:
    parent_by_order: dict[int, dict] = {}
    child_by_order: dict[int, tuple[dict, str]] = {}
    by_ref: dict[str, dict] = {}
    by_parent_ref: dict[str, dict] = {}

    for row in rows:
        parent_order_id = _nullable_int(row.get('order_id'))
        if parent_order_id is not None:
            parent_by_order[parent_order_id] = row
        for col, role in (
            ('take_profit_order_id', 'TAKE_PROFIT'),
            ('stop_loss_order_id', 'STOP_LOSS'),
        ):
            order_id = _nullable_int(row.get(col))
            if order_id is not None:
                child_by_order[order_id] = (row, role)
        order_ref = signal_store.signal_order_ref(row)
        if order_ref:
            by_ref[order_ref] = row
            by_parent_ref[order_ref] = row
            by_ref[f'{order_ref}:tp'] = row
            by_ref[f'{order_ref}:sl'] = row
    return {
        'parent_by_order': parent_by_order,
        'child_by_order': child_by_order,
        'by_ref': by_ref,
        'by_parent_ref': by_parent_ref,
    }


def _link_fill_to_signal(fill: dict, maps: dict) -> tuple[dict | None, str]:
    order_id = _nullable_int(fill.get('order_id'))
    if order_id is not None:
        parent = maps['parent_by_order'].get(order_id)
        if parent is not None:
            return parent, ENTRY_ROLE
        child = maps['child_by_order'].get(order_id)
        if child is not None:
            return child

    order_ref = str(fill.get('order_ref') or '').strip()
    role = _broker_role_from_ref(order_ref)
    row = maps['by_ref'].get(order_ref)
    if row is not None:
        return row, role
    parsed = _parse_signal_order_ref(order_ref)
    if parsed:
        row = maps['by_parent_ref'].get(parsed['parent_ref'])
        if row is not None:
            return row, role
    return None, role


def _link_order_snapshot_to_signal(snapshot: dict, maps: dict) -> tuple[dict | None, str]:
    order_id = _nullable_int(snapshot.get('order_id'))
    if order_id is not None:
        parent = maps['parent_by_order'].get(order_id)
        if parent is not None:
            return parent, ENTRY_ROLE
        child = maps['child_by_order'].get(order_id)
        if child is not None:
            return child

    order_ref = str(snapshot.get('order_ref') or '').strip()
    role = _broker_role_from_ref(order_ref)
    row = maps['by_ref'].get(order_ref)
    if row is not None:
        return row, role
    parsed = _parse_signal_order_ref(order_ref)
    if parsed:
        row = maps['by_parent_ref'].get(parsed['parent_ref'])
        if row is not None:
            return row, role
    return None, role


def _upsert_broker_execution_conn(conn, fill: dict, *, signal_row: dict | None, role: str, recorded_at: str) -> bool:
    exec_id = str(fill.get('exec_id') or '').strip()
    if not exec_id:
        return False
    fill_units = signal_store.normalize_units(fill.get('shares'))
    if fill_units <= 0:
        return False
    fill_time = fill.get('time')
    normalized_fill_time = _normalize_ts(pd.Timestamp(fill_time)) if fill_time is not None else None
    parsed = _parse_signal_order_ref(fill.get('order_ref'))
    pair = (
        str(fill.get('pair') or '').upper()
        or (signal_row or {}).get('pair')
        or (parsed or {}).get('pair')
    )
    if not pair:
        return False
    direction = (signal_row or {}).get('direction') or (parsed or {}).get('direction')
    conn.execute(
        """
        INSERT INTO broker_execution (
            exec_id, signal_id, pair, direction, role, side, order_id, perm_id,
            order_ref, fill_time, fill_price, fill_units, cum_qty, avg_price,
            commission, commission_currency, realized_pnl, recorded_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (exec_id) DO UPDATE SET
            signal_id = COALESCE(EXCLUDED.signal_id, broker_execution.signal_id),
            pair = EXCLUDED.pair,
            direction = COALESCE(EXCLUDED.direction, broker_execution.direction),
            role = COALESCE(NULLIF(EXCLUDED.role, 'UNKNOWN'), broker_execution.role, EXCLUDED.role),
            side = EXCLUDED.side,
            order_id = COALESCE(EXCLUDED.order_id, broker_execution.order_id),
            perm_id = COALESCE(EXCLUDED.perm_id, broker_execution.perm_id),
            order_ref = COALESCE(EXCLUDED.order_ref, broker_execution.order_ref),
            fill_time = COALESCE(EXCLUDED.fill_time, broker_execution.fill_time),
            fill_price = EXCLUDED.fill_price,
            fill_units = EXCLUDED.fill_units,
            cum_qty = EXCLUDED.cum_qty,
            avg_price = EXCLUDED.avg_price,
            commission = EXCLUDED.commission,
            commission_currency = EXCLUDED.commission_currency,
            realized_pnl = EXCLUDED.realized_pnl,
            updated_at = EXCLUDED.updated_at
        """,
        (
            exec_id,
            (signal_row or {}).get('signal_id'),
            pair,
            direction,
            role,
            _normalize_broker_side(fill.get('side')),
            _nullable_int(fill.get('order_id')),
            _nullable_int(fill.get('perm_id')),
            str(fill.get('order_ref') or '') or None,
            normalized_fill_time,
            float(fill.get('price') or fill.get('avg_price') or 0.0),
            fill_units,
            _nullable_float(fill.get('cum_qty')),
            _nullable_float(fill.get('avg_price')),
            _nullable_float(fill.get('commission')),
            fill.get('commission_currency') or None,
            _nullable_float(fill.get('realized_pnl')),
            recorded_at,
            recorded_at,
        ),
    )
    return conn.total_changes > 0


def _upsert_broker_order_snapshot_conn(
    conn,
    snapshot: dict,
    *,
    signal_row: dict | None,
    role: str,
    seen_at: str,
) -> bool:
    order_id = _nullable_int(snapshot.get('order_id'))
    if order_id is None:
        return False
    parsed = _parse_signal_order_ref(snapshot.get('order_ref'))
    pair = (
        str(snapshot.get('pair') or '').upper()
        or (signal_row or {}).get('pair')
        or (parsed or {}).get('pair')
    )
    if not pair:
        return False
    direction = (signal_row or {}).get('direction') or (parsed or {}).get('direction')
    conn.execute(
        """
        INSERT INTO broker_order_snapshot (
            order_id, signal_id, pair, direction, role, parent_id, perm_id,
            order_ref, order_type, action, status, total_units, filled_units,
            remaining_units, avg_fill_price, last_seen_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO UPDATE SET
            signal_id = COALESCE(EXCLUDED.signal_id, broker_order_snapshot.signal_id),
            pair = EXCLUDED.pair,
            direction = COALESCE(EXCLUDED.direction, broker_order_snapshot.direction),
            role = COALESCE(NULLIF(EXCLUDED.role, 'UNKNOWN'), broker_order_snapshot.role, EXCLUDED.role),
            parent_id = COALESCE(EXCLUDED.parent_id, broker_order_snapshot.parent_id),
            perm_id = COALESCE(EXCLUDED.perm_id, broker_order_snapshot.perm_id),
            order_ref = COALESCE(EXCLUDED.order_ref, broker_order_snapshot.order_ref),
            order_type = EXCLUDED.order_type,
            action = EXCLUDED.action,
            status = EXCLUDED.status,
            total_units = EXCLUDED.total_units,
            filled_units = EXCLUDED.filled_units,
            remaining_units = EXCLUDED.remaining_units,
            avg_fill_price = EXCLUDED.avg_fill_price,
            last_seen_at = EXCLUDED.last_seen_at,
            updated_at = EXCLUDED.updated_at
        """,
        (
            order_id,
            (signal_row or {}).get('signal_id'),
            pair,
            direction,
            role,
            _nullable_int(snapshot.get('parent_id')),
            _nullable_int(snapshot.get('perm_id')),
            str(snapshot.get('order_ref') or '') or None,
            snapshot.get('order_type') or None,
            snapshot.get('action') or None,
            snapshot.get('status') or None,
            _nullable_float(snapshot.get('total_units')),
            _nullable_float(snapshot.get('filled_units')),
            _nullable_float(snapshot.get('remaining_units')),
            _nullable_float(snapshot.get('avg_fill_price')),
            seen_at,
            seen_at,
        ),
    )
    return conn.total_changes > 0


def _reconciliation_since(rows: list[dict], signal_ids: Iterable[str] | None, db_path: str, now: pd.Timestamp) -> pd.Timestamp | None:
    if signal_ids is not None:
        signal_times = [
            ts for ts in (_to_utc_ts(row.get('signal_time')) for row in rows)
            if ts is not None
        ]
        if signal_times:
            return min(signal_times) - pd.Timedelta(minutes=CURSOR_OVERLAP_MINUTES)
        return None

    setting = get_setting(LAST_EXECUTION_CURSOR_KEY, db_path=db_path)
    last_ts = _to_utc_ts((setting or {}).get('value_ts'))
    if last_ts is None:
        return None
    overlapped = last_ts - pd.Timedelta(minutes=CURSOR_OVERLAP_MINUTES)
    lower_bound = now - pd.Timedelta(minutes=NORMAL_LOOKBACK_MINUTES)
    return max(overlapped, lower_bound)


def _fill_signed_units(fill: dict, signal_direction: str) -> float:
    units = float(fill.get('fill_units') or 0.0)
    side = _normalize_broker_side(fill.get('side'))
    role = str(fill.get('role') or '').upper()
    direction = str(signal_direction or '').upper()
    if side == 'BOT':
        return units
    if side == 'SLD':
        return -units
    if role == ENTRY_ROLE:
        return units if direction == 'LONG' else -units
    if role in EXIT_ROLES:
        return -units if direction == 'LONG' else units
    return 0.0


def _signal_broker_fill_summary_conn(conn, signal_id: str, signal_direction: str) -> dict:
    return _signal_broker_fill_summaries_conn(
        conn,
        signal_ids=[signal_id],
        signal_directions={signal_id: signal_direction},
    ).get(signal_id, _empty_signal_broker_fill_summary())


def _empty_signal_broker_fill_summary() -> dict:
    return {
        'fills': [],
        'net_units': 0.0,
        'entry_units': 0.0,
        'entry_avg_price': None,
        'entry_count': 0,
        'exit_units': 0.0,
        'first_entry_at': None,
        'last_fill_at': None,
        'latest_exit': None,
    }


def _signal_broker_fill_summaries_conn(
    conn,
    *,
    signal_ids: Iterable[str],
    signal_directions: dict[str, str],
) -> dict[str, dict]:
    signal_ids = [str(signal_id) for signal_id in signal_ids if signal_id]
    if not signal_ids:
        return {}

    placeholders = ",".join(["%s"] * len(signal_ids))
    cursor = conn.execute(
        f"""
        SELECT *
        FROM broker_execution
        WHERE signal_id IN ({placeholders})
        ORDER BY signal_id ASC, fill_time ASC, recorded_at ASC, exec_id ASC
        """,
        signal_ids,
    )
    grouped_fills: dict[str, list[dict]] = {signal_id: [] for signal_id in signal_ids}
    for row in cursor.fetchall():
        fill = signal_store.row_to_dict(cursor, row)
        fill_signal_id = str(fill.get('signal_id') or '')
        if fill_signal_id:
            grouped_fills.setdefault(fill_signal_id, []).append(fill)

    summaries: dict[str, dict] = {}
    for signal_id in signal_ids:
        fills = grouped_fills.get(signal_id, [])
        signal_direction = signal_directions.get(signal_id, '')
        net_units = 0.0
        entry_units = 0.0
        entry_weighted_sum = 0.0
        entry_count = 0
        exit_units = 0.0
        first_entry_at = None
        last_fill_at = None
        latest_exit = None
        for fill in fills:
            signed_units = _fill_signed_units(fill, signal_direction)
            net_units += signed_units
            role = str(fill.get('role') or '').upper()
            fill_units = float(fill.get('fill_units') or 0.0)
            fill_time = fill.get('fill_time')
            if fill_time is not None:
                last_fill_at = fill_time
            if role == ENTRY_ROLE:
                entry_units += fill_units
                entry_weighted_sum += fill_units * float(fill.get('fill_price') or 0.0)
                entry_count += 1
                if first_entry_at is None:
                    first_entry_at = fill_time
            elif role in EXIT_ROLES or (
                role == 'UNKNOWN'
                and signed_units
                and (signed_units > 0) != (str(signal_direction).upper() == 'LONG')
            ):
                exit_units += abs(signed_units or fill_units)
                latest_exit = fill
        entry_avg = entry_weighted_sum / entry_units if entry_units > 0 else None
        summaries[signal_id] = {
            'fills': fills,
            'net_units': net_units,
            'entry_units': entry_units,
            'entry_avg_price': entry_avg,
            'entry_count': entry_count,
            'exit_units': exit_units,
            'first_entry_at': first_entry_at,
            'last_fill_at': last_fill_at,
            'latest_exit': latest_exit,
        }
    return summaries


def _load_broker_order_snapshots_conn(conn, *, order_ids: Iterable[int]) -> dict[int, dict]:
    normalized_ids = sorted({int(order_id) for order_id in order_ids if order_id is not None})
    if not normalized_ids:
        return {}

    placeholders = ",".join(["%s"] * len(normalized_ids))
    cursor = conn.execute(
        f"""
        SELECT *
        FROM broker_order_snapshot
        WHERE order_id IN ({placeholders})
        """,
        normalized_ids,
    )
    snapshots: dict[int, dict] = {}
    for row in cursor.fetchall():
        row_dict = signal_store.row_to_dict(cursor, row)
        if row_dict.get('order_id') is not None:
            snapshots[int(row_dict['order_id'])] = row_dict
    return snapshots


def _build_signal_fill_summaries(rows: list[dict], conn) -> dict[str, dict]:
    signal_directions = {
        str(row['signal_id']): str(row.get('direction') or '')
        for row in rows
        if row.get('signal_id')
    }
    return _signal_broker_fill_summaries_conn(
        conn,
        signal_ids=signal_directions.keys(),
        signal_directions=signal_directions,
    )


def reconcile_broker_ledger(
    *,
    signal_ids: Iterable[str] | None = None,
    db_path: str | None = None,
    live_position_keys: set[str] | None = None,
) -> dict:
    """Fetch IBKR broker state and upsert the local broker ledger only."""

    del live_position_keys
    db_path = signal_store.ensure_signal_tables(db_path)
    now_ts = pd.Timestamp.now('UTC')
    now = _normalize_ts(now_ts)
    conn = _connect(db_path)
    try:
        rows = _signal_lookup_rows_conn(conn, signal_ids=signal_ids)
        if not rows:
            return {'fills': 0, 'orders': 0, 'since': None}
        maps = _broker_lookup_maps(rows)
        since = _reconciliation_since(rows, signal_ids, db_path, now_ts)

        order_ids = {
            int(row['order_id'])
            for row in rows
            if row.get('order_id') is not None
        }
        child_order_ids: set[int] = set()
        for row in rows:
            for key in ('take_profit_order_id', 'stop_loss_order_id'):
                raw = row.get(key)
                if raw is not None:
                    child_order_ids.add(int(raw))
        all_fetch_ids = order_ids | child_order_ids

        fills = ibkr.fetch_fx_fills(since=since)
        latest_fill_ts = None
        for fill in fills:
            fill_ts = _to_utc_ts(fill.get('time'))
            if fill_ts is not None and (latest_fill_ts is None or fill_ts > latest_fill_ts):
                latest_fill_ts = fill_ts
            signal_row, role = _link_fill_to_signal(fill, maps)
            _upsert_broker_execution_conn(
                conn,
                fill,
                signal_row=signal_row,
                role=role,
                recorded_at=now,
            )
            if signal_row is not None and role == ENTRY_ROLE:
                signal_store.record_detected_signal_fill_conn(conn, signal_row, fill, recorded_at=now)

        order_snapshots = ibkr.fetch_fx_order_statuses(
            order_ids=all_fetch_ids if all_fetch_ids else None
        )
        for snapshot in order_snapshots:
            signal_row, role = _link_order_snapshot_to_signal(snapshot, maps)
            _upsert_broker_order_snapshot_conn(
                conn,
                snapshot,
                signal_row=signal_row,
                role=role,
                seen_at=now,
            )

        if child_order_ids:
            for completed in ibkr.fetch_completed_fx_orders(order_ids=child_order_ids):
                signal_row, role = _link_order_snapshot_to_signal(completed, maps)
                _upsert_broker_order_snapshot_conn(
                    conn,
                    completed,
                    signal_row=signal_row,
                    role=role,
                    seen_at=now,
                )

        conn.commit()
    finally:
        conn.close()

    if signal_ids is None:
        set_setting(
            LAST_EXECUTION_CURSOR_KEY,
            value_ts=latest_fill_ts or now_ts,
            db_path=db_path,
        )
    return {
        'fills': len(fills),
        'orders': len(order_snapshots),
        'since': since.isoformat() if since is not None else None,
    }


def reconcile_detected_signal_orders(
    *,
    signal_ids: Iterable[str] | None = None,
    db_path: str | None = None,
    live_position_keys: set[str] | None = None,
) -> list[dict]:
    """Reconcile detected_signal lifecycle from the broker ledger."""

    reconcile_broker_ledger(
        signal_ids=signal_ids,
        db_path=db_path,
        live_position_keys=live_position_keys,
    )
    db_path = signal_store.ensure_signal_tables(db_path)
    now = _normalize_ts(pd.Timestamp.now('UTC'))
    conn = _connect(db_path)
    try:
        rows = _signal_lookup_rows_conn(conn, signal_ids=signal_ids)
        summaries = _build_signal_fill_summaries(rows, conn)
        broker_snapshots_by_order = _load_broker_order_snapshots_conn(
            conn,
            order_ids=[
                int(row['order_id'])
                for row in rows
                if row.get('order_id') is not None
            ],
        )
        updated_rows: list[dict] = []
        for existing in rows:
            direction = str(existing.get('direction') or '').upper()
            signal_id = str(existing['signal_id'])
            summary = summaries.get(signal_id, _empty_signal_broker_fill_summary())
            broker_snapshot = {}
            if existing.get('order_id') is not None:
                broker_snapshot = broker_snapshots_by_order.get(int(existing['order_id']), {})

            broker_order_status = broker_snapshot.get('status') or existing.get('broker_order_status')
            planned_units = signal_store.normalize_units(existing.get('planned_units'))
            entry_units = int(round(abs(summary['entry_units'])))
            net_units = float(summary['net_units'] or 0.0)
            open_units = int(round(abs(net_units))) if abs(net_units) >= 1.0 else 0
            if entry_units <= 0:
                open_units = max(
                    signal_store.normalize_units(existing.get('open_units')),
                    signal_store.normalize_units(broker_snapshot.get('filled_units')),
                )
                entry_units = open_units

            opened_price = summary['entry_avg_price']
            if opened_price is None and existing.get('opened_price') is not None:
                opened_price = float(existing['opened_price'])
            opened_at = summary['first_entry_at'] or existing.get('opened_at')
            last_fill_at = summary['last_fill_at'] or existing.get('last_fill_at')
            fill_count = max(int(summary['entry_count'] or 0), int(existing.get('fill_count') or 0))
            if planned_units > 0 and entry_units < planned_units:
                remaining_units = max(planned_units - entry_units, 0)
            else:
                remaining_units = 0

            status = existing.get('status') or 'SUBMITTED'
            note = existing.get('note')
            closed_at = existing.get('closed_at')
            closed_price = existing.get('closed_price')
            close_reason = existing.get('close_reason')
            close_source = existing.get('close_source')
            pnl_pips = existing.get('pnl_pips')
            position_key = f"{existing['pair']}:{existing['direction']}"
            position_still_live = live_position_keys is not None and position_key in live_position_keys

            latest_exit = summary['latest_exit']
            if (
                entry_units > 0
                and open_units <= 0
                and summary['exit_units'] > 0
                and not position_still_live
            ):
                status = 'CLOSED'
                closed_price = float(latest_exit.get('fill_price') or latest_exit.get('avg_price') or 0.0)
                closed_at = _normalize_ts(pd.Timestamp(latest_exit.get('fill_time') or pd.Timestamp.now('UTC')))
                role = str(latest_exit.get('role') or '').upper()
                if role == 'TAKE_PROFIT':
                    close_reason = 'TP'
                    close_source = 'broker_tp'
                elif role == 'STOP_LOSS':
                    close_reason = 'SL'
                    close_source = 'broker_sl'
                else:
                    close_reason = 'MANUAL'
                    close_source = 'broker_fill'
                if opened_price is not None and closed_price:
                    pip = signal_store.pair_pip(existing['pair'])
                    if direction == 'LONG':
                        pnl_pips = (closed_price - float(opened_price)) / pip
                    else:
                        pnl_pips = (float(opened_price) - closed_price) / pip
                note = f"broker {close_reason} filled @ {closed_price:.5f}"
            elif open_units > 0:
                if planned_units > 0 and entry_units < planned_units:
                    status = 'PARTIAL'
                    note = f"partial fill {entry_units:,}/{planned_units:,}"
                else:
                    status = 'OPEN' if status != 'EXIT_SIGNAL' else 'EXIT_SIGNAL'
                    if summary['exit_units'] > 0 and entry_units > open_units:
                        note = f"open {open_units:,}/{entry_units:,}"
                    elif planned_units > 0:
                        note = f"filled {entry_units:,}/{planned_units:,}"
                    else:
                        note = f"open {open_units:,}"
            elif broker_order_status:
                status = signal_store.derive_signal_execution_status(
                    existing,
                    open_units=open_units,
                    broker_order_status=broker_order_status,
                    order_found=True,
                )
                note = f"broker status {broker_order_status}"

            merged = signal_store.merge_row(
                existing,
                status=status,
                transacted=1 if entry_units > 0 or broker_order_status else existing.get('transacted'),
                opened_at=opened_at,
                opened_price=float(opened_price) if opened_price is not None else existing.get('opened_price'),
                open_units=open_units if entry_units > 0 else existing.get('open_units'),
                remaining_units=remaining_units,
                fill_count=fill_count,
                last_fill_at=last_fill_at,
                broker_order_status=broker_order_status,
                note=note,
                closed_at=closed_at,
                closed_price=closed_price,
                close_reason=close_reason,
                close_source=close_source,
                pnl_pips=pnl_pips,
                last_updated_at=now,
            )
            signal_store.replace_detected_signal_conn(conn, merged)
            updated_rows.append(merged)
        conn.commit()
        return updated_rows
    finally:
        conn.close()


def has_active_broker_activity_for_order_ref(
    order_ref: str,
    *,
    db_path: str | None = None,
    check_broker: bool = True,
) -> bool:
    """Return True if this strategy orderRef already exists locally or at IBKR."""

    parsed = _parse_signal_order_ref(order_ref)
    if parsed is None:
        return False
    db_path = signal_store.ensure_signal_tables(db_path)
    parent_ref = parsed['parent_ref']
    refs = {parent_ref, f'{parent_ref}:tp', f'{parent_ref}:sl'}
    signal_time = _normalize_ts(parsed['signal_time'])
    conn = _connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT 1
            FROM broker_execution
            WHERE order_ref=%s AND role='ENTRY'
            LIMIT 1
            """,
            (parent_ref,),
        ).fetchone()
        if row is not None:
            return True

        row = conn.execute(
            """
            SELECT 1
            FROM broker_order_snapshot
            WHERE order_ref IN (%s, %s, %s)
              AND UPPER(COALESCE(status, '')) NOT IN ('CANCELLED', 'APICANCELLED', 'INACTIVE', 'REJECTED')
            LIMIT 1
            """,
            (parent_ref, f'{parent_ref}:tp', f'{parent_ref}:sl'),
        ).fetchone()
        if row is not None:
            return True

        row = conn.execute(
            """
            SELECT 1
            FROM detected_signal
            WHERE pair=%s AND direction=%s AND signal_time=%s
              AND closed_at IS NULL
              AND (
                    order_id IS NOT NULL
                    OR open_units > 0
                    OR opened_price IS NOT NULL
                    OR (
                        transacted=1
                        AND status IN ('SUBMITTED', 'PRESUBMITTED', 'FILLED', 'PARTIAL', 'OPEN', 'EXIT_SIGNAL')
                    )
              )
            LIMIT 1
            """,
            (parsed['pair'], parsed['direction'], signal_time),
        ).fetchone()
        if row is not None:
            return True
    finally:
        conn.close()

    if not check_broker:
        return False

    for snapshot in ibkr.fetch_fx_order_statuses():
        if str(snapshot.get('order_ref') or '') not in refs:
            continue
        status = str(snapshot.get('status') or '').upper()
        if status not in BROKER_TERMINAL_STATUSES:
            return True

    since = parsed['signal_time'] - pd.Timedelta(minutes=CURSOR_OVERLAP_MINUTES)
    for fill in ibkr.fetch_fx_fills(pair=parsed['pair'], since=since):
        if str(fill.get('order_ref') or '') == parent_ref:
            return True
    return False


def load_open_broker_execution_positions(*, db_path: str | None = None) -> list[dict]:
    """Return net open exposure derived from all linked broker executions."""

    db_path = signal_store.ensure_signal_tables(db_path)
    conn = _connect(db_path)
    try:
        cursor = conn.execute(
            """
            SELECT signal_id, pair, direction, status, opened_at, signal_time
            FROM detected_signal
            WHERE closed_at IS NULL
              AND status IN ('SUBMITTED', 'PRESUBMITTED', 'FILLED', 'PARTIAL', 'OPEN', 'EXIT_SIGNAL')
            ORDER BY COALESCE(opened_at, signal_time) DESC
            """
        )
        rows = [signal_store.row_to_dict(cursor, row) for row in cursor.fetchall()]
        summaries = _build_signal_fill_summaries(rows, conn)
        positions: list[dict] = []
        for row in rows:
            summary = summaries.get(str(row['signal_id']), _empty_signal_broker_fill_summary())
            net_units = float(summary['net_units'] or 0.0)
            if abs(net_units) < 1.0 or summary['entry_units'] <= 0:
                continue
            avg_cost = summary['entry_avg_price']
            if avg_cost is None:
                continue
            direction = 'LONG' if net_units > 0 else 'SHORT'
            positions.append({
                'pair': str(row.get('pair') or '').upper(),
                'direction': direction,
                'size': float(net_units),
                'avg_cost': float(avg_cost),
                'source': 'broker_execution',
                'position_source': 'broker_execution',
                'signal_id': row.get('signal_id'),
                'broker_fill_count': len(summary['fills']),
                'last_broker_fill_at': summary['last_fill_at'],
            })
        return positions
    finally:
        conn.close()


def _broker_side_sign(side: str | None) -> int:
    normalized = _normalize_broker_side(side)
    if normalized == 'BOT':
        return 1
    if normalized == 'SLD':
        return -1
    return 0


def _is_broker_liquidation_ref(order_ref: str | None) -> bool:
    return str(order_ref or '').strip().lower().startswith('fxsr:liquidate:')


def _ts_in_range(
    ts,
    *,
    closed_after=None,
    closed_before=None,
) -> bool:
    if ts is None:
        return False
    value = pd.Timestamp(ts)
    def _bound(bound):
        bound_ts = pd.Timestamp(bound)
        if value.tzinfo is not None and bound_ts.tzinfo is None:
            return bound_ts.tz_localize(value.tzinfo)
        if value.tzinfo is None and bound_ts.tzinfo is not None:
            return bound_ts.tz_localize(None)
        return bound_ts

    if closed_after is not None and value < _bound(closed_after):
        return False
    if closed_before is not None and value >= _bound(closed_before):
        return False
    return True


def load_unmatched_broker_liquidation_trades_conn(
    conn,
    *,
    pair: str | None = None,
    closed_after=None,
    closed_before=None,
    limit: int | None = None,
) -> list[dict]:
    """Return closed trade rows for broker liquidations not linked to a signal.

    These rows cover emergency/manual liquidations where the broker fill exists
    but no detected_signal row can be closed.  They are derived from unlinked
    broker_execution fills so the UI and daily P/L do not hide real broker P/L.
    """

    params: list[object] = []
    filters = [
        "signal_id IS NULL",
        "side IN ('BOT', 'SLD')",
    ]
    if pair:
        filters.append("pair=%s")
        params.append(str(pair).upper())
    if closed_before is not None:
        filters.append("fill_time < %s")
        params.append(closed_before)

    query = """
        SELECT *
        FROM broker_execution
        WHERE {}
        ORDER BY fill_time ASC, recorded_at ASC, exec_id ASC
    """.format(" AND ".join(filters))
    cursor = conn.execute(query, params)
    rows = [signal_store.row_to_dict(cursor, row) for row in cursor.fetchall()]

    lots_by_pair: dict[str, list[dict]] = {}
    closed: list[dict] = []

    for row in rows:
        pair_id = str(row.get('pair') or '').upper()
        if not pair_id:
            continue
        fill_time = row.get('fill_time')
        if fill_time is None:
            continue
        side_sign = _broker_side_sign(row.get('side'))
        if side_sign == 0:
            continue
        units = float(row.get('fill_units') or 0.0)
        if units <= 0:
            continue

        signed_units = side_sign * units
        lots = lots_by_pair.setdefault(pair_id, [])
        remaining = abs(signed_units)
        matched: list[tuple[dict, float]] = []

        while remaining > 0 and lots and (lots[0]['signed_units'] > 0) != (signed_units > 0):
            lot = lots[0]
            available = abs(float(lot['signed_units']))
            take = min(available, remaining)
            matched.append((lot, take))
            remaining -= take

            lot_sign = 1 if lot['signed_units'] > 0 else -1
            lot['signed_units'] = lot_sign * (available - take)
            if abs(float(lot['signed_units'])) < 1e-9:
                lots.pop(0)

        if _is_broker_liquidation_ref(row.get('order_ref')) and matched and _ts_in_range(
            fill_time,
            closed_after=closed_after,
            closed_before=closed_before,
        ):
            matched_units = sum(qty for _lot, qty in matched)
            if matched_units > 0:
                entry_weighted = sum(float(lot['price']) * qty for lot, qty in matched)
                entry_price = entry_weighted / matched_units
                close_price = float(row.get('fill_price') or row.get('avg_price') or 0.0)
                entry_sign = 1 if matched[0][0]['signed_units_at_entry'] > 0 else -1
                direction = 'LONG' if entry_sign > 0 else 'SHORT'
                pip = signal_store.pair_pip(pair_id)
                pnl_pips = None
                if close_price and pip:
                    if direction == 'LONG':
                        pnl_pips = (close_price - entry_price) / pip
                    else:
                        pnl_pips = (entry_price - close_price) / pip

                opened_at = min(lot['fill_time'] for lot, _qty in matched)
                synthetic = {
                    'signal_id': f"BROKER-LIQUIDATION:{pair_id}:{row.get('order_id') or row.get('exec_id')}",
                    'pair': pair_id,
                    'direction': direction,
                    'signal_time': opened_at,
                    'detected_at': fill_time,
                    'entry_price': entry_price,
                    'submitted_entry_price': entry_price,
                    'sl_price': None,
                    'tp_price': None,
                    'zone_upper': None,
                    'zone_lower': None,
                    'zone_strength': 'broker',
                    'zone_type': 'broker',
                    'quality_score': None,
                    'status': 'CLOSED',
                    'transacted': 1,
                    'execution_enabled': 0,
                    'planned_units': int(round(matched_units)),
                    'risk_amount': None,
                    'account_currency': 'GBP',
                    'notional_account': None,
                    'order_id': row.get('order_id'),
                    'take_profit_order_id': None,
                    'stop_loss_order_id': None,
                    'note': f"broker-only liquidation; matched {len(matched)} unlinked fill(s)",
                    'executed_at': fill_time,
                    'opened_at': opened_at,
                    'opened_price': entry_price,
                    'open_units': int(round(matched_units)),
                    'remaining_units': 0,
                    'fill_count': len(matched) + 1,
                    'last_fill_at': fill_time,
                    'broker_order_status': 'Filled',
                    'exit_signal_at': fill_time,
                    'exit_signal_reason': 'LIVE_LIQUIDATE',
                    'exit_signal_price': close_price,
                    'closed_at': fill_time,
                    'closed_price': close_price,
                    'close_reason': 'MANUAL',
                    'close_source': 'broker_liquidation',
                    'pnl_pips': pnl_pips,
                    'execution_mode': 'broker',
                    'ibkr_account': None,
                    'submit_bid': None,
                    'submit_ask': None,
                    'submit_spread': None,
                    'quote_source': 'broker_execution',
                    'quote_time': fill_time,
                    'last_updated_at': fill_time,
                    'pnl_r': None,
                }
                try:
                    from .live_history import compute_pnl_gbp
                    synthetic['pnl_gbp'] = compute_pnl_gbp(
                        synthetic,
                        conn,
                        as_of=fill_time,
                        to_currency='GBP',
                    )
                except Exception:
                    synthetic['pnl_gbp'] = None
                closed.append(synthetic)

        if remaining > 0:
            lots.append({
                'signed_units': (1 if signed_units > 0 else -1) * remaining,
                'signed_units_at_entry': (1 if signed_units > 0 else -1) * remaining,
                'price': float(row.get('fill_price') or row.get('avg_price') or 0.0),
                'fill_time': fill_time,
                'exec_id': row.get('exec_id'),
                'order_id': row.get('order_id'),
            })

    closed.sort(key=lambda r: pd.Timestamp(r['closed_at']), reverse=True)
    if limit is not None:
        closed = closed[:int(limit)]
    return closed


def load_unmatched_broker_liquidation_trades(
    *,
    pair: str | None = None,
    closed_after=None,
    closed_before=None,
    limit: int | None = None,
    db_path: str | None = None,
) -> list[dict]:
    """Open a DB connection and load broker-only liquidation history rows."""

    db_path = signal_store.ensure_signal_tables(db_path)
    conn = _connect(db_path)
    try:
        return load_unmatched_broker_liquidation_trades_conn(
            conn,
            pair=pair,
            closed_after=closed_after,
            closed_before=closed_before,
            limit=limit,
        )
    finally:
        conn.close()
