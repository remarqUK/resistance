#!/usr/bin/env python
"""Migrate project data from the sqlite cache into Postgres.

This script:
- creates the configured Postgres database if missing,
- creates all required tables with Postgres-native column types,
- copies data from the sqlite source in batches,
- creates indexes for hot access paths used by the runtime queries,
- and reports row-count verification.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import os
import sqlite3
from datetime import datetime
from typing import Any
from urllib.parse import urlparse, urlunparse

import pandas as pd

try:
    from fx_sr.db import INTERVAL_TO_CODE, PAIR_TO_CODE, TICKER_TO_CODE
except Exception:  # pragma: no cover
    try:
        from fx_sr.profiles import PAIRS
    except Exception:  # pragma: no cover
        PAIRS = {}

    _KNOWN_TICKERS = sorted(
        {
            info['ticker']
            for info in PAIRS.values()
            if isinstance(info, dict) and info.get('ticker')
        }
    )
    TICKER_TO_CODE = {ticker: idx + 1 for idx, ticker in enumerate(_KNOWN_TICKERS)}
    _KNOWN_PAIRS = sorted(PAIRS.keys())
    PAIR_TO_CODE = {pair: idx + 1 for idx, pair in enumerate(_KNOWN_PAIRS)}
    INTERVAL_TO_CODE = {'1m': 1, '1h': 2, '1d': 3}

try:
    import psycopg
except ImportError as exc:
    try:
        import psycopg2 as psycopg  # type: ignore
    except ImportError as fallback_exc:
        raise SystemExit("psycopg or psycopg2 is required for PostgreSQL migration") from (
            fallback_exc
        )
_POSTGRES_DRIVER = 'psycopg2' if 'psycopg2' in psycopg.__name__ else 'psycopg'


def _quote_identifier(value: str) -> str:
    """Return a double-quoted SQL identifier."""

    return '"' + value.replace('"', '""') + '"'


DEFAULT_POSTGRES_URL = os.environ.get(
    "RESISTANCE_DATABASE_URL",
    "postgresql://postgres:Harrison12_!@localhost:5432/resistance",
)
DEFAULT_SQLITE_DB = "fx_data.db"
MIGRATION_TABLES = [
    "ohlc",
    "l2_snapshot",
    "l2_level",
    "backtest_result",
    "detected_signal",
    "detected_signal_fill",
    "open_trades",
]


def _parse_excluded_tables(value: str) -> set[str]:
    """Parse and normalize comma-separated table names."""

    return {name.strip().lower() for name in value.split(",") if name.strip()}


def _build_table_list(excluded_tables: set[str] | None) -> list[str]:
    excluded = excluded_tables or set()
    unknown = excluded - set(MIGRATION_TABLES)
    if unknown:
        raise ValueError(f"unknown tables in --exclude-tables: {', '.join(sorted(unknown))}")
    return [table for table in MIGRATION_TABLES if table not in excluded]


def _db_name_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = (parsed.path or "").lstrip("/")
    return name or "resistance"


def _replace_db_name(url: str, db_name: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, f"/{db_name}", "", "", ""))


def _ensure_database_exists(url: str) -> str:
    db_name = _db_name_from_url(url)
    admin_url = _replace_db_name(url, "postgres")

    if _POSTGRES_DRIVER == 'psycopg2':
        admin_conn = psycopg.connect(admin_url)
        admin_conn.autocommit = True
    else:
        admin_conn = psycopg.connect(admin_url, autocommit=True)

    try:
        with admin_conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            if cur.fetchone() is None:
                cur.execute(f"CREATE DATABASE {_quote_identifier(db_name)}")
    finally:
        admin_conn.close()

    return _replace_db_name(url, db_name)


TABLE_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    "ohlc": [
        ("ticker", "SMALLINT NOT NULL"),
        ("interval", "SMALLINT NOT NULL CHECK (interval IN (1, 2, 3))"),
        ("ts", "TIMESTAMPTZ NOT NULL"),
        ("open", "DOUBLE PRECISION"),
        ("high", "DOUBLE PRECISION"),
        ("low", "DOUBLE PRECISION"),
        ("close", "DOUBLE PRECISION"),
        ("volume", "DOUBLE PRECISION"),
    ],
    "l2_snapshot": [
        ("id", "BIGSERIAL"),
        ("ticker", "SMALLINT NOT NULL"),
        ("pair", "SMALLINT NOT NULL CHECK (pair > 0)"),
        ("ts", "TIMESTAMPTZ NOT NULL"),
        ("depth_requested", "INTEGER NOT NULL"),
        ("best_bid", "DOUBLE PRECISION"),
        ("best_ask", "DOUBLE PRECISION"),
        ("mid_price", "DOUBLE PRECISION"),
        ("spread", "DOUBLE PRECISION"),
        ("bid_levels", "INTEGER NOT NULL DEFAULT 0"),
        ("ask_levels", "INTEGER NOT NULL DEFAULT 0"),
    ],
    "l2_level": [
        ("snapshot_id", "BIGINT NOT NULL"),
        ("side", "TEXT NOT NULL"),
        ("level_no", "INTEGER NOT NULL"),
        ("price", "DOUBLE PRECISION NOT NULL"),
        ("size", "DOUBLE PRECISION"),
        ("market_maker", "TEXT"),
    ],
    "backtest_result": [
        ("pair", "TEXT NOT NULL"),
        ("params_hash", "TEXT NOT NULL"),
        ("hourly_days", "INTEGER NOT NULL"),
        ("zone_history_days", "INTEGER NOT NULL"),
        ("data_signature", "TEXT NOT NULL"),
        ("ticker", "SMALLINT NOT NULL"),
        ("strategy_version", "TEXT NOT NULL"),
        ("result_json", "TEXT NOT NULL"),
        ("run_config_json", "TEXT"),
        ("created_at", "TIMESTAMPTZ NOT NULL"),
        ("updated_at", "TIMESTAMPTZ NOT NULL"),
    ],
    "detected_signal": [
        ("signal_id", "TEXT PRIMARY KEY"),
        ("pair", "TEXT NOT NULL"),
        ("direction", "TEXT NOT NULL"),
        ("signal_time", "TIMESTAMPTZ NOT NULL"),
        ("detected_at", "TIMESTAMPTZ NOT NULL"),
        ("entry_price", "DOUBLE PRECISION NOT NULL"),
        ("sl_price", "DOUBLE PRECISION NOT NULL"),
        ("tp_price", "DOUBLE PRECISION NOT NULL"),
        ("zone_upper", "DOUBLE PRECISION NOT NULL"),
        ("zone_lower", "DOUBLE PRECISION NOT NULL"),
        ("zone_strength", "TEXT NOT NULL"),
        ("zone_type", "TEXT NOT NULL"),
        ("quality_score", "DOUBLE PRECISION NOT NULL DEFAULT 0"),
        ("status", "TEXT NOT NULL"),
        ("transacted", "INTEGER NOT NULL DEFAULT 0"),
        ("execution_enabled", "INTEGER NOT NULL DEFAULT 0"),
        ("planned_units", "BIGINT"),
        ("risk_amount", "DOUBLE PRECISION"),
        ("account_currency", "TEXT"),
        ("notional_account", "DOUBLE PRECISION"),
        ("order_id", "BIGINT"),
        ("take_profit_order_id", "BIGINT"),
        ("stop_loss_order_id", "BIGINT"),
        ("note", "TEXT"),
        ("executed_at", "TIMESTAMPTZ"),
        ("opened_at", "TIMESTAMPTZ"),
        ("opened_price", "DOUBLE PRECISION"),
        ("open_units", "BIGINT"),
        ("remaining_units", "BIGINT"),
        ("fill_count", "BIGINT"),
        ("last_fill_at", "TIMESTAMPTZ"),
        ("broker_order_status", "TEXT"),
        ("exit_signal_at", "TIMESTAMPTZ"),
        ("exit_signal_reason", "TEXT"),
        ("exit_signal_price", "DOUBLE PRECISION"),
        ("closed_at", "TIMESTAMPTZ"),
        ("closed_price", "DOUBLE PRECISION"),
        ("close_reason", "TEXT"),
        ("close_source", "TEXT"),
        ("pnl_pips", "DOUBLE PRECISION"),
        ("execution_mode", "TEXT"),
        ("ibkr_account", "TEXT"),
        ("submitted_entry_price", "DOUBLE PRECISION"),
        ("submitted_tp_price", "DOUBLE PRECISION"),
        ("submitted_sl_price", "DOUBLE PRECISION"),
        ("submit_bid", "DOUBLE PRECISION"),
        ("submit_ask", "DOUBLE PRECISION"),
        ("submit_spread", "DOUBLE PRECISION"),
        ("quote_source", "TEXT"),
        ("quote_time", "TIMESTAMPTZ"),
        ("last_updated_at", "TIMESTAMPTZ NOT NULL"),
    ],
    "detected_signal_fill": [
        ("exec_id", "TEXT PRIMARY KEY"),
        ("signal_id", "TEXT NOT NULL"),
        ("pair", "TEXT NOT NULL"),
        ("direction", "TEXT NOT NULL"),
        ("order_id", "BIGINT"),
        ("fill_time", "TIMESTAMPTZ"),
        ("fill_price", "DOUBLE PRECISION NOT NULL"),
        ("fill_units", "BIGINT NOT NULL"),
        ("cum_qty", "DOUBLE PRECISION"),
        ("avg_price", "DOUBLE PRECISION"),
        ("side", "TEXT"),
        ("order_ref", "TEXT"),
        ("recorded_at", "TIMESTAMPTZ NOT NULL"),
    ],
    "open_trades": [
        ("pair", "TEXT NOT NULL"),
        ("direction", "TEXT NOT NULL"),
        ("entry_time", "TIMESTAMPTZ NOT NULL"),
        ("entry_price", "DOUBLE PRECISION NOT NULL"),
        ("sl_price", "DOUBLE PRECISION NOT NULL"),
        ("tp_price", "DOUBLE PRECISION NOT NULL"),
        ("zone_upper", "DOUBLE PRECISION NOT NULL"),
        ("zone_lower", "DOUBLE PRECISION NOT NULL"),
        ("zone_strength", "TEXT NOT NULL"),
        ("risk", "DOUBLE PRECISION NOT NULL"),
        ("bars_monitored", "INTEGER DEFAULT 0"),
        ("ibkr_avg_cost", "DOUBLE PRECISION"),
        ("ibkr_size", "DOUBLE PRECISION"),
        ("signal_id", "TEXT"),
        ("pending_exit_reason", "TEXT"),
        ("pending_exit_price", "DOUBLE PRECISION"),
        ("pending_exit_detected_at", "TIMESTAMPTZ"),
        ("last_processed_bar_time", "TIMESTAMPTZ"),
        ("created_at", "TIMESTAMPTZ NOT NULL"),
    ],
}


PRIMARY_KEYS: dict[str, list[str]] = {
    "ohlc": ["ticker", "interval", "ts"],
    "l2_snapshot": ["id"],
    "l2_level": ["snapshot_id", "side", "level_no"],
    "backtest_result": ["pair", "params_hash", "hourly_days", "zone_history_days"],
    "detected_signal": ["signal_id"],
    "detected_signal_fill": ["exec_id"],
    "open_trades": ["pair", "direction"],
}


TIMESTAMP_COLUMNS = {
    "ohlc": {"ts"},
    "l2_snapshot": {"ts"},
    "l2_level": set(),
    "backtest_result": {"created_at", "updated_at"},
    "detected_signal": {
        "signal_time",
        "detected_at",
        "executed_at",
        "opened_at",
        "last_fill_at",
        "exit_signal_at",
        "closed_at",
        "quote_time",
        "last_updated_at",
    },
    "detected_signal_fill": {"fill_time", "recorded_at"},
    "open_trades": {
        "entry_time",
        "last_processed_bar_time",
        "pending_exit_detected_at",
        "created_at",
    },
}


INTEGER_COLUMNS = {
    "l2_snapshot": {"id", "depth_requested", "bid_levels", "ask_levels"},
    "backtest_result": {"hourly_days", "zone_history_days"},
    "detected_signal": {
        "transacted",
        "execution_enabled",
        "planned_units",
        "order_id",
        "take_profit_order_id",
        "stop_loss_order_id",
        "open_units",
        "remaining_units",
        "fill_count",
    },
    "detected_signal_fill": {"fill_units", "order_id"},
    "open_trades": {"bars_monitored"},
    "ohlc": set(),
    "l2_level": {"snapshot_id", "level_no"},
}


FLOAT_COLUMNS = {
    "ohlc": {"open", "high", "low", "close", "volume"},
    "l2_snapshot": {
        "best_bid",
        "best_ask",
        "mid_price",
        "spread",
    },
    "l2_level": {"price", "size"},
    "backtest_result": set(),
    "detected_signal": {
        "entry_price",
        "sl_price",
        "tp_price",
        "zone_upper",
        "zone_lower",
        "quality_score",
        "risk_amount",
        "notional_account",
        "opened_price",
        "exit_signal_price",
        "closed_price",
        "pnl_pips",
        "submitted_entry_price",
        "submitted_tp_price",
        "submitted_sl_price",
        "submit_bid",
        "submit_ask",
        "submit_spread",
    },
    "detected_signal_fill": {
        "fill_price",
        "cum_qty",
        "avg_price",
    },
    "open_trades": {
        "entry_price",
        "sl_price",
        "tp_price",
        "zone_upper",
        "zone_lower",
        "risk",
        "ibkr_avg_cost",
        "ibkr_size",
        "pending_exit_price",
    },
}


def _ensure_timestamp(value: Any) -> Any:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        timestamp = pd.to_datetime(value, utc=True, errors="coerce")
        if pd.isna(timestamp):
            return None
        return timestamp.to_pydatetime()
    if isinstance(value, datetime):
        return value
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.to_pydatetime()


def _coerce_ticker_value(value: Any) -> int:
    ticker_code = TICKER_TO_CODE.get(value)
    if ticker_code is not None:
        return int(ticker_code)
    if isinstance(value, int) and value > 0:
        return int(value)
    if isinstance(value, str) and value.isdigit():
        return int(value)
    raise ValueError(f"Unknown ticker for Postgres migration encoding: {value}")


def _coerce_pair_value(value: Any) -> int:
    pair_code = PAIR_TO_CODE.get(value)
    if pair_code is not None:
        return int(pair_code)
    if isinstance(value, int) and value > 0:
        return int(value)
    if isinstance(value, str) and value.isdigit():
        return int(value)
    raise ValueError(f"Unknown pair for Postgres migration encoding: {value}")


def _coerce_interval_value(value: Any) -> int:
    if isinstance(value, int) and value in INTERVAL_TO_CODE.values():
        return int(value)
    if isinstance(value, str) and value.isdigit():
        maybe = int(value)
        if maybe in INTERVAL_TO_CODE.values():
            return maybe
    interval_code = INTERVAL_TO_CODE.get(value)
    if interval_code is not None:
        return int(interval_code)
    raise ValueError(f"Unknown interval for Postgres migration encoding: {value}")


def _coerce_value(table: str, column: str, value: Any) -> Any:
    if value is None or value == "":
        return None
    if table == "ohlc" and column == "ticker":
        return _coerce_ticker_value(value)
    if table == "ohlc" and column == "interval":
        return _coerce_interval_value(value)
    if table == "l2_snapshot" and column == "ticker":
        return _coerce_ticker_value(value)
    if table == "l2_snapshot" and column == "pair":
        return _coerce_pair_value(value)
    if table == "backtest_result" and column == "ticker":
        return _coerce_ticker_value(value)
    if column in TIMESTAMP_COLUMNS.get(table, set()):
        return _ensure_timestamp(value)
    if column in INTEGER_COLUMNS.get(table, set()):
        return int(value)
    if column in FLOAT_COLUMNS.get(table, set()):
        return float(value)
    return value


def _sqlite_exists(conn: sqlite3.Connection, table: str) -> bool:
    cursor = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cursor.fetchone() is not None


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    row = conn.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    if row is None:
        return set()

    cursor = conn.execute(f"PRAGMA table_info({table})")
    return {entry[1] for entry in cursor.fetchall()}


def _build_insert(table: str, columns: list[tuple[str, str]]) -> str:
    column_names = [name for name, _ in columns]
    return (
        f"INSERT INTO {table} ({', '.join(column_names)}) "
        f"VALUES ({', '.join(['%s'] * len(column_names))}) "
        f"ON CONFLICT ({', '.join(PRIMARY_KEYS[table])}) DO NOTHING"
    )


def _create_open_trades(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS open_trades (
            pair TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry_time TIMESTAMPTZ NOT NULL,
            entry_price DOUBLE PRECISION NOT NULL,
            sl_price DOUBLE PRECISION NOT NULL,
            tp_price DOUBLE PRECISION NOT NULL,
            zone_upper DOUBLE PRECISION NOT NULL,
            zone_lower DOUBLE PRECISION NOT NULL,
            zone_strength TEXT NOT NULL,
            risk DOUBLE PRECISION NOT NULL,
            bars_monitored INTEGER DEFAULT 0,
            ibkr_avg_cost DOUBLE PRECISION,
            ibkr_size DOUBLE PRECISION,
            signal_id TEXT,
            pending_exit_reason TEXT,
            pending_exit_price DOUBLE PRECISION,
            pending_exit_detected_at TIMESTAMPTZ,
            last_processed_bar_time TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (pair, direction)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_open_trades_pair_direction ON open_trades (pair, direction)")


def _create_common_tables(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ohlc (
            ticker SMALLINT NOT NULL,
            interval SMALLINT NOT NULL CHECK (interval IN (1, 2, 3)),
            ts TIMESTAMPTZ NOT NULL,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume DOUBLE PRECISION,
            PRIMARY KEY (ticker, interval, ts)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ohlc_lookup ON ohlc (ticker, interval, ts)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS l2_snapshot (
            id BIGSERIAL PRIMARY KEY,
            ticker SMALLINT NOT NULL CHECK (ticker > 0),
            pair SMALLINT NOT NULL CHECK (pair > 0),
            ts TIMESTAMPTZ NOT NULL,
            depth_requested INTEGER NOT NULL,
            best_bid DOUBLE PRECISION,
            best_ask DOUBLE PRECISION,
            mid_price DOUBLE PRECISION,
            spread DOUBLE PRECISION,
            bid_levels INTEGER NOT NULL DEFAULT 0,
            ask_levels INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_l2_snapshot_lookup ON l2_snapshot (ticker, ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_l2_snapshot_ticker_pair ON l2_snapshot (ticker, pair)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS l2_level (
            snapshot_id BIGINT NOT NULL,
            side TEXT NOT NULL,
            level_no INTEGER NOT NULL,
            price DOUBLE PRECISION NOT NULL,
            size DOUBLE PRECISION,
            market_maker TEXT,
            PRIMARY KEY (snapshot_id, side, level_no),
            FOREIGN KEY (snapshot_id) REFERENCES l2_snapshot(id) ON DELETE CASCADE
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_l2_level_lookup ON l2_level (snapshot_id, side, level_no)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS backtest_result (
            pair TEXT NOT NULL,
            params_hash TEXT NOT NULL,
            hourly_days INTEGER NOT NULL,
            zone_history_days INTEGER NOT NULL,
            data_signature TEXT NOT NULL,
            ticker SMALLINT NOT NULL CHECK (ticker > 0),
            strategy_version TEXT NOT NULL,
            result_json TEXT NOT NULL,
            run_config_json TEXT,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (pair, params_hash, hourly_days, zone_history_days)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_backtest_lookup ON backtest_result (pair, params_hash, hourly_days, zone_history_days)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_backtest_pair_updated_at ON backtest_result (pair, updated_at DESC)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS detected_signal (
            signal_id TEXT PRIMARY KEY,
            pair TEXT NOT NULL,
            direction TEXT NOT NULL,
            signal_time TIMESTAMPTZ NOT NULL,
            detected_at TIMESTAMPTZ NOT NULL,
            entry_price DOUBLE PRECISION NOT NULL,
            sl_price DOUBLE PRECISION NOT NULL,
            tp_price DOUBLE PRECISION NOT NULL,
            zone_upper DOUBLE PRECISION NOT NULL,
            zone_lower DOUBLE PRECISION NOT NULL,
            zone_strength TEXT NOT NULL,
            zone_type TEXT NOT NULL,
            quality_score DOUBLE PRECISION NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            transacted INTEGER NOT NULL DEFAULT 0,
            execution_enabled INTEGER NOT NULL DEFAULT 0,
            planned_units BIGINT,
            risk_amount DOUBLE PRECISION,
            account_currency TEXT,
            notional_account DOUBLE PRECISION,
            order_id BIGINT,
            take_profit_order_id BIGINT,
            stop_loss_order_id BIGINT,
            note TEXT,
            executed_at TIMESTAMPTZ,
            opened_at TIMESTAMPTZ,
            opened_price DOUBLE PRECISION,
            open_units BIGINT,
            remaining_units BIGINT,
            fill_count BIGINT,
            last_fill_at TIMESTAMPTZ,
            broker_order_status TEXT,
            exit_signal_at TIMESTAMPTZ,
            exit_signal_reason TEXT,
            exit_signal_price DOUBLE PRECISION,
            closed_at TIMESTAMPTZ,
            closed_price DOUBLE PRECISION,
            close_reason TEXT,
            close_source TEXT,
            pnl_pips DOUBLE PRECISION,
            execution_mode TEXT,
            ibkr_account TEXT,
            submitted_entry_price DOUBLE PRECISION,
            submitted_tp_price DOUBLE PRECISION,
            submitted_sl_price DOUBLE PRECISION,
            submit_bid DOUBLE PRECISION,
            submit_ask DOUBLE PRECISION,
            submit_spread DOUBLE PRECISION,
            quote_source TEXT,
            quote_time TIMESTAMPTZ,
            last_updated_at TIMESTAMPTZ NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_detected_signal_pair_time ON detected_signal (pair, signal_time DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_detected_signal_status ON detected_signal (status, transacted, pair, direction)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_detected_signal_pair_status_time ON detected_signal (pair, status, signal_time DESC, detected_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_detected_signal_status_last_updated ON detected_signal (status, pair, last_updated_at DESC)")
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_detected_signal_claim
        ON detected_signal (pair, direction, status, opened_at, executed_at, detected_at)
        WHERE transacted = 1 AND closed_at IS NULL
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_detected_signal_reconcile
        ON detected_signal (order_id, status, pair)
        WHERE transacted = 1 AND closed_at IS NULL AND order_id IS NOT NULL
    """)

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS detected_signal_fill (
            exec_id TEXT PRIMARY KEY,
            signal_id TEXT NOT NULL,
            pair TEXT NOT NULL,
            direction TEXT NOT NULL,
            order_id BIGINT,
            fill_time TIMESTAMPTZ,
            fill_price DOUBLE PRECISION NOT NULL,
            fill_units BIGINT NOT NULL,
            cum_qty DOUBLE PRECISION,
            avg_price DOUBLE PRECISION,
            side TEXT,
            order_ref TEXT,
            recorded_at TIMESTAMPTZ NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_detected_signal_fill_signal_time ON detected_signal_fill (signal_id, fill_time DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_detected_signal_fill_signal_time_asc ON detected_signal_fill (signal_id, fill_time ASC, recorded_at ASC, exec_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_detected_signal_fill_order ON detected_signal_fill (order_id, fill_time DESC)")


def _create_tables(conn) -> None:
    with conn.cursor() as cur:
        _create_common_tables(cur)
        _create_open_trades(cur)


def _copy_table(
    sqlite_conn: sqlite3.Connection,
    pg_conn,
    table: str,
    batch_size: int,
) -> tuple[int, int]:
    sqlite_columns = _sqlite_columns(sqlite_conn, table)
    if not sqlite_columns:
        return 0, 0

    target_columns = [name for name, _ in TABLE_SCHEMAS[table]]
    source_columns = [
        name if name in sqlite_columns else f"NULL AS {name}" for name in target_columns
    ]
    select_sql = f"SELECT {', '.join(source_columns)} FROM {table}"

    insert_sql = _build_insert(table, TABLE_SCHEMAS[table])

    cursor = sqlite_conn.execute(select_sql)
    total = 0
    copied = 0

    with pg_conn.cursor() as pg_cursor:
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            total += len(rows)
            payload = [
                tuple(_coerce_value(table, col, row[idx]) for idx, col in enumerate(target_columns))
                for row in rows
            ]
            pg_cursor.executemany(insert_sql, payload)
            copied += len(payload)
            pg_conn.commit()

    return total, copied


def _verify_counts(sqlite_conn: sqlite3.Connection, pg_cursor, table: str) -> tuple[int, int]:
    if not _sqlite_exists(sqlite_conn, table):
        return 0, 0

    sqlite_count = sqlite_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    pg_cursor.execute(f"SELECT COUNT(*) FROM {table}")
    pg_count = pg_cursor.fetchone()[0]
    return int(sqlite_count or 0), int(pg_count or 0)


def migrate(
    sqlite_path: str,
    postgres_url: str,
    *,
    batch_size: int = 5_000,
    verify_only: bool = False,
    exclude_tables: set[str] | None = None,
) -> None:
    sqlite_path = str(Path(sqlite_path))
    if not Path(sqlite_path).exists():
        raise FileNotFoundError(f"sqlite database not found: {sqlite_path}")

    actual_url = _ensure_database_exists(postgres_url)
    sqlite_conn = sqlite3.connect(sqlite_path)
    try:
        pg_conn = psycopg.connect(actual_url)
        try:
            _create_tables(pg_conn)
            tables = _build_table_list(exclude_tables)
            if not verify_only:
                for table in tables:
                    if _sqlite_exists(sqlite_conn, table):
                        total, copied = _copy_table(sqlite_conn, pg_conn, table, batch_size)
                        print(f"{table}: copied {copied} rows (source fetched {total})")

                pg_conn.commit()

            if _sqlite_exists(sqlite_conn, "l2_snapshot"):
                with pg_conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT setval(
                                pg_get_serial_sequence('l2_snapshot', 'id'),
                                COALESCE((SELECT MAX(id) FROM l2_snapshot), 1),
                                true
                            )
                            """
                        )

            with pg_conn.cursor() as cur:
                for table in tables:
                    if _sqlite_exists(sqlite_conn, table):
                        source_count, target_count = _verify_counts(sqlite_conn, cur, table)
                        print(f"{table}: sqlite={source_count} postgres={target_count}")
                if exclude_tables:
                    print(f"Skipped tables: {', '.join(sorted(exclude_tables))}")
            pg_conn.commit()
        finally:
            pg_conn.close()
    finally:
        sqlite_conn.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate resistance sqlite cache into Postgres")
    parser.add_argument(
        "--sqlite-db",
        default=DEFAULT_SQLITE_DB,
        help="Path to source sqlite database",
    )
    parser.add_argument(
        "--postgres-url",
        default=DEFAULT_POSTGRES_URL,
        help="Postgres connection URL",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5_000,
        help="Rows to copy per batch",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Create schema and only verify row counts without copying",
    )
    parser.add_argument(
        "--exclude-tables",
        type=str,
        default="",
        help="Comma-separated tables to skip (e.g. ohlc).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    migrate(
        args.sqlite_db,
        args.postgres_url,
        batch_size=args.batch_size,
        verify_only=args.verify_only,
        exclude_tables=_parse_excluded_tables(args.exclude_tables),
    )


if __name__ == "__main__":
    main()
