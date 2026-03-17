"""Persistence helpers for OHLC, L2 snapshots, and backtest cache.

PostgreSQL is the canonical storage backend.
"""

from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from datetime import datetime
from urllib.parse import urlparse
from typing import Optional, Sequence, Tuple

import pandas as pd

try:
    import psycopg
    _POSTGRES_DRIVER = 'psycopg'
except ImportError:  # pragma: no cover
    psycopg = None
    _POSTGRES_DRIVER = 'psycopg'
    try:
        import psycopg2 as psycopg  # type: ignore
        _POSTGRES_DRIVER = 'psycopg2'
    except ImportError:
        psycopg = None

try:
    from fx_sr.profiles import PAIRS
except Exception:  # pragma: no cover - tests may patch import paths
    PAIRS = {}

_KNOWN_TICKERS = sorted(
    {
        info['ticker']
        for info in PAIRS.values()
        if isinstance(info, dict) and info.get('ticker')
    }
)
_KNOWN_PAIRS = sorted(PAIRS.keys())

TICKER_TO_CODE = {ticker: idx + 1 for idx, ticker in enumerate(_KNOWN_TICKERS)}
CODE_TO_TICKER = {code: ticker for ticker, code in TICKER_TO_CODE.items()}
PAIR_TO_CODE = {pair: idx + 1 for idx, pair in enumerate(_KNOWN_PAIRS)}
CODE_TO_PAIR = {code: pair for pair, code in PAIR_TO_CODE.items()}

INTERVAL_TO_CODE = {
    '1m': 1,
    '1h': 2,
    '1d': 3,
}
CODE_TO_INTERVAL = {code: interval for interval, code in INTERVAL_TO_CODE.items()}

DEFAULT_POSTGRES_URL = 'postgresql://postgres:Harrison12_!@localhost:5432/resistance'
RESISTANCE_DB_URL_ENV = 'RESISTANCE_DATABASE_URL'

_DB_SCHEMA_INIT_LOCK = threading.Lock()
_DB_SCHEMA_READY: set[str] = set()

def _default_db_url() -> str:
    return os.environ.get(RESISTANCE_DB_URL_ENV, DEFAULT_POSTGRES_URL)


def get_db_path() -> str:
    """Return the default database path."""

    return _default_db_url()


def _require_postgres_url(db_path: str | None) -> str:
    if not db_path:
        raise ValueError('Database path must be a Postgres URL')
    parsed = urlparse(db_path)
    if parsed.scheme not in {'postgres', 'postgresql'}:
        raise ValueError(f'Unsupported db path for this build: {db_path}')
    return db_path


def _db_exists(db_path: str) -> bool:
    # PostgreSQL tables are created lazily if missing.
    return True


def _rewrite_named_placeholders(sql: str) -> tuple[str, list[str]]:
    names: list[str] = []
    out: list[str] = []
    in_single = False
    in_double = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch == "'" and not in_double:
            in_single = not in_single
            out.append(ch)
            i += 1
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            out.append(ch)
            i += 1
            continue
        if not in_single and not in_double and ch == ':':
            if i + 1 < len(sql) and (sql[i + 1].isalpha() or sql[i + 1] == '_'):
                j = i + 1
                while j < len(sql) and (sql[j].isalnum() or sql[j] == '_'):
                    j += 1
                names.append(sql[i + 1 : j])
                out.append('%s')
                i = j
                continue
        out.append(ch)
        i += 1
    return ''.join(out), names


def _adapt_sql_and_params(
    sql: str,
    params: object,
) -> tuple[str, object]:
    if params is None:
        return sql, params
    if isinstance(params, dict):
        sql, names = _rewrite_named_placeholders(sql)
        return sql, tuple(params[name] for name in names)
    if isinstance(params, (list, tuple)):
        sql, _ = _rewrite_named_placeholders(sql)
        return sql, tuple(params)
    return sql, params


class _CompatCursor:
    """DB-API cursor wrapper with SQL adaptation for PostgreSQL."""

    def __init__(self, raw_cursor) -> None:
        self._raw_cursor = raw_cursor

    def execute(self, sql: str, params: object = None):
        sql, params = _adapt_sql_and_params(sql, params)
        if params is None:
            return self._raw_cursor.execute(sql)
        return self._raw_cursor.execute(sql, params)

    def executemany(self, sql: str, seq_of_parameters):
        parameters = list(seq_of_parameters)
        if not parameters:
            return self._raw_cursor.executemany(sql, parameters)

        converted_sql = _adapt_sql_and_params(sql, None)[0]
        if isinstance(parameters[0], dict):
            converted_sql, names = _rewrite_named_placeholders(converted_sql)
            converted_params = [tuple(row[name] for name in names) for row in parameters]
            return self._raw_cursor.executemany(converted_sql, converted_params)

        converted_sql, _ = _rewrite_named_placeholders(converted_sql)
        return self._raw_cursor.executemany(converted_sql, parameters)

    @property
    def description(self):
        return self._raw_cursor.description

    @property
    def rowcount(self):
        return self._raw_cursor.rowcount

    @property
    def lastrowid(self):
        return getattr(self._raw_cursor, "lastrowid", None)

    def fetchone(self):
        return self._raw_cursor.fetchone()

    def fetchall(self):
        return self._raw_cursor.fetchall()

    def fetchmany(self, size: int | None = None):
        if size is None:
            return self._raw_cursor.fetchmany()
        return self._raw_cursor.fetchmany(size=size)

    def __iter__(self):
        return iter(self._raw_cursor)

    def close(self):
        return self._raw_cursor.close()


class _CompatConnection:
    """Thin wrapper for normalized cursor behavior."""

    def __init__(self, raw_conn) -> None:
        self._conn = raw_conn
        self.backend = 'postgres'
        self._last_rowcount = 0

    @property
    def row_factory(self):
        return getattr(self._conn, 'row_factory', None)

    @row_factory.setter
    def row_factory(self, value):
        if hasattr(self._conn, 'row_factory'):
            self._conn.row_factory = value

    @property
    def total_changes(self):
        return self._last_rowcount

    def cursor(self, *args, **kwargs):
        return _CompatCursor(self._conn.cursor(*args, **kwargs))

    def execute(self, sql: str, params: object = None):
        cursor = self.cursor()
        cursor.execute(sql, params)
        self._last_rowcount = cursor.rowcount
        return cursor

    def executemany(self, sql: str, seq_of_parameters):
        cursor = self.cursor()
        cursor.executemany(sql, seq_of_parameters)
        self._last_rowcount = cursor.rowcount
        return cursor

    def commit(self):
        return self._conn.commit()

    def rollback(self):
        return self._conn.rollback()

    def close(self):
        return self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc is not None:
            self.rollback()
        self.close()


def _connect(db_path: str | None = None) -> _CompatConnection:
    """Create a compatibility-wrapped connection for the selected backend."""

    if db_path is None:
        db_path = get_db_path()
    db_path = _require_postgres_url(db_path)

    if psycopg is None:
        raise RuntimeError('psycopg or psycopg2 is required for PostgreSQL database paths')

    if _POSTGRES_DRIVER == 'psycopg2':
        conn = psycopg.connect(db_path)
        conn.autocommit = False
    else:
        conn = psycopg.connect(db_path, autocommit=False)
    return _CompatConnection(conn)


@contextmanager
def db_transaction(db_path: str | None = None):
    """Yield a connection that commits or rolls back atomically."""

    if db_path is None:
        db_path = get_db_path()
    conn = _connect(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _table_columns(conn: _CompatConnection, table: str) -> set[str]:
    """Return the set of columns available for ``table``."""
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table,),
    ).fetchall()
    return {row[0] for row in rows}


def _init_postgres_schema(conn: _CompatConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ohlc (
            ticker   SMALLINT NOT NULL,
            interval SMALLINT NOT NULL CHECK (interval IN (1, 2, 3)),
            ts       TIMESTAMPTZ NOT NULL,
            open     DOUBLE PRECISION,
            high     DOUBLE PRECISION,
            low      DOUBLE PRECISION,
            close    DOUBLE PRECISION,
            volume   DOUBLE PRECISION,
            PRIMARY KEY (ticker, interval, ts)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ohlc_lookup
        ON ohlc (ticker, interval, ts)
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS l2_snapshot (
            id               BIGSERIAL PRIMARY KEY,
            ticker           SMALLINT NOT NULL CHECK (ticker > 0),
            pair             SMALLINT NOT NULL CHECK (pair > 0),
            ts               TIMESTAMPTZ NOT NULL,
            depth_requested  INTEGER NOT NULL,
            best_bid         DOUBLE PRECISION,
            best_ask         DOUBLE PRECISION,
            mid_price        DOUBLE PRECISION,
            spread           DOUBLE PRECISION,
            bid_levels       INTEGER NOT NULL DEFAULT 0,
            ask_levels       INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_l2_snapshot_lookup
        ON l2_snapshot (ticker, ts)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_l2_snapshot_ticker_pair
        ON l2_snapshot (ticker, pair)
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS l2_level (
            snapshot_id   BIGINT NOT NULL,
            side          TEXT NOT NULL,
            level_no      INTEGER NOT NULL,
            price         DOUBLE PRECISION NOT NULL,
            size          DOUBLE PRECISION,
            market_maker  TEXT,
            PRIMARY KEY (snapshot_id, side, level_no),
            FOREIGN KEY (snapshot_id) REFERENCES l2_snapshot(id) ON DELETE CASCADE
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_l2_level_lookup
        ON l2_level (snapshot_id, side, level_no)
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS backtest_result (
            pair               TEXT NOT NULL,
            params_hash        TEXT NOT NULL,
            hourly_days        INTEGER NOT NULL,
            zone_history_days  INTEGER NOT NULL,
            data_signature     TEXT NOT NULL,
            ticker             SMALLINT NOT NULL CHECK (ticker > 0),
            strategy_version   TEXT NOT NULL,
            result_json        TEXT NOT NULL,
            run_config_json    TEXT,
            created_at         TIMESTAMPTZ NOT NULL,
            updated_at         TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (pair, params_hash, hourly_days, zone_history_days)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_backtest_lookup
        ON backtest_result (pair, params_hash, hourly_days, zone_history_days)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_backtest_pair_updated_at
        ON backtest_result (pair, updated_at DESC)
    """)


def _escape_sql_value(value: str) -> str:
    return value.replace("'", "''")


def _ticker_to_smallint_expr(column_name: str) -> str:
    cases = [
        f"WHEN {column_name}::text = '{_escape_sql_value(ticker)}' THEN {code}"
        for ticker, code in TICKER_TO_CODE.items()
    ]
    return (
        "CASE "
        f"WHEN {column_name}::text ~ '^[0-9]+$' THEN {column_name}::smallint "
        f"{' '.join(cases)} "
        "ELSE NULL END"
    )


def _interval_to_smallint_expr(column_name: str) -> str:
    cases = [
        f"WHEN {column_name}::text = '{_escape_sql_value(interval)}' THEN {code}"
        for interval, code in INTERVAL_TO_CODE.items()
    ]
    return (
        "CASE "
        f"WHEN {column_name}::text ~ '^[0-9]+$' THEN {column_name}::smallint "
        f"{' '.join(cases)} "
        "ELSE NULL END"
    )


def _pair_to_smallint_expr(column_name: str) -> str:
    cases = [
        f"WHEN upper({column_name}::text) = '{_escape_sql_value(pair.upper())}' THEN {code}"
        for pair, code in PAIR_TO_CODE.items()
    ]
    return (
        "CASE "
        f"WHEN {column_name}::text ~ '^[0-9]+$' THEN {column_name}::smallint "
        f"{' '.join(cases)} "
        "ELSE NULL END"
    )


def _column_type_name(conn: _CompatConnection, table: str, column: str) -> str | None:
    row = conn.execute(
        """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        """,
        (table, column),
    ).fetchone()
    if not row:
        return None
    return row[0]


def _ensure_smallint_column_with_mapping(
    conn: _CompatConnection,
    table: str,
    column: str,
    mapped_expr: str,
    allowed_values: set[int] | None = None,
) -> None:
    current_type = _column_type_name(conn, table, column)
    if current_type is None or current_type == 'smallint':
        return

    if conn.execute(f"SELECT 1 FROM {table} WHERE ({mapped_expr}) IS NULL LIMIT 1").fetchone() is not None:
        raise RuntimeError(f'Unmapped values in {table}.{column} for smallint migration')

    if allowed_values is not None and conn.execute(
        f"""
        SELECT 1 FROM {table}
        WHERE ({mapped_expr}) NOT IN ({','.join(str(v) for v in sorted(allowed_values))})
        LIMIT 1
        """,
    ).fetchone() is not None:
        raise RuntimeError(f'Out-of-range values in {table}.{column} for smallint migration')

    conn.execute(
        f"ALTER TABLE {table} ALTER COLUMN {column} TYPE SMALLINT USING ({mapped_expr})"
    )


def _migrate_legacy_postgres_schema(conn: _CompatConnection) -> None:
    _ensure_smallint_column_with_mapping(
        conn,
        'ohlc',
        'ticker',
        _ticker_to_smallint_expr('ticker'),
    )
    _ensure_smallint_column_with_mapping(
        conn,
        'ohlc',
        'interval',
        _interval_to_smallint_expr('interval'),
        allowed_values=set(INTERVAL_TO_CODE.values()),
    )
    _ensure_smallint_column_with_mapping(
        conn,
        'l2_snapshot',
        'ticker',
        _ticker_to_smallint_expr('ticker'),
    )
    _ensure_smallint_column_with_mapping(
        conn,
        'l2_snapshot',
        'pair',
        _pair_to_smallint_expr('pair'),
        allowed_values=set(PAIR_TO_CODE.values()),
    )
    _ensure_smallint_column_with_mapping(
        conn,
        'backtest_result',
        'ticker',
        _ticker_to_smallint_expr('ticker'),
    )
    conn.execute("""
        ALTER TABLE l2_snapshot
            DROP CONSTRAINT IF EXISTS l2_snapshot_ticker_chk
    """)
    conn.execute("ALTER TABLE backtest_result DROP CONSTRAINT IF EXISTS backtest_result_ticker_chk")
    conn.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'l2_snapshot'::regclass
                  AND conname = 'l2_snapshot_ticker_chk'
            ) THEN
                ALTER TABLE l2_snapshot
                    ADD CONSTRAINT l2_snapshot_ticker_chk
                    CHECK (ticker > 0);
            END IF;
        END
        $$;
    """)
    conn.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'backtest_result'::regclass
                  AND conname = 'backtest_result_ticker_chk'
            ) THEN
                ALTER TABLE backtest_result
                    ADD CONSTRAINT backtest_result_ticker_chk
                    CHECK (ticker > 0);
            END IF;
        END
        $$;
    """)
    conn.execute("""
        ALTER TABLE ohlc
            DROP CONSTRAINT IF EXISTS ohlc_interval_check
    """)
    conn.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'ohlc'::regclass
                  AND conname = 'ohlc_interval_check'
            ) THEN
                ALTER TABLE ohlc
                    ADD CONSTRAINT ohlc_interval_check
                    CHECK (interval IN (1, 2, 3));
            END IF;
        END
        $$;
    """)
    conn.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'l2_snapshot'
                  AND column_name = 'source'
            ) THEN
                ALTER TABLE l2_snapshot DROP COLUMN source;
            END IF;
        END
        $$;
    """)
    conn.execute("""
        ALTER TABLE l2_snapshot
            DROP CONSTRAINT IF EXISTS l2_snapshot_pair_chk
    """)
    conn.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'l2_snapshot'::regclass
                  AND conname = 'l2_snapshot_pair_chk'
            ) THEN
                ALTER TABLE l2_snapshot
                    ADD CONSTRAINT l2_snapshot_pair_chk
                    CHECK (pair > 0);
            END IF;
        END
        $$;
    """)


def init_db(db_path: str | None = None) -> None:
    """Create cache tables and apply schema migration where needed."""

    if db_path is None:
        db_path = get_db_path()
    db_path = _require_postgres_url(db_path)

    with _DB_SCHEMA_INIT_LOCK:
        if db_path in _DB_SCHEMA_READY:
            return

        conn = _connect(db_path)
        try:
            _init_postgres_schema(conn)
            _migrate_legacy_postgres_schema(conn)

            existing_columns = _table_columns(conn, 'backtest_result')
            if 'run_config_json' not in existing_columns:
                conn.execute('ALTER TABLE backtest_result ADD COLUMN run_config_json TEXT')
            conn.commit()
            _DB_SCHEMA_READY.add(db_path)
        finally:
            conn.close()


def _normalize_ts(ts: datetime | pd.Timestamp | str) -> str:
    """Convert a timestamp-like value to stable UTC string representation."""

    timestamp = pd.Timestamp(ts)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize('UTC')
    else:
        timestamp = timestamp.tz_convert('UTC')
    return str(timestamp)


def _ticker_to_db_value(conn: _CompatConnection, ticker: str) -> int:
    code = TICKER_TO_CODE.get(ticker)
    if code is None:
        raise ValueError(f'Unknown ticker for Postgres encoding: {ticker}')
    return code


def _pair_to_db_value(conn: _CompatConnection, pair: str) -> int:
    code = PAIR_TO_CODE.get(pair)
    if code is None:
        raise ValueError(f'Unknown pair for Postgres encoding: {pair}')
    return code


def _interval_to_db_value(conn: _CompatConnection, interval: str) -> int:
    code = INTERVAL_TO_CODE.get(interval)
    if code is None:
        raise ValueError(f'Unknown interval for Postgres encoding: {interval}')
    return code


def _ticker_from_db_value(conn: _CompatConnection | None, ticker_value: int | str) -> str:
    if isinstance(ticker_value, str):
        try:
            ticker_code = int(ticker_value)
        except ValueError as exc:
            raise ValueError(f'Invalid ticker code from Postgres: {ticker_value}') from exc
    else:
        ticker_code = int(ticker_value)
    return CODE_TO_TICKER.get(ticker_code, str(ticker_code))


def _interval_from_db_value(conn: _CompatConnection | None, interval_value: int | str) -> str:
    if isinstance(interval_value, str):
        try:
            interval_code = int(interval_value)
        except ValueError as exc:
            raise ValueError(f'Invalid interval code from Postgres: {interval_value}') from exc
    else:
        interval_code = int(interval_value)
    return CODE_TO_INTERVAL.get(interval_code, str(interval_code))


def _pair_from_db_value(conn: _CompatConnection | None, pair_value: int | str) -> str:
    if isinstance(pair_value, str):
        try:
            pair_code = int(pair_value)
        except ValueError as exc:
            raise ValueError(f'Invalid pair code from Postgres: {pair_value}') from exc
    else:
        pair_code = int(pair_value)
    return CODE_TO_PAIR.get(pair_code, str(pair_code))


def save_backtest_result(
    pair: str,
    params_hash: str,
    hourly_days: int,
    zone_history_days: int,
    data_signature: str,
    ticker: str,
    strategy_version: str,
    result_json: str,
    run_config_json: str | None = None,
    db_path: str | None = None,
) -> None:
    """Save or replace a cached backtest result."""

    if db_path is None:
        db_path = get_db_path()
    init_db(db_path)
    now = _normalize_ts(datetime.utcnow())

    delay = 0.1
    for attempt in range(5):
        conn = _connect(db_path)
        try:
            db_ticker = _ticker_to_db_value(conn, ticker)
            conn.execute(
                """
                INSERT INTO backtest_result (
                    pair, params_hash, hourly_days, zone_history_days, data_signature,
                    ticker, strategy_version, result_json, run_config_json, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(pair, params_hash, hourly_days, zone_history_days)
                DO UPDATE SET
                    data_signature = excluded.data_signature,
                    ticker = excluded.ticker,
                    strategy_version = excluded.strategy_version,
                    result_json = excluded.result_json,
                    run_config_json = excluded.run_config_json,
                    updated_at = excluded.updated_at
                """,
                (
                    pair,
                    params_hash,
                    int(hourly_days),
                    int(zone_history_days),
                    data_signature,
                    db_ticker,
                    strategy_version,
                    result_json,
                    run_config_json,
                    now,
                    now,
                ),
            )
            conn.commit()
            return
        except Exception:
            if attempt == 4:
                raise

            import time as _time

            _time.sleep(delay)
            delay = min(1.0, delay * 2)
        finally:
            conn.close()


def load_backtest_result(
    pair: str,
    params_hash: str,
    hourly_days: int,
    zone_history_days: int,
    db_path: str | None = None,
) -> tuple[str, str, str, str | None] | None:
    """Load cached backtest metadata and payload."""

    if db_path is None:
        db_path = get_db_path()
    if not _db_exists(db_path):
        return None
    init_db(db_path)

    conn = _connect(db_path)
    try:
        cursor = conn.execute(
            """
            SELECT data_signature, result_json, strategy_version, run_config_json
            FROM backtest_result
            WHERE pair=%s AND params_hash=%s AND hourly_days=%s AND zone_history_days=%s
            """,
            (pair, params_hash, int(hourly_days), int(zone_history_days)),
        )
        row = cursor.fetchone()
    finally:
        conn.close()

    if row is None:
        return None
    return row[0], row[1], row[2], row[3]


def load_backtest_results(
    pairs: Sequence[str] | None = None,
    db_path: str | None = None,
) -> list[dict]:
    """Load cached backtest rows for optional pair filters."""

    if db_path is None:
        db_path = get_db_path()
    if not _db_exists(db_path):
        return []
    init_db(db_path)

    query = """
        SELECT pair, params_hash, hourly_days, zone_history_days, data_signature,
            ticker, strategy_version, result_json, run_config_json, created_at, updated_at
        FROM backtest_result
    """
    params: list[object] = []
    if pairs is not None:
        pair_list = [p for p in pairs if p]
        if not pair_list:
            return []
        query += " WHERE pair IN ({})".format(",".join(["%s"] * len(pair_list)))
        params.extend(pair_list)

    query += " ORDER BY pair, updated_at DESC"

    conn = _connect(db_path)
    try:
        cursor = conn.execute(query, params)
        rows = cursor.fetchall()
    finally:
        conn.close()

    return [
        {
            'pair': row[0],
            'params_hash': row[1],
            'hourly_days': row[2],
            'zone_history_days': row[3],
            'data_signature': row[4],
            'ticker': _ticker_from_db_value(None, row[5]),
            'strategy_version': row[6],
            'result_json': row[7],
            'run_config_json': row[8],
            'created_at': row[9],
            'updated_at': row[10],
        }
        for row in rows
    ]


def delete_backtest_result(
    pair: str,
    params_hash: str,
    hourly_days: int,
    zone_history_days: int,
    db_path: str | None = None,
) -> None:
    """Delete one cached backtest result."""

    if db_path is None:
        db_path = get_db_path()
    if not _db_exists(db_path):
        return
    init_db(db_path)

    conn = _connect(db_path)
    try:
        conn.execute(
            """
            DELETE FROM backtest_result
            WHERE pair=%s AND params_hash=%s AND hourly_days=%s AND zone_history_days=%s
            """,
            (pair, params_hash, int(hourly_days), int(zone_history_days)),
        )
        conn.commit()
    finally:
        conn.close()


def save_ohlc(
    ticker: str,
    interval: str,
    df: pd.DataFrame,
    db_path: str | None = None,
):
    """Save a DataFrame of OHLC data to cache."""

    if df.empty:
        return

    if db_path is None:
        db_path = get_db_path()
    init_db(db_path)

    conn = _connect(db_path)
    try:
        db_ticker = _ticker_to_db_value(conn, ticker)
        db_interval = _interval_to_db_value(conn, interval)
        rows = []
        for ts, row in df.iterrows():
            ts_str = _normalize_ts(ts)
            rows.append(
                (
                    db_ticker,
                    db_interval,
                    ts_str,
                    float(row['Open']),
                    float(row['High']),
                    float(row['Low']),
                    float(row['Close']),
                    float(row.get('Volume', 0)),
                )
            )

        # Batch multi-row INSERT for performance (~50x faster than executemany
        # for large datasets like minute bars).
        BATCH_SIZE = 1000
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            placeholders = ', '.join(
                '(%s, %s, %s, %s, %s, %s, %s, %s)' for _ in batch
            )
            flat_params = [v for row in batch for v in row]
            conn.execute(
                f"""
                INSERT INTO ohlc (ticker, interval, ts, open, high, low, close, volume)
                VALUES {placeholders}
                ON CONFLICT (ticker, interval, ts) DO UPDATE
                SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
                """,
                flat_params,
            )
        conn.commit()
    finally:
        conn.close()


def load_ohlc(
    ticker: str,
    interval: str,
    start: datetime = None,
    end: datetime = None,
    db_path: str | None = None,
) -> pd.DataFrame:
    """Load OHLC data from cache."""

    if db_path is None:
        db_path = get_db_path()
    if not _db_exists(db_path):
        return pd.DataFrame()

    conn = _connect(db_path)
    try:
        db_ticker = _ticker_to_db_value(conn, ticker)
        db_interval = _interval_to_db_value(conn, interval)
        query = "SELECT ts, open, high, low, close, volume FROM ohlc WHERE ticker=%s AND interval=%s"
        params = [db_ticker, db_interval]

        if start is not None:
            query += " AND ts >= %s"
            params.append(_normalize_ts(start))
        if end is not None:
            query += " AND ts <= %s"
            params.append(_normalize_ts(end))

        query += " ORDER BY ts"
        cursor = conn.execute(query, params)
        rows = cursor.fetchall()
    finally:
        conn.close()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=['ts', 'Open', 'High', 'Low', 'Close', 'Volume'])
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.set_index("ts")
    return df


def get_cached_range(
    ticker: str,
    interval: str,
    db_path: str | None = None,
) -> Optional[Tuple[str, str, int]]:
    """Return cached first/last timestamp and row count."""

    if db_path is None:
        db_path = get_db_path()
    if not _db_exists(db_path):
        return None

    conn = _connect(db_path)
    try:
        db_ticker = _ticker_to_db_value(conn, ticker)
        db_interval = _interval_to_db_value(conn, interval)
        cursor = conn.execute(
            "SELECT MIN(ts), MAX(ts), COUNT(*) FROM ohlc WHERE ticker=%s AND interval=%s",
            (db_ticker, db_interval),
        )
        row = cursor.fetchone()
    finally:
        conn.close()

    if row is None or row[0] is None:
        return None
    return row[0], row[1], row[2]


def get_cache_summary(db_path: str | None = None) -> pd.DataFrame:
    """Get a summary of all cached OHLC data."""

    if db_path is None:
        db_path = get_db_path()
    if not _db_exists(db_path):
        return pd.DataFrame()

    conn = _connect(db_path)
    try:
        cursor = conn.execute(
            """
            SELECT ticker, interval, MIN(ts) AS first_ts, MAX(ts) AS last_ts, COUNT(*) AS bars
            FROM ohlc
            GROUP BY ticker, interval
            ORDER BY ticker, interval
            """
        )
        rows = cursor.fetchall()
        rows = [
            (
                _ticker_from_db_value(None, row[0]),
                _interval_from_db_value(None, row[1]),
                row[2],
                row[3],
                row[4],
            )
            for row in rows
        ]
    finally:
        conn.close()

    return pd.DataFrame(rows, columns=['ticker', 'interval', 'first_ts', 'last_ts', 'bars'])


def save_l2_snapshot(
    ticker: str,
    pair: str,
    captured_at: datetime | pd.Timestamp | str,
    bids: Sequence[dict],
    asks: Sequence[dict],
    depth_requested: int,
    mid_price: float | None = None,
    best_bid: float | None = None,
    best_ask: float | None = None,
    db_path: str | None = None,
) -> int:
    """Save one L2 snapshot and its level rows."""

    if db_path is None:
        db_path = get_db_path()
    init_db(db_path)

    best_bid_value = float(best_bid) if best_bid is not None else (float(bids[0]['price']) if bids else None)
    best_ask_value = float(best_ask) if best_ask is not None else (float(asks[0]['price']) if asks else None)
    spread = float(best_ask_value - best_bid_value) if best_bid_value is not None and best_ask_value is not None else None

    conn = _connect(db_path)
    try:
        db_ticker = _ticker_to_db_value(conn, ticker)
        db_pair = _pair_to_db_value(conn, pair)
        cursor = conn.execute(
            """
            INSERT INTO l2_snapshot (
                ticker, pair, ts, depth_requested,
                best_bid, best_ask, mid_price, spread, bid_levels, ask_levels
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                db_ticker,
                db_pair,
                _normalize_ts(captured_at),
                int(depth_requested),
                best_bid_value,
                best_ask_value,
                float(mid_price) if mid_price is not None else None,
                spread,
                len(bids),
                len(asks),
            ),
        )
        snapshot_row = cursor.fetchone()
        if not snapshot_row:
            raise RuntimeError("Failed to capture L2 snapshot ID")
        snapshot_id = int(snapshot_row[0])
        level_rows = []
        for side_name, side_levels in (('BID', bids), ('ASK', asks)):
            for level in side_levels:
                level_rows.append(
                    (
                        snapshot_id,
                        side_name,
                        int(level['level']),
                        float(level['price']),
                        float(level['size']) if level.get('size') is not None else None,
                        level.get('market_maker') or None,
                    )
                )

        if level_rows:
            conn.executemany(
                """
                INSERT INTO l2_level (
                    snapshot_id, side, level_no, price, size, market_maker
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (snapshot_id, side, level_no) DO UPDATE
                SET
                    price = EXCLUDED.price,
                    size = EXCLUDED.size,
                    market_maker = EXCLUDED.market_maker
                """,
                level_rows,
            )

        conn.commit()
        return snapshot_id
    finally:
        conn.close()


def load_l2_snapshots(
    ticker: str,
    start: datetime = None,
    end: datetime = None,
    limit: int | None = None,
    db_path: str | None = None,
) -> pd.DataFrame:
    """Load saved summary rows for L2 snapshots."""

    if db_path is None:
        db_path = get_db_path()
    if not _db_exists(db_path):
        return pd.DataFrame()

    conn = _connect(db_path)
    try:
        db_ticker = _ticker_to_db_value(conn, ticker)
        query = (
            "SELECT id AS snapshot_id, ticker, pair, ts, depth_requested, "
            "best_bid, best_ask, mid_price, spread, bid_levels, ask_levels "
            "FROM l2_snapshot WHERE ticker=%s"
        )
        params = [db_ticker]
        if start is not None:
            query += " AND ts >= %s"
            params.append(_normalize_ts(start))
        if end is not None:
            query += " AND ts <= %s"
            params.append(_normalize_ts(end))
        query += " ORDER BY ts"
        if limit is not None:
            query += " LIMIT %s"
            params.append(int(limit))

        cursor = conn.execute(query, params)
        rows = cursor.fetchall()
        rows = [
            (
                row[0],
                _ticker_from_db_value(None, row[1]),
                _pair_from_db_value(None, row[2]),
                row[3],
                row[4],
                row[5],
                row[6],
                row[7],
                row[8],
                row[9],
                row[10],
            )
            for row in rows
        ]
    finally:
        conn.close()

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(
        rows,
        columns=[
            'snapshot_id',
            'ticker',
            'pair',
            'ts',
            'depth_requested',
            'best_bid',
            'best_ask',
            'mid_price',
            'spread',
            'bid_levels',
            'ask_levels',
        ],
    )
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df.set_index("ts").sort_index()


def load_l2_levels(
    ticker: str,
    start: datetime = None,
    end: datetime = None,
    side: str | None = None,
    limit: int | None = None,
    db_path: str | None = None,
) -> pd.DataFrame:
    """Load saved per-level L2 rows."""

    if db_path is None:
        db_path = get_db_path()
    if not _db_exists(db_path):
        return pd.DataFrame()

    conn = _connect(db_path)
    try:
        db_ticker = _ticker_to_db_value(conn, ticker)
        query = (
            "SELECT s.id AS snapshot_id, s.ticker, s.pair, s.ts, l.side, "
            "l.level_no, l.price, l.size, l.market_maker "
            "FROM l2_level l "
            "JOIN l2_snapshot s ON s.id = l.snapshot_id "
            "WHERE s.ticker=%s"
        )
        params = [db_ticker]
        if start is not None:
            query += " AND s.ts >= %s"
            params.append(_normalize_ts(start))
        if end is not None:
            query += " AND s.ts <= %s"
            params.append(_normalize_ts(end))
        if side is not None:
            query += " AND l.side = %s"
            params.append(side.upper())
        query += " ORDER BY s.ts, l.side, l.level_no"
        if limit is not None:
            query += " LIMIT %s"
            params.append(int(limit))

        cursor = conn.execute(query, params)
        rows = cursor.fetchall()
        rows = [
            (
                row[0],
                _ticker_from_db_value(None, row[1]),
                _pair_from_db_value(None, row[2]),
                row[3],
                row[4],
                row[5],
                row[6],
                row[7],
                row[8],
            )
            for row in rows
        ]
    finally:
        conn.close()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        rows,
        columns=['snapshot_id', 'ticker', 'pair', 'ts', 'side', 'level_no', 'price', 'size', 'market_maker'],
    )
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df.set_index("ts").sort_index()


def get_l2_summary(
    ticker: str | None = None,
    db_path: str | None = None,
) -> pd.DataFrame:
    """Return aggregate L2 summary data."""

    if db_path is None:
        db_path = get_db_path()
    if not _db_exists(db_path):
        return pd.DataFrame()

    conn = _connect(db_path)
    try:
        db_ticker = None
        if ticker is not None:
            db_ticker = _ticker_to_db_value(conn, ticker)

        query = (
            "SELECT ticker, pair, COUNT(*) AS snapshots, MIN(ts) AS first_ts, "
            "MAX(ts) AS last_ts, MAX(depth_requested) AS max_depth, AVG(spread) AS avg_spread "
            "FROM l2_snapshot"
        )
        params: list[object] = []
        if ticker is not None:
            query += " WHERE ticker=%s"
            params.append(db_ticker)
        query += " GROUP BY ticker, pair ORDER BY ticker"
        cursor = conn.execute(query, params)
        rows = cursor.fetchall()
        rows = [
            (
                _ticker_from_db_value(None, row[0]),
                _pair_from_db_value(None, row[1]),
                row[2],
                row[3],
                row[4],
                row[5],
                row[6],
            )
            for row in rows
        ]
    finally:
        conn.close()

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(
        rows,
        columns=['ticker', 'pair', 'snapshots', 'first_ts', 'last_ts', 'max_depth', 'avg_spread'],
    )
