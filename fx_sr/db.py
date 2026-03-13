"""SQLite cache for OHLC and L2 depth data.

Stores daily and hourly candle data locally so that backtests
don't need to re-fetch from IBKR every run. Also stores optional
Level 2 depth snapshots for later analysis.
"""

import os
import sqlite3
from datetime import datetime
from typing import Optional, Sequence, Tuple
import time

import pandas as pd


DB_FILENAME = 'fx_data.db'


def _connect(db_path: str) -> sqlite3.Connection:
    """Open a SQLite connection, supporting URI-style in-memory paths."""
    return sqlite3.connect(
        db_path,
        uri=db_path.startswith('file:'),
        timeout=30.0,
    )


def _db_exists(db_path: str) -> bool:
    """Return True when the SQLite target can be opened."""
    return db_path.startswith('file:') or os.path.exists(db_path)


def get_db_path() -> str:
    """Return path to the SQLite database file (next to the project root)."""
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(project_root, DB_FILENAME)


def init_db(db_path: str = None):
    """Create the cache tables if they don't exist."""
    if db_path is None:
        db_path = get_db_path()
    conn = _connect(db_path)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ohlc (
                ticker   TEXT NOT NULL,
                interval TEXT NOT NULL,
                ts       TEXT NOT NULL,
                open     REAL,
                high     REAL,
                low      REAL,
                close    REAL,
                volume   REAL,
                PRIMARY KEY (ticker, interval, ts)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ohlc_lookup
            ON ohlc (ticker, interval, ts)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS l2_snapshot (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker           TEXT NOT NULL,
                pair             TEXT NOT NULL,
                ts               TEXT NOT NULL,
                source           TEXT NOT NULL DEFAULT 'IBKR',
                depth_requested  INTEGER NOT NULL,
                best_bid         REAL,
                best_ask         REAL,
                mid_price        REAL,
                spread           REAL,
                bid_levels       INTEGER NOT NULL DEFAULT 0,
                ask_levels       INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_l2_snapshot_lookup
            ON l2_snapshot (ticker, ts)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS l2_level (
                snapshot_id   INTEGER NOT NULL,
                side          TEXT NOT NULL,
                level_no      INTEGER NOT NULL,
                price         REAL NOT NULL,
                size          REAL,
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
                ticker             TEXT NOT NULL,
                strategy_version   TEXT NOT NULL,
                result_json        TEXT NOT NULL,
                run_config_json    TEXT,
                created_at         TEXT NOT NULL,
                updated_at         TEXT NOT NULL,
                PRIMARY KEY (pair, params_hash, hourly_days, zone_history_days)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_backtest_lookup
            ON backtest_result (pair, params_hash, hourly_days, zone_history_days)
        """)
        existing_columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(backtest_result)").fetchall()
        }
        if 'run_config_json' not in existing_columns:
            conn.execute("ALTER TABLE backtest_result ADD COLUMN run_config_json TEXT")
        conn.commit()
    finally:
        conn.close()


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
    db_path: str = None,
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
            conn.execute("PRAGMA busy_timeout = 30000")
            conn.execute(
                """
                INSERT INTO backtest_result (
                    pair, params_hash, hourly_days, zone_history_days, data_signature,
                    ticker, strategy_version, result_json, run_config_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    ticker,
                    strategy_version,
                    result_json,
                    run_config_json,
                    now,
                    now,
                ),
            )
            conn.commit()
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt == 4:
                raise
            time.sleep(delay)
            delay = min(1.0, delay * 2)
        finally:
            conn.close()


def load_backtest_result(
    pair: str,
    params_hash: str,
    hourly_days: int,
    zone_history_days: int,
    db_path: str = None,
) -> tuple[str, str, str, str | None] | None:
    """Load cached backtest metadata and payload.

    Returns:
        (data_signature, result_json, strategy_version, run_config_json) if cached, else None
    """
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
            WHERE pair=? AND params_hash=? AND hourly_days=? AND zone_history_days=?
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
    db_path: str = None,
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
        placeholders = ",".join("?" for _ in pair_list)
        query += f" WHERE pair IN ({placeholders})"
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
            'ticker': row[5],
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
    db_path: str = None,
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
            WHERE pair=? AND params_hash=? AND hourly_days=? AND zone_history_days=?
            """,
            (pair, params_hash, int(hourly_days), int(zone_history_days)),
        )
        conn.commit()
    finally:
        conn.close()


def _normalize_ts(ts: datetime | pd.Timestamp | str) -> str:
    """Convert a timestamp-like value to a stable UTC string."""
    timestamp = pd.Timestamp(ts)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize('UTC')
    else:
        timestamp = timestamp.tz_convert('UTC')
    return str(timestamp)


def save_ohlc(ticker: str, interval: str, df: pd.DataFrame, db_path: str = None):
    """Save a DataFrame of OHLC data to the cache.

    Args:
        ticker: Internal ticker/cache key (for example 'EURUSD=X')
        interval: '1d' or '1h'
        df: DataFrame with DatetimeIndex and Open/High/Low/Close/Volume columns
    """
    if df.empty:
        return

    if db_path is None:
        db_path = get_db_path()
    init_db(db_path)

    conn = _connect(db_path)
    try:
        rows = []
        for ts, row in df.iterrows():
            ts_str = str(ts)
            rows.append((
                ticker, interval, ts_str,
                float(row['Open']), float(row['High']),
                float(row['Low']), float(row['Close']),
                float(row.get('Volume', 0)),
            ))

        conn.executemany(
            "INSERT OR REPLACE INTO ohlc (ticker, interval, ts, open, high, low, close, volume) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    finally:
        conn.close()


def load_ohlc(
    ticker: str,
    interval: str,
    start: datetime = None,
    end: datetime = None,
    db_path: str = None,
) -> pd.DataFrame:
    """Load OHLC data from the cache.

    Args:
        ticker: Internal ticker/cache key
        interval: '1d' or '1h'
        start: earliest timestamp (inclusive), or None for all
        end: latest timestamp (inclusive), or None for all

    Returns:
        DataFrame with DatetimeIndex and OHLC columns, or empty DataFrame
    """
    if db_path is None:
        db_path = get_db_path()

    if not _db_exists(db_path):
        return pd.DataFrame()

    conn = _connect(db_path)
    try:
        query = "SELECT ts, open, high, low, close, volume FROM ohlc WHERE ticker=? AND interval=?"
        params = [ticker, interval]

        if start is not None:
            query += " AND ts >= ?"
            params.append(str(start))
        if end is not None:
            query += " AND ts <= ?"
            params.append(str(end))

        query += " ORDER BY ts"

        df = pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()

    if df.empty:
        return pd.DataFrame()

    df.index = pd.to_datetime(df['ts'], utc=True)
    df.index.name = None
    df = df.drop(columns=['ts'])
    df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

    return df


def get_cached_range(
    ticker: str,
    interval: str,
    db_path: str = None,
) -> Optional[Tuple[str, str, int]]:
    """Get the min/max timestamps and row count cached for a ticker+interval.

    Returns:
        (min_ts, max_ts, count) tuple, or None if no data cached
    """
    if db_path is None:
        db_path = get_db_path()

    if not _db_exists(db_path):
        return None

    conn = _connect(db_path)
    try:
        cursor = conn.execute(
            "SELECT MIN(ts), MAX(ts), COUNT(*) FROM ohlc WHERE ticker=? AND interval=?",
            (ticker, interval),
        )
        row = cursor.fetchone()
    finally:
        conn.close()

    if row is None or row[0] is None:
        return None

    return (row[0], row[1], row[2])


def get_cache_summary(db_path: str = None) -> pd.DataFrame:
    """Get a summary of all cached data."""
    if db_path is None:
        db_path = get_db_path()

    if not _db_exists(db_path):
        return pd.DataFrame()

    conn = _connect(db_path)
    try:
        df = pd.read_sql_query(
            "SELECT ticker, interval, MIN(ts) as first_ts, MAX(ts) as last_ts, COUNT(*) as bars "
            "FROM ohlc GROUP BY ticker, interval ORDER BY ticker, interval",
            conn,
        )
    finally:
        conn.close()
    return df


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
    source: str = 'IBKR',
    db_path: str = None,
) -> int:
    """Save one L2 order-book snapshot and its per-level rows."""
    if db_path is None:
        db_path = get_db_path()
    init_db(db_path)

    best_bid_value = (
        float(best_bid)
        if best_bid is not None
        else (float(bids[0]['price']) if bids else None)
    )
    best_ask_value = (
        float(best_ask)
        if best_ask is not None
        else (float(asks[0]['price']) if asks else None)
    )
    spread = (
        float(best_ask_value - best_bid_value)
        if best_bid_value is not None and best_ask_value is not None
        else None
    )

    conn = _connect(db_path)
    try:
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.execute(
            """
            INSERT INTO l2_snapshot (
                ticker, pair, ts, source, depth_requested,
                best_bid, best_ask, mid_price, spread, bid_levels, ask_levels
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker,
                pair,
                _normalize_ts(captured_at),
                source,
                int(depth_requested),
                best_bid_value,
                best_ask_value,
                float(mid_price) if mid_price is not None else None,
                spread,
                len(bids),
                len(asks),
            ),
        )
        snapshot_id = int(cursor.lastrowid)

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
                INSERT OR REPLACE INTO l2_level (
                    snapshot_id, side, level_no, price, size, market_maker
                )
                VALUES (?, ?, ?, ?, ?, ?)
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
    db_path: str = None,
) -> pd.DataFrame:
    """Load saved L2 snapshot summaries for one ticker."""
    if db_path is None:
        db_path = get_db_path()

    if not _db_exists(db_path):
        return pd.DataFrame()

    conn = _connect(db_path)
    try:
        query = (
            "SELECT id AS snapshot_id, ticker, pair, ts, source, depth_requested, "
            "best_bid, best_ask, mid_price, spread, bid_levels, ask_levels "
            "FROM l2_snapshot WHERE ticker=?"
        )
        params = [ticker]

        if start is not None:
            query += " AND ts >= ?"
            params.append(_normalize_ts(start))
        if end is not None:
            query += " AND ts <= ?"
            params.append(_normalize_ts(end))

        query += " ORDER BY ts"
        if limit is not None:
            query += " LIMIT ?"
            params.append(int(limit))

        df = pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()

    if df.empty:
        return pd.DataFrame()

    df.index = pd.to_datetime(df['ts'], utc=True)
    df.index.name = None
    return df.drop(columns=['ts'])


def load_l2_levels(
    ticker: str,
    start: datetime = None,
    end: datetime = None,
    side: str | None = None,
    limit: int | None = None,
    db_path: str = None,
) -> pd.DataFrame:
    """Load saved per-level L2 rows for one ticker."""
    if db_path is None:
        db_path = get_db_path()

    if not _db_exists(db_path):
        return pd.DataFrame()

    conn = sqlite3.connect(db_path)
    try:
        query = (
            "SELECT s.id AS snapshot_id, s.ticker, s.pair, s.ts, "
            "l.side, l.level_no, l.price, l.size, l.market_maker "
            "FROM l2_level l "
            "JOIN l2_snapshot s ON s.id = l.snapshot_id "
            "WHERE s.ticker=?"
        )
        params = [ticker]

        if start is not None:
            query += " AND s.ts >= ?"
            params.append(_normalize_ts(start))
        if end is not None:
            query += " AND s.ts <= ?"
            params.append(_normalize_ts(end))
        if side is not None:
            query += " AND l.side = ?"
            params.append(side.upper())

        query += " ORDER BY s.ts, l.side, l.level_no"
        if limit is not None:
            query += " LIMIT ?"
            params.append(int(limit))

        df = pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()

    if df.empty:
        return pd.DataFrame()

    df.index = pd.to_datetime(df['ts'], utc=True)
    df.index.name = None
    return df.drop(columns=['ts'])


def get_l2_summary(
    ticker: str | None = None,
    db_path: str = None,
) -> pd.DataFrame:
    """Return aggregate L2 cache coverage summary."""
    if db_path is None:
        db_path = get_db_path()

    if not _db_exists(db_path):
        return pd.DataFrame()

    conn = sqlite3.connect(db_path)
    try:
        query = (
            "SELECT ticker, pair, COUNT(*) AS snapshots, MIN(ts) AS first_ts, MAX(ts) AS last_ts, "
            "MAX(depth_requested) AS max_depth, AVG(spread) AS avg_spread "
            "FROM l2_snapshot"
        )
        params: list[object] = []
        if ticker is not None:
            query += " WHERE ticker=?"
            params.append(ticker)
        query += " GROUP BY ticker, pair ORDER BY ticker"
        df = pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()

    return df
