"""SQLite cache for OHLC price data.

Stores daily and hourly candle data locally so that backtests
don't need to re-fetch from IBKR every run.
"""

import sqlite3
import os
from datetime import datetime, timedelta
from typing import Optional, Tuple

import pandas as pd


DB_FILENAME = 'fx_data.db'


def get_db_path() -> str:
    """Return path to the SQLite database file (next to the project root)."""
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(project_root, DB_FILENAME)


def init_db(db_path: str = None):
    """Create the OHLC table if it doesn't exist."""
    if db_path is None:
        db_path = get_db_path()
    conn = sqlite3.connect(db_path)
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
    conn.commit()
    conn.close()


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

    conn = sqlite3.connect(db_path)
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

    if not os.path.exists(db_path):
        return pd.DataFrame()

    conn = sqlite3.connect(db_path)

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

    if not os.path.exists(db_path):
        return None

    conn = sqlite3.connect(db_path)
    cursor = conn.execute(
        "SELECT MIN(ts), MAX(ts), COUNT(*) FROM ohlc WHERE ticker=? AND interval=?",
        (ticker, interval),
    )
    row = cursor.fetchone()
    conn.close()

    if row is None or row[0] is None:
        return None

    return (row[0], row[1], row[2])


def get_cache_summary(db_path: str = None) -> pd.DataFrame:
    """Get a summary of all cached data."""
    if db_path is None:
        db_path = get_db_path()

    if not os.path.exists(db_path):
        return pd.DataFrame()

    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        "SELECT ticker, interval, MIN(ts) as first_ts, MAX(ts) as last_ts, COUNT(*) as bars "
        "FROM ohlc GROUP BY ticker, interval ORDER BY ticker, interval",
        conn,
    )
    conn.close()
    return df
