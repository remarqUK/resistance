"""Interactive Brokers data feed via ib_async (TWS connection).

Primary data source for historical and live FX data used by the strategy.
"""

import os
import threading

import pandas as pd
from typing import Optional

# IBKR pair mapping: our pair ID -> (symbol, currency)
# ib_async Forex('EURUSD') handles this automatically
PAIR_TO_IB = {
    'EURUSD': 'EURUSD',
    'USDJPY': 'USDJPY',
    'GBPUSD': 'GBPUSD',
    'USDCHF': 'USDCHF',
    'AUDUSD': 'AUDUSD',
    'USDCAD': 'USDCAD',
    'NZDUSD': 'NZDUSD',
    'EURGBP': 'EURGBP',
    'EURJPY': 'EURJPY',
    'GBPJPY': 'GBPJPY',
}

# Reverse: internal ticker/cache key -> our pair ID
TICKER_TO_PAIR = {
    'EURUSD=X': 'EURUSD',
    'JPY=X':    'USDJPY',
    'GBPUSD=X': 'GBPUSD',
    'CHF=X':    'USDCHF',
    'AUDUSD=X': 'AUDUSD',
    'CAD=X':    'USDCAD',
    'NZDUSD=X': 'NZDUSD',
    'EURGBP=X': 'EURGBP',
    'EURJPY=X': 'EURJPY',
    'GBPJPY=X': 'GBPJPY',
}

# TWS connection settings
DEFAULT_TWS_HOST = '127.0.0.1'
DEFAULT_TWS_PORT = 7497  # paper trading; 7496 for live
DEFAULT_TWS_CLIENT_ID = 60  # dedicated client ID for data fetching (50=RossCameron)


def _get_env_int(name: str, default: int) -> int:
    """Read an integer env var, falling back to default on empty/invalid values."""
    raw = os.getenv(name)
    if raw is None or raw == '':
        return default
    try:
        return int(raw)
    except ValueError:
        return default


TWS_HOST = os.getenv('IBKR_HOST', DEFAULT_TWS_HOST)
TWS_PORT = _get_env_int('IBKR_PORT', DEFAULT_TWS_PORT)
TWS_CLIENT_ID = _get_env_int('IBKR_CLIENT_ID', DEFAULT_TWS_CLIENT_ID)

# Thread-local connection state so each backtest worker can use its own
# IBKR client ID without clobbering peers in other threads.
_THREAD_STATE = threading.local()
_IBKR_LOCK = threading.RLock()


def _resolve_client_id(client_id: Optional[int] = None) -> int:
    """Return an explicit client ID or the configured default."""
    return TWS_CLIENT_ID if client_id is None else int(client_id)


def _get_thread_connection_state() -> tuple[object | None, bool, int | None]:
    """Return the current thread's cached IBKR connection state."""
    return (
        getattr(_THREAD_STATE, 'ib', None),
        getattr(_THREAD_STATE, 'connected', False),
        getattr(_THREAD_STATE, 'client_id', None),
    )


def _set_thread_connection_state(ib, connected: bool, client_id: Optional[int]) -> None:
    """Persist the current thread's IBKR connection state."""
    _THREAD_STATE.ib = ib
    _THREAD_STATE.connected = connected
    _THREAD_STATE.client_id = client_id


def configure_connection(
    host: Optional[str] = None,
    port: Optional[int] = None,
    client_id: Optional[int] = None,
) -> None:
    """Override IBKR connection defaults and reset this thread if changed."""
    global TWS_HOST, TWS_PORT, TWS_CLIENT_ID

    with _IBKR_LOCK:
        new_host = TWS_HOST if host is None else host
        new_port = TWS_PORT if port is None else int(port)
        new_client_id = TWS_CLIENT_ID if client_id is None else int(client_id)

        changed = (new_host, new_port, new_client_id) != (TWS_HOST, TWS_PORT, TWS_CLIENT_ID)
        TWS_HOST = new_host
        TWS_PORT = new_port
        TWS_CLIENT_ID = new_client_id

    if changed:
        disconnect()


def _get_connection(client_id: Optional[int] = None):
    """Get or create a TWS connection. Returns (ib, connected) tuple."""
    resolved_client_id = _resolve_client_id(client_id)
    ib, connected, active_client_id = _get_thread_connection_state()

    if connected and ib and ib.isConnected() and active_client_id == resolved_client_id:
        return ib, True

    try:
        from ib_async import IB
        if ib and ib.isConnected():
            ib.disconnect()
        ib = IB()
        ib.connect(TWS_HOST, TWS_PORT, clientId=resolved_client_id, timeout=5)
        _set_thread_connection_state(ib, True, resolved_client_id)
        return ib, True
    except Exception:
        _set_thread_connection_state(None, False, resolved_client_id)
        return None, False


def disconnect():
    """Cleanly disconnect the current thread from TWS."""
    ib, _, _ = _get_thread_connection_state()
    if ib and ib.isConnected():
        ib.disconnect()
    _set_thread_connection_state(None, False, None)


def is_available() -> bool:
    """Check if TWS connection is available."""
    _, connected = _get_connection()
    return connected


def _ticker_to_pair(ticker_symbol: str) -> Optional[str]:
    """Convert an internal ticker/cache key to our pair ID."""
    return TICKER_TO_PAIR.get(ticker_symbol)


def _make_contract(pair: str):
    """Create a qualified Forex contract."""
    from ib_async import Forex
    return Forex(pair)


def fetch_historical(
    ticker_symbol: str,
    interval: str,
    days: int,
    client_id: Optional[int] = None,
) -> Optional[pd.DataFrame]:
    """Fetch historical OHLC data from TWS.

    Args:
        ticker_symbol: Internal ticker/cache key (for example 'EURUSD=X')
        interval: '1d' or '1h'
        days: number of days of history

    Returns:
        DataFrame with OHLC columns and DatetimeIndex, or None if unavailable
    """
    pair = _ticker_to_pair(ticker_symbol)
    if not pair:
        return None

    ib, connected = _get_connection(client_id=client_id)
    if not connected:
        return None

    try:
        from ib_async import util

        contract = _make_contract(pair)
        ib.qualifyContracts(contract)

        # Map interval to IB bar size
        bar_size = '1 day' if interval == '1d' else '1 hour'

        # IB duration string
        if days <= 365:
            duration = f'{days} D'
        else:
            years = days // 365
            remaining = days % 365
            if remaining > 0:
                # IB doesn't support mixed units, use days
                duration = f'{days} D'
            else:
                duration = f'{years} Y'

        # For hourly data > 365 days, fetch in chunks to respect IB limits
        if interval == '1h' and days > 365:
            return _fetch_hourly_chunked(ib, contract, days)

        bars = ib.reqHistoricalData(
            contract,
            endDateTime='',
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow='MIDPOINT',
            useRTH=False,
            formatDate=2,
        )

        if not bars:
            return None

        df = util.df(bars)
        return _normalize_df(df)

    except Exception as e:
        print(f"    IBKR fetch failed for {pair} ({interval}): {e}")
        return None


def _fetch_hourly_chunked(ib, contract, total_days: int) -> Optional[pd.DataFrame]:
    """Fetch hourly data in chunks to avoid IB pacing limits."""
    from ib_async import util
    import time

    all_frames = []
    chunk_days = 300  # safe chunk size for hourly
    end_dt = ''
    remaining = total_days

    while remaining > 0:
        fetch_days = min(chunk_days, remaining)
        duration = f'{fetch_days} D'

        bars = ib.reqHistoricalData(
            contract,
            endDateTime=end_dt,
            durationStr=duration,
            barSizeSetting='1 hour',
            whatToShow='MIDPOINT',
            useRTH=False,
            formatDate=2,
        )

        if not bars:
            break

        df = util.df(bars)
        all_frames.insert(0, df)

        # Set end to oldest bar for next chunk
        end_dt = bars[0].date
        remaining -= fetch_days

        # Respect IB pacing: 15s between identical requests
        if remaining > 0:
            time.sleep(1)

    if not all_frames:
        return None

    combined = pd.concat(all_frames)
    combined = combined.drop_duplicates(subset=['date'], keep='first')
    combined = combined.sort_values('date')
    return _normalize_df(combined)


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize IB data into the strategy OHLCV DataFrame format."""
    if df.empty:
        return pd.DataFrame()

    # IB returns columns: date, open, high, low, close, volume, average, barCount
    df = df.copy()
    df.index = pd.to_datetime(df['date'], utc=True)
    df.index.name = None

    result = pd.DataFrame({
        'Open': df['open'].astype(float),
        'High': df['high'].astype(float),
        'Low': df['low'].astype(float),
        'Close': df['close'].astype(float),
        'Volume': df['volume'].astype(float) if 'volume' in df.columns else 0,
    })

    result = result[~result.index.duplicated(keep='first')]
    result = result.sort_index()
    result = result.dropna(subset=['Open', 'High', 'Low', 'Close'])

    return result


def fetch_positions() -> list:
    """Read current FX positions from TWS.

    Returns list of dicts:
        pair: str (our pair ID, e.g. 'EURUSD')
        size: float (positive=long base ccy, negative=short)
        avg_cost: float (average entry price)
    Only returns FX positions matching our tracked pairs.
    """
    with _IBKR_LOCK:
        ib, connected = _get_connection()
        if not connected:
            return []

        try:
            positions = ib.positions()
            # Build reverse map: 'EUR.USD' -> 'EURUSD', etc.
            local_to_pair = {}
            for pair_id in PAIR_TO_IB:
                local_sym = pair_id[:3] + '.' + pair_id[3:]
                local_to_pair[local_sym] = pair_id

            result = []
            for pos in positions:
                contract = pos.contract
                if contract.secType != 'CASH':
                    continue

                local_sym = getattr(contract, 'localSymbol', '')
                if not local_sym:
                    local_sym = contract.symbol + '.' + contract.currency

                pair_id = local_to_pair.get(local_sym)
                if pair_id and pos.position != 0:
                    result.append({
                        'pair': pair_id,
                        'size': float(pos.position),
                        'avg_cost': float(pos.avgCost),
                    })

            return result
        except Exception as e:
            print(f"    Warning: failed to read IBKR positions: {e}")
            return []


def fetch_latest_price(ticker_symbol: str) -> Optional[float]:
    """Fetch the latest mid price from TWS."""
    pair = _ticker_to_pair(ticker_symbol)
    if not pair:
        return None

    with _IBKR_LOCK:
        ib, connected = _get_connection()
        if not connected:
            return None

        try:
            contract = _make_contract(pair)
            ib.qualifyContracts(contract)

            # Request a snapshot
            ib.reqMktData(contract, '', True, False)
            ib.sleep(2)
            ticker = ib.ticker(contract)

            if ticker and ticker.bid and ticker.ask:
                mid = (ticker.bid + ticker.ask) / 2
                ib.cancelMktData(contract)
                return mid

            ib.cancelMktData(contract)
        except Exception:
            pass

    return None
