"""Interactive Brokers data feed via ib_async (TWS connection).

Primary data source for historical and live FX data used by the strategy.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from contextlib import contextmanager
import json
import os
import threading
import time
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET

from .db import _connect, get_db_path, init_db

import pandas as pd
from typing import Callable, Optional

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
    'AUDJPY': 'AUDJPY',
    'CADJPY': 'CADJPY',
    'CHFJPY': 'CHFJPY',
    'EURAUD': 'EURAUD',
    'EURCAD': 'EURCAD',
    'EURCHF': 'EURCHF',
    'GBPAUD': 'GBPAUD',
    'GBPCAD': 'GBPCAD',
    'GBPCHF': 'GBPCHF',
    'AUDNZD': 'AUDNZD',
    'NZDJPY': 'NZDJPY',
    'AUDCAD': 'AUDCAD',
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
    'AUDJPY=X': 'AUDJPY',
    'CADJPY=X': 'CADJPY',
    'CHFJPY=X': 'CHFJPY',
    'EURAUD=X': 'EURAUD',
    'EURCAD=X': 'EURCAD',
    'EURCHF=X': 'EURCHF',
    'GBPAUD=X': 'GBPAUD',
    'GBPCAD=X': 'GBPCAD',
    'GBPCHF=X': 'GBPCHF',
    'AUDNZD=X': 'AUDNZD',
    'NZDJPY=X': 'NZDJPY',
    'AUDCAD=X': 'AUDCAD',
}
PAIR_TO_TICKER = {pair: ticker for ticker, pair in TICKER_TO_PAIR.items()}

# TWS connection settings
DEFAULT_TWS_HOST = '127.0.0.1'
DEFAULT_TWS_PORT = 4002  # IB Gateway paper; 4001 for live (TWS: 7497/7496)
DEFAULT_TWS_CLIENT_ID = 60  # dedicated client ID for data fetching (50=RossCameron)


@dataclass(frozen=True)
class ExecutionQuote:
    """Fresh two-sided quote used for submit-time execution validation."""

    pair: str
    bid: float
    ask: float
    mid: float
    spread: float
    source: str
    captured_at: pd.Timestamp


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
_HISTORICAL_FETCH_CONCURRENCY = max(1, _get_env_int('IBKR_HISTORICAL_FETCH_CONCURRENCY', 2))
_HISTORICAL_FETCH_SEMAPHORE = threading.BoundedSemaphore(_HISTORICAL_FETCH_CONCURRENCY)
_HISTORICAL_REQUEST_GAP_SECONDS = max(0.0, _get_env_int('IBKR_HISTORICAL_REQUEST_GAP_MS', 200)) / 1000.0
_HISTORICAL_FETCH_SLOT_TIMEOUT_SECONDS = max(5.0, _get_env_int('IBKR_HISTORICAL_FETCH_SLOT_TIMEOUT_MS', 15000) / 1000.0)
_ALLOW_CLIENT_ID_FALLBACK = True
_LAST_FALLBACK_OFFSET = 0  # remember highest successful offset so new threads skip stale IDs


def log_order_event(
    *,
    function_name: str,
    action: str,
    pair: str | None = None,
    direction: str | None = None,
    request_data: dict | None = None,
    response_data: dict | None = None,
    error: str | None = None,
    order_ids: list[int] | None = None,
    duration_ms: float | None = None,
) -> None:
    """Write one row to order_audit_log. Fire-and-forget — never raises."""
    try:
        db_path = get_db_path()
        init_db(db_path)
        conn = _connect(db_path)
        try:
            conn.execute(
                """
                INSERT INTO order_audit_log
                    (function_name, pair, direction, action,
                     request_json, response_json, error, order_ids, duration_ms)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    function_name,
                    pair,
                    direction,
                    action,
                    json.dumps(request_data, default=str) if request_data else None,
                    json.dumps(response_data, default=str) if response_data else None,
                    error,
                    ','.join(str(i) for i in order_ids) if order_ids else None,
                    duration_ms,
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass  # Audit must never interfere with trading


@contextmanager
def _historical_fetch_slot(timeout_s: float | None = None):
    """Throttle concurrent historical requests across worker threads."""
    timeout = _HISTORICAL_FETCH_SLOT_TIMEOUT_SECONDS if timeout_s is None else timeout_s
    acquired = _HISTORICAL_FETCH_SEMAPHORE.acquire(timeout=timeout)
    if not acquired:
        raise TimeoutError(f'timed out waiting {timeout:.1f}s for historical fetch slot')
    try:
        yield
    finally:
        try:
            _HISTORICAL_FETCH_SEMAPHORE.release()
        except ValueError:
            pass


def set_historical_fetch_concurrency(max_concurrent: int | None = None) -> int:
    """Update or return the historical request concurrency limit."""
    global _HISTORICAL_FETCH_CONCURRENCY, _HISTORICAL_FETCH_SEMAPHORE
    if max_concurrent is None:
        return _HISTORICAL_FETCH_CONCURRENCY

    limit = max(1, int(max_concurrent))
    _HISTORICAL_FETCH_CONCURRENCY = limit
    _HISTORICAL_FETCH_SEMAPHORE = threading.BoundedSemaphore(limit)
    return limit


def set_client_id_fallback(enabled: bool) -> bool:
    """Enable or disable client-id fallback behavior."""

    global _ALLOW_CLIENT_ID_FALLBACK
    previous = _ALLOW_CLIENT_ID_FALLBACK
    _ALLOW_CLIENT_ID_FALLBACK = bool(enabled)
    return previous


def _respect_historical_request_gap() -> None:
    """Delay between consecutive historical requests to reduce TWS pacing pressure."""
    if _HISTORICAL_REQUEST_GAP_SECONDS > 0:
        time.sleep(_HISTORICAL_REQUEST_GAP_SECONDS)


def _is_retriable_historical_error(error: Exception) -> bool:
    """Return True when historical fetch errors are likely transient and worth retrying."""
    message = str(error).lower()
    return (
        'timeout' in message
        or 'timed out' in message
        or '366' in message
        or 'no historical data query found' in message
        or 'pacing violation' in message
    )


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
    """Persist the current thread's IBKR connection state.

    Disconnects any existing connection held by this thread before replacing it.
    """
    old_ib = getattr(_THREAD_STATE, 'ib', None)
    if old_ib is not None and old_ib is not ib:
        try:
            if old_ib.isConnected():
                old_ib.disconnect()
        except Exception:
            pass
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


def _get_connection_legacy(client_id: Optional[int] = None, retries: int = 3):
    """Get or create a TWS connection. Returns (ib, connected) tuple.

    Retries on client-ID-in-use errors (Error 326) to handle stale TWS sockets.
    """
    resolved_client_id = _resolve_client_id(client_id)
    ib, connected, active_client_id = _get_thread_connection_state()

    if connected and ib and ib.isConnected() and active_client_id == resolved_client_id:
        return ib, True

    from ib_async import IB

    for attempt in range(retries):
        try:
            if ib and ib.isConnected():
                ib.disconnect()
            ib = IB()
            ib.connect(TWS_HOST, TWS_PORT, clientId=resolved_client_id, timeout=5)
            _set_thread_connection_state(ib, True, resolved_client_id)
            return ib, True
        except Exception as exc:
            # Error 326 = client ID already in use — TWS hasn't released the old socket yet
            if 'client id' in str(exc).lower() or '326' in str(exc):
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
            _set_thread_connection_state(None, False, resolved_client_id)
            return None, False

    _set_thread_connection_state(None, False, resolved_client_id)
    return None, False


def _get_connection(client_id: Optional[int] = None, retries: int = 20):
    """Get or create a TWS connection. Returns (ib, connected) tuple.

    By default, retries client-id-in-use errors by walking up IDs from the requested base.
    When fallback is disabled, retries use only the exact requested client ID.
    """
    global _LAST_FALLBACK_OFFSET

    resolved_client_id = _resolve_client_id(client_id)
    max_attempts = max(1, int(retries))
    ib, connected, active_client_id = _get_thread_connection_state()

    # Reuse any working connection on this thread.  When fallback is enabled
    # accept any client ID; otherwise require an exact match.  This prevents
    # the cycle: connect on fallback ID 62 → next call resolves to 60 →
    # mismatch → disconnect 62 → fail on 60 → walk back to 62 → repeat.
    if connected and ib and ib.isConnected():
        if _ALLOW_CLIENT_ID_FALLBACK or active_client_id == resolved_client_id:
            return ib, True

    from ib_async import IB

    # Start from the highest known-good offset so new threads skip stale IDs
    # left behind by previous threads that died without disconnecting.
    start_offset = _LAST_FALLBACK_OFFSET if _ALLOW_CLIENT_ID_FALLBACK else 0

    for attempt in range(max_attempts):
        candidate_client_id = (
            resolved_client_id
            if not _ALLOW_CLIENT_ID_FALLBACK
            else resolved_client_id + start_offset + attempt
        )

        try:
            if ib and ib.isConnected():
                ib.disconnect()
            ib = IB()
            ib.connect(TWS_HOST, TWS_PORT, clientId=candidate_client_id, timeout=5)
            _set_thread_connection_state(ib, True, candidate_client_id)
            _LAST_FALLBACK_OFFSET = max(_LAST_FALLBACK_OFFSET, start_offset + attempt)
            return ib, True
        except Exception as exc:
            if attempt < max_attempts - 1:
                time.sleep(2)
                continue
            _set_thread_connection_state(None, False, candidate_client_id)
            return None, False

    _set_thread_connection_state(None, False, resolved_client_id)
    return None, False


_get_connection_legacy = _get_connection


def disconnect():
    """Cleanly disconnect the current thread from TWS."""
    ib, _, _ = _get_thread_connection_state()
    if ib and ib.isConnected():
        ib.disconnect()
    _set_thread_connection_state(None, False, None)


def is_available() -> bool:
    """Check if TWS connection can be established without consuming a thread slot."""

    base_client_id = _resolve_client_id()
    max_attempts = 20

    from ib_async import IB

    for attempt in range(max_attempts):
        probe = None
        candidate_client_id = (
            base_client_id
            if not _ALLOW_CLIENT_ID_FALLBACK
            else base_client_id + attempt
        )
        try:
            probe = IB()
            probe.connect(TWS_HOST, TWS_PORT, clientId=candidate_client_id, timeout=5)
            return True
        except Exception:
            # Try nearby client IDs in case the base ID is occupied.
            pass
        finally:
            if probe is not None:
                try:
                    probe.disconnect()
                except Exception:
                    pass

    return False


def get_execution_mode() -> str:
    """Return 'paper' or 'live' based on the configured TWS port."""

    if TWS_PORT == 7497:
        return 'paper'
    if TWS_PORT == 7496:
        return 'live'
    # Non-standard port — check if port looks like a paper gateway
    if TWS_PORT in (4002,):
        return 'paper'
    if TWS_PORT in (4001,):
        return 'live'
    return 'unknown'


def fetch_account_id() -> Optional[str]:
    """Return the first managed account ID from TWS, or None."""

    with _IBKR_LOCK:
        ib, connected = _get_connection()
        if not connected:
            return None
        try:
            accounts = ib.managedAccounts()
            if accounts:
                return accounts[0]
            return None
        except Exception:
            return None


def _ticker_to_pair(ticker_symbol: str) -> Optional[str]:
    """Convert an internal ticker/cache key to our pair ID."""
    return TICKER_TO_PAIR.get(ticker_symbol)


def _local_symbol_to_pair(local_symbol: str) -> Optional[str]:
    """Convert an IB local symbol like EUR.USD into our pair ID."""
    if not local_symbol:
        return None
    return PAIR_TO_IB.get(local_symbol.replace('.', ''))


def _make_contract(pair: str):
    """Create a qualified Forex contract."""
    from ib_async import Forex
    return Forex(pair)


def _pair_min_tick(pair: str) -> float:
    """Return a conservative fallback tick size for a pair."""

    from .config import PAIRS

    config = PAIRS.get((pair or '').upper())
    pip = float((config or {}).get('pip', 0.0001) or 0.0001)
    return pip / 2.0 if pip > 0 else 0.00005


def _contract_min_tick(ib, contract, pair: str) -> float:
    """Resolve the minimum price increment for an IBKR contract."""

    try:
        details = ib.reqContractDetails(contract) or []
    except Exception:
        details = []

    for detail in details:
        min_tick = float(getattr(detail, 'minTick', 0.0) or 0.0)
        if min_tick > 0:
            return min_tick
    return _pair_min_tick(pair)


def _round_price_to_tick(price: float, tick_size: float, mode: str = 'nearest') -> float:
    """Snap a price to the contract tick size using deterministic decimal math."""

    tick = Decimal(str(float(tick_size)))
    if tick <= 0:
        return float(price)

    value = Decimal(str(float(price))) / tick
    if mode == 'up':
        snapped = value.to_integral_value(rounding=ROUND_CEILING) * tick
    elif mode == 'down':
        snapped = value.to_integral_value(rounding=ROUND_FLOOR) * tick
    else:
        snapped = value.to_integral_value() * tick
    return float(snapped)


def _round_bracket_exit_prices(
    pair: str,
    direction: str,
    take_profit_price: float,
    stop_loss_price: float,
    *,
    ib=None,
    contract=None,
) -> tuple[float, float]:
    """Round TP/SL to valid IBKR ticks without making the trade less conservative."""

    tick_size = _contract_min_tick(ib, contract, pair) if ib is not None and contract is not None else _pair_min_tick(pair)
    normalized_direction = (direction or '').upper()
    if normalized_direction == 'LONG':
        tp_mode = 'down'
        sl_mode = 'up'
    else:
        tp_mode = 'up'
        sl_mode = 'down'

    return (
        _round_price_to_tick(take_profit_price, tick_size, tp_mode),
        _round_price_to_tick(stop_loss_price, tick_size, sl_mode),
    )


def _contract_to_pair(contract) -> Optional[str]:
    """Convert an IB contract object to our pair ID."""

    if contract is None or getattr(contract, 'secType', None) != 'CASH':
        return None

    local_sym = getattr(contract, 'localSymbol', '')
    if not local_sym:
        symbol = getattr(contract, 'symbol', '')
        currency = getattr(contract, 'currency', '')
        local_sym = f'{symbol}.{currency}' if symbol and currency else ''

    return _local_symbol_to_pair(local_sym)


def fetch_historical(
    ticker_symbol: str,
    interval: str,
    days: int,
    client_id: Optional[int] = None,
    end_datetime: datetime | pd.Timestamp | str | None = None,
) -> Optional[pd.DataFrame]:
    """Fetch historical OHLC data from TWS.

    Args:
        ticker_symbol: Internal ticker/cache key (for example 'EURUSD=X')
        interval: '1d' or '1h'
        days: number of days of history
        end_datetime: optional end timestamp for one bounded chunk

    Returns:
        DataFrame with OHLC columns and DatetimeIndex, or None if unavailable
    """
    pair = _ticker_to_pair(ticker_symbol)
    if not pair:
        return None

    ib, connected = _get_connection(client_id=client_id)
    if not connected:
        return None

    from ib_async import util

    contract = _make_contract(pair)
    try:
        ib.qualifyContracts(contract)
    except Exception as e:
        print(f'    IBKR fetch failed for {pair} ({interval}): {e}')
        return None

    # Map interval to IB bar size
    interval_map = {'1d': '1 day', '1h': '1 hour', '1m': '1 min'}
    bar_size = interval_map.get(interval, '1 hour')

    # IB duration string - minute data capped at 7 days (IB limit for 1-min bars)
    effective_days = min(days, 7) if interval == '1m' else days
    if effective_days <= 365:
        duration = f'{effective_days} D'
    else:
        years = effective_days // 365
        remaining = effective_days % 365
        if remaining > 0:
            duration = f'{effective_days} D'
        else:
            duration = f'{years} Y'

    # For hourly data > 365 days, fetch in chunks to respect IB limits
    if interval == '1h' and days > 365:
        return _fetch_hourly_chunked(ib, contract, days)

    request_end = _format_historical_end_datetime(end_datetime)

    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            with _historical_fetch_slot():
                bars = ib.reqHistoricalData(
                    contract,
                    endDateTime=request_end,
                    durationStr=duration,
                    barSizeSetting=bar_size,
                    whatToShow='MIDPOINT',
                    useRTH=False,
                    formatDate=2,
                )
            _respect_historical_request_gap()

            if not bars:
                return None

            df = util.df(bars)
            return _normalize_df(df)
        except Exception as e:
            if attempt < max_attempts and _is_retriable_historical_error(e):
                print(f'    IBKR fetch retry {attempt}/{max_attempts} for {pair} ({interval}): {e}')
                try:
                    ib.disconnect()
                except Exception:
                    pass
                _set_thread_connection_state(None, False, _resolve_client_id(client_id))
                ib, connected = _get_connection(client_id=client_id)
                if not connected:
                    break
                time.sleep(1.0 * attempt)
                continue

            print(f"    IBKR fetch failed for {pair} ({interval}): {e}")
            return None

    return None

def _format_historical_end_datetime(
    end_datetime: datetime | pd.Timestamp | str | None,
) -> str:
    """Return an IBKR-compatible historical request end timestamp."""

    if end_datetime in (None, ''):
        return ''
    if isinstance(end_datetime, str):
        return end_datetime

    ts = pd.Timestamp(end_datetime)
    if ts.tzinfo is None:
        ts = ts.tz_localize('UTC')
    else:
        ts = ts.tz_convert('UTC')
    return ts.strftime('%Y%m%d %H:%M:%S UTC')


def _fetch_hourly_chunked(ib, contract, total_days: int) -> Optional[pd.DataFrame]:
    """Fetch hourly data in chunks to avoid IB pacing limits."""
    from ib_async import util

    all_frames = []
    chunk_days = 300  # safe chunk size for hourly
    end_dt = ''
    remaining = total_days

    while remaining > 0:
        fetch_days = min(chunk_days, remaining)
        duration = f'{fetch_days} D'

        with _historical_fetch_slot():
            bars = ib.reqHistoricalData(
                contract,
                endDateTime=end_dt,
                durationStr=duration,
                barSizeSetting='1 hour',
                whatToShow='MIDPOINT',
                useRTH=False,
                formatDate=2,
            )
        _respect_historical_request_gap()

        if not bars:
            break

        df = util.df(bars)
        all_frames.insert(0, df)

        # Set end to oldest bar for next chunk
        end_dt = bars[0].date
        remaining -= fetch_days

        # Request spacing handled by _HISTORICAL_REQUEST_GAP_SECONDS.

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


def fetch_positions() -> list | None:
    """Read current FX positions from TWS.

    Returns list of dicts:
        pair: str (our pair ID, e.g. 'EURUSD')
        size: float (positive=long base ccy, negative=short)
        avg_cost: float (average entry price)
    Only returns FX positions matching our tracked pairs.
    Returns None when the broker connection is unavailable (distinct from
    an empty list which means "connected, no open positions").
    """
    with _IBKR_LOCK:
        ib, connected = _get_connection()
        if not connected:
            return None

        try:
            positions = ib.positions()
            result = []
            for pos in positions:
                pair_id = _contract_to_pair(getattr(pos, 'contract', None))
                if pair_id and pos.position != 0:
                    result.append({
                        'pair': pair_id,
                        'size': float(pos.position),
                        'avg_cost': float(pos.avgCost),
                    })

            return result
        except Exception as e:
            print(f"    Warning: failed to read IBKR positions: {e}")
            return None


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
            ticker = ib.reqMktData(contract, '', True, False)
            ib.sleep(2)

            mid = _ticker_mid_price(ticker or ib.ticker(contract))
            if mid is not None:
                ib.cancelMktData(contract)
                return mid

            ib.cancelMktData(contract)
        except Exception:
            pass

    return None


def _ticker_bid_ask(ticker) -> tuple[Optional[float], Optional[float]]:
    """Extract the best available bid/ask pair from an IB ticker."""

    if ticker is None:
        return None, None

    bid = getattr(ticker, 'bid', None)
    ask = getattr(ticker, 'ask', None)
    resolved_bid = float(bid) if bid is not None and bid > 0 else None
    resolved_ask = float(ask) if ask is not None and ask > 0 else None
    return resolved_bid, resolved_ask


def _ticker_mid_price(ticker) -> Optional[float]:
    """Extract the best available executable midpoint from an IB ticker."""

    if ticker is None:
        return None

    bid, ask = _ticker_bid_ask(ticker)
    if bid is not None and ask is not None and bid > 0 and ask > 0:
        return float((bid + ask) / 2.0)

    market_price = getattr(ticker, 'marketPrice', None)
    if callable(market_price):
        try:
            price = market_price()
            if price is not None and price > 0:
                return float(price)
        except Exception:
            pass

    for field_name in ('last', 'close'):
        price = getattr(ticker, field_name, None)
        if price is not None and price > 0:
            return float(price)

    return None


def _build_execution_quote(
    pair: str,
    *,
    bid: Optional[float],
    ask: Optional[float],
    source: str,
    captured_at: Optional[pd.Timestamp] = None,
) -> Optional[ExecutionQuote]:
    """Build a normalized two-sided execution quote when bid/ask are usable."""

    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        return None

    quote_time = pd.Timestamp.now(tz='UTC') if captured_at is None else pd.Timestamp(captured_at)
    if quote_time.tzinfo is None:
        quote_time = quote_time.tz_localize('UTC')
    else:
        quote_time = quote_time.tz_convert('UTC')

    spread = float(ask - bid)
    return ExecutionQuote(
        pair=pair,
        bid=float(bid),
        ask=float(ask),
        mid=float((bid + ask) / 2.0),
        spread=spread,
        source=source,
        captured_at=quote_time,
    )


def _extract_dom_levels(dom_levels, side: str, max_levels: int) -> list[dict]:
    """Normalize IB market-depth rows into plain dictionaries."""
    rows: list[dict] = []
    for level_no, dom_level in enumerate(list(dom_levels)[:max_levels], start=1):
        price = getattr(dom_level, 'price', None)
        if price is None or price <= 0:
            continue

        size = getattr(dom_level, 'size', None)
        rows.append(
            {
                'side': side,
                'level': level_no,
                'price': float(price),
                'size': float(size) if size is not None else None,
                'market_maker': getattr(dom_level, 'marketMaker', '') or '',
            }
        )
    return rows


def _build_market_depth_snapshot(pair: str, ticker, depth: int) -> Optional[dict]:
    """Build a serializable L2 snapshot from an IB market-depth ticker."""
    bids = _extract_dom_levels(getattr(ticker, 'domBids', []), 'BID', depth)
    asks = _extract_dom_levels(getattr(ticker, 'domAsks', []), 'ASK', depth)

    best_bid = bids[0]['price'] if bids else None
    if best_bid is None:
        ticker_bid, _ = _ticker_bid_ask(ticker)
        best_bid = ticker_bid

    best_ask = asks[0]['price'] if asks else None
    if best_ask is None:
        _, ticker_ask = _ticker_bid_ask(ticker)
        best_ask = ticker_ask

    mid_price = _ticker_mid_price(ticker)
    if mid_price is None and best_bid is not None and best_ask is not None:
        mid_price = float((best_bid + best_ask) / 2.0)

    if not bids and not asks and best_bid is None and best_ask is None and mid_price is None:
        return None

    spread = (
        float(best_ask - best_bid)
        if best_bid is not None and best_ask is not None
        else None
    )
    return {
        'pair': pair,
        'ticker': PAIR_TO_TICKER.get(pair, pair),
        'captured_at': pd.Timestamp.now(tz='UTC'),
        'depth_requested': int(depth),
        'best_bid': best_bid,
        'best_ask': best_ask,
        'mid_price': mid_price,
        'spread': spread,
        'bids': bids,
        'asks': asks,
    }


def fetch_market_depth_snapshot(
    ticker_symbol: str,
    depth: int = 5,
    wait_seconds: float = 2.0,
    client_id: Optional[int] = None,
) -> Optional[dict]:
    """Fetch a one-shot L2 snapshot from TWS."""
    pair = _ticker_to_pair(ticker_symbol)
    if not pair:
        return None

    with _IBKR_LOCK:
        ib, connected = _get_connection(client_id=client_id)
        if not connected or ib is None:
            return None

        contract = None
        try:
            contract = _make_contract(pair)
            ib.qualifyContracts(contract)
            ticker = ib.reqMktDepth(contract, numRows=max(int(depth), 1), isSmartDepth=False)

            deadline = time.monotonic() + max(float(wait_seconds), 0.1)
            snapshot = None
            while time.monotonic() < deadline:
                if hasattr(ib, 'waitOnUpdate'):
                    ib.waitOnUpdate(timeout=0.2)
                elif hasattr(ib, 'sleep'):
                    ib.sleep(0.2)
                else:
                    time.sleep(0.2)

                snapshot = _build_market_depth_snapshot(pair, ticker, max(int(depth), 1))
                if snapshot is not None and (snapshot['bids'] or snapshot['asks']):
                    break

            return snapshot
        except Exception as e:
            print(f"    Warning: failed to fetch L2 depth for {pair}: {e}")
            return None
        finally:
            if contract is not None:
                try:
                    ib.cancelMktDepth(contract, isSmartDepth=False)
                except Exception:
                    pass


def fetch_execution_quote(
    pair: str,
    prefer_depth: bool = True,
    depth: int = 1,
    wait_seconds: float = 1.0,
    client_id: Optional[int] = None,
) -> Optional[ExecutionQuote]:
    """Fetch a fresh two-sided quote for submit-time execution checks."""

    pair = (pair or '').upper()
    if pair not in PAIR_TO_IB:
        return None

    ticker_symbol = PAIR_TO_TICKER.get(pair)
    if prefer_depth and ticker_symbol is not None:
        depth_snapshot = fetch_market_depth_snapshot(
            ticker_symbol,
            depth=max(int(depth), 1),
            wait_seconds=wait_seconds,
            client_id=client_id,
        )
        if depth_snapshot is not None:
            quote = _build_execution_quote(
                pair,
                bid=depth_snapshot.get('best_bid'),
                ask=depth_snapshot.get('best_ask'),
                source='l2',
                captured_at=depth_snapshot.get('captured_at'),
            )
            if quote is not None:
                return quote

    with _IBKR_LOCK:
        ib, connected = _get_connection(client_id=client_id)
        if not connected or ib is None:
            return None

        contract = None
        try:
            contract = _make_contract(pair)
            ib.qualifyContracts(contract)
            ticker = ib.reqMktData(contract, '', True, False)

            deadline = time.monotonic() + max(float(wait_seconds), 0.1)
            while time.monotonic() < deadline:
                if hasattr(ib, 'waitOnUpdate'):
                    ib.waitOnUpdate(timeout=0.2)
                elif hasattr(ib, 'sleep'):
                    ib.sleep(0.2)
                else:
                    time.sleep(0.2)

                quote = _build_execution_quote(
                    pair,
                    bid=_ticker_bid_ask(ticker)[0],
                    ask=_ticker_bid_ask(ticker)[1],
                    source='l1',
                )
                if quote is not None:
                    return quote

            return None
        except Exception:
            return None
        finally:
            if contract is not None:
                try:
                    ib.cancelMktData(contract)
                except Exception:
                    pass


def stream_market_depth(
    pairs: list[str],
    on_snapshot: Callable[[dict], None],
    stop_event: threading.Event,
    depth: int = 5,
    interval_seconds: float = 1.0,
    client_id: Optional[int] = None,
) -> None:
    """Maintain IBKR depth subscriptions and emit periodic L2 snapshots."""
    if not pairs:
        return

    ib, connected = _get_connection(client_id=client_id)
    if not connected or ib is None:
        return

    subscriptions: list[tuple[object, object, str]] = []
    depth_rows = max(int(depth), 1)
    interval = max(float(interval_seconds), 0.1)

    try:
        for pair in pairs:
            try:
                contract = _make_contract(pair)
                ib.qualifyContracts(contract)
                ticker = ib.reqMktDepth(contract, numRows=depth_rows, isSmartDepth=False)
                ib.sleep(0.1)
                subscriptions.append((contract, ticker, pair))
            except Exception:
                continue

        if not subscriptions:
            print("    Warning: no market depth subscriptions succeeded")
            return

        next_emit = time.monotonic()
        while not stop_event.is_set():
            try:
                if hasattr(ib, 'waitOnUpdate'):
                    ib.waitOnUpdate(timeout=min(interval, 1.0))
                elif hasattr(ib, 'sleep'):
                    ib.sleep(min(interval, 1.0))
                else:
                    time.sleep(min(interval, 1.0))
            except Exception:
                time.sleep(min(interval, 1.0))

            if time.monotonic() < next_emit:
                continue
            next_emit = time.monotonic() + interval

            for _, ticker, pair in subscriptions:
                snapshot = _build_market_depth_snapshot(pair, ticker, depth_rows)
                if snapshot is None:
                    continue
                try:
                    on_snapshot(snapshot)
                except Exception:
                    continue
    finally:
        for contract, _, _ in subscriptions:
            try:
                ib.cancelMktDepth(contract, isSmartDepth=False)
            except Exception:
                continue
        disconnect()


def stream_live_quotes(
    pairs: list[str],
    on_price: Callable[[str, float], None],
    stop_event: threading.Event,
    client_id: Optional[int] = None,
) -> None:
    """Maintain live IBKR ticker subscriptions and invoke ``on_price`` on change."""

    if not pairs:
        return

    ib, connected = _get_connection(client_id=client_id)
    if not connected or ib is None:
        return

    # Cancel any stale subscriptions left over from a previous session
    for existing_ticker in list(ib.tickers()):
        try:
            ib.cancelMktData(existing_ticker.contract)
        except Exception:
            pass

    subscriptions: list[tuple[object, object, str]] = []
    last_prices: dict[str, float] = {}
    max_ticker_errors: set[str] = set()

    try:
        for pair in pairs:
            if pair in max_ticker_errors:
                continue
            try:
                contract = _make_contract(pair)
                ib.qualifyContracts(contract)
                ticker = ib.reqMktData(contract, '', False, False)
                # Give IB a moment to reject with Error 101 before continuing
                ib.sleep(0.1)
                subscriptions.append((contract, ticker, pair))
            except Exception:
                continue

        if not subscriptions:
            print("    Warning: no quote subscriptions succeeded (ticker limit reached)")
            return

        while not stop_event.is_set():
            try:
                if hasattr(ib, 'waitOnUpdate'):
                    ib.waitOnUpdate(timeout=1)
                elif hasattr(ib, 'sleep'):
                    ib.sleep(1)
                else:
                    time.sleep(1)
            except Exception:
                time.sleep(1)

            for _, ticker, pair in subscriptions:
                price = _ticker_mid_price(ticker)
                if price is None:
                    continue

                previous = last_prices.get(pair)
                if previous is not None and abs(previous - price) < 1e-12:
                    continue

                last_prices[pair] = price
                try:
                    on_price(pair, float(price))
                except Exception:
                    continue
    finally:
        for contract, _, _ in subscriptions:
            try:
                ib.cancelMktData(contract)
            except Exception:
                continue
        disconnect()


def stream_realtime_bars(
    pairs: list[str],
    on_bar: Callable[[str, object], None],
    stop_event: threading.Event,
    client_id: Optional[int] = None,
) -> None:
    """Stream 5-second MIDPOINT bars for all pairs via ``reqRealTimeBars``.

    Each bar is dispatched as ``on_bar(pair, bar)`` where ``bar`` has
    ``.time``, ``.open_``, ``.high``, ``.low``, ``.close``, ``.volume``.

    Blocks until ``stop_event`` is set.
    """

    if not pairs:
        return

    ib, connected = _get_connection(client_id=client_id)
    if not connected or ib is None:
        return

    subscriptions: list[tuple[object, object, str]] = []

    try:
        for pair in pairs:
            try:
                contract = _make_contract(pair)
                ib.qualifyContracts(contract)
                bars = ib.reqRealTimeBars(
                    contract,
                    barSize=5,
                    whatToShow='MIDPOINT',
                    useRTH=False,
                )
                ib.sleep(0.1)
                subscriptions.append((contract, bars, pair))
            except Exception:
                continue

        if not subscriptions:
            print("    Warning: no real-time bar subscriptions succeeded")
            return

        # Wire up the barUpdateEvent for each subscription
        def _make_handler(pair_id):
            def _handler(bars, has_new_bar):
                if has_new_bar and bars:
                    try:
                        on_bar(pair_id, bars[-1])
                    except Exception:
                        pass
            return _handler

        for _, bars, pair in subscriptions:
            bars.updateEvent += _make_handler(pair)

        while not stop_event.is_set():
            try:
                if hasattr(ib, 'waitOnUpdate'):
                    ib.waitOnUpdate(timeout=1)
                elif hasattr(ib, 'sleep'):
                    ib.sleep(1)
                else:
                    time.sleep(1)
            except Exception:
                time.sleep(1)
    finally:
        for contract, bars, _ in subscriptions:
            try:
                ib.cancelRealTimeBars(bars)
            except Exception:
                continue
        disconnect()


def fetch_account_net_liquidation() -> tuple[Optional[float], Optional[str]]:
    """Read account net liquidation from TWS.

    Prefers the BASE summary row because that is the broker's consolidated
    account value. The caller must provide the actual account currency if TWS
    only reports the literal string ``BASE``.
    """
    with _IBKR_LOCK:
        ib, connected = _get_connection()
        if not connected:
            return None, None

        try:
            summary = ib.accountSummary()
            base_value = None
            fallback_value = None
            fallback_currency = None

            for item in summary:
                if getattr(item, 'tag', None) != 'NetLiquidation':
                    continue

                try:
                    value = float(item.value)
                except (TypeError, ValueError):
                    continue

                currency = getattr(item, 'currency', None) or None
                if currency == 'BASE':
                    base_value = value
                elif fallback_value is None and currency:
                    fallback_value = value
                    fallback_currency = currency

            if base_value is not None:
                return base_value, 'BASE'
            if fallback_value is not None:
                return fallback_value, fallback_currency
            return None, None
        except Exception as e:
            print(f"    Warning: failed to read IBKR account summary: {e}")
            return None, None


def fetch_cash_balances() -> dict[str, float]:
    """Return cash balances by currency from IBKR account summary.

    Returns dict like ``{'GBP': 8934.99, 'USD': 64727.55, 'CHF': -63051.07}``.
    Excludes the BASE aggregate row and near-zero balances.
    """
    with _IBKR_LOCK:
        ib, connected = _get_connection()
        if not connected:
            return {}
        try:
            summary = ib.accountSummary()
        except Exception as exc:
            print(f"    Warning: failed to read cash balances: {exc}")
            return {}

    balances: dict[str, float] = {}
    for item in summary:
        if getattr(item, 'tag', None) == 'CashBalance' and getattr(item, 'currency', '') != 'BASE':
            try:
                val = float(item.value)
            except (ValueError, TypeError):
                continue
            if abs(val) > 0.001:
                balances[item.currency] = val
    return balances


def fetch_excess_liquidity() -> tuple[Optional[float], Optional[str]]:
    """Read ExcessLiquidity from the TWS account summary.

    Excess liquidity = equity - maintenance margin.
    When this hits zero, IBKR liquidates positions.
    """
    with _IBKR_LOCK:
        ib, connected = _get_connection()
        if not connected:
            return None, None

        try:
            summary = ib.accountSummary()
            base_value = None
            fallback_value = None
            fallback_currency = None

            for item in summary:
                if getattr(item, 'tag', None) != 'ExcessLiquidity':
                    continue
                try:
                    value = float(item.value)
                except (TypeError, ValueError):
                    continue

                currency = getattr(item, 'currency', None) or None
                if currency == 'BASE':
                    base_value = value
                elif fallback_value is None and currency:
                    fallback_value = value
                    fallback_currency = currency

            if base_value is not None:
                return base_value, 'BASE'
            if fallback_value is not None:
                return fallback_value, fallback_currency
            return None, None
        except Exception as e:
            print(f"    Warning: failed to read IBKR excess liquidity: {e}")
            return None, None


def _safe_float(value) -> Optional[float]:
    """Convert a whatIf field to float, handling IBKR sentinel values."""
    if value in (None, ''):
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    # IBKR uses Double.MAX_VALUE as a sentinel for unavailable fields
    if f > 1e300:
        return None
    return f


def _is_nonfatal_whatif_warning(error: Exception) -> bool:
    """Return True for IB warnings from whatIf that do not imply margin rejection."""

    text = str(error)
    if not text:
        return False
    normalized = text.lower()
    return (
        '10349' in normalized
        or 'order tif was set to day based on order preset' in normalized
    )


def whatif_margin_check(
    pair: str,
    direction: str,
    quantity: int,
) -> Optional[dict]:
    """Pre-flight margin check using IBKR's whatIf API.

    Returns margin impact without submitting an order.
    Returns None if the check fails or IBKR is unavailable.
    """
    if quantity <= 0:
        return None

    with _IBKR_LOCK:
        ib, connected = _get_connection()
        if not connected:
            return None

        try:
            from ib_async import MarketOrder

            contract = _make_contract(pair)
            ib.qualifyContracts(contract)

            action = 'BUY' if direction == 'LONG' else 'SELL'
            order = MarketOrder(action, int(quantity), tif='DAY')
            order.whatIf = True

            state = ib.whatIfOrder(contract, order)
            if state is None:
                return None

            # Extract all margin fields — IBKR may return '' for some
            raw = {
                'initMarginChange': getattr(state, 'initMarginChange', None),
                'maintMarginChange': getattr(state, 'maintMarginChange', None),
                'equityWithLoanAfter': getattr(state, 'equityWithLoanAfter', None),
                'equityWithLoanBefore': getattr(state, 'equityWithLoanBefore', None),
                'initMarginAfter': getattr(state, 'initMarginAfter', None),
                'maintMarginAfter': getattr(state, 'maintMarginAfter', None),
            }
            init_margin = _safe_float(raw['initMarginChange'])
            maint_margin = _safe_float(raw['maintMarginChange'])
            equity_after = _safe_float(raw['equityWithLoanAfter'])
            maint_after = _safe_float(raw['maintMarginAfter'])
            init_margin_after = _safe_float(raw['initMarginAfter'])
            equity_before = _safe_float(raw['equityWithLoanBefore'])

            print(f"    whatIf {pair} {direction} {quantity}: "
                  f"equity_before={equity_before} equity_after={equity_after} "
                  f"init_margin_change={init_margin} init_margin_after={init_margin_after} "
                  f"raw_initMarginAfter={raw['initMarginAfter']!r}")

            would_liquidate = False
            if equity_after is not None and maint_after is not None:
                would_liquidate = equity_after <= maint_after

            # Check if equity can cover the initial margin requirement
            margin_exceeded = False
            if equity_after is not None and init_margin_after is not None:
                margin_exceeded = equity_after < init_margin_after
            elif equity_before is not None and init_margin is not None:
                margin_exceeded = equity_before < init_margin

            return {
                'pair': pair,
                'direction': direction,
                'quantity': quantity,
                'init_margin_change': init_margin,
                'maint_margin_change': maint_margin,
                'equity_with_loan_after': equity_after,
                'init_margin_after': init_margin_after,
                'would_liquidate': would_liquidate,
                'margin_exceeded': margin_exceeded,
            }
        except Exception as e:
            if _is_nonfatal_whatif_warning(e):
                print(f"    Info: non-fatal whatIf warning for {pair} {direction} {quantity}: {e}")
                return {
                    'pair': pair,
                    'direction': direction,
                    'quantity': quantity,
                    'init_margin_change': None,
                    'maint_margin_change': None,
                    'equity_with_loan_after': None,
                    'init_margin_after': None,
                    'would_liquidate': False,
                    'margin_exceeded': False,
                }
            print(f"    Warning: whatIf margin check failed for {pair}: {e}")
            return None


def fetch_open_order_counts() -> dict[str, int]:
    """Return a dict of pair → count of active FX orders that are not terminal.

    Uses ``reqAllOpenOrders`` to see orders across all client sessions,
    not just the current one.  This is critical after a gateway restart
    where brackets placed by a previous session are still working.
    """
    terminal_statuses = {'Filled', 'Cancelled', 'ApiCancelled', 'Inactive'}

    with _IBKR_LOCK:
        ib, connected = _get_connection()
        if not connected:
            return {}

        try:
            trades = ib.reqAllOpenOrders()

            counts: dict[str, int] = {}
            for trade in trades:
                status = getattr(getattr(trade, 'orderStatus', None), 'status', '') or ''
                if status in terminal_statuses:
                    continue

                pair_id = _contract_to_pair(getattr(trade, 'contract', None))
                if pair_id:
                    counts[pair_id] = counts.get(pair_id, 0) + 1
            return counts
        except Exception as e:
            print(f"    Warning: failed to read IBKR open orders: {e}")
            return {}


def fetch_open_order_pairs() -> set[str]:
    """Return pairs with active FX orders that are not terminal."""
    return set(fetch_open_order_counts().keys())


def fetch_fx_fills(
    order_ids: Optional[set[int]] = None,
    *,
    pair: Optional[str] = None,
    since: Optional[datetime | pd.Timestamp | str] = None,
) -> list[dict]:
    """Return recent FX fills from IBKR, optionally filtered by order IDs."""

    with _IBKR_LOCK:
        ib, connected = _get_connection()
        if not connected:
            return []

        try:
            fills = ib.reqExecutions()
            since_ts = pd.Timestamp(since) if since is not None else None
            results: list[dict] = []
            for fill in fills:
                contract = getattr(fill, 'contract', None)
                pair_id = _contract_to_pair(contract)
                if pair_id is None:
                    continue
                if pair is not None and pair_id != pair:
                    continue

                execution = getattr(fill, 'execution', None)
                if execution is None:
                    continue
                order_id = getattr(execution, 'orderId', None)
                if order_ids and order_id not in order_ids:
                    continue

                fill_time = getattr(fill, 'time', None) or getattr(execution, 'time', None)
                fill_ts = pd.Timestamp(fill_time) if fill_time is not None else None
                if since_ts is not None and fill_ts is not None and fill_ts < since_ts:
                    continue

                commission_report = getattr(fill, 'commissionReport', None)
                commission = float(getattr(commission_report, 'commission', 0.0) or 0.0) if commission_report else 0.0
                commission_currency = getattr(commission_report, 'currency', '') or '' if commission_report else ''
                realized_pnl = getattr(commission_report, 'realizedPNL', None) if commission_report else None
                if realized_pnl is not None:
                    try:
                        realized_pnl = float(realized_pnl)
                    except (TypeError, ValueError):
                        realized_pnl = None

                results.append(
                    {
                        'pair': pair_id,
                        'order_id': int(order_id) if order_id is not None else None,
                        'side': getattr(execution, 'side', '') or '',
                        'price': float(getattr(execution, 'price', 0.0) or 0.0),
                        'avg_price': float(getattr(execution, 'avgPrice', 0.0) or 0.0),
                        'shares': float(getattr(execution, 'shares', 0.0) or 0.0),
                        'cum_qty': float(getattr(execution, 'cumQty', 0.0) or 0.0),
                        'order_ref': getattr(execution, 'orderRef', '') or '',
                        'time': fill_ts,
                        'exec_id': getattr(execution, 'execId', '') or '',
                        'commission': commission,
                        'commission_currency': commission_currency,
                        'realized_pnl': realized_pnl,
                    }
                )

            results.sort(
                key=lambda item: (
                    item['time'].timestamp()
                    if item['time'] is not None
                    else float('-inf')
                )
            )
            return results
        except Exception as e:
            print(f"    Warning: failed to read IBKR executions: {e}")
            return []


def fetch_completed_fx_orders(
    order_ids: Optional[set[int]] = None,
    *,
    pair: Optional[str] = None,
) -> list[dict]:
    """Return completed FX orders from IBKR, optionally filtered by order IDs."""

    with _IBKR_LOCK:
        ib, connected = _get_connection()
        if not connected:
            return []

        try:
            trades = ib.reqCompletedOrders(False)
            results: list[dict] = []
            for trade in trades:
                contract = getattr(trade, 'contract', None)
                pair_id = _contract_to_pair(contract)
                if pair_id is None:
                    continue
                if pair is not None and pair_id != pair:
                    continue

                order = getattr(trade, 'order', None)
                order_status = getattr(trade, 'orderStatus', None)
                order_id = getattr(order, 'orderId', None)
                if order_ids and order_id not in order_ids:
                    continue

                results.append(
                    {
                        'pair': pair_id,
                        'order_id': int(order_id) if order_id is not None else None,
                        'parent_id': getattr(order, 'parentId', None),
                        'order_ref': getattr(order, 'orderRef', '') or '',
                        'order_type': getattr(order, 'orderType', '') or '',
                        'action': getattr(order, 'action', '') or '',
                        'status': getattr(order_status, 'status', '') or '',
                        'avg_fill_price': float(getattr(order_status, 'avgFillPrice', 0.0) or 0.0),
                    }
                )

            return results
        except Exception as e:
            print(f"    Warning: failed to read IBKR completed orders: {e}")
            return []


def _float_or_zero(value) -> float:
    """Return a best-effort float for broker numeric fields."""

    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _extract_order_total_units(order, order_status) -> float:
    """Return the broker order's intended total units."""

    for candidate in (
        getattr(order, 'totalQuantity', None),
        getattr(order, 'total_quantity', None),
        getattr(order_status, 'totalQuantity', None),
        getattr(order_status, 'total_quantity', None),
    ):
        if candidate not in (None, ''):
            return _float_or_zero(candidate)
    return 0.0


def _extract_order_fill_state(order, order_status) -> tuple[float, float]:
    """Return filled and remaining units for one broker order snapshot."""

    total_units = _extract_order_total_units(order, order_status)
    filled_units = _float_or_zero(getattr(order_status, 'filled', None))
    remaining_units = _float_or_zero(getattr(order_status, 'remaining', None))

    if filled_units <= 0.0 and remaining_units > 0.0 and total_units > 0.0:
        filled_units = max(total_units - remaining_units, 0.0)
    if remaining_units <= 0.0 and total_units > 0.0 and filled_units >= 0.0:
        remaining_units = max(total_units - filled_units, 0.0)
    return filled_units, remaining_units


def _order_snapshot_from_trade(trade) -> Optional[dict]:
    """Normalize a broker trade object into one order-status snapshot."""

    contract = getattr(trade, 'contract', None)
    pair_id = _contract_to_pair(contract)
    if pair_id is None:
        return None

    order = getattr(trade, 'order', None)
    order_status = getattr(trade, 'orderStatus', None)
    order_id = getattr(order, 'orderId', None)
    total_units = _extract_order_total_units(order, order_status)
    filled_units, remaining_units = _extract_order_fill_state(order, order_status)
    return {
        'pair': pair_id,
        'order_id': int(order_id) if order_id is not None else None,
        'parent_id': getattr(order, 'parentId', None),
        'order_ref': getattr(order, 'orderRef', '') or '',
        'order_type': getattr(order, 'orderType', '') or '',
        'action': getattr(order, 'action', '') or '',
        'status': getattr(order_status, 'status', '') or '',
        'avg_fill_price': _float_or_zero(getattr(order_status, 'avgFillPrice', None)),
        'filled_units': filled_units,
        'remaining_units': remaining_units,
        'total_units': total_units,
    }


def fetch_fx_order_statuses(
    order_ids: Optional[set[int]] = None,
    *,
    pair: Optional[str] = None,
) -> list[dict]:
    """Return broker order snapshots for open and completed FX orders."""

    with _IBKR_LOCK:
        ib, connected = _get_connection()
        if not connected:
            return []

        try:
            snapshots: dict[int | None, dict] = {}

            if hasattr(ib, 'openTrades'):
                open_trades = ib.openTrades()
            else:
                open_trades = ib.trades()

            for trade in open_trades:
                snapshot = _order_snapshot_from_trade(trade)
                if snapshot is None:
                    continue
                order_id = snapshot.get('order_id')
                if order_ids and order_id not in order_ids:
                    continue
                if pair is not None and snapshot.get('pair') != pair:
                    continue
                snapshots[order_id] = snapshot

            completed_trades = ib.reqCompletedOrders(False)
            for trade in completed_trades:
                snapshot = _order_snapshot_from_trade(trade)
                if snapshot is None:
                    continue
                order_id = snapshot.get('order_id')
                if order_ids and order_id not in order_ids:
                    continue
                if pair is not None and snapshot.get('pair') != pair:
                    continue
                snapshots.setdefault(order_id, snapshot)

            return list(snapshots.values())
        except Exception as e:
            print(f"    Warning: failed to read IBKR FX order statuses: {e}")
            return []


def submit_fx_market_order(
    pair: str,
    direction: str,
    quantity: int,
    order_ref: str = '',
) -> Optional[dict]:
    """Submit a market FX order to TWS/IB Gateway."""
    if quantity <= 0:
        return None

    _audit_start = time.monotonic()

    with _IBKR_LOCK:
        ib, connected = _get_connection()
        if not connected:
            log_order_event(
                function_name='submit_fx_market_order',
                pair=pair,
                direction=direction,
                action='submit',
                request_data={
                    'quantity': quantity,
                    'order_ref': order_ref,
                },
                error='Not connected',
                duration_ms=(time.monotonic() - _audit_start) * 1000,
            )
            return None

        try:
            from ib_async import MarketOrder

            contract = _make_contract(pair)
            ib.qualifyContracts(contract)

            action = 'BUY' if direction == 'LONG' else 'SELL'
            order = MarketOrder(action, int(quantity), orderRef=order_ref)
            trade = ib.placeOrder(contract, order)
            if hasattr(ib, 'sleep'):
                ib.sleep(1)

            live_order = getattr(trade, 'order', None)
            order_status = getattr(trade, 'orderStatus', None)

            log_order_event(
                function_name='submit_fx_market_order',
                pair=pair,
                direction=direction,
                action='submit',
                request_data={
                    'quantity': quantity,
                    'order_ref': order_ref,
                },
                response_data={
                    'order_id': getattr(live_order, 'orderId', None),
                    'status': getattr(order_status, 'status', None),
                    'avg_fill_price': getattr(order_status, 'avgFillPrice', None),
                },
                order_ids=[getattr(live_order, 'orderId', None)],
                duration_ms=(time.monotonic() - _audit_start) * 1000,
            )
            return {
                'pair': pair,
                'direction': direction,
                'quantity': int(quantity),
                'order_id': getattr(live_order, 'orderId', None),
                'status': getattr(order_status, 'status', None),
                'avg_fill_price': getattr(order_status, 'avgFillPrice', None),
            }
        except Exception as e:
            print(f"    Warning: failed to submit FX order for {pair}: {e}")
            log_order_event(
                function_name='submit_fx_market_order',
                pair=pair,
                direction=direction,
                action='submit',
                request_data={
                    'quantity': quantity,
                    'order_ref': order_ref,
                },
                error=str(e),
                duration_ms=(time.monotonic() - _audit_start) * 1000,
            )
            return None


def neutralize_currency_balance(
    currency: str,
    amount: float,
    account_currency: str = 'GBP',
) -> Optional[dict]:
    """Submit a market FX order to neutralize a residual currency balance.

    Uses a CASH contract with symbol=currency, currency=account_currency so
    the quantity is expressed in the residual currency directly.  A positive
    amount means we hold excess currency and need to SELL it; negative means
    we owe it and need to BUY.
    """

    if abs(amount) < 1:
        return None

    quantity = int(abs(amount))
    action = 'SELL' if amount > 0 else 'BUY'
    order_ref = f'fxsr:neutralize:{currency}:{int(time.time() * 1000)}'

    _audit_start = time.monotonic()

    with _IBKR_LOCK:
        ib, connected = _get_connection()
        if not connected:
            log_order_event(
                function_name='neutralize_currency_balance',
                pair=f'{currency}{account_currency}',
                direction=action,
                action='submit',
                request_data={'currency': currency, 'amount': amount, 'quantity': quantity},
                error='Not connected',
                duration_ms=(time.monotonic() - _audit_start) * 1000,
            )
            return None

        try:
            from ib_async import Contract, MarketOrder

            # Try FXCONV first (pure cash conversion, no position created).
            # Fall back to IDEALPRO + immediate close if FXCONV unavailable
            # (e.g. paper trading accounts).
            from ib_async import Forex
            attempts = [
                (f'{account_currency.upper()}{currency.upper()}',
                 'BUY' if amount > 0 else 'SELL'),
                (f'{currency.upper()}{account_currency.upper()}', action),
            ]

            # Phase 1: try FXCONV
            contract = None
            for pair, try_action in attempts:
                c = Contract(
                    secType='CASH', symbol=pair[:3], currency=pair[3:],
                    exchange='FXCONV',
                )
                if ib.qualifyContracts(c) and c.conId > 0:
                    contract = c
                    action = try_action
                    break

            # Phase 2: fall back to IDEALPRO (Forex) if FXCONV not available.
            # IDEALPRO creates a "Virtual FX Position" but the cash converts.
            if contract is None:
                for pair, try_action in attempts:
                    c = Forex(pair)
                    if ib.qualifyContracts(c) and c.conId > 0:
                        contract = c
                        action = try_action
                        break

            if contract is None:
                log_order_event(
                    function_name='neutralize_currency_balance',
                    pair=f'{currency}{account_currency}',
                    direction=action,
                    action='submit',
                    request_data={'currency': currency, 'amount': amount, 'quantity': quantity},
                    error='Contract qualification failed',
                    duration_ms=(time.monotonic() - _audit_start) * 1000,
                )
                return None

            # Submit the conversion order.  IDEALPRO creates a "Virtual FX
            # Position" but the underlying cash balances DO convert.  We do
            # NOT close the virtual position — closing it would reverse the
            # cash conversion.  The virtual position is cosmetic.
            order = MarketOrder(action, 0, orderRef=order_ref)
            order.cashQty = quantity
            trade = ib.placeOrder(contract, order)
            if hasattr(ib, 'sleep'):
                ib.sleep(3)

            live_order = getattr(trade, 'order', None)
            order_status = getattr(trade, 'orderStatus', None)
            fill_status = getattr(order_status, 'status', None) or ''

            log_order_event(
                function_name='neutralize_currency_balance',
                pair=f'{currency}{account_currency}',
                direction=action,
                action='submit',
                request_data={'currency': currency, 'amount': amount, 'quantity': quantity},
                response_data={
                    'order_id': getattr(live_order, 'orderId', None),
                    'status': fill_status,
                    'avg_fill_price': getattr(order_status, 'avgFillPrice', None),
                    'exchange': contract.exchange,
                },
                order_ids=[getattr(live_order, 'orderId', None)],
                duration_ms=(time.monotonic() - _audit_start) * 1000,
            )
            return {
                'currency': currency,
                'action': action,
                'quantity': quantity,
                'order_id': getattr(live_order, 'orderId', None),
                'status': fill_status,
                'avg_fill_price': getattr(order_status, 'avgFillPrice', None),
                'exchange': contract.exchange,
            }
        except Exception as e:
            print(f"    Warning: failed to neutralize {currency} balance: {e}")
            log_order_event(
                function_name='neutralize_currency_balance',
                pair=f'{currency}{account_currency}',
                direction=action,
                action='submit',
                request_data={'currency': currency, 'amount': amount, 'quantity': quantity},
                error=str(e),
                duration_ms=(time.monotonic() - _audit_start) * 1000,
            )
            return None


def submit_fx_market_bracket_order(
    pair: str,
    direction: str,
    quantity: int,
    take_profit_price: float,
    stop_loss_price: float,
    order_ref: str = '',
    submit_bid: Optional[float] = None,
    submit_ask: Optional[float] = None,
) -> Optional[dict]:
    """Submit OCA bracket (TP + SL) first, then a market entry order.

    Brackets-first approach:
      Phase 1 – verify TP/SL are safely away from market, then place them
                as an OCA pair.
      Phase 2 – place the market entry order.
      Phase 3 – if partial fill, resize brackets to match filled quantity.
      Cleanup – if entry fails or doesn't fill, cancel the brackets.

    *submit_bid* / *submit_ask* are the caller's latest quote, used to
    verify that TP/SL prices are not immediately marketable before placing
    them as naked exit orders.

    Note: between bracket placement and entry fill there is a small
    window where the exit orders are live without a position.  If the
    market moves to one of those prices in that window, IBKR will
    execute it and open an unintended position.  The orphan sweep in
    ``sync_positions()`` is the safety net for this scenario.
    """

    if quantity <= 0:
        return None

    _audit_start = time.monotonic()

    # Pre-flight margin check via whatIf for the entry direction.
    margin = whatif_margin_check(pair, direction, quantity)
    if margin is not None and (margin.get('margin_exceeded') or margin.get('would_liquidate')):
        print(f"    Skipping bracket order for {pair}: "
              f"insufficient margin (equity_after={margin.get('equity_with_loan_after')}, "
              f"init_margin_after={margin.get('init_margin_after')})")
        log_order_event(
            function_name='submit_fx_market_bracket_order',
            pair=pair,
            direction=direction,
            action='submit',
            request_data={
                'quantity': quantity,
                'take_profit_price': take_profit_price,
                'stop_loss_price': stop_loss_price,
                'order_ref': order_ref,
            },
            error='margin_exceeded',
            duration_ms=(time.monotonic() - _audit_start) * 1000,
        )
        return None

    with _IBKR_LOCK:
        ib, connected = _get_connection()
        if not connected:
            return None

        # Track bracket order IDs so the outer except can cancel them
        # if an exception occurs after phase 1 succeeds.
        tp_order_id = None
        sl_order_id = None

        try:
            from ib_async import MarketOrder

            contract = _make_contract(pair)
            ib.qualifyContracts(contract)
            rounded_take_profit_price, rounded_stop_loss_price = _round_bracket_exit_prices(
                pair,
                direction,
                take_profit_price,
                stop_loss_price,
                ib=ib,
                contract=contract,
            )

            # ── Guard: verify TP/SL are safely away from market ──────
            # The brackets are placed before any position exists.  If a
            # price is already marketable, IBKR would execute it and open
            # an unintended position.
            if submit_bid is not None and submit_ask is not None:
                if direction == 'LONG':
                    # TP is SELL LIMIT — must be above ask; SL is SELL STOP — must be below bid
                    if rounded_take_profit_price <= submit_ask:
                        log_order_event(
                            function_name='submit_fx_market_bracket_order',
                            pair=pair, direction=direction, action='submit',
                            request_data={'tp': float(rounded_take_profit_price), 'ask': submit_ask},
                            error='TP price at or below ask — would fill immediately as naked exit',
                            duration_ms=(time.monotonic() - _audit_start) * 1000,
                        )
                        return None
                    if rounded_stop_loss_price >= submit_bid:
                        log_order_event(
                            function_name='submit_fx_market_bracket_order',
                            pair=pair, direction=direction, action='submit',
                            request_data={'sl': float(rounded_stop_loss_price), 'bid': submit_bid},
                            error='SL price at or above bid — would fill immediately as naked exit',
                            duration_ms=(time.monotonic() - _audit_start) * 1000,
                        )
                        return None
                else:
                    # TP is BUY LIMIT — must be below bid; SL is BUY STOP — must be above ask
                    if rounded_take_profit_price >= submit_bid:
                        log_order_event(
                            function_name='submit_fx_market_bracket_order',
                            pair=pair, direction=direction, action='submit',
                            request_data={'tp': float(rounded_take_profit_price), 'bid': submit_bid},
                            error='TP price at or above bid — would fill immediately as naked exit',
                            duration_ms=(time.monotonic() - _audit_start) * 1000,
                        )
                        return None
                    if rounded_stop_loss_price <= submit_ask:
                        log_order_event(
                            function_name='submit_fx_market_bracket_order',
                            pair=pair, direction=direction, action='submit',
                            request_data={'sl': float(rounded_stop_loss_price), 'ask': submit_ask},
                            error='SL price at or above ask — would fill immediately as naked exit',
                            duration_ms=(time.monotonic() - _audit_start) * 1000,
                        )
                        return None

            # ── Phase 1: place brackets (TP + SL) ─────────────────────
            oca_result = _submit_oca_bracket(
                ib, contract, pair, direction, int(quantity),
                rounded_take_profit_price, rounded_stop_loss_price,
                order_ref,
            )
            if oca_result is None:
                log_order_event(
                    function_name='submit_fx_market_bracket_order',
                    pair=pair,
                    direction=direction,
                    action='submit_bracket',
                    request_data={
                        'quantity': quantity,
                        'rounded_tp': float(rounded_take_profit_price),
                        'rounded_sl': float(rounded_stop_loss_price),
                    },
                    error='OCA bracket placement failed',
                    duration_ms=(time.monotonic() - _audit_start) * 1000,
                )
                return None

            tp_order_id = oca_result['take_profit_order_id']
            sl_order_id = oca_result['stop_loss_order_id']
            oca_group = oca_result.get('oca_group')

            log_order_event(
                function_name='submit_fx_market_bracket_order',
                pair=pair,
                direction=direction,
                action='submit_bracket',
                request_data={
                    'quantity': quantity,
                    'rounded_tp': float(rounded_take_profit_price),
                    'rounded_sl': float(rounded_stop_loss_price),
                    'oca_group': oca_group,
                },
                response_data={
                    'tp_order_id': tp_order_id,
                    'sl_order_id': sl_order_id,
                },
                order_ids=[tp_order_id, sl_order_id],
                duration_ms=(time.monotonic() - _audit_start) * 1000,
            )

            # ── Phase 2: place entry market order ──────────────────────
            action = 'BUY' if direction == 'LONG' else 'SELL'

            entry_order_id = ib.client.getReqId()
            entry_order = MarketOrder(
                action,
                int(quantity),
                orderId=entry_order_id,
                orderRef=order_ref,
                tif='GTC',
                transmit=True,
            )

            entry_trade = ib.placeOrder(contract, entry_order)

            # Wait up to 5 seconds for the market order to fill
            fill_timeout = 5
            for _ in range(fill_timeout):
                if hasattr(ib, 'sleep'):
                    ib.sleep(1)
                entry_status = getattr(entry_trade, 'orderStatus', None)
                status_str = getattr(entry_status, 'status', '') or ''
                if status_str in ('Filled', 'Cancelled', 'Inactive', 'ApiCancelled'):
                    break

            entry_status = getattr(entry_trade, 'orderStatus', None)
            status_str = getattr(entry_status, 'status', '') or ''
            filled = float(getattr(entry_status, 'filled', 0) or 0)
            if status_str not in ('Filled',) and filled <= 0:
                # Entry failed — cancel the brackets we placed in phase 1
                print(f"    Warning: {pair} market order not filled after {fill_timeout}s "
                      f"(status={status_str}), cancelling brackets")
                bracket_ids = {
                    int(oid) for oid in (tp_order_id, sl_order_id)
                    if oid is not None
                }
                if bracket_ids:
                    cancel_orders(bracket_ids, suppress_not_found=True)
                try:
                    ib.cancelOrder(getattr(entry_trade, 'order', entry_trade))
                    if hasattr(ib, 'sleep'):
                        ib.sleep(1)
                except Exception:
                    pass
                log_order_event(
                    function_name='submit_fx_market_bracket_order',
                    pair=pair,
                    direction=direction,
                    action='submit_entry',
                    request_data={
                        'quantity': quantity,
                        'order_ref': order_ref,
                    },
                    error=f'market order not filled after {fill_timeout}s (status={status_str}); brackets cancelled',
                    duration_ms=(time.monotonic() - _audit_start) * 1000,
                )
                return None

            # Extract entry fill state
            entry_live_order = getattr(entry_trade, 'order', None)
            entry_status = getattr(entry_trade, 'orderStatus', None)
            total_units = _extract_order_total_units(entry_live_order, entry_status)
            filled_units, remaining_units = _extract_order_fill_state(entry_live_order, entry_status)
            if total_units <= 0.0:
                total_units = float(quantity)
                if filled_units <= 0.0:
                    remaining_units = total_units

            log_order_event(
                function_name='submit_fx_market_bracket_order',
                pair=pair,
                direction=direction,
                action='submit_entry',
                request_data={
                    'quantity': quantity,
                    'order_ref': order_ref,
                },
                response_data={
                    'order_id': getattr(entry_live_order, 'orderId', None),
                    'status': getattr(entry_status, 'status', None),
                    'avg_fill_price': getattr(entry_status, 'avgFillPrice', None),
                    'filled_units': filled_units,
                },
                order_ids=[getattr(entry_live_order, 'orderId', None)],
                duration_ms=(time.monotonic() - _audit_start) * 1000,
            )

            # ── Phase 3: resize brackets on partial fill ───────────────
            actual_filled = int(filled_units) if filled_units and filled_units > 0 else int(quantity)
            if actual_filled < int(quantity):
                # Cancel oversized brackets and resubmit at filled quantity.
                old_bracket_ids = {
                    int(oid) for oid in (tp_order_id, sl_order_id)
                    if oid is not None
                }
                if old_bracket_ids:
                    cancel_orders(old_bracket_ids, suppress_not_found=True)
                resized = _submit_oca_bracket(
                    ib, contract, pair, direction, actual_filled,
                    rounded_take_profit_price, rounded_stop_loss_price,
                    order_ref,
                )
                if resized is not None:
                    tp_order_id = resized['take_profit_order_id']
                    sl_order_id = resized['stop_loss_order_id']
                    log_order_event(
                        function_name='submit_fx_market_bracket_order',
                        pair=pair, direction=direction,
                        action='resize_bracket',
                        request_data={
                            'original_qty': int(quantity),
                            'filled_qty': actual_filled,
                            'oca_group': resized.get('oca_group'),
                        },
                        response_data={
                            'tp_order_id': tp_order_id,
                            'sl_order_id': sl_order_id,
                        },
                        order_ids=[tp_order_id, sl_order_id],
                        duration_ms=(time.monotonic() - _audit_start) * 1000,
                    )
                else:
                    # Resize failed — brackets are gone, position unprotected.
                    # Caller's _ensure_protection_orders_live will catch this.
                    tp_order_id = None
                    sl_order_id = None
                    log_order_event(
                        function_name='submit_fx_market_bracket_order',
                        pair=pair, direction=direction,
                        action='resize_bracket',
                        request_data={
                            'original_qty': int(quantity),
                            'filled_qty': actual_filled,
                        },
                        error='bracket resize failed after partial fill',
                        duration_ms=(time.monotonic() - _audit_start) * 1000,
                    )

            return {
                'pair': pair,
                'direction': direction,
                'quantity': int(quantity),
                'order_id': getattr(entry_live_order, 'orderId', None),
                'status': getattr(entry_status, 'status', None),
                'broker_status': getattr(entry_status, 'status', None),
                'avg_fill_price': getattr(entry_status, 'avgFillPrice', None),
                'filled_units': filled_units,
                'remaining_units': remaining_units,
                'total_units': total_units,
                'take_profit_order_id': tp_order_id,
                'stop_loss_order_id': sl_order_id,
                'take_profit_price': float(rounded_take_profit_price),
                'stop_loss_price': float(rounded_stop_loss_price),
            }
        except Exception as e:
            # Cancel any brackets placed in phase 1 before the exception.
            orphan_ids = {
                int(oid) for oid in (tp_order_id, sl_order_id)
                if oid is not None
            }
            if orphan_ids:
                try:
                    cancel_orders(orphan_ids, suppress_not_found=True)
                except Exception:
                    pass
            log_order_event(
                function_name='submit_fx_market_bracket_order',
                pair=pair,
                direction=direction,
                action='submit',
                request_data={
                    'quantity': quantity,
                    'take_profit_price': take_profit_price,
                    'stop_loss_price': stop_loss_price,
                    'order_ref': order_ref,
                },
                error=str(e),
                duration_ms=(time.monotonic() - _audit_start) * 1000,
            )
            print(f"    Warning: failed to submit FX bracket order for {pair}: {e}")
            return None


def _submit_oca_bracket(
    ib,
    contract,
    pair: str,
    direction: str,
    quantity: int,
    rounded_tp: float,
    rounded_sl: float,
    order_ref: str = '',
) -> Optional[dict]:
    """Submit OCA-linked TP limit + SL stop orders.

    Returns a dict with ``take_profit_order_id``, ``stop_loss_order_id``,
    ``take_profit_price``, ``stop_loss_price``, and ``oca_group`` — or
    ``None`` on failure.  The caller must already hold ``_IBKR_LOCK`` and
    have a live *ib* connection with a qualified *contract*.
    """
    from ib_async import LimitOrder, StopOrder

    close_action = 'SELL' if direction == 'LONG' else 'BUY'
    oca_group = f'bracket_{pair}_{ib.client.getReqId()}'

    tp_order = LimitOrder(
        close_action,
        int(quantity),
        float(rounded_tp),
        orderId=ib.client.getReqId(),
        orderRef=f'{order_ref}:tp' if order_ref else ':tp',
        tif='GTC',
        ocaGroup=oca_group,
        ocaType=1,
        transmit=True,
    )
    sl_order = StopOrder(
        close_action,
        int(quantity),
        float(rounded_sl),
        orderId=ib.client.getReqId(),
        orderRef=f'{order_ref}:sl' if order_ref else ':sl',
        tif='GTC',
        ocaGroup=oca_group,
        ocaType=1,
        transmit=True,
    )

    # Capture any Error 202 reasons during placement
    _placement_errors: list[str] = []
    _orig_error = ib._onError

    def _capture_error(reqId, errorCode, errorString, contract=None):
        if errorCode == 202:
            _placement_errors.append(
                f'reqId={reqId} errorCode={errorCode} reason={errorString!r}'
            )
        _orig_error(reqId, errorCode, errorString, contract)

    ib.errorEvent -= _orig_error
    ib.errorEvent += _capture_error

    try:
        tp_trade = ib.placeOrder(contract, tp_order)
        sl_trade = ib.placeOrder(contract, sl_order)

        # Allow IBKR to process and send back status/errors
        if hasattr(ib, 'sleep'):
            ib.sleep(2)

        tp_live = getattr(tp_trade, 'order', None)
        sl_live = getattr(sl_trade, 'order', None)

        tp_id = getattr(tp_live, 'orderId', None)
        sl_id = getattr(sl_live, 'orderId', None)
        if tp_id is None or sl_id is None:
            return None

        # Verify both orders survived — check for immediate cancellation
        tp_status = getattr(getattr(tp_trade, 'orderStatus', None), 'status', '') or ''
        sl_status = getattr(getattr(sl_trade, 'orderStatus', None), 'status', '') or ''
        terminal = {'Cancelled', 'ApiCancelled', 'Inactive'}

        if tp_status in terminal or sl_status in terminal:
            reason_detail = '; '.join(_placement_errors) if _placement_errors else 'unknown'
            print(f"    OCA bracket rejected: TP status={tp_status}, SL status={sl_status}, "
                  f"errors=[{reason_detail}]")
            # Cancel the surviving leg to avoid orphans
            surviving = set()
            if tp_status not in terminal and tp_id is not None:
                surviving.add(int(tp_id))
            if sl_status not in terminal and sl_id is not None:
                surviving.add(int(sl_id))
            if surviving:
                try:
                    from ib_async import Order as _Order
                    for oid in surviving:
                        ib.cancelOrder(_Order(orderId=oid))
                except Exception:
                    pass
            return None

        return {
            'take_profit_order_id': tp_id,
            'stop_loss_order_id': sl_id,
            'take_profit_price': float(rounded_tp),
            'stop_loss_price': float(rounded_sl),
            'oca_group': oca_group,
        }
    finally:
        ib.errorEvent -= _capture_error
        ib.errorEvent += _orig_error


def submit_bracket_for_existing_position(
    pair: str,
    direction: str,
    quantity: int,
    take_profit_price: float,
    stop_loss_price: float,
    order_ref: str = '',
) -> Optional[dict]:
    """Submit standalone TP limit + SL stop orders for an existing position.

    Used to restore bracket protection after a gateway restart wipes
    working orders but preserves positions.  Unlike
    submit_fx_market_bracket_order this does NOT place a parent market
    order — the position already exists.
    """
    if quantity <= 0:
        return None

    _audit_start = time.monotonic()

    # Pre-flight margin check: these are closing orders for existing
    # positions, but IBKR's margin system may still reject them if the
    # account is near its margin limit.  The whatIf simulates the
    # closing side to verify IBKR will accept the order.
    close_direction = 'SHORT' if direction == 'LONG' else 'LONG'
    margin = whatif_margin_check(pair, close_direction, quantity)
    if margin is not None and (margin.get('margin_exceeded') or margin.get('would_liquidate')):
        print(f"    Skipping bracket resubmission for {pair}: "
              f"insufficient margin (equity_after={margin.get('equity_with_loan_after')}, "
              f"init_margin_after={margin.get('init_margin_after')})")
        log_order_event(
            function_name='submit_bracket_for_existing_position',
            pair=pair,
            direction=direction,
            action='resubmit',
            request_data={
                'quantity': quantity,
                'take_profit_price': take_profit_price,
                'stop_loss_price': stop_loss_price,
                'order_ref': order_ref,
            },
            error='margin_exceeded',
            duration_ms=(time.monotonic() - _audit_start) * 1000,
        )
        return None

    with _IBKR_LOCK:
        ib, connected = _get_connection()
        if not connected:
            return None

        try:
            contract = _make_contract(pair)
            ib.qualifyContracts(contract)
            rounded_tp, rounded_sl = _round_bracket_exit_prices(
                pair, direction, take_profit_price, stop_loss_price,
                ib=ib, contract=contract,
            )

            ref_prefix = f'{order_ref}:rebracket' if order_ref else 'rebracket'
            oca_result = _submit_oca_bracket(
                ib, contract, pair, direction, quantity,
                rounded_tp, rounded_sl, ref_prefix,
            )
            if oca_result is None:
                raise RuntimeError('OCA bracket submission returned None')

            result = {
                'pair': pair,
                'direction': direction,
                'take_profit_order_id': oca_result['take_profit_order_id'],
                'stop_loss_order_id': oca_result['stop_loss_order_id'],
                'take_profit_price': oca_result['take_profit_price'],
                'stop_loss_price': oca_result['stop_loss_price'],
            }
            log_order_event(
                function_name='submit_bracket_for_existing_position',
                pair=pair,
                direction=direction,
                action='resubmit',
                request_data={
                    'quantity': quantity,
                    'take_profit_price': take_profit_price,
                    'stop_loss_price': stop_loss_price,
                    'rounded_tp': float(rounded_tp),
                    'rounded_sl': float(rounded_sl),
                    'oca_group': oca_result.get('oca_group'),
                },
                response_data={
                    'tp_order_id': oca_result['take_profit_order_id'],
                    'sl_order_id': oca_result['stop_loss_order_id'],
                },
                order_ids=[
                    oca_result['take_profit_order_id'],
                    oca_result['stop_loss_order_id'],
                ],
                duration_ms=(time.monotonic() - _audit_start) * 1000,
            )
            return result
        except Exception as e:
            log_order_event(
                function_name='submit_bracket_for_existing_position',
                pair=pair,
                direction=direction,
                action='resubmit',
                request_data={
                    'quantity': quantity,
                    'take_profit_price': take_profit_price,
                    'stop_loss_price': stop_loss_price,
                    'order_ref': order_ref,
                },
                error=str(e),
                duration_ms=(time.monotonic() - _audit_start) * 1000,
            )
            print(f"    Warning: failed to resubmit brackets for {pair}: {e}")
            return None


def cancel_orders(order_ids: set[int], *, suppress_not_found: bool = False) -> list[int]:
    """Cancel one or more orders by ID. Returns the IDs that were successfully sent.

    When *suppress_not_found* is True, IBKR error 10147 (order not found) is
    silently swallowed — useful for cleaning up bracket children that may
    have already been filled or cancelled.
    """

    if not order_ids:
        return []

    _audit_start = time.monotonic()

    with _IBKR_LOCK:
        ib, connected = _get_connection()
        if not connected:
            log_order_event(
                function_name='cancel_orders',
                action='cancel',
                request_data={
                    'order_ids': sorted(order_ids),
                    'suppress_not_found': suppress_not_found,
                },
                order_ids=sorted(order_ids),
                error='Not connected',
                duration_ms=(time.monotonic() - _audit_start) * 1000,
            )
            return []

        # Temporarily swap ib_async's error handler to mute "order not found"
        if suppress_not_found:
            _orig = ib._onError
            def _filtered(reqId, errorCode, errorString, contract=None):
                if errorCode == 10147:
                    return
                _orig(reqId, errorCode, errorString, contract)
            ib.errorEvent -= _orig
            ib.errorEvent += _filtered

        cancelled: list[int] = []
        cancel_error = None
        try:
            from ib_async import Order

            for oid in order_ids:
                try:
                    order = Order(orderId=int(oid))
                    ib.cancelOrder(order)
                    cancelled.append(int(oid))
                except Exception as e:
                    print(f"    Warning: failed to cancel order {oid}: {e}")
                    cancel_error = str(e)
            if cancelled and hasattr(ib, 'sleep'):
                ib.sleep(1)
        except Exception as e:
            print(f"    Warning: cancel_orders failed: {e}")
            cancel_error = str(e)
        finally:
            if suppress_not_found:
                ib.errorEvent -= _filtered
                ib.errorEvent += _orig
        log_order_event(
            function_name='cancel_orders',
            action='cancel',
            request_data={
                'order_ids': sorted(order_ids),
                'suppress_not_found': suppress_not_found,
            },
            response_data={
                'cancelled': sorted(cancelled),
            },
            order_ids=sorted(order_ids),
            error=cancel_error,
            duration_ms=(time.monotonic() - _audit_start) * 1000,
        )
        return cancelled


# ---------------------------------------------------------------------------
# Flex Query — historical commission data
# ---------------------------------------------------------------------------

_FLEX_BASE_URL = "https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService"
_FLEX_TOKEN = "51883655659468721467564"
_FLEX_QUERY_ID = 1449104
_FLEX_DAILY_STATEMENT_QUERY_ID = 1454993


def _fetch_flex_xml(token: str, query_id: int, max_wait: int = 30) -> ET.Element:
    """Execute IBKR Flex Query and return parsed XML root element.

    Three-step process: SendRequest → poll GetStatement → return XML.
    """
    # Step 1: request the report
    send_url = f"{_FLEX_BASE_URL}.SendRequest?t={token}&q={query_id}&v=3"
    with urllib.request.urlopen(send_url, timeout=15) as resp:
        send_xml = ET.fromstring(resp.read())

    status = send_xml.findtext("Status", "")
    if status != "Success":
        error_msg = send_xml.findtext("ErrorMessage", "unknown error")
        raise RuntimeError(f"Flex SendRequest failed: {status} — {error_msg}")

    reference_code = send_xml.findtext("ReferenceCode", "")
    stmt_url = send_xml.findtext("Url", _FLEX_BASE_URL.replace("FlexStatementService", "FlexStatementService.GetStatement"))

    # Step 2: poll until the report is ready
    get_url = f"{stmt_url}?t={token}&q={reference_code}&v=3"
    deadline = time.time() + max_wait
    while True:
        with urllib.request.urlopen(get_url, timeout=15) as resp:
            raw = resp.read()

        root = ET.fromstring(raw)
        if root.tag == "FlexStatementResponse":
            poll_status = root.findtext("Status", "")
            if poll_status == "Statement generation in progress":
                if time.time() > deadline:
                    raise TimeoutError("Flex statement not ready after %ds" % max_wait)
                time.sleep(2)
                continue
            raise RuntimeError(f"Flex GetStatement failed: {poll_status} — {root.findtext('ErrorMessage', '')}")
        break  # got the actual report

    return root


def fetch_flex_commissions(
    token: str = None,
    query_id: int = None,
    *,
    max_wait: int = 30,
) -> list[dict]:
    """Fetch historical FX trade commissions via IBKR Flex Query.

    Returns a list of dicts (one per execution leg) with keys:
        pair, trade_date, side, quantity, price, commission,
        commission_currency, net_cash, realized_pnl, order_id, exec_id

    Credentials fall back to env vars IBKR_FLEX_TOKEN / IBKR_FLEX_QUERY_ID.
    """
    token = token or os.environ.get("IBKR_FLEX_TOKEN", _FLEX_TOKEN)
    query_id = query_id or int(os.environ.get("IBKR_FLEX_QUERY_ID", _FLEX_QUERY_ID))

    if not token:
        raise ValueError("IBKR Flex token required — pass token= or set IBKR_FLEX_TOKEN")

    root = _fetch_flex_xml(token, query_id, max_wait=max_wait)

    # Parse Trade elements
    results: list[dict] = []
    for trade in root.iter("Trade"):
        symbol = trade.get("symbol", "")
        # Normalise IBKR symbol to our pair ID (e.g. "EUR.USD" -> "EURUSD")
        pair_id = symbol.replace(".", "")

        raw_commission = trade.get("ibCommission", "0") or "0"
        try:
            commission = abs(float(raw_commission))  # IBKR reports as negative
        except ValueError:
            commission = 0.0

        raw_pnl = trade.get("fifoPnlRealized", None)
        try:
            realized_pnl = float(raw_pnl) if raw_pnl is not None else None
        except ValueError:
            realized_pnl = None

        trade_date_str = trade.get("tradeDate", "") or trade.get("reportDate", "")
        trade_time_str = trade.get("tradeTime", "")
        if trade_date_str and trade_time_str:
            try:
                trade_ts = pd.Timestamp(f"{trade_date_str} {trade_time_str}")
            except Exception:
                trade_ts = pd.Timestamp(trade_date_str) if trade_date_str else None
        elif trade_date_str:
            trade_ts = pd.Timestamp(trade_date_str) if trade_date_str else None
        else:
            trade_ts = None

        results.append({
            "pair": pair_id,
            "trade_date": trade_ts,
            "side": trade.get("buySell", ""),
            "quantity": float(trade.get("quantity", 0) or 0),
            "price": float(trade.get("tradePrice", 0) or 0),
            "commission": commission,
            "commission_currency": trade.get("ibCommissionCurrency", ""),
            "net_cash": float(trade.get("netCash", 0) or 0),
            "realized_pnl": realized_pnl,
            "order_id": trade.get("ibOrderID", ""),
            "exec_id": trade.get("ibExecID", ""),
        })

    results.sort(key=lambda r: r["trade_date"] or pd.Timestamp.min)
    return results


def fetch_flex_daily_statement(
    query_id: int | None = None,
    max_wait: int = 30,
) -> dict:
    """Fetch daily account statement from IBKR Flex Query.

    Returns structured dict with trades, interest, fees, and cash summary.
    Requires a Flex Query configured with Trade, InterestAccrual,
    CashTransaction sections.  Set IBKR_FLEX_DAILY_STATEMENT_QUERY_ID.
    """
    token = os.environ.get("IBKR_FLEX_TOKEN", _FLEX_TOKEN).strip()
    if not token:
        raise ValueError("IBKR_FLEX_TOKEN not set")
    if query_id is None:
        query_id = int(os.environ.get("IBKR_FLEX_DAILY_STATEMENT_QUERY_ID", _FLEX_DAILY_STATEMENT_QUERY_ID))

    root = _fetch_flex_xml(token, query_id, max_wait=max_wait)

    # Parse trades
    trades: list[dict] = []
    total_commissions = 0.0
    total_realized_pnl = 0.0
    for el in root.iter("Trade"):
        raw_comm = el.get("ibCommission", "0") or "0"
        try:
            comm = abs(float(raw_comm))
        except ValueError:
            comm = 0.0
        raw_pnl = el.get("fifoPnlRealized")
        try:
            pnl = float(raw_pnl) if raw_pnl is not None else 0.0
        except ValueError:
            pnl = 0.0
        total_commissions += comm
        total_realized_pnl += pnl
        symbol = el.get("symbol", "").replace(".", "")
        trades.append({
            "pair": symbol,
            "date": el.get("tradeDate", el.get("reportDate", "")),
            "side": el.get("buySell", ""),
            "quantity": float(el.get("quantity", 0) or 0),
            "price": float(el.get("tradePrice", 0) or 0),
            "commission": comm,
            "commission_currency": el.get("ibCommissionCurrency", ""),
            "realized_pnl": pnl,
        })

    # Parse interest accruals
    interest: list[dict] = []
    total_interest = 0.0
    for el in root.iter("InterestAccrual"):
        raw_amt = el.get("endingAccrualBalance") or el.get("accruedInterest") or "0"
        try:
            amt = float(raw_amt)
        except ValueError:
            amt = 0.0
        total_interest += amt
        interest.append({
            "currency": el.get("currency", ""),
            "amount": amt,
            "date": el.get("toDate", el.get("reportDate", "")),
        })

    # Parse cash transactions (fees, interest payments, deposits, etc.)
    cash_transactions: list[dict] = []
    total_fees = 0.0
    for el in root.iter("CashTransaction"):
        raw_amt = el.get("amount", "0") or "0"
        try:
            amt = float(raw_amt)
        except ValueError:
            amt = 0.0
        tx_type = el.get("type", "")
        if "fee" in tx_type.lower() or "commission" in tx_type.lower():
            total_fees += abs(amt)
        cash_transactions.append({
            "type": tx_type,
            "currency": el.get("currency", ""),
            "amount": amt,
            "description": el.get("description", ""),
            "date": el.get("reportDate", el.get("dateTime", "")),
        })

    return {
        "trades": trades,
        "interest": interest,
        "cash_transactions": cash_transactions,
        "totals": {
            "total_commissions": round(total_commissions, 4),
            "total_interest": round(total_interest, 4),
            "total_fees": round(total_fees, 4),
            "total_realized_pnl": round(total_realized_pnl, 4),
        },
    }


