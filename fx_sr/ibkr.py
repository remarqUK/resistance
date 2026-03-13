"""Interactive Brokers data feed via ib_async (TWS connection).

Primary data source for historical and live FX data used by the strategy.
"""

import os
import threading
import time

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


def _get_connection(client_id: Optional[int] = None, retries: int = 3):
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


def _local_symbol_to_pair(local_symbol: str) -> Optional[str]:
    """Convert an IB local symbol like EUR.USD into our pair ID."""
    if not local_symbol:
        return None
    return PAIR_TO_IB.get(local_symbol.replace('.', ''))


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
        interval_map = {'1d': '1 day', '1h': '1 hour', '1m': '1 min'}
        bar_size = interval_map.get(interval, '1 hour')

        # IB duration string — minute data capped at 7 days (IB limit for 1-min bars)
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
            result = []
            for pos in positions:
                contract = pos.contract
                if contract.secType != 'CASH':
                    continue

                local_sym = getattr(contract, 'localSymbol', '')
                if not local_sym:
                    local_sym = contract.symbol + '.' + contract.currency

                pair_id = _local_symbol_to_pair(local_sym)
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


def _ticker_mid_price(ticker) -> Optional[float]:
    """Extract the best available executable midpoint from an IB ticker."""

    if ticker is None:
        return None

    bid = getattr(ticker, 'bid', None)
    ask = getattr(ticker, 'ask', None)
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
        ticker_bid = getattr(ticker, 'bid', None)
        if ticker_bid is not None and ticker_bid > 0:
            best_bid = float(ticker_bid)

    best_ask = asks[0]['price'] if asks else None
    if best_ask is None:
        ticker_ask = getattr(ticker, 'ask', None)
        if ticker_ask is not None and ticker_ask > 0:
            best_ask = float(ticker_ask)

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


def fetch_open_order_pairs() -> set[str]:
    """Return pairs with active FX orders that are not terminal."""
    terminal_statuses = {'Filled', 'Cancelled', 'ApiCancelled', 'Inactive'}

    with _IBKR_LOCK:
        ib, connected = _get_connection()
        if not connected:
            return set()

        try:
            if hasattr(ib, 'openTrades'):
                trades = ib.openTrades()
            else:
                trades = ib.trades()

            result = set()
            for trade in trades:
                status = getattr(getattr(trade, 'orderStatus', None), 'status', '') or ''
                if status in terminal_statuses:
                    continue

                contract = getattr(trade, 'contract', None)
                if contract is None or getattr(contract, 'secType', None) != 'CASH':
                    continue

                local_sym = getattr(contract, 'localSymbol', '')
                if not local_sym:
                    symbol = getattr(contract, 'symbol', '')
                    currency = getattr(contract, 'currency', '')
                    local_sym = f'{symbol}.{currency}' if symbol and currency else ''

                pair_id = _local_symbol_to_pair(local_sym)
                if pair_id:
                    result.add(pair_id)
            return result
        except Exception as e:
            print(f"    Warning: failed to read IBKR open orders: {e}")
            return set()


def submit_fx_market_order(
    pair: str,
    direction: str,
    quantity: int,
    order_ref: str = '',
) -> Optional[dict]:
    """Submit a market FX order to TWS/IB Gateway."""
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
            order = MarketOrder(action, int(quantity), orderRef=order_ref)
            trade = ib.placeOrder(contract, order)
            if hasattr(ib, 'sleep'):
                ib.sleep(1)

            live_order = getattr(trade, 'order', None)
            order_status = getattr(trade, 'orderStatus', None)

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
            return None


def submit_fx_market_bracket_order(
    pair: str,
    direction: str,
    quantity: int,
    take_profit_price: float,
    stop_loss_price: float,
    order_ref: str = '',
) -> Optional[dict]:
    """Submit a market-entry FX bracket order with attached TP/SL protection."""

    if quantity <= 0:
        return None

    with _IBKR_LOCK:
        ib, connected = _get_connection()
        if not connected:
            return None

        try:
            from ib_async import LimitOrder, MarketOrder, StopOrder

            contract = _make_contract(pair)
            ib.qualifyContracts(contract)

            action = 'BUY' if direction == 'LONG' else 'SELL'
            reverse_action = 'SELL' if action == 'BUY' else 'BUY'

            parent_order_id = ib.client.getReqId()
            parent = MarketOrder(
                action,
                int(quantity),
                orderId=parent_order_id,
                orderRef=order_ref,
                transmit=False,
            )
            take_profit = LimitOrder(
                reverse_action,
                int(quantity),
                float(take_profit_price),
                orderId=ib.client.getReqId(),
                parentId=parent_order_id,
                orderRef=f'{order_ref}:tp' if order_ref else '',
                transmit=False,
            )
            stop_loss = StopOrder(
                reverse_action,
                int(quantity),
                float(stop_loss_price),
                orderId=ib.client.getReqId(),
                parentId=parent_order_id,
                orderRef=f'{order_ref}:sl' if order_ref else '',
                transmit=True,
            )

            parent_trade = ib.placeOrder(contract, parent)
            tp_trade = ib.placeOrder(contract, take_profit)
            sl_trade = ib.placeOrder(contract, stop_loss)
            if hasattr(ib, 'sleep'):
                ib.sleep(1)

            parent_live_order = getattr(parent_trade, 'order', None)
            parent_status = getattr(parent_trade, 'orderStatus', None)
            tp_live_order = getattr(tp_trade, 'order', None)
            sl_live_order = getattr(sl_trade, 'order', None)

            return {
                'pair': pair,
                'direction': direction,
                'quantity': int(quantity),
                'order_id': getattr(parent_live_order, 'orderId', None),
                'status': getattr(parent_status, 'status', None),
                'avg_fill_price': getattr(parent_status, 'avgFillPrice', None),
                'take_profit_order_id': getattr(tp_live_order, 'orderId', None),
                'stop_loss_order_id': getattr(sl_live_order, 'orderId', None),
            }
        except Exception as e:
            print(f"    Warning: failed to submit FX bracket order for {pair}: {e}")
            return None
