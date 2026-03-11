from pathlib import Path


def read_text(path_str: str) -> str:
    return Path(path_str).read_text(encoding='utf-8-sig').replace('\r\n', '\n')


def write_text(path_str: str, text: str) -> None:
    Path(path_str).write_text(text, encoding='utf-8', newline='\n')


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old not in text:
        raise SystemExit(f'missing pattern for {label}')
    return text.replace(old, new, 1)


def replace_between(text: str, start_marker: str, end_marker: str, new_block: str, label: str) -> str:
    start = text.find(start_marker)
    if start == -1:
        raise SystemExit(f'missing start marker for {label}')
    end = text.find(end_marker, start)
    if end == -1:
        raise SystemExit(f'missing end marker for {label}')
    return text[:start] + new_block + text[end:]


write_text('run.py', read_text('run.py'))
write_text('fx_sr/data.py', read_text('fx_sr/data.py'))

backtest_text = read_text('fx_sr/backtest.py')
if 'from . import ibkr\n' not in backtest_text:
    backtest_text = replace_once(
        backtest_text,
        "from .strategy import (\n    Trade, StrategyParams, Signal, generate_signal, check_exit,\n    check_momentum_filter, get_correlated_pairs, get_market_exit_price,\n    BLOCKED_PAIR_DIRECTIONS,\n)\n",
        "from .strategy import (\n    Trade, StrategyParams, Signal, generate_signal, check_exit,\n    check_momentum_filter, get_correlated_pairs, get_market_exit_price,\n    BLOCKED_PAIR_DIRECTIONS,\n)\nfrom . import ibkr\n",
        'backtest import block',
    )
backtest_text = replace_once(
    backtest_text,
    "def _backtest_pair(\n    pair: str,\n    pair_info: dict,\n    params: StrategyParams,\n    hourly_days: int,\n    zone_history_days: int,\n    force_refresh: bool = False,\n) -> Tuple[str, Optional[BacktestResult]]:\n    \"\"\"Fetch data and run backtest for a single pair. Thread-safe.\"\"\"\n    daily_df = fetch_daily_data(\n        pair_info['ticker'],\n        days=zone_history_days + hourly_days,\n        force_refresh=force_refresh,\n        allow_stale_cache=not force_refresh,\n    )\n    hourly_df = fetch_hourly_data(\n        pair_info['ticker'],\n        days=hourly_days,\n        force_refresh=force_refresh,\n        allow_stale_cache=not force_refresh,\n    )\n    if daily_df.empty or hourly_df.empty:\n        return pair, None\n    result = run_backtest(daily_df, hourly_df, pair, params, zone_history_days)\n    return pair, result\n\n\n",
    "def _backtest_pair(\n    pair: str,\n    pair_info: dict,\n    params: StrategyParams,\n    hourly_days: int,\n    zone_history_days: int,\n    force_refresh: bool = False,\n    client_id: int | None = None,\n) -> Tuple[str, Optional[BacktestResult]]:\n    \"\"\"Fetch data and run backtest for a single pair. Thread-safe.\"\"\"\n    daily_df = fetch_daily_data(\n        pair_info['ticker'],\n        days=zone_history_days + hourly_days,\n        force_refresh=force_refresh,\n        allow_stale_cache=not force_refresh,\n        client_id=client_id,\n    )\n    hourly_df = fetch_hourly_data(\n        pair_info['ticker'],\n        days=hourly_days,\n        force_refresh=force_refresh,\n        allow_stale_cache=not force_refresh,\n        client_id=client_id,\n    )\n    if daily_df.empty or hourly_df.empty:\n        return pair, None\n    result = run_backtest(daily_df, hourly_df, pair, params, zone_history_days)\n    return pair, result\n\n\ndef _pair_client_id(base_client_id: int | None, offset: int) -> int | None:\n    \"\"\"Derive a stable client ID for a pair from the configured base.\"\"\"\n    if base_client_id is None:\n        return None\n    return int(base_client_id) + offset\n\n\n",
    'backtest pair helper',
)
backtest_text = replace_once(
    backtest_text,
    "def run_all_backtests_parallel(\n    params: StrategyParams = None,\n    hourly_days: int = 30,\n    zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS,\n    pairs: Dict = None,\n    force_refresh: bool = False,\n) -> Dict[str, BacktestResult]:\n    \"\"\"Run all pair backtests.\n\n    Cached runs execute in parallel for speed. Forced refresh runs execute\n    sequentially so IBKR pacing limits are respected.\n    \"\"\"\n    if params is None:\n        params = StrategyParams()\n    if pairs is None:\n        pairs = PAIRS\n\n    results = {}\n    total = len(pairs)\n    done = 0\n\n    if force_refresh:\n        print(f\"  Refreshing {total} backtests from IBKR/TWS sequentially...\")\n        for pair, info in pairs.items():\n            pair, result = _backtest_pair(\n                pair, info, params, hourly_days, zone_history_days, force_refresh\n            )\n            done += 1\n            if result:\n                results[pair] = result\n                r = result\n                print(f\"    [{done}/{total}] {pair}: {r.total_trades} trades, \"\n                      f\"{r.win_rate:.0f}% WR, {r.total_pnl_pips:+.0f} pips\")\n            else:\n                print(f\"    [{done}/{total}] {pair}: no data\")\n        return results\n\n    print(f\"  Launching {total} backtests in parallel (using cache when available)...\")\n\n    with ThreadPoolExecutor(max_workers=total) as executor:\n        futures = {\n            executor.submit(\n                _backtest_pair, pair, info, params, hourly_days,\n                zone_history_days, force_refresh,\n            ): pair\n            for pair, info in pairs.items()\n        }\n        for future in as_completed(futures):\n            pair, result = future.result()\n            done += 1\n            if result:\n                results[pair] = result\n                r = result\n                print(f\"    [{done}/{total}] {pair}: {r.total_trades} trades, \"\n                      f\"{r.win_rate:.0f}% WR, {r.total_pnl_pips:+.0f} pips\")\n            else:\n                print(f\"    [{done}/{total}] {pair}: no data\")\n\n    return results\n",
    "def run_all_backtests_parallel(\n    params: StrategyParams = None,\n    hourly_days: int = 30,\n    zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS,\n    pairs: Dict = None,\n    force_refresh: bool = False,\n    base_client_id: int | None = None,\n) -> Dict[str, BacktestResult]:\n    \"\"\"Run all pair backtests.\n\n    Cached runs execute in parallel for speed. Forced refresh runs execute\n    sequentially so IBKR pacing limits are respected.\n    \"\"\"\n    if params is None:\n        params = StrategyParams()\n    if pairs is None:\n        pairs = PAIRS\n    if base_client_id is None:\n        base_client_id = ibkr.TWS_CLIENT_ID\n\n    results = {}\n    pair_items = list(pairs.items())\n    total = len(pair_items)\n    done = 0\n    client_id_suffix = ''\n    if total > 0 and base_client_id is not None:\n        last_client_id = _pair_client_id(base_client_id, total - 1)\n        if total == 1:\n            client_id_suffix = f\" with client ID {base_client_id}\"\n        else:\n            client_id_suffix = f\" with client IDs {base_client_id}-{last_client_id}\"\n\n    if force_refresh:\n        print(f\"  Refreshing {total} backtests from IBKR/TWS sequentially{client_id_suffix}...\")\n        for offset, (pair, info) in enumerate(pair_items):\n            pair, result = _backtest_pair(\n                pair,\n                info,\n                params,\n                hourly_days,\n                zone_history_days,\n                force_refresh,\n                client_id=_pair_client_id(base_client_id, offset),\n            )\n            done += 1\n            if result:\n                results[pair] = result\n                r = result\n                print(f\"    [{done}/{total}] {pair}: {r.total_trades} trades, \"\n                      f\"{r.win_rate:.0f}% WR, {r.total_pnl_pips:+.0f} pips\")\n            else:\n                print(f\"    [{done}/{total}] {pair}: no data\")\n        return results\n\n    print(f\"  Launching {total} backtests in parallel (using cache when available{client_id_suffix})...\")\n\n    with ThreadPoolExecutor(max_workers=total) as executor:\n        futures = {\n            executor.submit(\n                _backtest_pair,\n                pair,\n                info,\n                params,\n                hourly_days,\n                zone_history_days,\n                force_refresh,\n                _pair_client_id(base_client_id, offset),\n            ): pair\n            for offset, (pair, info) in enumerate(pair_items)\n        }\n        for future in as_completed(futures):\n            pair, result = future.result()\n            done += 1\n            if result:\n                results[pair] = result\n                r = result\n                print(f\"    [{done}/{total}] {pair}: {r.total_trades} trades, \"\n                      f\"{r.win_rate:.0f}% WR, {r.total_pnl_pips:+.0f} pips\")\n            else:\n                print(f\"    [{done}/{total}] {pair}: no data\")\n\n    return results\n",
    'backtest orchestrator',
)
write_text('fx_sr/backtest.py', backtest_text)

ibkr_text = read_text('fx_sr/ibkr.py')
ibkr_text = replace_between(
    ibkr_text,
    '# Connection singleton\n',
    'def _ticker_to_pair(ticker_symbol: str) -> Optional[str]:\n',
    "# Thread-local connection state so each backtest worker can use its own\n# IBKR client ID without clobbering peers in other threads.\n_THREAD_STATE = threading.local()\n_IBKR_LOCK = threading.RLock()\n\n\ndef _resolve_client_id(client_id: Optional[int] = None) -> int:\n    \"\"\"Return an explicit client ID or the configured default.\"\"\"\n    return TWS_CLIENT_ID if client_id is None else int(client_id)\n\n\ndef _get_thread_connection_state() -> tuple[object | None, bool, int | None]:\n    \"\"\"Return the current thread's cached IBKR connection state.\"\"\"\n    return (\n        getattr(_THREAD_STATE, 'ib', None),\n        getattr(_THREAD_STATE, 'connected', False),\n        getattr(_THREAD_STATE, 'client_id', None),\n    )\n\n\ndef _set_thread_connection_state(ib, connected: bool, client_id: Optional[int]) -> None:\n    \"\"\"Persist the current thread's IBKR connection state.\"\"\"\n    _THREAD_STATE.ib = ib\n    _THREAD_STATE.connected = connected\n    _THREAD_STATE.client_id = client_id\n\n\ndef configure_connection(\n    host: Optional[str] = None,\n    port: Optional[int] = None,\n    client_id: Optional[int] = None,\n) -> None:\n    \"\"\"Override IBKR connection defaults and reset this thread if changed.\"\"\"\n    global TWS_HOST, TWS_PORT, TWS_CLIENT_ID\n\n    with _IBKR_LOCK:\n        new_host = TWS_HOST if host is None else host\n        new_port = TWS_PORT if port is None else int(port)\n        new_client_id = TWS_CLIENT_ID if client_id is None else int(client_id)\n\n        changed = (new_host, new_port, new_client_id) != (TWS_HOST, TWS_PORT, TWS_CLIENT_ID)\n        TWS_HOST = new_host\n        TWS_PORT = new_port\n        TWS_CLIENT_ID = new_client_id\n\n    if changed:\n        disconnect()\n\n\ndef _get_connection(client_id: Optional[int] = None):\n    \"\"\"Get or create a TWS connection. Returns (ib, connected) tuple.\"\"\"\n    resolved_client_id = _resolve_client_id(client_id)\n    ib, connected, active_client_id = _get_thread_connection_state()\n\n    if connected and ib and ib.isConnected() and active_client_id == resolved_client_id:\n        return ib, True\n\n    try:\n        from ib_async import IB\n        if ib and ib.isConnected():\n            ib.disconnect()\n        ib = IB()\n        ib.connect(TWS_HOST, TWS_PORT, clientId=resolved_client_id, timeout=5)\n        _set_thread_connection_state(ib, True, resolved_client_id)\n        return ib, True\n    except Exception:\n        _set_thread_connection_state(None, False, resolved_client_id)\n        return None, False\n\n\ndef disconnect():\n    \"\"\"Cleanly disconnect the current thread from TWS.\"\"\"\n    ib, _, _ = _get_thread_connection_state()\n    if ib and ib.isConnected():\n        ib.disconnect()\n    _set_thread_connection_state(None, False, None)\n\n\ndef is_available() -> bool:\n    \"\"\"Check if TWS connection is available.\"\"\"\n    _, connected = _get_connection()\n    return connected\n\n\n",
    'ibkr connection block',
)
ibkr_text = replace_once(
    ibkr_text,
    "def fetch_historical(\n    ticker_symbol: str,\n    interval: str,\n    days: int,\n) -> Optional[pd.DataFrame]:\n",
    "def fetch_historical(\n    ticker_symbol: str,\n    interval: str,\n    days: int,\n    client_id: Optional[int] = None,\n) -> Optional[pd.DataFrame]:\n",
    'ibkr fetch_historical signature',
)
ibkr_text = replace_once(
    ibkr_text,
    "    with _IBKR_LOCK:\n        ib, connected = _get_connection()\n        if not connected:\n            return None\n\n        try:\n",
    "    ib, connected = _get_connection(client_id=client_id)\n    if not connected:\n        return None\n\n    try:\n",
    'ibkr fetch_historical connection block',
)
write_text('fx_sr/ibkr.py', ibkr_text)

print('patched')
