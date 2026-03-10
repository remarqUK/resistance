"""Position tracking: monitor IBKR positions against strategy exit rules.

Phase 1 (read-only):
- Reads open FX positions from TWS
- Matches them to S/R zones to construct Trade objects
- Runs check_exit() each scan cycle and alerts on exit conditions
- Persists state to SQLite so restarts resume monitoring
"""

import sqlite3
import os
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from .config import PAIRS, DEFAULT_ZONE_HISTORY_DAYS
from .data import fetch_daily_data, fetch_hourly_data
from .db import get_db_path
from .levels import detect_zones, get_nearest_zones, SRZone
from .strategy import Trade, StrategyParams, check_exit, get_market_exit_price
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
# SQLite persistence for tracked trades
# ---------------------------------------------------------------------------

_TABLE_INIT = False


def _ensure_table(db_path: str = None):
    """Create the open_trades table if it doesn't exist (once per session)."""
    global _TABLE_INIT
    if _TABLE_INIT:
        return
    if db_path is None:
        db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS open_trades (
            pair          TEXT NOT NULL,
            direction     TEXT NOT NULL,
            entry_time    TEXT NOT NULL,
            entry_price   REAL NOT NULL,
            sl_price      REAL NOT NULL,
            tp_price      REAL NOT NULL,
            zone_upper    REAL NOT NULL,
            zone_lower    REAL NOT NULL,
            zone_strength TEXT NOT NULL,
            risk          REAL NOT NULL,
            bars_monitored INTEGER DEFAULT 0,
            ibkr_avg_cost  REAL,
            ibkr_size      REAL,
            created_at    TEXT NOT NULL,
            PRIMARY KEY (pair, direction)
        )
    """)
    conn.commit()
    conn.close()
    _TABLE_INIT = True


def _db_execute(sql: str, params: tuple = (), db_path: str = None):
    """Execute a single SQL statement."""
    if db_path is None:
        db_path = get_db_path()
    _ensure_table(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute(sql, params)
    conn.commit()
    conn.close()


def _save_trade(pair: str, trade: Trade, ibkr_avg_cost: float, ibkr_size: float):
    """Save or update a tracked trade in the DB."""
    _db_execute(
        """INSERT OR REPLACE INTO open_trades
           (pair, direction, entry_time, entry_price, sl_price, tp_price,
            zone_upper, zone_lower, zone_strength, risk, bars_monitored,
            ibkr_avg_cost, ibkr_size, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)""",
        (
            pair, trade.direction, str(trade.entry_time),
            trade.entry_price, trade.sl_price, trade.tp_price,
            trade.zone_upper, trade.zone_lower, trade.zone_strength,
            trade.risk, ibkr_avg_cost, ibkr_size,
            datetime.now().isoformat(),
        ),
    )


def _load_trades() -> Dict[str, dict]:
    """Load all tracked trades from DB. Returns dict keyed by 'PAIR:DIRECTION'."""
    db_path = get_db_path()
    if not os.path.exists(db_path):
        return {}

    _ensure_table(db_path)
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT pair, direction, entry_time, entry_price, sl_price, tp_price, "
        "zone_upper, zone_lower, zone_strength, risk, bars_monitored, "
        "ibkr_avg_cost, ibkr_size FROM open_trades"
    ).fetchall()
    conn.close()

    result = {}
    for r in rows:
        key = f"{r[0]}:{r[1]}"
        result[key] = {
            'pair': r[0],
            'trade': Trade(
                entry_time=pd.Timestamp(r[2]),
                entry_price=r[3], direction=r[1],
                sl_price=r[4], tp_price=r[5],
                zone_upper=r[6], zone_lower=r[7],
                zone_strength=r[8], risk=r[9],
            ),
            'bars_monitored': r[10],
            'ibkr_avg_cost': r[11],
            'ibkr_size': r[12],
        }
    return result


def _remove_trade(pair: str, direction: str):
    """Remove a trade from the DB (position was closed)."""
    db_path = get_db_path()
    if os.path.exists(db_path):
        _db_execute("DELETE FROM open_trades WHERE pair=? AND direction=?",
                     (pair, direction))


def _increment_bars(pair: str, direction: str):
    """Increment bars_monitored counter for a tracked trade."""
    _db_execute(
        "UPDATE open_trades SET bars_monitored = bars_monitored + 1 "
        "WHERE pair=? AND direction=?",
        (pair, direction),
    )


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
    nearest_sup, nearest_res = get_nearest_zones(zones, avg_cost, major_only=True)

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


# ---------------------------------------------------------------------------
# Sync + monitoring
# ---------------------------------------------------------------------------

def sync_positions(
    params: StrategyParams = None,
    zone_history_days: int = DEFAULT_ZONE_HISTORY_DAYS,
) -> Dict[str, dict]:
    """Synchronize DB-tracked trades with live IBKR positions.

    - New positions (in IBKR, not DB) -> build Trade, save to DB
    - Closed positions (in DB, not IBKR) -> remove from DB
    - Returns merged dict of tracked trades
    """
    if params is None:
        params = StrategyParams()

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
            _remove_trade(info['pair'], info['trade'].direction)
            del db_trades[key]

    # Add new IBKR positions not yet tracked
    for key, pos in ibkr_by_key.items():
        if key not in db_trades:
            direction = 'LONG' if pos['size'] > 0 else 'SHORT'
            print(f"    New position detected: {pos['pair']} {direction} "
                  f"@ {pos['avg_cost']:.5f} (size: {pos['size']:.0f})")

            trade = _build_trade_from_position(
                pos['pair'], pos['avg_cost'], direction,
                params, zone_history_days,
            )
            if trade:
                _save_trade(pos['pair'], trade, pos['avg_cost'], pos['size'])
                db_trades[key] = {
                    'pair': pos['pair'],
                    'trade': trade,
                    'bars_monitored': 0,
                    'ibkr_avg_cost': pos['avg_cost'],
                    'ibkr_size': pos['size'],
                }

    return db_trades


def check_position_exits(
    tracked: Dict[str, dict],
    params: StrategyParams = None,
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

        ticker = pair_ticker(pair)
        if not ticker:
            continue

        hourly_df = fetch_hourly_data(ticker, days=1)
        if hourly_df.empty:
            continue

        last_bar = hourly_df.iloc[-1]
        bar_time = hourly_df.index[-1]
        current_price = float(last_bar['Close'])
        pnl_pips = calc_pnl_pips(trade, current_price, pair_pip(pair), params)

        snapshots[key] = {
            'current_price': current_price,
            'pnl_pips': pnl_pips,
        }

        result = check_exit(
            trade,
            bar_high=float(last_bar['High']),
            bar_low=float(last_bar['Low']),
            bar_close=current_price,
            bar_time=bar_time,
            bars_held=bars,
            params=params,
            pip=pair_pip(pair),
        )

        if result:
            exit_reason, exit_price = result
            alerts.append({
                'pair': pair,
                'direction': trade.direction,
                'exit_reason': exit_reason,
                'exit_price': exit_price,
                'entry_price': trade.entry_price,
                'current_price': current_price,
                'pnl_pips': pnl_pips,
                'bars_monitored': bars,
            })

        _increment_bars(pair, trade.direction)
        info['bars_monitored'] = bars + 1

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
