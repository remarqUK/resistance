"""Per-cycle diagnostic log writer.

Writes one JSON file per live monitor cycle under ``logs/minute_scans/``.
Each file captures enough per-ticker state to answer "why did/didn't this
pair trigger?" after the fact without re-running the scan.

Fire-and-forget: write failures never interrupt the scan loop.
"""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
import json
import os
from typing import Any, TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from .live import MonitorSnapshot, PairScanRow


_LOGS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'logs',
    'minute_scans',
)


def _to_jsonable(value: Any) -> Any:
    """Convert arbitrary values (timestamps, dataclasses, sets) to JSON-safe form."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, (set, frozenset)):
        return sorted(_to_jsonable(v) for v in value)
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(v) for v in value]
    if is_dataclass(value):
        return {k: _to_jsonable(v) for k, v in asdict(value).items()}
    return str(value)


def _serialize_zone(lower, upper, strength, dist_pct) -> dict | None:
    if lower is None and upper is None:
        return None
    return {
        'lower': float(lower) if lower is not None else None,
        'upper': float(upper) if upper is not None else None,
        'strength': strength,
        'dist_pct': float(dist_pct) if dist_pct is not None else None,
    }


def _serialize_pair_row(row: 'PairScanRow') -> dict:
    return {
        'pair': row.pair,
        'name': row.name,
        'decimals': row.decimals,
        'price': float(row.price) if row.price is not None else None,
        'state': row.state,
        'note': row.note,
        'support': _serialize_zone(
            row.support_lower, row.support_upper, row.support_strength, row.support_dist_pct,
        ),
        'resistance': _serialize_zone(
            row.resistance_lower, row.resistance_upper,
            row.resistance_strength, row.resistance_dist_pct,
        ),
        'signal': _to_jsonable(row.signal) if row.signal is not None else None,
    }


def _serialize_execution_result(result) -> dict:
    return _to_jsonable(result)


def _serialize_tracked_position(info: dict) -> dict:
    return _to_jsonable(info)


def write_cycle_snapshot(snapshot: 'MonitorSnapshot', *, profile: str | None = None) -> str | None:
    """Persist a full monitor cycle to logs/minute_scans/YYYY-MM-DD-HH-mm-ss.json.

    Returns the written path on success, None on any failure.
    """
    try:
        os.makedirs(_LOGS_DIR, exist_ok=True)

        ts = snapshot.scan_started_at
        filename = ts.strftime('%Y-%m-%d-%H-%M-%S') + '.json'

        execution_by_pair: dict[str, dict] = {}
        for result in snapshot.execution_results:
            execution_by_pair[result.pair] = _serialize_execution_result(result)

        tracked_by_pair: dict[str, dict] = {}
        for key, info in (snapshot.tracked or {}).items():
            pair = info.get('pair') if isinstance(info, dict) else None
            if pair:
                tracked_by_pair.setdefault(pair, _serialize_tracked_position(info))

        signal_by_pair: dict[str, dict] = {}
        for sig in snapshot.signals:
            signal_by_pair[sig.pair] = _to_jsonable(sig)

        position_snapshot_by_pair: dict[str, dict] = {}
        for key, snap in (snapshot.position_snapshots or {}).items():
            pair = snap.get('pair') if isinstance(snap, dict) else None
            if pair:
                position_snapshot_by_pair.setdefault(pair, _to_jsonable(snap))

        pairs_out: dict[str, dict] = {}
        for row in snapshot.pair_rows:
            pair_entry = _serialize_pair_row(row)
            pair_entry['signal'] = signal_by_pair.get(row.pair) or pair_entry['signal']
            pair_entry['execution'] = execution_by_pair.get(row.pair)
            pair_entry['tracked_position'] = tracked_by_pair.get(row.pair)
            pair_entry['position_snapshot'] = position_snapshot_by_pair.get(row.pair)
            pairs_out[row.pair] = pair_entry

        payload = {
            'cycle': {
                'scan_started_at': snapshot.scan_started_at.isoformat(),
                'scan_completed_at': snapshot.scan_completed_at.isoformat(),
                'duration_s': float(snapshot.scan_duration),
                'active_balance': (
                    float(snapshot.active_balance)
                    if snapshot.active_balance is not None else None
                ),
                'active_currency': snapshot.active_currency,
                'risk_pct': float(snapshot.risk_pct),
                'track_positions': bool(snapshot.track_positions),
                'execute_orders': bool(snapshot.execute_orders),
                'pending_pairs': sorted(snapshot.pending_pairs or ()),
                'profile': profile,
            },
            'pairs': pairs_out,
            'alerts': [_to_jsonable(a) for a in (snapshot.alerts or [])],
            'messages': list(snapshot.messages or []),
        }

        filepath = os.path.join(_LOGS_DIR, filename)
        with open(filepath, 'w') as f:
            json.dump(payload, f, indent=2, default=str)
        return filepath
    except Exception:
        return None


def read_cycle_snapshot(path: str) -> dict:
    """Load a cycle snapshot JSON file."""
    with open(path) as f:
        return json.load(f)
