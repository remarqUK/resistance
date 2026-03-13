"""L2 market-depth capture and formatting helpers."""

from __future__ import annotations

import threading
import time
from typing import Optional

import pandas as pd

from . import db, ibkr


DEFAULT_L2_DEPTH = 5
DEFAULT_L2_INTERVAL_SECONDS = 1.0
DEFAULT_L2_CAPTURE_SECONDS = 60.0


def _format_price(value: float | None) -> str:
    """Format a price field for plain-text display."""
    return '-' if value is None else f'{float(value):.6f}'


def _format_size(value: float | None) -> str:
    """Format a size field for plain-text display."""
    return '-' if value is None else f'{float(value):.2f}'


def _persist_snapshot(snapshot: dict, db_path: str | None = None) -> int:
    """Persist one normalized IBKR depth snapshot."""
    return db.save_l2_snapshot(
        ticker=snapshot['ticker'],
        pair=snapshot['pair'],
        captured_at=snapshot['captured_at'],
        bids=snapshot['bids'],
        asks=snapshot['asks'],
        depth_requested=snapshot['depth_requested'],
        mid_price=snapshot.get('mid_price'),
        best_bid=snapshot.get('best_bid'),
        best_ask=snapshot.get('best_ask'),
        source='IBKR',
        db_path=db_path,
    )


def capture_l2_once(
    pair_id: str,
    pair_info: dict,
    depth: int = DEFAULT_L2_DEPTH,
    client_id: int | None = None,
    db_path: str | None = None,
) -> Optional[dict]:
    """Fetch and persist a single L2 snapshot for one pair."""
    snapshot = ibkr.fetch_market_depth_snapshot(
        pair_info['ticker'],
        depth=depth,
        client_id=client_id,
    )
    if snapshot is None:
        return None

    snapshot['snapshot_id'] = _persist_snapshot(snapshot, db_path=db_path)
    return snapshot


def capture_l2_stream(
    pairs: dict,
    depth: int = DEFAULT_L2_DEPTH,
    interval_seconds: float = DEFAULT_L2_INTERVAL_SECONDS,
    duration_seconds: float | None = DEFAULT_L2_CAPTURE_SECONDS,
    max_snapshots: int | None = None,
    client_id: int | None = None,
    db_path: str | None = None,
) -> dict:
    """Capture and persist a stream of L2 snapshots until the stop condition is met."""
    stop_event = threading.Event()
    timer = None
    started_at = time.monotonic()
    stats = {
        'pairs': list(pairs.keys()),
        'depth': int(depth),
        'interval_seconds': float(interval_seconds),
        'duration_seconds': duration_seconds,
        'db_path': db_path or db.get_db_path(),
        'snapshots_saved': 0,
        'snapshots_per_pair': {},
        'first_capture': None,
        'last_capture': None,
    }

    if duration_seconds is not None and duration_seconds > 0:
        timer = threading.Timer(float(duration_seconds), stop_event.set)
        timer.daemon = True
        timer.start()

    def on_snapshot(snapshot: dict) -> None:
        snapshot_id = _persist_snapshot(snapshot, db_path=db_path)
        if snapshot_id <= 0:
            return

        stats['snapshots_saved'] += 1
        pair = snapshot['pair']
        stats['snapshots_per_pair'][pair] = stats['snapshots_per_pair'].get(pair, 0) + 1

        captured_at = pd.Timestamp(snapshot['captured_at'])
        if stats['first_capture'] is None:
            stats['first_capture'] = captured_at
        stats['last_capture'] = captured_at

        if max_snapshots is not None and stats['snapshots_saved'] >= max_snapshots:
            stop_event.set()

    try:
        ibkr.stream_market_depth(
            list(pairs.keys()),
            on_snapshot=on_snapshot,
            stop_event=stop_event,
            depth=depth,
            interval_seconds=interval_seconds,
            client_id=client_id,
        )
    finally:
        if timer is not None:
            timer.cancel()
        stats['elapsed_seconds'] = max(time.monotonic() - started_at, 0.0)

    return stats


def format_l2_snapshot(snapshot: dict) -> str:
    """Render one saved L2 snapshot as plain text."""
    captured_at = pd.Timestamp(snapshot['captured_at']).tz_convert('UTC')
    bids = list(snapshot.get('bids', []))
    asks = list(snapshot.get('asks', []))
    row_count = max(len(bids), len(asks), 1)

    lines = [
        "",
        "=" * 78,
        f"  L2 SNAPSHOT {snapshot['pair']}  {captured_at}",
        "=" * 78,
        (
            f"  Best bid: {snapshot.get('best_bid') if snapshot.get('best_bid') is not None else '-'}   "
            f"Best ask: {snapshot.get('best_ask') if snapshot.get('best_ask') is not None else '-'}   "
            f"Mid: {snapshot.get('mid_price') if snapshot.get('mid_price') is not None else '-'}   "
            f"Spread: {snapshot.get('spread') if snapshot.get('spread') is not None else '-'}"
        ),
        "-" * 78,
        f"  {'LVL':>3} {'BID':>12} {'BID SZ':>10}  |  {'ASK':>12} {'ASK SZ':>10}",
        "-" * 78,
    ]
    for idx in range(row_count):
        bid = bids[idx] if idx < len(bids) else {}
        ask = asks[idx] if idx < len(asks) else {}
        lines.append(
            f"  {idx + 1:>3} "
            f"{_format_price(bid.get('price')):>12} "
            f"{_format_size(bid.get('size')):>10}  |  "
            f"{_format_price(ask.get('price')):>12} "
            f"{_format_size(ask.get('size')):>10}"
        )
    lines.append("=" * 78)
    return "\n".join(lines)


def format_l2_capture_summary(stats: dict) -> str:
    """Render capture-run summary statistics."""
    lines = [
        "",
        "=" * 78,
        "  L2 CAPTURE SUMMARY",
        "=" * 78,
        f"  Pairs:             {', '.join(stats.get('pairs', []))}",
        f"  Depth requested:   {stats.get('depth')}",
        f"  Interval:          {stats.get('interval_seconds', 0):.2f}s",
        f"  Elapsed:           {stats.get('elapsed_seconds', 0):.1f}s",
        f"  Snapshots saved:   {stats.get('snapshots_saved', 0)}",
        f"  Database:          {stats.get('db_path')}",
    ]

    first_capture = stats.get('first_capture')
    last_capture = stats.get('last_capture')
    if first_capture is not None:
        lines.append(f"  First capture:     {pd.Timestamp(first_capture).tz_convert('UTC')}")
    if last_capture is not None:
        lines.append(f"  Last capture:      {pd.Timestamp(last_capture).tz_convert('UTC')}")

    per_pair = stats.get('snapshots_per_pair', {})
    if per_pair:
        lines.append("  Per pair:")
        for pair, count in sorted(per_pair.items()):
            lines.append(f"    {pair:<10} {count:>8}")
    lines.append("=" * 78)
    return "\n".join(lines)


def format_l2_library_summary(summary_df: pd.DataFrame) -> str:
    """Render cached L2 coverage summary rows."""
    if summary_df.empty:
        return "\n  No cached L2 snapshots found.\n"

    lines = [
        "",
        "=" * 108,
        "  L2 LIBRARY SUMMARY",
        "=" * 108,
        f"  {'PAIR':<10} {'TICKER':<12} {'SNAPSHOTS':>10} {'FROM':<26} {'TO':<26} {'DEPTH':>5} {'AVG SPRD':>10}",
        "-" * 108,
    ]
    for _, row in summary_df.iterrows():
        avg_spread = row['avg_spread']
        avg_spread_text = '-' if pd.isna(avg_spread) else f"{float(avg_spread):.6f}"
        lines.append(
            f"  {row['pair']:<10} {row['ticker']:<12} {int(row['snapshots']):>10} "
            f"{str(row['first_ts']):<26} {str(row['last_ts']):<26} "
            f"{int(row['max_depth']):>5} {avg_spread_text:>10}"
        )
    lines.append("=" * 108)
    return "\n".join(lines)
