"""Shared cache fill/backfill orchestration for CLI and live startup."""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass
import threading
import time
from typing import Callable

import pandas as pd

from . import ibkr
from .data import (
    _remaining_days_to_fetch,
    _aligned_expected_bar_at_or_after,
    _next_expected_bar_time,
    download_single_interval,
    refill_interval_from,
)
from .db import get_cache_summary, init_db


@dataclass(frozen=True)
class FillExecutionItem:
    """One cache fill work item."""

    pair_id: str
    pair_info: dict
    interval: str
    item_days: int
    gap_start: object | None


def find_cache_gaps(
    *,
    pairs: dict,
    target_days: int = 365,
    now=None,
    daily_extra_days: int = 0,
    only_pair: str | None = None,
) -> list[tuple[str, str, str]]:
    """Return list of (pair, ticker, interval) tuples that are missing or stale."""

    init_db()
    summary = get_cache_summary()
    now_ts = pd.Timestamp.now(tz='UTC') if now is None else pd.Timestamp(now)
    intervals = ['1d', '1h', '1m']
    effective_days = {
        '1d': target_days + max(0, daily_extra_days),
        '1h': target_days,
        '1m': target_days,
    }

    cached = {}
    for _, row in summary.iterrows():
        cached[(row['ticker'], row['interval'])] = (
            _coerce_utc(row['first_ts']),
            _coerce_utc(row['last_ts']),
            int(row['bars']),
        )

    start = now_ts - pd.Timedelta(days=int(effective_days['1d']))
    trading_days = len(pd.bdate_range(start.normalize(), now_ts.normalize(), freq='B'))
    trading_day_ratio = 5 / 7
    min_bars = {
        '1d': max(1, int(trading_days * 0.9)),
        '1h': int(effective_days['1h'] * 16),
        '1m': int(effective_days['1m'] * 1000 * trading_day_ratio),
    }
    gaps = []
    pairs_to_check = pairs if not only_pair else {only_pair: pairs[only_pair]} if only_pair in pairs else {}
    for pair_id, pair_info in pairs_to_check.items():
        ticker = pair_info['ticker']
        for iv in intervals:
            if (ticker, iv) not in cached:
                gaps.append((pair_id, ticker, iv))
                continue
            first_ts, last_ts, bars = cached[(ticker, iv)]
            requested_start = now_ts - pd.Timedelta(days=int(effective_days[iv]))
            if first_ts > requested_start or last_ts < requested_start:
                gaps.append((pair_id, ticker, iv))
                continue
            if bars < min_bars[iv]:
                gaps.append((pair_id, ticker, iv))
                continue
            if _remaining_days_to_fetch(
                interval=iv,
                requested_days=effective_days[iv],
                cached_range=(first_ts, last_ts, bars),
                now=now_ts,
            ) > 0:
                gaps.append((pair_id, ticker, iv))

    return gaps


def find_cache_gap_work_items(
    *,
    pairs: dict,
    target_days: int = 365,
    now=None,
    daily_extra_days: int = 0,
    only_pair: str | None = None,
    verbose: bool = False,
    progress_cb: Callable[[int, int, str, int], None] | None = None,
) -> list[tuple[str, str, str, object]]:
    """Return gap work items including the first missing timestamp when known."""

    now_ts = pd.Timestamp.now(tz='UTC') if now is None else pd.Timestamp(now)
    max_days = target_days + max(0, daily_extra_days)
    summary = get_cache_summary()
    cached = {
        (row['ticker'], row['interval']): (
            _coerce_utc(row['first_ts']),
            _coerce_utc(row['last_ts']),
            int(row['bars']),
        )
        for _, row in summary.iterrows()
    }

    start = now_ts - pd.Timedelta(days=int(max_days))
    trading_days = len(pd.bdate_range(start.normalize(), now_ts.normalize(), freq='B'))
    trading_day_ratio = 5 / 7
    min_bars = {
        '1d': max(1, int(trading_days * 0.9)),
        '1h': int(target_days * 16),
        '1m': int(target_days * 1000 * trading_day_ratio),
    }

    coverage_gaps = set(
        (pair_id, interval)
        for pair_id, _ticker, interval in find_cache_gaps(
            pairs=pairs,
            target_days=target_days,
            now=now_ts,
            daily_extra_days=daily_extra_days,
            only_pair=only_pair,
        )
    )

    work_items: list[tuple[str, str, str, object]] = []
    pairs_to_check = pairs if not only_pair else {only_pair: pairs[only_pair]} if only_pair in pairs else {}
    total_pairs = len(pairs_to_check)
    for idx, (pair_id, pair_info) in enumerate(pairs_to_check.items(), 1):
        if progress_cb is not None:
            progress_cb(idx, total_pairs, pair_id, 0)
        if verbose:
            print(f'    [{idx}/{total_pairs}] {pair_id}', end='', flush=True)
        ticker = pair_info['ticker']
        pair_gaps = 0
        for interval in ('1d', '1h', '1m'):
            if (pair_id, interval) not in coverage_gaps:
                continue
            cached_row = cached.get((ticker, interval))
            if cached_row is None:
                gap_start = None
            else:
                first_ts, last_ts, bars_in_summary = cached_row
                is_stale_coverage = bars_in_summary < min_bars[interval]
                requested_start = now_ts - pd.Timedelta(days=int(max_days if interval == '1d' else target_days))
                if requested_start < first_ts or requested_start > last_ts:
                    is_stale_coverage = True

                remaining_days = _remaining_days_to_fetch(
                    interval=interval,
                    requested_days=max_days if interval == '1d' else target_days,
                    cached_range=(first_ts, last_ts, bars_in_summary),
                    now=now_ts,
                )
                if is_stale_coverage:
                    gap_start = _aligned_expected_bar_at_or_after(start, interval)
                elif remaining_days > 0:
                    gap_start = _next_expected_bar_time(pd.Timestamp(last_ts), interval)
                else:
                    gap_start = _aligned_expected_bar_at_or_after(start, interval)
            work_items.append((pair_id, ticker, interval, gap_start))
            pair_gaps += 1
        if progress_cb is not None:
            progress_cb(idx, total_pairs, pair_id, pair_gaps)
        if verbose:
            if pair_gaps:
                print(f' ({pair_gaps} gaps)')
            else:
                print(' ok')

    return work_items


def _weekday_gap_days(from_ts, to_ts) -> int:
    """Estimate missing trading-calendar days between two timestamps (weekdays only)."""

    if from_ts is None or to_ts is None:
        return 0
    start = pd.Timestamp(from_ts).normalize() + pd.Timedelta(days=1)
    end = pd.Timestamp(to_ts).normalize()
    if end <= start:
        return 0
    return len(pd.bdate_range(start, end, freq='B'))


def _coerce_utc(ts: pd.Timestamp | str) -> pd.Timestamp:
    """Normalize a timestamp to a UTC-aware Timestamp."""

    value = pd.Timestamp(ts)
    if value.tzinfo is None:
        return value.tz_localize('UTC')
    return value.tz_convert('UTC')


def find_cache_gaps_verbose(
    *,
    pairs: dict,
    target_days: int = 365,
    now=None,
    daily_extra_days: int = 0,
    only_pair: str | None = None,
) -> list[tuple[str, str, str, str]]:
    """Like find_cache_gaps but returns (pair, ticker, interval, detail)."""

    init_db()
    summary = get_cache_summary()
    now_ts = pd.Timestamp.now(tz='UTC') if now is None else pd.Timestamp(now)
    intervals = ['1d', '1h', '1m']
    effective_days = {
        '1d': target_days + max(0, daily_extra_days),
        '1h': target_days,
        '1m': target_days,
    }

    cached = {}
    for _, row in summary.iterrows():
        cached[(row['ticker'], row['interval'])] = (
            _coerce_utc(row['first_ts']),
            _coerce_utc(row['last_ts']),
            int(row['bars']),
        )

    start = now_ts - pd.Timedelta(days=int(effective_days['1d']))
    trading_days = len(pd.bdate_range(start.normalize(), now_ts.normalize(), freq='B'))
    trading_day_ratio = 5 / 7
    min_bars = {
        '1d': max(1, int(trading_days * 0.9)),
        '1h': int(effective_days['1h'] * 16),
        '1m': int(effective_days['1m'] * 1000 * trading_day_ratio),
    }
    gaps = []
    for pair_id, pair_info in pairs.items():
        if only_pair and pair_id != only_pair:
            continue
        ticker = pair_info['ticker']
        for iv in intervals:
            if (ticker, iv) not in cached:
                gaps.append((pair_id, ticker, iv, 'no cached data'))
                continue
            first_ts, last_ts, bars = cached[(ticker, iv)]
            requested_start = now_ts - pd.Timedelta(days=int(effective_days[iv]))
            if first_ts > requested_start or last_ts < requested_start:
                gaps.append((
                    pair_id, ticker, iv,
                    f'cached range={first_ts} -> {last_ts}, first < requested_start={requested_start}, bars={bars}',
                ))
                continue
            effective_bars = bars
            remaining = _remaining_days_to_fetch(
                interval=iv,
                requested_days=effective_days[iv],
                cached_range=(first_ts, last_ts, bars),
                now=now_ts,
            )
            weekday_gap = _weekday_gap_days(last_ts, now_ts)
            if effective_bars < min_bars[iv]:
                gaps.append((
                    pair_id, ticker, iv,
                    f'bars={bars}, effective_bars={effective_bars} < min {min_bars[iv]}, '
                    f'range={first_ts} -> {last_ts}, need={remaining}d, '
                    f'weekdays_since_last={weekday_gap}',
                ))
                continue
            if remaining > 0:
                gaps.append((
                    pair_id, ticker, iv,
                    f'bars={bars}, range={first_ts} -> {last_ts}, '
                    f'need={remaining}d, weekdays_since_last={weekday_gap}',
                ))
    return gaps


def scan_recent_pair_gaps(
    pair_id: str,
    pair_info: dict,
    *,
    recent_cutoff: pd.Timestamp,
    now_utc: pd.Timestamp | None = None,
    skip_refill: bool = False,
    skip_reason: str = 'skipped',
) -> dict:
    """Scan one pair for recent refill holes and older report-only holes."""

    ticker = pair_info.get('ticker')
    if not ticker:
        return {
            'pair_id': pair_id,
            'ticker': ticker,
            'error': 'no ticker',
        }
    if skip_refill:
        return {
            'pair_id': pair_id,
            'ticker': ticker,
            'refill_holes': [],
            'reported_only_holes': [],
            'skipped': skip_reason,
            'error': None,
        }

    current_now = pd.Timestamp.now(tz='UTC') if now_utc is None else pd.Timestamp(now_utc)
    refill_holes: list[tuple[str, pd.Timestamp]] = []
    reported_only_holes: list[str] = []

    for interval in ('1d', '1h', '1m'):
        gap_start_recent = find_first_missing_cached_bar(
            ticker,
            interval,
            start=recent_cutoff,
            end=current_now,
            now=current_now,
            check_trailing=True,
        )
        if gap_start_recent is not None:
            gap_ts = pd.Timestamp(gap_start_recent)
            if gap_ts.tzinfo is None:
                gap_ts = gap_ts.tz_localize('UTC')
            else:
                gap_ts = gap_ts.tz_convert('UTC')
            refill_holes.append((interval, gap_ts))

        if interval == '1m':
            continue

        gap_start_old = find_first_missing_cached_bar(
            ticker,
            interval,
            end=recent_cutoff,
            now=recent_cutoff,
            check_trailing=False,
        )
        if gap_start_old is None:
            continue
        gap_ts = pd.Timestamp(gap_start_old)
        if gap_ts.tzinfo is None:
            gap_ts = gap_ts.tz_localize('UTC')
        else:
            gap_ts = gap_ts.tz_convert('UTC')
        reported_only_holes.append(f'{interval}@{gap_ts}')

    return {
        'pair_id': pair_id,
        'ticker': ticker,
        'refill_holes': refill_holes,
        'reported_only_holes': reported_only_holes,
        'skipped': None,
        'error': None,
    }


def build_fill_execution_items(
    gap_items: list[tuple[str, str, str, object]],
    *,
    pairs: dict,
    target_days: int,
    daily_target_days: int | None = None,
) -> list[FillExecutionItem]:
    """Expand scanned gap items into executable fill work items."""

    resolved_daily_days = target_days if daily_target_days is None else daily_target_days
    return [
        FillExecutionItem(
            pair_id=pair_id,
            pair_info=pairs[pair_id],
            interval=interval,
            item_days=resolved_daily_days if interval == '1d' else target_days,
            gap_start=gap_start,
        )
        for pair_id, _ticker, interval, gap_start in gap_items
    ]


def execute_fill_work_items(
    work_items: list[FillExecutionItem],
    *,
    base_fill_client_id: int,
    max_workers: int = 3,
    max_retries: int = 3,
    wait_timeout_s: float = 15.0,
    verbose: bool = False,
    debug: bool = False,
    retry_delay_s: float = 5.0,
    before_retry: Callable[[int, int], None] | None = None,
    wait_formatter: Callable[[FillExecutionItem, float], str] | None = None,
    on_wait: Callable[[list[str]], None] | None = None,
    on_item_done: Callable[[FillExecutionItem, int, float, int, int], None] | None = None,
    on_item_failed: Callable[[FillExecutionItem, Exception, int, int], None] | None = None,
    on_attempt_complete: Callable[[int, int, float], None] | None = None,
) -> dict[str, object]:
    """Execute fill/refill items with stable worker client IDs and retries."""

    if not work_items:
        return {
            'status': 'complete',
            'attempts': 0,
            'errors': 0,
            'items_processed': 0,
            'items_requested': 0,
            'remaining': 0,
            'failed_items': [],
            'elapsed': 0.0,
        }

    total_requested = len(work_items)
    effective_workers = max(1, min(int(max_workers), total_requested))
    slot_lock = threading.Lock()
    client_slots = [0]

    def _thread_client_id() -> int:
        thread = threading.current_thread()
        slot = getattr(thread, '_fill_client_id_slot', None)
        if slot is None:
            with slot_lock:
                slot = client_slots[0]
                client_slots[0] += 1
            thread._fill_client_id_slot = slot
        return base_fill_client_id + int(slot)

    def _run_work_item(item: FillExecutionItem) -> tuple[FillExecutionItem, int, float]:
        cid = _thread_client_id()
        item_start = time.perf_counter()
        if item.gap_start is not None:
            rows = len(
                refill_interval_from(
                    item.pair_info['ticker'],
                    item.interval,
                    item.gap_start,
                    client_id=cid,
                )
            )
        else:
            rows = download_single_interval(
                item.pair_id,
                item.pair_info,
                item.interval,
                item.item_days,
                client_id=cid,
                verbose=verbose,
            )
        return item, rows, time.perf_counter() - item_start

    t0 = time.perf_counter()
    pending = list(work_items)
    total_errors = 0
    total_items_processed = 0
    attempt = 0

    with ThreadPoolExecutor(max_workers=effective_workers) as executor:
        while pending and attempt < max_retries:
            attempt += 1
            attempt_start = time.perf_counter()
            if attempt > 1:
                if before_retry is not None:
                    before_retry(attempt, len(pending))
                time.sleep(retry_delay_s)

            completed = 0
            total = len(pending)
            failed: list[FillExecutionItem] = []
            futures = {
                executor.submit(_run_work_item, item): {
                    'item': item,
                    'submitted_at': time.perf_counter(),
                }
                for item in pending
            }
            pending_futures = set(futures)
            while pending_futures:
                done, pending_futures = wait(
                    pending_futures,
                    timeout=wait_timeout_s,
                    return_when=FIRST_COMPLETED,
                )
                if not done:
                    if on_wait is not None:
                        now_wait = time.perf_counter()
                        waiting_parts = []
                        for fut in sorted(
                            pending_futures,
                            key=lambda current_fut: futures[current_fut]['submitted_at'],
                        ):
                            item = futures[fut]['item']
                            waiting_s = now_wait - futures[fut]['submitted_at']
                            if wait_formatter is None:
                                waiting_parts.append(f'{item.pair_id} {item.interval} ({waiting_s:.0f}s)')
                            else:
                                waiting_parts.append(wait_formatter(item, waiting_s))
                        on_wait(waiting_parts)
                    continue

                for fut in done:
                    item = futures[fut]['item']
                    completed += 1
                    try:
                        _, rows, item_elapsed = fut.result()
                        total_items_processed += 1
                        if on_item_done is not None:
                            on_item_done(item, rows, item_elapsed, completed, total)
                    except Exception as exc:
                        failed.append(item)
                        total_errors += 1
                        if on_item_failed is not None:
                            on_item_failed(item, exc, completed, total)

            pending = failed
            if on_attempt_complete is not None:
                on_attempt_complete(
                    attempt,
                    len(failed),
                    time.perf_counter() - attempt_start,
                )

    return {
        'status': 'incomplete' if pending else 'complete',
        'attempts': attempt,
        'errors': total_errors,
        'items_processed': total_items_processed,
        'items_requested': total_requested,
        'remaining': len(pending),
        'failed_items': pending,
        'elapsed': time.perf_counter() - t0,
    }
