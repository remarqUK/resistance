"""Diagnose whether IBKR real-time bar streaming is actually working.

Connects on a dedicated client ID, requests 5-second MIDPOINT bars for a
single pair, listens for ~30 seconds, and prints exactly what happened —
including subscription exceptions that the live hub currently swallows.

Run with the live process still running (different client ID) or after
stopping it:

    python tools/probe_realtime_bars.py --pair EURUSD --seconds 30
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fx_sr import ibkr  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--pair', default='EURUSD', help='Pair id, e.g. EURUSD')
    parser.add_argument('--seconds', type=int, default=30, help='Listen duration')
    parser.add_argument('--client-id', type=int, default=9901, help='Dedicated client id')
    args = parser.parse_args()

    ib, connected = ibkr._get_connection(client_id=args.client_id)
    if not connected or ib is None:
        print('FAIL: could not connect to IBKR (check TWS/Gateway).')
        return 1

    print(f'Connected as client id {args.client_id}.')

    bar_count = 0
    bars_log: list[tuple[str, float]] = []

    try:
        contract = ibkr._make_contract(args.pair)
        ib.qualifyContracts(contract)
        print(f'Contract qualified: {contract}')

        try:
            bars = ib.reqRealTimeBars(
                contract,
                barSize=5,
                whatToShow='MIDPOINT',
                useRTH=False,
            )
        except Exception as exc:
            print(f'FAIL: reqRealTimeBars raised for {args.pair}: {type(exc).__name__}: {exc}')
            traceback.print_exc()
            return 2

        def _on_update(received, has_new_bar):
            nonlocal bar_count
            if has_new_bar and received:
                bar_count += 1
                bar = received[-1]
                bars_log.append((str(bar.time), float(bar.close)))

        bars.updateEvent += _on_update

        print(f'Subscribed. Listening for {args.seconds}s...')
        end = time.time() + args.seconds
        while time.time() < end:
            try:
                ib.waitOnUpdate(timeout=1)
            except Exception:
                pass

        print()
        print(f'Received {bar_count} real-time bars in {args.seconds}s.')
        if bars_log:
            print('First/last bars:')
            print(f'  first: {bars_log[0]}')
            print(f'  last:  {bars_log[-1]}')
        else:
            print('No bars arrived.')
            print('Likely causes:')
            print('  1) Market-data entitlement missing for this instrument on this account.')
            print('  2) Another client is holding the live subscription (TWS desktop with live quotes).')
            print('  3) Market data type is set to Delayed (reqMarketDataType(3)) — MIDPOINT delayed does not fire RealTimeBars.')
            print('  4) Data farm disconnected — check TWS bottom-right farm status.')

        try:
            ib.cancelRealTimeBars(bars)
        except Exception:
            pass
        return 0 if bar_count > 0 else 3
    finally:
        ibkr.disconnect()


if __name__ == '__main__':
    raise SystemExit(main())
