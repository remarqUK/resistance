from __future__ import annotations

import argparse
import time

from fx_sr.backtest import _backtest_pair
from fx_sr.config import PAIRS
from fx_sr.strategy import StrategyParams


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=365)
    parser.add_argument("--zone-history", type=int, default=180)
    parser.add_argument("--pair", type=str, default=None)
    args = parser.parse_args()

    params = StrategyParams()
    pairs = (
        {args.pair.upper().replace("/", ""): PAIRS[args.pair.upper().replace("/", "")]}
        if args.pair
        else PAIRS
    )

    for index, (pair, info) in enumerate(pairs.items(), 1):
        started = time.perf_counter()
        _, result = _backtest_pair(
            pair=pair,
            pair_info=info,
            params=params,
            hourly_days=args.days,
            zone_history_days=args.zone_history,
            force_refresh=False,
            client_id=None,
        )
        elapsed = time.perf_counter() - started
        if result is None:
            print(f"[{index}/{len(pairs)}] {pair}: no data in {elapsed:.2f}s")
        else:
            print(
                f"[{index}/{len(pairs)}] {pair}: "
                f"{result.total_trades} trades, {result.total_pnl_pips:+.1f} pips in {elapsed:.2f}s"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
