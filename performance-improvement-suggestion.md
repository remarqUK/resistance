# Performance Improvement Suggestions

Scope: live scan path only (`collect_scan_rows` -> `_scan_pair` -> `run_walk_forward`) because backtests are infrequent.

## Highest-ROI optimization (best return first)

1. **Cache walk-forward results per pair across scan cycles and avoid recompute when no signal-affecting input changed**
- Current hot path is repeated full `run_walk_forward` calls in `fx_sr/live.py`.
- Tighten cache keying to invalidate only on completed-hour changes (and strategy-relevant inputs), not every minute/minute freshness tick.
- Keep current output semantics by reusing prior computed signal unless new completed hourly context arrives.

## Next-high ROI

2. **Bound walk-forward input window to only the required lookback**
- In `_scan_pair`, consider trimming `scan_df` to a window covering decision-critical history instead of passing the full hourly history.
- Useful window anchor: `max(scan_lookback_bars, max_hold_bars + cooldown bars) + small buffer` from `StrategyParams`.
- This reduces loop iterations per cycle.

## Medium impact, low risk

3. **Memoize daily zone detection by `(pair, date)` in live scans**
- `_scan_pair` recalculates zones repeatedly for the same date in each scan.
- Store lookup map in-module for repeated date hits within the live session.
- Invalidate cache only when daily data for that pair extends.

4. **Make execution quote lookups index-driven**
- `execution_quote_provider` currently queries DataFrames for each candidate.
- Prebuild timestamp-index structures per pair per cycle (or cache) for `minute_df` and `l2_snapshots` so quote retrieval is O(1)-ish.
- Do not change planning logic, only lookup mechanics.

## Small but useful

5. **Reduce per-bar row-object allocations in walk-forward callbacks**
- In paths that don’t need per-bar callbacks (`on_bar` is `None`), avoid constructing `WalkForwardBar` payloads each bar.
- Keeps CPU/memory overhead down while preserving behavior.

6. **Keep static strategy checks outside the bar loop**
- Precompute/caching for run-stable predicates (pair-direction blocks/time filter settings) so only stateful checks run per bar.

## Operational tuning

7. **Tune process/thread parallelism for live scan workload**
- For environments with multiple cores, run pair scans with a configuration that avoids IBKR/cache contention while maximizing CPU use.
- Apply only if you see queueing or long scan tails in your environment.

## Implementation order suggestion

1. Cache-key tightening (live scan gate)  
2. Window bounding
3. Zone cache by pair/date
4. Quote indexing optimization
5. Callback elimination for no-`on_bar` path
6. Static-filter hoisting

This order keeps behavior unchanged while attacking the largest computational component first.
