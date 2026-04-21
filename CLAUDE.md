# CLAUDE.md

## Database

This project uses **PostgreSQL only**. There is no SQLite anywhere in the stack. Do not reference SQLite, `fx_data.db`, or `sqlite3` in code, docs, or suggestions. A legacy migration script exists at `scripts/migrate_sqlite_to_postgres.py` but is historical only.

## Backtest / Live Signal Parity

**The backtest is the source of truth.** The live walk-forward must produce identical signals to the backtest given the same data. Any change to signal generation, zone detection, entry logic, or execution quote resolution must be made in the shared code (`walkforward.py`, `strategy.py`, `execution.py`, `levels.py`) and tested via backtest first.

### Architecture

Both backtest and live call the same `run_walk_forward()` in `fx_sr/walkforward.py`. The backtest sets up the call in `fx_sr/backtest.py:run_backtest()`. The live system sets up the call in `fx_sr/live.py:_scan_pair()`. These two call sites **must pass identical parameters** — any divergence causes cascading signal differences.

### Parameters that must match

| Parameter | Source | Notes |
|-----------|--------|-------|
| `hourly_df` | Same `hourly_days` from profile | Live passes `hourly_days` through from `run.py` |
| `zone_provider` | Both use `slice_daily_window()` + `detect_zones()` | Same `zone_history_days` |
| `execution_quote_provider` | Both use `historical_execution_quote()` | Same `l2_snapshots`, `minute_df`, `allow_h1_fallback` |
| `minute_df` | Window matches `hourly_days` | Live: `_get_live_minute_data(days=hourly_span_days)` |
| `l2_snapshots` | Both load from PostgreSQL via `load_l2_snapshots()` | Live loads for the scan_df window |
| `allow_h1_fallback` | `not params.strict_backtest_execution and params.allow_h1_execution_fallback` | Never hardcode `True` in live |
| `force_close_end` | Backtest `True`, Live `False` | Live needs open trades preserved to surface signals |
| `on_bar` | Both `None` | No per-pair cooldown in walk-forward |
| `is_entry_blocked` | Both `None` | Cooldown handled at portfolio level |
| `execution_mode` | Same value from profile | `'intrabar'` or `'next_bar'` |

### Acceptable live-only differences

These are real-time constraints that cannot be eliminated:

- **`force_close_end=False` in live**: The backtest uses `True` to force-close open trades at the window end for clean stats. Live uses `False` so `wf_result.open_trade` preserves the current open trade for signal detection. With `True`, `open_trade` is always `None` and live can never detect signals.
- **Completed-bar filtering** (`next_bar` mode): Live excludes the currently forming hourly bar. The backtest processes all bars because they're historical.
- **Submit-time repricing**: Live reprices signals with a real IBKR quote at order submission via `_prepare_execution_plan()`. This may reject signals the walk-forward accepted (spread too wide, entry drift).
- **Margin/funding**: `whatif_margin_check`, margin slot budget, and excess liquidity checks may skip signals.
- **Portfolio cooldowns**: Applied at execution level in `execute_signal_plans()`, not inside the walk-forward.
- **Position blocking**: Can't enter a pair that already has an open position.

### Rules for contributors

1. **Never add live-only logic inside `_scan_pair()`'s walk-forward call.** If the backtest doesn't do it, live shouldn't either.
2. **Never hardcode parameters** in the live execution_quote_provider that differ from the backtest's. Use `params.*` fields.
3. **Thread new parameters** through the full chain: `run.py` → `live_web.py` → `live_dashboard.py` → `live.py:run_monitor_cycle()` → `collect_scan_rows()` → `_scan_pair()`.
4. **Test signal parity** by running a backtest and comparing its trades with live signals on the same data window.

### Trade storage

Backtest trades are persisted to **PostgreSQL** via the backtest-result cache. They are never written to `logs/` as JSON. For backtest vs live comparisons, read the backtest trades from the database.

Live trades are optionally dumped to `logs/YYYY-MM/DD/` as `live-{event}-{pair}-{timestamp}.json` when `StrategyParams.trade_snapshot_logging` is `True` (default). Each file contains the full state at the moment of the trade: signal, trade, execution quote, execution plan, bar OHLC, zones, exit reason. These are real-time-observability artifacts, not the system of record. The `logs/` directory is gitignored. Set `trade_snapshot_logging: false` to disable.

### Bar-close contract: 1m, not 1h

The walk-forward's decision unit is the **closed 1m bar**, not the closed 1h bar. Zones come from daily (and 1h context); entry triggers come from 1m bar closes. Live can therefore evaluate the currently-forming 1h bar using only the 1m bars inside it that have already closed.

Concretely:

- `_scan_pair` passes the raw `hourly_df` (including the forming bar) to the walk-forward — it does **not** strip the forming hour.
- `run_walk_forward` detects `_last_bar_is_forming` by checking whether the final hourly bar's close time is in the future. For backtest inputs this is always False; for live inputs during the current hour it is True.
- For a forming last bar, the walk-forward runs **only** the intrabar entry path (`find_intrabar_signal` against closed 1m bars) and skips the `select_entry_signal` hourly fallback — the fallback would read the bar's partial OHLC as if finalized and could fire a phantom signal that mutates as more minutes arrive.
- All exit logic in intrabar mode already uses minute-derived values; the hourly-OHLC exit fallback is only reached when minute data is unavailable, which never happens in a healthy live setup.

### Future refactor — 1m-native walk-forward

Today the walk-forward's outer loop iterates 1h bars and its inner intrabar pass walks 1m bars inside each. That structure is a performance optimisation (one outer call per ~60 inner evaluations) rather than a strategy requirement. A fuller refactor would collapse the nested loop and iterate 1m bars natively, resolving hourly/daily context lazily per minute — making backtest and live structurally identical and eliminating forming-bar handling entirely. The current shape is correct and sufficient; the 1m-native form is a cleanup worth doing if the intrabar logic ever needs to diverge further from the 1h cadence (e.g., finer exit granularity, per-minute zone checks, event-driven scanners).

## Zone Detection

Support/resistance zones are detected from daily OHLC in `fx_sr/levels.py`.

### Algorithm

1. **Pivot detection**: `find_daily_pivots()` identifies swing highs/lows using a 5-bar left/right window (configurable via `pivot_window`)
2. **DBSCAN clustering**: `_cluster_pivots_dbscan()` groups nearby pivots using density-based clustering (`sklearn.cluster.DBSCAN`, `min_samples=1`). The `cluster_tolerance` parameter (default 0.08%) is converted to an absolute price distance via the median pivot price. Unlike the previous greedy sequential clustering, DBSCAN is order-independent and produces consistent results regardless of data ordering.
3. **Zone bounds**: Resistance = `[min(body_tops), max(highs)]`, Support = `[min(lows), max(body_bottoms)]`
4. **Width filter**: Zones wider than `max_zone_width_pct` (default 0.35%) are discarded
5. **Touch counting**: Daily candles that enter and bounce from the zone are counted
6. **Strength grading**: Zones with 3+ touches are "major" (tradeable), others are "minor"
7. **Merging**: Overlapping zones are combined; post-merge width filter re-applied

### Key Parameters

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `pivot_window` | `5` | Bars left/right for pivot detection |
| `cluster_tolerance` | `0.08` | DBSCAN eps as % of median price |
| `max_zone_width_pct` | `0.35` | Reject zones wider than this % |
| `zone_history_days` | `180` | Daily lookback for zone detection |
| `max_sl_pct` | `0.25` | Skip signals where SL distance > this % of entry |

## Strategy: TP/SL Calculation

The default `high_volume` profile uses a multi-layer exit system built on S/R zone entries.

### Stop Loss — ATR-based (`sl_mode='atr'`)

SL is placed beyond the zone edge by an ATR-scaled buffer instead of a fixed percentage:
- **LONG**: `sl = zone.lower - ATR(14) * atr_sl_multiplier`
- **SHORT**: `sl = zone.upper + ATR(14) * atr_sl_multiplier`

ATR is pre-computed once per walk-forward from hourly OHLC (`fx_sr/atr.py:compute_atr()`). Falls back to fixed `sl_buffer_pct` when ATR is unavailable (first 14 bars). The `max_sl_pct` filter still rejects trades where SL distance exceeds a threshold.

### Take Profit — RR ratio

TP is set at `entry ± risk * rr_ratio` (default 1.1R). The optional `tp_mode='zone'` targets the opposing S/R zone edge instead, with a minimum RR floor (`tp_zone_min_rr`).

### Partial Close

When `partial_close_enabled=True`, the walk-forward splits a trade at an intermediate target:
1. Price reaches `partial_close_target_r` (default 1.0R)
2. A copy of the trade is finalized as `PARTIAL_TP` with `position_fraction` (default 0.5)
3. The remainder continues with SL moved to breakeven and `is_remainder=True`

Split trades share a `trade_group_id`. In portfolio simulation, remainder trades inherit their group's admission — they don't consume a separate correlation or margin slot.

### Trailing Stop

After partial close, the remainder's SL can trail via `trailing_mode`:
- `'breakeven'` — SL at entry price (default for `high_volume`)
- `'fixed_r'` — SL trails at `best_price - trailing_fixed_r * risk`, floored at entry
- `'atr'` — SL trails at `best_price - entry_atr * trailing_atr_multiplier`, floored at entry

Trailing activates after price reaches `trailing_activate_r` in profit. Gated by `trailing_requires_partial` (default `True`) so only remainder trades trail.

### Configurable Modes

All TP/SL behavior is configurable per profile via `StrategyParams`. Key fields:

| Field | Default | Purpose |
|-------|---------|---------|
| `sl_mode` | `'atr'` | `'fixed'` or `'atr'` |
| `atr_period` | `14` | ATR lookback in hourly bars |
| `atr_sl_multiplier` | `1.0` | SL = zone_edge ± ATR * this |
| `tp_mode` | `'rr'` | `'rr'` or `'zone'` |
| `partial_close_enabled` | `True` | Split trade at intermediate target |
| `partial_close_fraction` | `0.5` | Fraction to close at target |
| `partial_close_target_r` | `1.0` | R-multiple trigger for partial |
| `trailing_mode` | `'breakeven'` | `'none'`, `'breakeven'`, `'fixed_r'`, `'atr'` |

### Profile Inheritance

Profiles can use `'_base': 'profile_name'` to inherit all settings from a parent and override specific fields. `get_profile()` resolves this. A/B test profiles (`high_volume_zone_tp`, `high_volume_full`, etc.) use this to extend `high_volume`.

## Frontend

The dashboard is a **React SPA** (`frontend/live-dashboard/src/`), built with Vite and served as static files from `fx_sr/web_live/react/`. All frontend page changes must go in React components (`.tsx` files in `frontend/live-dashboard/src/pages/`), **not** in vanilla HTML files in `fx_sr/web_live/`.

- Vanilla JS files in `fx_sr/web_live/` (e.g. `replay.js`, `live_diary.js`, `diary_shared.js`) are loaded by React components via `<script>` tags — JS logic changes still go there.
- HTML structure changes must go in the `.tsx` components, not `.html` files.
- After React changes, run `npm run build` from the repo root to rebuild the SPA.
- Routes are defined in `frontend/live-dashboard/src/App.tsx` and served via `_index` (the SPA shell) in `fx_sr/live_web.py`.
