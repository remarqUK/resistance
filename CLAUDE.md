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
| `force_close_end` | Both `True` | |
| `on_bar` | Both `None` | No per-pair cooldown in walk-forward |
| `is_entry_blocked` | Both `None` | Cooldown handled at portfolio level |
| `execution_mode` | Same value from profile | `'intrabar'` or `'next_bar'` |

### Acceptable live-only differences

These are real-time constraints that cannot be eliminated:

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

### Trade snapshot logging

`StrategyParams.trade_snapshot_logging` (default `True`) writes a JSON file to `logs/` for every trade entry and exit in the walk-forward — both backtest and live. Files are named `{source}-{event}-{pair}-{timestamp}.json`.

Each file contains the full state at the moment of the trade: signal, trade, execution quote, execution plan, bar OHLC, zones, exit reason. This enables direct comparison between backtest and live to verify signal parity.

To compare: run a backtest, then start live on the same profile/days. Diff the JSON files for the same pair and timestamp. Fields should match exactly (except for acceptable live-only differences listed above).

Set `trade_snapshot_logging: false` in the profile or params to disable once parity is confirmed. The `logs/` directory is gitignored.

## Frontend

The dashboard is a **React SPA** (`frontend/live-dashboard/src/`), built with Vite and served as static files from `fx_sr/web_live/react/`. All frontend page changes must go in React components (`.tsx` files in `frontend/live-dashboard/src/pages/`), **not** in vanilla HTML files in `fx_sr/web_live/`.

- Vanilla JS files in `fx_sr/web_live/` (e.g. `replay.js`, `live_diary.js`, `diary_shared.js`) are loaded by React components via `<script>` tags — JS logic changes still go there.
- HTML structure changes must go in the `.tsx` components, not `.html` files.
- After React changes, run `npm run build` from the repo root to rebuild the SPA.
- Routes are defined in `frontend/live-dashboard/src/App.tsx` and served via `_index` (the SPA shell) in `fx_sr/live_web.py`.
