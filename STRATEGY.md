# Strategy Documentation

This document describes the executable implementation in this repo. `STRATEGY_RULES.txt` is the raw source playbook from the videos; the code here uses a tuned variant that keeps the same zone/entry structure while improving the corrected backtest profile.

## Core Concept

Trade mean reversion at obvious support and resistance zones on the daily chart. Enter on the 1-hour chart when price is inside a zone and prints a reversal candle. Cut losers early, hold winners to target.

## Zone Detection

Zones are identified from `180` days of daily OHLC data using pivot clustering:

1. Find pivot highs and lows with a `5` bar left/right window.
2. Build wick-based rejection zones from those pivots.
3. Cluster nearby pivots within `0.08%` of price.
4. Count touches and keep major zones with `3+` touches.
5. Merge overlaps and drop zones wider than `0.35%` of price.
6. Trade only the nearest major support and resistance around current price.

## Entry Rules

Checked on every 1-hour candle:

1. Candle close must be inside a major zone.
2. Price must be at or beyond the zone midpoint.
3. At support, the candle must close bullish for a long.
4. At resistance, the candle must close bearish for a short.
5. Entry candle body must be at least `15%` of candle range by default.
6. Momentum filter checks the previous `2` candles for strong movement into the zone.
7. Cooldown requires `2` hourly bars between entries on the same pair.
8. Pair-direction and time filters block historically weak setups by default.

## Exit Rules

Checked in priority order on each 1-hour bar:

| Priority | Rule | Condition | Exit price |
|----------|------|-----------|------------|
| 1 | Take Profit | High/Low reaches TP | TP price |
| 2 | Friday Close | Friday, in profit, and `>= 70%` to TP | Market price |
| 3 | Stop Loss | Low/High reaches SL | SL price |
| 4 | Early Exit | Close breaks the zone or reaches the configured loss threshold | Market price |
| 5 | Sideways | `15+` bars, `< 30%` progress toward TP | Market price |
| 6 | Time | `72+` bars without resolution and at or worse than entry | Market price |

The active early exit logic now uses both zone failure and `early_exit_r`. That parameter is no longer a no-op.

## Execution Model

Backtests now use a conservative fill model on top of IBKR `MIDPOINT` bars:

- Entry fills include an explicit `0.6` pip spread assumption
- Stop fills include `0.2` pip adverse slippage
- TP fills require the midpoint bar to clear half-spread beyond the target
- If TP and SL both print inside the same hourly bar, the backtest resolves the trade to `SL`

This does not create true tick-level path simulation, but it removes the most optimistic midpoint-bar assumptions from the old engine.

## Current Default Profile

The repo default profile is `high_volume`. It is also the only benchmark maintained as current in the repo docs.

| Parameter | Default |
|-----------|---------|
| `rr_ratio` | `1.1` |
| `sl_buffer_pct` | `0.15` |
| `early_exit_r` | `0.4` |
| `cooldown_bars` | `1` |
| `min_entry_candle_body_pct` | `0.05` |
| `momentum_lookback` | `2` |
| `max_correlated_trades` | `5` |
| `blocked_hours` | `{2, 3}` |
| `blocked_days` | `{}` |
| `risk_pct` | `8.0` |
| `dynamic_risk` | `enabled` |
| `dd_risk_start` | `5.0` |
| `dd_risk_full` | `18.0` |
| `dd_risk_floor` | `0.5` |

Latest direct CLI 365-day result with that profile:

- Raw signals: `944`
- Compounded trades: `944`
- Win rate: `46.0%`
- Compounded return: `+4,731,530.9%`
- Final balance: `GBP 47,316,309.06`
- Max drawdown: `19.9%`
- Max losing streak: `8`
- Assumptions: `GBP 1,000` starting balance, `8%` base risk with drawdown scaling to `0.5%`, `zone_history_days=180`, `0.6` pip spread, and `0.2` pip stop slippage

## Other Profiles

Other profiles remain available in `fx_sr/profiles.py`, but the repo no longer treats their results as the current benchmark.

- `optimized`: strictest filters and lowest trade count
- `source`: closest to the original NickShawnFX 1:1 playbook
- `balanced`: conservative fixed-risk alternative
- `aggressive`: higher-return fixed-risk variant with materially higher drawdown risk

Explicit strategy flags still override any preset values. For example, `--preset source --rr-ratio 1.2` keeps the source preset but replaces only the reward multiple.

## Parameter Sweep

Run:

```bash
python -m fx_sr.param_sweep
```

The sweep now uses the same corrected rolling `zone_history_days` window as the main backtest and starts from the tuned baseline in `StrategyParams`, so the optimizer, runner, and docs use the same defaults.

## Data Model

The strategy stack uses only:

1. SQLite cache for stored OHLC history
2. IBKR for fresh historical and latest-price data

There is no Yahoo Finance path left in the strategy, backtest, live scanner, or optimizer.

## Position Tracking

`positions.py` remains read-only:

- Syncs with IBKR TWS to detect open FX positions
- Matches each position to the nearest S/R zone
- Computes SL/TP levels from the same strategy parameters
- Runs exit rule checks and prints alerts
- Persists tracked state in SQLite for restart resilience
