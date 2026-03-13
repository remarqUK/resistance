# FX S/R Zone Trading System

Mean reversion trading system for forex, based on the NickShawnFX support/resistance workflow. The executable implementation now uses SQLite cache plus IBKR data only. There is no Yahoo Finance fallback anywhere in the strategy stack.

## Performance (365-day backtest, March 13, 2026)

Current benchmark: `high_volume` (repo default profile)

| Profile | Trades | Win rate | Return | Max drawdown | Max streak | Final balance |
|---------|--------|----------|--------|--------------|------------|---------------|
| `high_volume` | 944 | 46.0% | +4,731,530.9% | 19.9% | 8 | GBP 47,316,309.06 |

`high_volume` uses dynamic equity-curve risk sizing: 8% risk at equity highs, scaling linearly down to 0.5% during drawdowns (5-18% DD range). This preserves aggressive compounding during winning runs while capping drawdown at 20%.

All runs use the conservative execution model: IBKR `MIDPOINT` bars, `0.6` pip spread, `0.2` pip stop slippage, and worst-case same-bar TP/SL resolution to `SL`.

Assumptions: 22 FX pairs (`18` active after pair-direction blocking), `zone_history_days=180`, `GBP 1,000` starting balance. Avg win: +1.06R, avg loss: -0.45R.

## Named Profiles

| Profile | Purpose | Key differences |
|---------|---------|-----------------|
| `high_volume` | Primary profile — highest trade count with dynamic risk | `rr=1.1`, `zp=0.42`, `mom=0.6`, 8% base risk with DD-scaled floor at 0.5%, 17 pair blocks |
| `optimized` | Best risk-adjusted with strict filters | `rr=1.3`, `zp=0.55`, `body=0.15`, 26 pair blocks, lowest trade count |
| `source` | Closest to the original NickShawnFX 1:1 playbook | `rr=1.0`, `early_exit=0.4`, `max_correlated_trades=3` |
| `balanced` | Conservative fixed-risk alternative | `rr=1.2` profile with moderate filters |
| `aggressive` | Highest-return fixed-risk variant | `sl_buffer=0.10`, `body=0.10`, highest drawdown of the fixed-risk profiles |

## Quick Start

```bash
pip install -r requirements.txt
```

Fresh downloads and `--no-cache` refreshes require TWS or IB Gateway to be running. Cached backtests can run without an active IBKR session.

### 1. Download data

```bash
python run.py download
python run.py download --pair EURUSD --days 365
python run.py l2 --pair EURUSD --once
python run.py l2 --pair EURUSD --seconds 300 --interval 1
```

### 2. Backtest

```bash
python run.py backtest --days 365 --balance 1000 --risk-pct 5
python run.py backtest --preset source
python run.py backtest --preset aggressive
python run.py backtest --pair EURUSD -v
python run.py backtest --no-cache
python run.py backtest --preset source --rr-ratio 1.2
```

| Flag | Default | Description |
|------|---------|-------------|
| `--pair` | all 10 | Specific pair (for example `EURUSD`) |
| `--days` | 30 | Days of hourly data for execution |
| `--zone-history` | 180 | Days of daily data for zone detection |
| `--preset` | `high_volume` | Named profile: `high_volume`, `optimized`, `source`, `balanced`, or `aggressive` |
| `--rr-ratio` | preset value | Override preset risk:reward ratio |
| `--sl-buffer` | preset value | Override preset SL buffer % beyond zone |
| `--early-exit` | preset value | Override preset early exit R-multiple |
| `--cooldown-bars` | preset value | Override preset bars between entries |
| `--min-entry-body` | preset value | Override preset minimum entry candle body/range ratio |
| `--momentum-lookback` | preset value | Override preset momentum lookback |
| `--max-correlated-trades` | preset value | Override preset correlation cap |
| `--no-time-filters` | off | Disable blocked hours/days entry filters |
| `--no-pair-direction-filter` | off | Disable weak pair-direction blocks |
| `--blocked-hours` | `21 2 3 4` | Override blocked UTC hours |
| `--blocked-days` | `0` | Override blocked weekdays (Monday=0) |
| `--balance` | none | Starting balance for compounding P&L |
| `--risk-pct` | 5.0 | Risk per trade as % of balance |
| `--no-cache` | off | Bypass SQLite cache and refresh from IBKR |
| `-v` | off | Show individual trade details |

### 3. Live scanner

```bash
python run.py live --once
python run.py live --preset aggressive --once
python run.py live --once --balance 10000 --risk-pct 2
python run.py live
python run.py live --zones
python run.py live --pair EURUSD --interval 30
python run.py live --no-positions
```

### 4. L2 capture

```bash
python run.py l2 --pair EURUSD --once
python run.py l2 --pair EURUSD --seconds 300 --interval 1
python run.py l2 --pair EURUSD --summary
```

This captures IBKR market-depth snapshots into SQLite. Each snapshot stores top-of-book summary plus individual bid/ask levels.

### 5. Interactive chart

```bash
python run.py viz
python run.py viz --refresh
python run.py viz --port 3000
```

This starts a local HTTP server serving `chart.html` with Lightweight Charts. Press `Ctrl+C` to stop.

## Architecture

```text
run.py                  CLI entry point (download, backtest, live, viz)
export_viz.py           Backtest -> viz_data.json exporter
chart.html              Lightweight Charts interactive dashboard

fx_sr/
  config.py             Pair definitions and shared defaults
  strategy.py           Entry signals, exits, strategy parameters
  levels.py             S/R zone detection (pivots, clustering, touch counting)
  backtest.py           Walk-forward backtesting engine
  live.py               Real-time opportunity scanner
  l2.py                 L2 market-depth capture and formatting helpers
  data.py               Data layer: SQLite cache -> IBKR
  db.py                 SQLite OHLC + L2 cache (fx_data.db)
  ibkr.py               Interactive Brokers TWS connection (ib_async)
  positions.py          IBKR position tracking and exit monitoring
  param_sweep.py        Parameter optimization sweeps
```

### Data Flow

```text
SQLite cache (fx_data.db)
    |  (used when fresh)
    v
IBKR TWS / Gateway
    |
    v
Daily OHLC --> Zone detection (pivots + clustering)
    +
Hourly OHLC --> Signal generation (reversal candles in zones)
    +
L2 depth snapshots --> SQLite archive for order-book research
    |
    v
Backtest engine / Live scanner / Position monitor
    |
    v
viz_data.json --> chart.html
```

## Supported Pairs (22)

**Majors:** `EURUSD`, `USDJPY`, `GBPUSD`, `USDCHF`, `AUDUSD`, `USDCAD`, `NZDUSD`

**Crosses:** `EURGBP`, `EURJPY`, `GBPJPY`, `AUDJPY`, `CADJPY`, `CHFJPY`, `EURAUD`, `EURCAD`, `EURCHF`, `GBPAUD`, `GBPCAD`, `GBPCHF`, `AUDNZD`, `NZDJPY`, `AUDCAD`

## IBKR Setup

1. Enable API in TWS: `Configure > API > Settings`
2. Check `Enable ActiveX and Socket Clients`
3. Socket port: `7497` (paper) or `7496` (live)
4. The system uses `clientId 60` for data and monitoring

Position tracking is read-only. The system monitors existing positions and alerts on exit conditions, but does not place orders.
When position tracking is enabled, live scans suppress new entry signals on pairs that already have an open IBKR position so the monitor does not suggest stacking into an existing trade.
Live scans now use the same risk-per-trade compounding helper as backtests to suggest FX unit size per signal. If `--balance` is omitted in `live`, the tool tries to use IBKR `NetLiquidation`; `--account-currency` can override the detected currency.

## Dependencies

- Python 3.10+
- `ib_async>=2.1.0`
- `pandas>=2.0.0`
- `numpy>=1.24.0`
- `tabulate>=0.9.0`

See [STRATEGY.md](STRATEGY.md) for implementation details and [STRATEGY_RULES.txt](STRATEGY_RULES.txt) for the raw source playbook extracted from the videos.

