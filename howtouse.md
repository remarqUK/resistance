# How To Use the Live System

This project has two distinct live modes:

- `scanner only`: detect and record new signals, but do not place orders
- `automatic execution`: detect signals, submit trades, and track the full trade lifecycle

The same database records both the detected signal and what later happened to it.

## 1. Prerequisites

You need:

- Python dependencies installed
- IBKR TWS or IB Gateway running
- API access enabled in TWS
- FX market data permissions in IBKR

The default IBKR setup in this project is:

- IB Gateway paper port: `4002`
- IB Gateway live port: `4001`
- TWS paper port: `7497`
- TWS live port: `7496`
- default client ID: `60`

Host and port come from environment variables. `client_id` can also be overridden on the CLI.

Example shell setup:

```bash
export IBKR_PORT=4002
export IBKR_CLIENT_ID=60
export IBKR_ACCOUNT_CURRENCY=GBP
```

`4002` is the safe default for IB Gateway paper trading. If you use TWS instead, set `IBKR_PORT=7497`. Live trading uses `4001` on IB Gateway or `7496` on TWS.

For strict execution-parity backtests, backfill minute data separately:

```bash
python run.py download --minute-days 365 --minute-only
```

That command fills the `1m` cache in 7-day IBKR chunks so backtests have historical submit-time quotes.

## 2. First Run: Scanner Only

Run one scan without placing orders:

```bash
python run.py live --once --balance 10000 --account-currency GBP
```

What this does:

- scans configured pairs for valid entry signals
- sizes the signals
- writes them to the `detected_signal` table in PostgreSQL
- does not submit any orders

If you want one pair only:

```bash
python run.py live --pair EURUSD --once --balance 10000 --account-currency GBP
```

If you want a different preset:

```bash
python run.py live --preset aggressive --once --balance 10000 --account-currency GBP
```

## 3. Continuous Monitoring

Run the live dashboard and rescan continuously:

```bash
python run.py live --interval 60 --balance 10000 --account-currency GBP
```

Default dashboard URL:

```text
http://localhost:8765
```

What this does:

- rescans every `60` seconds
- tracks open IBKR positions
- suppresses duplicate entries on pairs with open positions or pending orders
- records each detected signal in the database

Do not use `--no-positions` in normal live operation. Position tracking is what links broker positions back to the original detected signal and later marks them closed correctly.

### 3b. Dashboard controls and views

- Header controls:
  - **Live status pill** on the left side of the control row
  - **Pause Entries** button to stop/resume paper/live entries
  - **Stop Server** to request a shutdown
  - **Strategy Replay**, **All Backtest Trades**, **Trade Diary** links on the far right
- Trade Diary (`/backtest-diary`) now includes:
  - cached-backtest run selector
  - transaction calendar
  - daily drill-down list for the selected date

## 4. Automatic Execution

To let the system place trades automatically, add `--paper-trade`:

```bash
python run.py live --interval 60 --paper-trade --balance 10000 --account-currency GBP
```

What this adds:

- submits market-entry FX bracket orders
- stores broker order IDs
- records whether the detected signal was actually transacted
- updates the signal row when the position opens
- records exit signals, close reason, close price, close source, and final P&L in pips

Execution only happens when:

- `--paper-trade` is passed
- balance is known
- account currency is known
- the signal passes duplicate-position, pending-order, risk, and correlation checks

## 5. What Gets Recorded

The main audit table is `detected_signal` in PostgreSQL.

Important fields:

- `status`
- `transacted`
- `execution_enabled`
- `planned_units`
- `order_id`
- `take_profit_order_id`
- `stop_loss_order_id`
- `opened_price`
- `open_units`
- `exit_signal_reason`
- `closed_price`
- `close_reason`
- `close_source`
- `pnl_pips`

Typical status flow:

- `DETECTED`: signal found by the scanner
- `SKIPPED`: signal not sent for execution
- `FAILED`: execution attempt failed
- `SUBMITTED` or `PRESUBMITTED`: order sent to IBKR
- `OPEN`: broker position detected and linked back to the signal
- `EXIT_SIGNAL`: strategy decided the trade should be exited
- `CLOSED`: position no longer exists and final outcome has been recorded

`transacted = 1` means the signal went through the execution path. It does not mean the trade is still open.

## 6. How Positions Are Tracked

When execution is enabled, the system:

1. records the detected signal
2. submits the bracket order
3. stores IBKR order IDs and fill price
4. detects the live broker position
5. links that position back to the original `signal_id`
6. monitors strategy exit conditions
7. detects when the broker position disappears
8. records how the trade closed

Close attribution works in this order:

- TP fill from IBKR child order
- SL fill from IBKR child order
- completed IBKR order history
- opposite-side broker fill as manual close
- last strategy exit signal as fallback
- `EXTERNAL_CLOSE` if no better evidence exists

## 7. How To Inspect Recent Signal History

Recent lifecycle rows:

```bash
python3 - <<'PY'
import psycopg
from fx_sr.db import get_connection_string

conn = psycopg.connect(get_connection_string())
for row in conn.execute("""
    SELECT pair, direction, signal_time, status, transacted,
           opened_price, closed_price, close_reason, close_source, pnl_pips
    FROM detected_signal
    ORDER BY detected_at DESC
    LIMIT 20
"""):
    print(row)
PY
```

Only closed trades:

```bash
python3 - <<'PY'
import psycopg
from fx_sr.db import get_connection_string

conn = psycopg.connect(get_connection_string())
for row in conn.execute("""
    SELECT pair, direction, signal_time, opened_price, closed_price,
           close_reason, close_source, pnl_pips
    FROM detected_signal
    WHERE status = 'CLOSED'
    ORDER BY closed_at DESC
    LIMIT 20
"""):
    print(row)
PY
```

## 8. Recommended Rollout

Use this order:

1. `scanner only` with `--once`
2. continuous scanner with `--interval 60`
3. paper execution with `--paper-trade` on port `7497`
4. only after that, consider switching IBKR to a live trading session

Recommended first commands:

```bash
python run.py live --once --balance 10000 --account-currency GBP
python run.py live --interval 60 --balance 10000 --account-currency GBP
python run.py live --interval 60 --paper-trade --balance 10000 --account-currency GBP
```

## 9. Common Problems

`--paper-trade` exits immediately:

- balance or account currency could not be resolved
- pass both `--balance` and `--account-currency`

No signals are being acted on:

- the signal may be recorded as `DETECTED` or `SKIPPED`
- the pair may already have an open position
- there may already be a pending IBKR order
- the signal may fail correlation or portfolio-risk checks

Signals are recorded but no position is linked:

- confirm TWS or Gateway is connected
- confirm API is enabled
- confirm the session is using the expected port and client ID

## 10. Minimal Safe Commands

Scanner only:

```bash
python run.py live --once --balance 10000 --account-currency GBP
```

Continuous scanner:

```bash
python run.py live --interval 60 --balance 10000 --account-currency GBP
```

Automatic paper execution:

```bash
python run.py live --interval 60 --paper-trade --balance 10000 --account-currency GBP
```
