# Windows Backfill Handoff

Use this file as the handoff point after running the minute-data backfill from Windows.

## Goal

Fill `fx_data.db` with `1m` bars for the last `365` days so the strict backtest has historical submit-time quotes.

## Expected Port

- IB Gateway paper: `4002`
- IB Gateway live: `4001`
- TWS paper: `7497`
- TWS live: `7496`

This repo currently expects IB Gateway paper on `4002` unless overridden.

## Command To Run On Windows

From the repo root in PowerShell:

```powershell
$env:IBKR_PORT="4002"
python run.py download --minute-days 365 --minute-only
```

From the repo root in `cmd.exe`:

```bat
set IBKR_PORT=4002
python run.py download --minute-days 365 --minute-only
```

## What Success Looks Like

- The command prints minute-chunk progress instead of failing immediately.
- `fx_data.db` ends up with `1m` coverage well before March 2026.
- No `ConnectionRefusedError` for `127.0.0.1:4002`.

## After It Finishes

Tell me:

1. Whether the command completed successfully.
2. The final few lines of output.
3. Whether the cache summary now shows `1m` coverage for older dates.

If you want a quick DB check from Windows after the backfill, run:

```powershell
python -c "import sqlite3; c=sqlite3.connect('fx_data.db'); cur=c.cursor(); print(cur.execute(\"select ticker,min(ts),max(ts),count(*) from ohlc where interval='1m' group by ticker order by ticker\").fetchall())"
```

## What I Will Do Next

Once you tell me the backfill succeeded, I will:

1. Rerun the 365-day backtest.
2. Compare the trade count against the earlier `900+` benchmark.
3. Check whether any remaining gap is code logic or data coverage.
