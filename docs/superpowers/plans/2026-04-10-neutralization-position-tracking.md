# Neutralization Position Tracking — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent currency neutralization via IDEALPRO from creating phantom trades that `sync_positions` brackets and monitors.

**Architecture:** After an IDEALPRO neutralization fill, record the resulting pair+direction in a new `neutralization_position` DB table. `sync_positions` loads this set at the start of each cycle and skips any matching IBKR position — no `open_trades` insert, no bracket submission. Cleanup removes stale records when the virtual position disappears from IBKR. A blocked-pair guard in `sync_positions` provides a secondary safety net.

**Tech Stack:** Python, PostgreSQL (psycopg), existing `fx_sr.db` helpers

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `fx_sr/positions.py` | Modify | Add table creation, load/save/cleanup helpers, filter in `sync_positions` |
| `fx_sr/ibkr.py` | Modify | After IDEALPRO fill, call recording function |
| `tests/test_neutralization_tracking.py` | Create | All tests for the new tracking logic |

---

### Task 1: Create the `neutralization_position` table and CRUD helpers

**Files:**
- Modify: `fx_sr/positions.py:160-214` (inside `_ensure_table`)
- Create: `tests/test_neutralization_tracking.py`

- [ ] **Step 1: Write the failing test for table creation and CRUD**

```python
# tests/test_neutralization_tracking.py
"""Tests for neutralization position tracking."""

import os
import pytest
import psycopg

from fx_sr.positions import (
    load_neutralization_positions,
    record_neutralization_position,
    remove_neutralization_position,
)

TEST_DB_URL = os.environ.get(
    'RESISTANCE_DATABASE_URL',
    'postgresql://postgres:Harrison12_!@localhost:5432/resistance',
)


@pytest.fixture(autouse=True)
def _clean_neutralization_table():
    """Wipe neutralization_position rows before each test."""
    conn = psycopg.connect(TEST_DB_URL, autocommit=True)
    try:
        conn.execute("DELETE FROM neutralization_position")
    except Exception:
        pass  # table may not exist yet on first run
    finally:
        conn.close()
    yield
    conn = psycopg.connect(TEST_DB_URL, autocommit=True)
    try:
        conn.execute("DELETE FROM neutralization_position")
    except Exception:
        pass
    finally:
        conn.close()


def test_record_and_load():
    record_neutralization_position('GBPJPY', 'LONG', order_id=10175, exchange='IDEALPRO')
    positions = load_neutralization_positions()
    assert ('GBPJPY', 'LONG') in positions


def test_record_is_idempotent():
    record_neutralization_position('GBPJPY', 'LONG', order_id=100)
    record_neutralization_position('GBPJPY', 'LONG', order_id=200)
    positions = load_neutralization_positions()
    assert ('GBPJPY', 'LONG') in positions
    assert len([k for k in positions if k == ('GBPJPY', 'LONG')]) == 1


def test_remove():
    record_neutralization_position('GBPJPY', 'LONG')
    remove_neutralization_position('GBPJPY', 'LONG')
    positions = load_neutralization_positions()
    assert ('GBPJPY', 'LONG') not in positions


def test_remove_nonexistent_is_safe():
    remove_neutralization_position('NZDUSD', 'SHORT')  # should not raise


def test_load_empty():
    positions = load_neutralization_positions()
    assert isinstance(positions, set)
    assert len(positions) == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd H:/source/repos/Resistance && python -m pytest tests/test_neutralization_tracking.py -v`
Expected: ImportError — `load_neutralization_positions` does not exist yet.

- [ ] **Step 3: Add table creation to `_ensure_table` and implement CRUD functions**

In `fx_sr/positions.py`, add the table creation inside `_ensure_table` (after the `open_trades` table and index, before `conn.commit()`):

```python
        conn.execute("""
            CREATE TABLE IF NOT EXISTS neutralization_position (
                pair       TEXT NOT NULL,
                direction  TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                order_id   INTEGER,
                exchange   TEXT,
                PRIMARY KEY (pair, direction)
            )
        """)
```

Then add three module-level functions after `_ensure_tracking_tables`:

```python
def record_neutralization_position(
    pair: str,
    direction: str,
    order_id: int | None = None,
    exchange: str | None = None,
    db_path: str | None = None,
) -> None:
    """Record that a currency neutralization created a virtual FX position."""
    if db_path is None:
        db_path = get_db_path()
    _ensure_table(db_path)
    with db_transaction(db_path) as conn:
        conn.execute(
            """INSERT INTO neutralization_position (pair, direction, order_id, exchange)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (pair, direction) DO UPDATE
               SET order_id = EXCLUDED.order_id,
                   exchange = EXCLUDED.exchange,
                   created_at = NOW()
            """,
            (pair, direction, order_id, exchange),
        )


def load_neutralization_positions(db_path: str | None = None) -> set[tuple[str, str]]:
    """Return the set of (pair, direction) combos that are neutralization virtual positions."""
    if db_path is None:
        db_path = get_db_path()
    _ensure_table(db_path)
    conn = _connect(db_path)
    try:
        cursor = conn.execute("SELECT pair, direction FROM neutralization_position")
        return {(row[0], row[1]) for row in cursor.fetchall()}
    finally:
        conn.close()


def remove_neutralization_position(
    pair: str,
    direction: str,
    db_path: str | None = None,
) -> None:
    """Remove a neutralization position record (e.g. when the virtual position disappears)."""
    if db_path is None:
        db_path = get_db_path()
    _ensure_table(db_path)
    with db_transaction(db_path) as conn:
        conn.execute(
            "DELETE FROM neutralization_position WHERE pair=%s AND direction=%s",
            (pair, direction),
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd H:/source/repos/Resistance && python -m pytest tests/test_neutralization_tracking.py -v`
Expected: All 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add fx_sr/positions.py tests/test_neutralization_tracking.py
git commit -m "feat: add neutralization_position table and CRUD helpers

Track which IBKR positions are virtual FX positions created by
currency neutralization so sync_positions can skip them."
```

---

### Task 2: Record neutralization positions after IDEALPRO fills

**Files:**
- Modify: `fx_sr/ibkr.py:1918-1976`
- Modify: `tests/test_neutralization_tracking.py`

The `neutralize_currency_balance` function needs to record the resulting pair+direction after an IDEALPRO fill. The tricky part: the function works with currency codes (e.g. "JPY", "GBP") and the fill pair may be in either order (GBPJPY or JPYGBP). We need to map back to our standard pair format from `PAIRS`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_neutralization_tracking.py`:

```python
from fx_sr.ibkr import _neutralization_pair_direction


def test_neutralization_pair_direction_buy_jpy():
    """BUY JPY against GBP -> sells GBP, buys JPY -> GBPJPY SHORT (selling GBP)."""
    # When we BUY foreign currency, we SELL base -> SHORT the pair
    pair, direction = _neutralization_pair_direction('JPY', 'GBP', 'SELL', 'GBPJPY')
    assert pair == 'GBPJPY'
    assert direction == 'SHORT'


def test_neutralization_pair_direction_sell_eur():
    """SELL EUR against GBP -> we sell EUR, buy GBP -> EURGBP SHORT."""
    pair, direction = _neutralization_pair_direction('EUR', 'GBP', 'SELL', 'EURGBP')
    assert pair == 'EURGBP'
    assert direction == 'SHORT'


def test_neutralization_pair_direction_buy_usd():
    """BUY USD against GBP via GBPUSD contract with BUY action -> GBPUSD LONG."""
    pair, direction = _neutralization_pair_direction('USD', 'GBP', 'BUY', 'GBPUSD')
    assert pair == 'GBPUSD'
    assert direction == 'LONG'


def test_neutralization_pair_direction_unknown_pair():
    """Unknown pair returns None."""
    result = _neutralization_pair_direction('XYZ', 'GBP', 'BUY', 'XYZGBP')
    assert result is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd H:/source/repos/Resistance && python -m pytest tests/test_neutralization_tracking.py::test_neutralization_pair_direction_buy_jpy -v`
Expected: ImportError — `_neutralization_pair_direction` does not exist.

- [ ] **Step 3: Add the pair-direction resolver and recording call**

In `fx_sr/ibkr.py`, add a helper function right before `neutralize_currency_balance`:

```python
def _neutralization_pair_direction(
    currency: str,
    account_currency: str,
    action: str,
    contract_pair: str,
) -> tuple[str, str] | None:
    """Map a neutralization fill to the standard (pair, direction) it creates.

    Returns (pair, direction) where pair is a key in PAIRS and direction
    is 'LONG' or 'SHORT', or None if the pair is not in our tracked set.
    """
    from .profiles import PAIRS

    # The contract_pair is the symbol used in the IBKR order (e.g. 'GBPJPY').
    # Check both orderings against our known pairs.
    candidates = [contract_pair, contract_pair[3:] + contract_pair[:3]]
    known_pair = None
    for candidate in candidates:
        if candidate in PAIRS:
            known_pair = candidate
            break
    if known_pair is None:
        return None

    # Determine direction: BUY the contract = LONG, SELL = SHORT.
    # But if the contract is reversed vs our known pair, flip.
    if known_pair == contract_pair:
        direction = 'LONG' if action == 'BUY' else 'SHORT'
    else:
        direction = 'SHORT' if action == 'BUY' else 'LONG'

    return (known_pair, direction)
```

Then in `neutralize_currency_balance`, after the successful IDEALPRO fill (after the `log_order_event` call at line ~1954, before the `return` at line ~1969), add recording logic. Find the block that starts with `log_order_event(` (the success path at ~line 1954) and after its closing `)`, add:

```python
            # Record IDEALPRO fills so sync_positions skips the virtual position.
            if contract is not None and contract.exchange == 'IDEALPRO':
                fill_action = action  # the resolved action used for the order
                contract_symbol = (
                    getattr(contract, 'symbol', '') + getattr(contract, 'currency', '')
                )
                pd_result = _neutralization_pair_direction(
                    currency, account_currency, fill_action, contract_symbol,
                )
                if pd_result is not None:
                    from .positions import record_neutralization_position
                    record_neutralization_position(
                        pd_result[0], pd_result[1],
                        order_id=getattr(live_order, 'orderId', None),
                        exchange='IDEALPRO',
                    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd H:/source/repos/Resistance && python -m pytest tests/test_neutralization_tracking.py -v`
Expected: All 9 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add fx_sr/ibkr.py tests/test_neutralization_tracking.py
git commit -m "feat: record IDEALPRO neutralization fills in tracking table

After a currency neutralization falls back to IDEALPRO, record
the resulting pair+direction so sync_positions can skip the
virtual FX position."
```

---

### Task 3: Make `sync_positions` skip neutralization positions and add blocked-pair guard

**Files:**
- Modify: `fx_sr/positions.py:819-951` (`sync_positions`)
- Modify: `tests/test_neutralization_tracking.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_neutralization_tracking.py`:

```python
from unittest.mock import patch, MagicMock
from fx_sr.positions import sync_positions, _load_trades, _ensure_table, load_neutralization_positions
from fx_sr.strategy import StrategyParams


def _make_ibkr_position(pair, size, avg_cost):
    return {'pair': pair, 'size': size, 'avg_cost': avg_cost}


@patch('fx_sr.positions.ibkr')
@patch('fx_sr.positions.reconcile_detected_signal_orders')
def test_sync_skips_neutralization_positions(mock_reconcile, mock_ibkr):
    """sync_positions should not create open_trades for neutralization positions."""
    mock_ibkr.fetch_positions.return_value = [
        _make_ibkr_position('GBPJPY', 30000, 213.76),
    ]
    mock_ibkr.fetch_open_order_counts.return_value = {}

    # Record GBPJPY LONG as a neutralization position
    record_neutralization_position('GBPJPY', 'LONG')

    params = StrategyParams()
    result = sync_positions(params=params)

    # GBPJPY:LONG should NOT appear in tracked trades
    assert 'GBPJPY:LONG' not in result


@patch('fx_sr.positions.ibkr')
@patch('fx_sr.positions.reconcile_detected_signal_orders')
def test_sync_skips_blocked_pair_directions(mock_reconcile, mock_ibkr):
    """sync_positions should not adopt positions for blocked pair+direction combos."""
    mock_ibkr.fetch_positions.return_value = [
        _make_ibkr_position('GBPJPY', 30000, 213.76),  # GBPJPY LONG is blocked
    ]
    mock_ibkr.fetch_open_order_counts.return_value = {}

    params = StrategyParams(use_pair_direction_filter=True)
    result = sync_positions(params=params)

    assert 'GBPJPY:LONG' not in result
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd H:/source/repos/Resistance && python -m pytest tests/test_neutralization_tracking.py::test_sync_skips_neutralization_positions -v`
Expected: FAIL — `sync_positions` currently adopts the position.

- [ ] **Step 3: Modify `sync_positions` to skip neutralization and blocked positions**

In `fx_sr/positions.py`, at the top of `sync_positions` (after `ibkr_positions = ibkr.fetch_positions()` and the None check), add:

```python
    # Load neutralization positions to skip virtual FX positions from currency conversion.
    neutralization_keys = load_neutralization_positions()
```

Add the import at the top of the file:

```python
from .profiles import BLOCKED_PAIR_DIRECTIONS
```

Then in the loop `for key, pos in ibkr_by_key.items():` (around line 888), add a skip check at the very beginning of the loop body, BEFORE the `is_new_position` check:

```python
        direction = 'LONG' if pos['size'] > 0 else 'SHORT'

        # Skip virtual FX positions created by currency neutralization.
        if (pos['pair'], direction) in neutralization_keys:
            # If this phantom position was previously synced into open_trades,
            # clean it up: remove the DB entry and cancel any brackets.
            if key in db_trades:
                info = db_trades[key]
                print(f"    Removing phantom neutralization position: {pos['pair']} {direction}")
                cancel_bracket_children(info.get('signal_id'))
                _cancel_orders_for_pairs({pos['pair']})
                with _tracking_db_transaction() as conn:
                    _remove_trade_conn(conn, pos['pair'], direction)
                del db_trades[key]
            continue

        # Skip positions for blocked pair+direction combos (safety net).
        if params.use_pair_direction_filter and (pos['pair'], direction) in BLOCKED_PAIR_DIRECTIONS:
            if key not in db_trades:
                print(f"    Skipping blocked pair+direction: {pos['pair']} {direction}")
                continue
```

Also add cleanup for stale neutralization records. After the `for key, pos in ibkr_by_key.items():` loop ends (after the full loop block), add:

```python
    # Cleanup: remove neutralization records for positions no longer in IBKR.
    for n_pair, n_dir in list(neutralization_keys):
        n_key = f"{n_pair}:{n_dir}"
        if n_key not in ibkr_by_key:
            remove_neutralization_position(n_pair, n_dir)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd H:/source/repos/Resistance && python -m pytest tests/test_neutralization_tracking.py -v`
Expected: All 11 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add fx_sr/positions.py tests/test_neutralization_tracking.py
git commit -m "feat: sync_positions skips neutralization and blocked-pair positions

- Load neutralization_position set at start of each sync cycle
- Skip any IBKR position matching a neutralization record
- Clean up open_trades and brackets for previously-synced phantoms
- Add BLOCKED_PAIR_DIRECTIONS guard for new unlinked positions
- Remove stale neutralization records when position leaves IBKR"
```

---

### Task 4: Seed migration for existing 5 phantom positions

**Files:**
- Modify: `fx_sr/positions.py` (inside `_ensure_table`)

The 5 current phantom positions need to be inserted into `neutralization_position` so the next `sync_positions` cycle cleans them up.

- [ ] **Step 1: Add one-time migration inside `_ensure_table`**

In `fx_sr/positions.py`, after the `CREATE TABLE IF NOT EXISTS neutralization_position` block and before `conn.commit()`, add:

```python
        # One-time migration: seed neutralization records for the 5 phantom
        # positions created by IDEALPRO currency neutralization on 2026-04-10.
        # These will be cleaned up automatically when the virtual positions
        # disappear from IBKR.
        conn.execute("""
            INSERT INTO neutralization_position (pair, direction, exchange)
            VALUES
                ('GBPJPY', 'LONG', 'IDEALPRO'),
                ('GBPCAD', 'SHORT', 'IDEALPRO'),
                ('GBPAUD', 'LONG', 'IDEALPRO'),
                ('GBPUSD', 'LONG', 'IDEALPRO'),
                ('EURGBP', 'SHORT', 'IDEALPRO')
            ON CONFLICT (pair, direction) DO NOTHING
        """)
```

- [ ] **Step 2: Verify the migration runs cleanly**

Run: `cd H:/source/repos/Resistance && python -c "from fx_sr.positions import _ensure_table, load_neutralization_positions; _ensure_table.__module__; from fx_sr.positions import _TABLE_INIT_PATHS; _TABLE_INIT_PATHS.clear(); _ensure_table(); print(load_neutralization_positions())"`

Expected: Output includes all 5 pairs: `{('GBPJPY', 'LONG'), ('GBPCAD', 'SHORT'), ('GBPAUD', 'LONG'), ('GBPUSD', 'LONG'), ('EURGBP', 'SHORT')}`

- [ ] **Step 3: Run full test suite**

Run: `cd H:/source/repos/Resistance && python -m pytest tests/test_neutralization_tracking.py -v`
Expected: All tests PASS (the fixture cleans neutralization rows before each test, so the seed data doesn't interfere).

- [ ] **Step 4: Commit**

```bash
git add fx_sr/positions.py
git commit -m "fix: seed neutralization records for existing phantom positions

Insert the 5 IDEALPRO virtual positions from 2026-04-10 so the
next sync_positions cycle removes their open_trades entries and
cancels their bracket orders."
```

---

### Task 5: Run existing tests to confirm no regressions

- [ ] **Step 1: Run position-related tests**

Run: `cd H:/source/repos/Resistance && python -m pytest tests/test_position_bar_tracking.py tests/test_live_execution.py tests/test_ibkr.py -v`
Expected: All PASS.

- [ ] **Step 2: Run the new test file one final time**

Run: `cd H:/source/repos/Resistance && python -m pytest tests/test_neutralization_tracking.py -v`
Expected: All PASS.

- [ ] **Step 3: Final commit with all changes**

If any fixups were needed, commit them:

```bash
git add -A
git commit -m "test: verify no regressions from neutralization tracking"
```
