# Order Audit Log Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist every IBKR order request and response so we can diagnose issues like orphaned brackets, failed cancellations, and OCA misbehavior after the fact.

**Architecture:** A single `order_audit_log` table in PostgreSQL stores one row per IBKR API interaction (submit, cancel, query). A thin `log_order_event()` helper in `fx_sr/ibkr.py` writes to it. Each order-mutating function calls the helper at its exit point (success or failure). The orphaned-order sweep in `positions.py` also logs. All writes are fire-and-forget — audit failures never block trading.

**Tech Stack:** PostgreSQL (TEXT column for JSON payloads, matching existing codebase convention), `json.dumps()`, existing `db.py` helpers.

---

### Task 1: Create the `order_audit_log` table

**Files:**
- Modify: `fx_sr/db.py:414-431` (inside `_init_postgres_schema`)

- [ ] **Step 1: Write the failing test**

In `tests/test_db.py`, add a test that the `order_audit_log` table exists after `init_db()`:

```python
class OrderAuditLogSchemaTests(unittest.TestCase):
    def setUp(self):
        self._db_ctx = temporary_test_database()
        self.db_path = self._db_ctx.__enter__()
        db_module.init_db(self.db_path)

    def tearDown(self):
        if self._db_ctx is not None:
            self._db_ctx.__exit__(None, None, None)

    def test_order_audit_log_table_exists(self):
        conn = db_module._connect(self.db_path)
        try:
            cols = db_module._table_columns(conn, 'order_audit_log')
            self.assertIn('id', cols)
            self.assertIn('event_ts', cols)
            self.assertIn('function_name', cols)
            self.assertIn('pair', cols)
            self.assertIn('direction', cols)
            self.assertIn('action', cols)
            self.assertIn('request_json', cols)
            self.assertIn('response_json', cols)
            self.assertIn('error', cols)
            self.assertIn('duration_ms', cols)
            self.assertIn('order_ids', cols)
        finally:
            conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_db.py::OrderAuditLogSchemaTests::test_order_audit_log_table_exists -v`
Expected: FAIL — `order_audit_log` table doesn't exist yet.

- [ ] **Step 3: Add the table to `_init_postgres_schema`**

In `fx_sr/db.py`, after the `app_settings` CREATE TABLE block (around line 431), add:

```python
    conn.execute("""
        CREATE TABLE IF NOT EXISTS order_audit_log (
            id              BIGSERIAL PRIMARY KEY,
            event_ts        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            function_name   TEXT NOT NULL,
            pair            TEXT,
            direction       TEXT,
            action          TEXT NOT NULL,
            request_json    TEXT,
            response_json   TEXT,
            error           TEXT,
            duration_ms     DOUBLE PRECISION,
            order_ids       TEXT
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_order_audit_log_ts
        ON order_audit_log (event_ts DESC)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_order_audit_log_pair
        ON order_audit_log (pair, event_ts DESC)
    """)
```

Column notes:
- `request_json` / `response_json`: JSON-encoded TEXT (matches codebase convention for `result_json` in `backtest_result`).
- `order_ids`: Comma-separated string of order IDs involved (e.g. `"2063,2064"`) for quick `LIKE` filtering.
- `action`: One of `submit`, `cancel`, `sweep`, `resubmit`, `margin_check`.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_db.py::OrderAuditLogSchemaTests::test_order_audit_log_table_exists -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add fx_sr/db.py tests/test_db.py
git commit -m "feat: add order_audit_log table to PostgreSQL schema"
```

---

### Task 2: Add `log_order_event()` helper to `ibkr.py`

**Files:**
- Modify: `fx_sr/ibkr.py` (add function near top, after imports)
- Test: `tests/test_ibkr.py`

- [ ] **Step 1: Write the failing test**

In `tests/test_ibkr.py`, add:

```python
class LogOrderEventTests(unittest.TestCase):
    def setUp(self):
        self._db_ctx = temporary_test_database()
        self.db_path = self._db_ctx.__enter__()
        db_module.init_db(self.db_path)

    def tearDown(self):
        if self._db_ctx is not None:
            self._db_ctx.__exit__(None, None, None)

    @patch('fx_sr.ibkr.get_db_path')
    def test_log_order_event_inserts_row(self, mock_db_path):
        mock_db_path.return_value = self.db_path
        from fx_sr.ibkr import log_order_event

        log_order_event(
            function_name='submit_fx_market_bracket_order',
            pair='EURUSD',
            direction='LONG',
            action='submit',
            request_data={'quantity': 10000, 'tp': 1.15, 'sl': 1.13},
            response_data={'order_id': 123, 'status': 'Filled'},
            order_ids=[123, 124, 125],
            duration_ms=342.5,
        )

        conn = db_module._connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT function_name, pair, direction, action, request_json, "
                "response_json, order_ids, duration_ms, error "
                "FROM order_audit_log ORDER BY id DESC LIMIT 1"
            ).fetchone()
            self.assertEqual(row[0], 'submit_fx_market_bracket_order')
            self.assertEqual(row[1], 'EURUSD')
            self.assertEqual(row[2], 'LONG')
            self.assertEqual(row[3], 'submit')
            self.assertIn('10000', row[4])  # request_json contains quantity
            self.assertIn('123', row[5])    # response_json contains order_id
            self.assertEqual(row[6], '123,124,125')
            self.assertAlmostEqual(row[7], 342.5)
            self.assertIsNone(row[8])       # no error
        finally:
            conn.close()

    @patch('fx_sr.ibkr.get_db_path')
    def test_log_order_event_stores_error(self, mock_db_path):
        mock_db_path.return_value = self.db_path
        from fx_sr.ibkr import log_order_event

        log_order_event(
            function_name='cancel_orders',
            action='cancel',
            error='Connection lost',
            order_ids=[999],
        )

        conn = db_module._connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT function_name, error, order_ids FROM order_audit_log LIMIT 1"
            ).fetchone()
            self.assertEqual(row[0], 'cancel_orders')
            self.assertEqual(row[1], 'Connection lost')
            self.assertEqual(row[2], '999')
        finally:
            conn.close()

    @patch('fx_sr.ibkr.get_db_path')
    def test_log_order_event_survives_db_failure(self, mock_db_path):
        mock_db_path.return_value = 'postgresql://bad:bad@localhost:1/nope'
        from fx_sr.ibkr import log_order_event

        # Must not raise — audit failures are silent
        log_order_event(
            function_name='test',
            action='test',
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_ibkr.py::LogOrderEventTests -v`
Expected: FAIL — `log_order_event` does not exist.

- [ ] **Step 3: Implement `log_order_event()`**

In `fx_sr/ibkr.py`, add near the top after existing imports (around line 20):

```python
import json

from .db import _connect, get_db_path, init_db
```

Then add the function (after the module-level constants, before the connection helpers):

```python
def log_order_event(
    *,
    function_name: str,
    action: str,
    pair: str | None = None,
    direction: str | None = None,
    request_data: dict | None = None,
    response_data: dict | None = None,
    error: str | None = None,
    order_ids: list[int] | None = None,
    duration_ms: float | None = None,
) -> None:
    """Write one row to order_audit_log. Fire-and-forget — never raises."""
    try:
        db_path = get_db_path()
        init_db(db_path)
        conn = _connect(db_path)
        try:
            conn.execute(
                """
                INSERT INTO order_audit_log
                    (function_name, pair, direction, action,
                     request_json, response_json, error, order_ids, duration_ms)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    function_name,
                    pair,
                    direction,
                    action,
                    json.dumps(request_data, default=str) if request_data else None,
                    json.dumps(response_data, default=str) if response_data else None,
                    error,
                    ','.join(str(i) for i in order_ids) if order_ids else None,
                    duration_ms,
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass  # Audit must never interfere with trading
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_ibkr.py::LogOrderEventTests -v`
Expected: PASS (all 3 tests)

- [ ] **Step 5: Commit**

```bash
git add fx_sr/ibkr.py tests/test_ibkr.py
git commit -m "feat: add log_order_event() helper for IBKR audit trail"
```

---

### Task 3: Audit `submit_fx_market_bracket_order`

**Files:**
- Modify: `fx_sr/ibkr.py:1735-1868` (`submit_fx_market_bracket_order`)
- Test: `tests/test_ibkr.py`

- [ ] **Step 1: Write the failing test**

```python
class SubmitBracketAuditTests(unittest.TestCase):
    def setUp(self):
        self._db_ctx = temporary_test_database()
        self.db_path = self._db_ctx.__enter__()
        db_module.init_db(self.db_path)

    def tearDown(self):
        if self._db_ctx is not None:
            self._db_ctx.__exit__(None, None, None)

    @patch('fx_sr.ibkr.get_db_path')
    @patch('fx_sr.ibkr._get_connection')
    @patch('fx_sr.ibkr.whatif_margin_check', return_value=None)
    def test_submit_bracket_logs_success(self, _margin, mock_conn, mock_db_path):
        mock_db_path.return_value = self.db_path

        # Build mock IB that returns plausible order objects
        ib = MagicMock()
        ib.client.getReqId.side_effect = [100, 101, 102]
        mock_conn.return_value = (ib, True)

        parent_trade = MagicMock()
        parent_trade.orderStatus.status = 'Filled'
        parent_trade.orderStatus.filled = 10000.0
        parent_trade.orderStatus.remaining = 0.0
        parent_trade.orderStatus.avgFillPrice = 1.145
        parent_trade.order.orderId = 100
        parent_trade.order.totalQuantity = 10000.0

        tp_trade = MagicMock()
        tp_trade.order.orderId = 101
        sl_trade = MagicMock()
        sl_trade.order.orderId = 102

        ib.placeOrder.side_effect = [parent_trade, tp_trade, sl_trade]

        from fx_sr.ibkr import submit_fx_market_bracket_order
        result = submit_fx_market_bracket_order(
            pair='EURUSD', direction='LONG', quantity=10000,
            take_profit_price=1.15, stop_loss_price=1.13,
            order_ref='test',
        )

        conn = db_module._connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT action, pair, direction, request_json, response_json, error "
                "FROM order_audit_log LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(row, "Expected an audit log row")
            self.assertEqual(row[0], 'submit')
            self.assertEqual(row[1], 'EURUSD')
            self.assertEqual(row[2], 'LONG')
            self.assertIsNone(row[5])  # no error
        finally:
            conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_ibkr.py::SubmitBracketAuditTests -v`
Expected: FAIL — no audit row written yet.

- [ ] **Step 3: Add audit logging to `submit_fx_market_bracket_order`**

At the top of the function body (line ~1743), capture the start time:

```python
    _audit_start = time.monotonic()
```

At the success return (line ~1850), before `return`:

```python
            log_order_event(
                function_name='submit_fx_market_bracket_order',
                pair=pair,
                direction=direction,
                action='submit',
                request_data={
                    'quantity': quantity,
                    'take_profit_price': take_profit_price,
                    'stop_loss_price': stop_loss_price,
                    'order_ref': order_ref,
                    'rounded_tp': float(rounded_take_profit_price),
                    'rounded_sl': float(rounded_stop_loss_price),
                },
                response_data={
                    'order_id': getattr(parent_live_order, 'orderId', None),
                    'status': getattr(parent_status, 'status', None),
                    'avg_fill_price': getattr(parent_status, 'avgFillPrice', None),
                    'filled_units': filled_units,
                    'tp_order_id': getattr(tp_live_order, 'orderId', None),
                    'sl_order_id': getattr(sl_live_order, 'orderId', None),
                },
                order_ids=[
                    getattr(parent_live_order, 'orderId', None),
                    getattr(tp_live_order, 'orderId', None),
                    getattr(sl_live_order, 'orderId', None),
                ],
                duration_ms=(time.monotonic() - _audit_start) * 1000,
            )
```

At the timeout-cancel path (line ~1837, before `return None`):

```python
                log_order_event(
                    function_name='submit_fx_market_bracket_order',
                    pair=pair,
                    direction=direction,
                    action='submit',
                    request_data={
                        'quantity': quantity,
                        'take_profit_price': take_profit_price,
                        'stop_loss_price': stop_loss_price,
                        'order_ref': order_ref,
                    },
                    error=f'Market order not filled after {fill_timeout}s (status={status_str})',
                    duration_ms=(time.monotonic() - _audit_start) * 1000,
                )
```

At the exception handler (line ~1867, before `return None`):

```python
            log_order_event(
                function_name='submit_fx_market_bracket_order',
                pair=pair,
                direction=direction,
                action='submit',
                request_data={
                    'quantity': quantity,
                    'take_profit_price': take_profit_price,
                    'stop_loss_price': stop_loss_price,
                    'order_ref': order_ref,
                },
                error=str(e),
                duration_ms=(time.monotonic() - _audit_start) * 1000,
            )
```

At the margin-rejection path (line ~1755, before `return None`):

```python
        log_order_event(
            function_name='submit_fx_market_bracket_order',
            pair=pair,
            direction=direction,
            action='submit',
            request_data={
                'quantity': quantity,
                'take_profit_price': take_profit_price,
                'stop_loss_price': stop_loss_price,
            },
            error=f"Margin rejected: equity_after={margin.get('equity_with_loan_after')}, init_margin_after={margin.get('init_margin_after')}",
            duration_ms=(time.monotonic() - _audit_start) * 1000,
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_ibkr.py::SubmitBracketAuditTests -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add fx_sr/ibkr.py tests/test_ibkr.py
git commit -m "feat: audit log submit_fx_market_bracket_order"
```

---

### Task 4: Audit `submit_bracket_for_existing_position`

**Files:**
- Modify: `fx_sr/ibkr.py:1871-1959` (`submit_bracket_for_existing_position`)
- Test: `tests/test_ibkr.py`

- [ ] **Step 1: Write the failing test**

```python
class ResubmitBracketAuditTests(unittest.TestCase):
    def setUp(self):
        self._db_ctx = temporary_test_database()
        self.db_path = self._db_ctx.__enter__()
        db_module.init_db(self.db_path)

    def tearDown(self):
        if self._db_ctx is not None:
            self._db_ctx.__exit__(None, None, None)

    @patch('fx_sr.ibkr.get_db_path')
    @patch('fx_sr.ibkr._get_connection')
    @patch('fx_sr.ibkr.whatif_margin_check', return_value=None)
    def test_resubmit_bracket_logs_success(self, _margin, mock_conn, mock_db_path):
        mock_db_path.return_value = self.db_path

        ib = MagicMock()
        ib.client.getReqId.side_effect = [200, 201, 202]
        mock_conn.return_value = (ib, True)

        tp_trade = MagicMock()
        tp_trade.order.orderId = 201
        sl_trade = MagicMock()
        sl_trade.order.orderId = 202
        ib.placeOrder.side_effect = [tp_trade, sl_trade]

        from fx_sr.ibkr import submit_bracket_for_existing_position
        result = submit_bracket_for_existing_position(
            pair='GBPUSD', direction='SHORT', quantity=5000,
            take_profit_price=1.30, stop_loss_price=1.33,
        )

        conn = db_module._connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT action, pair, direction, request_json, response_json, error "
                "FROM order_audit_log LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(row, "Expected an audit log row")
            self.assertEqual(row[0], 'resubmit')
            self.assertEqual(row[1], 'GBPUSD')
            self.assertEqual(row[2], 'SHORT')
            self.assertIsNone(row[5])  # no error
        finally:
            conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_ibkr.py::ResubmitBracketAuditTests -v`
Expected: FAIL

- [ ] **Step 3: Add audit logging to `submit_bracket_for_existing_position`**

Same pattern as Task 3. At function entry:

```python
    _audit_start = time.monotonic()
```

At success return (before `return result` around line 1949):

```python
            log_order_event(
                function_name='submit_bracket_for_existing_position',
                pair=pair,
                direction=direction,
                action='resubmit',
                request_data={
                    'quantity': quantity,
                    'take_profit_price': take_profit_price,
                    'stop_loss_price': stop_loss_price,
                    'rounded_tp': float(rounded_tp),
                    'rounded_sl': float(rounded_sl),
                    'oca_group': oca_group,
                },
                response_data={
                    'tp_order_id': getattr(tp_live, 'orderId', None),
                    'sl_order_id': getattr(sl_live, 'orderId', None),
                },
                order_ids=[
                    getattr(tp_live, 'orderId', None),
                    getattr(sl_live, 'orderId', None),
                ],
                duration_ms=(time.monotonic() - _audit_start) * 1000,
            )
```

At exception handler (before `return None`):

```python
            log_order_event(
                function_name='submit_bracket_for_existing_position',
                pair=pair,
                direction=direction,
                action='resubmit',
                request_data={
                    'quantity': quantity,
                    'take_profit_price': take_profit_price,
                    'stop_loss_price': stop_loss_price,
                },
                error=str(e),
                duration_ms=(time.monotonic() - _audit_start) * 1000,
            )
```

At margin-rejection path (before `return None`):

```python
        log_order_event(
            function_name='submit_bracket_for_existing_position',
            pair=pair,
            direction=direction,
            action='resubmit',
            request_data={
                'quantity': quantity,
                'take_profit_price': take_profit_price,
                'stop_loss_price': stop_loss_price,
            },
            error=f"Margin rejected: equity_after={margin.get('equity_with_loan_after')}, init_margin_after={margin.get('init_margin_after')}",
            duration_ms=(time.monotonic() - _audit_start) * 1000,
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_ibkr.py::ResubmitBracketAuditTests -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add fx_sr/ibkr.py tests/test_ibkr.py
git commit -m "feat: audit log submit_bracket_for_existing_position"
```

---

### Task 5: Audit `cancel_orders`

**Files:**
- Modify: `fx_sr/ibkr.py:1962-2010` (`cancel_orders`)
- Test: `tests/test_ibkr.py`

- [ ] **Step 1: Write the failing test**

```python
class CancelOrdersAuditTests(unittest.TestCase):
    def setUp(self):
        self._db_ctx = temporary_test_database()
        self.db_path = self._db_ctx.__enter__()
        db_module.init_db(self.db_path)

    def tearDown(self):
        if self._db_ctx is not None:
            self._db_ctx.__exit__(None, None, None)

    @patch('fx_sr.ibkr.get_db_path')
    @patch('fx_sr.ibkr._get_connection')
    def test_cancel_orders_logs_audit(self, mock_conn, mock_db_path):
        mock_db_path.return_value = self.db_path

        ib = MagicMock()
        mock_conn.return_value = (ib, True)

        # Mock openTrades to return matching trade objects
        trade1 = MagicMock()
        trade1.order.orderId = 500
        trade2 = MagicMock()
        trade2.order.orderId = 501
        ib.openTrades.return_value = [trade1, trade2]

        from fx_sr.ibkr import cancel_orders
        cancel_orders({500, 501})

        conn = db_module._connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT action, order_ids, error FROM order_audit_log LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(row, "Expected an audit log row")
            self.assertEqual(row[0], 'cancel')
            self.assertIn('500', row[1])
            self.assertIn('501', row[1])
        finally:
            conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_ibkr.py::CancelOrdersAuditTests -v`
Expected: FAIL

- [ ] **Step 3: Add audit logging to `cancel_orders`**

Read `cancel_orders` fully first. At function entry add timing. At the end (after the try/except block, before final return), add:

```python
    log_order_event(
        function_name='cancel_orders',
        action='cancel',
        request_data={
            'order_ids': sorted(order_ids),
            'suppress_not_found': suppress_not_found,
        },
        response_data={
            'cancelled': sorted(cancelled),
        },
        order_ids=sorted(order_ids),
        error=cancel_error if cancel_error else None,
        duration_ms=(time.monotonic() - _audit_start) * 1000,
    )
```

You'll need to capture errors into a `cancel_error` variable in the except blocks rather than only printing them, so the audit row records failures.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_ibkr.py::CancelOrdersAuditTests -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add fx_sr/ibkr.py tests/test_ibkr.py
git commit -m "feat: audit log cancel_orders"
```

---

### Task 6: Audit `submit_fx_market_order`

**Files:**
- Modify: `fx_sr/ibkr.py:1692-1733` (`submit_fx_market_order`)
- Test: `tests/test_ibkr.py`

- [ ] **Step 1: Write the failing test**

```python
class SubmitMarketOrderAuditTests(unittest.TestCase):
    def setUp(self):
        self._db_ctx = temporary_test_database()
        self.db_path = self._db_ctx.__enter__()
        db_module.init_db(self.db_path)

    def tearDown(self):
        if self._db_ctx is not None:
            self._db_ctx.__exit__(None, None, None)

    @patch('fx_sr.ibkr.get_db_path')
    @patch('fx_sr.ibkr._get_connection')
    def test_submit_market_order_logs_audit(self, mock_conn, mock_db_path):
        mock_db_path.return_value = self.db_path

        ib = MagicMock()
        ib.client.getReqId.return_value = 300
        mock_conn.return_value = (ib, True)

        trade = MagicMock()
        trade.order.orderId = 300
        trade.orderStatus.status = 'Filled'
        trade.orderStatus.avgFillPrice = 1.145
        ib.placeOrder.return_value = trade

        from fx_sr.ibkr import submit_fx_market_order
        result = submit_fx_market_order(
            pair='EURUSD', direction='LONG', quantity=10000,
        )

        conn = db_module._connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT action, pair, direction, error FROM order_audit_log LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(row, "Expected an audit log row")
            self.assertEqual(row[0], 'submit')
            self.assertEqual(row[1], 'EURUSD')
            self.assertEqual(row[2], 'LONG')
        finally:
            conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_ibkr.py::SubmitMarketOrderAuditTests -v`
Expected: FAIL

- [ ] **Step 3: Add audit logging to `submit_fx_market_order`**

Same pattern: `_audit_start` at entry, `log_order_event()` at success return and exception handler.

Success path:
```python
            log_order_event(
                function_name='submit_fx_market_order',
                pair=pair,
                direction=direction,
                action='submit',
                request_data={
                    'quantity': quantity,
                    'order_ref': order_ref,
                },
                response_data={
                    'order_id': getattr(live_order, 'orderId', None),
                    'status': getattr(status, 'status', None),
                    'avg_fill_price': getattr(status, 'avgFillPrice', None),
                },
                order_ids=[getattr(live_order, 'orderId', None)],
                duration_ms=(time.monotonic() - _audit_start) * 1000,
            )
```

Exception path:
```python
            log_order_event(
                function_name='submit_fx_market_order',
                pair=pair,
                direction=direction,
                action='submit',
                request_data={'quantity': quantity, 'order_ref': order_ref},
                error=str(e),
                duration_ms=(time.monotonic() - _audit_start) * 1000,
            )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_ibkr.py::SubmitMarketOrderAuditTests -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add fx_sr/ibkr.py tests/test_ibkr.py
git commit -m "feat: audit log submit_fx_market_order"
```

---

### Task 7: Audit the orphaned-order sweep in `positions.py`

**Files:**
- Modify: `fx_sr/positions.py:963-973` (orphaned-order sweep inside `sync_positions`)
- Test: `tests/test_live_web.py` or `tests/test_ibkr.py`

- [ ] **Step 1: Write the failing test**

```python
class OrphanSweepAuditTests(unittest.TestCase):
    def setUp(self):
        self._db_ctx = temporary_test_database()
        self.db_path = self._db_ctx.__enter__()
        db_module.init_db(self.db_path)

    def tearDown(self):
        if self._db_ctx is not None:
            self._db_ctx.__exit__(None, None, None)

    @patch('fx_sr.positions.get_db_path')
    @patch('fx_sr.positions.ibkr')
    @patch('fx_sr.positions._load_trades', return_value={})
    @patch('fx_sr.positions.reconcile_detected_signal_orders')
    def test_orphan_sweep_logs_audit(self, _recon, _load, mock_ibkr, mock_db_path):
        mock_db_path.return_value = self.db_path

        # No positions, but orders exist on CHFJPY
        mock_ibkr.fetch_positions.return_value = []
        mock_ibkr.fetch_open_order_pairs.return_value = {'CHFJPY'}

        from fx_sr.positions import sync_positions
        sync_positions()

        conn = db_module._connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT action, request_json FROM order_audit_log "
                "WHERE action = 'sweep' LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(row, "Expected a sweep audit log row")
            self.assertIn('CHFJPY', row[1])
        finally:
            conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_ibkr.py::OrphanSweepAuditTests -v` (or wherever this test class is placed)
Expected: FAIL

- [ ] **Step 3: Add audit logging to the orphan sweep**

In `fx_sr/positions.py`, add import at the top:

```python
from .ibkr import log_order_event
```

Then modify the sweep block (around line 969):

```python
    if orphaned_order_pairs:
        print(f"    Cancelling orphaned orders on pairs with no position: "
              f"{', '.join(sorted(orphaned_order_pairs))}")
        _cancel_orders_for_pairs(orphaned_order_pairs)
        log_order_event(
            function_name='sync_positions',
            action='sweep',
            request_data={
                'orphaned_pairs': sorted(orphaned_order_pairs),
                'live_pairs': sorted(live_pairs),
                'open_order_pairs': sorted(open_order_pairs),
            },
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_ibkr.py::OrphanSweepAuditTests -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add fx_sr/positions.py tests/test_ibkr.py
git commit -m "feat: audit log orphaned-order sweep in sync_positions"
```

---

### Task 8: Run full test suite and verify no regressions

- [ ] **Step 1: Run full test suite**

Run: `python -m pytest tests/ -v --tb=short`
Expected: All existing tests pass. No regressions.

- [ ] **Step 2: Verify table creation on live database**

```bash
python -c "
from fx_sr.db import init_db, _connect, get_db_path, _table_columns
init_db()
conn = _connect(get_db_path())
cols = _table_columns(conn, 'order_audit_log')
conn.close()
print('order_audit_log columns:', sorted(cols))
"
```

Expected: All 11 columns listed.

- [ ] **Step 3: Commit any final fixes**

```bash
git add -A
git commit -m "chore: final cleanup for order audit log"
```
