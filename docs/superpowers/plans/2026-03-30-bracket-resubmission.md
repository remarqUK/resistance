# Bracket Resubmission on Gateway Restart — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When IB Gateway restarts and wipes working bracket orders (TP/SL), detect orphaned positions and resubmit their protective brackets so trades aren't left exposed.

**Architecture:** Add a new function `resubmit_missing_brackets()` in `fx_sr/ibkr.py` that submits standalone TP limit + SL stop orders for a given position. Call it from `sync_positions()` in `fx_sr/positions.py` during the "positions still at IBKR" loop — if a tracked position has a signal with bracket order IDs but those orders are no longer working at IBKR, resubmit them and update the signal record.

**Tech Stack:** Python, ib_async, psycopg (PostgreSQL), unittest with mocks

---

### Task 1: Add `submit_bracket_for_existing_position()` to `fx_sr/ibkr.py`

**Files:**
- Modify: `fx_sr/ibkr.py` (after `submit_fx_market_bracket_order`, ~line 1797)
- Test: `tests/test_ibkr.py`

- [ ] **Step 1: Write the failing test**

In `tests/test_ibkr.py`, add:

```python
class SubmitBracketForExistingPositionTests(unittest.TestCase):
    def test_submits_limit_and_stop_orders(self):
        fake_ib_async = types.ModuleType('ib_async')
        fake_ib_async.LimitOrder = MagicMock()
        fake_ib_async.StopOrder = MagicMock()

        ib = MagicMock()
        ib.client.getReqId.side_effect = [200, 201]
        ib.qualifyContracts = MagicMock()
        contract = MagicMock()

        tp_trade = MagicMock()
        sl_trade = MagicMock()
        tp_trade.order = MagicMock(orderId=200)
        sl_trade.order = MagicMock(orderId=201)
        ib.placeOrder.side_effect = [tp_trade, sl_trade]

        with patch.dict(sys.modules, {'ib_async': fake_ib_async}), \
                patch('fx_sr.ibkr._get_connection', return_value=(ib, True)), \
                patch('fx_sr.ibkr._make_contract', return_value=contract), \
                patch('fx_sr.ibkr._round_bracket_exit_prices', return_value=(1.1050, 1.0950)):
            result = ibkr.submit_bracket_for_existing_position(
                pair='EURUSD',
                direction='LONG',
                quantity=10000,
                take_profit_price=1.1050,
                stop_loss_price=1.0950,
            )

        self.assertIsNotNone(result)
        self.assertEqual(result['take_profit_order_id'], 200)
        self.assertEqual(result['stop_loss_order_id'], 201)
        self.assertEqual(ib.placeOrder.call_count, 2)

    def test_returns_none_when_not_connected(self):
        with patch('fx_sr.ibkr._get_connection', return_value=(None, False)):
            result = ibkr.submit_bracket_for_existing_position(
                pair='EURUSD',
                direction='LONG',
                quantity=10000,
                take_profit_price=1.1050,
                stop_loss_price=1.0950,
            )
        self.assertIsNone(result)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_ibkr.py::SubmitBracketForExistingPositionTests -v`
Expected: FAIL — `AttributeError: module 'fx_sr.ibkr' has no attribute 'submit_bracket_for_existing_position'`

- [ ] **Step 3: Write the implementation**

Add to `fx_sr/ibkr.py` after `submit_fx_market_bracket_order` (after line 1797):

```python
def submit_bracket_for_existing_position(
    pair: str,
    direction: str,
    quantity: int,
    take_profit_price: float,
    stop_loss_price: float,
    order_ref: str = '',
) -> Optional[dict]:
    """Submit standalone TP limit + SL stop orders for an existing position.

    Used to restore bracket protection after a gateway restart wipes
    working orders but preserves positions.  Unlike
    submit_fx_market_bracket_order this does NOT place a parent market
    order — the position already exists.
    """
    if quantity <= 0:
        return None

    with _IBKR_LOCK:
        ib, connected = _get_connection()
        if not connected:
            return None

        try:
            from ib_async import LimitOrder, StopOrder

            contract = _make_contract(pair)
            ib.qualifyContracts(contract)
            rounded_tp, rounded_sl = _round_bracket_exit_prices(
                pair, direction, take_profit_price, stop_loss_price,
                ib=ib, contract=contract,
            )

            close_action = 'SELL' if direction == 'LONG' else 'BUY'
            ref_prefix = f'{order_ref}:rebracket' if order_ref else 'rebracket'

            tp_order = LimitOrder(
                close_action,
                int(quantity),
                float(rounded_tp),
                orderId=ib.client.getReqId(),
                orderRef=f'{ref_prefix}:tp',
                tif='GTC',
                transmit=False,
            )
            sl_order = StopOrder(
                close_action,
                int(quantity),
                float(rounded_sl),
                orderId=ib.client.getReqId(),
                orderRef=f'{ref_prefix}:sl',
                tif='GTC',
                transmit=True,
            )

            tp_trade = ib.placeOrder(contract, tp_order)
            sl_trade = ib.placeOrder(contract, sl_order)

            tp_live = getattr(tp_trade, 'order', None)
            sl_live = getattr(sl_trade, 'order', None)

            return {
                'pair': pair,
                'direction': direction,
                'take_profit_order_id': getattr(tp_live, 'orderId', None),
                'stop_loss_order_id': getattr(sl_live, 'orderId', None),
                'take_profit_price': float(rounded_tp),
                'stop_loss_price': float(rounded_sl),
            }
        except Exception as e:
            print(f"    Warning: failed to resubmit brackets for {pair}: {e}")
            return None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_ibkr.py::SubmitBracketForExistingPositionTests -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add fx_sr/ibkr.py tests/test_ibkr.py
git commit -m "feat: add submit_bracket_for_existing_position to ibkr module"
```

---

### Task 2: Add `_resubmit_missing_brackets()` to `fx_sr/positions.py`

**Files:**
- Modify: `fx_sr/positions.py` (new helper, before `sync_positions`)
- Test: `tests/test_live_execution.py`

- [ ] **Step 1: Write the failing test**

In `tests/test_live_execution.py`, add a new test class:

```python
class ResubmitMissingBracketsTests(unittest.TestCase):
    """Test that orphaned positions get their brackets restored."""

    def test_resubmits_when_bracket_orders_missing(self):
        """Position exists at IBKR, signal has bracket IDs, but orders are gone."""
        from fx_sr.positions import _resubmit_missing_brackets

        signal_row = {
            'signal_id': 'EURUSD:LONG:abc123',
            'pair': 'EURUSD',
            'direction': 'LONG',
            'take_profit_order_id': 100,
            'stop_loss_order_id': 101,
            'submitted_tp_price': 1.1050,
            'submitted_sl_price': 1.0950,
            'tp_price': 1.1050,
            'sl_price': 1.0950,
        }

        with patch(
            'fx_sr.positions.ibkr.fetch_open_order_pairs',
            return_value=set(),
        ), patch(
            'fx_sr.positions.ibkr.submit_bracket_for_existing_position',
            return_value={
                'take_profit_order_id': 200,
                'stop_loss_order_id': 201,
                'take_profit_price': 1.1050,
                'stop_loss_price': 1.0950,
            },
        ) as submit_mock, patch(
            'fx_sr.positions._update_signal_bracket_ids',
        ) as update_mock:
            _resubmit_missing_brackets(signal_row, ibkr_size=10000)

        submit_mock.assert_called_once_with(
            pair='EURUSD',
            direction='LONG',
            quantity=10000,
            take_profit_price=1.1050,
            stop_loss_price=1.0950,
        )
        update_mock.assert_called_once_with(
            'EURUSD:LONG:abc123', 200, 201,
        )

    def test_skips_when_orders_still_working(self):
        """Position has bracket orders that are still live at IBKR — no resubmission."""
        from fx_sr.positions import _resubmit_missing_brackets

        signal_row = {
            'signal_id': 'EURUSD:LONG:abc123',
            'pair': 'EURUSD',
            'direction': 'LONG',
            'take_profit_order_id': 100,
            'stop_loss_order_id': 101,
            'submitted_tp_price': 1.1050,
            'submitted_sl_price': 1.0950,
            'tp_price': 1.1050,
            'sl_price': 1.0950,
        }

        with patch(
            'fx_sr.positions.ibkr.fetch_open_order_pairs',
            return_value={'EURUSD'},
        ), patch(
            'fx_sr.positions.ibkr.submit_bracket_for_existing_position',
        ) as submit_mock, patch(
            'fx_sr.positions._update_signal_bracket_ids',
        ):
            _resubmit_missing_brackets(signal_row, ibkr_size=10000)

        submit_mock.assert_not_called()

    def test_skips_when_no_bracket_ids_on_signal(self):
        """Signal was never bracketed (e.g. external position) — skip."""
        from fx_sr.positions import _resubmit_missing_brackets

        signal_row = {
            'signal_id': 'EURUSD:LONG:abc123',
            'pair': 'EURUSD',
            'direction': 'LONG',
            'take_profit_order_id': None,
            'stop_loss_order_id': None,
            'submitted_tp_price': None,
            'submitted_sl_price': None,
            'tp_price': 1.1050,
            'sl_price': 1.0950,
        }

        with patch(
            'fx_sr.positions.ibkr.fetch_open_order_pairs',
            return_value=set(),
        ), patch(
            'fx_sr.positions.ibkr.submit_bracket_for_existing_position',
        ) as submit_mock:
            _resubmit_missing_brackets(signal_row, ibkr_size=10000)

        submit_mock.assert_not_called()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_live_execution.py::ResubmitMissingBracketsTests -v`
Expected: FAIL — `ImportError: cannot import name '_resubmit_missing_brackets'`

- [ ] **Step 3: Write the implementation**

Add to `fx_sr/positions.py`, before `sync_positions()` (around line 635):

```python
def _update_signal_bracket_ids(
    signal_id: str,
    tp_order_id: int | None,
    sl_order_id: int | None,
) -> None:
    """Overwrite the TP/SL order IDs on a detected_signal row."""
    from .live_history import load_detected_signal_conn, _replace_row_conn
    with _tracking_db_transaction() as conn:
        row = load_detected_signal_conn(conn, signal_id)
        if row is None:
            return
        row['take_profit_order_id'] = tp_order_id
        row['stop_loss_order_id'] = sl_order_id
        _replace_row_conn(conn, row)


def _resubmit_missing_brackets(
    signal_row: dict,
    ibkr_size: float,
) -> None:
    """Resubmit TP/SL bracket orders if the originals were lost (e.g. gateway restart).

    Checks whether the pair still has working orders at IBKR.  If not,
    and the signal has bracket prices, submits new TP limit + SL stop
    and updates the signal record with the new order IDs.
    """
    tp_oid = signal_row.get('take_profit_order_id')
    sl_oid = signal_row.get('stop_loss_order_id')

    # Only attempt if the signal originally had bracket orders
    if tp_oid is None and sl_oid is None:
        return

    pair = signal_row['pair']
    direction = signal_row['direction']

    # Check if the pair still has working orders at IBKR
    open_pairs = ibkr.fetch_open_order_pairs()
    if pair in open_pairs:
        return  # Brackets (or other orders) still live — nothing to do

    tp_price = (
        float(signal_row['submitted_tp_price'])
        if signal_row.get('submitted_tp_price') is not None
        else float(signal_row['tp_price'])
        if signal_row.get('tp_price') is not None
        else None
    )
    sl_price = (
        float(signal_row['submitted_sl_price'])
        if signal_row.get('submitted_sl_price') is not None
        else float(signal_row['sl_price'])
        if signal_row.get('sl_price') is not None
        else None
    )
    if tp_price is None or sl_price is None:
        return

    quantity = int(abs(ibkr_size))
    print(f"    Resubmitting brackets for {pair} {direction} "
          f"(TP={tp_price}, SL={sl_price}, qty={quantity})")

    result = ibkr.submit_bracket_for_existing_position(
        pair=pair,
        direction=direction,
        quantity=quantity,
        take_profit_price=tp_price,
        stop_loss_price=sl_price,
    )
    if result is None:
        print(f"    Warning: bracket resubmission failed for {pair}")
        return

    _update_signal_bracket_ids(
        signal_row['signal_id'],
        result['take_profit_order_id'],
        result['stop_loss_order_id'],
    )
    print(f"    Brackets restored for {pair}: "
          f"TP order={result['take_profit_order_id']}, "
          f"SL order={result['stop_loss_order_id']}")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_live_execution.py::ResubmitMissingBracketsTests -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add fx_sr/positions.py tests/test_live_execution.py
git commit -m "feat: add _resubmit_missing_brackets helper for orphaned positions"
```

---

### Task 3: Wire bracket resubmission into `sync_positions()`

**Files:**
- Modify: `fx_sr/positions.py` — the existing `sync_positions()` function, in the "positions still at IBKR" loop (~line 691-782)
- Test: `tests/test_live_execution.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_live_execution.py`:

```python
class SyncPositionsBracketResubmissionTests(unittest.TestCase):
    """sync_positions should resubmit brackets for tracked positions missing orders."""

    @patch('fx_sr.positions._resubmit_missing_brackets')
    @patch('fx_sr.positions._load_trades')
    @patch('fx_sr.positions.ibkr.fetch_positions')
    @patch('fx_sr.positions.reconcile_detected_signal_orders')
    @patch('fx_sr.positions.load_detected_signal')
    def test_calls_resubmit_for_existing_position_with_signal(
        self, mock_load_signal, mock_reconcile, mock_fetch_pos,
        mock_load_trades, mock_resubmit,
    ):
        from fx_sr.positions import sync_positions
        from fx_sr.strategy import StrategyParams, Trade

        trade = Trade(
            entry_time=pd.Timestamp('2026-03-30 08:00', tz='UTC'),
            entry_price=1.1000,
            direction='LONG',
            sl_price=1.0950,
            tp_price=1.1050,
        )
        signal_row = {
            'signal_id': 'EURUSD:LONG:abc123',
            'pair': 'EURUSD',
            'direction': 'LONG',
            'status': 'OPEN',
            'take_profit_order_id': 100,
            'stop_loss_order_id': 101,
            'submitted_tp_price': 1.1050,
            'submitted_sl_price': 1.0950,
            'tp_price': 1.1050,
            'sl_price': 1.0950,
            'exit_signal_reason': None,
            'exit_signal_price': None,
            'exit_signal_at': None,
        }
        mock_load_signal.return_value = signal_row

        mock_fetch_pos.return_value = [
            {'pair': 'EURUSD', 'size': 10000.0, 'avg_cost': 1.1000},
        ]
        mock_load_trades.return_value = {
            'EURUSD:LONG': {
                'pair': 'EURUSD',
                'trade': trade,
                'bars_monitored': 0,
                'ibkr_avg_cost': 1.1000,
                'ibkr_size': 10000.0,
                'signal_id': 'EURUSD:LONG:abc123',
                'signal_status': 'OPEN',
                'pending_exit_reason': None,
                'pending_exit_price': None,
                'pending_exit_detected_at': None,
                'last_processed_bar_time': None,
            },
        }

        with patch('fx_sr.positions.claim_signal_for_position_conn'), \
                patch('fx_sr.positions._save_trade_conn'):
            sync_positions(StrategyParams())

        mock_resubmit.assert_called_once_with(signal_row, ibkr_size=10000.0)

    @patch('fx_sr.positions._resubmit_missing_brackets')
    @patch('fx_sr.positions._load_trades')
    @patch('fx_sr.positions.ibkr.fetch_positions')
    @patch('fx_sr.positions.reconcile_detected_signal_orders')
    def test_skips_resubmit_when_no_signal(
        self, mock_reconcile, mock_fetch_pos,
        mock_load_trades, mock_resubmit,
    ):
        from fx_sr.positions import sync_positions
        from fx_sr.strategy import StrategyParams, Trade

        trade = Trade(
            entry_time=pd.Timestamp('2026-03-30 08:00', tz='UTC'),
            entry_price=1.1000,
            direction='LONG',
            sl_price=1.0950,
            tp_price=1.1050,
        )
        mock_fetch_pos.return_value = [
            {'pair': 'EURUSD', 'size': 10000.0, 'avg_cost': 1.1000},
        ]
        mock_load_trades.return_value = {
            'EURUSD:LONG': {
                'pair': 'EURUSD',
                'trade': trade,
                'bars_monitored': 0,
                'ibkr_avg_cost': 1.1000,
                'ibkr_size': 10000.0,
                'signal_id': None,
                'signal_status': None,
                'pending_exit_reason': None,
                'pending_exit_price': None,
                'pending_exit_detected_at': None,
                'last_processed_bar_time': None,
            },
        }

        with patch('fx_sr.positions.claim_signal_for_position_conn'), \
                patch('fx_sr.positions._save_trade_conn'):
            sync_positions(StrategyParams())

        mock_resubmit.assert_not_called()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_live_execution.py::SyncPositionsBracketResubmissionTests -v`
Expected: FAIL — `_resubmit_missing_brackets` never called (not yet wired in)

- [ ] **Step 3: Wire into sync_positions**

In `fx_sr/positions.py`, inside the `for key, pos in ibkr_by_key.items():` loop in `sync_positions()`, add the resubmission call after the signal row is resolved (after the `elif signal_id:` block that loads the signal, around line 742). Insert before the `signal_status = ...` line:

```python
        # --- Bracket resubmission for orphaned positions ---
        if signal_row is not None and not is_new_position and not size_changed:
            _resubmit_missing_brackets(signal_row, ibkr_size=pos['size'])
```

This goes right after line 742 (`signal_row = load_detected_signal(signal_id)`), before line 744 (`signal_status = ...`).

The conditions ensure we only resubmit for positions that:
1. Have an associated signal (so we know the TP/SL prices)
2. Are not newly discovered (those get fresh brackets from the normal execution flow)
3. Haven't changed size (size change triggers re-claim which handles its own brackets)

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_live_execution.py::SyncPositionsBracketResubmissionTests -v`
Expected: PASS

- [ ] **Step 5: Run the full test suite**

Run: `python -m pytest tests/ -v`
Expected: All existing tests still pass

- [ ] **Step 6: Commit**

```bash
git add fx_sr/positions.py tests/test_live_execution.py
git commit -m "feat: wire bracket resubmission into sync_positions for gateway restart resilience"
```
