# Margin Slot Budget Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Divide account margin into 5 equal slots so each position can only consume 1/5th of usable margin, preventing IBKR forced liquidation.

**Architecture:** Add `margin_slots` field to `StrategyParams`, compute `per_slot_margin = (balance * (1 - cushion/100)) / margin_slots` in both sizing locations in `live.py`, and pass it as `available_margin` with `margin_cushion_pct=0` (cushion already applied).

**Tech Stack:** Python, existing `fx_sr.strategy`, `fx_sr.live`, `fx_sr.sizing`, `fx_sr.margin` modules.

---

### Task 1: Add `margin_slots` to StrategyParams

**Files:**
- Modify: `fx_sr/strategy.py:160-163` (add field after `margin_cushion_pct`)
- Modify: `fx_sr/strategy.py:224-226` (add to `params_from_profile`)
- Test: `tests/test_margin.py`

- [ ] **Step 1: Write the failing test**

In `tests/test_margin.py`, add at the end of the file:

```python
class TestMarginSlots:
    def test_strategy_params_has_margin_slots_default(self):
        from fx_sr.strategy import StrategyParams
        params = StrategyParams()
        assert params.margin_slots == 5

    def test_params_from_profile_reads_margin_slots(self):
        from fx_sr.strategy import params_from_profile
        profile = {'margin_slots': 3}
        params = params_from_profile(profile)
        assert params.margin_slots == 3

    def test_params_from_profile_defaults_margin_slots(self):
        from fx_sr.strategy import params_from_profile
        params = params_from_profile({})
        assert params.margin_slots == 5
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_margin.py::TestMarginSlots -v`
Expected: FAIL — `StrategyParams` has no attribute `margin_slots`

- [ ] **Step 3: Add the field and profile mapping**

In `fx_sr/strategy.py`, add after `margin_cushion_pct: float = 10.0` (line 162):

```python
    margin_slots: int = 5                 # divide margin into N equal slots for concurrent positions
```

In `fx_sr/strategy.py`, in `params_from_profile`, add after the `margin_cushion_pct=merged.get(...)` line (line 225):

```python
        margin_slots=merged.get('margin_slots', 5),
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_margin.py::TestMarginSlots -v`
Expected: PASS (all 3)

- [ ] **Step 5: Commit**

```bash
git add fx_sr/strategy.py tests/test_margin.py
git commit -m "feat: add margin_slots field to StrategyParams (default 5)"
```

---

### Task 2: Apply per-slot margin budget in `execute_signal_plans`

**Files:**
- Modify: `fx_sr/live.py:1412-1415` (exec margin computation in `execute_signal_plans`)
- Modify: `fx_sr/live.py:1538-1544` (pass margin_cushion_pct=0 to `_prepare_execution_plan`)
- Modify: `fx_sr/live.py:1295-1337` (`_prepare_execution_plan` needs to accept and forward `margin_cushion_pct`)
- Test: `tests/test_live_execution.py`

- [ ] **Step 1: Write the failing test**

In `tests/test_live_execution.py`, add a test that verifies oversized positions get clamped by the slot budget. First, read the existing test patterns in the file to understand imports and helpers.

Add at the end of the `LiveExecutionTests` class:

```python
    @patch('fx_sr.live.ibkr.submit_fx_market_bracket_order')
    @patch('fx_sr.live.ibkr.fetch_execution_quote')
    @patch('fx_sr.live._account_cache')
    def test_execute_signal_plans_clamps_to_margin_slot(self, mock_cache, mock_quote, mock_submit):
        """With margin_slots=5 and balance=5000, each slot gets ~850 margin.
        A signal that would need more than that should have its units clamped."""
        from fx_sr.live import execute_signal_plans, Signal
        from fx_sr.strategy import StrategyParams

        mock_cache.get_excess_liquidity.return_value = None  # force balance fallback
        mock_quote.return_value = _quote('EURUSD', mid=1.1000)
        mock_submit.return_value = {
            'pair': 'EURUSD', 'direction': 'LONG', 'quantity': 1000,
            'order_id': 1, 'status': 'Filled', 'broker_status': 'Filled',
            'avg_fill_price': 1.1000, 'filled_units': 1000.0,
            'remaining_units': 0.0, 'total_units': 1000.0,
            'take_profit_order_id': 2, 'stop_loss_order_id': 3,
            'take_profit_price': 1.1100, 'stop_loss_price': 1.0900,
        }

        params = StrategyParams(
            enforce_margin=True,
            margin_slots=5,
            margin_cushion_pct=15.0,
        )
        signal = Signal(
            pair='EURUSD', direction='LONG',
            entry_price=1.1000, sl_price=1.0900, tp_price=1.1100,
            zone_upper=1.1010, zone_lower=1.0990, zone_strength='strong',
            quality_score=0.8,
            time=__import__('pandas').Timestamp('2026-01-01 10:00'),
        )

        plans = [_plan(signal, balance=5000.0, risk_pct=0.05)]

        results = execute_signal_plans(
            signals=[signal],
            size_plans=plans,
            params=params,
            balance=5000.0,
            risk_pct=0.05,
            account_currency='GBP',
        )

        # The key check: if submit was called, the units should be within
        # the slot budget. Per-slot margin = 5000 * 0.85 / 5 = 850.
        # EURUSD margin rate = 5% so max notional = 850/0.05 = 17000 units.
        if mock_submit.called:
            submitted_qty = mock_submit.call_args[1].get('quantity') or mock_submit.call_args[0][2]
            # With 5% risk on 5000 balance = 250 risk, and 100 pip SL,
            # risk-based units would be ~25000 but margin slot caps at ~17000.
            self.assertLessEqual(submitted_qty, 17500)  # slot-capped
```

NOTE: The exact test structure depends on what helpers (`_quote`, `_plan`, `_signal`) exist in the test file. Read the file to adapt. The important assertion is that submitted quantity is capped by the margin slot, not just the risk amount.

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_live_execution.py::LiveExecutionTests::test_execute_signal_plans_clamps_to_margin_slot -v`
Expected: FAIL — units not clamped because slot budget isn't implemented yet.

- [ ] **Step 3: Implement the per-slot margin budget**

**In `fx_sr/live.py`, modify `_prepare_execution_plan` signature** (around line 1295) to accept `margin_cushion_pct`:

Change:
```python
def _prepare_execution_plan(
    signal: Signal,
    size_plan: PositionSizePlan,
    params: StrategyParams,
    price_lookup: Callable[[str], Optional[float]],
    available_margin: Optional[float] = None,
) -> tuple[Optional[PreparedExecutionPlan], str]:
```

To:
```python
def _prepare_execution_plan(
    signal: Signal,
    size_plan: PositionSizePlan,
    params: StrategyParams,
    price_lookup: Callable[[str], Optional[float]],
    available_margin: Optional[float] = None,
    margin_cushion_pct: Optional[float] = None,
) -> tuple[Optional[PreparedExecutionPlan], str]:
```

And inside `_prepare_execution_plan`, change line 1334:
```python
        margin_cushion_pct=params.margin_cushion_pct,
```
To:
```python
        margin_cushion_pct=margin_cushion_pct if margin_cushion_pct is not None else params.margin_cushion_pct,
```

**In `fx_sr/live.py`, modify `execute_signal_plans`** (around lines 1412-1415):

Replace:
```python
    # Fetch available margin for repricing margin checks
    exec_available_margin: Optional[float] = None
    if params.enforce_margin:
        excess_liq = _account_cache.get_excess_liquidity()
        exec_available_margin = excess_liq if excess_liq is not None else balance
```

With:
```python
    # Compute per-slot margin budget: divide usable margin equally across slots.
    # Cushion is applied here so downstream clamp_units_to_margin gets cushion=0.
    exec_available_margin: Optional[float] = None
    exec_margin_cushion_pct: float = params.margin_cushion_pct
    if params.enforce_margin and balance is not None:
        usable = float(balance) * (1.0 - params.margin_cushion_pct / 100.0)
        per_slot_margin = usable / max(params.margin_slots, 1)
        exec_available_margin = per_slot_margin
        exec_margin_cushion_pct = 0.0  # cushion already baked into slot budget
```

**Update the call to `_prepare_execution_plan`** (around line 1538-1544):

Change:
```python
        prepared_plan, skip_note = _prepare_execution_plan(
            signal,
            plan,
            params,
            price_lookup,
            available_margin=exec_available_margin,
        )
```

To:
```python
        prepared_plan, skip_note = _prepare_execution_plan(
            signal,
            plan,
            params,
            price_lookup,
            available_margin=exec_available_margin,
            margin_cushion_pct=exec_margin_cushion_pct,
        )
```

**Remove the margin decrement** (around line 1594-1595):

Delete or comment out:
```python
        if exec_available_margin is not None and prepared_plan.size_plan.margin_required is not None:
            exec_available_margin = max(0.0, exec_available_margin - prepared_plan.size_plan.margin_required)
```

Since each signal now gets its own fixed slot budget, we no longer decrement. Every signal sees the same `per_slot_margin`.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_live_execution.py::LiveExecutionTests::test_execute_signal_plans_clamps_to_margin_slot -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add fx_sr/live.py tests/test_live_execution.py
git commit -m "feat: apply per-slot margin budget in execute_signal_plans"
```

---

### Task 3: Apply per-slot margin budget in `_build_signal_size_plans`

**Files:**
- Modify: `fx_sr/live.py:1153-1186` (display-time sizing in `_build_signal_size_plans`)
- Test: Run existing tests to verify no regressions

This is the display-time sizing that shows suggested sizes in the scanner output. It should use the same slot budget so the displayed size matches what would actually execute.

- [ ] **Step 1: Modify `_build_signal_size_plans`**

Replace the margin block (around lines 1153-1157):

```python
    # Use cached excess liquidity (refreshed every 5 min, not per cycle)
    available_margin: Optional[float] = None
    if params.enforce_margin:
        excess_liq = _account_cache.get_excess_liquidity()
        available_margin = excess_liq if excess_liq is not None else balance
```

With:
```python
    # Compute per-slot margin budget: same logic as execute_signal_plans.
    available_margin: Optional[float] = None
    display_margin_cushion_pct: float = params.margin_cushion_pct
    if params.enforce_margin and balance is not None:
        usable = float(balance) * (1.0 - params.margin_cushion_pct / 100.0)
        per_slot_margin = usable / max(params.margin_slots, 1)
        available_margin = per_slot_margin
        display_margin_cushion_pct = 0.0  # cushion already baked into slot budget
```

And update the `build_position_size_plan` call (around line 1181):

Change:
```python
            margin_cushion_pct=params.margin_cushion_pct,
```

To:
```python
            margin_cushion_pct=display_margin_cushion_pct,
```

Also remove the batch margin tracking since each signal now gets its own fixed slot:

Remove lines like:
```python
    batch_margin_used = 0.0
```
and:
```python
        current_margin = (
            max(0.0, available_margin - batch_margin_used)
            if available_margin is not None
            else None
        )
```

Replace with simply passing `available_margin` directly:
```python
            available_margin=available_margin,
```

And remove:
```python
        if plan is not None and plan.margin_required is not None:
            batch_margin_used += plan.margin_required
```

- [ ] **Step 2: Run full test suite**

Run: `python -m pytest tests/test_margin.py tests/test_live_execution.py -v`
Expected: All tests pass, including the new ones from Tasks 1 and 2.

- [ ] **Step 3: Commit**

```bash
git add fx_sr/live.py
git commit -m "feat: apply per-slot margin budget in display-time sizing"
```
