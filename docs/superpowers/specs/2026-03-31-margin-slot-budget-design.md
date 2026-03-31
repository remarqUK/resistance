# Margin Slot Budget Design

**Goal:** Prevent IBKR margin liquidation by dividing total account margin into equal slots, so each position can only consume its share and N concurrent positions can always coexist safely.

## Problem

The current sizing system calculates position size from risk (balance x risk% / stop distance), then clamps to available margin. But "available margin" is a snapshot of excess liquidity that doesn't account for future positions. When multiple signals fire in the same or nearby cycles, each sees roughly the same available margin and collectively overshoot the account's capacity.

Real example: 4 positions consumed £5,467 estimated margin against a £4,917 NLV account (111%), triggering IBKR forced liquidation.

## Design

### New StrategyParams Field

```python
margin_slots: int = 5  # Number of equal margin slots for concurrent positions
```

Configurable per profile, overridable via CLI. Default 5.

### Per-Slot Margin Budget

Computed in `execute_signal_plans` before sizing any signals:

```
per_slot_margin = (balance * (1 - margin_cushion_pct / 100)) / margin_slots
```

With £4,917 balance, 15% cushion, 5 slots:
- Usable margin: £4,917 * 0.85 = £4,179
- Per slot: £4,179 / 5 = **£836**
- Supports: ~£16.7K notional on majors (5% rate), ~£12.5K on crosses (6.67% rate)

### Integration Point

In `execute_signal_plans` (live.py), replace the current `exec_available_margin` calculation:

**Before:**
```python
exec_available_margin = excess_liq if excess_liq is not None else balance
```

**After:**
```python
usable = balance * (1.0 - params.margin_cushion_pct / 100.0)
per_slot_margin = usable / max(params.margin_slots, 1)
exec_available_margin = per_slot_margin
```

Each signal in the batch receives the same `per_slot_margin` as its available margin budget. This replaces the current approach of passing raw excess liquidity and decrementing it per signal.

### Cushion Double-Application Fix

`clamp_units_to_margin` in `margin.py` applies `margin_cushion_pct` internally:
```python
usable = available_margin * (1.0 - cushion_pct / 100.0)
```

Since the cushion is already baked into `per_slot_margin`, we pass `margin_cushion_pct=0` when calling the sizing functions under slot-based budgets. This is done in the `_prepare_execution_plan` call where `available_margin` and `margin_cushion_pct` are forwarded to `build_position_size_plan_for_risk_amount`.

### What Changes

| Component | Change |
|-----------|--------|
| `strategy.py` StrategyParams | Add `margin_slots: int = 5` |
| `live.py` execute_signal_plans | Compute `per_slot_margin` from balance, cushion, slots; pass as `available_margin` with `margin_cushion_pct=0` |
| Profile presets (profiles.py) | Optionally set `margin_slots` per profile (default 5 is fine for most) |

### What Doesn't Change

- **Risk budget** (`max_total_risk = slot_risk * max_correlated_trades`) — independent guard on total stop-loss exposure
- **Correlation filter** — independent guard on correlated pair count
- **`whatif_margin_check`** at submit time — final IBKR-side safety net before order placement
- **`clamp_units_to_margin`** logic — unchanged, just receives a per-slot budget instead of total excess liquidity
- **`_account_cache`** — still used for the whatif pre-flight, just not for slot sizing

### Interaction With Existing Guards

The three guards are now layered:

1. **Margin slot budget** (new) — caps each position's notional so N positions fit in the account. Prevents the root cause of the liquidation.
2. **Risk budget** (existing) — caps total stop-loss risk across all positions. Prevents excessive drawdown.
3. **whatif_margin_check** (existing) — IBKR's own pre-flight check at submit time. Final safety net.

All three are independent. A signal must pass all three to execute.

### Edge Cases

- **`margin_slots=1`**: All usable margin goes to one position. Equivalent to current behavior but with cushion applied once.
- **Balance unavailable**: Falls back to existing behavior (excess liquidity or skip).
- **Very small accounts**: Per-slot margin may be too small for even `min_order_units` (1000). The existing `clamp_units_to_margin` returns 0 in this case, and sizing returns None (signal skipped). This is correct — the account can't support that many concurrent positions.
