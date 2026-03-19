"""Shared sizing helpers for backtests and live signal planning."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Callable, Optional

from .config import PAIRS


PriceLookup = Callable[[str], Optional[float]]


@dataclass(frozen=True)
class PositionSizePlan:
    """Concrete live sizing plan for a signal."""
    pair: str
    direction: str
    units: int
    risk_amount: float
    risk_pct: float
    balance: float
    account_currency: str
    risk_per_unit_account: float
    notional_account: float
    margin_required: Optional[float] = None
    margin_rate: Optional[float] = None
    is_odd_lot: Optional[bool] = None
    estimated_commission: Optional[float] = None


def calculate_risk_amount(balance: float, risk_pct: float) -> float:
    """Return full-trade risk amount using the same compounding rule everywhere."""
    return max(float(balance), 0.0) * max(float(risk_pct), 0.0)


def split_pair(pair: str) -> tuple[str, str]:
    """Split a six-letter FX pair into base and quote currencies."""
    pair = pair.upper()
    if len(pair) != 6:
        raise ValueError(f"Unsupported FX pair format: {pair}")
    return pair[:3], pair[3:]


def convert_currency(
    amount: float,
    from_currency: str,
    to_currency: str,
    price_lookup: PriceLookup,
) -> Optional[float]:
    """Convert an amount between currencies using available FX pairs.

    The graph is built lazily from configured pairs. Each edge uses either the
    direct pair price or its inverse.
    """
    from_currency = from_currency.upper()
    to_currency = to_currency.upper()

    if amount == 0:
        return 0.0
    if from_currency == to_currency:
        return float(amount)

    queue = deque([(from_currency, 1.0)])
    visited = {from_currency}

    while queue:
        currency, rate = queue.popleft()

        for pair_id in PAIRS:
            base, quote = split_pair(pair_id)
            price = price_lookup(pair_id)
            if price is None or price <= 0:
                continue

            next_currency = None
            next_rate = None

            if base == currency and quote not in visited:
                next_currency = quote
                next_rate = rate * price
            elif quote == currency and base not in visited:
                next_currency = base
                next_rate = rate / price

            if next_currency is None or next_rate is None:
                continue

            if next_currency == to_currency:
                return float(amount) * next_rate

            visited.add(next_currency)
            queue.append((next_currency, next_rate))

    return None


def build_position_size_plan(
    pair: str,
    direction: str,
    entry_price: float,
    stop_price: float,
    balance: float,
    risk_pct: float,
    account_currency: str,
    price_lookup: PriceLookup,
    *,
    available_margin: Optional[float] = None,
    margin_cushion_pct: float = 10.0,
    enforce_margin: bool = True,
    min_order_units: int = 1000,
) -> Optional[PositionSizePlan]:
    """Size a live FX trade from account risk and stop distance.

    IBKR spot FX positions are sized in base-currency units, so the full-risk
    amount per one unit is the stop distance in quote currency converted back
    into the account currency.
    """
    risk_amount = calculate_risk_amount(balance, risk_pct)
    return build_position_size_plan_for_risk_amount(
        pair=pair,
        direction=direction,
        entry_price=entry_price,
        stop_price=stop_price,
        balance=balance,
        risk_amount=risk_amount,
        account_currency=account_currency,
        price_lookup=price_lookup,
        available_margin=available_margin,
        margin_cushion_pct=margin_cushion_pct,
        enforce_margin=enforce_margin,
        min_order_units=min_order_units,
    )


def estimate_position_risk_amount(
    pair: str,
    entry_price: float,
    stop_price: float,
    units: int,
    account_currency: str,
    price_lookup: PriceLookup,
) -> Optional[float]:
    """Estimate full stop-loss risk for an existing FX position in account currency."""

    _, quote = split_pair(pair)
    stop_distance = abs(float(entry_price) - float(stop_price))
    if stop_distance <= 0 or abs(int(units)) <= 0:
        return 0.0

    risk_quote = stop_distance * abs(int(units))
    risk_account = convert_currency(
        risk_quote,
        from_currency=quote,
        to_currency=account_currency,
        price_lookup=price_lookup,
    )
    if risk_account is None or risk_account < 0:
        return None
    return float(risk_account)


def build_position_size_plan_for_risk_amount(
    pair: str,
    direction: str,
    entry_price: float,
    stop_price: float,
    balance: float,
    risk_amount: float,
    account_currency: str,
    price_lookup: PriceLookup,
    *,
    available_margin: Optional[float] = None,
    margin_cushion_pct: float = 10.0,
    enforce_margin: bool = True,
    min_order_units: int = 1000,
) -> Optional[PositionSizePlan]:
    """Size a live FX trade for an explicit account-currency risk amount."""

    from .margin import (
        MIN_UNITS_IDEAL_PRO,
        compute_margin_requirement,
        clamp_units_to_margin,
    )

    _, quote = split_pair(pair)
    stop_distance = abs(float(entry_price) - float(stop_price))

    if risk_amount <= 0 or stop_distance <= 0:
        return None

    risk_per_unit_account = convert_currency(
        stop_distance,
        from_currency=quote,
        to_currency=account_currency,
        price_lookup=price_lookup,
    )
    if risk_per_unit_account is None or risk_per_unit_account <= 0:
        return None

    units = int(risk_amount / risk_per_unit_account)
    if units <= 0:
        return None

    # Enforce minimum order size
    effective_min = max(int(min_order_units), 1) if enforce_margin else 1
    if units < effective_min:
        return None

    # Clamp units to available margin budget
    if enforce_margin and available_margin is not None:
        units = clamp_units_to_margin(
            pair, units, entry_price, available_margin,
            account_currency, price_lookup, margin_cushion_pct,
        )
        if units < effective_min:
            return None

    # Compute margin requirement for the (possibly clamped) units
    margin_req = compute_margin_requirement(
        pair, units, entry_price, account_currency, price_lookup,
    )
    margin_required = margin_req.margin_required if margin_req else None
    margin_rate = margin_req.margin_rate if margin_req else None
    is_odd_lot = (units < MIN_UNITS_IDEAL_PRO) if margin_req else None

    # Recompute notional with potentially-clamped units
    notional_quote = units * float(entry_price)
    notional_account = convert_currency(
        notional_quote,
        from_currency=quote,
        to_currency=account_currency,
        price_lookup=price_lookup,
    )
    if notional_account is None:
        return None

    # Estimate round-turn commission
    from .commission import compute_round_turn_commission
    estimated_commission = compute_round_turn_commission(
        units=units,
        entry_price=float(entry_price),
        pair=pair,
        account_currency=account_currency.upper(),
        price_lookup=price_lookup,
    )

    return PositionSizePlan(
        pair=pair,
        direction=direction,
        units=units,
        risk_amount=risk_amount,
        risk_pct=(float(risk_amount) / float(balance)) if float(balance) > 0 else 0.0,
        balance=float(balance),
        account_currency=account_currency.upper(),
        risk_per_unit_account=float(risk_per_unit_account),
        notional_account=float(notional_account),
        margin_required=margin_required,
        margin_rate=margin_rate,
        is_odd_lot=is_odd_lot,
        estimated_commission=estimated_commission,
    )


def format_units(units: int) -> str:
    """Format FX units compactly for operator output."""
    abs_units = abs(int(units))
    if abs_units >= 1_000_000:
        return f"{abs_units / 1_000_000:.2f}M"
    if abs_units >= 1_000:
        return f"{abs_units / 1_000:.1f}K"
    return str(abs_units)
