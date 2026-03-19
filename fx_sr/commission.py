"""IBKR FX commission model.

Computes round-turn commission for spot FX trades using IBKR's tiered
pricing: 0.20 basis points of trade value per side with a $2.00 USD
minimum per order (monthly volume <= $1B tier).
"""

from __future__ import annotations

from typing import Optional

from .sizing import PriceLookup, split_pair, convert_currency


def compute_round_turn_commission(
    units: int,
    entry_price: float,
    pair: str,
    account_currency: str,
    price_lookup: PriceLookup,
    commission_bps: float = 0.20,
    commission_min_usd: float = 2.00,
) -> Optional[float]:
    """Compute round-turn IBKR commission in the account currency.

    Returns the total (entry + exit) commission converted to
    *account_currency*, or ``None`` if the currency conversion fails.
    """
    if units <= 0 or entry_price <= 0:
        return 0.0

    base, quote = split_pair(pair)

    # Notional in quote currency, then convert to USD for the fee calc.
    notional_quote = abs(units) * entry_price
    notional_usd = convert_currency(
        notional_quote,
        from_currency=quote,
        to_currency='USD',
        price_lookup=price_lookup,
    )
    if notional_usd is None:
        return None

    # Per-side commission in USD
    per_side_usd = max(notional_usd * commission_bps / 10_000.0, commission_min_usd)
    round_turn_usd = 2.0 * per_side_usd

    # Convert to account currency
    commission_account = convert_currency(
        round_turn_usd,
        from_currency='USD',
        to_currency=account_currency,
        price_lookup=price_lookup,
    )
    return commission_account


def commission_as_pips(
    commission_account: float,
    units: int,
    pip: float,
    account_currency: str,
    quote_currency: str,
    price_lookup: PriceLookup,
) -> Optional[float]:
    """Convert an account-currency commission into pip-equivalent cost.

    This expresses the round-turn commission as a number of pips so it
    can be subtracted directly from ``pnl_pips``.
    """
    if commission_account <= 0 or units <= 0 or pip <= 0:
        return 0.0

    # Convert commission from account currency to quote currency
    commission_quote = convert_currency(
        commission_account,
        from_currency=account_currency,
        to_currency=quote_currency,
        price_lookup=price_lookup,
    )
    if commission_quote is None:
        return None

    # commission_quote / (units * pip) = number of pips the commission represents
    return commission_quote / (abs(units) * pip)
