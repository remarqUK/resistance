"""FCA UK retail margin model for IBKR FX positions.

Provides margin rate classification, requirement computation, and position
size clamping for UK retail accounts under FCA leverage caps.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .sizing import split_pair, convert_currency, PriceLookup

# FCA major currencies — any pair composed entirely of these gets 30:1
MAJOR_CURRENCIES = frozenset({'USD', 'EUR', 'JPY', 'GBP', 'CAD', 'CHF'})

MARGIN_RATE_MAJOR = 1.0 / 30.0   # ~3.33%
MARGIN_RATE_MINOR = 1.0 / 20.0   # 5.00%

# IBKR minimum order sizes (base currency units)
MIN_UNITS_ODD_LOT = 1_000
MIN_UNITS_IDEAL_PRO = 25_000

# Default safety cushion — never consume the last N% of available margin
DEFAULT_MARGIN_CUSHION_PCT = 10.0


@dataclass(frozen=True)
class MarginRequirement:
    """Computed margin requirement for one FX position."""

    pair: str
    units: int
    notional_account: float      # position value in account currency
    margin_rate: float           # 0.0333 or 0.05
    margin_required: float       # notional_account * margin_rate
    is_odd_lot: bool             # True if units < 25,000
    is_below_minimum: bool       # True if units < 1,000


def get_margin_rate(pair: str) -> float:
    """Return the FCA UK retail margin rate for a pair.

    Major pairs (both currencies in USD/EUR/JPY/GBP/CAD/CHF): 3.33% (30:1).
    All others (involving AUD, NZD, etc.): 5% (20:1).
    """
    base, quote = split_pair(pair)
    if base in MAJOR_CURRENCIES and quote in MAJOR_CURRENCIES:
        return MARGIN_RATE_MAJOR
    return MARGIN_RATE_MINOR


def is_major_pair(pair: str) -> bool:
    """True if both currencies are in the FCA major set."""
    base, quote = split_pair(pair)
    return base in MAJOR_CURRENCIES and quote in MAJOR_CURRENCIES


def compute_margin_requirement(
    pair: str,
    units: int,
    entry_price: float,
    account_currency: str,
    price_lookup: PriceLookup,
) -> Optional[MarginRequirement]:
    """Compute the margin requirement for a proposed position.

    Notional is ``units`` of base currency converted to the account currency.
    Margin required = notional × margin rate.
    """
    base, _quote = split_pair(pair)
    margin_rate = get_margin_rate(pair)
    abs_units = abs(units)

    notional_account = convert_currency(
        float(abs_units),
        from_currency=base,
        to_currency=account_currency.upper(),
        price_lookup=price_lookup,
    )
    if notional_account is None:
        return None

    return MarginRequirement(
        pair=pair,
        units=abs_units,
        notional_account=notional_account,
        margin_rate=margin_rate,
        margin_required=notional_account * margin_rate,
        is_odd_lot=abs_units < MIN_UNITS_IDEAL_PRO,
        is_below_minimum=abs_units < MIN_UNITS_ODD_LOT,
    )


def check_margin_available(
    margin_required: float,
    available_margin: float,
    cushion_pct: float = DEFAULT_MARGIN_CUSHION_PCT,
) -> tuple[bool, float]:
    """Check if enough margin is available after applying a safety cushion.

    Returns ``(allowed, margin_after)`` where *margin_after* is the remaining
    available margin if the trade is accepted.
    """
    usable = available_margin * (1.0 - cushion_pct / 100.0)
    margin_after = available_margin - margin_required
    return margin_required <= usable, margin_after


def clamp_units_to_margin(
    pair: str,
    units: int,
    entry_price: float,
    available_margin: float,
    account_currency: str,
    price_lookup: PriceLookup,
    cushion_pct: float = DEFAULT_MARGIN_CUSHION_PCT,
) -> int:
    """Reduce *units* so the margin requirement fits within *available_margin*.

    Returns 0 if even :data:`MIN_UNITS_ODD_LOT` cannot be margined.
    """
    margin_req = compute_margin_requirement(
        pair, units, entry_price, account_currency, price_lookup,
    )
    if margin_req is None:
        return 0

    allowed, _ = check_margin_available(
        margin_req.margin_required, available_margin, cushion_pct,
    )
    if allowed:
        return abs(units)

    usable = available_margin * (1.0 - cushion_pct / 100.0)
    if usable <= 0 or margin_req.units <= 0 or margin_req.margin_required <= 0:
        return 0

    margin_per_unit = margin_req.margin_required / margin_req.units
    max_units = int(usable / margin_per_unit)

    if max_units < MIN_UNITS_ODD_LOT:
        return 0
    return max_units
