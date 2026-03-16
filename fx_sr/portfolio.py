"""Shared portfolio policy helpers for backtest and live execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Optional

import pandas as pd

from .sizing import calculate_risk_amount
from .strategy import StrategyParams, get_correlated_pairs


@dataclass(frozen=True)
class ClosedTradeSummary:
    """Normalized closed-trade facts used by shared portfolio policy."""

    pair: str
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    pnl_r: float
    quality_score: float = 0.0
    risk_amount: float | None = None
    pnl_amount: float | None = None
    trade_id: str | None = None


@dataclass(frozen=True)
class CorrelationExposure:
    """One active exposure considered by the shared correlation policy."""

    pair: str
    quality_score: float = 0.0
    replaceable: bool = True
    payload: object = None


@dataclass
class PortfolioState:
    """Incremental portfolio policy state shared by live and compounding paths."""

    params: StrategyParams
    latest_pair_closes: dict[str, ClosedTradeSummary] = field(default_factory=dict)
    consecutive_losses: int = 0
    pause_until: pd.Timestamp | None = None
    balance: float | None = None
    peak_balance: float | None = None

    def record_closed_trade(self, trade: ClosedTradeSummary) -> None:
        """Advance the portfolio state by one newly closed trade."""

        pair = trade.pair.upper()
        existing = self.latest_pair_closes.get(pair)
        if existing is None or trade.exit_time >= existing.exit_time:
            self.latest_pair_closes[pair] = trade

        self.consecutive_losses, self.pause_until = update_streak_pause_state(
            self.consecutive_losses,
            self.pause_until,
            pnl_r=trade.pnl_r,
            exit_time=trade.exit_time,
            params=self.params,
        )

        if self.balance is None or self.peak_balance is None:
            return
        if trade.pnl_amount is None:
            self.balance = None
            self.peak_balance = None
            return

        self.balance += float(trade.pnl_amount)
        self.peak_balance = max(float(self.peak_balance), float(self.balance))

    def latest_pair_close(self, pair: str) -> ClosedTradeSummary | None:
        """Return the latest closed trade for one pair, if any."""

        return self.latest_pair_closes.get(pair.upper())

    def entry_block(self, pair: str, entry_time: pd.Timestamp) -> tuple[str, str] | None:
        """Return the shared portfolio-policy block reason for one candidate entry."""

        entry_time = pd.Timestamp(entry_time)
        if self.pause_until is not None and entry_time <= self.pause_until:
            return (
                'PAUSED',
                f"Portfolio pause until {pd.Timestamp(self.pause_until).isoformat()}",
            )

        last_trade = self.latest_pair_close(pair)
        if last_trade is None:
            return None

        if is_pair_cooldown_active(
            entry_time,
            last_exit_time=last_trade.exit_time,
            last_pnl_r=last_trade.pnl_r,
            params=self.params,
        ):
            end_time = cooldown_end_time(last_trade.exit_time, last_trade.pnl_r, self.params)
            label = (
                'Loss cooldown'
                if last_trade.pnl_r <= 0 and self.params.loss_cooldown_bars > self.params.cooldown_bars
                else 'Cooldown'
            )
            return (
                'COOLDOWN',
                f"{label} until {pd.Timestamp(end_time).isoformat()}",
            )

        return None

    def effective_risk_pct(
        self,
        base_risk_pct: float,
        *,
        balance: float,
        quality_score: float = 0.0,
    ) -> float:
        """Apply the shared drawdown and quality adjustments using cached state."""

        return calculate_effective_risk_pct(
            base_risk_pct,
            params=self.params,
            balance=balance,
            peak_balance=self.peak_balance,
            quality_score=quality_score,
        )

    def sync_balance(self, current_balance: float | None) -> None:
        """Refresh the observed account balance without replaying history."""

        if current_balance is None:
            return

        self.balance = float(current_balance)
        if self.peak_balance is None:
            self.peak_balance = float(current_balance)
        else:
            self.peak_balance = max(float(self.peak_balance), float(current_balance))


def _as_timestamp(value) -> pd.Timestamp | None:
    if value in (None, ''):
        return None
    return pd.Timestamp(value)


def _coerce_float(value, default: float = 0.0) -> float:
    if value in (None, ''):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def closed_trade_summary_key(trade: ClosedTradeSummary) -> str:
    """Return a stable identity for one closed-trade summary."""

    if trade.trade_id:
        return str(trade.trade_id)
    return "|".join(
        [
            trade.pair.upper(),
            pd.Timestamp(trade.entry_time).isoformat(),
            pd.Timestamp(trade.exit_time).isoformat(),
        ]
    )


def build_portfolio_state(
    closed_trades: Iterable[ClosedTradeSummary],
    *,
    params: StrategyParams,
    current_balance: float | None = None,
) -> PortfolioState:
    """Build incremental portfolio state from closed-trade history."""

    trades = sorted(
        list(closed_trades),
        key=lambda trade: (trade.entry_time, trade.exit_time),
    )

    balance: float | None = None
    peak_balance: float | None = None
    if current_balance is not None:
        if not trades:
            balance = float(current_balance)
            peak_balance = float(current_balance)
        elif all(trade.pnl_amount is not None for trade in trades):
            pnl_total = sum(float(trade.pnl_amount or 0.0) for trade in trades)
            starting_balance = float(current_balance) - pnl_total
            if starting_balance > 0:
                balance = starting_balance
                peak_balance = starting_balance

    state = PortfolioState(
        params=params,
        balance=balance,
        peak_balance=peak_balance,
    )
    for trade in trades:
        state.record_closed_trade(trade)
    state.sync_balance(current_balance)
    return state


def closed_trade_summary_from_row(row: dict) -> ClosedTradeSummary | None:
    """Build a closed-trade summary from one detected-signal history row."""

    if not row:
        return None

    entry_time = (
        _as_timestamp(row.get('opened_at'))
        or _as_timestamp(row.get('executed_at'))
        or _as_timestamp(row.get('signal_time'))
    )
    exit_time = (
        _as_timestamp(row.get('closed_at'))
        or _as_timestamp(row.get('exit_signal_at'))
    )
    if entry_time is None or exit_time is None:
        return None

    entry_price = _coerce_float(
        row.get('opened_price'),
        default=_coerce_float(row.get('entry_price')),
    )
    stop_price = _coerce_float(row.get('sl_price'))
    close_price = row.get('closed_price')
    if close_price in (None, ''):
        close_price = row.get('exit_signal_price')

    pnl_r: float
    risk = abs(entry_price - stop_price)
    if close_price not in (None, '') and risk > 0:
        close_value = float(close_price)
        if (row.get('direction') or '').upper() == 'SHORT':
            pnl_r = (entry_price - close_value) / risk
        else:
            pnl_r = (close_value - entry_price) / risk
    else:
        pnl_pips = _coerce_float(row.get('pnl_pips'))
        if pnl_pips > 0:
            pnl_r = 1.0
        elif pnl_pips < 0:
            pnl_r = -1.0
        else:
            pnl_r = 0.0

    risk_amount_raw = row.get('risk_amount')
    risk_amount = (
        None
        if risk_amount_raw in (None, '')
        else float(risk_amount_raw)
    )
    pnl_amount = (
        None
        if risk_amount is None
        else float(risk_amount) * float(pnl_r)
    )

    return ClosedTradeSummary(
        trade_id=row.get('signal_id'),
        pair=(row.get('pair') or '').upper(),
        entry_time=entry_time,
        exit_time=exit_time,
        pnl_r=float(pnl_r),
        quality_score=_coerce_float(row.get('quality_score')),
        risk_amount=risk_amount,
        pnl_amount=pnl_amount,
    )


def update_streak_pause_state(
    consecutive_losses: int,
    pause_until: pd.Timestamp | None,
    *,
    pnl_r: float,
    exit_time: pd.Timestamp | None,
    params: StrategyParams,
) -> tuple[int, pd.Timestamp | None]:
    """Advance the losing-streak circuit-breaker state by one closed trade."""

    if pnl_r <= 0:
        consecutive_losses += 1
    else:
        consecutive_losses = 0
        pause_until = None

    if (
        params.streak_pause_trigger > 0
        and consecutive_losses >= params.streak_pause_trigger
        and exit_time is not None
    ):
        pause_until = pd.Timestamp(exit_time) + pd.Timedelta(hours=params.streak_pause_hours)
        consecutive_losses = 0

    return consecutive_losses, pause_until


def compute_pause_until(
    closed_trades: Iterable[ClosedTradeSummary],
    params: StrategyParams,
) -> pd.Timestamp | None:
    """Replay the closed-trade history and return the active streak pause, if any."""

    if isinstance(closed_trades, PortfolioState):
        return closed_trades.pause_until
    return build_portfolio_state(closed_trades, params=params).pause_until


def cooldown_end_time(
    last_exit_time: pd.Timestamp | None,
    last_pnl_r: float | None,
    params: StrategyParams,
) -> pd.Timestamp | None:
    """Return the first eligible entry time after the pair cooldown expires."""

    if last_exit_time is None:
        return None

    cooldown_hours = int(params.cooldown_bars)
    if (last_pnl_r or 0.0) <= 0 and params.loss_cooldown_bars > 0:
        cooldown_hours = max(cooldown_hours, int(params.loss_cooldown_bars))

    if cooldown_hours <= 0:
        return pd.Timestamp(last_exit_time)
    return pd.Timestamp(last_exit_time) + pd.Timedelta(hours=cooldown_hours)


def is_pair_cooldown_active(
    entry_time: pd.Timestamp,
    *,
    last_exit_time: pd.Timestamp | None,
    last_pnl_r: float | None,
    params: StrategyParams,
) -> bool:
    """Return True when a pair-level cooldown should block a new entry."""

    end_time = cooldown_end_time(last_exit_time, last_pnl_r, params)
    if end_time is None:
        return False
    return pd.Timestamp(entry_time) < end_time


def latest_pair_close(
    pair: str,
    closed_trades: Iterable[ClosedTradeSummary],
) -> ClosedTradeSummary | None:
    """Return the latest closed trade for one pair, if any."""

    if isinstance(closed_trades, PortfolioState):
        return closed_trades.latest_pair_close(pair)

    pair = pair.upper()
    latest: ClosedTradeSummary | None = None
    for trade in closed_trades:
        if trade.pair != pair:
            continue
        if latest is None or trade.exit_time > latest.exit_time:
            latest = trade
    return latest


def get_entry_block(
    pair: str,
    entry_time: pd.Timestamp,
    closed_trades: Iterable[ClosedTradeSummary],
    params: StrategyParams,
) -> tuple[str, str] | None:
    """Return a shared portfolio-policy block reason for a candidate entry."""

    if isinstance(closed_trades, PortfolioState):
        return closed_trades.entry_block(pair, entry_time)
    return build_portfolio_state(closed_trades, params=params).entry_block(pair, entry_time)


def reconstruct_peak_balance(
    current_balance: float,
    closed_trades: Iterable[ClosedTradeSummary],
) -> float | None:
    """Reconstruct peak equity from realized closed-trade cash P&L history."""

    if isinstance(closed_trades, PortfolioState):
        if closed_trades.peak_balance is not None:
            return max(float(closed_trades.peak_balance), float(current_balance))
        return float(current_balance) if not closed_trades.latest_pair_closes else None

    trades = list(closed_trades)
    if not trades:
        return float(current_balance)
    if any(trade.pnl_amount is None for trade in trades):
        return None

    ordered = sorted(trades, key=lambda trade: (trade.entry_time, trade.exit_time))
    pnl_total = sum(float(trade.pnl_amount or 0.0) for trade in ordered)
    starting_balance = float(current_balance) - pnl_total
    if starting_balance <= 0:
        return None

    balance = starting_balance
    peak_balance = starting_balance
    for trade in ordered:
        balance += float(trade.pnl_amount or 0.0)
        peak_balance = max(peak_balance, balance)

    return max(peak_balance, float(current_balance))


def calculate_effective_risk_pct(
    base_risk_pct: float,
    *,
    params: StrategyParams,
    balance: float,
    peak_balance: float | None = None,
    quality_score: float = 0.0,
) -> float:
    """Apply shared drawdown and quality risk adjustments to a base risk%."""

    effective_risk = max(float(base_risk_pct), 0.0)

    if params.dynamic_risk and peak_balance is not None and peak_balance > 0:
        dd_pct = (float(peak_balance) - float(balance)) / float(peak_balance) * 100.0
        if dd_pct <= params.dd_risk_start:
            effective_risk = max(float(base_risk_pct), 0.0)
        elif dd_pct >= params.dd_risk_full:
            effective_risk = max(float(params.dd_risk_floor) / 100.0, 0.0)
        else:
            frac = (dd_pct - params.dd_risk_start) / (params.dd_risk_full - params.dd_risk_start)
            floor = float(params.dd_risk_floor) / 100.0
            effective_risk = float(base_risk_pct) - (float(base_risk_pct) - floor) * frac

    if params.quality_sizing:
        multiplier = params.quality_risk_min + float(quality_score) * (
            params.quality_risk_max - params.quality_risk_min
        )
        effective_risk *= multiplier

    return max(float(effective_risk), 0.0)


def calculate_effective_risk_amount(
    balance: float,
    base_risk_pct: float,
    *,
    params: StrategyParams,
    peak_balance: float | None = None,
    quality_score: float = 0.0,
) -> float:
    """Return the cash risk for one entry after shared portfolio adjustments."""

    effective_risk_pct = calculate_effective_risk_pct(
        base_risk_pct,
        params=params,
        balance=balance,
        peak_balance=peak_balance,
        quality_score=quality_score,
    )
    return calculate_risk_amount(balance, effective_risk_pct)


def apply_correlation_policy(
    exposures: Iterable[CorrelationExposure],
    *,
    candidate_pair: str,
    candidate_quality: float,
    params: StrategyParams,
) -> tuple[bool, CorrelationExposure | None]:
    """Decide whether a candidate can be added under the shared correlation policy."""

    if not params.use_correlation_filter:
        return True, None

    candidate_pair = candidate_pair.upper()
    correlation_cap = max(int(params.max_correlated_trades), 1)
    correlated_pairs = get_correlated_pairs(candidate_pair)
    relevant = [
        exposure
        for exposure in exposures
        if exposure.pair == candidate_pair or exposure.pair in correlated_pairs
    ]

    if len(relevant) < correlation_cap:
        return True, None
    if not params.correlation_prefer_quality:
        return False, None

    replaceable = [exposure for exposure in relevant if exposure.replaceable]
    if not replaceable:
        return False, None

    worst = min(replaceable, key=lambda exposure: exposure.quality_score)
    if float(candidate_quality) > float(worst.quality_score):
        return True, worst
    return False, None
