"""Backtest-vs-live signal parity reporting.

The report is intentionally read-only. It compares cached backtest trade
entries with persisted live detected-signal rows and explains whether each
backtest signal was seen and whether a live order was placed.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date as date_cls, datetime, time, timedelta
import json
import os
from pathlib import Path
import re
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from .backtest import BACKTEST_CACHE_VERSION, _deserialize_backtest_result
from .config import PAIRS
from . import db
from .signal_store import ensure_signal_tables, row_to_dict


DEFAULT_LOCAL_TZ = os.getenv("FX_SR_LOCAL_TZ", "UTC")


def _to_utc_ts(value: Any) -> pd.Timestamp | None:
    if value in (None, ""):
        return None
    try:
        ts = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(ts):
        return None
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _to_iso(value: Any) -> str | None:
    ts = _to_utc_ts(value)
    if ts is None:
        return None
    return ts.isoformat()


def _to_local_iso(value: Any, *, local_tz: str = DEFAULT_LOCAL_TZ) -> str | None:
    ts = _to_utc_ts(value)
    if ts is None:
        return None
    return ts.tz_convert(local_tz).isoformat()


def _minute_ts(value: Any) -> pd.Timestamp | None:
    ts = _to_utc_ts(value)
    if ts is None:
        return None
    return ts.round("min")


def _date_window_utc(selected_date: date_cls | str | None, local_tz: str = DEFAULT_LOCAL_TZ) -> tuple[str, pd.Timestamp, pd.Timestamp]:
    if selected_date is None:
        local_date = datetime.now(ZoneInfo(local_tz)).date()
    elif isinstance(selected_date, date_cls):
        local_date = selected_date
    else:
        local_date = date_cls.fromisoformat(str(selected_date))

    tz = ZoneInfo(local_tz)
    local_start = datetime.combine(local_date, time.min, tzinfo=tz)
    local_end = local_start + timedelta(days=1)
    return (
        local_date.isoformat(),
        pd.Timestamp(local_start).tz_convert("UTC"),
        pd.Timestamp(local_end).tz_convert("UTC"),
    )


def _cache_key(row: dict) -> str:
    return "|".join(
        [
            str(row.get("params_hash") or ""),
            str(int(row.get("hourly_days") or 0)),
            str(int(row.get("zone_history_days") or 0)),
            str(row.get("execution_mode") or "intrabar"),
            str(row.get("run_id") or ""),
        ]
    )


def _legacy_cache_key(row: dict) -> str:
    return "|".join(
        [
            str(row.get("params_hash") or ""),
            str(int(row.get("hourly_days") or 0)),
            str(int(row.get("zone_history_days") or 0)),
            str(row.get("execution_mode") or "intrabar"),
        ]
    )


def _parse_run_config(raw: str | None) -> dict:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _compact_text(value: Any, limit: int = 220) -> str | None:
    if value in (None, ""):
        return None
    text = str(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _select_latest_backtest_rows(
    *,
    pair: str | None = None,
    backtest_key: str | None = None,
    db_path: str | None = None,
) -> tuple[list[dict], dict | None, list[dict]]:
    rows = [
        row for row in db.load_backtest_results(pairs=[pair] if pair else None, db_path=db_path)
        if row.get("strategy_version") == BACKTEST_CACHE_VERSION
    ]
    if not rows:
        return [], None, []

    groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        groups[_cache_key(row)].append(row)

    selected_key = None
    if backtest_key:
        for key, grouped in groups.items():
            if key == backtest_key or any(_legacy_cache_key(row) == backtest_key for row in grouped):
                selected_key = key
                break
        if selected_key is None:
            return [], None, []

    if selected_key is None:
        selected_key = max(
            groups,
            key=lambda key: max(_to_utc_ts(row.get("updated_at")) or pd.Timestamp.min.tz_localize("UTC") for row in groups[key]),
        )

    selected_rows = groups[selected_key]
    latest_by_pair: dict[str, dict] = {}
    for row in sorted(selected_rows, key=lambda item: _to_utc_ts(item.get("updated_at")) or pd.Timestamp.min.tz_localize("UTC"), reverse=True):
        latest_by_pair.setdefault(str(row.get("pair") or "").upper(), row)

    representative = next(iter(latest_by_pair.values())) if latest_by_pair else selected_rows[0]
    run_config = _parse_run_config(representative.get("run_config_json"))
    selected = {
        "key": selected_key,
        "legacy_key": _legacy_cache_key(representative),
        "run_id": representative.get("run_id") or "",
        "params_hash": representative.get("params_hash") or "",
        "hourly_days": int(representative.get("hourly_days") or 0),
        "zone_history_days": int(representative.get("zone_history_days") or 0),
        "execution_mode": representative.get("execution_mode") or "intrabar",
        "profile": run_config.get("resolved_profile") or run_config.get("requested_profile"),
        "selection_label": run_config.get("selection_label"),
        "updated_at": _to_iso(representative.get("updated_at")),
        "pair_count": len(latest_by_pair),
    }

    available = []
    for key, grouped in groups.items():
        newest = max(grouped, key=lambda item: _to_utc_ts(item.get("updated_at")) or pd.Timestamp.min.tz_localize("UTC"))
        available.append(
            {
                "key": key,
                "legacy_key": _legacy_cache_key(newest),
                "pair_count": len({row.get("pair") for row in grouped}),
                "updated_at": _to_iso(newest.get("updated_at")),
            }
        )
    available.sort(key=lambda item: item.get("updated_at") or "", reverse=True)
    return list(latest_by_pair.values()), selected, available[:10]


def _backtest_trade_row(pair: str, trade: Any, source_row: dict) -> dict:
    return {
        "source": "backtest",
        "pair": pair,
        "direction": str(getattr(trade, "direction", "") or "").upper(),
        "entry_time": _to_iso(getattr(trade, "entry_time", None)),
        "exit_time": _to_iso(getattr(trade, "exit_time", None)),
        "entry_price": _float_or_none(getattr(trade, "entry_price", None)),
        "sl_price": _float_or_none(getattr(trade, "sl_price", None)),
        "tp_price": _float_or_none(getattr(trade, "tp_price", None)),
        "zone_upper": _float_or_none(getattr(trade, "zone_upper", None)),
        "zone_lower": _float_or_none(getattr(trade, "zone_lower", None)),
        "zone_strength": getattr(trade, "zone_strength", None),
        "pnl_pips": _float_or_none(getattr(trade, "pnl_pips", None)),
        "pnl_r": _float_or_none(getattr(trade, "pnl_r", None)),
        "exit_reason": getattr(trade, "exit_reason", None),
        "bars_held": int(getattr(trade, "bars_held", 0) or 0),
        "backtest_updated_at": _to_iso(source_row.get("updated_at")),
        "backtest_run_id": source_row.get("run_id") or "",
        "backtest_params_hash": source_row.get("params_hash") or "",
        "backtest_execution_mode": source_row.get("execution_mode") or "intrabar",
    }


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_latest_backtest_trades(
    *,
    selected_date: date_cls | str | None = None,
    pair: str | None = None,
    backtest_key: str | None = None,
    local_tz: str = DEFAULT_LOCAL_TZ,
    db_path: str | None = None,
) -> tuple[list[dict], dict | None, list[dict]]:
    pair = pair.upper() if pair else None
    date_label, start_utc, end_utc = _date_window_utc(selected_date, local_tz)
    rows, selected, available = _select_latest_backtest_rows(
        pair=pair,
        backtest_key=backtest_key,
        db_path=db_path,
    )

    trades: list[dict] = []
    for row in rows:
        try:
            result = _deserialize_backtest_result(row["result_json"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            continue
        for trade in list(result.trades) + list(getattr(result, "pending_trades", []) or []):
            trade_row = _backtest_trade_row(str(row["pair"]).upper(), trade, row)
            entry_ts = _to_utc_ts(trade_row.get("entry_time"))
            if entry_ts is None:
                continue
            if start_utc <= entry_ts < end_utc:
                trades.append(trade_row)

    trades.sort(key=lambda item: item.get("entry_time") or "")
    if selected is not None:
        selected = {**selected, "date": date_label}
    return trades, selected, available


def load_live_signals_for_window(
    *,
    selected_date: date_cls | str | None = None,
    pair: str | None = None,
    local_tz: str = DEFAULT_LOCAL_TZ,
    db_path: str | None = None,
) -> tuple[list[dict], list[dict]]:
    pair = pair.upper() if pair else None
    _date_label, start_utc, end_utc = _date_window_utc(selected_date, local_tz)
    db_path = ensure_signal_tables(db_path)
    conn = db._connect(db_path)
    try:
        filters = ["signal_time >= %s", "signal_time < %s"]
        params: list[Any] = [start_utc.to_pydatetime(), end_utc.to_pydatetime()]
        if pair:
            filters.append("pair=%s")
            params.append(pair)
        cursor = conn.execute(
            f"""
            SELECT *
            FROM detected_signal
            WHERE {' AND '.join(filters)}
            ORDER BY signal_time, detected_at, signal_id
            """,
            params,
        )
        signals = [row_to_dict(cursor, row) for row in cursor.fetchall()]
        signals.extend(
            _load_order_audit_only_rows_conn(
                conn,
                start_utc=start_utc,
                end_utc=end_utc,
                pair=pair,
                detected_signals=signals,
            )
        )

        event_start = (start_utc - pd.Timedelta(days=1)).to_pydatetime()
        event_end = (end_utc + pd.Timedelta(days=1)).to_pydatetime()
        event_cursor = conn.execute(
            """
            SELECT event_time, event_type, detail
            FROM system_event
            WHERE event_time >= %s AND event_time < %s
            ORDER BY event_time
            """,
            (event_start, event_end),
        )
        events = [
            {
                "event_time": _to_iso(row[0]),
                "event_type": row[1],
                "detail": _compact_text(row[2]),
            }
            for row in event_cursor.fetchall()
        ]
        return signals, events
    finally:
        conn.close()


def _parse_json_object(raw: Any) -> dict:
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _order_id_tokens(value: Any) -> set[str]:
    if value in (None, ""):
        return set()
    if isinstance(value, (list, tuple, set)):
        return {str(item).strip() for item in value if str(item).strip()}
    return {
        token.strip()
        for token in str(value).replace("[", "").replace("]", "").split(",")
        if token.strip()
    }


def _first_present(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _audit_quantity(request: dict, response: dict) -> int | None:
    value = _first_present(
        request.get("quantity"),
        request.get("units"),
        request.get("size"),
        response.get("quantity"),
        response.get("units"),
        response.get("size"),
        response.get("filled_units"),
    )
    if value is None:
        return None
    try:
        return int(abs(float(value)))
    except (TypeError, ValueError):
        return None


def _parse_strategy_order_ref(order_ref: Any) -> dict | None:
    raw = str(order_ref or "").strip()
    parts = raw.split(":")
    if len(parts) < 4 or parts[0] != "fxsr":
        return None
    pair = parts[1].upper()
    direction = parts[2].upper()
    stamp = parts[3]
    if len(pair) != 6 or direction not in {"LONG", "SHORT"}:
        return None
    try:
        signal_time = pd.Timestamp(datetime.strptime(stamp, "%Y%m%d%H%M%S"), tz="UTC")
    except Exception:
        return None

    suffix = ":".join(parts[4:]) if len(parts) > 4 else ""
    role = "ENTRY"
    lower_suffix = suffix.lower()
    if lower_suffix == "tp" or ":tp" in lower_suffix:
        role = "TAKE_PROFIT"
    elif lower_suffix == "sl" or ":sl" in lower_suffix:
        role = "STOP_LOSS"
    elif "close" in lower_suffix or "liquidate" in lower_suffix or "recovery" in lower_suffix:
        role = "CLOSE"
    elif "rebracket" in lower_suffix:
        role = "PROTECTION"

    return {
        "raw": raw,
        "pair": pair,
        "direction": direction,
        "signal_time": signal_time,
        "parent_ref": f"fxsr:{pair}:{direction}:{stamp}",
        "suffix": suffix,
        "role": role,
    }


def _audit_order_ref_details(event: dict) -> dict | None:
    request = event.get("request") or {}
    response = event.get("response") or {}
    return _parse_strategy_order_ref(
        _first_present(
            request.get("order_ref"),
            response.get("order_ref"),
        )
    )


def _audit_has_execution_evidence(response: dict) -> bool:
    status = str(response.get("status") or "").strip().upper()
    if status in {"FILLED", "PARTIAL", "OPEN"}:
        return True
    if response.get("avg_fill_price") not in (None, ""):
        return True
    try:
        return float(response.get("filled_units") or 0) > 0
    except (TypeError, ValueError):
        return False


def _is_audit_only_row(row: dict | None) -> bool:
    return bool(row and (row.get("_audit_only") or str(row.get("status") or "").upper() == "AUDIT_ONLY"))


def _is_startup_replay_row(row: dict | None) -> bool:
    """Return True when a live row was reconstructed by a startup walk-forward.

    Rows written with ``detection_source='startup_replay'`` carry the marker
    on ``quote_source`` (see ``live_history.record_detected_signals``). They
    do not represent a real-time live detection — the walk-forward rebuilt
    them after the fact — so they must not consume a match slot against a
    backtest trade.
    """

    if not row:
        return False
    return str(row.get("quote_source") or "").strip().lower() == "startup_replay"


def _load_order_audit_only_rows_conn(
    conn,
    *,
    start_utc: pd.Timestamp,
    end_utc: pd.Timestamp,
    pair: str | None,
    detected_signals: list[dict],
) -> list[dict]:
    entry_functions = {"submit_fx_market_bracket_order", "submit_fx_market_order"}
    entry_actions = {"submit_entry", "submit"}
    detected_order_ids: set[str] = set()
    for row in detected_signals:
        for key in ("order_id", "take_profit_order_id", "stop_loss_order_id"):
            detected_order_ids.update(_order_id_tokens(row.get(key)))

    filters = ["event_ts >= %s", "event_ts < %s"]
    params: list[Any] = [start_utc.to_pydatetime(), end_utc.to_pydatetime()]
    if pair:
        filters.append("pair=%s")
        params.append(pair)

    try:
        cursor = conn.execute(
            f"""
            SELECT id, event_ts, function_name, pair, direction, action,
                   request_json, response_json, error, duration_ms, order_ids
            FROM order_audit_log
            WHERE {' AND '.join(filters)}
            ORDER BY event_ts ASC, id ASC
            """,
            params,
        )
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        return []

    events = []
    for raw in cursor.fetchall():
        event = {
            "event_id": raw[0],
            "event_ts": raw[1],
            "function_name": raw[2],
            "pair": str(raw[3] or "").upper() or None,
            "direction": str(raw[4] or "").upper() or None,
            "action": raw[5],
            "request": _parse_json_object(raw[6]),
            "response": _parse_json_object(raw[7]),
            "error": raw[8],
            "duration_ms": raw[9],
            "order_ids": raw[10],
        }
        if not event["pair"] or not event["direction"]:
            continue
        order_ids = _order_id_tokens(event["order_ids"])
        order_ids.update(_order_id_tokens(event["response"].get("order_id")))
        order_ids.update(_order_id_tokens(event["response"].get("take_profit_order_id")))
        order_ids.update(_order_id_tokens(event["response"].get("stop_loss_order_id")))
        if order_ids and detected_order_ids.intersection(order_ids):
            continue
        ref_details = _audit_order_ref_details(event)
        if str(event.get("function_name") or "") not in entry_functions:
            continue
        if str(event.get("action") or "").lower() not in entry_actions:
            continue
        if not ref_details or ref_details.get("role") != "ENTRY":
            continue
        signal_time = _to_utc_ts(ref_details.get("signal_time"))
        if signal_time is None or not (start_utc <= signal_time < end_utc):
            continue
        event["order_id"] = next(iter(sorted(order_ids)), None)
        event["ref_details"] = ref_details
        events.append(event)

    grouped: dict[str, list[dict]] = defaultdict(list)
    for event in events:
        grouped[str(event["ref_details"]["parent_ref"])].append(event)

    rows = []
    for parent_ref, grouped_events in grouped.items():
        grouped_events.sort(key=lambda item: (_to_utc_ts(item.get("event_ts")) or pd.Timestamp.min.tz_localize("UTC"), int(item.get("event_id") or 0)))
        first = grouped_events[0]
        latest = grouped_events[-1]
        request = first["request"]
        response = latest["response"] or first["response"]
        order_id = first.get("order_id") or latest.get("order_id")
        latest_status = str(_first_present(response.get("status"), latest.get("action"), first.get("action")) or "")
        has_execution_evidence = _audit_has_execution_evidence(response)
        ref_details = first["ref_details"]
        rows.append(
            {
                "signal_id": None,
                "pair": ref_details["pair"],
                "direction": ref_details["direction"],
                "status": "AUDIT_ONLY",
                "transacted": 1 if has_execution_evidence else 0,
                "execution_enabled": 1,
                "signal_time": ref_details["signal_time"],
                "detected_at": None,
                "opened_at": latest["event_ts"] if has_execution_evidence else None,
                "executed_at": latest["event_ts"] if has_execution_evidence else None,
                "closed_at": None,
                "entry_price": _first_present(
                    response.get("avg_fill_price"),
                    response.get("submitted_entry_price"),
                    request.get("entry_price"),
                ),
                "submitted_entry_price": _first_present(
                    response.get("submitted_entry_price"),
                    request.get("entry_price"),
                ),
                "opened_price": response.get("avg_fill_price"),
                "closed_price": None,
                "planned_units": _audit_quantity(request, response),
                "open_units": _audit_quantity(request, response) if has_execution_evidence else None,
                "remaining_units": response.get("remaining_units"),
                "fill_count": None,
                "pnl_pips": None,
                "pnl_r": None,
                "pnl_gbp": None,
                "broker_order_status": latest_status or None,
                "order_id": order_id,
                "note": "Order audit row (no matching detected signal)",
                "needs_investigation": any(bool(event.get("error")) for event in grouped_events),
                "investigation_reason": next((event.get("error") for event in grouped_events if event.get("error")), "") or "",
                "order_audit_event_count": len(grouped_events),
                "event_id": latest["event_id"],
                "event_ts": latest["event_ts"],
                "_audit_only": True,
                "_audit_parent_ref": parent_ref,
                "_audit_execution_evidence": has_execution_evidence,
            }
        )
    return rows


def _live_was_placed(row: dict) -> bool:
    status = str(row.get("status") or "").upper()
    if _is_audit_only_row(row):
        if row.get("_audit_execution_evidence") is not None:
            return bool(row.get("_audit_execution_evidence"))
        broker_status = str(row.get("broker_order_status") or "").strip().upper()
        return (
            broker_status in {"FILLED", "PARTIAL", "OPEN"}
            or row.get("opened_price") not in (None, "")
            or row.get("executed_at") not in (None, "")
        )
    if status == "CAPITAL_ONLY":
        return True
    if status == "NOT_TRANSACTED":
        return False
    if int(row.get("transacted") or 0) == 1:
        return True
    if row.get("broker_order_status") not in (None, "") and status not in {"SKIPPED", "FAILED"}:
        return True
    if row.get("order_id") is not None:
        return True
    if row.get("opened_at") not in (None, ""):
        return True
    try:
        return int(row.get("open_units") or 0) > 0
    except (TypeError, ValueError):
        return False


def _snake_reason(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return text or "not_executed"


def _reason_for_live_row(row: dict) -> str | None:
    if _live_was_placed(row):
        return None

    note = str(row.get("note") or "").strip()
    status = str(row.get("status") or "").strip().upper()
    lower = note.lower()

    if _is_audit_only_row(row):
        if row.get("needs_investigation"):
            return "audit_only_needs_investigation"
        return "audit_only_signal"

    if "entry drift" in lower:
        return "entry_drift_too_large"
    if "execution paused" in lower or "manual" in lower:
        return "manual_stop"
    if "risk budget" in lower or "margin" in lower or "liquidation" in lower:
        return "risk_reject"
    if "correlation" in lower:
        return "correlation_cap"
    if "position/order exists" in lower or "duplicate" in lower or "orderref already" in lower:
        return "duplicate_or_existing_order"
    if "quote unavailable" in lower or "stale quote" in lower:
        return "quote_unavailable"
    if "size unavailable" in lower or "not planned" in lower:
        return "sizing_unavailable"
    if status == "FAILED":
        return "broker_reject_or_failure"
    if status == "SKIPPED" and note:
        return _snake_reason(note)
    if status:
        return _snake_reason(status)
    return "matched_not_executed"


def _reason_for_unmatched_live_row(row: dict) -> str:
    reason = _reason_for_live_row(row)
    if reason:
        return reason
    if _is_audit_only_row(row):
        return "audit_only_signal"
    return "live_only_signal"


def _reason_for_missing_live(backtest_time: pd.Timestamp, system_events: list[dict], max_age_hours: float) -> str:
    startup_cutoff = pd.Timedelta(hours=float(max_age_hours))
    startup_times: list[pd.Timestamp] = []
    for event in system_events:
        if str(event.get("event_type") or "").lower() != "startup":
            continue
        event_time = _to_utc_ts(event.get("event_time"))
        if event_time is None:
            continue
        if event_time >= backtest_time:
            startup_times.append(event_time)
    if not startup_times:
        return "not_detected"

    first_startup_after_signal = min(startup_times)
    if backtest_time < first_startup_after_signal - startup_cutoff:
        return "stale_replay_window"
    return "not_detected"


def _compact_backtest(row: dict | None) -> dict | None:
    if row is None:
        return None
    keys = [
        "pair",
        "direction",
        "entry_time",
        "exit_time",
        "entry_price",
        "sl_price",
        "tp_price",
        "zone_upper",
        "zone_lower",
        "zone_strength",
        "pnl_pips",
        "pnl_r",
        "exit_reason",
        "bars_held",
        "backtest_run_id",
        "backtest_params_hash",
        "backtest_execution_mode",
    ]
    return {key: row.get(key) for key in keys if key in row}


def _compact_live(row: dict | None) -> dict | None:
    if row is None:
        return None
    keys = [
        "signal_id",
        "pair",
        "direction",
        "signal_time",
        "detected_at",
        "status",
        "transacted",
        "execution_enabled",
        "entry_price",
        "sl_price",
        "tp_price",
        "zone_upper",
        "zone_lower",
        "zone_strength",
        "zone_type",
        "quality_score",
        "planned_units",
        "risk_amount",
        "order_id",
        "take_profit_order_id",
        "stop_loss_order_id",
        "note",
        "executed_at",
        "opened_at",
        "opened_price",
        "open_units",
        "remaining_units",
        "broker_order_status",
        "submitted_entry_price",
        "submitted_tp_price",
        "submitted_sl_price",
        "submit_bid",
        "submit_ask",
        "submit_spread",
        "quote_source",
        "quote_time",
        "last_updated_at",
    ]
    compact = {key: row.get(key) for key in keys if key in row}
    for key, value in list(compact.items()):
        if hasattr(value, "isoformat"):
            compact[key] = value.isoformat()
    return compact


def _build_report_row(
    *,
    backtest: dict | None,
    live: dict | None,
    status: str,
    reason: str | None,
) -> dict:
    backtest_time = _to_utc_ts(backtest.get("entry_time")) if backtest else None
    live_time = _to_utc_ts(live.get("signal_time")) if live else None
    execution_time = None
    if live:
        execution_time = _to_utc_ts(live.get("executed_at") or live.get("opened_at"))

    latency_seconds = None
    if backtest_time is not None and live_time is not None:
        latency_seconds = round((live_time - backtest_time).total_seconds(), 3)

    execution_latency_seconds = None
    if backtest_time is not None and execution_time is not None:
        execution_latency_seconds = round((execution_time - backtest_time).total_seconds(), 3)

    source = "backtest" if backtest else "live_only"
    return {
        "source": source,
        "pair": (backtest or live or {}).get("pair"),
        "direction": (backtest or live or {}).get("direction"),
        "backtest_time": _to_iso(backtest_time),
        "live_signal_time": _to_iso(live_time),
        "execution_time": _to_iso(execution_time),
        "status": status,
        "live_trade_placed": bool(live and _live_was_placed(live)),
        "latency_seconds": latency_seconds,
        "execution_latency_seconds": execution_latency_seconds,
        "reason": reason,
        "source_signal_id": live.get("signal_id") if live else None,
        "rejection_note": live.get("note") if live else None,
        "live_status": live.get("status") if live else None,
        "live_transacted": int(live.get("transacted") or 0) if live else None,
        "order_id": live.get("order_id") if live else None,
        "backtest": _compact_backtest(backtest),
        "live": _compact_live(live),
    }


def build_parity_report_from_rows(
    *,
    backtest_trades: list[dict],
    live_signals: list[dict],
    selected_date: str,
    pair: str | None = None,
    tolerance_minutes: float = 1.0,
    max_age_hours: float = 2.0,
    selected_backtest: dict | None = None,
    available_backtests: list[dict] | None = None,
    system_events: list[dict] | None = None,
    include_live_only: bool = True,
) -> dict:
    tolerance = pd.Timedelta(minutes=float(tolerance_minutes))
    system_events = system_events or []
    selected_pair = pair.upper() if pair else None

    live_by_key: dict[tuple[str, str], list[dict]] = defaultdict(list)
    startup_replay_rows: list[dict] = []
    for live in live_signals:
        live_pair = str(live.get("pair") or "").upper()
        live_direction = str(live.get("direction") or "").upper()
        if selected_pair and live_pair != selected_pair:
            continue
        # Pull replay rows out before they can consume a match slot — they
        # are surfaced in their own section so restart-era reconstructions
        # are still visible in the report.
        if _is_startup_replay_row(live):
            startup_replay_rows.append(live)
            continue
        live_ts = _minute_ts(live.get("signal_time"))
        if not live_pair or not live_direction or live_ts is None:
            continue
        live["_parity_minute_ts"] = live_ts
        live_by_key[(live_pair, live_direction)].append(live)

    for rows in live_by_key.values():
        rows.sort(key=lambda item: (_to_utc_ts(item.get("signal_time")) or pd.Timestamp.max.tz_localize("UTC"), str(item.get("signal_id") or "")))

    consumed_live_ids: set[int] = set()
    report_rows: list[dict] = []

    for backtest in sorted(backtest_trades, key=lambda item: (_to_utc_ts(item.get("entry_time")) or pd.Timestamp.max.tz_localize("UTC"), item.get("pair") or "")):
        bt_pair = str(backtest.get("pair") or "").upper()
        bt_direction = str(backtest.get("direction") or "").upper()
        if selected_pair and bt_pair != selected_pair:
            continue
        bt_ts = _minute_ts(backtest.get("entry_time"))
        if bt_ts is None:
            continue

        candidates = [
            live
            for live in live_by_key.get((bt_pair, bt_direction), [])
            if id(live) not in consumed_live_ids
            and abs(live["_parity_minute_ts"] - bt_ts) <= tolerance
        ]
        candidates.sort(
            key=lambda live: (
                abs(live["_parity_minute_ts"] - bt_ts),
                live["_parity_minute_ts"],
                str(live.get("signal_id") or ""),
            )
        )

        if candidates:
            live = candidates[0]
            consumed_live_ids.add(id(live))
            status = "matched_executed" if _live_was_placed(live) else "matched_not_executed"
            reason = _reason_for_live_row(live)
            report_rows.append(_build_report_row(backtest=backtest, live=live, status=status, reason=reason))
        else:
            reason = _reason_for_missing_live(bt_ts, system_events, max_age_hours)
            report_rows.append(_build_report_row(backtest=backtest, live=None, status="no_live_signal", reason=reason))

    if include_live_only:
        for rows in live_by_key.values():
            for live in rows:
                if id(live) in consumed_live_ids:
                    continue
                if _is_audit_only_row(live):
                    report_rows.append(
                        _build_report_row(
                            backtest=None,
                            live=live,
                            status="audit_only_evidence",
                            reason=_reason_for_unmatched_live_row(live),
                        )
                    )
                    continue
                report_rows.append(
                    _build_report_row(
                        backtest=None,
                        live=live,
                        status="live_only",
                        reason=_reason_for_unmatched_live_row(live),
                    )
                )

    report_rows.sort(
        key=lambda row: (
            row.get("backtest_time") or row.get("live_signal_time") or "",
            row.get("pair") or "",
            row.get("direction") or "",
            row.get("source_signal_id") or "",
        )
    )

    status_counts = Counter(row["status"] for row in report_rows)
    reason_counts = Counter(row.get("reason") or "executed" for row in report_rows if row["status"] != "matched_executed")
    placed = sum(1 for row in report_rows if row.get("live_trade_placed"))
    backtest_count = sum(1 for row in report_rows if row.get("source") == "backtest")
    live_only_count = sum(1 for row in report_rows if row.get("status") == "live_only")
    audit_only_count = sum(1 for row in report_rows if row.get("status") == "audit_only_evidence")

    return {
        "summary": {
            "date": selected_date,
            "pair": selected_pair,
            "tolerance_minutes": float(tolerance_minutes),
            "max_age_hours": float(max_age_hours),
            "backtest_signals": backtest_count,
            "live_signals": len(live_signals),
            "live_trades_placed": placed,
            "live_only": live_only_count,
            "audit_only_evidence": audit_only_count,
            "startup_replay": len(startup_replay_rows),
            "status_counts": dict(status_counts),
            "mismatch_reasons": dict(reason_counts),
        },
        "selected_backtest": selected_backtest,
        "available_backtests": available_backtests or [],
        "system_events": system_events,
        "rows": report_rows,
        "startup_replay": [_compact_live(row) for row in startup_replay_rows],
    }


def build_parity_report(
    *,
    selected_date: date_cls | str | None = None,
    pair: str | None = None,
    tolerance_minutes: float = 1.0,
    max_age_hours: float = 2.0,
    backtest_key: str | None = None,
    local_tz: str = DEFAULT_LOCAL_TZ,
    include_live_only: bool = True,
    db_path: str | None = None,
) -> dict:
    if pair:
        pair = pair.upper()
        if pair not in PAIRS:
            raise ValueError(f"Unknown pair: {pair}")

    date_label, _start_utc, _end_utc = _date_window_utc(selected_date, local_tz)
    backtest_trades, selected_backtest, available_backtests = load_latest_backtest_trades(
        selected_date=date_label,
        pair=pair,
        backtest_key=backtest_key,
        local_tz=local_tz,
        db_path=db_path,
    )
    live_signals, system_events = load_live_signals_for_window(
        selected_date=date_label,
        pair=pair,
        local_tz=local_tz,
        db_path=db_path,
    )
    return build_parity_report_from_rows(
        backtest_trades=backtest_trades,
        live_signals=live_signals,
        selected_date=date_label,
        pair=pair,
        tolerance_minutes=tolerance_minutes,
        max_age_hours=max_age_hours,
        selected_backtest=selected_backtest,
        available_backtests=available_backtests,
        system_events=system_events,
        include_live_only=include_live_only,
    )


def _live_event_times(row: dict) -> list[tuple[str, Any]]:
    """Return candidate (label, timestamp) pairs for matching a live row.

    Rows backed by a detected_signal contribute only their detection-time
    fields. Broker execution/fill timestamps (``executed_at``, ``opened_at``,
    ``event_ts``) can drift by minutes behind detection and, if used for
    matching, let a delayed fill of one trade consume the match slot of a
    *later* same-pair/direction trade. Audit-only rows have no detection
    timestamp, so they fall back to broker/order-event fields.
    """

    if _is_audit_only_row(row):
        return [
            ("signal_time", row.get("signal_time")),
            ("event_ts", row.get("event_ts")),
            ("executed_at", row.get("executed_at")),
            ("opened_at", row.get("opened_at")),
        ]
    return [
        ("signal_time", row.get("signal_time")),
        ("detected_at", row.get("detected_at")),
    ]


def _best_live_match(
    backtest: dict,
    live_rows: list[dict],
    *,
    window_seconds: int,
    consumed_ids: set[int] | None = None,
) -> tuple[dict | None, str | None, int | None]:
    bt_ts = _to_utc_ts(backtest.get("entry_time"))
    if bt_ts is None:
        return None, None, None
    pair = str(backtest.get("pair") or "").upper()
    direction = str(backtest.get("direction") or "").upper()
    candidates = []
    for live in live_rows:
        if consumed_ids is not None and id(live) in consumed_ids:
            continue
        # Startup-replay rows are reconstructed by the walk-forward at
        # process start; they represent what the signal *would* have been
        # had live been running, not a real-time detection. Matching them
        # would hide downtime by reporting a real backtest trade as "seen".
        if _is_startup_replay_row(live):
            continue
        if str(live.get("pair") or "").upper() != pair:
            continue
        if str(live.get("direction") or "").upper() != direction:
            continue
        for time_key, raw_ts in _live_event_times(live):
            live_ts = _to_utc_ts(raw_ts)
            if live_ts is None:
                continue
            delta_seconds = abs(int((live_ts - bt_ts).total_seconds()))
            audit_rank = 1 if _is_audit_only_row(live) else 0
            placed_rank = 0 if _live_was_placed(live) else 1
            candidates.append((audit_rank, placed_rank, delta_seconds, time_key, live))
    if not candidates:
        return None, None, None
    in_window = [item for item in candidates if item[2] <= window_seconds]
    if in_window:
        in_window.sort(key=lambda item: (item[0], item[1], item[2], item[3], str(item[4].get("signal_id") or item[4].get("event_id") or "")))
        _audit_rank, _placed_rank, delta_seconds, time_key, live = in_window[0]
        return live, time_key, delta_seconds
    candidates.sort(key=lambda item: (item[0], item[2], item[3], str(item[4].get("signal_id") or item[4].get("event_id") or "")))
    _audit_rank, _placed_rank, delta_seconds, time_key, _live = candidates[0]
    return None, time_key, delta_seconds


def _nearest_live(backtest: dict, live_rows: list[dict]) -> tuple[dict | None, str | None, int | None]:
    bt_ts = _to_utc_ts(backtest.get("entry_time"))
    if bt_ts is None:
        return None, None, None
    pair = str(backtest.get("pair") or "").upper()
    direction = str(backtest.get("direction") or "").upper()
    candidates = []
    for live in live_rows:
        if _is_startup_replay_row(live):
            continue
        if str(live.get("pair") or "").upper() != pair:
            continue
        if str(live.get("direction") or "").upper() != direction:
            continue
        for time_key, raw_ts in _live_event_times(live):
            live_ts = _to_utc_ts(raw_ts)
            if live_ts is None:
                continue
            candidates.append((abs(int((live_ts - bt_ts).total_seconds())), 1 if _is_audit_only_row(live) else 0, time_key, live))
    if not candidates:
        return None, None, None
    candidates.sort(key=lambda item: (item[0], item[1], item[2], str(item[3].get("signal_id") or item[3].get("event_id") or "")))
    delta_seconds, _audit_rank, time_key, live = candidates[0]
    return live, time_key, delta_seconds


def _llm_classification(live: dict | None, *, matched: bool) -> str:
    if not matched or live is None:
        return "NO_LIVE_MATCH_WITHIN_WINDOW"
    if _is_audit_only_row(live):
        return "LIVE_ORDER_EVIDENCE_BUT_RECONCILIATION_NEEDS_INVESTIGATION"
    status = str(live.get("status") or "").upper()
    if _live_was_placed(live):
        if status == "NOT_TRANSACTED" or live.get("needs_investigation"):
            return "LIVE_ORDER_EVIDENCE_BUT_RECONCILIATION_NEEDS_INVESTIGATION"
        return "LIVE_TRADE_PLACED"
    if status == "SKIPPED":
        return "LIVE_SIGNAL_SKIPPED"
    return "LIVE_SIGNAL_ONLY"


def _compact_backtest_for_llm(row: dict, *, local_tz: str = DEFAULT_LOCAL_TZ) -> dict:
    return {
        "pair": row.get("pair"),
        "direction": row.get("direction"),
        "status": row.get("status"),
        "entry_time": _to_local_iso(row.get("entry_time"), local_tz=local_tz),
        "exit_time": _to_local_iso(row.get("exit_time"), local_tz=local_tz),
        "entry_price": _float_or_none(row.get("entry_price")),
        "exit_price": _float_or_none(row.get("exit_price")),
        "sl_price": _float_or_none(row.get("sl_price")),
        "tp_price": _float_or_none(row.get("tp_price")),
        "pnl_pips": _float_or_none(row.get("pnl_pips")),
        "pnl_r": _float_or_none(row.get("pnl_r")),
        "pnl_amount": _float_or_none(row.get("pnl_amount")),
        "exit_reason": row.get("exit_reason"),
    }


def _compact_live_for_llm(row: dict | None, *, local_tz: str = DEFAULT_LOCAL_TZ) -> dict | None:
    if row is None:
        return None
    return {
        "signal_id": row.get("signal_id"),
        "pair": row.get("pair"),
        "direction": row.get("direction"),
        "status": row.get("status"),
        "broker_order_status": row.get("broker_order_status"),
        "order_id": row.get("order_id"),
        "signal_time": _to_local_iso(row.get("signal_time"), local_tz=local_tz),
        "detected_at": _to_local_iso(row.get("detected_at"), local_tz=local_tz),
        "opened_at": _to_local_iso(row.get("opened_at"), local_tz=local_tz),
        "executed_at": _to_local_iso(row.get("executed_at"), local_tz=local_tz),
        "closed_at": _to_local_iso(row.get("closed_at"), local_tz=local_tz),
        "entry_price": _float_or_none(row.get("entry_price")),
        "submitted_entry_price": _float_or_none(row.get("submitted_entry_price")),
        "opened_price": _float_or_none(row.get("opened_price")),
        "closed_price": _float_or_none(row.get("closed_price")),
        "planned_units": row.get("planned_units"),
        "open_units": row.get("open_units"),
        "remaining_units": row.get("remaining_units"),
        "fill_count": row.get("fill_count"),
        "pnl_pips": _float_or_none(row.get("pnl_pips")),
        "pnl_r": _float_or_none(row.get("pnl_r")),
        "pnl_gbp": _float_or_none(row.get("pnl_gbp")),
        "note": row.get("note"),
        "needs_investigation": bool(row.get("needs_investigation")),
        "investigation_reason": row.get("investigation_reason") or "",
        "order_audit_event_count": row.get("order_audit_event_count"),
        "event_id": row.get("event_id"),
        "event_ts": _to_local_iso(row.get("event_ts"), local_tz=local_tz),
    }


def _llm_note(
    backtest: dict,
    live: dict | None,
    nearest: dict | None,
    *,
    matched: bool,
    delta_seconds: int | None,
    nearest_delta: int | None,
) -> str:
    pair = backtest.get("pair")
    direction = backtest.get("direction")
    if matched and live is not None:
        if _is_audit_only_row(live):
            return (
                f"{pair} {direction}: entry-side audit evidence exists within window but no detected signal row "
                f"was reconciled; broker={live.get('broker_order_status')}; note={live.get('note') or ''}"
            )
        status = live.get("status")
        note = live.get("note") or ""
        delta_text = f"{delta_seconds}s" if delta_seconds is not None else "unknown delta"
        if _live_was_placed(live):
            return (
                f"{pair} {direction}: equivalent live row found within window ({delta_text}); "
                f"status={status}; broker={live.get('broker_order_status')}; note={note}"
            )
        return f"{pair} {direction}: live signal found within window ({delta_text}) but not placed; status={status}; note={note}"
    if nearest is not None:
        return (
            f"{pair} {direction}: no equivalent live row inside window; nearest same-pair/direction row "
            f"is {nearest_delta}s away with status={nearest.get('status')} note={nearest.get('note') or ''}"
        )
    return f"{pair} {direction}: no same-pair/direction live row found in the loaded live data."


def build_llm_parity_report_from_rows(
    *,
    backtest_trades: list[dict],
    live_signals: list[dict],
    selected_date: str,
    pair: str | None = None,
    window_seconds: int = 60,
    local_tz: str = DEFAULT_LOCAL_TZ,
    selected_backtest: dict | None = None,
    available_backtests: list[dict] | None = None,
    system_events: list[dict] | None = None,
    include_live_only: bool = True,
) -> dict:
    selected_pair = pair.upper() if pair else None
    scoped_live = [
        row for row in live_signals
        if not selected_pair or str(row.get("pair") or "").upper() == selected_pair
    ]
    # Split startup-replay rows out of the match pool. They represent
    # after-the-fact reconstructions by the walk-forward at restart, so
    # consuming a match slot with one of them would hide the downtime gap
    # the report is supposed to expose.
    live_rows = [row for row in scoped_live if not _is_startup_replay_row(row)]
    startup_replay_rows = [row for row in scoped_live if _is_startup_replay_row(row)]
    backtest_rows = [
        row for row in backtest_trades
        if not selected_pair or str(row.get("pair") or "").upper() == selected_pair
    ]

    consumed_live_ids: set[int] = set()
    trades: list[dict] = []
    for trade in sorted(backtest_rows, key=lambda item: (_to_utc_ts(item.get("entry_time")) or pd.Timestamp.max.tz_localize("UTC"), item.get("pair") or "")):
        match, match_time_key, delta_seconds = _best_live_match(
            trade,
            live_rows,
            window_seconds=max(1, int(window_seconds)),
            consumed_ids=consumed_live_ids,
        )
        nearest, nearest_time_key, nearest_delta = _nearest_live(trade, live_rows)
        matched = match is not None
        if match is not None:
            consumed_live_ids.add(id(match))
        reason = (
            _reason_for_unmatched_live_row(match)
            if matched and match is not None and _is_audit_only_row(match)
            else _reason_for_live_row(match)
            if matched and match is not None
            else _reason_for_missing_live(
                _to_utc_ts(trade.get("entry_time")) or pd.Timestamp.now(tz="UTC"),
                system_events or [],
                2.0,
            )
        )
        trades.append(
            {
                "classification": _llm_classification(match, matched=matched),
                "matched_within_window": matched,
                "match_time_key": match_time_key,
                "match_delta_seconds": delta_seconds if matched else None,
                "nearest_time_key": nearest_time_key,
                "nearest_delta_seconds": nearest_delta,
                "reason": reason,
                "backtest": _compact_backtest_for_llm(trade, local_tz=local_tz),
                "live_match": _compact_live_for_llm(match, local_tz=local_tz),
                "nearest_live": _compact_live_for_llm(nearest, local_tz=local_tz) if nearest is not match else None,
                "llm_note": _llm_note(
                    trade,
                    match,
                    nearest,
                    matched=matched,
                    delta_seconds=delta_seconds,
                    nearest_delta=nearest_delta,
                ),
            }
        )

    live_only: list[dict] = []
    audit_only_evidence: list[dict] = []
    if include_live_only:
        for row in live_rows:
            if id(row) in consumed_live_ids:
                continue
            item = {
                "classification": "AUDIT_ONLY_EVIDENCE" if _is_audit_only_row(row) else "LIVE_ONLY",
                "reason": _reason_for_unmatched_live_row(row),
                "live": _compact_live_for_llm(row, local_tz=local_tz),
                "llm_note": (
                    f"{row.get('pair')} {row.get('direction')}: "
                    + (
                        "entry-side audit evidence exists on "
                        if _is_audit_only_row(row)
                        else "live detection evidence exists on "
                    )
                    + f"{selected_date} but no backtest trade matched it within the configured window."
                ),
            }
            if _is_audit_only_row(row):
                audit_only_evidence.append(item)
            else:
                live_only.append(item)

    startup_replay_items: list[dict] = []
    for row in startup_replay_rows:
        startup_replay_items.append(
            {
                "classification": "STARTUP_REPLAY",
                "reason": "startup_replay_reconstruction",
                "live": _compact_live_for_llm(row, local_tz=local_tz),
                "llm_note": (
                    f"{row.get('pair')} {row.get('direction')}: "
                    "signal reconstructed by walk-forward at startup; not a "
                    "real-time detection — downtime likely hid the live signal."
                ),
            }
        )

    counts: dict[str, int] = {}
    for item in trades:
        counts[item["classification"]] = counts.get(item["classification"], 0) + 1
    if live_only:
        counts["LIVE_ONLY"] = len(live_only)
    if audit_only_evidence:
        counts["AUDIT_ONLY_EVIDENCE"] = len(audit_only_evidence)
    if startup_replay_items:
        counts["STARTUP_REPLAY"] = len(startup_replay_items)

    mismatch_reasons = Counter()
    for item in trades:
        if item["classification"] == "LIVE_TRADE_PLACED":
            continue
        mismatch_reasons[item.get("reason") or "unclassified"] += 1
    for item in live_only:
        mismatch_reasons[item.get("reason") or "live_only_signal"] += 1
    for item in audit_only_evidence:
        mismatch_reasons[item.get("reason") or "audit_only_signal"] += 1
    for item in startup_replay_items:
        mismatch_reasons[item.get("reason") or "startup_replay_reconstruction"] += 1

    return {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "selected_day": selected_date,
        "timezone": local_tz,
        "match_window_seconds": max(1, int(window_seconds)),
        "pair_filter": selected_pair,
        "selected_backtest": selected_backtest,
        "available_backtests": available_backtests or [],
        "backtest_pair_count": selected_backtest.get("pair_count") if selected_backtest else None,
        "backtest_trade_count": len(backtest_rows),
        "live_row_count": len(live_rows),
        "startup_replay_count": len(startup_replay_items),
        "classification_counts": counts,
        "mismatch_reasons": dict(mismatch_reasons),
        "system_events": system_events or [],
        "trades": trades,
        "live_only": live_only,
        "audit_only_evidence": audit_only_evidence,
        "startup_replay": startup_replay_items,
    }


def build_llm_parity_report(
    *,
    selected_date: date_cls | str | None = None,
    pair: str | None = None,
    window_seconds: int = 60,
    backtest_key: str | None = None,
    local_tz: str = DEFAULT_LOCAL_TZ,
    include_live_only: bool = True,
    db_path: str | None = None,
) -> dict:
    if pair:
        pair = pair.upper()
        if pair not in PAIRS:
            raise ValueError(f"Unknown pair: {pair}")

    date_label, _start_utc, _end_utc = _date_window_utc(selected_date, local_tz)
    backtest_trades, selected_backtest, available_backtests = load_latest_backtest_trades(
        selected_date=date_label,
        pair=pair,
        backtest_key=backtest_key,
        local_tz=local_tz,
        db_path=db_path,
    )
    live_signals, system_events = load_live_signals_for_window(
        selected_date=date_label,
        pair=pair,
        local_tz=local_tz,
        db_path=db_path,
    )
    return build_llm_parity_report_from_rows(
        backtest_trades=backtest_trades,
        live_signals=live_signals,
        selected_date=date_label,
        pair=pair,
        window_seconds=window_seconds,
        local_tz=local_tz,
        selected_backtest=selected_backtest,
        available_backtests=available_backtests,
        system_events=system_events,
        include_live_only=include_live_only,
    )


def format_parity_markdown(report: dict) -> str:
    if "trades" in report or "classification_counts" in report:
        selected = report.get("selected_backtest") or {}
        lines = [
            "# Backtest vs Live Parity Report",
            "",
            f"- Generated UTC: `{report.get('generated_at')}`",
            f"- Trading day: `{report.get('selected_day')}` ({report.get('timezone')})",
            f"- Match window: `{report.get('match_window_seconds')}s`",
            f"- Backtest key: `{selected.get('key') or ''}`",
            f"- Pair filter: `{report.get('pair_filter') or 'ALL'}`",
            f"- Backtest trades: `{report.get('backtest_trade_count')}`",
            f"- Live rows considered: `{report.get('live_row_count')}`",
            "",
            "## Classification counts",
            "",
        ]
        counts = report.get("classification_counts") or {}
        if counts:
            for key in sorted(counts):
                lines.append(f"- `{key}`: {counts[key]}")
        else:
            lines.append("- none")
        mismatch_reasons = report.get("mismatch_reasons") or {}
        lines.extend(["", "## Mismatch reasons", ""])
        if mismatch_reasons:
            for key in sorted(mismatch_reasons):
                lines.append(f"- `{key}`: {mismatch_reasons[key]}")
        else:
            lines.append("- none")

        lines.extend(
            [
                "",
                "## Backtest trades and live match",
                "",
                "| Time | Pair | Dir | BT Pips | BT R | Live? | Classification | Reason | Live status | Delta | Note |",
                "|---|---:|---:|---:|---:|---:|---|---|---|---:|---|",
            ]
        )
        for item in report.get("trades") or []:
            bt = item.get("backtest") or {}
            live = item.get("live_match") or {}
            time_text = str(bt.get("entry_time") or "")[:19]
            live_text = "yes" if item.get("matched_within_window") else "no"
            delta = item.get("match_delta_seconds")
            delta_text = f"{delta}s" if delta is not None else (
                f"nearest {item.get('nearest_delta_seconds')}s"
                if item.get("nearest_delta_seconds") is not None
                else ""
            )
            note = (item.get("llm_note") or "").replace("|", "/")
            lines.append(
                f"| {time_text} | {bt.get('pair') or ''} | {bt.get('direction') or ''} | "
                f"{bt.get('pnl_pips') if bt.get('pnl_pips') is not None else ''} | "
                f"{bt.get('pnl_r') if bt.get('pnl_r') is not None else ''} | "
                f"{live_text} | {item.get('classification')} | {item.get('reason') or ''} | {live.get('status') or ''} | "
                f"{delta_text} | {note} |"
            )

        live_only = report.get("live_only") or []
        if live_only:
            lines.extend(
                [
                    "",
                    "## Live-only order evidence",
                    "",
                    "| Time | Pair | Dir | Status | Broker | Order | Note |",
                    "|---|---:|---:|---|---|---|---|",
                ]
            )
            for item in live_only:
                live = item.get("live") or {}
                time_text = str(live.get("opened_at") or live.get("signal_time") or live.get("event_ts") or "")[:19]
                note = (item.get("llm_note") or live.get("note") or "").replace("|", "/")
                lines.append(
                    f"| {time_text} | {live.get('pair') or ''} | {live.get('direction') or ''} | "
                    f"{live.get('status') or ''} | {live.get('broker_order_status') or ''} | "
                    f"{live.get('order_id') or ''} | {note} |"
                )

        audit_only = report.get("audit_only_evidence") or []
        if audit_only:
            lines.extend(
                [
                    "",
                    "## Audit-only entry evidence",
                    "",
                    "| Time | Pair | Dir | Status | Broker | Order | Reason | Note |",
                    "|---|---:|---:|---|---|---|---|---|",
                ]
            )
            for item in audit_only:
                live = item.get("live") or {}
                time_text = str(live.get("signal_time") or live.get("event_ts") or "")[:19]
                note = (item.get("llm_note") or live.get("note") or "").replace("|", "/")
                lines.append(
                    f"| {time_text} | {live.get('pair') or ''} | {live.get('direction') or ''} | "
                    f"{live.get('status') or ''} | {live.get('broker_order_status') or ''} | "
                    f"{live.get('order_id') or ''} | {item.get('reason') or ''} | {note} |"
                )

        startup_replay = report.get("startup_replay") or []
        if startup_replay:
            lines.extend(
                [
                    "",
                    "## Startup-replay reconstructions",
                    "",
                    "These live rows were rebuilt by the walk-forward when the",
                    "process started; they are excluded from match counts and",
                    "highlight periods where real-time detection was offline.",
                    "",
                    "| Time | Pair | Dir | Status | Note |",
                    "|---|---:|---:|---|---|",
                ]
            )
            for item in startup_replay:
                live = item.get("live") or {}
                time_text = str(live.get("signal_time") or live.get("detected_at") or "")[:19]
                note = (item.get("llm_note") or live.get("note") or "").replace("|", "/")
                lines.append(
                    f"| {time_text} | {live.get('pair') or ''} | {live.get('direction') or ''} | "
                    f"{live.get('status') or ''} | {note} |"
                )

        lines.extend(
            [
                "",
                "## Full JSON",
                "",
                "```json",
                json.dumps(report, indent=2, sort_keys=True, default=str),
                "```",
            ]
        )
        return "\n".join(lines)

    summary = report.get("summary", {})
    lines = [
        "# Live vs Backtest Trade Parity",
        "",
        f"- date: {summary.get('date')}",
        f"- pair: {summary.get('pair') or 'ALL'}",
        f"- tolerance_minutes: {summary.get('tolerance_minutes')}",
        f"- backtest_signals: {summary.get('backtest_signals')}",
        f"- live_signals: {summary.get('live_signals')}",
        f"- live_trades_placed: {summary.get('live_trades_placed')}",
        f"- status_counts: `{json.dumps(summary.get('status_counts', {}), sort_keys=True)}`",
        f"- mismatch_reasons: `{json.dumps(summary.get('mismatch_reasons', {}), sort_keys=True)}`",
        "",
    ]

    selected = report.get("selected_backtest")
    if selected:
        lines.extend(
            [
                "## Backtest Cache",
                "",
                f"- key: `{selected.get('key')}`",
                f"- updated_at: {selected.get('updated_at')}",
                f"- profile: {selected.get('profile') or selected.get('selection_label') or 'unknown'}",
                f"- pairs: {selected.get('pair_count')}",
                "",
            ]
        )

    lines.extend(
        [
            "## Rows",
            "",
            "| pair | dir | backtest_time | live_signal_time | execution_time | placed | status | reason | note |",
            "|---|---:|---|---|---|---:|---|---|---|",
        ]
    )
    for row in report.get("rows", []):
        note = str(row.get("rejection_note") or "").replace("|", "\\|")
        if len(note) > 90:
            note = note[:87] + "..."
        lines.append(
            "| {pair} | {direction} | {backtest_time} | {live_signal_time} | {execution_time} | {placed} | {status} | {reason} | {note} |".format(
                pair=row.get("pair") or "",
                direction=row.get("direction") or "",
                backtest_time=row.get("backtest_time") or "",
                live_signal_time=row.get("live_signal_time") or "",
                execution_time=row.get("execution_time") or "",
                placed="yes" if row.get("live_trade_placed") else "no",
                status=row.get("status") or "",
                reason=row.get("reason") or "",
                note=note,
            )
        )

    lines.extend(
        [
            "",
            "## JSON Payload",
            "",
            "```json",
            json.dumps(report, indent=2, sort_keys=True),
            "```",
        ]
    )
    return "\n".join(lines)


def _table_cell(value: Any, width: int) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\r", " ").replace("\n", " ")
    if len(text) > width:
        text = text[: max(0, width - 1)] + "…"
    return text.ljust(width)


def _short_time(value: Any) -> str:
    if not value:
        return ""
    text = str(value)
    if "T" in text:
        return text.split("T", 1)[1][:5]
    return text[:5]


def _short_number(value: Any, digits: int = 1) -> str:
    if value in (None, ""):
        return ""
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def format_parity_table(report: dict) -> str:
    """Return a capital-style human-readable parity list with no JSON appendix."""

    selected = report.get("selected_backtest") or {}
    counts = report.get("classification_counts") or {}
    mismatch_reasons = report.get("mismatch_reasons") or {}
    trades = report.get("trades") or []
    live_only = report.get("live_only") or []
    audit_only = report.get("audit_only_evidence") or []
    lines = [
        "Backtest vs Live Parity",
        "=" * 24,
        f"Day:          {report.get('selected_day')} ({report.get('timezone')})",
        f"Backtest:     {selected.get('profile') or selected.get('selection_label') or selected.get('key') or 'none'}",
        f"Pair filter:  {report.get('pair_filter') or 'ALL'}",
        f"Window:       {report.get('match_window_seconds')}s",
        f"BT trades:    {report.get('backtest_trade_count')}",
        f"Live rows:    {report.get('live_row_count')}",
        "",
        "Summary",
        "-------",
    ]
    if counts:
        for key in sorted(counts):
            lines.append(f"  {key:<58} {counts[key]}")
    else:
        lines.append("  No trades found for this day.")
    lines.extend(["", "Reasons", "-------"])
    if mismatch_reasons:
        for key in sorted(mismatch_reasons):
            lines.append(f"  {key:<58} {mismatch_reasons[key]}")
    else:
        lines.append("  No mismatches found.")

    lines.extend(["", "Backtest trades", "---------------"])

    if not trades:
        lines.append("  No backtest trades found.")
    for idx, item in enumerate(trades, start=1):
        bt = item.get("backtest") or {}
        live = item.get("live_match") or {}
        nearest = item.get("nearest_live") or {}
        time_text = str(bt.get("entry_time") or "")[:19]
        pnl_text = ""
        if bt.get("pnl_pips") is not None or bt.get("pnl_r") is not None:
            pnl_text = f" | BT P/L: {bt.get('pnl_pips') if bt.get('pnl_pips') is not None else ''} pips"
            if bt.get("pnl_r") is not None:
                pnl_text += f", {bt.get('pnl_r')}R"

        lines.append(
            f"{idx:>2}. {time_text}  {bt.get('pair') or ''} {bt.get('direction') or ''}"
            f"{pnl_text}"
        )
        lines.append(f"    Classification: {item.get('classification')}")
        if item.get("reason"):
            lines.append(f"    Reason:         {item.get('reason')}")
        if item.get("matched_within_window"):
            delta = item.get("match_delta_seconds")
            lines.append(
                f"    Live match:     {live.get('status') or ''} "
                f"via {item.get('match_time_key') or 'time'}"
                f"{f' ({delta}s)' if delta is not None else ''}"
            )
            if live.get("broker_order_status") or live.get("order_id"):
                lines.append(
                    f"    Broker/order:   {live.get('broker_order_status') or ''} "
                    f"{live.get('order_id') or ''}".rstrip()
                )
            price_bits = []
            for label, key in (
                ("entry", "entry_price"),
                ("opened", "opened_price"),
                ("closed", "closed_price"),
            ):
                if live.get(key) is not None:
                    price_bits.append(f"{label}={live.get(key)}")
            if price_bits:
                lines.append(f"    Live prices:    {', '.join(price_bits)}")
            live_pnl = []
            for label, key in (("pips", "pnl_pips"), ("R", "pnl_r"), ("GBP", "pnl_gbp")):
                if live.get(key) is not None:
                    live_pnl.append(f"{label}={live.get(key)}")
            if live_pnl:
                lines.append(f"    Live P/L:       {', '.join(live_pnl)}")
            if live.get("note"):
                lines.append(f"    Note:           {live.get('note')}")
        else:
            nearest_delta = item.get("nearest_delta_seconds")
            if nearest:
                lines.append(
                    f"    No match:       nearest {nearest.get('status') or ''} "
                    f"same pair/direction row is {nearest_delta}s away"
                )
                if nearest.get("note"):
                    lines.append(f"    Nearest note:   {nearest.get('note')}")
            else:
                lines.append("    No match:       no same pair/direction live row found")
        lines.append(f"    LLM note:       {item.get('llm_note') or ''}")
        lines.append("")

    if live_only:
        lines.extend(["Live-only detected trades", "------------------------"])
        for idx, item in enumerate(live_only, start=1):
            live = item.get("live") or {}
            lines.append(
                f"{idx:>2}. {str(live.get('opened_at') or live.get('signal_time') or live.get('event_ts') or '')[:19]}  "
                f"{live.get('pair') or ''} {live.get('direction') or ''} {live.get('status') or ''}"
            )
            if live.get("broker_order_status") or live.get("order_id"):
                lines.append(
                    f"    Broker/order: {live.get('broker_order_status') or ''} "
                    f"{live.get('order_id') or ''}".rstrip()
                )
            if live.get("note"):
                lines.append(f"    Note:         {live.get('note')}")
            if item.get("reason"):
                lines.append(f"    Reason:       {item.get('reason')}")
            lines.append(f"    LLM note:     {item.get('llm_note') or ''}")
            lines.append("")

    if audit_only:
        lines.extend(["Audit-only entry evidence", "-------------------------"])
        for idx, item in enumerate(audit_only, start=1):
            live = item.get("live") or {}
            lines.append(
                f"{idx:>2}. {str(live.get('signal_time') or live.get('event_ts') or '')[:19]}  "
                f"{live.get('pair') or ''} {live.get('direction') or ''} {live.get('status') or ''}"
            )
            if live.get("broker_order_status") or live.get("order_id"):
                lines.append(
                    f"    Broker/order: {live.get('broker_order_status') or ''} "
                    f"{live.get('order_id') or ''}".rstrip()
                )
            if live.get("note"):
                lines.append(f"    Note:         {live.get('note')}")
            if item.get("reason"):
                lines.append(f"    Reason:       {item.get('reason')}")
            lines.append(f"    LLM note:     {item.get('llm_note') or ''}")
            lines.append("")

    startup_replay = report.get("startup_replay") or []
    if startup_replay:
        lines.extend([
            "Startup-replay reconstructions",
            "------------------------------",
            "(Excluded from match counts; indicate gaps where live was offline.)",
        ])
        for idx, item in enumerate(startup_replay, start=1):
            live = item.get("live") or {}
            lines.append(
                f"{idx:>2}. {str(live.get('signal_time') or live.get('detected_at') or '')[:19]}  "
                f"{live.get('pair') or ''} {live.get('direction') or ''} {live.get('status') or ''}"
            )
            if live.get("note"):
                lines.append(f"    Note:         {live.get('note')}")
            lines.append(f"    LLM note:     {item.get('llm_note') or ''}")
            lines.append("")

    return "\n".join(lines).rstrip()


def write_report_output(report: dict, *, output: str | None, output_format: str) -> None:
    if not output:
        return
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    text = (
        json.dumps(report, indent=2, sort_keys=True)
        if output_format == "json"
        else format_parity_markdown(report)
        if output_format == "markdown"
        else format_parity_table(report)
    )
    path.write_text(text + "\n", encoding="utf-8")


async def handle_backtest_vs_live_api(request) -> Any:
    from aiohttp import web

    raw_date = (request.query.get("date", "") or "").strip() or None
    pair = (request.query.get("pair", "") or "").strip().upper() or None
    raw_window_seconds = request.query.get("window_seconds")
    raw_tolerance = request.query.get("tolerance_minutes", "1")
    backtest_key = (request.query.get("backtest", "") or "").strip() or None
    include_live_only = request.query.get("include_live_only", "1").lower() not in {"0", "false", "no"}

    try:
        report = build_llm_parity_report(
            selected_date=raw_date,
            pair=pair,
            window_seconds=(
                int(raw_window_seconds)
                if raw_window_seconds not in (None, "")
                else int(float(raw_tolerance) * 60)
            ),
            backtest_key=backtest_key,
            include_live_only=include_live_only,
        )
    except ValueError as exc:
        return web.json_response({"error": str(exc)}, status=400)
    return web.json_response(report)
