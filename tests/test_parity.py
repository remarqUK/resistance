import unittest

from fx_sr.parity import (
    build_llm_parity_report_from_rows,
    build_parity_report_from_rows,
    _date_window_utc,
)


def _bt(pair="EURUSD", direction="LONG", entry_time="2026-04-23T09:00:00Z"):
    return {
        "pair": pair,
        "direction": direction,
        "entry_time": entry_time,
        "entry_price": 1.1,
        "sl_price": 1.095,
        "tp_price": 1.11,
    }


def _live(
    signal_id="sig-1",
    pair="EURUSD",
    direction="LONG",
    signal_time="2026-04-23T09:00:30Z",
    status="SUBMITTED",
    transacted=1,
    note=None,
    executed_at="2026-04-23T09:00:35Z",
):
    return {
        "signal_id": signal_id,
        "pair": pair,
        "direction": direction,
        "signal_time": signal_time,
        "detected_at": signal_time,
        "status": status,
        "transacted": transacted,
        "note": note,
        "executed_at": executed_at,
    }


class ParityReportTests(unittest.TestCase):
    def test_exact_and_within_tolerance_match_as_executed(self):
        report = build_parity_report_from_rows(
            backtest_trades=[_bt()],
            live_signals=[_live()],
            selected_date="2026-04-23",
            tolerance_minutes=1,
            include_live_only=False,
        )

        self.assertEqual(report["summary"]["status_counts"], {"matched_executed": 1})
        row = report["rows"][0]
        self.assertEqual(row["status"], "matched_executed")
        self.assertTrue(row["live_trade_placed"])
        self.assertEqual(row["source_signal_id"], "sig-1")

    def test_duplicate_live_signal_is_consumed_once(self):
        report = build_parity_report_from_rows(
            backtest_trades=[
                _bt(entry_time="2026-04-23T09:00:00Z"),
                _bt(entry_time="2026-04-23T09:00:00Z"),
            ],
            live_signals=[
                _live(signal_id="sig-1", signal_time="2026-04-23T09:00:00Z"),
                _live(signal_id="sig-2", signal_time="2026-04-23T09:00:40Z"),
            ],
            selected_date="2026-04-23",
            tolerance_minutes=1,
            include_live_only=False,
        )

        self.assertEqual(report["summary"]["status_counts"], {"matched_executed": 2})
        self.assertEqual(
            [row["source_signal_id"] for row in report["rows"]],
            ["sig-1", "sig-2"],
        )

    def test_matched_not_executed_reports_execution_reason(self):
        report = build_parity_report_from_rows(
            backtest_trades=[_bt()],
            live_signals=[
                _live(
                    status="SKIPPED",
                    transacted=0,
                    note="entry drift too large",
                    executed_at=None,
                )
            ],
            selected_date="2026-04-23",
            tolerance_minutes=1,
            include_live_only=False,
        )

        row = report["rows"][0]
        self.assertEqual(row["status"], "matched_not_executed")
        self.assertEqual(row["reason"], "entry_drift_too_large")
        self.assertEqual(report["summary"]["mismatch_reasons"], {"entry_drift_too_large": 1})

    def test_missing_live_signal_after_startup_window_is_classified(self):
        report = build_parity_report_from_rows(
            backtest_trades=[_bt(entry_time="2026-04-23T07:00:00Z")],
            live_signals=[],
            selected_date="2026-04-23",
            tolerance_minutes=1,
            max_age_hours=2,
            system_events=[
                {
                    "event_time": "2026-04-23T10:00:00Z",
                    "event_type": "startup",
                    "detail": "profile=test",
                }
            ],
            include_live_only=False,
        )

        row = report["rows"][0]
        self.assertEqual(row["status"], "no_live_signal")
        self.assertEqual(row["reason"], "stale_replay_window")

    def test_later_restart_does_not_make_in_window_miss_stale(self):
        report = build_parity_report_from_rows(
            backtest_trades=[_bt(entry_time="2026-04-23T09:44:00Z")],
            live_signals=[],
            selected_date="2026-04-23",
            tolerance_minutes=1,
            max_age_hours=2,
            system_events=[
                {"event_time": "2026-04-23T10:03:00Z", "event_type": "startup"},
                {"event_time": "2026-04-23T12:05:00Z", "event_type": "startup"},
            ],
            include_live_only=False,
        )

        self.assertEqual(report["rows"][0]["reason"], "not_detected")

    def test_live_only_rows_close_audit_loop(self):
        report = build_parity_report_from_rows(
            backtest_trades=[],
            live_signals=[_live(signal_id="live-only")],
            selected_date="2026-04-23",
        )

        self.assertEqual(report["summary"]["status_counts"], {"live_only": 1})
        self.assertEqual(report["rows"][0]["source_signal_id"], "live-only")

    def test_audit_only_rows_are_reported_separately(self):
        report = build_parity_report_from_rows(
            backtest_trades=[],
            live_signals=[
                {
                    "signal_id": None,
                    "pair": "EURUSD",
                    "direction": "LONG",
                    "status": "AUDIT_ONLY",
                    "broker_order_status": "resubmit",
                    "order_id": "201",
                    "signal_time": "2026-04-23T12:05:24+01:00",
                    "opened_at": None,
                    "open_units": None,
                    "note": "Order audit row (no matching detected signal)",
                    "event_id": 252,
                    "event_ts": "2026-04-23T13:18:30+01:00",
                    "_audit_only": True,
                    "_audit_execution_evidence": False,
                }
            ],
            selected_date="2026-04-23",
        )

        self.assertEqual(report["summary"]["status_counts"], {"audit_only_evidence": 1})
        self.assertEqual(report["summary"]["audit_only_evidence"], 1)
        self.assertEqual(report["summary"]["live_only"], 0)
        self.assertEqual(report["rows"][0]["status"], "audit_only_evidence")
        self.assertFalse(report["rows"][0]["live_trade_placed"])

    def test_london_date_window_normalizes_to_utc(self):
        label, start, end = _date_window_utc("2026-04-23", "Europe/London")

        self.assertEqual(label, "2026-04-23")
        self.assertEqual(start.isoformat(), "2026-04-22T23:00:00+00:00")
        self.assertEqual(end.isoformat(), "2026-04-23T23:00:00+00:00")

    def test_llm_report_uses_classification_contract(self):
        report = build_llm_parity_report_from_rows(
            backtest_trades=[_bt()],
            live_signals=[_live()],
            selected_date="2026-04-23",
            window_seconds=60,
            local_tz="Europe/London",
            include_live_only=False,
        )

        self.assertEqual(report["classification_counts"], {"LIVE_TRADE_PLACED": 1})
        item = report["trades"][0]
        self.assertEqual(item["classification"], "LIVE_TRADE_PLACED")
        self.assertEqual(item["backtest"]["entry_time"], "2026-04-23T10:00:00+01:00")
        self.assertEqual(item["live_match"]["signal_id"], "sig-1")
        self.assertIsNone(item["reason"])
        self.assertIn("equivalent live row found", item["llm_note"])

    def test_llm_report_keeps_real_live_only_trade(self):
        report = build_llm_parity_report_from_rows(
            backtest_trades=[],
            live_signals=[_live(signal_id="live-only", status="OPEN")],
            selected_date="2026-04-23",
            window_seconds=60,
            local_tz="Europe/London",
        )

        self.assertEqual(report["classification_counts"], {"LIVE_ONLY": 1})
        self.assertEqual(len(report["live_only"]), 1)
        self.assertEqual(report["live_only"][0]["classification"], "LIVE_ONLY")
        self.assertEqual(report["live_only"][0]["reason"], "live_only_signal")
        self.assertEqual(report["audit_only_evidence"], [])

    def test_llm_report_keeps_unplaced_live_only_row(self):
        report = build_llm_parity_report_from_rows(
            backtest_trades=[],
            live_signals=[
                _live(
                    signal_id="skipped-live",
                    status="SKIPPED",
                    transacted=0,
                    executed_at=None,
                    note="risk filter skipped order",
                )
            ],
            selected_date="2026-04-23",
            window_seconds=60,
            local_tz="Europe/London",
        )

        self.assertEqual(report["classification_counts"], {"LIVE_ONLY": 1})
        self.assertEqual(len(report["live_only"]), 1)
        item = report["live_only"][0]
        self.assertEqual(item["classification"], "LIVE_ONLY")
        self.assertEqual(item["live"]["signal_id"], "skipped-live")
        self.assertEqual(item["live"]["status"], "SKIPPED")
        self.assertIn("live detection evidence exists", item["llm_note"])

    def test_llm_report_emits_audit_only_evidence_separately(self):
        report = build_llm_parity_report_from_rows(
            backtest_trades=[],
            live_signals=[
                {
                    "signal_id": None,
                    "pair": "EURUSD",
                    "direction": "LONG",
                    "status": "AUDIT_ONLY",
                    "broker_order_status": "resubmit",
                    "order_id": "201",
                    "signal_time": "2026-04-23T12:05:24+01:00",
                    "opened_at": "2026-04-23T12:05:24+01:00",
                    "open_units": 10000,
                    "note": "Order audit row (no matching detected signal)",
                    "event_id": 252,
                    "event_ts": "2026-04-23T13:18:30+01:00",
                }
            ],
            selected_date="2026-04-23",
            window_seconds=60,
            local_tz="Europe/London",
        )

        self.assertEqual(report["classification_counts"], {"AUDIT_ONLY_EVIDENCE": 1})
        self.assertEqual(report["live_only"], [])
        item = report["audit_only_evidence"][0]
        self.assertEqual(item["classification"], "AUDIT_ONLY_EVIDENCE")
        self.assertEqual(item["live"]["status"], "AUDIT_ONLY")
        self.assertEqual(item["live"]["order_id"], "201")
        self.assertEqual(item["reason"], "audit_only_signal")
        self.assertIn("entry-side audit evidence exists", item["llm_note"])

    def test_drifting_fill_time_does_not_bind_later_backtest_trade(self):
        # Two same-pair/direction backtest trades one hour apart. The only
        # live row is a SUBMITTED signal at 09:00 whose fill drifted to
        # 10:01:00 (a delayed execution). Matching must use detection time
        # — otherwise the fill-time drift would bind this live row to the
        # *later* 10:00 backtest trade and hide the detection miss.
        report = build_parity_report_from_rows(
            backtest_trades=[
                _bt(entry_time="2026-04-23T09:00:00Z"),
                _bt(entry_time="2026-04-23T10:00:00Z"),
            ],
            live_signals=[
                _live(
                    signal_id="sig-09",
                    signal_time="2026-04-23T09:00:30Z",
                    executed_at="2026-04-23T10:01:00Z",
                ),
            ],
            selected_date="2026-04-23",
            tolerance_minutes=1,
            include_live_only=False,
        )

        statuses = {row.get("backtest_time"): row.get("status") for row in report["rows"]}
        self.assertEqual(statuses["2026-04-23T09:00:00+00:00"], "matched_executed")
        self.assertEqual(statuses["2026-04-23T10:00:00+00:00"], "no_live_signal")

    def test_startup_replay_rows_do_not_satisfy_backtest_matches(self):
        # A replayed signal reconstructed by the walk-forward at startup
        # must never consume a backtest match slot; otherwise an outage is
        # reported as a successful live detection and the downtime gap
        # disappears from the report.
        replay_row = _live(
            signal_id="replay-sig",
            signal_time="2026-04-23T09:00:30Z",
            executed_at=None,
        )
        replay_row["quote_source"] = "startup_replay"

        report = build_parity_report_from_rows(
            backtest_trades=[_bt(entry_time="2026-04-23T09:00:00Z")],
            live_signals=[replay_row],
            selected_date="2026-04-23",
            tolerance_minutes=1,
            include_live_only=False,
        )

        row = report["rows"][0]
        self.assertEqual(row["status"], "no_live_signal")
        self.assertEqual(report["summary"]["startup_replay"], 1)
        self.assertEqual(report["startup_replay"][0]["pair"], "EURUSD")

    def test_llm_startup_replay_rows_are_surfaced_not_matched(self):
        replay_row = _live(
            signal_id="replay-sig",
            signal_time="2026-04-23T09:00:00Z",
            executed_at=None,
        )
        replay_row["quote_source"] = "startup_replay"

        report = build_llm_parity_report_from_rows(
            backtest_trades=[_bt(entry_time="2026-04-23T09:00:00Z")],
            live_signals=[replay_row],
            selected_date="2026-04-23",
            window_seconds=60,
            local_tz="UTC",
        )

        self.assertEqual(
            report["classification_counts"],
            {"NO_LIVE_MATCH_WITHIN_WINDOW": 1, "STARTUP_REPLAY": 1},
        )
        self.assertEqual(report["startup_replay_count"], 1)
        self.assertEqual(len(report["startup_replay"]), 1)
        self.assertEqual(
            report["startup_replay"][0]["classification"], "STARTUP_REPLAY"
        )
        self.assertEqual(report["trades"][0]["classification"], "NO_LIVE_MATCH_WITHIN_WINDOW")

    def test_llm_report_carries_missing_live_reason(self):
        report = build_llm_parity_report_from_rows(
            backtest_trades=[_bt(entry_time="2026-04-23T07:00:00Z")],
            live_signals=[],
            selected_date="2026-04-23",
            window_seconds=60,
            local_tz="Europe/London",
            system_events=[
                {
                    "event_time": "2026-04-23T10:00:00Z",
                    "event_type": "startup",
                }
            ],
            include_live_only=False,
        )

        self.assertEqual(report["classification_counts"], {"NO_LIVE_MATCH_WITHIN_WINDOW": 1})
        self.assertEqual(report["mismatch_reasons"], {"stale_replay_window": 1})
        self.assertEqual(report["trades"][0]["reason"], "stale_replay_window")


if __name__ == "__main__":
    unittest.main()
