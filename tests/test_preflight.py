import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from preflight import acceptance_criteria, final_telemetry_quality, _observe_feed
from shadow_cohort import ShadowStore, cohort_report, record_shadow, resolve_pending
from shadow_runtime import ShadowRuntime
from telemetry import BinanceDepthFeed, BinanceQuote, QuoteUnavailable, TelemetryConfig, TelemetryStore, Collector
from test_shadow_cohort import definition, snapshot
from test_shadow_runtime import inputs
from research_validation import day_cluster_sensitivity


class QualityRegressionTests(unittest.TestCase):
    def test_cluster_check_resamples_days_not_individual_rows(self):
        groups = {str(day): [value] * 8 for day, value in enumerate([-1, -1, 1, 1, 1])}
        groups["4"].extend([1, 1])
        result = day_cluster_sensitivity(groups)
        self.assertEqual(result["day_blocks"], 5)
        self.assertEqual(result["episode_representatives"], 42)
        self.assertLess(result["interval"]["lower"], 0)
        self.assertEqual(result, day_cluster_sensitivity(groups))
        self.assertEqual(day_cluster_sensitivity({"one": [1] * 42})["status"], "INSUFFICIENT_DAY_BLOCKS")

    def test_negative_lag_is_not_fresh_in_saved_report(self):
        from telemetry import _source_report
        rows = [{"binance_source_ts": 100, "binance_source_ts_kind": "payload_E_ms", "binance_source_lag_ms": lag}
                for lag in (-1, 0, 750, 751)]
        self.assertEqual(_source_report(rows, "binance")["payload_timestamp_within_750ms_pct"], 50)

    def test_acceptance_rejects_one_quote_then_outage_and_short_smoke(self):
        good = dict(duration_seconds=1800, valid_attempt_fraction=.99,
                    max_without_valid_quote_seconds=5, distinct_update_ids=300,
                    update_ids_monotonic=True, reconnect_requested=True, reconnect_recovered=True)
        self.assertTrue(all(acceptance_criteria(good).values()))
        for update in [dict(valid_attempt_fraction=.001, max_without_valid_quote_seconds=1799),
                       dict(duration_seconds=60), dict(reconnect_recovered=False),
                       dict(distinct_update_ids=1), dict(update_ids_monotonic=False)]:
            self.assertFalse(all(acceptance_criteria(dict(good, **update)).values()))

    def test_future_and_uncertain_quotes_fail_closed(self):
        feed = BinanceDepthFeed(TelemetryConfig("unused"), clock=lambda: 10)
        for ts, uncertainty, expected in [(11, 0, "ws_future_quote"),
                                          (9.3, 100, "ws_stale_quote"),
                                          (10, float("nan"), "ws_clock_uncertain")]:
            feed._publish(BinanceQuote(100, ts, "payload_E_ms", 10, 0, 1, 0, uncertainty))
            with self.assertRaises(QuoteUnavailable) as raised:
                feed.latest_quote()
            self.assertEqual(raised.exception.classification, expected)

    def test_observer_counts_failure_tail_and_stops_on_monotonic_clock(self):
        class Clock:
            value = 0.0
            def now(self): return self.value
            def sleep(self, seconds): self.value += seconds
        clock = Clock()
        class Feed:
            config = TelemetryConfig("unused")
            def start(self): pass
            def stop(self): pass
            def drain_diagnostics(self): return []
            def drain_gaps(self): return []
            def latest_quote(self, now):
                if clock.value > .1: raise QuoteUnavailable("ws_disconnect", "test")
                return BinanceQuote(100, now, "payload_E_ms", now, 0, 1, 0, 0)
        with patch("preflight.time.monotonic", clock.now), patch("preflight.time.time", return_value=100), patch("preflight.time.sleep", clock.sleep):
            result = _observe_feed(Feed(), 2, sample_seconds=.25)
        self.assertEqual(result["sample_attempts"], 8)
        self.assertEqual(result["quote_samples"], 1)
        self.assertEqual(result["max_without_valid_quote_seconds"], 2)
        self.assertEqual(result["unavailable"], {"ws_disconnect": 7})

    def test_observer_requires_new_synchronized_quote_after_injected_disconnect(self):
        elapsed = [0.0]
        class Feed:
            config = TelemetryConfig("unused")
            _lock = threading.Lock()
            _socket = None
            disconnected = False
            generation = 1
            _connection_generation = 1
            def start(self): self._socket = self
            def stop(self): pass
            def close(self): pass
            def _mark_disconnected(self, reason): self.disconnected = True
            def drain_diagnostics(self): return []
            def drain_gaps(self): return []
            def latest_quote(self, now):
                if self.disconnected:
                    self.disconnected = False; self.generation += 1
                    self._connection_generation += 1
                    raise QuoteUnavailable("ws_disconnect", "injected")
                return BinanceQuote(100, now, "payload_E_ms", now, 0, self.generation, 0, 0)
        def sleep(seconds): elapsed[0] += seconds
        with patch("preflight.time.monotonic", lambda: elapsed[0]), patch("preflight.time.time", lambda: elapsed[0]), patch("preflight.time.sleep", sleep):
            result = _observe_feed(Feed(), 2, sample_seconds=.25, exercise_reconnect=True)
        self.assertTrue(result["reconnect_requested"])
        self.assertTrue(result["reconnect_recovered"])
        self.assertEqual(result["unavailable"], {"ws_disconnect": 1})
        self.assertEqual(result["distinct_update_ids"], 2)

    def test_as_of_hides_future_candidates_and_outcomes(self):
        store = ShadowStore(Path(":memory:"))
        self.addCleanup(store.close)
        d = definition(); store.register_cohort(d, created_ts=100)
        record_shadow(store, d, snapshot(100.2))
        record_shadow(store, d, snapshot(3700.2, start=3600, market_id="future"))
        resolve_pending(store, d, lambda *_: "DOWN", now_ts=5000)
        before = cohort_report(store, d, as_of_ts=200)
        self.assertEqual(before["observations"], 1)
        self.assertEqual(before["resolved_candidate_rows"], 0)
        self.assertIsNone(before["primary_net_pnl"]["representative_bootstrap"])
        self.assertEqual(cohort_report(store, d, as_of_ts=5000)["resolved_candidate_rows"], 2)

    def test_as_of_preserves_retry_exhaustion_after_requeue(self):
        store = ShadowStore(Path(":memory:")); self.addCleanup(store.close)
        d = definition(); store.register_cohort(d, created_ts=100)
        record_shadow(store, d, snapshot(100.2))
        def offline(*_): raise TimeoutError("offline")
        resolve_pending(store, d, offline, now_ts=1000, max_fetch_failures=1)
        store.requeue_retry_exhausted(d.cohort_name, now_ts=1100)
        resolve_pending(store, d, lambda *_: "DOWN", now_ts=1200)
        self.assertEqual(cohort_report(store, d, as_of_ts=1050)["resolution_states"], {"RETRY_EXHAUSTED": 1})
        self.assertEqual(cohort_report(store, d, as_of_ts=1150)["resolution_states"], {"PENDING": 1})

    def test_all_failed_heartbeats_do_not_prove_coverage(self):
        store = ShadowStore(Path(":memory:")); self.addCleanup(store.close)
        d = definition(); store.register_cohort(d, created_ts=100)
        for ts in range(100, 1001, 100):
            store.record_heartbeat(d, ts=ts, complete=False, reason="offline")
        result = cohort_report(store, d, as_of_ts=1000)
        self.assertFalse(result["coverage"]["valid"])
        self.assertEqual(result["coverage"]["max_complete_snapshot_gap_seconds"], 900)

    def test_invalid_input_retains_raw_evidence_without_outcome(self):
        store = ShadowStore(Path(":memory:")); self.addCleanup(store.close)
        d = definition(); store.register_cohort(d, created_ts=100)
        runtime = ShadowRuntime(store, d)
        runtime.record_inputs(inputs(100, up_bid=.4, up_ask=.5, down_bid=.49, down_ask=.51, missing_source=True))
        row = store.conn.execute("SELECT payload_json,result_json FROM shadow_input_attempts").fetchone()
        self.assertIsNone(json.loads(row[0])["up_book"]["source_ts"])
        self.assertFalse(json.loads(row[1])["complete"])
        self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM shadow_observations").fetchone()[0], 0)

    def test_attempt_journal_includes_failure_and_exception(self):
        with tempfile.TemporaryDirectory() as directory:
            config = TelemetryConfig(str(Path(directory) / "t.sqlite3"))
            store = TelemetryStore(Path(config.db_path)); self.addCleanup(store.close)
            collector = Collector(config, store)
            for ts, value in [(100, False), (101, True)]:
                with patch.object(collector, "_tick", return_value=value):
                    self.assertEqual(collector.tick("test", ts), value)
            with patch.object(collector, "_tick", side_effect=RuntimeError("unexpected")):
                with self.assertRaises(RuntimeError): collector.tick("test", 102)
            self.assertEqual(store.conn.execute("SELECT successful FROM telemetry_attempts ORDER BY ts").fetchall(), [(0,), (1,), (0,)])
            store.close()

    def test_quality_counts_tail_and_legacy_denominator_is_unknown(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "t.sqlite3"
            store = TelemetryStore(path)
            store.conn.execute("INSERT INTO telemetry_runs VALUES ('r',100,2000,'{}')")
            store.conn.execute("INSERT INTO telemetry_snapshots(run_id,ts,day_utc,episode_id,market_id,market_slug,binance_mid,up_book_top3_json,down_book_top3_json) VALUES ('r',200,'1970-01-01','e','m','m',100,'{}','{}')")
            store.conn.commit(); store.close()
            report = {"source_freshness": {key: {"payload_timestamp_within_750ms_pct": 100} for key in ("polymarket_up", "polymarket_down", "binance")},
                      "snapshot_gaps_seconds": {"max": 0}, "midpoint_missing_analysis": {"rows_with_any_missing_pct": 0}}
            with patch("preflight.telemetry_report", return_value=report):
                result = final_telemetry_quality(path, "r", now_ts=1000, include_audit=False)
            self.assertEqual(result["tail_gap_seconds"], 800)
            self.assertFalse(result["criteria"]["maximum_gap_at_most_300_seconds"])
            self.assertFalse(result["criteria"]["complete_attempt_history"])


if __name__ == "__main__": unittest.main()
