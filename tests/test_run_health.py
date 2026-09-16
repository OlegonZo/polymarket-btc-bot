import argparse
from dataclasses import asdict
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from preflight import source_acceptance
from run_health import RunJournal, atomic_json, exclusive_run, read_status
from telemetry import (Book, BinanceQuote, Collector, Level, MarketPair, TelemetryConfig,
                       TelemetryStore, calculate_features, plan_run, report_command, _run_locked)
from shadow_runtime import FailoverBinanceDepthFeed, TransportEvent


class RunHealthTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_stale_running_file_is_not_claimed_as_a_live_process(self):
        path = self.root / "status.json"
        atomic_json(path, {"status": "running", "heartbeat_ts": 10})
        self.assertEqual(read_status(path, now_ts=131)["status"], "stale_unknown")
        self.assertEqual(read_status(path, now_ts=11)["status"], "running")
        self.assertEqual(read_status(path, now_ts=9)["status"], "stale_unknown")
        atomic_json(path, {"status": "complete", "heartbeat_ts": 10})
        self.assertEqual(read_status(path, now_ts=1000)["status"], "complete")

    def test_duplicate_process_lock_releases_after_exit(self):
        path = self.root / "run.lock"
        with exclusive_run(path):
            with self.assertRaises(RuntimeError):
                with exclusive_run(path):
                    self.fail("second process must not own this run")
        with exclusive_run(path):
            pass

    def test_acceptance_failure_records_terminal_state_and_preserves_existing_report(self):
        path = self.root / "acceptance.json"
        with patch("preflight.validate_live_fee_schedule", side_effect=TimeoutError("network")):
            with self.assertRaises(TimeoutError):
                source_acceptance(duration_seconds=1800, endpoint_probe_seconds=15, output=path)
        result = read_status(path.with_suffix(".progress.json"))
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["error_type"], "TimeoutError")
        self.assertFalse(path.exists())
        atomic_json(path, {"original": True})
        with self.assertRaises(FileExistsError):
            source_acceptance(duration_seconds=1800, endpoint_probe_seconds=15, output=path)
        self.assertEqual(json.loads(path.read_text()), {"original": True})

    def test_resume_requires_same_code_and_never_creates_another_expired_run(self):
        path = self.root / "run.sqlite3"
        config = TelemetryConfig(str(path), duration_seconds=100)
        with self.assertRaises(RuntimeError):
            plan_run(config, resume_latest=True, now_ts=10)
        self.assertFalse(path.exists())
        store = TelemetryStore(path)
        run_id = store.start_run(config, 10)
        store.close()
        journal = RunJournal(path.with_suffix(".status.json"), kind="telemetry_only", run_id=run_id)
        journal.update("running")
        self.assertEqual(plan_run(config, resume_latest=True, now_ts=20)["action"], "resume")
        with patch("telemetry.source_fingerprint", return_value={"changed": "source"}):
            with self.assertRaises(RuntimeError):
                plan_run(config, resume_latest=True, now_ts=20)
        before = path.read_bytes()
        with patch("telemetry.utc_now_ts", return_value=111):
            self.assertEqual(_run_locked(config, resume_latest=True), 0)
        self.assertEqual(path.read_bytes(), before)
        with self.assertRaises(RuntimeError):
            plan_run(config, resume_latest=False, now_ts=111)

    def test_report_does_not_create_a_database_for_a_typo(self):
        path = self.root / "typo.sqlite3"
        with self.assertRaises(sqlite3.OperationalError):
            report_command(argparse.Namespace(db=str(path), run_id=None))
        self.assertFalse(path.exists())

    def test_cli_collector_uses_failover_and_persists_endpoint_events(self):
        config = TelemetryConfig(str(self.root / "t.sqlite3"), binance_profile="failover")
        store = TelemetryStore(Path(config.db_path)); self.addCleanup(store.close)
        collector = Collector(config, store)
        self.assertIsInstance(collector.binance_feed, FailoverBinanceDepthFeed)
        collector.binance_feed._transport_events.append(TransportEvent(100, "wss://example.test", "endpoint_switch", "test"))
        collector._drain_feed_events("r")
        self.assertEqual(store.conn.execute("SELECT endpoint,classification FROM telemetry_errors").fetchone(),
                         ("wss://example.test", "endpoint_switch"))

    def test_ten_second_window_rejects_a_twenty_second_anchor(self):
        book = Book((Level(.49, 10),), (Level(.51, 10),))
        old = [(0, .4)]
        features = calculate_features(20, .5, .5, 100, old, old, [(0, 100)], book, book)
        self.assertIsNone(features.up_delta_10s)
        self.assertIsNone(features.down_delta_10s)
        self.assertIsNone(features.btc_return_10s)
        self.assertIsNone(features.baseline_ts)

    def test_slow_fetch_uses_actual_observation_time_and_persists_baseline(self):
        config = TelemetryConfig(str(self.root / "t.sqlite3"))
        store = TelemetryStore(Path(config.db_path)); self.addCleanup(store.close)
        class Feed:
            ws_url = "wss://example.test"
            def drain_diagnostics(self): return []
            def drain_gaps(self): return []
            def latest_quote(self, now):
                self.last_observed = now
                return BinanceQuote(100, now - .1, "payload_E_ms", now, 0, 1, 0, 20)
        feed = Feed()
        collector = Collector(config, store, feed)
        collector.up_history.append((10, .4))
        collector.down_history.append((10, .6))
        collector.binance_history.append((10, 100))
        book = Book((Level(.49, 100),), (Level(.51, 100),), 19.9, "payload_timestamp_ms", 20, 1000)
        market = MarketPair("m", "btc-updown-15m-0", 0, 900, "u", "d")
        with patch.object(collector, "_refresh_market", return_value=(market, 0)), \
             patch("telemetry.fetch_book", return_value=book), patch("telemetry.utc_now_ts", return_value=20):
            self.assertTrue(collector.tick("r", 18))
        self.assertEqual(feed.last_observed, 20)
        row = store.conn.execute("SELECT ts,observed_ts,feature_baseline_ts,up_delta_10s FROM telemetry_snapshots").fetchone()
        self.assertEqual(row[:3], (18, 20, 10))
        self.assertAlmostEqual(row[3], .1)
        self.assertEqual(collector.up_history[-1][0], 20)

    def test_stale_or_invalid_book_cannot_be_a_feature_anchor(self):
        good = Book((Level(.49, 10),), (Level(.51, 10),), 9.9, "payload_timestamp_ms", 10, 1)
        stale = Book(good.bids, good.asks, 9, "payload_timestamp_ms", 10, 1)
        bad = Book((Level(.5, -1),), good.asks, 9.9, "payload_timestamp_ms", 10, 1)
        quote = BinanceQuote(100, 9.9, "payload_E_ms", 10, 0, 1, 0, 100)
        self.assertTrue(Collector._complete_feature_inputs(10, good, good, quote))
        self.assertFalse(Collector._complete_feature_inputs(10, stale, good, quote))
        self.assertFalse(Collector._complete_feature_inputs(10, bad, good, quote))


if __name__ == "__main__":
    unittest.main()
