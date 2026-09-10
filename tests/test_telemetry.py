import json
import math
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from clock_sync import ClockResponse

from telemetry import (
    BinanceQuote,
    BinanceDepthFeed,
    Book,
    FeatureRow,
    FetchFailure,
    FetchResponse,
    Level,
    MarketPair,
    QuoteUnavailable,
    TelemetryConfig,
    TelemetryStore,
    WsGap,
    calculate_features,
    fetch_json,
    market_slug,
    parse_book,
    source_timestamp,
    telemetry_report,
)


class FakeResponse:
    def __init__(self, payload, headers=None):
        self.payload = payload
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class TelemetryTests(unittest.TestCase):
    def test_market_slug_uses_15_minute_floor(self):
        self.assertEqual(market_slug(1_800), "btc-updown-15m-1800")
        self.assertEqual(market_slug(1_999), "btc-updown-15m-1800")

    def test_book_is_sorted_and_imbalance_is_notional_weighted(self):
        book = parse_book(
            {"bids": [{"price": "0.45", "size": "10"}, {"price": "0.46", "size": "2"}],
             "asks": [{"price": "0.55", "size": "3"}, {"price": "0.54", "size": "5"}]}
        )
        self.assertEqual(book.best_bid, 0.46)
        self.assertEqual(book.best_ask, 0.54)
        self.assertAlmostEqual(book.midpoint, 0.5)
        self.assertAlmostEqual(book.imbalance(), (5.42 - 4.35) / (5.42 + 4.35))

    def test_midpoint_missing_reason_distinguishes_thin_book(self):
        self.assertEqual(Book((), (Level(0.5, 1),)).midpoint_missing_reason, "no_bid")
        self.assertEqual(Book((Level(0.5, 1),), ()).midpoint_missing_reason, "no_ask")
        self.assertEqual(Book((), ()).midpoint_missing_reason, "no_bid_or_ask")

    def test_feature_window_uses_only_values_at_least_ten_seconds_old(self):
        book = Book((Level(0.49, 10),), (Level(0.51, 10),))
        features = calculate_features(20, 0.54, 0.46, 101.0, [(9, 0.40), (10, 0.50), (19, 0.53)],
                                      [(10, 0.50)], [(10, 100.0)], book, book)
        self.assertAlmostEqual(features.up_delta_10s, 0.04)
        self.assertAlmostEqual(features.down_delta_10s, -0.04)
        self.assertAlmostEqual(features.btc_return_10s, math.log(1.01))

    def test_source_timestamp_prefers_payload_and_labels_http_date_fallback(self):
        source_ts, kind = source_timestamp({"timestamp": "1700000000123"}, {})
        self.assertEqual(source_ts, 1_700_000_000.123)
        self.assertEqual(kind, "payload_timestamp_ms")
        source_ts, kind = source_timestamp({}, {"Date": "Tue, 01 Sep 2026 08:50:04 GMT"})
        self.assertEqual(source_ts, 1_788_252_604.0)
        self.assertEqual(kind, "http_date_second_precision")

    def test_fetch_retries_with_exponential_backoff_and_classifies_timeout(self):
        attempts, sleeps = [], []

        def flaky_open(*_, **__):
            attempts.append(1)
            if len(attempts) < 3:
                raise TimeoutError("temporary")
            return FakeResponse({"timestamp": "1700000000123"})

        response = fetch_json("https://example.test/data", 1.0, stage="test", max_attempts=3,
                              retry_backoff_seconds=.25, open_url=flaky_open, clock=lambda: 1_700_000_000.5,
                              monotonic=lambda: 1.0, sleeper=sleeps.append)
        self.assertEqual(len(attempts), 3)
        self.assertEqual(sleeps, [.25, .5])
        self.assertEqual(response.source_ts_kind, "payload_timestamp_ms")

        def always_timeout(*_, **__):
            raise TimeoutError("still unavailable")

        with self.assertRaises(FetchFailure) as raised:
            fetch_json("https://example.test/data", 1.0, stage="test", max_attempts=2,
                       open_url=always_timeout, sleeper=lambda _: None)
        self.assertEqual([failure.classification for failure in raised.exception.failures], ["timeout", "timeout"])

    def test_websocket_quote_uses_same_750ms_stale_gate_and_disconnect_invalidates_cache(self):
        now = [10.749]
        feed = BinanceDepthFeed(TelemetryConfig(db_path="ignored"), clock=lambda: now[0])
        feed._publish(BinanceQuote(100.0, 10.0, "payload_E_ms", 10.1, 0.0, 123))
        self.assertEqual(feed.latest_quote().update_id, 123)
        now[0] = 10.751
        with self.assertRaises(QuoteUnavailable) as stale:
            feed.latest_quote()
        self.assertEqual(stale.exception.classification, "ws_stale_quote")
        now[0] = 10.2
        feed._mark_disconnected("ws_disconnect")
        with self.assertRaises(QuoteUnavailable) as disconnected:
            feed.latest_quote()
        self.assertEqual(disconnected.exception.classification, "ws_disconnect")
        now[0] = 10.4
        feed._publish(BinanceQuote(100.1, 10.4, "payload_E_ms", 10.4, 0.0, 124))
        gaps = feed.drain_gaps()
        self.assertEqual([gap.reason for gap in gaps], ["ws_initial_connect", "ws_disconnect"])
        self.assertAlmostEqual(gaps[-1].duration_ms, 200.0)

    def test_depth_updates_replace_and_remove_levels(self):
        book = {99.0: 1.0, 98.0: 2.0}
        BinanceDepthFeed._apply_updates(book, [["99", "0"], ["97", "3"]])
        self.assertEqual(book, {98.0: 2.0, 97.0: 3.0})

    def test_clock_sync_corrects_wall_clock_offset_before_stale_check(self):
        now = [1_700_000_001.2]
        config = TelemetryConfig(db_path="ignored", clock_sync_samples=1)
        feed = BinanceDepthFeed(config, clock=lambda: now[0])
        response = ClockResponse({"serverTime": 1_700_000_000_000}, 1_700_000_001.2, 200.0, 1000.0, "test")
        with patch("telemetry.ClockSampler.fetch", return_value=response):
            self.assertTrue(feed._sync_clock(required=True))
        self.assertAlmostEqual(feed._clock_offset_ms, 1100.0, delta=.001)
        self.assertAlmostEqual(feed._clock_uncertainty_ms, 101.0)
        quote = BinanceQuote(100.0, 1_700_000_000.0, "payload_E_ms", 1_700_000_001.4, 0.0, 1,
                             clock_offset_ms=1100.0, clock_uncertainty_ms=100.0)
        self.assertAlmostEqual(quote.source_lag_ms, 300.0, delta=.001)
        feed._publish(quote)
        now[0] = 1_700_000_001.7
        self.assertEqual(feed.latest_quote().update_id, 1)
        now[0] = 1_700_000_001.8  # 700ms age + 100ms uncertainty exceeds 750ms
        with self.assertRaises(QuoteUnavailable) as stale:
            feed.latest_quote()
        self.assertEqual(stale.exception.classification, "ws_stale_quote")

    def test_store_persists_source_timestamps_lag_cycles_and_health_report(self):
        with tempfile.TemporaryDirectory() as directory:
            store = TelemetryStore(Path(directory) / "telemetry.sqlite3")
            try:
                run_id = store.start_run(TelemetryConfig(db_path="ignored"), 0.0)
                up_book = Book((Level(0.49, 10),), (Level(0.51, 10),), 9.5, "payload_timestamp_ms", 10.0, 12.0)
                down_book = Book((), (Level(0.51, 10),), 9.6, "payload_timestamp_ms", 10.1, 13.0)
                binance = BinanceQuote(100.0, 9.0, "http_date_second_precision", 10.2, 8.0)
                market = MarketPair("m", "btc-updown-15m-0", 0, 900, "up", "down")
                store.record_snapshot(run_id, 10.0, market, up_book, down_book, binance,
                                      FeatureRow(0.04, None, 0.0001, -0.2, None))
                store.record_cycle(run_id, 10.0, "m", {
                    "market_fetch_ms": 1.0, "polymarket_up_fetch_ms": 12.0, "polymarket_down_fetch_ms": 13.0,
                    "binance_fetch_ms": 0.0, "binance_quote_lookup_ms": .2,
                    "parallel_fetch_wall_ms": 14.0, "sqlite_write_ms": 2.0,
                    "other_ms": 1.0, "cycle_ms": 17.0,
                })
                store.record_ws_gap(run_id, WsGap(9.0, 9.25, 250.0, "ws_disconnect"))
                columns = {row[1].lower() for row in store.conn.execute("PRAGMA table_info(telemetry_snapshots)")}
                self.assertTrue({"up_source_ts", "up_source_lag_ms", "down_midpoint_missing_reason"} <= columns)
                self.assertFalse({"pnl", "outcome", "win", "resolved"} & columns)
                row = store.conn.execute(
                    "SELECT up_source_ts,up_received_ts,up_source_lag_ms,down_midpoint_missing_reason "
                    "FROM telemetry_snapshots WHERE run_id = ?", (run_id,)
                ).fetchone()
                self.assertEqual(row, (9.5, 10.0, 500.0, "no_bid"))
                report = telemetry_report(store.conn, run_id)
                self.assertEqual(report["daily"][0]["snapshots"], 1)
                self.assertEqual(report["daily"][0]["shock_4c"], 1)
                self.assertEqual(report["cycle_timing_ms"]["cycle_ms"]["p50"], 17.0)
                self.assertTrue(report["source_freshness"]["polymarket_up"]["freshness_validatable"])
                self.assertFalse(report["source_freshness"]["binance"]["freshness_validatable"])
                self.assertEqual(report["midpoint_missing"], [{"side": "down", "reason": "no_bid", "count": 1}])
                self.assertEqual(report["midpoint_missing_analysis"]["rows_with_any_missing"], 1)
                self.assertEqual(report["midpoint_missing_analysis"]["rows_within_10s_after_collector_error"], 0)
                self.assertEqual(report["websocket_gaps"]["count"], 1)
            finally:
                store.close()

    def test_resume_reuses_active_run_only_with_identical_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            store = TelemetryStore(Path(directory) / "resume.sqlite3")
            try:
                config = TelemetryConfig(db_path="fixed.sqlite3", duration_seconds=100.0)
                run_id = store.start_run(config, 10.0)
                self.assertEqual(store.active_run(config, 20.0), (run_id, 110.0))
                with self.assertRaises(RuntimeError):
                    store.active_run(TelemetryConfig(db_path="fixed.sqlite3", poll_seconds=2.0,
                                                     duration_seconds=100.0), 20.0)
                self.assertIsNone(store.active_run(config, 111.0))
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
