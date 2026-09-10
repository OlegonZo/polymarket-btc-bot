from pathlib import Path
import tempfile
import unittest

from shadow_cohort import CohortDefinition, LOCKED_PARAMETERS, ShadowStore, cohort_report, resolve_pending
from shadow_runtime import FailoverBinanceDepthFeed, LiveInputs, ShadowLiveCollector, ShadowRuntime
from telemetry import BinanceQuote, Book, Level, QuoteUnavailable, TelemetryConfig


def definition() -> CohortDefinition:
    return CohortDefinition(
        "attribution-forward-20260917-reversion-v1",
        "microstructure-reversion-v1",
        LOCKED_PARAMETERS,
        "operator approval 2026-09-17",
        100.0,
        100.0,
    )


def market_payload() -> dict[str, object]:
    return {
        "id": "market-1",
        "slug": "btc-updown-15m-0",
        "active": True,
        "acceptingOrders": True,
        "enableOrderBook": True,
        "outcomes": '["UP","DOWN"]',
        "clobTokenIds": '["up-token","down-token"]',
        "feesEnabled": True,
        "feeSchedule": {"rate": "0.07", "exponent": 1, "takerOnly": True, "rebateRate": "0.20"},
    }


def source_book(bid: float, ask: float, ts: float, *, source_ts: float | None) -> Book:
    return Book(
        (Level(bid, 100),),
        (Level(ask, 100),),
        source_ts,
        "payload_timestamp_ms",
        ts,
        1.0,
    )


def inputs(ts: float, *, up_bid: float, up_ask: float, down_bid: float, down_ask: float,
           source_ts: float | None = None, missing_source: bool = False) -> LiveInputs:
    return LiveInputs(
        ts,
        market_payload(),
        source_book(up_bid, up_ask, ts, source_ts=None if missing_source else (ts if source_ts is None else source_ts)),
        source_book(down_bid, down_ask, ts, source_ts=None if missing_source else (ts if source_ts is None else source_ts)),
        BinanceQuote(100_000.0, ts, "payload_E_ms", ts, 0.0, 1, 0.0, 0.0),
    )


class ShadowRuntimeTests(unittest.TestCase):
    def test_source_inputs_flow_to_heartbeat_and_shadow_observation(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ShadowStore(Path(directory) / "shadow.sqlite3")
            try:
                cohort = definition()
                store.register_cohort(cohort, created_ts=100.0)
                runtime = ShadowRuntime(store, cohort)

                # Seed a complete baseline at t=100; it cannot itself form a
                # 10-second signal yet.
                first = runtime.record_inputs(inputs(100.0, up_bid=.40, up_ask=.50, down_bid=.49, down_ask=.51))
                self.assertEqual(first["reason"], "baseline_not_available_at_10_seconds")

                # At t=110, UP has jumped 5 cents, the Binance midpoint has
                # not moved, and DOWN is executable as the contrarian side.
                second = runtime.record_inputs(inputs(110.0, up_bid=.40, up_ask=.60, down_bid=.465, down_ask=.48))
                self.assertEqual(second["candidate_side"], "DOWN")
                self.assertTrue(second["would_enter"])
                self.assertTrue(second["recorded"])

                observation_count = store.conn.execute("SELECT COUNT(*) FROM shadow_observations").fetchone()[0]
                heartbeats = store.conn.execute(
                    "SELECT complete,reason FROM shadow_heartbeats ORDER BY ts"
                ).fetchall()
                self.assertEqual(observation_count, 1)
                self.assertEqual(heartbeats, [(0, "baseline_not_available_at_10_seconds"), (1, "directional_candidate")])

                self.assertEqual(resolve_pending(store, cohort, lambda *_: "DOWN", now_ts=1_000), {"resolved": 1})
                report = cohort_report(store, cohort, as_of_ts=1_000)
                self.assertEqual(report["resolved_accepted_episode_representatives"], 1)
                self.assertGreater(report["primary_net_pnl"]["episode_stability"]["mean"], 0)
            finally:
                store.close()

    def test_invalid_source_is_a_heartbeat_not_a_shadow_candidate(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ShadowStore(Path(directory) / "shadow.sqlite3")
            try:
                cohort = definition()
                store.register_cohort(cohort, created_ts=100.0)
                runtime = ShadowRuntime(store, cohort)
                result = runtime.record_inputs(
                    inputs(100.0, up_bid=.40, up_ask=.50, down_bid=.49, down_ask=.51, missing_source=True)
                )
                self.assertFalse(result["complete"])
                self.assertIn("missing_payload_source_timestamp", result["reason"])
                self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM shadow_observations").fetchone()[0], 0)
                self.assertEqual(store.conn.execute("SELECT complete FROM shadow_heartbeats").fetchone()[0], 0)
            finally:
                store.close()

    def test_failover_rotates_endpoint_and_uses_configured_backoff_ceiling(self):
        config = TelemetryConfig(Path("telemetry-test.sqlite3"), retry_backoff_seconds=.25, ws_max_reconnect_attempts=5,
                                 ws_max_backoff_seconds=30.0)
        feed = FailoverBinanceDepthFeed(
            config,
            endpoints=("wss://primary.example/ws", "wss://fallback.example/ws"),
            clock=lambda: 110.0,
        )
        self.assertEqual(feed.reconnect_delay_seconds(1), .25)
        self.assertEqual(feed.reconnect_delay_seconds(5), 4.0)
        self.assertEqual(feed.reconnect_delay_seconds(8), 30.0)
        self.assertEqual(feed.reconnect_delay_seconds(99), 30.0)

        feed._rotate_endpoint("ws_disconnect")
        self.assertEqual(feed.active_endpoint, "wss://fallback.example/ws")
        events = feed.drain_transport_events()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event, "endpoint_switch")
        self.assertEqual(events[0].endpoint, "wss://fallback.example/ws")
        self.assertEqual(events[0].reason, "ws_disconnect")

    def test_market_data_failover_rotates_websocket_snapshot_and_clock_together(self):
        feed = FailoverBinanceDepthFeed(
            TelemetryConfig(Path("telemetry-test.sqlite3")),
            endpoints=(
                "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms",
                "wss://data-stream.binance.vision/ws/btcusdt@depth@100ms",
            ),
            clock=lambda: 110.0,
        )
        feed._rotate_endpoint("http_451")
        self.assertTrue(feed.active_endpoint.startswith("wss://data-stream.binance.vision/"))
        self.assertTrue(feed.depth_snapshot_url.startswith("https://data-api.binance.vision/"))
        self.assertTrue(feed.server_time_url.startswith("https://data-api.binance.vision/"))

    def test_restricted_primary_is_quarantined_and_fallback_is_sticky(self):
        primary, fallback = "wss://primary.example/ws", "wss://fallback.example/ws"
        feed = FailoverBinanceDepthFeed(TelemetryConfig(Path("telemetry-test.sqlite3")),
                                        endpoints=(primary, fallback), clock=lambda: 110.0)
        restricted = QuoteUnavailable("ws_disconnect", "Handshake status 451: restricted location eligibility")
        self.assertTrue(feed._is_permanent_endpoint_failure(restricted))
        feed._quarantine_endpoint(primary, "http_451_restricted_location")
        feed._rotate_endpoint("ws_disconnect")
        self.assertEqual(feed.active_endpoint, fallback)
        # A transient fallback reconnect must not pay another known-451 round trip.
        feed._rotate_endpoint("ws_disconnect")
        self.assertEqual(feed.active_endpoint, fallback)
        events = feed.drain_transport_events()
        self.assertEqual(events[0].event, "endpoint_quarantined")
        self.assertEqual([event.event for event in events].count("endpoint_switch"), 1)

    def test_non_451_failures_are_not_quarantined(self):
        transient = QuoteUnavailable("ws_disconnect", "connection reset")
        self.assertFalse(FailoverBinanceDepthFeed._is_permanent_endpoint_failure(transient))

    def test_collector_persists_failover_events_with_the_cohort(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ShadowStore(Path(directory) / "shadow.sqlite3")
            try:
                cohort = definition()
                store.register_cohort(cohort, created_ts=100.0)
                runtime = ShadowRuntime(store, cohort)
                feed = FailoverBinanceDepthFeed(
                    TelemetryConfig(Path("telemetry-test.sqlite3")),
                    endpoints=("wss://primary.example/ws", "wss://fallback.example/ws"),
                    clock=lambda: 110.0,
                )
                feed._rotate_endpoint("ws_disconnect")
                collector = ShadowLiveCollector(
                    runtime, TelemetryConfig(Path("telemetry-test.sqlite3")), feed=feed, clock=lambda: 110.0
                )
                collector._drain_transport_events()
                self.assertEqual(
                    store.conn.execute(
                        "SELECT source,endpoint,event,reason FROM shadow_transport_events"
                    ).fetchall(),
                    [("binance_depth_ws", "wss://fallback.example/ws", "endpoint_switch", "ws_disconnect")],
                )
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
