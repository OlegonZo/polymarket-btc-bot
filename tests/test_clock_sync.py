import json
import unittest
from unittest.mock import Mock
from clock_sync import ClockResponse, ClockSampler


class Time:
    mono = 100.0
    step = 0.0
    def wall(self): return self.mono + self.step
    def tick(self, seconds): self.mono += seconds


class Connection:
    def __init__(self, clock):
        self.clock, self.sock = clock, None
        self.events, self.status, self.wall_step = [], 200, 0
    def connect(self):
        self.events.append("verified_tls_connect")
        self.clock.tick(1.75); self.sock = object()
    def request(self, method, path, headers):
        assert self.sock is not None
        assert method == "GET" and path == "/api/v3/time"
        self.events.append("request")
        self.clock.tick(.15)
        self.server_time = int(self.clock.mono * 1000)
    def getresponse(self): return self
    def read(self):
        self.clock.tick(.15); self.clock.step += self.wall_step
        return json.dumps({"serverTime": self.server_time}).encode()
    def close(self): self.sock = None


class ClockSyncTests(unittest.TestCase):
    def sampler(self, connection, clock):
        return ClockSampler("https://data-api.binance.vision/api/v3/time", 5,
                            fallback=Mock(side_effect=AssertionError("unexpected fallback")),
                            connection_factory=lambda *a, **k: connection,
                            clock=clock.wall, monotonic=lambda: clock.mono, proxies={})

    def test_tls_setup_is_separate_and_full_http_latency_is_retained(self):
        clock = Time(); connection = Connection(clock)
        with self.sampler(connection, clock) as sampler:
            first, second = sampler.fetch(), sampler.fetch()
        self.assertAlmostEqual(first.setup_ms, 1750)
        self.assertAlmostEqual(first.request_ms, 300)
        self.assertAlmostEqual(second.setup_ms, 0)
        self.assertAlmostEqual(second.request_ms, 300)
        self.assertEqual(connection.events, ["verified_tls_connect", "request", "request"])
        self.assertIsNone(connection.sock)

    def test_wall_clock_step_invalidates_sample(self):
        clock = Time(); connection = Connection(clock); connection.wall_step = 1
        with self.sampler(connection, clock) as sampler:
            with self.assertRaisesRegex(ValueError, "clock_step"): sampler.fetch()
        self.assertIsNone(connection.sock)

    def test_http_failure_closes_connection_and_next_attempt_reconnects(self):
        clock = Time(); connection = Connection(clock); connection.status = 503
        with self.sampler(connection, clock) as sampler:
            with self.assertRaisesRegex(ValueError, "503"): sampler.fetch()
            connection.status = 200
            result = sampler.fetch()
        self.assertAlmostEqual(result.setup_ms, 1750)
        self.assertEqual(connection.events.count("verified_tls_connect"), 2)

    def test_configured_proxy_is_not_bypassed(self):
        fallback = Mock(return_value=ClockResponse({"serverTime": 1000}, 1, 1200, 0, "test"))
        factory = Mock(side_effect=AssertionError("must not connect directly"))
        with ClockSampler("https://data-api.binance.vision/api/v3/time", 5, fallback=fallback,
                          connection_factory=factory, proxies={"https": "http://proxy.example"}) as sampler:
            result = sampler.fetch()
        factory.assert_not_called(); fallback.assert_called_once()
        self.assertEqual(result.request_ms, 1200)
        self.assertEqual(result.transport, "urllib_proxy_full_request_conservative")

    def test_only_known_public_time_endpoints_allowed(self):
        for url in ["http://data-api.binance.vision/api/v3/time", "https://example.com/api/v3/time",
                    "https://user:pass@data-api.binance.vision/api/v3/time", "https://api.binance.com/api/v3/order"]:
            with self.assertRaises(ValueError): ClockSampler(url, 5, fallback=Mock())


if __name__ == "__main__": unittest.main()
