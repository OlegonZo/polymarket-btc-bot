"""Public server-time sampling with explicit TLS setup and request timing.

TLS setup precedes the time request and therefore is not part of the interval
in which the server can generate serverTime. HTTP request/response latency is
kept in full; no assumed one-way latency is subtracted. TLS verification stays
enabled. Configured proxies use the existing conservative urllib transport.
"""
from dataclasses import dataclass
from http.client import HTTPSConnection
import json
import math
import time
from urllib.parse import urlsplit
from urllib.request import getproxies


@dataclass(frozen=True)
class ClockResponse:
    payload: dict
    received_ts: float
    request_ms: float
    setup_ms: float
    transport: str


class ClockSampler:
    def __init__(self, url, timeout, *, fallback, connection_factory=HTTPSConnection,
                 clock=time.time, monotonic=time.perf_counter, proxies=None):
        parsed = urlsplit(url)
        if (parsed.scheme != "https" or parsed.hostname not in
                {"api.binance.com", "data-api.binance.vision"} or
                parsed.path != "/api/v3/time" or parsed.username or parsed.password or
                parsed.port not in (None, 443) or parsed.query or parsed.fragment):
            raise ValueError("clock endpoint must be a supported public Binance HTTPS time URL")
        self.url, self.timeout, self.fallback = url, timeout, fallback
        self.clock, self.monotonic = clock, monotonic
        configured = getproxies() if proxies is None else proxies
        self.use_proxy_fallback = bool(configured.get("https") or configured.get("all"))
        self.connection = None if self.use_proxy_fallback else connection_factory(parsed.hostname, timeout=timeout)
        self.path = parsed.path

    def __enter__(self): return self

    def __exit__(self, *_):
        if self.connection is not None: self.connection.close()

    def fetch(self):
        if self.use_proxy_fallback:
            response = self.fallback()
            return ClockResponse(response.payload, response.received_ts, response.request_ms,
                                 0.0, "urllib_proxy_full_request_conservative")
        connection = self.connection
        setup_ms = 0.0
        try:
            if connection.sock is None:
                setup_start = self.monotonic()
                connection.connect()  # DNS/TCP/TLS complete BEFORE serverTime request
                setup_ms = (self.monotonic() - setup_start) * 1000
            wall_start, started = self.clock(), self.monotonic()
            connection.request("GET", self.path, headers={"User-Agent": "polymarket-telemetry/2.1",
                                                          "Cache-Control": "no-cache"})
            response = connection.getresponse()
            body = response.read()  # fully drain before reusing HTTP connection
            received_ts, finished = self.clock(), self.monotonic()
            request_ms = (finished - started) * 1000
            if response.status != 200:
                raise ValueError(f"clock_http_{response.status}")
            # A stepped local wall clock invalidates midpoint inference.
            if abs((received_ts - wall_start) * 1000 - request_ms) > 10:
                raise ValueError("local_clock_step_during_time_request")
            if not math.isfinite(request_ms) or request_ms < 0:
                raise ValueError("invalid_clock_round_trip")
            payload = json.loads(body)
            if not isinstance(payload, dict): raise ValueError("invalid_clock_payload")
            return ClockResponse(payload, received_ts, request_ms, setup_ms, "https_preconnected_request")
        except Exception:
            connection.close()  # next attempt gets a new verified connection
            raise
