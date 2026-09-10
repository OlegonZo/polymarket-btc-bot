"""Source-to-shadow integration for a separately approved future cohort.

This module has no command-line runner and no order-submission code.  A caller
must construct, register and explicitly start an approved ``CohortDefinition``
before it can write to a shadow database.
"""
from __future__ import annotations

from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
import math
import time
from typing import Any, Callable, Deque, Mapping, Optional, Sequence

from shadow_cohort import (
    Book as ShadowBook,
    CohortDefinition,
    FeatureBaseline,
    Level as ShadowLevel,
    MarketState,
    ShadowSnapshot,
    ShadowStore,
    book_freshness,
    choose_feature_baseline,
    market_state_from_gamma,
    record_shadow,
)
from telemetry import (
    BINANCE_DEPTH_WS_URL,
    BINANCE_DEPTH_SNAPSHOT_URL,
    BINANCE_SERVER_TIME_URL,
    CLOB_BOOK_URL,
    GAMMA_EVENT_URL,
    BinanceDepthFeed,
    BinanceQuote,
    Book as SourceBook,
    FetchFailure,
    QuoteUnavailable,
    TelemetryConfig,
    fetch_json,
    market_slug,
    parse_book,
)


# The first endpoint is Binance's documented Spot WebSocket endpoint. The
# secondary market-data endpoint is a transport fallback and must pass staged
# acceptance before a cohort may rely on it.
DEFAULT_BINANCE_DEPTH_ENDPOINTS = (
    BINANCE_DEPTH_WS_URL,
    "wss://data-stream.binance.vision/ws/btcusdt@depth@100ms",
)


def _rest_urls_for_ws(endpoint: str) -> tuple[str, str]:
    if endpoint.startswith("wss://data-stream.binance.vision/"):
        return (
            "https://data-api.binance.vision/api/v3/depth?symbol=BTCUSDT&limit=1000",
            "https://data-api.binance.vision/api/v3/time",
        )
    return BINANCE_DEPTH_SNAPSHOT_URL, BINANCE_SERVER_TIME_URL


@dataclass(frozen=True)
class TransportEvent:
    """An auditable transport event emitted by the Binance source adapter."""

    ts: float
    endpoint: str
    event: str
    reason: str


class FailoverBinanceDepthFeed(BinanceDepthFeed):
    """Future-only Binance feed with endpoint rotation and durable evidence.

    A reconnect never implies continuity: the parent feed invalidates its
    cached quote until the replacement session is sequence-synchronized.
    """

    def __init__(
        self,
        config: TelemetryConfig,
        *,
        endpoints: Sequence[str] = DEFAULT_BINANCE_DEPTH_ENDPOINTS,
        connector: Optional[Callable[..., Any]] = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        ordered = tuple(dict.fromkeys(endpoints))
        if not ordered or any(not endpoint.startswith("wss://") for endpoint in ordered):
            raise ValueError("endpoints must contain one or more secure WebSocket URLs")
        depth_url, time_url = _rest_urls_for_ws(ordered[0])
        kwargs: dict[str, Any] = {
            "clock": clock, "ws_url": ordered[0],
            "depth_snapshot_url": depth_url, "server_time_url": time_url,
        }
        if connector is not None:
            kwargs["connector"] = connector
        super().__init__(config, **kwargs)
        self._endpoints = ordered
        self._endpoint_index = 0
        # A geographic-policy 451 is deterministic for this process.  Retrying
        # that endpoint between healthy fallback sessions turns one transient
        # fallback break into a predictable second outage.
        self._quarantined_endpoints: set[str] = set()
        self._transport_events: Deque[TransportEvent] = deque()

    @property
    def active_endpoint(self) -> str:
        with self._lock:
            return self.ws_url

    def drain_transport_events(self) -> list[TransportEvent]:
        with self._lock:
            items = list(self._transport_events)
            self._transport_events.clear()
        return items

    def _record_transport_event(self, event: str, reason: str, *, endpoint: Optional[str] = None) -> None:
        target = self.active_endpoint if endpoint is None else endpoint
        with self._lock:
            self._transport_events.append(TransportEvent(self.clock(), target, event, reason))

    @staticmethod
    def _is_permanent_endpoint_failure(exc: QuoteUnavailable) -> bool:
        return "451" in str(exc) and ("restricted location" in str(exc).lower() or
                                       "eligibility" in str(exc).lower())

    def _quarantine_endpoint(self, endpoint: str, reason: str) -> None:
        with self._lock:
            self._quarantined_endpoints.add(endpoint)
        self._record_transport_event("endpoint_quarantined", reason, endpoint=endpoint)

    def _rotate_endpoint(self, reason: str) -> None:
        previous = self.active_endpoint
        with self._lock:
            eligible = [index for index, endpoint in enumerate(self._endpoints)
                        if endpoint not in self._quarantined_endpoints]
            if not eligible:
                # Preserve an explicit observable failure when every configured
                # source is unavailable; never silently invent a third source.
                eligible = [self._endpoint_index]
            for offset in range(1, len(self._endpoints) + 1):
                candidate = (self._endpoint_index + offset) % len(self._endpoints)
                if candidate in eligible:
                    self._endpoint_index = candidate
                    break
            self.ws_url = self._endpoints[self._endpoint_index]
            self.depth_snapshot_url, self.server_time_url = _rest_urls_for_ws(self.ws_url)
            current = self.ws_url
        if current != previous:
            self._record_transport_event("endpoint_switch", reason, endpoint=current)

    def _run(self) -> None:
        consecutive_failures = 0
        while not self._stop_event.is_set():
            with self._lock:
                self._session_published = False
            try:
                self._run_session()
                if self._stop_event.is_set():
                    break
                raise QuoteUnavailable("ws_disconnect", "Binance WebSocket session ended")
            except QuoteUnavailable as exc:
                if self._stop_event.is_set():
                    break
                with self._lock:
                    session_published = self._session_published
                if session_published:
                    consecutive_failures = 0
                consecutive_failures += 1
                delay_seconds = self.reconnect_delay_seconds(consecutive_failures)
                endpoint = self.active_endpoint
                self._mark_disconnected(exc.classification)
                self._emit(exc.classification, str(exc), consecutive_failures, delay_seconds * 1000.0)
                self._record_transport_event("disconnect", f"{exc.classification}:{exc}", endpoint=endpoint)
                if self._is_permanent_endpoint_failure(exc):
                    self._quarantine_endpoint(endpoint, "http_451_restricted_location")
                if consecutive_failures == self.config.ws_max_reconnect_attempts:
                    self._emit(
                        "ws_reconnect_exhausted",
                        f"{consecutive_failures} consecutive reconnect attempts failed; continuing at capped backoff",
                        consecutive_failures,
                        delay_seconds * 1000.0,
                    )
                    self._record_transport_event(
                        "reconnect_threshold_reached", f"{exc.classification}:{exc}", endpoint=endpoint
                    )
                self._rotate_endpoint(exc.classification)
                self._stop_event.wait(delay_seconds)


@dataclass(frozen=True)
class LiveInputs:
    """Full decision-time inputs returned by the public source adapters."""

    observed_ts: float
    market_payload: Mapping[str, object]
    up_book: SourceBook
    down_book: SourceBook
    binance_quote: BinanceQuote


def _market_from_event(payload: Mapping[str, object]) -> Mapping[str, object]:
    markets = payload.get("markets")
    if not isinstance(markets, list) or len(markets) != 1 or not isinstance(markets[0], Mapping):
        raise ValueError("expected_exactly_one_market_in_gamma_event")
    return markets[0]


def _shadow_book(book: SourceBook) -> ShadowBook:
    return ShadowBook(
        tuple(ShadowLevel(level.price, level.size) for level in book.bids),
        tuple(ShadowLevel(level.price, level.size) for level in book.asks),
        book.source_ts,
        book.source_ts_kind,
    )


def fetch_live_inputs(config: TelemetryConfig, feed: BinanceDepthFeed, *,
                      clock: Callable[[], float] = time.time) -> LiveInputs:
    """Read fresh market state and both full CLOB books, then read Binance.

    The market response is deliberately retained whole so the fee, outcome
    labels and token mapping written to the cohort come from the same source
    response that selected the two books.
    """
    requested_ts = clock()
    market_response = fetch_json(
        GAMMA_EVENT_URL.format(slug=market_slug(requested_ts)),
        config.timeout_seconds,
        stage="shadow_polymarket_market",
        max_attempts=config.max_fetch_attempts,
        retry_backoff_seconds=config.retry_backoff_seconds,
    )
    raw_market = _market_from_event(market_response.payload)
    market = market_state_from_gamma(raw_market)
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="shadow-book-fetch") as executor:
        futures = {
            "up": executor.submit(
                fetch_json,
                CLOB_BOOK_URL.format(token_id=market.up_token_id),
                config.timeout_seconds,
                stage="shadow_polymarket_up_book",
                max_attempts=config.max_fetch_attempts,
                retry_backoff_seconds=config.retry_backoff_seconds,
            ),
            "down": executor.submit(
                fetch_json,
                CLOB_BOOK_URL.format(token_id=market.down_token_id),
                config.timeout_seconds,
                stage="shadow_polymarket_down_book",
                max_attempts=config.max_fetch_attempts,
                retry_backoff_seconds=config.retry_backoff_seconds,
            ),
        }
        up_response, down_response = futures["up"].result(), futures["down"].result()
    observed_ts = clock()
    return LiveInputs(
        observed_ts,
        raw_market,
        parse_book(up_response.payload, up_response),
        parse_book(down_response.payload, down_response),
        feed.latest_quote(observed_ts),
    )


class ShadowRuntime:
    """Builds and records one immutable future-cohort decision at a time."""

    def __init__(self, store: ShadowStore, definition: CohortDefinition) -> None:
        self.store = store
        self.definition = definition
        self._history: Deque[FeatureBaseline] = deque()
        self._market_id: Optional[str] = None

    def record_failure(self, ts: float, reason: str) -> dict[str, object]:
        self.store.record_heartbeat(self.definition, ts=ts, complete=False, reason=reason)
        return {"recorded": False, "complete": False, "reason": reason}

    def record_inputs(self, inputs: LiveInputs) -> dict[str, object]:
        result = self._record_inputs(inputs)
        self.store.record_input_attempt(self.definition, ts=inputs.observed_ts,
                                        payload=asdict(inputs), result=result)
        return result

    def _record_inputs(self, inputs: LiveInputs) -> dict[str, object]:
        try:
            market = market_state_from_gamma(inputs.market_payload)
            if self._market_id != market.market_id:
                self._history.clear()
                self._market_id = market.market_id
            baseline = choose_feature_baseline(tuple(self._history), inputs.observed_ts)
            snapshot = ShadowSnapshot(
                ts=inputs.observed_ts,
                market=market,
                up_book=_shadow_book(inputs.up_book),
                down_book=_shadow_book(inputs.down_book),
                binance_mid=inputs.binance_quote.midpoint,
                binance_source_ts=inputs.binance_quote.source_ts,
                binance_source_ts_kind=inputs.binance_quote.source_ts_kind,
                system_clock_offset_ms=inputs.binance_quote.clock_offset_ms,
                system_clock_uncertainty_ms=inputs.binance_quote.clock_uncertainty_ms,
                baseline=baseline,
            )
        except Exception as exc:  # no raw input is converted into an entry
            return self.record_failure(inputs.observed_ts, f"input_error:{type(exc).__name__}")

        raw_ready, raw_reason = self._raw_ready(snapshot)
        if raw_ready:
            self._append_baseline(snapshot)
        if not raw_ready:
            return self.record_failure(inputs.observed_ts, raw_reason)
        if not snapshot.baseline_valid:
            return self.record_failure(inputs.observed_ts, "baseline_not_available_at_10_seconds")

        try:
            result = record_shadow(self.store, self.definition, snapshot)
        except Exception as exc:
            return self.record_failure(inputs.observed_ts, f"shadow_record_error:{type(exc).__name__}")
        reason = "directional_candidate" if result is not None else "no_directional_candidate"
        self.store.record_heartbeat(self.definition, ts=inputs.observed_ts, complete=True, reason=reason)
        return {
            "recorded": result is not None,
            "complete": True,
            "reason": reason,
            "candidate_side": None if result is None else result["candidate_side"],
            "would_enter": None if result is None else result["would_enter"],
        }

    @staticmethod
    def _raw_ready(snapshot: ShadowSnapshot) -> tuple[bool, str]:
        for side, book in (("up", snapshot.up_book), ("down", snapshot.down_book)):
            if reason := book.validation_reason():
                return False, f"invalid_{side}_book:{reason}"
        if not isinstance(snapshot.binance_mid, (int, float)) or not math.isfinite(snapshot.binance_mid):
            return False, "missing_binance_mid"
        freshness = book_freshness(snapshot)
        if not freshness.passed:
            return False, freshness.reason
        return True, "complete_raw_inputs"

    def _append_baseline(self, snapshot: ShadowSnapshot) -> None:
        self._history.append(
            FeatureBaseline(
                snapshot.ts,
                snapshot.up_book.midpoint,
                snapshot.down_book.midpoint,
                snapshot.binance_mid,
            )
        )
        cutoff = snapshot.ts - 12.0
        while self._history and self._history[0].observed_ts < cutoff:
            self._history.popleft()


class ShadowLiveCollector:
    """Thin, explicit live-source adapter for the future shadow collector."""

    def __init__(self, runtime: ShadowRuntime, config: TelemetryConfig,
                 *, feed: Optional[BinanceDepthFeed] = None,
                 clock: Callable[[], float] = time.time) -> None:
        self.runtime = runtime
        self.config = config
        self.feed = feed or FailoverBinanceDepthFeed(config, clock=clock)
        self.clock = clock

    def start(self) -> None:
        self.feed.start()

    def close(self) -> None:
        self.feed.stop()

    def tick(self) -> dict[str, object]:
        started_ts = self.clock()
        try:
            inputs = fetch_live_inputs(self.config, self.feed, clock=self.clock)
        except FetchFailure as exc:
            result = self.runtime.record_failure(started_ts, f"source_fetch_error:{exc.failures[-1].classification}")
        except QuoteUnavailable as exc:
            result = self.runtime.record_failure(started_ts, f"binance_quote_error:{exc.classification}")
        except Exception as exc:  # source protocol failures also remain visible
            result = self.runtime.record_failure(started_ts, f"source_error:{type(exc).__name__}")
        else:
            result = self.runtime.record_inputs(inputs)
        self._drain_transport_events()
        return result

    def _drain_transport_events(self) -> None:
        drain = getattr(self.feed, "drain_transport_events", None)
        if not callable(drain):
            return
        for event in drain():
            self.runtime.store.record_transport_event(
                self.runtime.definition,
                ts=event.ts,
                source="binance_depth_ws",
                endpoint=event.endpoint,
                event=event.event,
                reason=event.reason,
            )


__all__ = [
    "DEFAULT_BINANCE_DEPTH_ENDPOINTS",
    "FailoverBinanceDepthFeed",
    "LiveInputs",
    "ShadowLiveCollector",
    "ShadowRuntime",
    "TransportEvent",
    "fetch_live_inputs",
]
