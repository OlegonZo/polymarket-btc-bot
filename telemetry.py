"""Telemetry-only collector for the BTC 15-minute microstructure study.

The collector never submits orders and never records an outcome or PnL. It
records both observation time and source timestamps for later freshness checks.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import statistics
import threading
import time
import uuid
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from http.client import RemoteDisconnected
from pathlib import Path
from typing import Any, Callable, Deque, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import websocket
from clock_sync import ClockSampler

GAMMA_EVENT_URL = "https://gamma-api.polymarket.com/events/slug/{slug}"
CLOB_BOOK_URL = "https://clob.polymarket.com/book?token_id={token_id}"
BINANCE_DEPTH_SNAPSHOT_URL = "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"
BINANCE_SERVER_TIME_URL = "https://api.binance.com/api/v3/time"
BINANCE_DEPTH_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms"
BINANCE_MARKET_DATA_DEPTH_URL = "https://data-api.binance.vision/api/v3/depth?symbol=BTCUSDT&limit=1000"
BINANCE_MARKET_DATA_TIME_URL = "https://data-api.binance.vision/api/v3/time"
BINANCE_MARKET_DATA_WS_URL = "wss://data-stream.binance.vision/ws/btcusdt@depth@100ms"
SLOT_SECONDS = 15 * 60
HISTORY_SECONDS = 10.0
FRESHNESS_THRESHOLD_MS = 750.0


@dataclass(frozen=True)
class TelemetryConfig:
    db_path: str
    poll_seconds: float = 1.0
    duration_days: int = 14
    timeout_seconds: float = 5.0
    market_refresh_seconds: float = 30.0
    max_fetch_attempts: int = 3
    retry_backoff_seconds: float = 0.25
    duration_seconds: Optional[float] = None
    ws_freshness_ms: float = FRESHNESS_THRESHOLD_MS
    ws_receive_timeout_seconds: float = 5.0
    ws_max_reconnect_attempts: int = 5
    ws_max_backoff_seconds: float = 30.0
    clock_sync_samples: int = 3
    clock_resync_seconds: float = 300.0
    binance_profile: str = "primary"

    @property
    def effective_duration_seconds(self) -> float:
        return self.duration_days * 86400.0 if self.duration_seconds is None else self.duration_seconds


@dataclass(frozen=True)
class MarketPair:
    market_id: str
    slug: str
    open_ts: float
    close_ts: float
    up_token_id: str
    down_token_id: str


@dataclass(frozen=True)
class Level:
    price: float
    size: float


@dataclass(frozen=True)
class FetchResponse:
    payload: Mapping[str, Any]
    received_ts: float
    request_ms: float
    source_ts: Optional[float]
    source_ts_kind: str

    @property
    def source_lag_ms(self) -> Optional[float]:
        return None if self.source_ts is None else (self.received_ts - self.source_ts) * 1000.0


@dataclass(frozen=True)
class AttemptFailure:
    stage: str
    endpoint: str
    attempt: int
    max_attempts: int
    classification: str
    detail: str
    retry_delay_ms: Optional[float]


class FetchFailure(RuntimeError):
    def __init__(self, failures: Sequence[AttemptFailure]) -> None:
        if not failures:
            raise ValueError("FetchFailure needs at least one failed attempt")
        self.failures = tuple(failures)
        last = self.failures[-1]
        super().__init__(f"{last.stage} failed after {last.attempt} attempts: {last.detail}")


@dataclass(frozen=True)
class Book:
    bids: Tuple[Level, ...]
    asks: Tuple[Level, ...]
    source_ts: Optional[float] = None
    source_ts_kind: str = "unavailable"
    received_ts: Optional[float] = None
    request_ms: Optional[float] = None

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0].price if self.asks else None

    @property
    def midpoint(self) -> Optional[float]:
        return None if self.best_bid is None or self.best_ask is None else (self.best_bid + self.best_ask) / 2.0

    @property
    def midpoint_missing_reason(self) -> Optional[str]:
        if self.bids and self.asks:
            return None
        if not self.bids and not self.asks:
            return "no_bid_or_ask"
        return "no_bid" if not self.bids else "no_ask"

    @property
    def source_lag_ms(self) -> Optional[float]:
        return None if self.source_ts is None or self.received_ts is None else (self.received_ts - self.source_ts) * 1000.0

    def top_levels(self, count: int = 3) -> Dict[str, List[Dict[str, float]]]:
        return {"bids": [asdict(level) for level in self.bids[:count]], "asks": [asdict(level) for level in self.asks[:count]]}

    def imbalance(self, count: int = 3) -> Optional[float]:
        bid_notional = sum(level.price * level.size for level in self.bids[:count])
        ask_notional = sum(level.price * level.size for level in self.asks[:count])
        total = bid_notional + ask_notional
        return None if total <= 0.0 else (bid_notional - ask_notional) / total


@dataclass(frozen=True)
class BinanceQuote:
    midpoint: float
    source_ts: Optional[float]
    source_ts_kind: str
    received_ts: float
    request_ms: float
    update_id: Optional[int] = None
    clock_offset_ms: float = 0.0
    clock_uncertainty_ms: float = 0.0

    @property
    def source_lag_ms(self) -> Optional[float]:
        return None if self.source_ts is None else (self.received_ts - self.source_ts) * 1000.0 - self.clock_offset_ms


@dataclass(frozen=True)
class WsDiagnostic:
    ts: float
    classification: str
    detail: str
    attempt: Optional[int] = None
    retry_delay_ms: Optional[float] = None


@dataclass(frozen=True)
class WsGap:
    disconnected_ts: float
    reconnected_ts: float
    duration_ms: float
    reason: str


class QuoteUnavailable(RuntimeError):
    def __init__(self, classification: str, detail: str) -> None:
        self.classification = classification
        super().__init__(detail)


@dataclass(frozen=True)
class FeatureRow:
    up_delta_10s: Optional[float]
    down_delta_10s: Optional[float]
    btc_return_10s: Optional[float]
    up_imbalance_3: Optional[float]
    down_imbalance_3: Optional[float]


def utc_now_ts() -> float:
    return time.time()


def market_slug(ts: float) -> str:
    return f"btc-updown-15m-{int(ts // SLOT_SECONDS) * SLOT_SECONDS}"


def _parse_epoch(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) or (isinstance(value, str) and value.replace(".", "", 1).isdigit()):
        number = float(value)
        if number > 1e14:
            return number / 1_000_000.0
        if number > 1e11:
            return number / 1_000.0
        return number
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None
    return None


def source_timestamp(payload: Mapping[str, Any], headers: Mapping[str, str]) -> Tuple[Optional[float], str]:
    """Return a source-provided timestamp, retaining HTTP Date as a labelled fallback.

    CLOB books contain a millisecond payload timestamp. Binance REST bookTicker
    has no event timestamp, so HTTP Date is diagnostic-only and not valid proof
    that a price is fresher than 750ms.
    """
    for key in ("timestamp", "eventTime", "E", "time", "updatedAt"):
        parsed = _parse_epoch(payload.get(key))
        if parsed is not None:
            raw = str(payload[key]).split(".", 1)[0]
            return parsed, f"payload_{key}_{'ms' if len(raw) >= 12 else 'value'}"
    date_header = headers.get("Date") or headers.get("date")
    if date_header:
        try:
            return parsedate_to_datetime(date_header).timestamp(), "http_date_second_precision"
        except (TypeError, ValueError, IndexError):
            pass
    return None, "unavailable"


def classify_error(exc: Exception) -> str:
    if isinstance(exc, HTTPError):
        return f"http_{exc.code}"
    if isinstance(exc, TimeoutError):
        return "timeout"
    if isinstance(exc, RemoteDisconnected):
        return "connection_closed"
    if isinstance(exc, URLError):
        reason = exc.reason
        if isinstance(reason, TimeoutError):
            return "timeout"
        text = repr(reason).lower()
        if "ssl" in text or "tls" in text:
            return "tls"
        if "reset" in text:
            return "connection_reset"
        if "refused" in text or "unreachable" in text or "10013" in text:
            return "connection_unavailable"
        return "url_error"
    text = repr(exc).lower()
    if "ssl" in text or "tls" in text:
        return "tls"
    if "reset" in text:
        return "connection_reset"
    return type(exc).__name__.lower()


def fetch_json(
    url: str, timeout_seconds: float, *, stage: str, max_attempts: int = 3, retry_backoff_seconds: float = 0.25,
    open_url: Callable[..., Any] = urlopen, clock: Callable[[], float] = utc_now_ts,
    monotonic: Callable[[], float] = time.perf_counter, sleeper: Callable[[float], None] = time.sleep,
) -> FetchResponse:
    """Fetch JSON with bounded exponential retry and machine-readable errors."""
    if max_attempts < 1:
        raise ValueError("max_attempts must be positive")
    failures: List[AttemptFailure] = []
    endpoint = url.split("?", 1)[0]
    for attempt in range(1, max_attempts + 1):
        started = monotonic()
        try:
            request = Request(url, headers={"User-Agent": "polymarket-telemetry/2.0"})
            with open_url(request, timeout=timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
                headers = dict(response.headers.items())
            received_ts = clock()
            source_ts, source_ts_kind = source_timestamp(payload, headers)
            return FetchResponse(payload, received_ts, (monotonic() - started) * 1000.0, source_ts, source_ts_kind)
        except Exception as exc:  # noqa: BLE001 - failure details are persisted by Collector
            delay = None if attempt == max_attempts else retry_backoff_seconds * (2 ** (attempt - 1)) * 1000.0
            failures.append(AttemptFailure(stage, endpoint, attempt, max_attempts, classify_error(exc), repr(exc), delay))
            if delay is not None:
                sleeper(delay / 1000.0)
    raise FetchFailure(failures)


def parse_market_pair(event: Mapping[str, Any], now_ts: float) -> MarketPair:
    markets = event.get("markets")
    if not isinstance(markets, list) or len(markets) != 1:
        raise ValueError("expected exactly one market in event")
    market = markets[0]
    if not market.get("active") or not market.get("acceptingOrders"):
        raise ValueError("market is not accepting orders")
    if not market.get("enableOrderBook"):
        raise ValueError("market does not have an order book")
    slug = str(market["slug"])
    if not slug.startswith("btc-updown-15m-"):
        raise ValueError("unexpected market slug")
    try:
        slot_ts = float(slug.rsplit("-", 1)[1])
    except ValueError as exc:
        raise ValueError("invalid market slug timestamp") from exc
    if not slot_ts <= now_ts < slot_ts + SLOT_SECONDS:
        raise ValueError("market slot is not currently active")
    outcomes = json.loads(str(market["outcomes"]))
    token_ids = json.loads(str(market["clobTokenIds"]))
    if not isinstance(outcomes, list) or not isinstance(token_ids, list) or len(outcomes) != 2 or len(token_ids) != 2:
        raise ValueError("expected a two-outcome token pair")
    index_by_outcome = {str(outcome).upper(): index for index, outcome in enumerate(outcomes)}
    if set(index_by_outcome) != {"UP", "DOWN"}:
        raise ValueError("outcomes must be UP and DOWN")
    return MarketPair(str(market["id"]), slug, slot_ts, slot_ts + SLOT_SECONDS,
                      str(token_ids[index_by_outcome["UP"]]), str(token_ids[index_by_outcome["DOWN"]]))


def fetch_current_market(config: TelemetryConfig, now_ts: float) -> Tuple[MarketPair, FetchResponse]:
    response = fetch_json(GAMMA_EVENT_URL.format(slug=market_slug(now_ts)), config.timeout_seconds,
                          stage="polymarket_market", max_attempts=config.max_fetch_attempts,
                          retry_backoff_seconds=config.retry_backoff_seconds)
    return parse_market_pair(response.payload, now_ts), response


def parse_book(payload: Mapping[str, Any], response: Optional[FetchResponse] = None) -> Book:
    def levels(name: str, descending: bool) -> Tuple[Level, ...]:
        raw = payload.get(name, [])
        if not isinstance(raw, list):
            raise ValueError(f"book {name} is not a list")
        return tuple(sorted((Level(float(item["price"]), float(item["size"])) for item in raw),
                            key=lambda level: level.price, reverse=descending))
    return Book(levels("bids", True), levels("asks", False),
                None if response is None else response.source_ts,
                "unavailable" if response is None else response.source_ts_kind,
                None if response is None else response.received_ts,
                None if response is None else response.request_ms)


def fetch_book(token_id: str, config: TelemetryConfig, stage: str) -> Book:
    response = fetch_json(CLOB_BOOK_URL.format(token_id=token_id), config.timeout_seconds, stage=stage,
                          max_attempts=config.max_fetch_attempts, retry_backoff_seconds=config.retry_backoff_seconds)
    return parse_book(response.payload, response)


class BinanceDepthFeed:
    """Continuously maintain the latest BTCUSDT best bid/ask from diff-depth.

    Binance Spot bookTicker has no event timestamp. The diff-depth stream does,
    so it is synchronized to an official REST depth snapshot using update IDs.
    Only a connected, sequence-valid quote whose event time is at most 750ms old
    can be read by the telemetry cycle.
    """

    def __init__(
        self,
        config: TelemetryConfig,
        *,
        connector: Callable[..., Any] = websocket.create_connection,
        clock: Callable[[], float] = utc_now_ts,
        ws_url: str = BINANCE_DEPTH_WS_URL,
        depth_snapshot_url: str = BINANCE_DEPTH_SNAPSHOT_URL,
        server_time_url: str = BINANCE_SERVER_TIME_URL,
    ) -> None:
        if not ws_url.startswith("wss://") or not depth_snapshot_url.startswith("https://") or not server_time_url.startswith("https://"):
            raise ValueError("Binance source URLs must use secure transports")
        self.config = config
        self.connector = connector
        self.clock = clock
        self.ws_url = ws_url
        self.depth_snapshot_url = depth_snapshot_url
        self.server_time_url = server_time_url
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._socket: Optional[Any] = None
        self._latest_quote: Optional[BinanceQuote] = None
        self._connected = False
        self._diagnostics: Deque[WsDiagnostic] = deque()
        self._gaps: Deque[WsGap] = deque()
        self._gap_started_ts: Optional[float] = self.clock()
        self._gap_reason = "ws_initial_connect"
        self._session_published = False
        self._connection_generation = 0
        self._clock_offset_ms = 0.0
        self._clock_uncertainty_ms = float("inf")
        self._last_clock_sync_ts = 0.0
        self._clock_sync_details: Dict[str, Any] = {}

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="binance-depth-ws", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        with self._lock:
            active_socket = self._socket
        if active_socket is not None:
            try:
                active_socket.close()
            except Exception:  # noqa: BLE001 - shutdown is best effort
                pass
        if self._thread is not None:
            self._thread.join(timeout=max(2.0, self.config.ws_receive_timeout_seconds + 1.0))

    def latest_quote(self, now_ts: Optional[float] = None) -> BinanceQuote:
        now = self.clock() if now_ts is None else now_ts
        with self._lock:
            quote = self._latest_quote
            connected = self._connected
        if quote is None:
            raise QuoteUnavailable("ws_no_quote", "Binance WebSocket has not produced a synchronized quote")
        if not connected:
            raise QuoteUnavailable("ws_disconnect", "Binance WebSocket is disconnected; cached quote is invalid")
        age_ms = (now - (quote.source_ts or 0.0)) * 1000.0 - quote.clock_offset_ms
        uncertainty = quote.clock_uncertainty_ms
        if not math.isfinite(age_ms) or not math.isfinite(uncertainty) or uncertainty < 0:
            raise QuoteUnavailable("ws_clock_uncertain", "Quote clock evidence is invalid")
        if age_ms < 0:
            raise QuoteUnavailable("ws_future_quote", "Quote source timestamp is in the future")
        if quote.source_ts is None or age_ms + uncertainty > self.config.ws_freshness_ms:
            raise QuoteUnavailable(
                "ws_stale_quote",
                f"Binance quote age upper bound {age_ms + uncertainty:.3f}ms exceeds {self.config.ws_freshness_ms:.3f}ms",
            )
        return quote

    def rejected_quote_clock_diagnostics(self, now_ts: float) -> Dict[str, float]:
        """Clock evidence only; never returns a usable quote or bypasses freshness."""
        with self._lock:
            quote = self._latest_quote
        if quote is None or quote.source_ts is None:
            return {}
        age = (now_ts - quote.source_ts) * 1000.0 - quote.clock_offset_ms
        values = {"age_ms": age, "uncertainty_ms": quote.clock_uncertainty_ms,
                  "age_upper_bound_ms": age + quote.clock_uncertainty_ms,
                  "clock_offset_ms": quote.clock_offset_ms}
        return {name: value for name, value in values.items() if math.isfinite(value)}

    def clock_sync_diagnostics(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._clock_sync_details)

    def drain_diagnostics(self) -> List[WsDiagnostic]:
        with self._lock:
            items = list(self._diagnostics)
            self._diagnostics.clear()
        return items

    def drain_gaps(self) -> List[WsGap]:
        with self._lock:
            items = list(self._gaps)
            self._gaps.clear()
        return items

    def _emit(self, classification: str, detail: str, attempt: Optional[int] = None,
              retry_delay_ms: Optional[float] = None) -> None:
        with self._lock:
            self._diagnostics.append(WsDiagnostic(self.clock(), classification, detail, attempt, retry_delay_ms))

    def _mark_disconnected(self, reason: str) -> None:
        now = self.clock()
        with self._lock:
            self._connected = False
            if self._gap_started_ts is None:
                self._gap_started_ts = now
                self._gap_reason = reason

    def _publish(self, quote: BinanceQuote) -> None:
        with self._lock:
            self._latest_quote = quote
            self._session_published = True
            if not self._connected:
                self._connected = True
                if self._gap_started_ts is not None:
                    self._gaps.append(
                        WsGap(
                            self._gap_started_ts,
                            quote.received_ts,
                            max(0.0, (quote.received_ts - self._gap_started_ts) * 1000.0),
                            self._gap_reason,
                        )
                    )
                    self._gap_started_ts = None

    @staticmethod
    def _apply_updates(book: Dict[float, float], updates: Sequence[Sequence[Any]]) -> None:
        for raw_price, raw_quantity in updates:
            price, quantity = float(raw_price), float(raw_quantity)
            if quantity == 0.0:
                book.pop(price, None)
            else:
                book[price] = quantity

    def _load_depth_snapshot(self) -> Tuple[Dict[float, float], Dict[float, float], int]:
        try:
            response = fetch_json(
                self.depth_snapshot_url,
                self.config.timeout_seconds,
                stage="binance_depth_snapshot",
                max_attempts=self.config.max_fetch_attempts,
                retry_backoff_seconds=self.config.retry_backoff_seconds,
            )
        except FetchFailure as exc:
            for failure in exc.failures:
                self._emit(f"ws_snapshot_{failure.classification}", failure.detail, failure.attempt,
                           failure.retry_delay_ms)
            raise QuoteUnavailable("ws_disconnect", str(exc)) from exc
        try:
            bids = {float(price): float(quantity) for price, quantity in response.payload["bids"] if float(quantity) > 0.0}
            asks = {float(price): float(quantity) for price, quantity in response.payload["asks"] if float(quantity) > 0.0}
            return bids, asks, int(response.payload["lastUpdateId"])
        except (KeyError, TypeError, ValueError) as exc:
            raise QuoteUnavailable("ws_protocol_error", f"invalid Binance depth snapshot: {exc!r}") from exc

    def _sync_clock(self, required: bool) -> bool:
        samples = []
        def fallback():
            return fetch_json(self.server_time_url, self.config.timeout_seconds,
                              stage="binance_server_time", max_attempts=self.config.max_fetch_attempts,
                              retry_backoff_seconds=self.config.retry_backoff_seconds)
        with ClockSampler(self.server_time_url, self.config.timeout_seconds, fallback=fallback,
                          clock=self.clock) as sampler:
            for attempt in range(self.config.clock_sync_samples):
                if self._stop_event.is_set(): break
                try:
                    response = sampler.fetch()
                    server_ts = _parse_epoch(response.payload["serverTime"])
                    if server_ts is None:
                        raise ValueError("missing Binance serverTime")
                    rtt_ms = response.request_ms
                    local_midpoint_ts = response.received_ts - rtt_ms / 2000.0
                    samples.append(((local_midpoint_ts - server_ts) * 1000.0,
                                    rtt_ms / 2.0 + 1.0, response))  # millisecond timestamp quantization
                except Exception as exc:
                    self._emit("ws_clock_sync_error", repr(exc))
                    if attempt + 1 < self.config.clock_sync_samples:
                        self._stop_event.wait(self.reconnect_delay_seconds(attempt + 1))
        if not samples:
            if required:
                raise QuoteUnavailable("ws_clock_sync_error", "no successful Binance clock-offset sample")
            return False
        offset_ms, uncertainty_ms, selected = min(samples, key=lambda sample: sample[1])
        with self._lock:
            self._clock_offset_ms = offset_ms
            self._clock_uncertainty_ms = uncertainty_ms
            self._last_clock_sync_ts = self.clock()
            self._clock_sync_details = {"transport": selected.transport, "endpoint": self.server_time_url,
                                        "setup_ms": selected.setup_ms, "request_rtt_ms": selected.request_ms,
                                        "uncertainty_ms": uncertainty_ms, "offset_ms": offset_ms,
                                        "sample_count": len(samples), "selected_at": self._last_clock_sync_ts}
        return True

    def _run_session(self) -> None:
        try:
            active_socket = self.connector(self.ws_url, timeout=self.config.timeout_seconds)
            active_socket.settimeout(self.config.ws_receive_timeout_seconds)
        except websocket.WebSocketTimeoutException as exc:
            raise QuoteUnavailable("ws_connect_timeout", repr(exc)) from exc
        except Exception as exc:  # noqa: BLE001 - normalized for the reconnect loop
            raise QuoteUnavailable("ws_disconnect", repr(exc)) from exc
        with self._lock:
            self._socket = active_socket
            self._connection_generation += 1
            self._session_published = False
        try:
            self._sync_clock(required=True)
            bids, asks, snapshot_update_id = self._load_depth_snapshot()
            previous_update_id = snapshot_update_id
            synchronized = False
            while not self._stop_event.is_set():
                try:
                    raw_message = active_socket.recv()
                except websocket.WebSocketTimeoutException as exc:
                    raise QuoteUnavailable("ws_receive_timeout", repr(exc)) from exc
                except Exception as exc:  # noqa: BLE001 - normalized for reconnect
                    raise QuoteUnavailable("ws_disconnect", repr(exc)) from exc
                if not raw_message:
                    raise QuoteUnavailable("ws_disconnect", "Binance WebSocket closed without a message")
                received_ts = self.clock()
                with self._lock:
                    last_clock_sync_ts = self._last_clock_sync_ts
                if received_ts - last_clock_sync_ts >= self.config.clock_resync_seconds:
                    self._sync_clock(required=False)
                try:
                    message = json.loads(raw_message)
                    if message.get("e") != "depthUpdate" or message.get("s") != "BTCUSDT":
                        raise ValueError("unexpected stream event")
                    first_update_id, final_update_id = int(message["U"]), int(message["u"])
                    event_ts = _parse_epoch(message["E"])
                    if event_ts is None:
                        raise ValueError("missing event time E")
                except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise QuoteUnavailable("ws_protocol_error", f"invalid depth event: {exc!r}") from exc
                if final_update_id <= snapshot_update_id:
                    continue
                expected_update_id = previous_update_id + 1
                if not synchronized:
                    if not first_update_id <= expected_update_id <= final_update_id:
                        raise QuoteUnavailable(
                            "ws_sequence_gap",
                            f"first event [{first_update_id},{final_update_id}] does not cover {expected_update_id}",
                        )
                    synchronized = True
                elif first_update_id != expected_update_id:
                    raise QuoteUnavailable(
                        "ws_sequence_gap",
                        f"event begins at {first_update_id}; expected {expected_update_id}",
                    )
                self._apply_updates(bids, message["b"])
                self._apply_updates(asks, message["a"])
                previous_update_id = final_update_id
                if not bids or not asks:
                    raise QuoteUnavailable("ws_protocol_error", "synchronized Binance depth book has an empty side")
                best_bid, best_ask = max(bids), min(asks)
                if best_bid >= best_ask:
                    raise QuoteUnavailable("ws_protocol_error", f"crossed Binance book {best_bid} >= {best_ask}")
                self._publish(
                    BinanceQuote(
                        midpoint=(best_bid + best_ask) / 2.0,
                        source_ts=event_ts,
                        source_ts_kind="payload_E_ms",
                        received_ts=received_ts,
                        request_ms=0.0,
                        update_id=final_update_id,
                        clock_offset_ms=self._clock_offset_ms,
                        clock_uncertainty_ms=self._clock_uncertainty_ms,
                    )
                )
        finally:
            with self._lock:
                self._socket = None
            try:
                active_socket.close()
            except Exception:  # noqa: BLE001 - reconnect loop owns recovery
                pass

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
                self._mark_disconnected(exc.classification)
                self._emit(exc.classification, str(exc), consecutive_failures, delay_seconds * 1000.0)
                if consecutive_failures >= self.config.ws_max_reconnect_attempts:
                    self._emit(
                        "ws_reconnect_exhausted",
                        f"{consecutive_failures} consecutive reconnect attempts failed; continuing at capped backoff",
                        consecutive_failures,
                        delay_seconds * 1000.0,
                    )
                self._stop_event.wait(delay_seconds)

    def reconnect_delay_seconds(self, consecutive_failures: int) -> float:
        """Return an exponential delay with a hard time cap.

        ``ws_max_reconnect_attempts`` is an observability threshold, not a
        backoff ceiling.  Capping the exponent there silently limited the
        default delay to four seconds despite a configured 30-second cap.
        """
        if consecutive_failures < 1:
            raise ValueError("consecutive_failures must be positive")
        initial = self.config.retry_backoff_seconds
        maximum = self.config.ws_max_backoff_seconds
        if initial <= 0.0 or maximum <= 0.0:
            return 0.0
        if initial >= maximum:
            return maximum
        max_exponent = max(0, math.ceil(math.log2(maximum / initial)))
        exponent = min(consecutive_failures - 1, max_exponent)
        return min(initial * (2 ** exponent), maximum)


def value_before(history: Iterable[Tuple[float, float]], target_ts: float) -> Optional[float]:
    selected: Optional[float] = None
    for ts, value in history:
        if ts <= target_ts:
            selected = value
        else:
            break
    return selected


def calculate_features(current_ts: float, up_mid: Optional[float], down_mid: Optional[float], binance_mid: float,
                       up_history: Iterable[Tuple[float, float]], down_history: Iterable[Tuple[float, float]],
                       binance_history: Iterable[Tuple[float, float]], up_book: Book, down_book: Book) -> FeatureRow:
    target_ts = current_ts - HISTORY_SECONDS
    old_up, old_down, old_binance = (value_before(history, target_ts) for history in
                                     (up_history, down_history, binance_history))
    return FeatureRow(None if up_mid is None or old_up is None else up_mid - old_up,
                      None if down_mid is None or old_down is None else down_mid - old_down,
                      None if old_binance is None or old_binance <= 0.0 else math.log(binance_mid / old_binance),
                      up_book.imbalance(3), down_book.imbalance(3))


SCHEMA = """
CREATE TABLE IF NOT EXISTS telemetry_runs (
    run_id TEXT PRIMARY KEY, started_ts REAL NOT NULL, planned_end_ts REAL NOT NULL, config_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS telemetry_snapshots (
    run_id TEXT NOT NULL, ts REAL NOT NULL, day_utc TEXT NOT NULL, episode_id TEXT NOT NULL,
    market_id TEXT NOT NULL, market_slug TEXT NOT NULL, up_mid REAL, down_mid REAL, binance_mid REAL NOT NULL,
    up_delta_10s REAL, down_delta_10s REAL, btc_return_10s REAL, up_imbalance_3 REAL, down_imbalance_3 REAL,
    up_book_top3_json TEXT NOT NULL, down_book_top3_json TEXT NOT NULL,
    up_source_ts REAL, down_source_ts REAL, binance_source_ts REAL,
    up_received_ts REAL, down_received_ts REAL, binance_received_ts REAL,
    up_source_lag_ms REAL, down_source_lag_ms REAL, binance_source_lag_ms REAL,
    system_clock_offset_ms REAL, system_clock_uncertainty_ms REAL,
    up_source_ts_kind TEXT NOT NULL DEFAULT 'unavailable', down_source_ts_kind TEXT NOT NULL DEFAULT 'unavailable',
    binance_source_ts_kind TEXT NOT NULL DEFAULT 'unavailable',
    up_midpoint_missing_reason TEXT, down_midpoint_missing_reason TEXT,
    PRIMARY KEY (run_id, ts, market_id)
);
CREATE INDEX IF NOT EXISTS idx_telemetry_day ON telemetry_snapshots(run_id, day_utc);
CREATE TABLE IF NOT EXISTS telemetry_errors (
    run_id TEXT NOT NULL, ts REAL NOT NULL, stage TEXT NOT NULL, error TEXT NOT NULL, endpoint TEXT,
    classification TEXT NOT NULL DEFAULT 'unknown', attempt INTEGER, max_attempts INTEGER, retry_delay_ms REAL
);
CREATE TABLE IF NOT EXISTS telemetry_cycles (
    run_id TEXT NOT NULL, ts REAL NOT NULL, market_id TEXT NOT NULL, market_fetch_ms REAL NOT NULL,
    polymarket_up_fetch_ms REAL NOT NULL, polymarket_down_fetch_ms REAL NOT NULL, binance_fetch_ms REAL NOT NULL,
    binance_quote_lookup_ms REAL NOT NULL DEFAULT 0,
    parallel_fetch_wall_ms REAL NOT NULL, sqlite_write_ms REAL NOT NULL, other_ms REAL NOT NULL, cycle_ms REAL NOT NULL,
    PRIMARY KEY (run_id, ts, market_id)
);
CREATE TABLE IF NOT EXISTS telemetry_ws_gaps (
    run_id TEXT NOT NULL, disconnected_ts REAL NOT NULL, reconnected_ts REAL NOT NULL,
    duration_ms REAL NOT NULL, reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS telemetry_attempts (
    run_id TEXT NOT NULL, ts REAL NOT NULL, completed_ts REAL NOT NULL,
    successful INTEGER NOT NULL, duration_ms REAL NOT NULL,
    PRIMARY KEY(run_id, ts)
);
"""
_MIGRATIONS = {
    "up_source_ts": "REAL", "down_source_ts": "REAL", "binance_source_ts": "REAL",
    "up_received_ts": "REAL", "down_received_ts": "REAL", "binance_received_ts": "REAL",
    "up_source_lag_ms": "REAL", "down_source_lag_ms": "REAL", "binance_source_lag_ms": "REAL",
    "system_clock_offset_ms": "REAL", "system_clock_uncertainty_ms": "REAL",
    "up_source_ts_kind": "TEXT NOT NULL DEFAULT 'unavailable'", "down_source_ts_kind": "TEXT NOT NULL DEFAULT 'unavailable'",
    "binance_source_ts_kind": "TEXT NOT NULL DEFAULT 'unavailable'", "up_midpoint_missing_reason": "TEXT",
    "down_midpoint_missing_reason": "TEXT",
}
_ERROR_MIGRATIONS = {"endpoint": "TEXT", "classification": "TEXT NOT NULL DEFAULT 'unknown'", "attempt": "INTEGER",
                     "max_attempts": "INTEGER", "retry_delay_ms": "REAL"}
_CYCLE_MIGRATIONS = {"binance_quote_lookup_ms": "REAL NOT NULL DEFAULT 0"}


class TelemetryStore:
    """SQLite persistence restricted to observations and collector health."""
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.executescript(SCHEMA)
        self._migrate_columns("telemetry_snapshots", _MIGRATIONS)
        self._migrate_columns("telemetry_errors", _ERROR_MIGRATIONS)
        self._migrate_columns("telemetry_cycles", _CYCLE_MIGRATIONS)
        self.conn.commit()

    def _migrate_columns(self, table: str, columns: Mapping[str, str]) -> None:
        existing = {row[1] for row in self.conn.execute(f"PRAGMA table_info({table})")}
        for name, definition in columns.items():
            if name not in existing:
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")

    def close(self) -> None:
        self.conn.close()

    def start_run(self, config: TelemetryConfig, started_ts: float) -> str:
        run_id = str(uuid.uuid4())
        self.conn.execute("INSERT INTO telemetry_runs (run_id, started_ts, planned_end_ts, config_json) VALUES (?, ?, ?, ?)",
                          (run_id, started_ts, started_ts + config.effective_duration_seconds, json.dumps(asdict(config))))
        self.conn.commit()
        return run_id

    def active_run(self, config: TelemetryConfig, now_ts: float) -> Optional[Tuple[str, float]]:
        row = self.conn.execute(
            "SELECT run_id,planned_end_ts,config_json FROM telemetry_runs "
            "WHERE planned_end_ts > ? ORDER BY started_ts DESC LIMIT 1",
            (now_ts,),
        ).fetchone()
        if row is None:
            return None
        stored_config = json.loads(row[2])
        if stored_config != asdict(config):
            raise RuntimeError(
                "refusing to resume an active telemetry run with different configuration"
            )
        return str(row[0]), float(row[1])

    def record_snapshot(self, run_id: str, ts: float, market: MarketPair, up_book: Book, down_book: Book,
                        binance: BinanceQuote, features: FeatureRow) -> float:
        day_utc, episode_id = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat(), f"btc-episode-{int(ts // 3600)}"
        started = time.perf_counter()
        self.conn.execute(
            "INSERT OR IGNORE INTO telemetry_snapshots (run_id,ts,day_utc,episode_id,market_id,market_slug,up_mid,down_mid,binance_mid,"
            "up_delta_10s,down_delta_10s,btc_return_10s,up_imbalance_3,down_imbalance_3,up_book_top3_json,down_book_top3_json,"
            "up_source_ts,down_source_ts,binance_source_ts,up_received_ts,down_received_ts,binance_received_ts,up_source_lag_ms,"
            "down_source_lag_ms,binance_source_lag_ms,system_clock_offset_ms,system_clock_uncertainty_ms,"
            "up_source_ts_kind,down_source_ts_kind,binance_source_ts_kind,"
            "up_midpoint_missing_reason,down_midpoint_missing_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (run_id,ts,day_utc,episode_id,market.market_id,market.slug,up_book.midpoint,down_book.midpoint,binance.midpoint,
             features.up_delta_10s,features.down_delta_10s,features.btc_return_10s,features.up_imbalance_3,features.down_imbalance_3,
             json.dumps(up_book.top_levels()),json.dumps(down_book.top_levels()),up_book.source_ts,down_book.source_ts,binance.source_ts,
             up_book.received_ts,down_book.received_ts,binance.received_ts,
             None if up_book.source_lag_ms is None else up_book.source_lag_ms - binance.clock_offset_ms,
             None if down_book.source_lag_ms is None else down_book.source_lag_ms - binance.clock_offset_ms,
             binance.source_lag_ms,binance.clock_offset_ms,binance.clock_uncertainty_ms,
             up_book.source_ts_kind,down_book.source_ts_kind,binance.source_ts_kind,
             up_book.midpoint_missing_reason,down_book.midpoint_missing_reason))
        self.conn.commit()
        return (time.perf_counter() - started) * 1000.0

    def record_cycle(self, run_id: str, ts: float, market_id: str, metrics: Mapping[str, float]) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO telemetry_cycles (run_id,ts,market_id,market_fetch_ms,polymarket_up_fetch_ms,polymarket_down_fetch_ms,"
            "binance_fetch_ms,binance_quote_lookup_ms,parallel_fetch_wall_ms,sqlite_write_ms,other_ms,cycle_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (run_id,ts,market_id,metrics["market_fetch_ms"],metrics["polymarket_up_fetch_ms"],metrics["polymarket_down_fetch_ms"],
             metrics["binance_fetch_ms"],metrics["binance_quote_lookup_ms"],metrics["parallel_fetch_wall_ms"],
             metrics["sqlite_write_ms"],metrics["other_ms"],metrics["cycle_ms"]))
        self.conn.commit()

    def record_ws_gap(self, run_id: str, gap: WsGap) -> None:
        self.conn.execute(
            "INSERT INTO telemetry_ws_gaps (run_id,disconnected_ts,reconnected_ts,duration_ms,reason) VALUES (?,?,?,?,?)",
            (run_id, gap.disconnected_ts, gap.reconnected_ts, gap.duration_ms, gap.reason),
        )
        self.conn.commit()

    def record_error(self, run_id: str, ts: float, stage: str, error: str, *, endpoint: Optional[str] = None,
                     classification: str = "collector_error", attempt: Optional[int] = None,
                     max_attempts: Optional[int] = None, retry_delay_ms: Optional[float] = None) -> None:
        self.conn.execute("INSERT INTO telemetry_errors (run_id,ts,stage,error,endpoint,classification,attempt,max_attempts,retry_delay_ms) "
                          "VALUES (?,?,?,?,?,?,?,?,?)",
                          (run_id,ts,stage,error,endpoint,classification,attempt,max_attempts,retry_delay_ms))
        self.conn.commit()

    def record_fetch_failure(self, run_id: str, ts: float, failure: AttemptFailure) -> None:
        self.record_error(run_id, ts, failure.stage, failure.detail, endpoint=failure.endpoint,
                          classification=failure.classification, attempt=failure.attempt,
                          max_attempts=failure.max_attempts, retry_delay_ms=failure.retry_delay_ms)


class Collector:
    def __init__(self, config: TelemetryConfig, store: TelemetryStore,
                 binance_feed: Optional[BinanceDepthFeed] = None) -> None:
        self.config, self.store, self.market, self.last_market_refresh = config, store, None, 0.0
        if binance_feed is not None:
            self.binance_feed = binance_feed
        elif config.binance_profile == "market-data":
            self.binance_feed = BinanceDepthFeed(
                config, ws_url=BINANCE_MARKET_DATA_WS_URL,
                depth_snapshot_url=BINANCE_MARKET_DATA_DEPTH_URL,
                server_time_url=BINANCE_MARKET_DATA_TIME_URL,
            )
        elif config.binance_profile == "primary":
            self.binance_feed = BinanceDepthFeed(config)
        else:
            raise ValueError("unknown Binance profile")
        self.up_history: Deque[Tuple[float, float]] = deque()
        self.down_history: Deque[Tuple[float, float]] = deque()
        self.binance_history: Deque[Tuple[float, float]] = deque()

    def start(self) -> None:
        self.binance_feed.start()

    def close(self) -> None:
        self.binance_feed.stop()

    def _drain_feed_events(self, run_id: str) -> None:
        for diagnostic in self.binance_feed.drain_diagnostics():
            self.store.record_error(
                run_id,
                diagnostic.ts,
                "binance_ws",
                diagnostic.detail,
                endpoint=self.binance_feed.ws_url,
                classification=diagnostic.classification,
                attempt=diagnostic.attempt,
                max_attempts=self.config.ws_max_reconnect_attempts,
                retry_delay_ms=diagnostic.retry_delay_ms,
            )
        for gap in self.binance_feed.drain_gaps():
            self.store.record_ws_gap(run_id, gap)

    def _refresh_market(self, now_ts: float) -> Tuple[MarketPair, float]:
        if self.market is None or now_ts >= self.market.close_ts or now_ts - self.last_market_refresh >= self.config.market_refresh_seconds:
            market, response = fetch_current_market(self.config, now_ts)
            if self.market is None or market.market_id != self.market.market_id:
                self.up_history.clear(); self.down_history.clear(); self.binance_history.clear()
            self.market, self.last_market_refresh = market, now_ts
            return market, response.request_ms
        return self.market, 0.0

    def _record_fetch_failures(self, run_id: str, ts: float, failure: FetchFailure) -> None:
        for attempt in failure.failures:
            self.store.record_fetch_failure(run_id, ts, attempt)

    def tick(self, run_id: str, now_ts: Optional[float] = None) -> bool:
        poll_ts = utc_now_ts() if now_ts is None else now_ts
        started, successful = time.perf_counter(), False
        try:
            successful = self._tick(run_id, poll_ts)
            return successful
        finally:
            self.store.conn.execute("INSERT INTO telemetry_attempts VALUES (?,?,?,?,?)",
                                    (run_id, poll_ts, utc_now_ts(), int(successful),
                                     (time.perf_counter() - started) * 1000.0))
            self.store.conn.commit()

    def _tick(self, run_id: str, now_ts: Optional[float] = None) -> bool:
        poll_ts = utc_now_ts() if now_ts is None else now_ts
        cycle_started = time.perf_counter()
        try:
            self._drain_feed_events(run_id)
            try:
                market, market_fetch_ms = self._refresh_market(poll_ts)
            except FetchFailure as exc:
                self._record_fetch_failures(run_id, poll_ts, exc)
                return False
            parallel_started = time.perf_counter()
            with ThreadPoolExecutor(max_workers=2, thread_name_prefix="telemetry-fetch") as executor:
                futures = {"up": executor.submit(fetch_book, market.up_token_id, self.config, "polymarket_up_book"),
                           "down": executor.submit(fetch_book, market.down_token_id, self.config, "polymarket_down_book")}
                results: Dict[str, Any] = {}
                failures: List[FetchFailure] = []
                for name, future in futures.items():
                    try:
                        results[name] = future.result()
                    except FetchFailure as exc:
                        failures.append(exc)
                if failures:
                    for failure in failures:
                        self._record_fetch_failures(run_id, poll_ts, failure)
                    return False
            parallel_fetch_wall_ms = (time.perf_counter() - parallel_started) * 1000.0
            lookup_started = time.perf_counter()
            try:
                binance = self.binance_feed.latest_quote()
            except QuoteUnavailable as exc:
                self.store.record_error(
                    run_id, poll_ts, "binance_ws_quote", str(exc), endpoint=self.binance_feed.ws_url,
                    classification=exc.classification,
                )
                self._drain_feed_events(run_id)
                return False
            binance_quote_lookup_ms = (time.perf_counter() - lookup_started) * 1000.0
            up_book, down_book = results["up"], results["down"]
            features = calculate_features(poll_ts, up_book.midpoint, down_book.midpoint, binance.midpoint,
                                          self.up_history, self.down_history, self.binance_history, up_book, down_book)
            sqlite_write_ms = self.store.record_snapshot(run_id, poll_ts, market, up_book, down_book, binance, features)
            cycle_ms = (time.perf_counter() - cycle_started) * 1000.0
            self.store.record_cycle(run_id, poll_ts, market.market_id, {
                "market_fetch_ms": market_fetch_ms, "polymarket_up_fetch_ms": up_book.request_ms or 0.0,
                "polymarket_down_fetch_ms": down_book.request_ms or 0.0, "binance_fetch_ms": 0.0,
                "binance_quote_lookup_ms": binance_quote_lookup_ms,
                "parallel_fetch_wall_ms": parallel_fetch_wall_ms, "sqlite_write_ms": sqlite_write_ms,
                "other_ms": max(0.0, cycle_ms - market_fetch_ms - parallel_fetch_wall_ms - sqlite_write_ms
                                - binance_quote_lookup_ms), "cycle_ms": cycle_ms})
            if up_book.midpoint is not None: self.up_history.append((poll_ts, up_book.midpoint))
            if down_book.midpoint is not None: self.down_history.append((poll_ts, down_book.midpoint))
            self.binance_history.append((poll_ts, binance.midpoint)); self._trim_history(poll_ts)
            self._drain_feed_events(run_id)
            return True
        except Exception as exc:  # noqa: BLE001 - collector records and survives unexpected faults
            self.store.record_error(run_id, poll_ts, "collector", repr(exc), classification=classify_error(exc))
            return False

    def _trim_history(self, now_ts: float) -> None:
        cutoff = now_ts - 2 * HISTORY_SECONDS
        for history in (self.up_history, self.down_history, self.binance_history):
            while history and history[0][0] < cutoff:
                history.popleft()


def _distribution(values: Sequence[Optional[float]]) -> Dict[str, Optional[float]]:
    present = sorted(float(value) for value in values if value is not None)
    if not present:
        return {"n": 0, "p50": None, "p95": None, "max": None}
    return {"n": len(present), "p50": round(statistics.median(present), 3),
            "p95": round(present[max(0, math.ceil(len(present) * .95) - 1)], 3), "max": round(present[-1], 3)}


def _source_report(rows: Sequence[sqlite3.Row], source: str) -> Dict[str, Any]:
    lag_key, ts_key, kind_key = f"{source}_source_lag_ms", f"{source}_source_ts", f"{source}_source_ts_kind"
    timestamped = [row for row in rows if row[ts_key] is not None]
    payload_rows = [row for row in timestamped if str(row[kind_key]).startswith("payload_")]
    fresh = [row for row in payload_rows if row[lag_key] is not None and 0 <= row[lag_key] <= FRESHNESS_THRESHOLD_MS]
    return {"rows": len(rows), "source_timestamp_rows": len(timestamped),
            "timestamp_kinds": dict(Counter(row[kind_key] for row in timestamped)),
            "lag_ms": _distribution([row[lag_key] for row in timestamped]),
            "payload_timestamp_rows": len(payload_rows), "payload_timestamp_within_750ms": len(fresh),
            "payload_timestamp_within_750ms_pct": None if not payload_rows else round(100 * len(fresh) / len(payload_rows), 3),
            "freshness_validatable": bool(payload_rows),
            "denominator": "saved_payload_timestamp_rows_not_all_collection_attempts"}


def _midpoint_missing_analysis(snapshots: Sequence[sqlite3.Row], error_times: Sequence[float]) -> Dict[str, Any]:
    missing = [row for row in snapshots if row["up_midpoint_missing_reason"] or row["down_midpoint_missing_reason"]]
    clusters: List[Tuple[float, float, int]] = []
    for row in missing:
        ts = float(row["ts"])
        if not clusters or ts - clusters[-1][1] > 2.0:
            clusters.append((ts, ts, 1))
        else:
            start, _, count = clusters[-1]
            clusters[-1] = (start, ts, count + 1)
    near_error = 0
    sorted_errors = sorted(float(ts) for ts in error_times)
    error_index = 0
    latest_error: Optional[float] = None
    for row in missing:
        ts = float(row["ts"])
        while error_index < len(sorted_errors) and sorted_errors[error_index] <= ts:
            latest_error = sorted_errors[error_index]
            error_index += 1
        if latest_error is not None and ts - latest_error <= 10.0:
            near_error += 1
    return {
        "rows_with_any_missing": len(missing),
        "rows_with_any_missing_pct": None if not snapshots else round(100.0 * len(missing) / len(snapshots), 3),
        "rows_with_both_missing": sum(
            bool(row["up_midpoint_missing_reason"] and row["down_midpoint_missing_reason"]) for row in missing
        ),
        "cluster_count": len(clusters),
        "cluster_duration_seconds": _distribution([end - start for start, end, _ in clusters]),
        "cluster_size_rows": _distribution([count for _, _, count in clusters]),
        "rows_within_10s_after_collector_error": near_error,
        "rows_within_10s_after_collector_error_pct": None if not missing else round(100.0 * near_error / len(missing), 3),
    }


def telemetry_report(conn: sqlite3.Connection, run_id: Optional[str] = None) -> Dict[str, Any]:
    """Return raw frequency and feed-quality diagnostics; never outcome or PnL."""
    conn.row_factory = sqlite3.Row
    where, params = ("", ()) if run_id is None else ("WHERE run_id = ?", (run_id,))
    daily = conn.execute(
        "SELECT day_utc,COUNT(*) AS snapshots,COUNT(DISTINCT episode_id) AS episodes,"
        "SUM(CASE WHEN ABS(up_delta_10s)>=.02 OR ABS(down_delta_10s)>=.02 THEN 1 ELSE 0 END) AS shock_2c,"
        "SUM(CASE WHEN ABS(up_delta_10s)>=.03 OR ABS(down_delta_10s)>=.03 THEN 1 ELSE 0 END) AS shock_3c,"
        "SUM(CASE WHEN ABS(up_delta_10s)>=.04 OR ABS(down_delta_10s)>=.04 THEN 1 ELSE 0 END) AS shock_4c,"
        "SUM(CASE WHEN ABS(btc_return_10s)<=.0004 THEN 1 ELSE 0 END) AS binance_neutral_4bps "
        f"FROM telemetry_snapshots {where} GROUP BY day_utc ORDER BY day_utc", params).fetchall()
    snapshots = conn.execute(f"SELECT * FROM telemetry_snapshots {where} ORDER BY ts", params).fetchall()
    cycles = conn.execute(f"SELECT * FROM telemetry_cycles {where} ORDER BY ts", params).fetchall()
    ws_gaps = conn.execute(f"SELECT * FROM telemetry_ws_gaps {where} ORDER BY disconnected_ts", params).fetchall()
    errors = conn.execute(f"SELECT stage,endpoint,classification,COUNT(*) AS count FROM telemetry_errors {where} "
                          "GROUP BY stage,endpoint,classification ORDER BY count DESC,stage", params).fetchall()
    error_events = conn.execute(f"SELECT ts FROM telemetry_errors {where} ORDER BY ts", params).fetchall()
    midpoint = conn.execute(
        f"SELECT side,reason,COUNT(*) AS count FROM (SELECT 'up' AS side,up_midpoint_missing_reason AS reason FROM telemetry_snapshots {where} "
        f"UNION ALL SELECT 'down' AS side,down_midpoint_missing_reason AS reason FROM telemetry_snapshots {where}) "
        "WHERE reason IS NOT NULL GROUP BY side,reason ORDER BY count DESC", params + params).fetchall()
    gaps = [snapshots[index]["ts"] - snapshots[index - 1]["ts"] for index in range(1, len(snapshots))]
    timing_fields = ("market_fetch_ms", "polymarket_up_fetch_ms", "polymarket_down_fetch_ms", "binance_fetch_ms",
                     "binance_quote_lookup_ms",
                     "parallel_fetch_wall_ms", "sqlite_write_ms", "other_ms", "cycle_ms")
    binance_source = _source_report(snapshots, "binance")
    if binance_source["freshness_validatable"]:
        binance_source["source"] = "Binance diff-depth WebSocket event time E"
    else:
        binance_source["limitation"] = (
            "No millisecond price event timestamp is present; HTTP Date is diagnostic only and cannot validate 750ms freshness."
        )
    return {"run_id": run_id, "daily": [dict(row) for row in daily],
            "cycle_timing_ms": {field: _distribution([row[field] for row in cycles]) for field in timing_fields},
            "snapshot_gaps_seconds": {**_distribution(gaps), "over_10s": sum(gap > 10 for gap in gaps)},
            "source_freshness": {"threshold_ms": FRESHNESS_THRESHOLD_MS,
              "clock_sync": {"offset_ms": _distribution([row["system_clock_offset_ms"] for row in snapshots]),
                             "uncertainty_ms": _distribution([row["system_clock_uncertainty_ms"] for row in snapshots])},
              "polymarket_up": _source_report(snapshots, "up"), "polymarket_down": _source_report(snapshots, "down"),
              "binance": binance_source},
            "websocket_gaps": {"count": len(ws_gaps),
                               "duration_ms": _distribution([row["duration_ms"] for row in ws_gaps]),
                               "by_reason": dict(Counter(row["reason"] for row in ws_gaps))},
            "midpoint_missing": [dict(row) for row in midpoint], "errors": [dict(row) for row in errors],
            "midpoint_missing_analysis": _midpoint_missing_analysis(snapshots, [row["ts"] for row in error_events]),
            "notes": ["Rows and raw shocks are not independent observations; episode-level analysis remains required.",
                      "No outcome, resolution, PnL, wallet, or order data is stored."]}


def run_command(args: argparse.Namespace) -> int:
    config = TelemetryConfig(args.db, args.poll_seconds, args.duration_days, args.timeout_seconds,
                             max_fetch_attempts=args.max_fetch_attempts, retry_backoff_seconds=args.retry_backoff_seconds,
                             duration_seconds=args.duration_seconds, binance_profile=args.binance_profile)
    store = TelemetryStore(Path(config.db_path)); started_ts = utc_now_ts()
    active = store.active_run(config, started_ts) if args.resume_latest else None
    if active is None:
        run_id = store.start_run(config, started_ts)
        deadline = started_ts + config.effective_duration_seconds
        mode = "started"
    else:
        run_id, deadline = active
        previous_ts = store.conn.execute(
            "SELECT MAX(ts) FROM telemetry_snapshots WHERE run_id = ?", (run_id,)
        ).fetchone()[0]
        gap_seconds = None if previous_ts is None else max(0.0, started_ts - float(previous_ts))
        store.record_error(
            run_id, started_ts, "collector", f"process resumed; prior snapshot gap={gap_seconds}",
            classification="collector_resume",
        )
        mode = "resumed"
    collector = Collector(config, store)
    print(f"telemetry {mode} run_id={run_id} scheduled_end={datetime.fromtimestamp(deadline, tz=timezone.utc).isoformat()}", flush=True)
    try:
        collector.start()
        while utc_now_ts() < deadline:
            cycle_started = time.perf_counter(); collector.tick(run_id)
            time.sleep(max(0.0, config.poll_seconds - (time.perf_counter() - cycle_started)))
    except KeyboardInterrupt:
        print("telemetry interrupted by user", flush=True)
    finally:
        collector.close()
        collector._drain_feed_events(run_id)
        store.close()
    return 0


def report_command(args: argparse.Namespace) -> int:
    conn = sqlite3.connect(args.db)
    try:
        print(json.dumps(telemetry_report(conn, args.run_id), ensure_ascii=False, indent=2))
    finally:
        conn.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="BTC 15m telemetry-only collector")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--db", default="data/telemetry.sqlite3"); run_parser.add_argument("--duration-days", type=int, default=14)
    run_parser.add_argument("--poll-seconds", type=float, default=1.0); run_parser.add_argument("--timeout-seconds", type=float, default=5.0)
    run_parser.add_argument("--max-fetch-attempts", type=int, default=3); run_parser.add_argument("--retry-backoff-seconds", type=float, default=0.25)
    run_parser.add_argument("--duration-seconds", type=float, help="short, explicit duration for an acceptance run")
    run_parser.add_argument("--binance-profile", choices=("primary", "market-data"), default="primary")
    run_parser.add_argument("--resume-latest", action="store_true", help="resume the latest active run only when its saved configuration matches")
    run_parser.set_defaults(handler=run_command)
    report_parser = subparsers.add_parser("report"); report_parser.add_argument("--db", default="data/telemetry.sqlite3")
    report_parser.add_argument("--run-id"); report_parser.set_defaults(handler=report_command)
    args = parser.parse_args()
    if getattr(args, "duration_days", 1) < 1: parser.error("--duration-days must be positive")
    if getattr(args, "poll_seconds", 1.0) <= 0: parser.error("--poll-seconds must be positive")
    if getattr(args, "max_fetch_attempts", 1) < 1: parser.error("--max-fetch-attempts must be positive")
    if getattr(args, "retry_backoff_seconds", 0.0) < 0: parser.error("--retry-backoff-seconds must be non-negative")
    if getattr(args, "duration_seconds", None) is not None and args.duration_seconds <= 0: parser.error("--duration-seconds must be positive")
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
