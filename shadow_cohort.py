"""Forward-only shadow measurement for the BTC microstructure hypothesis.

This module has no wallet, credential, order, or scheduler code. It can only
write a separately approved shadow cohort. Existing telemetry is deliberately
not replayed into this store because it lacks complete decision-time evidence.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import math
from pathlib import Path
import random
import re
import sqlite3
from statistics import NormalDist, mean
from types import MappingProxyType
from typing import Callable, Mapping, Optional, Sequence, Union
from urllib.request import Request, urlopen
from research_validation import day_cluster_sensitivity


STRATEGY_VERSION = "microstructure-reversion-v1"
COHORT_NAME_RE = re.compile(r"attribution-forward-\d{8}-reversion-v1")
FRESHNESS_MS = 750.0
FEATURE_WINDOW_SECONDS = 10.0
FEATURE_BASELINE_MAX_STALENESS_SECONDS = 1.0
MICROSHOCK = 0.04
BINANCE_NEUTRALITY = 0.0004
IMBALANCE_MAX = -0.20
ENTRY_CAP = Decimal(".48")
SPREAD_CAP = Decimal(".015")
DEPTH_MIN = Decimal("20")
SHADOW_ORDER_SHARES = Decimal("1")
NEAR_TOUCH_OFFSET = Decimal(".01")
COOLDOWN_SECONDS = 60.0
MIN_CALENDAR_DAYS = 30
MIN_RESOLVED_CANDIDATES = 100
MIN_RESOLVED_EPISODES = 30
FEASIBILITY_STOP_DAYS = 60
BOOTSTRAP_SAMPLES = 4_000
MAX_HEARTBEAT_GAP_SECONDS = 300.0

LOCKED_PARAMETERS: Mapping[str, object] = MappingProxyType({
    "freshness_ms": FRESHNESS_MS,
    "feature_window_seconds": FEATURE_WINDOW_SECONDS,
    "feature_baseline_max_staleness_seconds": FEATURE_BASELINE_MAX_STALENESS_SECONDS,
    "microshock_delta_10s": MICROSHOCK,
    "binance_neutrality_log_return_10s": BINANCE_NEUTRALITY,
    "shocked_book_imbalance_max": IMBALANCE_MAX,
    "book_levels": 3,
    "entry_ask_cap": float(ENTRY_CAP),
    "entry_spread_cap": float(SPREAD_CAP),
    "entry_near_touch_depth_min": float(DEPTH_MIN),
    "shadow_order_shares": float(SHADOW_ORDER_SHARES),
    "entry_max_price_offset": float(NEAR_TOUCH_OFFSET),
    "window_after_open_min_seconds": 90,
    "window_after_open_max_seconds": 720,
    "window_before_close_min_seconds": 180,
    "market_cooldown_seconds": COOLDOWN_SECONDS,
    "episode_seconds": 3600,
    "minimum_calendar_days": MIN_CALENDAR_DAYS,
    "minimum_resolved_candidates": MIN_RESOLVED_CANDIDATES,
    "minimum_resolved_episodes": MIN_RESOLVED_EPISODES,
    "feasibility_stop_calendar_days": FEASIBILITY_STOP_DAYS,
    "max_heartbeat_gap_seconds": MAX_HEARTBEAT_GAP_SECONDS,
    "stability_groups": ("episode", "utc_day"),
})


def _finite(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _decimal(value: object) -> Optional[Decimal]:
    if isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return result if result.is_finite() else None


def _basis_points(value: object) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if 0 <= value <= 10_000 else None
    if isinstance(value, str) and re.fullmatch(r"\d+", value.strip()):
        parsed = int(value)
        return parsed if parsed <= 10_000 else None
    return None


@dataclass(frozen=True)
class Verdict:
    passed: bool
    reason: str


@dataclass(frozen=True)
class Level:
    price: float
    size: float


@dataclass(frozen=True)
class FillEstimate:
    shares: Decimal
    vwap: Decimal
    notional: Decimal
    slippage_from_best_ask: Decimal


@dataclass(frozen=True)
class Book:
    """Full book in exchange order, with payload source time for this book."""

    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    source_ts: Optional[float]
    source_ts_kind: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "bids", tuple(self.bids))
        object.__setattr__(self, "asks", tuple(self.asks))

    def validation_reason(self) -> Optional[str]:
        for name, levels, reverse in (("bids", self.bids, True), ("asks", self.asks, False)):
            if not levels:
                return f"missing_{name}"
            prices: list[Decimal] = []
            for level in levels:
                price, size = _decimal(level.price), _decimal(level.size)
                if price is None or size is None or not Decimal("0") < price <= Decimal("1") or size <= 0:
                    return f"invalid_{name}_level"
                prices.append(price)
            if len(set(prices)) != len(prices):
                return f"duplicate_{name}_price"
            if any((a <= b if reverse else a >= b) for a, b in zip(prices, prices[1:])):
                return f"unsorted_{name}"
        # A locked book is not an executable one-sided quote either: accepting
        # bid == ask as a valid taker price would hide a crossed/stale update.
        if _decimal(self.bids[0].price) >= _decimal(self.asks[0].price):
            return "crossed_or_locked_book"
        return None

    @property
    def best_bid(self) -> Optional[float]:
        return None if self.validation_reason() else self.bids[0].price

    @property
    def best_ask(self) -> Optional[float]:
        return None if self.validation_reason() else self.asks[0].price

    @property
    def midpoint(self) -> Optional[float]:
        bid, ask = self.best_bid, self.best_ask
        return None if bid is None or ask is None else (bid + ask) / 2.0

    def imbalance(self, count: int = 3) -> Optional[float]:
        if self.validation_reason():
            return None
        bid_notional = sum(level.price * level.size for level in self.bids[:count])
        ask_notional = sum(level.price * level.size for level in self.asks[:count])
        total = bid_notional + ask_notional
        return None if total <= 0 else (bid_notional - ask_notional) / total

    def near_touch_notional(self) -> Optional[Decimal]:
        best_ask = _decimal(self.best_ask)
        if best_ask is None:
            return None
        total = Decimal("0")
        # The imbalance intentionally uses only three levels.  Available
        # entry liquidity is a different criterion: it includes every visible
        # ask within the pre-registered near-touch price band.
        for level in self.asks:
            price, size = _decimal(level.price), _decimal(level.size)
            if price is not None and size is not None and price <= best_ask + NEAR_TOUCH_OFFSET:
                total += price * size
        return total

    def fill_estimate(self, shares: Decimal = SHADOW_ORDER_SHARES) -> Optional[FillEstimate]:
        best_ask = _decimal(self.best_ask)
        if best_ask is None or shares <= 0:
            return None
        remaining, notional = shares, Decimal("0")
        for level in self.asks:
            price, available = _decimal(level.price), _decimal(level.size)
            if price is None or available is None or price > best_ask + NEAR_TOUCH_OFFSET:
                break
            take = min(remaining, available)
            notional += take * price
            remaining -= take
            if remaining == 0:
                vwap = notional / shares
                return FillEstimate(shares, vwap, notional, vwap - best_ask)
        return None

    def payload(self) -> dict[str, object]:
        return {"bids": [asdict(level) for level in self.bids], "asks": [asdict(level) for level in self.asks],
                "source_ts": self.source_ts, "source_ts_kind": self.source_ts_kind}


@dataclass(frozen=True)
class FeatureBaseline:
    """The latest complete observation available at or before t - 10 seconds."""

    observed_ts: float
    up_mid: Optional[float]
    down_mid: Optional[float]
    binance_mid: Optional[float]


def choose_feature_baseline(history: Sequence[FeatureBaseline], decision_ts: float) -> Optional[FeatureBaseline]:
    """Select no future value and permit at most one second of anchor staleness."""
    if not _finite(decision_ts):
        return None
    target = decision_ts - FEATURE_WINDOW_SECONDS
    candidates = [sample for sample in history if _finite(sample.observed_ts) and sample.observed_ts <= target]
    if not candidates:
        return None
    selected = max(candidates, key=lambda sample: sample.observed_ts)
    return selected if target - selected.observed_ts <= FEATURE_BASELINE_MAX_STALENESS_SECONDS else None


@dataclass(frozen=True)
class MarketState:
    """Raw market state read at decision time, including the fee schedule."""

    market_id: str
    market_slug: str
    open_ts: float
    close_ts: float
    active: bool
    accepting_orders: bool
    order_book_enabled: bool
    outcomes: tuple[str, ...]
    up_token_id: str
    down_token_id: str
    taker_fee_bps: Optional[Union[int, str]]
    fee_schedule: Optional["FeeSchedule"] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "outcomes", tuple(str(value).upper() for value in self.outcomes))

    @property
    def taker_fee_rate(self) -> Optional[Decimal]:
        if self.fee_schedule is not None:
            return self.fee_schedule.rate
        bps = _basis_points(self.taker_fee_bps)
        if bps is None:
            return None
        return Decimal(bps) / Decimal("10000")

    def taker_fee_per_share(self, price: Decimal) -> Optional[Decimal]:
        if self.fee_schedule is not None:
            return self.fee_schedule.fee_per_share(price)
        rate = self.taker_fee_rate
        if rate is None or not Decimal("0") < price < Decimal("1"):
            return None
        return rate * price * (Decimal("1") - price)


@dataclass(frozen=True)
class FeeSchedule:
    """Current Gamma fee configuration for a marketable/taker order."""

    enabled: bool
    rate: Decimal
    exponent: int
    taker_only: bool

    def fee_per_share(self, price: Decimal) -> Optional[Decimal]:
        if not Decimal("0") < price < Decimal("1") or self.rate < 0 or self.exponent < 1:
            return None
        if not self.enabled:
            return Decimal("0")
        # Polymarket defines the base price component as p * (1-p); Gamma's
        # exponent is applied to that component.
        return self.rate * (price * (Decimal("1") - price)) ** self.exponent


def _fee_schedule_from_gamma(payload: Mapping[str, object]) -> Optional[FeeSchedule]:
    enabled = payload.get("feesEnabled")
    if enabled is False:
        return FeeSchedule(False, Decimal("0"), 1, True)
    if enabled is not True:
        return None
    raw = payload.get("feeSchedule")
    if not isinstance(raw, Mapping):
        return None
    rate = _decimal(raw.get("rate"))
    exponent = raw.get("exponent")
    taker_only = raw.get("takerOnly")
    if rate is None or not Decimal("0") <= rate <= Decimal("1"):
        return None
    if isinstance(exponent, bool) or not isinstance(exponent, int) or not 1 <= exponent <= 8:
        return None
    if type(taker_only) is not bool:
        return None
    return FeeSchedule(True, rate, exponent, taker_only)


def market_state_from_gamma(payload: Mapping[str, object]) -> MarketState:
    """Create auditable decision-time state from Gamma's market object.

    Current Gamma responses carry ``feesEnabled`` and ``feeSchedule``. A
    missing or malformed schedule blocks an entry instead of becoming a
    silent zero-fee assumption.
    """
    try:
        slug = str(payload["slug"])
        match = re.fullmatch(r"btc-updown-15m-(\d+)", slug)
        if match is None:
            raise ValueError("unexpected_market_slug")
        outcomes_raw, token_ids_raw = payload["outcomes"], payload["clobTokenIds"]
        outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
        token_ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else token_ids_raw
        if not isinstance(outcomes, list) or not isinstance(token_ids, list) or len(outcomes) != 2 or len(token_ids) != 2:
            raise ValueError("invalid_outcome_token_pair")
        normalized = [str(value).upper() for value in outcomes]
        index = {outcome: position for position, outcome in enumerate(normalized)}
        if set(index) != {"UP", "DOWN"}:
            raise ValueError("outcomes_not_up_down")
        fee_schedule = _fee_schedule_from_gamma(payload)
        if fee_schedule is None:
            raise ValueError("missing_or_invalid_fee_schedule")
        return MarketState(
            str(payload["id"]), slug, float(match[1]), float(match[1]) + 900.0,
            payload.get("active") is True, payload.get("acceptingOrders") is True,
            payload.get("enableOrderBook") is True, tuple(normalized), str(token_ids[index["UP"]]),
            str(token_ids[index["DOWN"]]), None, fee_schedule,
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid_gamma_market_state:{type(exc).__name__}") from exc


@dataclass(frozen=True)
class ShadowSnapshot:
    """Complete, pre-outcome evidence for a decision after all inputs arrived."""

    ts: float
    market: MarketState
    up_book: Book
    down_book: Book
    binance_mid: Optional[float]
    binance_source_ts: Optional[float]
    binance_source_ts_kind: str
    system_clock_offset_ms: Optional[float]
    system_clock_uncertainty_ms: Optional[float]
    baseline: Optional[FeatureBaseline]
    previous_accepted_ts: Optional[float] = None

    @property
    def feature_age_seconds(self) -> Optional[float]:
        if self.baseline is None or not _finite(self.ts) or not _finite(self.baseline.observed_ts):
            return None
        return self.ts - self.baseline.observed_ts

    @property
    def baseline_valid(self) -> bool:
        age = self.feature_age_seconds
        return age is not None and FEATURE_WINDOW_SECONDS <= age <= FEATURE_WINDOW_SECONDS + FEATURE_BASELINE_MAX_STALENESS_SECONDS

    @property
    def up_delta_10s(self) -> Optional[float]:
        if not self.baseline_valid or self.baseline is None or not _finite(self.up_book.midpoint) or not _finite(self.baseline.up_mid):
            return None
        return self.up_book.midpoint - self.baseline.up_mid

    @property
    def down_delta_10s(self) -> Optional[float]:
        if not self.baseline_valid or self.baseline is None or not _finite(self.down_book.midpoint) or not _finite(self.baseline.down_mid):
            return None
        return self.down_book.midpoint - self.baseline.down_mid

    @property
    def btc_return_10s(self) -> Optional[float]:
        if not self.baseline_valid or self.baseline is None or not _finite(self.binance_mid) or not _finite(self.baseline.binance_mid):
            return None
        if self.binance_mid <= 0 or self.baseline.binance_mid <= 0:
            return None
        return math.log(self.binance_mid / self.baseline.binance_mid)

    @property
    def candidate_side(self) -> Optional[str]:
        up, down = self.up_delta_10s, self.down_delta_10s
        if not _finite(up) or not _finite(down):
            return None
        up_shock, down_shock = up >= MICROSHOCK, down >= MICROSHOCK
        if up_shock == down_shock:
            return None
        return "DOWN" if up_shock else "UP"

    @property
    def shocked_side(self) -> Optional[str]:
        return {"UP": "DOWN", "DOWN": "UP"}.get(self.candidate_side)

    @property
    def candidate_book(self) -> Optional[Book]:
        return {"UP": self.up_book, "DOWN": self.down_book}.get(self.candidate_side)

    @property
    def entry_fill(self) -> Optional[FillEstimate]:
        return None if self.candidate_book is None else self.candidate_book.fill_estimate()

    @property
    def entry_fee_per_share(self) -> Optional[Decimal]:
        fill = self.entry_fill
        if fill is None:
            return None
        return self.market.taker_fee_per_share(fill.vwap)

    @property
    def episode_id(self) -> Optional[str]:
        return None if not _finite(self.ts) else f"btc-episode-{int(self.ts // 3600)}"


@dataclass(frozen=True)
class CohortDefinition:
    cohort_name: str
    strategy_version: str
    parameters: Mapping[str, object]
    approval_reference: str
    approved_ts: float
    collection_started_ts: float

    def __post_init__(self) -> None:
        if not COHORT_NAME_RE.fullmatch(self.cohort_name):
            raise ValueError("cohort_name must use attribution-forward-YYYYMMDD-reversion-v1")
        if self.strategy_version != STRATEGY_VERSION:
            raise ValueError("strategy_version must match the reviewed filter registry")
        if not self.approval_reference.strip():
            raise ValueError("approval_reference is required")
        if not _finite(self.approved_ts) or not _finite(self.collection_started_ts):
            raise ValueError("approval and collection start timestamps must be finite")
        if self.collection_started_ts < self.approved_ts:
            raise ValueError("collection cannot begin before approval")
        if dict(self.parameters) != dict(LOCKED_PARAMETERS):
            raise ValueError("parameters must exactly match LOCKED_PARAMETERS; a changed hypothesis needs a reviewed version")
        object.__setattr__(self, "parameters", MappingProxyType(dict(self.parameters)))

    @property
    def parameters_json(self) -> str:
        return json.dumps(dict(self.parameters), sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    @property
    def parameters_sha256(self) -> str:
        return sha256(self.parameters_json.encode("utf-8")).hexdigest()


def _source_fresh(decision_ts: object, source_ts: object, source_kind: object,
                  clock_offset_ms: object, uncertainty_ms: object) -> tuple[bool, str]:
    if not _finite(decision_ts) or not _finite(source_ts) or not str(source_kind).startswith("payload_"):
        return False, "missing_payload_source_timestamp"
    if not _finite(clock_offset_ms) or not _finite(uncertainty_ms) or uncertainty_ms < 0:
        return False, "missing_clock_uncertainty"
    # Binance's event time is on the exchange clock.  ``clock_offset_ms`` is
    # local minus exchange time, measured by the synchronized feed, so remove
    # it before applying the freshness threshold.  Polymarket source times use
    # zero adjustment because this collector has no equivalent clock sync.
    lag_ms = (float(decision_ts) - float(source_ts)) * 1000.0 - float(clock_offset_ms)
    if lag_ms < 0:
        return False, "negative_source_lag"
    if lag_ms + float(uncertainty_ms) > FRESHNESS_MS:
        return False, "stale_after_clock_uncertainty"
    return True, "fresh_within_750ms"


def market_integrity(snapshot: ShadowSnapshot) -> Verdict:
    market = snapshot.market
    identity = (bool(market.market_id), bool(market.market_slug), _finite(market.open_ts), _finite(market.close_ts),
                bool(market.up_token_id), bool(market.down_token_id))
    if not all(identity) or market.up_token_id == market.down_token_id:
        return Verdict(False, "invalid_market_identity_or_token_pair")
    if not (market.active and market.accepting_orders and market.order_book_enabled):
        return Verdict(False, "market_not_tradable_at_snapshot")
    if len(market.outcomes) != 2 or set(market.outcomes) != {"UP", "DOWN"}:
        return Verdict(False, "outcomes_not_exact_up_down_pair")
    if not _finite(snapshot.ts) or not market.open_ts < snapshot.ts < market.close_ts:
        return Verdict(False, "observation_outside_market_lifetime")
    for name, book in (("up", snapshot.up_book), ("down", snapshot.down_book)):
        if reason := book.validation_reason():
            return Verdict(False, f"{name}_{reason}")
    if not _finite(snapshot.binance_mid):
        return Verdict(False, "missing_binance_mid")
    if snapshot.entry_fill is None or snapshot.entry_fee_per_share is None:
        return Verdict(False, "missing_or_invalid_fee_schedule")
    if snapshot.candidate_side is None:
        return Verdict(False, "missing_or_ambiguous_directional_candidate")
    return Verdict(True, "complete_tradable_market_observation")


def evaluation_window(snapshot: ShadowSnapshot) -> Verdict:
    market = snapshot.market
    if not all(_finite(value) for value in (snapshot.ts, market.open_ts, market.close_ts)):
        return Verdict(False, "missing_market_clock")
    passed = 90 <= snapshot.ts - market.open_ts <= 720 and market.close_ts - snapshot.ts >= 180
    return Verdict(passed, "window_90_to_720_and_180_before_close")


def book_freshness(snapshot: ShadowSnapshot) -> Verdict:
    for name, source_ts, kind, offset_ms in (("up", snapshot.up_book.source_ts, snapshot.up_book.source_ts_kind, 0.0),
                                             ("down", snapshot.down_book.source_ts, snapshot.down_book.source_ts_kind, 0.0),
                                             ("binance", snapshot.binance_source_ts, snapshot.binance_source_ts_kind,
                                              snapshot.system_clock_offset_ms)):
        passed, reason = _source_fresh(snapshot.ts, source_ts, kind, offset_ms, snapshot.system_clock_uncertainty_ms)
        if not passed:
            return Verdict(False, f"{name}_{reason}")
    return Verdict(True, "all_sources_fresh_within_750ms")


def polymarket_microshock(snapshot: ShadowSnapshot) -> Verdict:
    if not snapshot.baseline_valid:
        return Verdict(False, "baseline_not_available_at_10_seconds")
    shocked = snapshot.shocked_side
    delta = snapshot.up_delta_10s if shocked == "UP" else snapshot.down_delta_10s if shocked == "DOWN" else None
    return Verdict(_finite(delta) and delta >= MICROSHOCK, "one_directional_4c_shock_required")


def binance_neutrality(snapshot: ShadowSnapshot) -> Verdict:
    value = snapshot.btc_return_10s
    return Verdict(_finite(value) and abs(value) <= BINANCE_NEUTRALITY, "absolute_log_return_max_4bps")


def reversal_orderbook(snapshot: ShadowSnapshot) -> Verdict:
    shocked = snapshot.shocked_side
    book = snapshot.up_book if shocked == "UP" else snapshot.down_book if shocked == "DOWN" else None
    imbalance = None if book is None else book.imbalance(3)
    return Verdict(_finite(imbalance) and -1.0 <= imbalance <= IMBALANCE_MAX, "shocked_side_imbalance_max_minus_020")


def executable_contrarian_entry(snapshot: ShadowSnapshot) -> Verdict:
    book, fill = snapshot.candidate_book, snapshot.entry_fill
    if book is None or fill is None:
        return Verdict(False, "candidate_not_fillable_for_one_share")
    bid, ask = _decimal(book.best_bid), _decimal(book.best_ask)
    near_touch = book.near_touch_notional()
    if bid is None or ask is None or near_touch is None:
        return Verdict(False, "invalid_candidate_book")
    if ask > ENTRY_CAP:
        return Verdict(False, "ask_above_048")
    if ask - bid > SPREAD_CAP:
        return Verdict(False, "spread_above_0015")
    if near_touch < DEPTH_MIN:
        return Verdict(False, "near_touch_top3_notional_below_20")
    return Verdict(True, "one_share_vwap_fillable_at_near_touch")


def market_cooldown(snapshot: ShadowSnapshot) -> Verdict:
    if snapshot.previous_accepted_ts is None:
        return Verdict(True, "no_previous_accepted_candidate")
    if not _finite(snapshot.previous_accepted_ts) or not _finite(snapshot.ts):
        return Verdict(False, "invalid_previous_accepted_timestamp")
    elapsed = Decimal(str(snapshot.ts)) - Decimal(str(snapshot.previous_accepted_ts))
    return Verdict(elapsed >= Decimal(str(COOLDOWN_SECONDS)), "cooldown_60_seconds")


FilterRegistry: Mapping[str, Callable[[ShadowSnapshot], Verdict]] = MappingProxyType({
    "market_integrity": market_integrity, "evaluation_window": evaluation_window, "book_freshness": book_freshness,
    "polymarket_microshock": polymarket_microshock, "binance_neutrality": binance_neutrality,
    "reversal_orderbook": reversal_orderbook, "executable_contrarian_entry": executable_contrarian_entry,
    "market_cooldown": market_cooldown,
})


def evaluate_all(snapshot: ShadowSnapshot, registry: Optional[Mapping[str, Callable[[ShadowSnapshot], Verdict]]] = None) -> dict[str, Verdict]:
    registry = FilterRegistry if registry is None else registry
    verdicts: dict[str, Verdict] = {}
    for name, function in registry.items():
        try:
            verdict = function(snapshot)
            if not isinstance(verdict, Verdict) or type(verdict.passed) is not bool:
                raise TypeError("filter must return Verdict(bool, reason)")
            verdicts[name] = verdict
        except Exception as exc:
            verdicts[name] = Verdict(False, f"exception:{type(exc).__name__}")
    return verdicts


def would_enter(verdicts: Mapping[str, Verdict]) -> bool:
    return set(verdicts) == set(FilterRegistry) and all(verdict.passed is True for verdict in verdicts.values())


def snapshot_payload(snapshot: ShadowSnapshot) -> dict[str, object]:
    return {
        "ts": snapshot.ts, "market": asdict(snapshot.market), "up_book": snapshot.up_book.payload(),
        "down_book": snapshot.down_book.payload(), "binance_mid": snapshot.binance_mid,
        "binance_source_ts": snapshot.binance_source_ts, "binance_source_ts_kind": snapshot.binance_source_ts_kind,
        "system_clock_offset_ms": snapshot.system_clock_offset_ms,
        "system_clock_uncertainty_ms": snapshot.system_clock_uncertainty_ms,
        "baseline": None if snapshot.baseline is None else asdict(snapshot.baseline),
        "derived": {"up_delta_10s": snapshot.up_delta_10s, "down_delta_10s": snapshot.down_delta_10s,
                    "btc_return_10s": snapshot.btc_return_10s, "feature_age_seconds": snapshot.feature_age_seconds,
                    "entry_fill": None if snapshot.entry_fill is None else asdict(snapshot.entry_fill),
                    "entry_fee_per_share": None if snapshot.entry_fee_per_share is None else str(snapshot.entry_fee_per_share)},
        "previous_accepted_ts": snapshot.previous_accepted_ts,
    }


def snapshot_id(snapshot: ShadowSnapshot) -> str:
    payload = snapshot_payload(replace(snapshot, previous_accepted_ts=None))
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False, default=str)
    return sha256(encoded.encode("utf-8")).hexdigest()


def _verdicts_json(verdicts: Mapping[str, Verdict]) -> str:
    return json.dumps({name: asdict(value) for name, value in verdicts.items()}, sort_keys=True, separators=(",", ":"))


def _read_verdicts(value: str) -> dict[str, Verdict]:
    raw = json.loads(value)
    parsed = {name: Verdict(type(item["passed"]) is bool and item["passed"], str(item["reason"])) for name, item in raw.items()}
    if set(parsed) != set(FilterRegistry):
        raise ValueError("stored verdict set does not match the immutable registry")
    return parsed


SCHEMA = """
CREATE TABLE IF NOT EXISTS shadow_cohorts (
    cohort_name TEXT PRIMARY KEY, strategy_version TEXT NOT NULL, parameters_json TEXT NOT NULL,
    parameters_sha256 TEXT NOT NULL, approval_reference TEXT NOT NULL, approved_ts REAL NOT NULL,
    collection_started_ts REAL NOT NULL, created_ts REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS shadow_observations (
    cohort_name TEXT NOT NULL REFERENCES shadow_cohorts(cohort_name), snapshot_id TEXT NOT NULL, ts REAL NOT NULL,
    day_utc TEXT NOT NULL, episode_id TEXT NOT NULL, market_id TEXT NOT NULL, market_slug TEXT NOT NULL,
    candidate_side TEXT NOT NULL CHECK(candidate_side IN ('UP','DOWN')), entry_price REAL, entry_fee_per_share REAL,
    snapshot_json TEXT NOT NULL, verdicts_json TEXT NOT NULL, would_enter INTEGER NOT NULL CHECK(would_enter IN (0,1)),
    is_episode_representative INTEGER NOT NULL CHECK(is_episode_representative IN (0,1)),
    resolution_state TEXT NOT NULL CHECK(resolution_state IN ('PENDING','RESOLVED','VOID','QUARANTINED','RETRY_EXHAUSTED')),
    outcome_side TEXT CHECK(outcome_side IN ('UP','DOWN')), gross_virtual_pnl REAL, net_virtual_pnl REAL,
    outcome_evidence_json TEXT, resolution_attempts INTEGER NOT NULL DEFAULT 0, last_resolution_error TEXT,
    next_retry_ts REAL, resolved_ts REAL, PRIMARY KEY(cohort_name, snapshot_id)
);
CREATE TABLE IF NOT EXISTS shadow_heartbeats (
    cohort_name TEXT NOT NULL REFERENCES shadow_cohorts(cohort_name), ts REAL NOT NULL,
    complete INTEGER NOT NULL CHECK(complete IN (0,1)), reason TEXT NOT NULL,
    PRIMARY KEY(cohort_name, ts)
);
CREATE TABLE IF NOT EXISTS shadow_transport_events (
    cohort_name TEXT NOT NULL REFERENCES shadow_cohorts(cohort_name), ts REAL NOT NULL,
    source TEXT NOT NULL, endpoint TEXT NOT NULL, event TEXT NOT NULL, reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS shadow_input_attempts (
    cohort_name TEXT NOT NULL REFERENCES shadow_cohorts(cohort_name), ts REAL NOT NULL,
    payload_json TEXT NOT NULL, result_json TEXT NOT NULL,
    PRIMARY KEY(cohort_name, ts)
);
CREATE TABLE IF NOT EXISTS shadow_state_history (
    event_id INTEGER PRIMARY KEY, cohort_name TEXT NOT NULL, snapshot_id TEXT NOT NULL,
    changed_ts REAL NOT NULL, state_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_shadow_state_history ON shadow_state_history(cohort_name, changed_ts, event_id);
CREATE INDEX IF NOT EXISTS idx_shadow_pending ON shadow_observations(cohort_name, resolution_state, next_retry_ts, ts);
CREATE INDEX IF NOT EXISTS idx_shadow_episodes ON shadow_observations(cohort_name, episode_id, is_episode_representative, ts, snapshot_id);
CREATE INDEX IF NOT EXISTS idx_shadow_heartbeats ON shadow_heartbeats(cohort_name, ts);
CREATE INDEX IF NOT EXISTS idx_shadow_transport_events ON shadow_transport_events(cohort_name, ts);
"""


class ShadowStore:
    def __init__(self, db_path: Path) -> None:
        if str(db_path) == ":memory:":
            self.conn = sqlite3.connect(":memory:")
        else:
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def register_cohort(self, definition: CohortDefinition, *, created_ts: float) -> None:
        if not _finite(created_ts):
            raise ValueError("created_ts must be finite")
        expected = (definition.strategy_version, definition.parameters_json, definition.parameters_sha256,
                    definition.approval_reference, definition.approved_ts, definition.collection_started_ts)
        existing = self.conn.execute("SELECT strategy_version,parameters_json,parameters_sha256,approval_reference,approved_ts,collection_started_ts FROM shadow_cohorts WHERE cohort_name=?", (definition.cohort_name,)).fetchone()
        if existing is not None:
            if tuple(existing) != expected:
                raise RuntimeError("cohort already exists with different immutable approval data")
            return
        self.conn.execute("INSERT INTO shadow_cohorts VALUES (?,?,?,?,?,?,?,?)", (definition.cohort_name, *expected, created_ts))
        self.conn.commit()

    def previous_accepted_ts(self, cohort_name: str, market_id: str, before_ts: float) -> Optional[float]:
        row = self.conn.execute("SELECT ts FROM shadow_observations WHERE cohort_name=? AND market_id=? AND would_enter=1 AND ts<? ORDER BY ts DESC,snapshot_id DESC LIMIT 1", (cohort_name, market_id, before_ts)).fetchone()
        return None if row is None else float(row[0])

    def record_heartbeat(self, definition: CohortDefinition, *, ts: float, complete: bool, reason: str) -> None:
        """Record every future evaluation attempt, including no-candidate and failed inputs."""
        if not _finite(ts) or ts < definition.collection_started_ts:
            raise ValueError("heartbeat timestamp must be finite and inside the cohort")
        if not reason:
            raise ValueError("heartbeat reason is required")
        self.conn.execute(
            "INSERT OR IGNORE INTO shadow_heartbeats (cohort_name,ts,complete,reason) VALUES (?,?,?,?)",
            (definition.cohort_name, ts, int(complete), reason[:500]),
        )
        self.conn.commit()

    def record_transport_event(self, definition: CohortDefinition, *, ts: float, source: str,
                               endpoint: str, event: str, reason: str) -> None:
        if not _finite(ts) or ts < definition.collection_started_ts:
            raise ValueError("transport-event timestamp must be finite and inside the cohort")
        if not all(isinstance(value, str) and value for value in (source, endpoint, event, reason)):
            raise ValueError("transport-event fields are required")
        self.conn.execute(
            "INSERT INTO shadow_transport_events (cohort_name,ts,source,endpoint,event,reason) VALUES (?,?,?,?,?,?)",
            (definition.cohort_name, ts, source[:100], endpoint[:500], event[:100], reason[:500]),
        )
        self.conn.commit()

    def record_input_attempt(self, definition: CohortDefinition, *, ts: float,
                             payload: Mapping[str, object], result: Mapping[str, object]) -> None:
        """Raw decision-time evidence, including unevaluable inputs; never an outcome."""
        if not _finite(ts) or ts < definition.collection_started_ts:
            raise ValueError("input attempt timestamp outside cohort")
        values = (json.dumps(payload, sort_keys=True, default=str), json.dumps(result, sort_keys=True))
        previous = self.conn.execute(
            "SELECT payload_json,result_json FROM shadow_input_attempts WHERE cohort_name=? AND ts=?",
            (definition.cohort_name, ts),
        ).fetchone()
        if previous is not None and tuple(previous) != values:
            raise RuntimeError("input attempt identity collided with changed evidence")
        self.conn.execute("INSERT OR IGNORE INTO shadow_input_attempts VALUES (?,?,?,?)",
                          (definition.cohort_name, ts, *values))
        self.conn.commit()

    def _record_state(self, cohort_name: str, identity: str, changed_ts: float) -> None:
        cursor = self.conn.execute("SELECT resolution_state,outcome_side,gross_virtual_pnl,net_virtual_pnl,"
                                   "resolved_ts,resolution_attempts,last_resolution_error,next_retry_ts "
                                   "FROM shadow_observations WHERE cohort_name=? AND snapshot_id=?",
                                   (cohort_name, identity))
        row = cursor.fetchone()
        state = dict(zip((column[0] for column in cursor.description), row))
        self.conn.execute("INSERT INTO shadow_state_history(cohort_name,snapshot_id,changed_ts,state_json) VALUES (?,?,?,?)",
                          (cohort_name, identity, changed_ts, json.dumps(state)))

    def save_observation(self, definition: CohortDefinition, snapshot: ShadowSnapshot, verdicts: Mapping[str, Verdict]) -> tuple[str, bool]:
        candidate, episode = snapshot.candidate_side, snapshot.episode_id
        if candidate is None or episode is None:
            raise ValueError("only finite directional candidates may be persisted")
        identity = snapshot_id(snapshot)
        payload, verdicts_json, enters = json.dumps(snapshot_payload(snapshot), sort_keys=True, separators=(",", ":"), default=str), _verdicts_json(verdicts), int(would_enter(verdicts))
        existing = self.conn.execute("SELECT snapshot_json,verdicts_json,would_enter FROM shadow_observations WHERE cohort_name=? AND snapshot_id=?", (definition.cohort_name, identity)).fetchone()
        if existing is not None:
            if tuple(existing) != (payload, verdicts_json, enters):
                raise RuntimeError("deterministic snapshot identity collided with changed evidence")
            return identity, False
        representative = 0
        if enters:
            earlier_rep = self.conn.execute("SELECT ts,snapshot_id FROM shadow_observations WHERE cohort_name=? AND episode_id=? AND is_episode_representative=1", (definition.cohort_name, episode)).fetchone()
            if earlier_rep is None:
                representative = 1
            elif (snapshot.ts, identity) < (float(earlier_rep[0]), str(earlier_rep[1])):
                raise RuntimeError("out_of_order_accepted_candidate_would_change_frozen_episode_representative")
        fill, fee = snapshot.entry_fill, snapshot.entry_fee_per_share
        row = (definition.cohort_name, identity, snapshot.ts, datetime.fromtimestamp(snapshot.ts, timezone.utc).date().isoformat(), episode, snapshot.market.market_id, snapshot.market.market_slug, candidate, None if fill is None else float(fill.vwap), None if fee is None else float(fee), payload, verdicts_json, enters, representative, "PENDING", None, None, None, None, 0, None, snapshot.ts, None)
        self.conn.execute("INSERT INTO shadow_observations (cohort_name,snapshot_id,ts,day_utc,episode_id,market_id,market_slug,candidate_side,entry_price,entry_fee_per_share,snapshot_json,verdicts_json,would_enter,is_episode_representative,resolution_state,outcome_side,gross_virtual_pnl,net_virtual_pnl,outcome_evidence_json,resolution_attempts,last_resolution_error,next_retry_ts,resolved_ts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row)
        self._record_state(definition.cohort_name, identity, snapshot.ts)
        self.conn.commit()
        return identity, True

    def pending(self, cohort_name: str, *, now_ts: float, limit: int) -> list[sqlite3.Row]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        self.conn.row_factory = sqlite3.Row
        return self.conn.execute("SELECT * FROM shadow_observations WHERE cohort_name=? AND resolution_state='PENDING' AND (next_retry_ts IS NULL OR next_retry_ts<=?) ORDER BY ts,snapshot_id LIMIT ?", (cohort_name, now_ts, limit)).fetchall()

    def resolve(self, cohort_name: str, identity: str, result: "OutcomeResult", gross: Decimal, net: Decimal, *, now_ts: float) -> None:
        self.conn.execute("UPDATE shadow_observations SET resolution_state='RESOLVED',outcome_side=?,gross_virtual_pnl=?,net_virtual_pnl=?,outcome_evidence_json=?,resolved_ts=?,last_resolution_error=NULL,next_retry_ts=NULL WHERE cohort_name=? AND snapshot_id=? AND resolution_state='PENDING'", (result.outcome_side, float(gross), float(net), result.evidence_json, now_ts, cohort_name, identity))
        self._record_state(cohort_name, identity, now_ts)
        self.conn.commit()

    def mark_state(self, cohort_name: str, identity: str, state: str, reason: str, *, now_ts: float, attempts: int, next_retry_ts: Optional[float], evidence_json: Optional[str] = None) -> None:
        if state not in {"PENDING", "VOID", "QUARANTINED", "RETRY_EXHAUSTED"}:
            raise ValueError("invalid non-resolved state")
        self.conn.execute("UPDATE shadow_observations SET resolution_state=?,resolution_attempts=?,last_resolution_error=?,next_retry_ts=?,outcome_evidence_json=?,resolved_ts=? WHERE cohort_name=? AND snapshot_id=? AND resolution_state='PENDING'", (state, attempts, reason[:500], next_retry_ts, evidence_json, now_ts if state in {"VOID", "QUARANTINED"} else None, cohort_name, identity))
        self._record_state(cohort_name, identity, now_ts)
        self.conn.commit()

    def requeue_retry_exhausted(self, cohort_name: str, *, now_ts: float, limit: int = 500) -> int:
        if limit <= 0:
            raise ValueError("limit must be positive")
        rows = self.conn.execute("SELECT snapshot_id FROM shadow_observations WHERE cohort_name=? AND resolution_state='RETRY_EXHAUSTED' ORDER BY ts,snapshot_id LIMIT ?", (cohort_name, limit)).fetchall()
        self.conn.executemany("UPDATE shadow_observations SET resolution_state='PENDING',next_retry_ts=?,last_resolution_error='manual_requeue_after_retry_exhaustion' WHERE cohort_name=? AND snapshot_id=?", [(now_ts, cohort_name, row[0]) for row in rows])
        for row in rows:
            self._record_state(cohort_name, row[0], now_ts)
        self.conn.commit()
        return len(rows)


def record_shadow(store: ShadowStore, definition: CohortDefinition, snapshot: ShadowSnapshot) -> Optional[dict[str, object]]:
    if snapshot.candidate_side is None:
        return None
    decision = replace(snapshot, previous_accepted_ts=store.previous_accepted_ts(definition.cohort_name, snapshot.market.market_id, snapshot.ts))
    verdicts = evaluate_all(decision)
    identity, inserted = store.save_observation(definition, decision, verdicts)
    return {"snapshot_id": identity, "inserted": inserted, "candidate_side": decision.candidate_side, "would_enter": would_enter(verdicts), "verdicts": verdicts}


class DeterministicResolutionError(ValueError):
    pass


@dataclass(frozen=True)
class OutcomeResult:
    state: str
    outcome_side: Optional[str]
    reason: str
    evidence: Mapping[str, object]

    def __post_init__(self) -> None:
        if self.state not in {"PENDING", "RESOLVED", "VOID"}:
            raise ValueError("invalid outcome state")
        if self.state == "RESOLVED" and self.outcome_side not in {"UP", "DOWN"}:
            raise ValueError("resolved outcome must be UP or DOWN")
        if self.state != "RESOLVED" and self.outcome_side is not None:
            raise ValueError("only resolved results have an outcome side")
        object.__setattr__(self, "evidence", MappingProxyType(dict(self.evidence)))

    @property
    def evidence_json(self) -> str:
        return json.dumps(dict(self.evidence), sort_keys=True, separators=(",", ":"), default=str)


def virtual_pnl(candidate_side: str, outcome_side: str, entry_price: object, entry_fee_per_share: object = 0) -> tuple[Decimal, Decimal]:
    if candidate_side not in {"UP", "DOWN"} or outcome_side not in {"UP", "DOWN"}:
        raise DeterministicResolutionError("invalid_outcome_or_candidate_side")
    price, fee = _decimal(entry_price), _decimal(entry_fee_per_share)
    if price is None or not Decimal("0") < price <= Decimal("1"):
        raise DeterministicResolutionError("invalid_entry_price")
    if fee is None or fee < 0:
        raise DeterministicResolutionError("invalid_entry_fee")
    gross = Decimal("1") - price if candidate_side == outcome_side else -price
    return gross, gross - fee


NON_FINAL_RESOLUTION_STATUSES = {"proposed", "pending", "disputed", "challenge", "challenged", "in_review"}
VOID_RESOLUTION_STATUSES = {"cancelled", "canceled", "void", "invalid", "refunded"}


def terminal_outcome_from_market(payload: Mapping[str, object]) -> OutcomeResult:
    """Interpret Gamma evidence conservatively; a dispute never becomes a win."""
    status, evidence = str(payload.get("umaResolutionStatus", "")).strip().casefold(), dict(payload)
    if status in VOID_RESOLUTION_STATUSES:
        return OutcomeResult("VOID", None, f"uma_resolution_{status}", evidence)
    if payload.get("closed") is not True:
        return OutcomeResult("PENDING", None, "market_not_closed", evidence)
    if status in NON_FINAL_RESOLUTION_STATUSES:
        return OutcomeResult("PENDING", None, f"uma_resolution_{status}", evidence)
    if status:
        return OutcomeResult("PENDING", None, f"unrecognized_uma_resolution_status:{status}", evidence)
    try:
        raw_outcomes, raw_prices = payload["outcomes"], payload["outcomePrices"]
        outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes
        prices = json.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices
        if not isinstance(outcomes, list) or not isinstance(prices, list) or len(outcomes) != len(prices) or len(outcomes) != 2:
            raise ValueError("invalid_outcome_arrays")
        labels, values = [str(item).upper() for item in outcomes], [_decimal(item) for item in prices]
        if set(labels) != {"UP", "DOWN"} or any(item is None for item in values):
            raise ValueError("invalid_directional_outcomes")
        winners, losers = [label for label, value in zip(labels, values) if value == Decimal("1")], [value for value in values if value == Decimal("0")]
        if len(winners) != 1 or len(losers) != 1:
            raise ValueError("closed_market_not_finally_settled")
        return OutcomeResult("RESOLVED", winners[0], "closed_with_final_one_zero_payout", evidence)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return OutcomeResult("PENDING", None, f"invalid_terminal_market_payload:{type(exc).__name__}", evidence)


OutcomeFetcher = Callable[[str, str], Union[OutcomeResult, Optional[str]]]
GAMMA_MARKET_URL = "https://gamma-api.polymarket.com/markets/{market_id}"


def make_polymarket_outcome_fetcher(*, timeout_seconds: float = 5.0, open_url: Callable = urlopen) -> OutcomeFetcher:
    if not _finite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    def fetch_outcome(market_id: str, _market_slug: str) -> OutcomeResult:
        if not market_id:
            raise ValueError("missing_market_id")
        request = Request(GAMMA_MARKET_URL.format(market_id=market_id), headers={"Accept": "application/json", "User-Agent": "shadow-cohort-research/2.0"})
        with open_url(request, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("market_response_not_object")
        if str(payload.get("id", market_id)) != str(market_id):
            raise ValueError("market_id_mismatch")
        return terminal_outcome_from_market(payload)
    return fetch_outcome


def _normalize_outcome(result: Union[OutcomeResult, Optional[str]]) -> OutcomeResult:
    if isinstance(result, OutcomeResult):
        return result
    if result is None:
        return OutcomeResult("PENDING", None, "outcome_not_terminal", {})
    if result in {"UP", "DOWN"}:
        return OutcomeResult("RESOLVED", result, "legacy_callback_result_without_raw_evidence", {})
    raise ValueError("invalid_outcome_callback_result")


def resolve_pending(store: ShadowStore, definition: CohortDefinition, fetch_outcome: OutcomeFetcher, *, now_ts: float, limit: int = 500, max_fetch_failures: int = 5, retry_seconds: float = 3600.0) -> dict[str, int]:
    """Resolve a bounded batch. Network exhaustion is recoverable, not quarantine."""
    if limit <= 0 or max_fetch_failures <= 0 or retry_seconds < 0:
        raise ValueError("invalid resolver configuration")
    stats: Counter[str] = Counter()
    cache: dict[tuple[str, str], tuple[str, object]] = {}
    for row in store.pending(definition.cohort_name, now_ts=now_ts, limit=limit):
        identity = str(row["snapshot_id"])
        try:
            price, fee = _decimal(row["entry_price"]), _decimal(row["entry_fee_per_share"])
            if price is None or not Decimal("0") < price <= Decimal("1"):
                raise DeterministicResolutionError("invalid_entry_price")
            if fee is None or fee < 0:
                raise DeterministicResolutionError("invalid_entry_fee")
            key = (str(row["market_id"]), str(row["market_slug"]))
            if key not in cache:
                try:
                    cache[key] = ("result", _normalize_outcome(fetch_outcome(*key)))
                except Exception as exc:
                    cache[key] = ("fetch_error", exc)
            kind, value = cache[key]
            if kind == "fetch_error":
                raise value
            outcome: OutcomeResult = value  # type: ignore[assignment]
            if outcome.state == "PENDING":
                store.mark_state(definition.cohort_name, identity, "PENDING", outcome.reason, now_ts=now_ts, attempts=int(row["resolution_attempts"]), next_retry_ts=now_ts + retry_seconds, evidence_json=outcome.evidence_json); stats["not_terminal"] += 1
            elif outcome.state == "VOID":
                store.mark_state(definition.cohort_name, identity, "VOID", outcome.reason, now_ts=now_ts, attempts=int(row["resolution_attempts"]), next_retry_ts=None, evidence_json=outcome.evidence_json); stats["void"] += 1
            else:
                gross, net = virtual_pnl(str(row["candidate_side"]), str(outcome.outcome_side), price, fee)
                store.resolve(definition.cohort_name, identity, outcome, gross, net, now_ts=now_ts); stats["resolved"] += 1
        except DeterministicResolutionError as exc:
            store.mark_state(definition.cohort_name, identity, "QUARANTINED", str(exc), now_ts=now_ts, attempts=int(row["resolution_attempts"]), next_retry_ts=None); stats["quarantined"] += 1
        except Exception as exc:
            attempts = int(row["resolution_attempts"]) + 1
            if attempts >= max_fetch_failures:
                state, next_retry = "RETRY_EXHAUSTED", None; stats["retry_exhausted"] += 1
            else:
                state, next_retry = "PENDING", now_ts + min(retry_seconds * (2 ** (attempts - 1)), 86_400.0); stats["fetch_retry"] += 1
            store.mark_state(definition.cohort_name, identity, state, f"fetch_error:{type(exc).__name__}:{exc}", now_ts=now_ts, attempts=attempts, next_retry_ts=next_retry)
    return dict(stats)


def resolve_due_batches(store: ShadowStore, definition: CohortDefinition, fetch_outcome: OutcomeFetcher, *, now_ts: float,
                        batch_limit: int = 500, max_batches: int = 4, max_fetch_failures: int = 5,
                        retry_seconds: float = 3600.0) -> dict[str, int]:
    """Drain a bounded amount of due work for one scheduled resolver run.

    The bound is explicit so a burst cannot turn one hourly job into an
    unbounded process. A caller records the returned ``batches`` and any
    remaining PENDING rows as operational telemetry.
    """
    if max_batches <= 0:
        raise ValueError("max_batches must be positive")
    totals: Counter[str] = Counter()
    for _ in range(max_batches):
        batch = resolve_pending(store, definition, fetch_outcome, now_ts=now_ts, limit=batch_limit,
                                max_fetch_failures=max_fetch_failures, retry_seconds=retry_seconds)
        if not batch:
            break
        totals.update(batch)
        totals["batches"] += 1
        # Every processed PENDING result is rescheduled beyond now, so a fresh
        # empty response means the due queue has drained.
        if sum(batch.values()) < batch_limit:
            break
    remaining = store.conn.execute(
        "SELECT COUNT(*) FROM shadow_observations WHERE cohort_name=? AND resolution_state='PENDING' "
        "AND (next_retry_ts IS NULL OR next_retry_ts<=?)", (definition.cohort_name, now_ts),
    ).fetchone()[0]
    totals["due_remaining"] = int(remaining)
    return dict(totals)


def wilson_interval(successes: int, total: int, *, z: float = 1.959963984540054) -> Optional[dict[str, float]]:
    if total <= 0 or successes < 0 or successes > total or not _finite(z) or z <= 0:
        return None
    p, denominator = successes / total, 1 + z * z / total
    centre = (p + z * z / (2 * total)) / denominator
    radius = z * math.sqrt((p * (1 - p) + z * z / (4 * total)) / total) / denominator
    return {"estimate": p, "lower": max(0.0, centre - radius), "upper": min(1.0, centre + radius), "z": z}


def bootstrap_mean_interval(values: Sequence[float], *, samples: int = BOOTSTRAP_SAMPLES, seed: int = 20260907) -> Optional[dict[str, float]]:
    if not values or samples <= 0 or any(not _finite(value) for value in values):
        return None
    rng, size = random.Random(seed), len(values)
    means = sorted(sum(values[rng.randrange(size)] for _ in range(size)) / size for _ in range(samples))
    return {"estimate": mean(values), "lower": means[int(.025 * (samples - 1))], "upper": means[int(.975 * (samples - 1))], "samples": samples, "seed": seed}


def episode_stability(values: Sequence[float]) -> dict[str, object]:
    if not values:
        return {"status": "NO_DATA", "mean": None, "drop_best_mean": None, "leave_one_episode_out_min_mean": None, "positive_after_every_removal": False}
    raw_mean = mean(values)
    if len(values) == 1:
        return {"status": "ONE_EPISODE", "mean": raw_mean, "drop_best_mean": None, "leave_one_episode_out_min_mean": None, "positive_after_every_removal": False}
    best = max(range(len(values)), key=values.__getitem__)
    without_best = [value for index, value in enumerate(values) if index != best]
    leave_one = [mean([value for j, value in enumerate(values) if j != index]) for index in range(len(values))]
    stable = raw_mean > 0 and mean(without_best) > 0 and min(leave_one) > 0
    return {"status": "STABLE_POSITIVE" if stable else "UNSTABLE_OR_NONPOSITIVE", "mean": raw_mean, "drop_best_mean": mean(without_best), "leave_one_episode_out_min_mean": min(leave_one), "positive_after_every_removal": stable}


def grouped_stability(values_by_group: Mapping[str, Sequence[float]]) -> dict[str, object]:
    """Leave one correlated UTC-day group out of the episode-weighted P&L."""
    groups = {name: list(values) for name, values in values_by_group.items() if values}
    flattened = [value for values in groups.values() for value in values]
    if not flattened:
        return {"status": "NO_DATA", "group_count": 0, "mean": None,
                "leave_one_group_out_min_mean": None, "positive_after_every_removal": False}
    if len(groups) < 2:
        return {"status": "ONE_GROUP", "group_count": len(groups), "mean": mean(flattened),
                "leave_one_group_out_min_mean": None, "positive_after_every_removal": False}
    leave_one = [mean([value for other, values in groups.items() if other != name for value in values])
                 for name in groups]
    stable = mean(flattened) > 0 and min(leave_one) > 0
    return {"status": "STABLE_POSITIVE" if stable else "UNSTABLE_OR_NONPOSITIVE",
            "group_count": len(groups), "mean": mean(flattened),
            "leave_one_group_out_min_mean": min(leave_one), "positive_after_every_removal": stable}


def _representatives(rows: Sequence[sqlite3.Row], predicate: Callable[[sqlite3.Row], bool]) -> list[sqlite3.Row]:
    chosen: dict[str, sqlite3.Row] = {}
    for row in rows:
        if predicate(row):
            chosen.setdefault(str(row["episode_id"]), row)
    return list(chosen.values())


def simulate_remove(rows: Sequence[sqlite3.Row], filter_name: str) -> dict[str, bool]:
    """Replay rows in time order; removing a rule recomputes downstream cooldown."""
    if filter_name not in FilterRegistry:
        raise ValueError("unknown filter")
    if filter_name == "polymarket_microshock":
        return {}
    prior: dict[str, float] = {}
    result: dict[str, bool] = {}
    for row in rows:
        verdicts = _read_verdicts(str(row["verdicts_json"]))
        modified = {name: verdict.passed for name, verdict in verdicts.items()}
        modified[filter_name] = True
        if filter_name != "market_cooldown":
            previous = prior.get(str(row["market_id"]))
            modified["market_cooldown"] = previous is None or Decimal(str(row["ts"])) - Decimal(str(previous)) >= Decimal(str(COOLDOWN_SECONDS))
        enters = all(modified.values())
        result[str(row["snapshot_id"])] = enters
        if enters:
            prior[str(row["market_id"])] = float(row["ts"])
    return result


def _filter_diagnostics(rows: Sequence[sqlite3.Row]) -> dict[str, object]:
    family_size = len(FilterRegistry)
    bonferroni_z = NormalDist().inv_cdf(1 - .05 / (2 * family_size))
    diagnostics: dict[str, object] = {}
    for name in FilterRegistry:
        if name == "polymarket_microshock":
            diagnostics[name] = {"status": "NOT_IDENTIFIABLE", "reason": "only_directional_microshock_candidates_are_stored"}
            continue
        replay = simulate_remove(rows, name)
        incremental = _representatives(rows, lambda row: replay.get(str(row["snapshot_id"]), False) and not bool(row["would_enter"]))
        resolved = [row for row in incremental if row["resolution_state"] == "RESOLVED"]
        pnls = [float(row["net_virtual_pnl"]) for row in resolved]
        wins = sum(float(row["net_virtual_pnl"]) > 0 for row in resolved)
        diagnostics[name] = {"family_size": family_size, "bonferroni_z": bonferroni_z, "candidate_episode_representatives": len(incremental), "resolved_episode_representatives": len(resolved), "status": "LOW_CONFIDENCE" if len(resolved) < MIN_RESOLVED_EPISODES else "DESCRIPTIVE", "net_pnl_bootstrap": bootstrap_mean_interval(pnls), "win_rate_wilson": wilson_interval(wins, len(resolved), z=bonferroni_z), "episode_stability": episode_stability(pnls), "limitation": "filter-removal replay is limited to stored directional candidates"}
    return diagnostics


def _coverage_report(store: ShadowStore, definition: CohortDefinition, as_of_ts: float,
                     min_complete_rate: float = .95) -> dict[str, object]:
    heartbeats = store.conn.execute(
        "SELECT ts,complete,reason FROM shadow_heartbeats WHERE cohort_name=? AND ts<=? ORDER BY ts",
        (definition.cohort_name, as_of_ts),
    ).fetchall()
    if not heartbeats:
        return {"valid": False, "heartbeat_count": 0, "complete_rate": None,
                "max_gap_seconds": None, "reason": "no_heartbeat_evidence"}
    timestamps = [float(row[0]) for row in heartbeats]
    gaps = [timestamps[0] - definition.collection_started_ts]
    gaps.extend(right - left for left, right in zip(timestamps, timestamps[1:]))
    gaps.append(as_of_ts - timestamps[-1])
    complete_rate = sum(bool(row[1]) for row in heartbeats) / len(heartbeats)
    max_gap = max(gaps)
    complete_times = [float(row[0]) for row in heartbeats if bool(row[1])]
    boundaries = [definition.collection_started_ts, *complete_times, as_of_ts]
    complete_gap = max(right - left for left, right in zip(boundaries, boundaries[1:]))
    quality_ok = bool(complete_times) and complete_rate >= min_complete_rate and complete_gap <= MAX_HEARTBEAT_GAP_SECONDS
    return {
        "valid": max_gap <= MAX_HEARTBEAT_GAP_SECONDS and quality_ok,
        "minimum_complete_rate": min_complete_rate,
        "max_complete_snapshot_gap_seconds": complete_gap,
        "heartbeat_count": len(heartbeats),
        "complete_rate": complete_rate,
        "reason_counts": dict(Counter(str(row[2]) for row in heartbeats)),
        "incomplete_reason_counts": dict(Counter(str(row[2]) for row in heartbeats if not bool(row[1]))),
        "max_gap_seconds": max_gap,
        "reason": ("heartbeat_gap_exceeds_lock" if max_gap > MAX_HEARTBEAT_GAP_SECONDS else
                   "incomplete_data_coverage" if not quality_ok else "within_max_gap"),
    }


def cohort_report(store: ShadowStore, definition: CohortDefinition, *, as_of_ts: float,
                  min_complete_rate: float = .95) -> dict[str, object]:
    """Pre-registered statistics. It never authorizes real execution."""
    if not _finite(as_of_ts) or as_of_ts < definition.collection_started_ts:
        raise ValueError("as_of_ts must be on or after cohort start")
    if not _finite(min_complete_rate) or not 0 < min_complete_rate <= 1:
        raise ValueError("min_complete_rate must be in (0,1]")
    store.conn.row_factory = sqlite3.Row
    rows = [dict(row) for row in store.conn.execute(
        "SELECT * FROM shadow_observations WHERE cohort_name=? AND ts<=? ORDER BY ts,snapshot_id",
        (definition.cohort_name, as_of_ts))]
    historical_states = {}
    for event in store.conn.execute(
        "SELECT snapshot_id,state_json FROM shadow_state_history WHERE cohort_name=? AND changed_ts<=? ORDER BY changed_ts,event_id",
        (definition.cohort_name, as_of_ts)):
        historical_states[event["snapshot_id"]] = json.loads(event["state_json"])
    for row in rows:
        historical = historical_states.get(row["snapshot_id"])
        if historical is not None:
            row.update(historical)
        else:
            # Legacy databases have no transition history. Do not invent an as-of state.
            row.update(resolution_state="AS_OF_UNKNOWN", outcome_side=None,
                       gross_virtual_pnl=None, net_virtual_pnl=None, resolved_ts=None)
    states = Counter(str(row["resolution_state"]) for row in rows)
    accepted_reps = [row for row in rows if row["is_episode_representative"]]
    resolved_reps = [row for row in accepted_reps if row["resolution_state"] == "RESOLVED"]
    episode_pnls = [float(row["net_virtual_pnl"]) for row in resolved_reps]
    daily_episode_pnls: dict[str, list[float]] = {}
    for row in resolved_reps:
        daily_episode_pnls.setdefault(str(row["day_utc"]), []).append(float(row["net_virtual_pnl"]))
    accepted_rows = [row for row in rows if row["would_enter"] and row["resolution_state"] == "RESOLVED"]
    all_resolved = [row for row in rows if row["resolution_state"] == "RESOLVED"]
    elapsed_days = (as_of_ts - definition.collection_started_ts) / 86_400.0
    coverage = _coverage_report(store, definition, as_of_ts, min_complete_rate)
    transport_rows = store.conn.execute(
        "SELECT source,endpoint,event,reason FROM shadow_transport_events WHERE cohort_name=? AND ts<=?",
        (definition.cohort_name, as_of_ts),
    ).fetchall()
    transport_summary = {
        "count": len(transport_rows),
        "by_event": dict(Counter(str(row[2]) for row in transport_rows)),
        "by_endpoint": dict(Counter(str(row[1]) for row in transport_rows)),
        "by_reason": dict(Counter(str(row[3]) for row in transport_rows)),
    }
    gates = {"minimum_calendar_days": elapsed_days >= MIN_CALENDAR_DAYS, "heartbeat_coverage": coverage["valid"], "minimum_resolved_candidates": len(all_resolved) >= MIN_RESOLVED_CANDIDATES, "minimum_resolved_accepted_episodes": len(resolved_reps) >= MIN_RESOLVED_EPISODES, "no_retry_exhaustion": states["RETRY_EXHAUSTED"] == 0, "no_quarantined_rows": states["QUARANTINED"] == 0}
    bootstrap, stability = bootstrap_mean_interval(episode_pnls), episode_stability(episode_pnls)
    gates["complete_as_of_history"] = states["AS_OF_UNKNOWN"] == 0
    daily_stability = grouped_stability(daily_episode_pnls)
    if not all(gates.values()):
        strategy_status = "INSUFFICIENT_OR_INVALID_DATA"
    elif stability["status"] != "STABLE_POSITIVE" or daily_stability["status"] != "STABLE_POSITIVE":
        strategy_status = "NO_STABLE_POSITIVE_NET_SHADOW_EV"
    elif bootstrap is None or bootstrap["lower"] <= 0:
        strategy_status = "POSITIVE_BUT_STATISTICALLY_INCONCLUSIVE"
    else:
        strategy_status = "POSITIVE_IID_ESTIMATE_REQUIRES_CLUSTER_AND_EXECUTION_VALIDATION"
    wins = sum(float(row["net_virtual_pnl"]) > 0 for row in resolved_reps)
    return {"utc_day_cluster_sensitivity": day_cluster_sensitivity(daily_episode_pnls), "cohort_name": definition.cohort_name, "strategy_version": definition.strategy_version, "parameters_sha256": definition.parameters_sha256, "mode": "shadow_only_no_orders", "as_of_ts": as_of_ts, "collection_elapsed_days": elapsed_days, "coverage": coverage, "transport": transport_summary, "observations": len(rows), "would_enter_rows": sum(int(row["would_enter"]) for row in rows), "resolution_states": dict(states), "resolved_candidate_rows": len(all_resolved), "accepted_episode_representatives": len(accepted_reps), "resolved_accepted_episode_representatives": len(resolved_reps), "pending_or_invalid_episode_representatives": len(accepted_reps) - len(resolved_reps), "first_accepted_representative_rule": "frozen_at_record_time_by_smallest_(ts,snapshot_id)", "primary_net_pnl": {"representative_bootstrap": bootstrap, "episode_stability": stability, "utc_day_stability": daily_stability, "row_weighted_mean": None if not accepted_rows else mean(float(row["net_virtual_pnl"]) for row in accepted_rows)}, "supporting_win_rate_wilson": wilson_interval(wins, len(resolved_reps)), "pre_registered_gates": gates, "strategy_status": strategy_status, "filter_diagnostics": _filter_diagnostics(rows), "live_buy_enabled": False, "limitations": ["P&L uses a one-share near-touch VWAP estimate and observed taker fee; it is not a fill.", "Slippage beyond displayed depth, partial fills, latency, exits, cancellations and inventory risk need a separate execution study.", "The microshock trigger cannot be attributed because no non-microshock rows are stored.", "A positive shadow report never authorizes live trading."]}
