"""Offline, read-only audit of the draft's eight filters. No trading or training.

Telemetry does not persist market status/token metadata. Integrity therefore
remains UNKNOWN; conditional candidates are estimates, never verified entries.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
from dataclasses import dataclass
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
from types import MappingProxyType
from typing import Callable, Mapping

AUDIT_VERSION = "draft-audit-v1"


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


@dataclass(frozen=True)
class Verdict:
    passed: bool | None
    reason: str


@dataclass(frozen=True)
class Snapshot:
    row: Mapping
    previous_candidate_ts: float | None = None

    def __post_init__(self):
        object.__setattr__(self, "row", MappingProxyType(dict(self.row)))

    @property
    def side(self):
        up, down = self.row.get("up_delta_10s"), self.row.get("down_delta_10s")
        if not finite(up) or not finite(down):
            return None
        a, b = up >= .04, down >= .04
        return "down" if a and not b else "up" if b and not a else None

    @property
    def age(self):
        match = re.fullmatch(r"btc-updown-15m-(\d+)", str(self.row.get("market_slug", "")))
        ts = self.row.get("ts")
        if not match or not finite(ts) or int(match[1]) % 900:
            return None
        return ts - int(match[1])


def integrity(s):
    if s.age is None or not 0 < s.age < 900 or not s.row.get("market_id"):
        return Verdict(False, "invalid_market_identity_or_slot")
    if s.side is None:
        return Verdict(False, "missing_or_ambiguous_direction")
    return Verdict(None, "historical_status_and_token_pair_not_persisted")


def window(s):
    return Verdict(s.age is not None and 90 <= s.age <= 720, "window_90_to_720_seconds")


def freshness(s):
    for source in ("up", "down", "binance"):
        lag = s.row.get(f"{source}_source_lag_ms")
        timestamp = s.row.get(f"{source}_source_ts")
        kind = str(s.row.get(f"{source}_source_ts_kind", ""))
        if not finite(lag) or not finite(timestamp) or not kind.startswith("payload_"):
            return Verdict(False, f"{source}_missing_source_evidence")
        if lag < 0:
            return Verdict(False, f"{source}_negative_lag_unverifiable")
        if lag > 750:
            return Verdict(False, f"{source}_stale")
    return Verdict(True, "recorded_lags_within_750ms")


def shock(s):
    return Verdict(s.side is not None, "one_directional_4c_shock_required")


def neutrality(s):
    value = s.row.get("btc_return_10s")
    return Verdict(finite(value) and abs(value) <= .0004, "absolute_log_return_max_4bps")


def reversal(s):
    if s.side is None:
        return Verdict(False, "missing_direction")
    value = s.row.get("up_imbalance_3" if s.side == "down" else "down_imbalance_3")
    return Verdict(finite(value) and -1 <= value <= -.20, "shocked_side_imbalance_max_minus_020")


def executable(s):
    if s.side is None:
        return Verdict(False, "missing_direction")
    try:
        book = json.loads(s.row[f"{s.side}_book_top3_json"])
        parsed = {}
        for key in ("bids", "asks"):
            parsed[key] = []
            for level in book[key]:
                p, q = Decimal(str(level["price"])), Decimal(str(level["size"]))
                if not p.is_finite() or not q.is_finite() or not 0 < p <= 1 or q <= 0:
                    return Verdict(False, "invalid_book_level")
                parsed[key].append((p, q))
            parsed[key].sort(reverse=key == "bids")
        if not parsed["bids"] or not parsed["asks"]:
            return Verdict(False, "one_sided_book")
        bid, ask = parsed["bids"][0][0], parsed["asks"][0][0]
        if bid > ask:
            return Verdict(False, "crossed_book")
        if ask > Decimal(".48"):
            return Verdict(False, "ask_above_048")
        if ask - bid > Decimal(".015"):
            return Verdict(False, "spread_above_0015")
        depth = sum(p * q for p, q in parsed["asks"][:3] if p <= ask + Decimal(".01"))
        return Verdict(depth >= 20, "near_touch_top3_notional_min_20")
    except (ValueError, KeyError, TypeError, ArithmeticError):
        return Verdict(False, "malformed_book")


def cooldown(s):
    if s.previous_candidate_ts is None:
        return Verdict(True, "no_previous_conditional_candidate")
    ts = s.row.get("ts")
    return Verdict(finite(ts) and ts - s.previous_candidate_ts >= 60, "cooldown_60_seconds")


FilterRegistry: Mapping[str, Callable[[Snapshot], Verdict]] = MappingProxyType({
    "market_integrity": integrity,
    "evaluation_window": window,
    "book_freshness": freshness,
    "polymarket_microshock": shock,
    "binance_neutrality": neutrality,
    "reversal_orderbook": reversal,
    "executable_contrarian_entry": executable,
    "market_cooldown": cooldown,
})


def evaluate_all(snapshot, registry=None):
    registry = FilterRegistry if registry is None else registry
    verdicts = {}
    for name, fn in registry.items():
        try:
            verdict = fn(snapshot)
            if not isinstance(verdict, Verdict) or (verdict.passed is not None and type(verdict.passed) is not bool):
                raise TypeError("filter must return Verdict")
            verdicts[name] = verdict
        except Exception as exc:
            verdicts[name] = Verdict(False, f"exception:{type(exc).__name__}")
    return verdicts


def would_enter(verdicts):
    # Missing, extra or unknown verdicts never constitute an entry.
    return set(verdicts) == set(FilterRegistry) and all(v.passed is True for v in verdicts.values())


def audit(db, run_id, until=None):
    # mode=ro prevents accidental creation/mutation of the collector database.
    uri = Path(db).resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("BEGIN")  # one consistent view while collector writes
        run = connection.execute("SELECT * FROM telemetry_runs WHERE run_id=?", (run_id,)).fetchone()
        if run is None:
            raise ValueError("unknown run_id")
        params = [run_id]
        sql = "SELECT * FROM telemetry_snapshots WHERE run_id=?"
        if until is not None:
            if not finite(until):
                raise ValueError("until must be finite")
            sql += " AND ts<=?"
            params.append(until)
        rows = connection.execute(sql + " ORDER BY ts,market_id", params).fetchall()
    counts = {name: Counter() for name in FilterRegistry}
    reasons = {name: Counter() for name in FilterRegistry}
    prior, representatives = {}, {}
    conditional = verified = 0
    flags = Counter()
    funnel = Counter()
    stages = ("polymarket_microshock", "evaluation_window", "book_freshness", "binance_neutrality",
              "reversal_orderbook", "executable_contrarian_entry", "market_cooldown")
    for raw in rows:
        r = dict(raw)
        s = Snapshot(MappingProxyType(r), prior.get(r["market_id"]))
        vs = evaluate_all(s)
        for name, v in vs.items():
            state = "unknown" if v.passed is None else "pass" if v.passed else "fail"
            counts[name][state] += 1
            if state != "pass":
                reasons[name][v.reason] += 1
        verified += int(would_enter(vs))
        cumulative = vs["market_integrity"].passed is not False
        for name in stages:
            cumulative = cumulative and vs[name].passed is True
            if cumulative:
                funnel[name] += 1
        if cumulative:
            conditional += 1
            prior[r["market_id"]] = r["ts"]
            ep = f"btc-episode-{int(r['ts'] // 3600)}"
            representatives.setdefault(ep, {"ts": r["ts"], "market_id": r["market_id"], "side": s.side})
        uncertainty = r.get("system_clock_uncertainty_ms")
        if vs["book_freshness"].passed and (
            not finite(uncertainty) or uncertainty < 0 or any(
                r[f"{src}_source_lag_ms"] + uncertainty > 750 for src in ("up", "down", "binance")
            )
        ):
            flags["fresh_point_estimate_but_uncertainty_not_clear"] += 1
        if not finite(r["ts"]):
            flags["invalid_observation_timestamp"] += 1
        elif r["episode_id"] != f"btc-episode-{int(r['ts'] // 3600)}":
            flags["episode_id_mismatch"] += 1
    return {
        "audit_version": AUDIT_VERSION,
        "audit_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "run_id": run_id,
        "mode": "offline_telemetry_audit",
        "sql": sql + " ORDER BY ts,market_id", "query_parameters": params,
        "rows": len(rows),
        "first_ts": rows[0]["ts"] if rows else None,
        "last_ts": rows[-1]["ts"] if rows else None,
        "filters": {n: {**{k: counts[n][k] for k in ("pass", "fail", "unknown")},
                         "reasons": dict(reasons[n])} for n in FilterRegistry},
        "conditional_funnel": {n: funnel[n] for n in stages},
        "conditional_candidates": conditional,
        "hour_bins_with_candidates": len(representatives),
        "first_representatives": representatives,
        "fully_verified_entries": verified,
        "quality_flags": dict(flags),
        "live_buy_enabled": False,
        "learning_status": "NO_OUTCOME_LABELS_NO_MODEL_TRAINED",
        "limitations": [
            "Conditional counts assume historical market status/token pairing; telemetry did not persist them.",
            "Hourly bins are predeclared clusters, not proof of statistical independence.",
            "Lag checks use recorded receive-time point estimates, not a simultaneous decision-time guarantee.",
            "Negative/nonfinite lags and invalid books are rejected as unverifiable by this audit only.",
            "10-second feature baselines and continuity across gaps were not certified by this audit.",
            "Observed depth does not guarantee fills, profitability, or positive EV after costs.",
            "Cooldown here follows conditional candidates; no orders, PnL, outcome fetch or cohort writes occur.",
        ],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--until", type=float, help="Inclusive fixed Unix cutoff for repeatable historical audits")
    args = parser.parse_args()
    print(json.dumps(audit(args.db, args.run_id, args.until), indent=2, ensure_ascii=False))
