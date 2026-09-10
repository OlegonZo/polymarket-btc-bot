"""Pre-cohort source acceptance and final telemetry quality gates.

This utility has no cohort registration, resolver, wallet or order code.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
from dataclasses import asdict
import json
import math
from pathlib import Path
import sqlite3
import time
from typing import Any, Optional

from decision_audit import audit
from shadow_cohort import market_state_from_gamma
from shadow_runtime import DEFAULT_BINANCE_DEPTH_ENDPOINTS, FailoverBinanceDepthFeed, _rest_urls_for_ws
from telemetry import (
    GAMMA_EVENT_URL,
    BinanceDepthFeed,
    QuoteUnavailable,
    TelemetryConfig,
    fetch_json,
    market_slug,
    telemetry_report,
)


def _distribution(values: list[float]) -> dict[str, Optional[float]]:
    if not values:
        return {"n": 0, "p50": None, "p95": None, "max": None}
    ordered = sorted(values)
    percentile = lambda p: ordered[max(0, math.ceil(len(ordered) * p) - 1)]
    return {"n": len(ordered), "p50": percentile(.50), "p95": percentile(.95), "max": ordered[-1]}


def validate_live_fee_schedule(config: TelemetryConfig) -> dict[str, Any]:
    now = time.time()
    response = fetch_json(
        GAMMA_EVENT_URL.format(slug=market_slug(now)), config.timeout_seconds,
        stage="preflight_gamma_market", max_attempts=config.max_fetch_attempts,
        retry_backoff_seconds=config.retry_backoff_seconds,
    )
    markets = response.payload.get("markets")
    if not isinstance(markets, list) or len(markets) != 1 or not isinstance(markets[0], dict):
        raise RuntimeError("expected_exactly_one_gamma_market")
    raw = markets[0]
    market = market_state_from_gamma(raw)
    schedule = market.fee_schedule
    return {
        "passed": schedule is not None,
        "market_id": market.market_id,
        "market_slug": market.market_slug,
        "fees_enabled": None if schedule is None else schedule.enabled,
        "fee_schedule": None if schedule is None else {
            "rate": str(schedule.rate), "exponent": schedule.exponent,
            "taker_only": schedule.taker_only,
        },
        "raw_fee_fields": {"feesEnabled": raw.get("feesEnabled"), "feeSchedule": raw.get("feeSchedule")},
        "request_ms": response.request_ms,
    }


def _observe_feed(feed: BinanceDepthFeed, duration_seconds: float, sample_seconds: float = .1,
                  *, exercise_reconnect: bool = False, progress_callback=None) -> dict[str, Any]:
    if not math.isfinite(duration_seconds) or duration_seconds <= 0 or not math.isfinite(sample_seconds) or sample_seconds <= 0:
        raise ValueError("duration and sample interval must be positive and finite")
    started_ts, started = time.time(), time.monotonic()
    quote_ages: list[float] = []
    update_ids: list[int] = []
    unavailable: Counter[str] = Counter()
    rejected_clock: dict[str, list[float]] = {}
    error_examples: dict[str, str] = {}
    sample_times: list[float] = []
    valid_times: list[float] = []
    upper_ages: list[float] = []
    reconnect_requested = reconnect_recovered = False
    reconnect_update_id = None
    reconnect_generation = None
    last_progress = -30.0
    diagnostics, gaps = [], []
    feed.start()
    try:
        while time.monotonic() - started < duration_seconds:
            now = time.time()
            elapsed = time.monotonic() - started
            sample_times.append(elapsed)
            try:
                quote = feed.latest_quote(now)
                age = (now - float(quote.source_ts)) * 1000.0 - quote.clock_offset_ms
                uncertainty = quote.clock_uncertainty_ms
                if not math.isfinite(age) or not math.isfinite(uncertainty) or uncertainty < 0:
                    raise QuoteUnavailable("ws_clock_uncertain", "Invalid quote clock bounds")
                if age < 0:
                    raise QuoteUnavailable("ws_future_quote", "Future-dated quote")
                if age + uncertainty > feed.config.ws_freshness_ms:
                    raise QuoteUnavailable("ws_stale_quote", "Age upper bound exceeds freshness gate")
                quote_ages.append(age)
                upper_ages.append(age + uncertainty)
                valid_times.append(elapsed)
                if quote.update_id is not None:
                    update_ids.append(quote.update_id)
                if (reconnect_requested and quote.update_id is not None and quote.update_id != reconnect_update_id
                        and feed._connection_generation > reconnect_generation):
                    reconnect_recovered = True
                if exercise_reconnect and not reconnect_requested and elapsed >= duration_seconds / 2:
                    # Explicit fault injection on this disposable acceptance feed only.
                    with feed._lock:
                        active_socket = feed._socket
                    if active_socket is not None:
                        reconnect_update_id = quote.update_id
                        reconnect_generation = feed._connection_generation
                        feed._mark_disconnected("acceptance_injected_disconnect")
                        active_socket.close()
                        reconnect_requested = True
            except QuoteUnavailable as exc:
                unavailable[exc.classification] += 1
                error_examples.setdefault(exc.classification, str(exc))
                if hasattr(feed, "rejected_quote_clock_diagnostics"):
                    for key, value in feed.rejected_quote_clock_diagnostics(now).items():
                        rejected_clock.setdefault(key, []).append(value)
            diagnostics.extend(feed.drain_diagnostics())
            gaps.extend(feed.drain_gaps())
            if progress_callback is not None and elapsed - last_progress >= 30:
                progress_callback({"status": "running", "elapsed_seconds": elapsed,
                                   "sample_attempts": len(sample_times), "valid_samples": len(valid_times),
                                   "unavailable": dict(unavailable), "reconnect_requested": reconnect_requested,
                                   "reconnect_recovered": reconnect_recovered})
                last_progress = elapsed
            time.sleep(min(sample_seconds, max(0, duration_seconds - (time.monotonic() - started))))
    finally:
        observed_seconds = time.monotonic() - started
        feed.stop()
    diagnostics.extend(feed.drain_diagnostics())
    gaps.extend(feed.drain_gaps())
    boundaries = [0.0, *valid_times, observed_seconds]
    return {
        "started_ts": started_ts,
        "duration_seconds": observed_seconds,
        "sample_attempts": len(sample_times),
        "valid_attempt_fraction": len(valid_times) / len(sample_times) if sample_times else 0.0,
        "max_without_valid_quote_seconds": max(right - left for left, right in zip(boundaries, boundaries[1:])),
        "quote_age_upper_bound_ms": _distribution(upper_ages),
        "reconnect_requested": reconnect_requested,
        "reconnect_recovered": reconnect_recovered,
        "clock_sync": feed.clock_sync_diagnostics() if hasattr(feed, "clock_sync_diagnostics") else {},
        "quote_samples": len(quote_ages),
        "quote_age_ms": _distribution(quote_ages),
        "distinct_update_ids": len(set(update_ids)),
        "update_ids_monotonic": all(left <= right for left, right in zip(update_ids, update_ids[1:])),
        "unavailable": dict(unavailable),
        "error_examples": error_examples,
        "rejected_quote_clock": {key: _distribution(values) for key, values in rejected_clock.items()},
        "diagnostics": dict(Counter(item.classification for item in diagnostics)),
        "gaps_ms": _distribution([item.duration_ms for item in gaps]),
    }


def acceptance_criteria(observation: dict[str, Any]) -> dict[str, bool]:
    """Engineering policy v2, not a trading parameter or cohort approval."""
    return {
        "minimum_1800_seconds": observation["duration_seconds"] >= 1800,
        "at_least_95pct_valid_attempts": observation["valid_attempt_fraction"] >= .95,
        "maximum_10_seconds_without_valid_quote": observation["max_without_valid_quote_seconds"] <= 10,
        "multiple_updates": observation["distinct_update_ids"] > 1,
        "monotonic_updates": observation["update_ids_monotonic"],
        "injected_disconnect_recovered": observation["reconnect_requested"] and observation["reconnect_recovered"],
    }


def source_acceptance(*, duration_seconds: float, endpoint_probe_seconds: float,
                      output: Optional[Path] = None) -> dict[str, Any]:
    config = TelemetryConfig(Path("data/preflight-unused.sqlite3"))
    fee = validate_live_fee_schedule(config)
    endpoints: dict[str, Any] = {}
    for endpoint in DEFAULT_BINANCE_DEPTH_ENDPOINTS:
        depth_url, time_url = _rest_urls_for_ws(endpoint)
        feed = BinanceDepthFeed(config, ws_url=endpoint, depth_snapshot_url=depth_url, server_time_url=time_url)
        endpoints[endpoint] = _observe_feed(feed, endpoint_probe_seconds)
        probe = endpoints[endpoint]
        probe["passed"] = bool(
            probe["quote_samples"] > 0 and probe["distinct_update_ids"] > 0
            and probe["update_ids_monotonic"]
            and probe["quote_age_ms"]["max"] is not None
            and probe["quote_age_ms"]["max"] <= 750.0
        )
    failover = FailoverBinanceDepthFeed(config)
    def progress(payload):
        if output is not None:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.with_suffix(".progress.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    transport = _observe_feed(failover, duration_seconds, sample_seconds=.25,
                              exercise_reconnect=True, progress_callback=progress)
    transport["events"] = [asdict(event) for event in failover.drain_transport_events()]
    usable_endpoints = [endpoint for endpoint, probe in endpoints.items() if probe["passed"]]
    result = {
        "mode": "pre_cohort_source_acceptance_no_orders_no_pnl",
        "started_ts": transport["started_ts"],
        "completed_ts": time.time(),
        "fee_schedule": fee,
        "endpoint_probes": endpoints,
        "usable_endpoints": usable_endpoints,
        "failover_observation": transport,
        "quality_policy": "source_acceptance_v2_engineering_only",
        "criteria": acceptance_criteria(transport),
        "passed": bool(fee["passed"] and usable_endpoints and all(acceptance_criteria(transport).values())),
    }
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        progress({"status": "complete", "passed": result["passed"], "report": str(output)})
    return result


def final_telemetry_quality(db: Path, run_id: str, *, now_ts: Optional[float] = None,
                          include_audit: bool = True) -> dict[str, Any]:
    now = time.time() if now_ts is None else now_ts
    if not math.isfinite(now):
        raise ValueError("now_ts must be finite")
    uri = db.resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN")  # All quality counts use one consistent read snapshot.
        run = conn.execute("SELECT started_ts,planned_end_ts,config_json FROM telemetry_runs WHERE run_id=?", (run_id,)).fetchone()
        if run is None:
            raise ValueError("unknown telemetry run")
        row = conn.execute(
            "SELECT COUNT(*),MIN(ts),MAX(ts),COUNT(DISTINCT printf('%.6f|%s',ts,market_id)) "
            "FROM telemetry_snapshots WHERE run_id=?", (run_id,),
        ).fetchone()
        if not row[0]:
            return {"run_id": run_id, "rows": 0, "passed": False, "reason": "no_snapshots"}
        report = telemetry_report(conn, run_id)
        has_attempts = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='telemetry_attempts'").fetchone()
        attempts = conn.execute("SELECT COUNT(*),SUM(successful),MIN(ts),MAX(ts) FROM telemetry_attempts WHERE run_id=?",
                                (run_id,)).fetchone() if has_attempts else (0, 0, None, None)
        poll_seconds = float(json.loads(run["config_json"]).get("poll_seconds", 1.0))
        if not math.isfinite(poll_seconds) or poll_seconds <= 0:
            raise ValueError("invalid stored poll cadence")
        end = min(now, float(run["planned_end_ts"]))
        successful_slots = conn.execute(
            "SELECT COUNT(DISTINCT CAST((ts-?)/? AS INTEGER)) FROM telemetry_attempts "
            "WHERE run_id=? AND successful=1 AND ts>=? AND ts<=?",
            (run["started_ts"], poll_seconds, run_id, run["started_ts"], end),
        ).fetchone()[0] if has_attempts else 0
        negatives = conn.execute(
            "SELECT SUM(CASE WHEN up_source_lag_ms<0 THEN 1 ELSE 0 END),"
            "SUM(CASE WHEN down_source_lag_ms<0 THEN 1 ELSE 0 END),"
            "SUM(CASE WHEN binance_source_lag_ms<0 THEN 1 ELSE 0 END) "
            "FROM telemetry_snapshots WHERE run_id=?", (run_id,),
        ).fetchone()
    rows, first_ts, last_ts, distinct_rows = int(row[0]), float(row[1]), float(row[2]), int(row[3])
    elapsed_days = (last_ts - first_ts) / 86_400.0
    end = min(now, float(run["planned_end_ts"]))
    if now < last_ts:
        raise ValueError("now_ts precedes saved data; historical telemetry reports are not supported")
    tail_gap = max(0.0, end - last_ts)
    total_gap = max(report["snapshot_gaps_seconds"]["max"] or 0.0,
                    first_ts - float(run["started_ts"]), tail_gap)
    attempt_count = int(attempts[0])
    attempt_fraction = (attempts[1] or 0) / attempt_count if attempt_count else None
    # A partial post-migration attempt journal cannot certify an older run.
    attempt_history_complete = bool(attempt_count and attempts[2] <= first_ts and attempts[3] >= last_ts
                                    and (attempts[1] or 0) >= rows)
    expected_slots = max(1, math.ceil((end - float(run["started_ts"])) / poll_seconds))
    slot_fraction = min(1.0, successful_slots / expected_slots)
    pm_up = report["source_freshness"]["polymarket_up"]["payload_timestamp_within_750ms_pct"]
    pm_down = report["source_freshness"]["polymarket_down"]["payload_timestamp_within_750ms_pct"]
    binance = report["source_freshness"]["binance"]["payload_timestamp_within_750ms_pct"]
    criteria = {
        "minimum_13_9_observed_days": elapsed_days >= 13.9,
        "unique_snapshot_grain": rows == distinct_rows,
        "maximum_gap_at_most_300_seconds": total_gap <= 300.0,
        "complete_attempt_history": attempt_history_complete,
        "successful_attempts_at_least_95pct": attempt_fraction is not None and attempt_fraction >= .95,
        "successful_scheduled_slots_at_least_95pct": slot_fraction >= .95,
        "polymarket_up_fresh_at_least_95pct": pm_up is not None and pm_up >= 95.0,
        "polymarket_down_fresh_at_least_95pct": pm_down is not None and pm_down >= 95.0,
        "binance_fresh_at_least_99pct_of_saved_rows": binance is not None and binance >= 99.0,
        "midpoint_missing_at_most_15pct": report["midpoint_missing_analysis"]["rows_with_any_missing_pct"] <= 15.0,
        "negative_lag_rate_at_most_0_5pct": sum(int(value or 0) for value in negatives) / (3 * rows) <= .005,
    }
    return {
        "mode": "final_telemetry_quality_no_edge_claim",
        "run_id": run_id, "rows": rows, "elapsed_days": elapsed_days,
        "evaluated_ts": now, "coverage_end_ts": end, "tail_gap_seconds": tail_gap,
        "max_coverage_gap_seconds": total_gap,
        "attempts": {"count": attempt_count, "successful_fraction": attempt_fraction,
                     "history_complete": attempt_history_complete,
                     "successful_scheduled_slot_fraction": slot_fraction,
                     "expected_slots": expected_slots},
        "negative_lag_counts": {"up": negatives[0] or 0, "down": negatives[1] or 0, "binance": negatives[2] or 0},
        "criteria": criteria, "passed": all(criteria.values()),
        "telemetry_report": report,
        "offline_audit": audit(db, run_id) if include_audit else None,
        "offline_audit_consistency": "separate_read_not_used_in_quality_gates",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sources = sub.add_parser("sources")
    sources.add_argument("--duration-seconds", type=float, default=1800.0)
    sources.add_argument("--endpoint-probe-seconds", type=float, default=15.0)
    sources.add_argument("--output", type=Path)
    quality = sub.add_parser("final-quality")
    quality.add_argument("--db", type=Path, required=True); quality.add_argument("--run-id", required=True)
    quality.add_argument("--output", type=Path)
    quality.add_argument("--skip-offline-audit", action="store_true",
                         help="Run snapshot-consistent quality gates without the separate descriptive replay")
    args = parser.parse_args()
    if args.command == "sources":
        result = source_acceptance(duration_seconds=args.duration_seconds,
                                   endpoint_probe_seconds=args.endpoint_probe_seconds, output=args.output)
    else:
        result = final_telemetry_quality(args.db, args.run_id, include_audit=not args.skip_offline_audit)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    displayed = result
    if getattr(args, "output", None) is not None and args.command == "final-quality":
        displayed = {key: value for key, value in result.items()
                     if key not in {"telemetry_report", "offline_audit"}}
        displayed["full_report"] = str(args.output)
    print(json.dumps(displayed, ensure_ascii=False, indent=2))
    return 0 if result["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
