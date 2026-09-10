from dataclasses import replace
from decimal import Decimal
import tempfile
import unittest
from pathlib import Path

from shadow_cohort import (
    Book, CohortDefinition, FeatureBaseline, Level, LOCKED_PARAMETERS, MarketState,
    ShadowSnapshot, ShadowStore, Verdict, choose_feature_baseline, cohort_report, grouped_stability,
    evaluate_all, make_polymarket_outcome_fetcher, market_state_from_gamma, record_shadow, resolve_due_batches, resolve_pending,
    simulate_remove, terminal_outcome_from_market, virtual_pnl, would_enter,
)


def book(*, bid=.465, ask=.48, source_ts=100.2, asks=None):
    return Book((Level(bid, 100),), (Level(ask, 100),) if asks is None else asks,
                source_ts, "payload_timestamp_ms")


def market(market_id="m", start=0):
    return MarketState(market_id, f"btc-updown-15m-{start}", start, start + 900,
                       True, True, True, ("UP", "DOWN"), "up-token", "down-token", 700)


def snapshot(ts=100.2, *, market_id="m", start=0, up_book=None, down_book=None, baseline=None):
    baseline = FeatureBaseline(ts - 10, .45, .50, 100_000) if baseline is None else baseline
    return ShadowSnapshot(
        ts=ts, market=market(market_id, start),
        up_book=book(bid=.40, ask=.60, source_ts=ts) if up_book is None else up_book,
        down_book=book(source_ts=ts) if down_book is None else down_book,
        binance_mid=100_010, binance_source_ts=ts, binance_source_ts_kind="payload_E_ms",
        system_clock_offset_ms=0.0, system_clock_uncertainty_ms=0.0, baseline=baseline,
    )


def definition(start=100.0):
    return CohortDefinition("attribution-forward-20260917-reversion-v1", "microstructure-reversion-v1",
                            LOCKED_PARAMETERS, "operator approval 2026-09-17", start, start)


class ShadowCohortTests(unittest.TestCase):
    def test_complete_snapshot_is_executable_and_uses_vwap_and_fee(self):
        s = snapshot()
        self.assertTrue(would_enter(evaluate_all(s)))
        self.assertEqual(s.candidate_side, "DOWN")
        self.assertEqual(s.entry_fill.vwap, Decimal("0.48"))
        self.assertEqual(s.entry_fee_per_share, Decimal("0.017472"))

    def test_gamma_fee_schedule_is_required_and_drives_the_shadow_fee(self):
        payload = {
            "id": "m", "slug": "btc-updown-15m-0", "active": True, "acceptingOrders": True,
            "enableOrderBook": True, "outcomes": '["UP","DOWN"]', "clobTokenIds": '["up","down"]',
            "feesEnabled": True,
            "feeSchedule": {"rate": "0.07", "exponent": 1, "takerOnly": True, "rebateRate": "0.20"},
        }
        parsed = market_state_from_gamma(payload)
        self.assertEqual(parsed.taker_fee_per_share(Decimal("0.48")), Decimal("0.017472"))
        missing = dict(payload); missing.pop("feeSchedule")
        with self.assertRaises(ValueError):
            market_state_from_gamma(missing)

    def test_day_level_stability_rejects_a_profit_concentrated_in_one_day(self):
        # Ten profitable-looking hourly rows from one BTC move are not ten
        # independent confirmations when removing that day reverses the mean.
        stability = grouped_stability({"2026-09-01": [1.0] * 10, "2026-09-02": [-2.0]})
        self.assertEqual(stability["status"], "UNSTABLE_OR_NONPOSITIVE")
        self.assertFalse(stability["positive_after_every_removal"])

    def test_missing_inputs_and_unsorted_or_crossed_books_never_enter(self):
        missing = replace(snapshot(), baseline=FeatureBaseline(90.2, .45, None, 100_000))
        self.assertFalse(would_enter(evaluate_all(missing)))
        unsorted = book(asks=(Level(.48, 100), Level(.47, 100)))
        self.assertFalse(would_enter(evaluate_all(replace(snapshot(), down_book=unsorted))))
        self.assertFalse(would_enter(evaluate_all(replace(snapshot(), down_book=book(bid=.49, ask=.48)))))
        self.assertFalse(would_enter(evaluate_all(replace(snapshot(), down_book=book(bid=.48, ask=.48)))))

    def test_entry_depth_uses_all_near_touch_levels_not_only_top_three(self):
        sparse_top_three = book(
            bid=.465,
            asks=(Level(.48, 1), Level(.485, 1), Level(.489, 1), Level(.49, 40)),
        )
        decision = replace(snapshot(), down_book=sparse_top_three)
        self.assertTrue(evaluate_all(decision)["executable_contrarian_entry"].passed)

    def test_feature_baseline_never_uses_future_or_stale_data(self):
        history = [FeatureBaseline(89.1, .4, .5, 100), FeatureBaseline(90.0, .45, .5, 100),
                   FeatureBaseline(90.1, .46, .5, 100)]
        self.assertEqual(choose_feature_baseline(history, 100.2).observed_ts, 90.1)
        self.assertIsNone(choose_feature_baseline([FeatureBaseline(88, .4, .5, 100)], 100.2))
        stale = replace(snapshot(), baseline=FeatureBaseline(89, .45, .5, 100))
        self.assertFalse(evaluate_all(stale)["polymarket_microshock"].passed)

    def test_binance_freshness_applies_measured_clock_offset(self):
        # Local time is 1.1s ahead of Binance.  The quote is actually 100ms
        # old, so it must pass rather than be rejected from its raw timestamp.
        corrected = replace(snapshot(), ts=101.2, up_book=book(source_ts=101.2), down_book=book(source_ts=101.2),
                            binance_source_ts=100.1,
                            system_clock_offset_ms=1_100.0, system_clock_uncertainty_ms=100.0)
        self.assertTrue(evaluate_all(corrected)["book_freshness"].passed)
        too_old = replace(corrected, ts=102.0, up_book=book(source_ts=102.0), down_book=book(source_ts=102.0))
        self.assertFalse(evaluate_all(too_old)["book_freshness"].passed)
        self.assertEqual(evaluate_all(too_old)["book_freshness"].reason, "binance_stale_after_clock_uncertainty")

    def test_all_filters_run_when_one_raises(self):
        calls = []
        verdicts = evaluate_all(snapshot(), {"bad": lambda _: 1, "later": lambda _: calls.append(1) or Verdict(True, "ok")})
        self.assertEqual(calls, [1])
        self.assertEqual(verdicts["bad"].reason, "exception:TypeError")

    def test_cooldown_boundary_is_exact(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ShadowStore(Path(directory) / "shadow.sqlite3")
            try:
                d = definition(); store.register_cohort(d, created_ts=100)
                first = record_shadow(store, d, snapshot(100.2))
                blocked = record_shadow(store, d, snapshot(150.0))
                allowed = record_shadow(store, d, snapshot(160.2))
                self.assertTrue(first["would_enter"])
                self.assertFalse(blocked["would_enter"])
                self.assertTrue(allowed["would_enter"])
            finally:
                store.close()

    def test_representative_is_frozen_before_outcome_and_gates_30_days(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ShadowStore(Path(directory) / "shadow.sqlite3")
            try:
                d = definition(); store.register_cohort(d, created_ts=100)
                early = record_shadow(store, d, snapshot(100.2, market_id="early"))
                late = record_shadow(store, d, snapshot(160.2, market_id="late"))
                resolve_pending(store, d, lambda market_id, _: "DOWN" if market_id == "late" else None, now_ts=1_000)
                report = cohort_report(store, d, as_of_ts=1_000)
                self.assertEqual(report["resolved_accepted_episode_representatives"], 0)
                self.assertFalse(report["pre_registered_gates"]["minimum_calendar_days"])
                self.assertEqual(store.conn.execute("SELECT is_episode_representative FROM shadow_observations WHERE snapshot_id=?", (early["snapshot_id"],)).fetchone()[0], 1)
                self.assertEqual(store.conn.execute("SELECT is_episode_representative FROM shadow_observations WHERE snapshot_id=?", (late["snapshot_id"],)).fetchone()[0], 0)
            finally:
                store.close()

    def test_report_needs_time_rows_and_episodes(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ShadowStore(Path(directory) / "shadow.sqlite3")
            try:
                d = definition(); store.register_cohort(d, created_ts=100)
                for index in range(100):
                    start = index * 3600
                    result = record_shadow(store, d, snapshot(start + 100.2, market_id=f"m{index}", start=start))
                    resolve_pending(store, d, lambda *_: "DOWN", now_ts=2_500_000)
                    self.assertIsNotNone(result)
                # Candidate rows cannot prove that the collector ran between
                # candidates.  Synthetic 5-minute heartbeats make the
                # intended 30-day coverage explicit without pretending that
                # those rows are strategy observations.
                store.conn.executemany(
                    "INSERT INTO shadow_heartbeats (cohort_name,ts,complete,reason) VALUES (?,?,?,?)",
                    [(d.cohort_name, 100 + step * 300, 1, "complete_snapshot")
                     for step in range(30 * 86400 // 300 + 1)],
                )
                store.conn.commit()
                too_early = cohort_report(store, d, as_of_ts=100 + 29 * 86400)
                ready = cohort_report(store, d, as_of_ts=100 + 30 * 86400)
                self.assertEqual(too_early["strategy_status"], "INSUFFICIENT_OR_INVALID_DATA")
                self.assertTrue(ready["pre_registered_gates"]["minimum_calendar_days"])
                self.assertTrue(ready["pre_registered_gates"]["heartbeat_coverage"])
                self.assertTrue(ready["pre_registered_gates"]["minimum_resolved_candidates"])
                self.assertIn("representative_bootstrap", ready["primary_net_pnl"])
                self.assertIn("filter_diagnostics", ready)
            finally:
                store.close()

    def test_heartbeat_evidence_is_required_and_gap_is_locked(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ShadowStore(Path(directory) / "shadow.sqlite3")
            try:
                d = definition(); store.register_cohort(d, created_ts=100)
                absent = cohort_report(store, d, as_of_ts=399)
                self.assertFalse(absent["pre_registered_gates"]["heartbeat_coverage"])
                self.assertEqual(absent["coverage"]["reason"], "no_heartbeat_evidence")
                with self.assertRaises(ValueError):
                    store.record_heartbeat(d, ts=99.9, complete=True, reason="complete_snapshot")
                store.record_heartbeat(d, ts=100, complete=True, reason="complete_snapshot")
                store.record_heartbeat(d, ts=399, complete=False, reason="invalid_source_timestamp")
                covered = cohort_report(store, d, as_of_ts=399)
                self.assertFalse(covered["pre_registered_gates"]["heartbeat_coverage"])
                self.assertEqual(covered["coverage"]["reason"], "incomplete_data_coverage")
                self.assertEqual(covered["coverage"]["complete_rate"], .5)
                stale = cohort_report(store, d, as_of_ts=700.1)
                self.assertFalse(stale["pre_registered_gates"]["heartbeat_coverage"])
                self.assertEqual(stale["coverage"]["reason"], "heartbeat_gap_exceeds_lock")
            finally:
                store.close()

    def test_resolver_isolated_cached_and_transient_errors_are_requeueable(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ShadowStore(Path(directory) / "shadow.sqlite3")
            try:
                d = definition(); store.register_cohort(d, created_ts=100)
                record_shadow(store, d, snapshot(100.2, market_id="same"))
                record_shadow(store, d, snapshot(200.2, market_id="same"))
                calls = []
                self.assertEqual(resolve_pending(store, d, lambda *args: calls.append(args) or "DOWN", now_ts=1_000), {"resolved": 2})
                self.assertEqual(calls, [("same", "btc-updown-15m-0")])
                record_shadow(store, d, snapshot(3_700.2, market_id="bad", start=3600))
                self.assertEqual(resolve_pending(store, d, lambda *_: (_ for _ in ()).throw(TimeoutError("network")), now_ts=10_000, max_fetch_failures=1), {"retry_exhausted": 1})
                self.assertEqual(store.requeue_retry_exhausted(d.cohort_name, now_ts=11_000), 1)
                self.assertEqual(resolve_pending(store, d, lambda *_: "DOWN", now_ts=11_000), {"resolved": 1})
            finally:
                store.close()

    def test_invalid_price_quarantines_only_its_row(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ShadowStore(Path(directory) / "shadow.sqlite3")
            try:
                d = definition(); store.register_cohort(d, created_ts=100)
                record_shadow(store, d, snapshot(100.2, market_id="bad"))
                record_shadow(store, d, snapshot(3_700.2, market_id="good", start=3600))
                store.conn.execute("UPDATE shadow_observations SET entry_price=1.1 WHERE market_id='bad'"); store.conn.commit()
                self.assertEqual(resolve_pending(store, d, lambda *_: "DOWN", now_ts=10_000), {"quarantined": 1, "resolved": 1})
            finally:
                store.close()

    def test_disputed_market_is_pending_and_raw_evidence_is_retained(self):
        payload = {"closed": True, "umaResolutionStatus": "disputed", "outcomes": '["UP","DOWN"]', "outcomePrices": '["1","0"]'}
        result = terminal_outcome_from_market(payload)
        self.assertEqual((result.state, result.outcome_side), ("PENDING", None))
        self.assertIn("disputed", result.reason)
        self.assertIn("umaResolutionStatus", result.evidence_json)

    def test_gamma_fetcher_returns_structured_result_without_network(self):
        class Response:
            def __enter__(self): return self
            def __exit__(self, *_): return False
            def read(self): return b'{"id":"m","closed":true,"outcomes":"[\\"UP\\",\\"DOWN\\"]","outcomePrices":"[\\"0\\",\\"1\\"]"}'
        result = make_polymarket_outcome_fetcher(open_url=lambda *_args, **_kwargs: Response())("m", "ignored")
        self.assertEqual((result.state, result.outcome_side), ("RESOLVED", "DOWN"))

    def test_gamma_market_state_uses_current_fee_schedule(self):
        state = market_state_from_gamma({"id": "m", "slug": "btc-updown-15m-0", "active": True,
                                         "acceptingOrders": True, "enableOrderBook": True,
                                         "outcomes": '["UP","DOWN"]', "clobTokenIds": '["u","d"]',
                                         "feesEnabled": True,
                                         "feeSchedule": {"rate": ".07", "exponent": 1,
                                                         "takerOnly": True, "rebateRate": ".20"}})
        self.assertEqual(state.taker_fee_rate, Decimal(".07"))

    def test_bounded_resolver_drains_multiple_batches(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ShadowStore(Path(directory) / "shadow.sqlite3")
            try:
                d = definition(); store.register_cohort(d, created_ts=100)
                for index in range(3):
                    start = index * 3600
                    record_shadow(store, d, snapshot(start + 100.2, market_id=f"m{index}", start=start))
                report = resolve_due_batches(store, d, lambda *_: "DOWN", now_ts=20_000, batch_limit=1, max_batches=3)
                self.assertEqual(report, {"resolved": 3, "batches": 3, "due_remaining": 0})
            finally:
                store.close()

    def test_filter_removal_replays_cooldown_and_microshock_is_unidentifiable(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ShadowStore(Path(directory) / "shadow.sqlite3")
            try:
                d = definition(); store.register_cohort(d, created_ts=100)
                record_shadow(store, d, snapshot(100.2))
                second = record_shadow(store, d, snapshot(150.0))
                store.conn.row_factory = __import__("sqlite3").Row
                rows = store.conn.execute("SELECT * FROM shadow_observations ORDER BY ts,snapshot_id").fetchall()
                self.assertEqual(simulate_remove(rows, "market_cooldown")[second["snapshot_id"]], True)
                self.assertEqual(simulate_remove(rows, "polymarket_microshock"), {})
            finally:
                store.close()

    def test_virtual_pnl_is_net_of_fee(self):
        self.assertEqual(virtual_pnl("UP", "UP", ".48", ".017472"), (Decimal(".52"), Decimal(".502528")))


if __name__ == "__main__":
    unittest.main()
