from dataclasses import replace
from decimal import Decimal
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from shadow_cohort import (FilterRegistry, ShadowStore, _filter_diagnostics, cohort_report,
                           episode_stability, record_shadow, resolve_pending, OutcomeResult)
from test_shadow_cohort import definition, snapshot


class ResearchReviewTests(unittest.TestCase):
    def test_pending_accepted_representative_blocks_a_conclusion(self):
        store = ShadowStore(Path(":memory:")); self.addCleanup(store.close)
        d = definition(); store.register_cohort(d, created_ts=100)
        record_shadow(store, d, snapshot())
        record_shadow(store, d, snapshot(3700.2, start=3600, market_id="second"))
        resolve_pending(store, d, lambda *_: "DOWN", now_ts=5000, limit=1)
        result = cohort_report(store, d, as_of_ts=5000)
        self.assertFalse(result["pre_registered_gates"]["all_accepted_representatives_terminal"])
        self.assertEqual(result["pending_or_invalid_episode_representatives"], 1)
        self.assertEqual(result["primary_family_size"], 1)

    def test_fixed_horizon_is_reported_as_infeasible_when_sample_is_missing(self):
        store = ShadowStore(Path(":memory:")); self.addCleanup(store.close)
        d = definition(); store.register_cohort(d, created_ts=100)
        result = cohort_report(store, d, as_of_ts=100 + 60 * 86400)
        self.assertEqual(result["strategy_status"], "INFEASIBLE_AT_REGISTERED_HORIZON")
        self.assertFalse(result["live_buy_enabled"])

    def test_all_blocked_is_descriptive_and_counts_episodes_not_rows(self):
        verdicts = {name: {"passed": name != "binance_neutrality", "reason": "test"} for name in FilterRegistry}
        rows = [{"snapshot_id": str(i), "episode_id": f"e{i // 10}", "ts": i,
                 "market_id": f"m{i}", "would_enter": 0,
                 "verdicts_json": json.dumps(verdicts),
                 "resolution_state": "RESOLVED", "net_virtual_pnl": -.5} for i in range(42)]
        result = _filter_diagnostics(rows)["binance_neutrality"]
        self.assertEqual(result["all_blocked_descriptive"]["resolved_episode_representatives"], 5)
        self.assertEqual(result["all_blocked_descriptive"]["status"], "LOW_CONFIDENCE")
        self.assertAlmostEqual(result["all_blocked_descriptive"]["win_rate_wilson"]["z"], 1.95996398454)
        self.assertGreater(result["solo_blocked"]["win_rate_wilson"]["z"], 2.7)
        rows[0]["resolution_state"] = "PENDING"
        result = _filter_diagnostics(rows)["binance_neutrality"]["solo_blocked"]
        self.assertEqual(result["eligible_episode_representatives"], 5)
        self.assertEqual(result["resolved_episode_representatives"], 4)

    def test_positive_first_trade_cannot_hide_losses_in_other_accepted_trades(self):
        store = ShadowStore(Path(":memory:")); self.addCleanup(store.close)
        d = definition(); store.register_cohort(d, created_ts=100)
        # 30 winning first representatives, each followed by three losing
        # accepted candidates: enough rows, but the actual accepted set loses.
        for day in range(30):
            for number, value in enumerate((.5, -.5, -.5, -.5)):
                start = day * 86400 + number * 900
                s = snapshot(start + 100.2, start=start, market_id=f"m{day}-{number}")
                record = record_shadow(store, d, s)
                self.assertTrue(record["would_enter"])
                store.resolve(d.cohort_name, record["snapshot_id"], OutcomeResult("RESOLVED", "DOWN" if value > 0 else "UP", "fixture", {}),
                              Decimal(str(value)), Decimal(str(value)), now_ts=s.ts + 900)
        with patch("shadow_cohort._coverage_report", return_value={"valid": True}):
            result = cohort_report(store, d, as_of_ts=100 + 30 * 86400)
        self.assertTrue(all(result["pre_registered_gates"].values()))
        self.assertGreater(result["primary_net_pnl"]["representative_bootstrap"]["lower"], 0)
        self.assertLess(result["equal_weighted_episode_mean"], 0)
        self.assertEqual(result["strategy_status"], "NO_STABLE_POSITIVE_NET_SHADOW_EV")

    def test_stability_does_not_reapply_minimum_sample_after_removal(self):
        result = episode_stability([.2] * 30)
        self.assertEqual(result["status"], "STABLE_POSITIVE")
        self.assertFalse(episode_stability([10] + [-.1] * 29)["positive_after_every_removal"])


if __name__ == "__main__":
    unittest.main()
