import json
from contextlib import closing
import sqlite3
import tempfile
import unittest
from pathlib import Path

from decision_audit import Snapshot, Verdict, FilterRegistry, evaluate_all, would_enter, audit


def row(ts=100):
    r = dict(ts=ts, market_id="m", market_slug="btc-updown-15m-0", episode_id="btc-episode-0",
             up_delta_10s=.05, down_delta_10s=-.05, btc_return_10s=.0001,
             up_imbalance_3=-.4, down_imbalance_3=.4, system_clock_uncertainty_ms=100.)
    for side in ("up", "down"):
        r[side + "_book_top3_json"] = json.dumps({"bids": [{"price": .46, "size": 100}],
                                                 "asks": [{"price": .47, "size": 100}]})
    for source in ("up", "down", "binance"):
        r[source + "_source_lag_ms"] = 200.
        r[source + "_source_ts"] = ts - .2
        r[source + "_source_ts_kind"] = "payload_timestamp_ms"
    return r


class DecisionAuditTests(unittest.TestCase):
    def test_snapshot_copies_and_freezes_input(self):
        r = row()
        s = Snapshot(r)
        r["ts"] = 1
        self.assertEqual(s.row["ts"], 100)
        with self.assertRaises(TypeError):
            s.row["ts"] = 1

    def test_all_eight_and_missing_integrity_is_not_an_entry(self):
        vs = evaluate_all(Snapshot(row()))
        self.assertEqual(len(vs), 8)
        self.assertIsNone(vs["market_integrity"].passed)
        self.assertFalse(would_enter(vs))
        self.assertFalse(would_enter({}))
        complete = {k: Verdict(True, "test") for k in FilterRegistry}
        self.assertTrue(would_enter(complete))
        complete.pop("evaluation_window")
        self.assertFalse(would_enter(complete))

    def test_window_boundaries_and_invalid_slug(self):
        for t, expected in [(89.999, False), (90, True), (720, True), (720.001, False)]:
            self.assertEqual(evaluate_all(Snapshot(row(t)))["evaluation_window"].passed, expected)
        r = row(); r["market_slug"] = "btc-updown-15m-1"
        self.assertFalse(evaluate_all(Snapshot(r))["market_integrity"].passed)

    def test_no_short_circuit_registry_name_survives_exception(self):
        calls = []
        def bad(_):
            raise ValueError("test")
        def good(_):
            calls.append("ran")
            return Verdict(True, "ok")
        result = evaluate_all(Snapshot(row()), {"stable_name": bad, "later": good, "bad_type": lambda _: True})
        self.assertEqual(calls, ["ran"])
        self.assertEqual(result["stable_name"].reason, "exception:ValueError")
        self.assertFalse(result["bad_type"].passed)
        self.assertTrue(result["later"].passed)

    def test_stale_negative_missing_and_nan_do_not_pass(self):
        for value in [-1., float("nan"), float("inf"), None, 750.01]:
            r = row(); r["up_source_lag_ms"] = value
            self.assertFalse(evaluate_all(Snapshot(r))["book_freshness"].passed)
        r = row(); r["up_source_lag_ms"] = 750.
        self.assertTrue(evaluate_all(Snapshot(r))["book_freshness"].passed)
        r["up_source_ts_kind"] = "http_date_second_precision"
        self.assertFalse(evaluate_all(Snapshot(r))["book_freshness"].passed)

    def test_missing_or_double_shock_cannot_select_direction(self):
        for value in [.05, None, float("nan")]:
            r = row(); r["down_delta_10s"] = value
            self.assertFalse(evaluate_all(Snapshot(r))["polymarket_microshock"].passed)

    def test_malformed_one_sided_and_crossed_books(self):
        for book in ["broken", "null", '{"bids":[],"asks":[]}',
                     '{"bids":[{"price":0.49,"size":100}],"asks":[{"price":0.47,"size":100}]}']:
            r = row(); r["down_book_top3_json"] = book
            self.assertFalse(evaluate_all(Snapshot(r))["executable_contrarian_entry"].passed)

    def test_exact_spread_boundary_uses_decimal(self):
        r = row(); r["down_book_top3_json"] = json.dumps({
            "bids": [{"price": .465, "size": 100}], "asks": [{"price": .48, "size": 100}]})
        self.assertTrue(evaluate_all(Snapshot(r))["executable_contrarian_entry"].passed)

    def test_cooldown_boundary(self):
        self.assertFalse(evaluate_all(Snapshot(row(159.999), 100))["market_cooldown"].passed)
        self.assertTrue(evaluate_all(Snapshot(row(160), 100))["market_cooldown"].passed)

    def test_readonly_replay_keeps_earliest_and_filters_before_cooldown(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "fixture.sqlite3"
            rows = [row(t) for t in [89, 90, 100, 149.9, 150, 720, 721]]
            columns = list(rows[0])
            with closing(sqlite3.connect(db)) as c:
                c.execute("CREATE TABLE telemetry_runs(run_id TEXT)")
                c.execute("INSERT INTO telemetry_runs VALUES ('r')")
                c.execute("CREATE TABLE telemetry_snapshots(run_id TEXT," + ",".join(columns) + ")")
                for r in rows:
                    c.execute("INSERT INTO telemetry_snapshots VALUES (" + ",".join("?" for _ in range(len(columns)+1)) + ")",
                              ["r"] + [r[k] for k in columns])
                c.commit()
            original = db.read_bytes()
            report = audit(db, "r", 720)
            self.assertEqual(report["rows"], 6)
            self.assertEqual(report["conditional_candidates"], 3)
            self.assertEqual(report["fully_verified_entries"], 0)
            self.assertEqual(report["hour_bins_with_candidates"], 1)
            self.assertEqual(report["first_representatives"]["btc-episode-0"]["ts"], 90)
            self.assertEqual(original, db.read_bytes())
            with self.assertRaises(ValueError):
                audit(db, "missing")
            with self.assertRaises(sqlite3.OperationalError):
                audit(Path(directory) / "missing.sqlite3", "r")
            self.assertFalse((Path(directory) / "missing.sqlite3").exists())


if __name__ == "__main__":
    unittest.main()
