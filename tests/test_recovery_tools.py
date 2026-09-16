from dataclasses import asdict
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from run_health import atomic_json, source_fingerprint
from scripts.backup_telemetry import backup
from scripts.prepare_telemetry import prepare
from telemetry import TelemetryConfig, TelemetryStore, plan_run


class RecoveryToolsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def reports(self, passed=True):
        source, collector = self.root / "source.json", self.root / "collector.json"
        atomic_json(source, {"passed": passed, "mode": "pre_cohort_source_acceptance_no_orders_no_pnl"})
        atomic_json(collector, {"passed": True, "mode": "disposable_end_to_end_telemetry_acceptance_no_cohort_approval",
                               "measurement_source_sha256": source_fingerprint(),
                               "run_config": asdict(TelemetryConfig("smoke", duration_seconds=900, binance_profile="failover"))})
        return source, collector

    def test_prepare_blocks_failed_acceptance_before_creating_any_database(self):
        source, collector = self.reports(False)
        path = self.root / "calibration.sqlite3"
        with self.assertRaises(RuntimeError): prepare(path, source, collector)
        self.assertFalse(path.exists())

    def test_prepared_run_matches_exact_scheduler_resume_and_is_not_recreated(self):
        source, collector = self.reports()
        path = self.root / "calibration.sqlite3"
        result = prepare(path, source, collector)
        config = TelemetryConfig(str(path.resolve()), binance_profile="failover")
        plan = plan_run(config, resume_latest=True, now_ts=result["started_ts"] + 1)
        self.assertEqual(plan["run_id"], result["run_id"])
        self.assertEqual(plan["action"], "resume")
        self.assertEqual(result["planned_end_ts"] - result["started_ts"], 14 * 86400)
        with self.assertRaises(RuntimeError): prepare(path, source, collector)

    def test_backup_captures_committed_wal_and_never_overwrites(self):
        path = self.root / "source.sqlite3"
        store = TelemetryStore(path); self.addCleanup(store.close)
        store.conn.execute("PRAGMA journal_mode=WAL")
        run_id = store.start_run(TelemetryConfig(str(path)), 100)
        output = self.root / "backup.sqlite3"
        manifest = backup(path, output)
        conn = sqlite3.connect(output)
        try:
            self.assertEqual(conn.execute("SELECT run_id FROM telemetry_runs").fetchone()[0], run_id)
        finally:
            conn.close()
        self.assertEqual(manifest["sqlite_quick_check"], ["ok"])
        self.assertTrue(output.with_suffix(".sqlite3.manifest.json").exists())
        with self.assertRaises(FileExistsError): backup(path, output)


if __name__ == "__main__": unittest.main()
