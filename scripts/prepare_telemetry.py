"""Prepare one new approved telemetry interval after completed engineering tests.

The scheduler then resumes this exact identity. It cannot invent a new interval
after expiry or when the database is absent. This does not register a strategy cohort.
"""
import argparse
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from run_health import RunJournal, exclusive_run, source_fingerprint
from telemetry import TelemetryConfig, TelemetryStore, plan_run


def prepare(db: Path, source_report: Path, collector_report: Path) -> dict:
    sources = json.loads(source_report.read_text(encoding="utf-8"))
    collector = json.loads(collector_report.read_text(encoding="utf-8"))
    if sources.get("passed") is not True or collector.get("passed") is not True:
        raise RuntimeError("both completed source and collector acceptance reports must pass")
    if sources.get("mode") != "pre_cohort_source_acceptance_no_orders_no_pnl":
        raise RuntimeError("unexpected source acceptance report type")
    if collector.get("mode") != "disposable_end_to_end_telemetry_acceptance_no_cohort_approval":
        raise RuntimeError("unexpected collector acceptance report type")
    current = source_fingerprint()
    measured_files = ("telemetry.py", "shadow_runtime.py", "clock_sync.py")
    if any(collector.get("measurement_source_sha256", {}).get(name) != current[name] for name in measured_files):
        raise RuntimeError("measurement source changed since collector acceptance")
    config = TelemetryConfig(str(db.resolve()), binance_profile="failover")
    from dataclasses import asdict
    for key, value in asdict(config).items():
        if key not in {"db_path", "duration_seconds", "duration_days"} and collector.get("run_config", {}).get(key) != value:
            raise RuntimeError(f"collector configuration differs from acceptance: {key}")
    with exclusive_run(db.with_suffix(".lock")):
        now = time.time()
        plan_run(config, resume_latest=False, now_ts=now)
        store = TelemetryStore(db)
        try:
            run_id = store.start_run(config, now)
        finally:
            store.close()
        journal = RunJournal(db.with_suffix(".status.json"), kind="telemetry_only", run_id=run_id,
                             planned_end_ts=now + config.effective_duration_seconds,
                             measurement_version=config.measurement_version, binance_profile=config.binance_profile)
        journal.update("prepared", calibration_protocol="new_full_14_calendar_days",
                       acceptance_reports=[source_report.name, collector_report.name])
        return {"run_id": run_id, "started_ts": now,
                "planned_end_ts": now + config.effective_duration_seconds, "db": str(db)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--source-report", required=True, type=Path)
    parser.add_argument("--collector-report", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(prepare(args.db, args.source_report, args.collector_report), indent=2))
