"""Create and verify a consistent local SQLite backup, including active WAL data.

No network access; the backup must be copied to another device separately.
"""
import argparse
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import time


def backup(source: Path, destination: Path) -> dict:
    source, destination = source.resolve(), destination.resolve()
    if source == destination or destination.exists():
        raise FileExistsError("choose a new backup path; existing files are never overwritten")
    with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as incoming:
        destination.parent.mkdir(parents=True, exist_ok=True)
        # Exclusive creation protects against accidentally replacing another backup.
        with destination.open("xb"):
            pass
        with closing(sqlite3.connect(destination)) as outgoing:
            incoming.backup(outgoing, pages=1024)
            integrity = [row[0] for row in outgoing.execute("PRAGMA quick_check")]
            if integrity != ["ok"]:
                raise RuntimeError(f"backup failed SQLite quick_check: {integrity}")
            runs = [dict(zip(("run_id", "rows", "first_ts", "last_ts"), row)) for row in outgoing.execute(
                "SELECT run_id,COUNT(*),MIN(ts),MAX(ts) FROM telemetry_snapshots GROUP BY run_id")]
    checksum = hashlib.sha256()
    with destination.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(block)
    manifest = {"created_ts": time.time(), "file": destination.name, "size_bytes": destination.stat().st_size,
                "sha256": checksum.hexdigest(), "sqlite_quick_check": integrity, "runs": runs,
                "location": "local_only_not_an_off_device_backup"}
    with destination.with_suffix(destination.suffix + ".manifest.json").open("x", encoding="utf-8") as stream:
        json.dump(manifest, stream, indent=2)
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(backup(args.source, args.output), indent=2))
