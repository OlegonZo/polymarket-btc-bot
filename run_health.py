"""Local process evidence for telemetry and disposable acceptance runs."""
from contextlib import contextmanager
from hashlib import sha256
import json
import os
from pathlib import Path
import socket
import tempfile
import time


def atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def source_fingerprint() -> dict[str, str]:
    root = Path(__file__).resolve().parent
    return {name: sha256((root / name).read_bytes()).hexdigest() for name in
            ("telemetry.py", "shadow_runtime.py", "clock_sync.py", "preflight.py", "run_health.py")}


class RunJournal:
    def __init__(self, path: Path, *, kind: str, **metadata):
        self.path = path
        self.payload = {"kind": kind, "pid": os.getpid(), "host": socket.gethostname(),
                        "started_ts": time.time(), "source_sha256": source_fingerprint(), **metadata}

    def update(self, status: str, **values) -> None:
        self.payload.update(values)
        self.payload.update(status=status, heartbeat_ts=time.time())
        atomic_json(self.path, self.payload)


def read_status(path: Path, *, now_ts=None, stale_after_seconds=120.0) -> dict:
    """A recent heartbeat is process evidence, never proof of valid input data."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    now = time.time() if now_ts is None else now_ts
    heartbeat = payload.get("heartbeat_ts", path.stat().st_mtime)
    age = now - heartbeat
    reported = payload.get("status", "unknown")
    if reported in {"running", "starting"} and (age < 0 or age > stale_after_seconds):
        status = "stale_unknown"  # a killed process cannot publish its own terminal state
    else:
        status = reported
    return {**payload, "reported_status": reported, "status": status, "heartbeat_age_seconds": age}


@contextmanager
def exclusive_run(path: Path):
    """OS-held lock; survives stale lock files and releases after a process dies."""
    path.parent.mkdir(parents=True, exist_ok=True)
    stream = path.open("a+b")
    locked = False
    try:
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"0"); stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError("another process already owns this run") from exc
        locked = True
        yield
    finally:
        if locked:
            stream.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream, fcntl.LOCK_UN)
        stream.close()
