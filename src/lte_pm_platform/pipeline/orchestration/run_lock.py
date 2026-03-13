from __future__ import annotations

import json
import os
from contextlib import contextmanager
from datetime import datetime, UTC
from pathlib import Path
from typing import Iterator


class PipelineCycleLockError(RuntimeError):
    def __init__(self, lock_path: Path, details: str | None = None) -> None:
        message = f"Another FTP pipeline cycle is already active: {lock_path}"
        if details:
            message = f"{message} ({details})"
        super().__init__(message)
        self.lock_path = lock_path


@contextmanager
def pipeline_cycle_lock(lock_path: Path) -> Iterator[None]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        details = None
        try:
            details = lock_path.read_text().strip() or None
        except Exception:
            details = None
        raise PipelineCycleLockError(lock_path, details) from exc

    try:
        payload = {
            "pid": os.getpid(),
            "started_at": datetime.now(UTC).isoformat(),
        }
        os.write(fd, json.dumps(payload).encode("utf-8"))
        yield
    finally:
        os.close(fd)
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass
