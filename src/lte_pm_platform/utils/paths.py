from __future__ import annotations

from pathlib import Path


def project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def sql_init_dir() -> Path:
    return project_root() / "sql" / "init"


def data_input_dir() -> Path:
    return project_root() / "data" / "input"


def ftp_download_dir() -> Path:
    return data_input_dir() / "ftp"


def runtime_dir() -> Path:
    return project_root() / "data" / "runtime"


def ftp_cycle_lock_path() -> Path:
    return runtime_dir() / "ftp_run_cycle.lock"


def archive_dir() -> Path:
    return project_root() / "data" / "archive"


def rejected_dir() -> Path:
    return project_root() / "data" / "rejected"
