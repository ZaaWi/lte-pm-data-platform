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


def archive_dir() -> Path:
    return project_root() / "data" / "archive"


def rejected_dir() -> Path:
    return project_root() / "data" / "rejected"
