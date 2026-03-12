from __future__ import annotations

import os
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime
from ftplib import FTP
from pathlib import Path

from lte_pm_platform.pipeline.ingest.file_discovery import (
    ParsedArchiveFile,
    RevisionPolicy,
    parse_archive_filename,
    select_parsed_files,
)


def is_zte_pm_zip_filename(filename: str) -> bool:
    return parse_archive_filename(Path(filename).name) is not None


class FtpClient:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        username: str,
        password: str,
        remote_directory: str,
        passive_mode: bool,
        ftp_factory: Callable[[], FTP] = FTP,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.remote_directory = remote_directory
        self.passive_mode = passive_mode
        self.ftp_factory = ftp_factory

    @contextmanager
    def session(self) -> Iterator[FTP]:
        ftp = self.connect()
        try:
            yield ftp
        finally:
            try:
                ftp.quit()
            except Exception:
                ftp.close()

    def connect(self) -> FTP:
        ftp = self.ftp_factory()
        ftp.connect(self.host, self.port)
        ftp.login(user=self.username, passwd=self.password)
        ftp.set_pasv(self.passive_mode)
        ftp.cwd(self.remote_directory)
        return ftp

    def list_files(self) -> list[str]:
        with self.session() as ftp:
            return sorted(ftp.nlst())

    def list_candidate_files(
        self,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        revision_policy: RevisionPolicy = "additive",
    ) -> list[str]:
        return [
            candidate.filename
            for candidate in self.list_candidate_details(
                start=start,
                end=end,
                revision_policy=revision_policy,
            )
        ]

    def list_candidate_details(
        self,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        revision_policy: RevisionPolicy = "additive",
    ) -> list[ParsedArchiveFile]:
        return select_parsed_files(
            self.list_files(),
            start=start,
            end=end,
            revision_policy=revision_policy,
        )

    def download_file(self, remote_filename: str, local_dir: Path) -> Path:
        local_dir.mkdir(parents=True, exist_ok=True)
        local_path = local_dir / Path(remote_filename).name
        fd, temp_name = tempfile.mkstemp(
            prefix=f"{local_path.stem}_",
            suffix=".part",
            dir=local_dir,
        )
        os.close(fd)
        temp_path = Path(temp_name)
        try:
            with self.session() as ftp, temp_path.open("wb") as local_handle:
                ftp.retrbinary(f"RETR {remote_filename}", local_handle.write)
            temp_path.replace(local_path)
        except Exception:
            if temp_path.exists():
                temp_path.unlink()
            raise
        return local_path
