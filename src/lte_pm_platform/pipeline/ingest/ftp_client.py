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
    def session(self, *, remote_directory: str | None = None) -> Iterator[FTP]:
        ftp = self.connect(remote_directory=remote_directory)
        try:
            yield ftp
        finally:
            try:
                ftp.quit()
            except Exception:
                ftp.close()

    def connect(self, *, remote_directory: str | None = None) -> FTP:
        ftp = self.ftp_factory()
        ftp.connect(self.host, self.port)
        ftp.login(user=self.username, passwd=self.password)
        ftp.set_pasv(self.passive_mode)
        ftp.cwd(remote_directory or self.remote_directory)
        return ftp

    def list_files(self, *, remote_directory: str | None = None) -> list[str]:
        with self.session(remote_directory=remote_directory) as ftp:
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
        remote_directory: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        revision_policy: RevisionPolicy = "additive",
    ) -> list[ParsedArchiveFile]:
        effective_remote_directory = remote_directory or self.remote_directory
        with self.session(remote_directory=effective_remote_directory) as ftp:
            filenames = sorted(ftp.nlst())
            parsed_candidates = select_parsed_files(
                filenames,
                start=start,
                end=end,
                revision_policy=revision_policy,
            )
            candidates_with_metadata: list[ParsedArchiveFile] = []
            for candidate in parsed_candidates:
                metadata = self._read_remote_metadata(ftp, candidate.path)
                candidates_with_metadata.append(
                    ParsedArchiveFile(
                        dataset_family=candidate.dataset_family,
                        filename=candidate.filename,
                        interval_start=candidate.interval_start,
                        revision=candidate.revision,
                        extension=candidate.extension,
                        path=str(Path(effective_remote_directory) / candidate.filename),
                        remote_size_bytes=metadata["remote_size_bytes"],
                        remote_modified_at=metadata["remote_modified_at"],
                    )
                )
            return candidates_with_metadata

    def download_file(self, remote_path: str, local_dir: Path) -> Path:
        local_dir.mkdir(parents=True, exist_ok=True)
        remote_directory = str(Path(remote_path).parent)
        remote_filename = Path(remote_path).name
        local_path = local_dir / remote_filename
        fd, temp_name = tempfile.mkstemp(
            prefix=f"{local_path.stem}_",
            suffix=".part",
            dir=local_dir,
        )
        os.close(fd)
        temp_path = Path(temp_name)
        try:
            with self.session(remote_directory=remote_directory) as ftp, temp_path.open("wb") as local_handle:
                ftp.retrbinary(f"RETR {remote_filename}", local_handle.write)
            temp_path.replace(local_path)
        except Exception:
            if temp_path.exists():
                temp_path.unlink()
            raise
        return local_path

    def _read_remote_metadata(self, ftp: FTP, remote_filename: str) -> dict[str, int | datetime | None]:
        return {
            "remote_size_bytes": self._read_remote_size(ftp, remote_filename),
            "remote_modified_at": self._read_remote_modified_at(ftp, remote_filename),
        }

    def _read_remote_size(self, ftp: FTP, remote_filename: str) -> int | None:
        try:
            size = ftp.size(remote_filename)
        except Exception:
            return None
        return int(size) if size is not None else None

    def _read_remote_modified_at(self, ftp: FTP, remote_filename: str) -> datetime | None:
        try:
            response = ftp.sendcmd(f"MDTM {remote_filename}")
        except Exception:
            return None
        if not response.startswith("213 "):
            return None
        timestamp = response.removeprefix("213 ").strip()
        try:
            return datetime.strptime(timestamp, "%Y%m%d%H%M%S")
        except ValueError:
            return None
