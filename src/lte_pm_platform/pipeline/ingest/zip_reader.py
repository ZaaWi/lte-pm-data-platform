from __future__ import annotations

import io
import tarfile
import zipfile
from collections.abc import Iterator
from pathlib import Path


def iter_csv_members(zip_path: Path) -> Iterator[tuple[str, io.TextIOWrapper]]:
    archive_name = zip_path.name.lower()
    if archive_name.endswith(".zip"):
        yield from iter_csv_from_zip(zip_path)
        return
    if archive_name.endswith(".tar.gz") or archive_name.endswith(".tgz"):
        yield from iter_csv_from_tar_gz(zip_path)
        return
    raise ValueError(f"Unsupported archive format: {zip_path}")


def iter_csv_from_zip(zip_path: Path) -> Iterator[tuple[str, io.TextIOWrapper]]:
    with zipfile.ZipFile(zip_path) as archive:
        for member in archive.infolist():
            if member.is_dir():
                continue
            if not member.filename.lower().endswith(".csv"):
                continue
            with archive.open(member, "r") as raw_stream:
                text_stream = io.TextIOWrapper(raw_stream, encoding="utf-8-sig", newline="")
                yield member.filename, text_stream


def iter_csv_from_tar_gz(archive_path: Path) -> Iterator[tuple[str, io.TextIOWrapper]]:
    with tarfile.open(archive_path, mode="r:gz") as archive:
        for member in archive.getmembers():
            if not member.isfile():
                continue
            if not member.name.lower().endswith(".csv"):
                continue
            extracted = archive.extractfile(member)
            if extracted is None:
                continue
            with extracted as raw_stream:
                text_stream = io.TextIOWrapper(raw_stream, encoding="utf-8-sig", newline="")
                yield member.name, text_stream
