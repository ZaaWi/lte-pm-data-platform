from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal

RevisionPolicy = Literal["additive", "base-only", "revisions-only", "latest-only"]

DEFAULT_LOCAL_SOURCE_ROOTS: dict[str, Path] = {
    "PM/itbbu/itbbuplat": Path.home() / "Downloads/FTP/PM/itbbu/itbbuplat",
    "PM/itbbu/ltefdd": Path.home() / "Downloads/FTP/PM/itbbu/ltefdd",
    "PM/sdr/ltefdd": Path.home() / "Downloads/FTP/PM/sdr/ltefdd",
}

FILENAME_FAMILY_PREFIXES: tuple[tuple[str, str], ...] = (
    ("UMEID_ITBBU_ITBBUPLAT_PM_COMMON_ZTE_", "PM/itbbu/itbbuplat"),
    ("UMEID_ITBBU_LTEFDD_PM_COMMON_ZTE_", "PM/itbbu/ltefdd"),
    ("UMEID_LTEFDD_PM_COMMON_ZTE_", "PM/sdr/ltefdd"),
)

ARCHIVE_FILENAME_PATTERN = re.compile(
    r"^(?P<prefix>UMEID(?:_ITBBU)?(?:_ITBBUPLAT|_LTEFDD)?_PM_COMMON_ZTE_)"
    r"(?P<date>\d{8})_(?P<time>\d{4})(?:_R(?P<revision>\d+))?"
    r"\.(?P<extension>tar\.gz|zip)$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ParsedArchiveFile:
    dataset_family: str
    filename: str
    interval_start: datetime
    revision: int
    extension: str
    path: str

    @property
    def interval_key(self) -> tuple[str, datetime]:
        return (self.dataset_family, self.interval_start)

    def as_dict(self) -> dict[str, str | int]:
        return {
            "dataset_family": self.dataset_family,
            "filename": self.filename,
            "interval_start": self.interval_start.strftime("%Y-%m-%d %H:%M:%S"),
            "revision": self.revision,
            "path": self.path,
        }


def parse_archive_filename(filename: str) -> ParsedArchiveFile | None:
    name = Path(filename).name
    match = ARCHIVE_FILENAME_PATTERN.fullmatch(name)
    if match is None:
        return None
    dataset_family = _dataset_family_from_prefix(match.group("prefix"))
    if dataset_family is None:
        return None
    interval_start = datetime.strptime(
        f"{match.group('date')}{match.group('time')}",
        "%Y%m%d%H%M",
    )
    revision = int(match.group("revision") or "0")
    return ParsedArchiveFile(
        dataset_family=dataset_family,
        filename=name,
        interval_start=interval_start,
        revision=revision,
        extension=match.group("extension").lower(),
        path=filename,
    )


def filter_by_time_range(
    files: list[ParsedArchiveFile],
    *,
    start: datetime | None,
    end: datetime | None,
) -> list[ParsedArchiveFile]:
    selected: list[ParsedArchiveFile] = []
    for file in files:
        if start is not None and file.interval_start < start:
            continue
        if end is not None and file.interval_start >= end:
            continue
        selected.append(file)
    return selected


def apply_revision_policy(
    files: list[ParsedArchiveFile],
    policy: RevisionPolicy,
) -> list[ParsedArchiveFile]:
    ordered = sorted(
        files,
        key=lambda item: (item.dataset_family, item.interval_start, item.revision),
    )
    if policy == "additive":
        return ordered
    if policy == "base-only":
        return [file for file in ordered if file.revision == 0]
    if policy == "revisions-only":
        return [file for file in ordered if file.revision > 0]
    if policy == "latest-only":
        latest_by_interval: dict[tuple[str, datetime], ParsedArchiveFile] = {}
        for file in ordered:
            latest_by_interval[file.interval_key] = file
        return sorted(
            latest_by_interval.values(),
            key=lambda item: (item.dataset_family, item.interval_start, item.revision),
        )
    raise ValueError(f"Unsupported revision policy: {policy}")


def discover_local_files(
    *,
    source_roots: dict[str, Path] | None = None,
    families: list[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    revision_policy: RevisionPolicy = "additive",
) -> list[ParsedArchiveFile]:
    roots = source_roots or DEFAULT_LOCAL_SOURCE_ROOTS
    selected_families = families or list(roots)
    parsed_files: list[ParsedArchiveFile] = []
    for family in selected_families:
        root = roots[family]
        for path in sorted(root.iterdir()):
            if not path.is_file():
                continue
            parsed = parse_archive_filename(path.name)
            if parsed is None or parsed.dataset_family != family:
                continue
            parsed_files.append(
                ParsedArchiveFile(
                    dataset_family=parsed.dataset_family,
                    filename=parsed.filename,
                    interval_start=parsed.interval_start,
                    revision=parsed.revision,
                    extension=parsed.extension,
                    path=str(path),
                )
            )
    return apply_revision_policy(
        filter_by_time_range(parsed_files, start=start, end=end),
        revision_policy,
    )


def select_parsed_files(
    filenames: list[str],
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    revision_policy: RevisionPolicy = "additive",
) -> list[ParsedArchiveFile]:
    parsed_files = [
        parsed for name in filenames if (parsed := parse_archive_filename(name)) is not None
    ]
    return apply_revision_policy(
        filter_by_time_range(parsed_files, start=start, end=end),
        revision_policy,
    )


def _dataset_family_from_prefix(prefix: str) -> str | None:
    for candidate_prefix, dataset_family in FILENAME_FAMILY_PREFIXES:
        if prefix.upper() == candidate_prefix.upper():
            return dataset_family
    return None
